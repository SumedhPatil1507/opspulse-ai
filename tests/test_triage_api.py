"""
Unit tests for the standardized explainable remediation contract and the
dual-ingestion upload / triage endpoints.

Strategy
--------
* ``build_remediation_report`` is tested as a pure function — deterministic,
  no LLM, no network.
* The FastAPI routes are driven through ``httpx.AsyncClient`` +
  ``ASGITransport``.  The LangGraph runner and Qdrant indexer are swapped
  via FastAPI dependency overrides, so no broker, vector DB, or LLM key is
  required.

Contract under test (must hold for EVERY triage response):
    {"verdict": str, "reasoning": str, "recommendation": str,
     "next_steps": [str, ...]}
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.agent.reporting import build_remediation_report
from src.api.app import create_app
from src.api.core.config import get_settings
from src.api.models.remediation import RemediationReport
from src.api.routes.logs import get_runbook_indexer, get_triage_runner

BASE_URL = "http://testserver"

OOM_STACKTRACE = (
    "2026-10-08T10:15:25Z ERROR kubelet: Kill container payment-app in pod "
    "payment-processor-7f8b9-k2x4m with OOMKilled\n"
    "java.lang.OutOfMemoryError: Java heap space\n"
    "    at com.payments.App.processCharge(App.java:42)\n"
    "2026-10-08T10:15:26Z ERROR kubelet: OOMKilled payment-processor-7f8b9-l5p9n\n"
    "2026-10-08T10:15:27Z ERROR kubelet: OOMKilled payment-processor-7f8b9-m8q3r\n"
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

def fake_triage_runner(alert_data: dict[str, Any]) -> dict[str, Any]:
    """Deterministic stand-in for run_workflow returning a rich state."""
    return {
        "alert_data": alert_data,
        "parsed_log": {"primary_exception": "OutOfMemoryError"},
        "retrieved_runbooks": [
            {
                "source_file": "oom_killed_pod",
                "rerank_score": 0.962,
                "dense_score": 0.881,
                "rrf_score": 0.0319,
            }
        ],
        "analysis_summary": "Memory limits too low for peak payment traffic.",
        "proposed_action": {
            "title": "Scale Deployment Replicas to 5",
            "tool_name": "scale_deployment",
            "risk_tier": "HIGH",
            "tool_parameters": {"namespace": "payment-service", "replicas": 5},
            "action_id": "act-123",
        },
        "workflow_status": "WorkflowStatus.AWAITING_HUMAN",
        "approval_status": "PENDING",
    }


def fake_indexer(path: Path, settings: Any) -> dict[str, Any]:
    """Stand-in for on-the-fly Qdrant indexing."""
    return {
        "indexed": True,
        "indexed_chunks": 3,
        "collection": settings.QDRANT_COLLECTION,
        "total_points": 42,
    }


@pytest_asyncio.fixture
async def client(tmp_path: Path):
    """Async client with triage runner + Qdrant indexer dependency overrides."""
    app = create_app()
    app.dependency_overrides[get_triage_runner] = lambda: fake_triage_runner
    app.dependency_overrides[get_runbook_indexer] = lambda: fake_indexer
    # Redirect RUNBOOKS_DIR to a temp path so uploads never pollute the repo.
    test_settings = get_settings().model_copy(
        update={"RUNBOOKS_DIR": str(tmp_path / "runbooks")}
    )
    app.dependency_overrides[get_settings] = lambda: test_settings

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
        yield ac


# ===========================================================================
# 1. RemediationReport schema contract
# ===========================================================================

class TestRemediationReportSchema:

    def test_report_has_exact_contract_fields(self) -> None:
        state = fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        report = build_remediation_report(state)

        assert set(report.model_dump().keys()) == {
            "verdict",
            "reasoning",
            "recommendation",
            "next_steps",
        }

    def test_verdict_is_uppercase_and_mentions_signature(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert "OOMKILLED" in report.verdict
        assert report.verdict == report.verdict.upper()

    def test_cascade_detected_for_multi_pod_oom(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert "CASCADE" in report.verdict

    def test_reasoning_contains_runbook_similarity_scores(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert "rerank_score=" in report.reasoning
        assert "oom_killed_pod" in report.reasoning
        assert "Matched stack trace line" in report.reasoning

    def test_recommendation_is_risk_rated(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert report.recommendation.startswith("[HIGH]")
        assert "Scale Deployment Replicas to 5" in report.recommendation

    def test_next_steps_are_ordered_and_numbered(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert len(report.next_steps) >= 3
        assert report.next_steps[0].startswith("Step 1:")
        assert report.next_steps[1].startswith("Step 2:")
        assert report.next_steps[2].startswith("Step 3:")

    def test_step2_references_approval_for_scale_tool(self) -> None:
        report = build_remediation_report(
            fake_triage_runner({"raw_log_stacktrace": OOM_STACKTRACE})
        )
        assert "Approve" in report.next_steps[1]

    def test_degraded_state_still_produces_valid_report(self) -> None:
        """No LLM / no runbooks → builder must still emit the contract."""
        report = build_remediation_report({
            "alert_data": {"raw_log_stacktrace": "CrashLoopBackOff back-off 5m0s"},
            "parsed_log": {},
            "retrieved_runbooks": [],
            "proposed_action": {},
        })
        assert isinstance(report, RemediationReport)
        assert "CRASHLOOPBACKOFF" in report.verdict
        assert report.next_steps

    def test_model_forbids_extra_fields(self) -> None:
        with pytest.raises(Exception):
            RemediationReport(  # type: ignore[call-arg]
                verdict="X", reasoning="Y", recommendation="Z",
                next_steps=["a"], surprise="extra",
            )


# ===========================================================================
# 2. POST /api/v1/triage — standardized JSON contract
# ===========================================================================

class TestTriageEndpoint:

    async def test_triage_returns_exact_contract(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/triage",
            json={"raw_log_stacktrace": OOM_STACKTRACE},
        )
        assert response.status_code == 200
        body = response.json()
        assert set(body.keys()) == {
            "verdict",
            "reasoning",
            "recommendation",
            "next_steps",
        }
        assert isinstance(body["next_steps"], list)
        assert all(isinstance(s, str) for s in body["next_steps"])

    async def test_triage_missing_stacktrace_is_422(self, client) -> None:
        ac = client
        response = await ac.post("/api/v1/triage", json={"service_name": "x"})
        assert response.status_code == 422

    async def test_triage_empty_stacktrace_is_422(self, client) -> None:
        ac = client
        response = await ac.post("/api/v1/triage", json={"raw_log_stacktrace": ""})
        assert response.status_code == 422


# ===========================================================================
# 3. POST /api/v1/logs/upload — drag-and-drop dual ingestion
# ===========================================================================

class TestLogUploadEndpoint:

    async def test_log_upload_returns_triage_block(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/logs/upload",
            files=[("files", ("crash.log", OOM_STACKTRACE.encode(), "text/plain"))],
        )
        assert response.status_code == 200
        body = response.json()
        assert "triage" in body
        assert set(body["triage"].keys()) == {
            "verdict",
            "reasoning",
            "recommendation",
            "next_steps",
        }

    async def test_txt_extension_supported(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/logs/upload",
            files=[("files", ("trace.txt", b"ValueError: boom", "text/plain"))],
        )
        assert response.status_code == 200
        assert "triage" in response.json()

    async def test_runbook_upload_indexes_into_qdrant(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/logs/upload",
            files=[
                (
                    "files",
                    ("disk_full_runbook.md", b"# PV Disk Full\n## Remediation\nExpand PVC.", "text/markdown"),
                )
            ],
        )
        assert response.status_code == 200
        body = response.json()
        assert "runbooks" in body
        assert body["runbooks"][0]["indexed"] is True
        assert body["runbooks"][0]["indexed_chunks"] == 3

    async def test_mixed_upload_returns_triage_and_runbooks(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/logs/upload",
            files=[
                ("files", ("app.log", OOM_STACKTRACE.encode(), "text/plain")),
                ("files", ("new_runbook.md", b"# Runbook\nContent.", "text/markdown")),
            ],
        )
        assert response.status_code == 200
        body = response.json()
        assert "triage" in body and "runbooks" in body

    async def test_unsupported_extension_rejected(self, client) -> None:
        ac = client
        response = await ac.post(
            "/api/v1/logs/upload",
            files=[("files", ("malware.exe", b"MZ", "application/octet-stream"))],
        )
        assert response.status_code == 400

    async def test_no_files_is_400(self, client) -> None:
        ac = client
        response = await ac.post("/api/v1/logs/upload", files=[])
        assert response.status_code in (400, 422)
