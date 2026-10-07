"""
Unit tests for the alert ingestion API.

Strategy
--------
* ``httpx.AsyncClient`` + ``ASGITransport`` drives the ASGI app directly —
  no real HTTP port is opened, no network dependency.
* The Celery ``send_task`` call is **always mocked** so tests never require
  a running Redis broker.  The mock is injected via FastAPI's dependency
  override mechanism, keeping the production code untouched.
* ``pytest-asyncio`` with ``asyncio_mode = auto`` (set in pytest.ini) means
  every ``async def test_*`` runs under asyncio automatically.

Test groups
-----------
1. AlertPayload model — pure Pydantic validation (no HTTP).
2. POST /api/v1/alerts/ingest — happy-path 202, all validation failure modes,
   Celery enqueue failure → 500, response shape, idempotency marker.
3. GET /healthz — liveness probe.
4. Custom 422 handler — shape of the error response.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from src.api.app import create_app
from src.api.core.config import get_settings
from src.api.models.alert import AlertPayload, AlertIngestionResponse, Environment, Severity
from src.api.routes.alerts import get_celery_app

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

BASE_URL = "http://testserver"
INGEST_URL = "/api/v1/alerts/ingest"


def _valid_payload(**overrides: Any) -> dict[str, Any]:
    """Return a minimal valid alert payload dict, optionally overriding fields."""
    base: dict[str, Any] = {
        "alert_id": "test-alert-001",
        "service_name": "payment-service",
        "severity": "critical",
        "raw_log_stacktrace": "Traceback (most recent call last):\n  File 'app.py', line 42\nValueError: boom",
        "timestamp": "2026-10-06T12:00:00Z",
        "environment": "production",
    }
    base.update(overrides)
    return base


def _make_mock_celery(task_id: str | None = None) -> MagicMock:
    """
    Build a MagicMock Celery app whose ``send_task`` returns an
    AsyncResult-like object carrying the given task_id.
    """
    fake_task_id = task_id or str(uuid.uuid4())
    mock_result = MagicMock()
    mock_result.id = fake_task_id

    mock_celery = MagicMock()
    mock_celery.send_task.return_value = mock_result
    return mock_celery


@pytest_asyncio.fixture
async def client():
    """
    Async HTTP client wired to the FastAPI app.

    The Celery dependency is overridden with a mock so no broker is needed.
    ``get_settings`` cache is cleared before each test to avoid cross-test
    pollution from env-var mutations.
    """
    get_settings.cache_clear()

    app = create_app()
    mock_celery = _make_mock_celery()

    # Override the Celery dependency — swaps out the real broker connection.
    app.dependency_overrides[get_celery_app] = lambda: mock_celery

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
        yield ac, mock_celery


@pytest_asyncio.fixture
async def client_with_task_id():
    """
    Like ``client`` but exposes the generated task_id for assertion.
    """
    get_settings.cache_clear()

    app = create_app()
    task_id = str(uuid.uuid4())
    mock_celery = _make_mock_celery(task_id=task_id)

    app.dependency_overrides[get_celery_app] = lambda: mock_celery

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
        yield ac, mock_celery, task_id


# ===========================================================================
# 1.  AlertPayload model — pure Pydantic validation
# ===========================================================================

class TestAlertPayloadModel:
    """Validate the Pydantic model in isolation, no HTTP layer involved."""

    def test_valid_payload_parses_correctly(self):
        alert = AlertPayload(**_valid_payload())
        assert alert.alert_id == "test-alert-001"
        assert alert.severity == Severity.CRITICAL
        assert alert.environment == Environment.PRODUCTION

    def test_timestamp_defaults_to_utc_now_when_omitted(self):
        payload = _valid_payload()
        del payload["timestamp"]
        alert = AlertPayload(**payload)
        assert alert.timestamp.tzinfo is not None

    def test_naive_timestamp_is_coerced_to_utc(self):
        naive_ts = datetime(2026, 10, 6, 12, 0, 0)  # no tzinfo
        alert = AlertPayload(**_valid_payload(timestamp=naive_ts))
        assert alert.timestamp.tzinfo == timezone.utc

    def test_alert_id_whitespace_is_stripped(self):
        alert = AlertPayload(**_valid_payload(alert_id="  my-alert  "))
        assert alert.alert_id == "my-alert"

    def test_service_name_whitespace_is_stripped(self):
        alert = AlertPayload(**_valid_payload(service_name="  svc  "))
        assert alert.service_name == "svc"

    def test_missing_required_field_raises_validation_error(self):
        payload = _valid_payload()
        del payload["service_name"]
        with pytest.raises(ValidationError) as exc_info:
            AlertPayload(**payload)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("service_name",) for e in errors)

    def test_invalid_severity_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(severity="unknown"))

    def test_invalid_environment_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(environment="space"))

    def test_empty_alert_id_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(alert_id=""))

    def test_empty_raw_log_stacktrace_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(raw_log_stacktrace=""))

    def test_extra_fields_are_forbidden(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(unexpected_field="oops"))

    # ── Cross-field rule: production must be high/critical ─────────────────

    def test_production_low_severity_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            AlertPayload(**_valid_payload(environment="production", severity="low"))
        assert "production" in str(exc_info.value).lower()

    def test_production_medium_severity_raises_validation_error(self):
        with pytest.raises(ValidationError):
            AlertPayload(**_valid_payload(environment="production", severity="medium"))

    def test_production_high_severity_is_valid(self):
        alert = AlertPayload(**_valid_payload(environment="production", severity="high"))
        assert alert.severity == Severity.HIGH

    def test_staging_low_severity_is_valid(self):
        alert = AlertPayload(**_valid_payload(environment="staging", severity="low"))
        assert alert.environment == Environment.STAGING

    def test_development_medium_severity_is_valid(self):
        alert = AlertPayload(**_valid_payload(environment="development", severity="medium"))
        assert alert.severity == Severity.MEDIUM

    # ── Response model ─────────────────────────────────────────────────────

    def test_alert_ingestion_response_defaults(self):
        resp = AlertIngestionResponse(tracking_id="abc-123")
        assert resp.status == "queued"
        assert resp.tracking_id == "abc-123"
        assert "accepted" in resp.message.lower()


# ===========================================================================
# 2.  POST /api/v1/alerts/ingest — HTTP layer
# ===========================================================================

class TestIngestEndpoint:

    # ── Happy path ─────────────────────────────────────────────────────────

    async def test_valid_alert_returns_202(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json=_valid_payload())
        assert response.status_code == 202

    async def test_response_contains_tracking_id(self, client_with_task_id):
        ac, _, task_id = client_with_task_id
        response = await ac.post(INGEST_URL, json=_valid_payload())
        body = response.json()
        assert body["tracking_id"] == task_id

    async def test_response_status_is_queued(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json=_valid_payload())
        assert response.json()["status"] == "queued"

    async def test_response_contains_message(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json=_valid_payload())
        assert "message" in response.json()

    async def test_celery_send_task_is_called_once(self, client):
        ac, mock_celery = client
        await ac.post(INGEST_URL, json=_valid_payload())
        mock_celery.send_task.assert_called_once()

    async def test_celery_send_task_receives_correct_task_name(self, client):
        ac, mock_celery = client
        await ac.post(INGEST_URL, json=_valid_payload())
        call_args = mock_celery.send_task.call_args
        assert call_args.args[0] == "src.worker.tasks.process_alert"

    async def test_celery_send_task_receives_alert_payload(self, client):
        ac, mock_celery = client
        payload = _valid_payload()
        await ac.post(INGEST_URL, json=payload)
        call_kwargs = mock_celery.send_task.call_args
        # args[0] = task name, args[1] = positional args list
        sent_alert = call_kwargs.kwargs["args"][0]
        assert sent_alert["alert_id"] == payload["alert_id"]
        assert sent_alert["service_name"] == payload["service_name"]

    async def test_celery_send_task_uses_alerts_queue(self, client):
        ac, mock_celery = client
        await ac.post(INGEST_URL, json=_valid_payload())
        call_kwargs = mock_celery.send_task.call_args.kwargs
        assert call_kwargs["queue"] == "alerts"

    async def test_tracking_id_is_a_valid_uuid(self, client_with_task_id):
        ac, _, task_id = client_with_task_id
        response = await ac.post(INGEST_URL, json=_valid_payload())
        returned_id = response.json()["tracking_id"]
        # Should not raise
        uuid.UUID(returned_id)

    async def test_timestamp_omitted_still_accepted(self, client):
        """Timestamp has a default; omitting it should still yield 202."""
        ac, _ = client
        payload = _valid_payload()
        del payload["timestamp"]
        response = await ac.post(INGEST_URL, json=payload)
        assert response.status_code == 202

    # ── Validation failures → 422 ──────────────────────────────────────────

    async def test_missing_alert_id_returns_422(self, client):
        ac, _ = client
        payload = _valid_payload()
        del payload["alert_id"]
        response = await ac.post(INGEST_URL, json=payload)
        assert response.status_code == 422

    async def test_missing_service_name_returns_422(self, client):
        ac, _ = client
        payload = _valid_payload()
        del payload["service_name"]
        response = await ac.post(INGEST_URL, json=payload)
        assert response.status_code == 422

    async def test_invalid_severity_returns_422(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json=_valid_payload(severity="nuclear"))
        assert response.status_code == 422

    async def test_invalid_environment_returns_422(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json=_valid_payload(environment="moon"))
        assert response.status_code == 422

    async def test_production_low_severity_returns_422(self, client):
        ac, _ = client
        response = await ac.post(
            INGEST_URL,
            json=_valid_payload(environment="production", severity="low"),
        )
        assert response.status_code == 422

    async def test_empty_body_returns_422(self, client):
        ac, _ = client
        response = await ac.post(INGEST_URL, json={})
        assert response.status_code == 422

    async def test_422_response_contains_errors_list(self, client):
        ac, _ = client
        payload = _valid_payload()
        del payload["alert_id"]
        response = await ac.post(INGEST_URL, json=payload)
        body = response.json()
        assert "errors" in body
        assert isinstance(body["errors"], list)
        assert len(body["errors"]) > 0

    async def test_422_error_entry_has_expected_keys(self, client):
        ac, _ = client
        payload = _valid_payload()
        del payload["alert_id"]
        response = await ac.post(INGEST_URL, json=payload)
        first_error = response.json()["errors"][0]
        assert "field" in first_error
        assert "message" in first_error
        assert "type" in first_error

    # ── Enqueue failure → 500 ─────────────────────────────────────────────

    async def test_celery_enqueue_failure_returns_500(self):
        """When Celery.send_task raises, the route must return 500."""
        get_settings.cache_clear()
        app = create_app()

        broken_celery = MagicMock()
        broken_celery.send_task.side_effect = ConnectionError("Redis unreachable")
        app.dependency_overrides[get_celery_app] = lambda: broken_celery

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
            response = await ac.post(INGEST_URL, json=_valid_payload())

        assert response.status_code == 500
        assert "retry" in response.json()["detail"].lower()

    async def test_500_does_not_expose_internal_exception_details(self):
        """The 500 body must not leak internal stack traces."""
        get_settings.cache_clear()
        app = create_app()

        broken_celery = MagicMock()
        broken_celery.send_task.side_effect = RuntimeError("secret internal error message")
        app.dependency_overrides[get_celery_app] = lambda: broken_celery

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
            response = await ac.post(INGEST_URL, json=_valid_payload())

        assert "secret internal error message" not in response.text

    # ── Content-type ──────────────────────────────────────────────────────

    async def test_non_json_content_type_returns_422(self, client):
        ac, _ = client
        response = await ac.post(
            INGEST_URL,
            content="alert_id=x",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        assert response.status_code == 422

    # ── Various severity / environment combos ─────────────────────────────

    @pytest.mark.parametrize(
        "severity,environment",
        [
            ("low", "development"),
            ("medium", "development"),
            ("high", "development"),
            ("critical", "development"),
            ("low", "staging"),
            ("medium", "staging"),
            ("high", "staging"),
            ("critical", "staging"),
            ("high", "production"),
            ("critical", "production"),
        ],
    )
    async def test_valid_severity_environment_combinations(
        self, client, severity: str, environment: str
    ):
        ac, _ = client
        response = await ac.post(
            INGEST_URL,
            json=_valid_payload(severity=severity, environment=environment),
        )
        assert response.status_code == 202, (
            f"Expected 202 for severity={severity}, environment={environment}. "
            f"Got {response.status_code}: {response.text}"
        )


# ===========================================================================
# 3.  GET /healthz
# ===========================================================================

class TestHealthCheck:

    async def test_healthz_returns_200(self, client):
        ac, _ = client
        response = await ac.get("/healthz")
        assert response.status_code == 200

    async def test_healthz_returns_ok_status(self, client):
        ac, _ = client
        response = await ac.get("/healthz")
        assert response.json()["status"] == "ok"

    async def test_healthz_returns_service_name(self, client):
        ac, _ = client
        response = await ac.get("/healthz")
        assert "service" in response.json()


# ===========================================================================
# 4.  OpenAPI / docs availability
# ===========================================================================

class TestOpenAPI:

    async def test_openapi_schema_is_accessible(self, client):
        ac, _ = client
        response = await ac.get("/openapi.json")
        assert response.status_code == 200

    async def test_openapi_schema_contains_ingest_path(self, client):
        ac, _ = client
        schema = (await ac.get("/openapi.json")).json()
        assert INGEST_URL in schema["paths"]

    async def test_docs_ui_is_accessible(self, client):
        ac, _ = client
        response = await ac.get("/docs")
        assert response.status_code == 200
