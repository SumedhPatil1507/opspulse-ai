"""
Explainable remediation report builder.

Transforms a finished (or in-flight) LangGraph ``IncidentState`` into the
standardized ``RemediationReport`` contract returned by every triage API:

    verdict → reasoning → recommendation → next_steps

Design decisions
----------------
* **Deterministic by construction** — the builder never calls an LLM, so
  API responses are stable and testable even when no LLM key is configured.
  When the planner produced rich content (``analysis_summary``,
  ``proposed_action``) it is woven in; otherwise safe fallbacks derived
  from the parsed exception signature are used.
* **Explainability first** — ``reasoning`` always names the matched stack
  lines and the runbook similarity scores that justified the verdict.
* **Risk-rating is mandatory** — ``recommendation`` is prefixed with the
  planner's risk tier (``[HIGH]``, ``[CRITICAL]`` …) so operators can triage
  the queue by blast radius at a glance.
"""

from __future__ import annotations

import re
from typing import Any

from src.api.models.remediation import RemediationReport

# Exception signature → (verdict noun, diagnosis hint, prevention hint)
_SIGNATURE_HINTS: list[tuple[re.Pattern[str], tuple[str, str, str]]] = [
    (
        re.compile(r"OOMKilled|OutOfMemoryError|Java heap space|OOM", re.I),
        (
            "OOMKILLED CASCADE",
            "Review active memory usage on Prometheus Grafana dashboard",
            "Update application memory limit in Helm values repository",
        ),
    ),
    (
        re.compile(r"CrashLoopBackOff|CrashLoop", re.I),
        (
            "CRASHLOOPBACKOFF",
            "Inspect container exit codes on the Kubernetes Pods dashboard",
            "Fix the failing startup command or missing Secret in Helm values",
        ),
    ),
    (
        re.compile(r"CoreDNS|coredns|DNS.*timeout|dial tcp.*53", re.I),
        (
            "COREDNS TIMEOUT",
            "Check CoreDNS latency panels on the cluster DNS dashboard",
            "Raise CoreDNS replica count and review node-local DNS cache config",
        ),
    ),
    (
        re.compile(r"ImagePullBackOff|ErrImagePull|failed to pull image", re.I),
        (
            "IMAGE PULL FAILURE",
            "Verify registry credentials and image tags in the CI/CD dashboard",
            "Pin a valid image tag and rotate the imagePullSecret",
        ),
    ),
    (
        re.compile(r"connection refused|ECONNREFUSED", re.I),
        (
            "CONNECTION REFUSED",
            "Review dependency health on the service topology dashboard",
            "Add readiness probes and connection pool limits",
        ),
    ),
    (
        re.compile(r"5xx|Internal Server Error|status.?50[0-9]", re.I),
        (
            "5XX SPIKE",
            "Review request error rates on the API gateway dashboard",
            "Add circuit breakers and retry budgets to the gateway config",
        ),
    ),
]

_DEFAULT_DIAGNOSIS = (
    "Inspect the service logs and golden-signals on the Grafana dashboard"
)
_DEFAULT_PREVENTION = (
    "Capture the failure mode in the service runbook and tighten SLO alerts"
)


def _classify_signature(raw_log: str, primary_exception: str) -> tuple[str, str, str]:
    """Map stack trace + primary exception to (verdict, diagnosis, prevention)."""
    haystack = f"{primary_exception}\n{raw_log}"
    for pattern, hints in _SIGNATURE_HINTS:
        if pattern.search(haystack):
            return hints
    return (
        primary_exception.upper().replace("ERROR", " ERROR").strip() or "INCIDENT",
        _DEFAULT_DIAGNOSIS,
        _DEFAULT_PREVENTION,
    )


def _detect_cascade(raw_log: str, pod_mentions: int) -> bool:
    """A cascade = the same fatal signature hit multiple pods/nodes."""
    oom_hits = len(re.findall(r"OOMKilled|OutOfMemoryError", raw_log, re.I))
    crash_hits = len(re.findall(r"CrashLoopBackOff", raw_log, re.I))
    return pod_mentions >= 3 or oom_hits >= 3 or crash_hits >= 3


def _format_runbook_scores(retrieved: list[dict[str, Any]], limit: int = 2) -> str:
    """Render runbook similarity scores for the reasoning field."""
    if not retrieved:
        return "no matching runbook found (vector index returned 0 candidates)"
    parts = []
    for rb in retrieved[:limit]:
        source = rb.get("source_file", "unknown")
        rerank = rb.get("rerank_score", 0.0) or 0.0
        dense = rb.get("dense_score", 0.0) or 0.0
        rrf = rb.get("rrf_score", 0.0) or 0.0
        parts.append(
            f"runbook '{source}' (rerank_score={rerank:.3f}, "
            f"dense_score={dense:.3f}, rrf_score={rrf:.4f})"
        )
    return "; ".join(parts)


def _match_stack_lines(raw_log: str, limit: int = 2) -> str:
    """Extract the most informative stack-trace lines for the reasoning field."""
    lines = [
        ln.strip()
        for ln in raw_log.splitlines()
        if re.search(r"Error|Exception|OOMKilled|CrashLoop|Caused by|at .*\(", ln, re.I)
    ]
    if not lines:
        trimmed = raw_log.strip().splitlines()
        lines = [ln.strip() for ln in trimmed[:1]]
    return " | ".join(lines[:limit])[:400] or "n/a"


def build_remediation_report(state: dict[str, Any]) -> RemediationReport:
    """
    Build the standardized explainable remediation JSON from an
    ``IncidentState`` dict (``run_workflow`` return value).

    Returns
    -------
    RemediationReport with exactly ``verdict``, ``reasoning``,
    ``recommendation``, and ``next_steps``.
    """
    alert: dict[str, Any] = state.get("alert_data") or {}
    parsed: dict[str, Any] = state.get("parsed_log") or {}
    retrieved: list[dict[str, Any]] = list(state.get("retrieved_runbooks") or [])
    action: dict[str, Any] = state.get("proposed_action") or {}
    analysis_summary: str = state.get("analysis_summary") or ""

    severity = str(alert.get("severity", "high")).upper()
    service = alert.get("service_name") or "unknown-service"
    raw_log = str(alert.get("raw_log_stacktrace", ""))
    primary_exc = str(parsed.get("primary_exception") or "UnknownError")

    signature, diagnosis_step, prevention_step = _classify_signature(raw_log, primary_exc)

    pod_mentions = len(
        set(re.findall(r"\b[\w-]+-[a-z0-9]{5,10}-[a-z0-9]{4,6}\b", raw_log))
    )
    cascade = _detect_cascade(raw_log, pod_mentions)

    # ── Verdict ───────────────────────────────────────────────────────────
    verdict = re.sub(
        r"\s+",
        " ",
        f"{severity} {signature} {'CASCADE DETECTED' if cascade else 'DETECTED'}",
    ).strip()

    # ── Reasoning ─────────────────────────────────────────────────────────
    stack_lines = _match_stack_lines(raw_log)
    runbook_scores = _format_runbook_scores(retrieved)
    reasoning = (
        f"Primary exception '{primary_exc}' on service '{service}'. "
        f"Matched stack trace line(s): {stack_lines}. "
        f"Runbook similarity: {runbook_scores}. "
    )
    if analysis_summary:
        reasoning += f"Agent analysis: {analysis_summary}"
    else:
        reasoning += (
            f"{signature} signature matched; "
            f"{pod_mentions or 1} distinct pod(s) implicated in the raw log."
        )

    # ── Recommendation (risk-rated) ───────────────────────────────────────
    risk_tier = str(action.get("risk_tier") or "HIGH").upper()
    title = str(action.get("title") or action.get("tool_name") or "").strip()
    if title:
        recommendation = f"[{risk_tier}] {title}"
    else:
        tool = str(action.get("tool_name") or "")
        params = action.get("tool_parameters") or {}
        if "scale" in tool and "replicas" in params:
            recommendation = (
                f"[{risk_tier}] Scale Deployment Replicas to {params['replicas']}"
            )
        elif "restart" in tool:
            recommendation = (
                f"[{risk_tier}] Rolling restart of the affected deployment"
            )
        else:
            recommendation = (
                f"[{risk_tier}] Apply runbook remediation for {signature.title()}"
            )

    # ── Next steps ────────────────────────────────────────────────────────
    tool_name = str(action.get("tool_name") or "")
    if "scale" in tool_name:
        execution_step = (
            "Execute kubectl scale command or click Approve to trigger "
            "automated replica scaling"
        )
    elif "rollback" in tool_name:
        execution_step = (
            "Execute kubectl rollout undo / Helm rollback or click Approve "
            "to trigger automated rollback"
        )
    else:
        execution_step = (
            "Execute kubectl command or click Approve to trigger automated "
            "K8s restart"
        )

    next_steps = [
        f"Step 1: {diagnosis_step}",
        f"Step 2: {execution_step}",
        f"Step 3: {prevention_step}",
    ]

    return RemediationReport(
        verdict=verdict,
        reasoning=reasoning.strip(),
        recommendation=recommendation,
        next_steps=next_steps,
    )
