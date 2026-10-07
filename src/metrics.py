"""
Prometheus metrics registry for OpsPulse AI.

All metrics live in a single module so:
* There is exactly one ``CollectorRegistry`` — avoids duplicate-metric errors
  when modules are reloaded in tests.
* Metric names are centralised — no magic strings scattered across nodes.
* The FastAPI app mounts the ``/metrics`` endpoint by importing ``REGISTRY``
  and using ``prometheus_client.make_asgi_app()``.

Metric catalogue
----------------

agent_execution_latency_seconds   Histogram
    Wall-clock time for a complete LangGraph workflow run, from initial state
    construction to the final node completing.
    Labels: status (completed | failed | awaiting_human | rejected),
            environment (production | staging | development)

incidents_auto_resolved_total     Counter
    Incremented each time the workflow reaches ``auto_execute_node``
    successfully (LOW/MEDIUM risk, no HITL required).
    Labels: service_name, severity, environment

hitl_approvals_total              Counter
    Incremented when a human responds to a Slack HITL notification.
    Labels: decision (approved | rejected), risk_tier, service_name

sandbox_operations_total          Counter
    Tracks individual Docker sandbox operation calls.
    Labels: operation (inspect | logs | restart), status (success | error),
            container_name

sandbox_operation_latency_seconds Histogram
    Latency of Docker SDK calls inside the sandbox.
    Labels: operation (inspect | logs | restart)

runbook_retrievals_total          Counter
    Tracks hybrid retrieval calls.
    Labels: status (success | empty | error)

llm_calls_total                   Counter
    Tracks LLM invocations from PlanRemediationNode.
    Labels: provider (groq | anthropic | unknown), status (success | error)

llm_call_latency_seconds          Histogram
    LLM API round-trip latency.
    Labels: provider (groq | anthropic | unknown)

Usage
-----
    # Record a completed workflow run
    from src.metrics import record_workflow_run
    record_workflow_run(status="completed", environment="production", duration_seconds=4.2)

    # Increment HITL counter
    from src.metrics import HITL_APPROVALS
    HITL_APPROVALS.labels(decision="approved", risk_tier="HIGH", service_name="payment-service").inc()

    # Use the timing context manager
    from src.metrics import track_sandbox_operation
    with track_sandbox_operation("restart", "my-container"):
        container.restart()

    # Mount metrics endpoint in FastAPI app:
    from prometheus_client import make_asgi_app
    from src.metrics import REGISTRY
    app.mount("/metrics", make_asgi_app(registry=REGISTRY))
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Generator

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from src.api.core.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Single shared registry — import this in the FastAPI app to mount /metrics
# ---------------------------------------------------------------------------

REGISTRY = CollectorRegistry(auto_describe=True)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ns(name: str) -> str:
    """Prefix a metric name with the configured namespace."""
    cfg = get_settings()
    return f"{cfg.METRICS_NAMESPACE}_{name}" if cfg.METRICS_NAMESPACE else name


# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

#: Latency buckets suited for AI workflow durations (sub-second to minutes)
_WORKFLOW_LATENCY_BUCKETS = (
    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0
)

#: Latency buckets for Docker SDK calls (typically sub-second)
_SANDBOX_LATENCY_BUCKETS = (
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0
)

#: Latency buckets for LLM API round-trips
_LLM_LATENCY_BUCKETS = (
    0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0
)

AGENT_EXECUTION_LATENCY = Histogram(
    name=_ns("agent_execution_latency_seconds"),
    documentation=(
        "End-to-end wall-clock time for a complete LangGraph incident "
        "remediation workflow run, from initial state to terminal node."
    ),
    labelnames=["status", "environment"],
    buckets=_WORKFLOW_LATENCY_BUCKETS,
    registry=REGISTRY,
)

SANDBOX_OPERATION_LATENCY = Histogram(
    name=_ns("sandbox_operation_latency_seconds"),
    documentation=(
        "Latency of individual Docker sandbox operations "
        "(inspect, logs, restart)."
    ),
    labelnames=["operation"],
    buckets=_SANDBOX_LATENCY_BUCKETS,
    registry=REGISTRY,
)

LLM_CALL_LATENCY = Histogram(
    name=_ns("llm_call_latency_seconds"),
    documentation="LLM API round-trip latency from PlanRemediationNode.",
    labelnames=["provider"],
    buckets=_LLM_LATENCY_BUCKETS,
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

INCIDENTS_AUTO_RESOLVED = Counter(
    name=_ns("incidents_auto_resolved_total"),
    documentation=(
        "Total incidents resolved automatically without human intervention "
        "(LOW/MEDIUM risk tier, auto_execute path)."
    ),
    labelnames=["service_name", "severity", "environment"],
    registry=REGISTRY,
)

HITL_APPROVALS = Counter(
    name=_ns("hitl_approvals_total"),
    documentation=(
        "Total human-in-the-loop decisions received via Slack. "
        "Tracks both approvals and rejections."
    ),
    labelnames=["decision", "risk_tier", "service_name"],
    registry=REGISTRY,
)

SANDBOX_OPERATIONS = Counter(
    name=_ns("sandbox_operations_total"),
    documentation=(
        "Total Docker sandbox operation invocations, labelled by operation "
        "type and outcome."
    ),
    labelnames=["operation", "status", "container_name"],
    registry=REGISTRY,
)

RUNBOOK_RETRIEVALS = Counter(
    name=_ns("runbook_retrievals_total"),
    documentation=(
        "Total hybrid runbook retrieval attempts from RetrieveRunbookNode."
    ),
    labelnames=["status"],
    registry=REGISTRY,
)

LLM_CALLS = Counter(
    name=_ns("llm_calls_total"),
    documentation="Total LLM API calls from PlanRemediationNode.",
    labelnames=["provider", "status"],
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# High-level recording helpers
# (preferred over calling label/inc directly — keeps metric logic out of nodes)
# ---------------------------------------------------------------------------

def record_workflow_run(
    status: str,
    environment: str,
    duration_seconds: float,
) -> None:
    """
    Record the outcome and latency of a completed LangGraph workflow run.

    Parameters
    ----------
    status          : Final ``WorkflowStatus`` value as a string
                      (completed | failed | awaiting_human | rejected).
    environment     : Alert environment (production | staging | development).
    duration_seconds: Total wall-clock seconds from invoke() to return.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    AGENT_EXECUTION_LATENCY.labels(
        status=status.lower(),
        environment=environment.lower(),
    ).observe(duration_seconds)
    logger.debug(
        "metrics: workflow_run status=%s env=%s duration=%.3fs",
        status, environment, duration_seconds,
    )


def record_auto_resolution(
    service_name: str,
    severity: str,
    environment: str,
) -> None:
    """
    Increment the auto-resolved counter after a successful LOW/MEDIUM execution.

    Parameters
    ----------
    service_name : Alert service_name label value.
    severity     : Alert severity (low | medium | high | critical).
    environment  : Deployment environment.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    INCIDENTS_AUTO_RESOLVED.labels(
        service_name=service_name,
        severity=severity.lower(),
        environment=environment.lower(),
    ).inc()
    logger.debug(
        "metrics: auto_resolved service=%s severity=%s env=%s",
        service_name, severity, environment,
    )


def record_hitl_decision(
    decision: str,
    risk_tier: str,
    service_name: str,
) -> None:
    """
    Record a human approval or rejection from the Slack HITL flow.

    Parameters
    ----------
    decision     : 'approved' or 'rejected'.
    risk_tier    : Risk tier of the action (HIGH | CRITICAL).
    service_name : Service the alert relates to.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    HITL_APPROVALS.labels(
        decision=decision.lower(),
        risk_tier=risk_tier.upper(),
        service_name=service_name,
    ).inc()
    logger.debug(
        "metrics: hitl_decision=%s risk=%s service=%s",
        decision, risk_tier, service_name,
    )


def record_sandbox_operation(
    operation: str,
    container_name: str,
    success: bool,
    duration_seconds: float,
) -> None:
    """
    Record a completed sandbox Docker operation.

    Parameters
    ----------
    operation        : 'inspect', 'logs', or 'restart'.
    container_name   : Target container name (used as label).
    success          : True if the operation succeeded.
    duration_seconds : Wall-clock seconds the operation took.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    status = "success" if success else "error"
    SANDBOX_OPERATIONS.labels(
        operation=operation,
        status=status,
        container_name=container_name,
    ).inc()
    SANDBOX_OPERATION_LATENCY.labels(operation=operation).observe(duration_seconds)
    logger.debug(
        "metrics: sandbox op=%s container=%s status=%s duration=%.3fs",
        operation, container_name, status, duration_seconds,
    )


def record_llm_call(
    provider: str,
    success: bool,
    duration_seconds: float,
) -> None:
    """
    Record an LLM API call from PlanRemediationNode.

    Parameters
    ----------
    provider         : 'groq', 'anthropic', or 'unknown'.
    success          : True if the call completed without error.
    duration_seconds : Round-trip latency in seconds.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    status = "success" if success else "error"
    LLM_CALLS.labels(provider=provider.lower(), status=status).inc()
    LLM_CALL_LATENCY.labels(provider=provider.lower()).observe(duration_seconds)
    logger.debug(
        "metrics: llm_call provider=%s status=%s duration=%.3fs",
        provider, status, duration_seconds,
    )


def record_runbook_retrieval(status: str) -> None:
    """
    Record the outcome of a runbook retrieval attempt.

    Parameters
    ----------
    status : 'success' (chunks found), 'empty' (zero results), or 'error'.
    """
    cfg = get_settings()
    if not cfg.METRICS_ENABLED:
        return
    RUNBOOK_RETRIEVALS.labels(status=status.lower()).inc()


# ---------------------------------------------------------------------------
# Context managers
# ---------------------------------------------------------------------------

@contextmanager
def track_workflow_run(
    environment: str,
) -> Generator[dict, None, None]:
    """
    Context manager that times a complete workflow run and records the result.

    Usage::

        with track_workflow_run("production") as ctx:
            result = app.invoke(state)
            ctx["status"] = result.get("workflow_status", "unknown")

    The caller sets ``ctx["status"]`` inside the ``with`` block.  If an
    unhandled exception propagates, status is automatically set to ``"failed"``.
    """
    ctx: dict = {"status": "unknown"}
    t_start = time.monotonic()
    try:
        yield ctx
    except Exception:
        ctx["status"] = "failed"
        raise
    finally:
        duration = time.monotonic() - t_start
        record_workflow_run(
            status=ctx.get("status", "unknown"),
            environment=environment,
            duration_seconds=duration,
        )


@contextmanager
def track_sandbox_operation(
    operation: str,
    container_name: str,
) -> Generator[None, None, None]:
    """
    Context manager that times a Docker sandbox operation and records it.

    Usage::

        with track_sandbox_operation("restart", "payment-service"):
            container.restart(timeout=10)
    """
    t_start = time.monotonic()
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        duration = time.monotonic() - t_start
        record_sandbox_operation(
            operation=operation,
            container_name=container_name,
            success=success,
            duration_seconds=duration,
        )


@contextmanager
def track_llm_call(provider: str) -> Generator[None, None, None]:
    """
    Context manager that times an LLM API call and records it.

    Usage::

        with track_llm_call("groq"):
            response = llm.invoke(messages)
    """
    t_start = time.monotonic()
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        duration = time.monotonic() - t_start
        record_llm_call(
            provider=provider,
            success=success,
            duration_seconds=duration,
        )


# ---------------------------------------------------------------------------
# ASGI app factory (mount in FastAPI)
# ---------------------------------------------------------------------------

def get_metrics_response() -> tuple[bytes, str]:
    """
    Return raw Prometheus exposition bytes and content-type string.

    Use when you cannot mount an ASGI sub-app (e.g. inside a plain HTTP
    handler or a health-check endpoint).

        data, content_type = get_metrics_response()
        return Response(content=data, media_type=content_type)
    """
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def make_metrics_app():
    """
    Return a Prometheus ASGI app pre-configured with the project registry.

    Mount in FastAPI::

        from prometheus_client import make_asgi_app
        metrics_app = make_metrics_app()
        app.mount("/metrics", metrics_app)
    """
    from prometheus_client import make_asgi_app  # lazy — avoids import cost at module level
    return make_asgi_app(registry=REGISTRY)
