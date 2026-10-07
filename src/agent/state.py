"""
State schema and supporting types for the incident remediation state machine.

Design principles
-----------------
* ``IncidentState`` is a LangGraph ``TypedDict`` — the single source of truth
  threaded through every node.  Nodes read from it and return partial updates;
  LangGraph merges those updates via the ``Annotated`` reducer pattern.
* All mutable collections (lists, dicts) use ``operator.add`` as the reducer
  so multiple nodes can append to them without overwriting each other.
* Immutable scalar fields (strings, enums, datetimes) use ``_last_value``
  reducer — the last writer wins, which is the correct semantic for status
  fields that a single node owns exclusively.
* Strict ``Enum`` types prevent magic-string bugs propagating through routing
  logic.  Every enum value has a clear docstring explaining when it is set.
* All ``TypedDict`` fields have ``total=False`` defaults where appropriate so
  the graph can be invoked with just the minimum ``alert_data`` entry-point
  and every other field hydrates as nodes run.

LangGraph state flow
--------------------

  ┌─────────────────────────────────────────────────────────────┐
  │                      IncidentState                          │
  │                                                             │
  │  alert_data ──► ParseLogNode ──► parsed_log                 │
  │                      │                                      │
  │                      ▼                                      │
  │              RetrieveRunbookNode ──► retrieved_runbooks      │
  │                      │                                      │
  │                      ▼                                      │
  │              PlanRemediationNode ──► proposed_action        │
  │                                     analysis_summary        │
  │                      │                                      │
  │                      ▼                                      │
  │               HITLCheckNode ──► approval_status             │
  │                                 slack_notification_sent     │
  │                      │                                      │
  │              [END or AWAITING_APPROVAL]                     │
  └─────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import operator
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any

from langgraph.graph import MessagesState
from typing_extensions import TypedDict


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class RiskTier(str, Enum):
    """
    Risk classification for a proposed remediation action.

    Assigned by ``PlanRemediationNode`` based on the LLM's assessment of
    the action's potential blast radius.
    """

    LOW = "LOW"
    """Safe to auto-execute. E.g., log rotation, cache invalidation."""

    MEDIUM = "MEDIUM"
    """Execute with audit log. E.g., service restart of a single replica."""

    HIGH = "HIGH"
    """Requires human approval via Slack HITL. E.g., container restart,
    connection pool flush, rolling deployment."""

    CRITICAL = "CRITICAL"
    """Requires human approval + incident commander sign-off.
    E.g., full DB flush, cluster drain, external API key rotation."""

    UNKNOWN = "UNKNOWN"
    """Default when the LLM cannot assess risk — treated as HIGH."""


class ApprovalStatus(str, Enum):
    """
    Tracks the human-in-the-loop approval lifecycle for a proposed action.
    """

    NOT_REQUIRED = "NOT_REQUIRED"
    """Risk tier is LOW or MEDIUM — auto-execution permitted."""

    PENDING = "PENDING"
    """Slack notification sent; waiting for human response."""

    APPROVED = "APPROVED"
    """Human approved the action via Slack interactive button or reply."""

    REJECTED = "REJECTED"
    """Human rejected or timed-out. Workflow halts without execution."""

    SKIPPED = "SKIPPED"
    """HITL node skipped because Slack webhook is not configured."""


class NodeName(str, Enum):
    """Canonical identifiers for every node in the state graph."""

    PARSE_LOG = "parse_log"
    RETRIEVE_RUNBOOK = "retrieve_runbook"
    PLAN_REMEDIATION = "plan_remediation"
    HITL_CHECK = "hitl_check"
    AUTO_EXECUTE = "auto_execute"
    AWAIT_APPROVAL = "await_approval"
    TERMINAL = "__end__"


class WorkflowStatus(str, Enum):
    """Overall run status — set by nodes, readable by callers."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


# ---------------------------------------------------------------------------
# Structured sub-objects (stored inside IncidentState)
# ---------------------------------------------------------------------------

class ExceptionSignature(TypedDict, total=False):
    """
    A single extracted exception from a raw log / stacktrace.

    Populated by ``ParseLogNode``.
    """

    exception_type: str
    """Fully-qualified exception class, e.g. 'sqlalchemy.exc.TimeoutError'."""

    exception_message: str
    """The exception message text stripped of dynamic values."""

    affected_file: str
    """Last meaningful file path in the traceback (not stdlib internals)."""

    affected_line: int | None
    """Line number at the point of failure, if parseable."""

    raw_frame: str
    """The raw last-frame text for reference."""


class ParsedLog(TypedDict, total=False):
    """
    Structured extraction produced by ``ParseLogNode``.
    """

    exception_signatures: list[ExceptionSignature]
    """All distinct exceptions found in the stacktrace."""

    primary_exception: str
    """The outermost / most specific exception type (used as BM25 query seed)."""

    error_keywords: list[str]
    """High-signal keywords for retrieval (e.g. ['connection pool', 'timeout'])."""

    service_context: str
    """Service name from alert_data.service_name, echoed here for convenience."""

    raw_log_excerpt: str
    """First 1 000 characters of the original stacktrace for LLM context."""

    parse_error: str | None
    """Set when log parsing partially or fully failed; nodes downstream must
    handle a degraded ParsedLog gracefully."""


class RetrievedRunbook(TypedDict, total=False):
    """
    A single runbook chunk returned by the hybrid retriever.
    Mirrors ``RetrievalResult.to_dict()`` from ``src.rag.retriever``.
    """

    rank: int
    source_file: str
    section_path: str
    dense_score: float
    sparse_score: float
    rrf_score: float
    rerank_score: float
    text_preview: str
    chunk_id: str


class ProposedAction(TypedDict, total=False):
    """
    Structured remediation plan produced by ``PlanRemediationNode``.

    The LLM is instructed to return a JSON object matching this schema.
    All fields are ``total=False`` so partial LLM outputs degrade gracefully.
    """

    action_id: str
    """UUID assigned at planning time for correlation in downstream systems."""

    title: str
    """One-line human-readable action title."""

    description: str
    """Full description of what will be done and why."""

    risk_tier: str
    """One of RiskTier enum values as a string."""

    tool_name: str
    """Name of the tool / runbook procedure to invoke (e.g. 'restart_container')."""

    tool_parameters: dict[str, Any]
    """JSON-serialisable parameters for the tool invocation."""

    estimated_impact: str
    """Plain-English description of potential blast radius."""

    rollback_procedure: str
    """How to undo this action if it makes things worse."""

    confidence_score: float
    """LLM self-assessed confidence 0.0–1.0."""

    reasoning: str
    """LLM chain-of-thought reasoning (kept for audit trail)."""

    llm_model_used: str
    """Which LLM model produced this plan (Groq or Claude)."""

    planned_at: str
    """ISO-8601 UTC timestamp when the plan was generated."""


class TraceEvent(TypedDict):
    """
    A single structured trace log entry appended by each node.

    Stored in ``IncidentState.trace_log`` as an append-only audit trail.
    """

    timestamp: str        # ISO-8601 UTC
    node: str             # NodeName value
    event: str            # e.g. "node_start", "node_end", "error"
    message: str          # Human-readable description
    duration_ms: float    # Wall-clock time for this event (0 for start events)
    metadata: dict[str, Any]  # Arbitrary node-specific context


# ---------------------------------------------------------------------------
# Primary state TypedDict
# ---------------------------------------------------------------------------

def _last_value(left: Any, right: Any) -> Any:
    """
    Reducer that returns the most recent (right) value.

    Used for scalar fields that are owned by a single node — the node writes
    once and subsequent reads always see the latest value.
    """
    return right if right is not None else left


class IncidentState(TypedDict, total=False):
    """
    The shared mutable state threaded through every node of the graph.

    Reducer annotations
    -------------------
    * ``Annotated[list, operator.add]``  — append-only; never overwrites.
    * ``Annotated[X, _last_value]``      — scalar; last writer wins.
    * Plain type (no annotation)         — default LangGraph last-write-wins.

    Mandatory fields (must be set before ``invoke()``)
    --------------------------------------------------
    * ``alert_data`` — the ingested ``AlertPayload`` dict from the API layer.

    All other fields are ``total=False`` (optional at construction time) and
    are populated progressively as nodes execute.
    """

    # ── Input ─────────────────────────────────────────────────────────────
    alert_data: dict[str, Any]
    """
    Raw alert payload dict from ``AlertPayload.model_dump(mode='json')``.
    Required to start the workflow.
    Keys: alert_id, service_name, severity, raw_log_stacktrace,
          timestamp, environment.
    """

    # ── ParseLogNode output ───────────────────────────────────────────────
    parsed_log: Annotated[ParsedLog | None, _last_value]
    """Structured extraction from ``ParseLogNode``. None until node runs."""

    # ── RetrieveRunbookNode output ────────────────────────────────────────
    retrieved_runbooks: Annotated[list[RetrievedRunbook], operator.add]
    """
    Accumulates runbook chunks from the hybrid retriever.
    Append-only so future multi-query expansion nodes can add more chunks.
    """

    # ── PlanRemediationNode output ────────────────────────────────────────
    analysis_summary: Annotated[str | None, _last_value]
    """
    Concise LLM-generated summary of root cause and recommended approach.
    Plain text, intended for display in Slack / incident tickets.
    """

    proposed_action: Annotated[ProposedAction | None, _last_value]
    """
    Structured remediation plan (JSON) produced by the LLM.
    None until ``PlanRemediationNode`` completes successfully.
    """

    # ── HITLCheckNode output ──────────────────────────────────────────────
    approval_status: Annotated[ApprovalStatus | None, _last_value]
    """
    Current HITL approval state.  Drives the conditional edge out of
    ``HITLCheckNode``.
    """

    slack_notification_sent: Annotated[bool | None, _last_value]
    """True if a Slack message was successfully posted for this run."""

    slack_message_ts: Annotated[str | None, _last_value]
    """Slack message timestamp for threading follow-up replies."""

    # ── Workflow-level metadata ────────────────────────────────────────────
    workflow_status: Annotated[WorkflowStatus | None, _last_value]
    """Overall run status, updated by each node."""

    error_message: Annotated[str | None, _last_value]
    """
    Last error message from any node.  Set on failure; cleared on recovery.
    Downstream nodes check this to decide whether to abort or degrade.
    """

    retry_count: Annotated[int | None, _last_value]
    """Current retry attempt for the active node (0-based)."""

    # ── Append-only audit trail ───────────────────────────────────────────
    trace_log: Annotated[list[TraceEvent], operator.add]
    """
    Chronological list of structured trace events from all nodes.
    Append-only — never overwritten.  Used for debugging, observability,
    and feeding into a future LangSmith / OpenTelemetry exporter.
    """


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def make_trace_event(
    node: NodeName | str,
    event: str,
    message: str,
    duration_ms: float = 0.0,
    metadata: dict[str, Any] | None = None,
) -> TraceEvent:
    """
    Construct a ``TraceEvent`` dict with a UTC timestamp.

    Parameters
    ----------
    node        : The ``NodeName`` (or raw string) of the emitting node.
    event       : Short event type string, e.g. ``"node_start"``, ``"llm_call"``.
    message     : Human-readable description of what happened.
    duration_ms : Elapsed milliseconds for timed operations; 0 for start events.
    metadata    : Arbitrary extra context (model name, token count, etc.).

    Returns
    -------
    TraceEvent dict ready to append to ``state["trace_log"]``.
    """
    return TraceEvent(
        timestamp=datetime.now(tz=timezone.utc).isoformat(),
        node=str(node),
        event=event,
        message=message,
        duration_ms=duration_ms,
        metadata=metadata or {},
    )


def initial_state(alert_data: dict[str, Any]) -> IncidentState:
    """
    Build a minimal valid ``IncidentState`` from an alert payload dict.

    This is the recommended entry-point when invoking the graph:

        state  = initial_state(alert.model_dump(mode="json"))
        result = app.invoke(state)
    """
    return IncidentState(
        alert_data=alert_data,
        parsed_log=None,
        retrieved_runbooks=[],
        analysis_summary=None,
        proposed_action=None,
        approval_status=None,
        slack_notification_sent=None,
        slack_message_ts=None,
        workflow_status=WorkflowStatus.RUNNING,
        error_message=None,
        retry_count=0,
        trace_log=[
            make_trace_event(
                node="__init__",
                event="workflow_start",
                message=f"Workflow started for alert_id={alert_data.get('alert_id', 'unknown')}",
                metadata={
                    "service_name": alert_data.get("service_name"),
                    "severity": alert_data.get("severity"),
                    "environment": alert_data.get("environment"),
                },
            )
        ],
    )
