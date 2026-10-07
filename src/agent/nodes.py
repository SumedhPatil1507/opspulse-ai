"""
LangGraph node implementations for the incident remediation state machine.

Each node is a pure function:

    node(state: IncidentState) -> dict   # partial state update

LangGraph merges the returned dict into the running IncidentState using the
reducers defined in state.py.  Nodes must NEVER mutate the input state dict.

Node execution order
--------------------
ParseLogNode → RetrieveRunbookNode → PlanRemediationNode → HITLCheckNode

Error handling contract
-----------------------
Nodes catch all exceptions internally, write ``error_message`` and
``WorkflowStatus.FAILED`` into their return dict, and return rather than
raise.  This keeps the graph from crashing and gives the conditional router
in workflow.py a chance to route to an error terminal node.

Trace logging contract
----------------------
Every node emits at minimum:
  1. A ``node_start`` TraceEvent at entry.
  2. A ``node_end`` TraceEvent at exit (success or failure) with duration_ms.
  3. Intermediate ``TraceEvent`` objects for significant sub-operations
     (LLM call start, LLM call end with token counts, Qdrant query, etc.).
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from src.agent.state import (
    ApprovalStatus,
    ExceptionSignature,
    IncidentState,
    NodeName,
    ParsedLog,
    ProposedAction,
    RiskTier,
    RetrievedRunbook,
    WorkflowStatus,
    make_trace_event,
)
from src.api.core.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared LLM factory
# ---------------------------------------------------------------------------

def _get_llm():
    """
    Return a LangChain chat model instance, preferring Groq then Claude.

    Raises
    ------
    RuntimeError
        If neither GROQ_API_KEY nor ANTHROPIC_API_KEY is configured.
    """
    cfg = get_settings()

    if cfg.GROQ_API_KEY:
        from langchain_groq import ChatGroq
        logger.debug("LLM backend: Groq / %s", cfg.GROQ_MODEL)
        return ChatGroq(
            api_key=cfg.GROQ_API_KEY,
            model_name=cfg.GROQ_MODEL,
            temperature=cfg.LLM_TEMPERATURE,
            max_tokens=cfg.LLM_MAX_TOKENS,
            timeout=cfg.LLM_TIMEOUT,
        )

    if cfg.ANTHROPIC_API_KEY:
        from langchain_anthropic import ChatAnthropic
        logger.debug("LLM backend: Anthropic / %s", cfg.ANTHROPIC_MODEL)
        return ChatAnthropic(
            api_key=cfg.ANTHROPIC_API_KEY,
            model=cfg.ANTHROPIC_MODEL,
            temperature=cfg.LLM_TEMPERATURE,
            max_tokens=cfg.LLM_MAX_TOKENS,
            timeout=cfg.LLM_TIMEOUT,
        )

    raise RuntimeError(
        "No LLM API key configured. Set GROQ_API_KEY or ANTHROPIC_API_KEY "
        "in your environment or .env file."
    )


# ---------------------------------------------------------------------------
# Regex patterns used by ParseLogNode
# ---------------------------------------------------------------------------

# Matches Python-style exception lines: "ExceptionType: message"
_PYTHON_EXCEPTION_RE = re.compile(
    r"^(?P<exc_type>[\w.]+(?:Error|Exception|Warning|Fault|Timeout|Killed"
    r"|Refused|Exhausted|Overflow|Kill))"
    r"(?:\s*:\s*(?P<exc_msg>.+))?$",
    re.MULTILINE | re.IGNORECASE,
)

# Matches Java-style exception: "at com.example.Class.method(File.java:42)"
_JAVA_FRAME_RE = re.compile(
    r"^\s+at\s+(?P<class>[\w.$]+)\.(?P<method>\w+)"
    r"\((?P<file>[^:)]+)(?::(?P<line>\d+))?\)",
    re.MULTILINE,
)

# File + line from Python tracebacks: '  File "path/to/file.py", line 42'
_PYTHON_FILE_RE = re.compile(
    r'File "(?P<file>[^"]+)", line (?P<line>\d+)',
    re.MULTILINE,
)

# OOMKill signatures
_OOM_RE = re.compile(r"OOMKill|OutOfMemory|java\.lang\.OutOfMemoryError", re.IGNORECASE)

# Connection-related keywords for keyword extraction
_CONN_KW_RE = re.compile(
    r"\b(connection|pool|timeout|refused|exhausted|overflow|max_connections"
    r"|socket|redis|postgres|mysql|mongodb)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Node 1 — ParseLogNode
# ---------------------------------------------------------------------------

def parse_log_node(state: IncidentState) -> dict[str, Any]:
    """
    Extract structured exception signatures from the raw log stacktrace.

    Input  : ``state["alert_data"]["raw_log_stacktrace"]``
    Output : ``parsed_log`` (ParsedLog TypedDict)

    Steps
    -----
    1. Extract all exception type + message pairs via regex.
    2. Extract file/line references from Python and Java stack frames.
    3. Identify high-signal retrieval keywords.
    4. Degrade gracefully on unparseable logs — always return something.
    """
    t_start = time.monotonic()
    node = NodeName.PARSE_LOG
    traces: list = []

    traces.append(make_trace_event(
        node=node, event="node_start",
        message="ParseLogNode started",
        metadata={"alert_id": state.get("alert_data", {}).get("alert_id")},
    ))

    alert_data: dict[str, Any] = state.get("alert_data", {})
    raw_log: str = alert_data.get("raw_log_stacktrace", "")
    service_name: str = alert_data.get("service_name", "unknown")

    try:
        exception_signatures: list[ExceptionSignature] = []
        error_keywords: list[str] = []

        # ── Extract exception signatures ──────────────────────────────────
        exc_matches = list(_PYTHON_EXCEPTION_RE.finditer(raw_log))
        for match in exc_matches[:5]:  # cap at 5 to avoid runaway extraction
            exc_type = match.group("exc_type") or ""
            exc_msg = (match.group("exc_msg") or "").strip()[:300]

            sig = ExceptionSignature(
                exception_type=exc_type,
                exception_message=exc_msg,
                affected_file="",
                affected_line=None,
                raw_frame=match.group(0)[:200],
            )
            exception_signatures.append(sig)

        # ── Extract file/line info ─────────────────────────────────────────
        py_files = list(_PYTHON_FILE_RE.finditer(raw_log))
        if py_files and exception_signatures:
            last_frame = py_files[-1]
            exception_signatures[-1]["affected_file"] = last_frame.group("file")
            try:
                exception_signatures[-1]["affected_line"] = int(last_frame.group("line"))
            except (TypeError, ValueError):
                pass

        java_frames = list(_JAVA_FRAME_RE.finditer(raw_log))
        if java_frames and exception_signatures:
            last_java = java_frames[-1]
            exception_signatures[-1]["affected_file"] = (
                f"{last_java.group('class')}.{last_java.group('method')}"
            )
            try:
                line_str = last_java.group("line")
                if line_str:
                    exception_signatures[-1]["affected_line"] = int(line_str)
            except (TypeError, ValueError):
                pass

        # ── OOMKill special-case ──────────────────────────────────────────
        if _OOM_RE.search(raw_log) and not exception_signatures:
            exception_signatures.append(ExceptionSignature(
                exception_type="OOMKilled",
                exception_message="Container or JVM exceeded memory limit",
                affected_file="",
                affected_line=None,
                raw_frame="",
            ))

        # ── Primary exception (outermost) ──────────────────────────────────
        primary = (
            exception_signatures[0]["exception_type"]
            if exception_signatures
            else "UnknownError"
        )

        # ── Keyword extraction ─────────────────────────────────────────────
        kw_hits = _CONN_KW_RE.findall(raw_log)
        error_keywords = list(dict.fromkeys(kw.lower() for kw in kw_hits))[:10]

        # Add exception type words as keywords too
        for sig in exception_signatures:
            parts = re.sub(r"([A-Z])", r" \1", sig["exception_type"]).lower().split()
            error_keywords.extend(p for p in parts if len(p) > 3)
        error_keywords = list(dict.fromkeys(error_keywords))[:15]

        parsed: ParsedLog = ParsedLog(
            exception_signatures=exception_signatures,
            primary_exception=primary,
            error_keywords=error_keywords,
            service_context=service_name,
            raw_log_excerpt=raw_log[:1000],
            parse_error=None,
        )

        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message=f"Extracted {len(exception_signatures)} exception(s); "
                    f"primary='{primary}'; keywords={error_keywords[:5]}",
            duration_ms=duration_ms,
            metadata={
                "exception_count": len(exception_signatures),
                "primary_exception": primary,
                "keyword_count": len(error_keywords),
            },
        ))
        logger.info(
            "[%s] parsed alert_id=%s primary_exception=%s keywords=%s",
            node.value, alert_data.get("alert_id"), primary, error_keywords[:5],
        )

        return {
            "parsed_log": parsed,
            "workflow_status": WorkflowStatus.RUNNING,
            "trace_log": traces,
        }

    except Exception as exc:
        duration_ms = (time.monotonic() - t_start) * 1000
        err_msg = f"ParseLogNode failed: {exc}"
        logger.exception("[%s] %s", node.value, err_msg)
        traces.append(make_trace_event(
            node=node, event="error",
            message=err_msg, duration_ms=duration_ms,
            metadata={"exception": str(exc)},
        ))
        # Degrade: return minimal ParsedLog so downstream nodes can still run
        return {
            "parsed_log": ParsedLog(
                exception_signatures=[],
                primary_exception="UnknownError",
                error_keywords=[],
                service_context=service_name,
                raw_log_excerpt=raw_log[:500],
                parse_error=err_msg,
            ),
            "workflow_status": WorkflowStatus.RUNNING,  # don't abort yet
            "error_message": err_msg,
            "trace_log": traces,
        }


# ---------------------------------------------------------------------------
# Node 2 — RetrieveRunbookNode
# ---------------------------------------------------------------------------

def retrieve_runbook_node(state: IncidentState) -> dict[str, Any]:
    """
    Run the hybrid Qdrant + BM25 retriever to fetch relevant runbook chunks.

    Input  : ``state["parsed_log"]`` for query construction
             (falls back to raw stacktrace if parsed_log is missing/degraded)
    Output : ``retrieved_runbooks`` (list of RetrievedRunbook dicts)

    Query construction
    ------------------
    The retrieval query combines:
    * Primary exception type (most discriminative signal)
    * Top error keywords
    * Service name
    * First 500 chars of the raw log excerpt
    """
    t_start = time.monotonic()
    node = NodeName.RETRIEVE_RUNBOOK
    traces: list = []

    traces.append(make_trace_event(
        node=node, event="node_start",
        message="RetrieveRunbookNode started",
    ))

    try:
        from src.rag.retriever import HybridSearchRetriever  # lazy import

        parsed: ParsedLog | None = state.get("parsed_log")
        alert_data: dict[str, Any] = state.get("alert_data", {})

        # ── Build retrieval query ─────────────────────────────────────────
        if parsed:
            parts = [
                parsed.get("primary_exception", ""),
                " ".join(parsed.get("error_keywords", [])[:5]),
                parsed.get("service_context", ""),
                parsed.get("raw_log_excerpt", "")[:500],
            ]
        else:
            parts = [alert_data.get("raw_log_stacktrace", "")[:800]]

        query = " ".join(p for p in parts if p).strip()
        if not query:
            query = "service error incident"

        traces.append(make_trace_event(
            node=node, event="retrieval_query",
            message=f"Query constructed (len={len(query)})",
            metadata={"query_preview": query[:200]},
        ))
        logger.info("[%s] retrieval query (len=%d): %s…", node.value, len(query), query[:120])

        # ── Execute hybrid retrieval ───────────────────────────────────────
        cfg = get_settings()
        retriever = HybridSearchRetriever.from_index(settings=cfg)
        raw_results = retriever.retrieve_as_dicts(query, top_k=cfg.TOP_K_RESULTS)

        retrieved: list[RetrievedRunbook] = [
            RetrievedRunbook(**{k: v for k, v in r.items() if k in RetrievedRunbook.__annotations__})
            for r in raw_results
        ]

        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message=f"Retrieved {len(retrieved)} runbook chunk(s)",
            duration_ms=duration_ms,
            metadata={
                "chunk_count": len(retrieved),
                "top_source": retrieved[0].get("source_file") if retrieved else None,
                "top_rerank_score": retrieved[0].get("rerank_score") if retrieved else None,
            },
        ))
        logger.info(
            "[%s] retrieved %d chunks; top=%s (score=%.3f)",
            node.value, len(retrieved),
            retrieved[0].get("source_file", "n/a") if retrieved else "n/a",
            retrieved[0].get("rerank_score", 0.0) if retrieved else 0.0,
        )

        return {
            "retrieved_runbooks": retrieved,
            "workflow_status": WorkflowStatus.RUNNING,
            "trace_log": traces,
        }

    except Exception as exc:
        duration_ms = (time.monotonic() - t_start) * 1000
        err_msg = f"RetrieveRunbookNode failed: {exc}"
        logger.exception("[%s] %s", node.value, err_msg)
        traces.append(make_trace_event(
            node=node, event="error",
            message=err_msg, duration_ms=duration_ms,
            metadata={"exception": str(exc)},
        ))
        # Return empty list so PlanRemediationNode can still run with LLM only
        return {
            "retrieved_runbooks": [],
            "workflow_status": WorkflowStatus.RUNNING,
            "error_message": err_msg,
            "trace_log": traces,
        }


# ---------------------------------------------------------------------------
# Node 3 — PlanRemediationNode
# ---------------------------------------------------------------------------

# System prompt for the LLM — instructs strict JSON output
_PLAN_SYSTEM_PROMPT = """You are an expert Site Reliability Engineer (SRE) specialising in incident remediation.

Your task: analyse an incident alert and produce a structured remediation plan as STRICT JSON.

RULES:
1. Output ONLY a valid JSON object. No markdown fences, no preamble, no explanation outside the JSON.
2. Base your plan on the retrieved runbook procedures when available.
3. Classify risk_tier conservatively: if unsure between MEDIUM and HIGH, choose HIGH.
4. HIGH risk actions: container restart, pod kill, DB connection flush, rolling restart, cache wipe.
5. CRITICAL risk actions: database flush/truncate, cluster drain, firewall rule changes, credential rotation.
6. LOW/MEDIUM risk: log rotation, cache TTL adjustment, replica scaling, config reload.

Required JSON schema:
{
  "action_id": "<uuid4>",
  "title": "<one-line title>",
  "description": "<what will be done and why>",
  "risk_tier": "<LOW|MEDIUM|HIGH|CRITICAL>",
  "tool_name": "<snake_case_tool_name>",
  "tool_parameters": {<key: value pairs>},
  "estimated_impact": "<blast radius description>",
  "rollback_procedure": "<how to undo>",
  "confidence_score": <0.0-1.0>,
  "reasoning": "<chain of thought>",
  "analysis_summary": "<2-3 sentence root cause + approach summary for humans>"
}"""


def _build_plan_user_prompt(
    alert_data: dict[str, Any],
    parsed_log: ParsedLog | None,
    runbooks: list[RetrievedRunbook],
) -> str:
    """Compose the user message for the remediation LLM call."""
    lines: list[str] = ["## Incident Alert"]
    lines.append(f"- Alert ID: {alert_data.get('alert_id', 'unknown')}")
    lines.append(f"- Service: {alert_data.get('service_name', 'unknown')}")
    lines.append(f"- Severity: {alert_data.get('severity', 'unknown')}")
    lines.append(f"- Environment: {alert_data.get('environment', 'unknown')}")
    lines.append(f"- Timestamp: {alert_data.get('timestamp', 'unknown')}")

    if parsed_log:
        lines.append("\n## Parsed Exception Information")
        lines.append(f"- Primary Exception: {parsed_log.get('primary_exception', 'N/A')}")
        keywords = parsed_log.get("error_keywords", [])
        if keywords:
            lines.append(f"- Error Keywords: {', '.join(keywords[:10])}")
        sigs = parsed_log.get("exception_signatures", [])
        for i, sig in enumerate(sigs[:3], 1):
            lines.append(f"- Exception {i}: {sig.get('exception_type')}: {sig.get('exception_message', '')[:200]}")

    lines.append("\n## Raw Log Excerpt (first 800 chars)")
    raw = alert_data.get("raw_log_stacktrace", "")[:800]
    lines.append(f"```\n{raw}\n```")

    if runbooks:
        lines.append("\n## Retrieved Runbook Procedures")
        for i, rb in enumerate(runbooks[:3], 1):
            lines.append(
                f"\n### Runbook {i}: {rb.get('source_file', 'unknown')} "
                f"— {rb.get('section_path', '')} "
                f"(rerank_score={rb.get('rerank_score', 0):.3f})"
            )
            lines.append(rb.get("text_preview", "")[:600])
    else:
        lines.append("\n## Retrieved Runbook Procedures\nNone retrieved — use general SRE knowledge.")

    lines.append("\n## Task")
    lines.append(
        "Produce a remediation plan as a single JSON object matching the schema above. "
        "Do NOT include any text outside the JSON."
    )
    return "\n".join(lines)


def plan_remediation_node(state: IncidentState) -> dict[str, Any]:
    """
    Call the LLM to produce a structured JSON remediation plan.

    Input  : ``state["alert_data"]``, ``state["parsed_log"]``,
             ``state["retrieved_runbooks"]``
    Output : ``proposed_action`` (ProposedAction TypedDict),
             ``analysis_summary`` (str)

    LLM selection: Groq → Claude (first available API key wins).
    Output is strict JSON; falls back to a safe UNKNOWN-risk skeleton on
    parse failure so the graph can still route to HITL.
    """
    t_start = time.monotonic()
    node = NodeName.PLAN_REMEDIATION
    traces: list = []
    cfg = get_settings()

    traces.append(make_trace_event(
        node=node, event="node_start",
        message="PlanRemediationNode started",
    ))

    alert_data: dict[str, Any] = state.get("alert_data", {})
    parsed_log: ParsedLog | None = state.get("parsed_log")
    runbooks: list[RetrievedRunbook] = state.get("retrieved_runbooks", [])

    try:
        llm = _get_llm()
        model_name = cfg.GROQ_MODEL if cfg.GROQ_API_KEY else cfg.ANTHROPIC_MODEL

        user_prompt = _build_plan_user_prompt(alert_data, parsed_log, runbooks)

        traces.append(make_trace_event(
            node=node, event="llm_call_start",
            message=f"Invoking LLM ({model_name}); prompt_len={len(user_prompt)}",
            metadata={"model": model_name, "prompt_len": len(user_prompt)},
        ))
        logger.info("[%s] invoking LLM model=%s prompt_len=%d", node.value, model_name, len(user_prompt))

        from langchain_core.messages import HumanMessage, SystemMessage
        messages = [
            SystemMessage(content=_PLAN_SYSTEM_PROMPT),
            HumanMessage(content=user_prompt),
        ]

        llm_t = time.monotonic()
        response = llm.invoke(messages)
        llm_duration_ms = (time.monotonic() - llm_t) * 1000

        raw_content: str = response.content.strip()
        usage = getattr(response, "usage_metadata", {}) or {}

        traces.append(make_trace_event(
            node=node, event="llm_call_end",
            message=f"LLM responded in {llm_duration_ms:.0f}ms",
            duration_ms=llm_duration_ms,
            metadata={
                "model": model_name,
                "input_tokens": usage.get("input_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "response_len": len(raw_content),
            },
        ))

        # ── Parse JSON response ────────────────────────────────────────────
        # Strip markdown fences if the LLM wraps the JSON anyway
        json_text = re.sub(r"^```(?:json)?\s*", "", raw_content, flags=re.IGNORECASE)
        json_text = re.sub(r"\s*```$", "", json_text).strip()

        try:
            plan_dict: dict[str, Any] = json.loads(json_text)
        except json.JSONDecodeError as je:
            # Try to extract the first {...} block as a fallback
            brace_match = re.search(r"\{.*\}", json_text, re.DOTALL)
            if brace_match:
                plan_dict = json.loads(brace_match.group(0))
            else:
                raise ValueError(f"LLM response is not valid JSON: {je}") from je

        # ── Normalise and validate required fields ─────────────────────────
        plan_dict.setdefault("action_id", str(uuid.uuid4()))
        plan_dict.setdefault("planned_at", datetime.now(tz=timezone.utc).isoformat())
        plan_dict["llm_model_used"] = model_name

        # Validate risk_tier enum
        raw_tier = str(plan_dict.get("risk_tier", "UNKNOWN")).upper()
        valid_tiers = {t.value for t in RiskTier}
        if raw_tier not in valid_tiers:
            logger.warning("[%s] LLM returned invalid risk_tier=%s — defaulting to UNKNOWN", node.value, raw_tier)
            raw_tier = RiskTier.UNKNOWN.value
        plan_dict["risk_tier"] = raw_tier

        # Clamp confidence score
        try:
            plan_dict["confidence_score"] = max(0.0, min(1.0, float(plan_dict.get("confidence_score", 0.5))))
        except (TypeError, ValueError):
            plan_dict["confidence_score"] = 0.5

        # Extract analysis_summary before building ProposedAction
        analysis_summary: str = plan_dict.pop("analysis_summary", "No summary provided.")

        proposed: ProposedAction = ProposedAction(**{
            k: plan_dict[k]
            for k in ProposedAction.__annotations__
            if k in plan_dict
        })

        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message=(
                f"Plan generated: tool={proposed.get('tool_name')} "
                f"risk={proposed.get('risk_tier')} "
                f"confidence={proposed.get('confidence_score', 0):.2f}"
            ),
            duration_ms=duration_ms,
            metadata={
                "tool_name": proposed.get("tool_name"),
                "risk_tier": proposed.get("risk_tier"),
                "confidence_score": proposed.get("confidence_score"),
                "action_id": proposed.get("action_id"),
            },
        ))
        logger.info(
            "[%s] plan ready action_id=%s tool=%s risk=%s confidence=%.2f",
            node.value,
            proposed.get("action_id"),
            proposed.get("tool_name"),
            proposed.get("risk_tier"),
            proposed.get("confidence_score", 0),
        )

        return {
            "proposed_action": proposed,
            "analysis_summary": analysis_summary,
            "workflow_status": WorkflowStatus.RUNNING,
            "trace_log": traces,
        }

    except Exception as exc:
        duration_ms = (time.monotonic() - t_start) * 1000
        err_msg = f"PlanRemediationNode failed: {exc}"
        logger.exception("[%s] %s", node.value, err_msg)
        traces.append(make_trace_event(
            node=node, event="error",
            message=err_msg, duration_ms=duration_ms,
            metadata={"exception": str(exc)},
        ))
        # Return a safe HIGH-risk skeleton so HITL always fires on failures
        fallback_action = ProposedAction(
            action_id=str(uuid.uuid4()),
            title="Remediation plan unavailable — manual review required",
            description="LLM planning failed. Human review is mandatory.",
            risk_tier=RiskTier.HIGH.value,
            tool_name="manual_review",
            tool_parameters={},
            estimated_impact="Unknown — treat as HIGH risk",
            rollback_procedure="N/A — no automated action taken",
            confidence_score=0.0,
            reasoning=f"Planning node error: {exc}",
            llm_model_used="none",
            planned_at=datetime.now(tz=timezone.utc).isoformat(),
        )
        return {
            "proposed_action": fallback_action,
            "analysis_summary": f"Automated planning failed: {exc}. Manual triage required.",
            "workflow_status": WorkflowStatus.RUNNING,
            "error_message": err_msg,
            "trace_log": traces,
        }


# ---------------------------------------------------------------------------
# Node 4 — HITLCheckNode
# ---------------------------------------------------------------------------

def hitl_check_node(state: IncidentState) -> dict[str, Any]:
    """
    Evaluate the proposed action's risk tier and route accordingly.

    Logic
    -----
    * Risk tier in HITL_RISK_TIERS (HIGH, CRITICAL):
        → Post to Slack Webhook with action details + approve/reject prompt.
        → Set ``approval_status = PENDING``.
        → Workflow transitions to ``await_approval`` node.
    * Risk tier LOW or MEDIUM:
        → Set ``approval_status = NOT_REQUIRED``.
        → Workflow transitions directly to ``auto_execute`` node.
    * Slack not configured (SLACK_WEBHOOK_URL is None):
        → Set ``approval_status = SKIPPED``.
        → Log a warning; proceed to ``auto_execute``.

    Input  : ``state["proposed_action"]``
    Output : ``approval_status``, ``slack_notification_sent``,
             ``slack_message_ts``
    """
    t_start = time.monotonic()
    node = NodeName.HITL_CHECK
    traces: list = []
    cfg = get_settings()

    traces.append(make_trace_event(
        node=node, event="node_start",
        message="HITLCheckNode started",
    ))

    proposed: ProposedAction | None = state.get("proposed_action")
    alert_data: dict[str, Any] = state.get("alert_data", {})

    if not proposed:
        err_msg = "HITLCheckNode: no proposed_action in state — aborting"
        logger.error("[%s] %s", node.value, err_msg)
        traces.append(make_trace_event(
            node=node, event="error", message=err_msg,
            duration_ms=(time.monotonic() - t_start) * 1000,
        ))
        return {
            "approval_status": ApprovalStatus.REJECTED,
            "workflow_status": WorkflowStatus.FAILED,
            "error_message": err_msg,
            "trace_log": traces,
        }

    risk_tier: str = str(proposed.get("risk_tier", RiskTier.UNKNOWN.value)).upper()
    hitl_tiers: list[str] = [t.upper() for t in cfg.HITL_RISK_TIERS]
    requires_hitl: bool = risk_tier in hitl_tiers

    logger.info(
        "[%s] risk_tier=%s requires_hitl=%s action_id=%s",
        node.value, risk_tier, requires_hitl, proposed.get("action_id"),
    )

    traces.append(make_trace_event(
        node=node, event="risk_evaluation",
        message=f"risk_tier={risk_tier} requires_hitl={requires_hitl}",
        metadata={
            "risk_tier": risk_tier,
            "hitl_tiers": hitl_tiers,
            "requires_hitl": requires_hitl,
            "tool_name": proposed.get("tool_name"),
        },
    ))

    if not requires_hitl:
        # LOW / MEDIUM — auto-approve
        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message=f"Auto-approved: risk_tier={risk_tier} below HITL threshold",
            duration_ms=duration_ms,
        ))
        return {
            "approval_status": ApprovalStatus.NOT_REQUIRED,
            "slack_notification_sent": False,
            "workflow_status": WorkflowStatus.RUNNING,
            "trace_log": traces,
        }

    # ── HIGH / CRITICAL — notify Slack ────────────────────────────────────
    if not cfg.SLACK_WEBHOOK_URL:
        logger.warning(
            "[%s] HIGH-risk action but SLACK_WEBHOOK_URL not configured — "
            "skipping HITL. Set SLACK_WEBHOOK_URL to enable approvals.",
            node.value,
        )
        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message="Slack not configured — HITL skipped (treat as PENDING externally)",
            duration_ms=duration_ms,
            metadata={"risk_tier": risk_tier},
        ))
        return {
            "approval_status": ApprovalStatus.SKIPPED,
            "slack_notification_sent": False,
            "workflow_status": WorkflowStatus.AWAITING_HUMAN,
            "trace_log": traces,
        }

    # ── Post Slack notification ────────────────────────────────────────────
    try:
        from src.agent.slack_client import SlackWebhookClient

        slack = SlackWebhookClient(
            webhook_url=cfg.SLACK_WEBHOOK_URL,
            timeout=cfg.SLACK_TIMEOUT,
        )
        slack_ts = slack.post_hitl_notification(
            alert_data=alert_data,
            proposed_action=proposed,
            analysis_summary=state.get("analysis_summary") or "",
        )

        duration_ms = (time.monotonic() - t_start) * 1000
        traces.append(make_trace_event(
            node=node, event="node_end",
            message=f"Slack HITL notification sent; ts={slack_ts}",
            duration_ms=duration_ms,
            metadata={"slack_ts": slack_ts, "risk_tier": risk_tier},
        ))
        logger.info(
            "[%s] Slack notification sent for action_id=%s risk=%s ts=%s",
            node.value, proposed.get("action_id"), risk_tier, slack_ts,
        )

        return {
            "approval_status": ApprovalStatus.PENDING,
            "slack_notification_sent": True,
            "slack_message_ts": slack_ts,
            "workflow_status": WorkflowStatus.AWAITING_HUMAN,
            "trace_log": traces,
        }

    except Exception as exc:
        duration_ms = (time.monotonic() - t_start) * 1000
        err_msg = f"Slack notification failed: {exc}"
        logger.exception("[%s] %s", node.value, err_msg)
        traces.append(make_trace_event(
            node=node, event="error",
            message=err_msg, duration_ms=duration_ms,
            metadata={"exception": str(exc)},
        ))
        # Fail safe: treat as PENDING so the action isn't auto-executed
        return {
            "approval_status": ApprovalStatus.PENDING,
            "slack_notification_sent": False,
            "workflow_status": WorkflowStatus.AWAITING_HUMAN,
            "error_message": err_msg,
            "trace_log": traces,
        }


# ---------------------------------------------------------------------------
# Terminal nodes
# ---------------------------------------------------------------------------

def auto_execute_node(state: IncidentState) -> dict[str, Any]:
    """
    Placeholder execution node for LOW / MEDIUM risk actions.

    In production: replace the stub with actual tool dispatch logic
    (kubectl calls, Redis flushes, PagerDuty API, etc.).
    """
    t_start = time.monotonic()
    node = NodeName.AUTO_EXECUTE
    proposed = state.get("proposed_action", {}) or {}

    logger.info(
        "[%s] AUTO-EXECUTING tool=%s params=%s",
        node.value,
        proposed.get("tool_name"),
        proposed.get("tool_parameters"),
    )

    duration_ms = (time.monotonic() - t_start) * 1000
    return {
        "workflow_status": WorkflowStatus.COMPLETED,
        "trace_log": [make_trace_event(
            node=node, event="node_end",
            message=f"Auto-executed tool={proposed.get('tool_name')} "
                    f"[STUB — replace with real dispatch]",
            duration_ms=duration_ms,
            metadata={
                "tool_name": proposed.get("tool_name"),
                "tool_parameters": proposed.get("tool_parameters"),
                "action_id": proposed.get("action_id"),
            },
        )],
    }


def await_approval_node(state: IncidentState) -> dict[str, Any]:
    """
    Terminal holding node for HIGH / CRITICAL actions awaiting Slack approval.

    The graph ends here.  External systems (Slack interactivity handler,
    webhook callback) resume the workflow by updating approval_status and
    re-invoking the compiled graph with the updated state.
    """
    node = NodeName.AWAIT_APPROVAL
    proposed = state.get("proposed_action", {}) or {}

    logger.info(
        "[%s] Workflow paused — awaiting human approval for action_id=%s",
        node.value, proposed.get("action_id"),
    )

    return {
        "workflow_status": WorkflowStatus.AWAITING_HUMAN,
        "trace_log": [make_trace_event(
            node=node, event="node_end",
            message="Workflow paused — awaiting human approval via Slack",
            metadata={
                "action_id": proposed.get("action_id"),
                "risk_tier": proposed.get("risk_tier"),
                "approval_status": str(state.get("approval_status")),
            },
        )],
    }
