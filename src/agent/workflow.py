"""
LangGraph incident remediation state machine.

Graph topology
--------------

  [START]
     │
     ▼
  parse_log  ──────────────────────────────────────────────────────► [END: FAILED]
     │ (always continues unless FAILED)
     ▼
  retrieve_runbook ────────────────────────────────────────────────► [END: FAILED]
     │
     ▼
  plan_remediation ────────────────────────────────────────────────► [END: FAILED]
     │
     ▼
  hitl_check ──────┬──── NOT_REQUIRED / SKIPPED ──► auto_execute ──► [END]
                   │
                   └──── PENDING / AWAITING_HUMAN ─► await_approval ► [END]

Conditional routing is driven by ``WorkflowStatus`` and ``ApprovalStatus``
fields in ``IncidentState``.

Usage
-----
    from src.agent.workflow import build_workflow
    from src.agent.state   import initial_state

    app    = build_workflow()
    result = app.invoke(initial_state(alert_payload_dict))

    # Inspect final state
    print(result["workflow_status"])
    print(result["proposed_action"])
    print(result["approval_status"])

Async usage (inside FastAPI / Celery async context)
---------------------------------------------------
    import asyncio
    result = await asyncio.to_thread(app.invoke, initial_state(alert_dict))

LangSmith tracing (optional)
-----------------------------
Set ``LANGCHAIN_TRACING_V2=true`` and ``LANGCHAIN_API_KEY`` in your env to
get automatic LangSmith traces for every graph execution.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from langgraph.graph import END, START, StateGraph

from src.agent.nodes import (
    auto_execute_node,
    await_approval_node,
    execute_action_node,
    hitl_check_node,
    parse_log_node,
    plan_remediation_node,
    retrieve_runbook_node,
)
from src.agent.state import (
    ApprovalStatus,
    IncidentState,
    NodeName,
    WorkflowStatus,
    initial_state,
    make_trace_event,
)
from src.api.core.config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conditional edge functions
# ---------------------------------------------------------------------------

def _route_after_parse(
    state: IncidentState,
) -> Literal["retrieve_runbook", "__end__"]:
    """
    After ParseLogNode: abort only on a hard FAILED status.
    A degraded parse (parse_error set but status=RUNNING) continues.
    """
    status = state.get("workflow_status")
    if status == WorkflowStatus.FAILED:
        logger.warning(
            "[router] parse_log → END (FAILED): %s", state.get("error_message")
        )
        return END
    return NodeName.RETRIEVE_RUNBOOK.value


def _route_after_retrieve(
    state: IncidentState,
) -> Literal["plan_remediation", "__end__"]:
    """
    After RetrieveRunbookNode: abort on FAILED; always continue otherwise
    (empty retrieval is acceptable — LLM will use general knowledge).
    """
    status = state.get("workflow_status")
    if status == WorkflowStatus.FAILED:
        logger.warning(
            "[router] retrieve_runbook → END (FAILED): %s", state.get("error_message")
        )
        return END
    return NodeName.PLAN_REMEDIATION.value


def _route_after_plan(
    state: IncidentState,
) -> Literal["hitl_check", "__end__"]:
    """
    After PlanRemediationNode: always proceed to HITL check.
    Even a fallback HIGH-risk skeleton must go through the HITL gate.
    """
    status = state.get("workflow_status")
    if status == WorkflowStatus.FAILED:
        logger.warning(
            "[router] plan_remediation → END (FAILED): %s", state.get("error_message")
        )
        return END
    return NodeName.HITL_CHECK.value


def _route_after_hitl(
    state: IncidentState,
) -> Literal["auto_execute", "await_approval", "__end__"]:
    """
    After HITLCheckNode — the primary routing decision.

    Routing table
    -------------
    ApprovalStatus.NOT_REQUIRED  → auto_execute  (LOW / MEDIUM risk)
    ApprovalStatus.SKIPPED       → auto_execute  (Slack not configured)
    ApprovalStatus.PENDING       → await_approval (Slack notified, hold)
    ApprovalStatus.APPROVED      → auto_execute  (human approved in Slack)
    ApprovalStatus.REJECTED      → END            (human rejected)
    None / unknown               → await_approval (fail-safe hold)
    WorkflowStatus.FAILED        → END
    """
    status = state.get("workflow_status")
    approval = state.get("approval_status")

    if status == WorkflowStatus.FAILED:
        logger.warning(
            "[router] hitl_check → END (FAILED): %s", state.get("error_message")
        )
        return END

    if approval in (ApprovalStatus.NOT_REQUIRED, ApprovalStatus.SKIPPED, ApprovalStatus.APPROVED):
        logger.info("[router] hitl_check → auto_execute (approval=%s)", approval)
        return NodeName.AUTO_EXECUTE.value

    if approval == ApprovalStatus.REJECTED:
        logger.info("[router] hitl_check → END (REJECTED by human)")
        return END

    # PENDING, None, or unknown → hold for human
    logger.info("[router] hitl_check → await_approval (approval=%s)", approval)
    return NodeName.AWAIT_APPROVAL.value


def _route_after_await(
    state: IncidentState,
) -> Literal["execute_action", "__end__"]:
    """
    After AwaitApprovalNode: the high-risk branch has already posted the
    Slack Block Kit webhook.  Execution proceeds **only** when both are true:

    1. ``approval_status == APPROVED`` (human clicked Approve), and
    2. ``auth_token`` carries a JWT for ``src/k8s_executor.py`` to verify.

    Otherwise the graph ends and waits for an external re-invocation with
    the approval + token attached (fail-safe hold — no JWT, no execution).
    """
    approval = state.get("approval_status")
    auth_token = state.get("auth_token")

    if approval == ApprovalStatus.APPROVED and auth_token:
        logger.info(
            "[router] await_approval → execute_action (approval=APPROVED, jwt=present)"
        )
        return NodeName.EXECUTE_ACTION.value

    logger.info(
        "[router] await_approval → END (approval=%s, jwt=%s) — holding for JWT authorization",
        approval,
        "present" if auth_token else "absent",
    )
    return END


def _route_after_auto(
    state: IncidentState,
) -> Literal["execute_action", "__end__"]:
    """
    After AutoExecuteNode: dispatch only when a JWT is attached; otherwise
    the node already flagged ``AWAITING_HUMAN`` and we hold at END.
    """
    if state.get("workflow_status") == WorkflowStatus.AWAITING_HUMAN:
        logger.info("[router] auto_execute → END (awaiting JWT authorization)")
        return END
    return NodeName.EXECUTE_ACTION.value


# ---------------------------------------------------------------------------
# Graph factory
# ---------------------------------------------------------------------------

def build_workflow(settings=None):
    """
    Construct, configure, and compile the incident remediation LangGraph.

    Returns
    -------
    ``CompiledStateGraph``
        A compiled, invocable LangGraph application.  Call ``.invoke(state)``
        or ``.stream(state)`` to run the workflow.

    Parameters
    ----------
    settings : Optional ``Settings`` instance.  Uses ``get_settings()`` if None.
    """
    cfg = settings or get_settings()

    # ── Build the graph ────────────────────────────────────────────────────
    graph = StateGraph(IncidentState)

    # ── Register nodes ─────────────────────────────────────────────────────
    graph.add_node(NodeName.PARSE_LOG.value, parse_log_node)
    graph.add_node(NodeName.RETRIEVE_RUNBOOK.value, retrieve_runbook_node)
    graph.add_node(NodeName.PLAN_REMEDIATION.value, plan_remediation_node)
    graph.add_node(NodeName.HITL_CHECK.value, hitl_check_node)
    graph.add_node(NodeName.AUTO_EXECUTE.value, auto_execute_node)
    graph.add_node(NodeName.AWAIT_APPROVAL.value, await_approval_node)
    graph.add_node(NodeName.EXECUTE_ACTION.value, execute_action_node)

    # ── Entry edge ─────────────────────────────────────────────────────────
    graph.add_edge(START, NodeName.PARSE_LOG.value)

    # ── Conditional edges (routing logic) ─────────────────────────────────
    graph.add_conditional_edges(
        NodeName.PARSE_LOG.value,
        _route_after_parse,
        {
            NodeName.RETRIEVE_RUNBOOK.value: NodeName.RETRIEVE_RUNBOOK.value,
            END: END,
        },
    )

    graph.add_conditional_edges(
        NodeName.RETRIEVE_RUNBOOK.value,
        _route_after_retrieve,
        {
            NodeName.PLAN_REMEDIATION.value: NodeName.PLAN_REMEDIATION.value,
            END: END,
        },
    )

    graph.add_conditional_edges(
        NodeName.PLAN_REMEDIATION.value,
        _route_after_plan,
        {
            NodeName.HITL_CHECK.value: NodeName.HITL_CHECK.value,
            END: END,
        },
    )

    graph.add_conditional_edges(
        NodeName.HITL_CHECK.value,
        _route_after_hitl,
        {
            NodeName.AUTO_EXECUTE.value: NodeName.AUTO_EXECUTE.value,
            NodeName.AWAIT_APPROVAL.value: NodeName.AWAIT_APPROVAL.value,
            END: END,
        },
    )

    # ── Terminal edges ─────────────────────────────────────────────────────
    # High-risk branch: Slack Block Kit posted → await JWT authorization →
    # only then may execution reach src/k8s_executor.py.
    graph.add_conditional_edges(
        NodeName.AWAIT_APPROVAL.value,
        _route_after_await,
        {
            NodeName.EXECUTE_ACTION.value: NodeName.EXECUTE_ACTION.value,
            END: END,
        },
    )

    graph.add_conditional_edges(
        NodeName.AUTO_EXECUTE.value,
        _route_after_auto,
        {
            NodeName.EXECUTE_ACTION.value: NodeName.EXECUTE_ACTION.value,
            END: END,
        },
    )

    # ExecuteActionNode is the single, JWT-gated exit to the cluster.
    graph.add_edge(NodeName.EXECUTE_ACTION.value, END)

    # ── Compile ────────────────────────────────────────────────────────────
    compiled = graph.compile()

    logger.info(
        "Incident remediation workflow compiled. "
        "Nodes: %s  |  HITL tiers: %s  |  Recursion limit: %d",
        [n.value for n in NodeName if n != NodeName.TERMINAL],
        cfg.HITL_RISK_TIERS,
        cfg.LANGGRAPH_RECURSION_LIMIT,
    )
    return compiled


# ---------------------------------------------------------------------------
# Convenience runner
# ---------------------------------------------------------------------------

def run_workflow(
    alert_data: dict[str, Any],
    settings=None,
    stream: bool = False,
) -> IncidentState:
    """
    Build the workflow and run it for a single alert payload.

    Parameters
    ----------
    alert_data : ``AlertPayload.model_dump(mode='json')`` dict.
    settings   : Optional ``Settings`` override.
    stream     : If ``True``, prints node-by-node state deltas to stdout
                 (useful for interactive debugging).

    Returns
    -------
    Final ``IncidentState`` dict after the graph reaches END.

    Examples
    --------
    >>> from src.agent.workflow import run_workflow
    >>> result = run_workflow(alert.model_dump(mode="json"))
    >>> print(result["workflow_status"])
    >>> print(result["proposed_action"]["risk_tier"])
    >>> for event in result["trace_log"]:
    ...     print(event["timestamp"], event["node"], event["event"], event["message"])
    """
    cfg = settings or get_settings()
    app = build_workflow(cfg)
    state = initial_state(alert_data)

    config = {"recursion_limit": cfg.LANGGRAPH_RECURSION_LIMIT}

    if stream:
        final: IncidentState = {}  # type: ignore[assignment]
        print("\n── Workflow stream ──────────────────────────────────")
        for step in app.stream(state, config=config):
            for node_name, node_output in step.items():
                status = node_output.get("workflow_status", "")
                print(f"  ✓ {node_name:<25} status={status}")
            final = step  # last step contains merged state
        print("── End of stream ────────────────────────────────────\n")
        return final  # type: ignore[return-value]

    return app.invoke(state, config=config)  # type: ignore[return-value]
