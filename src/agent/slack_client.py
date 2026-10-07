"""
Slack Incoming Webhook client for HITL approval notifications.

Responsibilities
----------------
* Format a rich Block Kit message describing the proposed remediation action.
* POST it to the configured Slack Incoming Webhook URL.
* Return the Slack response timestamp (``x-slack-ts`` header) for threading.

Design decisions
----------------
* Uses ``httpx`` (already a project dependency) with a short timeout so a
  slow Slack API never stalls the workflow node.
* Block Kit layout is structured for readability in incident channels:
  - Header with severity emoji
  - Key/value fields (service, environment, risk tier, tool)
  - Analysis summary section
  - Rollback procedure (collapsed in a context block)
  - Action buttons are described in the message but require a Slack App with
    Interactivity enabled to be functional.  Without it, operators reply in
    the thread — a pragmatic degradation.
* The client is intentionally synchronous.  ``hitl_check_node`` calls it
  inside ``asyncio.to_thread`` when used in an async context.
* No secrets are logged.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from src.agent.state import ProposedAction, RiskTier

logger = logging.getLogger(__name__)

# Risk tier → Slack emoji + colour for visual triage
_TIER_EMOJI: dict[str, str] = {
    RiskTier.LOW.value: "🟢",
    RiskTier.MEDIUM.value: "🟡",
    RiskTier.HIGH.value: "🔴",
    RiskTier.CRITICAL.value: "🚨",
    RiskTier.UNKNOWN.value: "⚠️",
}

_TIER_COLOUR: dict[str, str] = {
    RiskTier.LOW.value: "#36a64f",
    RiskTier.MEDIUM.value: "#ffa500",
    RiskTier.HIGH.value: "#e01e5a",
    RiskTier.CRITICAL.value: "#9b0000",
    RiskTier.UNKNOWN.value: "#cccccc",
}

_SEVERITY_EMOJI: dict[str, str] = {
    "low": "ℹ️",
    "medium": "⚠️",
    "high": "🔴",
    "critical": "🚨",
}


class SlackWebhookClient:
    """
    Thin wrapper around Slack Incoming Webhooks.

    Parameters
    ----------
    webhook_url : Full Slack Incoming Webhook URL.
    timeout     : HTTP request timeout in seconds (default 10).
    """

    def __init__(self, webhook_url: str, timeout: int = 10) -> None:
        if not webhook_url:
            raise ValueError("webhook_url must be a non-empty string.")
        self._url = webhook_url
        self._timeout = timeout

    # ── Public API ─────────────────────────────────────────────────────────

    def post_hitl_notification(
        self,
        alert_data: dict[str, Any],
        proposed_action: ProposedAction,
        analysis_summary: str = "",
    ) -> str:
        """
        Post a formatted HITL approval request to Slack.

        Parameters
        ----------
        alert_data        : Raw alert payload dict.
        proposed_action   : Structured remediation plan from PlanRemediationNode.
        analysis_summary  : Plain-text root cause summary.

        Returns
        -------
        str : Slack response body (typically ``"ok"``), used as a correlation
              token for threading.  Returns ``""`` on non-raising soft errors.

        Raises
        ------
        httpx.HTTPStatusError : On 4xx / 5xx Slack API responses.
        httpx.TimeoutException: If Slack does not respond within ``timeout``.
        """
        payload = self._build_payload(alert_data, proposed_action, analysis_summary)

        logger.info(
            "Posting HITL notification to Slack for action_id=%s risk=%s",
            proposed_action.get("action_id"),
            proposed_action.get("risk_tier"),
        )

        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(
                self._url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

        # Slack webhooks return plain "ok" or an error string as body
        slack_response_text = response.text.strip()
        logger.info("Slack response: %s", slack_response_text)
        return slack_response_text

    def post_simple_message(self, text: str) -> str:
        """Post a plain-text message. Useful for status updates."""
        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(self._url, json={"text": text})
            response.raise_for_status()
        return response.text.strip()

    # ── Block Kit message builder ──────────────────────────────────────────

    def _build_payload(
        self,
        alert_data: dict[str, Any],
        action: ProposedAction,
        summary: str,
    ) -> dict[str, Any]:
        """
        Build a Slack Block Kit message payload for a HITL approval request.

        Layout:
        ┌─────────────────────────────────────────────┐
        │  🚨 HITL APPROVAL REQUIRED — HIGH Risk       │  (header)
        │  Alert: payment-service | CRITICAL | prod    │  (context)
        ├─────────────────────────────────────────────┤
        │  📋 Analysis Summary                         │  (section)
        │  Root cause text...                          │
        ├─────────────────────────────────────────────┤
        │  🔧 Proposed Action Details                  │  (fields)
        │  Tool | Risk Tier | Confidence               │
        │  Parameters JSON                             │
        ├─────────────────────────────────────────────┤
        │  📖 Rollback Procedure                       │  (context)
        ├─────────────────────────────────────────────┤
        │  ✅ APPROVE    ❌ REJECT                     │  (actions)
        └─────────────────────────────────────────────┘
        """
        risk_tier: str = str(action.get("risk_tier", "UNKNOWN")).upper()
        tier_emoji = _TIER_EMOJI.get(risk_tier, "⚠️")
        severity = str(alert_data.get("severity", "unknown")).lower()
        sev_emoji = _SEVERITY_EMOJI.get(severity, "⚠️")
        action_id = action.get("action_id", "unknown")
        service = alert_data.get("service_name", "unknown")
        environment = alert_data.get("environment", "unknown")

        # Truncate long strings for Slack's 3000-char block text limit
        summary_text = (summary or "No summary available.")[:800]
        description = (action.get("description") or "")[:600]
        rollback = (action.get("rollback_procedure") or "Not specified.")[:400]
        estimated_impact = (action.get("estimated_impact") or "Unknown.")[:300]
        reasoning = (action.get("reasoning") or "")[:500]

        # Tool parameters as formatted JSON (capped for readability)
        try:
            params_str = str(action.get("tool_parameters") or {})
            if len(params_str) > 400:
                params_str = params_str[:400] + "…"
        except Exception:
            params_str = "{}"

        confidence = action.get("confidence_score", 0.0)
        confidence_bar = "▓" * int(confidence * 10) + "░" * (10 - int(confidence * 10))

        blocks: list[dict[str, Any]] = [
            # ── Header ────────────────────────────────────────────────────
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{tier_emoji} HITL APPROVAL REQUIRED — {risk_tier} Risk",
                    "emoji": True,
                },
            },
            # ── Alert context ──────────────────────────────────────────────
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"{sev_emoji} *Alert:* `{alert_data.get('alert_id', 'unknown')}`  |  "
                            f"*Service:* `{service}`  |  "
                            f"*Severity:* `{severity.upper()}`  |  "
                            f"*Env:* `{environment}`"
                        ),
                    }
                ],
            },
            {"type": "divider"},
            # ── Analysis summary ───────────────────────────────────────────
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📋 Analysis Summary*\n{summary_text}",
                },
            },
            {"type": "divider"},
            # ── Action title + description ─────────────────────────────────
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*🔧 Proposed Action:* {action.get('title', 'Unknown')}\n"
                        f"{description}"
                    ),
                },
            },
            # ── Key fields ─────────────────────────────────────────────────
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Tool Name*\n`{action.get('tool_name', 'unknown')}`"},
                    {"type": "mrkdwn", "text": f"*Risk Tier*\n{tier_emoji} `{risk_tier}`"},
                    {"type": "mrkdwn", "text": f"*Confidence*\n`{confidence_bar}` {confidence:.0%}"},
                    {"type": "mrkdwn", "text": f"*Action ID*\n`{action_id[:8]}…`"},
                    {"type": "mrkdwn", "text": f"*Estimated Impact*\n{estimated_impact}"},
                    {"type": "mrkdwn", "text": f"*LLM Model*\n`{action.get('llm_model_used', 'unknown')}`"},
                ],
            },
            # ── Tool parameters ────────────────────────────────────────────
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Tool Parameters*\n```{params_str}```",
                },
            },
            # ── Reasoning (collapsed context) ──────────────────────────────
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"*LLM Reasoning:* {reasoning}"}
                ],
            },
            {"type": "divider"},
            # ── Rollback ───────────────────────────────────────────────────
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📖 Rollback Procedure*\n{rollback}",
                },
            },
            {"type": "divider"},
            # ── Action buttons ─────────────────────────────────────────────
            # NOTE: These buttons require a Slack App with Interactivity.
            # Without it, operators reply in-thread: "approve" or "reject".
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Approve", "emoji": True},
                        "style": "primary",
                        "value": f"approve:{action_id}",
                        "action_id": "hitl_approve",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Reject", "emoji": True},
                        "style": "danger",
                        "value": f"reject:{action_id}",
                        "action_id": "hitl_reject",
                    },
                ],
            },
            # ── Footer ─────────────────────────────────────────────────────
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"🤖 *OpsPulse AI* · "
                            f"Planned at `{action.get('planned_at', 'unknown')}`  · "
                            f"Reply *approve* or *reject* in thread if buttons are inactive."
                        ),
                    }
                ],
            },
        ]

        return {
            "text": (
                f"{tier_emoji} HITL APPROVAL REQUIRED: {risk_tier} risk action "
                f"for `{service}` — `{action.get('tool_name', 'unknown')}`"
            ),
            "blocks": blocks,
        }
