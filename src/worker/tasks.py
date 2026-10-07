"""
Celery tasks for alert processing.

Each task receives a plain dict (JSON-serialisable) representation of an
AlertPayload so the message payload stays broker-agnostic.  Tasks
re-validate the payload via Pydantic before acting on it, providing a
second line of defence against malformed data that somehow bypassed the
HTTP layer.

Adding new processing stages
-----------------------------
1. Implement a new task in this file (or a sub-module).
2. Chain it from ``process_alert`` using Celery's ``chain()`` or ``chord()``.
3. Register the queue route in ``celery_app.py`` if you want queue isolation.
"""

from __future__ import annotations

import logging
from typing import Any

from celery import Task
from celery.exceptions import MaxRetriesExceededError

from src.api.models.alert import AlertPayload
from src.worker.celery_app import celery_app

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 5  # seconds; actual delay = base * 2^retry_count


# ---------------------------------------------------------------------------
# Task definition
# ---------------------------------------------------------------------------

@celery_app.task(
    name="src.worker.tasks.process_alert",
    bind=True,                  # gives access to ``self`` (the Task instance)
    max_retries=_MAX_RETRIES,
    acks_late=True,             # acknowledge only after the task completes
    reject_on_worker_lost=True, # re-queue if the worker crashes mid-task
)
def process_alert(self: Task, alert_data: dict[str, Any]) -> dict[str, Any]:
    """
    Primary alert processing task.

    Parameters
    ----------
    alert_data:
        JSON-serialisable dict produced by ``AlertPayload.model_dump(mode="json")``.

    Returns
    -------
    dict
        A summary result dict stored in the Celery result backend.

    Raises
    ------
    Retries the task with exponential back-off on transient failures.
    Raises ``MaxRetriesExceededError`` after ``_MAX_RETRIES`` attempts.
    """
    logger.info(
        "Processing alert task_id=%s alert_id=%s service=%s severity=%s",
        self.request.id,
        alert_data.get("alert_id"),
        alert_data.get("service_name"),
        alert_data.get("severity"),
    )

    try:
        # Re-validate to catch any corruption between producer and consumer.
        alert = AlertPayload.model_validate(alert_data)

        # ── Processing pipeline ──────────────────────────────────────────
        # Placeholder: replace / extend each stage with real integrations
        # (e.g. PagerDuty, Slack, Elasticsearch, anomaly detection model).

        result = _enrich_alert(alert)
        _route_alert(alert, result)

        logger.info(
            "Alert processed successfully alert_id=%s tracking_id=%s",
            alert.alert_id,
            self.request.id,
        )

        return {
            "task_id": self.request.id,
            "alert_id": alert.alert_id,
            "status": "processed",
            "severity": alert.severity,
            "environment": alert.environment,
        }

    except Exception as exc:
        logger.warning(
            "Alert processing failed (attempt %d/%d): %s",
            self.request.retries + 1,
            _MAX_RETRIES + 1,
            exc,
            exc_info=True,
        )
        try:
            # Exponential back-off: 5 s, 10 s, 20 s
            raise self.retry(
                exc=exc,
                countdown=_RETRY_BACKOFF_BASE * (2 ** self.request.retries),
            )
        except MaxRetriesExceededError:
            logger.error(
                "Max retries exceeded for alert_id=%s. Sending to DLQ.",
                alert_data.get("alert_id"),
            )
            raise


# ---------------------------------------------------------------------------
# Internal helpers  (private — not registered as Celery tasks)
# ---------------------------------------------------------------------------

def _enrich_alert(alert: AlertPayload) -> dict[str, Any]:
    """
    Enrich the alert with derived metadata.

    Extend this function to call external enrichment APIs (CMDB lookups,
    owner resolution, SLA classification, etc.).
    """
    return {
        "alert_id": alert.alert_id,
        "service_name": alert.service_name,
        "severity": alert.severity,
        "environment": alert.environment,
        # Derived field: flag for immediate paging
        "requires_immediate_page": alert.severity in ("high", "critical"),
    }


def _route_alert(alert: AlertPayload, enriched: dict[str, Any]) -> None:
    """
    Route the alert to the appropriate notification channel.

    Extend this function to dispatch to PagerDuty, Slack, email, etc.
    """
    if enriched.get("requires_immediate_page"):
        logger.info(
            "[ROUTE] Paging on-call for CRITICAL/HIGH alert: service=%s env=%s",
            alert.service_name,
            alert.environment,
        )
    else:
        logger.info(
            "[ROUTE] Logging non-critical alert: service=%s severity=%s",
            alert.service_name,
            alert.severity,
        )
