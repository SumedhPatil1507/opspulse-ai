"""
Alert ingestion route — POST /api/v1/alerts/ingest

Design decisions
----------------
* The route is fully async; it never blocks the event loop.
* Celery's ``.apply_async()`` is called via ``asyncio.to_thread`` because
  the Celery producer (kombu) uses blocking I/O under the hood.  This
  keeps the ASGI worker responsive under load.
* The route returns HTTP 202 Accepted immediately.  The caller can use
  the ``tracking_id`` (== Celery task ID) to poll ``/api/v1/alerts/status/{id}``
  (future endpoint) or subscribe to a webhook.
* Dependency injection is used for the Celery app so tests can swap it
  for a mock without monkey-patching module globals.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from src.api.core.config import Settings, get_settings
from src.api.models.alert import AlertIngestionResponse, AlertPayload
from src.worker.celery_app import celery_app as _default_celery_app
from src.worker.tasks import process_alert  # noqa: F401 — ensures task is registered

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/alerts",
    tags=["Alerts"],
)


# ---------------------------------------------------------------------------
# Dependency — injectable Celery app (makes unit-testing easy)
# ---------------------------------------------------------------------------

def get_celery_app():
    """Return the module-level Celery application instance."""
    return _default_celery_app


CeleryDep = Annotated[object, Depends(get_celery_app)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post(
    "/ingest",
    response_model=AlertIngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest an incident alert",
    description=(
        "Validates an incoming alert payload and enqueues it for asynchronous "
        "processing via Celery.  Returns 202 Accepted with a ``tracking_id`` "
        "that can be used to follow up on processing status."
    ),
    responses={
        202: {"description": "Alert accepted and queued for processing."},
        422: {"description": "Validation error — malformed payload."},
        500: {"description": "Failed to enqueue the alert."},
    },
)
async def ingest_alert(
    payload: AlertPayload,
    celery: CeleryDep,
    settings: SettingsDep,
    request: Request,
) -> AlertIngestionResponse:
    """
    Validate ``payload``, push it onto the Redis-backed Celery queue, and
    return a 202 response immediately.

    Parameters
    ----------
    payload:
        Pydantic-validated ``AlertPayload``.  FastAPI rejects requests that
        fail validation with a 422 before this function is ever called.
    celery:
        Injected Celery application (swappable in tests).
    settings:
        Application settings; used to resolve the target queue name.
    request:
        Raw ASGI request, used for structured logging (client IP, request ID).

    Returns
    -------
    AlertIngestionResponse
        202 body containing ``tracking_id``, ``status``, and ``message``.
    """
    # Serialise to a plain dict; Celery will JSON-encode it for the broker.
    alert_dict = payload.model_dump(mode="json")

    logger.info(
        "Received alert alert_id=%s service=%s severity=%s env=%s client=%s",
        payload.alert_id,
        payload.service_name,
        payload.severity,
        payload.environment,
        request.client.host if request.client else "unknown",
    )

    try:
        # apply_async uses blocking kombu I/O — offload to a thread pool so
        # we never stall the ASGI event loop.
        async_result = await asyncio.to_thread(
            _enqueue_alert,
            celery,
            alert_dict,
            settings.ALERT_QUEUE_NAME,
        )
        tracking_id: str = async_result.id
    except Exception as exc:
        logger.exception(
            "Failed to enqueue alert alert_id=%s: %s",
            payload.alert_id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to enqueue alert for processing. Please retry.",
        ) from exc

    logger.info(
        "Alert enqueued tracking_id=%s alert_id=%s queue=%s",
        tracking_id,
        payload.alert_id,
        settings.ALERT_QUEUE_NAME,
    )

    return AlertIngestionResponse(
        tracking_id=tracking_id,
        status="queued",
        message=(
            f"Alert '{payload.alert_id}' accepted and queued for processing."
        ),
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _enqueue_alert(
    celery_instance,
    alert_dict: dict,
    queue_name: str,
):
    """
    Synchronous helper that calls ``apply_async``.

    Isolated into its own function so ``asyncio.to_thread`` receives a
    clean callable without a closure over coroutine state.
    """
    return celery_instance.send_task(
        "src.worker.tasks.process_alert",
        args=[alert_dict],
        queue=queue_name,
        # Attach a correlation ID for distributed tracing.
        task_id=str(uuid.uuid4()),
    )
