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
import json
import logging
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status

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


@router.post(
    "/ingest/batch",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Batch ingest incident alerts from JSON file",
    description=(
        "Accepts a JSON file containing one or more alert payloads and "
        "enqueues them for asynchronous processing via Celery. Returns 202 "
        "Accepted with details about successful and failed ingestions."
    ),
    responses={
        202: {"description": "Alerts accepted and queued for processing."},
        400: {"description": "Invalid JSON file format."},
        500: {"description": "Failed to enqueue alerts."},
    },
)
async def ingest_alerts_batch(
    file: UploadFile,
    celery: CeleryDep,
    settings: SettingsDep,
    request: Request,
) -> dict:
    """
    Validate and enqueue multiple alerts from a JSON file upload.

    Parameters
    ----------
    file:
        Uploaded JSON file containing alert payloads. Can be a single alert
        object or an array of alert objects.
    celery:
        Injected Celery application (swappable in tests).
    settings:
        Application settings; used to resolve the target queue name.
    request:
        Raw ASGI request, used for structured logging.

    Returns
    -------
    dict
        202 body containing ``total``, ``successful``, ``failed``, and ``details``.
    """
    # Read and parse JSON file
    try:
        contents = await file.read()
        data = json.loads(contents.decode("utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse JSON file: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON file format.",
        ) from exc
    except Exception as exc:
        logger.exception("Failed to read uploaded file: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to read uploaded file.",
        ) from exc

    # Normalize to list format
    if isinstance(data, dict):
        alerts = [data]
    elif isinstance(data, list):
        alerts = data
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JSON must be an object or array of alert objects.",
        )

    logger.info(
        "Batch ingest request: %d alerts from file=%s client=%s",
        len(alerts),
        file.filename,
        request.client.host if request.client else "unknown",
    )

    # Enqueue each alert
    successful = []
    failed = []

    for alert_dict in alerts:
        try:
            # Validate with Pydantic
            validated_alert = AlertPayload(**alert_dict)
            alert_payload = validated_alert.model_dump(mode="json")

            # Enqueue with timeout to prevent hanging on Redis connection issues
            try:
                async_result = await asyncio.wait_for(
                    asyncio.to_thread(
                        _enqueue_alert,
                        celery,
                        alert_payload,
                        settings.ALERT_QUEUE_NAME,
                    ),
                    timeout=2.0  # 2 second timeout
                )

                successful.append({
                    "alert_id": validated_alert.alert_id,
                    "tracking_id": async_result.id,
                    "status": "queued",
                })
                logger.info(
                    "Alert enqueued tracking_id=%s alert_id=%s",
                    async_result.id,
                    validated_alert.alert_id,
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout enqueuing alert_id=%s (Redis may be unavailable)", validated_alert.alert_id)
                failed.append({
                    "alert_id": validated_alert.alert_id,
                    "error": "Timeout: Redis connection unavailable. Please ensure Redis is running.",
                })
        except Exception as exc:
            alert_id = alert_dict.get("alert_id", "unknown")
            logger.exception("Failed to enqueue alert_id=%s: %s", alert_id, exc)
            failed.append({
                "alert_id": alert_id,
                "error": str(exc),
            })

    return {
        "total": len(alerts),
        "successful": len(successful),
        "failed": len(failed),
        "details": {
            "successful": successful,
            "failed": failed,
        },
    }


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
