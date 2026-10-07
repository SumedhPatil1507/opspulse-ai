"""
Celery application factory.

Kept in its own module so both the FastAPI process (which only *sends*
tasks) and the Celery worker process (which *runs* tasks) can import the
same ``celery_app`` instance without pulling in FastAPI internals.

Usage
-----
Start a worker (from the project root)::

    celery -A src.worker.celery_app worker --loglevel=info -Q alerts

Inspect registered tasks::

    celery -A src.worker.celery_app inspect registered
"""

from __future__ import annotations

from celery import Celery

from src.api.core.config import get_settings

_settings = get_settings()

# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_celery_app() -> Celery:
    """
    Build and configure a Celery instance from application settings.

    The broker and result backend both point at Redis so we need only one
    external dependency in development / CI.  In production, swap the
    result backend for a dedicated store (e.g. PostgreSQL via
    celery[sqlalchemy]) by changing REDIS_URL or adding a separate
    CELERY_RESULT_BACKEND env var.
    """
    redis_url = str(_settings.REDIS_URL)

    app = Celery(
        # Main name — used as the default task module prefix.
        "opspulse_worker",
        broker=redis_url,
        backend=redis_url,
        # Auto-discover tasks from this package so we never need to import
        # them manually.
        include=["src.worker.tasks"],
    )

    app.conf.update(
        # Serialisation
        task_serializer=_settings.CELERY_TASK_SERIALIZER,
        result_serializer=_settings.CELERY_RESULT_SERIALIZER,
        accept_content=_settings.CELERY_ACCEPT_CONTENT,
        # Timezone — keep UTC everywhere.
        timezone="UTC",
        enable_utc=True,
        # Time limits
        task_soft_time_limit=_settings.CELERY_TASK_SOFT_TIME_LIMIT,
        task_time_limit=_settings.CELERY_TASK_TIME_LIMIT,
        # Route all alert tasks to the dedicated queue.
        task_routes={
            "src.worker.tasks.process_alert": {"queue": _settings.ALERT_QUEUE_NAME},
        },
        # Do not store successful results by default — keeps Redis lean.
        # Set to True if you need to poll task outcomes.
        task_ignore_result=False,
        # Retry failed broker connections up to 3 times on startup.
        broker_connection_retry_on_startup=True,
    )

    return app


# Module-level singleton — imported by tasks and the FastAPI route.
celery_app: Celery = create_celery_app()
