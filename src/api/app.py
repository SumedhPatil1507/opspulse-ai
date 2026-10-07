"""
FastAPI application factory.

Keeping the factory in its own module (rather than in ``main.py``) means
test fixtures can call ``create_app()`` directly with custom overrides
without spawning a real server process.

Usage
-----
Run with uvicorn (development)::

    uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000

Or point uvicorn at the factory for programmatic config::

    uvicorn "src.api.app:create_app" --factory --reload
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.core.config import get_settings
from src.api.core.logging_config import configure_logging
from src.api.routes.alerts import router as alerts_router

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated on_event handlers)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """
    Async context manager that runs setup before the server accepts requests
    and teardown when it shuts down.
    """
    settings = get_settings()
    configure_logging(debug=settings.DEBUG)

    logger.info(
        "Starting %s v%s [debug=%s]",
        settings.APP_NAME,
        settings.APP_VERSION,
        settings.DEBUG,
    )

    # Future: warm up connection pools, run DB migrations, etc.
    yield

    logger.info("Shutting down %s", settings.APP_NAME)
    # Future: flush metrics, close async DB sessions, etc.


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

async def _validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    """
    Return a consistent, developer-friendly 422 body instead of FastAPI's
    default nested ``detail`` structure.
    """
    errors = [
        {
            "field": " → ".join(str(loc) for loc in err["loc"]),
            "message": err["msg"],
            "type": err["type"],
        }
        for err in exc.errors()
    ]
    logger.warning(
        "Validation error on %s %s: %s",
        request.method,
        request.url.path,
        errors,
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": "Payload validation failed.", "errors": errors},
    )


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    """
    Construct and return the FastAPI application instance.

    All routers, middleware, and exception handlers are registered here so
    the factory is the single source of truth for application structure.
    """
    settings = get_settings()

    application = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description=(
            "OpsPulse AI — incident alert ingestion and routing service. "
            "Accepts alert payloads, validates them, and queues them for "
            "async processing via Celery + Redis."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ── Middleware ─────────────────────────────────────────────────────────
    application.add_middleware(
        CORSMiddleware,
        # Tighten ``allow_origins`` in production via an env var.
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Exception handlers ─────────────────────────────────────────────────
    application.add_exception_handler(
        RequestValidationError,
        _validation_exception_handler,  # type: ignore[arg-type]
    )

    # ── Routers ────────────────────────────────────────────────────────────
    application.include_router(alerts_router)

    # ── Health check (no auth required) ───────────────────────────────────
    @application.get(
        "/healthz",
        tags=["Health"],
        summary="Liveness probe",
        response_description="Service is alive.",
    )
    async def health_check() -> dict:
        return {"status": "ok", "service": settings.APP_NAME}

    return application


# Module-level singleton — used by uvicorn and by tests via TestClient.
app: FastAPI = create_app()
