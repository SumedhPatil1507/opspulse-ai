"""
Dual ingestion file-upload routes.

``POST /api/v1/logs/upload``
    Accepts raw ``.log`` / ``.txt`` stack-trace files **and** Markdown
    ``.md`` infrastructure runbooks in a single drag-and-drop request:

    * ``.log`` / ``.txt`` → runs the LangGraph triage workflow and returns
      the standardized ``RemediationReport`` JSON
      (``verdict`` / ``reasoning`` / ``recommendation`` / ``next_steps``).
    * ``.md`` / ``.markdown`` → saved into ``RUNBOOKS_DIR`` and indexed
      **on the fly** into the Qdrant vector collection so the next triage
      immediately benefits from the new runbook.

``POST /api/v1/triage``
    Synchronous triage for a JSON alert payload — same standardized output
    schema as the log-upload path, guaranteeing one contract for all
    incident triage responses.

Design decisions
----------------
* The triage runner and the Qdrant indexer are FastAPI *dependencies* so
  tests can override them without monkey-patching (same pattern as the
  Celery dependency in ``alerts.py``).
* Heavy work (LangGraph invoke, embedding) runs in ``asyncio.to_thread``
  so the event loop never blocks.
* Qdrant/SentenceTransformer unavailability degrades to a clear 503 with
  remediation hints instead of a stack trace.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Callable

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status

from src.agent.reporting import build_remediation_report
from src.agent.workflow import run_workflow
from src.api.core.config import Settings, get_settings
from src.api.models.remediation import RemediationReport

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["Ingestion"],
)

# Allowed extensions per file class
_LOG_SUFFIXES = {".log", ".txt"}
_RUNBOOK_SUFFIXES = {".md", ".markdown"}
_MAX_FILE_BYTES = 2 * 1024 * 1024  # 2 MB per file — plenty for stack traces


# ---------------------------------------------------------------------------
# Dependencies (overridable in tests)
# ---------------------------------------------------------------------------

def get_triage_runner() -> Callable[[dict[str, Any]], dict[str, Any]]:
    """
    Return the function used to run the LangGraph triage workflow.

    Default is ``run_workflow``; tests override via
    ``app.dependency_overrides[get_triage_runner]``.
    """
    return run_workflow


TriageRunnerDep = Annotated[
    Callable[[dict[str, Any]], dict[str, Any]], Depends(get_triage_runner)
]


def get_runbook_indexer() -> Callable[[Path, Settings], dict[str, Any]]:
    """
    Return the callable that indexes a single Markdown runbook into Qdrant.

    Default implementation chunks → embeds → upserts; tests override it.
    """
    return index_runbook_into_qdrant


IndexerDep = Annotated[
    Callable[[Path, Settings], dict[str, Any]], Depends(get_runbook_indexer)
]


def index_runbook_into_qdrant(path: Path, settings: Settings) -> dict[str, Any]:
    """
    On-the-fly runbook indexing: chunk the uploaded file, embed with the
    dense bi-encoder, and upsert into the configured Qdrant collection.

    Returns
    -------
    dict with ``indexed_chunks``, ``collection``, and ``indexed`` flags.
    """
    from qdrant_client import QdrantClient
    from sentence_transformers import SentenceTransformer

    from src.rag.chunker import RunbookChunker
    from src.rag.indexer import RunbookIndexer

    chunker = RunbookChunker(
        runbooks_dir=path.parent,
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
    )
    chunks = chunker.chunk_file(path)
    if not chunks:
        return {
            "indexed": False,
            "indexed_chunks": 0,
            "collection": settings.QDRANT_COLLECTION,
        }

    client = QdrantClient(
        host=settings.QDRANT_HOST,
        port=settings.QDRANT_PORT,
        api_key=settings.QDRANT_API_KEY,
        timeout=10,
    )
    encoder = SentenceTransformer(settings.DENSE_MODEL_NAME)
    indexer = RunbookIndexer(client=client, encoder=encoder, settings=settings)
    indexer.ensure_collection()
    indexer.upsert_chunks(chunks)

    return {
        "indexed": True,
        "indexed_chunks": len(chunks),
        "collection": settings.QDRANT_COLLECTION,
        "total_points": indexer.count(),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_alert_from_log(text: str, filename: str) -> dict[str, Any]:
    """
    Build an alert payload dict from an uploaded log file.

    The payload mirrors ``AlertPayload``'s field names so the workflow's
    parse/retrieve/plan nodes see the same contract as the Kafka and HTTP
    ingestion paths.
    """
    lowered = text.lower()
    if any(k in lowered for k in ("critical", "fatal", "oomkilled", "panic")):
        severity = "critical"
    elif any(k in lowered for k in ("error", "exception", "traceback")):
        severity = "high"
    else:
        severity = "medium"

    # Production alerts must be high/critical (matches AlertPayload rule).
    return {
        "alert_id": f"upload-{uuid.uuid4().hex[:12]}",
        "service_name": Path(filename).stem or "uploaded-service",
        "severity": severity,
        "raw_log_stacktrace": text,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "environment": "production",
    }


async def _read_text_upload(file: UploadFile) -> str:
    """Read + size-check an uploaded file, returning decoded UTF-8 text."""
    content = await file.read()
    if len(content) > _MAX_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"File '{file.filename}' exceeds "
                f"{_MAX_FILE_BYTES // (1024 * 1024)} MB limit."
            ),
        )
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File '{file.filename}' is not valid UTF-8 text.",
        ) from exc


# ---------------------------------------------------------------------------
# POST /api/v1/logs/upload — drag-and-drop log & runbook ingestion
# ---------------------------------------------------------------------------

SettingsDep = Annotated[Settings, Depends(get_settings)]


@router.post(
    "/logs/upload",
    status_code=status.HTTP_200_OK,
    summary="Upload log files (.log/.txt) and runbooks (.md)",
    description=(
        "Dual ingestion endpoint. Stack-trace files are triaged through the "
        "LangGraph agent and return the standardized RemediationReport "
        "(verdict / reasoning / recommendation / next_steps). Markdown "
        "runbooks are persisted and indexed into Qdrant on the fly."
    ),
    responses={
        200: {"description": "Triage report and/or runbook index results."},
        400: {"description": "No supported files or unreadable content."},
        413: {"description": "File exceeds size limit."},
        503: {"description": "Vector index unavailable for runbook ingestion."},
    },
)
async def upload_logs_and_runbooks(
    request: Request,
    files: list[UploadFile] = File(...),
    settings: SettingsDep = None,  # type: ignore[assignment]
    triage_runner: TriageRunnerDep = None,  # type: ignore[assignment]
    indexer: IndexerDep = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """
    Accept multipart uploads, dispatch by extension, and respond with:

    * ``triage`` — the standardized ``RemediationReport`` (when at least one
      ``.log`` / ``.txt`` file was uploaded), and/or
    * ``runbooks`` — per-file Qdrant index results (when ``.md`` files were
      uploaded).
    """
    settings = settings or get_settings()

    log_files: list[tuple[str, str]] = []      # (filename, text)
    runbook_files: list[tuple[str, str]] = []  # (filename, text)
    rejected: list[dict[str, str]] = []

    for file in files:
        suffix = Path(file.filename or "").suffix.lower()
        if suffix in _LOG_SUFFIXES:
            text = await _read_text_upload(file)
            log_files.append((file.filename or "unnamed.log", text))
        elif suffix in _RUNBOOK_SUFFIXES:
            text = await _read_text_upload(file)
            runbook_files.append((file.filename or "unnamed.md", text))
        else:
            rejected.append({
                "filename": file.filename or "unknown",
                "error": (
                    f"Unsupported extension '{suffix}'. "
                    f"Allowed: {sorted(_LOG_SUFFIXES | _RUNBOOK_SUFFIXES)}"
                ),
            })

    if not log_files and not runbook_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "No supported files in upload.",
                "rejected": rejected,
            },
        )

    response: dict[str, Any] = {}
    if rejected:
        response["rejected"] = rejected

    # ── Runbooks: persist + index into Qdrant on the fly ───────────────────
    if runbook_files:
        runbooks_dir = Path(settings.RUNBOOKS_DIR)
        runbooks_dir.mkdir(parents=True, exist_ok=True)
        runbook_results = []
        for filename, text in runbook_files:
            safe_name = Path(filename).name
            dest = runbooks_dir / safe_name
            dest.write_text(text, encoding="utf-8")

            try:
                index_result = await asyncio.to_thread(indexer, dest, settings)
            except Exception as exc:
                logger.exception("Qdrant indexing failed for %s: %s", safe_name, exc)
                # File IS saved; surface 503 so clients know it is not yet
                # searchable and can retry once Qdrant is reachable.
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=(
                        f"Runbook '{safe_name}' saved to {dest} but Qdrant "
                        f"indexing failed: {exc}. Ensure Qdrant is reachable "
                        f"at {settings.QDRANT_HOST}:{settings.QDRANT_PORT}."
                    ),
                ) from exc

            runbook_results.append(
                {"filename": safe_name, "path": str(dest), **index_result}
            )
        response["runbooks"] = runbook_results

    # ── Logs: run LangGraph triage → standardized RemediationReport ───────
    if log_files:
        combined_text = "\n\n".join(
            f"===== {name} =====\n{text}" for name, text in log_files
        )
        alert_payload = build_alert_from_log(combined_text, log_files[0][0])

        try:
            state = await asyncio.to_thread(triage_runner, alert_payload)
        except Exception as exc:
            logger.exception("Triage workflow failed for upload: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Triage workflow failed: {exc}",
            ) from exc

        report = build_remediation_report(state)
        response["triage"] = report.model_dump()
        response["workflow_status"] = str(state.get("workflow_status"))
        response["approval_status"] = str(state.get("approval_status"))

    logger.info(
        "Upload processed by %s: logs=%d runbooks=%d rejected=%d",
        request.client.host if request.client else "unknown",
        len(log_files), len(runbook_files), len(rejected),
    )
    return response


# ---------------------------------------------------------------------------
# POST /api/v1/triage — JSON triage with the same standardized schema
# ---------------------------------------------------------------------------

@router.post(
    "/triage",
    response_model=RemediationReport,
    status_code=status.HTTP_200_OK,
    summary="Run incident triage and return the standardized remediation JSON",
    description=(
        "Executes the LangGraph triage workflow for a raw stack trace and "
        "returns exactly {verdict, reasoning, recommendation, next_steps} — "
        "the single contract shared by every triage API response."
    ),
)
async def triage_alert(
    payload: dict[str, Any],
    triage_runner: TriageRunnerDep = None,  # type: ignore[assignment]
) -> RemediationReport:
    """
    Accept an alert-shaped JSON body (``raw_log_stacktrace`` required,
    other ``AlertPayload`` fields optional) and return the standardized
    ``RemediationReport``.

    Example body::

        {"raw_log_stacktrace": "java.lang.OutOfMemoryError: Java heap space\\n..."}
    """
    raw = payload.get("raw_log_stacktrace")
    if not raw or not isinstance(raw, str):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="'raw_log_stacktrace' (non-empty string) is required.",
        )

    alert_payload = build_alert_from_log(
        raw,
        str(payload.get("service_name") or "triage-request"),
    )
    # Honour caller-supplied overrides where valid.
    if payload.get("alert_id"):
        alert_payload["alert_id"] = str(payload["alert_id"])
    if payload.get("severity") in ("high", "critical"):
        alert_payload["severity"] = payload["severity"]
    if payload.get("environment") in ("development", "staging", "production"):
        alert_payload["environment"] = payload["environment"]

    try:
        state = await asyncio.to_thread(triage_runner, alert_payload)
    except Exception as exc:
        logger.exception("Triage workflow failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Triage workflow failed: {exc}",
        ) from exc

    return build_remediation_report(state)
