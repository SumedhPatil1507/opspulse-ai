"""
Runbook management routes — POST /api/v1/runbooks/upload

Provides endpoints for uploading and managing runbook files that are
indexed by the RAG system.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status

from src.api.core.config import Settings, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/runbooks",
    tags=["Runbooks"],
)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

SettingsDep = Annotated[Settings, Depends(get_settings)]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post(
    "/upload",
    status_code=status.HTTP_201_CREATED,
    summary="Upload a runbook file",
    description=(
        "Accepts a Markdown runbook file and saves it to the configured "
        "runbooks directory. The file will be indexed automatically on the "
        "next RAG system restart or can be manually reindexed."
    ),
    responses={
        201: {"description": "Runbook uploaded successfully."},
        400: {"description": "Invalid file or filename."},
        500: {"description": "Failed to save runbook."},
    },
)
async def upload_runbook(
    file: UploadFile,
    settings: SettingsDep,
) -> dict:
    """
    Save an uploaded runbook file to the runbooks directory.

    Parameters
    ----------
    file:
        Uploaded Markdown file containing runbook content.
    settings:
        Application settings; used to resolve the runbooks directory path.

    Returns
    -------
    dict
        201 body containing ``filename``, ``path``, and ``message``.
    """
    # Validate file extension
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required.",
        )

    filename = file.filename
    if not (filename.endswith(".md") or filename.endswith(".markdown")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a Markdown file (.md or .markdown).",
        )

    # Ensure runbooks directory exists
    runbooks_dir = Path(settings.RUNBOOKS_DIR)
    try:
        runbooks_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.exception("Failed to create runbooks directory: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create runbooks directory.",
        ) from exc

    # Save file
    file_path = runbooks_dir / filename
    try:
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
    except Exception as exc:
        logger.exception("Failed to save runbook file: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to save runbook file.",
        ) from exc

    logger.info("Runbook uploaded: filename=%s path=%s", filename, file_path)

    return {
        "filename": filename,
        "path": str(file_path),
        "message": (
            f"Runbook '{filename}' uploaded successfully. "
            "It will be indexed on the next RAG system restart."
        ),
    }


@router.get(
    "/list",
    summary="List available runbooks",
    description=(
        "Returns a list of all runbook files currently available in the "
        "runbooks directory."
    ),
    responses={
        200: {"description": "List of runbooks retrieved successfully."},
    },
)
async def list_runbooks(settings: SettingsDep) -> dict:
    """
    List all runbook files in the configured runbooks directory.

    Parameters
    ----------
    settings:
        Application settings; used to resolve the runbooks directory path.

    Returns
    -------
    dict
        200 body containing ``count`` and ``runbooks`` (list of filenames).
    """
    runbooks_dir = Path(settings.RUNBOOKS_DIR)
    
    if not runbooks_dir.exists():
        return {
            "count": 0,
            "runbooks": [],
            "message": "Runbooks directory does not exist yet.",
        }

    try:
        runbook_files = [
            f.name
            for f in runbooks_dir.iterdir()
            if f.is_file() and (f.suffix == ".md" or f.suffix == ".markdown")
        ]
    except Exception as exc:
        logger.exception("Failed to list runbooks: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list runbooks.",
        ) from exc

    return {
        "count": len(runbook_files),
        "runbooks": runbook_files,
    }
