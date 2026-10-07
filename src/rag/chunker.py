"""
Markdown runbook loader and text chunker.

Responsibilities
----------------
1. Discover all ``*.md`` files under the configured runbooks directory.
2. Parse each file into logical sections using Markdown heading boundaries
   (H1/H2/H3) so chunks stay semantically coherent — a chunk always belongs
   to exactly one section.
3. Further split oversized sections by character count with overlap so the
   dense encoder never receives a passage longer than its context window.
4. Return a flat list of ``RunbookChunk`` objects ready to be embedded and
   indexed.

Design notes
------------
* Pure-Python — no heavy Markdown AST library needed.  We split on heading
  patterns with ``re`` which is fast and dependency-free.
* ``RunbookChunk`` is a plain dataclass so it serialises trivially to/from
  JSON for caching or debugging.
* Heading hierarchy is preserved in ``section_path`` (e.g. "Root > H2 > H3")
  which is stored as Qdrant payload metadata and used in re-ranking prompts.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

# Matches Markdown headings: # Heading, ## Sub, ### Sub-sub
_HEADING_RE = re.compile(r"^(#{1,3})\s+(.+)$", re.MULTILINE)


@dataclass
class RunbookChunk:
    """
    A single indexed passage from a runbook file.

    Attributes
    ----------
    chunk_id        : SHA-256 hex digest of (source_file + chunk_index).
                      Stable across re-runs — same content → same ID.
    source_file     : Filename of the originating Markdown file (stem only).
    source_path     : Absolute path of the Markdown file.
    section_path    : Heading breadcrumb trail, e.g. "OOMKilled > Remediation".
    text            : Raw chunk text (stripped of leading/trailing whitespace).
    chunk_index     : 0-based position of this chunk within its source file.
    char_count      : Length of ``text`` in characters.
    token_estimate  : Rough token count (char_count // 4).
    """

    chunk_id: str
    source_file: str
    source_path: str
    section_path: str
    text: str
    chunk_index: int
    char_count: int = field(init=False)
    token_estimate: int = field(init=False)

    def __post_init__(self) -> None:
        self.char_count = len(self.text)
        self.token_estimate = self.char_count // 4

    def to_payload(self) -> dict:
        """Return a JSON-serialisable dict for Qdrant point payload."""
        return {
            "chunk_id": self.chunk_id,
            "source_file": self.source_file,
            "source_path": self.source_path,
            "section_path": self.section_path,
            "text": self.text,
            "chunk_index": self.chunk_index,
            "char_count": self.char_count,
            "token_estimate": self.token_estimate,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "RunbookChunk":
        """Reconstruct a RunbookChunk from a Qdrant point payload dict."""
        obj = cls.__new__(cls)
        obj.chunk_id = payload["chunk_id"]
        obj.source_file = payload["source_file"]
        obj.source_path = payload["source_path"]
        obj.section_path = payload["section_path"]
        obj.text = payload["text"]
        obj.chunk_index = payload["chunk_index"]
        obj.char_count = payload.get("char_count", len(obj.text))
        obj.token_estimate = payload.get("token_estimate", obj.char_count // 4)
        return obj


class RunbookChunker:
    """
    Loads all Markdown runbooks from a directory and splits them into
    semantically-bounded chunks.

    Parameters
    ----------
    runbooks_dir : Path or str pointing at the directory of ``*.md`` files.
    chunk_size   : Maximum character length of a single chunk.
    chunk_overlap: Character overlap between consecutive chunks of the same
                   section (helps preserve cross-sentence context).
    """

    def __init__(
        self,
        runbooks_dir: str | Path,
        chunk_size: int = 512,
        chunk_overlap: int = 64,
    ) -> None:
        self.runbooks_dir = Path(runbooks_dir).resolve()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        if not self.runbooks_dir.exists():
            raise FileNotFoundError(
                f"Runbooks directory not found: {self.runbooks_dir}"
            )

    # ── Public API ─────────────────────────────────────────────────────────

    def load_all(self) -> list[RunbookChunk]:
        """
        Discover and chunk every ``*.md`` file in ``runbooks_dir``.

        Returns a flat list of ``RunbookChunk`` objects sorted by
        (source_file, chunk_index).
        """
        md_files = sorted(self.runbooks_dir.glob("*.md"))
        if not md_files:
            logger.warning("No Markdown files found in %s", self.runbooks_dir)
            return []

        all_chunks: list[RunbookChunk] = []
        for md_path in md_files:
            try:
                file_chunks = list(self._process_file(md_path))
                logger.info(
                    "Chunked %s → %d chunks", md_path.name, len(file_chunks)
                )
                all_chunks.extend(file_chunks)
            except Exception:
                logger.exception("Failed to process runbook: %s", md_path)

        logger.info(
            "Total chunks across %d runbooks: %d",
            len(md_files),
            len(all_chunks),
        )
        return all_chunks

    # ── Internal helpers ───────────────────────────────────────────────────

    def _process_file(self, path: Path) -> Iterator[RunbookChunk]:
        """Yield chunks for a single Markdown file."""
        raw_text = path.read_text(encoding="utf-8")
        sections = self._split_by_headings(raw_text)

        global_chunk_index = 0
        for section_path, section_text in sections:
            for chunk_text in self._split_text(section_text):
                chunk_text = chunk_text.strip()
                if not chunk_text:
                    continue

                chunk_id = self._make_chunk_id(path.stem, global_chunk_index)
                yield RunbookChunk(
                    chunk_id=chunk_id,
                    source_file=path.stem,
                    source_path=str(path),
                    section_path=section_path,
                    text=chunk_text,
                    chunk_index=global_chunk_index,
                )
                global_chunk_index += 1

    def _split_by_headings(
        self, text: str
    ) -> list[tuple[str, str]]:
        """
        Split ``text`` on Markdown headings (H1–H3).

        Returns a list of (section_path, section_body) pairs.  The
        ``section_path`` is a " > "-joined breadcrumb of heading titles.
        Content before the first heading goes into a synthetic "Preamble"
        section.
        """
        matches = list(_HEADING_RE.finditer(text))
        if not matches:
            return [("Document", text)]

        sections: list[tuple[str, str]] = []
        heading_stack: list[tuple[int, str]] = []  # (level, title)

        # Text before first heading
        preamble = text[: matches[0].start()].strip()
        if preamble:
            sections.append(("Preamble", preamble))

        for i, match in enumerate(matches):
            level = len(match.group(1))   # number of '#' chars
            title = match.group(2).strip()

            # Maintain a breadcrumb stack: pop entries at same/lower priority
            while heading_stack and heading_stack[-1][0] >= level:
                heading_stack.pop()
            heading_stack.append((level, title))

            section_path = " > ".join(t for _, t in heading_stack)

            # Body = text between this heading and the next
            start = match.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[start:end].strip()

            if body:
                sections.append((section_path, body))

        return sections

    def _split_text(self, text: str) -> list[str]:
        """
        Split ``text`` into chunks of at most ``chunk_size`` characters
        with ``chunk_overlap`` character overlap.

        Tries to split on paragraph boundaries (double newlines) first so
        chunks don't break mid-sentence.  Falls back to hard character split
        if a single paragraph exceeds ``chunk_size``.
        """
        if len(text) <= self.chunk_size:
            return [text]

        # Try paragraph-aware splitting first
        paragraphs = re.split(r"\n{2,}", text)
        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for para in paragraphs:
            para_len = len(para)

            if current_len + para_len > self.chunk_size and current:
                chunk_text = "\n\n".join(current)
                chunks.append(chunk_text)

                # Carry-over the last paragraph as overlap
                overlap_para = current[-1] if current else ""
                current = [overlap_para] if len(overlap_para) <= self.chunk_overlap else []
                current_len = sum(len(p) for p in current)

            if para_len > self.chunk_size:
                # Hard-split oversized single paragraphs
                for hard_chunk in self._hard_split(para):
                    chunks.append(hard_chunk)
                current = []
                current_len = 0
            else:
                current.append(para)
                current_len += para_len

        if current:
            chunks.append("\n\n".join(current))

        return [c for c in chunks if c.strip()]

    def _hard_split(self, text: str) -> list[str]:
        """Character-level split with overlap for paragraphs exceeding chunk_size."""
        chunks = []
        start = 0
        while start < len(text):
            end = start + self.chunk_size
            chunks.append(text[start:end])
            start += self.chunk_size - self.chunk_overlap
        return chunks

    @staticmethod
    def _make_chunk_id(source_stem: str, index: int) -> str:
        """Stable SHA-256 chunk ID from (source file stem, chunk index)."""
        raw = f"{source_stem}::{index}"
        return hashlib.sha256(raw.encode()).hexdigest()
