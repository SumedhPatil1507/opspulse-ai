"""
BM25 sparse retrieval scorer for runbook chunks.

Architecture
------------
BM25 (Best Match 25) is a probabilistic term-frequency ranking function that
scores documents by how well their term frequencies match a query, penalising
very long documents and rewarding term exclusivity (IDF).

This module provides ``BM25SparseScorer``, a wrapper around ``rank_bm25``
that:

1. Builds a corpus index from a list of ``RunbookChunk`` objects.
2. Tokenises text with a lightweight, punctuation-aware tokeniser that strips
   Markdown artefacts (code fences, backticks, headings) so BM25 scores on
   prose tokens, not syntax noise.
3. Exposes ``query(text, top_k)`` → ranked ``(RunbookChunk, bm25_score)``
   pairs that can be fused with dense results in the hybrid retriever.
4. Provides ``get_normalised_scores(text)`` → ``dict[chunk_id, float]`` for
   smooth Reciprocal Rank Fusion (RRF): scores are min-max normalised to
   [0, 1] so they combine on a common scale with cosine similarity scores
   from Qdrant.

Design decisions
----------------
* ``rank_bm25.BM25Okapi`` — the standard Okapi BM25 variant (k1=1.5, b=0.75).
  These constants are tunable via constructor args.
* Tokenisation is intentionally simple (lowercase + regex split on non-alphanum)
  to stay dependency-free.  Swap ``_tokenise`` for a spaCy/NLTK pipeline if
  stopword removal or stemming is needed.
* The index is built eagerly in ``__init__`` so query latency is O(V) where V
  is the vocabulary size — acceptable for O(100–10 000) runbook chunks.
* Thread-safe for reads after construction.  Re-indexing requires creating a
  new instance (immutable index).
"""

from __future__ import annotations

import logging
import re
import string
from typing import Sequence

from rank_bm25 import BM25Okapi

from src.rag.chunker import RunbookChunk

logger = logging.getLogger(__name__)

# Characters and patterns we strip before tokenising
_MARKDOWN_NOISE_RE = re.compile(
    r"```.*?```"          # fenced code blocks
    r"|`[^`]+`"           # inline code
    r"|#+\s+"             # heading markers
    r"|\*{1,3}"           # bold / italic markers
    r"|_{1,3}"            # underscore bold / italic
    r"|\[([^\]]+)\]\([^)]+\)",  # [text](url) → keep text
    re.DOTALL,
)
_PUNCT_RE = re.compile(r"[^\w\s]")          # non-word, non-space
_WHITESPACE_RE = re.compile(r"\s+")


def _tokenise(text: str) -> list[str]:
    """
    Lightweight Markdown-aware tokeniser.

    Pipeline:
    1. Strip Markdown syntax (code fences, heading markers, links).
    2. Lowercase.
    3. Remove punctuation.
    4. Split on whitespace.
    5. Drop single-character tokens and pure-digit tokens to reduce noise.
    """
    # Step 1 — strip markdown artefacts, keep link text via backreference
    cleaned = _MARKDOWN_NOISE_RE.sub(lambda m: m.group(1) or " ", text)
    # Step 2 — lowercase
    cleaned = cleaned.lower()
    # Step 3 — remove punctuation
    cleaned = _PUNCT_RE.sub(" ", cleaned)
    # Step 4 — normalise whitespace and split
    tokens = _WHITESPACE_RE.sub(" ", cleaned).strip().split()
    # Step 5 — filter noise tokens
    tokens = [
        t for t in tokens
        if len(t) > 1 and not t.isdigit()
    ]
    return tokens


class BM25SparseScorer:
    """
    Corpus-level BM25 scorer over a fixed set of ``RunbookChunk`` objects.

    Parameters
    ----------
    chunks : Sequence of ``RunbookChunk`` to index.
    k1     : BM25 term-frequency saturation parameter (default 1.5).
    b      : BM25 document-length normalisation parameter (default 0.75).

    Raises
    ------
    ValueError : If ``chunks`` is empty.

    Examples
    --------
    >>> scorer = BM25SparseScorer(chunks)
    >>> results = scorer.query("connection pool exhausted timeout", top_k=5)
    >>> for chunk, score in results:
    ...     print(f"{score:.4f}  {chunk.source_file}  {chunk.section_path}")
    """

    def __init__(
        self,
        chunks: Sequence[RunbookChunk],
        k1: float = 1.5,
        b: float = 0.75,
    ) -> None:
        if not chunks:
            raise ValueError(
                "BM25SparseScorer requires at least one chunk to index."
            )

        self._chunks: list[RunbookChunk] = list(chunks)
        self._k1 = k1
        self._b = b

        logger.info(
            "Building BM25 index over %d chunks (k1=%.2f, b=%.2f)",
            len(self._chunks),
            k1,
            b,
        )

        tokenised_corpus = [_tokenise(chunk.text) for chunk in self._chunks]
        self._bm25 = BM25Okapi(tokenised_corpus, k1=k1, b=b)

        logger.info("BM25 index ready. Vocabulary size: %d", len(self._bm25.idf))

    # ── Public API ─────────────────────────────────────────────────────────

    def query(
        self,
        query_text: str,
        top_k: int = 10,
    ) -> list[tuple[RunbookChunk, float]]:
        """
        Return the top-k chunks most relevant to ``query_text`` by BM25 score.

        Parameters
        ----------
        query_text : Raw query string (e.g. an error stacktrace excerpt).
        top_k      : Number of results to return.

        Returns
        -------
        List of ``(RunbookChunk, bm25_score)`` sorted by descending score.
        Zero-score chunks are excluded.
        """
        query_tokens = _tokenise(query_text)
        if not query_tokens:
            logger.warning("BM25 query produced no tokens after tokenisation.")
            return []

        scores: list[float] = self._bm25.get_scores(query_tokens).tolist()

        # Pair each chunk with its score, filter zeros, sort descending
        scored = [
            (chunk, score)
            for chunk, score in zip(self._chunks, scores)
            if score > 0.0
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    def get_normalised_scores(
        self,
        query_text: str,
    ) -> dict[str, float]:
        """
        Return a ``{chunk_id: normalised_score}`` mapping for ALL chunks.

        Scores are min-max normalised to [0.0, 1.0] so they can be compared
        directly with cosine similarity scores from Qdrant during RRF fusion.

        Chunks with a raw score of 0 map to 0.0.

        Parameters
        ----------
        query_text : Raw query string.

        Returns
        -------
        Dict keyed by ``chunk_id``.
        """
        query_tokens = _tokenise(query_text)
        if not query_tokens:
            return {chunk.chunk_id: 0.0 for chunk in self._chunks}

        raw_scores: list[float] = self._bm25.get_scores(query_tokens).tolist()

        # Min-max normalise
        max_score = max(raw_scores) if raw_scores else 0.0
        min_score = min(s for s in raw_scores if s > 0.0) if any(
            s > 0 for s in raw_scores
        ) else 0.0

        score_range = max_score - min_score

        normalised: dict[str, float] = {}
        for chunk, raw in zip(self._chunks, raw_scores):
            if raw <= 0.0:
                normalised[chunk.chunk_id] = 0.0
            elif score_range == 0.0:
                normalised[chunk.chunk_id] = 1.0
            else:
                normalised[chunk.chunk_id] = (raw - min_score) / score_range

        return normalised

    def get_scores_for_chunks(
        self,
        query_text: str,
        chunk_ids: list[str],
    ) -> dict[str, float]:
        """
        Return raw BM25 scores for a specific subset of chunk IDs.

        Useful when the hybrid retriever needs to score only the union of
        dense + sparse candidate sets rather than the full corpus.

        Parameters
        ----------
        query_text : Raw query string.
        chunk_ids  : List of ``chunk_id`` strings to score.

        Returns
        -------
        Dict ``{chunk_id: raw_bm25_score}``.  Unknown IDs map to 0.0.
        """
        all_normalised = self.get_normalised_scores(query_text)
        return {cid: all_normalised.get(cid, 0.0) for cid in chunk_ids}

    # ── Corpus introspection ───────────────────────────────────────────────

    @property
    def corpus_size(self) -> int:
        """Number of chunks in the BM25 index."""
        return len(self._chunks)

    @property
    def vocabulary_size(self) -> int:
        """Number of unique terms in the indexed corpus."""
        return len(self._bm25.idf)

    def top_terms_for_query(self, query_text: str, n: int = 10) -> list[str]:
        """
        Return the top-n query tokens ranked by their corpus IDF score.

        Useful for debugging: high-IDF terms are the most discriminative
        for this query against the runbook corpus.
        """
        query_tokens = _tokenise(query_text)
        idf_map = self._bm25.idf  # dict[token, float]
        scored_tokens = [
            (token, idf_map.get(token, 0.0)) for token in set(query_tokens)
        ]
        scored_tokens.sort(key=lambda x: x[1], reverse=True)
        return [t for t, _ in scored_tokens[:n]]
