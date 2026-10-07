"""
Hybrid Search Retriever for runbook remediation procedures.

Pipeline overview
-----------------

  query (error stacktrace)
         │
         ├──────────────────────────────────────────────┐
         │  Dense leg                                    │  Sparse leg
         ▼                                               ▼
  SentenceTransformer.encode()              BM25SparseScorer.query()
         │ float32 vector [384]                          │ BM25 scores
         ▼                                               ▼
  QdrantClient.search()                     top-K BM25 chunks
  (cosine ANN, top-K candidates)
         │                                               │
         └──────────────┬────────────────────────────────┘
                        │  Union of candidates
                        ▼
              Reciprocal Rank Fusion (RRF)
              score_rrf(d) = Σ 1 / (k + rank_i(d))
                        │
                        ▼
              top-N fused candidates (default 10)
                        │
                        ▼
           CrossEncoder.predict(query, chunk_text)
           (ms-marco-MiniLM-L-6-v2)
                        │
                        ▼
              Re-ranked list, return top-2

Data contracts
--------------
Input  : str  — raw error stacktrace / alert description
Output : list[RetrievalResult]  — at most ``top_k`` results (default 2)

Each ``RetrievalResult`` carries:
  * The original ``RunbookChunk``
  * ``dense_score``  — cosine similarity from Qdrant (0–1)
  * ``sparse_score`` — normalised BM25 score (0–1)
  * ``rrf_score``    — fused Reciprocal Rank Fusion score
  * ``rerank_score`` — cross-encoder logit (higher = more relevant)
  * ``rank``         — final 1-based position in the returned list

Design decisions
----------------
* Retrieval is synchronous.  For async FastAPI routes, call
  ``asyncio.to_thread(retriever.retrieve, stacktrace)`` — the same pattern
  used for Celery enqueue in the alert route.
* Both legs fetch ``settings.RETRIEVAL_CANDIDATES`` (default 10) results.
  The union de-duplicated set goes into RRF, then the cross-encoder scores
  only those fused candidates — keeping re-ranking cost low (O(10–20) pairs).
* ``CrossEncoder`` is loaded lazily on first call to avoid slowing down import
  of the module in contexts where re-ranking is not needed.
* All model instances (encoder, cross-encoder) and the BM25 index are held
  as instance attributes so they are created once and reused across queries.
* Thread-safe for concurrent reads after construction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Sequence

from qdrant_client import QdrantClient
from sentence_transformers import CrossEncoder, SentenceTransformer

from src.api.core.config import Settings, get_settings
from src.rag.chunker import RunbookChunk
from src.rag.indexer import RunbookIndexer
from src.rag.sparse import BM25SparseScorer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RetrievalResult:
    """
    A single ranked remediation result returned by the retriever.

    Attributes
    ----------
    chunk         : The matched ``RunbookChunk`` containing the remediation text.
    dense_score   : Cosine similarity returned by Qdrant (0.0 – 1.0).
                    0.0 if this chunk was not in the dense leg's results.
    sparse_score  : Normalised BM25 score (0.0 – 1.0).
                    0.0 if this chunk was not in the sparse leg's results.
    rrf_score     : Reciprocal Rank Fusion score combining both legs.
    rerank_score  : Cross-encoder relevance logit.  Higher = more relevant.
                    No upper bound; typical range −10 to +10.
    rank          : 1-based final position in the returned result list.
    """

    chunk: RunbookChunk
    dense_score: float = 0.0
    sparse_score: float = 0.0
    rrf_score: float = 0.0
    rerank_score: float = 0.0
    rank: int = 0

    @property
    def source_file(self) -> str:
        return self.chunk.source_file

    @property
    def section_path(self) -> str:
        return self.chunk.section_path

    @property
    def text(self) -> str:
        return self.chunk.text

    def to_dict(self) -> dict:
        """JSON-serialisable summary for API responses or logging."""
        return {
            "rank": self.rank,
            "source_file": self.source_file,
            "section_path": self.section_path,
            "dense_score": round(self.dense_score, 4),
            "sparse_score": round(self.sparse_score, 4),
            "rrf_score": round(self.rrf_score, 6),
            "rerank_score": round(self.rerank_score, 4),
            "text_preview": self.text[:300] + ("…" if len(self.text) > 300 else ""),
            "chunk_id": self.chunk.chunk_id,
        }


# ---------------------------------------------------------------------------
# Hybrid retriever
# ---------------------------------------------------------------------------

class HybridSearchRetriever:
    """
    Combines dense ANN search (Qdrant) with BM25 sparse retrieval and
    cross-encoder re-ranking to return the most relevant runbook remediation
    procedures for an error stacktrace.

    Parameters
    ----------
    qdrant_client    : Connected ``QdrantClient``.
    encoder          : Loaded ``SentenceTransformer`` (dense bi-encoder).
    all_chunks       : Full list of ``RunbookChunk`` objects — used to build
                       the in-process BM25 index.
    settings         : Application settings.
    cross_encoder    : Optional pre-loaded ``CrossEncoder``.  If ``None``,
                       loaded lazily on first ``retrieve()`` call.

    Typical construction
    --------------------
    Use the ``from_index()`` classmethod which wires all dependencies and
    optionally triggers an index build:

        retriever = HybridSearchRetriever.from_index()
        results   = retriever.retrieve(stacktrace, top_k=2)
    """

    def __init__(
        self,
        qdrant_client: QdrantClient,
        encoder: SentenceTransformer,
        all_chunks: list[RunbookChunk],
        settings: Settings | None = None,
        cross_encoder: CrossEncoder | None = None,
    ) -> None:
        self._client = qdrant_client
        self._encoder = encoder
        self._settings = settings or get_settings()
        self._collection = self._settings.QDRANT_COLLECTION

        # Build BM25 index over the full chunk corpus
        logger.info("Building BM25 index over %d chunks …", len(all_chunks))
        self._bm25 = BM25SparseScorer(all_chunks)
        self._all_chunks = all_chunks
        # Map chunk_id → RunbookChunk for O(1) lookup during fusion
        self._chunk_map: dict[str, RunbookChunk] = {
            c.chunk_id: c for c in all_chunks
        }

        # Cross-encoder: stored as None until first use (lazy load)
        self._cross_encoder: CrossEncoder | None = cross_encoder

    # ── Factory ────────────────────────────────────────────────────────────

    @classmethod
    def from_index(
        cls,
        settings: Settings | None = None,
        rebuild_index: bool = False,
    ) -> "HybridSearchRetriever":
        """
        Build or connect to an existing index and return a ready retriever.

        Parameters
        ----------
        settings      : Application settings.  Uses ``get_settings()`` if None.
        rebuild_index : If ``True``, drop and rebuild the Qdrant collection.
                        Defaults to ``False`` — connect to existing collection.
        """
        from src.rag.chunker import RunbookChunker  # avoid circular at module level

        cfg = settings or get_settings()

        logger.info("Connecting to Qdrant at %s:%d", cfg.QDRANT_HOST, cfg.QDRANT_PORT)
        client = QdrantClient(
            host=cfg.QDRANT_HOST,
            port=cfg.QDRANT_PORT,
            api_key=cfg.QDRANT_API_KEY,
            timeout=30,
        )

        logger.info("Loading dense encoder: %s", cfg.DENSE_MODEL_NAME)
        encoder = SentenceTransformer(cfg.DENSE_MODEL_NAME)

        # Load all chunks for BM25 (always needed — BM25 is in-process)
        chunker = RunbookChunker(
            runbooks_dir=cfg.RUNBOOKS_DIR,
            chunk_size=cfg.CHUNK_SIZE,
            chunk_overlap=cfg.CHUNK_OVERLAP,
        )
        all_chunks = chunker.load_all()

        if rebuild_index:
            logger.info("rebuild_index=True — rebuilding Qdrant collection.")
            indexer = RunbookIndexer(client=client, encoder=encoder, settings=cfg)
            indexer.delete_collection()
            indexer.ensure_collection()
            indexer.upsert_chunks(all_chunks)
        else:
            # Ensure collection exists without touching existing data
            indexer = RunbookIndexer(client=client, encoder=encoder, settings=cfg)
            indexer.ensure_collection()
            if indexer.count() == 0:
                logger.info(
                    "Collection is empty — running initial index build."
                )
                indexer.upsert_chunks(all_chunks)

        return cls(
            qdrant_client=client,
            encoder=encoder,
            all_chunks=all_chunks,
            settings=cfg,
        )

    # ── Main entry point ───────────────────────────────────────────────────

    def retrieve(
        self,
        query: str,
        top_k: int | None = None,
    ) -> list[RetrievalResult]:
        """
        Return the top-k most relevant remediation chunks for ``query``.

        Parameters
        ----------
        query : Raw error stacktrace, log excerpt, or incident description.
        top_k : Number of final results to return.  Defaults to
                ``settings.TOP_K_RESULTS`` (2).

        Returns
        -------
        List of ``RetrievalResult`` sorted by ``rerank_score`` descending,
        length at most ``top_k``.  May be shorter if fewer candidates exist.
        """
        k = top_k if top_k is not None else self._settings.TOP_K_RESULTS
        n_candidates = self._settings.RETRIEVAL_CANDIDATES

        if not query.strip():
            logger.warning("retrieve() called with empty query.")
            return []

        logger.info(
            "Hybrid retrieval: query_len=%d candidates=%d top_k=%d",
            len(query),
            n_candidates,
            k,
        )

        # ── 1. Dense leg ──────────────────────────────────────────────────
        dense_hits = self._dense_search(query, n_candidates)
        logger.debug("Dense leg returned %d hits.", len(dense_hits))

        # ── 2. Sparse leg ─────────────────────────────────────────────────
        sparse_hits = self._sparse_search(query, n_candidates)
        logger.debug("Sparse leg returned %d hits.", len(sparse_hits))

        # ── 3. Reciprocal Rank Fusion ─────────────────────────────────────
        fused = self._reciprocal_rank_fusion(
            dense_hits=dense_hits,
            sparse_hits=sparse_hits,
            top_n=n_candidates,
        )
        logger.debug("RRF fusion produced %d candidates.", len(fused))

        if not fused:
            logger.warning("No candidates after RRF fusion — returning empty.")
            return []

        # ── 4. Cross-encoder re-ranking ───────────────────────────────────
        reranked = self._rerank(query, fused)
        logger.info(
            "Re-ranked %d candidates → returning top %d.",
            len(reranked),
            k,
        )

        # ── 5. Assign final ranks and slice ───────────────────────────────
        for i, result in enumerate(reranked[:k]):
            result.rank = i + 1

        return reranked[:k]

    # ── Dense search ───────────────────────────────────────────────────────

    def _dense_search(
        self, query: str, top_k: int
    ) -> list[tuple[RunbookChunk, float]]:
        """
        Encode the query and run Qdrant ANN search.

        Returns ``[(RunbookChunk, cosine_score)]`` sorted by score descending.
        """
        query_vector = self._encoder.encode(
            query,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).tolist()

        search_results = self._client.search(
            collection_name=self._collection,
            query_vector=query_vector,
            limit=top_k,
            with_payload=True,
            score_threshold=0.0,
        )

        hits: list[tuple[RunbookChunk, float]] = []
        for point in search_results:
            if point.payload:
                chunk = RunbookChunk.from_payload(point.payload)
                hits.append((chunk, float(point.score)))

        return hits

    # ── Sparse search ──────────────────────────────────────────────────────

    def _sparse_search(
        self, query: str, top_k: int
    ) -> list[tuple[RunbookChunk, float]]:
        """
        Run BM25 retrieval over the in-process index.

        Returns ``[(RunbookChunk, normalised_bm25_score)]`` sorted descending.
        The scores are already normalised to [0,1] by BM25SparseScorer.
        """
        raw_hits = self._bm25.query(query, top_k=top_k)
        norm_scores = self._bm25.get_normalised_scores(query)

        return [
            (chunk, norm_scores.get(chunk.chunk_id, 0.0))
            for chunk, _ in raw_hits
        ]

    # ── Reciprocal Rank Fusion ─────────────────────────────────────────────

    def _reciprocal_rank_fusion(
        self,
        dense_hits: list[tuple[RunbookChunk, float]],
        sparse_hits: list[tuple[RunbookChunk, float]],
        top_n: int,
    ) -> list[RetrievalResult]:
        """
        Fuse dense and sparse ranked lists using Reciprocal Rank Fusion.

        RRF formula:
            score(d) = Σ_i  1 / (k + rank_i(d))

        where ``k`` is the RRF constant (default 60, from settings.RRF_K)
        and ``rank_i(d)`` is the 1-based rank of document d in list i.
        Documents not appearing in a list receive rank = ∞ (score = 0).

        Returns up to ``top_n`` ``RetrievalResult`` objects sorted by
        descending RRF score.
        """
        k = self._settings.RRF_K

        # Build {chunk_id → RetrievalResult} accumulator
        results: dict[str, RetrievalResult] = {}

        def _get_or_create(chunk: RunbookChunk) -> RetrievalResult:
            if chunk.chunk_id not in results:
                results[chunk.chunk_id] = RetrievalResult(chunk=chunk)
            return results[chunk.chunk_id]

        # Dense leg — add RRF contribution and store dense_score
        for rank, (chunk, score) in enumerate(dense_hits, start=1):
            r = _get_or_create(chunk)
            r.rrf_score += 1.0 / (k + rank)
            r.dense_score = score

        # Sparse leg — add RRF contribution and store sparse_score
        for rank, (chunk, score) in enumerate(sparse_hits, start=1):
            r = _get_or_create(chunk)
            r.rrf_score += 1.0 / (k + rank)
            r.sparse_score = score

        # Sort by RRF score descending and return top-N
        fused = sorted(results.values(), key=lambda r: r.rrf_score, reverse=True)
        return fused[:top_n]

    # ── Cross-encoder re-ranking ───────────────────────────────────────────

    def _rerank(
        self,
        query: str,
        candidates: list[RetrievalResult],
    ) -> list[RetrievalResult]:
        """
        Score each candidate with the cross-encoder and sort by the result.

        The cross-encoder receives (query, passage) pairs and outputs a
        relevance logit.  Unlike the bi-encoder, it sees both texts together
        so it captures fine-grained semantic interactions.

        Model: ``cross-encoder/ms-marco-MiniLM-L-6-v2``
        Trained on MS-MARCO passage ranking → directly applicable to
        "query = error stacktrace, passage = runbook procedure" pairs.

        Returns ``candidates`` sorted by ``rerank_score`` descending.
        """
        cross_enc = self._get_cross_encoder()

        pairs = [(query, result.chunk.text) for result in candidates]
        scores: list[float] = cross_enc.predict(pairs, show_progress_bar=False).tolist()

        for result, score in zip(candidates, scores):
            result.rerank_score = float(score)

        candidates.sort(key=lambda r: r.rerank_score, reverse=True)
        return candidates

    def _get_cross_encoder(self) -> CrossEncoder:
        """Lazy-load the cross-encoder on first call."""
        if self._cross_encoder is None:
            model_name = self._settings.CROSS_ENCODER_MODEL_NAME
            logger.info("Loading cross-encoder: %s", model_name)
            self._cross_encoder = CrossEncoder(model_name)
            logger.info("Cross-encoder loaded.")
        return self._cross_encoder

    # ── Convenience helpers ────────────────────────────────────────────────

    def retrieve_as_dicts(
        self,
        query: str,
        top_k: int | None = None,
    ) -> list[dict]:
        """
        Like ``retrieve()`` but returns plain dicts for easy JSON serialisation.

        Suitable for direct use in FastAPI response models.
        """
        return [r.to_dict() for r in self.retrieve(query, top_k=top_k)]

    def warm_up(self) -> None:
        """
        Pre-load the cross-encoder and run a dummy query to JIT-compile
        any internal caches.  Call this during application startup to avoid
        first-query latency spikes.
        """
        logger.info("Warming up cross-encoder …")
        self._get_cross_encoder()
        # Dummy retrieval — result is discarded
        try:
            self.retrieve("warm up query", top_k=1)
        except Exception:
            pass  # collection may be empty during startup
        logger.info("Retriever warm-up complete.")
