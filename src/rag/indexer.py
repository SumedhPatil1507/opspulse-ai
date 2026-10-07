"""
Qdrant collection manager and runbook ingestion pipeline.

Responsibilities
----------------
1. Create (or verify) the Qdrant collection with the correct vector config.
2. Embed runbook chunks using the dense bi-encoder (all-MiniLM-L6-v2).
3. Upsert ``PointStruct`` objects into Qdrant in batches.
4. Expose a convenience ``build_index()`` classmethod that wires the full
   pipeline: chunk → embed → upsert.

Architecture
------------

                ┌─────────────┐
  *.md files ──►│RunbookChunker│─► [RunbookChunk, ...]
                └─────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ SentenceTransformer│  (all-MiniLM-L6-v2)
              │   .encode(texts) │
              └─────────────────┘
                       │ float32 vectors [N, 384]
                       ▼
              ┌─────────────────┐
              │  RunbookIndexer  │
              │  .upsert_chunks()│──► Qdrant collection "runbooks"
              └─────────────────┘

Design decisions
----------------
* ``sentence-transformers`` is used directly (not via LangChain) to keep the
  dependency surface minimal and give precise control over batch size and
  device placement.
* Qdrant ``COSINE`` distance — standard for bi-encoder similarity search.
* Chunks are upserted (not inserted) so re-indexing is idempotent: running
  the indexer twice does not duplicate documents.
* ``point_id`` is a UUID5 derived from ``chunk_id`` so the same runbook chunk
  always maps to the same Qdrant point ID, enabling targeted updates.
* Batch size defaults to 64 — balances GPU/CPU throughput with memory use.
  Override via ``embed_batch_size`` parameter.
* ``on_disk_payload=True`` on the collection keeps RAM usage low for large
  runbook sets; payload fields used for filtering are still indexed in-memory.
"""

from __future__ import annotations

import logging
import uuid
from typing import Sequence

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from sentence_transformers import SentenceTransformer

from src.api.core.config import Settings, get_settings
from src.rag.chunker import RunbookChunk, RunbookChunker

logger = logging.getLogger(__name__)

# Qdrant point IDs must be UUIDs; we use UUID5 with this namespace so
# chunk_id strings map deterministically to valid UUIDs.
_UUID5_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # RFC 4122 URL ns


def _chunk_id_to_point_id(chunk_id: str) -> str:
    """Convert a hex chunk_id to a deterministic UUID5 string for Qdrant."""
    return str(uuid.uuid5(_UUID5_NS, chunk_id))


class RunbookIndexer:
    """
    Manages the Qdrant vector collection lifecycle and runbook ingestion.

    Parameters
    ----------
    client          : Connected ``QdrantClient`` instance.
    encoder         : Loaded ``SentenceTransformer`` model for dense embeddings.
    settings        : Application settings (collection name, vector size, etc.).
    embed_batch_size: Number of chunks to encode per batch (default 64).
    """

    def __init__(
        self,
        client: QdrantClient,
        encoder: SentenceTransformer,
        settings: Settings | None = None,
        embed_batch_size: int = 64,
    ) -> None:
        self._client = client
        self._encoder = encoder
        self._settings = settings or get_settings()
        self._embed_batch_size = embed_batch_size
        self._collection = self._settings.QDRANT_COLLECTION

    # ── Public API ─────────────────────────────────────────────────────────

    @classmethod
    def build_index(
        cls,
        settings: Settings | None = None,
        embed_batch_size: int = 64,
    ) -> "RunbookIndexer":
        """
        Convenience factory: wire all dependencies and run a full index build.

        1. Instantiate QdrantClient and SentenceTransformer.
        2. Chunk all runbooks from ``settings.RUNBOOKS_DIR``.
        3. Create / verify the Qdrant collection.
        4. Embed and upsert all chunks.

        Returns the ready ``RunbookIndexer`` instance (useful for testing or
        when you need the indexer for later introspection).
        """
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

        indexer = cls(
            client=client,
            encoder=encoder,
            settings=cfg,
            embed_batch_size=embed_batch_size,
        )

        # Chunk runbooks
        chunker = RunbookChunker(
            runbooks_dir=cfg.RUNBOOKS_DIR,
            chunk_size=cfg.CHUNK_SIZE,
            chunk_overlap=cfg.CHUNK_OVERLAP,
        )
        chunks = chunker.load_all()
        if not chunks:
            logger.warning("No chunks produced — index will be empty.")
            return indexer

        # Ensure collection exists
        indexer.ensure_collection()

        # Embed + upsert
        indexer.upsert_chunks(chunks)

        logger.info(
            "Index build complete. Collection '%s' contains %d points.",
            cfg.QDRANT_COLLECTION,
            indexer.count(),
        )
        return indexer

    def ensure_collection(self) -> None:
        """
        Create the Qdrant collection if it does not already exist.

        Safe to call multiple times — skips creation when the collection is
        already present.  Does NOT delete/recreate an existing collection to
        protect live data.
        """
        existing = {c.name for c in self._client.get_collections().collections}

        if self._collection in existing:
            logger.info(
                "Collection '%s' already exists — skipping creation.",
                self._collection,
            )
            return

        logger.info(
            "Creating Qdrant collection '%s' (dim=%d, distance=COSINE).",
            self._collection,
            self._settings.DENSE_VECTOR_SIZE,
        )
        self._client.create_collection(
            collection_name=self._collection,
            vectors_config=qdrant_models.VectorParams(
                size=self._settings.DENSE_VECTOR_SIZE,
                distance=qdrant_models.Distance.COSINE,
            ),
            # Keep vector data on disk to minimise RAM — payload still fast
            on_disk_payload=True,
            optimizers_config=qdrant_models.OptimizersConfigDiff(
                # Trigger segment optimisation after indexing is done
                indexing_threshold=20_000,
            ),
        )

        # Create a keyword payload index on source_file for filtered search
        self._client.create_payload_index(
            collection_name=self._collection,
            field_name="source_file",
            field_schema=qdrant_models.PayloadSchemaType.KEYWORD,
        )
        logger.info("Collection '%s' created successfully.", self._collection)

    def upsert_chunks(self, chunks: Sequence[RunbookChunk]) -> None:
        """
        Embed and upsert a sequence of ``RunbookChunk`` objects into Qdrant.

        Uses batched encoding to control memory usage.  Each chunk is upserted
        with its full ``to_payload()`` dict as Qdrant point payload so the
        retriever can reconstruct ``RunbookChunk`` objects from search results
        without an additional lookup.

        Parameters
        ----------
        chunks : Chunks to embed and upsert.
        """
        total = len(chunks)
        logger.info("Upserting %d chunks in batches of %d …", total, self._embed_batch_size)

        for batch_start in range(0, total, self._embed_batch_size):
            batch = chunks[batch_start : batch_start + self._embed_batch_size]
            texts = [chunk.text for chunk in batch]

            # Encode: returns numpy float32 array [batch, vector_dim]
            vectors = self._encoder.encode(
                texts,
                batch_size=self._embed_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,   # L2-normalise → cosine = dot product
            )

            points = [
                qdrant_models.PointStruct(
                    id=_chunk_id_to_point_id(chunk.chunk_id),
                    vector=vector.tolist(),
                    payload=chunk.to_payload(),
                )
                for chunk, vector in zip(batch, vectors)
            ]

            self._client.upsert(
                collection_name=self._collection,
                points=points,
                wait=True,   # synchronous — ensures durability before returning
            )

            logger.debug(
                "Upserted batch %d–%d / %d",
                batch_start + 1,
                min(batch_start + self._embed_batch_size, total),
                total,
            )

        logger.info("Upsert complete. %d chunks indexed.", total)

    def delete_collection(self) -> None:
        """
        Drop the Qdrant collection entirely.

        Use with caution — irreversible.  Intended for CI teardown or full
        re-index workflows where you want a clean slate.
        """
        logger.warning("Deleting Qdrant collection '%s'.", self._collection)
        self._client.delete_collection(self._collection)

    def count(self) -> int:
        """Return the number of points currently in the collection."""
        try:
            info = self._client.get_collection(self._collection)
            return info.points_count or 0
        except Exception:
            return 0

    # ── Embedding helper (used by retriever) ──────────────────────────────

    def embed_query(self, text: str) -> list[float]:
        """
        Encode a single query string into a normalised dense vector.

        Returns a plain Python ``list[float]`` compatible with Qdrant's
        ``query_vector`` parameter.
        """
        vector = self._encoder.encode(
            text,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return vector.tolist()
