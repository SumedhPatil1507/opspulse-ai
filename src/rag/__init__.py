"""
src.rag — Retrieval-Augmented Generation pipeline for OpsPulse AI.

Public surface
--------------
RunbookChunk       — dataclass representing one indexed passage
BM25SparseScorer   — corpus-level BM25 scorer (sparse retrieval leg)
RunbookIndexer     — builds / refreshes the Qdrant collection
HybridSearchRetriever — dense + sparse fusion + cross-encoder re-ranking
"""

from src.rag.chunker import RunbookChunk, RunbookChunker
from src.rag.sparse import BM25SparseScorer
from src.rag.indexer import RunbookIndexer
from src.rag.retriever import HybridSearchRetriever

__all__ = [
    "RunbookChunk",
    "RunbookChunker",
    "BM25SparseScorer",
    "RunbookIndexer",
    "HybridSearchRetriever",
]
