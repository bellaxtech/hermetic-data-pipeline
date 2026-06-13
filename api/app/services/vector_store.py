"""
Async pgvector service for vector similarity search and document storage.

Provides a connection-pool-based interface to PostgreSQL + pgvector
for high-performance ANN (approximate nearest neighbour) queries.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import asyncpg

from app.core.config import settings

logger = logging.getLogger(__name__)

# Pool is lazily initialised on application startup
_pool: asyncpg.Pool | None = None


# ── Pool lifecycle ───────────────────────────────────────────────────────────

async def init_pool() -> None:
    """Create the asyncpg connection pool (called once on startup)."""
    global _pool
    if _pool is not None:
        logger.warning("pgvector pool already initialised – closing first")
        await _pool.close()

    async def _init_pgvector(conn: asyncpg.Connection) -> None:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")

    _pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn_str,
        min_size=settings.postgres_pool_min_size,
        max_size=settings.postgres_pool_max_size,
        command_timeout=30,
        init=_init_pgvector,
    )
    logger.info(
        "pgvector pool initialised (min=%d, max=%d)",
        settings.postgres_pool_min_size,
        settings.postgres_pool_max_size,
    )


async def close_pool() -> None:
    """Close the asyncpg pool (called on shutdown)."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("pgvector pool closed")


async def ensure_schema() -> None:
    """Create the schema and table if they do not exist yet.

    This is idempotent and safe to call on every startup.
    """
    async with _require_pool().acquire() as conn:
        await conn.execute("""
            CREATE SCHEMA IF NOT EXISTS rag;
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rag.document_chunks (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                document_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                content     TEXT NOT NULL,
                metadata    JSONB NOT NULL DEFAULT '{}',
                embedding   vector(%(dim)s) NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """ % {"dim": settings.embedding_dimensions})
        logger.info("Schema 'rag.document_chunks' is ready")


async def ensure_index() -> None:
    """Create the pgvector index if it does not exist.

    Supports both IVFFlat and HNSW index types based on configuration.
    """
    index_name = f"idx_document_chunks_embedding_{settings.vector_index_type}"
    index_ddl: str

    if settings.vector_index_type.lower() == "hnsw":
        index_ddl = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON rag.document_chunks
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = {settings.vector_index_lists});
        """
    else:
        # Default: IVFFlat
        index_ddl = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON rag.document_chunks
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = {settings.vector_index_lists});
        """

    async with _require_pool().acquire() as conn:
        await conn.execute(index_ddl)
        logger.info("Vector index '%s' is ready", index_name)


# ── Public helpers ───────────────────────────────────────────────────────────

def _require_pool() -> asyncpg.Pool:
    """Return the pool or raise if not initialised."""
    if _pool is None:
        raise RuntimeError(
            "pgvector pool is not initialised – did you call init_pool() on startup?"
        )
    return _pool


# ── CRUD ─────────────────────────────────────────────────────────────────────

async def insert_chunk(
    document_id: str,
    chunk_index: int,
    content: str,
    embedding: list[float],
    metadata: dict[str, Any] | None = None,
) -> str:
    """Insert a single document chunk with its embedding vector.

    Returns the newly created chunk UUID.
    """
    chunk_id = str(uuid.uuid4())
    async with _require_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO rag.document_chunks (id, document_id, chunk_index, content, metadata, embedding)
            VALUES ($1, $2, $3, $4, $5, $6::vector)
            """,
            chunk_id,
            document_id,
            chunk_index,
            content,
            metadata or {},
            embedding,
        )
    return chunk_id


async def search_similar(
    query_embedding: list[float],
    top_k: int | None = None,
    min_score: float | None = None,
) -> list[dict[str, Any]]:
    """Perform cosine-similarity search returning the top-K matching chunks.

    Each result dict contains: id, document_id, chunk_index, content,
    metadata, and score.
    """
    k = top_k or settings.vector_top_k
    threshold = min_score if min_score is not None else settings.vector_min_score

    async with _require_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                document_id,
                chunk_index,
                content,
                metadata,
                1 - (embedding <=> $1::vector) AS score
            FROM rag.document_chunks
            WHERE 1 - (embedding <=> $1::vector) >= $2
            ORDER BY score DESC
            LIMIT $3
            """,
            query_embedding,
            threshold,
            k,
        )

    return [
        {
            "id": str(row["id"]),
            "document_id": row["document_id"],
            "chunk_index": row["chunk_index"],
            "content": row["content"],
            "metadata": dict(row["metadata"]) if row["metadata"] else {},
            "score": float(row["score"]),
        }
        for row in rows
    ]


async def delete_document(document_id: str) -> int:
    """Delete all chunks belonging to a given document.

    Returns the number of deleted rows.
    """
    async with _require_pool().acquire() as conn:
        result = await conn.execute(
            "DELETE FROM rag.document_chunks WHERE document_id = $1",
            document_id,
        )
    # asyncpg returns "DELETE N"; parse the integer
    return int(result.split()[-1])


async def count_chunks() -> int:
    """Return the total number of chunks in the vector store."""
    async with _require_pool().acquire() as conn:
        row = await conn.fetchval("SELECT count(*) FROM rag.document_chunks")
    return row or 0


async def check_connectivity() -> bool:
    """Return ``True`` if the database is reachable and pgvector is available."""
    try:
        async with _require_pool().acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception:
        logger.exception("Database connectivity check failed")
        return False
