"""
Application configuration using Pydantic v2 BaseSettings.

Loads settings from environment variables with sensible defaults
for local development and production deployment.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import ClassVar

from pydantic import PostgresDsn, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Postgres / pgvector ──────────────────────────────────────────────
    postgres_dsn: PostgresDsn = Field(
        default=PostgresDsn("postgresql://postgres:postgres@localhost:5432/hermetic"),
        description="PostgreSQL connection string (used by asyncpg)",
    )
    postgres_pool_min_size: int = Field(
        default=2, ge=1, description="Minimum asyncpg pool connections"
    )
    postgres_pool_max_size: int = Field(
        default=10, ge=1, description="Maximum asyncpg pool connections"
    )

    # ── LLM API (private / self-hosted) ──────────────────────────────────
    llm_api_url: str = Field(
        default="http://localhost:8001/v1/completions",
        description="Private LLM endpoint (OpenAI-compatible or custom)",
    )
    llm_api_key: str = Field(
        default="", description="API key for private LLM endpoint"
    )
    llm_model_name: str = Field(
        default="hermes-3-llama-3.1-8b",
        description="Model identifier passed to the LLM API",
    )
    llm_max_tokens: int = Field(
        default=1024, ge=64, le=8192, description="Max tokens per LLM completion"
    )
    llm_temperature: float = Field(
        default=0.2, ge=0.0, le=2.0, description="LLM sampling temperature"
    )

    # ── Embedding model ──────────────────────────────────────────────────
    embedding_api_url: str = Field(
        default="http://localhost:8002/v1/embeddings",
        description="Embedding model endpoint",
    )
    embedding_api_key: str = Field(
        default="", description="API key for embedding endpoint"
    )
    embedding_model_name: str = Field(
        default="nomic-embed-text-v1.5",
        description="Embedding model identifier",
    )
    embedding_dimensions: int = Field(
        default=768, ge=128, le=4096, description="Output vector dimension"
    )

    # ── Vector index ─────────────────────────────────────────────────────
    vector_index_type: str = Field(
        default="ivfflat", description="pgvector index type (ivfflat | hnsw)"
    )
    vector_index_lists: int = Field(
        default=100, ge=1, description="IVFFlat lists or HNSW ef_construction"
    )
    vector_top_k: int = Field(
        default=5, ge=1, le=50, description="Default number of documents to retrieve"
    )
    vector_min_score: float = Field(
        default=0.65, ge=0.0, le=1.0, description="Minimum cosine similarity threshold"
    )

    # ── Application ──────────────────────────────────────────────────────
    app_name: str = Field(default="hermetic-rag-api", description="Application name")
    app_version: str = Field(default="0.1.0", description="Semantic version")
    log_level: str = Field(default="INFO", description="Logging level")
    cors_origins: list[str] = Field(
        default=["*"], description="Allowed CORS origins"
    )

    # ── Derived / internal helpers ───────────────────────────────────────
    PROJECT_ROOT: ClassVar[Path] = Path(__file__).resolve().parent.parent.parent

    @property
    def postgres_dsn_str(self) -> str:
        """Return the DSN as a plain string for libraries that don't support
        pydantic's PostgresDsn type."""
        return str(self.postgres_dsn)


# ── Singleton ────────────────────────────────────────────────────────────────
settings = Settings()

# ── Logging configuration ────────────────────────────────────────────────────
def configure_logging() -> None:
    """Set up structured logging for the application."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # Keep uvicorn / httpx chatter at a reasonable level
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


configure_logging()
logger = logging.getLogger(__name__)
