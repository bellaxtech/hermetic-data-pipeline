"""
Pydantic v2 schemas for the Hermetic RAG API.

Defines request / response models and internal document representations
used throughout the service.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── Request models ───────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    """Incoming chat / RAG query from a user."""

    query: str = Field(
        ...,
        min_length=1,
        max_length=4096,
        description="The user's question or prompt",
        examples=["What are the key findings in the Q3 report?"],
    )
    conversation_history: list[dict[str, str]] | None = Field(
        default=None,
        description=(
            "Optional list of prior messages for multi-turn context. "
            "Each entry should have ``{'role': 'user'|'assistant', 'content': '...'}``."
        ),
        examples=[
            [
                {"role": "user", "content": "What did Q2 say about revenue?"},
                {"role": "assistant", "content": "Revenue grew 12% quarter-over-quarter."},
            ]
        ],
    )
    top_k: int | None = Field(
        default=None,
        ge=1,
        le=50,
        description="Override the default number of documents to retrieve",
    )
    temperature: float | None = Field(
        default=None,
        ge=0.0,
        le=2.0,
        description="Override the default LLM temperature",
    )


# ── Response models ──────────────────────────────────────────────────────────

class DocumentChunk(BaseModel):
    """A single chunked document returned as supporting evidence."""

    id: str = Field(..., description="Unique chunk identifier (UUID)")
    document_id: str = Field(..., description="Source document identifier")
    chunk_index: int = Field(..., ge=0, description="Zero-based chunk position")
    content: str = Field(..., description="Text content of the chunk")
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata attached to the chunk (page, section, etc.)",
    )
    score: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Cosine similarity score from vector search",
    )


class ChatResponse(BaseModel):
    """Final response returned to the user after RAG synthesis."""

    answer: str = Field(..., description="Generated answer text")
    source_documents: list[DocumentChunk] = Field(
        default_factory=list,
        description="Top-K retrieved documents that informed the answer",
    )
    confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Aggregate confidence score based on retrieval scores",
    )
    total_tokens: int | None = Field(
        default=None,
        ge=0,
        description="Approximate total tokens used (prompt + completion)",
    )


# ── Health ───────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    """Response from the /health endpoint."""

    status: str = Field(default="ok", description="Service health status")
    version: str = Field(default="0.1.0", description="Application version")
    uptime_seconds: float = Field(
        default=0.0, description="Seconds since application started"
    )
    database: str = Field(
        default="unknown", description="Database connectivity status"
    )
