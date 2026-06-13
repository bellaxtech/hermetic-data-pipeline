"""
FastAPI router exposing RAG endpoints.

Provides a synchronous POST /chat, a streaming POST /chat/stream,
and a debug GET /search endpoint.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.models.schemas import ChatRequest, ChatResponse, DocumentChunk
from app.services import rag_chain, vector_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["chat"])


# ── Non-streaming RAG ────────────────────────────────────────────────────────

@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(body: ChatRequest) -> ChatResponse:
    """RAG-powered chat endpoint.

    Accepts a user query and optional conversation history, retrieves
    relevant document chunks from the vector store, and returns an
    LLM-generated answer with source citations.
    """
    try:
        result = await rag_chain.answer(
            query=body.query,
            top_k=body.top_k,
        )
        return ChatResponse(
            answer=result["answer"],
            source_documents=result["source_documents"],
            confidence=result["confidence"],
            total_tokens=result["total_tokens"],
        )
    except Exception:
        logger.exception("Error processing chat request")
        raise HTTPException(status_code=500, detail="Internal server error during RAG synthesis")


# ── Streaming RAG ────────────────────────────────────────────────────────────

@router.post("/chat/stream")
async def chat_stream_endpoint(body: ChatRequest) -> StreamingResponse:
    """Streaming RAG chat endpoint.

    Returns a Server-Sent Events (SSE) stream where each event is a
    JSON line prefixed with ``data: ``.

    Event types:
      - ``sources`` — retrieved document chunks
      - ``meta`` — confidence, token count
      - ``token`` — individual generated token
      - ``done`` — signals end of stream
    """
    try:
        stream = rag_chain.answer_stream(
            query=body.query,
            top_k=body.top_k,
        )
        return StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    except Exception:
        logger.exception("Error setting up streaming chat")
        raise HTTPException(status_code=500, detail="Internal server error during streaming RAG")


# ── Debug / query vector store ──────────────────────────────────────────────

@router.get("/search")
async def search_endpoint(
    query: str = Query(..., min_length=1, description="Search query text"),
    top_k: int = Query(default=5, ge=1, le=50, description="Number of results"),
) -> list[DocumentChunk]:
    """Raw vector search (debug/query endpoint).

    Performs a similarity search against the document store without
    LLM synthesis. Useful for inspecting what the vector DB returns
    for a given query.
    """
    try:
        query_embedding = await rag_chain._embed(query)
        results = await vector_store.search_similar(query_embedding, top_k=top_k)
        return [DocumentChunk(**r) for r in results]
    except Exception:
        logger.exception("Error during raw search")
        raise HTTPException(status_code=500, detail="Internal server error during vector search")
