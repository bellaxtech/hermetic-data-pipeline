"""
RAG chain service that orchestrates retrieval -> context assembly -> LLM call.

Uses LangChain's interface for prompt templating and streaming, but keeps
the vector-store integration custom (via :mod:`app.services.vector_store`)
so we stay independent of LangChain's ecosystem for embedding/indexing.
"""

from __future__ import annotations

import logging
from typing import AsyncGenerator

import httpx
from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, SystemMessagePromptTemplate
from langchain.schema import BaseMessage

from app.core.config import settings
from app.models.schemas import DocumentChunk
from app.services import vector_store

logger = logging.getLogger(__name__)


# ── Prompt template ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a helpful, accurate AI assistant.

You answer questions based *only* on the provided context documents.
If the context does not contain enough information to answer the question,
say so clearly — do not make up information.

Context documents:
{context}

Guidelines:
- Be concise but thorough.
- If the context includes conflicting information, mention it.
- Cite the source document title or ID when referencing specific facts.
- Answer in the same language as the user's question."""

chat_prompt = ChatPromptTemplate.from_messages([
    SystemMessagePromptTemplate.from_template(SYSTEM_PROMPT),
    HumanMessagePromptTemplate.from_template("Question: {question}"),
])


def _format_context(chunks: list[DocumentChunk]) -> str:
    """Build a single context string from the retrieved document chunks."""
    parts: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        source = chunk.document_id
        parts.append(f"[{i}] (source: {source})\n{chunk.content}\n")
    return "\n".join(parts)


def _estimate_tokens(text: str) -> int:
    """Rough token estimate (4 chars per token)."""
    return len(text) // 4


# ── Synchronous (non-streaming) RAG ──────────────────────────────────────────

async def answer(query: str, top_k: int | None = None) -> dict:
    """Full RAG pipeline: retrieve → assemble context → call LLM.

    Returns a dict with keys ``answer``, ``source_documents``, ``confidence``,
    and ``total_tokens``.
    """
    # 1. Embed the query
    query_embedding = await _embed(query)

    # 2. Retrieve relevant chunks from pgvector
    results = await vector_store.search_similar(query_embedding, top_k=top_k)
    chunks = [
        DocumentChunk(**r)
        for r in results
    ]

    # 3. Assemble context
    context_str = _format_context(chunks)

    # 4. Build prompt
    messages = chat_prompt.format_messages(question=query, context=context_str)
    prompt_text = _messages_to_text(messages)

    # 5. Call LLM
    answer_text, usage = await _call_llm(prompt_text)

    # 6. Compute aggregate confidence
    confidence = _aggregate_confidence(chunks)

    return {
        "answer": answer_text,
        "source_documents": chunks,
        "confidence": confidence,
        "total_tokens": usage.get("total_tokens", _estimate_tokens(prompt_text + answer_text)),
    }


# ── Streaming RAG ────────────────────────────────────────────────────────────

async def answer_stream(query: str, top_k: int | None = None) -> AsyncGenerator[str, None]:
    """Like :func:`answer` but yields LLM tokens as they arrive.

    The first two yielded messages are JSON metadata lines:
      - ``data: {"type": "sources", "documents": [...]}``
      - ``data: {"type": "meta", "confidence": 0.95}``

    Thereafter each line is a Server-Sent Event with the token text:
      - ``data: {"type": "token", "content": "..."}``

    Finally:
      - ``data: {"type": "done"}``
    """
    # 1. Embed
    query_embedding = await _embed(query)

    # 2. Retrieve
    results = await vector_store.search_similar(query_embedding, top_k=top_k)
    chunks = [DocumentChunk(**r) for r in results]

    # 3. Yield source metadata
    import json
    yield f"data: {json.dumps({'type': 'sources', 'documents': [c.model_dump() for c in chunks]})}\n\n"
    confidence = _aggregate_confidence(chunks)
    yield f"data: {json.dumps({'type': 'meta', 'confidence': confidence})}\n\n"

    # 4. Assemble context
    context_str = _format_context(chunks)
    messages = chat_prompt.format_messages(question=query, context=context_str)
    prompt_text = _messages_to_text(messages)

    # 5. Stream from LLM
    async for token_text in _call_llm_stream(prompt_text):
        yield f"data: {json.dumps({'type': 'token', 'content': token_text})}\n\n"

    yield "data: {\"type\": \"done\"}\n\n"


# ── Internal helpers ─────────────────────────────────────────────────────────

async def _embed(text: str) -> list[float]:
    """Call the embedding API to obtain a vector for *text*."""
    headers = {"Content-Type": "application/json"}
    if settings.embedding_api_key:
        headers["Authorization"] = f"Bearer {settings.embedding_api_key}"

    payload = {
        "model": settings.embedding_model_name,
        "input": text,
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            settings.embedding_api_url,
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    # Handle both OpenAI-style and simple-array responses
    if "data" in data and isinstance(data["data"], list):
        return data["data"][0]["embedding"]
    if "embedding" in data:
        return data["embedding"]
    raise ValueError(f"Unexpected embedding response format: {type(data)}")


async def _call_llm(prompt: str) -> tuple[str, dict]:
    """Call the private LLM endpoint (non-streaming). Returns (text, usage_dict)."""
    headers = {"Content-Type": "application/json"}
    if settings.llm_api_key:
        headers["Authorization"] = f"Bearer {settings.llm_api_key}"

    payload = {
        "model": settings.llm_model_name,
        "prompt": prompt,
        "max_tokens": settings.llm_max_tokens,
        "temperature": settings.llm_temperature,
        "stream": False,
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            settings.llm_api_url,
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    # Handle multiple response formats (OpenAI-compatible, Ollama, etc.)
    if "choices" in data:
        text = data["choices"][0].get("text", data["choices"][0].get("message", {}).get("content", ""))
    elif "response" in data:
        text = data["response"]
    else:
        text = str(data)

    usage = data.get("usage", {})
    return text.strip(), usage


async def _call_llm_stream(prompt: str) -> AsyncGenerator[str, None]:
    """Call the private LLM endpoint with streaming, yielding tokens."""
    headers = {"Content-Type": "application/json"}
    if settings.llm_api_key:
        headers["Authorization"] = f"Bearer {settings.llm_api_key}"

    payload = {
        "model": settings.llm_model_name,
        "prompt": prompt,
        "max_tokens": settings.llm_max_tokens,
        "temperature": settings.llm_temperature,
        "stream": True,
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        async with client.stream(
            "POST", settings.llm_api_url, headers=headers, json=payload
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                if line.startswith("data: "):
                    line = line[6:]
                if line.strip() == "[DONE]":
                    return
                try:
                    import json
                    chunk = json.loads(line)
                    # Support both token-per-char and full-delta formats
                    if "choices" in chunk:
                        delta = chunk["choices"][0].get("delta", {})
                        token = delta.get("content", delta.get("text", ""))
                    elif "response" in chunk:
                        token = chunk["response"]
                    else:
                        token = chunk.get("text", chunk.get("content", ""))
                    if token:
                        yield token
                except (json.JSONDecodeError, KeyError):
                    # Some servers send raw text instead of JSON
                    yield line


def _aggregate_confidence(chunks: list[DocumentChunk]) -> float:
    """Compute a single confidence score from retrieval scores.

    Uses the mean of the top-3 scores, or simply the mean if fewer chunks.
    """
    scores = [c.score for c in chunks if c.score is not None]
    if not scores:
        return 0.0
    top_n = scores[:3]
    return sum(top_n) / len(top_n)


def _messages_to_text(messages: list[BaseMessage]) -> str:
    """Concatenate a list of LangChain messages into a single prompt string."""
    parts: list[str] = []
    for msg in messages:
        prefix = f"{msg.type.upper()}:\n" if msg.type in ("system", "human", "ai") else ""
        parts.append(f"{prefix}{msg.content}")
    return "\n\n".join(parts)
