"""
FastAPI application entry point for the Hermetic RAG API.

Initialises the pgvector connection pool on startup, closes it on
shutdown, includes CORS middleware, and registers all routers.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.models.schemas import HealthResponse
from app.routers import chat as chat_router
from app.services import vector_store

logger = logging.getLogger(__name__)

# ── Uptime tracking ──────────────────────────────────────────────────────────
_start_time: float = time.monotonic()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup / shutdown."""
    # ── Startup ─────────────────────────────────────────────────────────
    logger.info(
        "%s v%s starting up …",
        settings.app_name,
        settings.app_version,
    )

    # Initialise pgvector pool and ensure schema + index exist
    try:
        await vector_store.init_pool()
        await vector_store.ensure_schema()
        await vector_store.ensure_index()
        logger.info("pgvector pool initialised")
    except Exception as exc:
        logger.warning(
            "pgvector unavailable — server running in degraded mode: %s",
            exc,
        )

    logger.info("Application ready")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    logger.info("Shutting down …")
    try:
        await vector_store.close_pool()
    except Exception:
        pass
    logger.info("Goodbye.")


# ── Application factory ──────────────────────────────────────────────────────

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ─────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────────────

app.include_router(chat_router.router, prefix="/api/v1")


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health_check() -> HealthResponse:
    """Liveness / readiness probe."""
    db_ok = await vector_store.check_connectivity()
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        version=settings.app_version,
        uptime_seconds=time.monotonic() - _start_time,
        database="connected" if db_ok else "unreachable",
    )
