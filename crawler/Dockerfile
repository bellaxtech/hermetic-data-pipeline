# =============================================================================
# Multi-stage Dockerfile for the Hermetic Data Pipeline Scrapy Crawler
# =============================================================================

# ----------- Stage 1: builder (compile / fetch wheels) -----------
FROM python:3.11-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY crawler/requirements.txt .
RUN pip wheel --no-cache-dir --no-deps -r requirements.txt -w /wheels


# ----------- Stage 2: runtime (minimal image) -----------
FROM python:3.11-slim-bookworm AS runtime

# Create non-root user
RUN groupadd -r crawler && useradd -r -g crawler -d /app -s /bin/bash crawler

# Runtime system deps (only what's needed for lxml/Scrapy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy pre-built wheels from builder
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Copy crawler source
COPY crawler/ .

# Volume for crawled output
VOLUME ["/app/data"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import scrapy; print('ok')" || exit 1

# Switch to non-root user
USER crawler

# Default entry: run the wiki spider
ENTRYPOINT ["scrapy", "crawl", "wiki_spider"]
CMD []
