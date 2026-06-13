# ── Stage 1: Build image ─────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install system build dependencies (if needed for psycopg2, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


# ── Stage 2: Runtime image ───────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /root/.local /root/.local

# Ensure local binaries are on PATH
ENV PATH=/root/.local/bin:$PATH

# Copy application code
COPY app/ ./app/

# ── Runtime metadata ─────────────────────────────────────────────────────────
EXPOSE 8000

ENV PYTHONPATH=/app:$PYTHONPATH

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "app.main:app", "--app-dir", "/app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
