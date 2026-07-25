# ── Base image ────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── System deps ───────────────────────────────────────────────────────────────
# curl — healthcheck only (no ffmpeg needed without download feature)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Working directory ─────────────────────────────────────────────────────────
WORKDIR /app

# ── Python deps — separate layer so code changes don't bust the cache ─────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ── App code ──────────────────────────────────────────────────────────────────
COPY app.py ./

# ── Ensure tmp dirs exist and are writable ────────────────────────────────────
RUN mkdir -p /tmp/ytm_cache \
 && chmod -R 777 /tmp/ytm_cache

# ── Hugging Face Spaces runs as a non-root user ───────────────────────────────
RUN chmod -R 755 /app

# ── Expose port 7860 (required by HF Spaces) ─────────────────────────────────
EXPOSE 7860

# ── Health check ──────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:7860/health || exit 1

# ── Start server ──────────────────────────────────────────────────────────────
# Single worker: in-memory chart cache (_cache_charts) is process-local.
# With 1 worker all requests share the same 10-hour chart cache.
# Disk cache (diskcache) provides durability across restarts.
# uvloop + httptools come in via uvicorn[standard].
CMD ["uvicorn", "app:app", \
     "--host",               "0.0.0.0", \
     "--port",               "7860", \
     "--workers",            "1", \
     "--loop",               "uvloop", \
     "--http",               "httptools", \
     "--timeout-keep-alive", "120", \
     "--access-log"]
