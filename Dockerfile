FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    KARAOKE_AUTO_OFFSET_ENABLED=0 \
    MIXTERIOSO_MP3_TOTAL_TIMEOUT_SEC=120 \
    MIXTERIOSO_MP3_MAX_SOURCE_SECONDS=90 \
    MIXTERIOSO_MP3_HOT_QUERY_SPEED_MODE=0 \
    KARAOAPI_NO_COOKIE_RECOVERY_TOTAL_TIMEOUT_SEC=120 \
    KARAOAPI_NO_COOKIE_RECOVERY_MAX_SOURCE_SECONDS=70

WORKDIR /app

# Install only runtime dependencies + Tor for proxy support + Node.js for yt-dlp JS runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ffmpeg \
        ca-certificates \
        tor \
        nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies with CPU-only PyTorch (CPU-only install for non-GPU runtime)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
        torch==2.1.0+cpu \
        torchaudio==2.1.0+cpu \
        --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && pip install --no-cache-dir "numpy<2" \
    && python -m pip install --no-cache-dir -U "yt-dlp[default]" \
    && command -v demucs >/dev/null \
    && find /usr/local/lib/python3.11/site-packages -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/local/lib/python3.11/site-packages -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true \
    && find /usr/local/lib/python3.11/site-packages -name "*.so" -exec strip {} + 2>/dev/null || true \
    && rm -rf /tmp/* /var/tmp/*

ENV MIXTERIOSO_YTDLP_JS_RUNTIMES=node

COPY . /app

EXPOSE 8080

# Use startup script that starts Tor proxy and then uvicorn
CMD ["bash", "/app/start-with-tor.sh"]
