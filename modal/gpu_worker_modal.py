#!/usr/bin/env python3
from __future__ import annotations

import os

import modal


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_str(name: str, default: str) -> str:
    value = (os.environ.get(name) or "").strip()
    return value or default


DEPLOY_ENV = _env_str("MIXTERIOSO_MODAL_ENV", "prod1").lower()
APP_NAME = _env_str("MIXTERIOSO_MODAL_APP_NAME", f"mixterioso-gpu-worker-{DEPLOY_ENV}")
SECRET_NAME = _env_str("MIXTERIOSO_MODAL_SECRET_NAME", f"mixterioso-gpu-worker-{DEPLOY_ENV}")
VOLUME_NAME = _env_str("MIXTERIOSO_MODAL_VOLUME_NAME", f"mixterioso-gpu-cache-{DEPLOY_ENV}")

GPU_TYPE = _env_str("MIXTERIOSO_MODAL_GPU", "L4")
CPU_CORES = float((os.environ.get("MIXTERIOSO_MODAL_CPU") or "4").strip() or "4")
MEMORY_MB = _env_int("MIXTERIOSO_MODAL_MEMORY_MB", 16384, minimum=1024)
TIMEOUT_SEC = _env_int("MIXTERIOSO_MODAL_TIMEOUT_SEC", 1800, minimum=60)
SCALEDOWN_WINDOW_SEC = _env_int("MIXTERIOSO_MODAL_SCALEDOWN_WINDOW_SEC", 30, minimum=5)
MIN_CONTAINERS = _env_int("MIXTERIOSO_MODAL_MIN_CONTAINERS", 0, minimum=0)
MAX_CONTAINERS = _env_int("MIXTERIOSO_MODAL_MAX_CONTAINERS", 1, minimum=1)
WORKER_MAX_CONCURRENT = _env_int("MIXTERIOSO_MODAL_WORKER_MAX_CONCURRENT", 1, minimum=1)

CACHE_MOUNT_PATH = "/cache"

image = (
    modal.Image.from_registry(
        "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime",
        add_python="3.11",
    )
    .apt_install("ffmpeg")
    .pip_install(
        "requests>=2.31.0",
        "yt-dlp[default]>=2024.8.6",
        "fastapi>=0.115.0",
        "python-multipart>=0.0.6",
        "demucs>=4.0.1",
        "numpy<2",
        "google-cloud-storage>=2.18.2",
        "redis>=5.0.0",
    )
    .add_local_python_source("scripts", "karaoapi")
)

app = modal.App(APP_NAME)
cache_volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)


@app.function(
    image=image,
    gpu=GPU_TYPE,
    cpu=CPU_CORES,
    memory=MEMORY_MB,
    timeout=TIMEOUT_SEC,
    scaledown_window=SCALEDOWN_WINDOW_SEC,
    min_containers=MIN_CONTAINERS,
    max_containers=MAX_CONTAINERS,
    secrets=[modal.Secret.from_name(SECRET_NAME)],
    volumes={CACHE_MOUNT_PATH: cache_volume},
)
@modal.asgi_app()
def web() -> object:
    # Values must be set before importing karaoapi.gpu_worker_app because the
    # module reads env vars at import time.
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_MODE", "1")
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_MAX_CONCURRENT", str(WORKER_MAX_CONCURRENT))
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_QUEUE_WAIT_SECS", "2")
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_JOB_TIMEOUT_SECS", "420")
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_CACHE_ENABLED", "1")
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_SINGLEFLIGHT_ENABLED", "1")
    os.environ.setdefault("MIXTERIOSO_GPU_WORKER_ALLOWED_AUDIO_ROOTS", f"/tmp,/root,{CACHE_MOUNT_PATH}")
    os.environ.setdefault("GPU_WORKER_ALLOW_UNAUTH", "0")
    os.environ.setdefault("GPU_WORKER_REQUIRE_HMAC", "1")

    from karaoapi.gpu_worker_app import app as fastapi_app

    return fastapi_app
