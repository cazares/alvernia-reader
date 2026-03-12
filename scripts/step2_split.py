#!/usr/bin/env python3
"""
Step 2 — split / mix audio

Behavior:
- Default: "full" mix (fast). Ensures mixes/<slug>.mp3 and mixes/<slug>.wav exist and match.
- Optional: "stems" mix (Demucs + ffmpeg mix) when requested via mix_mode or stem level overrides.

Stem levels are expressed as PERCENTAGES, not dB:
- 100 = unchanged
- 0 = muted
- 150 = +50% amplitude
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Tuple

from .common import (
    IOFlags,
    Paths,
    ffmpeg_has_encoder,
    resolve_ffmpeg_bin,
    log_timing,
    now_perf_ms,
    log,
    run_cmd,
    have_exe,
    write_json,
    resolve_demucs_bin,
    WHITE,
    CYAN,
    GREEN,
    YELLOW,
    RED,
)
from .worker_security import build_signed_headers
from .worker_storage import (
    download_file as gcs_download_file,
    is_gs_uri,
    object_exists as gcs_object_exists,
    parse_gs_uri,
    source_object_uri as build_source_object_uri,
    stems_object_uri as build_stems_object_uri,
    upload_file as gcs_upload_file,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except Exception:
        return default
    if minimum is not None:
        return max(minimum, value)
    return value


def _env_float(name: str, default: float, *, minimum: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except Exception:
        return default
    if minimum is not None:
        return max(minimum, value)
    return value


def _force_write_flags(flags: IOFlags) -> IOFlags:
    # Mix metadata should track the latest effective settings; stale mix.json
    # prevents cache reuse because subsequent runs think settings changed.
    return IOFlags(force=True, confirm=flags.confirm, dry_run=flags.dry_run)


def _write_mix_meta_if_changed(*, meta_path: Path, payload: dict[str, Any], flags: IOFlags) -> bool:
    """
    Write mix metadata only when payload changes so cache-hit runs stay fast.
    Returns True when a write occurred.
    """
    if (not flags.dry_run) and meta_path.exists():
        try:
            existing = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict) and existing == payload:
                log("MIX", f"Reusing existing mix metadata: {meta_path}", GREEN)
                return False
        except Exception:
            pass
    write_json(meta_path, payload, _force_write_flags(flags), label="mix_meta")
    return True


# Fast-path: when source audio is already non-mp3 (Step1 no-transcode mode),
# we can skip re-encoding mixes/<slug>.mp3 and render directly from WAV.
SKIP_FULL_MIX_MP3_WHEN_SOURCE_NOT_MP3_DEFAULT = _env_bool("MIXTERIOSO_STEP2_SKIP_FULL_MIX_MP3_WHEN_SOURCE_NOT_MP3", True)
# Stems/fallback path can also skip MP3 encode; step4 can consume WAV directly.
# Keep default conservative for compatibility and enable explicitly when needed.
SKIP_STEMS_MIX_MP3_DEFAULT = _env_bool("MIXTERIOSO_STEP2_SKIP_STEMS_MIX_MP3", False)

# Demucs quality/speed tradeoffs
DEMUCS_TWO_STEMS_DEFAULT = _env_bool("MIXTERIOSO_DEMUCS_TWO_STEMS", False)  # 2x speedup, only extract vocals
AUTO_TWO_STEMS_FOR_VOCALS_ONLY_DEFAULT = _env_bool("MIXTERIOSO_AUTO_TWO_STEMS_FOR_VOCALS_ONLY", False)
FAST_VOCALS_ONLY_FALLBACK_FIRST_DEFAULT = _env_bool("MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST", False)
FAST_VOCALS_ONLY_FALLBACK_MIN_PCT_DEFAULT = _env_float(
    "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT",
    50.0,
    minimum=0.0,
)
FAST_VOCALS_ONLY_FALLBACK_MAX_PCT_DEFAULT = _env_float(
    "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MAX_PCT",
    100.0,
    minimum=0.0,
)
FAST_RENDER_AUDIO_PREP_DEFAULT = _env_bool("MIXTERIOSO_FAST_RENDER_AUDIO_PREP", True)
FAST_RENDER_AUDIO_BITRATE_DEFAULT = (
    os.environ.get("MIXTERIOSO_FAST_RENDER_AUDIO_BITRATE", "64k").strip() or "64k"
)
DEMUCS_OVERLAP = float(os.environ.get("MIXTERIOSO_DEMUCS_OVERLAP", "0.10"))  # Default 10% overlap
DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP = float(
    os.environ.get("MIXTERIOSO_DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP", "0.0")
)
DEMUCS_SHIFTS = int(os.environ.get("MIXTERIOSO_DEMUCS_SHIFTS", "1"))  # Default 1 shift (good balance)
STEM_LEVEL_MIN_PCT = 0.0
STEM_LEVEL_MAX_PCT = 150.0
STEM_LEVEL_DEFAULT_PCT = 100.0
_DEMUCS_TWO_STEMS_CTX: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "mixterioso_demucs_two_stems",
    default=DEMUCS_TWO_STEMS_DEFAULT,
)
_PREPARED_RENDER_AUDIO_LOCK = threading.Lock()
_PREPARED_RENDER_AUDIO_TASKS: dict[str, dict[str, Any]] = {}


def _demucs_two_stems_enabled() -> bool:
    return bool(_DEMUCS_TWO_STEMS_CTX.get())


def _auto_two_stems_for_vocals_only_enabled() -> bool:
    # Read this at runtime so main.py can set a per-run default before step2 execution.
    return _env_bool("MIXTERIOSO_AUTO_TWO_STEMS_FOR_VOCALS_ONLY", AUTO_TWO_STEMS_FOR_VOCALS_ONLY_DEFAULT)


def _fast_vocals_only_fallback_first_enabled() -> bool:
    # Read this at runtime so main.py can set a per-run default before step2 execution.
    return _env_bool(
        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST",
        FAST_VOCALS_ONLY_FALLBACK_FIRST_DEFAULT,
    )


def _fast_vocals_only_fallback_max_pct() -> float:
    return _env_float(
        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MAX_PCT",
        FAST_VOCALS_ONLY_FALLBACK_MAX_PCT_DEFAULT,
        minimum=0.0,
    )


def _fast_vocals_only_fallback_min_pct() -> float:
    return _env_float(
        "MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT",
        FAST_VOCALS_ONLY_FALLBACK_MIN_PCT_DEFAULT,
        minimum=0.0,
    )


def _skip_full_mix_mp3_when_source_not_mp3_enabled() -> bool:
    # Read this at runtime so main.py can set a per-run default before step2 execution.
    return _env_bool(
        "MIXTERIOSO_STEP2_SKIP_FULL_MIX_MP3_WHEN_SOURCE_NOT_MP3",
        SKIP_FULL_MIX_MP3_WHEN_SOURCE_NOT_MP3_DEFAULT,
    )


def _skip_stems_mix_mp3_enabled() -> bool:
    # Read this at runtime so main.py can set a per-run default before step2 execution.
    return _env_bool(
        "MIXTERIOSO_STEP2_SKIP_STEMS_MIX_MP3",
        SKIP_STEMS_MIX_MP3_DEFAULT,
    )


def _fast_render_audio_prep_enabled() -> bool:
    return _env_bool(
        "MIXTERIOSO_FAST_RENDER_AUDIO_PREP",
        FAST_RENDER_AUDIO_PREP_DEFAULT,
    )


def _fast_render_audio_bitrate() -> str:
    raw = (os.environ.get("MIXTERIOSO_FAST_RENDER_AUDIO_BITRATE") or "").strip()
    return raw or FAST_RENDER_AUDIO_BITRATE_DEFAULT


def _prepared_render_audio_path(paths: Paths, slug: str) -> Path:
    return paths.mixes / f"{slug}.m4a"


def _prepared_render_audio_meta_path(render_audio_path: Path) -> Path:
    return render_audio_path.with_suffix(render_audio_path.suffix + ".meta.json")


def _prepared_render_audio_fingerprint(wav_path: Path, *, bitrate: str, encoder: str) -> dict[str, Any]:
    stat = wav_path.stat()
    return {
        "wav_path": str(wav_path.resolve()),
        "wav_size": int(stat.st_size),
        "wav_mtime_ns": int(stat.st_mtime_ns),
        "bitrate": str(bitrate),
        "encoder": str(encoder),
    }


@lru_cache(maxsize=1)
def _fast_render_audio_encoder() -> str:
    ffmpeg_bin = resolve_ffmpeg_bin()
    if sys.platform == "darwin" and ffmpeg_has_encoder(ffmpeg_bin, "aac_at"):
        return "aac_at"
    return "aac"


def _prepared_render_audio_is_fresh(render_audio_path: Path, wav_path: Path, *, bitrate: str, encoder: str) -> bool:
    if not render_audio_path.exists() or not wav_path.exists():
        return False
    meta_path = _prepared_render_audio_meta_path(render_audio_path)
    if not meta_path.exists():
        return False
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    try:
        expected = _prepared_render_audio_fingerprint(wav_path, bitrate=bitrate, encoder=encoder)
    except Exception:
        return False
    for key, value in expected.items():
        if payload.get(key) != value:
            return False
    return True


def _start_prepared_render_audio_async(paths: Paths, slug: str, wav_path: Path, *, flags: IOFlags) -> Optional[Path]:
    if flags.dry_run or (not _fast_render_audio_prep_enabled()) or (not wav_path.exists()):
        return None
    if not have_exe("ffmpeg"):
        return None

    render_audio_path = _prepared_render_audio_path(paths, slug)
    bitrate = _fast_render_audio_bitrate()
    encoder = _fast_render_audio_encoder()
    if _prepared_render_audio_is_fresh(render_audio_path, wav_path, bitrate=bitrate, encoder=encoder):
        return render_audio_path

    task_key = str(render_audio_path.resolve())
    with _PREPARED_RENDER_AUDIO_LOCK:
        existing = _PREPARED_RENDER_AUDIO_TASKS.get(task_key) or {}
        existing_thread = existing.get("thread")
        if (
            isinstance(existing_thread, threading.Thread)
            and existing_thread.is_alive()
            and existing.get("wav_path") == str(wav_path.resolve())
            and existing.get("bitrate") == str(bitrate)
            and existing.get("encoder") == str(encoder)
        ):
            return render_audio_path

    tmp_path = render_audio_path.with_name(f"{render_audio_path.stem}.tmp{render_audio_path.suffix}")
    meta_path = _prepared_render_audio_meta_path(render_audio_path)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    def _worker() -> None:
        try:
            cmd = [
                str(resolve_ffmpeg_bin()),
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(wav_path),
                "-c:a",
                str(encoder),
                "-b:a",
                str(bitrate),
                str(tmp_path),
            ]
            log(
                "MIX",
                f"Preparing render audio sidecar -> {render_audio_path.name} ({encoder} {bitrate})",
                CYAN,
            )
            proc = subprocess.run(cmd, capture_output=False, text=True)
            if int(proc.returncode) != 0 or (not tmp_path.exists()):
                log("MIX", f"Prepared render audio failed rc={proc.returncode}", YELLOW)
                return
            render_audio_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.replace(render_audio_path)
            meta_payload = _prepared_render_audio_fingerprint(
                wav_path,
                bitrate=bitrate,
                encoder=encoder,
            )
            meta_payload["updated_at_epoch_ms"] = int(now_perf_ms())
            meta_path.write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
            log("MIX", f"Prepared render audio ready: {render_audio_path.name}", GREEN)
        except Exception as exc:
            log("MIX", f"Prepared render audio failed: {exc}", YELLOW)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            with _PREPARED_RENDER_AUDIO_LOCK:
                current = _PREPARED_RENDER_AUDIO_TASKS.get(task_key)
                if current and current.get("thread") is thread:
                    _PREPARED_RENDER_AUDIO_TASKS.pop(task_key, None)

    thread = threading.Thread(
        target=_worker,
        name=f"prepare-render-audio-{slug}",
        daemon=True,
    )
    with _PREPARED_RENDER_AUDIO_LOCK:
        _PREPARED_RENDER_AUDIO_TASKS[task_key] = {
            "thread": thread,
            "wav_path": str(wav_path.resolve()),
            "bitrate": str(bitrate),
            "encoder": str(encoder),
        }
    thread.start()
    return render_audio_path


def wait_for_prepared_render_audio(mixes_dir: Path, slug: str, *, wait_timeout_sec: float = 0.0) -> Optional[Path]:
    render_audio_path = mixes_dir / f"{slug}.m4a"
    wav_path = mixes_dir / f"{slug}.wav"
    bitrate = _fast_render_audio_bitrate()
    encoder = _fast_render_audio_encoder()
    if _prepared_render_audio_is_fresh(render_audio_path, wav_path, bitrate=bitrate, encoder=encoder):
        return render_audio_path

    task_key = str(render_audio_path.resolve())
    thread: Optional[threading.Thread] = None
    with _PREPARED_RENDER_AUDIO_LOCK:
        task = _PREPARED_RENDER_AUDIO_TASKS.get(task_key) or {}
        maybe_thread = task.get("thread")
        if isinstance(maybe_thread, threading.Thread):
            thread = maybe_thread

    if thread is not None and thread.is_alive() and float(wait_timeout_sec) > 0.0:
        thread.join(timeout=max(0.0, float(wait_timeout_sec)))

    if _prepared_render_audio_is_fresh(render_audio_path, wav_path, bitrate=bitrate, encoder=encoder):
        return render_audio_path
    return None


@contextmanager
def _temporary_demucs_two_stems(enabled: bool):
    token = _DEMUCS_TWO_STEMS_CTX.set(bool(enabled))
    try:
        yield
    finally:
        _DEMUCS_TWO_STEMS_CTX.reset(token)


@dataclass(frozen=True)
class GPUWorkerConfig:
    url: str = ""
    timeout_secs: float = 900.0
    retries: int = 1
    fallback_to_cpu: bool = True
    api_key: str = ""
    hmac_secret: str = ""
    require_hmac: bool = False
    source_bucket: str = ""
    stems_bucket: str = ""
    model_version: str = "htdemucs"


def _normalize_worker_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    try:
        parsed = urllib.parse.urlparse(value)
    except Exception:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value
    path = (parsed.path or "").strip()
    if path in {"", "/"}:
        return urllib.parse.urlunparse(parsed._replace(path="/separate"))
    return value


def _gpu_worker_config() -> GPUWorkerConfig:
    disable_local_fallback = _env_bool(
        "MIXTERIOSO_DISABLE_LOCAL_SPLIT_FALLBACK",
        _env_bool("KARAOAPI_DISABLE_LOCAL_SPLIT_FALLBACK", False),
    )
    hmac_secret = str(
        os.environ.get("MIXTERIOSO_GPU_WORKER_HMAC_SECRET")
        or os.environ.get("KARAOAPI_GPU_WORKER_HMAC_SECRET")
        or ""
    ).strip()
    return GPUWorkerConfig(
        url=_normalize_worker_url(str(
            os.environ.get("MIXTERIOSO_GPU_WORKER_URL")
            or os.environ.get("KARAOAPI_GPU_WORKER_URL")
            or ""
        )),
        timeout_secs=_env_float(
            "MIXTERIOSO_GPU_WORKER_TIMEOUT_SEC",
            _env_float("MIXTERIOSO_GPU_WORKER_TIMEOUT_SECS", 900.0, minimum=1.0),
            minimum=1.0,
        ),
        retries=_env_int("MIXTERIOSO_GPU_WORKER_RETRIES", 1, minimum=0),
        fallback_to_cpu=_env_bool(
            "MIXTERIOSO_GPU_FALLBACK_TO_CPU",
            _env_bool("KARAOAPI_GPU_FALLBACK_TO_CPU", not disable_local_fallback),
        ),
        api_key=str(
            os.environ.get("MIXTERIOSO_GPU_WORKER_API_KEY")
            or os.environ.get("KARAOAPI_GPU_WORKER_API_KEY")
            or ""
        ).strip(),
        hmac_secret=hmac_secret,
        require_hmac=_env_bool(
            "MIXTERIOSO_GPU_WORKER_REQUIRE_HMAC",
            _env_bool("KARAOAPI_GPU_WORKER_REQUIRE_HMAC", bool(hmac_secret)),
        ),
        source_bucket=str(
            os.environ.get("STORAGE_SOURCE_BUCKET")
            or os.environ.get("MIXTERIOSO_SOURCE_BUCKET")
            or ""
        ).strip(),
        stems_bucket=str(
            os.environ.get("STORAGE_STEMS_BUCKET")
            or os.environ.get("MIXTERIOSO_STEMS_BUCKET")
            or ""
        ).strip(),
        model_version=str(
            os.environ.get("MIXTERIOSO_DEMUCS_MODEL")
            or os.environ.get("DEMUCS_MODEL")
            or "htdemucs"
        ).strip()
        or "htdemucs",
    )


GPU_WORKER_MAX_RESPONSE_BYTES = _env_int("MIXTERIOSO_GPU_WORKER_MAX_RESPONSE_BYTES", 2_000_000, minimum=1024)
GPU_STEM_MIN_BYTES = _env_int("MIXTERIOSO_GPU_STEM_MIN_BYTES", 1024, minimum=44)
GPU_WORKER_INLINE_SOURCE_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_INLINE_SOURCE_ENABLED", False)
GPU_WORKER_INLINE_SOURCE_MAX_BYTES = _env_int(
    "MIXTERIOSO_GPU_WORKER_INLINE_SOURCE_MAX_BYTES",
    12_000_000,
    minimum=1024,
)
GPU_WORKER_ALLOW_INSECURE_HTTP = _env_bool("MIXTERIOSO_GPU_WORKER_ALLOW_INSECURE_HTTP", False)
GPU_WORKER_CIRCUIT_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_CIRCUIT_ENABLED", True)
GPU_WORKER_CIRCUIT_FAIL_THRESHOLD = _env_int("MIXTERIOSO_GPU_WORKER_CIRCUIT_FAIL_THRESHOLD", 3, minimum=1)
GPU_WORKER_CIRCUIT_COOLDOWN_SECS = _env_float("MIXTERIOSO_GPU_WORKER_CIRCUIT_COOLDOWN_SECS", 180.0, minimum=5.0)
GPU_WORKER_RETRY_BACKOFF_SECS = _env_float("MIXTERIOSO_GPU_WORKER_RETRY_BACKOFF_SECS", 1.0, minimum=0.0)
GPU_WORKER_RETRY_BACKOFF_MAX_SECS = _env_float("MIXTERIOSO_GPU_WORKER_RETRY_BACKOFF_MAX_SECS", 8.0, minimum=0.0)
GPU_WORKER_KEY_CIRCUIT_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_KEY_CIRCUIT_ENABLED", True)
GPU_WORKER_KEY_CIRCUIT_FAIL_THRESHOLD = _env_int("MIXTERIOSO_GPU_WORKER_KEY_CIRCUIT_FAIL_THRESHOLD", 2, minimum=1)
GPU_WORKER_KEY_CIRCUIT_COOLDOWN_SECS = _env_float("MIXTERIOSO_GPU_WORKER_KEY_CIRCUIT_COOLDOWN_SECS", 600.0, minimum=5.0)
GPU_WORKER_KEY_CIRCUIT_MAX_ENTRIES = _env_int("MIXTERIOSO_GPU_WORKER_KEY_CIRCUIT_MAX_ENTRIES", 5000, minimum=1)
GPU_WORKER_KEY_CIRCUIT_MAX_AGE_SECS = _env_float("MIXTERIOSO_GPU_WORKER_KEY_CIRCUIT_MAX_AGE_SECS", 1209600.0, minimum=60.0)
GLOBAL_STEM_CACHE_ENABLED = _env_bool("MIXTERIOSO_GLOBAL_STEM_CACHE_ENABLED", True)
GLOBAL_STEM_CACHE_DIR_NAME = (
    os.environ.get("MIXTERIOSO_GLOBAL_STEM_CACHE_DIR_NAME", "_global_cache").strip() or "_global_cache"
)
GLOBAL_STEM_CACHE_USE_HARDLINKS = _env_bool("MIXTERIOSO_GLOBAL_STEM_CACHE_USE_HARDLINKS", True)
GLOBAL_STEM_CACHE_PRUNE_ENABLED = _env_bool("MIXTERIOSO_GLOBAL_STEM_CACHE_PRUNE_ENABLED", True)
GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS = _env_float("MIXTERIOSO_GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS", 120.0, minimum=0.0)
GLOBAL_STEM_CACHE_PRUNE_SCAN_LIMIT = _env_int(
    "MIXTERIOSO_GLOBAL_STEM_CACHE_PRUNE_SCAN_LIMIT",
    2500,
    minimum=100,
)
GLOBAL_STEM_CACHE_MAX_ENTRIES = _env_int("MIXTERIOSO_GLOBAL_STEM_CACHE_MAX_ENTRIES", 5000, minimum=1)
GLOBAL_STEM_CACHE_MAX_AGE_SECS = _env_float("MIXTERIOSO_GLOBAL_STEM_CACHE_MAX_AGE_SECS", 1209600.0, minimum=1.0)
DEMUCS_SINGLEFLIGHT_ENABLED = _env_bool("MIXTERIOSO_DEMUCS_SINGLEFLIGHT_ENABLED", True)
_AUDIO_SHA256_CACHE: dict[tuple[str, int, int], str] = {}
_AUDIO_SHA256_CACHE_LOCK = threading.Lock()
_DEMUCS_SINGLEFLIGHT_LOCK = threading.Lock()
_DEMUCS_SINGLEFLIGHT_LOCKS: dict[str, threading.Lock] = {}
_DEMUCS_SINGLEFLIGHT_REFS: dict[str, int] = {}
_GLOBAL_STEM_CACHE_PRUNE_LOCK = threading.Lock()
_GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO = 0.0
_GPU_WORKER_KEY_CIRCUIT_LOCK = threading.Lock()


# Cache torch device detection to avoid repeated probing (10-20ms overhead per call)
_TORCH_DEVICE_CACHE: Optional[Tuple[bool, bool]] = None

def _probe_torch_device_support() -> Tuple[bool, bool]:
    """
    Returns (cuda_available, mps_available) without hard-requiring torch.
    Cached at module level after first call.
    """
    global _TORCH_DEVICE_CACHE
    if _TORCH_DEVICE_CACHE is not None:
        return _TORCH_DEVICE_CACHE

    try:
        import torch  # type: ignore[import-not-found]
    except Exception:
        _TORCH_DEVICE_CACHE = (False, False)
        return _TORCH_DEVICE_CACHE

    cuda_available = False
    mps_available = False

    try:
        cuda_available = bool(torch.cuda.is_available())  # type: ignore[attr-defined]
    except Exception:
        cuda_available = False

    try:
        mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
        mps_available = bool(mps_backend and mps_backend.is_available())
    except Exception:
        mps_available = False

    _TORCH_DEVICE_CACHE = (cuda_available, mps_available)
    return _TORCH_DEVICE_CACHE


def _normalize_device(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = raw.strip().lower()
    if not value:
        return None
    aliases = {
        "gpu": "cuda",
        "metal": "mps",
        "apple": "mps",
        "auto": "auto",
    }
    return aliases.get(value, value)


def _resolve_demucs_device() -> str:
    """
    Resolve Demucs runtime device.
    Priority:
    1) MIXTERIOSO_DEMUCS_DEVICE / KARAOAPI_DEMUCS_DEVICE (cuda|mps|cpu|auto)
    2) Auto-detect: cuda > mps (macOS) > cpu

    Note:
    - Device checks run in the current Python runtime, while Demucs can run in a
      dedicated venv. On macOS we still prefer mps over cpu when auto mode has
      no cuda available.
    """
    requested = _normalize_device(
        os.environ.get("MIXTERIOSO_DEMUCS_DEVICE") or os.environ.get("KARAOAPI_DEMUCS_DEVICE")
    )
    cuda_available, mps_available = _probe_torch_device_support()
    assume_mps_available = _env_bool(
        "MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE",
        _env_bool("KARAOAPI_DEMUCS_ASSUME_MPS_AVAILABLE", False),
    )

    def _fallback_auto() -> str:
        if cuda_available:
            return "cuda"
        if sys.platform == "darwin" and (mps_available or assume_mps_available):
            return "mps"
        return "cpu"

    if requested in (None, "auto"):
        return _fallback_auto()

    if requested not in {"cuda", "mps", "cpu"}:
        log("SPLIT", f"Unknown Demucs device '{requested}', falling back to auto-detect", YELLOW)
        return _fallback_auto()

    if requested == "cuda" and not cuda_available:
        log("SPLIT", "Demucs device=cuda requested but unavailable; falling back to cpu", YELLOW)
        return "cpu"

    if requested == "mps" and sys.platform != "darwin":
        log("SPLIT", "Demucs device=mps requested but non-macOS runtime detected; falling back to cpu", YELLOW)
        return "cpu"

    if requested == "mps" and not mps_available:
        if assume_mps_available and sys.platform == "darwin":
            log("SPLIT", "Demucs device=mps requested with assume-mps override; proceeding on macOS", YELLOW)
            return "mps"
        log("SPLIT", "Demucs device=mps requested but unavailable; falling back to cpu", YELLOW)
        return "cpu"

    return requested


def _pct_to_gain(pct: float) -> float:
    try:
        return float(pct) / 100.0
    except Exception:
        return 1.0


def _normalize_stem_pct(value: Any, *, default: float = STEM_LEVEL_DEFAULT_PCT) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    if not math.isfinite(parsed):
        parsed = float(default)
    return max(float(STEM_LEVEL_MIN_PCT), min(float(STEM_LEVEL_MAX_PCT), parsed))


def _normalize_stem_profile(stem_profile: Optional[dict[str, Any]] = None) -> dict[str, float]:
    profile = stem_profile or {}
    return {
        "vocals": _normalize_stem_pct(profile.get("vocals", STEM_LEVEL_DEFAULT_PCT)),
        "bass": _normalize_stem_pct(profile.get("bass", STEM_LEVEL_DEFAULT_PCT)),
        "drums": _normalize_stem_pct(profile.get("drums", STEM_LEVEL_DEFAULT_PCT)),
        "other": _normalize_stem_pct(profile.get("other", STEM_LEVEL_DEFAULT_PCT)),
    }


_DEMUCS_RUNTIME_FALLBACK_TOKENS: tuple[str, ...] = (
    "numpy is not available",
    "_array_api",
    "couldn't find appropriate backend to handle uri",
    "no audio io backend is available",
    "no audio i/o backend is available",
    "torchaudio",
    "sox_io",
    "soundfile",
    "libsndfile",
    "demucs not found on path",
)


def _is_demucs_runtime_unavailable_error(error_text: str) -> bool:
    low = str(error_text or "").lower()
    if not low:
        return False
    return any(token in low for token in _DEMUCS_RUNTIME_FALLBACK_TOKENS)


def _is_vocals_only_adjustment(vocals: float, bass: float, drums: float, other: float) -> bool:
    try:
        _ = float(vocals)
        return (
            abs(float(bass) - 100.0) <= 1e-6
            and abs(float(drums) - 100.0) <= 1e-6
            and abs(float(other) - 100.0) <= 1e-6
        )
    except Exception:
        return False


def _is_full_vocal_mute_requested(vocals: float) -> bool:
    try:
        return abs(float(vocals)) <= 1e-6
    except Exception:
        return False


def _fast_vocals_fallback_allowed(vocals: float, bass: float, drums: float, other: float) -> bool:
    try:
        vocals_pct = float(vocals)
        fallback_min_pct = _fast_vocals_only_fallback_min_pct()
        fallback_max_pct = _fast_vocals_only_fallback_max_pct()
        return (
            _is_vocals_only_adjustment(vocals_pct, bass, drums, other)
            and (not _is_full_vocal_mute_requested(vocals))
            and vocals_pct >= (fallback_min_pct - 1e-6)
            and vocals_pct <= (fallback_max_pct + 1e-6)
        )
    except Exception:
        return False


def _effective_demucs_overlap(stem_profile: Optional[dict[str, Any]] = None) -> float:
    try:
        base_overlap = float(DEMUCS_OVERLAP)
    except Exception:
        base_overlap = 0.10
    if not _demucs_two_stems_enabled():
        return base_overlap
    profile = _normalize_stem_profile(stem_profile)
    if not _is_vocals_only_adjustment(
        profile.get("vocals", STEM_LEVEL_DEFAULT_PCT),
        profile.get("bass", STEM_LEVEL_DEFAULT_PCT),
        profile.get("drums", STEM_LEVEL_DEFAULT_PCT),
        profile.get("other", STEM_LEVEL_DEFAULT_PCT),
    ):
        return base_overlap
    try:
        fast_overlap = float(DEMUCS_FAST_TWO_STEMS_VOCALS_ONLY_OVERLAP)
    except Exception:
        return base_overlap
    if (not math.isfinite(fast_overlap)) or fast_overlap < 0.0:
        return base_overlap
    return fast_overlap


def _run_checked(cmd: list[str], *, tag: str, dry_run: bool, action: str) -> None:
    try:
        run_cmd(cmd, tag=tag, dry_run=dry_run, check=True)
    except RuntimeError as exc:
        raise RuntimeError(f"{action} failed: {exc}") from exc


@contextmanager
def _canonical_demucs_input_for_slug(
    *,
    paths: Paths,
    slug: str,
    src_audio: Path,
    dry_run: bool,
) -> Path:
    """
    Ensure the Demucs input basename is canonicalized to the job slug.

    Demucs derives output directory names from the input basename. Step1 can
    emit race-suffixed files (e.g. "<slug>.race2.m4a"), so we provide Demucs a
    temporary slug-named input to keep output under separated/htdemucs/<slug>.
    """
    canonical_slug = Path(str(slug or "").strip() or src_audio.stem).name.strip() or src_audio.stem
    if src_audio.stem == canonical_slug:
        yield src_audio
        return

    canonical_name = f"{canonical_slug}{src_audio.suffix or '.wav'}"
    if dry_run:
        yield src_audio.parent / canonical_name
        return

    scratch_root = paths.separated / ".demucs_inputs"
    scratch_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"{canonical_slug}-", dir=str(scratch_root)))
    demucs_src = tmp_dir / canonical_name
    try:
        linked = False
        try:
            os.link(str(src_audio), str(demucs_src))
            linked = True
        except Exception:
            pass
        if not linked:
            try:
                demucs_src.symlink_to(src_audio)
                linked = True
            except Exception:
                pass
        if not linked:
            shutil.copy2(str(src_audio), str(demucs_src))
        yield demucs_src
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _ensure_wav_from_audio(src_audio: Path, out_wav: Path, flags: IOFlags) -> None:
    """
    Ensure mixes/<slug>.wav exists and is not stale relative to src_audio.
    This prevents the renderer (4_mp4.py) from accidentally using an old WAV.
    """
    if out_wav.exists() and not flags.force:
        try:
            # Cache stat() calls to avoid redundant syscalls
            out_stat = out_wav.stat()
            src_stat = src_audio.stat()
            if out_stat.st_mtime >= src_stat.st_mtime:
                return
        except Exception:
            pass

    if not have_exe("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH (required to produce mixes/*.wav)")

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src_audio),
        "-c:a",
        "pcm_s16le",
        str(out_wav),
    ]
    log("MIX", f"Building WAV: {out_wav.name} (from {src_audio.name})", WHITE)
    _run_checked(cmd, tag="FFMPEG", dry_run=flags.dry_run, action="ffmpeg wav build")
    if not flags.dry_run and not out_wav.exists():
        raise RuntimeError(f"Failed to produce {out_wav}")


def _encode_mp3_from_wav(src_wav: Path, out_mp3: Path, flags: IOFlags) -> None:
    if out_mp3.exists() and not flags.force:
        try:
            # Cache stat() calls to avoid redundant syscalls
            out_stat = out_mp3.stat()
            src_stat = src_wav.stat()
            if out_stat.st_mtime >= src_stat.st_mtime:
                return
        except Exception:
            pass

    if not have_exe("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH (required to produce mixes/*.mp3)")

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src_wav),
        "-c:a",
        "libmp3lame",
        "-q:a",
        "2",
        str(out_mp3),
    ]
    log("MIX", f"Encoding MP3: {out_mp3.name} (from {src_wav.name})", WHITE)
    _run_checked(cmd, tag="FFMPEG", dry_run=flags.dry_run, action="ffmpeg mp3 encode")
    if not flags.dry_run and not out_mp3.exists():
        raise RuntimeError(f"Failed to produce {out_mp3}")


def _remove_if_exists(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _resolve_source_audio(paths: Paths, slug: str) -> Path:
    """
    Resolve source audio from Step1 output, supporting native audio extensions.
    Preference order:
    1) meta/<slug>.step1.json -> audio_path
    2) meta/<slug>.step1.json -> mp3
    3) mp3s/<slug>.mp3
    4) any mp3s/<slug>.*
    """
    step1_meta = paths.meta / f"{slug}.step1.json"
    if step1_meta.exists():
        try:
            data = json.loads(step1_meta.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        for key in ("audio_path", "mp3"):
            raw = str((data or {}).get(key) or "").strip()
            if not raw:
                continue
            p = Path(raw)
            if p.exists() and p.is_file() and p.stat().st_size > 0:
                return p

    src_mp3 = paths.mp3s / f"{slug}.mp3"
    if src_mp3.exists() and src_mp3.is_file() and src_mp3.stat().st_size > 0:
        return src_mp3

    # Cache stat() results to avoid repeated syscalls (2-5ms each)
    candidates_with_stat = []
    for p in paths.mp3s.glob(f"{slug}.*"):
        if p.is_file():
            stat_result = p.stat()
            if stat_result.st_size > 0:  # Only consider non-empty files
                candidates_with_stat.append((p, stat_result))

    if candidates_with_stat:
        # Use max() instead of sort since we only need the best candidate
        best = max(candidates_with_stat, key=lambda x: (x[1].st_mtime, x[1].st_size))
        return best[0]

    raise RuntimeError(f"Missing source audio for slug={slug} in {paths.mp3s} and {step1_meta}")


def _refresh_source_audio_if_missing(paths: Paths, slug: str, src_audio: Path) -> Path:
    try:
        if src_audio.exists() and src_audio.is_file() and src_audio.stat().st_size > 0:
            return src_audio
    except Exception:
        pass

    refreshed = _resolve_source_audio(paths, slug)
    if refreshed != src_audio:
        log(
            "SPLIT",
            f"Recovered source audio path for slug={slug}: {src_audio.name} -> {refreshed.name}",
            YELLOW,
        )
    return refreshed


def _required_stem_names() -> list[str]:
    if _demucs_two_stems_enabled():
        return ["vocals", "no_vocals"]
    return ["vocals", "bass", "drums", "other"]


def _resolve_stem_dir(paths: Paths, slug: str) -> Path:
    return paths.separated / "htdemucs" / slug


def _resolve_stem_cache_meta_path(paths: Paths, slug: str) -> Path:
    return paths.meta / f"{slug}.step2_stems.json"


def _resolve_gpu_worker_circuit_meta_path(paths: Paths) -> Path:
    return paths.meta / "gpu_worker_circuit.json"


def _load_gpu_worker_circuit_state(paths: Paths) -> dict[str, Any]:
    meta_path = _resolve_gpu_worker_circuit_meta_path(paths)
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_gpu_worker_circuit_state(paths: Paths, payload: dict[str, Any]) -> None:
    meta_path = _resolve_gpu_worker_circuit_meta_path(paths)
    try:
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _gpu_worker_circuit_status(paths: Paths) -> tuple[bool, float]:
    if not GPU_WORKER_CIRCUIT_ENABLED:
        return False, 0.0
    state = _load_gpu_worker_circuit_state(paths)
    now_epoch = time.time()
    opened_until_epoch = float(state.get("opened_until_epoch") or 0.0)
    if opened_until_epoch <= now_epoch:
        # Auto-heal circuit state once cooldown expires.
        if state:
            _write_gpu_worker_circuit_state(
                paths,
                {
                    "consecutive_failures": 0,
                    "opened_until_epoch": 0.0,
                    "last_error": "",
                    "updated_at_epoch": now_epoch,
                },
            )
        return False, 0.0
    return True, max(0.0, opened_until_epoch - now_epoch)


def _mark_gpu_worker_success(paths: Paths) -> None:
    if not GPU_WORKER_CIRCUIT_ENABLED:
        return
    _write_gpu_worker_circuit_state(
        paths,
        {
            "consecutive_failures": 0,
            "opened_until_epoch": 0.0,
            "last_error": "",
            "updated_at_epoch": time.time(),
        },
    )


def _mark_gpu_worker_failure(paths: Paths, *, error: str) -> None:
    if not GPU_WORKER_CIRCUIT_ENABLED:
        return
    state = _load_gpu_worker_circuit_state(paths)
    now_epoch = time.time()
    failures = int(state.get("consecutive_failures") or 0) + 1
    opened_until_epoch = float(state.get("opened_until_epoch") or 0.0)
    if failures >= int(GPU_WORKER_CIRCUIT_FAIL_THRESHOLD):
        opened_until_epoch = max(opened_until_epoch, now_epoch + float(GPU_WORKER_CIRCUIT_COOLDOWN_SECS))
    _write_gpu_worker_circuit_state(
        paths,
        {
            "consecutive_failures": failures,
            "opened_until_epoch": opened_until_epoch,
            "last_error": str(error or "")[:500],
            "updated_at_epoch": now_epoch,
        },
    )


def _resolve_gpu_worker_key_circuit_meta_path(paths: Paths) -> Path:
    return paths.meta / "gpu_worker_key_circuit.json"


def _load_gpu_worker_key_circuit_entries(paths: Paths) -> dict[str, dict[str, Any]]:
    meta_path = _resolve_gpu_worker_key_circuit_meta_path(paths)
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    raw_entries = payload.get("entries")
    if isinstance(raw_entries, dict):
        source = raw_entries
    else:
        source = payload

    out: dict[str, dict[str, Any]] = {}
    now_epoch = time.time()
    for raw_key, raw_state in source.items():
        key = str(raw_key or "").strip()
        if not key or not isinstance(raw_state, dict):
            continue
        try:
            failures = max(0, int(raw_state.get("consecutive_failures") or 0))
        except Exception:
            failures = 0
        try:
            opened_until_epoch = max(0.0, float(raw_state.get("opened_until_epoch") or 0.0))
        except Exception:
            opened_until_epoch = 0.0
        try:
            updated_at_epoch = max(0.0, float(raw_state.get("updated_at_epoch") or 0.0))
        except Exception:
            updated_at_epoch = 0.0
        if updated_at_epoch <= 0.0:
            updated_at_epoch = now_epoch
        out[key] = {
            "consecutive_failures": failures,
            "opened_until_epoch": opened_until_epoch,
            "last_error": str(raw_state.get("last_error") or "")[:500],
            "updated_at_epoch": updated_at_epoch,
        }
    return out


def _prune_gpu_worker_key_circuit_entries(
    entries: dict[str, dict[str, Any]],
    *,
    now_epoch: float,
) -> dict[str, dict[str, Any]]:
    if not entries:
        return {}
    max_age_secs = max(60.0, float(GPU_WORKER_KEY_CIRCUIT_MAX_AGE_SECS))
    survivors: list[tuple[float, str, dict[str, Any]]] = []
    for key, state in entries.items():
        try:
            opened_until = max(0.0, float(state.get("opened_until_epoch") or 0.0))
        except Exception:
            opened_until = 0.0
        try:
            updated_at = max(0.0, float(state.get("updated_at_epoch") or 0.0))
        except Exception:
            updated_at = 0.0
        freshness = max(updated_at, opened_until)
        if freshness <= 0.0:
            freshness = now_epoch
        if opened_until <= now_epoch and (now_epoch - freshness) > max_age_secs:
            continue
        survivors.append((freshness, key, state))

    if not survivors:
        return {}

    survivors.sort(key=lambda item: item[0], reverse=True)
    max_entries = max(1, int(GPU_WORKER_KEY_CIRCUIT_MAX_ENTRIES))
    trimmed = survivors[:max_entries]
    return {key: state for _freshness, key, state in trimmed}


def _write_gpu_worker_key_circuit_entries(paths: Paths, entries: dict[str, dict[str, Any]]) -> None:
    meta_path = _resolve_gpu_worker_key_circuit_meta_path(paths)
    payload = {
        "updated_at_epoch": time.time(),
        "entries": entries,
    }
    try:
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _gpu_worker_key_circuit_status(paths: Paths, *, key: str) -> tuple[bool, float]:
    if (not GPU_WORKER_KEY_CIRCUIT_ENABLED) or (not key):
        return False, 0.0
    now_epoch = time.time()
    with _GPU_WORKER_KEY_CIRCUIT_LOCK:
        entries = _load_gpu_worker_key_circuit_entries(paths)
        pruned = _prune_gpu_worker_key_circuit_entries(entries, now_epoch=now_epoch)
        dirty = pruned != entries
        entries = pruned

        state = entries.get(key)
        if not state:
            if dirty:
                _write_gpu_worker_key_circuit_entries(paths, entries)
            return False, 0.0

        opened_until_epoch = max(0.0, float(state.get("opened_until_epoch") or 0.0))
        if opened_until_epoch <= 0.0:
            if dirty:
                _write_gpu_worker_key_circuit_entries(paths, entries)
            return False, 0.0

        if opened_until_epoch <= now_epoch:
            entries.pop(key, None)
            _write_gpu_worker_key_circuit_entries(paths, entries)
            return False, 0.0

        if dirty:
            _write_gpu_worker_key_circuit_entries(paths, entries)
        return True, max(0.0, opened_until_epoch - now_epoch)


def _mark_gpu_worker_key_success(paths: Paths, *, key: str) -> None:
    if (not GPU_WORKER_KEY_CIRCUIT_ENABLED) or (not key):
        return
    now_epoch = time.time()
    with _GPU_WORKER_KEY_CIRCUIT_LOCK:
        entries = _load_gpu_worker_key_circuit_entries(paths)
        pruned = _prune_gpu_worker_key_circuit_entries(entries, now_epoch=now_epoch)
        existed = key in pruned
        if existed:
            pruned.pop(key, None)
        if existed or (pruned != entries):
            _write_gpu_worker_key_circuit_entries(paths, pruned)


def _mark_gpu_worker_key_failure(paths: Paths, *, key: str, error: str) -> None:
    if (not GPU_WORKER_KEY_CIRCUIT_ENABLED) or (not key):
        return
    now_epoch = time.time()
    with _GPU_WORKER_KEY_CIRCUIT_LOCK:
        entries = _load_gpu_worker_key_circuit_entries(paths)
        state = entries.get(key) if isinstance(entries.get(key), dict) else {}
        failures = int(state.get("consecutive_failures") or 0) + 1
        opened_until_epoch = float(state.get("opened_until_epoch") or 0.0)
        if failures >= int(GPU_WORKER_KEY_CIRCUIT_FAIL_THRESHOLD):
            opened_until_epoch = max(opened_until_epoch, now_epoch + float(GPU_WORKER_KEY_CIRCUIT_COOLDOWN_SECS))
        entries[key] = {
            "consecutive_failures": failures,
            "opened_until_epoch": opened_until_epoch,
            "last_error": str(error or "")[:500],
            "updated_at_epoch": now_epoch,
        }
        entries = _prune_gpu_worker_key_circuit_entries(entries, now_epoch=now_epoch)
        _write_gpu_worker_key_circuit_entries(paths, entries)


def _gpu_worker_retry_backoff_seconds(attempt_number: int) -> float:
    base = max(0.0, float(GPU_WORKER_RETRY_BACKOFF_SECS))
    if base <= 0.0:
        return 0.0
    cap = max(base, float(GPU_WORKER_RETRY_BACKOFF_MAX_SECS))
    exponent = max(0, int(attempt_number) - 1)
    return min(cap, base * (2 ** exponent))


def _resolve_global_stem_cache_root(paths: Paths) -> Path:
    return paths.separated / "htdemucs" / GLOBAL_STEM_CACHE_DIR_NAME


def _audio_sha256(src_audio: Path) -> str:
    stat = src_audio.stat()
    cache_key = (str(src_audio.resolve()), int(stat.st_size), int(stat.st_mtime_ns))
    with _AUDIO_SHA256_CACHE_LOCK:
        cached = _AUDIO_SHA256_CACHE.get(cache_key)
        if cached:
            return cached

    digest = hashlib.sha256()
    with src_audio.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    value = digest.hexdigest()
    with _AUDIO_SHA256_CACHE_LOCK:
        _AUDIO_SHA256_CACHE[cache_key] = value
    return value


def _global_stem_cache_key(
    src_audio: Path,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> str:
    stat = src_audio.stat()
    normalized_model = (str(model_version or "").strip() or "htdemucs").lower()
    effective_overlap = _effective_demucs_overlap(stem_profile)
    try:
        audio_sha = _audio_sha256(src_audio)
    except Exception:
        audio_sha = ""
    payload = {
        "audio_size": int(stat.st_size),
        "model_version": normalized_model,
        "two_stems": bool(_demucs_two_stems_enabled()),
        "shifts": int(DEMUCS_SHIFTS),
        "overlap": float(effective_overlap),
        "required_stems": _required_stem_names(),
    }
    if audio_sha:
        payload["audio_sha256"] = audio_sha
    else:
        payload["audio_path"] = str(src_audio.resolve())
        payload["audio_mtime_ns"] = int(stat.st_mtime_ns)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _resolve_global_stem_cache_dir(
    paths: Paths,
    src_audio: Path,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> Path:
    return _resolve_global_stem_cache_root(paths) / _global_stem_cache_key(
        src_audio,
        stem_profile=stem_profile,
        model_version=model_version,
    )


def _global_stem_cache_updated_epoch(cache_dir: Path, *, now_epoch: float) -> float:
    meta_path = cache_dir / ".cache_meta.json"
    try:
        if meta_path.exists():
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                updated_epoch = float(payload.get("published_at_epoch") or payload.get("updated_at_epoch") or 0.0)
                if updated_epoch > 0.0:
                    return updated_epoch
            return float(meta_path.stat().st_mtime)
    except Exception:
        pass
    try:
        return float(cache_dir.stat().st_mtime)
    except Exception:
        return now_epoch


def _prune_global_stem_cache(*, paths: Paths, preserve_dirs: list[Path]) -> None:
    global _GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO
    if not GLOBAL_STEM_CACHE_PRUNE_ENABLED:
        return

    now_mono = time.monotonic()
    if (now_mono - _GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO) < float(GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS):
        return

    with _GLOBAL_STEM_CACHE_PRUNE_LOCK:
        now_mono = time.monotonic()
        if (now_mono - _GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO) < float(GLOBAL_STEM_CACHE_PRUNE_INTERVAL_SECS):
            return
        _GLOBAL_STEM_CACHE_LAST_PRUNE_AT_MONO = now_mono

        cache_root = _resolve_global_stem_cache_root(paths)
        if not cache_root.exists():
            return

        keep = {str(p.resolve()) for p in preserve_dirs}
        now_epoch = time.time()
        survivors: list[tuple[float, Path]] = []

        children: list[Path] = []
        try:
            for idx, child in enumerate(cache_root.iterdir()):
                if idx >= int(GLOBAL_STEM_CACHE_PRUNE_SCAN_LIMIT):
                    break
                children.append(child)
        except Exception:
            return

        for child in children:
            try:
                if not child.is_dir():
                    continue
                if child.name.startswith(".tmp-"):
                    shutil.rmtree(child, ignore_errors=True)
                    continue

                resolved = str(child.resolve())
                if resolved in keep:
                    survivors.append((_global_stem_cache_updated_epoch(child, now_epoch=now_epoch), child))
                    continue

                if not (child / ".cache_meta.json").exists():
                    shutil.rmtree(child, ignore_errors=True)
                    continue
                try:
                    _validate_stem_dir(child)
                except Exception:
                    shutil.rmtree(child, ignore_errors=True)
                    continue

                updated_epoch = _global_stem_cache_updated_epoch(child, now_epoch=now_epoch)
                age_secs = max(0.0, now_epoch - float(updated_epoch))
                if age_secs > float(GLOBAL_STEM_CACHE_MAX_AGE_SECS):
                    shutil.rmtree(child, ignore_errors=True)
                    continue
                survivors.append((updated_epoch, child))
            except Exception:
                continue

        max_entries = int(max(1, GLOBAL_STEM_CACHE_MAX_ENTRIES))
        if len(survivors) <= max_entries:
            return
        survivors.sort(key=lambda item: item[0], reverse=True)
        for _updated, stale_dir in survivors[max_entries:]:
            if str(stale_dir.resolve()) in keep:
                continue
            shutil.rmtree(stale_dir, ignore_errors=True)


def _demucs_singleflight_key(
    src_audio: Path,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> str:
    effective_overlap = _effective_demucs_overlap(stem_profile)
    try:
        return _global_stem_cache_key(
            src_audio,
            stem_profile=stem_profile,
            model_version=model_version,
        )
    except Exception:
        try:
            stat = src_audio.stat()
            payload = {
                "audio_path": str(src_audio.resolve()),
                "audio_size": int(stat.st_size),
                "audio_mtime_ns": int(stat.st_mtime_ns),
                "two_stems": bool(_demucs_two_stems_enabled()),
                "shifts": int(DEMUCS_SHIFTS),
                "overlap": float(effective_overlap),
                "required_stems": _required_stem_names(),
            }
        except Exception:
            payload = {
                "audio_path": str(src_audio),
                "two_stems": bool(_demucs_two_stems_enabled()),
                "shifts": int(DEMUCS_SHIFTS),
                "overlap": float(effective_overlap),
                "required_stems": _required_stem_names(),
            }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _acquire_demucs_singleflight_lock(key: str) -> threading.Lock:
    with _DEMUCS_SINGLEFLIGHT_LOCK:
        lock = _DEMUCS_SINGLEFLIGHT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _DEMUCS_SINGLEFLIGHT_LOCKS[key] = lock
            _DEMUCS_SINGLEFLIGHT_REFS[key] = 0
        _DEMUCS_SINGLEFLIGHT_REFS[key] = int(_DEMUCS_SINGLEFLIGHT_REFS.get(key) or 0) + 1
    lock.acquire()
    return lock


def _release_demucs_singleflight_lock(key: str, lock: threading.Lock) -> None:
    try:
        lock.release()
    finally:
        with _DEMUCS_SINGLEFLIGHT_LOCK:
            refs = max(0, int(_DEMUCS_SINGLEFLIGHT_REFS.get(key) or 0) - 1)
            if refs <= 0:
                _DEMUCS_SINGLEFLIGHT_REFS.pop(key, None)
                if not lock.locked():
                    _DEMUCS_SINGLEFLIGHT_LOCKS.pop(key, None)
            else:
                _DEMUCS_SINGLEFLIGHT_REFS[key] = refs


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        try:
            dst.unlink()
        except Exception:
            pass
    if GLOBAL_STEM_CACHE_USE_HARDLINKS:
        try:
            os.link(str(src), str(dst))
            return
        except Exception:
            pass
    shutil.copyfile(src, dst)


def _restore_stems_from_global_cache(
    paths: Paths,
    slug: str,
    src_audio: Path,
    *,
    force: bool,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> bool:
    if not GLOBAL_STEM_CACHE_ENABLED or force:
        return False
    cache_dir = _resolve_global_stem_cache_dir(
        paths,
        src_audio,
        stem_profile=stem_profile,
        model_version=model_version,
    )
    if not cache_dir.exists():
        return False
    try:
        _validate_stem_dir(cache_dir)
    except Exception:
        return False

    stem_dir = _resolve_stem_dir(paths, slug)
    stem_dir.mkdir(parents=True, exist_ok=True)
    try:
        for name in _required_stem_names():
            _link_or_copy(cache_dir / f"{name}.wav", stem_dir / f"{name}.wav")
        _validate_stem_dir(stem_dir)
        _write_stem_cache_meta(
            paths,
            slug,
            src_audio,
            backend="global_stem_cache",
            stem_profile=stem_profile,
            model_version=model_version,
        )
        _prune_global_stem_cache(paths=paths, preserve_dirs=[cache_dir])
        log("SPLIT", f"Restored stems from global cache key={cache_dir.name}", GREEN)
        return True
    except Exception:
        _cleanup_stem_dir_for_retry(stem_dir)
        return False


def _publish_stems_to_global_cache(
    paths: Paths,
    slug: str,
    src_audio: Path,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> None:
    if not GLOBAL_STEM_CACHE_ENABLED:
        return
    stem_dir = _resolve_stem_dir(paths, slug)
    try:
        _validate_stem_dir(stem_dir)
    except Exception:
        return

    cache_root = _resolve_global_stem_cache_root(paths)
    cache_dir = _resolve_global_stem_cache_dir(
        paths,
        src_audio,
        stem_profile=stem_profile,
        model_version=model_version,
    )
    if cache_dir.exists():
        _prune_global_stem_cache(paths=paths, preserve_dirs=[cache_dir])
        return

    tmp_dir = cache_root / f".tmp-{cache_dir.name}-{os.getpid()}-{int(now_perf_ms())}"
    try:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        for name in _required_stem_names():
            _link_or_copy(stem_dir / f"{name}.wav", tmp_dir / f"{name}.wav")
        meta_payload = _current_stem_cache_meta(
            src_audio,
            stem_profile=stem_profile,
            model_version=model_version,
        )
        meta_payload["published_at_epoch"] = time.time()
        (tmp_dir / ".cache_meta.json").write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
        cache_root.mkdir(parents=True, exist_ok=True)
        tmp_dir.replace(cache_dir)
        _prune_global_stem_cache(paths=paths, preserve_dirs=[cache_dir])
    except FileExistsError:
        _prune_global_stem_cache(paths=paths, preserve_dirs=[cache_dir])
    except Exception:
        pass
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _current_stem_cache_meta(
    src_audio: Path,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> dict[str, Any]:
    stat = src_audio.stat()
    normalized_model = (str(model_version or "").strip() or "htdemucs").lower()
    effective_overlap = _effective_demucs_overlap(stem_profile)
    payload = {
        "audio_path": str(src_audio.resolve()),
        "audio_size": int(stat.st_size),
        "audio_mtime_ns": int(stat.st_mtime_ns),
        "model_version": normalized_model,
        "two_stems": bool(_demucs_two_stems_enabled()),
        "shifts": int(DEMUCS_SHIFTS),
        "overlap": float(effective_overlap),
        "required_stems": _required_stem_names(),
    }
    try:
        payload["audio_sha256"] = _audio_sha256(src_audio)
    except Exception:
        pass
    return payload


def _load_stem_cache_meta(paths: Paths, slug: str) -> dict[str, Any]:
    meta_path = _resolve_stem_cache_meta_path(paths, slug)
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_stem_cache_meta(
    paths: Paths,
    slug: str,
    src_audio: Path,
    *,
    backend: str,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> None:
    try:
        meta_path = _resolve_stem_cache_meta_path(paths, slug)
        payload = _current_stem_cache_meta(
            src_audio,
            stem_profile=stem_profile,
            model_version=model_version,
        )
        payload.update(
            {
                "backend": backend,
                "updated_at_epoch_ms": int(now_perf_ms()),
            }
        )
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as exc:
        log("SPLIT", f"Could not write stems cache metadata for slug={slug}: {exc}", YELLOW)


def _missing_stems(stem_dir: Path) -> list[str]:
    return [name for name in _required_stem_names() if not (stem_dir / f"{name}.wav").exists()]


def _validate_stem_dir(stem_dir: Path) -> None:
    if not stem_dir.exists():
        raise RuntimeError(f"Demucs output directory not found: {stem_dir}")
    missing = _missing_stems(stem_dir)
    if missing:
        raise RuntimeError(f"Demucs stems missing in {stem_dir}: {missing}")
    for name in _required_stem_names():
        wav_path = stem_dir / f"{name}.wav"
        try:
            size = int(wav_path.stat().st_size)
        except Exception as exc:
            raise RuntimeError(f"Demucs stem unreadable in {stem_dir}: {wav_path.name}") from exc
        if size < int(GPU_STEM_MIN_BYTES):
            raise RuntimeError(
                f"Demucs stem too small in {stem_dir}: {wav_path.name} ({size} bytes)"
            )


def _cleanup_stem_dir_for_retry(stem_dir: Path) -> None:
    if not stem_dir.exists():
        return
    for name in _required_stem_names():
        target = stem_dir / f"{name}.wav"
        try:
            target.unlink(missing_ok=True)
        except Exception:
            pass
    for tmp in stem_dir.glob("*.part"):
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _can_reuse_stem_cache(
    paths: Paths,
    slug: str,
    src_audio: Path,
    *,
    force: bool,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> bool:
    stem_dir = _resolve_stem_dir(paths, slug)
    if force:
        return False
    missing = _missing_stems(stem_dir)
    if missing:
        return False

    cached_meta = _load_stem_cache_meta(paths, slug)
    if not cached_meta:
        # Backward-compatible cache behavior for stems generated before cache-key metadata existed.
        return True

    try:
        expected = _current_stem_cache_meta(
            src_audio,
            stem_profile=stem_profile,
            model_version=model_version,
        )
    except Exception:
        return False
    for key, expected_value in expected.items():
        if key == "audio_sha256" and not cached_meta.get(key):
            # Backward compatibility for cache metadata produced before sha256 was introduced.
            continue
        if cached_meta.get(key) != expected_value:
            return False
    return True


def _is_local_http_url(raw: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _is_worker_auth_or_config_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    if not msg:
        return False
    markers = (
        "hmac required but worker secret is not configured",
        "worker hmac secret is required but not configured",
        "missing signature",
        "signature mismatch",
        "missing bearer token",
        "invalid bearer token",
        "worker authentication is not configured",
        "worker http 401",
        "worker http 403",
    )
    return any(marker in msg for marker in markers)


def _request_gpu_worker(config: GPUWorkerConfig, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed_url = urllib.parse.urlparse(config.url)
    except Exception as exc:
        raise RuntimeError(f"worker URL is invalid: {config.url}") from exc
    scheme = (parsed_url.scheme or "").strip().lower()
    if scheme not in {"https", "http"}:
        raise RuntimeError("worker URL must use http or https")
    if scheme == "http" and not (GPU_WORKER_ALLOW_INSECURE_HTTP or _is_local_http_url(config.url)):
        raise RuntimeError("worker URL must use https (or localhost http with explicit allow)")

    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    if config.hmac_secret:
        headers.update(build_signed_headers(body_bytes=data, secret=config.hmac_secret))
    elif config.require_hmac:
        raise RuntimeError("worker HMAC secret is required but not configured")

    req = urllib.request.Request(config.url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=float(config.timeout_secs)) as resp:
            body_bytes = resp.read(int(GPU_WORKER_MAX_RESPONSE_BYTES) + 1)
            if len(body_bytes) > int(GPU_WORKER_MAX_RESPONSE_BYTES):
                raise RuntimeError(
                    f"worker response exceeded {int(GPU_WORKER_MAX_RESPONSE_BYTES)} bytes"
                )
            body = body_bytes.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        detail = (body or str(exc)).strip()
        raise RuntimeError(f"worker HTTP {exc.code}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise RuntimeError(f"worker network error: {reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError("worker request timed out") from exc
    except Exception as exc:
        raise RuntimeError(f"worker request failed: {exc}") from exc

    try:
        parsed = json.loads(body or "{}")
    except Exception as exc:
        raise RuntimeError(f"worker returned non-JSON response: {(body or '').strip()[:220]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("worker response must be a JSON object")
    if isinstance(parsed.get("result"), dict):
        merged: dict[str, Any] = dict(parsed.get("result") or {})
        if "ok" not in merged and "ok" in parsed:
            merged["ok"] = parsed.get("ok")
        if "status" not in merged and "status" in parsed:
            merged["status"] = parsed.get("status")
        parsed = merged
    if parsed.get("ok") is False:
        detail = str(parsed.get("error") or parsed.get("detail") or "worker reported failure")
        raise RuntimeError(detail[:500])
    if str(parsed.get("status") or "").strip().lower() in {"error", "failed"}:
        detail = str(parsed.get("error") or parsed.get("detail") or parsed.get("status") or "worker reported failure")
        raise RuntimeError(detail[:500])
    return parsed


def _download_worker_stem(url: str, out_path: Path, timeout_secs: float) -> None:
    raw_url = str(url or "").strip()
    if not raw_url:
        raise RuntimeError("worker stem URI was empty")
    if is_gs_uri(raw_url):
        tmp_out = out_path.with_suffix(out_path.suffix + ".part")
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            gcs_download_file(raw_url, tmp_out)
            size = int(tmp_out.stat().st_size) if tmp_out.exists() else 0
            if size < int(GPU_STEM_MIN_BYTES):
                raise RuntimeError(f"worker stem download was too small: {raw_url} ({size} bytes)")
            tmp_out.replace(out_path)
            return
        except Exception as exc:
            raise RuntimeError(f"failed downloading worker stem from {raw_url}: {exc}") from exc
        finally:
            try:
                tmp_out.unlink(missing_ok=True)
            except Exception:
                pass

    if raw_url.startswith("file://"):
        local_path = Path(urllib.parse.urlparse(raw_url).path or "")
        if not local_path.exists():
            raise RuntimeError(f"worker stem local file missing: {raw_url}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, out_path)
        size = int(out_path.stat().st_size) if out_path.exists() else 0
        if size < int(GPU_STEM_MIN_BYTES):
            raise RuntimeError(f"worker stem download was too small: {raw_url} ({size} bytes)")
        return
    if (not raw_url.startswith("http://")) and (not raw_url.startswith("https://")):
        local_path = Path(raw_url)
        if local_path.exists():
            out_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path, out_path)
            size = int(out_path.stat().st_size) if out_path.exists() else 0
            if size < int(GPU_STEM_MIN_BYTES):
                raise RuntimeError(f"worker stem download was too small: {raw_url} ({size} bytes)")
            return

    req = urllib.request.Request(url, headers={"Accept": "audio/wav"})
    tmp_out = out_path.with_suffix(out_path.suffix + ".part")
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_secs)) as resp:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_out.open("wb") as fh:
                total = 0
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    fh.write(chunk)
            if total < int(GPU_STEM_MIN_BYTES):
                raise RuntimeError(f"worker stem download was too small: {url} ({total} bytes)")
            tmp_out.replace(out_path)
    except Exception as exc:
        raise RuntimeError(f"failed downloading worker stem from {url}: {exc}") from exc
    finally:
        try:
            tmp_out.unlink(missing_ok=True)
        except Exception:
            pass


def _materialize_worker_stems(
    *,
    paths: Paths,
    slug: str,
    payload: dict[str, Any],
    timeout_secs: float,
) -> Path:
    stem_dir = _resolve_stem_dir(paths, slug)
    stem_dir.mkdir(parents=True, exist_ok=True)

    response_stem_dir = str(payload.get("stems_dir") or "").strip()
    if response_stem_dir:
        candidate_dir = Path(response_stem_dir)
        if candidate_dir.exists() and candidate_dir.is_dir():
            if candidate_dir != stem_dir:
                for name in _required_stem_names():
                    src = candidate_dir / f"{name}.wav"
                    if src.exists():
                        shutil.copyfile(src, stem_dir / f"{name}.wav")
            # Worker may echo back the caller-provided output_dir path even when stems
            # are only available remotely (stems_uris). Validate, but if stems are
            # missing fall through and materialize from URIs below.
            try:
                _validate_stem_dir(stem_dir)
                return stem_dir
            except Exception:
                pass

    stems_payload = payload.get("stems")
    stems: dict[str, Any] = stems_payload if isinstance(stems_payload, dict) else {}
    stems_uris_payload = payload.get("stems_uris")
    stems_uris: dict[str, Any] = stems_uris_payload if isinstance(stems_uris_payload, dict) else {}
    for name in _required_stem_names():
        dest = stem_dir / f"{name}.wav"
        path_candidate = str(
            stems.get(f"{name}_path")
            or stems.get(name)
            or payload.get(f"{name}_path")
            or payload.get(name)
            or ""
        ).strip()
        if path_candidate and Path(path_candidate).exists():
            src = Path(path_candidate)
            if src.resolve() != dest.resolve():
                shutil.copyfile(src, dest)
            continue

        url_candidate = str(
            stems_uris.get(name)
            or stems_uris.get(f"{name}_uri")
            or stems.get(f"{name}_url")
            or stems.get(f"{name}_uri")
            or payload.get(f"{name}_url")
            or payload.get(f"{name}_uri")
            or ""
        ).strip()
        if url_candidate:
            _download_worker_stem(url_candidate, dest, timeout_secs=timeout_secs)
            continue

    _validate_stem_dir(stem_dir)
    return stem_dir


def _ensure_demucs_stems_via_worker(
    paths: Paths,
    slug: str,
    src_audio: Path,
    flags: IOFlags,
    config: GPUWorkerConfig,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> Path:
    stem_dir = _resolve_stem_dir(paths, slug)
    src_audio = _refresh_source_audio_if_missing(paths, slug, src_audio)
    normalized_profile = _normalize_stem_profile(stem_profile)
    normalized_model = (str(model_version or config.model_version or "htdemucs").strip() or "htdemucs").lower()
    if _can_reuse_stem_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=normalized_profile,
        model_version=normalized_model,
    ):
        log("SPLIT", f"Using existing stems cache: {stem_dir}", GREEN)
        return stem_dir
    if _restore_stems_from_global_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=normalized_profile,
        model_version=normalized_model,
    ):
        return stem_dir

    try:
        worker_key = _global_stem_cache_key(
            src_audio,
            stem_profile=normalized_profile,
            model_version=normalized_model,
        )
    except Exception:
        worker_key = ""

    if flags.dry_run:
        log("SPLIT", f"[dry-run] Would offload Demucs to GPU worker for slug={slug}", YELLOW)
        return stem_dir

    circuit_open, cooldown_left = _gpu_worker_circuit_status(paths)
    if circuit_open:
        raise RuntimeError(
            f"GPU worker circuit open for {int(cooldown_left)}s after repeated failures"
        )
    key_circuit_open, key_cooldown_left = _gpu_worker_key_circuit_status(paths, key=worker_key)
    if key_circuit_open:
        key_hint = worker_key[:8] if worker_key else "unknown"
        raise RuntimeError(
            f"GPU worker key circuit open key={key_hint} for {int(key_cooldown_left)}s after repeated failures"
        )

    requested_device = _normalize_device(
        os.environ.get("MIXTERIOSO_DEMUCS_DEVICE") or os.environ.get("KARAOAPI_DEMUCS_DEVICE")
    ) or "auto"
    payload = {
        "job_id": str(os.environ.get("MIXTERIOSO_JOB_ID", "") or f"{slug}-{uuid.uuid4().hex[:8]}"),
        "slug": slug,
        "audio_path": str(src_audio),  # legacy compatibility
        "output_dir": str(stem_dir),   # legacy compatibility
        "model": normalized_model,     # legacy compatibility
        "model_version": normalized_model,
        "stem_profile": normalized_profile,
        "two_stems": bool(_demucs_two_stems_enabled()),
        "shifts": int(DEMUCS_SHIFTS),
        "overlap": float(_effective_demucs_overlap(normalized_profile)),
        "requested_stems": _required_stem_names(),
        "device": requested_device,
    }
    if config.stems_bucket:
        payload["stems_bucket"] = config.stems_bucket

    if config.source_bucket:
        source_sha = _audio_sha256(src_audio)
        src_uri = build_source_object_uri(
            source_bucket=config.source_bucket,
            source_sha256=source_sha,
            suffix=(src_audio.suffix or ".bin"),
        )
        try:
            if not gcs_object_exists(src_uri):
                gcs_upload_file(src_uri, src_audio, if_absent=True)
        except Exception as exc:
            raise RuntimeError(f"could not upload source audio to object storage: {exc}") from exc
        payload["source_uri"] = src_uri
        payload["source_sha256"] = source_sha
    elif GPU_WORKER_INLINE_SOURCE_ENABLED:
        try:
            src_size = int(src_audio.stat().st_size)
        except Exception:
            src_size = -1
        if src_size <= 0:
            raise RuntimeError(f"source audio is empty or unreadable: {src_audio}")
        if src_size > int(GPU_WORKER_INLINE_SOURCE_MAX_BYTES):
            raise RuntimeError(
                "source audio too large for inline worker payload "
                f"({src_size} bytes > {int(GPU_WORKER_INLINE_SOURCE_MAX_BYTES)} bytes)"
            )
        source_sha = _audio_sha256(src_audio)
        try:
            inline_b64 = base64.b64encode(src_audio.read_bytes()).decode("ascii")
        except Exception as exc:
            raise RuntimeError(f"could not encode inline worker source audio: {exc}") from exc
        payload["inline_source_b64"] = inline_b64
        payload["inline_source_name"] = src_audio.name
        payload["source_sha256"] = source_sha

    attempts = max(1, int(config.retries) + 1)
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            _cleanup_stem_dir_for_retry(stem_dir)
            call_t0 = now_perf_ms()
            log("SPLIT", f"GPU worker separation start slug={slug} attempt={attempt}/{attempts}", WHITE)
            response = _request_gpu_worker(config, payload)
            result_dir = _materialize_worker_stems(
                paths=paths,
                slug=slug,
                payload=response,
                timeout_secs=config.timeout_secs,
            )
            elapsed_ms = max(0.0, now_perf_ms() - call_t0)
            log(
                "SPLIT",
                f"GPU worker separation done slug={slug} attempt={attempt}/{attempts} elapsed_ms={elapsed_ms:.1f}",
                GREEN,
            )
            _mark_gpu_worker_success(paths)
            _mark_gpu_worker_key_success(paths, key=worker_key)
            _write_stem_cache_meta(
                paths,
                slug,
                src_audio,
                backend="gpu_worker",
                stem_profile=normalized_profile,
                model_version=normalized_model,
            )
            _publish_stems_to_global_cache(
                paths,
                slug,
                src_audio,
                stem_profile=normalized_profile,
                model_version=normalized_model,
            )
            return result_dir
        except Exception as exc:
            last_error = exc
            _mark_gpu_worker_failure(paths, error=str(exc))
            _mark_gpu_worker_key_failure(paths, key=worker_key, error=str(exc))
            log(
                "SPLIT",
                f"GPU worker separation failed slug={slug} attempt={attempt}/{attempts} reason={exc}",
                YELLOW,
            )
            _cleanup_stem_dir_for_retry(stem_dir)
            if attempt >= attempts:
                continue
            key_circuit_open, key_cooldown_left = _gpu_worker_key_circuit_status(paths, key=worker_key)
            if key_circuit_open:
                key_hint = worker_key[:8] if worker_key else "unknown"
                log(
                    "SPLIT",
                    (
                        "GPU worker key circuit opened key=%s during retries; "
                        "aborting remaining attempts for %ds"
                    )
                    % (key_hint, int(key_cooldown_left)),
                    YELLOW,
                )
                break
            circuit_open, cooldown_left = _gpu_worker_circuit_status(paths)
            if circuit_open:
                log(
                    "SPLIT",
                    f"GPU worker circuit opened during retries; aborting remaining attempts for {int(cooldown_left)}s",
                    YELLOW,
                )
                break
            sleep_secs = _gpu_worker_retry_backoff_seconds(attempt)
            if sleep_secs > 0.0:
                log(
                    "SPLIT",
                    f"GPU worker retry backoff slug={slug} next_attempt={attempt + 1}/{attempts} sleep={sleep_secs:.1f}s",
                    YELLOW,
                )
                time.sleep(sleep_secs)

    raise RuntimeError(f"GPU worker separation failed after {attempts} attempt(s): {last_error}")


def _ensure_demucs_stems(
    paths: Paths,
    slug: str,
    src_audio: Path,
    flags: IOFlags,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> Path:
    """
    Ensure Demucs stems exist and return the stem directory containing vocals/bass/drums/other WAVs.

    Expected layout (Demucs default):
      separated/DEFAULT_DEMUCS_MODEL/<slug>/{vocals,bass,drums,other}.wav
    """
    model = (str(model_version or "htdemucs").strip() or "htdemucs")
    normalized_profile = _normalize_stem_profile(stem_profile)
    normalized_model = (str(model).strip() or "htdemucs").lower()
    stem_dir = _resolve_stem_dir(paths, slug)
    src_audio = _refresh_source_audio_if_missing(paths, slug, src_audio)
    effective_overlap = _effective_demucs_overlap(normalized_profile)

    if _can_reuse_stem_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=normalized_profile,
        model_version=normalized_model,
    ):
        log("SPLIT", f"Using existing stems cache: {stem_dir}", GREEN)
        return stem_dir
    if _restore_stems_from_global_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=normalized_profile,
        model_version=normalized_model,
    ):
        return stem_dir

    demucs_bin = resolve_demucs_bin()
    if not demucs_bin.exists() and not have_exe(str(demucs_bin)):
        raise RuntimeError("demucs not found on PATH (required for --mix-mode stems or stem level overrides)")
    _cleanup_stem_dir_for_retry(stem_dir)

    device = _resolve_demucs_device()
    with _canonical_demucs_input_for_slug(
        paths=paths,
        slug=slug,
        src_audio=src_audio,
        dry_run=flags.dry_run,
    ) as demucs_src_audio:
        if demucs_src_audio != src_audio:
            log(
                "SPLIT",
                f"Canonicalizing Demucs input name for slug={slug}: {src_audio.name} -> {demucs_src_audio.name}",
                WHITE,
            )

        cmd = [
            str(demucs_bin),
            "-n",
            model,
            "--shifts",
            str(DEMUCS_SHIFTS),
            "--overlap",
            str(effective_overlap),
            "-d",
            device,
            "-o",
            str(paths.separated),
        ]

        # Add two-stems mode for 2x speedup (only extract vocals + instrumental)
        if _demucs_two_stems_enabled():
            cmd.extend(["--two-stems", "vocals"])

        cmd.append(str(demucs_src_audio))

        mode_desc = "two-stems (vocals+instrumental)" if _demucs_two_stems_enabled() else "four-stems"
        log("SPLIT", f"Running Demucs ({model}, {mode_desc}, shifts={DEMUCS_SHIFTS}, overlap={effective_overlap}, device={device}) -> {paths.separated}", WHITE)
        _run_checked(cmd, tag="DEMUCS", dry_run=flags.dry_run, action="demucs separation")

    if flags.dry_run:
        return stem_dir

    _validate_stem_dir(stem_dir)
    _write_stem_cache_meta(
        paths,
        slug,
        src_audio,
        backend="local_demucs",
        stem_profile=normalized_profile,
        model_version=normalized_model,
    )
    _publish_stems_to_global_cache(
        paths,
        slug,
        src_audio,
        stem_profile=normalized_profile,
        model_version=normalized_model,
    )

    return stem_dir


def _effective_demucs_backend(paths: Paths, slug: str, *, default: str) -> str:
    meta = _load_stem_cache_meta(paths, slug)
    backend = str(meta.get("backend") or "").strip()
    return backend or default


def _ensure_demucs_stems_with_policy(
    paths: Paths,
    slug: str,
    src_audio: Path,
    flags: IOFlags,
    worker_cfg: GPUWorkerConfig,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> tuple[Path, str]:
    if worker_cfg.url:
        try:
            stem_dir = _ensure_demucs_stems_via_worker(
                paths,
                slug,
                src_audio,
                flags,
                worker_cfg,
                stem_profile=stem_profile,
                model_version=model_version,
            )
            return stem_dir, _effective_demucs_backend(paths, slug, default="gpu_worker")
        except Exception as worker_exc:
            force_cpu_fallback = (not worker_cfg.fallback_to_cpu) and _is_worker_auth_or_config_error(worker_exc)
            if worker_cfg.fallback_to_cpu or force_cpu_fallback:
                if force_cpu_fallback:
                    log(
                        "SPLIT",
                        (
                            "GPU worker auth/config failure detected for slug=%s; "
                            "overriding strict worker mode and falling back to local Demucs: %s"
                        )
                        % (slug, worker_exc),
                        YELLOW,
                    )
                else:
                    log(
                        "SPLIT",
                        f"GPU worker unavailable for slug={slug}; falling back to local Demucs: {worker_exc}",
                        YELLOW,
                    )
                stem_dir = _ensure_demucs_stems(
                    paths,
                    slug,
                    src_audio,
                    flags,
                    stem_profile=stem_profile,
                    model_version=model_version,
                )
                return stem_dir, _effective_demucs_backend(paths, slug, default="local_demucs_fallback")
            raise RuntimeError(
                f"GPU worker failed and CPU fallback is disabled: {worker_exc}"
            ) from worker_exc

    stem_dir = _ensure_demucs_stems(
        paths,
        slug,
        src_audio,
        flags,
        stem_profile=stem_profile,
        model_version=model_version,
    )
    return stem_dir, _effective_demucs_backend(paths, slug, default="local_demucs")


def _ensure_demucs_stems_singleflight(
    paths: Paths,
    slug: str,
    src_audio: Path,
    flags: IOFlags,
    worker_cfg: GPUWorkerConfig,
    *,
    stem_profile: Optional[dict[str, Any]] = None,
    model_version: str = "",
) -> tuple[Path, str]:
    stem_dir = _resolve_stem_dir(paths, slug)
    if _can_reuse_stem_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=stem_profile,
        model_version=model_version,
    ):
        log("SPLIT", f"Using existing stems cache: {stem_dir}", GREEN)
        return stem_dir, _effective_demucs_backend(paths, slug, default="local_stem_cache")
    if _restore_stems_from_global_cache(
        paths,
        slug,
        src_audio,
        force=flags.force,
        stem_profile=stem_profile,
        model_version=model_version,
    ):
        return stem_dir, "global_stem_cache"

    if flags.dry_run or (not DEMUCS_SINGLEFLIGHT_ENABLED):
        return _ensure_demucs_stems_with_policy(
            paths,
            slug,
            src_audio,
            flags,
            worker_cfg,
            stem_profile=stem_profile,
            model_version=model_version,
        )

    key = _demucs_singleflight_key(
        src_audio,
        stem_profile=stem_profile,
        model_version=model_version,
    )
    wait_t0 = now_perf_ms()
    lock = _acquire_demucs_singleflight_lock(key)
    waited_ms = max(0.0, now_perf_ms() - wait_t0)
    if waited_ms >= 20.0:
        log("SPLIT", f"Waiting for in-flight separation key={key[:8]} wait_ms={waited_ms:.1f}", WHITE)

    try:
        # Another request may have completed while we were waiting for this key.
        if _can_reuse_stem_cache(
            paths,
            slug,
            src_audio,
            force=flags.force,
            stem_profile=stem_profile,
            model_version=model_version,
        ):
            log("SPLIT", f"Using existing stems cache: {stem_dir}", GREEN)
            return stem_dir, _effective_demucs_backend(paths, slug, default="local_stem_cache")
        if _restore_stems_from_global_cache(
            paths,
            slug,
            src_audio,
            force=flags.force,
            stem_profile=stem_profile,
            model_version=model_version,
        ):
            return stem_dir, "global_stem_cache"
        return _ensure_demucs_stems_with_policy(
            paths,
            slug,
            src_audio,
            flags,
            worker_cfg,
            stem_profile=stem_profile,
            model_version=model_version,
        )
    finally:
        _release_demucs_singleflight_lock(key, lock)


def _mix_stems_to_wav(
    *,
    vocals_wav: Path,
    bass_wav: Path,
    drums_wav: Path,
    other_wav: Path,
    vocals_pct: float,
    bass_pct: float,
    drums_pct: float,
    other_pct: float,
    out_wav: Path,
    flags: IOFlags
) -> None:
    if not have_exe("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH (required for stems mixing)")

    # Check if we're in two-stems mode (bass, drums, other all point to same file)
    two_stems_mode = bass_wav == drums_wav == other_wav and bass_wav != vocals_wav

    if two_stems_mode:
        # Two-stems mode: vocals + no_vocals (instrumental)
        # Two-stems mode cannot independently adjust bass/drums/other.
        # This should stay equal because caller disables two-stems for non-vocal changes.
        vg = _pct_to_gain(vocals_pct)
        instrumental_levels = (float(bass_pct), float(drums_pct), float(other_pct))
        if max(instrumental_levels) - min(instrumental_levels) > 1e-6:
            raise RuntimeError(
                "Two-stems mode cannot apply independent bass/drums/other levels. "
                "Retry with four-stem separation."
            )
        instrumental_pct = instrumental_levels[0]
        ig = _pct_to_gain(instrumental_pct)

        fc = (
            f"[0:a]volume={vg}[v];"
            f"[1:a]volume={ig}[i];"
            f"[v][i]amix=inputs=2:normalize=0,alimiter=limit=0.98"
        )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(vocals_wav),
            "-i",
            str(bass_wav),  # This is actually no_vocals.wav
            "-filter_complex",
            fc,
            "-c:a",
            "pcm_s16le",
            str(out_wav),
        ]
    else:
        # Four-stems mode: original behavior
        vg = _pct_to_gain(vocals_pct)
        bg = _pct_to_gain(bass_pct)
        dg = _pct_to_gain(drums_pct)
        og = _pct_to_gain(other_pct)

        # Use linear volume factors (percentages), NOT dB.
        fc = (
            f"[0:a]volume={vg}[v];"
            f"[1:a]volume={bg}[b];"
            f"[2:a]volume={dg}[d];"
            f"[3:a]volume={og}[o];"
            f"[v][b][d][o]amix=inputs=4:normalize=0,alimiter=limit=0.98"
        )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(vocals_wav),
            "-i",
            str(bass_wav),
            "-i",
            str(drums_wav),
            "-i",
            str(other_wav),
            "-filter_complex",
            fc,
            "-c:a",
            "pcm_s16le",
            str(out_wav),
        ]

    log(
        "MIX",
        f"Stems mix -> {out_wav.name} | vocals={vocals_pct:.0f}% bass={bass_pct:.0f}% drums={drums_pct:.0f}% other={other_pct:.0f}%",
        WHITE,
    )
    _run_checked(cmd, tag="FFMPEG", dry_run=flags.dry_run, action="ffmpeg stems mix")

    if not flags.dry_run and not out_wav.exists():
        raise RuntimeError(f"Failed to produce {out_wav}")


def _mix_vocals_only_fallback_to_wav(
    *,
    src_audio: Path,
    vocals_pct: float,
    out_wav: Path,
    flags: IOFlags,
) -> None:
    """Fallback mix when Demucs is unavailable: suppress center channel vocals via ffmpeg."""
    if not have_exe("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH (required for fallback vocals mix)")

    vocals_pct_num = _normalize_stem_pct(vocals_pct)
    if vocals_pct_num > 100.0 + 1e-6:
        raise RuntimeError(
            "Demucs runtime unavailable and vocals boost (>100%) was requested. "
            "Please retry when stem separation is available."
        )
    # Use mid/side attenuation so vocals_pct is monotonic and predictable:
    # - 100% => original stereo (no attenuation)
    # - 0%   => maximum center reduction
    #
    # Pure side-only at 0% can sound phasey/"alien", so we blend back a small
    # amount of the original signal for better listening quality.
    center_gain = _pct_to_gain(vocals_pct_num)
    coef_main = 0.5 * (1.0 + center_gain)
    coef_cross = 0.5 * (center_gain - 1.0)
    dry_blend = 0.22 * (1.0 - center_gain)
    fc = (
        "[0:a]asplit=2[dry_src][proc_src];"
        f"[proc_src]pan=stereo|c0={coef_main:.6f}*FL+{coef_cross:.6f}*FR|"
        f"c1={coef_cross:.6f}*FL+{coef_main:.6f}*FR[proc];"
        f"[dry_src]volume={dry_blend:.6f}[dry];"
        "[proc][dry]amix=inputs=2:normalize=0,"
        "alimiter=limit=0.98"
    )
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(src_audio),
        "-filter_complex",
        fc,
        "-c:a",
        "pcm_s16le",
        str(out_wav),
    ]
    log(
        "MIX",
        (
            f"Fallback vocals mix -> {out_wav.name} | vocals={vocals_pct_num:.0f}% "
            f"(mid/side center attenuation, gain={center_gain:.3f}, dry_blend={dry_blend:.3f})"
        ),
        YELLOW,
    )
    _run_checked(cmd, tag="FFMPEG", dry_run=flags.dry_run, action="ffmpeg fallback vocals mix")
    if not flags.dry_run and not out_wav.exists():
        raise RuntimeError(f"Failed to produce {out_wav}")


def _apply_vocals_only_fallback_mix(
    *,
    paths: Paths,
    slug: str,
    src_audio: Path,
    out_wav: Path,
    out_mp3: Path,
    vocals: float,
    bass: float,
    drums: float,
    other: float,
    flags: IOFlags,
    reason_text: str,
    demucs_backend_label: str,
    demucs_error: str = "",
) -> None:
    skip_stems_mix_mp3 = _skip_stems_mix_mp3_enabled()
    src_audio = _refresh_source_audio_if_missing(paths, slug, src_audio)

    def _levels_match(meta_levels: dict[str, Any]) -> bool:
        try:
            return (
                abs(float(meta_levels.get("vocals", 100.0)) - float(vocals)) < 1e-6
                and abs(float(meta_levels.get("bass", 100.0)) - float(bass)) < 1e-6
                and abs(float(meta_levels.get("drums", 100.0)) - float(drums)) < 1e-6
                and abs(float(meta_levels.get("other", 100.0)) - float(other)) < 1e-6
            )
        except Exception:
            return False

    if out_wav.exists() and (not flags.force):
        try:
            meta_path = paths.mixes / f"{slug}.mix.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    meta_mode = str(meta.get("mode") or "").strip().lower()
                    meta_src = str(meta.get("src") or "").strip()
                    meta_levels = (meta.get("levels_percent") or {}) if isinstance(meta.get("levels_percent"), dict) else {}
                    mp3_ok = True
                    if not skip_stems_mix_mp3:
                        mp3_ok = out_mp3.exists() and str(meta.get("mix_mp3") or "").strip() == str(out_mp3)
                    if (
                        meta_mode == "fallback_vocals_only"
                        and str(meta.get("mix_wav") or "").strip() == str(out_wav)
                        and (meta_src == str(src_audio))
                        and _levels_match(meta_levels)
                        and mp3_ok
                    ):
                        if skip_stems_mix_mp3:
                            _start_prepared_render_audio_async(paths, slug, out_wav, flags=flags)
                        log("SPLIT", f"{reason_text}; reusing existing fallback vocals mix", GREEN)
                        return
        except Exception:
            pass

    log("SPLIT", f"{reason_text}; using vocals-only fallback mix (center-cancel)", YELLOW)
    fallback_t0 = now_perf_ms()
    _mix_vocals_only_fallback_to_wav(
        src_audio=src_audio,
        vocals_pct=vocals,
        out_wav=out_wav,
        flags=flags,
    )
    log_timing("step2", "fallback_vocals_mix_wav", fallback_t0, color=WHITE)

    if skip_stems_mix_mp3:
        _remove_if_exists(out_mp3, dry_run=flags.dry_run)
        log("MIX", "Skipping fallback MP3 encode (fast mode; step4 will use WAV)", YELLOW)
        _start_prepared_render_audio_async(paths, slug, out_wav, flags=flags)
    else:
        mp3_enc_t0 = now_perf_ms()
        _encode_mp3_from_wav(out_wav, out_mp3, flags)
        log_timing("step2", "fallback_vocals_mix_mp3_encode", mp3_enc_t0, color=WHITE)

    meta_t0 = now_perf_ms()
    _write_mix_meta_if_changed(
        meta_path=(paths.mixes / f"{slug}.mix.json"),
        payload={
            "mode": "fallback_vocals_only",
            "src": str(src_audio),
            "mix_mp3": str(out_mp3) if out_mp3.exists() else None,
            "mix_wav": str(out_wav),
            "mp3_skipped": bool(skip_stems_mix_mp3),
            "levels_percent": {
                "vocals": float(vocals),
                "bass": float(bass),
                "drums": float(drums),
                "other": float(other),
            },
            "demucs_backend": demucs_backend_label,
            "demucs_error": str(demucs_error or "")[:500],
        },
        flags=flags,
    )
    log_timing("step2", "write_mix_meta", meta_t0, color=WHITE)


def step2_split(
    paths: Paths,
    *,
    slug: str,
    mix_mode: str,
    vocals: float,
    bass: float,
    drums: float,
    other: float,
    flags: IOFlags,
) -> None:
    """
    Produce mixes/<slug>.wav and (optionally) mixes/<slug>.mp3.

    mix_mode:
      - "full": copy source mp3 to mixes/<slug>.mp3 when available, then ensure mixes/<slug>.wav matches source audio.
      - "stems": run Demucs (cached) + apply per-stem percentage levels, then ensure mixes/<slug>.mp3 matches.

    Stem level parameters are percentages (100 = unchanged).
    """
    step_t0 = now_perf_ms()
    t0 = now_perf_ms()
    src_audio = _resolve_source_audio(paths, slug)
    log_timing("step2", "resolve_source_audio", t0, color=WHITE)
    out_mp3 = paths.mixes / f"{slug}.mp3"
    out_wav = paths.mixes / f"{slug}.wav"
    src_is_mp3 = src_audio.suffix.lower() == ".mp3"
    skip_full_mix_mp3_when_source_not_mp3 = _skip_full_mix_mp3_when_source_not_mp3_enabled()
    skip_stems_mix_mp3 = _skip_stems_mix_mp3_enabled()

    mix_mode = (mix_mode or "full").strip().lower()
    vocals = _normalize_stem_pct(vocals)
    bass = _normalize_stem_pct(bass)
    drums = _normalize_stem_pct(drums)
    other = _normalize_stem_pct(other)

    # If any stem level is not the default (100%), we must use stems mode.
    need_stems = any(abs(float(v) - 100.0) > 1e-6 for v in (vocals, bass, drums, other))
    if need_stems and mix_mode != "stems":
        log("MIX", f"Stem levels requested; switching mix_mode=stems (was {mix_mode})", WHITE)
        mix_mode = "stems"

    if mix_mode not in ("full", "stems"):
        raise ValueError("mix_mode must be one of: full, stems")

    # Ensure output dirs exist (even in dry-run)
    paths.mixes.mkdir(parents=True, exist_ok=True)
    paths.separated.mkdir(parents=True, exist_ok=True)

    if mix_mode == "full":
        mode_t0 = now_perf_ms()
        if src_is_mp3:
            mp3_prep_t0 = now_perf_ms()
            if out_mp3.exists() and not flags.force:
                log("SPLIT", f"Using existing mix MP3: {out_mp3}", GREEN)
            else:
                if flags.dry_run:
                    log("SPLIT", f"[dry-run] Would copy {src_audio} -> {out_mp3}", YELLOW)
                else:
                    out_mp3.write_bytes(src_audio.read_bytes())
                    log("SPLIT", f"Copied full mix to {out_mp3}", GREEN)
            log_timing("step2", "full_mix_mp3_prepare", mp3_prep_t0, color=WHITE)
            wav_t0 = now_perf_ms()
            _ensure_wav_from_audio(out_mp3, out_wav, flags)
            log_timing("step2", "full_mix_wav_prepare", wav_t0, color=WHITE)
        else:
            log("SPLIT", f"Using native source audio: {src_audio.name}", WHITE)
            wav_t0 = now_perf_ms()
            _ensure_wav_from_audio(src_audio, out_wav, flags)
            log_timing("step2", "full_mix_wav_prepare", wav_t0, color=WHITE)
            if skip_full_mix_mp3_when_source_not_mp3:
                log("SPLIT", "Skipping mix MP3 encode for non-mp3 source (fast mode)", YELLOW)
                if flags.force and out_mp3.exists() and not flags.dry_run:
                    try:
                        out_mp3.unlink()
                    except Exception:
                        pass
            else:
                mp3_enc_t0 = now_perf_ms()
                _encode_mp3_from_wav(out_wav, out_mp3, flags)
                log_timing("step2", "full_mix_mp3_encode", mp3_enc_t0, color=WHITE)

        # Record mix metadata for debugging
        meta_t0 = now_perf_ms()
        _write_mix_meta_if_changed(
            meta_path=(paths.mixes / f"{slug}.mix.json"),
            payload={
                "mode": "full",
                "src": str(src_audio),
                "src_ext": str(src_audio.suffix).lower(),
                "mix_mp3": str(out_mp3) if out_mp3.exists() else None,
                "mix_wav": str(out_wav),
                "mp3_skipped": bool((not src_is_mp3) and skip_full_mix_mp3_when_source_not_mp3),
                "levels_percent": {"vocals": 100, "bass": 100, "drums": 100, "other": 100},
            },
            flags=flags,
        )
        log_timing("step2", "write_mix_meta", meta_t0, color=WHITE)

        log("SPLIT", "Step 2 complete (full mix guaranteed)", GREEN)
        log_timing("step2", "total_full", mode_t0, color=WHITE)
        log_timing("step2", "total", step_t0, color=WHITE)
        return

    # stems mode
    mode_t0 = now_perf_ms()
    requested_two_stems = _demucs_two_stems_enabled()
    vocals_only_adjustment = _is_vocals_only_adjustment(vocals, bass, drums, other)
    fast_vocals_fallback_first = bool(_fast_vocals_only_fallback_first_enabled())
    vocals_boost_requested = float(vocals) > (100.0 + 1e-6)
    fast_vocals_fallback_allowed = _fast_vocals_fallback_allowed(vocals, bass, drums, other)
    auto_two_stems = bool(_auto_two_stems_for_vocals_only_enabled() and vocals_only_adjustment)
    effective_two_stems = bool(requested_two_stems or auto_two_stems)
    if effective_two_stems and (not vocals_only_adjustment):
        log("SPLIT", "Two-stems requested but non-vocal stem levels were set; falling back to 4-stems", YELLOW)
        effective_two_stems = False
    if auto_two_stems and effective_two_stems:
        log("SPLIT", "Auto two-stems enabled for vocals-only mix (speed path)", CYAN)
    if fast_vocals_fallback_first and vocals_only_adjustment and (not fast_vocals_fallback_allowed):
        fallback_min_pct = _fast_vocals_only_fallback_min_pct()
        fallback_max_pct = _fast_vocals_only_fallback_max_pct()
        if _is_full_vocal_mute_requested(vocals):
            bypass_reason = "full vocal mute requested"
        elif vocals_boost_requested:
            bypass_reason = "vocals boost requested"
        elif float(vocals) < (fallback_min_pct - 1e-6):
            bypass_reason = (
                f"vocals={float(vocals):.0f}% is below fast fallback floor ({fallback_min_pct:.0f}%)"
            )
        else:
            bypass_reason = (
                f"vocals={float(vocals):.0f}% exceeds fast fallback ceiling ({fallback_max_pct:.0f}%)"
            )
        log(
            "SPLIT",
            f"Fast vocals-only fallback bypassed: {bypass_reason}; using Demucs stems instead",
            CYAN,
        )

    if fast_vocals_fallback_first and fast_vocals_fallback_allowed and (not vocals_boost_requested):
        _apply_vocals_only_fallback_mix(
            paths=paths,
            slug=slug,
            src_audio=src_audio,
            out_wav=out_wav,
            out_mp3=out_mp3,
            vocals=vocals,
            bass=bass,
            drums=drums,
            other=other,
            flags=flags,
            reason_text="Fast vocals-only path enabled (Demucs skipped)",
            demucs_backend_label="fallback_vocals_center_cancel_fast_first",
        )
        log("SPLIT", "Step 2 complete (fast fallback vocals mix guaranteed)", GREEN)
        log_timing("step2", "total_stems", mode_t0, color=WHITE)
        log_timing("step2", "total", step_t0, color=WHITE)
        return

    with _temporary_demucs_two_stems(effective_two_stems):
        stems_t0 = now_perf_ms()
        worker_cfg = _gpu_worker_config()
        stem_profile = {
            "vocals": float(vocals),
            "bass": float(bass),
            "drums": float(drums),
            "other": float(other),
        }
        model_version = worker_cfg.model_version
        demucs_backend = "local_demucs"
        try:
            stem_dir, demucs_backend = _ensure_demucs_stems_singleflight(
                paths,
                slug,
                src_audio,
                flags,
                worker_cfg,
                stem_profile=stem_profile,
                model_version=model_version,
            )
        except Exception as e:
            detail = str(e)
            detail_low = detail.lower()
            runtime_unavailable = _is_demucs_runtime_unavailable_error(detail)
            demucs_failed = runtime_unavailable or ("demucs" in detail_low)
            if vocals_only_adjustment and demucs_failed:
                if vocals_boost_requested:
                    raise RuntimeError(
                        "Could not process audio stems: vocals boost requires Demucs runtime."
                    ) from e

                fallback_reason = (
                    "Demucs runtime unavailable"
                    if runtime_unavailable
                    else "Demucs stem separation failed"
                )
                log(
                    "SPLIT",
                    (
                        "DEMUCS UNAVAILABLE: using fallback vocals attenuation "
                        "(center-cancel). Activate demucs_env or install Demucs to restore stem separation."
                    ),
                    RED,
                )
                _apply_vocals_only_fallback_mix(
                    paths=paths,
                    slug=slug,
                    src_audio=src_audio,
                    out_wav=out_wav,
                    out_mp3=out_mp3,
                    vocals=vocals,
                    bass=bass,
                    drums=drums,
                    other=other,
                    flags=flags,
                    reason_text=fallback_reason,
                    demucs_backend_label="fallback_vocals_center_cancel",
                    demucs_error=detail,
                )
                log("SPLIT", "Step 2 complete (fallback vocals mix guaranteed)", GREEN)
                log_timing("step2", "total_stems", mode_t0, color=WHITE)
                log_timing("step2", "total", step_t0, color=WHITE)
                return

            if demucs_failed:
                restored_from_global_cache = _restore_stems_from_global_cache(
                    paths,
                    slug,
                    src_audio,
                    # --force should still allow emergency restore when Demucs is unavailable.
                    force=False,
                    stem_profile=stem_profile,
                    model_version=model_version,
                )
                if restored_from_global_cache:
                    stem_dir = _resolve_stem_dir(paths, slug)
                    demucs_backend = "global_stem_cache_force_fallback"
                    log(
                        "SPLIT",
                        (
                            "Demucs unavailable for requested stem mix; "
                            "restored stems from global cache despite --force"
                        ),
                        YELLOW,
                    )
                else:
                    raise RuntimeError(
                        "Could not process audio stems for requested bass/drums/other levels right now. "
                        "Please try again."
                    ) from e
            else:
                raise
        log_timing("step2", "ensure_demucs_stems", stems_t0, color=WHITE)

        vocals_wav = stem_dir / "vocals.wav"

        # In two-stems mode, we have vocals + no_vocals (instrumental)
        # Map no_vocals to all three instrumental stems for compatibility.
        if _demucs_two_stems_enabled():
            no_vocals_wav = stem_dir / "no_vocals.wav"
            bass_wav = no_vocals_wav
            drums_wav = no_vocals_wav
            other_wav = no_vocals_wav
        else:
            bass_wav = stem_dir / "bass.wav"
            drums_wav = stem_dir / "drums.wav"
            other_wav = stem_dir / "other.wav"

        # Rebuild WAV (stems mix) if forced or missing.
        mix_wav_t0 = now_perf_ms()
        if out_wav.exists() and not flags.force:
            # If metadata exists and matches, we can reuse; otherwise rebuild.
            meta_path = paths.mixes / f"{slug}.mix.json"
            try:
                if meta_path.exists():
                    meta = __import__("json").loads(meta_path.read_text(encoding="utf-8"))
                    lev = (meta or {}).get("levels_percent", {})
                    if (
                        (meta or {}).get("mode") == "stems"
                        and abs(float(lev.get("vocals", 100)) - float(vocals)) < 1e-6
                        and abs(float(lev.get("bass", 100)) - float(bass)) < 1e-6
                        and abs(float(lev.get("drums", 100)) - float(drums)) < 1e-6
                        and abs(float(lev.get("other", 100)) - float(other)) < 1e-6
                        and out_wav.exists()
                    ):
                        log("MIX", f"Reusing existing stems mix WAV: {out_wav}", GREEN)
                    else:
                        raise RuntimeError("mix settings changed")
                else:
                    raise RuntimeError("no meta")
            except Exception:
                _mix_stems_to_wav(
                    vocals_wav=vocals_wav,
                    bass_wav=bass_wav,
                    drums_wav=drums_wav,
                    other_wav=other_wav,
                    vocals_pct=vocals,
                    bass_pct=bass,
                    drums_pct=drums,
                    other_pct=other,
                    out_wav=out_wav,
                    flags=flags,
                )
        else:
            _mix_stems_to_wav(
                vocals_wav=vocals_wav,
                bass_wav=bass_wav,
                drums_wav=drums_wav,
                other_wav=other_wav,
                vocals_pct=vocals,
                bass_pct=bass,
                drums_pct=drums,
                other_pct=other,
                out_wav=out_wav,
                flags=flags,
            )
        log_timing("step2", "stems_mix_wav", mix_wav_t0, color=WHITE)

        if skip_stems_mix_mp3:
            _remove_if_exists(out_mp3, dry_run=flags.dry_run)
            log("MIX", "Skipping stems MP3 encode (fast mode; step4 will use WAV)", YELLOW)
            _start_prepared_render_audio_async(paths, slug, out_wav, flags=flags)
        else:
            mp3_enc_t0 = now_perf_ms()
            _encode_mp3_from_wav(out_wav, out_mp3, flags)
            log_timing("step2", "stems_mix_mp3_encode", mp3_enc_t0, color=WHITE)

        meta_t0 = now_perf_ms()
        _write_mix_meta_if_changed(
            meta_path=(paths.mixes / f"{slug}.mix.json"),
            payload={
                "mode": "stems",
                "src": str(src_audio),
                "stems_dir": str(stem_dir),
                "demucs_backend": demucs_backend,
                "demucs_device": _resolve_demucs_device(),
                "mix_mp3": (str(out_mp3) if out_mp3.exists() else None),
                "mix_wav": str(out_wav),
                "mp3_skipped": bool(skip_stems_mix_mp3),
                "levels_percent": {"vocals": float(vocals), "bass": float(bass), "drums": float(drums), "other": float(other)},
            },
            flags=flags,
        )
        log_timing("step2", "write_mix_meta", meta_t0, color=WHITE)

    log("SPLIT", "Step 2 complete (stems mix guaranteed)", GREEN)
    log_timing("step2", "total_stems", mode_t0, color=WHITE)
    log_timing("step2", "total", step_t0, color=WHITE)


# end of step2_split.py
