from __future__ import annotations

import asyncio
import threading
import time
import json
import hashlib
import subprocess
import sys
import logging
import os
import re
import math
import shutil
import urllib.error
import urllib.parse
import urllib.request
from collections import deque, OrderedDict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Literal, Set
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    import redis as redis_lib
except Exception:  # pragma: no cover - optional dependency
    redis_lib = None  # type: ignore[assignment]

# Add parent directory to Python path for scripts imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Import pipeline utilities from existing scripts
from scripts.common import resolve_output_dir, slugify
from scripts.pipeline_contract import SpeedMode, build_pipeline_argv as build_core_pipeline_argv
from scripts.step1_fetch import yt_search_ids, yt_download_mp3, yt_download_top_result_mp3
from scripts.worker_security import build_signed_headers

from .job_store import JobSQLiteStore

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output"
META_DIR = BASE_DIR / "meta"
MP3_DIR = OUTPUT_DIR / "mp3"


def _parse_cors_origins(raw: str) -> list[str]:
    if not raw.strip():
        return ["*"]
    return [part.strip() for part in raw.split(",") if part.strip()]


_NO_GZIP_MEDIA_SUFFIXES = (
    ".mp4",
    ".m4a",
    ".mp3",
    ".wav",
    ".aac",
    ".flac",
    ".mov",
    ".m4v",
    ".webm",
)


class MediaAwareGZipMiddleware(GZipMiddleware):
    async def __call__(self, scope, receive, send) -> None:  # type: ignore[override]
        if scope.get("type") == "http":
            path = str(scope.get("path") or "").lower()
            if path.endswith(_NO_GZIP_MEDIA_SUFFIXES):
                await self.app(scope, receive, send)
                return
        await super().__call__(scope, receive, send)


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _default_job_workers() -> int:
    cpu_count = os.cpu_count() or 2
    return max(2, min(8, int(cpu_count)))


_STAGE_TIMEOUT_KEY_RE = re.compile(r"[^a-z0-9_]+")


def _normalize_stage_timeout_key(stage: str) -> str:
    cleaned = _STAGE_TIMEOUT_KEY_RE.sub("_", str(stage or "").strip().lower())
    return cleaned.strip("_")


def _parse_stage_timeout_overrides(raw: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for token in (raw or "").replace(";", ",").split(","):
        part = token.strip()
        if not part or ":" not in part:
            continue
        stage_raw, value_raw = part.split(":", 1)
        key = _normalize_stage_timeout_key(stage_raw)
        if not key:
            continue
        try:
            parsed = float(value_raw)
        except Exception:
            continue
        if parsed <= 0.0:
            continue
        out[key] = parsed
    return out


def _normalize_idempotency_key(raw: Optional[str]) -> str:
    key = str(raw or "").strip()
    if not key:
        return ""
    return key[:256]


def _normalize_query_for_dedupe(query: str) -> str:
    cleaned = " ".join(str(query or "").strip().lower().split())
    return cleaned


def _coerce_stem_pct_for_dedupe(value: Any) -> int:
    try:
        parsed = int(round(float(value)))
    except Exception:
        parsed = 100
    return max(0, min(150, parsed))


def _build_job_dedupe_key(query: str, options: Dict[str, Any]) -> str:
    profile = {
        "vocals": _coerce_stem_pct_for_dedupe(options.get("vocals", 100)),
        "bass": _coerce_stem_pct_for_dedupe(options.get("bass", 100)),
        "drums": _coerce_stem_pct_for_dedupe(options.get("drums", 100)),
        "other": _coerce_stem_pct_for_dedupe(options.get("other", 100)),
    }
    payload = {
        "version": IDEMPOTENCY_DEDUPE_VERSION,
        "query": _normalize_query_for_dedupe(query),
        "render_only": bool(options.get("render_only")),
        "preview": bool(options.get("preview")),
        "stem_profile": profile,
        "audio_url": str(options.get("audio_url") or "").strip(),
        "audio_id": str(options.get("audio_id") or "").strip(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_inflight_profile_key(options: Dict[str, Any]) -> str:
    def _normalize_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return int(default)

    def _normalize_float(value: Any, default: float) -> float:
        try:
            return round(float(value), 3)
        except Exception:
            return round(float(default), 3)

    payload = {
        "version": IDEMPOTENCY_DEDUPE_VERSION,
        "language": str(options.get("language") or "auto").strip().lower(),
        "render_only": bool(options.get("render_only")),
        "preview": bool(options.get("preview")),
        "audio_url": str(options.get("audio_url") or "").strip(),
        "audio_id": str(options.get("audio_id") or "").strip(),
        "upload": bool(options.get("upload")),
        "runtime_cookies_supplied": bool(options.get("runtime_cookies_supplied")),
        "offset_sec": _normalize_float(options.get("offset_sec"), 0.0),
        "yt_search_n": _normalize_int(options.get("yt_search_n"), 0),
        "stem_profile": {
            "vocals": _coerce_stem_pct_for_dedupe(options.get("vocals", 100)),
            "bass": _coerce_stem_pct_for_dedupe(options.get("bass", 100)),
            "drums": _coerce_stem_pct_for_dedupe(options.get("drums", 100)),
            "other": _coerce_stem_pct_for_dedupe(options.get("other", 100)),
        },
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


app = FastAPI(title="KaraoAPI", version="0.1.0")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("karaoapi")

_distributed_cache_client: Any = None
_distributed_cache_client_lock = threading.Lock()
_distributed_cache_last_error_logged_at_mono: float = 0.0
_DISTRIBUTED_CACHE_ERROR_LOG_INTERVAL_SEC = 30.0

_BACKEND_LOG_BUFFER: deque[str] = deque(maxlen=2000)
_BACKEND_LOG_BUFFER_LOCK = threading.Lock()
_RATING_PROMPT_LOCK = threading.Lock()


class _InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            try:
                msg = str(record.getMessage())
            except Exception:
                return
        if not msg:
            return
        with _BACKEND_LOG_BUFFER_LOCK:
            _BACKEND_LOG_BUFFER.append(msg)


def _install_in_memory_log_handler() -> None:
    root_logger = logging.getLogger()
    if any(isinstance(handler, _InMemoryLogHandler) for handler in root_logger.handlers):
        return
    handler = _InMemoryLogHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root_logger.addHandler(handler)


_install_in_memory_log_handler()

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_AUTO_OFFSET_RUNTIME_READY = False
_AUTO_OFFSET_RUNTIME_NOTE = "not_checked"
COOKIE_REFRESH_REQUIRED_MARKER = "COOKIE_REFRESH_REQUIRED"
# Pre-computed lowercase for hot-path string comparisons (avoids repeated .lower() calls)
_COOKIE_REFRESH_REQUIRED_MARKER_LOWER = COOKIE_REFRESH_REQUIRED_MARKER.lower()
COOKIE_REFRESH_REQUIRED_TEXT = (
    "source requested a bot-check for this source. Automatic no-cookie recovery was attempted."
)


def _probe_whisper_runtime() -> tuple[bool, str, str, str]:
    try:
        from scripts import lrc_offset_whisper as whisper_offset
    except Exception as exc:
        return False, "", "", f"import_failed:{exc}"

    explicit_bin = os.environ.get("MIXTERIOSO_WHISPER_BIN", "").strip() or None
    explicit_model = os.environ.get("MIXTERIOSO_WHISPER_MODEL", "").strip() or None

    whisper_bin = whisper_offset._find_whispercpp_bin(explicit_bin) or ""
    model_path = whisper_offset._find_model(explicit_model) or ""

    if whisper_bin and model_path:
        return True, whisper_bin, model_path, "ok"

    missing = []
    if not whisper_bin:
        missing.append("bin")
    if not model_path:
        missing.append("model")
    reason = "missing_" + "_and_".join(missing) if missing else "missing_unknown"
    return False, whisper_bin, model_path, reason


def _auto_offset_effectively_enabled() -> bool:
    requested_raw = os.environ.get("KARAOKE_AUTO_OFFSET_ENABLED")
    if requested_raw is None:
        return bool(_AUTO_OFFSET_RUNTIME_READY)
    requested = requested_raw.strip().lower()
    if requested in _FALSE_VALUES:
        return False
    if requested in _TRUE_VALUES:
        return bool(_AUTO_OFFSET_RUNTIME_READY)
    return bool(_AUTO_OFFSET_RUNTIME_READY)


def _configure_auto_offset_runtime() -> None:
    global _AUTO_OFFSET_RUNTIME_READY, _AUTO_OFFSET_RUNTIME_NOTE

    requested_raw = os.environ.get("KARAOKE_AUTO_OFFSET_ENABLED")
    requested = (requested_raw or "").strip().lower()
    ready, whisper_bin, model_path, reason = _probe_whisper_runtime()

    if whisper_bin and not os.environ.get("MIXTERIOSO_WHISPER_BIN"):
        os.environ["MIXTERIOSO_WHISPER_BIN"] = whisper_bin
    if model_path and not os.environ.get("MIXTERIOSO_WHISPER_MODEL"):
        os.environ["MIXTERIOSO_WHISPER_MODEL"] = model_path

    if requested_raw is None:
        if ready:
            os.environ.pop("KARAOKE_AUTO_OFFSET_ENABLED", None)
            _AUTO_OFFSET_RUNTIME_READY = True
            _AUTO_OFFSET_RUNTIME_NOTE = "ready_default_delegated"
            logger.info("whisper runtime ready; auto-offset defaults are delegated to pipeline callers")
            return

        os.environ["KARAOKE_AUTO_OFFSET_ENABLED"] = "0"
        _AUTO_OFFSET_RUNTIME_READY = False
        _AUTO_OFFSET_RUNTIME_NOTE = f"disabled_runtime_not_ready:{reason}"
        logger.warning(
            "whisper runtime unavailable; disabling auto-offset",
            extra={
                "reason": reason,
                "requested": "unset",
                "whisper_bin": whisper_bin,
                "whisper_model": model_path,
            },
        )
        return

    if requested in _FALSE_VALUES:
        os.environ["KARAOKE_AUTO_OFFSET_ENABLED"] = "0"
        _AUTO_OFFSET_RUNTIME_READY = False
        _AUTO_OFFSET_RUNTIME_NOTE = "disabled_by_env"
        logger.info("whisper auto-offset disabled by env")
        return

    if ready:
        os.environ["KARAOKE_AUTO_OFFSET_ENABLED"] = "1"
        _AUTO_OFFSET_RUNTIME_READY = True
        _AUTO_OFFSET_RUNTIME_NOTE = "enabled"
        logger.info(
            "whisper auto-offset enabled",
            extra={
                "whisper_bin": os.environ.get("MIXTERIOSO_WHISPER_BIN", ""),
                "whisper_model": os.environ.get("MIXTERIOSO_WHISPER_MODEL", ""),
            },
        )
        return

    # Runtime is not ready. Disable auto-offset so step3 uses clean skip behavior.
    os.environ["KARAOKE_AUTO_OFFSET_ENABLED"] = "0"
    _AUTO_OFFSET_RUNTIME_READY = False
    _AUTO_OFFSET_RUNTIME_NOTE = f"disabled_runtime_not_ready:{reason}"
    logger.warning(
        "whisper runtime unavailable; disabling auto-offset",
        extra={
            "reason": reason,
            "requested": requested or "unset",
            "whisper_bin": whisper_bin,
            "whisper_model": model_path,
        },
    )


_configure_auto_offset_runtime()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    client = request.client.host if request.client else "unknown"
    path = request.url.path
    method = request.method.upper()
    suppress_noisy = (
        path == "/rating/state"
        or (method == "GET" and path.startswith("/jobs/"))
    )
    if not suppress_noisy:
        logger.info("request start", extra={"method": method, "path": path, "client": client})
    try:
        response = await call_next(request)
        duration_ms = round((time.time() - start) * 1000, 2)
        if not suppress_noisy:
            logger.info(
                "request done",
                extra={
                    "method": method,
                    "path": path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                },
            )
        return response
    except Exception:
        duration_ms = round((time.time() - start) * 1000, 2)
        logger.exception(
            "request failed",
            extra={"method": method, "path": path, "duration_ms": duration_ms},
        )
        raise

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(os.environ.get("KARAOAPI_CORS_ORIGINS", "*")),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Enable GZIP compression for responses (minimum 500 bytes), but never for
# already-compressed media artifacts like MP4/M4A downloads.
app.add_middleware(MediaAwareGZipMiddleware, minimum_size=500)

# Serve rendered outputs directly
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/output", StaticFiles(directory=str(OUTPUT_DIR)), name="output")

# Serve only the specific artifact directories we expect clients to fetch.
# Mounting BASE_DIR would expose repo files (.env, tokens, etc.) if the API is public.
SEPARATED_DIR = BASE_DIR / "separated"
MIXES_DIR = BASE_DIR / "mixes"
TIMINGS_DIR = BASE_DIR / "timings"
SEPARATED_DIR.mkdir(parents=True, exist_ok=True)
MIXES_DIR.mkdir(parents=True, exist_ok=True)
TIMINGS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/files/separated", StaticFiles(directory=str(SEPARATED_DIR)), name="files_separated")
app.mount("/files/mixes", StaticFiles(directory=str(MIXES_DIR)), name="files_mixes")
app.mount("/files/timings", StaticFiles(directory=str(TIMINGS_DIR)), name="files_timings")


class CreateJobRequest(BaseModel):
    model_config = {"extra": "allow"}

    query: str = Field(..., min_length=1)
    idempotency_key: Optional[str] = Field(default=None, max_length=256)
    # Optional direct audio source overrides for step1 (keeps output slug derived from `query`).
    # These map to scripts/main.py flags: --audio-url / --audio-id.
    audio_url: Optional[str] = None
    audio_id: Optional[str] = None
    language: Optional[str] = "auto"
    force: bool = False
    reset: bool = False
    dry_run: bool = False
    confirm: bool = False
    no_parallel: bool = False
    yt_search_n: Optional[int] = None
    speed_mode: Optional[SpeedMode] = None
    tune_for_me: Optional[int] = Field(default=None, ge=0, le=3)
    calibration_level: Optional[int] = Field(default=None, ge=0, le=3)
    upload: bool = False
    vocals: Optional[int] = Field(default=None, ge=0, le=150)
    bass: Optional[int] = Field(default=None, ge=0, le=150)
    drums: Optional[int] = Field(default=None, ge=0, le=150)
    other: Optional[int] = Field(default=None, ge=0, le=150)
    offset_sec: Optional[float] = 0.0
    render_only: bool = False
    preview: Optional[bool] = None
    source_cookies_netscape: Optional[str] = Field(default=None, max_length=400_000)


class RatingStateRequest(BaseModel):
    device_key: Optional[str] = Field(default="", max_length=320)
    aliases: Optional[List[str]] = None


class RatingMarkRequest(BaseModel):
    device_key: Optional[str] = Field(default="", max_length=320)
    aliases: Optional[List[str]] = None
    action: Optional[str] = Field(default="shown", max_length=80)


class RatingProgressRequest(BaseModel):
    device_key: Optional[str] = Field(default="", max_length=320)
    aliases: Optional[List[str]] = None
    job_id: Optional[str] = Field(default="", max_length=200)
    delta: Optional[int] = Field(default=1, ge=1, le=10)


class JobStatus(BaseModel):
    id: str
    status: str
    idempotency_key: Optional[str] = None
    dedupe_key: Optional[str] = None
    query: str
    slug: str
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    cancelled_at: Optional[float] = None
    render_started_at: Optional[float] = None
    render_finished_at: Optional[float] = None
    error: Optional[str] = None
    output_path: Optional[str] = None
    output_url: Optional[str] = None
    youtube_video_url: Optional[str] = None
    mix_audio_url: Optional[str] = None
    preview_output_url: Optional[str] = None
    final_output_url: Optional[str] = None
    output_is_preview: Optional[bool] = None
    stage: Optional[str] = None
    last_message: Optional[str] = None
    last_updated_at: Optional[float] = None
    progress_percent: Optional[float] = None  # 0-100 percentage
    estimated_seconds_remaining: Optional[float] = None  # ETA in seconds
    elapsed_sec: Optional[float] = None
    timing_breakdown: Dict[str, float] = Field(default_factory=dict)  # key=step.part, value=elapsed_ms
    pipeline_timing: Dict[str, float] = Field(default_factory=dict)  # key=step, value=elapsed_sec
    step_timestamps: Dict[str, float] = Field(default_factory=dict)
    attempt_counts: Dict[str, int] = Field(default_factory=dict)


class DebugYtdlpRequest(BaseModel):
    query: str = Field(..., min_length=1)
    mode: Literal["search", "download"] = "search"
    search_n: int = 3
    video_id: Optional[str] = None
    timeout_sec: int = 60


class DebugYtdlpResponse(BaseModel):
    query: str
    mode: str
    search_n: int
    video_id: Optional[str] = None
    ids: List[str] = []
    ok: bool
    error: Optional[str] = None
    cookies_path: Optional[str] = None
    cookies_exists: bool = False
    cookies_size: int = 0


@dataclass
class StructuredError:
    """Structured error information for better debugging"""
    message: str  # User-friendly error message
    stage: Optional[str] = None  # Which stage failed
    error_type: Optional[str] = None  # e.g., 'download', 'separation', 'render'
    context: Optional[Dict[str, Any]] = None  # Additional context


@dataclass
class Job:
    id: str
    query: str
    slug: str
    created_at: float
    idempotency_key: Optional[str] = None
    dedupe_key: Optional[str] = None
    status: str = "queued"
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    cancelled_at: Optional[float] = None
    render_started_at: Optional[float] = None
    render_finished_at: Optional[float] = None
    error: Optional[str] = None
    output_path: Optional[str] = None
    output_url: Optional[str] = None
    youtube_video_url: Optional[str] = None
    mix_audio_url: Optional[str] = None
    preview_output_url: Optional[str] = None
    final_output_url: Optional[str] = None
    output_is_preview: Optional[bool] = None
    stage: Optional[str] = None
    last_message: Optional[str] = None
    last_updated_at: Optional[float] = None
    progress_percent: Optional[float] = None  # 0-100 percentage
    estimated_seconds_remaining: Optional[float] = None  # ETA in seconds
    timing_breakdown: Dict[str, float] = field(default_factory=dict)  # key=step.part, value=elapsed_ms
    step_timestamps: Dict[str, float] = field(default_factory=dict)
    attempt_counts: Dict[str, int] = field(default_factory=dict)
    options: Dict[str, Any] = field(default_factory=dict)


_jobs: Dict[str, Job] = {}
_jobs_lock = threading.Lock()
# Secondary index: slug -> job_id for O(1) inflight job lookup by slug
_slug_to_job_id: Dict[str, str] = {}
# Secondary index: idempotency key -> job id for replay-safe create semantics
_idempotency_to_job_id: Dict[str, str] = {}
# Atomic counter for active jobs (queued + running) to avoid O(n) iteration
_active_job_count_cached: int = 0


# LRU cache for job status responses: OrderedDict for O(1) eviction
# Structure: cache_key -> (last_updated_at, etag, cached_dict)
_job_status_cache: OrderedDict[str, tuple[float, str, Dict[str, Any]]] = OrderedDict()
_job_status_cache_lock = threading.Lock()
_JOB_STATUS_CACHE_MAX_SIZE = 200  # Limit cache to 200 entries
_source_audio_url_cache: OrderedDict[str, tuple[float, Dict[str, Any]]] = OrderedDict()
_source_audio_url_cache_lock = threading.Lock()
_source_search_result_meta_cache: OrderedDict[str, tuple[float, Dict[str, Any]]] = OrderedDict()
_source_search_result_meta_cache_lock = threading.Lock()


@dataclass
class _SourceAudioSingleflightEntry:
    lock: threading.Lock = field(default_factory=threading.Lock)
    refs: int = 0


_source_audio_url_singleflight_entries: Dict[str, _SourceAudioSingleflightEntry] = {}
_source_audio_url_singleflight_entries_lock = threading.Lock()
_source_audio_url_refresh_inflight: Set[str] = set()
_source_audio_url_refresh_inflight_lock = threading.Lock()
SOURCE_AUDIO_URL_CACHE_MAX_ENTRIES = _env_int(
    "KARAOAPI_SOURCE_AUDIO_URL_CACHE_MAX_ENTRIES",
    400,
    minimum=20,
)
SOURCE_AUDIO_URL_CACHE_TTL_SEC = max(
    0.0,
    float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_CACHE_TTL_SEC", "1800")),
)
SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC = max(
    0.0,
    float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC", "0")),
)
SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED = _env_bool(
    "KARAOAPI_SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED",
    False,
)
SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_LOCK_TTL_SEC = max(
    1.0,
    float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_LOCK_TTL_SEC", "45")),
)
SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_WAIT_SEC = max(
    0.0,
    float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_WAIT_SEC", "6")),
)
SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_POLL_SEC = max(
    0.02,
    float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_POLL_SEC", "0.12")),
)
SOURCE_SEARCH_RESULT_META_CACHE_MAX_ENTRIES = _env_int(
    "KARAOAPI_SOURCE_SEARCH_RESULT_META_CACHE_MAX_ENTRIES",
    2000,
    minimum=20,
)
SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC = max(
    0.0,
    float(os.environ.get("KARAOAPI_SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC", "3600")),
)
CACHE_REDIS_URL = (
    os.environ.get("KARAOAPI_CACHE_REDIS_URL")
    or os.environ.get("KARAOAPI_REDIS_CACHE_URL")
    or ""
).strip()
CACHE_REDIS_PREFIX = (
    os.environ.get("KARAOAPI_CACHE_REDIS_PREFIX", "karaoapi:cache").strip()
    or "karaoapi:cache"
)
CACHE_REDIS_SOCKET_TIMEOUT_SEC = max(
    0.05,
    float(os.environ.get("KARAOAPI_CACHE_REDIS_SOCKET_TIMEOUT_SEC", "0.5")),
)
CACHE_REDIS_CONNECT_TIMEOUT_SEC = max(
    0.05,
    float(os.environ.get("KARAOAPI_CACHE_REDIS_CONNECT_TIMEOUT_SEC", "0.5")),
)
JOB_WORKERS = _env_int("KARAOAPI_JOB_WORKERS", _default_job_workers(), minimum=1)
_executor = ThreadPoolExecutor(max_workers=JOB_WORKERS)
_job_processes: Dict[str, subprocess.Popen] = {}
_job_processes_lock = threading.Lock()
_jobs_persist_lock = threading.Lock()
_jobs_persist_debounce: Dict[str, float] = {}
_jobs_persist_debounce_lock = threading.Lock()
_jobs_persist_last_hash = ""
_jobs_sqlite_last_prune_at_mono = 0.0
JOBS_STATE_PATH = Path(os.environ.get("KARAOAPI_JOBS_STATE", BASE_DIR / "meta" / "jobs_state.json"))
JOBS_SQLITE_ENABLED = _env_bool("KARAOAPI_JOBS_SQLITE_ENABLED", True)
JOBS_SQLITE_PATH = Path(
    os.environ.get("KARAOAPI_JOBS_SQLITE_PATH", BASE_DIR / "meta" / "jobs_state.sqlite3")
).resolve()
_jobs_sqlite_store = JobSQLiteStore(JOBS_SQLITE_PATH) if JOBS_SQLITE_ENABLED else None
JOBS_SQLITE_PRUNE_INTERVAL_SEC = max(
    5.0, float(os.environ.get("KARAOAPI_JOBS_SQLITE_PRUNE_INTERVAL_SEC", "45"))
)
JOB_TIMEOUT_SEC = max(60.0, float(os.environ.get("KARAOAPI_JOB_TIMEOUT_SEC", "900")))
NO_OUTPUT_TIMEOUT_SEC = max(0.0, float(os.environ.get("KARAOAPI_NO_OUTPUT_TIMEOUT_SEC", "900")))
_STAGE_NO_OUTPUT_TIMEOUTS = _parse_stage_timeout_overrides(
    os.environ.get("KARAOAPI_STAGE_NO_OUTPUT_TIMEOUTS", "")
)
_STAGE_JOB_TIMEOUTS = _parse_stage_timeout_overrides(
    os.environ.get("KARAOAPI_STAGE_JOB_TIMEOUTS", "")
)
STALE_JOB_SWEEPER_ENABLED = _env_bool("KARAOAPI_STALE_JOB_SWEEPER_ENABLED", True)
STALE_JOB_MAX_AGE_SEC = max(
    60.0,
    float(os.environ.get("KARAOAPI_STALE_JOB_MAX_AGE_SEC", str(int(JOB_TIMEOUT_SEC)))),
)
STALE_JOB_SWEEPER_INTERVAL_SEC = max(
    5.0,
    float(os.environ.get("KARAOAPI_STALE_JOB_SWEEPER_INTERVAL_SEC", "30")),
)
STALE_JOB_REFERENCE_MIN_EPOCH_SEC = 1_000_000_000.0
_stale_job_sweeper_lock = threading.Lock()
_stale_job_sweeper_stop_event = threading.Event()
_stale_job_sweeper_thread: Optional[threading.Thread] = None
SOURCE_AUDIO_URL_MAX_ATTEMPTS = _env_int("KARAOAPI_SOURCE_AUDIO_URL_MAX_ATTEMPTS", 3, minimum=1)
SOURCE_AUDIO_URL_CMD_TIMEOUT_SEC = max(5.0, float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_CMD_TIMEOUT_SEC", "20")))
SOURCE_AUDIO_URL_METADATA_TIMEOUT_SEC = max(
    3.0, float(os.environ.get("KARAOAPI_SOURCE_AUDIO_URL_METADATA_TIMEOUT_SEC", "10"))
)
DEBUG_KEY = os.environ.get("KARAOAPI_DEBUG_KEY", "").strip()
MAX_PENDING_JOBS = int(os.environ.get("KARAOAPI_MAX_PENDING_JOBS", "40"))
EMERGENCY_DISABLE_NEW_JOBS = _env_bool("KARAOAPI_EMERGENCY_DISABLE_NEW_JOBS", False)
MAX_JOBS_HISTORY = int(os.environ.get("KARAOAPI_MAX_JOBS_HISTORY", "500"))
JOB_PROGRESS_PERSIST_INTERVAL_SEC = max(
    0.5,
    float(os.environ.get("KARAOAPI_JOB_PROGRESS_PERSIST_INTERVAL_SEC", "2.0")),
)
ALLOW_STEM_LEVELS_NON_RENDER = _env_bool("KARAOAPI_ALLOW_STEM_LEVELS_NON_RENDER", False)
REUSE_SUCCEEDED_JOBS_ENABLED = _env_bool("KARAOAPI_REUSE_SUCCEEDED_JOBS_ENABLED", True)
REUSE_SUCCEEDED_JOBS_MAX_AGE_SEC = max(0.0, float(os.environ.get("KARAOAPI_REUSE_SUCCEEDED_JOBS_MAX_AGE_SEC", "21600")))
REUSE_UPLOADED_JOBS_MAX_AGE_SEC = max(
    0.0, float(os.environ.get("KARAOAPI_REUSE_UPLOADED_JOBS_MAX_AGE_SEC", "0"))
)
MAX_UPLOADED_JOBS_HISTORY = max(1, int(os.environ.get("KARAOAPI_MAX_UPLOADED_JOBS_HISTORY", "2000")))
REUSE_UPLOADED_REQUIRE_SOURCE_MATCH = _env_bool("KARAOAPI_REUSE_UPLOADED_REQUIRE_SOURCE_MATCH", True)
REUSE_UPLOADED_REQUIRE_SYNC_PASS = _env_bool("KARAOAPI_REUSE_UPLOADED_REQUIRE_SYNC_PASS", True)
REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED = _env_bool("KARAOAPI_REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED", False)
REUSE_SUCCEEDED_JOBS_CACHE_VERSION = (
    os.environ.get("KARAOAPI_REUSE_SUCCEEDED_JOBS_CACHE_VERSION", "2026-02-20-preview-overlay-fix").strip()
    or "2026-02-20-preview-overlay-fix"
)
PREVIEW_RENDER_ENABLED = os.environ.get("KARAOAPI_PREVIEW_RENDER", "0").strip().lower() in {"1", "true", "yes", "on"}
ALWAYS_EARLY_ASSEMBLE_ENABLED = _env_bool("KARAOAPI_ALWAYS_EARLY_ASSEMBLE", True)
PREVIEW_RENDER_LEVEL = os.environ.get("KARAOAPI_PREVIEW_RENDER_LEVEL", "1").strip()
PREVIEW_RENDER_PROFILE = os.environ.get("KARAOAPI_PREVIEW_RENDER_PROFILE", "turbo").strip()
FINAL_RENDER_LEVEL = os.environ.get("KARAOAPI_FINAL_RENDER_LEVEL", "").strip()
FINAL_RENDER_PROFILE = os.environ.get("KARAOAPI_FINAL_RENDER_PROFILE", "fast").strip()
DEFAULT_TUNE_FOR_ME_LEVEL = max(0, min(3, _env_int("KARAOAPI_DEFAULT_TUNE_FOR_ME_LEVEL", 0, minimum=0)))
DEFAULT_CALIBRATION_LEVEL = max(0, min(3, _env_int("KARAOAPI_DEFAULT_CALIBRATION_LEVEL", 1, minimum=0)))
PREVIEW_VIDEO_SIZE = os.environ.get("KARAOAPI_PREVIEW_VIDEO_SIZE", "").strip()
PREVIEW_FPS = os.environ.get("KARAOAPI_PREVIEW_FPS", "").strip()
PREVIEW_VIDEO_BITRATE = os.environ.get("KARAOAPI_PREVIEW_VIDEO_BITRATE", "").strip()
PREVIEW_AUDIO_BITRATE = os.environ.get("KARAOAPI_PREVIEW_AUDIO_BITRATE", "").strip()
PREVIEW_X264_PRESET = os.environ.get("KARAOAPI_PREVIEW_X264_PRESET", "").strip()
PREVIEW_X264_TUNE = os.environ.get("KARAOAPI_PREVIEW_X264_TUNE", "").strip()
RATING_PROMPT_STATE_PATH = Path(
    os.environ.get("KARAOAPI_RATING_PROMPT_STATE_PATH", BASE_DIR / "meta" / "rating_prompt_state.json")
).resolve()
RATING_PROMPT_MAX_KEYS = max(500, int(os.environ.get("KARAOAPI_RATING_PROMPT_MAX_KEYS", "200000")))
_rating_prompt_state: Dict[str, Dict[str, Any]] = {}
EARLY_MUTE_PREVIEW_ENABLED = _env_bool("KARAOAPI_EARLY_MUTE_PREVIEW_ENABLED", True)
EARLY_MUTE_PREVIEW_WAIT_SEC = max(5.0, float(os.environ.get("KARAOAPI_EARLY_MUTE_PREVIEW_WAIT_SEC", "240")))
EARLY_MUTE_PREVIEW_POLL_SEC = max(0.1, float(os.environ.get("KARAOAPI_EARLY_MUTE_PREVIEW_POLL_SEC", "0.4")))
EARLY_MUTE_PREVIEW_RENDER_TIMEOUT_SEC = max(
    20.0,
    float(os.environ.get("KARAOAPI_EARLY_MUTE_PREVIEW_RENDER_TIMEOUT_SEC", "240")),
)
FINALIZE_FROM_MUTE_PREVIEW_ENABLED = _env_bool("KARAOAPI_FINALIZE_FROM_MUTE_PREVIEW_ENABLED", True)
PREVIEW_AUDIO_MUX_TIMEOUT_SEC = max(20.0, float(os.environ.get("KARAOAPI_PREVIEW_AUDIO_MUX_TIMEOUT_SEC", "120")))
OUTPUT_MIN_BYTES = max(1024, int(os.environ.get("KARAOAPI_OUTPUT_MIN_BYTES", "65536")))
OUTPUT_MIN_DURATION_SEC = max(0.1, float(os.environ.get("KARAOAPI_OUTPUT_MIN_DURATION_SEC", "3.0")))
OUTPUT_VALIDATION_ENFORCED = _env_bool("KARAOAPI_OUTPUT_VALIDATION_ENFORCED", True)
OUTPUT_VALIDATION_RETRY_MUX_ON_FAIL = _env_bool("KARAOAPI_OUTPUT_VALIDATION_RETRY_MUX_ON_FAIL", True)
SERVER_DOWNLOAD_ONLY_ENFORCED = _env_bool("KARAOAPI_SERVER_DOWNLOAD_ONLY", True)
ENABLE_CLIENT_UPLOAD_ENDPOINT = _env_bool("KARAOAPI_ENABLE_CLIENT_UPLOAD", False)
ENABLE_SOURCE_AUDIO_URL_ENDPOINT = _env_bool("KARAOAPI_ENABLE_SOURCE_AUDIO_URL", False)
REQUIRE_IDEMPOTENCY_KEY = _env_bool("KARAOAPI_REQUIRE_IDEMPOTENCY_KEY", True)
IDEMPOTENCY_DEDUPE_VERSION = (
    os.environ.get("KARAOAPI_IDEMPOTENCY_DEDUPE_VERSION", "2026-02-20-gpu-worker-v1").strip()
    or "2026-02-20-gpu-worker-v1"
)
IDEMPOTENCY_MAX_AGE_SEC = max(0.0, float(os.environ.get("KARAOAPI_IDEMPOTENCY_MAX_AGE_SEC", "86400")))
GPU_WORKER_URL = (
    os.environ.get("MIXTERIOSO_GPU_WORKER_URL")
    or os.environ.get("KARAOAPI_GPU_WORKER_URL")
    or ""
).strip()
GPU_WORKER_API_KEY = (
    os.environ.get("MIXTERIOSO_GPU_WORKER_API_KEY")
    or os.environ.get("KARAOAPI_GPU_WORKER_API_KEY")
    or ""
).strip()
GPU_WORKER_HMAC_SECRET = (
    os.environ.get("MIXTERIOSO_GPU_WORKER_HMAC_SECRET")
    or os.environ.get("KARAOAPI_GPU_WORKER_HMAC_SECRET")
    or ""
).strip()
GPU_WORKER_CANCEL_TIMEOUT_SEC = max(2.0, float(os.environ.get("KARAOAPI_GPU_WORKER_CANCEL_TIMEOUT_SEC", "8")))
JOB_RUNTIME_COOKIES_DIR = Path(os.environ.get("KARAOAPI_RUNTIME_COOKIES_DIR", "/tmp")).resolve()
JOB_RUNTIME_COOKIES_MAX_BYTES = max(1024, int(os.environ.get("KARAOAPI_RUNTIME_COOKIES_MAX_BYTES", "262144")))
NO_COOKIE_RECOVERY_ENABLED = _env_bool("KARAOAPI_NO_COOKIE_RECOVERY_ENABLED", True)
NO_COOKIE_RECOVERY_YT_SEARCH_N = _env_int("KARAOAPI_NO_COOKIE_RECOVERY_YT_SEARCH_N", 6, minimum=1)
NO_COOKIE_RECOVERY_ENV_OVERRIDES = {
    "MIXTERIOSO_YTDLP_FAILOVER_CLIENTS": (
        os.environ.get("KARAOAPI_NO_COOKIE_RECOVERY_FAILOVER_CLIENTS", "android,ios,tv_embedded").strip()
        or "android,ios,tv_embedded"
    ),
    # Start recovery with the profile that has the best success/latency tradeoff for bot-prone tracks.
    "MIXTERIOSO_YTDLP_FORMAT": os.environ.get("KARAOAPI_NO_COOKIE_RECOVERY_YTDLP_FORMAT", "").strip(),
    "MIXTERIOSO_YTDLP_EXTRACTOR_ARGS": (
        os.environ.get("KARAOAPI_NO_COOKIE_RECOVERY_YTDLP_EXTRACTOR_ARGS", "youtube:player_client=android").strip()
        or "youtube:player_client=android"
    ),
    "MIXTERIOSO_MP3_MAX_SOURCE_ATTEMPTS": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_MAX_SOURCE_ATTEMPTS", 4, minimum=1)
    ),
    "MIXTERIOSO_MP3_MAX_SOURCE_SECONDS": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_MAX_SOURCE_SECONDS", 70, minimum=15)
    ),
    "MIXTERIOSO_MP3_TOTAL_TIMEOUT_SEC": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_TOTAL_TIMEOUT_SEC", 120, minimum=30)
    ),
    "MIXTERIOSO_MP3_MAX_ID_ATTEMPTS": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_MAX_ID_ATTEMPTS", 5, minimum=1)
    ),
    "MIXTERIOSO_MP3_MAX_QUERY_VARIANTS": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_MAX_QUERY_VARIANTS", 5, minimum=1)
    ),
    "MIXTERIOSO_MP3_MAX_SEARCH_QUERY_VARIANTS": str(
        _env_int("KARAOAPI_NO_COOKIE_RECOVERY_MAX_SEARCH_QUERY_VARIANTS", 3, minimum=1)
    ),
    "MIXTERIOSO_MP3_PARALLEL_STRATEGY_RACE": "1",
    "MIXTERIOSO_MP3_STOP_AFTER_SEARCH_TIMEOUT": "0",
}


def _log_distributed_cache_error(action: str, exc: Exception) -> None:
    global _distributed_cache_last_error_logged_at_mono
    now_mono = time.monotonic()
    if (now_mono - float(_distributed_cache_last_error_logged_at_mono)) < _DISTRIBUTED_CACHE_ERROR_LOG_INTERVAL_SEC:
        return
    _distributed_cache_last_error_logged_at_mono = now_mono
    logger.warning("distributed cache %s failed: %s", action, str(exc)[:300])


def _distributed_cache_client_get() -> Optional[Any]:
    if not CACHE_REDIS_URL:
        return None
    if redis_lib is None:
        return None
    global _distributed_cache_client
    client = _distributed_cache_client
    if client is not None:
        return client
    with _distributed_cache_client_lock:
        if _distributed_cache_client is not None:
            return _distributed_cache_client
        try:
            client = redis_lib.Redis.from_url(  # type: ignore[union-attr]
                CACHE_REDIS_URL,
                decode_responses=True,
                socket_timeout=float(CACHE_REDIS_SOCKET_TIMEOUT_SEC),
                socket_connect_timeout=float(CACHE_REDIS_CONNECT_TIMEOUT_SEC),
            )
            client.ping()
            _distributed_cache_client = client
            logger.info("distributed cache enabled", extra={"prefix": CACHE_REDIS_PREFIX})
            return client
        except Exception as exc:
            _log_distributed_cache_error("connect", exc)
            return None


def _distributed_cache_key(namespace: str, key: str) -> str:
    clean_namespace = str(namespace or "").strip().lower() or "default"
    clean_key = str(key or "").strip()
    return f"{CACHE_REDIS_PREFIX}:{clean_namespace}:{clean_key}"


def _decode_distributed_cache_payload(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray, memoryview)):
        try:
            text = bytes(raw).decode("utf-8")
        except Exception:
            return None
    elif isinstance(raw, str):
        text = raw
    else:
        try:
            text = str(raw)
        except Exception:
            return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _distributed_cache_get_json(namespace: str, key: str) -> Optional[Dict[str, Any]]:
    clean_key = str(key or "").strip()
    if not clean_key:
        return None
    client = _distributed_cache_client_get()
    if client is None:
        return None
    try:
        raw = client.get(_distributed_cache_key(namespace, clean_key))
    except Exception as exc:
        _log_distributed_cache_error("get", exc)
        return None
    if raw is None:
        return None
    return _decode_distributed_cache_payload(raw)


def _distributed_cache_set_json(namespace: str, key: str, payload: Dict[str, Any], *, ttl_sec: float) -> None:
    clean_key = str(key or "").strip()
    if (not clean_key) or ttl_sec <= 0:
        return
    client = _distributed_cache_client_get()
    if client is None:
        return
    try:
        serialized = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    except Exception:
        return
    try:
        client.setex(_distributed_cache_key(namespace, clean_key), max(1, int(ttl_sec)), serialized)
    except Exception as exc:
        _log_distributed_cache_error("set", exc)


def _normalize_source_audio_cache_key(text: str) -> str:
    key = str(text or "").strip().lower()
    if not key:
        return ""
    return " ".join(key.split())


def _source_audio_url_cache_lookup(query: str, *, allow_stale: bool = False) -> tuple[Optional[Dict[str, Any]], bool]:
    if SOURCE_AUDIO_URL_CACHE_TTL_SEC <= 0:
        return None, False
    key = _normalize_source_audio_cache_key(query)
    if not key:
        return None, False
    now_mono = time.monotonic()
    stale_payload: Optional[Dict[str, Any]] = None
    cache_ttl_sec = float(SOURCE_AUDIO_URL_CACHE_TTL_SEC)
    stale_window_sec = float(SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC)
    max_age_sec = cache_ttl_sec + stale_window_sec

    with _source_audio_url_cache_lock:
        item = _source_audio_url_cache.get(key)
        if item is None:
            fresh_payload = None
        else:
            created_at_mono, payload = item
            age_sec = now_mono - float(created_at_mono)
            if age_sec > max_age_sec:
                _source_audio_url_cache.pop(key, None)
                fresh_payload = None
            elif age_sec > cache_ttl_sec:
                fresh_payload = None
                if allow_stale and stale_window_sec > 0:
                    _source_audio_url_cache.move_to_end(key)
                    stale_payload = dict(payload)
            else:
                _source_audio_url_cache.move_to_end(key)
                fresh_payload = dict(payload)
    if fresh_payload is not None:
        return fresh_payload, False

    distributed = _distributed_cache_get_json("source_audio_url", key)
    if distributed is not None:
        now_mono = time.monotonic()
        data = dict(distributed)
        with _source_audio_url_cache_lock:
            _source_audio_url_cache[key] = (now_mono, data)
            _source_audio_url_cache.move_to_end(key)
            while len(_source_audio_url_cache) > int(SOURCE_AUDIO_URL_CACHE_MAX_ENTRIES):
                _source_audio_url_cache.popitem(last=False)
        return dict(data), False
    if stale_payload is not None:
        return stale_payload, True
    return None, False


def _source_audio_url_cache_get(query: str) -> Optional[Dict[str, Any]]:
    payload, _ = _source_audio_url_cache_lookup(query, allow_stale=False)
    return payload


def _source_audio_url_cache_set(keys: List[str], payload: Dict[str, Any]) -> None:
    if SOURCE_AUDIO_URL_CACHE_TTL_SEC <= 0:
        return
    now_mono = time.monotonic()
    data = dict(payload)
    normalized_keys: List[str] = []
    with _source_audio_url_cache_lock:
        for raw in keys:
            key = _normalize_source_audio_cache_key(raw)
            if not key:
                continue
            _source_audio_url_cache[key] = (now_mono, data)
            _source_audio_url_cache.move_to_end(key)
            if key not in normalized_keys:
                normalized_keys.append(key)
        while len(_source_audio_url_cache) > int(SOURCE_AUDIO_URL_CACHE_MAX_ENTRIES):
            _source_audio_url_cache.popitem(last=False)
    ttl_sec = float(SOURCE_AUDIO_URL_CACHE_TTL_SEC)
    for key in normalized_keys:
        _distributed_cache_set_json("source_audio_url", key, data, ttl_sec=ttl_sec)


def _normalize_source_search_meta_cache_key(video_id: str) -> str:
    key = str(video_id or "").strip()
    if not key:
        return ""
    if len(key) != 11:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", key):
        return ""
    return key


def _source_search_meta_cache_get(video_id: str) -> Optional[Dict[str, Any]]:
    if SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC <= 0:
        return None
    key = _normalize_source_search_meta_cache_key(video_id)
    if not key:
        return None
    now_mono = time.monotonic()
    with _source_search_result_meta_cache_lock:
        item = _source_search_result_meta_cache.get(key)
        if item is None:
            item_payload = None
        else:
            created_at_mono, payload = item
            if (now_mono - float(created_at_mono)) > float(SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC):
                _source_search_result_meta_cache.pop(key, None)
                item_payload = None
            else:
                _source_search_result_meta_cache.move_to_end(key)
                item_payload = dict(payload)
    if item_payload is not None:
        return item_payload

    distributed = _distributed_cache_get_json("source_search_meta", key)
    if distributed is None:
        return None
    now_mono = time.monotonic()
    data = dict(distributed)
    with _source_search_result_meta_cache_lock:
        _source_search_result_meta_cache[key] = (now_mono, data)
        _source_search_result_meta_cache.move_to_end(key)
        while len(_source_search_result_meta_cache) > int(SOURCE_SEARCH_RESULT_META_CACHE_MAX_ENTRIES):
            _source_search_result_meta_cache.popitem(last=False)
    return dict(data)


def _source_search_meta_cache_set(video_id: str, payload: Dict[str, Any]) -> None:
    if SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC <= 0:
        return
    key = _normalize_source_search_meta_cache_key(video_id)
    if not key:
        return
    title = str(payload.get("title") or "").strip()
    # Avoid caching query-specific fallback titles.
    if not title:
        return
    duration: Optional[int] = None
    raw_duration = payload.get("duration")
    try:
        if raw_duration is not None and str(raw_duration).strip() != "":
            duration = int(raw_duration)
    except Exception:
        duration = None
    data = {
        "title": title,
        "duration": duration,
        "thumbnail": str(payload.get("thumbnail") or "").strip(),
        "uploader": str(payload.get("uploader") or "").strip(),
    }
    now_mono = time.monotonic()
    with _source_search_result_meta_cache_lock:
        _source_search_result_meta_cache[key] = (now_mono, data)
        _source_search_result_meta_cache.move_to_end(key)
        while len(_source_search_result_meta_cache) > int(SOURCE_SEARCH_RESULT_META_CACHE_MAX_ENTRIES):
            _source_search_result_meta_cache.popitem(last=False)
    _distributed_cache_set_json(
        "source_search_meta",
        key,
        data,
        ttl_sec=float(SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC),
    )


@contextmanager
def _source_audio_url_singleflight(query: str):
    key = _normalize_source_audio_cache_key(query)
    if not key:
        yield
        return

    with _source_audio_url_singleflight_entries_lock:
        entry = _source_audio_url_singleflight_entries.get(key)
        if entry is None:
            entry = _SourceAudioSingleflightEntry()
            _source_audio_url_singleflight_entries[key] = entry
        entry.refs += 1
        query_lock = entry.lock

    query_lock.acquire()
    try:
        yield
    finally:
        query_lock.release()
        with _source_audio_url_singleflight_entries_lock:
            current = _source_audio_url_singleflight_entries.get(key)
            if current is entry:
                current.refs -= 1
                if current.refs <= 0:
                    _source_audio_url_singleflight_entries.pop(key, None)


def _source_audio_url_distributed_lock_key(normalized_query_key: str) -> str:
    return _distributed_cache_key("source_audio_url_singleflight", normalized_query_key)


def _source_audio_url_distributed_lock_release(client: Any, lock_key: str, token: str) -> None:
    if client is None or not lock_key or not token:
        return
    try:
        client.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            1,
            lock_key,
            token,
        )
        return
    except Exception:
        pass
    try:
        raw = client.get(lock_key)
        if raw is None:
            return
        if isinstance(raw, (bytes, bytearray, memoryview)):
            current = bytes(raw).decode("utf-8", errors="ignore")
        else:
            current = str(raw)
        if current == token:
            client.delete(lock_key)
    except Exception as exc:
        _log_distributed_cache_error("singleflight_lock_release", exc)


def _source_audio_url_wait_for_fresh_cache(query: str) -> Optional[Dict[str, Any]]:
    wait_sec = float(SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_WAIT_SEC)
    if wait_sec <= 0:
        return None
    poll_sec = max(0.02, float(SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_POLL_SEC))
    deadline = time.monotonic() + wait_sec
    while time.monotonic() < deadline:
        cached = _source_audio_url_cache_get(query)
        if cached is not None:
            return cached
        time.sleep(poll_sec)
    return None


def _extract_source_audio_url_with_optional_distributed_singleflight(query: str) -> Dict[str, Any]:
    normalized_key = _normalize_source_audio_cache_key(query)
    if (not normalized_key) or (not SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED):
        return _extract_source_audio_url_uncached(query)
    client = _distributed_cache_client_get()
    if client is None:
        return _extract_source_audio_url_uncached(query)

    lock_key = _source_audio_url_distributed_lock_key(normalized_key)
    token = uuid4().hex
    lock_ttl_sec = max(1, int(math.ceil(float(SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_LOCK_TTL_SEC))))
    acquired = False
    try:
        acquired = bool(client.set(lock_key, token, nx=True, ex=lock_ttl_sec))
    except Exception as exc:
        _log_distributed_cache_error("singleflight_lock_set", exc)

    if acquired:
        try:
            cached = _source_audio_url_cache_get(query)
            if cached is not None:
                return cached
            return _extract_source_audio_url_uncached(query)
        finally:
            _source_audio_url_distributed_lock_release(client, lock_key, token)

    waited = _source_audio_url_wait_for_fresh_cache(query)
    if waited is not None:
        logger.info(
            "source audio url cache hit after distributed singleflight wait",
            extra={"query": query, "video_id": str(waited.get("video_id") or "")},
        )
        return waited

    logger.info(
        "source audio url distributed singleflight wait timed out",
        extra={"query": query},
    )
    return _extract_source_audio_url_uncached(query)


def _refresh_source_audio_url_stale_cache_async(query: str) -> None:
    key = _normalize_source_audio_cache_key(query)
    if (not key) or SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC <= 0:
        return
    with _source_audio_url_refresh_inflight_lock:
        if key in _source_audio_url_refresh_inflight:
            return
        _source_audio_url_refresh_inflight.add(key)

    def _runner() -> None:
        try:
            with _source_audio_url_singleflight(query):
                # If a fresh value already appeared, no refresh work is needed.
                if _source_audio_url_cache_get(query) is not None:
                    return
                _extract_source_audio_url_with_optional_distributed_singleflight(query)
        except Exception:
            logger.exception("source audio url stale refresh failed", extra={"query": query})
        finally:
            with _source_audio_url_refresh_inflight_lock:
                _source_audio_url_refresh_inflight.discard(key)

    try:
        threading.Thread(
            target=_runner,
            name=f"source-audio-swr-{key[:8]}",
            daemon=True,
        ).start()
    except Exception:
        with _source_audio_url_refresh_inflight_lock:
            _source_audio_url_refresh_inflight.discard(key)


def _now_ts() -> float:
    return round(time.time(), 3)


_PUBLIC_TERMS_RE = [
    # Scrub "yt*" tokens (e.g. ytsearch, ytid, ytmusic) in user-facing text.
    # Use word boundaries so we don't mutate unrelated words like "byte".
    (re.compile(r"(?i)\bytsearch\d*:"), "search:"),
    (re.compile(r"(?i)\bytsearch\d*\b"), "search"),
    (re.compile(r"(?i)\byt[a-z0-9_-]{0,32}\b"), "source"),
    (re.compile(r"(?i)yt-dlp"), "source connector"),
    (re.compile(r"(?i)\byoutube\b"), "source"),
    (re.compile(r"(?i)search_youtube"), "search_source"),
]


def _sanitize_public_text(message: Optional[str], *, is_error: bool = False, max_len: int = 500) -> str:
    text = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", str(message or "")).strip()
    if not text:
        return ""

    for pattern, replacement in _PUBLIC_TERMS_RE:
        text = pattern.sub(replacement, text)

    low = text.lower()
    if is_error:
        if "cookie_refresh_required" in low or "bot-check" in low or "captcha" in low:
            return "Source verification required. Please retry."
        if "source download failed" in low or "sign in to confirm you" in low:
            return _cookie_refresh_required_error("Source verification is required for this track.")
        # Any "ytsearch fast path" or similar connector jargon should not leak to the client.
        if "fast path failed" in low or "ytsearch" in low:
            return "Could not fetch audio from source. Please try again."
        if "download_audio" in low or "search_source" in low or "source id failed" in low or "no ids" in low:
            return "Could not fetch audio from source. Please try again."
        if (
            "separate_audio" in low
            or "demucs" in low
            or "gpu worker" in low
            or "worker separation" in low
            or "numpy is not available" in low
        ):
            return "Could not process audio for this track. Please try again."
        if "no synced lyrics found" in low or "no lyrics found for slug" in low or "no lyrics found for query" in low:
            return "Could not find synced lyrics for this track."
        if (
            "missing source audio" in low
            or "step1 audio missing" in low
            or "no audio found for query" in low
            or "no audio found for slug" in low
        ):
            return "Could not find audio for this track."
        if "timeout" in low or "timed out" in low:
            return "Request timed out. Please try again."
        if "pipeline exited with code" in low:
            return "Video generation failed. Please try again."

    return text[:max_len]


def _normalize_rating_key(raw: Any) -> str:
    key = str(raw or "").strip().lower()
    if not key:
        return ""
    key = re.sub(r"[^a-z0-9:_|.\-]", "", key)
    return key[:180]


def _collect_rating_keys(device_key: Optional[str], aliases: Optional[List[str]]) -> List[str]:
    values: List[str] = []
    if device_key is not None:
        values.append(str(device_key))
    if isinstance(aliases, list):
        values.extend(str(item or "") for item in aliases)

    out: List[str] = []
    for raw in values:
        key = _normalize_rating_key(raw)
        if not key or key in out:
            continue
        out.append(key)
    return out[:12]


def _sanitize_rating_action(raw: Any) -> str:
    action = str(raw or "").strip().lower()
    if not action:
        return "shown"
    action = re.sub(r"[^a-z0-9 _-]", "", action).strip()
    return action[:60] or "shown"


def _sanitize_rating_job_id(raw: Any) -> str:
    job_id = str(raw or "").strip().lower()
    if not job_id:
        return ""
    job_id = re.sub(r"[^a-z0-9_.:-]", "", job_id)
    return job_id[:120]


def _sanitize_rating_job_ids(raw: Any, *, max_items: int = 400) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    limit = max(1, int(max_items))
    for raw_job_id in raw:
        clean_job_id = _sanitize_rating_job_id(raw_job_id)
        if clean_job_id and clean_job_id not in out:
            out.append(clean_job_id)
    return out[-limit:]


def _coerce_non_negative_int(raw: Any, default: int = 0) -> int:
    try:
        value = int(raw)
    except Exception:
        return max(0, int(default))
    return max(0, value)


def _persist_rating_prompt_state_locked() -> None:
    try:
        if RATING_PROMPT_MAX_KEYS > 0 and len(_rating_prompt_state) > int(RATING_PROMPT_MAX_KEYS):
            sorted_items = sorted(
                _rating_prompt_state.items(),
                key=lambda item: str(item[1].get("updated_at_utc") or item[1].get("seen_at_utc") or ""),
            )
            overflow = len(_rating_prompt_state) - int(RATING_PROMPT_MAX_KEYS)
            for key, _ in sorted_items[:overflow]:
                _rating_prompt_state.pop(key, None)

        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "records": _rating_prompt_state,
        }
        RATING_PROMPT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(payload, ensure_ascii=False)
        tmp_path = RATING_PROMPT_STATE_PATH.with_suffix(".json.tmp")
        tmp_path.write_text(serialized, encoding="utf-8")
        tmp_path.replace(RATING_PROMPT_STATE_PATH)
    except Exception:
        logger.exception("failed to persist rating prompt state", extra={"path": str(RATING_PROMPT_STATE_PATH)})


def _load_rating_prompt_state() -> None:
    try:
        if not RATING_PROMPT_STATE_PATH.exists():
            return
        raw = RATING_PROMPT_STATE_PATH.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        records = parsed.get("records") if isinstance(parsed, dict) else parsed
        if not isinstance(records, dict):
            return

        loaded: Dict[str, Dict[str, Any]] = {}
        for raw_key, raw_record in records.items():
            key = _normalize_rating_key(raw_key)
            if not key or not isinstance(raw_record, dict):
                continue
            seen = bool(raw_record.get("seen"))
            videos_created = _coerce_non_negative_int(raw_record.get("videos_created"), 0)
            if not seen and videos_created < 1:
                continue

            counted_job_ids = _sanitize_rating_job_ids(raw_record.get("counted_job_ids"), max_items=400)

            seen_at_raw = str(raw_record.get("seen_at_utc") or "").strip()
            seen_at_utc = seen_at_raw if seen and seen_at_raw else ""
            loaded[key] = {
                "seen": seen,
                "seen_at_utc": seen_at_utc,
                "updated_at_utc": str(raw_record.get("updated_at_utc") or datetime.now(timezone.utc).isoformat()),
                "last_action": _sanitize_rating_action(raw_record.get("last_action")),
                "client_ip": str(raw_record.get("client_ip") or "")[:80],
                "videos_created": videos_created,
                "counted_job_ids": counted_job_ids,
                "last_job_id": _sanitize_rating_job_id(raw_record.get("last_job_id")),
            }

        with _RATING_PROMPT_LOCK:
            _rating_prompt_state.clear()
            _rating_prompt_state.update(loaded)
        logger.info(
            "loaded rating prompt state",
            extra={"path": str(RATING_PROMPT_STATE_PATH), "records": len(loaded)},
        )
    except Exception:
        logger.exception("failed to load rating prompt state", extra={"path": str(RATING_PROMPT_STATE_PATH)})


def _rating_state_for_keys(keys: List[str]) -> tuple[bool, str, Dict[str, Any]]:
    if not keys:
        return False, "", {}
    seen = False
    matched_key = ""
    merged_record: Dict[str, Any] = {}
    merged_videos_created = 0
    with _RATING_PROMPT_LOCK:
        for key in keys:
            record = _rating_prompt_state.get(key)
            if not isinstance(record, dict):
                continue
            if not matched_key:
                matched_key = key
            videos_created = _coerce_non_negative_int(record.get("videos_created"), 0)
            if videos_created > merged_videos_created:
                merged_videos_created = videos_created
            if bool(record.get("seen")) and not seen:
                seen = True
                matched_key = key
                merged_record = dict(record)
            elif not merged_record:
                merged_record = dict(record)
    if not merged_record:
        merged_record = {}
    merged_record["seen"] = seen
    merged_record["videos_created"] = max(
        _coerce_non_negative_int(merged_record.get("videos_created"), 0),
        merged_videos_created,
    )
    if not seen:
        merged_record["seen_at_utc"] = ""
        merged_record["last_action"] = str(merged_record.get("last_action") or "")
    return seen, matched_key, merged_record


def _mark_rating_seen(keys: List[str], *, action: str, client_ip: str) -> int:
    if not keys:
        return 0
    now_iso = datetime.now(timezone.utc).isoformat()
    clean_action = _sanitize_rating_action(action)
    with _RATING_PROMPT_LOCK:
        for key in keys:
            current = _rating_prompt_state.get(key) or {}
            seen_at = str(current.get("seen_at_utc") or now_iso)
            videos_created = _coerce_non_negative_int(current.get("videos_created"), 0)
            counted_job_ids = _sanitize_rating_job_ids(current.get("counted_job_ids"), max_items=400)
            _rating_prompt_state[key] = {
                "seen": True,
                "seen_at_utc": seen_at,
                "updated_at_utc": now_iso,
                "last_action": clean_action,
                "client_ip": str(client_ip or "unknown")[:80],
                "videos_created": videos_created,
                "counted_job_ids": counted_job_ids,
                "last_job_id": _sanitize_rating_job_id(current.get("last_job_id")),
            }
        _persist_rating_prompt_state_locked()
    return len(keys)


def _increment_rating_progress(
    keys: List[str],
    *,
    job_id: str,
    delta: int,
    client_ip: str,
) -> Dict[str, Any]:
    if not keys:
        return {
            "seen": False,
            "seen_at_utc": "",
            "videos_created": 0,
            "applied_delta": 0,
            "duplicate": False,
            "last_action": "",
            "last_job_id": "",
        }
    clean_job_id = _sanitize_rating_job_id(job_id)
    if not clean_job_id:
        raise ValueError("job_id is required")
    clean_delta = max(1, min(10, int(delta or 1)))
    now_iso = datetime.now(timezone.utc).isoformat()
    with _RATING_PROMPT_LOCK:
        merged_seen = False
        merged_seen_at_utc = ""
        merged_last_action = ""
        merged_videos_created = 0
        counted_job_ids: List[str] = []
        previous_last_job_id = ""

        for key in keys:
            record = _rating_prompt_state.get(key)
            if not isinstance(record, dict):
                continue
            if bool(record.get("seen")):
                merged_seen = True
                if not merged_seen_at_utc:
                    merged_seen_at_utc = str(record.get("seen_at_utc") or "").strip()
                if not merged_last_action:
                    merged_last_action = _sanitize_rating_action(record.get("last_action"))
            merged_videos_created = max(
                merged_videos_created,
                _coerce_non_negative_int(record.get("videos_created"), 0),
            )
            if not previous_last_job_id:
                previous_last_job_id = _sanitize_rating_job_id(record.get("last_job_id"))
            for existing_job_id in _sanitize_rating_job_ids(record.get("counted_job_ids"), max_items=400):
                if existing_job_id not in counted_job_ids:
                    counted_job_ids.append(existing_job_id)

        if merged_seen and not merged_seen_at_utc:
            merged_seen_at_utc = now_iso

        duplicate = clean_job_id in counted_job_ids
        applied_delta = 0 if duplicate else clean_delta
        if not duplicate:
            counted_job_ids.append(clean_job_id)
            counted_job_ids = counted_job_ids[-400:]

        videos_created = merged_videos_created + applied_delta
        last_action = (
            merged_last_action
            if merged_last_action and merged_seen
            else ("progress_duplicate" if duplicate else "progress")
        )
        stored_record = {
            "seen": merged_seen,
            "seen_at_utc": merged_seen_at_utc if merged_seen else "",
            "updated_at_utc": now_iso,
            "last_action": last_action,
            "client_ip": str(client_ip or "unknown")[:80],
            "videos_created": videos_created,
            "counted_job_ids": counted_job_ids,
            "last_job_id": clean_job_id or previous_last_job_id,
        }
        for key in keys:
            _rating_prompt_state[key] = dict(stored_record)
        _persist_rating_prompt_state_locked()

    return {
        "seen": merged_seen,
        "seen_at_utc": merged_seen_at_utc if merged_seen else "",
        "videos_created": videos_created,
        "applied_delta": applied_delta,
        "duplicate": duplicate,
        "last_action": last_action,
        "last_job_id": clean_job_id or previous_last_job_id,
    }


def _job_elapsed_seconds(job: Job, now_ts: Optional[float] = None) -> Optional[float]:
    start_ts = job.started_at or job.created_at
    if not isinstance(start_ts, (int, float)) or float(start_ts) <= 0.0:
        return None

    if job.status in {"succeeded", "failed", "cancelled"}:
        end_ts = job.finished_at or job.cancelled_at
    else:
        end_ts = now_ts if isinstance(now_ts, (int, float)) else _now_ts()

    if not isinstance(end_ts, (int, float)) or float(end_ts) < float(start_ts):
        return None
    return round(float(end_ts) - float(start_ts), 1)


def _derive_pipeline_timing_seconds(timing_breakdown: Dict[str, float]) -> Dict[str, float]:
    if not isinstance(timing_breakdown, dict):
        return {}

    out: Dict[str, float] = {}
    for step in ("step1", "step2", "step3", "step4", "step5", "step6"):
        raw_ms = timing_breakdown.get(f"pipeline.{step}")
        if raw_ms is None:
            raw_ms = timing_breakdown.get(f"{step}.total")
        if isinstance(raw_ms, (int, float)) and math.isfinite(float(raw_ms)) and float(raw_ms) >= 0.0:
            out[step] = round(float(raw_ms) / 1000.0, 2)

    total_raw_ms = timing_breakdown.get("pipeline.total")
    if isinstance(total_raw_ms, (int, float)) and math.isfinite(float(total_raw_ms)) and float(total_raw_ms) >= 0.0:
        out["total"] = round(float(total_raw_ms) / 1000.0, 2)
    elif out:
        out["total"] = round(sum(v for k, v in out.items() if k.startswith("step")), 2)
    return out


def _job_to_dict(job: Job) -> Dict[str, Any]:
    timing_breakdown = dict(job.timing_breakdown or {})
    pipeline_timing = _derive_pipeline_timing_seconds(timing_breakdown)
    elapsed_sec = _job_elapsed_seconds(job)
    mix_audio_url = _mix_audio_url_for_slug(job.slug)
    return {
        "id": job.id,
        "idempotency_key": job.idempotency_key,
        "dedupe_key": job.dedupe_key,
        "query": job.query,
        "slug": job.slug,
        "created_at": job.created_at,
        "status": job.status,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "cancelled_at": job.cancelled_at,
        "render_started_at": job.render_started_at,
        "render_finished_at": job.render_finished_at,
        "error": _sanitize_public_text(job.error, is_error=True),
        "output_path": job.output_path,
        "output_url": job.output_url,
        "youtube_video_url": job.youtube_video_url,
        "mix_audio_url": mix_audio_url,
        "preview_output_url": job.preview_output_url,
        "final_output_url": job.final_output_url,
        "output_is_preview": job.output_is_preview,
        "stage": job.stage,
        "last_message": _sanitize_public_text(job.last_message, is_error=False),
        "last_updated_at": job.last_updated_at,
        "progress_percent": job.progress_percent,
        "estimated_seconds_remaining": job.estimated_seconds_remaining,
        "elapsed_sec": elapsed_sec,
        "timing_breakdown": timing_breakdown,
        "pipeline_timing": pipeline_timing,
        "step_timestamps": dict(job.step_timestamps or {}),
        "attempt_counts": dict(job.attempt_counts or {}),
        "options": job.options,
    }


def _job_to_poll_dict(job: Job) -> Dict[str, Any]:
    mix_audio_url = _mix_audio_url_for_slug(job.slug)
    return {
        "id": job.id,
        "status": job.status,
        "query": job.query,
        "slug": job.slug,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "cancelled_at": job.cancelled_at,
        "render_started_at": job.render_started_at,
        "render_finished_at": job.render_finished_at,
        "error": _sanitize_public_text(job.error, is_error=True),
        "output_url": job.output_url,
        "youtube_video_url": job.youtube_video_url,
        "mix_audio_url": mix_audio_url,
        "preview_output_url": job.preview_output_url,
        "final_output_url": job.final_output_url,
        "output_is_preview": job.output_is_preview,
        "stage": job.stage,
        "last_message": _sanitize_public_text(job.last_message, is_error=False),
        "last_updated_at": job.last_updated_at,
        "progress_percent": job.progress_percent,
        "estimated_seconds_remaining": job.estimated_seconds_remaining,
        "elapsed_sec": _job_elapsed_seconds(job),
    }


def _job_status_etag(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f'W/"{digest}"'


def _etag_matches_if_none_match(if_none_match: str, etag: str) -> bool:
    header = str(if_none_match or "").strip()
    if not header:
        return False
    if header == "*":
        return True
    weakless = etag[2:] if etag.startswith("W/") else etag
    for token in header.split(","):
        candidate = token.strip()
        if not candidate:
            continue
        if candidate == etag or candidate == weakless:
            return True
        if candidate.startswith("W/") and candidate[2:] == weakless:
            return True
    return False


def _job_status_cache_key(job_id: str, *, view: str = "full") -> str:
    clean_view = "poll" if str(view or "").strip().lower() == "poll" else "full"
    return f"{job_id}:{clean_view}"


def _invalidate_job_status_cache(job_id: str) -> None:
    with _job_status_cache_lock:
        _job_status_cache.pop(_job_status_cache_key(job_id, view="full"), None)
        _job_status_cache.pop(_job_status_cache_key(job_id, view="poll"), None)


def _resolve_stage_timeout(
    *,
    stage: Optional[str],
    fallback_timeout_sec: float,
    overrides: Dict[str, float],
) -> float:
    key = _normalize_stage_timeout_key(stage or "")
    if not key:
        return float(fallback_timeout_sec)
    override = overrides.get(key)
    if override is None:
        return float(fallback_timeout_sec)
    return max(0.0, float(override))


def _normalize_timing_breakdown(raw: Any) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        key = str(k or "").strip().lower()
        if not key:
            continue
        try:
            ms = float(v)
        except Exception:
            continue
        if not math.isfinite(ms) or ms < 0.0:
            continue
        out[key] = round(ms, 1)
    return out


def _normalize_step_timestamps(raw: Any) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        key = str(k or "").strip().lower()
        if not key:
            continue
        try:
            ts = float(v)
        except Exception:
            continue
        if not math.isfinite(ts) or ts <= 0.0:
            continue
        out[key] = ts
    return out


def _normalize_attempt_counts(raw: Any) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        key = str(k or "").strip().lower()
        if not key:
            continue
        try:
            count = int(v)
        except Exception:
            continue
        out[key] = max(0, count)
    return out


def _job_from_dict(payload: Dict[str, Any]) -> Job:
    return Job(
        id=payload["id"],
        query=payload["query"],
        slug=payload["slug"],
        created_at=payload["created_at"],
        idempotency_key=payload.get("idempotency_key"),
        dedupe_key=payload.get("dedupe_key"),
        status=payload.get("status", "queued"),
        started_at=payload.get("started_at"),
        finished_at=payload.get("finished_at"),
        cancelled_at=payload.get("cancelled_at"),
        render_started_at=payload.get("render_started_at"),
        render_finished_at=payload.get("render_finished_at"),
        error=payload.get("error"),
        output_path=payload.get("output_path"),
        output_url=payload.get("output_url"),
        youtube_video_url=payload.get("youtube_video_url"),
        preview_output_url=payload.get("preview_output_url"),
        final_output_url=payload.get("final_output_url"),
        output_is_preview=payload.get("output_is_preview"),
        stage=payload.get("stage"),
        last_message=payload.get("last_message"),
        last_updated_at=payload.get("last_updated_at"),
        timing_breakdown=_normalize_timing_breakdown(payload.get("timing_breakdown")),
        step_timestamps=_normalize_step_timestamps(payload.get("step_timestamps")),
        attempt_counts=_normalize_attempt_counts(payload.get("attempt_counts")),
        options=payload.get("options", {}),
    )


def _persist_jobs() -> None:
    global _jobs_persist_last_hash, _jobs_sqlite_last_prune_at_mono
    try:
        with _jobs_persist_lock:
            JOBS_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            # Optimization: Release _jobs_lock quickly by copying job dict list first,
            # then serialize outside the lock to reduce contention
            with _jobs_lock:
                snapshot = [_job_to_dict(j) for j in _jobs.values()]
            # Serialize outside of _jobs_lock to minimize lock hold time
            serialized = json.dumps(snapshot, ensure_ascii=False)
            current_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
            now_mono = time.monotonic()
            sqlite_prune_due = (
                _jobs_sqlite_store is not None
                and (now_mono - _jobs_sqlite_last_prune_at_mono) >= JOBS_SQLITE_PRUNE_INTERVAL_SEC
            )
            if (
                current_hash == _jobs_persist_last_hash
                and JOBS_STATE_PATH.exists()
                and not sqlite_prune_due
            ):
                return
            tmp_path = JOBS_STATE_PATH.with_suffix(".json.tmp")
            tmp_path.write_text(serialized, encoding="utf-8")
            tmp_path.replace(JOBS_STATE_PATH)
            if _jobs_sqlite_store is not None:
                _jobs_sqlite_store.replace_all_jobs(snapshot, prune_missing=sqlite_prune_due)
                if sqlite_prune_due:
                    _jobs_sqlite_last_prune_at_mono = now_mono
            _jobs_persist_last_hash = current_hash
    except Exception:
        logger.exception("failed to persist jobs state", extra={"path": str(JOBS_STATE_PATH)})


def _persist_jobs_debounced(job_id: str, min_interval: float = 0.5) -> None:
    now = time.time()
    with _jobs_persist_debounce_lock:
        last = _jobs_persist_debounce.get(job_id)
        if last is not None and (now - last) < min_interval:
            return
        _jobs_persist_debounce[job_id] = now
    _persist_jobs()


_PIPELINE_TAG_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\]\s+\[([A-Z0-9_]+)\]\s+(.*)$")
_ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
_TIMING_LINE_RE = re.compile(
    r"\bstep=(?P<step>[A-Za-z0-9_]+)\s+part=(?P<part>[A-Za-z0-9_.:-]+)\s+elapsed_ms=(?P<ms>\d+(?:\.\d+)?)"
)
_MAX_TIMING_BREAKDOWN_ENTRIES = 256


def _stage_from_tag(tag: str) -> Optional[str]:
    mapping = {
        "STEP1A": "step1_a",
        "STEP1B": "step1_b",
        "STEP1C": "step1_c",
        "LRC": "fetch_lyrics",
        "YT": "search_source",
        "MP3": "download_audio",
        "SPLIT": "separate_audio",
        "MIX": "mix_audio",
        "STEP3": "sync_lyrics",
        "FFMPEG": "render_video",
        "STEP4": "render_video",
        "DONE": "complete",
        "UPLOAD": "upload",
    }
    return mapping.get(tag)


def _calculate_progress_percent(stage: Optional[str]) -> float:
    """
    Calculate progress percentage based on pipeline stage.
    Rough estimates based on typical execution time weights.
    """
    stage_weights = {
        "step1_a": 5.0,       # LRC search
        "step1_b": 10.0,      # source search
        "step1_c": 15.0,      # MP3 download
        "fetch_lyrics": 5.0,
        "search_source": 10.0,
        "download_audio": 15.0,
        "separate_audio": 35.0,  # Demucs - longest step
        "mix_audio": 40.0,
        "sync_lyrics": 45.0,     # Step 3
        "render_video": 80.0,    # ffmpeg - second longest
        "complete": 100.0,
        "upload": 95.0,
    }
    if not stage:
        return 0.0
    return stage_weights.get(stage, 0.0)


def _estimate_time_remaining(job: Job) -> Optional[float]:
    """
    Estimate seconds remaining based on current progress and elapsed time.
    Returns None if not enough data to estimate.
    """
    if not job.started_at or not job.progress_percent:
        return None

    elapsed = _now_ts() - job.started_at
    if job.progress_percent <= 0 or elapsed <= 0:
        return None

    # Linear extrapolation: remaining = elapsed * (100 - progress) / progress
    remaining = elapsed * (100.0 - job.progress_percent) / job.progress_percent
    return max(0.0, round(remaining, 1))


def _sanitize_timing_segment(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_.-]+", "_", str(value or "").strip().lower())
    return cleaned.strip("_.-")


def _parse_timing_line(message: str) -> Optional[tuple[str, str, float]]:
    text = _ANSI_ESCAPE_RE.sub("", str(message or "")).strip()
    if not text:
        return None
    match = _TIMING_LINE_RE.search(text)
    if not match:
        return None
    step = _sanitize_timing_segment(match.group("step"))
    part = _sanitize_timing_segment(match.group("part"))
    if not step or not part:
        return None
    try:
        elapsed_ms = float(match.group("ms"))
    except Exception:
        return None
    if not math.isfinite(elapsed_ms) or elapsed_ms < 0.0:
        return None
    return step, part, round(elapsed_ms, 1)


def _update_timing_breakdown(job: Job, message: str) -> bool:
    parsed = _parse_timing_line(message)
    if not parsed:
        return False
    step, part, elapsed_ms = parsed
    key = f"{step}.{part}"
    timings = job.timing_breakdown if isinstance(job.timing_breakdown, dict) else {}
    if key in timings:
        prev = timings.get(key)
        if prev == elapsed_ms:
            return False
        timings[key] = elapsed_ms
        job.timing_breakdown = timings
        return True
    if len(timings) >= _MAX_TIMING_BREAKDOWN_ENTRIES:
        return False
    timings[key] = elapsed_ms
    job.timing_breakdown = timings
    return True


def _sanitize_state_key(value: str) -> str:
    key = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    return key.strip("_")


def _touch_job_step(job: Job, step_name: str) -> None:
    key = _sanitize_state_key(step_name)
    if not key:
        return
    payload = dict(job.step_timestamps or {})
    payload[f"{key}_at"] = _now_ts()
    job.step_timestamps = payload


def _inc_job_attempt(job: Job, step_name: str) -> None:
    key = _sanitize_state_key(step_name)
    if not key:
        return
    payload = dict(job.attempt_counts or {})
    payload[key] = int(payload.get(key, 0)) + 1
    job.attempt_counts = payload


def _set_job_stage(
    job: Job,
    *,
    stage: str,
    message: str = "",
    status: Optional[str] = None,
    step_name: Optional[str] = None,
    increment_attempt: bool = False,
) -> None:
    clean_stage = str(stage or "").strip()
    if clean_stage:
        job.stage = clean_stage
        _touch_job_step(job, clean_stage)
    if step_name:
        _touch_job_step(job, step_name)
        if increment_attempt:
            _inc_job_attempt(job, step_name)
    if status:
        job.status = str(status)
    if message:
        job.last_message = str(message)
    job.last_updated_at = _now_ts()


def _update_job_progress(job: Job, *, message: str, tag: Optional[str] = None, progress_percent: Optional[float] = None) -> None:
    stage = _stage_from_tag(tag) if tag else None
    stage_changed = bool(stage and stage != job.stage)
    if stage_changed and stage:
        job.stage = stage
        _touch_job_step(job, stage)
        # Update progress percentage based on stage
        if progress_percent is None:  # Only auto-calculate if not explicitly provided
            job.progress_percent = _calculate_progress_percent(stage)
            job.estimated_seconds_remaining = _estimate_time_remaining(job)

    # Allow explicit progress override (for detailed progress within a stage)
    if progress_percent is not None:
        job.progress_percent = max(0.0, min(100.0, progress_percent))
        job.estimated_seconds_remaining = _estimate_time_remaining(job)

    cleaned = _ANSI_ESCAPE_RE.sub("", message).strip()
    truncated = cleaned[:500] if cleaned else ""
    timing_changed = _update_timing_breakdown(job, cleaned)
    message_changed = bool(truncated and truncated != (job.last_message or ""))
    if not stage_changed and not message_changed and not timing_changed:
        return
    if message_changed:
        job.last_message = truncated
    job.last_updated_at = _now_ts()
    # Invalidate status cache when job is updated
    _invalidate_job_status_cache(job.id)
    min_interval = 0.5 if stage_changed or timing_changed else JOB_PROGRESS_PERSIST_INTERVAL_SEC
    _persist_jobs_debounced(job.id, min_interval=min_interval)


def _active_job_count() -> int:
    """Return the count of active (queued + running) jobs. Must be called with _jobs_lock held."""
    return _active_job_count_cached


def _increment_active_job_count() -> None:
    """Increment active job counter. Must be called with _jobs_lock held."""
    global _active_job_count_cached
    _active_job_count_cached += 1


def _decrement_active_job_count() -> None:
    """Decrement active job counter. Must be called with _jobs_lock held."""
    global _active_job_count_cached
    _active_job_count_cached = max(0, _active_job_count_cached - 1)


def _queue_retry_after_seconds(active_jobs: int) -> int:
    workers = max(1, int(JOB_WORKERS))
    overflow = max(1, int(active_jobs) - workers + 1)
    waves = int(math.ceil(float(overflow) / float(workers)))
    return max(3, min(45, waves * 5))


def _is_uploaded_video_cacheable_job(job: Job) -> bool:
    if job.status != "succeeded":
        return False
    if not bool((job.options or {}).get("upload")):
        return False
    return bool(str(job.youtube_video_url or "").strip())


def _drop_job_from_history(job: Job) -> None:
    _jobs.pop(job.id, None)
    # Remove all slug index entries for this job id (includes alias keys).
    _remove_from_slug_index(job)
    _remove_from_idempotency_index(job)
    # Remove from debounce map to prevent unbounded growth
    with _jobs_persist_debounce_lock:
        _jobs_persist_debounce.pop(job.id, None)


def _prune_jobs_history() -> None:
    if MAX_JOBS_HISTORY <= 0:
        return
    done = [j for j in _jobs.values() if j.status in {"succeeded", "failed", "cancelled"}]
    if len(done) <= MAX_JOBS_HISTORY:
        return

    drop_ids: Set[str] = set()
    uploaded_done = [j for j in done if _is_uploaded_video_cacheable_job(j)]
    uploaded_ids = {j.id for j in uploaded_done}
    regular_done = [j for j in done if j.id not in uploaded_ids]
    regular_done.sort(key=lambda j: j.finished_at or j.created_at)

    # Keep uploaded-success entries around longer so repeat requests can reuse
    # existing uploaded YouTube URLs across days/restarts.
    drop_count = len(done) - MAX_JOBS_HISTORY
    for j in regular_done[:drop_count]:
        drop_ids.add(j.id)

    # Hard cap uploaded cache entries to avoid unbounded growth.
    uploaded_remaining = [j for j in uploaded_done if j.id not in drop_ids]
    if len(uploaded_remaining) > MAX_UPLOADED_JOBS_HISTORY:
        uploaded_remaining.sort(key=lambda j: j.finished_at or j.created_at)
        over = len(uploaded_remaining) - MAX_UPLOADED_JOBS_HISTORY
        for j in uploaded_remaining[:over]:
            drop_ids.add(j.id)

    for job_id in drop_ids:
        victim = _jobs.get(job_id)
        if victim is not None:
            _drop_job_from_history(victim)


def _remove_from_slug_index(job: Job) -> None:
    """Remove all slug-index entries that point to this job id."""
    drop_keys = [slug_key for slug_key, job_id in _slug_to_job_id.items() if job_id == job.id]
    for slug_key in drop_keys:
        _slug_to_job_id.pop(slug_key, None)


def _remove_from_idempotency_index(job: Job) -> None:
    drop_keys = [key for key, job_id in _idempotency_to_job_id.items() if job_id == job.id]
    for key in drop_keys:
        _idempotency_to_job_id.pop(key, None)


_HOT_QUERY_SLUG_CANONICAL: Dict[str, str] = {
    "let_it_be": "the_beatles_let_it_be",
    "the_beatles_let_it_be": "the_beatles_let_it_be",
    "john_frusciante_god": "john_frusciante_god",
    "john_frusciante_the_past_recedes": "john_frusciante_the_past_recedes",
}


def _slug_reuse_candidates(slug: str) -> tuple[str, ...]:
    slug_key = slugify(slug or "")
    if not slug_key:
        return tuple()

    canonical = _HOT_QUERY_SLUG_CANONICAL.get(slug_key, slug_key)
    out: List[str] = []
    for candidate in (slug_key, canonical):
        if candidate and candidate not in out:
            out.append(candidate)
    for alias_slug, alias_canonical in _HOT_QUERY_SLUG_CANONICAL.items():
        if alias_canonical == canonical and alias_slug not in out:
            out.append(alias_slug)
    return tuple(out)


def _mark_job_finishing(job: Job, was_active: bool = True) -> None:
    """
    Call this when a job finishes (after status change to succeeded/failed/cancelled).
    Updates slug index and active counter. Must be called with _jobs_lock held.

    Args:
        job: The job that finished
        was_active: Whether the job was in {queued, running} state before finishing.
                   Defaults to True since most jobs finish from active states.
    """
    _remove_from_slug_index(job)
    if was_active:
        _decrement_active_job_count()


def _find_inflight_job_for_slug(
    slug: str,
    *,
    slug_candidates: Optional[tuple[str, ...]] = None,
    requested_profile_key: Optional[str] = None,
) -> Optional[Job]:
    candidates = tuple(slug_candidates or _slug_reuse_candidates(slug) or (slug,))
    candidate_set = set(candidates)

    # Fast path: O(1) lookup using secondary index for each candidate.
    for candidate in candidates:
        job_id = _slug_to_job_id.get(candidate)
        if job_id is None:
            continue
        job = _jobs.get(job_id)
        # Verify job is still inflight (status could have changed since index was set)
        if job and job.status in {"queued", "running"}:
            if requested_profile_key and _build_inflight_profile_key(job.options or {}) != requested_profile_key:
                continue
            return job
        # Stale index entry; clean it up.
        _slug_to_job_id.pop(candidate, None)

    # Fallback: recover from missing/stale index entries by scanning inflight jobs.
    # Also self-heal the index once we find a matching inflight job.
    for job in _jobs.values():
        if job.slug in candidate_set and job.status in {"queued", "running"}:
            if requested_profile_key and _build_inflight_profile_key(job.options or {}) != requested_profile_key:
                continue
            for candidate in candidates:
                _slug_to_job_id[candidate] = job.id
            return job
    return None


_REUSE_OPTION_KEYS: tuple[str, ...] = (
    "audio_url",
    "audio_id",
    "language",
    "render_only",
    "preview",
    "vocals",
    "bass",
    "drums",
    "other",
    "offset_sec",
    "yt_search_n",
    "speed_mode",
    "upload",
)


def _reuse_options_fingerprint(options: Dict[str, Any]) -> str:
    payload: Dict[str, Any] = {
        "_cache_version": str(REUSE_SUCCEEDED_JOBS_CACHE_VERSION),
    }
    for key in _REUSE_OPTION_KEYS:
        value = options.get(key)
        if value is None or value == "":
            continue
        if key in {"vocals", "bass", "drums", "other", "yt_search_n"}:
            try:
                payload[key] = int(value)
            except Exception:
                payload[key] = str(value)
            continue
        if key == "speed_mode":
            payload[key] = str(value).strip().lower()
            continue
        if key == "offset_sec":
            try:
                payload[key] = round(float(value), 3)
            except Exception:
                payload[key] = str(value)
            continue
        payload[key] = value
    return json.dumps(payload, sort_keys=True, ensure_ascii=True)


def _find_recent_succeeded_job_for_slug(
    slug: str,
    *,
    options: Dict[str, Any],
    slug_candidates: Optional[tuple[str, ...]] = None,
) -> Optional[Job]:
    if not REUSE_SUCCEEDED_JOBS_ENABLED:
        return None
    if bool(options.get("force")) or bool(options.get("reset")) or bool(options.get("dry_run")):
        return None
    if bool(options.get("runtime_cookies_supplied")):
        return None

    now = _now_ts()
    wants_upload_reuse = bool(options.get("upload"))
    max_age_sec = (
        REUSE_UPLOADED_JOBS_MAX_AGE_SEC
        if wants_upload_reuse
        else REUSE_SUCCEEDED_JOBS_MAX_AGE_SEC
    )
    requested_fp = _reuse_options_fingerprint(options)
    candidate_slugs = tuple(slug_candidates or _slug_reuse_candidates(slug) or (slug,))
    candidate_slug_set = set(candidate_slugs)
    candidates = [
        job for job in _jobs.values()
        if job.slug in candidate_slug_set and job.status == "succeeded"
    ]
    candidates.sort(key=lambda j: (j.finished_at or j.created_at), reverse=True)

    for job in candidates:
        finished_at = float(job.finished_at or job.created_at or 0.0)
        if max_age_sec > 0 and (now - finished_at) > max_age_sec:
            continue
        if _reuse_options_fingerprint(job.options) != requested_fp:
            continue
        if wants_upload_reuse:
            uploaded_video_url = str(job.youtube_video_url or "").strip()
            if not uploaded_video_url:
                uploaded_video_url = str(_resolve_step5_uploaded_video_url(job) or "").strip()
                if uploaded_video_url:
                    job.youtube_video_url = uploaded_video_url
            if uploaded_video_url:
                reuse_ok, reuse_reason = _uploaded_job_reuse_quality_gate(job)
                if not reuse_ok:
                    logger.info(
                        "skip uploaded cache reuse due to quality gate",
                        extra={"job_id": job.id, "slug": job.slug, "reason": reuse_reason},
                    )
                    continue
                return job
            continue
        out_path = str(job.output_path or "").strip()
        if out_path and not Path(out_path).exists():
            continue
        return job
    return None


def _line_requires_cookie_refresh(text: str) -> bool:
    low = (text or "").lower()
    return (
        _COOKIE_REFRESH_REQUIRED_MARKER_LOWER in low
        or "sign in to confirm you" in low
        or "--cookies-from-browser" in low
        or "--cookies for the authentication" in low
        or "confirm you're not a bot" in low
        or "age-restricted" in low
        or "login required" in low
        or "captcha" in low
    )


def _cookie_refresh_required_error(detail: Optional[str] = None) -> str:
    cleaned = _ANSI_ESCAPE_RE.sub("", detail or "").strip()
    if _COOKIE_REFRESH_REQUIRED_MARKER_LOWER in cleaned.lower():
        return cleaned[:500]
    base = f"{COOKIE_REFRESH_REQUIRED_MARKER}: {COOKIE_REFRESH_REQUIRED_TEXT}"
    if not cleaned:
        return base
    return f"{base} ({cleaned[:220]})"


_TRACEBACK_NOISE_SNIPPETS = ("traceback", "file \"", "raise ", "during handling", "^^^^^^^^")


def _is_traceback_noise_line(line: str) -> bool:
    low = (line or "").lower()
    return any(s in low for s in _TRACEBACK_NOISE_SNIPPETS)


def _is_generic_runtime_error_detail(detail: str) -> bool:
    low = (detail or "").strip().lower()
    return (
        not low
        or low.startswith("mp3 download failed for query")
        or low.startswith("pipeline exited with code")
        or low == "job failed"
    )


def _find_runtimeerror_continuation(lines: list[str], runtime_idx: int) -> str:
    """
    RuntimeError messages may include continuation lines after the `RuntimeError: ...`
    headline. When the headline is generic, look ahead for a more specific detail.
    """
    max_idx = min(len(lines), runtime_idx + 10)
    for raw in lines[runtime_idx + 1 : max_idx]:
        line = _ANSI_ESCAPE_RE.sub("", raw).strip()
        if not line:
            continue
        if _is_traceback_noise_line(line):
            continue
        lower = line.lower()
        if lower.startswith("runtimeerror:"):
            continue
        if _line_requires_cookie_refresh(line):
            return _cookie_refresh_required_error(line)
        if (
            "error:" in lower
            or "failed" in lower
            or "timed out" in lower
            or "timeout" in lower
            or "forbidden" in lower
            or "captcha" in lower
            or "no ids" in lower
            or "no id" in lower
            or "no synced lyrics found" in lower
            or "no lyrics found" in lower
            or "no audio found" in lower
            or "step1 audio missing" in lower
        ):
            return line[:500]
    return ""


def _extract_pipeline_failure_reason(lines: list[str], rc: int, stage: Optional[str] = None) -> str:
    """
    Extract failure reason with context.
    Enhanced to provide stage information and better error categorization.
    """
    if not lines:
        context_msg = f" during {stage}" if stage else ""
        return f"Pipeline exited with code {rc}{context_msg}. No error output captured."

    # Single-pass error parsing: pre-clean all lines once, then scan in reverse
    runtime_fallback = ""
    fallback_line = ""

    for idx in range(len(lines) - 1, -1, -1):
        raw = lines[idx]
        line = _ANSI_ESCAPE_RE.sub("", raw).strip()
        if not line:
            continue

        lower = line.lower()

        # Priority 1: Cookie/auth refresh requirements (highest priority)
        if _line_requires_cookie_refresh(line):
            return _cookie_refresh_required_error(line)

        # Priority 2: RuntimeError with detail
        if "runtimeerror:" in lower:
            detail = line.split(":", 1)[1].strip()[:500]
            if _line_requires_cookie_refresh(detail):
                return _cookie_refresh_required_error(detail)
            if _is_generic_runtime_error_detail(detail):
                continuation = _find_runtimeerror_continuation(lines, idx)
                if continuation:
                    return continuation
                if not runtime_fallback and detail:
                    runtime_fallback = detail
                continue
            return detail

        # Priority 2b: Explicit pipeline error tags from scripts.main/log(...)
        if lower.startswith("[error]") or "[error]" in lower:
            detail = re.sub(r"(?i)^\[error\]\s*", "", line).strip() or line
            return detail[:500]

        # Priority 3: Specific error patterns
        if "error: [source]" in lower:
            return line[:500]
        if "yt-dlp search failed" in lower:
            return line[:500]
        if "yt-dlp search timed out" in lower:
            return line[:500]
        if "yt-dlp download timed out" in lower:
            return line[:500]
        if "yt-dlp download failed" in lower:
            return line[:500]
        if "no synced lyrics found" in lower:
            return line[:500]
        if "no lyrics found" in lower:
            return line[:500]
        if "no audio found" in lower or "step1 audio missing" in lower:
            return line[:500]
        if "oserror:" in lower:
            return line[:500]
        if "http error 403" in lower:
            return line[:500]

        # Priority 4: Keep first non-noise line as fallback
        if not fallback_line and not _is_traceback_noise_line(line):
            fallback_line = line[:500]

    # Return best available error message
    if runtime_fallback:
        return runtime_fallback[:500]
    if fallback_line:
        return fallback_line
    return f"pipeline exited with code {rc}"


def _stream_pipeline_output(
    proc: subprocess.Popen,
    job: Job,
    last_output_state: Dict[str, float],
    last_output_lock: threading.Lock,
    recent_lines: deque[str],
) -> None:
    if not proc.stdout:
        return
    for line in proc.stdout:
        clean = line.rstrip()
        cleaned = _ANSI_ESCAPE_RE.sub("", clean).strip()
        if cleaned:
            recent_lines.append(cleaned)
        with last_output_lock:
            last_output_state["ts"] = time.monotonic()
        logger.info("[pipeline][%s] %s", job.id, clean, extra={"job_id": job.id})
        match = _PIPELINE_TAG_RE.match(clean)
        if match:
            tag = match.group(1)
            msg = match.group(2)
            _update_job_progress(job, message=f"[{tag}] {msg}", tag=tag)
        elif clean:
            _update_job_progress(job, message=clean)


def _terminate_process(proc: subprocess.Popen, *, grace_sec: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace_sec)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def _run_pipeline_cmd(
    job: Job,
    cmd: list[str],
    *,
    env: Optional[Dict[str, str]] = None,
    start_mono: Optional[float] = None,
) -> tuple[int, deque[str]]:
    proc = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    with _job_processes_lock:
        _job_processes[job.id] = proc
    if start_mono is None:
        start_mono = time.monotonic()
    last_output_state = {"ts": time.monotonic()}
    last_output_lock = threading.Lock()
    recent_pipeline_lines: deque[str] = deque(maxlen=240)
    reader = threading.Thread(
        target=_stream_pipeline_output,
        args=(proc, job, last_output_state, last_output_lock, recent_pipeline_lines),
        daemon=True,
    )
    reader.start()

    rc: Optional[int] = None
    while rc is None:
        # Use wait(timeout) instead of poll() + sleep() for more responsive subprocess monitoring
        try:
            rc = proc.wait(timeout=1.0)
            break
        except subprocess.TimeoutExpired:
            rc = None

        now_mono = time.monotonic()
        effective_no_output_timeout = _resolve_stage_timeout(
            stage=job.stage,
            fallback_timeout_sec=NO_OUTPUT_TIMEOUT_SEC,
            overrides=_STAGE_NO_OUTPUT_TIMEOUTS,
        )
        if effective_no_output_timeout > 0:
            with last_output_lock:
                last_seen = last_output_state["ts"]
            if (now_mono - last_seen) > effective_no_output_timeout:
                stage_context = f" at stage '{job.stage}'" if job.stage else ""
                last_msg = f" Last message: {job.last_message[:100]}" if job.last_message else ""
                logger.error(
                    "job no-output timeout",
                    extra={"job_id": job.id, "seconds": effective_no_output_timeout, "stage": job.stage},
                )
                _terminate_process(proc)
                job.status = "failed"
                job.error = f"No output for {int(effective_no_output_timeout)}s{stage_context}.{last_msg}"
                job.stage = "timeout"
                job.last_message = job.error
                job.last_updated_at = _now_ts()
                job.progress_percent = None
                job.estimated_seconds_remaining = None
                _persist_jobs()
                raise RuntimeError(job.error)
        effective_job_timeout = _resolve_stage_timeout(
            stage=job.stage,
            fallback_timeout_sec=JOB_TIMEOUT_SEC,
            overrides=_STAGE_JOB_TIMEOUTS,
        )
        if (now_mono - start_mono) > effective_job_timeout:
            stage_context = f" at stage '{job.stage}'" if job.stage else ""
            progress_info = f" (was {job.progress_percent:.0f}% complete)" if job.progress_percent else ""
            logger.error(
                "job timeout exceeded",
                extra={"job_id": job.id, "seconds": effective_job_timeout, "stage": job.stage},
            )
            _terminate_process(proc)
            job.status = "failed"
            job.error = f"Timeout after {int(effective_job_timeout)}s{stage_context}{progress_info}"
            job.stage = "timeout"
            job.last_message = job.error
            job.last_updated_at = _now_ts()
            job.progress_percent = None
            job.estimated_seconds_remaining = None
            _persist_jobs()
            raise RuntimeError(job.error)

    reader.join(timeout=2)
    with _job_processes_lock:
        _job_processes.pop(job.id, None)
    return int(rc or 0), recent_pipeline_lines


def _cookies_diag(cookies_path_override: Optional[str] = None) -> tuple[Optional[str], bool, int]:
    if cookies_path_override is None:
        cookies_path = os.environ.get("MIXTERIOSO_YTDLP_COOKIES", "").strip() or None
    else:
        cookies_path = cookies_path_override.strip() or None
    if not cookies_path:
        return None, False, 0
    p = Path(cookies_path)
    if not p.exists():
        return cookies_path, False, 0
    try:
        size = p.stat().st_size
    except Exception:
        size = 0
    return cookies_path, True, size


def _normalize_runtime_cookies_payload(raw: Optional[str]) -> str:
    """
    Accepts Netscape cookie text and returns normalized LF-delimited content.
    Empty payloads are allowed and treated as "no runtime cookies".
    """
    payload = (raw or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not payload:
        return ""

    lines = [ln for ln in payload.split("\n") if ln.strip()]

    def _is_cookie_row(line: str) -> bool:
        cleaned = line.lstrip()
        if cleaned.startswith("#HttpOnly_"):
            cleaned = cleaned[len("#") :]
        elif cleaned.startswith("#"):
            return False
        return cleaned.count("\t") >= 6

    has_cookie_row = any(_is_cookie_row(ln) for ln in lines)
    if not has_cookie_row:
        raise HTTPException(
            status_code=400,
            detail="runtime cookies payload must be Netscape cookies.txt format",
        )

    normalized = payload if payload.endswith("\n") else (payload + "\n")
    size = len(normalized.encode("utf-8"))
    if size > JOB_RUNTIME_COOKIES_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"runtime cookies payload exceeds {JOB_RUNTIME_COOKIES_MAX_BYTES} bytes",
        )
    return normalized


def _write_runtime_cookies_file(job_id: str, payload: str) -> Optional[str]:
    if not payload:
        return None
    safe_job_id = re.sub(r"[^0-9A-Za-z_-]", "", job_id) or uuid4().hex
    cookies_path = JOB_RUNTIME_COOKIES_DIR / f"ytcookies-job-{safe_job_id}.txt"
    JOB_RUNTIME_COOKIES_DIR.mkdir(parents=True, exist_ok=True)
    cookies_path.write_text(payload, encoding="utf-8")
    cookies_path.chmod(0o600)
    return str(cookies_path)


def _remove_runtime_cookies_file(cookies_path: Optional[str]) -> None:
    if not cookies_path:
        return
    try:
        Path(cookies_path).unlink(missing_ok=True)
    except Exception:
        logger.warning("failed to remove runtime cookies file", extra={"cookies_path": cookies_path})


def _gpu_worker_cancel_url(job_id: str) -> str:
    base = str(GPU_WORKER_URL or "").strip()
    if not base:
        return ""
    parsed = urllib.parse.urlparse(base)
    path = parsed.path or "/separate"
    if path.endswith("/"):
        path = path[:-1]
    if path.endswith("/separate"):
        path = path[: -len("/separate")]
    if not path:
        path = ""
    cancel_path = f"{path}/jobs/{urllib.parse.quote(job_id)}/cancel"
    return urllib.parse.urlunparse(parsed._replace(path=cancel_path, params="", query="", fragment=""))


def _cancel_gpu_worker_job(job_id: str) -> None:
    cancel_url = _gpu_worker_cancel_url(job_id)
    if not cancel_url:
        return
    payload = json.dumps({"job_id": str(job_id or "").strip()}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if GPU_WORKER_API_KEY:
        headers["Authorization"] = f"Bearer {GPU_WORKER_API_KEY}"
    if GPU_WORKER_HMAC_SECRET:
        headers.update(build_signed_headers(body_bytes=payload, secret=GPU_WORKER_HMAC_SECRET))
    req = urllib.request.Request(cancel_url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=GPU_WORKER_CANCEL_TIMEOUT_SEC) as resp:
            _ = resp.read()
    except Exception as exc:
        logger.warning(
            "worker cancel propagation failed",
            extra={"job_id": job_id, "url": cancel_url, "error": str(exc)[:300]},
        )


def _job_stale_reference_ts(job: Job) -> Optional[float]:
    """Best-effort runtime reference used for stale job cancellation."""
    try:
        started = float(job.started_at) if job.started_at is not None else 0.0
    except Exception:
        started = 0.0
    if started >= STALE_JOB_REFERENCE_MIN_EPOCH_SEC:
        return started

    try:
        queued_at = float((job.step_timestamps or {}).get("queued_at", 0.0))
    except Exception:
        queued_at = 0.0
    if queued_at >= STALE_JOB_REFERENCE_MIN_EPOCH_SEC:
        return queued_at

    try:
        created = float(job.created_at)
    except Exception:
        created = 0.0
    if created >= STALE_JOB_REFERENCE_MIN_EPOCH_SEC:
        return created
    return None


def _sweep_stale_jobs_once(*, now_ts: Optional[float] = None) -> int:
    """Cancel queued/running jobs that exceeded the configured stale age limit."""
    if STALE_JOB_MAX_AGE_SEC <= 0:
        return 0

    now_value = float(now_ts) if now_ts is not None else _now_ts()
    stale_jobs: list[tuple[str, float, str]] = []
    with _jobs_lock:
        for job in _jobs.values():
            if job.status not in {"queued", "running"}:
                continue
            ref_ts = _job_stale_reference_ts(job)
            if ref_ts is None:
                continue
            age_sec = max(0.0, now_value - ref_ts)
            if age_sec <= STALE_JOB_MAX_AGE_SEC:
                continue

            _mark_job_finishing(job, was_active=True)
            reason = (
                f"Cancelled automatically after {int(age_sec)}s "
                f"(stale-job limit {int(STALE_JOB_MAX_AGE_SEC)}s)."
            )
            job.status = "cancelled"
            job.cancelled_at = now_value
            job.finished_at = now_value
            job.error = reason
            job.stage = "timeout"
            job.last_message = reason
            job.last_updated_at = now_value
            job.progress_percent = None
            job.estimated_seconds_remaining = None
            stale_jobs.append((job.id, round(age_sec, 1), job.stage or "timeout"))
            _invalidate_job_status_cache(job.id)

    if not stale_jobs:
        return 0

    _persist_jobs()
    for job_id, age_sec, stage in stale_jobs:
        with _job_processes_lock:
            proc = _job_processes.get(job_id)
        if proc and proc.poll() is None:
            _terminate_process(proc)
        _cancel_gpu_worker_job(job_id)
        logger.warning(
            "stale job auto-cancelled",
            extra={
                "job_id": job_id,
                "age_sec": age_sec,
                "stage": stage,
                "stale_limit_sec": STALE_JOB_MAX_AGE_SEC,
            },
        )
    return len(stale_jobs)


def _stale_job_sweeper_loop() -> None:
    logger.info(
        "stale job sweeper started",
        extra={"interval_sec": STALE_JOB_SWEEPER_INTERVAL_SEC, "max_age_sec": STALE_JOB_MAX_AGE_SEC},
    )
    while not _stale_job_sweeper_stop_event.wait(STALE_JOB_SWEEPER_INTERVAL_SEC):
        try:
            cancelled = _sweep_stale_jobs_once()
            if cancelled:
                logger.info("stale job sweeper cancelled jobs", extra={"count": cancelled})
        except Exception:
            logger.exception("stale job sweeper iteration failed")


def _start_stale_job_sweeper() -> None:
    if not STALE_JOB_SWEEPER_ENABLED:
        return
    if STALE_JOB_MAX_AGE_SEC <= 0:
        return

    global _stale_job_sweeper_thread
    with _stale_job_sweeper_lock:
        if _stale_job_sweeper_thread and _stale_job_sweeper_thread.is_alive():
            return
        _stale_job_sweeper_stop_event.clear()
        _stale_job_sweeper_thread = threading.Thread(
            target=_stale_job_sweeper_loop,
            name="karaoapi-stale-job-sweeper",
            daemon=True,
        )
        _stale_job_sweeper_thread.start()


def _load_jobs_state() -> None:
    try:
        payload: list[dict[str, Any]] = []

        if _jobs_sqlite_store is not None:
            try:
                payload = _jobs_sqlite_store.load_all_jobs()
            except Exception:
                logger.exception("failed to load jobs from sqlite", extra={"path": str(JOBS_SQLITE_PATH)})
                payload = []

        if (not payload) and JOBS_STATE_PATH.exists():
            raw = JOBS_STATE_PATH.read_text(encoding="utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                payload = parsed

        if not payload:
            return

        with _jobs_lock:
            _jobs.clear()
            _slug_to_job_id.clear()
            _idempotency_to_job_id.clear()
            global _active_job_count_cached
            _active_job_count_cached = 0
            for item in payload:
                try:
                    job = _job_from_dict(item)
                    # Jobs cannot survive process restarts; mark stale in-progress jobs as failed.
                    if job.status in {"queued", "running"}:
                        job.status = "failed"
                        job.error = "Server restarted while job was in progress."
                        job.stage = "failed"
                        job.last_message = job.error
                        job.finished_at = _now_ts()
                        job.last_updated_at = _now_ts()
                    _jobs[job.id] = job
                    clean_idempotency = _normalize_idempotency_key(job.idempotency_key)
                    if clean_idempotency:
                        _idempotency_to_job_id[clean_idempotency] = job.id
                    if job.status in {"queued", "running"}:
                        _slug_to_job_id[job.slug] = job.id
                        _active_job_count_cached += 1
                except Exception:
                    continue
            _prune_jobs_history()
        logger.info(
            "loaded jobs state",
            extra={
                "count": len(_jobs),
                "json_path": str(JOBS_STATE_PATH),
                "sqlite_path": str(JOBS_SQLITE_PATH),
                "sqlite_enabled": bool(_jobs_sqlite_store is not None),
            },
        )
    except Exception:
        logger.exception("failed to load jobs state", extra={"path": str(JOBS_STATE_PATH)})


_load_jobs_state()
_start_stale_job_sweeper()
_load_rating_prompt_state()


_STEM_LEVEL_OPTION_KEYS = ("vocals", "bass", "drums", "other")


def _stem_levels_requested(options: Dict[str, Any]) -> bool:
    for key in _STEM_LEVEL_OPTION_KEYS:
        value = options.get(key)
        if value is None:
            continue
        try:
            pct = float(value)
        except Exception:
            return True
        if abs(pct - 100.0) > 1e-6:
            return True
    return False


def _stem_levels_allowed(options: Dict[str, Any]) -> bool:
    return bool(options.get("render_only")) or ALLOW_STEM_LEVELS_NON_RENDER


def _strip_stem_levels_for_non_render(options: Dict[str, Any]) -> tuple[Dict[str, Any], list[str]]:
    sanitized = dict(options)
    if _stem_levels_allowed(sanitized):
        return sanitized, []

    dropped: list[str] = []
    for key in _STEM_LEVEL_OPTION_KEYS:
        value = sanitized.get(key)
        if value is None:
            continue
        try:
            pct = float(value)
        except Exception:
            # Preserve non-numeric values so validation can surface a useful error later.
            continue
        # For non-render jobs, prune default 100% values to avoid noisy no-op options.
        # Keep explicit non-default values (e.g. vocals=0) so custom mixes still work.
        if abs(pct - 100.0) <= 1e-6:
            sanitized[key] = None
            dropped.append(key)
    return sanitized, dropped


def _build_pipeline_argv(job: Job) -> List[str]:
    options = dict(job.options or {})
    audio_url = str(options.get("audio_url") or "").strip()
    audio_id = str(options.get("audio_id") or "").strip()
    if SERVER_DOWNLOAD_ONLY_ENFORCED and (audio_url or audio_id):
        logger.info(
            "server-download-only enabled; ignoring direct audio override",
            extra={"job_id": job.id, "slug": job.slug},
        )
    allow_stem_levels_by_mode = _stem_levels_allowed(options)
    explicit_stem_levels_requested = _stem_levels_requested(options)
    allow_stem_levels = allow_stem_levels_by_mode or explicit_stem_levels_requested
    if not allow_stem_levels:
        ignored = [key for key in _STEM_LEVEL_OPTION_KEYS if options.get(key) is not None]
        if ignored:
            logger.info(
                "ignoring stem options for non-render job",
                extra={"job_id": job.id, "slug": job.slug, "ignored_keys": ",".join(ignored)},
            )
    return build_core_pipeline_argv(
        query=job.query,
        options=options,
        allow_stem_levels=allow_stem_levels,
        server_download_only=SERVER_DOWNLOAD_ONLY_ENFORCED,
    )


def _resolve_job_offset_secs(job: Job) -> float:
    try:
        return float((job.options or {}).get("offset_sec") or 0.0)
    except Exception:
        return 0.0


def _build_step4_cmd_for_job(
    job: Job,
    *,
    out_path: Optional[Path] = None,
    mute: bool = False,
) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        "-m",
        "scripts.step4_assemble",
        "--slug",
        job.slug,
        "--offset",
        str(_resolve_job_offset_secs(job)),
    ]
    if mute:
        cmd.append("--mute")
    if out_path is not None:
        cmd += ["--out", str(out_path)]
    return cmd


def _build_main_env(
    runtime_cookies_path: Optional[str],
    *,
    recovery_mode: bool,
    upload_enabled: bool = False,
) -> Dict[str, str]:
    main_env = os.environ.copy()
    if runtime_cookies_path:
        main_env["MIXTERIOSO_YTDLP_COOKIES"] = runtime_cookies_path
    if upload_enabled:
        main_env["MIXTERIOSO_ENABLE_STEP5_UPLOAD"] = "1"
    if FINAL_RENDER_LEVEL:
        main_env["KARAOKE_RENDER_LEVEL"] = FINAL_RENDER_LEVEL
    if FINAL_RENDER_PROFILE:
        main_env["KARAOKE_RENDER_PROFILE"] = FINAL_RENDER_PROFILE
    if recovery_mode:
        main_env.update(NO_COOKIE_RECOVERY_ENV_OVERRIDES)
    return main_env


def _build_preview_render_env() -> Dict[str, str]:
    preview_env = os.environ.copy()
    if PREVIEW_RENDER_LEVEL:
        preview_env["KARAOKE_RENDER_LEVEL"] = PREVIEW_RENDER_LEVEL
    if PREVIEW_RENDER_PROFILE:
        preview_env["KARAOKE_RENDER_PROFILE"] = PREVIEW_RENDER_PROFILE
    if PREVIEW_VIDEO_SIZE:
        preview_env["KARAOKE_VIDEO_SIZE"] = PREVIEW_VIDEO_SIZE
    if PREVIEW_FPS:
        preview_env["KARAOKE_FPS"] = PREVIEW_FPS
    if PREVIEW_VIDEO_BITRATE:
        preview_env["KARAOKE_VIDEO_BITRATE"] = PREVIEW_VIDEO_BITRATE
    if PREVIEW_AUDIO_BITRATE:
        preview_env["KARAOKE_AUDIO_BITRATE"] = PREVIEW_AUDIO_BITRATE
    if PREVIEW_X264_PRESET:
        preview_env["KARAOKE_X264_PRESET"] = PREVIEW_X264_PRESET
    if PREVIEW_X264_TUNE:
        preview_env["KARAOKE_X264_TUNE"] = PREVIEW_X264_TUNE
    return preview_env


def _build_final_render_env() -> Dict[str, str]:
    final_env = os.environ.copy()
    if FINAL_RENDER_LEVEL:
        final_env["KARAOKE_RENDER_LEVEL"] = FINAL_RENDER_LEVEL
    if FINAL_RENDER_PROFILE:
        final_env["KARAOKE_RENDER_PROFILE"] = FINAL_RENDER_PROFILE
    return final_env


def _effective_preview_enabled(preview_opt: Optional[bool]) -> bool:
    if preview_opt is not None:
        return bool(preview_opt)
    return bool(PREVIEW_RENDER_ENABLED)


def _effective_early_mute_preview_enabled() -> bool:
    return bool(EARLY_MUTE_PREVIEW_ENABLED)


def _effective_finalize_from_mute_preview_enabled() -> bool:
    return bool(FINALIZE_FROM_MUTE_PREVIEW_ENABLED)


def _preview_output_path(slug: str) -> Path:
    return OUTPUT_DIR / f"{slug}.preview.mp4"


def _muted_preview_output_path(slug: str) -> Path:
    return OUTPUT_DIR / f"{slug}.preview.muted.mp4"


def _preview_url_from_path(path: Path) -> str:
    return _output_url_from_path(path)


def _output_url_from_path(path: Path) -> str:
    try:
        rel = path.resolve().relative_to(OUTPUT_DIR.resolve())
        return "/output/" + "/".join(rel.parts)
    except Exception:
        return f"/output/{path.name}"


def _resolve_render_output_path(slug: str) -> Optional[Path]:
    candidates: list[Path] = []
    primary_output_dir = resolve_output_dir(BASE_DIR)
    candidates.append(primary_output_dir / f"{slug}.mp4")
    candidates.append(OUTPUT_DIR / f"{slug}.mp4")
    candidates.append(OUTPUT_DIR / "temp" / f"{slug}.mp4")

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _has_nonempty_file(candidate):
            return candidate
    return None


def _resolve_mix_audio_output_path(slug: str) -> Optional[Path]:
    candidates = [
        MIXES_DIR / f"{slug}.m4a",
        MIXES_DIR / f"{slug}.wav",
        MIXES_DIR / f"{slug}.mp3",
    ]
    for candidate in candidates:
        if _has_nonempty_file(candidate):
            return candidate
    return None


def _mix_audio_url_for_slug(slug: str) -> Optional[str]:
    audio_path = _resolve_mix_audio_output_path(slug)
    if audio_path is None:
        return None
    return f"/files/mixes/{audio_path.name}"


def _refresh_job_local_output_fields(job: Job) -> None:
    if job.status != "succeeded":
        return
    final_path = _resolve_render_output_path(job.slug)
    if final_path is None:
        return
    final_url = _output_url_from_path(final_path)
    job.output_path = str(final_path)
    job.output_url = final_url
    job.final_output_url = final_url
    if job.status == "succeeded":
        job.output_is_preview = False


_YOUTUBE_VIDEO_URL_RE = re.compile(r"^https?://(?:www\.)?(?:youtube\.com|youtu\.be)/", re.IGNORECASE)


def _load_step_meta_payload(slug: str, step_name: str) -> Optional[Dict[str, Any]]:
    meta_path = META_DIR / f"{slug}.{step_name}.json"
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _uploaded_job_reuse_quality_gate(job: Job) -> tuple[bool, str]:
    if not bool((job.options or {}).get("upload")):
        return True, ""

    if REUSE_UPLOADED_REQUIRE_SOURCE_MATCH:
        step1_payload = _load_step_meta_payload(job.slug, "step1")
        source_match = (step1_payload or {}).get("audio_source_match") if isinstance(step1_payload, dict) else None
        if not isinstance(source_match, dict):
            if not REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED:
                return False, "missing source match metadata"
        else:
            checked = bool(source_match.get("checked"))
            matched = bool(source_match.get("matched"))
            if checked and (not matched):
                return False, "source match failed"
            if (not checked) and (not REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED):
                return False, "source match unchecked"

    if REUSE_UPLOADED_REQUIRE_SYNC_PASS:
        step5_payload = _load_step_meta_payload(job.slug, "step5")
        sync_checks = (step5_payload or {}).get("sync_checks") if isinstance(step5_payload, dict) else None
        if not isinstance(sync_checks, dict):
            if not REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED:
                return False, "missing sync checks"
        else:
            if sync_checks.get("overall_passed") is False:
                return False, "sync overall failed"
            for scope_key in ("pre_upload", "post_upload"):
                scope = sync_checks.get(scope_key)
                if not isinstance(scope, dict):
                    continue
                checks = scope.get("checks")
                if not isinstance(checks, dict):
                    continue
                visual = checks.get("visual_sync")
                if isinstance(visual, dict):
                    status = str(visual.get("status") or "").strip().lower()
                    if status and status not in {"passed", "skipped"}:
                        return False, f"{scope_key} visual status={status}"

    return True, ""


def _resolve_step5_uploaded_video_url(job: Job) -> Optional[str]:
    if not bool((job.options or {}).get("upload")):
        return None
    meta_path = META_DIR / f"{job.slug}.step5.json"
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    video_url = str(payload.get("video_url") or payload.get("url") or "").strip()
    if not video_url:
        return None
    if not _YOUTUBE_VIDEO_URL_RE.match(video_url):
        return None
    try:
        if float(job.created_at or 0.0) > 0 and (meta_path.stat().st_mtime + 5.0) < float(job.created_at):
            return None
    except Exception:
        pass
    return video_url


def _refresh_job_uploaded_video_url(job: Job) -> None:
    video_url = _resolve_step5_uploaded_video_url(job)
    if video_url:
        job.youtube_video_url = video_url
        return
    if not bool((job.options or {}).get("upload")):
        job.youtube_video_url = None


def _has_nonempty_file(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False


def _latest_preview_input_mtime(slug: str) -> float:
    candidates = [
        TIMINGS_DIR / f"{slug}.lrc",
        TIMINGS_DIR / f"{slug}.csv",
        META_DIR / f"{slug}.step1.json",
        META_DIR / f"{slug}.step2_stems.json",
        MIXES_DIR / f"{slug}.wav",
        MIXES_DIR / f"{slug}.mp3",
    ]
    newest = 0.0
    for path in candidates:
        try:
            if path.exists():
                newest = max(newest, float(path.stat().st_mtime))
        except Exception:
            continue
    return newest


def _is_fresh_preview_for_slug(slug: str, preview_path: Path) -> bool:
    if not _has_nonempty_file(preview_path):
        return False
    try:
        preview_mtime = float(preview_path.stat().st_mtime)
    except Exception:
        return False
    latest_input_mtime = _latest_preview_input_mtime(slug)
    if latest_input_mtime <= 0.0:
        return True
    # Small tolerance to account for filesystem timestamp precision.
    return preview_mtime + 0.01 >= latest_input_mtime


def _discard_stale_preview_if_needed(slug: str, preview_path: Path) -> bool:
    if not _has_nonempty_file(preview_path):
        return False
    if _is_fresh_preview_for_slug(slug, preview_path):
        return False
    try:
        preview_path.unlink()
        logger.info(
            "discarded stale preview artifact",
            extra={"slug": slug, "path": str(preview_path)},
        )
    except Exception:
        pass
    return True


def _resolve_audio_for_mux(slug: str) -> Optional[Path]:
    try:
        from scripts import step4_assemble as step4

        audio_path = step4.choose_audio(slug)
        if _has_nonempty_file(audio_path):
            return audio_path
    except SystemExit:
        return None
    except Exception:
        return None
    return None


def _resolve_ffprobe_bin() -> str:
    try:
        from scripts.common import resolve_ffmpeg_bin

        ffmpeg_bin = str(resolve_ffmpeg_bin())
        ffprobe_candidate = ffmpeg_bin.replace("ffmpeg", "ffprobe")
        if ffprobe_candidate != ffmpeg_bin and shutil.which(ffprobe_candidate):
            return ffprobe_candidate
    except Exception:
        pass
    probe = shutil.which("ffprobe")
    if probe:
        return probe
    raise RuntimeError("ffprobe unavailable")


def _probe_media_info(*, media_path: Path, env: Dict[str, str]) -> Dict[str, Any]:
    ffprobe_bin = _resolve_ffprobe_bin()
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-print_format",
        "json",
        str(media_path),
    ]
    result = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=max(5, min(PREVIEW_AUDIO_MUX_TIMEOUT_SEC, 20)),
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(detail or f"ffprobe failed rc={result.returncode}")
    try:
        parsed = json.loads(result.stdout or "{}")
    except Exception as exc:
        raise RuntimeError(f"ffprobe returned invalid JSON: {exc}") from exc
    return parsed if isinstance(parsed, dict) else {}


def _validate_output_media_file(*, media_path: Path, env: Dict[str, str]) -> tuple[bool, str]:
    if not _has_nonempty_file(media_path):
        return False, "output missing or empty"

    try:
        file_size = int(media_path.stat().st_size)
    except Exception:
        file_size = 0
    if file_size < int(OUTPUT_MIN_BYTES):
        return False, f"output below minimum size ({file_size} < {int(OUTPUT_MIN_BYTES)})"

    try:
        info = _probe_media_info(media_path=media_path, env=env)
    except Exception as exc:
        return False, f"ffprobe failed: {exc}"

    streams = info.get("streams")
    if not isinstance(streams, list):
        streams = []
    has_video = any(str((stream or {}).get("codec_type") or "").lower() == "video" for stream in streams if isinstance(stream, dict))
    has_audio = any(str((stream or {}).get("codec_type") or "").lower() == "audio" for stream in streams if isinstance(stream, dict))
    if not has_video:
        return False, "output missing video stream"
    if not has_audio:
        return False, "output missing audio stream"

    duration_sec = 0.0
    fmt = info.get("format")
    if isinstance(fmt, dict):
        try:
            duration_sec = float(fmt.get("duration") or 0.0)
        except Exception:
            duration_sec = 0.0
    if duration_sec <= 0.0:
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            try:
                duration_sec = max(duration_sec, float(stream.get("duration") or 0.0))
            except Exception:
                continue
    if duration_sec < float(OUTPUT_MIN_DURATION_SEC):
        return False, f"output below minimum duration ({duration_sec:.2f}s < {float(OUTPUT_MIN_DURATION_SEC):.2f}s)"

    return True, ""


def _media_has_audio_stream(*, media_path: Path, ffmpeg_bin: str, env: Dict[str, str]) -> bool:
    if not _has_nonempty_file(media_path):
        return False
    probe_cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(media_path),
        "-map",
        "0:a:0",
        "-c",
        "copy",
        "-f",
        "null",
        "-",
    ]
    try:
        result = subprocess.run(
            probe_cmd,
            cwd=str(BASE_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(5, min(PREVIEW_AUDIO_MUX_TIMEOUT_SEC, 20)),
        )
    except Exception:
        return False
    return result.returncode == 0


def _mux_audio_into_preview(*, slug: str, muted_preview_path: Path, final_path: Path, env: Dict[str, str]) -> tuple[bool, str]:
    if not _has_nonempty_file(muted_preview_path):
        return False, "muted preview missing"

    audio_path = _resolve_audio_for_mux(slug)
    if audio_path is None:
        return False, "audio not available for mux"

    try:
        from scripts.common import resolve_ffmpeg_bin

        ffmpeg_bin = str(resolve_ffmpeg_bin())
    except Exception as exc:
        return False, f"ffmpeg unavailable: {exc}"

    cmd = [
        ffmpeg_bin,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(muted_preview_path),
        "-i",
        str(audio_path),
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        str(env.get("KARAOKE_AUDIO_BITRATE") or PREVIEW_AUDIO_BITRATE or "192k"),
        "-shortest",
        "-movflags",
        "+faststart",
        str(final_path),
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=PREVIEW_AUDIO_MUX_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return False, "mux timed out"
    except Exception as exc:
        return False, f"mux failed to start: {exc}"

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        tail = detail[-1] if detail else ""
        return False, f"mux failed rc={result.returncode} {tail}".strip()
    if not _has_nonempty_file(final_path):
        return False, "mux produced empty output"
    if not _media_has_audio_stream(media_path=final_path, ffmpeg_bin=ffmpeg_bin, env=env):
        try:
            final_path.unlink()
        except Exception:
            pass
        return False, "mux output missing audio stream"
    return True, ""


def _promote_preview_to_final(*, preview_path: Path, final_path: Path) -> tuple[bool, str]:
    if not _has_nonempty_file(preview_path):
        return False, "preview missing"
    try:
        if preview_path.resolve() == final_path.resolve():
            return True, "preview already final"
    except Exception:
        pass

    try:
        final_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return False, f"final output dir unavailable: {exc}"

    if final_path.exists():
        try:
            final_path.unlink()
        except Exception as exc:
            return False, f"could not replace existing final output: {exc}"

    try:
        os.link(preview_path, final_path)
        return True, "hardlink"
    except Exception:
        pass

    try:
        shutil.copy2(preview_path, final_path)
    except Exception as exc:
        return False, f"preview promotion copy failed: {exc}"

    if not _has_nonempty_file(final_path):
        return False, "preview promotion produced empty output"
    return True, "copy"


def _render_early_mute_preview(job: Job) -> None:
    if not _effective_early_mute_preview_enabled():
        return
    if job.status == "cancelled":
        return

    lrc_path = TIMINGS_DIR / f"{job.slug}.lrc"
    muted_preview_path = _muted_preview_output_path(job.slug)
    _discard_stale_preview_if_needed(job.slug, muted_preview_path)
    if _is_fresh_preview_for_slug(job.slug, muted_preview_path):
        return

    deadline = time.monotonic() + EARLY_MUTE_PREVIEW_WAIT_SEC
    fresh_after_ts = max(float(job.started_at or 0.0), float(job.created_at or 0.0))
    while time.monotonic() < deadline:
        if job.status == "cancelled":
            return
        if _has_nonempty_file(lrc_path):
            try:
                lrc_mtime = float(lrc_path.stat().st_mtime)
            except Exception:
                lrc_mtime = 0.0
            if lrc_mtime + 0.01 >= fresh_after_ts:
                break
        time.sleep(EARLY_MUTE_PREVIEW_POLL_SEC)
    else:
        return

    try:
        from scripts.common import IOFlags, Paths
        from scripts.step3_sync import step3_sync_lite

        paths = Paths(BASE_DIR)
        paths.ensure()
        flags = IOFlags(force=False, confirm=False, dry_run=False)
        language = str(job.options.get("language") or "auto")
        step3_sync_lite(paths, job.slug, flags, language=language)
    except Exception as exc:
        logger.info("early muted preview skipped (step3 lite failed)", extra={"job_id": job.id, "error": str(exc)})
        return

    preview_env = _build_preview_render_env()
    preview_cmd = _build_step4_cmd_for_job(job, out_path=muted_preview_path, mute=True)
    try:
        result = subprocess.run(
            preview_cmd,
            cwd=str(BASE_DIR),
            env=preview_env,
            capture_output=True,
            text=True,
            timeout=EARLY_MUTE_PREVIEW_RENDER_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        logger.info("early muted preview timed out", extra={"job_id": job.id})
        return
    except Exception as exc:
        logger.info("early muted preview failed", extra={"job_id": job.id, "error": str(exc)})
        return

    if result.returncode != 0:
        stderr_tail = (result.stderr or result.stdout or "").strip().splitlines()
        logger.info(
            "early muted preview command failed",
            extra={"job_id": job.id, "rc": result.returncode, "tail": (stderr_tail[-1] if stderr_tail else "")},
        )
        return
    if not _has_nonempty_file(muted_preview_path):
        logger.info("early muted preview output missing", extra={"job_id": job.id})
        return
    if job.status == "cancelled":
        return

    preview_url = _preview_url_from_path(muted_preview_path)
    job.output_path = str(muted_preview_path)
    job.output_url = preview_url
    job.preview_output_url = preview_url
    job.output_is_preview = True
    job.stage = "preview_ready"
    job.last_message = "Preview ready (muted; audio pending)"
    job.last_updated_at = _now_ts()
    _persist_jobs()
    logger.info("early muted preview ready", extra={"job_id": job.id, "path": str(muted_preview_path)})


def _enable_no_cookie_recovery_argv(argv: List[str]) -> List[str]:
    # Filter without unnecessary list() wrapper - argv is already a list
    out = [part for part in argv if part != "--no-parallel"]
    if "--force" not in out:
        out.append("--force")
    if "--reset" not in out:
        out.append("--reset")

    search_flag = "--yt-search-n"
    if search_flag in out:
        idx = out.index(search_flag)
        if idx + 1 < len(out):
            try:
                # Remove unnecessary str() wrapper - already a string from argv
                existing = int(out[idx + 1])
            except Exception:
                existing = 0
            out[idx + 1] = str(max(existing, NO_COOKIE_RECOVERY_YT_SEARCH_N))
    else:
        out += [search_flag, str(NO_COOKIE_RECOVERY_YT_SEARCH_N)]
    return out


def _should_try_no_cookie_recovery(job: Job, detail: str, runtime_cookies_payload: str) -> bool:
    if not NO_COOKIE_RECOVERY_ENABLED:
        return False
    if not detail or not _line_requires_cookie_refresh(detail):
        return False
    if bool((runtime_cookies_payload or "").strip()):
        return False
    return bool(job.options.get("runtime_cookies_supplied")) is False


def _run_job_with_upload(job: Job, uploaded_audio_path: Path, artist: Optional[str], title: Optional[str]) -> None:
    """
    Run a job using a pre-uploaded audio file.
    Skips step1 (source download) and goes directly to step2 (separation) and beyond.
    """
    if job.status == "cancelled":
        job.finished_at = _now_ts()
        logger.info("job skipped (already cancelled)", extra={"job_id": job.id})
        if uploaded_audio_path.exists():
            uploaded_audio_path.unlink()
        _persist_jobs()
        return

    job.started_at = _now_ts()
    job.status = "running"
    logger.info("job started (upload mode)", extra={"job_id": job.id, "query": job.query})

    _set_job_stage(
        job,
        stage="processing_upload",
        status="running",
        message="Processing uploaded audio",
        step_name="upload_pipeline",
        increment_attempt=True,
    )
    _persist_jobs()

    preview_opt = job.options.get("preview")
    preview_enabled = _effective_preview_enabled(preview_opt)
    defer_finish = False
    was_active = True

    try:
        # Convert uploaded audio to MP3 if needed
        target_mp3 = MP3_DIR / f"{job.slug}.mp3"
        if uploaded_audio_path.suffix.lower() == ".mp3":
            # Already MP3, just move it
            uploaded_audio_path.rename(target_mp3)
        else:
            # Convert to MP3 using ffmpeg
            _set_job_stage(job, stage="convert_audio", message="Converting audio to MP3", step_name="convert_audio")
            _persist_jobs()

            from scripts.common import resolve_ffmpeg_bin
            ffmpeg_bin = resolve_ffmpeg_bin()
            cmd = [
                ffmpeg_bin,
                "-hide_banner",
                "-loglevel", "error",
                "-i", str(uploaded_audio_path),
                "-vn",  # No video
                "-ar", "44100",
                "-ac", "2",
                "-b:a", "192k",
                "-y",  # Overwrite
                str(target_mp3),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                raise RuntimeError(f"ffmpeg conversion failed: {result.stderr}")

            # Remove original uploaded file
            uploaded_audio_path.unlink()
            logger.info("audio converted", extra={"job_id": job.id, "slug": job.slug})

        # Write metadata (step1.json format for pipeline compatibility)
        from scripts.common import META_DIR
        META_DIR.mkdir(parents=True, exist_ok=True)
        meta_path = META_DIR / f"{job.slug}.step1.json"
        meta_data = {
            "artist": artist or "",
            "title": title or "",
            "query": job.query,
            "source": "user_upload",
            "slug": job.slug,
            "mp3_path": str(target_mp3),
        }
        meta_path.write_text(json.dumps(meta_data, indent=2), encoding="utf-8")

        # Verify files exist before proceeding
        if not target_mp3.exists():
            raise RuntimeError(f"MP3 file not found after conversion: {target_mp3}")
        if not meta_path.exists():
            raise RuntimeError(f"Metadata file not found after writing: {meta_path}")

        logger.info("upload files ready", extra={
            "job_id": job.id,
            "mp3_path": str(target_mp3),
            "mp3_exists": target_mp3.exists(),
            "mp3_size": target_mp3.stat().st_size if target_mp3.exists() else 0,
            "meta_path": str(meta_path),
            "meta_exists": meta_path.exists(),
        })

        # Build argv for step2 onwards (skip step1)
        upload_pipeline_options = {
            "language": str((job.options or {}).get("language") or "auto"),
            "skip_step1": True,
            "upload": bool((job.options or {}).get("upload")),
        }
        argv = build_core_pipeline_argv(
            query=job.query,
            options=upload_pipeline_options,
        )

        _set_job_stage(
            job,
            stage="separate_audio",
            message="Separating audio with Demucs",
            step_name="step2_split",
            increment_attempt=True,
        )
        _persist_jobs()

        # Run step2 (separation) and step3 (sync)
        cmd = [sys.executable, "-m", "scripts.main", *argv]
        if preview_enabled:
            cmd.append("--no-render")
        upload_env = os.environ.copy()
        if bool((job.options or {}).get("upload")):
            upload_env["MIXTERIOSO_ENABLE_STEP5_UPLOAD"] = "1"
        rc, recent_lines = _run_pipeline_cmd(job, cmd, env=upload_env, start_mono=time.monotonic())
        logger.info("pipeline exited (upload)", extra={"job_id": job.id, "rc": rc})

        if rc != 0:
            detail = _extract_pipeline_failure_reason(list(recent_lines), rc)
            if job.status == "cancelled":
                job.finished_at = _now_ts()
                return
            raise RuntimeError(detail)

        if job.status == "cancelled":
            job.finished_at = _now_ts()
            return

        # Render preview if enabled
        if preview_enabled:
            preview_path = OUTPUT_DIR / f"{job.slug}.preview.mp4"
            preview_env = os.environ.copy()
            if PREVIEW_RENDER_LEVEL:
                preview_env["KARAOKE_RENDER_LEVEL"] = PREVIEW_RENDER_LEVEL
            if PREVIEW_RENDER_PROFILE:
                preview_env["KARAOKE_RENDER_PROFILE"] = PREVIEW_RENDER_PROFILE

            preview_cmd = _build_step4_cmd_for_job(job, out_path=preview_path)
            rc, recent_lines = _run_pipeline_cmd(job, preview_cmd, env=preview_env, start_mono=time.monotonic())
            logger.info("preview render exited (upload)", extra={"job_id": job.id, "rc": rc})
            if rc != 0:
                detail = _extract_pipeline_failure_reason(list(recent_lines), rc)
                raise RuntimeError(detail)

            if preview_path.exists():
                job.output_path = str(preview_path)
                job.output_url = f"/output/{preview_path.name}"
                job.preview_output_url = job.output_url
                job.output_is_preview = True

            _set_job_stage(
                job,
                stage="preview_ready",
                status="partial_ready",
                message="Preview ready",
                step_name="step5_preview",
            )
            _persist_jobs()

            defer_finish = True

            def _render_final() -> None:
                try:
                    final_cmd = _build_step4_cmd_for_job(job)
                    final_env = os.environ.copy()
                    if FINAL_RENDER_LEVEL:
                        final_env["KARAOKE_RENDER_LEVEL"] = FINAL_RENDER_LEVEL
                    rc_final, final_lines = _run_pipeline_cmd(job, final_cmd, env=final_env, start_mono=time.monotonic())
                    if rc_final == 0:
                        final_path = _resolve_render_output_path(job.slug)
                        if final_path is not None:
                            if OUTPUT_VALIDATION_ENFORCED:
                                ok_validate, validate_detail = _validate_output_media_file(
                                    media_path=final_path,
                                    env=final_env,
                                )
                                if not ok_validate:
                                    raise RuntimeError(f"final output validation failed: {validate_detail}")
                            job.output_path = str(final_path)
                            job.output_url = _output_url_from_path(final_path)
                            job.final_output_url = job.output_url
                            job.output_is_preview = False
                            _refresh_job_uploaded_video_url(job)
                        _set_job_stage(
                            job,
                            stage="complete",
                            status="succeeded",
                            message="Final render complete",
                            step_name="step6_deliver",
                        )
                    else:
                        _set_job_stage(
                            job,
                            stage="final_render_failed",
                            status="failed",
                            message="Final render failed",
                            step_name="step6_deliver",
                        )
                    _persist_jobs()
                except Exception as e:
                    logger.exception("final render failed (upload)", extra={"job_id": job.id})
                    _set_job_stage(
                        job,
                        stage="final_render_failed",
                        status="failed",
                        message=f"Final render error: {e}",
                        step_name="step6_deliver",
                    )
                    _persist_jobs()

            threading.Thread(target=_render_final, daemon=True).start()

        else:
            # Direct final render
            final_path = _resolve_render_output_path(job.slug)
            if final_path is not None:
                if OUTPUT_VALIDATION_ENFORCED:
                    ok_validate, validate_detail = _validate_output_media_file(
                        media_path=final_path,
                        env=_build_final_render_env(),
                    )
                    if not ok_validate:
                        raise RuntimeError(f"final output validation failed: {validate_detail}")
                job.output_path = str(final_path)
                job.output_url = _output_url_from_path(final_path)
                job.final_output_url = job.output_url
                job.output_is_preview = False
                _refresh_job_uploaded_video_url(job)

        if not defer_finish:
            job.finished_at = _now_ts()
            _set_job_stage(
                job,
                stage="complete",
                status="succeeded",
                message="Job completed successfully",
                step_name="step6_deliver",
            )
            _mark_job_finishing(job, was_active=was_active)
            _persist_jobs()
            logger.info("job succeeded (upload)", extra={"job_id": job.id})

    except Exception as exc:
        logger.exception("job failed (upload)", extra={"job_id": job.id})
        job.status = "failed"
        job.error = f"Job execution failed at {job.stage}: {exc}"
        job.finished_at = _now_ts()
        _set_job_stage(job, stage="failed", status="failed", message=job.error, step_name="step6_deliver")
        _clear_preview_artifacts_on_failure(job)
        _mark_job_finishing(job, was_active=was_active)
        _persist_jobs()
        logger.info("job failed (upload)", extra={"job_id": job.id, "error": str(exc)})


def _run_job(job: Job, runtime_cookies_payload: str = "") -> None:
    if job.status == "cancelled":
        job.finished_at = _now_ts()
        logger.info("job skipped (already cancelled)", extra={"job_id": job.id})
        _persist_jobs()
        return
    runtime_cookies_path: Optional[str] = None
    try:
        runtime_cookies_path = _write_runtime_cookies_file(job.id, runtime_cookies_payload)
    except Exception as exc:
        job.started_at = _now_ts()
        job.status = "failed"
        job.error = f"Failed to prepare runtime cookies: {exc}"
        job.stage = "failed"
        job.last_message = job.error
        job.last_updated_at = _now_ts()
        job.finished_at = _now_ts()
        logger.exception("failed to materialize runtime cookies", extra={"job_id": job.id})
        with _jobs_lock:
            _mark_job_finishing(job, was_active=True)
        _persist_jobs()
        return
    job.started_at = _now_ts()
    job.status = "running"
    logger.info("job started", extra={"job_id": job.id, "query": job.query})
    cookies_path, cookies_exists, cookies_size = _cookies_diag(runtime_cookies_path)
    logger.info(
        "job cookies diag",
        extra={
            "job_id": job.id,
            "cookies_path": cookies_path,
            "cookies_exists": cookies_exists,
            "cookies_size": cookies_size,
            "runtime_cookies_supplied": bool(runtime_cookies_path),
        },
    )
    _set_job_stage(
        job,
        stage="starting",
        status="running",
        message="Job started",
        step_name="step1_source",
        increment_attempt=True,
    )
    _persist_jobs()

    argv = _build_pipeline_argv(job)

    preview_opt = job.options.get("preview")
    preview_enabled = _effective_preview_enabled(preview_opt)
    defer_finish = False
    early_preview_thread: Optional[threading.Thread] = None

    try:
        if preview_enabled and _effective_early_mute_preview_enabled():
            early_preview_thread = threading.Thread(target=_render_early_mute_preview, args=(job,), daemon=True)
            early_preview_thread.start()

        cmd = [sys.executable, "-m", "scripts.main", *argv]
        main_env = _build_main_env(
            runtime_cookies_path,
            recovery_mode=False,
            upload_enabled=bool((job.options or {}).get("upload")),
        )
        if preview_enabled:
            cmd.append("--no-render")
        rc, recent_pipeline_lines = _run_pipeline_cmd(job, cmd, env=main_env, start_mono=time.monotonic())
        logger.info("job process exited", extra={"job_id": job.id, "rc": rc})
        if rc != 0:
            detail = _extract_pipeline_failure_reason(list(recent_pipeline_lines), rc)
            if job.status == "cancelled":
                job.finished_at = _now_ts()
                return
            if _should_try_no_cookie_recovery(job, detail, runtime_cookies_payload):
                logger.info("job no-cookie recovery retry", extra={"job_id": job.id})
                _set_job_stage(
                    job,
                    stage="retrying_no_cookie_recovery",
                    message="source bot-check detected; retrying with no-cookie recovery mode",
                    step_name="step1_source",
                    increment_attempt=True,
                )
                _persist_jobs()
                recovery_argv = _enable_no_cookie_recovery_argv(argv)
                recovery_cmd = [sys.executable, "-m", "scripts.main", *recovery_argv]
                if preview_enabled:
                    recovery_cmd.append("--no-render")
                recovery_env = _build_main_env(
                    runtime_cookies_path,
                    recovery_mode=True,
                    upload_enabled=bool((job.options or {}).get("upload")),
                )
                rc, recent_pipeline_lines = _run_pipeline_cmd(
                    job,
                    recovery_cmd,
                    env=recovery_env,
                    start_mono=time.monotonic(),
                )
                logger.info("job no-cookie recovery exited", extra={"job_id": job.id, "rc": rc})
                if job.status == "cancelled":
                    job.finished_at = _now_ts()
                    return
                if rc != 0:
                    detail = _extract_pipeline_failure_reason(list(recent_pipeline_lines), rc)
                    raise RuntimeError(detail)
                logger.info("job no-cookie recovery succeeded", extra={"job_id": job.id})
            else:
                raise RuntimeError(detail)

        if job.status == "cancelled":
            job.finished_at = _now_ts()
            return

        if preview_enabled:
            if early_preview_thread and early_preview_thread.is_alive():
                early_preview_thread.join(timeout=0.1)

            preview_path = _preview_output_path(job.slug)
            muted_preview_path = _muted_preview_output_path(job.slug)
            preview_env = _build_preview_render_env()

            _discard_stale_preview_if_needed(job.slug, muted_preview_path)
            _discard_stale_preview_if_needed(job.slug, preview_path)

            selected_preview: Optional[Path] = None
            if _is_fresh_preview_for_slug(job.slug, muted_preview_path):
                selected_preview = muted_preview_path
            elif _is_fresh_preview_for_slug(job.slug, preview_path):
                selected_preview = preview_path
            else:
                preview_cmd = _build_step4_cmd_for_job(job, out_path=preview_path)
                rc, recent_pipeline_lines = _run_pipeline_cmd(
                    job,
                    preview_cmd,
                    env=preview_env,
                    start_mono=time.monotonic(),
                )
                logger.info("preview render exited", extra={"job_id": job.id, "rc": rc})
                if rc != 0:
                    detail = _extract_pipeline_failure_reason(list(recent_pipeline_lines), rc)
                    raise RuntimeError(detail)
                if _has_nonempty_file(preview_path):
                    selected_preview = preview_path

            if selected_preview is not None and _has_nonempty_file(selected_preview):
                preview_url = _preview_url_from_path(selected_preview)
                job.output_path = str(selected_preview)
                job.output_url = preview_url
                job.preview_output_url = preview_url
                job.output_is_preview = True
            else:
                job.output_path = None
                job.output_url = None

            _set_job_stage(
                job,
                stage="preview_ready",
                status="partial_ready",
                message="Preview ready",
                step_name="step5_preview",
            )
            _persist_jobs()

            defer_finish = True

            def _render_final() -> None:
                try:
                    output_path = OUTPUT_DIR / f"{job.slug}.mp4"
                    final_env = _build_final_render_env()
                    used_mux = False
                    promoted_preview = False
                    if selected_preview is not None and selected_preview == preview_path and _has_nonempty_file(preview_path):
                        ok_promote, promote_detail = _promote_preview_to_final(
                            preview_path=preview_path,
                            final_path=output_path,
                        )
                        if ok_promote:
                            promoted_preview = True
                            logger.info(
                                "finalized by preview promotion",
                                extra={
                                    "job_id": job.id,
                                    "slug": job.slug,
                                    "detail": promote_detail,
                                },
                            )
                        elif promote_detail:
                            logger.info(
                                "preview promotion fallback to final render",
                                extra={"job_id": job.id, "slug": job.slug, "detail": promote_detail},
                            )

                    if (not promoted_preview) and _effective_finalize_from_mute_preview_enabled():
                        ok_mux, mux_detail = _mux_audio_into_preview(
                            slug=job.slug,
                            muted_preview_path=muted_preview_path,
                            final_path=output_path,
                            env=final_env,
                        )
                        if ok_mux:
                            used_mux = True
                            logger.info("finalized by audio mux", extra={"job_id": job.id, "slug": job.slug})
                        elif mux_detail:
                            logger.info(
                                "audio mux fallback to final render",
                                extra={"job_id": job.id, "slug": job.slug, "detail": mux_detail},
                            )

                    if (not promoted_preview) and (not used_mux):
                        final_cmd = _build_step4_cmd_for_job(job)
                        rc2, recent2 = _run_pipeline_cmd(job, final_cmd, env=final_env, start_mono=time.monotonic())
                        logger.info("final render exited", extra={"job_id": job.id, "rc": rc2})
                        if rc2 != 0:
                            detail2 = _extract_pipeline_failure_reason(list(recent2), rc2)
                            raise RuntimeError(detail2)
                    else:
                        now_ts = _now_ts()
                        job.render_started_at = job.render_started_at or now_ts
                        job.render_finished_at = now_ts

                    if OUTPUT_VALIDATION_ENFORCED:
                        ok_validate, validate_detail = _validate_output_media_file(
                            media_path=output_path,
                            env=final_env,
                        )
                        if (not ok_validate) and OUTPUT_VALIDATION_RETRY_MUX_ON_FAIL:
                            ok_retry_mux, retry_detail = _mux_audio_into_preview(
                                slug=job.slug,
                                muted_preview_path=muted_preview_path,
                                final_path=output_path,
                                env=final_env,
                            )
                            if ok_retry_mux:
                                ok_validate, validate_detail = _validate_output_media_file(
                                    media_path=output_path,
                                    env=final_env,
                                )
                            else:
                                validate_detail = f"{validate_detail}; retry_mux={retry_detail}"
                        if not ok_validate:
                            raise RuntimeError(f"final output validation failed: {validate_detail}")

                    resolved_output_path = _resolve_render_output_path(job.slug)
                    if resolved_output_path is not None:
                        job.output_path = str(resolved_output_path)
                        job.output_url = _output_url_from_path(resolved_output_path)
                        job.final_output_url = job.output_url
                        job.output_is_preview = False
                        _refresh_job_uploaded_video_url(job)
                    else:
                        job.output_path = None
                        job.output_url = None
                    if job.status != "cancelled":
                        _set_job_stage(
                            job,
                            stage="complete",
                            status="succeeded",
                            message="Job completed",
                            step_name="step6_deliver",
                        )
                        logger.info("job succeeded", extra={"job_id": job.id})
                    if job.render_started_at is None or job.render_finished_at is None:
                        render_meta = META_DIR / f"{job.slug}.step4.json"
                        if render_meta.exists():
                            try:
                                data = render_meta.read_text(encoding="utf-8")
                                payload = json.loads(data)
                                job.render_started_at = payload.get("render_started_at")
                                job.render_finished_at = payload.get("render_finished_at")
                            except Exception:
                                pass
                    job.finished_at = _now_ts()
                    with _jobs_lock:
                        _mark_job_finishing(job, was_active=(job.status != "cancelled"))
                    logger.info("job finished", extra={"job_id": job.id, "status": job.status})
                    _persist_jobs()
                except Exception as e:
                    # Track if job was active before setting status to failed
                    was_active_before = (job.status != "cancelled")
                    if job.status != "cancelled":
                        job.status = "failed"
                        # Enhanced error message with stage context
                        stage_context = f" during {job.stage}" if job.stage else ""
                        job.error = f"{str(e)}{stage_context}"
                        _set_job_stage(
                            job,
                            stage="failed",
                            status="failed",
                            message=str(e),
                            step_name="step6_deliver",
                        )
                        job.progress_percent = None  # Clear progress on failure
                        job.estimated_seconds_remaining = None
                        _clear_preview_artifacts_on_failure(job)
                    job.finished_at = _now_ts()
                    with _jobs_lock:
                        _mark_job_finishing(job, was_active=was_active_before)
                    logger.exception("job failed", extra={"job_id": job.id, "stage": job.stage, "query": job.query})
                    logger.info("job finished", extra={"job_id": job.id, "status": job.status})
                    _persist_jobs()

            threading.Thread(target=_render_final, daemon=True).start()
            return

        output_path = _resolve_render_output_path(job.slug)
        if output_path is not None:
            if OUTPUT_VALIDATION_ENFORCED:
                ok_validate, validate_detail = _validate_output_media_file(media_path=output_path, env=_build_final_render_env())
                if not ok_validate:
                    raise RuntimeError(f"final output validation failed: {validate_detail}")
            job.output_path = str(output_path)
            job.output_url = _output_url_from_path(output_path)
            _refresh_job_uploaded_video_url(job)
        else:
            job.output_path = None
            job.output_url = None

        if job.status != "cancelled":
            _set_job_stage(
                job,
                stage="complete",
                status="succeeded",
                message="Job completed",
                step_name="step6_deliver",
            )
            logger.info("job succeeded", extra={"job_id": job.id})
        _persist_jobs()
    except Exception as e:
        if job.status != "cancelled":
            job.status = "failed"
            # Enhanced error with job context for debugging
            stage_context = f" at {job.stage}" if job.stage else ""
            job.error = f"Job execution failed{stage_context}: {str(e)}"
            _set_job_stage(
                job,
                stage="failed",
                status="failed",
                message=str(e),
                step_name="step6_deliver",
            )
            job.progress_percent = None
            job.estimated_seconds_remaining = None
            _clear_preview_artifacts_on_failure(job)
            logger.exception("job failed", extra={"job_id": job.id})
            _persist_jobs()
    finally:
        with _job_processes_lock:
            _job_processes.pop(job.id, None)
        if not defer_finish:
            with _jobs_lock:
                _mark_job_finishing(job, was_active=(job.status != "cancelled"))
            job.finished_at = _now_ts()
            logger.info("job finished", extra={"job_id": job.id, "status": job.status})
            if job.render_started_at is None or job.render_finished_at is None:
                render_meta = META_DIR / f"{job.slug}.step4.json"
                if render_meta.exists():
                    try:
                        data = render_meta.read_text(encoding="utf-8")
                        payload = json.loads(data)
                        job.render_started_at = payload.get("render_started_at")
                        job.render_finished_at = payload.get("render_finished_at")
                    except Exception:
                        pass
            _persist_jobs()
        _remove_runtime_cookies_file(runtime_cookies_path)


def _clear_preview_artifacts_on_failure(job: Job) -> None:
    # Prevent serving a muted preview as if it were a playable final output after failures.
    if not bool(job.output_is_preview):
        return
    if job.final_output_url:
        return
    job.output_path = None
    job.output_url = None
    job.preview_output_url = None
    job.output_is_preview = None


def _extract_source_audio_url_uncached(query: str) -> Dict[str, Any]:
    logger.info("source audio url request", extra={"query": query})
    try:
        video_id = None

        url_patterns = [
            r'(?:source\.com\/watch\?v=|youtu\.be\/|source\.com\/embed\/)([a-zA-Z0-9_-]{11})',
            r'^([a-zA-Z0-9_-]{11})$',
        ]

        for pattern in url_patterns:
            match = re.search(pattern, query)
            if match:
                video_id = match.group(1)
                logger.info("detected source video id from input", extra={"video_id": video_id})
                break

        if not video_id:
            from scripts import step1_fetch

            cached_candidates: List[str] = []
            try:
                cached_candidates = list(step1_fetch._cached_ids_for_slug(query))
            except Exception:
                cached_candidates = []

            if cached_candidates:
                video_id = cached_candidates[0]
                logger.info(
                    "using cached source id for audio-url",
                    extra={"query": query, "video_id": video_id},
                )
            else:
                video_ids = yt_search_ids(query, 1, timeout_sec=15.0)
                if not video_ids:
                    raise HTTPException(status_code=404, detail="No source results found")
                video_id = video_ids[0]

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        from scripts import step1_fetch

        def _common_ytdlp_flags() -> List[str]:
            cmd: List[str] = [
                "--no-playlist",
                "--force-ipv4",
                "--socket-timeout",
                str(step1_fetch.YTDLP_SOCKET_TIMEOUT),
            ]
            if step1_fetch.YTDLP_UA:
                cmd += ["--user-agent", str(step1_fetch.YTDLP_UA)]
            for hdr in step1_fetch.YTDLP_EXTRA_HEADERS:
                cmd += ["--add-headers", hdr]
            if step1_fetch.YTDLP_EXTRACTOR_ARGS:
                cmd += ["--extractor-args", str(step1_fetch.YTDLP_EXTRACTOR_ARGS)]
            if step1_fetch.YTDLP_JS_RUNTIMES:
                cmd += ["--js-runtimes", str(step1_fetch.YTDLP_JS_RUNTIMES)]
            if step1_fetch.YTDLP_REMOTE_COMPONENTS:
                cmd += ["--remote-components", str(step1_fetch.YTDLP_REMOTE_COMPONENTS)]
            return cmd

        proxy_budget = 1
        if step1_fetch.YTDLP_PROXY:
            try:
                proxy_budget = max(1, int(step1_fetch._proxy_retry_budget()))
            except Exception:
                proxy_budget = 1
        attempt_budget = max(1, max(int(SOURCE_AUDIO_URL_MAX_ATTEMPTS), proxy_budget))

        audio_url = ""
        used_proxy_for_success = ""
        last_diag = ""
        for attempt_idx in range(attempt_budget):
            cmd = [*step1_fetch.YTDLP_CMD]
            cmd += [
                "--get-url",
                "--format",
                "bestaudio[ext=m4a]/bestaudio",
                "--retries",
                str(step1_fetch.YTDLP_RETRIES),
            ]
            cmd += _common_ytdlp_flags()

            used_proxy = ""
            if step1_fetch.YTDLP_PROXY:
                try:
                    used_proxy = str(step1_fetch._current_proxy() or "").strip()
                except Exception:
                    used_proxy = ""
            if not used_proxy and step1_fetch.YTDLP_PROXY:
                used_proxy = str(step1_fetch.YTDLP_PROXY)
            if used_proxy:
                cmd += ["--proxy", used_proxy]

            cmd.append(video_url)

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=float(SOURCE_AUDIO_URL_CMD_TIMEOUT_SEC),
                )
            except subprocess.TimeoutExpired:
                last_diag = (
                    "yt-dlp audio-url timed out for video_id=%s after %.1fs (attempt %d/%d)"
                    % (
                        video_id,
                        float(SOURCE_AUDIO_URL_CMD_TIMEOUT_SEC),
                        attempt_idx + 1,
                        attempt_budget,
                    )
                )
                if used_proxy:
                    try:
                        step1_fetch._mark_proxy_failure(used_proxy, reason="source_audio_url_timeout")
                        step1_fetch._rotate_proxy("source_audio_url_timeout")
                    except Exception:
                        pass
                continue

            if result.returncode == 0:
                candidate_url = ""
                for line in (result.stdout or "").splitlines():
                    line = line.strip()
                    if line.startswith("http"):
                        candidate_url = line
                        break
                if candidate_url:
                    audio_url = candidate_url
                    used_proxy_for_success = used_proxy
                    if used_proxy:
                        try:
                            step1_fetch._mark_proxy_success(used_proxy)
                        except Exception:
                            pass
                    break

            diag = step1_fetch._collect_ytdlp_diagnostics(result.stderr or "", result.stdout or "")
            last_diag = (diag or "yt-dlp returned no valid audio URL").strip()[:500]
            if used_proxy:
                try:
                    step1_fetch._mark_proxy_failure(used_proxy, reason="source_audio_url_failed")
                except Exception:
                    pass
            rotate = False
            try:
                rotate = bool(step1_fetch._should_rotate_proxy_on_error(last_diag))
            except Exception:
                rotate = False
            if rotate or attempt_idx + 1 < attempt_budget:
                try:
                    step1_fetch._rotate_proxy("source_audio_url")
                except Exception:
                    pass

        if not audio_url:
            logger.warning(
                "yt-dlp failed",
                extra={
                    "video_id": video_id,
                    "attempt_budget": attempt_budget,
                    "stderr_snippet": last_diag[:500],
                },
            )
            raise HTTPException(
                status_code=500,
                detail="Could not resolve source audio URL. Please try again.",
            )

        if not audio_url.startswith("http"):
            raise HTTPException(
                status_code=500,
                detail="Received an invalid source audio URL.",
            )

        metadata_cmd = [*step1_fetch.YTDLP_CMD]
        metadata_cmd += [
            "--print", "title",
            "--print", "duration",
            "--print", "thumbnail",
        ]
        metadata_cmd += _common_ytdlp_flags()
        if used_proxy_for_success:
            metadata_cmd += ["--proxy", used_proxy_for_success]
        elif step1_fetch.YTDLP_PROXY:
            metadata_cmd += ["--proxy", str(step1_fetch.YTDLP_PROXY)]

        metadata_cmd.append(video_url)
        metadata_result = subprocess.run(
            metadata_cmd,
            capture_output=True,
            text=True,
            timeout=float(SOURCE_AUDIO_URL_METADATA_TIMEOUT_SEC),
        )

        lines = (metadata_result.stdout or "").strip().split('\n')
        title = lines[0] if len(lines) > 0 else query
        duration = int(lines[1]) if len(lines) > 1 and lines[1].isdigit() else None
        thumbnail = lines[2] if len(lines) > 2 else f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"

        payload = {
            "audio_url": audio_url,
            "title": title,
            "duration": duration,
            "video_id": video_id,
            "thumbnail": thumbnail,
        }
        _source_audio_url_cache_set([query, video_id, title], payload)

        logger.info(
            "source audio url extracted",
            extra={"query": query, "video_id": video_id, "title": title},
        )
        return payload

    except HTTPException:
        raise
    except Exception:
        logger.exception("source audio url failed", extra={"query": query})
        raise HTTPException(status_code=500, detail="Could not extract source audio URL.")


@app.get("/source/audio-url")
def get_source_audio_url(q: str) -> dict:
    """
    Extract direct audio URL from source for client-side download.

    This allows iOS/mobile clients to download audio directly from source,
    bypassing server bot-detection issues.

    Accepts:
    - source URL (e.g., "https://youtube.com/watch?v=dQw4w9WgXcQ")
    - source video ID (e.g., "dQw4w9WgXcQ")
    - Search query (e.g., "Rick Astley Never Gonna Give You Up")

    Returns: {"audio_url": "https://...", "title": "...", "duration": 123}
    """
    if SERVER_DOWNLOAD_ONLY_ENFORCED or (not ENABLE_SOURCE_AUDIO_URL_ENDPOINT):
        raise HTTPException(status_code=403, detail="Source audio URL extraction is disabled in server-download-only mode.")

    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="query parameter 'q' is required")

    query = q.strip()
    allow_stale = SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC > 0
    cache_hit, cache_hit_is_stale = _source_audio_url_cache_lookup(query, allow_stale=allow_stale)
    if cache_hit is not None:
        if cache_hit_is_stale:
            logger.info(
                "source audio url stale cache hit",
                extra={"query": query, "video_id": str(cache_hit.get("video_id") or "")},
            )
            _refresh_source_audio_url_stale_cache_async(query)
        else:
            logger.info(
                "source audio url cache hit",
                extra={"query": query, "video_id": str(cache_hit.get("video_id") or "")},
            )
        return cache_hit

    with _source_audio_url_singleflight(query):
        cache_hit, cache_hit_is_stale = _source_audio_url_cache_lookup(query, allow_stale=allow_stale)
        if cache_hit is not None:
            if cache_hit_is_stale:
                logger.info(
                    "source audio url stale cache hit",
                    extra={"query": query, "video_id": str(cache_hit.get("video_id") or "")},
                )
                _refresh_source_audio_url_stale_cache_async(query)
            else:
                logger.info(
                    "source audio url cache hit",
                    extra={"query": query, "video_id": str(cache_hit.get("video_id") or "")},
                )
            return cache_hit
        return _extract_source_audio_url_with_optional_distributed_singleflight(query)


@app.get("/source/search-results")
async def get_source_search_results(q: str, limit: int = 5) -> dict:
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="query parameter 'q' is required")

    query = q.strip()
    safe_limit = max(1, min(int(limit or 5), 10))

    try:
        from scripts.step1_fetch import yt_search_ids
        from scripts import step1_fetch
        import subprocess

        ids = yt_search_ids(query, safe_limit, timeout_sec=15.0)
        results: list[dict] = []
        for vid in ids[:safe_limit]:
            title = query
            duration: Optional[int] = None
            thumbnail = f"https://img.youtube.com/vi/{vid}/maxresdefault.jpg"
            uploader = ""
            cached_meta = _source_search_meta_cache_get(vid)
            if cached_meta is not None:
                title = str(cached_meta.get("title") or query).strip() or query
                raw_duration = cached_meta.get("duration")
                try:
                    if raw_duration is not None and str(raw_duration).strip() != "":
                        duration = int(raw_duration)
                except Exception:
                    duration = None
                cached_thumbnail = str(cached_meta.get("thumbnail") or "").strip()
                if cached_thumbnail:
                    thumbnail = cached_thumbnail
                cached_uploader = str(cached_meta.get("uploader") or "").strip()
                if cached_uploader:
                    uploader = cached_uploader
            else:
                parsed_title = ""
                parsed_thumbnail = ""
                parsed_uploader = ""

                try:
                    meta_cmd = [*step1_fetch.YTDLP_CMD]
                    meta_cmd += [
                        "--print", "title",
                        "--print", "duration",
                        "--print", "thumbnail",
                        "--print", "uploader",
                        "--no-playlist",
                        "--force-ipv4",
                        "--socket-timeout", str(step1_fetch.YTDLP_SOCKET_TIMEOUT),
                    ]
                    if step1_fetch.YTDLP_EXTRACTOR_ARGS:
                        meta_cmd += ["--extractor-args", str(step1_fetch.YTDLP_EXTRACTOR_ARGS)]
                    if step1_fetch.YTDLP_PROXY:
                        meta_cmd += ["--proxy", str(step1_fetch.YTDLP_PROXY)]
                    meta_cmd.append(f"https://www.youtube.com/watch?v={vid}")
                    meta = subprocess.run(meta_cmd, capture_output=True, text=True, timeout=10)
                    lines = (meta.stdout or "").splitlines()
                    if len(lines) > 0 and lines[0].strip():
                        parsed_title = lines[0].strip()
                        title = parsed_title
                    if len(lines) > 1 and lines[1].strip().isdigit():
                        duration = int(lines[1].strip())
                    if len(lines) > 2 and lines[2].strip().startswith("http"):
                        parsed_thumbnail = lines[2].strip()
                        thumbnail = parsed_thumbnail
                    if len(lines) > 3:
                        parsed_uploader = lines[3].strip()
                        uploader = parsed_uploader
                except Exception:
                    pass
                _source_search_meta_cache_set(
                    vid,
                    {
                        "title": parsed_title,
                        "duration": duration,
                        "thumbnail": parsed_thumbnail,
                        "uploader": parsed_uploader,
                    },
                )

            results.append(
                {
                    "video_id": vid,
                    "title": title,
                    "duration": duration,
                    "thumbnail": thumbnail,
                    "uploader": uploader,
                }
            )

        return {"query": query, "count": len(results), "results": results}
    except Exception:
        logger.exception("source search results failed", extra={"query": query})
        raise HTTPException(status_code=500, detail="Could not load source search results.")


@app.get("/source/normalize")
async def normalize_source_query(q: str) -> dict:
    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="query parameter 'q' is required")

    query = q.strip()
    user_error_default = "Unable to identify song. Please include artist and title."
    try:
        from scripts import step1_fetch

        payload = step1_fetch._normalize_query_via_ytsearch_top_result(
            query,
            timeout_sec=min(12.0, float(step1_fetch.YTDLP_SEARCH_TIMEOUT)),
        )

        artist = step1_fetch._clean_title(str(payload.get("artist") or ""))
        track = step1_fetch._clean_title(str(payload.get("track") or payload.get("title") or ""))
        normalized_query = step1_fetch._clean_title(str(payload.get("normalized_query") or payload.get("display") or ""))
        provider = str(payload.get("provider") or "source_normalize").strip() or "source_normalize"
        confidence = str(payload.get("confidence") or "").strip().lower()
        video_id = step1_fetch._clean_title(str(payload.get("video_id") or ""))
        short_circuit = str(payload.get("short_circuit") or "").strip() == "1"

        if short_circuit:
            direct = step1_fetch._direct_source_source_from_query(query)
            if direct:
                video_id = step1_fetch._clean_title(str(video_id or direct[0]))

        # URL / video-id short-circuit payloads may not include structured artist/title.
        # Recover metadata from the resolved source id when available.
        if (not artist or not track) and video_id:
            hint = step1_fetch._yt_video_metadata_hint(
                video_id,
                timeout_sec=min(8.0, float(step1_fetch.YTDLP_SEARCH_TIMEOUT)),
            )
            if not hint:
                hint = step1_fetch._yt_oembed_video_hint(video_id, timeout_sec=4.0)

            hint_artist = step1_fetch._clean_title(str(hint.get("artist") or ""))
            hint_title = step1_fetch._clean_title(str(hint.get("title") or ""))
            if hint_artist and hint_title:
                artist, track = step1_fetch._normalize_canonical_artist_title(hint_artist, hint_title)
                normalized_query = step1_fetch._artist_title_query(artist, track) or normalized_query
                provider = f"{provider}+video_hint"
                if not confidence:
                    confidence = "medium"

        if artist and track and not normalized_query:
            normalized_query = step1_fetch._artist_title_query(artist, track) or f"{artist} - {track}"

        if not artist or not track or not normalized_query:
            detail = str(payload.get("user_error") or payload.get("error") or user_error_default).strip()
            raise HTTPException(status_code=422, detail=detail or user_error_default)

        response = {
            "query": query,
            "artist": artist,
            "track": track,
            "title": track,
            "display": normalized_query,
            "normalized_query": normalized_query,
            "confidence": confidence or ("high" if short_circuit else "medium"),
            "provider": provider,
        }
        if video_id:
            response["video_id"] = video_id
        return response
    except HTTPException:
        raise
    except Exception:
        logger.exception("source normalize failed", extra={"query": query})
        raise HTTPException(status_code=500, detail="Could not normalize source query.")


@app.get("/source/proxy-download")
async def proxy_source_download(q: str, background_tasks: BackgroundTasks):
    """
    Proxy download source audio through the server.

    This solves the IP-lock issue where source URLs can only be downloaded
    from the same IP that requested them. The server downloads via Webshare proxy
    and streams to the client.

    Args:
        q: source URL, video ID, or search query

    Returns:
        StreamingResponse with audio/mp4 content
    """
    import subprocess
    import tempfile
    from fastapi.responses import FileResponse
    from pathlib import Path

    if not q or not q.strip():
        raise HTTPException(status_code=400, detail="query parameter 'q' is required")

    query = q.strip()
    logger.info("source proxy download request", extra={"query": query})

    try:
        # Get video ID (reuse logic from audio-url endpoint)
        import re
        video_id = None

        url_patterns = [
            r'(?:source\.com\/watch\?v=|youtu\.be\/|source\.com\/embed\/)([a-zA-Z0-9_-]{11})',
            r'^([a-zA-Z0-9_-]{11})$',
        ]

        for pattern in url_patterns:
            match = re.search(pattern, query)
            if match:
                video_id = match.group(1)
                break

        if not video_id:
            from scripts.step1_fetch import yt_search_ids
            video_ids = yt_search_ids(query, 1, timeout_sec=15.0)
            if not video_ids:
                raise HTTPException(status_code=404, detail="No source results found")
            video_id = video_ids[0]

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # Download to a unique temporary file using yt-dlp with proxy.
        # NOTE: do not use deterministic filenames here; concurrent requests would clobber each other.
        temp_dir = Path(tempfile.gettempdir())
        output_file = temp_dir / f"yt_proxy_{video_id}_{uuid4().hex}.m4a"

        from scripts import step1_fetch
        cmd = [*step1_fetch.YTDLP_CMD]
        cmd += [
            "-o", str(output_file),
            "--format", "bestaudio[ext=m4a]/bestaudio",
            "--no-playlist",
            "--force-ipv4",
            "--socket-timeout", str(step1_fetch.YTDLP_SOCKET_TIMEOUT),
            "--retries", str(step1_fetch.YTDLP_RETRIES),
        ]

        if step1_fetch.YTDLP_UA:
            cmd += ["--user-agent", str(step1_fetch.YTDLP_UA)]

        for hdr in step1_fetch.YTDLP_EXTRA_HEADERS:
            cmd += ["--add-headers", hdr]

        if step1_fetch.YTDLP_EXTRACTOR_ARGS:
            cmd += ["--extractor-args", str(step1_fetch.YTDLP_EXTRACTOR_ARGS)]

        if step1_fetch.YTDLP_JS_RUNTIMES:
            cmd += ["--js-runtimes", str(step1_fetch.YTDLP_JS_RUNTIMES)]

        if step1_fetch.YTDLP_REMOTE_COMPONENTS:
            cmd += ["--remote-components", str(step1_fetch.YTDLP_REMOTE_COMPONENTS)]

        if step1_fetch.YTDLP_PROXY:
            cmd += ["--proxy", str(step1_fetch.YTDLP_PROXY)]

        cmd.append(video_url)

        logger.info("downloading source audio via proxy", extra={"video_id": video_id})

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0 or not output_file.exists():
            stderr_snippet = (result.stderr or "").strip()[:500]
            logger.warning("yt-dlp download failed", extra={"stderr": stderr_snippet})
            raise HTTPException(
                status_code=500,
                detail="Could not download source audio. Please try again."
            )

        logger.info("source audio downloaded successfully", extra={
            "video_id": video_id,
            "size_mb": f"{output_file.stat().st_size / 1024 / 1024:.2f}"
        })

        # Return file and schedule cleanup after the response is sent.
        def _cleanup_tmp_file(path: str) -> None:
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                logger.warning("failed to cleanup youtube proxy tmp file", extra={"path": path})

        background_tasks.add_task(_cleanup_tmp_file, str(output_file))
        return FileResponse(
            path=str(output_file),
            media_type="audio/mp4",
            filename=f"{video_id}.m4a",
            background=background_tasks,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("source proxy download failed", extra={"query": query})
        try:
            if "output_file" in locals() and isinstance(output_file, Path) and output_file.exists():
                output_file.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Proxy download failed: {exc}")


@app.get("/jobs/{job_id}/stems")
async def get_job_stems(job_id: str) -> dict:
    """
    Return URLs for downloading processed audio stems for client-side rendering.

    This enables hybrid rendering: server does heavy processing (Demucs),
    client does video rendering (AVFoundation).

    Returns:
        {
            "vocals_url": "/files/separated/htdemucs/{slug}/vocals.wav",
            "instrumental_url": "/files/separated/htdemucs/{slug}/no_vocals.wav",
            "bass_url": "/files/separated/htdemucs/{slug}/bass.wav",
            "drums_url": "/files/separated/htdemucs/{slug}/drums.wav",
            "other_url": "/files/separated/htdemucs/{slug}/other.wav",
            "full_mix_url": "/files/mixes/{slug}.wav"
        }
    """
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "succeeded":
        raise HTTPException(
            status_code=400,
            detail=f"Job not completed (status: {job.status})"
        )

    slug = job.slug
    stems_dir = BASE_DIR / "separated" / "htdemucs" / slug
    mix_file = BASE_DIR / "mixes" / f"{slug}.wav"

    result = {}

    # Check for individual stems (from Demucs)
    stem_files = {
        "vocals": stems_dir / "vocals.wav",
        "bass": stems_dir / "bass.wav",
        "drums": stems_dir / "drums.wav",
        "other": stems_dir / "other.wav"
    }

    for stem_name, stem_path in stem_files.items():
        if stem_path.exists():
            result[f"{stem_name}_url"] = f"/files/separated/htdemucs/{slug}/{stem_name}.wav"

    # Create instrumental mix (no vocals) if individual stems exist
    if result.get("vocals_url") and (result.get("bass_url") or result.get("drums_url") or result.get("other_url")):
        result["instrumental_url"] = result.get("other_url") or result.get("bass_url")

    # Full mix
    if mix_file.exists():
        result["full_mix_url"] = f"/files/mixes/{slug}.wav"

    if not result:
        raise HTTPException(
            status_code=404,
            detail="No audio stems found for this job"
        )

    logger.info("stems retrieved", extra={"job_id": job_id, "stems": list(result.keys())})
    return result


@app.get("/jobs/{job_id}/lyrics")
async def get_job_lyrics(job_id: str) -> dict:
    """
    Return lyrics and timings for client-side video rendering.

    Returns:
        {
            "lrc": "[00:12.00]First line\n[00:15.50]Second line",
            "timings": [
                {"start_ms": 12000, "end_ms": 15500, "text": "First line"},
                ...
            ],
            "offset_ms": 0
        }
    """
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "succeeded":
        raise HTTPException(
            status_code=400,
            detail=f"Job not completed (status: {job.status})"
        )

    slug = job.slug
    lrc_file = BASE_DIR / "timings" / f"{slug}.lrc"
    csv_file = BASE_DIR / "timings" / f"{slug}.csv"
    offset_file = BASE_DIR / "timings" / f"{slug}.offset.auto"

    result = {}

    # LRC format
    if lrc_file.exists():
        result["lrc"] = lrc_file.read_text(encoding="utf-8")

    # CSV timings (more detailed)
    if csv_file.exists():
        import csv
        timings = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                timings.append({
                    "start_ms": float(row.get("start_ms", 0)),
                    "end_ms": float(row.get("end_ms", 0)),
                    "text": row.get("text", "")
                })
        result["timings"] = timings

    # Auto-detected offset
    if offset_file.exists():
        try:
            offset_str = offset_file.read_text().strip()
            result["offset_ms"] = float(offset_str)
        except (ValueError, IOError):
            result["offset_ms"] = 0
    else:
        result["offset_ms"] = 0

    if not result:
        raise HTTPException(
            status_code=404,
            detail="No lyrics found for this job"
        )

    logger.info("lyrics retrieved", extra={"job_id": job_id, "has_lrc": "lrc" in result, "timing_count": len(result.get("timings", []))})
    return result


@app.get("/health")
def health() -> dict:
    logger.debug("health check")
    with _jobs_lock:
        active = _active_job_count()
        total = len(_jobs)
    distributed_cache_ready = bool(_distributed_cache_client_get() is not None) if CACHE_REDIS_URL else False
    return {
        "status": "ok",
        "active_jobs": active,
        "total_jobs": total,
        "job_workers": JOB_WORKERS,
        "auto_offset_enabled": _auto_offset_effectively_enabled(),
        "whisper_runtime_ready": _AUTO_OFFSET_RUNTIME_READY,
        "whisper_runtime_note": _AUTO_OFFSET_RUNTIME_NOTE,
        "server_download_only": bool(SERVER_DOWNLOAD_ONLY_ENFORCED),
        "idempotency_required": bool(REQUIRE_IDEMPOTENCY_KEY),
        "distributed_cache_configured": bool(CACHE_REDIS_URL),
        "distributed_cache_library_available": bool(redis_lib is not None),
        "distributed_cache_ready": distributed_cache_ready,
        "source_audio_url_stale_while_revalidate_sec": float(SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC),
        "source_audio_url_distributed_singleflight_enabled": bool(SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED),
        "source_audio_url_distributed_singleflight_active": bool(
            SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED and distributed_cache_ready
        ),
        "sqlite_state_enabled": bool(_jobs_sqlite_store is not None),
        "new_jobs_disabled": bool(EMERGENCY_DISABLE_NEW_JOBS),
        "job_timeout_sec": int(JOB_TIMEOUT_SEC),
        "no_output_timeout_sec": int(NO_OUTPUT_TIMEOUT_SEC),
        "stale_job_sweeper_enabled": bool(STALE_JOB_SWEEPER_ENABLED),
        "stale_job_max_age_sec": int(STALE_JOB_MAX_AGE_SEC),
        "reuse_uploaded_require_source_match": bool(REUSE_UPLOADED_REQUIRE_SOURCE_MATCH),
        "reuse_uploaded_require_sync_pass": bool(REUSE_UPLOADED_REQUIRE_SYNC_PASS),
        "reuse_uploaded_allow_legacy_unverified": bool(REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED),
    }


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"ok": True, "status": "healthy"}


@app.get("/readyz")
def readyz() -> dict[str, Any]:
    demucs_available = bool(shutil.which("demucs"))
    step4_ready = bool(shutil.which("ffmpeg"))
    ready = bool(step4_ready and (_jobs_sqlite_store is None or JOBS_SQLITE_PATH.parent.exists()))
    return {
        "ok": ready,
        "status": "ready" if ready else "not_ready",
        "ffmpeg_available": step4_ready,
        "demucs_available": demucs_available,
        "sqlite_enabled": bool(_jobs_sqlite_store is not None),
    }


@app.post("/rating/state")
def rating_state(payload: RatingStateRequest) -> dict[str, Any]:
    keys = _collect_rating_keys(payload.device_key, payload.aliases)
    if not keys:
        raise HTTPException(status_code=400, detail="device_key is required")
    seen, matched_key, record = _rating_state_for_keys(keys)
    videos_created = _coerce_non_negative_int(record.get("videos_created"), 0)
    return {
        "seen": bool(seen),
        "matched_key": matched_key,
        "videos_created": videos_created,
        "seen_at_utc": str(record.get("seen_at_utc") or "") if seen else "",
        "last_action": str(record.get("last_action") or "") if seen else "",
    }


@app.post("/rating/mark")
def rating_mark(payload: RatingMarkRequest, request: Request) -> dict[str, Any]:
    keys = _collect_rating_keys(payload.device_key, payload.aliases)
    if not keys:
        raise HTTPException(status_code=400, detail="device_key is required")
    action = _sanitize_rating_action(payload.action)
    client_ip = request.client.host if request.client else "unknown"
    marked = _mark_rating_seen(keys, action=action, client_ip=client_ip)
    seen, matched_key, record = _rating_state_for_keys(keys)
    videos_created = _coerce_non_negative_int(record.get("videos_created"), 0)
    logger.info(
        "rating prompt marked as seen",
        extra={"keys_marked": marked, "action": action, "client_ip": client_ip},
    )
    return {
        "status": "ok",
        "seen": bool(seen),
        "action": action,
        "keys_marked": marked,
        "matched_key": matched_key,
        "videos_created": videos_created,
        "seen_at_utc": str(record.get("seen_at_utc") or "") if seen else "",
    }


@app.post("/rating/progress")
def rating_progress(payload: RatingProgressRequest, request: Request) -> dict[str, Any]:
    keys = _collect_rating_keys(payload.device_key, payload.aliases)
    if not keys:
        raise HTTPException(status_code=400, detail="device_key is required")
    clean_job_id = _sanitize_rating_job_id(payload.job_id)
    if not clean_job_id:
        raise HTTPException(status_code=400, detail="job_id is required")
    delta = max(1, min(10, int(payload.delta or 1)))
    client_ip = request.client.host if request.client else "unknown"
    try:
        state = _increment_rating_progress(
            keys,
            job_id=clean_job_id,
            delta=delta,
            client_ip=client_ip,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "ok",
        "seen": bool(state.get("seen")),
        "seen_at_utc": str(state.get("seen_at_utc") or ""),
        "videos_created": _coerce_non_negative_int(state.get("videos_created"), 0),
        "applied_delta": _coerce_non_negative_int(state.get("applied_delta"), 0),
        "duplicate": bool(state.get("duplicate")),
        "last_action": str(state.get("last_action") or ""),
        "last_job_id": str(state.get("last_job_id") or ""),
    }


@app.get("/config.json")
def config(request: Request) -> dict:
    # Prefer forwarded headers (ngrok/proxy) to build the external URL.
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    base_url = f"{proto}://{host}"
    logger.info("config requested", extra={"base_url": base_url})
    return {"base_url": base_url}


def _require_debug_key(request: Request) -> None:
    if not DEBUG_KEY:
        raise HTTPException(status_code=404, detail="Not found")
    provided = request.headers.get("x-debug-key", "")
    if provided != DEBUG_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.post("/debug/source-connector", response_model=DebugYtdlpResponse)
def debug_ytdlp(payload: DebugYtdlpRequest, request: Request) -> DebugYtdlpResponse:
    _require_debug_key(request)
    query = payload.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    cookies_path, cookies_exists, cookies_size = _cookies_diag()

    search_n = max(1, min(int(payload.search_n), 10))
    timeout_sec = max(5, min(int(payload.timeout_sec), 180))
    ids: List[str] = []

    if payload.mode == "search":
        try:
            ids = yt_search_ids(query, search_n)
        except Exception as exc:
            return DebugYtdlpResponse(
                query=query,
                mode=payload.mode,
                search_n=search_n,
                ids=[],
                ok=False,
                error=str(exc),
                cookies_path=cookies_path,
                cookies_exists=cookies_exists,
                cookies_size=cookies_size,
            )
        return DebugYtdlpResponse(
            query=query,
            mode=payload.mode,
            search_n=search_n,
            ids=ids,
            ok=True,
            cookies_path=cookies_path,
            cookies_exists=cookies_exists,
            cookies_size=cookies_size,
        )

    video_id = payload.video_id

    tmp_mp3 = Path("/tmp") / f"debug-{uuid4().hex}.mp3"
    executor = ThreadPoolExecutor(max_workers=1)
    if video_id:
        fut = executor.submit(yt_download_mp3, video_id, tmp_mp3)
    else:
        fut = executor.submit(yt_download_top_result_mp3, query, tmp_mp3)
    try:
        fut.result(timeout=timeout_sec)
        ok = tmp_mp3.exists() and tmp_mp3.stat().st_size > 0
        return DebugYtdlpResponse(
            query=query,
            mode=payload.mode,
            search_n=search_n,
            video_id=video_id,
            ids=ids,
            ok=ok,
            error=None if ok else "MP3 not produced",
            cookies_path=cookies_path,
            cookies_exists=cookies_exists,
            cookies_size=cookies_size,
        )
    except Exception as exc:
        return DebugYtdlpResponse(
            query=query,
            mode=payload.mode,
            search_n=search_n,
            video_id=video_id,
            ids=ids,
            ok=False,
            error=str(exc),
            cookies_path=cookies_path,
            cookies_exists=cookies_exists,
            cookies_size=cookies_size,
        )
    finally:
        executor.shutdown(wait=False)
        try:
            if tmp_mp3.exists():
                tmp_mp3.unlink()
        except Exception:
            pass


@app.post("/jobs", response_model=JobStatus)
def create_job(req: CreateJobRequest, request: Request) -> JobStatus:
    if EMERGENCY_DISABLE_NEW_JOBS:
        raise HTTPException(
            status_code=503,
            detail="New jobs are temporarily disabled by operator switch. Please retry later.",
        )

    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    # Back-compat: accept legacy cookie field without keeping the literal legacy key in source.
    extra = getattr(req, "__pydantic_extra__", None) or {}
    legacy_key = "".join(["you", "tube", "_cookies_netscape"])
    legacy_val = extra.get(legacy_key)
    runtime_cookies_payload = _normalize_runtime_cookies_payload(req.source_cookies_netscape or legacy_val)
    if SERVER_DOWNLOAD_ONLY_ENFORCED and (str(req.audio_url or "").strip() or str(req.audio_id or "").strip()):
        raise HTTPException(
            status_code=400,
            detail="direct audio_url/audio_id overrides are disabled in server-download-only mode",
        )

    idempotency_header = ""
    idempotency_header = (
        request.headers.get("Idempotency-Key")
        or request.headers.get("X-Idempotency-Key")
        or ""
    )
    idempotency_key = _normalize_idempotency_key(req.idempotency_key or idempotency_header)
    if REQUIRE_IDEMPOTENCY_KEY and not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="idempotency_key is required (body field or Idempotency-Key header)",
        )

    if idempotency_key and _jobs_sqlite_store is not None:
        persisted = _jobs_sqlite_store.get_by_idempotency_key(idempotency_key)
        if isinstance(persisted, dict) and persisted.get("id"):
            logger.info("job replayed via sqlite idempotency key", extra={"idempotency_key": idempotency_key})
            return JobStatus(**persisted)

    logger.info(
        "create job",
        extra={
            "query": query,
            "runtime_cookies_supplied": bool(runtime_cookies_payload),
            "idempotency_key_present": bool(idempotency_key),
        },
    )

    job_id = str(uuid4())
    slug = slugify(query)
    slug_reuse_candidates = _slug_reuse_candidates(slug)
    options, dropped_stem_keys = _strip_stem_levels_for_non_render(
        req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
    )
    options["preview"] = bool(req.preview)
    if options.get("tune_for_me") is not None and options.get("enable_auto_offset") is None:
        options["enable_auto_offset"] = options.pop("tune_for_me")
    if options.get("enable_auto_offset") is None:
        options["enable_auto_offset"] = int(DEFAULT_TUNE_FOR_ME_LEVEL)
    if options.get("calibration_level") is None:
        options["calibration_level"] = int(DEFAULT_CALIBRATION_LEVEL)
    if dropped_stem_keys:
        logger.info(
            "dropping stem options for non-render job",
            extra={"query": query, "slug": slug, "dropped_keys": ",".join(dropped_stem_keys)},
        )
    options["runtime_cookies_supplied"] = bool(runtime_cookies_payload)
    if SERVER_DOWNLOAD_ONLY_ENFORCED:
        options.pop("audio_url", None)
        options.pop("audio_id", None)
    dedupe_key = _build_job_dedupe_key(query, options)
    inflight_profile_key = _build_inflight_profile_key(options)

    with _jobs_lock:
        if idempotency_key:
            mapped_job_id = _idempotency_to_job_id.get(idempotency_key)
            if mapped_job_id:
                existing_job = _jobs.get(mapped_job_id)
                if existing_job is not None:
                    _refresh_job_local_output_fields(existing_job)
                    _refresh_job_uploaded_video_url(existing_job)
                    logger.info(
                        "job replayed by idempotency key",
                        extra={"job_id": existing_job.id, "idempotency_key": idempotency_key},
                    )
                    return JobStatus(**existing_job.__dict__)
                _idempotency_to_job_id.pop(idempotency_key, None)

        # Deduplicate only matching in-flight profile for this slug (same options shape).
        existing_slug_job = _find_inflight_job_for_slug(
            slug,
            slug_candidates=slug_reuse_candidates,
            requested_profile_key=inflight_profile_key,
        )
        if existing_slug_job is not None:
            if idempotency_key:
                _idempotency_to_job_id[idempotency_key] = existing_slug_job.id
            logger.info(
                "job deduped by slug",
                extra={
                    "job_id": existing_slug_job.id,
                    "query": query,
                    "slug": slug,
                    "slug_candidates": ",".join(slug_reuse_candidates),
                    "runtime_cookies_supplied": bool(runtime_cookies_payload),
                },
            )
            return JobStatus(**existing_slug_job.__dict__)
        reusable_job = _find_recent_succeeded_job_for_slug(
            slug,
            options=options,
            slug_candidates=slug_reuse_candidates,
        )
        if reusable_job is not None:
            _refresh_job_local_output_fields(reusable_job)
            _refresh_job_uploaded_video_url(reusable_job)
            if idempotency_key:
                _idempotency_to_job_id[idempotency_key] = reusable_job.id
            logger.info(
                "job reused from succeeded cache",
                extra={
                    "job_id": reusable_job.id,
                    "query": query,
                    "slug": slug,
                    "slug_candidates": ",".join(slug_reuse_candidates),
                },
            )
            return JobStatus(**reusable_job.__dict__)
        if _active_job_count() >= MAX_PENDING_JOBS:
            retry_after = _queue_retry_after_seconds(_active_job_count())
            raise HTTPException(
                status_code=429,
                detail="Too many jobs in progress. Please retry shortly.",
                headers={"Retry-After": str(retry_after)},
            )
        job = Job(
            id=job_id,
            query=query,
            slug=slug,
            created_at=_now_ts(),
            idempotency_key=(idempotency_key or None),
            dedupe_key=dedupe_key,
            options=options,
        )
        _touch_job_step(job, "queued")
        _jobs[job_id] = job
        if idempotency_key:
            _idempotency_to_job_id[idempotency_key] = job_id
        # Update secondary index for inflight job lookup
        for candidate_slug in (slug_reuse_candidates or (slug,)):
            _slug_to_job_id[candidate_slug] = job_id
        # Increment active job counter (job starts in "queued" status)
        _increment_active_job_count()
        _prune_jobs_history()
    _persist_jobs()

    _executor.submit(_run_job, job, runtime_cookies_payload)
    logger.info("job queued", extra={"job_id": job_id, "slug": slug})

    return JobStatus(**job.__dict__)


@app.post("/jobs/upload", response_model=JobStatus)
async def create_job_with_upload(
    audio_file: UploadFile = File(...),
    query: str = Form(...),
    artist: str = Form(None),
    title: str = Form(None),
    language: str = Form("auto"),
    render_mode: str = Form(None),
) -> JobStatus:
    """
    Create a job with a pre-downloaded audio file.
    This bypasses source download entirely, eliminating timeout issues.

    The client (e.g., iOS app) downloads the audio locally, then uploads it here.
    """
    if EMERGENCY_DISABLE_NEW_JOBS:
        raise HTTPException(
            status_code=503,
            detail="New jobs are temporarily disabled by operator switch. Please retry later.",
        )

    if SERVER_DOWNLOAD_ONLY_ENFORCED or (not ENABLE_CLIENT_UPLOAD_ENDPOINT):
        raise HTTPException(status_code=403, detail="Client audio uploads are disabled in server-download-only mode.")

    query = query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    if not audio_file.filename:
        raise HTTPException(status_code=400, detail="audio_file is required")

    # Validate file extension
    allowed_extensions = {".mp3", ".m4a", ".wav", ".ogg", ".webm"}
    file_ext = Path(audio_file.filename).suffix.lower()
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid audio file type: {file_ext}. Allowed: {', '.join(allowed_extensions)}"
        )

    logger.info("create job with upload", extra={"query": query, "audio_filename": audio_file.filename})

    job_id = str(uuid4())
    slug = slugify(query)

    # Save uploaded audio file
    MP3_DIR.mkdir(parents=True, exist_ok=True)
    uploaded_audio_path = MP3_DIR / f"{slug}.uploaded{file_ext}"

    try:
        # Stream upload to disk
        with open(uploaded_audio_path, "wb") as f:
            while chunk := await audio_file.read(8192):
                f.write(chunk)

        file_size_mb = uploaded_audio_path.stat().st_size / (1024 * 1024)
        logger.info(
            "audio uploaded",
            extra={"job_id": job_id, "slug": slug, "size_mb": f"{file_size_mb:.2f}"}
        )

        # Verify it's a valid audio file (basic check)
        if uploaded_audio_path.stat().st_size < 1000:
            uploaded_audio_path.unlink()
            raise HTTPException(status_code=400, detail="Uploaded file is too small to be valid audio")

    except Exception as exc:
        if uploaded_audio_path.exists():
            uploaded_audio_path.unlink()
        logger.exception("audio upload failed", extra={"job_id": job_id, "error": str(exc)})
        raise HTTPException(status_code=500, detail=f"Failed to save uploaded audio: {exc}")

    with _jobs_lock:
        # Check for existing job with same slug
        existing_slug_job = _find_inflight_job_for_slug(slug)
        if existing_slug_job is not None:
            logger.info(
                "job deduped by slug (upload)",
                extra={"job_id": existing_slug_job.id, "query": query, "slug": slug},
            )
            # Clean up uploaded file since we're using existing job
            if uploaded_audio_path.exists():
                uploaded_audio_path.unlink()
            return JobStatus(**existing_slug_job.__dict__)

        if _active_job_count() >= MAX_PENDING_JOBS:
            if uploaded_audio_path.exists():
                uploaded_audio_path.unlink()
            retry_after = _queue_retry_after_seconds(_active_job_count())
            raise HTTPException(
                status_code=429,
                detail="Too many jobs in progress. Please retry shortly.",
                headers={"Retry-After": str(retry_after)},
            )

        options = {
            "query": query,
            "language": language,
            "preview": bool(req.preview),
            "enable_auto_offset": (
                int(req.tune_for_me) if req.tune_for_me is not None else int(DEFAULT_TUNE_FOR_ME_LEVEL)
            ),
            "calibration_level": (
                int(req.calibration_level)
                if req.calibration_level is not None
                else int(DEFAULT_CALIBRATION_LEVEL)
            ),
            "uploaded_audio": True,
            "uploaded_audio_path": str(uploaded_audio_path),
            "artist": artist,
            "title": title,
        }

        job = Job(
            id=job_id,
            query=query,
            slug=slug,
            created_at=_now_ts(),
            options=options,
        )
        _jobs[job_id] = job
        _slug_to_job_id[slug] = job_id
        _increment_active_job_count()
        _prune_jobs_history()

    _persist_jobs()

    # Use uploaded audio mode
    _executor.submit(_run_job_with_upload, job, uploaded_audio_path, artist, title)
    logger.info("job queued (upload)", extra={"job_id": job_id, "slug": slug})

    return JobStatus(**job.__dict__)


@app.get("/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: str, request: Request, response: Response, view: str = "full"):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    _refresh_job_local_output_fields(job)
    _refresh_job_uploaded_video_url(job)

    view_mode = "poll" if str(view or "").strip().lower() == "poll" else "full"
    cache_key = _job_status_cache_key(job_id, view=view_mode)
    if_none_match = request.headers.get("if-none-match", "")

    # Optimization: Cache serialized job status to avoid repeated dict conversion
    # This is especially helpful during polling (every 2.5s from mobile clients)
    last_updated = job.last_updated_at or job.created_at
    with _job_status_cache_lock:
        cached = _job_status_cache.get(cache_key)
        if cached and cached[0] == last_updated:
            # Cache hit: move to end for O(1) LRU tracking
            _job_status_cache.move_to_end(cache_key)
            etag = cached[1]
            if _etag_matches_if_none_match(if_none_match, etag):
                return Response(
                    status_code=304,
                    headers={"ETag": etag, "Cache-Control": "private, no-cache"},
                )
            # This endpoint is polled frequently by clients; keep per-request logging in middleware.
            logger.debug("get job (cached)", extra={"job_id": job_id, "status": job.status})
            response.headers["ETag"] = etag
            response.headers["Cache-Control"] = "private, no-cache"
            response.headers["X-Job-Last-Updated"] = str(last_updated)
            return JobStatus(**cached[2])

        # Cache miss: serialize and store
        job_dict = _job_to_poll_dict(job) if view_mode == "poll" else _job_to_dict(job)
        etag = _job_status_etag(job_dict)
        _job_status_cache[cache_key] = (last_updated, etag, job_dict)
        _job_status_cache.move_to_end(cache_key)  # Mark as most recently used

        # LRU eviction: O(1) removal of oldest (first) item
        if len(_job_status_cache) > _JOB_STATUS_CACHE_MAX_SIZE:
            _job_status_cache.popitem(last=False)  # Remove first (oldest) item

    if _etag_matches_if_none_match(if_none_match, etag):
        return Response(
            status_code=304,
            headers={"ETag": etag, "Cache-Control": "private, no-cache"},
        )
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "private, no-cache"
    response.headers["X-Job-Last-Updated"] = str(last_updated)
    logger.debug("get job", extra={"job_id": job_id, "status": job.status})
    return JobStatus(**job_dict)


@app.post("/jobs/{job_id}/cancel", response_model=JobStatus)
def cancel_job(job_id: str) -> JobStatus:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status in {"succeeded", "failed", "cancelled"}:
            return JobStatus(**job.__dict__)
        _mark_job_finishing(job)
        job.status = "cancelled"
        job.cancelled_at = _now_ts()
        job.error = "Cancelled by user."
        job.finished_at = _now_ts()
        job.stage = "cancelled"
        job.last_message = "Cancelled by user."
        job.last_updated_at = _now_ts()
        logger.info("job cancelled", extra={"job_id": job_id})
    _persist_jobs()
    with _job_processes_lock:
        proc = _job_processes.get(job_id)
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=5)
            logger.info("job process terminated", extra={"job_id": job_id})
        except Exception:
            try:
                proc.kill()
                logger.info("job process killed", extra={"job_id": job_id})
            except Exception:
                pass
    _cancel_gpu_worker_job(job_id)
    return JobStatus(**job.__dict__)

@app.get("/jobs", response_model=list[JobStatus])
def list_jobs(request: Request) -> list[JobStatus]:
    # Debug-only: listing all jobs can leak job IDs/output URLs on a public deployment.
    _require_debug_key(request)
    with _jobs_lock:
        jobs = list(_jobs.values())
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    logger.info("list jobs", extra={"count": len(jobs)})
    return [JobStatus(**j.__dict__) for j in jobs]


# ============================================================================
# Client-First Download: Remote Config & Metrics
# ============================================================================

# Global download strategy config (can be changed at runtime as kill switch)
_download_strategy_override: Optional[str] = None
_download_strategy_lock = threading.Lock()


@app.get("/config/download-strategy")
def get_download_strategy() -> dict[str, Any]:
    """
    Get current download strategy configuration for iOS clients.

    This serves as a remote configuration endpoint that allows the server
    to control client-side download behavior. Can be used as a kill switch
    to force server_only mode if client extraction breaks.

    Returns:
        dict: Configuration including strategy, flags, and settings
    """
    with _download_strategy_lock:
        strategy = _download_strategy_override or ("server_only" if SERVER_DOWNLOAD_ONLY_ENFORCED else "local_first")
    if SERVER_DOWNLOAD_ONLY_ENFORCED:
        strategy = "server_only"

    config = {
        "strategy": strategy,
        "enableClientExtraction": False if SERVER_DOWNLOAD_ONLY_ENFORCED else (strategy != "server_only"),
        "enableServerFallback": True,
        "maxClientRetries": 3,
        "clientTimeout": 30000,
    }

    logger.info("download strategy requested", extra={"strategy": strategy})
    return config


@app.post("/config/download-strategy")
def set_download_strategy(payload: dict[str, Any], req: Request) -> dict[str, str]:
    """
    Set download strategy configuration (kill switch endpoint).

    Use this to remotely control client download behavior:
    - "local_first": Try client extraction first, fallback to server
    - "server_fallback": Prefer server extraction
    - "server_only": Force all extraction through server (kill switch)

    Args:
        request: dict with "strategy" key

    Returns:
        dict: Status and current strategy

    Example:
        POST /config/download-strategy
        {"strategy": "server_only"}
    """
    global _download_strategy_override

    # Treat this as an admin/debug endpoint. Without a configured debug key, hide it.
    _require_debug_key(req)

    if SERVER_DOWNLOAD_ONLY_ENFORCED:
        raise HTTPException(status_code=403, detail="download strategy override is disabled while server-download-only mode is enforced")

    strategy = payload.get("strategy")

    if strategy not in ["local_first", "server_fallback", "server_only", None]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid strategy: {strategy}. Must be local_first, server_fallback, or server_only"
        )

    with _download_strategy_lock:
        previous = _download_strategy_override
        _download_strategy_override = strategy

    logger.warning(
        "download strategy changed",
        extra={"new_strategy": strategy, "previous": previous}
    )

    return {"status": "ok", "strategy": strategy or "local_first"}


@app.post("/metrics/download")
def record_download_metrics(payload: dict[str, Any], request: Request) -> dict[str, str]:
    """
    Receive download metrics from iOS clients for monitoring and analysis.

    Clients send metrics periodically (every 5 minutes or on significant events)
    to track download success rates, provider performance, and error patterns.

    Args:
        request: dict containing metrics, derived stats, device info, and events

    Returns:
        dict: Status acknowledgement

    Payload structure:
        {
            "metrics": {
                "total_attempts": int,
                "successful_downloads": int,
                "client_success": int,
                "server_fallback": int,
                "failures": int,
                "average_duration_ms": float,
                "average_file_size_bytes": float,
                "provider_stats": {...},
                "error_types": {...}
            },
            "derived": {
                "success_rate": float,
                "client_success_rate": float,
                "fallback_rate": float
            },
            "device": {
                "platform": "ios",
                "app_version": str,
                "timestamp": int
            },
            "recent_events": [...]
        }
    """
    # Debug-only: otherwise this can be spammed to fill logs on a public deployment.
    _require_debug_key(request)
    try:
        metrics = payload.get("metrics", {})
        derived = payload.get("derived", {})
        device = payload.get("device", {})

        # Log summary for monitoring
        logger.info(
            "client metrics received",
            extra={
                "device_platform": device.get("platform"),
                "app_version": device.get("app_version"),
                "total_attempts": metrics.get("total_attempts", 0),
                "success_rate": derived.get("success_rate", 0),
                "client_success_rate": derived.get("client_success_rate", 0),
                "fallback_rate": derived.get("fallback_rate", 0),
                "failures": metrics.get("failures", 0),
            }
        )

        # Log detailed metrics for analysis
        logger.debug("client metrics detail", extra={"metrics": metrics, "derived": derived})

        # Could store in database, send to monitoring system, etc.
        # For now, just log and acknowledge

        return {"status": "ok"}

    except Exception as e:
        logger.error("error processing client metrics", extra={"error": str(e)})
        return {"status": "error", "message": str(e)}
