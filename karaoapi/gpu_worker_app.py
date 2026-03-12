from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from scripts.common import slugify
from scripts.worker_security import (
    NonceReplayCache,
    load_dual_hmac_secrets,
    verify_signed_headers,
)
from scripts.worker_storage import (
    create_temp_download,
    download_file as gcs_download_file,
    is_gs_uri,
    object_exists as gcs_object_exists,
    sha256_file,
    source_object_uri as build_source_object_uri,
    stems_object_uri as build_stem_uri,
    upload_file as gcs_upload_file,
)


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SEPARATED_ROOT = BASE_DIR / "separated"


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_first_nonempty(*names: str) -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is None:
            continue
        clean = raw.strip()
        if clean:
            return clean
    return ""


def _default_worker_max_concurrent() -> int:
    explicit = (os.environ.get("MIXTERIOSO_GPU_WORKER_MAX_CONCURRENT") or "").strip()
    if explicit:
        try:
            return max(1, int(explicit))
        except ValueError:
            pass

    auto_cuda = _env_int("MIXTERIOSO_GPU_WORKER_MAX_CONCURRENT_AUTO_CUDA", 2, minimum=1)
    auto_cpu = _env_int("MIXTERIOSO_GPU_WORKER_MAX_CONCURRENT_AUTO_CPU", 1, minimum=1)
    cuda_available = False
    try:
        import torch  # type: ignore[import-not-found]
        cuda_available = bool(torch.cuda.is_available())
    except Exception:
        cuda_available = bool(shutil.which("nvidia-smi"))
    return auto_cuda if cuda_available else auto_cpu


WORKER_API_KEY = _env_first_nonempty(
    "MIXTERIOSO_GPU_WORKER_API_KEY",
    "KARAOAPI_GPU_WORKER_API_KEY",
    "GPU_WORKER_API_KEY",
)
WORKER_MAX_CONCURRENT = max(1, _default_worker_max_concurrent())
WORKER_QUEUE_WAIT_SECS = _env_float("MIXTERIOSO_GPU_WORKER_QUEUE_WAIT_SECS", 5.0, minimum=0.1)
WORKER_JOB_TIMEOUT_SECS = _env_float("MIXTERIOSO_GPU_WORKER_JOB_TIMEOUT_SECS", 1800.0, minimum=1.0)
WORKER_STEM_MIN_BYTES = _env_int("MIXTERIOSO_GPU_STEM_MIN_BYTES", 1024, minimum=44)
WORKER_CACHE_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_CACHE_ENABLED", True)
WORKER_SINGLEFLIGHT_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_SINGLEFLIGHT_ENABLED", True)
WORKER_CACHE_PRUNE_ENABLED = _env_bool("MIXTERIOSO_GPU_WORKER_CACHE_PRUNE_ENABLED", True)
WORKER_CACHE_PRUNE_INTERVAL_SECS = _env_float("MIXTERIOSO_GPU_WORKER_CACHE_PRUNE_INTERVAL_SECS", 120.0, minimum=5.0)
WORKER_CACHE_PRUNE_SCAN_LIMIT = _env_int("MIXTERIOSO_GPU_WORKER_CACHE_PRUNE_SCAN_LIMIT", 2500, minimum=100)
WORKER_CACHE_MAX_DIRS = _env_int("MIXTERIOSO_GPU_WORKER_CACHE_MAX_DIRS", 5000, minimum=1)
WORKER_CACHE_MAX_AGE_SECS = _env_float("MIXTERIOSO_GPU_WORKER_CACHE_MAX_AGE_SECS", 604800.0, minimum=60.0)
WORKER_ENFORCE_AUDIO_ROOTS = _env_bool("MIXTERIOSO_GPU_WORKER_ENFORCE_AUDIO_ROOTS", False)
WORKER_ALLOWED_AUDIO_ROOTS_RAW = os.environ.get("MIXTERIOSO_GPU_WORKER_ALLOWED_AUDIO_ROOTS", "").strip()
GPU_WORKER_HMAC_SECRET = _env_first_nonempty(
    "GPU_WORKER_HMAC_SECRET",
    "MIXTERIOSO_GPU_WORKER_HMAC_SECRET",
    "KARAOAPI_GPU_WORKER_HMAC_SECRET",
)
GPU_WORKER_HMAC_SECRET_PREVIOUS = _env_first_nonempty(
    "GPU_WORKER_HMAC_SECRET_PREVIOUS",
    "MIXTERIOSO_GPU_WORKER_HMAC_SECRET_PREVIOUS",
    "KARAOAPI_GPU_WORKER_HMAC_SECRET_PREVIOUS",
)
GPU_WORKER_REQUIRE_HMAC = _env_bool(
    "GPU_WORKER_REQUIRE_HMAC",
    _env_bool(
        "MIXTERIOSO_GPU_WORKER_REQUIRE_HMAC",
        _env_bool("KARAOAPI_GPU_WORKER_REQUIRE_HMAC", bool(GPU_WORKER_HMAC_SECRET)),
    ),
)
GPU_WORKER_ALLOW_UNAUTH = _env_bool("GPU_WORKER_ALLOW_UNAUTH", False)
GPU_WORKER_REPLAY_WINDOW_SEC = _env_int("GPU_WORKER_REPLAY_WINDOW_SEC", 300, minimum=30)
GPU_WORKER_NONCE_TTL_SEC = _env_int("GPU_WORKER_NONCE_TTL_SEC", 900, minimum=60)
GPU_WORKER_NONCE_MAX_ENTRIES = _env_int("GPU_WORKER_NONCE_MAX_ENTRIES", 100_000, minimum=10_000)
SOURCE_BUCKET = (
    os.environ.get("SOURCE_BUCKET")
    or os.environ.get("STORAGE_SOURCE_BUCKET")
    or ""
).strip()
STEMS_BUCKET = (
    os.environ.get("STEMS_BUCKET")
    or os.environ.get("STORAGE_STEMS_BUCKET")
    or ""
).strip()
GPU_WORKER_INLINE_SOURCE_ENABLED = _env_bool("GPU_WORKER_INLINE_SOURCE_ENABLED", True)
GPU_WORKER_INLINE_SOURCE_MAX_BYTES = _env_int(
    "GPU_WORKER_INLINE_SOURCE_MAX_BYTES",
    _env_int("MIXTERIOSO_GPU_WORKER_INLINE_SOURCE_MAX_BYTES", 12_000_000, minimum=1024),
    minimum=1024,
)
DEMUCS_MODEL = (os.environ.get("DEMUCS_MODEL") or os.environ.get("MIXTERIOSO_DEMUCS_MODEL") or "htdemucs").strip() or "htdemucs"

_WORKER_SEMAPHORE = threading.Semaphore(WORKER_MAX_CONCURRENT)
_SINGLEFLIGHT_LOCK = threading.Lock()
_SINGLEFLIGHT_LOCKS: Dict[str, threading.Lock] = {}
_SINGLEFLIGHT_REFS: Dict[str, int] = {}
_CACHE_PRUNE_LOCK = threading.Lock()
_CACHE_PRUNE_LAST_AT_MONO = 0.0
_HMAC_REPLAY_CACHE = NonceReplayCache(ttl_sec=GPU_WORKER_NONCE_TTL_SEC, max_entries=GPU_WORKER_NONCE_MAX_ENTRIES)
_ACTIVE_CANCEL_EVENTS: Dict[str, threading.Event] = {}
_ACTIVE_PROCS: Dict[str, subprocess.Popen] = {}
_ACTIVE_LOCK = threading.Lock()

app = FastAPI(title="Mixterioso GPU Worker", version="0.2.0")
logging.basicConfig(
    level=(os.environ.get("LOG_LEVEL", "INFO") or "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mixterioso-gpu-worker")


class SeparateRequest(BaseModel):
    model_config = {"extra": "allow"}

    job_id: str = ""
    source_uri: str = ""
    source_sha256: str = ""
    inline_source_b64: str = ""
    inline_source_name: str = ""
    stem_profile: Dict[str, float] = Field(default_factory=dict)
    model_version: str = ""

    # Compatibility fields (legacy contract)
    slug: str = ""
    audio_path: str = ""
    output_dir: str = ""
    model: str = "htdemucs"
    two_stems: bool = False
    shifts: int = Field(default=1, ge=1, le=20)
    overlap: float = Field(default=0.1, ge=0.0, le=0.99)
    device: str = "auto"
    requested_stems: List[str] = Field(default_factory=list)
    force: bool = False
    stems_bucket: str = ""


def _normalize_device(raw: str) -> str:
    value = (raw or "").strip().lower()
    aliases = {
        "gpu": "cuda",
        "metal": "mps",
        "apple": "mps",
    }
    normalized = aliases.get(value, value or "auto")
    if normalized not in {"auto", "cuda", "mps", "cpu"}:
        normalized = "auto"

    available = _available_runtime_devices()
    preferred = _preferred_runtime_device(available)
    if normalized == "auto":
        return preferred
    if normalized in available:
        return normalized

    logger.warning(
        "requested demucs device unavailable; falling back",
        extra={"requested_device": normalized, "fallback_device": preferred},
    )
    return preferred


def _available_runtime_devices() -> set[str]:
    devices: set[str] = {"cpu"}
    try:
        import torch  # type: ignore[import-not-found]
    except Exception:
        if shutil.which("nvidia-smi"):
            devices.add("cuda")
        return devices

    try:
        if bool(torch.cuda.is_available()):
            devices.add("cuda")
    except Exception:
        if shutil.which("nvidia-smi"):
            devices.add("cuda")

    try:
        mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
        if mps_backend and bool(mps_backend.is_available()):
            devices.add("mps")
    except Exception:
        pass

    return devices


def _preferred_runtime_device(available: set[str]) -> str:
    if "cuda" in available:
        return "cuda"
    if "mps" in available:
        return "mps"
    return "cpu"


def _runtime_cuda_probe() -> Dict[str, Any]:
    probe: Dict[str, Any] = {
        "nvidia_smi_on_path": bool(shutil.which("nvidia-smi")),
    }

    try:
        import torch  # type: ignore[import-not-found]
    except Exception as exc:
        probe["torch_import_error"] = str(exc)[:300]
        return probe

    probe["torch_version"] = str(getattr(torch, "__version__", "unknown"))
    probe["torch_cuda_version"] = str(getattr(getattr(torch, "version", None), "cuda", None))

    try:
        cuda_backend = getattr(getattr(torch, "backends", None), "cuda", None)
        probe["torch_cuda_built"] = bool(cuda_backend and cuda_backend.is_built())
    except Exception as exc:
        probe["torch_cuda_built_error"] = str(exc)[:300]

    cuda_available = False
    try:
        cuda_available = bool(torch.cuda.is_available())
        probe["torch_cuda_available"] = cuda_available
    except Exception as exc:
        probe["torch_cuda_available_error"] = str(exc)[:300]

    if cuda_available:
        try:
            probe["torch_cuda_device_count"] = int(torch.cuda.device_count())
        except Exception as exc:
            probe["torch_cuda_device_count_error"] = str(exc)[:300]
        try:
            probe["torch_cuda_device_name"] = str(torch.cuda.get_device_name(0))
        except Exception as exc:
            probe["torch_cuda_device_name_error"] = str(exc)[:300]

    return probe


def _normalize_stem_profile(profile: Dict[str, Any]) -> Dict[str, float]:
    def _coerce(name: str) -> float:
        raw = profile.get(name, 100.0)
        try:
            value = float(raw)
        except Exception:
            value = 100.0
        if value < 0.0:
            return 0.0
        if value > 150.0:
            return 150.0
        return value

    return {
        "vocals": _coerce("vocals"),
        "bass": _coerce("bass"),
        "drums": _coerce("drums"),
        "other": _coerce("other"),
    }


def _required_stems(two_stems: bool, requested_stems: List[str]) -> List[str]:
    if two_stems:
        return ["vocals", "no_vocals"]
    if requested_stems:
        cleaned = []
        for name in requested_stems:
            n = (name or "").strip().lower()
            if n in {"vocals", "bass", "drums", "other"} and n not in cleaned:
                cleaned.append(n)
        if cleaned:
            return cleaned
    return ["vocals", "bass", "drums", "other"]


def _resolve_audio_path(raw: str) -> Path:
    p = Path((raw or "").strip()).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    return p


def _parse_allowed_audio_roots(raw: str) -> List[Path]:
    roots: List[Path] = []
    seen: set[str] = set()
    parts = [chunk.strip() for chunk in (raw or "").replace(";", ",").split(",")]
    if not any(parts):
        parts = [str(BASE_DIR / "mp3s"), "/tmp", str(BASE_DIR)]
    for chunk in parts:
        if not chunk:
            continue
        p = Path(chunk).expanduser()
        if not p.is_absolute():
            p = (BASE_DIR / p).resolve()
        else:
            p = p.resolve()
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        roots.append(p)
    return roots


WORKER_ALLOWED_AUDIO_ROOTS = _parse_allowed_audio_roots(WORKER_ALLOWED_AUDIO_ROOTS_RAW)


def _is_path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _audio_path_allowed(path: Path) -> bool:
    if not WORKER_ENFORCE_AUDIO_ROOTS:
        return True
    for root in WORKER_ALLOWED_AUDIO_ROOTS:
        if _is_path_within(path, root):
            return True
    return False


def _resolve_stem_dir(req: SeparateRequest, slug: str) -> tuple[Path, Path]:
    output_dir_raw = (req.output_dir or "").strip()
    model = (req.model_version or req.model or DEMUCS_MODEL).strip() or DEMUCS_MODEL
    if output_dir_raw:
        out = Path(output_dir_raw).expanduser()
        if not out.is_absolute():
            out = (BASE_DIR / out).resolve()
        if out.name == slug and out.parent.name == model:
            stem_dir = out
            output_base = out.parent.parent
        else:
            output_base = out
            stem_dir = output_base / model / slug
    else:
        output_base = DEFAULT_SEPARATED_ROOT
        stem_dir = output_base / model / slug
    return output_base, stem_dir


def _singleflight_key(
    *,
    source_sha256: str,
    model_version: str,
    two_stems: bool,
    shifts: int,
    overlap: float,
    device: str,
    required_stems: List[str],
    stem_profile: Dict[str, float],
) -> str:
    payload = {
        "source_sha256": str(source_sha256 or "").strip().lower(),
        "model_version": model_version,
        "two_stems": bool(two_stems),
        "shifts": int(shifts),
        "overlap": float(overlap),
        "device": device,
        "required_stems": list(required_stems),
        "stem_profile": stem_profile,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return raw


def _acquire_singleflight_lock(key: str) -> threading.Lock:
    with _SINGLEFLIGHT_LOCK:
        lock = _SINGLEFLIGHT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _SINGLEFLIGHT_LOCKS[key] = lock
            _SINGLEFLIGHT_REFS[key] = 0
        _SINGLEFLIGHT_REFS[key] = int(_SINGLEFLIGHT_REFS.get(key) or 0) + 1
    lock.acquire()
    return lock


def _release_singleflight_lock(key: str, lock: threading.Lock) -> None:
    try:
        lock.release()
    finally:
        with _SINGLEFLIGHT_LOCK:
            refs = max(0, int(_SINGLEFLIGHT_REFS.get(key) or 0) - 1)
            if refs <= 0:
                _SINGLEFLIGHT_REFS.pop(key, None)
                if not lock.locked():
                    _SINGLEFLIGHT_LOCKS.pop(key, None)
            else:
                _SINGLEFLIGHT_REFS[key] = refs


def _cache_meta_path(stem_dir: Path) -> Path:
    return stem_dir / ".worker_meta.json"


def _load_cache_meta(stem_dir: Path) -> Dict[str, Any]:
    meta_path = _cache_meta_path(stem_dir)
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_cache_meta(stem_dir: Path, payload: Dict[str, Any]) -> None:
    meta_path = _cache_meta_path(stem_dir)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _validate_stems(stem_dir: Path, required_stems: List[str], *, min_bytes: int) -> None:
    if not stem_dir.exists():
        raise RuntimeError(f"stems directory missing: {stem_dir}")
    missing = [name for name in required_stems if not (stem_dir / f"{name}.wav").exists()]
    if missing:
        raise RuntimeError(f"missing stems in {stem_dir}: {missing}")
    for name in required_stems:
        wav_path = stem_dir / f"{name}.wav"
        size = int(wav_path.stat().st_size)
        if size < int(min_bytes):
            raise RuntimeError(f"stem too small in {stem_dir}: {wav_path.name} ({size} bytes)")


def _cleanup_partial_stems(stem_dir: Path, required_stems: List[str]) -> None:
    for name in required_stems:
        try:
            (stem_dir / f"{name}.wav").unlink(missing_ok=True)
        except Exception:
            pass
    for p in stem_dir.glob("*.part"):
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass


def _maybe_prune_worker_cache(*, cache_root: Path, preserve_dirs: List[Path]) -> None:
    global _CACHE_PRUNE_LAST_AT_MONO
    if not WORKER_CACHE_PRUNE_ENABLED:
        return
    now_mono = time.monotonic()
    if (now_mono - _CACHE_PRUNE_LAST_AT_MONO) < float(WORKER_CACHE_PRUNE_INTERVAL_SECS):
        return
    with _CACHE_PRUNE_LOCK:
        now_mono = time.monotonic()
        if (now_mono - _CACHE_PRUNE_LAST_AT_MONO) < float(WORKER_CACHE_PRUNE_INTERVAL_SECS):
            return
        _CACHE_PRUNE_LAST_AT_MONO = now_mono
        if not cache_root.exists():
            return

        keep = {str(p.resolve()) for p in preserve_dirs}
        now_epoch = time.time()
        survivors: List[tuple[float, Path]] = []
        children: List[Path] = []
        try:
            for idx, child in enumerate(cache_root.iterdir()):
                if idx >= int(WORKER_CACHE_PRUNE_SCAN_LIMIT):
                    break
                children.append(child)
        except Exception:
            return
        for child in children:
            try:
                if not child.is_dir():
                    continue
                resolved = str(child.resolve())
                if resolved in keep:
                    continue
                meta_path = child / ".worker_meta.json"
                if not meta_path.exists():
                    continue
                try:
                    mtime = float(meta_path.stat().st_mtime)
                except Exception:
                    mtime = now_epoch
                age_sec = max(0.0, now_epoch - mtime)
                if age_sec > float(WORKER_CACHE_MAX_AGE_SECS):
                    shutil.rmtree(child, ignore_errors=True)
                    continue
                survivors.append((mtime, child))
            except Exception:
                continue

        max_dirs = int(max(1, WORKER_CACHE_MAX_DIRS))
        if len(survivors) <= max_dirs:
            return
        survivors.sort(key=lambda item: item[0], reverse=True)
        for _mtime, stale_dir in survivors[max_dirs:]:
            shutil.rmtree(stale_dir, ignore_errors=True)


def _register_active_job(job_id: str, cancel_event: threading.Event) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_CANCEL_EVENTS[job_id] = cancel_event


def _set_active_proc(job_id: str, proc: Optional[subprocess.Popen]) -> None:
    with _ACTIVE_LOCK:
        if proc is None:
            _ACTIVE_PROCS.pop(job_id, None)
            return
        _ACTIVE_PROCS[job_id] = proc


def _clear_active_job(job_id: str) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_CANCEL_EVENTS.pop(job_id, None)
        _ACTIVE_PROCS.pop(job_id, None)


class _JobCancelled(Exception):
    pass


def _run_demucs(
    *,
    source_audio: Path,
    output_base: Path,
    model: str,
    two_stems: bool,
    shifts: int,
    overlap: float,
    device: str,
    timeout_secs: float,
    cancel_event: threading.Event,
    job_id: str,
) -> None:
    if not shutil.which("demucs"):
        raise RuntimeError("demucs is not available on PATH")

    cmd = [
        "demucs",
        "-n",
        model,
        "--shifts",
        str(shifts),
        "--overlap",
        str(overlap),
        "-d",
        device,
        "-o",
        str(output_base),
    ]
    if two_stems:
        cmd.extend(["--two-stems", "vocals"])
    cmd.append(str(source_audio))

    logger.info(
        "demucs run start",
        extra={"job_id": job_id, "model": model, "device": device, "two_stems": two_stems},
    )
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as exc:
        raise RuntimeError(f"failed to execute demucs: {exc}") from exc

    _set_active_proc(job_id, proc)
    started = time.monotonic()
    try:
        while True:
            if cancel_event.is_set():
                try:
                    proc.terminate()
                    proc.wait(timeout=8)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                raise _JobCancelled("job canceled")

            rc = proc.poll()
            if rc is not None:
                break

            if (time.monotonic() - started) > float(timeout_secs):
                try:
                    proc.terminate()
                    proc.wait(timeout=8)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                raise RuntimeError(f"demucs timed out after {int(timeout_secs)}s")

            time.sleep(0.25)

        stderr_tail = ""
        stdout_tail = ""
        try:
            _stdout, _stderr = proc.communicate(timeout=2)
            stderr_tail = "\n".join((_stderr or "").splitlines()[-20:])
            stdout_tail = "\n".join((_stdout or "").splitlines()[-20:])
        except Exception:
            pass

        if int(rc) != 0:
            detail = (stderr_tail or stdout_tail or f"exit code {rc}").strip()
            raise RuntimeError(f"demucs failed: {detail[:800]}")
    finally:
        _set_active_proc(job_id, None)


def _cache_matches(
    meta: Dict[str, Any],
    *,
    source_sha256: str,
    model_version: str,
    stem_profile: Dict[str, float],
    two_stems: bool,
    shifts: int,
    overlap: float,
    device: str,
    required_stems: List[str],
) -> bool:
    if not meta:
        return False
    expected = {
        "source_sha256": source_sha256,
        "model_version": model_version,
        "stem_profile": stem_profile,
        "two_stems": bool(two_stems),
        "shifts": int(shifts),
        "overlap": float(overlap),
        "device": device,
        "required_stems": list(required_stems),
    }
    for key, expected_val in expected.items():
        if meta.get(key) != expected_val:
            return False
    return True


def _hmac_secrets() -> list[str]:
    current = GPU_WORKER_HMAC_SECRET or _env_first_nonempty("KARAOAPI_GPU_WORKER_HMAC_SECRET")
    previous = GPU_WORKER_HMAC_SECRET_PREVIOUS or _env_first_nonempty("KARAOAPI_GPU_WORKER_HMAC_SECRET_PREVIOUS")
    return [s for s in (current, previous) if s]


def _auth_configured() -> bool:
    return bool(_hmac_secrets() or WORKER_API_KEY or GPU_WORKER_ALLOW_UNAUTH)


def _authorize_request(req: Request, body_bytes: bytes) -> tuple[bool, str]:
    secrets = _hmac_secrets()
    if secrets:
        ok, detail = verify_signed_headers(
            body_bytes=body_bytes,
            headers=dict(req.headers),
            accepted_secrets=secrets,
            replay_cache=_HMAC_REPLAY_CACHE,
            max_skew_sec=GPU_WORKER_REPLAY_WINDOW_SEC,
        )
        if ok:
            return True, ""
        return False, detail

    if GPU_WORKER_REQUIRE_HMAC:
        return False, "hmac required but worker secret is not configured"

    if WORKER_API_KEY:
        auth_header = (req.headers.get("Authorization") or "").strip()
        if not auth_header.lower().startswith("bearer "):
            return False, "missing bearer token"
        token = auth_header.split(" ", 1)[1].strip()
        if token != WORKER_API_KEY:
            return False, "invalid bearer token"
        return True, ""

    if GPU_WORKER_ALLOW_UNAUTH:
        return True, ""
    return False, "worker authentication is not configured"


def _load_source_audio(req: SeparateRequest) -> tuple[Path, str, str, int]:
    """Returns local source path, source_uri, source_sha256, download_ms."""

    t0 = time.perf_counter()
    source_uri = str(req.source_uri or "").strip()
    inline_source_b64 = str(req.inline_source_b64 or "").strip()
    source_sha = str(req.source_sha256 or "").strip().lower()

    if source_uri:
        if not is_gs_uri(source_uri):
            raise HTTPException(status_code=400, detail="source_uri must be gs://")
        suffix = Path(source_uri).suffix or ".bin"
        source_audio = create_temp_download(source_uri, suffix=suffix)
    elif inline_source_b64:
        if not GPU_WORKER_INLINE_SOURCE_ENABLED:
            raise HTTPException(status_code=400, detail="inline source payloads are disabled")
        try:
            inline_bytes = base64.b64decode(inline_source_b64, validate=True)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid inline source payload: {exc}") from exc
        inline_size = len(inline_bytes)
        if inline_size <= 0:
            raise HTTPException(status_code=400, detail="inline source payload is empty")
        if inline_size > int(GPU_WORKER_INLINE_SOURCE_MAX_BYTES):
            raise HTTPException(
                status_code=413,
                detail=(
                    "inline source payload too large "
                    f"({inline_size} bytes > {int(GPU_WORKER_INLINE_SOURCE_MAX_BYTES)} bytes)"
                ),
            )
        inferred_name = str(req.inline_source_name or req.audio_path or req.slug or req.job_id or "").strip()
        suffix = Path(inferred_name).suffix or ".bin"
        tmp_fh = tempfile.NamedTemporaryFile(prefix="inline-source-", suffix=suffix, dir="/tmp", delete=False)
        tmp_fh.write(inline_bytes)
        tmp_fh.flush()
        tmp_fh.close()
        source_audio = Path(tmp_fh.name)
    else:
        legacy_audio = _resolve_audio_path(req.audio_path)
        source_audio = legacy_audio
        source_uri = ""

    if (not source_audio.exists()) or (not source_audio.is_file()):
        raise HTTPException(status_code=400, detail=f"audio file not found: {source_audio}")
    if not _audio_path_allowed(source_audio):
        raise HTTPException(status_code=403, detail=f"audio path is not allowed: {source_audio}")
    source_size = int(source_audio.stat().st_size)
    if source_size <= 0:
        raise HTTPException(status_code=400, detail=f"audio file is empty: {source_audio}")

    actual_sha = sha256_file(source_audio)
    if source_sha and source_sha != actual_sha:
        raise HTTPException(status_code=400, detail="source sha256 mismatch")
    if not source_sha:
        source_sha = actual_sha

    download_ms = int((time.perf_counter() - t0) * 1000.0)
    return source_audio, source_uri, source_sha, download_ms


def _separate_sync(req: SeparateRequest) -> Dict[str, Any]:
    started = time.perf_counter()
    safe_slug = slugify(
        req.slug
        or Path(req.inline_source_name or req.audio_path or req.source_uri or req.job_id or "source").stem
    )
    job_id = str(req.job_id or safe_slug or f"job-{int(time.time())}").strip() or f"job-{int(time.time())}"

    source_audio, source_uri, source_sha256, download_ms = _load_source_audio(req)
    cleanup_source = bool((source_uri or str(req.inline_source_b64 or "").strip()) and source_audio.exists())
    demucs_source_audio = source_audio
    cleanup_demucs_source = False
    source_suffix = source_audio.suffix or ".audio"
    if Path(source_audio).stem != safe_slug:
        candidate = source_audio.with_name(f"{safe_slug}{source_suffix}")
        if candidate != source_audio:
            shutil.copyfile(source_audio, candidate)
            demucs_source_audio = candidate
            cleanup_demucs_source = True

    output_base, stem_dir = _resolve_stem_dir(req, safe_slug)
    output_base.mkdir(parents=True, exist_ok=True)
    stem_dir.mkdir(parents=True, exist_ok=True)

    model_version = (req.model_version or req.model or DEMUCS_MODEL).strip() or DEMUCS_MODEL
    device = _normalize_device(req.device)
    two_stems = bool(req.two_stems)
    stem_profile = _normalize_stem_profile(req.stem_profile or {})
    required_stems = _required_stems(bool(req.two_stems), req.requested_stems)

    stems_bucket = str(req.stems_bucket or STEMS_BUCKET or "").strip()
    if source_uri and not stems_bucket:
        raise HTTPException(status_code=500, detail="STEMS_BUCKET is required for gs:// worker contract")

    def _legacy_stem_paths() -> Dict[str, str]:
        return {
            f"{name}_path": str((stem_dir / f"{name}.wav").resolve())
            for name in required_stems
        }

    def _response_payload(*, cache_hit: bool, stems_uris: Dict[str, str], durations: Dict[str, int]) -> Dict[str, Any]:
        return {
            "ok": True,
            "job_id": job_id,
            "status": "succeeded",
            "cache_hit": bool(cache_hit),
            "stems_uris": stems_uris,
            "durations_ms": durations,
            # legacy compatibility
            "slug": safe_slug,
            "model": model_version,
            "device": device,
            "cached": bool(cache_hit),
            "elapsed_ms": int((time.perf_counter() - started) * 1000.0),
            "stems_dir": str(stem_dir),
            "stems": _legacy_stem_paths(),
        }

    def _remote_stems_uris() -> Dict[str, str]:
        if not stems_bucket:
            return {}
        out: Dict[str, str] = {}
        for stem_name in required_stems:
            out[stem_name] = build_stem_uri(
                stems_bucket=stems_bucket,
                source_sha256=source_sha256,
                model_version=model_version,
                stem_profile=stem_profile,
                stem_name=stem_name,
            )
        return out

    remote_uris = _remote_stems_uris()

    def _all_remote_stems_exist() -> bool:
        if not remote_uris:
            return False
        for uri in remote_uris.values():
            if not gcs_object_exists(uri):
                return False
        return True

    def _download_remote_cache_to_local() -> None:
        for stem_name, uri in remote_uris.items():
            target = stem_dir / f"{stem_name}.wav"
            gcs_download_file(uri, target)

    cancel_event = threading.Event()
    _register_active_job(job_id, cancel_event)

    split_ms = 0
    upload_ms = 0
    cache_meta_written = False

    cache_meta = _load_cache_meta(stem_dir)
    local_cache_ok = False
    if WORKER_CACHE_ENABLED and (not req.force):
        if _cache_matches(
            cache_meta,
            source_sha256=source_sha256,
            model_version=model_version,
            stem_profile=stem_profile,
            two_stems=two_stems,
            shifts=int(req.shifts),
            overlap=float(req.overlap),
            device=device,
            required_stems=required_stems,
        ):
            try:
                _validate_stems(stem_dir, required_stems, min_bytes=int(WORKER_STEM_MIN_BYTES))
                local_cache_ok = True
            except Exception:
                _cleanup_partial_stems(stem_dir, required_stems)

    def _cache_payload() -> Dict[str, Any]:
        return {
            "source_sha256": source_sha256,
            "model_version": model_version,
            "stem_profile": stem_profile,
            "two_stems": two_stems,
            "shifts": int(req.shifts),
            "overlap": float(req.overlap),
            "device": device,
            "required_stems": required_stems,
            "updated_at_epoch_ms": int(time.time() * 1000),
        }

    def _write_local_cache_meta() -> None:
        nonlocal cache_meta_written
        if (not WORKER_CACHE_ENABLED) or cache_meta_written:
            return
        _write_cache_meta(stem_dir, _cache_payload())
        cache_meta_written = True

    try:
        if local_cache_ok:
            durations = {
                "download": int(download_ms),
                "split": 0,
                "upload": 0,
                "total": int((time.perf_counter() - started) * 1000.0),
            }
            _maybe_prune_worker_cache(cache_root=output_base / model_version, preserve_dirs=[stem_dir])
            return _response_payload(cache_hit=True, stems_uris=remote_uris, durations=durations)

        if WORKER_CACHE_ENABLED and (not req.force) and remote_uris and _all_remote_stems_exist():
            _download_remote_cache_to_local()
            _validate_stems(stem_dir, required_stems, min_bytes=int(WORKER_STEM_MIN_BYTES))
            durations = {
                "download": int(download_ms),
                "split": 0,
                "upload": 0,
                "total": int((time.perf_counter() - started) * 1000.0),
            }
            _write_cache_meta(
                stem_dir,
                _cache_payload(),
            )
            cache_meta_written = True
            return _response_payload(cache_hit=True, stems_uris=remote_uris, durations=durations)

        def _run_once() -> None:
            nonlocal split_ms
            _cleanup_partial_stems(stem_dir, required_stems)
            split_t0 = time.perf_counter()
            _run_demucs(
                source_audio=demucs_source_audio,
                output_base=output_base,
                model=model_version,
                two_stems=two_stems,
                shifts=int(req.shifts),
                overlap=float(req.overlap),
                device=device,
                timeout_secs=float(WORKER_JOB_TIMEOUT_SECS),
                cancel_event=cancel_event,
                job_id=job_id,
            )
            split_ms = int((time.perf_counter() - split_t0) * 1000.0)
            _validate_stems(stem_dir, required_stems, min_bytes=int(WORKER_STEM_MIN_BYTES))

        if WORKER_SINGLEFLIGHT_ENABLED:
            sf_key = _singleflight_key(
                source_sha256=source_sha256,
                model_version=model_version,
                two_stems=two_stems,
                shifts=int(req.shifts),
                overlap=float(req.overlap),
                device=device,
                required_stems=required_stems,
                stem_profile=stem_profile,
            )
            lock = _acquire_singleflight_lock(sf_key)
            try:
                if WORKER_CACHE_ENABLED and (not req.force):
                    cache_meta = _load_cache_meta(stem_dir)
                    if _cache_matches(
                        cache_meta,
                        source_sha256=source_sha256,
                        model_version=model_version,
                        stem_profile=stem_profile,
                        two_stems=two_stems,
                        shifts=int(req.shifts),
                        overlap=float(req.overlap),
                        device=device,
                        required_stems=required_stems,
                    ):
                        try:
                            _validate_stems(stem_dir, required_stems, min_bytes=int(WORKER_STEM_MIN_BYTES))
                            durations = {
                                "download": int(download_ms),
                                "split": 0,
                                "upload": 0,
                                "total": int((time.perf_counter() - started) * 1000.0),
                            }
                            _maybe_prune_worker_cache(cache_root=output_base / model_version, preserve_dirs=[stem_dir])
                            return _response_payload(cache_hit=True, stems_uris=remote_uris, durations=durations)
                        except Exception:
                            _cleanup_partial_stems(stem_dir, required_stems)

                _run_once()
                _write_local_cache_meta()
            finally:
                _release_singleflight_lock(sf_key, lock)
        else:
            _run_once()

        stems_uris: Dict[str, str] = {}
        if remote_uris:
            upload_t0 = time.perf_counter()
            for stem_name, uri in remote_uris.items():
                gcs_upload_file(uri, stem_dir / f"{stem_name}.wav", content_type="audio/wav", if_absent=False)
                stems_uris[stem_name] = uri
            upload_ms = int((time.perf_counter() - upload_t0) * 1000.0)

        _write_local_cache_meta()
        _maybe_prune_worker_cache(cache_root=output_base / model_version, preserve_dirs=[stem_dir])

        durations = {
            "download": int(download_ms),
            "split": int(split_ms),
            "upload": int(upload_ms),
            "total": int((time.perf_counter() - started) * 1000.0),
        }
        return _response_payload(cache_hit=False, stems_uris=stems_uris, durations=durations)
    except _JobCancelled:
        return {
            "ok": True,
            "job_id": job_id,
            "status": "canceled",
            "cache_hit": False,
            "stems_uris": {},
            "durations_ms": {
                "download": int(download_ms),
                "split": int(split_ms),
                "upload": int(upload_ms),
                "total": int((time.perf_counter() - started) * 1000.0),
            },
            "error": "job canceled",
        }
    finally:
        _clear_active_job(job_id)
        if cleanup_demucs_source:
            try:
                demucs_source_audio.unlink(missing_ok=True)
            except Exception:
                pass
        if cleanup_source:
            try:
                source_audio.unlink(missing_ok=True)
            except Exception:
                pass


@app.get("/health")
def health() -> Dict[str, Any]:
    available = sorted(_available_runtime_devices())
    preferred = _preferred_runtime_device(set(available))
    return {
        "ok": True,
        "service": "mixterioso-gpu-worker",
        "demucs_available": bool(shutil.which("demucs")),
        "available_devices": available,
        "preferred_device": preferred,
        "max_concurrent": int(WORKER_MAX_CONCURRENT),
        "cache_enabled": bool(WORKER_CACHE_ENABLED),
        "singleflight_enabled": bool(WORKER_SINGLEFLIGHT_ENABLED),
        "audio_root_enforced": bool(WORKER_ENFORCE_AUDIO_ROOTS),
        "hmac_enabled": bool(_hmac_secrets()),
        "hmac_required": bool(GPU_WORKER_REQUIRE_HMAC),
        "auth_configured": bool(_auth_configured()),
        "source_bucket": SOURCE_BUCKET,
        "stems_bucket": STEMS_BUCKET,
        "cuda_probe": _runtime_cuda_probe(),
    }


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "status": "healthy"}


@app.get("/readyz")
def readyz() -> Dict[str, Any]:
    ready = bool(shutil.which("demucs")) and bool(_auth_configured())
    return {
        "ok": ready,
        "status": "ready" if ready else "not_ready",
        "demucs_available": bool(shutil.which("demucs")),
        "auth_configured": bool(_auth_configured()),
    }


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> Dict[str, Any]:
    key = str(job_id or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="job_id is required")

    with _ACTIVE_LOCK:
        cancel_event = _ACTIVE_CANCEL_EVENTS.get(key)
        proc = _ACTIVE_PROCS.get(key)

    if cancel_event is None:
        return {"ok": True, "job_id": key, "status": "not_running"}

    cancel_event.set()
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
    return {"ok": True, "job_id": key, "status": "cancel_requested"}


@app.post("/separate")
async def separate(req: SeparateRequest, request: Request) -> Dict[str, Any]:
    body = await request.body()
    authorized, reason = _authorize_request(request, body)
    if not authorized:
        raise HTTPException(status_code=401, detail=reason or "unauthorized")

    acquired = _WORKER_SEMAPHORE.acquire(timeout=float(WORKER_QUEUE_WAIT_SECS))
    if not acquired:
        raise HTTPException(status_code=429, detail="worker busy; retry later")

    try:
        return await asyncio.to_thread(_separate_sync, req)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip()[:1000] or "unknown worker error"
        logger.exception("gpu worker separation failed")
        raise HTTPException(status_code=500, detail=detail)
    finally:
        _WORKER_SEMAPHORE.release()
