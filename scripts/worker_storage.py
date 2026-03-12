"""GCS helpers for source/stem objects used by Railway + GPU worker."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import tempfile
import atexit
import base64
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

_STORAGE_CLIENT = None
_STORAGE_IMPORT_ERROR: Optional[Exception] = None
_ADC_JSON_LOCK = threading.Lock()
_ADC_JSON_TEMP_PATH: Optional[str] = None


def _cleanup_adc_temp_file() -> None:
    global _ADC_JSON_TEMP_PATH
    path = _ADC_JSON_TEMP_PATH
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass
    _ADC_JSON_TEMP_PATH = None


def _maybe_install_adc_from_env() -> None:
    global _ADC_JSON_TEMP_PATH
    existing = str(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")).strip()
    if existing and Path(existing).exists():
        return

    raw_json = (
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        or os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        or os.environ.get("MIXTERIOSO_GCP_SERVICE_ACCOUNT_JSON")
        or ""
    ).strip()
    raw_b64 = (
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_B64")
        or os.environ.get("GCP_SERVICE_ACCOUNT_JSON_B64")
        or os.environ.get("MIXTERIOSO_GCP_SERVICE_ACCOUNT_JSON_B64")
        or ""
    ).strip()

    decoded_json = ""
    if raw_b64:
        try:
            decoded_json = base64.b64decode(raw_b64).decode("utf-8").strip()
        except Exception as exc:
            raise RuntimeError("invalid GOOGLE_APPLICATION_CREDENTIALS_B64 payload") from exc

    payload = decoded_json or raw_json
    if not payload:
        return

    try:
        parsed = json.loads(payload)
    except Exception as exc:
        raise RuntimeError("invalid service account JSON payload") from exc

    if not isinstance(parsed, dict) or str(parsed.get("type", "")).strip() != "service_account":
        raise RuntimeError("service account payload must be a JSON object with type=service_account")

    with _ADC_JSON_LOCK:
        existing = str(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")).strip()
        if existing and Path(existing).exists():
            return
        if _ADC_JSON_TEMP_PATH and Path(_ADC_JSON_TEMP_PATH).exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _ADC_JSON_TEMP_PATH
            return

        fd, tmp = tempfile.mkstemp(prefix="mixterioso-gcp-", suffix=".json")
        os.close(fd)
        p = Path(tmp)
        p.write_text(json.dumps(parsed, ensure_ascii=True, separators=(",", ":"), sort_keys=True), encoding="utf-8")
        try:
            p.chmod(0o600)
        except Exception:
            pass
        _ADC_JSON_TEMP_PATH = str(p)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _ADC_JSON_TEMP_PATH
        atexit.register(_cleanup_adc_temp_file)


def _ensure_adc_from_env_json() -> None:
    _maybe_install_adc_from_env()


def _require_storage_client():
    global _STORAGE_CLIENT, _STORAGE_IMPORT_ERROR
    if _STORAGE_CLIENT is not None:
        return _STORAGE_CLIENT
    if _STORAGE_IMPORT_ERROR is not None:
        raise RuntimeError(f"google-cloud-storage unavailable: {_STORAGE_IMPORT_ERROR}")
    try:
        from google.cloud import storage  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - import dependency failure
        _STORAGE_IMPORT_ERROR = exc
        raise RuntimeError(f"google-cloud-storage unavailable: {exc}") from exc
    _maybe_install_adc_from_env()
    _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def is_gs_uri(uri: str) -> bool:
    return str(uri or "").strip().lower().startswith("gs://")


def parse_gs_uri(uri: str) -> tuple[str, str]:
    raw = str(uri or "").strip()
    if not is_gs_uri(raw):
        raise ValueError(f"not a gs:// uri: {raw}")
    tail = raw[5:]
    if "/" not in tail:
        raise ValueError(f"invalid gs:// uri (missing object): {raw}")
    bucket, obj = tail.split("/", 1)
    bucket = bucket.strip()
    obj = obj.strip()
    if not bucket or not obj:
        raise ValueError(f"invalid gs:// uri: {raw}")
    return bucket, obj


def gs_uri(bucket: str, object_name: str) -> str:
    b = str(bucket or "").strip()
    o = str(object_name or "").lstrip("/").strip()
    if not b or not o:
        raise ValueError("bucket and object_name are required")
    return f"gs://{b}/{o}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _guess_content_type(path: Path) -> str:
    guessed, _enc = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def object_exists(uri: str) -> bool:
    bucket_name, object_name = parse_gs_uri(uri)
    client = _require_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return bool(blob.exists())


def upload_file(uri: str, local_path: Path, *, content_type: str = "", if_absent: bool = False) -> str:
    bucket_name, object_name = parse_gs_uri(uri)
    p = Path(local_path)
    if not p.exists() or not p.is_file():
        raise RuntimeError(f"local file missing: {p}")
    client = _require_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if if_absent and blob.exists():
        return uri
    blob.upload_from_filename(
        str(p),
        content_type=(content_type or _guess_content_type(p)),
        if_generation_match=(0 if if_absent else None),
    )
    return uri


def download_file(uri: str, local_path: Path) -> Path:
    bucket_name, object_name = parse_gs_uri(uri)
    client = _require_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    out = Path(local_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(out))
    return out


def create_temp_download(uri: str, *, suffix: str = "") -> Path:
    fd, tmp = tempfile.mkstemp(prefix="mixterioso-gcs-", suffix=(suffix or ""))
    os.close(fd)
    out = Path(tmp)
    try:
        return download_file(uri, out)
    except Exception:
        try:
            out.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def stem_profile_hash(stem_profile: dict[str, Any]) -> str:
    normalized = {
        "vocals": float(stem_profile.get("vocals", 100.0)),
        "bass": float(stem_profile.get("bass", 100.0)),
        "drums": float(stem_profile.get("drums", 100.0)),
        "other": float(stem_profile.get("other", 100.0)),
    }
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def source_object_uri(*, source_bucket: str, source_sha256: str, suffix: str) -> str:
    clean_sha = str(source_sha256 or "").strip().lower()
    if len(clean_sha) != 64:
        raise ValueError("source_sha256 must be 64-char hex")
    ext = str(suffix or "").strip().lower() or ".bin"
    if not ext.startswith("."):
        ext = f".{ext}"
    return gs_uri(source_bucket, f"sources/{clean_sha}{ext}")


def stems_object_uri(
    *,
    stems_bucket: str,
    source_sha256: str,
    model_version: str,
    stem_profile: dict[str, Any],
    stem_name: str,
) -> str:
    clean_sha = str(source_sha256 or "").strip().lower()
    if len(clean_sha) != 64:
        raise ValueError("source_sha256 must be 64-char hex")
    model = (str(model_version or "").strip() or "htdemucs").replace("/", "_")
    profile_hash = stem_profile_hash(stem_profile)
    clean_stem = str(stem_name or "").strip().lower()
    if not clean_stem:
        raise ValueError("stem_name is required")
    return gs_uri(stems_bucket, f"stems/{model}/{clean_sha}/{profile_hash}/{clean_stem}.wav")


def build_stems_object_uri(
    *,
    stems_bucket: str,
    source_sha256: str,
    model_version: str,
    stem_profile: dict[str, Any],
    stem_name: str,
) -> str:
    """Backward-compatible alias used by gpu_worker_app imports."""
    return stems_object_uri(
        stems_bucket=stems_bucket,
        source_sha256=source_sha256,
        model_version=model_version,
        stem_profile=stem_profile,
        stem_name=stem_name,
    )


def metadata_object_uri(
    *,
    stems_bucket: str,
    source_sha256: str,
    model_version: str,
    stem_profile: dict[str, Any],
) -> str:
    clean_sha = str(source_sha256 or "").strip().lower()
    if len(clean_sha) != 64:
        raise ValueError("source_sha256 must be 64-char hex")
    model = (str(model_version or "").strip() or "htdemucs").replace("/", "_")
    profile_hash = stem_profile_hash(stem_profile)
    return gs_uri(stems_bucket, f"stems/{model}/{clean_sha}/{profile_hash}/meta.json")


@dataclass(frozen=True)
class GCSObjectRef:
    uri: str

    @property
    def bucket(self) -> str:
        b, _o = parse_gs_uri(self.uri)
        return b

    @property
    def object_name(self) -> str:
        _b, o = parse_gs_uri(self.uri)
        return o
