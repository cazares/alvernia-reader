"""Shared HMAC signing + replay protection for worker API calls."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Optional


DEFAULT_MAX_SKEW_SEC = 300
DEFAULT_NONCE_TTL_SEC = 900
DEFAULT_NONCE_MAX_ENTRIES = 100_000


@dataclass(frozen=True)
class SignatureInput:
    timestamp_sec: int
    nonce: str
    body_sha256: str


class NonceReplayCache:
    """In-memory nonce replay cache with TTL + bounded size pruning."""

    def __init__(self, *, ttl_sec: int = DEFAULT_NONCE_TTL_SEC, max_entries: int = DEFAULT_NONCE_MAX_ENTRIES) -> None:
        self._ttl_sec = max(30, int(ttl_sec))
        self._max_entries = max(1000, int(max_entries))
        self._lock = threading.Lock()
        self._entries: dict[str, float] = {}

    def _prune_locked(self, *, now: float) -> None:
        expired = [key for key, exp in self._entries.items() if float(exp) <= now]
        for key in expired:
            self._entries.pop(key, None)
        if len(self._entries) <= self._max_entries:
            return
        # Drop soonest-to-expire entries first.
        overflow = len(self._entries) - self._max_entries
        by_exp = sorted(self._entries.items(), key=lambda item: float(item[1]))
        for key, _exp in by_exp[:overflow]:
            self._entries.pop(key, None)

    def check_and_store(self, *, nonce_key: str, now: Optional[float] = None) -> bool:
        ts = float(now if now is not None else time.time())
        with self._lock:
            self._prune_locked(now=ts)
            if nonce_key in self._entries:
                return False
            self._entries[nonce_key] = ts + float(self._ttl_sec)
            return True


def _normalize_secret(secret: str) -> bytes:
    return str(secret or "").encode("utf-8")


def _canonical_message(data: SignatureInput) -> bytes:
    return f"v1:{int(data.timestamp_sec)}:{data.nonce}:{data.body_sha256}".encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_signature(*, secret: str, data: SignatureInput) -> str:
    key = _normalize_secret(secret)
    if not key:
        raise ValueError("secret is required")
    return hmac.new(key, _canonical_message(data), hashlib.sha256).hexdigest()


def _clean_ts(raw_ts: str) -> int:
    text = str(raw_ts or "").strip()
    if not text:
        raise ValueError("missing timestamp")
    try:
        value = int(text)
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError("invalid timestamp") from exc
    return value


def _clean_nonce(raw_nonce: str) -> str:
    nonce = str(raw_nonce or "").strip()
    if not nonce:
        raise ValueError("missing nonce")
    if len(nonce) < 8 or len(nonce) > 256:
        raise ValueError("invalid nonce length")
    return nonce


def generate_nonce() -> str:
    return secrets.token_urlsafe(18)


def build_signed_headers(
    *,
    body_bytes: bytes,
    secret: str,
    timestamp_sec: Optional[int] = None,
    nonce: Optional[str] = None,
) -> dict[str, str]:
    ts = int(timestamp_sec if timestamp_sec is not None else int(time.time()))
    clean_nonce = _clean_nonce(nonce or generate_nonce())
    body_hash = sha256_hex(body_bytes)
    sig = make_signature(
        secret=secret,
        data=SignatureInput(timestamp_sec=ts, nonce=clean_nonce, body_sha256=body_hash),
    )
    return {
        "x-ts": str(ts),
        "x-nonce": clean_nonce,
        "x-signature": sig,
        "x-body-sha256": body_hash,
    }


def _iter_nonempty(secrets_in: Iterable[str]) -> list[str]:
    out: list[str] = []
    for raw in secrets_in:
        clean = str(raw or "").strip()
        if clean:
            out.append(clean)
    return out


def verify_signed_headers(
    *,
    body_bytes: bytes,
    headers: dict[str, str],
    accepted_secrets: Iterable[str],
    replay_cache: NonceReplayCache,
    now_sec: Optional[int] = None,
    max_skew_sec: int = DEFAULT_MAX_SKEW_SEC,
) -> tuple[bool, str]:
    secrets_list = _iter_nonempty(accepted_secrets)
    if not secrets_list:
        return False, "hmac secrets not configured"

    try:
        ts = _clean_ts(headers.get("x-ts") or headers.get("X-TS") or "")
        nonce = _clean_nonce(headers.get("x-nonce") or headers.get("X-NONCE") or "")
    except ValueError as exc:
        return False, str(exc)

    provided_sig = str(headers.get("x-signature") or headers.get("X-SIGNATURE") or "").strip().lower()
    if not provided_sig:
        return False, "missing signature"

    computed_body_hash = sha256_hex(body_bytes)
    body_hash_header = str(headers.get("x-body-sha256") or headers.get("X-BODY-SHA256") or "").strip().lower()
    if body_hash_header and body_hash_header != computed_body_hash:
        return False, "body hash mismatch"

    now_int = int(now_sec if now_sec is not None else int(time.time()))
    skew = abs(now_int - int(ts))
    if skew > max(30, int(max_skew_sec)):
        return False, "signature timestamp outside replay window"

    expected_data = SignatureInput(timestamp_sec=ts, nonce=nonce, body_sha256=computed_body_hash)
    sig_ok = False
    for secret in secrets_list:
        expected = make_signature(secret=secret, data=expected_data)
        if hmac.compare_digest(expected, provided_sig):
            sig_ok = True
            break
    if not sig_ok:
        return False, "signature mismatch"

    replay_key = f"{ts}:{nonce}"
    if not replay_cache.check_and_store(nonce_key=replay_key, now=float(now_int)):
        return False, "replay detected"

    return True, ""


def load_dual_hmac_secrets(*, current_env: str, previous_env: str = "") -> list[str]:
    current = str(os.environ.get(current_env, "") or "").strip()
    previous = str(os.environ.get(previous_env, "") or "").strip() if previous_env else ""
    out = [s for s in (current, previous) if s]
    return out
