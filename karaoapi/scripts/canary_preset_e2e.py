#!/usr/bin/env python3
"""Run real end-to-end canary jobs for preset song queries across vocals levels."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import json
import math
import os
import random
import re
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

TERMINAL_STATUSES = {"failed", "cancelled", "succeeded", "completed"}
SUCCESS_STATUSES = {"succeeded", "completed"}
_STRING_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')
_PRINT_LOCK = threading.Lock()
_TRANSIENT_HTTP_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
_TRANSIENT_ERROR_HINTS = ("timed out", "timeout", "temporarily", "network error", "connection reset", "remote end closed")
_T = TypeVar("_T")


def _log(message: str = "") -> None:
    with _PRINT_LOCK:
        print(message, flush=True)


def _retry_backoff_sec(attempt: int, *, base_sec: float, max_sec: float) -> float:
    exp = max(0.0, float(base_sec)) * (2.0 ** max(0, int(attempt) - 1))
    capped = min(max(0.0, float(max_sec)), exp if exp > 0 else max(0.0, float(base_sec)))
    jitter = random.uniform(0.0, max(0.0, float(base_sec)) * 0.25)
    return max(0.0, capped + jitter)


def _is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, HttpJsonError):
        if int(exc.status_code or 0) in _TRANSIENT_HTTP_STATUS:
            return True
        detail = str(exc.detail or "").lower()
        if any(hint in detail for hint in _TRANSIENT_ERROR_HINTS):
            return True
    text = str(exc).lower()
    return any(hint in text for hint in _TRANSIENT_ERROR_HINTS)


def _call_with_retries(
    fn: Callable[[], _T],
    *,
    case_label: str,
    action: str,
    max_attempts: int,
    retry_backoff_base_sec: float,
    retry_backoff_max_sec: float,
    should_retry: Callable[[Exception], bool] | None = None,
) -> _T:
    attempts = max(1, int(max_attempts))
    retry_check = should_retry or _is_transient_error
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt >= attempts or not retry_check(exc):
                raise
            sleep_sec = _retry_backoff_sec(
                attempt,
                base_sec=float(retry_backoff_base_sec),
                max_sec=float(retry_backoff_max_sec),
            )
            _log(
                "  [%s] retrying %s attempt=%d/%d backoff=%.2fs err=%s"
                % (case_label, action, attempt + 1, attempts, sleep_sec, str(exc))
            )
            time.sleep(sleep_sec)


class HttpJsonError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 0, detail: str = "") -> None:
        super().__init__(message)
        self.status_code = int(status_code or 0)
        self.detail = str(detail or "")


def _http_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    body_bytes = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body_bytes = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(url=url, method=method.upper(), headers=headers, data=body_bytes)
    try:
        with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace").strip()
    except urlerror.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace").strip()
        except Exception:
            detail = ""
        raise HttpJsonError(
            f"HTTP {exc.code} calling {url}: {detail or exc.reason}",
            status_code=int(exc.code or 0),
            detail=detail,
        ) from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"Network error calling {url}: {exc}") from exc

    if not raw:
        return {}
    out = json.loads(raw)
    if not isinstance(out, dict):
        raise RuntimeError(f"Expected JSON object from {url}, got {type(out)}")
    return out


def _unescape_ts_string(value: str) -> str:
    return (
        value.replace('\\"', '"')
        .replace("\\'", "'")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
    )


def _load_queries_from_preset_source(preset_source: Path, preset_constant: str) -> list[str]:
    if not preset_source.exists():
        raise RuntimeError(f"Preset source file not found: {preset_source}")
    text = preset_source.read_text(encoding="utf-8")
    anchor = f"const {preset_constant} = ["
    start_anchor = text.find(anchor)
    if start_anchor < 0:
        raise RuntimeError(f"Could not locate {preset_constant} in {preset_source}")
    start_bracket = text.find("[", start_anchor)
    end_array = text.find("];", start_bracket)
    if start_bracket < 0 or end_array < 0:
        raise RuntimeError(f"Could not parse array for {preset_constant} in {preset_source}")
    block = text[start_bracket : end_array + 1]
    matches = [_unescape_ts_string(m) for m in _STRING_RE.findall(block)]
    if not matches:
        raise RuntimeError(f"No preset queries found in {preset_source} for {preset_constant}")
    return matches


def _load_queries(
    *,
    inline_queries: Iterable[str],
    preset_source: Path,
    preset_constant: str,
    query_file: Path | None,
) -> list[str]:
    queries: list[str] = []
    for query in inline_queries:
        value = str(query or "").strip()
        if value:
            queries.append(value)

    queries.extend(_load_queries_from_preset_source(preset_source, preset_constant))

    if query_file is not None and query_file.exists():
        for raw in query_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            queries.append(line)

    deduped: list[str] = []
    seen = set()
    for query in queries:
        if query in seen:
            continue
        seen.add(query)
        deduped.append(query)
    return deduped


def _normalize_vocals_levels(levels: list[int]) -> list[int]:
    default_levels = [0, 10, 100]
    source = levels or default_levels
    out: list[int] = []
    seen = set()
    for raw in source:
        value = int(raw)
        if value < 0 or value > 150:
            raise RuntimeError(f"vocals level out of range (0-150): {value}")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _calc_elapsed_sec(job: dict[str, Any]) -> float | None:
    created_at = _to_float(job.get("created_at"))
    finished_at = _to_float(job.get("finished_at"))
    if created_at is None or finished_at is None:
        return None
    return max(0.0, finished_at - created_at)


def _slugify_for_key(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "query"


def _build_idempotency_key(query: str, vocals: int, case_index: int, nonce: str) -> str:
    slug = _slugify_for_key(query)[:36]
    return f"canary-e2e-{nonce}-{case_index}-v{vocals}-{slug}"


def _normalize_url(base_url: str, maybe_relative: str) -> str:
    raw = str(maybe_relative or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return urlparse.urljoin(base_url.rstrip("/") + "/", raw.lstrip("/"))


def _download_probe(url: str, *, timeout_sec: float, min_bytes: int) -> dict[str, Any]:
    req = urlrequest.Request(url=url, method="GET", headers={"Accept": "*/*"})
    with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
        headers = dict(resp.headers.items())
        chunk = resp.read(min_bytes + 1)
    return {
        "url": url,
        "status": 200,
        "content_type": str(headers.get("Content-Type", "")),
        "bytes_read": len(chunk),
        "ok": len(chunk) >= min_bytes,
    }


def _is_job_not_found_error(exc: Exception) -> bool:
    if not isinstance(exc, HttpJsonError):
        return False
    if int(getattr(exc, "status_code", 0) or 0) != 404:
        return False
    detail = (getattr(exc, "detail", "") or str(exc)).lower()
    return "job not found" in detail


def _poll_job(
    *,
    base_url: str,
    job_id: str,
    case_label: str,
    poll_interval_sec: float,
    poll_jitter_sec: float,
    max_wait_sec: float,
    http_timeout_sec: float,
    job_not_found_grace_sec: float,
    poll_transient_error_limit: int,
    retry_backoff_base_sec: float,
    retry_backoff_max_sec: float,
) -> dict[str, Any]:
    started = time.monotonic()
    not_found_since: float | None = None
    transient_error_count = 0
    while True:
        try:
            job = _http_json("GET", f"{base_url}/jobs/{job_id}", timeout_sec=http_timeout_sec)
            not_found_since = None
            transient_error_count = 0
        except Exception as exc:
            now = time.monotonic()
            if _is_job_not_found_error(exc):
                if not_found_since is None:
                    not_found_since = now
                if (now - not_found_since) < float(job_not_found_grace_sec):
                    sleep_sec = max(0.0, float(poll_interval_sec))
                    if poll_jitter_sec > 0:
                        sleep_sec += random.uniform(0.0, float(poll_jitter_sec))
                    time.sleep(sleep_sec)
                    continue
            elif _is_transient_error(exc):
                transient_error_count += 1
                if transient_error_count > max(1, int(poll_transient_error_limit)):
                    raise RuntimeError(
                        f"poll failed for job {job_id} after {transient_error_count} transient errors: {exc}"
                    ) from exc
                sleep_sec = max(
                    float(poll_interval_sec),
                    _retry_backoff_sec(
                        transient_error_count,
                        base_sec=float(retry_backoff_base_sec),
                        max_sec=float(retry_backoff_max_sec),
                    ),
                )
                if poll_jitter_sec > 0:
                    sleep_sec += random.uniform(0.0, float(poll_jitter_sec))
                _log(
                    "  [%s] transient poll error #%d/%d: %s"
                    % (
                        case_label,
                        transient_error_count,
                        max(1, int(poll_transient_error_limit)),
                        str(exc),
                    )
                )
                if (time.monotonic() - started) >= max_wait_sec:
                    raise RuntimeError(f"Timed out waiting for job {job_id} after {max_wait_sec:.1f}s") from exc
                time.sleep(max(0.0, sleep_sec))
                continue
            raise

        status = str(job.get("status") or "").strip().lower()
        stage = str(job.get("stage") or "").strip().lower()
        msg = str(job.get("last_message") or "")
        _log(f"  [{case_label}] status={status} stage={stage} msg={msg}")
        if status in TERMINAL_STATUSES:
            return job
        if (time.monotonic() - started) >= max_wait_sec:
            raise RuntimeError(f"Timed out waiting for job {job_id} after {max_wait_sec:.1f}s")
        sleep_sec = max(0.0, float(poll_interval_sec))
        if poll_jitter_sec > 0:
            sleep_sec += random.uniform(0.0, float(poll_jitter_sec))
        time.sleep(sleep_sec)


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * float(p)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return float(ordered[lower])
    frac = pos - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * frac)


def _latency_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "min_sec": None,
            "max_sec": None,
            "avg_sec": None,
            "p50_sec": None,
            "p90_sec": None,
            "p95_sec": None,
        }
    vals = [float(v) for v in values]
    count = len(vals)
    return {
        "count": count,
        "min_sec": min(vals),
        "max_sec": max(vals),
        "avg_sec": sum(vals) / count,
        "p50_sec": _percentile(vals, 0.50),
        "p90_sec": _percentile(vals, 0.90),
        "p95_sec": _percentile(vals, 0.95),
    }


def _run_case(
    *,
    case: dict[str, Any],
    base_url: str,
    force: bool,
    poll_interval_sec: float,
    poll_jitter_sec: float,
    start_jitter_sec: float,
    max_wait_sec: float,
    max_elapsed_sec: float,
    min_output_bytes: int,
    http_timeout_sec: float,
    job_not_found_grace_sec: float,
    job_not_found_recover_attempts: int,
    create_attempts: int,
    download_probe_attempts: int,
    poll_transient_error_limit: int,
    retry_backoff_base_sec: float,
    retry_backoff_max_sec: float,
) -> dict[str, Any]:
    case_index = int(case["case_index"])
    total_cases = int(case["total_cases"])
    query = str(case["query"])
    vocals = int(case["vocals"])
    idempotency_key = str(case["idempotency_key"])
    case_label = f"{case_index}/{total_cases} q={query} v={vocals}%"

    _log("")
    _log(f"[{case_label}] starting")
    if start_jitter_sec > 0:
        jitter = random.uniform(0.0, float(start_jitter_sec))
        if jitter > 0:
            _log(f"  [{case_label}] start_jitter_sec={jitter:.2f}")
            time.sleep(jitter)

    payload = {
        "query": query,
        "force": bool(force),
        "vocals": int(vocals),
        "idempotency_key": idempotency_key,
    }

    job_id = ""
    started_mono = time.monotonic()
    recovery_attempts_used = 0
    try:
        created = _call_with_retries(
            lambda: _http_json("POST", f"{base_url}/jobs", payload=payload, timeout_sec=float(http_timeout_sec)),
            case_label=case_label,
            action="create",
            max_attempts=max(1, int(create_attempts)),
            retry_backoff_base_sec=float(retry_backoff_base_sec),
            retry_backoff_max_sec=float(retry_backoff_max_sec),
        )
        job_id = str(created.get("id") or "").strip()
        if not job_id:
            raise RuntimeError(f"Missing job id in create response: {created}")
        _log(f"  [{case_label}] created job_id={job_id}")

        while True:
            try:
                final = _poll_job(
                    base_url=base_url,
                    job_id=job_id,
                    case_label=case_label,
                    poll_interval_sec=float(poll_interval_sec),
                    poll_jitter_sec=float(poll_jitter_sec),
                    max_wait_sec=float(max_wait_sec),
                    http_timeout_sec=float(http_timeout_sec),
                    job_not_found_grace_sec=float(job_not_found_grace_sec),
                    poll_transient_error_limit=max(1, int(poll_transient_error_limit)),
                    retry_backoff_base_sec=float(retry_backoff_base_sec),
                    retry_backoff_max_sec=float(retry_backoff_max_sec),
                )
                break
            except HttpJsonError as exc:
                if (not _is_job_not_found_error(exc)) or recovery_attempts_used >= int(job_not_found_recover_attempts):
                    raise
                recovery_attempts_used += 1
                sleep_sec = max(0.0, float(poll_interval_sec))
                if poll_jitter_sec > 0:
                    sleep_sec += random.uniform(0.0, float(poll_jitter_sec))
                time.sleep(sleep_sec)
                created = _call_with_retries(
                    lambda: _http_json("POST", f"{base_url}/jobs", payload=payload, timeout_sec=float(http_timeout_sec)),
                    case_label=case_label,
                    action="recreate-after-404",
                    max_attempts=max(1, int(create_attempts)),
                    retry_backoff_base_sec=float(retry_backoff_base_sec),
                    retry_backoff_max_sec=float(retry_backoff_max_sec),
                )
                job_id = str(created.get("id") or "").strip()
                if not job_id:
                    raise RuntimeError(f"Missing job id in recovery create response: {created}")
                _log(f"  [{case_label}] recreated job_id={job_id} recoveries={recovery_attempts_used}")

        wall_sec = max(0.0, time.monotonic() - started_mono)
        status = str(final.get("status") or "").strip().lower()
        elapsed_sec = _calc_elapsed_sec(final)
        error_text = str(final.get("error") or "")
        output_url = _normalize_url(base_url, str(final.get("final_output_url") or final.get("output_url") or ""))

        status_ok = status in SUCCESS_STATUSES
        latency_ok = (elapsed_sec is None) or (elapsed_sec <= float(max_elapsed_sec))
        output_probe: dict[str, Any] = {"ok": False, "url": output_url}
        if output_url and status_ok:
            try:
                output_probe = _call_with_retries(
                    lambda: _download_probe(
                        output_url,
                        timeout_sec=float(http_timeout_sec),
                        min_bytes=max(1024, int(min_output_bytes)),
                    ),
                    case_label=case_label,
                    action="download-probe",
                    max_attempts=max(1, int(download_probe_attempts)),
                    retry_backoff_base_sec=float(retry_backoff_base_sec),
                    retry_backoff_max_sec=float(retry_backoff_max_sec),
                )
            except Exception as exc:
                output_probe = {"ok": False, "url": output_url, "error": str(exc)}
        output_ok = bool(output_probe.get("ok"))

        failure_reasons: list[str] = []
        if not status_ok:
            failure_reasons.append("status")
        if not latency_ok:
            failure_reasons.append("latency")
        if not output_ok:
            failure_reasons.append("output")

        ok = bool(status_ok and latency_ok and output_ok)
        case_result: dict[str, Any] = {
            "case_index": case_index,
            "query": query,
            "vocals": int(vocals),
            "job_id": job_id,
            "status": status,
            "ok": ok,
            "elapsed_sec": elapsed_sec,
            "wall_sec": wall_sec,
            "latency_ok": latency_ok,
            "status_ok": status_ok,
            "output_probe": output_probe,
            "error": error_text,
            "stage": str(final.get("stage") or ""),
            "last_message": str(final.get("last_message") or ""),
            "recoveries": int(recovery_attempts_used),
            "failure_reasons": failure_reasons,
        }
        if isinstance(final.get("timing_breakdown"), dict):
            case_result["timing_breakdown"] = dict(final.get("timing_breakdown") or {})

        _log(
            "  [%s] result status=%s elapsed_sec=%s wall_sec=%.2f output_ok=%s ok=%s"
            % (
                case_label,
                status,
                ("%.2f" % elapsed_sec) if elapsed_sec is not None else "unknown",
                wall_sec,
                output_ok,
                ok,
            )
        )
        if failure_reasons:
            _log(f"  [{case_label}] failed_checks={','.join(failure_reasons)}")
        if error_text:
            _log(f"  [{case_label}] error={error_text}")
        return case_result
    except Exception as exc:
        wall_sec = max(0.0, time.monotonic() - started_mono)
        error_text = str(exc)
        case_result = {
            "case_index": case_index,
            "query": query,
            "vocals": int(vocals),
            "job_id": job_id,
            "status": "exception",
            "ok": False,
            "elapsed_sec": None,
            "wall_sec": wall_sec,
            "latency_ok": False,
            "status_ok": False,
            "output_probe": {"ok": False, "url": ""},
            "error": error_text,
            "stage": "",
            "last_message": "",
            "recoveries": int(recovery_attempts_used),
            "failure_reasons": ["exception"],
        }
        _log(f"  [{case_label}] exception={error_text}")
        return case_result


def _normalize_error_bucket(text: str) -> str:
    cleaned = " ".join(str(text or "").split())
    if not cleaned:
        return ""
    return cleaned[:220]


def _collect_timing_breakdown_stats(cases: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_key: dict[str, list[float]] = {}
    for case in cases:
        breakdown = case.get("timing_breakdown")
        if not isinstance(breakdown, dict):
            continue
        for key, raw_value in breakdown.items():
            if not isinstance(raw_value, (int, float)):
                continue
            # API timing_breakdown is in milliseconds.
            by_key.setdefault(str(key), []).append(float(raw_value) / 1000.0)
    return {key: _latency_stats(values) for key, values in sorted(by_key.items())}


def _score_out_of_ten(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    ratio = max(0.0, min(1.0, float(numerator) / float(denominator)))
    return round(ratio * 10.0, 2)


def _build_ratings(
    *,
    total_cases: int,
    passed_cases: int,
    output_ok_cases: int,
    unique_job_ids: int,
    nonempty_job_ids: int,
    exception_cases: int,
    elapsed_p90_sec: float | None,
    max_elapsed_sec: float,
) -> dict[str, Any]:
    reliability = _score_out_of_ten(passed_cases, max(1, total_cases))
    output_integrity = _score_out_of_ten(output_ok_cases, max(1, total_cases))
    uniqueness = _score_out_of_ten(unique_job_ids, max(1, nonempty_job_ids))
    exception_stability = 10.0 - _score_out_of_ten(exception_cases, max(1, total_cases))
    latency = 0.0
    if elapsed_p90_sec is not None and max_elapsed_sec > 0:
        latency = round(max(0.0, 10.0 * (1.0 - min(1.0, float(elapsed_p90_sec) / float(max_elapsed_sec)))), 2)
    overall = round((reliability + output_integrity + uniqueness + exception_stability + latency) / 5.0, 2)
    return {
        "overall": overall,
        "reliability": reliability,
        "output_integrity": output_integrity,
        "traffic_parallelism": uniqueness,
        "stability": round(exception_stability, 2),
        "latency": latency,
    }


def _run_cases_parallel(
    *,
    case_defs: list[dict[str, Any]],
    max_concurrency: int,
    per_query_max_concurrency: int,
    run_case_kwargs: dict[str, Any],
) -> list[dict[str, Any]]:
    if max_concurrency < 1:
        raise RuntimeError("max_concurrency must be >= 1")
    if per_query_max_concurrency < 1:
        raise RuntimeError("per_query_max_concurrency must be >= 1")

    pending = collections.deque(case_defs)
    in_flight: set[concurrent.futures.Future[dict[str, Any]]] = set()
    future_query: dict[concurrent.futures.Future[dict[str, Any]], str] = {}
    active_by_query: dict[str, int] = collections.defaultdict(int)
    results: list[dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        while pending or in_flight:
            scheduled_any = False
            scans_remaining = len(pending)
            while pending and len(in_flight) < max_concurrency and scans_remaining > 0:
                case = pending.popleft()
                query = str(case.get("query") or "")
                if active_by_query[query] >= per_query_max_concurrency:
                    pending.append(case)
                    scans_remaining -= 1
                    continue
                future = pool.submit(_run_case, case=case, **run_case_kwargs)
                in_flight.add(future)
                future_query[future] = query
                active_by_query[query] += 1
                scheduled_any = True
                scans_remaining = len(pending)

            if not in_flight:
                if pending:
                    raise RuntimeError("No cases could be scheduled; check per-query/max concurrency settings.")
                break

            if not scheduled_any and pending:
                _log("scheduler waiting for inflight case completion (per-query limit reached)")

            done, _ = concurrent.futures.wait(in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                in_flight.remove(future)
                query = future_query.pop(future, "")
                if query:
                    active_by_query[query] = max(0, active_by_query[query] - 1)
                results.append(future.result())

    return results


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run full preset-query E2E canary matrix.")
    p.add_argument(
        "--base-url",
        default=os.environ.get("KARAOAPI_BASE_URL", "https://karaoapi-ujhkqz2naa-uc.a.run.app").strip(),
        help="KaraoAPI base URL.",
    )
    p.add_argument(
        "--preset-source",
        default="karaoapp/App.tsx",
        help="Source file containing QUERY_PREFILL_BUTTONS array.",
    )
    p.add_argument(
        "--preset-constant",
        default="QUERY_PREFILL_BUTTONS",
        help="Name of the preset query constant in --preset-source.",
    )
    p.add_argument(
        "--query-file",
        default="",
        help="Optional file of extra queries to include (one per line).",
    )
    p.add_argument("--query", action="append", default=[], help="Extra inline query to include.")
    p.add_argument(
        "--vocals-level",
        action="append",
        type=int,
        default=[],
        help="Vocals percent level to run. Repeat for a custom matrix. Default: 0,10,100.",
    )
    p.add_argument("--force", dest="force", action="store_true", default=True, help="Create fresh jobs with force=true.")
    p.add_argument("--no-force", dest="force", action="store_false", help="Create jobs with force=false.")
    p.add_argument(
        "--max-concurrency",
        type=int,
        default=4,
        help="How many canary cases to execute in parallel.",
    )
    p.add_argument(
        "--per-query-max-concurrency",
        type=int,
        default=1,
        help="How many cases for the same query can run at once (default=1 for realistic traffic safety).",
    )
    p.add_argument(
        "--traffic-profile",
        choices=["real", "strict"],
        default="real",
        help="real=shuffle+jitter for traffic-like load; strict=deterministic/no jitter.",
    )
    p.add_argument(
        "--start-jitter-sec",
        type=float,
        default=1.2,
        help="Max random delay before each case create call when traffic-profile=real.",
    )
    p.add_argument(
        "--poll-jitter-sec",
        type=float,
        default=0.6,
        help="Max random delay added to polling sleeps when traffic-profile=real.",
    )
    p.add_argument(
        "--random-seed",
        type=int,
        default=0,
        help="Random seed for reproducible real-traffic shuffle/jitter patterns.",
    )
    p.add_argument("--poll-interval-sec", type=float, default=3.0, help="Polling interval.")
    p.add_argument("--max-wait-sec", type=float, default=2400.0, help="Max wait per case.")
    p.add_argument("--max-elapsed-sec", type=float, default=900.0, help="Latency gate from created_at to finished_at.")
    p.add_argument("--create-attempts", type=int, default=4, help="POST /jobs transient retry attempts.")
    p.add_argument("--download-probe-attempts", type=int, default=3, help="Output download probe retry attempts.")
    p.add_argument(
        "--poll-transient-error-limit",
        type=int,
        default=12,
        help="Consecutive transient poll errors allowed before failing a case.",
    )
    p.add_argument("--retry-backoff-base-sec", type=float, default=1.0, help="Base seconds for retry backoff.")
    p.add_argument("--retry-backoff-max-sec", type=float, default=8.0, help="Max seconds for retry backoff.")
    p.add_argument(
        "--min-success-rate",
        type=float,
        default=1.0,
        help="Minimum pass ratio required to return success (0.0-1.0).",
    )
    p.add_argument("--min-output-bytes", type=int, default=65536, help="Minimum bytes required from output URL probe.")
    p.add_argument("--http-timeout-sec", type=float, default=20.0, help="HTTP timeout.")
    p.add_argument(
        "--job-not-found-grace-sec",
        type=float,
        default=45.0,
        help="Grace window for transient 404 job-not-found while polling.",
    )
    p.add_argument(
        "--job-not-found-recover-attempts",
        type=int,
        default=2,
        help="How many times to recreate the job when polling fails with 404 job-not-found.",
    )
    p.add_argument("--report-path", default="", help="Optional JSON report output path.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    base_url = str(args.base_url or "").rstrip("/")
    if not base_url:
        raise SystemExit("base URL is required")
    if not (0.0 <= float(args.min_success_rate) <= 1.0):
        raise SystemExit("--min-success-rate must be between 0.0 and 1.0")

    query_file = Path(str(args.query_file)).resolve() if str(args.query_file).strip() else None
    preset_source = Path(str(args.preset_source)).resolve()
    vocals_levels = _normalize_vocals_levels(list(args.vocals_level or []))
    queries = _load_queries(
        inline_queries=args.query,
        preset_source=preset_source,
        preset_constant=str(args.preset_constant),
        query_file=query_file,
    )
    if not queries:
        raise SystemExit("No canary queries found after loading preset source and optional query inputs.")

    report: dict[str, Any] = {
        "base_url": base_url,
        "preset_source": str(preset_source),
        "preset_constant": str(args.preset_constant),
        "query_count": len(queries),
        "queries": queries,
        "vocals_levels": vocals_levels,
        "started_at_epoch": time.time(),
        "cases": [],
        "ok": False,
    }

    total_cases = len(queries) * len(vocals_levels)
    run_nonce = uuid.uuid4().hex[:10]
    max_concurrency = max(1, int(args.max_concurrency))
    per_query_max_concurrency = max(1, int(getattr(args, "per_query_max_concurrency", 1)))
    create_attempts = max(1, int(getattr(args, "create_attempts", 4)))
    download_probe_attempts = max(1, int(getattr(args, "download_probe_attempts", 3)))
    poll_transient_error_limit = max(1, int(getattr(args, "poll_transient_error_limit", 12)))
    retry_backoff_base_sec = max(0.0, float(getattr(args, "retry_backoff_base_sec", 1.0)))
    retry_backoff_max_sec = max(retry_backoff_base_sec, float(getattr(args, "retry_backoff_max_sec", 8.0)))
    traffic_profile = str(args.traffic_profile or "real").strip().lower()
    random_seed = int(args.random_seed or 0)
    if random_seed:
        random.seed(random_seed)

    if traffic_profile == "strict":
        start_jitter_sec = 0.0
        poll_jitter_sec = 0.0
    else:
        start_jitter_sec = max(0.0, float(args.start_jitter_sec))
        poll_jitter_sec = max(0.0, float(args.poll_jitter_sec))

    _log(f"Canary base URL: {base_url}")
    _log(f"Preset source: {preset_source}")
    _log(f"Queries loaded: {len(queries)}")
    _log(f"Vocals levels: {vocals_levels}")
    _log(f"Max concurrency: {max_concurrency}")
    _log(f"Per-query max concurrency: {per_query_max_concurrency}")
    _log(
        "Traffic profile: %s (start_jitter_sec=%.2f poll_jitter_sec=%.2f seed=%d)"
        % (traffic_profile, start_jitter_sec, poll_jitter_sec, random_seed)
    )
    _log(
        "Retries: create_attempts=%d download_probe_attempts=%d poll_transient_error_limit=%d backoff=[%.2f..%.2f]s"
        % (
            create_attempts,
            download_probe_attempts,
            poll_transient_error_limit,
            retry_backoff_base_sec,
            retry_backoff_max_sec,
        )
    )
    _log(
        "Gates: success_rate>=%.2f max_elapsed_sec<=%.1f min_output_bytes>=%d"
        % (float(args.min_success_rate), float(args.max_elapsed_sec), int(args.min_output_bytes))
    )

    report["config"] = {
        "max_concurrency": max_concurrency,
        "per_query_max_concurrency": per_query_max_concurrency,
        "traffic_profile": traffic_profile,
        "start_jitter_sec": float(start_jitter_sec),
        "poll_jitter_sec": float(poll_jitter_sec),
        "create_attempts": create_attempts,
        "download_probe_attempts": download_probe_attempts,
        "poll_transient_error_limit": poll_transient_error_limit,
        "retry_backoff_base_sec": retry_backoff_base_sec,
        "retry_backoff_max_sec": retry_backoff_max_sec,
    }

    case_defs: list[dict[str, Any]] = []
    case_index = 0
    for query in queries:
        for vocals in vocals_levels:
            case_index += 1
            case_defs.append(
                {
                    "case_index": case_index,
                    "total_cases": total_cases,
                    "query": query,
                    "vocals": int(vocals),
                    "idempotency_key": _build_idempotency_key(query, int(vocals), case_index, run_nonce),
                }
            )

    if traffic_profile == "real":
        random.shuffle(case_defs)

    report["execution_order_case_indexes"] = [int(c["case_index"]) for c in case_defs]
    run_case_kwargs = {
        "base_url": base_url,
        "force": bool(args.force),
        "poll_interval_sec": float(args.poll_interval_sec),
        "poll_jitter_sec": float(poll_jitter_sec),
        "start_jitter_sec": float(start_jitter_sec),
        "max_wait_sec": float(args.max_wait_sec),
        "max_elapsed_sec": float(args.max_elapsed_sec),
        "min_output_bytes": int(args.min_output_bytes),
        "http_timeout_sec": float(args.http_timeout_sec),
        "job_not_found_grace_sec": float(args.job_not_found_grace_sec),
        "job_not_found_recover_attempts": int(args.job_not_found_recover_attempts),
        "create_attempts": create_attempts,
        "download_probe_attempts": download_probe_attempts,
        "poll_transient_error_limit": poll_transient_error_limit,
        "retry_backoff_base_sec": retry_backoff_base_sec,
        "retry_backoff_max_sec": retry_backoff_max_sec,
    }
    case_results = _run_cases_parallel(
        case_defs=case_defs,
        max_concurrency=max_concurrency,
        per_query_max_concurrency=per_query_max_concurrency,
        run_case_kwargs=run_case_kwargs,
    )

    cases = sorted(case_results, key=lambda item: int(item.get("case_index", 0)))
    report["cases"] = cases
    passed = sum(1 for item in cases if bool(item.get("ok")))
    total = len(cases)
    success_rate = (passed / float(total)) if total else 0.0
    elapsed_values = [float(item["elapsed_sec"]) for item in cases if isinstance(item.get("elapsed_sec"), (int, float))]
    wall_values = [float(item["wall_sec"]) for item in cases if isinstance(item.get("wall_sec"), (int, float))]
    status_counts = collections.Counter(str(item.get("status") or "").strip().lower() for item in cases)
    failure_reason_counts = collections.Counter(
        reason
        for item in cases
        for reason in (item.get("failure_reasons") or [])
        if str(reason or "").strip()
    )
    output_ok_cases = sum(
        1 for item in cases if bool((item.get("output_probe") or {}).get("ok"))
    )
    recoveries_total = sum(int(item.get("recoveries") or 0) for item in cases)

    error_counts = collections.Counter(
        _normalize_error_bucket(item.get("error")) for item in cases if str(item.get("error") or "").strip()
    )
    if "" in error_counts:
        error_counts.pop("", None)

    job_id_to_cases: dict[str, list[int]] = {}
    for item in cases:
        job_id = str(item.get("job_id") or "").strip()
        if not job_id:
            continue
        job_id_to_cases.setdefault(job_id, []).append(int(item.get("case_index") or 0))
    duplicate_job_ids = {job_id: idxs for job_id, idxs in job_id_to_cases.items() if len(idxs) > 1}
    nonempty_job_ids = sum(1 for item in cases if str(item.get("job_id") or "").strip())
    unique_job_ids = len(job_id_to_cases)

    vocals_summary: dict[str, Any] = {}
    for vocals in vocals_levels:
        matches = [item for item in cases if int(item.get("vocals", -1)) == int(vocals)]
        v_passed = sum(1 for item in matches if bool(item.get("ok")))
        v_elapsed = [float(item["elapsed_sec"]) for item in matches if isinstance(item.get("elapsed_sec"), (int, float))]
        vocals_summary[str(vocals)] = {
            "cases": len(matches),
            "passed": v_passed,
            "success_rate": (v_passed / float(len(matches))) if matches else 0.0,
            "elapsed_stats": _latency_stats(v_elapsed),
        }

    query_summary: dict[str, Any] = {}
    for query in queries:
        matches = [item for item in cases if str(item.get("query")) == str(query)]
        q_passed = sum(1 for item in matches if bool(item.get("ok")))
        q_elapsed = [float(item["elapsed_sec"]) for item in matches if isinstance(item.get("elapsed_sec"), (int, float))]
        query_summary[str(query)] = {
            "cases": len(matches),
            "passed": q_passed,
            "success_rate": (q_passed / float(len(matches))) if matches else 0.0,
            "elapsed_stats": _latency_stats(q_elapsed),
        }

    elapsed_stats = _latency_stats(elapsed_values)
    ratings = _build_ratings(
        total_cases=total,
        passed_cases=passed,
        output_ok_cases=output_ok_cases,
        unique_job_ids=unique_job_ids,
        nonempty_job_ids=nonempty_job_ids,
        exception_cases=int(status_counts.get("exception", 0)),
        elapsed_p90_sec=elapsed_stats.get("p90_sec"),
        max_elapsed_sec=float(args.max_elapsed_sec),
    )

    summary = {
        "cases_total": total,
        "cases_passed": passed,
        "success_rate": success_rate,
        "elapsed_stats": elapsed_stats,
        "wall_stats": _latency_stats(wall_values),
        "status_counts": dict(status_counts),
        "failure_reason_counts": dict(failure_reason_counts),
        "error_counts": dict(error_counts),
        "output_ok_cases": output_ok_cases,
        "recoveries_total": recoveries_total,
        "recoveries_avg_per_case": (recoveries_total / float(total)) if total else 0.0,
        "duplicate_job_ids": duplicate_job_ids,
        "unique_job_ids": unique_job_ids,
        "nonempty_job_ids": nonempty_job_ids,
        "vocals_levels": vocals_summary,
        "by_query": query_summary,
        "timing_breakdown_stats_sec": _collect_timing_breakdown_stats(cases),
        "ratings_out_of_10": ratings,
        "failed_cases": [item for item in cases if not bool(item.get("ok"))],
    }
    report["summary"] = summary
    report["finished_at_epoch"] = time.time()
    report["ok"] = bool(success_rate >= float(args.min_success_rate))

    if args.report_path:
        out_path = Path(str(args.report_path)).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    _log("")
    _log("Canary summary")
    _log(f"  passed={passed}/{total}")
    _log(f"  success_rate={success_rate:.2f}")
    _log("  elapsed_stats=%s" % json.dumps(summary["elapsed_stats"], ensure_ascii=False))
    _log("  by_vocals=%s" % json.dumps(vocals_summary, ensure_ascii=False))
    _log(json.dumps(report, ensure_ascii=False))

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
