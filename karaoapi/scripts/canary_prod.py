#!/usr/bin/env python3
"""Run production canary jobs against KaraoAPI and enforce pass/fail gates."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from urllib import error as urlerror
from urllib import request as urlrequest

TERMINAL_STATUSES = {"failed", "cancelled", "succeeded", "completed"}
SUCCESS_STATUSES = {"succeeded", "completed"}
DEFAULT_USER_AGENT = os.environ.get("KARAOAPI_CANARY_USER_AGENT", "okhttp/4.12.0").strip()
DEFAULT_BASE_URL = "https://karaoapi-ujhkqz2naa-uc.a.run.app"


class HttpJsonError(RuntimeError):
    def __init__(self, code: int, url: str, detail: str):
        self.code = int(code)
        self.url = url
        self.detail = detail
        super().__init__(f"HTTP {self.code} calling {self.url}: {self.detail}")


def _default_base_url() -> str:
    return (os.environ.get("KARAOAPI_BASE_URL") or "").strip() or DEFAULT_BASE_URL


def _http_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout_sec: float = 20.0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, Any]:
    body_bytes = None
    headers = {"Accept": "application/json"}
    if user_agent:
        headers["User-Agent"] = user_agent
    if payload is not None:
        body_bytes = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(url=url, method=method.upper(), headers=headers, data=body_bytes)
    try:
        with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace").strip()
    except urlerror.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace").strip()
        except Exception:
            pass
        raise HttpJsonError(e.code, url, detail or e.reason) from e
    except urlerror.URLError as e:
        raise RuntimeError(f"Network error calling {url}: {e}") from e

    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Expected JSON object from {url}, got: {type(parsed)}")
    return parsed


def _load_queries(query_file: Path, inline_queries: Iterable[str]) -> list[str]:
    queries: list[str] = []
    for q in inline_queries:
        s = (q or "").strip()
        if s:
            queries.append(s)

    if query_file.exists():
        for raw in query_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            queries.append(line)

    deduped: list[str] = []
    seen = set()
    for q in queries:
        if q in seen:
            continue
        seen.add(q)
        deduped.append(q)
    return deduped


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


def _build_idempotency_key(query: str, idx: int) -> str:
    base = "".join(ch.lower() if ch.isalnum() else "-" for ch in query).strip("-")
    while "--" in base:
        base = base.replace("--", "-")
    if not base:
        base = "query"
    return f"canary-{int(time.time())}-{idx}-{base[:48]}"


def _poll_job(
    *,
    base_url: str,
    job_id: str,
    poll_interval_sec: float,
    max_wait_sec: float,
    http_timeout_sec: float,
    user_agent: str,
    job_not_found_grace_sec: float,
) -> dict[str, Any]:
    url = f"{base_url}/jobs/{job_id}"
    started = time.monotonic()
    while True:
        elapsed_sec = time.monotonic() - started
        try:
            job = _http_json("GET", url, timeout_sec=http_timeout_sec, user_agent=user_agent)
        except HttpJsonError as e:
            is_transient_not_found = e.code == 404 and "job not found" in e.detail.lower()
            if is_transient_not_found and elapsed_sec < job_not_found_grace_sec:
                print(
                    "  status=running stage=starting msg=Transient 404 (job not found); "
                    "retrying poll"
                )
                time.sleep(poll_interval_sec)
                continue
            raise

        status = str(job.get("status") or "")
        stage = str(job.get("stage") or "")
        msg = str(job.get("last_message") or "")
        print(f"  status={status} stage={stage} msg={msg}")
        if status in TERMINAL_STATUSES:
            return job
        if elapsed_sec >= max_wait_sec:
            raise RuntimeError(f"Timed out waiting for job {job_id} after {max_wait_sec:.1f}s")
        time.sleep(poll_interval_sec)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run KaraoAPI production canary checks.")
    p.add_argument(
        "--base-url",
        default=_default_base_url(),
        help="KaraoAPI base URL (default: env KARAOAPI_BASE_URL or production host).",
    )
    p.add_argument(
        "--query-file",
        default="karaoapi/canary/queries.txt",
        help="Path to query list file (one query per line, '#' comments allowed).",
    )
    p.add_argument(
        "--query",
        action="append",
        default=[],
        help="Inline query to include. Can be passed multiple times.",
    )
    p.add_argument("--force", dest="force", action="store_true", default=True, help="Create fresh jobs with force=true.")
    p.add_argument("--no-force", dest="force", action="store_false", help="Create jobs with force=false.")
    p.add_argument("--poll-interval-sec", type=float, default=3.0, help="Polling interval for job status.")
    p.add_argument("--max-wait-sec", type=float, default=900.0, help="Max time to wait for a job to finish.")
    p.add_argument(
        "--job-not-found-grace-sec",
        type=float,
        default=45.0,
        help="Treat initial HTTP 404 job-not-found polls as transient for this many seconds.",
    )
    p.add_argument("--max-elapsed-sec", type=float, default=240.0, help="Latency gate from created_at to finished_at.")
    p.add_argument(
        "--min-success-rate",
        type=float,
        default=1.0,
        help="Minimum success ratio required to pass (0.0-1.0). Default 1.0.",
    )
    p.add_argument("--http-timeout-sec", type=float, default=20.0, help="HTTP request timeout.")
    p.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header (default: okhttp/4.12.0; override via KARAOAPI_CANARY_USER_AGENT).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    base_url = (args.base_url or "").strip().rstrip("/")
    if not base_url:
        base_url = DEFAULT_BASE_URL

    query_file = Path(args.query_file)
    queries = _load_queries(query_file, args.query)
    if not queries:
        raise SystemExit("No canary queries found. Provide --query or a non-empty --query-file.")

    if not (0.0 <= float(args.min_success_rate) <= 1.0):
        raise SystemExit("--min-success-rate must be between 0.0 and 1.0")

    print(f"Canary base URL: {base_url}")
    print(f"Canary queries: {len(queries)}")
    print(f"Canary user-agent: {args.user_agent}")
    print(
        "Gates: success_rate>=%.2f, max_elapsed_sec<=%.1f, job_404_grace_sec<=%.1f"
        % (float(args.min_success_rate), float(args.max_elapsed_sec), float(args.job_not_found_grace_sec))
    )

    results: list[dict[str, Any]] = []
    for idx, query in enumerate(queries, start=1):
        print("")
        print(f"[{idx}/{len(queries)}] query={query}")
        idempotency_key = _build_idempotency_key(query, idx)
        job = _http_json(
            "POST",
            f"{base_url}/jobs",
            payload={
                "query": query,
                "force": bool(args.force),
                "idempotency_key": idempotency_key,
            },
            timeout_sec=float(args.http_timeout_sec),
            user_agent=args.user_agent,
        )
        job_id = str(job.get("id") or "")
        if not job_id:
            raise RuntimeError(f"Missing job id in response: {job}")
        print(f"  created job_id={job_id}")

        final = _poll_job(
            base_url=base_url,
            job_id=job_id,
            poll_interval_sec=float(args.poll_interval_sec),
            max_wait_sec=float(args.max_wait_sec),
            http_timeout_sec=float(args.http_timeout_sec),
            user_agent=args.user_agent,
            job_not_found_grace_sec=float(args.job_not_found_grace_sec),
        )

        status = str(final.get("status") or "")
        elapsed_sec = _calc_elapsed_sec(final)
        error_text = str(final.get("error") or "")
        latency_ok = (elapsed_sec is None) or (elapsed_sec <= float(args.max_elapsed_sec))
        status_ok = status in SUCCESS_STATUSES
        ok = status_ok and latency_ok

        results.append(
            {
                "query": query,
                "job_id": job_id,
                "status": status,
                "elapsed_sec": elapsed_sec,
                "ok": ok,
                "error": error_text,
            }
        )
        print(
            "  result status=%s elapsed_sec=%s ok=%s"
            % (status, ("%.2f" % elapsed_sec) if elapsed_sec is not None else "unknown", ok)
        )
        if error_text:
            print(f"  error={error_text}")

    successes = sum(1 for r in results if r["ok"])
    total = len(results)
    success_rate = successes / float(total)
    print("")
    print("Canary summary")
    print(f"  passed={successes}/{total}")
    print(f"  success_rate={success_rate:.2f}")
    print("  details=%s" % json.dumps(results, ensure_ascii=False))

    if success_rate < float(args.min_success_rate):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
