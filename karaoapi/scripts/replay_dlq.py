#!/usr/bin/env python3
"""Replay failed async jobs from a DLQ JSONL file.

This is a queue-integration scaffold for future async mode:
- Input rows are expected to be JSON objects with at least `query`.
- Optional fields are passed through to POST /jobs.
- Writes a replay report with success/failure reasons.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest


def _http_json(method: str, url: str, payload: dict[str, Any], timeout_sec: float) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        url=url,
        method=method.upper(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=body,
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace").strip()
    except urlerror.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace").strip()
        except Exception:
            detail = ""
        raise RuntimeError(f"HTTP {exc.code}: {detail or exc.reason}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"network error: {exc}") from exc

    if not raw:
        return {}
    out = json.loads(raw)
    if not isinstance(out, dict):
        raise RuntimeError(f"unexpected JSON type: {type(out)}")
    return out


def _build_payload(row: dict[str, Any], replay_index: int) -> dict[str, Any]:
    query = str(row.get("query") or "").strip()
    if not query:
        raise ValueError("missing query")
    payload = {
        "query": query,
        "idempotency_key": str(row.get("idempotency_key") or f"dlq-replay-{int(time.time())}-{replay_index}"),
    }
    passthrough_keys = (
        "force",
        "reset",
        "render_only",
        "preview",
        "vocals",
        "bass",
        "drums",
        "other",
        "language",
        "yt_search_n",
    )
    for key in passthrough_keys:
        if key in row:
            payload[key] = row[key]
    return payload


def _iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception as exc:
            raise RuntimeError(f"invalid JSON on line {line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"line {line_no}: expected object")
        rows.append(payload)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay DLQ jobs to KaraoAPI /jobs.")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("KARAOAPI_BASE_URL", "").strip(),
        help="KaraoAPI base URL (or env KARAOAPI_BASE_URL).",
    )
    parser.add_argument("--dlq-jsonl", required=True, help="Path to DLQ JSONL file.")
    parser.add_argument("--timeout-sec", type=float, default=20.0, help="HTTP timeout per replay request.")
    parser.add_argument("--max-items", type=int, default=100, help="Max rows to replay in one run.")
    parser.add_argument("--report-path", default="", help="Optional report JSON path.")
    args = parser.parse_args(argv or sys.argv[1:])

    base_url = str(args.base_url or "").rstrip("/")
    if not base_url:
        raise SystemExit("base URL is required (--base-url or KARAOAPI_BASE_URL)")

    dlq_path = Path(args.dlq_jsonl).resolve()
    if not dlq_path.exists():
        raise SystemExit(f"DLQ file not found: {dlq_path}")

    rows = _iter_jsonl(dlq_path)
    if not rows:
        print("No DLQ items found.")
        return 0

    max_items = max(1, int(args.max_items))
    report: dict[str, Any] = {
        "base_url": base_url,
        "dlq_path": str(dlq_path),
        "total_rows": len(rows),
        "processed_rows": 0,
        "replayed": 0,
        "failed": 0,
        "results": [],
    }

    for idx, row in enumerate(rows[:max_items], start=1):
        item: dict[str, Any] = {"index": idx, "query": str(row.get("query") or "")}
        try:
            payload = _build_payload(row, idx)
            resp = _http_json("POST", f"{base_url}/jobs", payload, timeout_sec=float(args.timeout_sec))
            item["ok"] = True
            item["job_id"] = str(resp.get("id") or "")
            report["replayed"] += 1
        except Exception as exc:
            item["ok"] = False
            item["error"] = str(exc)
            report["failed"] += 1
        report["processed_rows"] += 1
        report["results"].append(item)

    if args.report_path:
        report_path = Path(args.report_path).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False))
    return 0 if int(report["failed"]) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
