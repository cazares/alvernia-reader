#!/usr/bin/env python3
"""
Run step1-only query probes with timing output.

This is intended for fast query experimentation without running step2/3/4/5.
Each probe launches `python -m scripts.step1_fetch` and captures timing lines.

Examples:
  python3 scripts/step1_query_probe.py \
    --query "let it be" \
    --query "the beatles let it be" \
    --query "john frusciante god" \
    --query "john frusciante the past recedes" \
    --runs 1 \
    --json-out meta/step1_probe_latest.json

  python3 scripts/step1_query_probe.py --query-file karaoapi/canary/queries.txt --runs 2
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parent.parent
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TIMING_RE = re.compile(
    r"\[TIMING\]\s+step=step1\s+part=([a-z0-9_]+)\s+elapsed_ms=([0-9]+(?:\.[0-9]+)?)",
    re.IGNORECASE,
)


def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text or "")


def _slugify(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "song"


def _load_queries(query_file: Path | None, inline_queries: Iterable[str]) -> List[str]:
    queries: List[str] = []
    for raw in inline_queries:
        value = (raw or "").strip()
        if value:
            queries.append(value)

    if query_file is not None and query_file.exists():
        for raw in query_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            queries.append(line)

    deduped: List[str] = []
    seen = set()
    for query in queries:
        if query in seen:
            continue
        seen.add(query)
        deduped.append(query)
    return deduped


def _parse_timings(text: str) -> Dict[str, float]:
    timings: Dict[str, float] = {}
    clean = _strip_ansi(text)
    for line in clean.splitlines():
        match = TIMING_RE.search(line)
        if not match:
            continue
        part = str(match.group(1) or "").strip().lower()
        if not part:
            continue
        try:
            timings[part] = float(match.group(2))
        except Exception:
            continue
    return timings


def _extract_error(text: str, fallback: str = "") -> str:
    clean = _strip_ansi(text)
    lines = [line.strip() for line in clean.splitlines() if line.strip()]
    for line in reversed(lines):
        if "RuntimeError:" in line:
            return line.split("RuntimeError:", 1)[1].strip() or fallback
    for line in reversed(lines):
        if "[ERROR]" in line:
            idx = line.find("[ERROR]")
            msg = line[idx + len("[ERROR]") :].strip()
            if msg:
                return msg
    for line in reversed(lines):
        if line.lower().startswith("traceback"):
            continue
        return line
    return fallback


def _classify_error(error_text: str) -> str:
    low = (error_text or "").lower()
    if not low:
        return ""
    if "modulenotfounderror" in low or "no module named" in low:
        return "dependency_missing"
    if "no synced lyrics" in low or "no lyrics found" in low:
        return "lyrics_missing"
    if "no audio found" in low or "step1 audio missing" in low or "missing source audio" in low:
        return "audio_missing"
    if "timed out" in low or "timeout" in low:
        return "timeout"
    if (
        "sign in to confirm" in low
        or "captcha" in low
        or "cookie" in low
        or "forbidden" in low
        or "bot" in low
        or "429" in low
    ):
        return "bot_or_auth"
    if "yt-dlp" in low or "download failed" in low:
        return "download_error"
    return "other"


def _python_candidates(explicit: str) -> List[str]:
    if explicit and explicit.strip() and explicit.strip().lower() != "auto":
        return [explicit.strip()]

    out: List[str] = []
    preferred = [
        str((ROOT / ".venv" / "bin" / "python").resolve()),
        sys.executable,
        shutil.which("python3") or "",
        shutil.which("python") or "",
    ]
    for candidate in preferred:
        value = (candidate or "").strip()
        if not value:
            continue
        if value in out:
            continue
        out.append(value)
    return out


def _python_can_run_step1(python_bin: str) -> bool:
    try:
        probe = subprocess.run(
            [
                python_bin,
                "-c",
                "import requests; import scripts.step1_fetch",  # noqa: S603
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=8.0,
        )
        return int(probe.returncode) == 0
    except Exception:
        return False


def _resolve_python_bin(explicit: str) -> str:
    candidates = _python_candidates(explicit)
    if not candidates:
        raise RuntimeError("No python interpreter candidates found.")
    for candidate in candidates:
        if _python_can_run_step1(candidate):
            return candidate
    raise RuntimeError(
        "Could not find a python interpreter that can import requests and scripts.step1_fetch. "
        "Use --python-bin /path/to/python."
    )


def _build_env(args: argparse.Namespace) -> Dict[str, str]:
    env = dict(os.environ)
    if args.cold:
        # Cold-path mode: reduce local cache effects for search/audio reuse.
        env["MIXTERIOSO_YTDLP_SEARCH_CACHE_TTL_SEC"] = "0"
        env["MIXTERIOSO_YTDLP_SEARCH_DISK_CACHE_TTL_SEC"] = "0"
        env["MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_ENABLED"] = "0"
    for item in args.env:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            continue
        env[key] = value
    return env


def _format_ms(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value / 1000.0:.2f}s"


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _run_case(
    *,
    python_bin: str,
    query: str,
    run_index: int,
    case_index: int,
    case_total: int,
    args: argparse.Namespace,
    env: Dict[str, str],
) -> Dict[str, Any]:
    slug = _slugify(query)
    if args.unique_slug:
        slug = f"{slug}__r{run_index}"

    cmd = [
        python_bin,
        "-m",
        "scripts.step1_fetch",
        "--query",
        query,
        "--slug",
        slug,
        "--yt-search-n",
        str(int(args.yt_search_n)),
    ]
    if args.force:
        cmd.append("--force")
    if args.reset:
        cmd.append("--reset")
    if args.nuke:
        cmd.append("--nuke")
    if args.no_parallel:
        cmd.append("--no-parallel")

    print(
        f"[{case_index}/{case_total}] run={run_index} query={query!r} "
        f"(force={int(args.force)} reset={int(args.reset)} cold={int(args.cold)})",
        flush=True,
    )
    started = time.perf_counter()
    stdout = ""
    stderr = ""
    timed_out = False
    rc = 0
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=float(args.timeout_sec),
        )
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        rc = int(completed.returncode)
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        rc = 124
        stdout = _coerce_text(exc.stdout)
        stderr = _coerce_text(exc.stderr)

    elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
    combined = f"{stdout}\n{stderr}"
    timings = _parse_timings(combined)
    step1_total_ms = timings.get("total")
    error_text = ""
    if rc != 0:
        if timed_out:
            error_text = f"step1 probe timed out after {args.timeout_sec:.1f}s"
        else:
            error_text = _extract_error(combined, fallback=f"step1 exited rc={rc}")
    error_category = _classify_error(error_text)
    status = "ok" if rc == 0 else "failed"

    print(
        "  -> status=%s wall=%s step1_total=%s lyrics=%s audio=%s err=%s"
        % (
            status,
            _format_ms(elapsed_ms),
            _format_ms(step1_total_ms),
            _format_ms(timings.get("fetch_lyrics")),
            _format_ms(timings.get("download_audio")),
            error_category or "-",
        ),
        flush=True,
    )

    if args.log_tail_lines > 0:
        tail_lines = _strip_ansi(combined).splitlines()[-int(args.log_tail_lines) :]
        if tail_lines:
            print("  log tail:", flush=True)
            for line in tail_lines:
                print(f"    {line}", flush=True)

    return {
        "query": query,
        "run": int(run_index),
        "slug": slug,
        "status": status,
        "returncode": rc,
        "timed_out": bool(timed_out),
        "elapsed_ms": elapsed_ms,
        "step1_total_ms": step1_total_ms,
        "fetch_lyrics_ms": timings.get("fetch_lyrics"),
        "download_audio_ms": timings.get("download_audio"),
        "write_metadata_ms": timings.get("write_metadata"),
        "error": error_text,
        "error_category": error_category,
        "command": cmd,
    }


def _parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run step1-only query probes with timing output.")
    ap.add_argument("--query", action="append", default=[], help="Query text (repeatable).")
    ap.add_argument("--query-file", default="", help="Optional query file (one query per line, # comments allowed).")
    ap.add_argument("--runs", type=int, default=1, help="Runs per query.")
    ap.add_argument("--yt-search-n", type=int, default=8, help="Passed to step1_fetch --yt-search-n.")
    ap.add_argument("--timeout-sec", type=float, default=180.0, help="Per-query timeout.")
    ap.add_argument("--json-out", default="", help="Optional JSON output path.")
    ap.add_argument("--log-tail-lines", type=int, default=0, help="Print last N log lines per query.")
    ap.add_argument(
        "--python-bin",
        default="auto",
        help="Python interpreter used to run step1_fetch (default: auto-detect).",
    )
    ap.add_argument("--env", action="append", default=[], help="Extra env override: KEY=VALUE (repeatable).")
    ap.add_argument("--nuke", action="store_true", help="Pass --nuke to step1_fetch.")
    ap.add_argument("--no-parallel", action="store_true", help="Pass --no-parallel to step1_fetch.")
    ap.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Return non-zero exit code if any probe fails.",
    )

    ap.set_defaults(force=True, reset=True, cold=True, unique_slug=True)
    ap.add_argument("--force", dest="force", action="store_true", help="Force fresh step1 artifacts (default).")
    ap.add_argument("--no-force", dest="force", action="store_false", help="Allow cache reuse in step1 artifacts.")
    ap.add_argument("--reset", dest="reset", action="store_true", help="Reset slug artifacts before each probe (default).")
    ap.add_argument("--no-reset", dest="reset", action="store_false", help="Do not reset slug artifacts before probe.")
    ap.add_argument("--cold", dest="cold", action="store_true", help="Disable local step1 search/audio caches (default).")
    ap.add_argument("--warm", dest="cold", action="store_false", help="Keep local step1 caches enabled.")
    ap.add_argument(
        "--unique-slug",
        dest="unique_slug",
        action="store_true",
        help="Use unique slug suffix per run to reduce artifact reuse (default).",
    )
    ap.add_argument(
        "--shared-slug",
        dest="unique_slug",
        action="store_false",
        help="Reuse the same slug across runs for each query.",
    )
    args = ap.parse_args(argv)
    args.runs = max(1, int(args.runs))
    args.log_tail_lines = max(0, int(args.log_tail_lines))
    args.timeout_sec = max(5.0, float(args.timeout_sec))
    return args


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    python_bin = _resolve_python_bin(str(args.python_bin or "auto"))
    query_file = Path(args.query_file).expanduser() if args.query_file else None
    queries = _load_queries(query_file, args.query)
    if not queries:
        raise SystemExit("No queries supplied. Use --query and/or --query-file.")

    env = _build_env(args)
    total_cases = len(queries) * int(args.runs)
    print(
        "Step1 probe starting: queries=%d runs=%d total_cases=%d cold=%s force=%s reset=%s parallel=%s python=%s"
        % (
            len(queries),
            int(args.runs),
            total_cases,
            bool(args.cold),
            bool(args.force),
            bool(args.reset),
            not bool(args.no_parallel),
            python_bin,
        ),
        flush=True,
    )

    results: List[Dict[str, Any]] = []
    case_index = 0
    for run_index in range(1, int(args.runs) + 1):
        for query in queries:
            case_index += 1
            result = _run_case(
                python_bin=python_bin,
                query=query,
                run_index=run_index,
                case_index=case_index,
                case_total=total_cases,
                args=args,
                env=env,
            )
            results.append(result)

    ok_count = sum(1 for item in results if item.get("status") == "ok")
    fail_count = len(results) - ok_count
    elapsed_values = [float(item.get("elapsed_ms") or 0.0) for item in results if item.get("elapsed_ms") is not None]
    median_ms = statistics.median(elapsed_values) if elapsed_values else 0.0

    print("")
    print("Step1 probe summary")
    print("  passed=%d/%d" % (ok_count, len(results)))
    print("  failed=%d" % fail_count)
    print("  median_wall=%s" % _format_ms(median_ms))

    if fail_count:
        grouped: Dict[str, int] = {}
        for item in results:
            if item.get("status") == "ok":
                continue
            cat = str(item.get("error_category") or "other")
            grouped[cat] = grouped.get(cat, 0) + 1
        print("  failure_categories=%s" % json.dumps(grouped, sort_keys=True))

    if args.json_out:
        out_path = Path(args.json_out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at_epoch_sec": time.time(),
            "args": {
                "runs": int(args.runs),
                "cold": bool(args.cold),
                "force": bool(args.force),
                "reset": bool(args.reset),
                "nuke": bool(args.nuke),
                "no_parallel": bool(args.no_parallel),
                "timeout_sec": float(args.timeout_sec),
            },
            "results": results,
        }
        out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print("  wrote_json=%s" % out_path)

    if args.fail_on_error and fail_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
