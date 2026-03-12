#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common import slugify
from scripts.sync_quality import run_sync_quality_checks


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_queries(path: Path) -> List[str]:
    queries: List[str] = []
    seen = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = str(raw or "").strip()
        if not line or line.startswith("#"):
            continue
        if line in seen:
            continue
        seen.add(line)
        queries.append(line)
    return queries


def _extract_error_line(log_text: str) -> str:
    lines = [line.strip() for line in str(log_text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    for line in reversed(lines):
        if "RuntimeError:" in line:
            return line.split("RuntimeError:", 1)[1].strip()
    for line in reversed(lines):
        if "[ERROR]" in line:
            idx = line.find("[ERROR]")
            return line[idx + len("[ERROR]") :].strip()
    return lines[-1]


def _classify_pipeline_error(log_text: str) -> str:
    low = str(log_text or "").lower()
    if not low:
        return "unknown"
    if "no synced lyrics found" in low or "no lyrics found" in low or "failed to fetch lyrics" in low:
        return "lyrics_missing"
    if "sign in to confirm" in low or "captcha" in low or "forbidden" in low or "429" in low or "bot" in low:
        return "source_blocked_or_rate_limited"
    if "download failed" in low or "yt-dlp" in low:
        return "download_error"
    if "timed out" in low or "timeout" in low:
        return "timeout"
    if "step2" in low and ("error" in low or "failed" in low):
        return "step2_error"
    if "step3" in low and ("error" in low or "failed" in low):
        return "step3_error"
    if "step4" in low and ("error" in low or "failed" in low):
        return "step4_error"
    if "ffmpeg" in low:
        return "ffmpeg_error"
    return "pipeline_error"


def _parse_env_pairs(pairs: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in pairs:
        token = str(item or "").strip()
        if not token or "=" not in token:
            continue
        k, v = token.split("=", 1)
        k = k.strip()
        if not k:
            continue
        out[k] = v
    return out


def _load_resume_results(path: Path, resume: bool) -> List[Dict[str, Any]]:
    if not resume or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("results")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _write_payload(
    *,
    path: Path,
    campaign_id: str,
    started_at_utc: str,
    params: Dict[str, Any],
    results: List[Dict[str, Any]],
) -> None:
    summary = _build_summary(results)
    payload = {
        "campaign_id": campaign_id,
        "started_at_utc": started_at_utc,
        "updated_at_utc": _now_iso(),
        "params": params,
        "summary": summary,
        "results": results,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    by_case_status: Counter[str] = Counter()
    pipeline_error_class: Counter[str] = Counter()
    sync_reason: Counter[str] = Counter()
    visual_reason: Counter[str] = Counter()
    pass_rates: List[float] = []

    for row in results:
        case_status = str(row.get("case_status") or "unknown")
        by_case_status[case_status] += 1
        pipe = row.get("pipeline") or {}
        pclass = str(pipe.get("error_class") or "")
        if pclass:
            pipeline_error_class[pclass] += 1
        sync = row.get("sync") or {}
        sreason = str(sync.get("failure_class") or "")
        if sreason:
            sync_reason[sreason] += 1
        vreason = str(sync.get("visual_reason") or "")
        if vreason:
            visual_reason[vreason] += 1
        pr = sync.get("visual_pass_rate")
        if isinstance(pr, (int, float)):
            pass_rates.append(float(pr))

    avg_pr = round(sum(pass_rates) / len(pass_rates), 3) if pass_rates else None
    return {
        "total_cases": total,
        "case_status_counts": dict(by_case_status),
        "pipeline_error_class_counts": dict(pipeline_error_class),
        "sync_failure_class_counts": dict(sync_reason),
        "visual_reason_counts": dict(visual_reason),
        "visual_pass_rate_avg": avg_pr,
    }


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run large sync-breaker campaign and cluster failures.")
    p.add_argument("--query-file", required=True, help="Path to newline-delimited queries.")
    p.add_argument("--limit", type=int, default=100, help="Max number of queries to run.")
    p.add_argument("--campaign-id", default="", help="Optional campaign identifier.")
    p.add_argument("--out", default="", help="Output JSON report path.")
    p.add_argument("--logs-dir", default="", help="Directory for per-case logs.")
    p.add_argument("--resume", action="store_true", help="Resume from existing --out results.")
    p.add_argument("--yt-search-n", type=int, default=12)
    p.add_argument("--no-force", action="store_true", help="Do not pass --force to pipeline.")
    p.add_argument("--no-reset", action="store_true", help="Do not pass --reset to pipeline.")
    p.add_argument("--sleep-sec", type=float, default=0.0, help="Optional delay between cases.")
    p.add_argument("--render-profile", default="fast", help="KARAOKE_RENDER_PROFILE")
    p.add_argument("--env", action="append", default=[], help="Extra env vars (KEY=VALUE).")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    query_file = Path(str(args.query_file)).resolve()
    if not query_file.exists():
        raise SystemExit(f"Query file not found: {query_file}")

    campaign_id = str(args.campaign_id or "").strip() or f"sync-breaker-100-{int(time.time())}"
    out_path = Path(str(args.out or "")).resolve() if str(args.out or "").strip() else (ROOT / "meta" / f"{campaign_id}.json")
    logs_dir = Path(str(args.logs_dir or "")).resolve() if str(args.logs_dir or "").strip() else (ROOT / "meta" / "campaign_logs" / campaign_id)
    logs_dir.mkdir(parents=True, exist_ok=True)

    queries = _read_queries(query_file)
    if int(args.limit) > 0:
        queries = queries[: int(args.limit)]
    if not queries:
        raise SystemExit("No queries to run.")

    existing = _load_resume_results(out_path, bool(args.resume))
    done_queries = {str(row.get("query") or "") for row in existing}

    force = not bool(args.no_force)
    reset = not bool(args.no_reset)

    env = dict(os.environ)
    env["MIXTERIOSO_ENABLE_STEP5_UPLOAD"] = "0"
    env["KARAOKE_RENDER_PROFILE"] = str(args.render_profile)
    env.update(_parse_env_pairs(list(args.env)))

    started_at_utc = _now_iso()
    params = {
        "query_file": str(query_file),
        "limit": int(args.limit),
        "force": force,
        "reset": reset,
        "yt_search_n": int(args.yt_search_n),
        "render_profile": str(args.render_profile),
        "resume": bool(args.resume),
        "env_overrides": _parse_env_pairs(list(args.env)),
        "logs_dir": str(logs_dir),
    }

    results: List[Dict[str, Any]] = list(existing)
    pending = [q for q in queries if q not in done_queries]
    total = len(queries)

    print(f"Campaign: {campaign_id}")
    print(f"Query file: {query_file}")
    print(f"Total queries: {total} (pending: {len(pending)}, resumed: {len(existing)})")
    print(f"Output: {out_path}")
    print(f"Logs dir: {logs_dir}")

    pybin = sys.executable or "python3"
    for idx, query in enumerate(queries, start=1):
        if query in done_queries:
            continue

        slug = slugify(query)
        safe_slug = f"{idx:03d}_{slug}"
        pipeline_log_path = logs_dir / f"{safe_slug}.pipeline.log"
        sync_log_path = logs_dir / f"{safe_slug}.sync.log"

        cmd = [
            pybin,
            "-m",
            "scripts.main",
            "--query",
            query,
            "--yt-search-n",
            str(int(args.yt_search_n)),
        ]
        if force:
            cmd.append("--force")
        if reset:
            cmd.append("--reset")

        case_t0 = time.time()
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        pipeline_elapsed = round(time.time() - case_t0, 3)
        pipeline_out = str(proc.stdout or "")
        pipeline_log_path.write_text(pipeline_out, encoding="utf-8")

        row: Dict[str, Any] = {
            "index": idx,
            "query": query,
            "slug": slug,
            "pipeline": {
                "status": "ok" if int(proc.returncode) == 0 else "failed",
                "return_code": int(proc.returncode),
                "elapsed_sec": pipeline_elapsed,
                "log_path": str(pipeline_log_path),
            },
            "case_status": "pipeline_failed",
            "finished_at_utc": _now_iso(),
        }

        if int(proc.returncode) != 0:
            err_line = _extract_error_line(pipeline_out)
            err_class = _classify_pipeline_error(pipeline_out)
            row["pipeline"]["error_class"] = err_class
            row["pipeline"]["error_message"] = err_line
            results.append(row)
            _write_payload(
                path=out_path,
                campaign_id=campaign_id,
                started_at_utc=started_at_utc,
                params=params,
                results=results,
            )
            print(
                f"[{idx}/{total}] FAIL pipeline {query} | class={err_class} | elapsed={pipeline_elapsed:.1f}s | msg={err_line[:140]}"
            )
            if float(args.sleep_sec) > 0.0:
                time.sleep(float(args.sleep_sec))
            continue

        sync_stdout = io.StringIO()
        with redirect_stdout(sync_stdout):
            payload = run_sync_quality_checks(
                slug=slug,
                local_video_path=ROOT / "output" / f"{slug}.mp4",
                run_pre_upload=True,
                run_post_upload=False,
                timings_csv_path=ROOT / "timings" / f"{slug}.csv",
                lrc_path=ROOT / "timings" / f"{slug}.lrc",
                language="auto",
            )
        sync_log_path.write_text(sync_stdout.getvalue(), encoding="utf-8")

        pre = payload.get("pre_upload") or {}
        checks = pre.get("checks") or {}
        vis = checks.get("visual_sync") or {}
        ao = checks.get("audio_offset") or {}

        sync_status = str(vis.get("status") or "unknown")
        sync_reason = str(vis.get("reason") or "")
        is_visual_pass_like = sync_status in {"passed", "skipped"}
        row["sync"] = {
            "overall_passed": payload.get("overall_passed"),
            "elapsed_sec": float(payload.get("elapsed_sec") or 0.0),
            "visual_status": sync_status,
            "visual_pass_rate": vis.get("pass_rate"),
            "visual_reason": sync_reason,
            "audio_offset_status": ao.get("status"),
            "audio_offset_reason": ao.get("reason"),
            "failure_class": "" if is_visual_pass_like else (sync_reason or "sync_failed"),
            "log_path": str(sync_log_path),
        }
        row["case_status"] = "passed" if is_visual_pass_like else "sync_failed"
        row["finished_at_utc"] = _now_iso()
        results.append(row)

        _write_payload(
            path=out_path,
            campaign_id=campaign_id,
            started_at_utc=started_at_utc,
            params=params,
            results=results,
        )
        print(
            f"[{idx}/{total}] {row['case_status'].upper()} {query} | pipeline={pipeline_elapsed:.1f}s | "
            f"visual={sync_status} pr={vis.get('pass_rate')} reason={sync_reason or '-'}"
        )

        if float(args.sleep_sec) > 0.0:
            time.sleep(float(args.sleep_sec))

    _write_payload(
        path=out_path,
        campaign_id=campaign_id,
        started_at_utc=started_at_utc,
        params=params,
        results=results,
    )
    summary = _build_summary(results)
    print("Campaign complete.")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
