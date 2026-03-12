#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from . import step3_sync as step3
from .common import CYAN, GREEN, RED, ROOT, YELLOW, Paths, log

DEFAULT_CASES_PATH = ROOT / "tests" / "validation" / "auto_offset_benchmark_cases.json"
_AUDIO_SUFFIXES = (".mp3", ".mp4", ".m4a", ".wav", ".aac")


@dataclass(frozen=True)
class BenchmarkCase:
    slug: str
    target_offset_s: float
    notes: str = ""


def load_cases(path: Path, *, slugs: Optional[set[str]] = None) -> list[BenchmarkCase]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON list in {path}")

    wanted = {str(slug).strip() for slug in (slugs or set()) if str(slug).strip()}
    cases: list[BenchmarkCase] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("slug") or "").strip()
        if not slug:
            continue
        if wanted and slug not in wanted:
            continue
        try:
            target_offset_s = float(row.get("target_offset_s"))
        except Exception as exc:
            raise ValueError(f"Invalid target_offset_s for slug={slug!r} in {path}") from exc
        cases.append(
            BenchmarkCase(
                slug=slug,
                target_offset_s=target_offset_s,
                notes=str(row.get("notes") or "").strip(),
            )
        )
    return cases


def summarize_results(results: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(results)
    elapsed_values = [float(row["elapsed_sec"]) for row in rows if isinstance(row.get("elapsed_sec"), (int, float))]
    error_values = [float(row["abs_error_s"]) for row in rows if isinstance(row.get("abs_error_s"), (int, float))]
    completed = [row for row in rows if isinstance(row.get("applied_offset_s"), (int, float))]

    def _ratio(count: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return round(float(count) / float(total), 3)

    within_100ms = sum(1 for value in error_values if value <= 0.100)
    within_250ms = sum(1 for value in error_values if value <= 0.250)
    within_500ms = sum(1 for value in error_values if value <= 0.500)
    within_1000ms = sum(1 for value in error_values if value <= 1.000)

    return {
        "case_count": len(rows),
        "completed_case_count": len(completed),
        "mean_elapsed_sec": round(statistics.mean(elapsed_values), 3) if elapsed_values else None,
        "median_elapsed_sec": round(statistics.median(elapsed_values), 3) if elapsed_values else None,
        "max_elapsed_sec": round(max(elapsed_values), 3) if elapsed_values else None,
        "mean_abs_error_s": round(statistics.mean(error_values), 3) if error_values else None,
        "median_abs_error_s": round(statistics.median(error_values), 3) if error_values else None,
        "max_abs_error_s": round(max(error_values), 3) if error_values else None,
        "within_100ms_count": within_100ms,
        "within_100ms_ratio": _ratio(within_100ms, len(error_values)),
        "within_250ms_count": within_250ms,
        "within_250ms_ratio": _ratio(within_250ms, len(error_values)),
        "within_500ms_count": within_500ms,
        "within_500ms_ratio": _ratio(within_500ms, len(error_values)),
        "within_1000ms_count": within_1000ms,
        "within_1000ms_ratio": _ratio(within_1000ms, len(error_values)),
    }


def _find_source_audio(repo_root: Path, slug: str) -> Path:
    for suffix in _AUDIO_SUFFIXES:
        candidate = repo_root / "mp3s" / f"{slug}{suffix}"
        if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
            return candidate
    raise FileNotFoundError(f"Missing audio for {slug} in {repo_root / 'mp3s'}")


def _copy_or_symlink(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        dst.symlink_to(src)
    except Exception:
        shutil.copy2(src, dst)


def _read_offset_file(path: Path) -> Optional[float]:
    if not path.exists():
        return None
    try:
        first_line = (path.read_text(encoding="utf-8").splitlines() or [""])[0].strip()
        if not first_line:
            return None
        return float(first_line)
    except Exception:
        return None


@contextmanager
def _temporary_env(updates: dict[str, str]):
    previous: dict[str, Optional[str]] = {}
    try:
        for key, value in updates.items():
            previous[key] = os.environ.get(key)
            os.environ[key] = str(value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def _temporary_timings_dir(path: Path):
    previous = step3.TIMINGS_DIR
    step3.TIMINGS_DIR = path
    try:
        yield
    finally:
        step3.TIMINGS_DIR = previous


def benchmark_case(
    *,
    repo_root: Path,
    case: BenchmarkCase,
    accuracy_level: int,
    calibration_level: int,
    language: str,
    force_refresh: bool,
) -> dict[str, Any]:
    source_lrc = repo_root / "timings" / f"{case.slug}.lrc"
    if not source_lrc.exists():
        raise FileNotFoundError(f"Missing LRC for {case.slug} at {source_lrc}")
    source_audio = _find_source_audio(repo_root, case.slug)

    with tempfile.TemporaryDirectory(prefix=f"auto-offset-bench-{case.slug}-") as td:
        temp_root = Path(td)
        paths = Paths(root=temp_root)
        paths.ensure()

        shutil.copy2(source_lrc, paths.timings / source_lrc.name)
        _copy_or_symlink(source_audio, paths.mp3s / source_audio.name)

        started = time.perf_counter()
        error_message = ""
        try:
            with _temporary_timings_dir(paths.timings), _temporary_env(
                {
                    "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                    "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                }
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug=case.slug,
                    language=language,
                    default_enabled=True,
                    force_refresh=force_refresh,
                    accuracy_level=accuracy_level,
                    calibration_level=calibration_level,
                )
        except Exception as exc:
            error_message = str(exc).strip() or exc.__class__.__name__
        elapsed_sec = round(time.perf_counter() - started, 3)

        auto_path = paths.timings / f"{case.slug}.offset.auto"
        meta_path = paths.timings / f"{case.slug}.offset.auto.meta.json"
        applied_offset_s = _read_offset_file(auto_path)
        meta_payload: dict[str, Any] = {}
        if meta_path.exists():
            try:
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    meta_payload = payload
            except Exception:
                meta_payload = {}
        if (applied_offset_s is None) and isinstance(meta_payload.get("applied_offset_s"), (int, float)):
            applied_offset_s = float(meta_payload["applied_offset_s"])

        abs_error_s = (
            round(abs(float(applied_offset_s) - float(case.target_offset_s)), 3)
            if isinstance(applied_offset_s, (int, float))
            else None
        )
        status = str(meta_payload.get("status") or ("error" if error_message else "missing_auto_offset"))
        confidence = meta_payload.get("aggregate_confidence")
        if confidence is None:
            confidence = meta_payload.get("intro_gap_confidence")
        return {
            "slug": case.slug,
            "elapsed_sec": elapsed_sec,
            "applied_offset_s": applied_offset_s,
            "target_offset_s": round(float(case.target_offset_s), 3),
            "abs_error_s": abs_error_s,
            "status": status,
            "confidence": (round(float(confidence), 3) if isinstance(confidence, (int, float)) else None),
            "selected_samples": int(meta_payload.get("selected_samples") or 0),
            "high_conf_samples": int(meta_payload.get("high_confidence_samples") or 0),
            "mode_resolution": str(meta_payload.get("mode_resolution") or ""),
            "notes": case.notes,
            "error": error_message or None,
        }


def run_benchmarks(
    *,
    repo_root: Path,
    cases: list[BenchmarkCase],
    accuracy_level: int,
    calibration_level: int,
    language: str,
    force_refresh: bool,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for index, case in enumerate(cases, start=1):
        log(
            "BENCH",
            (
                f"[{index}/{len(cases)}] {case.slug} "
                f"(target={case.target_offset_s:+.3f}s, acc={accuracy_level}, cal={calibration_level})"
            ),
            CYAN,
        )
        result = benchmark_case(
            repo_root=repo_root,
            case=case,
            accuracy_level=accuracy_level,
            calibration_level=calibration_level,
            language=language,
            force_refresh=force_refresh,
        )
        color = GREEN if result.get("abs_error_s") is not None else RED
        error_label = (
            f"{float(result['abs_error_s']):.3f}s"
            if isinstance(result.get("abs_error_s"), (int, float))
            else "n/a"
        )
        applied_label = (
            f"{float(result['applied_offset_s']):+.3f}s"
            if isinstance(result.get("applied_offset_s"), (int, float))
            else "n/a"
        )
        log(
            "BENCH",
            (
                f"{case.slug}: elapsed={float(result['elapsed_sec']):.3f}s "
                f"auto={applied_label} error={error_label} status={result['status']}"
            ),
            color,
        )
        results.append(result)

    payload = {
        "cases": [asdict(case) for case in cases],
        "results": results,
        "summary": summarize_results(results),
        "accuracy_level": int(accuracy_level),
        "calibration_level": int(calibration_level),
        "language": language,
        "repo_root": str(repo_root),
    }
    return payload


def _print_summary(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print("")
    print("slug                                 elapsed   auto      target    abs_err   status")
    print("--------------------------------------------------------------------------------------")
    for row in payload.get("results") or []:
        slug = str(row.get("slug") or "")[:35]
        elapsed = f"{float(row.get('elapsed_sec') or 0.0):6.3f}s"
        applied = "   n/a  "
        if isinstance(row.get("applied_offset_s"), (int, float)):
            applied = f"{float(row['applied_offset_s']):+8.3f}"
        target = f"{float(row.get('target_offset_s') or 0.0):+8.3f}"
        abs_err = "   n/a  "
        if isinstance(row.get("abs_error_s"), (int, float)):
            abs_err = f"{float(row['abs_error_s']):7.3f}s"
        status = str(row.get("status") or "")
        print(f"{slug:<35} {elapsed:>8} {applied:>8} {target:>8} {abs_err:>8}   {status}")
    print("--------------------------------------------------------------------------------------")
    print(
        "summary: "
        f"mean_elapsed={summary.get('mean_elapsed_sec')}s, "
        f"max_elapsed={summary.get('max_elapsed_sec')}s, "
        f"mean_abs_error={summary.get('mean_abs_error_s')}s, "
        f"max_abs_error={summary.get('max_abs_error_s')}s, "
        f"within_250ms={summary.get('within_250ms_count')}/{summary.get('completed_case_count')}"
    )


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark auto-offset speed and accuracy.")
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES_PATH, help="JSON file of benchmark cases.")
    parser.add_argument("--slug", action="append", default=[], help="Run only the specified slug. Repeatable.")
    parser.add_argument("--accuracy-level", type=int, default=3, help="Tune-for-me accuracy level.")
    parser.add_argument("--calibration-level", type=int, default=2, help="Calibration level.")
    parser.add_argument("--language", default="auto", help="Language hint passed into auto-offset.")
    parser.add_argument("--repo-root", type=Path, default=ROOT, help="Repository root containing timings/ and mp3s/.")
    parser.add_argument("--json-output", type=Path, default=None, help="Optional path to write full JSON results.")
    parser.add_argument("--print-json", action="store_true", help="Print the full JSON payload after the table.")
    parser.add_argument("--reuse-existing", action="store_true", help="Allow cached auto-offset files inside temp workspaces.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    cases = load_cases(args.cases, slugs=set(args.slug or []))
    if not cases:
        raise SystemExit(f"No benchmark cases found in {args.cases}")

    payload = run_benchmarks(
        repo_root=args.repo_root.resolve(),
        cases=cases,
        accuracy_level=max(0, min(3, int(args.accuracy_level))),
        calibration_level=max(0, min(3, int(args.calibration_level))),
        language=str(args.language or "auto").strip() or "auto",
        force_refresh=(not bool(args.reuse_existing)),
    )
    _print_summary(payload)

    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log("BENCH", f"Wrote JSON results -> {args.json_output}", GREEN)

    if args.print_json:
        print("")
        print(json.dumps(payload, indent=2))

    error_rows = [row for row in (payload.get("results") or []) if row.get("status") in {"error", "missing_auto_offset"}]
    return 1 if error_rows else 0


if __name__ == "__main__":
    raise SystemExit(main())
