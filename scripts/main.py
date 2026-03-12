#!/usr/bin/env python3
"""Mixterioso main pipeline entrypoint.

Notes
- By default, reuses existing artifacts when possible
- -f/--force forces regeneration where supported by each step
- --reset deletes cached artifacts for this slug before running
- --new preset: fastest defaults + --force --reset --yt-search-n 3
- --language defaults to auto (no need to specify for en/es)

Steps
1) step1_fetch: fetch synced LRC + MP3
2) step2_split: optional remix (demucs) OR fast center-channel vocal attenuation
3) step3_sync : generate timings CSV from LRC and estimate offset (best-effort)
4) step4_assemble: render MP4
5) step5_distribute: optional YouTube upload
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from .common import (
    IOFlags,
    Paths,
    log,
    log_timing,
    now_perf_ms,
    slugify,
    CYAN,
    GREEN,
    YELLOW,
    RED,
    RESET,
)
from .pipeline_contract import SPEED_MODE_CHOICES
from .step1_fetch import step1_fetch, YT_SEARCH_N, STEP1_SPEED_MODE_DEFAULT
from .step2_split import step2_split
from .step3_sync import step3_sync, step3_sync_lite


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Step5 (YouTube upload) is temporarily disabled by default.
# Set MIXTERIOSO_ENABLE_STEP5_UPLOAD=1 only when we intentionally re-enable it.
ENABLE_STEP5_UPLOAD = _env_bool("MIXTERIOSO_ENABLE_STEP5_UPLOAD", False)
# Fast path for vanilla videos: skip step2 full-mix materialization and let step4 read step1 audio directly.
SKIP_STEP2_FOR_VANILLA = _env_bool("MIXTERIOSO_SKIP_STEP2_FOR_VANILLA", True)
# Fast path for vanilla videos: skip step3 raw CSV; auto-offset can still run.
STEP3_LITE_FOR_VANILLA = _env_bool("MIXTERIOSO_STEP3_LITE_FOR_VANILLA", True)
FAST_STEP4_SPEED_MODES = {"extra-turbo", "ultimate-light-speed"}
DEFAULT_PIPELINE_SPEED_MODE = "ultimate-light-speed"


def _parse_cli_bool_token(value: object, *, option_name: str) -> bool:
    if isinstance(value, bool):
        return value
    token = str(value if value is not None else "").strip().lower()
    if token in {"1", "true", "yes", "on", "y"}:
        return True
    if token in {"0", "false", "no", "off", "n", ""}:
        return False
    raise ValueError(f"{option_name} expects 0/1 (or true/false); got: {value!r}")


def _looks_like_cli_bool_token(value: object) -> bool:
    try:
        _parse_cli_bool_token(value, option_name="--use-cache")
        return True
    except ValueError:
        return False


def _normalize_optional_use_cache_argv(argv: List[str]) -> List[str]:
    """
    Keep --use-cache ergonomic without stealing positional query text.

    Examples:
      --use-cache "artist title"  -> --use-cache=1 "artist title"
      --use-cache 0 "artist title" -> unchanged
      --use-cache                  -> --use-cache=1
    """
    if not argv:
        return []
    out: List[str] = []
    i = 0
    while i < len(argv):
        tok = str(argv[i] or "")
        if tok == "--use-cache":
            next_tok = argv[i + 1] if (i + 1) < len(argv) else None
            if next_tok is None:
                out.append("--use-cache=1")
                i += 1
                continue
            next_raw = str(next_tok or "")
            if next_raw.startswith("-"):
                out.append("--use-cache=1")
                i += 1
                continue
            if _looks_like_cli_bool_token(next_raw):
                out.append(tok)
                out.append(next_raw)
                i += 2
                continue
            out.append("--use-cache=1")
            i += 1
            continue
        out.append(tok)
        i += 1
    return out


def _has_cli_option(raw_argv: List[str], option_name: str) -> bool:
    opt = str(option_name or "").strip()
    if not opt:
        return False
    prefix = opt + "="
    for tok in raw_argv:
        s = str(tok or "").strip()
        if s == opt or s.startswith(prefix):
            return True
    return False


def _validate_positional_query_placement(raw_argv: List[str], query_positional: str) -> Optional[str]:
    q = str(query_positional or "").strip()
    if not q:
        return None
    if not raw_argv:
        return None
    matches = [idx for idx, tok in enumerate(raw_argv) if str(tok) == q]
    if not matches:
        return None
    last_idx = len(raw_argv) - 1
    special_ok: set[int] = set()
    # Preserve historic shorthand: --new "query" --off ...
    if len(raw_argv) >= 2 and str(raw_argv[0]) == "--new":
        special_ok.add(1)
    if any((idx in {0, last_idx}) or (idx in special_ok) for idx in matches):
        return None
    return "positional query must be first/last (or directly after --new); use --query for middle placement"


def _set_env_if_empty(name: str, value: str) -> None:
    if not str(os.environ.get(name, "")).strip():
        os.environ[name] = str(value)


def _env_truthy(name: str) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _env_falsy(name: str) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    return raw in {"0", "false", "no", "off", "n"}


def _apply_default_fast_render_env(args: argparse.Namespace) -> None:
    # Respect explicit render controls when present.
    if str(os.environ.get("KARAOKE_RENDER_PROFILE", "")).strip() or str(os.environ.get("KARAOKE_RENDER_LEVEL", "")).strip():
        return

    default_profile = str(os.environ.get("MIXTERIOSO_DEFAULT_FAST_RENDER_PROFILE", "fast") or "fast").strip() or "fast"
    _set_env_if_empty("KARAOKE_RENDER_PROFILE", default_profile)
    _set_env_if_empty("KARAOKE_NO_FASTSTART", "1")
    _set_env_if_empty("KARAOKE_TURBO_VALIDATE_DURATION", "0")
    log(
        "RENDER",
        f"Auto fast render defaults enabled (profile={os.environ.get('KARAOKE_RENDER_PROFILE','')}, faststart={'off' if os.environ.get('KARAOKE_NO_FASTSTART') == '1' else 'on'})",
        CYAN,
    )


def _apply_default_demucs_runtime_env() -> None:
    # Global speed defaults for the overwhelmingly common vocals-only case.
    _set_env_if_empty("MIXTERIOSO_AUTO_TWO_STEMS_FOR_VOCALS_ONLY", "1")
    _set_env_if_empty("MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_FIRST", "1")
    _set_env_if_empty("MIXTERIOSO_FAST_VOCALS_ONLY_FALLBACK_MIN_PCT", "50")
    _set_env_if_empty("MIXTERIOSO_STEP2_SKIP_STEMS_MIX_MP3", "1")

    # Local macOS server: default to Metal path for stem separation unless explicitly disabled.
    if sys.platform != "darwin":
        return
    if _env_falsy("MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE"):
        return
    _set_env_if_empty("MIXTERIOSO_DEMUCS_ASSUME_MPS_AVAILABLE", "1")


def _reset_slug(paths: Paths, slug: str) -> None:
    # Delete artifacts for this slug across known dirs
    patterns = [
        paths.mp3s / f"{slug}.mp3",
        paths.timings / f"{slug}.lrc",
        paths.timings / f"{slug}.csv",
        paths.timings / f"{slug}.raw.csv",
        paths.timings / f"{slug}.offset",
        paths.timings / f"{slug}.offset.auto",
        paths.timings / f"{slug}.offset.auto.meta.json",
        paths.meta / f"{slug}.step1.json",
        paths.meta / f"{slug}.step4.offsets.json",
        paths.mixes / f"{slug}.wav",
        paths.mixes / f"{slug}.mp3",
        paths.output / f"{slug}.mp4",
    ]
    for p in patterns:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

    # separated/<model>/<slug>/...
    try:
        for d in paths.separated.glob(f"*/{slug}"):
            if d.is_dir():
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass

def parse_args(argv=None):
    raw_argv = list(argv) if argv is not None else list(sys.argv[1:])
    normalized_argv = _normalize_optional_use_cache_argv(raw_argv)
    p = argparse.ArgumentParser(description="Mixterioso pipeline")
    p.add_argument(
        "query_positional",
        nargs="?",
        default="",
        help="Query shorthand positional argument (equivalent to --query)",
    )
    p.add_argument("--query", default="")
    p.add_argument(
        "--lrc-artist",
        default="",
        help="Optional lyrics artist override for step1 synced-lyrics lookup",
    )
    p.add_argument(
        "--lrc-title",
        default="",
        help="Optional lyrics title override for step1 synced-lyrics lookup",
    )
    p.add_argument(
        "--lyric-start",
        default="",
        help="Optional first-lyric-line prefix constraint for synced lyrics selection",
    )
    p.add_argument(
        "--audio-url",
        "--url",
        dest="audio_url",
        default="",
        help="Optional direct YouTube URL/ID for step1 audio source (query remains the LRC query)",
    )
    p.add_argument(
        "--audio-id",
        default="",
        help="Alias of --audio-url; provide a YouTube video ID to force step1 audio source",
    )
    p.add_argument("--language", default="auto", help="en|es|auto (default auto)")
    p.add_argument("--vocals", type=int, default=None, help="Vocal volume percent (0-150). Omit for no remix")
    p.add_argument("--bass", type=int, default=None, help="Bass volume percent (0-150)")
    p.add_argument("--drums", type=int, default=None, help="Drums volume percent (0-150)")
    p.add_argument("--other", type=int, default=None, help="Other stem volume percent (0-150)")
    p.add_argument("-f", "--force", action="store_true", help="Force regeneration where supported")
    p.add_argument("--reset", action="store_true", help="Delete cached artifacts for this slug before running")
    p.add_argument("--nuke", action="store_true", help="Aggressive delete for step1_fetch")
    p.add_argument(
        "--new",
        dest="new_preset",
        action="store_true",
        help="Preset for fast fresh runs: --force --reset --yt-search-n 3",
    )
    p.add_argument(
        "--use-cache",
        nargs="?",
        const="1",
        default="0",
        metavar="0|1",
        help="Cache-first mode. No value enables cache-first; pass 0 to disable.",
    )
    p.add_argument("--yt-search-n", type=int, default=None, help="How many YouTube candidates to try (default from env)")
    p.add_argument(
        "--duration-aware-source-match",
        action="store_true",
        help="Enable duration-aware step1 source matching for ambiguous titles.",
    )
    p.add_argument(
        "--retry-attempt",
        type=int,
        default=None,
        help="Step1 fallback tier: 1=fast fail-hard, 2=balanced retries, 3=full recovery logic",
    )
    p.add_argument(
        "--speed-mode",
        choices=list(SPEED_MODE_CHOICES),
        default=DEFAULT_PIPELINE_SPEED_MODE,
        help=argparse.SUPPRESS,
    )
    p.add_argument("--offset", "--off", type=float, default=0.0, help="Global lyric timing offset in seconds")
    p.add_argument(
        "--title-card-display",
        default="",
        help="Optional title-card text override. Use literal \\n for line breaks.",
    )
    p.add_argument(
        "--font-size-percent",
        type=float,
        default=100.0,
        help="Title-card text size percent (applies to intro card text only; default 100).",
    )
    p.add_argument(
        "--tune-for-me",
        "--enable-auto-offset",
        "--tune-offset",
        dest="enable_auto_offset",
        nargs="?",
        const=3,
        default=3,
        type=int,
        choices=[0, 1, 2, 3],
        metavar="LEVEL",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--calibration-level",
        "--calibration",
        dest="calibration_level",
        nargs="?",
        const=2,
        default=2,
        type=int,
        choices=[0, 1, 2, 3],
        metavar="LEVEL",
        help=argparse.SUPPRESS,
    )
    p.add_argument("--render-only", action="store_true", help="Skip steps 1-3 and render using existing artifacts")
    p.add_argument("--no-parallel", action="store_true", help="Disable parallel fetch in step1")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("-c", "--confirm", action="store_true", help="Prompt before overwrites where supported")
    p.add_argument("--upload", action="store_true", help="Request YouTube upload step (currently disabled by default)")
    p.add_argument(
        "--upload-privacy",
        choices=["public", "unlisted", "private"],
        default="unlisted",
        help="Step5 privacy status when --upload is enabled (default: unlisted)",
    )
    p.add_argument(
        "--upload-title",
        default="",
        help="Full Step5 YouTube title override",
    )
    p.add_argument(
        "--upload-ending",
        default="",
        help="Step5 title ending used when --upload-title is not set",
    )
    p.add_argument(
        "--upload-interactive",
        action="store_true",
        help="Run Step5 in interactive mode (prompts for title/confirmation)",
    )
    p.add_argument(
        "--upload-open-output-dir",
        action="store_true",
        help="Open output directory after Step5 upload completes",
    )
    p.add_argument(
        "--open-disabled",
        action="store_true",
        help="Disable automatic opening/playing of the generated output video",
    )
    p.add_argument(
        "--output-subdir",
        default=os.environ.get("MIXTERIOSO_OUTPUT_SUBDIR", "output/temp"),
        help="Output directory (relative to repo root, or absolute). Default: output/temp",
    )
    p.add_argument("--no-render", action="store_true", help="Skip step4 render and step5 upload")
    p.add_argument("--skip-step1", action="store_true", help="Skip step1 (download) - for uploaded audio")

    args = p.parse_args(normalized_argv if argv is not None else None)
    opt_query = str(getattr(args, "query", "") or "").strip()
    pos_query = str(getattr(args, "query_positional", "") or "").strip()
    lrc_artist = str(getattr(args, "lrc_artist", "") or "").strip()
    lrc_title = str(getattr(args, "lrc_title", "") or "").strip()
    lrc_artist_title_query = " - ".join([part for part in [lrc_artist, lrc_title] if part]).strip()

    if not opt_query and not pos_query and not lrc_artist_title_query:
        p.error("query is required (provide --query, positional query, or --lrc-artist/--lrc-title)")
    if opt_query and pos_query and opt_query != pos_query:
        p.error("query provided twice with different values (use --query or positional query)")
    if not opt_query and pos_query:
        args.query = pos_query
    elif not opt_query and lrc_artist_title_query:
        args.query = lrc_artist_title_query

    positional_query_err = _validate_positional_query_placement(raw_argv, pos_query)
    if positional_query_err:
        p.error(positional_query_err)

    try:
        args.use_cache = _parse_cli_bool_token(getattr(args, "use_cache", "0"), option_name="--use-cache")
    except ValueError as exc:
        p.error(str(exc))
    args.use_cache_explicit = bool(_has_cli_option(raw_argv, "--use-cache"))
    args.output_subdir = str(getattr(args, "output_subdir", "") or "").strip() or "output/temp"

    return args


def _elapsed_seconds(start_ms: float) -> float:
    return max(0.0, (now_perf_ms() - float(start_ms)) / 1000.0)


def _elapsed_seconds_between(start_ms: float, end_ms: float) -> float:
    return max(0.0, (float(end_ms) - float(start_ms)) / 1000.0)


def _build_step4_argv(
    slug: str,
    args: argparse.Namespace,
    *,
    prefer_step1_audio: bool = False,
    out_mp4: Optional[Path] = None,
) -> List[str]:
    argv = ["--slug", slug, "--offset", str(float(getattr(args, "offset", 0.0) or 0.0))]
    title_card_display = str(getattr(args, "title_card_display", "") or "")
    if title_card_display.strip():
        argv.extend(["--title-card-display", title_card_display])
    title_font_percent = float(getattr(args, "font_size_percent", 100.0) or 100.0)
    if abs(title_font_percent - 100.0) > 1e-6:
        argv.extend(["--font-size-percent", str(title_font_percent)])
    if out_mp4 is not None:
        argv.extend(["--out", str(out_mp4)])
    if prefer_step1_audio:
        argv.append("--prefer-step1-audio")
    return argv


def _summary_table(rows: List[Tuple[str, str, float]], total_seconds: float, total_status: str) -> str:
    all_rows = [*rows, ("total", total_status, float(total_seconds))]
    step_w = max(len("Step"), *(len(step) for step, _status, _sec in all_rows))
    status_w = max(len("Status"), *(len(status) for _step, status, _sec in all_rows))
    seconds_w = max(len("Seconds"), *(len(f"{sec:.3f}") for _step, _status, sec in all_rows))
    sep = f"+-{'-' * step_w}-+-{'-' * status_w}-+-{'-' * seconds_w}-+"

    lines = [
        sep,
        f"| {'Step':<{step_w}} | {'Status':<{status_w}} | {'Seconds':>{seconds_w}} |",
        sep,
    ]
    def _status_color(status: str) -> str:
        s = (status or "").strip().lower()
        if s == "ok":
            return GREEN
        if s in {"failed", "error"}:
            return RED
        if s in {"skipped", "disabled", "not_run"}:
            return YELLOW
        return CYAN

    for step, status, seconds in rows:
        row = f"| {step:<{step_w}} | {status:<{status_w}} | {seconds:>{seconds_w}.3f} |"
        lines.append(f"{_status_color(status)}{row}{RESET}")
    lines.extend(
        [
            sep,
            f"{_status_color(total_status)}| {'total':<{step_w}} | {total_status:<{status_w}} | {float(total_seconds):>{seconds_w}.3f} |{RESET}",
            sep,
        ]
    )
    return "\n".join(lines)


def _normalize_summary_rows(rows: List[Tuple[str, str, float]], total_seconds: float) -> List[Tuple[str, str, float]]:
    ordered_steps = ("step1", "step2", "step3", "step4", "step5")
    by_step: Dict[str, Tuple[str, float]] = {step: ("not_run", 0.0) for step in ordered_steps}
    for step, status, seconds in rows:
        if step in by_step:
            by_step[step] = (status, float(seconds))
    normalized = [(step, by_step[step][0], by_step[step][1]) for step in ordered_steps]
    accounted = sum(float(seconds) for _step, _status, seconds in normalized)
    misc_seconds = float(total_seconds) - float(accounted)
    if abs(misc_seconds) < 0.0005:
        misc_seconds = 0.0
    residual_label = "parallel" if misc_seconds < 0.0 else "misc"
    normalized.append((residual_label, "adjust", misc_seconds))
    return normalized


def _log_summary_table(rows: List[Tuple[str, str, float]], total_seconds: float, total_status: str) -> None:
    log("SUMMARY", "Runtime summary (seconds)", CYAN)
    print(_summary_table(_normalize_summary_rows(rows, total_seconds), total_seconds, total_status), flush=True)


def _read_offset_file(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        raw = (path.read_text(encoding="utf-8", errors="replace").strip() or "").splitlines()
        if not raw:
            return None
        return float(raw[0].strip())
    except Exception:
        return None


def _auto_offset_summary(paths: Paths, slug: str) -> str:
    auto_path = paths.timings / f"{slug}.offset.auto"
    manual_path = paths.timings / f"{slug}.offset"
    auto_off = _read_offset_file(auto_path)
    manual_off = _read_offset_file(manual_path)

    auto_val = float(auto_off or 0.0)
    manual_val = float(manual_off or 0.0)
    if abs(manual_val) > 1e-9:
        return (
            f"Auto offset applied: {0.0:+.3f}s "
            f"(auto file {auto_val:+.3f}s overridden by manual {manual_val:+.3f}s)"
        )
    if auto_off is None:
        return "Auto offset applied: +0.000s (no auto offset file)"
    return f"Auto offset applied: {auto_val:+.3f}s"


def _read_json_file(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _sub_offsets_summary(paths: Paths, slug: str) -> str:
    meta_path = paths.timings / f"{slug}.offset.auto.meta.json"
    payload = _read_json_file(meta_path)
    if not payload:
        return "Sub-offsets: n/a (no auto-offset sample metadata)"

    samples = payload.get("samples")
    if not isinstance(samples, list):
        return "Sub-offsets: n/a (invalid auto-offset sample metadata)"

    sample_bits: List[str] = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        if str(sample.get("status") or "").lower() != "ok":
            continue
        try:
            idx = int(sample.get("index"))
        except Exception:
            idx = len(sample_bits) + 1
        offset_val = sample.get("offset_s")
        if not isinstance(offset_val, (int, float)):
            continue
        anchor = sample.get("anchor_time_s")
        confidence = sample.get("confidence")
        piece = f"#{idx}={float(offset_val):+.3f}s"
        if isinstance(anchor, (int, float)):
            piece += f"@{float(anchor):.2f}s"
        if isinstance(confidence, (int, float)):
            piece += f"(c={float(confidence):.2f})"
        sample_bits.append(piece)

    aggregate = payload.get("aggregate_offset_s")
    aggregate_conf = payload.get("aggregate_confidence")
    status = str(payload.get("status") or "").strip() or "unknown"

    if not sample_bits:
        return f"Sub-offsets: none successful (status={status})"

    aggregate_suffix = ""
    if isinstance(aggregate, (int, float)):
        aggregate_suffix = f" | aggregate={float(aggregate):+.3f}s"
        if isinstance(aggregate_conf, (int, float)):
            aggregate_suffix += f" (c={float(aggregate_conf):.2f})"
    return "Sub-offsets: " + ", ".join(sample_bits) + aggregate_suffix


def _auto_offset_recommendation(paths: Paths, slug: str) -> Optional[str]:
    payload = _read_json_file(paths.timings / f"{slug}.offset.auto.meta.json")
    if not payload:
        return None
    status = str(payload.get("status") or "").strip().lower()
    manual_recommended = bool(payload.get("manual_offset_recommended"))
    if (status not in {"low_confidence", "no_successful_samples", "insufficient_selected_samples"}) and (not manual_recommended):
        return None

    conf = payload.get("aggregate_confidence")
    sample_count = payload.get("sample_count")
    selected = payload.get("selected_samples")
    details: List[str] = []
    if isinstance(conf, (int, float)):
        details.append(f"confidence={float(conf):.2f}")
    if isinstance(selected, int) and isinstance(sample_count, int) and sample_count >= 0:
        details.append(f"samples={int(selected)}/{int(sample_count)}")
    suffix = f" ({', '.join(details)})" if details else ""
    return (
        "Auto-calibration not reliable; manual offset recommended"
        f"{suffix}. Try rerunning with `--off` after quick eyeballing."
    )


def _final_offset_summary(paths: Paths, slug: str, cli_offset: float) -> str:
    p = paths.meta / f"{slug}.step4.offsets.json"
    payload = _read_json_file(p)
    if payload:
        payload_cli = payload.get("cli_offset_s")
        if isinstance(payload_cli, (int, float)):
            if abs(float(payload_cli) - float(cli_offset or 0.0)) > 1e-6:
                payload = None
    if payload:
        def _num(name: str) -> float:
            val = payload.get(name, 0.0)
            return float(val) if isinstance(val, (int, float)) else 0.0

        source = str(payload.get("offset_source") or "unknown")
        pre_shift = bool(payload.get("pre_shift_detected"))
        return (
            "Final applied offset: "
            f"{_num('final_applied_offset_s'):+.3f}s "
            f"(source={source}, auto={_num('auto_offset_s'):+.3f}s, "
            f"manual={_num('manual_offset_s'):+.3f}s, cli={_num('cli_offset_s'):+.3f}s, "
            f"base={_num('base_offset_s'):+.3f}s, resolved={_num('resolved_offset_s'):+.3f}s, "
            f"clamped={_num('clamped_offset_s'):+.3f}s, pre_shift_detected={'yes' if pre_shift else 'no'})"
        )

    auto_val = float(_read_offset_file(paths.timings / f"{slug}.offset.auto") or 0.0)
    manual_val = float(_read_offset_file(paths.timings / f"{slug}.offset") or 0.0)
    if abs(manual_val) > 1e-9:
        base_val = manual_val
        source = "manual"
    elif abs(auto_val) > 1e-9:
        base_val = auto_val
        source = "auto"
    else:
        base_val = 0.0
        source = "none"
    estimated = base_val + float(cli_offset or 0.0)
    return (
        "Final applied offset (estimated): "
        f"{estimated:+.3f}s "
        f"(source={source}, auto={auto_val:+.3f}s, manual={manual_val:+.3f}s, "
        f"cli={float(cli_offset or 0.0):+.3f}s, base={base_val:+.3f}s)"
    )


def _offset_summary_lines(paths: Paths, slug: str, cli_offset: float) -> List[str]:
    lines: List[str] = [
        _auto_offset_summary(paths, slug),
        _sub_offsets_summary(paths, slug),
        _final_offset_summary(paths, slug, cli_offset),
    ]
    rec = _auto_offset_recommendation(paths, slug)
    if rec:
        lines.append(rec)
    return lines


def _wait_for_output_file_ready(path: Path, *, timeout_sec: float = 4.0, poll_sec: float = 0.12) -> bool:
    """
    Poll until output file exists and size is stable across two checks.
    Helps avoid QuickTime/Finder race conditions right after ffmpeg finalize.
    """
    p = Path(path)
    deadline = time.monotonic() + max(0.5, float(timeout_sec))
    last_size = -1
    stable_hits = 0
    while time.monotonic() < deadline:
        try:
            if p.exists() and p.is_file():
                sz = int(p.stat().st_size)
                if sz > 0:
                    if sz == last_size:
                        stable_hits += 1
                    else:
                        stable_hits = 0
                    last_size = sz
                    if stable_hits >= 1:
                        return True
        except Exception:
            pass
        time.sleep(max(0.05, float(poll_sec)))
    return bool(p.exists() and p.is_file())


def _spawn_detached(cmd: List[str]) -> bool:
    try:
        subprocess.Popen(
            [str(c) for c in cmd],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        return True
    except Exception:
        return False


def _spawn_delayed_open_file(
    path: Path,
    *,
    initial_delay_seconds: float = 0.4,
    max_wait_seconds: float = 2.0,
    poll_seconds: float = 0.1,
) -> bool:
    """
    Attempt QuickTime autoplay after a short delay, re-checking file existence
    right before launch. Falls back to normal `open` when AppleScript is
    unavailable or fails.
    """
    try:
        delay = max(0.0, float(initial_delay_seconds))
    except Exception:
        delay = 0.4
    try:
        max_wait = max(0.0, float(max_wait_seconds))
    except Exception:
        max_wait = 2.0
    try:
        poll = max(0.05, float(poll_seconds))
    except Exception:
        poll = 0.1
    attempts = max(1, int(math.ceil(max_wait / poll)))
    script = (
        "sleep \"$1\"; "
        "i=0; "
        "while [ \"$i\" -lt \"$3\" ]; do "
        "  if [ -s \"$2\" ]; then "
        "    open \"$2\" >/dev/null 2>&1 || true; "
        "    if command -v osascript >/dev/null 2>&1; then "
        "      sleep 0.2; "
        "      osascript "
        "        -e 'tell application \"QuickTime Player\"' "
        "        -e 'if (count of documents) > 0 then play front document' "
        "        -e 'activate' "
        "        -e 'end tell' "
        "        >/dev/null 2>&1 || true; "
        "    fi; "
        "    break; "
        "  fi; "
        "  i=$((i+1)); "
        "  sleep \"$4\"; "
        "done"
    )
    return _spawn_detached(
        [
            "/bin/sh",
            "-c",
            script,
            "mixterioso-open",
            f"{delay:.3f}",
            str(path),
            str(int(attempts)),
            f"{poll:.3f}",
        ]
    )


def _maybe_auto_open_output_video(paths: Paths, slug: str, args: argparse.Namespace, *, invoked_direct_cli: bool) -> None:
    if not invoked_direct_cli:
        return
    if bool(getattr(args, "open_disabled", False)):
        log("OPEN", "Auto-open skipped (--open-disabled)", CYAN)
        return
    if bool(getattr(args, "no_render", False)):
        return
    if _env_truthy("MIX_NO_OPEN") or _env_falsy("MIX_AUTO_OPEN") or _env_falsy("MIXTERIOSO_AUTO_OPEN_OUTPUT"):
        log("OPEN", "Auto-open skipped (env disabled)", CYAN)
        return
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return

    output_path = paths.output / f"{slug}.mp4"
    if not _wait_for_output_file_ready(output_path):
        return
    if shutil.which("open") is None:
        return

    # Use default app and delayed re-check (non-blocking) for robustness.
    opened = _spawn_delayed_open_file(output_path, initial_delay_seconds=0.4)

    if opened:
        log("OPEN", f"Auto-opened output: {output_path}", CYAN)


def main(argv=None) -> int:
    pipeline_t0 = now_perf_ms()
    pipeline_end_ms: Optional[float] = None
    invoked_direct_cli = (argv is None)
    raw_argv = list(argv) if argv is not None else list(sys.argv[1:])
    args = parse_args(raw_argv)
    summary_rows: List[Tuple[str, str, float]] = []
    total_status = "failed"

    if bool(getattr(args, "new_preset", False)):
        args.force = True
        args.reset = True
        if args.yt_search_n is None:
            args.yt_search_n = 3
    if "--speed-mode" in raw_argv:
        log("SPEED", "Ignoring deprecated --speed-mode flag; fastest defaults are always enabled", YELLOW)
    args.speed_mode = DEFAULT_PIPELINE_SPEED_MODE
    cache_explicit_off = bool(getattr(args, "use_cache_explicit", False) and (not bool(getattr(args, "use_cache", False))))
    force_explicit = bool(_has_cli_option(raw_argv, "--force") or _has_cli_option(raw_argv, "-f"))
    if bool(getattr(args, "use_cache", False)):
        args.force = False
        args.reset = False
        args.nuke = False
    elif cache_explicit_off:
        # Explicit --use-cache 0 means "cold run": bypass all fast reuse paths.
        args.force = True
        args.reset = True
        args.nuke = True
        log("CACHE", "Explicit --use-cache 0: forcing cold run (force/reset/nuke + no step1 cache reuse)", YELLOW)
    cache_first_mode = bool(getattr(args, "use_cache", False))
    # Cache-safe default:
    # - --use-cache / --use-cache 1 => keep step1 caches enabled
    # - --use-cache 0 => explicit cold run (force/reset/nuke + no cache)
    # - no --use-cache flag => avoid over-caching by disabling step1 runtime/disk caches
    no_cache_mode = (not cache_first_mode)
    if no_cache_mode and (not cache_explicit_off):
        log("CACHE", "Cache-safe mode: step1 runtime/disk caches disabled (enable with --use-cache)", YELLOW)
    _apply_default_fast_render_env(args)
    _apply_default_demucs_runtime_env()

    query = args.query
    slug = slugify(query)
    forced_audio_source = (args.audio_url or args.audio_id or "").strip()

    # Step4/5 resolve output dir at import-time via this env var.
    os.environ["MIXTERIOSO_OUTPUT_SUBDIR"] = str(getattr(args, "output_subdir", "output/temp") or "output/temp")
    paths = Paths(ROOT, output_subdir=str(getattr(args, "output_subdir", "output/temp") or "output/temp"))
    paths.ensure()

    flags = IOFlags(force=bool(args.force), confirm=bool(args.confirm), dry_run=bool(args.dry_run))
    step2_flags = IOFlags(
        force=bool(args.force) and (force_explicit or (not cache_explicit_off)),
        confirm=bool(args.confirm),
        dry_run=bool(args.dry_run),
    )

    if args.reset:
        log("RESET", f"Deleting cached artifacts for slug={slug}", YELLOW)
        _reset_slug(paths, slug)

    if args.upload and not ENABLE_STEP5_UPLOAD:
        log("UPLOAD", "Step5 upload is temporarily disabled; ignoring --upload", YELLOW)

    try:
        if args.render_only:
            vocals = args.vocals if hasattr(args, "vocals") and args.vocals is not None else 100
            bass = args.bass if hasattr(args, "bass") and args.bass is not None else 100
            drums = args.drums if hasattr(args, "drums") and args.drums is not None else 100
            other = args.other if hasattr(args, "other") and args.other is not None else 100
            need_stems = any(abs(float(v) - 100.0) > 1e-6 for v in (vocals, bass, drums, other))

            summary_rows.append(("step1", "skipped", 0.0))
            if need_stems:
                log("STEP2", "Render-only with stem levels requested; rebuilding step2 stems mix", YELLOW)
                t0 = now_perf_ms()
                try:
                    step2_split(
                        paths,
                        slug=slug,
                        mix_mode="stems",
                        vocals=vocals,
                        bass=bass,
                        drums=drums,
                        other=other,
                        flags=step2_flags,
                    )
                    summary_rows.append(("step2", "ok", _elapsed_seconds(t0)))
                except Exception:
                    summary_rows.append(("step2", "failed", _elapsed_seconds(t0)))
                    raise
                finally:
                    log_timing("pipeline", "step2", t0, color=CYAN)
            else:
                summary_rows.append(("step2", "skipped", 0.0))
            tune_level = int(getattr(args, "enable_auto_offset", 0) or 0)
            calibration_level = int(getattr(args, "calibration_level", 0) or 0)
            run_explicit_auto_offset = (tune_level > 0) or (calibration_level > 0)
            if run_explicit_auto_offset:
                if calibration_level > 0:
                    log("STEP3", f"Running calibration auto-offset pass (level={calibration_level})", YELLOW)
                else:
                    log("STEP3", "Running explicit Whisper auto-offset tune pass", YELLOW)
                t0 = now_perf_ms()
                try:
                    step3_sync(
                        paths,
                        slug,
                        flags,
                        language=args.language,
                        write_raw_csv=False,
                        run_auto_offset=True,
                        auto_offset_default_enabled=True,
                        auto_offset_force_refresh=(not cache_first_mode),
                        auto_offset_accuracy=tune_level,
                        auto_offset_calibration_level=calibration_level,
                        cli_offset_hint=float(getattr(args, "offset", 0.0) or 0.0),
                    )
                    summary_rows.append(("step3", "ok", _elapsed_seconds(t0)))
                except Exception:
                    summary_rows.append(("step3", "failed", _elapsed_seconds(t0)))
                    raise
                finally:
                    log_timing("pipeline", "step3", t0, color=CYAN)
            else:
                summary_rows.append(("step3", "skipped", 0.0))
            if not args.no_render:
                from . import step4_assemble as step4
                t0 = now_perf_ms()
                try:
                    step4.main(
                        _build_step4_argv(
                            slug,
                            args,
                            prefer_step1_audio=(not need_stems),
                            out_mp4=(paths.output / f"{slug}.mp4"),
                        )
                    )
                    summary_rows.append(("step4", "ok", _elapsed_seconds(t0)))
                except Exception:
                    summary_rows.append(("step4", "failed", _elapsed_seconds(t0)))
                    raise
                finally:
                    log_timing("pipeline", "step4", t0, color=CYAN)
            else:
                summary_rows.append(("step4", "skipped", 0.0))
            summary_rows.append(("step5", "skipped", 0.0))
            pipeline_end_ms = now_perf_ms()
            total_status = "ok"
            _maybe_auto_open_output_video(paths, slug, args, invoked_direct_cli=invoked_direct_cli)
            return 0

        # Step 1
        if not args.skip_step1:
            t0 = now_perf_ms()
            try:
                if forced_audio_source:
                    log("STEP1", "Using forced audio source from --audio-url/--url/--audio-id", YELLOW)
                step1_fetch(
                    query=query,
                    lrc_artist=str(args.lrc_artist or ""),
                    lrc_title=str(args.lrc_title or ""),
                    lyric_start=str(args.lyric_start or ""),
                    slug=slug,
                    force=bool(args.force),
                    reset=bool(args.reset),
                    nuke=bool(args.nuke),
                    yt_search_n=int(args.yt_search_n) if args.yt_search_n is not None else YT_SEARCH_N,
                    parallel=not bool(args.no_parallel),
                    retry_attempt=(int(args.retry_attempt) if args.retry_attempt is not None else None),
                    speed_mode=str(args.speed_mode or ""),
                    audio_source=forced_audio_source,
                    disable_cache=no_cache_mode,
                    cache_first=cache_first_mode,
                    duration_aware_source_match=bool(args.duration_aware_source_match),
                )
                summary_rows.append(("step1", "ok", _elapsed_seconds(t0)))
            except Exception:
                summary_rows.append(("step1", "failed", _elapsed_seconds(t0)))
                raise
            finally:
                log_timing("pipeline", "step1", t0, color=CYAN)
        else:
            log("STEP1", "Skipping step1 (using uploaded audio)", CYAN)
            summary_rows.append(("step1", "skipped", 0.0))

        vocals = args.vocals if hasattr(args, "vocals") and args.vocals is not None else 100
        bass = args.bass if hasattr(args, "bass") and args.bass is not None else 100
        drums = args.drums if hasattr(args, "drums") and args.drums is not None else 100
        other = args.other if hasattr(args, "other") and args.other is not None else 100
        mix_mode = args.mix_mode if hasattr(args, "mix_mode") else None
        mix_mode_norm = str(mix_mode or "full").strip().lower()
        need_stems = any(abs(float(v) - 100.0) > 1e-6 for v in (vocals, bass, drums, other))

        can_skip_step2 = (
            SKIP_STEP2_FOR_VANILLA
            and (not bool(args.skip_step1))
            and (not need_stems)
            and mix_mode_norm in {"", "full"}
        )

        # Step 2 + Step 3
        # - step3 can start as soon as step1 is done (it only needs step1 artifacts)
        # - step2 runs independently from step3 (both consume step1 outputs)
        # - step4 begins after dependencies are ready (step2 audio + step3 timings)
        if can_skip_step2:
            log("STEP2", "Skipping step2 for vanilla fast path (step4 will use step1 audio)", YELLOW)
            summary_rows.append(("step2", "skipped", 0.0))

            t0 = now_perf_ms()
            try:
                tune_level = int(getattr(args, "enable_auto_offset", 0) or 0)
                calibration_level = int(getattr(args, "calibration_level", 0) or 0)
                run_explicit_auto_offset = (tune_level > 0) or (calibration_level > 0)
                use_step3_lite = bool(can_skip_step2 and STEP3_LITE_FOR_VANILLA and (not run_explicit_auto_offset))
                if use_step3_lite:
                    log("STEP3", "Using step3 lite fast path for vanilla pipeline", YELLOW)
                    step3_sync_lite(
                        paths,
                        slug,
                        flags,
                        language=args.language,
                        cli_offset_hint=float(getattr(args, "offset", 0.0) or 0.0),
                        run_smart_micro_offset=True,
                    )
                elif run_explicit_auto_offset:
                    if calibration_level > 0:
                        log("STEP3", f"Running calibration auto-offset pass (level={calibration_level})", YELLOW)
                    else:
                        log("STEP3", "Running explicit Whisper auto-offset tune pass", YELLOW)
                    step3_sync(
                        paths,
                        slug,
                        flags,
                        language=args.language,
                        write_raw_csv=False,
                        run_auto_offset=True,
                        auto_offset_default_enabled=True,
                        auto_offset_force_refresh=(not cache_first_mode),
                        auto_offset_accuracy=tune_level,
                        auto_offset_calibration_level=calibration_level,
                        cli_offset_hint=float(getattr(args, "offset", 0.0) or 0.0),
                    )
                else:
                    step3_sync(
                        paths,
                        slug,
                        flags,
                        language=args.language,
                        cli_offset_hint=float(getattr(args, "offset", 0.0) or 0.0),
                        run_smart_micro_offset=True,
                    )
                summary_rows.append(("step3", "ok", _elapsed_seconds(t0)))
            except Exception:
                summary_rows.append(("step3", "failed", _elapsed_seconds(t0)))
                raise
            finally:
                log_timing("pipeline", "step3", t0, color=CYAN)
        else:
            log("PIPELINE", "Running step2 (audio) and step3 (lyrics timing) in parallel", CYAN)
            step2_t0 = now_perf_ms()
            step3_t0 = now_perf_ms()
            step2_status = "failed"
            step3_status = "failed"
            step2_elapsed = 0.0
            step3_elapsed = 0.0

            def _run_step2() -> None:
                nonlocal step2_status, step2_elapsed
                try:
                    step2_split(
                        paths,
                        slug=slug,
                        mix_mode=mix_mode,
                        vocals=vocals,
                        bass=bass,
                        drums=drums,
                        other=other,
                        flags=step2_flags,
                    )
                    step2_status = "ok"
                except Exception:
                    step2_status = "failed"
                    raise
                finally:
                    step2_elapsed = _elapsed_seconds(step2_t0)
                    log_timing("pipeline", "step2", step2_t0, color=CYAN)

            def _run_step3() -> None:
                nonlocal step3_status, step3_elapsed
                try:
                    tune_level = int(getattr(args, "enable_auto_offset", 0) or 0)
                    calibration_level = int(getattr(args, "calibration_level", 0) or 0)
                    run_explicit_auto_offset = (tune_level > 0) or (calibration_level > 0)
                    step3_sync(
                        paths,
                        slug,
                        flags,
                        language=args.language,
                        run_auto_offset=run_explicit_auto_offset,
                        auto_offset_default_enabled=run_explicit_auto_offset,
                        auto_offset_force_refresh=(run_explicit_auto_offset and (not cache_first_mode)),
                        auto_offset_accuracy=(tune_level if run_explicit_auto_offset else 1),
                        auto_offset_calibration_level=(calibration_level if run_explicit_auto_offset else 0),
                        cli_offset_hint=float(getattr(args, "offset", 0.0) or 0.0),
                        run_smart_micro_offset=(not run_explicit_auto_offset),
                    )
                    step3_status = "ok"
                except Exception:
                    step3_status = "failed"
                    raise
                finally:
                    step3_elapsed = _elapsed_seconds(step3_t0)
                    log_timing("pipeline", "step3", step3_t0, color=CYAN)

            step2_error = None
            step3_error = None
            with ThreadPoolExecutor(max_workers=2) as executor:
                fut2 = executor.submit(_run_step2)
                fut3 = executor.submit(_run_step3)
                try:
                    fut2.result()
                except Exception as exc:
                    step2_error = exc
                try:
                    fut3.result()
                except Exception as exc:
                    step3_error = exc

            summary_rows.append(("step2", step2_status, step2_elapsed))
            summary_rows.append(("step3", step3_status, step3_elapsed))

            if step2_error is not None:
                raise step2_error
            if step3_error is not None:
                raise step3_error

        if not args.no_render:
            # Step 4
            from . import step4_assemble as step4

            t0 = now_perf_ms()
            try:
                # step4_assemble.main is CLI-oriented; pass argv list
                step4.main(
                    _build_step4_argv(
                        slug,
                        args,
                        prefer_step1_audio=can_skip_step2,
                        out_mp4=(paths.output / f"{slug}.mp4"),
                    )
                )
                summary_rows.append(("step4", "ok", _elapsed_seconds(t0)))
            except Exception:
                summary_rows.append(("step4", "failed", _elapsed_seconds(t0)))
                raise
            finally:
                log_timing("pipeline", "step4", t0, color=CYAN)

            # Step 5 (optional)
            if args.upload and ENABLE_STEP5_UPLOAD:
                t0 = now_perf_ms()
                try:
                    from . import step5_distribute as step5

                    step5_args = ["--slug", slug, "--privacy", args.upload_privacy]
                    if args.upload_title:
                        step5_args += ["--title", str(args.upload_title)]
                    if args.upload_ending:
                        step5_args += ["--ending", str(args.upload_ending)]
                    if args.upload_open_output_dir:
                        step5_args += ["--open-output-dir"]
                    if args.upload_interactive:
                        pass
                    else:
                        step5_args += ["--non-interactive", "--yes"]

                    rc = int(step5.main(step5_args))
                    if rc != 0:
                        raise RuntimeError(f"step5 upload failed with exit code {rc}")
                    summary_rows.append(("step5", "ok", _elapsed_seconds(t0)))
                except Exception:
                    summary_rows.append(("step5", "failed", _elapsed_seconds(t0)))
                    raise
                finally:
                    log_timing("pipeline", "step5", t0, color=CYAN)
            elif args.upload and not ENABLE_STEP5_UPLOAD:
                summary_rows.append(("step5", "disabled", 0.0))
            else:
                summary_rows.append(("step5", "skipped", 0.0))
        else:
            summary_rows.append(("step4", "skipped", 0.0))
            summary_rows.append(("step5", "skipped", 0.0))

        log("DONE", f"Output: {paths.output / (slug + '.mp4')}", GREEN)
        pipeline_end_ms = now_perf_ms()
        _maybe_auto_open_output_video(paths, slug, args, invoked_direct_cli=invoked_direct_cli)
        total_status = "ok"
        return 0
    except Exception as exc:
        pipeline_end_ms = now_perf_ms()
        total_status = "failed"
        log("ERROR", str(exc), RED)
        return 1
    finally:
        end_ms = float(pipeline_end_ms) if pipeline_end_ms is not None else now_perf_ms()
        total_seconds = _elapsed_seconds_between(pipeline_t0, end_ms)
        total_ms = max(0.0, end_ms - float(pipeline_t0))
        log("TIMING", f"step=pipeline part=total elapsed_ms={total_ms:.1f}", CYAN)
        _log_summary_table(summary_rows, total_seconds, total_status)
        try:
            for line in _offset_summary_lines(paths, slug, float(getattr(args, "offset", 0.0) or 0.0)):
                log("SUMMARY", line, CYAN)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
