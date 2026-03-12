#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .common import CYAN, GREEN, RED, YELLOW, log, slugify
from .pipeline_contract import SPEED_MODE_CHOICES

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class AutoProfile:
    name: str
    env: dict[str, str]


AUTO_PROFILES: tuple[AutoProfile, ...] = (
    AutoProfile(
        name="balanced-default",
        env={},
    ),
    AutoProfile(
        name="flex-buckets",
        env={
            "KARAOKE_AUTO_OFFSET_BUCKET_MIN_LINES": "3",
            "KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE": "0.34",
            "KARAOKE_AUTO_OFFSET_MIN_SAMPLE_CONFIDENCE": "0.40",
        },
    ),
    AutoProfile(
        name="wide-net",
        env={
            "KARAOKE_AUTO_OFFSET_BUCKET_MIN_LINES": "2",
            "KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE": "0.30",
            "KARAOKE_AUTO_OFFSET_MIN_SAMPLE_CONFIDENCE": "0.35",
            "KARAOKE_AUTO_OFFSET_MIN_CONFIDENCE": "0.40",
        },
    ),
)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Mobile-guided tuning UX in CLI: run max auto tune first, retry with "
            "different algorithms, then manual offset loop if needed."
        )
    )
    p.add_argument("query", help='Song query, e.g. "Red Hot Chili Peppers - Higher Ground"')
    p.add_argument(
        "--max-algorithms",
        type=int,
        default=3,
        help="Max number of auto-tuning algorithms to try before manual fallback (default: 3)",
    )
    p.add_argument(
        "--speed-mode",
        choices=list(SPEED_MODE_CHOICES),
        default="extra-turbo",
        help="Pipeline speed mode (default: extra-turbo)",
    )
    p.add_argument("--yt-search-n", type=int, default=3, help="YouTube candidate count (default: 3)")
    p.add_argument("--vocals", type=int, default=None)
    p.add_argument("--bass", type=int, default=None)
    p.add_argument("--drums", type=int, default=None)
    p.add_argument("--other", type=int, default=None)
    p.add_argument("--no-render", action="store_true", help="Pass through --no-render to scripts.main")
    p.add_argument("--no-new", dest="new", action="store_false", help="Disable --new preset")
    p.set_defaults(new=True)
    return p.parse_args(argv)


def _offset_meta_paths(slug: str) -> tuple[Path, Path]:
    timings = ROOT / "timings"
    return timings / f"{slug}.offset.auto.meta.json", timings / f"{slug}.offset.auto"


def _load_auto_result(slug: str) -> dict[str, Any]:
    meta_path, auto_path = _offset_meta_paths(slug)
    payload: dict[str, Any] = {}
    if meta_path.exists():
        try:
            maybe = json.loads(meta_path.read_text(encoding="utf-8", errors="replace"))
            if isinstance(maybe, dict):
                payload = maybe
        except Exception:
            payload = {}

    auto_val = 0.0
    auto_exists = False
    if auto_path.exists():
        auto_exists = True
        try:
            raw = (auto_path.read_text(encoding="utf-8", errors="replace").strip() or "").splitlines()
            if raw:
                auto_val = float(raw[0].strip())
        except Exception:
            auto_val = 0.0

    status = str(payload.get("status") or ("unknown" if auto_exists else "no_auto_file"))
    selected = payload.get("selected_samples")
    sample_count = payload.get("sample_count")
    conf = payload.get("aggregate_confidence")

    return {
        "status": status,
        "auto_offset_s": float(auto_val),
        "auto_exists": bool(auto_exists),
        "selected_samples": (int(selected) if isinstance(selected, int) else None),
        "sample_count": (int(sample_count) if isinstance(sample_count, int) else None),
        "aggregate_confidence": (float(conf) if isinstance(conf, (int, float)) else None),
        "manual_offset_recommended": bool(payload.get("manual_offset_recommended")),
    }


def _build_main_cmd(
    args: argparse.Namespace,
    *,
    off: float,
    tune_level: int,
    calibration_level: int,
) -> list[str]:
    cmd: list[str] = [sys.executable, "-m", "scripts.main"]
    if bool(getattr(args, "new", True)):
        cmd.append("--new")

    cmd.extend(
        [
            "--query",
            str(args.query),
            "--speed-mode",
            str(args.speed_mode),
            "--yt-search-n",
            str(int(args.yt_search_n)),
            "--calibration-level",
            str(int(calibration_level)),
            "--tune-for-me",
            str(int(tune_level)),
            "--off",
            f"{float(off):.3f}",
        ]
    )

    for stem in ("vocals", "bass", "drums", "other"):
        val = getattr(args, stem, None)
        if isinstance(val, int):
            cmd.extend([f"--{stem}", str(int(val))])

    if bool(getattr(args, "no_render", False)):
        cmd.append("--no-render")
    return cmd


def _run_pipeline(cmd: list[str], *, env_overrides: Optional[dict[str, str]] = None) -> int:
    env = os.environ.copy()
    if env_overrides:
        env.update({str(k): str(v) for k, v in env_overrides.items()})
    log("MOBILE", "RUN " + " ".join(shlex.quote(str(c)) for c in cmd), CYAN)
    return int(subprocess.run(cmd, env=env).returncode)


def _prompt_yes_no(prompt: str, *, default_yes: bool = True) -> bool:
    suffix = "[Y/n]" if default_yes else "[y/N]"
    while True:
        raw = input(f"{prompt} {suffix} ").strip().lower()
        if not raw:
            return bool(default_yes)
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please answer yes or no.")


def _prompt_manual_offset() -> Optional[float]:
    while True:
        raw = input("Manual offset seconds (e.g. -1.0), or 'done': ").strip()
        if not raw:
            continue
        lowered = raw.lower()
        if lowered in {"done", "d", "q", "quit"}:
            return None
        try:
            return float(raw)
        except ValueError:
            print("Please enter a number like -1.0, 0.5, 1.25, or 'done'.")


def _print_auto_result(result: dict[str, Any]) -> None:
    status = str(result.get("status") or "unknown")
    off = float(result.get("auto_offset_s") or 0.0)
    conf = result.get("aggregate_confidence")
    selected = result.get("selected_samples")
    count = result.get("sample_count")

    parts = [f"status={status}", f"offset={off:+.3f}s"]
    if isinstance(conf, float):
        parts.append(f"confidence={conf:.2f}")
    if isinstance(selected, int) and isinstance(count, int):
        parts.append(f"samples={selected}/{count}")
    log("MOBILE", "Auto result: " + ", ".join(parts), CYAN)


def _manual_loop(args: argparse.Namespace, slug: str) -> int:
    log(
        "MOBILE",
        "Entering manual offset mode (auto tuning exhausted or declined).",
        YELLOW,
    )
    while True:
        manual_off = _prompt_manual_offset()
        if manual_off is None:
            log("MOBILE", "Done.", GREEN)
            return 0

        cmd = _build_main_cmd(args, off=float(manual_off), tune_level=0, calibration_level=0)
        rc = _run_pipeline(cmd, env_overrides=None)
        if rc != 0:
            log("MOBILE", f"Manual render failed (exit={rc}). Try a different offset.", RED)
            continue

        result = _load_auto_result(slug)
        _print_auto_result(result)
        if _prompt_yes_no("Keep this and finish?", default_yes=True):
            log("MOBILE", "Done.", GREEN)
            return 0


def guided_mobile_flow(args: argparse.Namespace) -> int:
    slug = slugify(str(args.query))
    max_algorithms = int(max(1, min(len(AUTO_PROFILES), int(args.max_algorithms or 1))))
    profiles = list(AUTO_PROFILES[:max_algorithms])

    log(
        "MOBILE",
        "Starting guided mobile tune: max auto tuning first, then retries, then manual fallback.",
        CYAN,
    )
    for idx, profile in enumerate(profiles, start=1):
        log("MOBILE", f"Auto attempt {idx}/{len(profiles)} using profile={profile.name}", CYAN)
        cmd = _build_main_cmd(args, off=0.0, tune_level=3, calibration_level=2)
        rc = _run_pipeline(cmd, env_overrides=profile.env)
        result = _load_auto_result(slug)
        _print_auto_result(result)

        status = str(result.get("status") or "").lower()
        auto_applied = bool(result.get("auto_exists")) and abs(float(result.get("auto_offset_s") or 0.0)) > 1e-6
        if rc == 0 and status == "applied" and auto_applied:
            if _prompt_yes_no("Auto tune applied. Finish now?", default_yes=True):
                log("MOBILE", "Done.", GREEN)
                return 0
            return _manual_loop(args, slug)

        has_next = idx < len(profiles)
        if has_next and _prompt_yes_no("Auto tune did not confidently apply. Try a different algorithm?", default_yes=True):
            continue
        return _manual_loop(args, slug)

    return _manual_loop(args, slug)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    return guided_mobile_flow(args)


if __name__ == "__main__":
    raise SystemExit(main())

