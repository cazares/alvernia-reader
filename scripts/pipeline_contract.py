#!/usr/bin/env python3
"""Shared pipeline option contract for thin CLI/API wrappers."""

from __future__ import annotations

from typing import Any, Literal, Mapping, TypeAlias

SpeedMode: TypeAlias = Literal["turbo", "extra-turbo", "ultimate-light-speed"]

SPEED_MODE_CHOICES: tuple[str, ...] = (
    "turbo",
    "extra-turbo",
    "ultimate-light-speed",
)

STEM_LEVEL_OPTION_KEYS: tuple[str, ...] = ("vocals", "bass", "drums", "other")


def _as_clean_str(value: Any) -> str:
    return str(value or "").strip()


def build_pipeline_argv(
    *,
    query: str,
    options: Mapping[str, Any],
    allow_stem_levels: bool = True,
    server_download_only: bool = False,
) -> list[str]:
    """Build canonical scripts.main argv from pipeline options."""
    argv = ["--query", str(query)]
    opts = options or {}

    audio_url = _as_clean_str(opts.get("audio_url"))
    audio_id = _as_clean_str(opts.get("audio_id"))
    if server_download_only:
        audio_url = ""
        audio_id = ""
    if audio_url:
        argv += ["--audio-url", audio_url]
    elif audio_id:
        argv += ["--audio-id", audio_id]

    language = _as_clean_str(opts.get("language"))
    if language:
        argv += ["--language", language]

    if opts.get("force"):
        argv.append("--force")
    if opts.get("reset"):
        argv.append("--reset")
    if opts.get("nuke"):
        argv.append("--nuke")
    if opts.get("dry_run"):
        argv.append("--dry-run")
    if opts.get("confirm"):
        argv.append("--confirm")
    if opts.get("no_parallel"):
        argv.append("--no-parallel")
    if opts.get("upload"):
        argv.append("--upload")
    if opts.get("render_only"):
        argv.append("--render-only")
    if opts.get("no_render"):
        argv.append("--no-render")
    if opts.get("skip_step1"):
        argv.append("--skip-step1")

    if opts.get("yt_search_n") is not None:
        argv += ["--yt-search-n", str(int(opts["yt_search_n"]))]
    if opts.get("retry_attempt") is not None:
        argv += ["--retry-attempt", str(int(opts["retry_attempt"]))]

    speed_mode = _as_clean_str(opts.get("speed_mode")).lower()
    if speed_mode:
        argv += ["--speed-mode", speed_mode]

    tune_for_me = opts.get("enable_auto_offset", opts.get("tune_for_me"))
    if tune_for_me is not None:
        try:
            argv += ["--tune-for-me", str(int(tune_for_me))]
        except Exception:
            pass

    calibration_level = opts.get("calibration_level")
    if calibration_level is not None:
        try:
            argv += ["--calibration-level", str(int(calibration_level))]
        except Exception:
            pass

    if allow_stem_levels and opts.get("vocals") is not None:
        argv += ["--vocals", str(int(opts["vocals"]))]
    if allow_stem_levels and opts.get("bass") is not None:
        argv += ["--bass", str(int(opts["bass"]))]
    if allow_stem_levels and opts.get("drums") is not None:
        argv += ["--drums", str(int(opts["drums"]))]
    if allow_stem_levels and opts.get("other") is not None:
        argv += ["--other", str(int(opts["other"]))]

    if opts.get("offset_sec") is not None:
        try:
            argv += ["--offset", str(float(opts["offset_sec"]))]
        except Exception:
            pass

    upload_privacy = _as_clean_str(opts.get("upload_privacy"))
    if upload_privacy:
        argv += ["--upload-privacy", upload_privacy]
    upload_title = _as_clean_str(opts.get("upload_title"))
    if upload_title:
        argv += ["--upload-title", upload_title]
    upload_ending = _as_clean_str(opts.get("upload_ending"))
    if upload_ending:
        argv += ["--upload-ending", upload_ending]
    if opts.get("upload_interactive"):
        argv.append("--upload-interactive")
    if opts.get("upload_open_output_dir"):
        argv.append("--upload-open-output-dir")

    return argv
