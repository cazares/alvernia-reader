#!/usr/bin/env python3
"""Run all distinct yt-dlp command variants seen in step1_fetch.py history.

Usage:
  scripts/yt_dlp_variants_test.py "Artist - Title"
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Tuple


YTDLP_BIN_NAME = "yt-dlp"


@dataclass
class Result:
    name: str
    cmd: List[str]
    ok: bool
    skipped: bool
    note: str


def _add_to_path(dir_path: str) -> None:
    if not dir_path:
        return
    path_env = os.environ.get("PATH", "")
    parts = path_env.split(os.pathsep) if path_env else []
    if dir_path not in parts:
        os.environ["PATH"] = dir_path + os.pathsep + path_env


def ensure_yt_dlp() -> Optional[List[str]]:
    path = shutil.which(YTDLP_BIN_NAME)
    if path:
        return [path]

    # Try Homebrew on macOS.
    if sys.platform == "darwin" and shutil.which("brew"):
        subprocess.run(["brew", "install", YTDLP_BIN_NAME], check=False)
        # Ensure Homebrew bin is on PATH.
        for brew_bin in ("/opt/homebrew/bin", "/usr/local/bin"):
            _add_to_path(brew_bin)
        path = shutil.which(YTDLP_BIN_NAME)
        if path:
            return [path]

    # Fallback to pip --user.
    py = sys.executable or "python3"
    subprocess.run([py, "-m", "pip", "install", "--user", "-U", YTDLP_BIN_NAME], check=False)
    local_bin = os.path.expanduser("~/.local/bin")
    _add_to_path(local_bin)

    # Last-ditch: check common locations directly.
    for candidate in (
        os.path.join(local_bin, YTDLP_BIN_NAME),
        f"/opt/homebrew/bin/{YTDLP_BIN_NAME}",
        f"/usr/local/bin/{YTDLP_BIN_NAME}",
    ):
        if os.path.isfile(candidate):
            _add_to_path(os.path.dirname(candidate))
            return [candidate]

    final_path = shutil.which(YTDLP_BIN_NAME)
    if final_path:
        return [final_path]

    # Last resort: run as a Python module.
    return [sys.executable or "python3", "-m", "yt_dlp"]


def run_cmd(name: str, cmd: List[str], timeout: float) -> Tuple[bool, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, "timeout"
    if p.returncode == 0:
        return True, "ok"
    tail = (p.stderr or p.stdout or "").strip().splitlines()[-8:]
    msg = " | ".join(tail) if tail else f"rc={p.returncode}"
    return False, msg


def pick_first_video_id_from_json_lines(text: str) -> Optional[str]:
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        vid = obj.get("id") or obj.get("video_id")
        if vid:
            return str(vid)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Run all yt-dlp command variants (from step1_fetch.py history).")
    ap.add_argument("query", help="Search query, e.g. 'Artist - Title'")
    ap.add_argument("--limit", type=int, default=5, help="Search result limit for ytsearch (default: 5)")
    ap.add_argument("--timeout", type=float, default=60.0, help="Per-command timeout seconds (default: 60)")
    ap.add_argument("--keep", action="store_true", help="Keep output files (default: delete temp dir)")
    ap.add_argument("--dry-run", action="store_true", help="Print commands without running them")
    ap.add_argument("--sub-langs", default="en.*,es.*,.*", help="Subtitle language pattern")
    args = ap.parse_args()

    yt_dlp = ensure_yt_dlp()
    if yt_dlp is None:
        print("yt-dlp not found and auto-install failed", file=sys.stderr)
        return 2

    q_basic = f"ytsearch{args.limit}:{args.query}"
    q_lyrics = f"ytsearch{args.limit}:\"{args.query}\" lyrics"

    socket_timeout = os.environ.get("MIXTERIOSO_YTDLP_SOCKET_TIMEOUT", "6")
    retries = os.environ.get("MIXTERIOSO_YTDLP_RETRIES", "1")
    frag_retries = os.environ.get("MIXTERIOSO_YTDLP_FRAGMENT_RETRIES", "1")
    concurrent_frags = os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS", "8")
    audio_quality = os.environ.get("MIXTERIOSO_YTDLP_AUDIO_QUALITY", "7")
    extractor_args = os.environ.get("MIXTERIOSO_YTDLP_EXTRACTOR_ARGS", "youtube:player_client=android")
    user_agent = os.environ.get("MIXTERIOSO_YTDLP_UA", "Mozilla/5.0")
    cookies_path = os.environ.get("MIXTERIOSO_YTDLP_COOKIES", "").strip()

    tmpdir_obj = tempfile.TemporaryDirectory(prefix="yt-dlp-variants-")
    tmpdir = tmpdir_obj.name
    outtmpl = os.path.join(tmpdir, "audio.%(ext)s")
    subs_tmpl = os.path.join(tmpdir, "subs.%(ext)s")

    results: List[Result] = []

    search_variants = [
        ("search_flat_basic", [*yt_dlp, "--dump-json", "--flat-playlist", q_basic]),
        ("search_flat_basic_lyrics", [*yt_dlp, "--dump-json", "--flat-playlist", q_lyrics]),
        (
            "search_flat_ipv4_socket",
            [
                *yt_dlp,
                "--dump-json",
                "--flat-playlist",
                "--no-warnings",
                "--force-ipv4",
                "--socket-timeout",
                str(socket_timeout),
                q_basic,
            ],
        ),
    ]

    picked_video_id: Optional[str] = None
    for name, cmd in search_variants:
        if args.dry_run:
            results.append(Result(name, cmd, ok=True, skipped=False, note="dry-run"))
            continue
        ok, note = run_cmd(name, cmd, args.timeout)
        results.append(Result(name, cmd, ok=ok, skipped=False, note=note))
        if ok and picked_video_id is None:
            try:
                p = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
                picked_video_id = pick_first_video_id_from_json_lines(p.stdout or "")
            except Exception:
                picked_video_id = None

    if picked_video_id:
        url = f"https://www.youtube.com/watch?v={picked_video_id}"
    else:
        url = f"ytsearch1:{args.query}"

    # Audio download variants
    audio_variants = [
        (
            "audio_minimal",
            [
                *yt_dlp,
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                "0",
                "-o",
                outtmpl,
                url,
            ],
        ),
        (
            "audio_ipv4_retries",
            [
                *yt_dlp,
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                "0",
                "--force-ipv4",
                "--retries",
                "10",
                "--fragment-retries",
                "10",
                "--user-agent",
                "Mozilla/5.0",
                "-o",
                outtmpl,
                url,
            ],
        ),
        (
            "audio_base_common_v1",
            [
                *yt_dlp,
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                str(audio_quality),
                "--no-playlist",
                "--no-warnings",
                "--force-ipv4",
                "--socket-timeout",
                str(socket_timeout),
                "--retries",
                str(retries),
                "--fragment-retries",
                str(frag_retries),
                "--concurrent-fragments",
                str(concurrent_frags),
                "--user-agent",
                str(user_agent),
                "-o",
                outtmpl,
                "--extractor-args",
                str(extractor_args),
                url,
            ],
        ),
        (
            "audio_base_common_v2",
            [
                *yt_dlp,
                "-f",
                "bestaudio/best",
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                str(audio_quality),
                "--no-playlist",
                "--no-warnings",
                "--force-ipv4",
                "--socket-timeout",
                str(socket_timeout),
                "--retries",
                str(retries),
                "--fragment-retries",
                str(frag_retries),
                "--concurrent-fragments",
                str(concurrent_frags),
                "--user-agent",
                str(user_agent),
                "-o",
                outtmpl,
                "--extractor-args",
                str(extractor_args),
                "--match-filter",
                "is_live != True",
                url,
            ],
        ),
    ]

    # Optional cookies flag for base variants
    if cookies_path:
        for idx, (name, cmd) in enumerate(audio_variants):
            if name.startswith("audio_base_common"):
                cmd = cmd + ["--cookies", cookies_path]
                audio_variants[idx] = (name + "_cookies", cmd)

    # Subtitle variants
    subs_variants = [
        (
            "subs_basic",
            [
                *yt_dlp,
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs",
                args.sub_langs,
                "--sub-format",
                "vtt",
                "-o",
                subs_tmpl,
                url,
            ],
        ),
        (
            "subs_ipv4_retries",
            [
                *yt_dlp,
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs",
                args.sub_langs,
                "--sub-format",
                "vtt",
                "--force-ipv4",
                "--retries",
                "10",
                "-o",
                subs_tmpl,
                url,
            ],
        ),
    ]

    for name, cmd in audio_variants + subs_variants:
        if args.dry_run:
            results.append(Result(name, cmd, ok=True, skipped=False, note="dry-run"))
            continue
        ok, note = run_cmd(name, cmd, args.timeout)
        results.append(Result(name, cmd, ok=ok, skipped=False, note=note))

    ok_cmds = [r for r in results if r.ok]
    failed_cmds = [r for r in results if not r.ok and not r.skipped]

    print("\nSuccessful commands:")
    if ok_cmds:
        for r in ok_cmds:
            print(" ", " ".join(r.cmd))
    else:
        print("  (none)")

    print("\nFailed commands:")
    if failed_cmds:
        for r in failed_cmds:
            print(f"  [{r.name}] {r.note}")
            print(" ", " ".join(r.cmd))
    else:
        print("  (none)")

    if args.dry_run and not ok_cmds:
        print("\nCommands (dry-run):")
        for r in results:
            print(" ", " ".join(r.cmd))

    if not args.keep:
        tmpdir_obj.cleanup()
    else:
        print(f"\nKeeping output dir: {tmpdir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
