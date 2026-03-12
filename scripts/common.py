#!/usr/bin/env python3
"""
Common utilities for Mixterioso scripts
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

# ---------------------------
# Paths
# ---------------------------

ROOT = Path(__file__).resolve().parent.parent
META_DIR = ROOT / "meta"

# ---------------------------
# ANSI colors
# ---------------------------

WHITE = "\033[97m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
ORANGE = "\033[38;5;208m"
RESET = "\033[0m"


# ---------------------------
# Logging
# ---------------------------

def log(tag: str, msg: str, color: str = "") -> None:
    ts = time.strftime("%H:%M:%S")
    if color:
        print(f"[{ts}] [{tag}] {color}{msg}{RESET}", flush=True)
    else:
        print(f"[{ts}] [{tag}] {msg}", flush=True)


# ---------------------------
# Global timer helpers
# ---------------------------

_timer_t0: Optional[float] = None


def timer_start() -> None:
    global _timer_t0
    _timer_t0 = time.time()


def timer_done() -> float:
    if _timer_t0 is None:
        return 0.0
    return time.time() - _timer_t0


def now_perf_ms() -> float:
    return time.perf_counter() * 1000.0


def elapsed_ms(start_ms: float) -> float:
    return max(0.0, (time.perf_counter() * 1000.0) - float(start_ms))


def log_timing(step: str, part: str, start_ms: float, *, color: str = CYAN) -> None:
    log("TIMING", f"step={step} part={part} elapsed_ms={elapsed_ms(start_ms):.1f}", color)


# ---------------------------
# Filesystem helpers
# ---------------------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def resolve_output_dir(root: Path, *, output_subdir: Optional[str] = None) -> Path:
    raw = str(output_subdir if output_subdir is not None else os.environ.get("MIXTERIOSO_OUTPUT_SUBDIR", "")).strip()
    if not raw:
        return root / "output"
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def slugify(s: str) -> str:
    import re

    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "song"


# ---------------------------
# IO flags and paths
# ---------------------------

@dataclass(frozen=True)
class IOFlags:
    dry_run: bool = False
    force: bool = False
    confirm: bool = False


@dataclass(frozen=True)
class Paths:
    root: Path
    output_subdir: str = "output"

    @property
    def mp3s(self) -> Path:
        return self.root / "mp3s"

    @property
    def timings(self) -> Path:
        return self.root / "timings"

    @property
    def meta(self) -> Path:
        return self.root / "meta"

    @property
    def separated(self) -> Path:
        return self.root / "separated"

    @property
    def mixes(self) -> Path:
        return self.root / "mixes"

    @property
    def output(self) -> Path:
        return resolve_output_dir(self.root, output_subdir=self.output_subdir)

    def ensure(self) -> None:
        for d in (
            self.mp3s,
            self.timings,
            self.meta,
            self.separated,
            self.mixes,
            self.output,
        ):
            ensure_dir(d)


# ---------------------------
# Execution helpers
# ---------------------------

def have_exe(name: str) -> bool:
    return shutil.which(name) is not None


def should_write(path: Path, flags: IOFlags, *, label: str = "file") -> bool:
    if flags.force:
        return True
    if not path.exists():
        return True
    if flags.confirm:
        ans = input(f"Overwrite {label} {path}? [y/N] ").strip().lower()
        return ans in ("y", "yes")
    log("SKIP", f"{label} exists: {path}", YELLOW)
    return False


def write_text(path: Path, s: str, flags: Optional[IOFlags] = None, *, label: str = "file") -> None:
    flags = flags or IOFlags()
    if not should_write(path, flags, label=label):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(s, encoding="utf-8")


def write_json(path: Path, obj: Any, flags: Optional[IOFlags] = None, *, label: str = "json") -> None:
    flags = flags or IOFlags()
    if not should_write(path, flags, label=label):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


# ---------------------------
# Subprocess wrappers
# ---------------------------

def run_cmd(
    cmd: list[str],
    *,
    tag: str = "RUN",
    dry_run: bool = False,
    check: bool = False,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    s = " ".join(str(c) for c in cmd)
    log(tag, s, CYAN)
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, "", "")

    p = subprocess.run(
        [str(c) for c in cmd],
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=False,
        text=True,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"{tag} failed rc={p.returncode}: {s}")
    return p


def run_cmd_capture(
    cmd: list[str],
    *,
    tag: str = "RUN",
    dry_run: bool = False,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
) -> Tuple[int, str]:
    s = " ".join(str(c) for c in cmd)
    log(tag, s, CYAN)
    if dry_run:
        return 0, ""
    p = subprocess.run(
        [str(c) for c in cmd],
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
    )
    out = (p.stdout or "").strip()
    if not out and p.stderr:
        out = p.stderr.strip()
    return p.returncode, out


# ---------------------------
# ffmpeg helpers
# ---------------------------

_FFMPEG_FILTERS_OUTPUT_CACHE: dict[str, str] = {}
_FFMPEG_ENCODERS_OUTPUT_CACHE: dict[str, str] = {}


def _ffmpeg_cache_key(ffmpeg_bin: Path | str) -> str:
    return str(Path(ffmpeg_bin))


def clear_ffmpeg_capability_cache() -> None:
    _FFMPEG_FILTERS_OUTPUT_CACHE.clear()
    _FFMPEG_ENCODERS_OUTPUT_CACHE.clear()


def _get_ffmpeg_filters_output(ffmpeg_bin: Path) -> str | None:
    key = _ffmpeg_cache_key(ffmpeg_bin)
    if key in _FFMPEG_FILTERS_OUTPUT_CACHE:
        return _FFMPEG_FILTERS_OUTPUT_CACHE[key]

    rc, out = run_cmd_capture([str(ffmpeg_bin), "-filters"], tag="FFMPEG", dry_run=False)
    if rc != 0:
        return None

    payload = out or ""
    _FFMPEG_FILTERS_OUTPUT_CACHE[key] = payload
    return payload


def _get_ffmpeg_encoders_output(ffmpeg_bin: Path) -> str | None:
    key = _ffmpeg_cache_key(ffmpeg_bin)
    if key in _FFMPEG_ENCODERS_OUTPUT_CACHE:
        return _FFMPEG_ENCODERS_OUTPUT_CACHE[key]

    rc, out = run_cmd_capture([str(ffmpeg_bin), "-hide_banner", "-encoders"], tag="FFMPEG", dry_run=False)
    if rc != 0:
        return None

    payload = out or ""
    _FFMPEG_ENCODERS_OUTPUT_CACHE[key] = payload
    return payload


def _ffmpeg_symbol_present(raw_text: str, symbol: str) -> bool:
    target = str(symbol or "").strip()
    if not target:
        return False
    pattern = rf"(?<![A-Za-z0-9_-]){re.escape(target)}(?![A-Za-z0-9_-])"
    return re.search(pattern, raw_text) is not None

def resolve_ffmpeg_bin() -> Path:
    for k in ("FFMPEG_BIN", "MIXTERIOSO_FFMPEG", "KARAOKE_FFMPEG"):
        v = os.environ.get(k, "").strip()
        if v:
            return Path(v)
    exe = shutil.which("ffmpeg")
    if exe:
        return Path(exe)
    return Path("ffmpeg")


def resolve_demucs_bin() -> Path:
    """
    Resolve demucs binary path, checking:
    1. Environment variable DEMUCS_BIN / MIXTERIOSO_DEMUCS
    2. Current interpreter / active virtual environment:
       - <sys.executable dir>/demucs
       - $VIRTUAL_ENV/bin/demucs
    3. Local virtual environments:
       - .venv/bin/demucs
       - demucs_env/bin/demucs
    4. System PATH via shutil.which()
    5. Fallback to "demucs" (will fail if not found)
    """
    for k in ("DEMUCS_BIN", "MIXTERIOSO_DEMUCS"):
        v = os.environ.get(k, "").strip()
        if v:
            return Path(v)

    local_candidates = []
    python_exe = Path(str(sys.executable or "")).expanduser()
    if python_exe.name.startswith("python"):
        local_candidates.append(python_exe.with_name("demucs"))

    virtual_env = os.environ.get("VIRTUAL_ENV", "").strip()
    if virtual_env:
        local_candidates.append(Path(virtual_env).expanduser() / "bin" / "demucs")

    local_candidates.extend([
        ROOT / ".venv" / "bin" / "demucs",
        ROOT / "demucs_env" / "bin" / "demucs",
    ])
    for candidate in local_candidates:
        if candidate.exists():
            return candidate

    exe = shutil.which("demucs")
    if exe:
        return Path(exe)
    return Path("demucs")


def ffmpeg_has_filter(ffmpeg_bin: Path, filter_name: str) -> bool:
    out = _get_ffmpeg_filters_output(ffmpeg_bin)
    if out is None:
        return False
    return _ffmpeg_symbol_present(out, filter_name)


def ffmpeg_has_encoder(ffmpeg_bin: Path, encoder_name: str) -> bool:
    out = _get_ffmpeg_encoders_output(ffmpeg_bin)
    if out is None:
        return False
    return _ffmpeg_symbol_present(out, encoder_name)


def ffmpeg_escape_filter_path(p: Path) -> str:
    s = str(p)
    s = s.replace("\\", "\\\\")
    s = s.replace("'", "\\'")
    s = s.replace(":", "\\:")
    return s


# end of common.py
