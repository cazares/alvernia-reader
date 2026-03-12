#!/usr/bin/env python3
# lrc_utils.py

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple


from datetime import datetime
import os

def _log(msg: str) -> None:
    if os.getenv("KARAOKE_DEBUG_LRC_UTILS", "0") not in ("1", "true", "TRUE", "yes", "YES"):
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [LRC_UTILS] {msg}", flush=True)


_TS_RE = re.compile(r"\[(\d+):(\d{2})(?:\.(\d{1,3}))?\]")
_META_RE = re.compile(r"^\s*\[([a-zA-Z]+)\s*:\s*(.*?)\s*\]\s*$")


@dataclass(frozen=True)
class LrcEvent:
    t: float
    text: str


def _frac_to_secs(frac: Optional[str]) -> float:
    if not frac:
        return 0.0
    # LRC commonly uses centiseconds, sometimes milliseconds
    # Normalize to seconds by treating 1-2 digits as centiseconds, 3 digits as milliseconds
    if len(frac) == 1:
        return int(frac) / 10.0
    if len(frac) == 2:
        return int(frac) / 100.0
    return int(frac[:3]) / 1000.0


def parse_lrc(path_or_text: str) -> Tuple[List[LrcEvent], dict]:
    """
    Returns (events, meta)

    Accepts either:
      - a filesystem path to an .lrc file, or
      - the raw .lrc file contents as a string

    Detection rule:
      - if the input contains a newline, it is treated as raw contents
      - otherwise it is treated as a path
    """
    meta: dict = {}
    events: List[LrcEvent] = []

    # If caller passed raw LRC text, avoid trying to open it as a filename
    if "\n" in path_or_text or "\r" in path_or_text:
        lines = path_or_text.splitlines()
    else:
        with open(path_or_text, "r", encoding="utf-8", errors="replace") as f:
            lines = [ln.rstrip("\n") for ln in f]

    for line in lines:
        m_meta = _META_RE.match(line)
        if m_meta and not _TS_RE.search(line):
            key = m_meta.group(1).strip().lower()
            val = m_meta.group(2).strip()
            meta[key] = val
            continue

        ts = list(_TS_RE.finditer(line))
        if not ts:
            continue

        # remove all timestamp tags to get lyric text
        text = _TS_RE.sub("", line).strip()
        # Allow blank lines, but they are usually unhelpful for alignment
        for m in ts:
            mm = int(m.group(1))
            ss = int(m.group(2))
            frac = m.group(3)
            t = mm * 60 + ss + _frac_to_secs(frac)
            events.append(LrcEvent(t=t, text=text))

    events.sort(key=lambda e: e.t)
    _log(f"parse_lrc events={len(events)} meta_keys={len(meta)}")
    return events, meta


def apply_global_offset(events: Iterable[LrcEvent], add_secs: float) -> List[LrcEvent]:
    return [LrcEvent(t=max(0.0, e.t + add_secs), text=e.text) for e in events]


def format_lrc(events: Iterable[LrcEvent], meta: Optional[dict] = None) -> str:
    lines: List[str] = []
    if meta:
        # keep stable ordering for common tags
        preferred = ["ar", "ti", "al", "by", "length", "offset"]
        for k in preferred:
            if k in meta:
                lines.append(f"[{k}:{meta[k]}]")
        for k, v in meta.items():
            if k not in preferred:
                lines.append(f"[{k}:{v}]")

    for e in events:
        mm = int(e.t // 60)
        ss = int(e.t % 60)
        cs = int(round((e.t - (mm * 60 + ss)) * 100))
        lines.append(f"[{mm:02d}:{ss:02d}.{cs:02d}] {e.text}".rstrip())
    return "\n".join(lines) + "\n"


def normalize_text_for_match(s: str) -> str:
    s = s.lower()
    # keep letters/numbers/spaces
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokens(s: str) -> List[str]:
    s = normalize_text_for_match(s)
    if not s:
        return []
    return s.split(" ")
