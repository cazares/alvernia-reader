#!/usr/bin/env python3
"""Estimate a single global offset between an LRC file and an audio file.

Primary use
- step3_sync calls this to write timings/<slug>.offset.auto

Approach
- Parse LRC into timed lines
- Transcribe a short clip around early lyric lines using whisper.cpp (whisper-cli)
- Align LRC lines to Whisper segments via token overlap
- Return the median (whisper_time - lrc_time) over matches

This is designed to be fast and resilient, not perfect alignment
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import unicodedata
import wave
from pathlib import Path
from difflib import SequenceMatcher
from typing import Any, Iterable, List, Optional, Tuple

from .lrc_utils import parse_lrc, tokens


_COMMON_FILLER_TOKENS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "de",
    "el",
    "en",
    "for",
    "i",
    "in",
    "is",
    "it",
    "la",
    "los",
    "me",
    "mi",
    "my",
    "no",
    "of",
    "on",
    "or",
    "que",
    "so",
    "te",
    "the",
    "to",
    "tu",
    "we",
    "y",
    "you",
    "your",
}

_TRUE_VALUES = {"1", "true", "yes", "on", "y"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _related_worktree_roots(root: Path) -> List[Path]:
    roots: List[Path] = []
    seen: set[str] = set()
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "worktree", "list", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except Exception:
        return roots
    if result.returncode != 0:
        return roots

    for raw_line in str(result.stdout or "").splitlines():
        if not raw_line.startswith("worktree "):
            continue
        candidate = Path(raw_line.split(" ", 1)[1].strip()).expanduser()
        candidate_key = str(candidate)
        if not candidate_key or candidate_key in seen:
            continue
        seen.add(candidate_key)
        roots.append(candidate)
    return roots


def _candidate_repo_roots() -> List[Path]:
    root = _repo_root()
    out: List[Path] = []
    seen: set[str] = set()
    for candidate in [root, *_related_worktree_roots(root)]:
        candidate_key = str(candidate)
        if not candidate_key or candidate_key in seen:
            continue
        seen.add(candidate_key)
        out.append(candidate)
    return out


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tok(s: str) -> List[str]:
    return [t for t in tokens(_norm(s)) if len(t) > 1]


def _run(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in _TRUE_VALUES


def _ffmpeg_clip_to_wav(ffmpeg_bin: str, audio_in: Path, wav_out: Path, start_s: float, dur_s: float) -> None:
    # Keep this quiet and non-interactive
    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-ss",
        f"{start_s:.3f}",
        "-t",
        f"{dur_s:.3f}",
        "-i",
        str(audio_in),
        "-ar",
        "16000",
        "-ac",
        "1",
        "-c:a",
        "pcm_s16le",
        str(wav_out),
    ]
    _run(cmd)


def _transcribe_wav_segments(
    *,
    wav_path: Path,
    language: str,
    whisper_bin: str,
    model_path: str,
    whisper_extra_args: Optional[List[str]] = None,
) -> List[Tuple[float, float, str]]:
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        out_prefix = td_path / "whisper_out"

        cmd = [
            whisper_bin,
            "-m",
            model_path,
            "-f",
            str(wav_path),
            "-oj",
            "-of",
            str(out_prefix),
            "-np",
            "-bs",
            "1",
            "-bo",
            "1",
            "-l",
            language,
        ]
        if whisper_extra_args:
            cmd.extend([str(v) for v in whisper_extra_args if str(v).strip()])
        _run(cmd)

        js_path = Path(str(out_prefix) + ".json")
        if not js_path.exists():
            raise RuntimeError("whisper.cpp did not produce JSON output")

        js = json.loads(js_path.read_text(encoding="utf-8", errors="replace"))
        segs = _load_segments_from_whisper_json(js)
        if not segs:
            raise RuntimeError("No segments in whisper output")
        return segs


def _find_whispercpp_bin(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        p = Path(explicit)
        return str(p) if p.exists() else None

    # Prefer repo-local build path, then fall back to other linked git worktrees.
    for root in _candidate_repo_roots():
        candidates = [
            root / "whisper.cpp" / "build" / "bin" / "whisper-cli",
            root / "whisper.cpp" / "build" / "bin" / "main",
            root / "whisper.cpp" / "main",
        ]
        for c in candidates:
            if c.exists():
                return str(c)

    # Fallback: PATH
    for name in ("whisper-cli", "main"):
        if shutil.which(name):
            return name

    return None


def _find_model(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        p = Path(explicit)
        return str(p) if p.exists() else None

    for root in _candidate_repo_roots():
        candidates = [
            # Prefer higher-accuracy local models for calibration reliability.
            root / "whisper.cpp" / "models" / "ggml-small.bin",
            root / "whisper.cpp" / "models" / "ggml-base.bin",
            root / "whisper.cpp" / "models" / "ggml-tiny.bin",
            root / "models" / "ggml-tiny.bin",
        ]
        for c in candidates:
            if c.exists():
                return str(c)
    return None


def _load_segments_from_whisper_json(js: dict) -> List[Tuple[float, float, str]]:
    out: List[Tuple[float, float, str]] = []
    # Newer whisper-cli JSON:
    # { "transcription": [ {"text": "...", "offsets": {"from": ms, "to": ms}}, ... ] }
    trans = js.get("transcription")
    if isinstance(trans, list):
        for seg in trans:
            if not isinstance(seg, dict):
                continue
            txt = str(seg.get("text", "")).strip()
            if not txt:
                continue
            offs = seg.get("offsets")
            if not isinstance(offs, dict):
                continue
            start_ms = offs.get("from", None)
            end_ms = offs.get("to", None)
            if not isinstance(start_ms, (int, float)):
                continue
            if isinstance(end_ms, (int, float)):
                end_s = float(end_ms) / 1000.0
            else:
                end_s = float(start_ms) / 1000.0
            out.append((float(start_ms) / 1000.0, end_s, txt))
        if out:
            return out

    # Older whisper.cpp style:
    # { "segments": [ {"start": sec, "text": ...} ] } or {"t0": 10ms units, ...}
    segs = js.get("segments")
    if not isinstance(segs, list):
        return out
    for seg in segs:
        if not isinstance(seg, dict):
            continue
        txt = str(seg.get("text", "")).strip()
        if not txt:
            continue
        start = seg.get("start", None)
        if isinstance(start, (int, float)):
            start_s = float(start)
            end_val = seg.get("end", None)
            if isinstance(end_val, (int, float)):
                end_s = float(end_val)
            else:
                end_s = start_s
        else:
            t0 = seg.get("t0", None)
            if isinstance(t0, int):
                # whisper.cpp commonly stores t0 in 10ms units
                start_s = t0 / 100.0
                t1 = seg.get("t1", None)
                if isinstance(t1, int):
                    end_s = t1 / 100.0
                elif isinstance(t1, float):
                    end_s = float(t1)
                else:
                    end_s = start_s
            elif isinstance(t0, float):
                start_s = float(t0)
                t1 = seg.get("t1", None)
                if isinstance(t1, (int, float)):
                    end_s = float(t1)
                else:
                    end_s = start_s
            else:
                continue
        out.append((start_s, end_s, txt))

    return out


def _token_overlap(a: List[str], b: List[str]) -> float:
    if not a or not b:
        return 0.0
    sa, sb = set(a), set(b)
    inter = len(sa & sb)
    return inter / max(len(sa), 1)


def _token_stem(tok: str) -> str:
    t = str(tok or "").strip().lower()
    if len(t) >= 5 and t.endswith("ing"):
        t = t[:-3]
    elif len(t) >= 4 and t.endswith("in"):
        t = t[:-2]
    elif len(t) >= 4 and t.endswith("ed"):
        t = t[:-2]
    elif len(t) >= 4 and t.endswith("es"):
        t = t[:-2]
    elif len(t) >= 3 and t.endswith("s"):
        t = t[:-1]
    return t


def _tokens_soft_match(raw_a: str, raw_b: str) -> bool:
    a = str(raw_a or "")
    b = str(raw_b or "")
    a_stem = _token_stem(a)
    b_stem = _token_stem(b)
    if a_stem and b_stem and a_stem == b_stem:
        return True
    if len(a_stem) >= 4 and len(b_stem) >= 4:
        return SequenceMatcher(None, a_stem, b_stem).ratio() >= 0.80
    return False


def _soft_token_intersection(a_tokens: List[str], b_tokens: List[str]) -> set[str]:
    out: set[str] = set()
    b_norm = [str(t) for t in b_tokens]
    for raw_a in a_tokens:
        a = str(raw_a)
        for b in b_norm:
            if _tokens_soft_match(a, b):
                out.add(a)
                break
    return out


def _ordered_soft_match_positions(needle_tokens: List[str], haystack_tokens: List[str]) -> List[int]:
    positions: List[int] = []
    if not needle_tokens or not haystack_tokens:
        return positions

    cursor = 0
    for raw_needle in needle_tokens:
        needle = str(raw_needle or "").strip()
        if not needle:
            continue
        for idx in range(cursor, len(haystack_tokens)):
            if _tokens_soft_match(needle, haystack_tokens[idx]):
                positions.append(int(idx))
                cursor = int(idx + 1)
                break
    return positions


def _segment_match_anchor_abs(row: dict[str, Any], seg: dict[str, Any]) -> float:
    default_anchor = float(seg.get("anchor_abs") or 0.0)
    if not bool(seg.get("allow_line_aware_anchor")):
        return default_anchor
    st_abs = float(seg.get("st_abs") or default_anchor)
    en_abs = float(seg.get("en_abs") or st_abs)
    seg_len = max(0.0, en_abs - st_abs)
    seg_tokens = [str(t) for t in list(seg.get("tokens") or []) if str(t).strip()]
    if seg_len <= 0.0 or len(seg_tokens) <= 1:
        return default_anchor

    candidate_sets = [
        list(row.get("informative_tokens") or []),
        list(row.get("content_tokens") or []),
        list(row.get("tokens") or []),
    ]
    best_positions: List[int] = []
    best_score: tuple[float, int, int] = (0.0, 0, 0)
    for candidate_tokens in candidate_sets:
        norm_tokens = [str(tok) for tok in candidate_tokens if str(tok).strip()]
        if not norm_tokens:
            continue
        positions = _ordered_soft_match_positions(norm_tokens, seg_tokens)
        coverage = float(len(positions)) / float(max(len(norm_tokens), 1))
        score = (coverage, len(positions), len(norm_tokens))
        if score > best_score:
            best_score = score
            best_positions = positions

    if not best_positions:
        return default_anchor

    frac = float(best_positions[0]) / float(max(len(seg_tokens) - 1, 1))
    frac = max(0.0, min(1.0, frac))
    return float(st_abs + (seg_len * frac))


def _mad(values: List[float]) -> float:
    if not values:
        return 0.0
    med = statistics.median(values)
    abs_dev = [abs(v - med) for v in values]
    return float(statistics.median(abs_dev))


def _collect_usable_lrc_candidates(events: List[object]) -> List[Tuple[float, str]]:
    candidates: List[Tuple[float, str]] = []
    for ev in events:
        txt = ev.text.strip()
        if not txt:
            continue
        tt = _tok(txt)
        if len(tt) < 3:
            continue
        candidates.append((ev.t, txt))
    return candidates


def _select_anchor_reference(
    candidates: List[Tuple[float, str]],
    anchor_time_s: Optional[float],
) -> Tuple[float, str]:
    if isinstance(anchor_time_s, (int, float)):
        return min(candidates, key=lambda c: abs(float(c[0]) - float(anchor_time_s)))
    return candidates[0]


def _build_window_rows(
    candidates: List[Tuple[float, str]],
    *,
    anchor_time_s: Optional[float],
    clip_dur_s: float,
) -> dict[str, Any]:
    anchor_ref = _select_anchor_reference(candidates, anchor_time_s)
    clip_start_s = max(0.0, float(anchor_ref[0]) - 5.0)
    window_start = clip_start_s - 2.0
    window_end = clip_start_s + float(clip_dur_s) + 2.0
    window_candidates = [c for c in candidates if window_start <= float(c[0]) <= window_end]
    if len(window_candidates) < 2:
        window_candidates = sorted(candidates, key=lambda c: abs(float(c[0]) - float(anchor_ref[0])))[:20]

    window_rows = []
    for lrc_time, lrc_txt in window_candidates:
        lrc_toks = _tok(lrc_txt)
        content_toks = [t for t in lrc_toks if t not in _COMMON_FILLER_TOKENS]
        window_rows.append(
            {
                "lrc_time": float(lrc_time),
                "lrc_txt": lrc_txt,
                "tokens": lrc_toks,
                "content_tokens": content_toks,
            }
        )

    token_df: Counter[str] = Counter()
    for row in window_rows:
        tok_set = set(row["content_tokens"] or row["tokens"])
        for tok in tok_set:
            token_df[str(tok)] += 1
    freq_cutoff = max(3, int(len(window_rows) * 0.5))
    frequent_tokens = {tok for tok, cnt in token_df.items() if int(cnt) >= freq_cutoff}
    for row in window_rows:
        base_tokens = row["content_tokens"] or row["tokens"]
        info_tokens = [t for t in base_tokens if t not in frequent_tokens]
        row["informative_tokens"] = info_tokens or base_tokens

    return {
        "anchor_time_s": (float(anchor_time_s) if isinstance(anchor_time_s, (int, float)) else None),
        "clip_start_s": float(clip_start_s),
        "clip_end_s": float(clip_start_s + float(clip_dur_s)),
        "window_rows": window_rows,
        "frequent_tokens": frequent_tokens,
    }


def _segment_anchor_params(first_lrc_time: float) -> tuple[float, float]:
    late_start_min_secs = float(os.environ.get("KARAOKE_AUTO_OFFSET_LATE_START_MIN_SECS", "8.0"))
    if "KARAOKE_AUTO_OFFSET_SEGMENT_ANCHOR_FRAC" in os.environ:
        anchor_frac = float(os.environ.get("KARAOKE_AUTO_OFFSET_SEGMENT_ANCHOR_FRAC", "0.10"))
    else:
        anchor_frac = 0.0 if first_lrc_time >= late_start_min_secs else 0.10
    anchor_frac = max(0.0, min(1.0, anchor_frac))
    if "KARAOKE_AUTO_OFFSET_ANCHOR_BIAS_SECS" in os.environ:
        anchor_bias_s = float(os.environ.get("KARAOKE_AUTO_OFFSET_ANCHOR_BIAS_SECS", "0.0"))
    else:
        anchor_bias_s = -0.25 if first_lrc_time >= late_start_min_secs else 0.0
    return float(anchor_frac), float(anchor_bias_s)


def _use_line_aware_segment_anchor(first_lrc_time: float) -> bool:
    min_first_lyric_s = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_LINE_AWARE_MIN_FIRST_LYRIC_SECS", "0.0")
    )
    return float(first_lrc_time) >= float(min_first_lyric_s)


def _build_segment_rows(
    segs: List[Tuple[float, float, str]],
    *,
    frequent_tokens: set[str],
    first_lrc_time: float,
) -> list[dict[str, Any]]:
    anchor_frac, anchor_bias_s = _segment_anchor_params(first_lrc_time)
    allow_line_aware_anchor = _use_line_aware_segment_anchor(first_lrc_time)
    seg_rows = []
    for st_abs, en_abs, txt in segs:
        seg_len = max(0.0, float(en_abs) - float(st_abs))
        anchor_abs = float(st_abs) + (seg_len * anchor_frac) + anchor_bias_s
        seg_toks = _tok(txt)
        seg_content = [t for t in seg_toks if t not in _COMMON_FILLER_TOKENS]
        seg_info = [t for t in (seg_content or seg_toks) if t not in frequent_tokens]
        seg_rows.append(
            {
                "st_abs": float(st_abs),
                "en_abs": float(en_abs),
                "anchor_abs": float(anchor_abs),
                "tokens": seg_toks,
                "content_tokens": seg_content,
                "informative_tokens": seg_info or (seg_content or seg_toks),
                "text": txt,
                "allow_line_aware_anchor": bool(allow_line_aware_anchor),
            }
        )
    return seg_rows


def _estimate_offset_from_rows(
    *,
    window_rows: list[dict[str, Any]],
    seg_rows: list[dict[str, Any]],
    max_abs_offset: float,
    return_confidence: bool,
) -> float | Tuple[float, float]:
    offsets: List[float] = []
    match_scores: List[float] = []
    last_seg_idx = -1
    last_anchor_abs = float("-inf")
    min_match_score = float(os.environ.get("KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE", "0.38"))
    reuse_tolerance_s = float(os.environ.get("KARAOKE_AUTO_OFFSET_SEGMENT_REUSE_TOLERANCE_SECS", "0.10"))
    allow_segment_reuse = bool(seg_rows and seg_rows[0].get("allow_line_aware_anchor"))
    for row in window_rows:
        best_score = 0.0
        best_start: Optional[float] = None
        best_idx: Optional[int] = None

        lrc_toks = list(row["tokens"])
        lrc_info = list(row["informative_tokens"] or lrc_toks)

        if allow_segment_reuse:
            seg_start_idx = int(max(0, last_seg_idx))
        else:
            seg_start_idx = int(max(0, last_seg_idx + 1))
        for seg_idx in range(seg_start_idx, len(seg_rows)):
            seg = seg_rows[seg_idx]
            seg_toks = list(seg["tokens"])
            seg_info = list(seg["informative_tokens"] or seg_toks)
            if not seg_toks:
                continue

            inter_info = _soft_token_intersection(lrc_info, seg_info)
            if not inter_info:
                continue

            score_info = len(inter_info) / max(len(set(lrc_info)), 1)
            score_all = _token_overlap(lrc_toks, seg_toks)
            score = (0.70 * score_info) + (0.30 * score_all)
            match_anchor_abs = _segment_match_anchor_abs(row, seg)
            if match_anchor_abs < (last_anchor_abs - reuse_tolerance_s):
                continue
            if score > best_score:
                best_score = score
                best_start = float(match_anchor_abs)
                best_idx = int(seg_idx)

        if best_start is None or best_score < min_match_score:
            continue

        last_seg_idx = int(best_idx if best_idx is not None else last_seg_idx)
        last_anchor_abs = float(max(last_anchor_abs, best_start))
        offsets.append(best_start - float(row["lrc_time"]))
        match_scores.append(best_score)

    if len(offsets) < 2:
        raise RuntimeError(f"Insufficient alignment matches ({len(offsets)})")

    final_offset = _robust_offset(offsets, max_abs_offset=max_abs_offset)
    if not return_confidence:
        return final_offset

    num_matches = len(offsets)
    mad = _mad(offsets)
    avg_match_score = sum(match_scores) / len(match_scores) if match_scores else 0.0
    match_factor = min(1.0, num_matches / 10.0)
    consistency_factor = max(0.0, 1.0 - (mad / 2.0))
    quality_factor = avg_match_score
    confidence = (0.5 * consistency_factor + 0.3 * quality_factor + 0.2 * match_factor)
    return (final_offset, confidence)


def _merge_anchor_specs(
    anchor_specs: list[dict[str, Any]],
    *,
    merge_gap_s: float,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for spec_index, spec in sorted(
        enumerate(anchor_specs),
        key=lambda item: (float(item[1]["clip_start_s"]), float(item[1]["clip_end_s"])),
    ):
        start_s = float(spec["clip_start_s"])
        end_s = float(spec["clip_end_s"])
        if merged and start_s <= (float(merged[-1]["clip_end_s"]) + float(merge_gap_s)):
            merged[-1]["clip_end_s"] = max(float(merged[-1]["clip_end_s"]), end_s)
            merged[-1]["spec_indices"].append(int(spec_index))
        else:
            merged.append(
                {
                    "clip_start_s": float(start_s),
                    "clip_end_s": float(end_s),
                    "spec_indices": [int(spec_index)],
                }
            )
    return merged


def _segments_for_anchor_spec(
    spec: dict[str, Any],
    merged_segments: List[Tuple[float, float, str]],
) -> List[Tuple[float, float, str]]:
    clip_start_s = float(spec["clip_start_s"])
    clip_end_s = float(spec["clip_end_s"])
    out: List[Tuple[float, float, str]] = []
    for st_abs, en_abs, txt in merged_segments:
        if float(en_abs) <= clip_start_s:
            continue
        if float(st_abs) >= clip_end_s:
            continue
        out.append(
            (
                max(float(st_abs), clip_start_s),
                min(float(en_abs), clip_end_s),
                str(txt),
            )
        )
    return out


def _transcribe_clip_segments(
    *,
    audio_path: Path,
    language: str,
    ffmpeg_bin: str,
    whisper_bin: str,
    model_path: str,
    clip_start_s: float,
    clip_dur_s: float,
    whisper_extra_args: Optional[List[str]] = None,
) -> List[Tuple[float, float, str]]:
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        wav = td_path / "clip.wav"

        _ffmpeg_clip_to_wav(ffmpeg_bin, audio_path, wav, start_s=clip_start_s, dur_s=clip_dur_s)
        segs = _transcribe_wav_segments(
            wav_path=wav,
            language=language,
            whisper_bin=whisper_bin,
            model_path=model_path,
            whisper_extra_args=whisper_extra_args,
        )

        return [
            (
                float(clip_start_s) + float(st),
                float(clip_start_s) + float(en),
                str(txt),
            )
            for st, en, txt in segs
        ]


def _transcribe_stitched_clip_segments(
    *,
    audio_path: Path,
    language: str,
    ffmpeg_bin: str,
    whisper_bin: str,
    model_path: str,
    clip_ranges: List[Tuple[float, float]],
    whisper_extra_args: Optional[List[str]] = None,
) -> List[List[Tuple[float, float, str]]]:
    if not clip_ranges:
        return []

    gap_s = max(0.0, float(os.environ.get("KARAOKE_AUTO_OFFSET_BATCH_STITCH_GAP_SECS", "0.35")))

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        stitched_wav = td_path / "stitched.wav"
        clip_entries: List[dict[str, float]] = []
        gap_frames = 0

        with wave.open(str(stitched_wav), "wb") as stitched:
            sample_width = 0
            frame_rate = 0
            channel_count = 0
            cursor_s = 0.0

            for clip_index, (clip_start_s, clip_dur_s) in enumerate(clip_ranges):
                clip_wav = td_path / f"clip_{clip_index}.wav"
                _ffmpeg_clip_to_wav(
                    ffmpeg_bin,
                    audio_path,
                    clip_wav,
                    start_s=float(clip_start_s),
                    dur_s=float(clip_dur_s),
                )

                with wave.open(str(clip_wav), "rb") as clip_reader:
                    if clip_index == 0:
                        channel_count = int(clip_reader.getnchannels())
                        sample_width = int(clip_reader.getsampwidth())
                        frame_rate = int(clip_reader.getframerate())
                        stitched.setnchannels(channel_count)
                        stitched.setsampwidth(sample_width)
                        stitched.setframerate(frame_rate)
                        gap_frames = int(round(gap_s * frame_rate))
                    else:
                        if (
                            int(clip_reader.getnchannels()) != channel_count
                            or int(clip_reader.getsampwidth()) != sample_width
                            or int(clip_reader.getframerate()) != frame_rate
                        ):
                            raise RuntimeError("Batch-stitched whisper clips disagree on WAV format")

                    frame_count = int(clip_reader.getnframes())
                    clip_data = clip_reader.readframes(frame_count)
                    actual_clip_dur_s = float(frame_count) / float(max(frame_rate, 1))
                    stitched_start_s = float(cursor_s)
                    stitched_end_s = float(stitched_start_s + actual_clip_dur_s)
                    stitched.writeframes(clip_data)
                    clip_entries.append(
                        {
                            "orig_start_s": float(clip_start_s),
                            "orig_end_s": float(clip_start_s) + float(actual_clip_dur_s),
                            "stitched_start_s": float(stitched_start_s),
                            "stitched_end_s": float(stitched_end_s),
                        }
                    )
                    cursor_s = float(stitched_end_s)

                    if clip_index < (len(clip_ranges) - 1) and gap_frames > 0:
                        stitched.writeframes(b"\x00" * gap_frames * channel_count * sample_width)
                        cursor_s += float(gap_frames) / float(max(frame_rate, 1))

        stitched_segments = _transcribe_wav_segments(
            wav_path=stitched_wav,
            language=language,
            whisper_bin=whisper_bin,
            model_path=model_path,
            whisper_extra_args=whisper_extra_args,
        )

        mapped_segments: List[List[Tuple[float, float, str]]] = [[] for _ in clip_entries]
        for stitched_start_s, stitched_end_s, txt in stitched_segments:
            midpoint_s = (float(stitched_start_s) + float(stitched_end_s)) / 2.0
            chosen_index: Optional[int] = None
            for clip_index, entry in enumerate(clip_entries):
                if float(entry["stitched_start_s"]) <= midpoint_s < float(entry["stitched_end_s"]):
                    chosen_index = int(clip_index)
                    break
            if chosen_index is None:
                continue

            entry = clip_entries[chosen_index]
            rel_start_s = max(0.0, float(stitched_start_s) - float(entry["stitched_start_s"]))
            rel_end_s = min(
                float(entry["stitched_end_s"]) - float(entry["stitched_start_s"]),
                max(rel_start_s, float(stitched_end_s) - float(entry["stitched_start_s"])),
            )
            mapped_segments[chosen_index].append(
                (
                    float(entry["orig_start_s"]) + rel_start_s,
                    float(entry["orig_start_s"]) + rel_end_s,
                    str(txt),
                )
            )

        return mapped_segments


def _robust_offset(offsets: List[float], max_abs_offset: float) -> float:
    if not offsets:
        raise RuntimeError("No offsets to aggregate")

    in_range = [v for v in offsets if abs(v) <= max_abs_offset]
    working = in_range if in_range else list(offsets)

    med = float(statistics.median(working))
    mad = _mad(working)
    if mad > 0.0:
        # Keep a wide band to remove only obvious bad matches.
        band = max(0.75, 3.5 * mad)
        trimmed = [v for v in working if abs(v - med) <= band]
        if trimmed:
            working = trimmed
            med = float(statistics.median(working))

    if abs(med) > max_abs_offset:
        # Guardrail: if we still landed on an extreme value, prefer in-range center.
        if in_range:
            med = float(statistics.median(in_range))
        else:
            med = 0.0

    return med


def estimate_offset(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: str,
    ffmpeg_bin: str,
    whisper_bin: str,
    model_path: str,
    clip_dur_s: float,
    max_abs_offset: float,
    whisper_extra_args: Optional[List[str]] = None,
    anchor_time_s: Optional[float] = None,
    return_confidence: bool = False,
) -> float | Tuple[float, float]:
    events, _meta = parse_lrc(lrc_path.read_text(encoding="utf-8", errors="replace"))
    candidates = _collect_usable_lrc_candidates(events)

    if not candidates:
        raise RuntimeError("No usable LRC lines found")

    anchor_spec = _build_window_rows(
        candidates,
        anchor_time_s=anchor_time_s,
        clip_dur_s=clip_dur_s,
    )

    segs = _transcribe_clip_segments(
        audio_path=audio_path,
        language=language,
        ffmpeg_bin=ffmpeg_bin,
        whisper_bin=whisper_bin,
        model_path=model_path,
        clip_start_s=float(anchor_spec["clip_start_s"]),
        clip_dur_s=clip_dur_s,
        whisper_extra_args=whisper_extra_args,
    )
    seg_rows = _build_segment_rows(
        segs,
        frequent_tokens=set(anchor_spec["frequent_tokens"]),
        first_lrc_time=float(candidates[0][0]) if candidates else 0.0,
    )
    return _estimate_offset_from_rows(
        window_rows=list(anchor_spec["window_rows"]),
        seg_rows=seg_rows,
        max_abs_offset=max_abs_offset,
        return_confidence=return_confidence,
    )


def estimate_offsets_batch(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: str,
    ffmpeg_bin: str,
    whisper_bin: str,
    model_path: str,
    clip_dur_s: float,
    max_abs_offset: float,
    anchor_times_s: list[Optional[float]],
    whisper_extra_args: Optional[List[str]] = None,
    return_confidence: bool = False,
) -> list[dict[str, Any]]:
    events, _meta = parse_lrc(lrc_path.read_text(encoding="utf-8", errors="replace"))
    candidates = _collect_usable_lrc_candidates(events)
    if not candidates:
        raise RuntimeError("No usable LRC lines found")

    anchor_specs = [
        _build_window_rows(
            candidates,
            anchor_time_s=anchor_time_s,
            clip_dur_s=clip_dur_s,
        )
        for anchor_time_s in anchor_times_s
    ]
    merge_gap_s = float(os.environ.get("KARAOKE_AUTO_OFFSET_BATCH_MERGE_GAP_SECS", "0.75"))
    merged_specs = _merge_anchor_specs(anchor_specs, merge_gap_s=merge_gap_s)

    merged_segments_by_spec_index: dict[int, List[Tuple[float, float, str]]] = {
        idx: [] for idx in range(len(anchor_specs))
    }
    stitched_batch_segments: Optional[List[List[Tuple[float, float, str]]]] = None
    stitch_batch_enabled = _env_flag("KARAOKE_AUTO_OFFSET_BATCH_STITCH", True)
    if stitch_batch_enabled and len(merged_specs) > 1:
        stitched_batch_segments = _transcribe_stitched_clip_segments(
            audio_path=audio_path,
            language=language,
            ffmpeg_bin=ffmpeg_bin,
            whisper_bin=whisper_bin,
            model_path=model_path,
            clip_ranges=[
                (
                    float(merged["clip_start_s"]),
                    max(0.1, float(merged["clip_end_s"]) - float(merged["clip_start_s"])),
                )
                for merged in merged_specs
            ],
            whisper_extra_args=whisper_extra_args,
        )

    for merged_index, merged in enumerate(merged_specs):
        clip_start_s = float(merged["clip_start_s"])
        clip_end_s = float(merged["clip_end_s"])
        if stitched_batch_segments is not None and merged_index < len(stitched_batch_segments):
            merged_segments = list(stitched_batch_segments[merged_index])
        else:
            merged_segments = _transcribe_clip_segments(
                audio_path=audio_path,
                language=language,
                ffmpeg_bin=ffmpeg_bin,
                whisper_bin=whisper_bin,
                model_path=model_path,
                clip_start_s=clip_start_s,
                clip_dur_s=max(0.1, clip_end_s - clip_start_s),
                whisper_extra_args=whisper_extra_args,
            )
        for spec_index in list(merged["spec_indices"]):
            merged_segments_by_spec_index[int(spec_index)] = _segments_for_anchor_spec(
                anchor_specs[int(spec_index)],
                merged_segments,
            )

    first_lrc_time = float(candidates[0][0]) if candidates else 0.0
    results: list[dict[str, Any]] = []
    for spec_index, spec in enumerate(anchor_specs):
        segs = merged_segments_by_spec_index.get(int(spec_index), [])
        try:
            seg_rows = _build_segment_rows(
                segs,
                frequent_tokens=set(spec["frequent_tokens"]),
                first_lrc_time=first_lrc_time,
            )
            value = _estimate_offset_from_rows(
                window_rows=list(spec["window_rows"]),
                seg_rows=seg_rows,
                max_abs_offset=max_abs_offset,
                return_confidence=return_confidence,
            )
            if return_confidence:
                offset_s, confidence = value  # type: ignore[misc]
            else:
                offset_s, confidence = value, None  # type: ignore[assignment]
            results.append(
                {
                    "anchor_time_s": spec["anchor_time_s"],
                    "offset_s": float(offset_s),
                    "confidence": (float(confidence) if isinstance(confidence, (int, float)) else None),
                    "error": None,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "anchor_time_s": spec["anchor_time_s"],
                    "offset_s": None,
                    "confidence": None,
                    "error": str(exc).strip() or exc.__class__.__name__,
                }
            )
    return results


def estimate_intro_gap_offset(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: str,
    ffmpeg_bin: str,
    whisper_bin: str,
    model_path: str,
    max_abs_offset: float,
    whisper_extra_args: Optional[List[str]] = None,
    return_confidence: bool = False,
) -> float | Tuple[float, float]:
    events, _meta = parse_lrc(lrc_path.read_text(encoding="utf-8", errors="replace"))
    candidates = _collect_usable_lrc_candidates(events)
    if not candidates:
        raise RuntimeError("No usable LRC lines found")

    first_lrc_time = float(candidates[0][0])
    scan_dur_s = float(
        os.environ.get(
            "KARAOKE_AUTO_OFFSET_INTRO_SCAN_CLIP_SECS",
            str(max(90.0, min(150.0, first_lrc_time + 60.0))),
        )
    )
    max_seed_lines = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_SCAN_MAX_LINES", "4"))))
    seed_rows = candidates[:max_seed_lines]

    segs = _transcribe_clip_segments(
        audio_path=audio_path,
        language=language,
        ffmpeg_bin=ffmpeg_bin,
        whisper_bin=whisper_bin,
        model_path=model_path,
        clip_start_s=0.0,
        clip_dur_s=scan_dur_s,
        whisper_extra_args=whisper_extra_args,
    )

    seg_rows: List[dict] = []
    for st_abs, en_abs, txt in segs:
        seg_toks = _tok(txt)
        if not seg_toks:
            continue
        seg_content = [t for t in seg_toks if t not in _COMMON_FILLER_TOKENS]
        seg_rows.append(
            {
                "start_abs": float(st_abs),
                "end_abs": float(en_abs),
                "tokens": seg_toks,
                "informative_tokens": seg_content or seg_toks,
                "text": txt,
            }
        )
    if not seg_rows:
        raise RuntimeError("No usable whisper segments in intro scan")

    bundle_rows: List[dict] = []
    max_bundle_lines = min(3, len(seed_rows))
    for bundle_size in range(max_bundle_lines, 0, -1):
        bundle = seed_rows[:bundle_size]
        bundle_text = " ".join(str(txt) for _t, txt in bundle)
        bundle_toks = _tok(bundle_text)
        bundle_info = [t for t in bundle_toks if t not in _COMMON_FILLER_TOKENS]
        if len(bundle_info or bundle_toks) < 2:
            continue
        bundle_rows.append(
            {
                "size": int(bundle_size),
                "lrc_time": float(bundle[0][0]),
                "tokens": bundle_toks,
                "informative_tokens": bundle_info or bundle_toks,
                "text": bundle_text,
            }
        )
    if not bundle_rows:
        raise RuntimeError("No usable intro LRC bundles found")

    grouped_segments: List[dict] = []
    max_segment_group = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_SCAN_MAX_SEGMENT_GROUP", "3"))))
    for group_size in range(1, max_segment_group + 1):
        for idx in range(0, len(seg_rows) - group_size + 1):
            group = seg_rows[idx : idx + group_size]
            combo_tokens: List[str] = []
            combo_info: List[str] = []
            combo_text_parts: List[str] = []
            for row in group:
                combo_tokens.extend(list(row["tokens"]))
                combo_info.extend(list(row["informative_tokens"]))
                combo_text_parts.append(str(row["text"]))
            grouped_segments.append(
                {
                    "start_abs": float(group[0]["start_abs"]),
                    "end_abs": float(group[-1]["end_abs"]),
                    "tokens": combo_tokens,
                    "informative_tokens": combo_info or combo_tokens,
                    "group_size": int(group_size),
                    "rows": list(group),
                    "text": " ".join(combo_text_parts),
                }
            )

    min_match_score = float(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_SCAN_MIN_MATCH_SCORE", "0.34"))
    min_positive_offset = float(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_SCAN_MIN_POSITIVE_OFFSET_SECS", "5.0"))
    matches: List[Tuple[float, float, int, float]] = []
    for bundle in bundle_rows:
        bundle_toks = list(bundle["tokens"])
        bundle_info = list(bundle["informative_tokens"] or bundle_toks)
        for seg in grouped_segments:
            seg_toks = list(seg["tokens"])
            seg_info = list(seg["informative_tokens"] or seg_toks)
            if not seg_toks:
                continue
            inter_info = _soft_token_intersection(bundle_info, seg_info)
            if len(inter_info) < 2:
                continue
            anchor_start = float(seg["start_abs"])
            for row in list(seg.get("rows") or []):
                row_info = list(row.get("informative_tokens") or row.get("tokens") or [])
                if _soft_token_intersection(bundle_info, row_info):
                    anchor_start = float(row.get("start_abs") or anchor_start)
                    break
            score_info = len(inter_info) / max(len(set(bundle_info)), 1)
            score_all = _token_overlap(bundle_toks, seg_toks)
            score = (0.70 * score_info) + (0.30 * score_all)
            offset = float(anchor_start) - float(bundle["lrc_time"])
            if score < min_match_score or offset < min_positive_offset:
                continue
            matches.append((float(score), float(offset), int(bundle["size"]), float(anchor_start)))

    if not matches:
        raise RuntimeError("No intro-gap matches found")

    best_score, best_offset, best_bundle_size, best_start_abs = sorted(
        matches,
        key=lambda item: (
            -float(item[0]),
            -int(item[2]),
            float(item[3]),
        ),
    )[0]
    if abs(best_offset) > max_abs_offset:
        raise RuntimeError(f"Intro-gap offset out of range ({best_offset:+.3f}s > ±{max_abs_offset:.1f}s)")

    if not return_confidence:
        return float(best_offset)

    confidence = min(
        0.99,
        max(
            0.0,
            (0.80 * float(best_score)) + (0.20 * (float(best_bundle_size) / 3.0)),
        ),
    )
    return (float(best_offset), float(confidence))


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Estimate global LRC offset using whisper.cpp")
    p.add_argument("--lrc", required=True, help="Path to timings/<slug>.lrc")
    p.add_argument("--audio", required=True, help="Path to audio file (mp3/wav)")
    p.add_argument("--language", default="auto", help="auto|en|es")
    p.add_argument("--ffmpeg", default=None, help="ffmpeg binary (defaults to PATH)")
    p.add_argument("--whisper-bin", default=None, help="whisper-cli binary")
    p.add_argument("--model", default=None, help="whisper.cpp ggml model path")
    p.add_argument("--clip-secs", type=float, default=35.0, help="Clip duration for transcription")
    p.add_argument(
        "--max-abs-offset",
        type=float,
        default=float(os.getenv("KARAOKE_MAX_AUTO_OFFSET_SECS", "45.0")),
        help="Clamp suspicious global offsets to +/- this many seconds",
    )
    p.add_argument("--print-offset-only", action="store_true", help="Print float offset and exit")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    lrc_path = Path(args.lrc)
    audio_path = Path(args.audio)

    if not lrc_path.exists():
        raise SystemExit(f"LRC not found: {lrc_path}")
    if not audio_path.exists():
        raise SystemExit(f"Audio not found: {audio_path}")

    ffmpeg_bin = args.ffmpeg or "ffmpeg"

    # Resolve whisper/bin + model
    whisper_bin = None
    if args.whisper_bin:
        whisper_bin = args.whisper_bin
    else:
        # Try repo-local layout used by this project
        root = Path(__file__).resolve().parent.parent
        for cand in (
            root / "whisper.cpp" / "build" / "bin" / "whisper-cli",
            root / "whisper.cpp" / "build" / "bin" / "main",
            root / "whisper.cpp" / "main",
        ):
            if cand.exists():
                whisper_bin = str(cand)
                break
        if whisper_bin is None:
            # PATH
            for name in ("whisper-cli", "main"):
                if shutil.which(name):
                    whisper_bin = name
                    break

    model_path = None
    if args.model:
        model_path = args.model
    else:
        root = Path(__file__).resolve().parent.parent
        for cand in (
            root / "whisper.cpp" / "models" / "ggml-tiny.bin",
            root / "whisper.cpp" / "models" / "ggml-base.bin",
        ):
            if cand.exists():
                model_path = str(cand)
                break

    if whisper_bin is None or model_path is None:
        raise SystemExit("whisper.cpp binary/model not found; provide --whisper-bin and --model")

    try:
        off = estimate_offset(
            lrc_path=lrc_path,
            audio_path=audio_path,
            language=args.language,
            ffmpeg_bin=ffmpeg_bin,
            whisper_bin=whisper_bin,
            model_path=model_path,
            clip_dur_s=float(args.clip_secs),
            max_abs_offset=float(args.max_abs_offset),
        )
    except Exception as e:
        if args.print_offset_only:
            print("0.0")
            return 0
        raise

    if args.print_offset_only:
        print(f"{off:.3f}")
    else:
        print(json.dumps({"offset_secs": off}, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
