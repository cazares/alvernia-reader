#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .common import CYAN, GREEN, RED, YELLOW, elapsed_ms, log, log_timing, now_perf_ms, resolve_ffmpeg_bin, run_cmd_capture

ROOT = Path(__file__).resolve().parent.parent
TIMINGS_DIR = ROOT / "timings"

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}

MUSIC_NOTE_CHARS = "♪♫♬♩♭♯"
MUSIC_NOTE_KEYWORDS = {"instrumental", "solo", "guitar solo", "piano solo"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    cleaned = str(raw).strip().lower()
    if cleaned in _TRUE_VALUES:
        return True
    if cleaned in _FALSE_VALUES:
        return False
    return bool(default)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return int(default)
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return int(default)


def _status_payload(
    *,
    name: str,
    status: str,
    elapsed_ms_value: float,
    reason: str = "",
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": str(name),
        "status": str(status),
        "elapsed_ms": round(max(0.0, float(elapsed_ms_value)), 1),
        "elapsed_sec": round(max(0.0, float(elapsed_ms_value)) / 1000.0, 3),
    }
    if reason:
        payload["reason"] = str(reason)
    if data:
        payload.update(data)
    if status == "passed":
        payload["passed"] = True
    elif status == "failed":
        payload["passed"] = False
    else:
        payload["passed"] = None
    return payload


def _normalize_text(raw: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\u00c0-\u017f]+", " ", str(raw or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _token_set(raw: str) -> set[str]:
    text = _normalize_text(raw)
    if not text:
        return set()
    return {token for token in text.split(" ") if token}


def _text_similarity_score(expected: str, observed: str) -> float:
    a = _normalize_text(expected)
    b = _normalize_text(observed)
    if not a or not b:
        return 0.0
    ratio = SequenceMatcher(None, a, b).ratio()
    tokens_a = _token_set(a)
    tokens_b = _token_set(b)
    if not tokens_a or not tokens_b:
        return max(0.0, min(1.0, ratio))
    overlap = len(tokens_a & tokens_b)
    precision = overlap / max(1, len(tokens_b))
    recall = overlap / max(1, len(tokens_a))
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return round(max(ratio, f1), 4)


def _is_music_only(text: str) -> bool:
    if not text:
        return False
    stripped = str(text).strip()
    if not stripped:
        return False
    if any(ch in MUSIC_NOTE_CHARS for ch in stripped):
        return True
    if not any(ch.isalnum() for ch in stripped):
        return True
    lower = stripped.lower()
    for keyword in MUSIC_NOTE_KEYWORDS:
        if keyword in lower:
            return True
    return False


def _read_timings_csv(csv_path: Path) -> List[Tuple[float, str]]:
    rows: List[Tuple[float, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)

        if header and "time_secs" in header:
            idx_time = header.index("time_secs")
            idx_text = header.index("text") if "text" in header else None
            for row in reader:
                if not row or len(row) <= idx_time:
                    continue
                try:
                    t = float(str(row[idx_time]).strip())
                except Exception:
                    continue
                txt = ""
                if idx_text is not None and len(row) > idx_text:
                    txt = str(row[idx_text] or "")
                rows.append((t, txt.strip()))
        else:
            # Legacy shape: time,text
            for row in reader:
                if len(row) < 2:
                    continue
                try:
                    t = float(str(row[0]).strip())
                except Exception:
                    continue
                rows.append((t, str(row[1] or "").strip()))

    rows.sort(key=lambda item: float(item[0]))
    return rows


def _extract_frame(
    *,
    ffmpeg_bin: Path,
    video_path: Path,
    at_sec: float,
    out_png: Path,
) -> Tuple[bool, str]:
    # Crop around the primary lyric region (upper-middle area), then scale for OCR stability.
    vf = "crop=iw*0.86:ih*0.44:iw*0.07:ih*0.08,scale=960:-1"
    cmd = [
        str(ffmpeg_bin),
        "-v",
        "error",
        "-y",
        "-ss",
        f"{max(0.0, float(at_sec)):.3f}",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-vf",
        vf,
        str(out_png),
    ]
    rc, out = run_cmd_capture(cmd, tag="SYNC_FRAME")
    if rc != 0:
        return False, out or f"ffmpeg failed rc={rc}"
    if not out_png.exists() or out_png.stat().st_size <= 0:
        return False, "frame extraction produced empty image"
    return True, ""


def _ocr_frame_text(image_path: Path) -> Tuple[bool, str]:
    tesseract_bin = shutil.which("tesseract")
    if not tesseract_bin:
        return False, "tesseract not found"

    def _run(lang: str) -> Tuple[int, str]:
        cmd = [
            tesseract_bin,
            str(image_path),
            "stdout",
            "--psm",
            "6",
            "-l",
            lang,
        ]
        rc, out = run_cmd_capture(cmd, tag="SYNC_OCR")
        return rc, out or ""

    for lang in ("eng+spa", "eng"):
        rc, out = _run(lang)
        if rc == 0:
            return True, out
    return False, "tesseract OCR failed"


def _parse_offset_list_env(name: str, default_values: List[float]) -> List[float]:
    raw = os.environ.get(name)
    values: List[float] = []
    if raw is not None and str(raw).strip():
        for token in re.split(r"[,\s;]+", str(raw).strip()):
            part = str(token or "").strip()
            if not part:
                continue
            try:
                sec = abs(float(part))
            except Exception:
                continue
            if sec >= 0.02:
                values.append(sec)
    if not values:
        values = [abs(float(v)) for v in default_values if float(v) >= 0.02]

    uniq: List[float] = []
    for value in sorted(values):
        rounded = round(max(0.02, float(value)), 3)
        if uniq and abs(uniq[-1] - rounded) < 0.005:
            continue
        uniq.append(rounded)
    return uniq


def _bounded_offsets(base_offsets: List[float], max_offset: float) -> List[float]:
    cap = max(0.05, float(max_offset))
    out: List[float] = []
    for value in base_offsets:
        bounded = min(max(0.05, float(value)), cap)
        rounded = round(bounded, 3)
        if out and abs(out[-1] - rounded) < 0.005:
            continue
        out.append(rounded)
    return out or [round(cap, 3)]


def run_visual_sync_check(
    *,
    video_path: Path,
    timings_csv_path: Path,
) -> Dict[str, Any]:
    start_ms = now_perf_ms()
    ffmpeg_bin = resolve_ffmpeg_bin()
    if not video_path.exists():
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason=f"video missing: {video_path}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )
    if not timings_csv_path.exists():
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason=f"timings missing: {timings_csv_path}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )
    if shutil.which(str(ffmpeg_bin)) is None and not ffmpeg_bin.exists():
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason=f"ffmpeg unavailable: {ffmpeg_bin}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )
    if not shutil.which("tesseract"):
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason="tesseract unavailable",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    similarity_threshold = max(0.0, min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_SIMILARITY_THRESHOLD", 0.42)))
    pass_rate_threshold = max(0.0, min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_PASS_RATE_THRESHOLD", 0.70)))
    sample_before_sec = max(0.05, _env_float("MIXTERIOSO_SYNC_VISUAL_SAMPLE_BEFORE_SEC", 0.25))
    sample_after_sec = max(0.05, _env_float("MIXTERIOSO_SYNC_VISUAL_SAMPLE_AFTER_SEC", 0.35))
    before_offsets_base = _parse_offset_list_env(
        "MIXTERIOSO_SYNC_VISUAL_BEFORE_OFFSETS_SEC",
        [sample_before_sec, max(sample_before_sec, 0.55)],
    )
    after_offsets_base = _parse_offset_list_env(
        "MIXTERIOSO_SYNC_VISUAL_AFTER_OFFSETS_SEC",
        [sample_after_sec, max(sample_after_sec, 0.95)],
    )
    transition_advantage_min = max(0.0, _env_float("MIXTERIOSO_SYNC_VISUAL_TRANSITION_ADVANTAGE_MIN", 0.06))
    dominance_slack = max(0.0, _env_float("MIXTERIOSO_SYNC_VISUAL_DOMINANCE_SLACK", 0.08))
    relaxed_multiplier = max(0.50, min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_RELAXED_MULTIPLIER", 0.80)))
    one_sided_support_multiplier = max(
        0.25,
        min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_ONE_SIDED_SUPPORT_MULTIPLIER", 0.50)),
    )
    similar_boundary_skip_threshold = max(
        0.0,
        min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_SKIP_SIMILAR_BOUNDARY_THRESHOLD", 0.92)),
    )
    min_informative_similarity = max(
        0.0,
        min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_MIN_INFORMATIVE_SIMILARITY", 0.30)),
    )
    dominant_side_similarity = max(
        0.0,
        min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_DOMINANT_SIDE_SIMILARITY", 0.75)),
    )
    dominant_side_weak_adv_allowance = max(
        0.0,
        _env_float("MIXTERIOSO_SYNC_VISUAL_DOMINANT_SIDE_WEAK_ADV_ALLOWANCE", 0.30),
    )
    single_side_dominance_min_similarity = max(
        0.0,
        min(1.0, _env_float("MIXTERIOSO_SYNC_VISUAL_SINGLE_SIDE_DOMINANCE_MIN_SIMILARITY", 0.75)),
    )
    single_side_dominance_min_advantage = max(
        0.0,
        _env_float("MIXTERIOSO_SYNC_VISUAL_SINGLE_SIDE_DOMINANCE_MIN_ADVANTAGE", transition_advantage_min),
    )
    similarity_threshold_relaxed = max(0.0, min(1.0, similarity_threshold * relaxed_multiplier))
    min_boundary_time_sec = max(0.0, _env_float("MIXTERIOSO_SYNC_VISUAL_MIN_BOUNDARY_TIME_SEC", 7.0))
    max_boundaries = max(1, _env_int("MIXTERIOSO_SYNC_VISUAL_MAX_BOUNDARIES", 12))
    min_boundaries = max(1, _env_int("MIXTERIOSO_SYNC_VISUAL_MIN_BOUNDARIES", 4))

    try:
        rows = _read_timings_csv(timings_csv_path)
    except Exception as exc:
        return _status_payload(
            name="visual_sync",
            status="failed",
            reason=f"failed to read timings csv: {exc}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    lyric_rows = [(t, txt) for (t, txt) in rows if str(txt).strip() and not _is_music_only(txt)]
    if len(lyric_rows) < 2:
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason="not enough lyric lines in timings for boundary check",
            elapsed_ms_value=elapsed_ms(start_ms),
            data={"candidate_boundaries": 0},
        )

    candidates: List[Tuple[float, str, str, float, Optional[float]]] = []
    skipped_similar_boundaries = 0
    skipped_single_side_boundaries = 0
    for idx in range(1, len(lyric_rows)):
        boundary_t = float(lyric_rows[idx][0])
        if boundary_t < min_boundary_time_sec:
            continue
        prev_text = str(lyric_rows[idx - 1][1] or "").strip()
        next_text = str(lyric_rows[idx][1] or "").strip()
        if not prev_text or not next_text:
            continue
        # Repeated/near-identical adjacent lines carry little timing signal.
        if _text_similarity_score(prev_text, next_text) >= similar_boundary_skip_threshold:
            skipped_similar_boundaries += 1
            continue
        prev_boundary_t = float(lyric_rows[idx - 1][0])
        next_boundary_t = float(lyric_rows[idx + 1][0]) if idx + 1 < len(lyric_rows) else None
        prev_gap = max(0.05, boundary_t - prev_boundary_t)
        next_gap = (max(0.05, next_boundary_t - boundary_t) if next_boundary_t is not None else None)
        candidates.append((boundary_t, prev_text, next_text, prev_gap, next_gap))

    if len(candidates) < min_boundaries:
        return _status_payload(
            name="visual_sync",
            status="skipped",
            reason=f"not enough boundaries after {min_boundary_time_sec:.1f}s",
            elapsed_ms_value=elapsed_ms(start_ms),
            data={"candidate_boundaries": len(candidates)},
        )

    if len(candidates) > max_boundaries:
        sampled: List[Tuple[float, str, str, float, Optional[float]]] = []
        used: set[int] = set()
        for i in range(max_boundaries):
            idx = int(math.floor((i * len(candidates)) / max_boundaries))
            idx = max(0, min(len(candidates) - 1, idx))
            if idx in used:
                continue
            used.add(idx)
            sampled.append(candidates[idx])
        candidates = sampled or candidates[:max_boundaries]

    boundary_results: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="mix-sync-visual-") as td:
        tmp_dir = Path(td)
        for i, (boundary_t, prev_text, next_text, prev_gap, next_gap) in enumerate(candidates):
            before_offsets = _bounded_offsets(before_offsets_base, max_offset=max(0.10, prev_gap * 0.80))
            if next_gap is None:
                max_after_offset = max(after_offsets_base)
            else:
                max_after_offset = max(0.10, float(next_gap) * 0.80)
            after_offsets = _bounded_offsets(after_offsets_base, max_offset=max_after_offset)

            before_scores: List[Dict[str, float]] = []
            after_scores: List[Dict[str, float]] = []
            frame_error = ""
            ocr_error = ""

            for j, offset_sec in enumerate(before_offsets):
                before_t = max(0.0, boundary_t - offset_sec)
                before_png = tmp_dir / f"b{i:03d}_{j:02d}.png"
                ok_before, err_before = _extract_frame(
                    ffmpeg_bin=ffmpeg_bin,
                    video_path=video_path,
                    at_sec=before_t,
                    out_png=before_png,
                )
                if not ok_before:
                    frame_error = err_before or "frame extraction failed"
                    continue
                ocr_ok_before, ocr_before = _ocr_frame_text(before_png)
                if not ocr_ok_before:
                    ocr_error = "ocr failed"
                    continue

                before_prev_score = _text_similarity_score(prev_text, ocr_before)
                before_next_score = _text_similarity_score(next_text, ocr_before)
                before_scores.append(
                    {
                        "offset_sec": round(offset_sec, 3),
                        "prev_score": round(before_prev_score, 3),
                        "next_score": round(before_next_score, 3),
                        "prev_minus_next": round(before_prev_score - before_next_score, 3),
                    }
                )

            for j, offset_sec in enumerate(after_offsets):
                after_t = max(0.0, boundary_t + offset_sec)
                after_png = tmp_dir / f"a{i:03d}_{j:02d}.png"
                ok_after, err_after = _extract_frame(
                    ffmpeg_bin=ffmpeg_bin,
                    video_path=video_path,
                    at_sec=after_t,
                    out_png=after_png,
                )
                if not ok_after:
                    frame_error = err_after or "frame extraction failed"
                    continue
                ocr_ok_after, ocr_after = _ocr_frame_text(after_png)
                if not ocr_ok_after:
                    ocr_error = "ocr failed"
                    continue

                after_prev_score = _text_similarity_score(prev_text, ocr_after)
                after_next_score = _text_similarity_score(next_text, ocr_after)
                after_scores.append(
                    {
                        "offset_sec": round(offset_sec, 3),
                        "prev_score": round(after_prev_score, 3),
                        "next_score": round(after_next_score, 3),
                        "next_minus_prev": round(after_next_score - after_prev_score, 3),
                    }
                )

            if not before_scores or not after_scores:
                failure_reason = frame_error or ocr_error or "insufficient scored samples"
                boundary_results.append(
                    {
                        "boundary_sec": round(boundary_t, 3),
                        "passed": False,
                        "reason": failure_reason,
                    }
                )
                continue

            best_before = max(before_scores, key=lambda item: (item["prev_minus_next"], item["prev_score"]))
            best_after = max(after_scores, key=lambda item: (item["next_minus_prev"], item["next_score"]))

            before_prev_score = float(best_before["prev_score"])
            before_next_score = float(best_before["next_score"])
            after_prev_score = float(best_after["prev_score"])
            after_next_score = float(best_after["next_score"])

            before_adv = before_prev_score - before_next_score
            after_adv = after_next_score - after_prev_score
            strongest_similarity = max(before_prev_score, before_next_score, after_prev_score, after_next_score)
            if strongest_similarity < min_informative_similarity:
                boundary_results.append(
                    {
                        "boundary_sec": round(boundary_t, 3),
                        "passed": None,
                        "reason": "uninformative boundary",
                        "before_prev_score": round(before_prev_score, 3),
                        "before_next_score": round(before_next_score, 3),
                        "after_prev_score": round(after_prev_score, 3),
                        "after_next_score": round(after_next_score, 3),
                    }
                )
                continue
            before_prev_dom = (
                before_prev_score >= single_side_dominance_min_similarity
                and (before_prev_score - before_next_score) >= single_side_dominance_min_advantage
            )
            before_next_dom = (
                before_next_score >= single_side_dominance_min_similarity
                and (before_next_score - before_prev_score) >= single_side_dominance_min_advantage
            )
            after_prev_dom = (
                after_prev_score >= single_side_dominance_min_similarity
                and (after_prev_score - after_next_score) >= single_side_dominance_min_advantage
            )
            after_next_dom = (
                after_next_score >= single_side_dominance_min_similarity
                and (after_next_score - after_prev_score) >= single_side_dominance_min_advantage
            )
            # If both snapshots strongly favor the same side, this boundary does not
            # provide evidence about the actual transition moment.
            if (before_prev_dom and after_prev_dom) or (before_next_dom and after_next_dom):
                skipped_single_side_boundaries += 1
                boundary_results.append(
                    {
                        "boundary_sec": round(boundary_t, 3),
                        "passed": None,
                        "reason": "single-side dominance boundary",
                        "before_prev_score": round(before_prev_score, 3),
                        "before_next_score": round(before_next_score, 3),
                        "after_prev_score": round(after_prev_score, 3),
                        "after_next_score": round(after_next_score, 3),
                    }
                )
                continue
            support_floor = similarity_threshold_relaxed * one_sided_support_multiplier
            has_before_support = before_prev_score >= support_floor
            has_after_support = after_next_score >= support_floor
            strong_before = before_prev_score >= similarity_threshold_relaxed and before_adv >= transition_advantage_min
            strong_after = after_next_score >= similarity_threshold_relaxed and after_adv >= transition_advantage_min
            soft_before = has_before_support and before_adv >= -dominance_slack
            soft_after = has_after_support and after_adv >= -dominance_slack

            passed = (
                (strong_before and strong_after)
                or (strong_before and soft_after and has_after_support)
                or (strong_after and soft_before and has_before_support)
            )

            if not passed:
                dominant_after = strong_after and after_next_score >= dominant_side_similarity
                dominant_before = strong_before and before_prev_score >= dominant_side_similarity
                if dominant_after and has_before_support and before_adv >= -dominant_side_weak_adv_allowance:
                    passed = True
                elif dominant_before and has_after_support and after_adv >= -dominant_side_weak_adv_allowance:
                    passed = True

            boundary_result: Dict[str, Any] = {
                "boundary_sec": round(boundary_t, 3),
                "passed": bool(passed),
                "before_prev_score": round(before_prev_score, 3),
                "before_next_score": round(before_next_score, 3),
                "after_prev_score": round(after_prev_score, 3),
                "after_next_score": round(after_next_score, 3),
                "before_offset_sec": round(float(best_before["offset_sec"]), 3),
                "after_offset_sec": round(float(best_after["offset_sec"]), 3),
                "before_advantage": round(before_adv, 3),
                "after_advantage": round(after_adv, 3),
            }
            if not passed:
                if before_prev_score < similarity_threshold_relaxed and after_next_score < similarity_threshold_relaxed:
                    boundary_result["reason"] = "similarity threshold not met"
                else:
                    boundary_result["reason"] = "transition dominance threshold not met"
            boundary_results.append(boundary_result)

    considered = [item for item in boundary_results if item.get("passed") is not None]
    evaluated = len(considered)
    skipped_uninformative = max(0, len(boundary_results) - evaluated)
    passed_count = sum(1 for item in considered if bool(item.get("passed")))
    pass_rate = (passed_count / evaluated) if evaluated > 0 else 0.0
    if evaluated < min_boundaries:
        status = "skipped"
    elif pass_rate >= pass_rate_threshold:
        status = "passed"
    else:
        status = "failed"
    reason = ""
    if status != "passed":
        reason = (
            f"visual pass rate {pass_rate:.2f} below threshold {pass_rate_threshold:.2f}"
            if status == "failed"
            else f"evaluated boundaries {evaluated} below minimum {min_boundaries}"
        )

    payload = _status_payload(
        name="visual_sync",
        status=status,
        elapsed_ms_value=elapsed_ms(start_ms),
        reason=reason,
        data={
            "evaluated_boundaries": evaluated,
            "passed_boundaries": passed_count,
            "pass_rate": round(pass_rate, 3),
            "similarity_threshold": round(similarity_threshold, 3),
            "similarity_threshold_relaxed": round(similarity_threshold_relaxed, 3),
            "pass_rate_threshold": round(pass_rate_threshold, 3),
            "transition_advantage_min": round(transition_advantage_min, 3),
            "dominance_slack": round(dominance_slack, 3),
            "one_sided_support_multiplier": round(one_sided_support_multiplier, 3),
            "similar_boundary_skip_threshold": round(similar_boundary_skip_threshold, 3),
            "min_informative_similarity": round(min_informative_similarity, 3),
            "dominant_side_similarity": round(dominant_side_similarity, 3),
            "dominant_side_weak_adv_allowance": round(dominant_side_weak_adv_allowance, 3),
            "single_side_dominance_min_similarity": round(single_side_dominance_min_similarity, 3),
            "single_side_dominance_min_advantage": round(single_side_dominance_min_advantage, 3),
            "skipped_similar_boundaries": skipped_similar_boundaries,
            "skipped_single_side_boundaries": skipped_single_side_boundaries,
            "skipped_uninformative_boundaries": skipped_uninformative,
            "before_offsets_base_sec": before_offsets_base,
            "after_offsets_base_sec": after_offsets_base,
            "samples": boundary_results[:12],
        },
    )
    return payload


def run_audio_offset_sync_check(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: str = "auto",
) -> Dict[str, Any]:
    start_ms = now_perf_ms()
    if not lrc_path.exists():
        return _status_payload(
            name="audio_offset",
            status="skipped",
            reason=f"lrc missing: {lrc_path}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )
    if not audio_path.exists():
        return _status_payload(
            name="audio_offset",
            status="skipped",
            reason=f"audio/video missing: {audio_path}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    try:
        from . import lrc_offset_whisper as whisper_offset
    except Exception as exc:
        return _status_payload(
            name="audio_offset",
            status="skipped",
            reason=f"whisper module unavailable: {exc}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    whisper_bin = whisper_offset._find_whispercpp_bin(os.environ.get("MIXTERIOSO_WHISPER_BIN"))
    model_path = whisper_offset._find_model(os.environ.get("MIXTERIOSO_WHISPER_MODEL"))
    if not whisper_bin or not model_path:
        return _status_payload(
            name="audio_offset",
            status="skipped",
            reason="whisper runtime unavailable (bin/model not found)",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    max_abs_offset = abs(_env_float("MIXTERIOSO_SYNC_AUDIO_MAX_ABS_OFFSET_SEC", 0.70))
    min_confidence = max(0.0, min(1.0, _env_float("MIXTERIOSO_SYNC_AUDIO_MIN_CONFIDENCE", 0.65)))
    clip_secs = max(8.0, _env_float("MIXTERIOSO_SYNC_AUDIO_CLIP_SECS", 30.0))
    ffmpeg_bin = resolve_ffmpeg_bin()

    try:
        offset_sec, confidence = whisper_offset.estimate_offset(
            lrc_path=lrc_path,
            audio_path=audio_path,
            language=language or "auto",
            ffmpeg_bin=str(ffmpeg_bin),
            whisper_bin=whisper_bin,
            model_path=model_path,
            clip_dur_s=clip_secs,
            max_abs_offset=max_abs_offset,
            whisper_extra_args=[],
            return_confidence=True,
        )
    except Exception as exc:
        return _status_payload(
            name="audio_offset",
            status="failed",
            reason=f"offset estimation failed: {exc}",
            elapsed_ms_value=elapsed_ms(start_ms),
        )

    passed = abs(float(offset_sec)) <= max_abs_offset and float(confidence) >= min_confidence
    status = "passed" if passed else "failed"
    reason = ""
    if not passed:
        reason = (
            f"offset={float(offset_sec):+.3f}s (<= {max_abs_offset:.3f}s) "
            f"confidence={float(confidence):.2f} (>= {min_confidence:.2f})"
        )
    return _status_payload(
        name="audio_offset",
        status=status,
        elapsed_ms_value=elapsed_ms(start_ms),
        reason=reason,
        data={
            "offset_sec": round(float(offset_sec), 3),
            "confidence": round(float(confidence), 3),
            "max_abs_offset_sec": round(float(max_abs_offset), 3),
            "min_confidence": round(float(min_confidence), 3),
        },
    )


def _download_youtube_video_mp4(youtube_video_url: str, *, out_dir: Path) -> Tuple[Optional[Path], str]:
    yt_dlp_bin = shutil.which("yt-dlp")
    if not yt_dlp_bin:
        return None, "yt-dlp unavailable"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_template = out_dir / "post_upload.%(ext)s"
    cmd = [
        yt_dlp_bin,
        "--quiet",
        "--no-warnings",
        "--no-progress",
        "--no-playlist",
        "--no-part",
        "--retries",
        "2",
        "--fragment-retries",
        "2",
        "--format",
        "mp4[height<=360][ext=mp4]/mp4[ext=mp4]/best[ext=mp4]/best",
        "-o",
        str(out_template),
        "--print",
        "after_move:filepath",
        str(youtube_video_url),
    ]
    rc, out = run_cmd_capture(cmd, tag="SYNC_DL")
    if rc != 0:
        return None, out or f"yt-dlp failed rc={rc}"

    lines = [line.strip() for line in str(out or "").splitlines() if line.strip()]
    for line in reversed(lines):
        candidate = Path(line)
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate, ""

    matches = sorted(out_dir.glob("post_upload.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in matches:
        if path.is_file() and path.stat().st_size > 0:
            return path, ""
    return None, "yt-dlp finished without a local MP4 path"


def _scope_from_checks(scope_name: str, checks: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    considered = [item for item in checks.values() if isinstance(item, dict) and item.get("passed") is not None]
    if not considered:
        scope_passed: Optional[bool] = None
    else:
        scope_passed = all(bool(item.get("passed")) for item in considered)
    total_ms = sum(float(item.get("elapsed_ms") or 0.0) for item in checks.values() if isinstance(item, dict))
    return {
        "scope": scope_name,
        "passed": scope_passed,
        "elapsed_ms": round(total_ms, 1),
        "elapsed_sec": round(total_ms / 1000.0, 3),
        "checks": checks,
    }


def _sum_scope_ms(scope: Optional[Dict[str, Any]]) -> float:
    if not isinstance(scope, dict):
        return 0.0
    try:
        return max(0.0, float(scope.get("elapsed_ms") or 0.0))
    except Exception:
        return 0.0


def _flatten_timing_seconds(pre_scope: Optional[Dict[str, Any]], post_scope: Optional[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if isinstance(pre_scope, dict):
        checks = pre_scope.get("checks") or {}
        if isinstance(checks, dict):
            for key, item in checks.items():
                if not isinstance(item, dict):
                    continue
                out[f"pre_upload.{key}"] = round(float(item.get("elapsed_sec") or 0.0), 3)
        out["pre_upload.total"] = round(_sum_scope_ms(pre_scope) / 1000.0, 3)
    if isinstance(post_scope, dict):
        checks = post_scope.get("checks") or {}
        if isinstance(checks, dict):
            for key, item in checks.items():
                if not isinstance(item, dict):
                    continue
                out[f"post_upload.{key}"] = round(float(item.get("elapsed_sec") or 0.0), 3)
        out["post_upload.total"] = round(_sum_scope_ms(post_scope) / 1000.0, 3)
    out["total"] = round(((_sum_scope_ms(pre_scope) + _sum_scope_ms(post_scope)) / 1000.0), 3)
    return out


def run_sync_quality_checks(
    *,
    slug: str,
    local_video_path: Path,
    youtube_video_url: Optional[str] = None,
    run_pre_upload: bool = True,
    run_post_upload: bool = False,
    timings_csv_path: Optional[Path] = None,
    lrc_path: Optional[Path] = None,
    language: str = "auto",
) -> Dict[str, Any]:
    start_ms = now_perf_ms()
    started_at_utc = _utc_now_iso()
    clean_slug = str(slug or "").strip() or "song"
    timings_csv = timings_csv_path or (TIMINGS_DIR / f"{clean_slug}.csv")
    lrc = lrc_path or (TIMINGS_DIR / f"{clean_slug}.lrc")

    pre_scope: Optional[Dict[str, Any]] = None
    post_scope: Optional[Dict[str, Any]] = None

    if run_pre_upload:
        checks: Dict[str, Dict[str, Any]] = {}
        t0 = now_perf_ms()
        checks["audio_offset"] = run_audio_offset_sync_check(
            lrc_path=lrc,
            audio_path=local_video_path,
            language=language,
        )
        log_timing("step5", "sync_pre_audio_offset", t0, color=CYAN)
        t1 = now_perf_ms()
        checks["visual_sync"] = run_visual_sync_check(
            video_path=local_video_path,
            timings_csv_path=timings_csv,
        )
        log_timing("step5", "sync_pre_visual", t1, color=CYAN)
        pre_scope = _scope_from_checks("pre_upload", checks)

    post_tmp_dir: Optional[Path] = None
    try:
        if run_post_upload:
            checks = {}
            if not str(youtube_video_url or "").strip():
                checks["download"] = _status_payload(
                    name="download",
                    status="skipped",
                    reason="youtube_video_url missing",
                    elapsed_ms_value=0.0,
                )
                post_scope = _scope_from_checks("post_upload", checks)
            else:
                dl_t0 = now_perf_ms()
                post_tmp_dir = Path(tempfile.mkdtemp(prefix="mix-sync-post-"))
                downloaded_video, dl_err = _download_youtube_video_mp4(str(youtube_video_url).strip(), out_dir=post_tmp_dir)
                if not downloaded_video:
                    checks["download"] = _status_payload(
                        name="download",
                        status="failed",
                        reason=dl_err or "download failed",
                        elapsed_ms_value=elapsed_ms(dl_t0),
                    )
                    log_timing("step5", "sync_post_download", dl_t0, color=CYAN)
                    post_scope = _scope_from_checks("post_upload", checks)
                else:
                    checks["download"] = _status_payload(
                        name="download",
                        status="passed",
                        elapsed_ms_value=elapsed_ms(dl_t0),
                        data={"path": str(downloaded_video)},
                    )
                    log_timing("step5", "sync_post_download", dl_t0, color=CYAN)

                    vis_t0 = now_perf_ms()
                    checks["visual_sync"] = run_visual_sync_check(
                        video_path=downloaded_video,
                        timings_csv_path=timings_csv,
                    )
                    log_timing("step5", "sync_post_visual", vis_t0, color=CYAN)

                    if _env_bool("MIXTERIOSO_SYNC_CHECK_POST_AUDIO", False):
                        aud_t0 = now_perf_ms()
                        checks["audio_offset"] = run_audio_offset_sync_check(
                            lrc_path=lrc,
                            audio_path=downloaded_video,
                            language=language,
                        )
                        log_timing("step5", "sync_post_audio_offset", aud_t0, color=CYAN)
                    post_scope = _scope_from_checks("post_upload", checks)
    finally:
        if post_tmp_dir and post_tmp_dir.exists() and not _env_bool("MIXTERIOSO_SYNC_KEEP_TEMP_FILES", False):
            shutil.rmtree(post_tmp_dir, ignore_errors=True)

    scope_values = [scope.get("passed") for scope in (pre_scope, post_scope) if isinstance(scope, dict)]
    considered_values = [value for value in scope_values if value is not None]
    overall_passed: Optional[bool]
    if not considered_values:
        overall_passed = None
    else:
        overall_passed = all(bool(value) for value in considered_values)

    total_elapsed_ms = elapsed_ms(start_ms)
    log_timing("step5", "sync_total", start_ms, color=CYAN)
    if overall_passed is True:
        log("SYNC", f"Sync checks passed for '{clean_slug}'", GREEN)
    elif overall_passed is False:
        log("SYNC", f"Sync checks failed for '{clean_slug}'", RED)
    else:
        log("SYNC", f"Sync checks skipped for '{clean_slug}'", YELLOW)

    payload: Dict[str, Any] = {
        "slug": clean_slug,
        "started_at_utc": started_at_utc,
        "finished_at_utc": _utc_now_iso(),
        "overall_passed": overall_passed,
        "pre_upload": pre_scope,
        "post_upload": post_scope,
        "timings_sec": _flatten_timing_seconds(pre_scope, post_scope),
        "elapsed_ms": round(total_elapsed_ms, 1),
        "elapsed_sec": round(total_elapsed_ms / 1000.0, 3),
    }
    return payload


def merge_sync_check_runs(*runs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    pre_scope = None
    post_scope = None
    total_ms = 0.0
    slug = "song"
    for run in runs:
        if not isinstance(run, dict):
            continue
        if run.get("slug"):
            slug = str(run.get("slug"))
        if run.get("pre_upload"):
            pre_scope = run.get("pre_upload")
        if run.get("post_upload"):
            post_scope = run.get("post_upload")
        try:
            total_ms += float(run.get("elapsed_ms") or 0.0)
        except Exception:
            pass

    scope_values = [scope.get("passed") for scope in (pre_scope, post_scope) if isinstance(scope, dict)]
    considered_values = [value for value in scope_values if value is not None]
    if not considered_values:
        overall_passed: Optional[bool] = None
    else:
        overall_passed = all(bool(value) for value in considered_values)

    return {
        "slug": slug,
        "overall_passed": overall_passed,
        "pre_upload": pre_scope,
        "post_upload": post_scope,
        "timings_sec": _flatten_timing_seconds(pre_scope, post_scope),
        "elapsed_ms": round(max(0.0, total_ms), 1),
        "elapsed_sec": round(max(0.0, total_ms) / 1000.0, 3),
        "merged_at_utc": _utc_now_iso(),
    }
