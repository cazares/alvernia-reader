#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import shlex
import re
import json
import math
import random
import statistics
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

from scripts.common import log, log_timing, now_perf_ms, GREEN, YELLOW, CYAN, resolve_ffmpeg_bin

ROOT = Path(__file__).resolve().parent.parent
TIMINGS_DIR = ROOT / "timings"


def _extract_event(ev: Any):
    # Supports:
    # - LrcEvent (has .t and .text)
    # - object with .time/.text
    # - tuple/list (time, text)
    # - dict with keys time/text or t/text
    if hasattr(ev, "t") and hasattr(ev, "text"):
        return float(ev.t), str(ev.text)
    if hasattr(ev, "time") and hasattr(ev, "text"):
        return float(ev.time), str(ev.text)
    if isinstance(ev, (list, tuple)) and len(ev) >= 2:
        return float(ev[0]), str(ev[1])
    if isinstance(ev, dict):
        t = ev.get("time", ev.get("t", 0.0))
        txt = ev.get("text", "")
        return float(t), str(txt)
    raise TypeError(f"Unsupported event type: {type(ev)}")


def _write_timings_csv(path: Path, events: Iterable[Any]) -> int:
    events = list(events)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["line_index", "time_secs", "text"])
        for i, ev in enumerate(events):
            t, text = _extract_event(ev)
            w.writerow([i, f"{t:.3f}", (text or "").strip()])
    return len(events)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


STRICT_REQUIRE_LYRICS = _env_flag("MIXTERIOSO_STRICT_REQUIRE_LYRICS", True)
_AUTO_OFFSET_LANG_CACHE: dict[str, str] = {}


_EN_HINT_TOKENS = {
    "the",
    "and",
    "you",
    "your",
    "my",
    "me",
    "we",
    "our",
    "with",
    "for",
    "from",
    "that",
    "this",
    "what",
    "when",
    "where",
    "keep",
    "love",
    "heart",
    "world",
}

_ES_HINT_TOKENS = {
    "que",
    "de",
    "la",
    "el",
    "los",
    "las",
    "con",
    "por",
    "para",
    "como",
    "pero",
    "porque",
    "cuando",
    "donde",
    "amor",
    "vida",
    "corazon",
    "baila",
    "noche",
}

_ANCHOR_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "con",
    "de",
    "del",
    "el",
    "en",
    "for",
    "i",
    "in",
    "is",
    "it",
    "la",
    "las",
    "los",
    "me",
    "mi",
    "my",
    "no",
    "of",
    "on",
    "or",
    "para",
    "por",
    "que",
    "so",
    "te",
    "the",
    "to",
    "tu",
    "we",
    "with",
    "y",
    "you",
    "your",
}


def _guess_auto_offset_language(lrc_path: Path) -> str:
    key = str(lrc_path.resolve()) if lrc_path.exists() else str(lrc_path)
    cached = _AUTO_OFFSET_LANG_CACHE.get(key)
    if cached:
        return cached

    guess = "en"
    try:
        raw = lrc_path.read_text(encoding="utf-8", errors="replace")
        text = re.sub(r"\[[^\]]+\]", " ", raw)
        _norm_text = re.sub(r"[^a-zA-Z0-9\u00C0-\u017F\s']", " ", text).lower()
        toks = re.findall(r"[a-z\u00C0-\u017F']+", _norm_text)
        if toks:
            accent_chars = sum(1 for ch in _norm_text if ch in "áéíóúñü")
            en_hits = sum(1 for t in toks if t in _EN_HINT_TOKENS)
            es_hits = sum(1 for t in toks if t in _ES_HINT_TOKENS)
            if accent_chars >= 2:
                es_hits += 2
            if es_hits >= max(3, int(en_hits * 1.15)):
                guess = "es"
            elif en_hits >= max(3, int(es_hits * 1.10)):
                guess = "en"
            else:
                # Prefer English in ambiguous cases to avoid unstable Whisper auto language picks.
                guess = "en"
    except Exception:
        guess = "en"

    _AUTO_OFFSET_LANG_CACHE[key] = guess
    return guess


def _tokenize_anchor_text(text: str) -> list[str]:
    return [t.strip("'").lower() for t in re.findall(r"[a-zA-Z0-9\u00C0-\u017F']+", str(text or ""))]


def _is_low_information_line(tokens: list[str]) -> bool:
    if len(tokens) < 3:
        return True
    unique = {t for t in tokens if t}
    informative = [t for t in unique if len(t) >= 3 and t not in _ANCHOR_STOPWORDS]
    if len(informative) >= 2:
        return False

    # Reject short repeated vocalizations like "ahhh", "ohhh", "na na".
    if unique and all(re.fullmatch(r"(ha+|ah+|oh+|na+|la+|wo+)", t or "") for t in unique):
        return True
    return True


def _collect_anchor_times(lrc_path: Path) -> list[float]:
    return [float(row["time"]) for row in _collect_anchor_rows(lrc_path)]


def _collect_anchor_rows(lrc_path: Path) -> list[dict[str, Any]]:
    from scripts.lrc_utils import parse_lrc

    events, _meta = parse_lrc(str(lrc_path))
    rows: list[dict[str, Any]] = []
    for ev in events:
        t, text = _extract_event(ev)
        txt = str(text or "").strip()
        if not txt:
            continue
        toks = _tokenize_anchor_text(txt)
        if _is_low_information_line(toks):
            continue
        informative = [tok for tok in toks if len(tok) >= 3 and tok not in _ANCHOR_STOPWORDS]
        rows.append(
            {
                "time": float(t),
                "text": txt,
                "tokens": toks,
                "informative_tokens": informative,
                "score": 0.0,
            }
        )

    if not rows:
        # Fallback to legacy loose behavior.
        legacy_rows: list[dict[str, Any]] = []
        for ev in events:
            t, text = _extract_event(ev)
            txt = str(text or "").strip()
            if not txt:
                continue
            if len(re.findall(r"[a-zA-Z0-9']+", txt)) < 3:
                continue
            toks = _tokenize_anchor_text(txt)
            legacy_rows.append(
                {
                    "time": float(t),
                    "text": txt,
                    "tokens": toks,
                    "informative_tokens": [tok for tok in toks if len(tok) >= 3],
                    "score": 0.0,
                }
            )
        # Preserve unique times only.
        seen_legacy: set[str] = set()
        unique_legacy: list[dict[str, Any]] = []
        for row in sorted(legacy_rows, key=lambda r: float(r.get("time", 0.0))):
            key = f"{float(row.get('time', 0.0)):.3f}"
            if key in seen_legacy:
                continue
            seen_legacy.add(key)
            unique_legacy.append(row)
        return unique_legacy

    rows.sort(key=lambda r: float(r.get("time", 0.0)))

    # If there is a sparse early prelude followed by a long silence and dense lyrics,
    # ignore the prelude for anchor selection.
    if len(rows) >= 6:
        times = [float(r.get("time", 0.0)) for r in rows]
        trim_idx: Optional[int] = None
        for i in range(len(times) - 1):
            gap = float(times[i + 1] - times[i])
            if gap >= 35.0 and times[i] <= 70.0:
                early_count = i + 1
                late_count = len(times) - early_count
                if early_count <= 6 and late_count >= 3:
                    trim_idx = early_count
                break
        if trim_idx is not None:
            rows = rows[int(trim_idx) :]

    # Score each candidate row by token rarity/information so anchor selection
    # prefers distinctive lines over repeated chorus fragments.
    n_rows = len(rows)
    df: dict[str, int] = {}
    for row in rows:
        toks = set(str(tok) for tok in (row.get("informative_tokens") or row.get("tokens") or []))
        for tok in toks:
            df[tok] = int(df.get(tok, 0)) + 1

    for row in rows:
        info_toks = [str(tok) for tok in (row.get("informative_tokens") or row.get("tokens") or [])]
        uniq = sorted(set(info_toks))
        rarity = 0.0
        for tok in uniq:
            # Smooth IDF-style weight.
            rarity += math.log((1.0 + float(n_rows)) / (1.0 + float(df.get(tok, 1)))) + 1.0
        token_count = len(uniq)
        length_bonus = min(1.5, max(0.0, (float(len(info_toks)) - 2.0) * 0.2))
        row["score"] = float(rarity + (0.35 * float(token_count)) + length_bonus)

    # Keep unique times; if duplicates, keep the highest-score row.
    best_by_time: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = f"{float(row.get('time', 0.0)):.3f}"
        prev = best_by_time.get(key)
        if prev is None or float(row.get("score", 0.0)) > float(prev.get("score", 0.0)):
            best_by_time[key] = row

    return sorted(best_by_time.values(), key=lambda r: float(r.get("time", 0.0)))


def _pick_scored_anchor(
    candidates: list[dict[str, Any]],
    *,
    seen_keys: set[str],
    selected_times: list[float],
    prefer_center: Optional[float] = None,
    min_gap_s: float = 8.0,
) -> Optional[float]:
    pool: list[dict[str, Any]] = []
    for row in candidates:
        try:
            t = float(row.get("time", 0.0))
        except Exception:
            continue
        key = f"{t:.3f}"
        if key in seen_keys:
            continue
        if any(abs(t - float(prev)) < float(min_gap_s) for prev in selected_times):
            continue
        pool.append(row)
    if not pool:
        return None

    center = float(prefer_center) if isinstance(prefer_center, (int, float)) else None

    def _rank_key(row: dict[str, Any]) -> tuple:
        t = float(row.get("time", 0.0))
        score = float(row.get("score", 0.0))
        center_dist = abs(t - center) if center is not None else 0.0
        # Higher score first, then nearest center, then earlier timestamp.
        return (-score, center_dist, t)

    best = sorted(pool, key=_rank_key)[0]
    return float(best.get("time", 0.0))


def _split_rows_into_count_buckets(rows: list[dict[str, Any]], bucket_count: int = 3) -> list[list[dict[str, Any]]]:
    if bucket_count <= 0:
        return []
    n = len(rows)
    if n <= 0:
        return [[] for _ in range(bucket_count)]
    base = n // bucket_count
    rem = n % bucket_count
    sizes = [base + (1 if i < rem else 0) for i in range(bucket_count)]
    out: list[list[dict[str, Any]]] = []
    idx = 0
    for sz in sizes:
        out.append(rows[idx : idx + sz])
        idx += sz
    return out


def _expand_bucket_to_min_lines(
    *,
    bucket_rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    prefer_center: float,
    min_lines: int,
) -> list[dict[str, Any]]:
    target = int(max(1, min_lines))
    if len(bucket_rows) >= target:
        return list(bucket_rows)

    existing = {f"{float(r.get('time', 0.0)):.3f}" for r in bucket_rows}
    extras = sorted(
        [r for r in all_rows if f"{float(r.get('time', 0.0)):.3f}" not in existing],
        key=lambda r: abs(float(r.get("time", 0.0)) - float(prefer_center)),
    )
    out = list(bucket_rows)
    for row in extras:
        if len(out) >= target:
            break
        out.append(row)
    return out


def _pick_weighted_random_anchor_time(rows: list[dict[str, Any]]) -> Optional[float]:
    candidates: list[dict[str, Any]] = []
    weights: list[float] = []
    for row in rows:
        try:
            t = float(row.get("time", 0.0))
        except Exception:
            continue
        candidates.append(row)
        score = float(row.get("score", 0.0))
        weights.append(max(0.10, score))
    if not candidates:
        return None
    picked = random.choices(candidates, weights=weights, k=1)[0]
    return float(picked.get("time", 0.0))


def _canonical_anchor_line_key(text: str) -> str:
    raw = str(text or "").strip().lower()
    raw = re.sub(r"[^a-z0-9\u00C0-\u017F\s']", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _pick_bridge_anchor_time(rows: list[dict[str, Any]], *, first: float, last: float) -> Optional[float]:
    if len(rows) < 4:
        return None

    lo = float(first)
    hi = float(last)
    span = max(0.0, hi - lo)
    if span < 20.0:
        return None

    # Bridge commonly appears in the later-middle portion of the song.
    win_start = lo + (0.45 * span)
    win_end = lo + (0.90 * span)
    bridge_center = lo + (0.72 * span)

    line_counts: dict[str, int] = {}
    for row in rows:
        key = _canonical_anchor_line_key(str(row.get("text") or ""))
        if not key:
            continue
        line_counts[key] = int(line_counts.get(key, 0)) + 1

    candidates: list[tuple[float, float]] = []
    for row in rows:
        try:
            t = float(row.get("time", 0.0))
        except Exception:
            continue
        if t < win_start or t > win_end:
            continue
        key = _canonical_anchor_line_key(str(row.get("text") or ""))
        repeat_count = int(line_counts.get(key, 1))
        uniqueness_bonus = 1.6 if repeat_count <= 1 else -0.8 * float(repeat_count - 1)
        center_penalty = 0.02 * abs(t - bridge_center)
        row_score = float(row.get("score", 0.0))
        bridge_score = row_score + uniqueness_bonus - center_penalty
        candidates.append((bridge_score, t))

    if not candidates:
        return None

    # Prefer strongest bridge score; tie-break toward the bridge center.
    best = sorted(candidates, key=lambda p: (-float(p[0]), abs(float(p[1]) - bridge_center), float(p[1])))[0]
    return float(best[1])


def _offset_auto_meta_path(slug: str) -> Path:
    return TIMINGS_DIR / f"{slug}.offset.auto.meta.json"


def _clear_auto_offset_artifacts(slug: str) -> bool:
    removed_any = False
    for path in (TIMINGS_DIR / f"{slug}.offset.auto", _offset_auto_meta_path(slug)):
        try:
            if path.exists():
                path.unlink()
                removed_any = True
        except Exception:
            continue
    return removed_any


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _mad(values: list[float]) -> float:
    if not values:
        return 0.0
    med = float(statistics.median(values))
    return float(statistics.median([abs(float(v) - med) for v in values]))


def _weighted_median(values: list[float], weights: list[float]) -> float:
    if not values:
        raise RuntimeError("No values for weighted median")
    pairs = sorted(zip(values, weights), key=lambda p: float(p[0]))
    total = float(sum(max(0.0, float(w)) for _, w in pairs))
    if total <= 0.0:
        return float(statistics.median(values))
    target = total / 2.0
    running = 0.0
    for val, w in pairs:
        running += max(0.0, float(w))
        if running >= target:
            return float(val)
    return float(pairs[-1][0])


def _aggregate_sample_offsets(
    samples: list[dict[str, Any]],
    *,
    min_sample_confidence: float,
) -> Optional[dict[str, Any]]:
    ok_rows: list[dict[str, Any]] = []
    for row in samples:
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").lower() != "ok":
            continue
        off = row.get("offset_s")
        conf = row.get("confidence")
        if not isinstance(off, (int, float)) or not isinstance(conf, (int, float)):
            continue
        ok_rows.append(
            {
                "index": int(row.get("index") or (len(ok_rows) + 1)),
                "offset_s": float(off),
                "confidence": float(conf),
            }
        )

    if not ok_rows:
        return None

    hi_conf = [r for r in ok_rows if float(r["confidence"]) >= float(min_sample_confidence)]
    working = hi_conf if len(hi_conf) >= 2 else list(ok_rows)
    pre_trim_count = len(working)

    if len(working) >= 3:
        vals = [float(r["offset_s"]) for r in working]
        center = float(statistics.median(vals))
        mad = _mad(vals)
        band = max(0.60, 3.5 * mad) if mad > 0.0 else 1.00
        trimmed = [r for r in working if abs(float(r["offset_s"]) - center) <= band]
        if len(trimmed) >= 2:
            working = trimmed

    vals = [float(r["offset_s"]) for r in working]
    confs = [max(0.05, float(r["confidence"])) for r in working]
    agg_offset = _weighted_median(vals, confs)
    agg_conf = float(statistics.median([float(r["confidence"]) for r in working]))
    spread_s = (max(vals) - min(vals)) if vals else 0.0
    mad_s = _mad(vals)
    return {
        "offset_s": float(agg_offset),
        "confidence": float(agg_conf),
        "used_rows": working,
        "ok_rows": ok_rows,
        "hi_conf_rows": hi_conf,
        "pre_trim_count": int(pre_trim_count),
        "spread_s": float(spread_s),
        "mad_s": float(mad_s),
    }


def _aggregate_quality(agg: Optional[dict[str, Any]]) -> tuple[int, int, float]:
    if not agg:
        return (0, 0, 0.0)
    hi = len(list(agg.get("hi_conf_rows") or []))
    used = len(list(agg.get("used_rows") or []))
    conf = float(agg.get("confidence") or 0.0)
    return (int(hi), int(used), float(conf))


def _aggregate_early_anchor_consensus(
    sample_rows: list[dict[str, Any]],
    *,
    min_sample_confidence: float,
) -> Optional[dict[str, Any]]:
    if not _env_flag("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_ENABLED", True):
        return None

    max_anchor_time_s = float(os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MAX_ANCHOR_SECS", "90.0"))
    max_rows = int(max(2, int(os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MAX_ROWS", "3"))))

    deduped_rows: list[dict[str, Any]] = []
    seen_anchor_keys: set[str] = set()
    ok_rows = [
        row
        for row in sample_rows
        if isinstance(row, dict)
        and str(row.get("status") or "").lower() == "ok"
        and isinstance(row.get("anchor_time_s"), (int, float))
        and float(row.get("anchor_time_s")) <= float(max_anchor_time_s)
    ]
    ok_rows = sorted(ok_rows, key=lambda row: float(row.get("anchor_time_s") or 0.0))
    for row in ok_rows:
        anchor_key = str(row.get("anchor_key") or _anchor_key(row.get("anchor_time_s")))
        if anchor_key in seen_anchor_keys:
            continue
        seen_anchor_keys.add(anchor_key)
        deduped_rows.append(row)
        if len(deduped_rows) >= max_rows:
            break

    if len(deduped_rows) < 2:
        return None
    return _aggregate_sample_offsets(deduped_rows, min_sample_confidence=min_sample_confidence)


def _should_progressive_calibration_exit(
    *,
    requested_level: int,
    calibration_anchor_count: int,
    calibration_agg: Optional[dict[str, Any]],
    min_selected_samples: int,
    min_hi_conf_samples: int,
    min_confidence: float,
) -> bool:
    if requested_level <= 0:
        return False
    if calibration_anchor_count < 3:
        return False
    if not _env_flag("KARAOKE_AUTO_OFFSET_PROGRESSIVE_CALIBRATION_EXIT", True):
        return False
    if not calibration_agg:
        return False

    used_rows = list(calibration_agg.get("used_rows") or [])
    hi_conf_rows = list(calibration_agg.get("hi_conf_rows") or [])
    ok_rows = list(calibration_agg.get("ok_rows") or [])
    if len(used_rows) < int(min_selected_samples):
        return False
    if len(hi_conf_rows) < int(min_hi_conf_samples):
        return False

    progressive_min_confidence = float(
        os.environ.get(
            "KARAOKE_AUTO_OFFSET_PROGRESSIVE_MIN_CONFIDENCE",
            str(max(float(min_confidence), 0.78)),
        )
    )
    if float(calibration_agg.get("confidence") or 0.0) < progressive_min_confidence:
        return False

    progressive_max_spread = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_PROGRESSIVE_MAX_SPREAD_SECS", "0.45")
    )
    spread_s = float(calibration_agg.get("spread_s") or 0.0)
    if len(used_rows) >= 2 and spread_s > progressive_max_spread:
        return False

    progressive_min_ok = int(
        max(
            int(min_selected_samples),
            int(os.environ.get("KARAOKE_AUTO_OFFSET_PROGRESSIVE_MIN_OK_SAMPLES", str(min_selected_samples))),
        )
    )
    if len(ok_rows) < progressive_min_ok:
        return False

    return True


def _maybe_dampen_weak_single_calibration_offset(
    *,
    offset_s: float,
    confidence: float,
    used_rows: list[dict[str, Any]],
    sample_rows: list[dict[str, Any]],
    mode_resolution: str,
    large_lead_gap_detected: bool,
) -> tuple[float, Optional[dict[str, float]]]:
    if str(mode_resolution or "").strip().lower() != "calibration_only":
        return float(offset_s), None
    if bool(large_lead_gap_detected):
        return float(offset_s), None
    if len(used_rows) != 1 or len(sample_rows) <= 1:
        return float(offset_s), None

    weak_conf_max = float(os.environ.get("KARAOKE_AUTO_OFFSET_WEAK_SINGLE_SAMPLE_CONFIDENCE_MAX", "0.65"))
    damp_max_abs = float(os.environ.get("KARAOKE_AUTO_OFFSET_WEAK_SINGLE_CALIBRATION_MAX_ABS_SECS", "2.50"))
    if float(confidence) > weak_conf_max or abs(float(offset_s)) <= damp_max_abs:
        return float(offset_s), None

    damped = math.copysign(float(damp_max_abs), float(offset_s))
    return float(damped), {
        "raw_offset_s": float(offset_s),
        "weak_confidence_max": float(weak_conf_max),
        "damped_max_abs_s": float(damp_max_abs),
    }


def _anchor_key(anchor: Optional[float]) -> str:
    if isinstance(anchor, (int, float)):
        return f"{float(anchor):.3f}"
    return "None"


@contextmanager
def _temporary_env(overrides: dict[str, str]):
    prev: dict[str, Optional[str]] = {}
    for key, val in overrides.items():
        prev[key] = os.environ.get(key)
        os.environ[key] = str(val)
    try:
        yield
    finally:
        for key, old_val in prev.items():
            if old_val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_val


def _run_auto_offset_samples(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
    anchors: list[Optional[float]],
    max_sample_abs_offset: float,
    pass_label: str,
    index_start: int = 1,
) -> list[dict[str, Any]]:
    use_batch = bool(len(anchors) > 1 and _env_flag("KARAOKE_AUTO_OFFSET_BATCH_WHISPER", True))
    if use_batch:
        try:
            return _run_auto_offset_samples_batch(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
                anchors=anchors,
                max_sample_abs_offset=max_sample_abs_offset,
                pass_label=pass_label,
                index_start=index_start,
            )
        except Exception as exc:
            log("STEP3", f"Batch auto-offset pass failed; falling back to serial samples: {exc}", YELLOW)
    return _run_auto_offset_samples_serial(
        lrc_path=lrc_path,
        audio_path=audio_path,
        language=language,
        anchors=anchors,
        max_sample_abs_offset=max_sample_abs_offset,
        pass_label=pass_label,
        index_start=index_start,
    )


def _run_auto_offset_samples_batch(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
    anchors: list[Optional[float]],
    max_sample_abs_offset: float,
    pass_label: str,
    index_start: int = 1,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    results = _estimate_auto_offset_batch(
        lrc_path=lrc_path,
        audio_path=audio_path,
        language=language,
        anchor_times_s=anchors,
    )
    for anchor_idx, (anchor, result) in enumerate(zip(anchors, results, strict=False), start=1):
        sample_index = int(index_start + anchor_idx - 1)
        error_text = str(result.get("error") or "").strip()
        if error_text:
            rows.append(
                {
                    "index": sample_index,
                    "status": "error",
                    "anchor_time_s": (float(anchor) if isinstance(anchor, (int, float)) else None),
                    "anchor_key": _anchor_key(anchor),
                    "error": error_text,
                    "pass": pass_label,
                }
            )
            pass_prefix = f"{pass_label} " if pass_label else ""
            log(
                "STEP3",
                f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} {pass_prefix}skipped: {error_text}",
                YELLOW,
            )
            continue

        sample_offset = float(result.get("offset_s") or 0.0)
        sample_conf = float(result.get("confidence") or 0.0)
        if abs(sample_offset) > float(max_sample_abs_offset):
            error_text = f"Sample offset out of range ({sample_offset:+.3f}s > ±{max_sample_abs_offset:.1f}s)"
            rows.append(
                {
                    "index": sample_index,
                    "status": "error",
                    "anchor_time_s": (float(anchor) if isinstance(anchor, (int, float)) else None),
                    "anchor_key": _anchor_key(anchor),
                    "error": error_text,
                    "pass": pass_label,
                }
            )
            pass_prefix = f"{pass_label} " if pass_label else ""
            log(
                "STEP3",
                f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} {pass_prefix}skipped: {error_text}",
                YELLOW,
            )
            continue

        rows.append(
            {
                "index": sample_index,
                "status": "ok",
                "anchor_time_s": (float(anchor) if isinstance(anchor, (int, float)) else None),
                "anchor_key": _anchor_key(anchor),
                "offset_s": float(sample_offset),
                "confidence": float(sample_conf),
                "pass": pass_label,
            }
        )
        pass_prefix = f"{pass_label} " if pass_label else ""
        if isinstance(anchor, (int, float)):
            log(
                "STEP3",
                (
                    f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} "
                    f"{pass_prefix}anchor={float(anchor):.2f}s "
                    f"offset={sample_offset:+.3f}s confidence={sample_conf:.2f}"
                ),
                CYAN,
            )
        else:
            log(
                "STEP3",
                (
                    f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} "
                    f"{pass_prefix}offset={sample_offset:+.3f}s confidence={sample_conf:.2f}"
                ),
                CYAN,
            )
    return rows


def _run_auto_offset_samples_serial(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
    anchors: list[Optional[float]],
    max_sample_abs_offset: float,
    pass_label: str,
    index_start: int = 1,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for anchor_idx, anchor in enumerate(anchors, start=1):
        sample_index = int(index_start + anchor_idx - 1)
        try:
            sample_offset, sample_conf = _estimate_auto_offset(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
                anchor_time_s=anchor,
            )
            if abs(float(sample_offset)) > float(max_sample_abs_offset):
                raise RuntimeError(
                    f"Sample offset out of range ({sample_offset:+.3f}s > ±{max_sample_abs_offset:.1f}s)"
                )
            rows.append(
                {
                    "index": sample_index,
                    "status": "ok",
                    "anchor_time_s": (float(anchor) if isinstance(anchor, (int, float)) else None),
                    "anchor_key": _anchor_key(anchor),
                    "offset_s": float(sample_offset),
                    "confidence": float(sample_conf),
                    "pass": pass_label,
                }
            )
            pass_prefix = f"{pass_label} " if pass_label else ""
            if isinstance(anchor, (int, float)):
                log(
                    "STEP3",
                    (
                        f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} "
                        f"{pass_prefix}anchor={float(anchor):.2f}s "
                        f"offset={sample_offset:+.3f}s confidence={sample_conf:.2f}"
                    ),
                    CYAN,
                )
            else:
                log(
                    "STEP3",
                    (
                        f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} "
                        f"{pass_prefix}offset={sample_offset:+.3f}s confidence={sample_conf:.2f}"
                    ),
                    CYAN,
                )
        except Exception as exc:
            err_text = str(exc).strip() or exc.__class__.__name__
            rows.append(
                {
                    "index": sample_index,
                    "status": "error",
                    "anchor_time_s": (float(anchor) if isinstance(anchor, (int, float)) else None),
                    "anchor_key": _anchor_key(anchor),
                    "error": err_text,
                    "pass": pass_label,
                }
            )
            pass_prefix = f"{pass_label} " if pass_label else ""
            log(
                "STEP3",
                f"Auto-offset sample {sample_index}/{index_start + len(anchors) - 1} {pass_prefix}skipped: {err_text}",
                YELLOW,
            )
    return rows


def _resolve_audio_for_offset(paths: Optional[Any], slug: str) -> Optional[Path]:
    candidates: list[Path] = []

    root = TIMINGS_DIR.parent
    step1_meta = root / "meta" / f"{slug}.step1.json"
    if step1_meta.exists():
        try:
            payload = json.loads(step1_meta.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                audio_raw = str(payload.get("audio_path") or "").strip()
                if audio_raw:
                    candidates.append(Path(audio_raw))
        except Exception:
            pass

    if paths is not None:
        mixes = getattr(paths, "mixes", None)
        mp3s = getattr(paths, "mp3s", None)
        if mixes is not None:
            mixes_path = Path(mixes)
            candidates.extend(
                [
                    mixes_path / f"{slug}.wav",
                    mixes_path / f"{slug}.mp3",
                ]
            )
        if mp3s is not None:
            mp3s_path = Path(mp3s)
            candidates.extend(
                [
                    mp3s_path / f"{slug}.mp3",
                    mp3s_path / f"{slug}.mp4",
                    mp3s_path / f"{slug}.m4a",
                ]
            )

    candidates.extend(
        [
            root / "mixes" / f"{slug}.wav",
            root / "mixes" / f"{slug}.mp3",
            root / "mp3s" / f"{slug}.mp3",
            root / "mp3s" / f"{slug}.mp4",
            root / "mp3s" / f"{slug}.m4a",
        ]
    )

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
            return candidate
    return None


def _probe_audio_duration_seconds(audio_path: Path) -> Optional[float]:
    ffmpeg_bin = resolve_ffmpeg_bin()
    ffprobe_bin = ffmpeg_bin.with_name("ffprobe")
    cmd: list[str]
    if ffprobe_bin.exists():
        cmd = [str(ffprobe_bin)]
    else:
        cmd = ["ffprobe"]
    cmd.extend(
        [
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(audio_path),
        ]
    )
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        dur = float(out)
        if math.isfinite(dur) and dur > 0.0:
            return float(dur)
    except Exception:
        return None
    return None


def _collect_first_last_lyric_times(lrc_path: Path) -> tuple[Optional[float], Optional[float]]:
    from scripts.lrc_utils import parse_lrc

    events, _meta = parse_lrc(str(lrc_path))
    times: list[float] = []
    for ev in events:
        t, text = _extract_event(ev)
        txt = str(text or "").strip()
        if not txt:
            continue
        # Keep very short lines too; we only need a robust first spoken anchor.
        toks = _tokenize_anchor_text(txt)
        if not toks:
            continue
        times.append(float(t))

    if not times:
        return (None, None)
    return (float(times[0]), float(times[-1]))


def _should_run_smart_micro_offset(
    *,
    first_lyric_s: float,
    last_lyric_s: float,
    audio_duration_s: Optional[float],
) -> tuple[bool, str]:
    first_min_s = float(os.environ.get("MIXTERIOSO_SMART_MICRO_FIRST_MIN_S", "0.35"))
    first_max_s = float(os.environ.get("MIXTERIOSO_SMART_MICRO_FIRST_MAX_S", "120.0"))
    if first_lyric_s < first_min_s:
        return (False, f"first_lyric_too_early({first_lyric_s:.2f}s)")
    if first_lyric_s > first_max_s:
        return (False, f"first_lyric_too_late({first_lyric_s:.2f}s)")

    if audio_duration_s is None or audio_duration_s <= 0.0:
        return (True, "audio_duration_unavailable")

    audio_duration = float(audio_duration_s)
    tail_gap = float(audio_duration - last_lyric_s)
    coverage = max(0.0, float(last_lyric_s - first_lyric_s)) / max(audio_duration, 1e-6)

    # Skip extra work only when alignment shape already looks very healthy.
    if first_lyric_s <= 0.80 and abs(tail_gap) <= 1.50 and coverage >= 0.80:
        return (False, "alignment_shape_looks_good")

    if abs(tail_gap) > 1.00:
        return (True, f"tail_gap={tail_gap:+.2f}s")
    if first_lyric_s >= 0.90:
        return (True, f"first_lyric_delay={first_lyric_s:.2f}s")
    if coverage < 0.75:
        return (True, f"lyric_coverage={coverage:.2f}")

    return (False, "no_alignment_risk_detected")


def _maybe_write_smart_micro_offset(
    paths: Optional[Any],
    slug: str,
    language: Optional[str],
    *,
    force_refresh: bool = False,
    cli_offset_hint: float = 0.0,
) -> None:
    if not _env_flag("MIXTERIOSO_SMART_MICRO_OFFSET_ENABLED", True):
        return

    manual_skip_abs = float(os.environ.get("MIXTERIOSO_SMART_MICRO_MANUAL_SKIP_ABS_S", "0.05"))
    if abs(float(cli_offset_hint or 0.0)) >= manual_skip_abs:
        log(
            "STEP3",
            f"Smart micro-offset skipped: manual --off detected ({float(cli_offset_hint):+.3f}s)",
            CYAN,
        )
        return

    lrc_path = TIMINGS_DIR / f"{slug}.lrc"
    audio_path = _resolve_audio_for_offset(paths, slug)
    if audio_path is None or (not lrc_path.exists()):
        return

    auto_offset_path = TIMINGS_DIR / f"{slug}.offset.auto"
    auto_meta_path = _offset_auto_meta_path(slug)
    reuse_existing = _env_flag("KARAOKE_AUTO_OFFSET_REUSE_EXISTING", True)
    if auto_offset_path.exists() and reuse_existing and (not force_refresh):
        try:
            cached = float((auto_offset_path.read_text(encoding="utf-8").strip() or "0.0").splitlines()[0].strip())
            log("STEP3", f"Reusing cached auto offset {cached:+.3f}s -> {auto_offset_path}", CYAN)
            return
        except Exception:
            pass

    first_lyric_s, last_lyric_s = _collect_first_last_lyric_times(lrc_path)
    if not isinstance(first_lyric_s, (int, float)) or not isinstance(last_lyric_s, (int, float)):
        return

    audio_duration_s = _probe_audio_duration_seconds(audio_path)
    should_run, reason = _should_run_smart_micro_offset(
        first_lyric_s=float(first_lyric_s),
        last_lyric_s=float(last_lyric_s),
        audio_duration_s=audio_duration_s,
    )

    meta_payload: dict[str, Any] = {
        "slug": slug,
        "mode": "smart_micro_first_line",
        "status": "skipped",
        "reason": reason,
        "first_lyric_s": float(first_lyric_s),
        "last_lyric_s": float(last_lyric_s),
        "audio_duration_s": (float(audio_duration_s) if isinstance(audio_duration_s, (int, float)) else None),
        "requested_accuracy_level": 0,
        "requested_calibration_level": 0,
    }

    if not should_run:
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log("STEP3", f"Smart micro-offset skipped: {reason}", CYAN)
        return

    clip_secs = float(os.environ.get("MIXTERIOSO_SMART_MICRO_CLIP_SECS", "6.0"))
    clip_secs = max(4.0, min(12.0, clip_secs))
    max_abs_sample = float(os.environ.get("MIXTERIOSO_SMART_MICRO_MAX_SAMPLE_ABS_S", "6.0"))
    max_abs_apply = float(os.environ.get("MIXTERIOSO_SMART_MICRO_MAX_APPLY_ABS_S", "4.0"))
    min_conf = float(os.environ.get("MIXTERIOSO_SMART_MICRO_MIN_CONFIDENCE", "0.45"))
    min_apply_abs = float(os.environ.get("MIXTERIOSO_SMART_MICRO_MIN_APPLY_ABS_S", "0.08"))

    overrides = {
        "KARAOKE_AUTO_OFFSET_CLIP_SECS": f"{clip_secs:.3f}",
        "KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE": os.environ.get("MIXTERIOSO_SMART_MICRO_MIN_MATCH_SCORE", "0.34"),
        "KARAOKE_AUTO_OFFSET_SEGMENT_ANCHOR_FRAC": os.environ.get("MIXTERIOSO_SMART_MICRO_SEGMENT_ANCHOR_FRAC", "0.08"),
        "KARAOKE_AUTO_OFFSET_ANCHOR_BIAS_SECS": os.environ.get("MIXTERIOSO_SMART_MICRO_ANCHOR_BIAS_SECS", "0.0"),
    }

    try:
        with _temporary_env(overrides):
            offset_s, confidence = _estimate_auto_offset(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
                anchor_time_s=float(first_lyric_s),
            )
    except Exception as exc:
        err = str(exc).strip() or exc.__class__.__name__
        meta_payload.update({"status": "error", "error": err, "manual_offset_recommended": True})
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log("STEP3", f"Smart micro-offset skipped: {err}", YELLOW)
        return

    sample_rows = [
        {
            "index": 1,
            "status": "ok",
            "anchor_time_s": float(first_lyric_s),
            "anchor_key": _anchor_key(float(first_lyric_s)),
            "offset_s": float(offset_s),
            "confidence": float(confidence),
            "pass": "smart_micro",
        }
    ]
    meta_payload.update(
        {
            "sample_count": 1,
            "anchor_count": 1,
            "samples": sample_rows,
            "successful_samples": 1,
            "high_confidence_samples": (1 if float(confidence) >= min_conf else 0),
            "selected_samples": 1,
            "selected_sample_indexes": [1],
            "aggregate_offset_s": float(offset_s),
            "aggregate_confidence": float(confidence),
            "sub_offsets": sample_rows,
        }
    )

    if abs(float(offset_s)) > max_abs_sample:
        meta_payload.update(
            {
                "status": "offset_out_of_bounds",
                "applied_offset_s": 0.0,
                "manual_offset_recommended": True,
            }
        )
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log(
            "STEP3",
            f"Smart micro-offset rejected: sample {offset_s:+.3f}s exceeds ±{max_abs_sample:.1f}s",
            YELLOW,
        )
        return

    if confidence < min_conf:
        meta_payload.update(
            {
                "status": "low_confidence",
                "applied_offset_s": 0.0,
                "manual_offset_recommended": True,
            }
        )
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log("STEP3", f"Smart micro-offset rejected: confidence {confidence:.2f} < {min_conf:.2f}", YELLOW)
        return

    if abs(float(offset_s)) < min_apply_abs:
        meta_payload.update({"status": "near_zero", "applied_offset_s": 0.0})
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log("STEP3", f"Smart micro-offset skipped: tiny delta {offset_s:+.3f}s", CYAN)
        return

    applied = max(-max_abs_apply, min(max_abs_apply, float(offset_s)))
    auto_offset_path.write_text(f"{applied:.3f}\n", encoding="utf-8")
    meta_payload.update({"status": "applied", "applied_offset_s": float(applied), "low_confidence": False})
    try:
        _write_json_atomic(auto_meta_path, meta_payload)
    except Exception:
        pass
    log(
        "STEP3",
        (
            f"✓ Applied smart micro offset {applied:+.3f}s "
            f"(sample={offset_s:+.3f}s, confidence={confidence:.2f}, reason={reason})"
        ),
        GREEN,
    )


def _estimate_auto_offset(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
    anchor_time_s: Optional[float] = None,
) -> Tuple[float, float]:
    from scripts import lrc_offset_whisper as whisper_offset

    whisper_bin = whisper_offset._find_whispercpp_bin(os.environ.get("MIXTERIOSO_WHISPER_BIN"))
    model_path = whisper_offset._find_model(os.environ.get("MIXTERIOSO_WHISPER_MODEL"))
    if whisper_bin is None or model_path is None:
        raise RuntimeError(
            "whisper.cpp binary/model not found; "
            "set MIXTERIOSO_WHISPER_BIN and MIXTERIOSO_WHISPER_MODEL if not using repo defaults"
        )

    clip_secs = float(os.environ.get("KARAOKE_AUTO_OFFSET_CLIP_SECS", "35.0"))
    max_abs_offset = float(os.environ.get("KARAOKE_MAX_AUTO_OFFSET_SECS", "90.0"))
    whisper_extra_args_raw = (os.environ.get("MIXTERIOSO_WHISPER_ARGS") or "").strip()
    whisper_extra_args: list[str] = []
    if whisper_extra_args_raw:
        try:
            whisper_extra_args = shlex.split(whisper_extra_args_raw)
        except ValueError as exc:
            log("STEP3", f"Ignoring MIXTERIOSO_WHISPER_ARGS (parse error): {exc}", YELLOW)

    resolved_language = str(language or "auto").strip().lower()
    if resolved_language in {"", "auto"}:
        resolved_language = _guess_auto_offset_language(lrc_path)

    result = whisper_offset.estimate_offset(
        lrc_path=lrc_path,
        audio_path=audio_path,
        language=resolved_language,
        ffmpeg_bin=str(resolve_ffmpeg_bin()),
        whisper_bin=whisper_bin,
        model_path=model_path,
        clip_dur_s=clip_secs,
        max_abs_offset=max_abs_offset,
        whisper_extra_args=whisper_extra_args,
        anchor_time_s=anchor_time_s,
        return_confidence=True,
    )

    # result is now (offset, confidence)
    return result


def _estimate_auto_offset_batch(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
    anchor_times_s: list[Optional[float]],
) -> list[dict[str, Any]]:
    from scripts import lrc_offset_whisper as whisper_offset

    whisper_bin = whisper_offset._find_whispercpp_bin(os.environ.get("MIXTERIOSO_WHISPER_BIN"))
    model_path = whisper_offset._find_model(os.environ.get("MIXTERIOSO_WHISPER_MODEL"))
    if whisper_bin is None or model_path is None:
        raise RuntimeError(
            "whisper.cpp binary/model not found; "
            "set MIXTERIOSO_WHISPER_BIN and MIXTERIOSO_WHISPER_MODEL if not using repo defaults"
        )

    clip_secs = float(os.environ.get("KARAOKE_AUTO_OFFSET_CLIP_SECS", "35.0"))
    max_abs_offset = float(os.environ.get("KARAOKE_MAX_AUTO_OFFSET_SECS", "90.0"))
    whisper_extra_args_raw = (os.environ.get("MIXTERIOSO_WHISPER_ARGS") or "").strip()
    whisper_extra_args: list[str] = []
    if whisper_extra_args_raw:
        try:
            whisper_extra_args = shlex.split(whisper_extra_args_raw)
        except ValueError as exc:
            log("STEP3", f"Ignoring MIXTERIOSO_WHISPER_ARGS (parse error): {exc}", YELLOW)

    resolved_language = str(language or "auto").strip().lower()
    if resolved_language in {"", "auto"}:
        resolved_language = _guess_auto_offset_language(lrc_path)

    return whisper_offset.estimate_offsets_batch(
        lrc_path=lrc_path,
        audio_path=audio_path,
        language=resolved_language,
        ffmpeg_bin=str(resolve_ffmpeg_bin()),
        whisper_bin=whisper_bin,
        model_path=model_path,
        clip_dur_s=clip_secs,
        max_abs_offset=max_abs_offset,
        anchor_times_s=anchor_times_s,
        whisper_extra_args=whisper_extra_args,
        return_confidence=True,
    )


def _estimate_intro_gap_offset(
    *,
    lrc_path: Path,
    audio_path: Path,
    language: Optional[str],
) -> Tuple[float, float]:
    from scripts import lrc_offset_whisper as whisper_offset

    whisper_bin = whisper_offset._find_whispercpp_bin(os.environ.get("MIXTERIOSO_WHISPER_BIN"))
    model_path = whisper_offset._find_model(os.environ.get("MIXTERIOSO_WHISPER_MODEL"))
    if whisper_bin is None or model_path is None:
        raise RuntimeError(
            "whisper.cpp binary/model not found; "
            "set MIXTERIOSO_WHISPER_BIN and MIXTERIOSO_WHISPER_MODEL if not using repo defaults"
        )

    max_abs_offset = float(os.environ.get("KARAOKE_MAX_AUTO_OFFSET_SECS", "90.0"))
    whisper_extra_args_raw = (os.environ.get("MIXTERIOSO_WHISPER_ARGS") or "").strip()
    whisper_extra_args: list[str] = []
    if whisper_extra_args_raw:
        try:
            whisper_extra_args = shlex.split(whisper_extra_args_raw)
        except ValueError as exc:
            log("STEP3", f"Ignoring MIXTERIOSO_WHISPER_ARGS (parse error): {exc}", YELLOW)

    resolved_language = str(language or "auto").strip().lower()
    if resolved_language in {"", "auto"}:
        resolved_language = _guess_auto_offset_language(lrc_path)

    return whisper_offset.estimate_intro_gap_offset(
        lrc_path=lrc_path,
        audio_path=audio_path,
        language=resolved_language,
        ffmpeg_bin=str(resolve_ffmpeg_bin()),
        whisper_bin=whisper_bin,
        model_path=model_path,
        max_abs_offset=max_abs_offset,
        whisper_extra_args=whisper_extra_args,
        return_confidence=True,
    )


def _choose_auto_offset_anchors(
    lrc_path: Path,
    slug: str,
    accuracy_level: int,
    *,
    avoid_anchor_keys: Optional[set[str]] = None,
) -> list[Optional[float]]:
    level = int(max(1, min(3, int(accuracy_level or 1))))

    rows = _collect_anchor_rows(lrc_path)
    times = [float(r.get("time", 0.0)) for r in rows]

    if not times:
        return [None]
    if (max(times) - min(times)) < 1.0:
        return [float(sorted(times)[len(times) // 2])]
    # Use count-based buckets so each region has enough lyric lines.
    # User target: minimum 4 lines, ideally ~6-8 when possible.
    first, middle, last = _split_rows_into_count_buckets(rows, bucket_count=3)
    bucket_min_lines = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_BUCKET_MIN_LINES", "4"))))

    if level == 1:
        active_buckets: list[list[dict[str, Any]]] = [middle if middle else rows]
    elif level == 2:
        active_buckets = [middle if middle else rows, last if last else rows]
    else:
        active_buckets = [first if first else rows, middle if middle else rows, last if last else rows]

    selected: list[float] = []
    seen_keys: set[str] = set(avoid_anchor_keys or set())
    for bucket_rows in active_buckets:
        bucket_times = [float(r.get("time", 0.0)) for r in bucket_rows] or times
        center = float(statistics.median(bucket_times))
        candidate_rows = _expand_bucket_to_min_lines(
            bucket_rows=bucket_rows,
            all_rows=rows,
            prefer_center=center,
            min_lines=bucket_min_lines,
        )
        choice = _pick_scored_anchor(
            candidate_rows if candidate_rows else rows,
            seen_keys=seen_keys,
            selected_times=selected,
            prefer_center=center,
        )
        if choice is None:
            choice = _pick_scored_anchor(
                rows,
                seen_keys=seen_keys,
                selected_times=selected,
                prefer_center=center,
                min_gap_s=0.0,
            )
        if choice is None:
            continue
        key = f"{choice:.3f}"
        seen_keys.add(key)
        selected.append(choice)

    if len(selected) < len(active_buckets):
        remaining = sorted(
            [r for r in rows if f"{float(r.get('time', 0.0)):.3f}" not in seen_keys],
            key=lambda r: (-float(r.get("score", 0.0)), float(r.get("time", 0.0))),
        )
        for row in remaining:
            if len(selected) >= len(active_buckets):
                break
            extra = float(row.get("time", 0.0))
            key = f"{extra:.3f}"
            seen_keys.add(key)
            selected.append(extra)

    if selected:
        return selected
    best_global = sorted(rows, key=lambda r: (-float(r.get("score", 0.0)), float(r.get("time", 0.0))))
    if best_global:
        return [float(best_global[0].get("time", 0.0))]
    return [float(sorted(times)[len(times) // 2])]


def _choose_calibration_anchors(lrc_path: Path, calibration_level: int) -> list[Optional[float]]:
    level = int(max(1, min(3, int(calibration_level or 1))))
    rows = _collect_anchor_rows(lrc_path)
    times = [float(r.get("time", 0.0)) for r in rows]

    if not times:
        return [None]

    unique_times = sorted(list(dict.fromkeys(float(t) for t in times)))
    first = float(unique_times[0])
    last = float(unique_times[-1])

    if level == 1:
        if len(unique_times) >= 2:
            return [first, last]
        return [first]

    _first_bucket, middle_bucket, _last_bucket = _split_rows_into_count_buckets(rows, bucket_count=3)
    center = (first + last) / 2.0
    bucket_min_lines = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_BUCKET_MIN_LINES", "4"))))
    middle_rows = _expand_bucket_to_min_lines(
        bucket_rows=list(middle_bucket),
        all_rows=rows,
        prefer_center=center,
        min_lines=bucket_min_lines,
    )
    if middle_rows:
        if level >= 3:
            middle = _pick_weighted_random_anchor_time(middle_rows)
            if middle is None:
                middle = float(center)
        else:
            middle = float(
                sorted(
                    middle_rows,
                    key=lambda r: (
                        -float(r.get("score", 0.0)),
                        abs(float(r.get("time", 0.0)) - center),
                        float(r.get("time", 0.0)),
                    ),
                )[0].get("time", center)
            )
        return [first, middle, last]

    if len(unique_times) >= 2:
        return [first, last]
    return [first]


def _non_blank_lrc_timing_stats(lrc_path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {
        "line_count": 0,
        "first_time_s": 0.0,
        "second_time_s": 0.0,
        "first_gap_s": 0.0,
        "last_time_s": 0.0,
        "lyrics_span_s": 0.0,
        "lead_gap_ratio": 0.0,
        "large_lead_gap": False,
    }
    try:
        from scripts.lrc_utils import parse_lrc

        events, _meta = parse_lrc(lrc_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return out

    points: list[float] = []
    for ev in events:
        t, txt = _extract_event(ev)
        if str(txt or "").strip():
            points.append(float(t))
    if not points:
        return out

    first_t = max(0.0, min(points))
    last_t = max(0.0, max(points))
    second_t = float(sorted(points)[1]) if len(points) >= 2 else float(first_t)
    span = max(0.0, last_t - first_t)
    first_gap_s = max(0.0, second_t - first_t)
    lead_ratio = (first_t / max(last_t, 1.0))

    lead_gap_min_secs = float(os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_LEAD_GAP_MIN_SECS", "30.0"))
    lead_gap_min_ratio = float(os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_LEAD_GAP_MIN_RATIO", "0.25"))
    min_lines = int(max(4, int(os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_LEAD_GAP_MIN_LINES", "8"))))
    min_span = float(os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_LEAD_GAP_MIN_LYRICS_SPAN_SECS", "20.0"))
    large_lead_gap = (
        (len(points) >= min_lines)
        and (first_t >= lead_gap_min_secs)
        and (lead_ratio >= lead_gap_min_ratio)
        and (span >= min_span)
    )

    out.update(
        {
            "line_count": int(len(points)),
            "first_time_s": float(first_t),
            "second_time_s": float(second_t),
            "first_gap_s": float(first_gap_s),
            "last_time_s": float(last_t),
            "lyrics_span_s": float(span),
            "lead_gap_ratio": float(lead_ratio),
            "large_lead_gap": bool(large_lead_gap),
        }
    )
    return out


def _maybe_write_auto_offset(
    paths: Optional[Any],
    slug: str,
    language: Optional[str],
    *,
    default_enabled: bool = False,
    force_refresh: bool = False,
    accuracy_level: int = 1,
    calibration_level: int = 0,
) -> None:
    if not _env_flag("KARAOKE_AUTO_OFFSET_ENABLED", default_enabled):
        log("STEP3", "Auto offset disabled (KARAOKE_AUTO_OFFSET_ENABLED)", YELLOW)
        return

    lrc_path = TIMINGS_DIR / f"{slug}.lrc"
    audio_path = _resolve_audio_for_offset(paths, slug)
    if audio_path is None:
        log("STEP3", f"No audio found for auto-offset ({slug}); skipping", YELLOW)
        return

    auto_offset_path = TIMINGS_DIR / f"{slug}.offset.auto"
    auto_meta_path = _offset_auto_meta_path(slug)
    lrc_stats = _non_blank_lrc_timing_stats(lrc_path)
    min_confidence = float(os.environ.get("KARAOKE_AUTO_OFFSET_MIN_CONFIDENCE", "0.45"))
    min_sample_confidence = float(os.environ.get("KARAOKE_AUTO_OFFSET_MIN_SAMPLE_CONFIDENCE", "0.45"))
    max_sample_abs_offset = float(os.environ.get("KARAOKE_AUTO_OFFSET_MAX_SAMPLE_ABS_SECS", "90.0"))
    max_apply_abs_offset = float(os.environ.get("KARAOKE_AUTO_OFFSET_MAX_APPLY_ABS_SECS", "90.0"))
    mode_disagreement_secs = float(os.environ.get("KARAOKE_AUTO_OFFSET_MODE_DISAGREE_SECS", "2.0"))
    reuse_existing = _env_flag("KARAOKE_AUTO_OFFSET_REUSE_EXISTING", True)
    retry_on_weak = _env_flag("KARAOKE_AUTO_OFFSET_RETRY_ON_WEAK", True)
    large_gap_fallback_enabled = _env_flag("KARAOKE_AUTO_OFFSET_LARGE_GAP_FALLBACK", True)
    large_gap_reuse_min_abs = float(os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_GAP_REUSE_MIN_ABS_SECS", "8.0"))
    large_gap_max_apply_abs = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_LARGE_GAP_MAX_APPLY_ABS_SECS", str(max_apply_abs_offset))
    )
    intro_gap_fallback_enabled = _env_flag("KARAOKE_AUTO_OFFSET_INTRO_GAP_FALLBACK", True)
    intro_gap_min_first_lyric = float(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_GAP_MIN_FIRST_LYRIC_SECS", "25.0"))
    intro_gap_min_confidence = float(os.environ.get("KARAOKE_AUTO_OFFSET_INTRO_GAP_MIN_CONFIDENCE", "0.40"))
    positive_intro_fast_path_enabled = _env_flag("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_PATH", True)
    positive_intro_fast_max_first_lyric = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_MAX_FIRST_LYRIC_SECS", "1.0")
    )
    positive_intro_fast_min_first_gap = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_MIN_FIRST_GAP_SECS", "8.0")
    )
    positive_intro_fast_min_offset = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_MIN_OFFSET_SECS", "10.0")
    )
    positive_intro_fast_min_confidence = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_MIN_CONFIDENCE", "0.75")
    )
    positive_intro_fast_scan_clip_secs = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_POSITIVE_INTRO_FAST_SCAN_CLIP_SECS", "45.0")
    )

    if auto_offset_path.exists() and reuse_existing and (not force_refresh):
        try:
            cached = float((auto_offset_path.read_text(encoding="utf-8").strip() or "0.0").splitlines()[0].strip())
            if bool(lrc_stats.get("large_lead_gap")) and abs(float(cached)) < max(0.0, float(large_gap_reuse_min_abs)):
                log(
                    "STEP3",
                    (
                        f"Ignoring cached auto offset {cached:+.3f}s for {slug}: "
                        "large lyric lead-gap detected"
                    ),
                    YELLOW,
                )
            else:
                log("STEP3", f"Reusing cached auto offset {cached:+.3f}s -> {auto_offset_path}", CYAN)
                return
        except Exception:
            pass

    def _maybe_apply_intro_gap_fallback(meta_payload: dict[str, Any], *, reason: str) -> bool:
        if not intro_gap_fallback_enabled:
            return False
        first_t = float(lrc_stats.get("first_time_s") or 0.0)
        if first_t < float(intro_gap_min_first_lyric):
            return False
        try:
            intro_offset, intro_confidence = _estimate_intro_gap_offset(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
            )
        except Exception as exc:
            log("STEP3", f"Intro-gap fallback skipped: {exc}", CYAN)
            return False

        intro_offset = float(intro_offset)
        intro_confidence = float(intro_confidence)
        if abs(intro_offset) > float(max_apply_abs_offset):
            log(
                "STEP3",
                (
                    "Intro-gap fallback skipped: inferred shift out of range "
                    f"({intro_offset:+.3f}s > ±{float(max_apply_abs_offset):.1f}s)"
                ),
                CYAN,
            )
            return False
        if intro_confidence < float(intro_gap_min_confidence):
            log(
                "STEP3",
                (
                    "Intro-gap fallback skipped: confidence too low "
                    f"({intro_confidence:.2f} < {float(intro_gap_min_confidence):.2f})"
                ),
                CYAN,
            )
            return False

        auto_offset_path.write_text(f"{intro_offset:.3f}\n", encoding="utf-8")
        meta_payload.update(
            {
                "status": "applied_positive_intro_gap_fallback",
                "aggregate_offset_s": float(intro_offset),
                "aggregate_confidence": max(
                    float(meta_payload.get("aggregate_confidence") or 0.0),
                    float(intro_confidence),
                ),
                "applied_offset_s": float(intro_offset),
                "low_confidence": False,
                "manual_offset_recommended": False,
                "fallback_reason": str(reason),
                "positive_intro_gap_detected": True,
                "intro_gap_confidence": float(intro_confidence),
            }
        )
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log(
            "STEP3",
            (
                f"Applied positive intro-gap fallback offset {intro_offset:+.3f}s "
                f"(reason={reason}, first_lyric={first_t:.3f}s, confidence={intro_confidence:.2f})"
            ),
            YELLOW,
        )
        return True

    def _maybe_apply_positive_intro_fast_path(
        *,
        mode_label: str,
        anchor_count: int,
    ) -> bool:
        if not positive_intro_fast_path_enabled:
            return False

        first_t = float(lrc_stats.get("first_time_s") or 0.0)
        first_gap_s = float(lrc_stats.get("first_gap_s") or 0.0)
        if first_t > float(positive_intro_fast_max_first_lyric):
            return False
        if first_gap_s < float(positive_intro_fast_min_first_gap):
            return False

        intro_env = {
            "KARAOKE_AUTO_OFFSET_INTRO_SCAN_CLIP_SECS": str(positive_intro_fast_scan_clip_secs),
        }
        try:
            with _temporary_env(intro_env):
                intro_offset, intro_confidence = _estimate_intro_gap_offset(
                    lrc_path=lrc_path,
                    audio_path=audio_path,
                    language=language,
                )
        except Exception as exc:
            log("STEP3", f"Positive intro fast-path skipped: {exc}", CYAN)
            return False

        intro_offset = float(intro_offset)
        intro_confidence = float(intro_confidence)
        if intro_offset < float(positive_intro_fast_min_offset):
            log(
                "STEP3",
                (
                    "Positive intro fast-path skipped: inferred shift too small "
                    f"({intro_offset:+.3f}s < +{float(positive_intro_fast_min_offset):.1f}s)"
                ),
                CYAN,
            )
            return False
        if abs(intro_offset) > float(max_apply_abs_offset):
            log(
                "STEP3",
                (
                    "Positive intro fast-path skipped: inferred shift out of range "
                    f"({intro_offset:+.3f}s > ±{float(max_apply_abs_offset):.1f}s)"
                ),
                CYAN,
            )
            return False
        if intro_confidence < float(positive_intro_fast_min_confidence):
            log(
                "STEP3",
                (
                    "Positive intro fast-path skipped: confidence too low "
                    f"({intro_confidence:.2f} < {float(positive_intro_fast_min_confidence):.2f})"
                ),
                CYAN,
            )
            return False

        auto_offset_path.write_text(f"{intro_offset:.3f}\n", encoding="utf-8")
        meta_payload: dict[str, Any] = {
            "slug": slug,
            "mode": mode_label,
            "requested_accuracy_level": int(requested_level),
            "requested_calibration_level": int(calibration),
            "min_confidence": float(min_confidence),
            "min_sample_confidence": float(min_sample_confidence),
            "anchor_count": int(anchor_count),
            "sample_count": 0,
            "samples": [],
            "mode_resolution": "positive_intro_gap_fast_path",
            "mode_disagreement_s": None,
            "calibration_offset_s": None,
            "tune_offset_s": None,
            "lrc_line_count": int(lrc_stats.get("line_count") or 0),
            "first_lyric_time_s": (
                float(lrc_stats.get("first_time_s")) if int(lrc_stats.get("line_count") or 0) > 0 else None
            ),
            "last_lyric_time_s": (
                float(lrc_stats.get("last_time_s")) if int(lrc_stats.get("line_count") or 0) > 0 else None
            ),
            "lead_gap_ratio": (
                float(lrc_stats.get("lead_gap_ratio")) if int(lrc_stats.get("line_count") or 0) > 0 else None
            ),
            "large_lead_gap_detected": bool(lrc_stats.get("large_lead_gap")),
            "status": "applied_positive_intro_gap_fast_path",
            "successful_samples": 0,
            "high_confidence_samples": 0,
            "selected_samples": 0,
            "selected_sample_indexes": [],
            "aggregate_offset_s": float(intro_offset),
            "aggregate_confidence": float(intro_confidence),
            "applied_offset_s": float(intro_offset),
            "low_confidence": False,
            "manual_offset_recommended": False,
            "positive_intro_gap_detected": True,
            "positive_intro_gap_fast_path": True,
            "intro_gap_confidence": float(intro_confidence),
            "first_lyric_gap_s": float(first_gap_s),
        }
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log(
            "STEP3",
            (
                f"Applied positive intro fast-path offset {intro_offset:+.3f}s "
                f"(first_lyric={first_t:.3f}s, first_gap={first_gap_s:.3f}s, "
                f"confidence={intro_confidence:.2f})"
            ),
            YELLOW,
        )
        return True

    def _maybe_apply_large_gap_fallback(meta_payload: dict[str, Any], *, reason: str) -> bool:
        if (not large_gap_fallback_enabled) or (not bool(lrc_stats.get("large_lead_gap"))):
            return False
        first_t = float(lrc_stats.get("first_time_s") or 0.0)
        fallback_offset = -float(first_t)
        if abs(fallback_offset) > float(large_gap_max_apply_abs):
            log(
                "STEP3",
                (
                    "Large lead-gap fallback skipped: inferred shift out of range "
                    f"({fallback_offset:+.3f}s > ±{float(large_gap_max_apply_abs):.1f}s)"
                ),
                CYAN,
            )
            return False
        auto_offset_path.write_text(f"{fallback_offset:.3f}\n", encoding="utf-8")
        meta_payload.update(
            {
                "status": "applied_large_lead_gap_fallback",
                "aggregate_offset_s": float(fallback_offset),
                "aggregate_confidence": max(
                    float(meta_payload.get("aggregate_confidence") or 0.0),
                    0.51,
                ),
                "applied_offset_s": float(fallback_offset),
                "low_confidence": False,
                "manual_offset_recommended": False,
                "fallback_reason": str(reason),
            }
        )
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        log(
            "STEP3",
            (
                f"Applied large lead-gap fallback offset {fallback_offset:+.3f}s "
                f"(reason={reason}, first_lyric={first_t:.3f}s)"
            ),
            YELLOW,
        )
        return True

    calibration = int(max(0, min(3, int(calibration_level or 0))))
    requested_level = int(max(0, min(3, int(accuracy_level or 0))))

    mode_parts: list[str] = []
    calibration_anchors: list[Optional[float]] = []
    tune_anchors: list[Optional[float]] = []
    all_anchors: list[Optional[float]] = []

    cal_anchor_keys: set[str] = set()
    if calibration > 0:
        mode_parts.append(f"calibration level={calibration}")
        calibration_anchors = _choose_calibration_anchors(lrc_path=lrc_path, calibration_level=calibration)
        all_anchors.extend(calibration_anchors)
        for a in calibration_anchors:
            if isinstance(a, (int, float)):
                cal_anchor_keys.add(f"{float(a):.3f}")

    if requested_level > 0:
        mode_parts.append(f"tune-for-me level={requested_level}")
        tune_anchors = _choose_auto_offset_anchors(
            lrc_path=lrc_path,
            slug=slug,
            accuracy_level=requested_level,
            avoid_anchor_keys=cal_anchor_keys,
        )
        all_anchors.extend(tune_anchors)

    if not all_anchors:
        # Safety fallback for legacy callers that enable auto-offset without explicit levels.
        requested_level = 1
        mode_parts.append("tune-for-me level=1")
        tune_anchors = _choose_auto_offset_anchors(
            lrc_path=lrc_path,
            slug=slug,
            accuracy_level=requested_level,
            avoid_anchor_keys=cal_anchor_keys,
        )
        all_anchors.extend(tune_anchors)

    # Deduplicate by rounded anchor time while preserving order.
    anchors: list[Optional[float]] = []
    seen_anchor_keys: set[str] = set()
    for anchor in all_anchors:
        if isinstance(anchor, (int, float)):
            key = f"{float(anchor):.3f}"
        else:
            key = "None"
        if key in seen_anchor_keys:
            continue
        seen_anchor_keys.add(key)
        anchors.append(anchor)

    if not anchors:
        anchors = [None]

    calibration_anchors = [
        anchor
        for anchor in anchors
        if (
            (not isinstance(anchor, (int, float)) and "None" in cal_anchor_keys)
            or (
                isinstance(anchor, (int, float))
                and f"{float(anchor):.3f}" in cal_anchor_keys
            )
        )
    ]
    tune_anchors = [
        anchor
        for anchor in anchors
        if not (
            (not isinstance(anchor, (int, float)) and "None" in cal_anchor_keys)
            or (
                isinstance(anchor, (int, float))
                and f"{float(anchor):.3f}" in cal_anchor_keys
            )
        )
    ]

    mode_label = " + ".join(mode_parts)
    log("STEP3", f"Auto-offset mode={mode_label} anchors={len(anchors)}", CYAN)
    if _maybe_apply_positive_intro_fast_path(mode_label=mode_label, anchor_count=len(anchors)):
        return

    min_selected_samples = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_MIN_SELECTED_SAMPLES", "2"))))
    if len(anchors) <= 2:
        min_selected_samples = 1
    min_hi_conf_samples = int(max(1, int(os.environ.get("KARAOKE_AUTO_OFFSET_MIN_HI_CONF_SAMPLES", "2"))))
    if len(anchors) <= 2:
        min_hi_conf_samples = 1

    sample_rows: list[dict[str, Any]]
    agg: Optional[dict[str, Any]]
    forced_mode_resolution: Optional[str] = None
    progressive_early_exit = False

    if calibration_anchors and tune_anchors:
        sample_rows = _run_auto_offset_samples(
            lrc_path=lrc_path,
            audio_path=audio_path,
            language=language,
            anchors=calibration_anchors,
            max_sample_abs_offset=max_sample_abs_offset,
            pass_label="default",
            index_start=1,
        )
        agg = _aggregate_sample_offsets(sample_rows, min_sample_confidence=min_sample_confidence)

        if _should_progressive_calibration_exit(
            requested_level=requested_level,
            calibration_anchor_count=len(calibration_anchors),
            calibration_agg=agg,
            min_selected_samples=min_selected_samples,
            min_hi_conf_samples=min_hi_conf_samples,
            min_confidence=min_confidence,
        ):
            progressive_early_exit = True
            forced_mode_resolution = "calibration_only_progressive_early_exit"
            log(
                "STEP3",
                (
                    "Auto-offset progressive early exit: calibration anchors converged; "
                    f"skipping {len(tune_anchors)} tune anchors"
                ),
                CYAN,
            )
        else:
            tune_rows = _run_auto_offset_samples(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
                anchors=tune_anchors,
                max_sample_abs_offset=max_sample_abs_offset,
                pass_label="default",
                index_start=(len(sample_rows) + 1),
            )
            sample_rows.extend(tune_rows)
            agg = _aggregate_sample_offsets(sample_rows, min_sample_confidence=min_sample_confidence)
    else:
        sample_rows = _run_auto_offset_samples(
            lrc_path=lrc_path,
            audio_path=audio_path,
            language=language,
            anchors=anchors,
            max_sample_abs_offset=max_sample_abs_offset,
            pass_label="default",
            index_start=1,
        )
        agg = _aggregate_sample_offsets(sample_rows, min_sample_confidence=min_sample_confidence)

    needs_retry = bool(retry_on_weak)
    if progressive_early_exit:
        needs_retry = False
    if needs_retry and agg:
        needs_retry = len(list(agg.get("hi_conf_rows") or [])) < min_hi_conf_samples
    if needs_retry and not agg:
        needs_retry = True

    if needs_retry:
        retry_overrides = {
            "KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE": os.environ.get("KARAOKE_AUTO_OFFSET_RETRY_MIN_MATCH_SCORE", "0.32"),
            "KARAOKE_AUTO_OFFSET_SEGMENT_ANCHOR_FRAC": os.environ.get(
                "KARAOKE_AUTO_OFFSET_RETRY_SEGMENT_ANCHOR_FRAC", "0.16"
            ),
            "KARAOKE_AUTO_OFFSET_CLIP_SECS": os.environ.get("KARAOKE_AUTO_OFFSET_RETRY_CLIP_SECS", "42.0"),
        }
        retry_max_sample_abs_offset = float(
            os.environ.get("KARAOKE_AUTO_OFFSET_RETRY_MAX_SAMPLE_ABS_SECS", str(max_sample_abs_offset))
        )
        log(
            "STEP3",
            (
                "Auto-offset retry pass enabled "
                f"(match>={retry_overrides['KARAOKE_AUTO_OFFSET_MIN_MATCH_SCORE']}, "
                f"clip={retry_overrides['KARAOKE_AUTO_OFFSET_CLIP_SECS']}s)"
            ),
            CYAN,
        )
        with _temporary_env(retry_overrides):
            retry_rows = _run_auto_offset_samples(
                lrc_path=lrc_path,
                audio_path=audio_path,
                language=language,
                anchors=anchors,
                max_sample_abs_offset=retry_max_sample_abs_offset,
                pass_label="retry",
                index_start=(len(sample_rows) + 1),
            )
        sample_rows.extend(retry_rows)
        retry_agg = _aggregate_sample_offsets(retry_rows, min_sample_confidence=min_sample_confidence)
        combined_agg = _aggregate_sample_offsets(sample_rows, min_sample_confidence=min_sample_confidence)
        candidates = [c for c in [agg, retry_agg, combined_agg] if c]
        agg = max(candidates, key=_aggregate_quality) if candidates else None

    calibration_rows = [r for r in sample_rows if str(r.get("anchor_key") or "None") in cal_anchor_keys]
    tune_rows = [r for r in sample_rows if str(r.get("anchor_key") or "None") not in cal_anchor_keys]
    calibration_agg = (
        _aggregate_sample_offsets(calibration_rows, min_sample_confidence=min_sample_confidence)
        if calibration > 0
        else None
    )
    tune_agg = (
        _aggregate_sample_offsets(tune_rows, min_sample_confidence=min_sample_confidence)
        if requested_level > 0
        else None
    )
    mode_resolution = "combined"
    mode_disagreement_s: Optional[float] = None
    if calibration_agg and tune_agg:
        cal_off = float(calibration_agg.get("offset_s") or 0.0)
        tune_off = float(tune_agg.get("offset_s") or 0.0)
        mode_disagreement_s = abs(cal_off - tune_off)
        if mode_disagreement_s >= float(mode_disagreement_secs):
            tune_hi = len(list(tune_agg.get("hi_conf_rows") or []))
            tune_used = len(list(tune_agg.get("used_rows") or []))
            if tune_hi >= min_hi_conf_samples and tune_used >= min_selected_samples:
                agg = tune_agg
                mode_resolution = "prefer_tune_large_mode_disagreement"
            else:
                agg = tune_agg if _aggregate_quality(tune_agg) > _aggregate_quality(calibration_agg) else calibration_agg
                mode_resolution = "best_quality_large_mode_disagreement"
            log(
                "STEP3",
                (
                    f"Mode disagreement detected (cal={cal_off:+.3f}s vs tune={tune_off:+.3f}s, "
                    f"delta={mode_disagreement_s:.3f}s) -> {mode_resolution}"
                ),
                YELLOW,
            )
    elif tune_agg:
        mode_resolution = "tune_only"
    elif calibration_agg:
        mode_resolution = "calibration_only"
    if forced_mode_resolution:
        mode_resolution = str(forced_mode_resolution)

    early_consensus_agg = _aggregate_early_anchor_consensus(
        sample_rows,
        min_sample_confidence=min_sample_confidence,
    )
    early_consensus_reason: Optional[str] = None
    first_lyric_time_s = float(lrc_stats.get("first_time_s") or 0.0)
    early_consensus_min_first_lyric = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MIN_FIRST_LYRIC_SECS", "12.0")
    )
    early_consensus_min_confidence = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MIN_CONFIDENCE", "0.50")
    )
    early_consensus_max_spread = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MAX_SPREAD_SECS", "1.25")
    )
    early_consensus_min_disagreement = float(
        os.environ.get("KARAOKE_AUTO_OFFSET_EARLY_CONSENSUS_MIN_DISAGREE_SECS", "4.0")
    )
    if (
        agg
        and early_consensus_agg
        and first_lyric_time_s >= float(early_consensus_min_first_lyric)
        and float(early_consensus_agg.get("confidence") or 0.0) >= float(early_consensus_min_confidence)
        and float(early_consensus_agg.get("spread_s") or 0.0) <= float(early_consensus_max_spread)
    ):
        global_offset = float(agg.get("offset_s") or 0.0)
        early_offset = float(early_consensus_agg.get("offset_s") or 0.0)
        disagreement = abs(global_offset - early_offset)
        sign_flip = (
            (global_offset < 0.0 and early_offset > 0.0)
            or (global_offset > 0.0 and early_offset < 0.0)
        )
        if sign_flip and disagreement >= float(early_consensus_min_disagreement):
            agg = early_consensus_agg
            mode_resolution = "prefer_early_consensus_late_opening_disagreement"
            early_consensus_reason = "large_sign_disagreement"
            log(
                "STEP3",
                (
                    f"Early-anchor consensus selected (early={early_offset:+.3f}s vs aggregate={global_offset:+.3f}s, "
                    f"first_lyric={first_lyric_time_s:.3f}s)"
                ),
                YELLOW,
            )

    meta_payload: dict[str, Any] = {
        "slug": slug,
        "mode": mode_label,
        "requested_accuracy_level": int(requested_level),
        "requested_calibration_level": int(calibration),
        "min_confidence": float(min_confidence),
        "min_sample_confidence": float(min_sample_confidence),
        "anchor_count": int(len(anchors)),
        "sample_count": int(len(sample_rows)),
        "samples": sample_rows,
        "mode_resolution": mode_resolution,
        "mode_disagreement_s": (float(mode_disagreement_s) if isinstance(mode_disagreement_s, (int, float)) else None),
        "calibration_offset_s": (
            float(calibration_agg.get("offset_s")) if isinstance(calibration_agg, dict) and ("offset_s" in calibration_agg) else None
        ),
        "tune_offset_s": (float(tune_agg.get("offset_s")) if isinstance(tune_agg, dict) and ("offset_s" in tune_agg) else None),
        "lrc_line_count": int(lrc_stats.get("line_count") or 0),
        "first_lyric_time_s": (
            float(lrc_stats.get("first_time_s")) if int(lrc_stats.get("line_count") or 0) > 0 else None
        ),
        "last_lyric_time_s": (
            float(lrc_stats.get("last_time_s")) if int(lrc_stats.get("line_count") or 0) > 0 else None
        ),
        "lead_gap_ratio": (
            float(lrc_stats.get("lead_gap_ratio")) if int(lrc_stats.get("line_count") or 0) > 0 else None
        ),
        "large_lead_gap_detected": bool(lrc_stats.get("large_lead_gap")),
        "progressive_early_exit": bool(progressive_early_exit),
        "skipped_tune_anchor_count": (int(len(tune_anchors)) if progressive_early_exit else 0),
        "early_consensus_offset_s": (
            float(early_consensus_agg.get("offset_s"))
            if isinstance(early_consensus_agg, dict) and ("offset_s" in early_consensus_agg)
            else None
        ),
        "early_consensus_confidence": (
            float(early_consensus_agg.get("confidence"))
            if isinstance(early_consensus_agg, dict) and ("confidence" in early_consensus_agg)
            else None
        ),
        "early_consensus_reason": early_consensus_reason,
    }
    if not agg:
        log("STEP3", "Auto offset skipped: no successful tuning samples", YELLOW)
        meta_payload.update(
            {
                "status": "no_successful_samples",
                "successful_samples": 0,
                "aggregate_offset_s": None,
                "aggregate_confidence": None,
                "applied_offset_s": None,
                "manual_offset_recommended": True,
            }
        )
        if _maybe_apply_intro_gap_fallback(meta_payload, reason="no_successful_samples"):
            return
        if _maybe_apply_large_gap_fallback(meta_payload, reason="no_successful_samples"):
            return
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        return

    used_rows = list(agg.get("used_rows") or [])
    ok_rows = list(agg.get("ok_rows") or [])
    hi_conf_rows = list(agg.get("hi_conf_rows") or [])
    offset_secs = float(agg["offset_s"])
    confidence = float(agg["confidence"])
    selected_sample_indexes = [int(r.get("index") or 0) for r in used_rows]

    confidence_rows = hi_conf_rows if len(hi_conf_rows) >= min_hi_conf_samples else used_rows
    if confidence_rows:
        confidence = float(statistics.median([float(r.get("confidence", 0.0)) for r in confidence_rows]))

    raw_offset_secs = float(offset_secs)
    damped_offset, damp_meta = _maybe_dampen_weak_single_calibration_offset(
        offset_s=offset_secs,
        confidence=confidence,
        used_rows=used_rows,
        sample_rows=sample_rows,
        mode_resolution=mode_resolution,
        large_lead_gap_detected=bool(lrc_stats.get("large_lead_gap")),
    )
    if damp_meta:
        log(
            "STEP3",
            (
                "Weak single calibration sample detected; "
                f"clamping auto offset {raw_offset_secs:+.3f}s -> {damped_offset:+.3f}s "
                f"(confidence={confidence:.2f})"
            ),
            YELLOW,
        )
        offset_secs = float(damped_offset)
        meta_payload.update(
            {
                "weak_single_sample_damped": True,
                "raw_aggregate_offset_s": float(raw_offset_secs),
                "damped_aggregate_offset_s": float(damped_offset),
                "weak_single_sample_confidence_max": float(damp_meta["weak_confidence_max"]),
                "weak_single_sample_max_abs_s": float(damp_meta["damped_max_abs_s"]),
            }
        )

    log(
        "STEP3",
        (
            f"Whisper offset: {offset_secs:+.3f}s "
            f"(confidence: {confidence:.2f}, samples={len(used_rows)}/{len(sample_rows)}, "
            f"successful={len(ok_rows)}, hi_conf={len(hi_conf_rows)})"
        ),
        CYAN,
    )
    meta_payload.update(
        {
            "successful_samples": int(len(ok_rows)),
            "high_confidence_samples": int(len(hi_conf_rows)),
            "selected_samples": int(len(used_rows)),
            "selected_sample_indexes": selected_sample_indexes,
            "aggregate_offset_s": float(offset_secs),
            "aggregate_confidence": float(confidence),
        }
    )

    if len(used_rows) < min_selected_samples:
        log(
            "STEP3",
            (
                f"Auto offset sample count too low ({len(used_rows)} < {min_selected_samples}), "
                "not applying"
            ),
            YELLOW,
        )
        meta_payload.update(
            {
                "status": "insufficient_selected_samples",
                "applied_offset_s": 0.0,
                "manual_offset_recommended": True,
            }
        )
        if _maybe_apply_intro_gap_fallback(meta_payload, reason="insufficient_selected_samples"):
            return
        if _maybe_apply_large_gap_fallback(meta_payload, reason="insufficient_selected_samples"):
            return
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        return

    if len(hi_conf_rows) < min_hi_conf_samples:
        log(
            "STEP3",
            (
                f"High-confidence sample count too low ({len(hi_conf_rows)} < {min_hi_conf_samples}), "
                "not applying"
            ),
            YELLOW,
        )
        meta_payload.update(
            {
                "status": "insufficient_high_confidence_samples",
                "applied_offset_s": 0.0,
                "manual_offset_recommended": True,
            }
        )
        if _maybe_apply_intro_gap_fallback(meta_payload, reason="insufficient_high_confidence_samples"):
            return
        if _maybe_apply_large_gap_fallback(meta_payload, reason="insufficient_high_confidence_samples"):
            return
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        return

    if abs(float(offset_secs)) > float(max_apply_abs_offset):
        log(
            "STEP3",
            (
                f"Auto offset out of safe range ({offset_secs:+.3f}s > ±{max_apply_abs_offset:.1f}s), "
                "not applying"
            ),
            YELLOW,
        )
        meta_payload.update(
            {
                "status": "offset_out_of_bounds",
                "applied_offset_s": 0.0,
                "manual_offset_recommended": True,
            }
        )
        if _maybe_apply_intro_gap_fallback(meta_payload, reason="offset_out_of_bounds"):
            return
        if _maybe_apply_large_gap_fallback(meta_payload, reason="offset_out_of_bounds"):
            return
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        return

    if confidence < min_confidence:
        log("STEP3", f"Auto offset confidence too low ({confidence:.2f} < {min_confidence:.2f}), not applying", YELLOW)
        if _maybe_apply_intro_gap_fallback(meta_payload, reason="low_confidence"):
            return
        if _maybe_apply_large_gap_fallback(meta_payload, reason="low_confidence"):
            return
        # Write a marker file to indicate low confidence
        auto_offset_path.write_text(f"0.0\n# Low confidence: {confidence:.2f}\n", encoding="utf-8")
        meta_payload.update(
            {
                "status": "low_confidence",
                "applied_offset_s": 0.0,
                "low_confidence": True,
                "manual_offset_recommended": True,
            }
        )
        try:
            _write_json_atomic(auto_meta_path, meta_payload)
        except Exception:
            pass
        return

    if bool(lrc_stats.get("large_lead_gap")) and abs(float(offset_secs)) < max(0.0, float(large_gap_reuse_min_abs)):
        log(
            "STEP3",
            (
                f"Whisper offset {offset_secs:+.3f}s looks too small for detected large lead-gap; "
                "applying fallback heuristic"
            ),
            YELLOW,
        )
        if _maybe_apply_large_gap_fallback(meta_payload, reason="offset_too_small_for_large_gap"):
            return

    auto_offset_path.write_text(f"{offset_secs:.3f}\n", encoding="utf-8")
    meta_payload.update(
        {
            "status": "applied",
            "applied_offset_s": float(offset_secs),
            "low_confidence": False,
        }
    )
    try:
        _write_json_atomic(auto_meta_path, meta_payload)
    except Exception:
        pass
    log("STEP3", f"✓ Applied auto offset {offset_secs:+.3f}s (confidence: {confidence:.2f}) -> {auto_offset_path}", GREEN)


def step3_sync(
    paths,
    slug,
    flags,
    language=None,
    *,
    write_raw_csv: bool = True,
    run_auto_offset: bool = True,
    auto_offset_default_enabled: bool = False,
    auto_offset_force_refresh: bool = False,
    auto_offset_accuracy: int = 1,
    auto_offset_calibration_level: int = 0,
    cli_offset_hint: float = 0.0,
    run_smart_micro_offset: bool = False,
):
    step_t0 = now_perf_ms()
    raw_csv = TIMINGS_DIR / f"{slug}.raw.csv"
    final_csv = TIMINGS_DIR / f"{slug}.csv"

    lrc_path = TIMINGS_DIR / f"{slug}.lrc"
    if not lrc_path.exists():
        if STRICT_REQUIRE_LYRICS:
            raise RuntimeError(f"No lyrics found for slug={slug} (missing LRC file)")
        log("[STEP3] no LRC found, skipping", GREEN)
        log_timing("step3", "total", step_t0)
        return

    from scripts.lrc_utils import parse_lrc

    # parse_lrc returns (events, meta)
    t0 = now_perf_ms()
    events, _meta = parse_lrc(str(lrc_path))
    non_blank = [ev for ev in events if str(_extract_event(ev)[1] or "").strip()]
    if not non_blank:
        if STRICT_REQUIRE_LYRICS:
            raise RuntimeError(f"No lyrics found for slug={slug} (empty/invalid LRC)")
        try:
            raw_lrc = lrc_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            raw_lrc = ""

        recovered_lines: list[str] = []
        for line in raw_lrc.splitlines():
            txt = re.sub(r"\[\d+:\d{2}(?:\.\d{1,3})?\]", "", line).strip()
            if not txt:
                continue
            if re.match(r"^\s*\[[a-zA-Z]+\s*:", line):
                continue
            recovered_lines.append(txt)

        if not recovered_lines:
            meta_path = TIMINGS_DIR.parent / "meta" / f"{slug}.step1.json"
            if meta_path.exists():
                try:
                    data = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
                title = str((data or {}).get("title") or "").strip()
                artist = str((data or {}).get("artist") or "").strip()
                query = str((data or {}).get("query") or "").strip()
                seed = " - ".join([x for x in [artist, title] if x]).strip() or title or artist or query
                if seed:
                    recovered_lines = [seed]

        if recovered_lines:
            start_secs = max(0.0, float(os.environ.get("MIXTERIOSO_LRC_PSEUDO_START_SECS", "8.0")))
            step_secs = max(0.25, float(os.environ.get("MIXTERIOSO_LRC_PSEUDO_STEP_SECS", "3.0")))
            events = [{"t": start_secs + (i * step_secs), "text": txt} for i, txt in enumerate(recovered_lines)]
            log("STEP3", f"No parseable lyric events; synthesized {len(events)} fallback events", YELLOW)
    log_timing("step3", "parse_lrc", t0)

    n_written = 0
    if write_raw_csv:
        t0 = now_perf_ms()
        _write_timings_csv(raw_csv, events)
        log_timing("step3", "write_raw_csv", t0)
    else:
        log("STEP3", "Skipping raw timings CSV (lite mode)", CYAN)

    t0 = now_perf_ms()
    n_written = _write_timings_csv(final_csv, events)
    log_timing("step3", "write_final_csv", t0)
    if run_auto_offset:
        t0 = now_perf_ms()
        _maybe_write_auto_offset(
            paths,
            slug,
            language=language,
            default_enabled=auto_offset_default_enabled,
            force_refresh=(auto_offset_force_refresh or bool(getattr(flags, "force", False))),
            accuracy_level=int(max(0, min(3, int(auto_offset_accuracy or 0)))),
            calibration_level=int(max(0, min(3, int(auto_offset_calibration_level or 0)))),
        )
        log_timing("step3", "auto_offset", t0)
    else:
        if _clear_auto_offset_artifacts(slug):
            log("STEP3", "Cleared stale auto offset artifacts", CYAN)
        log("STEP3", "Skipping explicit auto offset", CYAN)
        if run_smart_micro_offset:
            t0 = now_perf_ms()
            _maybe_write_smart_micro_offset(
                paths,
                slug,
                language=language,
                force_refresh=bool(getattr(flags, "force", False)),
                cli_offset_hint=float(cli_offset_hint or 0.0),
            )
            log_timing("step3", "smart_micro_offset", t0)

    log(f"[STEP3] wrote {n_written} lyric lines", GREEN)
    log_timing("step3", "total", step_t0)


def step3_sync_lite(
    paths,
    slug,
    flags,
    language=None,
    *,
    cli_offset_hint: float = 0.0,
    run_smart_micro_offset: bool = False,
):
    step3_sync(
        paths,
        slug,
        flags,
        language=language,
        write_raw_csv=False,
        run_auto_offset=False,
        cli_offset_hint=float(cli_offset_hint or 0.0),
        run_smart_micro_offset=bool(run_smart_micro_offset),
    )
    run_lite_auto_offset = _env_flag("MIXTERIOSO_STEP3_LITE_AUTO_OFFSET", False)
    if not run_lite_auto_offset:
        return

    t0 = now_perf_ms()
    try:
        _maybe_write_auto_offset(
            paths,
            slug,
            language=language,
            default_enabled=True,
            force_refresh=bool(getattr(flags, "force", False)),
            accuracy_level=1,
            calibration_level=0,
        )
    finally:
        log_timing("step3", "auto_offset_lite", t0)
# end of step3_sync.py
