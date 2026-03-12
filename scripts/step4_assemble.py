#!/usr/bin/env python3
import argparse
import csv
import json
import subprocess
import sys
import time
import shutil
from pathlib import Path
import os
import hashlib
import re
from io import StringIO

from .common import (
    ffmpeg_escape_filter_path,
    ffmpeg_has_filter,
    ffmpeg_has_encoder,
    log_timing,
    now_perf_ms,
    resolve_output_dir,
    resolve_ffmpeg_bin,
    run_cmd_capture,
)

RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
BLUE = "\033[34m"

BASE_DIR = Path(__file__).resolve().parent.parent
TXT_DIR = BASE_DIR / "txts"
MP3_DIR = BASE_DIR / "mp3s"
MIXES_DIR = BASE_DIR / "mixes"
TIMINGS_DIR = BASE_DIR / "timings"
OUTPUT_DIR = resolve_output_dir(BASE_DIR)
META_DIR = BASE_DIR / "meta"

# =============================================================================
# FAST RENDER DEFAULTS
# =============================================================================
# You can override at runtime via env vars:
#   KARAOKE_RENDER_LEVEL="1..10" (aliases: low|medium|high|normal|preview|fast)
#   KARAOKE_RENDER_PROFILE="fast|turbo"  (dedicated speed presets)
#   KARAOKE_VIDEO_SIZE="480x270"
#   KARAOKE_FPS="2"
#   KARAOKE_VIDEO_BITRATE="100k"
#   KARAOKE_AUDIO_BITRATE="96k"
#   KARAOKE_NO_FASTSTART=1
#   KARAOKE_FORCE_LIBX264=1
#   KARAOKE_FFMPEG_BENCH=1
#   KARAOKE_PREFER_MP3_FOR_RENDER=1|0 (default: auto; turbo prefers MP3)
#   KARAOKE_TURBO_VALIDATE_DURATION=1 (default: 0; skip ffprobe duration check in turbo)
# =============================================================================
_RENDER_LEVEL_ENV = (os.environ.get("KARAOKE_RENDER_LEVEL") or "").strip().lower()
_RENDER_PROFILE_ENV = (os.environ.get("KARAOKE_RENDER_PROFILE") or "").strip().lower()
_VIDEO_SIZE_ENV = (os.environ.get("KARAOKE_VIDEO_SIZE") or "").strip().lower()
_FPS_ENV = (os.environ.get("KARAOKE_FPS") or "").strip()

_RENDER_LEVEL_PRESETS: dict[int, dict[str, str | int]] = {
    1: {"size": "426x240", "fps": 2, "video_bitrate": "64k", "audio_bitrate": "96k", "x264_preset": "ultrafast", "x264_tune": "zerolatency"},
    2: {"size": "480x270", "fps": 2, "video_bitrate": "80k", "audio_bitrate": "112k", "x264_preset": "ultrafast", "x264_tune": "zerolatency"},
    3: {"size": "640x360", "fps": 3, "video_bitrate": "96k", "audio_bitrate": "128k", "x264_preset": "superfast", "x264_tune": "zerolatency"},
    4: {"size": "640x360", "fps": 4, "video_bitrate": "120k", "audio_bitrate": "160k", "x264_preset": "veryfast", "x264_tune": ""},
    5: {"size": "854x480", "fps": 4, "video_bitrate": "160k", "audio_bitrate": "192k", "x264_preset": "veryfast", "x264_tune": ""},
    6: {"size": "960x540", "fps": 5, "video_bitrate": "220k", "audio_bitrate": "192k", "x264_preset": "faster", "x264_tune": ""},
    7: {"size": "1280x720", "fps": 6, "video_bitrate": "320k", "audio_bitrate": "224k", "x264_preset": "faster", "x264_tune": ""},
    8: {"size": "1280x720", "fps": 8, "video_bitrate": "480k", "audio_bitrate": "256k", "x264_preset": "fast", "x264_tune": ""},
    9: {"size": "1600x900", "fps": 10, "video_bitrate": "700k", "audio_bitrate": "256k", "x264_preset": "medium", "x264_tune": ""},
    10: {"size": "1920x1080", "fps": 12, "video_bitrate": "1000k", "audio_bitrate": "320k", "x264_preset": "medium", "x264_tune": ""},
}

_RENDER_LEVEL_ALIASES = {
    "preview": 1,
    "fast": 1,
    "ultrafast": 1,
    "ultra_fast": 1,
    "draft": 2,
    "low": 3,
    "medium": 5,
    "normal": 5,
    "default": 5,
    "high": 7,
    "ultra": 8,
    "max": 10,
}

_RENDER_PROFILE_PRESETS: dict[str, dict[str, str | int]] = {
    # Dedicated speed-first profile tuned for quick previews and low-latency runs.
    "fast": {
        "size": "854x480",
        "fps": 2,
        "video_bitrate": "56k",
        "audio_bitrate": "64k",
        "x264_preset": "ultrafast",
        "x264_tune": "zerolatency",
    },
    # Extreme speed profile for "first playable output" latency.
    "turbo": {
        "size": "854x480",
        "fps": 1,
        "video_bitrate": "48k",
        "audio_bitrate": "64k",
        "x264_preset": "ultrafast",
        "x264_tune": "zerolatency",
    },
}


def _resolve_render_level(raw_level: str, raw_profile: str) -> tuple[int, str]:
    if raw_level:
        if raw_level in _RENDER_LEVEL_ALIASES:
            return _RENDER_LEVEL_ALIASES[raw_level], "render_level_alias"
        try:
            value = int(float(raw_level))
            value = max(1, min(10, value))
            return value, "render_level_numeric"
        except Exception:
            pass
    if raw_profile:
        if raw_profile in _RENDER_LEVEL_ALIASES:
            return _RENDER_LEVEL_ALIASES[raw_profile], "render_profile_alias"
    return 5, "default"


RENDER_LEVEL, RENDER_LEVEL_SOURCE = _resolve_render_level(_RENDER_LEVEL_ENV, _RENDER_PROFILE_ENV)
RENDER_PROFILE_NAME = ""
if _RENDER_PROFILE_ENV in _RENDER_PROFILE_PRESETS:
    _preset = dict(_RENDER_PROFILE_PRESETS[_RENDER_PROFILE_ENV])
    RENDER_PROFILE_NAME = _RENDER_PROFILE_ENV
    RENDER_LEVEL_SOURCE = "render_profile_preset"
else:
    _preset = dict(_RENDER_LEVEL_PRESETS[RENDER_LEVEL])

_size_w_s, _size_h_s = str(_preset["size"]).split("x", 1)
VIDEO_WIDTH = int(_size_w_s)
VIDEO_HEIGHT = int(_size_h_s)
FPS = int(_preset["fps"])
VIDEO_BITRATE = str(_preset["video_bitrate"])
AUDIO_BITRATE = str(_preset["audio_bitrate"])
X264_PRESET = str(_preset["x264_preset"])
X264_TUNE = str(_preset["x264_tune"])

if _VIDEO_SIZE_ENV and "x" in _VIDEO_SIZE_ENV:
    try:
        w_s, h_s = _VIDEO_SIZE_ENV.split("x", 1)
        VIDEO_WIDTH = max(64, int(w_s))
        VIDEO_HEIGHT = max(64, int(h_s))
    except Exception:
        pass

if _FPS_ENV:
    try:
        FPS = max(1, int(float(_FPS_ENV)))
    except Exception:
        pass

VIDEO_BITRATE = os.environ.get("KARAOKE_VIDEO_BITRATE", VIDEO_BITRATE)
VIDEO_GOP_SECONDS = float(os.environ.get("KARAOKE_VIDEO_GOP_SECONDS", "30") or "30")
AUDIO_BITRATE = os.environ.get("KARAOKE_AUDIO_BITRATE", AUDIO_BITRATE)
X264_PRESET = (os.environ.get("KARAOKE_X264_PRESET") or X264_PRESET).strip()
X264_TUNE = (os.environ.get("KARAOKE_X264_TUNE") or X264_TUNE).strip()

DISABLE_FASTSTART = (os.environ.get("KARAOKE_NO_FASTSTART") or "").strip().lower() in ("1", "true", "yes", "y")
ENABLE_BENCH = (os.environ.get("KARAOKE_FFMPEG_BENCH") or "").strip().lower() in ("1", "true", "yes", "y")
FORCE_LIBX264 = (os.environ.get("KARAOKE_FORCE_LIBX264") or "").strip().lower() in ("1", "true", "yes", "y")
VIDEO_ENCODER_OVERRIDE = (os.environ.get("KARAOKE_VIDEO_ENCODER") or "").strip()
AUDIO_ENCODER_OVERRIDE = (os.environ.get("KARAOKE_AUDIO_ENCODER") or "").strip()
AUDIO_COPY_WHEN_COMPATIBLE = (os.environ.get("KARAOKE_AUDIO_COPY_WHEN_COMPATIBLE") or "1").strip().lower() in ("1", "true", "yes", "y")

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _is_turbo_profile_active() -> bool:
    return str(RENDER_PROFILE_NAME or "").strip().lower() == "turbo"


def _prefer_mp3_audio_for_render() -> bool:
    raw = (os.environ.get("KARAOKE_PREFER_MP3_FOR_RENDER") or "").strip().lower()
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    return _is_turbo_profile_active()


def _prepared_render_audio_wait_secs() -> float:
    raw = (os.environ.get("KARAOKE_WAIT_FOR_PREPARED_RENDER_AUDIO_SECS") or "").strip()
    if raw:
        try:
            return max(0.0, float(raw))
        except Exception:
            return 0.0
    profile = str(RENDER_PROFILE_NAME or "").strip().lower()
    if profile in {"fast", "turbo"}:
        return 2.0
    return 0.0


def _should_validate_duration_probe() -> bool:
    raw = (os.environ.get("KARAOKE_VALIDATE_DURATION") or "").strip().lower()
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    if _is_turbo_profile_active():
        return (os.environ.get("KARAOKE_TURBO_VALIDATE_DURATION", "0") or "").strip().lower() in _TRUE_VALUES
    return True

# =============================================================================
# LAYOUT CONSTANTS
# =============================================================================
BOTTOM_BOX_HEIGHT_FRACTION = 0.20
TOP_BAND_FRACTION = 1.0 - BOTTOM_BOX_HEIGHT_FRACTION

NEXT_LYRIC_TOP_MARGIN_PX = 50
NEXT_LYRIC_BOTTOM_MARGIN_PX = 50

DIVIDER_LINE_OFFSET_UP_PX = 0
DIVIDER_HEIGHT_PX = 0.25

DIVIDER_LEFT_MARGIN_PX = VIDEO_WIDTH * 0.035
DIVIDER_RIGHT_MARGIN_PX = DIVIDER_LEFT_MARGIN_PX

VERTICAL_OFFSET_FRACTION = 0.0
TITLE_EXTRA_OFFSET_FRACTION = -0.20

NEXT_LINE_FONT_SCALE = 0.475
NEXT_LABEL_FONT_SCALE = NEXT_LINE_FONT_SCALE * 0.55

NEXT_LABEL_TOP_MARGIN_PX = 10
NEXT_LABEL_LEFT_MARGIN_PX = DIVIDER_LEFT_MARGIN_PX + NEXT_LABEL_TOP_MARGIN_PX

FADE_IN_MS = 0
FADE_OUT_MS = 0

# =============================================================================
# COLOR AND OPACITY CONSTANTS
# =============================================================================
GLOBAL_NEXT_COLOR_RGB = "FFFFFF"
GLOBAL_NEXT_ALPHA_HEX = "4D"

DIVIDER_COLOR_RGB = "FFFFFF"
DIVIDER_ALPHA_HEX = "80"

TOP_LYRIC_TEXT_COLOR_RGB = "FFFFFF"
TOP_LYRIC_TEXT_ALPHA_HEX = "00"

BOTTOM_BOX_BG_COLOR_RGB = "000000"
BOTTOM_BOX_BG_ALPHA_HEX = "00"

TOP_BOX_BG_COLOR_RGB = "000000"
TOP_BOX_BG_ALPHA_HEX = "00"

NEXT_LABEL_COLOR_RGB = "FFFFFF"
NEXT_LABEL_ALPHA_HEX = GLOBAL_NEXT_ALPHA_HEX

DEFAULT_UI_FONT_SIZE = max(12, int(round(120 * (VIDEO_HEIGHT / 720.0))))
ASS_FONT_MULTIPLIER = 1.5 * 0.67

LYRICS_OFFSET_SECS = float(os.getenv("KARAOKE_OFFSET_SECS", "0.0") or "0.0")
OFFSET_SECS_MIN = float(os.getenv("KARAOKE_OFFSET_SECS_MIN", "-120.0") or "-120.0")
OFFSET_SECS_MAX = float(os.getenv("KARAOKE_OFFSET_SECS_MAX", "120.0") or "120.0")
if OFFSET_SECS_MIN > OFFSET_SECS_MAX:
    OFFSET_SECS_MIN, OFFSET_SECS_MAX = OFFSET_SECS_MAX, OFFSET_SECS_MIN
MUTE_PREVIEW_PAD_SECS = max(1.0, float(os.getenv("KARAOKE_MUTE_PREVIEW_PAD_SECS", "6.0") or "6.0"))

MUSIC_NOTE_CHARS = "♪♫♬♩♭♯"
MUSIC_NOTE_KEYWORDS = {"instrumental", "solo", "guitar solo", "piano solo"}
TITLE_CARD_CREDIT_TEXT_EN = "This video was architected, engineered, and auto-generated by Miguel Cázares"
TITLE_CARD_CREDIT_TEXT_ES = "Este video fue diseñado, ideado, y generado automáticamente por Miguel Cázares"
TITLE_CARD_CREDIT_FONT_SCALE = 0.288
TITLE_CARD_CREDIT_MARGIN_X_PX = 0
TITLE_CARD_CREDIT_MARGIN_Y_PX = 30


def _now_ts() -> float:
    return round(time.time(), 3)


def clamp_offset_secs(value: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = 0.0
    return max(OFFSET_SECS_MIN, min(OFFSET_SECS_MAX, parsed))


def _clip_window_to_duration(start: float, end: float, duration: float) -> tuple[float, float] | None:
    upper = max(0.0, float(duration))
    start = max(0.0, min(float(start), upper))
    end = max(0.0, min(float(end), upper))
    if end <= start:
        return None
    return (start, end)


def _resolve_video_encoder(ffmpeg_bin: Path) -> str:
    if VIDEO_ENCODER_OVERRIDE:
        return VIDEO_ENCODER_OVERRIDE

    # Fast profile keeps software encode for predictable low-latency startup.
    if FORCE_LIBX264 or RENDER_PROFILE_NAME == "fast":
        return "libx264"

    if sys.platform == "darwin":
        return "h264_videotoolbox" if ffmpeg_has_encoder(ffmpeg_bin, "h264_videotoolbox") else "libx264"

    # Low-hanging Linux acceleration path: use NVENC when present.
    if sys.platform.startswith("linux"):
        if ffmpeg_has_encoder(ffmpeg_bin, "h264_nvenc"):
            nvidia_smi = shutil.which("nvidia-smi")
            if nvidia_smi:
                rc, out = run_cmd_capture([nvidia_smi, "-L"], tag="NVENC", dry_run=False)
                if rc == 0 and (out or "").strip():
                    return "h264_nvenc"
            # Some runtimes expose libcuda without nvidia-smi on PATH.
            for libcuda_path in (
                "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
                "/usr/lib64/libcuda.so.1",
                "/usr/lib/wsl/lib/libcuda.so.1",
            ):
                if Path(libcuda_path).exists():
                    return "h264_nvenc"
            log("FFMPEG", "NVENC encoder present but CUDA runtime unavailable; using libx264", YELLOW)
        return "libx264"

    return "libx264"


def _resolve_audio_encoder(ffmpeg_bin: Path) -> str:
    if AUDIO_ENCODER_OVERRIDE:
        return AUDIO_ENCODER_OVERRIDE

    if sys.platform == "darwin" and ffmpeg_has_encoder(ffmpeg_bin, "aac_at"):
        return "aac_at"

    return "aac"


def _audio_encoder_profile_args(audio_encoder: str) -> list[str]:
    if str(audio_encoder or "").strip().lower() == "aac_at":
        return []
    return ["-profile:a", "aac_low"]


def log(prefix: str, msg: str, color: str = RESET) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{color}[{ts}] [{prefix}] {msg}{RESET}")


def _log_step4_timing(part: str, start_ms: float) -> None:
    log_timing("step4", part, start_ms, color=CYAN)


def slugify(text: str) -> str:
    import re
    base = text.strip().lower()
    base = re.sub(r"\s+", "_", base)
    base = re.sub(r"[^\w\-]+", "", base)
    return base or "song"


def seconds_to_ass_time(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    total_cs = int(round(sec * 100))
    if total_cs < 0:
        total_cs = 0
    total_seconds, cs = divmod(total_cs, 100)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def rgb_to_bgr(rrggbb: str) -> str:
    s = (rrggbb or "").strip().lstrip("#")
    s = s.zfill(6)[-6:]
    rr = s[0:2]
    gg = s[2:4]
    bb = s[4:6]
    return f"{bb}{gg}{rr}"


def is_music_only(text: str) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if not stripped:
        return False
    if any(ch in MUSIC_NOTE_CHARS for ch in stripped):
        return True
    if not any(ch.isalnum() for ch in stripped):
        return True
    lower = stripped.lower()
    for kw in MUSIC_NOTE_KEYWORDS:
        if kw in lower:
            return True
    return False


def _split_query_artist_title_for_card(query_text: str) -> tuple[str, str]:
    raw = " ".join(str(query_text or "").split()).strip()
    if not raw:
        return "", ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return "", ""

    m = re.match(r"^\s*(.+?)\s*[-–—:|/]\s*(.+?)\s*$", raw)
    if not m:
        return "", ""

    artist = str(m.group(1) or "").strip(" -–—:|/")
    title = str(m.group(2) or "").strip(" -–—:|/")
    if not artist or not title:
        return "", ""
    return artist, title


def _looks_slug_like_title(value: str, slug: str) -> bool:
    raw = " ".join(str(value or "").split()).strip()
    if not raw:
        return True
    raw_slug = slugify(raw)
    expected_slug = slugify(str(slug or ""))
    if raw_slug and expected_slug and raw_slug == expected_slug:
        return True
    if "_" in raw:
        return True
    return False


def read_meta(slug: str) -> tuple[str, str]:
    meta_path = None
    cand1 = META_DIR / f"{slug}.step1.json"
    cand2 = META_DIR / f"{slug}.json"

    artist = ""
    title = slug

    if cand1.exists():
        meta_path = cand1
    elif cand2.exists():
        meta_path = cand2

    if meta_path and meta_path.exists():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            artist = str(data.get("artist") or data.get("lrc_artist") or "").strip()
            title = str(data.get("title") or data.get("lrc_title") or title).strip()
            query_for_card = str(
                data.get("query")
                or data.get("lrc_lookup_query_effective")
                or data.get("lrc_lookup_query")
                or data.get("lookup_query")
                or data.get("query_sanitized")
                or ""
            ).strip()
            query_artist, query_title = _split_query_artist_title_for_card(query_for_card)
            if query_artist and query_title:
                artist = query_artist
                title = query_title
            elif query_for_card and (not query_for_card.startswith("http://")) and (not query_for_card.startswith("https://")):
                if not title or _looks_slug_like_title(title, slug):
                    title = query_for_card
        except Exception as e:
            log("META", f"Failed to read meta {meta_path}: {e}", YELLOW)

    artist, title = _normalize_title_artist_for_card(artist, title)
    return artist, title


def _normalize_title_artist_for_card(artist: str, title: str) -> tuple[str, str]:
    artist = str(artist or "").strip()
    title = str(title or "").strip()
    if not artist or not title:
        return artist, title

    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.lower())

    def _strip_lyrics_suffix(s: str) -> str:
        pat = re.compile(
            r"(?:\s*[-–—:|/(),\[\]]\s*)?"
            r"(?:(?:official|original|music)\s+)?"
            r"(?:lyric(?:s)?(?:\s+video)?|with\s+lyrics?|video\s+lyrics?|letra(?:s)?)\s*$",
            flags=re.IGNORECASE,
        )
        out = str(s or "").strip()
        prev = ""
        while out and out != prev:
            prev = out
            out = pat.sub("", out).strip()
        return out

    def _looks_handle(s: str) -> bool:
        raw = str(s or "").strip()
        if not raw:
            return False
        if re.search(r"\d", raw):
            return True
        if any(ch in raw for ch in ("_", "@", ".")):
            return True
        return len(raw.split()) == 1 and len(raw) >= 10

    title = _strip_lyrics_suffix(title) or title
    artist_clean = re.sub(r"(?:\s*[-–—:|/]*)?(?:topic|vevo|official|channel)\s*$", "", artist, flags=re.IGNORECASE).strip()
    noisy_artist = bool(artist_clean != artist)
    handle_like_artist = _looks_handle(artist_clean or artist)
    artist_norm = _norm(artist_clean or artist)
    if not artist_norm:
        return artist, title

    # Repeatedly strip leading "Artist - " / "Artist: " style prefixes.
    # Handles cases like "The Beatles - The Beatles - Let It Be".
    for _ in range(4):
        pattern = rf"^\s*{re.escape(artist)}\s*[-–—:|/]\s*"
        next_title = re.sub(pattern, "", title, count=1, flags=re.IGNORECASE).strip()
        if next_title == title:
            break
        title = next_title

    tokens = [t.strip() for t in re.split(r"\s*[-–—:|/]\s*", title) if t.strip()]
    if (noisy_artist or handle_like_artist) and len(tokens) >= 2:
        first_norm = _norm(tokens[0])
        if first_norm:
            artist_clean = tokens[0]
            artist_norm = first_norm
            tokens = tokens[1:]

    # Remove repeated artist-like stutters and duplicated leading segments.
    while len(tokens) >= 2:
        first_norm = _norm(tokens[0])
        second_norm = _norm(tokens[1])
        first_matches_artist = bool(
            first_norm
            and artist_norm
            and (
                first_norm == artist_norm
                or first_norm in artist_norm
                or artist_norm in first_norm
            )
        )
        if first_matches_artist:
            if noisy_artist:
                artist_clean = tokens[0]
                artist_norm = _norm(artist_clean)
            tokens = tokens[1:]
            continue
        if first_norm and second_norm and first_norm == second_norm:
            tokens = tokens[1:]
            continue
        break
    title = _strip_lyrics_suffix(" - ".join(tokens).strip() or title) or title

    if artist_clean:
        artist = artist_clean

    return artist, (title or str(title).strip())


def _is_metadata_like_preview_line(text: str, *, artist: str, title: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True

    def _norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

    preview_norm = _norm(raw)
    if not preview_norm:
        return True

    combos = {
        _norm(title),
        _norm(artist),
        _norm(f"{artist} {title}"),
        _norm(f"{title} {artist}"),
    }
    combos = {c for c in combos if c}
    for token in combos:
        if preview_norm == token:
            return True
        if preview_norm.startswith(token) or token.startswith(preview_norm):
            return True

    lower = raw.lower()
    if "with lyrics" in lower or "lyrics video" in lower:
        return True
    if re.search(r"\b(topic|vevo|official|channel)\b", lower):
        return True
    return False


def _estimate_mute_duration_from_timings(timings, *, min_secs: float = 8.0, pad_secs: float = MUTE_PREVIEW_PAD_SECS) -> float:
    try:
        points = [float(row[0]) for row in (timings or []) if len(row) >= 1]
    except Exception:
        points = []
    if not points:
        return float(min_secs)
    last_ts = max(0.0, max(points))
    return max(float(min_secs), last_ts + max(0.5, float(pad_secs)))


def read_timings(slug: str):
    timing_path = TIMINGS_DIR / f"{slug}.csv"
    if not timing_path.exists():
        print(f"Timing CSV not found for slug={slug}: {timing_path}")
        sys.exit(1)

    rows = []
    with timing_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if header and "time_secs" in header:
            try:
                idx_time = header.index("time_secs")
            except ValueError:
                idx_time = 1
            try:
                idx_li = header.index("line_index")
            except ValueError:
                idx_li = None
            idx_text = header.index("text") if "text" in header else None

            for row in reader:
                if not row or len(row) <= idx_time:
                    continue
                t_str = row[idx_time].strip()
                if not t_str:
                    continue
                try:
                    t = float(t_str)
                except ValueError:
                    continue

                if idx_li is not None and len(row) > idx_li:
                    try:
                        line_index = int(row[idx_li])
                    except ValueError:
                        line_index = 0
                else:
                    line_index = 0

                text = ""
                if idx_text is not None and len(row) > idx_text:
                    text = row[idx_text]

                rows.append((t, text, line_index))
        else:
            for row in reader:
                if len(row) < 2:
                    continue
                t_str = row[0].strip()
                if not t_str:
                    continue
                try:
                    t = float(t_str)
                except ValueError:
                    continue
                text = row[1]
                rows.append((t, text, 0))

    rows.sort(key=lambda x: x[0])
    log("TIMINGS", f"Loaded {len(rows)} timing rows from {timing_path}", CYAN)
    return rows


def resolve_ffprobe_bin(ffmpeg_bin: str) -> str:
    env = os.environ.get("KARAOKE_FFPROBE_BIN") or os.environ.get("FFPROBE_BIN")
    if env:
        return env
    try:
        p = Path(ffmpeg_bin)
        if p.is_file():
            cand = p.with_name("ffprobe")
            if cand.exists():
                return str(cand)
    except Exception:
        pass
    return "ffprobe"


def _scaled_bitrate(value: str, factor: float, *, min_kbps: int = 32) -> str:
    raw = (value or "").strip()
    m = re.match(r"^(\d+(?:\.\d+)?)([kKmM]?)$", raw)
    if not m:
        return raw

    amount = float(m.group(1))
    suffix = (m.group(2) or "").lower()

    if suffix == "m":
        kbps = max(int(min_kbps), int(round(amount * 1000.0 * factor)))
        return f"{kbps}k"
    if suffix == "k":
        kbps = max(int(min_kbps), int(round(amount * factor)))
        return f"{kbps}k"

    scaled = max(1, int(round(amount * factor)))
    return str(scaled)


def _remove_flag_and_value(cmd: list[str], flag: str) -> None:
    while True:
        try:
            idx = cmd.index(flag)
        except ValueError:
            return
        del cmd[idx:min(len(cmd), idx + 2)]


def _probe_media_duration_secs(media_path: Path, ffprobe_bin: str) -> tuple[float | None, str]:
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(media_path),
    ]
    rc, out = run_cmd_capture(cmd, tag="FFPROBE")
    if rc != 0:
        return None, f"ffprobe failed rc={rc}"
    try:
        dur = float((out or "").strip())
    except Exception:
        return None, "ffprobe returned non-numeric duration"
    if dur <= 0:
        return None, f"invalid duration {dur}"
    return dur, ""


def _truthy_env(name: str, default: str = "0") -> bool:
    raw = (os.environ.get(name, default) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _ffprobe_usable(ffprobe_bin: str) -> bool:
    # Absolute/relative paths should exist; plain executable names should resolve via PATH.
    if "/" in ffprobe_bin:
        return Path(ffprobe_bin).exists()
    return shutil.which(ffprobe_bin) is not None


def _validate_render_output(
    out_mp4: Path,
    ffprobe_bin: str,
    *,
    expected_audio_duration: float,
    require_ffprobe: bool = False,
) -> tuple[bool, str]:
    if not out_mp4.exists():
        return False, f"output missing: {out_mp4}"

    min_bytes_raw = os.environ.get("KARAOKE_MIN_MP4_BYTES", "32768").strip() or "32768"
    min_duration_raw = os.environ.get("KARAOKE_MIN_MP4_DURATION_SEC", "0.5").strip() or "0.5"
    try:
        min_bytes = max(1024, int(min_bytes_raw))
    except Exception:
        min_bytes = 32768
    try:
        min_duration = max(0.1, float(min_duration_raw))
    except Exception:
        min_duration = 0.5

    try:
        size_bytes = int(out_mp4.stat().st_size)
    except Exception as e:
        return False, f"unable to stat output: {e}"
    if size_bytes < min_bytes:
        return False, f"output too small ({size_bytes} bytes < {min_bytes} bytes)"

    if not _should_validate_duration_probe():
        return True, ""

    rendered_dur, err = _probe_media_duration_secs(out_mp4, ffprobe_bin)
    if rendered_dur is None:
        if not require_ffprobe:
            return True, f"Validation skipped: {err}"
        return False, err
    if rendered_dur < min_duration:
        return False, f"duration too short ({rendered_dur:.3f}s < {min_duration:.3f}s)"

    if expected_audio_duration >= 10.0:
        min_ratio = float(os.environ.get("KARAOKE_MIN_DURATION_RATIO", "0.50") or "0.50")
        if rendered_dur < expected_audio_duration * min_ratio:
            return (
                False,
                f"duration too short vs audio ({rendered_dur:.3f}s < {expected_audio_duration * min_ratio:.3f}s)",
            )

    return True, ""


def _file_fingerprint(p: Path) -> dict:
    try:
        st = p.stat()
        return {"path": str(p), "exists": True, "size": int(st.st_size), "mtime": float(st.st_mtime)}
    except Exception:
        return {"path": str(p), "exists": False}


def _small_file_content_fingerprint(p: Path) -> dict:
    """
    Content-based fingerprint for small control files (timings/meta),
    stable across rewrites where bytes are unchanged.
    """
    try:
        raw = p.read_bytes()
        return {
            "path": str(p),
            "exists": True,
            "size": int(len(raw)),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
    except Exception:
        return {"path": str(p), "exists": False}


def _stable_hash_obj(obj) -> str:
    b = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _try_load_json(p: Path):
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def _write_json_atomic(p: Path, data: dict) -> None:
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _duration_cache_path(slug: str) -> Path:
    return OUTPUT_DIR / f"{slug}.duration.cache.json"


def _ass_cache_path(slug: str) -> Path:
    return OUTPUT_DIR / f"{slug}.ass.cache.json"


def probe_audio_duration(path: Path, *, slug: str) -> float:
    t0 = now_perf_ms()
    if not path.exists():
        _log_step4_timing("probe_audio_duration.missing", t0)
        return 0.0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cache_p = _duration_cache_path(slug)
    fp = _file_fingerprint(path)
    cached = _try_load_json(cache_p) or {}

    if cached.get("fingerprint") == fp and isinstance(cached.get("duration_secs"), (int, float)):
        _log_step4_timing("probe_audio_duration.cache_hit", t0)
        return float(cached["duration_secs"])

    ffmpeg_bin = resolve_ffmpeg_bin()
    ffprobe_bin = resolve_ffprobe_bin(ffmpeg_bin)
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    log("FFPROBE", f"Probing duration of {path}", BLUE)
    try:
        rc, out = run_cmd_capture(cmd, tag="FFPROBE")
        if rc != 0:
            raise RuntimeError(f"ffprobe failed rc={rc}")
        dur = float(out.strip())
        _write_json_atomic(cache_p, {"fingerprint": fp, "duration_secs": dur})
        _log_step4_timing("probe_audio_duration.ffprobe", t0)
        return dur
    except Exception as e:
        log("FFPROBE", f"Failed to probe duration: {e}", YELLOW)
        _log_step4_timing("probe_audio_duration.ffprobe_failed", t0)
        return 0.0


def _title_card_connector_for_timings(timings) -> str:
    spanish_markers = {
        "de", "que", "con", "para", "una", "uno", "esta", "estoy", "tengo", "quiero",
        "eres", "como", "porque", "cuando", "donde", "nunca", "siempre", "amor", "vida",
    }
    english_markers = {
        "the", "and", "you", "your", "with", "for", "this", "that", "love", "never",
        "always", "when", "where", "what", "why", "how", "is", "are", "was", "were",
    }
    joined = " ".join(str(row[1] if len(row) >= 2 else "") for row in (timings or [])[:120]).lower()
    tokens = re.findall(r"[a-zA-Z\u00C0-\u017F']+", joined)
    if not tokens:
        return "by"
    es = sum(1 for t in tokens if t in spanish_markers)
    en = sum(1 for t in tokens if t in english_markers)
    return "de" if es >= 2 and es >= (en + 1) else "by"


def _title_card_credit_text_for_connector(connector_word: str) -> str:
    connector = str(connector_word or "").strip().lower()
    if connector == "de":
        return TITLE_CARD_CREDIT_TEXT_ES
    return TITLE_CARD_CREDIT_TEXT_EN


def compute_default_title_card_lines(slug: str, artist: str, title: str, connector_word: str = "by") -> list[str]:
    pretty_slug = slug.replace("_", " ").title()

    if title and artist:
        connector = "de" if str(connector_word).strip().lower() == "de" else "by"
        return [title, "", connector, "", artist]
    if title:
        return [title]
    if artist:
        return [artist]
    return [pretty_slug]


def _parse_title_card_display_lines(raw_display: str) -> list[str]:
    raw = str(raw_display or "")
    if not raw:
        return []
    expanded = raw.replace("\\n", "\n")
    lines = [line.strip() for line in expanded.splitlines()]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return lines


def prompt_title_card_lines(
    slug: str,
    artist: str,
    title: str,
    connector_word: str = "by",
    title_card_display: str = "",
) -> list[str]:
    custom_lines = _parse_title_card_display_lines(title_card_display)
    default_lines = custom_lines or compute_default_title_card_lines(slug, artist, title, connector_word=connector_word)

    if not sys.stdin.isatty():
        if custom_lines:
            log("TITLE", "Non-interactive mode; using --title-card-display override.", CYAN)
        else:
            log("TITLE", "Non-interactive mode; using default title card.", CYAN)
        return default_lines

    print()
    print(f"{CYAN}Title Card Preview (before lyrics):{RESET}")
    print("  Default card would say:\n")
    for line in default_lines:
        print(f"    {line}")
    print()
    print("Options:")
    print("  1) Use default")
    print("  2) Edit title card text manually (blank = pure black screen)")
    print()

    return default_lines


def build_ass(
    slug: str,
    artist: str,
    title: str,
    timings,
    audio_duration: float,
    font_name: str,
    font_size_script: int,
    title_card_lines: list[str] | None = None,
    title_card_font_percent: float = 100.0,
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ass_path = OUTPUT_DIR / f"{slug}.ass"

    if audio_duration <= 0.0:
        if timings:
            audio_duration = max(t for t, _, _ in timings) + 5
        else:
            audio_duration = 5.0

    playresx = VIDEO_WIDTH
    playresy = VIDEO_HEIGHT

    scale_y = playresy / 720.0

    next_top_margin_px = int(round(NEXT_LYRIC_TOP_MARGIN_PX * scale_y))
    next_bottom_margin_px = int(round(NEXT_LYRIC_BOTTOM_MARGIN_PX * scale_y))
    next_label_top_margin_px = int(round(NEXT_LABEL_TOP_MARGIN_PX * scale_y))

    top_band_height = int(playresy * TOP_BAND_FRACTION)
    y_divider_nominal = top_band_height
    bottom_band_height = playresy - y_divider_nominal
    center_top = top_band_height // 2
    offset_px = int(top_band_height * VERTICAL_OFFSET_FRACTION)
    y_main_top = center_top + offset_px
    y_center_full = playresy // 2

    inner_bottom = max(1, bottom_band_height - next_top_margin_px - next_bottom_margin_px)
    y_next = y_divider_nominal + next_top_margin_px + inner_bottom // 2
    line_y = max(0, y_divider_nominal - DIVIDER_LINE_OFFSET_UP_PX)

    preview_font = max(1, int(font_size_script * NEXT_LINE_FONT_SCALE))
    next_label_font = max(1, int(font_size_script * NEXT_LABEL_FONT_SCALE))
    credit_font = max(16, int(round(font_size_script * TITLE_CARD_CREDIT_FONT_SCALE)))
    title_card_percent = max(10.0, min(200.0, float(title_card_font_percent or 100.0)))
    title_card_font_size = max(8, int(round(font_size_script * (title_card_percent / 100.0))))
    credit_margin_x = max(0, int(round(TITLE_CARD_CREDIT_MARGIN_X_PX * scale_y)))
    credit_margin_y = max(12, int(round(TITLE_CARD_CREDIT_MARGIN_Y_PX * scale_y)))
    credit_x = min(max(1, (playresx // 2) + credit_margin_x), playresx - 1)
    credit_y = max(1, playresy - credit_margin_y)

    top_primary_ass = f"&H{TOP_LYRIC_TEXT_ALPHA_HEX}{rgb_to_bgr(TOP_LYRIC_TEXT_COLOR_RGB)}"
    secondary_ass = "&H000000FF"
    outline_ass = "&H00000000"
    back_ass = f"&H{TOP_BOX_BG_ALPHA_HEX}{rgb_to_bgr(TOP_BOX_BG_COLOR_RGB)}"

    outline_px = max(1, int(round(2.0 * scale_y)))  # smaller outline => cheaper
    shadow_px = 0

    def ass_escape(text: str):
        return text.replace("{", "(").replace("}", ")").replace("\n", r"\N")

    # Use StringIO for efficient string building (30-40% faster than list + join)
    output = StringIO()
    output.write("[Script Info]\n")
    output.write("ScriptType: v4.00+\n")
    output.write(f"PlayResX: {playresx}\n")
    output.write(f"PlayResY: {playresy}\n")
    output.write("\n")
    output.write("[V4+ Styles]\n")
    output.write(
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
    )
    output.write(
        f"Style: Default,{font_name},{font_size_script},"
        f"{top_primary_ass},{secondary_ass},{outline_ass},{back_ass},"
        f"0,0,0,0,100,100,0,0,1,{outline_px},{shadow_px},5,50,50,0,0\n"
    )
    output.write("\n")
    output.write("[Events]\n")
    output.write("Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")

    unified = []
    for t, raw, idx in timings:
        if 0 <= t <= audio_duration:
            raw = (raw or "")
            unified.append((t, raw.strip(), idx))
    unified.sort(key=lambda x: x[0])

    offset = clamp_offset_secs(LYRICS_OFFSET_SECS)
    non_blank = [u for u in unified if (u[1] or "").strip()]

    if title_card_lines:
        title_lines = title_card_lines
    else:
        title_lines = compute_default_title_card_lines(slug, artist, title)

    intro_text = ass_escape("\\N".join(title_lines))
    credit_connector = _title_card_connector_for_timings(timings)
    credit_text = ass_escape(_title_card_credit_text_for_connector(credit_connector))

    if len(non_blank) == 0:
        output.write(
            "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(0.0),
                seconds_to_ass_time(audio_duration),
                f"{{\\an5\\pos({playresx//2},{y_center_full})\\fs{title_card_font_size}}}{intro_text}",
            )
        )
        if credit_text:
            output.write(
                "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                    seconds_to_ass_time(0.0),
                    seconds_to_ass_time(audio_duration),
                    f"{{\\an2\\pos({credit_x},{credit_y})\\fs{credit_font}}}{credit_text}",
                )
            )
        ass_path.write_text(output.getvalue(), encoding="utf-8")
        return ass_path

    first_lyric_start = max(0.0, min(audio_duration, non_blank[0][0] + offset))
    title_end = max(0.0, min(audio_duration, min(5.0, first_lyric_start)))
    intro_overlay_end = max(0.0, min(audio_duration, max(title_end, first_lyric_start)))

    if title_end > 0.0:
        output.write(
            "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(0.0),
                seconds_to_ass_time(title_end),
                f"{{\\an5\\pos({playresx//2},{y_center_full})\\fs{title_card_font_size}}}{intro_text}",
            )
        )
        if credit_text:
            output.write(
                "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                    seconds_to_ass_time(0.0),
                    seconds_to_ass_time(title_end),
                    f"{{\\an2\\pos({credit_x},{credit_y})\\fs{credit_font}}}{credit_text}",
                )
            )

    fade_tag_main = ""
    if FADE_IN_MS > 0 or FADE_OUT_MS > 0:
        fade_tag_main = f"\\fad({FADE_IN_MS},{FADE_OUT_MS})"

    left = float(DIVIDER_LEFT_MARGIN_PX)
    right = float(playresx - DIVIDER_RIGHT_MARGIN_PX)
    divider_height = max(0.5, float(DIVIDER_HEIGHT_PX))
    next_color_bgr = rgb_to_bgr(GLOBAL_NEXT_COLOR_RGB)
    divider_color_bgr = rgb_to_bgr(DIVIDER_COLOR_RGB)
    next_label_color_bgr = rgb_to_bgr(NEXT_LABEL_COLOR_RGB)

    n = len(unified)
    next_preview_after: list[str] = [""] * n
    next_candidate = ""
    for i in range(n - 1, -1, -1):
        next_preview_after[i] = next_candidate
        cand = (unified[i][1] or "").strip()
        if cand and (not is_music_only(cand)):
            next_candidate = cand

    # Title card remains clean: black background + vertically centered title text only.

    for i, (t, raw, _) in enumerate(unified):
        start = max(0.0, t + offset)
        end = unified[i + 1][0] + offset if i < n - 1 else audio_duration
        clipped = _clip_window_to_duration(start, end, audio_duration)
        if not clipped:
            continue
        start, end = clipped

        if end <= intro_overlay_end:
            continue
        if start < intro_overlay_end:
            start = intro_overlay_end
        clipped = _clip_window_to_duration(start, end, audio_duration)
        if not clipped:
            continue
        start, end = clipped

        text_stripped = raw.strip()
        music_only = is_music_only(text_stripped)

        # Main lyric line (can be blank or [music])
        y_line = y_center_full if music_only else y_main_top
        output.write(
            "Dialogue: 1,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(start),
                seconds_to_ass_time(end),
                f"{{\\an5\\pos({playresx//2},{y_line}){fade_tag_main}}}{ass_escape(text_stripped)}",
            )
        )

        # Persistent Next/preview: show the next *lyric* line even during [music] or blank rows
        next_text = next_preview_after[i]
        if not next_text:
            continue

        divider = (
            f"{{\\an7\\pos(0,{line_y})"
            f"\\1c&H{divider_color_bgr}&"
            f"\\1a&H{DIVIDER_ALPHA_HEX}&"
            f"\\bord0\\shad0\\p1}}"
            f"m {left} 0 l {right} 0 l {right} {divider_height} l {left} {divider_height}{{\\p0}}"
        )
        output.write(
            "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(start),
                seconds_to_ass_time(end),
                divider,
            )
        )

        next_label_left = int(round(left + next_label_top_margin_px))
        output.write(
            "Dialogue: 0,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(start),
                seconds_to_ass_time(end),
                (
                    f"{{\\an7\\pos({next_label_left},{line_y + next_label_top_margin_px})"
                    f"\\fs{next_label_font}"
                    f"\\1c&H{next_label_color_bgr}&"
                    f"\\1a&H{NEXT_LABEL_ALPHA_HEX}&"
                    f"\\bord0\\shad0}}Next:"
                ),
            )
        )

        preview = (
            f"{{\\an5\\pos({playresx//2},{y_next})"
            f"\\fs{preview_font}"
            f"\\1c&H{next_color_bgr}&"
            f"\\1a&H{GLOBAL_NEXT_ALPHA_HEX}&"
            f"\\bord0\\shad0"
            f"{fade_tag_main}}}{ass_escape(next_text)}"
        )
        output.write(
            "Dialogue: 2,{},{},Default,,0,0,0,,{}\n".format(
                seconds_to_ass_time(start),
                seconds_to_ass_time(end),
                preview,
            )
        )

    ass_path.write_text(output.getvalue(), encoding="utf-8")
    return ass_path


def _escape_drawtext_text(s: str) -> str:
    s = (s or "")
    s = s.replace("\\", "\\\\")
    s = s.replace("'", "\\'")
    s = s.replace(":", "\\:")
    s = s.replace("%", "\\%")
    return s


def build_drawtext_vf(
    timings: list[tuple[float, str, int]],
    *,
    audio_duration: float,
    font: str,
    fontsize: int,
    offset_secs: float,
) -> str:
    audio_duration = max(0.0, float(audio_duration))
    offset_secs = clamp_offset_secs(offset_secs)
    filters: list[str] = []
    for i, (t, txt, li) in enumerate(timings):
        start = float(t) + float(offset_secs)
        end = float(audio_duration)
        if i + 1 < len(timings):
            end = float(timings[i + 1][0]) + float(offset_secs)

        if end <= start:
            end = start + 0.50
        clipped = _clip_window_to_duration(start, end, audio_duration)
        if not clipped:
            continue
        start, end = clipped

        safe_txt = _escape_drawtext_text((txt or "").strip())
        y_expr = "(h-text_h)/2"
        if is_music_only(txt):
            y_expr = "h*0.70"

        filters.append(
            "drawtext="
            f"font='{_escape_drawtext_text(font)}':"
            f"text='{safe_txt}':"
            f"fontsize={int(fontsize)}:"
            "fontcolor=white:"
            "shadowcolor=black:shadowx=2:shadowy=2:"
            "x=(w-text_w)/2:"
            f"y={y_expr}:"
            f"enable='between(t,{start:.3f},{end:.3f})'"
        )
    return ",".join(filters)


def _peek_first_time_secs(csv_path: Path):
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, None)
            for row in r:
                if not row:
                    continue
                if len(row) >= 2 and row[0].strip().lstrip("-").isdigit():
                    try:
                        return float(row[1])
                    except Exception:
                        pass
                try:
                    return float(row[0])
                except Exception:
                    continue
    except Exception:
        return None
    return None


def _resolve_step1_audio(slug: str) -> Path | None:
    step1_meta = META_DIR / f"{slug}.step1.json"
    candidates: list[Path] = []

    if step1_meta.exists():
        try:
            payload = json.loads(step1_meta.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        for key in ("audio_path", "mp3"):
            raw = str((payload or {}).get(key) or "").strip()
            if not raw:
                continue
            p = Path(raw)
            if not p.is_absolute():
                p = (BASE_DIR / p).resolve()
            candidates.append(p)

    # Common step1 output names (prefer mp3 alias first).
    candidates.extend(
        [
            MP3_DIR / f"{slug}.mp3",
            MP3_DIR / f"{slug}.m4a",
            MP3_DIR / f"{slug}.webm",
            MP3_DIR / f"{slug}.opus",
            MP3_DIR / f"{slug}.ogg",
            MP3_DIR / f"{slug}.wav",
            MP3_DIR / f"{slug}.aac",
        ]
    )

    # Last-chance wildcard lookup for uncommon extensions.
    for p in MP3_DIR.glob(f"{slug}.*"):
        candidates.append(p)

    seen: set[str] = set()
    for p in candidates:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        try:
            if p.exists() and p.is_file() and p.stat().st_size > 0:
                return p
        except Exception:
            continue
    return None


def choose_audio(slug: str, *, prefer_step1_audio: bool = False) -> Path:
    mix_m4a = MIXES_DIR / f"{slug}.m4a"
    mix_wav = MIXES_DIR / f"{slug}.wav"
    mix_mp3 = MIXES_DIR / f"{slug}.mp3"

    if prefer_step1_audio:
        step1_audio = _resolve_step1_audio(slug)
        if step1_audio is not None:
            print(f"[AUDIO] Using step1 source audio (preferred): {step1_audio}")
            return step1_audio

    prepared_render_audio: Optional[Path] = None
    wait_secs = _prepared_render_audio_wait_secs()
    if wait_secs > 0.0 or mix_m4a.exists():
        try:
            from .step2_split import wait_for_prepared_render_audio

            prepared_render_audio = wait_for_prepared_render_audio(
                MIXES_DIR,
                slug,
                wait_timeout_sec=wait_secs,
            )
        except Exception:
            prepared_render_audio = None
    if prepared_render_audio is not None:
        print(f"[AUDIO] Using prepared render audio: {prepared_render_audio}")
        return prepared_render_audio

    # Turbo profile favors lower I/O latency by preferring MP3 over WAV.
    if _prefer_mp3_audio_for_render():
        if mix_mp3.exists():
            print(f"[AUDIO] Using mixed MP3: {mix_mp3}")
            return mix_mp3
        if mix_wav.exists():
            print(f"[AUDIO] Using mixed WAV: {mix_wav}")
            return mix_wav
    else:
        if mix_wav.exists():
            print(f"[AUDIO] Using mixed WAV: {mix_wav}")
            return mix_wav
        if mix_mp3.exists():
            print(f"[AUDIO] Using mixed MP3: {mix_mp3}")
            return mix_mp3

    step1_audio = _resolve_step1_audio(slug)
    if step1_audio is not None:
        print(f"[AUDIO] Using step1 source audio: {step1_audio}")
        return step1_audio

    print(
        f"\n[AUDIO-ERROR] No mixed audio found for slug={slug}.\n"
        f"Expected one of:\n"
        f"   {mix_m4a}\n"
        f"   {mix_wav}\n"
        f"   {mix_mp3}\n\n"
        f"Run step2_split.py to generate the mix, or ensure step1 artifacts exist.\n"
    )
    sys.exit(1)


def _probe_primary_audio_stream(path: Path, ffprobe_bin: str) -> dict[str, str]:
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_name,profile,codec_type",
        "-of",
        "json",
        str(path),
    ]
    rc, out = run_cmd_capture(cmd, tag="FFPROBE")
    if rc != 0:
        return {}
    try:
        payload = json.loads(out or "{}")
    except Exception:
        return {}
    streams = payload.get("streams")
    if not isinstance(streams, list):
        return {}
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        if str(stream.get("codec_type") or "").lower() != "audio":
            continue
        return {
            "codec_name": str(stream.get("codec_name") or "").strip(),
            "profile": str(stream.get("profile") or "").strip(),
        }
    return {}


def _audio_stream_copy_supported(path: Path, *, ffprobe_bin: str | None = None) -> bool:
    exts: set[str] = set()
    ext = str(path.suffix or "").lower().strip()
    if ext:
        exts.add(ext)
    try:
        resolved = path.resolve()
        resolved_ext = str(resolved.suffix or "").lower().strip()
        if resolved_ext:
            exts.add(resolved_ext)
    except Exception:
        pass
    # Keep stream-copy only for audio/container combos that are broadly compatible
    # with MP4 playback in QuickTime/iOS. MP3-in-MP4 can be flaky across players.
    if not any(e in {".m4a", ".aac", ".mp4"} for e in exts):
        return False
    if not ffprobe_bin:
        return True

    audio_stream = _probe_primary_audio_stream(path, ffprobe_bin)
    codec_name = str(audio_stream.get("codec_name") or "").strip().lower()
    profile = str(audio_stream.get("profile") or "").strip().lower()
    if codec_name != "aac":
        return False
    if not profile:
        return True
    return profile in {"lc", "aac lc", "low complexity"}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Generate karaoke MP4 from slug.")
    p.add_argument("--slug", required=False, help="Song slug, e.g. californication")
    p.add_argument("slug_pos", nargs="?", help="Song slug (positional), e.g. californication")
    p.add_argument("--font-size", type=int, help="Subtitle font size (20–200). Default 120.")
    p.add_argument("--font-name", type=str, default="Helvetica", help="Subtitle font name. Default Helvetica.")
    p.add_argument("--offset", type=str, default="0.0", help="Offset in seconds")
    p.add_argument(
        "--title-card-display",
        type=str,
        default="",
        help="Optional title-card text override. Use literal \\n for line breaks.",
    )
    p.add_argument(
        "--font-size-percent",
        type=float,
        default=100.0,
        help="Title-card text size percent (intro card only; default 100).",
    )
    p.add_argument("--out", type=str, default="", help="Output mp4 path (default output/<slug>.mp4)")
    p.add_argument("--mute", action="store_true", help="Render video-only output with no audio track")
    p.add_argument(
        "--prefer-step1-audio",
        action="store_true",
        help="Prefer step1 source audio over mixes for fastest no-step2 render path.",
    )
    return p.parse_args(argv)


def _ass_inputs_digest(
    slug: str,
    *,
    timings_csv: Path,
    meta1: Path,
    meta2: Path,
    audio_fp: dict,
    ass_font_size: int,
    font_name: str,
    offset_secs: float,
    title_card_display: str = "",
    title_card_font_percent: float = 100.0,
) -> str:
    obj = {
        "slug": slug,
        "video": {"w": VIDEO_WIDTH, "h": VIDEO_HEIGHT, "fps": FPS},
        "layout": {
            "BOTTOM_BOX_HEIGHT_FRACTION": BOTTOM_BOX_HEIGHT_FRACTION,
            "NEXT_LYRIC_TOP_MARGIN_PX": NEXT_LYRIC_TOP_MARGIN_PX,
            "NEXT_LYRIC_BOTTOM_MARGIN_PX": NEXT_LYRIC_BOTTOM_MARGIN_PX,
            "DIVIDER_LINE_OFFSET_UP_PX": DIVIDER_LINE_OFFSET_UP_PX,
            "DIVIDER_HEIGHT_PX": DIVIDER_HEIGHT_PX,
            "VERTICAL_OFFSET_FRACTION": VERTICAL_OFFSET_FRACTION,
            "NEXT_LINE_FONT_SCALE": NEXT_LINE_FONT_SCALE,
            "NEXT_LABEL_FONT_SCALE": NEXT_LABEL_FONT_SCALE,
            "FADE_IN_MS": FADE_IN_MS,
            "FADE_OUT_MS": FADE_OUT_MS,
        },
        "colors": {
            "GLOBAL_NEXT_COLOR_RGB": GLOBAL_NEXT_COLOR_RGB,
            "GLOBAL_NEXT_ALPHA_HEX": GLOBAL_NEXT_ALPHA_HEX,
            "DIVIDER_COLOR_RGB": DIVIDER_COLOR_RGB,
            "DIVIDER_ALPHA_HEX": DIVIDER_ALPHA_HEX,
            "TOP_LYRIC_TEXT_COLOR_RGB": TOP_LYRIC_TEXT_COLOR_RGB,
            "TOP_LYRIC_TEXT_ALPHA_HEX": TOP_LYRIC_TEXT_ALPHA_HEX,
            "TOP_BOX_BG_COLOR_RGB": TOP_BOX_BG_COLOR_RGB,
            "TOP_BOX_BG_ALPHA_HEX": TOP_BOX_BG_ALPHA_HEX,
        },
        "font": {"name": font_name, "ass_size": ass_font_size},
        "offset_secs": float(offset_secs),
        "title_card_display": str(title_card_display or ""),
        "title_card_font_percent": float(title_card_font_percent),
        "timings": _small_file_content_fingerprint(timings_csv),
        "meta": [_small_file_content_fingerprint(meta1), _small_file_content_fingerprint(meta2)],
        "audio": audio_fp,
    }
    return _stable_hash_obj(obj)


def _render_cache_path(slug: str, out_mp4: Path) -> Path:
    if out_mp4.name.endswith(".preview.mp4"):
        return OUTPUT_DIR / f"{slug}.render.preview.cache.json"
    return OUTPUT_DIR / f"{slug}.render.cache.json"


def _render_inputs_digest(
    *,
    slug: str,
    out_mp4: Path,
    ass_path: Path,
    audio_fp: dict,
    audio_copy_mode: bool,
    mute_render: bool,
    encoder: str,
    ffmpeg_bin: str,
    vf: str,
) -> str:
    obj = {
        "slug": str(slug),
        "out_mp4": str(out_mp4),
        "video": {
            "width": int(VIDEO_WIDTH),
            "height": int(VIDEO_HEIGHT),
            "fps": int(FPS),
            "bitrate": str(VIDEO_BITRATE),
            "gop_seconds": float(VIDEO_GOP_SECONDS),
            "encoder": str(encoder),
            "x264_preset": str(X264_PRESET or ""),
            "x264_tune": str(X264_TUNE or ""),
        },
        "audio": {
            "mute": bool(mute_render),
            "copy_mode": bool(audio_copy_mode),
            "bitrate": str(AUDIO_BITRATE),
            "fingerprint": dict(audio_fp or {}),
        },
        "movflags_faststart": bool(not DISABLE_FASTSTART),
        "ffmpeg_bin": str(ffmpeg_bin),
        "filter_graph_sha256": _stable_hash_obj({"vf": str(vf)}),
        "ass": _file_fingerprint(ass_path),
    }
    return _stable_hash_obj(obj)


def _offset_meta_path(slug: str) -> Path:
    return META_DIR / f"{slug}.step4.offsets.json"


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])

    if not getattr(args, "slug", None):
        args.slug = getattr(args, "slug_pos", None)
    if not args.slug:
        raise SystemExit("Missing slug (pass --slug <slug> or positional <slug>)")

    step4_t0 = now_perf_ms()
    global LYRICS_OFFSET_SECS
    try:
        cli_offset = float(args.offset) if args.offset is not None else 0.0
        pre_shift_detected = False
        pre_shift_delta: float | None = None

        offset_t0 = now_perf_ms()
        auto_off = 0.0
        manual_off = 0.0
        try:
            p_auto = (TIMINGS_DIR / f"{args.slug}.offset.auto")
            if p_auto.exists():
                auto_off = float(p_auto.read_text(encoding="utf-8").strip() or "0.0")
        except Exception:
            auto_off = 0.0
        try:
            p_man = (TIMINGS_DIR / f"{args.slug}.offset")
            if p_man.exists():
                manual_off = float(p_man.read_text(encoding="utf-8").strip() or "0.0")
        except Exception:
            manual_off = 0.0

        # Determine base offset with clear precedence: manual > auto > 0
        # Manual offset completely replaces auto-offset (no adding)
        base_offset = 0.0
        offset_source = "none"

        if abs(manual_off) > 1e-6:
            base_offset = manual_off
            offset_source = "manual"
            if abs(auto_off) > 1e-6:
                log("OFFSET", f"Using manual offset {manual_off:+.3f}s (whisper auto {auto_off:+.3f}s ignored)", CYAN)
            else:
                log("OFFSET", f"Using manual offset {manual_off:+.3f}s", CYAN)
        elif abs(auto_off) > 1e-6:
            base_offset = auto_off
            offset_source = "auto"
            log("OFFSET", f"Using whisper auto-offset {auto_off:+.3f}s", CYAN)
        else:
            log("OFFSET", "No offset applied (manual=0, auto=0)", CYAN)

        try:
            raw_csv = (TIMINGS_DIR / f"{args.slug}.raw.csv")
            final_csv = (TIMINGS_DIR / f"{args.slug}.csv")
            if raw_csv.exists() and final_csv.exists() and abs(base_offset) > 1e-6:
                tr = _peek_first_time_secs(raw_csv)
                tf = _peek_first_time_secs(final_csv)
                if tr is not None and tf is not None:
                    delta_csv = float(tf) - float(tr)
                    pre_shift_delta = float(delta_csv)
                    if abs(delta_csv - base_offset) < 0.25:
                        log("OFFSET", "Detected pre-shifted timings CSV; ignoring offset for rendering", YELLOW)
                        base_offset = 0.0
                        pre_shift_detected = True
        except Exception:
            pass
        _log_step4_timing("offset_resolution", offset_t0)

        setup_t0 = now_perf_ms()
        resolved_offset = base_offset + cli_offset
        clamped_offset = clamp_offset_secs(resolved_offset)
        if abs(clamped_offset - resolved_offset) > 1e-6:
            log(
                "OFFSET",
                (
                    f"Offset {resolved_offset:+.3f}s exceeded range "
                    f"[{OFFSET_SECS_MIN:+.0f}, {OFFSET_SECS_MAX:+.0f}]s; clamped to {clamped_offset:+.3f}s"
                ),
                YELLOW,
            )
        LYRICS_OFFSET_SECS = clamped_offset
        slug = slugify(args.slug)
        try:
            _write_json_atomic(
                _offset_meta_path(slug),
                {
                    "slug": slug,
                    "offset_source": offset_source,
                    "manual_offset_s": float(manual_off),
                    "auto_offset_s": float(auto_off),
                    "cli_offset_s": float(cli_offset),
                    "base_offset_s": float(base_offset),
                    "resolved_offset_s": float(resolved_offset),
                    "clamped_offset_s": float(clamped_offset),
                    "final_applied_offset_s": float(LYRICS_OFFSET_SECS),
                    "pre_shift_detected": bool(pre_shift_detected),
                    "pre_shift_delta_s": (float(pre_shift_delta) if pre_shift_delta is not None else None),
                    "offset_min_s": float(OFFSET_SECS_MIN),
                    "offset_max_s": float(OFFSET_SECS_MAX),
                },
            )
        except Exception:
            pass

        font_size_value = args.font_size
        if font_size_value is None:
            font_size_value = DEFAULT_UI_FONT_SIZE
        ui_font_size = max(8, min(400, int(round(font_size_value))))
        ass_font_size = int(ui_font_size * ASS_FONT_MULTIPLIER)

        log("FONT", f"Using UI font size {ui_font_size} (ASS Fontsize={ass_font_size})", CYAN)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        log("MP4GEN", f"Slug={slug}", CYAN)
        log(
            "RENDER",
            (
                f"level={RENDER_LEVEL} profile={RENDER_PROFILE_NAME or '-'} source={RENDER_LEVEL_SOURCE} "
                f"size={VIDEO_WIDTH}x{VIDEO_HEIGHT} fps={FPS} "
                f"vbr={VIDEO_BITRATE} abr={AUDIO_BITRATE}"
            ),
            CYAN,
        )
        _log_step4_timing("setup", setup_t0)

        meta_t0 = now_perf_ms()
        artist, title = read_meta(slug)
        timings = read_timings(slug)
        connector_word = _title_card_connector_for_timings(timings)
        log("META", f'Artist="{artist}", Title="{title}", entries={len(timings)}', CYAN)
        title_card_lines = prompt_title_card_lines(
            slug,
            artist,
            title,
            connector_word=connector_word,
            title_card_display=str(getattr(args, "title_card_display", "") or ""),
        )
        _log_step4_timing("meta_timings_title", meta_t0)

        audio_t0 = now_perf_ms()
        mute_render = bool(getattr(args, "mute", False))
        audio_fp = {"mode": "mute", "exists": False} if mute_render else {}
        audio_copy_mode = False
        ffmpeg_bin = ""
        ffprobe_bin = ""
        if args.out:
            out_mp4 = Path(args.out)
            if not out_mp4.is_absolute():
                out_mp4 = OUTPUT_DIR / out_mp4
        else:
            out_mp4 = OUTPUT_DIR / f"{slug}.mp4"

        if mute_render:
            audio_path = None
            audio_duration = _estimate_mute_duration_from_timings(timings)
            log("AUDIO", f"Mute render enabled; skipping audio input (duration={audio_duration:.3f}s)", YELLOW)
        else:
            audio_path = choose_audio(slug, prefer_step1_audio=bool(getattr(args, "prefer_step1_audio", False)))
            audio_fp = _file_fingerprint(audio_path)
            audio_duration = probe_audio_duration(audio_path, slug=slug)
            ffmpeg_bin = resolve_ffmpeg_bin()
            ffprobe_bin = resolve_ffprobe_bin(ffmpeg_bin)
            audio_copy_mode = bool(
                AUDIO_COPY_WHEN_COMPATIBLE
                and _audio_stream_copy_supported(audio_path, ffprobe_bin=ffprobe_bin)
            )
            if audio_copy_mode:
                log("AUDIO", f"Using stream-copy audio path ({audio_path.suffix.lower()}) for faster render", CYAN)
            else:
                audio_stream = _probe_primary_audio_stream(audio_path, ffprobe_bin)
                codec_name = str(audio_stream.get("codec_name") or "").strip()
                profile = str(audio_stream.get("profile") or "").strip()
                if codec_name or profile:
                    log(
                        "AUDIO",
                        (
                            f"Re-encoding audio for MP4 compatibility "
                            f"(codec={codec_name or 'unknown'}, profile={profile or 'unknown'})"
                        ),
                        YELLOW,
                    )
            if audio_duration <= 0:
                log("DUR", f"Audio duration unknown or zero for {audio_path}", YELLOW)
                audio_duration = 1.0
        _log_step4_timing("audio_selection_and_probe", audio_t0)

        # ASS caching: if inputs are unchanged, skip re-writing .ass
        ass_t0 = now_perf_ms()
        timings_csv = TIMINGS_DIR / f"{slug}.csv"
        meta1 = META_DIR / f"{slug}.step1.json"
        meta2 = META_DIR / f"{slug}.json"
        ass_cache_p = _ass_cache_path(slug)
        ass_p = OUTPUT_DIR / f"{slug}.ass"

        digest = _ass_inputs_digest(
            slug,
            timings_csv=timings_csv,
            meta1=meta1,
            meta2=meta2,
            audio_fp=audio_fp,
            ass_font_size=ass_font_size,
            font_name=args.font_name,
            offset_secs=LYRICS_OFFSET_SECS,
            title_card_display=str(getattr(args, "title_card_display", "") or ""),
            title_card_font_percent=float(getattr(args, "font_size_percent", 100.0) or 100.0),
        )
        cached = _try_load_json(ass_cache_p) or {}
        if ass_p.exists() and cached.get("digest") == digest:
            log("ASS", f"Reusing cached ASS: {ass_p}", CYAN)
            ass_path = ass_p
        else:
            ass_path = build_ass(
                slug,
                artist,
                title,
                timings,
                audio_duration,
                args.font_name,
                ass_font_size,
                title_card_lines,
                title_card_font_percent=float(getattr(args, "font_size_percent", 100.0) or 100.0),
            )
            _write_json_atomic(ass_cache_p, {"digest": digest})
        _log_step4_timing("ass_prepare", ass_t0)

        ffmpeg_setup_t0 = now_perf_ms()
        if not ffmpeg_bin:
            ffmpeg_bin = resolve_ffmpeg_bin()
        if not ffprobe_bin:
            ffprobe_bin = resolve_ffprobe_bin(ffmpeg_bin)
        has_subtitles = ffmpeg_has_filter(ffmpeg_bin, "subtitles")
        has_drawtext = False
        if not has_subtitles:
            has_drawtext = ffmpeg_has_filter(ffmpeg_bin, "drawtext")

        color_src = f"color=c=black:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:r={FPS}:d={max(audio_duration, 1.0):.3f}"

        if has_subtitles:
            ass_escaped = ffmpeg_escape_filter_path(str(ass_path))
            vf = f"subtitles=filename='{ass_escaped}'"
        elif has_drawtext:
            vf = build_drawtext_vf(
                timings,
                audio_duration=audio_duration,
                font=args.font_name,
                fontsize=ass_font_size,
                offset_secs=LYRICS_OFFSET_SECS,
            )
        else:
            raise SystemExit(
                "[FFMPEG] Missing both 'subtitles' and 'drawtext' filters in your ffmpeg build\n"
                "Install a more complete ffmpeg or point $KARAOKE_FFMPEG_BIN/$FFMPEG_BIN to one\n"
            )
        _log_step4_timing("ffmpeg_filter_detect_and_vf", ffmpeg_setup_t0)

        cmd_build_t0 = now_perf_ms()
        encoder = _resolve_video_encoder(Path(ffmpeg_bin))
        audio_encoder = _resolve_audio_encoder(Path(ffmpeg_bin))
        gop = max(1, int(round(float(FPS) * float(VIDEO_GOP_SECONDS))))

        cmd = [
            ffmpeg_bin,
            "-y",
            "-stats",
            "-loglevel",
            "info",
        ]

        if ENABLE_BENCH:
            cmd += ["-benchmark", "-benchmark_all"]

        cmd += [
            "-f",
            "lavfi",
            "-i",
            color_src,
        ]
        if not mute_render:
            cmd += [
                "-i",
                str(audio_path),
            ]
        # Always map video from the synthetic color source (input 0) so we never
        # accidentally render the source mp4 video track in turbo/cache paths.
        cmd += ["-map", "0:v:0"]
        if not mute_render:
            cmd += ["-map", "1:a:0", "-shortest"]
        cmd += [
            "-vf",
            vf,
            "-r",
            str(FPS),
            "-c:v",
            encoder,
            "-b:v",
            str(VIDEO_BITRATE),
            "-g",
            str(gop),
            "-pix_fmt",
            "yuv420p",
        ]
        if mute_render:
            cmd += ["-an"]
        else:
            if audio_copy_mode:
                cmd += ["-c:a", "copy"]
                if str(audio_path.suffix or "").lower() == ".aac":
                    cmd += ["-bsf:a", "aac_adtstoasc"]
            else:
                cmd += [
                    "-c:a",
                    audio_encoder,
                    "-b:a",
                    str(AUDIO_BITRATE),
                ]
                cmd += _audio_encoder_profile_args(audio_encoder)
        if encoder == "libx264" and X264_PRESET:
            cmd += ["-preset", X264_PRESET]
        if encoder == "libx264" and X264_TUNE:
            cmd += ["-tune", X264_TUNE]

        if not DISABLE_FASTSTART:
            cmd += ["-movflags", "+faststart"]

        cmd += [str(out_mp4)]
        _log_step4_timing("build_ffmpeg_cmd", cmd_build_t0)

        if out_mp4.name.endswith(".preview.mp4"):
            render_meta_path = META_DIR / f"{slug}.step4.preview.json"
        else:
            render_meta_path = META_DIR / f"{slug}.step4.json"
        render_started_at = _now_ts()
        render_finished_at = None
        rc = None
        last_error_detail = ""
        require_ffprobe = _truthy_env("KARAOKE_REQUIRE_FFPROBE_VALIDATION", "0")
        if not _ffprobe_usable(ffprobe_bin):
            if require_ffprobe:
                raise SystemExit(
                    f"[FFMPEG] ffprobe is required but unavailable: {ffprobe_bin}. "
                    "Set KARAOKE_REQUIRE_FFPROBE_VALIDATION=0 to allow fallback."
                )
            log(
                "FFPROBE",
                f"ffprobe unavailable ({ffprobe_bin}); render-duration validation will be skipped.",
                YELLOW,
            )

        def _run_attempt(label: str, attempt_cmd: list[str], *, color: str) -> tuple[int, str]:
            attempt_t0 = now_perf_ms()
            attempt_key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_") or "run"
            log("FFMPEG", f"{label}: {' '.join(map(str, attempt_cmd))}", color)
            try:
                proc = subprocess.run(attempt_cmd)
                rc_local = int(proc.returncode)
            except FileNotFoundError:
                rc_local = 127
                log("FFMPEG", f"Command not found: {ffmpeg_bin}", RED)

            if rc_local != 0:
                _log_step4_timing(f"ffmpeg_attempt.{attempt_key}", attempt_t0)
                return rc_local, f"ffmpeg rc={rc_local}"

            ok, validate_msg = _validate_render_output(
                out_mp4,
                ffprobe_bin,
                expected_audio_duration=audio_duration,
                require_ffprobe=require_ffprobe,
            )
            if not ok:
                log("MP4", f"Validation failed: {validate_msg}", YELLOW)
                _log_step4_timing(f"ffmpeg_attempt.{attempt_key}", attempt_t0)
                return 1, f"validation failed: {validate_msg}"
            if validate_msg:
                log("MP4", validate_msg, YELLOW)

            _log_step4_timing(f"ffmpeg_attempt.{attempt_key}", attempt_t0)
            return 0, ""

        render_cache_p = _render_cache_path(slug, out_mp4)
        render_digest = _render_inputs_digest(
            slug=slug,
            out_mp4=out_mp4,
            ass_path=ass_path,
            audio_fp=audio_fp,
            audio_copy_mode=audio_copy_mode,
            mute_render=mute_render,
            encoder=encoder,
            ffmpeg_bin=ffmpeg_bin,
            vf=vf,
        )
        cached_render = _try_load_json(render_cache_p) or {}
        out_fp = _file_fingerprint(out_mp4)
        can_reuse_render = bool(
            out_fp.get("exists")
            and cached_render.get("digest") == render_digest
            and cached_render.get("output_fingerprint") == out_fp
            and int(out_fp.get("size", 0) or 0) > 0
        )

        if can_reuse_render:
            rc = 0
            last_error_detail = ""
            log("MP4", f"Reusing cached render: {out_mp4}", CYAN)
        else:
            rc, last_error_detail = _run_attempt("RUN", cmd, color=CYAN)

        # Generic safe retry for any render failure (encoder failure or invalid output).
        if rc != 0:
            try:
                safe_fps = int(float((os.environ.get("KARAOKE_SAFE_RETRY_FPS") or "3").strip() or "3"))
            except Exception:
                safe_fps = 3
            safe_fps = max(1, min(int(FPS), safe_fps))

            try:
                safe_v_factor = float((os.environ.get("KARAOKE_SAFE_VIDEO_BITRATE_FACTOR") or "0.70").strip() or "0.70")
            except Exception:
                safe_v_factor = 0.70
            try:
                safe_a_factor = float((os.environ.get("KARAOKE_SAFE_AUDIO_BITRATE_FACTOR") or "0.85").strip() or "0.85")
            except Exception:
                safe_a_factor = 0.85

            safe_video_bitrate = _scaled_bitrate(VIDEO_BITRATE, safe_v_factor, min_kbps=48)
            safe_audio_bitrate = _scaled_bitrate(AUDIO_BITRATE, safe_a_factor, min_kbps=64)
            safe_gop = max(1, int(round(float(safe_fps) * float(VIDEO_GOP_SECONDS))))
            safe_preset = (os.environ.get("KARAOKE_SAFE_X264_PRESET") or "veryfast").strip()
            safe_tune = (os.environ.get("KARAOKE_SAFE_X264_TUNE") or "zerolatency").strip()

            cmd2 = [*cmd]
            for j in range(len(cmd2) - 1):
                if cmd2[j] == "-c:v":
                    cmd2[j + 1] = "libx264"
                elif cmd2[j] == "-r":
                    cmd2[j + 1] = str(safe_fps)
                elif cmd2[j] == "-b:v":
                    cmd2[j + 1] = str(safe_video_bitrate)
                elif cmd2[j] == "-b:a":
                    cmd2[j + 1] = str(safe_audio_bitrate)
                elif cmd2[j] == "-g":
                    cmd2[j + 1] = str(safe_gop)

            _remove_flag_and_value(cmd2, "-preset")
            _remove_flag_and_value(cmd2, "-tune")
            try:
                idx = cmd2.index("libx264")
                insertion = []
                if safe_preset:
                    insertion += ["-preset", safe_preset]
                if safe_tune:
                    insertion += ["-tune", safe_tune]
                cmd2[idx + 1:idx + 1] = insertion
            except Exception:
                pass

            rc, last_error_detail = _run_attempt("SAFE RETRY", cmd2, color=YELLOW)

        finalize_t0 = now_perf_ms()
        render_finished_at = _now_ts()
        try:
            _write_json_atomic(
                render_meta_path,
                {
                    "slug": slug,
                    "render_started_at": render_started_at,
                    "render_finished_at": render_finished_at,
                    "render_rc": rc,
                },
            )
        except Exception:
            pass

        if rc != 0:
            suffix = f" :: {last_error_detail}" if last_error_detail else ""
            raise SystemExit(f"[FFMPEG] Failed to render MP4 (rc={rc}){suffix}")

        try:
            _write_json_atomic(
                render_cache_p,
                {
                    "slug": slug,
                    "digest": render_digest,
                    "output_fingerprint": _file_fingerprint(out_mp4),
                    "updated_at": _now_ts(),
                },
            )
        except Exception:
            pass

        log("MP4", f"Wrote {out_mp4}", GREEN)

        # Summary of key settings
        log("SUMMARY", "=" * 60, CYAN)
        log("SUMMARY", f"Output: {out_mp4.name}", GREEN)
        log("SUMMARY", f"Offset: {LYRICS_OFFSET_SECS:+.3f}s (source: {offset_source})", CYAN)
        log("SUMMARY", f"Resolution: {VIDEO_WIDTH}x{VIDEO_HEIGHT} @ {FPS}fps", CYAN)
        log("SUMMARY", f"Render level: {RENDER_LEVEL} ({RENDER_PROFILE_NAME or 'default'})", CYAN)
        log("SUMMARY", "=" * 60, CYAN)

        _log_step4_timing("finalize", finalize_t0)
    finally:
        _log_step4_timing("total", step4_t0)


if __name__ == "__main__":
    main()

# end of step4_assemble.py
