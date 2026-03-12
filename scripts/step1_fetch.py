#!/usr/bin/env python3
"""
Step 1: Fetch synced LRC + MP3 (yt-dlp)

Goals
- Fast, reliable defaults for English + Spanish songs
- Reuse cached artifacts by default
- Fail fast when recovery is not possible
- Minimize network calls and avoid extra search/download loops
"""

from __future__ import annotations

import argparse
import concurrent.futures
import difflib
import hashlib
import random
import html
import importlib.util
import json
import os
import re
import math
import unicodedata
import selectors
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import OrderedDict, deque
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any
from urllib.parse import parse_qs, urlparse

import requests

from .common import log, log_timing, now_perf_ms, CYAN, GREEN, YELLOW, slugify, ensure_dir

# Try to import Invidious client for source proxy
# DISABLED: Client-side download only, no Invidious fallback
INVIDIOUS_AVAILABLE = False
# try:
#     from karaoapi.invidious_client import InvidiousClient
#     INVIDIOUS_AVAILABLE = True
# except ImportError:
#     INVIDIOUS_AVAILABLE = False

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
MP3_DIR = ROOT / "mp3s"
TIMINGS_DIR = ROOT / "timings"
META_DIR = ROOT / "meta"

# ─────────────────────────────────────────────
# Config (reliability-first defaults; env-overridable)
# ─────────────────────────────────────────────

LRCLIB_TIMEOUT = float(os.environ.get("MIXTERIOSO_LRCLIB_TIMEOUT", "10.0"))
LRCLIB_MAX_RETRIES = max(1, int(os.environ.get("MIXTERIOSO_LRCLIB_MAX_RETRIES", "3")))
LRC_RELAXED_RECOVERY_TIMEOUT_SEC = max(
    1.0, float(os.environ.get("MIXTERIOSO_LRC_RELAXED_RECOVERY_TIMEOUT_SEC", "12.0"))
)
LRC_RELAXED_RECOVERY_RETRIES = max(
    1, int(os.environ.get("MIXTERIOSO_LRC_RELAXED_RECOVERY_RETRIES", "3"))
)
LRC_RELAXED_RECOVERY_ENABLED = os.environ.get("MIXTERIOSO_LRC_RELAXED_RECOVERY_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}

# Kept for debug/search tooling only. Production download path uses ytsearchN in one command.
YT_SEARCH_N = int(os.environ.get("MIXTERIOSO_YT_SEARCH_N", "8"))

# Aggressive timeouts and minimal retries: faster to try next ID than to retry hard
YTDLP_SOCKET_TIMEOUT = os.environ.get("MIXTERIOSO_YTDLP_SOCKET_TIMEOUT", "6")
YTDLP_RETRIES = os.environ.get("MIXTERIOSO_YTDLP_RETRIES", "1")
YTDLP_FRAG_RETRIES = os.environ.get("MIXTERIOSO_YTDLP_FRAGMENT_RETRIES", "1")

# Fragment parallelism (major win on DASH/HLS)
YTDLP_CONCURRENT_FRAGS = os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS", "2")
YTDLP_CONCURRENT_FRAGS_ADAPTIVE = os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS_ADAPTIVE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
YTDLP_CONCURRENT_FRAGS_MAX = max(
    1,
    int(os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS_MAX", "8")),
)
YTDLP_CONCURRENT_FRAGS_HOT_QUERY = max(
    1,
    int(os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS_HOT_QUERY", "6")),
)
YTDLP_CONCURRENT_FRAGS_TIGHT_REMAINING_SEC = max(
    1.0,
    float(os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS_TIGHT_REMAINING_SEC", "12")),
)
YTDLP_CONCURRENT_FRAGS_TIGHT_VALUE = max(
    1,
    int(os.environ.get("MIXTERIOSO_YTDLP_CONCURRENT_FRAGMENTS_TIGHT_VALUE", "2")),
)

# Match proven local recipe by default:
# prefer m4a bestaudio for transcode paths, but allow fallback.
YTDLP_AUDIO_QUALITY = os.environ.get("MIXTERIOSO_YTDLP_AUDIO_QUALITY", "0")
# Speed/quality balance: prefer AAC-ish ~128k class streams that are usually
# perceptually close while downloading materially faster than max-bitrate audio.
# YouTube commonly reports the "128k" m4a rung as ~129k, so keep a little headroom.
YTDLP_FORMAT = os.environ.get(
    "MIXTERIOSO_YTDLP_FORMAT",
    "bestaudio[acodec^=mp4a][abr<=160]/bestaudio[abr<=160]/18/bestaudio/best",
).strip()
YTDLP_PROGRESSIVE_FALLBACK_FORMAT = os.environ.get(
    "MIXTERIOSO_YTDLP_PROGRESSIVE_FALLBACK_FORMAT",
    "18/bestaudio/best",
).strip() or "18/bestaudio/best"

# Default to no forced client (matches proven local/VM success path).
YTDLP_EXTRACTOR_ARGS = os.environ.get(
    "MIXTERIOSO_YTDLP_EXTRACTOR_ARGS",
    "",
).strip()
YTDLP_FALLBACK_EXTRACTOR_ARGS = os.environ.get(
    "MIXTERIOSO_YTDLP_FALLBACK_EXTRACTOR_ARGS",
    "youtube:player_client=android",
).strip()

# Single-command resilience for search downloads:
# try a small window and stop on first success.
YTDLP_SEARCH_SPAN = max(1, int(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_SPAN", "1")))

YTDLP_JS_RUNTIMES = os.environ.get(
    "MIXTERIOSO_YTDLP_JS_RUNTIMES",
    "",
).strip()
YTDLP_REMOTE_COMPONENTS = os.environ.get(
    "MIXTERIOSO_YTDLP_REMOTE_COMPONENTS",
    "",
).strip()

YTDLP_UA = os.environ.get(
    "MIXTERIOSO_YTDLP_UA",
    "",
).strip()
YTDLP_EXTRA_HEADERS_RAW = os.environ.get(
    "MIXTERIOSO_YTDLP_EXTRA_HEADERS",
    "",
).strip()
YTDLP_COOKIES_PATH = os.environ.get("MIXTERIOSO_YTDLP_COOKIES", "").strip()
YTDLP_PROXY = os.environ.get("MIXTERIOSO_YTDLP_PROXY", "").strip()
YTDLP_PROXY_POOL_RAW = os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL", "").strip()
YTDLP_PROXY_POOL_FILE = os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL_FILE", "").strip()
YTDLP_PROXY_POOL_URL = os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL_URL", "").strip()
YTDLP_PROXY_RANGE_HOST = os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_HOST", "").strip()
YTDLP_PROXY_RANGE_PORT = os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_PORT", "").strip()
YTDLP_PROXY_RANGE_USER_PREFIX = os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_USER_PREFIX", "").strip()
YTDLP_PROXY_RANGE_PASSWORD = os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_PASSWORD", "").strip()
YTDLP_PROXY_RANGE_START = max(0, int(os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_START", "0")))
YTDLP_PROXY_RANGE_END = max(0, int(os.environ.get("MIXTERIOSO_YTDLP_PROXY_RANGE_END", "0")))
YTDLP_PROXY_ROTATE_ON_BOTCHECK = os.environ.get("MIXTERIOSO_YTDLP_PROXY_ROTATE_ON_BOTCHECK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
YTDLP_PROXY_MAX_ROTATIONS = max(1, int(os.environ.get("MIXTERIOSO_YTDLP_PROXY_MAX_ROTATIONS", "3")))
YTDLP_PROXY_PER_CALL_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_YTDLP_PROXY_PER_CALL_ATTEMPTS", "2")))
YTDLP_PROXY_FAILURE_BASE_COOLDOWN_SEC = max(
    1.0, float(os.environ.get("MIXTERIOSO_YTDLP_PROXY_FAILURE_BASE_COOLDOWN_SEC", "30"))
)
YTDLP_PROXY_FAILURE_MAX_COOLDOWN_SEC = max(
    YTDLP_PROXY_FAILURE_BASE_COOLDOWN_SEC,
    float(os.environ.get("MIXTERIOSO_YTDLP_PROXY_FAILURE_MAX_COOLDOWN_SEC", "180")),
)
YTDLP_PROXY_POOL_FETCH_TIMEOUT_SEC = max(
    2.0, float(os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL_FETCH_TIMEOUT_SEC", "10"))
)
YTDLP_PROXY_POOL_REFRESH_SEC = max(
    30.0, float(os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL_REFRESH_SEC", "1800"))
)
YTDLP_PROXY_POOL_MAX_ENTRIES = max(
    1, int(os.environ.get("MIXTERIOSO_YTDLP_PROXY_POOL_MAX_ENTRIES", "50000"))
)
YTDLP_PROXY_SELECTION_POLICY = os.environ.get(
    "MIXTERIOSO_YTDLP_PROXY_SELECTION_POLICY",
    "random",
).strip().lower() or "random"
YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES = os.environ.get(
    "MIXTERIOSO_YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES",
    "0",
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
source_DATA_API_KEY = os.environ.get("MIXTERIOSO_YT_DATA_API_KEY", "").strip()
GENIUS_TIMEOUT = max(1.0, float(os.environ.get("MIXTERIOSO_GENIUS_TIMEOUT", "4.0")))
GENIUS_API_BASE = os.environ.get("MIXTERIOSO_GENIUS_API_BASE", "https://api.genius.com").strip().rstrip("/")
YOUTUBE_SUGGEST_API = os.environ.get(
    "MIXTERIOSO_YOUTUBE_SUGGEST_API",
    "https://suggestqueries.google.com/complete/search",
).strip()
YOUTUBE_SUGGEST_TIMEOUT = max(
    1.0,
    float(
        os.environ.get(
            "MIXTERIOSO_YOUTUBE_SUGGEST_TIMEOUT",
            str(min(6.0, max(2.0, GENIUS_TIMEOUT))),
        )
    ),
)
YTDLP_SEARCH_TIMEOUT = float(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_TIMEOUT", "20"))
YTDLP_CMD_TIMEOUT = float(os.environ.get("MIXTERIOSO_YTDLP_CMD_TIMEOUT", "75"))
YTDLP_DIAG_LINES = max(20, int(os.environ.get("MIXTERIOSO_YTDLP_DIAG_LINES", "120")))
YTDLP_CAPTURE_LINES = max(
    YTDLP_DIAG_LINES,
    int(os.environ.get("MIXTERIOSO_YTDLP_CAPTURE_LINES", "400")),
)
YTDLP_PROGRESS_HEARTBEAT_SEC = max(
    2.0, float(os.environ.get("MIXTERIOSO_YTDLP_PROGRESS_HEARTBEAT_SEC", "8"))
)
YTDLP_NO_PROGRESS_TIMEOUT_SEC = max(
    0.0, float(os.environ.get("MIXTERIOSO_YTDLP_NO_PROGRESS_TIMEOUT_SEC", "5"))
)
YTDLP_CMD_RAW = os.environ.get("MIXTERIOSO_YTDLP_CMD", "").strip()
YTDLP_SEARCH_CACHE_TTL_SEC = max(0.0, float(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_CACHE_TTL_SEC", "900")))
YTDLP_SEARCH_CACHE_MAX_ENTRIES = max(10, int(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_CACHE_MAX_ENTRIES", "2000")))
YTDLP_SEARCH_DISK_CACHE_TTL_SEC = max(
    0.0,
    float(
        os.environ.get(
            "MIXTERIOSO_YTDLP_SEARCH_DISK_CACHE_TTL_SEC",
            str(max(900.0, YTDLP_SEARCH_CACHE_TTL_SEC)),
        )
    ),
)
YTDLP_SEARCH_DISK_CACHE_MAX_ENTRIES = max(
    100,
    int(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_DISK_CACHE_MAX_ENTRIES", "5000")),
)
YTDLP_SEARCH_DISK_CACHE_PRUNE_INTERVAL_SEC = max(
    5.0,
    float(os.environ.get("MIXTERIOSO_YTDLP_SEARCH_DISK_CACHE_PRUNE_INTERVAL_SEC", "60")),
)
YTDLP_SEARCH_SINGLEFLIGHT_ENABLED = os.environ.get("MIXTERIOSO_YTDLP_SEARCH_SINGLEFLIGHT_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
_SEARCH_DISK_CACHE_DIR_RAW = os.environ.get("MIXTERIOSO_YTDLP_SEARCH_DISK_CACHE_DIR", "").strip()
YTDLP_SEARCH_DISK_CACHE_DIR = Path(_SEARCH_DISK_CACHE_DIR_RAW) if _SEARCH_DISK_CACHE_DIR_RAW else (META_DIR / "cache" / "yt_search_ids")
YTDLP_AUDIO_DISK_CACHE_ENABLED = os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
_AUDIO_DISK_CACHE_DIR_RAW = os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_DIR", "").strip()
YTDLP_AUDIO_DISK_CACHE_DIR = (
    Path(_AUDIO_DISK_CACHE_DIR_RAW)
    if _AUDIO_DISK_CACHE_DIR_RAW
    else (MP3_DIR / "_global_audio_cache")
)
YTDLP_AUDIO_DISK_CACHE_TTL_SEC = max(
    0.0,
    float(os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_TTL_SEC", "604800")),
)
YTDLP_AUDIO_DISK_CACHE_MAX_ENTRIES = max(
    20,
    int(os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_MAX_ENTRIES", "3000")),
)
YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC = max(
    10.0,
    float(os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC", "120")),
)
YTDLP_AUDIO_DISK_CACHE_MIN_BYTES = max(
    256,
    int(os.environ.get("MIXTERIOSO_YTDLP_AUDIO_DISK_CACHE_MIN_BYTES", "2048")),
)
YTDLP_AUDIO_SINGLEFLIGHT_ENABLED = os.environ.get("MIXTERIOSO_YTDLP_AUDIO_SINGLEFLIGHT_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED = os.environ.get("MIXTERIOSO_YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
YTDLP_SOURCE_FAIL_COOLDOWN_SEC = max(
    5.0,
    float(os.environ.get("MIXTERIOSO_YTDLP_SOURCE_FAIL_COOLDOWN_SEC", "300")),
)
YTDLP_SOURCE_FAIL_COOLDOWN_MAX_ENTRIES = max(
    100,
    int(os.environ.get("MIXTERIOSO_YTDLP_SOURCE_FAIL_COOLDOWN_MAX_ENTRIES", "5000")),
)

# MP3 robustness knobs (server-side resilience)
MP3_PRIMARY_SEARCH_SPAN = max(1, int(os.environ.get("MIXTERIOSO_MP3_PRIMARY_SEARCH_SPAN", "3")))
MP3_MAX_ID_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_MAX_ID_ATTEMPTS", "4")))
MP3_MAX_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_MAX_QUERY_VARIANTS", "4")))
MP3_MAX_SEARCH_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_MAX_SEARCH_QUERY_VARIANTS", "2")))
MP3_MAX_SOURCE_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_MAX_SOURCE_ATTEMPTS", "3")))
MP3_MAX_SOURCE_SECONDS = max(15.0, float(os.environ.get("MIXTERIOSO_MP3_MAX_SOURCE_SECONDS", "60")))
MP3_TOTAL_TIMEOUT_SEC = max(5.0, float(os.environ.get("MIXTERIOSO_MP3_TOTAL_TIMEOUT_SEC", "75")))
MP3_STOP_AFTER_SEARCH_TIMEOUT = os.environ.get("MIXTERIOSO_MP3_STOP_AFTER_SEARCH_TIMEOUT", "1").strip().lower() not in {"0","false","no","off"}
MP3_PRIMARY_USE_COOKIES = os.environ.get("MIXTERIOSO_MP3_PRIMARY_USE_COOKIES", "1").strip().lower() not in {"0","false","no","off"}
MP3_ENABLE_ID_PREFETCH = os.environ.get("MIXTERIOSO_MP3_ENABLE_ID_PREFETCH", "1").strip().lower() not in {"0","false","no","off"}
MP3_FAIL_FAST_ON_BOT_GATE_NO_COOKIES = (
    os.environ.get("MIXTERIOSO_MP3_FAIL_FAST_ON_BOT_GATE_NO_COOKIES", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
# Keep a final ytsearch fallback enabled by default. This catches cases where
# a resolved source ID is valid but blocked/throttled for direct extraction.
MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK = os.environ.get("MIXTERIOSO_MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", "1").strip().lower() not in {"0","false","no","off"}
MP3_ENABLE_SOUNDCLOUD = os.environ.get("MIXTERIOSO_MP3_ENABLE_SOUNDCLOUD", "0").strip().lower() not in {"0","false","no","off"}
MP3_PREFER_LYRICS_VERSION = os.environ.get("MIXTERIOSO_MP3_PREFER_LYRICS_VERSION", "1").strip().lower() not in {"0","false","no","off"}
MP3_DURATION_AWARE_SOURCE_MATCH_DEFAULT = os.environ.get("MIXTERIOSO_STEP1_DURATION_AWARE_SOURCE_MATCH", "0").strip().lower() not in {"0","false","no","off"}
MP3_PREFER_OFFICIAL_AUDIO_VERSION = os.environ.get("MIXTERIOSO_MP3_PREFER_OFFICIAL_AUDIO_VERSION", "1").strip().lower() not in {"0","false","no","off"}
MP3_PREFER_NON_LIVE_VERSION = os.environ.get("MIXTERIOSO_MP3_PREFER_NON_LIVE_VERSION", "1").strip().lower() not in {"0","false","no","off"}
MP3_NON_LIVE_MIN_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_NON_LIVE_MIN_SEARCH_N", "3")))
MP3_ENABLE_CACHED_ID_FASTPATH = os.environ.get("MIXTERIOSO_MP3_ENABLE_CACHED_ID_FASTPATH", "1").strip().lower() not in {"0","false","no","off"}
MP3_SOUNDCLOUD_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_SOUNDCLOUD_SEARCH_N", "5")))
MP3_ENABLE_DYNAMIC_SEARCH_BUDGET = os.environ.get("MIXTERIOSO_MP3_DYNAMIC_SEARCH_BUDGET", "1").strip().lower() not in {"0", "false", "no", "off"}
MP3_ENABLE_PARALLEL_STRATEGY_RACE = os.environ.get("MIXTERIOSO_MP3_PARALLEL_STRATEGY_RACE", "1").strip().lower() not in {"0", "false", "no", "off"}
MP3_DYNAMIC_MID_REMAINING_SEC = max(5.0, float(os.environ.get("MIXTERIOSO_MP3_DYNAMIC_MID_REMAINING_SEC", "30")))
MP3_DYNAMIC_TIGHT_REMAINING_SEC = max(2.0, float(os.environ.get("MIXTERIOSO_MP3_DYNAMIC_TIGHT_REMAINING_SEC", "12")))
MP3_DYNAMIC_MID_MAX_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_DYNAMIC_MID_MAX_SEARCH_N", "2")))
MP3_DYNAMIC_TIGHT_MAX_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_DYNAMIC_TIGHT_MAX_SEARCH_N", "1")))
MP3_HOT_QUERY_SPEED_MODE = os.environ.get("MIXTERIOSO_MP3_HOT_QUERY_SPEED_MODE", "1").strip().lower() not in {"0", "false", "no", "off"}
MP3_HOT_QUERY_SPEED_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_HOT_QUERY_SPEED_SEARCH_N", "2")))
MP3_HOT_QUERY_SPEED_MAX_ID_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_HOT_QUERY_SPEED_MAX_ID_ATTEMPTS", "2")))
MP3_HOT_QUERY_SPEED_MAX_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_HOT_QUERY_SPEED_MAX_QUERY_VARIANTS", "2")))
MP3_HOT_QUERY_SPEED_MAX_SEARCH_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_HOT_QUERY_SPEED_MAX_SEARCH_QUERY_VARIANTS", "1")))
MP3_PARALLEL_SEARCH_QUERIES = max(1, int(os.environ.get("MIXTERIOSO_MP3_PARALLEL_SEARCH_QUERIES", "2")))
MP3_SOURCE_RANKING_ENABLED = os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_SOURCE_RANKING_TIMEOUT_SEC = max(
    1.0,
    float(os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_TIMEOUT_SEC", "4.0")),
)
MP3_SOURCE_RANKING_MAX_IDS = max(
    1,
    int(os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_MAX_IDS", "6")),
)
MP3_SOURCE_RANKING_MIN_SCORE = min(
    1.0,
    max(0.0, float(os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_MIN_SCORE", "0.62"))),
)
MP3_SOURCE_RANKING_MIN_MARGIN = min(
    1.0,
    max(0.0, float(os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_MIN_MARGIN", "0.10"))),
)
MP3_SOURCE_RANKING_MIN_IMPROVEMENT_OVER_FIRST = min(
    1.0,
    max(0.0, float(os.environ.get("MIXTERIOSO_MP3_SOURCE_RANKING_MIN_IMPROVEMENT_OVER_FIRST", "0.12"))),
)
MP3_EXHAUSTIVE_DURATION_MATCH = os.environ.get("MIXTERIOSO_MP3_EXHAUSTIVE_DURATION_MATCH", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_DURATION_MATCH_TOLERANCE_MS = max(0, int(os.environ.get("MIXTERIOSO_MP3_DURATION_MATCH_TOLERANCE_MS", "1500")))
MP3_DURATION_MATCH_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_DURATION_MATCH_SEARCH_N", "25")))
MP3_DURATION_MATCH_MAX_ID_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_MP3_DURATION_MATCH_MAX_ID_ATTEMPTS", "30")))
MP3_DURATION_MATCH_PROBE_TIMEOUT_SEC = max(
    1.0,
    float(os.environ.get("MIXTERIOSO_MP3_DURATION_MATCH_PROBE_TIMEOUT_SEC", "10.0")),
)
STEP1_USE_LRC_AUDIO_QUERY = os.environ.get("MIXTERIOSO_STEP1_USE_LRC_AUDIO_QUERY", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_TOP_HIT_MODE = os.environ.get("MIXTERIOSO_MP3_TOP_HIT_MODE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
STEP1_ENFORCE_SOURCE_MATCH_RETRY = os.environ.get("MIXTERIOSO_STEP1_ENFORCE_SOURCE_MATCH_RETRY", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
# Keep selection logic intentionally simple/fast by default:
# preserve yt relevance order and pick first viable non-live candidate.
MP3_SIMPLE_FIRST_RESULT_MODE = os.environ.get("MIXTERIOSO_MP3_SIMPLE_FIRST_RESULT_MODE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_TOP_HIT_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_TOP_HIT_SEARCH_N", "3")))
MP3_ONE_CALL_SIMPLE_MODE = os.environ.get("MIXTERIOSO_MP3_ONE_CALL_SIMPLE_MODE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_ONE_CALL_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_MP3_ONE_CALL_SEARCH_N", "3")))
MP3_ONE_CALL_MAX_SECONDS = max(0.5, float(os.environ.get("MIXTERIOSO_MP3_ONE_CALL_MAX_SECONDS", "2.5")))
MP3_FAST_QUERY_RESOLVE_DIRECT_MODE = os.environ.get(
    "MIXTERIOSO_MP3_FAST_QUERY_RESOLVE_DIRECT_MODE",
    "1",
).strip().lower() not in {"0", "false", "no", "off"}
MP3_FAST_QUERY_RESOLVE_TIMEOUT_SEC = max(
    0.5,
    float(os.environ.get("MIXTERIOSO_MP3_FAST_QUERY_RESOLVE_TIMEOUT_SEC", "2.0")),
)
STEP1_FAST_SKIP_DURATION_HINT = os.environ.get("MIXTERIOSO_STEP1_FAST_SKIP_DURATION_HINT", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MP3_DURATION_MATCH_DISABLE_DIRECT_YTSEARCH_FALLBACK = os.environ.get(
    "MIXTERIOSO_MP3_DURATION_MATCH_DISABLE_DIRECT_YTSEARCH_FALLBACK",
    "1",
).strip().lower() not in {"0", "false", "no", "off"}
STEP1_FAST_NO_TRANSCODE = os.environ.get("MIXTERIOSO_STEP1_FAST_NO_TRANSCODE", "1").strip().lower() not in {"0", "false", "no", "off"}
STEP1_FAST_NO_TRANSCODE_FORMAT = os.environ.get(
    "MIXTERIOSO_STEP1_FAST_NO_TRANSCODE_FORMAT",
    "bestaudio[acodec^=mp4a][abr<=160]/bestaudio[abr<=160]/18/bestaudio/best",
).strip()
STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT = os.environ.get(
    "MIXTERIOSO_STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT",
    "bestaudio[acodec^=mp4a][abr<=96][vcodec=none]/bestaudio[abr<=96][vcodec=none]/worstaudio[acodec^=mp4a][vcodec=none]/worstaudio[vcodec=none]",
).strip()
STEP1_FAST_AUDIO_ONLY_FORMAT = os.environ.get(
    "MIXTERIOSO_STEP1_FAST_AUDIO_ONLY_FORMAT",
    "bestaudio[acodec^=mp4a][abr<=160][vcodec=none]/bestaudio[abr<=160][vcodec=none]/bestaudio[vcodec=none]/bestaudio",
).strip()
STEP1_FAST_ALIAS_MP3 = os.environ.get("MIXTERIOSO_STEP1_FAST_ALIAS_MP3", "1").strip().lower() not in {"0", "false", "no", "off"}
STEP1_FAIL_FAST = os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST", "1").strip().lower() not in {"0", "false", "no", "off"}
STEP1_FAIL_FAST_SEARCH_N = max(1, int(os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_SEARCH_N", "1")))
STEP1_FAIL_FAST_MAX_ID_ATTEMPTS = max(1, int(os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_MAX_ID_ATTEMPTS", "1")))
STEP1_FAIL_FAST_MAX_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_MAX_QUERY_VARIANTS", "1")))
STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS = max(1, int(os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS", "1")))
STEP1_FAIL_FAST_SKIP_CANONICAL_RECOVERY = os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_SKIP_CANONICAL_RECOVERY", "1").strip().lower() not in {"0", "false", "no", "off"}
STEP1_FAIL_FAST_SKIP_PSEUDO_LRC = os.environ.get("MIXTERIOSO_STEP1_FAIL_FAST_SKIP_PSEUDO_LRC", "1").strip().lower() not in {"0", "false", "no", "off"}
STEP1_DEFAULT_RETRY_ATTEMPT = max(
    1,
    int(
        os.environ.get(
            "MIXTERIOSO_STEP1_RETRY_ATTEMPT",
            "3",
        )
    ),
)
STEP1_SPEED_MODE_DEFAULT = (
    os.environ.get("MIXTERIOSO_STEP1_SPEED_MODE", "extra-turbo").strip().lower() or "extra-turbo"
)
LRC_FAST_TOTAL_TIMEOUT_SEC = max(
    0.5,
    float(
        os.environ.get(
            "MIXTERIOSO_LRC_FAST_TOTAL_TIMEOUT_SEC",
            "3.5",
        )
    ),
)
LRC_FAST_MAX_ROWS = max(1, int(os.environ.get("MIXTERIOSO_LRC_FAST_MAX_ROWS", "8")))
_STEP1_SPEED_MODE_TO_RETRY: Dict[str, int] = {
    "turbo": 2,
    "extra-turbo": 1,
    "ultimate-light-speed": 1,
}
if STEP1_SPEED_MODE_DEFAULT not in _STEP1_SPEED_MODE_TO_RETRY:
    STEP1_SPEED_MODE_DEFAULT = "extra-turbo"
MP3_QUERY_SUFFIXES_RAW = os.environ.get(
    "MIXTERIOSO_MP3_QUERY_SUFFIXES",
    "official audio|audio|topic|lyrics|karaoke|instrumental|letra|audio oficial",
).strip()
MP3_QUERY_SUFFIXES: Tuple[str, ...] = tuple([p.strip() for p in MP3_QUERY_SUFFIXES_RAW.split("|") if p.strip()])
MP3_ENABLE_DIRECT_SOURCE_FASTPATH = os.environ.get("MIXTERIOSO_MP3_DIRECT_SOURCE_FASTPATH", "1").strip().lower() not in {"0","false","no","off"}
MP3_DIRECT_SOCKET_TIMEOUT = os.environ.get("MIXTERIOSO_MP3_DIRECT_SOCKET_TIMEOUT", "8")
MP3_DIRECT_RETRIES = os.environ.get("MIXTERIOSO_MP3_DIRECT_RETRIES", "3")
MP3_DIRECT_FRAG_RETRIES = os.environ.get("MIXTERIOSO_MP3_DIRECT_FRAGMENT_RETRIES", "3")
MP3_DIRECT_EXTRACTOR_ARGS = os.environ.get(
    "MIXTERIOSO_MP3_DIRECT_EXTRACTOR_ARGS",
    "youtube:player_client=android",
).strip()
MP3_DIRECT_CMD_TIMEOUT = max(5.0, float(os.environ.get("MIXTERIOSO_MP3_DIRECT_CMD_TIMEOUT", "90")))

YTDLP_FAILOVER_CLIENTS_RAW = os.environ.get(
    "MIXTERIOSO_YTDLP_FAILOVER_CLIENTS",
    "android,ios,tv_embedded,mweb,web_safari",
).strip()
YTDLP_FAILOVER_CLIENTS: Tuple[str, ...] = tuple([p.strip() for p in YTDLP_FAILOVER_CLIENTS_RAW.split(",") if p.strip()])
MP3_PINNED_IDS_RAW = os.environ.get(
    "MIXTERIOSO_MP3_PINNED_IDS",
    (
        "the_beatles_let_it_be:5AnNkJ_VK9E|CGj85pVzRJs,"
        "john_frusciante_god:jrMvYbgF1H0,"
        "john_frusciante_the_past_recedes:3gvI3b9-wPY"
    ),
).strip()

_source_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")
_LYRICS_TITLE_RE = re.compile(
    r"\b(lyrics?|lyric\s+video|official\s+lyrics?|letra|letras)\b",
    re.IGNORECASE,
)
_LIVE_QUERY_INTENT_RE = re.compile(
    r"\b(live|concert|festival|unplugged|en\s+vivo|ao\s+vivo)\b",
    re.IGNORECASE,
)
_LIVE_TITLE_BRACKET_RE = re.compile(r"[\(\[][^)\]]*\blive\b[^)\]]*[\)\]]", re.IGNORECASE)
_LIVE_TITLE_CONTEXT_RE = re.compile(r"\blive\s+(?:at|from|in)\b|\s-\s*live\b", re.IGNORECASE)
_LIVE_TITLE_TERM_RE = re.compile(r"\b(en\s+vivo|ao\s+vivo|unplugged|concert|show)\b", re.IGNORECASE)
_LIVE_TITLE_ANY_RE = re.compile(r"\blive\b", re.IGNORECASE)
# Treat date-bearing performance titles as live-like by policy.
_LIVE_TITLE_DATE_RE = re.compile(
    r"(\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b|\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b|\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b)",
    re.IGNORECASE,
)
_OFFICIAL_AUDIO_TITLE_RE = re.compile(
    r"\b(official\s+audio|audio\s+oficial|official\s+track|official\s+song)\b",
    re.IGNORECASE,
)
_AUDIO_TITLE_RE = re.compile(r"\baudio\b", re.IGNORECASE)
_OFFICIAL_VIDEO_TITLE_RE = re.compile(r"\bofficial\s+video\b", re.IGNORECASE)
_OFFICIAL_AUDIO_BYPASS_QUERY_RE = re.compile(
    r"\b(lyrics?|letra|karaoke|instrumental|remix|cover)\b",
    re.IGNORECASE,
)


# LRC fetching robustness knobs
LRC_PREFER_LANGS_RAW = os.environ.get("MIXTERIOSO_LRC_PREFER_LANGS", "en,es").strip()
LRC_PREFER_LANGS: Tuple[str, ...] = tuple([p.strip() for p in LRC_PREFER_LANGS_RAW.split(",") if p.strip()]) or ("en", "es")
LRC_TOTAL_TIMEOUT_SEC = max(1.0, float(os.environ.get("MIXTERIOSO_LRC_TOTAL_TIMEOUT_SEC", "24.0")))
LRC_METADATA_HINT_TIMEOUT_SEC = max(0.5, float(os.environ.get("MIXTERIOSO_LRC_METADATA_HINT_TIMEOUT_SEC", "1.2")))
LRC_LRCLIB_MAX_VARIANTS = max(2, int(os.environ.get("MIXTERIOSO_LRC_LRCLIB_MAX_VARIANTS", "6")))
LRC_LRCLIB_PARALLELISM = max(1, int(os.environ.get("MIXTERIOSO_LRC_LRCLIB_PARALLELISM", "2")))
LRC_BOUNDED_MIN_REQUEST_TIMEOUT_SEC = max(
    0.25,
    float(os.environ.get("MIXTERIOSO_LRC_BOUNDED_MIN_REQUEST_TIMEOUT_SEC", "1.5")),
)
LRC_ENABLE_YT_CAPTIONS_FALLBACK = os.environ.get("MIXTERIOSO_LRC_YT_CAPTIONS_FALLBACK", "1").strip().lower() not in {"0", "false", "no", "off"}
LRC_YT_CAPTIONS_TIMEOUT = float(os.environ.get("MIXTERIOSO_LRC_YT_CAPTIONS_TIMEOUT", "35.0"))
LRC_YT_CAPTIONS_DIRECT_TIMEOUT = max(
    LRC_YT_CAPTIONS_TIMEOUT,
    float(os.environ.get("MIXTERIOSO_LRC_YT_CAPTIONS_DIRECT_TIMEOUT", "90.0")),
)
LRC_MIN_LINES = max(3, int(os.environ.get("MIXTERIOSO_LRC_MIN_LINES", "6")))
LRC_ENABLE_TEXT_PSEUDO_FALLBACK = os.environ.get("MIXTERIOSO_LRC_TEXT_PSEUDO_FALLBACK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LRC_ENABLE_PLAIN_LYRICS_FALLBACK = os.environ.get("MIXTERIOSO_LRC_PLAIN_FALLBACK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LRC_PSEUDO_START_SECS = max(0.0, float(os.environ.get("MIXTERIOSO_LRC_PSEUDO_START_SECS", "8.0")))
LRC_PSEUDO_STEP_SECS = max(0.25, float(os.environ.get("MIXTERIOSO_LRC_PSEUDO_STEP_SECS", "3.0")))
LRC_PSEUDO_MAX_LINES = max(20, int(os.environ.get("MIXTERIOSO_LRC_PSEUDO_MAX_LINES", "400")))
LRC_PARALLEL_CAPTION_MODES = max(
    1, min(2, int(os.environ.get("MIXTERIOSO_LRC_PARALLEL_CAPTION_MODES", "2")))
)
LRC_CANONICAL_MAX_QUERIES = max(0, int(os.environ.get("MIXTERIOSO_LRC_CANONICAL_MAX_QUERIES", "1")))
LRC_LOW_CONFIDENCE_MIN_SCORE = min(
    0.95,
    max(0.0, float(os.environ.get("MIXTERIOSO_LRC_LOW_CONFIDENCE_MIN_SCORE", "0.22"))),
)
LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP = min(
    1.0,
    max(0.0, float(os.environ.get("MIXTERIOSO_LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP", "0.50"))),
)
LRC_REUSE_REFRESH_ON_WEAK = os.environ.get("MIXTERIOSO_LRC_REUSE_REFRESH_ON_WEAK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LRC_RESULT_CACHE_TTL_SEC = max(0.0, float(os.environ.get("MIXTERIOSO_LRC_RESULT_CACHE_TTL_SEC", "86400")))
LRC_RESULT_CACHE_MAX_ENTRIES = max(10, int(os.environ.get("MIXTERIOSO_LRC_RESULT_CACHE_MAX_ENTRIES", "2000")))
STRICT_REQUIRE_LYRICS = os.environ.get("MIXTERIOSO_STRICT_REQUIRE_LYRICS", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
STRICT_REQUIRE_AUDIO = os.environ.get("MIXTERIOSO_STRICT_REQUIRE_AUDIO", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE = os.environ.get(
    "MIXTERIOSO_STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE",
    "1",
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}


def _resolve_ytdlp_cmd() -> List[str]:
    if YTDLP_CMD_RAW:
        try:
            parsed = shlex.split(YTDLP_CMD_RAW)
        except ValueError:
            parsed = []
        if parsed:
            return parsed

    if shutil.which("yt-dlp"):
        return ["yt-dlp"]

    # Fallback when the console script is missing but the package exists.
    if importlib.util.find_spec("yt_dlp") is not None:
        return [sys.executable, "-m", "yt_dlp"]

    return ["yt-dlp"]


YTDLP_CMD = _resolve_ytdlp_cmd()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_step1_speed_mode(mode: str) -> str:
    raw = " ".join(str(mode or "").strip().lower().split())
    aliases = {
        "fast": "turbo",
        "turbo": "turbo",
        "extra turbo": "extra-turbo",
        "extra-turbo": "extra-turbo",
        "extra_turbo": "extra-turbo",
        "lightspeed": "ultimate-light-speed",
        "light speed": "ultimate-light-speed",
        "ultimate-light-speed": "ultimate-light-speed",
        "ultimate_light_speed": "ultimate-light-speed",
    }
    return aliases.get(raw, "")


def _retry_attempt_from_speed_mode(mode: str, *, fallback: int) -> int:
    normalized = _normalize_step1_speed_mode(mode)
    if normalized:
        return int(_STEP1_SPEED_MODE_TO_RETRY.get(normalized, fallback))
    return int(fallback)


def _genius_access_token() -> str:
    for env_name in (
        "MIXTERIOSO_GENIUS_API_KEY",
        "MIXTERIOSO_GENIUS_ACCESS_TOKEN",
        "GENIUS_API_KEY",
        "GENIUS_ACCESS_TOKEN",
    ):
        token = os.environ.get(env_name, "").strip()
        if token:
            return token
    return ""


# Debug mode for server-side diagnostics.
YTDLP_NO_WARNINGS = _env_bool("MIXTERIOSO_YTDLP_NO_WARNINGS", False)
YTDLP_VERBOSE = _env_bool("MIXTERIOSO_YTDLP_VERBOSE", False)

_RUNTIME_COOKIES_PATH: Optional[str] = None
_PROXY_CURSOR_LOCK = threading.Lock()
_PROXY_CURSOR = 0
_PROXY_STATE: Dict[str, Dict[str, Any]] = {}
_PROXY_POOL_CACHE: List[str] = []
_PROXY_POOL_CACHE_AT_MONO = 0.0
_YT_SEARCH_IDS_CACHE_LOCK = threading.Lock()
_YT_SEARCH_IDS_CACHE: Dict[Tuple[str, int, int], Tuple[float, List[str]]] = {}
_YT_SEARCH_IDS_DISK_CACHE_LOCK = threading.Lock()
_YT_SEARCH_IDS_DISK_CACHE_LAST_PRUNE_AT_MONO = 0.0
_YT_SEARCH_SINGLEFLIGHT_LOCK = threading.Lock()
_YT_SEARCH_SINGLEFLIGHT_LOCKS: Dict[str, threading.Lock] = {}
_YT_SEARCH_SINGLEFLIGHT_REFS: Dict[str, int] = {}
_YT_AUDIO_DISK_CACHE_LOCK = threading.Lock()
_YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO = 0.0
_YT_AUDIO_SINGLEFLIGHT_LOCK = threading.Lock()
_YT_AUDIO_SINGLEFLIGHT_LOCKS: Dict[str, threading.Lock] = {}
_YT_AUDIO_SINGLEFLIGHT_REFS: Dict[str, int] = {}
_YTDLP_SOURCE_FAIL_COOLDOWN_LOCK = threading.Lock()
_YTDLP_SOURCE_FAIL_UNTIL_EPOCH: Dict[str, float] = {}
_LRC_RESULT_CACHE_LOCK = threading.Lock()
_LRC_RESULT_CACHE: "OrderedDict[str, Tuple[float, Dict[str, Any], str]]" = OrderedDict()


def _apply_runtime_cache_policy(*, disable_cache: bool) -> Any:
    """
    Temporarily disable Step1 in-process cache layers for true cold runs.
    Returns a restore callable.
    """
    if not disable_cache:
        return (lambda: None)

    global YTDLP_SEARCH_CACHE_TTL_SEC
    global YTDLP_SEARCH_DISK_CACHE_TTL_SEC
    global LRC_RESULT_CACHE_TTL_SEC
    global YTDLP_AUDIO_DISK_CACHE_ENABLED
    global MP3_ENABLE_CACHED_ID_FASTPATH

    prev = {
        "YTDLP_SEARCH_CACHE_TTL_SEC": float(YTDLP_SEARCH_CACHE_TTL_SEC),
        "YTDLP_SEARCH_DISK_CACHE_TTL_SEC": float(YTDLP_SEARCH_DISK_CACHE_TTL_SEC),
        "LRC_RESULT_CACHE_TTL_SEC": float(LRC_RESULT_CACHE_TTL_SEC),
        "YTDLP_AUDIO_DISK_CACHE_ENABLED": bool(YTDLP_AUDIO_DISK_CACHE_ENABLED),
        "MP3_ENABLE_CACHED_ID_FASTPATH": bool(MP3_ENABLE_CACHED_ID_FASTPATH),
    }

    YTDLP_SEARCH_CACHE_TTL_SEC = 0.0
    YTDLP_SEARCH_DISK_CACHE_TTL_SEC = 0.0
    LRC_RESULT_CACHE_TTL_SEC = 0.0
    YTDLP_AUDIO_DISK_CACHE_ENABLED = False
    MP3_ENABLE_CACHED_ID_FASTPATH = False

    with _YT_SEARCH_IDS_CACHE_LOCK:
        _YT_SEARCH_IDS_CACHE.clear()
    with _LRC_RESULT_CACHE_LOCK:
        _LRC_RESULT_CACHE.clear()

    log("STEP1", "Runtime cache bypass enabled for this run", YELLOW)

    def _restore() -> None:
        global YTDLP_SEARCH_CACHE_TTL_SEC
        global YTDLP_SEARCH_DISK_CACHE_TTL_SEC
        global LRC_RESULT_CACHE_TTL_SEC
        global YTDLP_AUDIO_DISK_CACHE_ENABLED
        global MP3_ENABLE_CACHED_ID_FASTPATH
        YTDLP_SEARCH_CACHE_TTL_SEC = float(prev["YTDLP_SEARCH_CACHE_TTL_SEC"])
        YTDLP_SEARCH_DISK_CACHE_TTL_SEC = float(prev["YTDLP_SEARCH_DISK_CACHE_TTL_SEC"])
        LRC_RESULT_CACHE_TTL_SEC = float(prev["LRC_RESULT_CACHE_TTL_SEC"])
        YTDLP_AUDIO_DISK_CACHE_ENABLED = bool(prev["YTDLP_AUDIO_DISK_CACHE_ENABLED"])
        MP3_ENABLE_CACHED_ID_FASTPATH = bool(prev["MP3_ENABLE_CACHED_ID_FASTPATH"])

    return _restore


def _normalize_proxy_url(raw: str) -> str:
    token = " ".join((raw or "").split()).strip().strip(",;")
    if not token:
        return ""
    if token.startswith("http://") or token.startswith("https://"):
        return token.rstrip("/")
    parts = token.split(":")
    if len(parts) >= 4:
        host = parts[0].strip()
        port = parts[1].strip()
        user = parts[2].strip()
        pwd = ":".join(parts[3:]).strip()
        if host and port and user and pwd:
            return "http://%s:%s@%s:%s" % (user, pwd, host, port)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return "http://%s:%s" % (parts[0].strip(), parts[1].strip())
    return token


def _proxy_pool_values() -> List[str]:
    global _PROXY_POOL_CACHE, _PROXY_POOL_CACHE_AT_MONO
    with _PROXY_CURSOR_LOCK:
        now = time.monotonic()
        if _PROXY_POOL_CACHE_AT_MONO > 0.0 and (now - _PROXY_POOL_CACHE_AT_MONO) < YTDLP_PROXY_POOL_REFRESH_SEC:
            return list(_PROXY_POOL_CACHE)

    values: List[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        p = _normalize_proxy_url(raw)
        if not p or p in seen:
            return
        seen.add(p)
        values.append(p)

    if YTDLP_PROXY:
        _add(YTDLP_PROXY)
    if YTDLP_PROXY_POOL_RAW:
        chunks = re.split(r"[\n,\s;|]+", YTDLP_PROXY_POOL_RAW)
        for c in chunks:
            _add(c)
    if YTDLP_PROXY_POOL_FILE:
        try:
            with Path(YTDLP_PROXY_POOL_FILE).open("r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    if len(values) >= YTDLP_PROXY_POOL_MAX_ENTRIES:
                        break
                    _add(line)
        except Exception as e:
            log("PROXY", "Failed to read proxy pool file: %s (%s)" % (YTDLP_PROXY_POOL_FILE, e), YELLOW)
    if (
        YTDLP_PROXY_RANGE_HOST
        and YTDLP_PROXY_RANGE_PORT
        and YTDLP_PROXY_RANGE_USER_PREFIX
        and YTDLP_PROXY_RANGE_PASSWORD
        and YTDLP_PROXY_RANGE_END >= YTDLP_PROXY_RANGE_START
    ):
        for idx in range(YTDLP_PROXY_RANGE_START, YTDLP_PROXY_RANGE_END + 1):
            if len(values) >= YTDLP_PROXY_POOL_MAX_ENTRIES:
                break
            _add(
                "http://%s%s:%s@%s:%s"
                % (
                    YTDLP_PROXY_RANGE_USER_PREFIX,
                    idx,
                    YTDLP_PROXY_RANGE_PASSWORD,
                    YTDLP_PROXY_RANGE_HOST,
                    YTDLP_PROXY_RANGE_PORT,
                )
            )
    if YTDLP_PROXY_POOL_URL:
        try:
            resp = requests.get(YTDLP_PROXY_POOL_URL, timeout=YTDLP_PROXY_POOL_FETCH_TIMEOUT_SEC)
            if resp.status_code == 200:
                for line in (resp.text or "").splitlines():
                    if len(values) >= YTDLP_PROXY_POOL_MAX_ENTRIES:
                        break
                    _add(line)
            else:
                log("PROXY", "Proxy pool URL returned HTTP %s" % resp.status_code, YELLOW)
        except Exception as e:
            log("PROXY", "Failed to fetch proxy pool URL: %s" % e, YELLOW)
    with _PROXY_CURSOR_LOCK:
        _PROXY_POOL_CACHE = list(values)
        _PROXY_POOL_CACHE_AT_MONO = time.monotonic()
    return values


def _mask_proxy(proxy: str) -> str:
    p = (proxy or "").strip()
    if not p:
        return ""
    return re.sub(r"//([^:@/]+):([^@/]+)@", r"//***:***@", p)


def _proxy_retry_budget() -> int:
    pool_size = len(_proxy_pool_values()) or 1
    if pool_size == 1:
        only_proxy = ""
        try:
            only_proxy = (_proxy_pool_values() or [""])[0]
        except Exception:
            only_proxy = ""
        low = (only_proxy or "").lower()
        looks_like_provider_rotation = (
            YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES
            or ("p.webshare.io" in low and ("rotate" in low or "-rotating" in low))
        )
        if looks_like_provider_rotation:
            return max(1, min(YTDLP_PROXY_MAX_ROTATIONS, YTDLP_PROXY_PER_CALL_ATTEMPTS))
    return max(1, min(pool_size, YTDLP_PROXY_MAX_ROTATIONS, YTDLP_PROXY_PER_CALL_ATTEMPTS))


def _proxy_from_cmd(cmd: List[str]) -> str:
    for i, token in enumerate(cmd):
        if token == "--proxy" and i + 1 < len(cmd):
            return str(cmd[i + 1] or "").strip()
    return ""


def _proxy_state(proxy: str) -> Dict[str, Any]:
    if not proxy:
        return {}
    state = _PROXY_STATE.get(proxy)
    if state is None:
        state = {"consecutive_failures": 0, "cooldown_until": 0.0, "successes": 0}
        _PROXY_STATE[proxy] = state
    return state


def _current_proxy() -> str:
    pool = _proxy_pool_values()
    if not pool:
        return ""
    with _PROXY_CURSOR_LOCK:
        now = time.monotonic()
        ready_indices = [
            idx for idx, proxy in enumerate(pool)
            if float(_proxy_state(proxy).get("cooldown_until") or 0.0) <= now
        ]
        if not ready_indices:
            # All proxies are cooling down: pick the one that becomes available first.
            best_idx = min(
                range(len(pool)),
                key=lambda i: float(_proxy_state(pool[i]).get("cooldown_until") or 0.0),
            )
            return pool[best_idx]

        policy = (YTDLP_PROXY_SELECTION_POLICY or "random").strip().lower()
        if policy in {"rr", "round_robin", "round-robin"}:
            global _PROXY_CURSOR
            start = _PROXY_CURSOR % len(pool)
            best_idx: Optional[int] = None
            for offset in range(len(pool)):
                idx = (start + offset) % len(pool)
                if idx in ready_indices:
                    best_idx = idx
                    break
            if best_idx is None:
                best_idx = ready_indices[0]
            _PROXY_CURSOR = (best_idx + 1) % len(pool)
            return pool[best_idx]

        # Default: randomized selection with light weighting toward healthier proxies.
        weighted: List[Tuple[int, float]] = []
        for idx in ready_indices:
            state = _proxy_state(pool[idx])
            failures = int(state.get("consecutive_failures") or 0)
            successes = int(state.get("successes") or 0)
            failure_penalty = 1.0 / (1.0 + float(failures))
            success_bonus = 1.0 + min(5, successes) * 0.02
            weighted.append((idx, max(0.05, failure_penalty * success_bonus)))
        indices = [idx for idx, _weight in weighted]
        weights = [weight for _idx, weight in weighted]
        try:
            selected_idx = random.choices(indices, weights=weights, k=1)[0]
        except Exception:
            selected_idx = random.choice(indices)
        return pool[selected_idx]


def _mark_proxy_success(proxy: str) -> None:
    if not proxy:
        return
    with _PROXY_CURSOR_LOCK:
        state = _proxy_state(proxy)
        state["consecutive_failures"] = 0
        state["cooldown_until"] = 0.0
        state["successes"] = int(state.get("successes") or 0) + 1


def _mark_proxy_failure(proxy: str, *, reason: str) -> None:
    if not proxy:
        return
    with _PROXY_CURSOR_LOCK:
        state = _proxy_state(proxy)
        failures = int(state.get("consecutive_failures") or 0) + 1
        state["consecutive_failures"] = failures
        cooldown = min(
            YTDLP_PROXY_FAILURE_MAX_COOLDOWN_SEC,
            YTDLP_PROXY_FAILURE_BASE_COOLDOWN_SEC * (2 ** min(4, failures - 1)),
        )
        state["cooldown_until"] = time.monotonic() + cooldown
    log(
        "PROXY",
        "Cooling proxy (%s) for %.0fs after %s"
        % (_mask_proxy(proxy), float(cooldown), reason),
        YELLOW,
    )


def _rotate_proxy(reason: str) -> None:
    pool = _proxy_pool_values()
    if len(pool) <= 1:
        if pool and YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES:
            log(
                "PROXY",
                "Provider-side rotating endpoint active; retrying endpoint for %s (%s)"
                % (reason, _mask_proxy(pool[0])),
                YELLOW,
            )
        return
    global _PROXY_CURSOR
    with _PROXY_CURSOR_LOCK:
        _PROXY_CURSOR = (_PROXY_CURSOR + 1) % len(pool)
        nxt = pool[_PROXY_CURSOR]
    log("PROXY", "Rotated proxy (%s) -> %s" % (reason, _mask_proxy(nxt)), YELLOW)


def _should_rotate_proxy_on_error(text: str) -> bool:
    if not YTDLP_PROXY_ROTATE_ON_BOTCHECK:
        return False
    low = (text or "").lower()
    return (
        "confirm you" in low and "not a bot" in low
        or "sign in to confirm you" in low
        or "captcha" in low
        or "too many requests" in low
        or "http error 429" in low
        or "cookie_refresh_required" in low
    )


def _parse_ytdlp_headers(raw: str) -> List[str]:
    out: List[str] = []
    for part in (raw or "").split("|"):
        header = part.strip()
        if not header or ":" not in header:
            continue
        k, v = header.split(":", 1)
        if not k.strip() or not v.strip():
            continue
        out.append("%s:%s" % (k.strip(), v.strip()))
    return out


YTDLP_EXTRA_HEADERS = _parse_ytdlp_headers(YTDLP_EXTRA_HEADERS_RAW)


def _ytdlp_missing_message() -> str:
    attempted = " ".join(YTDLP_CMD) if YTDLP_CMD else "yt-dlp"
    return (
        "yt-dlp is not installed or not on PATH (attempted: %s). "
        "Install dependencies inside a venv with "
        "'python3 -m venv .venv && source .venv/bin/activate && python -m pip install -r requirements.txt' "
        "or set MIXTERIOSO_YTDLP_CMD to a working command."
    ) % attempted


def _collect_ytdlp_diagnostics(stderr: str, stdout: str) -> str:
    combined = "\n".join(part for part in [(stderr or ""), (stdout or "")] if part)
    lines = [ln.rstrip() for ln in combined.splitlines() if ln.strip()]
    if not lines:
        return ""

    terms = (
        "error:",
        "warning:",
        "[source]",
        "[debug]",
        "signature",
        "nsig",
        "jsc",
        "player",
        "forbidden",
        "captcha",
        "429",
    )
    focused = [ln for ln in lines if any(t in ln.lower() for t in terms)]
    picked = focused if focused else lines
    return "\n".join(picked[-YTDLP_DIAG_LINES:])

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _reset_slug(slug: str, *, nuke: bool) -> None:
    """
    Remove step1 artifacts. If nuke=True, remove any file in mp3s/timings/meta
    starting with "<slug>".
    """
    targets = [
        TIMINGS_DIR / f"{slug}.lrc",
        META_DIR / f"{slug}.step1.json",
    ]

    # Source audio can now be mp3 or native audio extensions in fast mode.
    for p in MP3_DIR.glob(f"{slug}.*"):
        if p.is_file():
            targets.append(p)

    for p in targets:
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    if nuke:
        for d in (MP3_DIR, TIMINGS_DIR, META_DIR):
            if not d.exists():
                continue
            for p in d.glob(f"{slug}*"):
                try:
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()
                except Exception:
                    pass


def _is_valid_source_id(value: str) -> bool:
    return bool(_source_ID_RE.fullmatch((value or "").strip()))


def _prune_source_fail_cooldown_locked(*, now_epoch: float) -> None:
    stale = [vid for vid, until in _YTDLP_SOURCE_FAIL_UNTIL_EPOCH.items() if float(until or 0.0) <= now_epoch]
    for vid in stale:
        _YTDLP_SOURCE_FAIL_UNTIL_EPOCH.pop(vid, None)

    max_entries = int(max(100, YTDLP_SOURCE_FAIL_COOLDOWN_MAX_ENTRIES))
    if len(_YTDLP_SOURCE_FAIL_UNTIL_EPOCH) <= max_entries:
        return
    survivors = sorted(
        _YTDLP_SOURCE_FAIL_UNTIL_EPOCH.items(),
        key=lambda item: float(item[1]),
        reverse=True,
    )[:max_entries]
    _YTDLP_SOURCE_FAIL_UNTIL_EPOCH.clear()
    for vid, until in survivors:
        _YTDLP_SOURCE_FAIL_UNTIL_EPOCH[vid] = float(until)


def _source_fail_cooldown_remaining(source_id: str) -> float:
    if (not YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED) or (not _is_valid_source_id(source_id)):
        return 0.0
    now_epoch = time.time()
    with _YTDLP_SOURCE_FAIL_COOLDOWN_LOCK:
        _prune_source_fail_cooldown_locked(now_epoch=now_epoch)
        until = float(_YTDLP_SOURCE_FAIL_UNTIL_EPOCH.get(source_id) or 0.0)
        if until <= now_epoch:
            _YTDLP_SOURCE_FAIL_UNTIL_EPOCH.pop(source_id, None)
            return 0.0
        return max(0.0, until - now_epoch)


def _mark_source_fail_cooldown(source_id: str, *, reason: str) -> None:
    if (not YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED) or (not _is_valid_source_id(source_id)):
        return
    now_epoch = time.time()
    cooldown_secs = max(5.0, float(YTDLP_SOURCE_FAIL_COOLDOWN_SEC))
    until = now_epoch + cooldown_secs
    with _YTDLP_SOURCE_FAIL_COOLDOWN_LOCK:
        prior = float(_YTDLP_SOURCE_FAIL_UNTIL_EPOCH.get(source_id) or 0.0)
        _YTDLP_SOURCE_FAIL_UNTIL_EPOCH[source_id] = max(prior, until)
        _prune_source_fail_cooldown_locked(now_epoch=now_epoch)
    log(
        "MP3",
        "Cooling source id=%s for %.0fs after auth/bot-check failure: %s"
        % (source_id, cooldown_secs, str(reason or "")[:120]),
        YELLOW,
    )


def _parse_mp3_pinned_ids(raw: str) -> Dict[str, Tuple[str, ...]]:
    out: Dict[str, Tuple[str, ...]] = {}
    for part in str(raw or "").split(","):
        chunk = part.strip()
        if not chunk or ":" not in chunk:
            continue
        slug_part, ids_part = chunk.split(":", 1)
        slug_key = slugify(slug_part.strip())
        if not slug_key:
            continue
        ids: List[str] = []
        for token in re.split(r"[|\s]+", ids_part.strip()):
            vid = token.strip()
            if _is_valid_source_id(vid) and vid not in ids:
                ids.append(vid)
        if ids:
            out[slug_key] = tuple(ids)
    return out


MP3_PINNED_IDS = _parse_mp3_pinned_ids(MP3_PINNED_IDS_RAW)
MP3_PINNED_ID_SET = {
    vid
    for ids in MP3_PINNED_IDS.values()
    for vid in ids
    if _is_valid_source_id(vid)
}
_HOT_QUERY_SLUG_ALIASES: Dict[str, Tuple[str, ...]] = {
    "let_it_be": ("the_beatles_let_it_be",),
    "the_beatles_let_it_be": ("let_it_be",),
    "john_frusciante_god": (),
    "john_frusciante_the_past_recedes": (),
}


def _read_cached_id_from_slug_meta(slug: str) -> Optional[str]:
    meta_path = META_DIR / ("%s.step1.json" % slug)
    if not meta_path.exists():
        return None

    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None

    candidates: List[str] = []
    seen: set[str] = set()

    def _append(raw: Any) -> None:
        if raw is None:
            return
        if isinstance(raw, dict):
            for key in (
                "source_id",
                "youtube_id",
                "id",
                "video_id",
                "url",
                "watch_url",
                "webpage_url",
            ):
                _append(raw.get(key))
            return
        if isinstance(raw, (list, tuple)):
            for item in raw:
                _append(item)
            return

        token = str(raw or "").strip()
        if not token:
            return
        if token not in seen:
            seen.add(token)
            candidates.append(token)
        extracted = _extract_source_id_from_url(token)
        if extracted and extracted not in seen:
            seen.add(extracted)
            candidates.append(extracted)

    _append(data.get("source_id"))
    _append(data.get("youtube_id"))
    _append(data.get("youtube_picked"))
    _append(data.get("video_id"))
    _append(data.get("id"))

    for candidate in candidates:
        if _is_valid_source_id(candidate):
            return candidate
    return None


def _cached_ids_for_slug(slug: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    slug_key = slugify(slug or "")
    slug_keys: List[str] = [slug_key]
    for alias in _HOT_QUERY_SLUG_ALIASES.get(slug_key, ()):
        if alias not in slug_keys:
            slug_keys.append(alias)

    for candidate_slug in slug_keys:
        for pinned in MP3_PINNED_IDS.get(candidate_slug, ()):
            if pinned in seen:
                continue
            seen.add(pinned)
            out.append(pinned)

        slug_vid = _read_cached_id_from_slug_meta(candidate_slug)
        if slug_vid and slug_vid not in seen:
            seen.add(slug_vid)
            out.append(slug_vid)

    return out


def _resolve_step1_audio_from_meta(slug: str) -> Optional[Path]:
    meta_path = META_DIR / ("%s.step1.json" % slug)
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    audio_path_raw = str(payload.get("audio_path") or "").strip()
    if audio_path_raw:
        p = Path(audio_path_raw)
        if p.exists() and p.is_file():
            return p

    mp3_path_raw = str(payload.get("mp3") or "").strip()
    if mp3_path_raw:
        p = Path(mp3_path_raw)
        if p.exists() and p.is_file():
            return p
    return None


def _find_any_step1_audio(slug: str) -> Optional[Path]:
    # Prefer known extensions first, then fall back to any slug-matching file.
    preferred_exts = (".mp3", ".m4a", ".webm", ".opus", ".ogg", ".wav", ".aac")
    for ext in preferred_exts:
        p = MP3_DIR / ("%s%s" % (slug, ext))
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            return p

    # Cache stat() and use max() instead of sort (only need best candidate)
    candidates_with_stat: List[Tuple[Path, Any]] = []
    for p in MP3_DIR.glob("%s.*" % slug):
        if p.is_file():
            stat_result = p.stat()
            if stat_result.st_size > 0:  # Only consider non-empty files
                candidates_with_stat.append((p, stat_result))

    if not candidates_with_stat:
        return None

    # Use max() instead of sort since we only need the newest/largest
    best = max(candidates_with_stat, key=lambda x: (x[1].st_mtime, x[1].st_size))
    return best[0]


def _resolve_existing_step1_audio(slug: str, mp3_hint: Path) -> Optional[Path]:
    if mp3_hint.exists() and mp3_hint.stat().st_size > 0:
        return mp3_hint
    meta_audio = _resolve_step1_audio_from_meta(slug)
    if meta_audio is not None:
        return meta_audio
    return _find_any_step1_audio(slug)


def _hot_alias_candidates_for_slug(slug: str) -> List[str]:
    slug_key = slugify(slug or "")
    out: List[str] = []
    for alias in _HOT_QUERY_SLUG_ALIASES.get(slug_key, ()):
        alias_key = slugify(alias)
        if alias_key and alias_key != slug_key and alias_key not in out:
            out.append(alias_key)
    return out


def _symlink_or_copy(src: Path, dst: Path) -> bool:
    try:
        if dst.exists() or dst.is_symlink():
            dst.unlink()
    except Exception:
        pass

    try:
        rel = os.path.relpath(str(src), start=str(dst.parent))
        dst.symlink_to(rel)
        return True
    except Exception:
        pass

    try:
        shutil.copy2(str(src), str(dst))
        return True
    except Exception:
        return False


def _hydrate_hot_alias_artifacts(slug: str, query: str, *, lrc_path: Path, mp3_path: Path, meta_path: Path) -> bool:
    """
    Materialize missing step1 artifacts for known hot-query aliases.
    This lets alias queries reuse completed artifacts without network work.
    """
    if slugify(slug) not in _HOT_QUERY_SLUG_ALIASES:
        return False
    if lrc_path.exists() and meta_path.exists() and _resolve_existing_step1_audio(slug, mp3_path) is not None:
        return False

    for alias_slug in _hot_alias_candidates_for_slug(slug):
        alias_lrc = TIMINGS_DIR / ("%s.lrc" % alias_slug)
        alias_meta = META_DIR / ("%s.step1.json" % alias_slug)
        alias_audio = _resolve_existing_step1_audio(alias_slug, MP3_DIR / ("%s.mp3" % alias_slug))
        if (not alias_lrc.exists()) or (not alias_meta.exists()) or (alias_audio is None):
            continue

        changed = False
        try:
            ensure_dir(lrc_path.parent)
            ensure_dir(mp3_path.parent)
            ensure_dir(meta_path.parent)
        except Exception:
            pass

        if not lrc_path.exists():
            try:
                shutil.copy2(str(alias_lrc), str(lrc_path))
                changed = True
            except Exception:
                pass

        if _resolve_existing_step1_audio(slug, mp3_path) is None:
            if _symlink_or_copy(alias_audio, mp3_path):
                changed = True

        if not meta_path.exists():
            payload = _read_step1_meta_payload(alias_meta)
            payload["slug"] = slug
            payload["query"] = query
            payload["source_slug_alias"] = alias_slug
            payload["lrc"] = str(lrc_path)
            payload["mp3"] = str(mp3_path)
            resolved_audio = _resolve_existing_step1_audio(slug, mp3_path)
            if resolved_audio is not None:
                payload["audio_path"] = str(resolved_audio)
            if not _is_valid_source_id(str(payload.get("source_id") or "")):
                alias_source_id = _read_cached_id_from_slug_meta(alias_slug)
                if alias_source_id:
                    payload["source_id"] = alias_source_id
            try:
                meta_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
                changed = True
            except Exception:
                pass

        if changed and lrc_path.exists() and meta_path.exists() and _resolve_existing_step1_audio(slug, mp3_path) is not None:
            log("STEP1", "Hydrated alias artifacts slug=%s from alias=%s" % (slug, alias_slug), CYAN)
            return True

    return False


def _normalize_lrc_cache_key(query: str) -> str:
    return " ".join((query or "").split()).strip().lower()


def _lrc_result_cache_get(query: str) -> Optional[Tuple[Dict[str, Any], str]]:
    ttl = float(LRC_RESULT_CACHE_TTL_SEC)
    if ttl <= 0:
        return None
    key = _normalize_lrc_cache_key(query)
    if not key:
        return None
    now = time.monotonic()
    with _LRC_RESULT_CACHE_LOCK:
        entry = _LRC_RESULT_CACHE.get(key)
        if entry is None:
            return None
        expires_at, info, lrc_text = entry
        if expires_at <= now:
            _LRC_RESULT_CACHE.pop(key, None)
            return None
        _LRC_RESULT_CACHE.move_to_end(key)
        return dict(info), str(lrc_text)


def _lrc_result_cache_set(query: str, info: Dict[str, Any], lrc_text: str) -> None:
    ttl = float(LRC_RESULT_CACHE_TTL_SEC)
    if ttl <= 0:
        return
    key = _normalize_lrc_cache_key(query)
    if not key:
        return
    provider = str((info or {}).get("provider") or "").strip().lower()
    if provider in {"step1_fallback_pseudo", ""}:
        return
    line_count = _lrc_line_count(str(lrc_text or ""))
    if line_count < int(max(1, LRC_MIN_LINES)):
        return
    payload = dict(info or {})
    payload["provider"] = provider
    now = time.monotonic()
    with _LRC_RESULT_CACHE_LOCK:
        _LRC_RESULT_CACHE[key] = (now + ttl, payload, str(lrc_text or ""))
        _LRC_RESULT_CACHE.move_to_end(key)
        while len(_LRC_RESULT_CACHE) > int(LRC_RESULT_CACHE_MAX_ENTRIES):
            _LRC_RESULT_CACHE.popitem(last=False)


def _read_step1_meta_payload(meta_path: Path) -> Dict[str, Any]:
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _should_refresh_cached_lyrics(meta_path: Path, lrc_path: Path) -> bool:
    if not bool(LRC_REUSE_REFRESH_ON_WEAK):
        return False
    if not lrc_path.exists():
        return True
    payload = _read_step1_meta_payload(meta_path)
    lrc_fetch = payload.get("lrc_fetch") if isinstance(payload, dict) else None
    if not isinstance(lrc_fetch, dict):
        return False
    provider = str(lrc_fetch.get("provider") or "").strip().lower()
    if provider in {"", "step1_fallback_pseudo"}:
        return True
    score = lrc_fetch.get("score")
    try:
        score_val = float(score)
    except Exception:
        score_val = None
    if provider.startswith("lrclib_search") and score_val is not None and score_val < float(LRC_LOW_CONFIDENCE_MIN_SCORE):
        return True
    try:
        line_count = _lrc_line_count(lrc_path.read_text(encoding="utf-8"))
    except Exception:
        return True
    if line_count < int(max(1, LRC_MIN_LINES)):
        return True
    return False


def _dynamic_search_budget(*, remaining_sec: float, base_search_n: int) -> Dict[str, int | str]:
    """
    Tune search effort based on remaining time budget.

    Profiles:
    - full: default behavior (best success probability)
    - mid: reduced probe counts (balanced)
    - tight: minimal probes to avoid timeout thrash
    """
    search_n = max(1, int(base_search_n))
    id_attempt_limit = max(1, int(MP3_MAX_ID_ATTEMPTS))
    variant_limit = max(1, int(MP3_MAX_QUERY_VARIANTS))
    search_query_limit = max(1, int(MP3_MAX_SEARCH_QUERY_VARIANTS))

    if not MP3_ENABLE_DYNAMIC_SEARCH_BUDGET:
        return {
            "profile": "disabled",
            "search_n": search_n,
            "id_attempt_limit": id_attempt_limit,
            "variant_limit": variant_limit,
            "search_query_limit": search_query_limit,
        }

    if remaining_sec <= MP3_DYNAMIC_TIGHT_REMAINING_SEC:
        return {
            "profile": "tight",
            "search_n": min(search_n, MP3_DYNAMIC_TIGHT_MAX_SEARCH_N),
            "id_attempt_limit": min(id_attempt_limit, 2),
            "variant_limit": min(variant_limit, 2),
            "search_query_limit": 1,
        }

    if remaining_sec <= MP3_DYNAMIC_MID_REMAINING_SEC:
        mid_variant_limit = max(2, min(variant_limit, (variant_limit + 1) // 2))
        return {
            "profile": "mid",
            "search_n": min(search_n, MP3_DYNAMIC_MID_MAX_SEARCH_N),
            "id_attempt_limit": min(id_attempt_limit, 4),
            "variant_limit": mid_variant_limit,
            "search_query_limit": 1,
        }

    return {
        "profile": "full",
        "search_n": search_n,
        "id_attempt_limit": id_attempt_limit,
        "variant_limit": variant_limit,
        "search_query_limit": search_query_limit,
    }


def _writable_cookies_path() -> str:
    """
    yt-dlp may persist cookies on shutdown. Secret Manager mounts are read-only,
    so copy cookies into /tmp once per process and reuse that path.
    """
    global _RUNTIME_COOKIES_PATH
    if _RUNTIME_COOKIES_PATH is not None:
        return _RUNTIME_COOKIES_PATH

    source = (YTDLP_COOKIES_PATH or "").strip()
    if not source:
        _RUNTIME_COOKIES_PATH = ""
        return ""

    src = Path(source)
    if not src.exists():
        _RUNTIME_COOKIES_PATH = source
        return source

    dst = Path("/tmp") / ("ytcookies-%d.txt" % os.getpid())
    try:
        shutil.copy2(src, dst)
        dst.chmod(0o600)
        _RUNTIME_COOKIES_PATH = str(dst)
        log("MP3", "Copied cookies to writable path: %s" % dst, CYAN)
    except Exception as e:
        log("MP3", "Could not copy cookies to /tmp (%s); using original path" % e, YELLOW)
        _RUNTIME_COOKIES_PATH = source
    return _RUNTIME_COOKIES_PATH


# ─────────────────────────────────────────────
# LRC (robust)
# ─────────────────────────────────────────────

_LRCLIB_BASE = "https://lrclib.net/api"
_LRCLIB_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (YTDLP_UA or "mixterioso/1.0 (step1_fetch)"),
}

# Connection pooling for lrclib API requests (50-200ms speedup per request)
_lrclib_session: Optional[requests.Session] = None
_lrclib_session_lock = threading.Lock()

def _get_lrclib_session() -> requests.Session:
    """Get or create a shared requests.Session for connection pooling."""
    global _lrclib_session
    if _lrclib_session is None:
        with _lrclib_session_lock:
            if _lrclib_session is None:
                _lrclib_session = requests.Session()
                _lrclib_session.headers.update(_LRCLIB_HEADERS)
    return _lrclib_session

def _exponential_backoff_sleep(attempt: int, base: float = 0.1, max_sleep: float = 2.0) -> None:
    """
    Sleep with exponential backoff and jitter.
    Formula: min(base * 2^attempt + jitter, max_sleep)
    """
    delay = min(base * (2 ** attempt), max_sleep)
    jitter = random.uniform(0, delay * 0.1)  # Add up to 10% jitter
    time.sleep(delay + jitter)


def _time_remaining(deadline_monotonic: Optional[float]) -> Optional[float]:
    if deadline_monotonic is None:
        return None
    return max(0.0, float(deadline_monotonic - time.monotonic()))


def _deadline_exceeded(deadline_monotonic: Optional[float]) -> bool:
    remaining = _time_remaining(deadline_monotonic)
    return bool(remaining is not None and remaining <= 0.0)


def _bounded_timeout(
    default_timeout: float,
    deadline_monotonic: Optional[float],
    *,
    minimum: float = LRC_BOUNDED_MIN_REQUEST_TIMEOUT_SEC,
) -> Optional[float]:
    if deadline_monotonic is None:
        return max(minimum, float(default_timeout))
    remaining = _time_remaining(deadline_monotonic)
    if remaining is None or remaining <= 0.0:
        return None
    remaining_f = float(remaining)
    if remaining_f < float(minimum):
        return None
    return min(float(default_timeout), remaining_f)


_SPLIT_DASH_RE = re.compile(r"\s+[-–—]\s+")
_FEAT_RE = re.compile(r"\b(feat\.?|ft\.?|featuring)\b.*$", re.IGNORECASE)
_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*")
_BRACKET_RE = re.compile(r"\s*\[[^\]]*\]\s*")
_WS_RE = re.compile(r"\s+")
# Pre-compile regex for _normalize_key hot path (called 30-60x per LRCLIB search)
_NORMALIZE_ALPHANUM_RE = re.compile(r"[^a-z0-9\s]")
# Pre-compile regex for WebVTT tag removal in caption cleaning
_WEBVTT_TAG_RE = re.compile(r"<[^>]+>")
_NOISY_ARTIST_WORD_RE = re.compile(r"\b(topic|vevo|official|channel)\b", re.IGNORECASE)
_NOISY_ARTIST_TAIL_RE = re.compile(r"(?:\s*[-–—:|/]*)?(?:topic|vevo|official|channel)\s*$", re.IGNORECASE)
_LYRICS_SUFFIX_RE = re.compile(
    r"(?:\s*[-–—:|/(),\[\]]\s*)?"
    r"(?:(?:official|original|music)\s+)?"
    r"(?:lyric(?:s)?(?:\s+video)?|with\s+lyrics?|video\s+lyrics?|letra(?:s)?)\s*$",
    re.IGNORECASE,
)


def _clean_title(s: str) -> str:
    s = (s or "").strip()
    s = _PAREN_RE.sub(" ", s)
    s = _BRACKET_RE.sub(" ", s)
    s = _FEAT_RE.sub("", s).strip()
    s = s.replace("'", "'").replace(""", '"').replace(""", '"')
    s = _WS_RE.sub(" ", s).strip()
    return s


def _normalize_key(s: str) -> str:
    s = _clean_title(s).lower()
    # Fold diacritics so Spanish/French titles map to stable cache/hot-query keys.
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = _NORMALIZE_ALPHANUM_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _sanitize_search_query(query: str) -> str:
    """
    Lightweight normalization before remote lookups/search probes.
    """
    q = html.unescape(str(query or ""))
    q = unicodedata.normalize("NFKC", q)
    q = q.replace("–", "-").replace("—", "-")
    q = _WS_RE.sub(" ", q).strip()
    return q


def _strip_lyrics_title_noise(s: str) -> str:
    base = _clean_title(s)
    if not base:
        return ""
    out = base
    prev = ""
    while out and out != prev:
        prev = out
        out = _LYRICS_SUFFIX_RE.sub("", out).strip()
        out = _clean_title(out)
    return out or base


def _normalize_artist_key(s: str) -> str:
    cleaned = _clean_title(s)
    if not cleaned:
        return ""
    cleaned = _NOISY_ARTIST_TAIL_RE.sub("", cleaned).strip()
    base = _normalize_key(cleaned)
    if not base:
        return ""
    parts = [p for p in base.split() if p and p not in {"topic", "vevo", "official", "channel"}]
    return "".join(parts)


def _is_noisy_artist_label(s: str) -> bool:
    cleaned = _clean_title(s)
    if not cleaned:
        return False
    return bool(_NOISY_ARTIST_WORD_RE.search(cleaned) or _NOISY_ARTIST_TAIL_RE.search(cleaned))


def _is_handle_like_artist_label(s: str) -> bool:
    cleaned = _clean_title(s)
    if not cleaned:
        return False
    if re.search(r"\d", cleaned):
        return True
    if any(ch in cleaned for ch in ("_", "@", ".")):
        return True
    parts = cleaned.split()
    return len(parts) == 1 and len(cleaned) >= 10


def _is_lyrics_like_title(title: str) -> bool:
    t = " ".join((title or "").split()).strip()
    if not t:
        return False
    return bool(_LYRICS_TITLE_RE.search(t))


def _query_requests_lyrics_version(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    return bool(_LYRICS_TITLE_RE.search(q))


def _query_has_explicit_audio_variant_intent(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    return bool(_AUDIO_VARIANT_INTENT_QUERY_RE.search(q))


def _should_prefer_broad_direct_fast_format(*, query: str = "", source_label: str = "") -> bool:
    if _query_requests_lyrics_version(query):
        return True
    if _is_lyrics_like_title(source_label):
        return True
    return False


def _query_requests_live_version(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    return bool(_LIVE_QUERY_INTENT_RE.search(q))


def _is_live_like_title(title: str) -> bool:
    t = " ".join((title or "").split()).strip()
    if not t:
        return False
    return bool(
        _LIVE_TITLE_BRACKET_RE.search(t)
        or _LIVE_TITLE_CONTEXT_RE.search(t)
        or _LIVE_TITLE_TERM_RE.search(t)
        or _LIVE_TITLE_ANY_RE.search(t)
        or _LIVE_TITLE_DATE_RE.search(t)
    )


def _query_bypasses_official_audio_preference(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    return bool(_OFFICIAL_AUDIO_BYPASS_QUERY_RE.search(q))


def _official_audio_penalty(title: str) -> int:
    t = " ".join((title or "").split()).strip()
    if not t:
        return 3
    if _OFFICIAL_AUDIO_TITLE_RE.search(t):
        return 0
    if _AUDIO_TITLE_RE.search(t) and (not _OFFICIAL_VIDEO_TITLE_RE.search(t)) and (not _is_lyrics_like_title(t)):
        return 1
    if _OFFICIAL_VIDEO_TITLE_RE.search(t):
        return 3
    return 2


def _float_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _int_or_none(value: Any) -> Optional[int]:
    try:
        out = int(value)
    except Exception:
        return None
    return out


def _duration_rank_value(duration_sec: Optional[float], target_duration_sec: float) -> Tuple[int, int]:
    if duration_sec is None or duration_sec <= 0.0:
        return (1, 10**9)
    delta_ms = int(round(abs(float(duration_sec) - float(target_duration_sec)) * 1000.0))
    return (0, max(0, delta_ms))


def _prioritize_search_ids_for_query(
    query: str,
    ids: List[str],
    title_by_id: Dict[str, str],
    *,
    duration_by_id: Optional[Dict[str, float]] = None,
    view_count_by_id: Optional[Dict[str, int]] = None,
    target_duration_sec: Optional[float] = None,
) -> List[str]:
    if not ids:
        return []

    wants_live = _query_requests_live_version(query)
    ordered_ids: List[str] = list(ids)
    prioritized_ids: List[str] = ordered_ids

    # For fast mode we intentionally keep relevance ordering from ytsearch
    # and only apply a light live/non-live filter.
    if wants_live:
        live_only = [vid for vid in ordered_ids if _is_live_like_title(title_by_id.get(vid, ""))]
        if live_only:
            prioritized_ids = live_only
        return _rank_source_ids_for_query(prioritized_ids, query)

    if MP3_PREFER_NON_LIVE_VERSION:
        non_live_only = [vid for vid in ordered_ids if not _is_live_like_title(title_by_id.get(vid, ""))]
        if non_live_only:
            prioritized_ids = non_live_only

    duration_target = _float_or_none(target_duration_sec)
    if duration_target is not None and duration_target > 0.0:
        ranked_by_duration = sorted(
            enumerate(prioritized_ids),
            key=lambda row: (
                _duration_rank_value((duration_by_id or {}).get(row[1]), float(duration_target)),
                _official_audio_penalty(title_by_id.get(row[1], "")),
                -int((view_count_by_id or {}).get(row[1], 0) or 0),
                row[0],
            ),
        )
        prioritized_ids = [vid for _idx, vid in ranked_by_duration]
    return _rank_source_ids_for_query(prioritized_ids, query)


def _maybe_split_artist_title(query: str) -> Tuple[str, str]:
    q = (query or "").strip()
    if not q:
        return "", ""
    parts = _SPLIT_DASH_RE.split(q, maxsplit=1)
    if len(parts) == 2:
        a, t = parts[0].strip(), parts[1].strip()
        # heuristic: if user likely typed "title - artist", flip when it smells like artist on right
        if len(a.split()) <= 2 and len(t.split()) > 3 and ("," in a or "&" in a):
            return _clean_title(t), _clean_title(a)
        return _clean_title(a), _clean_title(t)
    return "", ""


def _artist_title_query(artist: str, title: str) -> str:
    a = _clean_title(artist)
    t = _clean_title(title)
    if a and t:
        return f"{a} - {t}"
    return a or t


def _fallback_seed_lines(query: str, *, hint_artist: str, hint_title: str) -> List[str]:
    artist = _clean_title(hint_artist)
    title = _clean_title(hint_title)
    if artist and title:
        artist, title = _normalize_canonical_artist_title(artist, title)
    elif title:
        title = _strip_lyrics_title_noise(title)

    if not artist and not title:
        query_artist, query_title = _maybe_split_artist_title(query)
        artist, title = _normalize_canonical_artist_title(query_artist, query_title)
    if not title:
        title = _strip_lyrics_title_noise(_clean_title(query))

    if artist and (_is_noisy_artist_label(artist) or _is_handle_like_artist_label(artist)):
        artist = ""

    seed_primary = " - ".join([x for x in [artist, title] if x]).strip() or title
    if not seed_primary:
        seed_primary = "Lyrics unavailable"
    seed_secondary = title or seed_primary

    out: List[str] = []
    for raw in (seed_primary, seed_secondary):
        line = " ".join(str(raw or "").split()).strip()
        if not line:
            continue
        if line in out:
            continue
        out.append(line)
    if not out:
        out = ["Lyrics unavailable"]
    return out


def _seq_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, _normalize_key(a), _normalize_key(b)).ratio()


def _token_set(text: str) -> set[str]:
    normalized = _normalize_key(text)
    if not normalized:
        return set()
    return {tok for tok in normalized.split() if tok}


def _query_similarity_score(query: str, row_artist: str, row_title: str) -> float:
    q = " ".join((query or "").split()).strip()
    if not q:
        return 0.0
    artist = str(row_artist or "").strip()
    title = str(row_title or "").strip()
    candidates = [
        title,
        ("%s %s" % (artist, title)).strip(),
        ("%s %s" % (title, artist)).strip(),
    ]
    best = 0.0
    for cand in candidates:
        if not cand:
            continue
        best = max(best, _seq_ratio(q, cand))
    return best


def _query_token_overlap_score(query: str, row_artist: str, row_title: str) -> float:
    q_tokens = _token_set(query)
    if not q_tokens:
        return 0.0
    row_tokens = _token_set("%s %s" % (row_artist, row_title))
    if not row_tokens:
        return 0.0
    return float(len(q_tokens & row_tokens)) / float(len(q_tokens))


_SOURCE_TITLE_PENALTY_RE = re.compile(
    r"\b(lyrics?|lyric video|video oficial|official video|live|karaoke|instrumental|cover|tribute|reaction)\b",
    re.IGNORECASE,
)
_SOURCE_TITLE_BONUS_RE = re.compile(
    r"\b(official audio|audio oficial|topic)\b",
    re.IGNORECASE,
)
_SOURCE_CHANNEL_TOPIC_RE = re.compile(r"\btopic\b", re.IGNORECASE)


def _source_metadata_quality_adjustment(
    *,
    expected_artist: str,
    expected_title: str,
    candidate_artist: str,
    candidate_title: str,
    candidate_raw_title: str,
    candidate_raw_uploader: str,
    candidate_raw_channel: str,
) -> float:
    adjustment = 0.0
    raw_title = str(candidate_raw_title or "").strip()
    raw_uploader = str(candidate_raw_uploader or "").strip()
    raw_channel = str(candidate_raw_channel or "").strip()

    if raw_title and _SOURCE_TITLE_PENALTY_RE.search(raw_title):
        adjustment -= 0.12
    if raw_title and _SOURCE_TITLE_BONUS_RE.search(raw_title):
        adjustment += 0.08

    expected_artist_norm = _normalize_key(expected_artist)
    expected_title_norm = _normalize_key(expected_title)
    candidate_artist_norm = _normalize_key(candidate_artist)
    candidate_title_norm = _normalize_key(candidate_title)
    channel_norm = _normalize_key(raw_channel or raw_uploader)

    if expected_artist_norm and channel_norm:
        if _SOURCE_CHANNEL_TOPIC_RE.search(raw_channel) and expected_artist_norm in channel_norm:
            adjustment += 0.08
        elif _seq_ratio(expected_artist_norm, channel_norm) >= 0.85:
            adjustment += 0.06

    if expected_title_norm and candidate_title_norm and candidate_title_norm == expected_title_norm:
        adjustment += 0.03
    if expected_artist_norm and candidate_artist_norm and candidate_artist_norm == expected_artist_norm:
        adjustment += 0.03

    return adjustment


def _source_expected_artist_title(query: str) -> Tuple[str, str]:
    q = " ".join((query or "").split()).strip()
    if not q:
        return "", ""
    artist, title = _maybe_split_artist_title(query)
    if artist and title:
        return _normalize_canonical_artist_title(artist, title)
    if len(q.split()) < 3 or len(q) < 8:
        return "", ""

    hint = _yt_search_top_result_hint(
        query,
        timeout_sec=min(float(MP3_SOURCE_RANKING_TIMEOUT_SEC), float(YTDLP_SEARCH_TIMEOUT)),
    )
    hint_artist = _clean_title(str(hint.get("artist") or ""))
    hint_title = _clean_title(str(hint.get("title") or ""))
    if hint_artist or hint_title:
        return _normalize_canonical_artist_title(hint_artist, hint_title)
    return "", ""


def _source_metadata_match_score(
    *,
    query: str,
    expected_artist: str,
    expected_title: str,
    candidate_artist: str,
    candidate_title: str,
) -> float:
    query_score = _query_similarity_score(query, candidate_artist, candidate_title)
    title_score = _seq_ratio(expected_title, candidate_title) if (expected_title and candidate_title) else 0.0
    artist_score = _seq_ratio(expected_artist, candidate_artist) if (expected_artist and candidate_artist) else 0.0
    overlap_score = _query_token_overlap_score(query, candidate_artist, candidate_title)

    if expected_artist and expected_title:
        return (title_score * 0.55) + (artist_score * 0.25) + (query_score * 0.15) + (overlap_score * 0.05)
    if expected_title:
        return (title_score * 0.65) + (query_score * 0.25) + (overlap_score * 0.10)
    return (query_score * 0.70) + (overlap_score * 0.30)


def _rank_source_ids_for_query(ids: List[str], query: str) -> List[str]:
    unique_ids: List[str] = []
    seen: set[str] = set()
    for token in ids:
        vid = str(token or "").strip()
        if not vid:
            continue
        if vid in seen:
            continue
        seen.add(vid)
        unique_ids.append(vid)

    if not bool(MP3_SOURCE_RANKING_ENABLED):
        return unique_ids
    if len(unique_ids) <= 1:
        return unique_ids

    expected_artist, expected_title = _source_expected_artist_title(query)
    if not expected_artist and not expected_title:
        return unique_ids

    inspect_n = min(len(unique_ids), int(max(1, MP3_SOURCE_RANKING_MAX_IDS)))
    scored: List[Tuple[str, float, int]] = []
    score_by_id: Dict[str, float] = {}
    for idx, vid in enumerate(unique_ids[:inspect_n]):
        hint: Dict[str, str] = {}
        if _is_valid_source_id(vid):
            hint = _yt_video_metadata_hint(
                vid,
                timeout_sec=min(float(MP3_SOURCE_RANKING_TIMEOUT_SEC), float(YTDLP_SEARCH_TIMEOUT)),
            )
        cand_artist = _clean_title(str(hint.get("artist") or ""))
        cand_title = _clean_title(str(hint.get("title") or ""))
        score = _source_metadata_match_score(
            query=query,
            expected_artist=expected_artist,
            expected_title=expected_title,
            candidate_artist=cand_artist,
            candidate_title=cand_title,
        )
        quality_adjust = _source_metadata_quality_adjustment(
            expected_artist=expected_artist,
            expected_title=expected_title,
            candidate_artist=cand_artist,
            candidate_title=cand_title,
            candidate_raw_title=str(hint.get("raw_title") or cand_title or ""),
            candidate_raw_uploader=str(hint.get("raw_uploader") or ""),
            candidate_raw_channel=str(hint.get("raw_channel") or ""),
        )
        total_score = float(score) + float(quality_adjust)
        score_by_id[vid] = total_score
        scored.append((vid, total_score, idx))

    if not scored:
        return unique_ids

    ranked = sorted(scored, key=lambda item: (-item[1], item[2]))
    top_score = float(ranked[0][1])
    second_score = float(ranked[1][1]) if len(ranked) > 1 else -1.0
    margin = top_score - second_score
    current_first_id = unique_ids[0]
    current_first_score = float(score_by_id.get(current_first_id, 0.0))
    top_id = ranked[0][0]
    improvement_over_first = top_score - current_first_score if top_id != current_first_id else 0.0
    promoted_by_margin = margin >= float(MP3_SOURCE_RANKING_MIN_MARGIN)
    promoted_by_improvement = improvement_over_first >= float(MP3_SOURCE_RANKING_MIN_IMPROVEMENT_OVER_FIRST)
    if (top_score < float(MP3_SOURCE_RANKING_MIN_SCORE)) or (not (promoted_by_margin or promoted_by_improvement)):
        return unique_ids

    ranked_ids = [vid for vid, _score, _idx in ranked]
    for vid in unique_ids:
        if vid not in ranked_ids:
            ranked_ids.append(vid)

    log(
        "MP3",
        (
            "source ranking promoted id=%s score=%.2f margin=%.2f improve_first=%.2f reason=%s query=%s"
            % (
                ranked_ids[0],
                top_score,
                margin,
                improvement_over_first,
                "margin" if promoted_by_margin else "improvement",
                query,
            )
        ),
        CYAN,
    )
    return ranked_ids


def _source_video_matches_expected(video_id: str, *, expected_artist: str, expected_title: str) -> bool:
    vid = str(video_id or "").strip()
    if not _is_valid_source_id(vid):
        return False
    if not expected_artist and not expected_title:
        return True

    hint = _yt_oembed_video_hint(
        vid,
        timeout_sec=min(2.0, float(MP3_SOURCE_RANKING_TIMEOUT_SEC), float(YTDLP_SEARCH_TIMEOUT)),
    )
    if not hint.get("title"):
        hint = _yt_video_metadata_hint(
            vid,
            timeout_sec=min(float(MP3_SOURCE_RANKING_TIMEOUT_SEC), float(YTDLP_SEARCH_TIMEOUT)),
        )
    candidate_artist = _clean_title(str(hint.get("artist") or ""))
    candidate_title = _clean_title(str(hint.get("title") or ""))
    if not candidate_artist and not candidate_title:
        return False

    title_score = _seq_ratio(expected_title, candidate_title) if (expected_title and candidate_title) else 0.0
    artist_score = _seq_ratio(expected_artist, candidate_artist) if (expected_artist and candidate_artist) else 0.0
    query = _artist_title_query(expected_artist, expected_title)
    query_score = _query_similarity_score(query, candidate_artist, candidate_title)

    if expected_title:
        if title_score >= 0.70 and (not expected_artist or artist_score >= 0.35):
            return True
        if query_score >= 0.85:
            return True
        return False
    return query_score >= 0.80 or artist_score >= 0.70


def _audio_query_from_lrc_info(default_query: str, lrc_info: Dict[str, Any]) -> str:
    if not bool(STEP1_USE_LRC_AUDIO_QUERY):
        return default_query
    if not bool((lrc_info or {}).get("ok")):
        return default_query

    provider = str((lrc_info or {}).get("provider") or "").strip().lower()
    if provider == "step1_fallback_pseudo":
        return default_query

    lrc_artist = _clean_title(str((lrc_info or {}).get("artist") or ""))
    lrc_title = _clean_title(str((lrc_info or {}).get("title") or ""))
    if not lrc_title:
        return default_query

    query_artist, _query_title = _maybe_split_artist_title(default_query)
    if not lrc_artist and query_artist:
        lrc_artist = query_artist
    lrc_artist, lrc_title = _normalize_canonical_artist_title(lrc_artist, lrc_title)
    resolved = _artist_title_query(lrc_artist, lrc_title)
    return resolved or default_query


def _query_needs_duration_disambiguation(query: str) -> bool:
    artist, title = _maybe_split_artist_title(query)
    candidate = _clean_title(title or query)
    if not candidate:
        return False
    token_count = len([tok for tok in _normalize_key(candidate).split() if tok])
    compact_len = len(re.sub(r"[^a-z0-9]+", "", _normalize_key(candidate)))
    if token_count <= 1:
        return True
    return token_count <= 2 and compact_len <= 8


def _lrclib_row_score(
    *,
    artist: str,
    title: str,
    query: str,
    row_artist: str,
    row_title: str,
    lyric_text: str,
) -> float:
    length_score = min(1.0, len((lyric_text or "").strip()) / 6000.0)
    query_score = _query_similarity_score(query, row_artist, row_title)
    if artist and title:
        artist_score = _seq_ratio(artist, row_artist)
        title_score = _seq_ratio(title, row_title)
        return (title_score * 0.55) + (artist_score * 0.25) + (query_score * 0.10) + (length_score * 0.10)
    overlap_score = _query_token_overlap_score(query, row_artist, row_title)
    return (query_score * 0.65) + (overlap_score * 0.25) + (length_score * 0.10)


def _lrclib_duration_sec(row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(row, dict):
        return None
    duration = _float_or_none(row.get("duration"))
    if duration is None or duration <= 0.0:
        return None
    return float(round(duration, 3))


def _guess_lrc_target_duration_sec(query: str, *, timeout_sec: float = 2.5) -> Optional[float]:
    q0 = " ".join((query or "").split()).strip()
    if not q0:
        return None
    try:
        deadline_monotonic = time.monotonic() + max(0.5, float(timeout_sec))
        artist, title = _maybe_split_artist_title(q0)

        if artist and title and (not _deadline_exceeded(deadline_monotonic)):
            row = _lrclib_get_budgeted(track_name=title, artist_name=artist, deadline_monotonic=deadline_monotonic)
            duration = _lrclib_duration_sec(row)
            if duration is not None:
                return duration

        if _deadline_exceeded(deadline_monotonic):
            return None
        rows = _lrclib_search_budgeted({"q": _clean_title(q0)}, deadline_monotonic=deadline_monotonic)
        if not rows:
            return None

        best_duration: Optional[float] = None
        best_score = -1.0
        for row in rows[: max(1, int(LRC_FAST_MAX_ROWS))]:
            duration = _lrclib_duration_sec(row)
            if duration is None:
                continue
            synced_txt = str(row.get("syncedLyrics") or "").strip()
            if not synced_txt:
                continue
            row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
            row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
            score = _lrclib_row_score(
                artist=artist,
                title=title,
                query=q0,
                row_artist=row_artist,
                row_title=row_title,
                lyric_text=synced_txt,
            )
            if score > best_score:
                best_score = score
                best_duration = duration
        if best_duration is not None:
            return best_duration

        for row in rows:
            duration = _lrclib_duration_sec(row)
            if duration is not None:
                return duration
        return None
    except Exception:
        return None


def _guess_lrc_target_duration_from_path(lrc_path: Path) -> Optional[float]:
    try:
        from scripts.lrc_utils import parse_lrc

        events, _meta = parse_lrc(lrc_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None

    last_time: Optional[float] = None
    for ev in events:
        txt = str(getattr(ev, "text", ev[1] if isinstance(ev, tuple) and len(ev) >= 2 else "") or "").strip()
        if not txt:
            continue
        try:
            t = float(getattr(ev, "t", getattr(ev, "time", ev[0] if isinstance(ev, tuple) and ev else 0.0)))
        except Exception:
            continue
        if last_time is None or t > last_time:
            last_time = t
    if last_time is None or last_time <= 0.0:
        return None
    return float(round(last_time, 3))


def _wait_for_local_lrc_target_duration_sec(lrc_path: Path, *, timeout_sec: float = 0.9) -> Optional[float]:
    deadline = time.monotonic() + max(0.0, float(timeout_sec))
    while True:
        if lrc_path.exists():
            duration = _guess_lrc_target_duration_from_path(lrc_path)
            if duration is not None:
                return duration
        if time.monotonic() >= deadline:
            return None
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

def _lrclib_get_any(
    track_name: str,
    artist_name: str,
    *,
    album_name: str = "",
    duration_ms: Optional[int] = None,
    deadline_monotonic: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "track_name": track_name,
        "artist_name": artist_name,
    }
    if album_name:
        params["album_name"] = album_name
    if duration_ms is not None and duration_ms > 0:
        params["duration"] = int(duration_ms)

    url = _LRCLIB_BASE + "/get"
    session = _get_lrclib_session()
    for attempt in range(int(LRCLIB_MAX_RETRIES)):
        timeout = _bounded_timeout(LRCLIB_TIMEOUT, deadline_monotonic)
        if timeout is None:
            log("LRC", "lrclib_get budget exceeded before request", YELLOW)
            return None
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict):
                    synced = str(j.get("syncedLyrics") or "").strip()
                    plain = str(j.get("plainLyrics") or "").strip()
                    if synced or plain:
                        return j
                return None
            if r.status_code in (429, 500, 502, 503, 504):
                log("LRC", "lrclib_get transient status=%s artist=%s title=%s" % (r.status_code, artist_name, track_name), YELLOW)
                _exponential_backoff_sleep(attempt, base=0.1, max_sleep=0.8)
                continue
            return None
        except Exception as exc:
            log("LRC", "lrclib_get error artist=%s title=%s err=%s" % (artist_name, track_name, exc), YELLOW)
            _exponential_backoff_sleep(attempt, base=0.1, max_sleep=0.8)
    return None


def _lrclib_get(
    track_name: str,
    artist_name: str,
    *,
    album_name: str = "",
    duration_ms: Optional[int] = None,
    deadline_monotonic: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    row = _lrclib_get_any(
        track_name=track_name,
        artist_name=artist_name,
        album_name=album_name,
        duration_ms=duration_ms,
        deadline_monotonic=deadline_monotonic,
    )
    if not isinstance(row, dict):
        return None
    synced = str(row.get("syncedLyrics") or "").strip()
    if synced:
        return row
    return None


def _lrclib_search(params: Dict[str, Any], *, deadline_monotonic: Optional[float] = None) -> List[Dict[str, Any]]:
    url = _LRCLIB_BASE + "/search"
    session = _get_lrclib_session()
    for attempt in range(int(LRCLIB_MAX_RETRIES)):
        timeout = _bounded_timeout(LRCLIB_TIMEOUT, deadline_monotonic)
        if timeout is None:
            log("LRC", "lrclib_search budget exceeded before request params=%s" % params, YELLOW)
            return []
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, list):
                    return [x for x in j if isinstance(x, dict)]
                return []
            if r.status_code in (429, 500, 502, 503, 504):
                log("LRC", "lrclib_search transient status=%s params=%s" % (r.status_code, params), YELLOW)
                _exponential_backoff_sleep(attempt, base=0.1, max_sleep=0.8)
                continue
            return []
        except Exception as exc:
            log("LRC", "lrclib_search error params=%s err=%s" % (params, exc), YELLOW)
            _exponential_backoff_sleep(attempt, base=0.1, max_sleep=0.8)
    return []


def _lrclib_get_budgeted(
    track_name: str,
    artist_name: str,
    *,
    album_name: str = "",
    duration_ms: Optional[int] = None,
    deadline_monotonic: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    try:
        return _lrclib_get(
            track_name=track_name,
            artist_name=artist_name,
            album_name=album_name,
            duration_ms=duration_ms,
            deadline_monotonic=deadline_monotonic,
        )
    except TypeError as exc:
        # Test doubles may still use the old call signature.
        if "deadline_monotonic" not in str(exc):
            raise
        return _lrclib_get(
            track_name=track_name,
            artist_name=artist_name,
            album_name=album_name,
            duration_ms=duration_ms,
        )


def _lrclib_get_any_budgeted(
    track_name: str,
    artist_name: str,
    *,
    album_name: str = "",
    duration_ms: Optional[int] = None,
    deadline_monotonic: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    try:
        return _lrclib_get_any(
            track_name=track_name,
            artist_name=artist_name,
            album_name=album_name,
            duration_ms=duration_ms,
            deadline_monotonic=deadline_monotonic,
        )
    except TypeError as exc:
        # Test doubles may still use the old call signature.
        if "deadline_monotonic" not in str(exc):
            raise
        return _lrclib_get_any(
            track_name=track_name,
            artist_name=artist_name,
            album_name=album_name,
            duration_ms=duration_ms,
        )


def _lrclib_search_budgeted(params: Dict[str, Any], *, deadline_monotonic: Optional[float] = None) -> List[Dict[str, Any]]:
    try:
        return _lrclib_search(params, deadline_monotonic=deadline_monotonic)
    except TypeError as exc:
        # Test doubles may still use the old call signature.
        if "deadline_monotonic" not in str(exc):
            raise
        return _lrclib_search(params)


def _lrclib_get_any_relaxed_once(track_name: str, artist_name: str, *, timeout_sec: float) -> Optional[Dict[str, Any]]:
    """
    One-shot LRCLIB /get fallback that bypasses tight per-request budget logic.
    Useful when the strict budget is exhausted due transient TLS/read issues.
    """
    url = _LRCLIB_BASE + "/get"
    params: Dict[str, Any] = {
        "track_name": track_name,
        "artist_name": artist_name,
    }
    headers = dict(_LRCLIB_HEADERS)
    headers["Connection"] = "close"
    for attempt in range(int(LRC_RELAXED_RECOVERY_RETRIES)):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=max(1.0, float(timeout_sec)),
            )
            if resp.status_code != 200:
                _exponential_backoff_sleep(attempt, base=0.15, max_sleep=1.0)
                continue
            payload = resp.json() if resp.text else {}
            if not isinstance(payload, dict):
                return None
            synced = str(payload.get("syncedLyrics") or "").strip()
            plain = str(payload.get("plainLyrics") or "").strip()
            if not synced and not plain:
                return None
            return payload
        except Exception as exc:
            log(
                "LRC",
                "lrclib_get_relaxed error artist=%s title=%s attempt=%s/%s err=%s"
                % (artist_name, track_name, attempt + 1, int(LRC_RELAXED_RECOVERY_RETRIES), exc),
                YELLOW,
            )
            _exponential_backoff_sleep(attempt, base=0.15, max_sleep=1.0)
    return None


def fetch_lrclib_lrc(query: str, out_path: Path) -> bool:
    """
    Backward-compatible wrapper: fetch best syncedLyrics from LRCLIB only.
    """
    info = fetch_best_synced_lrc(query, out_path, prefer_langs=LRC_PREFER_LANGS, enable_source_fallback=False)
    return bool(info.get("ok"))


def _clean_caption_text(s: str) -> str:
    s = html.unescape(s or "")
    s = _WEBVTT_TAG_RE.sub(" ", s)  # drop WebVTT styling tags (pre-compiled)
    s = s.replace("\u200b", "")
    s = _WS_RE.sub(" ", s).strip()  # Use pre-compiled pattern
    # skip non-lyric-y content
    low = s.lower()  # Already stripped above, no need to strip() again
    if not low:
        return ""
    if low in {"[music]", "(music)", "♪", "♪♪"}:
        return ""
    if low.startswith("[") and low.endswith("]") and "music" in low:
        return ""
    return s


def _parse_timecode(ts: str) -> Optional[float]:
    ts = (ts or "").strip()
    if not ts:
        return None
    ts = ts.replace(",", ".")
    m = re.match(r"^(?:(\d{1,2}):)?(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?$", ts)
    if not m:
        return None
    h = int(m.group(1) or 0)
    mm = int(m.group(2) or 0)
    ss = int(m.group(3) or 0)
    frac = m.group(4) or "0"
    if len(frac) == 1:
        ms = int(frac) * 100
    elif len(frac) == 2:
        ms = int(frac) * 10
    else:
        ms = int(frac[:3])
    return h * 3600.0 + mm * 60.0 + ss + (ms / 1000.0)


def _parse_sub_to_cues(text: str) -> List[Tuple[float, str]]:
    """
    Parse VTT or SRT-ish text into (start_seconds, cleaned_text) cues.
    """
    cues: List[Tuple[float, str]] = []
    lines = (text or "").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip("\ufeff").strip()
        if not line:
            i += 1
            continue
        # SRT index line
        if line.isdigit():
            i += 1
            continue
        if "-->" in line:
            left = line.split("-->", 1)[0].strip()
            start = _parse_timecode(left)
            i += 1
            chunk: List[str] = []
            while i < len(lines) and lines[i].strip():
                chunk.append(lines[i].strip())
                i += 1
            txt = _clean_caption_text(" ".join(chunk))
            if start is not None and txt:
                cues.append((float(start), txt))
            continue
        i += 1
    return cues


def _format_lrc_timestamp(secs: float) -> str:
    if secs < 0:
        secs = 0.0
    total_cs = int(round(secs * 100.0))
    mm = total_cs // (60 * 100)
    rem = total_cs % (60 * 100)
    ss = rem // 100
    cs = rem % 100
    return "[%02d:%02d.%02d]" % (mm, ss, cs)


def _cues_to_lrc(cues: List[Tuple[float, str]]) -> str:
    if not cues:
        return ""
    # sort and de-dupe (but keep earliest time)
    cues_sorted = sorted(cues, key=lambda x: (x[0], x[1]))
    out_lines: List[str] = []
    last_time = -1.0
    last_text = ""
    for t, txt in cues_sorted:
        if not txt:
            continue
        if txt == last_text:
            continue
        # enforce monotonic time (min step 0.01s)
        if t <= last_time:
            t = last_time + 0.01
        out_lines.append(_format_lrc_timestamp(t) + txt)
        last_time = t
        last_text = txt
    return "\n".join(out_lines).strip() + ("\n" if out_lines else "")


def _lrc_line_count(lrc_text: str) -> int:
    return sum(1 for ln in (lrc_text or "").splitlines() if ln.strip())


def _normalize_lyric_prefix(s: str) -> str:
    base = unicodedata.normalize("NFKD", str(s or "").strip().lower())
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = re.sub(r"[^\w\s]", " ", base)
    return _WS_RE.sub(" ", base).strip()


def _first_lrc_line_text(lrc_text: str) -> str:
    for raw_line in (lrc_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^(?:\[[0-9:.]+\]\s*)+", "", line).strip()
        if line:
            return line
    return ""


def _lrc_matches_lyric_start(lrc_path: Path, lyric_start: str) -> Tuple[bool, str]:
    expected = _normalize_lyric_prefix(lyric_start)
    if not expected:
        return True, ""
    try:
        text = lrc_path.read_text(encoding="utf-8")
    except Exception:
        return False, ""
    first_line = _first_lrc_line_text(text)
    normalized_first = _normalize_lyric_prefix(first_line)
    return normalized_first.startswith(expected), first_line


def _extract_text_lines_for_pseudo_lrc(raw_text: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw_line in (raw_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        up = line.upper()
        if up == "WEBVTT" or up.startswith("NOTE"):
            continue
        if up.startswith("KIND:") or up.startswith("LANGUAGE:"):
            continue
        if up.startswith("STYLE") or up.startswith("REGION"):
            continue
        if up.startswith("X-TIMESTAMP-MAP"):
            continue
        if "-->" in line:
            continue
        if line.isdigit():
            continue
        cleaned = _clean_caption_text(line)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def _extract_plain_lyrics_lines(raw_text: str) -> List[str]:
    out: List[str] = []
    for raw_line in (raw_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Section headers are useful in plain text but usually noisy in karaoke lines.
        if line.startswith("[") and line.endswith("]") and len(line) <= 80:
            continue
        cleaned = _clean_caption_text(line)
        if not cleaned:
            continue
        out.append(cleaned)
    return out


def _pseudo_lrc_from_lines(lines: List[str], *, start_secs: float, step_secs: float, max_lines: int) -> str:
    out_lines: List[str] = []
    ts = max(0.0, float(start_secs))
    step = max(0.25, float(step_secs))
    for line in lines[: max(1, int(max_lines))]:
        txt = _clean_caption_text(line)
        if not txt:
            continue
        out_lines.append(_format_lrc_timestamp(ts) + txt)
        ts += step
    return "\n".join(out_lines).strip() + ("\n" if out_lines else "")


def _pseudo_lrc_from_text(raw_text: str) -> str:
    lines = _extract_text_lines_for_pseudo_lrc(raw_text)
    return _pseudo_lrc_from_lines(
        lines,
        start_secs=LRC_PSEUDO_START_SECS,
        step_secs=LRC_PSEUDO_STEP_SECS,
        max_lines=LRC_PSEUDO_MAX_LINES,
    )


def _ytdlp_fetch_subtitles_to_temp(query: str, *, prefer_langs: Tuple[str, ...], auto: bool) -> Tuple[List[Path], str]:
    """
    Use yt-dlp to fetch subtitles (manual or auto) for the top ytsearch result.
    Returns (list_of_sub_files, diagnostics_on_error).
    """
    with tempfile.TemporaryDirectory(prefix="mixterioso-subs-") as td:
        tdir = Path(td)
        langs = []
        for lang in prefer_langs:
            l = (lang or "").strip()
            if not l:
                continue
            langs.append(l)
            # common variants
            if l == "en":
                langs += ["en-US", "en-GB"]
            if l == "es":
                langs += ["es-419", "es-ES", "es-MX"]
        # stable de-dupe
        seen = set()
        uniq_langs = []
        for l in langs:
            if l not in seen:
                seen.add(l)
                uniq_langs.append(l)
        lang_arg = ",".join(uniq_langs) if uniq_langs else "en,es"

        outtmpl = str(tdir / "%(id)s.%(ext)s")
        direct = _direct_source_source_from_query(query)
        if direct is not None:
            source = direct[1]
        else:
            source = "ytsearch%d:%s" % (YTDLP_SEARCH_SPAN, query)

        timeout_sec = LRC_YT_CAPTIONS_DIRECT_TIMEOUT if direct is not None else LRC_YT_CAPTIONS_TIMEOUT

        extractor_attempts: List[str] = []
        if YTDLP_EXTRACTOR_ARGS:
            extractor_attempts.append(str(YTDLP_EXTRACTOR_ARGS))
        else:
            extractor_attempts.append("")
        if YTDLP_FALLBACK_EXTRACTOR_ARGS and str(YTDLP_FALLBACK_EXTRACTOR_ARGS) not in extractor_attempts:
            extractor_attempts.append(str(YTDLP_FALLBACK_EXTRACTOR_ARGS))

        diagnostics: List[str] = []
        for extractor_args in extractor_attempts:
            cmd: List[str] = [*YTDLP_CMD]
            if YTDLP_NO_WARNINGS:
                cmd.append("--no-warnings")
            if YTDLP_VERBOSE:
                cmd.append("--verbose")
            cmd += ["--skip-download", "--force-ipv4", "--socket-timeout", str(YTDLP_SOCKET_TIMEOUT)]
            cmd += ["--retries", str(YTDLP_RETRIES)]
            cmd += ["--sub-format", "vtt/srt/best"]
            cmd += ["--sub-lang", lang_arg]
            cmd += ["-o", outtmpl]
            cmd += ["--match-filter", "is_live != True"]
            cmd += ["--ignore-errors", "--max-downloads", "1"]

            if auto:
                cmd += ["--write-auto-subs"]
            else:
                cmd += ["--write-subs"]

            if YTDLP_UA:
                cmd += ["--user-agent", str(YTDLP_UA)]
            for hdr in YTDLP_EXTRA_HEADERS:
                cmd += ["--add-headers", hdr]
            if extractor_args:
                cmd += ["--extractor-args", extractor_args]
            proxy = _current_proxy()
            if proxy:
                cmd += ["--proxy", str(proxy)]

            cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())
            if cookies_configured:
                cookies_path = _writable_cookies_path()
                if cookies_path:
                    cmd += ["--cookies", cookies_path]

            cmd += [source]

            log("LRC", "yt-dlp subtitles (%s): %s" % ("auto" if auto else "manual", " ".join(cmd)), CYAN)

            try:
                p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                _mark_proxy_failure(proxy, reason="yt_subtitles_timeout")
                diagnostics.append("yt-dlp subtitles timed out after %.1fs" % timeout_sec)
                continue
            except FileNotFoundError:
                return [], _ytdlp_missing_message()

            if p.returncode != 0:
                msg = _collect_ytdlp_diagnostics(p.stderr or "", p.stdout or "")
                _mark_proxy_failure(proxy, reason="yt_subtitles_failed")
                diagnostics.append(msg or "yt-dlp subtitles failed")
                if _should_rotate_proxy_on_error(msg):
                    _rotate_proxy("yt_subtitles")
                continue

            # yt-dlp will write into temp dir; collect and return by copying paths list out (caller reads text)
            subs = []
            for ext in ("vtt", "srt"):
                subs.extend(list(tdir.glob("*.%s" % ext)))
                subs.extend(list(tdir.glob("*.%s.%s" % ("*", ext))))  # defensive, though glob doesn't support like that

            if not subs:
                diagnostics.append("yt-dlp subtitles produced no subtitle files")
                continue
            _mark_proxy_success(proxy)

            stable_paths: List[Path] = []
            for s in subs:
                try:
                    if not s.exists() or s.stat().st_size == 0:
                        continue
                    dst = Path("/tmp") / ("mixterioso-sub-%d-%d-%s" % (os.getpid(), time.time_ns(), s.name))
                    shutil.copy2(s, dst)
                    stable_paths.append(dst)
                except Exception:
                    continue

            if stable_paths:
                return stable_paths, ""
            diagnostics.append("yt-dlp subtitles produced only empty subtitle files")

        if diagnostics:
            return [], "\n".join(diagnostics[-12:])
        return [], ""


def _extract_lang_from_sub_path(p: Path) -> str:
    name = p.name
    parts = name.split(".")
    if len(parts) >= 3:
        # ... .<lang>.<ext>
        return parts[-2]
    return ""


def _pick_best_sub_file(paths: List[Path], prefer_langs: Tuple[str, ...]) -> Optional[Path]:
    if not paths:
        return None

    def lang_rank(lang: str) -> int:
        if not lang:
            return 99
        lang_l = lang.lower()
        for idx, pref in enumerate(prefer_langs):
            pref_l = pref.lower()
            if lang_l == pref_l or lang_l.startswith(pref_l + "-") or lang_l.startswith(pref_l + "_"):
                return idx
        # English/Spanish fuzzy fallback
        if lang_l.startswith("en"):
            return 10
        if lang_l.startswith("es"):
            return 11
        return 99

    best = None
    best_key = None
    for p in paths:
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore")
            cues = _parse_sub_to_cues(txt)
            n_chars = sum(len(t) for _, t in cues)
            lang = _extract_lang_from_sub_path(p)
            key = (lang_rank(lang), -n_chars, -len(cues))
            if best_key is None or key < best_key:
                best_key = key
                best = p
        except Exception:
            continue
    return best


def _try_source_captions_lrc(query: str, out_path: Path, *, prefer_langs: Tuple[str, ...]) -> Optional[Dict[str, Any]]:
    """
    Fallback: fetch source captions (manual first, then auto), convert to LRC.
    Only used when LRCLIB fails.
    """
    def _attempt_mode(auto: bool) -> Optional[Dict[str, Any]]:
        paths, diag = _ytdlp_fetch_subtitles_to_temp(query, prefer_langs=prefer_langs, auto=auto)
        if diag:
            for ln in diag.splitlines()[-15:]:
                log("LRC", "subs diag: %s" % ln, YELLOW)
        best = _pick_best_sub_file(paths, prefer_langs)
        if not best:
            return None

        try:
            raw = best.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None

        cues = _parse_sub_to_cues(raw)
        lrc_text = _cues_to_lrc(cues)
        line_count = _lrc_line_count(lrc_text)
        timing_mode = "native"

        if line_count < LRC_MIN_LINES and LRC_ENABLE_TEXT_PSEUDO_FALLBACK:
            pseudo_lrc = _pseudo_lrc_from_text(raw)
            pseudo_lines = _lrc_line_count(pseudo_lrc)
            if pseudo_lines >= LRC_MIN_LINES:
                lrc_text = pseudo_lrc
                line_count = pseudo_lines
                timing_mode = "pseudo"
                log("LRC", "Using pseudo-timed captions fallback (%s) for %s lines" % (query, pseudo_lines), YELLOW)

        if line_count < LRC_MIN_LINES:
            return None

        return {
            "provider": "source_captions",
            "mode": "auto" if auto else "manual",
            "timing_mode": timing_mode,
            "sub_file": str(best),
            "lines": int(line_count),
            "lrc_text": lrc_text,
        }

    def _commit_lrc(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        lrc_text = str(candidate.get("lrc_text") or "")
        if not lrc_text:
            return None
        try:
            ensure_dir(out_path.parent)
            out_path.write_text(lrc_text, encoding="utf-8")
        except Exception:
            return None
        out = dict(candidate)
        out.pop("lrc_text", None)
        return out

    modes = [False, True]  # manual and auto
    if LRC_PARALLEL_CAPTION_MODES <= 1:
        for auto in modes:
            got = _attempt_mode(auto)
            if not got:
                continue
            committed = _commit_lrc(got)
            if committed:
                return committed
        return None

    max_workers = min(len(modes), int(LRC_PARALLEL_CAPTION_MODES))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_attempt_mode, auto): auto for auto in modes}
        for fut in concurrent.futures.as_completed(futures):
            try:
                got = fut.result()
            except Exception:
                got = None
            if not got:
                continue
            committed = _commit_lrc(got)
            if not committed:
                continue
            for other in futures:
                if other is fut:
                    continue
                other.cancel()
            return committed
    return None


def fetch_best_synced_lrc(
    query: str,
    out_path: Path,
    *,
    prefer_langs: Tuple[str, ...] = LRC_PREFER_LANGS,
    enable_source_fallback: bool = True,
) -> Dict[str, Any]:
    """
    Robust synced LRC fetcher.

    Strategy:
    1) LRCLIB /get by (artist,title) when parseable
    2) LRCLIB /search with multiple query variants and stronger ranking
    3) Optional fallback: source captions via yt-dlp (manual subs, then auto subs) → converted to LRC

    Returns a dict containing {ok: bool, provider: str, ...} and writes LRC to out_path on success.
    """
    q0 = (query or "").strip()
    if not q0:
        return {"ok": False, "provider": "", "reason": "empty query"}
    cached = _lrc_result_cache_get(q0)
    if cached is not None:
        cached_info, cached_lrc_text = cached
        try:
            ensure_dir(out_path.parent)
            out_path.write_text(cached_lrc_text, encoding="utf-8")
            out = dict(cached_info or {})
            out["ok"] = True
            out["provider"] = str(out.get("provider") or "lrc_cache")
            out["cache_hit"] = True
            return out
        except Exception:
            pass
    deadline_monotonic = time.monotonic() + float(LRC_TOTAL_TIMEOUT_SEC)

    artist, title = _maybe_split_artist_title(q0)
    hot_artist_title_applied = False
    if not artist or not title:
        hot_artist, hot_title = _hot_query_artist_title(q0)
        if hot_artist and hot_title:
            artist, title = hot_artist, hot_title
            hot_artist_title_applied = True
    if (not artist or not title) and len(q0.split()) >= 3 and (not _deadline_exceeded(deadline_monotonic)):
        # Use source's top-result metadata early to reduce LRCLIB ambiguity churn.
        hint = _yt_search_top_result_hint(q0, timeout_sec=min(LRC_METADATA_HINT_TIMEOUT_SEC, YTDLP_SEARCH_TIMEOUT))
        hinted_artist, hinted_title = _normalize_canonical_artist_title(
            str(hint.get("artist") or ""),
            str(hint.get("title") or ""),
        )
        if hinted_artist and hinted_title:
            artist, title = hinted_artist, hinted_title
            log("LRC", "Using source metadata hint artist=%s title=%s" % (artist, title), CYAN)
    # Query hints we can try for LRCLIB search
    variants: List[Dict[str, Any]] = []
    base_q = _clean_title(q0)

    # some users enter "Artist - Title (Live)", so we expand a few de-noised variants
    denoised = _clean_title(base_q)
    just_words = _WS_RE.sub(" ", denoised).strip()

    # language nudges (low-cost) + typo/truncation recovery probes
    nudges = [just_words, just_words + " lyrics", just_words + " letra"]
    for recovered in _build_trailing_token_recovery_variants(base_q):
        if recovered not in nudges:
            nudges.append(recovered)

    # /get fast path when we have both
    if artist and title:
        got = _lrclib_get_budgeted(track_name=title, artist_name=artist, deadline_monotonic=deadline_monotonic)
        synced_text = str((got or {}).get("syncedLyrics") or "").strip()
        if synced_text:
            try:
                ensure_dir(out_path.parent)
                lrc_text = synced_text + "\n"
                out_path.write_text(lrc_text, encoding="utf-8")
                info = {"ok": True, "provider": "lrclib_get", "artist": artist, "title": title}
                _lrc_result_cache_set(q0, info, lrc_text)
                return info
            except Exception as e:
                return {"ok": False, "provider": "lrclib_get", "reason": "write_failed:%s" % e}

        # fallback /search constrained
        variants.append({"track_name": title, "artist_name": artist, "q": "%s %s" % (artist, title)})

    # generic /search variants
    for q in nudges:
        variants.append({"q": q})
        if artist and title:
            variants.append({"track_name": title, "artist_name": artist, "q": q})
        # if we only parsed one side in future, keep options minimal

    # stable de-dupe (json-serialized)
    seen = set()
    uniq_variants: List[Dict[str, Any]] = []
    for v in variants:
        key = json.dumps(v, sort_keys=True, ensure_ascii=True)
        if key not in seen:
            seen.add(key)
            uniq_variants.append(v)
    uniq_variants = uniq_variants[: max(1, int(LRC_LRCLIB_MAX_VARIANTS))]

    # rank rows by length + match quality
    best_row: Optional[Dict[str, Any]] = None
    best_row_score = -1.0
    best_plain_row: Optional[Dict[str, Any]] = None
    best_plain_score = -1.0

    if LRC_LRCLIB_PARALLELISM > 1 and len(uniq_variants) > 1:
        max_workers = max(1, min(int(LRC_LRCLIB_PARALLELISM), len(uniq_variants)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut_map = {
                ex.submit(_lrclib_search_budgeted, v, deadline_monotonic=deadline_monotonic): v
                for v in uniq_variants
            }
            for fut in concurrent.futures.as_completed(fut_map):
                if _deadline_exceeded(deadline_monotonic):
                    break
                try:
                    rows = fut.result()
                except Exception:
                    rows = []
                if not rows:
                    continue

                for row in rows:
                    row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
                    row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
                    if hot_artist_title_applied and artist and row_artist and _seq_ratio(artist, row_artist) < 0.40:
                        continue

                    synced_txt = row.get("syncedLyrics")
                    if isinstance(synced_txt, str):
                        synced_txt_stripped = synced_txt.strip()  # Cache strip() result
                        if synced_txt_stripped:
                            if (not artist and not title) and (
                                _query_token_overlap_score(base_q, row_artist, row_title)
                                < float(LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP)
                            ):
                                continue
                            score = _lrclib_row_score(
                                artist=artist,
                                title=title,
                                query=base_q,
                                row_artist=row_artist,
                                row_title=row_title,
                                lyric_text=synced_txt_stripped,
                            )
                            if score > best_row_score:
                                best_row_score = score
                                best_row = row

                    plain_txt = row.get("plainLyrics")
                    if LRC_ENABLE_PLAIN_LYRICS_FALLBACK and isinstance(plain_txt, str):
                        plain_txt_stripped = plain_txt.strip()  # Cache strip() result
                        if plain_txt_stripped:
                            if (not artist and not title) and (
                                _query_token_overlap_score(base_q, row_artist, row_title)
                                < float(LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP)
                            ):
                                continue
                            plain_score = _lrclib_row_score(
                                artist=artist,
                                title=title,
                                query=base_q,
                                row_artist=row_artist,
                                row_title=row_title,
                                lyric_text=plain_txt_stripped,
                            )
                            if plain_score > best_plain_score:
                                best_plain_score = plain_score
                                best_plain_row = row
                # if we already have a very good match, stop early
                if best_row is not None and best_row_score >= 0.82:
                    break
    else:
        for v in uniq_variants:
            if _deadline_exceeded(deadline_monotonic):
                break
            rows = _lrclib_search_budgeted(v, deadline_monotonic=deadline_monotonic)
            if not rows:
                continue

            for row in rows:
                row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
                row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
                if hot_artist_title_applied and artist and row_artist and _seq_ratio(artist, row_artist) < 0.40:
                    continue

                synced_txt = row.get("syncedLyrics")
                if isinstance(synced_txt, str):
                    synced_txt_stripped = synced_txt.strip()  # Cache strip() result
                    if synced_txt_stripped:
                        if (not artist and not title) and (
                            _query_token_overlap_score(base_q, row_artist, row_title)
                            < float(LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP)
                        ):
                            continue
                        score = _lrclib_row_score(
                            artist=artist,
                            title=title,
                            query=base_q,
                            row_artist=row_artist,
                            row_title=row_title,
                            lyric_text=synced_txt_stripped,
                        )
                        if score > best_row_score:
                            best_row_score = score
                            best_row = row

                plain_txt = row.get("plainLyrics")
                if LRC_ENABLE_PLAIN_LYRICS_FALLBACK and isinstance(plain_txt, str):
                    plain_txt_stripped = plain_txt.strip()  # Cache strip() result
                    if plain_txt_stripped:
                        if (not artist and not title) and (
                            _query_token_overlap_score(base_q, row_artist, row_title)
                            < float(LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP)
                        ):
                            continue
                        plain_score = _lrclib_row_score(
                            artist=artist,
                            title=title,
                            query=base_q,
                            row_artist=row_artist,
                            row_title=row_title,
                            lyric_text=plain_txt_stripped,
                        )
                        if plain_score > best_plain_score:
                            best_plain_score = plain_score
                            best_plain_row = row

            # if we already have a very good match, stop early
            if best_row is not None and best_row_score >= 0.82:
                break

    if best_row is not None:
        if ((not artist and not title) or hot_artist_title_applied) and best_row_score < float(LRC_LOW_CONFIDENCE_MIN_SCORE):
            log(
                "LRC",
                "Rejecting low-confidence LRCLIB row for query=%r score=%.3f"
                % (q0, float(best_row_score)),
                YELLOW,
            )
            best_row = None

    if best_row is not None:
        try:
            ensure_dir(out_path.parent)
            lrc_text = str(best_row.get("syncedLyrics") or "").strip() + "\n"
            out_path.write_text(lrc_text, encoding="utf-8")
            info = {
                "ok": True,
                "provider": "lrclib_search",
                "artist": artist,
                "title": title,
                "score": float(best_row_score),
            }
            _lrc_result_cache_set(q0, info, lrc_text)
            return info
        except Exception as e:
            return {"ok": False, "provider": "lrclib_search", "reason": "write_failed:%s" % e}

    # Artist-constrained fuzzy fallback:
    # helps truncated titles like "john frusciante - go" -> "God".
    if artist and title:
        artist_rows: List[Dict[str, Any]] = []
        if not _deadline_exceeded(deadline_monotonic):
            artist_rows = _lrclib_search_budgeted({"artist_name": artist, "q": artist}, deadline_monotonic=deadline_monotonic)
        fuzzy_best: Optional[Dict[str, Any]] = None
        fuzzy_best_score = -1.0
        norm_title = _normalize_key(title)
        for row in artist_rows:
            synced_txt = row.get("syncedLyrics")
            if not isinstance(synced_txt, str) or not synced_txt.strip():
                continue
            row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
            if row_artist and _seq_ratio(artist, row_artist) < 0.40:
                continue
            row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
            if not row_title:
                continue
            norm_row_title = _normalize_key(row_title)
            if not norm_row_title:
                continue
            prefix_bonus = 1.0 if (norm_row_title.startswith(norm_title) or norm_title.startswith(norm_row_title)) else 0.0
            ratio = _seq_ratio(title, row_title)
            q_ratio = _seq_ratio(base_q, (row_title + " " + artist).strip())
            score = (prefix_bonus * 0.45) + (ratio * 0.40) + (q_ratio * 0.15)
            if score > fuzzy_best_score:
                fuzzy_best_score = score
                fuzzy_best = row

        if fuzzy_best is not None and fuzzy_best_score >= 0.55:
            try:
                ensure_dir(out_path.parent)
                lrc_text = str(fuzzy_best.get("syncedLyrics") or "").strip() + "\n"
                out_path.write_text(lrc_text, encoding="utf-8")
                info = {
                    "ok": True,
                    "provider": "lrclib_search_artist_fuzzy",
                    "artist": artist,
                    "title": title,
                    "score": float(fuzzy_best_score),
                }
                _lrc_result_cache_set(q0, info, lrc_text)
                return info
            except Exception as e:
                return {"ok": False, "provider": "lrclib_search_artist_fuzzy", "reason": "write_failed:%s" % e}

    # One last cheap LRCLIB /get pass for plain lyrics when synced text is unavailable.
    if artist and title and LRC_ENABLE_PLAIN_LYRICS_FALLBACK and (best_plain_row is None) and (not _deadline_exceeded(deadline_monotonic)):
        got_any = _lrclib_get_any_budgeted(
            track_name=title,
            artist_name=artist,
            deadline_monotonic=deadline_monotonic,
        )
        plain_txt = str((got_any or {}).get("plainLyrics") or "").strip()
        if plain_txt:
            plain_lrc = _pseudo_lrc_from_lines(
                _extract_plain_lyrics_lines(plain_txt),
                start_secs=LRC_PSEUDO_START_SECS,
                step_secs=LRC_PSEUDO_STEP_SECS,
                max_lines=LRC_PSEUDO_MAX_LINES,
            )
            plain_lines = _lrc_line_count(plain_lrc)
            if plain_lines >= LRC_MIN_LINES:
                try:
                    ensure_dir(out_path.parent)
                    out_path.write_text(plain_lrc, encoding="utf-8")
                    info = {
                        "ok": True,
                        "provider": "lrclib_get_plain",
                        "artist": artist,
                        "title": title,
                        "lines": int(plain_lines),
                    }
                    _lrc_result_cache_set(q0, info, plain_lrc)
                    return info
                except Exception as e:
                    return {"ok": False, "provider": "lrclib_get_plain", "reason": "write_failed:%s" % e}

    # source captions fallback (manual → auto) converted to LRC
    if enable_source_fallback and LRC_ENABLE_YT_CAPTIONS_FALLBACK and (not _deadline_exceeded(deadline_monotonic)):
        yt_info = _try_source_captions_lrc(q0, out_path, prefer_langs=prefer_langs)
        if yt_info:
            yt_info["ok"] = True
            try:
                _lrc_result_cache_set(q0, dict(yt_info), out_path.read_text(encoding="utf-8"))
            except Exception:
                pass
            return yt_info

    if LRC_ENABLE_PLAIN_LYRICS_FALLBACK and best_plain_row is not None:
        plain_txt = str(best_plain_row.get("plainLyrics") or "").strip()
        plain_lrc = _pseudo_lrc_from_lines(
            _extract_plain_lyrics_lines(plain_txt),
            start_secs=LRC_PSEUDO_START_SECS,
            step_secs=LRC_PSEUDO_STEP_SECS,
            max_lines=LRC_PSEUDO_MAX_LINES,
        )
        plain_lines = _lrc_line_count(plain_lrc)
        if plain_lines >= LRC_MIN_LINES:
            try:
                ensure_dir(out_path.parent)
                out_path.write_text(plain_lrc, encoding="utf-8")
                info = {
                    "ok": True,
                    "provider": "lrclib_plain",
                    "artist": artist,
                    "title": title,
                    "score": float(best_plain_score),
                    "lines": int(plain_lines),
                }
                _lrc_result_cache_set(q0, info, plain_lrc)
                return info
            except Exception as e:
                return {"ok": False, "provider": "lrclib_plain", "reason": "write_failed:%s" % e}

    # source-corrected top-result hint fallback for heavily misspelled free-form queries.
    # Example: "johsdf frusciana the past recedds" -> title "The Past Recedes", artist "John Frusciante".
    if (not artist or not title) and (not _deadline_exceeded(deadline_monotonic)):
        hint = _yt_search_top_result_hint(q0, timeout_sec=min(12.0, YTDLP_SEARCH_TIMEOUT))
        hint_title = _clean_title(str(hint.get("title") or ""))
        hint_artist = _clean_title(str(hint.get("artist") or ""))

        if hint_title:
            if hint_artist:
                got = _lrclib_get_budgeted(
                    track_name=hint_title,
                    artist_name=hint_artist,
                    deadline_monotonic=deadline_monotonic,
                )
                if got:
                    try:
                        ensure_dir(out_path.parent)
                        lrc_text = str(got.get("syncedLyrics") or "").strip() + "\n"
                        out_path.write_text(lrc_text, encoding="utf-8")
                        info = {
                            "ok": True,
                            "provider": "lrclib_get_source_hint",
                            "artist": hint_artist,
                            "title": hint_title,
                        }
                        _lrc_result_cache_set(q0, info, lrc_text)
                        return info
                    except Exception as e:
                        return {"ok": False, "provider": "lrclib_get_source_hint", "reason": "write_failed:%s" % e}

            hint_queries = [hint_title]
            if hint_artist:
                hint_queries += [
                    "%s %s" % (hint_artist, hint_title),
                    "%s - %s" % (hint_artist, hint_title),
                ]

            seen_hint_q: set[str] = set()
            for hq in hint_queries:
                qq = " ".join(hq.split()).strip()
                if not qq or qq in seen_hint_q:
                    continue
                seen_hint_q.add(qq)
                if _deadline_exceeded(deadline_monotonic):
                    break
                rows = _lrclib_search_budgeted({"q": qq}, deadline_monotonic=deadline_monotonic)
                if not rows:
                    continue

                best_hint_row: Optional[Dict[str, Any]] = None
                best_hint_score = -1.0
                for row in rows:
                    synced_txt = row.get("syncedLyrics")
                    if not isinstance(synced_txt, str) or not synced_txt.strip():
                        continue
                    row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
                    row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
                    if not row_title:
                        continue
                    hint_title_score = _seq_ratio(hint_title, row_title)
                    hint_artist_score = _seq_ratio(hint_artist, row_artist) if hint_artist else 0.0
                    query_score = _seq_ratio(q0, ("%s %s" % (row_artist, row_title)).strip())
                    score = (hint_title_score * 0.65) + (hint_artist_score * 0.20) + (query_score * 0.15)
                    if score > best_hint_score:
                        best_hint_score = score
                        best_hint_row = row

                if best_hint_row is not None and best_hint_score >= 0.55:
                    try:
                        ensure_dir(out_path.parent)
                        lrc_text = str(best_hint_row.get("syncedLyrics") or "").strip() + "\n"
                        out_path.write_text(lrc_text, encoding="utf-8")
                        info = {
                            "ok": True,
                            "provider": "lrclib_search_source_hint",
                            "artist": hint_artist,
                            "title": hint_title,
                            "score": float(best_hint_score),
                        }
                        _lrc_result_cache_set(q0, info, lrc_text)
                        return info
                    except Exception as e:
                        return {"ok": False, "provider": "lrclib_search_source_hint", "reason": "write_failed:%s" % e}

    # Final resilience pass: one relaxed /get call outside strict deadline budget.
    # This helps correct artist/title queries survive transient LRCLIB network churn.
    if artist and title and LRC_RELAXED_RECOVERY_ENABLED and not hot_artist_title_applied:
        relaxed_row = _lrclib_get_any_relaxed_once(
            track_name=title,
            artist_name=artist,
            timeout_sec=LRC_RELAXED_RECOVERY_TIMEOUT_SEC,
        )
        if isinstance(relaxed_row, dict):
            synced_txt = str(relaxed_row.get("syncedLyrics") or "").strip()
            if synced_txt:
                try:
                    ensure_dir(out_path.parent)
                    lrc_text = synced_txt + "\n"
                    out_path.write_text(lrc_text, encoding="utf-8")
                    info = {
                        "ok": True,
                        "provider": "lrclib_get_relaxed",
                        "artist": artist,
                        "title": title,
                    }
                    _lrc_result_cache_set(q0, info, lrc_text)
                    return info
                except Exception as e:
                    return {"ok": False, "provider": "lrclib_get_relaxed", "reason": "write_failed:%s" % e}

            if LRC_ENABLE_PLAIN_LYRICS_FALLBACK:
                plain_txt = str(relaxed_row.get("plainLyrics") or "").strip()
                if plain_txt:
                    plain_lrc = _pseudo_lrc_from_lines(
                        _extract_plain_lyrics_lines(plain_txt),
                        start_secs=LRC_PSEUDO_START_SECS,
                        step_secs=LRC_PSEUDO_STEP_SECS,
                        max_lines=LRC_PSEUDO_MAX_LINES,
                    )
                    plain_lines = _lrc_line_count(plain_lrc)
                    if plain_lines >= LRC_MIN_LINES:
                        try:
                            ensure_dir(out_path.parent)
                            out_path.write_text(plain_lrc, encoding="utf-8")
                            info = {
                                "ok": True,
                                "provider": "lrclib_get_relaxed_plain",
                                "artist": artist,
                                "title": title,
                                "lines": int(plain_lines),
                            }
                            _lrc_result_cache_set(q0, info, plain_lrc)
                            return info
                        except Exception as e:
                            return {"ok": False, "provider": "lrclib_get_relaxed_plain", "reason": "write_failed:%s" % e}

    if _deadline_exceeded(deadline_monotonic):
        return {"ok": False, "provider": "", "reason": "lrc_timeout_budget_exceeded"}
    return {"ok": False, "provider": "", "reason": "no_synced_lyrics_found"}


def fetch_best_synced_lrc_fast(
    query: str,
    out_path: Path,
    *,
    timeout_sec: float = LRC_FAST_TOTAL_TIMEOUT_SEC,
) -> Dict[str, Any]:
    """
    Fast-fail lyrics resolver for speed modes.
    - No YouTube hint lookups
    - No captions fallback
    - Minimal LRCLIB calls
    """
    q0 = (query or "").strip()
    if not q0:
        return {"ok": False, "provider": "", "reason": "empty query"}
    if not hasattr(requests, "Session"):
        return fetch_best_synced_lrc(
            q0,
            out_path,
            prefer_langs=LRC_PREFER_LANGS,
            enable_source_fallback=False,
        )

    cached = _lrc_result_cache_get(q0)
    if cached is not None:
        cached_info, cached_lrc_text = cached
        try:
            ensure_dir(out_path.parent)
            out_path.write_text(cached_lrc_text, encoding="utf-8")
            out = dict(cached_info or {})
            out["ok"] = True
            out["provider"] = str(out.get("provider") or "lrc_cache")
            out["cache_hit"] = True
            return out
        except Exception:
            pass

    deadline_monotonic = time.monotonic() + max(0.5, float(timeout_sec))
    artist, title = _maybe_split_artist_title(q0)
    if (not artist or not title):
        split_norm = _normalize_query_from_explicit_split(q0, provider="fast_split")
        if split_norm:
            artist = _clean_title(str(split_norm.get("artist") or artist))
            title = _clean_title(str(split_norm.get("track") or split_norm.get("title") or title))

    if artist and title and (not _deadline_exceeded(deadline_monotonic)):
        got = _lrclib_get_budgeted(track_name=title, artist_name=artist, deadline_monotonic=deadline_monotonic)
        synced_text = str((got or {}).get("syncedLyrics") or "").strip()
        if synced_text:
            try:
                ensure_dir(out_path.parent)
                lrc_text = synced_text + "\n"
                out_path.write_text(lrc_text, encoding="utf-8")
                info = {"ok": True, "provider": "lrclib_get_fast", "artist": artist, "title": title}
                _lrc_result_cache_set(q0, info, lrc_text)
                return info
            except Exception as e:
                return {"ok": False, "provider": "lrclib_get_fast", "reason": "write_failed:%s" % e}

    if _deadline_exceeded(deadline_monotonic):
        return {"ok": False, "provider": "", "reason": "lrc_timeout_budget_exceeded_fast"}

    search_q = _artist_title_query(artist, title) if artist and title else _clean_title(q0)
    if not search_q:
        return {"ok": False, "provider": "", "reason": "empty_fast_query"}
    rows = _lrclib_search_budgeted({"q": search_q}, deadline_monotonic=deadline_monotonic)
    if not rows:
        return {"ok": False, "provider": "lrclib_search_fast", "reason": "no_rows"}

    best_row: Optional[Dict[str, Any]] = None
    best_score = -1.0
    query_norm = _clean_title(q0)
    for row in rows[: max(1, int(LRC_FAST_MAX_ROWS))]:
        synced_txt = str(row.get("syncedLyrics") or "").strip()
        if not synced_txt:
            continue
        row_artist = str(row.get("artistName") or row.get("artist") or "").strip()
        row_title = str(row.get("trackName") or row.get("track") or row.get("title") or "").strip()
        if (not artist and not title) and (
            _query_token_overlap_score(query_norm, row_artist, row_title) < float(LRC_QUERY_ONLY_MIN_TOKEN_OVERLAP)
        ):
            continue
        score = _lrclib_row_score(
            artist=artist,
            title=title,
            query=query_norm,
            row_artist=row_artist,
            row_title=row_title,
            lyric_text=synced_txt,
        )
        if score > best_score:
            best_score = score
            best_row = row
        if score >= 0.82:
            break

    if best_row is None:
        return {"ok": False, "provider": "lrclib_search_fast", "reason": "no_synced_lyrics_found"}

    try:
        ensure_dir(out_path.parent)
        lrc_text = str(best_row.get("syncedLyrics") or "").strip() + "\n"
        out_path.write_text(lrc_text, encoding="utf-8")
        info = {
            "ok": True,
            "provider": "lrclib_search_fast",
            "artist": artist,
            "title": title,
            "score": float(best_score),
        }
        _lrc_result_cache_set(q0, info, lrc_text)
        return info
    except Exception as e:
        return {"ok": False, "provider": "lrclib_search_fast", "reason": "write_failed:%s" % e}


# ─────────────────────────────────────────────
# yt-dlp search
# ─────────────────────────────────────────────

def _genius_search_top_hit(query: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    """
    Resolve artist/title from Genius search top hit.
    """
    token = _genius_access_token()
    q = _sanitize_search_query(query)
    if (not token) or (not q) or (not GENIUS_API_BASE):
        return {}

    effective_timeout = float(GENIUS_TIMEOUT if timeout_sec is None else max(1.0, float(timeout_sec)))
    try:
        resp = requests.get(
            f"{GENIUS_API_BASE}/search",
            params={"q": q},
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "User-Agent": (YTDLP_UA or "mixterioso/1.0 (step1_fetch)"),
            },
            timeout=effective_timeout,
        )
        if resp.status_code != 200:
            return {}
        payload = resp.json() if resp.text else {}
        response = payload.get("response") if isinstance(payload, dict) else None
        hits = response.get("hits") if isinstance(response, dict) else None
        if not isinstance(hits, list):
            return {}
        for hit in hits:
            result = hit.get("result") if isinstance(hit, dict) else None
            if not isinstance(result, dict):
                continue
            title = _clean_title(str(result.get("title") or ""))
            artist = ""
            primary_artist = result.get("primary_artist")
            if isinstance(primary_artist, dict):
                artist = _clean_title(str(primary_artist.get("name") or ""))
            if not artist:
                raw_artist = result.get("artist")
                if isinstance(raw_artist, dict):
                    artist = _clean_title(str(raw_artist.get("name") or ""))
                elif raw_artist is not None:
                    artist = _clean_title(str(raw_artist))
            if not artist:
                artist = _clean_title(str(result.get("artist_names") or ""))
            if not title:
                title = _clean_title(str(result.get("full_title") or ""))
            if not artist or not title:
                continue
            canon_artist, canon_title = _normalize_canonical_artist_title(artist, title)
            return {
                "artist": canon_artist or artist,
                "title": canon_title or title,
                "query": q,
                "provider": "genius_search",
            }
    except Exception:
        return {}
    return {}


def _yt_data_api_top_result_hint(query: str, *, timeout_sec: float = 6.0) -> Dict[str, str]:
    """
    Top-result hint via source Data API (optional, key-based).
    Returns {title, artist, video_id} when available.
    """
    if not source_DATA_API_KEY:
        return {}
    q = " ".join((query or "").split()).strip()
    if not q:
        return {}

    preset = str(os.environ.get("MIXTERIOSO_YT_DATA_API_PRESET", "preset_miguel_1") or "").strip().lower()
    is_miguel_1 = (preset == "preset_miguel_1")

    try:
        search_resp = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet",
                "maxResults": 12 if is_miguel_1 else 1,
                "type": "video",
                "order": "relevance",
                "regionCode": "US" if is_miguel_1 else None,
                "q": q,
                "key": source_DATA_API_KEY,
            },
            timeout=max(1.0, float(timeout_sec)),
        )
        if search_resp.status_code != 200:
            return {}
        payload = search_resp.json()
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list) or not items:
            return {}
        if not is_miguel_1:
            item0 = items[0] if isinstance(items[0], dict) else {}
            item_id = item0.get("id") if isinstance(item0, dict) else None
            video_id = str(item_id.get("videoId") or "").strip() if isinstance(item_id, dict) else ""
            snippet = items[0].get("snippet") if isinstance(items[0], dict) else None
            if not isinstance(snippet, dict):
                return {}
            title = _clean_title(str(snippet.get("title") or ""))
            channel = _clean_title(str(snippet.get("channelTitle") or ""))
            channel = re.sub(r"\s*[-–—]?\s*topic$", "", channel, flags=re.IGNORECASE).strip()
            if not title:
                return {}
            canon_artist, canon_title = _normalize_canonical_artist_title(channel, title) if channel else ("", _strip_lyrics_title_noise(title))
            return {
                "title": canon_title or title,
                "artist": canon_artist or _clean_title(channel),
                "video_id": video_id,
            }

        # preset_miguel_1:
        # 1) regionCode=US, order=relevance
        # 2) early filter out live or live-like titles
        # 3) tie-break remaining by highest viewCount (videos.list)
        candidates: List[Tuple[str, str, str]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            snippet = item.get("snippet")
            if not isinstance(snippet, dict):
                continue
            live_broadcast = str(snippet.get("liveBroadcastContent") or "").strip().lower()
            title = _clean_title(str(snippet.get("title") or ""))
            channel = _clean_title(str(snippet.get("channelTitle") or ""))
            video_id = ""
            item_id = item.get("id")
            if isinstance(item_id, dict):
                video_id = str(item_id.get("videoId") or "").strip()
            if not video_id or not title:
                continue
            if live_broadcast in {"live", "upcoming"}:
                continue
            if _is_live_like_title(title):
                continue
            candidates.append((video_id, title, channel))

        if not candidates:
            # Backward-compatible fallback for minimal payloads (tests/mocks)
            for item in items:
                if not isinstance(item, dict):
                    continue
                snippet = item.get("snippet")
                if not isinstance(snippet, dict):
                    continue
                title = _clean_title(str(snippet.get("title") or ""))
                channel = _clean_title(str(snippet.get("channelTitle") or ""))
                video_id = ""
                item_id = item.get("id")
                if isinstance(item_id, dict):
                    video_id = str(item_id.get("videoId") or "").strip()
                if not title:
                    continue
                channel = re.sub(r"\s*[-–—]?\s*topic$", "", channel, flags=re.IGNORECASE).strip()
                canon_artist, canon_title = _normalize_canonical_artist_title(channel, title) if channel else ("", _strip_lyrics_title_noise(title))
                return {
                    "title": canon_title or title,
                    "artist": canon_artist or _clean_title(channel),
                    "video_id": video_id,
                }
            return {}

        ids_csv = ",".join([cid for cid, _t, _c in candidates[:10]])
        view_count_by_id: Dict[str, int] = {}
        try:
            v_resp = requests.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "part": "statistics",
                    "id": ids_csv,
                    "key": source_DATA_API_KEY,
                },
                timeout=max(1.0, float(timeout_sec)),
            )
            if int(v_resp.status_code or 0) == 200:
                v_payload = v_resp.json()
                v_items = v_payload.get("items") if isinstance(v_payload, dict) else None
                if isinstance(v_items, list):
                    for v_item in v_items:
                        if not isinstance(v_item, dict):
                            continue
                        vid = str(v_item.get("id") or "").strip()
                        stats = v_item.get("statistics") if isinstance(v_item.get("statistics"), dict) else {}
                        try:
                            vc = int(str(stats.get("viewCount") or "0"))
                        except Exception:
                            vc = 0
                        if vid:
                            view_count_by_id[vid] = max(0, vc)
        except Exception:
            pass

        # Preserve relevance order from search.list, then sort by views desc.
        best = sorted(
            enumerate(candidates),
            key=lambda row: (-(view_count_by_id.get(row[1][0], 0)), row[0]),
        )[0][1]
        _best_vid, best_title, best_channel = best
        best_channel = re.sub(r"\s*[-–—]?\s*topic$", "", _clean_title(best_channel), flags=re.IGNORECASE).strip()
        canon_artist, canon_title = _normalize_canonical_artist_title(best_channel, best_title) if best_channel else ("", _strip_lyrics_title_noise(best_title))
        return {
            "title": canon_title or best_title,
            "artist": canon_artist or _clean_title(best_channel),
            "video_id": _best_vid,
        }
    except Exception:
        return {}


_TOP_RESULT_TITLE_NOISE_RE = re.compile(
    r"\s*[\(\[][^)\]]*(official|video|audio|lyrics?|lyric|hd|4k|visualizer|remaster|mv)[^)\]]*[\)\]]",
    flags=re.IGNORECASE,
)
_TOP_RESULT_BRACKET_RE = re.compile(r"\s*\[[^\]]*]\s*$", flags=re.IGNORECASE)
_TOP_RESULT_NOISE_SEGMENT_RE = re.compile(r"(official|video|audio|lyrics?|lyric|hd|4k|visualizer|remaster|mv)", flags=re.IGNORECASE)
_TOP_RESULT_SPLIT_SEPARATORS = (" - ", " – ", " — ", " | ", " : ")
_NORMALIZE_SHORT_CIRCUIT_YTSEARCH_RE = re.compile(r"^ytsearch\d*:", flags=re.IGNORECASE)
_NORMALIZE_SHORT_CIRCUIT_VIDEO_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")
_YT_INITIAL_DATA_RE = re.compile(r"var ytInitialData = (\{.*?\});</script>", flags=re.DOTALL)
_YOUTUBE_SUGGEST_EXCLUDED_TERMS = (
    "lyrics",
    "live",
    "cover",
    "reaction",
    "remix",
    "instrumental",
    "karaoke",
    "slowed",
    "reverb",
)
_YOUTUBE_WEB_SEARCH_NOISE_RE = re.compile(
    r"\b(karaoke|cover|reaction|slowed|reverb|remix)\b",
    flags=re.IGNORECASE,
)
_NORMALIZATION_ERROR = "Unable to confidently resolve artist and title."
_NORMALIZATION_USER_ERROR = "Unable to identify song. Please include artist and title."


def _is_normalization_short_circuit_query(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    if _NORMALIZE_SHORT_CIRCUIT_VIDEO_ID_RE.fullmatch(q):
        return True
    lowered = q.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return True
    if ("youtube.com" in lowered) or ("youtu.be" in lowered):
        return True
    return bool(_NORMALIZE_SHORT_CIRCUIT_YTSEARCH_RE.match(q))


def _youtube_suggest_candidates(query: str, *, timeout_sec: Optional[float] = None) -> List[str]:
    q = _sanitize_search_query(query)
    if not q:
        return []

    effective_timeout = float(YOUTUBE_SUGGEST_TIMEOUT if timeout_sec is None else max(1.0, float(timeout_sec)))
    try:
        resp = requests.get(
            YOUTUBE_SUGGEST_API,
            params={
                "client": "firefox",
                "ds": "yt",
                "hl": "en",
                "q": q,
            },
            headers={
                "Accept": "application/json",
                "User-Agent": (YTDLP_UA or "mixterioso/1.0 (step1_fetch suggest)"),
            },
            timeout=effective_timeout,
        )
        if resp.status_code != 200:
            return []
        payload = resp.json() if resp.text else []
    except Exception:
        return []

    suggestions_raw: List[Any] = []
    if isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], list):
        suggestions_raw = payload[1]
    elif isinstance(payload, dict):
        maybe = payload.get("suggestions")
        if isinstance(maybe, list):
            suggestions_raw = maybe

    out: List[str] = []
    seen: set[str] = set()
    for item in suggestions_raw:
        suggestion = _sanitize_search_query(str(item or ""))
        if not suggestion:
            continue
        lowered = suggestion.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(suggestion)
    return out


def _pick_best_suggest_candidate(suggestions: List[str]) -> str:
    if not suggestions:
        return ""

    filtered = [
        suggestion
        for suggestion in suggestions
        if not any(term in suggestion.lower() for term in _YOUTUBE_SUGGEST_EXCLUDED_TERMS)
    ]
    candidate = filtered[0] if filtered else suggestions[0]
    candidate = _sanitize_search_query(candidate)
    if len(candidate) < 3:
        return ""
    return candidate


def _yt_initial_data_from_html(html_text: str) -> Dict[str, Any]:
    raw = str(html_text or "")
    if not raw:
        return {}
    match = _YT_INITIAL_DATA_RE.search(raw)
    if not match:
        match = re.search(r"ytInitialData\s*=\s*(\{.*?\});", raw, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _yt_web_text_from_runs(node: Any) -> str:
    if isinstance(node, dict):
        simple = " ".join(str(node.get("simpleText") or "").split()).strip()
        if simple:
            return simple
        runs = node.get("runs")
        if isinstance(runs, list):
            parts = [
                " ".join(str(item.get("text") or "").split()).strip()
                for item in runs
                if isinstance(item, dict)
            ]
            return " ".join([part for part in parts if part]).strip()
    return ""


def _yt_web_search_candidate_score(query: str, *, title: str, channel: str, index: int) -> float:
    score = float(_query_token_overlap_score(query, channel, title))
    score -= float(index) * 0.1

    lowered = ("%s %s" % (title, channel)).strip().lower()
    wants_lyrics = _query_requests_lyrics_version(query)
    if _is_live_like_title(title):
        score -= 10.0
    if re.search(r"\blive\b", lowered):
        score -= 10.0
    if _is_lyrics_like_title(title):
        score += 5.0 if wants_lyrics else -2.5
    if wants_lyrics:
        if _OFFICIAL_VIDEO_TITLE_RE.search(title):
            score -= 3.0
        elif _OFFICIAL_AUDIO_TITLE_RE.search(title):
            score -= 1.5
    if _YOUTUBE_WEB_SEARCH_NOISE_RE.search(lowered):
        score -= 4.0

    if (not wants_lyrics) and _OFFICIAL_VIDEO_TITLE_RE.search(title):
        score += 2.5
    elif (not wants_lyrics) and _OFFICIAL_AUDIO_TITLE_RE.search(title):
        score += 1.8
    elif (not wants_lyrics) and _AUDIO_TITLE_RE.search(title) and (not _is_lyrics_like_title(title)):
        score += 0.8
    elif (not wants_lyrics) and "official" in lowered:
        score += 0.4

    return score


def _yt_web_search_top_result_hint(query: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    q = _sanitize_search_query(query)
    if not q:
        return {}

    effective_timeout = float(YTDLP_SEARCH_TIMEOUT if timeout_sec is None else max(0.5, float(timeout_sec)))
    try:
        resp = requests.get(
            "https://www.youtube.com/results",
            params={"search_query": q},
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": (YTDLP_UA or "Mozilla/5.0 (mixterioso web search)"),
            },
            timeout=effective_timeout,
        )
        if int(resp.status_code or 0) != 200:
            return {}
        payload = _yt_initial_data_from_html(resp.text)
    except Exception:
        return {}

    if not payload:
        return {}

    candidates: List[Tuple[str, str, str]] = []

    def _walk(node: Any) -> None:
        if len(candidates) >= 12:
            return
        if isinstance(node, dict):
            renderer = node.get("videoRenderer")
            if isinstance(renderer, dict):
                video_id = str(renderer.get("videoId") or "").strip()
                title = _yt_web_text_from_runs(renderer.get("title"))
                channel = _yt_web_text_from_runs(renderer.get("ownerText"))
                if video_id and title:
                    candidates.append((video_id, title, channel))
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for value in node:
                _walk(value)

    _walk(payload)
    if not candidates:
        return {}

    non_live_candidates = [
        candidate
        for candidate in candidates
        if (
            not _is_live_like_title(candidate[1])
            and (not re.search(r"\blive\b", ("%s %s" % (candidate[1], candidate[2])).lower()))
        )
    ]
    if non_live_candidates:
        candidates = non_live_candidates

    ranked = sorted(
        enumerate(candidates),
        key=lambda row: -_yt_web_search_candidate_score(
            q,
            title=row[1][1],
            channel=row[1][2],
            index=row[0],
        ),
    )
    best_video_id, best_title, best_channel = ranked[0][1]
    cleaned_channel = re.sub(
        r"\s*[-–—]?\s*topic$",
        "",
        _clean_title(best_channel),
        flags=re.IGNORECASE,
    ).strip()
    canon_artist, canon_title = (
        _normalize_canonical_artist_title(cleaned_channel, best_title)
        if cleaned_channel
        else ("", _strip_lyrics_title_noise(best_title))
    )
    return {
        "title": canon_title or _strip_lyrics_title_noise(best_title) or best_title,
        "artist": canon_artist or cleaned_channel,
        "video_id": best_video_id,
    }


def _normalize_query_from_explicit_split(query: str, *, provider: str) -> Dict[str, str]:
    artist, track = _maybe_split_artist_title(query)
    artist, track = _normalize_canonical_artist_title(artist, track)
    normalized_query = _artist_title_query(artist, track)
    if not artist or not track or not normalized_query:
        return {}
    return {
        "artist": artist,
        "track": track,
        "title": track,
        "display": normalized_query,
        "confidence": "medium",
        "normalized_query": normalized_query,
        "provider": provider,
        "suggestion": _sanitize_search_query(query),
    }


def _strip_top_result_title_noise(text: str) -> str:
    out = _clean_title(text)
    if not out:
        return ""
    # Remove obvious trailing marketing labels but keep the song's core title.
    out = _TOP_RESULT_TITLE_NOISE_RE.sub("", out)
    out = _TOP_RESULT_BRACKET_RE.sub("", out)
    out = _strip_lyrics_title_noise(out)
    # Drop leading "noise segments" like "4K REMASTER - <title>".
    dash_parts = [part.strip() for part in re.split(r"\s*[-–—]\s*", out) if part.strip()]
    if len(dash_parts) > 1:
        first_part = dash_parts[0].lower()
        if re.search(r"(official|video|audio|lyrics?|lyric|hd|4k|visualizer|remaster|mv)", first_part):
            out = " - ".join(dash_parts[1:]).strip()
    out = re.sub(r"\s{2,}", " ", out).strip(" -")
    return out or _clean_title(text)


def _strip_vevo_suffix(text: str) -> str:
    return re.sub(r"\s*[-–—]?\s*vevo$", "", _clean_title(text), flags=re.IGNORECASE).strip()


def _artist_channel_match(reference: str, candidate: str) -> bool:
    ref_key = _normalize_artist_key(reference)
    cand_key = _normalize_artist_key(candidate)
    if not ref_key or not cand_key:
        return False
    return (
        ref_key == cand_key
        or ref_key in cand_key
        or cand_key in ref_key
        or _seq_ratio(ref_key, cand_key) >= 0.72
    )


def _clean_artist_segment(text: str) -> str:
    artist = _strip_vevo_suffix(text)
    parts = [part.strip() for part in re.split(r"\s*[-–—]\s*", artist) if part.strip()]
    if len(parts) > 1 and _TOP_RESULT_NOISE_SEGMENT_RE.search(parts[-1]):
        artist = " - ".join(parts[:-1]).strip()
    return _strip_vevo_suffix(artist)


def _extract_artist_title_from_top_result(title: str, uploader: str, channel: str) -> Tuple[str, str, str]:
    clean_title = _clean_title(title)
    uploader_hint = _strip_vevo_suffix(channel or uploader)

    for sep in _TOP_RESULT_SPLIT_SEPARATORS:
        idx = clean_title.find(sep)
        if idx <= 0:
            continue
        left = _clean_title(clean_title[:idx])
        right = _clean_title(clean_title[idx + len(sep):])
        if not left or not right:
            continue

        left_matches_uploader = bool(uploader_hint and _artist_channel_match(uploader_hint, left))
        right_matches_uploader = bool(uploader_hint and _artist_channel_match(uploader_hint, right))

        if right_matches_uploader and not left_matches_uploader:
            artist = _clean_artist_segment(right)
            track = _strip_top_result_title_noise(left)
        else:
            artist = _clean_artist_segment(left)
            track = _strip_top_result_title_noise(right)
        if artist and track:
            track_parts = [part.strip() for part in re.split(r"\s*[-–—]\s*", track) if part.strip()]
            if len(track_parts) > 1 and _artist_channel_match(artist, track_parts[0]):
                track = " - ".join(track_parts[1:]).strip()
                track_parts = [part.strip() for part in re.split(r"\s*[-–—]\s*", track) if part.strip()]
            if len(track_parts) > 1 and _artist_channel_match(artist, track_parts[-1]):
                track = " - ".join(track_parts[:-1]).strip()
        if artist and track:
            return artist, track, "medium"

    artist = _clean_artist_segment(uploader_hint)
    track = _strip_top_result_title_noise(clean_title)
    return artist, track, "low"


def _ytsearch1_dump_json_top_result(query: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    q = " ".join((query or "").split()).strip()
    if not q:
        return {}

    effective_timeout = float(YTDLP_SEARCH_TIMEOUT if timeout_sec is None else max(1.0, timeout_sec))
    cmd = [*YTDLP_CMD]
    if YTDLP_NO_WARNINGS:
        cmd.append("--no-warnings")
    if YTDLP_VERBOSE:
        cmd.append("--verbose")
    cmd += [
        "--dump-json",
        "--skip-download",
        "--no-playlist",
        "--force-ipv4",
        "--socket-timeout",
        str(YTDLP_SOCKET_TIMEOUT),
    ]
    if YTDLP_UA:
        cmd += ["--user-agent", str(YTDLP_UA)]
    for hdr in YTDLP_EXTRA_HEADERS:
        cmd += ["--add-headers", hdr]
    if YTDLP_EXTRACTOR_ARGS:
        cmd += ["--extractor-args", str(YTDLP_EXTRACTOR_ARGS)]

    cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())
    if cookies_configured:
        cookies_path = _writable_cookies_path()
        if cookies_path:
            cmd += ["--cookies", cookies_path]

    proxy = _current_proxy()
    if proxy:
        cmd += ["--proxy", str(proxy)]

    cmd += ["ytsearch1:%s" % q]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout)
        if proc.returncode != 0:
            diag = _collect_ytdlp_diagnostics(proc.stderr or "", proc.stdout or "")
            _mark_proxy_failure(proxy, reason="ytsearch1_query_normalize_failed")
            if _should_rotate_proxy_on_error(diag):
                _rotate_proxy("ytsearch1_query_normalize")
            return {}

        _mark_proxy_success(proxy)
        for raw_line in (proc.stdout or "").splitlines():
            raw = (raw_line or "").strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            title = _clean_title(str(payload.get("title") or ""))
            uploader = _clean_title(str(payload.get("uploader") or ""))
            channel = _clean_title(str(payload.get("channel") or ""))
            video_id = _clean_title(str(payload.get("id") or ""))
            artist = _clean_title(str(payload.get("artist") or ""))
            album_artist = _clean_title(str(payload.get("album_artist") or ""))
            track = _clean_title(str(payload.get("track") or ""))
            artists_value = payload.get("artists")
            artists = ""
            if isinstance(artists_value, list):
                artist_parts: List[str] = []
                seen_artist_parts: set[str] = set()
                for item in artists_value:
                    token = _clean_title(str(item or ""))
                    key = token.lower()
                    if (not token) or (key in seen_artist_parts):
                        continue
                    seen_artist_parts.add(key)
                    artist_parts.append(token)
                artists = ", ".join(artist_parts)
            elif artists_value:
                artists = _clean_title(str(artists_value))
            if not title and not uploader and not channel:
                continue
            return {
                "title": title,
                "uploader": uploader,
                "channel": channel,
                "video_id": video_id,
                "artist": artist,
                "album_artist": album_artist,
                "track": track,
                "artists": artists,
            }
    except Exception:
        return {}
    return {}


def _normalize_query_via_ytsearch_top_result(query: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    raw_query = " ".join((query or "").split()).strip()
    if not raw_query:
        return {
            "provider": "strict_song_artist_normalization",
            "error": _NORMALIZATION_ERROR,
            "user_error": _NORMALIZATION_USER_ERROR,
        }

    if _is_normalization_short_circuit_query(raw_query):
        return {
            "provider": "normalization_short_circuit",
            "normalized_query": raw_query,
            "display": raw_query,
            "short_circuit": "1",
        }

    hot_artist, hot_track = _hot_query_artist_title(raw_query)
    if hot_artist and hot_track:
        hot_display = _artist_title_query(hot_artist, hot_track)
        return {
            "artist": hot_artist,
            "track": hot_track,
            "title": hot_track,
            "display": hot_display,
            "confidence": "high",
            "normalized_query": hot_display,
            "provider": "hot_query_artist_title",
            "suggestion": _sanitize_search_query(raw_query),
        }

    split_fallback = _normalize_query_from_explicit_split(raw_query, provider="query_split_fallback")
    q = _sanitize_search_query(raw_query)
    suggestions = _youtube_suggest_candidates(q, timeout_sec=min(6.0, YOUTUBE_SUGGEST_TIMEOUT))
    candidate = _pick_best_suggest_candidate(suggestions)
    top_result_provider = "yt_suggest_ytsearch1"
    if not candidate:
        if split_fallback:
            return split_fallback
        candidate = q
        top_result_provider = "ytsearch1_query_fallback"
    if not candidate:
        return {
            "provider": "yt_suggest",
            "error": _NORMALIZATION_ERROR,
            "user_error": _NORMALIZATION_USER_ERROR,
            "suggestion": "",
        }

    fast_hint_payload: Optional[Dict[str, str]] = None
    fast_hint = _yt_search_top_result_hint(candidate, timeout_sec=timeout_sec)
    fast_title = _clean_title(str(fast_hint.get("title") or ""))
    fast_artist = _clean_title(str(fast_hint.get("artist") or ""))
    fast_video_id = _clean_title(str(fast_hint.get("video_id") or ""))
    if fast_title or fast_artist:
        artist, track = _normalize_canonical_artist_title(fast_artist, fast_title)
        normalized_query = _artist_title_query(artist, track)
        if artist and track and normalized_query:
            fast_hint_payload = {
                "artist": artist,
                "track": track,
                "title": track,
                "display": normalized_query,
                "confidence": "medium",
                "normalized_query": normalized_query,
                "provider": top_result_provider,
                "suggestion": candidate,
                "raw_title": fast_title,
                "raw_uploader": "",
                "raw_channel": "",
                "raw_track": "",
                "raw_artist": fast_artist,
                "raw_album_artist": "",
                "raw_artists": "",
                "video_id": fast_video_id,
            }
            if _normalize_key(candidate) == _normalize_key(raw_query):
                return fast_hint_payload

    top = _ytsearch1_dump_json_top_result(candidate, timeout_sec=timeout_sec)
    raw_title = _clean_title(str(top.get("title") or ""))
    raw_uploader = _clean_title(str(top.get("uploader") or ""))
    raw_channel = _clean_title(str(top.get("channel") or ""))
    video_id = _clean_title(str(top.get("video_id") or ""))
    raw_track = _clean_title(str(top.get("track") or ""))
    raw_artist = _clean_title(str(top.get("artist") or ""))
    raw_album_artist = _clean_title(str(top.get("album_artist") or ""))
    raw_artists = _clean_title(str(top.get("artists") or ""))
    if not raw_title and not raw_uploader and not raw_channel:
        if fast_hint_payload:
            return fast_hint_payload
        if split_fallback:
            split_fallback["provider"] = f"{top_result_provider}+query_split_fallback"
            split_fallback["suggestion"] = candidate
            return split_fallback
        return {
            "provider": "ytsearch1_top_result",
            "error": _NORMALIZATION_ERROR,
            "user_error": _NORMALIZATION_USER_ERROR,
            "suggestion": candidate,
            "raw_title": raw_title,
            "raw_uploader": raw_uploader,
            "raw_channel": raw_channel,
            "raw_track": raw_track,
            "raw_artist": raw_artist,
            "raw_album_artist": raw_album_artist,
            "raw_artists": raw_artists,
            "video_id": video_id,
        }

    artist = ""
    track = ""
    confidence = ""
    metadata_artist = raw_artist or raw_album_artist or raw_artists
    if raw_track and metadata_artist:
        artist = metadata_artist
        track = raw_track
        confidence = "high"
    elif raw_title:
        artist, track, confidence = _extract_artist_title_from_top_result(raw_title, raw_uploader, raw_channel)

    if (not artist) and (raw_uploader or raw_channel):
        artist = _strip_vevo_suffix(raw_channel or raw_uploader)
        if not confidence:
            confidence = "low"
    if not track:
        track = _strip_top_result_title_noise(raw_title)
        if not confidence:
            confidence = "low"

    artist, track = _normalize_canonical_artist_title(artist, track)
    if not artist or not track:
        if fast_hint_payload:
            return fast_hint_payload
        if split_fallback:
            split_fallback["provider"] = f"{top_result_provider}+query_split_fallback"
            split_fallback["suggestion"] = candidate
            return split_fallback
        return {
            "provider": "ytsearch1_top_result",
            "error": _NORMALIZATION_ERROR,
            "user_error": _NORMALIZATION_USER_ERROR,
            "suggestion": candidate,
            "raw_title": raw_title,
            "raw_uploader": raw_uploader,
            "raw_channel": raw_channel,
            "raw_track": raw_track,
            "raw_artist": raw_artist,
            "raw_album_artist": raw_album_artist,
            "raw_artists": raw_artists,
            "video_id": video_id,
        }

    normalized_query = _artist_title_query(artist, track)
    if not normalized_query:
        if fast_hint_payload:
            return fast_hint_payload
        if split_fallback:
            split_fallback["provider"] = f"{top_result_provider}+query_split_fallback"
            split_fallback["suggestion"] = candidate
            return split_fallback
        return {
            "provider": "ytsearch1_top_result",
            "error": _NORMALIZATION_ERROR,
            "user_error": _NORMALIZATION_USER_ERROR,
            "suggestion": candidate,
            "raw_title": raw_title,
            "raw_uploader": raw_uploader,
            "raw_channel": raw_channel,
            "raw_track": raw_track,
            "raw_artist": raw_artist,
            "raw_album_artist": raw_album_artist,
            "raw_artists": raw_artists,
            "video_id": video_id,
        }

    return {
        "artist": artist,
        "track": track,
        "title": track,
        "display": normalized_query,
        "confidence": confidence or "low",
        "normalized_query": normalized_query,
        "provider": top_result_provider,
        "suggestion": candidate,
        "raw_title": raw_title,
        "raw_uploader": raw_uploader,
        "raw_channel": raw_channel,
        "raw_track": raw_track,
        "raw_artist": raw_artist,
        "raw_album_artist": raw_album_artist,
        "raw_artists": raw_artists,
        "video_id": video_id,
    }


def _yt_search_top_result_hint(query: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    """
    Ask yt-dlp for the top ytsearch result metadata so we can reuse source's
    correction/suggestion behavior for heavily misspelled user queries.
    """
    effective_timeout = float(YTDLP_SEARCH_TIMEOUT if timeout_sec is None else max(1.0, timeout_sec))
    q = " ".join((query or "").split()).strip()
    if not q:
        return {}

    # Prefer official source Data API when key is available.
    data_api_hint = _yt_data_api_top_result_hint(q, timeout_sec=min(6.0, effective_timeout))
    if data_api_hint.get("title"):
        return data_api_hint

    def _run(extractor_args: str) -> Tuple[subprocess.CompletedProcess, str]:
        cmd = [*YTDLP_CMD]
        if YTDLP_NO_WARNINGS:
            cmd.append("--no-warnings")
        if YTDLP_VERBOSE:
            cmd.append("--verbose")

        cmd += [
            "--flat-playlist",
            "--print",
            "%(id)s\t%(title)s\t%(uploader)s\t%(channel)s",
            "--force-ipv4",
            "--socket-timeout",
            str(YTDLP_SOCKET_TIMEOUT),
        ]

        if YTDLP_UA:
            cmd += ["--user-agent", str(YTDLP_UA)]
        for hdr in YTDLP_EXTRA_HEADERS:
            cmd += ["--add-headers", hdr]

        cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())
        if cookies_configured:
            cookies_path = _writable_cookies_path()
            if cookies_path:
                cmd += ["--cookies", cookies_path]

        if extractor_args:
            cmd += ["--extractor-args", str(extractor_args)]

        proxy = _current_proxy()
        if proxy:
            cmd += ["--proxy", str(proxy)]

        cmd += ["ytsearch1:%s" % q]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout), proxy

    plan: List[Tuple[str, str]] = []
    seen_args: set[str] = set()
    for label, args in (
        ("primary", YTDLP_EXTRACTOR_ARGS),
        ("fallback", YTDLP_FALLBACK_EXTRACTOR_ARGS),
        ("bare", ""),
    ):
        normalized = (args or "").strip()
        if normalized in seen_args:
            continue
        seen_args.add(normalized)
        plan.append((label, normalized))

    for _, args in plan:
        for proxy_try in range(_proxy_retry_budget()):
            try:
                p, used_proxy = _run(args)
            except Exception:
                break
            if p.returncode == 0:
                _mark_proxy_success(used_proxy)
                for line in (p.stdout or "").splitlines():
                    raw = (line or "").strip()
                    if not raw:
                        continue
                    parts = raw.split("\t")
                    video_id = str(parts[0] or "").strip() if len(parts) >= 1 else ""
                    title = _clean_title(parts[1]) if len(parts) >= 2 else ""
                    uploader = _clean_title(parts[2]) if len(parts) >= 3 else ""
                    channel = _clean_title(parts[3]) if len(parts) >= 4 else ""

                    artist_hint = channel or uploader
                    artist_hint = re.sub(
                        r"\s*[-–—]?\s*topic$",
                        "",
                        _clean_title(artist_hint),
                        flags=re.IGNORECASE,
                    ).strip()

                    if title:
                        canon_artist, canon_title = _normalize_canonical_artist_title(artist_hint, title) if artist_hint else ("", _strip_lyrics_title_noise(title))
                        return {
                            "title": canon_title or title,
                            "artist": canon_artist or artist_hint,
                            "video_id": video_id,
                        }
                break
            diag = _collect_ytdlp_diagnostics(p.stderr or "", p.stdout or "")
            _mark_proxy_failure(used_proxy, reason="yt_hint_search_failed")
            if _should_rotate_proxy_on_error(diag) and proxy_try + 1 < _proxy_retry_budget():
                _rotate_proxy("yt_hint_search")
                continue
            break
    return {}


def _resolve_fast_query_source(
    query: str,
    *,
    timeout_sec: Optional[float] = None,
    target_duration_sec: Optional[float] = None,
) -> Optional[Tuple[str, str, Dict[str, str]]]:
    q = " ".join((query or "").split()).strip()
    if not q:
        return None

    effective_timeout = float(
        MP3_FAST_QUERY_RESOLVE_TIMEOUT_SEC if timeout_sec is None else max(0.5, float(timeout_sec))
    )
    duration_target = _float_or_none(target_duration_sec)
    explicit_variant_intent = _query_has_explicit_audio_variant_intent(q)
    if (not explicit_variant_intent) and duration_target is not None and duration_target > 0.0:
        try:
            ids = yt_search_ids(
                q,
                max(3, int(MP3_TOP_HIT_SEARCH_N), int(MP3_NON_LIVE_MIN_SEARCH_N)),
                timeout_sec=min(effective_timeout, float(YTDLP_SEARCH_TIMEOUT)),
                target_duration_sec=float(duration_target),
            )
        except Exception:
            ids = []
        for video_id in ids:
            vid = str(video_id or "").strip()
            if not _source_ID_RE.match(vid):
                continue
            hint = _yt_oembed_video_hint(vid, timeout_sec=min(2.0, effective_timeout))
            if not hint.get("title"):
                hint = _yt_video_metadata_hint(vid, timeout_sec=min(effective_timeout, float(YTDLP_SEARCH_TIMEOUT)))
            hint = dict(hint or {})
            hint["video_id"] = vid
            return (vid, "https://www.youtube.com/watch?v=%s" % vid, hint)

    if explicit_variant_intent or (not _query_needs_duration_disambiguation(q)):
        web_hint = _yt_web_search_top_result_hint(
            q,
            timeout_sec=min(1.5, effective_timeout),
        )
        web_video_id = str(web_hint.get("video_id") or "").strip()
        if _source_ID_RE.match(web_video_id):
            return (web_video_id, "https://www.youtube.com/watch?v=%s" % web_video_id, web_hint)

    hint = _yt_search_top_result_hint(q, timeout_sec=min(effective_timeout, float(YTDLP_SEARCH_TIMEOUT)))
    video_id = str(hint.get("video_id") or "").strip()
    if not _source_ID_RE.match(video_id):
        return None
    return (video_id, "https://www.youtube.com/watch?v=%s" % video_id, hint)

def _yt_video_metadata_hint(video_id: str, *, timeout_sec: Optional[float] = None) -> Dict[str, str]:
    """
    Fetch title/uploader metadata for a concrete source video id.
    """
    vid = " ".join((video_id or "").split()).strip()
    if not vid:
        return {}

    effective_timeout = float(YTDLP_SEARCH_TIMEOUT if timeout_sec is None else max(1.0, timeout_sec))
    source = "https://www.youtube.com/watch?v=%s" % vid

    def _run(extractor_args: str) -> Tuple[subprocess.CompletedProcess, str]:
        cmd = [*YTDLP_CMD]
        if YTDLP_NO_WARNINGS:
            cmd.append("--no-warnings")
        if YTDLP_VERBOSE:
            cmd.append("--verbose")
        cmd += [
            "--skip-download",
            "--print",
            "%(title)s\t%(uploader)s\t%(channel)s",
            "--force-ipv4",
            "--socket-timeout",
            str(YTDLP_SOCKET_TIMEOUT),
        ]
        if YTDLP_UA:
            cmd += ["--user-agent", str(YTDLP_UA)]
        for hdr in YTDLP_EXTRA_HEADERS:
            cmd += ["--add-headers", hdr]
        cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())
        if cookies_configured:
            cookies_path = _writable_cookies_path()
            if cookies_path:
                cmd += ["--cookies", cookies_path]
        if extractor_args:
            cmd += ["--extractor-args", str(extractor_args)]
        proxy = _current_proxy()
        if proxy:
            cmd += ["--proxy", str(proxy)]
        cmd += [source]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout), proxy

    tried: set[str] = set()
    for extractor_args in (str(YTDLP_EXTRACTOR_ARGS or "").strip(), str(YTDLP_FALLBACK_EXTRACTOR_ARGS or "").strip(), ""):
        if extractor_args in tried:
            continue
        tried.add(extractor_args)
        try:
            p, used_proxy = _run(extractor_args)
        except Exception:
            continue
        if p.returncode != 0:
            _mark_proxy_failure(used_proxy, reason="yt_video_metadata_hint_failed")
            continue
        _mark_proxy_success(used_proxy)
        for line in (p.stdout or "").splitlines():
            raw = (line or "").strip()
            if not raw:
                continue
            parts = raw.split("\t")
            title = _clean_title(parts[0]) if len(parts) >= 1 else ""
            uploader = _clean_title(parts[1]) if len(parts) >= 2 else ""
            channel = _clean_title(parts[2]) if len(parts) >= 3 else ""
            artist_hint = _clean_title(channel or uploader)
            artist_hint = re.sub(r"\s*[-–—]?\s*topic$", "", artist_hint, flags=re.IGNORECASE).strip()
            if title:
                canon_artist, canon_title = _normalize_canonical_artist_title(artist_hint, title) if artist_hint else ("", _strip_lyrics_title_noise(title))
                return {"title": canon_title or title, "artist": canon_artist or artist_hint}
    return {}

def _yt_oembed_video_hint(video_id: str, *, timeout_sec: float = 5.0) -> Dict[str, str]:
    """
    Lightweight metadata hint via source oEmbed for a concrete video id.
    """
    vid = " ".join((video_id or "").split()).strip()
    if not vid:
        return {}
    try:
        resp = requests.get(
            "https://www.youtube.com/oembed",
            params={
                "url": "https://www.youtube.com/watch?v=%s" % vid,
                "format": "json",
            },
            timeout=max(1.0, float(timeout_sec)),
        )
        if resp.status_code != 200:
            return {}
        data = resp.json() if resp.text else {}
        if not isinstance(data, dict):
            return {}
        title = _clean_title(str(data.get("title") or ""))
        artist = _clean_title(str(data.get("author_name") or ""))
        artist = re.sub(r"\s*[-–—]?\s*topic$", "", artist, flags=re.IGNORECASE).strip()
        if not title:
            return {}
        canon_artist, canon_title = _normalize_canonical_artist_title(artist, title) if artist else ("", _strip_lyrics_title_noise(title))
        return {"title": canon_title or title, "artist": canon_artist or artist}
    except Exception:
        return {}

def _is_probable_source_video_id(value: str) -> bool:
    v = " ".join((value or "").split()).strip()
    return bool(re.match(r"^[A-Za-z0-9_-]{11}$", v))

def _normalize_canonical_artist_title(artist: str, title: str) -> Tuple[str, str]:
    canon_artist = _clean_title(artist)
    canon_title = _strip_lyrics_title_noise(title)
    if not canon_artist or not canon_title:
        return canon_artist, canon_title

    artist_key = _normalize_artist_key(canon_artist)
    noisy_artist = _is_noisy_artist_label(canon_artist)
    handle_like_artist = _is_handle_like_artist_label(canon_artist)

    # If the source title already includes an artist prefix, prefer that split.
    split_artist, split_title = _maybe_split_artist_title(canon_title)
    split_title = _strip_lyrics_title_noise(split_title)
    split_artist_key = _normalize_artist_key(split_artist)
    split_matches_artist = bool(
        artist_key
        and split_artist_key
        and (
            artist_key == split_artist_key
            or artist_key in split_artist_key
            or split_artist_key in artist_key
        )
    )
    if split_artist and split_title and (
        split_matches_artist
        or _seq_ratio(canon_artist, split_artist) >= 0.45
        or noisy_artist
        or handle_like_artist
    ):
        canon_title = split_title
        if (noisy_artist or handle_like_artist) and split_artist_key:
            canon_artist = split_artist
            artist_key = split_artist_key

    # Prevent repeated "Artist - Artist - Title" query expansion.
    prefix_re = re.compile(r"^\s*%s\s*[-–—:]\s*" % re.escape(canon_artist), re.IGNORECASE)
    original = canon_title
    for _ in range(3):
        m = prefix_re.match(canon_title)
        if not m:
            break
        stripped = _clean_title(canon_title[m.end():])
        if not stripped:
            canon_title = original
            break
        canon_title = stripped

    # Strip artist-like stutters even when channel labels are noisy (e.g. TheBeatlesVEVO).
    parts = [p.strip() for p in re.split(r"\s*[-–—:|/]\s*", canon_title) if p.strip()]
    while len(parts) >= 2:
        first_key = _normalize_artist_key(parts[0])
        second_key = _normalize_artist_key(parts[1])
        first_matches_artist = bool(
            artist_key
            and first_key
            and (
                first_key == artist_key
                or first_key in artist_key
                or artist_key in first_key
            )
        )
        if first_matches_artist:
            parts = parts[1:]
            continue
        if first_key and second_key and first_key == second_key:
            parts = parts[1:]
            continue
        break
    canon_title = _strip_lyrics_title_noise(" - ".join(parts).strip() or canon_title)
    return canon_artist, canon_title

def _resolve_canonical_artist_title(
    query: str,
    lrc_info: Dict[str, Any],
    source_id: Optional[str],
    *,
    prefer_query_hint: bool = False,
) -> Tuple[str, str]:
    """
    Best-effort canonical artist/title for metadata and opening title card.
    """
    hot_artist, hot_title = _hot_query_artist_title(query)
    if hot_artist or hot_title:
        return _normalize_canonical_artist_title(hot_artist, hot_title)

    info_artist = _clean_title(str((lrc_info or {}).get("artist") or ""))
    info_title = _clean_title(str((lrc_info or {}).get("title") or ""))
    if info_artist or info_title:
        return _normalize_canonical_artist_title(info_artist, info_title)

    if source_id and _is_probable_source_video_id(str(source_id)):
        vid_hint = _yt_oembed_video_hint(str(source_id), timeout_sec=5.0)
        if not vid_hint.get("title"):
            vid_hint = _yt_video_metadata_hint(str(source_id), timeout_sec=min(8.0, YTDLP_SEARCH_TIMEOUT))
        vh_artist = _clean_title(str(vid_hint.get("artist") or ""))
        vh_title = _clean_title(str(vid_hint.get("title") or ""))
        if vh_artist or vh_title:
            return _normalize_canonical_artist_title(vh_artist, vh_title)

    artist, title = _maybe_split_artist_title(query)
    clean_artist = _clean_title(artist)
    clean_title = _clean_title(title)

    should_try_query_hint = bool(prefer_query_hint and (not clean_artist or not clean_title))
    should_try_query_hint = bool(should_try_query_hint and (" " in str(query or "").strip()))
    should_try_query_hint = bool(should_try_query_hint and len(str(query or "").strip()) >= 8)

    if should_try_query_hint:
        query_hint = _yt_search_top_result_hint(query, timeout_sec=min(8.0, YTDLP_SEARCH_TIMEOUT))
        qh_artist = _clean_title(str(query_hint.get("artist") or ""))
        qh_title = _clean_title(str(query_hint.get("title") or ""))
        if qh_artist or qh_title:
            return _normalize_canonical_artist_title(qh_artist, qh_title)

    return _normalize_canonical_artist_title(clean_artist, clean_title)


def _canonical_lrc_queries(artist: str, title: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        cleaned = _clean_title(raw)
        if not cleaned:
            return
        key = _normalize_key(cleaned)
        if not key or key in seen:
            return
        seen.add(key)
        out.append(cleaned)

    if artist and title:
        _add(f"{artist} - {title}")
        _add(f"{artist} {title}")
    if title:
        _add(title)
    if artist:
        _add(artist)
    return out


_HOT_QUERY_CANONICAL: Dict[str, str] = {
    "let it be": "the beatles let it be",
    "the beatles let it be": "the beatles let it be",
    "john frusciante god": "john frusciante god",
    "john frusciante the past recedes": "john frusciante the past recedes",
    # Cold-run validation rescue aliases for otherwise brittle queries.
    "nirvana on a plain": "nirvana on a plain unplugged",
    "shakira loca": "shakira loca spanish version",
    "shakira pies descalzos": "shakira pies descalzos suenos blancos",
    "red hot chili peppers otherside": "red hot chili peppers otherside",
    "red hot chili peppers californication": "red hot chili peppers californication",
    "red hot chili peppers under the bridge": "red hot chili peppers under the bridge",
    "the eagles hotel california": "the eagles hotel california",
    "linkin park in the end": "linkin park in the end",
    "nirvana smells like teen spirit": "nirvana smells like teen spirit",
    # Spanish catalog aliases that are brittle under free-form search text.
    "grupo mazz estupido romantico": "mazz estupido romantico",
    "grupo mazz estúpido romántico": "mazz estupido romantico",
    "carlos y jose el arbolito": "carlos y jose al pie de un arbol",
    "carlos y jose el arbolito en espanol": "carlos y jose al pie de un arbol",
    "carlos y jose el arbolito en español": "carlos y jose al pie de un arbol",
    "carlos y jose mi casa nueva": "los invasores de nuevo leon mi casa nueva",
    "carlos y jose mi casa nueva en espanol": "los invasores de nuevo leon mi casa nueva",
    "carlos y jose mi casa nueva en español": "los invasores de nuevo leon mi casa nueva",
    "a boy named sue": "a boy named sue",
    "johnny cash a boy named sue": "a boy named sue",
}
_HOT_QUERY_ARTIST_TITLE: Dict[str, Tuple[str, str]] = {
    "the beatles let it be": ("The Beatles", "Let It Be"),
    "john frusciante god": ("John Frusciante", "God"),
    "john frusciante the past recedes": ("John Frusciante", "The Past Recedes"),
    "nirvana on a plain unplugged": ("Nirvana", "On A Plain"),
    "shakira loca spanish version": ("Shakira", "Loca"),
    "shakira pies descalzos suenos blancos": ("Shakira", "Pies Descalzos, Suenos Blancos"),
    "red hot chili peppers otherside": ("Red Hot Chili Peppers", "Otherside"),
    "red hot chili peppers californication": ("Red Hot Chili Peppers", "Californication"),
    "red hot chili peppers under the bridge": ("Red Hot Chili Peppers", "Under The Bridge"),
    "the eagles hotel california": ("The Eagles", "Hotel California"),
    "linkin park in the end": ("Linkin Park", "In The End"),
    "nirvana smells like teen spirit": ("Nirvana", "Smells Like Teen Spirit"),
    "mazz estupido romantico": ("Mazz", "Estupido Romantico"),
    "carlos y jose al pie de un arbol": ("Carlos Y Jose", "Al Pie De Un Arbol"),
    "los invasores de nuevo leon mi casa nueva": ("Los Invasores De Nuevo Leon", "Mi Casa Nueva"),
    "a boy named sue": ("Johnny Cash", "A Boy Named Sue"),
}


def _is_hot_query(query: str) -> bool:
    q = " ".join((query or "").split()).strip()
    if not q:
        return False
    key = _normalize_key(q)
    return key in _HOT_QUERY_CANONICAL


def _canonicalize_hot_query(query: str) -> str:
    q = " ".join((query or "").split()).strip()
    if not q:
        return ""
    key = _normalize_key(q)
    return _HOT_QUERY_CANONICAL.get(key, q)


def _hot_query_artist_title(query: str) -> Tuple[str, str]:
    q = " ".join((query or "").split()).strip()
    if not q:
        return ("", "")
    canonical = _canonicalize_hot_query(q)
    key = _normalize_key(canonical)
    return _HOT_QUERY_ARTIST_TITLE.get(key, ("", ""))


def _apply_hot_query_speed_budget(query: str, budget: Dict[str, int | str]) -> Dict[str, int | str]:
    if (not MP3_HOT_QUERY_SPEED_MODE) or (not _is_hot_query(query)):
        return budget

    tuned = dict(budget)
    profile = str(tuned.get("profile") or "full")
    tuned["profile"] = "hot-" + profile
    tuned["search_n"] = min(int(tuned.get("search_n") or 1), int(MP3_HOT_QUERY_SPEED_SEARCH_N))
    tuned["id_attempt_limit"] = min(int(tuned.get("id_attempt_limit") or 1), int(MP3_HOT_QUERY_SPEED_MAX_ID_ATTEMPTS))
    tuned["variant_limit"] = min(int(tuned.get("variant_limit") or 1), int(MP3_HOT_QUERY_SPEED_MAX_QUERY_VARIANTS))
    tuned["search_query_limit"] = min(
        int(tuned.get("search_query_limit") or 1),
        int(MP3_HOT_QUERY_SPEED_MAX_SEARCH_QUERY_VARIANTS),
    )
    return tuned


def _lyrics_metadata_mismatch(lrc_info: Dict[str, Any], *, canonical_artist: str, canonical_title: str) -> bool:
    if not canonical_artist and not canonical_title:
        return False

    lrc_artist = _clean_title(str((lrc_info or {}).get("artist") or ""))
    lrc_title = _clean_title(str((lrc_info or {}).get("title") or ""))
    provider = str((lrc_info or {}).get("provider") or "").strip().lower()

    # Pseudo fallback lines are expected to be weakly aligned and are handled elsewhere.
    if provider == "step1_fallback_pseudo":
        return False

    canonical_artist_score = _seq_ratio(canonical_artist, lrc_artist) if (canonical_artist and lrc_artist) else 0.0
    canonical_title_score = _seq_ratio(canonical_title, lrc_title) if (canonical_title and lrc_title) else 0.0

    if canonical_title and not lrc_title:
        return True
    if canonical_artist and not lrc_artist and canonical_title_score < 0.55:
        return True
    if canonical_title and canonical_title_score < 0.55:
        return True
    if canonical_artist and lrc_artist and canonical_artist_score < 0.45:
        return True
    return False


def _yt_search_cache_key(
    query: str,
    n: int,
    *,
    target_duration_sec: Optional[float] = None,
) -> Tuple[str, int, int, int, int, int, int, int]:
    normalized_query = _canonicalize_hot_query(query).lower()
    duration_key_ms = 0
    duration_val = _float_or_none(target_duration_sec)
    if duration_val is not None and duration_val > 0.0:
        duration_key_ms = int(round(duration_val * 1000.0))
    return (
        normalized_query,
        int(max(1, n)),
        int(bool(MP3_PREFER_LYRICS_VERSION)),
        int(bool(MP3_PREFER_OFFICIAL_AUDIO_VERSION)),
        int(bool(MP3_PREFER_NON_LIVE_VERSION)),
        int(max(1, MP3_NON_LIVE_MIN_SEARCH_N)),
        duration_key_ms,
        6,  # cache schema version (simple first-viable relevance ordering)
    )


def _yt_search_ids_cache_get(query: str, n: int, *, target_duration_sec: Optional[float] = None) -> Optional[List[str]]:
    ttl = float(YTDLP_SEARCH_CACHE_TTL_SEC)
    if ttl <= 0:
        return None
    key = _yt_search_cache_key(query, n, target_duration_sec=target_duration_sec)
    now = time.monotonic()
    with _YT_SEARCH_IDS_CACHE_LOCK:
        entry = _YT_SEARCH_IDS_CACHE.get(key)
        if entry is None:
            return None
        expires_at, ids = entry
        if expires_at <= now:
            _YT_SEARCH_IDS_CACHE.pop(key, None)
            return None
        return list(ids)


def _yt_search_ids_cache_set(
    query: str,
    n: int,
    ids: List[str],
    *,
    target_duration_sec: Optional[float] = None,
) -> None:
    ttl = float(YTDLP_SEARCH_CACHE_TTL_SEC)
    if ttl <= 0:
        return
    key = _yt_search_cache_key(query, n, target_duration_sec=target_duration_sec)
    now = time.monotonic()
    expires_at = now + ttl
    bounded = list(ids[: max(1, int(n))])
    with _YT_SEARCH_IDS_CACHE_LOCK:
        _YT_SEARCH_IDS_CACHE[key] = (expires_at, bounded)
        # Opportunistically prune expired entries and keep bounded memory.
        expired_keys = [k for k, (exp, _vals) in _YT_SEARCH_IDS_CACHE.items() if exp <= now]
        for stale_key in expired_keys:
            _YT_SEARCH_IDS_CACHE.pop(stale_key, None)
        max_entries = int(max(10, YTDLP_SEARCH_CACHE_MAX_ENTRIES))
        while len(_YT_SEARCH_IDS_CACHE) > max_entries:
            oldest_key = next(iter(_YT_SEARCH_IDS_CACHE))
            _YT_SEARCH_IDS_CACHE.pop(oldest_key, None)


def _yt_search_ids_disk_cache_path(query: str, n: int, *, target_duration_sec: Optional[float] = None) -> Path:
    key_payload = json.dumps(
        _yt_search_cache_key(query, n, target_duration_sec=target_duration_sec),
        sort_keys=True,
        ensure_ascii=True,
    )
    key_hash = hashlib.sha1(key_payload.encode("utf-8")).hexdigest()
    return YTDLP_SEARCH_DISK_CACHE_DIR / f"{key_hash}.json"


def _yt_search_ids_disk_cache_get(
    query: str,
    n: int,
    *,
    target_duration_sec: Optional[float] = None,
) -> Optional[List[str]]:
    ttl = float(YTDLP_SEARCH_DISK_CACHE_TTL_SEC)
    if ttl <= 0:
        return None
    path = _yt_search_ids_disk_cache_path(query, n, target_duration_sec=target_duration_sec)
    now_epoch = time.time()
    with _YT_SEARCH_IDS_DISK_CACHE_LOCK:
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        expires_at = float(payload.get("expires_at_epoch") or 0.0)
        if expires_at <= now_epoch:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            return None
        ids_raw = payload.get("ids")
        if not isinstance(ids_raw, list):
            return None
        out: List[str] = []
        for token in ids_raw:
            vid = str(token or "").strip()
            if _is_valid_source_id(vid) and vid not in out:
                out.append(vid)
            if len(out) >= max(1, int(n)):
                break
        if not out:
            return None
        return out


def _yt_search_ids_disk_cache_prune(*, now_mono: float, now_epoch: float) -> None:
    global _YT_SEARCH_IDS_DISK_CACHE_LAST_PRUNE_AT_MONO
    if (now_mono - _YT_SEARCH_IDS_DISK_CACHE_LAST_PRUNE_AT_MONO) < YTDLP_SEARCH_DISK_CACHE_PRUNE_INTERVAL_SEC:
        return
    _YT_SEARCH_IDS_DISK_CACHE_LAST_PRUNE_AT_MONO = now_mono

    cache_dir = YTDLP_SEARCH_DISK_CACHE_DIR
    if not cache_dir.exists():
        return

    try:
        cache_files = [p for p in cache_dir.glob("*.json") if p.is_file()]
    except Exception:
        return

    entries: List[Tuple[float, Path]] = []
    for path in cache_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            continue
        if not isinstance(payload, dict):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            continue
        expires_at = float(payload.get("expires_at_epoch") or 0.0)
        if expires_at <= now_epoch:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            continue
        try:
            mtime = float(path.stat().st_mtime)
        except Exception:
            mtime = now_epoch
        entries.append((mtime, path))

    max_entries = int(max(100, YTDLP_SEARCH_DISK_CACHE_MAX_ENTRIES))
    if len(entries) <= max_entries:
        return
    entries.sort(key=lambda item: item[0], reverse=True)
    for _, stale in entries[max_entries:]:
        try:
            stale.unlink(missing_ok=True)
        except Exception:
            pass


def _yt_search_ids_disk_cache_set(
    query: str,
    n: int,
    ids: List[str],
    *,
    target_duration_sec: Optional[float] = None,
) -> None:
    ttl = float(YTDLP_SEARCH_DISK_CACHE_TTL_SEC)
    if ttl <= 0:
        return
    bounded: List[str] = []
    for token in ids:
        vid = str(token or "").strip()
        if not _is_valid_source_id(vid):
            continue
        if vid in bounded:
            continue
        bounded.append(vid)
        if len(bounded) >= max(1, int(n)):
            break
    if not bounded:
        return

    now_epoch = time.time()
    now_mono = time.monotonic()
    payload = {
        "query": " ".join((query or "").split()).strip(),
        "n": int(max(1, n)),
        "ids": bounded,
        "expires_at_epoch": now_epoch + ttl,
        "updated_at_epoch": now_epoch,
    }
    path = _yt_search_ids_disk_cache_path(query, n, target_duration_sec=target_duration_sec)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with _YT_SEARCH_IDS_DISK_CACHE_LOCK:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
            tmp_path.replace(path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return
        _yt_search_ids_disk_cache_prune(now_mono=now_mono, now_epoch=now_epoch)


def _yt_search_singleflight_key(query: str, n: int, *, target_duration_sec: Optional[float] = None) -> str:
    raw = json.dumps(
        _yt_search_cache_key(query, n, target_duration_sec=target_duration_sec),
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _yt_search_singleflight_acquire(
    query: str,
    n: int,
    *,
    target_duration_sec: Optional[float] = None,
) -> Tuple[str, Optional[threading.Lock]]:
    if not YTDLP_SEARCH_SINGLEFLIGHT_ENABLED:
        return ("", None)
    key = _yt_search_singleflight_key(query, n, target_duration_sec=target_duration_sec)
    with _YT_SEARCH_SINGLEFLIGHT_LOCK:
        lock = _YT_SEARCH_SINGLEFLIGHT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _YT_SEARCH_SINGLEFLIGHT_LOCKS[key] = lock
            _YT_SEARCH_SINGLEFLIGHT_REFS[key] = 0
        _YT_SEARCH_SINGLEFLIGHT_REFS[key] = int(_YT_SEARCH_SINGLEFLIGHT_REFS.get(key, 0)) + 1
    return (key, lock)


def _yt_search_singleflight_release(key: str) -> None:
    if not key:
        return
    with _YT_SEARCH_SINGLEFLIGHT_LOCK:
        refs = int(_YT_SEARCH_SINGLEFLIGHT_REFS.get(key, 0)) - 1
        if refs <= 0:
            _YT_SEARCH_SINGLEFLIGHT_REFS.pop(key, None)
            _YT_SEARCH_SINGLEFLIGHT_LOCKS.pop(key, None)
        else:
            _YT_SEARCH_SINGLEFLIGHT_REFS[key] = refs


def yt_search_ids(
    query: str,
    n: int,
    *,
    timeout_sec: Optional[float] = None,
    target_duration_sec: Optional[float] = None,
) -> List[str]:
    """
    Robust yt-dlp search for source IDs.

    Server-side notes:
    - Adds UA/headers/cookies if configured
    - Tries both primary extractor args and fallback extractor args when search fails
    """
    cached = _yt_search_ids_cache_get(query, n, target_duration_sec=target_duration_sec)
    if cached is not None:
        log("YT", "cache hit for yt-search query=%r n=%s ids=%s" % (query, n, len(cached)), GREEN)
        return cached
    disk_cached = _yt_search_ids_disk_cache_get(query, n, target_duration_sec=target_duration_sec)
    if disk_cached is not None:
        _yt_search_ids_cache_set(query, n, disk_cached, target_duration_sec=target_duration_sec)
        log("YT", "disk cache hit for yt-search query=%r n=%s ids=%s" % (query, n, len(disk_cached)), GREEN)
        return disk_cached

    singleflight_key, singleflight_lock = _yt_search_singleflight_acquire(
        query,
        n,
        target_duration_sec=target_duration_sec,
    )

    def _search_uncached() -> List[str]:
        effective_timeout = float(YTDLP_SEARCH_TIMEOUT if timeout_sec is None else max(1.0, timeout_sec))
        requested_n = max(1, int(n))
        effective_search_n = requested_n
        include_duration_metadata = bool(
            (target_duration_sec is not None and float(target_duration_sec) > 0.0)
            or (not MP3_SIMPLE_FIRST_RESULT_MODE)
        )
        if (
            MP3_PREFER_NON_LIVE_VERSION
            and requested_n <= 1
            and (not _query_requests_live_version(query))
        ):
            effective_search_n = max(requested_n, int(MP3_NON_LIVE_MIN_SEARCH_N))

        def _run(extractor_args: str) -> Tuple[subprocess.CompletedProcess, str]:
            cmd = [*YTDLP_CMD]
            if YTDLP_NO_WARNINGS:
                cmd.append("--no-warnings")
            if YTDLP_VERBOSE:
                cmd.append("--verbose")

            cmd += [
                "--flat-playlist",
                "--print",
                "%(id)s\t%(title)s\t%(duration)s\t%(view_count)s"
                if include_duration_metadata
                else "%(id)s\t%(title)s",
                "--force-ipv4",
                "--socket-timeout",
                str(YTDLP_SOCKET_TIMEOUT),
            ]

            if YTDLP_UA:
                cmd += ["--user-agent", str(YTDLP_UA)]
            for hdr in YTDLP_EXTRA_HEADERS:
                cmd += ["--add-headers", hdr]

            cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())
            if cookies_configured:
                cookies_path = _writable_cookies_path()
                if cookies_path:
                    cmd += ["--cookies", cookies_path]

            if extractor_args:
                cmd += ["--extractor-args", str(extractor_args)]

            proxy = _current_proxy()
            if proxy:
                cmd += ["--proxy", str(proxy)]

            cmd += ["ytsearch%d:%s" % (effective_search_n, query)]

            log("YT", " ".join(cmd), CYAN)
            return subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout), proxy

        # Avoid repeating identical search calls when extractor-arg variants collapse
        # to the same value (common in prod when primary/fallback are both blank).
        plan: List[Tuple[str, str]] = []
        seen_args: set[str] = set()
        for label, args in (
            ("primary", YTDLP_EXTRACTOR_ARGS),
            ("fallback", YTDLP_FALLBACK_EXTRACTOR_ARGS),
            ("bare", ""),
        ):
            normalized = (args or "").strip()
            if normalized in seen_args:
                continue
            seen_args.add(normalized)
            plan.append((label, normalized))

        last_diag = ""
        for label, args in plan:
            for proxy_try in range(_proxy_retry_budget()):
                try:
                    p, used_proxy = _run(args)
                except subprocess.TimeoutExpired:
                    last_diag = "yt-dlp search timed out after %.1fs (%s)" % (effective_timeout, label)
                    break
                except FileNotFoundError as e:
                    raise RuntimeError(_ytdlp_missing_message()) from e

                if p.returncode == 0:
                    _mark_proxy_success(used_proxy)
                    rows: List[Tuple[str, str, Optional[float], Optional[int]]] = []
                    for line in (p.stdout or "").splitlines():
                        raw = (line or "").strip()
                        if not raw:
                            continue
                        if _is_valid_source_id(raw):
                            rows.append((raw, "", None, None))
                            continue
                        if "\t" in raw:
                            parts = raw.split("\t")
                            vid_part = parts[0] if len(parts) >= 1 else ""
                            title_part = parts[1] if len(parts) >= 2 else ""
                            duration_part = parts[2] if len(parts) >= 3 else ""
                            view_count_part = parts[3] if len(parts) >= 4 else ""
                            vid = (vid_part or "").strip()
                            if _is_valid_source_id(vid):
                                duration_val = _float_or_none((duration_part or "").strip())
                                view_count_val = _int_or_none((view_count_part or "").strip())
                                rows.append((vid, (title_part or "").strip(), duration_val, view_count_val))
                                continue
                        # Backward-compatible parse in case old yt-dlp emits JSON here.
                        try:
                            j = json.loads(raw)
                            vid = str(j.get("id") or "").strip()
                            if _is_valid_source_id(vid):
                                rows.append(
                                    (
                                        vid,
                                        str(j.get("title") or "").strip(),
                                        _float_or_none(j.get("duration")),
                                        _int_or_none(j.get("view_count")),
                                    )
                                )
                        except Exception:
                            continue

                    # stable de-dupe
                    seen = set()
                    out: List[str] = []
                    title_by_id: Dict[str, str] = {}
                    duration_by_id: Dict[str, float] = {}
                    view_count_by_id: Dict[str, int] = {}
                    for vid, title, duration, view_count in rows:
                        if vid not in seen:
                            seen.add(vid)
                            out.append(vid)
                        if vid not in title_by_id:
                            title_by_id[vid] = title
                        if vid not in duration_by_id and duration is not None and duration > 0.0:
                            duration_by_id[vid] = float(duration)
                        if vid not in view_count_by_id and view_count is not None and view_count >= 0:
                            view_count_by_id[vid] = int(view_count)

                    if out:
                        out = _prioritize_search_ids_for_query(
                            query,
                            out,
                            title_by_id,
                            duration_by_id=duration_by_id,
                            view_count_by_id=view_count_by_id,
                            target_duration_sec=target_duration_sec,
                        )
                    if out and target_duration_sec is not None and target_duration_sec > 0.0:
                        top = out[0]
                        top_duration = duration_by_id.get(top)
                        top_views = view_count_by_id.get(top)
                        if top_duration is not None:
                            log(
                                "YT",
                                "duration-aware pick id=%s target=%.3fs got=%.3fs delta=%.3fs views=%s"
                                % (
                                    top,
                                    float(target_duration_sec),
                                    float(top_duration),
                                    abs(float(top_duration) - float(target_duration_sec)),
                                    str(top_views if top_views is not None else "n/a"),
                                ),
                                CYAN,
                            )

                    if out:
                        _yt_search_ids_cache_set(query, n, out, target_duration_sec=target_duration_sec)
                        _yt_search_ids_disk_cache_set(query, n, out, target_duration_sec=target_duration_sec)
                        return out

                    last_diag = "yt-dlp search returned no IDs (%s)" % label
                    break

                last_diag = _collect_ytdlp_diagnostics(p.stderr or "", p.stdout or "") or ("yt-dlp search failed (%s)" % label)
                _mark_proxy_failure(used_proxy, reason="yt_search_failed")
                if _should_rotate_proxy_on_error(last_diag) and proxy_try + 1 < _proxy_retry_budget():
                    _rotate_proxy("yt_search_ids")
                    continue
                break

        raise RuntimeError("yt-dlp search failed\\n%s" % last_diag)

    if singleflight_lock is None:
        return _search_uncached()

    waited_t0 = now_perf_ms()
    try:
        with singleflight_lock:
            waited_ms = max(0.0, now_perf_ms() - waited_t0)
            if waited_ms >= 10.0:
                log("YT", "Waiting for in-flight yt-search query=%r wait_ms=%.1f" % (query, waited_ms), CYAN)
            cached = _yt_search_ids_cache_get(query, n, target_duration_sec=target_duration_sec)
            if cached is not None:
                log("YT", "cache hit for yt-search query=%r n=%s ids=%s" % (query, n, len(cached)), GREEN)
                return cached
            disk_cached = _yt_search_ids_disk_cache_get(query, n, target_duration_sec=target_duration_sec)
            if disk_cached is not None:
                _yt_search_ids_cache_set(query, n, disk_cached, target_duration_sec=target_duration_sec)
                log("YT", "disk cache hit for yt-search query=%r n=%s ids=%s" % (query, n, len(disk_cached)), GREEN)
                return disk_cached
            return _search_uncached()
    finally:
        _yt_search_singleflight_release(singleflight_key)


def _yt_audio_disk_cache_key(source_id: str) -> str:
    payload = {
        "source_id": str(source_id or "").strip(),
        "fast_no_transcode": bool(STEP1_FAST_NO_TRANSCODE),
        "fast_preferred_audio_only_format": str(STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT or ""),
        "fast_audio_only_format": str(STEP1_FAST_AUDIO_ONLY_FORMAT or ""),
        "fast_no_transcode_format": str(STEP1_FAST_NO_TRANSCODE_FORMAT or ""),
        "audio_quality": str(YTDLP_AUDIO_QUALITY),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _yt_audio_disk_cache_dir(source_id: str) -> Path:
    return YTDLP_AUDIO_DISK_CACHE_DIR / _yt_audio_disk_cache_key(source_id)


def _yt_audio_singleflight_key(source_id: str) -> str:
    if not _is_valid_source_id(source_id):
        return ""
    return _yt_audio_disk_cache_key(source_id)


def _yt_audio_singleflight_acquire(source_id: str) -> Tuple[str, Optional[threading.Lock]]:
    if not YTDLP_AUDIO_SINGLEFLIGHT_ENABLED:
        return ("", None)
    key = _yt_audio_singleflight_key(source_id)
    if not key:
        return ("", None)

    with _YT_AUDIO_SINGLEFLIGHT_LOCK:
        lock = _YT_AUDIO_SINGLEFLIGHT_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _YT_AUDIO_SINGLEFLIGHT_LOCKS[key] = lock
            _YT_AUDIO_SINGLEFLIGHT_REFS[key] = 0
        _YT_AUDIO_SINGLEFLIGHT_REFS[key] = int(_YT_AUDIO_SINGLEFLIGHT_REFS.get(key, 0)) + 1
    return (key, lock)


def _yt_audio_singleflight_release(key: str) -> None:
    if not key:
        return
    with _YT_AUDIO_SINGLEFLIGHT_LOCK:
        refs = int(_YT_AUDIO_SINGLEFLIGHT_REFS.get(key, 0)) - 1
        if refs <= 0:
            _YT_AUDIO_SINGLEFLIGHT_REFS.pop(key, None)
            _YT_AUDIO_SINGLEFLIGHT_LOCKS.pop(key, None)
        else:
            _YT_AUDIO_SINGLEFLIGHT_REFS[key] = refs


def _yt_audio_disk_cache_prune(*, now_mono: float, now_epoch: float) -> None:
    global _YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO
    if not YTDLP_AUDIO_DISK_CACHE_ENABLED:
        return
    if (now_mono - _YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO) < YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC:
        return

    with _YT_AUDIO_DISK_CACHE_LOCK:
        if (now_mono - _YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO) < YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC:
            return
        _YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO = now_mono

        cache_root = YTDLP_AUDIO_DISK_CACHE_DIR
        if not cache_root.exists():
            return
        try:
            cache_dirs = [p for p in cache_root.iterdir() if p.is_dir() and not p.name.startswith(".tmp-")]
        except Exception:
            return

        survivors: List[Tuple[float, Path]] = []
        ttl = float(YTDLP_AUDIO_DISK_CACHE_TTL_SEC)
        for cache_dir in cache_dirs:
            meta_path = cache_dir / ".meta.json"
            try:
                if not meta_path.exists():
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                rel_name = str(payload.get("audio_file") or "").strip()
                if not rel_name or Path(rel_name).name != rel_name:
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                audio_path = cache_dir / rel_name
                if (not audio_path.exists()) or (not audio_path.is_file()):
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                try:
                    audio_size = int(audio_path.stat().st_size)
                except Exception:
                    audio_size = 0
                if audio_size < int(YTDLP_AUDIO_DISK_CACHE_MIN_BYTES):
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                updated_at_epoch = float(payload.get("updated_at_epoch") or 0.0)
                if updated_at_epoch <= 0.0:
                    updated_at_epoch = max(float(meta_path.stat().st_mtime), float(audio_path.stat().st_mtime))
                if ttl > 0.0 and (now_epoch - updated_at_epoch) > ttl:
                    shutil.rmtree(cache_dir, ignore_errors=True)
                    continue
                survivors.append((updated_at_epoch, cache_dir))
            except Exception:
                shutil.rmtree(cache_dir, ignore_errors=True)
                continue

        max_entries = int(max(20, YTDLP_AUDIO_DISK_CACHE_MAX_ENTRIES))
        if len(survivors) <= max_entries:
            return
        survivors.sort(key=lambda item: item[0], reverse=True)
        for _updated, stale_dir in survivors[max_entries:]:
            shutil.rmtree(stale_dir, ignore_errors=True)


def _yt_audio_disk_cache_restore(source_id: str, out_mp3: Path) -> Optional[Path]:
    if (not YTDLP_AUDIO_DISK_CACHE_ENABLED) or (not _is_valid_source_id(source_id)):
        return None

    now_epoch = time.time()
    now_mono = time.monotonic()
    _yt_audio_disk_cache_prune(now_mono=now_mono, now_epoch=now_epoch)

    cache_dir = _yt_audio_disk_cache_dir(source_id)
    meta_path = cache_dir / ".meta.json"
    with _YT_AUDIO_DISK_CACHE_LOCK:
        if not meta_path.exists():
            return None
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        rel_name = str(payload.get("audio_file") or "").strip()
        if not rel_name or Path(rel_name).name != rel_name:
            return None
        cached_audio = cache_dir / rel_name
        if (not cached_audio.exists()) or (not cached_audio.is_file()):
            return None
        try:
            cache_size = int(cached_audio.stat().st_size)
        except Exception:
            return None
        if cache_size < int(YTDLP_AUDIO_DISK_CACHE_MIN_BYTES):
            return None
        updated_at_epoch = float(payload.get("updated_at_epoch") or 0.0)
        if updated_at_epoch <= 0.0:
            updated_at_epoch = float(cached_audio.stat().st_mtime)
        ttl = float(YTDLP_AUDIO_DISK_CACHE_TTL_SEC)
        if ttl > 0.0 and (now_epoch - updated_at_epoch) > ttl:
            return None

        ext = str(cached_audio.suffix or "").lower()
        if (not STEP1_FAST_NO_TRANSCODE) and ext != ".mp3":
            return None
        target = out_mp3 if ext == ".mp3" else out_mp3.with_suffix(ext)
        tmp_target = target.with_suffix(target.suffix + ".cachetmp")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(cached_audio), str(tmp_target))
            tmp_target.replace(target)
            if ext != ".mp3" and STEP1_FAST_ALIAS_MP3 and target != out_mp3:
                try:
                    if out_mp3.exists() or out_mp3.is_symlink():
                        out_mp3.unlink()
                except Exception:
                    pass
                try:
                    out_mp3.symlink_to(target.name)
                except Exception:
                    try:
                        shutil.copy2(str(target), str(out_mp3))
                    except Exception:
                        pass
            payload["updated_at_epoch"] = now_epoch
            try:
                meta_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
            except Exception:
                pass
            log("MP3", "audio disk cache hit for source_id=%s ext=%s" % (source_id, ext or ".mp3"), GREEN)
            return target
        except Exception:
            try:
                tmp_target.unlink(missing_ok=True)
            except Exception:
                pass
            return None


def _yt_audio_disk_cache_store(source_id: str, produced_audio: Path) -> None:
    if (not YTDLP_AUDIO_DISK_CACHE_ENABLED) or (not _is_valid_source_id(source_id)):
        return
    try:
        if (not produced_audio.exists()) or (not produced_audio.is_file()):
            return
        produced_size = int(produced_audio.stat().st_size)
    except Exception:
        return
    if produced_size < int(YTDLP_AUDIO_DISK_CACHE_MIN_BYTES):
        return

    ext = str(produced_audio.suffix or "").lower()
    if (not STEP1_FAST_NO_TRANSCODE) and ext != ".mp3":
        return
    if not ext:
        ext = ".bin"

    now_epoch = time.time()
    now_mono = time.monotonic()
    _yt_audio_disk_cache_prune(now_mono=now_mono, now_epoch=now_epoch)

    cache_dir = _yt_audio_disk_cache_dir(source_id)
    tmp_dir = cache_dir.parent / (
        ".tmp-%s-%d-%d" % (cache_dir.name, os.getpid(), int(now_epoch * 1000))
    )

    with _YT_AUDIO_DISK_CACHE_LOCK:
        try:
            meta_path = cache_dir / ".meta.json"
            if meta_path.exists():
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    rel_name = str(payload.get("audio_file") or "").strip()
                    candidate = cache_dir / rel_name
                    if (
                        rel_name
                        and Path(rel_name).name == rel_name
                        and candidate.exists()
                        and candidate.is_file()
                        and int(candidate.stat().st_size) >= int(YTDLP_AUDIO_DISK_CACHE_MIN_BYTES)
                    ):
                        payload["updated_at_epoch"] = now_epoch
                        meta_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
                        return
        except Exception:
            pass

        try:
            if cache_dir.exists():
                shutil.rmtree(cache_dir, ignore_errors=True)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            cache_audio = tmp_dir / ("audio%s" % ext)
            shutil.copy2(str(produced_audio), str(cache_audio))
            payload = {
                "source_id": str(source_id),
                "audio_file": cache_audio.name,
                "audio_ext": ext,
                "audio_size": int(cache_audio.stat().st_size),
                "updated_at_epoch": now_epoch,
            }
            (tmp_dir / ".meta.json").write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
            cache_dir.parent.mkdir(parents=True, exist_ok=True)
            tmp_dir.replace(cache_dir)
        except Exception:
            pass
        finally:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)


# ─────────────────────────────────────────────
# yt-dlp download
# ─────────────────────────────────────────────


def _default_concurrent_fragments() -> int:
    try:
        return max(1, int(str(YTDLP_CONCURRENT_FRAGS).strip()))
    except Exception:
        return 2


def _resolve_concurrent_fragments(
    *,
    source: str,
    deadline_monotonic: Optional[float],
    hot_query: bool,
) -> int:
    base = _default_concurrent_fragments()
    if not YTDLP_CONCURRENT_FRAGS_ADAPTIVE:
        return min(int(YTDLP_CONCURRENT_FRAGS_MAX), base)

    value = base
    if source.startswith("ytsearch"):
        value = max(value, min(int(YTDLP_CONCURRENT_FRAGS_MAX), base + 1))
    if hot_query:
        value = max(value, int(YTDLP_CONCURRENT_FRAGS_HOT_QUERY))

    if deadline_monotonic is not None:
        remaining = float(deadline_monotonic - time.monotonic())
        if remaining <= float(YTDLP_CONCURRENT_FRAGS_TIGHT_REMAINING_SEC):
            value = min(value, int(YTDLP_CONCURRENT_FRAGS_TIGHT_VALUE))

    return max(1, min(int(YTDLP_CONCURRENT_FRAGS_MAX), int(value)))


def _yt_dlp_base_common(
    outtmpl: str,
    *,
    fmt: str,
    extractor_args: Optional[str],
    # Whether the input is a search playlist spec (e.g. ytsearch/scsearch) rather than a direct URL/ID.
    search_source: bool,
    use_cookies: bool,
    transcode_to_mp3: bool,
    concurrent_fragments: Optional[int] = None,
) -> List[str]:
    effective_concurrent_fragments = (
        max(1, int(concurrent_fragments))
        if concurrent_fragments is not None
        else _default_concurrent_fragments()
    )
    base = [*YTDLP_CMD]
    if transcode_to_mp3:
        base += [
            "-x",
            "--audio-format",
            "mp3",
            "--audio-quality",
            str(YTDLP_AUDIO_QUALITY),
        ]

    base += [
        "--force-ipv4",
        "--socket-timeout", str(YTDLP_SOCKET_TIMEOUT),
        "--retries", str(YTDLP_RETRIES),
        "--fragment-retries", str(YTDLP_FRAG_RETRIES),
        "--concurrent-fragments", str(effective_concurrent_fragments),
        "-o", outtmpl,
    ]
    if YTDLP_UA:
        base += ["--user-agent", str(YTDLP_UA)]
    if YTDLP_NO_WARNINGS:
        base += ["--no-warnings"]
    if YTDLP_VERBOSE:
        base += ["--verbose"]

    if not search_source:
        base += ["--no-playlist"]

    for hdr in YTDLP_EXTRA_HEADERS:
        base += ["--add-headers", hdr]

    if fmt:
        base += ["-f", fmt]
    if extractor_args:
        base += ["--extractor-args", str(extractor_args)]
    if YTDLP_JS_RUNTIMES:
        base += ["--js-runtimes", str(YTDLP_JS_RUNTIMES)]
    if YTDLP_REMOTE_COMPONENTS:
        base += ["--remote-components", str(YTDLP_REMOTE_COMPONENTS)]
    proxy = _current_proxy()
    if proxy:
        base += ["--proxy", str(proxy)]

    if use_cookies:
        cookies_path = _writable_cookies_path()
        if cookies_path:
            log("MP3", "Using cookies: %s" % cookies_path, CYAN)
            base += ["--cookies", cookies_path]

    # If aria2c is available, it can be faster on some networks
    if shutil.which("aria2c"):
        base += [
            "--downloader", "aria2c",
            "--downloader-args", "aria2c:-x16 -s16 -k1M",
        ]

    # avoid live streams
    base += ["--match-filter", "is_live != True"]

    if search_source:
        # For ytsearch playlists, continue on bad entries and stop once one download succeeds.
        base += ["--ignore-errors", "--max-downloads", "1"]

    return base


def _is_sig_or_forbidden_or_transient(text: str) -> bool:
    low = (text or "").lower()
    return (
        "signature" in low
        or "nsig" in low
        or "forbidden" in low
        or "http error 403" in low
        or "http error 429" in low
        or "too many requests" in low
        or "captcha" in low
        or "confirm you\'re not a bot" in low
    )


def _is_signin_or_cookie_error(text: str) -> bool:
    low = (text or "").lower()
    return (
        "sign in to confirm you" in low
        or "--cookies-from-browser" in low
        or "--cookies for the authentication" in low
        or "cookie_refresh_required" in low
    )


def _is_bot_or_cookie_gate_error(text: str) -> bool:
    low = (text or "").lower()
    return (
        _is_signin_or_cookie_error(text)
        or ("confirm you" in low and "not a bot" in low)
        or "captcha" in low
    )


def _is_format_selection_error(text: str) -> bool:
    low = (text or "").lower()
    return (
        "requested format is not available" in low
        or "format is not available" in low
        or "no video formats found" in low
    )


def _has_runtime_cookies_configured() -> bool:
    return bool((YTDLP_COOKIES_PATH or "").strip())


def _should_fail_fast_on_bot_gate(text: str) -> bool:
    if not MP3_FAIL_FAST_ON_BOT_GATE_NO_COOKIES:
        return False
    return _is_bot_or_cookie_gate_error(text) and (not _has_runtime_cookies_configured())


_URL_IN_TEXT_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
def _normalize_downloaded_mp3(out_mp3: Path, *, source_label: str) -> None:
    produced = out_mp3 if out_mp3.exists() else None

    if produced is None:
        stem = out_mp3.stem
        matches = sorted(
            out_mp3.parent.glob(stem + ".*"),
            key=lambda x: x.stat().st_size if x.exists() else 0,
            reverse=True,
        )
        for m in matches:
            if m.exists() and m.stat().st_size > 0:
                produced = m
                break

    if produced is None or produced.stat().st_size == 0:
        raise RuntimeError("MP3 download produced no usable file for %s" % source_label)

    if produced != out_mp3:
        try:
            if out_mp3.exists():
                out_mp3.unlink()
            produced.rename(out_mp3)
        except Exception:
            shutil.copy2(str(produced), str(out_mp3))
            try:
                produced.unlink()
            except Exception:
                pass

    if not out_mp3.exists() or out_mp3.stat().st_size == 0:
        raise RuntimeError("MP3 download produced empty file for %s" % source_label)


def _finalize_downloaded_audio_output(
    out_mp3: Path,
    *,
    source_label: str,
    pre_existing: Optional[Dict[str, int]] = None,
    preferred_path: Optional[Path] = None,
) -> Path:
    produced: Optional[Path] = None
    if preferred_path is not None:
        try:
            if preferred_path.exists() and preferred_path.is_file() and preferred_path.stat().st_size > 0:
                produced = preferred_path
        except Exception:
            produced = None

    if produced is None:
        candidates: List[tuple[bool, int, int, Path]] = []
        stem = out_mp3.stem
        prior_outputs = pre_existing or {}
        for m in out_mp3.parent.glob(stem + ".*"):
            if not m.exists() or not m.is_file():
                continue
            try:
                stat = m.stat()
            except Exception:
                continue
            if stat.st_size <= 0:
                continue
            key = str(m.resolve())
            prev_mtime = int(prior_outputs.get(key, -1))
            is_new = prev_mtime < 0 or int(stat.st_mtime_ns) > prev_mtime
            candidates.append((is_new, int(stat.st_mtime_ns), int(stat.st_size), m))

        if candidates:
            candidates.sort(key=lambda row: (1 if row[0] else 0, row[1], row[2]), reverse=True)
            produced = candidates[0][3]

    if produced is None and out_mp3.exists() and out_mp3.stat().st_size > 0:
        produced = out_mp3

    if produced is None or produced.stat().st_size == 0:
        raise RuntimeError("MP3 download produced no usable file for %s" % source_label)

    # Fast native mode keeps source codec/container and returns real file path.
    if STEP1_FAST_NO_TRANSCODE and produced.suffix.lower() != ".mp3":
        # Direct/race downloads can emit transient names like "<slug>.race1.mp4".
        # Normalize to a stable canonical path so downstream steps never rely
        # on ephemeral artifact names.
        stable_native = out_mp3.with_suffix((produced.suffix or ".m4a").lower())
        if produced != stable_native:
            try:
                if stable_native.exists() or stable_native.is_symlink():
                    stable_native.unlink()
            except Exception:
                pass
            try:
                produced.rename(stable_native)
                produced = stable_native
            except Exception:
                try:
                    shutil.copy2(str(produced), str(stable_native))
                    try:
                        produced.unlink()
                    except Exception:
                        pass
                    produced = stable_native
                except Exception:
                    pass

        if STEP1_FAST_ALIAS_MP3 and produced != out_mp3:
            try:
                if out_mp3.exists() or out_mp3.is_symlink():
                    out_mp3.unlink()
            except Exception:
                pass

            try:
                out_mp3.symlink_to(produced.name)
                log("MP3", "Created MP3 alias symlink: %s -> %s" % (out_mp3.name, produced.name), CYAN)
            except Exception:
                try:
                    shutil.copy2(str(produced), str(out_mp3))
                    log("MP3", "Created MP3 alias copy: %s <- %s" % (out_mp3.name, produced.name), CYAN)
                except Exception as e:
                    log("MP3", "MP3 alias creation failed (%s): %s" % (out_mp3.name, e), YELLOW)

        log("MP3", "Fast no-transcode source selected: %s" % produced.name, CYAN)
        return produced

    if produced != out_mp3:
        try:
            if out_mp3.exists() or out_mp3.is_symlink():
                out_mp3.unlink()
            produced.rename(out_mp3)
        except Exception:
            shutil.copy2(str(produced), str(out_mp3))
            try:
                produced.unlink()
            except Exception:
                pass

    if not out_mp3.exists() or out_mp3.stat().st_size == 0:
        raise RuntimeError("MP3 download produced empty file for %s" % source_label)

    return out_mp3


def _extract_source_id_from_url(raw_url: str) -> Optional[str]:
    parsed = urlparse((raw_url or "").strip())
    host = (parsed.netloc or "").lower().split(":", 1)[0]
    path = parsed.path or ""

    # Accept both real provider hosts and our generic "source" aliases to avoid leaking provider
    # strings into user-facing surfaces.
    if host in {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "source.com",
        "www.source.com",
        "m.source.com",
        "music.source.com",
    }:
        q = parse_qs(parsed.query or "")
        v = (q.get("v") or [None])[0]
        if v and _source_ID_RE.match(v):
            return v
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live"} and _source_ID_RE.match(parts[1]):
            return parts[1]
        return None

    if host in {"youtu.be", "www.youtu.be", "source.be", "www.source.be"}:
        vid = (path or "").lstrip("/").split("/", 1)[0]
        return vid if _source_ID_RE.match(vid or "") else None

    return None


def _unwrap_query_text(raw: str) -> str:
    q = (raw or "").strip()
    # Accept wrapped input like '"https://youtu.be/..."' from chat/copy-paste.
    for _ in range(2):
        if len(q) >= 2 and q[0] == q[-1] and q[0] in {"'", '"', "`"}:
            q = q[1:-1].strip()
        else:
            break
    return q


def _extract_first_url_from_text(raw: str) -> Optional[str]:
    m = _URL_IN_TEXT_RE.search(raw or "")
    if not m:
        return None
    # Trim punctuation that may trail pasted URLs in free-form text.
    return m.group(0).rstrip(").,;\"'")


def _direct_source_source_from_query(query: str) -> Optional[Tuple[str, str]]:
    q = _unwrap_query_text(query)
    if not q:
        return None

    if _source_ID_RE.match(q):
        return q, "https://www.youtube.com/watch?v=%s" % q

    source = q
    if not (source.startswith("https://") or source.startswith("http://")):
        embedded = _extract_first_url_from_text(q)
        if not embedded:
            return None
        source = embedded

    vid = _extract_source_id_from_url(source)
    if not vid:
        return None
    # If user pasted a provider URL, keep it; otherwise we use the generic source alias.
    return vid, source


def _yt_download_direct_fast(
    source: str,
    out_mp3: Path,
    *,
    source_label: str,
    deadline_monotonic: Optional[float] = None,
    hot_query: bool = False,
    prefer_broad_format: bool = False,
) -> Path:
    """
    One-shot fast path for direct source URL/ID input.
    This intentionally skips multi-attempt search/failover logic.
    """
    ensure_dir(out_mp3.parent)
    outtmpl = str(out_mp3.with_suffix(".%(ext)s"))
    pre_existing: Dict[str, int] = {}
    stem = out_mp3.stem
    for p in out_mp3.parent.glob(stem + ".*"):
        if not p.is_file():
            continue
        try:
            pre_existing[str(p.resolve())] = int(p.stat().st_mtime_ns)
        except Exception:
            continue
    concurrent_fragments = _resolve_concurrent_fragments(
        source=source,
        deadline_monotonic=deadline_monotonic,
        hot_query=hot_query,
    )
    transcode_to_mp3 = not STEP1_FAST_NO_TRANSCODE

    effective_timeout = float(MP3_DIRECT_CMD_TIMEOUT)
    if deadline_monotonic is not None:
        remaining_total = deadline_monotonic - time.monotonic()
        if remaining_total <= 0:
            raise RuntimeError(
                "yt-dlp direct fast-path skipped for %s (total timeout exhausted)"
                % source_label
            )
        effective_timeout = min(effective_timeout, max(1.0, remaining_total))

    proxy = _current_proxy()

    def _build_cmd(fmt: str, *, extractor_args_override: Optional[str] = None) -> List[str]:
        cmd: List[str] = [
            *YTDLP_CMD,
            "--no-playlist",
            "--no-warnings",
            "--force-ipv4",
            "--socket-timeout",
            str(MP3_DIRECT_SOCKET_TIMEOUT),
            "--retries",
            str(MP3_DIRECT_RETRIES),
            "--fragment-retries",
            str(MP3_DIRECT_FRAG_RETRIES),
            "--concurrent-fragments",
            str(concurrent_fragments),
            "--match-filter",
            "is_live != True",
            "-o",
            outtmpl,
        ]
        if transcode_to_mp3:
            cmd[1:1] = [
                "-x",
                "--audio-format",
                "mp3",
                "--audio-quality",
                str(YTDLP_AUDIO_QUALITY),
            ]
        else:
            cmd += ["-f", str(fmt or "bestaudio")]

        if YTDLP_UA:
            cmd += ["--user-agent", str(YTDLP_UA)]
        for hdr in YTDLP_EXTRA_HEADERS:
            cmd += ["--add-headers", hdr]

        effective_extractor_args = (
            MP3_DIRECT_EXTRACTOR_ARGS
            if extractor_args_override is None
            else str(extractor_args_override or "")
        ).strip()
        if effective_extractor_args:
            cmd += ["--extractor-args", str(effective_extractor_args)]
        if YTDLP_JS_RUNTIMES:
            cmd += ["--js-runtimes", str(YTDLP_JS_RUNTIMES)]
        if YTDLP_REMOTE_COMPONENTS:
            cmd += ["--remote-components", str(YTDLP_REMOTE_COMPONENTS)]
        if proxy:
            cmd += ["--proxy", str(proxy)]

        if MP3_PRIMARY_USE_COOKIES:
            cookies_path = _writable_cookies_path()
            if cookies_path:
                cmd += ["--cookies", cookies_path]

        cmd += [source]
        return cmd

    attempt_candidates: List[Tuple[str, Optional[str]]] = []
    if transcode_to_mp3:
        attempt_candidates = [("", None)]
    else:
        preferred_audio_only = str(STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT or "").strip()
        strict_audio_only = str(STEP1_FAST_AUDIO_ONLY_FORMAT or "").strip()
        broad_fast_format = str(STEP1_FAST_NO_TRANSCODE_FORMAT or "bestaudio").strip() or "bestaudio"
        if prefer_broad_format and broad_fast_format:
            attempt_candidates = [(broad_fast_format, None)]
        else:
            for fmt, extractor_args in (
                (preferred_audio_only, ""),
                (strict_audio_only, ""),
                (broad_fast_format, None),
            ):
                if fmt and (fmt, extractor_args) not in attempt_candidates:
                    attempt_candidates.append((fmt, extractor_args))
        if not attempt_candidates:
            attempt_candidates = [("bestaudio", None)]

    last_msg = ""
    for idx, (fmt, extractor_args_override) in enumerate(attempt_candidates):
        cmd = _build_cmd(fmt, extractor_args_override=extractor_args_override)
        fmt_label = "transcode_mp3" if transcode_to_mp3 else fmt
        if idx == 0:
            log("MP3", "Direct fast-path: %s" % " ".join(cmd), CYAN)
        else:
            log("MP3", "Direct fast-path fallback format=%s: %s" % (fmt_label, " ".join(cmd)), CYAN)
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout)
        except subprocess.TimeoutExpired:
            _mark_proxy_failure(proxy, reason="yt_direct_fast_timeout")
            raise RuntimeError(
                "yt-dlp direct fast-path timed out for %s after %.1fs"
                % (source_label, effective_timeout)
            )
        except FileNotFoundError:
            raise RuntimeError(_ytdlp_missing_message())

        if p.returncode == 0:
            _mark_proxy_success(proxy)
            return _finalize_downloaded_audio_output(
                out_mp3,
                source_label=source_label,
                pre_existing=pre_existing,
            )

        msg = _collect_ytdlp_diagnostics(p.stderr or "", p.stdout or "")
        last_msg = msg or last_msg
        if (
            (idx + 1) < len(attempt_candidates)
            and _is_format_selection_error(msg)
        ):
            log(
                "MP3",
                "Direct fast-path audio-only format unavailable; retrying broader format",
                YELLOW,
            )
            continue

        _mark_proxy_failure(proxy, reason="yt_direct_fast_failed")
        if _should_rotate_proxy_on_error(msg):
            _rotate_proxy("yt_direct_fast")
        raise RuntimeError(
            "yt-dlp direct fast-path failed for %s\n%s"
            % (source_label, msg or "yt-dlp failed without diagnostics")
        )

    _mark_proxy_failure(proxy, reason="yt_direct_fast_failed")
    if _should_rotate_proxy_on_error(last_msg):
        _rotate_proxy("yt_direct_fast")
    raise RuntimeError(
        "yt-dlp direct fast-path failed for %s\n%s"
        % (source_label, last_msg or "yt-dlp failed without diagnostics")
    )


def _yt_download_from_source(
    source: str,
    out_mp3: Path,
    *,
    source_label: str,
    deadline_monotonic: Optional[float] = None,
    hot_query: bool = False,
) -> Path:
    """
    Download audio from a yt-dlp source.

    Default mode:
    - Transcode to MP3 and return `out_mp3`.

    Fast mode (`MIXTERIOSO_STEP1_FAST_NO_TRANSCODE=1`):
    - Preserve native source codec/container and return the real produced file path.
    - Optionally create/update `<slug>.mp3` alias for backward compatibility.
    """
    ensure_dir(out_mp3.parent)

    outtmpl = str(out_mp3.with_suffix(".%(ext)s"))
    source_for_log = source if len(source) <= 140 else (source[:137] + "...")

    pre_existing: Dict[str, int] = {}
    stem = out_mp3.stem
    for p in out_mp3.parent.glob(stem + ".*"):
        if not p.is_file():
            continue
        try:
            pre_existing[str(p.resolve())] = int(p.stat().st_mtime_ns)
        except Exception:
            continue

    def _run_attempt(
        label: str,
        fmt: str,
        extractor_args: str,
        *,
        use_cookies: bool,
        attempt_timeout_sec: float,
        outtmpl_override: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> tuple[int, str]:
        search_source = source.startswith(("ytsearch", "scsearch"))
        is_soundcloud_source = source.startswith("scsearch")

        # SoundCloud sources do not benefit from source cookies/extractor args and
        # may fail or slow down when they are forced on.
        effective_extractor_args = "" if is_soundcloud_source else extractor_args
        effective_use_cookies = (use_cookies and not is_soundcloud_source)
        transcode_to_mp3 = (not STEP1_FAST_NO_TRANSCODE) or is_soundcloud_source
        effective_fmt = "" if is_soundcloud_source else fmt
        if (not transcode_to_mp3) and (not effective_fmt):
            effective_fmt = STEP1_FAST_NO_TRANSCODE_FORMAT or "bestaudio"

        outtmpl_for_attempt = outtmpl_override or outtmpl
        effective_concurrent_fragments = _resolve_concurrent_fragments(
            source=source,
            deadline_monotonic=deadline_monotonic,
            hot_query=hot_query,
        )

        cmd = _yt_dlp_base_common(
            outtmpl_for_attempt,
            fmt=effective_fmt,
            extractor_args=effective_extractor_args,
            search_source=search_source,
            use_cookies=effective_use_cookies,
            transcode_to_mp3=transcode_to_mp3,
            concurrent_fragments=effective_concurrent_fragments,
        ) + [source]
        used_proxy = _proxy_from_cmd(cmd)

        fmt_for_log = effective_fmt or "auto"
        client_for_log = effective_extractor_args or "auto"
        cookies_for_log = "on" if effective_use_cookies else "off"
        mode_for_log = "transcode_mp3" if transcode_to_mp3 else "native_no_transcode"
        log(
            "MP3",
            "Attempt [%s]: query_or_id=%s source=%s mode=%s format=%s client=%s cookies=%s timeout=%.1fs" % (
                label,
                source_label,
                source_for_log,
                mode_for_log,
                fmt_for_log,
                client_for_log,
                cookies_for_log,
                float(attempt_timeout_sec),
            ),
            CYAN,
        )
        try:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            def _stop_process() -> None:
                try:
                    p.terminate()
                    p.wait(timeout=2)
                except Exception:
                    try:
                        p.kill()
                    except Exception:
                        pass

            def _attempt_output_bytes() -> int:
                base_raw = outtmpl_for_attempt.replace("%(ext)s", "").rstrip(".")
                base = Path(base_raw) if base_raw else out_mp3.with_suffix("")
                total_bytes = 0
                for cand in base.parent.glob(base.name + ".*"):
                    if not cand.is_file():
                        continue
                    try:
                        st = cand.stat()
                    except Exception:
                        continue
                    prev_mtime = pre_existing.get(str(cand.resolve()))
                    if prev_mtime is not None and int(st.st_mtime_ns) <= int(prev_mtime):
                        continue
                    total_bytes += max(0, int(st.st_size))
                return total_bytes

            no_progress_timeout_sec = 0.0
            if YTDLP_NO_PROGRESS_TIMEOUT_SEC > 0.0:
                no_progress_timeout_sec = min(
                    float(attempt_timeout_sec),
                    float(YTDLP_NO_PROGRESS_TIMEOUT_SEC),
                )

            out_lines = deque(maxlen=YTDLP_CAPTURE_LINES)
            err_lines = deque(maxlen=YTDLP_CAPTURE_LINES)
            sel = selectors.DefaultSelector()
            if p.stdout is not None:
                sel.register(p.stdout, selectors.EVENT_READ)
            if p.stderr is not None:
                sel.register(p.stderr, selectors.EVENT_READ)

            t0 = time.monotonic()
            next_heartbeat = t0 + YTDLP_PROGRESS_HEARTBEAT_SEC
            last_observed_progress = t0
            last_observed_bytes = _attempt_output_bytes()

            while True:
                if stop_event is not None and stop_event.is_set():
                    _stop_process()
                    return 130, "yt-dlp attempt cancelled after sibling success (%s)" % label

                now = time.monotonic()
                elapsed = now - t0
                if elapsed > attempt_timeout_sec:
                    _stop_process()
                    _mark_proxy_failure(used_proxy, reason="yt_download_timeout")
                    return 124, "yt-dlp download timed out for %s after %.1fs (%s)" % (
                        source_label,
                        float(attempt_timeout_sec),
                        label,
                    )

                events = sel.select(timeout=1.0)
                saw_stream_activity = False
                for key, _ in events:
                    stream = key.fileobj
                    line = stream.readline()
                    if line == "":
                        try:
                            sel.unregister(stream)
                        except Exception:
                            pass
                        continue
                    saw_stream_activity = True
                    if stream is p.stdout:
                        out_lines.append(line)
                    else:
                        err_lines.append(line)

                now_after_io = time.monotonic()
                if saw_stream_activity:
                    last_observed_progress = now_after_io

                observed_bytes = _attempt_output_bytes()
                if observed_bytes > last_observed_bytes:
                    last_observed_bytes = observed_bytes
                    last_observed_progress = now_after_io

                if (
                    no_progress_timeout_sec > 0.0
                    and (now_after_io - last_observed_progress) > no_progress_timeout_sec
                ):
                    _stop_process()
                    _mark_proxy_failure(used_proxy, reason="yt_download_no_progress")
                    _rotate_proxy("yt_download_no_progress")
                    return 124, (
                        "yt-dlp download made no progress for %s after %.1fs "
                        "(threshold %.1fs, %s)"
                    ) % (
                        source_label,
                        float(now_after_io - last_observed_progress),
                        float(no_progress_timeout_sec),
                        label,
                    )

                if (not events) and (now_after_io >= next_heartbeat):
                    log(
                        "STEP1B",
                        "download_audio in progress (%s, %ds): %s" % (
                            label,
                            int(now_after_io - t0),
                            source_label,
                        ),
                        CYAN,
                    )
                    next_heartbeat = now_after_io + YTDLP_PROGRESS_HEARTBEAT_SEC

                if p.poll() is not None and not sel.get_map():
                    break

            rc = p.returncode if p.returncode is not None else 1
            if rc == 0:
                _mark_proxy_success(used_proxy)
                return 0, ""
            msg = _collect_ytdlp_diagnostics("".join(err_lines), "".join(out_lines))
            if msg:
                for ln in msg.splitlines()[-40:]:
                    log("MP3", "diag: %s" % ln, YELLOW)

            # Some videos expose only progressive muxed formats when SABR blocks
            # DASH audio URLs. Retry once in-process with a progressive-first
            # selector so we can still extract a usable source file.
            low_msg = (msg or "").lower()
            sabr_like_error = (
                ("unable to download video data" in low_msg and "403" in low_msg)
                or ("sabr" in low_msg)
                or ("missing a url" in low_msg)
            )
            if (
                rc != 0
                and sabr_like_error
                and str(YTDLP_PROGRESSIVE_FALLBACK_FORMAT) not in (effective_fmt or "")
            ):
                elapsed_total = max(0.0, time.monotonic() - t0)
                retry_timeout_sec = max(1.0, float(attempt_timeout_sec) - elapsed_total)
                retry_fmt = str(YTDLP_PROGRESSIVE_FALLBACK_FORMAT)
                retry_cmd = _yt_dlp_base_common(
                    outtmpl_for_attempt,
                    fmt=retry_fmt,
                    extractor_args=effective_extractor_args,
                    search_source=search_source,
                    use_cookies=effective_use_cookies,
                    transcode_to_mp3=transcode_to_mp3,
                    concurrent_fragments=effective_concurrent_fragments,
                ) + [source]
                retry_proxy = _proxy_from_cmd(retry_cmd)
                retry_label = "%s-progressive-fallback" % label
                log(
                    "MP3",
                    "Attempt [%s]: query_or_id=%s source=%s mode=%s format=%s client=%s cookies=%s timeout=%.1fs" % (
                        retry_label,
                        source_label,
                        source_for_log,
                        mode_for_log,
                        retry_fmt,
                        client_for_log,
                        cookies_for_log,
                        float(retry_timeout_sec),
                    ),
                    CYAN,
                )
                try:
                    p2 = subprocess.run(
                        retry_cmd,
                        capture_output=True,
                        text=True,
                        timeout=retry_timeout_sec,
                    )
                    if p2.returncode == 0:
                        _mark_proxy_success(retry_proxy)
                        return 0, ""
                    retry_msg = _collect_ytdlp_diagnostics(
                        p2.stderr or "",
                        p2.stdout or "",
                    )
                    if retry_msg:
                        for ln in retry_msg.splitlines()[-20:]:
                            log("MP3", "diag: %s" % ln, YELLOW)
                    if retry_msg:
                        msg = ("%s\n%s" % (msg or "", retry_msg)).strip()
                    _mark_proxy_failure(retry_proxy, reason="yt_download_progressive_fallback_failed")
                except subprocess.TimeoutExpired:
                    _mark_proxy_failure(retry_proxy, reason="yt_download_progressive_fallback_timeout")
                    msg = ("%s\n%s" % (msg or "", "progressive fallback timed out")).strip()

            _mark_proxy_failure(used_proxy, reason="yt_download_failed")
            if _should_rotate_proxy_on_error(msg):
                _rotate_proxy("yt_download")
            return rc, msg or "yt-dlp failed without diagnostics"
        except FileNotFoundError:
            return 127, _ytdlp_missing_message()

    cookies_configured = bool((YTDLP_COOKIES_PATH or "").strip())

    def _client_args(client: str) -> str:
        c = (client or "").strip()
        if not c:
            return ""
        return "youtube:player_client=%s" % c

    # Build an ordered attempt plan, dynamically extended based on failures.
    attempted: set[tuple[str, str, bool]] = set()
    attempt_count = 0
    budget_started = time.monotonic()
    race_produced_override: Optional[Path] = None

    class _AttemptBudgetExceeded(RuntimeError):
        pass

    def _resolve_output_for_base(base_path: Path) -> Optional[Path]:
        candidates: List[Path] = []
        for p in base_path.parent.glob(base_path.name + ".*"):
            if p.is_file():
                try:
                    if p.stat().st_size > 0:
                        candidates.append(p)
                except Exception:
                    continue
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x.stat().st_mtime_ns, x.stat().st_size), reverse=True)
        return candidates[0]

    def _try_parallel_race(primary_use_cookies: bool) -> tuple[int, str, Optional[Path]]:
        nonlocal attempt_count
        if not MP3_ENABLE_PARALLEL_STRATEGY_RACE:
            return 1, "parallel strategy race disabled", None
        if source.startswith("scsearch"):
            return 1, "parallel strategy race skipped for soundcloud source", None

        now_mono = time.monotonic()
        elapsed = now_mono - budget_started
        if attempt_count >= MP3_MAX_SOURCE_ATTEMPTS:
            raise _AttemptBudgetExceeded(
                "yt-dlp attempt budget exhausted for %s (%d attempts, %.1fs elapsed)"
                % (source_label, attempt_count, elapsed)
            )
        if elapsed >= MP3_MAX_SOURCE_SECONDS:
            raise _AttemptBudgetExceeded(
                "yt-dlp time budget exhausted for %s (%.1fs elapsed)"
                % (source_label, elapsed)
            )

        # Keep the two-way race resilient: with the current cap we only run two
        # attempts here, so prefer a fully bare extractor as the second branch.
        # Some tracks expose formats only when no forced player_client is used.
        # Prefer android-client race early: many current SABR/403 failures on
        # default web/auto are avoided by android player client.
        race_candidates: List[tuple[str, str, str, bool]] = [
            ("race-client-android", YTDLP_FORMAT, _client_args("android"), False),
            ("race-primary", YTDLP_FORMAT, YTDLP_EXTRACTOR_ARGS, primary_use_cookies),
            ("race-autoformat-bare", "", "", False),
            ("race-autoformat-fallback-client", "", YTDLP_FALLBACK_EXTRACTOR_ARGS, False),
        ]
        race_plan: List[tuple[str, str, str, bool, tuple[str, str, bool]]] = []
        seen_plan: set[tuple[str, str, bool]] = set()
        for label, fmt, extractor_args, use_cookies in race_candidates:
            dedupe_key = (fmt or "", extractor_args or "", bool(use_cookies))
            if dedupe_key in seen_plan:
                continue
            if dedupe_key in attempted:
                continue
            seen_plan.add(dedupe_key)
            race_plan.append((label, fmt, extractor_args, bool(use_cookies), dedupe_key))
        if len(race_plan) < 2:
            return 1, "parallel strategy race skipped (not enough distinct candidates)", None

        remaining_slots = MP3_MAX_SOURCE_ATTEMPTS - attempt_count
        if remaining_slots < 2:
            return 1, "parallel strategy race skipped (attempt budget too tight)", None
        race_plan = race_plan[: min(2, remaining_slots)]

        attempt_timeout_sec = min(float(YTDLP_CMD_TIMEOUT), max(1.0, MP3_MAX_SOURCE_SECONDS - elapsed))
        if deadline_monotonic is not None:
            remaining_total_sec = deadline_monotonic - now_mono
            if remaining_total_sec <= 0:
                raise _AttemptBudgetExceeded(
                    "yt-dlp total budget exhausted for %s (no time remaining)"
                    % source_label
                )
            attempt_timeout_sec = min(attempt_timeout_sec, max(1.0, remaining_total_sec))

        for _, _, _, _, dedupe_key in race_plan:
            attempted.add(dedupe_key)
        attempt_count += len(race_plan)

        stop_event = threading.Event()
        race_bases: Dict[str, Path] = {}
        futures: Dict[concurrent.futures.Future, str] = {}
        winner_label: Optional[str] = None
        errors: List[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(race_plan)) as ex:
            for idx, (label, fmt, extractor_args, use_cookies, _dedupe_key) in enumerate(race_plan, start=1):
                race_base = out_mp3.parent / ("%s.race%d" % (stem, idx))
                race_bases[label] = race_base
                race_outtmpl = str(race_base) + ".%(ext)s"
                fut = ex.submit(
                    _run_attempt,
                    label,
                    fmt,
                    extractor_args,
                    use_cookies=use_cookies,
                    attempt_timeout_sec=attempt_timeout_sec,
                    outtmpl_override=race_outtmpl,
                    stop_event=stop_event,
                )
                futures[fut] = label

            for fut in concurrent.futures.as_completed(futures):
                label = futures[fut]
                try:
                    rc_local, msg_local = fut.result()
                except Exception as e:
                    rc_local, msg_local = (1, str(e))
                if rc_local == 0 and winner_label is None:
                    winner_label = label
                    stop_event.set()
                elif rc_local not in {0, 130} and msg_local:
                    errors.append("%s: %s" % (label, msg_local))

        if winner_label is not None:
            winner_base = race_bases[winner_label]
            winner_path = _resolve_output_for_base(winner_base)
            if winner_path is not None:
                for label, base in race_bases.items():
                    if label == winner_label:
                        continue
                    for p in base.parent.glob(base.name + ".*"):
                        try:
                            p.unlink()
                        except Exception:
                            pass
                log("MP3", "Parallel strategy race winner: %s" % winner_label, CYAN)
                return 0, "", winner_path
            errors.append("%s: race winner produced no output file" % winner_label)

        for base in race_bases.values():
            for p in base.parent.glob(base.name + ".*"):
                try:
                    p.unlink()
                except Exception:
                    pass

        if errors:
            return 1, "\n".join(errors[-6:]), None
        return 1, "parallel strategy race yielded no successful attempt", None

    def _try(label: str, fmt: str, extractor_args: str, use_cookies: bool) -> tuple[int, str]:
        nonlocal attempt_count
        key = (fmt or "", extractor_args or "", bool(use_cookies))
        if key in attempted:
            return 1, "skipped duplicate attempt"
        now_mono = time.monotonic()
        elapsed = now_mono - budget_started
        if attempt_count >= MP3_MAX_SOURCE_ATTEMPTS:
            raise _AttemptBudgetExceeded(
                "yt-dlp attempt budget exhausted for %s (%d attempts, %.1fs elapsed)"
                % (source_label, attempt_count, elapsed)
            )
        if elapsed >= MP3_MAX_SOURCE_SECONDS:
            raise _AttemptBudgetExceeded(
                "yt-dlp time budget exhausted for %s (%.1fs elapsed)"
                % (source_label, elapsed)
            )
        attempt_timeout_sec = min(float(YTDLP_CMD_TIMEOUT), max(1.0, MP3_MAX_SOURCE_SECONDS - elapsed))
        if deadline_monotonic is not None:
            remaining_total_sec = deadline_monotonic - now_mono
            if remaining_total_sec <= 0:
                raise _AttemptBudgetExceeded(
                    "yt-dlp total budget exhausted for %s (no time remaining)"
                    % source_label
                )
            attempt_timeout_sec = min(attempt_timeout_sec, max(1.0, remaining_total_sec))

        attempted.add(key)
        attempt_count += 1
        return _run_attempt(
            label,
            fmt,
            extractor_args,
            use_cookies=use_cookies,
            attempt_timeout_sec=attempt_timeout_sec,
        )

    try:
        # 1) Primary attempt: fastest
        primary_use_cookies = cookies_configured and MP3_PRIMARY_USE_COOKIES
        rc, msg, race_produced_override = _try_parallel_race(primary_use_cookies)
        primary_key = (YTDLP_FORMAT or "", YTDLP_EXTRACTOR_ARGS or "", bool(primary_use_cookies))
        if rc != 0 and primary_key not in attempted:
            rc, msg = _try("primary", YTDLP_FORMAT, YTDLP_EXTRACTOR_ARGS, primary_use_cookies)

        # 2) Cookies only when upstream explicitly demands auth
        if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
            rc, msg = _try("cookies-auth", YTDLP_FORMAT, YTDLP_EXTRACTOR_ARGS, True)

        # 3) Client failover when signature/403/429/captcha-ish patterns show up
        if rc != 0 and _is_sig_or_forbidden_or_transient(msg):
            for client in YTDLP_FAILOVER_CLIENTS:
                args = _client_args(client)
                if args and args == (YTDLP_EXTRACTOR_ARGS or ""):
                    continue
                rc, msg = _try("client-%s" % client, YTDLP_FORMAT, args, False)
                if rc == 0:
                    break
                if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                    rc, msg = _try("client-%s-cookies" % client, YTDLP_FORMAT, args, True)
                    if rc == 0:
                        break

                # tiny backoff if we hit obvious rate-limit/captcha
                if "429" in (msg or "").lower() or "too many requests" in (msg or "").lower():
                    time.sleep(0.75)

        # 4) Format relaxation when format selection is the failure mode
        if rc != 0 and _is_format_selection_error(msg):
            # first: force a progressive-capable selector with auto client.
            rc, msg = _try("progressive-bare", YTDLP_PROGRESSIVE_FALLBACK_FORMAT, "", False)
            if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                rc, msg = _try("progressive-bare-cookies", YTDLP_PROGRESSIVE_FALLBACK_FORMAT, "", True)

            # next: progressive selector + fallback client.
            if rc != 0:
                rc, msg = _try(
                    "progressive-fallback-client",
                    YTDLP_PROGRESSIVE_FALLBACK_FORMAT,
                    YTDLP_FALLBACK_EXTRACTOR_ARGS,
                    False,
                )
                if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                    rc, msg = _try(
                        "progressive-fallback-client-cookies",
                        YTDLP_PROGRESSIVE_FALLBACK_FORMAT,
                        YTDLP_FALLBACK_EXTRACTOR_ARGS,
                        True,
                    )

            # first: auto format + fallback client
            if rc != 0:
                rc, msg = _try("autoformat-fallback-client", "", YTDLP_FALLBACK_EXTRACTOR_ARGS, False)
            if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                rc, msg = _try("autoformat-fallback-client-cookies", "", YTDLP_FALLBACK_EXTRACTOR_ARGS, True)

            # then: auto format across clients
            if rc != 0:
                for client in YTDLP_FAILOVER_CLIENTS:
                    args = _client_args(client)
                    rc, msg = _try("autoformat-%s" % client, "", args, False)
                    if rc == 0:
                        break
                    if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                        rc, msg = _try("autoformat-%s-cookies" % client, "", args, True)
                        if rc == 0:
                            break

            # finally: fully bare (auto format + auto client)
            if rc != 0:
                rc, msg = _try("autoformat-bare", "", "", False)
                if rc != 0 and _is_signin_or_cookie_error(msg) and cookies_configured:
                    rc, msg = _try("autoformat-bare-cookies", "", "", True)

        # 5) Last resort: even if not a format error, try relaxed format with fallback client
        if rc != 0:
            rc2, msg2 = _try("last-resort-autoformat", "", YTDLP_FALLBACK_EXTRACTOR_ARGS, False)
            if rc2 == 0:
                rc, msg = rc2, msg2
            elif _is_signin_or_cookie_error(msg2) and cookies_configured:
                rc2, msg2 = _try("last-resort-autoformat-cookies", "", YTDLP_FALLBACK_EXTRACTOR_ARGS, True)
                if rc2 == 0:
                    rc, msg = rc2, msg2
    except _AttemptBudgetExceeded as budget_err:
        log("MP3", str(budget_err), YELLOW)
        rc, msg = 124, str(budget_err)

    if rc != 0:
        raise RuntimeError("yt-dlp download failed for %s\n%s" % (source_label, msg))

    return _finalize_downloaded_audio_output(
        out_mp3,
        source_label=source_label,
        pre_existing=pre_existing,
        preferred_path=race_produced_override,
    )


def _try_invidious_download(video_id: str, out_mp3: Path) -> Optional[Path]:
    """Try downloading via Invidious proxy to bypass source blocking."""
    if not INVIDIOUS_AVAILABLE:
        return None

    try:
        log("MP3", f"Trying Invidious proxy for {video_id}", CYAN)
        client = InvidiousClient()
        audio_url, metadata = client.get_audio_url(video_id)

        # Download the audio file
        response = requests.get(audio_url, stream=True, timeout=90)
        response.raise_for_status()

        # Save to temporary file first
        temp_file = out_mp3.with_suffix(".tmp.m4a")
        with open(temp_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Convert to MP3 using ffmpeg
        subprocess.run(
            ["ffmpeg", "-i", str(temp_file), "-vn", "-ab", "128k", "-ar", "44100", "-y", str(out_mp3)],
            check=True,
            capture_output=True
        )

        # Clean up temp file
        temp_file.unlink(missing_ok=True)

        log("MP3", f"✓ Invidious download succeeded: {metadata.get('title')}", GREEN)
        return out_mp3

    except Exception as e:
        log("MP3", f"Invidious download failed: {e}", YELLOW)
        return None


def yt_download_mp3(
    video_id: str,
    out_mp3: Path,
    *,
    deadline_monotonic: Optional[float] = None,
    bypass_source_fail_cooldown: bool = False,
) -> Path:
    singleflight_key, singleflight_lock = _yt_audio_singleflight_acquire(video_id)

    def _download() -> Path:
        cached = _yt_audio_disk_cache_restore(video_id, out_mp3)
        if cached is not None:
            return cached
        cooldown_left = _source_fail_cooldown_remaining(video_id)
        if cooldown_left > 0.0:
            if bypass_source_fail_cooldown:
                log(
                    "MP3",
                    "Bypassing source cooldown for pinned id=%s (remaining=%ds)"
                    % (video_id, int(cooldown_left)),
                    YELLOW,
                )
            else:
                raise RuntimeError(
                    "source id %s cooling down for %ds after auth/bot-check failures"
                    % (video_id, int(cooldown_left))
                )

        # DISABLED: Invidious is disabled, client-side download only
        # if result := _try_invidious_download(video_id, out_mp3):
        #     return result

        # Use yt-dlp (server-side MP3 download should not happen with client-side flow)
        log("MP3", "SERVER-SIDE MP3 DOWNLOAD (should not happen with client upload!)", YELLOW)
        url = "https://www.youtube.com/watch?v=%s" % video_id
        try:
            produced = _yt_download_from_source(
                url,
                out_mp3,
                source_label=video_id,
                deadline_monotonic=deadline_monotonic,
            )
        except Exception as exc:
            if _is_bot_or_cookie_gate_error(str(exc)):
                _mark_source_fail_cooldown(video_id, reason=str(exc))
            raise
        _yt_audio_disk_cache_store(video_id, produced)
        return produced

    if singleflight_lock is None:
        return _download()

    waited_t0 = now_perf_ms()
    try:
        with singleflight_lock:
            waited_ms = max(0.0, now_perf_ms() - waited_t0)
            if waited_ms >= 10.0:
                log("MP3", "Waiting for in-flight source download id=%s wait_ms=%.1f" % (video_id, waited_ms), CYAN)
            return _download()
    finally:
        _yt_audio_singleflight_release(singleflight_key)


def yt_download_top_result_mp3(
    query: str,
    out_mp3: Path,
    *,
    deadline_monotonic: Optional[float] = None,
) -> Path:
    # Try Invidious search + download first
    if INVIDIOUS_AVAILABLE:
        try:
            log("MP3", f"Trying Invidious search for: {query}", CYAN)
            client = InvidiousClient()
            results = client.search(query, limit=1)
            if results:
                video_id = results[0].get("videoId")
                if video_id and (result := _try_invidious_download(video_id, out_mp3)):
                    return result
        except Exception as e:
            log("MP3", f"Invidious search failed: {e}", YELLOW)

    # Fall back to yt-dlp search
    log("MP3", "Falling back to yt-dlp search", CYAN)
    source = "ytsearch%d:%s" % (MP3_PRIMARY_SEARCH_SPAN, query)
    return _yt_download_from_source(
        source,
        out_mp3,
        source_label=query,
        deadline_monotonic=deadline_monotonic,
        hot_query=_is_hot_query(query),
    )


def _build_mp3_query_variants(query: str, *, retry_attempt: int = 3) -> List[str]:
    q0 = (query or "").strip()
    if not q0:
        return []
    direct = _direct_source_source_from_query(q0)
    if direct is not None:
        return [direct[1]]
    base = q0
    canonical_query = _canonicalize_hot_query(q0)

    # Reuse existing cleaner if present (defined in LRC section)
    try:
        cleaned = _clean_title(q0)  # type: ignore[name-defined]
    except Exception:
        cleaned = q0

    variants: List[str] = [canonical_query, base, cleaned]
    retry_tier = max(1, int(retry_attempt))

    if retry_tier <= 1:
        out_fast: List[str] = []
        seen_fast: set[str] = set()
        for v in variants:
            vv = " ".join((v or "").split()).strip()
            if not vv:
                continue
            k = _normalize_key(vv)
            if (not k) or (k in seen_fast):
                continue
            seen_fast.add(k)
            out_fast.append(vv)
            if len(out_fast) >= int(STEP1_FAIL_FAST_MAX_QUERY_VARIANTS):
                break
        return out_fast or [q0]

    if _normalize_key(canonical_query) in _HOT_QUERY_CANONICAL:
        # Hot-query latency path: keep variants minimal to avoid extra yt-search probes.
        dedup: List[str] = []
        seen_hot: set[str] = set()
        for v in variants:
            vv = " ".join((v or "").split()).strip()
            if not vv:
                continue
            k = _normalize_key(vv)
            if (not k) or (k in seen_hot):
                continue
            seen_hot.add(k)
            dedup.append(vv)
            if len(dedup) >= 2:
                break
        return dedup

    # Heuristic: if user provided "Artist - Title" also try flipped
    try:
        a, t = _maybe_split_artist_title(q0)  # type: ignore[name-defined]
        if a and t:
            variants.append('"%s" "%s"' % (a, t))
            variants.append("%s +%s" % (a, t))
            variants.append("%s %s" % (a, t))
            variants.append("%s %s" % (t, a))
            variants.append("%s - %s" % (a, t))
            variants.append("%s - %s" % (t, a))
    except Exception:
        pass

    # Suffix expansions (English + Spanish), but bounded
    for suf in MP3_QUERY_SUFFIXES:
        variants.append("%s %s" % (cleaned, suf))
        variants.append("%s %s" % (base, suf))

    # Trailing token typo recovery (English + Spanish), e.g. "... and theb".
    variants.extend(_build_trailing_token_recovery_variants(base))

    # stable de-dupe and bound
    seen = set()
    out: List[str] = []
    max_variants = int(MP3_MAX_QUERY_VARIANTS)
    if retry_tier == 2:
        max_variants = min(max_variants, max(2, int(STEP1_FAIL_FAST_MAX_QUERY_VARIANTS) + 1))

    for v in variants:
        vv = " ".join((v or "").split()).strip()
        if not vv:
            continue
        if vv in seen:
            continue
        seen.add(vv)
        out.append(vv)
        if len(out) >= max_variants:
            break
    return out


_MP3_HINTY_QUERY_RE = re.compile(
    r"\b(lyrics?|letra|karaoke|instrumental|official\s+audio|audio\s+oficial|topic|audio)\b",
    re.IGNORECASE,
)
_AUDIO_VARIANT_INTENT_QUERY_RE = re.compile(
    r"\b(lyrics?|lyric\s+video|letra(?:s)?|karaoke|instrumental|live|acoustic|remix|cover)\b",
    re.IGNORECASE,
)

_QUERY_TRAILING_TOKEN_LEXICON: Tuple[str, ...] = (
    "the",
    "then",
    "and",
    "with",
    "without",
    "of",
    "in",
    "on",
    "for",
    "to",
    "a",
    "an",
    "y",
    "de",
    "del",
    "la",
    "el",
    "los",
    "las",
    "con",
    "sin",
    "en",
    "por",
    "para",
    "que",
    "como",
    "cuando",
    "donde",
    "si",
    "lyrics",
    "letra",
    "karaoke",
    "instrumental",
    "official",
    "audio",
    "oficial",
    "video",
    "music",
    "musica",
    "song",
    "cancion",
)


def _fold_query_token(s: str) -> str:
    base = unicodedata.normalize("NFKD", (s or "").strip().lower())
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", base)


def _is_single_edit_away(a: str, b: str) -> bool:
    """
    True when edit distance is <=1, including one adjacent transposition.
    """
    if a == b:
        return True

    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False

    if la == lb:
        mismatches = [i for i, (ca, cb) in enumerate(zip(a, b)) if ca != cb]
        if len(mismatches) == 1:
            return True
        if len(mismatches) == 2:
            i, j = mismatches
            return (j == i + 1) and (a[i] == b[j]) and (a[j] == b[i])
        return False

    # insertion/deletion case
    short, long = (a, b) if la < lb else (b, a)
    i = 0
    j = 0
    skipped = False
    while i < len(short) and j < len(long):
        if short[i] == long[j]:
            i += 1
            j += 1
            continue
        if skipped:
            return False
        skipped = True
        j += 1
    return True


def _best_trailing_token_correction(token: str) -> Optional[str]:
    normalized = _fold_query_token(token)
    if not normalized or len(normalized) < 2:
        return None

    lexicon = [_fold_query_token(w) for w in _QUERY_TRAILING_TOKEN_LEXICON]
    if normalized in lexicon:
        return None

    candidates = [cand for cand in lexicon if _is_single_edit_away(normalized, cand)]
    if not candidates:
        return None

    def _rank(cand: str) -> Tuple[int, int, str]:
        same_first = 1 if cand[:1] == normalized[:1] else 0
        length_delta = abs(len(cand) - len(normalized))
        return (same_first, -length_delta, cand)

    best = sorted(candidates, key=_rank, reverse=True)[0]
    return best


def _build_trailing_token_recovery_variants(query: str) -> List[str]:
    q = " ".join((query or "").split()).strip()
    if not q:
        return []
    if _direct_source_source_from_query(q) is not None:
        return []

    parts = q.split()
    if len(parts) < 2:
        return []

    out: List[str] = []

    # Recovery for accidental trailing partial token, e.g., "... and theb".
    if len(parts) >= 3:
        out.append(" ".join(parts[:-1]))
        if len(parts[-1]) >= 3:
            out.append(" ".join([*parts[:-1], parts[-1][:-1]]))

    corrected = _best_trailing_token_correction(parts[-1])
    if corrected:
        out.append(" ".join([*parts[:-1], corrected]))

    return out


def _build_mp3_search_queries(query_variant: str, *, retry_attempt: int = 3) -> List[str]:
    """
    Build bounded yt search probes for a query variant.

    Optimization: avoid the extra quoted-lyrics probe when the variant already
    carries lyric/audio intent terms, which saves a full yt-dlp search call.
    Exact query always goes first to match manual source behavior.
    """
    qv = " ".join((query_variant or "").split()).strip()
    if not qv:
        return []

    retry_tier = max(1, int(retry_attempt))
    search_queries: List[str] = [qv]
    if retry_tier <= 1:
        return search_queries[: int(STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS)]
    if MP3_PREFER_LYRICS_VERSION and not _MP3_HINTY_QUERY_RE.search(qv):
        search_queries.append('"%s" lyrics' % qv)

    max_search_queries = int(MP3_MAX_SEARCH_QUERY_VARIANTS)
    if retry_tier == 2:
        max_search_queries = min(max_search_queries, max(1, int(STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS) + 1))
    return search_queries[: max_search_queries]


def _try_download_source(
    query_variant: str,
    out_mp3: Path,
    *,
    retry_attempt: int = STEP1_DEFAULT_RETRY_ATTEMPT,
    search_n: int,
    tried_ids: set[str],
    search_cache: Optional[Dict[str, List[str]]] = None,
    id_attempt_limit: Optional[int] = None,
    search_query_limit: Optional[int] = None,
    deadline_monotonic: Optional[float] = None,
    expected_duration_sec: Optional[float] = None,
    prefer_top_hit: bool = False,
) -> Optional[Tuple[str, Path]]:
    # Combine both proven paths:
    # 1) yt-dlp dump-json search (codespaces-proven) + per-ID download
    # 2) optional direct ytsearch download fallback (off by default)
    direct = _direct_source_source_from_query(query_variant)
    if direct is not None:
        vid, source = direct
        label = vid or query_variant
        prefer_broad_fast = _should_prefer_broad_direct_fast_format(
            query=query_variant,
            source_label=label,
        )
        if vid:
            cached_audio = _yt_audio_disk_cache_restore(vid, out_mp3)
            if cached_audio is not None:
                return (vid, cached_audio)
        if MP3_ENABLE_DIRECT_SOURCE_FASTPATH:
            try:
                produced_fast = _yt_download_direct_fast(
                    source,
                    out_mp3,
                    source_label=label,
                    deadline_monotonic=deadline_monotonic,
                    hot_query=_is_hot_query(query_variant),
                    prefer_broad_format=prefer_broad_fast,
                )
                if vid:
                    _yt_audio_disk_cache_store(vid, produced_fast)
                return (vid, produced_fast)
            except Exception as e:
                log("MP3", "direct fast-path failed (%s); falling back to resilient URL flow" % e, YELLOW)
        produced = _yt_download_from_source(
            source,
            out_mp3,
            source_label=label,
            deadline_monotonic=deadline_monotonic,
            hot_query=_is_hot_query(query_variant),
        )
        if vid:
            _yt_audio_disk_cache_store(vid, produced)
        return (vid, produced)

    retry_tier = max(1, int(retry_attempt))
    duration_hint = None if prefer_top_hit else _float_or_none(expected_duration_sec)
    duration_match_mode = bool(
        MP3_EXHAUSTIVE_DURATION_MATCH
        and (duration_hint is not None)
        and (duration_hint > 0.0)
    )
    duration_tolerance_ms = int(max(0, int(MP3_DURATION_MATCH_TOLERANCE_MS)))
    search_queries = _build_mp3_search_queries(query_variant, retry_attempt=retry_tier)
    last_error: Optional[str] = None
    if search_query_limit is not None:
        search_queries = search_queries[: max(1, int(search_query_limit))]

    if not search_queries:
        search_queries = [query_variant]

    max_id_attempts = max(1, int(id_attempt_limit or MP3_MAX_ID_ATTEMPTS))
    if duration_match_mode:
        max_id_attempts = max(max_id_attempts, int(MP3_DURATION_MATCH_MAX_ID_ATTEMPTS))

    if MP3_ENABLE_ID_PREFETCH:
        # First: ID probe path using yt_search_ids (dump-json + flat-playlist).
        def _search_ids_for_query(sq: str) -> List[str]:
            search_timeout = float(YTDLP_SEARCH_TIMEOUT)
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("yt id search timed out before start (%s)" % sq)
                search_timeout = min(search_timeout, max(1.0, remaining))
            kwargs: Dict[str, Any] = {"timeout_sec": search_timeout}
            if duration_hint is not None and duration_hint > 0.0:
                kwargs["target_duration_sec"] = float(duration_hint)
            return yt_search_ids(sq, max(1, int(search_n)), **kwargs)

        prefetched: Dict[str, Tuple[List[str], Optional[Exception]]] = {}
        if MP3_PARALLEL_SEARCH_QUERIES > 1:
            prefetch_queries: List[str] = []
            for sq in search_queries:
                if search_cache is not None and sq in search_cache:
                    continue
                prefetch_queries.append(sq)
                if len(prefetch_queries) >= MP3_PARALLEL_SEARCH_QUERIES:
                    break
            if len(prefetch_queries) > 1:
                log("MP3", "Prefetching yt-id searches in parallel (%d variants)" % len(prefetch_queries), CYAN)
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(prefetch_queries)) as ex:
                    fut_map = {ex.submit(_search_ids_for_query, sq): sq for sq in prefetch_queries}
                    for fut in concurrent.futures.as_completed(fut_map):
                        sq = fut_map[fut]
                        try:
                            ids = list(fut.result())
                            prefetched[sq] = (ids, None)
                        except Exception as e:
                            prefetched[sq] = ([], e)

        attempted = 0
        for sq in search_queries:
            ids: List[str]
            search_err: Optional[Exception] = None
            if search_cache is not None and sq in search_cache:
                ids = list(search_cache[sq])
            elif sq in prefetched:
                ids, search_err = prefetched[sq]
                if search_err is None and search_cache is not None:
                    # Cache successful search responses (including empty lists)
                    # to avoid repeating identical yt-dlp search subprocesses.
                    search_cache[sq] = list(ids)
            else:
                try:
                    ids = _search_ids_for_query(sq)
                    if search_cache is not None:
                        # Cache successful search responses (including empty lists)
                        # to avoid repeating identical yt-dlp search subprocesses.
                        search_cache[sq] = list(ids)
                except Exception as e:
                    ids = []
                    search_err = e

            if search_err is not None:
                msg = "yt id search failed (%s): %s" % (sq, search_err)
                last_error = msg
                log("MP3", msg, YELLOW)
                if _should_fail_fast_on_bot_gate(msg):
                    raise RuntimeError(msg)
                if MP3_STOP_AFTER_SEARCH_TIMEOUT and "timed out" in str(search_err).lower():
                    log("MP3", "search timeout hit; skipping remaining query variants for %s" % query_variant, YELLOW)
                    break
                continue

            if not ids:
                last_error = "yt search returned no ids for %r" % sq
            for vid in ids:
                if vid in tried_ids:
                    continue
                tried_ids.add(vid)
                attempted += 1
                try:
                    produced = yt_download_mp3(vid, out_mp3, deadline_monotonic=deadline_monotonic)
                    if duration_match_mode:
                        log(
                            "MP3",
                            (
                                "duration-ranked metadata mode selected id=%s "
                                "(target=%.3fs, tol=%dms)"
                            )
                            % (
                                vid,
                                float(duration_hint or 0.0),
                                int(duration_tolerance_ms),
                            ),
                            GREEN,
                        )
                    return (vid, produced)
                except Exception as e:
                    msg = "yt id failed (%s): %s" % (vid, e)
                    last_error = msg
                    log("MP3", msg, YELLOW)
                    # Treat auth/cookie gates as soft failures so we can keep
                    # scanning alternate IDs/variants for downloadable versions.
                    if _is_bot_or_cookie_gate_error(msg):
                        if _should_fail_fast_on_bot_gate(msg):
                            raise RuntimeError(msg)
                        attempted = max(0, attempted - 1)
                        continue
                if attempted >= max_id_attempts:
                    break
            if attempted >= max_id_attempts:
                break
    else:
        log("MP3", "ID prefetch disabled; skipping source ID flow for %s" % query_variant, YELLOW)

    fast_path_error: Optional[str] = None
    if (
        MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK
        and retry_tier >= 2
        and (not (duration_match_mode and MP3_DURATION_MATCH_DISABLE_DIRECT_YTSEARCH_FALLBACK))
    ):
        for sq in search_queries:
            try:
                source = "ytsearch%d:%s" % (MP3_PRIMARY_SEARCH_SPAN, sq)
                produced = _yt_download_from_source(
                    source,
                    out_mp3,
                    source_label=sq,
                    deadline_monotonic=deadline_monotonic,
                    hot_query=_is_hot_query(query_variant),
                )
                return ("", produced)
            except Exception as e:
                # IMPORTANT: this direct-search fallback is optional. If it fails, do not let it
                # overwrite a more informative earlier search/id failure; that would prevent
                # later retry strategies (like top-result hint recovery) from triggering.
                msg = "search fast path failed (%s): %s" % (sq, e)
                if _should_fail_fast_on_bot_gate(msg):
                    raise RuntimeError(msg)
                fast_path_error = msg
                log("MP3", msg, YELLOW)

    # Prefer earlier failures (ID search/no-ids) so downstream recovery logic can detect them.
    if last_error:
        raise RuntimeError(last_error)
    if fast_path_error:
        raise RuntimeError(fast_path_error)
    return None


def _try_download_soundcloud(
    query_variant: str,
    out_mp3: Path,
    *,
    deadline_monotonic: Optional[float] = None,
) -> Optional[Path]:
    if not MP3_ENABLE_SOUNDCLOUD:
        return None
    source = "scsearch%d:%s" % (MP3_SOUNDCLOUD_SEARCH_N, query_variant)
    try:
        return _yt_download_from_source(
            source,
            out_mp3,
            source_label="soundcloud:%s" % query_variant,
            deadline_monotonic=deadline_monotonic,
            hot_query=_is_hot_query(query_variant),
        )
    except Exception as e:
        log("MP3", "soundcloud failed (%s): %s" % (query_variant, e), YELLOW)
        return None


def download_first_working_mp3(
    query: str,
    out_mp3: Path,
    *,
    search_n: int,
    retry_attempt: int = 3,
    expected_duration_sec: Optional[float] = None,
    prefer_top_hit: bool = False,
) -> Tuple[str, Path]:
    """
    High-resilience MP3 downloader optimized for server-side Linux.

    Goals:
    - Near-zero chance of "song not found" by trying:
        1) ytsearch fast path (small window)
        2) ID prefetch + per-ID download attempts (bounded)
        3) Optional SoundCloud fallback (scsearch) as last resort
    - English + Spanish query variants
    - Additive behavior: keeps existing yt-dlp knobs and caching behavior
    """
    initial_audio_sigs: Dict[str, Tuple[int, int]] = {}
    stem = out_mp3.stem
    for p in out_mp3.parent.glob(stem + ".*"):
        if not p.is_file():
            continue
        try:
            initial_audio_sigs[str(p.resolve())] = (int(p.stat().st_size), int(p.stat().st_mtime_ns))
        except Exception:
            continue

    def _has_fresh_audio(path: Any) -> bool:
        if not isinstance(path, Path):
            try:
                return bool(path) and out_mp3.exists() and out_mp3.stat().st_size > 0
            except Exception:
                return False
        if (not path.exists()) or path.stat().st_size <= 0:
            return False
        key = str(path.resolve())
        sig = (int(path.stat().st_size), int(path.stat().st_mtime_ns))
        old = initial_audio_sigs.get(key)
        return old is None or sig != old

    retry_tier = max(1, int(retry_attempt))
    top_hit_mode = bool((prefer_top_hit and MP3_TOP_HIT_MODE) or MP3_SIMPLE_FIRST_RESULT_MODE)
    resolve_duration_hint = _float_or_none(expected_duration_sec)
    duration_hint = None if top_hit_mode else resolve_duration_hint
    duration_match_mode = bool(
        MP3_EXHAUSTIVE_DURATION_MATCH
        and (duration_hint is not None)
        and (duration_hint > 0.0)
    )
    if top_hit_mode:
        top_search_n = max(int(MP3_TOP_HIT_SEARCH_N), int(MP3_NON_LIVE_MIN_SEARCH_N))
        search_n = max(1, min(int(search_n), int(top_search_n)))
        log(
            "MP3",
            "top-hit mode active: relevance+non-live ranking with minimal search breadth (search_n=%d)"
            % int(search_n),
            CYAN,
        )
    if duration_match_mode:
        if retry_tier <= 1:
            search_n = max(int(search_n), int(MP3_DURATION_MATCH_SEARCH_N))
        else:
            search_n = max(int(search_n), int(MP3_DURATION_MATCH_SEARCH_N))
        log(
            "MP3",
            (
                "duration-match metadata mode target=%.3fs tolerance=%dms "
                "search_n=%d id_attempts<=%d"
            )
            % (
                float(duration_hint or 0.0),
                int(MP3_DURATION_MATCH_TOLERANCE_MS),
                int(search_n),
                int(MP3_DURATION_MATCH_MAX_ID_ATTEMPTS),
            ),
            CYAN,
        )
    budget_started = time.monotonic()
    deadline_monotonic = budget_started + MP3_TOTAL_TIMEOUT_SEC

    direct = _direct_source_source_from_query(query)
    if direct is not None:
        vid, source = direct
        label = vid or query
        prefer_broad_fast = _should_prefer_broad_direct_fast_format(
            query=query,
            source_label=label,
        )
        if vid:
            cached_audio = _yt_audio_disk_cache_restore(vid, out_mp3)
            if cached_audio is not None and (_has_fresh_audio(cached_audio) or _has_fresh_audio(out_mp3)):
                return (vid, cached_audio)
        if MP3_ENABLE_DIRECT_SOURCE_FASTPATH:
            try:
                produced_fast = _yt_download_direct_fast(
                    source,
                    out_mp3,
                    source_label=label,
                    deadline_monotonic=deadline_monotonic,
                    hot_query=_is_hot_query(query),
                    prefer_broad_format=prefer_broad_fast,
                )
                if vid:
                    _yt_audio_disk_cache_store(vid, produced_fast)
                if _has_fresh_audio(produced_fast) or _has_fresh_audio(out_mp3):
                    return (vid or "", produced_fast)
                raise RuntimeError("direct fast-path produced no fresh audio for %s" % label)
            except Exception as e:
                log("MP3", "direct fast-path failed (%s); falling back to resilient URL flow" % e, YELLOW)
        produced = _yt_download_from_source(
            source,
            out_mp3,
            source_label=label,
            deadline_monotonic=deadline_monotonic,
            hot_query=_is_hot_query(query),
        )
        if vid:
            _yt_audio_disk_cache_store(vid, produced)
        if _has_fresh_audio(produced) or _has_fresh_audio(out_mp3):
            return (vid or "", produced or out_mp3)
        raise RuntimeError("direct source produced no fresh audio for %s" % label)

    if bool(prefer_top_hit) and top_hit_mode and MP3_FAST_QUERY_RESOLVE_DIRECT_MODE:
        resolved = _resolve_fast_query_source(
            query,
            timeout_sec=min(float(MP3_FAST_QUERY_RESOLVE_TIMEOUT_SEC), float(YTDLP_SEARCH_TIMEOUT)),
            target_duration_sec=resolve_duration_hint,
        )
        if resolved is not None:
            resolved_vid, resolved_source, resolved_hint = resolved
            resolved_title = _clean_title(str(resolved_hint.get("title") or ""))
            resolved_artist = _clean_title(str(resolved_hint.get("artist") or ""))
            resolved_label = resolved_vid
            if resolved_artist and resolved_title:
                resolved_label = "%s - %s" % (resolved_artist, resolved_title)
            prefer_broad_fast = _should_prefer_broad_direct_fast_format(
                query=query,
                source_label=resolved_label,
            )
            log("MP3", "fast-resolved top result id=%s via metadata hint" % resolved_vid, CYAN)
            cached_audio = _yt_audio_disk_cache_restore(resolved_vid, out_mp3)
            if cached_audio is not None and (_has_fresh_audio(cached_audio) or _has_fresh_audio(out_mp3)):
                return (resolved_vid, cached_audio)
            if MP3_ENABLE_DIRECT_SOURCE_FASTPATH:
                try:
                    produced_fast = _yt_download_direct_fast(
                        resolved_source,
                        out_mp3,
                        source_label=resolved_label,
                        deadline_monotonic=deadline_monotonic,
                        hot_query=_is_hot_query(query),
                        prefer_broad_format=prefer_broad_fast,
                    )
                    _yt_audio_disk_cache_store(resolved_vid, produced_fast)
                    if _has_fresh_audio(produced_fast) or _has_fresh_audio(out_mp3):
                        return (resolved_vid, produced_fast)
                    raise RuntimeError("fast-resolved direct fast-path produced no fresh audio for %s" % resolved_label)
                except Exception as e:
                    log("MP3", "fast-resolved direct fast-path failed (%s); trying resilient direct source" % e, YELLOW)
            try:
                produced = _yt_download_from_source(
                    resolved_source,
                    out_mp3,
                    source_label=resolved_label,
                    deadline_monotonic=deadline_monotonic,
                    hot_query=_is_hot_query(query),
                )
                _yt_audio_disk_cache_store(resolved_vid, produced)
                if _has_fresh_audio(produced) or _has_fresh_audio(out_mp3):
                    return (resolved_vid, produced or out_mp3)
                raise RuntimeError("fast-resolved direct source produced no fresh audio for %s" % resolved_label)
            except Exception as e:
                log("MP3", "fast-resolved direct source failed; falling back to query search: %s" % e, YELLOW)

    if bool(prefer_top_hit) and MP3_ONE_CALL_SIMPLE_MODE:
        single_search_n = max(1, int(MP3_ONE_CALL_SEARCH_N))
        single_source = "ytsearch%d:%s" % (single_search_n, query)
        one_call_deadline = min(deadline_monotonic, time.monotonic() + float(MP3_ONE_CALL_MAX_SECONDS))
        log(
            "MP3",
            "one-call simple mode: single yt-dlp fetch via %s (max %.1fs)"
            % ("ytsearch%d" % int(single_search_n), float(MP3_ONE_CALL_MAX_SECONDS)),
            CYAN,
        )
        try:
            produced = _yt_download_from_source(
                single_source,
                out_mp3,
                source_label=query,
                deadline_monotonic=one_call_deadline,
                hot_query=_is_hot_query(query),
            )
            if _has_fresh_audio(produced) or _has_fresh_audio(out_mp3):
                return ("", produced or out_mp3)
            raise RuntimeError("one-call simple mode produced no fresh audio for %r" % query)
        except Exception as e:
            log("MP3", "one-call simple mode failed; falling back to resilient path: %s" % e, YELLOW)

    tried_ids: set[str] = set()
    search_cache: Dict[str, List[str]] = {}
    budget_timed_out = False
    hot_query_mode = bool(MP3_HOT_QUERY_SPEED_MODE and _is_hot_query(query))

    last_err: Optional[str] = None
    last_budget_profile: Optional[str] = None

    def _try_download_source_with_retry(*, qv: str, budget: Dict[str, int | str]) -> Optional[Tuple[str, Path]]:
        kwargs = dict(
            retry_attempt=retry_tier,
            search_n=int(budget["search_n"]),
            tried_ids=tried_ids,
            search_cache=search_cache,
            id_attempt_limit=int(budget["id_attempt_limit"]),
            search_query_limit=int(budget["search_query_limit"]),
            deadline_monotonic=deadline_monotonic,
            prefer_top_hit=bool(top_hit_mode),
        )
        duration_hint = _float_or_none(expected_duration_sec)
        if (not top_hit_mode) and duration_hint is not None and duration_hint > 0.0:
            kwargs["expected_duration_sec"] = float(duration_hint)
        try:
            return _try_download_source(qv, out_mp3, **kwargs)
        except TypeError as exc:
            err = str(exc)
            if ("prefer_top_hit" in err) and ("unexpected keyword argument" in err):
                kwargs.pop("prefer_top_hit", None)
                return _try_download_source(qv, out_mp3, **kwargs)
            if ("expected_duration_sec" in err) and ("unexpected keyword argument" in err):
                kwargs.pop("expected_duration_sec", None)
                return _try_download_source(qv, out_mp3, **kwargs)
            if ("retry_attempt" not in err) and ("unexpected keyword argument" not in err):
                raise
            # Backward compatibility for test mocks with old positional signature.
            return _try_download_source(  # type: ignore[misc]
                qv,
                out_mp3,
                int(budget["search_n"]),
                tried_ids,
                search_cache,
                int(budget["id_attempt_limit"]),
                int(budget["search_query_limit"]),
                deadline_monotonic,
            )

    # Highest-ROI fast path for repeated requests:
    # try known-good IDs before paying for fresh search calls.
    if MP3_ENABLE_CACHED_ID_FASTPATH:
        slug = out_mp3.stem
        for cached_vid in _cached_ids_for_slug(slug):
            if cached_vid in tried_ids:
                continue
            tried_ids.add(cached_vid)
            elapsed = time.monotonic() - budget_started
            if elapsed >= MP3_TOTAL_TIMEOUT_SEC:
                budget_timed_out = True
                break
            try:
                log("MP3", "Trying cached id first: %s" % cached_vid, CYAN)
                produced = yt_download_mp3(
                    cached_vid,
                    out_mp3,
                    deadline_monotonic=deadline_monotonic,
                    bypass_source_fail_cooldown=(cached_vid in MP3_PINNED_ID_SET),
                )
                if _has_fresh_audio(produced) or _has_fresh_audio(out_mp3):
                    return (cached_vid, produced)
                last_err = "cached id produced no fresh audio for %s" % cached_vid
            except Exception as e:
                last_err = str(e)
                log("MP3", "cached id failed (%s): %s" % (cached_vid, e), YELLOW)

    variants = _build_mp3_query_variants(query, retry_attempt=retry_tier)
    if not variants:
        raise RuntimeError("Empty query for MP3 download")

    for i, qv in enumerate(variants):
        elapsed = time.monotonic() - budget_started
        if elapsed >= MP3_TOTAL_TIMEOUT_SEC:
            budget_timed_out = True
            break
        remaining = max(0.0, MP3_TOTAL_TIMEOUT_SEC - elapsed)
        budget = _dynamic_search_budget(remaining_sec=remaining, base_search_n=search_n)
        budget = _apply_hot_query_speed_budget(query, budget)
        if retry_tier <= 1:
            if duration_match_mode:
                # Keep enough search breadth for duration-aware ranking to work;
                # otherwise fail-fast caps can collapse ytsearchN to effectively one result.
                budget["search_n"] = max(
                    int(budget["search_n"]),
                    int(MP3_DURATION_MATCH_SEARCH_N),
                )
                budget["id_attempt_limit"] = max(
                    int(budget["id_attempt_limit"]),
                    min(8, int(MP3_DURATION_MATCH_MAX_ID_ATTEMPTS)),
                )
            elif top_hit_mode:
                top_search_n = max(int(MP3_TOP_HIT_SEARCH_N), int(MP3_NON_LIVE_MIN_SEARCH_N))
                budget["search_n"] = int(top_search_n)
                budget["id_attempt_limit"] = min(
                    int(budget["id_attempt_limit"]),
                    int(top_search_n),
                )
                budget["search_query_limit"] = 1
                budget["variant_limit"] = 1
            else:
                budget["search_n"] = min(int(budget["search_n"]), int(STEP1_FAIL_FAST_SEARCH_N))
                budget["id_attempt_limit"] = min(
                    int(budget["id_attempt_limit"]),
                    int(STEP1_FAIL_FAST_MAX_ID_ATTEMPTS),
                )
                budget["search_query_limit"] = min(
                    int(budget["search_query_limit"]),
                    int(STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS),
                )
                budget["variant_limit"] = min(
                    int(budget["variant_limit"]),
                    int(STEP1_FAIL_FAST_MAX_QUERY_VARIANTS),
                )
        elif retry_tier == 2:
            budget["search_n"] = min(int(budget["search_n"]), max(2, int(STEP1_FAIL_FAST_SEARCH_N) + 1))
            budget["id_attempt_limit"] = min(
                int(budget["id_attempt_limit"]),
                max(2, int(STEP1_FAIL_FAST_MAX_ID_ATTEMPTS) + 1),
            )
            budget["search_query_limit"] = min(
                int(budget["search_query_limit"]),
                max(1, int(STEP1_FAIL_FAST_MAX_SEARCH_QUERY_VARIANTS) + 1),
            )
            budget["variant_limit"] = min(
                int(budget["variant_limit"]),
                max(2, int(STEP1_FAIL_FAST_MAX_QUERY_VARIANTS) + 1),
            )
        profile = str(budget.get("profile") or "")
        if profile != last_budget_profile:
            last_budget_profile = profile
            log(
                "MP3",
                "dynamic budget profile=%s remaining=%.1fs search_n=%s id_attempts=%s variants<=%s search_queries<=%s"
                % (
                    profile,
                    remaining,
                    budget.get("search_n"),
                    budget.get("id_attempt_limit"),
                    budget.get("variant_limit"),
                    budget.get("search_query_limit"),
                ),
                CYAN,
            )
            if hot_query_mode:
                log("MP3", "hot-query speed mode active for %s" % query, CYAN)
        if i >= int(budget["variant_limit"]):
            log("MP3", "dynamic budget stopped additional query variants (remaining=%.1fs)" % remaining, YELLOW)
            break
        try:
            result = _try_download_source_with_retry(qv=qv, budget=budget)
            if result is not None:
                _vid, _produced = result
                if _has_fresh_audio(_produced) or _has_fresh_audio(out_mp3):
                    return result
                last_err = "source produced no fresh audio for query %r" % qv
        except Exception as e:
            last_err = str(e)

    # Retry with source-corrected top-result hint when query text is heavily misspelled.
    # This mirrors what users see in youtube.com "Showing results for ...".
    low_last_err = str(last_err or "").lower()
    if retry_tier >= 2 and (("no ids" in low_last_err) or ("search failed" in low_last_err) or ("fast path failed" in low_last_err)):
        hint = _yt_search_top_result_hint(query, timeout_sec=min(8.0, YTDLP_SEARCH_TIMEOUT))
        hint_title = _clean_title(str(hint.get("title") or ""))
        hint_artist = _clean_title(str(hint.get("artist") or ""))
        hint_candidates: List[str] = []
        if hint_artist and hint_title:
            hint_candidates.extend(
                [
                    "%s %s" % (hint_artist, hint_title),
                    "%s - %s" % (hint_artist, hint_title),
                ]
            )
        if hint_title:
            hint_candidates.extend([hint_title, "%s lyrics" % hint_title])

        seen_hint: set[str] = set()
        for hq in hint_candidates:
            qq = " ".join((hq or "").split()).strip()
            if not qq or qq in seen_hint:
                continue
            seen_hint.add(qq)
            try:
                log("MP3", "Retrying with source hint query: %s" % qq, CYAN)
                hint_budget: Dict[str, int | str] = {
                    "search_n": (
                        max(1, min(max(3, int(search_n)), int(MP3_HOT_QUERY_SPEED_SEARCH_N)))
                        if hot_query_mode
                        else max(3, int(search_n))
                    ),
                    "id_attempt_limit": (
                        max(1, min(max(2, min(6, int(MP3_MAX_ID_ATTEMPTS))), int(MP3_HOT_QUERY_SPEED_MAX_ID_ATTEMPTS)))
                        if hot_query_mode
                        else max(2, min(6, int(MP3_MAX_ID_ATTEMPTS)))
                    ),
                    "search_query_limit": (
                        max(1, min(max(1, min(2, int(MP3_MAX_SEARCH_QUERY_VARIANTS))), int(MP3_HOT_QUERY_SPEED_MAX_SEARCH_QUERY_VARIANTS)))
                        if hot_query_mode
                        else max(1, min(2, int(MP3_MAX_SEARCH_QUERY_VARIANTS)))
                    ),
                }
                result = _try_download_source_with_retry(qv=qq, budget=hint_budget)
                if result is not None:
                    _vid, _produced = result
                    if _has_fresh_audio(_produced) or _has_fresh_audio(out_mp3):
                        return result
            except Exception as e:
                last_err = str(e)

    # Last resort: SoundCloud
    if retry_tier >= 3:
        for i, qv in enumerate(variants):
            elapsed = time.monotonic() - budget_started
            if elapsed >= MP3_TOTAL_TIMEOUT_SEC:
                budget_timed_out = True
                break
            remaining = max(0.0, MP3_TOTAL_TIMEOUT_SEC - elapsed)
            budget = _dynamic_search_budget(remaining_sec=remaining, base_search_n=search_n)
            budget = _apply_hot_query_speed_budget(query, budget)
            if i >= int(budget["variant_limit"]):
                break
            produced = _try_download_soundcloud(qv, out_mp3, deadline_monotonic=deadline_monotonic)
            if produced is not None:
                if _has_fresh_audio(produced) or _has_fresh_audio(out_mp3):
                    return ("", produced)
                last_err = "soundcloud produced no fresh audio for query %r" % qv

    if budget_timed_out:
        elapsed = time.monotonic() - budget_started
        raise RuntimeError("MP3 download timed out for query %r after %.1fs" % (query, elapsed))

    if last_err:
        raise RuntimeError("MP3 download failed for query %r\n%s" % (query, last_err))
    raise RuntimeError("MP3 download failed for query %r" % query)

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Step1: fetch LRC + source audio")
    ap.add_argument("--query", default="")
    ap.add_argument(
        "--lrc-artist",
        default="",
        help="Optional lyrics artist override (audio query remains unchanged)",
    )
    ap.add_argument(
        "--lrc-title",
        default="",
        help="Optional lyrics title override (audio query remains unchanged)",
    )
    ap.add_argument(
        "--lyric-start",
        default="",
        help="Optional first-lyric-line prefix constraint for synced lyrics selection",
    )
    ap.add_argument(
        "--audio-url",
        "--url",
        dest="audio_url",
        default="",
        help="Optional direct YouTube URL/ID for step1 audio source (query remains the LRC query)",
    )
    ap.add_argument(
        "--audio-id",
        default="",
        help="Alias of --audio-url; provide a YouTube video ID to force step1 audio source",
    )
    ap.add_argument("--slug", default="")
    ap.add_argument("-f", "--force", action="store_true")
    ap.add_argument("--reset", "-r", action="store_true", help="Delete step1 artifacts for this slug before running")
    ap.add_argument("--nuke", action="store_true", help="Aggressively delete all files starting with <slug> in mp3s/timings/meta")
    ap.add_argument("--yt-search-n", type=int, default=YT_SEARCH_N)
    ap.add_argument(
        "--retry-attempt",
        type=int,
        default=None,
        help="Fallback tier: 1=fast fail-hard, 2=balanced retries, 3=full recovery logic",
    )
    ap.add_argument(
        "--speed-mode",
        default=STEP1_SPEED_MODE_DEFAULT,
        choices=["turbo", "extra-turbo", "ultimate-light-speed"],
        help="Named speed profile for Step1 behavior",
    )
    ap.add_argument("--no-parallel", action="store_true", help="Disable LRC+MP3 parallel fetch (debug)")
    args = ap.parse_args(argv)
    query = str(getattr(args, "query", "") or "").strip()
    lrc_artist = str(getattr(args, "lrc_artist", "") or "").strip()
    lrc_title = str(getattr(args, "lrc_title", "") or "").strip()
    lrc_artist_title_query = " - ".join([part for part in [lrc_artist, lrc_title] if part]).strip()
    if not query and not lrc_artist_title_query:
        ap.error("query is required (provide --query or --lrc-artist/--lrc-title)")
    if not query and lrc_artist_title_query:
        args.query = lrc_artist_title_query
    return args


def step1_fetch(
    *,
    query: str,
    slug: str,
    force: bool,
    reset: bool,
    nuke: bool,
    yt_search_n: int,
    parallel: bool,
    retry_attempt: Optional[int] = None,
    speed_mode: str = "",
    lrc_artist: str = "",
    lrc_title: str = "",
    lyric_start: str = "",
    audio_source: str = "",
    disable_cache: bool = False,
    cache_first: bool = False,
    duration_aware_source_match: bool = MP3_DURATION_AWARE_SOURCE_MATCH_DEFAULT,
) -> dict:
    step_t0 = now_perf_ms()
    restore_cache_policy = _apply_runtime_cache_policy(disable_cache=bool(disable_cache))
    speed_mode_norm = _normalize_step1_speed_mode(speed_mode or "")
    if retry_attempt is not None:
        retry_tier = max(1, int(retry_attempt))
    elif speed_mode_norm:
        retry_tier = max(
            1,
            int(_retry_attempt_from_speed_mode(speed_mode_norm, fallback=int(STEP1_DEFAULT_RETRY_ATTEMPT))),
        )
    else:
        retry_tier = max(1, int(STEP1_DEFAULT_RETRY_ATTEMPT))
    fail_fast_mode = retry_tier <= 1
    balanced_mode = retry_tier == 2
    full_recovery_mode = retry_tier >= 3
    use_fast_lrc_mode = speed_mode_norm in {"extra-turbo", "ultimate-light-speed"}
    use_top_hit_audio_mode = bool((use_fast_lrc_mode and MP3_TOP_HIT_MODE) or MP3_SIMPLE_FIRST_RESULT_MODE)
    user_query = " ".join((query or "").split()).strip()
    slug = slugify(slug or user_query)
    normalized_query = _sanitize_search_query(user_query) or user_query
    raw_audio_intent_query = _sanitize_search_query(user_query) or user_query
    lrc_artist_override = _clean_title(str(lrc_artist or ""))
    lrc_title_override = _clean_title(str(lrc_title or ""))
    lyric_start_prefix = " ".join((lyric_start or "").split()).strip()
    duration_aware_source_match = bool(duration_aware_source_match)
    effective_yt_search_n = max(1, int(yt_search_n))
    if fail_fast_mode:
        effective_yt_search_n = min(effective_yt_search_n, int(STEP1_FAIL_FAST_SEARCH_N))
    elif balanced_mode:
        effective_yt_search_n = min(effective_yt_search_n, max(2, int(STEP1_FAIL_FAST_SEARCH_N) + 1))

    lookup_query = normalized_query
    forced_audio_source = _sanitize_search_query(str(audio_source or ""))
    audio_lookup_query = forced_audio_source or lookup_query
    lrc_lookup_query = lookup_query
    lookup_query_source = "normalized_query"
    lrc_lookup_query_source = lookup_query_source
    audio_lookup_query_source = "audio_source_override" if forced_audio_source else "lookup_query"
    query_normalization: Dict[str, Any] = {}
    normalization_error = ""
    normalized_artist = ""
    normalized_title = ""
    normalized_lookup = ""
    normalized_source_id = ""
    explicit_audio_variant_intent = bool((not forced_audio_source) and _query_has_explicit_audio_variant_intent(user_query))
    if fail_fast_mode:
        split_norm = _normalize_query_from_explicit_split(normalized_query, provider="retry_attempt_1_split")
        if split_norm:
            normalized_artist = _clean_title(str(split_norm.get("artist") or ""))
            normalized_title = _clean_title(str(split_norm.get("track") or split_norm.get("title") or ""))
            normalized_lookup = _clean_title(str(split_norm.get("normalized_query") or ""))
            lookup_query = normalized_lookup or lookup_query
            lookup_query_source = str(split_norm.get("provider") or "retry_attempt_1_split")
            query_normalization = dict(split_norm)
            log("QUERY", "retry-attempt=1 using split normalization: %s" % lookup_query, GREEN)
        else:
            lookup_query = _sanitize_search_query(normalized_query) or _sanitize_search_query(user_query) or user_query
            lookup_query_source = "retry_attempt_1_raw_query"
            query_normalization = {
                "provider": lookup_query_source,
                "normalized_query": lookup_query,
                "display": lookup_query,
            }
            log("QUERY", "retry-attempt=1 using raw query: %s" % lookup_query, YELLOW)

        # Keep fail-fast mode fast, but still canonicalize known brittle hot queries.
        # This avoids no-lyrics failures for a handful of validated aliases.
        hot_lookup_query = _canonicalize_hot_query(lookup_query)
        hot_artist, hot_title = _hot_query_artist_title(hot_lookup_query)
        if hot_artist and hot_title:
            normalized_artist = _clean_title(hot_artist)
            normalized_title = _clean_title(hot_title)
            lookup_query = _artist_title_query(normalized_artist, normalized_title) or hot_lookup_query or lookup_query
            lookup_query_source = "retry_attempt_1_hot_query"
            query_normalization = {
                "provider": lookup_query_source,
                "artist": normalized_artist,
                "track": normalized_title,
                "title": normalized_title,
                "normalized_query": lookup_query,
                "display": lookup_query,
                "short_circuit": "hot_query_canonical",
            }
            log("QUERY", "retry-attempt=1 hot-query canonicalized: %s" % lookup_query, CYAN)
        elif hot_lookup_query and (_normalize_key(hot_lookup_query) != _normalize_key(lookup_query)):
            lookup_query = hot_lookup_query
            lookup_query_source = "retry_attempt_1_hot_query"
            query_normalization = {
                "provider": lookup_query_source,
                "normalized_query": lookup_query,
                "display": lookup_query,
                "short_circuit": "hot_query_canonical",
            }
            log("QUERY", "retry-attempt=1 canonicalized query: %s" % lookup_query, CYAN)
    else:
        query_normalization = _normalize_query_via_ytsearch_top_result(
            normalized_query,
            timeout_sec=min(8.0, YTDLP_SEARCH_TIMEOUT),
        )
        normalization_error = str(query_normalization.get("error") or "").strip()
        if normalization_error:
            user_error = str(query_normalization.get("user_error") or normalization_error).strip()
            log("QUERY", "Strict normalization failed for %r (%s)" % (query, normalization_error), YELLOW)
            fallback_query = _sanitize_search_query(normalized_query) or _sanitize_search_query(user_query) or user_query
            if not fallback_query:
                raise RuntimeError(user_error)
            lookup_query = fallback_query
            lookup_query_source = "normalization_fallback_raw_query"
            log("QUERY", "Using raw query fallback: %s" % lookup_query, YELLOW)
        else:
            normalized_artist = _clean_title(str(query_normalization.get("artist") or ""))
            normalized_title = _clean_title(str(query_normalization.get("track") or query_normalization.get("title") or ""))
            normalized_lookup = _clean_title(str(query_normalization.get("normalized_query") or ""))
            normalized_source_id = str(query_normalization.get("video_id") or "").strip()
            if normalized_artist and normalized_title:
                lookup_query = _artist_title_query(normalized_artist, normalized_title) or normalized_lookup or lookup_query
                lookup_query_source = str(query_normalization.get("provider") or "yt_suggest_ytsearch1")
                log("QUERY", "Normalized via suggest+ytsearch1: %s" % lookup_query, GREEN)
            elif normalized_lookup:
                lookup_query = normalized_lookup
                lookup_query_source = str(query_normalization.get("provider") or "yt_suggest_ytsearch1")
                log("QUERY", "Using strict normalized query: %s" % lookup_query, GREEN)
            else:
                raise RuntimeError(_NORMALIZATION_USER_ERROR)
    if not forced_audio_source:
        if explicit_audio_variant_intent:
            audio_lookup_query = raw_audio_intent_query
            audio_lookup_query_source = "user_query_explicit_audio_intent"
        else:
            audio_lookup_query = lookup_query
            audio_lookup_query_source = "lookup_query"
    if lrc_artist_override or lrc_title_override:
        query_artist, query_title = _maybe_split_artist_title(lookup_query)
        effective_lrc_artist = lrc_artist_override or _clean_title(query_artist)
        effective_lrc_title = lrc_title_override or _clean_title(query_title)
        lrc_lookup_query = (
            _artist_title_query(effective_lrc_artist, effective_lrc_title)
            or effective_lrc_title
            or effective_lrc_artist
            or lookup_query
        )
        if lrc_artist_override and lrc_title_override:
            lrc_lookup_query_source = "lrc_artist_title_override"
        elif lrc_artist_override:
            lrc_lookup_query_source = "lrc_artist_override"
        else:
            lrc_lookup_query_source = "lrc_title_override"
    else:
        lrc_lookup_query = lookup_query
        lrc_lookup_query_source = lookup_query_source
    lrc_lookup_query_effective = lrc_lookup_query
    try:
        if reset or nuke:
            log("RESET", "Resetting slug=%s nuke=%s" % (slug, nuke), YELLOW)
            _reset_slug(slug, nuke=nuke)

        lrc_path = TIMINGS_DIR / ("%s.lrc" % slug)
        mp3_path = MP3_DIR / ("%s.mp3" % slug)
        meta_path = META_DIR / ("%s.step1.json" % slug)

        if force:
            # Force mode should do a true fresh fetch. If stale artifacts remain,
            # downstream checks may appear to "succeed" without a new download.
            for path in (mp3_path, lrc_path, meta_path):
                try:
                    if path.exists() or path.is_symlink():
                        path.unlink()
                except Exception:
                    pass
            try:
                for extra in MP3_DIR.glob("%s.*" % slug):
                    if extra == mp3_path:
                        continue
                    if extra.exists() or extra.is_symlink():
                        extra.unlink()
            except Exception:
                pass

        if (not force) and (not reset) and (not nuke):
            try:
                _hydrate_hot_alias_artifacts(
                    slug,
                    query,
                    lrc_path=lrc_path,
                    mp3_path=mp3_path,
                    meta_path=meta_path,
                )
            except Exception:
                pass

        existing_audio = _resolve_existing_step1_audio(slug, mp3_path)
        if (
            bool(cache_first)
            and lrc_path.exists()
            and existing_audio is not None
            and meta_path.exists()
            and (not force)
            and (not lyric_start_prefix)
            and (not lrc_artist_override)
            and (not lrc_title_override)
        ):
            # Cache-first mode favors immediate reuse over opportunistic lyric refresh
            # to keep repeat runs near-instant.
            log("STEP1", "Cache-first reuse for %s (skipping weak-lyrics refresh checks)" % slug, GREEN)
            return {
                "slug": slug,
                "reused": True,
                "source_id": _read_cached_id_from_slug_meta(slug),
                "audio_path": str(existing_audio),
            }

        refresh_cached_lyrics = False
        if lrc_path.exists() and meta_path.exists() and not force:
            refresh_cached_lyrics = _should_refresh_cached_lyrics(meta_path, lrc_path)
        if lrc_path.exists() and (not force) and lyric_start_prefix:
            matches_prefix, _first_line = _lrc_matches_lyric_start(lrc_path, lyric_start_prefix)
            if not matches_prefix:
                refresh_cached_lyrics = True
                log("STEP1", "Refreshing cached lyrics to satisfy --lyric-start constraint", YELLOW)

        if lrc_path.exists() and existing_audio is not None and meta_path.exists() and (not force) and (not refresh_cached_lyrics):
            log("STEP1", "Reusing existing artifacts for %s" % slug, GREEN)
            return {
                "slug": slug,
                "reused": True,
                "source_id": _read_cached_id_from_slug_meta(slug),
                "audio_path": str(existing_audio),
            }
        if refresh_cached_lyrics:
            log("STEP1", "Refreshing weak cached lyrics for %s while reusing audio" % slug, YELLOW)

        need_lrc = force or (not lrc_path.exists()) or refresh_cached_lyrics
        need_audio = force or (existing_audio is None)
        mode = "parallel" if (need_lrc and need_audio and parallel) else "sequential"
        log(
            "STEP1A",
            "step1 start mode=%s speed_mode=%s retry_attempt=%s need_lrc=%s need_audio=%s query=%s lookup_query=%s lrc_query=%s audio_query=%s lyric_start=%s"
            % (
                mode,
                (speed_mode_norm or "-"),
                retry_tier,
                need_lrc,
                need_audio,
                query,
                lookup_query,
                lrc_lookup_query,
                audio_lookup_query,
                (lyric_start_prefix or "-"),
            ),
            CYAN,
        )

        lrc_info: Dict[str, Any] = {}
        audio_path_used: Optional[Path] = existing_audio
        audio_source_match: Dict[str, Any] = {}

        def _audio_file_present(path: Optional[Path]) -> bool:
            try:
                return bool(path and path.exists() and path.stat().st_size > 0)
            except Exception:
                return False

        def _do_lrc() -> None:
            nonlocal lrc_info, lrc_lookup_query_effective, audio_lookup_query, audio_lookup_query_source
            t0 = now_perf_ms()
            try:
                log("STEP1A", "fetch_lyrics started", CYAN)
                if lyric_start_prefix:
                    log("LRC", "Applying lyric-start constraint: %r" % lyric_start_prefix, CYAN)

                def _fetch_lrc_once(lyrics_query: str, *, allow_fast: bool) -> Dict[str, Any]:
                    if allow_fast:
                        fast_timeout = float(LRC_FAST_TOTAL_TIMEOUT_SEC)
                        if speed_mode_norm == "ultimate-light-speed":
                            fast_timeout = min(fast_timeout, 2.2)
                        elif speed_mode_norm == "extra-turbo":
                            fast_timeout = min(fast_timeout, 3.5)
                        return fetch_best_synced_lrc_fast(
                            lyrics_query,
                            lrc_path,
                            timeout_sec=fast_timeout,
                        )
                    return fetch_best_synced_lrc(
                        lyrics_query,
                        lrc_path,
                        prefer_langs=LRC_PREFER_LANGS,
                        enable_source_fallback=bool(retry_tier >= 2),
                    )

                query_candidates: List[str] = []
                seen_candidates: set[str] = set()

                def _add_candidate(raw_query: str) -> None:
                    q = _sanitize_search_query(" ".join((raw_query or "").split()).strip())
                    if not q:
                        return
                    key = q.lower()
                    if key in seen_candidates:
                        return
                    seen_candidates.add(key)
                    query_candidates.append(q)

                def _title_variants(raw_title: str) -> List[str]:
                    title = _sanitize_search_query(raw_title)
                    if not title:
                        return []
                    out = [title]
                    lowered = title.lower()
                    if lowered.startswith("las "):
                        out.append("La " + title[4:])
                    elif lowered.startswith("la "):
                        out.append("Las " + title[3:])
                    elif lowered.startswith("los "):
                        out.append("El " + title[4:])
                    elif lowered.startswith("el "):
                        out.append("Los " + title[3:])
                    return out

                _add_candidate(lrc_lookup_query)
                if lyric_start_prefix:
                    _add_candidate("%s %s" % (lrc_lookup_query, lyric_start_prefix))
                    split_artist, split_title = _maybe_split_artist_title(lrc_lookup_query)
                    lrc_title_seed = _clean_title(
                        lrc_title_override
                        or split_title
                        or (lrc_lookup_query if (not split_artist and not split_title) else "")
                    )
                    for title_variant in _title_variants(lrc_title_seed):
                        _add_candidate("%s %s" % (title_variant, lyric_start_prefix))
                        _add_candidate(title_variant)

                if not query_candidates:
                    query_candidates = [lrc_lookup_query]

                info: Dict[str, Any] = {"ok": False, "provider": "", "reason": "no_synced_lyrics_found"}
                matched_first_line = ""
                matched_query = ""
                mismatch_reason = ""
                matched_constraint = False
                for idx, lyrics_query in enumerate(query_candidates):
                    if idx == 0:
                        log("LRC", "Fetching synced lyrics: %s" % lyrics_query, CYAN)
                    else:
                        log(
                            "LRC",
                            "Retrying synced lyrics (%d/%d): %s"
                            % (idx + 1, len(query_candidates), lyrics_query),
                            YELLOW,
                        )
                    fetched = _fetch_lrc_once(
                        lyrics_query,
                        allow_fast=bool(use_fast_lrc_mode and idx == 0 and (not lyric_start_prefix)),
                    )
                    info = dict(fetched or {})
                    if not fetched.get("ok"):
                        continue
                    if lyric_start_prefix:
                        matches_prefix, first_line = _lrc_matches_lyric_start(lrc_path, lyric_start_prefix)
                        if not matches_prefix:
                            mismatch_reason = (
                                "lyric_start_mismatch expected=%r got=%r"
                                % (lyric_start_prefix, first_line or "")
                            )
                            log("LRC", "Rejected lyrics candidate: %s" % mismatch_reason, YELLOW)
                            continue
                        matched_first_line = first_line
                        matched_query = lyrics_query
                        matched_constraint = True
                    lrc_lookup_query_effective = lyrics_query
                    break

                if lyric_start_prefix and (not matched_constraint):
                    info = {
                        "ok": False,
                        "provider": str((info or {}).get("provider") or ""),
                        "reason": mismatch_reason or str((info or {}).get("reason") or "no_synced_lyrics_found"),
                    }
                if lyric_start_prefix and info.get("ok"):
                    info = dict(info)
                    info["lyric_start"] = lyric_start_prefix
                    info["lyric_start_first_line"] = matched_first_line
                    info["lyric_start_query"] = matched_query or lrc_lookup_query_effective

                lrc_info = dict(info or {})
                if not info.get("ok"):
                    hard_fail_lyrics = bool(
                        STRICT_REQUIRE_LYRICS
                        or use_fast_lrc_mode
                        or (fail_fast_mode and STEP1_FAIL_FAST_SKIP_PSEUDO_LRC)
                        or bool(lyric_start_prefix)
                    )
                    if hard_fail_lyrics:
                        reason = str((info or {}).get("reason") or "no_synced_lyrics_found")
                        lrc_info = {
                            "ok": False,
                            "provider": str((info or {}).get("provider") or ""),
                            "reason": reason,
                        }
                        log("LRC", "Strict lyrics mode: synced lyrics missing (%s); deferring final failure check" % reason, YELLOW)
                    else:
                        hint = _yt_search_top_result_hint(lookup_query, timeout_sec=min(8.0, YTDLP_SEARCH_TIMEOUT))
                        hint_artist = _clean_title(str(hint.get("artist") or ""))
                        hint_title = _clean_title(str(hint.get("title") or ""))
                        lines = _fallback_seed_lines(lookup_query, hint_artist=hint_artist, hint_title=hint_title)
                        if not lines:
                            lines = ["Lyrics unavailable"]
                        seed_line = lines[0]
                        fallback_lrc = _pseudo_lrc_from_lines(
                            lines,
                            start_secs=LRC_PSEUDO_START_SECS,
                            step_secs=max(2.0, LRC_PSEUDO_STEP_SECS),
                            max_lines=max(2, LRC_PSEUDO_MAX_LINES),
                        )
                        ensure_dir(lrc_path.parent)
                        lrc_path.write_text(fallback_lrc, encoding="utf-8")
                        lrc_info = {
                            "ok": True,
                            "provider": "step1_fallback_pseudo",
                            "reason": info.get("reason"),
                            "seed_line": seed_line,
                        }
                        log(
                            "LRC",
                            "No synced lyrics found; wrote pseudo fallback LRC to %s (seed=%s)"
                            % (lrc_path, seed_line),
                            YELLOW,
                        )
                else:
                    log("LRC", "Wrote %s (provider=%s)" % (lrc_path, info.get("provider")), GREEN)
                if not forced_audio_source:
                    lrc_provider = str((lrc_info or {}).get("provider") or "").strip().lower()
                    lrc_title = _clean_title(str((lrc_info or {}).get("title") or ""))
                    lrc_driven_query = bool(
                        bool(STEP1_USE_LRC_AUDIO_QUERY)
                        and bool((lrc_info or {}).get("ok"))
                        and lrc_provider != "step1_fallback_pseudo"
                        and bool(lrc_title)
                        and (not explicit_audio_variant_intent)
                    )
                    resolved_audio_query = _audio_query_from_lrc_info(lookup_query, lrc_info)
                    if lrc_driven_query:
                        audio_lookup_query_source = "lrc_metadata"
                    if lrc_driven_query and _normalize_key(resolved_audio_query) != _normalize_key(audio_lookup_query):
                        audio_lookup_query = resolved_audio_query
                        log("MP3", "Using LRC-derived source query: %s" % audio_lookup_query, CYAN)
                log("STEP1A", "fetch_lyrics completed", GREEN)
            finally:
                log_timing("step1", "fetch_lyrics", t0, color=CYAN)

        def _do_mp3() -> Tuple[Optional[str], Path]:
            nonlocal audio_lookup_query_source
            t0 = now_perf_ms()
            try:
                log("STEP1B", "download_audio started", CYAN)
                query_for_audio = audio_lookup_query or lookup_query
                if (
                    (not forced_audio_source)
                    and _is_valid_source_id(normalized_source_id)
                    and (_normalize_key(query_for_audio) == _normalize_key(lookup_query))
                ):
                    query_for_audio = normalized_source_id
                    audio_lookup_query_source = "query_normalization_video_id"
                    log("MP3", "Using normalized query video id fast path: %s" % normalized_source_id, CYAN)
                target_duration_sec: Optional[float] = None
                local_lrc_duration_timeout = max(
                    0.0,
                    float(os.environ.get("MIXTERIOSO_STEP1_LOCAL_LRC_HINT_WAIT_SECS", "1.4")),
                )
                should_use_duration_match = bool(
                    duration_aware_source_match
                    and (not forced_audio_source)
                    and (not explicit_audio_variant_intent)
                    and _query_needs_duration_disambiguation(query_for_audio)
                )
                if (
                    should_use_duration_match
                    and use_top_hit_audio_mode
                    and local_lrc_duration_timeout > 0.0
                ):
                    target_duration_sec = _wait_for_local_lrc_target_duration_sec(
                        lrc_path,
                        timeout_sec=local_lrc_duration_timeout,
                    )
                    if target_duration_sec is not None:
                        log("MP3", "using local LRC duration hint target=%.3fs" % float(target_duration_sec), CYAN)
                elif (not duration_aware_source_match) and _query_needs_duration_disambiguation(query_for_audio):
                    log("MP3", "duration-aware source matching disabled; skipping duration-hint probe", CYAN)
                elif explicit_audio_variant_intent and _query_needs_duration_disambiguation(query_for_audio):
                    log("MP3", "explicit audio variant intent: skipping duration-hint probe", CYAN)
                elif MP3_SIMPLE_FIRST_RESULT_MODE:
                    log("MP3", "simple first-result mode: skipping duration-hint probe", CYAN)
                elif should_use_duration_match and not (use_top_hit_audio_mode and STEP1_FAST_SKIP_DURATION_HINT):
                    target_duration_sec = _guess_lrc_target_duration_sec(query_for_audio, timeout_sec=1.8)
                else:
                    log("MP3", "top-hit fast mode: skipping duration-hint probe", CYAN)
                if target_duration_sec is not None:
                    log("MP3", "duration-aware ranking target=%.3fs" % float(target_duration_sec), CYAN)
                call_kwargs: Dict[str, Any] = {
                    "search_n": effective_yt_search_n,
                    "retry_attempt": retry_tier,
                    "expected_duration_sec": target_duration_sec,
                    "prefer_top_hit": use_top_hit_audio_mode,
                }
                # Backward compatibility for tests/mocks that still use old signatures.
                # Remove unknown kwargs progressively until a compatible call succeeds.
                while True:
                    try:
                        vid, produced_audio = download_first_working_mp3(
                            query_for_audio,
                            mp3_path,
                            **call_kwargs,
                        )
                        break
                    except TypeError as exc:
                        err = str(exc)
                        if "unexpected keyword argument" not in err:
                            raise
                        removed = False
                        for key in ("prefer_top_hit", "expected_duration_sec", "retry_attempt"):
                            if (key in err) and (key in call_kwargs):
                                call_kwargs.pop(key, None)
                                removed = True
                                break
                        if not removed:
                            for key in ("prefer_top_hit", "expected_duration_sec", "retry_attempt"):
                                if key in call_kwargs:
                                    call_kwargs.pop(key, None)
                                    removed = True
                                    break
                        if not removed:
                            raise
                if STRICT_REQUIRE_AUDIO and (not _audio_file_present(produced_audio)) and (not _audio_file_present(mp3_path)):
                    raise RuntimeError("No audio found for query: %r" % query)
                log("MP3", "Wrote %s" % produced_audio, GREEN)
                log("STEP1B", "download_audio completed", GREEN)
                return (vid or None, produced_audio)
            finally:
                log_timing("step1", "download_audio", t0, color=CYAN)

        vid_used: Optional[str] = None

        if need_lrc and need_audio and parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fut_lrc = ex.submit(_do_lrc)
                fut_mp3 = ex.submit(_do_mp3)
                _ = fut_lrc.result()
                vid_used, audio_path_used = fut_mp3.result()
        else:
            if need_lrc:
                _do_lrc()
            if need_audio:
                vid_used, audio_path_used = _do_mp3()

        if audio_path_used is None:
            audio_path_used = _resolve_existing_step1_audio(slug, mp3_path)
        if audio_path_used is None:
            raise RuntimeError("Step1 audio missing for slug %s after download/reuse" % slug)
        if STRICT_REQUIRE_AUDIO and (not audio_path_used.exists() or audio_path_used.stat().st_size <= 0):
            raise RuntimeError("No audio found for slug %s after download/reuse" % slug)

        # If typo-heavy query fallback produced weak pseudo lyrics, retry captions using
        # the actual resolved video id (more reliable than query-based subtitle search).
        if full_recovery_mode and vid_used and _is_probable_source_video_id(str(vid_used)):
            lrc_provider = str((lrc_info or {}).get("provider") or "")
            lrc_ok = bool((lrc_info or {}).get("ok"))
            lrc_existing_lines = 0
            try:
                lrc_existing_lines = _lrc_line_count(lrc_path.read_text(encoding="utf-8"))
            except Exception:
                lrc_existing_lines = 0
            should_recover_lrc = (not lrc_ok) or (lrc_provider == "step1_fallback_pseudo") or (lrc_existing_lines < 2)
            # Direct caption recovery is expensive and often bot-blocked without cookies.
            has_runtime_cookies = bool((YTDLP_COOKIES_PATH or "").strip())
            allow_no_cookie_recovery = bool(STRICT_REQUIRE_LYRICS and STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE)
            if should_recover_lrc:
                if has_runtime_cookies or allow_no_cookie_recovery:
                    try:
                        direct_source = "https://www.youtube.com/watch?v=%s" % vid_used
                        recovered = _try_source_captions_lrc(direct_source, lrc_path, prefer_langs=LRC_PREFER_LANGS)
                        if recovered and bool(recovered.get("ok")):
                            lrc_info = recovered
                            log("LRC", "Recovered lyrics from resolved video id=%s" % vid_used, GREEN)
                    except Exception:
                        pass
                else:
                    log("LRC", "Skipping direct video-id lyric recovery (no runtime cookies)", YELLOW)

        if fail_fast_mode and STEP1_FAIL_FAST_SKIP_CANONICAL_RECOVERY:
            canonical_artist = _clean_title(normalized_artist or str((lrc_info or {}).get("artist") or ""))
            canonical_title = _clean_title(normalized_title or str((lrc_info or {}).get("title") or ""))
            if not canonical_artist and not canonical_title:
                split_artist, split_title = _maybe_split_artist_title(lookup_query)
                canonical_artist = _clean_title(split_artist)
                canonical_title = _clean_title(split_title)
        else:
            canonical_artist, canonical_title = _resolve_canonical_artist_title(
                lookup_query,
                lrc_info,
                vid_used,
                prefer_query_hint=(not bool((lrc_info or {}).get("ok"))) or str((lrc_info or {}).get("provider") or "") == "step1_fallback_pseudo",
            )

        # One more lyric recovery pass when canonical source metadata disagrees with lyrics metadata.
        lrc_provider = str((lrc_info or {}).get("provider") or "").strip().lower()
        canonical_queries = _canonical_lrc_queries(canonical_artist, canonical_title)
        should_retry_canonical_lrc = bool(canonical_queries) and (
            lrc_provider == "step1_fallback_pseudo"
            or _lyrics_metadata_mismatch(
                lrc_info,
                canonical_artist=canonical_artist,
                canonical_title=canonical_title,
            )
        )
        if full_recovery_mode and should_retry_canonical_lrc:
            try:
                for canonical_query in canonical_queries[: max(0, int(LRC_CANONICAL_MAX_QUERIES))]:
                    recovered_lrc = fetch_best_synced_lrc(
                        canonical_query,
                        lrc_path,
                        prefer_langs=LRC_PREFER_LANGS,
                        enable_source_fallback=False,
                    )
                    if recovered_lrc and bool(recovered_lrc.get("ok")):
                        lrc_info = recovered_lrc
                        if canonical_artist:
                            lrc_info["artist"] = canonical_artist
                        if canonical_title:
                            lrc_info["title"] = canonical_title
                        log("LRC", "Recovered synced lyrics from canonical query: %s" % canonical_query, GREEN)
                        break
            except Exception:
                pass

        # Keep fallback pseudo lines human-readable/canonical when possible.
        if str((lrc_info or {}).get("provider") or "") == "step1_fallback_pseudo":
            noisy_artist = bool(re.search(r"\b(topic|records?|entity|official|music|vevo)\b", canonical_artist.lower()))
            if noisy_artist and canonical_title:
                canonical_seed = canonical_title
            else:
                canonical_seed = " - ".join([x for x in [canonical_artist, canonical_title] if x]).strip() or canonical_title or canonical_artist
            if canonical_seed:
                lines = [line for line in [canonical_seed, canonical_title or canonical_seed] if line]
                fallback_lrc = _pseudo_lrc_from_lines(
                    lines,
                    start_secs=LRC_PSEUDO_START_SECS,
                    step_secs=max(2.0, LRC_PSEUDO_STEP_SECS),
                    max_lines=max(2, LRC_PSEUDO_MAX_LINES),
                )
                ensure_dir(lrc_path.parent)
                lrc_path.write_text(fallback_lrc, encoding="utf-8")
                lrc_info = dict(lrc_info or {})
                lrc_info["seed_line"] = canonical_seed
                lrc_info["artist"] = canonical_artist
                lrc_info["title"] = canonical_title

        if (
            bool(STEP1_ENFORCE_SOURCE_MATCH_RETRY)
            and vid_used
            and _is_probable_source_video_id(str(vid_used))
            and (canonical_artist or canonical_title)
        ):
            matched_source = _source_video_matches_expected(
                str(vid_used),
                expected_artist=canonical_artist,
                expected_title=canonical_title,
            )
            audio_source_match = {
                "checked": True,
                "matched": bool(matched_source),
                "expected_artist": canonical_artist,
                "expected_title": canonical_title,
                "source_id": str(vid_used),
            }
            if not matched_source:
                retry_query = _artist_title_query(canonical_artist, canonical_title) or audio_lookup_query or lookup_query
                audio_source_match["retry_query"] = retry_query
                if _normalize_key(retry_query) != _normalize_key(audio_lookup_query):
                    audio_source_match["retry_attempted"] = True
                    try:
                        log(
                            "MP3",
                            "source metadata mismatch for id=%s; retrying with canonical query=%s"
                            % (vid_used, retry_query),
                            YELLOW,
                        )
                        retry_vid, retry_audio_path = download_first_working_mp3(
                            retry_query,
                            mp3_path,
                            search_n=effective_yt_search_n,
                        )
                        retry_used = str(retry_vid or "").strip()
                        if retry_used and _is_valid_source_id(retry_used):
                            vid_used = retry_used
                        audio_path_used = retry_audio_path
                        audio_lookup_query = retry_query
                        audio_lookup_query_source = "canonical_retry_query"
                        retry_matched = False
                        if vid_used and _is_probable_source_video_id(str(vid_used)):
                            retry_matched = _source_video_matches_expected(
                                str(vid_used),
                                expected_artist=canonical_artist,
                                expected_title=canonical_title,
                            )
                        audio_source_match["matched_after_retry"] = bool(retry_matched)
                    except Exception as retry_exc:
                        audio_source_match["retry_error"] = str(retry_exc)
                        log("MP3", "source match retry failed: %s" % retry_exc, YELLOW)

        if lyric_start_prefix:
            matches_prefix, first_line = _lrc_matches_lyric_start(lrc_path, lyric_start_prefix)
            if not matches_prefix:
                raise RuntimeError(
                    "No synced lyrics found for query: %r (lyric_start_mismatch expected=%r got=%r)"
                    % (query, lyric_start_prefix, first_line or "")
                )
            lrc_info = dict(lrc_info or {})
            lrc_info["lyric_start"] = lyric_start_prefix
            lrc_info["lyric_start_first_line"] = first_line
            lrc_info["lyric_start_query"] = str((lrc_info or {}).get("lyric_start_query") or lrc_lookup_query_effective)

        if STRICT_REQUIRE_LYRICS or lyric_start_prefix:
            if lyric_start_prefix and (not bool((lrc_info or {}).get("ok"))):
                reason = str((lrc_info or {}).get("reason") or "lyric_start_constraint_not_met")
                raise RuntimeError("No synced lyrics found for query: %r (%s)" % (query, reason))
            strict_lrc_lines = 0
            try:
                strict_lrc_lines = _lrc_line_count(lrc_path.read_text(encoding="utf-8"))
            except Exception:
                strict_lrc_lines = 0
            if strict_lrc_lines < 1:
                reason = str((lrc_info or {}).get("reason") or "no_synced_lyrics_found")
                raise RuntimeError("No synced lyrics found for query: %r (%s)" % (query, reason))

        ensure_dir(meta_path.parent)
        log("STEP1C", "writing step1 metadata", CYAN)
        meta_t0 = now_perf_ms()
        meta_path.write_text(
            json.dumps(
                {
                    "slug": slug,
                    "query": query,
                    "query_sanitized": normalized_query,
                    "lookup_query": lookup_query,
                    "audio_lookup_query": audio_lookup_query,
                    "lookup_query_source": lookup_query_source,
                    "audio_lookup_query_source": audio_lookup_query_source,
                    "audio_source_match": audio_source_match,
                    "lrc_lookup_query": lrc_lookup_query,
                    "lrc_lookup_query_effective": lrc_lookup_query_effective,
                    "lrc_lookup_query_source": lrc_lookup_query_source,
                    "lyric_start_constraint": lyric_start_prefix,
                    "query_normalization": {
                        "provider": str(query_normalization.get("provider") or ""),
                        "artist": str(query_normalization.get("artist") or ""),
                        "track": str(query_normalization.get("track") or query_normalization.get("title") or ""),
                        "title": str(query_normalization.get("track") or query_normalization.get("title") or ""),
                        "display": str(query_normalization.get("display") or ""),
                        "confidence": str(query_normalization.get("confidence") or ""),
                        "normalized_query": str(query_normalization.get("normalized_query") or ""),
                        "suggestion": str(query_normalization.get("suggestion") or ""),
                        "short_circuit": str(query_normalization.get("short_circuit") or ""),
                        "error": str(query_normalization.get("error") or ""),
                        "user_error": str(query_normalization.get("user_error") or ""),
                        "raw_title": str(query_normalization.get("raw_title") or ""),
                        "raw_uploader": str(query_normalization.get("raw_uploader") or ""),
                        "raw_channel": str(query_normalization.get("raw_channel") or ""),
                        "raw_track": str(query_normalization.get("raw_track") or ""),
                        "raw_artist": str(query_normalization.get("raw_artist") or ""),
                        "raw_album_artist": str(query_normalization.get("raw_album_artist") or ""),
                        "raw_artists": str(query_normalization.get("raw_artists") or ""),
                        "video_id": str(query_normalization.get("video_id") or ""),
                    },
                    "artist": canonical_artist,
                    "title": canonical_title,
                    "mp3": str(mp3_path),
                    "audio_path": str(audio_path_used),
                    "audio_ext": str(audio_path_used.suffix or "").lower(),
                    "audio_no_transcode": bool(STEP1_FAST_NO_TRANSCODE and str(audio_path_used.suffix or "").lower() != ".mp3"),
                    "audio_alias_mp3": bool(STEP1_FAST_ALIAS_MP3),
                    "lrc": str(lrc_path),
                    "source_id": vid_used,
                    "yt_search_n": int(effective_yt_search_n),
                    "retry_attempt": int(retry_tier),
                    "speed_mode": str(speed_mode_norm),
                    "lrc_fetch": lrc_info,
                    "ytdlp": {
                        "socket_timeout": str(YTDLP_SOCKET_TIMEOUT),
                        "retries": str(YTDLP_RETRIES),
                        "fragment_retries": str(YTDLP_FRAG_RETRIES),
                        "concurrent_fragments": str(_default_concurrent_fragments()),
                        "concurrent_fragments_adaptive": bool(YTDLP_CONCURRENT_FRAGS_ADAPTIVE),
                        "audio_quality": str(YTDLP_AUDIO_QUALITY),
                        "format": str(YTDLP_FORMAT),
                        "extractor_args": str(YTDLP_EXTRACTOR_ARGS),
                        "fast_no_transcode": bool(STEP1_FAST_NO_TRANSCODE),
                        "fast_preferred_audio_only_format": str(STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT),
                        "fast_audio_only_format": str(STEP1_FAST_AUDIO_ONLY_FORMAT),
                        "fast_no_transcode_format": str(STEP1_FAST_NO_TRANSCODE_FORMAT),
                        "parallel_strategy_race": bool(MP3_ENABLE_PARALLEL_STRATEGY_RACE),
                        "dynamic_search_budget": bool(MP3_ENABLE_DYNAMIC_SEARCH_BUDGET),
                        "hot_query_speed_mode": bool(MP3_HOT_QUERY_SPEED_MODE),
                        "cmd": " ".join(YTDLP_CMD),
                        "search_span": int(YTDLP_SEARCH_SPAN),
                        "js_runtimes": str(YTDLP_JS_RUNTIMES),
                        "remote_components": str(YTDLP_REMOTE_COMPONENTS),
                        "user_agent": str(YTDLP_UA),
                        "cookies": bool(YTDLP_COOKIES_PATH),
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        log_timing("step1", "write_metadata", meta_t0, color=CYAN)
        log("META", "Wrote %s" % meta_path, GREEN)
        log("STEP1C", "step1 complete", GREEN)

        return {"slug": slug, "reused": False, "source_id": vid_used, "audio_path": str(audio_path_used)}
    finally:
        try:
            restore_cache_policy()
        except Exception:
            pass
        log_timing("step1", "total", step_t0, color=CYAN)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    _ = step1_fetch(
        query=(args.query or "").strip(),
        lrc_artist=(args.lrc_artist or "").strip(),
        lrc_title=(args.lrc_title or "").strip(),
        lyric_start=(args.lyric_start or "").strip(),
        audio_source=((args.audio_url or "").strip() or (args.audio_id or "").strip()),
        slug=(args.slug or "").strip(),
        force=bool(args.force),
        reset=bool(args.reset),
        nuke=bool(args.nuke),
        yt_search_n=int(args.yt_search_n),
        parallel=not bool(args.no_parallel),
        retry_attempt=(int(args.retry_attempt) if args.retry_attempt is not None else None),
        speed_mode=str(args.speed_mode or STEP1_SPEED_MODE_DEFAULT),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# end of scripts/step1_fetch.py
