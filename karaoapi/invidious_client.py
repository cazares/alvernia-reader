"""
Invidious API client for YouTube audio extraction.
Bypasses YouTube blocking by using Invidious proxy instances.
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Public Invidious instances (fallback list)
INVIDIOUS_INSTANCES = [
    "https://invidious.fdn.fr",
    "https://inv.tux.pizza",
    "https://invidious.privacyredirect.com",
    "https://y.com.sb",
    "https://invidious.nerdvpn.de",
]

DEFAULT_CONNECT_TIMEOUT_SEC = 4.0
DEFAULT_READ_TIMEOUT_SEC = 12.0
INSTANCE_COOLDOWN_BASE_SEC = 30.0
INSTANCE_COOLDOWN_MAX_SEC = 300.0
MAX_SEARCH_LIMIT = 25
MAX_QUERY_LENGTH = 300


class InvidiousClient:
    """Client for interacting with Invidious API."""

    _instance_lock = threading.Lock()
    _instance_failure_streak: Dict[str, int] = {}
    _instance_cooldown_until: Dict[str, float] = {}

    def __init__(
        self,
        instance_url: Optional[str] = None,
        *,
        session: Optional[requests.Session] = None,
        connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT_SEC,
        read_timeout_sec: float = DEFAULT_READ_TIMEOUT_SEC,
        max_http_retries: int = 1,
    ):
        """
        Initialize Invidious client.

        Args:
            instance_url: Specific Invidious instance to use. If None, will try public instances.
            session: Optional preconfigured requests session.
            connect_timeout_sec: TCP connect timeout in seconds.
            read_timeout_sec: Response body timeout in seconds.
            max_http_retries: Requests-level retries for transient 5xx/429 errors.
        """
        configured = str(instance_url or "").strip()
        self.instances = [configured] if configured else INVIDIOUS_INSTANCES.copy()
        if not self.instances:
            raise ValueError("At least one Invidious instance is required")
        self._timeout = (
            max(0.5, float(connect_timeout_sec)),
            max(1.0, float(read_timeout_sec)),
        )
        self._session = session or self._build_session(max_http_retries=max_http_retries)

    @staticmethod
    def _build_session(*, max_http_retries: int) -> requests.Session:
        retries = max(0, int(max_http_retries))
        retry_config = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=0.25,
            allowed_methods=frozenset({"GET"}),
            status_forcelist=(429, 500, 502, 503, 504),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_config, pool_connections=8, pool_maxsize=8)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "mixterioso-invidious-client/1.0",
            }
        )
        return session

    @classmethod
    def _mark_instance_success(cls, instance: str) -> None:
        with cls._instance_lock:
            cls._instance_failure_streak.pop(instance, None)
            cls._instance_cooldown_until.pop(instance, None)

    @classmethod
    def _mark_instance_failure(cls, instance: str) -> float:
        with cls._instance_lock:
            streak = int(cls._instance_failure_streak.get(instance, 0)) + 1
            cls._instance_failure_streak[instance] = streak
            cooldown_sec = min(INSTANCE_COOLDOWN_BASE_SEC * (2 ** (streak - 1)), INSTANCE_COOLDOWN_MAX_SEC)
            cls._instance_cooldown_until[instance] = time.monotonic() + cooldown_sec
            return float(cooldown_sec)

    @classmethod
    def _cooldown_remaining_sec(cls, instance: str) -> float:
        with cls._instance_lock:
            until = float(cls._instance_cooldown_until.get(instance, 0.0))
        remaining = until - time.monotonic()
        return remaining if remaining > 0 else 0.0

    def _iter_instances(self) -> List[str]:
        ready: List[str] = []
        cooling: List[Tuple[float, str]] = []
        for instance in self.instances:
            remaining = self._cooldown_remaining_sec(instance)
            if remaining <= 0:
                ready.append(instance)
            else:
                cooling.append((remaining, instance))
        if ready:
            return ready
        cooling.sort(key=lambda item: item[0])
        return [instance for _, instance in cooling]

    def _request_json(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, str]:
        errors: List[str] = []
        for instance in self._iter_instances():
            url = f"{instance}{path}"
            try:
                logger.info("Invidious request %s", url)
                response = self._session.get(url, params=params, timeout=self._timeout)
                response.raise_for_status()
                payload = response.json()
                self._mark_instance_success(instance)
                return payload, instance
            except Exception as exc:
                cooldown = self._mark_instance_failure(instance)
                errors.append(f"{instance}: {exc}")
                logger.warning(
                    "Invidious instance failed: %s (cooldown=%ss, error=%s)",
                    instance,
                    int(round(cooldown)),
                    exc,
                )
                continue
        summary = "; ".join(errors[:3])
        raise RuntimeError(f"All Invidious instances failed ({summary})")

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _normalize_stream_url(url: str, instance: str) -> str:
        cleaned = str(url or "").strip()
        if not cleaned:
            return ""
        if cleaned.startswith("//"):
            return f"https:{cleaned}"
        if cleaned.startswith("/"):
            return urljoin(f"{instance.rstrip('/')}/", cleaned.lstrip("/"))
        return cleaned

    @staticmethod
    def _sanitize_query(query: str) -> str:
        text = str(query or "").strip()
        if not text:
            raise ValueError("query is required")
        if len(text) > MAX_QUERY_LENGTH:
            return text[:MAX_QUERY_LENGTH]
        return text

    @staticmethod
    def _sanitize_video_id(video_id: str) -> str:
        value = str(video_id or "").strip()
        if not value:
            raise ValueError("video_id is required")
        return value

    def _get_video_info_with_instance(self, video_id: str) -> Tuple[Dict[str, Any], str]:
        clean_video_id = self._sanitize_video_id(video_id)
        payload, instance = self._request_json(f"/api/v1/videos/{clean_video_id}")
        if not isinstance(payload, dict):
            raise RuntimeError("Invidious returned invalid video info payload")
        logger.info("Got video info from %s: %s", instance, payload.get("title", "N/A"))
        return payload, instance

    def search(self, query: str, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos on YouTube via Invidious.

        Args:
            query: Search query
            limit: Number of results to return

        Returns:
            List of video metadata dicts
        """
        clean_query = self._sanitize_query(query)
        safe_limit = max(1, min(int(limit), MAX_SEARCH_LIMIT))
        payload, instance = self._request_json(
            "/api/v1/search",
            params={
                "q": clean_query,
                "type": "video",
            },
        )
        if not isinstance(payload, list):
            raise RuntimeError("Invidious returned invalid search payload")
        results = [item for item in payload if isinstance(item, dict)]
        logger.info("Found %s results from %s", len(results), instance)
        return results[:safe_limit]

    def get_video_info(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed video information including audio streams.

        Args:
            video_id: YouTube video ID

        Returns:
            Video metadata including audio stream URLs
        """
        video_info, _instance = self._get_video_info_with_instance(video_id)
        return video_info

    def get_audio_url(self, video_id: str) -> tuple[str, Dict[str, Any]]:
        """
        Get the best audio stream URL for a video.

        Args:
            video_id: YouTube video ID

        Returns:
            Tuple of (audio_url, metadata)
        """
        clean_video_id = self._sanitize_video_id(video_id)
        video_info, instance = self._get_video_info_with_instance(clean_video_id)

        # Get adaptive formats (audio-only streams)
        adaptive_formats = video_info.get("adaptiveFormats", [])
        if not isinstance(adaptive_formats, list):
            adaptive_formats = []

        # Find best audio format (prefer opus/m4a)
        audio_streams = [
            f for f in adaptive_formats
            if isinstance(f, dict)
            if f.get("type", "").startswith("audio/")
        ]

        if not audio_streams:
            raise RuntimeError("No audio streams found")

        # Sort by bitrate (highest first)
        audio_streams.sort(key=lambda x: self._coerce_int(x.get("bitrate"), 0), reverse=True)

        audio_url = ""
        for stream in audio_streams:
            stream_url = self._normalize_stream_url(str(stream.get("url") or ""), instance)
            if stream_url:
                audio_url = stream_url
                break
        if not audio_url:
            raise RuntimeError("No audio URL in stream")

        metadata = {
            "title": str(video_info.get("title") or ""),
            "duration": self._coerce_int(video_info.get("lengthSeconds"), 0),
            "video_id": clean_video_id,
            "thumbnail": self._get_best_thumbnail(video_info),
        }

        logger.info("Audio URL extracted: %s (%ss)", metadata["title"], metadata["duration"])
        return audio_url, metadata

    def _get_best_thumbnail(self, video_info: Dict[str, Any]) -> str:
        """Get the highest quality thumbnail URL."""
        thumbnails = video_info.get("videoThumbnails", [])
        if isinstance(thumbnails, list) and thumbnails:
            quality_rank = {
                "maxres": 5,
                "sddefault": 4,
                "hqdefault": 3,
                "mqdefault": 2,
                "default": 1,
            }

            def _thumb_score(thumb: Dict[str, Any]) -> Tuple[int, int]:
                quality = str(thumb.get("quality") or "").lower()
                width = self._coerce_int(thumb.get("width"), 0)
                return quality_rank.get(quality, 0), width

            best = sorted(
                (thumb for thumb in thumbnails if isinstance(thumb, dict)),
                key=_thumb_score,
                reverse=True,
            )
            if best:
                url = str(best[0].get("url") or "").strip()
                if url:
                    return self._normalize_stream_url(url, self.instances[0])
        clean_video_id = str(video_info.get("videoId") or "").strip()
        return f"https://img.youtube.com/vi/{clean_video_id}/maxresdefault.jpg"


def search_and_get_audio(query: str) -> tuple[str, Dict[str, Any]]:
    """
    Search for a song and get its audio URL.

    Args:
        query: Search query (song name, artist, etc.)

    Returns:
        Tuple of (audio_url, metadata)
    """
    client = InvidiousClient()

    # Search for video
    results = client.search(query, limit=1)
    if not results:
        raise RuntimeError(f"No results found for: {query}")

    video_id = results[0].get("videoId")
    if not video_id:
        raise RuntimeError("No video ID in search results")

    # Get audio URL
    return client.get_audio_url(video_id)
