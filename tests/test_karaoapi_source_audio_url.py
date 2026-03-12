import types
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

try:
    from fastapi.testclient import TestClient
    from karaoapi import app as appmod
    _FASTAPI_AVAILABLE = True
except Exception:
    TestClient = None  # type: ignore[assignment]
    appmod = None  # type: ignore[assignment]
    _FASTAPI_AVAILABLE = False


@unittest.skipUnless(_FASTAPI_AVAILABLE, "fastapi test dependencies are not installed")
class SourceAudioUrlEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(appmod.app)
        with appmod._source_audio_url_cache_lock:
            appmod._source_audio_url_cache.clear()
        with appmod._source_search_result_meta_cache_lock:
            appmod._source_search_result_meta_cache.clear()
        with appmod._source_audio_url_singleflight_entries_lock:
            appmod._source_audio_url_singleflight_entries.clear()
        with appmod._source_audio_url_refresh_inflight_lock:
            appmod._source_audio_url_refresh_inflight.clear()
        self._orig_enable_source_audio = appmod.ENABLE_SOURCE_AUDIO_URL_ENDPOINT
        self._orig_server_download_only = appmod.SERVER_DOWNLOAD_ONLY_ENFORCED
        appmod.ENABLE_SOURCE_AUDIO_URL_ENDPOINT = True
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = False

    def tearDown(self) -> None:
        appmod.ENABLE_SOURCE_AUDIO_URL_ENDPOINT = self._orig_enable_source_audio
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = self._orig_server_download_only

    def test_source_audio_url_retries_with_rotating_proxy_then_succeeds(self) -> None:
        fake_results = [
            types.SimpleNamespace(returncode=1, stdout="", stderr="Sign in to confirm you're not a bot"),
            types.SimpleNamespace(returncode=0, stdout="https://audio.example/test.m4a\n", stderr=""),
            types.SimpleNamespace(returncode=0, stdout="Song Title\n123\nhttps://img.example/thumb.jpg\n", stderr=""),
        ]

        with (
            mock.patch("karaoapi.app.subprocess.run", side_effect=fake_results) as run_mock,
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", "http://proxy-default:80"),
            mock.patch("scripts.step1_fetch.YTDLP_RETRIES", "1"),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_UA", ""),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRA_HEADERS", []),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_JS_RUNTIMES", ""),
            mock.patch("scripts.step1_fetch.YTDLP_REMOTE_COMPONENTS", ""),
            mock.patch("scripts.step1_fetch._proxy_retry_budget", return_value=2),
            mock.patch("scripts.step1_fetch._current_proxy", side_effect=["http://proxy-1:80", "http://proxy-2:80"]),
            mock.patch("scripts.step1_fetch._collect_ytdlp_diagnostics", return_value="botcheck"),
            mock.patch("scripts.step1_fetch._should_rotate_proxy_on_error", return_value=True),
            mock.patch("scripts.step1_fetch._mark_proxy_failure") as mark_fail_mock,
            mock.patch("scripts.step1_fetch._mark_proxy_success") as mark_ok_mock,
            mock.patch("scripts.step1_fetch._rotate_proxy") as rotate_mock,
        ):
            resp = self.client.get("/source/audio-url", params={"q": "O_oirh7_inA"})

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["audio_url"], "https://audio.example/test.m4a")
        self.assertEqual(payload["title"], "Song Title")
        self.assertEqual(payload["duration"], 123)
        self.assertEqual(run_mock.call_count, 3)
        mark_fail_mock.assert_any_call("http://proxy-1:80", reason="source_audio_url_failed")
        mark_ok_mock.assert_any_call("http://proxy-2:80")
        rotate_mock.assert_called()

    def test_source_audio_url_returns_500_after_exhausting_attempts(self) -> None:
        fake_fail = types.SimpleNamespace(returncode=1, stdout="", stderr="HTTP Error 429: Too Many Requests")

        with (
            mock.patch("karaoapi.app.subprocess.run", side_effect=[fake_fail, fake_fail, fake_fail]) as run_mock,
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", "http://proxy-default:80"),
            mock.patch("scripts.step1_fetch.YTDLP_RETRIES", "1"),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_UA", ""),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRA_HEADERS", []),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_JS_RUNTIMES", ""),
            mock.patch("scripts.step1_fetch.YTDLP_REMOTE_COMPONENTS", ""),
            mock.patch("scripts.step1_fetch._proxy_retry_budget", return_value=3),
            mock.patch("scripts.step1_fetch._current_proxy", side_effect=["http://proxy-1:80", "http://proxy-2:80", "http://proxy-3:80"]),
            mock.patch("scripts.step1_fetch._collect_ytdlp_diagnostics", return_value="HTTP Error 429"),
            mock.patch("scripts.step1_fetch._should_rotate_proxy_on_error", return_value=True),
            mock.patch("scripts.step1_fetch._mark_proxy_failure"),
            mock.patch("scripts.step1_fetch._rotate_proxy"),
        ):
            resp = self.client.get("/source/audio-url", params={"q": "O_oirh7_inA"})

        self.assertEqual(resp.status_code, 500)
        self.assertIn("Could not resolve source audio URL", resp.json().get("detail", ""))
        self.assertEqual(run_mock.call_count, 3)

    def test_source_audio_url_query_cache_avoids_repeat_ytdlp_calls(self) -> None:
        fake_results = [
            types.SimpleNamespace(returncode=0, stdout="https://audio.example/cached.m4a\n", stderr=""),
            types.SimpleNamespace(returncode=0, stdout="Cached Song\n240\nhttps://img.example/cached.jpg\n", stderr=""),
        ]
        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 3600.0),
            mock.patch("karaoapi.app.subprocess.run", side_effect=fake_results) as run_mock,
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", ""),
            mock.patch("scripts.step1_fetch.YTDLP_RETRIES", "1"),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_UA", ""),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRA_HEADERS", []),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_JS_RUNTIMES", ""),
            mock.patch("scripts.step1_fetch.YTDLP_REMOTE_COMPONENTS", ""),
        ):
            first = self.client.get("/source/audio-url", params={"q": "the beatles let it be"})
            second = self.client.get("/source/audio-url", params={"q": "the beatles let it be"})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["audio_url"], "https://audio.example/cached.m4a")
        self.assertEqual(second.json()["audio_url"], "https://audio.example/cached.m4a")
        self.assertEqual(run_mock.call_count, 2)

    def test_source_audio_url_uses_distributed_cache_when_local_miss(self) -> None:
        payload = {
            "audio_url": "https://audio.example/distributed.m4a",
            "title": "Distributed Song",
            "duration": 201,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/distributed.jpg",
        }
        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 3600.0),
            mock.patch("karaoapi.app._distributed_cache_get_json", return_value=payload) as distributed_get,
            mock.patch("karaoapi.app.subprocess.run", side_effect=AssertionError("should not run subprocess")) as run_mock,
        ):
            resp = self.client.get("/source/audio-url", params={"q": "Rick Astley Never Gonna Give You Up"})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["audio_url"], payload["audio_url"])
        self.assertEqual(resp.json()["video_id"], payload["video_id"])
        self.assertEqual(run_mock.call_count, 0)
        self.assertGreaterEqual(distributed_get.call_count, 1)

    def test_distributed_cache_get_json_accepts_bytes_payload(self) -> None:
        payload = {
            "audio_url": "https://audio.example/distributed-bytes.m4a",
            "title": "Distributed Bytes Song",
            "duration": 188,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/distributed-bytes.jpg",
        }
        fake_client = mock.Mock()
        fake_client.get.return_value = (
            b'{"audio_url":"https://audio.example/distributed-bytes.m4a",'
            b'"title":"Distributed Bytes Song","duration":188,'
            b'"video_id":"dQw4w9WgXcQ","thumbnail":"https://img.example/distributed-bytes.jpg"}'
        )

        with mock.patch("karaoapi.app._distributed_cache_client_get", return_value=fake_client):
            got = appmod._distributed_cache_get_json("source_audio_url", "Rick Astley")

        self.assertEqual(got, payload)

    def test_distributed_cache_get_json_rejects_invalid_payload(self) -> None:
        fake_client = mock.Mock()
        fake_client.get.return_value = b"\xff\xfe\xfd"

        with mock.patch("karaoapi.app._distributed_cache_client_get", return_value=fake_client):
            got = appmod._distributed_cache_get_json("source_audio_url", "Rick Astley")

        self.assertIsNone(got)

    def test_distributed_singleflight_waits_for_remote_fill_before_fallback(self) -> None:
        payload = {
            "audio_url": "https://audio.example/remote-fill.m4a",
            "title": "Remote Filled Song",
            "duration": 222,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/remote-fill.jpg",
        }
        fake_client = mock.Mock()
        fake_client.set.return_value = False

        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED", True),
            mock.patch("karaoapi.app._distributed_cache_client_get", return_value=fake_client),
            mock.patch("karaoapi.app._source_audio_url_wait_for_fresh_cache", return_value=payload),
            mock.patch(
                "karaoapi.app._extract_source_audio_url_uncached",
                side_effect=AssertionError("should not compute uncached when wait succeeds"),
            ) as uncached_mock,
        ):
            got = appmod._extract_source_audio_url_with_optional_distributed_singleflight("Rick Astley")

        self.assertEqual(got, payload)
        self.assertEqual(uncached_mock.call_count, 0)

    def test_distributed_singleflight_falls_back_when_wait_times_out(self) -> None:
        payload = {
            "audio_url": "https://audio.example/fallback.m4a",
            "title": "Fallback Song",
            "duration": 199,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/fallback.jpg",
        }
        fake_client = mock.Mock()
        fake_client.set.return_value = False

        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_DISTRIBUTED_SINGLEFLIGHT_ENABLED", True),
            mock.patch("karaoapi.app._distributed_cache_client_get", return_value=fake_client),
            mock.patch("karaoapi.app._source_audio_url_wait_for_fresh_cache", return_value=None),
            mock.patch("karaoapi.app._extract_source_audio_url_uncached", return_value=payload) as uncached_mock,
        ):
            got = appmod._extract_source_audio_url_with_optional_distributed_singleflight("Rick Astley")

        self.assertEqual(got, payload)
        self.assertEqual(uncached_mock.call_count, 1)

    def test_source_audio_url_serves_stale_and_triggers_refresh(self) -> None:
        payload = {
            "audio_url": "https://audio.example/stale.m4a",
            "title": "Stale Song",
            "duration": 180,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/stale.jpg",
        }
        key = appmod._normalize_source_audio_cache_key("the beatles let it be")
        with appmod._source_audio_url_cache_lock:
            appmod._source_audio_url_cache[key] = (time.monotonic() - 5.0, dict(payload))

        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 1.0),
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC", 30.0),
            mock.patch("karaoapi.app._distributed_cache_get_json", return_value=None),
            mock.patch("karaoapi.app._refresh_source_audio_url_stale_cache_async") as refresh_mock,
            mock.patch(
                "karaoapi.app._extract_source_audio_url_uncached",
                side_effect=AssertionError("should not call uncached while stale value is served"),
            ),
        ):
            resp = self.client.get("/source/audio-url", params={"q": "the beatles let it be"})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["audio_url"], payload["audio_url"])
        refresh_mock.assert_called_once()

    def test_source_audio_url_cache_lookup_marks_stale_entries(self) -> None:
        payload = {
            "audio_url": "https://audio.example/stale-lookup.m4a",
            "title": "Stale Lookup Song",
            "duration": 175,
            "video_id": "dQw4w9WgXcQ",
            "thumbnail": "https://img.example/stale-lookup.jpg",
        }
        key = appmod._normalize_source_audio_cache_key("rick astley")
        with appmod._source_audio_url_cache_lock:
            appmod._source_audio_url_cache[key] = (time.monotonic() - 5.0, dict(payload))

        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 1.0),
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_STALE_WHILE_REVALIDATE_SEC", 30.0),
            mock.patch("karaoapi.app._distributed_cache_get_json", return_value=None),
        ):
            fresh, is_stale_fresh = appmod._source_audio_url_cache_lookup("rick astley", allow_stale=False)
            stale, is_stale = appmod._source_audio_url_cache_lookup("rick astley", allow_stale=True)

        self.assertIsNone(fresh)
        self.assertFalse(is_stale_fresh)
        self.assertEqual(stale, payload)
        self.assertTrue(is_stale)
    def test_source_audio_url_singleflight_avoids_duplicate_inflight_extracts(self) -> None:
        call_counts = {"audio_extract": 0, "metadata": 0}

        def _fake_run(cmd, capture_output=True, text=True, timeout=20):  # type: ignore[no-untyped-def]
            if "--get-url" in cmd:
                call_counts["audio_extract"] += 1
                time.sleep(0.15)
                return types.SimpleNamespace(returncode=0, stdout="https://audio.example/singleflight.m4a\n", stderr="")
            call_counts["metadata"] += 1
            return types.SimpleNamespace(
                returncode=0,
                stdout="Singleflight Song\n180\nhttps://img.example/singleflight.jpg\n",
                stderr="",
            )

        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 3600.0),
            mock.patch("karaoapi.app.subprocess.run", side_effect=_fake_run),
            mock.patch("karaoapi.app.yt_search_ids", return_value=["O_oirh7_inA"]),
            mock.patch("scripts.step1_fetch._cached_ids_for_slug", return_value=[]),
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", ""),
            mock.patch("scripts.step1_fetch.YTDLP_RETRIES", "1"),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_UA", ""),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRA_HEADERS", []),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_JS_RUNTIMES", ""),
            mock.patch("scripts.step1_fetch.YTDLP_REMOTE_COMPONENTS", ""),
        ):
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_a = pool.submit(appmod.get_source_audio_url, "the beatles let it be")
                fut_b = pool.submit(appmod.get_source_audio_url, "The Beatles   Let It Be")
                payload_a = fut_a.result(timeout=5)
                payload_b = fut_b.result(timeout=5)

        self.assertEqual(payload_a["audio_url"], "https://audio.example/singleflight.m4a")
        self.assertEqual(payload_b["audio_url"], "https://audio.example/singleflight.m4a")
        self.assertEqual(call_counts["audio_extract"], 1)
        self.assertEqual(call_counts["metadata"], 1)

    def test_source_search_results_metadata_cache_avoids_repeat_metadata_subprocess(self) -> None:
        fake_metadata = types.SimpleNamespace(
            returncode=0,
            stdout="Cached Song\n210\nhttps://img.example/cached.jpg\nCached Uploader\n",
            stderr="",
        )
        with (
            mock.patch("karaoapi.app.SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC", 3600.0),
            mock.patch("scripts.step1_fetch.yt_search_ids", return_value=["dQw4w9WgXcQ"]),
            mock.patch("karaoapi.app.subprocess.run", return_value=fake_metadata) as run_mock,
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", ""),
        ):
            first = self.client.get("/source/search-results", params={"q": "rick astley", "limit": 1})
            second = self.client.get("/source/search-results", params={"q": "rick astley", "limit": 1})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(run_mock.call_count, 1)
        payload = second.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["title"], "Cached Song")
        self.assertEqual(payload["results"][0]["uploader"], "Cached Uploader")

    def test_source_search_results_uses_distributed_metadata_cache_when_local_miss(self) -> None:
        def _distributed_get(namespace: str, key: str):  # type: ignore[no-untyped-def]
            if namespace == "source_search_meta" and key == "dQw4w9WgXcQ":
                return {
                    "title": "Distributed Metadata Song",
                    "duration": 222,
                    "thumbnail": "https://img.example/distributed-meta.jpg",
                    "uploader": "Distributed Uploader",
                }
            return None

        with (
            mock.patch("karaoapi.app.SOURCE_SEARCH_RESULT_META_CACHE_TTL_SEC", 3600.0),
            mock.patch("scripts.step1_fetch.yt_search_ids", return_value=["dQw4w9WgXcQ"]),
            mock.patch("karaoapi.app._distributed_cache_get_json", side_effect=_distributed_get),
            mock.patch("karaoapi.app.subprocess.run", side_effect=AssertionError("should not run metadata subprocess")) as run_mock,
        ):
            resp = self.client.get("/source/search-results", params={"q": "rick astley", "limit": 1})

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["title"], "Distributed Metadata Song")
        self.assertEqual(payload["results"][0]["uploader"], "Distributed Uploader")
        self.assertEqual(run_mock.call_count, 0)
    def test_source_audio_url_prefers_pinned_cached_id_before_search(self) -> None:
        fake_results = [
            types.SimpleNamespace(returncode=0, stdout="https://audio.example/pinned.m4a\n", stderr=""),
            types.SimpleNamespace(returncode=0, stdout="Pinned Song\n210\nhttps://img.example/pinned.jpg\n", stderr=""),
        ]
        with (
            mock.patch("karaoapi.app.SOURCE_AUDIO_URL_CACHE_TTL_SEC", 0.0),
            mock.patch("scripts.step1_fetch._cached_ids_for_slug", return_value=["CGj85pVzRJs"]),
            mock.patch("karaoapi.app.yt_search_ids", side_effect=AssertionError("should not call yt_search_ids")),
            mock.patch("karaoapi.app.subprocess.run", side_effect=fake_results),
            mock.patch("scripts.step1_fetch.YTDLP_CMD", ["yt-dlp"]),
            mock.patch("scripts.step1_fetch.YTDLP_PROXY", ""),
            mock.patch("scripts.step1_fetch.YTDLP_RETRIES", "1"),
            mock.patch("scripts.step1_fetch.YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch("scripts.step1_fetch.YTDLP_UA", ""),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRA_HEADERS", []),
            mock.patch("scripts.step1_fetch.YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch.YTDLP_JS_RUNTIMES", ""),
            mock.patch("scripts.step1_fetch.YTDLP_REMOTE_COMPONENTS", ""),
        ):
            resp = self.client.get("/source/audio-url", params={"q": "let it be"})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["video_id"], "CGj85pVzRJs")

    def test_source_audio_url_rejected_when_server_download_only_enabled(self) -> None:
        with mock.patch.object(appmod, "SERVER_DOWNLOAD_ONLY_ENFORCED", True):
            resp = self.client.get("/source/audio-url", params={"q": "let it be"})
        self.assertEqual(resp.status_code, 403)

    def test_source_normalize_returns_structured_artist_and_track(self) -> None:
        with mock.patch(
            "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
            return_value={
                "artist": "The Beatles",
                "track": "Let It Be",
                "normalized_query": "The Beatles - Let It Be",
                "display": "The Beatles - Let It Be",
                "confidence": "high",
                "provider": "yt_suggest_ytsearch1",
            },
        ):
            resp = self.client.get("/source/normalize", params={"q": "the beatles let it be"})

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["artist"], "The Beatles")
        self.assertEqual(payload["track"], "Let It Be")
        self.assertEqual(payload["normalized_query"], "The Beatles - Let It Be")

    def test_source_normalize_returns_422_when_resolution_fails(self) -> None:
        with mock.patch(
            "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
            return_value={
                "error": "Unable to confidently resolve artist and title.",
                "user_error": "Unable to identify song. Please include artist and title.",
                "provider": "yt_suggest",
            },
        ):
            resp = self.client.get("/source/normalize", params={"q": "asdf qwer zxcv"})

        self.assertEqual(resp.status_code, 422)
        self.assertIn("Unable to identify song", resp.json().get("detail", ""))

    def test_source_normalize_short_circuit_url_uses_video_metadata_hint(self) -> None:
        with (
            mock.patch(
                "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                return_value={
                    "provider": "normalization_short_circuit",
                    "short_circuit": "1",
                    "normalized_query": "https://youtu.be/CGj85pVzRJs",
                    "display": "https://youtu.be/CGj85pVzRJs",
                },
            ),
            mock.patch(
                "scripts.step1_fetch._direct_source_source_from_query",
                return_value=("CGj85pVzRJs", "https://youtu.be/CGj85pVzRJs"),
            ),
            mock.patch(
                "scripts.step1_fetch._yt_video_metadata_hint",
                return_value={"artist": "The Beatles", "title": "Let It Be"},
            ),
        ):
            resp = self.client.get("/source/normalize", params={"q": "https://youtu.be/CGj85pVzRJs"})

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["artist"], "The Beatles")
        self.assertEqual(payload["track"], "Let It Be")
        self.assertEqual(payload["normalized_query"], "The Beatles - Let It Be")

if __name__ == "__main__":
    unittest.main()
