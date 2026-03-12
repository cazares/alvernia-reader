import json
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

# Keep tests self-contained even when third-party deps are absent in the runner.
if "requests" not in sys.modules:
    fake_requests = types.ModuleType("requests")
    fake_requests.get = lambda *args, **kwargs: None  # type: ignore[assignment]
    sys.modules["requests"] = fake_requests

from scripts import step1_fetch as step1


class Step1HelperTests(unittest.TestCase):
    def test_env_bool_respects_default_and_truthy_values(self) -> None:
        with mock.patch.dict("os.environ", {}, clear=False):
            self.assertTrue(step1._env_bool("MISSING_FLAG", True))
            self.assertFalse(step1._env_bool("MISSING_FLAG", False))

        with mock.patch.dict("os.environ", {"UNIT_BOOL_FLAG": " YeS "}, clear=False):
            self.assertTrue(step1._env_bool("UNIT_BOOL_FLAG", False))

        with mock.patch.dict("os.environ", {"UNIT_BOOL_FLAG": "off"}, clear=False):
            self.assertFalse(step1._env_bool("UNIT_BOOL_FLAG", True))

    def test_parse_mp3_pinned_ids_parses_slug_and_filters_invalid_ids(self) -> None:
        got = step1._parse_mp3_pinned_ids(
            "The Beatles - Let It Be:CGj85pVzRJs|O_oirh7_inA|not_valid, bad_entry"
        )
        self.assertIn("the_beatles_let_it_be", got)
        self.assertEqual(got["the_beatles_let_it_be"], ("CGj85pVzRJs", "O_oirh7_inA"))

    def test_cached_ids_for_slug_prefers_pinned_before_meta(self) -> None:
        with (
            mock.patch.object(step1, "MP3_PINNED_IDS", {"the_beatles_let_it_be": ("CGj85pVzRJs",)}),
            mock.patch.object(step1, "_read_cached_id_from_slug_meta", return_value="O_oirh7_inA"),
        ):
            got = step1._cached_ids_for_slug("The Beatles - Let It Be")
        self.assertEqual(got, ["CGj85pVzRJs", "O_oirh7_inA"])

    def test_cached_ids_for_slug_uses_hot_alias_meta(self) -> None:
        def _meta_lookup(slug: str) -> str | None:
            if slug == "the_beatles_let_it_be":
                return "CGj85pVzRJs"
            return None

        with (
            mock.patch.object(step1, "MP3_PINNED_IDS", {}),
            mock.patch.object(step1, "_read_cached_id_from_slug_meta", side_effect=_meta_lookup),
        ):
            got = step1._cached_ids_for_slug("let it be")
        self.assertEqual(got, ["CGj85pVzRJs"])

    def test_read_cached_id_from_slug_meta_supports_legacy_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            slug = "legacy_song"
            meta_path = meta_dir / f"{slug}.step1.json"
            meta_path.write_text(
                json.dumps(
                    {
                        "source_id": "",
                        "youtube_id": "",
                        "youtube_picked": {"id": "A1b2C3d4E5F"},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(step1, "META_DIR", meta_dir):
                got = step1._read_cached_id_from_slug_meta(slug)
        self.assertEqual(got, "A1b2C3d4E5F")

    def test_parse_ytdlp_headers_filters_invalid_entries(self) -> None:
        got = step1._parse_ytdlp_headers("A:1|B: two|invalid|:x|C: ")
        self.assertEqual(got, ["A:1", "B:two"])

    def test_step1_fetch_cache_first_reuses_existing_artifacts_without_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            timings_dir = root / "timings"
            mp3_dir = root / "mp3s"
            meta_dir = root / "meta"
            timings_dir.mkdir(parents=True, exist_ok=True)
            mp3_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)

            slug = "red_hot_chili_peppers_right_on_time"
            (timings_dir / f"{slug}.lrc").write_text("[00:44.56]Right on time\n", encoding="utf-8")
            (mp3_dir / f"{slug}.mp3").write_bytes(b"mp3")
            (meta_dir / f"{slug}.step1.json").write_text(
                json.dumps(
                    {
                        "source_id": "abc123def45",
                        "lrc_fetch": {"provider": "step1_fallback_pseudo"},
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(step1, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "_should_refresh_cached_lyrics", return_value=True) as refresh_mock,
            ):
                info = step1.step1_fetch(
                    query="red hot chili peppers right on time",
                    slug=slug,
                    force=False,
                    reset=False,
                    nuke=False,
                    yt_search_n=1,
                    parallel=False,
                    cache_first=True,
                )

            self.assertTrue(info.get("reused"))
            refresh_mock.assert_not_called()

    def test_collect_ytdlp_diagnostics_prefers_focus_lines(self) -> None:
        stderr = "noise\nWARNING: one\nstill noise\nERROR: two\n"
        stdout = "other\n[debug] details\n"
        with mock.patch.object(step1, "YTDLP_DIAG_LINES", 2):
            got = step1._collect_ytdlp_diagnostics(stderr, stdout)
        self.assertEqual(got.splitlines(), ["ERROR: two", "[debug] details"])

    def test_clean_title_and_normalize_key(self) -> None:
        raw = 'Song Title (Live) [Official] feat. Guest “Mix”'
        cleaned = step1._clean_title(raw)
        self.assertEqual(cleaned, "Song Title")
        self.assertEqual(step1._normalize_key(cleaned), "song title")

    def test_maybe_split_artist_title(self) -> None:
        artist, title = step1._maybe_split_artist_title("Red Hot Chili Peppers - Californication")
        self.assertEqual(artist, "Red Hot Chili Peppers")
        self.assertEqual(title, "Californication")
        self.assertEqual(step1._maybe_split_artist_title("just words"), ("", ""))

    def test_is_live_like_title_detects_live_show_and_en_vivo(self) -> None:
        self.assertTrue(step1._is_live_like_title("Artist - Song (Live at Wembley)"))
        self.assertTrue(step1._is_live_like_title("Artist - Song - Show in NYC"))
        self.assertTrue(step1._is_live_like_title("Artista - Cancion en vivo"))

    def test_parse_timecode_supports_common_formats(self) -> None:
        self.assertAlmostEqual(step1._parse_timecode("01:02:03.45"), 3723.45, places=2)
        self.assertAlmostEqual(step1._parse_timecode("03:07,9"), 187.9, places=2)
        self.assertIsNone(step1._parse_timecode("nonsense"))

    def test_parse_sub_to_cues_and_cues_to_lrc(self) -> None:
        text = (
            "1\n"
            "00:00:01.000 --> 00:00:02.000\n"
            "<i>Hello</i>\n\n"
            "2\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "World\n\n"
        )
        cues = step1._parse_sub_to_cues(text)
        self.assertEqual(cues[0], (1.0, "Hello"))
        self.assertEqual(cues[1], (1.0, "World"))

        lrc = step1._cues_to_lrc(cues)
        lines = [ln for ln in lrc.splitlines() if ln.strip()]
        self.assertEqual(lines, ["[00:01.00]Hello", "[00:01.01]World"])

    def test_extract_text_lines_for_pseudo_lrc_filters_metadata_and_dedupes(self) -> None:
        raw = (
            "WEBVTT\n"
            "Kind: captions\n"
            "Language: en\n"
            "NOTE generated by YouTube\n"
            "00:00:01.000 --> 00:00:02.000\n"
            "Hello\n"
            "hello\n"
            "<i>World</i>\n"
        )
        got = step1._extract_text_lines_for_pseudo_lrc(raw)
        self.assertEqual(got, ["Hello", "World"])

    def test_extract_plain_lyrics_lines_skips_headers_but_keeps_repeats(self) -> None:
        raw = "[Verse 1]\nHello\nHello\n[Chorus]\nWorld\n"
        got = step1._extract_plain_lyrics_lines(raw)
        self.assertEqual(got, ["Hello", "Hello", "World"])

    def test_extract_lang_from_sub_path(self) -> None:
        self.assertEqual(step1._extract_lang_from_sub_path(Path("abc.en.vtt")), "en")
        self.assertEqual(step1._extract_lang_from_sub_path(Path("no_lang.vtt")), "")

    def test_pick_best_sub_file_prefers_requested_language(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p_en = Path(td) / "abc.en.vtt"
            p_es = Path(td) / "abc.es.vtt"
            sub_text = "00:00:01.000 --> 00:00:02.000\nhola mundo\n"
            p_en.write_text(sub_text, encoding="utf-8")
            p_es.write_text(sub_text, encoding="utf-8")

            best = step1._pick_best_sub_file([p_en, p_es], ("es", "en"))
            self.assertEqual(best, p_es)

    def test_build_mp3_query_variants_is_deduped_and_bounded(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ("audio", "lyrics")), mock.patch.object(
            step1, "MP3_MAX_QUERY_VARIANTS", 5
        ):
            variants = step1._build_mp3_query_variants("Artist - Song (Live)")
        self.assertTrue(variants)
        self.assertEqual(variants[0], "Artist - Song (Live)")
        self.assertEqual(len(variants), len(set(variants)))
        self.assertLessEqual(len(variants), 5)

    def test_canonicalize_hot_query_maps_let_it_be_variants(self) -> None:
        self.assertEqual(step1._canonicalize_hot_query("let it be"), "the beatles let it be")
        self.assertEqual(step1._canonicalize_hot_query("THE BEATLES LET IT BE"), "the beatles let it be")

    def test_canonicalize_hot_query_maps_validation_rescue_variants(self) -> None:
        self.assertEqual(step1._canonicalize_hot_query("nirvana on a plain"), "nirvana on a plain unplugged")
        self.assertEqual(step1._canonicalize_hot_query("shakira loca"), "shakira loca spanish version")
        self.assertEqual(step1._canonicalize_hot_query("shakira pies descalzos"), "shakira pies descalzos suenos blancos")
        self.assertEqual(step1._canonicalize_hot_query("grupo mazz estupido romantico"), "mazz estupido romantico")
        self.assertEqual(step1._canonicalize_hot_query("carlos y jose el arbolito"), "carlos y jose al pie de un arbol")
        self.assertEqual(
            step1._canonicalize_hot_query("carlos y jose mi casa nueva"),
            "los invasores de nuevo leon mi casa nueva",
        )

    def test_hot_query_artist_title_returns_expected_metadata(self) -> None:
        self.assertEqual(
            step1._hot_query_artist_title("let it be"),
            ("The Beatles", "Let It Be"),
        )
        self.assertEqual(
            step1._hot_query_artist_title("john frusciante the past recedes"),
            ("John Frusciante", "The Past Recedes"),
        )
        self.assertEqual(
            step1._hot_query_artist_title("grupo mazz estúpido romántico"),
            ("Mazz", "Estupido Romantico"),
        )
        self.assertEqual(step1._hot_query_artist_title("unknown song"), ("", ""))

    def test_resolve_canonical_artist_title_prefers_hot_query_mapping(self) -> None:
        with (
            mock.patch("scripts.step1_fetch._yt_oembed_video_hint", side_effect=AssertionError("should not call oembed")),
            mock.patch("scripts.step1_fetch._yt_video_metadata_hint", side_effect=AssertionError("should not call metadata hint")),
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", side_effect=AssertionError("should not call query hint")),
        ):
            artist, title = step1._resolve_canonical_artist_title(
                "let it be",
                {"artist": "Joan Baez", "title": "Let It Be"},
                "CGj85pVzRJs",
                prefer_query_hint=True,
            )
        self.assertEqual((artist, title), ("The Beatles", "Let It Be"))

    def test_build_mp3_query_variants_hot_query_is_short(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ("audio", "lyrics")):
            variants = step1._build_mp3_query_variants("let it be")
        self.assertGreaterEqual(len(variants), 1)
        self.assertLessEqual(len(variants), 2)
        self.assertEqual(variants[0], "the beatles let it be")

    def test_build_mp3_query_variants_hot_query_is_short_for_successful_songs(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ("audio", "lyrics")):
            for query in (
                "red hot chili peppers otherside",
                "red hot chili peppers californication",
                "red hot chili peppers under the bridge",
            ):
                variants = step1._build_mp3_query_variants(query)
                self.assertGreaterEqual(len(variants), 1)
                self.assertLessEqual(
                    len(variants),
                    2,
                    msg=f"expected hot-query bounded variants for {query}, got={variants}",
                )

    def test_build_mp3_query_variants_hot_query_is_short_for_additional_successful_songs(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ("audio", "lyrics")):
            for query in (
                "the eagles hotel california",
                "linkin park in the end",
                "nirvana smells like teen spirit",
            ):
                variants = step1._build_mp3_query_variants(query)
                self.assertGreaterEqual(len(variants), 1)
                self.assertLessEqual(
                    len(variants),
                    2,
                    msg=f"expected hot-query bounded variants for {query}, got={variants}",
                )

    def test_build_mp3_query_variants_recovers_english_trailing_typo(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ()), mock.patch.object(step1, "MP3_MAX_QUERY_VARIANTS", 10):
            variants = step1._build_mp3_query_variants("the beatles now and theb")
        self.assertIn("the beatles now and the", variants)
        self.assertIn("the beatles now and then", variants)

    def test_build_mp3_query_variants_recovers_spanish_trailing_typo(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ()), mock.patch.object(step1, "MP3_MAX_QUERY_VARIANTS", 10):
            variants = step1._build_mp3_query_variants("shakira y qu")
        self.assertIn("shakira y", variants)
        self.assertIn("shakira y que", variants)

    def test_build_mp3_search_queries_skips_redundant_lyrics_probe(self) -> None:
        with mock.patch.object(step1, "MP3_MAX_SEARCH_QUERY_VARIANTS", 2):
            base = step1._build_mp3_search_queries("Artist Song")
            hinted = step1._build_mp3_search_queries("Artist Song lyrics")
        self.assertEqual(base, ["Artist Song", '"Artist Song" lyrics'])
        self.assertEqual(hinted, ["Artist Song lyrics"])

    def test_yt_data_api_top_result_hint_parses_title_and_channel(self) -> None:
        fake_payload = {
            "items": [
                {
                    "id": {"videoId": "A1b2C3d4E5F"},
                    "snippet": {
                        "title": "The Past Recedes",
                        "channelTitle": "John Frusciante - Topic",
                    }
                }
            ]
        }
        with (
            mock.patch.object(step1, "source_DATA_API_KEY", "abc123"),
            mock.patch("scripts.step1_fetch.requests.get", return_value=types.SimpleNamespace(status_code=200, json=lambda: fake_payload)),
        ):
            hint = step1._yt_data_api_top_result_hint("johsdf frusciana the past recedds")
        self.assertEqual(hint.get("title"), "The Past Recedes")
        self.assertEqual(hint.get("artist"), "John Frusciante")
        self.assertEqual(hint.get("video_id"), "A1b2C3d4E5F")

    def test_yt_search_top_result_hint_returns_video_id(self) -> None:
        proc = types.SimpleNamespace(
            returncode=0,
            stdout="O_oirh7_inA\tDani California\tRed Hot Chili Peppers\tRed Hot Chili Peppers - Topic\n",
            stderr="",
        )
        with (
            mock.patch.object(step1, "source_DATA_API_KEY", ""),
            mock.patch("scripts.step1_fetch.subprocess.run", return_value=proc),
        ):
            hint = step1._yt_search_top_result_hint("red hot chili peppers dani california", timeout_sec=2.0)

        self.assertEqual(hint.get("title"), "Dani California")
        self.assertEqual(hint.get("artist"), "Red Hot Chili Peppers")
        self.assertEqual(hint.get("video_id"), "O_oirh7_inA")

    def test_yt_web_search_top_result_hint_prefers_non_live_official_candidate(self) -> None:
        payload = {
            "contents": {
                "twoColumnSearchResultsRenderer": {
                    "primaryContents": {
                        "sectionListRenderer": {
                            "contents": [
                                {
                                    "itemSectionRenderer": {
                                        "contents": [
                                            {
                                                "videoRenderer": {
                                                    "videoId": "LIVE1234567",
                                                    "title": {"runs": [{"text": "Shakira - Soltera (Live)"}]},
                                                    "ownerText": {"runs": [{"text": "Shakira"}]},
                                                }
                                            },
                                            {
                                                "videoRenderer": {
                                                    "videoId": "oBofuVYDoG4",
                                                    "title": {"runs": [{"text": "Shakira - Soltera (Official Video)"}]},
                                                    "ownerText": {"runs": [{"text": "Shakira"}]},
                                                }
                                            },
                                            {
                                                "videoRenderer": {
                                                    "videoId": "oC3pVeJraUo",
                                                    "title": {"runs": [{"text": "Shakira - Soltera (Official Lyric Video)"}]},
                                                    "ownerText": {"runs": [{"text": "Shakira"}]},
                                                }
                                            },
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        html = "<html><script>var ytInitialData = %s;</script></html>" % json.dumps(payload)
        with mock.patch(
            "scripts.step1_fetch.requests.get",
            return_value=types.SimpleNamespace(status_code=200, text=html),
        ):
            hint = step1._yt_web_search_top_result_hint("shakira soltera", timeout_sec=1.5)

        self.assertEqual(hint.get("video_id"), "oBofuVYDoG4")
        self.assertEqual(hint.get("artist"), "Shakira")
        self.assertEqual(hint.get("title"), "Soltera")

    def test_yt_web_search_top_result_hint_prefers_lyrics_candidate_when_query_requests_lyrics(self) -> None:
        payload = {
            "contents": {
                "twoColumnSearchResultsRenderer": {
                    "primaryContents": {
                        "sectionListRenderer": {
                            "contents": [
                                {
                                    "itemSectionRenderer": {
                                        "contents": [
                                            {
                                                "videoRenderer": {
                                                    "videoId": "YlUKcNNmywk",
                                                    "title": {"runs": [{"text": "Red Hot Chili Peppers - Californication [Official Music Video]"}]},
                                                    "ownerText": {"runs": [{"text": "RHCP"}]},
                                                }
                                            },
                                            {
                                                "videoRenderer": {
                                                    "videoId": "sqLWfFCbYBI",
                                                    "title": {"runs": [{"text": "Red Hot Chili Peppers - Californication (Lyrics)"}]},
                                                    "ownerText": {"runs": [{"text": "Lyrics World"}]},
                                                }
                                            },
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        html = "<html><script>var ytInitialData = %s;</script></html>" % json.dumps(payload)
        with mock.patch(
            "scripts.step1_fetch.requests.get",
            return_value=types.SimpleNamespace(status_code=200, text=html),
        ):
            hint = step1._yt_web_search_top_result_hint("red hot chili peppers californication (lyrics)", timeout_sec=1.5)

        self.assertEqual(hint.get("video_id"), "sqLWfFCbYBI")

    def test_prioritize_search_ids_for_query_uses_duration_hint_for_ambiguous_versions(self) -> None:
        ids = ["KewfYKJy8YU", "XAhTt60W7qo", "bdioIFdkLag"]
        titles = {
            "KewfYKJy8YU": "Shakira - Loca ft. Dizzee Rascal",
            "XAhTt60W7qo": "Shakira - Loca (Spanish Version) ft. El Cata",
            "bdioIFdkLag": "Shakira - Loca (Spanish Version) ft. El Cata (Audio)",
        }
        durations = {
            "KewfYKJy8YU": 205.0,
            "XAhTt60W7qo": 161.0,
            "bdioIFdkLag": 185.0,
        }
        views = {
            "KewfYKJy8YU": 100,
            "XAhTt60W7qo": 200,
            "bdioIFdkLag": 50,
        }

        with mock.patch("scripts.step1_fetch._rank_source_ids_for_query", side_effect=lambda ranked, _query: list(ranked)):
            got = step1._prioritize_search_ids_for_query(
                "shakira loca",
                ids,
                titles,
                duration_by_id=durations,
                view_count_by_id=views,
                target_duration_sec=180.53,
            )

        self.assertEqual(got[0], "bdioIFdkLag")

    def test_resolve_fast_query_source_uses_duration_ranked_ids_when_available(self) -> None:
        with (
            mock.patch("scripts.step1_fetch.yt_search_ids", return_value=["bdioIFdkLag"]) as ids_mock,
            mock.patch("scripts.step1_fetch._yt_oembed_video_hint", return_value={"title": "Loca", "artist": "Shakira"}),
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", side_effect=AssertionError("should not use plain top-result hint")),
        ):
            got = step1._resolve_fast_query_source(
                "Shakira - Loca",
                timeout_sec=2.0,
                target_duration_sec=180.53,
            )

        self.assertEqual(
            got,
            (
                "bdioIFdkLag",
                "https://www.youtube.com/watch?v=bdioIFdkLag",
                {"title": "Loca", "artist": "Shakira", "video_id": "bdioIFdkLag"},
            ),
        )
        ids_mock.assert_called_once_with(
            "Shakira - Loca",
            3,
            timeout_sec=2.0,
            target_duration_sec=180.53,
        )

    def test_resolve_fast_query_source_uses_web_hint_before_yt_dlp_for_simple_queries(self) -> None:
        with (
            mock.patch("scripts.step1_fetch._query_needs_duration_disambiguation", return_value=False),
            mock.patch(
                "scripts.step1_fetch._yt_web_search_top_result_hint",
                return_value={"title": "Soltera", "artist": "Shakira", "video_id": "oBofuVYDoG4"},
            ) as web_mock,
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", side_effect=AssertionError("should not use yt-dlp hint")),
        ):
            got = step1._resolve_fast_query_source(
                "shakira soltera",
                timeout_sec=2.0,
                target_duration_sec=None,
            )

        self.assertEqual(
            got,
            (
                "oBofuVYDoG4",
                "https://www.youtube.com/watch?v=oBofuVYDoG4",
                {"title": "Soltera", "artist": "Shakira", "video_id": "oBofuVYDoG4"},
            ),
        )
        web_mock.assert_called_once_with("shakira soltera", timeout_sec=1.5)

    def test_resolve_fast_query_source_skips_duration_disambiguation_for_explicit_lyrics_queries(self) -> None:
        with (
            mock.patch("scripts.step1_fetch._query_needs_duration_disambiguation", return_value=True),
            mock.patch("scripts.step1_fetch.yt_search_ids", side_effect=AssertionError("should skip duration-ranked search")),
            mock.patch(
                "scripts.step1_fetch._yt_web_search_top_result_hint",
                return_value={"title": "Californication (Lyrics)", "artist": "Red Hot Chili Peppers", "video_id": "sqLWfFCbYBI"},
            ) as web_mock,
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", side_effect=AssertionError("should not use yt-dlp hint")),
        ):
            got = step1._resolve_fast_query_source(
                "red hot chili peppers californication lyrics",
                timeout_sec=2.0,
                target_duration_sec=321.0,
            )

        self.assertEqual(
            got,
            (
                "sqLWfFCbYBI",
                "https://www.youtube.com/watch?v=sqLWfFCbYBI",
                {"title": "Californication (Lyrics)", "artist": "Red Hot Chili Peppers", "video_id": "sqLWfFCbYBI"},
            ),
        )
        web_mock.assert_called_once_with("red hot chili peppers californication lyrics", timeout_sec=1.5)

    def test_wait_for_local_lrc_target_duration_sec_reads_last_lyric_time(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lrc_path = Path(td) / "song.lrc"
            lrc_path.write_text(
                "[00:01.00]one\n"
                "[00:10.00]two\n"
                "[03:05.50]three\n",
                encoding="utf-8",
            )

            got = step1._wait_for_local_lrc_target_duration_sec(lrc_path, timeout_sec=0.01)

        self.assertAlmostEqual(float(got or 0.0), 185.5, places=2)

    def test_query_needs_duration_disambiguation_flags_short_titles(self) -> None:
        self.assertTrue(step1._query_needs_duration_disambiguation("Shakira - Loca"))
        self.assertTrue(step1._query_needs_duration_disambiguation("Beyonce - Halo"))
        self.assertFalse(step1._query_needs_duration_disambiguation("Red Hot Chili Peppers - Dani California"))
        self.assertFalse(step1._query_needs_duration_disambiguation("Ashe - Moral Of The Story"))

    def test_genius_search_top_hit_parses_primary_artist_and_title(self) -> None:
        fake_payload = {
            "response": {
                "hits": [
                    {
                        "result": {
                            "title": "Let It Be",
                            "primary_artist": {"name": "The Beatles"},
                        }
                    }
                ]
            }
        }
        with (
            mock.patch("scripts.step1_fetch._genius_access_token", return_value="abc123"),
            mock.patch.object(step1, "GENIUS_API_BASE", "https://api.genius.com"),
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="{}", json=lambda: fake_payload),
            ) as get_mock,
        ):
            hint = step1._genius_search_top_hit("the   beatles   let it be")
        self.assertEqual(hint.get("artist"), "The Beatles")
        self.assertEqual(hint.get("title"), "Let It Be")
        self.assertEqual(get_mock.call_args.kwargs["params"], {"q": "the beatles let it be"})
        self.assertEqual(get_mock.call_args.args[0], "https://api.genius.com/search")

    def test_normalize_query_via_ytsearch_top_result_short_circuits_urls(self) -> None:
        got = step1._normalize_query_via_ytsearch_top_result("https://www.youtube.com/watch?v=CGj85pVzRJs")
        self.assertEqual(got.get("provider"), "normalization_short_circuit")
        self.assertEqual(got.get("normalized_query"), "https://www.youtube.com/watch?v=CGj85pVzRJs")
        self.assertEqual(got.get("short_circuit"), "1")

    def test_normalize_query_via_ytsearch_top_result_short_circuits_video_id(self) -> None:
        got = step1._normalize_query_via_ytsearch_top_result("CGj85pVzRJs")
        self.assertEqual(got.get("provider"), "normalization_short_circuit")
        self.assertEqual(got.get("normalized_query"), "CGj85pVzRJs")
        self.assertEqual(got.get("short_circuit"), "1")

    def test_normalize_query_via_ytsearch_top_result_falls_back_to_ytsearch_when_suggest_empty(self) -> None:
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="[]", json=lambda: ["q", []]),
            ),
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", return_value={}),
            mock.patch(
                "scripts.step1_fetch._ytsearch1_dump_json_top_result",
                return_value={
                    "title": "John Frusciante - The Past Recedes",
                    "uploader": "John Frusciante - Topic",
                    "channel": "John Frusciante - Topic",
                    "video_id": "A1b2C3d4E5F",
                },
            ) as top_mock,
        ):
            got = step1._normalize_query_via_ytsearch_top_result("johnf fruadnsf the past recedes")
        self.assertEqual(got.get("provider"), "ytsearch1_query_fallback")
        self.assertEqual(got.get("artist"), "John Frusciante")
        self.assertEqual(got.get("track"), "The Past Recedes")
        self.assertEqual(got.get("normalized_query"), "John Frusciante - The Past Recedes")
        self.assertEqual(got.get("suggestion"), "johnf fruadnsf the past recedes")
        self.assertEqual(top_mock.call_args.args[0], "johnf fruadnsf the past recedes")

    def test_normalize_query_via_ytsearch_top_result_uses_explicit_split_when_suggest_empty(self) -> None:
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="[]", json=lambda: ["q", []]),
            ),
            mock.patch("scripts.step1_fetch._ytsearch1_dump_json_top_result") as top_mock,
        ):
            got = step1._normalize_query_via_ytsearch_top_result("John Frusciante - Wishing")
        self.assertEqual(got.get("provider"), "query_split_fallback")
        self.assertEqual(got.get("artist"), "John Frusciante")
        self.assertEqual(got.get("track"), "Wishing")
        self.assertEqual(got.get("normalized_query"), "John Frusciante - Wishing")
        top_mock.assert_not_called()

    def test_normalize_query_via_ytsearch_top_result_uses_filtered_suggestion(self) -> None:
        fake_suggest_payload = [
            "johnf fruadnsf the past recedes",
            [
                "john frusciante the past recedes lyrics",
                "john frusciante the past recedes",
            ],
        ]
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="{}", json=lambda: fake_suggest_payload),
            ),
            mock.patch("scripts.step1_fetch._yt_search_top_result_hint", return_value={}),
            mock.patch(
                "scripts.step1_fetch._ytsearch1_dump_json_top_result",
                return_value={
                    "title": "John Frusciante - The Past Recedes (Official Video)",
                    "uploader": "JohnFruscianteVEVO",
                    "channel": "John Frusciante - Topic",
                    "video_id": "A1b2C3d4E5F",
                },
            ) as top_mock,
        ):
            got = step1._normalize_query_via_ytsearch_top_result("johnf fruadnsf the past recedes")

        self.assertEqual(top_mock.call_args.args[0], "john frusciante the past recedes")
        self.assertEqual(got.get("artist"), "John Frusciante")
        self.assertEqual(got.get("track"), "The Past Recedes")
        self.assertEqual(got.get("display"), "John Frusciante - The Past Recedes")
        self.assertEqual(got.get("normalized_query"), "John Frusciante - The Past Recedes")
        self.assertEqual(got.get("provider"), "yt_suggest_ytsearch1")
        self.assertEqual(got.get("suggestion"), "john frusciante the past recedes")
        self.assertEqual(got.get("confidence"), "medium")

    def test_normalize_query_via_ytsearch_top_result_prefers_structured_track_artist(self) -> None:
        fake_suggest_payload = [
            "johnf fruadnsf the past recedes",
            ["john frusciante the past recedes"],
        ]
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="{}", json=lambda: fake_suggest_payload),
            ),
            mock.patch(
                "scripts.step1_fetch._ytsearch1_dump_json_top_result",
                return_value={
                    "title": "Some messy title",
                    "uploader": "RandomUploader",
                    "channel": "RandomUploader",
                    "video_id": "A1b2C3d4E5F",
                    "track": "The Past Recedes",
                    "artist": "John Frusciante",
                    "album_artist": "",
                    "artists": "",
                },
            ),
        ):
            got = step1._normalize_query_via_ytsearch_top_result("johnf fruadnsf the past recedes")
        self.assertEqual(got.get("artist"), "John Frusciante")
        self.assertEqual(got.get("track"), "The Past Recedes")
        self.assertEqual(got.get("confidence"), "high")

    def test_normalize_query_via_ytsearch_top_result_does_not_short_circuit_fast_hint_for_corrected_suggestions(self) -> None:
        fake_suggest_payload = [
            "johnf fruadnsf the past recedes",
            ["john frusciante the past recedes"],
        ]
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="{}", json=lambda: fake_suggest_payload),
            ),
            mock.patch(
                "scripts.step1_fetch._yt_search_top_result_hint",
                return_value={
                    "title": "The Past Recedes",
                    "artist": "John Frusciante",
                    "video_id": "A1b2C3d4E5F",
                },
            ),
            mock.patch(
                "scripts.step1_fetch._ytsearch1_dump_json_top_result",
                return_value={
                    "title": "Some messy title",
                    "uploader": "RandomUploader",
                    "channel": "RandomUploader",
                    "video_id": "A1b2C3d4E5F",
                    "track": "The Past Recedes",
                    "artist": "John Frusciante",
                    "album_artist": "",
                    "artists": "",
                },
            ) as dump_json_mock,
        ):
            got = step1._normalize_query_via_ytsearch_top_result("johnf fruadnsf the past recedes")

        self.assertEqual(got.get("artist"), "John Frusciante")
        self.assertEqual(got.get("track"), "The Past Recedes")
        self.assertEqual(got.get("confidence"), "high")
        dump_json_mock.assert_called_once_with("john frusciante the past recedes", timeout_sec=None)

    def test_normalize_query_via_ytsearch_top_result_prefers_fast_hint_before_dump_json(self) -> None:
        fake_suggest_payload = [
            "red hot chili peppers dani california",
            ["red hot chili peppers dani california"],
        ]
        with (
            mock.patch(
                "scripts.step1_fetch.requests.get",
                return_value=types.SimpleNamespace(status_code=200, text="{}", json=lambda: fake_suggest_payload),
            ),
            mock.patch(
                "scripts.step1_fetch._yt_search_top_result_hint",
                return_value={
                    "title": "Dani California",
                    "artist": "Red Hot Chili Peppers",
                    "video_id": "Sb5aq5HcS1A",
                },
            ) as fast_hint_mock,
            mock.patch("scripts.step1_fetch._ytsearch1_dump_json_top_result") as dump_json_mock,
        ):
            got = step1._normalize_query_via_ytsearch_top_result("red hot chili peppers dani california")

        self.assertEqual(got.get("artist"), "Red Hot Chili Peppers")
        self.assertEqual(got.get("track"), "Dani California")
        self.assertEqual(got.get("normalized_query"), "Red Hot Chili Peppers - Dani California")
        self.assertEqual(got.get("video_id"), "Sb5aq5HcS1A")
        self.assertEqual(got.get("provider"), "yt_suggest_ytsearch1")
        self.assertEqual(got.get("suggestion"), "red hot chili peppers dani california")
        fast_hint_mock.assert_called_once_with("red hot chili peppers dani california", timeout_sec=None)
        dump_json_mock.assert_not_called()

    def test_source_video_matches_expected_prefers_oembed_before_ytdlp(self) -> None:
        with (
            mock.patch(
                "scripts.step1_fetch._yt_oembed_video_hint",
                return_value={"title": "Dani California", "artist": "Red Hot Chili Peppers"},
            ) as oembed_mock,
            mock.patch(
                "scripts.step1_fetch._yt_video_metadata_hint",
                side_effect=AssertionError("yt-dlp metadata hint should not run when oEmbed succeeds"),
            ),
        ):
            matched = step1._source_video_matches_expected(
                "Sb5aq5HcS1A",
                expected_artist="Red Hot Chili Peppers",
                expected_title="Dani California",
            )

        self.assertTrue(matched)
        oembed_mock.assert_called_once()

    def test_build_mp3_query_variants_adds_artist_title_search_formats(self) -> None:
        with mock.patch.object(step1, "MP3_QUERY_SUFFIXES", ()), mock.patch.object(step1, "MP3_MAX_QUERY_VARIANTS", 8):
            variants = step1._build_mp3_query_variants("Queen - Bohemian Rhapsody")
        self.assertIn('"Queen" "Bohemian Rhapsody"', variants)
        self.assertIn("Queen +Bohemian Rhapsody", variants)

    def test_yt_search_ids_keeps_relevance_order_for_non_live_candidates(self) -> None:
        non_lyrics_id = "A1b2C3d4E5F"
        lyrics_id = "Z9y8X7w6V5U"
        fake_stdout = (
            f"{non_lyrics_id}\tMaluma - Felices los 4 (Official Video)\n"
            f"{lyrics_id}\tMaluma - Felices los 4 (Official Lyrics Video)\n"
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", True),
            mock.patch(
                "scripts.step1_fetch.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr=""),
            ),
        ):
            got = step1.yt_search_ids("maluma felices los 4", 8)

        self.assertEqual(got[0], non_lyrics_id)
        self.assertEqual(got[1], lyrics_id)

    def test_yt_search_ids_keeps_relevance_order_with_official_audio_candidates(self) -> None:
        official_audio_id = "OFF1CIA1A0D"
        lyrics_id = "LYR1CSV1DEO"
        fake_stdout = (
            f"{lyrics_id}\tArtist - Song (Official Lyrics Video)\n"
            f"{official_audio_id}\tArtist - Song (Official Audio)\n"
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", True),
            mock.patch.object(step1, "MP3_PREFER_OFFICIAL_AUDIO_VERSION", True),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch(
                "scripts.step1_fetch.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr=""),
            ),
        ):
            got = step1.yt_search_ids("artist song", 3)

        self.assertEqual(got[0], lyrics_id)
        self.assertEqual(got[1], official_audio_id)

    def test_yt_search_ids_keeps_relevance_order_for_lyrics_queries(self) -> None:
        official_audio_id = "OFF1CIA1A0D"
        lyrics_id = "LYR1CSV1DEO"
        fake_stdout = (
            f"{official_audio_id}\tArtist - Song (Official Audio)\n"
            f"{lyrics_id}\tArtist - Song (Official Lyrics Video)\n"
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", True),
            mock.patch.object(step1, "MP3_PREFER_OFFICIAL_AUDIO_VERSION", True),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch(
                "scripts.step1_fetch.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr=""),
            ),
        ):
            got = step1.yt_search_ids("artist song lyrics", 3)

        self.assertEqual(got[0], official_audio_id)
        self.assertEqual(got[1], lyrics_id)

    def test_yt_search_ids_prefers_non_live_titles_by_default(self) -> None:
        live_id = "ClivCUC9R_4"
        studio_id = "T1tLeStUd10"
        fake_stdout = (
            f"{live_id}\tRed Hot Chili Peppers - Subway To Venus (Live at Slane Castle)\n"
            f"{studio_id}\tRed Hot Chili Peppers - Subway To Venus (Official Audio)\n"
        )
        run_mock = mock.Mock(
            return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr="")
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", False),
            mock.patch.object(step1, "MP3_PREFER_NON_LIVE_VERSION", True),
            mock.patch.object(step1, "MP3_NON_LIVE_MIN_SEARCH_N", 3),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch("scripts.step1_fetch.subprocess.run", run_mock),
        ):
            got = step1.yt_search_ids("red hot chili peppers subway to venus", 1)

        self.assertEqual(got[0], studio_id)
        cmd = run_mock.call_args.args[0]
        self.assertIn("ytsearch3:red hot chili peppers subway to venus", cmd)

    def test_yt_search_ids_prefers_live_when_query_requests_live(self) -> None:
        live_id = "ClivCUC9R_4"
        studio_id = "T1tLeStUd10"
        fake_stdout = (
            f"{studio_id}\tRed Hot Chili Peppers - Subway To Venus (Official Audio)\n"
            f"{live_id}\tRed Hot Chili Peppers - Subway To Venus (Live at Slane Castle)\n"
        )
        run_mock = mock.Mock(
            return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr="")
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", False),
            mock.patch.object(step1, "MP3_PREFER_NON_LIVE_VERSION", True),
            mock.patch.object(step1, "MP3_NON_LIVE_MIN_SEARCH_N", 3),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch("scripts.step1_fetch.subprocess.run", run_mock),
        ):
            got = step1.yt_search_ids("red hot chili peppers subway to venus live", 1)

        self.assertEqual(got[0], live_id)
        cmd = run_mock.call_args.args[0]
        self.assertIn("ytsearch1:red hot chili peppers subway to venus live", cmd)

    def test_yt_search_ids_all_live_candidates_keep_relevance_order(self) -> None:
        live_id_lo = "ClivCUC9R_4"
        live_id_hi = "L1veVIEW5H1"
        fake_stdout = (
            f"{live_id_lo}\tSong Name (Live at Venue)\t200\t1000\n"
            f"{live_id_hi}\tSong Name (Live from Tour)\t201\t5000\n"
        )
        run_mock = mock.Mock(
            return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr="")
        )

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "MP3_PREFER_LYRICS_VERSION", False),
            mock.patch.object(step1, "MP3_PREFER_NON_LIVE_VERSION", True),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch("scripts.step1_fetch.subprocess.run", run_mock),
        ):
            got = step1.yt_search_ids("song name", 2)

        self.assertEqual(got[0], live_id_lo)

    def test_yt_search_ids_uses_process_cache_to_avoid_repeat_calls(self) -> None:
        fake_stdout = "A1b2C3d4E5F\tSample Song\n"
        run_mock = mock.Mock(return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr=""))

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 3600.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch("scripts.step1_fetch.subprocess.run", run_mock),
        ):
            first = step1.yt_search_ids("cached query demo", 5)
            second = step1.yt_search_ids("cached query demo", 5)

        self.assertEqual(first, second)
        self.assertEqual(run_mock.call_count, 1)

    def test_yt_search_ids_uses_disk_cache_to_avoid_repeat_calls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            disk_dir = Path(td) / "yt_cache"
            with (
                mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 0.0),
                mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 3600.0),
                mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_DIR", disk_dir),
                mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
                mock.patch("scripts.step1_fetch.subprocess.run") as run_mock,
            ):
                step1._yt_search_ids_disk_cache_set("cached query demo", 5, ["A1b2C3d4E5F"])
                got = step1.yt_search_ids("cached query demo", 5)

        self.assertEqual(got, ["A1b2C3d4E5F"])
        run_mock.assert_not_called()

    def test_yt_search_ids_singleflight_dedupes_parallel_searches(self) -> None:
        fake_stdout = "A1b2C3d4E5F\tSample Song\n"
        results: list[list[str]] = []
        errors: list[Exception] = []

        def fake_run(*args, **kwargs):  # type: ignore[no-untyped-def]
            del args, kwargs
            time.sleep(0.12)
            return types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr="")

        def worker() -> None:
            try:
                results.append(step1.yt_search_ids("parallel query", 5))
            except Exception as e:  # pragma: no cover - assertion handles this
                errors.append(e)

        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_SEARCH_SINGLEFLIGHT_ENABLED", True),
            mock.patch.object(step1, "YTDLP_SEARCH_CACHE_TTL_SEC", 3600.0),
            mock.patch.object(step1, "YTDLP_SEARCH_DISK_CACHE_TTL_SEC", 0.0),
            mock.patch.object(step1, "_YT_SEARCH_IDS_CACHE", {}),
            mock.patch.object(step1, "_YT_SEARCH_SINGLEFLIGHT_LOCKS", {}),
            mock.patch.object(step1, "_YT_SEARCH_SINGLEFLIGHT_REFS", {}),
            mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run) as run_mock,
        ):
            t1 = threading.Thread(target=worker)
            t2 = threading.Thread(target=worker)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], ["A1b2C3d4E5F"])
        self.assertEqual(results[1], ["A1b2C3d4E5F"])
        self.assertEqual(run_mock.call_count, 1)

    def test_proxy_pool_values_reads_pool_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxies.txt"
            pool_file.write_text(
                "http://user:pass@10.0.0.1:8080\n10.0.0.2:8080:user:pass\n",
                encoding="utf-8",
            )
            with (
                mock.patch.object(step1, "YTDLP_PROXY", ""),
                mock.patch.object(step1, "YTDLP_PROXY_POOL_RAW", ""),
                mock.patch.object(step1, "YTDLP_PROXY_POOL_URL", ""),
                mock.patch.object(step1, "YTDLP_PROXY_POOL_FILE", str(pool_file)),
                mock.patch.object(step1, "YTDLP_PROXY_POOL_MAX_ENTRIES", 10),
                mock.patch.object(step1, "_PROXY_POOL_CACHE", []),
                mock.patch.object(step1, "_PROXY_POOL_CACHE_AT_MONO", 0.0),
            ):
                got = step1._proxy_pool_values()

        self.assertEqual(len(got), 2)
        self.assertTrue(any("10.0.0.1:8080" in item for item in got))
        self.assertTrue(any("10.0.0.2:8080" in item for item in got))

    def test_current_proxy_random_policy_uses_ready_proxy_pool(self) -> None:
        with (
            mock.patch("scripts.step1_fetch._proxy_pool_values", return_value=["http://p1", "http://p2"]),
            mock.patch.object(step1, "YTDLP_PROXY_SELECTION_POLICY", "random"),
            mock.patch("scripts.step1_fetch.random.choices", return_value=[1]),
        ):
            step1._PROXY_STATE.clear()
            step1._PROXY_STATE["http://p1"] = {"consecutive_failures": 0, "cooldown_until": time.monotonic() + 60, "successes": 0}
            step1._PROXY_STATE["http://p2"] = {"consecutive_failures": 0, "cooldown_until": 0.0, "successes": 0}
            got = step1._current_proxy()
        self.assertEqual(got, "http://p2")

    def test_proxy_retry_budget_single_non_rotating_proxy_is_one(self) -> None:
        with (
            mock.patch("scripts.step1_fetch._proxy_pool_values", return_value=["http://proxy.local:8080"]),
            mock.patch.object(step1, "YTDLP_PROXY_MAX_ROTATIONS", 5),
            mock.patch.object(step1, "YTDLP_PROXY_PER_CALL_ATTEMPTS", 4),
            mock.patch.object(step1, "YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES", False),
        ):
            self.assertEqual(step1._proxy_retry_budget(), 1)

    def test_proxy_retry_budget_rotating_single_endpoint_uses_attempt_budget(self) -> None:
        with (
            mock.patch(
                "scripts.step1_fetch._proxy_pool_values",
                return_value=["http://fnwuvwpz-US-rotate:secret@p.webshare.io:80"],
            ),
            mock.patch.object(step1, "YTDLP_PROXY_MAX_ROTATIONS", 5),
            mock.patch.object(step1, "YTDLP_PROXY_PER_CALL_ATTEMPTS", 4),
            mock.patch.object(step1, "YTDLP_PROXY_SINGLE_ENDPOINT_ROTATES", True),
        ):
            self.assertEqual(step1._proxy_retry_budget(), 4)

    def test_yt_video_metadata_hint_parses_title_and_artist(self) -> None:
        fake_stdout = "Let It Be\tThe Beatles - Topic\tThe Beatles - Topic\n"
        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_UA", ""),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            mock.patch(
                "scripts.step1_fetch.subprocess.run",
                return_value=types.SimpleNamespace(returncode=0, stdout=fake_stdout, stderr=""),
            ),
        ):
            got = step1._yt_video_metadata_hint("CGj85pVzRJs")

        self.assertEqual(got.get("title"), "Let It Be")
        self.assertEqual(got.get("artist"), "The Beatles")

    def test_normalize_canonical_artist_title_handles_noisy_channel_and_stuttered_title(self) -> None:
        artist, title = step1._normalize_canonical_artist_title(
            "TheBeatlesVEVO",
            "The Beatles - The Beatles - Let It Be",
        )
        self.assertEqual(artist, "The Beatles")
        self.assertEqual(title, "Let It Be")

    def test_normalize_canonical_artist_title_prefers_split_artist_for_handle_like_uploader(self) -> None:
        artist, title = step1._normalize_canonical_artist_title(
            "twinkle8539",
            "John Frusciante - God with Lyrics",
        )
        self.assertEqual(artist, "John Frusciante")
        self.assertEqual(title, "God")

    def test_fallback_seed_lines_avoids_handle_like_artist_noise(self) -> None:
        lines = step1._fallback_seed_lines(
            "john frusciante god",
            hint_artist="twinkle8539",
            hint_title="John Frusciante - God with Lyrics",
        )
        self.assertTrue(lines)
        self.assertEqual(lines[0], "John Frusciante - God")

    def test_should_refresh_cached_lyrics_when_provider_is_pseudo(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta_path = root / "song.step1.json"
            lrc_path = root / "song.lrc"
            meta_path.write_text(
                json.dumps(
                    {
                        "lrc_fetch": {
                            "provider": "step1_fallback_pseudo",
                            "ok": True,
                        }
                    }
                ),
                encoding="utf-8",
            )
            lrc_path.write_text("[00:08.00]Lyrics unavailable\n", encoding="utf-8")
            with mock.patch.object(step1, "LRC_REUSE_REFRESH_ON_WEAK", True):
                self.assertTrue(step1._should_refresh_cached_lyrics(meta_path, lrc_path))

    def test_lyrics_metadata_mismatch_detects_title_disagreement(self) -> None:
        got = step1._lyrics_metadata_mismatch(
            {"provider": "lrclib_search", "artist": "Joan Baez", "title": "Let It Be"},
            canonical_artist="The Beatles",
            canonical_title="Let It Be",
        )
        self.assertTrue(got)

    def test_direct_source_source_from_query_supports_url_and_id(self) -> None:
        self.assertEqual(
            step1._direct_source_source_from_query("O_oirh7_inA"),
            ("O_oirh7_inA", "https://www.youtube.com/watch?v=O_oirh7_inA"),
        )
        self.assertEqual(
            step1._direct_source_source_from_query("https://www.youtube.com/watch?v=O_oirh7_inA"),
            ("O_oirh7_inA", "https://www.youtube.com/watch?v=O_oirh7_inA"),
        )
        self.assertEqual(
            step1._direct_source_source_from_query("https://youtu.be/O_oirh7_inA"),
            ("O_oirh7_inA", "https://youtu.be/O_oirh7_inA"),
        )
        self.assertEqual(
            step1._direct_source_source_from_query('"https://www.youtube.com/watch?v=O_oirh7_inA"'),
            ("O_oirh7_inA", "https://www.youtube.com/watch?v=O_oirh7_inA"),
        )
        self.assertEqual(
            step1._direct_source_source_from_query("watch this: https://youtu.be/O_oirh7_inA?t=12"),
            ("O_oirh7_inA", "https://youtu.be/O_oirh7_inA?t=12"),
        )
        self.assertIsNone(step1._direct_source_source_from_query("https://placeholder.invalid/watch?v=O_oirh7_inA"))
        self.assertIsNone(step1._direct_source_source_from_query("Artist - Song"))

    def test_ytdlp_fetch_subtitles_uses_direct_source_for_youtube_url(self) -> None:
        captured_cmd = {}

        def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
            captured_cmd["cmd"] = list(cmd)
            return types.SimpleNamespace(returncode=1, stdout="", stderr="subtitle failure")

        with (
            mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
        ):
            paths, _diag = step1._ytdlp_fetch_subtitles_to_temp(
                "check this https://www.youtube.com/watch?v=O_oirh7_inA",
                prefer_langs=("en",),
                auto=False,
            )

        self.assertEqual(paths, [])
        cmd = captured_cmd["cmd"]
        self.assertEqual(cmd[-1], "https://www.youtube.com/watch?v=O_oirh7_inA")
        self.assertFalse(any(str(part).startswith("ytsearch") for part in cmd))

    def test_ytdlp_fetch_subtitles_direct_uses_direct_timeout_and_fallback_client(self) -> None:
        calls = []

        def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
            calls.append((list(cmd), float(timeout)))
            if len(calls) == 1:
                raise step1.subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
            return types.SimpleNamespace(returncode=1, stdout="", stderr="subtitle failure")

        with (
            mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", False),
            mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
            mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", "youtube:player_client=android"),
            mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
            mock.patch.object(step1, "LRC_YT_CAPTIONS_TIMEOUT", 35.0),
            mock.patch.object(step1, "LRC_YT_CAPTIONS_DIRECT_TIMEOUT", 90.0),
        ):
            paths, diag = step1._ytdlp_fetch_subtitles_to_temp(
                "https://www.youtube.com/watch?v=O_oirh7_inA",
                prefer_langs=("en",),
                auto=True,
            )

        self.assertEqual(paths, [])
        self.assertIn("timed out after 90.0s", diag)
        self.assertGreaterEqual(len(calls), 2)
        self.assertEqual(calls[0][1], 90.0)
        self.assertFalse("--extractor-args" in calls[0][0])
        self.assertIn("--extractor-args", calls[1][0])
        self.assertEqual(calls[1][0][-1], "https://www.youtube.com/watch?v=O_oirh7_inA")

    def test_yt_download_direct_fast_uses_native_no_transcode_outputs_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")
            captured_cmd = {}

            def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
                captured_cmd["cmd"] = list(cmd)
                native.write_bytes(b"aac")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
                mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", True),
                mock.patch.object(step1, "STEP1_FAST_ALIAS_MP3", True),
                mock.patch.object(step1, "STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE_FORMAT", "bestaudio[acodec^=mp4a]/bestaudio"),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "MP3_DIRECT_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_UA", ""),
                mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
                mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
                mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
                mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            ):
                produced = step1._yt_download_direct_fast(
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    out,
                    source_label="O_oirh7_inA",
                    deadline_monotonic=time.monotonic() + 30.0,
                )

            self.assertEqual(produced, native)
            self.assertTrue(native.exists())
            self.assertTrue(out.exists())
            cmd = captured_cmd["cmd"]
            self.assertNotIn("-x", cmd)
            self.assertIn("-f", cmd)
            self.assertIn("bestaudio[vcodec=none]", cmd)

    def test_yt_download_direct_fast_falls_back_to_broader_format_when_audio_only_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")
            calls: list[list[str]] = []

            def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
                calls.append(list(cmd))
                if len(calls) == 1:
                    return types.SimpleNamespace(
                        returncode=1,
                        stdout="",
                        stderr="ERROR: Requested format is not available",
                    )
                native.write_bytes(b"aac")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
                mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", True),
                mock.patch.object(step1, "STEP1_FAST_ALIAS_MP3", True),
                mock.patch.object(step1, "STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE_FORMAT", "bestaudio[acodec^=mp4a]/18/best"),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "MP3_DIRECT_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_UA", ""),
                mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
                mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
                mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
                mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            ):
                produced = step1._yt_download_direct_fast(
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    out,
                    source_label="O_oirh7_inA",
                    deadline_monotonic=time.monotonic() + 30.0,
                )

            self.assertEqual(produced, native)
            self.assertEqual(len(calls), 2)
            self.assertIn("bestaudio[vcodec=none]", calls[0])
            self.assertIn("bestaudio[acodec^=mp4a]/18/best", calls[1])

    def test_yt_download_direct_fast_uses_default_client_for_strict_audio_only_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")
            calls: list[list[str]] = []

            def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
                calls.append(list(cmd))
                native.write_bytes(b"aac")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
                mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", True),
                mock.patch.object(step1, "STEP1_FAST_ALIAS_MP3", True),
                mock.patch.object(step1, "STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_AUDIO_ONLY_FORMAT", "bestaudio[vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE_FORMAT", "bestaudio[acodec^=mp4a]/18/best"),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "MP3_DIRECT_EXTRACTOR_ARGS", "youtube:player_client=android"),
                mock.patch.object(step1, "YTDLP_UA", ""),
                mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
                mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
                mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
                mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            ):
                produced = step1._yt_download_direct_fast(
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    out,
                    source_label="O_oirh7_inA",
                    deadline_monotonic=time.monotonic() + 30.0,
                )

            self.assertEqual(produced, native)
            self.assertEqual(len(calls), 1)
            self.assertNotIn("--extractor-args", calls[0])

    def test_yt_download_direct_fast_prefers_smaller_audio_only_rung_before_default_audio_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")
            calls: list[list[str]] = []

            def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
                calls.append(list(cmd))
                if len(calls) == 1:
                    return types.SimpleNamespace(
                        returncode=1,
                        stdout="",
                        stderr="ERROR: Requested format is not available",
                    )
                native.write_bytes(b"aac")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
                mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", True),
                mock.patch.object(step1, "STEP1_FAST_ALIAS_MP3", True),
                mock.patch.object(step1, "STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT", "bestaudio[abr<=96][vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_AUDIO_ONLY_FORMAT", "bestaudio[abr<=160][vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE_FORMAT", "bestaudio[acodec^=mp4a]/18/best"),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "MP3_DIRECT_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_UA", ""),
                mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
                mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
                mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
                mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            ):
                produced = step1._yt_download_direct_fast(
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    out,
                    source_label="O_oirh7_inA",
                    deadline_monotonic=time.monotonic() + 30.0,
                )

            self.assertEqual(produced, native)
            self.assertEqual(len(calls), 2)
            self.assertIn("bestaudio[abr<=96][vcodec=none]", calls[0])
            self.assertIn("bestaudio[abr<=160][vcodec=none]", calls[1])

    def test_yt_download_direct_fast_prefers_broad_format_for_lyrics_like_sources(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")
            calls: list[list[str]] = []

            def fake_run(cmd, capture_output, text, timeout):  # type: ignore[no-untyped-def]
                calls.append(list(cmd))
                native.write_bytes(b"aac")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                mock.patch("scripts.step1_fetch.subprocess.run", side_effect=fake_run),
                mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", True),
                mock.patch.object(step1, "STEP1_FAST_ALIAS_MP3", True),
                mock.patch.object(step1, "STEP1_FAST_PREFERRED_AUDIO_ONLY_FORMAT", "bestaudio[abr<=96][vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_AUDIO_ONLY_FORMAT", "bestaudio[abr<=160][vcodec=none]"),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE_FORMAT", "bestaudio[acodec^=mp4a]/18/best"),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "MP3_DIRECT_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_UA", ""),
                mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", []),
                mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
                mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
                mock.patch("scripts.step1_fetch._current_proxy", return_value=""),
            ):
                produced = step1._yt_download_direct_fast(
                    "https://www.youtube.com/watch?v=sqLWfFCbYBI",
                    out,
                    source_label="Red Hot Chili Peppers - Californication (Lyrics)",
                    deadline_monotonic=time.monotonic() + 30.0,
                    prefer_broad_format=True,
                )

            self.assertEqual(produced, native)
            self.assertEqual(len(calls), 1)
            self.assertIn("bestaudio[acodec^=mp4a]/18/best", calls[0])
            self.assertNotIn("bestaudio[abr<=96][vcodec=none]", calls[0])

    def test_is_sig_or_forbidden_or_transient(self) -> None:
        self.assertTrue(step1._is_sig_or_forbidden_or_transient("HTTP Error 403 Forbidden"))
        self.assertTrue(step1._is_sig_or_forbidden_or_transient("nsig extraction failed"))
        self.assertFalse(step1._is_sig_or_forbidden_or_transient("file not found"))

    def test_is_bot_or_cookie_gate_error_detects_botcheck(self) -> None:
        self.assertTrue(step1._is_bot_or_cookie_gate_error("Sign in to confirm you're not a bot"))
        self.assertTrue(step1._is_bot_or_cookie_gate_error("COOKIE_REFRESH_REQUIRED"))
        self.assertFalse(step1._is_bot_or_cookie_gate_error("download produced no file"))

    def test_ytdlp_base_common_builds_expected_flags(self) -> None:
        with (
            mock.patch.object(step1, "YTDLP_CMD", ["yt-dlp"]),
            mock.patch.object(step1, "YTDLP_AUDIO_QUALITY", "0"),
            mock.patch.object(step1, "YTDLP_SOCKET_TIMEOUT", "6"),
            mock.patch.object(step1, "YTDLP_RETRIES", "2"),
            mock.patch.object(step1, "YTDLP_FRAG_RETRIES", "3"),
            mock.patch.object(step1, "YTDLP_CONCURRENT_FRAGS", "4"),
            mock.patch.object(step1, "YTDLP_UA", "ua"),
            mock.patch.object(step1, "YTDLP_EXTRA_HEADERS", ["A:1"]),
            mock.patch.object(step1, "YTDLP_NO_WARNINGS", True),
            mock.patch.object(step1, "YTDLP_VERBOSE", True),
            mock.patch.object(step1, "YTDLP_JS_RUNTIMES", ""),
            mock.patch.object(step1, "YTDLP_REMOTE_COMPONENTS", ""),
            mock.patch("scripts.step1_fetch.shutil.which", return_value=None),
            mock.patch("scripts.step1_fetch._writable_cookies_path", return_value="/tmp/cookies.txt"),
        ):
            cmd = step1._yt_dlp_base_common(
                "out.%(ext)s",
                fmt="bestaudio",
                extractor_args="youtube:player_client=android",
                search_source=False,
                use_cookies=True,
                transcode_to_mp3=True,
            )
        self.assertIn("--cookies", cmd)
        self.assertIn("--no-playlist", cmd)
        self.assertIn("bestaudio", cmd)
        self.assertIn("--extractor-args", cmd)
        self.assertIn("-x", cmd)

    def test_yt_download_from_source_no_progress_timeout_is_configurable(self) -> None:
        class _FakeSelector:
            def __init__(self) -> None:
                self._map = {1: object()}

            def register(self, fileobj, events):  # type: ignore[no-untyped-def]
                del fileobj, events

            def unregister(self, fileobj):  # type: ignore[no-untyped-def]
                del fileobj

            def select(self, timeout=None):  # type: ignore[no-untyped-def]
                del timeout
                return []

            def get_map(self):  # type: ignore[no-untyped-def]
                return self._map

        class _FakeProc:
            def __init__(self) -> None:
                self.stdout = object()
                self.stderr = object()
                self.returncode = None

            def terminate(self) -> None:
                self.returncode = 1

            def wait(self, timeout=None):  # type: ignore[no-untyped-def]
                del timeout
                return 1

            def kill(self) -> None:
                self.returncode = 1

            def poll(self):  # type: ignore[no-untyped-def]
                return None

        now = {"t": 0.0}

        def fake_monotonic() -> float:
            now["t"] += 1.0
            return float(now["t"])

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            with (
                mock.patch.object(step1, "MP3_ENABLE_PARALLEL_STRATEGY_RACE", False),
                mock.patch.object(step1, "MP3_MAX_SOURCE_ATTEMPTS", 2),
                mock.patch.object(step1, "MP3_MAX_SOURCE_SECONDS", 90.0),
                mock.patch.object(step1, "YTDLP_CMD_TIMEOUT", 90.0),
                mock.patch.object(step1, "YTDLP_FORMAT", ""),
                mock.patch.object(step1, "YTDLP_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_FALLBACK_EXTRACTOR_ARGS", ""),
                mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
                mock.patch.object(step1, "MP3_PRIMARY_USE_COOKIES", False),
                mock.patch.object(step1, "YTDLP_NO_PROGRESS_TIMEOUT_SEC", 5.0),
                mock.patch.object(step1, "YTDLP_CAPTURE_LINES", 20),
                mock.patch("scripts.step1_fetch._yt_dlp_base_common", return_value=["yt-dlp"]),
                mock.patch("scripts.step1_fetch._proxy_from_cmd", return_value="http://proxy-a"),
                mock.patch("scripts.step1_fetch.selectors.DefaultSelector", return_value=_FakeSelector()),
                mock.patch("scripts.step1_fetch.subprocess.Popen", return_value=_FakeProc()),
                mock.patch("scripts.step1_fetch.time.monotonic", side_effect=fake_monotonic),
                mock.patch("scripts.step1_fetch._mark_proxy_failure") as mark_failure_mock,
                mock.patch("scripts.step1_fetch._rotate_proxy") as rotate_proxy_mock,
            ):
                with self.assertRaises(RuntimeError) as cm:
                    step1._yt_download_from_source(
                        "https://www.youtube.com/watch?v=O_oirh7_inA",
                        out,
                        source_label="O_oirh7_inA",
                    )

        self.assertIn("made no progress", str(cm.exception))
        mark_failure_mock.assert_any_call("http://proxy-a", reason="yt_download_no_progress")
        rotate_proxy_mock.assert_any_call("yt_download_no_progress")

    def test_dynamic_search_budget_tightens_under_low_remaining_time(self) -> None:
        with (
            mock.patch.object(step1, "MP3_ENABLE_DYNAMIC_SEARCH_BUDGET", True),
            mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 12),
            mock.patch.object(step1, "MP3_MAX_QUERY_VARIANTS", 10),
            mock.patch.object(step1, "MP3_MAX_SEARCH_QUERY_VARIANTS", 2),
            mock.patch.object(step1, "MP3_DYNAMIC_MID_REMAINING_SEC", 30.0),
            mock.patch.object(step1, "MP3_DYNAMIC_TIGHT_REMAINING_SEC", 12.0),
            mock.patch.object(step1, "MP3_DYNAMIC_MID_MAX_SEARCH_N", 2),
            mock.patch.object(step1, "MP3_DYNAMIC_TIGHT_MAX_SEARCH_N", 1),
        ):
            full = step1._dynamic_search_budget(remaining_sec=80.0, base_search_n=8)
            mid = step1._dynamic_search_budget(remaining_sec=20.0, base_search_n=8)
            tight = step1._dynamic_search_budget(remaining_sec=8.0, base_search_n=8)
        self.assertEqual(full["profile"], "full")
        self.assertEqual(mid["profile"], "mid")
        self.assertEqual(tight["profile"], "tight")
        self.assertLessEqual(int(mid["search_n"]), 2)
        self.assertEqual(int(tight["search_n"]), 1)

    def test_apply_hot_query_speed_budget_tightens_limits(self) -> None:
        with (
            mock.patch.object(step1, "MP3_HOT_QUERY_SPEED_MODE", True),
            mock.patch.object(step1, "MP3_HOT_QUERY_SPEED_SEARCH_N", 2),
            mock.patch.object(step1, "MP3_HOT_QUERY_SPEED_MAX_ID_ATTEMPTS", 2),
            mock.patch.object(step1, "MP3_HOT_QUERY_SPEED_MAX_QUERY_VARIANTS", 2),
            mock.patch.object(step1, "MP3_HOT_QUERY_SPEED_MAX_SEARCH_QUERY_VARIANTS", 1),
        ):
            tuned = step1._apply_hot_query_speed_budget(
                "let it be",
                {
                    "profile": "full",
                    "search_n": 8,
                    "id_attempt_limit": 6,
                    "variant_limit": 4,
                    "search_query_limit": 2,
                },
            )
        self.assertEqual(tuned["profile"], "hot-full")
        self.assertEqual(int(tuned["search_n"]), 2)
        self.assertEqual(int(tuned["id_attempt_limit"]), 2)
        self.assertEqual(int(tuned["variant_limit"]), 2)
        self.assertEqual(int(tuned["search_query_limit"]), 1)

    def test_writable_cookies_path_uses_source_when_missing(self) -> None:
        with mock.patch.object(step1, "_RUNTIME_COOKIES_PATH", None), mock.patch.object(
            step1, "YTDLP_COOKIES_PATH", "/tmp/nope-cookies.txt"
        ):
            got = step1._writable_cookies_path()
        self.assertEqual(got, "/tmp/nope-cookies.txt")


class Step1FlowTests(unittest.TestCase):
    def setUp(self) -> None:
        with step1._LRC_RESULT_CACHE_LOCK:
            step1._LRC_RESULT_CACHE.clear()

    def test_fetch_best_synced_lrc_prefers_get_fast_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("Artist", "Title")),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value={"syncedLyrics": "[00:00.00]Hi"}),
            ):
                info = step1.fetch_best_synced_lrc("Artist - Title", out, enable_source_fallback=False)
            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_get")
            self.assertEqual(out.read_text(encoding="utf-8"), "[00:00.00]Hi\n")

    def test_fetch_best_synced_lrc_can_use_plain_from_lrclib_get(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("A", "T")),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch(
                    "scripts.step1_fetch._lrclib_get_any",
                    return_value={"plainLyrics": "Line 1\nLine 2\nLine 3\n"},
                ),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=[]),
                mock.patch.object(step1, "LRC_ENABLE_PLAIN_LYRICS_FALLBACK", True),
                mock.patch.object(step1, "LRC_MIN_LINES", 2),
                mock.patch.object(step1, "LRC_PSEUDO_START_SECS", 0.0),
                mock.patch.object(step1, "LRC_PSEUDO_STEP_SECS", 1.0),
                mock.patch.object(step1, "LRC_PSEUDO_MAX_LINES", 10),
            ):
                info = step1.fetch_best_synced_lrc("A - T", out, enable_source_fallback=False)
            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_get_plain")
            self.assertIn("Line 1", out.read_text(encoding="utf-8"))

    def test_fetch_best_synced_lrc_uses_search_when_get_misses(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows = [{"artistName": "A", "trackName": "T", "syncedLyrics": "[00:00.00]Line"}]
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("A", "T")),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=rows),
            ):
                info = step1.fetch_best_synced_lrc("A - T", out, enable_source_fallback=False)
            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_search")
            self.assertEqual(out.read_text(encoding="utf-8"), "[00:00.00]Line\n")

    def test_fetch_best_synced_lrc_rejects_low_confidence_ambiguous_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows = [
                {
                    "artistName": "Pitbull",
                    "trackName": "Let the Good Times Roll",
                    "syncedLyrics": "[00:00.00]line\n[00:01.00]line2",
                }
            ]
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("", "")),
                mock.patch("scripts.step1_fetch._yt_search_top_result_hint", return_value={}),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=rows),
                mock.patch.object(step1, "LRC_LOW_CONFIDENCE_MIN_SCORE", 0.30),
                mock.patch.object(step1, "LRC_ENABLE_PLAIN_LYRICS_FALLBACK", False),
                mock.patch.object(step1, "LRC_ENABLE_YT_CAPTIONS_FALLBACK", False),
            ):
                info = step1.fetch_best_synced_lrc("let it be", out, enable_source_fallback=False)
            self.assertFalse(info["ok"])
            self.assertEqual(info["reason"], "no_synced_lyrics_found")
            self.assertFalse(out.exists())

    def test_fetch_best_synced_lrc_accepts_query_only_match_with_strong_token_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows = [
                {
                    "artistName": "The Beatles",
                    "trackName": "Let It Be",
                    "syncedLyrics": "[00:00.00]When I find myself in times of trouble",
                }
            ]
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("", "")),
                mock.patch("scripts.step1_fetch._yt_search_top_result_hint", return_value={}),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=rows),
                mock.patch.object(step1, "LRC_ENABLE_PLAIN_LYRICS_FALLBACK", False),
                mock.patch.object(step1, "LRC_ENABLE_YT_CAPTIONS_FALLBACK", False),
            ):
                info = step1.fetch_best_synced_lrc("the beatles let it be", out, enable_source_fallback=False)
            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_search")
            self.assertTrue(out.exists())

    def test_fetch_best_synced_lrc_uses_artist_fuzzy_fallback_for_truncated_title(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows_artist = [
                {"artistName": "John Frusciante", "trackName": "God", "syncedLyrics": "[00:00.00]God line"},
                {"artistName": "John Frusciante", "trackName": "Going Inside", "syncedLyrics": "[00:00.00]Other line"},
            ]

            def fake_search(params: dict[str, str]) -> list[dict[str, str]]:
                if params.get("artist_name") == "John Frusciante" and params.get("q") == "John Frusciante":
                    return rows_artist
                return []

            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("John Frusciante", "go")),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch("scripts.step1_fetch._lrclib_search", side_effect=fake_search),
            ):
                info = step1.fetch_best_synced_lrc("john frusciante - go", out, enable_source_fallback=False)
            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_search_artist_fuzzy")
            self.assertEqual(out.read_text(encoding="utf-8"), "[00:00.00]God line\n")

    def test_fetch_best_synced_lrc_uses_youtube_hint_for_heavy_typos(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows = [
                {
                    "artistName": "John Frusciante",
                    "trackName": "The Past Recedes",
                    "syncedLyrics": "[00:00.00]line",
                }
            ]

            def fake_search(params: dict[str, str]) -> list[dict[str, str]]:
                q = str(params.get("q") or "").lower()
                if "the past recedes" in q:
                    return rows
                return []

            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("", "")),
                mock.patch("scripts.step1_fetch._lrclib_search", side_effect=fake_search),
                mock.patch(
                    "scripts.step1_fetch._yt_search_top_result_hint",
                    return_value={"title": "The Past Recedes", "artist": "John Frusciante"},
                ) as hint_mock,
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
            ):
                info = step1.fetch_best_synced_lrc(
                    "johsdf frusciana the past recedds",
                    out,
                    enable_source_fallback=False,
                )
            self.assertTrue(info["ok"])
            self.assertIn(info["provider"], {"lrclib_search", "lrclib_search_source_hint"})
            hint_mock.assert_called()
            self.assertEqual(out.read_text(encoding="utf-8"), "[00:00.00]line\n")

    def test_fetch_best_synced_lrc_can_fallback_to_youtube_captions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("", "")),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=[]),
                mock.patch(
                    "scripts.step1_fetch._try_source_captions_lrc",
                    return_value={"provider": "source_captions", "mode": "manual"},
                ),
                mock.patch.object(step1, "LRC_ENABLE_YT_CAPTIONS_FALLBACK", True),
            ):
                info = step1.fetch_best_synced_lrc("query", out, enable_source_fallback=True)
        self.assertTrue(info["ok"])
        self.assertEqual(info["provider"], "source_captions")

    def test_try_source_captions_lrc_can_use_pseudo_timing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            sub = Path(td) / "abc.en.vtt"
            sub.write_text(
                "WEBVTT\n\nKind: captions\nLanguage: en\n\nhello\nworld\nhello\n",
                encoding="utf-8",
            )

            with (
                mock.patch("scripts.step1_fetch._ytdlp_fetch_subtitles_to_temp", return_value=([sub], "")),
                mock.patch("scripts.step1_fetch._pick_best_sub_file", return_value=sub),
                mock.patch.object(step1, "LRC_ENABLE_TEXT_PSEUDO_FALLBACK", True),
                mock.patch.object(step1, "LRC_MIN_LINES", 2),
                mock.patch.object(step1, "LRC_PSEUDO_START_SECS", 5.0),
                mock.patch.object(step1, "LRC_PSEUDO_STEP_SECS", 2.0),
                mock.patch.object(step1, "LRC_PSEUDO_MAX_LINES", 10),
            ):
                info = step1._try_source_captions_lrc("query", out, prefer_langs=("en",))

            self.assertIsNotNone(info)
            assert info is not None
            self.assertEqual(info["provider"], "source_captions")
            self.assertEqual(info["timing_mode"], "pseudo")
            self.assertEqual(out.read_text(encoding="utf-8"), "[00:05.00]hello\n[00:07.00]world\n")

    def test_fetch_best_synced_lrc_plain_lyrics_fallback_writes_pseudo_lrc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            rows = [{"artistName": "A", "trackName": "T", "plainLyrics": "[Verse]\nLine 1\nLine 2\nLine 2\n"}]
            with (
                mock.patch("scripts.step1_fetch._maybe_split_artist_title", return_value=("A", "T")),
                mock.patch("scripts.step1_fetch._lrclib_get", return_value=None),
                mock.patch("scripts.step1_fetch._lrclib_search", return_value=rows),
                mock.patch.object(step1, "LRC_ENABLE_PLAIN_LYRICS_FALLBACK", True),
                mock.patch.object(step1, "LRC_MIN_LINES", 2),
                mock.patch.object(step1, "LRC_PSEUDO_START_SECS", 0.0),
                mock.patch.object(step1, "LRC_PSEUDO_STEP_SECS", 1.0),
                mock.patch.object(step1, "LRC_PSEUDO_MAX_LINES", 10),
            ):
                info = step1.fetch_best_synced_lrc("A - T", out, enable_source_fallback=False)

            self.assertTrue(info["ok"])
            self.assertEqual(info["provider"], "lrclib_plain")
            self.assertEqual(
                out.read_text(encoding="utf-8"),
                "[00:00.00]Line 1\n[00:01.00]Line 2\n[00:02.00]Line 2\n",
            )

    def test_yt_download_mp3_uses_audio_disk_cache_before_ytdlp(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            out.write_bytes(b"cached-audio")
            with (
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=out) as cache_restore,
                mock.patch("scripts.step1_fetch._yt_download_from_source") as download_mock,
            ):
                got = step1.yt_download_mp3("O_oirh7_inA", out)

            self.assertEqual(got, out)
            cache_restore.assert_called_once()
            download_mock.assert_not_called()

    def test_yt_download_mp3_singleflight_dedupes_parallel_downloads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache_dir = root / "audio_cache"
            out_a = root / "a.mp3"
            out_b = root / "b.mp3"
            errors: list[Exception] = []
            results: list[Path] = []

            def fake_download(  # type: ignore[no-untyped-def]
                source: str,
                out_mp3: Path,
                *,
                source_label: str,
                deadline_monotonic=None,
            ) -> Path:
                del source, source_label, deadline_monotonic
                time.sleep(0.12)
                out_mp3.write_bytes(b"parallel-audio")
                return out_mp3

            def run_download(path: Path) -> None:
                try:
                    results.append(step1.yt_download_mp3("O_oirh7_inA", path))
                except Exception as e:  # pragma: no cover - assertion handles this
                    errors.append(e)

            with (
                mock.patch.object(step1, "YTDLP_AUDIO_SINGLEFLIGHT_ENABLED", True),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_ENABLED", True),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_DIR", cache_dir),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_TTL_SEC", 3600.0),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_MAX_ENTRIES", 100),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC", 0.0),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_MIN_BYTES", 1),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", False),
                mock.patch.object(step1, "_YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO", 0.0),
                mock.patch.object(step1, "_YT_AUDIO_SINGLEFLIGHT_LOCKS", {}),
                mock.patch.object(step1, "_YT_AUDIO_SINGLEFLIGHT_REFS", {}),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_download) as download_mock,
            ):
                t1 = threading.Thread(target=run_download, args=(out_a,))
                t2 = threading.Thread(target=run_download, args=(out_b,))
                t1.start()
                t2.start()
                t1.join()
                t2.join()

            self.assertFalse(errors)
            self.assertEqual(download_mock.call_count, 1)
            self.assertEqual(len(results), 2)
            self.assertTrue(out_a.exists())
            self.assertTrue(out_b.exists())
            self.assertEqual(out_a.read_bytes(), b"parallel-audio")
            self.assertEqual(out_b.read_bytes(), b"parallel-audio")

    def test_yt_download_mp3_singleflight_disabled_allows_parallel_downloads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_a = root / "a.mp3"
            out_b = root / "b.mp3"
            errors: list[Exception] = []

            def fake_download(  # type: ignore[no-untyped-def]
                source: str,
                out_mp3: Path,
                *,
                source_label: str,
                deadline_monotonic=None,
            ) -> Path:
                del source, source_label, deadline_monotonic
                time.sleep(0.05)
                out_mp3.write_bytes(b"parallel-audio")
                return out_mp3

            def run_download(path: Path) -> None:
                try:
                    step1.yt_download_mp3("O_oirh7_inA", path)
                except Exception as e:  # pragma: no cover - assertion handles this
                    errors.append(e)

            with (
                mock.patch.object(step1, "YTDLP_AUDIO_SINGLEFLIGHT_ENABLED", False),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_store"),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_download) as download_mock,
            ):
                t1 = threading.Thread(target=run_download, args=(out_a,))
                t2 = threading.Thread(target=run_download, args=(out_b,))
                t1.start()
                t2.start()
                t1.join()
                t2.join()

            self.assertFalse(errors)
            self.assertEqual(download_mock.call_count, 2)

    def test_yt_download_mp3_cools_source_id_after_auth_gate_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            with (
                mock.patch.object(step1, "YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED", True),
                mock.patch.object(step1, "YTDLP_SOURCE_FAIL_COOLDOWN_SEC", 600.0),
                mock.patch.object(step1, "YTDLP_SOURCE_FAIL_COOLDOWN_MAX_ENTRIES", 1000),
                mock.patch.object(step1, "_YTDLP_SOURCE_FAIL_UNTIL_EPOCH", {}),
                mock.patch.object(step1, "YTDLP_AUDIO_SINGLEFLIGHT_ENABLED", False),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch(
                    "scripts.step1_fetch._yt_download_from_source",
                    side_effect=RuntimeError("Sign in to confirm you're not a bot"),
                ) as download_mock,
            ):
                with self.assertRaises(RuntimeError):
                    step1.yt_download_mp3("O_oirh7_inA", out)
                with self.assertRaises(RuntimeError) as cm:
                    step1.yt_download_mp3("O_oirh7_inA", out)

            self.assertIn("cooling down", str(cm.exception).lower())
            self.assertEqual(download_mock.call_count, 1)

    def test_yt_download_mp3_can_bypass_source_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            now = time.time()

            def fake_download(  # type: ignore[no-untyped-def]
                source: str,
                target: Path,
                *,
                source_label: str,
                deadline_monotonic=None,
                hot_query=False,
            ) -> Path:
                del source, source_label, deadline_monotonic, hot_query
                target.write_bytes(b"audio-bytes")
                return target

            with (
                mock.patch.object(step1, "YTDLP_SOURCE_FAIL_COOLDOWN_ENABLED", True),
                mock.patch.object(step1, "_YTDLP_SOURCE_FAIL_UNTIL_EPOCH", {"O_oirh7_inA": now + 600.0}),
                mock.patch.object(step1, "YTDLP_AUDIO_SINGLEFLIGHT_ENABLED", False),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_download) as download_mock,
            ):
                got = step1.yt_download_mp3(
                    "O_oirh7_inA",
                    out,
                    bypass_source_fail_cooldown=True,
                )

            self.assertEqual(got, out)
            self.assertTrue(out.exists())
            self.assertGreater(out.stat().st_size, 0)
            self.assertEqual(download_mock.call_count, 1)

    def test_download_first_working_mp3_bypasses_cooldown_for_pinned_cached_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_download(video_id: str, target: Path, *, deadline_monotonic=None, bypass_source_fail_cooldown=False):  # type: ignore[no-untyped-def]
                del deadline_monotonic
                self.assertEqual(video_id, "O_oirh7_inA")
                self.assertTrue(bypass_source_fail_cooldown)
                target.write_bytes(b"fresh-audio")
                return target

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", True),
                mock.patch.object(step1, "MP3_PINNED_ID_SET", {"O_oirh7_inA"}),
                mock.patch.object(step1, "MP3_TOTAL_TIMEOUT_SEC", 30.0),
                mock.patch("scripts.step1_fetch._cached_ids_for_slug", return_value=["O_oirh7_inA"]),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=fake_download) as download_mock,
            ):
                vid, produced = step1.download_first_working_mp3("The Beatles - Let It Be", out, search_n=1)

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, out)
            self.assertEqual(download_mock.call_count, 1)

    def test_yt_audio_disk_cache_roundtrip_store_then_restore(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache_dir = root / "audio_cache"
            produced = root / "produced.mp3"
            produced.write_bytes(b"mp3-bytes")
            out = root / "target.mp3"

            with (
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_ENABLED", True),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_DIR", cache_dir),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_TTL_SEC", 3600.0),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_MAX_ENTRIES", 100),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_PRUNE_INTERVAL_SEC", 0.0),
                mock.patch.object(step1, "YTDLP_AUDIO_DISK_CACHE_MIN_BYTES", 1),
                mock.patch.object(step1, "STEP1_FAST_NO_TRANSCODE", False),
                mock.patch.object(step1, "_YT_AUDIO_DISK_CACHE_LAST_PRUNE_AT_MONO", 0.0),
            ):
                step1._yt_audio_disk_cache_store("O_oirh7_inA", produced)
                restored = step1._yt_audio_disk_cache_restore("O_oirh7_inA", out)

            self.assertEqual(restored, out)
            self.assertTrue(out.exists())
            self.assertEqual(out.read_bytes(), b"mp3-bytes")

    def test_try_download_source_direct_query_returns_tuple_from_cache_hit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            out.write_bytes(b"cached")
            with (
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=out),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast") as fast_mock,
                mock.patch("scripts.step1_fetch._yt_download_from_source") as resilient_mock,
            ):
                got = step1._try_download_source(
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    out,
                    search_n=3,
                    tried_ids=set(),
                )

            self.assertEqual(got, ("O_oirh7_inA", out))
            fast_mock.assert_not_called()
            resilient_mock.assert_not_called()

    def test_download_first_working_mp3_succeeds_when_youtube_download_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_try_download(  # type: ignore[no-untyped-def]
                qv,
                out_mp3,
                search_n,
                tried_ids,
                search_cache=None,
                id_attempt_limit=None,
                search_query_limit=None,
                deadline_monotonic=None,
            ):
                self.assertEqual(qv, "q1")
                out_mp3.write_bytes(b"mp3")
                return ("vid123", out_mp3)

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch("scripts.step1_fetch._build_mp3_query_variants", return_value=["q1"]),
                mock.patch("scripts.step1_fetch._try_download_source", side_effect=fake_try_download),
                mock.patch("scripts.step1_fetch._try_download_soundcloud", return_value=False),
            ):
                vid, produced = step1.download_first_working_mp3("query", out, search_n=3)
            self.assertEqual(vid, "vid123")
            self.assertEqual(produced, out)
            self.assertTrue(out.exists())

    def test_download_first_working_mp3_rejects_preexisting_stale_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            out.write_bytes(b"stale")
            stale_sig = (int(out.stat().st_size), int(out.stat().st_mtime_ns))

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch("scripts.step1_fetch._build_mp3_query_variants", return_value=["q1"]),
                mock.patch("scripts.step1_fetch._try_download_source", return_value=("vid123", out)),
                mock.patch("scripts.step1_fetch._try_download_soundcloud", return_value=None),
                self.assertRaises(RuntimeError) as ctx,
            ):
                step1.download_first_working_mp3("query", out, search_n=3)

            self.assertIn("no fresh audio", str(ctx.exception).lower())
            self.assertEqual((int(out.stat().st_size), int(out.stat().st_mtime_ns)), stale_sig)

    def test_download_first_working_mp3_retries_with_youtube_hint_when_no_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_try_download(  # type: ignore[no-untyped-def]
                qv,
                out_mp3,
                search_n,
                tried_ids,
                search_cache=None,
                id_attempt_limit=None,
                search_query_limit=None,
                deadline_monotonic=None,
            ):
                if qv == "bad query":
                    raise RuntimeError("yt search returned no ids")
                if qv == "John Frusciante The Past Recedes":
                    out_mp3.write_bytes(b"mp3")
                    return ("vidhint", out_mp3)
                raise RuntimeError("unexpected query variant")

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch("scripts.step1_fetch._build_mp3_query_variants", return_value=["bad query"]),
                mock.patch("scripts.step1_fetch._try_download_source", side_effect=fake_try_download),
                mock.patch("scripts.step1_fetch._try_download_soundcloud", return_value=None),
                mock.patch(
                    "scripts.step1_fetch._yt_search_top_result_hint",
                    return_value={"artist": "John Frusciante", "title": "The Past Recedes"},
                ),
            ):
                vid, produced = step1.download_first_working_mp3("johsdf frusciana the past recedds", out, search_n=3)

            self.assertEqual(vid, "vidhint")
            self.assertEqual(produced, out)
            self.assertTrue(out.exists())

    def test_try_download_source_passes_deadline_to_search_and_download(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            tried_ids: set[str] = set()
            observed: dict[str, float] = {}
            deadline = time.monotonic() + 2.0

            def fake_search(query, n, timeout_sec=None):  # type: ignore[no-untyped-def]
                self.assertIn(query, {'"q1" lyrics', "q1"})
                self.assertEqual(n, 3)
                self.assertIsNotNone(timeout_sec)
                observed["search_timeout"] = float(timeout_sec)
                return ["vid123"]

            def fake_download(video_id, out_mp3, deadline_monotonic=None):  # type: ignore[no-untyped-def]
                self.assertEqual(video_id, "vid123")
                observed["download_deadline"] = float(deadline_monotonic)
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ENABLE_ID_PREFETCH", True),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", False),
                mock.patch.object(step1, "MP3_MAX_SEARCH_QUERY_VARIANTS", 1),
                mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 1),
                mock.patch.object(step1, "YTDLP_SEARCH_TIMEOUT", 20.0),
                mock.patch("scripts.step1_fetch.yt_search_ids", side_effect=fake_search),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=fake_download),
            ):
                got = step1._try_download_source(
                    "q1",
                    out,
                    search_n=3,
                    tried_ids=tried_ids,
                    deadline_monotonic=deadline,
                )

            self.assertIsNotNone(got)
            self.assertEqual(got[0], "vid123")
            self.assertIn("search_timeout", observed)
            self.assertLessEqual(observed["search_timeout"], 2.0)
            self.assertIn("download_deadline", observed)
            self.assertEqual(observed["download_deadline"], deadline)

    def test_try_download_source_cookie_gate_does_not_exhaust_id_budget(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            tried_ids: set[str] = set()
            download_calls: list[str] = []

            def fake_download(video_id, out_mp3, deadline_monotonic=None):  # type: ignore[no-untyped-def]
                download_calls.append(video_id)
                if video_id == "vid_cookie":
                    raise RuntimeError("Sign in to confirm you're not a bot")
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ENABLE_ID_PREFETCH", True),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", False),
                mock.patch.object(step1, "MP3_MAX_SEARCH_QUERY_VARIANTS", 1),
                mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 1),
                mock.patch.object(step1, "MP3_FAIL_FAST_ON_BOT_GATE_NO_COOKIES", False),
                mock.patch("scripts.step1_fetch.yt_search_ids", return_value=["vid_cookie", "vid_ok"]),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=fake_download),
            ):
                got = step1._try_download_source(
                    "q1",
                    out,
                    search_n=3,
                    tried_ids=tried_ids,
                )

            self.assertIsNotNone(got)
            assert got is not None
            self.assertEqual(got[0], "vid_ok")
            self.assertEqual(download_calls, ["vid_cookie", "vid_ok"])

    def test_try_download_source_cookie_gate_fails_fast_without_cookies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_download(video_id, out_mp3, deadline_monotonic=None):  # type: ignore[no-untyped-def]
                raise RuntimeError("Sign in to confirm you're not a bot")

            with (
                mock.patch.object(step1, "MP3_ENABLE_ID_PREFETCH", True),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", False),
                mock.patch.object(step1, "MP3_MAX_SEARCH_QUERY_VARIANTS", 1),
                mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 3),
                mock.patch.object(step1, "MP3_FAIL_FAST_ON_BOT_GATE_NO_COOKIES", True),
                mock.patch.object(step1, "YTDLP_COOKIES_PATH", ""),
                mock.patch("scripts.step1_fetch.yt_search_ids", return_value=["vid_cookie", "vid_ok"]),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=fake_download),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    step1._try_download_source(
                        "q1",
                        out,
                        search_n=3,
                        tried_ids=set(),
                    )

            self.assertIn("yt id failed (vid_cookie)", str(ctx.exception))

    def test_download_first_working_mp3_times_out_when_budget_is_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            with (
                mock.patch("scripts.step1_fetch._build_mp3_query_variants", return_value=["q1", "q2"]),
                mock.patch.object(step1, "MP3_TOTAL_TIMEOUT_SEC", 0.0),
                self.assertRaises(RuntimeError) as ctx,
            ):
                step1.download_first_working_mp3("query", out, search_n=3)
        self.assertIn("timed out", str(ctx.exception).lower())

    def test_download_first_working_mp3_uses_direct_source_fast_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_direct_fast(
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
                prefer_broad_format=False,
            ):  # type: ignore[no-untyped-def]
                self.assertFalse(prefer_broad_format)
                self.assertEqual(source_label, "O_oirh7_inA")
                self.assertIsNotNone(deadline_monotonic)
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=fake_direct_fast) as fast_mock,
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search path should be skipped for direct URL"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "https://www.youtube.com/watch?v=O_oirh7_inA", out, search_n=3
                )

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, out)
            fast_mock.assert_called_once()

    def test_download_first_working_mp3_direct_fast_can_return_native_audio(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = out.with_suffix(".m4a")

            def fake_direct_fast(
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
                prefer_broad_format=False,
            ):  # type: ignore[no-untyped-def]
                del source, source_label, deadline_monotonic, hot_query, prefer_broad_format
                native.write_bytes(b"m4a")
                return native

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_store") as cache_store,
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=fake_direct_fast),
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search path should be skipped for direct URL"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "https://www.youtube.com/watch?v=O_oirh7_inA", out, search_n=3
                )

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, native)
            cache_store.assert_called_once_with("O_oirh7_inA", native)

    def test_download_first_working_mp3_direct_fast_falls_back_to_resilient(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_resilient(
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
            ):  # type: ignore[no-untyped-def]
                self.assertEqual(source_label, "O_oirh7_inA")
                self.assertIsNotNone(deadline_monotonic)
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=RuntimeError("boom")),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_resilient) as resilient_mock,
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search path should be skipped for direct URL"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "https://www.youtube.com/watch?v=O_oirh7_inA", out, search_n=3
                )

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, out)
            self.assertTrue(out.exists())
            resilient_mock.assert_called_once()

    def test_download_first_working_mp3_one_call_simple_mode_uses_single_ytsearch_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_from_source(  # type: ignore[no-untyped-def]
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
            ):
                del source_label, deadline_monotonic, hot_query
                self.assertEqual(source, "ytsearch1:primus jerry was a racecar driver")
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ONE_CALL_SIMPLE_MODE", True),
                mock.patch.object(step1, "MP3_ONE_CALL_SEARCH_N", 1),
                mock.patch.object(step1, "MP3_FAST_QUERY_RESOLVE_DIRECT_MODE", False),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_from_source) as source_mock,
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("query variant fallback path should not run in one-call mode"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "primus jerry was a racecar driver",
                    out,
                    search_n=3,
                    prefer_top_hit=True,
                )

            self.assertEqual(vid, "")
            self.assertEqual(produced, out)
            source_mock.assert_called_once()

    def test_download_first_working_mp3_fast_query_resolve_uses_direct_fast_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = Path(td) / "song.m4a"

            def fake_direct_fast(  # type: ignore[no-untyped-def]
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
                prefer_broad_format=False,
            ):
                self.assertEqual(source, "https://www.youtube.com/watch?v=O_oirh7_inA")
                self.assertEqual(source_label, "Red Hot Chili Peppers - Dani California")
                self.assertIsNotNone(deadline_monotonic)
                self.assertFalse(hot_query)
                self.assertFalse(prefer_broad_format)
                native.write_bytes(b"m4a")
                return native

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch.object(step1, "MP3_FAST_QUERY_RESOLVE_DIRECT_MODE", True),
                mock.patch.object(step1, "MP3_ONE_CALL_SIMPLE_MODE", True),
                mock.patch("scripts.step1_fetch._resolve_fast_query_source", return_value=(
                    "O_oirh7_inA",
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    {"artist": "Red Hot Chili Peppers", "title": "Dani California", "video_id": "O_oirh7_inA"},
                )),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=fake_direct_fast) as fast_mock,
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_store") as cache_store,
                mock.patch(
                    "scripts.step1_fetch._yt_download_from_source",
                    side_effect=AssertionError("one-call fallback should be skipped when fast query resolve succeeds"),
                ),
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search variants should not be used when fast query resolve succeeds"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "red hot chili peppers dani california",
                    out,
                    search_n=3,
                    prefer_top_hit=True,
                )

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, native)
            fast_mock.assert_called_once()
            cache_store.assert_called_once_with("O_oirh7_inA", native)

    def test_download_first_working_mp3_fast_query_resolve_prefers_broad_direct_format_for_lyrics_queries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            native = Path(td) / "song.m4a"

            def fake_direct_fast(  # type: ignore[no-untyped-def]
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
                prefer_broad_format=False,
            ):
                self.assertEqual(source, "https://www.youtube.com/watch?v=sqLWfFCbYBI")
                self.assertEqual(source_label, "Red Hot Chili Peppers - Californication")
                self.assertIsNotNone(deadline_monotonic)
                self.assertTrue(prefer_broad_format)
                native.write_bytes(b"m4a")
                return native

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch.object(step1, "MP3_FAST_QUERY_RESOLVE_DIRECT_MODE", True),
                mock.patch.object(step1, "MP3_ONE_CALL_SIMPLE_MODE", True),
                mock.patch("scripts.step1_fetch._resolve_fast_query_source", return_value=(
                    "sqLWfFCbYBI",
                    "https://www.youtube.com/watch?v=sqLWfFCbYBI",
                    {"artist": "Red Hot Chili Peppers", "title": "Californication", "video_id": "sqLWfFCbYBI"},
                )),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=fake_direct_fast) as fast_mock,
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_store") as cache_store,
                mock.patch(
                    "scripts.step1_fetch._yt_download_from_source",
                    side_effect=AssertionError("one-call fallback should be skipped when fast query resolve succeeds"),
                ),
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search variants should not be used when fast query resolve succeeds"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "red hot chili peppers californication (lyrics)",
                    out,
                    search_n=3,
                    prefer_top_hit=True,
                )

            self.assertEqual(vid, "sqLWfFCbYBI")
            self.assertEqual(produced, native)
            fast_mock.assert_called_once()
            cache_store.assert_called_once_with("sqLWfFCbYBI", native)

    def test_download_first_working_mp3_fast_query_resolve_falls_back_to_direct_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            def fake_resilient(  # type: ignore[no-untyped-def]
                source,
                out_mp3,
                *,
                source_label,
                deadline_monotonic=None,
                hot_query=False,
            ):
                self.assertEqual(source, "https://www.youtube.com/watch?v=O_oirh7_inA")
                self.assertEqual(source_label, "Red Hot Chili Peppers - Dani California")
                self.assertIsNotNone(deadline_monotonic)
                self.assertFalse(hot_query)
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_SOURCE_FASTPATH", True),
                mock.patch.object(step1, "MP3_FAST_QUERY_RESOLVE_DIRECT_MODE", True),
                mock.patch("scripts.step1_fetch._resolve_fast_query_source", return_value=(
                    "O_oirh7_inA",
                    "https://www.youtube.com/watch?v=O_oirh7_inA",
                    {"artist": "Red Hot Chili Peppers", "title": "Dani California", "video_id": "O_oirh7_inA"},
                )),
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_restore", return_value=None),
                mock.patch("scripts.step1_fetch._yt_download_direct_fast", side_effect=RuntimeError("boom")),
                mock.patch("scripts.step1_fetch._yt_download_from_source", side_effect=fake_resilient) as resilient_mock,
                mock.patch("scripts.step1_fetch._yt_audio_disk_cache_store") as cache_store,
                mock.patch(
                    "scripts.step1_fetch._build_mp3_query_variants",
                    side_effect=AssertionError("search variants should not run after direct fallback succeeds"),
                ),
            ):
                vid, produced = step1.download_first_working_mp3(
                    "red hot chili peppers dani california",
                    out,
                    search_n=3,
                    prefer_top_hit=True,
                )

            self.assertEqual(vid, "O_oirh7_inA")
            self.assertEqual(produced, out)
            resilient_mock.assert_called_once()
            cache_store.assert_called_once_with("O_oirh7_inA", out)

    def test_try_download_source_reuses_cached_search_results(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            cache: dict[str, list[str]] = {}
            search_mock = mock.Mock(return_value=["vid1"])

            with (
                mock.patch.object(step1, "MP3_ENABLE_ID_PREFETCH", True),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", False),
                mock.patch.object(step1, "MP3_PARALLEL_SEARCH_QUERIES", 1),
                mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 1),
                mock.patch("scripts.step1_fetch.yt_search_ids", search_mock),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=RuntimeError("fail")),
            ):
                with self.assertRaises(RuntimeError):
                    step1._try_download_source("query", out, search_n=3, tried_ids=set(), search_cache=cache)
                with self.assertRaises(RuntimeError):
                    step1._try_download_source("query", out, search_n=3, tried_ids=set(), search_cache=cache)

            self.assertEqual(search_mock.call_count, 1)

    def test_try_download_source_parallel_prefetches_first_search_variants(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"
            observed_queries: list[str] = []

            def fake_search(query, n, timeout_sec=None):  # type: ignore[no-untyped-def]
                observed_queries.append(query)
                if query == "q2":
                    time.sleep(0.05)
                return {"q1": ["vid1"], "q2": ["vid2"]}.get(query, [])

            def fake_download(video_id, out_mp3, deadline_monotonic=None):  # type: ignore[no-untyped-def]
                out_mp3.write_bytes(b"mp3")
                return out_mp3

            with (
                mock.patch("scripts.step1_fetch._build_mp3_search_queries", return_value=["q1", "q2"]),
                mock.patch.object(step1, "MP3_ENABLE_ID_PREFETCH", True),
                mock.patch.object(step1, "MP3_PARALLEL_SEARCH_QUERIES", 2),
                mock.patch.object(step1, "MP3_ENABLE_DIRECT_YTSEARCH_FALLBACK", False),
                mock.patch.object(step1, "MP3_MAX_ID_ATTEMPTS", 1),
                mock.patch("scripts.step1_fetch.yt_search_ids", side_effect=fake_search),
                mock.patch("scripts.step1_fetch.yt_download_mp3", side_effect=fake_download),
            ):
                got = step1._try_download_source("query", out, search_n=3, tried_ids=set(), search_cache={})

            self.assertIsNotNone(got)
            assert got is not None
            self.assertEqual(got[0], "vid1")
            self.assertEqual(set(observed_queries), {"q1", "q2"})

    def test_try_source_captions_lrc_parallel_starts_manual_and_auto(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.lrc"
            manual_sub = Path(td) / "manual.en.vtt"
            auto_sub = Path(td) / "auto.en.vtt"
            manual_sub.write_text("00:00:01.000 --> 00:00:02.000\nmanual\n", encoding="utf-8")
            auto_sub.write_text("00:00:01.000 --> 00:00:02.000\nauto\n", encoding="utf-8")

            observed_modes: list[bool] = []
            auto_started = threading.Event()

            def fake_fetch(query, prefer_langs, auto):  # type: ignore[no-untyped-def]
                observed_modes.append(bool(auto))
                if auto:
                    auto_started.set()
                    time.sleep(0.10)
                    return ([auto_sub], "")
                auto_started.wait(timeout=1.0)
                return ([manual_sub], "")

            with (
                mock.patch("scripts.step1_fetch._ytdlp_fetch_subtitles_to_temp", side_effect=fake_fetch),
                mock.patch("scripts.step1_fetch._pick_best_sub_file", side_effect=lambda paths, _langs: paths[0] if paths else None),
                mock.patch.object(step1, "LRC_PARALLEL_CAPTION_MODES", 2),
                mock.patch.object(step1, "LRC_MIN_LINES", 1),
            ):
                info = step1._try_source_captions_lrc("query", out, prefer_langs=("en",))

            self.assertIsNotNone(info)
            assert info is not None
            self.assertCountEqual(observed_modes, [False, True])
            self.assertEqual(info["provider"], "source_captions")
            self.assertIn(info["mode"], {"manual", "auto"})
            self.assertTrue(out.exists())

    def test_download_first_working_mp3_includes_youtube_failure_detail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "song.mp3"

            with (
                mock.patch.object(step1, "MP3_ENABLE_CACHED_ID_FASTPATH", False),
                mock.patch("scripts.step1_fetch._build_mp3_query_variants", return_value=["q1"]),
                mock.patch("scripts.step1_fetch._try_download_source", side_effect=RuntimeError("yt search returned no ids")),
                mock.patch("scripts.step1_fetch._try_download_soundcloud", return_value=None),
                self.assertRaises(RuntimeError) as ctx,
            ):
                step1.download_first_working_mp3("query", out, search_n=3)

            self.assertIn("yt search returned no ids", str(ctx.exception).lower())

    def test_step1_fetch_reuses_existing_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)
            slug = "song_slug"
            (mp3_dir / f"{slug}.mp3").write_bytes(b"x")
            (lrc_dir / f"{slug}.lrc").write_text("[00:00.00]x\n", encoding="utf-8")
            (meta_dir / f"{slug}.step1.json").write_text("{}", encoding="utf-8")

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Artist",
                        "track": "Song",
                        "title": "Song",
                        "display": "Artist - Song",
                        "normalized_query": "Artist - Song",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=AssertionError("should not fetch lrc")),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=AssertionError("should not fetch mp3")),
            ):
                info = step1.step1_fetch(
                    query="Song",
                    slug=slug,
                    force=False,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )
        self.assertTrue(info["reused"])

    def test_step1_fetch_hydrates_hot_alias_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            alias_slug = "the_beatles_let_it_be"
            target_slug = "let_it_be"
            (mp3_dir / f"{alias_slug}.mp3").write_bytes(b"alias-audio")
            (lrc_dir / f"{alias_slug}.lrc").write_text(
                "\n".join([f"[00:{i:02d}.00]line{i}" for i in range(6)]) + "\n",
                encoding="utf-8",
            )
            (meta_dir / f"{alias_slug}.step1.json").write_text(
                json.dumps(
                    {
                        "slug": alias_slug,
                        "query": "the beatles let it be",
                        "source_id": "A1b2C3d4E5F",
                        "lrc_fetch": {"ok": True, "provider": "lrclib_get"},
                    }
                ),
                encoding="utf-8",
            )

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "LRC_REUSE_REFRESH_ON_WEAK", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "The Beatles",
                        "track": "Let It Be",
                        "title": "Let It Be",
                        "display": "The Beatles - Let It Be",
                        "normalized_query": "The Beatles - Let It Be",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=AssertionError("should not fetch lrc")),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=AssertionError("should not fetch mp3")),
            ):
                info = step1.step1_fetch(
                    query="let it be",
                    slug=target_slug,
                    force=False,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertTrue(info["reused"])
            self.assertTrue((lrc_dir / f"{target_slug}.lrc").exists())
            self.assertTrue((meta_dir / f"{target_slug}.step1.json").exists())
            self.assertTrue((mp3_dir / f"{target_slug}.mp3").exists())

    def test_step1_fetch_sequential_writes_meta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                self.assertEqual(query, "Artist - Title")
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                self.assertEqual(search_n, 9)
                out_path.write_bytes(b"mp3")
                return ("vid999", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Artist",
                        "track": "Title",
                        "title": "Title",
                        "display": "Artist - Title",
                        "normalized_query": "Artist - Title",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="Artist - Title",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=9,
                    parallel=False,
                )

            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertFalse(info["reused"])
            self.assertEqual(info["source_id"], "vid999")
            self.assertEqual(meta["lrc_fetch"]["provider"], "unit")
            self.assertEqual(meta["source_id"], "vid999")
            self.assertEqual(meta["audio_path"], str(mp3_dir / f"{slug}.mp3"))

    def test_step1_fetch_uses_strict_normalized_query_for_lrc_and_mp3(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "The Beatles", "title": "Let It Be"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del search_n
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("CGj85pVzRJs", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "The Beatles",
                        "track": "Let It Be",
                        "title": "Let It Be",
                        "display": "The Beatles - Let It Be",
                        "normalized_query": "The Beatles - Let It Be",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="the beatles let. it be",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertEqual(observed.get("lrc_query"), "The Beatles - Let It Be")
            self.assertEqual(observed.get("mp3_query"), "The Beatles - Let It Be")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lookup_query"], "The Beatles - Let It Be")
            self.assertEqual(meta["lookup_query_source"], "yt_suggest_ytsearch1")
            self.assertEqual(meta["query_sanitized"], "the beatles let. it be")
            self.assertEqual(meta["query_normalization"]["artist"], "The Beatles")
            self.assertEqual(meta["query_normalization"]["track"], "Let It Be")
            self.assertEqual(meta["query_normalization"]["confidence"], "high")

    def test_step1_fetch_uses_lrc_metadata_for_audio_query(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {
                    "ok": True,
                    "provider": "lrclib_get",
                    "artist": "Selena Y Los Dinos",
                    "title": "Como La Flor",
                }

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del search_n
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("CGj85pVzRJs", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STEP1_USE_LRC_AUDIO_QUERY", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Selena",
                        "track": "Como La Flor",
                        "title": "Como La Flor",
                        "display": "Selena - Como La Flor",
                        "normalized_query": "Selena - Como La Flor",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._source_video_matches_expected", return_value=True),
            ):
                info = step1.step1_fetch(
                    query="selena como la flor",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertEqual(observed.get("lrc_query"), "Selena - Como La Flor")
            self.assertEqual(observed.get("mp3_query"), "Selena Y Los Dinos - Como La Flor")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["audio_lookup_query"], "Selena Y Los Dinos - Como La Flor")
            self.assertEqual(meta["audio_lookup_query_source"], "lrc_metadata")

    def test_step1_fetch_reuses_normalized_video_id_for_audio_query(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {
                    "ok": True,
                    "provider": "lrclib_get",
                    "artist": "Red Hot Chili Peppers",
                    "title": "Dani California",
                }

            def fake_download(query, out_path, search_n, retry_attempt=3, expected_duration_sec=None, prefer_top_hit=False):  # type: ignore[no-untyped-def]
                del search_n, retry_attempt, expected_duration_sec, prefer_top_hit
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("Sb5aq5HcS1A", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STEP1_USE_LRC_AUDIO_QUERY", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Red Hot Chili Peppers",
                        "track": "Dani California",
                        "title": "Dani California",
                        "display": "Red Hot Chili Peppers - Dani California",
                        "normalized_query": "Red Hot Chili Peppers - Dani California",
                        "confidence": "high",
                        "video_id": "Sb5aq5HcS1A",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._source_video_matches_expected", return_value=True),
            ):
                info = step1.step1_fetch(
                    query="red hot chili peppers dani california",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertEqual(observed.get("lrc_query"), "Red Hot Chili Peppers - Dani California")
            self.assertEqual(observed.get("mp3_query"), "Sb5aq5HcS1A")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["audio_lookup_query"], "Red Hot Chili Peppers - Dani California")
            self.assertEqual(meta["audio_lookup_query_source"], "query_normalization_video_id")

    def test_step1_fetch_preserves_explicit_lyrics_intent_for_audio_query(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {
                    "ok": True,
                    "provider": "lrclib_get",
                    "artist": "Red Hot Chili Peppers",
                    "title": "Californication",
                }

            def fake_download(query, out_path, search_n, retry_attempt=3, expected_duration_sec=None, prefer_top_hit=False):  # type: ignore[no-untyped-def]
                del search_n, retry_attempt, expected_duration_sec, prefer_top_hit
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("sqLWfFCbYBI", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STEP1_USE_LRC_AUDIO_QUERY", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Red Hot Chili Peppers",
                        "track": "Californication",
                        "title": "Californication",
                        "display": "Red Hot Chili Peppers - Californication",
                        "normalized_query": "Red Hot Chili Peppers - Californication",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._source_video_matches_expected", return_value=True),
            ):
                info = step1.step1_fetch(
                    query="red hot chili peppers californication (lyrics)",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertEqual(observed.get("lrc_query"), "Red Hot Chili Peppers - Californication")
            self.assertEqual(observed.get("mp3_query"), "red hot chili peppers californication (lyrics)")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lookup_query"], "Red Hot Chili Peppers - Californication")
            self.assertEqual(meta["audio_lookup_query"], "red hot chili peppers californication (lyrics)")
            self.assertEqual(meta["audio_lookup_query_source"], "user_query_explicit_audio_intent")

    def test_step1_fetch_disables_duration_aware_matching_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, object] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del query, prefer_langs, enable_source_fallback
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Shakira", "title": "Loca"}

            def fake_download(query, out_path, search_n, retry_attempt=3, expected_duration_sec=None, prefer_top_hit=False):  # type: ignore[no-untyped-def]
                del query, search_n, retry_attempt, prefer_top_hit
                observed["expected_duration_sec"] = expected_duration_sec
                out_path.write_bytes(b"mp3")
                return ("bdioIFdkLag", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STEP1_USE_LRC_AUDIO_QUERY", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Shakira",
                        "track": "Loca",
                        "title": "Loca",
                        "display": "Shakira - Loca",
                        "normalized_query": "Shakira - Loca",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._wait_for_local_lrc_target_duration_sec", side_effect=AssertionError("should not probe local duration")),
                mock.patch("scripts.step1_fetch._guess_lrc_target_duration_sec", side_effect=AssertionError("should not guess duration")),
                mock.patch("scripts.step1_fetch._source_video_matches_expected", return_value=True),
            ):
                step1.step1_fetch(
                    query="shakira loca",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertIsNone(observed.get("expected_duration_sec"))

    def test_step1_fetch_can_enable_duration_aware_matching(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, object] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del query, prefer_langs, enable_source_fallback
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Shakira", "title": "Loca"}

            def fake_download(query, out_path, search_n, retry_attempt=3, expected_duration_sec=None, prefer_top_hit=False):  # type: ignore[no-untyped-def]
                del query, search_n, retry_attempt, prefer_top_hit
                observed["expected_duration_sec"] = expected_duration_sec
                out_path.write_bytes(b"mp3")
                return ("bdioIFdkLag", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STEP1_USE_LRC_AUDIO_QUERY", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Shakira",
                        "track": "Loca",
                        "title": "Loca",
                        "display": "Shakira - Loca",
                        "normalized_query": "Shakira - Loca",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._wait_for_local_lrc_target_duration_sec", return_value=180.53),
                mock.patch("scripts.step1_fetch._source_video_matches_expected", return_value=True),
            ):
                step1.step1_fetch(
                    query="shakira loca",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                    duration_aware_source_match=True,
                )

            self.assertEqual(observed.get("expected_duration_sec"), 180.53)

    def test_step1_fetch_retry_attempt_1_hot_query_canonicalizes_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Carlos Y Jose", "title": "Al Pie De Un Arbol"}

            def fake_fetch_best_synced_lrc_fast(query, out_path, timeout_sec):  # type: ignore[no-untyped-def]
                del timeout_sec
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Carlos Y Jose", "title": "Al Pie De Un Arbol"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del search_n
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("BxtKhMezsgw", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    side_effect=AssertionError("retry_attempt=1 should not call strict normalizer"),
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc_fast", side_effect=fake_fetch_best_synced_lrc_fast),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="carlos y jose el arbolito",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                    retry_attempt=1,
                    speed_mode="ultimate-light-speed",
                )

            self.assertEqual(observed.get("lrc_query"), "Carlos Y Jose - Al Pie De Un Arbol")
            self.assertEqual(observed.get("mp3_query"), "Carlos Y Jose - Al Pie De Un Arbol")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lookup_query"], "Carlos Y Jose - Al Pie De Un Arbol")
            self.assertEqual(meta["lookup_query_source"], "retry_attempt_1_hot_query")
            self.assertEqual(meta["query_normalization"]["short_circuit"], "hot_query_canonical")

    def test_step1_fetch_lrc_artist_title_override_applies_only_to_lyrics_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "The Beatles", "title": "Let It Be"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del search_n
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("CGj85pVzRJs", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "The Beatles",
                        "track": "Let It Be",
                        "title": "Let It Be",
                        "display": "The Beatles - Let It Be",
                        "normalized_query": "The Beatles - Let It Be",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="the beatles let it be",
                    lrc_artist="joan jett",
                    lrc_title="i love rock n roll",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertEqual(observed.get("lrc_query"), "joan jett - i love rock n roll")
            self.assertEqual(observed.get("mp3_query"), "The Beatles - Let It Be")
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lrc_lookup_query"], "joan jett - i love rock n roll")
            self.assertEqual(meta["lrc_lookup_query_source"], "lrc_artist_title_override")
            self.assertEqual(meta["audio_lookup_query"], "The Beatles - Let It Be")

    def test_step1_fetch_lyric_start_constraint_iterates_candidates_until_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed_queries: list[str] = []

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                q = str(query)
                observed_queries.append(q)
                if "que linda esta la" in q.lower():
                    out_path.write_text("[00:00.00]Que linda esta la mañana\n", encoding="utf-8")
                    return {"ok": True, "provider": "unit", "artist": "Las Mananitas", "title": "La Mañanitas"}
                out_path.write_text("[00:00.00]Estas son las mañanitas\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Nat King Cole", "title": "Las Mañanitas"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del query, search_n
                out_path.write_bytes(b"mp3")
                return ("kcrxPu2qyr0", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Alfonso Esparza Oteo",
                        "track": "Las Mañanitas",
                        "title": "Las Mañanitas",
                        "display": "Alfonso Esparza Oteo - Las Mañanitas",
                        "normalized_query": "Alfonso Esparza Oteo - Las Mañanitas",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="las mañanitas",
                    lrc_artist="Alfonso Esparza Oteo",
                    lrc_title="Las Mañanitas",
                    lyric_start="que linda esta la",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertGreaterEqual(len(observed_queries), 2)
            self.assertEqual(observed_queries[0], "Alfonso Esparza Oteo - Las Mañanitas")
            self.assertTrue(any("que linda esta la" in q.lower() for q in observed_queries[1:]))
            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lyric_start_constraint"], "que linda esta la")
            self.assertIn("que linda esta la", str(meta["lrc_lookup_query_effective"]).lower())
            self.assertEqual(meta["lrc_fetch"]["lyric_start_first_line"], "Que linda esta la mañana")

    def test_step1_fetch_lyric_start_constraint_fails_when_no_candidate_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del query, prefer_langs, enable_source_fallback
                out_path.write_text("[00:00.00]Estas son las mañanitas\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "Nat King Cole", "title": "Las Mañanitas"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del query, search_n
                out_path.write_bytes(b"mp3")
                return ("kcrxPu2qyr0", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_LRC_VIDEOID_RECOVERY_ALLOW_NO_COOKIE", False),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Alfonso Esparza Oteo",
                        "track": "Las Mañanitas",
                        "title": "Las Mañanitas",
                        "display": "Alfonso Esparza Oteo - Las Mañanitas",
                        "normalized_query": "Alfonso Esparza Oteo - Las Mañanitas",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    step1.step1_fetch(
                        query="las mañanitas",
                        lrc_artist="Alfonso Esparza Oteo",
                        lrc_title="Las Mañanitas",
                        lyric_start="que linda esta la",
                        slug="",
                        force=True,
                        reset=False,
                        nuke=False,
                        yt_search_n=4,
                        parallel=False,
                    )
        self.assertIn("lyric_start_mismatch", str(ctx.exception))

    def test_step1_fetch_falls_back_to_raw_query_when_strict_normalization_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            observed: dict[str, str] = {}
            lrc_calls: list[str] = []

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                del prefer_langs, enable_source_fallback
                lrc_calls.append(str(query))
                observed["lrc_query"] = str(query)
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit", "artist": "", "title": ""}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                del search_n
                observed["mp3_query"] = str(query)
                out_path.write_bytes(b"mp3")
                return ("CGj85pVzRJs", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest",
                        "error": "Unable to confidently resolve artist and title.",
                        "user_error": "Unable to identify song. Please include artist and title.",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="johnf fruadnsf the past recedes",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            slug = info["slug"]
            self.assertTrue(lrc_calls)
            self.assertEqual(lrc_calls[0], "johnf fruadnsf the past recedes")
            self.assertEqual(observed.get("mp3_query"), "johnf fruadnsf the past recedes")
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["lookup_query"], "johnf fruadnsf the past recedes")
            self.assertEqual(meta["lookup_query_source"], "normalization_fallback_raw_query")
            self.assertEqual(
                meta["query_normalization"]["error"],
                "Unable to confidently resolve artist and title.",
            )

    def test_step1_fetch_retries_canonical_lyrics_when_metadata_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            first_info = {
                "ok": True,
                "provider": "lrclib_search",
                "artist": "Joan Baez",
                "title": "Let It Be",
            }
            second_info = {
                "ok": True,
                "provider": "lrclib_search",
                "artist": "The Beatles",
                "title": "Let It Be",
            }
            calls: list[str] = []

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                calls.append(str(query))
                if len(calls) == 1:
                    out_path.write_text("[00:00.00]wrong artist\n", encoding="utf-8")
                    return dict(first_info)
                out_path.write_text("[00:00.00]beatles line\n", encoding="utf-8")
                return dict(second_info)

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                out_path.write_bytes(b"mp3")
                return ("CGj85pVzRJs", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "The Beatles",
                        "track": "Let It Be",
                        "title": "Let It Be",
                        "display": "The Beatles - Let It Be",
                        "normalized_query": "The Beatles - Let It Be",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._resolve_canonical_artist_title", return_value=("The Beatles", "Let It Be")),
            ):
                info = step1.step1_fetch(
                    query="let it be",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            slug = info["slug"]
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertGreaterEqual(len(calls), 2)
            self.assertEqual(meta["artist"], "The Beatles")
            self.assertEqual(meta["title"], "Let It Be")
            self.assertEqual(meta["lrc_fetch"]["artist"], "The Beatles")
            self.assertEqual(meta["lrc_fetch"]["title"], "Let It Be")

    def test_step1_fetch_refreshes_weak_cached_lyrics_without_redownloading_audio(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            slug = "john_frusciante_god"
            audio_path = mp3_dir / f"{slug}.mp3"
            lrc_path = lrc_dir / f"{slug}.lrc"
            meta_path = meta_dir / f"{slug}.step1.json"
            audio_path.write_bytes(b"cached-audio")
            lrc_path.write_text("[00:08.00]Lyrics unavailable\n", encoding="utf-8")
            meta_path.write_text(
                json.dumps(
                    {
                        "slug": slug,
                        "query": "john frusciante god",
                        "source_id": "esPy-AvRoXA",
                        "lrc_fetch": {"ok": True, "provider": "step1_fallback_pseudo"},
                    }
                ),
                encoding="utf-8",
            )

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                out_path.write_text("[00:00.00]God\n[00:02.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "lrclib_get", "artist": "John Frusciante", "title": "God"}

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "LRC_REUSE_REFRESH_ON_WEAK", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "John Frusciante",
                        "track": "God",
                        "title": "God",
                        "display": "John Frusciante - God",
                        "normalized_query": "John Frusciante - God",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc) as fetch_mock,
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=AssertionError("should not redownload audio")),
            ):
                info = step1.step1_fetch(
                    query="john frusciante god",
                    slug=slug,
                    force=False,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertFalse(info["reused"])
            self.assertEqual(fetch_mock.call_count, 1)
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["lrc_fetch"]["provider"], "lrclib_get")

    def test_step1_fetch_force_removes_stale_artifacts_before_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            slug = "song_slug"
            stale_mp3 = mp3_dir / f"{slug}.mp3"
            stale_m4a = mp3_dir / f"{slug}.m4a"
            stale_lrc = lrc_dir / f"{slug}.lrc"
            stale_meta = meta_dir / f"{slug}.step1.json"
            stale_mp3.write_bytes(b"old-mp3")
            stale_m4a.write_bytes(b"old-m4a")
            stale_lrc.write_text("[00:00.00]old\n", encoding="utf-8")
            stale_meta.write_text("{}", encoding="utf-8")

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                out_path.write_text("[00:00.00]new\n", encoding="utf-8")
                return {"ok": True, "provider": "unit"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                # Force mode should have cleared stale outputs first.
                self.assertFalse(out_path.exists())
                self.assertFalse(stale_m4a.exists())
                out_path.write_bytes(b"new-mp3")
                return ("vid-force", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Artist",
                        "track": "Song",
                        "title": "Song",
                        "display": "Artist - Song",
                        "normalized_query": "Artist - Song",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
            ):
                info = step1.step1_fetch(
                    query="Song",
                    slug=slug,
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertFalse(info["reused"])
            self.assertEqual(info["source_id"], "vid-force")
            self.assertEqual(stale_mp3.read_bytes(), b"new-mp3")

    def test_step1_fetch_writes_pseudo_lrc_when_synced_lyrics_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                return {"ok": False, "provider": "", "reason": "no_synced_lyrics_found"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                out_path.write_bytes(b"mp3")
                return ("vid-fallback", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "John Frusciante",
                        "track": "The Past Recedes",
                        "title": "The Past Recedes",
                        "display": "John Frusciante - The Past Recedes",
                        "normalized_query": "John Frusciante - The Past Recedes",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                mock.patch("scripts.step1_fetch._yt_search_top_result_hint", return_value={"artist": "John Frusciante", "title": "The Past Recedes"}),
                mock.patch.object(step1, "LRC_PSEUDO_START_SECS", 0.0),
                mock.patch.object(step1, "LRC_PSEUDO_STEP_SECS", 1.0),
                mock.patch.object(step1, "LRC_PSEUDO_MAX_LINES", 10),
                mock.patch.object(step1, "STRICT_REQUIRE_LYRICS", False),
            ):
                info = step1.step1_fetch(
                    query="johsdf frusciana the past recedds",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            slug = info["slug"]
            lrc_text = (lrc_dir / f"{slug}.lrc").read_text(encoding="utf-8")
            meta = json.loads((meta_dir / f"{slug}.step1.json").read_text(encoding="utf-8"))
            self.assertIn("John Frusciante - The Past Recedes", lrc_text)
            self.assertEqual(meta["lrc_fetch"]["provider"], "step1_fallback_pseudo")

    def test_step1_fetch_fails_when_synced_lyrics_missing_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                return {"ok": False, "provider": "", "reason": "no_synced_lyrics_found"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                out_path.write_bytes(b"mp3")
                return ("vid-fallback", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_REQUIRE_LYRICS", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "John Frusciante",
                        "track": "The Past Recedes",
                        "title": "The Past Recedes",
                        "display": "John Frusciante - The Past Recedes",
                        "normalized_query": "John Frusciante - The Past Recedes",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                self.assertRaises(RuntimeError) as ctx,
            ):
                step1.step1_fetch(
                    query="johsdf frusciana the past recedds",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertIn("No synced lyrics found", str(ctx.exception))

    def test_step1_fetch_fails_when_audio_missing_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            mp3_dir = root / "mp3s"
            lrc_dir = root / "timings"
            meta_dir = root / "meta"
            for d in (mp3_dir, lrc_dir, meta_dir):
                d.mkdir(parents=True, exist_ok=True)

            def fake_fetch_best_synced_lrc(query, out_path, prefer_langs, enable_source_fallback):  # type: ignore[no-untyped-def]
                out_path.write_text("[00:00.00]line\n", encoding="utf-8")
                return {"ok": True, "provider": "unit"}

            def fake_download(query, out_path, search_n):  # type: ignore[no-untyped-def]
                # Return a path but intentionally do not write audio bytes.
                return ("vid-no-audio", out_path)

            with (
                mock.patch.object(step1, "MP3_DIR", mp3_dir),
                mock.patch.object(step1, "TIMINGS_DIR", lrc_dir),
                mock.patch.object(step1, "META_DIR", meta_dir),
                mock.patch.object(step1, "STRICT_REQUIRE_AUDIO", True),
                mock.patch(
                    "scripts.step1_fetch._normalize_query_via_ytsearch_top_result",
                    return_value={
                        "provider": "yt_suggest_ytsearch1",
                        "artist": "Artist",
                        "track": "Title",
                        "title": "Title",
                        "display": "Artist - Title",
                        "normalized_query": "Artist - Title",
                        "confidence": "high",
                    },
                ),
                mock.patch("scripts.step1_fetch.fetch_best_synced_lrc", side_effect=fake_fetch_best_synced_lrc),
                mock.patch("scripts.step1_fetch.download_first_working_mp3", side_effect=fake_download),
                self.assertRaises(RuntimeError) as ctx,
            ):
                step1.step1_fetch(
                    query="artist title",
                    slug="",
                    force=True,
                    reset=False,
                    nuke=False,
                    yt_search_n=4,
                    parallel=False,
                )

            self.assertIn("No audio found", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
