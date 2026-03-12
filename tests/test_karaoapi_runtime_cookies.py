import tempfile
import unittest
import subprocess
import os
import time
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

try:
    from fastapi import HTTPException
    from karaoapi import app as api
except ModuleNotFoundError:
    HTTPException = Exception  # type: ignore[assignment]
    api = None


@unittest.skipIf(api is None, "fastapi is not installed in this test environment")
class KaraoApiRuntimeCookieTests(unittest.TestCase):
    def setUp(self) -> None:
        with api._jobs_lock:
            self._jobs_snapshot = dict(api._jobs)
            self._slug_snapshot = dict(api._slug_to_job_id)
            self._active_snapshot = int(api._active_job_count_cached)
            api._jobs.clear()
            api._slug_to_job_id.clear()
            api._active_job_count_cached = 0

    def tearDown(self) -> None:
        with api._jobs_lock:
            api._jobs.clear()
            api._jobs.update(self._jobs_snapshot)
            api._slug_to_job_id.clear()
            api._slug_to_job_id.update(self._slug_snapshot)
            api._active_job_count_cached = self._active_snapshot

    @staticmethod
    def _mock_request(headers: dict | None = None):
        req = mock.Mock()
        req.headers = headers or {}
        return req

    def test_normalize_runtime_cookies_payload_allows_empty(self) -> None:
        self.assertEqual(api._normalize_runtime_cookies_payload(None), "")
        self.assertEqual(api._normalize_runtime_cookies_payload("   "), "")

    def test_normalize_runtime_cookies_payload_rejects_non_netscape(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            api._normalize_runtime_cookies_payload("SID=abc; HSID=def")
        self.assertEqual(ctx.exception.status_code, 400)

    def test_normalize_runtime_cookies_payload_accepts_netscape_rows(self) -> None:
        raw = "# Netscape HTTP Cookie File\r\n.youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123"
        normalized = api._normalize_runtime_cookies_payload(raw)
        self.assertIn(".youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123", normalized)
        self.assertTrue(normalized.endswith("\n"))

    def test_normalize_runtime_cookies_payload_accepts_httponly_rows(self) -> None:
        raw = "#HttpOnly_.youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123"
        normalized = api._normalize_runtime_cookies_payload(raw)
        self.assertIn("#HttpOnly_.youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123", normalized)

    def test_write_and_remove_runtime_cookies_file_roundtrip(self) -> None:
        payload = ".youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123\n"
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(api, "JOB_RUNTIME_COOKIES_DIR", Path(td)):
                cookies_path = api._write_runtime_cookies_file("job-123", payload)
                self.assertIsNotNone(cookies_path)
                assert cookies_path is not None
                p = Path(cookies_path)
                self.assertTrue(p.exists())
                self.assertEqual(p.read_text(encoding="utf-8"), payload)
                api._remove_runtime_cookies_file(cookies_path)
                self.assertFalse(p.exists())

    def test_env_int_defaults_and_clamps(self) -> None:
        self.assertEqual(api._env_int("__missing_env_int__", 7, minimum=1), 7)
        with mock.patch.dict("os.environ", {"__test_env_int__": "not-an-int"}, clear=False):
            self.assertEqual(api._env_int("__test_env_int__", 7, minimum=1), 7)
        with mock.patch.dict("os.environ", {"__test_env_int__": "0"}, clear=False):
            self.assertEqual(api._env_int("__test_env_int__", 7, minimum=1), 1)
        with mock.patch.dict("os.environ", {"__test_env_int__": "3"}, clear=False):
            self.assertEqual(api._env_int("__test_env_int__", 7, minimum=1), 3)

    def test_env_bool_defaults_and_truthy_values(self) -> None:
        self.assertEqual(api._env_bool("__missing_env_bool__", True), True)
        self.assertEqual(api._env_bool("__missing_env_bool__", False), False)
        with mock.patch.dict("os.environ", {"__test_env_bool__": "1"}, clear=False):
            self.assertEqual(api._env_bool("__test_env_bool__", False), True)
        with mock.patch.dict("os.environ", {"__test_env_bool__": "on"}, clear=False):
            self.assertEqual(api._env_bool("__test_env_bool__", False), True)
        with mock.patch.dict("os.environ", {"__test_env_bool__": "off"}, clear=False):
            self.assertEqual(api._env_bool("__test_env_bool__", True), False)

    def test_enable_no_cookie_recovery_argv_adds_expected_flags(self) -> None:
        argv = api._enable_no_cookie_recovery_argv(["--query", "The Beatles - Let It Be"])
        self.assertIn("--force", argv)
        self.assertIn("--reset", argv)
        self.assertNotIn("--no-parallel", argv)
        self.assertIn("--yt-search-n", argv)
        idx = argv.index("--yt-search-n")
        self.assertGreaterEqual(int(argv[idx + 1]), int(api.NO_COOKIE_RECOVERY_YT_SEARCH_N))

    def test_enable_no_cookie_recovery_argv_keeps_higher_search_n(self) -> None:
        argv = api._enable_no_cookie_recovery_argv(
            ["--query", "The Beatles - Let It Be", "--yt-search-n", "40"]
        )
        idx = argv.index("--yt-search-n")
        self.assertEqual(int(argv[idx + 1]), 40)

    def test_enable_no_cookie_recovery_argv_removes_no_parallel(self) -> None:
        argv = api._enable_no_cookie_recovery_argv(
            ["--query", "The Beatles - Let It Be", "--no-parallel", "--force"]
        )
        self.assertNotIn("--no-parallel", argv)
        self.assertIn("--force", argv)

    def test_should_try_no_cookie_recovery_requires_cookie_marker_and_no_runtime_cookies(self) -> None:
        job = api.Job(
            id="job-no-cookie",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            status="running",
            options={"runtime_cookies_supplied": False},
        )
        self.assertTrue(
            api._should_try_no_cookie_recovery(
                job,
                "COOKIE_REFRESH_REQUIRED: YouTube requested sign-in",
                runtime_cookies_payload="",
            )
        )
        self.assertFalse(
            api._should_try_no_cookie_recovery(
                job,
                "yt-dlp search failed",
                runtime_cookies_payload="",
            )
        )
        self.assertFalse(
            api._should_try_no_cookie_recovery(
                job,
                "COOKIE_REFRESH_REQUIRED: YouTube requested sign-in",
                runtime_cookies_payload=".youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc",
            )
        )

    def test_sanitize_public_text_maps_gpu_worker_failures_to_audio_processing_message(self) -> None:
        got = api._sanitize_public_text(
            "GPU worker separation failed and CPU fallback is disabled: timeout",
            is_error=True,
        )
        self.assertEqual(got, "Could not process audio for this track. Please try again.")

    def test_sanitize_public_text_maps_missing_lyrics_to_user_message(self) -> None:
        got = api._sanitize_public_text(
            "RuntimeError: No lyrics found for slug=the_beatles_let_it_be (missing LRC file)",
            is_error=True,
        )
        self.assertEqual(got, "Could not find synced lyrics for this track.")

    def test_sanitize_public_text_maps_missing_audio_to_user_message(self) -> None:
        got = api._sanitize_public_text(
            "RuntimeError: No audio found for slug the_beatles_let_it_be after download/reuse",
            is_error=True,
        )
        self.assertEqual(got, "Could not find audio for this track.")

    def test_parse_timing_line_parses_step_part_and_elapsed_ms(self) -> None:
        parsed = api._parse_timing_line("step=step1 part=fetch_lyrics elapsed_ms=49.44")
        self.assertEqual(parsed, ("step1", "fetch_lyrics", 49.4))

    def test_parse_timing_line_returns_none_for_invalid_payload(self) -> None:
        self.assertIsNone(api._parse_timing_line("no timing here"))
        self.assertIsNone(api._parse_timing_line("step=step1 part=fetch elapsed_ms=-1"))

    def test_update_job_progress_records_timing_breakdown(self) -> None:
        job = api.Job(
            id="job-timing",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            status="running",
            started_at=1.0,
        )
        api._update_job_progress(
            job,
            message="[TIMING] step=step1 part=fetch_lyrics elapsed_ms=49.44",
            tag="TIMING",
        )
        self.assertIn("step1.fetch_lyrics", job.timing_breakdown)
        self.assertEqual(job.timing_breakdown["step1.fetch_lyrics"], 49.4)

    def test_derive_pipeline_timing_seconds_prefers_pipeline_keys(self) -> None:
        derived = api._derive_pipeline_timing_seconds(
            {
                "pipeline.step1": 1200.0,
                "step1.total": 900.0,
                "step3.total": 2500.0,
                "pipeline.total": 5000.0,
            }
        )
        self.assertEqual(derived.get("step1"), 1.2)
        self.assertEqual(derived.get("step3"), 2.5)
        self.assertEqual(derived.get("total"), 5.0)

    def test_job_to_dict_includes_elapsed_and_pipeline_timing(self) -> None:
        job = api.Job(
            id="job-serialize",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=100.0,
            started_at=101.0,
            finished_at=108.25,
            status="succeeded",
            timing_breakdown={
                "pipeline.step1": 3000.0,
                "pipeline.step3": 4000.0,
                "pipeline.total": 9000.0,
            },
        )
        out = api._job_to_dict(job)
        self.assertEqual(out.get("elapsed_sec"), 7.2)
        self.assertEqual(out.get("pipeline_timing"), {"step1": 3.0, "step3": 4.0, "total": 9.0})

    def test_strip_stem_levels_for_non_render_job(self) -> None:
        options, dropped = api._strip_stem_levels_for_non_render(
            {"render_only": False, "vocals": 100, "bass": 100, "drums": 100, "other": 100}
        )
        self.assertEqual(dropped, ["vocals", "bass", "drums", "other"])
        self.assertIsNone(options.get("vocals"))
        self.assertIsNone(options.get("bass"))
        self.assertIsNone(options.get("drums"))
        self.assertIsNone(options.get("other"))

    def test_strip_stem_levels_kept_for_render_only(self) -> None:
        options, dropped = api._strip_stem_levels_for_non_render(
            {"render_only": True, "vocals": 80, "bass": 90, "drums": 110, "other": 100}
        )
        self.assertEqual(dropped, [])
        self.assertEqual(options.get("vocals"), 80)
        self.assertEqual(options.get("bass"), 90)
        self.assertEqual(options.get("drums"), 110)
        self.assertEqual(options.get("other"), 100)

    def test_strip_stem_levels_keeps_explicit_non_default_for_non_render(self) -> None:
        with mock.patch.object(api, "ALLOW_STEM_LEVELS_NON_RENDER", False):
            options, dropped = api._strip_stem_levels_for_non_render(
                {"render_only": False, "vocals": 0, "bass": 100, "drums": 100, "other": 100}
            )
        self.assertEqual(dropped, ["bass", "drums", "other"])
        self.assertEqual(options.get("vocals"), 0)
        self.assertIsNone(options.get("bass"))
        self.assertIsNone(options.get("drums"))
        self.assertIsNone(options.get("other"))

    def test_build_pipeline_argv_keeps_explicit_stem_levels_for_non_render(self) -> None:
        job = api.Job(
            id="job-stems-requested",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            status="queued",
            options={"render_only": False, "vocals": 80, "bass": 80, "drums": 80, "other": 80},
        )
        with mock.patch.object(api, "ALLOW_STEM_LEVELS_NON_RENDER", False):
            argv = api._build_pipeline_argv(job)
        self.assertIn("--vocals", argv)
        self.assertIn("--bass", argv)
        self.assertIn("--drums", argv)
        self.assertIn("--other", argv)

    def test_build_pipeline_argv_ignores_default_stem_levels_for_non_render(self) -> None:
        job = api.Job(
            id="job-default-stems",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            status="queued",
            options={"render_only": False, "vocals": 100, "bass": 100, "drums": 100, "other": 100},
        )
        with mock.patch.object(api, "ALLOW_STEM_LEVELS_NON_RENDER", False):
            argv = api._build_pipeline_argv(job)
        self.assertNotIn("--vocals", argv)
        self.assertNotIn("--bass", argv)
        self.assertNotIn("--drums", argv)
        self.assertNotIn("--other", argv)

    def test_build_pipeline_argv_keeps_stem_levels_for_render_only(self) -> None:
        job = api.Job(
            id="job-render-stems",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            status="queued",
            options={"render_only": True, "vocals": 80, "bass": 80, "drums": 80, "other": 80},
        )
        argv = api._build_pipeline_argv(job)
        self.assertIn("--vocals", argv)
        self.assertIn("--bass", argv)
        self.assertIn("--drums", argv)
        self.assertIn("--other", argv)

    def test_build_preview_render_env_applies_preview_overrides(self) -> None:
        with (
            mock.patch.object(api, "PREVIEW_RENDER_LEVEL", "2"),
            mock.patch.object(api, "PREVIEW_RENDER_PROFILE", "fast"),
            mock.patch.object(api, "PREVIEW_VIDEO_SIZE", "640x360"),
            mock.patch.object(api, "PREVIEW_FPS", "4"),
            mock.patch.object(api, "PREVIEW_VIDEO_BITRATE", "120k"),
            mock.patch.object(api, "PREVIEW_AUDIO_BITRATE", "96k"),
            mock.patch.object(api, "PREVIEW_X264_PRESET", "ultrafast"),
            mock.patch.object(api, "PREVIEW_X264_TUNE", "zerolatency"),
        ):
            env = api._build_preview_render_env()
        self.assertEqual(env.get("KARAOKE_RENDER_LEVEL"), "2")
        self.assertEqual(env.get("KARAOKE_RENDER_PROFILE"), "fast")
        self.assertEqual(env.get("KARAOKE_VIDEO_SIZE"), "640x360")
        self.assertEqual(env.get("KARAOKE_FPS"), "4")
        self.assertEqual(env.get("KARAOKE_VIDEO_BITRATE"), "120k")
        self.assertEqual(env.get("KARAOKE_AUDIO_BITRATE"), "96k")
        self.assertEqual(env.get("KARAOKE_X264_PRESET"), "ultrafast")
        self.assertEqual(env.get("KARAOKE_X264_TUNE"), "zerolatency")

    def test_mux_audio_into_preview_returns_false_when_preview_missing(self) -> None:
        ok, detail = api._mux_audio_into_preview(
            slug="song",
            muted_preview_path=Path("/tmp/does-not-exist-preview.mp4"),
            final_path=Path("/tmp/does-not-exist-final.mp4"),
            env={},
        )
        self.assertFalse(ok)
        self.assertIn("muted preview missing", detail)

    def test_mux_audio_into_preview_uses_explicit_stream_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            muted_preview = base / "preview.muted.mp4"
            audio_path = base / "mix.mp3"
            final_path = base / "final.mp4"
            muted_preview.write_bytes(b"x" * 4096)
            audio_path.write_bytes(b"x" * 4096)
            calls: list[list[str]] = []

            def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
                calls.append(list(cmd))
                # Probe command: ffmpeg ... -map 0:a:0 -f null -
                if "0:a:0" in cmd:
                    return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
                final_path.write_bytes(b"x" * 4096)
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            with (
                mock.patch.object(api, "_resolve_audio_for_mux", return_value=audio_path),
                mock.patch("scripts.common.resolve_ffmpeg_bin", return_value=Path("/usr/bin/ffmpeg")),
                mock.patch("subprocess.run", side_effect=_fake_run),
            ):
                ok, detail = api._mux_audio_into_preview(
                    slug="song",
                    muted_preview_path=muted_preview,
                    final_path=final_path,
                    env={},
                )

            self.assertTrue(ok)
            self.assertEqual(detail, "")
            self.assertGreaterEqual(len(calls), 2)
            mux_cmd = calls[0]
            self.assertIn("-map", mux_cmd)
            self.assertIn("0:v:0", mux_cmd)
            self.assertIn("1:a:0", mux_cmd)

    def test_mux_audio_into_preview_rejects_output_without_audio_stream(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            muted_preview = base / "preview.muted.mp4"
            audio_path = base / "mix.mp3"
            final_path = base / "final.mp4"
            muted_preview.write_bytes(b"x" * 4096)
            audio_path.write_bytes(b"x" * 4096)

            def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
                # Probe command fails => no audio stream in mux output.
                if "0:a:0" in cmd:
                    return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="", stderr="no stream")
                final_path.write_bytes(b"x" * 4096)
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            with (
                mock.patch.object(api, "_resolve_audio_for_mux", return_value=audio_path),
                mock.patch("scripts.common.resolve_ffmpeg_bin", return_value=Path("/usr/bin/ffmpeg")),
                mock.patch("subprocess.run", side_effect=_fake_run),
            ):
                ok, detail = api._mux_audio_into_preview(
                    slug="song",
                    muted_preview_path=muted_preview,
                    final_path=final_path,
                    env={},
                )

            self.assertFalse(ok)
            self.assertIn("missing audio stream", detail)
            self.assertFalse(final_path.exists())

    def test_resolve_audio_for_mux_handles_choose_audio_exit(self) -> None:
        with mock.patch("scripts.step4_assemble.choose_audio", side_effect=SystemExit(1)):
            self.assertIsNone(api._resolve_audio_for_mux("song"))

    def test_clear_preview_artifacts_on_failure_removes_muted_preview_only_output(self) -> None:
        job = api.Job(
            id="job-preview-only",
            query="The Beatles - Let It Be",
            slug="the_beatles_let_it_be",
            created_at=1.0,
            status="failed",
        )
        job.output_path = "/tmp/the_beatles_let_it_be.preview.muted.mp4"
        job.output_url = "/output/the_beatles_let_it_be.preview.muted.mp4"
        job.preview_output_url = "/output/the_beatles_let_it_be.preview.muted.mp4"
        job.output_is_preview = True
        job.final_output_url = None

        api._clear_preview_artifacts_on_failure(job)

        self.assertIsNone(job.output_path)
        self.assertIsNone(job.output_url)
        self.assertIsNone(job.preview_output_url)
        self.assertIsNone(job.output_is_preview)

    def test_clear_preview_artifacts_on_failure_keeps_final_output(self) -> None:
        job = api.Job(
            id="job-final",
            query="The Beatles - Let It Be",
            slug="the_beatles_let_it_be",
            created_at=1.0,
            status="failed",
        )
        job.output_path = "/tmp/the_beatles_let_it_be.mp4"
        job.output_url = "/output/the_beatles_let_it_be.mp4"
        job.preview_output_url = "/output/the_beatles_let_it_be.preview.muted.mp4"
        job.output_is_preview = True
        job.final_output_url = "/output/the_beatles_let_it_be.mp4"

        api._clear_preview_artifacts_on_failure(job)

        self.assertEqual(job.output_url, "/output/the_beatles_let_it_be.mp4")
        self.assertEqual(job.final_output_url, "/output/the_beatles_let_it_be.mp4")

    def test_render_early_mute_preview_sets_preview_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_dir = base / "timings"
            output_dir = base / "output"
            meta_dir = base / "meta"
            mixes_dir = base / "mixes"
            separated_dir = base / "separated"
            timings_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            mixes_dir.mkdir(parents=True, exist_ok=True)
            separated_dir.mkdir(parents=True, exist_ok=True)
            slug = "the_beatles_let_it_be"
            (timings_dir / f"{slug}.lrc").write_text("[00:01.00]Let it be\n", encoding="utf-8")

            job = api.Job(
                id="job-preview-mute",
                query="The Beatles - Let It Be",
                slug=slug,
                created_at=1.0,
                status="running",
                options={"language": "auto"},
            )

            muted_preview = output_dir / f"{slug}.preview.muted.mp4"

            def _fake_run(*_args, **_kwargs):
                muted_preview.write_bytes(b"x" * 4096)
                return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

            with (
                mock.patch.object(api, "TIMINGS_DIR", timings_dir),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.object(api, "META_DIR", meta_dir),
                mock.patch.object(api, "MIXES_DIR", mixes_dir),
                mock.patch.object(api, "SEPARATED_DIR", separated_dir),
                mock.patch.object(api, "EARLY_MUTE_PREVIEW_WAIT_SEC", 1.0),
                mock.patch.object(api, "EARLY_MUTE_PREVIEW_POLL_SEC", 0.01),
                mock.patch.object(api, "_effective_early_mute_preview_enabled", return_value=True),
                mock.patch.object(api, "_discard_stale_preview_if_needed", return_value=False),
                mock.patch.object(api, "_is_fresh_preview_for_slug", return_value=False),
                mock.patch("scripts.step3_sync.step3_sync_lite", return_value=None),
                mock.patch("subprocess.run", side_effect=_fake_run),
                mock.patch.object(api, "_persist_jobs"),
            ):
                api._render_early_mute_preview(job)

            self.assertEqual(job.stage, "preview_ready")
            self.assertTrue(bool(job.output_is_preview))
            self.assertTrue(str(job.output_url or "").endswith(".preview.muted.mp4"))

    def test_render_early_mute_preview_ignores_stale_lrc_from_previous_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_dir = base / "timings"
            output_dir = base / "output"
            meta_dir = base / "meta"
            mixes_dir = base / "mixes"
            separated_dir = base / "separated"
            timings_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            mixes_dir.mkdir(parents=True, exist_ok=True)
            separated_dir.mkdir(parents=True, exist_ok=True)
            slug = "the_beatles_let_it_be"
            lrc_path = timings_dir / f"{slug}.lrc"
            lrc_path.write_text("[00:01.00]Let it be\n", encoding="utf-8")
            os.utime(lrc_path, (10.0, 10.0))

            job = api.Job(
                id="job-preview-stale",
                query="The Beatles - Let It Be",
                slug=slug,
                created_at=20.0,
                started_at=20.0,
                status="running",
                options={"language": "auto", "preview": True},
            )

            mono_values = iter([0.0, 0.01, 0.02, 0.06])
            with (
                mock.patch.object(api, "TIMINGS_DIR", timings_dir),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.object(api, "META_DIR", meta_dir),
                mock.patch.object(api, "MIXES_DIR", mixes_dir),
                mock.patch.object(api, "SEPARATED_DIR", separated_dir),
                mock.patch.object(api, "EARLY_MUTE_PREVIEW_WAIT_SEC", 0.05),
                mock.patch.object(api, "EARLY_MUTE_PREVIEW_POLL_SEC", 0.01),
                mock.patch.object(api, "_effective_early_mute_preview_enabled", return_value=True),
                mock.patch.object(api, "_discard_stale_preview_if_needed", return_value=False),
                mock.patch.object(api, "_is_fresh_preview_for_slug", return_value=False),
                mock.patch("scripts.step3_sync.step3_sync_lite") as step3_lite_mock,
                mock.patch("subprocess.run") as run_mock,
                mock.patch("time.monotonic", side_effect=lambda: next(mono_values)),
                mock.patch("time.sleep", return_value=None),
            ):
                api._render_early_mute_preview(job)

            step3_lite_mock.assert_not_called()
            run_mock.assert_not_called()
            self.assertIsNone(job.output_url)
            self.assertIsNone(job.preview_output_url)
            self.assertNotEqual(job.stage, "preview_ready")

    def test_discard_stale_preview_when_inputs_are_newer(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            timings_dir = base / "timings"
            output_dir = base / "output"
            meta_dir = base / "meta"
            mixes_dir = base / "mixes"
            timings_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            mixes_dir.mkdir(parents=True, exist_ok=True)

            slug = "the_beatles_let_it_be"
            lrc = timings_dir / f"{slug}.lrc"
            lrc.write_text("[00:01.00]Let it be\n", encoding="utf-8")

            preview = output_dir / f"{slug}.preview.mp4"
            preview.write_bytes(b"x" * 4096)

            now_ts = time.time()
            os.utime(preview, (now_ts - 120.0, now_ts - 120.0))
            os.utime(lrc, (now_ts, now_ts))

            with (
                mock.patch.object(api, "TIMINGS_DIR", timings_dir),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.object(api, "META_DIR", meta_dir),
                mock.patch.object(api, "MIXES_DIR", mixes_dir),
            ):
                self.assertFalse(api._is_fresh_preview_for_slug(slug, preview))

    def test_resolve_render_output_path_finds_nested_temp_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            output_dir = base / "output"
            nested_output = output_dir / "temp"
            nested_output.mkdir(parents=True, exist_ok=True)
            target = nested_output / "the_beatles_let_it_be.mp4"
            target.write_bytes(b"x" * 4096)

            with (
                mock.patch.object(api, "BASE_DIR", base),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.dict(os.environ, {}, clear=False),
            ):
                resolved = api._resolve_render_output_path("the_beatles_let_it_be")
                self.assertEqual(resolved, target)
                self.assertEqual(api._output_url_from_path(target), "/output/temp/the_beatles_let_it_be.mp4")

    def test_refresh_job_local_output_fields_rehydrates_succeeded_job(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            output_dir = base / "output"
            nested_output = output_dir / "temp"
            nested_output.mkdir(parents=True, exist_ok=True)
            target = nested_output / "the_beatles_let_it_be.mp4"
            target.write_bytes(b"x" * 4096)
            job = api.Job(
                id="job-rehydrate",
                query="The Beatles - Let It Be",
                slug="the_beatles_let_it_be",
                created_at=1.0,
                status="succeeded",
            )

            with (
                mock.patch.object(api, "BASE_DIR", base),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.dict(os.environ, {}, clear=False),
            ):
                api._refresh_job_local_output_fields(job)

            self.assertEqual(job.output_path, str(target))
            self.assertEqual(job.output_url, "/output/temp/the_beatles_let_it_be.mp4")
            self.assertEqual(job.final_output_url, "/output/temp/the_beatles_let_it_be.mp4")
            self.assertFalse(bool(job.output_is_preview))

    def test_refresh_job_local_output_fields_ignores_running_job(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            output_dir = base / "output"
            nested_output = output_dir / "temp"
            nested_output.mkdir(parents=True, exist_ok=True)
            target = nested_output / "the_beatles_let_it_be.mp4"
            target.write_bytes(b"x" * 4096)
            job = api.Job(
                id="job-running",
                query="The Beatles - Let It Be",
                slug="the_beatles_let_it_be",
                created_at=1.0,
                status="running",
            )

            with (
                mock.patch.object(api, "BASE_DIR", base),
                mock.patch.object(api, "OUTPUT_DIR", output_dir),
                mock.patch.dict(os.environ, {}, clear=False),
            ):
                api._refresh_job_local_output_fields(job)

            self.assertIsNone(job.output_path)
            self.assertIsNone(job.output_url)
            self.assertIsNone(job.final_output_url)

    def test_job_to_poll_dict_includes_mix_audio_url_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            mixes_dir = base / "mixes"
            mixes_dir.mkdir(parents=True, exist_ok=True)
            slug = "the_beatles_let_it_be"
            (mixes_dir / f"{slug}.m4a").write_bytes(b"x" * 4096)
            job = api.Job(
                id="job-mix-audio",
                query="The Beatles - Let It Be",
                slug=slug,
                created_at=1.0,
                status="running",
            )

            with mock.patch.object(api, "MIXES_DIR", mixes_dir):
                payload = api._job_to_poll_dict(job)

            self.assertEqual(payload.get("mix_audio_url"), f"/files/mixes/{slug}.m4a")

    def test_promote_preview_to_final_copies_preview_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            preview = base / "preview.mp4"
            final = base / "final.mp4"
            preview.write_bytes(b"x" * 4096)

            ok, detail = api._promote_preview_to_final(preview_path=preview, final_path=final)

            self.assertTrue(ok)
            self.assertIn(detail, {"hardlink", "copy", "preview already final"})
            self.assertTrue(final.exists())
            self.assertEqual(final.read_bytes(), preview.read_bytes())

    def test_run_job_promotes_playable_preview_instead_of_rerendering_final(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            output_dir = base / "output"
            timings_dir = base / "timings"
            meta_dir = base / "meta"
            mixes_dir = base / "mixes"
            separated_dir = base / "separated"
            output_dir.mkdir(parents=True, exist_ok=True)
            timings_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            mixes_dir.mkdir(parents=True, exist_ok=True)
            separated_dir.mkdir(parents=True, exist_ok=True)

            job = api.Job(
                id="job-preview-promote",
                query="The Beatles - Let It Be",
                slug="the_beatles_let_it_be",
                created_at=api._now_ts(),
                status="queued",
                options={"preview": True},
            )

            class _ImmediateThread:
                def __init__(self, *, target=None, args=(), kwargs=None, daemon=None):
                    self._target = target
                    self._args = args
                    self._kwargs = kwargs or {}

                def start(self):
                    if self._target is not None:
                        self._target(*self._args, **self._kwargs)

                def is_alive(self):
                    return False

                def join(self, timeout=None):
                    return None

            preview_path = output_dir / f"{job.slug}.preview.mp4"
            final_path = output_dir / f"{job.slug}.mp4"
            pipeline_calls: list[list[str]] = []

            def _fake_run_pipeline_cmd(_job, cmd, **_kwargs):
                pipeline_calls.append(list(cmd))
                if "--no-render" in cmd:
                    return 0, []
                preview_path.write_bytes(b"x" * 4096)
                return 0, []

            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(api, "BASE_DIR", base))
                stack.enter_context(mock.patch.object(api, "OUTPUT_DIR", output_dir))
                stack.enter_context(mock.patch.object(api, "TIMINGS_DIR", timings_dir))
                stack.enter_context(mock.patch.object(api, "META_DIR", meta_dir))
                stack.enter_context(mock.patch.object(api, "MIXES_DIR", mixes_dir))
                stack.enter_context(mock.patch.object(api, "SEPARATED_DIR", separated_dir))
                stack.enter_context(mock.patch.object(api, "_write_runtime_cookies_file", return_value=None))
                stack.enter_context(mock.patch.object(api, "_remove_runtime_cookies_file"))
                stack.enter_context(mock.patch.object(api, "_persist_jobs"))
                stack.enter_context(mock.patch.object(api, "_effective_preview_enabled", return_value=True))
                stack.enter_context(mock.patch.object(api, "_effective_early_mute_preview_enabled", return_value=False))
                stack.enter_context(
                    mock.patch.object(api, "_effective_finalize_from_mute_preview_enabled", return_value=True)
                )
                stack.enter_context(mock.patch.object(api, "_build_pipeline_argv", return_value=["--query", job.query]))
                stack.enter_context(mock.patch.object(api, "_build_main_env", return_value=os.environ.copy()))
                stack.enter_context(mock.patch.object(api, "_build_preview_render_env", return_value=os.environ.copy()))
                stack.enter_context(mock.patch.object(api, "_build_final_render_env", return_value=os.environ.copy()))
                stack.enter_context(
                    mock.patch.object(api, "_build_step4_cmd_for_job", side_effect=[["preview-cmd"], ["final-cmd"]])
                )
                stack.enter_context(mock.patch.object(api, "_run_pipeline_cmd", side_effect=_fake_run_pipeline_cmd))
                stack.enter_context(mock.patch.object(api, "_validate_output_media_file", return_value=(True, "")))
                stack.enter_context(mock.patch.object(api, "OUTPUT_VALIDATION_ENFORCED", True))
                stack.enter_context(mock.patch.object(api, "_refresh_job_uploaded_video_url"))
                stack.enter_context(mock.patch("karaoapi.app.threading.Thread", _ImmediateThread))
                api._run_job(job, runtime_cookies_payload="")

            self.assertEqual(job.status, "succeeded")
            self.assertEqual(job.stage, "complete")
            self.assertEqual(job.output_path, str(final_path))
            self.assertEqual(job.final_output_url, f"/output/{final_path.name}")
            self.assertFalse(bool(job.output_is_preview))
            self.assertTrue(final_path.exists())
            self.assertEqual(len(pipeline_calls), 2)
            self.assertIn("--no-render", pipeline_calls[0])
            self.assertEqual(pipeline_calls[1], ["preview-cmd"])

    def test_create_job_dedupes_inflight_slug(self) -> None:
        with api._jobs_lock:
            existing = api.Job(
                id="job-existing",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=1.0,
                status="running",
            )
            api._jobs[existing.id] = existing

        req = api.CreateJobRequest(query="HELLO world!!!", idempotency_key="test-hello-world")
        with mock.patch.object(api, "_persist_jobs") as persist_mock, mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        self.assertEqual(got.slug, existing.slug)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_preserves_preview_request_flag(self) -> None:
        req = api.CreateJobRequest(query="hello world", idempotency_key="preview-job", preview=True)
        with mock.patch.object(api, "_persist_jobs"), mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req, self._mock_request())

        with api._jobs_lock:
            stored = api._jobs[got.id]
        self.assertTrue(bool(stored.options.get("preview")))
        executor_mock.submit.assert_called_once()

    def test_create_job_applies_fast_mobile_offset_defaults_when_unspecified(self) -> None:
        req = api.CreateJobRequest(query="hello world", idempotency_key="mobile-defaults")
        with (
            mock.patch.object(api, "_persist_jobs"),
            mock.patch.object(api, "_executor"),
            mock.patch.object(api, "DEFAULT_TUNE_FOR_ME_LEVEL", 0),
            mock.patch.object(api, "DEFAULT_CALIBRATION_LEVEL", 1),
        ):
            got = api.create_job(req, self._mock_request())

        with api._jobs_lock:
            stored = api._jobs[got.id]
        self.assertEqual(stored.options.get("enable_auto_offset"), 0)
        self.assertEqual(stored.options.get("calibration_level"), 1)

    def test_build_job_dedupe_key_changes_when_preview_mode_changes(self) -> None:
        base = {
            "render_only": False,
            "preview": False,
            "vocals": 100,
            "bass": 100,
            "drums": 100,
            "other": 100,
        }
        preview = dict(base)
        preview["preview"] = True
        self.assertNotEqual(
            api._build_job_dedupe_key("hello world", base),
            api._build_job_dedupe_key("hello world", preview),
        )

    def test_create_job_requires_idempotency_key_when_enabled(self) -> None:
        with mock.patch.object(api, "REQUIRE_IDEMPOTENCY_KEY", True):
            with self.assertRaises(HTTPException) as ctx:
                api.create_job(
                    api.CreateJobRequest(query="hello world", idempotency_key=None),
                    self._mock_request(),
                )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_create_job_respects_emergency_disable_switch(self) -> None:
        with mock.patch.object(api, "EMERGENCY_DISABLE_NEW_JOBS", True):
            with self.assertRaises(HTTPException) as ctx:
                api.create_job(
                    api.CreateJobRequest(query="hello world", idempotency_key="emergency-switch-key"),
                    self._mock_request(),
                )
        self.assertEqual(ctx.exception.status_code, 503)

    def test_create_job_replays_same_job_for_same_idempotency_key(self) -> None:
        req = api.CreateJobRequest(query="hello world", idempotency_key="same-key")
        with (
            mock.patch.object(api, "_persist_jobs"),
            mock.patch.object(api, "_executor") as executor_mock,
        ):
            first = api.create_job(req, self._mock_request())
            second = api.create_job(req, self._mock_request())
        self.assertEqual(first.id, second.id)
        executor_mock.submit.assert_called_once()

    def test_create_job_request_stem_levels_enforce_supported_range(self) -> None:
        api.CreateJobRequest(query="hello world", vocals=0, bass=100, drums=150, other=42)
        with self.assertRaises(Exception):
            api.CreateJobRequest(query="hello world", vocals=-1)
        with self.assertRaises(Exception):
            api.CreateJobRequest(query="hello world", bass=151)

    def test_create_job_dedupes_inflight_hot_alias_slug(self) -> None:
        with api._jobs_lock:
            existing = api.Job(
                id="job-alias-inflight",
                query="the beatles let it be",
                slug=api.slugify("the beatles let it be"),
                created_at=1.0,
                status="running",
            )
            api._jobs[existing.id] = existing

        req = api.CreateJobRequest(query="let it be", idempotency_key="test-let-it-be")
        with mock.patch.object(api, "_persist_jobs") as persist_mock, mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        self.assertEqual(got.slug, existing.slug)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_does_not_dedupe_inflight_when_stem_profile_differs(self) -> None:
        req_existing = api.CreateJobRequest(
            query="The Beatles - Let It Be",
            render_only=False,
            vocals=0,
            bass=100,
            drums=100,
            other=100,
            idempotency_key="test-inflight-stem-existing",
        )
        existing_options, _ = api._strip_stem_levels_for_non_render(
            req_existing.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        existing_options["runtime_cookies_supplied"] = False
        with api._jobs_lock:
            existing = api.Job(
                id="job-inflight-stem-a",
                query=req_existing.query,
                slug=api.slugify(req_existing.query),
                created_at=api._now_ts(),
                status="running",
                options=existing_options,
            )
            api._jobs[existing.id] = existing
            for candidate in api._slug_reuse_candidates(existing.slug):
                api._slug_to_job_id[candidate] = existing.id

        req_variant = api.CreateJobRequest(
            query="The Beatles - Let It Be",
            render_only=False,
            vocals=10,
            bass=100,
            drums=100,
            other=100,
            idempotency_key="test-inflight-stem-variant",
        )
        with mock.patch.object(api, "_persist_jobs"), mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req_variant, self._mock_request())

        self.assertNotEqual(got.id, existing.id)
        executor_mock.submit.assert_called_once()

    def test_create_job_reuses_recent_succeeded_slug(self) -> None:
        req = api.CreateJobRequest(query="hello world", idempotency_key="test-reuse-recent")
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-succeeded",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=now_ts - 30,
                finished_at=now_ts - 10,
                status="succeeded",
                options=options,
            )
            api._jobs[existing.id] = existing

        with mock.patch.object(api, "_persist_jobs") as persist_mock, mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_reuses_recent_succeeded_hot_alias_slug(self) -> None:
        req = api.CreateJobRequest(query="let it be", idempotency_key="test-alias-reuse")
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-alias-succeeded",
                query="the beatles let it be",
                slug=api.slugify("the beatles let it be"),
                created_at=now_ts - 30,
                finished_at=now_ts - 10,
                status="succeeded",
                options=options,
            )
            api._jobs[existing.id] = existing

        with mock.patch.object(api, "_persist_jobs") as persist_mock, mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_does_not_reuse_succeeded_when_force_true(self) -> None:
        req = api.CreateJobRequest(query="hello world", idempotency_key="test-force-no-reuse-base")
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-succeeded",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=now_ts - 30,
                finished_at=now_ts - 10,
                status="succeeded",
                options=options,
            )
            api._jobs[existing.id] = existing

        req_force = api.CreateJobRequest(query="hello world", force=True, idempotency_key="test-force-no-reuse-force")
        with mock.patch.object(api, "_persist_jobs"), mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req_force, self._mock_request())

        self.assertNotEqual(got.id, existing.id)
        executor_mock.submit.assert_called_once()

    def test_create_job_does_not_reuse_succeeded_when_stem_levels_differ(self) -> None:
        req = api.CreateJobRequest(
            query="The Beatles - Let It Be",
            render_only=False,
            vocals=0,
            bass=100,
            drums=100,
            other=100,
            idempotency_key="test-stem-variant-a",
        )
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-stem-variant-a",
                query=req.query,
                slug=api.slugify(req.query),
                created_at=now_ts - 30,
                finished_at=now_ts - 10,
                status="succeeded",
                options=options,
            )
            api._jobs[existing.id] = existing

        req_variant = api.CreateJobRequest(
            query="The Beatles - Let It Be",
            render_only=False,
            vocals=0,
            bass=0,
            drums=100,
            other=100,
            idempotency_key="test-stem-variant-b",
        )
        with mock.patch.object(api, "_persist_jobs"), mock.patch.object(api, "_executor") as executor_mock:
            got = api.create_job(req_variant, self._mock_request())

        self.assertNotEqual(got.id, existing.id)
        executor_mock.submit.assert_called_once()

    def test_create_job_reuses_uploaded_result_beyond_default_age(self) -> None:
        req = api.CreateJobRequest(
            query="hello world",
            upload=True,
            idempotency_key="test-upload-reuse-old",
        )
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-upload-old",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=now_ts - 90_000,
                finished_at=now_ts - 86_400,
                status="succeeded",
                youtube_video_url="https://youtube.com/watch?v=uploadOld",
                options=options,
            )
            api._jobs[existing.id] = existing

        with (
            mock.patch.object(api, "REUSE_SUCCEEDED_JOBS_MAX_AGE_SEC", 1.0),
            mock.patch.object(api, "REUSE_UPLOADED_JOBS_MAX_AGE_SEC", 0.0),
            mock.patch.object(api, "REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED", True),
            mock.patch.object(api, "_persist_jobs") as persist_mock,
            mock.patch.object(api, "_executor") as executor_mock,
        ):
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_reuses_uploaded_result_when_output_path_missing(self) -> None:
        req = api.CreateJobRequest(
            query="hello world",
            upload=True,
            idempotency_key="test-upload-reuse-missing-path",
        )
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-upload-missing-path",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=now_ts - 60,
                finished_at=now_ts - 30,
                status="succeeded",
                youtube_video_url="https://youtube.com/watch?v=uploadMissingPath",
                output_path="/tmp/does-not-exist.mp4",
                options=options,
            )
            api._jobs[existing.id] = existing

        with (
            mock.patch.object(api, "REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED", True),
            mock.patch.object(api, "_persist_jobs") as persist_mock,
            mock.patch.object(api, "_executor") as executor_mock,
        ):
            got = api.create_job(req, self._mock_request())

        self.assertEqual(got.id, existing.id)
        persist_mock.assert_not_called()
        executor_mock.submit.assert_not_called()

    def test_create_job_does_not_reuse_uploaded_result_when_unverified(self) -> None:
        req = api.CreateJobRequest(
            query="hello world",
            upload=True,
            idempotency_key="test-upload-no-reuse-unverified",
        )
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()
        with api._jobs_lock:
            existing = api.Job(
                id="job-upload-unverified",
                query="hello world",
                slug=api.slugify("hello world"),
                created_at=now_ts - 90,
                finished_at=now_ts - 30,
                status="succeeded",
                youtube_video_url="https://youtube.com/watch?v=uploadUnverified",
                options=options,
            )
            api._jobs[existing.id] = existing

        with (
            mock.patch.object(api, "REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED", False),
            mock.patch.object(api, "_persist_jobs"),
            mock.patch.object(api, "_executor") as executor_mock,
        ):
            got = api.create_job(req, self._mock_request())

        self.assertNotEqual(got.id, existing.id)
        executor_mock.submit.assert_called_once()

    def test_create_job_reuses_uploaded_result_when_verified(self) -> None:
        req = api.CreateJobRequest(
            query="hello world",
            upload=True,
            idempotency_key="test-upload-reuse-verified",
        )
        options, _dropped = api._strip_stem_levels_for_non_render(
            req.model_dump(exclude={"source_cookies_netscape", "idempotency_key"})
        )
        options["preview"] = False
        options["runtime_cookies_supplied"] = False
        now_ts = api._now_ts()

        with tempfile.TemporaryDirectory() as td:
            meta_dir = Path(td)
            slug = api.slugify("hello world")
            (meta_dir / f"{slug}.step1.json").write_text(
                '{"audio_source_match":{"checked":true,"matched":true}}',
                encoding="utf-8",
            )
            (meta_dir / f"{slug}.step5.json").write_text(
                (
                    '{"video_url":"https://youtube.com/watch?v=uploadVerified",'
                    '"sync_checks":{"overall_passed":true,'
                    '"pre_upload":{"checks":{"visual_sync":{"status":"passed"}}}}}'
                ),
                encoding="utf-8",
            )

            with api._jobs_lock:
                existing = api.Job(
                    id="job-upload-verified",
                    query="hello world",
                    slug=slug,
                    created_at=now_ts - 90,
                    finished_at=now_ts - 30,
                    status="succeeded",
                    youtube_video_url="https://youtube.com/watch?v=uploadVerified",
                    options=options,
                )
                api._jobs[existing.id] = existing

            with (
                mock.patch.object(api, "META_DIR", meta_dir),
                mock.patch.object(api, "REUSE_UPLOADED_ALLOW_LEGACY_UNVERIFIED", False),
                mock.patch.object(api, "_persist_jobs") as persist_mock,
                mock.patch.object(api, "_executor") as executor_mock,
            ):
                got = api.create_job(req, self._mock_request())

            self.assertEqual(got.id, existing.id)
            persist_mock.assert_not_called()
            executor_mock.submit.assert_not_called()

    def test_prune_jobs_history_keeps_uploaded_video_entries(self) -> None:
        now_ts = api._now_ts()
        uploaded = api.Job(
            id="job-keep-uploaded",
            query="hello world",
            slug=api.slugify("hello world"),
            created_at=now_ts - 200,
            finished_at=now_ts - 180,
            status="succeeded",
            youtube_video_url="https://youtube.com/watch?v=keepUploaded",
            options={"upload": True},
        )
        regular_old = api.Job(
            id="job-drop-old",
            query="regular old",
            slug=api.slugify("regular old"),
            created_at=now_ts - 150,
            finished_at=now_ts - 140,
            status="succeeded",
            options={"upload": False},
        )
        regular_new = api.Job(
            id="job-drop-new",
            query="regular new",
            slug=api.slugify("regular new"),
            created_at=now_ts - 120,
            finished_at=now_ts - 110,
            status="succeeded",
            options={"upload": False},
        )

        with api._jobs_lock:
            api._jobs[uploaded.id] = uploaded
            api._jobs[regular_old.id] = regular_old
            api._jobs[regular_new.id] = regular_new

            with (
                mock.patch.object(api, "MAX_JOBS_HISTORY", 1),
                mock.patch.object(api, "MAX_UPLOADED_JOBS_HISTORY", 5),
            ):
                api._prune_jobs_history()

        self.assertIn(uploaded.id, api._jobs)
        self.assertNotIn(regular_old.id, api._jobs)
        self.assertNotIn(regular_new.id, api._jobs)

    def test_run_job_decrements_active_counter_on_runtime_cookie_setup_failure(self) -> None:
        job = api.Job(
            id="job-runtime-cookie-setup-failure",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=api._now_ts(),
            status="queued",
            options={},
        )
        with api._jobs_lock:
            api._jobs[job.id] = job
            api._slug_to_job_id[job.slug] = job.id
            api._active_job_count_cached = 1

        with (
            mock.patch.object(api, "_write_runtime_cookies_file", side_effect=RuntimeError("boom")),
            mock.patch.object(api, "_persist_jobs"),
        ):
            api._run_job(job, runtime_cookies_payload="")

        self.assertEqual(job.status, "failed")
        with api._jobs_lock:
            self.assertEqual(api._active_job_count_cached, 0)

    def test_sweep_stale_jobs_cancels_jobs_older_than_limit(self) -> None:
        now_ts = api._now_ts()
        job = api.Job(
            id="job-stale-running",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=now_ts - 2000,
            started_at=now_ts - 1200,
            status="running",
            step_timestamps={"queued_at": now_ts - 1300},
        )
        with api._jobs_lock:
            api._jobs[job.id] = job
            api._slug_to_job_id[job.slug] = job.id
            api._active_job_count_cached = 1

        with (
            mock.patch.object(api, "STALE_JOB_MAX_AGE_SEC", 900.0),
            mock.patch.object(api, "_persist_jobs") as persist_mock,
            mock.patch.object(api, "_cancel_gpu_worker_job") as cancel_mock,
        ):
            cancelled = api._sweep_stale_jobs_once(now_ts=now_ts)

        self.assertEqual(cancelled, 1)
        self.assertEqual(job.status, "cancelled")
        self.assertEqual(job.stage, "timeout")
        self.assertIsNotNone(job.cancelled_at)
        self.assertIsNotNone(job.finished_at)
        persist_mock.assert_called_once()
        cancel_mock.assert_called_once_with(job.id)
        with api._jobs_lock:
            self.assertEqual(api._active_job_count_cached, 0)

    def test_sweep_stale_jobs_ignores_fresh_jobs(self) -> None:
        now_ts = api._now_ts()
        job = api.Job(
            id="job-fresh-running",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=now_ts - 60,
            started_at=now_ts - 30,
            status="running",
            step_timestamps={"queued_at": now_ts - 90},
        )
        with api._jobs_lock:
            api._jobs[job.id] = job
            api._slug_to_job_id[job.slug] = job.id
            api._active_job_count_cached = 1

        with (
            mock.patch.object(api, "STALE_JOB_MAX_AGE_SEC", 900.0),
            mock.patch.object(api, "_persist_jobs") as persist_mock,
            mock.patch.object(api, "_cancel_gpu_worker_job") as cancel_mock,
        ):
            cancelled = api._sweep_stale_jobs_once(now_ts=now_ts)

        self.assertEqual(cancelled, 0)
        self.assertEqual(job.status, "running")
        persist_mock.assert_not_called()
        cancel_mock.assert_not_called()
        with api._jobs_lock:
            self.assertEqual(api._active_job_count_cached, 1)

    def test_sweep_stale_jobs_skips_synthetic_jobs_without_real_epoch_timestamps(self) -> None:
        now_ts = api._now_ts()
        job = api.Job(
            id="job-synthetic-old-running",
            query="The Beatles - Let It Be",
            slug=api.slugify("The Beatles - Let It Be"),
            created_at=1.0,
            started_at=None,
            status="running",
            step_timestamps={},
        )
        with api._jobs_lock:
            api._jobs[job.id] = job
            api._slug_to_job_id[job.slug] = job.id
            api._active_job_count_cached = 1

        with (
            mock.patch.object(api, "STALE_JOB_MAX_AGE_SEC", 900.0),
            mock.patch.object(api, "_persist_jobs") as persist_mock,
            mock.patch.object(api, "_cancel_gpu_worker_job") as cancel_mock,
        ):
            cancelled = api._sweep_stale_jobs_once(now_ts=now_ts)

        self.assertEqual(cancelled, 0)
        self.assertEqual(job.status, "running")
        persist_mock.assert_not_called()
        cancel_mock.assert_not_called()
        with api._jobs_lock:
            self.assertEqual(api._active_job_count_cached, 1)


@unittest.skipIf(api is None, "fastapi is not installed in this test environment")
class KaraoApiFailureReasonTests(unittest.TestCase):
    def test_extract_prefers_runtime_continuation_when_headline_is_generic(self) -> None:
        lines = [
            "Traceback (most recent call last):",
            "  File \"/app/scripts/main.py\", line 10, in <module>",
            "RuntimeError: MP3 download failed for query 'The Beatles - Let It Be'",
            "yt-dlp search failed",
        ]
        detail = api._extract_pipeline_failure_reason(lines, rc=1)
        self.assertEqual(detail, "yt-dlp search failed")

    def test_extract_returns_cookie_refresh_marker_from_continuation(self) -> None:
        lines = [
            "RuntimeError: MP3 download failed for query 'The Beatles - Let It Be'",
            "ERROR: [youtube] Sign in to confirm you're not a bot",
        ]
        detail = api._extract_pipeline_failure_reason(lines, rc=1)
        self.assertIn("COOKIE_REFRESH_REQUIRED", detail)

    def test_extract_keeps_specific_runtimeerror_detail(self) -> None:
        lines = [
            "RuntimeError: No synced lyrics found for query: 'The Beatles - Let It Be'",
        ]
        detail = api._extract_pipeline_failure_reason(lines, rc=1)
        self.assertEqual(detail, "No synced lyrics found for query: 'The Beatles - Let It Be'")

    def test_extract_keeps_missing_audio_runtimeerror_detail(self) -> None:
        lines = [
            "RuntimeError: No audio found for query: 'The Beatles - Let It Be'",
        ]
        detail = api._extract_pipeline_failure_reason(lines, rc=1)
        self.assertEqual(detail, "No audio found for query: 'The Beatles - Let It Be'")

    def test_extract_accepts_no_ids_runtime_continuation(self) -> None:
        lines = [
            "RuntimeError: MP3 download failed for query 'The Beatles - Let It Be'",
            "yt search returned no ids for 'The Beatles - Let It Be'",
        ]
        detail = api._extract_pipeline_failure_reason(lines, rc=1)
        self.assertEqual(detail, "yt search returned no ids for 'The Beatles - Let It Be'")
