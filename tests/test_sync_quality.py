import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import sync_quality as sync


class SyncQualityTests(unittest.TestCase):
    def test_text_similarity_score(self) -> None:
        high = sync._text_similarity_score("Hello from the other side", "hello from the other side")
        low = sync._text_similarity_score("Hello from the other side", "completely different words")
        self.assertGreaterEqual(high, 0.9)
        self.assertLess(low, 0.6)

    def test_run_visual_sync_check_skips_when_missing_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            result = sync.run_visual_sync_check(
                video_path=base / "missing.mp4",
                timings_csv_path=base / "missing.csv",
            )
        self.assertEqual(result.get("status"), "skipped")
        self.assertIn("video missing", str(result.get("reason") or ""))

    def test_run_visual_sync_check_passes_with_mocked_ocr_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "video.mp4"
            csv_path = root / "song.csv"
            video.write_bytes(b"x" * 5000)
            csv_path.write_text(
                "line_index,time_secs,text\n"
                "0,8.000,Line One\n"
                "1,12.000,Line Two\n"
                "2,16.000,Line Three\n",
                encoding="utf-8",
            )

            ocr_outputs = [
                (True, "Line One"),
                (True, "Line Two"),
                (True, "Line Two"),
                (True, "Line Three"),
            ]
            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_SYNC_VISUAL_MIN_BOUNDARIES": "2",
                        "MIXTERIOSO_SYNC_VISUAL_MAX_BOUNDARIES": "2",
                        "MIXTERIOSO_SYNC_VISUAL_SIMILARITY_THRESHOLD": "0.40",
                        "MIXTERIOSO_SYNC_VISUAL_PASS_RATE_THRESHOLD": "0.50",
                        "MIXTERIOSO_SYNC_VISUAL_BEFORE_OFFSETS_SEC": "0.25",
                        "MIXTERIOSO_SYNC_VISUAL_AFTER_OFFSETS_SEC": "0.35",
                    },
                    clear=False,
                ),
                mock.patch("scripts.sync_quality.resolve_ffmpeg_bin", return_value=Path("/tmp/ffmpeg")),
                mock.patch(
                    "scripts.sync_quality.shutil.which",
                    side_effect=lambda name: "/tmp/fake" if name in {"tesseract", "/tmp/ffmpeg"} else None,
                ),
                mock.patch("scripts.sync_quality._extract_frame", return_value=(True, "")),
                mock.patch("scripts.sync_quality._ocr_frame_text", side_effect=ocr_outputs),
            ):
                result = sync.run_visual_sync_check(video_path=video, timings_csv_path=csv_path)

        self.assertEqual(result.get("status"), "passed")
        self.assertGreaterEqual(int(result.get("evaluated_boundaries") or 0), 2)
        self.assertGreaterEqual(float(result.get("pass_rate") or 0.0), 0.5)

    def test_run_visual_sync_check_passes_when_transition_is_delayed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "video.mp4"
            csv_path = root / "song.csv"
            video.write_bytes(b"x" * 5000)
            csv_path.write_text(
                "line_index,time_secs,text\n"
                "0,8.000,Line One\n"
                "1,12.000,Line Two\n"
                "2,16.000,Line Three\n",
                encoding="utf-8",
            )

            # Per boundary: before(older line), after-near(still old), after-late(new line)
            ocr_outputs = [
                (True, "Line One"),
                (True, "Line One"),
                (True, "Line Two"),
                (True, "Line Two"),
                (True, "Line Two"),
                (True, "Line Three"),
            ]
            with (
                mock.patch.dict(
                    "os.environ",
                    {
                        "MIXTERIOSO_SYNC_VISUAL_MIN_BOUNDARIES": "2",
                        "MIXTERIOSO_SYNC_VISUAL_MAX_BOUNDARIES": "2",
                        "MIXTERIOSO_SYNC_VISUAL_SIMILARITY_THRESHOLD": "0.40",
                        "MIXTERIOSO_SYNC_VISUAL_PASS_RATE_THRESHOLD": "0.50",
                        "MIXTERIOSO_SYNC_VISUAL_BEFORE_OFFSETS_SEC": "0.25",
                        "MIXTERIOSO_SYNC_VISUAL_AFTER_OFFSETS_SEC": "0.35,1.0",
                        "MIXTERIOSO_SYNC_VISUAL_TRANSITION_ADVANTAGE_MIN": "0.05",
                    },
                    clear=False,
                ),
                mock.patch("scripts.sync_quality.resolve_ffmpeg_bin", return_value=Path("/tmp/ffmpeg")),
                mock.patch(
                    "scripts.sync_quality.shutil.which",
                    side_effect=lambda name: "/tmp/fake" if name in {"tesseract", "/tmp/ffmpeg"} else None,
                ),
                mock.patch("scripts.sync_quality._extract_frame", return_value=(True, "")),
                mock.patch("scripts.sync_quality._ocr_frame_text", side_effect=ocr_outputs),
            ):
                result = sync.run_visual_sync_check(video_path=video, timings_csv_path=csv_path)

        self.assertEqual(result.get("status"), "passed")
        self.assertGreaterEqual(float(result.get("pass_rate") or 0.0), 0.5)

    def test_run_audio_offset_sync_check_skips_when_missing_lrc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            result = sync.run_audio_offset_sync_check(
                lrc_path=root / "missing.lrc",
                audio_path=root / "video.mp4",
            )
        self.assertEqual(result.get("status"), "skipped")
        self.assertIn("lrc missing", str(result.get("reason") or ""))

    def test_run_sync_quality_checks_merges_pre_and_post_timings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "song.mp4"
            timings = root / "song.csv"
            lrc = root / "song.lrc"
            downloaded = root / "downloaded.mp4"
            video.write_bytes(b"x" * 5000)
            downloaded.write_bytes(b"y" * 6000)
            timings.write_text("line_index,time_secs,text\n0,8.000,A\n1,12.000,B\n", encoding="utf-8")
            lrc.write_text("[00:08.00]A\n[00:12.00]B\n", encoding="utf-8")

            def _audio_side_effect(**kwargs):  # type: ignore[no-untyped-def]
                path = Path(kwargs["audio_path"])
                if path == video:
                    return {"name": "audio_offset", "status": "passed", "passed": True, "elapsed_ms": 1200, "elapsed_sec": 1.2}
                return {"name": "audio_offset", "status": "passed", "passed": True, "elapsed_ms": 1300, "elapsed_sec": 1.3}

            def _visual_side_effect(**kwargs):  # type: ignore[no-untyped-def]
                path = Path(kwargs["video_path"])
                if path == video:
                    return {"name": "visual_sync", "status": "passed", "passed": True, "elapsed_ms": 2100, "elapsed_sec": 2.1}
                return {"name": "visual_sync", "status": "passed", "passed": True, "elapsed_ms": 2200, "elapsed_sec": 2.2}

            with (
                mock.patch("scripts.sync_quality.run_audio_offset_sync_check", side_effect=_audio_side_effect),
                mock.patch("scripts.sync_quality.run_visual_sync_check", side_effect=_visual_side_effect),
                mock.patch("scripts.sync_quality._download_youtube_video_mp4", return_value=(downloaded, "")),
                mock.patch.dict("os.environ", {"MIXTERIOSO_SYNC_CHECK_POST_AUDIO": "1"}, clear=False),
            ):
                payload = sync.run_sync_quality_checks(
                    slug="song",
                    local_video_path=video,
                    youtube_video_url="https://youtube.com/watch?v=abc",
                    run_pre_upload=True,
                    run_post_upload=True,
                    timings_csv_path=timings,
                    lrc_path=lrc,
                    language="auto",
                )

        self.assertTrue(payload.get("overall_passed"))
        self.assertIn("pre_upload", payload)
        self.assertIn("post_upload", payload)
        timings_sec = payload.get("timings_sec") or {}
        self.assertIn("pre_upload.audio_offset", timings_sec)
        self.assertIn("post_upload.visual_sync", timings_sec)
        self.assertGreater(float(timings_sec.get("total") or 0.0), 0.0)

    def test_merge_sync_check_runs(self) -> None:
        pre = {
            "slug": "song",
            "elapsed_ms": 3000,
            "pre_upload": {"scope": "pre_upload", "passed": True, "elapsed_ms": 3000, "checks": {}},
        }
        post = {
            "slug": "song",
            "elapsed_ms": 5000,
            "post_upload": {"scope": "post_upload", "passed": False, "elapsed_ms": 5000, "checks": {}},
        }
        merged = sync.merge_sync_check_runs(pre, post)
        self.assertEqual(merged.get("slug"), "song")
        self.assertEqual(merged.get("overall_passed"), False)
        self.assertAlmostEqual(float(merged.get("elapsed_sec") or 0.0), 8.0, places=3)


if __name__ == "__main__":
    unittest.main()
