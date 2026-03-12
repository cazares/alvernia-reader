import json
import tempfile
import unittest
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts.common import Paths
from scripts import step3_sync as step3


class Step3SyncTests(unittest.TestCase):
    def test_extract_event_supports_multiple_shapes(self) -> None:
        self.assertEqual(step3._extract_event(SimpleNamespace(t=1.25, text="a")), (1.25, "a"))
        self.assertEqual(step3._extract_event(SimpleNamespace(time=2.5, text="b")), (2.5, "b"))
        self.assertEqual(step3._extract_event((3.75, "c")), (3.75, "c"))
        self.assertEqual(step3._extract_event({"t": 4.0, "text": "d"}), (4.0, "d"))
        with self.assertRaises(TypeError):
            step3._extract_event(object())

    def test_write_timings_csv_writes_expected_schema(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "out.csv"
            n = step3._write_timings_csv(p, [(1.2, "hello"), {"time": 2.3, "text": "world"}])
            self.assertEqual(n, 2)
            text = p.read_text(encoding="utf-8")
            self.assertIn("line_index,time_secs,text", text)
            self.assertIn("0,1.200,hello", text)
            self.assertIn("1,2.300,world", text)

    def test_choose_auto_offset_anchors_respects_accuracy_level(self) -> None:
        events = [
            SimpleNamespace(t=10.0, text="one two three"),
            SimpleNamespace(t=20.0, text="four five six"),
            SimpleNamespace(t=30.0, text="seven eight nine"),
            SimpleNamespace(t=40.0, text="ten eleven twelve"),
            SimpleNamespace(t=50.0, text="thirteen fourteen fifteen"),
            SimpleNamespace(t=60.0, text="sixteen seventeen eighteen"),
        ]
        with mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})):
            level1 = step3._choose_auto_offset_anchors(Path("song.lrc"), "song_slug", 1)
            level2 = step3._choose_auto_offset_anchors(Path("song.lrc"), "song_slug", 2)
            level3 = step3._choose_auto_offset_anchors(Path("song.lrc"), "song_slug", 3)
            level3_again = step3._choose_auto_offset_anchors(Path("song.lrc"), "song_slug", 3)

        self.assertEqual(len(level1), 1)
        self.assertEqual(len(level2), 2)
        self.assertEqual(len(level3), 3)
        self.assertEqual(level3, level3_again)
        self.assertEqual(len({f"{float(t):.3f}" for t in level2}), len(level2))
        self.assertEqual(len({f"{float(t):.3f}" for t in level3}), len(level3))

    def test_choose_auto_offset_anchors_respects_avoid_keys(self) -> None:
        events = [
            SimpleNamespace(t=10.0, text="one two three"),
            SimpleNamespace(t=20.0, text="four five six"),
            SimpleNamespace(t=30.0, text="seven eight nine"),
            SimpleNamespace(t=40.0, text="ten eleven twelve"),
            SimpleNamespace(t=50.0, text="thirteen fourteen fifteen"),
            SimpleNamespace(t=60.0, text="sixteen seventeen eighteen"),
        ]
        with mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})):
            anchors = step3._choose_auto_offset_anchors(
                Path("song.lrc"),
                "song_slug",
                3,
                avoid_anchor_keys={"20.000", "40.000"},
            )

        keys = {f"{float(t):.3f}" for t in anchors}
        self.assertEqual(len(keys), len(anchors))
        self.assertNotIn("20.000", keys)
        self.assertNotIn("40.000", keys)

    def test_choose_calibration_anchors_respects_level(self) -> None:
        events = [
            SimpleNamespace(t=10.0, text="one two three"),
            SimpleNamespace(t=20.0, text="four five six"),
            SimpleNamespace(t=30.0, text="seven eight nine"),
            SimpleNamespace(t=40.0, text="ten eleven twelve"),
            SimpleNamespace(t=50.0, text="thirteen fourteen fifteen"),
            SimpleNamespace(t=60.0, text="sixteen seventeen eighteen"),
        ]
        with mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})):
            level1 = step3._choose_calibration_anchors(Path("song.lrc"), 1)
            level2 = step3._choose_calibration_anchors(Path("song.lrc"), 2)

        self.assertEqual(len(level1), 2)
        self.assertEqual(len(level2), 3)
        self.assertAlmostEqual(float(level1[0]), 10.0, places=3)
        self.assertAlmostEqual(float(level1[-1]), 60.0, places=3)

    def test_estimate_auto_offset_passes_optional_whisper_args(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lrc_path = root / "song.lrc"
            audio_path = root / "song.mp3"
            lrc_path.write_text("[00:01.00]hello\n", encoding="utf-8")
            audio_path.write_bytes(b"mp3")

            with (
                mock.patch.dict("os.environ", {"MIXTERIOSO_WHISPER_ARGS": "--gpu-layers 35 --flash-attn"}, clear=False),
                mock.patch("scripts.lrc_offset_whisper._find_whispercpp_bin", return_value="/tmp/whisper-cli"),
                mock.patch("scripts.lrc_offset_whisper._find_model", return_value="/tmp/model.bin"),
                mock.patch("scripts.step3_sync.resolve_ffmpeg_bin", return_value=Path("ffmpeg")),
                mock.patch("scripts.step3_sync._guess_auto_offset_language", return_value="en"),
                mock.patch("scripts.lrc_offset_whisper.estimate_offset", return_value=(0.321, 0.95)) as estimate_mock,
            ):
                got = step3._estimate_auto_offset(lrc_path=lrc_path, audio_path=audio_path, language="auto")

            self.assertAlmostEqual(got[0], 0.321)
            kwargs = estimate_mock.call_args.kwargs
            self.assertEqual(kwargs["whisper_extra_args"], ["--gpu-layers", "35", "--flash-attn"])
            self.assertEqual(kwargs["language"], "en")

    def test_guess_auto_offset_language_detects_spanish_markers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lrc = Path(td) / "song.lrc"
            lrc.write_text(
                "[00:01.00]que la noche y el amor\n[00:02.00]corazon para ti\n",
                encoding="utf-8",
            )
            with mock.patch.object(step3, "_AUTO_OFFSET_LANG_CACHE", {}):
                self.assertEqual(step3._guess_auto_offset_language(lrc), "es")

    def test_step3_sync_fails_when_lrc_missing_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "STRICT_REQUIRE_LYRICS", True),
                self.assertRaises(RuntimeError),
            ):
                step3.step3_sync(paths=None, slug="song", flags=None)

            self.assertFalse((timings_dir / "song.csv").exists())
            self.assertFalse((timings_dir / "song.raw.csv").exists())

    def test_step3_sync_skips_when_lrc_missing_if_strict_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "STRICT_REQUIRE_LYRICS", False),
            ):
                step3.step3_sync(paths=None, slug="song", flags=None)

            self.assertFalse((timings_dir / "song.csv").exists())
            self.assertFalse((timings_dir / "song.raw.csv").exists())

    def test_step3_sync_reads_lrc_and_writes_both_csv_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n[00:02.00]World\n", encoding="utf-8")

            events = [SimpleNamespace(t=1.0, text="Hello"), SimpleNamespace(t=2.0, text="World")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
            ):
                step3.step3_sync(paths=None, slug="song", flags=None)

            raw_csv = (timings_dir / "song.raw.csv").read_text(encoding="utf-8")
            final_csv = (timings_dir / "song.csv").read_text(encoding="utf-8")
            self.assertIn("0,1.000,Hello", raw_csv)
            self.assertIn("1,2.000,World", final_csv)

    def test_step3_sync_fails_when_lrc_has_no_nonblank_lines_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            (timings_dir / "song.lrc").write_text("[00:01.00]\n", encoding="utf-8")
            events = [SimpleNamespace(t=1.0, text="")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "STRICT_REQUIRE_LYRICS", True),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                self.assertRaises(RuntimeError),
            ):
                step3.step3_sync(paths=None, slug="song", flags=None)

    def test_step3_sync_writes_auto_offset_when_estimation_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n[00:02.00]World\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            events = [SimpleNamespace(t=1.0, text="Hello"), SimpleNamespace(t=2.0, text="World")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset", return_value=(1.234, 0.95)) as estimate_mock,
                mock.patch.dict("os.environ", {"KARAOKE_AUTO_OFFSET_ENABLED": "1"}, clear=False),
            ):
                step3.step3_sync(paths=paths, slug="song", flags=None, language="en")

            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "1.234")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied")
            self.assertAlmostEqual(float(meta_payload.get("applied_offset_s")), 1.234, places=3)
            self.assertEqual(int(meta_payload.get("successful_samples")), 1)
            samples = meta_payload.get("samples") or []
            self.assertEqual(len(samples), 1)
            self.assertAlmostEqual(float(samples[0].get("offset_s")), 1.234, places=3)
            estimate_mock.assert_called_once()
            kwargs = estimate_mock.call_args.kwargs
            self.assertEqual(kwargs["lrc_path"], timings_dir / "song.lrc")
            self.assertEqual(kwargs["audio_path"], paths.mixes / "song.mp3")
            self.assertEqual(kwargs["language"], "en")

    def test_maybe_write_auto_offset_applies_large_gap_fallback_when_samples_fail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            lrc_lines = [
                "[00:44.56]line one",
                "[00:48.00]line two",
                "[00:52.00]line three",
                "[00:56.00]line four",
                "[01:00.00]line five",
                "[01:04.00]line six",
                "[01:08.00]line seven",
                "[01:12.00]line eight",
            ]
            (timings_dir / "song.lrc").write_text("\n".join(lrc_lines) + "\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_run_auto_offset_samples", return_value=[]),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=1,
                    calibration_level=0,
                )

            applied = (timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip()
            self.assertEqual(applied, "-44.560")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied_large_lead_gap_fallback")
            self.assertAlmostEqual(float(meta_payload.get("applied_offset_s")), -44.56, places=2)

    def test_maybe_write_auto_offset_applies_positive_intro_gap_fallback_when_intro_scan_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            lrc_lines = [
                "[00:36.08]Up on Melancholy Hill",
                "[00:39.00]There's a plastic tree",
                "[00:42.03]Are you here with me?",
                "[00:48.07]Just looking out on the day",
                "[00:51.06]Of another dream",
                "[00:54.06]Well you can't get what you want",
                "[00:59.01]But you can get me",
                "[01:02.04]So let's set out to sea, love",
            ]
            (timings_dir / "song.lrc").write_text("\n".join(lrc_lines) + "\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_run_auto_offset_samples", return_value=[]),
                mock.patch.object(step3, "_estimate_intro_gap_offset", return_value=(35.92, 0.88)),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            applied = (timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip()
            self.assertEqual(applied, "35.920")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied_positive_intro_gap_fallback")
            self.assertAlmostEqual(float(meta_payload.get("applied_offset_s")), 35.92, places=2)
            self.assertTrue(bool(meta_payload.get("positive_intro_gap_detected")))
            self.assertEqual(meta_payload.get("fallback_reason"), "no_successful_samples")

    def test_maybe_write_auto_offset_applies_positive_intro_fast_path_before_sampling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            lrc_lines = [
                "[00:00.58]Shada",
                "[00:10.64]Sali pa despejarme",
                "[00:13.91]Y ya cansada de estar apagada",
                "[00:18.78]Cambie de amigos",
                "[00:22.32]A las malas me toco aprender",
                "[01:19.08]Otra linea",
                "[02:40.99]Otra mas",
                "[03:30.11]Final",
            ]
            (timings_dir / "song.lrc").write_text("\n".join(lrc_lines) + "\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_estimate_intro_gap_offset", return_value=(22.48, 0.86)) as intro_mock,
                mock.patch.object(step3, "_run_auto_offset_samples") as sample_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="es",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            intro_mock.assert_called_once()
            sample_mock.assert_not_called()
            applied = (timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip()
            self.assertEqual(applied, "22.480")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied_positive_intro_gap_fast_path")
            self.assertAlmostEqual(float(meta_payload.get("applied_offset_s")), 22.48, places=2)
            self.assertTrue(bool(meta_payload.get("positive_intro_gap_detected")))
            self.assertTrue(bool(meta_payload.get("positive_intro_gap_fast_path")))

    def test_maybe_write_auto_offset_skips_positive_intro_fast_path_for_dense_early_lyrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            lrc_lines = [
                "[00:00.05]Shoot me",
                "[00:02.59]Shoot me",
                "[00:05.72]Shoot me",
                "[00:08.38]Shoot me",
                "[00:11.83]Here come old flat-top",
                "[00:15.10]He come groovin up slowly",
                "[00:18.45]He got joo-joo eyeball",
                "[00:22.10]He one holy roller",
            ]
            (timings_dir / "song.lrc").write_text("\n".join(lrc_lines) + "\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")
            sample_rows = [
                {
                    "index": 1,
                    "status": "ok",
                    "anchor_time_s": 2.59,
                    "anchor_key": "2.590",
                    "offset_s": 0.2,
                    "confidence": 0.95,
                    "pass": "default",
                },
                {
                    "index": 2,
                    "status": "ok",
                    "anchor_time_s": 11.83,
                    "anchor_key": "11.830",
                    "offset_s": 0.2,
                    "confidence": 0.93,
                    "pass": "default",
                },
                {
                    "index": 3,
                    "status": "ok",
                    "anchor_time_s": 22.10,
                    "anchor_key": "22.100",
                    "offset_s": 0.2,
                    "confidence": 0.92,
                    "pass": "default",
                },
            ]

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_estimate_intro_gap_offset") as intro_mock,
                mock.patch.object(step3, "_run_auto_offset_samples", return_value=sample_rows) as sample_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            intro_mock.assert_not_called()
            self.assertEqual(sample_mock.call_count, 2)
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "0.200")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied")
            self.assertFalse(bool(meta_payload.get("positive_intro_gap_fast_path")))

    def test_step3_sync_passes_auto_offset_accuracy_to_writer(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            timings_dir = Path(td)
            (timings_dir / "song.lrc").write_text("[00:01.00]one two three\n[00:02.00]four five six\n", encoding="utf-8")
            events = [SimpleNamespace(t=1.0, text="one two three"), SimpleNamespace(t=2.0, text="four five six")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_maybe_write_auto_offset") as maybe_mock,
            ):
                step3.step3_sync(
                    paths=None,
                    slug="song",
                    flags=None,
                    run_auto_offset=True,
                    auto_offset_default_enabled=True,
                    auto_offset_accuracy=3,
                )

            maybe_mock.assert_called_once()
            self.assertEqual(int(maybe_mock.call_args.kwargs["accuracy_level"]), 3)
            self.assertEqual(int(maybe_mock.call_args.kwargs["calibration_level"]), 0)

    def test_maybe_write_auto_offset_uses_only_calibration_anchors_when_tune_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]one two three\n[00:40.00]four five six\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[1.0, 40.0]) as cal_mock,
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[20.0]) as tune_mock,
                mock.patch.object(step3, "_estimate_auto_offset", return_value=(0.5, 0.95)),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_BATCH_WHISPER": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=0,
                    calibration_level=1,
                )

            cal_mock.assert_called_once()
            tune_mock.assert_not_called()
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "0.500")

    def test_maybe_write_auto_offset_combines_calibration_and_tune_anchors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]one two three\n[00:40.00]four five six\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[1.0, 40.0]) as cal_mock,
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[20.0]) as tune_mock,
                mock.patch.object(step3, "_estimate_auto_offset", side_effect=[(0.1, 0.95), (0.2, 0.95), (0.3, 0.95)]),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_BATCH_WHISPER": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=1,
                )

            cal_mock.assert_called_once()
            tune_mock.assert_called_once()
            tune_kwargs = tune_mock.call_args.kwargs
            self.assertEqual(set(tune_kwargs.get("avoid_anchor_keys", set())), {"1.000", "40.000"})
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "0.200")

    def test_maybe_write_auto_offset_progressive_exit_skips_tune_anchors_when_calibration_converges(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text(
                "[00:01.00]one two three\n[00:40.00]four five six\n[01:20.00]seven eight nine\n",
                encoding="utf-8",
            )
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            calibration_rows = [
                {
                    "index": 1,
                    "status": "ok",
                    "anchor_time_s": 1.0,
                    "anchor_key": "1.000",
                    "offset_s": 0.10,
                    "confidence": 0.92,
                    "pass": "default",
                },
                {
                    "index": 2,
                    "status": "ok",
                    "anchor_time_s": 40.0,
                    "anchor_key": "40.000",
                    "offset_s": 0.12,
                    "confidence": 0.90,
                    "pass": "default",
                },
                {
                    "index": 3,
                    "status": "ok",
                    "anchor_time_s": 80.0,
                    "anchor_key": "80.000",
                    "offset_s": 0.08,
                    "confidence": 0.89,
                    "pass": "default",
                },
            ]

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[1.0, 40.0, 80.0]),
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[20.0, 60.0, 100.0]),
                mock.patch.object(step3, "_run_auto_offset_samples", return_value=calibration_rows) as sample_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            sample_mock.assert_called_once()
            first_call = sample_mock.call_args_list[0]
            self.assertEqual(list(first_call.kwargs["anchors"]), [1.0, 40.0, 80.0])
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "0.100")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertTrue(bool(meta_payload.get("progressive_early_exit")))
            self.assertEqual(meta_payload.get("mode_resolution"), "calibration_only_progressive_early_exit")
            self.assertEqual(int(meta_payload.get("skipped_tune_anchor_count") or 0), 3)

    def test_maybe_write_auto_offset_progressive_exit_escalates_when_calibration_uncertain(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text(
                "[00:01.00]one two three\n[00:40.00]four five six\n[01:20.00]seven eight nine\n",
                encoding="utf-8",
            )
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            calibration_rows = [
                {
                    "index": 1,
                    "status": "ok",
                    "anchor_time_s": 1.0,
                    "anchor_key": "1.000",
                    "offset_s": 0.10,
                    "confidence": 0.92,
                    "pass": "default",
                },
                {
                    "index": 2,
                    "status": "ok",
                    "anchor_time_s": 40.0,
                    "anchor_key": "40.000",
                    "offset_s": 1.20,
                    "confidence": 0.90,
                    "pass": "default",
                },
                {
                    "index": 3,
                    "status": "error",
                    "anchor_time_s": 80.0,
                    "anchor_key": "80.000",
                    "error": "boom",
                    "pass": "default",
                },
            ]
            tune_rows = [
                {
                    "index": 4,
                    "status": "ok",
                    "anchor_time_s": 20.0,
                    "anchor_key": "20.000",
                    "offset_s": 0.20,
                    "confidence": 0.95,
                    "pass": "default",
                },
                {
                    "index": 5,
                    "status": "ok",
                    "anchor_time_s": 60.0,
                    "anchor_key": "60.000",
                    "offset_s": 0.25,
                    "confidence": 0.94,
                    "pass": "default",
                },
                {
                    "index": 6,
                    "status": "ok",
                    "anchor_time_s": 100.0,
                    "anchor_key": "100.000",
                    "offset_s": 0.22,
                    "confidence": 0.93,
                    "pass": "default",
                },
            ]

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[1.0, 40.0, 80.0]),
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[20.0, 60.0, 100.0]),
                mock.patch.object(
                    step3,
                    "_run_auto_offset_samples",
                    side_effect=[calibration_rows, tune_rows],
                ) as sample_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            self.assertEqual(sample_mock.call_count, 2)
            self.assertEqual(list(sample_mock.call_args_list[0].kwargs["anchors"]), [1.0, 40.0, 80.0])
            self.assertEqual(list(sample_mock.call_args_list[1].kwargs["anchors"]), [20.0, 60.0, 100.0])
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "0.200")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertFalse(bool(meta_payload.get("progressive_early_exit")))
            self.assertEqual(int(meta_payload.get("skipped_tune_anchor_count") or 0), 0)

    def test_maybe_write_auto_offset_prefers_early_consensus_for_late_opening_song(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text(
                "\n".join(
                    [
                        "[00:20.27]Psychic spies from China try to steal your mind's elation",
                        "[00:49.72]The Sun may rise in the East, at least it's settled in the final location",
                        "[02:23.90]Space may be the final frontier, but it's made in a Hollywood basement",
                        "[02:29.11]And Cobain, can you hear the spheres, singing songs off station to station?",
                        "[04:47.62]Sicker than the rest, there is no test, but this is what you're craving",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            calibration_rows = [
                {
                    "index": 1,
                    "status": "ok",
                    "anchor_time_s": 20.27,
                    "anchor_key": "20.270",
                    "offset_s": 3.35,
                    "confidence": 0.57,
                    "pass": "default",
                },
                {
                    "index": 2,
                    "status": "ok",
                    "anchor_time_s": 149.11,
                    "anchor_key": "149.110",
                    "offset_s": -4.727,
                    "confidence": 0.60,
                    "pass": "default",
                },
                {
                    "index": 3,
                    "status": "ok",
                    "anchor_time_s": 316.8,
                    "anchor_key": "316.800",
                    "offset_s": 1.87,
                    "confidence": 0.17,
                    "pass": "default",
                },
            ]
            tune_rows = [
                {
                    "index": 4,
                    "status": "ok",
                    "anchor_time_s": 49.72,
                    "anchor_key": "49.720",
                    "offset_s": 3.132,
                    "confidence": 0.52,
                    "pass": "default",
                },
                {
                    "index": 5,
                    "status": "ok",
                    "anchor_time_s": 143.90,
                    "anchor_key": "143.900",
                    "offset_s": -3.341,
                    "confidence": 0.72,
                    "pass": "default",
                },
                {
                    "index": 6,
                    "status": "ok",
                    "anchor_time_s": 287.62,
                    "anchor_key": "287.620",
                    "offset_s": -11.05,
                    "confidence": 0.37,
                    "pass": "default",
                },
            ]

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[20.27, 149.11, 316.8]),
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[49.72, 143.90, 287.62]),
                mock.patch.object(step3, "_run_auto_offset_samples", side_effect=[calibration_rows, tune_rows]),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=3,
                    calibration_level=2,
                )

            applied = float((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip())
            self.assertGreater(applied, 3.0)
            self.assertLess(applied, 3.4)
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("mode_resolution"), "prefer_early_consensus_late_opening_disagreement")
            self.assertEqual(meta_payload.get("early_consensus_reason"), "large_sign_disagreement")

    def test_step3_sync_skips_auto_offset_when_no_audio(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n", encoding="utf-8")

            events = [SimpleNamespace(t=1.0, text="Hello")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset") as estimate_mock,
            ):
                step3.step3_sync(paths=paths, slug="song", flags=None, language="auto")

            estimate_mock.assert_not_called()
            self.assertFalse((timings_dir / "song.offset.auto").exists())

    def test_step3_sync_does_not_fail_when_auto_offset_estimation_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n", encoding="utf-8")
            (paths.mixes / "song.wav").write_bytes(b"wav")

            events = [SimpleNamespace(t=1.0, text="Hello")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset", side_effect=RuntimeError("boom")),
            ):
                step3.step3_sync(paths=paths, slug="song", flags=None, language="auto")

            self.assertTrue((timings_dir / "song.csv").exists())
            self.assertFalse((timings_dir / "song.offset.auto").exists())

    def test_step3_sync_respects_auto_offset_disable_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            events = [SimpleNamespace(t=1.0, text="Hello")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset") as estimate_mock,
                mock.patch.dict("os.environ", {"KARAOKE_AUTO_OFFSET_ENABLED": "0"}, clear=False),
            ):
                step3.step3_sync(paths=paths, slug="song", flags=None, language="auto")

            estimate_mock.assert_not_called()
            self.assertFalse((timings_dir / "song.offset.auto").exists())

    def test_step3_sync_lite_skips_auto_offset_when_lite_auto_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n[00:02.00]World\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")
            (timings_dir / "song.offset.auto").write_text("3.250\n", encoding="utf-8")
            (timings_dir / "song.offset.auto.meta.json").write_text('{"status":"stale"}', encoding="utf-8")

            events = [SimpleNamespace(t=1.0, text="Hello"), SimpleNamespace(t=2.0, text="World")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset") as estimate_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "MIXTERIOSO_STEP3_LITE_AUTO_OFFSET": "0",
                    },
                    clear=False,
                ),
            ):
                step3.step3_sync_lite(paths=paths, slug="song", flags=None, language="en")

            self.assertTrue((timings_dir / "song.csv").exists())
            self.assertFalse((timings_dir / "song.raw.csv").exists())
            self.assertFalse((timings_dir / "song.offset.auto").exists())
            self.assertFalse((timings_dir / "song.offset.auto.meta.json").exists())
            estimate_mock.assert_not_called()

    def test_step3_sync_lite_writes_auto_offset_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]Hello\n[00:02.00]World\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            events = [SimpleNamespace(t=1.0, text="Hello"), SimpleNamespace(t=2.0, text="World")]
            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch("scripts.lrc_utils.parse_lrc", return_value=(events, {})),
                mock.patch.object(step3, "_estimate_auto_offset", return_value=(-1.5, 0.95)) as estimate_mock,
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "MIXTERIOSO_STEP3_LITE_AUTO_OFFSET": "1",
                    },
                    clear=False,
                ),
            ):
                step3.step3_sync_lite(paths=paths, slug="song", flags=None, language="en")

            self.assertTrue((timings_dir / "song.csv").exists())
            self.assertFalse((timings_dir / "song.raw.csv").exists())
            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "-1.500")
            estimate_mock.assert_called_once()

    def test_resolve_audio_for_offset_uses_step1_meta_audio_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            timings_dir = root / "timings"
            meta_dir = root / "meta"
            mp3s_dir = root / "mp3s"
            timings_dir.mkdir(parents=True, exist_ok=True)
            meta_dir.mkdir(parents=True, exist_ok=True)
            mp3s_dir.mkdir(parents=True, exist_ok=True)

            mp4_path = mp3s_dir / "song.mp4"
            mp4_path.write_bytes(b"mp4")
            (meta_dir / "song.step1.json").write_text(
                json.dumps({"audio_path": str(mp4_path)}),
                encoding="utf-8",
            )

            with mock.patch.object(step3, "TIMINGS_DIR", timings_dir):
                got = step3._resolve_audio_for_offset(paths=None, slug="song")

            self.assertEqual(got, mp4_path)

    def test_maybe_write_auto_offset_marks_manual_recommended_on_outlier_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]one two three\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[1.0]),
                mock.patch.object(step3, "_estimate_auto_offset", return_value=(15.0, 0.99)),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_MAX_SAMPLE_ABS_SECS": "12",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=1,
                    calibration_level=0,
                )

            self.assertFalse((timings_dir / "song.offset.auto").exists())
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "no_successful_samples")
            self.assertTrue(bool(meta_payload.get("manual_offset_recommended")))

    def test_maybe_write_auto_offset_requires_min_selected_samples(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text("[00:01.00]one two three\n", encoding="utf-8")
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_auto_offset_anchors", return_value=[1.0, 2.0, 3.0]),
                mock.patch.object(
                    step3,
                    "_estimate_auto_offset",
                    side_effect=[(0.55, 0.95), RuntimeError("boom"), RuntimeError("boom")],
                ),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_BATCH_WHISPER": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=1,
                    calibration_level=0,
                )

            self.assertFalse((timings_dir / "song.offset.auto").exists())
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "insufficient_selected_samples")
            self.assertTrue(bool(meta_payload.get("manual_offset_recommended")))

    def test_maybe_write_auto_offset_dampens_weak_single_calibration_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(root=root)
            paths.ensure()
            timings_dir = paths.timings
            (timings_dir / "song.lrc").write_text(
                "[00:20.27]line one\n[05:16.80]line two\n",
                encoding="utf-8",
            )
            (paths.mixes / "song.mp3").write_bytes(b"mp3")

            sample_rows = [
                {
                    "index": 1,
                    "status": "ok",
                    "anchor_time_s": 20.27,
                    "anchor_key": "20.270",
                    "offset_s": 3.85,
                    "confidence": 0.56,
                    "pass": "default",
                },
                {
                    "index": 2,
                    "status": "error",
                    "anchor_time_s": 316.8,
                    "anchor_key": "316.800",
                    "error": "Insufficient alignment matches (0)",
                    "pass": "default",
                },
            ]

            with (
                mock.patch.object(step3, "TIMINGS_DIR", timings_dir),
                mock.patch.object(step3, "_choose_calibration_anchors", return_value=[20.27, 316.8]),
                mock.patch.object(step3, "_run_auto_offset_samples", return_value=sample_rows),
                mock.patch.dict(
                    "os.environ",
                    {
                        "KARAOKE_AUTO_OFFSET_ENABLED": "1",
                        "KARAOKE_AUTO_OFFSET_REUSE_EXISTING": "0",
                    },
                    clear=False,
                ),
            ):
                step3._maybe_write_auto_offset(
                    paths=paths,
                    slug="song",
                    language="en",
                    default_enabled=True,
                    force_refresh=True,
                    accuracy_level=0,
                    calibration_level=1,
                )

            self.assertEqual((timings_dir / "song.offset.auto").read_text(encoding="utf-8").strip(), "2.500")
            meta_payload = json.loads((timings_dir / "song.offset.auto.meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta_payload.get("status"), "applied")
            self.assertTrue(bool(meta_payload.get("weak_single_sample_damped")))
            self.assertAlmostEqual(float(meta_payload.get("raw_aggregate_offset_s")), 3.85, places=2)
            self.assertAlmostEqual(float(meta_payload.get("applied_offset_s")), 2.5, places=2)

    def test_run_auto_offset_samples_prefers_batch_estimator_for_multiple_anchors(self) -> None:
        with (
            mock.patch.object(
                step3,
                "_estimate_auto_offset_batch",
                return_value=[
                    {"anchor_time_s": 1.0, "offset_s": 0.1, "confidence": 0.9, "error": None},
                    {"anchor_time_s": 2.0, "offset_s": 0.2, "confidence": 0.8, "error": None},
                ],
            ) as batch_mock,
            mock.patch.object(step3, "_estimate_auto_offset") as single_mock,
            mock.patch.dict("os.environ", {"KARAOKE_AUTO_OFFSET_BATCH_WHISPER": "1"}, clear=False),
        ):
            rows = step3._run_auto_offset_samples(
                lrc_path=Path("song.lrc"),
                audio_path=Path("song.m4a"),
                language="en",
                anchors=[1.0, 2.0],
                max_sample_abs_offset=10.0,
                pass_label="default",
                index_start=1,
            )

        batch_mock.assert_called_once()
        single_mock.assert_not_called()
        self.assertEqual([row["status"] for row in rows], ["ok", "ok"])
        self.assertAlmostEqual(float(rows[0]["offset_s"]), 0.1, places=3)
        self.assertAlmostEqual(float(rows[1]["offset_s"]), 0.2, places=3)

    def test_run_auto_offset_samples_falls_back_to_serial_when_batch_fails(self) -> None:
        with (
            mock.patch.object(step3, "_estimate_auto_offset_batch", side_effect=RuntimeError("boom")),
            mock.patch.object(step3, "_estimate_auto_offset", side_effect=[(0.1, 0.9), (0.2, 0.8)]) as single_mock,
            mock.patch.dict("os.environ", {"KARAOKE_AUTO_OFFSET_BATCH_WHISPER": "1"}, clear=False),
        ):
            rows = step3._run_auto_offset_samples(
                lrc_path=Path("song.lrc"),
                audio_path=Path("song.m4a"),
                language="en",
                anchors=[1.0, 2.0],
                max_sample_abs_offset=10.0,
                pass_label="default",
                index_start=1,
            )

        self.assertEqual(single_mock.call_count, 2)
        self.assertEqual([row["status"] for row in rows], ["ok", "ok"])


if __name__ == "__main__":
    unittest.main()
