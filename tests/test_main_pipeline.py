import json
import os
import sys
import threading
import time
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from scripts import main as main_mod


class MainPipelineTests(unittest.TestCase):
    def test_auto_offset_summary_reports_no_auto_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            msg = main_mod._auto_offset_summary(paths, "artist_title")
        self.assertIn("Auto offset applied: +0.000s", msg)
        self.assertIn("no auto offset file", msg)

    def test_auto_offset_summary_reports_auto_value(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.timings / "artist_title.offset.auto").write_text("-1.250\n", encoding="utf-8")
            msg = main_mod._auto_offset_summary(paths, "artist_title")
        self.assertIn("Auto offset applied: -1.250s", msg)

    def test_auto_offset_summary_reports_manual_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.timings / "artist_title.offset.auto").write_text("-1.250\n", encoding="utf-8")
            (paths.timings / "artist_title.offset").write_text("+0.500\n", encoding="utf-8")
            msg = main_mod._auto_offset_summary(paths, "artist_title")
        self.assertIn("Auto offset applied: +0.000s", msg)
        self.assertIn("overridden by manual +0.500s", msg)

    def test_offset_summary_lines_include_sub_offsets_and_final_offset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.timings / "artist_title.offset.auto").write_text("-0.250\n", encoding="utf-8")
            (paths.timings / "artist_title.offset.auto.meta.json").write_text(
                json.dumps(
                    {
                        "status": "applied",
                        "samples": [
                            {"index": 1, "status": "ok", "anchor_time_s": 12.0, "offset_s": -0.200, "confidence": 0.93},
                            {"index": 2, "status": "ok", "anchor_time_s": 56.0, "offset_s": -0.300, "confidence": 0.91},
                        ],
                        "aggregate_offset_s": -0.250,
                        "aggregate_confidence": 0.92,
                    }
                ),
                encoding="utf-8",
            )
            (paths.meta / "artist_title.step4.offsets.json").write_text(
                json.dumps(
                    {
                        "offset_source": "auto",
                        "auto_offset_s": -0.250,
                        "manual_offset_s": 0.0,
                        "cli_offset_s": 0.500,
                        "base_offset_s": -0.250,
                        "resolved_offset_s": 0.250,
                        "clamped_offset_s": 0.250,
                        "final_applied_offset_s": 0.250,
                        "pre_shift_detected": False,
                    }
                ),
                encoding="utf-8",
            )
            lines = main_mod._offset_summary_lines(paths, "artist_title", 0.5)

        combined = "\n".join(lines)
        self.assertIn("Sub-offsets:", combined)
        self.assertIn("#1=-0.200s@12.00s(c=0.93)", combined)
        self.assertIn("#2=-0.300s@56.00s(c=0.91)", combined)
        self.assertIn("aggregate=-0.250s", combined)
        self.assertIn("Final applied offset: +0.250s", combined)
        self.assertIn("cli=+0.500s", combined)

    def test_offset_summary_lines_fallback_without_step4_meta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.timings / "artist_title.offset.auto").write_text("-0.200\n", encoding="utf-8")
            lines = main_mod._offset_summary_lines(paths, "artist_title", 0.5)
        combined = "\n".join(lines)
        self.assertIn("Sub-offsets: n/a", combined)
        self.assertIn("Final applied offset (estimated): +0.300s", combined)

    def test_offset_summary_lines_include_manual_recommendation_when_auto_calibration_unreliable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.timings / "artist_title.offset.auto.meta.json").write_text(
                json.dumps(
                    {
                        "status": "low_confidence",
                        "manual_offset_recommended": True,
                        "aggregate_confidence": 0.31,
                        "selected_samples": 1,
                        "sample_count": 6,
                    }
                ),
                encoding="utf-8",
            )
            lines = main_mod._offset_summary_lines(paths, "artist_title", 0.0)

        combined = "\n".join(lines)
        self.assertIn("Auto-calibration not reliable; manual offset recommended", combined)
        self.assertIn("samples=1/6", combined)

    def test_final_offset_summary_ignores_step4_meta_with_cli_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            paths = main_mod.Paths(root=Path(td))
            paths.ensure()
            (paths.meta / "artist_title.step4.offsets.json").write_text(
                json.dumps(
                    {
                        "offset_source": "none",
                        "auto_offset_s": 0.0,
                        "manual_offset_s": 0.0,
                        "cli_offset_s": -1.0,
                        "base_offset_s": 0.0,
                        "resolved_offset_s": -1.0,
                        "clamped_offset_s": -1.0,
                        "final_applied_offset_s": -1.0,
                        "pre_shift_detected": False,
                    }
                ),
                encoding="utf-8",
            )
            line = main_mod._final_offset_summary(paths, "artist_title", -0.4)

        self.assertIn("Final applied offset (estimated): -0.400s", line)

    def test_parse_args_supports_positional_query_shorthand(self) -> None:
        args = main_mod.parse_args(["artist title"])
        self.assertEqual(args.query, "artist title")

    def test_parse_args_supports_new_preset(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--new"])
        self.assertTrue(args.new_preset)

    def test_parse_args_supports_new_preset_with_positional_query(self) -> None:
        args = main_mod.parse_args(["--new", "artist title"])
        self.assertTrue(args.new_preset)
        self.assertEqual(args.query, "artist title")

    def test_parse_args_supports_use_cache_flag(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--use-cache"])
        self.assertTrue(args.use_cache)
        self.assertTrue(bool(getattr(args, "use_cache_explicit", False)))

    def test_parse_args_supports_use_cache_flag_with_zero(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--use-cache", "0"])
        self.assertFalse(args.use_cache)
        self.assertTrue(bool(getattr(args, "use_cache_explicit", False)))

    def test_parse_args_supports_use_cache_flag_with_one(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--use-cache", "1"])
        self.assertTrue(args.use_cache)
        self.assertTrue(bool(getattr(args, "use_cache_explicit", False)))

    def test_parse_args_default_use_cache_not_explicit(self) -> None:
        args = main_mod.parse_args(["--query", "artist title"])
        self.assertFalse(args.use_cache)
        self.assertFalse(bool(getattr(args, "use_cache_explicit", False)))

    def test_parse_args_use_cache_does_not_consume_positional_query(self) -> None:
        args = main_mod.parse_args(["--use-cache", "artist title"])
        self.assertTrue(args.use_cache)
        self.assertEqual(args.query, "artist title")

    def test_parse_args_use_cache_zero_then_positional_query(self) -> None:
        args = main_mod.parse_args(["--use-cache", "0", "artist title"])
        self.assertFalse(args.use_cache)
        self.assertEqual(args.query, "artist title")

    def test_parse_args_use_cache_before_query_with_other_flags(self) -> None:
        args = main_mod.parse_args(["--use-cache", "1", "--off", "0.0", "artist title"])
        self.assertTrue(args.use_cache)
        self.assertEqual(args.query, "artist title")
        self.assertAlmostEqual(float(args.offset), 0.0, places=3)

    def test_parse_args_use_cache_after_query_with_other_flags(self) -> None:
        args = main_mod.parse_args(["artist title", "--off", "0.0", "--use-cache"])
        self.assertTrue(args.use_cache)
        self.assertEqual(args.query, "artist title")
        self.assertAlmostEqual(float(args.offset), 0.0, places=3)

    def test_parse_args_rejects_middle_positional_query(self) -> None:
        with self.assertRaises(SystemExit):
            main_mod.parse_args(["--off", "0.0", "artist title", "--use-cache", "1"])

    def test_parse_args_allows_new_then_positional_query(self) -> None:
        args = main_mod.parse_args(["--new", "artist title", "--off", "0.0"])
        self.assertTrue(args.new_preset)
        self.assertEqual(args.query, "artist title")

    def test_parse_args_supports_off_alias_for_offset(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--off", "1.5"])
        self.assertAlmostEqual(float(args.offset), 1.5, places=3)

    def test_parse_args_supports_url_alias_for_audio_source(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--url", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"])
        self.assertEqual(args.audio_url, "https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    def test_parse_args_supports_lrc_artist_title_overrides(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--lrc-artist", "Pink Floyd", "--lrc-title", "Wish You Were Here"])
        self.assertEqual(args.lrc_artist, "Pink Floyd")
        self.assertEqual(args.lrc_title, "Wish You Were Here")

    def test_parse_args_supports_lyric_start_constraint(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--lyric-start", "que linda esta la"])
        self.assertEqual(args.lyric_start, "que linda esta la")

    def test_parse_args_supports_title_card_display(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--title-card-display", "Line 1\\nLine 2"])
        self.assertEqual(args.title_card_display, "Line 1\\nLine 2")

    def test_parse_args_supports_font_size_percent(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--font-size-percent", "85"])
        self.assertAlmostEqual(float(args.font_size_percent), 85.0, places=3)

    def test_parse_args_supports_open_disabled(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--open-disabled"])
        self.assertTrue(args.open_disabled)

    def test_parse_args_uses_lrc_artist_title_when_query_omitted(self) -> None:
        args = main_mod.parse_args(["--lrc-artist", "Mariachi Vargas", "--lrc-title", "Las Mañanitas Tapatias"])
        self.assertEqual(args.query, "Mariachi Vargas - Las Mañanitas Tapatias")
        self.assertEqual(args.lrc_artist, "Mariachi Vargas")
        self.assertEqual(args.lrc_title, "Las Mañanitas Tapatias")

    def test_parse_args_defaults_auto_offset_to_strong_stable_levels(self) -> None:
        args = main_mod.parse_args(["--query", "artist title"])
        self.assertEqual(int(args.enable_auto_offset), 3)
        self.assertEqual(int(args.calibration_level), 2)

    def test_parse_args_supports_tune_for_me_flag(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--tune-for-me"])
        self.assertEqual(int(args.enable_auto_offset), 3)

    def test_parse_args_supports_tune_for_me_level_zero(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--tune-for-me", "0"])
        self.assertEqual(int(args.enable_auto_offset), 0)

    def test_parse_args_supports_tune_for_me_level_three(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--tune-for-me", "3"])
        self.assertEqual(int(args.enable_auto_offset), 3)

    def test_parse_args_supports_calibration_default_level(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--calibration"])
        self.assertEqual(int(args.calibration_level), 2)

    def test_parse_args_supports_calibration_level_zero(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--calibration-level", "0"])
        self.assertEqual(int(args.calibration_level), 0)

    def test_parse_args_supports_calibration_level_two(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--calibration-level", "2"])
        self.assertEqual(int(args.calibration_level), 2)

    def test_parse_args_supports_duration_aware_source_match(self) -> None:
        args = main_mod.parse_args(["--query", "artist title", "--duration-aware-source-match"])
        self.assertTrue(bool(args.duration_aware_source_match))

    def test_parse_args_includes_upload_step5_options(self) -> None:
        args = main_mod.parse_args(
            [
                "--query",
                "artist title",
                "--upload",
                "--upload-privacy",
                "private",
                "--upload-title",
                "My Title",
                "--upload-ending",
                "No Bass",
                "--upload-interactive",
                "--upload-open-output-dir",
            ]
        )
        self.assertTrue(args.upload)
        self.assertEqual(args.upload_privacy, "private")
        self.assertEqual(args.upload_title, "My Title")
        self.assertEqual(args.upload_ending, "No Bass")
        self.assertTrue(args.upload_interactive)
        self.assertTrue(args.upload_open_output_dir)

    def test_summary_table_contains_ansi_colors(self) -> None:
        table = main_mod._summary_table(
            [("step1", "ok", 1.234), ("step2", "failed", 0.456)],
            total_seconds=1.690,
            total_status="failed",
        )
        self.assertIn(main_mod.GREEN, table)
        self.assertIn(main_mod.RED, table)
        self.assertIn(main_mod.RESET, table)

    def test_main_upload_calls_step5_non_interactive_by_default(self) -> None:
        captured = {}

        def fake_step5_main(argv):  # type: ignore[no-untyped-def]
            captured["argv"] = list(argv)
            return 0

        with (
            mock.patch.object(main_mod, "ENABLE_STEP5_UPLOAD", True),
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            fake_step5_module = types.ModuleType("scripts.step5_distribute")
            fake_step5_module.main = fake_step5_main  # type: ignore[attr-defined]
            with (
                mock.patch.dict(sys.modules, {"scripts.step5_distribute": fake_step5_module}),
                mock.patch.object(sys.modules["scripts"], "step5_distribute", fake_step5_module, create=True),
            ):
                rc = main_mod.main(
                    [
                        "--query",
                        "artist title",
                        "--skip-step1",
                        "--upload",
                        "--upload-privacy",
                        "private",
                        "--upload-title",
                        "Custom Upload Title",
                    ]
                )

        self.assertEqual(rc, 0)
        self.assertIn("--non-interactive", captured["argv"])
        self.assertIn("--yes", captured["argv"])
        self.assertIn("--privacy", captured["argv"])
        self.assertIn("private", captured["argv"])
        self.assertIn("--title", captured["argv"])
        self.assertIn("Custom Upload Title", captured["argv"])

    def test_main_upload_interactive_omits_non_interactive_flags(self) -> None:
        captured = {}

        def fake_step5_main(argv):  # type: ignore[no-untyped-def]
            captured["argv"] = list(argv)
            return 0

        with (
            mock.patch.object(main_mod, "ENABLE_STEP5_UPLOAD", True),
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            fake_step5_module = types.ModuleType("scripts.step5_distribute")
            fake_step5_module.main = fake_step5_main  # type: ignore[attr-defined]
            with (
                mock.patch.dict(sys.modules, {"scripts.step5_distribute": fake_step5_module}),
                mock.patch.object(sys.modules["scripts"], "step5_distribute", fake_step5_module, create=True),
            ):
                rc = main_mod.main(
                    [
                        "--query",
                        "artist title",
                        "--skip-step1",
                        "--upload",
                        "--upload-interactive",
                    ]
                )

        self.assertEqual(rc, 0)
        self.assertNotIn("--non-interactive", captured["argv"])
        self.assertNotIn("--yes", captured["argv"])

    def test_main_upload_returns_nonzero_when_step5_returns_nonzero(self) -> None:
        with (
            mock.patch.object(main_mod, "ENABLE_STEP5_UPLOAD", True),
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            fake_step5_module = types.ModuleType("scripts.step5_distribute")
            fake_step5_module.main = lambda argv: 2  # type: ignore[attr-defined]
            with (
                mock.patch.dict(sys.modules, {"scripts.step5_distribute": fake_step5_module}),
                mock.patch.object(sys.modules["scripts"], "step5_distribute", fake_step5_module, create=True),
            ):
                rc = main_mod.main(["--query", "artist title", "--skip-step1", "--upload"])
        self.assertEqual(rc, 1)

    def test_main_uses_full_step3_sync_on_vanilla_fast_path_when_auto_offset_defaults_on(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", True),
            mock.patch.object(main_mod, "STEP3_LITE_FOR_VANILLA", True),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step2_split") as step2_mock,
            mock.patch("scripts.main.step3_sync_lite") as step3_lite_mock,
            mock.patch("scripts.main.step3_sync") as step3_sync_mock,
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title"])

        self.assertEqual(rc, 0)
        step2_mock.assert_not_called()
        step3_lite_mock.assert_not_called()
        step3_sync_mock.assert_called_once()

    def test_main_tune_for_me_level_passes_accuracy_to_step3_sync(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", True),
            mock.patch.object(main_mod, "STEP3_LITE_FOR_VANILLA", True),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync_lite") as step3_lite_mock,
            mock.patch("scripts.main.step3_sync") as step3_sync_mock,
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--tune-for-me", "3"])

        self.assertEqual(rc, 0)
        step3_lite_mock.assert_not_called()
        step3_sync_mock.assert_called_once()
        kwargs = step3_sync_mock.call_args.kwargs
        self.assertTrue(kwargs.get("run_auto_offset"))
        self.assertEqual(int(kwargs.get("auto_offset_accuracy")), 3)

    def test_main_defaults_to_strong_stable_auto_offset_levels(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", True),
            mock.patch.object(main_mod, "STEP3_LITE_FOR_VANILLA", True),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync_lite") as step3_lite_mock,
            mock.patch("scripts.main.step3_sync") as step3_sync_mock,
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title"])

        self.assertEqual(rc, 0)
        step3_lite_mock.assert_not_called()
        step3_sync_mock.assert_called_once()
        kwargs = step3_sync_mock.call_args.kwargs
        self.assertTrue(kwargs.get("run_auto_offset"))
        self.assertEqual(int(kwargs.get("auto_offset_accuracy")), 3)
        self.assertEqual(int(kwargs.get("auto_offset_calibration_level")), 2)

    def test_main_calibration_level_passes_to_step3_sync(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", True),
            mock.patch.object(main_mod, "STEP3_LITE_FOR_VANILLA", True),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync_lite") as step3_lite_mock,
            mock.patch("scripts.main.step3_sync") as step3_sync_mock,
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--calibration-level", "2"])

        self.assertEqual(rc, 0)
        step3_lite_mock.assert_not_called()
        step3_sync_mock.assert_called_once()
        kwargs = step3_sync_mock.call_args.kwargs
        self.assertTrue(kwargs.get("run_auto_offset"))
        self.assertEqual(int(kwargs.get("auto_offset_calibration_level")), 2)

    def test_main_combines_tune_and_calibration_levels(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", True),
            mock.patch.object(main_mod, "STEP3_LITE_FOR_VANILLA", True),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync_lite") as step3_lite_mock,
            mock.patch("scripts.main.step3_sync") as step3_sync_mock,
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(
                ["--query", "artist title", "--tune-for-me", "2", "--calibration-level", "1"]
            )

        self.assertEqual(rc, 0)
        step3_lite_mock.assert_not_called()
        step3_sync_mock.assert_called_once()
        kwargs = step3_sync_mock.call_args.kwargs
        self.assertTrue(kwargs.get("run_auto_offset"))
        self.assertEqual(int(kwargs.get("auto_offset_accuracy")), 2)
        self.assertEqual(int(kwargs.get("auto_offset_calibration_level")), 1)

    def test_main_forwards_title_card_display_to_step4(self) -> None:
        with (
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0) as step4_main_mock,
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--skip-step1",
                    "--title-card-display",
                    "Line 1\\nLine 2",
                ]
            )

        self.assertEqual(rc, 0)
        argv = step4_main_mock.call_args.args[0]
        self.assertIn("--title-card-display", argv)
        self.assertIn("Line 1\\nLine 2", argv)

    def test_main_forwards_font_size_percent_to_step4(self) -> None:
        with (
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0) as step4_main_mock,
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--skip-step1",
                    "--font-size-percent",
                    "80",
                ]
            )

        self.assertEqual(rc, 0)
        argv = step4_main_mock.call_args.args[0]
        self.assertIn("--font-size-percent", argv)
        self.assertIn("80.0", argv)

    def test_main_fast_path_prefers_step1_audio_for_step4(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0) as step4_main_mock,
        ):
            rc = main_mod.main(["--query", "artist title"])

        self.assertEqual(rc, 0)
        argv = step4_main_mock.call_args.args[0]
        self.assertIn("--prefer-step1-audio", argv)

    def test_main_stems_path_does_not_force_prefer_step1_audio(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step2_split", return_value=None),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0) as step4_main_mock,
        ):
            rc = main_mod.main(["--query", "artist title", "--bass", "0"])

        self.assertEqual(rc, 0)
        argv = step4_main_mock.call_args.args[0]
        self.assertNotIn("--prefer-step1-audio", argv)

    def test_main_new_preset_applies_preset_flags(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--new"])

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertTrue(kwargs.get("force"))
        self.assertTrue(kwargs.get("reset"))
        self.assertEqual(int(kwargs.get("yt_search_n")), 3)
        self.assertEqual(kwargs.get("speed_mode"), main_mod.DEFAULT_PIPELINE_SPEED_MODE)

    def test_main_new_preset_keeps_explicit_search_n_override(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--new", "--yt-search-n", "5"])

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertEqual(int(kwargs.get("yt_search_n")), 5)

    def test_main_speed_mode_flag_is_accepted_but_ignored(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--new", "--speed-mode", "turbo"])

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertEqual(kwargs.get("speed_mode"), main_mod.DEFAULT_PIPELINE_SPEED_MODE)

    def test_main_new_preset_with_positional_query(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--new", "artist title", "--off", "0.0"])

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertEqual(kwargs.get("query"), "artist title")
        self.assertTrue(kwargs.get("force"))
        self.assertTrue(kwargs.get("reset"))
        self.assertEqual(int(kwargs.get("yt_search_n")), 3)
        self.assertEqual(kwargs.get("speed_mode"), main_mod.DEFAULT_PIPELINE_SPEED_MODE)

    def test_main_use_cache_disables_force_reset_nuke_even_with_new_preset(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--new",
                    "--force",
                    "--reset",
                    "--nuke",
                    "--use-cache",
                ]
            )

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertFalse(kwargs.get("force"))
        self.assertFalse(kwargs.get("reset"))
        self.assertFalse(kwargs.get("nuke"))
        self.assertEqual(int(kwargs.get("yt_search_n")), 3)
        self.assertEqual(kwargs.get("speed_mode"), main_mod.DEFAULT_PIPELINE_SPEED_MODE)

    def test_main_use_cache_zero_forces_cold_step1(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--use-cache", "0"])

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertTrue(kwargs.get("force"))
        self.assertTrue(kwargs.get("reset"))
        self.assertTrue(kwargs.get("nuke"))
        self.assertTrue(kwargs.get("disable_cache"))

    def test_main_use_cache_zero_keeps_step2_cache_reusable_without_explicit_force(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", False),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step2_split", return_value=None) as step2_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--use-cache",
                    "0",
                    "--vocals",
                    "20",
                    "--no-render",
                ]
            )

        self.assertEqual(rc, 0)
        flags = step2_mock.call_args.kwargs.get("flags")
        self.assertIsNotNone(flags)
        self.assertFalse(bool(flags.force))

    def test_main_explicit_force_still_forces_step2_when_use_cache_zero(self) -> None:
        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", False),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step2_split", return_value=None) as step2_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--use-cache",
                    "0",
                    "--force",
                    "--vocals",
                    "20",
                    "--no-render",
                ]
            )

        self.assertEqual(rc, 0)
        flags = step2_mock.call_args.kwargs.get("flags")
        self.assertIsNotNone(flags)
        self.assertTrue(bool(flags.force))

    def test_main_direct_cli_calls_auto_open_hook(self) -> None:
        with (
            mock.patch.object(sys, "argv", ["scripts.main", "--query", "artist title"]),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
            mock.patch("scripts.main._maybe_auto_open_output_video") as auto_open_mock,
        ):
            rc = main_mod.main(None)

        self.assertEqual(rc, 0)
        self.assertEqual(auto_open_mock.call_count, 1)
        self.assertTrue(bool(auto_open_mock.call_args.kwargs.get("invoked_direct_cli")))

    def test_main_programmatic_invocation_disables_auto_open_hook(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
            mock.patch("scripts.main._maybe_auto_open_output_video") as auto_open_mock,
        ):
            rc = main_mod.main(["--query", "artist title"])

        self.assertEqual(rc, 0)
        self.assertEqual(auto_open_mock.call_count, 1)
        self.assertFalse(bool(auto_open_mock.call_args.kwargs.get("invoked_direct_cli")))

    def test_main_uses_output_subdir_for_step4_out(self) -> None:
        with (
            mock.patch.dict("os.environ", {"MIXTERIOSO_OUTPUT_SUBDIR": ""}, clear=False),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0) as step4_main,
        ):
            rc = main_mod.main(["--query", "artist title", "--output-subdir", "output/temp"])

        self.assertEqual(rc, 0)
        argv = list(step4_main.call_args.args[0])
        self.assertIn("--out", argv)
        out_path = Path(argv[argv.index("--out") + 1])
        self.assertEqual(out_path, main_mod.ROOT / "output" / "temp" / "artist_title.mp4")

    def test_parse_args_defaults_output_subdir_to_output_temp(self) -> None:
        with mock.patch.dict("os.environ", {"MIXTERIOSO_OUTPUT_SUBDIR": ""}, clear=False):
            args = main_mod.parse_args(["--query", "artist title"])
        self.assertEqual(args.output_subdir, "output/temp")

    def test_spawn_delayed_open_file_uses_delayed_open_and_autoplay(self) -> None:
        with mock.patch("scripts.main._spawn_detached", return_value=True) as spawn_mock:
            opened = main_mod._spawn_delayed_open_file(Path("/tmp/out.mp4"), initial_delay_seconds=0.4)

        self.assertTrue(opened)
        cmd = spawn_mock.call_args.args[0]
        self.assertEqual(cmd[0:3], ["/bin/sh", "-c", cmd[2]])
        script = cmd[2]
        self.assertIn('open "$2"', script)
        self.assertIn("osascript", script)
        self.assertIn("play front document", script)

    def test_main_use_cache_sets_fast_render_env_defaults(self) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "KARAOKE_RENDER_PROFILE": "",
                    "KARAOKE_RENDER_LEVEL": "",
                    "KARAOKE_NO_FASTSTART": "",
                    "KARAOKE_TURBO_VALIDATE_DURATION": "",
                },
                clear=False,
            ),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--use-cache"])
            self.assertEqual(os.environ.get("KARAOKE_RENDER_PROFILE"), "fast")
            self.assertEqual(os.environ.get("KARAOKE_NO_FASTSTART"), "1")
            self.assertEqual(os.environ.get("KARAOKE_TURBO_VALIDATE_DURATION"), "0")

        self.assertEqual(rc, 0)

    def test_main_preserves_explicit_render_env_override(self) -> None:
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "KARAOKE_RENDER_PROFILE": "fast",
                    "KARAOKE_RENDER_LEVEL": "",
                    "KARAOKE_NO_FASTSTART": "0",
                },
                clear=False,
            ),
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}),
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--use-cache"])
            self.assertEqual(os.environ.get("KARAOKE_RENDER_PROFILE"), "fast")
            self.assertEqual(os.environ.get("KARAOKE_NO_FASTSTART"), "0")

        self.assertEqual(rc, 0)

    def test_main_url_keeps_query_for_lyrics_and_uses_audio_source_for_download(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "las mañanitas (con mariachi)",
                    "--url",
                    "https://www.youtube.com/watch?v=7gYcjAifYzI",
                ]
            )

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertEqual(kwargs.get("query"), "las mañanitas (con mariachi)")
        self.assertEqual(kwargs.get("audio_source"), "https://www.youtube.com/watch?v=7gYcjAifYzI")

    def test_main_passes_duration_aware_source_match_to_step1(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--duration-aware-source-match",
                ]
            )

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertTrue(bool(kwargs.get("duration_aware_source_match")))

    def test_main_passes_lrc_overrides_and_lyric_start_to_step1(self) -> None:
        with (
            mock.patch("scripts.main.step1_fetch", return_value={"slug": "artist_title"}) as step1_mock,
            mock.patch("scripts.main.step3_sync", return_value=None),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(
                [
                    "--query",
                    "artist title",
                    "--lrc-artist",
                    "custom artist",
                    "--lrc-title",
                    "custom title",
                    "--lyric-start",
                    "que linda esta la",
                ]
            )

        self.assertEqual(rc, 0)
        kwargs = step1_mock.call_args.kwargs
        self.assertEqual(kwargs.get("query"), "artist title")
        self.assertEqual(kwargs.get("lrc_artist"), "custom artist")
        self.assertEqual(kwargs.get("lrc_title"), "custom title")
        self.assertEqual(kwargs.get("lyric_start"), "que linda esta la")

    def test_main_runs_step3_before_step2_finishes(self) -> None:
        order: list[str] = []
        order_lock = threading.Lock()

        def fake_step2(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            with order_lock:
                order.append("step2_start")
            time.sleep(0.15)
            with order_lock:
                order.append("step2_end")

        def fake_step3(*_args, **_kwargs):  # type: ignore[no-untyped-def]
            with order_lock:
                order.append("step3_start")
            time.sleep(0.02)
            with order_lock:
                order.append("step3_end")

        with (
            mock.patch.object(main_mod, "SKIP_STEP2_FOR_VANILLA", False),
            mock.patch("scripts.main.step2_split", side_effect=fake_step2),
            mock.patch("scripts.main.step3_sync", side_effect=fake_step3),
            mock.patch("scripts.step4_assemble.main", return_value=0),
        ):
            rc = main_mod.main(["--query", "artist title", "--skip-step1"])

        self.assertEqual(rc, 0)
        self.assertIn("step2_end", order)
        self.assertIn("step3_start", order)
        self.assertLess(order.index("step3_start"), order.index("step2_end"))


if __name__ == "__main__":
    unittest.main()
