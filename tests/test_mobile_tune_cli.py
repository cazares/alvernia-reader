import argparse
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts import mobile_tune_cli as mt


class MobileTuneCliTests(unittest.TestCase):
    def test_build_main_cmd_includes_max_tune_and_calibration(self) -> None:
        args = SimpleNamespace(
            query="artist title",
            new=True,
            speed_mode="extra-turbo",
            yt_search_n=3,
            vocals=None,
            bass=None,
            drums=None,
            other=None,
            no_render=True,
        )
        cmd = mt._build_main_cmd(args, off=0.0, tune_level=3, calibration_level=2)
        joined = " ".join(cmd)
        self.assertIn("-m scripts.main", joined)
        self.assertIn("--query artist title", joined)
        self.assertIn("--calibration-level 2", joined)
        self.assertIn("--tune-for-me 3", joined)
        self.assertIn("--off 0.000", joined)
        self.assertIn("--no-render", joined)

    def test_load_auto_result_reads_meta_and_offset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta_path = root / "song.offset.auto.meta.json"
            auto_path = root / "song.offset.auto"
            meta_path.write_text(
                json.dumps(
                    {
                        "status": "applied",
                        "selected_samples": 3,
                        "sample_count": 6,
                        "aggregate_confidence": 0.72,
                    }
                ),
                encoding="utf-8",
            )
            auto_path.write_text("-1.250\n", encoding="utf-8")

            with mock.patch.object(mt, "_offset_meta_paths", return_value=(meta_path, auto_path)):
                got = mt._load_auto_result("song")

        self.assertEqual(got["status"], "applied")
        self.assertAlmostEqual(float(got["auto_offset_s"]), -1.25, places=3)
        self.assertEqual(int(got["selected_samples"]), 3)
        self.assertEqual(int(got["sample_count"]), 6)
        self.assertAlmostEqual(float(got["aggregate_confidence"]), 0.72, places=2)

    def test_guided_flow_falls_back_to_manual_when_retry_declined(self) -> None:
        args = argparse.Namespace(
            query="artist title",
            max_algorithms=2,
            new=True,
            speed_mode="extra-turbo",
            yt_search_n=3,
            vocals=None,
            bass=None,
            drums=None,
            other=None,
            no_render=True,
        )
        fail_result = {
            "status": "no_successful_samples",
            "auto_offset_s": 0.0,
            "auto_exists": False,
            "selected_samples": None,
            "sample_count": 6,
            "aggregate_confidence": None,
            "manual_offset_recommended": True,
        }
        manual_result = {
            "status": "no_auto_file",
            "auto_offset_s": 0.0,
            "auto_exists": False,
            "selected_samples": None,
            "sample_count": None,
            "aggregate_confidence": None,
            "manual_offset_recommended": False,
        }

        with (
            mock.patch.object(mt, "_run_pipeline", side_effect=[0, 0]) as run_mock,
            mock.patch.object(mt, "_load_auto_result", side_effect=[fail_result, manual_result]),
            mock.patch.object(mt, "_prompt_yes_no", side_effect=[False, True]),
            mock.patch.object(mt, "_prompt_manual_offset", side_effect=[-1.0]),
        ):
            rc = mt.guided_mobile_flow(args)

        self.assertEqual(rc, 0)
        self.assertEqual(run_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()

