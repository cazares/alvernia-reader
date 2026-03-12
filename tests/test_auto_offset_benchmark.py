import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import auto_offset_benchmark as bench


class AutoOffsetBenchmarkTests(unittest.TestCase):
    def test_load_cases_filters_by_slug(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cases.json"
            path.write_text(
                json.dumps(
                    [
                        {"slug": "song_one", "target_offset_s": -0.25, "notes": "first"},
                        {"slug": "song_two", "target_offset_s": 1.5, "notes": "second"},
                    ]
                ),
                encoding="utf-8",
            )

            cases = bench.load_cases(path, slugs={"song_two"})

        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0].slug, "song_two")
        self.assertAlmostEqual(cases[0].target_offset_s, 1.5, places=3)
        self.assertEqual(cases[0].notes, "second")

    def test_summarize_results_reports_threshold_counts(self) -> None:
        summary = bench.summarize_results(
            [
                {"elapsed_sec": 2.0, "applied_offset_s": 0.1, "abs_error_s": 0.05},
                {"elapsed_sec": 4.0, "applied_offset_s": 0.2, "abs_error_s": 0.40},
                {"elapsed_sec": 6.0, "applied_offset_s": None, "abs_error_s": None},
            ]
        )

        self.assertEqual(summary["case_count"], 3)
        self.assertEqual(summary["completed_case_count"], 2)
        self.assertAlmostEqual(float(summary["mean_elapsed_sec"]), 4.0, places=3)
        self.assertAlmostEqual(float(summary["mean_abs_error_s"]), 0.225, places=3)
        self.assertEqual(summary["within_100ms_count"], 1)
        self.assertEqual(summary["within_500ms_count"], 2)

    def test_benchmark_case_reads_auto_offset_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "timings").mkdir(parents=True, exist_ok=True)
            (repo_root / "mp3s").mkdir(parents=True, exist_ok=True)
            (repo_root / "timings" / "song.lrc").write_text("[00:01.00]Hello\n", encoding="utf-8")
            (repo_root / "mp3s" / "song.mp3").write_bytes(b"mp3")

            def _fake_write(**kwargs):  # type: ignore[no-untyped-def]
                slug = kwargs["slug"]
                timings_dir = bench.step3.TIMINGS_DIR
                (timings_dir / f"{slug}.offset.auto").write_text("1.250\n", encoding="utf-8")
                (timings_dir / f"{slug}.offset.auto.meta.json").write_text(
                    json.dumps(
                        {
                            "status": "applied",
                            "aggregate_confidence": 0.88,
                            "selected_samples": 4,
                            "high_confidence_samples": 3,
                            "mode_resolution": "combined",
                        }
                    ),
                    encoding="utf-8",
                )

            with mock.patch.object(bench.step3, "_maybe_write_auto_offset", side_effect=_fake_write):
                result = bench.benchmark_case(
                    repo_root=repo_root,
                    case=bench.BenchmarkCase(slug="song", target_offset_s=1.0, notes="demo"),
                    accuracy_level=3,
                    calibration_level=2,
                    language="auto",
                    force_refresh=True,
                )

        self.assertEqual(result["status"], "applied")
        self.assertAlmostEqual(float(result["applied_offset_s"]), 1.25, places=3)
        self.assertAlmostEqual(float(result["abs_error_s"]), 0.25, places=3)
        self.assertAlmostEqual(float(result["confidence"]), 0.88, places=3)
        self.assertEqual(int(result["selected_samples"]), 4)
        self.assertEqual(int(result["high_conf_samples"]), 3)


if __name__ == "__main__":
    unittest.main()
