from __future__ import annotations

import importlib.util
import json
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import unittest


_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "karaoapi" / "scripts" / "canary_preset_e2e.py"
_SPEC = importlib.util.spec_from_file_location("canary_preset_e2e_script", _SCRIPT_PATH)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Could not load module spec for {_SCRIPT_PATH}")
canary = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(canary)


class CanaryPresetE2ETests(unittest.TestCase):
    def test_load_queries_from_preset_source_extracts_constant(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            app_path = Path(td) / "App.tsx"
            app_path.write_text(
                '\n'.join(
                    [
                        "const QUERY_PREFILL_BUTTONS = [",
                        '  "Song A - Alpha",',
                        '  "Song B - Beta",',
                        "];",
                    ]
                ),
                encoding="utf-8",
            )
            got = canary._load_queries_from_preset_source(app_path, "QUERY_PREFILL_BUTTONS")
        self.assertEqual(got, ["Song A - Alpha", "Song B - Beta"])

    def test_run_case_includes_vocals_payload(self) -> None:
        sent_payloads: list[dict[str, object]] = []

        def _fake_http_json(method: str, url: str, payload=None, timeout_sec=20.0):  # type: ignore[no-untyped-def]
            _ = timeout_sec
            if method == "POST" and url.endswith("/jobs"):
                sent_payloads.append(dict(payload or {}))
                return {"id": "job-123"}
            raise AssertionError(f"unexpected call: {method} {url}")

        with (
            mock.patch.object(canary, "_http_json", side_effect=_fake_http_json),
            mock.patch.object(
                canary,
                "_poll_job",
                return_value={
                    "status": "succeeded",
                    "created_at": 10.0,
                    "finished_at": 25.0,
                    "output_url": "https://example.invalid/files/final.mp4",
                },
            ),
            mock.patch.object(
                canary,
                "_download_probe",
                return_value={"ok": True, "bytes_read": 90000, "status": 200},
            ),
        ):
            result = canary._run_case(
                case={
                    "case_index": 1,
                    "total_cases": 1,
                    "query": "The Beatles - Let It Be",
                    "vocals": 10,
                    "idempotency_key": "canary-case-1",
                },
                base_url="https://example.invalid",
                force=True,
                poll_interval_sec=0.0,
                poll_jitter_sec=0.0,
                start_jitter_sec=0.0,
                max_wait_sec=5.0,
                max_elapsed_sec=60.0,
                min_output_bytes=1024,
                http_timeout_sec=5.0,
                job_not_found_grace_sec=1.0,
                job_not_found_recover_attempts=0,
                create_attempts=1,
                download_probe_attempts=1,
                poll_transient_error_limit=3,
                retry_backoff_base_sec=0.01,
                retry_backoff_max_sec=0.05,
            )

        self.assertEqual(len(sent_payloads), 1)
        self.assertEqual(sent_payloads[0].get("vocals"), 10)
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(result.get("status"), "succeeded")

    def test_main_builds_query_vocals_matrix(self) -> None:
        called_cases: list[tuple[str, int]] = []

        def _fake_run_case(*, case, **kwargs):  # type: ignore[no-untyped-def]
            _ = kwargs
            called_cases.append((str(case["query"]), int(case["vocals"])))
            return {
                "case_index": int(case["case_index"]),
                "query": str(case["query"]),
                "vocals": int(case["vocals"]),
                "ok": True,
                "elapsed_sec": 12.0,
                "wall_sec": 15.0,
            }

        with tempfile.TemporaryDirectory() as td:
            report_path = Path(td) / "canary-report.json"
            fake_args = SimpleNamespace(
                base_url="https://example.invalid",
                preset_source="karaoapp/App.tsx",
                preset_constant="QUERY_PREFILL_BUTTONS",
                query_file="",
                query=[],
                vocals_level=[0, 10, 100],
                force=True,
                max_concurrency=3,
                per_query_max_concurrency=1,
                traffic_profile="strict",
                start_jitter_sec=0.0,
                poll_jitter_sec=0.0,
                random_seed=7,
                poll_interval_sec=0.0,
                max_wait_sec=60.0,
                max_elapsed_sec=120.0,
                create_attempts=2,
                download_probe_attempts=2,
                poll_transient_error_limit=4,
                retry_backoff_base_sec=0.1,
                retry_backoff_max_sec=0.4,
                min_success_rate=1.0,
                min_output_bytes=1024,
                http_timeout_sec=10.0,
                job_not_found_grace_sec=1.0,
                job_not_found_recover_attempts=0,
                report_path=str(report_path),
            )

            with (
                mock.patch.object(canary, "_parse_args", return_value=fake_args),
                mock.patch.object(canary, "_load_queries", return_value=["Song A", "Song B"]),
                mock.patch.object(canary, "_run_case", side_effect=_fake_run_case),
                mock.patch.object(canary, "_log"),
            ):
                rc = canary.main([])

            self.assertEqual(rc, 0)
            self.assertTrue(report_path.exists())
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(report.get("query_count"), 2)
        self.assertEqual(report.get("vocals_levels"), [0, 10, 100])
        self.assertEqual(report.get("summary", {}).get("cases_total"), 6)
        self.assertIn("ratings_out_of_10", report.get("summary", {}))
        self.assertEqual(
            set(called_cases),
            {
                ("Song A", 0),
                ("Song A", 10),
                ("Song A", 100),
                ("Song B", 0),
                ("Song B", 10),
                ("Song B", 100),
            },
        )

    def test_run_cases_parallel_respects_per_query_limit(self) -> None:
        active_by_query: dict[str, int] = {}
        max_by_query: dict[str, int] = {}
        lock = threading.Lock()

        def _fake_run_case(*, case, **kwargs):  # type: ignore[no-untyped-def]
            _ = kwargs
            query = str(case["query"])
            with lock:
                active_by_query[query] = active_by_query.get(query, 0) + 1
                max_by_query[query] = max(max_by_query.get(query, 0), active_by_query[query])
            time.sleep(0.03)
            with lock:
                active_by_query[query] = max(0, active_by_query.get(query, 0) - 1)
            return {
                "case_index": int(case["case_index"]),
                "query": query,
                "vocals": int(case["vocals"]),
                "ok": True,
                "elapsed_sec": 1.0,
                "wall_sec": 1.0,
            }

        case_defs = []
        idx = 0
        for query in ("Song A", "Song B"):
            for vocals in (0, 10, 100):
                idx += 1
                case_defs.append({"case_index": idx, "query": query, "vocals": vocals})

        with mock.patch.object(canary, "_run_case", side_effect=_fake_run_case):
            results = canary._run_cases_parallel(
                case_defs=case_defs,
                max_concurrency=4,
                per_query_max_concurrency=1,
                run_case_kwargs={},
            )

        self.assertEqual(len(results), 6)
        self.assertEqual(max_by_query.get("Song A"), 1)
        self.assertEqual(max_by_query.get("Song B"), 1)


if __name__ == "__main__":
    unittest.main()
