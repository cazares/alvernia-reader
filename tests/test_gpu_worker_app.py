import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

try:
    from karaoapi import gpu_worker_app as worker
except Exception:
    worker = None

try:
    from fastapi.testclient import TestClient
except Exception:
    TestClient = None  # type: ignore[assignment]


@unittest.skipIf(worker is None, "gpu worker module is not available")
class GPUWorkerHelperTests(unittest.TestCase):
    def test_required_stems_for_two_stems_and_full_stems(self) -> None:
        self.assertEqual(worker._required_stems(True, []), ["vocals", "no_vocals"])
        self.assertEqual(worker._required_stems(False, []), ["vocals", "bass", "drums", "other"])
        self.assertEqual(worker._required_stems(False, ["vocals", "bass", "bad"]), ["vocals", "bass"])

    def test_hmac_secrets_support_karaoapi_aliases(self) -> None:
        with (
            mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET", ""),
            mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET_PREVIOUS", ""),
            mock.patch.dict(
                "os.environ",
                {
                    "KARAOAPI_GPU_WORKER_HMAC_SECRET": "current-secret",
                    "KARAOAPI_GPU_WORKER_HMAC_SECRET_PREVIOUS": "previous-secret",
                },
                clear=False,
            ),
        ):
            self.assertEqual(worker._hmac_secrets(), ["current-secret", "previous-secret"])

    def test_singleflight_runs_demucs_once_for_identical_parallel_requests(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "song.mp3"
            audio.write_bytes(b"a" * 4096)
            stem_dir = root / "separated" / "htdemucs" / "song"
            req = worker.SeparateRequest(
                slug="song",
                audio_path=str(audio),
                output_dir=str(stem_dir),
            )

            results: list[dict] = []
            errors: list[Exception] = []

            def fake_run_demucs(**kwargs):  # type: ignore[no-untyped-def]
                _ = kwargs
                time.sleep(0.10)
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            def run_call() -> None:
                try:
                    results.append(worker._separate_sync(req))
                except Exception as exc:  # pragma: no cover - assertion below checks this
                    errors.append(exc)

            with (
                mock.patch.object(worker, "WORKER_CACHE_ENABLED", True),
                mock.patch.object(worker, "WORKER_SINGLEFLIGHT_ENABLED", True),
                mock.patch.object(worker, "WORKER_ENFORCE_AUDIO_ROOTS", False),
                mock.patch.object(worker, "WORKER_STEM_MIN_BYTES", 1024),
                mock.patch.object(worker, "_SINGLEFLIGHT_LOCKS", {}),
                mock.patch.object(worker, "_SINGLEFLIGHT_REFS", {}),
                mock.patch.object(worker, "_run_demucs", side_effect=fake_run_demucs) as run_mock,
            ):
                t1 = threading.Thread(target=run_call)
                t2 = threading.Thread(target=run_call)
                t1.start()
                t2.start()
                t1.join(timeout=3.0)
                t2.join(timeout=3.0)

            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            self.assertEqual(run_mock.call_count, 1)
            self.assertEqual(sorted(bool(r.get("cached")) for r in results), [False, True])

    def test_cache_prune_keeps_newest_dirs_within_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache_root = root / "separated" / "htdemucs"
            old_dir = cache_root / "song_old"
            new_dir = cache_root / "song_new"
            old_dir.mkdir(parents=True, exist_ok=True)
            new_dir.mkdir(parents=True, exist_ok=True)
            (old_dir / ".worker_meta.json").write_text("{}", encoding="utf-8")
            (new_dir / ".worker_meta.json").write_text("{}", encoding="utf-8")
            now = time.time()
            old_ts = now - 120
            new_ts = now - 10
            os.utime(old_dir / ".worker_meta.json", (old_ts, old_ts))
            os.utime(new_dir / ".worker_meta.json", (new_ts, new_ts))

            with (
                mock.patch.object(worker, "WORKER_CACHE_PRUNE_ENABLED", True),
                mock.patch.object(worker, "WORKER_CACHE_PRUNE_INTERVAL_SECS", 0.0),
                mock.patch.object(worker, "WORKER_CACHE_MAX_DIRS", 1),
                mock.patch.object(worker, "WORKER_CACHE_MAX_AGE_SECS", 3600.0),
                mock.patch.object(worker, "_CACHE_PRUNE_LAST_AT_MONO", 0.0),
            ):
                worker._maybe_prune_worker_cache(cache_root=cache_root, preserve_dirs=[])

            self.assertFalse(old_dir.exists())
            self.assertTrue(new_dir.exists())


@unittest.skipIf(worker is None or TestClient is None, "fastapi test dependencies are not available")
class GPUWorkerEndpointTests(unittest.TestCase):

    def test_health_endpoint_returns_expected_shape(self) -> None:
        client = TestClient(worker.app)
        res = client.get("/health")
        self.assertEqual(res.status_code, 200)
        payload = res.json()
        self.assertTrue(payload.get("ok"))
        self.assertIn("demucs_available", payload)
        self.assertIn("singleflight_enabled", payload)

    def test_separate_requires_bearer_when_api_key_set(self) -> None:
        with (
            mock.patch.object(worker, "WORKER_API_KEY", "topsecret"),
            mock.patch.object(worker, "_WORKER_SEMAPHORE", threading.Semaphore(1)),
        ):
            client = TestClient(worker.app)
            res = client.post("/separate", json={"slug": "song", "audio_path": "/tmp/nope.mp3"})
            self.assertEqual(res.status_code, 401)

    def test_separate_uses_cache_and_skips_demucs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "song.mp3"
            audio.write_bytes(b"a" * 4096)
            stem_dir = root / "separated" / "htdemucs" / "song"
            stem_dir.mkdir(parents=True, exist_ok=True)
            for name in ("vocals", "bass", "drums", "other"):
                (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            cache_meta = {
                "source_sha256": worker.sha256_file(audio),
                "model_version": "htdemucs",
                "stem_profile": {
                    "vocals": 100.0,
                    "bass": 100.0,
                    "drums": 100.0,
                    "other": 100.0,
                },
                "two_stems": False,
                "shifts": 1,
                "overlap": 0.1,
                "device": "cpu",
                "required_stems": ["vocals", "bass", "drums", "other"],
            }
            (stem_dir / ".worker_meta.json").write_text(json.dumps(cache_meta), encoding="utf-8")

            with (
                mock.patch.object(worker, "WORKER_API_KEY", ""),
                mock.patch.object(worker, "GPU_WORKER_REQUIRE_HMAC", False),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET", ""),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET_PREVIOUS", ""),
                mock.patch.object(worker, "GPU_WORKER_ALLOW_UNAUTH", True),
                mock.patch.object(worker, "WORKER_CACHE_ENABLED", True),
                mock.patch.object(worker, "_WORKER_SEMAPHORE", threading.Semaphore(1)),
                mock.patch.object(worker, "WORKER_STEM_MIN_BYTES", 1024),
                mock.patch.object(worker, "_run_demucs") as run_mock,
            ):
                client = TestClient(worker.app)
                res = client.post(
                    "/separate",
                    json={
                        "slug": "song",
                        "audio_path": str(audio),
                        "output_dir": str(stem_dir),
                        "device": "cpu",
                    },
                )

            self.assertEqual(res.status_code, 200)
            payload = res.json()
            self.assertTrue(payload.get("ok"))
            self.assertTrue(payload.get("cached"))
            run_mock.assert_not_called()

    def test_separate_runs_demucs_when_cache_miss(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "song.mp3"
            audio.write_bytes(b"a" * 4096)
            stem_dir = root / "separated" / "htdemucs" / "song"

            def fake_run_demucs(**kwargs):  # type: ignore[no-untyped-def]
                _ = kwargs
                stem_dir.mkdir(parents=True, exist_ok=True)
                for name in ("vocals", "bass", "drums", "other"):
                    (stem_dir / f"{name}.wav").write_bytes(b"x" * 4096)

            with (
                mock.patch.object(worker, "WORKER_API_KEY", ""),
                mock.patch.object(worker, "GPU_WORKER_REQUIRE_HMAC", False),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET", ""),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET_PREVIOUS", ""),
                mock.patch.object(worker, "GPU_WORKER_ALLOW_UNAUTH", True),
                mock.patch.object(worker, "WORKER_CACHE_ENABLED", True),
                mock.patch.object(worker, "_WORKER_SEMAPHORE", threading.Semaphore(1)),
                mock.patch.object(worker, "WORKER_STEM_MIN_BYTES", 1024),
                mock.patch.object(worker, "_run_demucs", side_effect=fake_run_demucs) as run_mock,
            ):
                client = TestClient(worker.app)
                res = client.post(
                    "/separate",
                    json={
                        "slug": "song",
                        "audio_path": str(audio),
                        "output_dir": str(stem_dir),
                        "force": True,
                    },
                )

            self.assertEqual(res.status_code, 200)
            payload = res.json()
            self.assertTrue(payload.get("ok"))
            self.assertFalse(payload.get("cached"))
            self.assertTrue((stem_dir / ".worker_meta.json").exists())
            run_mock.assert_called_once()

    def test_separate_rejects_audio_outside_allowed_roots_when_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "song.mp3"
            audio.write_bytes(b"a" * 4096)

            with (
                mock.patch.object(worker, "WORKER_API_KEY", ""),
                mock.patch.object(worker, "GPU_WORKER_REQUIRE_HMAC", False),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET", ""),
                mock.patch.object(worker, "GPU_WORKER_HMAC_SECRET_PREVIOUS", ""),
                mock.patch.object(worker, "GPU_WORKER_ALLOW_UNAUTH", True),
                mock.patch.object(worker, "WORKER_ENFORCE_AUDIO_ROOTS", True),
                mock.patch.object(worker, "WORKER_ALLOWED_AUDIO_ROOTS", [Path("/definitely/not/allowed")]),
                mock.patch.object(worker, "_WORKER_SEMAPHORE", threading.Semaphore(1)),
            ):
                client = TestClient(worker.app)
                res = client.post(
                    "/separate",
                    json={
                        "slug": "song",
                        "audio_path": str(audio),
                    },
                )

            self.assertEqual(res.status_code, 403)


if __name__ == "__main__":
    unittest.main()
