import unittest
from unittest import mock

try:
    from fastapi import HTTPException
    from fastapi.testclient import TestClient
    from karaoapi import app as api
except ModuleNotFoundError:
    HTTPException = Exception  # type: ignore[assignment]
    TestClient = None  # type: ignore[assignment]
    api = None


@unittest.skipIf(api is None, "fastapi is not installed in this test environment")
class KaraoApiRegressionGuards(unittest.TestCase):
    def test_output_route_skips_gzip_for_mp4(self) -> None:
        client = TestClient(api.app)
        probe_name = "_pytest_media_gzip_probe.mp4"
        probe_path = api.OUTPUT_DIR / probe_name
        probe_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Keep file above gzip minimum-size threshold to prove route exclusion works.
            probe_path.write_bytes(b"\x00" * 2048)
            response = client.get(f"/output/{probe_name}", headers={"accept-encoding": "gzip"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers.get("content-type"), "video/mp4")
            self.assertNotEqual((response.headers.get("content-encoding") or "").lower(), "gzip")
        finally:
            probe_path.unlink(missing_ok=True)

    def test_build_pipeline_argv_supports_forced_audio_url_and_id(self) -> None:
        base = api.Job(
            id="job-1",
            query="Artist - Title",
            slug=api.slugify("Artist - Title"),
            created_at=1.0,
            status="queued",
        )

        with mock.patch.object(api, "SERVER_DOWNLOAD_ONLY_ENFORCED", False):
            with_url = api.Job(**{**base.__dict__, "options": {"audio_url": "https://placeholder.invalid/audio.m4a"}})
            argv = api._build_pipeline_argv(with_url)
            self.assertIn("--audio-url", argv)
            self.assertIn("https://placeholder.invalid/audio.m4a", argv)
            self.assertNotIn("--audio-id", argv)

            with_id = api.Job(**{**base.__dict__, "options": {"audio_id": "dQw4w9WgXcQ"}})
            argv = api._build_pipeline_argv(with_id)
            self.assertIn("--audio-id", argv)
            self.assertIn("dQw4w9WgXcQ", argv)
            self.assertNotIn("--audio-url", argv)

            with_both = api.Job(
                **{**base.__dict__, "options": {"audio_url": "https://placeholder.invalid/a.m4a", "audio_id": "ignored"}}
            )
            argv = api._build_pipeline_argv(with_both)
            # audio_url takes precedence
            self.assertIn("--audio-url", argv)
            self.assertIn("https://placeholder.invalid/a.m4a", argv)
            self.assertNotIn("--audio-id", argv)

    def test_require_debug_key_hides_endpoints_when_unset(self) -> None:
        req = mock.Mock()
        req.headers = {"x-debug-key": "anything"}
        with mock.patch.object(api, "DEBUG_KEY", ""):
            with self.assertRaises(HTTPException) as ctx:
                api._require_debug_key(req)  # type: ignore[arg-type]
            self.assertEqual(ctx.exception.status_code, 404)

    def test_require_debug_key_rejects_missing_or_wrong_key(self) -> None:
        with mock.patch.object(api, "DEBUG_KEY", "secret"):
            req = mock.Mock()
            req.headers = {}
            with self.assertRaises(HTTPException) as ctx:
                api._require_debug_key(req)  # type: ignore[arg-type]
            self.assertEqual(ctx.exception.status_code, 403)

            req = mock.Mock()
            req.headers = {"x-debug-key": "wrong"}
            with self.assertRaises(HTTPException) as ctx:
                api._require_debug_key(req)  # type: ignore[arg-type]
            self.assertEqual(ctx.exception.status_code, 403)

            req = mock.Mock()
            req.headers = {"x-debug-key": "secret"}
            api._require_debug_key(req)  # type: ignore[arg-type]

    def test_configure_auto_offset_runtime_delegates_default_when_runtime_is_ready(self) -> None:
        with (
            mock.patch.dict(api.os.environ, {}, clear=True),
            mock.patch.object(
                api,
                "_probe_whisper_runtime",
                return_value=(True, "/tmp/whisper-cli", "/tmp/ggml-base.bin", "ok"),
            ),
        ):
            api._AUTO_OFFSET_RUNTIME_READY = False
            api._AUTO_OFFSET_RUNTIME_NOTE = ""

            api._configure_auto_offset_runtime()

            self.assertNotIn("KARAOKE_AUTO_OFFSET_ENABLED", api.os.environ)
            self.assertEqual(api.os.environ.get("MIXTERIOSO_WHISPER_BIN"), "/tmp/whisper-cli")
            self.assertEqual(api.os.environ.get("MIXTERIOSO_WHISPER_MODEL"), "/tmp/ggml-base.bin")
            self.assertTrue(api._AUTO_OFFSET_RUNTIME_READY)
            self.assertEqual(api._AUTO_OFFSET_RUNTIME_NOTE, "ready_default_delegated")
            self.assertTrue(api._auto_offset_effectively_enabled())

    def test_configure_auto_offset_runtime_disables_default_when_runtime_is_missing(self) -> None:
        with (
            mock.patch.dict(api.os.environ, {}, clear=True),
            mock.patch.object(
                api,
                "_probe_whisper_runtime",
                return_value=(False, "", "", "missing_bin_and_model"),
            ),
        ):
            api._AUTO_OFFSET_RUNTIME_READY = True
            api._AUTO_OFFSET_RUNTIME_NOTE = ""

            api._configure_auto_offset_runtime()

            self.assertEqual(api.os.environ.get("KARAOKE_AUTO_OFFSET_ENABLED"), "0")
            self.assertFalse(api._AUTO_OFFSET_RUNTIME_READY)
            self.assertEqual(api._AUTO_OFFSET_RUNTIME_NOTE, "disabled_runtime_not_ready:missing_bin_and_model")
            self.assertFalse(api._auto_offset_effectively_enabled())
