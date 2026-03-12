import unittest

try:
    from fastapi.testclient import TestClient
    from karaoapi import app as appmod
    _FASTAPI_AVAILABLE = True
except Exception:
    TestClient = None  # type: ignore[assignment]
    appmod = None  # type: ignore[assignment]
    _FASTAPI_AVAILABLE = False


@unittest.skipUnless(_FASTAPI_AVAILABLE, "fastapi test dependencies are not installed")
class DownloadStrategyConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(appmod.app)
        self._orig_debug_key = appmod.DEBUG_KEY
        self._orig_server_download_only = appmod.SERVER_DOWNLOAD_ONLY_ENFORCED
        with appmod._download_strategy_lock:
            self._orig_override = appmod._download_strategy_override
            appmod._download_strategy_override = None
        appmod.DEBUG_KEY = "test-debug-key"
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = False

    def tearDown(self) -> None:
        appmod.DEBUG_KEY = self._orig_debug_key
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = self._orig_server_download_only
        with appmod._download_strategy_lock:
            appmod._download_strategy_override = self._orig_override

    def test_get_defaults_to_local_first_when_server_download_only_disabled(self) -> None:
        resp = self.client.get("/config/download-strategy")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload.get("strategy"), "local_first")
        self.assertTrue(payload.get("enableClientExtraction"))

    def test_get_forces_server_only_when_server_download_only_enabled(self) -> None:
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = True
        with appmod._download_strategy_lock:
            appmod._download_strategy_override = "local_first"
        resp = self.client.get("/config/download-strategy")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload.get("strategy"), "server_only")
        self.assertFalse(payload.get("enableClientExtraction"))

    def test_post_hidden_when_debug_key_unset(self) -> None:
        appmod.DEBUG_KEY = ""
        resp = self.client.post(
            "/config/download-strategy",
            json={"strategy": "server_only"},
            headers={"x-debug-key": "anything"},
        )
        self.assertEqual(resp.status_code, 404)

    def test_post_rejects_missing_or_wrong_debug_key(self) -> None:
        missing = self.client.post("/config/download-strategy", json={"strategy": "server_only"})
        self.assertEqual(missing.status_code, 403)

        wrong = self.client.post(
            "/config/download-strategy",
            json={"strategy": "server_only"},
            headers={"x-debug-key": "wrong"},
        )
        self.assertEqual(wrong.status_code, 403)

    def test_post_rejects_invalid_strategy(self) -> None:
        resp = self.client.post(
            "/config/download-strategy",
            json={"strategy": "bad_strategy"},
            headers={"x-debug-key": "test-debug-key"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid strategy", resp.json().get("detail", ""))

    def test_post_forbidden_when_server_download_only_enabled(self) -> None:
        appmod.SERVER_DOWNLOAD_ONLY_ENFORCED = True
        resp = self.client.post(
            "/config/download-strategy",
            json={"strategy": "local_first"},
            headers={"x-debug-key": "test-debug-key"},
        )
        self.assertEqual(resp.status_code, 403)
        self.assertIn("disabled", resp.json().get("detail", ""))

    def test_post_sets_override_and_get_reflects_it(self) -> None:
        set_resp = self.client.post(
            "/config/download-strategy",
            json={"strategy": "server_fallback"},
            headers={"x-debug-key": "test-debug-key"},
        )
        self.assertEqual(set_resp.status_code, 200)
        self.assertEqual(set_resp.json().get("strategy"), "server_fallback")

        get_resp = self.client.get("/config/download-strategy")
        self.assertEqual(get_resp.status_code, 200)
        payload = get_resp.json()
        self.assertEqual(payload.get("strategy"), "server_fallback")
        self.assertTrue(payload.get("enableClientExtraction"))


if __name__ == "__main__":
    unittest.main()
