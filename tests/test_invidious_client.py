import unittest
from unittest import mock

import requests

from karaoapi.invidious_client import InvidiousClient


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._payload


class InvidiousClientTests(unittest.TestCase):
    def setUp(self) -> None:
        InvidiousClient._instance_failure_streak.clear()
        InvidiousClient._instance_cooldown_until.clear()

    def test_search_falls_back_and_cooldown_skips_failed_instance(self) -> None:
        session = mock.Mock(spec=requests.Session)
        session.get.side_effect = [
            requests.Timeout("timeout"),
            _FakeResponse([{"videoId": "a"}]),
            _FakeResponse([{"videoId": "b"}]),
        ]
        client = InvidiousClient(session=session, max_http_retries=0)
        client.instances = ["https://bad.example", "https://good.example"]

        first = client.search("song one", limit=1)
        second = client.search("song two", limit=1)

        self.assertEqual(first[0]["videoId"], "a")
        self.assertEqual(second[0]["videoId"], "b")
        self.assertEqual(session.get.call_count, 3)
        third_call_url = session.get.call_args_list[2].args[0]
        self.assertTrue(str(third_call_url).startswith("https://good.example/"))

    def test_get_audio_url_normalizes_relative_stream_url(self) -> None:
        session = mock.Mock(spec=requests.Session)
        session.get.return_value = _FakeResponse(
            {
                "title": "Song",
                "videoId": "dQw4w9WgXcQ",
                "lengthSeconds": "212",
                "adaptiveFormats": [
                    {"type": "audio/webm", "bitrate": "120000", "url": "/latest_version?id=abc"}
                ],
                "videoThumbnails": [
                    {"quality": "hqdefault", "url": "/vi/hq.jpg"},
                    {"quality": "maxres", "url": "/vi/maxres.jpg"},
                ],
            }
        )
        client = InvidiousClient(session=session, max_http_retries=0)
        client.instances = ["https://media.example"]

        audio_url, metadata = client.get_audio_url("dQw4w9WgXcQ")

        self.assertEqual(audio_url, "https://media.example/latest_version?id=abc")
        self.assertEqual(metadata["duration"], 212)
        self.assertEqual(metadata["thumbnail"], "https://media.example/vi/maxres.jpg")

    def test_search_validates_input_and_caps_limit(self) -> None:
        session = mock.Mock(spec=requests.Session)
        session.get.return_value = _FakeResponse(
            [{"videoId": str(i)} for i in range(100)]
        )
        client = InvidiousClient(session=session, max_http_retries=0)
        client.instances = ["https://api.example"]

        with self.assertRaises(ValueError):
            client.search("   ", limit=1)

        rows = client.search("hello", limit=10_000)
        self.assertEqual(len(rows), 25)


if __name__ == "__main__":
    unittest.main()
