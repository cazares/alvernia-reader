import time
import unittest

from scripts.worker_security import (
    NonceReplayCache,
    build_signed_headers,
    verify_signed_headers,
)


class WorkerSecurityTests(unittest.TestCase):
    def test_sign_and_verify_round_trip(self) -> None:
        body = b'{"job_id":"abc","source_uri":"gs://bucket/source.mp3"}'
        secret = "super-secret"
        headers = build_signed_headers(body_bytes=body, secret=secret, timestamp_sec=int(time.time()), nonce="nonce-12345678")
        ok, detail = verify_signed_headers(
            body_bytes=body,
            headers=headers,
            accepted_secrets=[secret],
            replay_cache=NonceReplayCache(ttl_sec=120, max_entries=1000),
            max_skew_sec=120,
        )
        self.assertTrue(ok)
        self.assertEqual(detail, "")

    def test_verify_rejects_replay(self) -> None:
        body = b'{"job_id":"abc"}'
        secret = "super-secret"
        nonce = "nonce-replay-123"
        ts = int(time.time())
        headers = build_signed_headers(body_bytes=body, secret=secret, timestamp_sec=ts, nonce=nonce)
        cache = NonceReplayCache(ttl_sec=120, max_entries=1000)

        ok1, detail1 = verify_signed_headers(
            body_bytes=body,
            headers=headers,
            accepted_secrets=[secret],
            replay_cache=cache,
            max_skew_sec=120,
        )
        ok2, detail2 = verify_signed_headers(
            body_bytes=body,
            headers=headers,
            accepted_secrets=[secret],
            replay_cache=cache,
            max_skew_sec=120,
        )
        self.assertTrue(ok1)
        self.assertEqual(detail1, "")
        self.assertFalse(ok2)
        self.assertIn("replay", detail2)

    def test_verify_rejects_skew(self) -> None:
        body = b'{"job_id":"abc"}'
        secret = "super-secret"
        old_ts = int(time.time()) - 10_000
        headers = build_signed_headers(
            body_bytes=body,
            secret=secret,
            timestamp_sec=old_ts,
            nonce="nonce-skew-1234",
        )
        ok, detail = verify_signed_headers(
            body_bytes=body,
            headers=headers,
            accepted_secrets=[secret],
            replay_cache=NonceReplayCache(ttl_sec=120, max_entries=1000),
            max_skew_sec=60,
        )
        self.assertFalse(ok)
        self.assertIn("replay window", detail)

    def test_verify_accepts_previous_secret(self) -> None:
        body = b'{"job_id":"abc"}'
        prev_secret = "secret-v1"
        headers = build_signed_headers(
            body_bytes=body,
            secret=prev_secret,
            timestamp_sec=int(time.time()),
            nonce="nonce-prev-secret",
        )
        ok, detail = verify_signed_headers(
            body_bytes=body,
            headers=headers,
            accepted_secrets=["secret-v2", prev_secret],
            replay_cache=NonceReplayCache(ttl_sec=120, max_entries=1000),
            max_skew_sec=120,
        )
        self.assertTrue(ok)
        self.assertEqual(detail, "")


if __name__ == "__main__":
    unittest.main()
