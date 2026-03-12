#!/usr/bin/env python3
"""Lightweight API integration smoke test for CI.

This exercises core KaraoAPI routes without running the full pipeline:
- GET /health
- GET /healthz
- GET /readyz
- POST /jobs
- GET /jobs/{id}
- POST /jobs/{id}/cancel
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

try:
    from fastapi.testclient import TestClient
except Exception:  # pragma: no cover - optional in lightweight local envs
    TestClient = None  # type: ignore[assignment]

# Ensure repo root is importable when this script is run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from karaoapi import app as api
except Exception:  # pragma: no cover - optional in lightweight local envs
    api = None  # type: ignore[assignment]


class _DummyFuture:
    def result(self, timeout: float | None = None) -> None:
        return None


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> None:
    if TestClient is None or api is None:
        print("API integration smoke skipped: fastapi test dependencies are not installed.")
        return

    with api._jobs_lock:
        jobs_snapshot = dict(api._jobs)
        slug_snapshot = dict(api._slug_to_job_id)
        active_snapshot = api._active_job_count_cached

    try:
        with (
            mock.patch.object(api, "_persist_jobs", return_value=None),
            mock.patch.object(api._executor, "submit", return_value=_DummyFuture()),
        ):
            client = TestClient(api.app)

            health = client.get("/health")
            _require(health.status_code == 200, f"/health expected 200, got {health.status_code}")
            health_payload = health.json()
            _require(health_payload.get("status") == "ok", f"/health unexpected payload: {health_payload}")

            healthz = client.get("/healthz")
            _require(healthz.status_code == 200, f"/healthz expected 200, got {healthz.status_code}")
            _require(bool(healthz.json().get("ok")), f"/healthz unexpected payload: {healthz.json()}")

            readyz = client.get("/readyz")
            _require(readyz.status_code == 200, f"/readyz expected 200, got {readyz.status_code}")

            create = client.post(
                "/jobs",
                json={
                    "query": "smoke test artist - smoke test title",
                    "idempotency_key": "smoke-job-1",
                },
            )
            _require(create.status_code == 200, f"POST /jobs expected 200, got {create.status_code}")
            created_payload = create.json()
            job_id = str(created_payload.get("id") or "").strip()
            _require(bool(job_id), f"POST /jobs did not return job id: {created_payload}")

            poll = client.get(f"/jobs/{job_id}")
            _require(poll.status_code == 200, f"GET /jobs/{{id}} expected 200, got {poll.status_code}")
            polled_payload = poll.json()
            _require(polled_payload.get("id") == job_id, f"GET /jobs/{{id}} returned wrong id: {polled_payload}")

            cancel = client.post(f"/jobs/{job_id}/cancel")
            _require(cancel.status_code == 200, f"POST /jobs/{{id}}/cancel expected 200, got {cancel.status_code}")

            print("API integration smoke passed.")
    finally:
        with api._jobs_lock:
            api._jobs.clear()
            api._jobs.update(jobs_snapshot)
            api._slug_to_job_id.clear()
            api._slug_to_job_id.update(slug_snapshot)
            api._active_job_count_cached = active_snapshot


if __name__ == "__main__":
    main()
