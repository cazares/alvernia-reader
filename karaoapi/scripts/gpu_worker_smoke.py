#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def _normalize_base_url(raw: str) -> str:
    value = (raw or "").strip().rstrip("/")
    if not value:
        raise ValueError("base URL is required")
    if not value.startswith("http://") and not value.startswith("https://"):
        value = "https://" + value
    return value


def _request_json(url: str, payload: dict | None, *, timeout: float, api_key: str) -> dict:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(url, data=body, headers=headers, method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"request failed for {url}: {exc}") from exc

    try:
        parsed = json.loads(raw or "{}")
    except Exception as exc:
        raise RuntimeError(f"non-JSON response from {url}: {raw[:500]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"unexpected response type from {url}: {type(parsed).__name__}")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Smoke test a Mixterioso GPU worker endpoint")
    p.add_argument("--base-url", required=True, help="Worker base URL, e.g. https://gpu.placeholder.invalid")
    p.add_argument("--audio-path", required=True, help="Path to source audio visible to worker runtime")
    p.add_argument("--slug", default="gpu_worker_smoke", help="Slug for smoke run")
    p.add_argument("--timeout-sec", type=float, default=60.0, help="HTTP timeout for each request")
    p.add_argument("--api-key", default="", help="Optional bearer token")
    p.add_argument("--output-dir", default="", help="Optional output stem dir/base dir for worker")
    p.add_argument("--two-stems", action="store_true", help="Request two-stem mode")
    p.add_argument("--device", default="auto", help="Demucs device override (auto|cuda|mps|cpu)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    base = _normalize_base_url(args.base_url)
    audio_path = Path(args.audio_path)
    if not audio_path.exists():
        print(f"[smoke] audio path does not exist: {audio_path}", file=sys.stderr)
        return 2

    health_url = urllib.parse.urljoin(base + "/", "health")
    separate_url = urllib.parse.urljoin(base + "/", "separate")

    print(f"[smoke] GET {health_url}")
    health = _request_json(health_url, None, timeout=float(args.timeout_sec), api_key=args.api_key)
    print(json.dumps(health, indent=2))

    payload = {
        "slug": args.slug,
        "audio_path": str(audio_path),
        "two_stems": bool(args.two_stems),
        "device": str(args.device or "auto"),
    }
    if args.output_dir:
        payload["output_dir"] = str(args.output_dir)

    print(f"[smoke] POST {separate_url}")
    print(f"[smoke] payload={json.dumps(payload)}")
    result = _request_json(separate_url, payload, timeout=float(args.timeout_sec), api_key=args.api_key)
    print(json.dumps(result, indent=2))

    if not bool(result.get("ok")):
        print("[smoke] worker returned ok=false", file=sys.stderr)
        return 1
    print("[smoke] worker separation smoke succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
