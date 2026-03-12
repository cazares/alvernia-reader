#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

if ! command -v demucs >/dev/null 2>&1; then
  echo "demucs is required on PATH." >&2
  exit 1
fi

if ! python3 - <<'PY' >/dev/null 2>&1; then
import importlib.util
mods = ("fastapi", "uvicorn")
missing = [m for m in mods if importlib.util.find_spec(m) is None]
if missing:
    raise SystemExit(1)
PY
  echo "fastapi/uvicorn are required in this Python environment." >&2
  exit 1
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8080}"

echo "Starting GPU worker on ${HOST}:${PORT}"
exec python3 -m uvicorn karaoapi.gpu_worker_app:app --host "${HOST}" --port "${PORT}" --proxy-headers
