#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN=""
PYTHON_SOURCE=""

if [[ -x "venv/bin/python" ]]; then
  PYTHON_BIN="venv/bin/python"
  PYTHON_SOURCE="venv"
elif [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
  PYTHON_SOURCE=".venv"
elif command -v python3 >/dev/null 2>&1; then
  if [[ ! -x ".venv/bin/python" ]]; then
    echo "[local-fast] creating .venv via python3 -m venv .venv"
    python3 -m venv .venv
  fi
  PYTHON_BIN=".venv/bin/python"
  PYTHON_SOURCE="auto-created .venv"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
  PYTHON_SOURCE="python"
else
  echo "[local-fast] no Python interpreter found (tried venv/.venv/python3/python)." >&2
  exit 1
fi

echo "[local-fast] using Python interpreter (${PYTHON_SOURCE}): ${PYTHON_BIN}"

if ! "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
  echo "[local-fast] python pip is unavailable for ${PYTHON_BIN}" >&2
  exit 1
fi

# tests/test_main_pipeline imports scripts/step1_fetch, which depends on requests.
if ! "${PYTHON_BIN}" -c "import requests" >/dev/null 2>&1; then
  echo "[local-fast] requests missing; installing requests"
  "${PYTHON_BIN}" -m pip install --disable-pip-version-check requests
fi

if [[ ! -d "karaoapp/node_modules" ]]; then
  echo "[local-fast] karaoapp/node_modules missing; installing app deps via npm ci"
  npm -C karaoapp ci
fi

echo "[local-fast] backend unittest (${PYTHON_BIN})"
"${PYTHON_BIN}" -m unittest discover -s tests -p 'test_*.py' -v

if [[ "${SKIP_API_SMOKE:-0}" != "1" ]]; then
  echo "[local-fast] backend api integration smoke"
  "${PYTHON_BIN}" tests/smoke_api_integration.py
else
  echo "[local-fast] backend api integration smoke skipped (SKIP_API_SMOKE=1)"
fi

echo "[local-fast] frontend typecheck"
(
  cd karaoapp
  npx tsc --noEmit
)

echo "[local-fast] frontend e2e"
(
  cd karaoapp
  npm run test:e2e
)

echo "[local-fast] all checks passed"
