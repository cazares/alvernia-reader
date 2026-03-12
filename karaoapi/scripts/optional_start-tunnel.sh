#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${ROOT}/logs"
LOG_FILE="${LOG_DIR}/karaoapi-server.log"
PID_FILE="${LOG_DIR}/karaoapi-server.pid"
STARTED_SERVER=0

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

port_listening() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:8000 -sTCP:LISTEN -P -n >/dev/null 2>&1
    return $?
  fi
  return 1
}

require_cmd ngrok

if port_listening; then
  echo "Port 8000 already in use; assuming KaraoAPI is running."
else
  mkdir -p "${LOG_DIR}"
  echo "Starting KaraoAPI..."
  nohup "${ROOT}/karaoapi/scripts/start-server.sh" > "${LOG_FILE}" 2>&1 &
  SERVER_PID=$!
  echo "${SERVER_PID}" > "${PID_FILE}"
  STARTED_SERVER=1

  sleep 1
  if ! port_listening; then
    echo "KaraoAPI failed to start. Log: ${LOG_FILE}"
    tail -n 60 "${LOG_FILE}" || true
    exit 1
  fi
fi

cleanup() {
  if [[ "${STARTED_SERVER}" -eq 1 ]]; then
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "Stopping KaraoAPI (pid ${SERVER_PID})"
      kill "${SERVER_PID}" || true
    fi
  fi
}
trap cleanup EXIT

echo "Starting ngrok..."
echo "When you see the https://... URL, use it as EXPO_PUBLIC_API_BASE_URL."
ngrok http 8000 --log=stdout
