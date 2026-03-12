#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${ROOT}/logs"
LOG_FILE="${LOG_DIR}/karaoapi-server.log"
PID_FILE="${LOG_DIR}/karaoapi-server.pid"

mkdir -p "${LOG_DIR}"

if [[ -n "${CODESPACE_NAME:-}" ]]; then
  BASE_HINT="https://${CODESPACE_NAME}-8000.app.github.dev"
else
  BASE_HINT="https://<codespace-name>-8000.app.github.dev"
fi

echo "Codespaces API helper"
echo "1) Install deps if needed:"
echo "   python3 -m venv ${ROOT}/.venv"
echo "   source ${ROOT}/.venv/bin/activate"
echo "   python -m pip install -r ${ROOT}/requirements.txt"
echo "2) Start the API (backgrounded)"

nohup "${ROOT}/karaoapi/scripts/start-server.sh" > "${LOG_FILE}" 2>&1 &
SERVER_PID=$!
echo "${SERVER_PID}" > "${PID_FILE}"

sleep 1

echo "API log: ${LOG_FILE}"
echo "If the API failed to start, check the log above."
echo

cat <<EOF_TIP
Next steps:
- In the Ports panel, set port 8000 to Public.
- Health check:
  curl ${BASE_HINT}/health
- Use in Expo:
  EXPO_PUBLIC_API_BASE_URL=${BASE_HINT} npx expo start
EOF_TIP
