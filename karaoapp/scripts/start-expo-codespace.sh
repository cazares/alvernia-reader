#!/usr/bin/env bash
set -euo pipefail

if ! command -v gh >/dev/null 2>&1; then
  echo "Missing gh CLI. Install GitHub CLI and run: gh auth refresh -h github.com -s codespace"
  exit 1
fi

CWD_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if command -v git >/dev/null 2>&1; then
  echo "Updating local repo..."
  git -C "${CWD_ROOT}" pull --rebase || true
fi

ARG="${1:-}"
if [[ -z "${ARG}" ]]; then
  echo "Usage: $0 <codespace-name|public-url>"
  echo "Example: $0 effective-xylophone-xrvp9q7p7qhv4p7"
  echo "Example: $0 https://effective-xylophone-xrvp9q7p7qhv4p7-8000.app.github.dev/"
  exit 1
fi

BASE_URL=""
if [[ "${ARG}" == http://* || "${ARG}" == https://* ]]; then
  BASE_URL="${ARG%/}"
else
  CODESPACE_NAME="${ARG}"
  echo "Ensuring port 8000 is public in Codespace..."
  gh codespace ports visibility 8000:public -c "${CODESPACE_NAME}" >/dev/null 2>&1 || true

  PORTS_OUTPUT="$(gh codespace ports -c "${CODESPACE_NAME}")"
  BASE_URL="$(echo "${PORTS_OUTPUT}" | awk '$2=="8000" {print $4; exit}')"
  if [[ -n "${BASE_URL}" ]]; then
    BASE_URL="${BASE_URL%/}"
  fi
fi

if [[ -z "${BASE_URL}" ]]; then
  echo "Could not find public URL for port 8000."
  echo "Run: gh codespace ports visibility 8000:public -c <codespace-name>"
  exit 1
fi

ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
cat > "${ENV_FILE}" <<EOF_ENV
EXPO_PUBLIC_API_BASE_URL=${BASE_URL}
EOF_ENV

echo "Wrote ${ENV_FILE}"
echo "Using API base URL: ${BASE_URL}"
echo "Reminder: the API must be running in the Codespace:"
echo "  cd /workspaces/mixterioso"
echo "  /workspaces/mixterioso/karaoapi/scripts/start-server.sh"

cd "$(dirname "${ENV_FILE}")"

# Optional: quick health check before launching Expo.
if command -v curl >/dev/null 2>&1; then
  echo "Health check: ${BASE_URL}/health"
  if ! curl -sS "${BASE_URL}/health" >/dev/null; then
    echo "Warning: health check failed. Ensure the API is running in Codespace."
  fi
fi

# Use tunnel so the QR code is not localhost/LAN-dependent.
exec npx expo start --tunnel
