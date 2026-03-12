#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$APP_ROOT/.." && pwd)"
DEFAULT_API_BASE_URL="http://127.0.0.1:8000"

if [[ -z "${EXPO_PUBLIC_API_BASE_URL:-}" ]]; then
  export EXPO_PUBLIC_API_BASE_URL="${DEFAULT_API_BASE_URL}"
fi

echo "Using EXPO_PUBLIC_API_BASE_URL=${EXPO_PUBLIC_API_BASE_URL}"
echo "Starting local karaoapi backend (CLI-backed)..."
"$REPO_ROOT/start-backend-local.sh"

cd "$APP_ROOT"
exec npx expo start --ios --clear --localhost
