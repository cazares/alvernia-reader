#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if command -v git >/dev/null 2>&1; then
  echo "Updating repo in Codespace..."
  git -C "${ROOT}" pull --rebase || true
fi

cd "${ROOT}"

export PYTHONPATH="${ROOT}"

exec /workspaces/mixterioso/karaoapi/scripts/start-server.sh
