#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEVICE_ID="${MIXTERIOSO_MPHONE_DEVICE_ID:-00008150-000819343A38401C}"
PORT="${MIXTERIOSO_EXPO_PORT:-8086}"

echo "Bumping app build/version..."
"$ROOT_DIR/scripts/bump_app_deploy_version.sh"

echo "Installing Release build to device: $DEVICE_ID"
cd "$ROOT_DIR/karaoapp"
npx expo run:ios --device "$DEVICE_ID" --configuration Release --port "$PORT"
