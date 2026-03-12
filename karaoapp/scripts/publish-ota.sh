#!/usr/bin/env bash

set -euo pipefail

CHANNEL="${1:-production}"
PLATFORM="${2:-ios}"
MESSAGE="${3:-}"

if ! command -v npx >/dev/null 2>&1; then
  echo "[ota] npx is required but was not found in PATH."
  exit 1
fi

if [ -z "${EXPO_TOKEN:-}" ]; then
  echo "[ota] EXPO_TOKEN is not set. EAS may ask for interactive login."
fi

if ! bash ./scripts/ota-doctor.sh --strict; then
  echo "[ota] OTA doctor failed. Fix configuration before publishing."
  exit 1
fi

PROJECT_ID="$(
  npx expo config --json \
    | node -e 'let data="";process.stdin.on("data",(c)=>data+=c);process.stdin.on("end",()=>{const cfg=JSON.parse(data);const id=((cfg.extra||{}).eas||{}).projectId||"";process.stdout.write(String(id).trim());});'
)"

if [ -z "$PROJECT_ID" ]; then
  cat <<'EOF'
[ota] Missing EAS project id.
[ota] Set EXPO_PUBLIC_EAS_PROJECT_ID in .env or run: npx eas project:init
EOF
  exit 1
fi

echo "[ota] Publishing update to channel=$CHANNEL platform=$PLATFORM projectId=$PROJECT_ID"

if [ -n "$MESSAGE" ]; then
  npx eas update --channel "$CHANNEL" --platform "$PLATFORM" --auto --message "$MESSAGE"
else
  npx eas update --channel "$CHANNEL" --platform "$PLATFORM" --auto
fi
