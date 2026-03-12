#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <api-origin>"
  echo "Example: $0 https://my-live-api.example.com"
  exit 1
fi

ORIGIN="${1%/}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "${SCRIPT_DIR}"
npx wrangler deploy --var "API_ORIGIN=${ORIGIN}"
