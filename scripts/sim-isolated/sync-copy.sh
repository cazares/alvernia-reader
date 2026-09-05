#!/usr/bin/env bash
# sync-copy.sh — refresh the throwaway simulator copy ($SV_SIM_COPY, default ~/sv-sim-build) from THIS tree.
#
# Excludes everything that is expensive or generated (deps, Pods, DerivedData, built web bundle). --delete
# is safe ONLY because every helper the copy needs now lives in this tree under scripts/sim-isolated/ —
# on 2026-09-04 a sync with --delete removed helper scripts that lived only in the copy, the "rebuild"
# silently did nothing, and the OLD app was reinstalled and tested as if it were the fix.
set -euo pipefail
SRC="$(cd "$(dirname "$0")/../.." && pwd)"
COPY="${SV_SIM_COPY:-$HOME/sv-sim-build}"
[ -d "$COPY" ] || { echo "no copy at $COPY — create it once with: cp -R <tree> $COPY && (cd $COPY && npm ci && cd ios && pod install)"; exit 2; }
rsync -a --delete \
  --exclude .git --exclude .claude --exclude node_modules --exclude ios/Pods --exclude build-sim \
  --exclude web/dist --exclude ios/WebBundle --exclude ios/build --exclude sync-worker/.wrangler \
  "$SRC/" "$COPY/"
echo "synced $SRC -> $COPY (isolation patches will re-apply on the next sim-build.sh run)"
