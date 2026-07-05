#!/usr/bin/env bash
#
# Lockstep release — ONE version, BOTH surfaces.
#
# The native app is a thin shell that WRAPS the web bundle, so web + native are really one artifact
# and MUST carry the same build number. This script is the only blessed way to cut a build: it bumps
# a single source (version.json), then builds the web bundle, the native IPA, AND deploys the web —
# all from that one number. The native overlay (`b<N>`), the WebBundle baked into the app, and
# signovivo.com's `v<N>` badge therefore always agree.
#
# DO NOT bump/build/deploy one surface by hand — that's exactly how the numbers drift.
#
# Usage:  bash scripts/release.sh            # bump + build web + build native IPA + deploy web (PROD)
#         SKIP_NATIVE=1 bash scripts/release.sh   # web-only refresh (still rebuilds+deploys web at the bumped version)
#         STAGING=1 bash scripts/release.sh   # CANARY: build web at the CURRENT version + deploy to the
#                                             #   isolated Pages preview branch 'staging'. NO version bump,
#                                             #   NO native archive, NEVER touches production ('main') or
#                                             #   TestFlight. Prove a build here (signovivo.com?env=staging)
#                                             #   before promoting to prod.
#
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8
LOG=/tmp/release-native.log

# STAGING is the canary path — physically incapable of touching prod: it implies
# no-native + no-bump and flips the Pages deploy to the preview branch. With STAGING
# unset, everything below is byte-for-byte the production flow.
STAGING="${STAGING:-0}"
if [ "$STAGING" = "1" ]; then
  SKIP_NATIVE=1
  DEPLOY_BRANCH="staging"
else
  DEPLOY_BRANCH="main"
fi

if [ "$STAGING" = "1" ]; then
  echo "==> 1/6  STAGING/CANARY -> skip bump; build web at the CURRENT version (prod untouched)"
  BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
  echo "         build = $BUILD (unchanged)"
else
  echo "==> 1/6  Bump version.json (+ native CFBundleVersion / Info.plist / app.json / offlineWebBundle)"
  node scripts/bump-build.mjs
  BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
  echo "         build = $BUILD"
fi

echo "==> 2/6  Rebuild web bundle (bakes v$BUILD into the badge + a content-hashed cache version)"
node web/build.mjs >/dev/null

if [ "$STAGING" = "1" ]; then
  echo "==> 3/6  STAGING -> skip ios/WebBundle sync (web-only canary; native untouched)"
else
  echo "==> 3/6  Sync web/dist -> ios/WebBundle (native wraps the SAME bundle)"
  rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle
fi

if [ "${SKIP_NATIVE:-0}" = "1" ]; then
  echo "==> 4/6  SKIP_NATIVE=1 -> skipping the native archive"
else
  echo "==> 4/6  Build native IPA (Release archive + export) — logging to $LOG"
  cp ios/Pods/Manifest.lock ios/Podfile.lock         # pod-guard workaround (Ruby 4.0.1 pod install is broken)
  # Bake the REAL standard-director codes (gitignored PII) into the RN bundle for this archive only,
  # then restore the empty committed file — keeps the numbers out of the public repo.
  RESTORE_CODES=""
  if [ -f director-codes.private.json ]; then
    cp director-codes.json director-codes.committed.bak
    cp director-codes.private.json director-codes.json
    RESTORE_CODES="1"
    echo "         baked director-codes.private.json into the bundle"
  else
    echo "         WARNING: director-codes.private.json missing — standard-director entry disabled in this build"
  fi
  restore_codes() { [ -n "$RESTORE_CODES" ] && mv -f director-codes.committed.bak director-codes.json; }
  rm -rf build
  if ! xcodebuild -workspace ios/SignoVivo.xcworkspace -scheme SignoVivo -configuration Release \
        -archivePath build/SignoVivo.xcarchive -sdk iphoneos -allowProvisioningUpdates clean archive >"$LOG" 2>&1; then
    git checkout -- ios/Podfile.lock; restore_codes; echo "ARCHIVE FAILED — tail of $LOG:"; tail -25 "$LOG"; exit 1
  fi
  if ! xcodebuild -exportArchive -archivePath build/SignoVivo.xcarchive -exportPath build/export \
        -exportOptionsPlist ios/exportOptions.app-store.plist -allowProvisioningUpdates >>"$LOG" 2>&1; then
    git checkout -- ios/Podfile.lock; restore_codes; echo "EXPORT FAILED — tail of $LOG:"; tail -25 "$LOG"; exit 1
  fi
  git checkout -- ios/Podfile.lock
  restore_codes
  cp build/export/SignoVivo.ipa "$HOME/Desktop/SignoVivo-$BUILD.ipa"
  echo "         IPA -> ~/Desktop/SignoVivo-$BUILD.ipa"
fi

if [ "$STAGING" = "1" ]; then
  echo "==> 5/6  Deploy web to the ISOLATED Pages preview branch '$DEPLOY_BRANCH' (NOT signovivo.com)"
else
  echo "==> 5/6  Deploy web to signovivo.com (production = Pages branch '$DEPLOY_BRANCH')"
fi
npx wrangler pages deploy web/dist --project-name alvernia-reader --branch "$DEPLOY_BRANCH" --commit-dirty=true

if [ "$STAGING" = "1" ]; then
  echo "==> 6/6  DONE — staging preview deployed. Prove it on the canary iPad (open the preview URL"
  echo "         printed above, or signovivo.com?env=staging once promoted), THEN promote to prod:"
  echo "           bash scripts/release.sh          # full prod (bump + native + deploy main)"
  echo "         Production (branch 'main' / signovivo.com) and TestFlight were NOT touched."
else
  echo "==> 6/6  DONE — signovivo.com == native == v$BUILD"
  if [ "${SKIP_NATIVE:-0}" != "1" ]; then
    echo "         Final manual step (native to TestFlight):"
    echo "           open -a Transporter ~/Desktop/SignoVivo-$BUILD.ipa   then click DELIVER"
  fi
fi
