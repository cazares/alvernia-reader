#!/usr/bin/env bash
# sim-build.sh — build the REAL SignoVivo app for the iOS Simulator from a THROWAWAY COPY of this tree, with
# the mesh and relay ISOLATED so nothing built here can ever pair with, or publish to, a real device or the
# production relay. Two simulators built this way DO pair with each other over real Multipeer (BLE cannot run
# in a simulator). See README.md in this directory for the full recipe.
#
#   • Multipeer service type  signovivo  → svsimtest   (Swift constant + both NSBonjourServices entries)
#   • DIRECTOR_SESSION        "1234"     → "7777"
#   • RELAY_BASE (native transmitter, native check-in/log, web bundle) → http://localhost:8787 (a local
#     `wrangler dev --local` of sync-worker — relay-dev.sh; NSAllowsLocalNetworking is already true in Info.plist)
#
# The copy lives in $SV_SIM_COPY (default ~/sv-sim-build) and is NEVER committed. Refresh it with sync-copy.sh.
# Product: $SV_SIM_COPY/build-sim/Build/Products/Release-iphonesimulator/SignoVivo.app
set -euo pipefail
COPY="${SV_SIM_COPY:-$HOME/sv-sim-build}"
cd "$COPY"
[ "$PWD" = "$COPY" ] || { echo "refusing: not in the throwaway copy"; exit 2; }
[ -d ios/Pods ] || { echo "refusing: ios/Pods missing in the copy (run pod install there once)"; exit 2; }
[ -d node_modules ] || { echo "refusing: node_modules missing in the copy (run npm ci there once)"; exit 2; }

if ! grep -q 'serviceType = "svsimtest"' ios/SignoVivo/DirectorSyncModule.swift; then
python3 - <<'PY'
def sub(p, a, b):
    s = open(p).read()
    assert s.count(a) == 1, (p, a, s.count(a))
    open(p, "w").write(s.replace(a, b))
sub("ios/SignoVivo/DirectorSyncModule.swift", 'private static let serviceType = "signovivo"', 'private static let serviceType = "svsimtest"')
sub("ios/SignoVivo/Info.plist", "<string>_signovivo._tcp</string>", "<string>_svsimtest._tcp</string>")
sub("ios/SignoVivo/Info.plist", "<string>_signovivo._udp</string>", "<string>_svsimtest._udp</string>")
sub("PdfReaderApp.tsx", 'const DIRECTOR_SESSION = "1234";', 'const DIRECTOR_SESSION = "7777";')
sub("PdfReaderApp.tsx", 'const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";', 'const RELAY_BASE = "http://localhost:8787";')
sub("src/directorRelaySync.js", 'const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";', 'const RELAY_BASE = "http://localhost:8787";')
print("isolation patches applied")
PY
fi
grep -q 'serviceType = "svsimtest"' ios/SignoVivo/DirectorSyncModule.swift
grep -q '_svsimtest._tcp' ios/SignoVivo/Info.plist
grep -q 'DIRECTOR_SESSION = "7777"' PdfReaderApp.tsx
! grep -q 'signovivo-sync.4j4982y8jp.workers.dev' PdfReaderApp.tsx src/directorRelaySync.js
echo "isolation verified: svsimtest / 7777 / localhost relay"

export LANG=en_US.UTF-8
echo "==> web bundle (relay → localhost:8787)"
ALVERNIA_RELAY_BASE=http://localhost:8787 node web/build.mjs > /tmp/sv-sim-web.log 2>&1
grep -q 'localhost:8787' web/dist/app.js || { echo "web bundle did not take the local relay base"; exit 1; }
rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle

# "The sandbox is not in sync with the Podfile.lock": the tracked Podfile.lock comes from git while
# Pods/Manifest.lock came from `pod install`. release.sh works around it the same way (its line
# `cp ios/Pods/Manifest.lock ios/Podfile.lock`); this copy is throwaway, so nothing to restore.
cp ios/Pods/Manifest.lock ios/Podfile.lock
echo "==> xcodebuild (simulator, Release) — log /tmp/sv-sim-xcodebuild.log"
xcodebuild -workspace ios/SignoVivo.xcworkspace -scheme SignoVivo -configuration Release \
  -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' \
  -derivedDataPath build-sim build > /tmp/sv-sim-xcodebuild.log 2>&1
APP=build-sim/Build/Products/Release-iphonesimulator/SignoVivo.app
[ -d "$APP" ] || { echo "BUILD FAILED — tail of log:"; tail -20 /tmp/sv-sim-xcodebuild.log; exit 1; }
echo "SIM BUILD OK: $PWD/$APP"
plutil -p "$APP/Info.plist" | grep -E 'CFBundleVersion|NSBonjourServices' -A2 | head -6
