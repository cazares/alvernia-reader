#!/usr/bin/env bash
# Rebuild the web bundle and sync it into ios/WebBundle for a LOCAL Xcode GUI build/run.
#
# This is the two commands release.sh's step 2-3 always runs before a real archive
# (node web/build.mjs; rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle), pulled out on
# their own so a plain `xcodebuild`/Xcode-GUI Run has a populated ios/WebBundle to bake in
# without going through release.sh's archive/export/upload machinery. Xcode's own build does
# NOT run web/build.mjs for you — a fresh worktree's ios/WebBundle does not exist at all until
# something runs this (or release.sh), and the build fails with "The file WebBundle couldn't be
# opened because there is no such file."
#
# ios/WebBundle MUST stay the target (see .gitignore) — the pbxproj's WebBundle file reference
# resolves relative to ios/ (a bare `path = WebBundle`, not `SignoVivo/WebBundle`), matching
# every other release.sh-driven build. Pointing this anywhere else desyncs from release.sh and,
# if ever committed, bakes ~27 MB of generated page images into git history by accident.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> Building web bundle (web/dist)"
node web/build.mjs

echo "==> Syncing web/dist -> ios/WebBundle"
rm -rf ios/WebBundle
cp -R web/dist ios/WebBundle

echo "==> Done. Open ios/SignoVivo.xcworkspace, Product > Clean Build Folder, then Run."
