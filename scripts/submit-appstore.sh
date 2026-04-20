#!/bin/bash
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$REPO_ROOT/ios/SignoVivo.xcworkspace"
SCHEME="SignoVivo"
ARCHIVE_PATH="$REPO_ROOT/build/SignoVivo.xcarchive"
EXPORT_PATH="$REPO_ROOT/build/SignoVivo-ipa"
EXPORT_OPTIONS="$REPO_ROOT/scripts/export-options.plist"

echo "▶ Step 1: Archive"
xcodebuild archive \
  -workspace "$WORKSPACE" \
  -scheme "$SCHEME" \
  -configuration Release \
  -archivePath "$ARCHIVE_PATH" \
  -allowProvisioningUpdates

echo "▶ Step 2: Export & Upload"
xcodebuild -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$EXPORT_PATH" \
  -exportOptionsPlist "$EXPORT_OPTIONS" \
  -allowProvisioningUpdates

echo "✓ Done — check App Store Connect for the build."
