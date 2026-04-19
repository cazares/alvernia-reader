#!/bin/bash
set -e

WORKSPACE="ios/SignoVivo.xcworkspace"
SCHEME="SignoVivo"
ARCHIVE_PATH="build/SignoVivo.xcarchive"
EXPORT_PATH="build/SignoVivo-ipa"
EXPORT_OPTIONS="scripts/export-options.plist"

echo "▶ Step 1: Archive"
xcodebuild archive \
  -workspace "$WORKSPACE" \
  -scheme "$SCHEME" \
  -configuration Release \
  -archivePath "$ARCHIVE_PATH" \
  -allowProvisioningUpdates \
  | xcpretty || true

echo "▶ Step 2: Export & Upload"
xcodebuild -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$EXPORT_PATH" \
  -exportOptionsPlist "$EXPORT_OPTIONS" \
  -allowProvisioningUpdates

echo "✓ Done — check App Store Connect for the build."
