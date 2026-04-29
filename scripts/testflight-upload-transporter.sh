#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SCHEME="${SCHEME:-SignoVivo}"
WORKSPACE="${WORKSPACE:-ios/SignoVivo.xcworkspace}"
CONFIGURATION="${CONFIGURATION:-Release}"
EXPORT_OPTIONS_PLIST="${EXPORT_OPTIONS_PLIST:-ios/exportOptions.app-store.plist}"

BUILD_DIR="${BUILD_DIR:-build}"
ARCHIVE_PATH="${ARCHIVE_PATH:-$BUILD_DIR/SignoVivo.xcarchive}"
EXPORT_PATH="${EXPORT_PATH:-$BUILD_DIR/export}"

IPA_NAME="${IPA_NAME:-SignoVivo.ipa}"
IPA_PATH="$EXPORT_PATH/$IPA_NAME"

ASC_KEY_ID="${ASC_KEY_ID:-}"
ASC_ISSUER_ID="${ASC_ISSUER_ID:-}"

echo "==> Archiving ($SCHEME)"
rm -rf "$BUILD_DIR"
xcodebuild \
  -workspace "$WORKSPACE" \
  -scheme "$SCHEME" \
  -configuration "$CONFIGURATION" \
  -archivePath "$ARCHIVE_PATH" \
  -sdk iphoneos \
  -allowProvisioningUpdates \
  clean archive

echo "==> Exporting IPA"
mkdir -p "$EXPORT_PATH"
xcodebuild \
  -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$EXPORT_PATH" \
  -exportOptionsPlist "$EXPORT_OPTIONS_PLIST" \
  -allowProvisioningUpdates

if [[ ! -f "$IPA_PATH" ]]; then
  echo "Expected IPA not found at: $IPA_PATH" >&2
  echo "Contents of export dir:" >&2
  ls -la "$EXPORT_PATH" >&2
  exit 1
fi

if [[ -z "$ASC_KEY_ID" || -z "$ASC_ISSUER_ID" ]]; then
  echo "Set ASC_KEY_ID + ASC_ISSUER_ID for Transporter upload." >&2
  exit 1
fi

if ! xcrun iTMSTransporter -help >/dev/null 2>&1; then
  echo "iTMSTransporter not available via xcrun. Install Xcode and/or Transporter." >&2
  exit 1
fi

echo "==> Uploading via Transporter"
xcrun iTMSTransporter \
  -m upload \
  -assetFile "$IPA_PATH" \
  -apiKey "$ASC_KEY_ID" \
  -apiIssuer "$ASC_ISSUER_ID"

echo "==> Done. Build uploaded: $IPA_PATH"

