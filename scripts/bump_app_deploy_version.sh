#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$ROOT_DIR/karaoapp/version.json"
IOS_PBXPROJ="$ROOT_DIR/karaoapp/ios/Mixterious.xcodeproj/project.pbxproj"
IOS_INFO_PLIST="$ROOT_DIR/karaoapp/ios/Mixterious/Info.plist"

mkdir -p "$(dirname "$VERSION_FILE")"

NEXT_VERSION="$(node - "$VERSION_FILE" <<'NODE'
const fs = require("fs");

const versionFile = process.argv[2];
const defaults = {
  baseVersion: "1.0",
  buildNumber: 0,
};

let data = defaults;
try {
  const parsed = JSON.parse(fs.readFileSync(versionFile, "utf8"));
  data = { ...defaults, ...(parsed || {}) };
} catch {
  data = defaults;
}

const baseVersion = String(data.baseVersion || defaults.baseVersion).trim() || defaults.baseVersion;
let buildNumber = Number(data.buildNumber);
if (!Number.isFinite(buildNumber) || buildNumber < 0) buildNumber = 0;
buildNumber = Math.floor(buildNumber) + 1;

const next = {
  baseVersion,
  buildNumber,
};

fs.writeFileSync(versionFile, `${JSON.stringify(next, null, 2)}\n`, "utf8");
process.stdout.write(`${baseVersion}.${buildNumber}`);
NODE
)"

BASE_VERSION="${NEXT_VERSION%.*}"
BUILD_NUMBER="${NEXT_VERSION##*.}"

if [[ -f "$IOS_PBXPROJ" ]]; then
  perl -0pi -e "s/CURRENT_PROJECT_VERSION = [^;]+;/CURRENT_PROJECT_VERSION = ${BUILD_NUMBER};/g; s/MARKETING_VERSION = [^;]+;/MARKETING_VERSION = ${BASE_VERSION};/g" "$IOS_PBXPROJ"
fi

if [[ -f "$IOS_INFO_PLIST" ]]; then
  /usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString \$(MARKETING_VERSION)" "$IOS_INFO_PLIST" >/dev/null 2>&1 || \
    /usr/libexec/PlistBuddy -c "Add :CFBundleShortVersionString string \$(MARKETING_VERSION)" "$IOS_INFO_PLIST"
  /usr/libexec/PlistBuddy -c "Set :CFBundleVersion \$(CURRENT_PROJECT_VERSION)" "$IOS_INFO_PLIST" >/dev/null 2>&1 || \
    /usr/libexec/PlistBuddy -c "Add :CFBundleVersion string \$(CURRENT_PROJECT_VERSION)" "$IOS_INFO_PLIST"
fi

printf "%s\n" "$NEXT_VERSION"
