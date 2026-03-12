#!/usr/bin/env bash
set -euo pipefail

# Deploy the latest Android debug APK to a connected physical device.
# Usage examples:
#   ./scripts/deploy-android-device.sh
#   ./scripts/deploy-android-device.sh --serial ZL8325BK6C
#   ./scripts/deploy-android-device.sh --apk ./android/app/build/outputs/apk/debug/app-debug.apk

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_APK="${APP_ROOT}/android/app/build/outputs/apk/debug/app-debug.apk"

PACKAGE_NAME="com.cazares.mixterious"
MAIN_ACTIVITY=".MainActivity"
APK_PATH="${DEFAULT_APK}"
DEVICE_SERIAL=""
WAIT_SECONDS=60

print_usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --serial <id>       Device serial (defaults to first physical ADB device)
  --apk <path>        APK path (default: ${DEFAULT_APK})
  --package <name>    Android package (default: ${PACKAGE_NAME})
  --activity <name>   Activity name (default: ${MAIN_ACTIVITY})
  --wait <seconds>    Max wait for device visibility (default: ${WAIT_SECONDS})
  -h, --help          Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --serial)
      DEVICE_SERIAL="${2:-}"
      shift 2
      ;;
    --apk)
      APK_PATH="${2:-}"
      shift 2
      ;;
    --package)
      PACKAGE_NAME="${2:-}"
      shift 2
      ;;
    --activity)
      MAIN_ACTIVITY="${2:-}"
      shift 2
      ;;
    --wait)
      WAIT_SECONDS="${2:-}"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

if ! command -v adb >/dev/null 2>&1; then
  echo "adb not found in PATH." >&2
  exit 1
fi

if [[ ! -f "${APK_PATH}" ]]; then
  echo "APK not found: ${APK_PATH}" >&2
  echo "Build it first, e.g. with: (cd ${APP_ROOT} && npx expo run:android --no-bundler)" >&2
  exit 1
fi

resolve_device_serial() {
  if [[ -n "${DEVICE_SERIAL}" ]]; then
    echo "${DEVICE_SERIAL}"
    return 0
  fi

  adb devices | awk 'NR>1 && $1 != "" && $2 == "device" && $1 !~ /^emulator-/{print $1; exit}'
}

SECONDS_WAITED=0
TARGET_SERIAL="$(resolve_device_serial || true)"
while [[ -z "${TARGET_SERIAL}" && "${SECONDS_WAITED}" -lt "${WAIT_SECONDS}" ]]; do
  sleep 2
  SECONDS_WAITED=$((SECONDS_WAITED + 2))
  TARGET_SERIAL="$(resolve_device_serial || true)"
done

if [[ -z "${TARGET_SERIAL}" ]]; then
  echo "No physical Android device detected via adb within ${WAIT_SECONDS}s." >&2
  echo "Tip: unlock phone, enable USB debugging, choose File Transfer mode, and accept RSA prompt." >&2
  adb devices -l || true
  exit 2
fi

echo "Using device: ${TARGET_SERIAL}"
echo "Installing APK: ${APK_PATH}"
adb -s "${TARGET_SERIAL}" install -r "${APK_PATH}"

echo "Launching ${PACKAGE_NAME}/${MAIN_ACTIVITY}"
adb -s "${TARGET_SERIAL}" shell am start -n "${PACKAGE_NAME}/${MAIN_ACTIVITY}"

echo "Deploy complete on ${TARGET_SERIAL}."
