#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_DIR="${ROOT_DIR}/karaoapp"
OUT_DIR="${ROOT_DIR}/release/playstore/assets-61"
PHONE_DIR="${OUT_DIR}/phone-screenshots"
TAB7_DIR="${OUT_DIR}/tablet-7inch-screenshots"
TAB10_DIR="${OUT_DIR}/tablet-10inch-screenshots"

ICON_SRC="${APP_DIR}/assets/icon.png"
SPLASH_SRC="${APP_DIR}/assets/splash.png"

mkdir -p "${OUT_DIR}" "${PHONE_DIR}" "${TAB7_DIR}" "${TAB10_DIR}"

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg is required but not installed." >&2
  exit 1
fi

if [[ ! -f "${ICON_SRC}" ]]; then
  echo "Missing icon source: ${ICON_SRC}" >&2
  exit 1
fi

if [[ ! -f "${SPLASH_SRC}" ]]; then
  echo "Missing splash source: ${SPLASH_SRC}" >&2
  exit 1
fi

build_icon() {
  ffmpeg -y -loglevel error \
    -i "${ICON_SRC}" \
    -vf "scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black" \
    -frames:v 1 \
    "${OUT_DIR}/mixterious-play-icon-512.png"
}

build_feature_graphic() {
  ffmpeg -y -loglevel error \
    -i "${SPLASH_SRC}" \
    -i "${ICON_SRC}" \
    -filter_complex "[0:v]scale=1024:500:force_original_aspect_ratio=increase,crop=1024:500,boxblur=18:2[bg];[1:v]scale=300:300[logo];[bg]drawbox=x=0:y=0:w=iw:h=ih:color=black@0.24:t=fill[bg2];[bg2][logo]overlay=(W-w)/2:(H-h)/2" \
    -frames:v 1 \
    "${OUT_DIR}/mixterious-feature-graphic-1024x500.png"
}

build_screenshot_variant() {
  local width="$1"
  local height="$2"
  local variant="$3"
  local output_file="$4"

  local logo_lg=$(( width * 55 / 100 ))
  local logo_md=$(( width * 40 / 100 ))
  local logo_sm=$(( width * 24 / 100 ))
  local top_y=$(( height * 14 / 100 ))
  local panel_y=$(( height * 62 / 100 ))
  local panel_h=$(( height - panel_y ))
  local card_x=$(( width * 10 / 100 ))
  local card_y=$(( height * 10 / 100 ))
  local card_w=$(( width * 80 / 100 ))
  local card_h=$(( height * 80 / 100 ))

  local filter=""

  case "${variant}" in
    1)
      filter="[0:v]scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},boxblur=16:2[bg];[1:v]scale=${logo_lg}:-1[logo];[bg]drawbox=x=0:y=0:w=iw:h=ih:color=black@0.22:t=fill[bg2];[bg2][logo]overlay=(W-w)/2:(H-h)/2"
      ;;
    2)
      filter="[0:v]scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},boxblur=16:2[bg];[1:v]scale=${logo_md}:-1[logo];[bg]drawbox=x=0:y=0:w=iw:h=ih:color=black@0.18:t=fill[bg2];[bg2]drawbox=x=0:y=${panel_y}:w=iw:h=${panel_h}:color=black@0.50:t=fill[layered];[layered][logo]overlay=(W-w)/2:${top_y}"
      ;;
    3)
      filter="[0:v]scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},boxblur=16:2[bg];[1:v]scale=${logo_md}:-1[l1];[1:v]scale=${logo_sm}:-1[l2];[bg]drawbox=x=0:y=0:w=iw:h=ih:color=black@0.25:t=fill[bg2];[bg2][l1]overlay=(W-w)/2:${top_y}[tmp];[tmp][l2]overlay=(W-w)/2:${panel_y}"
      ;;
    4)
      filter="[0:v]scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},boxblur=16:2[bg];[1:v]scale=${logo_md}:-1[logo];[bg]drawbox=x=0:y=0:w=iw:h=ih:color=black@0.20:t=fill[bg2];[bg2]drawbox=x=${card_x}:y=${card_y}:w=${card_w}:h=${card_h}:color=black@0.45:t=fill[card];[card]drawbox=x=${card_x}:y=${card_y}:w=${card_w}:h=${card_h}:color=white@0.25:t=4[card2];[card2][logo]overlay=(W-w)/2:(H-h)/2"
      ;;
    *)
      echo "Unknown screenshot variant: ${variant}" >&2
      exit 1
      ;;
  esac

  ffmpeg -y -loglevel error \
    -i "${SPLASH_SRC}" \
    -i "${ICON_SRC}" \
    -filter_complex "${filter}" \
    -frames:v 1 \
    "${output_file}"
}

build_icon
build_feature_graphic

# Phone screenshots: 9:16, 1080x1920
build_screenshot_variant 1080 1920 1 "${PHONE_DIR}/mixterious-phone-01-1080x1920.png"
build_screenshot_variant 1080 1920 2 "${PHONE_DIR}/mixterious-phone-02-1080x1920.png"
build_screenshot_variant 1080 1920 3 "${PHONE_DIR}/mixterious-phone-03-1080x1920.png"
build_screenshot_variant 1080 1920 4 "${PHONE_DIR}/mixterious-phone-04-1080x1920.png"

# 7-inch tablet screenshots: 9:16, 1260x2240
build_screenshot_variant 1260 2240 1 "${TAB7_DIR}/mixterious-tablet7-01-1260x2240.png"
build_screenshot_variant 1260 2240 2 "${TAB7_DIR}/mixterious-tablet7-02-1260x2240.png"

# 10-inch tablet screenshots: 9:16, 1440x2560
build_screenshot_variant 1440 2560 3 "${TAB10_DIR}/mixterious-tablet10-01-1440x2560.png"
build_screenshot_variant 1440 2560 4 "${TAB10_DIR}/mixterious-tablet10-02-1440x2560.png"

echo "Generated Play Store asset pack at: ${OUT_DIR}"
