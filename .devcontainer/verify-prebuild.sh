#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

WHISPER_MODEL="${WHISPER_MODEL:-tiny}"
WHISPER_MODEL_PATH="whisper.cpp/models/ggml-${WHISPER_MODEL}.bin"

RED=$'\033[31m'
GREEN=$'\033[32m'
YELLOW=$'\033[33m'
RESET=$'\033[0m'

FAILURES=0

pass() {
  echo "${GREEN}PASS${RESET} $*"
}

warn() {
  echo "${YELLOW}WARN${RESET} $*"
}

fail() {
  echo "${RED}FAIL${RESET} $*"
  FAILURES=$((FAILURES + 1))
}

check_cmd() {
  local bin="$1"
  local label="$2"
  if command -v "${bin}" >/dev/null 2>&1; then
    pass "${label}: $(command -v "${bin}")"
  else
    fail "${label}: '${bin}' not found on PATH"
  fi
}

echo "Verifying Codespaces prebuild dependencies..."

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
  pass "python virtualenv present (.venv)"
else
  fail "python virtualenv missing (.venv). Run: bash .devcontainer/on-create.sh"
fi

check_cmd "python3" "python3"
check_cmd "node" "node"
check_cmd "npm" "npm"
check_cmd "ffmpeg" "ffmpeg"
check_cmd "ffprobe" "ffprobe"
check_cmd "cmake" "cmake"

if python - <<'PY'
import importlib.util, sys
mods = ("yt_dlp", "fastapi", "uvicorn", "demucs")
missing = [m for m in mods if importlib.util.find_spec(m) is None]
if missing:
    print("missing python modules: " + ", ".join(missing))
    sys.exit(1)
print("python modules ok")
PY
then
  pass "python package imports (yt_dlp, fastapi, uvicorn, demucs)"
else
  fail "required python packages are missing from .venv"
fi

if command -v yt-dlp >/dev/null 2>&1 || python -m yt_dlp --version >/dev/null 2>&1; then
  pass "yt-dlp command/module available"
else
  fail "yt-dlp is not usable (command missing and python -m yt_dlp failed)"
fi

if [[ -f "karaoapp/package.json" ]]; then
  if [[ -d "karaoapp/node_modules" ]]; then
    pass "karaoapp node_modules present"
  else
    warn "karaoapp node_modules missing; run: npm --prefix karaoapp ci"
  fi
fi

WHISPER_BIN=""
for cand in \
  "whisper.cpp/build/bin/whisper-cli" \
  "whisper.cpp/build/bin/main" \
  "whisper.cpp/main"
do
  if [[ -x "${cand}" ]]; then
    WHISPER_BIN="${cand}"
    break
  fi
done

if [[ -n "${WHISPER_BIN}" ]]; then
  pass "whisper.cpp binary present (${WHISPER_BIN})"
else
  fail "whisper.cpp binary missing; run: bash .devcontainer/on-create.sh"
fi

if [[ -f "${WHISPER_MODEL_PATH}" ]]; then
  pass "whisper model present (${WHISPER_MODEL_PATH})"
else
  fail "whisper model missing (${WHISPER_MODEL_PATH})"
fi

if [[ "${FAILURES}" -eq 0 ]]; then
  echo
  echo "${GREEN}All checks passed.${RESET}"
  exit 0
fi

echo
echo "${RED}${FAILURES} check(s) failed.${RESET}"
exit 1
