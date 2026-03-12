#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export PYTHONPATH="${ROOT}"
cd "${ROOT}"

activate_project_venv() {
  if [[ -f "${ROOT}/.venv/bin/activate" ]]; then
    # Prefer project venv if present
    # shellcheck source=/dev/null
    source "${ROOT}/.venv/bin/activate"
  fi
}

ensure_project_venv_active() {
  if [[ ! -x "${ROOT}/.venv/bin/python3" ]]; then
    echo "Creating project virtual environment at ${ROOT}/.venv..."
    python3 -m venv "${ROOT}/.venv"
  fi

  # shellcheck source=/dev/null
  source "${ROOT}/.venv/bin/activate"
}

activate_project_venv

has_python_module() {
  local module_name="${1}"
  python3 -c 'import importlib.util,sys; sys.exit(0 if importlib.util.find_spec(sys.argv[1]) else 1)' "${module_name}" >/dev/null 2>&1
}

has_required_python_modules() {
  has_python_module "yt_dlp" && has_python_module "fastapi" && has_python_module "uvicorn" && has_python_module "demucs"
}

has_demucs_cli() {
  command -v demucs >/dev/null 2>&1
}

ensure_codespace_system_deps() {
  if [[ -z "${CODESPACE_NAME:-}" ]]; then
    return 0
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    return 0
  fi

  local missing=0
  command -v ffmpeg >/dev/null 2>&1 || missing=1
  command -v node >/dev/null 2>&1 || missing=1
  command -v python3 >/dev/null 2>&1 || missing=1
  python3 -m venv --help >/dev/null 2>&1 || missing=1
  if [[ "${missing}" -eq 0 ]]; then
    return 0
  fi

  if [[ -f /etc/apt/sources.list.d/yarn.list ]]; then
    sudo mv /etc/apt/sources.list.d/yarn.list /etc/apt/sources.list.d/yarn.list.disabled || true
  fi

  echo "Installing Codespace system deps (ffmpeg, nodejs, python3-venv)..."
  sudo apt-get update
  sudo apt-get install -y ffmpeg nodejs python3-venv
}

ensure_python_deps_ready() {
  if command -v yt-dlp >/dev/null 2>&1 && has_python_module "fastapi" && has_python_module "uvicorn" && has_demucs_cli; then
    return 0
  fi

  if has_required_python_modules && has_demucs_cli; then
    echo "yt-dlp CLI not found; pipeline will use python module fallback."
    return 0
  fi

  if [[ -n "${CODESPACE_NAME:-}" ]]; then
    echo "Python dependencies missing in this Codespace; installing into ${ROOT}/.venv..."
    ensure_codespace_system_deps
    ensure_project_venv_active
    python3 -m pip install -r "${ROOT}/requirements.txt"
    python3 -m pip install -U "yt-dlp[default]" "demucs>=4.0.1"
    if has_demucs_cli && ( command -v yt-dlp >/dev/null 2>&1 || has_required_python_modules ); then
      return 0
    fi
  fi

  echo "Python dependencies are missing (yt-dlp/fastapi/uvicorn/demucs)." >&2
  echo "Run:" >&2
  echo "  python3 -m venv ${ROOT}/.venv" >&2
  echo "  source ${ROOT}/.venv/bin/activate" >&2
  echo "  python -m pip install -r ${ROOT}/requirements.txt" >&2
  exit 1
}

ensure_python_deps_ready

get_ip() {
  ipconfig getifaddr en0 2>/dev/null || true
}

IP="$(get_ip)"
if [[ -n "${IP}" ]]; then
  echo "Starting KaraoAPI on 0.0.0.0:8000 (LAN http://${IP}:8000)"
else
  echo "Starting KaraoAPI on 0.0.0.0:8000 (LAN IP unavailable)"
fi
python3 -m uvicorn karaoapi.app:app --host 0.0.0.0 --port 8000 --proxy-headers
