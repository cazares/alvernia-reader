#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

TORCH_CPU_INDEX_URL="${TORCH_CPU_INDEX_URL:-https://download.pytorch.org/whl/cpu}"
TORCH_CPU_VERSION="${TORCH_CPU_VERSION:-2.5.1}"
WHISPER_CPP_REPO_URL="${WHISPER_CPP_REPO_URL:-https://github.com/ggerganov/whisper.cpp.git}"
WHISPER_CPP_REF="${WHISPER_CPP_REF:-master}"
WHISPER_MODEL="${WHISPER_MODEL:-tiny}"
WHISPER_DIR="${WHISPER_DIR:-whisper.cpp}"

pip_install() {
  python -m pip install --no-cache-dir "$@"
}

build_jobs() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
    return
  fi
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu
    return
  fi
  echo 4
}

ensure_whisper_repo() {
  if [[ -d "${WHISPER_DIR}" ]]; then
    return 0
  fi
  git clone --depth 1 --branch "${WHISPER_CPP_REF}" "${WHISPER_CPP_REPO_URL}" "${WHISPER_DIR}"
}

ensure_whisper_build() {
  cmake -S "${WHISPER_DIR}" -B "${WHISPER_DIR}/build" -DGGML_NATIVE=OFF
  cmake --build "${WHISPER_DIR}/build" -j"$(build_jobs)"
}

ensure_whisper_model() {
  local model_path="${WHISPER_DIR}/models/ggml-${WHISPER_MODEL}.bin"
  if [[ -f "${model_path}" ]]; then
    return 0
  fi
  if [[ -x "${WHISPER_DIR}/models/download-ggml-model.sh" ]]; then
    bash "${WHISPER_DIR}/models/download-ggml-model.sh" "${WHISPER_MODEL}"
    return 0
  fi
  echo "warning: whisper model download helper not found at ${WHISPER_DIR}/models/download-ggml-model.sh" >&2
}

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

pip_install --upgrade pip
pip_install -r requirements.txt "yt-dlp[default]"

# Keep demucs on CPU-only torch to avoid CUDA wheels exhausting disk.
pip_install \
  --index-url "${TORCH_CPU_INDEX_URL}" \
  "torch==${TORCH_CPU_VERSION}+cpu" \
  "torchaudio==${TORCH_CPU_VERSION}+cpu"

pip_install demucs

if [[ -f "karaoapp/package.json" ]]; then
  npm --prefix karaoapp ci
fi

ensure_whisper_repo
ensure_whisper_build
ensure_whisper_model
