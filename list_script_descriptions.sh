#!/usr/bin/env bash
set -euo pipefail

# Prints a Markdown table of repo scripts and what they do.
# It extracts the first non-empty top-of-file comment line (after shebang) as the description.
#
# Usage:
#   ./list_script_descriptions.sh
#   ./list_script_descriptions.sh --all        # include more directories (still prunes heavy/vendor trees)
#   ./list_script_descriptions.sh --help

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

show_help() {
  cat <<'EOF'
Usage: ./list_script_descriptions.sh [--all] [--help]

Outputs a Markdown table of *.sh scripts with a short description extracted from the file header.

Flags:
  --all   Broaden the search to the whole repo (still prunes vendor/large trees).
  --help  Show this help.
EOF
}

MODE="focused"
case "${1:-}" in
  --help|-h) show_help; exit 0 ;;
  --all) MODE="all" ;;
  "") ;;
  *) echo "Unknown argument: $1" >&2; echo ""; show_help; exit 2 ;;
esac

cd "$ROOT_DIR"

# Keep the default list tight: scripts we actually own/use for dev/deploy.
focused_roots=(
  "."
  "./scripts"
  "./karaoapi/scripts"
  "./karaoapp/scripts"
  "./.devcontainer"
)

prune_args=(
  -path "./.git" -o
  -path "./karaoapp/node_modules" -o
  -path "./karaoapp/ios" -o
  -path "./karaoapp/android" -o
  -path "./venv" -o
  -path "./align_env" -o
  -path "./demucs_env" -o
  -path "./lyrics-align-env" -o
  -path "./mfa_env" -o
  -path "./assets" -o
  -path "./gentle-full" -o
  -path "./gentle_broken_1" -o
  -path "./whisper.cpp" -o
  -path "./whisper_env" -o
  -path "./.cache" -o
  -path "./.next" -o
  -path "./dist" -o
  -path "./build" -o
  -path "./scripts/demucs_env"
)

find_scripts() {
  if [[ "$MODE" == "focused" ]]; then
    # Search only known script roots (avoid "." which can double-count).
    find "${focused_roots[@]}" \
      \( "${prune_args[@]}" \) -prune -o \
      -type f -name "*.sh" -print
  else
    # Search the whole repo but still prune big/vendor trees.
    find . \
      \( "${prune_args[@]}" \) -prune -o \
      -type f -name "*.sh" -print
  fi
}

extract_desc() {
  local file="$1"
  # Extract first meaningful header comment line, after shebang.
  # Also supports "Description:" / "Purpose:" prefixes.
  awk '
    NR==1 && $0 ~ /^#!/ { next }
    {
      # Stop when we hit real code (first non-comment, non-empty line).
      if ($0 !~ /^[[:space:]]*#/ && $0 !~ /^[[:space:]]*$/) { exit }
      if ($0 ~ /^[[:space:]]*#/) {
        line = $0
        sub(/^[[:space:]]*#[[:space:]]*/, "", line)
        gsub(/[[:space:]]+$/, "", line)
        if (line == "") next
        if (tolower(line) ~ /^(description|purpose)[[:space:]]*:/) {
          sub(/^[^:]*:[[:space:]]*/, "", line)
        }
        print line
        exit
      }
    }
  ' "$file"
}

scripts="$(find_scripts | sed 's|^\./||' | sort | uniq)"

echo "| Script | Description |"
echo "| --- | --- |"

while IFS= read -r rel; do
  [[ -z "$rel" ]] && continue
  desc="$(extract_desc "./$rel" || true)"
  if [[ -z "$desc" ]]; then
    desc="(no header description found)"
  fi
  # Escape Markdown pipe chars.
  desc="${desc//|/\\|}"
  echo "| \`$rel\` | $desc |"
done <<< "$scripts"
