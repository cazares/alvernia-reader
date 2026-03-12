#!/bin/bash
set -e

# Resolve where client secrets should live.
resolve_client_secrets_path() {
    local raw_path="${YOUTUBE_CLIENT_SECRETS_JSON:-}"

    if [ -z "$raw_path" ]; then
        echo "/tmp/client_secret.json"
        return
    fi

    if [ -d "$raw_path" ] || [[ "$raw_path" == */ ]]; then
        echo "${raw_path%/}/client_secret.json"
        return
    fi

    if [[ "$(basename "$raw_path")" == *.json ]]; then
        echo "$raw_path"
    else
        echo "${raw_path%/}/client_secret.json"
    fi
}

# Write JSON from an env var into a file and validate that it parses.
materialize_json_env_to_file() {
    local env_name="$1"
    local out_path="$2"
    local label="$3"
    local raw_json="${!env_name:-}"

    if [ -z "$raw_json" ]; then
        return 0
    fi

    mkdir -p "$(dirname "$out_path")"
    printf "%s" "$raw_json" > "$out_path"
    chmod 600 "$out_path" || true

    if ! python3 - "$out_path" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
if not isinstance(data, dict):
    raise SystemExit(1)
PY
    then
        echo "ERROR: ${label} JSON in ${env_name} is invalid." >&2
        exit 1
    fi

    echo "Materialized ${label} to ${out_path}"
}

materialize_youtube_auth_files() {
    local client_secrets_path
    client_secrets_path="$(resolve_client_secrets_path)"
    export YOUTUBE_CLIENT_SECRETS_JSON="$client_secrets_path"

    materialize_json_env_to_file "YOUTUBE_CLIENT_SECRETS_JSON_RAW" "$client_secrets_path" "YouTube client secrets"

    local token_path
    token_path="$(dirname "$client_secrets_path")/youtube_token.json"
    materialize_json_env_to_file "YOUTUBE_TOKEN_JSON_RAW" "$token_path" "YouTube OAuth token"

    if [ -f "$token_path" ]; then
        local has_refresh
        has_refresh="$(python3 - "$token_path" <<'PY'
import json
import sys

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as handle:
        token = json.load(handle)
    print("1" if token.get("refresh_token") else "0")
except Exception:
    print("0")
PY
)"
        if [ "$has_refresh" != "1" ]; then
            echo "WARNING: youtube_token.json has no refresh_token; Step5 may require browser login."
        fi
    fi
}

materialize_youtube_auth_files

# Check if we should use Tor proxy or external proxy
USE_TOR=${USE_TOR_PROXY:-true}

if [ "$USE_TOR" = "true" ] && [ -z "$MIXTERIOSO_YTDLP_PROXY" ]; then
    echo "Starting Tor proxy..."
    # Start Tor in the background
    tor &
    TOR_PID=$!

    # Wait for Tor to be ready (simple sleep approach)
    echo "Waiting for Tor to be ready..."
    sleep 10
    echo "Tor should be ready on port 9050"

    # Export proxy environment variable for yt-dlp
    export MIXTERIOSO_YTDLP_PROXY="socks5://localhost:9050"
    echo "Using Tor proxy: $MIXTERIOSO_YTDLP_PROXY"
    echo "⚠️  WARNING: YouTube blocks Tor exit nodes. Consider using a paid residential proxy."
elif [ -n "$MIXTERIOSO_YTDLP_PROXY" ]; then
    echo "Using external proxy: $MIXTERIOSO_YTDLP_PROXY"
else
    echo "No proxy configured - using direct connection (may be blocked by YouTube)"
fi

# Start the application
echo "Starting uvicorn..."
ACCESS_LOG_ENABLED=${KARAOAPI_UVICORN_ACCESS_LOG:-0}
if [ "$ACCESS_LOG_ENABLED" = "1" ] || [ "$ACCESS_LOG_ENABLED" = "true" ] || [ "$ACCESS_LOG_ENABLED" = "yes" ]; then
    exec uvicorn karaoapi.app:app --host 0.0.0.0 --port ${PORT:-8080}
else
    exec uvicorn karaoapi.app:app --host 0.0.0.0 --port ${PORT:-8080} --no-access-log
fi
