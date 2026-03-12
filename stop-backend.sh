#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$PROJECT_ROOT/.backend.pid"

if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE")"
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping backend server (PID: $PID)..."
        kill "$PID" 2>/dev/null || true
        sleep 1
    else
        echo "Backend process not running"
    fi
    rm -f "$PID_FILE"
fi

PIDS="$(lsof -ti:8000 2>/dev/null || true)"
if [ -n "$PIDS" ]; then
    echo "Stopping remaining process(es) on port 8000..."
    for pid in $PIDS; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
fi

if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "Backend still listening on port 8000"
    exit 1
fi

echo "✓ Backend stopped"
