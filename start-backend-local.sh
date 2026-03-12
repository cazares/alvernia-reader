#!/bin/bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_BACKEND_ROOT_CANDIDATE="/Users/cazares/Documents/karaoke-time-by-miguel"
BACKEND_ROOT="${MIXTERIOSO_BACKEND_ROOT:-$PROJECT_ROOT}"
VENV_DIR="$PROJECT_ROOT/.venv"
PYTHON_BIN_DEFAULT="$VENV_DIR/bin/python3"
PYTHON_BIN_FALLBACK="$VENV_DIR/bin/python"
CLIENT_SECRET_DEFAULT="$PROJECT_ROOT/client_secret.json"
LAN_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || true)"

if [ ! -f "$BACKEND_ROOT/karaoapi/app.py" ] && [ -f "$DEFAULT_BACKEND_ROOT_CANDIDATE/karaoapi/app.py" ]; then
    BACKEND_ROOT="$DEFAULT_BACKEND_ROOT_CANDIDATE"
fi

if [ "$BACKEND_ROOT" != "$PROJECT_ROOT" ]; then
    echo -e "${YELLOW}⚠ Using backend from $BACKEND_ROOT${NC}"
    exec "$BACKEND_ROOT/start-backend-local.sh"
fi

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           Mixterious - Start Local Backend               ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}✗ Virtual environment not found${NC}"
    echo -e "${YELLOW}Run ./setup-and-run-venv.sh first${NC}"
    exit 1
fi

PYTHON_BIN="$PYTHON_BIN_DEFAULT"
if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="$PYTHON_BIN_FALLBACK"
fi
if [ ! -x "$PYTHON_BIN" ]; then
    echo -e "${RED}✗ Python executable not found in $VENV_DIR/bin${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Using Python: $PYTHON_BIN${NC}"
echo ""

# Optional defaults so app-side YouTube embed has a URL from local jobs.
export MIXTERIOSO_ENABLE_STEP5_UPLOAD="${MIXTERIOSO_ENABLE_STEP5_UPLOAD:-1}"
export MIXTERIOSO_YOUTUBE_ALLOW_BROWSER_OAUTH="${MIXTERIOSO_YOUTUBE_ALLOW_BROWSER_OAUTH:-0}"
if [ -z "${YOUTUBE_CLIENT_SECRETS_JSON:-}" ] && [ -f "$CLIENT_SECRET_DEFAULT" ]; then
    export YOUTUBE_CLIENT_SECRETS_JSON="$CLIENT_SECRET_DEFAULT"
fi
export PYTHONPATH="$PROJECT_ROOT"

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠ Port 8000 is already in use. Stopping existing process...${NC}"
    for pid in $(lsof -ti:8000); do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
fi

# Start backend
cd "$PROJECT_ROOT"
echo -e "${BLUE}Starting backend on http://localhost:8000...${NC}"
if [ -n "$LAN_IP" ]; then
    echo -e "${BLUE}LAN URL: http://$LAN_IP:8000${NC}"
fi
echo -e "${YELLOW}Logs: $PROJECT_ROOT/backend.log${NC}"
echo ""

nohup "$PYTHON_BIN" -m uvicorn karaoapi.app:app --host 0.0.0.0 --port 8000 --proxy-headers > "$PROJECT_ROOT/backend.log" 2>&1 < /dev/null &
BACKEND_PID=$!

# Save PID
echo $BACKEND_PID > "$PROJECT_ROOT/.backend.pid"

# Wait and verify
sleep 3

if kill -0 $BACKEND_PID 2>/dev/null; then
    echo -e "${GREEN}✓ Backend started (PID: $BACKEND_PID)${NC}"
    echo -e "${GREEN}  URL: http://localhost:8000${NC}"
    if [ -n "$LAN_IP" ]; then
        echo -e "${GREEN}  LAN: http://$LAN_IP:8000${NC}"
    fi
    echo ""

    # Test health endpoint
    if curl -s http://localhost:8000/health > /dev/null; then
        echo -e "${GREEN}✓ Health check passed${NC}"
    else
        echo -e "${YELLOW}⚠ Health check failed - check logs${NC}"
    fi
else
    echo -e "${RED}✗ Backend failed to start${NC}"
    echo -e "${RED}  Check: cat $PROJECT_ROOT/backend.log${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}Backend is running. To stop:${NC}"
echo -e "  ./stop-backend.sh"
echo ""
