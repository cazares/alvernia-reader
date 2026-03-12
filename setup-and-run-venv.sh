#!/bin/bash

# Setup and Run Script (with venv)
# This script sets up and runs the Mixterious app locally.

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root (portable: resolves to this repo regardless of where it lives on disk)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$PROJECT_ROOT/karaoapi"
FRONTEND_DIR="$PROJECT_ROOT/karaoapp"
VENV_DIR="$PROJECT_ROOT/venv"

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║                Mixterious - Local Setup                  ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ============================================================================
# Step 1: Check Prerequisites
# ============================================================================

echo -e "${YELLOW}[1/8] Checking prerequisites...${NC}"

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ Python found: $PYTHON_VERSION${NC}"
else
    echo -e "${RED}✗ Python 3 not found. Please install Python 3.11+${NC}"
    exit 1
fi

# Check Node/npm
if command -v npm &> /dev/null; then
    NODE_VERSION=$(node --version)
    NPM_VERSION=$(npm --version)
    echo -e "${GREEN}✓ Node.js found: $NODE_VERSION${NC}"
    echo -e "${GREEN}✓ npm found: $NPM_VERSION${NC}"
else
    echo -e "${RED}✗ npm not found. Please install Node.js and npm${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 2: Create Python Virtual Environment
# ============================================================================

echo -e "${YELLOW}[2/8] Setting up Python virtual environment...${NC}"

cd "$PROJECT_ROOT"

if [ -d "$VENV_DIR" ]; then
    echo -e "${BLUE}Virtual environment already exists${NC}"
else
    echo -e "${BLUE}Creating virtual environment...${NC}"
	    python3 -m venv "$VENV_DIR"
	    echo -e "${GREEN}✓ Virtual environment created${NC}"
	fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"
echo -e "${GREEN}✓ Virtual environment activated${NC}"
echo ""

# ============================================================================
# Step 3: Install Backend Dependencies
# ============================================================================

echo -e "${YELLOW}[3/8] Installing backend dependencies...${NC}"

# Upgrade pip first (avoid mismatched pip shims)
python -m pip install --quiet --upgrade pip

# Install core dependencies
echo -e "${BLUE}Installing core Python packages...${NC}"
python -m pip install --quiet fastapi uvicorn python-multipart aiofiles pydantic || {
    echo -e "${RED}✗ Failed to install core dependencies${NC}"
    exit 1
}

# Install yt-dlp
echo -e "${BLUE}Installing yt-dlp...${NC}"
python -m pip install --quiet -U "yt-dlp[default]" || {
    echo -e "${RED}✗ Failed to install yt-dlp${NC}"
    exit 1
}

# Try to install demucs (optional, skip if it fails)
echo -e "${BLUE}Installing audio processing libraries (optional)...${NC}"
python -m pip install --quiet demucs 2>/dev/null && echo -e "${GREEN}✓ demucs installed${NC}" || echo -e "${YELLOW}⚠ demucs skipped (not critical for downloads)${NC}"

echo -e "${GREEN}✓ Backend dependencies installed${NC}"
echo ""

# ============================================================================
# Step 4: Install Frontend Dependencies
# ============================================================================

echo -e "${YELLOW}[4/8] Installing frontend dependencies...${NC}"

cd "$FRONTEND_DIR"

if [ ! -f "package.json" ]; then
    echo -e "${RED}✗ package.json not found in $FRONTEND_DIR${NC}"
    exit 1
fi

echo -e "${BLUE}Running npm install (this may take a minute)...${NC}"
npm install || {
    echo -e "${RED}✗ npm install failed${NC}"
    exit 1
}

echo -e "${GREEN}✓ Frontend dependencies installed${NC}"
echo ""

# ============================================================================
# Step 5: Configure Environment
# ============================================================================

echo -e "${YELLOW}[5/8] Configuring environment...${NC}"

cd "$FRONTEND_DIR"

# Create .env file for local development
cat > .env << 'EOF'
# Local development configuration
EXPO_PUBLIC_API_URL=http://localhost:8000
EOF

echo -e "${GREEN}✓ Created .env file: EXPO_PUBLIC_API_URL=http://localhost:8000${NC}"
echo ""

# ============================================================================
# Step 6: Start Backend Server
# ============================================================================

echo -e "${YELLOW}[6/8] Starting backend server...${NC}"

cd "$BACKEND_DIR"

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠ Port 8000 is already in use. Killing existing process...${NC}"
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    sleep 2
fi

# Start backend in background (with venv python)
echo -e "${BLUE}Starting uvicorn server...${NC}"
nohup "$VENV_DIR/bin/python" -m uvicorn app:app --reload --host 0.0.0.0 --port 8000 > "$PROJECT_ROOT/backend.log" 2>&1 &
BACKEND_PID=$!

# Save PID for later
echo $BACKEND_PID > "$PROJECT_ROOT/.backend.pid"

# Wait for backend to start
echo -e "${BLUE}Waiting for backend to start...${NC}"
sleep 5

# Check if backend is running
if kill -0 $BACKEND_PID 2>/dev/null; then
    echo -e "${GREEN}✓ Backend server started (PID: $BACKEND_PID)${NC}"
    echo -e "${GREEN}  Logs: $PROJECT_ROOT/backend.log${NC}"
    echo -e "${GREEN}  venv: $VENV_DIR${NC}"
else
    echo -e "${RED}✗ Backend server failed to start${NC}"
    echo -e "${RED}  Check logs: cat $PROJECT_ROOT/backend.log${NC}"
    exit 1
fi

echo ""

# ============================================================================
# Step 7: Test Backend Endpoints
# ============================================================================

echo -e "${YELLOW}[7/8] Testing backend endpoints...${NC}"

# Test health endpoint
echo -e "${BLUE}Testing /health endpoint...${NC}"
if curl -s -f http://localhost:8000/health > /dev/null 2>&1; then
    HEALTH=$(curl -s http://localhost:8000/health)
    echo -e "${GREEN}✓ Health check passed: $HEALTH${NC}"
else
    echo -e "${RED}✗ Health check failed${NC}"
    echo -e "${RED}  Backend might not be ready. Check: cat $PROJECT_ROOT/backend.log${NC}"
    exit 1
fi

# Test config endpoint
echo -e "${BLUE}Testing /config/download-strategy endpoint...${NC}"
if curl -s -f http://localhost:8000/config/download-strategy > /dev/null 2>&1; then
    CONFIG=$(curl -s http://localhost:8000/config/download-strategy)
    echo -e "${GREEN}✓ Config endpoint working${NC}"
    echo -e "${BLUE}  Strategy: $(echo $CONFIG | grep -o '"strategy":"[^"]*"')${NC}"
else
    echo -e "${RED}✗ Config endpoint failed${NC}"
fi

echo ""

# ============================================================================
# Step 8: Instructions for Running iOS App
# ============================================================================

echo -e "${YELLOW}[8/8] Setup complete! 🎉${NC}"
echo ""

echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Backend is running! Now start the iOS app:              ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${BLUE}To run on iOS Simulator:${NC}"
echo -e "  cd $FRONTEND_DIR"
echo -e "  npm run ios"
echo ""

echo -e "${BLUE}To test a download:${NC}"
echo -e "  1. Search: 'Rick Astley Never Gonna Give You Up'"
echo -e "  2. Press download"
echo -e "  3. Watch for: 🔍 Finding... → 📥 Downloading... → ✓ Complete!"
echo ""

echo -e "${BLUE}Monitor logs:${NC}"
echo -e "  Backend:  tail -f $PROJECT_ROOT/backend.log"
echo ""

echo -e "${BLUE}Stop backend:${NC}"
echo -e "  kill $BACKEND_PID"
echo -e "  # Or: ./stop-backend.sh"
echo ""

echo -e "${BLUE}Restart backend:${NC}"
echo -e "  source venv/bin/activate"
echo -e "  cd karaoapi"
echo -e "  python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000"
echo ""

echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Backend PID: $BACKEND_PID | Port: 8000 | venv: active${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
