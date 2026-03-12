#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/karaoapp" && pwd)"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║      Mixterious - Start Metro (Local Backend Mode)       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Update .env for localhost
echo -e "${BLUE}[1/4] Bumping app deploy version...${NC}"
APP_VERSION="$("$ROOT_DIR/scripts/bump_app_deploy_version.sh")"
echo -e "${GREEN}✓ App deploy version: v${APP_VERSION}${NC}"
echo ""

echo -e "${BLUE}[2/4] Configuring for localhost backend...${NC}"
cd "$APP_DIR"

cat > .env << 'EOF'
# Local development configuration
EXPO_PUBLIC_API_BASE_URL=http://localhost:8000
EXPO_PUBLIC_API_URL=http://localhost:8000
EOF

echo "EXPO_PUBLIC_APP_VERSION=${APP_VERSION}" >> .env

echo -e "${GREEN}✓ .env configured for localhost${NC}"
echo ""

# Kill existing Expo
echo -e "${BLUE}[3/4] Stopping any running Expo instances...${NC}"
pkill -f "expo start" 2>/dev/null || true
sleep 2
echo -e "${GREEN}✓ Cleaned up${NC}"
echo ""

# Start Metro
echo -e "${BLUE}[4/4] Starting Metro bundler + iOS app...${NC}"
echo -e "${YELLOW}App will connect to: http://localhost:8000${NC}"
echo -e "${YELLOW}Deploy version: v${APP_VERSION}${NC}"
echo -e "${YELLOW}Make sure backend is running: ./start-backend-local.sh${NC}"
echo ""

npx expo start --ios --clear

echo ""
echo -e "${GREEN}✓ Metro started!${NC}"
