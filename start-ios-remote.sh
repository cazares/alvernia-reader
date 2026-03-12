#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REMOTE_URL="${1:-https://supreme-space-invention-r7p6rvqrvw3p5wx-8000.app.github.dev}"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/karaoapp" && pwd)"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Mixterious - Start iOS with Remote Backend       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Show configuration
echo -e "${YELLOW}Remote Backend URL:${NC}"
echo -e "  ${REMOTE_URL}"
echo ""

# Update .env file
echo -e "${BLUE}[1/4] Bumping app deploy version...${NC}"
APP_VERSION="$("$ROOT_DIR/scripts/bump_app_deploy_version.sh")"
echo -e "${GREEN}✓ App deploy version: v${APP_VERSION}${NC}"
echo ""

echo -e "${BLUE}[2/4] Updating .env configuration...${NC}"
cd "$APP_DIR"

cat > .env << EOF
# Remote backend configuration
EXPO_PUBLIC_API_BASE_URL=${REMOTE_URL}
EXPO_PUBLIC_API_URL=${REMOTE_URL}
EXPO_PUBLIC_APP_VERSION=${APP_VERSION}
EOF

echo -e "${GREEN}✓ .env updated${NC}"
echo ""

# Kill any existing Expo processes
echo -e "${BLUE}[3/4] Stopping any running Expo instances...${NC}"
pkill -f "expo start" 2>/dev/null || true
sleep 2
echo -e "${GREEN}✓ Cleaned up${NC}"
echo ""

# Start Expo
echo -e "${BLUE}[4/4] Starting iOS app with remote backend...${NC}"
echo -e "${YELLOW}App will connect to: ${REMOTE_URL}${NC}"
echo -e "${YELLOW}Deploy version: v${APP_VERSION}${NC}"
echo ""

npx expo start --ios --clear

echo ""
echo -e "${GREEN}✓ App started!${NC}"
