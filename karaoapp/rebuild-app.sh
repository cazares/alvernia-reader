#!/bin/bash

# Rebuild iOS app with fresh environment variables

echo "🔄 Rebuilding Mixterious app with new configuration..."
echo ""

cd "$(dirname "$0")"

# Step 1: Bump deploy version
echo "🔢 Bumping app deploy version..."
if [ -x "../scripts/bump_app_deploy_version.sh" ]; then
    APP_VERSION="$(../scripts/bump_app_deploy_version.sh)"
    echo "✓ Deploy version: v${APP_VERSION}"
else
    echo "⚠ Version bump script missing: ../scripts/bump_app_deploy_version.sh"
fi
echo ""

# Step 2: Verify .env
echo "📋 Checking .env file..."
if [ -f ".env" ]; then
    echo "✓ .env found:"
    cat .env
else
    echo "⚠ Creating .env file..."
    echo "EXPO_PUBLIC_API_BASE_URL=http://192.168.1.197:8000" > .env
    echo "✓ .env created"
fi
echo ""

# Step 3: Clear caches
echo "🧹 Clearing caches..."
rm -rf .expo 2>/dev/null
rm -rf node_modules/.cache 2>/dev/null
rm -rf ios/build 2>/dev/null
echo "✓ Caches cleared"
echo ""

# Step 4: Kill any running Metro bundler
echo "🛑 Stopping Metro bundler..."
pkill -f "node.*metro" 2>/dev/null
pkill -f "expo start" 2>/dev/null
pkill -f "react-native start" 2>/dev/null
sleep 2
echo "✓ Metro stopped"
echo ""

# Step 5: Rebuild
echo "🔨 Rebuilding iOS app (this takes 1-2 minutes)..."
echo ""
npm run ios

echo ""
echo "✅ Rebuild complete!"
echo ""
echo "The app should now use: http://192.168.1.197:8000"
echo "Try searching for 'john frusciante - god' and press Create Karaoke!"
