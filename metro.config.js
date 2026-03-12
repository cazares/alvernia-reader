const fs = require('fs');
const path = require('path');
const { getDefaultConfig } = require('expo/metro-config');

const config = getDefaultConfig(__dirname);
config.resolver.useWatchman = false;
config.resolver.unstable_enableSymlinks = true;

const nodeModulesPath = path.join(__dirname, 'node_modules');
try {
  const realNodeModulesPath = fs.realpathSync(nodeModulesPath);
  config.watchFolders = Array.from(new Set([...(config.watchFolders || []), realNodeModulesPath]));
} catch {
  // Keep default config when node_modules is local or missing.
}

module.exports = config;
