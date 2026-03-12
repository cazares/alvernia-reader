import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const thisDir = path.dirname(fileURLToPath(import.meta.url));
const easConfigPath = path.resolve(thisDir, "../eas.json");
const appConfigPath = path.resolve(thisDir, "../app.json");
const packageJsonPath = path.resolve(thisDir, "../package.json");
const versionPath = path.resolve(thisDir, "../version.json");
const appRoot = path.resolve(thisDir, "..");

let resolvedExpoConfig = null;

const readEasConfig = () => {
  const raw = fs.readFileSync(easConfigPath, "utf8");
  return JSON.parse(raw);
};

const readAppConfig = () => {
  const raw = fs.readFileSync(appConfigPath, "utf8");
  return JSON.parse(raw);
};

const readPackageJson = () => {
  const raw = fs.readFileSync(packageJsonPath, "utf8");
  return JSON.parse(raw);
};

const readVersion = () => {
  const raw = fs.readFileSync(versionPath, "utf8");
  return JSON.parse(raw);
};

const readResolvedExpoConfig = () => {
  if (resolvedExpoConfig) return resolvedExpoConfig;
  const raw = execSync("npx expo config --json", {
    cwd: appRoot,
    env: {
      ...process.env,
      EXPO_NO_DOCTOR: "1",
    },
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  resolvedExpoConfig = JSON.parse(raw);
  return resolvedExpoConfig;
};

test("EAS CLI uses local app version source", () => {
  const easConfig = readEasConfig();
  assert.equal(easConfig?.cli?.appVersionSource, "local");
});

test("EAS production submit is configured with iOS ASC app id", () => {
  const easConfig = readEasConfig();
  assert.equal(easConfig?.submit?.production?.ios?.ascAppId, "6758872751");
});

test("EAS production build keeps store distribution and production channel", () => {
  const easConfig = readEasConfig();
  assert.equal(easConfig?.build?.production?.distribution, "store");
  assert.equal(easConfig?.build?.production?.channel, "production");
  assert.equal(
    easConfig?.build?.production?.env?.EXPO_PUBLIC_API_BASE_URL,
    "https://api.miguelendpoint.com"
  );
});

test("Static app config keeps Android package and EAS project id", () => {
  const appConfig = readAppConfig();
  assert.equal(appConfig?.expo?.android?.package, "com.cazares.alverniareader");
  assert.equal(appConfig?.expo?.newArchEnabled, false);
  assert.equal(appConfig?.expo?.extra?.eas?.projectId, "8f4aeff3-940f-4ec2-b82d-89b430f5c8be");
  assert.equal(appConfig?.expo?.notification?.icon, "./icons/notification-icon.png");
  assert.equal(appConfig?.expo?.notification?.color, "#4a90e2");
});

test("Release assets required by app config exist in-repo", () => {
  const requiredPaths = [
    "assets/icon.png",
    "assets/adaptive-icon.png",
    "assets/splash.png",
    "assets/favicon.png",
    "icons/notification-icon.png",
  ];

  for (const relativePath of requiredPaths) {
    const fullPath = path.resolve(appRoot, relativePath);
    assert.equal(
      fs.existsSync(fullPath),
      true,
      `Missing required release asset: ${relativePath}`
    );
  }
});

test("Resolved Expo config preserves release identity and versioning", () => {
  const version = readVersion();
  const expoConfig = readResolvedExpoConfig();

  assert.equal(expoConfig?.version, version.baseVersion);
  assert.equal(expoConfig?.android?.package, "com.cazares.alverniareader");
  assert.equal(expoConfig?.android?.versionCode, Number(version.buildNumber));
  assert.equal(expoConfig?.ios?.bundleIdentifier, "com.cazares.alverniareader");
  assert.equal(expoConfig?.ios?.buildNumber, String(version.buildNumber));
  assert.deepEqual(expoConfig?.platforms, ["ios", "android", "web"]);
  assert.equal(expoConfig?.newArchEnabled, false);
  assert.equal(typeof expoConfig?.runtimeVersion, "string");
  assert.equal(expoConfig?.runtimeVersion, version.baseVersion);
  assert.equal(expoConfig?.extra?.eas?.projectId, "8f4aeff3-940f-4ec2-b82d-89b430f5c8be");
});

test("Resolved Expo config enforces plugin and OTA guardrails", () => {
  const expoConfig = readResolvedExpoConfig();
  const packageJson = readPackageJson();
  const plugins = Array.isArray(expoConfig?.plugins) ? expoConfig.plugins : [];
  const pluginNames = new Set(
    plugins.map((plugin) => (Array.isArray(plugin) ? plugin[0] : plugin))
  );

  assert.equal(pluginNames.has("expo-notifications"), true);
  assert.equal(pluginNames.has("expo-secure-store"), true);
  assert.equal(pluginNames.has("expo-video"), true);
  assert.equal(typeof packageJson?.dependencies?.["expo-secure-store"], "string");

  const notificationPlugin = plugins.find(
    (plugin) => Array.isArray(plugin) && plugin[0] === "expo-notifications"
  );
  assert.ok(notificationPlugin, "expo-notifications plugin missing");
  assert.equal(notificationPlugin[1]?.icon, "./icons/notification-icon.png");
  assert.equal(notificationPlugin[1]?.color, "#4a90e2");
  assert.equal(expoConfig?.notification?.icon, "./icons/notification-icon.png");
  assert.equal(expoConfig?.notification?.color, "#4a90e2");

  const videoPlugin = plugins.find(
    (plugin) => Array.isArray(plugin) && plugin[0] === "expo-video"
  );
  assert.ok(videoPlugin, "expo-video plugin missing");
  assert.equal(videoPlugin[1]?.supportsBackgroundPlayback, true);

  assert.equal(expoConfig?.updates?.enabled, false);
  assert.equal(expoConfig?.updates?.url, undefined);
  assert.equal(expoConfig?.updates?.checkAutomatically, "NEVER");
  assert.equal(expoConfig?.extra?.ota?.channel, "production");
  assert.equal(expoConfig?.extra?.ota?.enabled, false);
  assert.equal(expoConfig?.extra?.ota?.autoCheckOnForeground, false);
});

test("Resolved Expo config keeps Android media permissions unblocked for save/share parity", () => {
  const expoConfig = readResolvedExpoConfig();
  const blockedPermissions = Array.isArray(expoConfig?.android?.blockedPermissions)
    ? expoConfig.android.blockedPermissions
    : [];

  const mediaPermissions = [
    "android.permission.READ_EXTERNAL_STORAGE",
    "android.permission.WRITE_EXTERNAL_STORAGE",
    "android.permission.READ_MEDIA_IMAGES",
    "android.permission.READ_MEDIA_VIDEO",
  ];

  for (const permission of mediaPermissions) {
    assert.equal(
      blockedPermissions.includes(permission) === false,
      true,
      `Expected Android blockedPermissions to exclude ${permission}`
    );
  }
});
