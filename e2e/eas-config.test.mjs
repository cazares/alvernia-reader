import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const thisDir = path.dirname(fileURLToPath(import.meta.url));
const easConfigPath = path.resolve(thisDir, "../eas.json");
const appConfigPath = path.resolve(thisDir, "../app.json");
const versionPath = path.resolve(thisDir, "../version.json");
const appRoot = path.resolve(thisDir, "..");

let resolvedExpoConfig = null;

const readJson = (filePath) => JSON.parse(fs.readFileSync(filePath, "utf8"));

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
  const easConfig = readJson(easConfigPath);
  assert.equal(easConfig?.cli?.appVersionSource, "local");
});

test("EAS production build keeps store distribution and production channel", () => {
  const easConfig = readJson(easConfigPath);
  assert.equal(easConfig?.build?.production?.distribution, "store");
  assert.equal(easConfig?.build?.production?.channel, "production");
});

test("Static app config keeps standalone reader identity", () => {
  const appConfig = readJson(appConfigPath);
  assert.equal(appConfig?.expo?.name, "Signo Vivo");
  assert.equal(appConfig?.expo?.slug, "alvernia-reader");
  assert.equal(appConfig?.expo?.android?.package, "com.cazares.alverniareader");
  assert.equal(appConfig?.expo?.ios?.bundleIdentifier, "com.cazares.alverniareader");
  assert.equal(appConfig?.expo?.extra?.eas?.projectId, "8f4aeff3-940f-4ec2-b82d-89b430f5c8be");
});

test("Release assets required by app config exist in-repo", () => {
  const requiredPaths = [
    "assets/icon.png",
    "assets/adaptive-icon.png",
    "assets/splash.png",
    "assets/favicon.png",
    "assets/alvernia_manual_2.pdf"
  ];

  for (const relativePath of requiredPaths) {
    const fullPath = path.resolve(appRoot, relativePath);
    assert.equal(fs.existsSync(fullPath), true, `Missing required release asset: ${relativePath}`);
  }
});

test("Resolved Expo config preserves versioning and embedded-only updates", () => {
  const version = readJson(versionPath);
  const expoConfig = readResolvedExpoConfig();

  assert.equal(expoConfig?.version, version.baseVersion);
  assert.equal(expoConfig?.runtimeVersion, version.baseVersion);
  assert.equal(expoConfig?.newArchEnabled, false);
  assert.deepEqual(expoConfig?.platforms, ["ios", "android"]);
  assert.equal(expoConfig?.android?.package, "com.cazares.alverniareader");
  assert.equal(expoConfig?.android?.versionCode, Number(version.buildNumber));
  assert.equal(expoConfig?.ios?.bundleIdentifier, "com.cazares.alverniareader");
  assert.equal(expoConfig?.ios?.buildNumber, String(version.buildNumber));
  assert.equal(expoConfig?.updates?.enabled, false);
  assert.equal(expoConfig?.updates?.checkAutomatically, "NEVER");
});
