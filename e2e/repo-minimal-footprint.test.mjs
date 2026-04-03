import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

const readJson = (relativePath) => JSON.parse(
  fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8"),
);

test("package.json keeps the offline PDF-reader dependency footprint lean", () => {
  const packageJson = readJson("package.json");

  assert.deepEqual(Object.keys(packageJson.scripts).sort(), [
    "android",
    "build:web",
    "build:web:offline",
    "ios",
    "ios:local",
    "start",
    "test:e2e",
    "typecheck",
  ]);

  assert.deepEqual(Object.keys(packageJson.devDependencies).sort(), [
    "@types/react",
    "babel-preset-expo",
    "typescript",
  ]);

  const dependencyNames = Object.keys(packageJson.dependencies).sort();
  assert.equal(dependencyNames.includes("expo"), true);
  assert.equal(dependencyNames.includes("expo-asset"), true);

  for (const removedName of [
    "@react-native-community/netinfo",
    "@react-native-async-storage/async-storage",
    "busboy",
    "expo-application",
    "expo-constants",
    "expo-file-system",
    "expo-haptics",
    "expo-media-library",
    "expo-notifications",
    "expo-secure-store",
    "expo-sharing",
    "expo-task-manager",
    "expo-updates",
    "expo-video",
    "react-native-pdf",
    "react-native-webview",
    "react-dom",
    "react-native-web",
  ]) {
    assert.equal(dependencyNames.includes(removedName), false, `Unexpected dependency left behind: ${removedName}`);
  }
});

test("repo keeps build-time web source but removes shipped offline web assets", () => {
  for (const relativePath of [
    "web",
    "web/src/index.html",
    "web/src/app.js",
    "web/src/styles.css",
  ]) {
    assert.equal(fs.existsSync(path.join(APP_ROOT, relativePath)), true, `Expected web path missing: ${relativePath}`);
  }

  for (const relativePath of [
    "web/dist/signo-vino-offline.html",
    "assets/offline-web",
    "cloudflare",
    "CNAME",
    "scripts/upload-server.mjs",
    "scripts/keynote-promote.mjs",
    "src/directorSync.js",
    "src/directorSync.d.ts",
    "assets/signo-vino-native.html",
    "src/offlineWebAssets.js",
    "src/offlineWebAssets.d.ts",
    "src/offlineWebBundle.js",
    "src/offlineWebBundle.d.ts",
    "src/offlinePageAssets.js",
    "src/offlinePageAssets.d.ts",
  ]) {
    assert.equal(fs.existsSync(path.join(APP_ROOT, relativePath)), false, `Unexpected leftover path: ${relativePath}`);
  }
});

test("repo keeps the song index data used by the web build", () => {
  for (const relativePath of [
    "src/alverniaManual2SongIndex.js",
    "src/alverniaManual2SongIndex.d.ts",
  ]) {
    assert.equal(fs.existsSync(path.join(APP_ROOT, relativePath)), true, `Expected helper path missing: ${relativePath}`);
  }
});
