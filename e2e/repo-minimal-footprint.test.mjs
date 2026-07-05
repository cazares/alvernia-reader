import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

const readJson = (relativePath) => JSON.parse(
  fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8"),
);

test("package.json keeps the offline web-host dependency footprint lean", () => {
  const packageJson = readJson("package.json");

  assert.deepEqual(Object.keys(packageJson.scripts).sort(), [
    "android",
    "build:web",
    "deploy:web",
    "ios",
    "ios:local",
    "ios:mpad",
    "postinstall",
    "preios",
    "preios:mpad",
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
  assert.equal(dependencyNames.includes("expo-haptics"), true);
  assert.equal(dependencyNames.includes("react-native-webview"), true);

  for (const removedName of [
    "@react-native-community/netinfo",
    "@react-native-picker/picker",
    "tonal",
    "busboy",
    "expo-application",
    "expo-constants",
    "expo-media-library",
    "expo-notifications",
    "expo-sharing",
    "expo-task-manager",
    "expo-updates",
    "expo-video",
    "react-native-pdf",
    "react-dom",
    "react-native-web",
  ]) {
    assert.equal(dependencyNames.includes(removedName), false, `Unexpected dependency left behind: ${removedName}`);
  }
});

test("repo keeps the restored web source but removes old cloud upload entrypoints", () => {
  for (const relativePath of [
    "web",
    "web/src/index.html",
    "web/src/app.js",
    "web/src/styles.css",
  ]) {
    assert.equal(fs.existsSync(path.join(APP_ROOT, relativePath)), true, `Expected web path missing: ${relativePath}`);
  }

  for (const relativePath of [
    "cloudflare",
    "CNAME",
    "scripts/upload-server.mjs",
    "scripts/keynote-promote.mjs",
    "scripts/gen-offline-book-asset-map.mjs",
    "src/directorSync.js",
    "src/directorSync.d.ts",
    "src/offlineWebBundle.d.ts",
    "assets/signo-vino-native.html",
    "src/offlineWebAssets.js",
    "src/offlineWebAssets.d.ts",
    "src/offlinePageAssets.js",
    "src/offlinePageAssets.d.ts",
    "web/build-offline.mjs",
    // Retired: stale committed song JSON that fed only dead offlineBooks exports.
    // The web build generates the live index from src/alverniaManual2SongIndex.js + the PDF.
    "assets/standard/pages.json",
    "assets/standard/song-titles.json",
    "assets/standard/song-search-index.json",
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
