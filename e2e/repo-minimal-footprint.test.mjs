import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

const readJson = (relativePath) => JSON.parse(
  fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8"),
);

test("package.json only keeps reader and native sync dependencies", () => {
  const packageJson = readJson("package.json");

  assert.deepEqual(Object.keys(packageJson.scripts).sort(), [
    "android",
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
  assert.equal(dependencyNames.includes("react-native-pdf"), true);
  assert.equal(dependencyNames.includes("react-native-gesture-handler"), true);
  assert.equal(dependencyNames.includes("expo"), true);

  for (const removedName of [
    "@react-native-community/netinfo",
    "busboy",
    "expo-application",
    "expo-constants",
    "expo-file-system",
    "expo-media-library",
    "expo-notifications",
    "expo-secure-store",
    "expo-sharing",
    "expo-task-manager",
    "expo-updates",
    "expo-video",
    "react-dom",
    "react-native-web",
    "react-native-webview",
    "tonal",
  ]) {
    assert.equal(dependencyNames.includes(removedName), false, `Unexpected dependency left behind: ${removedName}`);
  }
});

test("repo removes old web, cloud, and upload entrypoints", () => {
  for (const relativePath of [
    "cloudflare",
    "web",
    "CNAME",
    "scripts/upload-server.mjs",
    "scripts/keynote-promote.mjs",
    "src/directorSync.js",
    "src/directorSync.d.ts",
    "src/songNavigation.js",
    "src/songNavigation.d.ts",
    "src/alverniaManual2SongIndex.js",
    "src/alverniaManual2SongIndex.d.ts",
  ]) {
    assert.equal(fs.existsSync(path.join(APP_ROOT, relativePath)), false, `Unexpected leftover path: ${relativePath}`);
  }
});
