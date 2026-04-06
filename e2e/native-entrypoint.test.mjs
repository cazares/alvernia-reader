import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("package main points at the native app entrypoint", () => {
  const packageJsonPath = path.join(APP_ROOT, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  assert.match(packageJson.main, /^(index\.js|expo\/AppEntry\.js)$/);
  assert.equal(packageJson.dependencies["expo-file-system"], "~19.0.21");
  assert.equal(packageJson.dependencies["react-native-webview"], "13.15.0");
  assert.equal(packageJson.dependencies.tonal, "^6.4.3");
});

test("native app entrypoint registers the root App component", () => {
  const entryPath = path.join(APP_ROOT, "index.js");
  const source = fs.readFileSync(entryPath, "utf8");

  assert.match(source, /registerRootComponent/);
  assert.match(source, /import App from "\.\/(App|PdfReaderApp)"/);
});

test("native shell hosts the bundled signovivo.com experience locally", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const source = fs.readFileSync(appPath, "utf8");

  assert.match(source, /react-native-webview/);
  assert.match(source, /OFFLINE_WEB_BUNDLE_ASSETS/);
  assert.match(source, /buildOfflineReaderHtml/);
  assert.match(source, /readAsStringAsync/);
  assert.match(source, /<WebView/);
  assert.match(source, /window\.OFFLINE_PAGES/);
  assert.match(source, /__SIGNO_VINO_NATIVE_FILE_MODE/);
  assert.match(source, /source=\{\{ html: readerHtml/);
  assert.doesNotMatch(source, /web\/dist\/signo-vino-offline\.html/);
  assert.doesNotMatch(source, /react-native-blob-util/);
  assert.doesNotMatch(source, /nearbyDirectorSync/);
  assert.doesNotMatch(source, /copyAsync/);
  assert.doesNotMatch(source, /documentDirectory/);
});

test("offline web bundle version matches the app build number so updates refresh cached files", () => {
  const assetsPath = path.join(APP_ROOT, "src", "offlineWebBundle.js");
  const assetsSource = fs.readFileSync(assetsPath, "utf8");
  const versionJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "version.json"), "utf8"));

  const match = assetsSource.match(/OFFLINE_WEB_BUNDLE_VERSION = "(\d+)"/);
  assert.ok(match, "Expected offline bundle version constant");
  assert.equal(match[1], String(versionJson.buildNumber));
});

test("native iOS project build number matches version.json", () => {
  const versionJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "version.json"), "utf8"));
  const projectSource = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo.xcodeproj", "project.pbxproj"),
    "utf8",
  );

  const matches = [...projectSource.matchAll(/CURRENT_PROJECT_VERSION = (\d+);/g)].map((match) => match[1]);
  assert.ok(matches.length >= 2, "Expected native iOS build number entries");
  for (const buildNumber of matches) {
    assert.equal(buildNumber, String(versionJson.buildNumber));
  }
});

test("restored web source still includes the drawer, numpad, browse, and hidden sync experience", () => {
  const indexHtml = fs.readFileSync(path.join(APP_ROOT, "web", "src", "index.html"), "utf8");
  const webApp = fs.readFileSync(path.join(APP_ROOT, "web", "src", "app.js"), "utf8");

  assert.match(indexHtml, /drawer-handle/);
  assert.match(indexHtml, /mode-btn-numpad/);
  assert.match(indexHtml, /mode-btn-browse/);
  assert.match(indexHtml, /numberpad-grid/);
  assert.match(indexHtml, /search-input/);
  assert.match(indexHtml, /director-sync-panel/);
  assert.match(indexHtml, /offline-gate/);
  assert.match(indexHtml, /help-settings-label/);
  assert.match(webApp, /switchDrawerMode/);
  assert.match(webApp, /goToDraftSong/);
  assert.match(webApp, /renderActiveTab/);
  assert.match(webApp, /bindReaderEvents/);
  assert.match(webApp, /postNativeBridge/);
  assert.match(webApp, /applyNativeSyncEvent/);
  assert.match(webApp, /NATIVE_FILE_MODE/);
  assert.match(webApp, /resolveAppPath/);
  assert.match(webApp, /prefetchSongPage/);
  assert.match(webApp, /img\.src = resolvePageSrc\(pageNumber\)/);
  assert.match(webApp, /nextPageUrl = resolvePageSrc\(nextPage\)/);
  assert.match(webApp, /sync-start-director/);
  assert.match(webApp, /sync-start-follower/);
});

test("reader renders exactly one visible page surface at a time", () => {
  const indexHtml = fs.readFileSync(path.join(APP_ROOT, "web", "src", "index.html"), "utf8");
  const styles = fs.readFileSync(path.join(APP_ROOT, "web", "src", "styles.css"), "utf8");

  const pageImageMatches = indexHtml.match(/id="page-image"/g) || [];
  assert.equal(pageImageMatches.length, 1, "Expected a single page image element");
  assert.match(styles, /\.viewer-shell \{[\s\S]*overflow: hidden;/);
  assert.match(styles, /\.viewer-shell \{[\s\S]*contain: layout paint style;/);
  assert.match(styles, /#page-image \{[\s\S]*object-fit: contain;/);
});

test("iOS app disables non-exempt encryption and removes local-network sync permissions", () => {
  const plistPath = path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist");
  const plistSource = fs.readFileSync(plistPath, "utf8");

  assert.match(plistSource, /ITSAppUsesNonExemptEncryption/);
  assert.doesNotMatch(plistSource, /NSLocalNetworkUsageDescription/);
  assert.doesNotMatch(plistSource, /NSBonjourServices/);
});

test("metro bundles the offline html asset", () => {
  const metroConfigPath = path.join(APP_ROOT, "metro.config.js");
  const source = fs.readFileSync(metroConfigPath, "utf8");

  assert.match(source, /assetExts\.push\("html"\)/);
});

test("nearby director sync source is absent from the shipped app", () => {
  const syncClientPath = path.join(APP_ROOT, "src", "nearbyDirectorSync.js");
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const bridgePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModuleBridge.m");

  assert.equal(fs.existsSync(syncClientPath), false);
  assert.equal(fs.existsSync(swiftModulePath), false);
  assert.equal(fs.existsSync(bridgePath), false);
});
