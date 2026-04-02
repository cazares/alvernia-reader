import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("package main points at the native app entrypoint", () => {
  const packageJsonPath = path.join(APP_ROOT, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  assert.match(packageJson.main, /^(index\.js|expo\/AppEntry\.js)$/);
  assert.equal(packageJson.dependencies["react-native-webview"], "^13.15.0");
  assert.equal(packageJson.dependencies.tonal, "^6.4.3");
});

test("native app entrypoint registers the root App component", () => {
  const entryPath = path.join(APP_ROOT, "index.js");
  const source = fs.readFileSync(entryPath, "utf8");

  assert.match(source, /registerRootComponent/);
  assert.match(source, /import App from "\.\/(App|PdfReaderApp)"/);
});

test("native shell loads the bundled offline web reader as the source of truth", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const source = fs.readFileSync(appPath, "utf8");
  const assetsPath = path.join(APP_ROOT, "src", "offlineWebBundle.js");
  const assetsSource = fs.readFileSync(assetsPath, "utf8");

  assert.match(source, /react-native-webview/);
  assert.match(source, /react-native-blob-util/);
  assert.match(source, /OFFLINE_WEB_BUNDLE_ASSETS/);
  assert.match(source, /OFFLINE_WEB_BUNDLE_VERSION/);
  assert.match(source, /OFFLINE_BUNDLE_BATCH_SIZE = 12/);
  assert.match(source, /PAGE_ASSET_PREFIX = "pages\/"/);
  assert.match(source, /signovivo-offline-web/);
  assert.match(source, /ReactNativeBlobUtil\.fs\.cp/);
  assert.match(source, /asset\.downloadAsync\(\)/);
  assert.match(source, /coreBundleEntries\.slice\(start, start \+ OFFLINE_BUNDLE_BATCH_SIZE\)/);
  assert.match(source, /window\.OFFLINE_PAGES = /);
  assert.match(source, /pageBundleEntries\.map/);
  assert.match(source, /file:\/\/\$\{OFFLINE_BUNDLE_DIR\}\/index\.html/);
  assert.match(source, /injectedJavaScriptBeforeContentLoaded/);
  assert.match(source, /Asset\.fromModule/);
  assert.doesNotMatch(source, /Asset\.loadAsync\(bundleEntries\.map/);
  assert.match(source, /available: false/);
  assert.match(assetsSource, /offline-web\/index\.html/);
  assert.match(assetsSource, /pages\/page-001\.jpg/);
  assert.match(source, /<WebView/);
  assert.match(source, /__signoVivoReceiveNativeEvent/);
  assert.doesNotMatch(source, /nearbyDirectorSync/);
  assert.doesNotMatch(source, /react-native-pdf/);
});

test("offline web bundle version matches the app build number so updates refresh cached files", () => {
  const assetsPath = path.join(APP_ROOT, "src", "offlineWebBundle.js");
  const assetsSource = fs.readFileSync(assetsPath, "utf8");
  const versionJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "version.json"), "utf8"));

  const match = assetsSource.match(/OFFLINE_WEB_BUNDLE_VERSION = "(\d+)"/);
  assert.ok(match, "Expected offline bundle version constant");
  assert.equal(match[1], String(versionJson.buildNumber));
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
  assert.match(indexHtml, /help-settings-label/);
  assert.match(webApp, /switchDrawerMode/);
  assert.match(webApp, /goToDraftSong/);
  assert.match(webApp, /renderActiveTab/);
  assert.match(webApp, /bindReaderEvents/);
  assert.match(webApp, /postNativeBridge/);
  assert.match(webApp, /applyNativeSyncEvent/);
  assert.match(webApp, /NATIVE_FILE_MODE/);
  assert.match(webApp, /resolveAppPath/);
  assert.match(webApp, /sync-start-director/);
  assert.match(webApp, /sync-start-follower/);
});

test("iOS app includes nearby offline director sync permissions and native bridge", () => {
  const plistPath = path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist");
  const plistSource = fs.readFileSync(plistPath, "utf8");
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const swiftSource = fs.readFileSync(swiftModulePath, "utf8");

  assert.match(plistSource, /NSLocalNetworkUsageDescription/);
  assert.match(plistSource, /_signovivo\._tcp/);
  assert.match(swiftSource, /MultipeerConnectivity/);
  assert.match(swiftSource, /MCNearbyServiceAdvertiser/);
  assert.match(swiftSource, /MCNearbyServiceBrowser/);
});

test("metro bundles the offline html asset", () => {
  const metroConfigPath = path.join(APP_ROOT, "metro.config.js");
  const source = fs.readFileSync(metroConfigPath, "utf8");

  assert.match(source, /assetExts\.push\("html"\)/);
});

test("native sync module does not shadow the emitState method during reset", () => {
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const swiftSource = fs.readFileSync(swiftModulePath, "utf8");

  assert.match(swiftSource, /private func resetTransport\(emitState shouldEmitState: Bool\)/);
  assert.match(swiftSource, /if shouldEmitState \{\s*emitState\(status: "idle"\)/s);
  assert.doesNotMatch(swiftSource, /private func resetTransport\(emitState: Bool\)/);
});
