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
  const assetsPath = path.join(APP_ROOT, "src", "offlineWebAssets.js");
  const assetsSource = fs.readFileSync(assetsPath, "utf8");

  assert.match(source, /react-native-webview/);
  assert.match(source, /OFFLINE_PAGE_MODULES/);
  assert.match(source, /injectedJavaScriptBeforeContentLoaded/);
  assert.match(source, /window\.OFFLINE_PAGES/);
  assert.match(source, /Asset\.fromModule/);
  assert.match(assetsSource, /signo-vino-native\.html/);
  assert.match(source, /<WebView/);
  assert.match(source, /onMessage=\{handleWebMessage\}/);
  assert.match(source, /sendNearbyDirectorPageUpdate/);
  assert.match(source, /startNearbyDirector/);
  assert.match(source, /startNearbyFollower/);
  assert.match(source, /stopNearbyDirectorSync/);
  assert.match(source, /__signoVivoReceiveNativeEvent/);
  assert.doesNotMatch(source, /react-native-pdf/);
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

test("offline director native module remains available for later bridge work", () => {
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const swiftSource = fs.readFileSync(swiftModulePath, "utf8");

  assert.match(swiftSource, /guard !session\.connectedPeers\.isEmpty else \{/);
  assert.match(swiftSource, /emitState\(status: "waiting-followers"\)/);
  assert.match(swiftSource, /resolve\(\["deliveredPeers": 0\]\)/);
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
