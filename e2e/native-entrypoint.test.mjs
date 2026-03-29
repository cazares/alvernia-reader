import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("package main points at the native app entrypoint", () => {
  const packageJsonPath = path.join(APP_ROOT, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  assert.match(packageJson.main, /^(index\.js|expo\/AppEntry\.js)$/);
  assert.equal(packageJson.dependencies["react-native-gesture-handler"], "~2.28.0");
});

test("native app entrypoint registers the root App component", () => {
  const entryPath = path.join(APP_ROOT, "index.js");
  const source = fs.readFileSync(entryPath, "utf8");

  assert.match(source, /registerRootComponent/);
  assert.match(source, /import App from "\.\/(App|PdfReaderApp)"/);
});

test("pdf reader stays focused on the embedded PDF with a hidden long-press sync entry", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const source = fs.readFileSync(appPath, "utf8");

  assert.match(source, /GestureHandlerRootView/);
  assert.match(source, /accessibilityLabel="Página actual"/);
  assert.match(source, /style=\{styles\.pageBadge\}/);
  assert.doesNotMatch(source, /onTouchStartCapture/);
  assert.doesNotMatch(source, /onTouchMoveCapture/);
  assert.doesNotMatch(source, /onTouchEndCapture/);
  assert.doesNotMatch(source, /Ir a cancion/);
  assert.doesNotMatch(source, /songNavigation/);
});

test("pdf reader includes a hidden long-press director sync mode", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const source = fs.readFileSync(appPath, "utf8");

  assert.match(source, /delayLongPress=\{DIRECTOR_LONG_PRESS_MS\}/);
  assert.match(source, /onLongPress=\{openSyncModal\}/);
  assert.match(source, /Entrar como director/);
  assert.match(source, /Seguir director/);
  assert.match(source, /sin internet/);
  assert.doesNotMatch(source, /directorSync/);
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

test("offline director mode remains usable even with zero connected iPads", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const appSource = fs.readFileSync(appPath, "utf8");
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const swiftSource = fs.readFileSync(swiftModulePath, "utf8");

  assert.match(appSource, /Director offline listo en/);
  assert.match(swiftSource, /guard !session\.connectedPeers\.isEmpty else \{/);
  assert.match(swiftSource, /emitState\(status: "waiting-followers"\)/);
  assert.match(swiftSource, /resolve\(\["deliveredPeers": 0\]\)/);
});
