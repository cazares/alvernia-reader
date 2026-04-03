import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("package main points at the native app entrypoint", () => {
  const packageJsonPath = path.join(APP_ROOT, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  assert.match(packageJson.main, /^(index\.js|expo\/AppEntry\.js)$/);
  assert.equal(packageJson.dependencies["expo-asset"], "^12.0.12");
});

test("native app entrypoint registers the root App component", () => {
  const entryPath = path.join(APP_ROOT, "index.js");
  const source = fs.readFileSync(entryPath, "utf8");

  assert.match(source, /registerRootComponent/);
  assert.match(source, /import App from "\.\/(App|PdfReaderApp)"/);
});

test("native shell hosts a single bundled PDF with native controls", () => {
  const appPath = path.join(APP_ROOT, "PdfReaderApp.tsx");
  const source = fs.readFileSync(appPath, "utf8");

  assert.match(source, /Asset\.fromModule/);
  assert.match(source, /assets\/alvernia_manual_2\.pdf/);
  assert.match(source, /SignoVivoPdfNativeView/);
  assert.match(source, /keyboardType="number-pad"/);
  assert.match(source, /Ir a pagina/);
  assert.doesNotMatch(source, /react-native-blob-util/);
  assert.doesNotMatch(source, /nearbyDirectorSync/);
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

test("native iOS target includes a dedicated PDFKit bridge", () => {
  const managerPath = path.join(APP_ROOT, "ios", "SignoVivo", "SignoVivoPdfViewManager.swift");
  const bridgePath = path.join(APP_ROOT, "ios", "SignoVivo", "SignoVivoPdfViewManagerBridge.m");
  const managerSource = fs.readFileSync(managerPath, "utf8");
  const bridgeSource = fs.readFileSync(bridgePath, "utf8");

  assert.match(managerSource, /import PDFKit/);
  assert.match(managerSource, /class SignoVivoPdfViewManager/);
  assert.match(managerSource, /PDFView/);
  assert.match(bridgeSource, /RCT_EXTERN_MODULE\(SignoVivoPdfView/);
});

test("iOS app disables non-exempt encryption and removes local-network sync permissions", () => {
  const plistPath = path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist");
  const plistSource = fs.readFileSync(plistPath, "utf8");

  assert.match(plistSource, /ITSAppUsesNonExemptEncryption/);
  assert.doesNotMatch(plistSource, /NSAppTransportSecurity/);
  assert.doesNotMatch(plistSource, /NSLocalNetworkUsageDescription/);
  assert.doesNotMatch(plistSource, /NSBonjourServices/);
});

test("metro can still bundle html assets for build-time tooling if needed", () => {
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
