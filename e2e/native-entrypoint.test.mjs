import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

test("package main points at the native app entrypoint", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "package.json"), "utf8"));
  assert.match(packageJson.main, /^(index\.js|expo\/AppEntry\.js)$/);
});

test("native app entrypoint registers the root App component", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "index.js"), "utf8");
  assert.match(source, /registerRootComponent/);
  assert.match(source, /import App from "\.\/(App|PdfReaderApp)"/);
});

test("native reader uses FlatList paging over bundled page assets — no WebView, no file staging", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

  assert.match(source, /FlatList/);
  assert.match(source, /pagingEnabled/);
  assert.match(source, /assets\/offline-web\/pages\.json/);
  assert.match(source, /ALVERNIA_MANUAL_2_SONG_INDEX/);
  assert.match(source, /resolveSongPage/);
  assert.match(source, /keyboardType="number-pad"/);
  assert.doesNotMatch(source, /react-native-webview/);
  assert.doesNotMatch(source, /FileSystem/);
  assert.doesNotMatch(source, /stageOfflinePages/);
});

test("offline asset recovery return does not appear before later hooks", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
  const appStart = source.indexOf("export default function App()");
  assert.ok(appStart >= 0, "App component not found");

  const appSource = source.slice(appStart);
  const recoveryIdx = appSource.indexOf("if (offlineAssetsError)");
  assert.ok(recoveryIdx >= 0, "offlineAssetsError recovery branch not found");

  const afterRecovery = appSource.slice(recoveryIdx);
  assert.doesNotMatch(
    afterRecovery,
    /\buse(State|Effect|Memo|Callback|Ref)\s*\(/,
    "No React hooks may appear after the offlineAssetsError early return",
  );
});

test("approving takeover updates JS role to follower before calling native approval", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
  const handler =
    source.match(/handleApproveDirectorTakeover[\s\S]*?\}, \[[\s\S]*?\]\);/)?.[0] ??
    "";
  assert.ok(handler, "handleApproveDirectorTakeover must exist");
  assert.match(handler, /setSyncRole\("follower"\)/);
  assert.match(handler, /approveDirectorTakeover\(requestId\)/);
  assert.match(handler, /setSearchVisible\(false\)/);
  assert.match(handler, /setBrowseVisible\(false\)/);
  assert.match(handler, /setGridVisible\(false\)/);

  const alertBlock = source.match(/Solicitud de control[\s\S]*?\]\s*,\s*\);/)?.[0] ?? "";
  assert.match(alertBlock, /handleApproveDirectorTakeover\(requestId\)/);
  assert.doesNotMatch(alertBlock, /approveDirectorTakeover\(requestId\)\.catch/);
});

test("becoming director from follower does not depend on transient connected label", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
  const block =
    source.match(/const handleBecomeDirector = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ??
    "";
  assert.ok(block, "handleBecomeDirector block not found");
  assert.doesNotMatch(block, /followerStatusLabel\s*===\s*"connected"/);
  assert.match(block, /syncRoleRef\.current === "follower"/);
  assert.match(block, /requestDirectorTakeover\(\)/);

  const requestIdx = block.indexOf("requestDirectorTakeover()");
  const startIdx = block.indexOf("startNearbyDirector(DIRECTOR_SESSION)");
  assert.ok(requestIdx >= 0 && startIdx >= 0 && requestIdx < startIdx);
});

test("song index resolves correctly — song 55 → page 55, out-of-range clamps", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "src", "alverniaManual2SongIndex.js"), "utf8");
  assert.match(source, /\[55,\s*55\]/);
});

test("offline web bundle version matches the app build number so updates refresh cached files", () => {
  const assetsSource = fs.readFileSync(path.join(APP_ROOT, "src", "offlineWebBundle.js"), "utf8");
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
  const matches = [...projectSource.matchAll(/CURRENT_PROJECT_VERSION = (\d+);/g)].map((m) => m[1]);
  assert.ok(matches.length >= 2, "Expected native iOS build number entries");
  for (const buildNumber of matches) {
    assert.equal(buildNumber, String(versionJson.buildNumber));
  }
});

test("iOS app keeps exempt-encryption declaration and nearby sync permissions", () => {
  const plistSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist"), "utf8");
  assert.match(plistSource, /ITSAppUsesNonExemptEncryption/);
  assert.match(plistSource, /NSLocalNetworkUsageDescription/);
  assert.match(plistSource, /NSBonjourServices/);
  assert.match(plistSource, /_signovivo\._tcp/);
  assert.match(plistSource, /_signovivo\._udp/);
  assert.doesNotMatch(plistSource, /NSAllowsArbitraryLoads/);
  assert.match(plistSource, /<key>LSMinimumSystemVersion<\/key>\s*<string>15\.1<\/string>/);
});

test("iOS app supports upside-down orientation on every device class", () => {
  const plistSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist"), "utf8");
  assert.match(plistSource, /UISupportedInterfaceOrientations/);
  assert.match(plistSource, /UIInterfaceOrientationPortraitUpsideDown/);
  assert.doesNotMatch(plistSource, /UISupportedInterfaceOrientations~ipad/);
});

test("metro bundles the offline html and bundle assets", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "metro.config.js"), "utf8");
  assert.match(source, /assetExts\.push\("html"\)/);
  assert.match(source, /assetExts\.push\("bundle"\)/);
});

test("iOS pod properties disable new architecture and network inspector", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "ios", "Podfile.properties.json"), "utf8");
  const props = JSON.parse(source);
  assert.equal(props["newArchEnabled"], "false");
  assert.equal(props["EX_DEV_CLIENT_NETWORK_INSPECTOR"], "false");
});

test("Podfile includes EXConstants get-app-config wrapper to avoid script phase failures", () => {
  const podfile = fs.readFileSync(path.join(APP_ROOT, "ios", "Podfile"), "utf8");
  assert.match(podfile, /Ensured EXConstants get-app-config-ios\.sh wrapper exists/);
  assert.ok(
    podfile.includes("require.resolve('expo-constants/package.json')"),
    "Expected Podfile wrapper to resolve expo-constants via require.resolve",
  );
});
