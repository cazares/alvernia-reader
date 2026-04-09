import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");

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
  assert.match(source, /const OFFLINE_BOOT_TIMEOUT_MS = 90000;/);
  assert.match(source, /const OFFLINE_STAGE_BATCH_TIMEOUT_MS = 15000;/);
  assert.match(source, /const withTimeout = async <T,>\(promise: Promise<T>, timeoutMs: number, timeoutMessage: string\)/);
  assert.match(source, /const stageOfflinePages = async \(onProgress\?: \(stage: number, total: number\) => void\) =>/);
  assert.match(source, /const resolveOfflineReaderUri = async \(onProgress\?: \(stage: number, total: number\) => void\) =>/);
  assert.match(source, /readAsStringAsync/);
  assert.match(source, /<WebView/);
  assert.match(source, /__SIGNO_VINO_NATIVE_FILE_MODE/);
  assert.match(source, /source=\{\{ uri: readerUri/);
  assert.match(source, /onMessage=\{handleWebMessage\}/);
  assert.match(source, /onLoadEnd=\{sendBridgeState\}/);
  assert.match(source, /const \[bootMessage, setBootMessage\] = useState\("Preparando Signo Vino"\);/);
  assert.match(source, /const \[bootDetail, setBootDetail\] = useState\("Abriendo la experiencia offline\."\);/);
  assert.match(source, /setBootMessage\("Preparando páginas offline"\);/);
  assert.match(source, /setBootDetail\(`\$\{stage\} \/ \$\{total\} páginas listas`\);/);
  assert.match(source, /setBootMessage\("No se pudo preparar Signo Vino"\);/);
  assert.match(source, /nearbyDirectorSync/);
  assert.match(source, /sync-start-director/);
  assert.match(source, /sync-start-follower/);
  assert.match(source, /Director listo\. Esperando seguidoras cercanas\./);
  assert.match(source, /Buscando director cercano\.\.\./);
  assert.doesNotMatch(source, /web\/dist\/signo-vino-offline\.html/);
  assert.doesNotMatch(source, /react-native-blob-util/);
  assert.match(source, /FileSystem\.copyAsync/);
  assert.doesNotMatch(source, /window\.OFFLINE_PAGES/);
  assert.doesNotMatch(source, /documentDirectory/);
});

test("native offline template keeps the initial page relative and the script entrypoint stable", () => {
  const indexHtml = fs.readFileSync(path.join(APP_ROOT, "assets", "offline-web", "index.html"), "utf8");

  assert.match(indexHtml, /src="pages\/page-002\.jpg"/);
  assert.match(indexHtml, /<script defer src="app\.js"><\/script>/);
});

test("shared reader resolver parses numeric input strictly and uses exact lookups first", () => {
  const appSource = fs.readFileSync(path.join(APP_ROOT, "web", "src", "app.js"), "utf8");

  assert.match(appSource, /const buildSongPageLookup = \(songIndex\) => new Map\(songIndex\.map/);
  assert.match(appSource, /const normalizeSongDraftNumber = \(draft\) =>/);
  assert.match(appSource, /state\.songPageLookup = buildSongPageLookup\(state\.songIndex\);/);
  assert.match(appSource, /const resolveSongPage = \(songNumber\) =>/);
  assert.match(appSource, /const exact = state\.songPageLookup\.get\(normalized\);/);
  assert.match(appSource, /const next = state\.songIndex\.find\(\(entry\) => entry\.song >= normalized\);/);
  assert.match(appSource, /const PAGE_IMAGE_LOAD_TIMEOUT_MS = 3000;/);
  assert.match(appSource, /const waitForImageDisplay = \(image, timeoutMs = PAGE_IMAGE_LOAD_TIMEOUT_MS\) => new Promise/);
  assert.match(appSource, /const loadPageImage = async \(pageNumber, retryToken = ""\) =>/);
  assert.match(appSource, /const loadState = await waitForImageDisplay\(pageImage\);/);
});

test("song 55 still resolves to page 55 through the manual index and exact-match lookup", () => {
  const songIndexSource = fs.readFileSync(path.join(APP_ROOT, "src", "alverniaManual2SongIndex.js"), "utf8");
  const appSource = fs.readFileSync(path.join(APP_ROOT, "web", "src", "app.js"), "utf8");

  assert.match(songIndexSource, /\[55,\s*55\]/);
  assert.match(appSource, /const buildSongPageLookup = \(songIndex\) => new Map\(songIndex\.map/);
  assert.match(appSource, /const normalizeSongDraftNumber = \(draft\) =>/);
  assert.match(appSource, /state\.songPageLookup = buildSongPageLookup\(state\.songIndex\);/);
  assert.match(appSource, /const resolveSongPage = \(songNumber\) =>/);
  assert.match(appSource, /const exact = state\.songPageLookup\.get\(normalized\);/);
  assert.match(appSource, /const next = state\.songIndex\.find\(\(entry\) => entry\.song >= normalized\);/);
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
  assert.match(indexHtml, /director-mode-badge/);
  assert.match(indexHtml, /offline-gate/);
  assert.match(indexHtml, /help-settings-label/);
  assert.match(webApp, /switchDrawerMode/);
  assert.match(webApp, /goToDraftSong/);
  assert.match(webApp, /activateDirectorShortcut/);
  assert.match(webApp, /renderActiveTab/);
  assert.match(webApp, /bindReaderEvents/);
  assert.match(webApp, /postNativeBridge/);
  assert.match(webApp, /applyNativeSyncEvent/);
  assert.match(webApp, /requestAutoFollowerMode/);
  assert.match(webApp, /DEFAULT_NATIVE_SYNC_SESSION = "2046"/);
  assert.match(webApp, /SECRET_DIRECTOR_DRAFT = "2046"/);
  assert.match(webApp, /nativeSyncAutoStartRequested/);
  assert.match(webApp, /NATIVE_FILE_MODE/);
  assert.match(webApp, /resolveAppPath/);
  assert.match(webApp, /prefetchSongPage/);
  assert.match(webApp, /img\.src = resolvePageSrc\(pageNumber\)/);
  assert.match(webApp, /nextPageUrl = resolvePageSrc\(nextPage\)/);
  assert.match(webApp, /if \(OFFLINE_PAGES \|\| NATIVE_FILE_MODE\) \{/);
  assert.match(webApp, /sync-start-director/);
  assert.match(webApp, /sync-start-follower/);
  assert.match(webApp, /requestAutoFollowerMode/);
  assert.match(webApp, /activateDirectorShortcut/);
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

test("iOS app keeps exempt-encryption declaration and nearby sync permissions", () => {
  const plistPath = path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist");
  const plistSource = fs.readFileSync(plistPath, "utf8");

  assert.match(plistSource, /ITSAppUsesNonExemptEncryption/);
  assert.match(plistSource, /NSLocalNetworkUsageDescription/);
  assert.match(plistSource, /NSBonjourServices/);
  assert.match(plistSource, /_signovivo\._tcp/);
  assert.match(plistSource, /_signovivo\._udp/);
});

test("metro bundles the offline html and bundle assets", () => {
  const metroConfigPath = path.join(APP_ROOT, "metro.config.js");
  const source = fs.readFileSync(metroConfigPath, "utf8");

  assert.match(source, /assetExts\.push\("html"\)/);
  assert.match(source, /assetExts\.push\("bundle"\)/);
});

test("nearby director sync source is present in the shipped app", () => {
  const syncClientPath = path.join(APP_ROOT, "src", "nearbyDirectorSync.js");
  const swiftModulePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift");
  const bridgePath = path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModuleBridge.m");
  const swiftSource = fs.readFileSync(swiftModulePath, "utf8");

  assert.equal(fs.existsSync(syncClientPath), true);
  assert.equal(fs.existsSync(swiftModulePath), true);
  assert.equal(fs.existsSync(bridgePath), true);
  assert.match(swiftSource, /encryptionPreference: \.none/);
  assert.match(swiftSource, /didReceiveCertificate/);
});
