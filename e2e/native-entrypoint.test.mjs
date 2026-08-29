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

test("native app is a react-native-webview shell — no native FlatList reader", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

  // The reader UI now lives in a bundled web app loaded into a WKWebView.
  assert.match(source, /react-native-webview/);
  assert.match(source, /injectedJavaScriptBeforeContentLoaded/);
  assert.match(source, /__signoVivoReceiveNativeEvent/);

  // The old 3,536-line native FlatList/PDF reader is gone. Ignore the historical-note
  // comment that still references it; assert the component is neither imported nor rendered.
  const code = source.replace(/\/\/[^\n]*\n/g, "\n").replace(/\/\*[\s\S]*?\*\//g, "");
  assert.doesNotMatch(code, /\bFlatList\b/);
  assert.doesNotMatch(code, /pagingEnabled/);
});

// Removed: "offline asset recovery return does not appear before later hooks" —
// pinned the `offlineAssetsError` offline-staging early-return hooks ordering of the
// native FlatList reader. The WebView shell loads the bundle from file:// and has no
// such offline-staging machinery. Dead-behavior test; restore from git if it returns.

// Removed: "approving takeover…" and "becoming director from follower…".
// These pinned the director-takeover UI flow that the "kill takeover / lock to
// director forever" refactor dismantled (the asserted alert strings are gone from
// PdfReaderApp.tsx). Dead-behavior tests — restore from git history if takeover returns.

test("song index resolves correctly — song 55 → page 55, out-of-range clamps", async () => {
  // What the old assertion missed: its whole body was `assert.match(source, /\[55, 55\]/)` against
  // src/alverniaManual2SongIndex.js — a forty-line file holding a literal and an Object.freeze, with
  // no resolver and no clamp anywhere in it. It never opened the file where clamping actually lives,
  // so deleting the upper bound from clampPage (web/src/app.js) left this green. It also passed on a
  // commented-out pair. This version resolves the song through the REAL frozen index and then runs
  // the REAL clampPage as code, so a missing bound changes a returned NUMBER, not a line of text.
  const { ALVERNIA_MANUAL_2_SONG_INDEX } = await import("../src/alverniaManual2SongIndex.js");
  const songToPage = new Map(ALVERNIA_MANUAL_2_SONG_INDEX.map(({ song, page }) => [song, page]));
  assert.equal(songToPage.get(55), 55, "song 55 no longer resolves to page 55");
  assert.equal(songToPage.get(347), 346, "the one deliberately irregular pair [347, 346] moved");
  assert.equal(songToPage.get(83), undefined, "song 83 is a gap in the book — it must not resolve");

  // Lift clampPage out of web/src/app.js by its declaration and run it. Both endpoints are
  // structural (the declaration itself, and the NEXT declaration) and both are asserted found, so a
  // rename or a deletion fails here instead of silently widening the window to the end of the file.
  const app = fs.readFileSync(path.join(APP_ROOT, "web", "src", "app.js"), "utf8");
  const start = app.indexOf("const clampPage = (pageNumber) => {");
  assert.ok(start > 0, "clampPage is gone from web/src/app.js — nothing bounds a page number now");
  const end = app.indexOf("const clampSongIndex", start);
  assert.ok(end > start, "clampPage's slice has no closing marker — refusing to read to EOF");
  const makeClamp = new Function("state", `${app.slice(start, end)}\nreturn clampPage;`);

  // A 373-page book, the reader parked on page 7.
  const clampPage = makeClamp({ totalPages: 373, currentPage: 7 });
  assert.equal(clampPage(songToPage.get(55)), 55, "an in-range page must pass through untouched");
  assert.equal(clampPage(373), 373, "the last page of the book must not be clamped away");

  // THE UPPER BOUND. Without it, a page past the end reaches pageFileName → page-999.webp → 404,
  // and the render sticks — the failure clampPage's own comment says it exists to prevent. This is
  // the assertion that goes red when `Math.min(n, total)` is dropped.
  assert.equal(clampPage(374), 373, "a page one past the end escaped the clamp");
  assert.equal(clampPage(999), 373, "a page far past the end escaped the clamp — page-999.webp 404s");

  // The lower bound and the coercions, which the same function owns.
  assert.equal(clampPage(0), 1, "page 0 escaped the lower clamp");
  assert.equal(clampPage(-5), 1, "a negative page escaped the lower clamp");
  assert.equal(clampPage(2.7), 2, "a float reached pageFileName — page-2.7.webp would 404");
  assert.equal(clampPage(NaN), 7, "NaN must fall back to the current page, not page-NaN.webp");

  // Before pages.json lands, totalPages is unknown and the book is treated as one page long. A
  // director's snapshot arriving in that window must not be believed either.
  const clampBeforeManifest = makeClamp({ totalPages: 0, currentPage: 1 });
  assert.equal(clampBeforeManifest(99), 1, "a page was trusted before totalPages was known");
});

test("app build number bumps with releases (version.json is the source of truth)", () => {
  const versionJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "version.json"), "utf8"));
  assert.ok(Number(versionJson.buildNumber) > 0, "version.json buildNumber must be > 0");
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
