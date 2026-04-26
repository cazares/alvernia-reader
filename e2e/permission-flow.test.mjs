import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const appSource = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
const jsSyncSource = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.js"), "utf8");
const swiftSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
const bridgeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModuleBridge.m"), "utf8");
const infoPlist = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "Info.plist"), "utf8");
const pbxproj = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo.xcodeproj", "project.pbxproj"), "utf8");
const versionJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "version.json"), "utf8"));
const appJson = JSON.parse(fs.readFileSync(path.join(APP_ROOT, "app.json"), "utf8"));

// Build 258 shipped without these plist keys, leading iOS to silently deny Local Network access
// for any subsequent build on the same install. Re-confirm every required key on every build.
test("Info.plist declares every iOS permission MultipeerConnectivity needs", () => {
  assert.match(infoPlist, /<key>NSLocalNetworkUsageDescription<\/key>\s*<string>[^<]+<\/string>/);
  assert.match(infoPlist, /<key>NSBluetoothAlwaysUsageDescription<\/key>\s*<string>[^<]+<\/string>/);
  assert.match(infoPlist, /<key>NSBonjourServices<\/key>/);
  assert.match(infoPlist, /<string>_signovivo\._tcp<\/string>/);
  assert.match(infoPlist, /<string>_signovivo\._udp<\/string>/);
});

// The `primePermissions` priming flow only triggers the iOS Local Network dialog when a real
// MCNearbyServiceBrowser with a delegate is held strongly long enough for iOS to surface the
// system prompt. Build 261 shipped a delegate-less browser that no-op'd silently — guard
// against that regression in every layer.
test("primePermissions exists in JS, bridge, and Swift layers", () => {
  assert.match(jsSyncSource, /export const primeNearbyPermissions/);
  assert.match(jsSyncSource, /nativeModule\.primePermissions/);
  assert.match(bridgeSource, /RCT_EXTERN_METHOD\(primePermissions:/);
  assert.match(swiftSource, /@objc\(primePermissions:rejecter:\)/);
  assert.match(swiftSource, /func primePermissions\(/);
});

test("director takeover flow is wired through JS, bridge, and Swift layers", () => {
  assert.match(jsSyncSource, /export const requestDirectorTakeover/);
  assert.match(jsSyncSource, /nativeModule\.requestDirectorTakeover/);
  assert.match(bridgeSource, /RCT_EXTERN_METHOD\(requestDirectorTakeover:/);
  assert.match(swiftSource, /@objc\(requestDirectorTakeover:rejecter:\)/);

  assert.match(jsSyncSource, /export const approveDirectorTakeover/);
  assert.match(bridgeSource, /RCT_EXTERN_METHOD\(approveDirectorTakeover:/);
  assert.match(swiftSource, /@objc\(approveDirectorTakeover:resolver:rejecter:\)/);

  assert.match(jsSyncSource, /export const denyDirectorTakeover/);
  assert.match(bridgeSource, /RCT_EXTERN_METHOD\(denyDirectorTakeover:/);
  assert.match(swiftSource, /@objc\(denyDirectorTakeover:resolver:rejecter:\)/);
});

test("Swift primePermissions sets delegates and retains browser/advertiser", () => {
  // Find the body of the primePermissions function.
  const match = swiftSource.match(/func primePermissions\([\s\S]*?\n  \}\n/);
  assert.ok(match, "primePermissions function body not found");
  const body = match[0];

  // Delegate must be set — without it iOS won't surface the Local Network dialog.
  assert.match(body, /browser\.delegate\s*=\s*self/);
  assert.match(body, /advertiser\.delegate\s*=\s*self/);

  // Both must be retained on `self` so they outlive the function and stay alive during the prompt.
  assert.match(body, /self\.primingBrowser\s*=\s*browser/);
  assert.match(body, /self\.primingAdvertiser\s*=\s*advertiser/);
  assert.match(swiftSource, /private var primingBrowser: MCNearbyServiceBrowser\?/);
  assert.match(swiftSource, /private var primingAdvertiser: MCNearbyServiceAdvertiser\?/);

  // Both must actually start, so iOS sees an active service request.
  assert.match(body, /startBrowsingForPeers\(\)/);
  assert.match(body, /startAdvertisingPeer\(\)/);
});

// The fix must trigger immediately on mount, regardless of mode/onboarding/persisted state.
test("primeNearbyPermissions fires from a top-level useEffect with empty deps", () => {
  assert.match(appSource, /import \{[\s\S]*?primeNearbyPermissions[\s\S]*?\} from "\.\/src\/nearbyDirectorSync"/);
  assert.match(
    appSource,
    /useEffect\(\(\) => \{\s*primeNearbyPermissions\(\)\.catch\(\(\) => \{\}\);\s*\},\s*\[\]\);/,
  );
});

// First launch should NOT show the choir-code modal — the app must boot straight into the
// hymnal so iOS surfaces Local Network permission immediately on the first session.
test("first launch skips onboarding and boots directly into the nonStandard hymnal", () => {
  assert.match(appSource, /\/\/ First launch — skip code prompt, boot directly as hymnal/);
  assert.match(appSource, /const firstBook: BookId = "hymns-4";/);
  assert.match(
    appSource,
    /\[STORAGE_KEYS\.onboardingComplete, "1"\],\s*\[STORAGE_KEYS\.mode, "nonStandard"\],\s*\[STORAGE_KEYS\.activeBookId, firstBook\],/,
  );
  assert.match(appSource, /setOnboardingVisible\(false\);\s*setBooted\(true\);/);
});

// Soft-reset (744668486 / triple refresh) must NOT bounce back to the onboarding modal —
// regression we caught in the 259→260 cycle.
test("soft reset re-boots into hymnal and skips onboarding", () => {
  // Cleared bookstate followed by an immediate re-seed of nonStandard hymnal storage keys.
  assert.match(
    appSource,
    /clearAllBookState\(\)[\s\S]{0,400}AsyncStorage\.multiSet\(\[\s*\[STORAGE_KEYS\.onboardingComplete, "1"\],\s*\[STORAGE_KEYS\.mode, "nonStandard"\],\s*\[STORAGE_KEYS\.activeBookId, book\],/,
  );
  assert.match(appSource, /setOnboardingVisible\(false\);[\s\S]{0,200}forceRerender/);
});

// When director-mode startup fails (typically Local Network denied), users need a one-tap
// path into Settings plus a force-close instruction (reinstall is no longer recommended).
test("director-mode error offers Settings deep link and force-close guidance", () => {
  assert.match(appSource, /import \{[\s\S]*?Linking,[\s\S]*?\} from "react-native"/);
  assert.match(appSource, /No se pudo activar el modo director/);
  assert.match(appSource, /Linking\.openURL\("app-settings:"\)/);
  assert.match(appSource, /cierra la app completamente/);
  assert.doesNotMatch(appSource, /desinstala y reinstala/);
});

// Versions must stay in lockstep across all manifests; mismatches caused build 254/255 confusion.
test("build number is consistent across version.json, app.json, Info.plist, and pbxproj", () => {
  const buildNumber = String(versionJson.buildNumber);
  const baseVersion = versionJson.baseVersion;

  assert.equal(appJson.expo.version, baseVersion, "app.json version must match version.json baseVersion");
  assert.equal(appJson.expo.ios.buildNumber, buildNumber, "app.json iOS buildNumber drift");
  assert.equal(appJson.expo.android.versionCode, Number(buildNumber), "android versionCode drift");

  assert.match(infoPlist, new RegExp(`<key>CFBundleVersion</key>\\s*<string>${buildNumber}</string>`));
  assert.match(infoPlist, new RegExp(`<key>CFBundleShortVersionString</key>\\s*<string>${baseVersion}</string>`));

  // pbxproj sets these once per Debug+Release config — both must match.
  const projectVersionMatches = pbxproj.match(/CURRENT_PROJECT_VERSION = (\d+);/g) || [];
  assert.ok(projectVersionMatches.length >= 2, "expected CURRENT_PROJECT_VERSION in both build configs");
  for (const line of projectVersionMatches) {
    assert.equal(line, `CURRENT_PROJECT_VERSION = ${buildNumber};`, `pbxproj drift: ${line}`);
  }
  const marketingMatches = pbxproj.match(/MARKETING_VERSION = ([^;]+);/g) || [];
  for (const line of marketingMatches) {
    assert.equal(line, `MARKETING_VERSION = ${baseVersion};`, `pbxproj MARKETING_VERSION drift: ${line}`);
  }
});

// The bootstrap effect must fire whenever sync becomes available, not just once — it's the
// real trigger that calls startNearbyFollower (which uses the delegate-attached browser
// inside Swift) and is the most reliable second chance to surface the iOS dialog.
test("bootstrap effect calls startNearbyFollower when sync is available", () => {
  assert.match(appSource, /useEffect\(\(\) => \{\s*if \(!syncAvailable\) return;[\s\S]{0,400}bootstrapNearbySyncRole/);
  assert.match(appSource, /await startNearbyFollower\(DIRECTOR_SESSION\)\.catch\(\(\) => \{\}\);/);
});

// Director session code is fixed; if it ever changes the followers will silently fail to
// match. Lock it down so accidental edits are caught.
test("DIRECTOR_SESSION constant is the fixed shared session code", () => {
  assert.match(appSource, /const DIRECTOR_SESSION = "1234";/);
});
