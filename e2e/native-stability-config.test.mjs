import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("iOS pod properties disable new architecture and network inspector", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "ios", "Podfile.properties.json"), "utf8");

  assert.match(source, /"newArchEnabled"\s*:\s*"false"/);
  assert.match(source, /"EX_DEV_CLIENT_NETWORK_INSPECTOR"\s*:\s*"false"/);
});

test("Android gradle properties disable new architecture and network inspector", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "android", "gradle.properties"), "utf8");

  assert.match(source, /^newArchEnabled=false$/m);
  assert.match(source, /^EX_DEV_CLIENT_NETWORK_INSPECTOR=false$/m);
});

test("Expo.plist disables OTA updates for the embedded-only reader build", () => {
  const source = fs.readFileSync(
    path.join(APP_ROOT, "ios", "Mixterious", "Supporting", "Expo.plist"),
    "utf8",
  );

  assert.match(source, /<key>EXUpdatesEnabled<\/key>\s*<false\/>/);
  assert.match(source, /<key>EXUpdatesCheckOnLaunch<\/key>\s*<string>NEVER<\/string>/);
  assert.doesNotMatch(source, /<key>EXUpdatesURL<\/key>/);
});

test("Info.plist disables new architecture and background audio mode", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "ios", "Mixterious", "Info.plist"), "utf8");

  assert.match(source, /<key>RCTNewArchEnabled<\/key>\s*<false\/>/);
  assert.doesNotMatch(source, /<key>UIBackgroundModes<\/key>/);
});
