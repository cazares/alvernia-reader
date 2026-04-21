import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const appSource = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
const jsSyncSource = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.js"), "utf8");
const dtsSyncSource = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.d.ts"), "utf8");
const swiftSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
const bridgeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModuleBridge.m"), "utf8");

test("nearby sync page updates include mode and book identity", () => {
  assert.match(jsSyncSource, /sendPageUpdate\(\s*page,\s*totalPages,\s*String\(context\.mode/);
  assert.match(jsSyncSource, /String\(context\.bookId/);
  assert.match(dtsSyncSource, /context\?: \{ mode\?: "standard" \| "nonStandard"; bookId\?: string \}/);
  assert.match(bridgeSource, /mode:\(NSString \*\)mode/);
  assert.match(bridgeSource, /bookId:\(NSString \*\)bookId/);
  assert.match(swiftSource, /"v": Self\.protocolVersion/);
  assert.match(swiftSource, /"mode": mode/);
  assert.match(swiftSource, /"bookId": bookId/);
});

test("followers switch to the director book before applying synced pages", () => {
  assert.match(appSource, /pendingSyncPageRef/);
  assert.match(appSource, /event\.mode === "standard" \|\| event\.mode === "nonStandard"/);
  assert.match(appSource, /NON_STANDARD_BOOK_IDS\.includes\(event\.bookId\)/);
  assert.match(appSource, /setMode\(incomingMode\)/);
  assert.match(appSource, /setActiveBookId\(incomingBookId\)/);
  assert.match(appSource, /sendNearbyDirectorPageUpdate\(page, totalPages, \{ mode, bookId: activeBookId \}\)/);
});
