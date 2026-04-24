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

test("soft app reset clears native sync transport and guards stale callbacks", () => {
  assert.match(appSource, /Restablecer app/);
  assert.match(appSource, /Esto vuelve a empezar la app y la conexion desde cero\./);
  assert.match(appSource, /Tus cantos, ajustes y contenido no se borran\./);
  assert.match(appSource, /performSoftAppReset/);
  assert.match(appSource, /resetNearbyDirectorSync/);
  assert.match(appSource, /setAppResetKey\(\(v\) => v \+ 1\)/);
  assert.match(appSource, /if \(appResettingRef\.current\) return/);
  assert.match(appSource, /Restableciendo\.\.\./);
  assert.match(appSource, /Listo/);
  assert.match(jsSyncSource, /resetNearbyDirectorSync/);
  assert.match(jsSyncSource, /nativeModule\.resetForAppReset/);
  assert.match(dtsSyncSource, /resetNearbyDirectorSync/);
  assert.match(bridgeSource, /resetForAppReset/);
  assert.match(swiftSource, /resetGeneration = UUID\(\)/);
  assert.match(swiftSource, /guard self\.mcSessions\.contains\(where: \{ \$0 === session \}\) else \{ return \}/);
  assert.doesNotMatch(appSource, /exit\(0\)/);
});
