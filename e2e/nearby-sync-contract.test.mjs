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

// Root cause of 1-director/10-follower failures: both sides calling invitePeer simultaneously
// created duplicate MCSession objects per pair. Fix: director never invites — followers are
// the sole inviters; director only accepts via advertiser delegate.
test("director does not call invitePeer in browser:foundPeer — followers are sole inviters", () => {
  const browserFoundPeerBlock = swiftSource.match(
    /func browser\(_ browser: MCNearbyServiceBrowser, foundPeer[\s\S]*?(?=\n  func )/
  )?.[0] ?? "";
  assert.ok(browserFoundPeerBlock.length > 0, "browser:foundPeer delegate must exist");
  // Check for actual method call (not comments that mention invitePeer)
  assert.doesNotMatch(browserFoundPeerBlock, /\.invitePeer\(/, "director must not call .invitePeer() in foundPeer");
});

// Peer display names longer than 63 chars cause an ObjC exception in MCPeerID init.
test("Swift caps peer display name to 50 chars before creating MCPeerID", () => {
  assert.match(swiftSource, /prefix\(50\)/);
});

// sendPageUpdate must store the new page state BEFORE the early-return guard that checks
// for connected peers — otherwise late-joining followers get a nil snapshot.
test("Swift stores page state before the empty-peers guard in sendPageUpdate", () => {
  const sendPageUpdateBlock = swiftSource.match(
    /func sendPageUpdate[\s\S]*?(?=\n  func |\n  \/\/ MARK)/
  )?.[0] ?? "";
  assert.ok(sendPageUpdateBlock.length > 0, "sendPageUpdate must exist");
  const storeIdx = sendPageUpdateBlock.search(/currentPageNumber\s*=/);
  const guardIdx = sendPageUpdateBlock.search(/guard\s*!connected\.isEmpty/);
  assert.ok(storeIdx > -1, "must store currentPageNumber in sendPageUpdate");
  assert.ok(guardIdx > -1, "must have guard for empty connected peers");
  assert.ok(storeIdx < guardIdx, "state must be stored BEFORE the empty-peers guard");
});

// Dedup guard prevents multiple in-flight invitations to the same peer (reconsiderFollowerTarget).
test("Swift deduplicates in-flight invitations with pendingInvitePeer guard", () => {
  assert.match(swiftSource, /guard pendingInvitePeer != target else/);
});

// UX Fix B: reset confirmation alert must not stack on top of sync modal — modal must be
// dismissed first so only one surface is on screen at a time.
test("UX Fix B: sync modal dismissed before reset alert is shown", () => {
  const confirmResetBlock = appSource.match(
    /confirmResetApp[\s\S]*?Alert\.alert\(/
  )?.[0] ?? "";
  assert.ok(confirmResetBlock.length > 0, "confirmResetApp must exist and contain Alert.alert");
  const dismissIdx = confirmResetBlock.search(/setSyncModal\(false\)/);
  const alertIdx = confirmResetBlock.search(/Alert\.alert\(/);
  assert.ok(dismissIdx > -1, "setSyncModal(false) must be called before Alert.alert in confirmResetApp");
  assert.ok(dismissIdx < alertIdx, "modal must be dismissed BEFORE Alert.alert");
});

// UX Fix C: permission error must not tell the user to reinstall — force-close is enough.
test("UX Fix C: director permission error uses force-close not reinstall guidance", () => {
  assert.match(appSource, /cierra la app completamente/);
  assert.doesNotMatch(appSource, /desinstala y reinstala/);
});

// UX Fix E: follower status label appears below the reconnect button with colour variants.
test("UX Fix E: follower status label with connected/searching/failed variants is rendered", () => {
  assert.match(appSource, /followerStatusLabel/);
  assert.match(appSource, /followerStatusConnected/);
  assert.match(appSource, /followerStatusSearching/);
  assert.match(appSource, /followerStatusFailed/);
  assert.match(appSource, /Conectado ✓/);
  assert.match(appSource, /Buscando\.\.\./);
  assert.match(appSource, /Sin conexión/);
});

// Follower-first scenario: browser failure (permission denied) must surface to JS so the
// follower is told to check Settings rather than waiting forever on "searching".
test("FOLLOWER_START_FAILED: browser failure emits error to JS and shows Settings alert", () => {
  assert.match(swiftSource, /FOLLOWER_START_FAILED/);
  // Must guard against the priming browser — only the real browser emits.
  assert.match(swiftSource, /browser === self\.browser.*currentRole == "follower"[\s\S]{0,60}FOLLOWER_START_FAILED/);
  assert.match(appSource, /FOLLOWER_START_FAILED/);
  assert.match(appSource, /followerStartFailedAlertShownRef/);
  assert.match(appSource, /No se puede buscar al director/);
});

// Both start-failed refs must be cleared on soft reset so alerts can fire again on next session.
test("start-failed alert refs reset on soft app reset", () => {
  const resetBlock = appSource.match(/clearVolatileRuntimeState[\s\S]*?setFollowerStatusLabel/)?.[0] ?? "";
  assert.match(resetBlock, /directorStartFailedAlertShownRef\.current = false/);
  assert.match(resetBlock, /followerStartFailedAlertShownRef\.current = false/);
});

// UX Fix E: follower status must auto-clear after a timeout via a timer ref — no permanent label.
test("UX Fix E: follower status label uses auto-clear timer via ref", () => {
  assert.match(appSource, /followerStatusTimerRef/);
  assert.match(appSource, /clearTimeout\(followerStatusTimerRef\.current\)/);
});
