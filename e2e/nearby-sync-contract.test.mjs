import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// The native UI is now a thin react-native-webview shell (PdfReaderApp.tsx); the old
// FlatList reader and its reconnect-overlay / AppState / memory-tuning / onboarding
// machinery are gone. What remains — and what this file pins — is the Multipeer offline
// sync WIRE CONTRACT across the JS wrapper, its .d.ts, the ObjC bridge, and the Swift
// engine. Those layers are KEPT (and extended), so these assertions still guard real
// failure modes. Every assertion that read PdfReaderApp.tsx for removed reader UI was
// deleted (dead-behavior); restore from git history if that UI ever returns.

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
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

test("JS can force-request a current snapshot from the director", () => {
  assert.match(jsSyncSource, /requestCurrentSnapshot/);
  assert.match(dtsSyncSource, /requestCurrentSnapshot/);
  assert.match(bridgeSource, /requestCurrentSnapshot/);
  assert.match(swiftSource, /func requestCurrentSnapshot/);
  // Implementation must send a follower hello (director responds with snapshot).
  assert.match(swiftSource, /forceFollowerHelloNow/);
  assert.match(swiftSource, /"type": "hello"/);
});

test("unsupported-platform sync entrypoints reject through promises instead of throwing synchronously", () => {
  assert.match(jsSyncSource, /return Promise\.reject\(new Error\("La sincronización offline solo está disponible en iPad\."\)\);/);
  assert.doesNotMatch(jsSyncSource, /throw new Error\("La sincronización offline solo está disponible en iPad\."\);/);
});

test("happy path: director immediately snapshots on connect and on hello", () => {
  assert.match(swiftSource, /case \.connected:[\s\S]*currentRole == "director"[\s\S]*sendCurrentPageSnapshot/);
  assert.match(swiftSource, /if type == "hello"[\s\S]*currentRole == "director"[\s\S]*sendCurrentPageSnapshot/);
});

test("happy path: takeover approved/denied messages are handled only by follower", () => {
  assert.match(swiftSource, /if type == "takeover_approved"[\s\S]*currentRole == "follower"/);
  assert.match(swiftSource, /if type == "takeover_denied"[\s\S]*currentRole == "follower"/);
});

test("happy path: sendPageUpdate includes protocol version and mode/bookId fields", () => {
  assert.match(swiftSource, /"v": Self\.protocolVersion/);
  assert.match(swiftSource, /"mode": mode/);
  assert.match(swiftSource, /"bookId": bookId/);
});

test("happy path: discovery refresh cadence uses early burst then steady refresh", () => {
  assert.match(swiftSource, /earlyRefreshCycleCount/);
  assert.match(swiftSource, /earlyRefreshInterval/);
  assert.match(swiftSource, /discoveryRefreshInterval/);
});

test("soft app reset clears native sync transport and guards stale callbacks", () => {
  assert.match(jsSyncSource, /resetNearbyDirectorSync/);
  assert.match(jsSyncSource, /nativeModule\.resetForAppReset/);
  assert.match(dtsSyncSource, /resetNearbyDirectorSync/);
  assert.match(bridgeSource, /resetForAppReset/);
  assert.match(swiftSource, /resetGeneration = UUID\(\)/);
  assert.match(swiftSource, /guard self\.mcSessions\.contains\(where: \{ \$0 === session \}\) else \{ return \}/);
});

// Root cause of 1-director/10-follower failures: both sides calling invitePeer simultaneously
// created duplicate MCSession objects per pair. Fix: director never invites — followers are
// the sole inviters; director only accepts via advertiser delegate.
test("director does not eagerly call invitePeer in browser:foundPeer (legacy fallback allowed)", () => {
  const browserFoundPeerBlock = swiftSource.match(
    /func browser\(_ browser: MCNearbyServiceBrowser, foundPeer[\s\S]*?(?=\n  func )/
  )?.[0] ?? "";
  assert.ok(browserFoundPeerBlock.length > 0, "browser:foundPeer delegate must exist");
  // Directors may keep an immediate fallback invite for legacy followers (build ≤226) that wait
  // to be invited. Modern followers self-invite, so the director must not invite them here.
  assert.match(browserFoundPeerBlock, /Modern follower: it will self-invite us/);
  assert.match(browserFoundPeerBlock, /guard !self\.allConnectedPeers\.contains\(peerID\) else \{ return \}/);
  assert.match(browserFoundPeerBlock, /\.invitePeer\(/);
});

test("follower invite ownership lives in reconsiderFollowerTarget", () => {
  const reconsiderBlock = swiftSource.match(
    /private func reconsiderFollowerTarget\(\)[\s\S]*?(?=\n  private func )/
  )?.[0] ?? "";
  assert.ok(reconsiderBlock.length > 0, "reconsiderFollowerTarget must exist");
  assert.match(reconsiderBlock, /browser\?\.invitePeer\(capturedTarget, to: capturedSession/);
  assert.match(reconsiderBlock, /Modern director: we initiate/);
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
  assert.match(swiftSource, /if let pending = pendingInvitePeer/);
  assert.match(swiftSource, /if pending == target/);
  assert.match(swiftSource, /if discoveredDirectors\[pending\] != nil/);
});

test("late joiners receive immediate snapshots from the director", () => {
  assert.match(swiftSource, /sendCurrentPageSnapshot\(to: peerID, via: session\)/);
  assert.match(swiftSource, /if type == "hello"[\s\S]*sendCurrentPageSnapshot\(to: peerID, via: session\)/);
});

// Belt-and-suspenders: MPC can drop the first reliable send right at .connected, so the
// director's proactive snapshot AND the follower's first hello can both vanish. A one-shot
// ~1.5s probe re-requests the snapshot if no page has arrived, so a joining/reconnecting
// follower snaps to the director's current page fast instead of waiting a full hello tick (8s).
test("late joiner: follower schedules a one-shot snapshot-recovery probe on connect", () => {
  assert.match(swiftSource, /followerSnapshotProbeDelay/);
  assert.match(swiftSource, /private func scheduleFollowerSnapshotProbe\(\)/);
  const probeBlock = swiftSource.match(/private func scheduleFollowerSnapshotProbe\(\)[\s\S]*?\n  \}/)?.[0] ?? "";
  assert.ok(probeBlock.length > 0, "scheduleFollowerSnapshotProbe must exist");
  // Must be generation-guarded (a reset cancels it) and gated on "no page received yet".
  assert.match(probeBlock, /self\.resetGeneration == generation/);
  assert.match(probeBlock, /lastFollowerPageReceivedAt == 0/);
  assert.match(probeBlock, /forceFollowerHelloNow\(\)/);
  // Must be invoked on the .connected follower path, after the hello timer starts.
  const connectedBlock = swiftSource.match(
    /case \.connected:[\s\S]*?self\.startFollowerHelloTimer\(\)[\s\S]*?scheduleFollowerSnapshotProbe\(\)/
  )?.[0] ?? "";
  assert.ok(connectedBlock.length > 0, "probe must be scheduled on .connected after startFollowerHelloTimer");
});

test("discovery cadence keeps early burst then steady 25-second refreshes", () => {
  assert.match(swiftSource, /private static let discoveryRefreshInterval: TimeInterval = 25/);
  assert.match(swiftSource, /private static let earlyRefreshInterval: TimeInterval = 5/);
  assert.match(swiftSource, /private static let earlyRefreshCycleCount = 6/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining = Self\.earlyRefreshCycleCount/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining > 0/);
});

test("timers are generation-guarded so stale callbacks cannot survive reset", () => {
  assert.match(swiftSource, /let generation = resetGeneration/);
  assert.match(swiftSource, /self\.resetGeneration == generation/);
  assert.match(swiftSource, /DispatchQueue\.main\.asyncAfter\(deadline: \.now\(\) \+ Self\.followerRetryDelay\)[\s\S]*self\.resetGeneration == generation/);
});

test("director can accept up to 10 followers via multi-session allocation", () => {
  assert.match(swiftSource, /private static let maxFollowersPerSession = 7/);
  assert.match(swiftSource, /private static let maxSessions = 2/);
  assert.match(swiftSource, /availableSessionForNewFollower\(\)/);
});

test("FOLLOWER_START_FAILED: browser failure emits error to JS so the follower can recover", () => {
  assert.match(swiftSource, /FOLLOWER_START_FAILED/);
  // Must guard against the priming browser — only the real browser emits.
  assert.match(swiftSource, /browser === self\.browser.*currentRole == "follower"[\s\S]{0,60}FOLLOWER_START_FAILED/);
});

test("native emits memory-warning event to JS on iOS memory pressure", () => {
  assert.match(swiftSource, /UIApplication\.didReceiveMemoryWarningNotification/);
  assert.match(swiftSource, /"type": "memoryWarning"/);
});

test("follower pauses discovery refresh timer when connected to director", () => {
  assert.match(swiftSource, /pauseDiscoveryRefreshWhileConnected\(\)/);
  assert.match(swiftSource, /private func pauseDiscoveryRefreshWhileConnected\(\)/);
  // Pause must be called on the .connected path before startFollowerHelloTimer
  const connectedBlock = swiftSource.match(
    /case \.connected:[\s\S]*?self\.startFollowerHelloTimer\(\)/
  )?.[0] ?? "";
  assert.ok(connectedBlock.includes("pauseDiscoveryRefreshWhileConnected"), "pause must be called on connect");
});

test("follower resumes discovery fast-burst on disconnect so late director is found quickly", () => {
  assert.match(swiftSource, /resumeDiscoveryRefreshAfterDisconnect\(\)/);
  assert.match(swiftSource, /private func resumeDiscoveryRefreshAfterDisconnect\(\)/);
  // Resume must fire before startSelfDirectedTimer on .notConnected path
  const disconnBlock = swiftSource.match(
    /case \.notConnected:[\s\S]*?self\.startSelfDirectedTimer\(\)/
  )?.[0] ?? "";
  assert.ok(disconnBlock.includes("resumeDiscoveryRefreshAfterDisconnect"), "resume must be called on disconnect");
});

test("native state emissions are deduplicated to cut JS bridge churn", () => {
  assert.match(swiftSource, /lastEmittedStatus/);
  assert.match(swiftSource, /lastEmittedPeerCount/);
  // Dedup guard must appear inside emitState, not just as a property declaration
  const emitFn = swiftSource.match(
    /private func emitState\(status: String[\s\S]*?\n  \}/
  )?.[0] ?? "";
  assert.ok(emitFn.includes("lastEmittedStatus"), "dedup guard must be inside emitState");
});

test("refreshDiscovery wraps MPC object churn in autoreleasepool", () => {
  const refreshFn = swiftSource.match(
    /private func refreshDiscovery\(\)[\s\S]*?\n  \}/
  )?.[0] ?? "";
  assert.ok(refreshFn.includes("autoreleasepool"), "refreshDiscovery must use autoreleasepool");
});

test("edge case: protocolVersion mismatch is ignored (v != 0 and != protocolVersion)", () => {
  assert.match(swiftSource, /if v != 0, v != Self\.protocolVersion \{ return \}/);
});

test("edge case: parseInboundPayload gracefully rejects invalid JSON", () => {
  assert.match(swiftSource, /private func parseInboundPayload/);
  assert.match(swiftSource, /try\?\s*JSONSerialization\.jsonObject/);
  assert.match(swiftSource, /return nil/);
});

test("edge case: followerHello is throttled when pages are being received", () => {
  assert.match(swiftSource, /lastFollowerPageReceivedAt/);
  assert.match(swiftSource, /now - lastFollowerPageReceivedAt < Self\.followerHelloInterval \* 2/);
});

test("edge case: followerHello is throttled when a hello was sent recently", () => {
  assert.match(swiftSource, /lastFollowerHelloAt/);
  assert.match(swiftSource, /now - lastFollowerHelloAt < Self\.followerHelloInterval/);
});

test("edge case: self-directed timer only fires when follower has no connectedDirectorPeer and no pendingInvitePeer", () => {
  const timerBlock = swiftSource.match(/private func startSelfDirectedTimer\(\)[\s\S]*?\}\n\s*\}\n\s*\}\n/)?.[0] ?? "";
  assert.ok(timerBlock.length > 0, "startSelfDirectedTimer must exist");
  assert.match(timerBlock, /currentRole == "follower"/);
  assert.match(timerBlock, /connectedDirectorPeer == nil/);
  assert.match(timerBlock, /pendingInvitePeer == nil/);
});

test("edge case: follower disconnect schedules a retry after followerRetryDelay", () => {
  assert.match(swiftSource, /followerRetryDelay/);
  assert.match(swiftSource, /DispatchQueue\.main\.asyncAfter\(deadline: \.now\(\) \+ Self\.followerRetryDelay\)/);
  assert.match(swiftSource, /reconsiderFollowerTarget\(\)/);
});

test("edge case: requestCurrentSnapshot is exposed on the ObjC bridge", () => {
  assert.match(bridgeSource, /requestCurrentSnapshot/);
});

test("edge case: requestCurrentSnapshot only sends when follower is connected to a director", () => {
  assert.match(swiftSource, /private func forceFollowerHelloNow\(\)/);
  assert.match(swiftSource, /connectedDirectorPeer/);
  assert.match(swiftSource, /session\.connectedPeers\.contains/);
});

test("edge case: director sendCurrentPageSnapshot uses reliable delivery", () => {
  assert.match(swiftSource, /private func sendCurrentPageSnapshot/);
  assert.match(swiftSource, /with: \.reliable/);
  assert.match(swiftSource, /with: \.unreliable/);
});

// ── The peer bundle-push rail is RETIRED (plan §5.12 / Q4, red team A5) ──────
//
// This rail streamed a director's own WebBundle to a follower over Multipeer. It is the ONLY
// writer of Documents/WebBundle and therefore the sole source of the D1 stale-bundle trap, and it
// compares CFBundleVersion — the SHELL's number, which says nothing about which book either device
// holds — so it can push a songbook BACKWARDS.
//
// These pin the guards at their SOURCE, because a disabled feature with no test is a feature that
// quietly comes back. The receive side is what matters: a peer running an older build still offers
// and still sends, so guarding only the send side would leave the rail wide open.
const appSource = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

test("mesh bundle push is disabled by a build-baked constant, not a remote flag", () => {
  assert.match(swiftSource, /private static let meshBundlePushEnabled = false/);
});

test("GUARD 1/4 — handleBundleOffer refuses before any version comparison", () => {
  const fn = swiftSource.slice(swiftSource.indexOf("private func handleBundleOffer"));
  const guardIdx = fn.indexOf("guard Self.meshBundlePushEnabled else { return }");
  const roleIdx = fn.indexOf('guard currentRole == "follower"');
  assert.ok(guardIdx > -1, "handleBundleOffer is unguarded");
  assert.ok(guardIdx < roleIdx, "the kill switch must come FIRST, before any other predicate");
});

test("GUARD 2/4 — handleBundleRequest refuses to serve a bundle", () => {
  const fn = swiftSource.slice(swiftSource.indexOf("private func handleBundleRequest"));
  assert.ok(fn.indexOf("guard Self.meshBundlePushEnabled else { return }") > -1);
});

test("GUARD 3/4 — didFinishReceivingResource drops the transfer AND deletes the temp file", () => {
  // The site with no role guard at all, which never inspected resourceName: anything a peer chose
  // to send landed here and went straight to installReceivedBundle.
  const fn = swiftSource.slice(swiftSource.indexOf("didFinishReceivingResourceWithName"));
  const guardIdx = fn.indexOf("guard Self.meshBundlePushEnabled else {");
  assert.ok(guardIdx > -1, "the receive boundary is unguarded");
  const body = fn.slice(guardIdx, guardIdx + 400);
  assert.match(body, /bundleTransferInFlight = false/, "must clear the in-flight flag or transfers wedge forever");
  assert.match(body, /removeItem\(at: localURL\)/, "must not leave ~27 MB of temp file behind");
  // Match the CALL, not the identifier — the surrounding comment mentions the function by name.
  const callIdx = fn.search(/\binstallReceivedBundle\(at:/);
  assert.ok(callIdx > -1, "expected an installReceivedBundle call site in this delegate");
  assert.ok(guardIdx < callIdx, "the guard must precede the install call");
});

test("GUARD 4/4 — installReceivedBundle itself refuses, as defence in depth", () => {
  const fn = swiftSource.slice(swiftSource.indexOf("private func installReceivedBundle"));
  const guardIdx = fn.indexOf("guard Self.meshBundlePushEnabled else {");
  assert.ok(guardIdx > -1, "the last line before Documents/WebBundle is unguarded");
  // Must precede every filesystem write in the function.
  const firstWrite = Math.min(
    ...["createDirectory", "moveItem", "copyItem", "FileHandle"]
      .map((s) => fn.indexOf(s))
      .filter((i) => i > -1),
  );
  assert.ok(guardIdx < firstWrite, "the guard must come before any filesystem mutation");
});

test("the JS bundleUpdated handler no longer auto-remounts the WebView", () => {
  // It used to re-resolve and remount ON THE SPOT with no human gate and no timing check — a peer
  // arriving mid-Mass could swap the songbook out from under a singer mid-verse.
  const handler = appSource.slice(appSource.indexOf('case "bundleUpdated"'), appSource.indexOf('case "bundleUpdated"') + 900);
  assert.doesNotMatch(handler, /setMountKey/, "bundleUpdated must not remount");
  assert.doesNotMatch(handler, /setBundleUri/, "bundleUpdated must not swap the bundle");
  assert.match(handler, /mesh-bundleUpdated-ignored/, "it should leave a breadcrumb that it was ignored");
});
