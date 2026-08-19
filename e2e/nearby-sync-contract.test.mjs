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

test("discovery cadence keeps early burst then steady 12-second refreshes", () => {
  // 25 -> 12 and burst 6 -> 12 cycles, build 433. A follower that lost the director waited a FULL
  // refresh interval before looking again — that was the 14 s recovery measured when a
  // backgrounded director returned. Not lower than ~5 s: each refresh tears down and rebuilds both
  // transports, so past that you disrupt discovery more than you perform it.
  assert.match(swiftSource, /private static let discoveryRefreshInterval: TimeInterval = 12/);
  assert.match(swiftSource, /private static let earlyRefreshInterval: TimeInterval = 5/);
  assert.match(swiftSource, /private static let earlyRefreshCycleCount = 12/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining = Self\.earlyRefreshCycleCount/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining > 0/);
});

test("timers are generation-guarded so stale callbacks cannot survive reset", () => {
  assert.match(swiftSource, /let generation = resetGeneration/);
  assert.match(swiftSource, /self\.resetGeneration == generation/);
  assert.match(swiftSource, /DispatchQueue\.main\.asyncAfter\(deadline: \.now\(\) \+ Self\.followerRetryDelay\)[\s\S]*self\.resetGeneration == generation/);
});

test("each follower gets its OWN session — no follower-to-follower cross-connect", () => {
  // Miguel, 2026-08-18: "nuke peer sharing and any peer connections... one director to one
  // follower... that setup times N followers". MCSession connects every member of ONE session to
  // every OTHER member — not a choice this codebase made, Apple's framework behavior — so any
  // session with more than 2 peers lets followers see each other's traffic at the protocol level.
  // A prior measured incident (2026-08-16, comment near "22 follower-to-follower connections")
  // documented this exact cross-connect causing a follower to misidentify ANOTHER FOLLOWER as its
  // director. maxFollowersPerSession=1 makes that structurally impossible: a session with exactly
  // 2 members (director + one follower) has no other follower in it to cross-connect with.
  assert.match(swiftSource, /private static let maxFollowersPerSession = 1/,
    "maxFollowersPerSession is not 1 — followers can still cross-connect within a shared session");
  const m = swiftSource.match(/private static let maxSessions = (\d+)/);
  assert.ok(m, "maxSessions constant missing");
  assert.ok(Number(m[1]) >= 8,
    `maxSessions=${m?.[1]} is too low for real fleet capacity now that each session holds only 1 follower`);
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

test("peer bundle push is GONE — not disabled, not reachable, no receiver", () => {
  // Replaces five tests that pinned the guards on a DISABLED rail. Build 434 deleted the subsystem
  // outright, and the guards went with it. This asserts the stronger property.
  //
  // WHY IT IS A SECURITY TEST, not tidiness. From the project's own audit
  // (docs/audit-findings-raw.md:240): any device in range advertising role=director on the
  // hard-coded public session code could push an arbitrary web bundle onto every follower iPad —
  // no auth, no signature, no consent — into a WebView with originWhitelist ['*'] and
  // allowFileAccess, surviving reboot. It shipped for many builds behind ONE boolean. It was also
  // the only writer of Documents/WebBundle (the directory the boot resolver prefers forever) and
  // could push a book BACKWARDS, since its check compared CFBundleVersion rather than the book.
  //
  // Comments are stripped first: the tombstone doc-block deliberately NAMES what was removed so a
  // future reader understands why, and that explanation must not trip its own test.
  const code = swiftSource
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .split("\n").filter((l) => !l.trim().startsWith("//") && !l.trim().startsWith("///")).join("\n");

  // If any of these names come back, the mechanism came back with them.
  for (const gone of [
    "meshBundlePushEnabled", "bundleTransferInFlight", "bundleTransferGeneration",
    "sendBundleOffer", "handleBundleOffer", "handleBundleRequest",
    "packWebBundle", "installReceivedBundle", "beginBundleTransfer",
    "bundle_offer", "bundle_request",
  ]) {
    assert.ok(!code.includes(gone), `peer bundle push is back: ${gone} reappeared in the mesh module`);
  }

  // The two MCSessionDelegate resource methods must REMAIN (protocol requirements) and must not
  // keep what a peer sends. An empty didStart plus a delete in didFinish is the correct posture:
  // before, a stranger's archive was written to the container before anything rejected it.
  assert.match(swiftSource, /didStartReceivingResourceWithName[\s\S]{0,200}\{\}/,
    "didStartReceivingResource must exist with an EMPTY body");
  const fin = swiftSource.slice(swiftSource.indexOf("didFinishReceivingResourceWithName"));
  assert.match(fin.slice(0, 400), /removeItem\(at: localURL\)/,
    "didFinishReceivingResource must DELETE anything a peer sent");
});

test("the JS bundleUpdated handler no longer auto-remounts the WebView", () => {
  // It used to re-resolve and remount ON THE SPOT with no human gate and no timing check — a peer
  // arriving mid-Mass could swap the songbook out from under a singer mid-verse.
  const handler = appSource.slice(appSource.indexOf('case "bundleUpdated"'), appSource.indexOf('case "bundleUpdated"') + 900);
  assert.doesNotMatch(handler, /setMountKey/, "bundleUpdated must not remount");
  assert.doesNotMatch(handler, /setBundleUri/, "bundleUpdated must not swap the bundle");
  assert.match(handler, /mesh-bundleUpdated-ignored/, "it should leave a breadcrumb that it was ignored");
});

test("BLE contention cooldown outlasts a single sliding-window gap, not just one packet", () => {
  // Live hardware capture (2026-08-18), verified via Xcode Console on a real device mid-repro:
  // nonce af9b/page 99 and nonce 5990/page 50 were BOTH broadcasting; the existing abstain logic
  // correctly suppressed page-apply WHILE both were within the 4s contentionWindow, but the moment
  // af9b's last-seen timestamp aged out of that window, page 50 (also wrong) was trusted and
  // applied on the very next packet. Nothing had proven af9b actually stopped — only that it
  // hadn't been re-heard recently enough. This pins the fix: a SUSTAINED cooldown after
  // contention, not just the first uncontested packet.
  const bleSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  assert.match(bleSource, /private var lastContentionAt: TimeInterval = 0/,
    "lastContentionAt tracking is gone — the cooldown has nothing to measure from");
  assert.match(bleSource, /private static let contentionCooldown: TimeInterval = 4\.0/,
    "contentionCooldown constant is gone or changed unexpectedly");
  assert.match(bleSource, /lastContentionAt = now/,
    "contention detection no longer stamps lastContentionAt — the cooldown would never arm");
  assert.match(bleSource, /now - lastContentionAt < Self\.contentionCooldown/,
    "the cooldown check is gone — a rival can win the instant its sliding-window entry expires");
});

test("BLE page broadcasts are HMAC-bound to the session code — a lone broadcaster must prove it knows the code", () => {
  // Hardened 2026-08-18 after THREE live hardware captures (Xcode Console, real devices) each
  // showed a "ghost" page (nonce+page matching no song any device had actually navigated to) get
  // applied even after: the MCSession one-follower-per-session fix, the contention-cooldown fix,
  // and an explicit force-quit of all 4 devices. A bare nonce proves nothing about WHO sent it —
  // any broadcaster in range that can format "SV<nonce>.<seq>.<page>" was trusted. This closes that
  // gap with zero added latency: the tag is a local HMAC comparison, not a round trip, so BLE keeps
  // its sub-second reaction time while a scanner now refuses any packet whose tag doesn't verify
  // against the session code it was itself given.
  const bleSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  assert.match(bleSource, /var sessionCode: String = ""/,
    "sessionCode is gone — there is nothing left to bind the tag to");
  assert.match(bleSource, /HMAC<SHA256>\.authenticationCode/,
    "the HMAC tag computation is gone — broadcasts are unauthenticated again");
  assert.match(bleSource, /guard parts\.count == 4/,
    "parse() no longer requires the tag field — untagged packets would be accepted");
  assert.match(bleSource, /tag == Self\.authTag\(sessionCode: sessionCode,/,
    "parse() no longer verifies the tag — any well-formed packet would be trusted again");

  const nativeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const setSites = (nativeSource.match(/self\.bleBeacon\.sessionCode = normalizedSessionCode/g) || []).length;
  assert.strictEqual(setSites, 2,
    "expected bleBeacon.sessionCode to be set from BOTH startDirector and startFollower — found " + setSites);
});
