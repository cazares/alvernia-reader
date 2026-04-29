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
  assert.match(appSource, /latestDirectorSnapshotRef/);
  assert.match(appSource, /event\.mode === "standard" \|\| event\.mode === "nonStandard"/);
  assert.match(appSource, /NON_STANDARD_BOOK_IDS\.includes\(event\.bookId\)/);
  assert.match(appSource, /setMode\(incomingMode\)/);
  assert.match(appSource, /setActiveBookId\(incomingBookId\)/);
  assert.match(appSource, /sendNearbyDirectorPageUpdate\(page, totalPages, \{ mode, bookId: activeBookId \}\)/);
});

test("follower startup replays director snapshot after persisted launch state boots", () => {
  const replayBlock = appSource.match(
    /useEffect\(\(\) => \{[\s\S]*?latestDirectorSnapshotRef\.current[\s\S]*?goToPage\(snapshot\.page\);[\s\S]*?\}, \[activeBookId, booted, goToPage, mode, syncRole\]\);/
  )?.[0] ?? "";
  assert.ok(replayBlock.length > 0, "director snapshot replay effect must exist");
  assert.match(replayBlock, /if \(!booted \|\| syncRole !== "follower"\) return/);
  assert.match(replayBlock, /Date\.now\(\) - snapshot\.receivedAt > 30_000/);
  assert.match(replayBlock, /setMode\(snapshot\.mode\)/);
  assert.match(replayBlock, /setActiveBookId\(snapshot\.bookId\)/);
  assert.match(replayBlock, /pendingSyncPageRef\.current = snapshot\.page/);
});

test("saved page restore cannot override a fresh director startup snapshot", () => {
  const restoreBlock = appSource.match(
    /\/\/ Restore last page when entering a non-standard book[\s\S]*?restore\(\);/
  )?.[0] ?? "";
  assert.ok(restoreBlock.length > 0, "saved-page restore effect must exist");
  assert.match(restoreBlock, /latestDirectorSnapshotRef\.current/);
  assert.match(restoreBlock, /Date\.now\(\) - snapshot\.receivedAt <= 30_000/);
  assert.match(restoreBlock, /return/);
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

test("follower invite ownership lives in reconsiderFollowerTarget", () => {
  const reconsiderBlock = swiftSource.match(
    /private func reconsiderFollowerTarget\(\)[\s\S]*?(?=\n  private func )/
  )?.[0] ?? "";
  assert.ok(reconsiderBlock.length > 0, "reconsiderFollowerTarget must exist");
  assert.match(reconsiderBlock, /browser\?\.invitePeer\(target, to: session/);
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

// UX Fix E: follower status is shown as an inline icon on the refresh button (not text),
// with green/orange pulse and a red X for no connection.
test("UX Fix E: follower refresh shows green/orange pulse and red X status indicator", () => {
  assert.match(appSource, /reconnectStatusX/);
  assert.match(appSource, /PulsingDot color=\"#4cff91\"/);
  assert.match(appSource, /PulsingDot color=\"#f0c040\"/);
  assert.match(appSource, /✕/);
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

test("refresh button restarts follower transport so refresh re-syncs to the director's current page", () => {
  const reconnectBlock = appSource.match(/const handleReconnectPress = useCallback\([\s\S]*?\n\s*\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(reconnectBlock.length > 0, "handleReconnectPress block must exist");
  assert.match(reconnectBlock, /startNearbyFollower\(DIRECTOR_SESSION\)/);
  assert.match(reconnectBlock, /refreshNearbyDiscovery\(\)/);
  assert.ok(
    reconnectBlock.indexOf("startNearbyFollower") < reconnectBlock.indexOf("refreshNearbyDiscovery"),
    "refresh must restart follower transport before refreshing discovery"
  );
});

test("idle/waiting-followers state is debounced before showing the red X", () => {
  assert.match(appSource, /scheduleFollowerFailure/);
  // Avoid immediate red X from brief native idle blips.
  assert.doesNotMatch(
    appSource,
    /event\.status === "waiting-followers"[\s\S]{0,200}setFollowerStatus\("failed"/,
  );
  assert.doesNotMatch(
    appSource,
    /event\.status === "idle"[\s\S]{0,200}setFollowerStatus\("failed"/,
  );
});

test("late joiners receive immediate snapshots from the director", () => {
  assert.match(swiftSource, /sendCurrentPageSnapshot\(to: peerID, via: session\)/);
  assert.match(swiftSource, /if type == "hello"[\s\S]*sendCurrentPageSnapshot\(to: peerID, via: session\)/);
});

test("discovery cadence keeps early burst then steady 25-second refreshes", () => {
  assert.match(swiftSource, /private static let discoveryRefreshInterval: TimeInterval = 25/);
  assert.match(swiftSource, /private static let earlyRefreshInterval: TimeInterval = 5/);
  assert.match(swiftSource, /private static let earlyRefreshCycleCount = 6/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining = Self\.earlyRefreshCycleCount/);
  assert.match(swiftSource, /let interval: TimeInterval = earlyRefreshCyclesRemaining > 0/);
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

test("follower reconnect and bootstrap paths emit explicit searching/failed states", () => {
  assert.match(appSource, /event\.status === "searching" \|\|/);
  assert.match(appSource, /event\.status === "resolving-conflict"/);
  assert.match(appSource, /setFollowerStatus\("failed", 6000\)/);
  assert.match(appSource, /setFollowerStatus\("failed", 5000\)/);
  assert.match(appSource, /setFollowerStatus\("searching"\)/);
});

test("director view does not render the old upper-right tab affordances", () => {
  assert.doesNotMatch(appSource, /searchOverlayTrigger/);
  assert.doesNotMatch(appSource, /navTriggerArrow/);
});

// ── Long-session crash prevention (A1, A2, A3, A4, B1, B2) ──────────────────

test("FlatList render window is minimized to limit decoded image memory", () => {
  assert.match(appSource, /maxToRenderPerBatch=\{1\}/);
  assert.match(appSource, /windowSize=\{2\}/);
  assert.match(appSource, /initialNumToRender=\{1\}/);
});

test("clearVolatileRuntimeState clears followerFailureTimerRef to prevent stale red-X", () => {
  const clearFn = appSource.match(
    /const clearVolatileRuntimeState = useCallback\(\(\) => \{[\s\S]*?\}, \[\]\);/
  )?.[0] ?? "";
  assert.ok(clearFn.length > 0, "clearVolatileRuntimeState must exist");
  assert.match(clearFn, /followerFailureTimerRef\.current/);
  assert.match(clearFn, /clearTimeout\(followerFailureTimerRef\.current\)/);
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

test("native emits memory-warning event to JS on iOS memory pressure", () => {
  assert.match(swiftSource, /UIApplication\.didReceiveMemoryWarningNotification/);
  assert.match(swiftSource, /"type": "memory-warning"/);
});

test("JS listener handles memory-warning by shedding heavy overlays", () => {
  const listenerBlock = appSource.match(
    /addNearbyDirectorSyncListener\(\(event: any\) => \{[\s\S]*?\}, \[/
  )?.[0] ?? "";
  assert.ok(listenerBlock.includes('"memory-warning"'), "listener must handle memory-warning event type");
  assert.ok(listenerBlock.includes("setSearchVisible(false)") || appSource.includes("setSearchVisible(false)"), "must shed search overlay");
});

test("breadcrumb heartbeat writes bounded data to a single AsyncStorage key", () => {
  assert.match(appSource, /sv_bc/);
  assert.match(appSource, /writeBreadcrumb/);
  // Heartbeat interval must be 60_000 ms
  assert.match(appSource, /60_000/);
  // Size cap must be present
  assert.match(appSource, /2000/);
});
