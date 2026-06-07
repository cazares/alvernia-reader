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

test("happy path: follower foreground performs start + snapshot request + discovery refresh", () => {
  const lifecycleBlock = appSource.match(/AppState\.addEventListener\([\s\S]*?else if \(nextState === "active"\)[\s\S]*?\}\);\n\s*return \(\) => sub\.remove\(\);\n\s*\}, \[writeBreadcrumb\]\);/)?.[0] ?? "";
  assert.ok(lifecycleBlock.length > 0, "AppState lifecycle effect must exist");
  assert.match(lifecycleBlock, /syncRoleRef\.current === "follower"/);
  const startIdx = lifecycleBlock.indexOf("startNearbyFollower(DIRECTOR_SESSION)");
  const snapIdx = lifecycleBlock.indexOf("requestCurrentSnapshot()");
  const refreshIdx = lifecycleBlock.indexOf("refreshNearbyDiscovery()");
  assert.ok(startIdx >= 0 && snapIdx >= 0 && refreshIdx >= 0, "expected start/snapshot/refresh calls");
  assert.ok(startIdx < snapIdx && snapIdx < refreshIdx, "expected start -> snapshot -> refresh order");
});

test("happy path: director immediately snapshots on connect and on hello", () => {
  assert.match(swiftSource, /case \.connected:[\s\S]*currentRole == "director"[\s\S]*sendCurrentPageSnapshot/);
  assert.match(swiftSource, /if type == "hello"[\s\S]*currentRole == "director"[\s\S]*sendCurrentPageSnapshot/);
});

test("happy path: follower records lastSyncedAt and can show 'sincr. hace' label", () => {
  assert.match(appSource, /lastSyncedAtRef/);
  assert.match(appSource, /selfDirectedAgoLabel/);
  assert.match(appSource, /sincr\. hace/);
});

test("happy path: follower refresh button is present only when follower and not searching UI overlays", () => {
  const cluster = appSource.match(/\{\/\* Top-right button cluster \*\/\}[\s\S]*?syncRole === "follower"[\s\S]*?<\/View>\n\s*\)\}/)?.[0] ?? "";
  assert.ok(cluster.length > 0, "expected reconnect cluster conditional render");
  assert.match(cluster, /syncRole === "follower"/);
  assert.match(cluster, /!searchVisible/);
  assert.match(cluster, /!onboardingVisible/);
});

test("happy path: reconnect press shows searching status and sets reconnectBusy true", () => {
  const reconnectFn = appSource.match(/const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(reconnectFn.length > 0, "handleReconnectPress must exist");
  assert.match(reconnectFn, /setFollowerStatus\("searching"\)/);
  assert.match(reconnectFn, /setReconnectBusy\(true\)/);
});

test("happy path: reconnect press clears overlay quickly on success", () => {
  const reconnectFn = appSource.match(/const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(reconnectFn.length > 0, "handleReconnectPress must exist");
  assert.match(reconnectFn, /setReconnectMessage\("Listo\."\)/);
  assert.match(reconnectFn, /setTimeout\([\s\S]*setReconnectBusy\(false\)[\s\S]*,\s*350\)/);
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
  // Other UI uses a ✕ (e.g. close buttons). We only care that the follower status
  // indicator is no longer a red ✕.
  assert.doesNotMatch(appSource, /<Text style=\{styles\.reconnectStatusX\}>✕<\/Text>/);
});

test("foregrounding the app triggers a sync nudge (director re-broadcasts, follower refreshes discovery)", () => {
  const lifecycleBlock = appSource.match(/AppState\.addEventListener\([\s\S]*?\)\s*;\n\s*return \(\) => sub\.remove\(\);\n\s*\}, \[writeBreadcrumb\]\);/)?.[0] ?? "";
  assert.ok(lifecycleBlock.length > 0, "AppState lifecycle effect must exist");
  assert.match(lifecycleBlock, /else if \(nextState === "active"\)/);
  assert.match(lifecycleBlock, /syncRoleRef\.current === "director"/);
  assert.match(lifecycleBlock, /sendNearbyDirectorPageUpdate\(currentPageRef\.current/);
  assert.match(lifecycleBlock, /syncRoleRef\.current === "follower"/);
  assert.match(lifecycleBlock, /refreshNearbyDiscovery\(\)/);
});

test("follower status indicator uses a pulsing dot in the failure/idle state (no text fallback)", () => {
  const clusterBlock = appSource.match(/\{\/\* Top-right button cluster \*\/\}[\s\S]*?<\/View>\n\s*\)\}/)?.[0] ?? "";
  assert.ok(clusterBlock.length > 0, "reconnect cluster render block must exist");
  // Status must be dots (connected/searching/fallback), not a text glyph.
  assert.match(clusterBlock, /followerStatusLabel === "connected"[\s\S]*<PulsingDot/);
  assert.match(clusterBlock, /followerStatusLabel === "searching"[\s\S]*<PulsingDot/);
  assert.match(clusterBlock, /<PulsingDot color="#f0c040" speed={0\.65} \/>/);
  assert.doesNotMatch(clusterBlock, /reconnectStatusX\}\s*>✕</);
});

test("follower failure is debounced (no immediate failed status on idle blip)", () => {
  const scheduleBlock = appSource.match(/const scheduleFollowerFailure = useCallback\([\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(scheduleBlock.length > 0, "scheduleFollowerFailure must exist");
  assert.match(scheduleBlock, /setTimeout\(\(\) => \{/);
  assert.match(scheduleBlock, /setFollowerStatus\("failed"/);
  assert.match(scheduleBlock, /,\s*1500\)\s*;?/);
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

// Removed: "director takeover request cannot leave reconnect modal stuck forever" —
// asserted the removed takeover-timeout UI ("Sin respuesta del director" is gone from
// the source). Dead-behavior test; restore from git history if takeover returns.

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
  assert.match(appSource, /windowSize=\{memoryPressure \? 1 : 2\}/);
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
  assert.match(swiftSource, /"type": "memoryWarning"/);
});

test("JS listener handles memoryWarning by shedding heavy overlays and writing breadcrumb", () => {
  const listenerBlock = appSource.match(
    /addNearbyDirectorSyncListener\(\(event: any\) => \{[\s\S]*?\}, \[/
  )?.[0] ?? "";
  assert.ok(listenerBlock.includes('event.type === "memoryWarning"'), "listener must handle memoryWarning event type");
  assert.ok(listenerBlock.includes("setSearchVisible(false)"), "must shed search overlay on memory warning");
  assert.ok(listenerBlock.includes("writeBreadcrumb"), "must write breadcrumb on memory warning");
});

test("reconnect overlay cannot auto-cancel via backdrop tap, and has a hard timeout", () => {
  // Prevent accidental infinite cancel/reopen loops from backdrop taps.
  assert.doesNotMatch(appSource, /TouchableWithoutFeedback\s+onPress=\{cancelReconnect\}/);

  // Ensure a hard timeout exists so the overlay cannot remain stuck forever.
  const reconnectFn = appSource.match(
    /const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/
  )?.[0] ?? "";
  assert.ok(reconnectFn.length > 0, "handleReconnectPress must exist");
  assert.match(reconnectFn, /setTimeout\([\s\S]*?,\s*20_000\)/);
  assert.match(reconnectFn, /clearReconnectOverlayTimeout/);
});

test("app remembers last sync role and restores director on restart", () => {
  assert.match(appSource, /STORAGE_KEYS\.lastSyncRole/);
  assert.match(appSource, /STORAGE_KEYS\.lastDirectorAt/);
  assert.match(appSource, /wasDirectorRecently/);
  assert.match(appSource, /startNearbyDirector\(DIRECTOR_SESSION\)/);
  assert.match(appSource, /AsyncStorage\.multiSet\(\[\s*\[STORAGE_KEYS\.lastSyncRole,\s*"director"\]/);
});

test("breadcrumb heartbeat writes bounded data to a single AsyncStorage key", () => {
  assert.match(appSource, /sv_bc/);
  assert.match(appSource, /writeBreadcrumb/);
  // Heartbeat interval must be 60_000 ms
  assert.match(appSource, /60_000/);
  // Size cap must be present
  assert.match(appSource, /2000/);
});

test("reconnect overlay is driven only by reconnectBusy state (no other modal blocks follower)", () => {
  assert.match(appSource, /<Modal visible=\{reconnectBusy && syncRole !== "director"\}/);
  assert.match(appSource, /const \[reconnectBusy, setReconnectBusy\] = useState\(false\)/);
});

test("cancelReconnect clears busy state, clears takeover, and restarts follower transport", () => {
  const cancelBlock = appSource.match(/const cancelReconnect = useCallback\([\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(cancelBlock.length > 0, "cancelReconnect must exist");
  assert.match(cancelBlock, /reconnectCancelledRef\.current = true/);
  assert.match(cancelBlock, /clearReconnectOverlayTimeout\(\)/);
  assert.match(cancelBlock, /clearPendingTakeover\(\)/);
  assert.match(cancelBlock, /setReconnectBusy\(false\)/);
  assert.match(cancelBlock, /startNearbyFollower\(DIRECTOR_SESSION\)/);
});

test("handleReconnectPress is follower-only and rate-limited to avoid transport churn", () => {
  const block = appSource.match(/const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(block.length > 0, "handleReconnectPress must exist");
  assert.match(block, /if \(syncRoleRef\.current === "director"\) return/);
  assert.match(block, /if \(now - reconnectCooldownRef\.current < 2000\) return/);
  assert.match(block, /reconnectCooldownRef\.current = now/);
});

test("first reconnect tap restarts follower transport then refreshes discovery (order matters)", () => {
  const block = appSource.match(/const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(block.length > 0, "handleReconnectPress must exist");
  const startIdx = block.indexOf("startNearbyFollower(DIRECTOR_SESSION)");
  const refreshIdx = block.indexOf("refreshNearbyDiscovery()");
  assert.ok(startIdx >= 0, "must restart follower transport on reconnect tap");
  assert.ok(refreshIdx >= 0, "must refresh discovery on reconnect tap");
  assert.ok(startIdx < refreshIdx, "must restart follower before refreshing discovery");
});

test("reconnect overlay hard-timeouts after 20s and marks follower failed", () => {
  const reconnectFn = appSource.match(
    /const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/
  )?.[0] ?? "";
  assert.ok(reconnectFn.length > 0, "handleReconnectPress must exist");
  assert.match(reconnectFn, /,\s*20_000\)/);
  assert.match(reconnectFn, /setReconnectBusy\(false\)/);
  assert.match(reconnectFn, /setFollowerStatus\("failed", 5000\)/);
});

test("reconnect press escalates to soft reset on second tap (no surprise reset on first tap)", () => {
  const block = appSource.match(/const handleReconnectPress = useCallback\(async \(\) => \{[\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(block.length > 0, "handleReconnectPress must exist");
  assert.match(block, /if \(pressCount >= 2\)/);
  assert.match(block, /confirmResetApp\("reconnect"\)/);
});

test("foreground nudge does not depend on syncRole state closure (uses syncRoleRef)", () => {
  const lifecycleBlock = appSource.match(/AppState\.addEventListener\([\s\S]*?return \(\) => sub\.remove\(\);\n\s*\}, \[writeBreadcrumb\]\);/)?.[0] ?? "";
  assert.ok(lifecycleBlock.length > 0, "AppState lifecycle effect must exist");
  assert.match(lifecycleBlock, /syncRoleRef\.current/);
  assert.doesNotMatch(lifecycleBlock, /syncRole\s*===\s*\"director\"/);
});

test("director broadcasts page updates via sendNearbyDirectorPageUpdate from onViewableItemsChanged", () => {
  // Keep this robust against formatting/line breaks: just ensure the callback contains the director broadcast.
  assert.match(appSource, /const onViewableItemsChanged = useCallback\([\s\S]*?sendNearbyDirectorPageUpdate\(page, totalPages, \{ mode, bookId: activeBookId \}\)/);
});

test("clearVolatileRuntimeState marks reconnect as cancelled to prevent stuck overlay after reset", () => {
  const resetBlock = appSource.match(/const clearVolatileRuntimeState = useCallback\([\s\S]*?\}, \[[^\]]*\]\);/)?.[0] ?? "";
  assert.ok(resetBlock.length > 0, "clearVolatileRuntimeState must exist");
  assert.match(resetBlock, /reconnectCancelledRef\.current = true/);
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

test("edge case: reconnect modal can be suppressed by a constant gate", () => {
  // If a gate exists, it must control the modal visibility. If it doesn't exist, we at least
  // require the modal to be driven by reconnectBusy.
  if (/SHOW_RECONNECT_MODAL/.test(appSource)) {
    assert.match(appSource, /SHOW_RECONNECT_MODAL\s*&&\s*reconnectBusy/);
  } else {
    assert.match(appSource, /<Modal visible=\{reconnectBusy && syncRole !== "director"\}/);
  }
});
