import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// DUMB FOLLOWERS, SIMPLE DIRECTOR — pinned (owner's rule, 2026-08-17).
//
// THE BUG THIS GUARDS. Director sits on song 16 and backgrounds. Followers are walked to other
// songs by hand — iPad 1 to 14, iPad 2 to 19. Director foregrounds, still on 16. Nobody comes
// back. The iPads sit on 14 and 19 indefinitely, reading as followed when they are not.
//
// Two observations made it diagnosable, and both are worth keeping because they are what a future
// regression will look like:
//
//   - the director then turned to 15 and every follower snapped to 15 INSTANTLY. So the mesh was
//     healthy the whole time. Only the re-assertion of a page a follower thought it was already on
//     was being dropped.
//   - the iPhone recovered on its own, while the two healthy iPads did not. Backwards — unless
//     recovery comes from RECONNECTING, because the connect path sends a snapshot down a route the
//     dropped guard did not sit on. The bug rewarded a broken link, which is why the flakiest
//     device looked fine and the good ones stayed wedged.
//
// THE RULE. A follower does not get a vote on whether to obey the director. The native layer must
// forward every page packet to the web; only the web may decide that no work is needed, because
// only the web can see the rendered <img>. A guard built on a REMEMBERED page number will silently
// drift from what is on the glass, and every drift wedges that follower until the director happens
// to move to a different page.
//
// WHY THIS FILE EXISTS RATHER THAN A COMMENT. The de-dupe being removed was added deliberately, to
// fix a real crash (2026-08-06: a follower left on one screen, renderPage doing a load request +
// loading indicator + src assignment + AsyncStorage write sixty times a minute on an eight-year-old
// iPad). That makes it exactly the kind of thing someone re-adds for good reasons. The protection
// against that crash is REAL but it belongs in the web guard, which returns before any of that work
// — so this file pins both halves: no native de-dupe, and the web guard still present.

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const app = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");
const web = fs.readFileSync(path.join(APP_ROOT, "web", "src", "app.js"), "utf8");

// Comments describe intent; only code can wedge a follower. Strip them before the NEGATIVE
// assertion, so the tombstone explaining the removed guard cannot itself satisfy the check.
//
// Stripping is only used where it is needed. Applied to web/src/app.js this same regex removed 80%
// of the file — a runaway block-comment match — which would have made every assertion below pass
// against nothing. Positive assertions therefore read RAW source, and the stripper is sanity-pinned
// below. A test whose instrument silently mangles its input is worse than no test.
const stripComments = (s) =>
  s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/[^\n]*/g, "$1");
const appCode = stripComments(app);

test("the comment stripper does not mangle its input (guards every assertion below)", () => {
  const kept = appCode.length / app.length;
  assert.ok(kept > 0.3, `stripper kept only ${(kept * 100).toFixed(0)}% of PdfReaderApp.tsx`);
  // Landmarks that must survive stripping, or the negative assertion proves nothing.
  assert.match(appCode, /case "page":/);
  assert.match(appCode, /currentPageRef\.current = page/);
});

test("native NEVER drops a mesh page because it matches the remembered page", () => {
  // The exact shape of the removed guard, and any near-relative of it. A follower that returns
  // early here is a follower deciding not to obey the director.
  assert.doesNotMatch(
    appCode,
    /if\s*\(\s*page\s*===\s*currentPageRef\.current[\s\S]{0,80}?\)\s*break\s*;/,
    "native de-dupe is back: a follower whose remembered page drifts from the screen will ignore " +
      "every re-assertion of that page and stay wedged until the director turns elsewhere",
  );
});

test("every mesh page reaches the web layer", () => {
  // The forward itself must survive. Without it there is nothing to render.
  assert.match(
    appCode,
    /injectEvent\(\{\s*type:\s*"sync-event",\s*event:\s*\{\s*type:\s*"page"/,
    "the native 'page' case must forward to the web as a sync-event",
  );
});

test("the web keeps the reality-based guard that makes forwarding cheap", () => {
  // This is what stops the 1 Hz re-render storm behind the 2026-08-06 crash. It is safe to rely on
  // precisely because every clause inspects the DOM rather than a remembered number: same page AND
  // the <img> is showing it AND it finished loading AND it has real pixels.
  assert.match(web, /nextPage === state\.currentPage/);
  assert.match(web, /pageImageMatches\(nextPage\)/);
  assert.match(web, /pageImage\.complete/);
  assert.match(web, /pageImage\.naturalWidth > 0/);
});

test("a director re-arms its heartbeat AND re-asserts its page on foreground", () => {
  // The 1 Hz beat is a JS setInterval, which does not run while iOS holds the process suspended.
  // Every other recovery path re-arms on foreground; the one timer the whole choir depends on did
  // not. Both calls are required: the broadcast makes recovery immediate, the restarted beat keeps
  // re-asserting if that single packet is dropped.
  const foreground = appCode.slice(appCode.indexOf('AppState.addEventListener'));
  const directorBranch = foreground.slice(
    foreground.indexOf('roleRef.current === "director"'),
    foreground.indexOf("explicitTransmitterRef.current)"),
  );
  assert.match(directorBranch, /startDirectorHeartbeat\(\)/, "director must re-arm the heartbeat on foreground");
  assert.match(directorBranch, /broadcastPage\(/, "director must re-assert its page on foreground");
});

test("startDirectorHeartbeat is idempotent, so re-arming cannot leak an interval", () => {
  // Calling it on every foreground is only safe because it clears first.
  const body = appCode.slice(appCode.indexOf("const startDirectorHeartbeat"));
  const firstStatement = body.slice(0, body.indexOf("setInterval"));
  assert.match(firstStatement, /stopDirectorHeartbeat\(\)/);
});

// ── The diagnostics door ───────────────────────────────────────────────────────
//
// The build badge is the ONLY way to open this device's crumb log, and the tap never worked.
// styles.css sets `pointer-events: none` on .build-badge so a label floating over the music can
// never swallow a page turn — right for a label, fatal for the button app.js turns it into in the
// native shell. cursor, role, tabindex and the click listener were all applied to an element that
// could not receive a click, on every device, since it shipped.
//
// Reported 2026-08-17 while trying to read the role off an iPhone that would not sync: there was no
// way to get the answer out of the device. Same class as #342 removing the "¿Algo anda mal?"
// drawer — six devices each holding a written account of what they did, and no door to open it.
test("the build badge can actually be tapped in the native shell", () => {
  const css = fs.readFileSync(path.join(APP_ROOT, "web", "src", "styles.css"), "utf8");
  // The stylesheet rule stays: on signovivo.com the badge must remain a pure, non-blocking label.
  assert.match(css, /\.build-badge\s*\{[^}]*pointer-events:\s*none/s);
  // ...so the shell branch MUST re-enable it, or the listener below it is decoration.
  // Anchor on the badge block itself. "hasNativeBridge() || NATIVE_FILE_MODE" also occurs ~29k
  // chars earlier for an unrelated reason, and anchoring there made this assertion read a window
  // that never contained the badge code at all — it would have failed no matter what was written.
  const shellBranch = web.slice(web.indexOf("const buildBadge = document.getElementById"));
  assert.match(
    shellBranch.slice(0, 6000),
    /buildBadge\.style\.pointerEvents\s*=\s*"auto"/,
    "badge has a click listener but pointer-events:none — the diagnostics viewer is unreachable",
  );
  assert.match(
    shellBranch.slice(0, 6000),
    /buildBadge\.addEventListener\("click"/,
    "the listener itself must survive",
  );
});

// ── The device must be able to speak without a network ─────────────────────────
//
// Until 2026-08-17 DirectorSyncModule.swift produced ZERO local output — no os_log, no NSLog, no
// print. Every breadcrumb went to Cloudflare and nowhere else, which meant the fleet was mute
// whenever the relay was unreachable, and PERMANENTLY mute at Mass, where the followers are on no
// network at all. That is the single most expensive gap in this project's history: the setting the
// app exists for is the one setting in which no follower could ever say what it did, and a full day
// was spent inferring causes from an absence of evidence guaranteed by construction.
//
// This pins the local channel so it cannot quietly disappear again.
test("every mesh breadcrumb is also written to the device's own log", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  assert.match(swift, /^import os$/m, "os must be imported for Logger");
  assert.match(swift, /Logger\(subsystem:\s*"com\.cazares\.signovivo"/,
    "a Logger with a stable subsystem is what makes `log stream --predicate` usable");
  // .notice, NOT .info — os_log's .info is MEMORY-ONLY by default, so it never reaches
  // `log collect` or a sysdiagnose. Build 438 shipped .info and the channel was silent in every
  // collected archive: a real 440 capture converted to ZERO com.cazares.signovivo rows. The whole
  // point is reading a device after the fact, offline, so persistence is the feature.
  assert.match(swift, /deviceLog\.notice\(/, "breadcrumbs must be logged at .notice or they do not persist");
  assert.doesNotMatch(swift, /deviceLog\.info\(/, ".info is memory-only — sysdiagnose and log collect will not see it");
  // .public or the whole log reads <private> — an unreadable log is the problem, not the fix.
  assert.match(swift, /privacy:\s*\.public/);
});

test("the local log is written BEFORE the batching queue, so nothing can silence it", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const body = swift.slice(swift.indexOf("private func dbgLog("));
  const local = body.indexOf("deviceLog.notice(");
  const queued = body.indexOf("logQueue.async");
  assert.ok(local > 0 && queued > 0, "both paths must exist");
  assert.ok(
    local < queued,
    "the device log must be written before the relay queue — the LOG_INTERVAL_MS kill switch " +
      "silences the network, and it must never also blind the device in front of you",
  );
});

// ── Discovery storm: the timer leak behind issue #352 ──────────────────────────
//
// scheduleNextDiscoveryRefresh assigned discoveryRefreshTimer WITHOUT invalidating the previous
// timer. A Timer.scheduledTimer retains itself in the run loop, so overwriting the property loses
// the handle without stopping the timer; the orphan fires and schedules another. Ten call sites
// reach that function, so the live timer population DOUBLED on every overlapping schedule — and the
// biggest doubler was foregrounding, which PdfReaderApp.tsx made twice as bad by calling
// refreshNearbyDiscovery TWICE per foreground.
//
// Measured from the iPhone's own unified log on 2026-08-17: 66 advertiser start/stop events per
// SECOND, sustained, against an intended one every 5-12 s. An MCSession invite cannot complete when
// the advertiser it must answer on lives ~15 ms. That is the "handshake fails, delivery is fine"
// signature in #352 — an issue carrying EIGHT disproved theories, every one about the device rather
// than the timer, and it explains why the iPhone was worst: it is the one picked up all day.
test("scheduleNextDiscoveryRefresh invalidates before it schedules", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const fn = swift.slice(swift.indexOf("private func scheduleNextDiscoveryRefresh"));
  // Anchor on the ASSIGNMENT, not the words "Timer.scheduledTimer" — the explanatory
  // comment above the fix contains that phrase, so searching for it cut the slice before the fix
  // and the assertion failed against code that was correct.
  const body = fn.slice(0, fn.indexOf("discoveryRefreshTimer = Timer.scheduledTimer"));
  assert.match(
    body, /discoveryRefreshTimer\?\.invalidate\(\)/,
    "a new discovery timer must never be scheduled without stopping the old one — that leak is #352",
  );
});

test("discovery churn has a hard floor no caller can bypass", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  assert.match(swift, /minRefreshInterval: TimeInterval = 2\.0/);
  const fn = swift.slice(swift.indexOf("private func refreshDiscovery"));
  assert.match(
    fn.slice(0, 2600), /now - lastRefreshAt >= Self\.minRefreshInterval/,
    "refreshDiscovery must throttle regardless of caller",
  );
});

test("a refresh loop raises an alarm, and counts attempts the throttle drops", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const fn = swift.slice(swift.indexOf("private func refreshDiscovery"));
  const body = fn.slice(0, 2600);
  assert.match(body, /refresh:STORM/, "a loop must announce itself");
  // The alarm must sit BEFORE the throttle's early return, or the guard that contains the loop
  // also hides it — turning a loud bug into a silent one.
  const alarm = body.indexOf("refreshAttemptTimes.append");
  const guardIdx = body.indexOf("now - lastRefreshAt >= Self.minRefreshInterval");
  assert.ok(alarm > 0 && guardIdx > 0 && alarm < guardIdx,
    "attempts must be counted before the throttle drops them");
});

test("foreground triggers exactly ONE discovery refresh", () => {
  const fg = appCode.slice(appCode.indexOf("AppState.addEventListener"));
  const window = fg.slice(0, fg.indexOf("return () => sub.remove()"));
  const calls = (window.match(/refreshNearbyDiscovery\(\)/g) || []).length;
  assert.equal(calls, 1, `foreground calls refreshNearbyDiscovery ${calls}x; each one schedules a timer`);
});

// ── Peer identity must be STABLE across role changes ──────────────────────────
//
// configureTransport minted `UIDevice.name + UUID().prefix(6)` fresh on EVERY call, and it is
// called on every role transition — startDirector, startFollower, approveDirectorTakeover. So a
// device became a stranger to the whole mesh each time it changed role.
//
// MEASURED on the owner's fleet, build 440, from mPad's own unified log:
//
//     21:06:26  advertising as iPad-92A6C5
//     21:08:07  advertising as iPad-CF034B     <- same iPad, two minutes later
//     21:08:34  advertising as iPad-AE6CD6     <- and again
//     11 distinct peer identities for a 4-device fleet
//
// Fatal for sync: a follower tracks its director by MCPeerID (connectedDirectorPeer,
// discoveredDirectors), so a director that renames itself silently abandons every follower. It also
// manufactured a phantom split-brain — iPad-92A6C5 and iPad-AE6CD6 were the same iPad.
//
// The random suffix must STAY: since iOS 16 UIDevice.name returns a generic "iPad" to apps without
// a special entitlement, so without it every device in the loft advertises the same name.
test("the peer identity is persisted, not minted per role change", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const fn = swift.slice(swift.indexOf("private func configureTransport"));
  const body = fn.slice(0, fn.indexOf("mcSessions"));
  assert.doesNotMatch(
    body, /MCPeerID\(displayName:\s*"\\\(peerName\)-\\\(UUID\(\)/,
    "configureTransport is minting a random peer name again — every role change renames the device",
  );
  assert.match(body, /localPeerID \?\? loadOrCreatePeerID\(\)/,
    "configureTransport must reuse the existing peer, then fall back to the persisted one");
});

test("the peer id survives resetTransport, which nils it", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  // resetTransport clearing localPeerID is WHY an in-memory cache is not enough — it runs at the
  // top of startDirector and approveDirectorTakeover, the exact transitions this must survive.
  const reset = swift.slice(swift.indexOf("private func resetTransport"));
  assert.match(reset.slice(0, reset.indexOf("\n  }")), /localPeerID = nil/,
    "if reset no longer nils the peer, this test's premise changed — re-read the fix");
  // So the identity has to come off disk, archived. A stable NAME alone is not enough: Apple treats
  // the MCPeerID OBJECT as the identity.
  assert.match(swift, /NSKeyedArchiver\.archivedData\(withRootObject:/);
  assert.match(swift, /NSKeyedUnarchiver\.unarchivedObject\(ofClass: MCPeerID\.self/);
  assert.match(swift, /UserDefaults\.standard/);
});

test("the uniqueness suffix is kept — iOS 16+ gives every device the same UIDevice.name", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const prop = swift.slice(swift.indexOf("private var stablePeerName"));
  const body = prop.slice(0, prop.indexOf("\n  }"));
  assert.match(body, /UUID\(\)\.uuidString\.prefix\(6\)/, "the suffix must still exist");
  assert.match(body, /defaults\.set\(suffix, forKey: key\)/, "and it must be persisted, not random");
});

// ── A remembered peer dies with the browser that found it ─────────────────────
//
// An MCPeerID is only meaningful to the MCNearbyServiceBrowser instance that discovered it.
// refreshDiscovery tears down and recreates the browser every 5-12 s, so the new one starts with an
// EMPTY peers dictionary — but discoveredDirectors/discoveredFollowers used to keep entries for 90
// seconds, more than SEVEN browser generations. reconsiderFollowerTarget invites straight out of
// those maps, so it invited peers the live browser had never seen. Multipeer does not fail loudly:
//
//     Cannot find peer with idString [2gbyj11r6mftw] in the peers dictionary.
//
// ...and the invite evaporates. No error, no delegate callback, no session. That is the symptom that
// survived six builds — every peer found at -37 dBm, nothing ever connecting — and it explains the
// intermittency exactly: an invite only worked when it happened to fire in the same generation that
// discovered its target, so one device would follow and the rest would not.
//
// Caught from Apple's own message in the owner's Xcode console, not from ours.
test("recreating the browser forgets the peers the old browser found", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const fn = swift.slice(swift.indexOf("private func refreshDiscovery"));
  const body = fn.slice(0, fn.indexOf("\n  }"));

  const cleared = body.indexOf("discoveredDirectors.removeAll()");
  const restart = body.indexOf("startBrowsing()");
  assert.ok(cleared > 0, "discovered peers must be cleared when the browser is recreated");
  assert.ok(restart > 0 && cleared < restart,
    "they must be cleared BEFORE browsing restarts, or the new browser inherits ghosts");
  for (const m of ["discoveredFollowers.removeAll()", "discoveredDirectorInfo.removeAll()",
                   "discoveredFollowerInfo.removeAll()", "discoveredDirectorSeenAt.removeAll()"]) {
    assert.ok(body.includes(m), `${m} — every peer map must be cleared, not just some`);
  }
});

test("the 90s prune is gone — it is what let ghosts outlive their browser", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const fn = swift.slice(swift.indexOf("private func refreshDiscovery"));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  assert.doesNotMatch(
    body, /discoveredDirectorSeenAt\.filter\s*\{[^}]*>\s*90/,
    "a time-based prune is the wrong lifetime entirely: peers die with their BROWSER, not on a clock",
  );
});


// ── BLE: the beacon must be switched OFF when not directing ───────────────────
//
// BlePageBeacon.stopPublishing() existed from the first cut and was NEVER CALLED. resetTransport —
// which runs on every role change — never touched the class. So a device that had once directed
// advertised its last page FOREVER, including while it was a follower.
//
// That ghost is what build 444 rendered: devices flashed song 357 (a page some device had directed
// earlier) before the mesh corrected them to 101. It was misdiagnosed at the time as a freshness
// problem needing a nonce; the nonce is needed too, but the actual defect was a missing lifecycle.
test("the beacon stops advertising whenever the device stops directing", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const reset = swift.slice(swift.indexOf("private func resetTransport"));
  assert.match(reset.slice(0, reset.indexOf("\n  }")), /bleBeacon\.stopPublishing\(\)/,
    "resetTransport runs on every role change and MUST silence the beacon");
  // SLICE TO THE FUNCTION'S END, not to a fixed character count. This read the first 3000 chars of
  // startFollower and broke on 2026-08-18 when a comment pushed the call to 3639 — a passing test
  // turning red because an explanation got longer is a test measuring the wrong thing, and the
  // tempting "fix" is to bump the number until it passes again, which just resets the fuse.
  const fStart = swift.indexOf("func startFollower(");
  assert.ok(fStart > 0, "startFollower is gone — re-derive this test");
  const fEnd = swift.indexOf("\n  func ", fStart + 1);
  const follower = swift.slice(fStart, fEnd > fStart ? fEnd : undefined);
  assert.match(follower, /bleBeacon\.stopPublishing\(\)/,
    "a follower must never advertise a page");
});

test("stopPublishing resets state, or a re-promoted director stays silent", () => {
  const ble = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  const fn = ble.slice(ble.indexOf("func stopPublishing()"));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  // publish() early-returns when the page has not changed, so a device that stops directing on 357
  // and is re-promoted still on 357 would never re-advertise.
  assert.match(body, /lastPublishedPage = -1/, "must clear the published page");
  assert.match(body, /sessionNonce = Self\.newNonce\(\)/, "the next session must be a NEW advertiser");
});

test("a scanner keeps a seq floor PER ADVERTISER, so a new director is never mis-ordered", () => {
  const ble = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  // seq is monotonic only WITHIN a session — it restarts at 0 per launch and is per-device. Without
  // a per-advertiser floor a follower holding "last seen 1743" ignores a new director starting at 1.
  //
  // This pinned the OLD mechanism: one shared baseline, cleared by `lastSeenSeq = -1` whenever the
  // nonce changed. That clear is exactly what let a FROZEN advertisement re-qualify — a force-quit
  // director's bluetoothd keeps radiating a validly-tagged page, and when the real director dropped,
  // the rebase handed the ghost a floor of -1 and the loft rendered its stale song. Keyed per nonce
  // the floor is naturally -1 for a genuinely new advertiser and unchanged for a returning one, so
  // the rebase is unnecessary and the ghost hole closes.
  assert.match(ble, /private var seenSeqByNonce: \[String: \(seq: Int, at: TimeInterval\)\]/,
    "the per-advertiser seq floor is gone — one shared baseline lets a frozen ghost re-qualify");
  assert.match(ble, /guard parsed\.seq > priorSeq else \{ return \}/,
    "the monotonic guard no longer reads the per-advertiser floor");
  assert.doesNotMatch(ble, /lastSeenSeq = -1/,
    "the shared-baseline reset is back — that is the mechanism the ghost exploited");
});

test("legacy two-field BLE advertisements are rejected", () => {
  const ble = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  const fn = ble.slice(ble.indexOf("private func parse("));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  // Builds 433-445 ship a beacon that never stops. During a mixed-build window those frozen
  // advertisements are in the air, and accepting them is exactly the wrong-song flash. Since the
  // HMAC tag (#374) the only accepted shape is the 4-field "SV<nonce>.<seq>.<page>.<tag>".
  assert.match(body, /parts\.count == 4/, "only the 4-field HMAC-tagged format may be accepted");
  assert.doesNotMatch(body, /parts\.count == 2\b/, "the 2-field legacy format must not be parsed");
  assert.doesNotMatch(body, /parts\.count == 3\b/, "the 3-field pre-tag format must not be parsed");
});

test("BLE renders standalone again, now that no stale beacon can exist", () => {
  // BLE is the ONLY path that can put the right page on the glass before the mesh finishes its
  // ~10s handshake, so it must be able to render having heard nothing from the mesh at all. A gate
  // on mesh-derived state — lastKnownTotalPages, lastKnownPage, a connected peer — reads as a
  // harmless sanity check and is in fact a kill switch: lastKnownTotalPages defaults to 0, so the
  // fallback is dead in exactly the case it exists for, and dead SILENTLY.
  //
  // WHAT THE OLD ASSERTION MISSED. It was a whole-file grep for the declaration
  // `private var lastKnownBookId = "standard"` — a property on line 301, nowhere near the apply
  // path, with a failure message about a render gate it never looked at. Re-adding a
  // `guard self.lastKnownTotalPages != 0 else { return }` immediately above the monotonic guard
  // left it green. What is pinned now is the apply closure itself: every mention of mesh state
  // inside it must be a VALUE handed to emitPage, never a condition, and the closure must have
  // exactly the three early returns it is allowed to have.
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

  // Both endpoints asserted, and both are executable statements rather than comment banners: a
  // slice whose end marker has been deleted runs to EOF and turns the "window" into the whole file.
  const start = swift.indexOf("self.bleBeacon.onPage = {");
  assert.ok(start > 0, "the BLE apply closure is gone — re-derive this test");
  const end = swift.indexOf("self.bleBeacon.stopPublishing()", start + 1);
  assert.ok(end > start, "the statement that bounds the closure is gone — re-derive this test");
  // Comments describe intent; only code can drop a page. Strip them so a tombstone explaining a
  // removed gate cannot satisfy — or trip — the checks below.
  const applyPath = swift.slice(start, end).replace(/\/\/[^\n]*/g, "");

  // The slice really is the apply path, not an empty or drifted window.
  assert.match(applyPath, /self\.emitPage\([\s\S]*?src: "ble"\)/,
    "the BLE apply path no longer renders a page at all");
  assert.match(applyPath, /guard seq > self\.bleAppliedSeq/,
    "the within-session monotonic guard is missing — this window is not the apply path");

  // THE INVARIANT. Mesh-derived state may be READ here (emitPage carries totalPages/mode/bookId
  // through), but it may never decide whether the page is rendered.
  const MESH_STATE =
    /lastKnownTotalPages|lastKnownPage\b|currentPageNumber|connectedDirectorPeer|mcSessions|discoveredDirectors|meshAppliedSeq/;
  const meshLines = applyPath.split("\n").filter((l) => MESH_STATE.test(l));
  assert.ok(meshLines.length > 0,
    "no mesh state is carried into emitPage any more — this test's premise changed, re-read it");
  for (const line of meshLines) {
    assert.doesNotMatch(line, /\b(guard|if|while|else)\b/,
      `BLE is gated on mesh state: ${line.trim()} — lastKnownTotalPages is 0 until the mesh ` +
        "delivers, so this disables the fallback exactly when the mesh is broken");
    assert.doesNotMatch(line, /\breturn\b/,
      `the BLE apply path returns on mesh state: ${line.trim()}`);
  }

  // And no NEW early return of any shape, whatever it is spelled with. Exactly three are allowed:
  //   1. not a follower (or self deallocated)
  //   2. no book id at all — the one unrecoverable render
  //   3. the within-session monotonic seq guard
  // Anything else is a follower deciding not to obey a page it has already heard.
  const returns = (applyPath.match(/\breturn\b/g) || []).length;
  assert.equal(returns, 3,
    `the BLE apply path has ${returns} early returns; exactly 3 are sanctioned (non-follower, ` +
      "unknown book, stale seq). A fourth is a new precondition on rendering — say why in the " +
      "prose above before changing this number");

  // The book id is the one piece of state BLE does depend on, and it is why the guard above is
  // unreachable rather than a gate: it is seeded at declaration, not by the first mesh packet.
  assert.match(swift, /private var lastKnownBookId = "standard"/,
    "lastKnownBookId no longer defaults, so the ble:skip-no-book guard becomes a real mesh gate");
});
