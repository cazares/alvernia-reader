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
  assert.match(swift, /deviceLog\.info\(/, "dbgLog must mirror to the device log");
  // .public or the whole log reads <private> — an unreadable log is the problem, not the fix.
  assert.match(swift, /privacy:\s*\.public/);
});

test("the local log is written BEFORE the batching queue, so nothing can silence it", () => {
  const swift = fs.readFileSync(
    path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const body = swift.slice(swift.indexOf("private func dbgLog("));
  const local = body.indexOf("deviceLog.info(");
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
