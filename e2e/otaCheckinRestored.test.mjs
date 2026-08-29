import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const WORKER = fs.readFileSync("sync-worker/src/index.ts", "utf8");

// WHY THIS FILE EXISTS. "kill the fleet dashboard" (2026-08-18) deleted /fleet/checkin wholesale.
// That was the RIGHT call for the dashboard itself — but OTA arming was piggybacked on that same
// call's response ("no new endpoint, no new poll"), so the deletion also silently removed the only
// way a device ever learns BOOK_UPDATE_VERSION exists, and the client-side signal (lastCheckinOkAt)
// canApplyNow's safety gate depends on. Caught before merge, not after — see the git history around
// 96995e1 for the original mechanism this restores, narrower.

test("the worker has its OWN narrow OTA route, never touching the deleted fleet store", () => {
  assert.match(WORKER, /if \(url\.pathname === "\/ota\/checkin"\)/, "no OTA route exists");
  const route = WORKER.slice(WORKER.indexOf('if (url.pathname === "/ota/checkin")'));
  const body = route.slice(0, route.indexOf('if (url.pathname === "/log")'));
  assert.match(body, /request\.method !== "POST"/, "the route accepts more than POST");
  assert.match(body, /getByName\("__ota__"\)/, "the route reads/writes the deleted fleet DO instance");
  assert.doesNotMatch(body, /getByName\("__fleet__"\)/, "the route touches the deleted fleet store");
  assert.match(body, /decideBookUpdate\(/, "the route never computes an arming decision");
});

test("otaCheckin stores ONLY deviceId/bookVersion/bookStage — no path back to PII", () => {
  const fn = WORKER.slice(WORKER.indexOf("async otaCheckin(input"));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  for (const forbidden of ["label", "role", "surface", "deviceKind", "nativeBuild", "webCached"]) {
    assert.doesNotMatch(body, new RegExp(`o\\.${forbidden}\\b`),
      `otaCheckin reads "${forbidden}" — that is exactly the PII-adjacent field the dashboard stored`);
  }
  assert.match(body, /entry\.bookVersion/, "bookVersion is not recorded — decideBookUpdate needs it");
  assert.match(body, /entry\.bookStage/, "bookStage is not recorded — the fleet-wide throttle needs it");
});

test("/ota/checkin is metered as non-essential, like /log", () => {
  const gate = WORKER.slice(WORKER.indexOf("const NON_ESSENTIAL ="));
  const line = gate.slice(0, gate.indexOf(";"));
  assert.match(line, /\/ota\/checkin/, "OTA check-ins are not counted against the reserved budget");
});

test("the client checks in unconditionally — boot, a slow interval, and foreground", () => {
  assert.match(NATIVE, /const otaCheckin = useCallback/, "no client-side otaCheckin exists");

  // Three trigger sites, none gated on page/song state — Miguel: "make sure if devices are
  // stationary on say page 1 or page N that the app will still auto-update". A device that never
  // turns a page must still check in.
  const boot = NATIVE.slice(NATIVE.indexOf('dbgLog("boot", { syncAvailable });'), NATIVE.indexOf('}, [dbgLog, syncAvailable, otaCheckin]);'));
  assert.match(boot, /otaCheckin\(\);/, "boot no longer checks in for an update");

  const intervalBlock = NATIVE.slice(NATIVE.indexOf("const OTA_CHECKIN_INTERVAL_MS"));
  const intervalMs = Number(intervalBlock.match(/OTA_CHECKIN_INTERVAL_MS = (\d+) \* (\d+) \* (\d+);/)?.slice(1).reduce((a, b) => a * Number(b), 1));
  assert.ok(intervalMs > 0, "the interval constant could not be parsed");
  assert.ok(intervalMs < 90 * 1000 * 10, "the interval is absurdly long — a real arming test would time out waiting");
  assert.ok(intervalMs >= 60 * 1000, "the interval is back near the old 90s dashboard cadence — that cost 3,840 req/day");
  assert.match(intervalBlock.slice(0, 300), /setInterval\(otaCheckin,/, "the interval does not call otaCheckin");

  const fg = NATIVE.slice(NATIVE.indexOf("void autoApplyIfSafeRef.current?.();"), NATIVE.indexOf("if (syncAvailable) refreshNearbyDiscovery"));
  assert.match(fg, /otaCheckin\(\);/, "foregrounding the app no longer checks in for an update");
});

test("otaCheckin restores BOTH broken signals: lastCheckinOkAt and the staging pipeline trigger", () => {
  const fn = NATIVE.slice(NATIVE.indexOf("const otaCheckin = useCallback"), NATIVE.indexOf("// Stable per-install device id"));
  assert.match(fn, /lastCheckinOkAtRef\.current = at/, "the live-internet-proof signal is not restored");
  assert.match(fn, /AsyncStorage\.setItem\(STORAGE_KEYS\.lastCheckinOkAt/, "lastCheckinOkAt is not persisted");
  assert.match(fn, /onCheckinResponseRef\.current\?\.\(body\)/, "the response is never fed into the staging pipeline");
  // Only these two fields go out — no label/role, matching the server-side contract above.
  assert.doesNotMatch(fn, /label:|role:|deviceKind:/, "the client sends fleet-dashboard fields again");
});

test("the solo-reader cache restore is GONE — never nuked twice", () => {
  // Miguel, testing on hardware, 2026-08-18: the cache restore (added earlier the same day to fix
  // a page-restore-after-OTA-swap gap) regressed the ORIGINAL headline director-page-race fix a
  // SECOND time. It wrote a REMEMBERED (possibly stale) page into currentPageRef unconditionally;
  // tagging it src:"cache" stopped it from falsely satisfying the boot-reveal gate, but did NOT
  // stop it from overwriting currentPageRef/state.currentPage — so a device that tapped "Ser
  // Director" shortly after boot, before any real sync arrived, broadcast the STALE cached page
  // to the whole choir. "why even cache at all????" — no good answer survived two regressions.
  // Removed entirely: the read, both write sites, and the STORAGE_KEYS entry itself.
  const start = NATIVE.indexOf('case "bridge-ready"');
  const end = NATIVE.indexOf('case "page-changed"');
  const block = NATIVE.slice(start, end);
  assert.doesNotMatch(block, /SOLO READER/, "the solo-reader cache-restore branch is back");
  assert.doesNotMatch(NATIVE, /lastPagePrefix/, "a lastPagePrefix reference survives somewhere in the native shell");

  const offlineBooks = fs.readFileSync("src/offlineBooks.ts", "utf8");
  assert.doesNotMatch(offlineBooks, /lastPagePrefix/, "STORAGE_KEYS.lastPagePrefix still exists — the key should be gone, not just unused");
});

test("shell-too-old is already a wired refusal (MIN_SHELL_BUILD), and it's no longer silent", () => {
  // Miguel, 2026-08-18: "can you easily bake in forced binary updates for incompat. between new
  // OTA and old binary... where things bust unless binary is updated by user". This mechanism
  // already existed end-to-end (web/build.mjs's MIN_SHELL_BUILD, src/bookUpdate.js's stageBook +
  // canApplyNow gates) — a binary too old to run a book can never stage or apply it. What was
  // missing: nothing told the PERSON. This pins the fix — an immediate toast plus a real native
  // modal on the "shell-too-old" refusal — without re-testing the refusal itself (already covered
  // by e2e/bookUpdate.test.mjs).
  const start = NATIVE.indexOf("const onCheckinResponse = useCallback");
  const end = NATIVE.indexOf("onCheckinResponseRef.current = onCheckinResponse;");
  const body = NATIVE.slice(start, end);
  assert.match(body, /rec\.error === "shell-too-old"/,
    "onCheckinResponse no longer distinguishes the shell-too-old failure from any other stage failure");
  assert.match(body, /type: "toast"/,
    "the shell-too-old branch no longer surfaces a toast — back to silent refusal");
});

test("shell-too-old also shows a REAL modal, not just an easy-to-miss toast", () => {
  // Miguel, 2026-08-18: "MUST show a modal or *something* or else choir members will be mega
  // confused". A toast alone auto-dismisses and is easy to miss for something this rare and this
  // consequential (the device is permanently behind until someone updates it).
  const start = NATIVE.indexOf("const onCheckinResponse = useCallback");
  const end = NATIVE.indexOf("onCheckinResponseRef.current = onCheckinResponse;");
  const body = NATIVE.slice(start, end);
  assert.match(body, /Alert\.alert\(/, "no native Alert — still just the toast");
  assert.match(body, /TESTFLIGHT_APP_URL/, "the modal has no action wired to actually get them updated");
  assert.match(body, /Linking\.openURL\(TESTFLIGHT_APP_URL\)/, "the update button doesn't open anything");
});

test("the modal is gated on mesh-idle — never blocks a director's screen mid-Mass", () => {
  // Followers have NO internet at Mass at all (project_mass_network_reality), so this check-in
  // could not even reach them then — but the DIRECTOR carries cellular and could be mid-Mass when
  // this fires. A blocking modal on their screen at that exact moment is worse than the confusion
  // it exists to prevent.
  const start = NATIVE.indexOf("const onCheckinResponse = useCallback");
  const end = NATIVE.indexOf("onCheckinResponseRef.current = onCheckinResponse;");
  const body = NATIVE.slice(start, end);
  const shellTooOldIdx = body.indexOf('rec.error === "shell-too-old"');
  const modalSection = body.slice(shellTooOldIdx, shellTooOldIdx + 1200);
  assert.match(modalSection, /meshPeerCountRef\.current === 0/,
    "the modal is not gated on mesh being idle — it could interrupt an active Mass/rehearsal");
  // The one-shot guard must be set ONLY inside the mesh-idle branch, so a busy check-in retries
  // the modal on the next one instead of permanently suppressing it.
  const idleGateIdx = modalSection.indexOf("meshPeerCountRef.current === 0");
  const oneShotIdx = modalSection.indexOf("didShellTooOldNoticeRef.current = true");
  assert.ok(idleGateIdx > 0 && oneShotIdx > idleGateIdx,
    "the one-shot guard is set outside the mesh-idle branch — a busy device would never get the modal");
});

test("native build freshness: client reports its own build, server confirms only when current", () => {
  // Miguel, 2026-08-18: "force everyone to get on this version... with in-app code... backend" —
  // TestFlight has no API to force-push an install, so this is the in-app equivalent: the client
  // reports its build, the server confirms it against LATEST_NATIVE_BUILD, and the client nudges
  // (never blocks) when it can't be confirmed current.
  assert.match(NATIVE, /nativeBuild: Number\(BUILD_VERSION\)/,
    "the checkin body no longer reports the device's own native build");
  assert.match(WORKER, /LATEST_NATIVE_BUILD\?: string/, "the Env type lost LATEST_NATIVE_BUILD");
  assert.match(WORKER, /nativeBuildConfirmedLatest/, "the worker no longer computes the confirmation flag");
});

test("the confirmation flag fails toward NOT confirmed on any ambiguous input", () => {
  // Old binaries (built before this feature existed) send no nativeBuild at all — Number(undefined)
  // is NaN, so Number.isFinite(reported) is false and the flag is correctly omitted, never a false
  // positive. Same for a malformed/missing LATEST_NATIVE_BUILD.
  const idx = WORKER.indexOf("NATIVE BUILD FRESHNESS");
  const body = WORKER.slice(idx, idx + 1000);
  assert.match(body, /Number\.isFinite\(latest\)/, "no finiteness check on the configured latest build");
  assert.match(body, /Number\.isFinite\(reported\)/, "no finiteness check on the device-reported build");
  assert.match(body, /reported >= latest/, "the comparison direction is wrong or missing");
});

test("the nudge never blocks, and NEITHER half fires while the mesh is busy", () => {
  const idx = NATIVE.indexOf("NATIVE BUILD FRESHNESS NUDGE");
  assert.ok(idx > 0, "the native-build nudge block is missing");
  const body = NATIVE.slice(idx, NATIVE.indexOf("const staged = await readStored", idx));
  assert.match(body, /type: "toast"/, "no non-blocking toast — the only signal is the blocking modal");
  assert.match(body, /meshPeerCountRef\.current === 0/,
    "the modal is not gated on mesh being idle — it could interrupt an active Mass/rehearsal");
  assert.match(body, /didNativeBuildNudgeRef\.current = true/, "no one-shot guard for the modal");

  // THE TOAST MUST BE INSIDE THE GATE TOO. This test used to assert the opposite in its own title
  // ("fires a toast unconditionally"), and that is exactly the bug: the one-shot flag is set only in
  // the mesh-idle branch, so a device WITH peers connected re-toasted on every 4-minute check-in and
  // every foreground. The only device at Mass with both internet and peers is the director's iPad,
  // which meant the notice slid over the songbook every four minutes for the whole service.
  // CONTAINMENT, NOT ORDERING. Comparing string offsets only proves the toast appears after the
  // gate's condition — it catches the exact historical shape (the toast sat on the line ABOVE the
  // `if`) and nothing else. Anything that reintroduces the toast AFTER the gate block reads as
  // "later, therefore inside", and the four-minute-toast-during-Mass bug comes back green.
  const gateStart = body.indexOf("if (meshPeerCountRef.current === 0) {");
  assert.ok(gateStart > 0, "the mesh-idle gate is gone");
  // Walk to the matching brace so the block is bounded by structure rather than by a marker.
  let depth = 0, gateEnd = -1;
  for (let i = body.indexOf("{", gateStart); i < body.length; i++) {
    if (body[i] === "{") depth++;
    else if (body[i] === "}") { depth--; if (depth === 0) { gateEnd = i; break; } }
  }
  assert.ok(gateEnd > gateStart, "could not find the end of the mesh-idle gate block");
  const inGate = body.slice(gateStart, gateEnd);
  const outsideGate = body.slice(0, gateStart) + body.slice(gateEnd);
  assert.match(inGate, /type: "toast"/, "the toast is not inside the mesh-idle gate");
  assert.doesNotMatch(outsideGate, /type: "toast"/,
    "a toast fires outside the mesh-idle gate — it will repeat every check-in during Mass");
});

test("the nudge offers BOTH signovivo.com and TestFlight — never a dead end", () => {
  // Miguel, 2026-08-18: "don't just lock them out... link out to both actually — you don't wanna
  // just middle finger them." Both this nudge AND the pre-existing shell-too-old modal must offer
  // a path forward that works right now (the web) alongside the actual fix (updating the binary).
  const idx = NATIVE.indexOf("NATIVE BUILD FRESHNESS NUDGE");
  // Bounded by the next block rather than a character count, so adding a comment inside the nudge
  // cannot silently slide its links out of the window and fail a test about behaviour that is intact.
  const body = NATIVE.slice(idx, NATIVE.indexOf("const staged = await readStored", idx));
  assert.match(body, /SIGNOVIVO_URL/, "no signovivo.com link in the native-build nudge");
  assert.match(body, /TESTFLIGHT_APP_URL/, "no TestFlight link in the native-build nudge");

  const shellTooOldIdx = NATIVE.indexOf('rec.error === "shell-too-old"');
  const shellBody = NATIVE.slice(shellTooOldIdx, shellTooOldIdx + 1200);
  assert.match(shellBody, /SIGNOVIVO_URL/, "shell-too-old still only offers TestFlight, not signovivo.com too");
});

test("release.sh only moves LATEST_NATIVE_BUILD alongside a REAL native upload, never on SKIP_NATIVE=1", () => {
  const RELEASE = fs.readFileSync("scripts/release.sh", "utf8");
  const idx = RELEASE.indexOf("LATEST_NATIVE_BUILD must NOT move");
  assert.ok(idx > 0, "the SKIP_NATIVE guard comment/logic is missing");
  const body = RELEASE.slice(idx, idx + 500);
  assert.match(body, /SKIP_NATIVE:-0\}" != "1"/, "no SKIP_NATIVE guard before writing LATEST_NATIVE_BUILD");
  assert.match(body, /TF_UPLOADED" = "1"/, "not gated on a real TestFlight upload having happened");
});


test("becomeFollower clears lastDirectorSnapshotRef — no stale memory survives a role transition", () => {
  // Miguel: "still has cache issue aka shows previous song on new director" — confirmed on
  // FOLLOWERS, both fresh-launch and mid-session, on build 465 (which had already removed the
  // solo-reader cache). Root cause: lastDirectorSnapshotRef (a follower's memory of the PREVIOUS
  // director's last page) was never cleared on a role transition, so a follower that reloaded or
  // resynced in the window between an old director stepping down and a new one's first broadcast
  // resynced to the stale snapshot instead of waiting for the real one.
  const fn = NATIVE.slice(NATIVE.indexOf("const becomeFollower = useCallback"));
  const body = fn.slice(0, fn.indexOf("[syncAvailable, injectEvent, stopDirectorHeartbeat]"));
  assert.match(body, /lastDirectorSnapshotRef\.current = null/,
    "becomeFollower no longer clears the stale director snapshot on every role transition");
});

test("no page-value usage of lastDirectorSnapshotRef survives — simple and stateless", () => {
  // Miguel: "I thought we got rid of caching which should include remembered STATE like this...
  // why even cache at all". Three call sites pre-filled currentPageRef/injected a sync-event from
  // this REMEMBERED ref before a fresh request completed (bridge-ready's follower-resync branch,
  // the resync/⟳ handler, and the foreground handler) — all three removed. The ONLY surviving
  // uses must be: the SET site (when a real page arrives) and TIMESTAMP-only reads (the
  // live-director-warning check, canApplyNow's lastDirectorSnapshotAt gate) — never a page value
  // read back into currentPageRef or injected into the web.
  const pageValueReads = [...NATIVE.matchAll(/const \{ page, book \} = lastDirectorSnapshotRef\.current/g)];
  assert.equal(pageValueReads.length, 0,
    `${pageValueReads.length} remaining site(s) destructure a page/book out of the remembered ref`);
});

test("typing the director code threads the current page too, not just the pill tap", () => {
  // Adversarial hunt (2026-08-18), after two prior regressions of the same bug class: the
  // aa68c9e fix (thread state.currentPage through becomeDirector) was only ever wired for the
  // "Ser Director" pill's request-director bridge message. Typing the code on the numpad went
  // through case "director-code" with NO currentPage — becomeDirector's knownCurrentPage was
  // undefined, and whatever currentPageRef already held (right or wrong) is what got broadcast.
  const appJs = fs.readFileSync("web/src/app.js", "utf8");
  assert.match(appJs, /postNativeBridge\(\{ type: "director-code", code, currentPage: state\.currentPage \}\)/,
    "the web no longer sends currentPage with a typed director code");

  const start = NATIVE.indexOf('case "director-code"');
  const end = NATIVE.indexOf('case "request-director"');
  const body = NATIVE.slice(start, end);
  assert.match(body, /msg\.currentPage/, "the typed-code handler no longer reads currentPage from the bridge message");
  assert.match(body, /onDirectorCode\(msg\.code, knownPage\)/, "the typed-code handler no longer threads knownPage into onDirectorCode");
});
