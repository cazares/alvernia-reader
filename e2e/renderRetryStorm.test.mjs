import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";

// EXECUTE the shipped rule, never a copy of it. The first version of this file re-implemented the
// pacing arithmetic locally; a mutation that deleted the guard from app.js outright passed all
// twelve assertions. Same UMD/createRequire pattern as svSyncDecision's tests.
const require = createRequire(import.meta.url);
const { shouldPaceRender, RENDER_RETRY_COOLDOWN_MS } = require("../web/src/lib/svRenderPace.js");

const APP = fs.readFileSync("web/src/app.js", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const SWIFT = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

// ── The two ways a follower dies while the pill says it is fine ───────────────────────────────
//
// Both were found preparing to stress-test six devices, both are older than build 428, and both
// are present in build 381 — so neither is fixed by going back. They are the reason a Mass ends
// with "half the devices synced".
//
//   A. THE RENDER-RETRY STORM. A failed render makes native reset its page reference to an
//      impossible sentinel so the director's next heartbeat re-drives this follower. That recovery
//      is correct and deliberate — without it a transient failure strands the device on the error
//      overlay forever. But it had no floor: a page that CANNOT render (uncached and offline,
//      which is every follower in a church, or a poisoned cache slot) turns the sentinel and the
//      1 Hz heartbeat into a closed loop, two off-DOM Images and their timers per pass, once a
//      second, for as long as the director stays on that page.
//
//   B. THE DEAF FOLLOWER. Multipeer's browser losing sight of a peer is not the peer's MCSession
//      dropping, and treating it as one clears the reference that every page delivery is gated on
//      — permanently, because the recovery path is itself gated on the field that was cleared.
//
// These tests execute the real cooldown arithmetic rather than asserting on prose, and pin the
// Swift guard against the source.

/**
 * Index of the `)` that closes the `(` at openIdx. Used to lift a whole `if (...)` condition out of
 * the source by STRUCTURE, so an assertion can survive reformatting, a longer comment, or an extra
 * argument — and so a slice can never silently run to EOF. Returns -1 if the parens never balance.
 */
const matchParen = (src, openIdx) => {
  let depth = 0;
  for (let i = openIdx; i < src.length; i += 1) {
    if (src[i] === "(") depth += 1;
    else if (src[i] === ")") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
};

// ── A. the render-retry storm ────────────────────────────────────────────────────────────────

const COOLDOWN = RENDER_RETRY_COOLDOWN_MS;
/** The SHIPPED decision, imported — not a local re-implementation. */
const isPaced = (failure, page, now) => shouldPaceRender(failure, page, now);

test("the cooldown exists and is a sane pace against a 1 Hz heartbeat", () => {
  assert.ok(COOLDOWN >= 2000, `${COOLDOWN}ms is too short to blunt a 1 Hz retry storm`);
  assert.ok(COOLDOWN <= 15000, `${COOLDOWN}ms would leave a recoverable page broken too long`);
});

test("a page that cannot render is retried on a PACE, not on every heartbeat", () => {
  // The failure this fixes: the director sits on one song for three minutes while a follower
  // cannot load it. Count the expensive attempts across that window.
  const HEARTBEAT_MS = 1000;
  const WINDOW_MS = 180000; // three minutes on one song — an ordinary hymn
  let failure = null;
  let attempts = 0;
  for (let now = 0; now < WINDOW_MS; now += HEARTBEAT_MS) {
    if (isPaced(failure, 42, now)) continue;
    attempts += 1;
    failure = { page: 42, at: now }; // it fails again
  }
  const unpaced = WINDOW_MS / HEARTBEAT_MS;
  assert.ok(attempts < unpaced / 4, `${attempts} attempts of a possible ${unpaced} — not paced enough`);
  assert.ok(attempts >= 2, "must keep retrying — a page can become loadable at any moment");
});

test("recovery still happens on its own, within one cooldown of the page becoming loadable", () => {
  // The pace must never become a dead end: this is the property that lets an offline follower
  // rejoin without anyone touching it.
  let failure = { page: 7, at: 0 };
  const loadableAt = 2500;
  let renderedAt = null;
  for (let now = 0; now < 60000; now += 1000) {
    if (isPaced(failure, 7, now)) continue;
    if (now >= loadableAt) { renderedAt = now; break; }
    failure = { page: 7, at: now };
  }
  assert.notEqual(renderedAt, null, "the page never recovered — the cooldown became a dead end");
  assert.ok(renderedAt - loadableAt <= COOLDOWN, `recovery took ${renderedAt - loadableAt}ms, over one cooldown`);
});

test("a DIFFERENT page from the director is never delayed by a failing one", () => {
  // The director turning the page must land instantly even while another page is failing —
  // otherwise the fix for a stuck follower would make every follower laggy.
  const failure = { page: 42, at: 1000 };
  assert.equal(isPaced(failure, 43, 1001), false, "a new page was paced by the previous failure");
  assert.equal(isPaced(failure, 42, 1001), true, "the failing page should still be paced");
});

test("the memo is CLEARED on a successful render, so a page that recovers is never paced again", () => {
  const commit = APP.slice(APP.indexOf("    state.currentPage = nextPage;"));
  assert.match(
    commit.slice(0, 200),
    /state\.lastRenderFailure = null/,
    "a successful render does not clear lastRenderFailure — a recovered page stays paced",
  );
});

test("the memo is RECORDED on the failure path, beside the native signal", () => {
  const idx = APP.indexOf('type: "render-failed"');
  assert.ok(idx > 0, "the render-failed bridge post is gone");
  assert.match(
    APP.slice(Math.max(0, idx - 400), idx),
    /state\.lastRenderFailure = \{ page: nextPage, at: Date\.now\(\) \}/,
    "the failure is not recorded, so nothing can pace it",
  );
});

test("a human tapping ⟳ bypasses the cooldown entirely", () => {
  const fn = APP.slice(APP.indexOf("const reconnectRelay = () =>"), APP.indexOf("const reconnectRelay = () =>") + 900);
  assert.match(fn, /state\.lastRenderFailure = null/, "⟳ does not clear the pace — the one control a singer has would feel dead");
});

test("native still resets its page ref on failure — the pace must not replace the recovery", () => {
  // If this sentinel ever goes away, a follower whose render failed is de-duped by the heartbeat
  // and stranded on the error overlay. The cooldown paces that recovery; it does not replace it.
  const start = NATIVE.indexOf('case "render-failed"');
  // To the end of the case, not a fixed byte count: the explanatory comment above the code is long
  // and a fixed window silently sliced the assertion target out of view.
  const block = NATIVE.slice(start, NATIVE.indexOf("break;", start));
  assert.match(block, /currentPageRef\.current = -1/, "the re-drive sentinel is gone");
  assert.match(block, /roleRef\.current === "follower"/, "the sentinel is no longer follower-only — a broadcaster would publish -1");
});

// ── B. the deaf follower ─────────────────────────────────────────────────────────────────────

test("lostPeer does NOT tear down a director whose MCSession is still alive", () => {
  const fn = SWIFT.slice(
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, lostPeer"),
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers"),
  );
  assert.ok(fn.length > 100, "could not find lostPeer");
  assert.match(fn, /allConnectedPeers\.contains\(peerID\)/, "the session-still-live guard is missing — this is the wedge");
  // The guard must come BEFORE the teardown, or it guards nothing.
  assert.ok(
    fn.indexOf("allConnectedPeers.contains(peerID)") < fn.indexOf("self.connectedDirectorPeer = nil"),
    "the guard runs after the teardown it is supposed to prevent",
  );
});

test("the guard leaves early — it must not fall through into reconsiderFollowerTarget", () => {
  // Falling through would emit a misleading searching -> connected flap on every advertisement
  // lapse, which is exactly the noise that made this bug invisible in the log.
  const fn = SWIFT.slice(
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, lostPeer"),
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers"),
  );
  const guardIdx = fn.indexOf("allConnectedPeers.contains(peerID)");
  assert.match(fn.slice(guardIdx, guardIdx + 220), /return/, "the guard does not return");
});

test("the guard is observable in the relay log — a silent fix cannot be confirmed in the field", () => {
  const fn = SWIFT.slice(
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, lostPeer"),
    SWIFT.indexOf("func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers"),
  );
  assert.match(fn, /dbgLog\("lost:session-still-live"/, "no breadcrumb — there would be no way to prove it fired at Mass");
});

test(".notConnected keeps its own twin of the guard", () => {
  // The two must stay symmetric: they are the same defect reached from the two directions MPC can
  // report it, and only one of them was guarded before.
  const idx = SWIFT.indexOf("case .notConnected:");
  const block = SWIFT.slice(idx, idx + 1600);
  assert.match(block, /if self\.allConnectedPeers\.contains\(peerID\)/, ".notConnected lost its parallel-path guard");
});

test("app.js actually CALLS the shared rule inside renderPage — a correct module that is never invoked fixes nothing", () => {
  // WHAT THE OLD ASSERTION MISSED. It looked for `svShouldPaceRender(...)` anywhere in the prologue
  // and, separately, for a bare `return;` anywhere in the same window. The prologue already contains
  // an unrelated early return — the already-on-this-page guard — so the second assertion was
  // satisfied no matter what. Deleting the short-circuit while keeping the call
  // (`if (… svShouldPaceRender(…)) { /* nothing */ }`) restored the full 1 Hz retry storm and the
  // test stayed green. So now the call and its consequence are pinned as ONE statement: the guard is
  // located structurally, its condition is EXECUTED with a stub in place of the shipped rule, and
  // the token immediately after the condition's closing paren must be the `return`.
  const start = APP.indexOf("const renderPage = async");
  assert.ok(start > 0, "renderPage is gone");
  const end = APP.indexOf("const requestId = state.pageLoadRequest + 1;", start);
  assert.ok(end > start, "renderPage's prologue no longer ends at the page-load request — re-anchor this test");
  const fn = APP.slice(start, end);

  const callIdx = fn.indexOf("svShouldPaceRender(");
  assert.ok(callIdx > 0, "renderPage does not consult the pacing rule");
  const ifIdx = fn.lastIndexOf("if (", callIdx);
  assert.ok(ifIdx >= 0, "the pacing call is not inside an `if` — it cannot be short-circuiting anything");
  const openIdx = fn.indexOf("(", ifIdx);
  const closeIdx = matchParen(fn, openIdx);
  assert.ok(closeIdx > openIdx, "the pacing guard's condition never closes its parentheses");
  assert.ok(closeIdx > callIdx, "the pacing call is not part of that `if` condition");

  // THE SHORT-CIRCUIT. Whatever the condition ends up being, the very next thing after it must be a
  // bare `return;` — not a block, not a log, not a comment standing in for one.
  const after = fn.slice(closeIdx + 1).trimStart();
  assert.ok(
    after.startsWith("return;"),
    `the pacing guard does not short-circuit the render; it is followed by ${JSON.stringify(after.slice(0, 40))}`,
  );

  // THE CONDITION, EXECUTED. A stub stands in for the shipped rule so we learn what renderPage
  // actually asks it, and what renderPage does with the answer.
  const cond = fn.slice(openIdx + 1, closeIdx);
  let guard;
  try {
    guard = new Function("userInitiated", "state", "nextPage", "svShouldPaceRender", `return !!(${cond});`);
  } catch {
    assert.fail(`the pacing guard's condition is no longer evaluable on its own: ${cond}`);
  }

  const failure = { page: 42, at: 1000 };
  const calls = [];
  const paced = (...args) => { calls.push(args); return true; };
  const notPaced = (...args) => { calls.push(args); return false; };

  assert.equal(guard(false, { lastRenderFailure: failure }, 42, paced), true,
    "a director re-drive of a page the rule says to pace is not short-circuited — the storm is back");
  assert.equal(calls.length, 1, "the shipped rule was not consulted exactly once");
  assert.equal(calls[0][0], failure, "the rule is not given the recorded failure — it can only ever say 'no'");
  assert.equal(calls[0][1], 42, "the rule is asked about the wrong page");
  assert.ok(Math.abs(calls[0][2] - Date.now()) < 5000, "the rule is not given the current time, so the cooldown never expires");

  calls.length = 0;
  assert.equal(guard(false, { lastRenderFailure: failure }, 42, notPaced), false,
    "a render is short-circuited even when the rule says it is fine — every follower would go laggy");

  // AND THE HUMAN EXEMPTION, from the same statement: a swipe must never be swallowed (2026-08-17,
  // "it wouldn't let me swipe to songs 4 or 5"). It must not even ask the rule.
  calls.length = 0;
  assert.equal(guard(true, { lastRenderFailure: failure }, 42, paced), false,
    "a human-initiated render is paced — swipes go dead for a cooldown with no error and no feedback");
  assert.equal(calls.length, 0, "a human tap still consults the pacing rule; the exemption must short-circuit first");
});

test("the page loads the pacing lib, or the fallback silently disables it forever", () => {
  const html = fs.readFileSync("web/src/index.html", "utf8");
  assert.match(html, /lib\/svRenderPace\.js/, "index.html never loads svRenderPace.js");
  const idx = html.indexOf("lib/svRenderPace.js");
  assert.ok(idx < html.indexOf('src="app.js"'), "svRenderPace must load BEFORE app.js reads it");
});

// ── C. the group-session director mix-up (the Mass bug, found 2026-08-16 on 4 devices) ────────

test("a follower only accepts the DIRECTOR as its director — not a fellow follower", () => {
  // An MCSession is a GROUP: the director admits every follower into one session, and Multipeer
  // then connects each member to every other member. A follower's .connected therefore fires for
  // its peers too, and assigning connectedDirectorPeer unconditionally made the LAST peer to
  // connect "the director" — usually another follower. Every real page was then dropped by the
  // peer-match guard in didReceive, the 3s watchdog tore the link down, and the race repeated: a
  // ~4s reconnect loop that never converges. Measured: 22 follower-to-follower connections and
  // heartbeat delivery of 519/29/4 across three followers.
  const idx = SWIFT.indexOf("case .connected:");
  assert.ok(idx > 0, "the .connected handler is gone");
  const block = SWIFT.slice(idx, SWIFT.indexOf("case .connecting:", idx));
  const guardIdx = block.indexOf("isDirector");
  assert.ok(guardIdx > 0, "no director check before claiming connectedDirectorPeer — this is the Mass bug");
  assert.ok(
    guardIdx < block.indexOf("self.connectedDirectorPeer = peerID"),
    "the check runs AFTER the assignment it is supposed to gate",
  );
  assert.match(block, /guard isDirector else \{/, "the check does not actually gate anything");
});

test("the director predicate accepts every legitimate way we know a director", () => {
  // Must not reject the real director: we may know it as the peer we invited, by its token, or by
  // its advertised role — and lostPeer can clear the token map moments before .connected lands.
  const idx = SWIFT.indexOf("case .connected:");
  const block = SWIFT.slice(idx, SWIFT.indexOf("case .connecting:", idx));
  assert.match(block, /pendingInvitePeer == peerID/, "the peer we invited is not accepted");
  assert.match(block, /discoveredDirectors\[peerID\] != nil/, "a known director token is not accepted");
  assert.match(block, /discoveredDirectorInfo\[peerID\]\?\["role"\] == "director"/, "an advertised director role is not accepted");
});

test("a non-director peer is IGNORED, not treated as a disconnect", () => {
  // It must return before touching the watchdog, hello timer or discovery state — a fellow
  // follower joining our session is a non-event, not a change of director.
  const idx = SWIFT.indexOf("case .connected:");
  const block = SWIFT.slice(idx, SWIFT.indexOf("case .connecting:", idx));
  const g = block.indexOf("guard isDirector else {");
  assert.match(block.slice(g, g + 220), /return/, "it falls through instead of ignoring the peer");
  assert.match(block.slice(g, g + 220), /dbgLog\("session:peer-not-director"/, "no breadcrumb — the fix could not be confirmed from the field");
});
