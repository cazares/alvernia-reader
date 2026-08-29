// Boundary and contract tests for the relay sync path.
//
// WHERE THESE CAME FROM. Every test in this file closes a gap that
// `node scripts/mutation-sweep.mjs` found by breaking the real source and watching the suite stay
// green. That matters because none of these gaps were visible by reading: publishSeq.js and
// svSyncDecision.js are among the best-tested modules in the repo, and the sweep still walked
// straight through the exact boundary both of them are built around.
//
// Two themes:
//
//   1. THE OFF-BY-ONE AT THE EDGE OF THE FRESHNESS WINDOW. "A director is live for 90 seconds" is
//      decided independently in two modules that never speak to each other, and nothing checked
//      that they draw the line in the same place. Flipping `<=` to `<` on one side, or `floor` to
//      `ceil`, survived every existing test on both sides.
//
//   2. THE FIELD NOBODY READS. A decision can be computed perfectly and then dropped — svSyncDecision
//      can stop setting `reveal` and no test notices, even though `reveal` is the only thing that
//      puts the page on a follower's screen. This is the same shape as the bug that left
//      `forceFollowerReconnectNow` implemented in Swift, missing from the bridge, and silently
//      resolving to null on every ⟳ tap for months.

import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { decidePublish, sanitizeSeq, NO_DIRECTOR_SEQ } from "../sync-worker/src/publishSeq.js";

const require = createRequire(import.meta.url);
const LIB_PATH = "../web/src/lib/svSyncDecision.js";
const { decideRelaySnapshot, clockOffsetFromDateHeader, clockOffsetFromServerNow } = require(LIB_PATH);

const MAX_AGE_S = 90;
// A whole second, so the relay's floor(nowMs/1000) and the follower's unrounded (nowMs/1000) agree
// exactly. Comparing the two sides at a fractional millisecond would introduce a sub-second
// difference that has nothing to do with the rule being tested.
const NOW_MS = 1_787_000_000_000;
const NOW_S = NOW_MS / 1000;

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 1. THE FRESHNESS WINDOW — the same line, drawn by two modules that never talk.
// ════════════════════════════════════════════════════════════════════════════════════════════════

/** Does the RELAY consider a room with a snapshot this old still held by a live director? */
const relaySaysLive = (ageS) =>
  decidePublish({
    rawSeq: NOW_MS, nowMs: NOW_MS,
    snapshotSeq: 1, snapshotTs: NOW_S - ageS, maxAgeS: MAX_AGE_S,
  }).reason === "advance";   // "advance" = a live room; "takeover" = the relay saw it as abandoned

/** Does a FOLLOWER consider a snapshot this old to be from a live director? */
const followerSaysLive = (ageS) =>
  decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - ageS },
    { lastSeq: -1, hasDirector: false, browsing: false, currentPage: 1, force: false,
      nowMs: NOW_MS, clockOffsetMs: 0, maxAgeS: MAX_AGE_S },
  ).action !== "demote";

test("the relay and a follower draw the live/dead line at exactly the same second", () => {
  // THE HOLE THIS PREVENTS. If the relay held a director live for 90 s and followers for 60 s,
  // there would be a 30-second window in every Mass where the relay cheerfully accepts page turns —
  // answering the director 200 {ok:true}, pill green — that every follower discards as stale. The
  // director sees nothing wrong and the congregation sees nothing at all.
  //
  // Nothing in production shares this constant: it is declared three times and compared never.
  for (const ageS of [0, 1, 45, 88, 89, 90, 91, 92, 200]) {
    assert.equal(relaySaysLive(ageS), followerSaysLive(ageS),
      `at ${ageS}s old the relay says live=${relaySaysLive(ageS)} but a follower says live=${followerSaysLive(ageS)} — ` +
      "a director in that window can publish to a congregation that has already written them off");
  }
});

test("the freshness boundary is inclusive on both sides: exactly maxAgeS old is still LIVE", () => {
  // Pinned separately from the agreement above, because "both sides are wrong in the same
  // direction" would satisfy that test. maxAgeS is a MAXIMUM AGE: a snapshot exactly that old is
  // still within it. One second past it is not.
  assert.equal(relaySaysLive(MAX_AGE_S), true, `the relay treats a snapshot exactly ${MAX_AGE_S}s old as abandoned`);
  assert.equal(followerSaysLive(MAX_AGE_S), true, `a follower demotes a director whose last update is exactly ${MAX_AGE_S}s old`);
  assert.equal(relaySaysLive(MAX_AGE_S + 1), false, "the relay still holds a room one second past the window");
  assert.equal(followerSaysLive(MAX_AGE_S + 1), false, "a follower still trusts a director one second past the window");
});

test("the relay measures age against ITS OWN clock, rounded down, never up", () => {
  // floor vs ceil is one character and shifts the whole window by up to a second in the direction
  // that matters: rounding UP ages every snapshot early, so a director alive for 89.4 s is declared
  // gone. Fractional milliseconds are the only place the two differ, which is exactly why no test
  // that used whole seconds could ever see it.
  const justInside = NOW_MS + 999;   // 0.999 s past a whole second
  const d = decidePublish({
    rawSeq: justInside, nowMs: justInside,
    snapshotSeq: 1, snapshotTs: NOW_S - MAX_AGE_S, maxAgeS: MAX_AGE_S,
  });
  assert.equal(d.reason, "advance",
    "rounding the server clock UP declared a director dead almost a second early");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 2. SEQ EDGE CASES THE WIRE CAN ACTUALLY PRODUCE.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a publish with NO seq field at all is treated as the reserved value, not as seq 1", () => {
  // /publish is OPEN — no credential, since 2026-08-06 — so the relay's input is whatever anyone
  // POSTs, not only what our transmitter sends. A body with no `seq` must fold to the reserved 0,
  // which means: refused outright while a director is live, and in a stale room taking over at
  // snapshotSeq + 1 so it out-ranks whatever poisoned value the departed director left behind.
  //
  // Folding it to 1 instead looks harmless and is not: the takeover would store seq 1, and every
  // later publish from a real device (a wall-clock millisecond, ~1.8e12) is fine — but a room that
  // had been poisoned to a huge seq would now be RESET to 1 by a malformed body, handing control to
  // whoever sent it.
  for (const missing of [undefined, null]) {
    assert.equal(sanitizeSeq(missing, NOW_MS, 500), NO_DIRECTOR_SEQ,
      `a seq of ${String(missing)} did not fold to the reserved value`);
  }
  const live = decidePublish({ rawSeq: undefined, nowMs: NOW_MS, snapshotSeq: 500, snapshotTs: NOW_S, maxAgeS: MAX_AGE_S });
  assert.equal(live.apply, false, "a seq-less body was applied while a director was live");
  assert.equal(live.reason, "seq_reserved");

  const takeover = decidePublish({
    rawSeq: undefined, nowMs: NOW_MS,
    snapshotSeq: 999_999_999_999_999, snapshotTs: NOW_S - 1000, maxAgeS: MAX_AGE_S,
  });
  assert.equal(takeover.apply, true, "a seq-less body could not take over an abandoned room");
  assert.equal(takeover.seq, 999_999_999_999_999 + 1,
    "a seq-less takeover reset the room's seq instead of stepping past the poisoned one");
});

test("a director whose clock exactly matches the server is left alone, not clamped", () => {
  // The boundary of the clamp itself. `seq > nowMs` and `seq >= nowMs` differ only for the device
  // whose clock is perfectly correct — which is the common case, not an edge case. Clamping it
  // would replace its honest seq with max(nowMs, currentSeq + 1), quietly pushing the room a
  // millisecond into the future on every single publish by a well-behaved device.
  assert.equal(sanitizeSeq(NOW_MS, NOW_MS, NOW_MS + 500), NOW_MS,
    "a device whose clock exactly matches the server had its seq rewritten");
  assert.equal(sanitizeSeq(NOW_MS + 1, NOW_MS, 10), NOW_MS,
    "a device one millisecond fast was NOT clamped — the ceiling is off by one");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 3. A DECISION THAT NOBODY EXECUTES IS NOT A DECISION.
// ════════════════════════════════════════════════════════════════════════════════════════════════

const ctx = (over = {}) => ({
  lastSeq: -1, hasDirector: false, browsing: false, currentPage: 1, force: false,
  nowMs: NOW_MS, clockOffsetMs: 0, maxAgeS: MAX_AGE_S, ...over,
});
const fresh = (over = {}) => ({ page: 42, seq: 5000, ts: NOW_S, ...over });

test("every outcome that puts a page on screen also reveals the reader", () => {
  // `reveal` is the single flag that takes a follower from the loading state to showing the
  // songbook. Its absence is not a cosmetic regression: the device sits on a blank screen for the
  // whole Mass while every other piece of state is perfectly correct, and the person holding it has
  // no way to tell that from a network problem.
  //
  // Both branches that put content in front of somebody must set it — including "browsing", where
  // the follower is looking at a page they chose rather than the director's.
  const follow = decideRelaySnapshot(fresh(), ctx());
  assert.equal(follow.action, "follow");
  assert.equal(follow.reveal, true, "following a director does not reveal the reader — the screen stays blank");

  const browsing = decideRelaySnapshot(fresh({ page: 77 }), ctx({ browsing: true, hasDirector: true }));
  assert.equal(browsing.action, "browsing");
  assert.equal(browsing.reveal, true, "a follower browsing the songbook never gets the reader revealed");
});

test("every outcome that changes what the pill should say asks for the pill to be redrawn", () => {
  // The pill is the follower's ONLY indication of whether a director is live and whether they are
  // still following. A decision that changes liveness or following-ness without setting renderPill
  // leaves a green "en vivo" dot on a director who is gone, or hides the "volver a en vivo"
  // affordance from somebody who is off-live and has no other way back.
  const cases = [
    ["demote", decideRelaySnapshot(fresh({ ts: NOW_S - 1000 }), ctx({ hasDirector: true }))],
    ["live-dup", decideRelaySnapshot(fresh({ seq: 10 }), ctx({ hasDirector: true, lastSeq: 10 }))],
    ["follow", decideRelaySnapshot(fresh(), ctx())],
    ["browsing", decideRelaySnapshot(fresh({ page: 77 }), ctx({ browsing: true, hasDirector: true }))],
  ];
  for (const [name, d] of cases) {
    assert.equal(d.action, name, `expected action ${name}`);
    assert.equal(d.renderPill, true, `the "${name}" outcome never asks for the pill to be redrawn`);
  }
  // The one action that must NOT touch the UI: a malformed frame is not news.
  const rejected = decideRelaySnapshot({ page: NaN, seq: 1, ts: NOW_S }, ctx({ hasDirector: true }));
  assert.equal(rejected.action, "reject");
  assert.equal(rejected.renderPill, false, "a malformed frame redraws the UI");
  assert.equal(rejected.reveal, false, "a malformed frame reveals the reader");
});

test("a rejected frame returns the follower's state UNCHANGED, so a blind caller cannot corrupt it", () => {
  // applyRelaySnapshot returns early on "reject", but the decision object is documented to carry
  // safe "no change" values in every field so that a caller applying them blindly is still correct.
  // If those defaults drifted — hasDirector defaulting to false, lastSeq to 0 — a single malformed
  // frame would silently demote a live director or rewind the de-dup floor.
  const before = ctx({ hasDirector: true, lastSeq: 900, browsing: true });
  const d = decideRelaySnapshot({ page: "not a number", seq: 1, ts: NOW_S }, before);
  assert.equal(d.action, "reject");
  assert.equal(d.hasDirector, before.hasDirector, "a malformed frame changed hasDirector");
  assert.equal(d.lastSeq, before.lastSeq, "a malformed frame rewound the seq de-dup floor");
  assert.equal(d.browsing, before.browsing, "a malformed frame changed browse mode");
  assert.equal(d.livePage, undefined, "a malformed frame reported a live page");
  assert.equal(d.renderPage, undefined, "a malformed frame asked for a page render");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 4. THE MODULE HAS TO BE REACHABLE THE WAY THE BROWSER REACHES IT.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("the lib is published on BOTH access paths — the browser's global and CommonJS", () => {
  // THE TRAP THIS EXISTS FOR. Every test of this module reaches it through `require()`. The BROWSER
  // reaches it through `globalThis.svSyncDecision`, and app.js's applyRelaySnapshot begins:
  //
  //     const lib = globalThis.svSyncDecision;
  //     if (!lib || typeof lib.decideRelaySnapshot !== "function") { ...inline fallback... }
  //
  // So if the global assignment broke, every test here would keep passing while the shipped app
  // silently ran a SECOND, less-tested implementation of the same rule — the one that already
  // diverged for ten days in 2026-08 and put a stale song in front of the whole congregation.
  //
  // This is the JS twin of `forceFollowerReconnectNow`: implemented, tested, and unreachable from
  // the only place that calls it.
  assert.ok(globalThis.svSyncDecision, "the module no longer publishes itself as globalThis.svSyncDecision — " +
    "the browser would silently fall back to app.js's inline copy of this logic");
  for (const fn of ["decideRelaySnapshot", "clockOffsetFromDateHeader", "clockOffsetFromServerNow"]) {
    assert.equal(typeof globalThis.svSyncDecision[fn], "function", `globalThis.svSyncDecision.${fn} is not callable`);
    assert.equal(globalThis.svSyncDecision[fn], require(LIB_PATH)[fn],
      `${fn} differs between the global and the CommonJS export — the browser is running different code than these tests`);
  }
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 5. CLOCK CALIBRATION FAILS SAFE.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("an unusable clock reading keeps the PREVIOUS offset instead of resetting to zero", () => {
  // Calibration happens on /state responses. A worker old enough not to send `now`, a proxy that
  // strips the Date header, or a response that arrives garbled must all leave the follower's
  // existing offset alone. Falling back to 0 would be worse than not calibrating at all: a device
  // that had correctly measured itself five minutes fast would forget, judge every subsequent
  // snapshot against its own wrong clock, and demote a live director permanently.
  const PREV = -300_000;   // this device previously measured itself 5 minutes fast
  for (const bad of [null, undefined, "", "not a date", "Tue, 99 Xxx 9999", NaN]) {
    assert.equal(clockOffsetFromDateHeader(bad, NOW_MS, PREV), PREV,
      `a Date header of ${JSON.stringify(bad)} discarded a good calibration`);
  }
  for (const bad of [null, undefined, 0, -1, NaN, Infinity, "1787000000"]) {
    assert.equal(clockOffsetFromServerNow(bad, NOW_MS, PREV), PREV,
      `a body.now of ${JSON.stringify(bad)} discarded a good calibration`);
  }
  // And a GOOD reading must actually replace it, or the fallback above is indistinguishable from
  // never calibrating at all.
  assert.equal(clockOffsetFromServerNow(NOW_S + 42, NOW_MS, PREV), 42_000,
    "a valid server time did not update the offset");
});

test("the offset is (server - device) in milliseconds, in that order", () => {
  // A sign error here is invisible in every single-device test and doubles the error in the field:
  // a device 5 minutes fast would be corrected to 10 minutes fast. The follower adds this offset to
  // its own clock, so it must be positive when the SERVER is ahead.
  const deviceIsSlowBy = 90_000;
  assert.equal(clockOffsetFromServerNow(NOW_S, NOW_MS - deviceIsSlowBy, 0), deviceIsSlowBy,
    "the offset is inverted — correcting a skewed clock would double the skew");
  assert.equal(clockOffsetFromServerNow((NOW_MS - deviceIsSlowBy) / 1000, NOW_MS, 0), -deviceIsSlowBy);
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 6. THE TRANSMITTER'S OWN SEQ MUST BE STRICTLY INCREASING, EVEN INSIDE ONE MILLISECOND.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("two page turns in the same millisecond still get distinct, increasing seqs", async () => {
  // nextSeq is `Math.max(seqCounter + 1, Date.now())`. Without the `+ 1` floor, two publishes inside
  // one millisecond would carry the SAME seq — and the relay's monotonic guard refuses the second
  // as `seq_not_newer`, with a 200 {ok:true} the transmitter reads as success. A director tapping
  // quickly would silently lose page turns.
  //
  // Driven through the real module, with Date.now() frozen so both calls genuinely land in one
  // millisecond. Freezing it is the whole point: at wall-clock speed the two calls would land in
  // different milliseconds and the floor would never be exercised.
  const mod = await import("../src/directorRelaySync.js?boundaries=1");
  const seqs = [];
  const realFetch = globalThis.fetch;
  const realNow = Date.now;
  globalThis.fetch = async (_url, init) => { seqs.push(JSON.parse(init.body).seq); return { ok: true, status: 200 }; };
  Date.now = () => NOW_MS;
  try {
    mod.setRelayPublishing(true);
    for (let i = 0; i < 5; i++) {
      mod.publishPageToRelay(300 + i, 400, { mode: "reader" });
      await new Promise((r) => setImmediate(r));   // let the coalescer drain between turns
    }
  } finally {
    Date.now = realNow;
    globalThis.fetch = realFetch;
    mod.setRelayPublishing(false);
  }

  assert.ok(seqs.length >= 2, `only ${seqs.length} publishes reached the wire`);
  for (let i = 1; i < seqs.length; i++) {
    assert.ok(seqs[i] > seqs[i - 1],
      `two publishes in the same millisecond shared seq ${seqs[i]} — the relay refuses the second one silently`);
  }
});

test("the transmitter never puts a page below 1 on the wire", async () => {
  // A follower that fails to render sets its page sentinel to -1. If that device then becomes
  // director before a real page lands, broadcasting the sentinel would clamp to page 1 on every
  // follower and yank the whole congregation to the front of the book. The relay clamps too; this
  // is the near-side lock on the same door, and it is the one that runs first.
  const mod = await import("../src/directorRelaySync.js?boundaries=2");
  const sent = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (_url, init) => { sent.push(JSON.parse(init.body)); return { ok: true, status: 200 }; };
  try {
    mod.setRelayPublishing(true);
    for (const page of [-1, 0, NaN, undefined, "abc", -999]) {
      mod.publishPageToRelay(page, 400, { mode: "reader" });
      await new Promise((r) => setImmediate(r));
    }
  } finally {
    globalThis.fetch = realFetch;
    mod.setRelayPublishing(false);
  }
  assert.ok(sent.length > 0, "nothing reached the wire");
  for (const payload of sent) {
    assert.ok(payload.page >= 1, `page ${payload.page} left the device — every follower would clamp to page 1`);
    // The exact value, not just a bound. `Math.max(0, …)` and `Math.min(0, …)` both satisfy ">= 0",
    // and the second silently pins totalPages at zero for every follower.
    assert.equal(payload.totalPages, 400, `totalPages arrived as ${payload.totalPages} instead of 400`);
    // ts is epoch SECONDS. seq, on the same wire, is epoch MILLISECONDS. Confusing the two is the
    // single most destructive mistake available in this system — a ts in milliseconds is fresh
    // forever, so no director is ever demoted and no follower ever recovers — and the two fields sit
    // adjacent in this payload.
    assert.ok(payload.ts > 1e9 && payload.ts < 1e10,
      `ts is ${payload.ts}, which is not epoch seconds — a millisecond ts never goes stale`);
  }
});

test("the director is warned ONCE when the relay refuses a publish, and told when it recovers", async () => {
  // WHY THIS PATH EXISTS. A resolved fetch() is not proof of success: a rejected publish comes back
  // !ok WITHOUT throwing. Unchecked, that failure is swallowed silently — the director's app shows
  // nothing while EVERY signovivo.com follower freezes on the last page for the whole Mass, because
  // the relay is the only sync path to web phones.
  //
  // Both halves matter and both are one-shot. Warning on every page turn would spam a burst into a
  // wall of banners; never retracting means a fixed relay keeps shouting, and on 2026-08-06 that
  // banner was still on screen long after the deploy that fixed it. A warning that cannot retract
  // itself teaches people to ignore warnings.
  //
  // The existing relayAuthRecovery.test.mjs slices this function out of the source and runs it with
  // `new Function`, which is a genuinely good trick — but its slice ends at the `} else if`, so the
  // refusal branch below is outside it and was never executed by anything.
  const mod = await import("../src/directorRelaySync.js?boundaries=4");
  const warned = [];
  const recovered = [];
  mod.setRelayAuthErrorHandler((status) => warned.push(status));
  mod.setRelayAuthOkHandler(() => recovered.push(true));

  let response = { ok: false, status: 401 };
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => response;
  const turn = async (page) => {
    mod.publishPageToRelay(page, 400, { mode: "reader" });
    for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  };
  try {
    mod.setRelayPublishing(true);

    await turn(1);
    assert.deepEqual(warned, [401], "a refused publish did not warn the director that the congregation is frozen");
    await turn(2);
    await turn(3);
    assert.deepEqual(warned, [401], "the warning fired on every page turn — a burst becomes a wall of banners");

    response = { ok: true, status: 200 };
    await turn(4);
    assert.equal(recovered.length, 1, "the relay started working again and nothing retracted the warning");
    await turn(5);
    assert.equal(recovered.length, 1, "recovery fired on every successful publish, not just on the transition");

    // A 5xx or a 429 is transient and already covered by the re-publish loop. Warning on those
    // would cry wolf on exactly the blips that fix themselves.
    response = { ok: false, status: 503 };
    await turn(6);
    response = { ok: false, status: 429 };
    await turn(7);
    assert.deepEqual(warned, [401], "a transient 5xx/429 raised the permanent-failure banner");

    // …but a genuine 403 must be able to warn again, because the latch was re-armed by the recovery.
    response = { ok: false, status: 403 };
    await turn(8);
    assert.deepEqual(warned, [401, 403], "a later genuine refusal could not warn — the latch never re-armed");
  } finally {
    globalThis.fetch = realFetch;
    mod.setRelayPublishing(false);
    mod.setRelayAuthErrorHandler(null);
    mod.setRelayAuthOkHandler(null);
  }
});

test("a freshly loaded transmitter publishes NOTHING until it is explicitly told to direct", async () => {
  // The default matters more than the setter. Every device in the parish loads this module at boot,
  // and all but one of them is a follower. If the module defaulted to enabled, a follower would
  // start POSTing its own page to the room the moment anything called publishPageToRelay — and
  // /publish is open, so nothing upstream would refuse it. The congregation would be driven by
  // whichever iPad happened to turn a page.
  //
  // Deliberately never calls setRelayPublishing: the point is the value the module starts with.
  const mod = await import("../src/directorRelaySync.js?boundaries=5");
  let calls = 0;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => { calls++; return { ok: true, status: 200 }; };
  try {
    mod.publishPageToRelay(370, 400, { mode: "reader" });
    for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  } finally {
    globalThis.fetch = realFetch;
  }
  assert.equal(calls, 0, "a transmitter that was never told to direct published to the whole congregation at boot");
});

test("a device that is not directing publishes nothing at all", async () => {
  // The local gate. /publish is open, so nothing upstream will refuse a stray publish from a
  // follower or an ex-director — this is the only thing standing between a non-director and the
  // whole web congregation's page.
  const mod = await import("../src/directorRelaySync.js?boundaries=3");
  let calls = 0;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => { calls++; return { ok: true, status: 200 }; };
  try {
    mod.setRelayPublishing(false);
    for (let i = 0; i < 10; i++) {
      mod.publishPageToRelay(300 + i, 400, { mode: "reader" });
      await new Promise((r) => setImmediate(r));
    }
  } finally {
    globalThis.fetch = realFetch;
  }
  assert.equal(calls, 0, `a device that is not directing sent ${calls} publishes to the congregation`);
});
