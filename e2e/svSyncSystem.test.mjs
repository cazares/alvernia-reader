// End-to-end system tests for the relay sync path: a director, the relay room, and N followers,
// wired out of the REAL modules and run against a virtual clock.
//
// WHAT MAKES THESE DIFFERENT FROM THE REST OF e2e/. Most tests in this directory assert on source
// TEXT — they prove the code still contains a string. That style has failed here in every direction
// it can: 13 of 13 hand-written mutations once slipped past three entire test files; the build-475
// campaign shipped three brand-new tests that could not fail at all; and five separate files broke
// by slicing a fixed number of characters and silently running to EOF. The unit tests that DO
// execute real code each cover one module alone — and every bug that actually reached the
// congregation lived in the SEAM between two modules that were each correct by themselves.
//
// So these tests do not read source. They run a Mass:
//
//   src/directorRelaySync.js  ──POST──▶  publishSeq.js  ──WS──▶  svSyncDecision.js  ──▶  a page
//     (the real transmitter)              (the real           (the real follower       somebody
//      with its own device clock           relay rule)          decision) x N            reads
//
// and assert on what a person in the pews would have seen. Every assertion names the outage it
// exists to prevent. See e2e/helpers/sv-sync-sim.mjs for what is real and what is simulated — the
// short version is that no decision in the harness is the harness's own.
//
// PROVEN ABLE TO FAIL. Every test here was mutation-tested against the real sources with
// `node scripts/mutation-sweep.mjs`; the counts are in docs/test-meaningfulness-2026-08.md. The
// first block below is the vacuity guard: it proves the SIMULATION can express failure at all,
// because the fastest way to get a green suite that means nothing is a harness in which no
// scenario is expressible.

import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

import {
  Sim,
  RELAY_LIVE_MAX_AGE_S,
  assertHarnessFidelity,
  freshnessWindows,
  decisionContract,
  readSource,
} from "./helpers/sv-sync-sim.mjs";

const require = createRequire(import.meta.url);
const { decideRelaySnapshot } = require("../web/src/lib/svSyncDecision.js");

/** A page sequence a follower rendered must be a SUBSEQUENCE of what the director published: it may
 *  miss pages (a drop, a coalesced burst) but it may never invent one and never reorder two. */
function isSubsequence(sub, sup) {
  let i = 0;
  for (const x of sup) if (i < sub.length && sub[i] === x) i++;
  return i === sub.length;
}

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 0. VACUITY GUARDS — prove the simulation can express failure before trusting anything it says.
//
// The single most likely way this file becomes worthless is a unit mistake. seq is epoch
// MILLISECONDS and ts is epoch SECONDS; a snapshot whose ts is stamped in ms is fresh forever, so
// every demotion, dead-director and clock-skew test below would pass vacuously — green, fast, and
// blind to the exact class of bug they exist to catch.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("fidelity: the harness's copies of the relay still match the relay", () => {
  // RUNS FIRST, because every relay assertion below is only worth anything if this passes.
  //
  // The harness delegates the relay's one DECISION to the real decidePublish, but it hand-copies
  // the mechanical step after it — the page floor, the totalPages floor, the 64-character caps, the
  // server ts stamp — plus the snapshot-on-accept push, the ping reply, and /state's `now`. Those
  // copies are the one way this file can quietly become a green test about a system that no longer
  // exists. This reads both sides and compares them to each other, so drift on EITHER fails.
  //
  // It exists because an adversarial re-hunt of this campaign found that the harness's header
  // promised exactly this function and nobody had written it.
  assertHarnessFidelity(assert);
});

test("vacuity: the simulated follower CAN demote — otherwise every staleness test is a no-op", async () => {
  const sim = new Sim({ seed: 1 });
  const f = sim.addFollower({ id: "f" });
  await sim.settle();
  const ancient = {
    v: 1, page: 7, seq: sim.now, ts: Math.floor(sim.now / 1000) - (RELAY_LIVE_MAX_AGE_S + 60),
    totalPages: 400, mode: "reader", bookId: "standard",
  };
  const d = f.receiveRaw(ancient);
  assert.equal(d.action, "demote",
    "an hour-old snapshot did not demote — the harness's ts is almost certainly in the wrong unit, " +
    "and every freshness assertion in this file is passing without testing anything");
  assert.equal(f.currentPage, 1, "a stale snapshot rendered its page");
  sim.uninstall();
});

test("vacuity: the simulated follower CAN follow — otherwise every convergence test is a no-op", async () => {
  const sim = new Sim({ seed: 1 });
  const f = sim.addFollower({ id: "f" });
  await sim.settle();
  const d = f.receiveRaw({
    v: 1, page: 7, seq: sim.now, ts: Math.floor(sim.now / 1000),
    totalPages: 400, mode: "reader", bookId: "standard",
  });
  assert.equal(d.action, "follow");
  assert.equal(f.currentPage, 7);
  sim.uninstall();
});

test("vacuity: the simulated relay CAN refuse — otherwise every ordering test is a no-op", async () => {
  const sim = new Sim({ seed: 1 });
  const first = sim.relay.publish({ page: 5, seq: sim.now }, sim.now);
  assert.equal(first.body.ignored, undefined, "a first publish into an empty room must be applied");
  const rewind = sim.relay.publish({ page: 4, seq: sim.now - 5000 }, sim.now);
  assert.equal(rewind.body.ignored, true, "the relay accepted a rewind — the monotonic guard is not wired in");
  sim.uninstall();
});

// ── the cross-component contracts the whole system rests on ─────────────────────────────────────

test("the freshness window is the SAME number in all three places it is declared", () => {
  // RELAY_LIVE_MAX_AGE_S exists independently in the worker, in app.js, and as svSyncDecision's
  // default. Nothing in production imports it from a shared place, so nothing stops one from
  // drifting. If the relay held a director live for 90 s and followers for 60 s, there would be a
  // 30 s hole in every Mass in which the relay cheerfully accepts page turns that every follower
  // discards as stale — and the director's pill stays green throughout.
  const w = freshnessWindows();
  assert.equal(typeof w.worker, "number", "could not find RELAY_LIVE_MAX_AGE_S in sync-worker/src/index.ts");
  assert.equal(typeof w.web, "number", "could not find RELAY_LIVE_MAX_AGE_S in web/src/app.js");
  assert.equal(typeof w.libDefault, "number", "could not find the maxAgeS default in svSyncDecision.js");
  assert.equal(w.web, w.worker, "web/src/app.js and the worker disagree about how long a director stays live");
  assert.equal(w.libDefault, w.worker, "svSyncDecision's default disagrees with the worker");
  assert.equal(w.harness, w.worker, "this test harness has drifted from the worker");
});

test("every field the decision module produces is consumed by the code that executes it", () => {
  // A decision field that no caller reads is a decision that silently does nothing. That is not a
  // hypothetical here: `forceFollowerReconnectNow` was implemented in Swift, never declared in the
  // bridge .m, and the JS guard that was supposed to notice wrote its check as an old-shell
  // fallback — so every ⟳ tap for months resolved to null and the one control a person has for a
  // wedged session did nothing at all. This is the same shape, one layer up.
  const { produced, consumed, executorFound } = decisionContract();
  assert.ok(executorFound, "could not locate applyRelaySnapshot's executor block in web/src/app.js — " +
    "the markers this test slices on have moved, so it is no longer checking anything");
  assert.ok(produced.length >= 8, `only found ${produced.length} decision fields — the scan is broken`);
  const ignored = produced.filter((f) => !consumed.includes(f));
  assert.deepEqual(ignored, [],
    `svSyncDecision sets ${ignored.join(", ")} and applyRelaySnapshot never reads it — ` +
    "the decision is made and thrown away");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 1. A LIVE DIRECTOR IS NEVER STARVED — at any clock skew, in either direction.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("every page turn reaches the congregation at every device clock skew", async () => {
  // THE FROZEN-CONGREGATION WEDGE. The transmitter's seq IS the device's wall clock, so a director
  // whose iPad clock is off trips the relay's future-ceiling on every single publish. The first
  // version folded those to the reserved 0, which the freshness gate then refused: measured 2 of 20
  // page turns applied, one per ~90 s. Every refusal answered HTTP 200 {ok:true}, so the director's
  // app showed nothing wrong and the pill stayed green while signovivo.com sat on a stale page.
  //
  // A clock set by hand is the ORDINARY case, not the extreme one — which is why ±45 s is in here
  // next to ±2 minutes. The second version of the clamp only engaged ABOVE a 60 s tolerance and
  // left exactly that case broken.
  for (const skewMs of [-120000, -45000, -5000, 0, 5000, 45000, 120000]) {
    const sim = new Sim({ seed: 3 });
    const director = await sim.addDirector({ id: `d${skewMs}`, skewMs });
    const follower = sim.addFollower({ id: `f${skewMs}` });
    await sim.settle();

    const pages = [];
    for (let turn = 0; turn < 20; turn++) {
      const page = 300 + turn;
      pages.push(page);
      director.turnTo(page);
      await sim.settle(5000);
    }

    const applied = sim.relay.publishLog.filter((p) => p.applied).length;
    assert.equal(applied, 20,
      `at ${skewMs / 1000}s skew only ${applied}/20 page turns reached the relay — ` +
      `refused: ${sim.relay.publishLog.filter((p) => !p.applied).map((p) => p.reason).join(",")}`);
    assert.equal(follower.currentPage, 319, `at ${skewMs / 1000}s skew the follower ended on the wrong page`);
    assert.ok(isSubsequence(follower.pageHistory, pages), "the follower rendered a page out of order");
    assert.equal(follower.hasDirector, true, `at ${skewMs / 1000}s skew a LIVE director was seen as dead`);
    sim.uninstall();
  }
});

test("the room is never parked in the future by a fast device clock", async () => {
  // A room whose seq sits ahead of every honest clock locks out the NEXT director for exactly that
  // long: its correct-clock seq reads as "not newer" and every page it turns is refused, silently.
  //
  // The bound is max(serverNow, prevSeq + 1), not serverNow — the +1 floor deliberately puts the
  // room k-1 ms ahead when k publishes land inside one server millisecond, and asserting the
  // literal "never exceeds now" would red-line correct code.
  const sim = new Sim({ seed: 4 });
  const director = await sim.addDirector({ id: "fast", skewMs: 120000 });
  await sim.settle();

  let prevSeq = 0;
  for (let turn = 0; turn < 12; turn++) {
    director.turnTo(200 + turn);
    await sim.settle(1000);
    const seq = sim.relay.snapshot.seq;
    assert.ok(seq <= Math.max(sim.now, prevSeq + 1),
      `the room is ${seq - sim.now} ms ahead of the server after turn ${turn} — ` +
      "the next director will be locked out for that long");
    prevSeq = seq;
  }
  sim.uninstall();
});

test("a correct-clock director takes over immediately after a fast-clocked one", async () => {
  // The regression the first ceiling-clamp introduced: the room held now+60s, so the incoming
  // director's every publish was refused as not-newer until real time caught up.
  const sim = new Sim({ seed: 5 });
  const fast = await sim.addDirector({ id: "fast", skewMs: 120000 });
  const follower = sim.addFollower({ id: "f" });
  await sim.settle();
  fast.turnTo(370);
  await sim.settle(2000);
  fast.stepDown();

  const normal = await sim.addDirector({ id: "normal", skewMs: 0 });
  normal.turnTo(371);
  await sim.settle(2000);

  assert.equal(sim.relay.snapshot.page, 371, "the incoming director was locked out by the previous one's clamp");
  assert.equal(follower.currentPage, 371, "the congregation stayed on the ex-director's page");
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 2. A REFUSED PUBLISH CHANGES NOTHING.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a refused publish mutates nothing in the room — not page, not seq, above all not ts", async () => {
  // ts is the dangerous one. A refused publish that still refreshed ts would keep a wedged room
  // looking live to every follower, hiding the wedge for as long as it lasted. This is the exact
  // mechanism the wedge cycle depends on ("next 90 s every page turn ignored, ts never refreshed"),
  // so it is worth pinning against a future "just refresh ts so followers stay live" patch.
  const sim = new Sim({ seed: 6 });
  sim.relay.publish({ page: 370, seq: sim.now, totalPages: 400, mode: "reader", bookId: "standard" }, sim.now);
  const before = JSON.parse(JSON.stringify(sim.relay.snapshot));

  sim.now += 30000;
  for (const [label, body] of [
    ["a rewind", { page: 5, seq: before.seq - 10000 }],
    ["a duplicate seq", { page: 6, seq: before.seq }],
    ["the reserved 0", { page: 7, seq: 0 }],
    ["a negative seq", { page: 8, seq: -1 }],
    ["NaN", { page: 9, seq: NaN }],
    ["Infinity-as-null", { page: 10, seq: null }],
  ]) {
    const res = sim.relay.publish(body, sim.now);
    assert.equal(res.body.ignored, true, `${label} was ACCEPTED`);
    assert.deepEqual(sim.relay.snapshot, before, `${label} was refused but still changed the room`);
  }
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 3. A DEAD DIRECTOR IS DEMOTED — even though the relay keeps replaying it forever.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a dead director is demoted on every follower, despite the relay replaying it every heartbeat", async () => {
  // OBSERVED AT WEDNESDAY PRACTICE. The relay answers each 4 s "ping" with the CURRENT snapshot, so
  // a director who stopped an hour ago keeps arriving on every healthy socket with an unchanged seq,
  // four times a minute, forever. The old inline logic ran the seq de-dup BEFORE the freshness
  // check, so every one of those replies was dropped as a duplicate and never reached the staleness
  // test — the green "en vivo" pill stayed lit on a dead director indefinitely.
  //
  // Reordering those two guards is a one-line change that no source-grep would notice.
  const sim = new Sim({ seed: 7 });
  const director = await sim.addDirector({ id: "d" });
  const followers = [0, 1, 2, 3].map((i) => sim.addFollower({ id: `f${i}` }));
  await sim.settle();
  for (const f of followers) f.startHeartbeat(4000);

  director.turnTo(372);
  await sim.settle(5000);
  for (const f of followers) {
    assert.equal(f.hasDirector, true, "a live director was not seen");
    assert.equal(f.currentPage, 372);
  }

  // The director goes silent. Nothing else changes: the sockets stay healthy and the heartbeat
  // keeps ticking, which is precisely the situation the pill used to survive.
  director.stepDown();
  await sim.idle(RELAY_LIVE_MAX_AGE_S * 1000 + 10000);
  await sim.settle(10000);

  for (const f of followers) {
    assert.equal(f.hasDirector, false, `${f.id} still believes a director who stopped ${RELAY_LIVE_MAX_AGE_S}s ago is live`);
    assert.equal(f.pillHistory.at(-1), "hidden", `${f.id}'s pill is still lit for a dead director`);
    assert.equal(f.currentPage, 372, "demotion must not move anybody off the page they are on");
  }

  // And it must STAY demoted, without a poll storm. A demotion triggers exactly one recalibrating
  // /state poll; if that poll could re-demote and re-poll, the fix for a clock jump would be an
  // unbounded request loop pointed at a Worker on a 100,000-request daily account quota.
  const pollsAfterDemotion = followers.map((f) => f.pollCount || 0);
  await sim.idle(RELAY_LIVE_MAX_AGE_S * 5 * 1000);
  await sim.settle(10000);
  followers.forEach((f, i) => {
    assert.equal(f.hasDirector, false, `${f.id} resurrected a dead director`);
    assert.ok((f.pollCount || 0) - pollsAfterDemotion[i] <= 1,
      `${f.id} issued ${(f.pollCount || 0) - pollsAfterDemotion[i]} extra polls while idle — demotion is looping`);
  });
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 4. A SNAPSHOT THAT CANNOT PROVE ITS AGE IS NEVER RENDERED.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a snapshot with no usable timestamp never renders and never promotes", async () => {
  // "Every device jumping to a stale song 2 before the mesh corrected them to 372", 2026-08-18. The
  // freshness test read `!isFinite(snap.ts) || <in window>`, so a snapshot that could not prove its
  // age was treated as FRESH and applied unconditionally — and a snapshot that cannot prove its age
  // is exactly the one that is indistinguishable from something written hours ago.
  //
  // The fix shipped that day landed only in applyRelaySnapshot's INLINE FALLBACK, the branch that
  // runs when the lib fails to load, i.e. essentially never. The path that always runs kept failing
  // open for ten more days.
  const sim = new Sim({ seed: 8 });
  const director = await sim.addDirector({ id: "d" });
  const follower = sim.addFollower({ id: "f" });
  await sim.settle();
  director.turnTo(372);
  await sim.settle(2000);
  assert.equal(follower.currentPage, 372);

  for (const ts of [undefined, null, NaN, Infinity, "1787000000", {}]) {
    const d = follower.receiveRaw({ v: 1, page: 2, seq: sim.now + 1e6, ts, totalPages: 400, mode: "reader", bookId: "standard" });
    assert.equal(d.action, "demote", `ts=${String(ts)} was treated as datable`);
    assert.equal(follower.currentPage, 372,
      `ts=${String(ts)} put page 2 in front of the congregation while the director was on 372`);
  }
  sim.uninstall();
});

test("the inline fallback in app.js fails closed on an undateable snapshot too", () => {
  // The two implementations of this rule are the whole reason the 2026-08-18 fix was invisible for
  // ten days. app.js cannot be imported here (4,600 lines of DOM), so this is a structural check —
  // but it is anchored on the two markers that bound the fallback, and it asserts BOTH markers
  // exist, because a slice whose end marker has been deleted runs to EOF and would then be
  // satisfied by the lib-path code further down the file.
  const app = readSource("web/src/app.js");
  const start = app.indexOf("if (!lib || typeof lib.decideRelaySnapshot !== \"function\") {");
  const end = app.indexOf("const wasLive = relay.hasDirector;");
  assert.ok(start > 0, "the inline fallback's opening guard has moved — this test no longer checks it");
  assert.ok(end > start, "the marker that bounds the inline fallback has moved — the window ran to EOF");
  const fallback = app.slice(start, end);
  assert.match(fallback, /const dateable = Number\.isFinite\(snap\.ts\)/,
    "the fallback no longer establishes whether the snapshot is dateable");
  assert.match(fallback, /const fresh = hasPub && dateable &&/,
    "the fallback's freshness test no longer REQUIRES a dateable ts — it has gone back to failing open");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 5. A DIRECTOR THAT RESTARTS IS FOLLOWABLE AGAIN.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a director that restarts with a LOW seq is taken over by the relay and followed by everyone", async () => {
  // Two independent guards have to co-operate for this, and each looks correct alone.
  //
  // The relay: a stale room accepts anything, which is what lets a new director in and what
  // self-heals a seq poisoned by a director that is gone.
  // The follower (F4): demotion must RESET lastSeq to -1. Without it, the restarted director's low
  // seq reads as `lowSeq <= lastSeq` — a duplicate — forever on a perfectly healthy socket, and
  // every follower is stranded until somebody taps ⟳.
  const sim = new Sim({ seed: 9 });
  const first = await sim.addDirector({ id: "d1" });
  const followers = [0, 1, 2].map((i) => sim.addFollower({ id: `f${i}` }));
  await sim.settle();
  for (const f of followers) f.startHeartbeat(4000);

  first.turnTo(380);
  await sim.settle(2000);
  const highSeq = sim.relay.snapshot.seq;
  first.stepDown();

  // The room ages out and every follower demotes.
  await sim.idle(RELAY_LIVE_MAX_AGE_S * 1000 + 10000);
  await sim.settle(10000);
  for (const f of followers) assert.equal(f.hasDirector, false, `${f.id} never demoted, so this test proves nothing`);

  // A DIFFERENT iPad picks up the mesh — its own transmitter instance, its own seqCounter starting
  // at zero, and a clock ten minutes slow, so nextSeq() lands far below the seq still in the room.
  const restarted = await sim.addDirector({ id: "d2", skewMs: -10 * 60 * 1000 });
  restarted.turnTo(140);
  await sim.settle(5000);

  assert.ok(sim.relay.snapshot.seq < highSeq,
    "the restarted director's seq was not actually lower — the scenario did not reproduce");
  assert.equal(sim.relay.snapshot.page, 140, "the relay refused a restarted director in a stale room");
  for (const f of followers) {
    assert.equal(f.currentPage, 140, `${f.id} is stranded on the old director's page`);
    assert.equal(f.hasDirector, true, `${f.id} never saw the new director come up`);
  }
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 6. STEPPING DOWN IS TOTAL.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a director that steps down sends NOTHING further — including a page already in the coalescer", async () => {
  // Publishes are coalesced latest-wins, so at the moment a director steps down there can be a page
  // sitting in `pending` that has not gone out yet. It leaves as soon as the in-flight POST settles.
  //
  // This used to be survivable by accident: /publish required a code, so the straggler was rejected
  // 401 and never applied. /publish has been OPEN since 2026-08-06, so the straggler now SUCCEEDS —
  // shoving the ex-director's page onto every signovivo.com follower after they have already been
  // handed to somebody else. src/directorRelaySync.js:45-56 documents this exact failure and says
  // the refusal moved to a local gate; the gate was at the entry point only, and the coalescer's
  // drain path went around it.
  const sim = new Sim({ seed: 10 });
  const director = await sim.addDirector({ id: "d" });
  await sim.settle();

  // Wedge the first POST in flight so the second page is forced into the coalescer.
  let release;
  const held = new Promise((r) => { release = r; });
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => { await held; return realFetch(url, init); };

  director.turnTo(10);
  director.turnTo(11);          // coalesced into `pending`
  director.stepDown();          // ANOTHER DEVICE IS THE DIRECTOR NOW
  release();
  globalThis.fetch = realFetch;
  await sim.settle(2000);

  const pages = sim.relay.publishLog.map((p) => p.page);
  assert.deepEqual(pages, [10],
    `page ${pages.slice(1).join(",")} left the device AFTER it stepped down — ` +
    "an ex-director just moved the whole congregation");
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 7. THE COALESCER IS LATEST-WINS, AND A HUNG SOCKET CANNOT WEDGE IT.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a burst of page turns collapses to a few POSTs and always ends on the director's real page", async () => {
  // The property that keeps a weak link from queueing a backlog: intermediate pages may be dropped,
  // but the LAST page the relay receives is always where the director actually is. A coalescer that
  // kept the FIRST payload instead would leave the congregation an arbitrary distance behind.
  const sim = new Sim({ seed: 11, latencyMs: 50 });
  const director = await sim.addDirector({ id: "d" });
  const follower = sim.addFollower({ id: "f" });
  await sim.settle();

  for (let page = 1; page <= 100; page++) director.turnTo(page);
  await sim.settle(5000);

  const sent = sim.relay.publishLog.map((p) => p.page);
  assert.ok(sent.length < 10, `100 synchronous page turns produced ${sent.length} POSTs — the coalescer is not collapsing`);
  assert.equal(sent.at(-1), 100, "the relay's last page is not the director's actual page");
  assert.equal(follower.currentPage, 100, "the congregation ended up behind the director");
  sim.uninstall();
});

test("a black-holed network drains the coalescer instead of wedging it forever", async () => {
  // Parish wifi that associates but does not route: a bare fetch() never settles, so without the
  // AbortController `inFlight` stays true and every later page turn and keepalive only overwrites
  // `pending` and returns — freezing the whole web congregation on the last sent page for the rest
  // of Mass, with no self-heal even after connectivity comes back.
  //
  // THIS TEST USED TO BE THREE REGEXES over the module plus `assert.equal(aborted, false)`, and an
  // adversarial re-hunt showed it could not catch either regression that puts the outage back: the
  // signal not being handed to fetch at all (`signal: undefined`), or the timeout defanged to 700
  // seconds. Worse, `aborted === false` is satisfied MORE easily when the wiring is broken. So it
  // observes the drain instead: the timer is intercepted rather than waited out, which is what makes
  // a 7-second real timeout testable at all.
  const sim = new Sim({ seed: 12 });
  const director = await sim.addDirector({ id: "d" });
  await sim.settle();

  const simFetch = globalThis.fetch;          // the sim's transport, restored for the drain
  const realSetTimeout = globalThis.setTimeout;
  const delays = [];
  let signalWasHonoured = false;
  let blackHoled = 0;

  // Intercept EVERY long timer for the duration, not just the first. Latching on the first one is
  // what made an earlier version of this test hang: the drain schedules its own abort timer, and a
  // passed-through 7 s (or, under the defanged-timeout regression, 700 s) timer keeps the runner
  // alive that long. Recording the delay and firing immediately is what makes this testable.
  globalThis.setTimeout = (fn, ms, ...rest) => {
    if (typeof ms === "number" && ms > 1000) { delays.push(ms); return realSetTimeout(fn, 0); }
    return realSetTimeout(fn, ms, ...rest);
  };
  // Only the FIRST request falls down the hole. The page queued behind it must be able to leave
  // once the abort clears the coalescer — that departure is the behaviour under test.
  globalThis.fetch = (url, init) => {
    if (blackHoled++ > 0) return simFetch(url, init);
    return new Promise((_res, rej) => {
      if (!init.signal) return;   // no signal: genuinely unabortable, exactly the pre-fix behaviour
      init.signal.addEventListener("abort", () => { signalWasHonoured = true; rej(new Error("aborted")); });
    });
  };

  try {
    director.turnTo(50);                      // goes out, and hangs
    await new Promise((r) => realSetTimeout(r, 20));
    director.turnTo(51);                      // coalesced into `pending` behind the hung request
    await sim.settle(2000);
    await new Promise((r) => realSetTimeout(r, 20));
    await sim.settle(2000);
  } finally {
    globalThis.fetch = simFetch;
    globalThis.setTimeout = realSetTimeout;
  }

  const scheduledDelay = delays[0] ?? null;
  assert.ok(scheduledDelay !== null,
    "no abort timer was scheduled for a publish — a black-holed socket now wedges the coalescer for " +
    "the rest of Mass, and every later page turn only overwrites `pending` and returns");
  assert.ok(scheduledDelay <= 15000,
    `the publish abort is scheduled ${scheduledDelay} ms out — a director on dead-but-associated wifi ` +
    "would freeze the congregation for that long before the coalescer could drain");
  assert.equal(signalWasHonoured, true,
    "the AbortController's signal never reached fetch, so nothing can cancel a hung request — " +
    "this is the `signal: undefined` regression, and it reinstates the whole-Mass freeze");

  // The behaviour that matters: once the hung request is aborted, the page queued behind it must
  // actually leave.
  const reached = sim.relay.publishLog.map((p) => p.page);
  assert.ok(reached.includes(51),
    `after the black-holed request was aborted the coalescer did not drain — the relay saw ` +
    `${JSON.stringify(reached)}, and page 51 never left the device`);
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 8. UNDER LOSS AND REORDERING, NOBODY GOES BACKWARDS.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("under 20% loss and heavy jitter, no follower ever renders a page out of order", async () => {
  // Reordering is not simulated as a special case — it falls out of per-message jitter, which is how
  // it happens in a church. Two publishes 5 ms apart with 300 ms of jitter routinely arrive
  // backwards, and a late older one must never drag the congregation back a page.
  //
  // Randomised, but reproducible: the seed is in every failure message.
  for (const seed of [21, 22, 23, 24, 25]) {
    const sim = new Sim({ seed, latencyMs: 30, jitterMs: 300, lossRate: 0.2 });
    const director = await sim.addDirector({ id: "d" });
    const followers = [0, 1, 2, 3, 4, 5].map((i) => sim.addFollower({ id: `f${i}` }));
    await sim.settle();
    for (const f of followers) f.startHeartbeat(4000);

    for (let turn = 0; turn < 60; turn++) {
      director.turnTo(300 + turn);
      await sim.settle(3000);
    }
    await sim.settle(20000);

    const appliedSeqs = sim.relay.publishLog.filter((p) => p.applied).map((p) => p.seq);
    for (let i = 1; i < appliedSeqs.length; i++) {
      assert.ok(appliedSeqs[i] > appliedSeqs[i - 1],
        `seed ${seed}: the room's seq went backwards (${appliedSeqs[i - 1]} → ${appliedSeqs[i]})`);
    }
    const appliedPages = sim.relay.publishLog.filter((p) => p.applied).map((p) => p.page);
    for (const f of followers) {
      assert.ok(isSubsequence(f.pageHistory, appliedPages),
        `seed ${seed}: ${f.id} rendered ${JSON.stringify(f.pageHistory.slice(-8))}, which is not in the ` +
        `order the relay accepted (${JSON.stringify(appliedPages.slice(-8))})`);
      assert.ok(f.pageHistory.every((p) => appliedPages.includes(p)),
        `seed ${seed}: ${f.id} rendered a page the relay never accepted — a ghost page in front of the congregation`);
    }
    sim.uninstall();
  }
});

test("after the network settles, every follower is on the director's page — however lossy the Mass was", async () => {
  // Convergence is the property the congregation actually experiences. A system can preserve
  // ordering perfectly and still leave half the room a page behind forever.
  for (const seed of [31, 32, 33]) {
    const sim = new Sim({ seed, latencyMs: 40, jitterMs: 200, lossRate: 0.3 });
    const director = await sim.addDirector({ id: "d" });
    const followers = [0, 1, 2, 3, 4, 5].map((i) => sim.addFollower({ id: `f${i}`, skewMs: (i - 3) * 20000 }));
    await sim.settle();
    for (const f of followers) f.startHeartbeat(4000);

    for (let turn = 0; turn < 40; turn++) {
      director.turnTo(200 + turn);
      await sim.settle(4000);
    }
    // The director keeps the room alive while the last drops are retried, exactly as the real
    // keepalive does. Convergence is only meaningful at quiescence.
    for (let k = 0; k < 6; k++) { director.turnTo(239); await sim.settle(4000); }
    await sim.settle(20000);

    for (const f of followers) {
      assert.equal(f.currentPage, 239,
        `seed ${seed}: ${f.id} (skew ${f.skewMs / 1000}s) ended on ${f.currentPage} while the director was on 239`);
      assert.equal(f.revealed, true, `seed ${seed}: ${f.id} never revealed the reader — its screen stayed blank`);
    }
    sim.uninstall();
  }
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 9. THE RELAY STAMPS ITS OWN TIME AND CLAMPS ITS OWN PAGE.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("the relay ignores the client's ts and page, and stamps/clamps its own", async () => {
  // The transmitter SENDS a ts and the relay must completely ignore it — otherwise a fast-clocked
  // director looks permanently fresh and a slow one permanently stale, and the freshness window
  // stops meaning anything.
  //
  // The page clamp matters for a specific reason: a follower that fails to render sets its page
  // sentinel to -1. If that device then becomes director before a real page lands, a raw broadcast
  // of -1 would clamp to page 1 on every follower and yank the whole congregation to the front of
  // the book.
  const sim = new Sim({ seed: 13 });
  await sim.settle();
  const serverS = Math.floor(sim.now / 1000);

  const r = sim.relay.publish(
    { page: 370, seq: sim.now, ts: serverS + 9999, totalPages: 400, mode: "x".repeat(500), bookId: "y".repeat(500) },
    sim.now,
  );
  assert.equal(r.body.ignored, undefined);
  assert.equal(sim.relay.snapshot.ts, serverS, "the relay stored the CLIENT's timestamp");
  assert.equal(sim.relay.snapshot.mode.length, 64, "mode is not length-capped");
  assert.equal(sim.relay.snapshot.bookId.length, 64, "bookId is not length-capped");

  for (const [page, expected] of [[0, 1], [-1, 1], [-999, 1], ["abc", 1], [1e9, 100000], [370.7, 370.7]]) {
    sim.now += 1000;
    sim.relay.publish({ page, seq: sim.now }, sim.now);
    assert.equal(sim.relay.snapshot.page, expected, `page ${JSON.stringify(page)} was not clamped to ${expected}`);
    assert.ok(sim.relay.snapshot.page >= 1, "a page below 1 reached the congregation");
  }
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 10. A BROWSING FOLLOWER IS NEVER YANKED, AND CAN ALWAYS GET BACK.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a follower browsing the songbook is never pulled off their page, and lands exactly on the director when they ask", async () => {
  const sim = new Sim({ seed: 14 });
  const director = await sim.addDirector({ id: "d" });
  const browser = sim.addFollower({ id: "browsing" });
  const normal = sim.addFollower({ id: "normal" });
  await sim.settle();
  // The heartbeat is what carries a demotion to a follower that the director is no longer pushing
  // to. Without it a follower simply never hears anything again and keeps believing — which is
  // correct behaviour for a device receiving nothing, and would make the last third of this test
  // vacuous rather than red.
  browser.startHeartbeat(4000);
  normal.startHeartbeat(4000);

  director.turnTo(370);
  await sim.settle(2000);
  browser.browseTo(5);                       // tapped a song title and jumped

  for (const page of [371, 372, 373]) { director.turnTo(page); await sim.settle(2000); }

  assert.equal(browser.currentPage, 5, "a browsing follower was yanked off the page they chose");
  assert.equal(browser.livePage, 373, "the director's page was not tracked for 'volver a en vivo'");
  assert.equal(normal.currentPage, 373, "a normal follower did not track the director");

  browser.goLive();
  assert.equal(browser.currentPage, 373, "'volver a en vivo' did not land on the director's current page");
  assert.equal(browser.browsing, false);

  // And a browsing follower must be released when the director goes away — otherwise the go-live
  // bar keeps offering to return to somebody who is not there.
  browser.browseTo(9);
  director.stepDown();
  await sim.idle(RELAY_LIVE_MAX_AGE_S * 1000 + 10000);
  await sim.settle(10000);
  assert.equal(browser.browsing, false, "a browsing follower was never released when the director left");
  assert.equal(browser.hasDirector, false);
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 11. NOTHING ON ANY WIRE CAN THROW.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("no malformed frame can throw out of a follower's message handler", async () => {
  // A thrown exception here aborts a relay message handler mid-Mass, and the follower stops
  // receiving anything at all — a silent, permanent freeze on one device that looks like a network
  // problem. Every shape below is something a truncated proxy response, an older worker, or a
  // future protocol version could actually put on the wire.
  const sim = new Sim({ seed: 15 });
  const follower = sim.addFollower({ id: "f" });
  await sim.settle();

  const hostile = [
    null, undefined, 0, "", "not json", [], [1, 2, 3], true,
    {}, { page: 1 }, { seq: 1 }, { ts: 1 },
    { page: Infinity, seq: 1, ts: 1 }, { page: -Infinity, seq: 1, ts: 1 },
    { page: 1, seq: Infinity, ts: 1 }, { page: 1, seq: 1, ts: Infinity },
    { page: "370", seq: "1", ts: "1" },
    { page: 1, seq: 1, ts: 1, v: 999 },
    { page: 1, seq: 1, ts: 1, extra: { deeply: { nested: [1, 2, 3] } } },
    JSON.parse('{"__proto__":{"polluted":true},"page":1,"seq":1,"ts":1}'),
    { get page() { throw new Error("hostile getter"); } },
  ];
  // The label is built lazily inside the failure path. Building it eagerly would call
  // JSON.stringify on the throwing-getter case and blow up the assertion itself, reporting the
  // test's own message construction as if it were a defect in the code under test.
  const label = (snap, i) => { try { return `#${i} ${JSON.stringify(snap)}`; } catch { return `#${i} <unserializable>`; } };
  hostile.forEach((snap, i) => {
    try { follower.receiveRaw(snap); }
    catch (e) { assert.fail(`a relay frame threw out of the message handler: ${label(snap, i)} → ${e.message}`); }
  });
  assert.equal({}.polluted, undefined, "a snapshot polluted Object.prototype");

  // The same battery through the pure decision function, with a hostile ctx as well.
  for (const snap of hostile) {
    for (const c of [null, undefined, {}, { lastSeq: NaN, nowMs: NaN, maxAgeS: NaN }]) {
      assert.doesNotThrow(() => decideRelaySnapshot(snap, c));
    }
  }
  sim.uninstall();
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 12. A FOLLOWER WHOSE OWN CLOCK IS WRONG STILL FOLLOWS.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a follower with a badly wrong clock calibrates from /state and follows normally", async () => {
  // Freshness is judged against (deviceClock + clockOffsetMs), and that offset is calibrated in
  // exactly ONE place: the `now` field of a /state body. WebSocket pushes do not carry it. So a
  // device whose clock is minutes off — an iPad whose "Set Automatically" is disabled — reads every
  // push as stale and demotes, and nothing about a healthy socket ever recovers it.
  for (const skewMs of [-10 * 60 * 1000, -90 * 1000, 90 * 1000, 10 * 60 * 1000]) {
    const sim = new Sim({ seed: 16 });
    const director = await sim.addDirector({ id: "d" });
    const follower = sim.addFollower({ id: `f${skewMs}`, skewMs });
    await sim.settle();

    follower.pollState({ force: true });      // the boot poll, before any director exists
    await sim.settle(2000);

    director.turnTo(365);
    await sim.settle(3000);

    assert.equal(follower.currentPage, 365,
      `a follower whose clock is ${skewMs / 60000} minutes off did not follow a live director`);
    assert.equal(follower.hasDirector, true, `skew ${skewMs}: a live director was judged dead by the follower's own clock`);
    sim.uninstall();
  }
});

test("a follower can calibrate from an EMPTY room, before any director exists", async () => {
  // /state answers with `now` on every response, including the no-director snapshot. If calibration
  // required a live director, a badly-clocked device that boots before the director does would have
  // no way to correct itself and would demote the director's very first publish.
  const sim = new Sim({ seed: 17 });
  const follower = sim.addFollower({ id: "f", skewMs: 5 * 60 * 1000 });
  await sim.settle();
  assert.equal(sim.relay.snapshot.seq, 0, "the room is not actually empty");

  follower.pollState({ force: true });
  await sim.settle(2000);
  assert.ok(Math.abs(follower.clockOffsetMs + 5 * 60 * 1000) < 2000,
    `a follower could not calibrate against an empty room (offset ${follower.clockOffsetMs})`);

  const director = await sim.addDirector({ id: "d" });
  director.turnTo(12);
  await sim.settle(3000);
  assert.equal(follower.currentPage, 12, "the director's FIRST publish was demoted by an uncalibrated follower");
  sim.uninstall();
});
