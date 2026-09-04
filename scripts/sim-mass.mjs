#!/usr/bin/env node
// sim-mass.mjs — run a whole Mass through the REAL sync modules with N followers, and report what a
// person in the pews would have seen.
//
// This is not a unit test. It is the closest thing this repo has to a multi-device rehearsal that
// does not need iPads: e2e/helpers/sv-sync-sim.mjs wires the real transmitter
// (src/directorRelaySync.js), the real relay rule (sync-worker/src/publishSeq.js) and the real
// follower decision (web/src/lib/svSyncDecision.js) — the same code baked into the native WebBundle
// and served at signovivo.com — over a lossy, jittery network with skewed device clocks, and runs a
// service against a virtual clock. Nothing here decides anything; every branch is the real module.
//
// What it CANNOT cover, said plainly: the Multipeer mesh and the BLE beacon are Swift and run only on
// hardware. This exercises the relay path (signovivo.com followers, and the director's transmitter).
//
// FIDELITY NOTES — things the first version got wrong about the real app, each of which produced a
// "violation" that was the harness's fault, not the code's:
//   • Every real follower does a forced /state poll at BOOT (relayPollOnce's "boot poll"), and that
//     poll is the only path that calibrates the device clock. Without it a +90 s fast-clock follower
//     is deaf for the whole service. Modelled: pollState({force:true}) right after connect.
//   • The real director re-publishes its current page every 30 s (the relay keepalive in
//     startDirectorHeartbeat). A publish lost to packet loss is therefore a ≤30 s lag, not a held
//     page. Modelled: a 30 s keepalive per live director. The final settle is longer than one tick.
//   • A follower that BROWSES renders a page the director never published. That is the user, not a
//     sync error; browsed pages are excluded from the "only published pages" invariant.
//
//   node scripts/sim-mass.mjs                       # 8 followers, 3 seeds
//   node scripts/sim-mass.mjs --followers 12 --seeds 10 --loss 0.08 --jitter 400
//
// Exit code is non-zero if ANY invariant is violated in ANY run. Every violation names the follower,
// the seed, and the invariant, so a failure is reproducible from its own message.
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Sim, RELAY_LIVE_MAX_AGE_S } from "../e2e/helpers/sv-sync-sim.mjs";

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.chdir(REPO);

const arg = (n, d) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? process.argv[i + 1] : d; };
const N = Number(arg("followers", 8));
const SEEDS = Number(arg("seeds", 3));
const LOSS = Number(arg("loss", 0.05));
const JITTER = Number(arg("jitter", 250));
const TOTAL_PAGES = 372;
const KEEPALIVE_MS = 30_000;   // startDirectorHeartbeat's relay tick, PdfReaderApp.tsx
const RELAY_POLL_MS = 15_000;  // startRelayPolling's forced /state poll, web/src/app.js
const NO_POLL = process.argv.includes("--no-poll");   // omit the 15 s poll (to reproduce the fidelity gap)

/** A rendered sequence must be a SUBSEQUENCE of what was published: pages may be missed (a drop, a
 *  coalesced burst) but never invented and never reordered. */
function isSubsequence(sub, sup) {
  let i = 0;
  for (const x of sup) if (i < sub.length && sub[i] === x) i++;
  return i === sub.length;
}

const ONLY_SEED = arg("only-seed", null);   // re-run one seed with diagnostics, e.g. --only-seed 7

const violations = [];
const fail = (seed, who, what) => violations.push(`seed ${seed} · ${who} · ${what}`);

/** When a follower violates an invariant, print what it actually saw — enough to tell a harness
 *  fidelity gap from a real decision bug without re-running under a debugger. */
const diagnose = (sim, f, published) => {
  const last = (a, n) => JSON.stringify(a.slice(-n));
  console.log(`   ↳ ${f.id}: skew ${f.skewMs} ms · clockOffset ${f.clockOffsetMs} ms · lastSeq ${f.lastSeq} vs relay seq ${sim.relay.snapshot.seq} (relay page ${sim.relay.snapshot.page})`);
  console.log(`      hasDirector ${f.hasDirector} · following ${f.following} · browsing ${f.browsing} · polls ${f.pollCount || 0} · pings sent ${f.heartbeat?.sent ?? "n/a"} · rejects ${f.rejects} · demotions ${f.demotions}`);
  console.log(`      pages ${last(f.pageHistory, 6)} · pills ${last(f.pillHistory, 6)} · published tail ${last(published, 4)}`);
  // Where exactly does the rendered sequence stop being a subsequence of what was published?
  let i = 0, j = 0;
  while (i < f.pageHistory.length && j < published.length) { if (f.pageHistory[i] === published[j]) i++; j++; }
  if (i < f.pageHistory.length) {
    console.log(`      ✖ diverges at rendered[${i}] = ${f.pageHistory[i]} — published (tried) = ${JSON.stringify(published)}`);
    console.log(`        relay APPLIED pages = ${JSON.stringify(sim.relay.publishLog.filter((e) => e.applied).map((e) => e.page))}`);
    console.log(`        rendered           = ${JSON.stringify(f.pageHistory)}`);
  }
};

for (let seed = ONLY_SEED ? Number(ONLY_SEED) : 1; seed <= (ONLY_SEED ? Number(ONLY_SEED) : SEEDS); seed++) {
  const sim = new Sim({ seed, latencyMs: 40, jitterMs: JITTER, lossRate: LOSS });

  const joinFollower = (id, skewMs) => {
    const f = sim.addFollower({ id, skewMs });
    f.startHeartbeat();
    f.pollState({ force: true });   // the real boot poll — the ONLY path that calibrates the clock
    // startRelayPolling, web/src/app.js: `setInterval(() => relayPollOnce(true), RELAY_POLL_MS)` with
    // RELAY_POLL_MS = 15000. A FORCED /state poll every 15 s — it re-homes a follower that missed a
    // frame and recalibrates the clock of one whose boot poll was lost. Without modelling this, an
    // 8%-loss run showed a +90 s follower deaf all Mass and another one page behind at the end; both
    // were the harness lacking a mechanism the real app has. --no-poll reproduces that for the record.
    if (!NO_POLL) {
      const poll = () => { if (f.socket) { f.pollState({ force: true }); sim.at(RELAY_POLL_MS, poll, `poll:${id}`); } };
      sim.at(RELAY_POLL_MS, poll, `poll:${id}`);
    }
    return f;
  };

  // The congregation: N signovivo.com followers with realistic clock skew. One is badly fast — the
  // exact device class that froze the web congregation before publishSeq.js existed.
  const followers = [];
  for (let i = 0; i < N; i++) {
    const skewMs = i === 0 ? 90_000 : i === 1 ? -45_000 : Math.round((sim.rand() - 0.5) * 20_000);
    followers.push(joinFollower(`f${i}`, skewMs));
  }
  await sim.settle(3_000);

  // A director with the real 30 s relay keepalive: re-publish the current page until stepped down.
  const published = [];
  let page = 12;
  const live = { d: null, page: null };
  const turn = (d, p) => { published.push(p); live.d = d; live.page = p; d.turnTo(p); };
  const keepalive = () => {
    if (live.d && live.page != null) live.d.turnTo(live.page);   // same page: the module refreshes ts, publishes no new turn
    sim.at(KEEPALIVE_MS, keepalive, "director-keepalive");
  };
  sim.at(KEEPALIVE_MS, keepalive, "director-keepalive");

  // Director A, correct clock. Turns pages at human pace through the first half of the service.
  const A = await sim.addDirector({ id: "A", skewMs: 0, totalPages: TOTAL_PAGES });
  for (let k = 0; k < 18; k++) { page += 1 + Math.floor(sim.rand() * 3); turn(A, page); await sim.settle(8_000 + sim.rand() * 20_000); }

  // Two followers lose signal for a minute (a phone in a pocket, an iPad walked out of range) while A
  // keeps turning, then come back. They must re-home without a human touching anything.
  const away = [followers[2], followers[3]];
  for (const f of away) sim.network.partition(f.id);
  for (let k = 0; k < 4; k++) { page += 2; turn(A, page); await sim.settle(15_000); }
  for (const f of away) sim.network.heal(f.id);
  await sim.settle(12_000);

  // A late joiner opens the app mid-service and must land on the live page.
  const late = joinFollower("late", 5_000);
  followers.push(late);
  await sim.settle(6_000);

  // HANDOVER. A steps down (a phone call, a dead battery); B takes the seat with a clock that TRAILS
  // A's by 100 s. The relay accepts the lower seq once A's snapshot has aged past the freshness
  // window; the forced poll is the rescue. This is the #391 scenario: before the fix, every web
  // follower rendered exactly ONE frame from B and then judged every later turn a duplicate.
  A.stepDown();
  live.d = null;                                          // A's keepalive stops with A
  const handoverIndex = published.length;                 // everything from here on is B's
  await sim.idle((RELAY_LIVE_MAX_AGE_S + 5) * 1000);      // A's snapshot ages out; followers demote + force-poll
  const B = await sim.addDirector({ id: "B", skewMs: -100_000, totalPages: TOTAL_PAGES });
  for (let k = 0; k < 12; k++) { page += 1 + Math.floor(sim.rand() * 2); turn(B, page); await sim.settle(8_000 + sim.rand() * 15_000); }

  // One follower browses ahead on their own, then taps "volver a en vivo".
  const browser = followers[4];
  const browsedPage = page + 40;
  browser.browseTo(browsedPage);
  turn(B, page += 1); await sim.settle(6_000);
  browser.goLive();
  await sim.settle(3_000);

  // Final page, then let everything drain — longer than one keepalive, so a lost final publish is
  // re-sent the way the real 30 s tick re-sends it.
  turn(B, page += 1);
  await sim.settle(KEEPALIVE_MS + 20_000);
  const finalPage = page;
  const bPages = published.slice(handoverIndex);

  // ── what the pews saw ──
  // The reference is what the RELAY APPLIED, not what the director tried: the transmitter's latest-wins
  // coalescer legitimately drops an intermediate page when two turns overlap in flight (page 82 in one
  // run), and no follower can render a page the relay never had.
  const applied = sim.relay.publishLog.filter((e) => e.applied).map((e) => e.page);
  const before = violations.length;
  for (const f of followers) {
    const mine = violations.length;
    const rendered = f === browser ? f.pageHistory.filter((p) => p !== browsedPage) : f.pageHistory;
    if (!isSubsequence(rendered, applied)) {
      fail(seed, f.id, `rendered a page the relay never applied, or out of order: ${JSON.stringify(rendered.slice(-8))}`);
    }
    if (f.currentPage !== finalPage) {
      fail(seed, f.id, `ended on page ${f.currentPage}, director is on ${finalPage} (skew ${f.skewMs} ms)`);
    }
    if (!f.hasDirector) fail(seed, f.id, "ended with NO director while B is live — pill would be off/grey");
    const lastPill = f.pillHistory[f.pillHistory.length - 1];
    if (lastPill !== "live") fail(seed, f.id, `pill ended "${lastPill}", expected "live"`);
    // Everyone must have followed B: a follower that saw only A's pages held a stale page for the
    // whole second half of the service (the #391 freeze).
    if (!f.pageHistory.some((p) => bPages.includes(p))) fail(seed, f.id, "never rendered a page from director B — held A's last page for the second half");
    if (violations.length > mine) diagnose(sim, f, published);
  }
  if (violations.length > before) console.log(`   (seed ${seed}: ${violations.length - before} violation(s) above — diagnostics printed per follower)`);
  sim.uninstall();

  const drops = sim.network.dropped, delivered = sim.network.delivered;
  console.log(`seed ${seed}: ${followers.length} followers · ${published.length} turns · ${delivered} msgs delivered, ${drops} dropped (${(100 * drops / (drops + delivered)).toFixed(1)}%) · relay applied ${applied.length}/${sim.relay.publishLog.length} publishes (turns + keepalives)`);
}

console.log();
if (violations.length) {
  console.log(`❌ ${violations.length} invariant violation(s):`);
  for (const v of violations) console.log("   " + v);
  process.exit(1);
}
console.log(`✅ every follower in every run ended on the director's page, live, having rendered only published pages in order — ${N}(+1 late) followers × ${SEEDS} seeds, loss ${LOSS}, jitter ${JITTER} ms.`);
