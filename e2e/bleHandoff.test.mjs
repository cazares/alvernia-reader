import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const BEACON = fs.readFileSync("ios/SignoVivo/BlePageBeacon.swift", "utf8");
const MODULE = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

// WHY THIS FILE EXISTS. BLE is the only path that can show a follower the right page BEFORE the
// mesh finishes its ~10s first handshake, and it never once did — because the seq guard exists in
// TWO layers and only one of them knew about advertising sessions.
//
// Swift cannot run here, so this models both guards and plays a director handover through them.
// That is stronger than grepping for the fix: the model FAILS on the old logic and passes on the
// new, so it is testing the behaviour rather than the presence of a line.

// ── The two guards, transliterated ────────────────────────────────────────────
const CONTENTION_WINDOW = 4;
const makeBeacon = () => ({ nonce: "", seq: -1, recent: new Map() });
const beaconRecv = (b, adv, now = 0) => {
  // Contention: two live advertisers => abstain entirely.
  b.recent.set(adv.nonce, now);
  for (const [n, t] of b.recent) if (now - t > CONTENTION_WINDOW) b.recent.delete(n);
  if (b.recent.size > 1) return null;
  if (adv.nonce !== b.nonce) { b.nonce = adv.nonce; b.seq = -1; }   // new advertiser => rebase
  if (!(adv.seq > b.seq)) return null;                              // monotonic WITHIN a session
  b.seq = adv.seq;
  return { page: adv.page, seq: adv.seq, nonce: adv.nonce };
};

// DirectorSyncModule.onPage. `rebases` models the fix; false reproduces what shipped in 448-452.
const makeModule = () => ({ appliedSeq: -1, appliedNonce: "" });
const moduleRecv = (m, hit, rebases) => {
  if (!hit) return null;
  if (rebases && hit.nonce !== m.appliedNonce) { m.appliedNonce = hit.nonce; m.appliedSeq = -1; }
  if (!(hit.seq > m.appliedSeq)) return null;   // the guard that silently ate every page
  m.appliedSeq = hit.seq;
  return hit.page;
};

// A director advertises pages; seq ALWAYS restarts at 0 per launch (bleSeq = 0 in Swift).
const run = (rebases) => {
  const b = makeBeacon(), m = makeModule();
  const rendered = [];
  let clock = 0;
  const advertise = (nonce, pages) => pages.forEach((page, i) => {
    clock += 1;
    const out = moduleRecv(m, beaconRecv(b, { nonce, seq: i + 1, page }, clock), rebases);
    if (out !== null) rendered.push(out);
  });
  advertise("aaaa", [10, 11, 12, 13, 14]);   // director A: seq 1..5
  const afterA = rendered.length;
  clock += CONTENTION_WINDOW + 1;            // A goes quiet: no contention
  advertise("bbbb", [372]);                  // director B takes over: seq restarts at 1
  return { afterA, rendered, sawNewDirector: rendered.length > afterA };
};

test("a new director's page reaches the follower over BLE", () => {
  const r = run(true);
  assert.equal(r.afterA, 5, "the first director's pages did not render");
  assert.ok(r.sawNewDirector, "the NEW director's page never rendered — BLE cannot cover the mesh gap");
  assert.equal(r.rendered.at(-1), 372, `last rendered page was ${r.rendered.at(-1)}, expected 372`);
});

test("THE BUG: without the module-level rebase, the handover is silently dropped", () => {
  // Proves this test can fail. Under the shipped logic the beacon rebased and handed up seq 1,
  // the module compared 1 > 5, and the page vanished with no error and no log — identical from the
  // outside to BLE being switched off. It stayed dropped until the new director's seq climbed past
  // the old one, i.e. for about as many page turns as the previous director had made.
  const r = run(false);
  assert.equal(r.afterA, 5, "the first director's pages did not render even in the broken model");
  assert.equal(r.sawNewDirector, false,
    "the broken model rendered the handover — the model no longer reproduces the bug it guards");
});

test("both layers carry the nonce, so neither can drift from the other", () => {
  // The 448 fix landed in the beacon only. The contract is now explicit in the callback signature,
  // which is what makes a repeat of that half-landing a compile error rather than a silent regression.
  assert.match(BEACON, /var onPage: \(\(Int, Int, String\) -> Void\)\?/,
    "onPage no longer carries the nonce — a caller can go back to guessing across sessions");
  assert.match(BEACON, /onPage\?\(parsed\.page, parsed\.seq, parsed\.nonce\)/, "the nonce is not passed through");
  assert.match(BEACON, /if parsed\.nonce != lastSeenNonce/, "the beacon no longer rebases on a new advertiser");

  assert.match(MODULE, /private var bleAppliedNonce = ""/, "the module tracks no advertising session");
  assert.match(MODULE, /onPage = \{ \[weak self\] page, seq, nonce in/, "the module ignores the nonce again");
  const handler = MODULE.slice(MODULE.indexOf("self.bleBeacon.onPage = {"), MODULE.indexOf("A FOLLOWER MUST NEVER ADVERTISE"));
  assert.match(handler, /if nonce != self\.bleAppliedNonce \{[\s\S]*?self\.bleAppliedSeq = -1/,
    "the module does not reset its seq on a new advertiser — the exact half-landing this fixes");
  // Order matters: rebase must happen BEFORE the monotonic guard, or it changes nothing.
  assert.ok(handler.indexOf("bleAppliedSeq = -1") < handler.indexOf("guard seq > self.bleAppliedSeq"),
    "the rebase runs AFTER the monotonic guard, so the guard still rejects the first page");
});

test("the safety rules that made BLE dangerous in 444 are still in place", () => {
  // BLE renders without a handshake, so it has no freshness guarantee of its own. Two rules keep it
  // safe, and a rebase must not quietly remove either.
  const handler = MODULE.slice(MODULE.indexOf("self.bleBeacon.onPage = {"), MODULE.indexOf("A FOLLOWER MUST NEVER ADVERTISE"));
  assert.match(handler, /guard !self\.lastKnownBookId\.isEmpty/,
    "BLE can render a page number with no known book — the one unrecoverable failure in this app");
  assert.match(handler, /guard seq > self\.bleAppliedSeq/,
    "the within-session monotonic guard is gone — a stale packet can drag a follower backwards");
  // Legacy 2-field advertisements must stay rejected: pre-448 devices never stop advertising.
  assert.match(BEACON, /guard parts\.count == 3/,
    "legacy 2-field beacons are accepted again — those devices broadcast a frozen page forever");
  // And a device that stops directing must stop advertising.
  assert.match(MODULE, /bleBeacon\.stopPublishing\(\)/, "the beacon is never switched off");
});


test("TWO LIVE ADVERTISERS: render NEITHER, rather than flapping or trusting the wrong one", () => {
  // The edge case the nonce rebase OPENED. Before it, the stale seq guard suppressed the second
  // advertiser by accident; after it, alternating packets each rebase the floor and every one
  // applies — the follower ping-pongs between two pages, several times a second.
  //
  // The tempting fix, preferring whichever advertiser was seen first, is WORSE: a backgrounded or
  // crashed director keeps advertising, and stickiness would pin every follower to its dead page
  // permanently. That is the 444 wrong-song flash turned into a wrong-song hold.
  //
  // BLE carries no authority — nothing in the packet proves the sender is the director — so the
  // only safe answer under contention is silence. It costs a few seconds of acceleration and the
  // mesh resolves the conflict by token anyway.
  const b = makeBeacon(), m = makeModule();
  const rendered = [];
  let clock = 0;
  for (let i = 1; i <= 6; i++) {           // A and B interleaved, both live
    clock += 1;
    for (const [nonce, page] of [["aaaa", 100], ["bbbb", 372]]) {
      const out = moduleRecv(m, beaconRecv(b, { nonce, seq: i, page }, clock), true);
      if (out !== null) rendered.push(out);
    }
  }
  assert.deepEqual(rendered, [100],
    `contested advertisers rendered ${JSON.stringify(rendered)} — expected at most the first, then silence`);

  // And once the loser goes quiet, the survivor is adopted rather than being locked out forever.
  clock += CONTENTION_WINDOW + 1;
  const out = moduleRecv(m, beaconRecv(b, { nonce: "bbbb", seq: 7, page: 372 }, clock), true);
  assert.equal(out, 372, "after contention clears, the remaining advertiser is still ignored");
});

test("the advertised seq is bounded by page turns, not by wall-clock seconds", () => {
  // sendPageUpdate runs at 1 Hz from the director heartbeat. Passing ITS counter meant the number
  // in a fixed-size BLE name grew all Mass — ~5400 after 90 minutes and never resetting — in a
  // 31-byte advertisement already carrying a 128-bit service UUID.
  assert.match(BEACON, /private var advertSeq = 0/, "the beacon does not own its seq");
  assert.match(BEACON, /func publish\(page: Int\)/, "publish still takes a caller-supplied seq");
  const pub = BEACON.slice(BEACON.indexOf("func publish(page: Int)"));
  const body = pub.slice(0, pub.indexOf("\n  }"));
  assert.ok(body.indexOf("guard page != lastPublishedPage") < body.indexOf("advertSeq += 1"),
    "seq increments even when the page has not changed — that is the unbounded growth again");
  assert.match(MODULE, /publish\(page: self\.currentPageNumber \?\? page\.intValue\)/,
    "the module still passes its own seq");
  assert.doesNotMatch(MODULE, /private var bleSeq/, "the module's per-second counter is back");
  // A new session must restart the count, or a re-promoted director resumes a stale ladder.
  const stop = BEACON.slice(BEACON.indexOf("func stopPublishing()"));
  assert.match(stop.slice(0, stop.indexOf("\n  }")), /advertSeq = 0/, "stopPublishing does not reset the seq");
});


test("BLE self-heals if the radio goes quiet without telling us", () => {
  // THE FAILURE CLASS: both sides can stop working with no callback, no error, and no way to notice
  // from inside the app. A director's advertisement is only ever (re)started by publish(), which
  // early-returns on an unchanged page — so a director that went dark stayed dark until it happened
  // to turn a page. A follower's scan is guarded by a LOCAL bool, so if iOS stopped the scan the
  // guard refused to restart it and the device was deaf while believing it was listening.
  assert.match(BEACON, /func ensureAdvertising\(\)/, "nothing re-asserts a lost advertisement");
  assert.match(BEACON, /func ensureScanning\(\)/, "nothing re-asserts a lost scan");

  // Must ask the FRAMEWORK, not our own memory — the local flag is exactly what lies here.
  const adv = BEACON.slice(BEACON.indexOf("func ensureAdvertising()"));
  assert.match(adv.slice(0, adv.indexOf("\n  }")), /!p\.isAdvertising/,
    "ensureAdvertising trusts a local flag instead of CBPeripheralManager.isAdvertising");
  const scan = BEACON.slice(BEACON.indexOf("func ensureScanning()"));
  const scanBody = scan.slice(0, scan.indexOf("\n  }"));
  assert.match(scanBody, /!c\.isScanning/,
    "ensureScanning trusts the local isScanning bool — the very flag that goes stale");
  assert.match(scanBody, /isScanning = false/,
    "the stale local flag is not cleared, so scanIfReady's guard still blocks the restart");

  // And both must be DRIVEN by something that ticks regardless of page changes.
  assert.match(MODULE, /bleBeacon\.ensureAdvertising\(\)/, "the director never re-asserts advertising");
  assert.match(MODULE, /bleBeacon\.ensureScanning\(\)/, "the follower never re-asserts scanning");
  // The follower hook belongs in the watchdog, which runs while HUNTING — the state that needs it.
  const wd = MODULE.slice(MODULE.indexOf("private func startFollowerWatchdog"));
  assert.match(wd.slice(0, 2500), /bleBeacon\.ensureScanning\(\)/,
    "ensureScanning is not on the follower watchdog, so it does not run while hunting");
});

test("a follower scans continuously — BLE is never switched off by connecting", () => {
  // If connecting to the mesh stopped the scan, BLE would cover the first gap and then be dead for
  // every later one — a wedged session, a director restart, a follower that drops. It is the
  // fallback precisely for the moments the mesh is not working.
  assert.doesNotMatch(MODULE, /bleBeacon\.stopScanning\(\)/,
    "something now stops scanning — BLE would stop covering everything after the first connection");
});
