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
const makeBeacon = () => ({ nonce: "", seq: -1 });          // BlePageBeacon (rebases since build 448)
const beaconRecv = (b, adv) => {
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
  const advertise = (nonce, pages) => pages.forEach((page, i) => {
    const out = moduleRecv(m, beaconRecv(b, { nonce, seq: i + 1, page }), rebases);
    if (out !== null) rendered.push(out);
  });
  advertise("aaaa", [10, 11, 12, 13, 14]);   // director A: seq 1..5
  const afterA = rendered.length;
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
