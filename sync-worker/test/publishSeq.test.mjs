import assert from "node:assert/strict";
import test from "node:test";

import { decidePublish, sanitizeSeq, NO_DIRECTOR_SEQ, SEQ_FUTURE_TOLERANCE_MS } from "../src/publishSeq.js";

const MAX_AGE_S = 90;
const NOW = 1787060000000;
const live = (seq) => ({ snapshotSeq: seq, snapshotTs: Math.floor(NOW / 1000) });
const stale = (seq) => ({ snapshotSeq: seq, snapshotTs: Math.floor(NOW / 1000) - (MAX_AGE_S + 1) });
const decide = (rawSeq, snap, nowMs = NOW) =>
  decidePublish({ rawSeq, nowMs, ...snap, maxAgeS: MAX_AGE_S });

test("a normal page turn from a live director advances the room", () => {
  const d = decide(NOW + 1, live(NOW));
  assert.equal(d.apply, true);
  assert.equal(d.seq, NOW + 1);
});

test("an out-of-order burst cannot rewind the song", () => {
  // The reason the monotonic guard exists: page turns on a weak link can arrive reordered, and a
  // late older one must never drag the choir back a page.
  const d = decide(NOW - 5000, live(NOW));
  assert.equal(d.apply, false);
  assert.equal(d.reason, "seq_not_newer");
  assert.equal(d.seq, NOW, "the room's seq must be unchanged by a refused publish");
});

test("the reserved 0 is refused while a director is live", () => {
  const d = decide(0, live(NOW));
  assert.equal(d.apply, false);
  assert.equal(d.reason, "seq_reserved");
});

test("a stale room accepts anything — that is how a NEW director takes over", () => {
  // Also the self-heal for a poisoned seq left behind by a director that is gone: without it, a
  // too-high stale seq would silently block every future director forever.
  const d = decide(0, stale(999999999999999));
  assert.equal(d.apply, true);
  assert.equal(d.reason, "takeover");
  assert.equal(d.seq, 999999999999999 + 1);
});

test("a new director publishing a LOWER seq takes over a stale room", () => {
  const d = decide(NOW - 10 * 60 * 1000, stale(NOW));
  assert.equal(d.apply, true, "a restarted director must be able to take a room nobody is holding");
});

test("Infinity and NaN collapse to the reserved value, never out-ranking a real director", () => {
  // Infinity serializes as null and, since every finite seq is <= Infinity, would block every future
  // director for the whole live window.
  for (const bad of [Infinity, NaN, -1, "nonsense"]) {
    assert.equal(sanitizeSeq(bad, NOW, 5), NO_DIRECTOR_SEQ, `${String(bad)} must not survive`);
    assert.equal(decide(bad, live(5)).apply, false, `${String(bad)} must not be applied while live`);
  }
});

// ── THE FROZEN-CONGREGATION WEDGE ────────────────────────────────────────────────────────────
// The transmitter's seq is the DEVICE's wall clock, so a director whose clock runs fast trips the
// future-ceiling on every single publish. Folding those to 0 met the reserved-value gate above, and
// the two rules wedged each other: one publish landed per ~90 s and every other page turn returned
// {ok: true} with nothing changed — no banner, no breadcrumb, green pill, stale page.

test("a director with a fast clock is clamped, not collapsed", () => {
  const skewed = NOW + 120000; // device 2 minutes ahead
  const seq = sanitizeSeq(skewed, NOW, 10);
  assert.notEqual(seq, NO_DIRECTOR_SEQ, "a real seq was folded into the reserved value");
  assert.equal(seq, NOW + SEQ_FUTURE_TOLERANCE_MS, "clamped to the ceiling under the SERVER's clock");
});

test("two publishes clamped in the same server millisecond still advance", () => {
  // Both clamp to the same ceiling, so without the currentSeq + 1 floor the second would be refused
  // by the monotonic guard.
  const ceiling = NOW + SEQ_FUTURE_TOLERANCE_MS;
  assert.equal(sanitizeSeq(NOW + 120000, NOW, ceiling), ceiling + 1);
});

test("every page turn from a fast-clocked director reaches the congregation", () => {
  // The regression test proper: 20 page turns, 5 s apart, from a device 2 minutes fast. Before the
  // clamp this applied 2 of 20 and silently ignored 18.
  let snapshotSeq = 0;
  let snapshotTs = 0;
  let applied = 0;
  let nowMs = NOW;
  for (let turn = 0; turn < 20; turn++) {
    nowMs += 5000;
    const d = decidePublish({
      rawSeq: nowMs + 120000,
      nowMs,
      snapshotSeq,
      snapshotTs,
      maxAgeS: MAX_AGE_S,
    });
    if (d.apply) {
      applied++;
      snapshotSeq = d.seq;
      snapshotTs = Math.floor(nowMs / 1000);
    }
  }
  assert.equal(applied, 20, "a fast device clock still freezes the web followers");
});

test("a fast-clocked director does not permanently lock out a normal one", () => {
  // The clamp must not poison the room: once the fast director stops and the snapshot goes stale, a
  // device with a correct clock has to be able to take over.
  const ceiling = NOW + SEQ_FUTURE_TOLERANCE_MS;
  const later = NOW + 10 * 60 * 1000;
  const d = decidePublish({
    rawSeq: later,
    nowMs: later,
    snapshotSeq: ceiling,
    snapshotTs: Math.floor(NOW / 1000),
    maxAgeS: MAX_AGE_S,
  });
  assert.equal(d.apply, true, "a normal director cannot take the room back");
});
