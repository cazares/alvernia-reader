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
  assert.equal(seq, NOW, "clamped to the SERVER's now");
});

test("the clamp lands on server-now, never a minute in the future", () => {
  // Clamping to the ceiling (now + tolerance) parks the room ahead of every honest clock, so the
  // NEXT director's correct seq reads as "not newer" and its page turns are refused for up to a
  // minute. That is the same poisoning this function exists to prevent, just smaller.
  const seq = sanitizeSeq(NOW + 10 * 60 * 1000, NOW, 10);
  assert.ok(seq <= NOW, `clamp left the room ${seq - NOW} ms in the future`);
});

test("two publishes clamped in the same server millisecond still advance", () => {
  // Both clamp to the same instant, so without the currentSeq + 1 floor the second would be refused
  // by the monotonic guard.
  assert.equal(sanitizeSeq(NOW + 120000, NOW, NOW), NOW + 1);
});

test("a normal director takes over immediately after a fast-clocked one", () => {
  // The regression the ceiling clamp introduced: the room held now+60s, so a correct-clock
  // director's every publish was refused as not-newer until real time caught up.
  const roomSeq = sanitizeSeq(NOW + 120000, NOW, 10);   // fast director's last publish
  const handover = NOW + 1000;                          // one second later, correct clock
  const d = decidePublish({
    rawSeq: handover, nowMs: handover,
    snapshotSeq: roomSeq, snapshotTs: Math.floor(NOW / 1000), maxAgeS: MAX_AGE_S,
  });
  assert.equal(d.apply, true, "a correct-clock director is locked out by the previous one's clamp");
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

test("a SMALL clock skew parks the room in the future just as surely as a large one", () => {
  // The trap that survived two earlier versions of the clamp. Anything above the server's clock
  // locks out the next director for exactly that long, and a device whose time was set by hand is
  // typically seconds-to-minutes fast — the ORDINARY case, not the extreme one. A 45 s skew sat
  // comfortably under the 60 s tolerance and was passed through untouched.
  for (const skewMs of [1000, 45000, SEQ_FUTURE_TOLERANCE_MS - 1, SEQ_FUTURE_TOLERANCE_MS + 1, 10 * 60 * 1000]) {
    const stored = sanitizeSeq(NOW + skewMs, NOW, 10);
    assert.ok(stored <= NOW, `a ${skewMs} ms skew left the room ${stored - NOW} ms ahead of the server`);
  }
});

test("a handover after ANY skew is accepted on the next page turn", () => {
  for (const skewMs of [1000, 45000, 120000]) {
    const roomSeq = sanitizeSeq(NOW + skewMs, NOW, 10);       // fast director's last publish
    const handover = NOW + 3000;                              // 3 s later, correct clock
    const d = decidePublish({
      rawSeq: handover, nowMs: handover,
      snapshotSeq: roomSeq, snapshotTs: Math.floor(NOW / 1000), maxAgeS: MAX_AGE_S,
    });
    assert.equal(d.apply, true, `after a ${skewMs} ms skew the next director is still locked out`);
  }
});

test("a SLOW clock is left alone — it is already orderable and must not be promoted", () => {
  const behind = NOW - 30000;
  assert.equal(sanitizeSeq(behind, NOW, 10), behind,
    "a slow-clocked device's seq was pushed up, handing it an unearned advantage over a correct one");
});
