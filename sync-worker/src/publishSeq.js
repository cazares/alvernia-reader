// Publish sequencing — the rule that decides whether a director's page turn reaches the
// congregation, extracted to plain JS for the same reason as bookArming.js and logBuffer.js: it is
// load-bearing, it has no runtime dependencies, and it had a bug that node --test would have caught
// on the day it shipped.
//
// WHAT WENT WRONG. The native transmitter's seq IS the device's wall clock in milliseconds
// (src/directorRelaySync.js: `Math.max(seqCounter + 1, Date.now())`). The worker folded any seq more
// than a minute past its OWN clock down to 0 — and 0 is the RESERVED value meaning "nobody has
// published", which the freshness gate then rejects while a director is live. The two rules wedged
// each other for any director whose clock ran fast:
//
//   first publish   snapshot is stale, so seq 0 is allowed through, ts refreshed
//   next 90 s       every page turn and every keepalive "ignored", ts never refreshed
//   then            exactly one more lands, and the cycle repeats
//
// Measured by simulation at a 2-minute skew and a page turn every 5 s: 2 of 20 turns applied. Every
// rejected publish returned {ok: true}, so the director's app showed nothing wrong and the pill
// stayed green while signovivo.com followers sat on a stale page.

/** Reserved: "no director has published in this room yet." */
export const NO_DIRECTOR_SEQ = 0;

/** Kept for callers/tests that reason about "how far ahead is obviously wrong". The clamp itself no
 *  longer uses a tolerance: any seq above the server's clock is anchored to it, because even a few
 *  seconds of overshoot locks out the next director for exactly that long. */
export const SEQ_FUTURE_TOLERANCE_MS = 60000;

/**
 * Coerce an incoming seq into a usable one.
 *
 * Non-finite (Infinity/NaN — Infinity serializes as null and would out-rank every future director
 * forever) and negative values collapse to the reserved 0. A finite value that is merely AHEAD of
 * the server is a real seq from a device with a fast clock, so it is anchored to the server's clock
 * rather than destroyed: that keeps the director monotonic without parking the room in the future,
 * and the `currentSeq + 1` floor keeps two publishes inside the same server millisecond distinct.
 */
export function sanitizeSeq(rawSeq, nowMs, currentSeq) {
  const seq = Number(rawSeq ?? 0);
  if (!Number.isFinite(seq) || seq < 0) return NO_DIRECTOR_SEQ;
  if (seq > nowMs) {
    // THE ROOM'S SEQ MAY NEVER EXCEED THE SERVER'S CLOCK.
    //
    // Two earlier versions of this line were softer and both left the same trap. Collapsing an
    // over-ceiling seq to the reserved 0 froze the fast director's own publishes. Clamping to the
    // ceiling (now + tolerance) parked the room a full minute ahead instead. And clamping only
    // ABOVE the tolerance left every skew under it untouched — a director 45 s fast stores
    // now + 45000, which is not the extreme case, it is the ORDINARY one: a device whose clock was
    // set by hand. The next director's correct-clock seq then reads as "not newer" and every page
    // it turns is refused with {ok: true, ignored: true}, which the transmitter treats as success.
    // Web followers freeze with a green pill for as long as the skew lasted.
    //
    // Anchoring to server-now removes the trap at every magnitude: the ordering the seq provides is
    // by the one clock all publishers share, and the currentSeq + 1 floor keeps two publishes inside
    // the same server millisecond distinct. A device whose clock is SLOW is left alone — its seq is
    // below now, already orderable, and clamping it up would hand it an unearned advantage over a
    // correct-clock director.
    return Math.max(nowMs, currentSeq + 1);
  }
  return seq;
}

/**
 * Should this publish be applied, and with what seq?
 *
 * Returns { apply, seq, reason }. A stale snapshot (nobody live) accepts anything, which is what
 * lets a NEW director take over and what self-heals a poisoned seq left behind by a gone one.
 */
export function decidePublish({ rawSeq, nowMs, snapshotSeq, snapshotTs, maxAgeS }) {
  const seq = sanitizeSeq(rawSeq, nowMs, snapshotSeq);
  const nowSec = Math.floor(nowMs / 1000);
  const stale = snapshotSeq === NO_DIRECTOR_SEQ || nowSec - snapshotTs > maxAgeS;

  // While a director is demonstrably live, the reserved 0 is not a takeover — it is malformed input
  // or an override attempt. Only a stale room honours it.
  if (!stale && seq === NO_DIRECTOR_SEQ) return { apply: false, seq: snapshotSeq, reason: "seq_reserved" };
  // Monotonic while live: a burst of page turns arriving out of order must not rewind the song.
  if (!stale && seq <= snapshotSeq) return { apply: false, seq: snapshotSeq, reason: "seq_not_newer" };

  return { apply: true, seq: seq > NO_DIRECTOR_SEQ ? seq : snapshotSeq + 1, reason: stale ? "takeover" : "advance" };
}
