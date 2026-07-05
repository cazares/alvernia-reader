// Unit tests for web/src/lib/svSyncDecision.js — the pure follower-sync decision.
//
// svSyncDecision.js is a UMD CommonJS-compatible module (no "type":"module"), so
// we import it through createRequire. These run in the SAFE subset (node --test),
// never against the live relay. They pin the load-bearing ordering that a broken
// director must NOT freeze the "en vivo" pill (P2-SEQ) and that a skewed device
// clock must NOT hide a live director (P2-CLOCKSKEW).
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { decideRelaySnapshot, clockOffsetFromDateHeader, clockOffsetFromServerNow } = require("../web/src/lib/svSyncDecision.js");

const MAX_AGE_S = 90;
// A fixed "now" so tests are deterministic (no Date.now()).
const NOW_MS = 1_700_000_000_000;
const NOW_S = NOW_MS / 1000;

// Base follower context: a fresh boot, no director seen yet, not browsing.
function ctx(over = {}) {
  return {
    lastSeq: -1,
    hasDirector: false,
    browsing: false,
    currentPage: 1,
    force: false,
    nowMs: NOW_MS,
    clockOffsetMs: 0,
    maxAgeS: MAX_AGE_S,
    ...over,
  };
}

// ---- malformed input -----------------------------------------------------------

test("reject: a non-finite page is ignored (no state change)", () => {
  for (const page of [NaN, undefined, null, "5", Infinity]) {
    const d = decideRelaySnapshot({ page, seq: 10, ts: NOW_S }, ctx());
    assert.equal(d.action, "reject", `page=${String(page)} must reject`);
  }
});

test("reject: null snapshot / null ctx never throws", () => {
  assert.equal(decideRelaySnapshot(null, ctx()).action, "reject");
  assert.equal(decideRelaySnapshot({ page: 5, seq: 1, ts: NOW_S }, null).action, "reject");
});

// ---- demotion (freshness) ------------------------------------------------------

test("demote: a director that never published (seq<=0) is not live", () => {
  const d = decideRelaySnapshot({ page: 42, seq: 0, ts: NOW_S }, ctx());
  assert.equal(d.action, "demote");
  assert.equal(d.hasDirector, false);
  assert.equal(d.browsing, false);
  assert.equal(d.hideGoLiveBar, true);
  assert.equal(d.renderPill, true);
});

test("demote: a stale director (ts older than maxAge) is demoted", () => {
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - (MAX_AGE_S + 5) }, // 95s old
    ctx({ hasDirector: true, lastSeq: 5000 }),
  );
  assert.equal(d.action, "demote");
  assert.equal(d.hasDirector, false);
});

// ---- P2-SEQ: the Wednesday freeze — a stale DUPLICATE seq must still demote -----

test("P2-SEQ: a dead director's duplicate-seq heartbeat DEMOTES (does not freeze live)", () => {
  // Director was live on page 42 at seq 5000; follower is following. The director
  // then goes silent. Every WS heartbeat reply re-delivers seq 5000 with the ORIGINAL
  // (now stale) ts. The OLD code dropped this at the seq guard and never demoted →
  // green pill frozen forever. It must demote now.
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - 120 }, // duplicate seq, stale ts
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 42 }),
  );
  assert.equal(d.action, "demote", "a stale duplicate-seq snapshot must demote, not freeze");
  assert.equal(d.hasDirector, false);
});

test("P2-SEQ: a FRESH duplicate-seq heartbeat keeps the director live (no re-render)", () => {
  // Same seq, but ts is fresh → the director is alive and sitting still. Must stay
  // live (pill green) but NOT re-render the page.
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - 3 },
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 42 }),
  );
  assert.equal(d.action, "live-dup");
  assert.equal(d.hasDirector, true);
  assert.equal(d.renderPage, undefined, "a duplicate seq must not re-render the page");
  assert.equal(d.renderPill, true);
  assert.equal(d.lastSeq, 5000, "lastSeq unchanged on a duplicate");
});

// ---- follow / browsing ---------------------------------------------------------

test("follow: a fresh newer-seq move snaps the follower to the director's page", () => {
  const d = decideRelaySnapshot(
    { page: 43, seq: 5001, ts: NOW_S - 1 },
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 42 }),
  );
  assert.equal(d.action, "follow");
  assert.equal(d.hasDirector, true);
  assert.equal(d.lastSeq, 5001);
  assert.equal(d.livePage, 43);
  assert.equal(d.renderPage, 43, "must render the new page");
  assert.equal(d.reveal, true);
});

test("follow: already on the director's page → live but no redundant render", () => {
  const d = decideRelaySnapshot(
    { page: 43, seq: 5001, ts: NOW_S - 1 },
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 43 }),
  );
  assert.equal(d.action, "follow");
  assert.equal(d.renderPage, undefined, "no re-render when already on the page");
});

test("browsing: a live move is tracked but does NOT yank a browsing user", () => {
  const d = decideRelaySnapshot(
    { page: 50, seq: 5002, ts: NOW_S - 1 },
    ctx({ hasDirector: true, browsing: true, lastSeq: 5001, currentPage: 12 }),
  );
  assert.equal(d.action, "browsing");
  assert.equal(d.livePage, 50, "director's page is tracked for 'volver a en vivo'");
  assert.equal(d.renderPage, undefined, "must NOT render — the user is browsing");
  assert.equal(d.browsing, true, "browsing state preserved");
});

// ---- force (resync) ------------------------------------------------------------

test("force: a duplicate seq re-homes a stationary director on a forced resync", () => {
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - 10 }, // same seq as lastSeq, but fresh
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 7, force: true }),
  );
  assert.equal(d.action, "follow", "force bypasses the seq guard for a fresh director");
  assert.equal(d.renderPage, 42, "forced resync re-homes onto the director's current page");
});

test("force: does NOT resurrect a stale director", () => {
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - 300 }, // forced, but 5 min stale
    ctx({ hasDirector: true, lastSeq: 5000, currentPage: 7, force: true }),
  );
  assert.equal(d.action, "demote", "force still respects staleness — a dead director stays dead");
});

// ---- P2-CLOCKSKEW: a skewed device clock must not hide a live director ----------

test("P2-CLOCKSKEW: a device clock fast by 2 min still sees a fresh director as LIVE", () => {
  // Device clock is 120s AHEAD of the server. A brand-new snapshot (ts = server now)
  // looks 120s old by the raw device clock → would be judged stale. With the server
  // offset applied it is correctly fresh.
  const skewMs = 120_000;
  const serverTs = NOW_S; // director published "now" on the server clock
  const deviceNowMs = NOW_MS + skewMs; // device thinks it is 2 min later
  const withoutOffset = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: serverTs },
    ctx({ nowMs: deviceNowMs, clockOffsetMs: 0, hasDirector: true, lastSeq: 4999 }),
  );
  assert.equal(withoutOffset.action, "demote", "raw skewed clock wrongly demotes (documents the bug)");

  const offsetMs = clockOffsetFromDateHeader(new Date(NOW_MS).toUTCString(), deviceNowMs, 0);
  const withOffset = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: serverTs },
    ctx({ nowMs: deviceNowMs, clockOffsetMs: offsetMs, hasDirector: true, lastSeq: 4999 }),
  );
  assert.equal(withOffset.action, "follow", "server-calibrated offset restores correct liveness");
});

test("P2-CLOCKSKEW: a genuinely old snapshot is still stale even after offset correction", () => {
  // Offset corrects skew, it must NOT mask a real dead director: a 5-min-old publish
  // is old on the server clock too.
  const skewMs = 120_000;
  const deviceNowMs = NOW_MS + skewMs;
  const offsetMs = clockOffsetFromDateHeader(new Date(NOW_MS).toUTCString(), deviceNowMs, 0);
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S - 300 },
    ctx({ nowMs: deviceNowMs, clockOffsetMs: offsetMs, hasDirector: true, lastSeq: 4999 }),
  );
  assert.equal(d.action, "demote");
});

// ---- clockOffsetFromDateHeader edge cases --------------------------------------

test("clockOffsetFromDateHeader: parses a valid Date header into (server - device)", () => {
  const off = clockOffsetFromDateHeader(new Date(NOW_MS).toUTCString(), NOW_MS - 5000, 0);
  // Server ~5s ahead of the device (device captured 5s earlier). HTTP dates have 1s
  // resolution, so allow a 1s slop.
  assert.ok(Math.abs(off - 5000) <= 1000, `expected ~5000ms, got ${off}`);
});

test("clockOffsetFromDateHeader: missing/garbage header keeps the previous offset", () => {
  assert.equal(clockOffsetFromDateHeader(null, NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromDateHeader("", NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromDateHeader("not-a-date", NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromDateHeader("garbage", NOW_MS, undefined), 0);
});

test("clockOffsetFromServerNow: body `now` (epoch s) → (server - device) ms", () => {
  // Server says it is NOW_S; device captured 5s earlier → server is ~5s ahead.
  const off = clockOffsetFromServerNow(NOW_S, NOW_MS - 5000, 0);
  assert.equal(off, 5000);
});

test("clockOffsetFromServerNow: absent/invalid `now` keeps the previous offset (old worker safe)", () => {
  assert.equal(clockOffsetFromServerNow(undefined, NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromServerNow(NaN, NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromServerNow(0, NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromServerNow(-1, NOW_MS, 4200), 4200);
  assert.equal(clockOffsetFromServerNow(NOW_S, NaN, 4200), 4200);
  assert.equal(clockOffsetFromServerNow(undefined, NOW_MS, undefined), 0);
});

test("clockOffsetFromServerNow: corrects a skewed device so a fresh director follows", () => {
  const skewMs = 120_000; // device 2 min fast
  const deviceNowMs = NOW_MS + skewMs;
  const off = clockOffsetFromServerNow(NOW_S, deviceNowMs, 0); // server now = NOW_S
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000, ts: NOW_S, now: NOW_S },
    ctx({ nowMs: deviceNowMs, clockOffsetMs: off, hasDirector: true, lastSeq: 4999 }),
  );
  assert.equal(d.action, "follow", "server-now offset restores correct liveness under skew");
});

test("no snapshot ts (older wire shape) is treated as fresh, never demoted for age", () => {
  const d = decideRelaySnapshot(
    { page: 42, seq: 5000 }, // no ts field
    ctx({ hasDirector: true, lastSeq: 4999 }),
  );
  assert.equal(d.action, "follow", "absent ts must not demote — no regression vs the old wire");
});
