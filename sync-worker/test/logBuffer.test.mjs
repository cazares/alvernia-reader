// sync-worker/src/logBuffer.js — the instrument every mesh diagnosis depends on.
//
// It earns tests because it is the thing you consult when you cannot trust anything else: there is
// no internet in the church and no console on an iPad, so `GET /log` is the only account of what
// happened. An instrument that quietly drops evidence is worse than no instrument, because it reads
// as "nothing happened" — which is exactly how the two defects it now fixes went unnoticed.
import test from "node:test";
import assert from "node:assert/strict";
import {
  foldLogEntries,
  logSignature,
  LOG_MAX_BATCH,
  LOG_MAX_BYTES,
  LOG_MAX_ENTRIES,
  LOG_RATE_BURST,
  LOG_RATE_PER_SEC,
} from "../src/logBuffer.js";

const beat = (over = {}) => ({ t: 1000, dev: "ipad-1", role: "follower", event: "mesh:page-recv", page: 11, dup: true, ...over });

test("consecutive identical keepalives collapse into ONE row", () => {
  const out = foldLogEntries([], [beat({ t: 1 }), beat({ t: 2 }), beat({ t: 3 })], 99);
  assert.equal(out.length, 1, "three identical heartbeats should be one row");
  assert.equal(out[0].n, 3);
});

test("the collapsed row still spans every occurrence — the heartbeat RATE survives", () => {
  // This is the whole justification for collapsing: n across (t0 -> t) is strictly more precise
  // than 271 copies of the same line, so nothing diagnostic is lost.
  const out = foldLogEntries([], [beat({ t: 1000 }), beat({ t: 2000 }), beat({ t: 3000 })], 99);
  const [row] = out;
  assert.equal(row.t0, 1000, "first timestamp lost");
  assert.equal(row.t, 3000, "last timestamp lost");
  const rate = (row.n - 1) / ((row.t - row.t0) / 1000);
  assert.equal(rate, 1, "should read back as a 1 Hz heartbeat");
});

test("a DIFFERENT page is a different row — a real page turn is never folded away", () => {
  const out = foldLogEntries([], [beat({ page: 11 }), beat({ page: 12 }), beat({ page: 12 })], 99);
  assert.equal(out.length, 2);
  assert.deepEqual(out.map((r) => r.page), [11, 12]);
});

test("a different DEVICE is a different row — two iPads never merge", () => {
  const out = foldLogEntries([], [beat({ dev: "ipad-1" }), beat({ dev: "ipad-2" })], 99);
  assert.equal(out.length, 2);
});

test("a different EVENT is a different row — a disconnect is never folded into a heartbeat", () => {
  // The exact eviction this fix exists to prevent: the one row that explains a failure must
  // survive next to the keepalives around it.
  const entries = [beat(), beat(), { t: 5, dev: "ipad-1", event: "session:notConnected", peer: "iPad-x" }, beat(), beat()];
  const out = foldLogEntries([], entries, 99);
  assert.equal(out.length, 3, "the disconnect must stand alone between the two runs");
  assert.equal(out[1].event, "session:notConnected");
});

test("INTERLEAVED devices each fold onto their own row, and counts never cross devices", () => {
  // The case that matters and the one an earlier version got wrong: real traffic never arrives in
  // per-device runs. Five followers beating at 1 Hz interleave, so folding into "the previous row"
  // folds nothing. Each device must accumulate on its OWN last row — and a's count must never
  // include b's beats.
  const entries = [
    beat({ dev: "a", t: 1 }), beat({ dev: "b", t: 1 }),
    beat({ dev: "a", t: 2 }), beat({ dev: "b", t: 2 }),
    beat({ dev: "a", t: 3 }),
  ];
  const out = foldLogEntries([], entries, 99);
  assert.equal(out.length, 2, "should be one row per device");
  const byDev = Object.fromEntries(out.map((r) => [r.dev, r]));
  assert.equal(byDev.a.n, 3, "device a miscounted");
  assert.equal(byDev.b.n, 2, "device b miscounted");
  assert.equal(byDev.a.t, 3);
  assert.equal(byDev.b.t, 2);
});

test("an intervening event from the SAME device ends that device's run", () => {
  // Otherwise a heartbeat before a disconnect and one after it would merge into a single row that
  // spans the outage — the buffer would show continuous beating across the exact gap being hunted.
  const entries = [
    beat({ dev: "a", t: 1 }),
    { t: 2, dev: "a", event: "session:notConnected", peer: "p" },
    beat({ dev: "a", t: 3 }),
  ];
  const out = foldLogEntries([], entries, 99);
  assert.equal(out.length, 3, "the run must not span the disconnect");
  assert.equal(out[2].n, undefined, "post-disconnect beat wrongly folded into the pre-disconnect run");
});

test("folding continues onto rows ALREADY in the buffer, across POSTs", () => {
  // Devices batch ~1/sec, so a run of heartbeats arrives as many separate POSTs. If folding only
  // worked within one batch, the buffer would still fill with keepalive.
  let buf = foldLogEntries([], [beat({ t: 1 })], 1);
  buf = foldLogEntries(buf, [beat({ t: 2 })], 2);
  buf = foldLogEntries(buf, [beat({ t: 3 })], 3);
  assert.equal(buf.length, 1);
  assert.equal(buf[0].n, 3);
  assert.equal(buf[0].t0, 1);
  assert.equal(buf[0].t, 3);
});

test("rows with no device or no event are NEVER folded", () => {
  // logSignature returns "" for an unrecognised shape, and "" must not match "". Failing toward
  // more detail is the only safe direction for an instrument.
  assert.equal(logSignature({ t: 1 }), "");
  assert.equal(logSignature(null), "");
  const odd = [{ t: 1, msg: "x" }, { t: 2, msg: "x" }, { t: 3, msg: "x" }];
  assert.equal(foldLogEntries([], odd, 9).length, 3);
});

test("every entry is stamped with the server receive time", () => {
  // Devices can have wrong clocks; rx is what makes rows from different devices orderable.
  const out = foldLogEntries([], [{ t: 1, dev: "a", event: "boot" }], 12345);
  assert.equal(out[0].rx, 12345);
});

test("a non-object entry still lands, wrapped, rather than being dropped", () => {
  const out = foldLogEntries([], ["a string", 42], 7);
  assert.equal(out.length, 2);
  assert.equal(out[0].v, "a string");
  assert.equal(out[0].rx, 7);
});

test("one POST cannot contribute more than LOG_MAX_BATCH entries", () => {
  const many = Array.from({ length: LOG_MAX_BATCH + 500 }, (_, i) => ({ t: i, dev: "d", event: "e" + i }));
  assert.equal(foldLogEntries([], many, 1).length, LOG_MAX_BATCH);
});

test("the buffer never exceeds the entry ceiling", () => {
  const many = Array.from({ length: 300 }, (_, i) => ({ t: i, dev: "d", event: "e" + i }));
  let buf = [];
  for (let i = 0; i < 40; i += 1) buf = foldLogEntries(buf, many.map((e) => ({ ...e, event: e.event + "-" + i })), i);
  assert.ok(buf.length <= LOG_MAX_ENTRIES, `buffer grew to ${buf.length}`);
});

test("the buffer never exceeds the BYTE ceiling — the DO write must not fail", () => {
  // The load-bearing guard: a Durable Object value is capped at 128 KiB and an oversized write
  // fails outright, which would discard the WHOLE log rather than its oldest rows.
  const fat = Array.from({ length: 200 }, (_, i) => ({ t: i, dev: "d", event: "e" + i, blob: "x".repeat(900) }));
  let buf = [];
  for (let i = 0; i < 30; i += 1) buf = foldLogEntries(buf, fat.map((e) => ({ ...e, event: e.event + "-" + i })), i);
  const bytes = JSON.stringify(buf).length;
  assert.ok(bytes <= LOG_MAX_BYTES, `buffer is ${bytes} bytes, over the ${LOG_MAX_BYTES} ceiling`);
  assert.ok(buf.length > 0, "the trim must not empty the buffer");
});

test("the byte trim keeps the NEWEST rows", () => {
  // Trimming the wrong end would throw away what just happened, which is the only part anyone
  // reads after an incident.
  const fat = Array.from({ length: 200 }, (_, i) => ({ t: i, dev: "d", event: "old" + i, blob: "x".repeat(900) }));
  let buf = foldLogEntries([], fat, 1);
  for (let i = 0; i < 30; i += 1) buf = foldLogEntries(buf, fat.map((e) => ({ ...e, event: "new" + i + "-" + e.event })), i);
  assert.ok(String(buf[buf.length - 1].event).startsWith("new"), "newest row was trimmed away");
});

test("the rate limit clears a real fleet on one NAT", () => {
  // The defect this replaces: 3/sec sustained, while six devices behind one router present ~6/sec.
  // Ten devices is the realistic ceiling for this parish; the limit must clear it with margin.
  const DEVICES = 10;
  const FLUSHES_PER_SEC = 1;
  assert.ok(
    LOG_RATE_PER_SEC >= DEVICES * FLUSHES_PER_SEC * 2,
    `sustained limit ${LOG_RATE_PER_SEC}/s is too tight for ${DEVICES} devices on one IP`,
  );
  assert.ok(LOG_RATE_BURST >= DEVICES * 5, "burst too small for a fleet foregrounding at once");
});

test("a realistic 6-device stress run stays inside both ceilings and keeps its lifecycle events", () => {
  // End-to-end shape check: five followers beating at 1 Hz for ten minutes, with the handful of
  // lifecycle events that actually matter sprinkled through. Before the fold this was ~100
  // seconds of buffer; it must now cover the whole run AND still contain every disconnect.
  let buf = [];
  const SECONDS = 600;
  let disconnects = 0;
  for (let s = 0; s < SECONDS; s += 1) {
    const batch = [];
    for (let d = 1; d <= 5; d += 1) batch.push({ t: s * 1000, dev: "ipad-" + d, role: "follower", event: "mesh:page-recv", page: Math.floor(s / 60) + 1, dup: true });
    if (s % 90 === 0) { batch.push({ t: s * 1000, dev: "ipad-2", event: "session:notConnected", peer: "iPad-dir" }); disconnects += 1; }
    buf = foldLogEntries(buf, batch, s);
  }
  assert.ok(JSON.stringify(buf).length <= LOG_MAX_BYTES, "10-minute 6-device run blew the byte ceiling");
  const kept = buf.filter((r) => r.event === "session:notConnected").length;
  assert.equal(kept, disconnects, `lost disconnect evidence: kept ${kept} of ${disconnects}`);
});
