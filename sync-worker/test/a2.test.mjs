/**
 * A2 worker tests — rate limiting + the seq=0 gate.
 *
 * Runs against a LOCAL `wrangler dev` (never production). Disabled by default: it
 * throws at load unless RELAY_TEST_BASE + RELAY_TEST_TOKEN point at a local relay.
 * Run via: bash sync-worker/test/run-a2.sh
 */
import test from "node:test";
import assert from "node:assert/strict";

const BASE = process.env.RELAY_TEST_BASE || "";
const CODE = process.env.RELAY_TEST_CODE || "";
if (!BASE || !CODE) {
  throw new Error(
    "a2.test.mjs is disabled by default — set RELAY_TEST_BASE + RELAY_TEST_CODE to a LOCAL wrangler dev relay.",
  );
}
if (/signovivo|workers\.dev/.test(BASE)) {
  throw new Error(`a2.test.mjs refuses to run against a non-local base: ${BASE}`);
}

const FLEET_KEY = process.env.RELAY_TEST_FLEET_KEY || "";

const pub = (room, body) =>
  fetch(`${BASE}/r/${room}/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Director-Code": CODE },
    body: JSON.stringify(body),
  });
const state = (room) => fetch(`${BASE}/r/${room}/state`).then((r) => r.json());

test("baseline: a valid publish is accepted and sets the page/seq", async () => {
  const room = "a2-baseline";
  const r = await pub(room, { v: 1, page: 42, totalPages: 371, seq: 1000 });
  assert.equal(r.status, 200);
  const body = await r.json();
  assert.equal(body.ok, true);
  assert.equal(body.ignored, undefined);
  const s = await state(room);
  assert.equal(s.page, 42);
  assert.equal(s.seq, 1000);
});

test("A2 seq=0 gate: seq=0 is rejected while a director is live (no page override)", async () => {
  const room = "a2-seqzero";
  await pub(room, { v: 1, page: 10, totalPages: 371, seq: 5000 }); // live director on page 10
  const r = await pub(room, { v: 1, page: 99, totalPages: 371, seq: 0 }); // seq=0 override attempt
  assert.equal(r.status, 200);
  const body = await r.json();
  assert.equal(body.ignored, true, "seq=0 must be ignored while a director is live");
  const s = await state(room);
  assert.equal(s.page, 10, "the live director's page must NOT be moved by a seq=0 publish");
  assert.equal(s.seq, 5000, "seq must not advance on an ignored publish");
});

test("A2 seq=0 gate: seq=0 IS honored as a reset when the room is stale (no director)", async () => {
  const room = "a2-seqzero-stale";
  // Fresh/empty room → snapshot stale → a seq=0 (or invalid) publish is a legit first/reset publish.
  const r = await pub(room, { v: 1, page: 7, totalPages: 371, seq: 0 });
  assert.equal(r.status, 200);
  const s = await state(room);
  assert.equal(s.page, 7, "a seq=0 publish to a stale/empty room IS accepted (takeover/reset)");
  assert.ok(s.seq >= 1, "stale seq=0 gets assigned the next monotonic seq");
});

test("P2-CLOCKSKEW: /state includes a server `now` (epoch seconds) for clock calibration", async () => {
  const room = "p2-now";
  await pub(room, { v: 1, page: 5, totalPages: 371, seq: 3000 });
  const s = await state(room);
  assert.equal(typeof s.now, "number", "/state must expose a numeric `now`");
  assert.ok(Number.isInteger(s.now) && s.now > 1_600_000_000, `now must be epoch seconds; got ${s.now}`);
  // Sanity: server now is within a wide window of the test host's clock (both ~real time).
  const localNow = Math.floor(Date.now() / 1000);
  assert.ok(Math.abs(s.now - localNow) < 3600, `server now (${s.now}) should be near local (${localNow})`);
  // Existing fields must be untouched by the additive `now`.
  assert.equal(s.page, 5);
  assert.equal(s.seq, 3000);
});

test("P6-LOG: POST /log stays OPEN (devices post breadcrumbs without a secret)", async () => {
  const r = await fetch(`${BASE}/log`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([{ kind: "crash", where: "test", msg: "hello", t: Date.now() }]),
  });
  assert.equal(r.status, 200, "POST /log must remain open for device telemetry");
  const b = await r.json();
  assert.equal(b.ok, true);
});

test("P6-LOG: GET /log is 401 WITHOUT the fleet key, 200 WITH it", async () => {
  const noKey = await fetch(`${BASE}/log`);
  assert.equal(noKey.status, 401, "GET /log must be gated — the buffer is not world-readable");
  const nb = await noKey.json();
  assert.equal(nb.ok, false);

  if (!FLEET_KEY) return; // key not provided to the harness — the positive case is skipped
  const withKey = await fetch(`${BASE}/log?k=${encodeURIComponent(FLEET_KEY)}`);
  assert.equal(withKey.status, 200, "the fleet key must unlock GET /log");
  const wb = await withKey.json();
  assert.equal(wb.ok, true);
  assert.ok(Array.isArray(wb.entries), "GET /log returns the entries array when authorized");
});

test("P6-LOG: GET /log accepts the fleet key via X-Fleet-Key header too", async () => {
  if (!FLEET_KEY) return;
  const r = await fetch(`${BASE}/log`, { headers: { "X-Fleet-Key": FLEET_KEY } });
  assert.equal(r.status, 200, "X-Fleet-Key header must also authorize");
});

test("P6-LOG: DELETE /log is 401 WITHOUT the key, 200 WITH it", async () => {
  const noKey = await fetch(`${BASE}/log`, { method: "DELETE" });
  assert.equal(noKey.status, 401, "DELETE /log must be gated — the buffer is not world-wipeable");

  if (!FLEET_KEY) return;
  const withKey = await fetch(`${BASE}/log?k=${encodeURIComponent(FLEET_KEY)}`, { method: "DELETE" });
  assert.equal(withKey.status, 200, "the fleet key must unlock DELETE /log");
  const b = await withKey.json();
  assert.equal(b.cleared, true);
});

test("Slice D: a posted crash appears on the gated fleet dashboard", async () => {
  if (!FLEET_KEY) return;
  const marker = "boom-" + Math.floor(Date.now() / 1000);
  const post = await fetch(`${BASE}/log`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([{ kind: "crash", dev: "testdev", build: "999", where: "unit", msg: marker, t: Date.now() }]),
  });
  assert.equal(post.status, 200);
  const dash = await fetch(`${BASE}/fleet-dashboard?k=${encodeURIComponent(FLEET_KEY)}`);
  assert.equal(dash.status, 200, "dashboard must render for an authorized key");
  const html = await dash.text();
  assert.match(html, /Fallos recientes/, "dashboard shows the recent-crashes panel");
  assert.ok(html.includes(marker), "the posted crash message appears on the dashboard");
});

test("A2 rate limit: a publish flood is throttled (429s after the burst; some allowed)", async () => {
  const room = "a2-flood";
  const statuses = await Promise.all(
    Array.from({ length: 40 }, (_, i) =>
      pub(room, { v: 1, page: 1, totalPages: 371, seq: 20000 + i }).then((r) => r.status),
    ),
  );
  const limited = statuses.filter((s) => s === 429).length;
  const notLimited = statuses.filter((s) => s !== 429).length;
  assert.ok(limited >= 10, `a 40-request flood must produce 429s; got ${limited}`);
  assert.ok(notLimited >= 10, `the burst (~15) must be allowed through; got ${notLimited} not-limited`);
});
