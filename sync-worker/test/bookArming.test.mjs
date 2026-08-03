// sync-worker/src/bookArming.js — who gets told about a new songbook.
//
// The worker had ZERO test coverage. This is the one piece of it that fans out to every iPad at
// once, so it is where coverage starts. Most of these assert that NOTHING is handed out.
import test from "node:test";
import assert from "node:assert/strict";
import { decideBookUpdate, STAGE_SLOT_TTL_SEC } from "../src/bookArming.js";

const V = "bv_0123456789abcdef";
const NOW = 1_700_000_000;
const dev = (over = {}) => ({ deviceId: "ipad-1", bookVersion: "bv_ffffffffffffffff", bookStage: "active", ts: NOW, ...over });
const armedEnv = (over = {}) => ({
  BOOK_UPDATE_VERSION: V,
  BOOK_UPDATE_DEVICES: "ipad-1",
  BOOK_UPDATE_ALLOW_FLEET: "",
  BOOK_UPDATE_BASE: "https://signovivo.com",
  BOOK_UPDATE_CONCURRENCY: "2",
  ...over,
});

// ─── Dormant is the shipped default ─────────────────────────────────────────

test("SHIPPED STATE: empty BOOK_UPDATE_VERSION hands out nothing at all", () => {
  assert.equal(decideBookUpdate({}, dev(), [], NOW), null);
  assert.equal(decideBookUpdate({ BOOK_UPDATE_VERSION: "" }, dev(), [], NOW), null);
  assert.equal(decideBookUpdate({ BOOK_UPDATE_VERSION: "   " }, dev(), [], NOW), null);
});

test("a version set but NO devices armed still hands out nothing", () => {
  assert.equal(decideBookUpdate(armedEnv({ BOOK_UPDATE_DEVICES: "" }), dev(), [], NOW), null);
});

test("malformed config fails CLOSED rather than open", () => {
  for (const bad of ["latest", "bv_XYZ", "bv_0123456789abcde", "*"]) {
    assert.equal(decideBookUpdate(armedEnv({ BOOK_UPDATE_VERSION: bad }), dev(), [], NOW), null, bad);
  }
});

// ─── One iPad first ─────────────────────────────────────────────────────────

test("a NAMED device is armed", () => {
  assert.deepEqual(decideBookUpdate(armedEnv(), dev(), [], NOW), { bookVersion: V, base: "https://signovivo.com" });
});

test("an unnamed device beside a named one gets nothing", () => {
  assert.equal(decideBookUpdate(armedEnv(), dev({ deviceId: "ipad-9" }), [], NOW), null);
});

test("a named device is NOT subject to the fleet queue — 'prove it on one iPad' must always work", () => {
  const busy = Array.from({ length: 5 }, (_, i) => dev({ deviceId: `x${i}`, bookStage: "downloading:10%" }));
  assert.ok(decideBookUpdate(armedEnv(), dev(), busy, NOW));
});

test("a device already on the target book is told nothing — the pointer is not a heartbeat", () => {
  assert.equal(decideBookUpdate(armedEnv(), dev({ bookVersion: V }), [], NOW), null);
});

test("a device with no id is never armed", () => {
  assert.equal(decideBookUpdate(armedEnv(), dev({ deviceId: "" }), [], NOW), null);
});

// ─── The two independent switches (red team A2) ─────────────────────────────

test('"*" ALONE does nothing — fat-fingering one var cannot fan out to the fleet', () => {
  const env = armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "" });
  assert.equal(decideBookUpdate(env, dev({ deviceId: "any" }), [], NOW), null);
});

test('BOOK_UPDATE_ALLOW_FLEET must be exactly "yes" — not "true", not "1", not "YES"', () => {
  for (const v of ["true", "1", "YES", "y", "yes "]) {
    const env = armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: v });
    assert.equal(decideBookUpdate(env, dev({ deviceId: "any" }), [], NOW), null, `accepted ${JSON.stringify(v)}`);
  }
});

test('both switches set arms the fleet', () => {
  const env = armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "yes" });
  assert.ok(decideBookUpdate(env, dev({ deviceId: "any" }), [], NOW));
});

// ─── The throttle gates STARTS, never work in progress ──────────────────────
//
// Returning null is byte-identical to "disarmed", and the client treats an absent pointer as a
// REVOKE (PdfReaderApp.tsx:1376 deletes the staged directory). Throttling a device that already
// downloaded therefore destroys ~27 MB of verified work and restarts it — the exact opposite of
// what the throttle is for. Gate on bookStage: a device reports bookVersion = the ACTIVE book
// (PdfReaderApp.tsx:273), so a staged-and-ready device still reports the OLD version.

const queueEnv = (over) => armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "yes", ...over });
const queueBusy = (n) => Array.from({ length: n }, (_, i) => dev({ deviceId: `other${i}`, bookStage: "downloading:40%" }));

test("a device that is ALREADY READY keeps its pointer even when the queue is full", () => {
  const me = dev({ deviceId: "me", bookStage: "ready" });
  const fleet = [me, ...queueBusy(5)];
  assert.deepEqual(
    decideBookUpdate(queueEnv(), me, fleet, NOW),
    { bookVersion: V, base: "https://signovivo.com" },
    "a full queue revoked a verified staged copy — it would be deleted and re-downloaded",
  );
});

test("a device MID-DOWNLOAD keeps its pointer even when the queue is full", () => {
  const me = dev({ deviceId: "me", bookStage: "downloading:70%" });
  const fleet = [me, ...queueBusy(5)];
  assert.ok(decideBookUpdate(queueEnv(), me, fleet, NOW), "a partial download was thrown away mid-flight");
});

test("a device that has NOT started IS still throttled — the queue must still protect the AP", () => {
  const me = dev({ deviceId: "me" });
  assert.equal(decideBookUpdate(queueEnv(), me, [me, ...queueBusy(5)], NOW), null);
});

test("an unstarted device is armed once the queue drains", () => {
  const me = dev({ deviceId: "me" });
  assert.ok(decideBookUpdate(queueEnv(), me, [me], NOW));
});

// ─── K-at-a-time ────────────────────────────────────────────────────────────

const fleetEnv = () => armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "yes", BOOK_UPDATE_CONCURRENCY: "2" });
const downloading = (id, tsOffset = 0) => dev({ deviceId: id, bookStage: "downloading:30%", ts: NOW - tsOffset });

test("holds a device back when the concurrency limit is already taken", () => {
  const fleet = [downloading("a"), downloading("b")];
  assert.equal(decideBookUpdate(fleetEnv(), dev({ deviceId: "c" }), fleet, NOW), null);
});

test("lets one through when a slot frees", () => {
  assert.ok(decideBookUpdate(fleetEnv(), dev({ deviceId: "c" }), [downloading("a")], NOW));
});

test("a STALE mid-download claim stops occupying a slot — one crashed iPad cannot block the fleet forever", () => {
  const fleet = [downloading("a", STAGE_SLOT_TTL_SEC + 1), downloading("b", STAGE_SLOT_TTL_SEC + 1)];
  assert.ok(decideBookUpdate(fleetEnv(), dev({ deviceId: "c" }), fleet, NOW));
});

test("a device that FINISHED does not occupy a slot", () => {
  const fleet = [dev({ deviceId: "a", bookStage: "active", bookVersion: V }), downloading("b")];
  assert.ok(decideBookUpdate(fleetEnv(), dev({ deviceId: "c" }), fleet, NOW));
});

test("the device asking never counts itself as occupying a slot", () => {
  const fleet = [downloading("c"), downloading("a")];
  const env = armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "yes", BOOK_UPDATE_CONCURRENCY: "2" });
  // 'c' is mid-download itself; only 'a' is another device, so 1 < 2 and it may resume.
  assert.ok(decideBookUpdate(env, dev({ deviceId: "c" }), fleet, NOW));
});

test("concurrency is clamped to at least 1 even if misconfigured", () => {
  for (const c of ["0", "-5", "abc", ""]) {
    const env = armedEnv({ BOOK_UPDATE_DEVICES: "*", BOOK_UPDATE_ALLOW_FLEET: "yes", BOOK_UPDATE_CONCURRENCY: c });
    assert.ok(decideBookUpdate(env, dev({ deviceId: "c" }), [], NOW), `c=${c} blocked everything`);
    assert.equal(decideBookUpdate(env, dev({ deviceId: "c" }), [downloading("a"), downloading("b")], NOW), null);
  }
});

// ─── The abort ──────────────────────────────────────────────────────────────

test("ABORT: clearing the version stops arming immediately, for every device", () => {
  const env = { ...fleetEnv(), BOOK_UPDATE_VERSION: "" };
  for (const id of ["ipad-1", "any", "c"]) {
    assert.equal(decideBookUpdate(env, dev({ deviceId: id }), [], NOW), null);
  }
});
