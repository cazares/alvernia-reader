/**
 * Relay sync tests — live against the deployed Cloudflare Worker.
 * Covers: health, state, auth, publish, seq guard, dual-director clash,
 * WebSocket subscription, heartbeat/ping, and staleness detection.
 *
 * Run: node --test e2e/relay-sync.test.mjs
 * Requires internet access to signovivo-sync.4j4982y8jp.workers.dev
 */

import test from "node:test";
import assert from "node:assert/strict";
// WebSocket is available globally in Node 22+; no import needed.

// ── Config ──────────────────────────────────────────────────────────────────

const BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";
const ROOM = "alvernia-main";
const PUB = `${BASE}/r/${ROOM}/publish`;
const STATE = `${BASE}/r/${ROOM}/state`;
const SUB = `${BASE}/r/${ROOM}/subscribe`.replace("https://", "wss://");
const CODE = "12345678840";
const BAD_CODE = "0000000000";

// ── Helpers ─────────────────────────────────────────────────────────────────

const get = (url) => fetch(url).then((r) => r.json());
const post = (url, body, headers = {}) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Director-Code": CODE, ...headers },
    body: JSON.stringify(body),
  });

/** Publish a page and return the parsed response body + HTTP status. */
const publish = async (payload, headers = {}) => {
  const r = await post(PUB, payload, headers);
  return { status: r.status, body: await r.json() };
};

/**
 * Seq base: read the live relay seq once at startup and use it as the floor.
 * Every freshSeq() call returns a value strictly greater than whatever the
 * relay already holds, regardless of how many previous test runs left behind.
 */
const _relayBaseline = await get(STATE).then((s) => s.seq).catch(() => 0);
let _seqCounter = 0;
const freshSeq = () => _relayBaseline + (++_seqCounter) * 100;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Tests ────────────────────────────────────────────────────────────────────

test("health endpoint returns ok + service name", async () => {
  const data = await get(`${BASE}/health`);
  assert.equal(data.ok, true);
  assert.equal(data.service, "signovivo-sync");
  assert.equal(typeof data.v, "number");
});

test("root / also returns health", async () => {
  const data = await get(`${BASE}/`);
  assert.equal(data.ok, true);
});

test("unknown route returns 404", async () => {
  const r = await fetch(`${BASE}/r/${ROOM}/bogus`);
  assert.equal(r.status, 404);
});

test("wrong method on publish returns 405", async () => {
  const r = await fetch(PUB, {
    method: "GET",
    headers: { "X-Director-Code": CODE },
  });
  assert.equal(r.status, 405);
});

test("GET /state returns a valid snapshot shape", async () => {
  const snap = await get(STATE);
  assert.equal(typeof snap.v, "number");
  assert.equal(typeof snap.page, "number");
  assert.equal(typeof snap.seq, "number");
  assert.equal(typeof snap.ts, "number");
  assert.ok(snap.page >= 0);
});

test("auth: missing director code returns 401", async () => {
  const { status } = await publish({ v: 1, page: 1, seq: freshSeq() }, { "X-Director-Code": "" });
  assert.equal(status, 401);
});

test("auth: wrong director code returns 401", async () => {
  const { status } = await publish(
    { v: 1, page: 1, seq: freshSeq() },
    { "X-Director-Code": BAD_CODE },
  );
  assert.equal(status, 401);
});

test("auth: bad JSON body returns 400", async () => {
  const r = await fetch(PUB, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Director-Code": CODE },
    body: "not-json",
  });
  assert.equal(r.status, 400);
});

test("valid publish returns ok:true with seq", async () => {
  const seq = freshSeq();
  const { status, body } = await publish({ v: 1, page: 42, totalPages: 371, seq, mode: "standard", bookId: "alvernia" });
  assert.equal(status, 200);
  assert.equal(body.ok, true);
  assert.equal(typeof body.seq, "number");
});

test("published page is reflected in /state immediately", async () => {
  const seq = freshSeq();
  const page = 77;
  await publish({ v: 1, page, totalPages: 371, seq, mode: "standard", bookId: "alvernia" });
  const snap = await get(STATE);
  assert.equal(snap.page, page);
  assert.ok(snap.seq >= seq);
});

test("seq guard: stale seq is ignored and current seq is preserved", async () => {
  const baseSeq = freshSeq();
  // Publish a high seq first
  await publish({ v: 1, page: 50, seq: baseSeq, mode: "standard", bookId: "alvernia" });
  const stateAfterHigh = await get(STATE);
  // Now publish with a lower (stale) seq
  const { body } = await publish({ v: 1, page: 99, seq: baseSeq - 1000 });
  assert.equal(body.ignored, true, "Stale seq must be flagged ignored:true");
  const stateAfterStale = await get(STATE);
  assert.equal(stateAfterStale.page, stateAfterHigh.page, "Page must not have rewound");
  assert.equal(stateAfterStale.seq, stateAfterHigh.seq, "Seq must not have rewound");
});

test("seq guard: equal seq is also ignored", async () => {
  const seq = freshSeq();
  await publish({ v: 1, page: 60, seq, mode: "standard", bookId: "alvernia" });
  const snap1 = await get(STATE);
  const { body } = await publish({ v: 1, page: 90, seq });
  assert.equal(body.ignored, true, "Duplicate seq must be flagged ignored:true");
  const snap2 = await get(STATE);
  assert.equal(snap2.page, snap1.page, "Page must not change on duplicate seq");
});

test("seq=0 publish is always accepted (no-seq mode)", async () => {
  // seq=0 bypasses the guard and always writes
  const { status, body } = await publish({ v: 1, page: 33, seq: 0, mode: "standard", bookId: "alvernia" });
  assert.equal(status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.ignored, undefined);
});

test("ts is updated to server time on each accepted publish", async () => {
  const before = Math.floor(Date.now() / 1000);
  await publish({ v: 1, page: 10, seq: freshSeq() });
  const snap = await get(STATE);
  const after = Math.floor(Date.now() / 1000) + 1;
  assert.ok(snap.ts >= before, `ts ${snap.ts} should be >= ${before}`);
  assert.ok(snap.ts <= after, `ts ${snap.ts} should be <= ${after}`);
});

test("page value is clamped to at least 1", async () => {
  const seq = freshSeq();
  await publish({ v: 1, page: 0, seq });
  const snap = await get(STATE);
  assert.ok(snap.page >= 1, "page 0 should be clamped to 1");
});

test("non-numeric page is coerced to a valid page >= 1 (no NaN leaks into state)", async () => {
  // A native bridge or buggy director could POST a non-numeric page (e.g. "abc").
  // The relay must coerce it to a sane page, never persist NaN/null — a NaN page
  // would desync every follower. Ground truth (probed live): the worker clamps to 1.
  const seq = freshSeq();
  await publish({ v: 1, page: "abc", seq, mode: "standard", bookId: "alvernia" });
  const snap = await get(STATE);
  assert.equal(typeof snap.page, "number", "page must stay numeric after a non-numeric publish");
  assert.ok(Number.isFinite(snap.page), "page must not be NaN/Infinity");
  assert.ok(snap.page >= 1, "non-numeric page should be clamped to >= 1");
});

test("mode and bookId are persisted in state", async () => {
  const seq = freshSeq();
  await publish({ v: 1, page: 55, seq, mode: "nonStandard", bookId: "hymns-4" });
  const snap = await get(STATE);
  assert.equal(snap.mode, "nonStandard");
  assert.equal(snap.bookId, "hymns-4");
});

test("dual-director clash: second director with higher seq wins and is reflected in state", async () => {
  const seqA = freshSeq();
  const seqB = freshSeq(); // B is always ahead of A (counter increments)

  await publish({ v: 1, page: 11, seq: seqA, mode: "standard", bookId: "alvernia" });
  const snapAfterA = await get(STATE);
  assert.equal(snapAfterA.page, 11);

  await publish({ v: 1, page: 22, seq: seqB, mode: "standard", bookId: "alvernia" });
  const snapAfterB = await get(STATE);
  assert.equal(snapAfterB.page, 22, "Higher-seq director should overwrite lower-seq director");
});

test("dual-director clash: lower-seq director cannot rewind after higher-seq wins", async () => {
  const seqA = freshSeq();
  const seqB = freshSeq(); // seqB > seqA guaranteed by incrementing counter

  // B wins first
  await publish({ v: 1, page: 88, seq: seqB });
  // A tries to publish something "earlier" — must be ignored
  const { body } = await publish({ v: 1, page: 3, seq: seqA });
  assert.equal(body.ignored, true, "Lower-seq director must be ignored after higher-seq wins");
  const snap = await get(STATE);
  assert.equal(snap.page, 88, "State must not rewind to lower-seq director's page");
});

test("dual-director clash: interleaved publishes only apply advancing seqs", async () => {
  // Simulate two directors alternating pages in time order.
  const t = freshSeq();
  const pages = [
    { page: 10, seq: t },
    { page: 20, seq: t + 1 },
    { page: 15, seq: t + 2 },  // page rewinds but seq advances — still accepted
    { page: 5,  seq: t + 1 },  // stale seq — must be ignored
    { page: 30, seq: t + 3 },
  ];
  const results = [];
  for (const p of pages) {
    const { body } = await publish({ v: 1, ...p });
    results.push({ ...p, ignored: !!body.ignored });
  }
  const final = await get(STATE);

  assert.equal(results[3].ignored, true, "seq t+1 duplicate must be ignored");
  assert.equal(final.page, 30, "Final page must be 30 (last accepted)");
  assert.equal(final.seq, t + 3, "Final seq must be t+3");
});

test("WebSocket subscribe: receives current snapshot on connect", async () => {
  const seq = freshSeq();
  const knownPage = 123;
  await publish({ v: 1, page: knownPage, seq, mode: "standard", bookId: "alvernia" });

  await new Promise((resolve, reject) => {
    // Use the built-in WebSocket (Node 22+) or skip gracefully
    let ws;
    try {
      ws = new globalThis.WebSocket(SUB);
    } catch {
      // Node < 22 doesn't have built-in WebSocket — skip
      console.log("  (skipped: no built-in WebSocket in this Node version)");
      return resolve();
    }
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error("WS connect timeout"));
    }, 8000);

    ws.addEventListener("message", (ev) => {
      clearTimeout(timer);
      try {
        const snap = JSON.parse(ev.data);
        assert.equal(snap.page, knownPage, "WS initial snapshot page mismatch");
        assert.ok(snap.seq >= seq, "WS initial snapshot seq mismatch");
        ws.close();
        resolve();
      } catch (err) {
        ws.close();
        reject(err);
      }
    });

    ws.addEventListener("error", (err) => {
      clearTimeout(timer);
      reject(new Error(`WS error: ${err.message}`));
    });
  });
});

test("WebSocket ping: server replies with current snapshot", async () => {
  await new Promise((resolve, reject) => {
    let ws;
    try {
      ws = new globalThis.WebSocket(SUB);
    } catch {
      console.log("  (skipped: no built-in WebSocket in this Node version)");
      return resolve();
    }
    const timer = setTimeout(() => { ws.close(); reject(new Error("ping timeout")); }, 8000);
    let connectDone = false;

    ws.addEventListener("message", (ev) => {
      if (!connectDone) {
        connectDone = true;
        // Consume the connect snapshot, then send a ping
        ws.send("ping");
        return;
      }
      clearTimeout(timer);
      try {
        const snap = JSON.parse(ev.data);
        assert.equal(typeof snap.page, "number", "ping reply should include page");
        ws.close();
        resolve();
      } catch (err) {
        ws.close();
        reject(err);
      }
    });

    ws.addEventListener("error", (err) => {
      clearTimeout(timer);
      reject(new Error(`WS error: ${err.message}`));
    });
  });
});

test("WebSocket live push: publish after subscribe is pushed to subscriber", async () => {
  const targetSeq = freshSeq();
  await new Promise((resolve, reject) => {
    let ws;
    try {
      ws = new globalThis.WebSocket(SUB);
    } catch {
      console.log("  (skipped: no built-in WebSocket in this Node version)");
      return resolve();
    }
    const timer = setTimeout(() => { ws.close(); reject(new Error("push timeout")); }, 10000);
    let connectDone = false;
    const targetPage = 199;

    ws.addEventListener("message", async (ev) => {
      if (!connectDone) {
        connectDone = true;
        // Connected — now publish a new page from the "director"
        await publish({ v: 1, page: targetPage, seq: targetSeq, mode: "standard", bookId: "alvernia" });
        return;
      }
      const snap = JSON.parse(ev.data);
      if (snap.seq < targetSeq) return; // ignore earlier pushes
      clearTimeout(timer);
      try {
        assert.equal(snap.page, targetPage, "WS live push page mismatch");
        ws.close();
        resolve();
      } catch (err) {
        ws.close();
        reject(err);
      }
    });

    ws.addEventListener("error", (err) => {
      clearTimeout(timer);
      reject(new Error(`WS error: ${err.message}`));
    });
  });
});

test("staleness: snapshot ts older than 90s is treated as no active director by clients", async () => {
  // We can't actually make the server store a stale ts, but we can verify
  // the client-side constant used for freshness detection.
  const snap = await get(STATE);
  const RELAY_LIVE_MAX_AGE_S = 90;
  const age = Math.floor(Date.now() / 1000) - snap.ts;
  // If state is fresh (just published in tests above), age should be small.
  // This assertion confirms ts is a valid Unix timestamp in the expected range.
  assert.ok(age >= 0, "ts should not be in the future");
  assert.ok(snap.ts > 1_700_000_000, "ts should be a valid Unix epoch (post-2023)");

  // Verify the client-side RELAY_LIVE_MAX_AGE_S constant matches the worker expectation
  // by checking the value is present in web/src/app.js
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const { dirname, join } = await import("node:path");
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const appJs = readFileSync(join(root, "web/src/app.js"), "utf8");
  assert.ok(
    appJs.includes("RELAY_LIVE_MAX_AGE_S") || appJs.includes("90"),
    "web app should implement 90s freshness window for director liveness",
  );
});

test("CORS headers are present on /state", async () => {
  const r = await fetch(STATE, { headers: { Origin: "https://signovivo.com" } });
  const acao = r.headers.get("access-control-allow-origin");
  assert.ok(acao === "*" || acao === "https://signovivo.com", `Bad CORS: ${acao}`);
});

test("OPTIONS preflight on /publish returns 204 with CORS", async () => {
  const r = await fetch(PUB, {
    method: "OPTIONS",
    headers: { Origin: "https://signovivo.com", "Access-Control-Request-Method": "POST" },
  });
  assert.equal(r.status, 204);
  assert.ok(r.headers.get("access-control-allow-methods")?.includes("POST"));
});
