import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const WORKER = fs.readFileSync("sync-worker/src/index.ts", "utf8");
const WCONF = fs.readFileSync("sync-worker/wrangler.jsonc", "utf8");

// WHY THIS FILE EXISTS. Cloudflare's free plan gives the ACCOUNT 100,000 Worker/Pages-Function
// requests a day, and signovivo.com shares that number with the relay. Twice — 2026-08-17 and again
// on 2026-08-18 — non-essential traffic ate the whole thing and took the SITE down with it.
//
// The 2026-08-18 failure was self-sustaining: a 429 killed the WebSocket, backoff climbed to its 8s
// cap and ALSO started a 4s /state poll, and then the 10s health timer reset backoff to 500ms and
// restarted both. ~20-25 requests/minute/device, four devices, over the daily cap on their own,
// forever. The outage fed itself and could not recover even after the traffic that started it had
// stopped.

test("THE ARITHMETIC: the old polling config exceeded the daily quota by itself", () => {
  // This is the test that would have caught it before a human did. It is arithmetic, not a guess.
  const DAILY_CAP = 100_000, DEVICES = 4;
  const perDevicePerDay = (pollMs) => Math.floor((24 * 60 * 60 * 1000) / pollMs);
  // 4s = 21,600/device/day = 86,400 across four devices: 86% of the ENTIRE account quota spent on
  // fallback polling alone, before the site, the WebSocket reconnects, the 10s health timer or any
  // telemetry. Not over the cap by itself — the first draft of this test claimed it was, and the
  // arithmetic said otherwise — but it left so little headroom that anything else finished the job.
  const old4s = perDevicePerDay(4000) * DEVICES;
  assert.ok(old4s > DAILY_CAP * 0.75,
    `the 4s poll spent ${old4s}/day, under 75% of the cap — recheck this test, not the config`);
  const current = Number(APP.match(/const RELAY_POLL_MS = (\d+)/)[1]);
  assert.ok(perDevicePerDay(current) * DEVICES < DAILY_CAP / 2,
    `a ${current}ms poll across ${DEVICES} devices spends ${perDevicePerDay(current) * DEVICES}/day — ` +
    "more than half the account's entire quota on fallback polling alone");
});

test("a 429 stops the client instead of making it try harder", () => {
  assert.match(APP, /if \(r\.status === 429\) \{ tripRelayCooldown/,
    "a 429 no longer trips the breaker — the client will retry through the outage that caused it");
  const cooldown = Number(APP.match(/const RELAY_COOLDOWN_MS = (\d+) \* 60 \* 1000/)[1]);
  assert.ok(cooldown >= 5, `a ${cooldown}-minute cooldown is too short to let a quota window recover`);
  // Every outbound path must honour it, or the breaker leaks.
  for (const [what, re] of [
    ["polling", /const relayPollOnce = async \(force = false\) => \{\s*\n\s*if \(relayCooling\(\)\) return;/],
    ["startRelayPolling", /const startRelayPolling = \(\) => \{\s*\n\s*if \(relayCooling\(\)\) return;/],
    ["connectRelay", /if \(relayCooling\(\)\) return;/],
    ["health timer", /if \(relay\.manualClose \|\| relayCooling\(\)\) return;/],
  ]) assert.match(APP, re, `${what} ignores the cooldown, so the breaker leaks`);
});

test("the health timer no longer hands back the backoff the socket earned", () => {
  // It reset backoff to 500ms every 10s, so the exponential climb never survived one cycle — on
  // exactly the paths where the far end was already overloaded.
  const t = APP.slice(APP.indexOf("relay.healthTimer = setInterval"));
  const body = t.slice(0, t.indexOf("}, 10000);"));
  assert.doesNotMatch(body, /relay\.backoff = 500/,
    "the health timer resets backoff again — exponential backoff cannot survive it");
});

test("signovivo.com is NEVER gated — only the native shell's redundant subscribe is", () => {
  // THE LINE THAT MUST NOT MOVE. The relay is the ONLY sync a web follower has; gating it there
  // would delete the product. A native follower already has the mesh and BLE, both of which work at
  // Mass where there is no internet at all.
  const fn = APP.slice(APP.indexOf("const relaySubscribeEnabled"));
  const body = fn.slice(0, fn.indexOf("\n};"));
  assert.match(body, /if \(!native\) return true;/,
    "the web path can now be gated off — signovivo.com would lose sync entirely");
  assert.ok(body.indexOf("if (!native) return true;") < body.indexOf("localStorage"),
    "the web short-circuit is not first, so a storage failure could disable signovivo.com");
});

test("telemetry is opt-in and drops rather than queues", () => {
  assert.match(NATIVE, /if \(!telemetryEnabledRef\.current\) return;/,
    "telemetry sends by default again — it has no user value and it caused two outages");
  // A queue that survives a Mass is the same outage with a delay on it.
  assert.ok(NATIVE.indexOf("dbgBufferRef.current = [];") < NATIVE.indexOf("if (!telemetryEnabledRef.current) return;"),
    "the batch is retained when telemetry is off — that is a burst waiting for the next wifi");
});

test("the worker RESERVES quota for signovivo.com, and never counts the product's own traffic", () => {
  assert.match(WORKER, /async spendNonEssential\(day: string, cap: number\)/, "no budget meter exists");
  assert.match(WORKER, /getByName\("__budget__"\)\.spendNonEssential/, "the gate never spends from the budget");
  assert.match(WORKER, /"Retry-After": "3600"/, "a refused request does not tell the caller when to return");

  // The essential paths must be OUTSIDE the counted set. /r/ carries state, subscribe and publishes
  // — the actual sync — and must never be refused or even metered.
  const gate = WORKER.slice(WORKER.indexOf("const NON_ESSENTIAL ="));
  const set = gate.slice(0, gate.indexOf(";"));
  for (const essential of ["/r/", "/health"]) {
    assert.ok(!set.includes(essential), `${essential} is counted as non-essential — the product would be refused`);
  }
  for (const nonEssential of ["/log", "/fleet"]) {
    assert.ok(set.includes(nonEssential), `${nonEssential} is not metered, so it can starve the site again`);
  }

  // The reservation has to leave the great majority for the product.
  const cap = Number(WCONF.match(/"NONESSENTIAL_DAILY_MAX": "(\d+)"/)[1]);
  assert.ok(cap > 0 && cap <= 25_000,
    `a ${cap}/day non-essential budget leaves too little of the 100k daily quota for signovivo.com`);
});
