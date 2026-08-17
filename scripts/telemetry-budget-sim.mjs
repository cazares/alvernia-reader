#!/usr/bin/env node
// Telemetry budget simulator — "how many Cloudflare requests would config X have cost?"
//
// WHY THIS EXISTS (2026-08-17): the account tripped Cloudflare's free-plan Workers cap
// (100,000 requests/day, error 1027) and production went 429. Measured that day:
//
//     account-wide total      99,428 / 100,000     <- cap
//     signovivo-sync          87,258  (87.8%)
//     everything else         ~12,000  (10 client sites, 23 workers, 39 zones)
//
// `dbgLog` POSTs one request per mesh event with no batching, so a device in a bad
// discovery loop is an unbounded traffic generator. The fix has to be SIZED, not guessed:
// this replays real capture JSONL through candidate client-side configs and reports the
// request count each would have produced.
//
// It answers the only question that matters: does config X fit in the budget?
//
// Usage:
//   node scripts/telemetry-budget-sim.mjs <capture.jsonl> [more.jsonl ...]
//   node scripts/telemetry-budget-sim.mjs --day-requests 87258 run*.jsonl
//
// `--day-requests N` scales the simulated reduction ratio up to a real measured day, since
// a capture only covers the window it was running for. The RATIO is what generalises; the
// absolute count from one capture does not.

import fs from "node:fs";

const args = process.argv.slice(2);
let dayRequests = null;
const files = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--day-requests") dayRequests = Number(args[++i]);
  else files.push(args[i]);
}
if (!files.length) {
  console.error("usage: node scripts/telemetry-budget-sim.mjs [--day-requests N] <capture.jsonl> ...");
  process.exit(2);
}

// ── Load + de-duplicate ────────────────────────────────────────────────────────
// The capture tool re-polls a ring buffer every few seconds, so the SAME row appears in
// many polls. Counting raw lines overstates traffic several-fold. Identity is the client
// timestamp + device + event + subject.
const seen = new Set();
const events = [];
for (const f of files) {
  let text;
  try {
    text = fs.readFileSync(f, "utf8");
  } catch (e) {
    console.error(`skip ${f}: ${e.message}`);
    continue;
  }
  for (const line of text.split("\n")) {
    const s = line.trim();
    if (!s) continue;
    let r;
    try {
      r = JSON.parse(s);
    } catch {
      continue;
    }
    const key = `${r.t}|${r.dev}|${r.event}|${r.peer ?? ""}|${r.page ?? ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (typeof r.t === "number" && r.dev) events.push(r);
  }
}
events.sort((a, b) => a.t - b.t);
if (!events.length) {
  console.error("no usable rows — INCONCLUSIVE");
  process.exit(2);
}

const spanMin = (events.at(-1).t - events[0].t) / 60000;
const devices = new Set(events.map((e) => e.dev));

// ── Candidate configs ──────────────────────────────────────────────────────────
// Each returns the number of POSTs a device fleet would have issued.
//
// `coalesce` collapses repeats of the same (event, subject) INSIDE one flush window into a
// single entry carrying a count. This is not sampling: nothing is lost except the fact that
// N identical rows were separate HTTP requests. The relay already run-length folds on the
// server (logBuffer.js foldLogEntries), so this just moves the same fold earlier, where it
// actually saves a request.
//
// `drop` is real sampling and DOES lose rows — used only for the highest-volume, lowest-
// information-per-row events.
const MAX_BATCH = 200; // sync-worker/src/logBuffer.js LOG_MAX_BATCH

function simulate({ windowMs, coalesce = false, keepEvery = {} }) {
  const perDevice = new Map();
  for (const e of events) {
    if (!perDevice.has(e.dev)) perDevice.set(e.dev, []);
    perDevice.get(e.dev).push(e);
  }
  let posts = 0;
  let kept = 0;
  const counters = new Map();
  for (const [dev, list] of perDevice) {
    const buckets = new Map();
    for (const e of list) {
      const every = keepEvery[e.event] ?? keepEvery[e.event?.split(":")[0] + ":*"] ?? 1;
      if (every > 1) {
        const ck = `${dev}|${e.event}`;
        const n = (counters.get(ck) ?? 0) + 1;
        counters.set(ck, n);
        if (n % every !== 0) continue; // sampled out
      }
      const b = Math.floor(e.t / windowMs);
      if (!buckets.has(b)) buckets.set(b, []);
      buckets.get(b).push(e);
    }
    for (const [, rows] of buckets) {
      let n = rows.length;
      if (coalesce) {
        const uniq = new Set(rows.map((r) => `${r.event}|${r.peer ?? ""}|${r.page ?? ""}`));
        n = uniq.size;
      }
      kept += n;
      posts += Math.max(1, Math.ceil(n / MAX_BATCH)); // one POST per window, split if huge
    }
  }
  return { posts, kept };
}

// ── Report ─────────────────────────────────────────────────────────────────────
const baseline = events.length; // today: one POST per event
console.log(`\n=== Telemetry budget simulation ===`);
console.log(`capture: ${events.length.toLocaleString()} unique events · ${devices.size} devices · ${spanMin.toFixed(0)} min span`);
console.log(`baseline (today's behaviour: 1 POST per event): ${baseline.toLocaleString()} requests`);
if (dayRequests) console.log(`measured real day for signovivo-sync: ${dayRequests.toLocaleString()} requests`);

const HOT = { "mesh:page-recv": 4, "ble:page-recv": 4, "mesh:state": 2 };
const configs = [
  ["batch 5s", { windowMs: 5000 }],
  ["batch 10s", { windowMs: 10000 }],
  ["batch 15s", { windowMs: 15000 }],
  ["batch 30s", { windowMs: 30000 }],
  ["batch 10s + coalesce", { windowMs: 10000, coalesce: true }],
  ["batch 15s + coalesce", { windowMs: 15000, coalesce: true }],
  ["batch 30s + coalesce", { windowMs: 30000, coalesce: true }],
  ["batch 15s + coalesce + sample hot", { windowMs: 15000, coalesce: true, keepEvery: HOT }],
  ["batch 30s + coalesce + sample hot", { windowMs: 30000, coalesce: true, keepEvery: HOT }],
];

console.log(
  `\n${"config".padEnd(36)}${"POSTs".padStart(9)}${"cut".padStart(8)}${"rows kept".padStart(11)}${"fidelity".padStart(10)}${"→ real day".padStart(12)}`,
);
console.log("-".repeat(86));
for (const [name, cfg] of configs) {
  const { posts, kept } = simulate(cfg);
  const cut = baseline / posts;
  const projected = dayRequests ? Math.round(dayRequests / cut) : null;
  console.log(
    name.padEnd(36) +
      posts.toLocaleString().padStart(9) +
      `${cut.toFixed(1)}x`.padStart(8) +
      kept.toLocaleString().padStart(11) +
      `${((100 * kept) / baseline).toFixed(0)}%`.padStart(10) +
      (projected !== null ? projected.toLocaleString().padStart(12) : "".padStart(12)),
  );
}

console.log(
  `\nfidelity = share of original rows still delivered. Coalescing keeps 100% of distinct\n` +
    `(event, peer, page) facts; only duplicate rows inside one window collapse.\n`,
);
