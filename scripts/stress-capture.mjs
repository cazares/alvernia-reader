#!/usr/bin/env node
/**
 * stress-capture — turn the relay's diagnostic ring buffer into a permanent, de-duplicated
 * timeline of a live mesh test.
 *
 * WHY THIS EXISTS. `GET /log` returns a ring buffer: the newest N rows, and older ones are gone
 * forever. That is fine for glancing at a mesh, and useless for a stress test, where the whole
 * point is to compare what happened at minute two against what happened at minute twenty. This
 * polls the buffer, keeps every row it has ever seen, and writes an append-only JSONL file that
 * outlives the session.
 *
 * GET is deliberately the right call to poll. It is NOT rate-limited (only publish, /log POST and
 * the fleet check-in are), so polling costs the DEVICES nothing — it never competes with the
 * telemetry it is trying to capture. Polling the write path would have been self-defeating.
 *
 * De-duplication is by content, not by index: the buffer is a moving window, so consecutive polls
 * overlap almost entirely. Rows also MUTATE in place — the worker folds a run of identical
 * keepalives into one row and bumps its count — so a row's identity is its device + event +
 * subject + FIRST timestamp, and a later poll carrying a higher count REPLACES the earlier copy
 * rather than appearing twice.
 *
 * Usage:
 *   node scripts/stress-capture.mjs --key <FLEET_DASHBOARD_KEY> [--out FILE] [--interval 5]
 *   node scripts/stress-capture.mjs --key ... --clear     # wipe the buffer first (start a scenario)
 *
 * The key is passed as a HEADER, never as ?k=. The worker accepts both, but it runs with
 * observability enabled, so a query parameter puts the secret into Cloudflare's invocation logs
 * and into shell history.
 */
import fs from "node:fs";

const RELAY = process.env.SV_RELAY || "https://signovivo-sync.4j4982y8jp.workers.dev";
const args = process.argv.slice(2);
const arg = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const KEY = arg("--key", process.env.FLEET_KEY || "");
const OUT = arg("--out", "stress-capture.jsonl");
const INTERVAL_S = Number(arg("--interval", "5"));
const CLEAR = args.includes("--clear");

if (!KEY) {
  console.error("✖ need --key <FLEET_DASHBOARD_KEY> (or FLEET_KEY in the environment)");
  process.exit(2);
}

const headers = { "X-Fleet-Key": KEY };

/** Identity of a row across polls. Must NOT include the mutable count or last-seen timestamp. */
const rowId = (r) => {
  const subject = ["page", "peer", "to", "from", "target", "status", "dup", "code"]
    .map((k) => (r[k] === undefined ? "" : String(r[k])))
    .join("|");
  // t0 is set once the row starts folding; before that, t IS the first timestamp.
  const first = r.t0 !== undefined ? r.t0 : r.t;
  return [r.dev, r.event, subject, first].join("~");
};

const seen = new Map(); // id -> the highest-count copy written so far
let polls = 0;
let written = 0;

const poll = async () => {
  let body;
  try {
    const res = await fetch(`${RELAY}/log`, { headers });
    if (!res.ok) {
      process.stderr.write(`\n! /log -> ${res.status}\n`);
      return;
    }
    body = await res.json();
  } catch (err) {
    process.stderr.write(`\n! poll failed: ${String(err && err.message)}\n`);
    return; // a transient network blip must never end the capture
  }
  polls += 1;
  const rows = Array.isArray(body.entries) ? body.entries : [];
  const fresh = [];
  for (const r of rows) {
    if (!r || typeof r !== "object") continue;
    const id = rowId(r);
    const prev = seen.get(id);
    const n = typeof r.n === "number" ? r.n : 1;
    // A folded row grows in place; only re-emit when it actually carries more than last time.
    if (prev !== undefined && n <= prev) continue;
    seen.set(id, n);
    fresh.push({ ...r, capturedAt: Date.now() });
  }
  if (fresh.length) {
    fs.appendFileSync(OUT, fresh.map((r) => JSON.stringify(r)).join("\n") + "\n");
    written += fresh.length;
  }
  process.stdout.write(`\rpolls ${polls} · buffer ${rows.length} · captured ${written} rows -> ${OUT}   `);
};

const main = async () => {
  if (CLEAR) {
    const res = await fetch(`${RELAY}/log`, { method: "DELETE", headers });
    console.log(res.ok ? "✅ buffer cleared — scenario starts from empty" : `✖ clear failed: ${res.status}`);
  }
  console.log(`capturing every ${INTERVAL_S}s → ${OUT}   (Ctrl-C to stop)`);
  await poll();
  setInterval(poll, INTERVAL_S * 1000);
};

main();
