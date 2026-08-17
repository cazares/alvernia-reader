#!/usr/bin/env node
/**
 * stress-analyze — score a stress-capture JSONL against the group-session fix.
 *
 * WHY THIS EXISTS. `stress-capture.mjs` produces the timeline; reading it by eye is how the
 * follower-takes-a-follower-for-the-director bug hid for 48 builds. The three numbers that decide
 * whether the fix works are all derived, not printed: you have to know which device was the
 * director before a `session:connected` row means anything. This computes them the same way every
 * time, so a before-run and an after-run are scored by identical arithmetic.
 *
 * THE VERDICT (build 430, commit 1be7719):
 *   1. follower<->follower `session:connected` pairs  ... must be ZERO
 *   2. `watchdog:half-open-reconnect`                 ... must be ~0 (each one is a torn-down session)
 *   3. `session:peer-not-director`                    ... POSITIVE proof the new guard fired
 *
 * (3) is the one that cannot be faked by a quiet room: a test where no follower ever tried to
 * cross-connect produces zero of everything and looks identical to a working fix. If (1) is zero
 * AND (3) is zero, the run proved nothing — it needs THREE+ devices to reproduce at all, because
 * with a single follower there is no second follower to cross-connect with.
 *
 * FOLDED ROWS. The worker collapses a run of identical events into one row and bumps `n`. Every
 * count here sums `n`, never rows — counting rows undercounts by ~3x on a busy mesh.
 *
 * LAYERS. A Swift peer name (`iPad-A5FDF5`) and a JS device id (`5eut7o`) are the SAME physical
 * device seen from two layers. Mesh/session events are Swift-side and are the ones that matter
 * here; the JS ids are reported for orientation only.
 *
 * Usage:
 *   node scripts/stress-analyze.mjs <capture.jsonl> [--baseline <before.jsonl>]
 */
import fs from "node:fs";

const args = process.argv.slice(2);
const file = args.find((a) => !a.startsWith("--"));
const baselineIdx = args.indexOf("--baseline");
const baselineFile = baselineIdx >= 0 ? args[baselineIdx + 1] : null;

if (!file) {
  console.error("usage: node scripts/stress-analyze.mjs <capture.jsonl> [--baseline <before.jsonl>]");
  process.exit(2);
}

const read = (p) =>
  fs
    .readFileSync(p, "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((l) => JSON.parse(l));

/** Count of an event, summing the worker's fold counter. */
const countEvent = (rows, event) =>
  rows.filter((r) => r.event === event).reduce((a, r) => a + (r.n || 1), 0);

function analyze(rows, label) {
  // Role map from the Swift layer. A device that ever advertised as director IS the director --
  // `off` and transient roles never override it, because only a human makes a director (v428).
  const roles = new Map();
  for (const r of rows) {
    if (!r.dev || !r.role) continue;
    const prev = roles.get(r.dev);
    if (r.role === "director" || !prev) roles.set(r.dev, r.role === "off" && prev ? prev : r.role);
  }
  const directors = [...roles.entries()].filter(([, v]) => v === "director").map(([k]) => k);
  const isDirector = (name) => directors.includes(name);

  // THE metric. A follower reporting `session:connected` for a peer that is not a director was the
  // whole bug: an MCSession is a group, so those rows exist -- but before the fix the handler then
  // assigned that peer as `connectedDirectorPeer`.
  const f2f = [];
  const f2d = [];
  for (const r of rows) {
    if (r.event !== "session:connected") continue;
    if (r.role !== "follower" || !r.peer) continue;
    (isDirector(r.peer) ? f2d : f2f).push(r);
  }

  const builds = [...new Set(rows.map((r) => r.build).filter(Boolean))].sort();
  const halfOpen = countEvent(rows, "watchdog:half-open-reconnect");
  const notDirector = countEvent(rows, "session:peer-not-director");

  // Heartbeat / page delivery per follower -- the "519 / 29 / 4" signature of the bug.
  const recv = new Map();
  for (const r of rows) {
    if (r.event !== "mesh:page-recv" || !r.dev) continue;
    recv.set(r.dev, (recv.get(r.dev) || 0) + (r.n || 1));
  }

  const span = (() => {
    const ts = rows.map((r) => r.t0 || r.t).filter(Boolean);
    if (!ts.length) return "unknown";
    const mins = (Math.max(...ts) - Math.min(...ts)) / 60000;
    return `${mins.toFixed(1)} min`;
  })();

  return {
    label,
    rows: rows.length,
    builds,
    span,
    directors,
    followers: [...roles.entries()].filter(([, v]) => v !== "director").map(([k]) => k),
    f2fCount: f2f.reduce((a, r) => a + (r.n || 1), 0),
    f2fPairs: [...new Set(f2f.map((r) => `${r.dev} -> ${r.peer}`))],
    f2dCount: f2d.reduce((a, r) => a + (r.n || 1), 0),
    halfOpen,
    notDirector,
    recv: [...recv.entries()].sort((a, b) => b[1] - a[1]),
  };
}

function report(a) {
  console.log(`\n=== ${a.label} ===`);
  console.log(`rows ${a.rows} · builds ${a.builds.join(",") || "?"} · span ${a.span}`);
  console.log(`director(s): ${a.directors.join(", ") || "(none seen)"}`);
  console.log(`followers  : ${a.followers.join(", ") || "(none seen)"}`);
  console.log("");
  console.log(`  follower->follower session:connected : ${a.f2fCount}   ${a.f2fCount === 0 ? "✅" : "❌"}`);
  for (const p of a.f2fPairs) console.log(`      ${p}`);
  console.log(`  follower->director session:connected : ${a.f2dCount}   (healthy, expected > 0)`);
  console.log(`  watchdog:half-open-reconnect         : ${a.halfOpen}   ${a.halfOpen === 0 ? "✅" : a.halfOpen <= 2 ? "⚠️" : "❌"}`);
  console.log(`  session:peer-not-director (guard)    : ${a.notDirector}   ${a.notDirector > 0 ? "✅ guard fired" : "— not seen"}`);
  console.log("\n  page/heartbeat delivery per device:");
  for (const [d, n] of a.recv) console.log(`      ${String(n).padStart(5)}  ${d}`);
}

const cur = analyze(read(file), `CAPTURE ${file}`);
if (baselineFile) report(analyze(read(baselineFile), `BASELINE ${baselineFile}`));
report(cur);

// ---- verdict -------------------------------------------------------------
const guardPresent = cur.builds.some((b) => Number(b) >= 430);
console.log("\n=== VERDICT ===");
const fail = [];
if (cur.f2fCount > 0) fail.push(`${cur.f2fCount} follower->follower session:connected (must be 0)`);
if (cur.halfOpen > 2) fail.push(`${cur.halfOpen} half-open reconnects (must be ~0)`);
if (guardPresent && cur.notDirector === 0 && cur.f2fCount === 0) {
  fail.push(
    "guard never fired AND no cross-connects seen — this run did not exercise the bug. " +
      "Re-run with 3+ devices (1 director + 2+ followers); 2 devices always pass."
  );
}
if (!guardPresent) {
  console.log("⚠️  No build >= 430 in this capture — this is a PRE-FIX baseline, not a verification.");
}
if (fail.length) {
  console.log("❌ FAIL");
  for (const f of fail) console.log("   · " + f);
  process.exit(1);
}
console.log("✅ PASS — no follower mistook a fellow follower for the director.");
