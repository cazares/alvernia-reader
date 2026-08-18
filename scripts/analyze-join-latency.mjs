#!/usr/bin/env node
/**
 * analyze-join-latency — "everyone starts on a different song, one becomes director: how long
 * until every follower is on the director's page?"
 *
 * THE TEST (owner, 2026-08-17):
 *   1. start every device on a DIFFERENT song
 *   2. make one of them director
 *   3. measure how long each follower takes to converge
 *
 * That third step is the whole product. A follower that converges in 0.2 s and a follower that
 * converges in 112 s both look like "it works" in a room; only one of them works at Mass. And a
 * follower that never converges is the failure being chased right now — one iPad sitting on song 11
 * while the director and everyone else are on 372.
 *
 * WHY A SEPARATE TOOL FROM analyze-resync. That one asks whether a follower obeys a re-assertion of
 * a page it thinks it is already on. This asks how fast a COLD follower converges on a director that
 * just appeared. Different question, different failure: the first is a wedge, this is latency (or a
 * device that is not in the mesh at all).
 *
 * WHAT IT MEASURES, and how each conclusion is reachable:
 *   - director assert  : `page:send` / `become:director` from the director device
 *   - follower receipt : `mesh:page-recv` (logged BEFORE any de-dup, so it fires on every packet)
 *   - convergence      : the first receipt carrying the director's page
 *   - NEVER converged  : a follower that reported at all but never received that page. This is the
 *                        one worth shouting about, so it is called out separately from a slow one.
 *
 * SILENCE IS NOT SUCCESS. A device that reported nothing is INCONCLUSIVE, never a pass — at Mass the
 * followers are on no network and cannot report at all, which is exactly how a broken fleet once got
 * read as a flawless one. Devices seen in the capture but silent in the window are named.
 *
 * FOLDED ROWS: the relay collapses identical consecutive events and bumps `n`. Latency uses the
 * FIRST timestamp of a row, which is what folding preserves.
 *
 * Usage:
 *   node scripts/analyze-join-latency.mjs <capture.jsonl> [--build 441] [--since HH:MM:SS]
 */
import fs from "node:fs";

const args = process.argv.slice(2);
const file = args.find((a) => !a.startsWith("--"));
const flag = (n) => {
  const i = args.indexOf(n);
  return i >= 0 && args[i + 1] ? args[i + 1] : null;
};
const onlyBuild = flag("--build");
const since = flag("--since");

if (!file) {
  console.error("usage: node scripts/analyze-join-latency.mjs <capture.jsonl> [--build N] [--since HH:MM:SS]");
  process.exit(2);
}

const utc = (ms) => new Date(ms).toISOString().slice(11, 19);
const ct = (ms) => new Date(ms - 5 * 3600 * 1000).toISOString().slice(11, 19);

const seen = new Set();
let rows = [];
for (const line of fs.readFileSync(file, "utf8").split("\n")) {
  const s = line.trim();
  if (!s) continue;
  let r;
  try { r = JSON.parse(s); } catch { continue; }
  if (!r || typeof r.t !== "number" || !r.dev) continue;
  if (onlyBuild && String(r.build) !== String(onlyBuild)) continue;
  const key = `${r.t}|${r.dev}|${r.event}|${r.peer ?? ""}|${r.page ?? ""}`;
  if (seen.has(key)) continue;
  seen.add(key);
  rows.push(r);
}
rows.sort((a, b) => a.t - b.t);

if (since) {
  const [h, m, sec] = since.split(":").map(Number);
  rows = rows.filter((r) => {
    const d = new Date(r.t);
    return d.getUTCHours() * 3600 + d.getUTCMinutes() * 60 + d.getUTCSeconds() >= h * 3600 + m * 60 + (sec || 0);
  });
}
if (!rows.length) {
  console.error("✖ no rows in window — INCONCLUSIVE (exit 2).");
  process.exit(2);
}

const isSwift = (r) => r.src === "swift";
// mesh:page-recv and page:send are JS-layer events; session/found/lost are Swift. Same devices,
// two names — never merge the rosters (see analyze-resync.mjs).
const jsDevices = [...new Set(rows.filter((r) => !isSwift(r)).map((r) => r.dev))];

// ── Who directed, and when did they first assert? ─────────────────────────────
const becomes = rows.filter((r) => r.event === "become:director");
const sends = rows.filter((r) => r.event === "page:send");
const director = becomes.at(-1)?.dev ?? sends.at(-1)?.dev ?? null;
const assertAt = becomes.at(-1)?.t ?? sends[0]?.t ?? null;

console.log(`\n=== join latency — ${file.split("/").pop()} ===`);
console.log(`window ${utc(rows[0].t)}–${utc(rows.at(-1).t)} UTC (${ct(rows[0].t)}–${ct(rows.at(-1).t)} CT) · ${rows.length} rows`);
console.log(`director: ${director ?? "UNKNOWN — no become:director or page:send in window"}`);
if (onlyBuild) console.log(`build gate: ${onlyBuild}`);
if (assertAt === null) {
  console.error("\n✖ no director assertion found — INCONCLUSIVE (exit 2). Did the director take the role inside this window?");
  process.exit(2);
}
console.log(`took the role at ${utc(assertAt)}Z (${ct(assertAt)} CT)`);

// The page the director was actually on, from its own sends after taking the role.
const dirSends = sends.filter((r) => r.dev === director && r.t >= assertAt);
const targetPage = dirSends.at(-1)?.page ?? dirSends[0]?.page ?? null;
console.log(`director's page: ${targetPage ?? "unknown (no page:send after the assert)"}`);

// ── Per-follower convergence ──────────────────────────────────────────────────
console.log(`\n${"follower".padEnd(16)}${"first recv".padStart(12)}${"converged".padStart(12)}${"latency".padStart(10)}  note`);
console.log("-".repeat(74));

const followers = jsDevices.filter((d) => d !== director);
const results = [];
for (const dev of followers) {
  const recv = rows.filter((r) => r.dev === dev && r.event === "mesh:page-recv" && r.t >= assertAt);
  const first = recv[0];
  const hit = targetPage == null ? recv[0] : recv.find((r) => Number(r.page) === Number(targetPage));
  const lat = hit ? (hit.t - assertAt) / 1000 : null;
  results.push({ dev, lat, any: recv.length });
  const note = !recv.length
    ? "NEVER RECEIVED ANYTHING — not in the mesh"
    : !hit
      ? `NEVER CONVERGED — got pages ${[...new Set(recv.map((r) => r.page))].slice(0, 6).join(",")}`
      : lat > 10 ? "slow" : "";
  console.log(
    dev.padEnd(16) +
      (first ? utc(first.t) : "—").padStart(12) +
      (hit ? utc(hit.t) : "—").padStart(12) +
      (lat === null ? "—" : `${lat.toFixed(1)}s`).padStart(10) +
      "  " + note,
  );
}
if (!followers.length) console.log("  (no JS-layer followers reported)");

// ── Verdict ───────────────────────────────────────────────────────────────────
const converged = results.filter((r) => r.lat !== null);
const stuck = results.filter((r) => r.lat === null);
console.log(`\n=== VERDICT ===`);
if (!results.length) {
  console.log("INCONCLUSIVE — no followers reported. On a network-less device telemetry does not");
  console.log("exist, and silence must never be read as success.");
  process.exit(2);
}
if (converged.length) {
  const ls = converged.map((r) => r.lat).sort((a, b) => a - b);
  const med = ls[Math.floor(ls.length / 2)];
  console.log(`${converged.length}/${results.length} converged · median ${med.toFixed(1)}s · worst ${ls.at(-1).toFixed(1)}s`);
}
if (stuck.length) {
  console.log(`\n🔴 ${stuck.length} follower(s) NEVER reached the director's page: ${stuck.map((r) => r.dev).join(", ")}`);
  console.log(`   A follower that received OTHER pages but not this one is a wedge — see`);
  console.log(`   scripts/analyze-resync.mjs. One that received nothing at all never joined the mesh.`);
  process.exit(1);
}
console.log("\n✅ every reporting follower reached the director's page.");
