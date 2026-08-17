#!/usr/bin/env node
/**
 * analyze-resync — score the "director backgrounds, followers wander, nobody comes back" bug.
 *
 * THE SCENARIO (reported by the owner, 2026-08-17):
 *
 *   1. director sits on song 16
 *   2. director backgrounds (~30 s, more or less)
 *   3. followers navigate away by hand — iPad 1 to 14, iPad 2 to 19, iPhone elsewhere
 *   4. director foregrounds, still on 16
 *   5. NOBODY returns to 16. The iPads sit on 14 and 19 indefinitely.
 *   6. the director then turns to 15 — and every follower snaps to 15 instantly
 *   7. the iPhone, separately, DID eventually reach 16
 *
 * Step 6 is what makes this worth a dedicated tool: the mesh is provably healthy. Sessions,
 * discovery and delivery all work, because a genuine page CHANGE lands immediately on every
 * device. What fails is narrower — re-sending the page the director was ALREADY on does not
 * move a follower that has wandered off it.
 *
 * Step 7 is the tell that points at where. The flakiest device recovered and the two healthy
 * ones did not, which is backwards unless recovery comes from RECONNECTING:
 * `sendCurrentPageSnapshot` fires unconditionally when a peer connects, while the steady-state
 * path is the 1 Hz heartbeat. A dropped-and-restored session gets a snapshot the healthy
 * sessions never receive.
 *
 * WHY `mesh:page-recv` IS THE WHOLE MEASUREMENT. Three candidate causes were each killed by
 * reading the source, so this tool exists to stop the guessing and measure instead:
 *
 *   - follower de-dup (PdfReaderApp.tsx:1971)  — a manual turn DOES update currentPageRef
 *     (:1117 via the web's page-changed at app.js:1636), so 16 !== 14 and the guard is skipped
 *   - follower re-broadcast (PdfReaderApp.tsx:1127) — broadcastPage is role-gated at :666
 *   - director change-gate (DirectorSyncModule.swift sendPageUpdate) — there isn't one; it
 *     always sends
 *
 * The director's heartbeat is INVISIBLE in telemetry: it calls sendNearbyDirectorPageUpdate
 * directly (PdfReaderApp.tsx:714), bypassing broadcastPage's `page:send`, and the Swift side
 * logs nothing. So the follower's `mesh:page-recv` — logged at :1959 BEFORE the dup check, and
 * therefore on every packet including heartbeats — is the only witness to whether the director
 * is still transmitting at all. That single fact splits the problem in half:
 *
 *     rows arriving  -> transport + heartbeat fine  -> the bug is in apply/render
 *     no rows        -> the director went silent    -> the bug is the heartbeat or the session
 *
 * TWO LAYERS, ONE DEVICE. A Swift peer name (`iPad-7A9DAF`) and a JS device id (`5eut7o`) are the
 * SAME physical iPad seen from two places, distinguished by `src`. It matters here because the
 * events split cleanly along that seam: `mesh:page-recv` and `page:send` exist ONLY on the JS
 * layer, while `session:*`, `found` and `lost` exist ONLY in Swift. An early version of this tool
 * listed both and reported "iPad-7A9DAF received 0" — which reads as "this iPad got nothing" when
 * the truth is "iPads never log page receipt under their Swift name." That is a false NEGATIVE of
 * the same family as the four false verdicts stress-analyze shipped: a tool that misreads its own
 * instrument. The two layers are therefore reported separately and never merged into one roster.
 *
 * FOLDED ROWS. The relay collapses a run of identical events into one row and bumps `n`.
 * Every count here sums `n`, never rows — counting rows undercounts by ~3x on a busy mesh.
 *
 * Usage:
 *   node scripts/analyze-resync.mjs <capture.jsonl> [--build 436] [--since HH:MM:SS]
 */
import fs from "node:fs";

const args = process.argv.slice(2);
const file = args.find((a) => !a.startsWith("--"));
const flag = (name) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : null;
};
const onlyBuild = flag("--build");
const since = flag("--since");

if (!file) {
  console.error("usage: node scripts/analyze-resync.mjs <capture.jsonl> [--build N] [--since HH:MM:SS]");
  process.exit(2);
}

const utc = (ms) => new Date(ms).toISOString().slice(11, 19);
const ct = (ms) => new Date(ms - 5 * 3600 * 1000).toISOString().slice(11, 19);

// ── Load ───────────────────────────────────────────────────────────────────────
const seen = new Set();
let rows = [];
for (const line of fs.readFileSync(file, "utf8").split("\n")) {
  const s = line.trim();
  if (!s) continue;
  let r;
  try {
    r = JSON.parse(s);
  } catch {
    continue;
  }
  if (!r || typeof r.t !== "number" || !r.dev) continue;
  if (onlyBuild && String(r.build) !== String(onlyBuild)) continue;
  // Identity: a folded row MUTATES in place (its `n` grows), so a later copy REPLACES an earlier one.
  const key = `${r.t}|${r.dev}|${r.event}|${r.peer ?? ""}|${r.page ?? ""}`;
  if (seen.has(key)) rows = rows.filter((x) => x.__k !== key);
  seen.add(key);
  r.__k = key;
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
  // An empty window is INCONCLUSIVE, never a pass. stress-analyze printed PASS on zero rows
  // once; that is how a run with no telemetry got read as a run with no problems.
  console.error("✖ no rows in window — INCONCLUSIVE (exit 2). Check --build / --since, and that the relay was reachable.");
  process.exit(2);
}

const n = (r) => Number(r.n) || 1;
const isSwift = (r) => r.src === "swift";
const jsDevices = [...new Set(rows.filter((r) => !isSwift(r)).map((r) => r.dev))];
const swiftDevices = [...new Set(rows.filter(isSwift).map((r) => r.dev))];
// The director is identified on the JS layer, where page:send and become:director live.
const director = rows.find((r) => r.event === "become:director" && !isSwift(r))?.dev
  ?? rows.find((r) => r.event === "page:send")?.dev
  ?? rows.find((r) => r.role === "director" && !isSwift(r))?.dev
  ?? null;

console.log(`\n=== resync analysis — ${file.split("/").pop()} ===`);
console.log(`window ${utc(rows[0].t)}–${utc(rows.at(-1).t)} UTC (${ct(rows[0].t)}–${ct(rows.at(-1).t)} CT) · ${rows.length} rows`);
console.log(`layers: ${jsDevices.length} JS ids + ${swiftDevices.length} Swift peer names — TWO VIEWS OF THE SAME DEVICES, not two fleets`);
console.log(`director: ${director ?? "UNKNOWN — no become:director or role=director row"}`);
if (onlyBuild) console.log(`build gate: ${onlyBuild}`);

// ── The background/foreground boundary ─────────────────────────────────────────
const marks = rows.filter((r) =>
  ["bg:grace-begin", "bg:grace-expired", "advertiser:foreground-restart"].includes(r.event),
);
console.log(`\n--- director background/foreground timeline ---`);
if (!marks.length) {
  console.log("  (none — the director never backgrounded in this window, or its telemetry is missing)");
} else {
  for (const m of marks) console.log(`  ${utc(m.t)}Z  ${ct(m.t)}CT  ${m.dev.padEnd(14)} ${m.event}`);
}

// The moment the director came back. advertiser:foreground-restart only fires when the room was
// EMPTY, so fall back to the last grace mark — foreground itself is otherwise unlogged.
const resume = rows.find((r) => r.event === "advertiser:foreground-restart")?.t
  ?? marks.at(-1)?.t
  ?? null;

// ── THE MEASUREMENT ────────────────────────────────────────────────────────────
console.log(`\n--- mesh:page-recv per follower AFTER the director returned ---`);
if (resume === null) {
  console.log("  cannot segment: no background marker found. Showing the whole window instead.");
}
const from = resume ?? rows[0].t;
const after = rows.filter((r) => r.t >= from);

const followers = jsDevices.filter((d) => d !== director);
let anyReceiving = false;
for (const dev of followers) {
  const recv = after.filter((r) => r.dev === dev && r.event === "mesh:page-recv");
  const total = recv.reduce((a, r) => a + n(r), 0);
  const dups = recv.filter((r) => r.dup === true).reduce((a, r) => a + n(r), 0);
  const pages = [...new Set(recv.map((r) => r.page))].filter((p) => p !== undefined);
  if (total > 0) anyReceiving = true;
  console.log(
    `  ${dev.padEnd(16)} received ${String(total).padStart(5)}  (dup ${String(dups).padStart(5)})  pages seen: ${pages.length ? pages.join(",") : "—"}`,
  );
}
if (!followers.length) console.log("  (no JS-layer followers in window)");
console.log("  (JS layer only — mesh:page-recv does not exist under a Swift peer name)");

// ── Session churn: did anyone reconnect? (the iPhone's suspected recovery route) ──
console.log(`\n--- session churn after return (a reconnect delivers an unconditional snapshot) ---`);
console.log("  (Swift layer only — these are the SAME devices as above, under their peer names)");
for (const dev of swiftDevices) {
  const ev = (name) => after.filter((r) => r.dev === dev && r.event === name).reduce((a, r) => a + n(r), 0);
  const conn = ev("session:connected");
  const notc = ev("session:notConnected");
  const wd = ev("watchdog:half-open-reconnect");
  const forced = ev("resync:force-reconnect");
  const tag = conn + notc + wd + forced > 0 ? "  <- RECONNECTED (would get a snapshot)" : "";
  console.log(`  ${dev.padEnd(16)} connected ${conn}  notConnected ${notc}  watchdog ${wd}  forced ${forced}${tag}`);
}

// ── Verdict ────────────────────────────────────────────────────────────────────
console.log(`\n=== VERDICT ===`);
if (!followers.length) {
  console.log("INCONCLUSIVE — no followers reported. Were they on Wi-Fi? Follower telemetry does not");
  console.log("exist on a network-less device, and silence is not evidence.");
  process.exit(2);
}
if (anyReceiving) {
  console.log("The director's heartbeat IS arriving after foreground.");
  console.log("=> Transport, session and heartbeat are all fine. The bug is DOWNSTREAM of receipt:");
  console.log("   the follower got the packet and did not end up on that page. Look at the dup column");
  console.log("   above, then PdfReaderApp.tsx:1971 (native de-dup) and web renderPage's");
  console.log("   already-on-this-page guard (app.js:1588).");
} else {
  console.log("NO mesh:page-recv reached any follower after the director returned.");
  console.log("=> The director went SILENT. The bug is upstream: either the 1 Hz JS heartbeat did not");
  console.log("   survive backgrounding (PdfReaderApp.tsx:701 setInterval; note the AppState 'active'");
  console.log("   handler at :2074 restarts nothing for a director), or the sessions are half-open.");
  console.log("   Cross-check: did the director log anything at all in this window?");
}
const dirRows = after.filter((r) => r.dev === director).length;
console.log(`\n(director rows in window: ${dirRows}${dirRows === 0 ? " — director telemetry MISSING, treat the above with care" : ""})`);
