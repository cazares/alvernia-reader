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
 *   1. every follower<->follower cross-connect is MATCHED by a `session:peer-not-director` refusal
 *   2. `watchdog:half-open-reconnect` ... must be ~0 (each one is a torn-down session)
 *   3. the run actually exercised the bug (at least one cross-connect happened)
 *
 * (1) IS NOT "ZERO CROSS-CONNECTS". That was this tool's first criterion and it was WRONG: it
 * printed FAIL on the 2026-08-17 hardware run that actually PROVED the fix. An MCSession is a
 * GROUP — Multipeer always connects every member to every other member, and
 * `dbgLog("session:\(stateName)")` fires BEFORE the switch, unconditionally. Follower<->follower
 * `session:connected` rows therefore exist on a perfectly healthy mesh and can never be zero.
 * The fix prevents the MISATTRIBUTION, not the connection. What must be zero is a cross-connect
 * the guard did NOT refuse — that is the one that becomes a false director.
 *
 * (3) is what a quiet room cannot fake: zero cross-connects means the bug was never exercised,
 * which is indistinguishable from a working fix. It needs THREE+ devices to reproduce at all,
 * because with a single follower there is no second follower to cross-connect with.
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
// Score only rows from this build. A device WITHOUT the fix in the mesh reports
// `session:connected` for its fellow followers exactly like a broken build does, so one stray
// old device fails an otherwise-perfect run. Same false-signal class as leaving a simulator on.
const buildIdx = args.indexOf("--build");
const onlyBuild = buildIdx >= 0 ? String(Number(args[buildIdx + 1])) : null;
// Phase segmentation. A multi-phase stress run restarts the mesh several times, and each restart
// mints NEW MCPeerID display names -- so a whole-file analysis silently mixes generations and
// scores a page sent in generation 1 against a follower that only joined in generation 2.
// Accepts epoch ms, or "HH:MM:SS" / "HH:MM" matched against the UTC clock of the capture.
const timeArg = (flag) => {
  const i = args.indexOf(flag);
  if (i < 0) return null;
  const v = args[i + 1];
  if (/^\d{10,}$/.test(v)) return Number(v);
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(v);
  if (!m) { console.error(`bad time for ${flag}: ${v}`); process.exit(2); }
  return { h: +m[1], m: +m[2], s: +(m[3] || 0) };
};
const sinceArg = timeArg("--since");
const untilArg = timeArg("--until");
const toMs = (spec, sample) => {
  if (spec == null) return null;
  if (typeof spec === "number") return spec;
  const d = new Date(sample);
  d.setUTCHours(spec.h, spec.m, spec.s, 0);
  return d.getTime();
};

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

  // A cross-connect is only a BUG if the guard failed to refuse it. Match on (reporter, peer):
  // the guard logs `session:peer-not-director` with the same two names microseconds later.
  const rejected = new Set(
    rows.filter((r) => r.event === "session:peer-not-director").map((r) => `${r.dev}|${r.peer}`)
  );
  const unrefused = f2f.filter((r) => !rejected.has(`${r.dev}|${r.peer}`));

  const builds = [...new Set(rows.map((r) => r.build).filter(Boolean))].sort();
  const halfOpen = countEvent(rows, "watchdog:half-open-reconnect");
  const notDirector = countEvent(rows, "session:peer-not-director");

  // Heartbeat / page delivery per follower -- the "519 / 29 / 4" signature of the bug.
  const recv = new Map();
  for (const r of rows) {
    if (r.event !== "mesh:page-recv" || !r.dev) continue;
    recv.set(r.dev, (recv.get(r.dev) || 0) + (r.n || 1));
  }

  // PAGE-SYNC LATENCY — what the congregation actually experiences. The director stamps
  // `page:send` with the page number; each follower stamps `mesh:page-recv` with the same one.
  // Use `t0` on folded rows (the FIRST arrival, not the last of a collapsed run) and ignore
  // arrivals from before the send, which are the follower already sitting on that page.
  const sends = rows.filter((r) => r.event === "page:send" && r.page != null).sort((a, b) => a.t - b.t);
  const pageRecvs = rows.filter((r) => r.event === "mesh:page-recv" && r.page != null);
  // A PAGE SENT BEFORE A FOLLOWER JOINED IS NOT A MISSED PAGE. During a cold start the director
  // can broadcast while devices are still booting -- measured 2026-08-17: `page:send` fired 0.1 s
  // after become:director and the first follower connected 1.8 s LATER. Scoring those as failures
  // made a healthy 4-device cold start report "2 pages NEVER reached a follower" plus a 10.4 s
  // turn. You cannot deliver a page to a device that is not in the session yet.
  //
  // Join time is taken from the follower's own first `mesh:page-recv`, deliberately the SAME
  // telemetry layer as the latency measurement: session events are Swift peer names
  // (`iPad-4A1A69`) while page events are JS device ids (`5eut7o`), and the two cannot be joined
  // without a mapping the relay does not carry. A follower that never receives anything at all is
  // still caught -- it shows up with zero delivery in `recv`.
  const devs = [...new Set(pageRecvs.map((r) => r.dev))];
  const joinedAt = new Map(
    devs.map((d) => [
      d,
      Math.min(...pageRecvs.filter((r) => r.dev === d).map((r) => r.t0 || r.t)),
    ])
  );
  const latency = [];
  for (const s of sends) {
    for (const dev of devs) {
      if (s.t < joinedAt.get(dev)) {
        latency.push({ page: s.page, dev, ms: undefined, notJoined: true });
        continue;
      }
      const arrivals = pageRecvs
        .filter((r) => r.dev === dev && r.page === s.page)
        .map((r) => r.t0 || r.t)
        .filter((t) => t >= s.t - 500);
      if (!arrivals.length) {
        latency.push({ page: s.page, dev, ms: null }); // joined, but never got it -- REAL miss
        continue;
      }
      latency.push({ page: s.page, dev, ms: Math.min(...arrivals) - s.t });
    }
  }
  // The FIRST page a follower sees includes join/discovery/handshake, so it is not a page-turn
  // measurement. Judge steady-state turns only; the join cost is reported separately.
  const firstPage = sends.length ? sends[0].page : null;
  const turns = latency.filter((l) => l.page !== firstPage && !l.notJoined);
  const missed = turns.filter((l) => l.ms === null);
  const worst = turns.filter((l) => l.ms !== null).sort((a, b) => b.ms - a.ms)[0] || null;

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
    unrefusedCount: unrefused.length,
    unrefusedPairs: [...new Set(unrefused.map((r) => `${r.dev} -> ${r.peer}`))],
    f2dCount: f2d.reduce((a, r) => a + (r.n || 1), 0),
    halfOpen,
    notDirector,
    recv: [...recv.entries()].sort((a, b) => b[1] - a[1]),
    latency,
    turns,
    missed,
    worst,
    firstPage,
  };
}

function report(a) {
  console.log(`\n=== ${a.label} ===`);
  console.log(`rows ${a.rows} · builds ${a.builds.join(",") || "?"} · span ${a.span}`);
  console.log(`director(s): ${a.directors.join(", ") || "(none seen)"}`);
  console.log(`followers  : ${a.followers.join(", ") || "(none seen)"}`);
  console.log("");
  console.log(`  follower->follower cross-connects    : ${a.f2fCount}   (Multipeer always does this — not a defect)`);
  for (const p of a.f2fPairs) console.log(`      ${p}`);
  console.log(`  ...of those, UNREFUSED by the guard  : ${a.unrefusedCount}   ${a.unrefusedCount === 0 ? "✅ none became a false director" : "❌ THESE BECAME FALSE DIRECTORS"}`);
  for (const p of a.unrefusedPairs) console.log(`      ${p}`);
  console.log(`  follower->director session:connected : ${a.f2dCount}   (healthy, expected > 0)`);
  console.log(`  watchdog:half-open-reconnect         : ${a.halfOpen}   ${a.halfOpen === 0 ? "✅" : a.halfOpen <= 2 ? "⚠️" : "❌"}`);
  console.log(`  session:peer-not-director (guard)    : ${a.notDirector}   ${a.notDirector > 0 ? "✅ guard fired" : "— not seen"}`);
  console.log("\n  page/heartbeat delivery per device:");
  for (const [d, n] of a.recv) console.log(`      ${String(n).padStart(5)}  ${d}`);

  if (a.latency.length) {
    const devs = [...new Set(a.latency.map((l) => l.dev))].sort();
    const pages = [...new Set(a.latency.map((l) => l.page))];
    console.log("\n  page-sync latency (director -> follower):");
    console.log("      page   " + devs.map((d) => d.padEnd(11)).join(""));
    for (const p of pages) {
      const cells = devs.map((d) => {
        const l = a.latency.find((x) => x.page === p && x.dev === d);
        if (!l) return "".padEnd(11);
        if (l.notJoined) return "(not yet)".padEnd(11);
        if (l.ms === null) return "NEVER ❌".padEnd(11);
        return ((l.ms / 1000).toFixed(2) + "s" + (l.ms > 1000 ? " ⚠️" : "")).padEnd(11);
      });
      const tag = p === a.firstPage ? "  (join)" : "";
      console.log("      " + String(p).padEnd(7) + cells.join("") + tag);
    }
    if (a.worst)
      console.log(
        `      worst page-turn: ${(a.worst.ms / 1000).toFixed(2)}s (page ${a.worst.page} -> ${a.worst.dev})`
      );
  }
}

const allRows = read(file);

// CONTAMINATION CHECK. Report which build each device is on BEFORE scoring anything. A device
// running a pre-430 build has no `session:peer-not-director` guard, so it reports
// `session:connected` for its fellow followers exactly like a broken build would -- one stray old
// device fails an otherwise-perfect run, and the failure looks like the fix not working.
const buildOf = new Map();
for (const r of allRows) if (r.dev && r.build) buildOf.set(r.dev, String(r.build));
const buildsSeen = [...new Set(buildOf.values())].sort();
if (buildsSeen.length > 1) {
  console.log("\n⚠️  MIXED BUILDS IN THIS CAPTURE — the mesh had devices on different builds:");
  for (const b of buildsSeen) {
    const devs = [...buildOf.entries()].filter(([, v]) => v === b).map(([k]) => k);
    console.log(`     build ${b}: ${devs.join(", ")}`);
  }
  if (!onlyBuild)
    console.log("     Re-run with --build 430 to score only the fixed devices, or close the app on the others.");
}

let rows = onlyBuild ? allRows.filter((r) => String(r.build) === onlyBuild) : allRows;
if (onlyBuild) console.log(`\n(scoring only build ${onlyBuild}: ${rows.length} of ${allRows.length} rows)`);
const sampleT = allRows.length ? allRows[0].t : Date.now();
const sinceMs = toMs(sinceArg, sampleT);
const untilMs = toMs(untilArg, sampleT);
if (sinceMs != null) rows = rows.filter((r) => (r.t0 || r.t) >= sinceMs);
if (untilMs != null) rows = rows.filter((r) => (r.t0 || r.t) <= untilMs);
if (sinceMs != null || untilMs != null)
  console.log(
    `(phase window ${sinceMs ? new Date(sinceMs).toISOString().slice(11, 19) : "start"} -> ` +
      `${untilMs ? new Date(untilMs).toISOString().slice(11, 19) : "end"}: ${rows.length} rows)`
  );

const cur = analyze(rows, `CAPTURE ${file}`);
if (baselineFile) report(analyze(read(baselineFile), `BASELINE ${baselineFile}`));
report(cur);

// ---- verdict -------------------------------------------------------------
const guardPresent = cur.builds.some((b) => Number(b) >= 430);
console.log("\n=== VERDICT ===");

// NO DATA IS NOT A PASS. An empty window printed "✅ PASS" on 2026-08-17 while every device in the
// fleet had in fact been silent for ten minutes -- the single most dangerous output this tool can
// produce, because silence is exactly what a dead mesh looks like. Exit 2 (not 0, not 1): the run
// is INCONCLUSIVE, and the caller must go look at the devices.
if (!cur.rows) {
  console.log("⚠️  INCONCLUSIVE — zero rows in this window.");
  console.log("   Nothing reported. That is NOT a pass: a healthy idle mesh still emits heartbeats,");
  console.log("   so silence means the devices slept, the telemetry path died, or the mesh collapsed.");
  console.log("   Check the devices directly — the page number, not the pill.");
  process.exit(2);
}
const fail = [];
if (cur.unrefusedCount > 0)
  fail.push(`${cur.unrefusedCount} cross-connect(s) the guard did NOT refuse — each became a false director`);
if (cur.halfOpen > 2) fail.push(`${cur.halfOpen} half-open reconnects (must be ~0)`);
if (cur.missed.length)
  fail.push(
    `${cur.missed.length} page turn(s) NEVER reached a follower: ` +
      cur.missed.map((m) => `page ${m.page} -> ${m.dev}`).join(", ")
  );
if (cur.worst && cur.worst.ms > 3000)
  fail.push(
    `slowest page turn ${(cur.worst.ms / 1000).toFixed(2)}s (page ${cur.worst.page} -> ${cur.worst.dev}) — target is ~1s`
  );
// "Did this run exercise the bug?" only makes sense where sessions were ESTABLISHED. Cross-connects
// happen at JOIN time; a steady-state window (pure page-turning on an already-formed mesh) contains
// none by construction, and failing it there is meaningless -- it made a flawless 25-turn phase
// report FAIL. So: no session activity at all => steady-state, note it and judge on latency alone.
// Sessions formed but ZERO cross-connects with 2+ followers => genuinely suspicious, keep failing.
const sessionActivity = cur.f2dCount > 0 || cur.f2fCount > 0;
if (guardPresent && !sessionActivity) {
  console.log(
    "ℹ️  steady-state window — no sessions were established here, so the guard could not be " +
      "exercised. Judged on latency and reconnect churn only."
  );
} else if (guardPresent && cur.f2fCount === 0 && cur.followers.length >= 2) {
  fail.push(
    "sessions formed with 2+ followers but NO follower->follower cross-connect — the bug was " +
      "not exercised, which is indistinguishable from a working fix. Needs 3+ devices."
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
console.log(`✅ PASS — ${cur.f2fCount} cross-connect(s) occurred and the guard refused every one.`);
console.log("   No follower mistook a fellow follower for the director.");
