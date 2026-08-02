#!/usr/bin/env node
/**
 * verify-smoke-guards.mjs — mutation-tests the smoke-boot WIRING guards.
 *
 * smoke-boot.mjs asserts that certain call sites still exist. Those assertions are
 * uniquely easy to write wrong: a regex that merely quotes the current source passes
 * forever and catches nothing, and nobody notices because a passing test looks identical
 * either way. The only honest proof is to BREAK the thing and watch the guard fail.
 *
 * So: copy the built bundle, apply a mutation that re-introduces a real regression, and
 * assert the named check actually reports FAIL. A mutation that slips through is itself a
 * failure of this script.
 *
 * Both gaps this caught when it was written (2026-08-01) were live bugs in my own
 * assertions, not hypotheticals:
 *   - mutating web/dist directly, while smoke-boot rebuilds dist from src by default —
 *     so the mutation was overwritten and the run re-verified pristine output;
 *   - a guard that accepted EITHER cache read, so reverting only the PAGE_CACHE one (the
 *     husk the SW's activate keep-policy actually has to score) passed clean.
 *
 * Usage:  node scripts/verify-smoke-guards.mjs
 * Requires web/dist to exist (run `node web/build.mjs` first); it never builds, so it is
 * fast and cannot race a concurrent build the way an in-place mutation does.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DIST = path.join(ROOT, "web", "dist");

/**
 * Each case names the smoke-boot check it must trip, and the edit that should trip it.
 * `find` must be present in the built app.js or the case is reported as STALE — that is
 * how this script tells you a mutation has drifted from the source rather than silently
 * testing nothing.
 */
const MUTATIONS = [
  {
    name: "unwire fleetCheckin (webCached back to the raw sticky flag)",
    check: "verifies the offline bundle before claiming webCached",
    // Verbatim the pre-fix expression, so this reproduces the real regression rather than
    // some synthetic edit that happens to trip the regex.
    edits: [[
      "webCached: verifiedReady,",
      'webCached: localStorage.getItem(OFFLINE_READY_KEY) === "ready",',
    ]],
  },
  {
    name: "delete the isOfflineBundleReady call site (the original rot)",
    check: "verifies the offline bundle before claiming webCached",
    edits: [["isOfflineBundleReady(totalPages).catch(() => false)", "Promise.resolve(false)"]],
  },
  {
    name: "revert both cache reads to the CREATING caches.open()",
    check: "inspects caches without creating them",
    edits: [
      ["openExistingCache(STATIC_CACHE)", "caches.open(STATIC_CACHE)"],
      ["openExistingCache(PAGE_CACHE)", "caches.open(PAGE_CACHE)"],
    ],
  },
  {
    name: "revert ONLY the PAGE_CACHE read (the husk the SW keep-policy scores)",
    check: "inspects caches without creating them",
    edits: [["openExistingCache(PAGE_CACHE)", "caches.open(PAGE_CACHE)"]],
  },
  {
    name: "revert ONLY the STATIC_CACHE read",
    check: "inspects caches without creating them",
    edits: [["openExistingCache(STATIC_CACHE)", "caches.open(STATIC_CACHE)"]],
  },
  {
    // The call sites can stay untouched while the helper stops being a helper. Without a
    // body assertion this passes every other guard and silently restores husk-minting.
    name: "gut openExistingCache's body to a bare creating caches.open()",
    check: "openExistingCache is itself non-creating",
    edits: [[
      "if (!(await caches.keys()).includes(name)) return null;",
      "// (existence check removed)",
    ]],
  },
  {
    // The original bug re-introduced as an OR-fallback rather than a replacement: the
    // verifier is still called, so a naive "is it referenced?" guard would pass.
    name: "re-add the sticky flag as an OR-fallback beside the verifier",
    check: "verifies the offline bundle before claiming webCached",
    edits: [[
      "webCached: verifiedReady,",
      'webCached: verifiedReady || localStorage.getItem(OFFLINE_READY_KEY) === "ready",',
    ]],
  },
  {
    // Reproduces the fleet-wide post-deploy false red found by the adversarial re-hunt:
    // a readiness checklist demanding an asset the SW never installs.
    name: "un-exempt /books.json so readiness demands an asset the SW never installs",
    check: "readiness checklist matches the SW's install set",
    edits: [['new Set([resolveAppPath("/books.json")])', "new Set([])"]],
  },
];

/** Run smoke-boot against `dir` without building, and return its combined output. */
const runSmoke = (dir) =>
  spawnSync(process.execPath, [path.join(ROOT, "scripts", "smoke-boot.mjs")], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...process.env, SMOKE_SKIP_BUILD: "1", SMOKE_DIST: dir },
  });

/** True when smoke-boot reported the named check as FAILING. */
const checkFailed = (output, label) =>
  output.split("\n").some((line) => line.includes("FAIL") && line.includes(label));
const checkPassed = (output, label) =>
  output.split("\n").some((line) => line.trim().startsWith("ok") && line.includes(label));

if (!fs.existsSync(path.join(DIST, "app.js"))) {
  console.error("✖ web/dist/app.js not found — run `node web/build.mjs` first.");
  process.exit(1);
}

const work = fs.mkdtempSync(path.join(os.tmpdir(), "sv-smoke-guards-"));
const failures = [];

try {
  // Baseline: every guarded check must PASS on an unmutated copy. Without this, a guard
  // that fails unconditionally would "catch" all five mutations and look perfect.
  const basedir = path.join(work, "baseline");
  fs.cpSync(DIST, basedir, { recursive: true });
  const baseline = runSmoke(basedir);
  for (const label of new Set(MUTATIONS.map((m) => m.check))) {
    if (checkPassed(baseline.stdout || "", label)) {
      console.log(`  ok   baseline passes: ${label}`);
    } else {
      failures.push(`baseline does NOT pass "${label}" — the guard is broken, not the code`);
      console.log(`  FAIL baseline does not pass: ${label}`);
    }
  }

  MUTATIONS.forEach((mutation, index) => {
    const dir = path.join(work, `mut-${index}`);
    fs.cpSync(DIST, dir, { recursive: true });
    const file = path.join(dir, "app.js");
    let source = fs.readFileSync(file, "utf8");

    for (const [find, replace] of mutation.edits) {
      if (!source.includes(find)) {
        failures.push(`STALE mutation "${mutation.name}": ${JSON.stringify(find)} not in the bundle`);
        console.log(`  FAIL stale mutation (source drifted): ${mutation.name}`);
        return;
      }
      source = source.split(find).join(replace);
    }
    fs.writeFileSync(file, source);

    const caught = checkFailed(runSmoke(dir).stdout || "", mutation.check);
    if (caught) {
      console.log(`  ok   caught: ${mutation.name}`);
    } else {
      failures.push(`NOT CAUGHT: ${mutation.name} — "${mutation.check}" still passed`);
      console.log(`  FAIL NOT caught: ${mutation.name}`);
    }
  });
} finally {
  fs.rmSync(work, { recursive: true, force: true });
}

console.log("");
if (failures.length) {
  console.error(`✖ ${failures.length} guard(s) do not catch their regression:`);
  for (const f of failures) console.error(`   • ${f}`);
  console.error("\nA wiring guard that cannot fail is decoration. Fix the assertion.");
  process.exit(1);
}
console.log(`✓ all ${MUTATIONS.length} mutations caught — the smoke-boot wiring guards have teeth.`);
