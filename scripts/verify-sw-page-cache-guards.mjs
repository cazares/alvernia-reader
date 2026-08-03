#!/usr/bin/env node
/**
 * verify-sw-page-cache-guards.mjs — mutation-tests e2e/sw-page-cache.test.mjs.
 *
 * Sibling of verify-smoke-guards.mjs, same reasoning: a green test proves nothing until you
 * have watched it go red. These particular tests guard the page-slot poisoning fix — the one
 * where Cloudflare Pages' SPA fallback (200 text/html for any unmatched path) gets written
 * into PAGE_CACHE under page-NNN.webp and, because that branch is cache-first with no
 * revalidation, breaks that page on that device online AND offline until BOOK_VERSION changes.
 * There is no remedy for that inside the church, so the guards had better actually hold.
 *
 * Method: copy web/src to a temp dir, revert ONE guard to its pre-fix form, point the test
 * suite at the copy via SW_TEST_SRC, and assert the named test FAILS. A mutation that slips
 * through is a failure of this script — it means the test is decoration.
 *
 * Usage:  node scripts/verify-sw-page-cache-guards.mjs
 * Reads only web/src; never builds, never mutates the working tree.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SRC = path.join(ROOT, "web", "src");
const SUITE = path.join(ROOT, "e2e", "sw-page-cache.test.mjs");

/**
 * Each case reverts one guard to the code that shipped BEFORE the fix, and names a test that
 * must therefore fail. `find` must exist in the source or the case is reported STALE — that is
 * how this script announces it has drifted rather than silently testing nothing.
 */
const MUTATIONS = [
  {
    name: "revert the cache WRITE guard (the headline bug: HTML stored as a page)",
    // Verbatim the pre-fix line, so this reproduces the real regression.
    edits: [["          if (!isCacheablePageImage(response)) return;\n", ""]],
    expect: "a 200 text/html SPA fallback is NEVER written into the page cache",
  },
  {
    name: "revert the SERVE guard (HTML wins over a real previous edition)",
    edits: [[
      "if (winner !== timedOut && winner && winner.ok && !isNotPageImage(winner)) return winner;",
      "if (winner !== timedOut && winner && winner.ok) return winner;",
    ]],
    expect: "the SPA fallback does not win over a previous edition of the page",
  },
  {
    name: "revert the self-heal (an already-poisoned slot is served forever)",
    edits: [[
      "        if (cached && isNotPageImage(cached)) {\n"
        + "          await Promise.all([\n"
        + "            cache.delete(event.request, { ignoreSearch: true }),\n"
        + "            cache.delete(cacheKey),\n"
        + "          ]).catch(() => {});\n"
        + "        } else if (cached) {\n"
        + "          return cached;\n"
        + "        }",
      "        if (cached) return cached;",
    ]],
    expect: "an already-poisoned slot is evicted and refilled from the network",
  },
  {
    // The belt-and-braces half: ignoreSearch is the least-exercised corner of the Cache API on
    // the old iPads this fleet runs. Dropping the explicit bare-key delete leaves the heal
    // dependent on it, and a browser that no-ops the option re-downloads the page on every view.
    name: "drop the explicit bare-key delete, leaving the heal dependent on ignoreSearch",
    edits: [["            cache.delete(cacheKey),\n", ""]],
    expect: "a poisoned slot still heals when the browser ignores ignoreSearch on delete",
  },
  {
    name: "revert the previous-edition scan guard (poison found in an older cache is served)",
    edits: [["    if (hit && isNotPageImage(hit)) continue;\n", ""]],
    expect: "a poisoned PREVIOUS edition is skipped so a real older one can still be found",
  },
  {
    // The asymmetry matters: caching demands a POSITIVE image/*, so a response we cannot
    // classify is never persisted. Loosening it to "reject only what we know is bad" would
    // still pass the headline test (HTML is positively bad) — this pins the strict side.
    name: "loosen the WRITE guard to the permissive SERVE rule",
    edits: [["if (!isCacheablePageImage(response)) return;", "if (isNotPageImage(response)) return;"]],
    expect: "refuses to cache a page response with no content-type at all",
  },
  {
    name: "revert app.js's precache guard (the OTHER writer poisons, and certifies it ready)",
    file: "app.js",
    edits: [[
      "  if (!isPageImageResponse(response)) {\n"
        + "    throw new Error(`Página ${pageNumber} no devolvió una imagen — reintentando en línea`);\n"
        + "  }\n",
      "",
    ]],
    expect: "app.js's precache refuses to persist a page response that is not an image",
  },
  {
    name: "revert app.js's poisoned-slot repair back to a bare presence check",
    file: "app.js",
    edits: [[
      "  const existing = await cache.match(url);",
      "  if (await cache.match(url)) return false;\n  const existing = null;",
    ]],
    expect: "app.js's precache refuses to persist a page response that is not an image",
  },
  {
    // Without this, the repair above is unreachable: ensureOfflineBundle only calls
    // cacheSinglePage for pages getCachedPageSet reports missing, and a poisoned slot has a
    // perfectly good key. Reverting to key-only counting makes the poison permanent again AND
    // restores the false-green readiness claim.
    name: "revert getCachedPageSet to key-only counting (repair becomes unreachable)",
    file: "app.js",
    edits: [["      return canInspect ? !isPoisonedPageEntry(responses[index]) : true;", "      return true;"]],
    expect: "app.js counts cached pages by CONTENT, so a poisoned slot reads as missing",
  },
  {
    // The asymmetry on the app side: judging cached entries with the STRICT write-rule would
    // condemn any entry we cannot classify and re-download the whole book fleet-wide.
    name: "judge cached entries with the strict write-rule instead of the permissive one",
    file: "app.js",
    edits: [[
      "      return canInspect ? !isPoisonedPageEntry(responses[index]) : true;",
      "      return canInspect ? isPageImageResponse(responses[index]) : true;",
    ]],
    expect: "app.js counts cached pages by CONTENT, so a poisoned slot reads as missing",
  },
  {
    name: "drop the matchAll/keys length correlation check",
    file: "app.js",
    edits: [[
      "  const canInspect = Array.isArray(responses) && responses.length === keys.length;",
      "  const canInspect = Array.isArray(responses);",
    ]],
    expect: "app.js counts cached pages by CONTENT, so a poisoned slot reads as missing",
  },
];

/** Run the suite against `dir` and return its combined output. */
const runSuite = (dir) => {
  const result = spawnSync(process.execPath, ["--test", SUITE], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...process.env, SW_TEST_SRC: dir },
  });
  return `${result.stdout || ""}\n${result.stderr || ""}`;
};

/** node:test TAP: "not ok N - <name>" is a failure, "ok N - <name>" a pass. */
const testFailed = (output, name) =>
  output.split("\n").some((line) => line.trim().startsWith("not ok") && line.includes(name));
const testPassed = (output, name) =>
  output.split("\n").some((line) => line.trim().startsWith("ok") && line.includes(name));

const work = fs.mkdtempSync(path.join(os.tmpdir(), "sv-sw-guards-"));
const failures = [];

try {
  // Baseline: every named test must PASS unmutated. Without this, a test that fails
  // unconditionally would "catch" every mutation and look perfect.
  const basedir = path.join(work, "baseline");
  fs.cpSync(SRC, basedir, { recursive: true });
  const baseline = runSuite(basedir);
  for (const name of new Set(MUTATIONS.map((m) => m.expect))) {
    if (testPassed(baseline, name)) {
      console.log(`  ok   baseline passes: ${name}`);
    } else {
      failures.push(`baseline does NOT pass "${name}" — the test is broken, not the code`);
      console.log(`  FAIL baseline does not pass: ${name}`);
    }
  }

  MUTATIONS.forEach((mutation, index) => {
    const dir = path.join(work, `mut-${index}`);
    fs.cpSync(SRC, dir, { recursive: true });
    const target = path.join(dir, mutation.file || "sw.js");
    let source = fs.readFileSync(target, "utf8");

    let stale = false;
    for (const [find, replace] of mutation.edits) {
      if (!source.includes(find)) {
        failures.push(
          `STALE mutation "${mutation.name}": ${JSON.stringify(find.slice(0, 60))} not in ${mutation.file || "sw.js"}`,
        );
        console.log(`  FAIL stale mutation (source drifted): ${mutation.name}`);
        stale = true;
        break;
      }
      source = source.split(find).join(replace);
    }
    if (stale) return;
    fs.writeFileSync(target, source);

    if (testFailed(runSuite(dir), mutation.expect)) {
      console.log(`  ok   caught: ${mutation.name}`);
    } else {
      failures.push(`NOT CAUGHT: ${mutation.name} — "${mutation.expect}" still passed`);
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
  console.error("\nA guard that cannot fail is decoration. Fix the assertion.");
  process.exit(1);
}
console.log(`✓ all ${MUTATIONS.length} mutations caught — the page-cache guards have teeth.`);
