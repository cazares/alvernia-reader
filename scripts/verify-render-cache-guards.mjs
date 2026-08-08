#!/usr/bin/env node
/**
 * verify-render-cache-guards.mjs — mutation-tests e2e/renderCache.test.mjs.
 *
 * The render cache's only real risk is serving pixels that are not the pixels this build
 * would have produced — wrong music on the iPads, looking perfectly healthy. The tests
 * guard the cache KEY and the completeness checks; this script proves those tests bite by
 * re-introducing each regression and watching them fail.
 *
 * Lesson baked in from the first run of this harness (2026-08-08): a mutation that fails
 * to APPLY looks exactly like a test that fails to CATCH. Two "survivors" in round one
 * were shell-escaping no-ops — the mutation text never matched, nothing was mutated, and
 * the verdict was garbage. So every mutation here asserts its target text exists before
 * the run counts for anything.
 *
 * Usage:  node scripts/verify-render-cache-guards.mjs
 * Mutates web/build.mjs in place (restored via finally) — do not run concurrently with a build.
 */
import fs from "node:fs";
import { spawnSync } from "node:child_process";

const FILE = "web/build.mjs";
const TESTS = "e2e/renderCache.test.mjs";
const original = fs.readFileSync(FILE, "utf8");

// Each entry re-introduces a regression the tests exist to prevent.
const MUTATIONS = [
  {
    name: "drop the PDF bytes from the cache key",
    old: "h.update(fs.readFileSync(pdfPath));",
    new: "",
  },
  {
    name: "drop quality + pad width from the cache key",
    old: "dpi=${PDF_RENDER_DPI};q=${WEBP_QUALITY};pad=${PAGE_PAD_WIDTH}",
    new: "dpi=${PDF_RENDER_DPI}",
  },
  {
    name: "drop BOTH renderer versions from the key",
    old: `  h.update(\`pdftoppm=\${toolVersion("pdftoppm", ["-v"])};cwebp=\${toolVersion("cwebp", ["-version"])}\`);`,
    new: "",
  },
  {
    // The TDZ bug hit live on the very first cold build: buildBook() runs at module top
    // level, so a helper declared below it is still uninitialized. `node --check` passes.
    name: "declare toolVersion after the cache key uses it",
    old: "const toolVersion = (bin, args) => {",
    new: "const toolVersionMOVED = (bin, args) => {",
  },
  {
    name: "trust an incomplete cache directory",
    old: "cached.length === expected",
    new: "cached.length > 0",
  },
  {
    name: "populate the cache from a partial render",
    old: "out.length === expected",
    new: "out.length >= 0",
  },
  {
    name: "skip the per-page encode verification",
    old: "!fs.existsSync(pairs[i])",
    new: "false",
  },
];

let bad = 0;
try {
  for (const m of MUTATIONS) {
    if (!original.includes(m.old)) {
      console.log(`  ⚠️  NO-OP: mutation target not found — "${m.name}"`);
      console.log(`      Either build.mjs changed shape (update this harness) or the`);
      console.log(`      guard it exercised is gone. A no-op proves NOTHING.`);
      bad += 1;
      continue;
    }
    fs.writeFileSync(FILE, original.replace(m.old, m.new));
    const r = spawnSync("node", ["--test", TESTS], { stdio: "pipe" });
    if (r.status === 0) {
      console.log(`  ❌ SURVIVED: ${m.name} — the tests do not catch this regression`);
      bad += 1;
    } else {
      console.log(`  ✅ caught:   ${m.name}`);
    }
  }
} finally {
  fs.writeFileSync(FILE, original);
}

if (bad) {
  console.error(`\n${bad} mutation(s) not caught — the render-cache tests are decorative.`);
  process.exit(1);
}
console.log("\nAll mutations caught: the render-cache tests execute, they don't decorate.");
