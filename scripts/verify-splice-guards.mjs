#!/usr/bin/env node
/**
 * verify-splice-guards.mjs — mutation-tests e2e/spliceSongPages.test.mjs.
 *
 * splice-song-pages.py rewrites assets/songbook.pdf, so its failure modes are the expensive
 * kind: a page silently re-rendered, a blank page a singer can land on, a sheet left at the
 * wrong geometry. The tests claim to catch those. This script proves it, by re-introducing
 * each regression and requiring the suite to go RED.
 *
 * Same discipline as verify-render-cache-guards.mjs, including its hard-won lesson: a mutation
 * that fails to APPLY is indistinguishable from a test that fails to CATCH, so every mutation
 * asserts its target text exists first and a miss is counted as a failure, not a pass.
 *
 * It also refuses to report anything if the UNMUTATED suite is already red — with a broken
 * baseline every mutation "fails" and the harness would print a clean sweep it did not earn.
 *
 * Usage:  node scripts/verify-splice-guards.mjs
 * Mutates scripts/splice-song-pages.py in place (restored via finally).
 */
import fs from "node:fs";
import { spawnSync } from "node:child_process";

const FILE = "scripts/splice-song-pages.py";
const TESTS = "e2e/spliceSongPages.test.mjs";

// The mutations below are applied to the file ON DISK and restored in a `finally` — which never
// runs if this process is killed mid-mutation. That happened on the harness's very first outing
// (2026-08-15): an interrupted run left "if False:" in place of the duplicate-page guard, the
// suite went red, and the next run reported a red baseline with no hint why. So: refuse to start
// from a dirty copy of the tool, name the fix, and take `original` from the committed version —
// then a crash cannot make the working tree the thing this harness restores TO.
const dirty = spawnSync("git", ["diff", "--quiet", "--", FILE]).status !== 0;
if (dirty) {
  console.error(`✖ ${FILE} differs from HEAD. A previous mutation run was probably interrupted`);
  console.error("  before its finally-restore ran. Look at the diff, then either commit it or:");
  console.error(`    git checkout -- ${FILE}`);
  process.exit(2);
}
const original = fs.readFileSync(FILE, "utf8");

// Restore on ANY exit — Ctrl-C included — not only on the normal path through `finally`.
const restore = () => { try { fs.writeFileSync(FILE, original); } catch { /* best effort */ } };
for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(sig, () => { restore(); process.exit(130); });

const run = () => spawnSync("node", ["--test", TESTS], { stdio: "pipe", encoding: "utf8" });

// A skipped suite exits 0 and would make every mutation look "survived". Establish the baseline
// first and say plainly which case we are in.
const baseline = run();
if (/# skipped [1-9]/.test(baseline.stdout || "")) {
  console.error("✖ the splice tests SKIPPED (pikepdf/Pillow or poppler missing).");
  console.error("  Install them first — this harness cannot prove anything against a skip:");
  console.error("    pip3 install pikepdf pillow");
  process.exit(2);
}
if (baseline.status !== 0) {
  console.error("✖ the splice tests are RED before any mutation. Fix that first — with a broken");
  console.error("  baseline every mutation 'fails' and this harness reports a sweep it did not earn.");
  process.exit(2);
}

// A mutation is one or more {old,new} edits applied together. Most are single. The multi-edit
// ones exist because the tool has two defences against the same failure — a structural fix and a
// verify-before-write — and a single mutation can only ever disable one of them, so the other
// keeps the tests green and the harness would report the disabled one as "not caught".
const COALESCE = {
  old: "    page.contents_coalesce()",
  new: "    pass  # coalesce disabled",
};
const VERIFY = {
  old: "            verify_rendered(pending, prepared, tmp)",
  new: "            pass  # verify disabled",
};

const MUTATIONS = [
  {
    // v425 shipped page 371 at 540x720 in a 768x1024 book. web/build.mjs renders at a fixed DPI,
    // so that page came out 863px wide beside its neighbours' 1227px.
    name: "skip the geometry normalisation (splice the sheet at its native size)",
    old: "    page.mediabox = [0, 0, target_w, target_h]",
    new: "    pass",
  },
  {
    // The empty-stream trap: without coalescing, the fixture's blank content member can be
    // written as a zero-byte /FlateDecode stream and poppler renders the page blank. With the
    // verify step still armed the tool must REFUSE to write — either way the tests go red.
    name: "splice the sheet's content streams as-is (no coalesce)",
    edits: [COALESCE],
  },
  {
    // The verify step alone: disable BOTH defences and the blank page must be caught by the
    // tests' own poppler render — proving that assertion has teeth. (Disabling verify alone
    // proves nothing, because coalesce keeps the page renderable.)
    name: "no coalesce AND no verify-before-write (the blank page reaches the book)",
    edits: [COALESCE, VERIFY],
  },
  {
    name: "stretch to fill instead of preserving aspect ratio",
    old: "    scale = min(target_w / w, target_h / h)",
    new: "    scale = max(target_w / w, target_h / h)",
  },
  {
    // Appending 374 to a 371-page book would create 372 and 373 as blanks — the exact pages a
    // singer types a number to reach.
    name: "allow a gap when appending",
    old: "    if appends != expected:",
    new: "    if False:",
  },
  {
    name: "ignore --expect-pages",
    old: "    if args.expect_pages is not None and final_pages != args.expect_pages:",
    new: "    if False:",
  },
  {
    name: "let --dry-run write the file anyway",
    old: "    if args.dry_run:",
    new: "    if False:",
  },
  {
    name: "accept the same page number twice",
    old: "    if len({n for n, _ in targets}) != len(targets):",
    new: "    if False:",
  },
  {
    name: "accept a --src-page past the end of the sheet",
    old: "    if src_page_no < 1 or src_page_no > len(pdf.pages):",
    new: "    if False:",
  },
  {
    name: "accept a source path that does not exist",
    old: "        if not os.path.exists(src):",
    new: "        if False:",
  },
  {
    // The replace/append split is index arithmetic, and off-by-one here silently overwrites the
    // wrong song — the one class of bug that looks completely healthy in every page count.
    name: "off-by-one: replace the page after the one named",
    old: "                book.pages[n - 1] = src.pages[0]",
    new: "                book.pages[n] = src.pages[0]",
  },
];

let bad = 0;
try {
  for (const m of MUTATIONS) {
    const edits = m.edits ?? [{ old: m.old, new: m.new }];
    const missing = edits.filter((e) => !original.includes(e.old));
    if (missing.length) {
      console.log(`  ⚠️  NO-OP: mutation target not found — "${m.name}"`);
      console.log("      Either the script changed shape (update this harness) or the guard is");
      console.log("      gone. A no-op proves NOTHING, so it counts as a failure.");
      for (const e of missing) console.log(`      missing: ${JSON.stringify(e.old)}`);
      bad += 1;
      continue;
    }
    let mutated = original;
    for (const e of edits) mutated = mutated.replace(e.old, e.new);
    fs.writeFileSync(FILE, mutated);
    if (run().status === 0) {
      console.log(`  ❌ SURVIVED: ${m.name} — the tests do not catch this regression`);
      bad += 1;
    } else {
      console.log(`  ✅ caught:   ${m.name}`);
    }
  }
} finally {
  restore();
}

if (bad) {
  console.error(`\n${bad} mutation(s) not caught — the splice tests are decorative.`);
  process.exit(1);
}
console.log("\nAll mutations caught: the splice tests execute, they don't decorate.");
