import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { spawnSync } from "node:child_process";

// THE DIRECTOR SEARCH IS THE BUILD-381 GEOMETRY. Owner's call, twice: 2026-08-06 ("restored
// verbatim from 381"), and again 2026-08-15 after seeing a v425 bundle on an iPhone — where the
// interim compact 380px box anchored top-right reads as a side drawer — "must have same exact
// UI/UX on director search bar as build 381."
//
// It had already drifted once WITH a "verbatim" comment sitting on top of it: the panel block was
// restored, but `.drawer-pane-content` kept the compact era's `max-height: min(60vh, 540px)`, so the
// panel was 381's size while the results list inside stopped at ~540px and the bottom half of the
// panel sat empty on an iPad. A comment is not a pin. This test is.
//
// It compares EVERY search/drawer/dropdown rule in web/src/styles.css against the same rules in
// build 381 (d507509), by selector, at rule granularity — not a substring of one property, which
// is exactly the kind of check that passes while the geometry it guards has been swapped out
// around it. Comments are stripped first, so a note can be added without tripping it.
//
// THE REFERENCE IS A COMMITTED FIXTURE, e2e/fixtures/search-css-381.json, extracted from
// d507509 by this file's own rulesOf(). CI checks out a SHALLOW clone, so `git show d507509:` is
// not available there (the first CI run of this test failed on exactly that). Whenever history IS
// available, the fixture is asserted equal to the live extraction — so it cannot be hand-edited
// into agreeing with a drift without that assertion going red on every developer machine.
//
// If a search rule must change for a REAL reason, change it and update ALLOWED_DRIFT below with
// the reason — a reviewer then sees the exception, instead of a comment claiming verbatim.

const REF_381 = "d507509";
const FILE = "web/src/styles.css";
const FIXTURE = "e2e/fixtures/search-css-381.json";
const SEARCH_RULE = /search|drawer|as-dropdown|sort/i;

// Rules that legitimately differ from 381, by selector. Empty on purpose today.
const ALLOWED_DRIFT = new Map([
  // ["selector", "why it differs from 381"],
]);

const stripComments = (css) => css.replace(/\/\*[\s\S]*?\*\//g, "");
const rulesOf = (css) => {
  const out = new Map();
  const re = /([^{}]+?)\s*\{([^{}]*)\}/g;
  let m;
  while ((m = re.exec(stripComments(css)))) {
    const sel = m[1].split(/\s+/).join(" ").trim();
    if (!SEARCH_RULE.test(sel)) continue;
    // Normalise whitespace and property order so a re-flow is not a diff.
    const props = m[2].split(";").map((s) => s.trim()).filter(Boolean).sort();
    out.set(sel, props.join("; "));
  }
  return out;
};

const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const REF = new Map(Object.entries(fixture.rules));

const git = spawnSync("git", ["show", `${REF_381}:${FILE}`], { encoding: "utf8" });
const HAVE_381 = git.status === 0 && git.stdout.length > 1000;

test("the committed 381 fixture matches the real d507509 (when history is available)", { skip: !HAVE_381 && "shallow clone — fixture is the reference here" }, () => {
  // Proves the fixture is what it claims to be. Skips (loudly) in a shallow CI clone; runs on
  // every developer machine, so a hand-edited fixture cannot survive a local test run.
  const live = rulesOf(git.stdout);
  assert.equal(fixture.ref, REF_381);
  assert.deepEqual([...REF.entries()].sort(), [...live.entries()].sort(),
    `${FIXTURE} does not match git show ${REF_381}:${FILE} — regenerate it, do not hand-edit it`);
});

test("every search/drawer CSS rule is IDENTICAL to build 381, at rule granularity", () => {
  const ref = REF;
  const now = rulesOf(fs.readFileSync(FILE, "utf8"));
  assert.ok(ref.size > 50, `only ${ref.size} search rules in the fixture — it is truncated or the filter is wrong`);

  const problems = [];
  for (const [sel, body] of ref) {
    if (!now.has(sel)) problems.push(`MISSING vs 381: ${sel}`);
    else if (now.get(sel) !== body && !ALLOWED_DRIFT.has(sel)) {
      problems.push(`CHANGED vs 381: ${sel}\n      381: ${body}\n      now: ${now.get(sel)}`);
    }
  }
  for (const sel of now.keys()) {
    if (!ref.has(sel) && !ALLOWED_DRIFT.has(sel)) problems.push(`NEW (not in 381): ${sel}`);
  }
  assert.equal(problems.length, 0, `search UI drifted from build 381:\n  ${problems.join("\n  ")}`);
});

test("the ⌕ dropdown is the near-full-screen panel, not the compact top-right box", () => {
  // The two shapes the owner has explicitly rejected, named so a future reader knows which is
  // which. Independent of the 381 comparison, so it still fires in a shallow clone.
  const css = stripComments(fs.readFileSync(FILE, "utf8"));
  const start = css.indexOf(".navigation-drawer.as-dropdown.search-fullscreen {");
  assert.ok(start > 0, "the ⌕ dropdown rule is gone");
  const block = css.slice(start, css.indexOf("}", start));
  for (const side of ["top", "bottom", "left", "right"]) {
    assert.match(block, new RegExp(`${side}:\\s*max\\(1\\.75rem`), `${side} is not pinned 1.75rem from the edge (381 ring)`);
  }
  assert.doesNotMatch(block, /width:\s*min\(380px/, "this is the compact 380px box (v362-v425), not 381");
  assert.doesNotMatch(block, /bottom:\s*auto/, "bottom:auto — the panel no longer reaches the floor");
  const paneStart = css.indexOf(".navigation-drawer.as-dropdown.search-fullscreen .drawer-pane-content {");
  const pane = css.slice(paneStart, css.indexOf("}", paneStart));
  assert.match(pane, /max-height:\s*none/, "results list is capped — it must fill the panel (381)");
  assert.doesNotMatch(pane, /min\(60vh/, "the compact-era 60vh cap is back on the results list");
});
