// A TestFlight build must NEVER be attached to an external testing group by these scripts.
//
// Miguel, 2026-09-04: "just don't make testflight build available to 'TODOS' external testing group,
// NOR any external testing group on testflight … that goes moving forward, remember that persistently."
// External groups are the whole choir; a build there is a build in front of the congregation. He
// hardware-tests on his own devices first, every time, and adds testers himself.
//
// What made this a live risk: testflight-distribute.mjs with no --groups attached the build to EVERY
// beta group — external ones included — and then submitted it for beta review. release.sh step 4c
// calls it exactly that way after an API-key upload. So the default had to flip: internal groups only,
// and touching an external group requires an explicit --allow-external that release.sh never passes.
//
// The selection rule is lifted out of the script and EXECUTED here against a modelled group list, so
// this proves the decision, not the presence of a flag name. Re-injected by
// scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = readFileSync(join(ROOT, "scripts", "testflight-distribute.mjs"), "utf8");
const RELEASE = readFileSync(join(ROOT, "scripts", "release.sh"), "utf8");

/** Lift `function selectTargets(...) { ... }` out of the script and return it as a callable. */
function liftSelectTargets() {
  const at = DIST.indexOf("function selectTargets(");
  assert.notEqual(at, -1, "selectTargets is gone — the group-selection rule is no longer a testable unit");
  // The parameter list holds a destructuring pattern, so it has its own `{` and its own comma. Find
  // the BODY brace as the first `{` after the closing `)` of the parameters — the first `{` after the
  // name is the pattern's, and matching from there lifts ` only, allowExternal ` as the "body". And
  // hand the whole parameter list to the Function constructor as ONE string, never split on commas.
  const paramsOpen = DIST.indexOf("(", at);
  const paramsClose = DIST.indexOf(")", paramsOpen);
  const params = DIST.slice(paramsOpen + 1, paramsClose);
  const open = DIST.indexOf("{", paramsClose);
  let depth = 0;
  for (let i = open; i < DIST.length; i++) {
    if (DIST[i] === "{") depth++;
    else if (DIST[i] === "}") {
      depth--;
      if (depth === 0) {
        // eslint-disable-next-line no-new-func
        return new Function(params, DIST.slice(open + 1, i));
      }
    }
  }
  assert.fail("unbalanced braces in selectTargets");
}

const GROUPS = [
  { attributes: { name: "Internal", isInternalGroup: true } },
  { attributes: { name: "TODOS", isInternalGroup: false } },
  { attributes: { name: "Directores", isInternalGroup: false } },
];
const names = (gs) => gs.map((g) => g.attributes.name);

test("by default only INTERNAL groups are selected — an external group is never attached implicitly", () => {
  const selectTargets = liftSelectTargets();
  assert.deepEqual(names(selectTargets(GROUPS, { only: undefined, allowExternal: false })), ["Internal"],
    "the default selection reached an external group — that is the whole choir");
});

test("naming an external group without --allow-external selects NOTHING, not the group", () => {
  const selectTargets = liftSelectTargets();
  assert.deepEqual(names(selectTargets(GROUPS, { only: "TODOS", allowExternal: false })), [],
    "--groups TODOS must not be enough on its own to reach an external group");
  assert.deepEqual(names(selectTargets(GROUPS, { only: "todos,internal", allowExternal: false })), ["Internal"],
    "a mixed list must keep only the internal member");
});

// (Title must not START with `--`: the mutation harness passes test names through --test-name-pattern,
//  and node's argv parser swallows a leading `--…` as an option, so the name would match nothing.)
test("only --allow-external reaches an external group, and it still requires naming it", () => {
  const selectTargets = liftSelectTargets();
  assert.deepEqual(names(selectTargets(GROUPS, { only: "TODOS", allowExternal: true })), ["TODOS"],
    "a deliberate, named, opted-in external group must still be reachable");
  assert.deepEqual(names(selectTargets(GROUPS, { only: undefined, allowExternal: true })), ["Internal"],
    "--allow-external without --groups must NOT mean 'every external group'");
});

test("release.sh never opts into external groups", () => {
  const call = RELEASE.match(/node scripts\/testflight-distribute\.mjs[^\n]*/g) || [];
  assert.ok(call.length >= 1, "release.sh no longer calls testflight-distribute.mjs — check step 4c");
  for (const c of call) {
    assert.doesNotMatch(c, /--allow-external/, `release.sh opts into external groups: ${c}`);
  }
});
