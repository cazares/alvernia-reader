// A superseded becomeDirector must never release a NEWER becomeDirector's in-flight claim.
//
// #392 gated the follower render-failed sentinel (currentPageRef = -1) on
// !becomeDirectorInFlightRef.current, because roleRef stays "follower" for the whole of
// becomeDirector's await window (resetNearbyDirectorSync, startNearbyDirector, a 2 s retry sleep,
// AsyncStorage). But the ref was ONE shared boolean, set `true` at the top of every becomeDirector
// and set `false` on every exit — INCLUDING the exits taken because `myGen !== roleGenerationRef`,
// i.e. because a NEWER becomeDirector had already started. So two overlapping calls (a code entered
// and confirmed twice while the mesh was still starting — the exact overlap roleGenerationRef exists
// to survive) left the newer call unprotected for the rest of its window: the OLDER call's
// superseded exit wrote `false`, a render-failed passed the gate, and the mirror correction the
// newer call had just made from the page the user was actually on was silently undone.
//
// The fix makes the ref hold the GENERATION that claimed it, and every release is conditional on
// still owning that claim. 0 is falsy, so the render-failed gate reads it unchanged.
//
// These are structural pins on a file that cannot be imported without a React Native runtime. They
// are only worth anything because scripts/verify-behavioural-guards.mjs re-injects the defect (one
// release made unconditional again) and requires the NAMED test below to go red.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SRC = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");

// Extract a balanced brace-delimited body starting at `from`. Never runs to EOF on a missing
// marker (the failure shape that made five files in this repo assert nothing).
function bodyAt(from, what) {
  const open = SRC.indexOf("{", from);
  assert.notEqual(open, -1, `no body found for ${what}`);
  let depth = 0;
  for (let i = open; i < SRC.length; i++) {
    if (SRC[i] === "{") depth++;
    else if (SRC[i] === "}") {
      depth--;
      if (depth === 0) return SRC.slice(open, i + 1);
    }
  }
  assert.fail(`unbalanced braces reading ${what}`);
}

const count = (re) => (SRC.match(re) ?? []).length;

// A plain assignment (`=`), never `===` — `(?!=)` keeps the comparison in the guard out of this.
const ANY_ASSIGN = /becomeDirectorInFlightRef\.current\s*=(?!=)\s*[^;]+;/g;
const CLAIM = /becomeDirectorInFlightRef\.current\s*=(?!=)\s*myGen;/g;
const RELEASE = /becomeDirectorInFlightRef\.current\s*=(?!=)\s*0;/g;
const GUARDED_RELEASE =
  /if\s*\(\s*becomeDirectorInFlightRef\.current\s*===\s*myGen\s*\)\s*becomeDirectorInFlightRef\.current\s*=\s*0;/g;

test("becomeDirector claims the in-flight ref with its own generation, not a boolean", () => {
  // assert.ok over a RegExp test, not assert.match: on failure match() prints the whole 2,000-line
  // source as `actual`, which buries the one line that says why.
  assert.ok(
    /const becomeDirectorInFlightRef = useRef(?:<[^>]*>)?\(0\);/.test(SRC),
    "the ref must be a generation number (0 = nobody in flight), not useRef(false)",
  );
  assert.ok(
    !/becomeDirectorInFlightRef\.current\s*=(?!=)\s*true\b/.test(SRC),
    "a boolean claim cannot tell two overlapping becomeDirector calls apart",
  );

  const at = SRC.indexOf("const becomeDirector = useCallback(");
  assert.notEqual(at, -1, "becomeDirector must exist");
  const body = bodyAt(at, "becomeDirector");
  const genAt = body.indexOf("const myGen = ++roleGenerationRef.current;");
  const claimAt = body.search(CLAIM);
  const firstAwait = body.indexOf("await ");
  assert.notEqual(genAt, -1, "becomeDirector must still capture its generation");
  assert.notEqual(claimAt, -1, "becomeDirector must write its generation into the in-flight ref");
  assert.ok(genAt < claimAt, "the claim must use the generation captured at entry");
  assert.ok(firstAwait === -1 || claimAt < firstAwait, "the claim must be made before the first await opens the window");
});

test("no exit path releases the in-flight ref by writing false", () => {
  assert.equal(
    count(/becomeDirectorInFlightRef\.current\s*=(?!=)\s*false\b/g),
    0,
    "an unconditional `= false` on a superseded exit releases a NEWER call's claim",
  );
});

test("every release of the in-flight claim is guarded by === myGen", () => {
  const assigns = count(ANY_ASSIGN);
  const claims = count(CLAIM);
  const releases = count(RELEASE);
  const guarded = count(GUARDED_RELEASE);

  assert.equal(claims, 1, "exactly one site claims the ref (the top of becomeDirector)");
  assert.ok(releases >= 1, "there must be at least one release, or the claim is never let go");
  assert.equal(
    releases,
    guarded,
    `${releases} release(s) but only ${guarded} guarded — an unguarded release lets a superseded call clear a newer call's claim`,
  );
  assert.equal(assigns, claims + releases, "every write to the ref is either the claim or a guarded release");
});
