import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const RELEASE = fs.readFileSync("scripts/release.sh", "utf8");
const DIST = fs.readFileSync("scripts/testflight-distribute.mjs", "utf8");

// WHY THIS FILE EXISTS. Build 451 was uploaded, reported as a success by every step that ran, and
// reached nobody: it sat at "Ready to Submit" because attaching a build to an external beta group
// is not the same as submitting it for beta review, and release.sh stopped before both. The gap was
// invisible from the terminal and obvious in the App Store Connect UI.
//
// These are cheap string assertions on purpose — the real behaviour needs Apple's API and cannot run
// in CI. What they pin is that the STEPS still exist and still abort the release, which is the part
// that silently regresses when someone simplifies the tail of a shell script.

test("release.sh distributes the build — uploading is not shipping", () => {
  assert.match(RELEASE, /node scripts\/testflight-distribute\.mjs --build "\$BUILD"/,
    "release.sh no longer attaches the build to its beta groups — it will upload and reach nobody");
  // Must ABORT. A distribution failure that only prints leaves you believing the choir has a build
  // they cannot install, which is strictly worse than a failed release.
  const block = RELEASE.slice(RELEASE.indexOf("4c/6"), RELEASE.indexOf("4c/6") + 600);
  assert.match(block, /if ! node scripts\/testflight-distribute/, "the distribution result is not checked");
  assert.match(block, /exit 1/, "a failed distribution no longer aborts the release");
  // The old ending told a human to go do it by hand. That instruction is what got missed.
  assert.doesNotMatch(RELEASE, /Add it to the choir group/,
    "release.sh again defers the last step to a human — that is the failure this replaced");
});

test("the distributor submits for beta review, not just group assignment", () => {
  assert.match(DIST, /betaAppReviewSubmissions/,
    "the beta review submission is gone — an external group leaves the build at 'Ready to Submit'");
  assert.match(DIST, /buildBetaDetail/,
    "nothing reads back the real state; every POST can succeed while the build reaches no one");
});

test("an unrecognised external state fails rather than passing quietly", () => {
  // The first version of this map omitted IN_BETA_TESTING — the actual success value. A map that
  // defaults unknown to OK would reproduce the exact bug it exists to catch, so the default is FAIL.
  assert.match(DIST, /const SHIPPED = \{[^}]*IN_BETA_TESTING/, "IN_BETA_TESTING is not treated as shipped");
  assert.match(DIST, /const BROKEN\s*=\s*\{[^}]*READY_FOR_BETA_SUBMISSION/,
    "'Ready to Submit' is no longer classified as broken — that is the state 451 was stuck in");
  // Slice from the LAST else so the assertion is about the fallthrough branch specifically.
  // ([^}]* fails here: the branch logs a template literal, and ${v} closes the character class early.)
  const tail = DIST.slice(DIST.lastIndexOf("else {"));
  assert.match(tail, /process\.exitCode = 1/,
    "the fallthrough branch no longer fails — an unrecognised Apple state would read as success");
  assert.doesNotMatch(tail, /^\s*else \{\s*\}/, "the fallthrough branch is empty");
});
