import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const SH = fs.readFileSync("scripts/release.sh", "utf8");

// A build number is spent the moment it is uploaded — App Store Connect refuses duplicates. The
// archive still succeeds, so you only learn about it ~10 minutes later, from Transporter. That
// happened twice in one evening (412, then 413) because nothing in this repo records what was
// UPLOADED, only what was BUILT — leaving the check to somebody's memory.

// THE GUARD IS RUN, NOT READ.
//
// What the old assertion missed: it grepped a fixed 1800-character window after the bump for the
// literal text `exit 1`. release.sh carries `exit 1` in five other places, and the guard's own abort
// stays in the window even when its CONDITION is inverted — so flipping `[ -e "$IPA_OUT" ]` to
// `[ ! -e "$IPA_OUT" ]`, which aborts every legitimate build and waves the 412/413 double-spend
// straight through, left the test green. A regex can see that an abort exists; it cannot see what
// the abort is conditioned on. So the block is now cut out of release.sh and EXECUTED against a
// throwaway $HOME, and the assertions are on its exit status in each of the four situations that
// matter.
//
// It is sliced on structural bounds — the line that computes IPA_OUT, through the `fi` that closes
// its `if` at that `if`'s own indentation — never a character count, and both endpoints are checked
// so a deleted end marker cannot silently turn the "window" into the rest of the file.
const guardSnippet = () => {
  const startMarker = 'IPA_OUT="$HOME/Desktop/SignoVivo-${BUILD}.ipa"';
  const endMarker = "\n  fi\n";
  const start = SH.indexOf(startMarker);
  assert.ok(start > 0, "the IPA collision guard no longer computes an IPA path at the new build number");
  const end = SH.indexOf(endMarker, start);
  assert.ok(end > start, "could not find the fi that closes the IPA collision guard");
  const snippet = SH.slice(start, end + endMarker.length);
  assert.match(snippet, /^\s*if\s+\[/m, "the extracted block contains no conditional — it is not a guard");
  return snippet;
};

/**
 * Run the real guard block with a throwaway $HOME. Returns its exit status and stderr.
 * `existingIpas` are basenames dropped into $HOME/Desktop before the guard runs.
 */
const runGuard = ({ build, existingIpas = [], env = {} }) => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "sv-guard-home-"));
  try {
    fs.mkdirSync(path.join(home, "Desktop"));
    for (const name of existingIpas) fs.writeFileSync(path.join(home, "Desktop", name), "not really an ipa");
    const script = path.join(home, "guard.sh");
    fs.writeFileSync(script, `set -euo pipefail\nBUILD="\${BUILD}"\n${guardSnippet()}\nexit 0\n`);
    const r = spawnSync("bash", [script], {
      encoding: "utf8",
      env: { PATH: process.env.PATH, HOME: home, BUILD: String(build), ...env },
    });
    assert.equal(r.error, undefined, `could not run the extracted guard: ${r.error}`);
    assert.notEqual(r.status, null, "the extracted guard was killed by a signal");
    return { status: r.status, stderr: r.stderr };
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
};

test("the bump refuses a number whose IPA already exists", () => {
  // The collision it was written to stop: build 413's IPA is already sitting on the Desktop.
  const collision = runGuard({ build: 413, existingIpas: ["SignoVivo-413.ipa"] });
  assert.equal(collision.status, 1,
    "release.sh proceeds to archive a build number whose IPA already exists — this is the 412/413 double-spend");
  assert.match(collision.stderr, /already has an IPA on your Desktop/,
    "the guard aborts without saying why");
  assert.match(collision.stderr, /SignoVivo-413\.ipa/, "the abort does not name the colliding file");
});

test("the guard lets a genuinely fresh build number through", () => {
  // The other half of the condition, and the half a `exit 1`-presence regex could never see: an
  // inverted test aborts EVERY legitimate release, which is a worse outage than the one it prevents.
  assert.equal(runGuard({ build: 414 }).status, 0,
    "release.sh refuses a build number that has never been archived — the guard's condition is inverted");
  // An IPA at a DIFFERENT number must not count; the guard is about THIS build, not any build.
  assert.equal(runGuard({ build: 414, existingIpas: ["SignoVivo-413.ipa"] }).status, 0,
    "an unrelated IPA on the Desktop blocks the build — the guard is not keyed on ${BUILD}");
});

test("ALLOW_REUSED_BUILD=1 actually overrides the refusal", () => {
  // A build that was archived but never uploaded is legitimately replaceable. The escape hatch is
  // only worth documenting if it works, so it is exercised rather than grepped for.
  const overridden = runGuard({
    build: 413,
    existingIpas: ["SignoVivo-413.ipa"],
    env: { ALLOW_REUSED_BUILD: "1" },
  });
  assert.equal(overridden.status, 0,
    "ALLOW_REUSED_BUILD=1 no longer gets past the guard — the documented way out is a dead end");
});

test("the guard runs BEFORE the archive, not after", () => {
  // Catching it afterwards costs exactly the ten minutes the guard exists to save.
  const guard = SH.indexOf("SignoVivo-${BUILD}.ipa");
  const archive = SH.indexOf("xcodebuild");
  assert.ok(guard > 0 && archive > 0, "could not locate both the guard and the archive step");
  assert.ok(guard < archive, "the guard fires after xcodebuild — the ten minutes are already spent");
});

test("the override is opt-in, and only the exact value opts in", () => {
  // A build that was archived but never uploaded is legitimately replaceable — but that has to be
  // an explicit statement, not the fallback. Previously this quoted the source text of the
  // condition, which a rewrite would pass through; it now demonstrates the default by running it.
  const collide = (env) => runGuard({ build: 413, existingIpas: ["SignoVivo-413.ipa"], env }).status;
  assert.equal(collide({}), 1, "with the variable unset the guard already allows the reuse — it is not opt-in");
  assert.equal(collide({ ALLOW_REUSED_BUILD: "0" }), 1, "ALLOW_REUSED_BUILD=0 is being read as permission");
  assert.equal(collide({ ALLOW_REUSED_BUILD: "yes" }), 1, "any truthy-looking value opens the override");
});

test("the error names the file, its date, and both ways out", () => {
  // Someone hitting this at 11:40 on a Sunday needs the decision made for them, not a puzzle.
  const at = SH.indexOf("already has an IPA on your Desktop");
  assert.ok(at > 0, "the message no longer names the collision");
  const msg = SH.slice(at, at + 900);
  assert.match(msg, /date -r/, "does not show WHEN the existing build was made");
  assert.match(msg, /bump-build\.mjs && bash scripts\/release\.sh/, "does not give the bump-past command");
  assert.match(msg, /ALLOW_REUSED_BUILD=1 bash scripts\/release\.sh/, "does not give the override command");
});
