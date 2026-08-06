import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const SH = fs.readFileSync("scripts/release.sh", "utf8");

// A build number is spent the moment it is uploaded — App Store Connect refuses duplicates. The
// archive still succeeds, so you only learn about it ~10 minutes later, from Transporter. That
// happened twice in one evening (412, then 413) because nothing in this repo records what was
// UPLOADED, only what was BUILT — leaving the check to somebody's memory.

test("the bump refuses a number whose IPA already exists", () => {
  const at = SH.indexOf("node scripts/bump-build.mjs");
  assert.ok(at > 0, "the bump is gone");
  const after = SH.slice(at, at + 1800);
  assert.match(after, /SignoVivo-\$\{BUILD\}\.ipa/, "nothing checks for an existing IPA at the new number");
  assert.match(after, /exit 1/, "the guard warns but does not stop the build");
});

test("the guard runs BEFORE the archive, not after", () => {
  // Catching it afterwards costs exactly the ten minutes the guard exists to save.
  const guard = SH.indexOf("SignoVivo-${BUILD}.ipa");
  const archive = SH.indexOf("xcodebuild");
  assert.ok(guard > 0 && archive > 0, "could not locate both the guard and the archive step");
  assert.ok(guard < archive, "the guard fires after xcodebuild — the ten minutes are already spent");
});

test("there is a deliberate override, and it is not the default", () => {
  // A build that was archived but never uploaded is legitimately replaceable — but that has to be
  // an explicit statement, not the fallback.
  assert.match(SH, /ALLOW_REUSED_BUILD/, "no way to deliberately replace an un-uploaded build");
  assert.match(SH, /\$\{ALLOW_REUSED_BUILD:-0\}" != "1"/, "the override is not opt-in");
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
