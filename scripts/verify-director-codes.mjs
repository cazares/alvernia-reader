#!/usr/bin/env node
/**
 * verify-director-codes.mjs — prove the codes that let a human become director are usable.
 *
 *   node scripts/verify-director-codes.mjs [path]     # default: director-codes.private.json
 *
 * WHY THIS EXISTS. On 2026-07-01 a build shipped in which nobody could become director, and it was
 * discovered at Mass. `release.sh` swaps the gitignored director-codes.private.json over the tracked
 * (empty) director-codes.json for the archive, and if that file is absent it printed one WARNING
 * line inside a ~10-minute build log and carried on. PdfReaderApp.tsx then reads
 * `standardDirectorCodes || []`, so the app builds perfectly, installs perfectly, and rejects every
 * code anyone types. A broken build is INDISTINGUISHABLE from a good one until a director needs it.
 *
 * NEVER PRINTS A CODE. These are real phone numbers. Output is counts, digit lengths and indexes
 * only, so this is safe to run with someone looking over your shoulder and safe to paste into a
 * bug report.
 *
 * Exit 0 = a director can take the role in a build made from this file. Exit 1 = they cannot.
 */
import fs from "node:fs";
import path from "node:path";

// Kept in sync with PdfReaderApp.tsx by e2e/director-codes.test.mjs. A director code equal to any
// of these would be shadowed: the numpad handler checks them BEFORE the director set, so the code
// would soft-reset the device or apply a book instead of granting the role.
const RESERVED = {
  "744668486": "SOFT_RESET_CODE",
  "265134902": "BOOK_APPLY_CODE",
  "907315268": "BOOK_FORCE_BAKED_CODE",
};

const digits = (c) => String(c).replace(/[^0-9]/g, "");
const file = process.argv[2] || "director-codes.private.json";
const problems = [];
const note = (m) => problems.push(m);

if (!fs.existsSync(file)) {
  console.error(`✖ ${path.resolve(file)} does not exist.`);
  console.error("  release.sh bakes this file into the archive; without it the IPA builds fine and");
  console.error("  NOBODY CAN BECOME DIRECTOR. That is the 2026-07-01 outage.");
  process.exit(1);
}

let raw;
try {
  raw = JSON.parse(fs.readFileSync(file, "utf8"));
} catch (e) {
  console.error(`✖ ${file} is not valid JSON: ${e.message}`);
  process.exit(1);
}

// `|| []` in PdfReaderApp.tsx means a misspelled key yields an EMPTY set rather than a crash, so a
// typo here is exactly as fatal as a missing file and just as quiet. Check the key, not the value.
if (!Array.isArray(raw.standardDirectorCodes)) {
  note("standardDirectorCodes is missing or not an array — the app will read it as [] and reject every code");
}
if (raw.superAdminCodes !== undefined && !Array.isArray(raw.superAdminCodes)) {
  note("superAdminCodes is present but not an array");
}

const std = (Array.isArray(raw.standardDirectorCodes) ? raw.standardDirectorCodes : []).map(digits);
const sa = (Array.isArray(raw.superAdminCodes) ? raw.superAdminCodes : []).map(digits);

if (std.length === 0) note("standardDirectorCodes is EMPTY — nobody can become director");

std.forEach((c, i) => {
  if (!c) note(`standardDirectorCodes[${i}] has no digits at all`);
});

const seen = new Map();
std.forEach((c, i) => {
  if (seen.has(c)) note(`standardDirectorCodes[${i}] duplicates index ${seen.get(c)}`);
  else seen.set(c, i);
});

std.forEach((c, i) => {
  if (RESERVED[c]) note(`standardDirectorCodes[${i}] equals ${RESERVED[c]} — the numpad handles that BEFORE the director check, so this code can never grant the role`);
});

// THE SUBSET TRAP. PdfReaderApp.tsx:820 rejects anything not in STANDARD_DIRECTOR_CODES and returns
// immediately; the super-admin branch at :830 is only ever reached for a code that already passed.
// So a super-admin code missing from the standard list is not "super admin without the label" — it
// is simply refused, and the one person most likely to be debugging at 7pm cannot take the role.
sa.forEach((c, i) => {
  if (!seen.has(c)) note(`superAdminCodes[${i}] is NOT in standardDirectorCodes — PdfReaderApp.tsx rejects it before the super-admin branch runs, so this code CANNOT direct`);
});

// Read off a laminated card, in poor light, in a church, by someone under pressure. Same reasoning
// that put BOOK_APPLY_CODE at distance 9 from SOFT_RESET_CODE.
for (let i = 0; i < std.length; i += 1) {
  for (let k = i + 1; k < std.length; k += 1) {
    if (std[i].length !== std[k].length) continue;
    let d = 0;
    for (let z = 0; z < std[i].length; z += 1) if (std[i][z] !== std[k][z]) d += 1;
    if (d === 1) note(`standardDirectorCodes[${i}] and [${k}] differ by ONE digit — a single misread hands the role to the wrong person`);
  }
}

console.log(`director codes in ${file}`);
console.log(`  standard   : ${std.length} (${std.length ? `digit lengths ${JSON.stringify(std.map((c) => c.length))}` : "none"})`);
console.log(`  super admin: ${sa.length}${sa.length ? ` (indexes in standard: ${JSON.stringify(sa.map((c) => (seen.has(c) ? seen.get(c) : "MISSING")))})` : ""}`);

if (problems.length) {
  console.error("");
  for (const p of problems) console.error(`  ✖ ${p}`);
  console.error("");
  console.error("A build made from this file cannot be directed. Fix the file, then re-run.");
  process.exit(1);
}
console.log("✅ a director can take the role in a build made from this file.");
