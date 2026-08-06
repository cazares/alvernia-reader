import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";

const VERIFY = "scripts/verify-director-codes.mjs";
const run = (obj) => {
  const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "dc-")), "c.json");
  fs.writeFileSync(f, JSON.stringify(obj));
  const r = spawnSync("node", [VERIFY, f], { encoding: "utf8" });
  return { ok: r.status === 0, out: `${r.stdout}${r.stderr}` };
};

// THE DRIFT THIS PINS. verify-director-codes.mjs keeps its own copy of the numpad's reserved codes,
// because it validates a JSON file and cannot import a .tsx. If someone changes SOFT_RESET_CODE or
// a book code in PdfReaderApp.tsx and not here, the verifier stops catching a director code that
// collides with it — and a colliding code silently soft-resets the device instead of granting the
// role, because the numpad checks those BEFORE the director set.
test("reserved codes in the verifier match PdfReaderApp.tsx", () => {
  const app = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const verifier = fs.readFileSync(VERIFY, "utf8");
  for (const name of ["SOFT_RESET_CODE", "BOOK_APPLY_CODE", "BOOK_FORCE_BAKED_CODE"]) {
    const m = app.match(new RegExp(`const ${name} = "(\\d+)"`));
    assert.ok(m, `${name} not found in PdfReaderApp.tsx — did it get renamed?`);
    assert.ok(
      verifier.includes(`"${m[1]}": "${name}"`),
      `${name} is ${m[1]} in PdfReaderApp.tsx but the verifier's RESERVED map disagrees`,
    );
  }
});

// The numpad handler checks the director set at PdfReaderApp.tsx:820 and returns immediately when a
// code is absent. The super-admin branch below it is therefore unreachable for a code that is not
// ALSO a standard code — so this is not a cosmetic label problem, it is "the super admin cannot
// direct". Asserted here because nothing in the app would ever tell you.
test("a super-admin code outside the standard list is rejected", () => {
  const r = run({ standardDirectorCodes: ["5550001111"], superAdminCodes: ["5559999999"] });
  assert.equal(r.ok, false);
  assert.match(r.out, /CANNOT direct/);
});

test("an empty or mistyped codes file is rejected", () => {
  assert.equal(run({ standardDirectorCodes: [], superAdminCodes: [] }).ok, false);
  assert.equal(run({ standardDirectorCode: ["5550001111"] }).ok, false);
});

test("a director code colliding with a reserved code is rejected", () => {
  const soft = fs.readFileSync("PdfReaderApp.tsx", "utf8").match(/const SOFT_RESET_CODE = "(\d+)"/)[1];
  assert.equal(run({ standardDirectorCodes: [soft] }).ok, false);
});

test("two director codes one digit apart are rejected", () => {
  assert.equal(run({ standardDirectorCodes: ["5550001111", "5550001112"] }).ok, false);
});

test("a well-formed file is accepted, and prints no digits", () => {
  const r = run({
    standardDirectorCodes: ["555-000-1111", "5550002222"],
    superAdminCodes: ["5550002222"],
  });
  assert.equal(r.ok, true, r.out);
  // Real phone numbers. The output must be safe to paste into a bug report.
  assert.ok(!/5550001111|5550002222/.test(r.out), `output leaked a code:\n${r.out}`);
});

// release.sh must ABORT rather than warn — the 2026-07-01 outage was a warning nobody saw inside a
// ten-minute build log, producing an IPA that installed fine and rejected every code at Mass.
test("release.sh fails closed when the codes file is missing", () => {
  const sh = fs.readFileSync("scripts/release.sh", "utf8");
  const block = sh.slice(sh.indexOf("if [ -f director-codes.private.json ]"));
  assert.ok(block.includes("ALLOW_NO_DIRECTOR_CODES"), "no explicit override switch");
  assert.ok(/exit 1/.test(block.slice(0, block.indexOf("rm -rf build"))), "missing file does not abort the archive");
  assert.ok(block.includes("verify-director-codes.mjs"), "release.sh does not validate a PRESENT file");
});
