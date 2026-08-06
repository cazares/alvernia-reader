import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");

// v1.0.4 (404-406-373) — binary, web bundle, pages. Owner's format, 2026-08-06.
//
// These three numbers get read aloud across a choir loft and compared between two devices, so the
// ORDER is the whole contract: if slot 1 and slot 2 ever swapped, "the first number didn't change"
// would mean the opposite of what it means today, and the one check that proves an OTA worked would
// silently invert. Executed rather than grepped, so a reorder changes the RESULT.
const buildLabel = (() => {
  const src = APP.slice(APP.indexOf("const slot = (v) =>"), APP.indexOf("if (appVersionLabel)"));
  return (nativeBuild, webBuild, bookPages, baseVersion, deviceKind) =>
    new Function("nativeBuild", "webBuild", "bookPages", "baseVersion", "deviceKind",
      `${src}; return { buildLabel, kindSuffix };`,
    )(nativeBuild, webBuild, bookPages, baseVersion, deviceKind);
})();

test("the badge reads v<base> (binary-web-pages)", () => {
  const { buildLabel: label, kindSuffix } = buildLabel("404", "406", 373, "1.0.4", "PAD");
  assert.equal(label, "404-406-373");
  assert.equal(`v1.0.4 (${label})${kindSuffix}`, "v1.0.4 (404-406-373) PAD");
});

test("slot 1 is the BINARY and slot 2 is the WEB bundle, never swapped", () => {
  // The proof that an OTA worked is "the first number did not move while the second did". A swap
  // would keep every test that merely checks for three hyphen-separated numbers green while
  // inverting the only signal anyone reads.
  const { buildLabel: label } = buildLabel("404", "411", 380, "1.0.4", "PAD");
  assert.equal(label.split("-")[0], "404", "slot 1 is not the binary build");
  assert.equal(label.split("-")[1], "411", "slot 2 is not the web build");
  assert.equal(label.split("-")[2], "380", "slot 3 is not the page count");
});

test("the public web has no binary, and says so", () => {
  const { buildLabel: label, kindSuffix } = buildLabel("", "406", 373, "1.0.4", "");
  assert.equal(label, "web-406-373", "the missing binary slot must read 'web', not be dropped");
  assert.equal(kindSuffix, "", "device kind must be absent on the web — it is never guessed from a UA");
});

test("a missing value holds its column instead of collapsing the triple", () => {
  // Dropping a slot would shift the others left, so "404-373" could be read as binary 404 / web 373.
  const { buildLabel: label } = buildLabel("404", "406", 0, "1.0.4", "PAD");
  assert.equal(label.split("-").length, 3, "a missing value collapsed the positional triple");
  assert.equal(label, "404-406-—");
});

test("device kind stays outside the parentheses", () => {
  // Inside, it would make the triple mean different things on different devices.
  const { kindSuffix } = buildLabel("404", "406", 373, "1.0.4", "PHN");
  assert.equal(kindSuffix, " PHN");
  // Check INSIDE the parentheses specifically. `/\(.*PHN/` was wrong: `.*` runs straight past the
  // closing paren, so it matched the perfectly correct "(404-406-373) PHN".
  const rendered = `v1.0.4 (404-406-373)${kindSuffix}`;
  const inside = rendered.slice(rendered.indexOf("(") + 1, rendered.indexOf(")"));
  assert.ok(!/PHN|PAD/.test(inside), `device kind leaked into the triple: ${inside}`);
});

test("both surfaces print the same shape", () => {
  // The settings line and the small badge are read interchangeably over the phone; if they disagree
  // about what they are showing, the number someone reads out cannot be trusted.
  const settings = APP.slice(APP.indexOf("if (appVersionLabel)"), APP.indexOf("const buildBadge"));
  const badge = APP.slice(APP.indexOf("if (buildBadge)"));
  for (const [name, block] of [["settings line", settings], ["build badge", badge]]) {
    assert.match(block, /\$\{buildLabel\}/, `${name} does not use the shared label`);
    assert.match(block, /\$\{kindSuffix\}/, `${name} drops the device kind`);
    assert.match(block, /\(\$\{buildLabel\}\)/, `${name} does not parenthesise the triple`);
  }
});
