import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");

// v1.0.4 (404-406) · 373 cantos — binary, web bundle, then a NAMED song count.
//
// The count left the parenthesised triple on 2026-08-17: a bare third integer read as "how long is
// this document", which nobody in a loft needs, and the owner asked for songs rather than pages.
// Song n IS page n in this book, so only the noun changed — but naming it also removes the one slot
// whose meaning you had to know positionally.
//
// These three numbers get read aloud across a choir loft and compared between two devices, so the
// ORDER is the whole contract: if slot 1 and slot 2 ever swapped, "the first number didn't change"
// would mean the opposite of what it means today, and the one check that proves an OTA worked would
// silently invert. Executed rather than grepped, so a reorder changes the RESULT.
const buildLabel = (() => {
  const src = APP.slice(APP.indexOf("const slot = (v) =>"), APP.indexOf("if (appVersionLabel)"));
  return (nativeBuild, webBuild, bookPages, baseVersion, deviceKind) =>
    new Function("nativeBuild", "webBuild", "bookPages", "baseVersion", "deviceKind",
      `${src}; return { buildLabel, kindSuffix, songCount };`,
    )(nativeBuild, webBuild, bookPages, baseVersion, deviceKind);
})();

test("the badge reads v<base> (binary-web) · N cantos", () => {
  const { buildLabel: label, kindSuffix, songCount } = buildLabel("404", "406", 373, "1.0.4", "PAD");
  assert.equal(label, "404-406");
  assert.equal(songCount, "373 cantos", "the count must be NAMED, and must say cantos, not páginas");
  assert.equal(`v1.0.4 (${label})${kindSuffix} · ${songCount}`, "v1.0.4 (404-406) PAD · 373 cantos");
});

test("slot 1 is the BINARY and slot 2 is the WEB bundle, never swapped", () => {
  // The proof that an OTA worked is "the first number did not move while the second did". A swap
  // would keep every test that merely checks for three hyphen-separated numbers green while
  // inverting the only signal anyone reads.
  const { buildLabel: label, songCount } = buildLabel("404", "411", 380, "1.0.4", "PAD");
  assert.equal(label.split("-")[0], "404", "slot 1 is not the binary build");
  assert.equal(label.split("-")[1], "411", "slot 2 is not the web build");
  assert.equal(songCount, "380 cantos", "the count must survive leaving the triple");
});

test("the public web has no binary, and says so", () => {
  const { buildLabel: label, kindSuffix } = buildLabel("", "406", 373, "1.0.4", "");
  assert.equal(label, "web-406", "the missing binary slot must read 'web', not be dropped");
  assert.equal(kindSuffix, "", "device kind must be absent on the web — it is never guessed from a UA");
});

test("a missing value holds its column instead of collapsing the pair", () => {
  // Dropping a slot would shift the other left, so "406" alone could be read as the BINARY build —
  // exactly inverting the one signal anyone checks ("the first number didn't move, the second did").
  // Still the whole point of the placeholder; there are just two columns now instead of three.
  const { buildLabel: label } = buildLabel("404", "", 373, "1.0.4", "PAD");
  assert.equal(label.split("-").length, 2, "a missing value collapsed the positional pair");
  assert.equal(label, "404-—");
});

test("a missing song count is omitted, not shown as an empty noun", () => {
  // Unlike the positional slots, this one is NAMED — nothing shifts if it is absent, and "— cantos"
  // would be worse than saying nothing at all.
  const { songCount } = buildLabel("404", "406", 0, "1.0.4", "PAD");
  assert.equal(songCount, "", "a zero/absent count must vanish rather than render a placeholder");
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
