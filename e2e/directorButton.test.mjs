import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const HTML = fs.readFileSync("web/src/index.html", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const NATIVE_FILE_MODE = false; // placeholder for the executed reveal decision below

// Becoming director used to require typing a memorised 9-digit code. That fails in the one scenario
// that matters — the director's iPad dies five minutes before Mass and whoever picks up the spare
// does not know the number. The button replaces the memory requirement; the native confirmation,
// not the secrecy of a number, is what still prevents an accidental takeover.

// Presence checks over a big slice are what made the first version of this file worthless: both
// this test and the rescue one were satisfied by the single surviving `const inShell = …` line even
// after either gate was deleted. Execute the reveal decision instead of grepping near it.
const revealDecision = () => {
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  const body = open.replace(/^[\s\S]*?=> \{/, "").replace(/\};?\s*$/, "");
  return (NATIVE_FILE_MODE, hasBridge, role) => {
    const calls = {};
    const el = (name) => ({ classList: { toggle: (_c, v) => { calls[name] = v; }, add: () => { calls[name + ":collapsed"] = true; } } });
    const fn = new Function(
      "NATIVE_FILE_MODE", "hasNativeBridge", "state", "directButton", "rescueWrap", "rescueActions",
      "rescueToggle", "songJumpModal", "clearDraft",
      body.replace(/songJumpModal\.classList\.remove\("is-hidden"\);/, "").replace(/state\.songJumpOpen = true;/, ""),
    );
    fn(NATIVE_FILE_MODE, () => hasBridge, { nativeSyncRole: role, songJumpOpen: false },
       el("direct"), el("rescue"), el("rescueActions"), { setAttribute() {} }, { classList: { remove() {} } }, () => {});
    return calls;
  };
};

test("the button never appears on the public web", () => {
  // signovivo.com serves this same bundle to anyone. No mesh, no role to take — an offer to
  // 'direct the choir' there is dead at best. `true` passed to toggle() means HIDDEN.
  const decide = revealDecision();
  assert.equal(decide(false, false, "follower").direct, true, "direct button REVEALED on the public web");
  assert.equal(decide(false, false, "follower").rescue, true, "rescue block REVEALED on the public web");
  // ...and it must still work in the shell, or the gate is just "always hidden".
  assert.equal(decide(true, false, "follower").direct, false, "button hidden inside the native shell");
  assert.equal(decide(false, true, "follower").direct, false, "button hidden when the bridge exists");
  assert.match(HTML, /class="song-jump-direct is-hidden"/, "must ship hidden and be revealed by JS");
});

test("the button is not offered to a device already directing", () => {
  const decide = revealDecision();
  assert.equal(decide(true, true, "director").direct, true, "a director is still offered the role");
  assert.equal(decide(true, true, "follower").direct, false, "a follower is denied the role");
});

test("the rescue block re-collapses on every open", () => {
  // Otherwise it is left expanded from a previous visit, and once-a-year controls sit permanently
  // beside the one people use constantly.
  const decide = revealDecision();
  assert.equal(decide(true, true, "follower")["rescueActions:collapsed"], true, "stays expanded between visits");
});

test("the web never learns the director code — it asks", () => {
  // Read the code FROM the native source rather than hardcoding it: the previous version pinned the
  // literal "333444555", so rotating DIRECTOR_CODE would have left this guarding a dead value while
  // the live one leaked freely. Also catch ANY 9-digit literal, since a leak rarely arrives spelled
  // the way you expected.
  const code = NATIVE.match(/const DIRECTOR_CODE = "(\d+)"/)[1];
  for (const [name, text] of [["app.js", APP], ["index.html", HTML]]) {
    assert.ok(!text.includes(code), `DIRECTOR_CODE leaked into ${name}`);
    const digits = [...text.matchAll(/(?<![\d.])(\d{9})(?![\d.])/g)].map((m) => m[1]);
    assert.deepEqual(digits, [], `${name} contains 9-digit literal(s) that may be operator codes: ${digits}`);
  }
  assert.match(APP, /postNativeBridge\(\{ type: "request-director" \}\)/, "button does not ask native");
});

test("the native shell understands request-director", () => {
  // A bundle that sends a message no shell handles is a button that silently does nothing. This
  // handler must be in the BINARY even though the button ships over the air.
  assert.match(NATIVE, /case "request-director":/, "native has no handler for the web button");
});

test("the request goes through the same confirmation as a typed code", () => {
  // Not a shortcut past the dialog — the takeover warning, the live-director check and
  // becomeDirector must all still run.
  // Scoped to the case body and required to be the ONLY call — the old 200-char window happily
  // accepted `void becomeDirector(DIRECTOR_CODE); if (0) onDirectorCode(DIRECTOR_CODE);`.
  const at = NATIVE.indexOf('case "request-director":');
  const body = NATIVE.slice(at, NATIVE.indexOf("break;", at));
  assert.match(body, /onDirectorCode\(DIRECTOR_CODE\)/, "bypasses onDirectorCode");
  assert.ok(!/becomeDirector\(/.test(body), "calls becomeDirector directly, skipping the confirmation");
});
