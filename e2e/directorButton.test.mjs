import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const HTML = fs.readFileSync("web/src/index.html", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");

// Becoming director used to require typing a memorised 9-digit code. That fails in the one scenario
// that matters — the director's iPad dies five minutes before Mass and whoever picks up the spare
// does not know the number. The button replaces the memory requirement; the native confirmation,
// not the secrecy of a number, is what still prevents an accidental takeover.

test("the button never appears on the public web", () => {
  // signovivo.com serves this same bundle to anyone. There is no mesh there and no role to take —
  // an offer to 'direct the choir' on the public site is at best dead, at worst confusing.
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  assert.match(open, /NATIVE_FILE_MODE \|\| hasNativeBridge\(\)/, "reveal is not gated on the native shell");
  assert.match(HTML, /id="direct-button"[\s\S]{0,80}?type="button"/, "button missing from the modal");
  assert.match(HTML, /class="song-jump-direct is-hidden"/, "button must ship hidden and be revealed by JS");
});

test("the button is not offered to a device already directing", () => {
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  assert.match(open, /state\.nativeSyncRole !== "director"/, "a director is still offered the role");
});

test("the reveal is re-evaluated on every open, not once at boot", () => {
  // A director who steps down must be able to take it back without relaunching the app.
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  assert.match(open, /directButton\.classList\.toggle\("is-hidden"/, "reveal is not recomputed on open");
});

test("the web never learns the director code — it asks", () => {
  // The whole point of routing through the shell: the bundle is served publicly over HTTPS.
  assert.ok(!APP.includes("333444555"), "DIRECTOR_CODE leaked into the web bundle");
  assert.ok(!HTML.includes("333444555"), "DIRECTOR_CODE leaked into index.html");
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
  const handler = NATIVE.slice(NATIVE.indexOf('case "request-director":'));
  assert.match(handler.slice(0, 200), /onDirectorCode\(DIRECTOR_CODE\)/, "bypasses onDirectorCode");
});
