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
      "NATIVE_FILE_MODE", "hasNativeBridge", "state", "rescueWrap", "rescueActions",
      "rescueToggle", "songJumpModal", "clearDraft",
      body.replace(/songJumpModal\.classList\.remove\("is-hidden"\);/, "").replace(/state\.songJumpOpen = true;/, ""),
    );
    fn(NATIVE_FILE_MODE, () => hasBridge, { nativeSyncRole: role, songJumpOpen: false },
       el("rescue"), el("rescueActions"), { setAttribute() {} }, { classList: { remove() {} } }, () => {});
    return calls;
  };
};

test("the way in is the pill, not a button buried in the song-jump modal", () => {
  // "Dirigir el coro" used to sit inside a dialog titled IR A CANTO — wrong twice over: nobody
  // looking to direct opens the go-to-song keypad, and on a 10.2" portrait iPad it fell below the
  // card's fold. The pill shows the room's state AND is the control for it.
  assert.ok(!/id="direct-button"/.test(HTML), "the modal director button is back");
  assert.ok(!/directButton/.test(APP), "dead references to the removed button remain");
  assert.match(APP, /postNativeBridge\(\{ type: "request-director", currentPage: state\.currentPage \}\)/,
    "nothing asks native for the role any more");
});

// Brace-match a `const NAME = (...) => { ... }` body out of the source. Both endpoints are
// asserted, so a rename or a deleted declaration fails loudly instead of silently widening the
// window to EOF the way a missing end-marker does.
const arrowBody = (src, decl) => {
  const at = src.indexOf(decl);
  assert.ok(at >= 0, `could not find the declaration ${decl}`);
  const open = src.indexOf("{", src.indexOf("=>", at));
  assert.ok(open > at, `could not find the body of ${decl}`);
  let depth = 0, end = -1;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}") { depth--; if (depth === 0) { end = i; break; } }
  }
  assert.ok(end > open, `could not find the closing brace of ${decl}`);
  return src.slice(open + 1, end);
};

// Run the REAL renderDirectorModeBadge body against stub DOM/mesh dependencies. Stubs stand in for
// collaborators only — every branch that decides anything is the shipped source's.
const renderBadge = new Function(
  "NATIVE_FILE_MODE", "hasNativeBridge", "state", "directorModeBadge",
  "syncPillState", "SYNC_PILL", "document",
  arrowBody(APP, "const renderDirectorModeBadge = () => {"),
);

const runRenderBadge = ({ nativeFileMode = false, bridge = false, role = "follower", key = "nobody" } = {}) => {
  const classes = new Set(["is-hidden"]); // hidden markup is the boot state
  const badge = {
    classList: {
      add: (...c) => c.forEach((x) => classes.add(x)),
      remove: (...c) => c.forEach((x) => classes.delete(x)),
    },
    setAttribute() {},
  };
  const dataset = {};
  const doc = { documentElement: { dataset }, getElementById: () => ({ setAttribute() {}, textContent: "" }) };
  const pill = {
    directing: { cls: "", title: "d", action: "" },
    following: { cls: "is-following", title: "f", action: "" },
    nobody: { cls: "is-nobody", title: "n", action: "" },
  };
  renderBadge(nativeFileMode, () => bridge, { nativeSyncRole: role }, badge, () => key, pill, doc);
  return { classes, dataset };
};

test("the pill is the only thing that asks for the role, and it is shell-only", () => {
  // One request site. Two would drift, and the second is always the one nobody re-checks.
  assert.equal((APP.match(/type: "request-director"/g) || []).length, 1,
    "more than one place asks for the director role");

  // WHAT THE OLD ASSERTION MISSED. It grepped for the literal `NATIVE_FILE_MODE || hasNativeBridge()`
  // inside a slice whose END was the decorative `// ── Sync "working"` banner — and that same
  // expression appears three more times further down app.js. Reword the banner (a behaviour-neutral
  // edit) and the window ran past it, so `const inShell = true;` — the pill rendering on
  // signovivo.com, where there is no mesh and no role to take — passed green. This EXECUTES the
  // shipped renderDirectorModeBadge body instead, so the gate has to actually gate.
  const web = runRenderBadge({ bridge: false, nativeFileMode: false });
  assert.equal(web.dataset.shell, "web", "a plain browser is being told it is inside the native shell");
  assert.ok(web.classes.has("is-hidden"),
    "the pill is not gated to the native shell — signovivo.com has no room to describe");
  assert.equal(web.dataset.mesh, undefined,
    "the web path ran on past the shell gate and published mesh state it cannot know");

  // …and both ways into the shell still show it, so the gate cannot be 'fixed' by nailing it shut.
  for (const shell of [{ bridge: true }, { nativeFileMode: true }]) {
    const native = runRenderBadge(shell);
    assert.equal(native.dataset.shell, "native", `the shell is unrecognised for ${JSON.stringify(shell)}`);
    assert.ok(!native.classes.has("is-hidden"), `the pill is hidden inside the shell for ${JSON.stringify(shell)}`);
  }

  // The role attribute is set for EVERYONE, shell or not — that is what drives ⟳+♪ vs ♪+⌕, and the
  // web must default to follower controls from boot.
  assert.equal(runRenderBadge({ bridge: false, role: "director" }).dataset.role, "director");
  assert.equal(web.dataset.role, "follower");
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
  assert.match(APP, /postNativeBridge\(\{ type: "request-director", currentPage: state\.currentPage \}\)/,
    "button does not ask native");
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
  // onDirectorCode now also receives the web's true page (see relayQuotaGuards.test.mjs for why),
  // so the call is no longer a bare 1-arg form — match the call, not its exact argument list.
  assert.match(body, /onDirectorCode\(DIRECTOR_CODE(?:,[^)]*)?\)/, "bypasses onDirectorCode");
  assert.ok(!/becomeDirector\(/.test(body), "calls becomeDirector directly, skipping the confirmation");
});
