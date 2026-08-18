import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const HTML = fs.readFileSync("web/src/index.html", "utf8");

// EXECUTE the ⟳ handler rather than grep it. The claim is about a BRANCH — "with no director, do
// not reconnect, explain instead" — and a presence check for `dataset.mesh` passes just as happily
// when the early return under it has been deleted and the reconnect runs anyway.
const tapResync = (mesh) => {
  const body = (() => {
    const from = APP.indexOf('if (resyncFab) resyncFab.addEventListener("click", () => {');
    const open = APP.indexOf("{", APP.indexOf("() => {", from));
    let depth = 0, i = open;
    for (; i < APP.length; i++) {
      if (APP[i] === "{") depth++;
      else if (APP[i] === "}" && --depth === 0) break;
    }
    return APP.slice(open + 1, i);
  })();

  const calls = { reconnect: 0, spin: 0, tipShown: false, tipHidden: false };
  const tip = { classList: { remove: (c) => { if (c === "is-hidden") calls.tipShown = true; },
                             add:    (c) => { if (c === "is-hidden") calls.tipHidden = true; } } };
  const fab = { classList: { add: () => { calls.spin++; }, remove() {} } };
  const doc = { documentElement: { dataset: { mesh } } };

  new Function("resyncFab", "document", "haptic", "noDirTip", "closeNoDirTip", "reconnectRelay", "window", body)(
    fab, doc, () => {}, tip, () => tip.classList.add("is-hidden"),
    () => { calls.reconnect++; }, { setTimeout: () => 0 },
  );
  return calls;
};

test("with nobody directing, ⟳ explains itself instead of pretending to work", () => {
  // The reconnect is a genuine no-op with no director to reconnect TO, so running it would spin the
  // icon, change nothing, and teach the choir that the button is broken. That is precisely what the
  // crossing-out is warning about before the tap.
  const r = tapResync("nobody");
  assert.equal(r.tipShown, true, "tapping the crossed-out ⟳ does not open its explanation");
  assert.equal(r.reconnect, 0, "it still ran the reconnect — the button lies about having worked");
  assert.equal(r.spin, 0, "it still spun, which reads as 'I did something' when nothing happened");
});

test("in every other state ⟳ is unchanged — it reconnects", () => {
  // The tip must not become a gate in front of the resync. This button is the documented workaround
  // for the iPhone recovery stall (#352, 'tap ⟳'), so breaking it breaks a live mitigation.
  for (const mesh of ["following", "directing", undefined]) {
    const r = tapResync(mesh);
    assert.equal(r.reconnect, 1, `mesh=${mesh}: ⟳ no longer reconnects`);
    assert.equal(r.spin, 1, `mesh=${mesh}: no spin, so a slow reconnect looks like a dead button`);
    assert.equal(r.tipShown, false, `mesh=${mesh}: the no-director tip opened when a director exists`);
  }
});

test("the tip explains the state and does NOT teach anyone to take the role", () => {
  // THE POINT OF THE COPY. The audience is six FOLLOWERS, and #role-gate warns in as many words
  // that anyone who is not Braulio taking the role "va a causar problemas para TODO EL CORO". A
  // how-to on the one screen every singer sees when the seat is empty is instructions for producing
  // a split brain — which is strictly worse than the empty seat it would be explaining.
  // Comments stripped FIRST. The concern is what a singer can read on the glass, and the comment
  // above this markup necessarily names the control it is explaining the absence of — asserting
  // against the raw slice failed on the rationale rather than on the copy.
  const raw = HTML.slice(HTML.indexOf('id="nodir-info"'), HTML.indexOf('id="role-gate"'));
  const tip = raw.replace(/<!--[\s\S]*?-->/g, "");
  assert.match(tip, /Tu iPad est&aacute; bien/, "it no longer says the iPad is fine — a crossed-out button reads as broken");
  assert.match(tip, /no van a cambiar solas/, "it no longer says pages will stop turning, which is the actual consequence");
  assert.doesNotMatch(tip, /Ser Director/, "the tip teaches followers how to take the role");
  assert.doesNotMatch(tip, /Ir a Canto/, "the tip routes followers toward the role control");
});

test("¿Eres Braulio? opens the gate — it does not take the role", () => {
  // Braulio forgets the procedure, and the empty seat is exactly when he needs it; without this his
  // path is five taps. Phrased as a QUESTION it is self-filtering — a singer named María reads it
  // and thinks "no" — where a how-to line would have taught all six of them.
  //
  // It must remain a shortcut to the CHECK and not past it: the typed word and the red warning are
  // still the only way through, so this adds no authority whatsoever.
  const h = APP.slice(APP.indexOf('const braulio = document.getElementById("nodir-info-braulio")'));
  const line = h.slice(0, h.indexOf("\n", h.indexOf("addEventListener")));
  assert.match(line, /openRoleGate\(\)/, "it does not open the gate");
  assert.doesNotMatch(line, /postNativeBridge/, "it posts to the bridge directly, bypassing the typed-word gate");

  // The invariant the whole role design rests on, restated here because this file added a new path
  // toward the role: exactly ONE site may ask for it.
  assert.equal((APP.match(/type: "request-director"/g) || []).length, 1,
    "a second request-director site exists — every extra one is an ungated hole through #role-gate");
  // And the gate still gates: Confirmar starts disabled and only the word enables it.
  assert.match(APP, /roleGateConfirm\.disabled = roleGateInput\.value\.trim\(\)\.toLowerCase\(\) !== ROLE_GATE_WORD/,
    "the typed word no longer controls Confirmar");
});
