import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");

// WHY THIS FILE EXISTS. A boot notice read "Estabas dirigiendo. Toca el estado arriba a la izquierda
// para volver a dirigir." The top-left status stopped taking the role in build 435 and the sentence
// was never updated, so for weeks it sent whoever read it to the wrong control — at the one moment
// they were already flustered, having just had the app die on them mid-Mass. On 2026-08-18 the way
// in moved to the top-RIGHT, making it wronger still.
//
// The lesson is not "keep the text in sync". It is that a notice describing where a control lives is
// a promise about the layout, and this layout moves. Notices carry the control instead.

// Toast strings only — the surrounding comments deliberately quote the old wording.
const toastTexts = () => [...NATIVE.matchAll(/type:\s*"toast",\s*\n\s*text:\s*"([^"]+)"/g)].map((m) => m[1]);

test("no notice tells anyone where a control is", () => {
  const texts = toastTexts();
  assert.ok(texts.length >= 2, `found ${texts.length} toast strings — the matcher drifted`);
  // Directional words are the tell. Any of them means the sentence is describing the layout.
  const DIRECTIONAL = /\b(arriba|abajo|izquierda|derecha|esquina|superior|inferior)\b/i;
  for (const t of texts) {
    assert.doesNotMatch(t, DIRECTIONAL,
      `a notice describes a position: "${t}" — carry the control instead, or it rots when the layout moves`);
  }
});

test("the resume notice carries a button, and that button opens the GATE", () => {
  // Losing the role unexpectedly is the moment someone most needs one tap, not a scavenger hunt.
  assert.match(NATIVE, /text: "Estabas dirigiendo cuando se cerró la app\.",\s*\n\s*action: "resume-director"/,
    "the crash-resume notice no longer carries its action");
  assert.match(NATIVE, /text: "Estabas transmitiendo cuando se cerró la app\.",\s*\n\s*action: "resume-director"/,
    "the relay-transmitter notice no longer carries its action");

  // It must open the gate, NOT take the role: the typed word is the only thing standing between a
  // flustered tap and a second director. request-director keeps exactly one call site.
  const handler = APP.slice(APP.indexOf('if (payload.type === "toast")'), APP.indexOf('if (payload.type === "diagnostics")'));
  assert.match(handler, /resume-director/, "the toast action is unwired");
  assert.match(handler, /requestRoleGate\(\)/, "the action does not open the gate");
  assert.doesNotMatch(handler, /postNativeBridge/, "the toast posts to the bridge directly, bypassing the gate");
  assert.equal((APP.match(/type: "request-director"/g) || []).length, 1,
    "a second request-director site exists — every extra one is an ungated hole through #role-gate");
});

test("a device still boots as a FOLLOWER — the notice informs, it does not promote", () => {
  // The seat stays empty until a hand takes it. Auto-promotion on a persisted role was tried between
  // 2026-08-05 and build 427 and each version could mint a second director beside a human's and win.
  // An empty seat is the choir on the last page; two directors is the choir split.
  assert.match(NATIVE, /\.finally\(\(\) => \{\s*\n\s*becomeFollower\(\);/,
    "the boot path no longer forces follower — a persisted role could promote this device");
  const boot = NATIVE.slice(NATIVE.indexOf("if (!didBootstrapRef.current)"), NATIVE.indexOf("const sub = addNearbyDirectorSyncListener"));
  assert.doesNotMatch(boot, /becomeDirector\(/, "the boot path can promote to director without a human");
});
