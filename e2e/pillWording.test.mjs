import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const CSS = fs.readFileSync("web/src/styles.css", "utf8");

// EXECUTE the pill's render decision. A presence check for `NATIVE_FILE_MODE || hasNativeBridge()`
// passes even when the early return that USES it is deleted — the const stays behind and satisfies
// the regex while the pill renders on signovivo.com anyway. Measured: that mutation slipped past.
const renderPill = (nativeFileMode, hasBridge, role, meshStatus = "", lastPageAgoMs = 1e9) => {
  const start = APP.indexOf("const SYNC_PILL = {");
  const end = APP.indexOf("// ── Sync \"working\"");
  const body = APP.slice(start, end);
  let hidden = null;
  const titleEl = { textContent: "" }, actionEl = { textContent: "" };
  const badge = {
    classList: {
      remove: (...c) => { if (c.includes("is-hidden")) hidden = false; },
      add: (c) => { if (c === "is-hidden") hidden = true; },
    },
    setAttribute() {},
  };
  new Function("NATIVE_FILE_MODE", "hasNativeBridge", "state", "directorModeBadge", "document", "Date",
    `${body}; renderDirectorModeBadge();`,
  )(nativeFileMode, () => hasBridge,
    { nativeSyncRole: role },
    badge,
    { documentElement: { dataset: {} }, getElementById: (id) => {
        if (id === "sync-pill-title") return titleEl;
        if (id === "role-toggle") return { classList: { toggle() {} } };
        if (id === "role-toggle-label") return { textContent: "" };
        return actionEl;
      } },
    { now: () => lastPageAgoMs });
  return { hidden, title: titleEl.textContent, action: actionEl.textContent };
};

test("the pill never renders on the public web", () => {
  // No mesh there, no role to take — it would describe a room the viewer is not in.
  assert.equal(renderPill(false, false, "follower").hidden, true, "the pill RENDERED on signovivo.com");
  assert.equal(renderPill(true, false, "follower").hidden, false, "the pill is hidden inside the shell");
  assert.equal(renderPill(false, true, "follower").hidden, false, "the pill is hidden when the bridge exists");
});

// Read the rendered strings only — the surrounding comments deliberately DISCUSS the words we
// rejected, so grepping the whole file for them reports a leak that isn't there.
const states = () => {
  const start = APP.indexOf("const SYNC_PILL = {");
  const block = APP.slice(start, APP.indexOf("};", start));
  const out = {};
  for (const m of block.matchAll(/(\w+):\s*\{[^}]*title: "([^"]*)"[^}]*action: "([^"]*)"/g)) {
    out[m[1]] = { title: m[2], action: m[3] };
  }
  return out;
};

test("all three states are present; the pill reports STATE, it no longer takes the role", () => {
  // WHAT CHANGED (build 435). The pill used to BE the role control, so every state had to carry an
  // action or it looked inert. The role now lives behind the IR A CANTO modal (★ Dirigir), because
  // a permanently visible become-director button on six choir iPads is a split-brain generator and
  // the choir is non-technical. So the pill is now a readout, and a follower is a bare dot.
  //
  // "SIGUIENDO" was removed on purpose: it was a CLAIM, and it lied — two iPads sat stranded on
  // song 59 for minutes still reading SIGUIENDO. A dot is just a light.
  const s = states();
  assert.deepEqual(Object.keys(s).sort(), ["directing", "following", "nobody"]);
  for (const [name, v] of Object.entries(s)) {
    assert.ok(v.title.length > 0, `${name} has no title`);
  }
  // The one state that still earns WORDS is "nobody is driving" — the moment a singer must notice.
  assert.match(s.nobody.title, /nadie/i, "the no-director state must still say so in words");
  assert.equal(s.following.action, "", "following must be a bare dot — no action, no claim");
});

test("no state promises an approval that does not exist", () => {
  // Tapping makes you director immediately. Nothing grants it and the current director is never
  // asked, so "Pedir"/"Solicitar"/"request" would have someone waiting for a reply that never comes.
  for (const [name, v] of Object.entries(states())) {
    const text = `${v.title} ${v.action}`;
    assert.ok(!/pedir|solicit|request/i.test(text),
      `${name} uses approval language ("${text}") for something nothing approves`);
  }
});

test("no label promises a dialog the app may not show", () => {
  // THE MISTAKE THIS REPLACES. An earlier version asserted following → "Tomar el control", matching
  // the takeover button. But which confirm appears is decided NATIVELY at tap time by `liveDirector`
  // — an 8s window on lastDirectorSnapshotRef — while the pill's SIGUIENDO uses a 15s window on mesh
  // page events, deliberately longer so a radio hiccup does not flash NADIE DIRIGE mid-hymn.
  //
  // Between 8s and 15s the two disagree and the dialog reads "¿Dirigir el coro?", so the label was
  // wrong exactly when someone was reading it. A label may only claim what is invariant.
  const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const pillWindow = Number(APP.match(/const DIRECTOR_SEEN_WINDOW_MS = (\d+)/)[1]);
  const dialogWindow = Number(NATIVE.match(/const LIVE_DIRECTOR_WINDOW_MS = (\d+)/)[1]);
  assert.notEqual(pillWindow, dialogWindow,
    "the windows now agree — if that is deliberate, a label MAY name the dialog; re-read this test");
  for (const [name, v] of Object.entries(states())) {
    assert.ok(!/Tomar el control/.test(v.action),
      `${name} names the takeover button, which only appears inside a ${dialogWindow}ms window`);
  }
});

test("the role control names the role, and is not a bare verb", () => {
  // These two concerns followed the control to its new home rather than being deleted: an earlier
  // label "Tomar" failed because it was a bare verb ("take WHAT?"), and both paths to directing must
  // describe the same destination. The control is now #role-toggle inside the IR A CANTO modal.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  assert.match(HTML, /id="role-toggle"/, "the role control is gone");
  // Sentence case, not caps: all-caps is fine for a one-time alarm, distracting on a label you live
  // with. The two labels are set in app.js.
  assert.match(APP, /"Salir de director"\s*:\s*"Dirigir"/,
    "the role control must read Dirigir / Salir de director");
  // Naming the ROLE is what "Tomar" lacked — both labels do.
  for (const label of ["Dirigir", "Salir de director"]) {
    assert.ok(/dirig|director/i.test(label), `${label} does not name the role`);
  }
});

test("taking the role is gated on a typed word, stepping down is not", () => {
  // Asymmetric friction, matched to consequence. Taking the role changes the page for EVERY device
  // in the loft; stepping down is recoverable. The word is "braulio", not a real director code:
  // everyone watches Braulio type his code, so secrecy was never the threat model — carelessness is.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  assert.match(HTML, /id="role-gate"/, "the take-the-role gate is gone");
  assert.match(HTML, /ESTO CAMBIA LA P/, "the gate no longer warns that it changes everyone's page");
  assert.match(APP, /ROLE_GATE_WORD = "braulio"/, "the typed-word gate is gone");
  assert.match(APP, /roleGateConfirm\.disabled =/, "Confirmar must stay disabled until the word matches");
  assert.match(APP, /¿Salir de director\?/, "stepping down must still confirm, plainly");
});

test("LEGACY (pre-435): both ways of taking the role named the same outcome", { skip: "the pill no longer takes the role — see the two tests above" }, () => {
  // Following and nobody differ in SITUATION, not in result: either way you end up directing. The
  // title (SIGUIENDO vs NADIE DIRIGE) and the tint carry the difference; the action states the
  // destination, which never varies.
  const s = states();
  const plain = (a) => a.replace(/[✕▶]\s*/, "").trim();
  assert.equal(plain(s.following.action), plain(s.nobody.action),
    "the two paths to directing describe different outcomes, but the outcome is identical");
  // Must name the ROLE, not just a verb — that is what "Tomar" lacked.
  assert.match(plain(s.nobody.action), /director/i, "the action does not name the role being taken");
});

test("LEGACY (pre-435): no pill label is a bare verb", { skip: "the pill no longer carries an action — see 'the role control names the role'" }, () => {
  // The specific failure being guarded: "Tomar" reads as "take" with nothing taken.
  for (const [name, v] of Object.entries(states())) {
    const plain = v.action.replace(/[✕▶]\s*/, "").trim();
    assert.ok(plain.split(/\s+/).length >= 2 || /Salir/.test(plain),
      `${name} action "${plain}" is a bare verb — say what it acts on`);
  }
});

test("the following action is dimmed, not hidden", () => {
  // Hidden is what made it inert. Dimmed keeps it from competing with the music while still
  // teaching that the pill is tappable.
  const rule = CSS.slice(CSS.indexOf(".director-mode-badge.is-following .director-mode-badge-exit"));
  const body = rule.slice(0, rule.indexOf("}") + 1);
  assert.ok(!/display:\s*none/.test(body), "the follower action is hidden again — the pill reads as inert");
  assert.match(body, /opacity/, "no dimming, so it competes with the music");
});
