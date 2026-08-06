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
    { documentElement: { dataset: {} }, getElementById: (id) => (id === "sync-pill-title" ? titleEl : actionEl) },
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

test("all three states are present and each carries an action", () => {
  const s = states();
  assert.deepEqual(Object.keys(s).sort(), ["directing", "following", "nobody"]);
  for (const [name, v] of Object.entries(s)) {
    assert.ok(v.title.length > 0, `${name} has no title`);
    // Following used to render a blank action, which made the pill look inert — so nobody would
    // learn the seat can be taken without being told out loud, and the moment that matters is the
    // one where the person who knows is not in the room.
    assert.ok(v.action.length > 0, `${name} renders no action, so the pill looks unclickable`);
  }
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

test("both ways of taking the role name the same outcome", () => {
  // Following and nobody differ in SITUATION, not in result: either way you end up directing. The
  // title (SIGUIENDO vs NADIE DIRIGE) and the tint carry the difference; the action states the
  // destination, which never varies.
  const s = states();
  const plain = (a) => a.replace(/[✕▶]\s*/, "").trim();
  assert.equal(plain(s.following.action), plain(s.nobody.action),
    "the two paths to directing describe different outcomes, but the outcome is identical");
  assert.match(plain(s.nobody.action), /coro/i, "the action does not say what is being directed");
});

test("no label is a bare verb with no object", () => {
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
