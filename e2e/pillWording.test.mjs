import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const CSS = fs.readFileSync("web/src/styles.css", "utf8");

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

test("taking over is named as a takeover", () => {
  const s = states();
  assert.match(s.following.action, /Tomar/, "the follower state does not say it takes control");
  assert.match(s.nobody.action, /Dirigir/, "the empty-seat state does not offer to direct");
  assert.match(s.directing.action, /Salir/, "the director state does not offer a way out");
});

test("the following action is dimmed, not hidden", () => {
  // Hidden is what made it inert. Dimmed keeps it from competing with the music while still
  // teaching that the pill is tappable.
  const rule = CSS.slice(CSS.indexOf(".director-mode-badge.is-following .director-mode-badge-exit"));
  const body = rule.slice(0, rule.indexOf("}") + 1);
  assert.ok(!/display:\s*none/.test(body), "the follower action is hidden again — the pill reads as inert");
  assert.match(body, /opacity/, "no dimming, so it competes with the music");
});
