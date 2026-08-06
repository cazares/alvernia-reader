import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");
const HTML = fs.readFileSync("web/src/index.html", "utf8");
const KEYS = fs.readFileSync("src/offlineBooks.ts", "utf8");

// ── The panic switches ────────────────────────────────────────────────────────
// SOFT_RESET_CODE and BOOK_FORCE_BAKED_CODE exist for the five minutes before Mass when something
// has already gone wrong. Reaching them required a memorised 9-digit number — in exactly the moment
// nobody can look anything up. These pin the button path without removing the codes.

test("both panic switches are reachable without a code", () => {
  assert.match(NATIVE, /case "request-force-baked":/, "no button path to the baked songbook");
  assert.match(NATIVE, /case "request-soft-reset":/, "no button path to the soft reset");
  assert.match(APP, /postNativeBridge\(\{ type: "request-force-baked" \}\)/);
  assert.match(APP, /postNativeBridge\(\{ type: "request-soft-reset" \}\)/);
});

test("the original numpad codes still work", () => {
  // The buttons are an ADDITIONAL route, not a replacement. Anyone who knows the codes keeps them.
  assert.match(NATIVE, /const SOFT_RESET_CODE = "\d+"/);
  assert.match(NATIVE, /const BOOK_FORCE_BAKED_CODE = "\d+"/);
  assert.match(NATIVE, /if \(code === SOFT_RESET_CODE\)/, "typed soft-reset path gone");
  assert.match(NATIVE, /if \(code === BOOK_FORCE_BAKED_CODE\)/, "typed force-baked path gone");
});

test("each panic button confirms before doing anything", () => {
  // A button is far easier to hit by accident than nine specific digits, so unlike the typed codes
  // these must ask first. Without this, a stray tap wipes a device's role mid-Mass.
  for (const kind of ["request-force-baked", "request-soft-reset"]) {
    const at = NATIVE.indexOf(`case "${kind}":`);
    assert.ok(at > 0, `${kind} missing`);
    const block = NATIVE.slice(at, at + 700);
    assert.match(block, /Alert\.alert\(/, `${kind} fires with no confirmation`);
    assert.match(block, /style: "cancel"/, `${kind} has no way out`);
  }
});

test("rescue controls never appear on the public web", () => {
  // signovivo.com has no mesh, no staged bundle and no crumb log — every one of these is dead there.
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  assert.match(open, /NATIVE_FILE_MODE \|\| hasNativeBridge\(\)/, "reveal not gated on the shell");
  assert.match(open, /rescueWrap\.classList\.toggle\("is-hidden"/, "rescue block never gated");
  assert.match(HTML, /class="song-jump-rescue is-hidden"/, "must ship hidden and be revealed by JS");
});

test("the rescue block re-collapses every time the modal opens", () => {
  // Otherwise it is left expanded from a previous visit and the once-a-year controls sit permanently
  // next to the one people use constantly.
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  assert.match(open, /rescueActions\.classList\.add\("is-hidden"\)/, "stays open between visits");
});

// ── Breadcrumbs ───────────────────────────────────────────────────────────────
// There is no internet inside the church and no MDM on these iPads, so the /log telemetry cannot
// reach the worker during Mass. This buffer is the ONLY record of what a device did.

test("breadcrumbs are a bounded ring buffer, not one overwritten value", () => {
  // It used to write a single key and overwrite it on every call: exactly one crumb survived, and
  // it was the last one, which is almost never the interesting one.
  assert.match(KEYS, /breadcrumbs: "sv\.diag\.breadcrumbs"/, "no storage key for the buffer");
  assert.match(NATIVE, /const BREADCRUMB_LIMIT = \d+/, "buffer is unbounded");
  const fn = NATIVE.slice(NATIVE.indexOf("const breadcrumb = useCallback"), NATIVE.indexOf("// Read back the previous session"));
  assert.match(fn, /next\.splice\(0, next\.length - BREADCRUMB_LIMIT\)/, "buffer never trims");
  assert.match(fn, /STORAGE_KEYS\.breadcrumbs/, "buffer never persisted");
});

test("a crumb logged before the restore cannot clobber the previous session", () => {
  // THE ORDERING HAZARD. The first crumb after a crash lands within milliseconds of boot — before
  // AsyncStorage has returned the old buffer. Persisting then would write a one-element array over
  // the history, destroying it at exactly the moment it matters.
  const fn = NATIVE.slice(NATIVE.indexOf("const breadcrumb = useCallback"), NATIVE.indexOf("// Read back the previous session"));
  assert.match(fn, /if \(breadcrumbsLoadedRef\.current\)/, "persists before the restore has run");
  const restore = NATIVE.slice(NATIVE.indexOf("// Read back the previous session"), NATIVE.indexOf("// ── Remote sync telemetry"));
  assert.match(restore, /\.\.\.prev/, "restore drops the previous session");
  assert.match(restore, /\.\.\.breadcrumbsRef\.current/, "restore drops crumbs logged during boot");
  assert.match(restore, /breadcrumbsLoadedRef\.current = true/, "persistence never re-enabled");
});

test("a corrupt or missing buffer does not break boot", () => {
  // A diagnostic must never be able to break the thing it is diagnosing.
  const restore = NATIVE.slice(NATIVE.indexOf("// Read back the previous session"), NATIVE.indexOf("// ── Remote sync telemetry"));
  assert.match(restore, /catch/, "no guard around JSON.parse of stored text");
  assert.match(restore, /Array\.isArray\(parsed\)/, "trusts whatever was stored");
});

test("diagnostics are served to the web, which renders them", () => {
  // Native captures and serves; the viewer is web so it can improve over the air.
  assert.match(NATIVE, /case "request-diagnostics":/, "no way to read the buffer back");
  assert.match(APP, /postNativeBridge\(\{ type: "request-diagnostics" \}\)/, "nothing asks for it");
  assert.match(APP, /payload\.type === "diagnostics"/, "web ignores the reply");
  assert.match(APP, /const showDiagnostics/, "no renderer");
});

test("the diagnostics dump is selectable so it can be copied", () => {
  // Its whole job is to be read aloud over the phone or pasted into a message afterwards.
  const css = fs.readFileSync("web/src/styles.css", "utf8");
  const block = css.slice(css.indexOf("#sv-diag pre"), css.indexOf("#sv-diag button"));
  assert.match(block, /user-select: text/, "the dump cannot be selected");
});

test("diagnostics answer which songbook the device holds, with no internet", () => {
  // The title-page stamp used to be the only offline way to tell which book an iPad was carrying
  // ("¿la suya dice agosto?"). Deleting it on 2026-08-05 removed that answer. This restores it from
  // the device's own resolved state, which — unlike ink on page 1 — cannot disagree with the book.
  const at = NATIVE.indexOf('case "request-diagnostics":');
  const block = NATIVE.slice(at, at + 800);
  assert.match(block, /book: activeBookVersionRef\.current/, "no book identity in the dump");
  assert.match(block, /pages: totalPagesRef\.current/, "no page count in the dump");
  assert.match(APP, /payload\.pages/, "the viewer drops the page count");
  assert.match(APP, /payload\.book/, "the viewer drops the book identity");
});

test("operator overlays render ABOVE the modal that opens them", () => {
  // The diagnostics request is fired from inside .song-jump-modal (z-index 200). At the z-index the
  // overlay originally had (80) it rendered BEHIND that modal — present in the DOM, invisible on
  // screen, with no clue why. The sync notice had the same defect at 69. Both are things the
  // operator must SEE; being silently covered is the failure class this whole session kept finding.
  const css = fs.readFileSync("web/src/styles.css", "utf8");
  const modalZ = Number(css.slice(css.indexOf(".song-jump-modal {")).match(/z-index:\s*(\d+)/)[1]);
  const diagZ = Number(css.slice(css.indexOf("#sv-diag {")).match(/z-index:\s*(\d+)/)[1]);
  assert.ok(diagZ > modalZ, `diagnostics z-index ${diagZ} is not above the modal's ${modalZ}`);
  // The notice's CSS is built by concatenating string literals, so the selector and its z-index sit
  // in different quoted chunks. Join them before matching or the pattern can never span the seam.
  const joined = APP.replace(/"\s*\+\s*"/g, "");
  const noticeMatch = joined.match(/#sv-sync-note\{[^"]*z-index:(\d+)/);
  assert.ok(noticeMatch, "could not find the sync notice z-index");
  const noticeZ = Number(noticeMatch[1]);
  assert.ok(noticeZ > modalZ, `sync notice z-index ${noticeZ} is not above the modal's ${modalZ}`);
});
