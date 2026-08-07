import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");
const HTML = fs.readFileSync("web/src/index.html", "utf8");
const KEYS = fs.readFileSync("src/offlineBooks.ts", "utf8");

// Executes openSongJump's reveal decision with fakes, so a deleted gate changes the RESULT rather
// than merely removing a line some other assertion still matches.
const reveal = (nativeFileMode, hasBridge, role = "follower") => {
  const open = APP.slice(APP.indexOf("const openSongJump"), APP.indexOf("const closeSongJump"));
  const body = open.replace(/^[\s\S]*?=> \{/, "").replace(/\};?\s*$/, "")
    .replace(/songJumpModal\.classList\.remove\("is-hidden"\);/, "").replace(/state\.songJumpOpen = true;/, "");
  const calls = {};
  const el = (n) => ({ classList: { toggle: (_c, v) => { calls[n] = v; }, add: () => { calls[n + ":collapsed"] = true; } } });
  new Function("NATIVE_FILE_MODE", "hasNativeBridge", "state", "rescueWrap",
    "rescueActions", "rescueToggle", "songJumpModal", "clearDraft", body)(
    nativeFileMode, () => hasBridge, { nativeSyncRole: role }, el("rescue"),
    el("rescueActions"), { setAttribute() {} }, { classList: { remove() {} } }, () => {});
  return calls;
};

// ── The panic switches ────────────────────────────────────────────────────────
// SOFT_RESET_CODE and BOOK_FORCE_BAKED_CODE exist for the five minutes before Mass when something
// has already gone wrong. Reaching them required a memorised 9-digit number — in exactly the moment
// nobody can look anything up. These pin the button path without removing the codes.

test("the panic switches are reachable by CODE, and the bridge handlers survive", () => {
  // The "¿Algo anda mal?" block was removed from the IR A CANTO modal on 2026-08-06 — that dialog is
  // opened constantly during Mass and had grown a once-a-year maintenance drawer under it. The
  // numpad codes remain the way in, and the native handlers stay so any future entry point can use
  // them without a new binary.
  assert.match(NATIVE, /case "request-force-baked":/, "the bridge handler was deleted with the button");
  assert.match(NATIVE, /case "request-soft-reset":/, "the bridge handler was deleted with the button");
  assert.match(NATIVE, /if \(code === SOFT_RESET_CODE\)/, "the typed soft-reset path is gone too");
  assert.match(NATIVE, /if \(code === BOOK_FORCE_BAKED_CODE\)/, "the typed force-baked path is gone too");
  // ...and nothing in the web still posts them, or it would be a button that no longer exists.
  assert.ok(!/request-force-baked/.test(APP), "dead web caller for a removed button");
  assert.ok(!/request-soft-reset/.test(APP), "dead web caller for a removed button");
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
    const body = NATIVE.slice(at, NATIVE.indexOf("break;", at));
    assert.match(body, /Alert\.alert\(/, `${kind} fires with no confirmation`);
    assert.match(body, /style: "cancel"/, `${kind} has no way out`);
    // THE MUTATION THAT SLIPPED PAST BEFORE: `onDirectorCode(CODE); Alert.alert(…)` — the action
    // fires and the dialog appears afterwards as decoration. Every invocation must be inside an
    // onPress handler, so require it to appear ONLY there.
    const invocations = [...body.matchAll(/onDirectorCode\(/g)].length;
    const inOnPress = [...body.matchAll(/onPress: \(\) => onDirectorCode\(/g)].length;
    assert.equal(invocations, inOnPress, `${kind} calls onDirectorCode outside the confirm's onPress`);
    assert.equal(inOnPress, 1, `${kind} should confirm exactly one action`);
  }
});

// The two "rescue controls never appear on the public web" / "re-collapses on every open" tests
// were deleted with the block itself on 2026-08-06. Keeping them would have meant asserting the
// behaviour of markup that no longer exists — the shape of test that passes forever and proves
// nothing, which this file was rewritten once already to stop doing.

// ── Breadcrumbs ───────────────────────────────────────────────────────────────
// There is no internet inside the church and no MDM on these iPads, so the /log telemetry cannot
// reach the worker during Mass. This buffer is the ONLY record of what a device did.

test("breadcrumbs are a bounded ring buffer, not one overwritten value", () => {
  // It used to write a single key and overwrite it on every call: exactly one crumb survived, and
  // it was the last one, which is almost never the interesting one.
  assert.match(KEYS, /breadcrumbs: "sv\.diag\.breadcrumbs"/, "no storage key for the buffer");
  const LIMIT = Number(NATIVE.match(/const BREADCRUMB_LIMIT = (\d+)/)[1]);
  // BREADCRUMB_LIMIT = 1 restores the exact one-crumb bug the buffer replaced, and the old
  // assertion (`/const BREADCRUMB_LIMIT = \d+/`) waved it straight through.
  assert.ok(LIMIT >= 100, `BREADCRUMB_LIMIT ${LIMIT} cannot cover a Mass plus the boot before it`);
  // Execute the trim so a neutered splice cannot pass.
  const trim = NATIVE.match(/if \(next\.length > BREADCRUMB_LIMIT\) (next\.splice\([^;]+\));/)[1];
  const next = Array.from({ length: LIMIT + 25 }, (_, i) => `c${i}`);
  new Function("next", "BREADCRUMB_LIMIT", trim + ";")(next, LIMIT);
  assert.equal(next.length, LIMIT, "the ring buffer does not actually trim");
  assert.equal(next[next.length - 1], `c${LIMIT + 24}`, "trim dropped the NEWEST crumbs");
  assert.equal(next[0], "c25", "trim kept the wrong end of the buffer");
});

test("a crumb logged before the restore cannot clobber the previous session", () => {
  // THE ORDERING HAZARD. The first crumb after a crash lands within milliseconds of boot — before
  // AsyncStorage has returned the old buffer. Persisting then would write a one-element array over
  // the history, destroying it at exactly the moment it matters.
  const fn = NATIVE.slice(NATIVE.indexOf("const breadcrumb = useCallback"), NATIVE.indexOf("// Read back the previous session"));
  assert.match(fn, /if \(breadcrumbsLoadedRef\.current\)/, "persists before the restore has run");
  // Execute the merge: order matters and a swap (…current, marker, …prev) drops the NEW session's
  // crumbs, which the old presence checks accepted because both spreads were still present.
  const mergeExpr = NATIVE.match(/const merged = (\[[^;]+\]);/)[1];
  const merged = new Function("prev", "breadcrumbsRef", `return ${mergeExpr};`)(
    ["old1", "old2"], { current: ["new1"] },
  );
  assert.deepEqual(merged.filter((x) => !x.includes("app start")), ["old1", "old2", "new1"],
    "restore merge is out of order — the previous or current session is being dropped");
  assert.ok(merged.findIndex((x) => x.includes("app start")) === 2, "the boot marker is misplaced");
  const restore = NATIVE.slice(NATIVE.indexOf("// Read back the previous session"), NATIVE.indexOf("// ── Remote sync telemetry"));
  assert.match(restore, /breadcrumbsLoadedRef\.current = true/, "persistence never re-enabled");
});

test("a corrupt or missing buffer does not break boot", () => {
  // A diagnostic must never be able to break the thing it is diagnosing.
  const restore = NATIVE.slice(NATIVE.indexOf("// Read back the previous session"), NATIVE.indexOf("// ── Remote sync telemetry"));
  assert.match(restore, /catch/, "no guard around JSON.parse of stored text");
  assert.match(restore, /Array\.isArray\(parsed\)/, "trusts whatever was stored");
});

test("the diagnostics plumbing survives, even though nothing invokes it yet", () => {
  // ⚠️ REACHABILITY GAP, knowingly accepted. "Ver diagnóstico" lived in the removed block, so the
  // crumb log currently has NO entry point in the UI. The capture, the bridge handler and the
  // renderer are all intact, so restoring a way in is a web-only change — but as of 2026-08-06
  // nobody can open it on a device.
  assert.match(NATIVE, /case "request-diagnostics":/, "native can no longer serve the buffer");
  assert.match(APP, /payload\.type === "diagnostics"/, "the web could not render a reply");
  assert.match(APP, /const showDiagnostics/, "the renderer was deleted — restoring access needs a rewrite");
  assert.ok(!/postNativeBridge\(\{ type: "request-diagnostics" \}\)/.test(APP),
    "something DOES ask for diagnostics now — update this test, the gap is closed");
});

test("the diagnostics dump is selectable so it can be copied", () => {
  // Its whole job is to be read aloud over the phone or pasted into a message afterwards.
  const css = fs.readFileSync("web/src/styles.css", "utf8");
  const block = css.slice(css.indexOf("#sv-diag pre"), css.indexOf("#sv-diag button"));
  assert.match(block, /-webkit-user-select: text/, "WKWebView honours the PREFIXED property; unprefixed alone does nothing on the iPads");
  assert.match(block, /(?<!-)user-select: text/, "unprefixed user-select missing");
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
