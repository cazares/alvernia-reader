import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");

// WHY THIS FILE EXISTS. Reported 2026-08-18, right after an OTA book swap + binary update put
// every device through a fresh WebView mount at once: "becoming director makes other devices go
// to page 1 still THEN target page." The mechanism: index.html's <img id="page-image"> is
// hardcoded to page-001.webp for the browser's very first paint, and native's own
// DEFAULT_START_PAGE render used to follow it — both painted and REVEALED before native's
// bridge-ready response (the only channel carrying the real page) had a chance to arrive, because
// that response was requested at the very END of initReader, after revealReader() already ran.
//
// The fix moves the bridge-ready request earlier and makes NATIVE (only) await the first real
// page — or a bounded timeout — before ever revealing. web-only boot is untouched: it already had
// its own (separate, pre-existing) relay-poll-before-reveal logic.
//
// Written as source-text assertions, same convention as the PdfReaderApp.tsx guards elsewhere in
// this suite — the logic runs inside a real WebView/DOM boot sequence that these tests cannot
// execute without one.

test("a native boot gate exists, keyed off hasNativeBridge()", () => {
  assert.match(APP, /const firstNativePageSignal = hasNativeBridge\(\)/,
    "firstNativePageSignal is missing or no longer tied to hasNativeBridge()");
  assert.match(APP, /let resolveFirstNativePageSignal = null;/,
    "the resolver variable is missing");
});

test("the gate is bounded — a brand-new install with nobody directing must still reveal", () => {
  const m = APP.match(/const FIRST_NATIVE_PAGE_TIMEOUT_MS = (\d+);/);
  assert.ok(m, "FIRST_NATIVE_PAGE_TIMEOUT_MS constant missing");
  const ms = Number(m[1]);
  assert.ok(ms > 0 && ms <= 5000, `FIRST_NATIVE_PAGE_TIMEOUT_MS=${ms}ms is not a sane bound`);
});

test("a CONFIRMED page sync-event resolves the gate", () => {
  const idx = APP.indexOf('event.type === "page" && Number.isFinite(event.page)');
  assert.ok(idx > 0, "the page sync-event handler moved or was removed");
  const body = APP.slice(idx, idx + 2200);
  assert.match(body, /resolveFirstNativePageSignal\(\)/,
    "the page sync-event handler no longer resolves the boot gate — a follower could reveal on the wrong page forever");
});

test("a CACHED (remembered, not live) page does NOT resolve the gate", () => {
  // 2026-08-18: a solo reader's remembered last page (src: "cache", PdfReaderApp.tsx) is a GUESS,
  // not proof anyone is directing right now. Miguel, testing on hardware: "it initially shows on
  // followers whatever song they WERE on, so off by one" — the cache satisfied the gate before a
  // real mesh/BLE sync could correct it, so a follower whose real director was on a DIFFERENT song
  // flashed the stale cached one first. isConfirmed must gate BOTH the reveal AND the "director is
  // alive" pill signal — a replayed memory is neither.
  const idx = APP.indexOf('event.type === "page" && Number.isFinite(event.page)');
  const body = APP.slice(idx, idx + 2200);
  assert.match(body, /event\.src !== "cache"/, "no isConfirmed / cache-source check — the gate resolves on any page, including a stale replay");
  const isConfirmedIdx = body.indexOf("isConfirmed");
  const resolveIdx = body.indexOf("resolveFirstNativePageSignal()");
  const guardedResolve = body.slice(0, resolveIdx).lastIndexOf("if (isConfirmed");
  assert.ok(guardedResolve > isConfirmedIdx, "resolveFirstNativePageSignal is not gated on isConfirmed");
  const guardedBadge = body.slice(0, body.indexOf("renderDirectorModeBadge()")).lastIndexOf("if (isConfirmed)");
  assert.ok(guardedBadge > isConfirmedIdx, "renderDirectorModeBadge/lastDirectorPageAt (the 'director is alive' signal) is not gated on isConfirmed — a cached replay would falsely claim a live director");
});

test("bridge-ready is requested BEFORE the reveal gate, not after", () => {
  // The bug: bridge-ready used to fire at the very END of initReader, so revealReader() had
  // already run by the time native's response (the only source of the real page) could arrive.
  const bridgeReadyIdx = APP.indexOf('type: "bridge-ready"');
  const revealIdx = APP.indexOf("revealReader();", APP.indexOf("const initReader"));
  assert.ok(bridgeReadyIdx > 0, "bridge-ready post is missing entirely");
  assert.ok(revealIdx > 0, "revealReader() call inside initReader is missing");
  assert.ok(bridgeReadyIdx < revealIdx,
    "bridge-ready is posted AFTER revealReader() — this is exactly the flash bug: native's " +
    "correction can never arrive before the wrong page is already on screen");
});

test("bridge-ready is posted exactly once per boot", () => {
  const count = (APP.match(/type: "bridge-ready"/g) || []).length;
  assert.equal(count, 1,
    `bridge-ready posted ${count} times — posting it twice re-runs native's bridge-ready handler ` +
    "twice per mount (AsyncStorage writes, a possible re-broadcast) for zero benefit");
});

test("native awaits the gate before revealing; web-only keeps its own separate relay-poll gate", () => {
  const initReaderIdx = APP.indexOf("const initReader = async () => {");
  const revealIdx = APP.indexOf("revealReader();", initReaderIdx);
  const gateSection = APP.slice(initReaderIdx, revealIdx);
  assert.match(gateSection, /hasNativeBridge\(\) \|\| NATIVE_FILE_MODE/,
    "the native/web branch before the reveal is missing or restructured unrecognizably");
  assert.match(gateSection, /await Promise\.race\(\s*\[\s*firstNativePageSignal/,
    "native no longer races firstNativePageSignal against the timeout before revealing");
  assert.match(gateSection, /relayPollOnce\(true\)/,
    "the web-only relay-poll-before-reveal path was removed — that's a separate, pre-existing gate");
});
