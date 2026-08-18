import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const WORKER = fs.readFileSync("sync-worker/src/index.ts", "utf8");

// WHY THIS FILE EXISTS. "kill the fleet dashboard" (2026-08-18) deleted /fleet/checkin wholesale.
// That was the RIGHT call for the dashboard itself — but OTA arming was piggybacked on that same
// call's response ("no new endpoint, no new poll"), so the deletion also silently removed the only
// way a device ever learns BOOK_UPDATE_VERSION exists, and the client-side signal (lastCheckinOkAt)
// canApplyNow's safety gate depends on. Caught before merge, not after — see the git history around
// 96995e1 for the original mechanism this restores, narrower.

test("the worker has its OWN narrow OTA route, never touching the deleted fleet store", () => {
  assert.match(WORKER, /if \(url\.pathname === "\/ota\/checkin"\)/, "no OTA route exists");
  const route = WORKER.slice(WORKER.indexOf('if (url.pathname === "/ota/checkin")'));
  const body = route.slice(0, route.indexOf('if (url.pathname === "/log")'));
  assert.match(body, /request\.method !== "POST"/, "the route accepts more than POST");
  assert.match(body, /getByName\("__ota__"\)/, "the route reads/writes the deleted fleet DO instance");
  assert.doesNotMatch(body, /getByName\("__fleet__"\)/, "the route touches the deleted fleet store");
  assert.match(body, /decideBookUpdate\(/, "the route never computes an arming decision");
});

test("otaCheckin stores ONLY deviceId/bookVersion/bookStage — no path back to PII", () => {
  const fn = WORKER.slice(WORKER.indexOf("async otaCheckin(input"));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  for (const forbidden of ["label", "role", "surface", "deviceKind", "nativeBuild", "webCached"]) {
    assert.doesNotMatch(body, new RegExp(`o\\.${forbidden}\\b`),
      `otaCheckin reads "${forbidden}" — that is exactly the PII-adjacent field the dashboard stored`);
  }
  assert.match(body, /entry\.bookVersion/, "bookVersion is not recorded — decideBookUpdate needs it");
  assert.match(body, /entry\.bookStage/, "bookStage is not recorded — the fleet-wide throttle needs it");
});

test("/ota/checkin is metered as non-essential, like /log", () => {
  const gate = WORKER.slice(WORKER.indexOf("const NON_ESSENTIAL ="));
  const line = gate.slice(0, gate.indexOf(";"));
  assert.match(line, /\/ota\/checkin/, "OTA check-ins are not counted against the reserved budget");
});

test("the client checks in unconditionally — boot, a slow interval, and foreground", () => {
  assert.match(NATIVE, /const otaCheckin = useCallback/, "no client-side otaCheckin exists");

  // Three trigger sites, none gated on page/song state — Miguel: "make sure if devices are
  // stationary on say page 1 or page N that the app will still auto-update". A device that never
  // turns a page must still check in.
  const boot = NATIVE.slice(NATIVE.indexOf('dbgLog("boot", { syncAvailable });'), NATIVE.indexOf('}, [dbgLog, syncAvailable, otaCheckin]);'));
  assert.match(boot, /otaCheckin\(\);/, "boot no longer checks in for an update");

  const intervalBlock = NATIVE.slice(NATIVE.indexOf("const OTA_CHECKIN_INTERVAL_MS"));
  const intervalMs = Number(intervalBlock.match(/OTA_CHECKIN_INTERVAL_MS = (\d+) \* (\d+) \* (\d+);/)?.slice(1).reduce((a, b) => a * Number(b), 1));
  assert.ok(intervalMs > 0, "the interval constant could not be parsed");
  assert.ok(intervalMs < 90 * 1000 * 10, "the interval is absurdly long — a real arming test would time out waiting");
  assert.ok(intervalMs >= 60 * 1000, "the interval is back near the old 90s dashboard cadence — that cost 3,840 req/day");
  assert.match(intervalBlock.slice(0, 300), /setInterval\(otaCheckin,/, "the interval does not call otaCheckin");

  const fg = NATIVE.slice(NATIVE.indexOf("void autoApplyIfSafeRef.current?.();"), NATIVE.indexOf("if (syncAvailable) refreshNearbyDiscovery"));
  assert.match(fg, /otaCheckin\(\);/, "foregrounding the app no longer checks in for an update");
});

test("otaCheckin restores BOTH broken signals: lastCheckinOkAt and the staging pipeline trigger", () => {
  const fn = NATIVE.slice(NATIVE.indexOf("const otaCheckin = useCallback"), NATIVE.indexOf("// Stable per-install device id"));
  assert.match(fn, /lastCheckinOkAtRef\.current = at/, "the live-internet-proof signal is not restored");
  assert.match(fn, /AsyncStorage\.setItem\(STORAGE_KEYS\.lastCheckinOkAt/, "lastCheckinOkAt is not persisted");
  assert.match(fn, /onCheckinResponseRef\.current\?\.\(body\)/, "the response is never fed into the staging pipeline");
  // Only these two fields go out — no label/role, matching the server-side contract above.
  assert.doesNotMatch(fn, /label:|role:|deviceKind:/, "the client sends fleet-dashboard fields again");
});

test("a solo reader's page survives an OTA reload; a director or live follower is untouched", () => {
  // The narrower finding: director and actively-following-a-live-director were ALREADY correct
  // (currentPageRef lives in native memory and survives a WebView-only remount; a follower resyncs
  // to the director's live snapshot). The gap was only the THIRD case — nobody to be authoritative
  // for a solo reader — where the saved page was written and never read.
  const start = NATIVE.indexOf('case "bridge-ready"');
  const end = NATIVE.indexOf('case "page-changed"');
  const block = NATIVE.slice(start, end);

  const soloStart = block.indexOf("} else {", block.indexOf("SOLO READER") - 20);
  assert.ok(block.includes("SOLO READER"), "the solo-reader restore is gone");
  const solo = block.slice(block.indexOf("} else {", block.lastIndexOf("if (syncAvailable) requestCurrentSnapshot")));

  assert.match(solo, /AsyncStorage\.getItem\(\s*`\$\{STORAGE_KEYS\.lastPagePrefix\}\$\{currentBookRef\.current\}`/,
    "the solo-reader branch does not read the page performApplySwap saved");
  assert.match(solo, /Number\.isFinite\(page\) && page >= 1/, "no validation on the restored page — a corrupt value would be trusted");
  assert.match(solo, /type: "sync-event"/, "the restored page is never re-driven into the web");

  // Must be the FALLTHROUGH else, not reachable from the two already-correct branches.
  const isDirectorBranch = block.slice(block.indexOf("if (isDirectorAuthority)"), block.indexOf("} else if (roleRef.current === \"follower\""));
  assert.doesNotMatch(isDirectorBranch, /lastPagePrefix/, "the director branch also reads the saved page — redundant and could race the live re-assert");
});

test("performApplySwap still saves the page this restore depends on", () => {
  // The save half of this was never broken — confirms the fix wires into EXISTING behaviour rather
  // than needing a second save site.
  const fn = NATIVE.slice(NATIVE.indexOf("const performApplySwap = useCallback"));
  const body = fn.slice(0, fn.indexOf("[breadcrumb, bookFs, setBookStage]"));
  assert.match(body, /lastPagePrefix\}\$\{currentBookRef\.current\}`,\s*\n\s*String\(currentPageRef\.current\)/,
    "performApplySwap no longer saves the reader's place before swapping");
});
