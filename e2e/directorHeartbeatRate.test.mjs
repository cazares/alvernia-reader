import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");

// WHY THIS FILE EXISTS. Miguel, 2026-08-18: measured multi-second lag on a real director handoff
// ("nobody directing" -> independent song changes on each device -> promote) and called it "very
// concerning" — speed was named a topmost priority. The director's mesh heartbeat (which ALSO
// fires BLE's publish() every tick — see DirectorSyncModule.swift's sendPageUpdate) went from
// 1000ms to 100ms after settling through 250/500ms. "ok then 100ms on both BLE and mesh it is
// then" is the final word.

test("the director heartbeat ticks at 100ms, not the old 1000ms", () => {
  const start = NATIVE.indexOf("const startDirectorHeartbeat = useCallback");
  const end = NATIVE.indexOf("relayHeartbeatRef.current = setInterval", start);
  const meshBlock = NATIVE.slice(start, end);
  assert.match(meshBlock, /}, 100\);/, "the mesh/BLE heartbeat interval is not 100ms");
  assert.doesNotMatch(meshBlock, /}, 1000\);/, "still ticking at the old 1000ms rate");
});

test("the AsyncStorage stamp write stays independently throttled at 20s regardless of tick rate", () => {
  // Tightening the tick 10x must not also multiply how often this real disk write happens.
  const start = NATIVE.indexOf("const startDirectorHeartbeat = useCallback");
  const end = NATIVE.indexOf("relayHeartbeatRef.current = setInterval", start);
  const meshBlock = NATIVE.slice(start, end);
  assert.match(meshBlock, /nowMs - lastDirectorAtWrittenRef\.current >= DIRECTOR_STAMP_THROTTLE_MS/,
    "the stamp write is no longer independently throttled — it would now write to disk 10x/sec");
});

test("the receiver is safe at any heartbeat frequency: renderPage no-ops on an unchanged page", () => {
  // This is what makes 100ms safe rather than reopening the 2026-08-06 crash (repeated heartbeat
  // work on an old iPad) that this exact guard was written to fix — verified here so a future
  // removal of the guard is caught before it ships alongside an even faster heartbeat.
  const idx = APP.indexOf("const renderPage = async (pageNumber");
  assert.ok(idx > 0, "renderPage moved or was renamed");
  const body = APP.slice(idx, idx + 2000);
  assert.match(body, /nextPage === state\.currentPage/, "the same-page early-return guard is gone");
  assert.match(body, /pageImageMatches\(nextPage\)/, "the guard no longer verifies the image itself matches");
});
