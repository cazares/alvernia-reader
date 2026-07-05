import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

// svRelayRoom.js is a UMD CommonJS-compatible module (no "type":"module" in the
// root package.json), so require() runs its IIFE and returns module.exports.
const require = createRequire(import.meta.url);
const { resolveRelayRoom, PRODUCTION_ROOM, STAGING_ROOM } = require("../web/src/lib/svRelayRoom.js");

test("defaults to the production room when there is no ?env", () => {
  assert.equal(resolveRelayRoom(""), "alvernia-main");
  assert.equal(resolveRelayRoom("?foo=bar"), "alvernia-main");
  assert.equal(resolveRelayRoom("?env=prod"), "alvernia-main");
  assert.equal(resolveRelayRoom("?env="), "alvernia-main");
  assert.equal(resolveRelayRoom("?practice=1"), "alvernia-main"); // practice mode is a later feature
});

test("returns the staging room ONLY for ?env=staging", () => {
  assert.equal(resolveRelayRoom("?env=staging"), "alvernia-staging");
  assert.equal(resolveRelayRoom("?a=1&env=staging&b=2"), "alvernia-staging");
  assert.equal(resolveRelayRoom("env=staging"), "alvernia-staging"); // leading ? optional for URLSearchParams
});

test("never throws and always returns a known room on hostile input", () => {
  for (const bad of [null, undefined, 12345, {}, [], NaN, Symbol.iterator]) {
    const room = resolveRelayRoom(bad);
    assert.ok(room === "alvernia-main" || room === "alvernia-staging", `unexpected room for ${String(bad)}: ${room}`);
  }
  // The default for all hostile input must be the SAFE production room.
  assert.equal(resolveRelayRoom(null), "alvernia-main");
  assert.equal(resolveRelayRoom(undefined), "alvernia-main");
});

test("exposes the room-name constants used across the app + worker", () => {
  assert.equal(PRODUCTION_ROOM, "alvernia-main");
  assert.equal(STAGING_ROOM, "alvernia-staging");
});
