import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { computeChecks } = require("../web/src/lib/svSelftest.js");

const okDeps = (overrides = {}) => ({
  loadImage: async () => true,
  fetchImpl: async () => ({ ok: true }),
  pageUrl: "books/standard/pages/page-001.webp",
  totalPages: 371,
  relayBase: "https://example.test",
  relayRoom: "alvernia-staging",
  buildNumber: "374",
  cacheVersion: "abc123-deadbeef",
  isNativeFileMode: false,
  bridgeAvailable: false,
  timeoutMs: 200,
  ...overrides,
});

test("all-green on a healthy web device", async () => {
  const r = await computeChecks(okDeps());
  assert.equal(r.allOk, true);
  assert.equal(r.checks.length, 5);
  const relay = r.checks.find((c) => c.id === "relay");
  assert.match(relay.label, /alvernia-staging/); // the room is shown LOUDLY
  const bridge = r.checks.find((c) => c.id === "puente");
  assert.equal(bridge.applicable, false); // web: bridge is n/a, not red
});

test("page-1 load failure turns the card red", async () => {
  const r = await computeChecks(okDeps({ loadImage: async () => false }));
  assert.equal(r.allOk, false);
  assert.equal(r.checks.find((c) => c.id === "pagina1").ok, false);
});

test("relay unreachable (non-ok / throw / hang) turns the card red without hanging", async () => {
  for (const fetchImpl of [
    async () => ({ ok: false }),
    async () => { throw new Error("offline"); },
    () => new Promise(() => {}), // hang → timeout path
  ]) {
    const r = await computeChecks(okDeps({ fetchImpl }));
    assert.equal(r.allOk, false, "relay failure must fail the card");
    assert.equal(r.checks.find((c) => c.id === "relay").ok, false);
  }
});

test("bogus totalPages (0, NaN, float, string) turns the card red", async () => {
  for (const totalPages of [0, 1, NaN, 2.5, "371", undefined]) {
    const r = await computeChecks(okDeps({ totalPages }));
    assert.equal(r.checks.find((c) => c.id === "paginas").ok, false, `totalPages=${String(totalPages)}`);
  }
});

test("native: bridge becomes applicable — red when unavailable, green when up", async () => {
  const down = await computeChecks(okDeps({ isNativeFileMode: true, bridgeAvailable: false }));
  const bridgeDown = down.checks.find((c) => c.id === "puente");
  assert.equal(bridgeDown.applicable, true);
  assert.equal(bridgeDown.ok, false);
  assert.equal(down.allOk, false);

  const up = await computeChecks(okDeps({ isNativeFileMode: true, bridgeAvailable: true }));
  assert.equal(up.checks.find((c) => c.id === "puente").ok, true);
  assert.equal(up.allOk, true);
});

test("hostile/missing deps never throw — resolves with a red card", async () => {
  for (const deps of [undefined, null, {}, { loadImage: "nope", fetchImpl: 42, timeoutMs: 50 }]) {
    const r = await computeChecks(deps);
    assert.ok(Array.isArray(r.checks) && r.checks.length === 5);
    assert.equal(r.allOk, false);
  }
});

test("unreplaced __BUILD_NUMBER__ token displays as dev, not as the raw token", async () => {
  const r = await computeChecks(okDeps({ buildNumber: "__BUILD_NUMBER__" }));
  const v = r.checks.find((c) => c.id === "version");
  assert.equal(v.ok, true);
  assert.match(v.detail, /^vdev/);
});
