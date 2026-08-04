// The resolveBundleUri decision table (src/bookResolve.js) — defect D1.
//
// This is the logic that decides which songbook eight iPads boot into, in a building with no
// internet and no remedy. Every row is pinned here, including the ones that must REFUSE the
// downloaded copy, because the failure this replaces was silent: the device rendered an old book
// while reporting itself current, and nothing in the app could see it.
import test from "node:test";
import assert from "node:assert/strict";
import {
  decideBundle,
  isQuarantined,
  recordBundleFailure,
  clearBundleFailures,
  isValidManifest,
  nextHealAction,
  MAX_BOOT_ATTEMPTS,
  QUARANTINE_FAILURE_LIMIT,
} from "../src/bookResolve.js";

const man = (over = {}) => ({
  schema: 1,
  bookVersion: "bv_aaaaaaaaaaaaaaaa",
  totalPages: 373,
  builtFromShellBuild: 384,
  ...over,
});

const baseCtx = (over = {}) => ({
  docExists: true,
  docManifest: man({ bookVersion: "bv_bbbbbbbbbbbbbbbb", builtFromShellBuild: 390 }),
  bakedManifest: man(),
  bakedExists: true,
  forceBundled: false,
  bootAttempts: 0,
  bootProved: true,
  quarantine: [],
  ...over,
});

// ── The D1 retirement: the whole installed base, on first launch ─────────────

test("D1: a Documents bundle with NO manifest is refused — this evicts every mesh-pushed copy in the field", () => {
  // Every Documents/WebBundle that exists today was written by the Multipeer push, which predates
  // manifests. One rule retires the trap across the entire fleet with no migration code.
  const d = decideBundle(baseCtx({ docManifest: null }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "doc-manifest-missing-or-invalid");
});

test("D1: a Documents manifest that is unparseable garbage is refused", () => {
  for (const bad of [{}, { bookVersion: "nope" }, { bookVersion: "bv_zzzz" }, "string", 42, []]) {
    assert.equal(decideBundle(baseCtx({ docManifest: bad })).source, "bundled", `accepted ${JSON.stringify(bad)}`);
  }
});

test("D1: a bookVersion of the wrong SHAPE is not a manifest", () => {
  assert.equal(isValidManifest(man()), true);
  assert.equal(isValidManifest(man({ bookVersion: "bv_TOOSHORT" })), false);
  assert.equal(isValidManifest(man({ bookVersion: "bv_AAAAAAAAAAAAAAAA" })), false); // uppercase hex
  assert.equal(isValidManifest(man({ totalPages: 0 })), false);
});

// ── Prefer known-good over newer ─────────────────────────────────────────────

test("a TestFlight install always wins: baked shell >= documents shell boots the baked copy", () => {
  const d = decideBundle(baseCtx({
    docManifest: man({ bookVersion: "bv_cccccccccccccccc", builtFromShellBuild: 380 }),
    bakedManifest: man({ builtFromShellBuild: 384 }),
  }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "baked-is-newer-or-equal-shell");
});

// THE EQUAL CASE IS LOAD-BEARING, not an oversight. It was changed to tie-break on generatedAt on
// 2026-08-04 and reverted the same night: the justification was that a songbook-only deploy would
// produce doc == baked, but `SKIP_NATIVE=1 scripts/release.sh` still bumps (scripts/bump-build.mjs),
// so a web-only update always arrives with doc > baked. What actually produces the equal case is two
// tabs releasing at once — which shipped two different books both numbered v391 on 2026-08-03 — and
// there the code-signed copy is precisely the one to trust.
test("equal shell builds also prefer the code-signed copy", () => {
  const d = decideBundle(baseCtx({
    docManifest: man({
      bookVersion: "bv_cccccccccccccccc", builtFromShellBuild: 384,
      generatedAt: "2026-09-01T00:00:00.000Z", // a NEWER stamp must not win the tie
    }),
    bakedManifest: man({ builtFromShellBuild: 384, generatedAt: "2026-08-04T00:00:00.000Z" }),
  }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "baked-is-newer-or-equal-shell");
});

test("the shell catching up makes the Documents copy redundant", () => {
  const same = "bv_dddddddddddddddd";
  const d = decideBundle(baseCtx({
    docManifest: man({ bookVersion: same, builtFromShellBuild: 999 }),
    bakedManifest: man({ bookVersion: same, builtFromShellBuild: 384 }),
  }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "shell-caught-up");
});

test("THE ONLY path that boots a downloaded bundle: identified, newer shell, not quarantined", () => {
  const d = decideBundle(baseCtx({
    docManifest: man({ bookVersion: "bv_eeeeeeeeeeeeeeee", builtFromShellBuild: 390 }),
    bakedManifest: man({ builtFromShellBuild: 384 }),
  }));
  assert.equal(d.source, "documents");
  assert.equal(d.reason, "documents-newer");
});

// ── The crash nets ───────────────────────────────────────────────────────────

test("hard-crash net: two unproven boots quarantine the Documents copy and fall to baked", () => {
  const d = decideBundle(baseCtx({ bootProved: false, bootAttempts: MAX_BOOT_ATTEMPTS }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "boot-attempts-exhausted");
  assert.equal(d.quarantineDoc, true, "must quarantine — otherwise it retries the killer bundle forever");
});

test("a PROVED boot never trips the crash net, however high the attempt count", () => {
  const d = decideBundle(baseCtx({ bootProved: true, bootAttempts: 99 }));
  assert.equal(d.source, "documents");
});

test("one unproven attempt is not yet a crash — remount is still the cheaper hypothesis", () => {
  const d = decideBundle(baseCtx({ bootProved: false, bootAttempts: 1 }));
  assert.equal(d.source, "documents");
});

test("the operator panic switch outranks every rule below it", () => {
  const d = decideBundle(baseCtx({ forceBundled: true }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "force-bundled");
});

test("a quarantined book is refused even when it looks newer", () => {
  const bad = "bv_ffffffffffffffff";
  const d = decideBundle(baseCtx({
    docManifest: man({ bookVersion: bad, builtFromShellBuild: 999 }),
    quarantine: [{ bookVersion: bad, failures: QUARANTINE_FAILURE_LIMIT, lastFailureAt: 1 }],
  }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "quarantined");
});

// ── The floor itself ─────────────────────────────────────────────────────────

test("a missing code-signed bundle is reported, never returned as a URI to nothing", () => {
  // The old resolver returned the baked path WITHOUT stat'ing it, so a release that failed to copy
  // ios/WebBundle produced a black rectangle with no floor at all.
  const d = decideBundle(baseCtx({ bakedExists: false, docExists: false }));
  assert.equal(d.source, "none");
  assert.equal(d.reason, "no-bundle-anywhere");
});

test("no baked bundle but a usable Documents copy: boot it rather than nothing", () => {
  const d = decideBundle(baseCtx({ bakedExists: false }));
  assert.equal(d.source, "documents");
  assert.equal(d.reason, "no-baked-bundle");
});

test("no baked bundle and the only Documents copy is quarantined: surface it, do not boot known-bad", () => {
  const bad = "bv_ffffffffffffffff";
  const d = decideBundle(baseCtx({
    bakedExists: false,
    docManifest: man({ bookVersion: bad }),
    quarantine: [{ bookVersion: bad, failures: 5, lastFailureAt: 1 }],
  }));
  assert.equal(d.source, "none");
});

test("no Documents bundle at all is the ordinary fresh-install path", () => {
  const d = decideBundle(baseCtx({ docExists: false, docManifest: null }));
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "no-documents-bundle");
});

test("an empty/garbage context never throws and never invents a source", () => {
  for (const ctx of [undefined, null, {}, { docExists: true }]) {
    const d = decideBundle(ctx);
    assert.ok(["documents", "bundled", "none"].includes(d.source), `bad source for ${JSON.stringify(ctx)}`);
  }
});

// ── Quarantine is a counter, not a tombstone (red team NI5) ──────────────────

test("one failure does NOT blacklist — a transient jetsam must not cost the choir its book", () => {
  let q = recordBundleFailure([], "bv_1111111111111111", 1000);
  assert.equal(isQuarantined(q, "bv_1111111111111111"), false);
  q = recordBundleFailure(q, "bv_1111111111111111", 2000);
  assert.equal(isQuarantined(q, "bv_1111111111111111"), false, "two is still not enough");
  q = recordBundleFailure(q, "bv_1111111111111111", 3000);
  assert.equal(isQuarantined(q, "bv_1111111111111111"), true, `blacklists at ${QUARANTINE_FAILURE_LIMIT}`);
});

test("a proved boot clears the counter, so failures must be CONSECUTIVE to accumulate", () => {
  let q = recordBundleFailure([], "bv_2222222222222222", 1);
  q = recordBundleFailure(q, "bv_2222222222222222", 2);
  q = clearBundleFailures(q, "bv_2222222222222222");
  q = recordBundleFailure(q, "bv_2222222222222222", 3);
  assert.equal(isQuarantined(q, "bv_2222222222222222"), false, "the clear reset it to one");
});

test("failures are tracked per bookVersion, never globally", () => {
  let q = recordBundleFailure([], "bv_3333333333333333", 1);
  q = recordBundleFailure(q, "bv_4444444444444444", 2);
  q = recordBundleFailure(q, "bv_3333333333333333", 3);
  q = recordBundleFailure(q, "bv_3333333333333333", 4);
  assert.equal(isQuarantined(q, "bv_3333333333333333"), true);
  assert.equal(isQuarantined(q, "bv_4444444444444444"), false);
});

test("recordBundleFailure never mutates its input", () => {
  const before = [{ bookVersion: "bv_5555555555555555", failures: 1, lastFailureAt: 1 }];
  const frozen = JSON.stringify(before);
  recordBundleFailure(before, "bv_5555555555555555", 9);
  assert.equal(JSON.stringify(before), frozen);
});

// ── The self-heal ladder (D2) ────────────────────────────────────────────────

test("heal ladder: remount once, then abandon the source, then give up to the native floor", () => {
  assert.deepEqual(nextHealAction(0, "documents"), { action: "remount", quarantineCurrent: false });
  assert.deepEqual(nextHealAction(1, "documents"), { action: "fall-back", quarantineCurrent: true });
  assert.deepEqual(nextHealAction(2, "bundled"), { action: "give-up", quarantineCurrent: false });
});

test("heal ladder: a failing BAKED bundle is never quarantined — it is the code-signed floor", () => {
  // Quarantining the floor would leave the device with nothing to fall back to.
  assert.equal(nextHealAction(1, "bundled").quarantineCurrent, false);
});
