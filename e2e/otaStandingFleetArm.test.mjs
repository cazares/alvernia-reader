import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const WRANGLER = fs.readFileSync("sync-worker/wrangler.jsonc", "utf8");
const RELEASE = fs.readFileSync("scripts/release.sh", "utf8");

// WHY THIS FILE EXISTS. OTA arming shipped DORMANT by design (PR #356) — a deliberate
// safety-by-friction default requiring a human to hand-arm one device, prove it, then the
// fleet. Miguel's explicit call afterward (2026-08-18): every device, always, all at once,
// because a rollback is just another deploy. That is a real reversal of the shipped default,
// so it needs its own pin — a future "let's be careful again" edit that quietly re-dormants
// BOOK_UPDATE_VERSION or forgets to keep it in lockstep should fail loud, not silently ship
// stale.

test("the fleet is armed by default: version set, wildcard devices, fleet allowed", () => {
  assert.match(WRANGLER, /"BOOK_UPDATE_VERSION":\s*"bv_[0-9a-f]{16}"/,
    "BOOK_UPDATE_VERSION reverted to dormant (empty) — no device can act on it");
  assert.match(WRANGLER, /"BOOK_UPDATE_DEVICES":\s*"\*"/,
    "BOOK_UPDATE_DEVICES is not wildcarded — only named devices would be armed");
  assert.match(WRANGLER, /"BOOK_UPDATE_ALLOW_FLEET":\s*"yes"/,
    "BOOK_UPDATE_ALLOW_FLEET is not \"yes\" — the wildcard alone does nothing (bookArming.js)");
});

test("concurrency is high enough that a fleet this size never gets staggered", () => {
  const m = WRANGLER.match(/"BOOK_UPDATE_CONCURRENCY":\s*"(\d+)"/);
  assert.ok(m, "BOOK_UPDATE_CONCURRENCY missing");
  assert.ok(Number(m[1]) >= 10, `BOOK_UPDATE_CONCURRENCY=${m[1]} is low enough to stagger an 8-device fleet`);
});

test("release.sh keeps BOOK_UPDATE_VERSION in lockstep with the shipped book, every prod release", () => {
  assert.match(RELEASE, /BOOK_UPDATE_VERSION.*NEW_BOOK_VERSION/,
    "release.sh no longer rewrites BOOK_UPDATE_VERSION — the worker would arm a stale/wrong book");
  assert.match(RELEASE, /cd sync-worker && npx wrangler deploy/,
    "release.sh no longer deploys the sync-worker — the rewritten var would never go live");
});

test("the OTA arm step is skipped for STAGING (canary) releases — never arms devices from a preview build", () => {
  const staging = RELEASE.slice(0, RELEASE.indexOf('if [ "$STAGING" != "1" ]; then'));
  // The arm block itself must live INSIDE the `[ "$STAGING" != "1" ]` guard that already wraps
  // the additive-baseline refresh, not as a sibling unguarded step.
  const guardedBlock = RELEASE.slice(
    RELEASE.indexOf('if [ "$STAGING" != "1" ]; then\n  cp web/dist/bundle-manifest.json'),
  );
  const armIdx = guardedBlock.indexOf("Arm OTA fleet-wide");
  const fiIdx = guardedBlock.indexOf("\nfi\n");
  assert.ok(armIdx > 0 && fiIdx > 0 && armIdx < fiIdx,
    "the OTA arm step is not nested inside the same STAGING guard as the baseline refresh");
});

test("a failed OTA arm/deploy does not abort the release — it must fail soft and say so", () => {
  // Marker-bounded, not character-bounded: the block runs from its banner to the `fi` that closes
  // the sync-worker branch. A fixed 1200-char window went red the moment a comment was added above
  // the deploy line — and would have stayed GREEN had the fallback been deleted from a longer block.
  const start = RELEASE.indexOf("Arm OTA fleet-wide");
  const tail = RELEASE.indexOf('skipping OTA arm"', start);
  assert.ok(start > 0 && tail > start, "the OTA arm block's markers moved");
  const block = RELEASE.slice(start, RELEASE.indexOf("\n  fi\n", tail));
  assert.match(block, /npx wrangler deploy[\s\S]*?\|\|\s*echo/,
    "no fallback echo after the worker deploy — a failed deploy would abort the whole release script under set -e");
});
