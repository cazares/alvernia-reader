// release.sh step 5b armed the OTA fleet-wide on EVERY production release, with no way to say no.
//
// The standing posture for Sunday 2026-09-07 is "OTA stays UNARMED": a hymnal update must not start a
// 27 MB download mid-Mass, and the worker redeploy that rides along with the arm also ships HEAD's relay
// code. Build 479 dodged step 5b only because the script crashed before it; a script that is skipped by
// luck is a script that arms the fleet next time. Confirmed by Phase 1 as release-tooling-1 (the arm is
// ungated, seconds after `pages deploy`, before any canary narrowing).
//
// This EXECUTES the real step-5b block out of scripts/release.sh with a fake `npx` on PATH:
//   SKIP_OTA_ARM=1 → the block prints SKIPPED, rewrites nothing, never calls wrangler;
//   default        → the block still rewrites BOOK_UPDATE_VERSION and calls `wrangler deploy` (unchanged).
// Re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, mkdtempSync, writeFileSync, mkdirSync, existsSync, chmodSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SH = readFileSync(join(ROOT, "scripts", "release.sh"), "utf8");

// Step 5b: from its banner to the `fi` that closes the sync-worker branch.
const step5b = () => {
  const start = SH.indexOf('echo "==> 5b/6 Arm OTA fleet-wide');
  assert.notEqual(start, -1, "step 5b's banner moved");
  const tail = SH.indexOf('skipping OTA arm"', start);
  assert.notEqual(tail, -1, "step 5b's sync-worker-missing branch moved");
  const end = SH.indexOf("\n  fi\n", tail);
  assert.notEqual(end, -1, "step 5b's closing fi moved");
  return SH.slice(start, end + "\n  fi\n".length);
};

// Run the block in a scratch tree: a sync-worker/wrangler.jsonc with the real patterns and a fake npx.
const runStep5b = (env) => {
  const dir = mkdtempSync(join(tmpdir(), "sv-release-5b-"));
  try {
    mkdirSync(join(dir, "sync-worker"));
    writeFileSync(join(dir, "sync-worker", "wrangler.jsonc"),
      '{ "vars": { "BOOK_UPDATE_VERSION": "bv_old", "LATEST_NATIVE_BUILD": "471" } }\n');
    mkdirSync(join(dir, "bin"));
    const log = join(dir, "npx.log");
    writeFileSync(join(dir, "bin", "npx"), `#!/bin/sh\necho "$@" >> "${log}"\necho deployed\n`);
    chmodSync(join(dir, "bin", "npx"), 0o755);
    const script = `set -u\nNEW_BOOK_VERSION=bv_new\nexport NATIVE_BUILD_JUST_SHIPPED=480\n${step5b()}`;
    const r = spawnSync("bash", ["-c", script], {
      cwd: dir,
      env: { ...process.env, ...env, PATH: `${join(dir, "bin")}:${process.env.PATH}` },
      encoding: "utf8",
    });
    return {
      status: r.status, out: `${r.stdout}\n${r.stderr}`,
      wrangler: readFileSync(join(dir, "sync-worker", "wrangler.jsonc"), "utf8"),
      npxCalled: existsSync(log) ? readFileSync(log, "utf8") : "",
    };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
};

test("SKIP_OTA_ARM=1 leaves the worker untouched: no rewrite, no deploy, and it says so", () => {
  const r = runStep5b({ SKIP_OTA_ARM: "1" });
  assert.equal(r.status, 0, `step 5b exited ${r.status}: ${r.out}`);
  assert.match(r.out, /SKIP_OTA_ARM/, "the skip is silent — an operator cannot tell the fleet was NOT armed");
  assert.match(r.wrangler, /"BOOK_UPDATE_VERSION": "bv_old"/, "BOOK_UPDATE_VERSION was rewritten despite SKIP_OTA_ARM=1 — the next plain deploy arms the new book");
  assert.match(r.wrangler, /"LATEST_NATIVE_BUILD": "471"/, "LATEST_NATIVE_BUILD was rewritten despite SKIP_OTA_ARM=1");
  assert.equal(r.npxCalled, "", `wrangler was invoked despite SKIP_OTA_ARM=1: ${r.npxCalled}`);
});

test("without the flag, step 5b still arms and deploys exactly as before", () => {
  const r = runStep5b({ SKIP_OTA_ARM: "0" });
  assert.equal(r.status, 0, `step 5b exited ${r.status}: ${r.out}`);
  assert.match(r.wrangler, /"BOOK_UPDATE_VERSION": "bv_new"/, "the default path no longer arms the new book");
  assert.match(r.wrangler, /"LATEST_NATIVE_BUILD": "480"/, "the default path no longer records the shipped native build");
  assert.match(r.npxCalled, /wrangler deploy/, "the default path no longer deploys the worker");
  assert.match(r.out, /OTA armed/, "the default path no longer reports the arm");
});
