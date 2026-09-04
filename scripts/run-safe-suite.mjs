#!/usr/bin/env node
// run-safe-suite.mjs — the ONLY way to run the e2e suite.
//
// `npm run test:e2e` globs e2e/*.test.mjs, and that glob includes e2e/relay-sync.test.mjs, which
// publishes to the PRODUCTION relay room and flips live followers' pages. CLAUDE.md has told every
// session to type
//     node --test $(ls e2e/*.test.mjs | grep -v 'relay-sync.test.mjs' | tr '\n' ' ')
// instead — a shell incantation that is easy to mistype, impossible to run from a tool that refuses
// command substitution, and different from what CI runs. This is that incantation as a script, with
// the exclusions named in one place.
//
//   node scripts/run-safe-suite.mjs            # every safe e2e file
//   node scripts/run-safe-suite.mjs --worker   # plus the worker unit tests that need no wrangler
//   node scripts/run-safe-suite.mjs --list     # print the file list, run nothing
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/** Never run from here, and why. Keep this the single source of truth for the exclusion. */
export const EXCLUDED = {
  "e2e/relay-sync.test.mjs": "publishes to the PRODUCTION relay room — flips live followers' pages",
  "e2e/eas-config.test.mjs": "shells out to `npx expo config`, which hangs; pins the banned EAS path",
  "sync-worker/test/a2.test.mjs": "needs a live local `wrangler dev`",
};

const args = process.argv.slice(2);
const e2e = fs.readdirSync(path.join(REPO, "e2e"))
  .filter((f) => f.endsWith(".test.mjs"))
  .map((f) => `e2e/${f}`)
  .filter((f) => !(f in EXCLUDED))
  .sort();
const worker = args.includes("--worker")
  ? fs.readdirSync(path.join(REPO, "sync-worker", "test"))
      .filter((f) => f.endsWith(".test.mjs"))
      .map((f) => `sync-worker/test/${f}`)
      .filter((f) => !(f in EXCLUDED))
      .sort()
  : [];
const files = [...e2e, ...worker];

if (args.includes("--list")) {
  for (const f of files) console.log(f);
  process.exit(0);
}
if (!files.length) { console.error("no test files found"); process.exit(2); }

const r = spawnSync(process.execPath, ["--test", ...files], { cwd: REPO, stdio: "inherit" });
process.exit(r.status ?? 1);
