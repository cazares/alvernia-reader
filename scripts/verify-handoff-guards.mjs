#!/usr/bin/env node
/**
 * verify-handoff-guards.mjs — mutation-tests e2e/noStaleHandoff.test.mjs.
 *
 * The guard it checks exists because a committed handoff doc rots. A `HANDOFF.md`
 * sat at this repo's root and went stale three times — by 5 weeks and 41 builds
 * (#284), with three claims outright wrong (3f9b842), and finally from 2026-08-18
 * to 2026-09-01 telling every cold tab "read it before touching anything" while
 * describing a superseded build and an unmerged branch. Two audits filed it OPEN
 * and it kept misleading sessions regardless.
 *
 * A guard against that is worth exactly as much as its ability to fail. So: break
 * each thing it protects and assert the suite goes red. A mutation that slips
 * through is itself a failure of this script — and so is a SKIP.
 *
 * Mutations run in a throwaway clone, never in the working tree: an earlier
 * session left a source file mutated because `cp` declined a restore as
 * "identical (not copied)". Nothing here can touch your checkout.
 *
 * Usage:  node scripts/verify-handoff-guards.mjs
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

const ROOT = spawnSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" })
  .stdout.trim();
const TEST = "e2e/noStaleHandoff.test.mjs";

/** True when the guard suite passes inside `dir`. */
function suitePasses(dir) {
  const r = spawnSync(process.execPath, ["--test", TEST], { cwd: dir, encoding: "utf8" });
  return /^# fail 0$/m.test(r.stdout || "");
}

const MUTATIONS = [
  {
    name: "a handoff doc is committed at the repo root again (the original sin)",
    apply(dir) {
      fs.writeFileSync(path.join(dir, "HANDOFF.md"), "# stale handoff\nread me first\n");
      spawnSync("git", ["add", "-f", "HANDOFF.md"], { cwd: dir });
    },
  },
  {
    name: "the HANDOFF ignore rules are dropped from .gitignore",
    apply(dir) {
      const p = path.join(dir, ".gitignore");
      fs.writeFileSync(
        p,
        fs.readFileSync(p, "utf8").replace(/^HANDOFF\.md\nHANDOFF-\*\.md\n\*-HANDOFF\.md\n/m, "")
      );
    },
  },
  {
    name: "CLAUDE.md stops saying handoffs are never committed",
    apply(dir) {
      const p = path.join(dir, "CLAUDE.md");
      fs.writeFileSync(
        p,
        fs
          .readFileSync(p, "utf8")
          .replace("Session handoffs are **never committed**.", "Session handoffs are fine.")
      );
    },
  },
  {
    name: "CLAUDE.md is untracked, so no session ever reads the rule",
    apply(dir) {
      spawnSync("git", ["rm", "-q", "--cached", "CLAUDE.md"], { cwd: dir });
    },
  },
];

const work = fs.mkdtempSync(path.join(os.tmpdir(), "handoff-guards-"));
const failures = [];

try {
  for (const m of MUTATIONS) {
    const dir = path.join(work, m.name.replace(/\W+/g, "-").slice(0, 40));
    // A local clone of the working tree: cheap, and isolated from the real checkout.
    const c = spawnSync("git", ["clone", "--quiet", "--no-hardlinks", ROOT, dir], { encoding: "utf8" });
    if (c.status !== 0) {
      failures.push(`SETUP FAILED: ${m.name} — ${c.stderr.trim()}`);
      continue;
    }
    // The clone holds HEAD, which may predate this change. Bring it to the working
    // tree's INTENT so the harness runs identically before and after the commit lands:
    // the guarded files as they are now, and no handoff doc tracked at the root.
    for (const f of [".gitignore", "CLAUDE.md", TEST]) {
      fs.mkdirSync(path.dirname(path.join(dir, f)), { recursive: true });
      fs.copyFileSync(path.join(ROOT, f), path.join(dir, f));
    }
    const trackedRootHandoffs = spawnSync("git", ["ls-files"], { cwd: dir, encoding: "utf8" })
      .stdout.split("\n")
      .filter((f) => f && !f.includes("/") && /handoff/i.test(f) && /\.md$/i.test(f));
    for (const f of trackedRootHandoffs) {
      spawnSync("git", ["rm", "-q", "--cached", f], { cwd: dir });
      fs.rmSync(path.join(dir, f), { force: true });
    }
    spawnSync("git", ["add", "-A"], { cwd: dir });

    if (!suitePasses(dir)) {
      failures.push(`BASELINE RED: ${m.name} — the suite failed before mutating.`);
      continue;
    }
    m.apply(dir);
    if (suitePasses(dir)) failures.push(`NOT CAUGHT: ${m.name}`);
  }
} finally {
  fs.rmSync(work, { recursive: true, force: true });
}

const caught = MUTATIONS.length - failures.length;
console.log(`${failures.length} MISSED, 0 SKIPPED, ${caught} CAUGHT`);
for (const f of failures) console.error(`  ${f}`);
if (failures.length) {
  console.error("\nThe stale-handoff guard is decorative. Fix it before trusting it.");
  process.exit(1);
}
