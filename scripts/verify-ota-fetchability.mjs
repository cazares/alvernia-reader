#!/usr/bin/env node
/**
 * PRE-ARM GATE — prove the deployed book is actually downloadable before arming any iPad.
 *
 * WHY THIS EXISTS. `src/bookUpdate.js` fetches every manifest entry as `${base}/${f.p}` with NO
 * cache-buster (the manifest fetch itself uses `?v=${bookVersion}`, but the file fetches do not),
 * then `verifyStaged` demands an exact size AND md5 match for each. Cloudflare's edge holds assets
 * with `s-maxage=604800` — SEVEN DAYS. So any path whose CONTENT changed while its PATH stayed the
 * same can serve stale bytes to a device long after a successful deploy.
 *
 * A release that rewrites in place — every version-stamped release does, and the v390 web-only
 * bump changed exactly 4 files at unchanged paths — is therefore the highest-risk shape there is.
 *
 * The failure is silent by design-accident: `sync-worker/src/index.ts` only renders a stage tag for
 * `downloading*`/`ready`, so a device failing verify looks IDENTICAL to one that was never armed.
 * You would sit watching a dashboard cell that never changes, with no error anywhere.
 *
 * This script is the independent ground truth: it fetches each file the way the DEVICE would —
 * same URL shape, no cache-buster — and checks size + md5 against the served manifest, reporting
 * the CDN cache status for anything that disagrees.
 *
 * Usage:
 *   node scripts/verify-ota-fetchability.mjs                 # non-page files (fast, the risky set)
 *   node scripts/verify-ota-fetchability.mjs --all           # every file (~27 MB, the real thing)
 *   node scripts/verify-ota-fetchability.mjs --base https://…  # target another origin
 *
 * Exit 0 = every checked file is byte-exact. Non-zero = DO NOT ARM.
 */
import { createHash } from "node:crypto";

const args = process.argv.slice(2);
const BASE = (args.includes("--base") ? args[args.indexOf("--base") + 1] : "https://signovivo.com")
  .replace(/\/+$/, "");
const ALL = args.includes("--all");
const CONCURRENCY = 8;

const md5 = (buf) => createHash("md5").update(buf).digest("hex");
const isPage = (p) => /^books\/.*\/pages\/page-\d+\.webp$/.test(p);

const main = async () => {
  // Mirror the client exactly: the manifest IS cache-busted, the files are not.
  const manifestUrl = `${BASE}/bundle-manifest.json?cb=${Date.now()}`;
  const mres = await fetch(manifestUrl, { headers: { "cache-control": "no-cache" } });
  const text = await mres.text();

  // A Cloudflare Pages 200 is not evidence a file exists — the SPA fallback returns 200 + HTML for
  // any unknown path. Parse the body or you are trusting nothing at all.
  let manifest;
  try {
    manifest = JSON.parse(text);
  } catch {
    console.error(`✖ ${BASE}/bundle-manifest.json is not JSON (${mres.status}, ${text.length} B).`);
    console.error(`  First byte: ${JSON.stringify(text.slice(0, 1))} — this is the SPA fallback.`);
    process.exit(2);
  }

  const files = manifest.files || [];
  const pick = ALL ? files : files.filter((f) => !isPage(f.p));
  console.log(`base        : ${BASE}`);
  console.log(`bookVersion : ${manifest.bookVersion}`);
  console.log(`shell build : ${manifest.builtFromShellBuild}`);
  console.log(`files       : ${files.length} total, checking ${pick.length}` +
    `${ALL ? " (ALL)" : " (non-page — the set a version bump rewrites in place)"}\n`);

  const bad = [];
  let done = 0;
  const queue = [...pick];

  const worker = async () => {
    for (;;) {
      const f = queue.shift();
      if (!f) return;
      // EXACTLY the client's URL shape — no cache-buster. That is the whole point.
      const url = `${BASE}/${f.p}`;
      try {
        const r = await fetch(url);
        const buf = Buffer.from(await r.arrayBuffer());
        const gotSize = buf.length;
        const gotMd5 = md5(buf);
        // Size lives under `n`, NOT `s` (manifest entries are {p,n,h,m}). The first version of
        // this gate read f.s, which is always undefined — so the size check silently never ran and
        // every mismatch report printed "want size=undefined". md5 still caught the real failure,
        // but a gate whose check quietly no-ops is worse than no gate: it reads as coverage.
        const sizeOk = f.n === undefined || gotSize === f.n;
        const md5Ok = !f.m || gotMd5 === f.m;
        if (!sizeOk || !md5Ok) {
          bad.push({
            p: f.p,
            want: { size: f.n, md5: f.m },
            got: { size: gotSize, md5: gotMd5 },
            cf: r.headers.get("cf-cache-status"),
            age: r.headers.get("age"),
            cc: r.headers.get("cache-control"),
            status: r.status,
          });
        }
      } catch (e) {
        bad.push({ p: f.p, error: String(e && e.message) });
      }
      if (++done % 25 === 0 || done === pick.length) {
        process.stdout.write(`\r  checked ${done}/${pick.length}`);
      }
    }
  };

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  process.stdout.write("\n\n");

  if (!bad.length) {
    console.log(`✅ all ${pick.length} checked files are byte-exact against the served manifest.`);
    console.log(`   No stale-edge mismatch. Safe to arm ${manifest.bookVersion}.`);
    if (!ALL) console.log(`   (Run with --all to check all ${files.length} incl. page images.)`);
    return;
  }

  console.error(`✖ ${bad.length} MISMATCH(ES) — DO NOT ARM. A device would pull the whole book and`);
  console.error(`  fail verifyStaged, which the fleet dashboard renders identically to "never armed".\n`);
  for (const b of bad.slice(0, 20)) {
    if (b.error) {
      console.error(`  ${b.p}\n    fetch error: ${b.error}`);
      continue;
    }
    console.error(`  ${b.p}`);
    console.error(`    want size=${b.want.size} md5=${b.want.md5}`);
    console.error(`    got  size=${b.got.size} md5=${b.got.md5}`);
    console.error(`    http=${b.status} cf-cache-status=${b.cf} age=${b.age} cache-control=${b.cc}`);
  }
  if (bad.length > 20) console.error(`  … and ${bad.length - 20} more`);
  console.error(`\n  If cf-cache-status is HIT with a large age, this is the stale-edge defect:`);
  console.error(`  purge those paths at the CDN, re-run this, and only arm once it is clean.`);
  process.exit(1);
};

main().catch((e) => {
  console.error("verify-ota-fetchability failed:", e);
  process.exit(3);
});
