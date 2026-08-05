#!/usr/bin/env node
/**
 * additive-gate — refuse to publish a songbook that changed a page a device already has.
 *
 * THE INVARIANT (docs/choir-pdf-distribution-plan.md §4, decision 7): the songbook only ever GROWS
 * AT THE END. Existing page numbers are permanent, and the bytes behind them never change. That is
 * what keeps every stale offline copy valid forever — an AirDropped PDF, a service-worker page
 * cache, a previous-edition fallback cache, a natively staged download. Break it and every one of
 * those becomes silently wrong on devices that are, by design, in a building with no internet and
 * no remedy.
 *
 * IT HAS ALREADY BEEN BROKEN ONCE IN PRODUCTION. Build 377 / PR #257 re-rendered ~290 pages in
 * place under unchanged page-NNN.webp filenames. Nothing fired. `pdfinfo` could not see it — the
 * page COUNT was identical — and every hash in the pipeline measures the artifact against itself,
 * so a self-consistent wrong book passes cleanly. This gate is the thing that would have caught it.
 *
 * WHY THE BASELINE IS READ FROM `git show HEAD:` AND NEVER THE WORKING TREE: a gate that diffs
 * against a file the same commit may edit can approve itself. Requiring the baseline bump to be its
 * own commit makes "I am changing pages on purpose" a reviewable act rather than a side effect.
 *
 * WHY NOT RE-RENDER THE BASELINE IN CI: `.github/workflows/ci.yml` installs poppler and webp
 * UNPINNED, so the runner and the build Mac can encode identical PDF input to different bytes. A
 * re-render baseline would red the gate for a toolchain difference (or, worse, mask a real change
 * behind one). The renderer versions are recorded in the manifest and compared instead, so a
 * toolchain drift is one loud explicit line rather than 372 silent hash mismatches.
 *
 * Usage:
 *   node scripts/additive-gate.mjs                       # baseline = git show HEAD:web/manifest-baseline.json
 *   node scripts/additive-gate.mjs --next web/dist/bundle-manifest.json
 *   node scripts/additive-gate.mjs --baseline <file>      # escape hatch for local experiments
 *   node scripts/additive-gate.mjs --allow-renderer-drift # CI: skip byte-identity when encoders differ
 *   ADDITIVE_OVERRIDE="yes I am changing published pages" node scripts/additive-gate.mjs
 *
 * Exit 0 = additive (or no baseline yet). Exit 1 = a published page changed.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export const OVERRIDE_PHRASE = "yes I am changing published pages";
export const BASELINE_PATH = "web/manifest-baseline.json";

const PAGE_RE = /^books\/[^/]+\/pages\/page-\d+\.webp$/;

/**
 * Pure decision function — no filesystem, no process. Everything the gate concludes is derived
 * from two manifest objects, so it is fully testable in node (e2e/additiveGate.test.mjs).
 *
 * Returns { violations: string[], additions: {...} }. Empty violations = safe to publish.
 *
 * The split is deliberate: PAGE IMAGES are the immutable surface (they are what offline copies are
 * keyed by); everything else — the shell, the manifests, the search index — is expected to change
 * on every build and is not policed here. `books/<id>/pages.json` legitimately changes the moment a
 * page is appended, so treating it as immutable would red every legitimate release.
 */
export function compareManifests(baseline, next, opts = {}) {
  const violations = [];
  const notes = [];
  if (!baseline || !next) return { violations: ["missing manifest"], additions: {}, notes };

  // ── Renderer drift makes BYTE-IDENTITY meaningless, but nothing else ──────────────────────────
  //
  // Page hashes only mean something when both sides came out of the same encoders. CI installs
  // poppler/webp UNPINNED, so its render of an unchanged PDF legitimately differs from the build
  // Mac's — measured 2026-08-03: pdftoppm 26.04.0 vs 26.07.0 changed exactly ONE page of 373.
  // Failing on that is a false alarm; ignoring it wholesale would hide real changes.
  //
  // So: when the renderers differ AND the caller opted in (`--allow-renderer-drift`, which CI
  // passes and scripts/release.sh deliberately does NOT), byte-identity is reported as a NOTE and
  // every renderer-INDEPENDENT invariant is still enforced — pages disappearing, the book
  // shrinking, songs moving to different pages, the pad width changing. Those are the ones that
  // actually break an offline device, and none of them depend on which encoder ran.
  //
  // On the publish path the renderers match by construction (same machine that made the baseline),
  // so a difference there is a REAL event: someone upgraded poppler, and the operator must
  // re-baseline deliberately rather than have it waved through.
  const r1 = baseline.renderer || {};
  const r2 = next.renderer || {};
  const rendererKeys = ["dpi", "webpQuality", "pdftoppm", "cwebp"];
  const rendererDiffs = rendererKeys.filter((k) => r1[k] !== undefined && r1[k] !== r2[k]);
  const skipByteIdentity = rendererDiffs.length > 0 && !!opts.allowRendererDrift;

  const pagesOf = (m) => new Map(
    (m.files || []).filter((f) => PAGE_RE.test(f.p)).map((f) => [f.p, f.h]),
  );
  const basePages = pagesOf(baseline);
  const nextPages = pagesOf(next);

  // 1. A book may shrink — but only by dropping pages NO SONG POINTS AT.
  //
  //    This was a blanket refusal. The fear that actually matters is narrow and concrete: a singer
  //    types a number and lands on a page that is gone. That needs the shrink to remove a page the
  //    SONG INDEX references. Trailing matter past the last song — canaries, blanks — is the book's
  //    to drop, and forbidding that made the songbook a one-way ratchet: three throwaway canary
  //    pages added to prove the OTA worked could then only be removed by a TestFlight round, which
  //    is precisely what an over-the-air songbook exists to avoid.
  //
  //    Mirrors verifyStaged's rule in src/bookUpdate.js so the release gate and the DEVICE agree —
  //    a shrink this gate waves through must be one every device will also accept. Fails CLOSED:
  //    no usable songPages ⇒ the old strict behaviour.
  const songFloor = (() => {
    const pairs = Array.isArray(next.songPages) ? next.songPages : null;
    if (!pairs || !pairs.length) return null;
    let max = 0;
    for (const e of pairs) {
      if (!Array.isArray(e) || e.length < 2) continue;
      const pg = Number(e[1]);
      if (Number.isFinite(pg) && pg > max) max = pg;
    }
    return max > 0 ? max : null;
  })();
  const pageNumOf = (p) => {
    const m = /page-(\d+)\.webp$/.exec(p);
    return m ? Number(m[1]) : NaN;
  };

  if (Number(next.totalPages) < Number(baseline.totalPages)) {
    if (songFloor == null) {
      violations.push(
        `book SHRANK: ${baseline.totalPages} → ${next.totalPages} pages, and the new manifest has no ` +
          `usable songPages, so it cannot prove no song was stranded.`,
      );
    } else if (Number(next.totalPages) < songFloor) {
      violations.push(
        `book SHRANK PAST ITS SONGS: ${baseline.totalPages} → ${next.totalPages} pages, but a song ` +
          `still points at page ${songFloor}. Typing that number would land on nothing.`,
      );
    }
  }

  // 2/3. Every published page must still exist, byte-for-byte. This is the #257 check.
  //      Pages ABOVE the song floor are exempt from the "disappeared" half — that is the whole
  //      point of rule 1 above — but a MODIFIED page is still a violation at any page number,
  //      because a device that already cached it would silently keep the old bytes forever.
  const removed = [];
  const modified = [];
  for (const [p, h] of basePages) {
    const nh = nextPages.get(p);
    if (nh === undefined) removed.push(p);
    else if (nh !== h) modified.push(p);
  }
  const strandedByRemoval = songFloor == null ? removed : removed.filter((p) => {
    const n = pageNumOf(p);
    return !Number.isFinite(n) || n <= songFloor;
  });
  if (strandedByRemoval.length) {
    violations.push(
      `${strandedByRemoval.length} published page(s) DISAPPEARED: ${summarize(strandedByRemoval)}`,
    );
  } else if (removed.length) {
    console.log(
      `  note: dropped ${removed.length} trailing page(s) above the last song page (${songFloor}) — ` +
        `no song points at them: ${summarize(removed)}`,
    );
  }
  if (modified.length) {
    const msg =
      `${modified.length} published page(s) CHANGED IN PLACE: ${summarize(modified)}. ` +
      "Every device that already cached these keeps the old bytes unless BOOK_VERSION busts it, " +
      "and offline copies can never be corrected at all.";
    if (skipByteIdentity) {
      notes.push(
        `byte-identity NOT verified (renderer differs: ${rendererDiffs.join(", ")}) — ` +
        `${modified.length} page(s) differ, which is expected across encoders and proves nothing either way. ` +
        "Only the publish path can check this.",
      );
    } else {
      violations.push(msg);
    }
  }

  // 4. Song numbers are how the choir addresses the book out loud. A song that still exists but
  //    now opens a different page is the signature of a SUBSTITUTED edition rather than an
  //    appended one — and a page-hash diff alone would report that as an ordinary large revision.
  const basePairs = new Map((baseline.songPages || []).map(([s, p]) => [s, p]));
  const nextPairs = new Map((next.songPages || []).map(([s, p]) => [s, p]));
  const moved = [];
  for (const [song, page] of basePairs) {
    const np = nextPairs.get(song);
    if (np === undefined) moved.push(`song ${song} REMOVED (was p${page})`);
    else if (np !== page) moved.push(`song ${song}: p${page} → p${np}`);
  }
  if (moved.length) {
    violations.push(`${moved.length} song(s) no longer open the same page: ${summarize(moved)}`);
  }

  // 5. Renderer drift. On the publish path this is a hard failure: the baseline came off this same
  //    machine, so a change means the toolchain moved and the operator must re-baseline knowingly.
  //    Under --allow-renderer-drift (CI) it is a note, because there it is expected and constant.
  for (const k of rendererDiffs) {
    const line = `renderer.${k} changed: ${JSON.stringify(r1[k])} → ${JSON.stringify(r2[k])}`;
    if (opts.allowRendererDrift) notes.push(line);
    else violations.push(line);
  }

  // 6. The frozen page-URL pad width. Changing it renames every page image at once, which is the
  //    single largest possible violation of this invariant, expressed as one integer.
  if (baseline.pagePadWidth !== undefined && baseline.pagePadWidth !== next.pagePadWidth) {
    violations.push(
      `pagePadWidth changed ${baseline.pagePadWidth} → ${next.pagePadWidth} — this RENAMES every page URL.`,
    );
  }

  // 7. Self-consistency of the new manifest: pages 1..totalPages must all be present by name.
  //    (Completeness against the real directory is the caller's job — see runCli.)
  const present = new Set(
    [...nextPages.keys()].map((p) => Number(p.match(/page-(\d+)\.webp$/)[1])),
  );
  const missing = [];
  for (let i = 1; i <= Number(next.totalPages || 0); i += 1) if (!present.has(i)) missing.push(i);
  if (missing.length) {
    violations.push(`manifest omits page(s) 1..totalPages: ${summarize(missing.map(String))}`);
  }

  const addedPages = [...nextPages.keys()].filter((p) => !basePages.has(p));
  const addedSongs = [...nextPairs.keys()].filter((s) => !basePairs.has(s));
  return { violations, additions: { pages: addedPages.length, songs: addedSongs.length }, notes };
}

const summarize = (list, max = 6) =>
  list.length <= max ? list.join(", ") : `${list.slice(0, max).join(", ")} … (+${list.length - max} more)`;

// ─── CLI ──────────────────────────────────────────────────────────────────────

const readCommittedBaseline = (root) => {
  // `git show HEAD:` — deliberately NOT the working tree. See the header.
  const r = spawnSync("git", ["show", `HEAD:${BASELINE_PATH}`], { cwd: root, encoding: "utf8" });
  if (r.status !== 0) return null; // no baseline committed yet — first run
  try {
    return JSON.parse(r.stdout);
  } catch {
    return null;
  }
};

export function runCli(argv, root) {
  const arg = (name, fallback = null) => {
    const i = argv.indexOf(name);
    return i === -1 ? fallback : (argv[i + 1] ?? fallback);
  };
  const nextPath = path.resolve(root, arg("--next", "web/dist/bundle-manifest.json"));
  if (!fs.existsSync(nextPath)) {
    console.error(`additive-gate: no manifest at ${nextPath} — run web/build.mjs first.`);
    return 1;
  }
  const next = JSON.parse(fs.readFileSync(nextPath, "utf8"));

  const baselineArg = arg("--baseline");
  const baseline = baselineArg
    ? JSON.parse(fs.readFileSync(path.resolve(root, baselineArg), "utf8"))
    : readCommittedBaseline(root);

  if (!baseline) {
    console.log(`additive-gate: no committed ${BASELINE_PATH} — nothing to compare against yet.`);
    console.log(`  Establish it with:  cp ${path.relative(root, nextPath)} ${BASELINE_PATH}`);
    console.log("  and commit it ON ITS OWN, so future page changes have to be deliberate.");
    return 0;
  }

  // Completeness against the real directory, which the pure function cannot see: a manifest that
  // simply omits a file would otherwise satisfy every check above.
  const distDir = path.dirname(nextPath);
  const walk = (d, b = d) => fs.readdirSync(d, { withFileTypes: true }).flatMap((e) => {
    const f = path.join(d, e.name);
    return e.isDirectory() ? walk(f, b) : [path.relative(b, f).split(path.sep).join("/")];
  });
  // Cloudflare Pages CONFIG ships in the deploy directory but is never served — wrangler drops it
  // from the upload walk, so a GET returns the SPA fallback, and any such path listed in the
  // manifest makes the OTA download the whole book and then fail verification forever (see
  // web/build.mjs NOT_FETCHABLE). It is therefore excluded from files[] BY DESIGN, and this
  // completeness check has to know that or it reds on every build from here on — and a gate that
  // always reds is a gate everyone learns to override. Deliberately duplicated from build.mjs and
  // smoke-boot rather than read out of the manifest: a gate that takes its expectation from the
  // artifact it is checking proves nothing. It encodes a Cloudflare platform fact, not our choice.
  const PAGES_CONFIG = new Set(["_headers", "_redirects", "_routes.json", "_worker.js"]);
  const onDisk = walk(distDir)
    .filter((p) => p !== "bundle-manifest.json" && !PAGES_CONFIG.has(p))
    .sort();
  const listed = (next.files || []).map((f) => f.p).sort();
  const completeness = [];
  if (onDisk.length !== listed.length || onDisk.some((p, i) => p !== listed[i])) {
    const missing = onDisk.filter((p) => !listed.includes(p));
    const extra = listed.filter((p) => !onDisk.includes(p));
    completeness.push(
      `manifest does not match the built directory — ${missing.length} unlisted file(s)` +
      `${missing.length ? ` (${summarize(missing)})` : ""}, ${extra.length} listed-but-absent` +
      `${extra.length ? ` (${summarize(extra)})` : ""}`,
    );
  }

  // DOES THE MANIFEST ACTUALLY DESCRIBE THESE BYTES?
  //
  // Everything above compares one manifest to another. Neither side was ever checked against the
  // files it claims to describe — so a manifest that has drifted from its own directory compares
  // perfectly against itself and reads GREEN.
  //
  // That is not hypothetical: the first committed baseline here was generated from a dist whose
  // page-005.webp had been hand-mutated by a test. The file was restored afterwards but the
  // manifest was not regenerated, so the baseline recorded 97945 bytes for a 97944-byte page and
  // claimed to be the book live on signovivo.com. It read green for four commits, because it was
  // being compared against the very manifest it came from, and only surfaced on the next real
  // rebuild. Production's own bytes settled it. This check catches that class at the source.
  const drifted = [];
  for (const f of next.files || []) {
    const abs = path.join(distDir, f.p);
    let bytes;
    try {
      bytes = fs.readFileSync(abs);
    } catch {
      continue; // already reported by the presence check above
    }
    if (bytes.length !== Number(f.n)) { drifted.push(`${f.p} (size ${f.n} vs ${bytes.length})`); continue; }
    if (crypto.createHash("sha256").update(bytes).digest("hex") !== f.h) drifted.push(`${f.p} (sha256)`);
    if (drifted.length > 8) break;
  }
  if (drifted.length) {
    completeness.push(
      `the manifest DOES NOT DESCRIBE the files it sits beside — ${summarize(drifted)}. ` +
      "Regenerate it with `node web/build.mjs`; do not hand-edit a manifest.",
    );
  }

  const { violations, additions, notes } = compareManifests(baseline, next, {
    allowRendererDrift: argv.includes("--allow-renderer-drift"),
  });
  const all = [...completeness, ...violations];

  console.log(`additive-gate: ${baseline.bookVersion} → ${next.bookVersion}`);
  console.log(`  ${baseline.totalPages} → ${next.totalPages} pages · +${additions.pages} page(s), +${additions.songs} song(s)`);

  for (const n of notes) console.log(`  ⓘ ${n}`);
  if (all.length === 0) {
    console.log(
      notes.length
        ? "✅ additive as far as this path can tell — no page removed, no shrink, no song moved."
        : "✅ additive — every previously published page is byte-identical and every song still opens the same page.",
    );
    return 0;
  }

  console.error("\n✖ ADDITIVE-ONLY VIOLATION:");
  for (const v of all) console.error(`   • ${v}`);

  if (process.env.ADDITIVE_OVERRIDE === OVERRIDE_PHRASE) {
    console.error(`\n⚠️  ADDITIVE_OVERRIDE matched — proceeding anyway.`);
    console.error("   Every device holding an offline copy of a changed page keeps the OLD bytes");
    console.error("   until BOOK_VERSION busts its cache, and pre-positioned copies never update.");
    return 0;
  }
  console.error(`\nIf this is deliberate, re-run with:`);
  console.error(`  ADDITIVE_OVERRIDE=${JSON.stringify(OVERRIDE_PHRASE)} <command>`);
  console.error(`and update ${BASELINE_PATH} in its OWN commit.\n`);
  return 1;
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  process.exit(runCli(process.argv.slice(2), root));
}
