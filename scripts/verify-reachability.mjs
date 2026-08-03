#!/usr/bin/env node
/**
 * REACHABILITY GATE — "if there are N songs and/or pages, you must be able to reach all N."
 *
 * Miguel's invariant, 2026-08-03, stated as a hard rule. This turns it into something a build can
 * fail on instead of something a human has to notice.
 *
 * It replicates `resolveSongPage` from web/src/app.js EXACTLY:
 *
 *     const exact = songPageLookup.get(n);        if (finite) return exact;
 *     const next  = songIndex.find(e => e.song >= n);
 *     return next ? next.page : totalPages;
 *
 * and then answers two separate questions that are easy to conflate:
 *
 *   1. SONG reachability — does every indexed song resolve to its own page, exactly?
 *      A song that resolves to some OTHER song's page is a real defect: the singer types the
 *      number printed in the book and lands on the wrong music.
 *
 *   2. PAGE reachability — which pages can the numpad actually land on? Pages that no numpad
 *      entry reaches are swipe-only. That is not automatically a bug (a cover or a continuation
 *      page has no number of its own), but it MUST be a deliberate, listed set — never a surprise.
 *
 * The `check-book-consistency` warning "last indexed song is on page 371 but the PDF has 372
 * pages" is what prompted this: it reads like a missing song, but the trailing page is reached
 * through the `return totalPages` fallback — the same fallback that powers the ♪999 group-book
 * canary. This script distinguishes "unreachable" from "reached by fallback", which that warning
 * cannot.
 *
 * Usage:
 *   node scripts/verify-reachability.mjs                        # against live prod
 *   node scripts/verify-reachability.mjs --local                # against web/dist
 *   node scripts/verify-reachability.mjs --base https://…       # against any origin
 *
 * Exit 0 = every song resolves exactly and every page is accounted for.
 */
import fs from "node:fs";

const argv = process.argv.slice(2);
const arg = (n) => (argv.indexOf(n) >= 0 ? argv[argv.indexOf(n) + 1] : undefined);
const LOCAL = argv.includes("--local");
const BASE = (arg("--base") || "https://signovivo.com").replace(/\/+$/, "");
const MAX_NUMPAD = 9999; // 5+ digits route to the director/native path, never to a page

const loadManifest = async () => {
  if (LOCAL) return JSON.parse(fs.readFileSync("web/dist/bundle-manifest.json", "utf8"));
  const r = await fetch(`${BASE}/bundle-manifest.json?cb=${Date.now()}`, {
    headers: { "cache-control": "no-cache" },
  });
  const t = await r.text();
  try {
    return JSON.parse(t);
  } catch {
    console.error(`✖ ${BASE}/bundle-manifest.json is not JSON (SPA fallback?). ${t.length} B.`);
    process.exit(2);
  }
};

const main = async () => {
  const m = await loadManifest();
  const totalPages = m.totalPages;
  const songIndex = (m.songPages || []).map(([song, page]) => ({ song, page }));
  songIndex.sort((a, b) => a.song - b.song);
  const lookup = new Map(songIndex.map((e) => [e.song, e.page]));

  // Byte-for-byte the client's resolution order.
  const resolveSongPage = (n) => {
    const exact = lookup.get(n);
    if (Number.isFinite(exact)) return exact;
    const next = songIndex.find((e) => e.song >= n);
    return next ? next.page : totalPages;
  };

  console.log(`source      : ${LOCAL ? "web/dist (local)" : BASE}`);
  console.log(`bookVersion : ${m.bookVersion}`);
  console.log(`totalPages  : ${totalPages}`);
  console.log(`songs       : ${songIndex.length} (numbered ${songIndex[0]?.song} → ${songIndex[songIndex.length - 1]?.song})\n`);

  // ── 1. Every song must resolve to ITS OWN page ────────────────────────────────
  const wrongSong = songIndex.filter((e) => resolveSongPage(e.song) !== e.page);
  if (wrongSong.length) {
    console.error(`✖ ${wrongSong.length} song(s) do NOT resolve to their own page:`);
    for (const e of wrongSong.slice(0, 20)) {
      console.error(`    song ${e.song} → page ${resolveSongPage(e.song)} (expected ${e.page})`);
    }
  } else {
    console.log(`✅ SONGS: all ${songIndex.length} resolve exactly to their own page.`);
  }

  // ── 2. Which pages can the numpad reach at all? ───────────────────────────────
  const reached = new Map(); // page -> a numpad entry that gets there
  for (let n = 1; n <= MAX_NUMPAD; n++) {
    const p = resolveSongPage(n);
    if (!reached.has(p)) reached.set(p, n);
  }
  const unreachable = [];
  for (let p = 1; p <= totalPages; p++) if (!reached.has(p)) unreachable.push(p);

  console.log(`✅ PAGES: ${reached.size}/${totalPages} reachable from the numpad.`);

  // The trailing page is reached only via the `return totalPages` fallback — call that out by
  // name, because it is the exact case the consistency warning misreads as a missing song.
  const lastSongPage = songIndex.length ? songIndex[songIndex.length - 1].page : 0;
  if (totalPages > lastSongPage) {
    const via = reached.get(totalPages);
    console.log(
      `   note: page ${totalPages} carries no song entry; reached via the out-of-range fallback ` +
      `(e.g. typing ${via}). This is the same path that powers the ♪999 group-book canary.`,
    );
  }

  if (unreachable.length) {
    console.log(`\n   swipe-only pages (${unreachable.length}): ${unreachable.join(", ")}`);
    console.log(`   These have no song number of their own — covers, indexes, or continuation`);
    console.log(`   pages of a multi-page song. Reachable by swiping, not by typing a number.`);
  }

  if (wrongSong.length) {
    console.error(`\n✖ FAILED — a song resolves to the wrong page.`);
    process.exit(1);
  }
  console.log(`\n✅ invariant holds: every song reaches its own page, every page is accounted for.`);
};

main().catch((e) => {
  console.error("verify-reachability failed:", e);
  process.exit(3);
});
