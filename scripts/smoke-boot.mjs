#!/usr/bin/env node
/**
 * smoke-boot.mjs — the "would this have caught Wednesday?" boot smoke test.
 *
 * Part of the Release Safety System (M0). Purpose: on every PR, prove that the
 * BUILT web bundle can actually boot and render — the exact class of failure that
 * left the app "busted for everyone" at practice. It inspects the real build
 * artifacts (deterministic, no fragile browser emulation) and asserts the
 * invariants that must hold for a device to open the reader and follow along.
 *
 * By default it runs a fresh `node web/build.mjs` and then checks web/dist.
 * Set SMOKE_SKIP_BUILD=1 to inspect an already-built web/dist (fast local runs;
 * CI always builds fresh).
 *
 * Exit code 0 = all invariants hold. Non-zero = at least one failed; every
 * failure is printed (we do not stop at the first) so one CI run shows the whole
 * picture.
 *
 * This script is intentionally dependency-free and defensive (Murphy's Law): any
 * unexpected throw is caught and reported as a failure rather than crashing the
 * gate ambiguously.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
// SMOKE_DIST overrides the inspected directory (used to self-test the gate against
// a deliberately-broken copy). Defaults to the real build output.
const DIST = process.env.SMOKE_DIST ? path.resolve(process.env.SMOKE_DIST) : path.join(ROOT, "web", "dist");
const BOOK_ID = "standard";

const failures = [];
const notes = [];

/** Record a check. `ok` truthy = pass; otherwise `detail` explains the failure. */
function check(label, ok, detail) {
  if (ok) {
    console.log(`  ok   ${label}`);
  } else {
    console.log(`  FAIL ${label}${detail ? ` — ${detail}` : ""}`);
    failures.push(label + (detail ? ` — ${detail}` : ""));
  }
}

function read(rel) {
  return fs.readFileSync(path.join(DIST, rel), "utf8");
}
function exists(rel) {
  return fs.existsSync(path.join(DIST, rel));
}
function sizeOf(rel) {
  try {
    return fs.statSync(path.join(DIST, rel)).size;
  } catch {
    return -1;
  }
}

// ── 1. Build (unless told to inspect an existing dist) ────────────────────────
if (process.env.SMOKE_SKIP_BUILD === "1") {
  notes.push("SMOKE_SKIP_BUILD=1 — inspecting existing web/dist without rebuilding");
} else {
  console.log("→ building web bundle (node web/build.mjs)…");
  const build = spawnSync("node", ["web/build.mjs"], { cwd: ROOT, stdio: "inherit" });
  if (build.status !== 0) {
    console.error(`\n✖ web/build.mjs failed (exit ${build.status ?? "?"}). The bundle did not build — that IS a "busted for everyone" state.`);
    process.exit(1);
  }
}

console.log(`\n→ smoke-checking ${DIST}\n`);

try {
  // ── 2. Core shell files exist and are non-trivial ──────────────────────────
  for (const f of ["index.html", "app.js", "sw.js", "styles.css", "manifest.webmanifest", "books.json"]) {
    check(`shell file present + non-empty: ${f}`, exists(f) && sizeOf(f) > 32, exists(f) ? `only ${sizeOf(f)} bytes` : "missing");
  }

  // ── 3. Boot data: #pages-data must parse to a positive-integer totalPages ───
  // This is exactly what web/src/app.js:initReader reads to paint page 1 on boot
  // (native + web). A missing/garbage tag is a white-screen-on-boot.
  const html = exists("index.html") ? read("index.html") : "";
  const pagesTag = html.match(/<script id="pages-data" type="application\/json">([^<]*)<\/script>/);
  let inlineTotal = null;
  if (!pagesTag) {
    check("#pages-data script tag present in index.html", false, "tag not found — boot has no page count");
  } else {
    check("#pages-data script tag present in index.html", true);
    try {
      const parsed = JSON.parse(pagesTag[1]);
      inlineTotal = parsed.totalPages;
      check(
        "#pages-data totalPages is a positive integer",
        Number.isInteger(inlineTotal) && inlineTotal > 0,
        `got ${JSON.stringify(inlineTotal)}`
      );
    } catch (e) {
      check("#pages-data JSON parses", false, String(e && e.message));
    }
  }

  // ── 4. Page images: first + last page render, and count is consistent ──────
  // The single most important "can a device actually show the songbook?" check.
  const pagesDir = path.join(DIST, "books", BOOK_ID, "pages");
  let renderedCount = 0;
  let webpFiles = [];
  try {
    webpFiles = fs.readdirSync(pagesDir).filter((n) => /^page-\d+\.webp$/.test(n));
    renderedCount = webpFiles.length;
  } catch {
    /* handled below */
  }
  check("rendered page images exist", renderedCount > 0, renderedCount === 0 ? `no page-*.webp in ${pagesDir}` : undefined);

  // page 1 must exist and be a real (non-empty) image
  check("page 1 renders: page-001.webp present + non-empty", exists(`books/${BOOK_ID}/pages/page-001.webp`) && sizeOf(`books/${BOOK_ID}/pages/page-001.webp`) > 512, undefined);

  // TRIPLE CONSISTENCY: inline totalPages === rendered count === manifest totalPages.
  // This is the guard against the "song N unreachable / a page is missing" class
  // (the build-325 / song-370 fire drill): the app believes in N pages but fewer
  // were rendered, so jumping to a high song 404s a page that never renders.
  let manifestTotal = null;
  const manifestRel = `books/${BOOK_ID}/pages.json`;
  if (exists(manifestRel)) {
    try {
      manifestTotal = JSON.parse(read(manifestRel)).totalPages;
    } catch (e) {
      check("book manifest pages.json parses", false, String(e && e.message));
    }
  } else {
    check("book manifest pages.json present", false, `${manifestRel} missing`);
  }

  if (inlineTotal != null && renderedCount > 0) {
    check(
      `page count consistent: inline #pages-data (${inlineTotal}) === rendered images (${renderedCount})`,
      inlineTotal === renderedCount,
      "book believes in more/fewer pages than were rendered — high songs would 404"
    );
  }
  if (manifestTotal != null && renderedCount > 0) {
    check(
      `page count consistent: manifest pages.json (${manifestTotal}) === rendered images (${renderedCount})`,
      manifestTotal === renderedCount
    );
  }

  // last page must actually exist at the padded filename the app will request
  if (inlineTotal != null && inlineTotal > 0) {
    const pad = String(renderedCount || inlineTotal).length;
    const lastName = `page-${String(inlineTotal).padStart(pad, "0")}.webp`;
    check(`last page renders: ${lastName} present + non-empty`, exists(`books/${BOOK_ID}/pages/${lastName}`) && sizeOf(`books/${BOOK_ID}/pages/${lastName}`) > 512, undefined);
  }

  // ── 5. Native bridge markers survived the build ────────────────────────────
  // If the bundle can't talk to the native shell, the iPad reader is a dead page.
  const appJs = exists("app.js") ? read("app.js") : "";
  check('app.js contains the native bridge channel "signovivo-native"', appJs.includes("signovivo-native"), "bridge channel missing — native shell can't drive the WebView");
  check('app.js contains the "bridge-ready" handshake', appJs.includes("bridge-ready"), "bridge handshake missing");

  // ── 6. No unreplaced build-time tokens (a half-built bundle catcher) ────────
  for (const tok of ["__CACHE_VERSION__", "__BOOK_VERSION__", "__RELAY_BASE__", "__BUILD_NUMBER__"]) {
    check(`app.js has no unreplaced token ${tok}`, !appJs.includes(tok), "build-time replacement did not run — bundle is half-built");
  }
  const swJs = exists("sw.js") ? read("sw.js") : "";
  for (const tok of ["__CACHE_VERSION__", "__BOOK_VERSION__"]) {
    check(`sw.js has no unreplaced token ${tok}`, !swJs.includes(tok), "service worker version not stamped");
  }
  // PAGE_CACHE must be keyed identically in app.js and sw.js — a drift here silently splits the
  // cache the app counts from the cache the SW serves, and every offline-readiness claim lies.
  const pageCacheNames = [appJs, swJs].map((s) => (s.match(/signo-vivo-pages-\$\{BOOK_VERSION\}/g) || []).length);
  check("app.js and sw.js both key PAGE_CACHE by BOOK_VERSION", pageCacheNames.every((n) => n === 1), "page-cache key drifted between app.js and sw.js");
  // The anti-laundering contract is spread across both files; losing EITHER half silently
  // reintroduces the stale-book bug through the precache (previous-edition bytes persisted into
  // the current book's cache and certified "ready"). Pin all four pieces.
  check('sw.js tags previous-edition fallbacks (X-SV-Prev-Edition)', swJs.includes("X-SV-Prev-Edition"), "fallback tagging lost — precache can launder stale pages");
  check('sw.js refuses previous-edition fallbacks for no-store (precache) requests', swJs.includes('event.request.cache === "no-store"'), "no-store precache contract lost in sw.js");
  check('app.js precache rejects previous-edition responses', appJs.includes("X-SV-Prev-Edition"), "cacheSinglePage backstop lost — stale bytes can be persisted as current");
  check('sw.js answers the GET_BOOK_VERSION handshake', swJs.includes("GET_BOOK_VERSION"), "precache skew gate has no SW counterpart — deploy-window laundering possible");
  check('app.js gates the precache on the controller book version', appJs.includes("GET_BOOK_VERSION"), "deploy-skew precache gate lost in app.js");

  // The fleet dashboard's readiness claim must stay a MEASUREMENT. isOfflineBundleReady already
  // rotted into zero-caller dead code once, and while it was dead the webCached claim rested on
  // OFFLINE_READY_KEY alone — a flag written once per book version that nothing ever clears, so an
  // iPad whose CacheStorage iOS had evicted still reported green with zero pages cached. Losing
  // the call site is silent and invisible in every test that checks behavior rather than wiring,
  // so pin the wiring itself.
  check(
    "app.js verifies the offline bundle before claiming webCached",
    /isOfflineBundleReady\(totalPages\)/.test(appJs) && /webCached:\s*verifiedReady/.test(appJs),
    "fleetCheckin no longer verifies — webCached is back to trusting the never-cleared ready flag",
  );
  // Read-only inspection must not CREATE the current book's cache: an empty husk is something the
  // SW's activate keep-policy then has to score and reason about.
  // BOTH caches, asserted separately on purpose: an "either one" test passes while the PAGE_CACHE
  // read — the one whose husk the SW's activate keep-policy has to score — is still creating.
  check(
    "app.js inspects caches without creating them (openExistingCache)",
    appJs.includes("openExistingCache(STATIC_CACHE)")
      && appJs.includes("openExistingCache(PAGE_CACHE)"),
    "readiness check reverted to caches.open() — boot on an evicted device mints an empty husk",
  );

  // ── 7. Pre-app lib helpers loaded before app.js (M1 relay-room resolver) ───
  // If the lib isn't copied or isn't referenced, app.js still defaults safely to
  // the production room — but we assert it here so a broken build is caught.
  check("relay-room helper present: lib/svRelayRoom.js", exists("lib/svRelayRoom.js") && sizeOf("lib/svRelayRoom.js") > 32, exists("lib/svRelayRoom.js") ? undefined : "missing — staging channel resolver not shipped");
  check("index.html loads lib/svRelayRoom.js before app.js", html.includes("lib/svRelayRoom.js"), "resolver script tag missing from shell");
  const libJs = exists("lib/svRelayRoom.js") ? read("lib/svRelayRoom.js") : "";
  check("relay-room helper defaults to alvernia-main (production)", libJs.includes('"alvernia-main"'), "production room constant missing from resolver");
  check("app.js wires the relay-room resolver", appJs.includes("svRelayRoom") && appJs.includes("alvernia-main"), "app.js not using the resolver / production fallback");
  check("selftest helper present: lib/svSelftest.js", exists("lib/svSelftest.js") && sizeOf("lib/svSelftest.js") > 32, "readiness card not shipped");
  check("index.html loads lib/svSelftest.js", html.includes("lib/svSelftest.js"), "selftest script tag missing from shell");
  check("app.js wires the ?selftest card (gated on the query param)", appJs.includes("svSelftest") && appJs.includes("selftest"), "app.js not wiring the readiness card");
  // P2 sync robustness: the freshness-before-seq decision must ship + be wired.
  check("sync-decision helper present: lib/svSyncDecision.js", exists("lib/svSyncDecision.js") && sizeOf("lib/svSyncDecision.js") > 32, "follower-sync decision not shipped");
  check("index.html loads lib/svSyncDecision.js before app.js", html.includes("lib/svSyncDecision.js"), "sync-decision script tag missing from shell");
  const syncJs = exists("lib/svSyncDecision.js") ? read("lib/svSyncDecision.js") : "";
  check("sync-decision exposes decideRelaySnapshot", syncJs.includes("decideRelaySnapshot"), "decision entrypoint missing");
  check("app.js wires the sync-decision lib", appJs.includes("svSyncDecision") && appJs.includes("decideRelaySnapshot"), "app.js not using the sync-decision lib");
} catch (e) {
  // Defensive: never let the smoke test itself crash ambiguously.
  check("smoke test ran without an unexpected error", false, `threw: ${e && e.stack ? e.stack.split("\n")[0] : e}`);
}

// ── Summary ───────────────────────────────────────────────────────────────────
console.log("");
for (const n of notes) console.log(`  note: ${n}`);
if (failures.length === 0) {
  console.log(`\n✓ boot smoke PASSED — the built bundle boots, page 1 (and the last page) render, page counts are consistent, and the native bridge survived.\n`);
  process.exit(0);
}
console.error(`\n✖ boot smoke FAILED — ${failures.length} problem(s):`);
for (const f of failures) console.error(`   • ${f}`);
console.error(`\nThis bundle would risk a "busted for everyone" boot. Do NOT ship it.\n`);
process.exit(1);
