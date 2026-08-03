/**
 * sw-page-cache.test.mjs — the page slot must never be poisoned by a document.
 *
 * THE BUG THIS PINS (measured 2026-08-02 against the staging deploy):
 *
 *   curl -s -D - https://staging.alvernia-reader.pages.dev/books/standard/pages/page-374.webp
 *   → HTTP/2 200, content-type: text/html; charset=utf-8, body = index.html
 *
 * Cloudflare Pages answers ANY unmatched path with the SPA shell, so asking for a page image
 * that does not exist at that PoP yet returns 200 with a DOCUMENT rather than a 404. sw.js's
 * page branch used to gate its cache write on shouldCacheResponse alone, which only looks for
 * `no-store` — so the HTML got stored under page-NNN.webp. And because that branch is
 * cache-FIRST with no revalidation, the slot stayed poisoned: that page rendered broken online
 * and offline until BOOK_VERSION changed. Inside the church there is no network and no remedy.
 *
 * The trigger is a routine deploy, not an exotic failure: the shell (which carries totalPages)
 * and the page assets do not flip atomically at a PoP. During the 2026-08-02 page-373 deploy,
 * books.json read 372 at the staging alias for ~3 minutes while pages.json at the same host
 * already read 373.
 *
 * These tests run the REAL web/src/sw.js (see helpers/sw-harness.mjs) and assert on what
 * actually lands in CacheStorage, so they fail if the guard is deleted OR inverted — unlike a
 * source grep, which passes either way.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import {
  ORIGIN,
  SRC_DIR,
  loadServiceWorker,
  pageImageResponse,
  spaFallbackResponse,
} from "./helpers/sw-harness.mjs";

const PAGE_URL = "/books/standard/pages/page-374.webp";

test("a 200 text/html SPA fallback is NEVER written into the page cache", async () => {
  const sw = loadServiceWorker();
  sw.setNetwork(async () => spaFallbackResponse());

  await sw.dispatchFetch(PAGE_URL);

  assert.deepEqual(
    await sw.entriesOf(sw.pageCacheName),
    [],
    "the SPA shell was persisted as a page image — that slot is now permanently broken",
  );
});

test("a real image/webp response IS still cached, normalized to the bare URL", async () => {
  const sw = loadServiceWorker();
  sw.setNetwork(async () => pageImageResponse("real-pixels"));

  const { response } = await sw.dispatchFetch(`${PAGE_URL}?retry=2`);

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "image/webp");
  // ?retry= must be stripped on the way in, or every retry would cache a duplicate entry.
  assert.deepEqual(await sw.entriesOf(sw.pageCacheName), [`${ORIGIN}${PAGE_URL}`]);
  const cached = await sw.readCached(sw.pageCacheName, PAGE_URL);
  assert.equal(cached.body, "real-pixels");
});

test("refuses to cache a page response with no content-type at all", async () => {
  // The asymmetry, pinned. CACHING demands a POSITIVE image/* because a bad write is permanent
  // and unrecoverable offline; SERVING rejects only a POSITIVE non-image, because demoting
  // unclassifiable-but-probably-fine bytes below a stale edition is its own regression. So an
  // untyped response is handed to the page and NOT persisted.
  const sw = loadServiceWorker();
  // A BufferSource body, deliberately: `new Response("string")` auto-sets text/plain, which is
  // positively-not-an-image and so would be rejected by BOTH rules — the test would pass while
  // testing nothing. (Caught by scripts/verify-sw-page-cache-guards.mjs, 2026-08-03.)
  sw.setNetwork(async () => new Response(new TextEncoder().encode("mystery-bytes"), { status: 200 }));

  const { response } = await sw.dispatchFetch(PAGE_URL);
  assert.equal(response.headers.get("content-type"), null, "this test needs an UNTYPED response");

  assert.equal(await response.text(), "mystery-bytes", "unclassifiable bytes must still be served");
  assert.deepEqual(
    await sw.entriesOf(sw.pageCacheName),
    [],
    "a response we cannot positively identify as an image must never be persisted",
  );
});

test("a healthy cached page is still served cache-first, without touching the network", async () => {
  const sw = loadServiceWorker();
  await sw.seed(sw.pageCacheName, PAGE_URL, pageImageResponse("cached-pixels"));
  sw.setNetwork(async () => {
    throw new Error("the network must not be consulted on a healthy cache hit");
  });

  const { response } = await sw.dispatchFetch(PAGE_URL);

  assert.equal(await response.text(), "cached-pixels");
  assert.deepEqual(sw.networkRequests, [], "cache-first was lost — page turns now wait on the network");
});

test("the SPA fallback does not win over a previous edition of the page", async () => {
  const sw = loadServiceWorker({ bookVersion: "book-new" });
  // A full bundle from the previous book, exactly what activate() keeps around.
  await sw.seed("signo-vivo-pages-book-old", PAGE_URL, pageImageResponse("previous-edition-pixels"));
  sw.setNetwork(async () => spaFallbackResponse());

  const { response } = await sw.dispatchFetch(PAGE_URL);

  assert.equal(response.headers.get("content-type"), "image/webp");
  assert.equal(await response.text(), "previous-edition-pixels");
  assert.equal(
    response.headers.get("X-SV-Prev-Edition"),
    "1",
    "previous-edition bytes must stay tagged so the precache refuses to launder them",
  );
  assert.deepEqual(await sw.entriesOf(sw.pageCacheName), []);
});

test("an already-poisoned slot is evicted and refilled from the network", async () => {
  // The state of every device that hit this bug before the fix shipped. A code-only deploy does
  // NOT rotate PAGE_CACHE (it is keyed by BOOK_VERSION, the book's hash), so prevention alone
  // would leave these devices broken forever.
  const sw = loadServiceWorker();
  await sw.seed(sw.pageCacheName, PAGE_URL, spaFallbackResponse("poison"));
  sw.setNetwork(async () => pageImageResponse("healed-pixels"));

  const { response } = await sw.dispatchFetch(PAGE_URL);

  assert.equal(response.headers.get("content-type"), "image/webp");
  assert.equal(await response.text(), "healed-pixels");
  const cached = await sw.readCached(sw.pageCacheName, PAGE_URL);
  assert.equal(cached.contentType, "image/webp", "the healed bytes must survive the eviction");
  assert.equal(cached.body, "healed-pixels");
});

test("every query variant of a poisoned slot is purged, not just the one requested", async () => {
  const sw = loadServiceWorker();
  await sw.seed(sw.pageCacheName, `${PAGE_URL}?reload=abc`, spaFallbackResponse("poison"));
  sw.setNetwork(async () => pageImageResponse("healed-pixels"));

  await sw.dispatchFetch(PAGE_URL);

  const entries = await sw.entriesOf(sw.pageCacheName);
  assert.deepEqual(
    entries,
    [`${ORIGIN}${PAGE_URL}`],
    "a ?reload= variant survived the purge and will be served on the next retry",
  );
});

test("a poisoned slot still heals when the browser ignores ignoreSearch on delete", async () => {
  // ignoreSearch is the least-exercised corner of the Cache API and this fleet runs iOS 15
  // iPads. If a browser accepts the option and quietly no-ops it, deleting only `event.request`
  // misses the stored entry whenever the two differ — and they DO differ on exactly the path
  // that matters: app.js's <img> error handler retries as page-NNN.webp?retry=N while the
  // writer normalizes to the bare URL. Offline, nothing overwrites the slot either, so without
  // the explicit bare-key delete the poison survives and is served again on the next view.
  const sw = loadServiceWorker({ brokenIgnoreSearchDelete: true });
  await sw.seed(sw.pageCacheName, PAGE_URL, spaFallbackResponse("poison"));
  // Network stays dead (harness default): the delete is the ONLY thing that can clear the slot.

  await sw.dispatchFetch(`${PAGE_URL}?retry=1`);

  assert.deepEqual(
    await sw.entriesOf(sw.pageCacheName),
    [],
    "the retry path left the poisoned entry behind — that page stays broken on the next view",
  );
});

test("a poisoned slot is dropped even when the network is dead, so it can heal later", async () => {
  const sw = loadServiceWorker();
  await sw.seed(sw.pageCacheName, PAGE_URL, spaFallbackResponse("poison"));
  // Offline: the harness default network rejects.

  const { response, error } = await sw.dispatchFetch(PAGE_URL);

  assert.equal(response, null, "a document was served as a page image");
  assert.ok(error, "the failure must surface so the page's own retry UI runs");
  assert.deepEqual(
    await sw.entriesOf(sw.pageCacheName),
    [],
    "poison survived an offline view — the slot stays broken forever",
  );
});

test("a poisoned PREVIOUS edition is skipped so a real older one can still be found", async () => {
  const sw = loadServiceWorker({ bookVersion: "book-new" });
  // Oldest first: insertion order is what matchAnyPageCache reverses to prefer the freshest.
  await sw.seed("signo-vivo-pages-book-oldest", PAGE_URL, pageImageResponse("oldest-real-pixels"));
  await sw.seed("signo-vivo-pages-book-older", PAGE_URL, spaFallbackResponse("poison"));

  const { response } = await sw.dispatchFetch(PAGE_URL);

  assert.equal(response.headers.get("content-type"), "image/webp");
  assert.equal(
    await response.text(),
    "oldest-real-pixels",
    "the scan stopped at a poisoned previous edition instead of continuing",
  );
});

test("the no-store precache path still reports the network verbatim, and caches nothing bad", async () => {
  const sw = loadServiceWorker();
  sw.setNetwork(async () => spaFallbackResponse());

  // cacheSinglePage's contract: it must SEE what the origin said and decide for itself.
  const { response } = await sw.dispatchFetch(PAGE_URL, { cache: "no-store" });

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "text/html; charset=utf-8");
  assert.deepEqual(
    await sw.entriesOf(sw.pageCacheName),
    [],
    "the SW's background write poisoned the cache on a precache request",
  );
});

test("a no-store precache request is never answered from a previous edition", async () => {
  const sw = loadServiceWorker({ bookVersion: "book-new" });
  await sw.seed("signo-vivo-pages-book-old", PAGE_URL, pageImageResponse("previous-edition-pixels"));

  const { response, error } = await sw.dispatchFetch(PAGE_URL, { cache: "no-store" });

  assert.equal(response, null);
  assert.ok(error, "an offline precache must fail honestly instead of completing with stale bytes");
});

test("the image-only rule is scoped to the page branch — shell assets still cache", async () => {
  // Guard against over-reach: text/css is not an image, and must still be cached by the
  // stale-while-revalidate branch. The shell/manifest paths take the same SPA fallback but
  // revalidate on every load, so a bad copy self-corrects rather than sticking.
  const sw = loadServiceWorker();
  sw.setNetwork(async () => new Response("body{}", {
    status: 200,
    headers: { "content-type": "text/css" },
  }));

  const { response } = await sw.dispatchFetch("/styles.css");

  assert.equal(await response.text(), "body{}");
  const cached = await sw.readCached(sw.staticCacheName, "/styles.css");
  assert.ok(cached, "the page-image guard leaked into the shell branch — styles.css no longer caches");
  assert.equal(cached.contentType, "text/css");
});

// ── The OTHER writer ────────────────────────────────────────────────────────────
// app.js's cacheSinglePage calls cache.put() on PAGE_CACHE DIRECTLY — that write never passes
// through the SW's fetch handler, so the guards above cannot see it. Losing this half is worse
// than losing the SW half: the precache would persist the document AND count it toward
// completeness, so isOfflineBundleReady certifies the bundle and the fleet dashboard reports
// green over a page that renders broken. app.js is a browser module too large to evaluate here,
// so pin the wiring the way smoke-boot.mjs does for the anti-laundering contract.
test("app.js's precache refuses to persist a page response that is not an image", () => {
  const appJs = fs.readFileSync(path.join(SRC_DIR, "app.js"), "utf8");

  assert.match(
    appJs,
    /const isPageImageResponse = \(response\) =>[\s\S]{0,200}?startsWith\("image\/"\)/,
    "cacheSinglePage's content-type guard is gone — the precache can poison a page slot",
  );
  // The guard must run BEFORE the put, not merely exist somewhere in the file.
  const guardIndex = appJs.indexOf("if (!isPageImageResponse(response))");
  const putIndex = appJs.indexOf("await cache.put(url, response.clone());");
  assert.ok(guardIndex > 0, "the precache no longer checks the response content-type before caching");
  assert.ok(putIndex > 0, "cacheSinglePage's put moved — re-verify this test still pins the write");
  assert.ok(guardIndex < putIndex, "the content-type guard must run before the page bytes are persisted");
  // And an ALREADY-poisoned slot must be repaired, not skipped by the presence check.
  assert.match(
    appJs,
    /if \(existing && isPoisonedPageEntry\(existing\)\) \{\s*await cache\.delete\(url\);/,
    "cacheSinglePage skips existing entries without validating them — poison can never heal",
  );
  // The repair block above can sit there as pure decoration while an UNVALIDATED presence check
  // returns first — the mutation harness proved the regex alone misses that, because the dead
  // block still matches. Pin the absence of the pre-fix line itself.
  assert.ok(
    !appJs.includes("if (await cache.match(url)) return false;"),
    "cacheSinglePage returns on a bare presence check again — a poisoned slot is never re-fetched",
  );
});

test("app.js counts cached pages by CONTENT, so a poisoned slot reads as missing", () => {
  // This is what makes the repair above reachable at all. ensureOfflineBundle only calls
  // cacheSinglePage for pages getCachedPageSet reports as MISSING — and a poisoned slot has a
  // perfectly good page-NNN.webp key. Counting keys alone therefore hid the poison from the
  // repair (permanent breakage) AND let isOfflineBundleReady certify a bundle containing it,
  // so the pre-Mass dashboard showed green over a page that renders broken.
  const appJs = fs.readFileSync(path.join(SRC_DIR, "app.js"), "utf8");
  const getCachedPageSet = appJs.slice(
    appJs.indexOf("const getCachedPageSet"),
    appJs.indexOf("const cacheAssetsNoStore"),
  );

  assert.ok(getCachedPageSet.length > 0, "getCachedPageSet moved — re-verify this test still pins it");
  assert.ok(
    getCachedPageSet.includes("isPoisonedPageEntry"),
    "getCachedPageSet counts keys without inspecting content — poisoned slots read as cached",
  );
  // The permissive rule, not the strict one: an entry we cannot classify must stay counted, or a
  // misfire re-downloads the whole 25MB book on every device in the fleet.
  assert.ok(
    !getCachedPageSet.includes("isPageImageResponse"),
    "getCachedPageSet uses the STRICT write-rule — an untyped entry would trigger a full re-download",
  );
  // Must degrade to key-only counting rather than declaring everything missing.
  assert.match(
    getCachedPageSet,
    /responses\.length === keys\.length/,
    "the matchAll/keys correlation is unchecked — a mismatch would mislabel good pages as missing",
  );
});
