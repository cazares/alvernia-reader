const CACHE_VERSION = "ba33537-c0aabb4d";
// Content-address of the BOOK (source PDF bytes + render knobs, hashed by build.mjs).
// Distinct from CACHE_VERSION (the shell hash) ON PURPOSE:
//   - shell-only deploy → bookVersion unchanged → PAGE_CACHE name unchanged → every cached
//     page stays a hit; no 25MB re-download for a CSS fix.
//   - ANY book change — including a page revised IN PLACE under an unchanged page-NNN.webp
//     filename — → new PAGE_CACHE name → the network-first page handler below fetches the
//     new bytes instead of resurrecting the old ones from cache.
// A book-only change still propagates without a shell bump because this token changes the
// emitted sw.js bytes, which is what triggers the browser's SW update flow.
const BOOK_VERSION = "2dc91b796f72";
const STATIC_CACHE = `signo-vivo-static-${CACHE_VERSION}`;
const PAGE_CACHE = `signo-vivo-pages-${BOOK_VERSION}`;
const STATIC_CACHE_PREFIX = "signo-vivo-static-";
const PAGE_CACHE_PREFIX = "signo-vivo-pages-";
// We KEEP the previous book's page cache across a version bump (instead of deleting it in
// activate) and the page handler falls back to it when the network can't help. That is what
// saves a follower who goes offline right after a book deploy: the freshly-activated SW's own
// PAGE_CACHE is empty (app.js re-precaches into it lazily), but the previous version's full
// bundle is still there to serve — a stale page beats a blank one, and ONLY when offline.
// Two newest kept so storage can't grow unbounded: current + the immediately-previous full one.
//
// HISTORY, do not regress this: the fallback used to run BEFORE the network, justified first by
// an `immutable` header that never actually applied in prod (Cloudflare Pages drops `_headers`
// rules with two `*`s — see PR #273), then by an "additive-only" convention that build 377 / PR
// #257 had already violated by re-rendering ~290 pages in place. Old-cache-before-network meant
// a page changed under an unchanged filename NEVER reached an already-cached device — the
// re-precache itself was served from the old cache, copying stale bytes forward version after
// version. Network-first-on-miss (below) + the book-keyed cache name is the fix. M6's
// hash-keyed page URLs remain the long-term design; until then `immutable` stays inert.
const PAGE_CACHES_TO_KEEP = 2;
// Bound the network attempt for a PAGE image before falling back to a previous book's cache.
// Truly offline (church) fetches reject in milliseconds, so this timer is irrelevant at Mass;
// it only matters on pathological weak-signal networks, where showing the PREVIOUS edition of
// a page after 3s beats an indefinite spinner mid-song. The slow fetch is not abandoned — it
// finishes via waitUntil and lands in the cache for the next page-turn.
const PAGE_NETWORK_TIMEOUT_MS = 3000;
// The shell assets a returning follower MUST have cached to load/reload offline. If the install
// precache can't populate these (e.g. install ran on the last bar of signal, then the network
// died), we must NOT let this half-baked SW take over from a fully-cached older one.
const CRITICAL_SHELL_ASSETS = ["/", "/index.html", "/styles.css", "/app.js"];
// The book manifests ARE included (an earlier comment excluded them fearing an aborted atomic
// install — stale since the install went per-asset Promise.allSettled). They are deliberately
// NOT in CRITICAL_SHELL_ASSETS: a missed manifest must never block activation, it just gets
// picked up by SWR / the precache's strict step. Installing them matters because STATIC_CACHE
// rotates on EVERY deploy and activate deletes the old one — without an install copy (plus the
// activate salvage below), the deploy boundary destroyed the only cached manifests, and an
// offline boot in that window had pages but no song index: every numpad jump landed on the
// last page. Pages get two-edition retention; the manifests get install + salvage.
const CORE_ASSETS = [
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  // books.json: app.js's SHELL_ASSETS includes it, so the shell healer checks for it — if the
  // install doesn't cache it, the healer finds a gap after EVERY deploy and refetches the whole
  // shell on every device, deterministically. Install and heal must agree on the asset list.
  "/books.json",
  "/icon.png",
  "/icon-192.png",
  "/icon-512.png",
  "/books/standard/pages.json",
  "/books/standard/search-index.json",
];
// The two book manifests, named once: install precaches them, and activate salvages them from a
// retiring static cache when the new one lacks them (stale manifest beats NO manifest — the same
// judgment the previous-edition page fallback makes).
const BOOK_MANIFEST_PATHS = ["/books/standard/pages.json", "/books/standard/search-index.json"];
const NETWORK_FIRST_PATHS = new Set([
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  // The book manifests: their display READERS (song-index hydration, search) use the default
  // cache mode and need stale-while-revalidate semantics — fresh online, served from cache
  // offline. Their WRITER (the strict post-gate precache step) uses no-store and never reaches
  // this branch. Without these entries the manifests fell to the generic cache-first handler,
  // where a cached copy was never refreshed.
  "/books/standard/pages.json",
  "/books/standard/search-index.json",
]);

// NOTE: bulk offline precaching lives in app.js (ensureOfflineBundle), which caches
// /books/standard/pages/... into PAGE_CACHE. The old SW-side backgroundCacheAllPages()
// helper was dead code with stale /pages/ paths and has been removed to avoid confusion.

// True when every CRITICAL_SHELL_ASSET is present in the NEW static cache — i.e. this SW could
// actually serve an offline reload. Used to gate activation so we never replace a complete old
// SW with a shell-less new one (Promise.allSettled below "succeeds" even when offline left the
// cache empty/partial).
const isShellCached = async () => {
  const cache = await caches.open(STATIC_CACHE);
  const matches = await Promise.all(
    CRITICAL_SHELL_ASSETS.map((asset) => cache.match(asset)),
  );
  return matches.every(Boolean);
};

// True when no older static cache exists yet — a genuine first-ever install. In that case there
// is no complete old SW to fall back on, so there's nothing to protect by deferring; we activate
// with whatever we managed to cache (a cold first install can only have happened online anyway).
const isFirstInstall = async () => {
  const keys = await caches.keys();
  return !keys.some((key) => key.startsWith(STATIC_CACHE_PREFIX) && key !== STATIC_CACHE);
};

// Only take over (skipWaiting → activate → controllerchange reload) when this SW can actually
// serve the shell offline, OR there's no older SW worth protecting. Otherwise stay WAITING and let
// the fully-cached old SW keep serving; a later online update will install a complete shell and
// activate then. Guards both the install auto-skip and the app.js-driven SKIP_WAITING message.
const skipWaitingIfShellReady = async () => {
  if ((await isShellCached()) || (await isFirstInstall())) {
    await self.skipWaiting();
  }
};

self.addEventListener("install", (event) => {
  // Resilient precache: cache each core asset independently so a single failure (404 /
  // offline) can never abort the whole install the way the atomic cache.addAll() would.
  event.waitUntil(
    caches
      .open(STATIC_CACHE)
      .then((cache) => Promise.allSettled(CORE_ASSETS.map((asset) => cache.add(asset))))
      // Don't unconditionally take over: a shell-less install (cached offline) would otherwise
      // skipWaiting + activate + wipe the old static cache, leaving offline reloads with no shell.
      .then(() => skipWaitingIfShellReady()),
  );
});

self.addEventListener("activate", (event) => {
  // Clean up old-version caches. We deliberately do NOT pre-fetch all 370 pages
  // anymore — that ~34 MB background download froze first loads. Followers cache
  // pages on demand (cache-first handler below); the offline iPad still preloads
  // the whole manual via signovivo.com?admin=1.
  //
  // Two deliberate departures from a naive "delete everything that isn't the current version":
  //   1. PAGE caches: keep the newest PAGE_CACHES_TO_KEEP (current + previous) instead of nuking
  //      the old one. The new PAGE_CACHE starts empty and only fills lazily, so deleting the old
  //      full bundle here would strand a follower who goes offline right after a book deploy. The
  //      surviving old cache is the PREVIOUS edition — possibly stale bytes for a revised page —
  //      which the handler serves only when the network can't provide the current ones.
  //   2. STATIC caches: only delete the old shell once the NEW shell is actually cached. If we
  //      somehow activate without a complete shell (shouldn't happen — skipWaiting is gated above —
  //      but be defensive), keep the old static cache so offline reloads still find a shell.
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();

      const pageCaches = keys.filter((key) => key.startsWith(PAGE_CACHE_PREFIX));
      // Retention: the CURRENT cache (whether or not it exists yet — it's created lazily) plus
      // the (PAGE_CACHES_TO_KEEP - 1) BEST previous edition(s). "Best" is judged by CONTENT, not
      // recency: an interrupted precache or a mere boot-time existence check can leave a NEWER
      // cache that is empty or nearly so, and recency-based keeping (`slice(-N)`) would evict the
      // only FULL edition in its favor — leaving a once-fully-cached device with BLANK pages the
      // next time it goes offline mid-migration. Entry counts are one cheap await per cache
      // (at most a couple of previous caches exist). Ties break to the newest (reverse scan).
      const previous = pageCaches.filter((key) => key !== PAGE_CACHE);
      const counted = await Promise.all(
        previous.map(async (key, index) => {
          try {
            const entries = await (await caches.open(key)).keys();
            // Score unique PAGE COVERAGE, not raw entries. Pre-build-347 caches hold duplicate
            // `?retry=`/`?reload=` query-variant entries (normalization began at cb853ac), so a
            // June-era cache can hold 373+ raw keys while covering fewer real pages than a
            // modern, normalized 372-entry edition — and raw counting would let that ancient
            // cache win the keep slot over the complete immediately-previous edition on EVERY
            // future deploy, a self-perpetuating pin until someone clears site storage.
            const coverage = new Set(
              entries
                .filter((r) => r.url.includes("/pages/"))
                .map((r) => new URL(r.url).pathname),
            );
            return { key, index, count: coverage.size };
          } catch (_) {
            return { key, index, count: 0 };
          }
        }),
      );
      const keepPrevious = counted
        .filter((c) => c.count > 0) // an empty cache is never worth a keep slot
        .sort((a, b) => b.count - a.count || b.index - a.index)
        .slice(0, PAGE_CACHES_TO_KEEP - 1)
        .map((c) => c.key);
      const pageCachesToKeep = new Set([PAGE_CACHE, ...keepPrevious]);

      const shellReady = await isShellCached();

      // MANIFEST SALVAGE, before any retiring static cache is deleted. STATIC_CACHE rotates on
      // every deploy, and deleting the old one used to destroy the device's ONLY cached book
      // manifests — the offline song index — until a full online precache completed (which after
      // a book bump means the entire page download finishing first). Copy the manifests forward
      // when the new cache lacks them: a stale manifest beats no manifest, exactly like the
      // kept previous-edition page cache, and SWR / the precache's strict step replace it with
      // fresh bytes on the next online run.
      try {
        const currentStatic = await caches.open(STATIC_CACHE);
        for (const manifestPath of BOOK_MANIFEST_PATHS) {
          if (await currentStatic.match(manifestPath)) continue;
          for (const key of keys) {
            if (!key.startsWith(STATIC_CACHE_PREFIX) || key === STATIC_CACHE) continue;
            const hit = await caches.match(manifestPath, { cacheName: key });
            if (hit) {
              await currentStatic.put(manifestPath, hit);
              break;
            }
          }
        }
      } catch (_) {
        // Salvage is best-effort; never let it block activation or cleanup.
      }

      await Promise.all(
        keys
          .filter((key) => {
            if (key.startsWith(PAGE_CACHE_PREFIX)) return !pageCachesToKeep.has(key);
            if (key.startsWith(STATIC_CACHE_PREFIX)) {
              // Never delete the current static cache; only retire OLD ones, and only once the new
              // shell is verified cached so we don't orphan offline reloads.
              return key !== STATIC_CACHE && shellReady;
            }
            // Any other (foreign/legacy) cache key is fair game to delete.
            return true;
          })
          .map((key) => caches.delete(key)),
      );
    })(),
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "SKIP_WAITING") {
    // app.js posts this on statechange='installed'. Honor the same shell-ready gate as install so
    // a shell-less (offline-installed) SW doesn't get pushed live and wipe the old shell.
    event.waitUntil(skipWaitingIfShellReady());
  }
  // Version handshake for the precache gate. app.js refuses to run ensureOfflineBundle until the
  // CONTROLLING SW answers with the SAME book version — otherwise, during the deploy skew window,
  // a NEW app.js running under an OLD (cache-first, pre-BOOK_VERSION) SW would have its precache
  // fetches answered instantly from the old SW's stale page cache and would launder previous-
  // edition bytes into the new book-keyed cache. Old SWs simply never reply; app.js treats
  // silence as "wrong SW, don't precache yet" and retries after the controllerchange reload.
  if (event.data?.type === "GET_BOOK_VERSION") {
    event.ports?.[0]?.postMessage({ bookVersion: BOOK_VERSION });
  }
});

// Matches "/books/standard/pages/page-NNN.webp" so page images use the dedicated PAGE_CACHE
// (consistent with app.js ensureOfflineBundle + the isOfflineBundleReady page count).
const isPageImageRequest = (requestUrl) => requestUrl.pathname.includes("/pages/");
const shouldCacheResponse = (response) => {
  if (!response) return false;
  const cacheControl = response.headers.get("cache-control") || "";
  return !cacheControl.toLowerCase().includes("no-store");
};

// PAGE-SLOT POISONING GUARD. Cloudflare Pages answers ANY unmatched path with the SPA shell —
// HTTP 200, `content-type: text/html`, the bytes of index.html — so a request for a page image
// that does not exist AT THIS PoP YET comes back "ok" with a document in it. Nothing else here
// can tell that apart from real pixels: shouldCacheResponse only looks for no-store. Written
// into PAGE_CACHE, that HTML poisons the page slot PERMANENTLY — the page branch below is
// cache-FIRST with no revalidation, so the page renders broken online AND offline until
// BOOK_VERSION changes, and inside the church there is no network and no remedy.
//
// The trigger is routine, not hypothetical: the shell (index.html / books.json / pages.json,
// which carry totalPages) and the page assets do NOT flip atomically at a PoP. Measured on the
// 2026-08-02 page-373 deploy, books.json read 372 at the staging alias for ~3 minutes while
// pages.json at the same host already read 373 — a device landing in that window asks for a page
// its PoP cannot serve and caches the SPA shell as that page, forever.
//
// The two predicates are deliberately ASYMMETRIC, because writing and serving carry opposite
// risks here:
//   - CACHING demands a POSITIVE `image/*`. A bad write is permanent and unrecoverable offline,
//     so anything we cannot positively identify as an image is not worth persisting.
//   - SERVING rejects only a POSITIVE non-image. A response we merely can't classify (no
//     content-type at all) is still the best bytes we have; demoting it below a previous edition
//     would trade fresh pixels for stale ones over a missing header.
// Scope is the PAGE branch only. The shell/manifest paths take the same SPA fallback, but they
// are stale-while-revalidate — a bad copy self-corrects on the next online load rather than
// sticking — so they are deliberately left alone.
const responseContentType = (response) =>
  (response?.headers?.get("content-type") || "").toLowerCase().trim();
const isCacheablePageImage = (response) => responseContentType(response).startsWith("image/");
const isNotPageImage = (response) => {
  const contentType = responseContentType(response);
  return contentType !== "" && !contentType.startsWith("image/");
};

// Look for a page image in any PREVIOUS book version's page cache (kept by activate). A hit is
// that page as of the PREVIOUS edition — right bytes for an unchanged page, stale bytes for a
// revised one — so this runs strictly AFTER the network has had its chance: it exists so a book
// bump (which starts the new PAGE_CACHE empty) can't strand an OFFLINE follower whose full bundle
// lives in the previous cache. ignoreSearch covers app.js's ?retry=/?reload= cache-busting params.
const matchAnyPageCache = async (request) => {
  // Newest-first: insertion order puts the freshest surviving edition LAST, and when several
  // caches hold the same URL the least-stale copy should win the fallback.
  const keys = (await caches.keys()).reverse();
  for (const key of keys) {
    if (key === PAGE_CACHE || !key.startsWith(PAGE_CACHE_PREFIX)) continue;
    // caches.match({cacheName}) instead of caches.open(): open() CREATES a cache that was
    // deleted between our keys() snapshot and now (activate cleanup racing a fallback scan),
    // and the resurrected empty husk — newest by insertion order — would later win the
    // activate keep slot over a full edition. match() never creates.
    const hit = await caches.match(request, { cacheName: key, ignoreSearch: true });
    // A PREVIOUS edition's cache can itself hold poison: it was written by a build that shipped
    // before the guard above existed, and a code-only deploy does NOT rotate PAGE_CACHE (the
    // name is keyed by BOOK_VERSION, the book's hash). Skip such an entry and keep scanning —
    // an older-but-real edition of the page is worth far more than a document.
    if (hit && isNotPageImage(hit)) continue;
    if (hit) {
      // Tag the response so callers can TELL it is previous-edition material. Display code
      // ignores the header; app.js's cacheSinglePage treats it as a failed download so these
      // bytes are never persisted into the current book's cache as if they were fresh
      // (defense-in-depth behind the no-store branch in the fetch handler).
      const headers = new Headers(hit.headers);
      headers.set("X-SV-Prev-Edition", "1");
      return new Response(hit.body, { status: hit.status, statusText: hit.statusText, headers });
    }
  }
  return null;
};

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;

  const requestUrl = new URL(event.request.url);
  if (requestUrl.origin !== self.location.origin) return;

  if (isPageImageRequest(requestUrl)) {
    event.respondWith(
      (async () => {
        // ignoreSearch so a RETRY/RELOAD of a page (app.js appends ?retry= / ?reload= to bust a
        // transient first-load failure) still hits the precached BARE page-NNN.webp. Without it,
        // every offline retry of an already-cached page would miss and hit the dead network —
        // the recovery path that exists to fix transient failures would be a guaranteed miss
        // offline, stranding the follower on "No se pudo cargar esta página." for a cached page.
        // A hit here is CURRENT-edition bytes by construction (PAGE_CACHE is book-keyed and only
        // ever filled from the network below / app.js's precache), so serving it instantly is safe.
        // Bare, query-stripped key. Hoisted above the cache-first read because the self-heal
        // below has to purge it explicitly, not only via ignoreSearch.
        const cacheKey = new Request(requestUrl.origin + requestUrl.pathname);
        const cache = await caches.open(PAGE_CACHE);
        const cached = await cache.match(event.request, { ignoreSearch: true });
        // SELF-HEAL, before the cache-first return. A slot poisoned by an earlier build (the
        // guard below is new; the caches on already-deployed devices are not) would otherwise be
        // served from here forever — this branch is cache-first precisely so it never revalidates,
        // and PAGE_CACHE is keyed by BOOK_VERSION, so shipping this file does NOT rotate it. Evict
        // and fall through to the network: online the slot refills with real bytes on this very
        // request; offline we land on the previous-edition fallback or the page's own retry UI,
        // which is where a broken render already left the follower, only now it is repairable.
        // ignoreSearch on the delete purges every ?retry=/?reload= variant of the same page.
        // Cost on the healthy path is one header read on an already-resolved Response.
        //
        // AWAITED, not waitUntil: the refill below writes the SAME key, so a delete still in
        // flight when the network lands would erase the good bytes it just replaced them with,
        // re-arming the exact bug on the next page turn. Sequencing costs one CacheStorage
        // delete, and only ever on the poisoned path — the healthy path never reaches here.
        //
        // BOTH delete forms on purpose. ignoreSearch is the one that sweeps ?retry=/?reload=
        // variants, but it is the least-exercised corner of the Cache API on the old iPads this
        // fleet runs; if a browser no-ops it, deleting only the exact (possibly query-bearing)
        // request would leave the bare entry poisoned while the refill writes the bare key —
        // and the next ignoreSearch READ could still return the stale variant first, healing and
        // re-downloading the page on every single view. Naming the bare key explicitly makes the
        // repair terminate no matter how ignoreSearch behaves.
        if (cached && isNotPageImage(cached)) {
          await Promise.all([
            cache.delete(event.request, { ignoreSearch: true }),
            cache.delete(cacheKey),
          ]).catch(() => {});
        } else if (cached) {
          return cached;
        }

        // Miss → NETWORK FIRST, then previous-edition caches. Order is load-bearing: the old
        // order (previous caches first) meant a page revised under an unchanged filename could
        // never reach an already-cached device — even the re-precache was answered from the old
        // cache, copying stale bytes forward forever. Offline cost: none — a dead network rejects
        // in milliseconds and we land on the fallback exactly like before. Weak-signal cost is
        // bounded by PAGE_NETWORK_TIMEOUT_MS, and the race is on response HEADERS (TTFB), not on
        // the full body: the winning response streams to the page while the cache write happens
        // in the background, so a slow-but-alive network never stalls a page turn on the put.
        let networkError = null;
        const fetched = fetch(event.request).catch((error) => {
          networkError = error;
          return null; // settled-but-failed marker
        });
        // Persist in the background whenever the fetch succeeds. clone() is taken in the same
        // microtask turn the response settles in — before the page starts consuming the body.
        // A failed put (storage quota, private mode) must NEVER cost the page its bytes: the
        // network delivered them; caching is best-effort. Normalized to the bare URL (drop any
        // ?retry=/?reload=) to stay deduplicated and consistent with getCachedPageSet.
        const putChain = fetched.then(async (response) => {
          if (!response || !response.ok || !shouldCacheResponse(response)) return;
          // The poisoning guard, on the only SW-side writer of PAGE_CACHE. A 200 text/html SPA
          // fallback stops here instead of becoming this page slot's permanent contents.
          if (!isCacheablePageImage(response)) return;
          try {
            await cache.put(cacheKey, response.clone());
          } catch (_) {
            /* best-effort */
          }
        });
        event.waitUntil(putChain);

        // PRECACHE CONTRACT: app.js's cacheSinglePage — the ONLY writer that persists page bytes
        // app-side — fetches with {cache:"no-store"}. Those requests must NEVER be answered from
        // a previous edition: the caller's entire purpose is to install CURRENT-edition bytes,
        // and a stale 200 here would be written into the new book's cache and certified as ready
        // (the original drift bug, reintroduced through the app's side door). Fail honestly and
        // let ensureOfflineBundle's catch retry when the network is real. Live <img> loads use
        // the default cache mode and never take this branch.
        if (event.request.cache === "no-store") {
          const settled = await fetched;
          if (settled) return settled; // non-ok included: cacheSinglePage's !ok check handles it
          throw networkError;
        }

        const timedOut = Symbol("timeout");
        const winner = await Promise.race([
          fetched,
          new Promise((resolve) => setTimeout(() => resolve(timedOut), PAGE_NETWORK_TIMEOUT_MS)),
        ]);
        // A 200 text/html SPA fallback is NOT this page — treat it exactly like a non-ok
        // response and let the previous edition below have its turn. If nothing is cached
        // anywhere it is still returned as the last resort at the bottom, where the <img>
        // onerror handler turns it into the page's own retry UI.
        if (winner !== timedOut && winner && winner.ok && !isNotPageImage(winner)) return winner;

        // Network failed / non-ok / not-an-image / slow-to-first-byte → previous edition from an older cache
        // (tagged X-SV-Prev-Edition by matchAnyPageCache). A slow fetch that eventually lands
        // still reaches the cache via putChain, so the NEXT view of this page is current.
        const fallback = await matchAnyPageCache(event.request);
        if (fallback) return fallback;

        // No cached copy anywhere: the network is all we have — give it its full chance.
        const settled = await fetched;
        if (settled) return settled; // includes non-ok responses: let the page's own retry UI run
        throw networkError; // fetch rejected outright — surfaces exactly like the old code's throw
      })(),
    );
    return;
  }

  // NO-STORE CONTRACT, non-page half. no-store marks cache WRITERS and network probes ONLY:
  // app.js's cacheAssetsNoStore (shell healer + strict manifest step), and the ?selftest
  // card's connectivity probes (which SHOULD see the real network — serving those from cache
  // made the selftest lie about network health). These requests exist to obtain verified-fresh
  // bytes, so they go straight to the network and fail honestly offline; a cache-first answer
  // here is how an old edition's pages.json could be laundered into the new static cache.
  // Display READERS — song-index hydration, search-index, the boot manifest fallback — use the
  // default cache mode by contract (app.js documents each) and are served by the network-first
  // branch below: fresh online, cached offline. (Page images have their own no-store handling
  // in the page branch above; cross-origin relay traffic never reaches this listener.)
  if (event.request.cache === "no-store") {
    event.respondWith(fetch(event.request));
    return;
  }

  // Stale-while-revalidate: serve the cached copy INSTANTLY (critical on weak cell —
  // never wait on the network for assets we already have), then refresh the cache in
  // the background so the next load is current. A deploy is thus picked up one load
  // later, which is safe: the director's live page arrives over the relay WebSocket,
  // not these cached files. Cold cache falls back to network; dead network to cache.
  if (NETWORK_FIRST_PATHS.has(requestUrl.pathname)) {
    const revalidate = caches.open(STATIC_CACHE).then(async (cache) => {
      try {
        const response = await fetch(event.request);
        if (response.ok && shouldCacheResponse(response)) {
          await cache.put(event.request, response.clone());
        }
        return response;
      } catch (error) {
        return null;
      }
    });
    event.waitUntil(revalidate);
    event.respondWith(
      caches.match(event.request).then(
        (cached) => cached || revalidate.then((response) => response || fetch(event.request)),
      ),
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then(async (cached) => {
      if (cached) return cached;
      const response = await fetch(event.request);
      if (response.ok && shouldCacheResponse(response)) {
        const cache = await caches.open(STATIC_CACHE);
        cache.put(event.request, response.clone());
      }
      return response;
    }),
  );
});
