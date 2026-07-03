const CACHE_VERSION = "__CACHE_VERSION__";
const STATIC_CACHE = `signo-vivo-static-${CACHE_VERSION}`;
const PAGE_CACHE = `signo-vivo-pages-${CACHE_VERSION}`;
const STATIC_CACHE_PREFIX = "signo-vivo-static-";
const PAGE_CACHE_PREFIX = "signo-vivo-pages-";
// Page images are content-immutable (Cache-Control: max-age=31536000, immutable — see
// build.mjs _headers), so a precached page-NNN.webp from an OLDER cache version is byte-identical
// to the new one. We therefore KEEP previous page caches across a version bump (instead of
// deleting them in activate) and let the page handler fall back across them. This is what saves a
// follower who goes offline right after a deploy: the freshly-activated SW's own PAGE_CACHE is
// empty (app.js re-precaches into it lazily), but the previous version's full bundle is still
// there to serve. We retain the two newest page caches so storage can't grow unbounded across many
// deploys — the current (new) one plus the immediately-previous full one is all the rollover needs.
const PAGE_CACHES_TO_KEEP = 2;
// The shell assets a returning follower MUST have cached to load/reload offline. If the install
// precache can't populate these (e.g. install ran on the last bar of signal, then the network
// died), we must NOT let this half-baked SW take over from a fully-cached older one.
const CRITICAL_SHELL_ASSETS = ["/", "/index.html", "/styles.css", "/app.js"];
// NOTE: the book manifests (/books/standard/pages.json, /books/standard/search-index.json) are NOT
// listed here — they live under /books/standard/ and are cached by app.js at their real paths.
// Listing them here would risk aborting the atomic install precache if a fetch 404s.
const CORE_ASSETS = [
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  "/icon.png",
  "/icon-192.png",
  "/icon-512.png",
];
const NETWORK_FIRST_PATHS = new Set([
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
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
  //      full bundle here would strand a follower who goes offline right after a deploy. Pages are
  //      immutable, so the surviving old cache is byte-valid and the page handler falls back to it.
  //   2. STATIC caches: only delete the old shell once the NEW shell is actually cached. If we
  //      somehow activate without a complete shell (shouldn't happen — skipWaiting is gated above —
  //      but be defensive), keep the old static cache so offline reloads still find a shell.
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();

      const pageCaches = keys.filter((key) => key.startsWith(PAGE_CACHE_PREFIX));
      // caches.keys() preserves insertion order, so the tail is the newest. Always retain the
      // current PAGE_CACHE plus enough recent ones to total PAGE_CACHES_TO_KEEP.
      const pageCachesToKeep = new Set([PAGE_CACHE, ...pageCaches.slice(-PAGE_CACHES_TO_KEEP)]);

      const shellReady = await isShellCached();

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
});

// Matches "/books/standard/pages/page-NNN.webp" so page images use the dedicated PAGE_CACHE
// (consistent with app.js ensureOfflineBundle + the isOfflineBundleReady page count).
const isPageImageRequest = (requestUrl) => requestUrl.pathname.includes("/pages/");
const shouldCacheResponse = (response) => {
  if (!response) return false;
  const cacheControl = response.headers.get("cache-control") || "";
  return !cacheControl.toLowerCase().includes("no-store");
};

// Look for a page image in ANY surviving page cache (current + the previous version kept by
// activate). Pages are immutable, so a hit in an older version's cache is byte-identical. Used as
// a fallback so a deploy bump (which starts the new PAGE_CACHE empty) can't strand an offline
// follower whose full bundle lives in the previous cache. ignoreSearch covers retry/reload params.
const matchAnyPageCache = async (request) => {
  const keys = await caches.keys();
  for (const key of keys) {
    if (key === PAGE_CACHE || !key.startsWith(PAGE_CACHE_PREFIX)) continue;
    const cache = await caches.open(key);
    const hit = await cache.match(request, { ignoreSearch: true });
    if (hit) return hit;
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
        const cache = await caches.open(PAGE_CACHE);
        const cached = await cache.match(event.request, { ignoreSearch: true });
        if (cached) return cached;

        // Fall back across previous-version page caches (kept in activate). After a deploy the new
        // PAGE_CACHE is empty until app.js re-precaches; the prior version's full, immutable bundle
        // is still here. This is what keeps a follower who goes offline right after a bump working.
        const fallback = await matchAnyPageCache(event.request);
        if (fallback) return fallback;

        const response = await fetch(event.request);
        if (response.ok && shouldCacheResponse(response)) {
          // Normalize the key to the bare URL (drop any ?retry=/?reload=) so cache entries stay
          // deduplicated and consistent with app.js's getCachedPageSet (which counts by pathname).
          const cacheKey = new Request(requestUrl.origin + requestUrl.pathname);
          cache.put(cacheKey, response.clone());
        }
        return response;
      })(),
    );
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
