const CACHE_VERSION = "__CACHE_VERSION__";
const STATIC_CACHE = `signo-vivo-static-${CACHE_VERSION}`;
const PAGE_CACHE = `signo-vivo-pages-${CACHE_VERSION}`;
// NOTE: per-book manifests (/books/<id>/pages.json, /books/<id>/search-index.json) are NOT
// listed here — they don't exist at the root in the multi-book build and are cached by
// app.js coreAssetsForBook() at their real /books/<id>/ paths. Listing the old single-book
// root paths here previously risked aborting the atomic install precache (404 → addAll rejects).
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

// NOTE: bulk offline precaching lives in app.js (ensureOfflineBundle), which is
// multi-book-aware (caches /books/<book>/pages/... into PAGE_CACHE per book). The old
// SW-side single-book backgroundCacheAllPages() helper was dead code with stale
// /pages.json + /pages/ paths and has been removed to avoid confusion.

self.addEventListener("install", (event) => {
  // Resilient precache: cache each core asset independently so a single failure (404 /
  // offline) can never abort the whole install the way the atomic cache.addAll() would.
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) =>
      Promise.allSettled(CORE_ASSETS.map((asset) => cache.add(asset))),
    ),
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  // Clean up old-version caches. We deliberately do NOT pre-fetch all 370 pages
  // anymore — that ~34 MB background download froze first loads. Followers cache
  // pages on demand (cache-first handler below); the offline iPad still preloads
  // the whole manual via signovivo.com?admin=1.
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys
        .filter((key) => ![STATIC_CACHE, PAGE_CACHE].includes(key))
        .map((key) => caches.delete(key)),
    )),
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

// Matches both the legacy single-book "/pages/page-NNN.webp" and the multi-book
// "/books/<book>/pages/page-NNN.webp" so page images use the dedicated PAGE_CACHE
// (consistent with app.js ensureOfflineBundle + the isOfflineBundleReady page count).
const isPageImageRequest = (requestUrl) => requestUrl.pathname.includes("/pages/");
const shouldCacheResponse = (response) => {
  if (!response) return false;
  const cacheControl = response.headers.get("cache-control") || "";
  return !cacheControl.toLowerCase().includes("no-store");
};

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;

  const requestUrl = new URL(event.request.url);
  if (requestUrl.origin !== self.location.origin) return;

  if (isPageImageRequest(requestUrl)) {
    event.respondWith(
      caches.open(PAGE_CACHE).then(async (cache) => {
        const cached = await cache.match(event.request);
        if (cached) return cached;
        const response = await fetch(event.request);
        if (response.ok && shouldCacheResponse(response)) {
          cache.put(event.request, response.clone());
        }
        return response;
      }),
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
