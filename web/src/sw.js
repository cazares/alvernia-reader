const STATIC_CACHE = "alvernia-static-v10";
const PAGE_CACHE = "alvernia-pages-v10";
const CORE_ASSETS = [
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  "/pages.json",
  "/search-index.json",
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
  "/pages.json",
  "/search-index.json",
]);

const backgroundCacheAllPages = async () => {
  try {
    const manifest = await fetch("/pages.json").then((r) => r.json());
    const cache = await caches.open(PAGE_CACHE);
    for (let i = 1; i <= manifest.totalPages; i++) {
      const url = `/pages/page-${String(i).padStart(3, "0")}.jpg`;
      if (!(await cache.match(url))) {
        try {
          const res = await fetch(url);
          if (res.ok) await cache.put(url, res);
        } catch {
          // skip individual failures silently
        }
      }
    }
  } catch {
    // ignore if offline or manifest unavailable
  }
};

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(CORE_ASSETS)),
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys
          .filter((key) => ![STATIC_CACHE, PAGE_CACHE].includes(key))
          .map((key) => caches.delete(key)),
      ))
      .then(() => backgroundCacheAllPages()),
  );
  self.clients.claim();
});

const isPageImageRequest = (requestUrl) => requestUrl.pathname.startsWith("/pages/");
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

  if (NETWORK_FIRST_PATHS.has(requestUrl.pathname)) {
    event.respondWith(
      caches.open(STATIC_CACHE).then(async (cache) => {
        try {
          const response = await fetch(event.request);
          if (response.ok && shouldCacheResponse(response)) {
            cache.put(event.request, response.clone());
          }
          return response;
        } catch (error) {
          const cached = await cache.match(event.request);
          if (cached) return cached;
          throw error;
        }
      }),
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
