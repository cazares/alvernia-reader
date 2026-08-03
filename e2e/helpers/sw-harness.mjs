/**
 * sw-harness.mjs — run the REAL web/src/sw.js in Node and drive its fetch handler.
 *
 * Why this exists: sw.js is the most load-bearing file in the repo, and every guard in it
 * (previous-edition tagging, the no-store precache contract, book-keyed page caches, the
 * page-slot poisoning guard) is a BEHAVIOR. The other checks we have on it — smoke-boot.mjs —
 * are string greps over the built bundle: they prove a line still exists, not that it still
 * works. A grep passes happily while the condition around it is inverted.
 *
 * So this loads the actual source (build tokens substituted exactly as web/build.mjs does),
 * hands it a stub `self` / `caches` / `fetch`, and lets a test dispatch a synthetic FetchEvent
 * and then look at what really landed in CacheStorage.
 *
 * Deliberately dependency-free and realm-honest: Request/Response/Headers/URL are Node's own
 * (Node 18+ ships them globally), so content-type handling, ok/status semantics and body
 * cloning behave the way they do in a browser rather than the way a hand-rolled fake would.
 *
 * The stubs cover only the CacheStorage surface sw.js actually uses. Add to them rather than
 * loosening a test if a future sw.js change needs more.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
// SW_TEST_SRC overrides the inspected source directory, exactly like smoke-boot's SMOKE_DIST.
// scripts/verify-sw-page-cache-guards.mjs points it at a mutated COPY to prove these tests
// actually fail when the guards are reverted — the only honest evidence that they have teeth.
export const SRC_DIR = process.env.SW_TEST_SRC
  ? path.resolve(process.env.SW_TEST_SRC)
  : path.join(ROOT, "web", "src");
const SW_SOURCE_PATH = path.join(SRC_DIR, "sw.js");

export const ORIGIN = "https://signovivo.test";

/** Full URL string for a Request-or-string, resolved against the harness origin. */
const urlOf = (request) =>
  typeof request === "string" ? new URL(request, ORIGIN).toString() : request.url;

/** Same URL with any ?retry=/?reload= stripped — how ignoreSearch compares keys. */
const withoutSearch = (url) => {
  const parsed = new URL(url);
  parsed.search = "";
  return parsed.toString();
};

class StubCache {
  constructor(name, { brokenIgnoreSearchDelete = false } = {}) {
    this.name = name;
    /** @type {Map<string, Response>} insertion-ordered, like a real Cache */
    this.entries = new Map();
    // Simulates a browser that accepts CacheQueryOptions on delete() and quietly ignores them.
    // ignoreSearch is the least-exercised corner of the Cache API, and this fleet runs iOS 15
    // iPads; code that depends on it should be proven to still terminate without it.
    this.brokenIgnoreSearchDelete = brokenIgnoreSearchDelete;
  }

  async put(request, response) {
    this.entries.set(urlOf(request), response);
  }

  async match(request, options = {}) {
    const url = urlOf(request);
    let hit = this.entries.get(url);
    if (!hit && options.ignoreSearch) {
      const bare = withoutSearch(url);
      for (const [key, value] of this.entries) {
        if (withoutSearch(key) === bare) {
          hit = value;
          break;
        }
      }
    }
    // A real Cache hands out a fresh Response every time; returning the stored one would let a
    // single body read poison every later match and make tests lie about which path ran.
    return hit ? hit.clone() : undefined;
  }

  async keys() {
    return [...this.entries.keys()].map((url) => new Request(url));
  }

  async delete(request, options = {}) {
    const url = urlOf(request);
    if (this.entries.delete(url)) return true;
    if (!options.ignoreSearch || this.brokenIgnoreSearchDelete) return false;
    const bare = withoutSearch(url);
    let deleted = false;
    for (const key of [...this.entries.keys()]) {
      if (withoutSearch(key) === bare) {
        this.entries.delete(key);
        deleted = true;
      }
    }
    return deleted;
  }

  async add(request) {
    // install-time precache. Tests that care about install seed caches directly instead.
    this.entries.set(urlOf(request), new Response("stub", { status: 200 }));
  }
}

class StubCacheStorage {
  constructor(cacheOptions = {}) {
    /** @type {Map<string, StubCache>} insertion order is load-bearing: matchAnyPageCache
     * reverses caches.keys() to prefer the freshest surviving edition. */
    this.caches = new Map();
    this.cacheOptions = cacheOptions;
  }

  async open(name) {
    if (!this.caches.has(name)) this.caches.set(name, new StubCache(name, this.cacheOptions));
    return this.caches.get(name);
  }

  async keys() {
    return [...this.caches.keys()];
  }

  async has(name) {
    return this.caches.has(name);
  }

  async delete(name) {
    return this.caches.delete(name);
  }

  async match(request, options = {}) {
    if (options.cacheName) {
      // Must NOT create the cache — sw.js relies on match() never resurrecting a deleted one.
      const cache = this.caches.get(options.cacheName);
      return cache ? cache.match(request, options) : undefined;
    }
    for (const cache of this.caches.values()) {
      const hit = await cache.match(request, options);
      if (hit) return hit;
    }
    return undefined;
  }
}

/**
 * Load sw.js into a fresh sandbox.
 *
 * @param {{cacheVersion?: string, bookVersion?: string}} [options]
 * @returns a handle exposing the stub CacheStorage, a settable network, and dispatchFetch().
 */
export const loadServiceWorker = ({
  cacheVersion = "shell-test",
  bookVersion = "book-test",
  brokenIgnoreSearchDelete = false,
} = {}) => {
  const source = fs
    .readFileSync(SW_SOURCE_PATH, "utf8")
    // Exactly the substitution web/build.mjs performs, so we exercise shipped code.
    .replaceAll("__CACHE_VERSION__", cacheVersion)
    .replaceAll("__BOOK_VERSION__", bookVersion);

  const listeners = new Map();
  const cacheStorage = new StubCacheStorage({ brokenIgnoreSearchDelete });
  /** Requests the SW actually put on the wire — lets a test prove the network was consulted. */
  const networkRequests = [];
  /** Default network: offline. Tests opt into a response with setNetwork(). */
  let network = async () => {
    throw new TypeError("Failed to fetch");
  };

  const self = {
    location: new URL(ORIGIN),
    addEventListener: (type, handler) => {
      listeners.set(type, handler);
    },
    skipWaiting: async () => {},
    clients: { claim: async () => {} },
    registration: {},
  };

  const fetchStub = async (request, init) => {
    networkRequests.push(urlOf(request));
    return network(request, init);
  };

  // new Function over vm: sw.js is plain top-level script, and staying in this realm keeps
  // Response/Headers/URL identity intact (cross-realm instanceof would silently misbehave).
  // eslint-disable-next-line no-new-func
  const evaluate = new Function("self", "caches", "fetch", source);
  evaluate(self, cacheStorage, fetchStub);

  const pageCacheName = `signo-vivo-pages-${bookVersion}`;
  const staticCacheName = `signo-vivo-static-${cacheVersion}`;

  return {
    caches: cacheStorage,
    pageCacheName,
    staticCacheName,
    networkRequests,
    setNetwork: (impl) => {
      network = impl;
    },
    /** Seed a cache entry the way a previous deploy would have left one. */
    seed: async (cacheName, url, response) => {
      const cache = await cacheStorage.open(cacheName);
      await cache.put(new URL(url, ORIGIN).toString(), response);
    },
    /** Every URL currently held by a cache, in insertion order. */
    entriesOf: async (cacheName) => {
      const cache = cacheStorage.caches.get(cacheName);
      return cache ? [...cache.entries.keys()] : [];
    },
    /** Read a cached entry back as {status, contentType, body}. */
    readCached: async (cacheName, url) => {
      const cache = cacheStorage.caches.get(cacheName);
      if (!cache) return null;
      const hit = await cache.match(new URL(url, ORIGIN).toString(), { ignoreSearch: true });
      if (!hit) return null;
      return {
        status: hit.status,
        contentType: hit.headers.get("content-type"),
        body: await hit.text(),
      };
    },
    /**
     * Dispatch a GET through the SW's fetch listener and settle everything it started.
     * Resolves to {handled, response, error} — `error` captures a rejected respondWith,
     * which is how the SW surfaces "network died and nothing was cached".
     */
    dispatchFetch: async (url, init = {}) => {
      const handler = listeners.get("fetch");
      if (!handler) throw new Error("sw.js registered no fetch listener");
      const request = new Request(new URL(url, ORIGIN).toString(), { method: "GET", ...init });
      const pending = [];
      let responsePromise = null;
      handler({
        request,
        respondWith: (value) => {
          responsePromise = Promise.resolve(value);
        },
        waitUntil: (value) => {
          // Swallow rejections here exactly like a browser does — a failed background job must
          // not turn into an unhandled rejection that fails the whole test run.
          pending.push(Promise.resolve(value).catch(() => {}));
        },
      });
      let response = null;
      let error = null;
      if (responsePromise) {
        try {
          response = await responsePromise;
        } catch (thrown) {
          error = thrown;
        }
      }
      // Let waitUntil work (the background cache write) finish before assertions read the cache.
      await Promise.all(pending);
      return { handled: Boolean(responsePromise), response, error };
    },
  };
};

/** A response shaped like Cloudflare Pages' SPA fallback for an unmatched path. */
export const spaFallbackResponse = (body = "<!doctype html><html><body>SignoVivo</body></html>") =>
  new Response(body, {
    status: 200,
    headers: { "content-type": "text/html; charset=utf-8" },
  });

/** A response shaped like a real rendered page image. */
export const pageImageResponse = (body = "RIFF....WEBPVP8 fake-pixels") =>
  new Response(body, { status: 200, headers: { "content-type": "image/webp" } });
