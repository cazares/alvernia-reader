import assert from "node:assert/strict";
import test from "node:test";

import worker, { isBodylessMethod, isMediaPath, normalizeOrigin } from "../src/index.js";

test("normalizeOrigin trims whitespace and trailing slash", () => {
  assert.equal(normalizeOrigin(" https://example.com/ "), "https://example.com");
});

test("isBodylessMethod only returns true for GET/HEAD", () => {
  assert.equal(isBodylessMethod("GET"), true);
  assert.equal(isBodylessMethod("HEAD"), true);
  assert.equal(isBodylessMethod("POST"), false);
});

test("isMediaPath identifies media endpoints", () => {
  assert.equal(isMediaPath("/output/temp/example.mp4"), true);
  assert.equal(isMediaPath("/files/separated/foo.wav"), true);
  assert.equal(isMediaPath("/health"), false);
});

test("proxy forwards request to configured upstream", async () => {
  const originalFetch = globalThis.fetch;
  let capturedRequest = null;
  let capturedInit = null;
  globalThis.fetch = async (request, init) => {
    capturedRequest = request;
    capturedInit = init;
    return new Response('{"ok":true}', {
      status: 200,
      headers: {
        "content-type": "application/json",
      },
    });
  };

  try {
    const request = new Request("https://api.miguelendpoint.com/health?deep=1", {
      method: "GET",
      headers: {
        "cf-connecting-ip": "203.0.113.1",
        connection: "keep-alive",
      },
    });
    const response = await worker.fetch(request, {
      API_ORIGIN: "https://upstream.example.com/",
    });

    assert.ok(capturedRequest);
    assert.equal(capturedRequest.url, "https://upstream.example.com/health?deep=1");
    assert.equal(capturedRequest.headers.get("host"), "upstream.example.com");
    assert.equal(capturedRequest.headers.get("x-forwarded-host"), "api.miguelendpoint.com");
    assert.equal(capturedRequest.headers.get("x-forwarded-for"), "203.0.113.1");
    assert.equal(capturedRequest.headers.get("connection"), null);
    assert.equal(capturedInit, undefined);

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("x-mixterious-api-origin"), "https://upstream.example.com");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("proxy bypasses cache and marks media responses as no-store", async () => {
  const originalFetch = globalThis.fetch;
  let capturedRequest = null;
  let capturedInit = null;
  globalThis.fetch = async (request, init) => {
    capturedRequest = request;
    capturedInit = init;
    return new Response("video", {
      status: 200,
      headers: {
        "content-type": "video/mp4",
        etag: "\"abc123\"",
        "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT",
      },
    });
  };

  try {
    const response = await worker.fetch(
      new Request("https://api.miguelendpoint.com/output/temp/sample.mp4", {
        headers: {
          "if-none-match": "\"abc\"",
          "if-modified-since": "Wed, 21 Oct 2015 07:28:00 GMT",
        },
      }),
      { API_ORIGIN: "https://upstream.example.com" }
    );

    assert.equal(capturedRequest.url, "https://upstream.example.com/output/temp/sample.mp4");
    assert.equal(capturedRequest.headers.get("range"), "bytes=0-");
    assert.equal(capturedRequest.headers.get("if-none-match"), null);
    assert.equal(capturedRequest.headers.get("if-modified-since"), null);
    assert.deepEqual(capturedInit, {
      cf: {
        cacheEverything: false,
        cacheTtl: 0,
      },
    });
    assert.equal(response.headers.get("cache-control"), "no-store, max-age=0");
    assert.equal(response.headers.get("pragma"), "no-cache");
    assert.equal(response.headers.get("cdn-cache-control"), "no-store");
    assert.equal(response.headers.get("cloudflare-cdn-cache-control"), "no-store");
    assert.equal(response.headers.get("etag"), null);
    assert.equal(response.headers.get("last-modified"), null);
    assert.equal(response.headers.get("accept-ranges"), "bytes");
    assert.equal(response.headers.get("x-mixterious-api-origin"), "https://upstream.example.com");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("proxy bypasses cache when range header is requested", async () => {
  const originalFetch = globalThis.fetch;
  let capturedInit = null;
  let capturedRequest = null;
  globalThis.fetch = async (request, init) => {
    capturedRequest = request;
    capturedInit = init;
    return new Response("ab", {
      status: 206,
      headers: {
        "content-type": "video/mp4",
        "content-range": "bytes 0-1/123",
      },
    });
  };

  try {
    const response = await worker.fetch(
      new Request("https://api.miguelendpoint.com/health", {
        headers: {
          range: "bytes=0-1",
        },
      }),
      { API_ORIGIN: "https://upstream.example.com" }
    );

    assert.deepEqual(capturedInit, {
      cf: {
        cacheEverything: false,
        cacheTtl: 0,
      },
    });
    assert.equal(capturedRequest.headers.get("range"), "bytes=0-1");
    assert.equal(response.status, 206);
    assert.equal(response.headers.get("cache-control"), "no-store, max-age=0");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("proxy returns 503 when upstream fetch throws", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("dial timeout");
  };

  try {
    const response = await worker.fetch(
      new Request("https://api.miguelendpoint.com/health"),
      { API_ORIGIN: "https://upstream.example.com" }
    );
    assert.equal(response.status, 503);

    const body = await response.json();
    assert.equal(body.error, "backend_unreachable");
    assert.equal(body.upstream, "https://upstream.example.com");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
