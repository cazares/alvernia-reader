import test from "node:test";
import assert from "node:assert/strict";

import { buildProxyUrl, normalizeProxyPath } from "../cloudflare/alvernia-link/src/index.js";

test("normalizeProxyPath redirects bare /alvernia to a trailing slash", () => {
  assert.deepEqual(normalizeProxyPath({ host: "miguelengineer.com", pathname: "/alvernia" }), {
    redirectToTrailingSlash: true,
    proxiedPath: "/",
  });
});

test("normalizeProxyPath keeps nested asset paths under the reader route", () => {
  assert.deepEqual(normalizeProxyPath({ host: "miguelengineer.com", pathname: "/alvernia/pages/page-052.jpg" }), {
    redirectToTrailingSlash: false,
    proxiedPath: "/pages/page-052.jpg",
  });
});

test("normalizeProxyPath keeps miguelcoro.com at the domain root", () => {
  assert.deepEqual(normalizeProxyPath({ host: "miguelcoro.com", pathname: "/pages/page-052.jpg" }), {
    redirectToTrailingSlash: false,
    proxiedPath: "/pages/page-052.jpg",
  });
});

test("normalizeProxyPath still accepts legacy root domains", () => {
  assert.deepEqual(normalizeProxyPath({ host: "miguelbase.com", pathname: "/pages/page-052.jpg" }), {
    redirectToTrailingSlash: false,
    proxiedPath: "/pages/page-052.jpg",
  });
});

test("buildProxyUrl forwards root and assets to the live Pages deployment", () => {
  assert.equal(
    buildProxyUrl("https://miguelcoro.com/").toString(),
    "https://alvernia-reader.pages.dev/",
  );

  assert.equal(
    buildProxyUrl("https://miguelcoro.com/pages.json?cache=1").toString(),
    "https://alvernia-reader.pages.dev/pages.json?cache=1",
  );
});
