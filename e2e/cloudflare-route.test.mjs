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

test("normalizeProxyPath keeps miguelbase.com at the domain root", () => {
  assert.deepEqual(normalizeProxyPath({ host: "miguelbase.com", pathname: "/pages/page-052.jpg" }), {
    redirectToTrailingSlash: false,
    proxiedPath: "/pages/page-052.jpg",
  });
});

test("buildProxyUrl forwards root and assets to the live Pages deployment", () => {
  assert.equal(
    buildProxyUrl("https://miguelbase.com/").toString(),
    "https://dev-standalone-alvernia-read.alvernia-reader.pages.dev/",
  );

  assert.equal(
    buildProxyUrl("https://miguelbase.com/pages.json?cache=1").toString(),
    "https://dev-standalone-alvernia-read.alvernia-reader.pages.dev/pages.json?cache=1",
  );
});
