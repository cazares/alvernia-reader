import test from "node:test";
import assert from "node:assert/strict";

import {
  buildDirectorSyncPayload,
  buildProxyUrl,
  buildTunnelUrl,
  isDirectorSyncStateStale,
  isValidDirectorPage,
  isValidDirectorTotalPages,
  normalizeDirectorSessionCode,
  isSpecialRoute,
  isPageAssetRoute,
  normalizeProxyPath,
} from "../cloudflare/alvernia-link/src/index.js";

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

test("normalizeProxyPath keeps signovino.com at the domain root", () => {
  assert.deepEqual(normalizeProxyPath({ host: "signovino.com", pathname: "/pages/page-052.jpg" }), {
    redirectToTrailingSlash: false,
    proxiedPath: "/pages/page-052.jpg",
  });
});

test("buildProxyUrl forwards root and assets to the live Pages deployment", () => {
  assert.equal(
    buildProxyUrl("https://signovino.com/").toString(),
    "https://alvernia-reader.pages.dev/",
  );

  assert.equal(
    buildProxyUrl("https://signovino.com/pages.json?cache=1").toString(),
    "https://alvernia-reader.pages.dev/pages.json?cache=1",
  );
});

test("buildTunnelUrl targets the upload tunnel origin", () => {
  assert.equal(
    buildTunnelUrl("https://signovino.com/upload?foo=1", "https://upload.signovino.com").toString(),
    "https://upload.signovino.com/upload?foo=1",
  );
});

test("special routes stay on the worker", () => {
  assert.equal(isSpecialRoute("/upload"), true);
  assert.equal(isSpecialRoute("/promote"), true);
  assert.equal(isSpecialRoute("/download"), true);
  assert.equal(isSpecialRoute("/director-sync"), true);
  assert.equal(isSpecialRoute("/"), false);
});

test("page asset routes are identified for R2 overrides", () => {
  assert.equal(isPageAssetRoute("/pages.json"), true);
  assert.equal(isPageAssetRoute("/pages/page-001.jpg"), true);
  assert.equal(isPageAssetRoute("/app.js"), false);
});

test("director sync session codes are compact and uppercase", () => {
  assert.equal(normalizeDirectorSessionCode(" coro 2026!! "), "CORO2026");
  assert.equal(normalizeDirectorSessionCode("abcdefghijklmnop"), "ABCDEFGHIJKL");
});

test("director sync route stays special even with trailing slash normalization", () => {
  assert.equal(isSpecialRoute("/director-sync/"), true);
});

test("director sync payload marks an active director session", () => {
  assert.deepEqual(
    buildDirectorSyncPayload("CORO2026", {
      page: 7,
      totalPages: 200,
      updatedAt: "2026-03-29T00:00:00.000Z",
    }),
    {
      session: "CORO2026",
      page: 7,
      totalPages: 200,
      updatedAt: "2026-03-29T00:00:00.000Z",
      directorPresent: true,
    },
  );
});

test("director sync validation rejects invalid page metadata", () => {
  assert.equal(isValidDirectorPage(1), true);
  assert.equal(isValidDirectorPage(0), false);
  assert.equal(isValidDirectorPage(1.5), false);
  assert.equal(isValidDirectorTotalPages(0), true);
  assert.equal(isValidDirectorTotalPages(-1), false);
});

test("director sync stale detection expires old sessions", () => {
  assert.equal(isDirectorSyncStateStale({
    updatedAt: new Date(Date.now() - 60_000).toISOString(),
  }), true);
  assert.equal(isDirectorSyncStateStale({
    updatedAt: new Date().toISOString(),
  }), false);
});
