#!/usr/bin/env node
/**
 * serve-web-dist.mjs — serve web/dist the way Cloudflare Pages actually serves it.
 *
 * The point is the LAST rule: any unmatched path returns HTTP 200 with index.html and
 * `content-type: text/html`, not a 404. That single behavior is what poisoned the page cache
 * (a request for a page image that does not exist yet comes back "ok" with a document in it),
 * and no off-the-shelf static server reproduces it by default — `npx serve` 404s, which makes
 * the bug invisible locally and is why it was only ever seen against a real deploy.
 *
 * So: reproducing the production fallback is the whole reason this file exists. Use it to
 * verify page-cache behavior in a real browser with a real service worker, where the
 * node:test harness (e2e/helpers/sw-harness.mjs) can only simulate CacheStorage.
 *
 * Zero dependencies on purpose — it must run in a fresh worktree with nothing installed.
 *
 * Usage:  node scripts/serve-web-dist.mjs [--port 4000] [--dir web/dist]
 * A service worker needs a secure context; http://localhost qualifies, so plain HTTP is fine.
 */
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const argOf = (flag, fallback) => {
  const index = process.argv.indexOf(flag);
  return index > -1 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
};
const PORT = Number(argOf("--port", 4000));
const DIST = path.resolve(ROOT, argOf("--dir", path.join("web", "dist")));

const CONTENT_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".webmanifest": "application/manifest+json",
  ".webp": "image/webp",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
};

if (!fs.existsSync(path.join(DIST, "index.html"))) {
  console.error(`✖ ${DIST} has no index.html — run \`node web/build.mjs\` first.`);
  process.exit(1);
}

const server = http.createServer((request, response) => {
  let pathname;
  try {
    ({ pathname } = new URL(request.url, `http://localhost:${PORT}`));
  } catch {
    pathname = "/";
  }
  pathname = decodeURIComponent(pathname);

  // Contain the resolved path inside DIST — a served directory must never be an escape hatch.
  const resolved = path.resolve(DIST, `.${pathname}`);
  const candidate = resolved.startsWith(DIST)
    ? (pathname.endsWith("/") ? path.join(resolved, "index.html") : resolved)
    : null;

  if (candidate && fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
    const type = CONTENT_TYPES[path.extname(candidate).toLowerCase()] || "application/octet-stream";
    response.writeHead(200, { "content-type": type });
    fs.createReadStream(candidate).pipe(response);
    return;
  }

  // THE CLOUDFLARE PAGES SPA FALLBACK — 200 + the shell for anything unmatched, including a
  // page image that does not exist. This is the production behavior the page-cache guards in
  // sw.js and app.js exist to survive; serving a 404 here would hide the very thing being tested.
  console.log(`  SPA fallback (200 text/html): ${pathname}`);
  response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
  fs.createReadStream(path.join(DIST, "index.html")).pipe(response);
});

server.listen(PORT, () => {
  console.log(`serving ${DIST}`);
  console.log(`  http://localhost:${PORT}  (unmatched paths return 200 + index.html, like Pages)`);
});
