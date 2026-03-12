import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");
const webSrcDir = path.join(APP_ROOT, "web", "src");

const readText = (relativePath) => fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8");

test("web shell includes iOS standalone metadata and native-like controls", () => {
  const source = readText("web/src/index.html");

  assert.match(source, /apple-mobile-web-app-capable/);
  assert.match(source, /apple-mobile-web-app-status-bar-style/);
  assert.match(source, /id="launch-screen"/);
  assert.match(source, /id="launch-fullscreen"/);
  assert.match(source, /id="launch-window"/);
  assert.match(source, /id="install-button"/);
  assert.match(source, /id="window-button"/);
  assert.match(source, /id="fullscreen-button"/);
  assert.match(source, /id="share-button"/);
  assert.match(source, /id="top-chrome"/);
  assert.match(source, /id="bottom-chrome"/);
});

test("web app script supports install, share, fullscreen, new windows, and deep links", () => {
  const source = readText("web/src/app.js");

  assert.match(source, /beforeinstallprompt/);
  assert.match(source, /navigator\.share/);
  assert.match(source, /requestFullscreen/);
  assert.match(source, /window\.open/);
  assert.match(source, /serviceWorker\.register/);
  assert.match(source, /searchParams\.get\("song"\)/);
  assert.match(source, /searchParams\.get\("mode"\)/);
  assert.match(source, /history\.replaceState/);
});

test("manifest is configured for standalone install from the domain root", () => {
  const manifest = JSON.parse(readText("web/src/manifest.webmanifest"));

  assert.equal(manifest.display, "standalone");
  assert.equal(manifest.scope, "/");
  assert.equal(manifest.start_url, "/");
  assert.equal(manifest.orientation, "any");
  assert.equal(manifest.icons.some((icon) => icon.src === "./icon-192.png"), true);
  assert.equal(manifest.icons.some((icon) => icon.src === "./icon-512.png"), true);
});

test("service worker caches shell assets and page images for faster reopen", () => {
  const source = readText("web/src/sw.js");

  assert.match(source, /alvernia-static-v3/);
  assert.match(source, /alvernia-pages-v3/);
  assert.match(source, /pages\.json/);
  assert.match(source, /icon-192\.png/);
  assert.match(source, /icon-512\.png/);
  assert.match(source, /pathname\.startsWith\("\/pages\/"\)/);
  assert.match(source, /cache\.put/);
});

test("web build emits install assets and generated icons", () => {
  const source = readText("web/build.mjs");

  assert.match(source, /sw\.js/);
  assert.match(source, /generateIcon\(192/);
  assert.match(source, /generateIcon\(512/);
  assert.equal(fs.existsSync(path.join(webSrcDir, "sw.js")), true);
});

test("web styles include launch and fullscreen affordances", () => {
  const source = readText("web/src/styles.css");

  assert.match(source, /\.launch-screen/);
  assert.match(source, /\.launch-card/);
  assert.match(source, /\.nav-button-fullscreen/);
});
