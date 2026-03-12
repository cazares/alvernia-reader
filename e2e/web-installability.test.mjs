import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");
const webSrcDir = path.join(APP_ROOT, "web", "src");

const readText = (relativePath) => fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8");

test("web shell includes iOS standalone metadata and simplified native-like controls", () => {
  const source = readText("web/src/index.html");

  assert.match(source, /apple-mobile-web-app-capable/);
  assert.match(source, /apple-mobile-web-app-status-bar-style/);
  assert.match(source, /maximum-scale=1, user-scalable=no/);
  assert.match(source, /id="top-chrome"/);
  assert.match(source, /id="bottom-chrome"/);
  assert.match(source, /id="fullscreen-button"/);
  assert.match(source, /id="jump-cta"/);
  assert.match(source, /id="jump-form"/);
  assert.match(source, /id="jump-song"[^>]*type="text"[^>]*inputmode="numeric"[^>]*pattern="\[0-9\]\*"[^>]*enterkeyhint="go"/);
  assert.match(source, /id="jump-submit"/);
  assert.match(source, /&larr; Anterior/);
  assert.match(source, /Siguiente &rarr;/);
  assert.match(source, /id="fullscreen-guard"/);
  assert.match(source, /id="resume-fullscreen"/);
  assert.match(source, /id="dismiss-fullscreen-guard"/);
  assert.match(source, /Instalar app|Usalo como app/);
  assert.doesNotMatch(source, /id="launch-screen"/);
  assert.doesNotMatch(source, /id="launch-fullscreen"/);
  assert.doesNotMatch(source, /id="launch-install"/);
  assert.doesNotMatch(source, /id="go-modal"/);
  assert.doesNotMatch(source, /id="song-number"/);
  assert.doesNotMatch(source, /id="cancel-go"/);
});

test("web app script supports install, sticky fullscreen, inline song navigation, and touch controls", () => {
  const source = readText("web/src/app.js");

  assert.match(source, /beforeinstallprompt/);
  assert.match(source, /requestFullscreen/);
  assert.match(source, /serviceWorker\.register/);
  assert.match(source, /searchParams\.get\("song"\)/);
  assert.match(source, /searchParams\.get\("page"\)/);
  assert.match(source, /stickyFullscreenWanted/);
  assert.match(source, /userRequestedFullscreenExit/);
  assert.match(source, /setFullscreenGuardVisible/);
  assert.match(source, /recoverFullscreen/);
  assert.match(source, /jumpForm\.addEventListener\("submit"/);
  assert.match(source, /jumpInput\.addEventListener\("input"/);
  assert.match(source, /sanitizeSongValue/);
  assert.match(source, /viewerShell\.addEventListener\("touchstart"/);
  assert.match(source, /viewerShell\.addEventListener\("touchend"/);
  assert.match(source, /viewerShell\.addEventListener\("click"/);
  assert.match(source, /clearInitialDeepLink/);
  assert.doesNotMatch(source, /openModal/);
  assert.doesNotMatch(source, /closeModal/);
  assert.doesNotMatch(source, /dblclick/);
  assert.doesNotMatch(source, /launchScreen/);
  assert.doesNotMatch(source, /songDraft/);
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

  assert.match(source, /alvernia-static-v4/);
  assert.match(source, /alvernia-pages-v4/);
  assert.match(source, /NETWORK_FIRST_PATHS/);
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

test("web styles include inline jump form and fullscreen affordances", () => {
  const source = readText("web/src/styles.css");

  assert.match(source, /\.jump-cta/);
  assert.match(source, /\.jump-form/);
  assert.match(source, /\.jump-input/);
  assert.match(source, /\.nav-button-fullscreen/);
  assert.match(source, /\.fullscreen-guard/);
  assert.match(source, /touch-action: manipulation/);
  assert.doesNotMatch(source, /\.launch-screen/);
  assert.doesNotMatch(source, /\.modal/);
});
