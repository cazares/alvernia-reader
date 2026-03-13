import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");
const webSrcDir = path.join(APP_ROOT, "web", "src");

const readText = (relativePath) => fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8");

test("web shell includes standalone metadata and the navigation numberpad UI", () => {
  const source = readText("web/src/index.html");

  assert.match(source, /apple-mobile-web-app-capable/);
  assert.match(source, /apple-mobile-web-app-status-bar-style/);
  assert.match(source, /maximum-scale=1, user-scalable=no/);
  assert.match(source, /id="overlay-controls"/);
  assert.match(source, /id="navigation-numberpad"/);
  assert.match(source, /id="song-status"/);
  assert.match(source, /Canción 0 de 0/);
  assert.match(source, /id="song-display"[^>]*readonly/);
  assert.match(source, /Número de canción/);
  assert.match(source, /id="numberpad-grid"/);
  assert.match(source, /Navigation numberpad/);
  assert.match(source, />Limpiar</);
  assert.match(source, />Borrar</);
  assert.match(source, /id="go-button"/);
  assert.match(source, />Ir</);
  assert.match(source, /id="prev-page"/);
  assert.match(source, /&larr; Anterior/);
  assert.match(source, /id="next-page"/);
  assert.match(source, /Siguiente &rarr;/);
  assert.match(source, /id="fullscreen-button"/);
  assert.match(source, /Pantalla completa/);
  assert.doesNotMatch(source, /launch-screen/);
  assert.doesNotMatch(source, /go-modal/);
  assert.doesNotMatch(source, /install-sheet/);
});

test("web app script supports first-page startup, song-based navigation numberpad input, and simple fullscreen", () => {
  const source = readText("web/src/app.js");

  assert.match(source, /const state = \{[\s\S]*currentPage: 1,[\s\S]*songDraft: "",[\s\S]*immersiveMode: false/);
  assert.match(source, /renderPage\(1\)/);
  assert.match(source, /numberpadGrid\.addEventListener\("click"/);
  assert.match(source, /appendDigit/);
  assert.match(source, /clearDraft/);
  assert.match(source, /backspaceDraft/);
  assert.match(source, /goToDraftSong/);
  assert.match(source, /findSongPage/);
  assert.match(source, /findSongIndexAtOrBeforePage/);
  assert.match(source, /turnSong/);
  assert.match(source, /keepOverlay = false/);
  assert.match(source, /keepOverlay: true/);
  assert.match(source, /setOverlayVisible\(false\)/);
  assert.match(source, /canOfferPseudoFullscreen/);
  assert.match(source, /window\.matchMedia\("\(display-mode: standalone\)"\)/);
  assert.match(source, /requestFullscreen/);
  assert.match(source, /exitFullscreen/);
  assert.match(source, /viewerShell\.addEventListener\("touchstart"/);
  assert.match(source, /viewerShell\.addEventListener\("touchend"/);
  assert.match(source, /viewerShell\.addEventListener\("click"/);
  assert.match(source, /serviceWorker\.register/);
  assert.match(source, /songIndex/);
  assert.doesNotMatch(source, /beforeinstallprompt/);
  assert.doesNotMatch(source, /fullscreenGuard/);
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

test("web styles include the centered navigation numberpad and overlay controls", () => {
  const source = readText("web/src/styles.css");

  assert.match(source, /\.overlay-controls/);
  assert.match(source, /\.navigation-numberpad/);
  assert.match(source, /\.numberpad-grid/);
  assert.match(source, /\.numberpad-display/);
  assert.match(source, /\.go-button/);
  assert.match(source, /\.overlay-actions/);
  assert.match(source, /\.nav-button-left/);
  assert.match(source, /\.nav-button-right/);
  assert.match(source, /touch-action: manipulation/);
  assert.doesNotMatch(source, /\.install-sheet/);
});
