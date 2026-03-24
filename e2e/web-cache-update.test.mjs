import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

const readText = (relativePath) => fs.readFileSync(path.join(APP_ROOT, relativePath), "utf8");

test("service worker source uses a build-injected cache version and supports skip-waiting activation", () => {
  const source = readText("web/src/sw.js");

  assert.match(source, /const CACHE_VERSION = "__CACHE_VERSION__";/);
  assert.match(source, /signo-vino-static-\$\{CACHE_VERSION\}/);
  assert.match(source, /signo-vino-pages-\$\{CACHE_VERSION\}/);
  assert.match(source, /cache:\s*"no-store"/);
  assert.match(source, /event\.data\?\.type === "SKIP_WAITING"/);
});

test("web app registration forces service worker refresh, blocks until full offline cache is ready, and avoids audio clicks", () => {
  const source = readText("web/src/app.js");

  assert.match(source, /const DEFAULT_START_PAGE = 2;/);
  assert.match(source, /const isAdminSetupMode = initialUrl\.searchParams\.get\("admin"\) === "1";/);
  assert.match(source, /const SW_RELOAD_FLAG = "sv-sw-reload-pending";/);
  assert.match(source, /const CACHE_VERSION = "__CACHE_VERSION__";/);
  assert.match(source, /const STATIC_CACHE = `signo-vino-static-\$\{CACHE_VERSION\}`;/);
  assert.match(source, /const PAGE_CACHE = `signo-vino-pages-\$\{CACHE_VERSION\}`;/);
  assert.match(source, /const OFFLINE_READY_KEY = `sv-offline-ready-\$\{CACHE_VERSION\}`;/);
  assert.match(source, /const OFFLINE_DB_NAME = "signo-vino-offline";/);
  assert.match(source, /const OFFLINE_DB_STORE = "bundle-status";/);
  assert.match(source, /const OFFLINE_DB_RECORD_ID = "current";/);
  assert.match(source, /const OFFLINE_PAGES = window\.OFFLINE_PAGES \|\| null;/);
  assert.match(source, /const setViewportCssVars = \(\) =>/);
  assert.match(source, /window\.visualViewport\?\.addEventListener\("resize", setViewportCssVars/);
  assert.match(source, /window\.addEventListener\("orientationchange", setViewportCssVars/);
  assert.match(source, /document\.documentElement\.style\.setProperty\("--viewport-height", `\$\{height\}px`\)/);
  assert.match(source, /const resolvePageSrc = \(pageNumber, retryToken = ""\) =>/);
  assert.match(source, /const pageImageMatches = \(pageNumber\) =>/);
  assert.match(source, /updateViaCache:\s*"none"/);
  assert.match(source, /registration\.waiting/);
  assert.match(source, /navigator\.serviceWorker\.addEventListener\("controllerchange"/);
  assert.match(source, /window\.location\.reload\(\)/);
  assert.match(source, /worker\.postMessage\(\{ type: "SKIP_WAITING" \}\)/);
  assert.match(source, /const requireOfflineBundle = async \(totalPages\) =>/);
  assert.match(source, /const readOfflineMetadata = async \(\) =>/);
  assert.match(source, /const writeOfflineMetadata = async \(payload\) =>/);
  assert.match(source, /await ensureOfflineBundle\(totalPages, \(progress, total\) =>/);
  assert.match(source, /if \(OFFLINE_PAGES\) \{/);
  assert.match(source, /Conéctate una vez/);
  assert.match(source, /title: "Offline listo"/);
  assert.match(source, /ready: true/);
  assert.match(source, /Verificado: \$\{verifiedAtText\}/);
  assert.match(source, /offlineContinueButton\.addEventListener\("click"/);
  assert.match(source, /showAdminNote && isAdminSetupMode/);
  assert.match(source, /canTestOffline && isAdminSetupMode/);
  assert.match(source, /pageImageMatches\(nextPage\) && pageImage\.complete && pageImage\.naturalWidth > 0/);
  assert.match(source, /state\.currentPage = DEFAULT_START_PAGE;/);
  assert.match(source, /hideLoadingIndicator\(\);\s+await requireOfflineBundle\(state\.totalPages\);/);
  assert.match(source, /renderPage\(DEFAULT_START_PAGE, \{ pushToHistory: false \}\);/);
  assert.match(source, /setLoading\(true, "No se pudo cargar esta página\."\);\s+closeDrawer\(\);/);
  assert.match(source, /navigator\.vibrate\?\.\(ms\)/);
  assert.doesNotMatch(source, /AudioContext/);
  assert.doesNotMatch(source, /createBuffer/);
});

test("web build injects a cache version into the generated service worker", () => {
  const source = readText("web/build.mjs");

  assert.match(source, /git", \["rev-parse", "--short", "HEAD"\]/);
  assert.match(source, /replaceAll\("__CACHE_VERSION__", cacheVersion\)/);
  assert.match(source, /replaceAll\("__CACHE_VERSION__", cacheVersion\)/);
  assert.match(source, /writeFileSync\(path\.join\(distDir, "sw\.js"\), serviceWorkerSource\)/);
  assert.match(source, /writeFileSync\(path\.join\(distDir, "app\.js"\), appSource\)/);
});

test("offline html build emits the Signo Vino self-contained artifact", () => {
  const pkg = JSON.parse(readText("package.json"));
  const source = readText("web/build-offline.mjs");

  assert.doesNotMatch(pkg.scripts["build:web"], /build-offline\.mjs/);
  assert.match(pkg.scripts["build:web:offline"], /build-offline\.mjs/);
  assert.match(source, /signo-vino-offline\.html/);
  assert.match(source, /window\.OFFLINE_PAGES=/);
  assert.match(source, /src="\/pages\/page-002\.jpg"/);
});

test("initial web shell points directly at page 2", () => {
  const source = readText("web/src/index.html");

  assert.match(source, /id="page-image" src="\/pages\/page-002\.jpg"/);
  assert.match(source, /id="offline-gate"/);
  assert.match(source, /id="offline-progress-bar"/);
  assert.match(source, /id="offline-ready-note"/);
  assert.match(source, /id="offline-meta-note"/);
  assert.match(source, /id="offline-continue-button"/);
  assert.match(source, /id="offline-retry-button"/);
});

test("loading pill stays above the drawer overlay", () => {
  const source = readText("web/src/styles.css");

  assert.match(source, /\.loading \{[\s\S]*z-index: 30;/);
  assert.match(source, /\.offline-ready-note \{/);
  assert.match(source, /\.offline-meta-note \{/);
  assert.match(source, /\.offline-continue-button,/);
  assert.match(source, /--viewport-width: 100vw;/);
  assert.match(source, /--viewport-height: 100dvh;/);
  assert.match(source, /\.viewer-shell \{[\s\S]*height: var\(--viewport-height\);/);
  assert.match(source, /#page-image \{[\s\S]*max-height: calc\(var\(--viewport-height\) - var\(--safe-top\) - var\(--safe-bottom\)\);/);
  assert.match(source, /@media \(orientation: landscape\) \{/);
});
