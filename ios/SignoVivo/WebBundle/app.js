// ── Boot guard: global fail-soft net (crash-proofing, M2) ─────────────────────
// The FIRST executable code, so it is armed before anything below can throw. A throw
// during module evaluation — e.g. a storage-disabled / private-mode browser hitting an
// unguarded localStorage read — would otherwise abort the whole script, so the reader
// never boots and every device shows a permanent WHITE SCREEN at once (the exact
// "busted for everyone" failure). This converts that into a visible "Reintentar" card.
// Self-contained (depends on nothing below), never throws itself, and only takes over
// the screen when the reader never confirmed a boot (window.__svBooted) — so a benign
// error AFTER the app is up is recorded, never a spurious hijack of a working reader.
// Uses non-capturing window "error" so it catches uncaught SCRIPT errors, not benign
// image/resource 404s (the reader handles those itself).
(function bootGuard() {
  function showRecovery(where, err) {
    try {
      try { window.__SV_LAST_ERROR = { where: where, msg: String((err && err.message) || err || ""), stack: String((err && err.stack) || ""), t: Date.now() }; } catch (_) {}
      // Best-effort crash telemetry (M2 Slice D). The reporter is registered by app.js once it
      // loads; if a crash beats it, app.js flushes __SV_LAST_ERROR on register. Guarded — a
      // reporting failure must never interfere with recovery.
      try { if (typeof window.__svReportCrash === "function") window.__svReportCrash(where, String((err && err.message) || err || ""), (err && err.stack) || ""); } catch (_) {}
      if (window.__svBooted === true) return;            // app is up — record only, never take over
      if (window.__svRecoveryShown === true) return;     // idempotent
      window.__svRecoveryShown = true;
      try {
        var g = document.getElementById("geo-gate"); if (g) g.classList.add("is-hidden");
        var o = document.getElementById("offline-gate"); if (o) o.classList.add("is-hidden");
      } catch (_) {}
      var host = document.body || document.documentElement;
      if (!host) return;
      var wrap = document.createElement("div");
      wrap.id = "sv-crash-banner";
      wrap.setAttribute("role", "alert");
      wrap.style.cssText = "position:fixed;inset:0;z-index:2147483600;display:flex;align-items:center;justify-content:center;background:#0d0d1a;color:#fff;font:16px/1.5 -apple-system,system-ui,sans-serif;padding:24px;text-align:center;";
      var box = document.createElement("div"); box.style.cssText = "max-width:340px;";
      var h = document.createElement("div"); h.textContent = "Signo Vivo se está recuperando"; h.style.cssText = "font-size:20px;font-weight:700;margin-bottom:8px;";
      var p = document.createElement("div"); p.textContent = "Algo no cargó bien. Toca para reintentar."; p.style.cssText = "color:#c8c8dc;margin-bottom:18px;";
      var btn = document.createElement("button"); btn.type = "button"; btn.textContent = "Reintentar";
      btn.style.cssText = "padding:13px 28px;border:0;border-radius:12px;background:#3b6df6;color:#fff;font:600 17px -apple-system,system-ui,sans-serif;";
      btn.onclick = function () { try { window.location.reload(); } catch (_) {} };
      box.appendChild(h); box.appendChild(p); box.appendChild(btn); wrap.appendChild(box); host.appendChild(wrap);
    } catch (_) { /* recovery must never throw */ }
  }
  try {
    window.__svRecover = showRecovery;
    window.addEventListener("error", function (e) { showRecovery("error", e && (e.error || e.message)); });
    window.addEventListener("unhandledrejection", function (e) { showRecovery("unhandledrejection", e && e.reason); });
  } catch (_) { /* arming the net must never break boot */ }
})();

// ── DOM references ────────────────────────────────────────────────────────────
const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
// The app is now SINGLE-BOOK and fully public — there is no geo resolution, no book to wait for,
// and nothing to gate on. If the (legacy) #geo-gate overlay still ships in the HTML, hide it at
// once so the reader shows immediately. revealReader is kept as a thin, idempotent helper so the
// many existing call sites still compile and simply reveal the reader.
const geoGate = document.getElementById("geo-gate");
let gateLifted = false;
const liftGateNow = () => {
  if (gateLifted) return;
  gateLifted = true;
  geoGate?.classList.add("is-hidden");
  // Signal a successful boot: content (or a handled boot-error message) is now shown,
  // so the bootGuard should no longer treat later errors as a white-screen state.
  try { window.__svBooted = true; } catch (_) {}
};
const revealReader = () => liftGateNow();
// Recover a transient first-load image failure (cold cache / flaky network) by retrying the src
// instead of leaving a broken image. Capped so a genuinely-missing asset eventually gives up.
if (pageImage) {
  let pageImgRetries = 0;
  pageImage.addEventListener("error", () => {
    if (pageImgRetries++ >= 4) return;
    const base = (pageImage.currentSrc || pageImage.src).split("?")[0];
    window.setTimeout(() => { pageImage.src = `${base}?retry=${pageImgRetries}`; }, 350 * pageImgRetries);
  });
}
const offlineGate = document.getElementById("offline-gate");
const offlineGateTitle = document.getElementById("offline-gate-title");
const offlineGateBody = document.getElementById("offline-gate-body");
const offlineSpinner = document.getElementById("offline-spinner");
const offlineProgressValue = document.getElementById("offline-progress-value");
const offlineReadyNote = document.getElementById("offline-ready-note");
const offlineMetaNote = document.getElementById("offline-meta-note");
const offlineContinueButton = document.getElementById("offline-continue-button");
const offlineRetryButton = document.getElementById("offline-retry-button");
const overlayControls = document.getElementById("overlay-controls");
const drawerHandle = document.getElementById("drawer-handle");
const drawerBackdrop = document.getElementById("drawer-backdrop");
const navigationDrawer = document.getElementById("navigation-drawer");
const drawerBack = document.getElementById("drawer-back");
const songStatus = document.getElementById("song-status");
const songIntroEl = document.getElementById("song-intro");
const songDisplay = document.getElementById("song-display");
const displayClearButton = document.getElementById("display-clear");
const numberpadGrid = document.getElementById("numberpad-grid");
const backspaceButton = document.getElementById("backspace-button");
const goButton = document.getElementById("go-button");
const directorModeBadge = document.getElementById("director-mode-badge");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const fullscreenButton = document.getElementById("fullscreen-button");
const fullscreenFab = document.getElementById("fullscreen-fab");
const prevCornerButton = document.getElementById("prev-corner");
const searchInput = document.getElementById("search-input");
const searchResults = document.getElementById("search-results");
const searchClearButton = document.getElementById("search-clear");
const searchIndexButton = document.getElementById("search-index");
const searchColHeader = document.getElementById("search-col-header");
const searchSortToggle = document.getElementById("search-sort-toggle");
const helpButton = document.getElementById("help-button");
const helpPanel = document.getElementById("help-panel");
const helpCloseButton = document.getElementById("help-close");
const helpSettingsLabel = document.getElementById("help-settings-label");
const hapticToggleButton = document.getElementById("haptic-toggle");
const numpadTipWrap = document.getElementById("numpad-tip-wrap");
const tipDismissButton = document.getElementById("tip-dismiss");
const drawerCloseButton = document.getElementById("drawer-close");
const searchCancelButton = document.getElementById("search-cancel"); // kept for compat (hidden)
const drawerTabRail    = document.getElementById("drawer-tab-rail");
const drawerPaneContent = document.getElementById("drawer-pane-content");
const searchRow        = document.getElementById("search-row");
const searchBackButton = document.getElementById("search-back");
const modeBtnNumpad    = document.getElementById("mode-btn-numpad");
const modeBtnBrowse    = document.getElementById("mode-btn-browse");
const appVersionLabel  = document.getElementById("app-version-label");
const songJumpModal    = document.getElementById("song-jump-modal");
const songJumpBackdrop = document.getElementById("song-jump-backdrop");
const songJumpTrigger  = document.getElementById("song-jump-trigger");
const songCancelButton = document.getElementById("song-cancel");

// ── State ─────────────────────────────────────────────────────────────────────
const PREFS_KEY = "nc-sort-prefs";

const loadSortPrefs = () => {
  try { return JSON.parse(localStorage.getItem(PREFS_KEY) || "{}"); } catch { return {}; }
};
const saveSortPrefs = () => {
  try { localStorage.setItem(PREFS_KEY, JSON.stringify(state.indexSortPrefs)); } catch {}
};

const savedPrefs = loadSortPrefs();

const state = {
  totalPages: 1,
  totalSongs: 0,
  currentPage: 1,
  songDraft: "",
  songIndex: [],
  songPageLookup: new Map(),
  themeIndex: [],
  pageHistory: [],
  searchIndexPages: [],
  drawerOpen: false,
  songJumpOpen: false,
  indexDrillDown: false,
  loadingTimer: 0,
  pageLoadRequest: 0,
  // The page whose render most recently FAILED, and when. Paces retries of a page that cannot be
  // rendered at all — see RENDER_RETRY_COOLDOWN_MS.
  lastRenderFailure: null,
  prefetchedPages: new Set(),
  prefetchingPages: new Set(),
  touchStart: null,
  lastTouchEndedAt: 0,
  indexVisible: false,
  indexTab: "themes",
  indexSortPrefs: {
    themes:     savedPrefs.themes     || "az",
    alpha:      savedPrefs.alpha      || "az",
    length:     savedPrefs.length     || "longest",
    keywords:   savedPrefs.keywords   || "freq",
    complexity: savedPrefs.complexity || "simple",
  },
  searchSortMode: "best",  // search-result re-sort (restores native 277): best | num | az
  activeTab: "todas",
  prevTab: "todas",     // where to return when exiting search fullscreen
  drawerMode: "browse", // browse-only — jump-to-song is now a centered modal
  currentBook: "standard", // single-book app: always the Manual Alvernia ("standard")
  syncRole: "none",       // last role the native shell reported (director/follower/none)
  nativeBridgeAvailable: false,
  nativeSyncRole: "off",   // drives the DIRECTOR badge visibility
};

// ── Single book ─────────────────────────────────────────────────────────────────
// Signo Vivo serves ONE book, the "Manual Alvernia" (id "standard"), to everyone — no geo, no
// registry, no book switching. The build output path layout is unchanged (/books/standard/…).
const BOOK_ID = "standard";
// Fallback page count used for page-filename pad width before the manifest lands (the real value
// comes from /books/standard/pages.json → state.totalPages). Kept generous so pad width is right.
// The degraded-path page count, used only when #pages-data is missing AND pages.json cannot be
// fetched. Baked by web/build.mjs from the book actually shipped, because a hardcoded literal
// silently drifts every time the songbook grows: it read 372 while the shipped book was already
// 373 pages, which would have hidden the last page from any device on the degraded path. The
// `|| 372` keeps raw (unsubstituted) app.js valid and gives the token a sane floor.
const STANDARD_TOTAL_PAGES = Number("372") || 372;
state.currentBook = BOOK_ID;

let cachedSongKeys = null;
let cachedSongLengths = null;
let cachedKeywords = null;

// ── Adjacent-page prefetch (perceived-speed win for live followers) ─────────────
// Warm the NEXT page(s) so a director's +1 page-turn is instant. URL-keyed to skip dupes.
const prefetchedPageUrls = new Set();

// ── Environment detection ─────────────────────────────────────────────────────
const initialUrl = new URL(window.location.href);
const userAgent = navigator.userAgent;
const isIOS = /iphone|ipad|ipod/i.test(userAgent);
const isStandaloneApp = window.matchMedia("(display-mode: standalone)").matches
  || window.matchMedia("(display-mode: fullscreen)").matches
  || window.navigator.standalone === true;
const fullscreenTarget = document.documentElement;
const nativeFullscreenSupported = Boolean(
  document.fullscreenEnabled
    || document.webkitFullscreenEnabled
    || fullscreenTarget.requestFullscreen
    || fullscreenTarget.webkitRequestFullscreen,
);
const canOfferPseudoFullscreen = isIOS && isStandaloneApp;
const supportsFullscreen = nativeFullscreenSupported || canOfferPseudoFullscreen;
const DEFAULT_START_PAGE = 2;
// Pacing for a page that keeps failing to render — the pure rule lives in lib/svRenderPace.js so a
// test can EXECUTE it rather than quote it. See that file for the loop it breaks.
const { shouldPaceRender: svShouldPaceRender } = (typeof self !== "undefined" && self.svRenderPace) || {
  // Defensive fallback: if the lib script failed to load, never pace — an un-paced retry storm is
  // bad, but a follower that silently refuses to render is worse.
  shouldPaceRender: () => false,
};
const SW_RELOAD_FLAG = "sv-sw-reload-pending";
const CACHE_VERSION = "ba1e0a7-20ba523c";
// Content-address of the BOOK (source PDF + render knobs, hashed by build.mjs). Keys everything
// page-image-related SEPARATELY from the shell's CACHE_VERSION: a shell deploy leaves the cached
// book untouched (no 25MB re-download), while ANY book change — even a page revised in place under
// an unchanged filename — starts a fresh page cache that the SW fills from the network. MUST stay
// in lockstep with sw.js's PAGE_CACHE name, or getCachedPageSet counts a cache the SW isn't using.
const BOOK_VERSION = "2dc91b796f72";
// Human-facing build number, baked from version.json at build time, shown as a small "v<NNN>" badge
// so signovivo.com always reveals which build it's on. Native injects its own version + overlay.
const BUILD_NUMBER = "473";
const STATIC_CACHE = `signo-vivo-static-${CACHE_VERSION}`;
const PAGE_CACHE = `signo-vivo-pages-${BOOK_VERSION}`;
// Book-keyed (not shell-keyed): "fully cached offline" is a claim about the BOOK, so a shell-only
// deploy must not un-ready a device, and a book change must — the new key reads as not-ready, so
// fleetCheckin stops claiming cached and ensureOfflineBundle re-fetches into the new page cache.
// The flag is a set-once HINT, never a claim on its own: it is written when a bundle completes and
// nothing ever clears it, so isOfflineBundleReady must confirm it against the real caches before
// anyone reports this device ready. See its comment for the eviction case that forces this.
const OFFLINE_READY_KEY = `sv-offline-ready-${BOOK_VERSION}`;
const OFFLINE_DB_NAME = "signo-vivo-offline";
const OFFLINE_DB_STORE = "bundle-status";
const OFFLINE_DB_RECORD_ID = "current";
const NATIVE_FILE_MODE = Boolean(window.__SIGNO_VINO_NATIVE_FILE_MODE || window.location.protocol === "file:");
const NATIVE_BRIDGE_CHANNEL = "signovivo-native";
const resolveAppPath = (pathname) => {
  if (!NATIVE_FILE_MODE) return pathname;
  if (pathname === "/") return "./";
  return pathname.replace(/^\//, "");
};
// Shell assets that are book-agnostic (always cached).
const SHELL_ASSETS = [
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  "/books.json",
  "/icon.png",
  "/icon-192.png",
  "/icon-512.png",
].map(resolveAppPath);
// The checklist for "this device can still render the book with no network" —
// isOfflineBundleReady is the only consumer. It must stay a SUBSET of what the SW installs at
// activate: anything here that install does not cache is absent from a freshly-rotated
// post-deploy STATIC_CACHE until the DEFERRED precache writes it, which would make every device
// report not-ready for that window — a fleet-wide false red at the one moment the dashboard is
// read. smoke-boot pins that subset relation; keep the two lists in step rather than special-
// casing this one.
const coreAssets = () => [
  ...SHELL_ASSETS,
  resolveAppPath(`/books/${BOOK_ID}/pages.json`),
  resolveAppPath(`/books/${BOOK_ID}/search-index.json`),
];

// ── Utilities ────────────────────────────────────────────────────────────────
// FROZEN pad width — page-001.webp means page 1, permanently, at every book size.
//
// This used to be `String(state.totalPages).length`, i.e. derived from the live page count. That is
// stable only while the book stays in one decade of digits: the first book to reach 1000 pages would
// have flipped every URL from page-001.webp to page-0001.webp at once. Page filenames are the key
// that every offline copy is stored under — the SW's page cache, the previous-edition fallback
// cache, a staged native download, an AirDropped bundle — so that rename would have invalidated the
// entire installed base in a single deploy, silently, and the additive-only invariant
// (docs/choir-pdf-distribution-plan.md §4 decision 7) exists precisely to make that impossible.
// Fixed-minimum-3 padding keeps 1..999 permanent and lets page 1000+ simply be longer.
// web/build.mjs declares the same constant on the writer side; smoke-boot pins the pair.
const PAGE_PAD_WIDTH = 3;
const pageFileName = (pageNumber) => {
  const padded = String(pageNumber).padStart(PAGE_PAD_WIDTH, "0");
  // Always RELATIVE and book-scoped so the same path resolves over https
  // (signovivo.com) AND file:// (native WKWebView bundle). No leading slash.
  return `books/${BOOK_ID}/pages/page-${padded}.webp`;
};
const pageFileUrl = (pageNumber, retryToken = "") => retryToken
  ? `${pageFileName(pageNumber)}?reload=${retryToken}`
  : pageFileName(pageNumber);
const resolvePageSrc = (pageNumber, retryToken = "") => pageFileUrl(pageNumber, retryToken);
const pageImageMatches = (pageNumber) => {
  const currentSrc = pageImage.getAttribute("src") || "";
  return currentSrc.endsWith(pageFileName(pageNumber));
};
// ALWAYS returns an in-range INTEGER. A float (2.7) or NaN must never reach
// pageFileName → page-2.7.webp / page-NaN.webp would 404 and stick the render.
const clampPage = (pageNumber) => {
  const n = Math.trunc(Number(pageNumber));
  if (!Number.isFinite(n)) return state.currentPage || 1;
  const total = Number.isFinite(state.totalPages) && state.totalPages > 0 ? state.totalPages : 1;
  return Math.max(1, Math.min(n, total));
};
const clampSongIndex = (index) => Math.max(0, Math.min(index, state.totalSongs - 1));
const getFullscreenElement = () => document.fullscreenElement || document.webkitFullscreenElement || null;
const isFullscreen = () => Boolean(getFullscreenElement());
const scheduleIdleWork = window.requestIdleCallback
  ? window.requestIdleCallback.bind(window)
  : (callback) => window.setTimeout(callback, 140);
const PAGE_IMAGE_LOAD_TIMEOUT_MS = 3000;
const setViewportCssVars = () => {
  const viewport = window.visualViewport;
  const width = Math.max(1, Math.round((viewport?.width || window.innerWidth) * 100) / 100);
  const height = Math.max(1, Math.round((viewport?.height || window.innerHeight) * 100) / 100);
  document.documentElement.style.setProperty("--viewport-width", `${width}px`);
  document.documentElement.style.setProperty("--viewport-height", `${height}px`);
  // The version stamp is pinned to where the PAGE was drawn, so it must be repositioned AFTER these
  // vars change — they are what size the <img>. Called from here rather than from its own listener
  // so the ordering is guaranteed and so it inherits this function's full event coverage: plain
  // resize, orientationchange, AND visualViewport resize/scroll, which is how iOS actually reports a
  // rotation. A separate listener got only two of the four and would have gone stale on device.
  // Hoisted function declaration, so calling it from above its definition is fine.
  positionBuildBadge();
};
const bindViewportMetrics = () => {
  setViewportCssVars();
  window.addEventListener("resize", setViewportCssVars, { passive: true });
  window.addEventListener("orientationchange", setViewportCssVars, { passive: true });
  window.visualViewport?.addEventListener("resize", setViewportCssVars, { passive: true });
  window.visualViewport?.addEventListener("scroll", setViewportCssVars, { passive: true });
};

const normalizeText = (text) => String(text ?? "")
  .normalize("NFD")
  .replace(/[\u0300-\u036f]/g, "")
  .toLowerCase();

const hasNativeBridge = () => Boolean(window.ReactNativeWebView?.postMessage);

const postNativeBridge = (payload) => {
  if (!hasNativeBridge()) return false;
  try {
    window.ReactNativeWebView.postMessage(JSON.stringify({
      channel: NATIVE_BRIDGE_CHANNEL,
      ...payload,
    }));
    return true;
  } catch (error) {
    console.error("No se pudo hablar con el app nativa", error);
    return false;
  }
};

// ── First-native-page gate (2026-08-18) ─────────────────────────────────────────
//
// WHY THIS EXISTS. On a native WebView (re)mount, the reader used to reveal on
// DEFAULT_START_PAGE immediately, then correct to the real page once native's bridge-ready
// response arrived. That correction is exactly the wrong-page flash the director-page-race fix
// (aa68c9e) already closed for a DIRECTOR's own broadcast — but it was still wide open on the
// FOLLOWER side of every native boot, because bridge-ready's response (native's ONLY channel for
// saying "here is the real page") was requested at the very END of initReader, after the reader
// had already revealed. A device that just went through an OTA book swap + binary update remounts
// its WebView on every follower at once, so the flash became visible fleet-wide.
//
// firstNativePageSignal resolves the instant a real page arrives from native (any of the three
// bridge-ready branches — director reasserting, follower resyncing, or solo-reader restore — all
// inject a sync-event/page; see PdfReaderApp.tsx's "bridge-ready" case). initReader awaits it,
// bounded by FIRST_NATIVE_PAGE_TIMEOUT_MS, BEFORE revealing — so a follower never shows a page
// number that was never confirmed real. The bound exists for the one branch that injects nothing:
// a genuinely brand-new install/book with no saved page and nobody directing yet.
const FIRST_NATIVE_PAGE_TIMEOUT_MS = 2500;
let resolveFirstNativePageSignal = null;
const firstNativePageSignal = hasNativeBridge()
  ? new Promise((resolve) => { resolveFirstNativePageSignal = resolve; })
  : null;

const openOfflineDb = () => new Promise((resolve, reject) => {
  if (!("indexedDB" in window)) {
    reject(new Error("IndexedDB no disponible"));
    return;
  }
  const request = indexedDB.open(OFFLINE_DB_NAME, 1);
  request.onupgradeneeded = () => {
    const database = request.result;
    if (!database.objectStoreNames.contains(OFFLINE_DB_STORE)) {
      database.createObjectStore(OFFLINE_DB_STORE, { keyPath: "id" });
    }
  };
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error || new Error("No se pudo abrir IndexedDB"));
});

const readOfflineMetadata = async () => {
  try {
    const database = await openOfflineDb();
    return await new Promise((resolve, reject) => {
      const transaction = database.transaction(OFFLINE_DB_STORE, "readonly");
      const store = transaction.objectStore(OFFLINE_DB_STORE);
      const request = store.get(OFFLINE_DB_RECORD_ID);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error || new Error("No se pudo leer el estado offline"));
      transaction.oncomplete = () => database.close();
    });
  } catch {
    return null;
  }
};

const writeOfflineMetadata = async (payload) => {
  try {
    const database = await openOfflineDb();
    await new Promise((resolve, reject) => {
      const transaction = database.transaction(OFFLINE_DB_STORE, "readwrite");
      const store = transaction.objectStore(OFFLINE_DB_STORE);
      store.put({ id: OFFLINE_DB_RECORD_ID, ...payload });
      transaction.oncomplete = () => resolve(true);
      transaction.onerror = () => reject(transaction.error || new Error("No se pudo guardar el estado offline"));
      transaction.onabort = () => reject(transaction.error || new Error("No se pudo guardar el estado offline"));
    });
    database.close();
  } catch {}
};

// ── Haptic feedback ───────────────────────────────────────────────────────────
const HAPTIC_PREF_KEY = "sv-haptic";
// Guarded: localStorage.getItem THROWS in a storage-disabled / private-mode browser,
// and this runs at module-eval — an unguarded throw here aborts the whole script and
// white-screens every device (the Wednesday failure). Default to enabled on failure.
let hapticEnabled = true;
try { hapticEnabled = localStorage.getItem(HAPTIC_PREF_KEY) !== "off"; } catch (_) {}
const haptic = (ms = 10) => {
  if (!hapticEnabled) return;
  try { navigator.vibrate?.(ms); } catch {}
};

// ── Tip dismissal ─────────────────────────────────────────────────────────────
const TIP_KEY = "sv-tip";
// Guarded for the same reason as HAPTIC above — an unguarded module-eval localStorage
// read white-screens every device in a storage-disabled browser.
try {
  if (localStorage.getItem(TIP_KEY) === "dismissed") {
    numpadTipWrap.classList.add("is-hidden");
    tipDismissButton.classList.add("is-hidden");
  }
} catch (_) {}

// ── Recientes (recently viewed songs) ────────────────────────────────────────
const RECIENTES_KEY = "sv-recientes";
const MAX_RECIENTES = 15;

const getRecientes = () => {
  try { return JSON.parse(localStorage.getItem(RECIENTES_KEY) || "[]"); } catch { return []; }
};

const addToRecientes = (songNum) => {
  try {
    let list = getRecientes().filter((n) => n !== songNum);
    list.unshift(songNum);
    if (list.length > MAX_RECIENTES) list = list.slice(0, MAX_RECIENTES);
    localStorage.setItem(RECIENTES_KEY, JSON.stringify(list));
  } catch {}
};

// ── Fullscreen helpers ────────────────────────────────────────────────────────
const requestFullscreen = async () => {
  if (fullscreenTarget.requestFullscreen) {
    return fullscreenTarget.requestFullscreen({ navigationUI: "hide" }).catch(() => fullscreenTarget.requestFullscreen());
  }
  if (fullscreenTarget.webkitRequestFullscreen) {
    return fullscreenTarget.webkitRequestFullscreen();
  }
  return null;
};

const exitFullscreen = async () => {
  if (document.exitFullscreen) return document.exitFullscreen();
  if (document.webkitExitFullscreen) return document.webkitExitFullscreen();
  return null;
};

// ── Loading state ─────────────────────────────────────────────────────────────
const setLoading = (active, text = "Cargando...") => {
  loading.textContent = text;
  loading.classList.toggle("is-hidden", !active);
  pageImage.classList.toggle("is-loading", active);
};

const clearLoadingTimer = () => {
  if (!state.loadingTimer) return;
  window.clearTimeout(state.loadingTimer);
  state.loadingTimer = 0;
};

const scheduleLoadingIndicator = (text = "Cargando...") => {
  clearLoadingTimer();
  loading.textContent = text;
  loading.classList.add("is-hidden");
  pageImage.classList.add("is-loading");
  state.loadingTimer = window.setTimeout(() => {
    loading.classList.remove("is-hidden");
  }, 90);
};

const hideLoadingIndicator = () => {
  clearLoadingTimer();
  loading.classList.add("is-hidden");
  pageImage.classList.remove("is-loading");
};

// Rotating loading phrases shown under the spinner during the initial download.
// (A spinner replaces the old progress bar — page-count % was misleading.)
const LOADING_PHRASES = [
  "Preparando todo…",
  "Afinando las voces…",
  "Guardando los cantos…",
  "Descargando el manual…",
  "Organizando las páginas…",
  "Cargando los himnos…",
  "Repasando las letras…",
  "Buscando el tono…",
  "Acomodando los acordes…",
  "Sincronizando el coro…",
  "Puliendo los detalles…",
  "Reuniendo al coro…",
  "Preparando la alabanza…",
  "Guardando para uso sin internet…",
  "Casi listo…",
  "Ya merito…",
  "Tantito más…",
  "Un momento…",
  "Listo en un instante…",
  "Gracias por tu paciencia…",
];

let loadingPhraseTimer = null;
const showRandomLoadingPhrase = () => {
  offlineProgressValue.textContent =
    LOADING_PHRASES[Math.floor(Math.random() * LOADING_PHRASES.length)];
};
const startLoadingPhrases = () => {
  if (loadingPhraseTimer) return;
  showRandomLoadingPhrase();
  loadingPhraseTimer = setInterval(showRandomLoadingPhrase, 1700);
};
const stopLoadingPhrases = () => {
  if (loadingPhraseTimer) { clearInterval(loadingPhraseTimer); loadingPhraseTimer = null; }
};

const setOfflineGateState = ({
  visible,
  title = "Preparando Signo Vivo",
  body = "Descargando todo el manual para que funcione 100% offline.",
  progress = 0,
  total = 0,
  showAdminNote = false,
  ready = false,
  metadataText = "",
  canTestOffline = false,
  canRetry = false,
} = {}) => {
  offlineGate.classList.toggle("is-hidden", !visible);
  offlineGateTitle.textContent = title;
  offlineGateBody.textContent = body;
  const downloading = visible && !ready && !canRetry;
  offlineSpinner.classList.toggle("is-hidden", !downloading);
  if (downloading) {
    startLoadingPhrases();
  } else {
    stopLoadingPhrases();
    offlineProgressValue.textContent = "";
  }
  offlineReadyNote.classList.toggle("is-hidden", !ready);
  offlineMetaNote.textContent = metadataText;
  offlineMetaNote.classList.toggle("is-hidden", !metadataText);
  offlineContinueButton.classList.toggle("is-hidden", !ready);
  offlineRetryButton.classList.toggle("is-hidden", !canRetry);
};

const extractCachedPageNumber = (request) => {
  const pathname = new URL(request.url).pathname;
  // Page path: /books/standard/pages/page-NNN.webp.
  const match = pathname.match(/\/books\/([^/]+)\/pages\/page-(\d+)\.webp$/);
  if (!match) return null;
  if (match[1] !== BOOK_ID) return null;
  return Number.parseInt(match[2], 10);
};

// caches.open() CREATES the cache when it is absent. For read-only inspection that is a real
// side effect — an empty husk under the current book's name is something the SW's activate
// keep-policy then has to reason about — so any code that merely *asks* what is cached must go
// through here and treat "absent" as "empty". (countCachedPageImages enforces the same rule
// inline for the same reason.)
const openExistingCache = async (name) => {
  if (!(await caches.keys()).includes(name)) return null;
  return caches.open(name);
};

// PAGE-SLOT CONTENT GUARD. A page slot holds a real image only if the response says
// `content-type: image/*`. Cloudflare Pages answers an unmatched path with the SPA shell — 200,
// text/html, the bytes of index.html — so during the window where the shell advertises more
// pages than a PoP can serve, a page fetch "succeeds" with a document. sw.js refuses to cache
// those; these are the same guards on the app side, which needs its own because cacheSinglePage
// writes to PAGE_CACHE with a DIRECT cache.put that never passes through the SW's fetch handler.
//
// Two rules, asymmetric for the same reason sw.js's are:
//   - WRITING demands a POSITIVE image/*. A bad write is permanent and unrecoverable offline.
//   - JUDGING what is ALREADY cached condemns only a positively-wrong type. An entry we cannot
//     classify is left alone, because a false "missing" verdict re-downloads the whole 25MB book
//     on every device — a far larger harm than the one page it would be guessing about.
const pageResponseContentType = (response) =>
  (response?.headers?.get("content-type") || "").toLowerCase().trim();
const isPageImageResponse = (response) => pageResponseContentType(response).startsWith("image/");
const isPoisonedPageEntry = (response) => {
  const contentType = pageResponseContentType(response);
  return contentType !== "" && !contentType.startsWith("image/");
};

const getCachedPageSet = async (cache) => {
  const keys = await cache.keys();
  const pageNumbers = keys.map(extractCachedPageNumber);
  // CONTENT-AWARE, not merely key-aware. A poisoned slot still has a perfectly good
  // page-NNN.webp KEY, and counting keys alone was wrong twice over: ensureOfflineBundle never
  // listed that page as missing (so cacheSinglePage's repair below could never fire, and the
  // poison was permanent), and isOfflineBundleReady certified a bundle containing a page that
  // renders broken — the pre-Mass dashboard showing green over it.
  //
  // matchAll() reads every entry's headers in ONE call instead of one match() per page, and its
  // result is in the same insertion order as keys(). If that correlation ever fails to hold — or
  // matchAll is unavailable — the length check drops us back to key-only counting, because
  // mislabelling good pages as missing is the more expensive way to be wrong.
  let responses = null;
  try {
    responses = await cache.matchAll();
  } catch (_) {
    responses = null;
  }
  const canInspect = Array.isArray(responses) && responses.length === keys.length;
  return new Set(
    pageNumbers.filter((pageNumber, index) => {
      if (!Number.isFinite(pageNumber)) return false;
      return canInspect ? !isPoisonedPageEntry(responses[index]) : true;
    }),
  );
};

// Fetch-and-put with {cache:"no-store"} instead of cache.add(). no-store is the CONTRACT with
// the SW (both branches of it): these requests exist to install verified-fresh bytes, so the SW
// passes them straight to the network — a cache-first answer here is how a previous edition's
// pages.json (or a stale shell file served by an OLD still-controlling SW) could be laundered
// into the new static cache and then served cache-first forever. Non-strict = allSettled
// resilience (heal what you can); strict = first failure throws (for assets whose staleness
// must block the "ready" certification).
const cacheAssetsNoStore = async (cache, assets, { strict = false } = {}) => {
  const results = await Promise.allSettled(
    assets.map(async (asset) => {
      const response = await fetch(asset, { cache: "no-store" });
      if (!response.ok) throw new Error(`no se pudo descargar ${asset}`);
      await cache.put(asset, response.clone());
    }),
  );
  if (strict) {
    const failed = results.find((r) => r.status === "rejected");
    if (failed) throw failed.reason;
  }
};

// SHELL assets only — deliberately NOT the book manifests. This runs BEFORE the deploy-skew
// gate (it is the app-side writer that completes a partially-installed shell so a WAITING SW's
// skipWaiting gate can pass), and anything cached pre-gate can be served by an OLD
// still-controlling SW. For shell files that risk is bounded: "/" and friends are
// network-first/stale-while-revalidate, so a stale copy self-corrects one load later. The book
// manifests have NO such refresh path (generic cache-first handler) — a stale pages.json cached
// here would be served forever — so they are cached strictly, post-gate, after the pages.
const ensureShellAssetsCached = async () => {
  const cache = await caches.open(STATIC_CACHE);
  // Heal only when something is MISSING — but then refetch the WHOLE shell, TRANSACTIONALLY.
  // The presence check keeps healthy-device retries free (zero fetches when the shell is
  // complete — without it, every armed-trigger retry re-downloaded the shell, since no-store
  // bypasses every cache by contract). The refill keeps the shell COHERENT: the cache name pins
  // a deploy, but a no-store fetch returns whatever the origin serves NOW, so healing only the
  // gaps could assemble old index.html + new app.js in one cache — a torn shell that
  // presence-only gates (isShellCached → skipWaiting) would push live, and that crashes on the
  // next OFFLINE boot with no revalidation possible.
  //
  // TRANSACTIONAL means fetch-all-THEN-put-all: a per-asset settle-and-put (the earlier shape)
  // could itself tear the critical pair on weak signal — index.html succeeding at the new
  // deploy's bytes while app.js fails and stays old, OVERWRITING a coherent shell with a torn
  // one. Every fetch must succeed before a single byte is written; any failure heals nothing
  // and the armed retries try again later. A complete-but-stale shell keeps working offline;
  // a torn shell does not.
  const assets = SHELL_ASSETS.map(resolveAppPath);
  let missing = 0;
  for (const asset of assets) {
    if (!(await cache.match(asset))) missing += 1;
  }
  if (missing > 0) {
    // BEST-EFFORT to the caller, transactional inside. The heal must never block the page
    // precache: a persistently-failing single asset (a 404'd icon after some future deploy —
    // install's allSettled leaves it missing on every device forever) would otherwise abort
    // ensureOfflineBundle before a single page fetched, converting a tolerated decorative gap
    // into a total offline-bundle blackout. Stale-beats-blank; an icon never outranks the book.
    // The gap persists, so every later armed-trigger run re-attempts this heal anyway.
    try {
      const fetched = await Promise.all(
        assets.map(async (asset) => {
          const response = await fetch(asset, { cache: "no-store" });
          if (!response.ok) throw new Error(`no se pudo descargar ${asset}`);
          return { asset, response };
        }),
      ); // any rejection propagates: nothing below runs, nothing was written — no torn shell
      for (const { asset, response } of fetched) {
        await cache.put(asset, response.clone());
      }
    } catch (error) {
      console.warn("Shell heal incompleto (se reintentará):", error);
    }
  }
};

const cacheSinglePage = async (cache, pageNumber) => {
  const url = pageFileName(pageNumber);
  const existing = await cache.match(url);
  // REPAIR, not just skip. The presence check is what keeps re-runs free, but a slot poisoned by
  // a build that predates these guards is "present" too, and skipping it would make the poison
  // permanent — a code-only deploy does not rotate PAGE_CACHE (it is keyed by BOOK_VERSION, the
  // book's hash). getCachedPageSet is what surfaces such a page as missing in the first place;
  // this drops the bad bytes so the fetch below can replace them. Healthy devices pay nothing:
  // the match already happened, and a good entry returns here exactly as it always did.
  if (existing && isPoisonedPageEntry(existing)) {
    await cache.delete(url);
  } else if (existing) {
    return false;
  }
  // {cache:"no-store"} is a CONTRACT with the SW, not just an HTTP-cache bypass: the SW's page
  // handler refuses to answer no-store requests from a previous edition's cache, so an offline /
  // weak-signal precache fails honestly instead of "completing" instantly with stale bytes.
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`No se pudo descargar la página ${pageNumber}`);
  }
  // Backstop for the same invariant (e.g. an OLD pre-BOOK_VERSION SW that ignores the no-store
  // contract but whose fallback still tags — or any future code path that serves a tagged
  // response): previous-edition bytes must never be persisted into THIS book's cache as if they
  // were current, nor counted toward its completeness. That silent laundering was the original
  // stale-book bug; failing the download keeps ensureOfflineBundle retryable and the ready flag
  // unset until genuine current-edition bytes arrive.
  if (response.headers.get("X-SV-Prev-Edition")) {
    throw new Error(`Página ${pageNumber} servida de una edición anterior — reintentando en línea`);
  }
  // Same shape as the check above, same reason: a 200 that is not an image must fail the download
  // rather than be persisted and counted. Throwing keeps ensureOfflineBundle retryable and leaves
  // the ready flag unset until the PoP can actually serve this page.
  if (!isPageImageResponse(response)) {
    throw new Error(`Página ${pageNumber} no devolvió una imagen — reintentando en línea`);
  }
  await cache.put(url, response.clone());
  return true;
};

// Ask the CONTROLLING service worker which book version it serves. Returns:
//   null       — no controller (first-ever load; fetches go straight to the network — safe)
//   "<hex>"    — the controller's BOOK_VERSION
//   "no-reply" — controller didn't answer after retries (an OLD pre-BOOK_VERSION SW, or a hung one)
// Retried with backoff because a cold SW start on an old, memory-pressured iPad can exceed a
// single short window — one transient miss must read as "try again", not "wrong SW".
const controllerBookVersionOnce = (timeoutMs) =>
  new Promise((resolve) => {
    const controller = navigator.serviceWorker?.controller;
    if (!controller) return resolve(null);
    const channel = new MessageChannel();
    const timer = setTimeout(() => resolve("no-reply"), timeoutMs);
    channel.port1.onmessage = (event) => {
      clearTimeout(timer);
      resolve(event.data?.bookVersion || "no-reply");
    };
    try {
      controller.postMessage({ type: "GET_BOOK_VERSION" }, [channel.port2]);
    } catch (_) {
      clearTimeout(timer);
      resolve("no-reply");
    }
  });
const controllerBookVersion = async () => {
  for (const timeoutMs of [1500, 3000, 5000]) {
    const answer = await controllerBookVersionOnce(timeoutMs);
    if (answer !== "no-reply") return answer; // null (no controller) or a real version
  }
  return "no-reply";
};

const ensureOfflineBundle = async (totalPages, onProgress) => {
  // SHELL assets first, before the skew gate below — running this unconditionally is
  // load-bearing: when a deploy's install precache only partially filled the new static cache
  // (offline mid-install), the new SW waits, refusing skipWaiting until the shell is complete.
  // This call is the app-side writer that completes that shell so the next SKIP_WAITING passes.
  // Putting it behind the skew gate (which throws under the old SW — exactly the stuck-WAITING
  // situation) would starve the only healer and wedge the device on the old SW until the next
  // deploy. Shell ONLY: the book manifests are cached post-gate, after the pages (see below).
  await ensureShellAssetsCached();
  // DEPLOY-SKEW GATE. During an update there is a window where NEW app.js (this code) runs under
  // the OLD, still-controlling SW — whose page handler is cache-first and predates the no-store
  // contract. Precaching through it would be answered instantly from the OLD edition's cache and
  // laundered into this book's cache at full speed, then certified "ready". Refuse to PRECACHE
  // PAGES until the controller confirms it serves THIS book version; the controllerchange reload
  // that accompanies a SW update — or the retry triggers in deferOfflinePrecache — re-run this
  // moments later under the right SW. Throwing (vs returning) keeps deferOfflinePrecache's catch
  // in charge: the started flag resets and the next trigger retries.
  const swBook = await controllerBookVersion();
  if (swBook !== null && swBook !== BOOK_VERSION) {
    throw new Error(`sw-version-skew: controller=${String(swBook).slice(0, 16)} app=${BOOK_VERSION}`);
  }
  const cache = await caches.open(PAGE_CACHE);
  const cachedPages = await getCachedPageSet(cache);
  let completed = cachedPages.size;
  onProgress(completed, totalPages);

  const missingPages = [];
  for (let pageNumber = 1; pageNumber <= totalPages; pageNumber += 1) {
    if (!cachedPages.has(pageNumber)) {
      missingPages.push(pageNumber);
    }
  }

  const concurrency = Math.min(4, Math.max(1, missingPages.length));
  const workers = Array.from({ length: concurrency }, async () => {
    while (missingPages.length > 0) {
      const pageNumber = missingPages.shift();
      if (!pageNumber) return;
      await cacheSinglePage(cache, pageNumber);
      completed += 1;
      onProgress(completed, totalPages);
    }
  });

  await Promise.all(workers);
  // Book manifests LAST, strict, and only after every page landed. Ordering is deliberate on
  // both sides: after the pages so a flaky manifest fetch can never block the page precache
  // (the old ensureCoreAssetsCached-first shape re-hit the same weak link before any page ever
  // cached); strict so the "ready" flag below can never certify a bundle whose pages.json /
  // search-index.json are a different edition than its pages — the manifests are served
  // cache-first with no refresh path, so certifying stale ones would pin them forever.
  await cacheAssetsNoStore(
    await caches.open(STATIC_CACHE),
    [resolveAppPath(`/books/${BOOK_ID}/pages.json`), resolveAppPath(`/books/${BOOK_ID}/search-index.json`)],
    { strict: true },
  );
  // Best-effort: a storage-disabled browser must not fail an otherwise-successful cache. The
  // pages ARE cached and the reader works offline regardless; what is lost is only the READINESS
  // CLAIM — isOfflineBundleReady short-circuits to false without the flag, so such a device
  // reports not-ready on the dashboard until a later run sets it. That is the safe direction to
  // fail. No data loss, and no thrown rejection out of a finished download.
  try { localStorage.setItem(OFFLINE_READY_KEY, "ready"); } catch (_) {}
  // Metadata BEFORE the check-in, and the check-in makes no claim of its own. Ordering is the
  // whole point: this used to post fleetCheckin({webCached:true}) — an ASSERTION that bypassed
  // verification via the trailing ...extra spread — and it had to, because the metadata
  // isOfflineBundleReady reads was not written until afterwards. Writing it first lets the
  // completion check-in MEASURE what it reports, so no path can claim the green light unverified.
  await writeOfflineMetadata({
    version: BOOK_VERSION, // paired with isOfflineBundleReady's check — a book claim, not a shell claim
    totalPages,
    verifiedAt: new Date().toISOString(),
  });
};

// Defer the ~13 MB background pre-cache until AFTER the reader is revealed. On weak connections
// an immediate pre-cache competes with the critical first-paint fetches (current page image +
// relay WebSocket) and drags out first load. We wait for gateLifted (poll every 1.5s) plus a
// base settle delay so the critical fetches win the bandwidth, THEN start the pre-cache.
// Non-blocking; guarded-once so it never double-runs.
let offlinePrecacheStarted = false;
// Set the moment the controllerchange handler commits to location.reload(): the dying document
// must not start a fresh precache run (it would re-create the old static cache activate just
// deleted, and start page downloads that die at reload commit). Module state is per-document,
// so the fresh post-reload document starts with this false.
let swReloadPending = false;
const deferOfflinePrecache = (totalPages) => {
  if (NATIVE_FILE_MODE || !("caches" in window)) return;
  if (offlinePrecacheStarted) return;
  const SETTLE_MS = 4000;   // base delay so first paint + relay handshake finish first
  const POLL_MS = 1500;     // how often we re-check the gate after the settle delay
  let lastFailureAt = 0;
  const RETRY_COOLDOWN_MS = 60000; // armed-trigger retries after a failure: at most once a minute
  const kick = () => {
    if (offlinePrecacheStarted || swReloadPending) return;
    offlinePrecacheStarted = true;
    ensureOfflineBundle(totalPages, () => {}).catch((error) => {
      offlinePrecacheStarted = false;   // failed → let a later trigger retry
      lastFailureAt = Date.now();
      console.warn("Pre-cache offline incompleto:", error);
      // Surface honest progress to the fleet dashboard even on failure — a device stuck
      // mid-migration should read as "recaching", not freeze at its boot-time snapshot.
    });
  };
  // Retry entry for the armed triggers: cooldown after a failure so a persistent problem (e.g.
  // a 404'ing manifest on a partially-deployed origin) can't loop the handshake + manifest
  // fetches on every foreground/online flap. The shell heal-check and cache-hit page scan make
  // each retry cheap, but "cheap" times "every visibilitychange" still adds up on cell.
  //
  // A suppressed trigger is DEFERRED, never discarded: 'online' is edge-triggered and fires
  // ONCE when the network returns, and the fleet's iPads run wake-locked with Auto-Lock Never —
  // visibilitychange may never fire either. Discarding the one wakeup inside the cooldown
  // window would leave the precache un-retried for the entire session on exactly those devices.
  let deferredRetryTimer = null;
  const retryKick = () => {
    const remainingMs = RETRY_COOLDOWN_MS - (Date.now() - lastFailureAt);
    if (remainingMs > 0) {
      if (!deferredRetryTimer) {
        deferredRetryTimer = setTimeout(() => {
          deferredRetryTimer = null;
          kick();
        }, remainingMs);
      }
      return;
    }
    kick();
  };
  // RETRY TRIGGERS. A failed run (offline, weak signal, sw-version-skew, handshake no-reply)
  // resets the started flag above, but the gate poll below fires kick() exactly once — without
  // these, one transient failure would cost the whole session's precache. Each is a cheap no-op
  // while a run is in flight or after success (the started flag guards). controllerchange also
  // covers the deploy-skew case where the page is NOT reloaded (SW_RELOAD_FLAG already set).
  //
  // Armed only at the FIRST settle-gated kick, never earlier: these exist to RETRY after a
  // failed run, and the first run cannot have failed before it was allowed to start. Arming
  // them at deferOfflinePrecache() time let a visibilitychange/'online' inside the first ~4s
  // start the full ~13MB precache during first paint on weak signal — the exact contention
  // SETTLE_MS exists to prevent.
  let triggersArmed = false;
  const armRetryTriggers = () => {
    if (triggersArmed) return;
    triggersArmed = true;
    window.addEventListener("online", () => retryKick());
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") retryKick();
    });
    try {
      // controllerchange bypasses the cooldown deliberately: a NEW controlling SW is precisely
      // the event that can resolve an sw-version-skew failure, so waiting a minute would be
      // waiting on nothing.
      navigator.serviceWorker?.addEventListener("controllerchange", () => setTimeout(kick, 1000));
    } catch (_) {}
  };
  const waitForGate = () => {
    if (gateLifted) { armRetryTriggers(); kick(); return; }
    setTimeout(waitForGate, POLL_MS);
  };
  setTimeout(waitForGate, SETTLE_MS);
};

// Turns the OFFLINE_READY_KEY *hint* into a verified readiness CLAIM by re-measuring the caches
// it refers to. This exists because the flag is written once when a bundle completes and nothing
// ever clears it — on its own it is a memory, not a measurement. iOS evicts CacheStorage under
// storage pressure while localStorage survives, so a sacristy iPad can sit there insisting it is
// "ready" with zero pages actually cached, and the pre-Mass dashboard would show it green right
// up until parish wifi dies mid-hymn. fleetCheckin is the caller: the dashboard's green light has
// to mean "this device can still render the book with no network."
//
// NON-CREATING on both caches (see openExistingCache): a check that runs at boot on an evicted
// device must not mint an empty current-book cache as a side effect of asking the question.
const isOfflineBundleReady = async (totalPages) => {
  if (!("caches" in window)) return false;
  // A zero/NaN page count would make the completeness test below vacuously true. Never certify
  // readiness against a total we don't actually know.
  if (!Number.isFinite(totalPages) || totalPages <= 0) return false;
  // Guarded read: localStorage throws in a storage-disabled browser. Treat any failure as
  // "not ready" (safe default → re-verify/re-cache), never let it reject this check.
  let readyFlag = false;
  try { readyFlag = localStorage.getItem(OFFLINE_READY_KEY) === "ready"; } catch (_) {}
  if (!readyFlag) return false;
  try {
    const metadata = await readOfflineMetadata();
    if (!metadata) return false;
    // Book-keyed on purpose: this readiness claim is about the BOOK. A shell deploy must not
    // invalidate it (metadata from a pre-BOOK_VERSION build simply reads as a mismatch → one
    // cheap cache-hit re-verify); a book change must and does.
    if (metadata.version !== BOOK_VERSION) return false;
    if (metadata.totalPages !== totalPages) return false;

    const staticCache = await openExistingCache(STATIC_CACHE);
    if (!staticCache) return false;
    const coreMatches = await Promise.all(
      coreAssets().map((asset) => staticCache.match(asset)),
    );
    if (coreMatches.some((match) => !match)) return false;

    const pageCache = await openExistingCache(PAGE_CACHE);
    if (!pageCache) return false;
    const cachedPages = await getCachedPageSet(pageCache);
    return cachedPages.size >= totalPages;
  } catch {
    return false;
  }
};

// ── History helpers ───────────────────────────────────────────────────────────
const clearInitialUrl = () => {
  if (!initialUrl.search) return;
  // Keep the Release-Safety params visible: ?env=staging pins the device to the
  // isolated staging relay room and ?selftest shows the readiness card. Stripping
  // them would make a RELOAD silently fall back to the LIVE Mass room mid-canary-walk
  // — the exact silent-mode-switch the operator must never hit. Normal visitors never
  // carry these params, so their URL is cleaned exactly as before.
  if (/(?:^|[?&])(env=staging|selftest)(?:=|&|$)/.test(initialUrl.search)) return;
  window.history.replaceState({}, "", initialUrl.pathname || "/");
};

const buildSongPageLookup = (songIndex) => new Map(songIndex.map((entry) => [entry.song, entry.page]));

const findSongIndexAtOrBeforePage = (pageNumber) => {
  let index = -1;
  for (let i = 0; i < state.songIndex.length; i += 1) {
    if (state.songIndex[i].page > pageNumber) break;
    index = i;
  }
  return index;
};

const normalizeSongDraftNumber = (draft) => {
  const trimmed = String(draft ?? "").trim();
  if (!/^\d+$/.test(trimmed)) return null;

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed <= 0) return null;
  return parsed;
};

const resolveSongPage = (songNumber) => {
  const normalized = normalizeSongDraftNumber(songNumber);
  if (normalized === null) return 1;

  const exact = state.songPageLookup.get(normalized);
  if (Number.isFinite(exact)) return exact;

  const next = state.songIndex.find((entry) => entry.song >= normalized);
  return next ? next.page : state.totalPages;
};

const findSongPage = (songNumber) => resolveSongPage(songNumber);

const getCurrentSongNumber = () => {
  const index = findSongIndexAtOrBeforePage(state.currentPage);
  return index >= 0 ? state.songIndex[index].song : 0;
};

const getAdjacentSongPages = () => {
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  const pages = [];
  if (currentSongIndex < 0) {
    if (state.totalSongs > 0) pages.push(state.songIndex[0].page);
    return pages;
  }
  if (currentSongIndex > 0) pages.push(state.songIndex[currentSongIndex - 1].page);
  if (currentSongIndex < state.totalSongs - 1) pages.push(state.songIndex[currentSongIndex + 1].page);
  return [...new Set(pages)];
};

const prefetchSongPage = (pageNumber) => {
  if (pageNumber < 1 || pageNumber > state.totalPages) return;
  if (pageNumber === state.currentPage) return;
  if (state.prefetchedPages.has(pageNumber) || state.prefetchingPages.has(pageNumber)) return;
  state.prefetchingPages.add(pageNumber);
  scheduleIdleWork(() => {
    const img = new Image();
    img.onload = () => {
      state.prefetchedPages.add(pageNumber);
      state.prefetchingPages.delete(pageNumber);
    };
    img.onerror = () => { state.prefetchingPages.delete(pageNumber); };
    img.src = resolvePageSrc(pageNumber);
  });
};

// Warm the immediate page NEIGHBORS so the next page-turn is instant. A live
// follower almost always advances +1, so +1/+2 matter most; -1 covers a step back.
// Fire-and-forget Image() warming — works for file:// (warms the decode) and https
// (warms HTTP + service-worker cache). Scheduled off the critical paint so it never
// delays the current page. URL-keyed via prefetchedPageUrls to skip dupes.
const prefetchNeighborPages = (pageNumber) => {
  const neighbors = [pageNumber + 1, pageNumber + 2, pageNumber - 1];
  const schedule = window.requestIdleCallback
    ? (cb) => window.requestIdleCallback(cb, { timeout: 300 })
    : (cb) => window.setTimeout(cb, 0);
  schedule(() => {
    for (const n of neighbors) {
      if (n < 1 || n > state.totalPages) continue;
      const url = pageFileName(n);
      if (prefetchedPageUrls.has(url)) continue;
      prefetchedPageUrls.add(url);
      const im = new Image();
      im.decoding = "async";
      // Neighbor prefetch must never compete with the CURRENT page's fetch.
      im.fetchPriority = "low";
      im.src = url;
      // Decode ahead so the eventual swap to this page paints instantly (no
      // decode stall). Some browsers reject decode() on detached images — swallow it.
      im.decode().catch(() => {});
    }
  });
};

// ── Status rendering ──────────────────────────────────────────────────────────
const renderStatus = () => {
  // The song index hydrates lazily, off the critical first paint. Until it lands,
  // show a neutral "Página N" instead of a bogus "Canción 0", and park song-nav.
  if (!state.songIndex.length) {
    songStatus.textContent = `Página ${state.currentPage}`;
    songIntroEl.classList.add("is-hidden");
    prevPageButton.disabled = true;
    nextPageButton.disabled = true;
    prevCornerButton.disabled = state.pageHistory.length === 0;
    prevCornerButton.classList.toggle("is-unavailable", state.pageHistory.length === 0);
    return;
  }
  const index = findSongIndexAtOrBeforePage(state.currentPage);
  const entry = index >= 0 ? state.songIndex[index] : null;

  if (entry && entry.title) {
    songStatus.textContent = `${entry.song}. ${entry.title}`;
  } else {
    songStatus.textContent = `Canción ${getCurrentSongNumber()}`;
  }

  // Intro chord display. Guard chords: a malformed entry (intro present but chords
  // not an array) must not crash the status render — hide the intro instead.
  if (entry && entry.intro && Array.isArray(entry.intro.chords)) {
    const { key, solfege, chords, capo } = entry.intro;
    let introText = `Intro en ${key} (${solfege}): ${chords.join(", ")}`;
    if (capo) introText += ` – Capo ${capo}`;
    songIntroEl.textContent = introText;
    songIntroEl.classList.remove("is-hidden");
  } else {
    songIntroEl.classList.add("is-hidden");
  }

  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  const hasPrevSong = currentSongIndex > 0;
  const hasNextSong = currentSongIndex < 0
    ? state.totalSongs > 0
    : currentSongIndex < state.totalSongs - 1;

  const hasHistory = state.pageHistory.length > 0;
  prevPageButton.disabled = !hasPrevSong;
  nextPageButton.disabled = !hasNextSong;
  prevCornerButton.disabled = !hasHistory;
  prevCornerButton.classList.toggle("is-unavailable", !hasHistory);
};

const renderDraft = () => {
  songDisplay.textContent = state.songDraft;
  displayClearButton.classList.toggle("is-hidden", !state.songDraft);
  goButton.disabled = !state.songDraft;
};

// Brief inline feedback on the numpad display — e.g. flashing "Código no válido" when a long code
// is entered on the web (where there is nothing to unlock). Auto-reverts to the live draft so the
// numpad is immediately reusable.
let songDisplayFlashTimer = 0;
const flashSongDisplay = (msg, kind) => {
  if (!songDisplay) return;
  if (songDisplayFlashTimer) clearTimeout(songDisplayFlashTimer);
  songDisplay.textContent = msg;
  songDisplay.classList.remove("is-ok", "is-err");
  songDisplay.classList.add(kind === "ok" ? "is-ok" : "is-err");
  songDisplayFlashTimer = window.setTimeout(() => {
    songDisplay.classList.remove("is-ok", "is-err");
    renderDraft();
  }, 1600);
};

// The native shell reports director/follower role over the bridge; the only surviving
// surface is the tiny "Modo activo / DIRECTOR" badge — show it when this device directs.
// ── The sync pill: what the ROOM is doing, and the control for it ───────────────────────────────
//
// The director role here is not a permission, it is the state of the room: one iPad drives and the
// rest follow. A permission can hide behind a gate; a room state has to be legible from every seat.
//
// It used to be legible from exactly one. The badge appeared only for the director, so the state
// most worth shouting about — NOBODY IS DIRECTING — rendered identically to everything being fine:
// follower controls, no badge, and the ⟳ spinner going quiet, which reads as "connected, settled".
// Eight iPads could sit through a whole Mass each turning its own pages with nothing on any screen
// saying the seat was empty. Every signal needed to know already crossed the native bridge; the web
// simply threw it away.
const SYNC_PILL = {
  // EACH LABEL NAMES THE OUTCOME, not the dialog. "Tomar" alone was vague — take WHAT? — but
  // "Tomar el control" would have been worse: a promise the app cannot keep.
  //
  // Which confirm appears is decided NATIVELY at tap time by `liveDirector`, an 8s window on
  // lastDirectorSnapshotRef (PdfReaderApp.tsx). The pill's SIGUIENDO uses a 15s window on mesh page
  // events, deliberately longer so a brief radio hiccup does not flash NADIE DIRIGE at the choir
  // mid-hymn. Between 8s and 15s the two disagree and the dialog reads "¿Dirigir el coro?", NOT
  // "Tomar el control" — so a label promising the latter is wrong roughly half the time it matters.
  //
  // What never varies is where you end up: directing the choir. The pill says that. The TITLE
  // (SIGUIENDO vs NADIE DIRIGE) and the tint carry the difference between the two situations, and
  // the red takeover warning still appears when it applies.
  directing: { cls: "", title: "DIRECTOR", action: "" },
  // "Tomar", not "Pedir". Nothing approves this: tap, confirm, and you are the director — whoever
  // currently holds it is never asked. A word like "request" would promise a handshake that exists
  // nowhere in this system, and the person tapping would wait for a reply that never comes. The red
  // "⚠️ Ya hay un director activo / Tomar el control" confirm is what makes it safe, not the label
  // being coy about what it does.
  // "Convertirme en director" names the ROLE, reflexively — who is becoming what. Earlier attempts
  // failed in both directions: "Tomar" was a bare verb (take WHAT?), and "Tomar el control" promised
  // the takeover dialog, which only appears inside an 8s window the pill cannot see. Naming the role
  // is true in every case and needs no context to read.
  // SIGUIENDO is gone: CSS collapses this state to a bare breathing dot. The strings stay for
  // screen readers and for the non-native web, where there is no mesh and no dot styling.
  // FOLLOWING collapses to a dot on the ⟳ fab — a word there was a CLAIM, and it lied all morning
  // on 2026-08-17 while devices sat on the wrong song. A light is just a light.
  following: { cls: "is-following", title: "Siguiendo", action: "" },
  // NOBODY is the one state that earns words: it is the only one needing a human to act.
  nobody:    { cls: "is-nobody", title: "Nadie dirige", action: "" },
};

// A director's mesh heartbeat is ~1s. Well past that but well under a page's worth of silence, so a
// brief radio hiccup does not flash "nobody is directing" at the choir mid-hymn.
const DIRECTOR_SEEN_WINDOW_MS = 15000;
let lastDirectorPageAt = 0;
let lastMeshStatus = "";

const syncPillState = () => {
  if (state.nativeSyncRole === "director") return "directing";
  if (Date.now() - lastDirectorPageAt < DIRECTOR_SEEN_WINDOW_MS) return "following";
  // ONLY the native mesh's own verdict flips this to "nobody". Guessing from silence would light
  // the pill during the ~10s of boot discovery on every device, every Sunday, and a warning that
  // cries wolf at the start of every Mass is one nobody reads by the third week.
  if (lastMeshStatus === "self-directed") return "nobody";
  return "following";
};

// PAGE 1 ONLY. The badge answers "which build and which book is this device holding?" — a question
// asked once while checking a device, not something anyone needs floating over the music for the
// other 372 pages. Page 1 is where a person already looks to identify the book, so it costs nothing
// there and stops being clutter everywhere else.
//
// A hoisted declaration, and it re-reads the element each time. renderPage calls this from ~2600
// lines above where the badge is built, and a `const` arrow would sit in the temporal dead zone if
// anything ever calls renderPage at module top level. Today nothing does; this makes that a
// non-question instead of a trap for whoever adds the first one.
const BUILD_BADGE_PAGE = 1;
function syncBuildBadgeVisibility() {
  const el = document.getElementById("build-badge");
  if (el) el.classList.toggle("is-shown", state.currentPage === BUILD_BADGE_PAGE);
  positionBuildBadge();
}

/**
 * GLUE THE STAMP TO THE PAGE, not to the screen.
 *
 * It was fixed to the VIEWPORT's bottom-right at 8px / 38% black. On an iPad in portrait the page
 * fills the width, so it landed on white paper and read fine. On an iPhone the page is letterboxed
 * — object-fit: contain puts BLACK bars above and below — so the stamp landed on black, dark grey
 * on black, and looked like it was missing entirely. It never was; it was invisible.
 *
 * The printed date stamp in the page's lower-LEFT is part of the image and therefore always sits on
 * paper, at the paper's corner, in every orientation and on every device. This makes the version
 * stamp behave the same way: compute where object-fit: contain actually drew the image and pin the
 * badge to THAT rectangle's bottom-right corner.
 *
 * Cheap and idempotent, so it is safe to call from render, resize and orientation change.
 */
function positionBuildBadge() {
  const el = document.getElementById("build-badge");
  const img = document.getElementById("page-image");
  if (!el || !img) return;
  // Defensive: e2e tests execute syncBuildBadgeVisibility against stub elements that have no DOM
  // geometry. Positioning is a nicety; the VISIBILITY rule above is the contract, and it must not
  // be taken down by a missing method on a fake node.
  if (typeof img.getBoundingClientRect !== "function") return;
  const nW = img.naturalWidth, nH = img.naturalHeight;
  const r = img.getBoundingClientRect();
  if (!nW || !nH || !r.width || !r.height) return;   // nothing drawn yet; a later call will fix it
  // object-fit: contain — the drawn box is the natural size scaled to fit, then centred.
  const scale = Math.min(r.width / nW, r.height / nH);
  const drawnW = nW * scale, drawnH = nH * scale;
  const right = r.left + (r.width - drawnW) / 2 + drawnW;
  const bottom = r.top + (r.height - drawnH) / 2 + drawnH;
  // A hair inside the paper's edge, scaled with the page so it looks the same at any size.
  const inset = Math.max(3, Math.round(drawnW * 0.008));
  el.style.right = `${Math.max(0, Math.round(window.innerWidth - right + inset))}px`;
  el.style.bottom = `${Math.max(0, Math.round(window.innerHeight - bottom + inset))}px`;
}
// No listeners here on purpose: setViewportCssVars owns every viewport event and calls this once
// the CSS vars that size the <img> have been written. Two independent listeners racing to position
// against the same element is how you get a stamp that is correct on resize but not on rotation.

const renderDirectorModeBadge = () => {
  const isDirector = state.nativeSyncRole === "director";
  // Drive the control layout: followers (web + any non-director native) get ⟳ resync + ♪;
  // a director gets ♪ + ⌕ search. Default "follower" so signovivo.com is right from boot.
  document.documentElement.dataset.role = isDirector ? "director" : "follower";
  if (!directorModeBadge) return;
  // Native shell only. On signovivo.com there is no mesh and no role to take, so the pill would be
  // describing a room the viewer is not in.
  const inShell = NATIVE_FILE_MODE || hasNativeBridge();
  // Published for CSS so ★ Ser Director can be gated on it. Set on EVERY render rather than once at
  // init: the function that used to hide the keypad's copy of that control ran exactly once and
  // never again on a role change, so it never actually hid once you were directing.
  document.documentElement.dataset.shell = inShell ? "native" : "web";
  if (!inShell) {
    directorModeBadge.classList.add("is-hidden");
    return;
  }
  const key = syncPillState();
  const st = SYNC_PILL[key];
  // Drive the crossed-out ⟳ from the SAME verdict that drives the pill, so the two can never
  // disagree — one state, one source. On <html> beside data-role rather than a body class, to
  // match the existing pattern and to be in place before first paint.
  document.documentElement.dataset.mesh = key;
  directorModeBadge.classList.remove("is-following", "is-nobody");
  if (st.cls) directorModeBadge.classList.add(st.cls);
  directorModeBadge.classList.remove("is-hidden");
  const titleEl = document.getElementById("sync-pill-title");
  const actionEl = document.getElementById("sync-pill-action");
  if (titleEl) titleEl.textContent = st.title;
  if (actionEl) actionEl.textContent = st.action;
  // BRANCH ON THE KEY, NOT THE TITLE. This compared st.title === "NADIE DIRIGE" while the title is
  // "Nadie dirige" — the branch could never match, so a follower with NO director was announced to
  // a screen reader as "Estás siguiendo al director": the opposite of the truth, in the one state
  // that needs a human. Both surviving labels also promised that tapping takes the role, which it
  // has not done since build 435.
  directorModeBadge.setAttribute(
    "aria-label",
    key === "directing" ? "Estás dirigiendo. Tocar para salir."
      : key === "nobody" ? "Nadie está dirigiendo."
      : "Estás siguiendo al director.",
  );
  // While nobody directs, the pill is display:none and the crossed-out ⟳ IS the indicator — so the
  // announcement has to move to the control that is actually on screen, or the state goes silent.
  const resyncEl = document.getElementById("resync-fab");
  if (resyncEl) {
    resyncEl.setAttribute("aria-label", key === "nobody"
      ? "Nadie está dirigiendo. Tocar para saber más."
      : "Volver a sincronizar con el director");
  }
};

// ── Sync "working" indicator ──────────────────────────────────────────────────
// Reuses the ⟳ resync fab's own tap-spin — same "is-spinning" class, same ~1.1s duration — as the
// "still working" indicator for native mesh state (searching / connecting / resolving-conflict),
// instead of a bespoke top-center pill. One visual language: whether the user taps ⟳ or the native
// mesh is quietly retrying underneath, the same icon spins the same way for the same length of time.
// Hidden the instant we're connected (or idle / self-directed). Native-only: it's driven by the
// mesh's state events; web relay followers use the relay pill instead.
const SYNC_SPIN_MS = 1100;
let syncSpinTimer = 0;
const setSyncWorking = (status) => {
  const working = status === "searching" || status === "connecting" || status === "resolving-conflict";
  const fab = document.getElementById("resync-fab");
  if (!fab) return;
  // Every status transition re-decides the spin, so clear any pending stop first.
  if (syncSpinTimer) { clearTimeout(syncSpinTimer); syncSpinTimer = 0; }
  if (working) {
    fab.classList.add("is-spinning");
    syncSpinTimer = window.setTimeout(() => {
      syncSpinTimer = 0;
      fab.classList.remove("is-spinning");
    }, SYNC_SPIN_MS);
  } else {
    fab.classList.remove("is-spinning");
  }
};

// ── Relay-auth warning banner ─────────────────────────────────────────────────
// Shown to the DIRECTOR (inside the native WebView) when the relay rejects their publish (a bad or
// retired director code → 401). This is the ONLY signal that the web congregation on signovivo.com
// has gone dark — without it the failure is invisible. Fixed, dismissible, idempotent (a re-fire
// re-shows the same banner, never stacks). The native shell latches upstream so this can't fire on
// every page turn.
let relayAuthWarningEl = null;
const showRelayAuthWarning = (status) => {
  if (relayAuthWarningEl) {
    // Already built — a later re-fire (recovery → re-failure) just re-reveals it without stacking.
    relayAuthWarningEl.classList.add("is-on");
    return;
  }
  const style = document.createElement("style");
  style.textContent =
    "#sv-relay-warn{position:fixed;top:max(0.7rem,env(safe-area-inset-top,0px));left:50%;" +
    "transform:translateX(-50%);z-index:255;display:none;max-width:min(92vw,34rem);" +
    "align-items:flex-start;gap:0.55rem;padding:0.8rem 0.9rem;border-radius:0.85rem;" +
    "background:#b91c1c;color:#fff;font:600 0.92rem/1.35 system-ui,-apple-system,sans-serif;" +
    "box-shadow:0 6px 22px rgba(0,0,0,.45);-webkit-tap-highlight-color:transparent}" +
    "#sv-relay-warn.is-on{display:flex;animation:sv-relaywarn-in .2s ease}" +
    "@keyframes sv-relaywarn-in{from{opacity:0;transform:translate(-50%,-8px)}" +
    "to{opacity:1;transform:translate(-50%,0)}}" +
    "#sv-relay-warn .sv-rw-x{flex:0 0 auto;margin-left:0.25rem;width:1.6rem;height:1.6rem;border:0;" +
    "border-radius:50%;background:rgba(255,255,255,0.22);color:#fff;font-size:1.05rem;line-height:1;" +
    "cursor:pointer;-webkit-tap-highlight-color:transparent}";
  document.head.appendChild(style);
  relayAuthWarningEl = document.createElement("div");
  relayAuthWarningEl.id = "sv-relay-warn";
  relayAuthWarningEl.setAttribute("role", "alert");
  relayAuthWarningEl.setAttribute("aria-live", "assertive");
  const msg = document.createElement("span");
  msg.textContent = "El relé rechazó el código de director. Los seguidores en signovivo.com NO están sincronizados.";
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "sv-rw-x";
  closeBtn.setAttribute("aria-label", "Cerrar aviso");
  closeBtn.textContent = "×";
  closeBtn.addEventListener("click", () => relayAuthWarningEl.classList.remove("is-on"));
  relayAuthWarningEl.appendChild(msg);
  relayAuthWarningEl.appendChild(closeBtn);
  document.body.appendChild(relayAuthWarningEl);
  // Add the reveal class synchronously (not via rAF alone, which can be throttled in a backgrounded
  // WebView): guarantees the banner is displayed even if the slide-in keyframe is skipped.
  relayAuthWarningEl.classList.add("is-on");
};
const hideRelayAuthWarning = () => {
  if (relayAuthWarningEl) relayAuthWarningEl.classList.remove("is-on");
};

// ── Neutral sync notice ───────────────────────────────────────────────────────
// Same shape as the relay-auth banner above, in slate instead of red, for things the operator must
// SEE but that are not failures: "you got the direction back", "someone else is directing now".
//
// It exists because the alternative was a native Alert. A modal during Mass blocks the page the
// choir is reading from and requires someone to notice and tap it — at the exact moment they are
// singing. Informational text should never take the screen hostage. Auto-dismisses; the message is
// a courtesy, not a decision.
let syncNoticeEl = null;
let syncNoticeTimer = null;
// Set from inside the init closure below. A toast needs to be able to OPEN the role gate, and the
// gate's opener lives in that closure; without this the toast could only describe the control, which
// is exactly how it went stale for weeks.
let requestRoleGate = null;

const showSyncNotice = (text, action) => {
  if (!text) return;
  if (!syncNoticeEl) {
    const style = document.createElement("style");
    style.textContent =
      "#sv-sync-note{position:fixed;top:max(0.7rem,env(safe-area-inset-top,0px));left:50%;" +
      "transform:translateX(-50%);z-index:250;display:none;max-width:min(92vw,34rem);" +
      "align-items:flex-start;gap:0.55rem;padding:0.8rem 0.9rem;border-radius:0.85rem;" +
      "background:#1e293b;color:#fff;font:600 0.92rem/1.35 system-ui,-apple-system,sans-serif;" +
      "box-shadow:0 6px 22px rgba(0,0,0,.45);-webkit-tap-highlight-color:transparent}" +
      "#sv-sync-note.is-on{display:flex}" +
      "#sv-sync-note .sv-sn-act{flex:0 0 auto;margin-left:0.4rem;padding:0.4rem 0.7rem;border:0;" +
      "border-radius:0.6rem;background:#0A84FF;color:#fff;font:700 0.86rem/1 system-ui,sans-serif;" +
      "cursor:pointer;-webkit-tap-highlight-color:transparent}" +
      "#sv-sync-note .sv-sn-x{flex:0 0 auto;margin-left:0.25rem;width:1.6rem;height:1.6rem;border:0;" +
      "border-radius:50%;background:rgba(255,255,255,0.22);color:#fff;font-size:1.05rem;line-height:1;" +
      "cursor:pointer;-webkit-tap-highlight-color:transparent}";
    document.head.appendChild(style);
    syncNoticeEl = document.createElement("div");
    syncNoticeEl.id = "sv-sync-note";
    syncNoticeEl.setAttribute("role", "status");
    syncNoticeEl.setAttribute("aria-live", "polite");
    const msg = document.createElement("span");
    msg.className = "sv-sn-msg";
    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "sv-sn-x";
    closeBtn.setAttribute("aria-label", "Cerrar aviso");
    closeBtn.textContent = "×";
    closeBtn.addEventListener("click", () => syncNoticeEl.classList.remove("is-on"));
    // OPTIONAL ACTION. A notice that tells someone to go find a control is a promise about the UI
    // that rots the moment the UI moves — the "toca el estado arriba a la izquierda" text outlived
    // the pill that took the role by three builds and pointed at the wrong corner the whole time.
    // Carrying the control removes the description, and with it that whole class of bug.
    const actBtn = document.createElement("button");
    actBtn.type = "button";
    actBtn.className = "sv-sn-act";
    actBtn.style.display = "none";
    syncNoticeEl.appendChild(msg);
    syncNoticeEl.appendChild(actBtn);
    syncNoticeEl.appendChild(closeBtn);
    document.body.appendChild(syncNoticeEl);
  }
  syncNoticeEl.querySelector(".sv-sn-msg").textContent = String(text);
  const actBtn = syncNoticeEl.querySelector(".sv-sn-act");
  if (action && action.label && typeof action.onTap === "function") {
    actBtn.textContent = action.label;
    actBtn.style.display = "";
    actBtn.onclick = () => { syncNoticeEl.classList.remove("is-on"); action.onTap(); };
  } else {
    actBtn.style.display = "none";
    actBtn.onclick = null;
  }
  // Synchronously, not via rAF — a backgrounded WebView throttles rAF and the notice would never
  // appear (same reason the relay banner does it this way).
  syncNoticeEl.classList.add("is-on");
  if (syncNoticeTimer) clearTimeout(syncNoticeTimer);
  syncNoticeTimer = setTimeout(() => syncNoticeEl && syncNoticeEl.classList.remove("is-on"), 12000);
};

// ── Diagnostics dump ──────────────────────────────────────────────────────────
// Renders the native crumb ring buffer full-screen, selectable, newest LAST so it reads as a
// timeline. Deliberately ugly and text-only: its job is to be read over the phone by someone
// standing in a church, or copied into a message afterwards.
// The dump gets read aloud over the phone by a chorister. "follower"/"off" in an otherwise
// all-Spanish UI is a word they have to spell out.
const ROLE_ES = { director: "director", follower: "siguiendo", off: "sin conexión" };
let diagEl = null;
const showDiagnostics = (payload) => {
  if (!diagEl) {
    diagEl = document.createElement("div");
    diagEl.id = "sv-diag";
    diagEl.setAttribute("role", "dialog");
    diagEl.setAttribute("aria-modal", "true");
    diagEl.setAttribute("aria-label", "Diagnóstico del dispositivo");
    const header = document.createElement("header");
    const label = document.createElement("span");
    label.className = "sv-diag-label";
    const close = document.createElement("button");
    close.type = "button";
    close.textContent = "Cerrar";
    close.addEventListener("click", hideDiagnostics);
    header.appendChild(label);
    header.appendChild(close);
    const pre = document.createElement("pre");
    pre.className = "sv-diag-body";

    // ── Debug settings, right where you are already looking ────────────────────────────────────
    //
    // sv.logSink and sv.telemetry live in AsyncStorage, which only the native shell can write, so
    // until now they could only be set by rebuilding — absurd for two strings whose entire purpose
    // is to be changed mid-session. This is the one screen a person opens when something is wrong,
    // so it is where they belong.
    //
    // Pointing the sink at scripts/log-sink.mjs on Miguel's Mac makes telemetry cost Cloudflare
    // NOTHING, which is what allows debug-level logging without competing with signovivo.com for
    // the same 100,000/day quota.
    const dbg = document.createElement("div");
    dbg.className = "sv-diag-dbg";
    const sinkInput = document.createElement("input");
    sinkInput.type = "url";
    sinkInput.placeholder = "http://192.168.x.x:8787  (vacío = Cloudflare)";
    sinkInput.className = "sv-diag-sink";
    sinkInput.autocapitalize = "none";
    sinkInput.autocomplete = "off";
    sinkInput.spellcheck = false;
    const telLabel = document.createElement("label");
    telLabel.className = "sv-diag-tel";
    const telBox = document.createElement("input");
    telBox.type = "checkbox";
    telLabel.appendChild(telBox);
    telLabel.appendChild(document.createTextNode(" Telemetría"));
    const save = document.createElement("button");
    save.type = "button";
    save.className = "sv-diag-save";
    save.textContent = "Guardar";
    save.addEventListener("click", () => {
      postNativeBridge({ type: "set-debug-settings", logSink: sinkInput.value, telemetry: telBox.checked });
    });
    dbg.appendChild(sinkInput);
    dbg.appendChild(telLabel);
    dbg.appendChild(save);

    diagEl.appendChild(header);
    diagEl.appendChild(dbg);
    diagEl.appendChild(pre);
    document.body.appendChild(diagEl);
  }
  const lines = Array.isArray(payload.lines) ? payload.lines : [];
  diagEl.querySelector(".sv-diag-label").textContent =
    [
      `build ${payload.build || "?"}`,
      // A genuine 0 must not render as "?" — native sends `totalPagesRef.current || 0`, so
      // "this device resolved zero pages" is exactly the state diagnostics exist to surface.
      `${Number.isFinite(payload.pages) ? payload.pages : "?"} págs.`,
      ROLE_ES[payload.role] || payload.role || "?",
      `${lines.length} eventos`,
    ].join(" · ") + `\n${payload.book || "?"}`;
  diagEl.querySelector(".sv-diag-body").textContent =
    lines.length ? lines.join("\n") : "Sin eventos registrados todavía.";
  // Native echoes the live values in the payload, so the fields show what IS set rather than
  // whatever was typed last time — a settings box that lies is worse than no settings box.
  const sinkEl = diagEl.querySelector(".sv-diag-sink");
  const telEl = diagEl.querySelector(".sv-diag-tel input");
  if (sinkEl && typeof payload.logSink === "string") sinkEl.value = payload.logSink;
  if (telEl) telEl.checked = Boolean(payload.telemetry);
  diagEl.classList.add("is-on");
};
// True while the dump covers the screen. The rescue handlers closeSongJump() before posting, so
// state.songJumpOpen is false and the global keydown handler would otherwise fall through to
// turnSong(±1) — silently moving the whole congregation under an opaque overlay.
const diagnosticsOpen = () => Boolean(diagEl && diagEl.classList.contains("is-on"));
const hideDiagnostics = () => { if (diagEl) diagEl.classList.remove("is-on"); };

const applyNativeSyncEvent = async (payload) => {
  // Whole-body guard (M2 Slice C): the native shell calls this via evaluateJavaScript on
  // EVERY mesh sync event. A throw from any branch (a malformed payload, or a downstream
  // renderPage / badge / warning call) would propagate back into the bridge — dropping that
  // sync event and risking a wedged channel on the choir's iPads. Swallow + record a
  // breadcrumb (like the boot guard's __SV_LAST_ERROR) so the NEXT event is always handled
  // cleanly. The happy path below is unchanged.
  try {
    if (!payload || typeof payload !== "object") return;

    if (payload.type === "bridge-state") {
      state.nativeBridgeAvailable = Boolean(payload.available);
      return;
    }

    // Single-book app: a legacy "set-book" message from an older native shell is a no-op.
    if (payload.type === "set-book") return;

    // Role changes after a code / conflict / takeover. Store it and surface the tiny
    // director badge — never throw.
    if (payload.type === "role") {
      if (typeof payload.role === "string") {
        state.syncRole = payload.role;
        if (payload.role === "director" || payload.role === "follower" || payload.role === "none") {
          state.nativeSyncRole = payload.role === "none" ? "off" : payload.role;
          renderDirectorModeBadge();
        }
      }
      return;
    }

    // The relay REJECTED the director's publish (401 bad/retired code). The native shell bridges it
    // here as a one-time signal so the director sees a visible warning — otherwise the publish fails
    // silently and every signovivo.com follower freezes for the whole Mass. (Native latches it, so
    // this never fires on every page turn.)
    if (payload.type === "relay-auth-error") {
      showRelayAuthWarning(payload.status);
      return;
    }

    // Publishing recovered. Take the banner down: it is shown on failure and, before this existed,
    // could only be removed by tapping its × — so a fixed relay went on shouting, and a stale
    // warning looks exactly like a live one.
    if (payload.type === "relay-auth-ok") {
      hideRelayAuthWarning();
      return;
    }

    // Neutral operator notices from the native shell — director role resumed after a crash, or
    // refused because another device picked it up during the reboot.
    if (payload.type === "toast") {
      // The one action a toast can carry today. Named rather than passed as a function because this
      // arrives over the native bridge as JSON, and it still goes through the SAME typed-word gate —
      // it opens the gate, it does not take the role.
      const act = payload.action === "resume-director" && requestRoleGate
        ? { label: "Volver a dirigir", onTap: () => requestRoleGate() }
        : null;
      showSyncNotice(payload.text, act);
      return;
    }

    // The crumb log, rendered for a human to read or read ALOUD over the phone. There is no
    // internet in the church, so this is the only account of what a device did during Mass.
    if (payload.type === "diagnostics") {
      showDiagnostics(payload);
      return;
    }

    if (payload.type !== "sync-event") return;

    const event = payload.event || {};

    // Connection lifecycle from the native mesh → drive the "working" spinner.
    if (event.type === "state") {
      const status = String(event.status || "");
      lastMeshStatus = status;
      // "self-directed" is the mesh saying it waited its full timeout and found no director. It was
      // reaching the web already and being dropped on the floor.
      setSyncWorking(status);
      renderDirectorModeBadge();
      return;
    }

    if (event.type === "page" && Number.isFinite(event.page)) {
      // WHERE DID THIS PAGE COME FROM? Native tags every page with the channel that delivered it
      // ("mesh" or "ble"); the relay path logs itself separately in applyRelaySnapshot. Without this
      // a page simply APPEARS in front of the choir and the only honest answer to "why is it on song
      // 2" is a guess — which is exactly where 2026-08-18 left us.
      try {
        console.info("[sv] page", event.page, "via", event.src || "mesh?", "(was", state.currentPage + ")");
      } catch {}
      // A page arriving over the mesh is proof a director is alive right now — the only such proof
      // this layer gets, and enough to keep the pill honest without any new plumbing.
      lastDirectorPageAt = Date.now();
      renderDirectorModeBadge();
      // Single-book app: ignore any event.book and just render the director's page.
      renderPage(event.page, { pushToHistory: false });
      // First real page since this WebView mounted — let initReader's reveal gate through.
      // Idempotent: a resolved promise ignores every later resolve() call, and this fires on
      // every page turn for the rest of the session, not just the first one.
      if (resolveFirstNativePageSignal) { resolveFirstNativePageSignal(); resolveFirstNativePageSignal = null; }
    }
  } catch (err) {
    try {
      window.__SV_LAST_ERROR = {
        where: "native-sync-event",
        msg: String((err && err.message) || err || ""),
        t: Date.now(),
      };
    } catch (_) {}
  }
};

window.__signoVivoReceiveNativeEvent = applyNativeSyncEvent;

// ── Image loading ─────────────────────────────────────────────────────────────
const preloadImage = (src, timeoutMs = PAGE_IMAGE_LOAD_TIMEOUT_MS) => new Promise((resolve, reject) => {
  const loader = new Image();
  loader.decoding = "async";

  let settled = false;
  const finish = (handler, value) => {
    if (settled) return;
    settled = true;
    window.clearTimeout(timer);
    loader.removeEventListener("load", onLoad);
    loader.removeEventListener("error", onError);
    handler(value);
  };

  const onLoad = () => finish(resolve, "ready");
  const onError = () => finish(reject, new Error("No se pudo decodificar la imagen"));
  const timer = window.setTimeout(() => finish(resolve, "timeout"), timeoutMs);

  loader.addEventListener("load", onLoad, { once: true });
  loader.addEventListener("error", onError, { once: true });
  loader.src = src;
});

const loadPageImage = async (pageNumber, retryToken = "") => {
  const url = resolvePageSrc(pageNumber, retryToken);
  // Preload into an OFF-DOM Image only — never touch the live <img> here. Committing
  // pageImage.src before renderPage's stale-request guard (the requestId check) ran would
  // flash the older page when fast page-turns stack: a slow loadPageImage(A) resolving after
  // renderPage(B) started would slam A onto the visible <img> even though B already won. The
  // live <img> is committed exactly once, post-guard, in renderPage (pageImage.src = nextPageUrl).
  const loadState = await preloadImage(url);
  return { url, loadState };
};

const renderPage = async (pageNumber, { pushToHistory = true, direction = 0, userInitiated = false } = {}) => {
  const nextPage = clampPage(pageNumber);

  // ALREADY ON THIS PAGE — do nothing. A director's mesh heartbeat arrives once per SECOND, and
  // every one of them called through here even when the page had not moved. Each pass bumped the
  // load request, scheduled a loading indicator, reassigned pageImage.src, and posted a
  // `page-changed` message to native — which writes AsyncStorage. Sixty times a minute, for the
  // length of a Mass, on an eight-year-old iPad. That is the shape of the crash reported on
  // 2026-08-06: a follower left on one screen for several minutes, doing nothing.
  //
  // Guarded narrowly on purpose: only when the page is unchanged AND the <img> is genuinely showing
  // it. A pending or failed load still falls through and re-renders, so a follower can never get
  // stuck on a blank while the mesh insists the page is current.
  if (
    nextPage === state.currentPage &&
    pageImageMatches(nextPage) &&
    pageImage.complete &&
    pageImage.naturalWidth > 0
  ) {
    return;
  }

  // STILL FAILING THIS PAGE — do not hammer it. The error overlay is already up and native is
  // re-driving us once a second; without this the retry rate is the heartbeat rate. Cleared on any
  // successful render and by an explicit human retry, so this only ever paces a page that is
  // genuinely not renderable right now.
  // PACING NEVER APPLIES TO A HUMAN. This guard exists to blunt the director's 1 Hz re-drive when a
  // page genuinely cannot render, and it does that job. But it lives in renderPage, so it was also
  // swallowing SWIPES: a page that failed once became unreachable by hand for five seconds, with no
  // error and no feedback — the swipe simply did nothing.
  //
  // Reported 2026-08-17: "I was on song 3 or 4 and it wouldn't let me swipe to songs 4 or 5... going
  // back and returning then retrying fixed the issue". That is this window expiring, not a fix.
  //
  // svRenderPace's own docstring already said retries are cleared "by an explicit human retry" —
  // that intent was never wired to a caller. A person asking for a page is not a retry storm; they
  // asked once, and they are entitled to the attempt and to the error overlay if it fails.
  if (!userInitiated && svShouldPaceRender(state.lastRenderFailure, nextPage, Date.now())) return;

  const requestId = state.pageLoadRequest + 1;
  state.pageLoadRequest = requestId;
  scheduleLoadingIndicator();

  try {
    let nextPageUrl = "";
    let loadState = "ready";
    if (pageImageMatches(nextPage) && pageImage.complete && pageImage.naturalWidth > 0) {
      nextPageUrl = resolvePageSrc(nextPage);
    } else {
      try {
        ({ url: nextPageUrl, loadState } = await loadPageImage(nextPage));
      } catch (firstError) {
        console.warn("Primer intento falló al cargar la página", nextPage, firstError);
        ({ url: nextPageUrl, loadState } = await loadPageImage(nextPage, Date.now()));
      }
    }

    if (requestId !== state.pageLoadRequest) return;

    if (pushToHistory && state.currentPage > 0 && state.currentPage !== nextPage) {
      state.pageHistory.push(state.currentPage);
      if (state.pageHistory.length > 50) state.pageHistory.shift();
    }

    state.currentPage = nextPage;
    state.lastRenderFailure = null; // this page renders again — stop pacing it
    syncBuildBadgeVisibility();
    pageImage.src = nextPageUrl;
    pageImage.dataset.page = String(nextPage);
    if (loadState === "timeout") {
      console.warn("La carga de la página tardó demasiado", nextPage);
    }
    postNativeBridge({
      type: "page-changed",
      page: nextPage,
      totalPages: state.totalPages,
      book: state.currentBook,
    });
    if (direction !== 0) {
      const animClass = direction > 0 ? "slide-from-right" : "slide-from-left";
      pageImage.classList.remove("slide-from-right", "slide-from-left");
      void pageImage.offsetWidth;
      pageImage.classList.add(animClass);
      pageImage.addEventListener("animationend", () => pageImage.classList.remove(animClass), { once: true });
    }
    renderStatus();
    hideLoadingIndicator();
    getAdjacentSongPages().forEach(prefetchSongPage);
    // Warm the next page(s) so the director's +1 page-turn lands instantly. Fire-and-
    // forget; scheduled off this paint so it never delays the current page.
    prefetchNeighborPages(nextPage);
  } catch (error) {
    if (requestId !== state.pageLoadRequest) return;
    clearLoadingTimer();
    console.error("No se pudo cargar la página solicitada", nextPage, error);
    setLoading(true, "No se pudo cargar esta página.");
    closeDrawer();
    // Tell native the render did NOT take. The mesh de-dupe optimistically sets currentPageRef
    // to P before injecting, so a director heartbeat re-sending P is dropped — leaving an offline
    // follower wedged on the error overlay even though the heartbeat is faithfully re-sending the
    // right page. A render-failed signal lets native clear its optimistic ref so the next
    // heartbeat re-drives the render. (Native must handle this message for it to take effect —
    // paired fix; harmless/no-op if unhandled.)
    state.lastRenderFailure = { page: nextPage, at: Date.now() };
    postNativeBridge({
      type: "render-failed",
      page: nextPage,
      book: state.currentBook,
    });
  }
};

// Reflect the (single) active book on the root element for any book-scoped CSS.
const updateBookLabel = () => {
  document.documentElement.dataset.book = BOOK_ID;
};

// ── Drawer open / close ───────────────────────────────────────────────────────
const openDrawer = () => {
  state.drawerOpen = true;
  overlayControls.classList.add("drawer-open");
  drawerHandle.classList.add("is-hidden");
  // Hide the floating ♪/⌕ while the drawer is open — they'd overlap it, and the drawer
  // already surfaces navigation. (Fabs sit before overlay-controls in the DOM, so a CSS
  // sibling selector can't reach them — a body class does.)
  document.body.classList.add("sv-drawer-open");
  // Drawer is browse-only now (jump-to-song lives in the modal); always open to it.
  switchDrawerMode("browse");
};

const closeDrawer = () => {
  state.drawerOpen = false;
  overlayControls.classList.remove("drawer-open");
  navigationDrawer.classList.remove("as-dropdown");   // exit ⌕-dropdown mode if it was on
  drawerHandle.classList.remove("is-hidden");
  document.body.classList.remove("sv-drawer-open");
};

// ── Fullscreen ────────────────────────────────────────────────────────────────
const updateFullscreenButton = () => {
  // Hide on native too: the iPad shell is already fullscreen, so a fullscreen toggle is
  // meaningless there (and some WKWebViews falsely report fullscreen support).
  if (!supportsFullscreen || NATIVE_FILE_MODE) {
    fullscreenButton.classList.add("is-hidden");
    fullscreenFab?.classList.add("is-hidden");
    return;
  }
  fullscreenButton.classList.remove("is-hidden");
  fullscreenFab?.classList.remove("is-hidden");
};

// ── Draft management ──────────────────────────────────────────────────────────
const appendDigit = (digit) => {
  // Songs are <= 3 digits, but director / secret codes run up to 10 digits and are
  // entered on this same numpad (then routed to native in goToDraftSong).
  if (state.songDraft.length >= 10) return;
  state.songDraft = `${state.songDraft}${digit}`;
  renderDraft();
};

const clearDraft = () => {
  state.songDraft = "";
  renderDraft();
};

const backspaceDraft = () => {
  state.songDraft = state.songDraft.slice(0, -1);
  renderDraft();
};

// ── Jump-to-song modal (native-style) ──────────────────────────────────────────
const openSongJump = () => {
  clearDraft();
  // Offer to direct only where it can work (the native shell owns the mesh) and only to a device
  // that is not already directing. Re-evaluated on every open rather than once at boot, because the
  // role changes mid-session — a director who steps down must be able to take it back.
  const inShell = NATIVE_FILE_MODE || hasNativeBridge();

  songJumpModal.classList.remove("is-hidden");
  state.songJumpOpen = true;
};

const closeSongJump = () => {
  songJumpModal.classList.add("is-hidden");
  state.songJumpOpen = false;
  clearDraft();
};

const goToDraftSong = () => {
  const songNumber = normalizeSongDraftNumber(state.songDraft);
  if (songNumber === null) { closeSongJump(); return; }
  // Director / secret codes are LONG codes (5+ digits) ONLY. Song NUMBERS are not page numbers —
  // resolveSongPage maps a song number to its page — so gate on digit-length alone: 2-4 digit songs
  // always resolve here, and a genuine 5+ digit code still routes to native.
  if (String(songNumber).length >= 5) {
    const code = String(songNumber);
    // NATIVE: hand the long code to the shell (director / transmitter flow over Multipeer).
    // WEB: the app is fully public and single-book — there is nothing to unlock, so a long code
    // is meaningless; flash an error and keep the numpad open.
    if (NATIVE_FILE_MODE || hasNativeBridge()) {
      // currentPage rides with the code the same way it already rides with request-director's tap
      // (2026-08-18 fix, aa68c9e) — closes the SAME wrong-page-flash class of bug for the typed
      // numpad path, which never got the fix originally because request-director was the only
      // path anyone tested it through.
      postNativeBridge({ type: "director-code", code, currentPage: state.currentPage });
      clearDraft();
      closeSongJump();
    } else {
      clearDraft();
      flashSongDisplay("Código no válido", "err");
    }
    return;
  }
  const targetPage = findSongPage(songNumber);
  renderPage(targetPage, { userInitiated: true });                    // IR A CANTO keypad
  addToRecientes(songNumber);
  closeSongJump();
  // Jumping OFF the director's live page = intentional browsing: pause auto-follow and
  // surface the "Volver a en vivo" bar so getting back is obvious. (Jumping TO the
  // director's page keeps you live.)
  if (relay.hasDirector && relay.livePage != null && targetPage !== relay.livePage) {
    relay.browsing = true;
    relay.following = false;
    showGoLiveBar();
    renderRelayPill();
  }
};

const goBackInHistory = () => {
  if (state.pageHistory.length === 0) return;
  const prevPage = state.pageHistory.pop();
  renderPage(prevPage, { pushToHistory: false, userInitiated: true }); // back
};

// ── Search index loading ──────────────────────────────────────────────────────
const loadSearchIndex = async () => {
  const inlined = document.getElementById("search-data");
  if (inlined) {
    // Defensive inline parse: a malformed inline blob must not throw out of this
    // un-awaited loader → fall through to the fetch path instead.
    try {
      const data = JSON.parse(inlined.textContent);
      state.searchIndexPages = data.pages || [];
      return;
    } catch (error) {
      console.warn("Índice de búsqueda inline inválido, recurriendo a la red", error);
    }
  }
  try {
    // Default cache mode ON PURPOSE (no-store is reserved for cache WRITERS): this is a display
    // READER, and it must flow through the SW's network-first-with-cache-fallback branch so an
    // offline device can read the search index its own precache wrote. With no-store it would hit
    // the SW's network-only passthrough and search would silently die offline.
    const response = await fetch(resolveAppPath(`/books/${BOOK_ID}/search-index.json`));
    const data = await response.json();
    state.searchIndexPages = data.pages || [];
  } catch (error) {
    console.warn("No se pudo cargar el índice de búsqueda", error);
  }
};

const getSongForPage = (pageNum) => {
  let songNum = 0;
  for (const entry of state.songIndex) {
    if (entry.page > pageNum) break;
    songNum = entry.song;
  }
  return songNum;
};

// ── Search ────────────────────────────────────────────────────────────────────
const searchPages = (query) => {
  const normalizedQuery = normalizeText(query.trim());
  if (!normalizedQuery) return [];
  const words = normalizedQuery.split(/\s+/).filter(Boolean);
  // Title lookup by song number (the raw search index only carries {page, text};
  // song/title live in songIndex — same source renderSearchResults reads).
  const titleBySong = new Map();
  for (const s of state.songIndex) if (s.title) titleBySong.set(s.song, s.title);
  // Decorate each text match with its resolved song # + title so we can rank/sort.
  const decorated = [];
  for (const entry of state.searchIndexPages) {
    // Skip malformed entries with no string text — they can't match and downstream
    // snippet rendering (entry.text.slice) would otherwise throw.
    if (typeof entry?.text !== "string") continue;
    const normalizedText = normalizeText(entry.text);
    if (!words.every((word) => normalizedText.includes(word))) continue;
    const song = getSongForPage(entry.page);
    decorated.push({ entry, song: song > 0 ? song : Infinity, title: titleBySong.get(song) || "" });
  }
  // Sort BEFORE the 30-cap so the best matches survive it. state.searchSortMode restores the old
  // native search's re-sort: "best" (relevance: exact title → title-prefix → title-contains →
  // lyric-only, then by song #), "num" (song number), "az" (title A→Z). Default "best".
  const rank = (d) => {
    const t = normalizeText(d.title);
    if (!t) return 3;
    if (t === normalizedQuery) return 0;
    if (t.startsWith(normalizedQuery)) return 1;
    if (words.every((w) => t.includes(w))) return 2;
    return 3;
  };
  const mode = state.searchSortMode || "best";
  decorated.sort((a, b) => {
    if (mode === "num") return a.song - b.song;
    if (mode === "az") return normalizeText(a.title).localeCompare(normalizeText(b.title)) || a.song - b.song;
    return rank(a) - rank(b) || a.song - b.song; // "best"
  });
  return decorated.slice(0, 30).map((d) => d.entry);
};

const renderSearchResults = (results, query) => {
  searchResults.innerHTML = "";
  if (results.length === 0) {
    const p = document.createElement("p");
    p.className = "search-no-results";
    p.textContent = "Sin resultados.";
    searchResults.appendChild(p);
    return;
  }

  const normalizedQuery = normalizeText(query.trim());
  const normalizedQ = normalizedQuery.split(/\s+/)[0];
  const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

  for (const entry of results) {
    const songNum = getSongForPage(entry.page);
    const songEntry = songNum > 0 ? state.songIndex.find((e) => e.song === songNum) : null;
    const title = songEntry?.title || "";

    const item = document.createElement("button");
    item.className = "search-result-item";
    item.type = "button";
    item.dataset.page = String(entry.page);

    const header = document.createElement("div");
    header.className = "search-result-header";

    const numBadge = document.createElement("span");
    numBadge.className = "search-result-num";
    numBadge.textContent = songNum > 0 ? String(songNum) : `p.${entry.page}`;

    const titleSpan = document.createElement("span");
    titleSpan.className = "search-result-title";
    titleSpan.textContent = title;

    header.appendChild(numBadge);
    header.appendChild(titleSpan);

    const snippet = document.createElement("span");
    snippet.className = "search-result-snippet";
    const lowerText = normalizeText(entry.text);
    const matchIdx = lowerText.indexOf(normalizedQ);
    const start = Math.max(0, matchIdx - 30);
    const raw = entry.text.slice(start, start + 140).replace(/\s+/g, " ");
    const prefix = start > 0 ? "…" : "";
    const normRaw = normalizeText(raw);
    const matchInRaw = normRaw.indexOf(normalizedQ);
    if (matchInRaw >= 0) {
      snippet.innerHTML =
        esc(prefix) +
        esc(raw.slice(0, matchInRaw)) +
        "<mark>" + esc(raw.slice(matchInRaw, matchInRaw + normalizedQ.length)) + "</mark>" +
        esc(raw.slice(matchInRaw + normalizedQ.length));
    } else {
      snippet.textContent = prefix + raw;
    }

    item.appendChild(header);
    item.appendChild(snippet);
    searchResults.appendChild(item);
  }
};

const searchByTheme = (query) => {
  if (!state.themeIndex.length) return null;
  const norm = normalizeText(query.trim());
  if (!norm) return null;
  for (const theme of state.themeIndex) {
    const normId = normalizeText(String(theme.id ?? "").replace(/_/g, " "));
    const normLabel = normalizeText(String(theme.label ?? ""));
    if (normLabel.includes(norm) || normId.includes(norm) || norm === theme.emoji) {
      const songs = state.songIndex.filter((entry) => entry.themes && entry.themes.includes(theme.id));
      return { theme, songs };
    }
  }
  return null;
};

const renderThemeResults = (songs, themeLabel) => {
  searchResults.innerHTML = "";
  const header = document.createElement("p");
  header.className = "search-theme-header";
  header.textContent = themeLabel;
  searchResults.appendChild(header);
  if (songs.length === 0) {
    const p = document.createElement("p");
    p.className = "search-no-results";
    p.textContent = "Sin canciones etiquetadas.";
    searchResults.appendChild(p);
    return;
  }
  // Honor the search-result sort toggle here too (a theme set has no query-relevance
  // ranking, so "best" and "num" both fall to song #; "az" sorts by title).
  const mode = state.searchSortMode || "best";
  const ordered = [...songs].sort((a, b) =>
    mode === "az"
      ? normalizeText(a.title || "").localeCompare(normalizeText(b.title || "")) || (a.song || 0) - (b.song || 0)
      : (a.song || 0) - (b.song || 0));
  for (const entry of ordered) {
    const item = document.createElement("button");
    item.className = "search-result-item";
    item.type = "button";
    item.dataset.page = String(entry.page);

    const hdr = document.createElement("div");
    hdr.className = "search-result-header";

    const numBadge = document.createElement("span");
    numBadge.className = "search-result-num";
    numBadge.textContent = String(entry.song);

    const titleSpan = document.createElement("span");
    titleSpan.className = "search-result-title";
    titleSpan.textContent = entry.title || "";

    hdr.appendChild(numBadge);
    hdr.appendChild(titleSpan);
    item.appendChild(hdr);
    searchResults.appendChild(item);
  }
};

// ── Index panel helpers ───────────────────────────────────────────────────────

// Use pre-computed keys from pages.json
const computeSongKeys = () => {
  if (cachedSongKeys) return cachedSongKeys;
  cachedSongKeys = new Map(state.songIndex.map((e) => [e.song, e.key || null]));
  return cachedSongKeys;
};

const computeSongLengths = () => {
  if (cachedSongLengths) return cachedSongLengths;
  cachedSongLengths = new Map(
    state.songIndex.map((e) => {
      const next = state.songIndex.find((n) => n.song > e.song);
      const end = next ? next.page - 1 : state.totalPages;
      const len = state.searchIndexPages
        .filter((p) => p.page >= e.page && p.page <= end)
        .reduce((s, p) => s + (typeof p?.text === "string" ? p.text.length : 0), 0);
      return [e.song, len];
    }),
  );
  return cachedSongLengths;
};

// Build a "No. 142" song button for index panels
const makeSongButton = (entry) => {
  const btn = document.createElement("button");
  btn.className = "search-result-item";
  btn.type = "button";
  btn.dataset.page = String(entry.page);
  const hdr = document.createElement("div");
  hdr.className = "search-result-header";
  const num = document.createElement("span");
  num.className = "search-result-num";
  num.textContent = `No. ${entry.song}`;
  const title = document.createElement("span");
  title.className = "search-result-title";
  title.textContent = entry.title || `Canción ${entry.song}`;
  hdr.appendChild(num);
  hdr.appendChild(title);
  btn.appendChild(hdr);
  return btn;
};

// Render a row of sort tab pills at the top of an index content area.
const renderSortTabs = (container, tabs, currentSort, context) => {
  const row = document.createElement("div");
  row.className = "index-sort-tabs";
  for (const tab of tabs) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.dataset.context = context;
    if (tab.isAlphaToggle) {
      const isActive = currentSort === "az" || currentSort === "za";
      btn.className = `index-sort-tab${isActive ? " is-active" : ""}`;
      btn.textContent = currentSort === "za" ? "↓ Z–A" : "↑ A–Z";
      btn.dataset.sortId = currentSort === "az" ? "za" : "az";
    } else {
      btn.className = `index-sort-tab${tab.id === currentSort ? " is-active" : ""}`;
      btn.textContent = tab.label;
      btn.dataset.sortId = tab.id;
    }
    row.appendChild(btn);
  }
  container.appendChild(row);
};

const INDEX_TABS = [
  { id: "themes",     emoji: "🎵", label: "Temas"      },
  { id: "alpha",      emoji: "📋", label: "Todas"      },
  { id: "key",        emoji: "🎸", label: "Tono"       },
  { id: "length",     emoji: "📏", label: "Largo"      },
  { id: "keywords",   emoji: "💬", label: "Palabras"   },
  { id: "complexity", emoji: "🎯", label: "Dificultad" },
];

// ── Liturgical calendar ──────────────────────────────────────────────────────
const computeEaster = (year) => {
  const a = year % 19, b = Math.floor(year / 100), c = year % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3), h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4), k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const mo = Math.floor((h + l - 7 * m + 114) / 31) - 1;
  const dy = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(year, mo, dy);
};

const getLiturgicalSeason = () => {
  const today = new Date();
  const year = today.getFullYear();
  const add = (d, n) => { const r = new Date(d); r.setDate(r.getDate() + n); return r; };
  const MONTHS = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
  const fmt = (d) => MONTHS[d.getMonth()];

  const easter = computeEaster(year);
  const ashWed = add(easter, -46);
  const holyThursday = add(easter, -3);
  const pentecost = add(easter, 49);
  const dec25 = new Date(year, 11, 25);
  const dow = dec25.getDay();
  const advent = add(dec25, dow === 0 ? -28 : -(dow + 21));
  const baptismOfLord = new Date(year, 0, 13);

  const seasonMonths = {
    adviento:    `${fmt(advent)}–Dic`,
    navidad:     "Dic–Ene",
    cuaresma:    `${fmt(ashWed)}–${fmt(add(easter, -1))}`,
    pascua:      `${fmt(easter)}–${fmt(pentecost)}`,
    espiritu_santo: fmt(pentecost),
    resurreccion:   `${fmt(easter)}–${fmt(pentecost)}`,
  };

  let season;
  const t = today;
  if (t >= advent && t < dec25)                            season = "adviento";
  else if (t >= dec25 || (t.getMonth() === 0 && t <= baptismOfLord)) season = "navidad";
  else if (t >= ashWed && t < holyThursday)                season = "cuaresma";
  else if (t >= holyThursday && t < add(pentecost, 1))     season = "pascua";
  else                                                      season = "ordinario";

  const nextMap = {
    adviento: "navidad", navidad: "ordinario",
    cuaresma: "pascua",  pascua: "ordinario",
    ordinario: t < ashWed ? "cuaresma" : t < advent ? "adviento" : "navidad",
  };
  const next = nextMap[season];

  const currentThemeIds = {
    adviento: ["adviento", "navidad"],
    navidad:  ["navidad"],
    cuaresma: ["cuaresma"],
    pascua:   ["resurreccion", "espiritu_santo"],
    ordinario: [],
  }[season] ?? [];

  const nextThemeIds = {
    adviento: ["navidad"],
    navidad:  [],
    cuaresma: ["resurreccion"],
    pascua:   ["espiritu_santo"],
    ordinario: next === "cuaresma" ? ["cuaresma"] : next === "adviento" ? ["adviento"] : [],
  }[season] ?? [];

  return { season, next, currentThemeIds, nextThemeIds, seasonMonths };
};

// ── Index content renderers ───────────────────────────────────────────────────

const renderIndexThemesContent = (container) => {
  renderSortTabs(container, [
    { isAlphaToggle: true },
    { id: "count", label: "Cantidad" },
  ], state.indexSortPrefs.themes, "themes");

  const allThemes = state.themeIndex
    .map((t) => ({
      ...t,
      songs: state.songIndex.filter((e) => e.themes?.includes(t.id)),
    }))
    .filter((t) => t.songs.length > 0);

  const { currentThemeIds, nextThemeIds, seasonMonths } = getLiturgicalSeason();

  let themes;
  if (state.indexSortPrefs.themes === "count") {
    themes = [...allThemes].sort((a, b) => b.songs.length - a.songs.length);
  } else if (state.indexSortPrefs.themes === "za") {
    themes = [...allThemes].sort((a, b) => normalizeText(b.label).localeCompare(normalizeText(a.label)));
  } else {
    const seasonal = new Set([...currentThemeIds, ...nextThemeIds]);
    const seasonalThemes = allThemes
      .filter((t) => seasonal.has(t.id))
      .sort((a, b) => {
        const ia = currentThemeIds.indexOf(a.id) >= 0 ? 0 : 1;
        const ib = currentThemeIds.indexOf(b.id) >= 0 ? 0 : 1;
        return ia - ib;
      });
    const restThemes = allThemes
      .filter((t) => !seasonal.has(t.id))
      .sort((a, b) => normalizeText(a.label).localeCompare(normalizeText(b.label)));
    themes = [...seasonalThemes, ...restThemes];
  }

  const makeThemeChip = (theme, badge) => {
    const chip = document.createElement("button");
    chip.className = "index-theme-chip";
    chip.type = "button";
    chip.dataset.themeLabel = theme.label;
    const months = seasonMonths[theme.id] || null;
    const monthsHtml = months ? `<span class="index-chip-months">${months}</span>` : "";
    const badgeHtml = badge ? `<span class="index-chip-badge">${badge}</span>` : "";
    chip.innerHTML = `<span class="index-chip-emoji">${theme.emoji}</span><span class="index-chip-label">${theme.label}${monthsHtml}</span>${badgeHtml}<span class="index-chip-count">${theme.songs.length}</span>`;
    return chip;
  };

  const grid = document.createElement("div");
  grid.className = "index-themes-grid";

  if (state.indexSortPrefs.themes !== "count" && state.indexSortPrefs.themes !== "za"
      && (currentThemeIds.length > 0 || nextThemeIds.length > 0)) {
    const hdr = document.createElement("p");
    hdr.className = "index-group-header";
    hdr.textContent = "🗓 Tiempo Litúrgico";
    grid.appendChild(hdr);
    const seasonSet = new Set([...currentThemeIds, ...nextThemeIds]);
    for (const theme of themes.filter((t) => seasonSet.has(t.id))) {
      const isCurrent = currentThemeIds.includes(theme.id);
      grid.appendChild(makeThemeChip(theme, isCurrent ? "AHORA" : "PRÓX."));
    }
    const restHdr = document.createElement("p");
    restHdr.className = "index-group-header";
    restHdr.textContent = "Todos los temas";
    grid.appendChild(restHdr);
    for (const theme of themes.filter((t) => !seasonSet.has(t.id))) {
      grid.appendChild(makeThemeChip(theme, null));
    }
  } else {
    for (const theme of themes) {
      grid.appendChild(makeThemeChip(theme, null));
    }
  }

  container.appendChild(grid);
};

const renderIndexAlphaContent = (container) => {
  renderSortTabs(container, [
    { isAlphaToggle: true },
  ], state.indexSortPrefs.alpha, "alpha");

  const dir = state.indexSortPrefs.alpha === "za" ? -1 : 1;
  const withTitle = [...state.songIndex]
    .filter((e) => e.title)
    .sort((a, b) => dir * normalizeText(a.title).localeCompare(normalizeText(b.title)));
  const noTitle = state.songIndex.filter((e) => !e.title).sort((a, b) => a.song - b.song);
  const sorted = dir > 0 ? [...withTitle, ...noTitle] : [...noTitle, ...withTitle];

  const groups = {};
  for (const entry of sorted) {
    const letter = (entry.title
      ? normalizeText(entry.title[0]).toUpperCase()
      : "#");
    if (!groups[letter]) groups[letter] = [];
    groups[letter].push(entry);
  }
  const letters = Object.keys(groups).sort((a, b) => dir * a.localeCompare(b));
  for (const letter of letters) {
    const hdr = document.createElement("p");
    hdr.className = "index-group-header";
    hdr.textContent = letter;
    container.appendChild(hdr);
    for (const entry of groups[letter]) {
      container.appendChild(makeSongButton(entry));
    }
  }
};

const renderIndexKeyContent = (container) => {
  const keys = computeSongKeys();
  const grouped = {};
  for (const entry of state.songIndex) {
    const k = keys.get(entry.song) || "—";
    if (!grouped[k]) grouped[k] = [];
    grouped[k].push(entry);
  }

  const KEY_ORDER = ["C", "Db", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"];
  const sortedKeys = Object.keys(grouped).sort((a, b) => {
    if (a === "—") return 1;
    if (b === "—") return -1;
    const ia = KEY_ORDER.indexOf(a), ib = KEY_ORDER.indexOf(b);
    if (ia === -1 && ib === -1) return a.localeCompare(b);
    if (ia === -1) return 1;
    if (ib === -1) return -1;
    return ia - ib;
  });

  const SOLFEGE = {
    C: "Do", D: "Re", E: "Mi", F: "Fa", G: "Sol", A: "La", B: "Si",
    "C#": "Do sostenido", "D#": "Re sostenido", "F#": "Fa sostenido",
    "G#": "Sol sostenido", "A#": "La sostenido",
    Db: "Re bemol", Eb: "Mi bemol", Gb: "Sol bemol", Ab: "La bemol", Bb: "Si bemol",
  };

  for (const k of sortedKeys) {
    const hdr = document.createElement("p");
    hdr.className = "index-group-header index-group-header--sticky";
    if (k === "—") {
      hdr.textContent = "Sin tono";
    } else {
      const sol = SOLFEGE[k] || k;
      hdr.textContent = sol !== k ? `${sol} (${k})` : k;
    }
    container.appendChild(hdr);
    for (const entry of grouped[k].sort((a, b) => a.song - b.song)) {
      container.appendChild(makeSongButton(entry));
    }
  }
};

const renderIndexLengthContent = (container) => {
  renderSortTabs(container, [
    { id: "longest",  label: "Más largo" },
    { id: "shortest", label: "Más corto" },
    { id: "medium",   label: "Mediana" },
  ], state.indexSortPrefs.length, "length");

  const lengths = computeSongLengths();
  const mode = state.indexSortPrefs.length;

  if (mode === "medium") {
    const allLens = [...lengths.values()].sort((a, b) => b - a);
    const p25 = allLens[Math.floor(allLens.length * 0.25)] ?? 0;
    const p75 = allLens[Math.floor(allLens.length * 0.75)] ?? 0;
    const sorted = [...state.songIndex]
      .filter((e) => {
        const len = lengths.get(e.song) ?? 0;
        return len >= p75 && len <= p25;
      })
      .sort((a, b) => (lengths.get(b.song) ?? 0) - (lengths.get(a.song) ?? 0));

    const hdr = document.createElement("p");
    hdr.className = "index-group-header";
    hdr.textContent = "Largo mediano";
    container.appendChild(hdr);
    for (const entry of sorted) container.appendChild(makeSongButton(entry));
  } else {
    const dir = mode === "shortest" ? 1 : -1;
    const sorted = [...state.songIndex]
      .sort((a, b) => dir * ((lengths.get(b.song) ?? 0) - (lengths.get(a.song) ?? 0)));

    const bannerHdr = document.createElement("p");
    bannerHdr.className = "index-group-header";
    bannerHdr.textContent = mode === "shortest" ? "Canción más corta →" : "Canción más larga →";
    container.appendChild(bannerHdr);
    for (const entry of sorted) container.appendChild(makeSongButton(entry));
  }
};

// Keyword helpers
const STOPWORDS = new Set([
  "de","la","el","en","y","a","que","es","los","las","por","para","un","una","con",
  "del","al","se","su","tu","me","te","le","lo","mi","si","mas","pero","o","ni",
  "muy","ya","nos","como","sus","son","fue","oh","ti","hay","no","e","u","les",
  "han","era","ser","bien","aun","pues","tan","vez","este","esta","cada","todo",
  "toda","cuando","donde","quien","cual","eres","somos","voy",
]);

const getKeywordFreq = (text) => {
  const freq = new Map();
  for (const w of normalizeText(text).split(/\W+/)) {
    if (w.length >= 3 && !STOPWORDS.has(w) && !/^\d+$/.test(w)) {
      freq.set(w, (freq.get(w) || 0) + 1);
    }
  }
  return [...freq.entries()].sort((a, b) => b[1] - a[1]);
};

const getTopKeywords = () => {
  if (cachedKeywords) return cachedKeywords;
  const freq = new Map();
  for (const page of state.searchIndexPages) {
    for (const w of normalizeText(page.text).split(/\W+/)) {
      if (w.length >= 3 && !STOPWORDS.has(w) && !/^\d+$/.test(w)) {
        freq.set(w, (freq.get(w) || 0) + 1);
      }
    }
  }
  cachedKeywords = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 40).map(([w]) => w);
  return cachedKeywords;
};

const makeKeywordChip = (word) => {
  const chip = document.createElement("button");
  chip.className = "index-keyword-chip";
  chip.type = "button";
  chip.dataset.keyword = word;
  chip.textContent = word;
  return chip;
};

const renderIndexKeywordsContent = (container) => {
  const currentSort = state.indexSortPrefs.keywords;
  renderSortTabs(container, [
    { id: "freq",  label: "Frecuencia" },
    { id: "theme", label: "Por Tema" },
    { isAlphaToggle: true },
  ], currentSort, "keywords");

  if (currentSort === "theme") {
    for (const theme of state.themeIndex) {
      const themeSongs = state.songIndex.filter((e) => e.themes?.includes(theme.id));
      if (!themeSongs.length) continue;

      const themeText = themeSongs.flatMap((e) => {
        const next = state.songIndex.find((n) => n.song > e.song);
        const end = next ? next.page - 1 : state.totalPages;
        return state.searchIndexPages
          .filter((p) => p.page >= e.page && p.page <= end)
          .map((p) => p.text);
      }).join(" ");

      const themeWords = getKeywordFreq(themeText).slice(0, 8).map(([w]) => w);
      if (!themeWords.length) continue;

      const hdr = document.createElement("p");
      hdr.className = "index-group-header";
      hdr.textContent = `${theme.emoji} ${theme.label}`;
      container.appendChild(hdr);

      const wrap = document.createElement("div");
      wrap.className = "index-keywords-wrap";
      for (const word of themeWords) wrap.appendChild(makeKeywordChip(word));
      container.appendChild(wrap);
    }
  } else {
    let words = getTopKeywords();
    if (currentSort === "az") words = [...words].sort((a, b) => a.localeCompare(b));
    else if (currentSort === "za") words = [...words].sort((a, b) => b.localeCompare(a));

    const wrap = document.createElement("div");
    wrap.className = "index-keywords-wrap";
    for (const word of words) wrap.appendChild(makeKeywordChip(word));
    container.appendChild(wrap);
  }
};

const COMPLEXITY_LABELS = {
  simple:   { label: "Simple",    emoji: "🟢", desc: "1–3 acordes únicos" },
  medio:    { label: "Medio",     emoji: "🟡", desc: "4–5 acordes únicos" },
  avanzado: { label: "Avanzado",  emoji: "🔴", desc: "6+ acordes o complejos" },
};

const renderIndexComplexityContent = (container) => {
  renderSortTabs(container, [
    { id: "simple",   label: "🟢 Simple"  },
    { id: "medio",    label: "🟡 Medio"   },
    { id: "avanzado", label: "🔴 Avanzado"},
  ], state.indexSortPrefs.complexity || "simple", "complexity");

  const mode = state.indexSortPrefs.complexity || "simple";
  const order = mode === "avanzado" ? ["avanzado","medio","simple"]
              : mode === "medio"    ? ["medio","avanzado","simple"]
              :                       ["simple","medio","avanzado"];

  for (const c of order) {
    const info = COMPLEXITY_LABELS[c];
    const songs = state.songIndex.filter((e) => (e.complexity || "simple") === c);
    if (!songs.length) continue;
    const hdr = document.createElement("p");
    hdr.className = "index-group-header index-group-header--sticky";
    hdr.textContent = `${info.emoji} ${info.label} — ${info.desc} (${songs.length})`;
    container.appendChild(hdr);
    for (const entry of songs.sort((a, b) => a.song - b.song)) {
      container.appendChild(makeSongButton(entry));
    }
  }
};

const renderIndexTabContent = (container) => {
  container.innerHTML = "";
  switch (state.indexTab) {
    case "themes":     renderIndexThemesContent(container);     break;
    case "alpha":      renderIndexAlphaContent(container);      break;
    case "key":        renderIndexKeyContent(container);        break;
    case "length":     renderIndexLengthContent(container);     break;
    case "keywords":   renderIndexKeywordsContent(container);   break;
    case "complexity": renderIndexComplexityContent(container); break;
  }
};

// ── Index panel ───────────────────────────────────────────────────────────────
const renderIndexPanel = () => {
  state.indexVisible = true;
  state.indexDrillDown = false;
  drawerBack.classList.add("is-hidden");
  searchInput.value = "";
  searchClearButton.classList.add("is-hidden");
  searchIndexButton?.classList.add("is-active");
  navigationDrawer.classList.add("search-focused", "index-visible");
  searchColHeader.classList.add("is-hidden");

  searchResults.innerHTML = "";
  const layout = document.createElement("div");
  layout.className = "index-layout";

  const sidebar = document.createElement("nav");
  sidebar.className = "index-sidebar";
  for (const tab of INDEX_TABS) {
    const isActive = state.indexTab === tab.id;
    const btn = document.createElement("button");
    btn.className = `index-tab-btn${isActive ? " is-active" : ""}`;
    btn.type = "button";
    btn.dataset.tabId = tab.id;
    btn.innerHTML = `<span class="index-tab-emoji">${tab.emoji}</span><span class="index-tab-label">${tab.label}</span>`;
    sidebar.appendChild(btn);
  }

  const content = document.createElement("div");
  content.className = "index-content";
  renderIndexTabContent(content);

  layout.appendChild(sidebar);
  layout.appendChild(content);
  searchResults.appendChild(layout);
};

const activateSearchFromIndex = (query) => {
  state.indexVisible = false;
  state.indexDrillDown = true;
  drawerBack.classList.remove("is-hidden");
  searchIndexButton?.classList.remove("is-active");
  navigationDrawer.classList.remove("index-visible");
  navigationDrawer.classList.add("search-focused");
  searchInput.value = query;
  searchClearButton.classList.remove("is-hidden");
  searchColHeader.classList.remove("is-hidden");
  handleSearchInput();
};

const clearSearch = () => {
  state.indexVisible = false;
  state.indexDrillDown = false;
  drawerBack.classList.add("is-hidden");
  searchIndexButton?.classList.remove("is-active");
  navigationDrawer.classList.remove("index-visible");
  // Return to where the user came from (or todas as fallback)
  activateTab(state.prevTab || "todas");
};

const handleSearchInput = () => {
  if (state.indexVisible) {
    state.indexVisible = false;
    searchIndexButton?.classList.remove("is-active");
    navigationDrawer.classList.remove("index-visible");
  }
  const query = searchInput.value;
  searchClearButton.classList.toggle("is-hidden", !query);

  if (!query.trim()) {
    searchResults.innerHTML = "";
    searchColHeader.classList.add("is-hidden");
    renderActiveTab();
    return;
  }

  searchColHeader.classList.remove("is-hidden");
  const themeResult = searchByTheme(query);
  if (themeResult) {
    renderThemeResults(themeResult.songs, `${themeResult.theme.emoji} ${themeResult.theme.label}`);
    return;
  }
  const results = searchPages(query);
  renderSearchResults(results, query);
};

// ── Song / page navigation ────────────────────────────────────────────────────
const turnSong = (direction, { keepOverlay = false } = {}) => {
  if (direction === 0 || state.totalSongs === 0) return;
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  if (currentSongIndex < 0) {
    if (direction > 0) {
      renderPage(state.songIndex[0].page, { direction, userInitiated: true });
      clearDraft();
      if (!keepOverlay) closeDrawer();
    }
    return;
  }
  const nextIndex = clampSongIndex(currentSongIndex + direction);
  if (nextIndex === currentSongIndex) return;
  renderPage(state.songIndex[nextIndex].page, { direction, userInitiated: true });  // next/prev song
  clearDraft();
  if (!keepOverlay) closeDrawer();
};

const turnPage = (direction) => {
  const nextPage = clampPage(state.currentPage + direction);
  if (nextPage === state.currentPage) return;
  renderPage(nextPage, { direction, userInitiated: true });          // swipe / arrow
};

// ── Fullscreen toggle ─────────────────────────────────────────────────────────
const toggleFullscreen = async () => {
  if (!supportsFullscreen) return;
  if (canOfferPseudoFullscreen) {
    // For iOS standalone, just try to go fullscreen (limited effect)
    return;
  }
  try {
    if (isFullscreen()) await exitFullscreen();
    else await requestFullscreen();
  } catch (error) {
    console.error("No se pudo cambiar la pantalla completa", error);
  } finally {
    updateFullscreenButton();
  }
};

// ── Service worker ────────────────────────────────────────────────────────────
const requestServiceWorkerActivation = (worker) => {
  if (!worker) return;
  try {
    sessionStorage.setItem(SW_RELOAD_FLAG, "1");
  } catch {}
  worker.postMessage({ type: "SKIP_WAITING" });
};

const wireServiceWorkerRegistration = (registration) => {
  if (!registration) return;

  if (registration.waiting) {
    requestServiceWorkerActivation(registration.waiting);
  }

  registration.addEventListener("updatefound", () => {
    const installing = registration.installing;
    if (!installing) return;

    installing.addEventListener("statechange", () => {
      if (installing.state === "installed" && navigator.serviceWorker.controller) {
        requestServiceWorkerActivation(installing);
      }
    });
  });

  const refreshRegistration = () => registration.update().catch(() => {});
  window.addEventListener("online", refreshRegistration);
  // Adaptive new-deploy check: fast (3s) right after the tab is foregrounded — so a
  // follower picks up a fresh deploy within seconds during active use/testing — then
  // back off ×1.5 toward 60s while it sits idle, sparing weak-cell data + battery over
  // a long Mass. Foregrounding snaps it back to 3s. On a new version the SW chain
  // auto-reloads the tab (controllerchange handler).
  let swUpdateTimer = 0;
  let swUpdateDelay = 3000;
  const scheduleUpdateCheck = () => {
    clearTimeout(swUpdateTimer);
    swUpdateTimer = setTimeout(() => {
      if (document.visibilityState === "visible") refreshRegistration();
      swUpdateDelay = Math.min(Math.round(swUpdateDelay * 1.5), 60000);
      scheduleUpdateCheck();
    }, swUpdateDelay);
  };
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      refreshRegistration();
      swUpdateDelay = 3000;
      scheduleUpdateCheck();
    }
  });
  scheduleUpdateCheck();
};

const registerServiceWorker = async () => {
  if (NATIVE_FILE_MODE) return;
  if (!("serviceWorker" in navigator) || !window.isSecureContext) return;
  try {
    // Was the page already controlled by a SW at boot? If so, ANY later controllerchange is a
    // real UPDATE (a newer SW took over) — not a first install. We must reload to drop the stale
    // app.js even when THIS tab didn't post SKIP_WAITING: a new SW can activate on its own (no
    // other clients, or a sibling tab already skipped waiting), firing controllerchange with the
    // SW_RELOAD_FLAG unset. The old flag-only guard then left the returning cached device — the
    // one that matters most at a parish — running stale book/geo/relay logic against a newer
    // worker until a manual reload. First install (no controller at boot) stays no-reload, and
    // hasReloadedForUpdate prevents any reload loop.
    const hadControllerAtBoot = Boolean(navigator.serviceWorker.controller);
    let hasReloadedForUpdate = false;
    navigator.serviceWorker.addEventListener("controllerchange", () => {
      if (hasReloadedForUpdate) return;
      const flagged = sessionStorage.getItem(SW_RELOAD_FLAG) === "1";
      // Reload on a flagged activation (this tab requested it) OR on any controller swap when a
      // controller already existed at boot (a true update that activated without our flag).
      if (!flagged && !hadControllerAtBoot) return;   // first install — nothing to refresh
      hasReloadedForUpdate = true;
      swReloadPending = true; // this document is dying — no precache kick may start in it
      sessionStorage.removeItem(SW_RELOAD_FLAG);
      window.location.reload();
    });

    const registration = await navigator.serviceWorker.register(resolveAppPath("/sw.js"), {
      updateViaCache: "none",
    });
    wireServiceWorkerRegistration(registration);
    await registration.update();
  } catch (error) {
    console.error("No se pudo registrar el service worker", error);
  }
};

// ── Swipe gesture tracking ────────────────────────────────────────────────────
// State for edge-swipe (right edge → open drawer)
let edgeSwipe = null;
// State for drawer swipe (swipe right inside drawer → close)
let drawerSwipe = null;

// ── Tab rendering functions ──────────────────────────────────────────────────

const MISA_PARTS = [
  // ── Ordered by liturgical sequence ──────────────────────────────────────────
  { label: "🚪 Entrada",
    check: (s) => (s.themes || []).includes("entrada") || (s.title && /\bentrada\b/i.test(s.title)) },
  { label: "🙏 Señor Ten Piedad (Kyrie)",
    check: (s) => s.title && /piedad|kyrie|ten\s+piedad/i.test(s.title) },
  { label: "✨ Gloria",
    check: (s) => s.title && /\bgloria\b/i.test(s.title) },
  { label: "📖 Salmo Responsorial",
    check: (s) => s.title && /\bsalmo\b/i.test(s.title) },
  { label: "🎺 Aleluya",
    check: (s) => s.title && /\baleluya\b|\baluluya\b/i.test(s.title) },
  { label: "🕊️ Santo / Hosanna",
    check: (s) => s.title && /\bsanto\b|\bhosanna\b|\bsanctus\b/i.test(s.title) },
  { label: "🥖 Cordero de Dios (Agnus Dei)",
    check: (s) => s.title && /cordero|agnus/i.test(s.title) },
  { label: "🍞 Comunión / Eucaristía",
    check: (s) => (s.themes || []).includes("eucaristia") },
  { label: "🕯️ Ofertorio / Presentación",
    check: (s) => (s.themes || []).includes("ofertorio") || (s.title && /ofertorio/i.test(s.title)) },
  { label: "🚶 Procesión",
    check: (s) => (s.themes || []).includes("procesion") },
  { label: "🌟 Envío / Salida",
    check: (s) => (s.themes || []).includes("envio") || (s.title && /\benvio\b|\bsalida\b/i.test(s.title)) },
  { label: "⛪ General de Misa",
    check: (s) => (s.themes || []).includes("misa") },
];

const TEMPORADA_GROUPS = [
  { label: "Adviento 🕯️",        check: (s) => (s.themes || []).includes("adviento") },
  { label: "Navidad 🎄",          check: (s) => (s.themes || []).includes("navidad") },
  { label: "Cuaresma / Semana Santa ✝️", check: (s) => (s.themes || []).includes("cuaresma") },
  { label: "Pascua / Resurrección 🌅", check: (s) => (s.themes || []).includes("resurreccion") },
  { label: "Tiempo Ordinario ⭐", check: (s) => !(s.themes || []).some((t) => ["adviento","navidad","cuaresma","resurreccion"].includes(t)) },
];

// 8-color cycling palette — headers and number badges share the same gc class
const GC = ["gc-0","gc-1","gc-2","gc-3","gc-4","gc-5","gc-6","gc-7"];

const renderSongItem = (song, container, gc = "") => {
  const btn = document.createElement("button");
  btn.className = ["search-result-item", gc].filter(Boolean).join(" ");
  btn.type = "button";
  btn.dataset.page = String(song.page);
  const keyBadge = song.key
    ? `<span class="search-result-key">${song.key}</span>`
    : "";
  btn.innerHTML = `
    <div class="search-result-header">
      <span class="search-result-num">${song.song}</span>
      <span class="search-result-title">${song.title || `Canción ${song.song}`}</span>
      ${keyBadge}
    </div>`;
  container.appendChild(btn);
};

const renderGroupedSongs = (groups, allSongs, container) => {
  const assigned = new Set();
  let ci = 0;
  for (const group of groups) {
    const songs = allSongs.filter((s) => !assigned.has(s.song) && group.check(s));
    if (!songs.length) continue;
    songs.forEach((s) => assigned.add(s.song));
    const gc = GC[ci % GC.length]; ci++;
    const header = document.createElement("p");
    header.className = `browse-group-header ${gc}`;
    header.textContent = group.label;
    container.appendChild(header);
    songs.forEach((s) => renderSongItem(s, container, gc));
  }
};

const renderTabMisa = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  renderGroupedSongs(MISA_PARTS, state.songIndex, searchResults);
  if (!searchResults.children.length) {
    const p = document.createElement("p");
    p.className = "browse-empty";
    p.textContent = "No hay canciones con temas de misa asignados aún.";
    searchResults.appendChild(p);
  }
};

const renderTabRecientes = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  const nums = getRecientes();
  if (!nums.length) {
    const p = document.createElement("p");
    p.className = "browse-empty";
    p.textContent = "Aquí aparecerán las canciones que hayas visitado recientemente.";
    searchResults.appendChild(p);
    return;
  }
  for (const num of nums) {
    const song = state.songIndex.find((s) => s.song === num);
    if (song) renderSongItem(song, searchResults);
  }
};

const renderTabTemas = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  const themes = state.themeIndex
    .map((t) => ({ ...t, songs: state.songIndex.filter((s) => s.themes?.includes(t.id)) }))
    .filter((t) => t.songs.length > 0)
    .sort((a, b) => a.label.localeCompare(b.label, "es"));
  themes.forEach((theme, i) => {
    const gc = GC[i % GC.length];
    const hdr = document.createElement("p");
    hdr.className = `browse-group-header ${gc}`;
    hdr.textContent = `${theme.emoji} ${theme.label}  (${theme.songs.length})`;
    searchResults.appendChild(hdr);
    theme.songs
      .sort((a, b) => (a.title || "").localeCompare(b.title || "", "es"))
      .forEach((s) => renderSongItem(s, searchResults, gc));
  });
};

const renderTabTemporada = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  renderGroupedSongs(TEMPORADA_GROUPS, state.songIndex, searchResults);
};

const renderTabTodas = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  // Sort A-Z by title (Spanish locale), numbers at bottom
  const titled = [...state.songIndex].filter((s) => s.title).sort((a, b) => a.title.localeCompare(b.title, "es", { sensitivity: "base" }));
  const untitled = state.songIndex.filter((s) => !s.title).sort((a, b) => a.song - b.song);
  let currentLetter = null;
  for (const s of titled) {
    const letter = s.title[0].toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, ""); // strip accent for grouping
    if (letter !== currentLetter) {
      currentLetter = letter;
      const hdr = document.createElement("p");
      hdr.className = "browse-group-header browse-letter-header";
      hdr.textContent = letter;
      searchResults.appendChild(hdr);
    }
    renderSongItem(s, searchResults);
  }
  if (untitled.length) {
    const hdr = document.createElement("p");
    hdr.className = "browse-group-header gc-4";
    hdr.textContent = "Sin título";
    searchResults.appendChild(hdr);
    untitled.forEach((s) => renderSongItem(s, searchResults, "gc-4"));
  }
};

const SOLFEGE_MAP = { C:"Do", Db:"Re♭", D:"Re", Eb:"Mi♭", E:"Mi", F:"Fa", "F#":"Fa#", G:"Sol", Ab:"La♭", A:"La", Bb:"Si♭", B:"Si" };
const renderTabTono = () => {
  searchColHeader.classList.add("is-hidden");
  searchResults.innerHTML = "";
  const KEY_ORDER = ["C","Db","D","Eb","E","F","F#","G","Ab","A","Bb","B"];
  const byKey = {};
  for (const s of state.songIndex) {
    const k = s.key || "?";
    if (!byKey[k]) byKey[k] = [];
    byKey[k].push(s);
  }
  const groups = KEY_ORDER.filter((k) => byKey[k]);
  groups.forEach((k, i) => {
    const gc = GC[i % GC.length];
    const solfege = SOLFEGE_MAP[k] || k;
    const header = document.createElement("p");
    header.className = `browse-group-header ${gc}`;
    header.textContent = `${solfege}  (${k})  — ${byKey[k].length} canción${byKey[k].length !== 1 ? "es" : ""}`;
    searchResults.appendChild(header);
    byKey[k].forEach((s) => renderSongItem(s, searchResults, gc));
  });
  if (byKey["?"]) {
    const gc = GC[groups.length % GC.length];
    const header = document.createElement("p");
    header.className = `browse-group-header ${gc}`;
    header.textContent = `Tonalidad no especificada — ${byKey["?"].length} canciones`;
    searchResults.appendChild(header);
    byKey["?"].forEach((s) => renderSongItem(s, searchResults, gc));
  }
};

const renderActiveTab = () => {
  switch (state.activeTab) {
    case "buscar":    renderTabBuscar();    break;
    case "misa":      renderTabMisa();      break;
    case "recientes": renderTabRecientes(); break;
    case "temas":     renderTabTemas();     break;
    case "temporada": renderTabTemporada(); break;
    case "todas":     renderTabTodas();     break;
    case "tono":      renderTabTono();      break;
    default:          renderTabTodas();
  }
};

const renderTabBuscar = () => {
  searchRow.classList.remove("is-hidden");
  if (!searchInput.value.trim()) {
    searchResults.innerHTML = "";
    searchColHeader.classList.add("is-hidden");
    const p = document.createElement("p");
    p.className = "browse-empty";
    p.textContent = "Escribe para buscar por título, letra o tema…";
    searchResults.appendChild(p);
  }
  // If there's already a query, results are kept as-is from handleSearchInput
};

// ── Drawer mode switcher: "numpad" ↔ "browse" ────────────────────────────────
const switchDrawerMode = (mode) => {
  state.drawerMode = mode;
  const isBrowse = mode === "browse";
  navigationDrawer.classList.toggle("mode-browse", isBrowse);
  modeBtnNumpad.classList.toggle("is-active", !isBrowse);
  modeBtnBrowse.classList.toggle("is-active", isBrowse);
  modeBtnNumpad.setAttribute("aria-selected", String(!isBrowse));
  modeBtnBrowse.setAttribute("aria-selected", String(isBrowse));
  if (isBrowse) renderActiveTab();
};

// ── Tab activation helper ─────────────────────────────────────────────────────
const activateTab = (tabId) => {
  const isBuscar = tabId === "buscar";
  // Remember where we came from so the back button can return there
  if (isBuscar && state.activeTab !== "buscar") {
    state.prevTab = state.activeTab;
  }
  state.activeTab = tabId;

  // Lazy-load the search index the first time search opens — keeps ~45 KB out of
  // first paint, since web followers almost never search.
  if (isBuscar && !state.searchIndexPages.length) loadSearchIndex();

  // Update rail highlight
  drawerTabRail.querySelectorAll(".rail-tab").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.tab === tabId);
  });

  // Full-screen search: hide rail, expand pane; also hides numpad via search-focused
  navigationDrawer.classList.toggle("search-focused", isBuscar);
  navigationDrawer.classList.toggle("search-fullscreen", isBuscar);

  // Show/hide search row (contains back button + input)
  searchRow.classList.toggle("is-hidden", !isBuscar);

  if (!isBuscar) {
    searchInput.value = "";
    searchClearButton.classList.add("is-hidden");
    searchColHeader.classList.add("is-hidden");
  }
  renderActiveTab();
  if (isBuscar) {
    // Small delay to let layout settle before focusing (iOS keyboard timing)
    setTimeout(() => searchInput.focus(), 60);
  }
};

// ── Event binding ─────────────────────────────────────────────────────────────
const bindReaderEvents = () => {
  offlineContinueButton.addEventListener("click", () => {
    setOfflineGateState({ visible: false });
  });

  offlineRetryButton.addEventListener("click", () => {
    window.location.reload();
  });

  // Numpad digit press
  numberpadGrid.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-digit]");
    if (!button) return;
    haptic();
    appendDigit(button.dataset.digit);
  });

  // Backspace: tap deletes one digit; press-and-hold repeats (matches native).
  let backspaceRepeatTimer = null;
  let backspaceRepeatInterval = null;
  const stopBackspaceRepeat = () => {
    if (backspaceRepeatTimer) { clearTimeout(backspaceRepeatTimer); backspaceRepeatTimer = null; }
    if (backspaceRepeatInterval) { clearInterval(backspaceRepeatInterval); backspaceRepeatInterval = null; }
  };
  backspaceButton.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    haptic();
    backspaceDraft();
    stopBackspaceRepeat();
    backspaceRepeatTimer = setTimeout(() => {
      backspaceRepeatInterval = setInterval(() => backspaceDraft(), 100);
    }, 500);
  });
  ["pointerup", "pointerleave", "pointercancel"].forEach((evt) => {
    backspaceButton.addEventListener(evt, stopBackspaceRepeat);
  });

  goButton.addEventListener("click", () => {
    haptic(12);
    goToDraftSong();
  });

  // ── Jump-to-song modal: trigger / cancel / backdrop ──
  songJumpTrigger.addEventListener("click", () => { haptic(); openSongJump(); });
  // Tapping the song title is the discoverable, deliberate entry to jump-to-song / browse.
  songStatus.addEventListener("click", () => { haptic(); openSongJump(); });
  // ── The take-the-role gate (opened by ★ Ser Director, top-right) ──────────────
  const roleGate = document.getElementById("role-gate");
  const roleGateInput = document.getElementById("role-gate-input");
  const roleGateConfirm = document.getElementById("role-gate-confirm");
  const roleGateCancel = document.getElementById("role-gate-cancel");
  const roleGateBackdrop = document.getElementById("role-gate-backdrop");
  // A typed word, not a code. Everyone watches Braulio enter his real code anyway, so secrecy was
  // never the threat model — carelessness is. A word also cannot be muscle-memoried in a dialog
  // whose muscle memory is digits.
  const ROLE_GATE_WORD = "braulio";


  const closeRoleGate = () => {
    if (!roleGate) return;
    roleGate.classList.add("is-hidden");
    if (roleGateInput) roleGateInput.value = "";
    if (roleGateConfirm) roleGateConfirm.disabled = true;
  };
  if (roleGateInput && roleGateConfirm) {
    roleGateInput.addEventListener("input", () => {
      roleGateConfirm.disabled = roleGateInput.value.trim().toLowerCase() !== ROLE_GATE_WORD;
    });
  }
  if (roleGateCancel) roleGateCancel.addEventListener("click", () => { haptic(); closeRoleGate(); });
  if (roleGateBackdrop) roleGateBackdrop.addEventListener("click", closeRoleGate);
  if (roleGateConfirm) roleGateConfirm.addEventListener("click", () => {
    if (roleGateConfirm.disabled) return;
    haptic(18);
    closeRoleGate();
    closeSongJump();
    // `request-director` is the shell's existing entry point (PdfReaderApp.tsx:1141 -> DIRECTOR_CODE).
    // The web bundle never holds the code; it asks, and the shell decides.
    //
    // currentPage RIDES WITH THE REQUEST (2026-08-18). becomeDirector used to trust
    // currentPageRef.current — a native-side MIRROR updated asynchronously by an earlier bridge
    // message — for its very first broadcast. On real hardware that mirror was caught mid-lag: the
    // web renders a jump instantly (no native round trip needed to draw a page), so tapping Ser
    // Director right after a jump could broadcast whatever page was true a moment ago — the WebView
    // boot default, or the page before the jump — to the whole choir, correcting only once the mesh
    // heartbeat caught up ~10s later. The web always knows its OWN true page with zero lag; sending
    // it inline removes the race instead of chasing the mirror.
    postNativeBridge({ type: "request-director", currentPage: state.currentPage });
  });

  // (The keypad ✕ that used to live here was removed on 2026-08-18 along with #role-exit-x. The way
  // out of the role is the DIRECTOR status pill itself — see its handler below. This block outlived
  // its element and its comment claimed the ✕ was "the ONLY way out", which had stopped being true.)

  // Two things open this gate — the keypad's ★ Ser Director and the tip's "¿Eres Braulio?" — so it
  // is a function rather than the same six lines twice. Opening the gate is not taking the role:
  // the typed word and the red warning are still the only way through, and the ONE
  // request-director call site is still the gate's own Confirmar.
  const openRoleGate = () => {
    // Guarded even though both callers are hidden while directing: a second director is the failure
    // that breaks a Mass, and a guard is cheaper than the Sunday that finds the hole.
    if (state.nativeSyncRole === "director") return;
    if (!roleGate) return;
    roleGate.classList.remove("is-hidden");
    if (roleGateInput) { roleGateInput.value = ""; roleGateInput.focus(); }
    if (roleGateConfirm) roleGateConfirm.disabled = true;
  };

  // ★ Ser Director — the ONE way into the role, now visible in the top-right instead of buried in
  // a modal named IR A CANTO. It OPENS THE GATE; it does not take the role. request-director still
  // has exactly one call site (the gate's Confirmar), which e2e/directorButton.test.mjs pins.
  const becomePill = document.getElementById("become-director-pill");
  if (becomePill) becomePill.addEventListener("click", () => { haptic(); openRoleGate(); });
  requestRoleGate = openRoleGate;   // let a toast offer the same gate, without describing where it is

  const searchFab = document.getElementById("search-fab");
  if (searchFab) searchFab.addEventListener("click", () => { haptic(); navigationDrawer.classList.add("as-dropdown"); openDrawer(); activateTab("buscar"); });

  // "Dirigir el coro" — ask the native shell for the role. The web bundle never holds DIRECTOR_CODE;
  // it sends a request and the shell runs the same confirmation, live-director takeover warning and
  // ⟳ resync fab (top-left, follower) — rejoin the live director + refresh the connection.
  // Spin the icon for ~1.1s on tap so it's obvious it's reconnecting/refreshing.
  // The "nobody directing" tip — a bubble anchored under the ⟳, opened by tapping the crossed-out
  // button. Tapping anywhere, or OK, closes it. No auto-dismiss: it is three lines of explanation
  // and vanishing mid-sentence is worse than a stray tap.
  const noDirTip = document.getElementById("nodir-info");
  const closeNoDirTip = () => { if (noDirTip) noDirTip.classList.add("is-hidden"); };
  if (noDirTip) {
    const closeBtn = document.getElementById("nodir-info-close");
    const catcher = document.getElementById("nodir-info-backdrop");
    if (closeBtn) closeBtn.addEventListener("click", () => { haptic(); closeNoDirTip(); });
    if (catcher) catcher.addEventListener("click", closeNoDirTip);
    const braulio = document.getElementById("nodir-info-braulio");
    if (braulio) braulio.addEventListener("click", () => { haptic(); closeNoDirTip(); openRoleGate(); });
  }

  const resyncFab = document.getElementById("resync-fab");
  if (resyncFab) resyncFab.addEventListener("click", () => {
    // NOTHING TO RESYNC TO. With no director the reconnect is a no-op that looks like a fix, so the
    // button explains itself instead of spinning and changing nothing — which is exactly what the
    // crossing-out is telling you before you tap.
    if (document.documentElement.dataset.mesh === "nobody") {
      haptic();
      if (noDirTip) noDirTip.classList.remove("is-hidden");
      return;
    }
    closeNoDirTip();
    resyncFab.classList.add("is-spinning");
    window.setTimeout(() => resyncFab.classList.remove("is-spinning"), 1100);
    reconnectRelay();
  });
  // ⛶ fullscreen fab (bottom-right) — same toggle as the legacy hidden button
  if (fullscreenFab) fullscreenFab.addEventListener("click", () => { haptic(); toggleFullscreen().catch(() => {}); });

  // ONE control, three meanings — whatever the pill is showing is what tapping it acts on. The
  // affordance sits on the information, so nobody has to go looking in a modal named "IR A CANTO"
  // for the way to direct a choir.
  //
  // Taking the role goes through the native confirm (request-director → onDirectorCode), which is
  // where the "someone is already directing" warning lives. Stepping down keeps its own confirm so
  // a mid-Mass mistap cannot silently drop the director.
  // THE PILL NO LONGER TAKES THE ROLE (build 435). It is a readout: a bare dot while following,
  // amber "Nadie dirige" when nothing is driving, and nothing at all for a director (he is the
  // SOURCE, not a receiver). The role is taken through ★ Dirigir inside the IR A CANTO modal,
  // behind the typed-word gate.
  //
  // Leaving this handler in place would have been a hole straight THROUGH that gate: a second
  // ungated path to request-director, on the control most likely to be tapped by accident. Caught
  // by e2e/directorButton.test.mjs asserting exactly one request site — which is precisely the
  // drift it exists to prevent.
  if (directorModeBadge) directorModeBadge.addEventListener("click", () => {
    haptic();
    if (syncPillState() === "directing") {
      // THE STATUS IS ALSO THE WAY OUT. It reads DIRECTOR, and tapping it asks whether you meant
      // to stop — so the label never lies and the destructive step still needs an explicit yes.
      // A separate ✕ lived beside this briefly: two controls for one act, and the ✕ said nothing
      // about which role you were in.
      //
      // Plain confirm, not the red typed-word gate. Stepping DOWN is recoverable; only TAKING the
      // role from someone else changes everyone's page and earns the heavy friction.
      if (window.confirm("¿Salir de director?")) postNativeBridge({ type: "exit-director" });
      return;
    }
    openSongJump();   // point at the control instead of acting: discoverable, not destructive
  });

  songCancelButton.addEventListener("click", () => { haptic(); closeSongJump(); });
  songJumpBackdrop.addEventListener("click", () => { closeSongJump(); });

  // Drawer nav prev/next (stay in drawer so keepOverlay=true)
  prevPageButton.addEventListener("click", () => {
    haptic(12);
    turnSong(-1, { keepOverlay: true });
  });

  nextPageButton.addEventListener("click", () => {
    haptic(12);
    turnSong(1, { keepOverlay: true });
  });

  // History back
  prevCornerButton.addEventListener("click", () => {
    haptic();
    goBackInHistory();
  });

  // Drawer close button
  drawerCloseButton.addEventListener("click", () => {
    haptic();
    closeDrawer();
  });

  // Mode switch buttons: 🔢 Teclado ↔ 📚 Explorar
  modeBtnNumpad.addEventListener("click", () => {
    haptic();
    switchDrawerMode("numpad");
  });
  modeBtnBrowse.addEventListener("click", () => {
    haptic();
    switchDrawerMode("browse");
  });

  // Search back button → exit fullscreen search, return to previous tab (stay in browse mode)
  searchBackButton.addEventListener("click", () => {
    haptic();
    searchInput.blur();
    // ⌕-dropdown: "Volver" closes the dropdown. Full left drawer: return to the previous tab.
    if (navigationDrawer.classList.contains("as-dropdown")) closeDrawer();
    else activateTab(state.prevTab || "todas");
  });

  // Drawer handle tap → open
  drawerHandle.addEventListener("click", () => {
    haptic();
    openDrawer();
  });

  // Backdrop tap → close
  drawerBackdrop.addEventListener("click", () => {
    haptic();
    closeDrawer();
  });

  // Drawer back breadcrumb → return to index
  drawerBack.addEventListener("click", () => {
    haptic();
    renderIndexPanel();
  });

  // Search input
  searchInput.addEventListener("focus", () => {
    navigationDrawer.classList.add("search-focused");
  });

  searchInput.addEventListener("blur", () => {
    // Only collapse if no value and not in drill-down or index mode
    if (!searchInput.value.trim() && !state.indexVisible && !state.indexDrillDown) {
      navigationDrawer.classList.remove("search-focused");
      searchColHeader.classList.add("is-hidden");
    }
  });

  searchInput.addEventListener("input", handleSearchInput);

  searchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      searchInput.blur();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      clearSearch();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      const first = searchResults.querySelector(".search-result-item");
      if (first) first.focus();
    }
  });

  searchResults.addEventListener("keydown", (event) => {
    const items = [...searchResults.querySelectorAll(".search-result-item")];
    const idx = items.indexOf(document.activeElement);
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (idx < items.length - 1) items[idx + 1].focus();
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (idx > 0) items[idx - 1].focus();
      else searchInput.focus();
    } else if (event.key === "Escape") {
      clearSearch();
    }
  });

  // Search-result re-sort toggle (restores the native 277 best/#/A–Z cycle).
  const SEARCH_SORT_LABELS = { best: "⇅ Mejor", num: "⇅ Nº", az: "⇅ A–Z" };
  if (searchSortToggle) {
    searchSortToggle.addEventListener("click", () => {
      haptic();
      const order = ["best", "num", "az"];
      const next = order[(order.indexOf(state.searchSortMode || "best") + 1) % order.length];
      state.searchSortMode = next;
      searchSortToggle.textContent = SEARCH_SORT_LABELS[next];
      handleSearchInput(); // re-run the current query with the new sort
    });
  }

  searchClearButton.addEventListener("click", () => {
    haptic();
    if (state.indexDrillDown) {
      // Go back to index instead of fully clearing
      renderIndexPanel();
    } else {
      clearSearch();
    }
  });

  // Search cancel — hidden in new design (rail handles this), kept for compat
  searchCancelButton?.addEventListener("click", () => {
    haptic();
    clearSearch();
    searchInput.blur();
  });

  searchResults.addEventListener("click", (event) => {
    // Song result — navigate, keep drawer open
    const item = event.target.closest(".search-result-item[data-page]");
    if (item) {
      haptic();
      const pageNum = Number.parseInt(item.dataset.page, 10);
      if (Number.isFinite(pageNum)) {
        renderPage(pageNum, { userInitiated: true });                  // song index tap
        // Find song for this page and track it
        const songForPage = state.songIndex.find((s) => s.page === pageNum);
        if (songForPage) addToRecientes(songForPage.song);
        // Stay in browse mode (jump-to-song is now a separate modal)
        switchDrawerMode("browse");
      }
      return;
    }

    // Sort tab click
    const sortTab = event.target.closest(".index-sort-tab[data-sort-id]");
    if (sortTab) {
      haptic();
      const sortId = sortTab.dataset.sortId;
      const context = sortTab.dataset.context;
      if (context && sortId) {
        state.indexSortPrefs[context] = sortId;
        saveSortPrefs();
        const layout = searchResults.querySelector(".index-layout");
        if (layout) {
          const content = layout.querySelector(".index-content");
          if (content) renderIndexTabContent(content);
        }
      }
      return;
    }

    // Index: tab button (sidebar)
    const tabBtn = event.target.closest(".index-tab-btn[data-tab-id]");
    if (tabBtn) {
      haptic();
      const tabId = tabBtn.dataset.tabId;
      state.indexTab = tabId;
      const layout = searchResults.querySelector(".index-layout");
      if (layout) {
        layout.querySelectorAll(".index-tab-btn").forEach((btn) => {
          btn.classList.toggle("is-active", btn.dataset.tabId === tabId);
        });
        const content = layout.querySelector(".index-content");
        if (content) renderIndexTabContent(content);
      }
      return;
    }

    // Index: theme chip → drill in
    const themeChip = event.target.closest(".index-theme-chip[data-theme-label]");
    if (themeChip) {
      haptic();
      activateSearchFromIndex(themeChip.dataset.themeLabel);
      return;
    }

    // Index: keyword chip → drill in
    const kwChip = event.target.closest(".index-keyword-chip[data-keyword]");
    if (kwChip) {
      haptic();
      activateSearchFromIndex(kwChip.dataset.keyword);
      return;
    }
  });

  // Index toggle button (button removed from HTML; guard against null)
  searchIndexButton?.addEventListener("click", () => {
    haptic();
    if (state.indexVisible) {
      clearSearch();
    } else {
      renderIndexPanel();
    }
  });

  // Tab rail — persistent left-side navigation
  drawerTabRail.addEventListener("click", (event) => {
    const tab = event.target.closest(".rail-tab[data-tab]");
    if (!tab) return;
    haptic();
    activateTab(tab.dataset.tab);
  });

  // Help panel
  helpButton.addEventListener("click", () => {
    haptic();
    helpPanel.classList.remove("is-hidden");
  });

  helpCloseButton.addEventListener("click", () => {
    haptic();
    helpPanel.classList.add("is-hidden");
  });

  // Tip dismiss (× button on the banner itself)
  tipDismissButton.addEventListener("click", () => {
    haptic();
    numpadTipWrap.classList.add("is-hidden");
    try { localStorage.setItem(TIP_KEY, "dismissed"); } catch {}
  });

  // Haptic toggle
  const syncHapticToggle = () => {
    hapticToggleButton.setAttribute("aria-pressed", hapticEnabled ? "true" : "false");
  };
  syncHapticToggle();
  hapticToggleButton.addEventListener("click", () => {
    hapticEnabled = !hapticEnabled;
    try { localStorage.setItem(HAPTIC_PREF_KEY, hapticEnabled ? "on" : "off"); } catch {}
    syncHapticToggle();
    haptic(12); // Give immediate feedback when turning ON
  });

  // Fullscreen
  fullscreenButton.addEventListener("click", () => {
    haptic();
    toggleFullscreen().catch((error) => {
      console.error("No se pudo activar la pantalla completa", error);
    });
  });

  // ── Touch: page-swipe on viewer ────────────────────────────────────────────
  viewerShell.addEventListener("touchstart", (event) => {
    if (event.touches.length !== 1) {
      state.touchStart = null;
      return;
    }
    // When the viewport is zoomed in, let native pan/zoom handle the touch
    if ((window.visualViewport?.scale ?? 1) > 1.05) {
      state.touchStart = null;
      return;
    }
    const touch = event.touches[0];
    state.touchStart = { x: touch.clientX, y: touch.clientY, time: Date.now() };
  }, { passive: true });

  viewerShell.addEventListener("touchend", (event) => {
    if (!state.touchStart || event.changedTouches.length !== 1) return;
    const touch = event.changedTouches[0];
    const deltaX = touch.clientX - state.touchStart.x;
    const deltaY = touch.clientY - state.touchStart.y;
    const startX = state.touchStart.x;
    state.lastTouchEndedAt = Date.now();
    state.touchStart = null;

    // Left-edge swipe inward → open drawer
    // Must start within 44px of left edge AND move at least 40px right
    if (startX < 44 && deltaX > 40 && Math.abs(deltaX) > Math.abs(deltaY)) {
      event.preventDefault();
      haptic();
      openDrawer();
      return;
    }

    // Horizontal page swipe (not from edge zone)
    if (Math.abs(deltaX) > 48 && Math.abs(deltaX) > Math.abs(deltaY) && startX >= 44) {
      event.preventDefault();
      turnPage(deltaX < 0 ? 1 : -1);
      return;
    }

    // No single-tap toggle (Keynote-style: drawer only opens via swipe or handle)
  }, { passive: false });

  // ── Touch: swipe right on drawer → close ──────────────────────────────────
  navigationDrawer.addEventListener("touchstart", (event) => {
    if (event.touches.length !== 1) return;
    const t = event.touches[0];
    drawerSwipe = { x: t.clientX, y: t.clientY };
  }, { passive: true });

  navigationDrawer.addEventListener("touchmove", (event) => {
    if (!drawerSwipe || event.touches.length !== 1) return;
    const t = event.touches[0];
    const dx = t.clientX - drawerSwipe.x;
    const dy = t.clientY - drawerSwipe.y;
    // If gesture is clearly vertical, abandon swipe tracking so native scroll works
    if (Math.abs(dy) > Math.abs(dx) * 1.2) { drawerSwipe = null; return; }
    // Live drag: only if clearly horizontal left-ward (closing)
    if (dx < -12 && Math.abs(dx) > Math.abs(dy) * 1.5) {
      navigationDrawer.classList.add("is-dragging");
      const clampedDx = Math.max(dx, -navigationDrawer.offsetWidth);
      navigationDrawer.style.transform = `translateX(${clampedDx}px)`;
      const progress = 1 - Math.abs(clampedDx) / navigationDrawer.offsetWidth;
      drawerBackdrop.style.background = `rgba(0,0,0,${(0.38 * progress).toFixed(3)})`;
    }
  }, { passive: true });

  navigationDrawer.addEventListener("touchend", (event) => {
    if (!drawerSwipe || event.changedTouches.length !== 1) return;
    const t = event.changedTouches[0];
    const dx = t.clientX - drawerSwipe.x;
    const dy = t.clientY - drawerSwipe.y;
    drawerSwipe = null;

    // Reset any live-drag styles
    navigationDrawer.classList.remove("is-dragging");
    navigationDrawer.style.transform = "";
    drawerBackdrop.style.background = "";

    // If swiped left far enough, close
    if (dx < -60 && Math.abs(dx) > Math.abs(dy)) {
      haptic();
      closeDrawer();
    }
    // else snap back (CSS transition handles it automatically)
  }, { passive: true });

  // ── Touch: edge swipe (window level) to open drawer ───────────────────────
  // This handles the case where the touch starts at the right edge
  // but viewerShell may not cover the full width
  window.addEventListener("touchstart", (event) => {
    if (event.touches.length !== 1) return;
    const t = event.touches[0];
    if (t.clientX < 44) {
      edgeSwipe = { x: t.clientX, y: t.clientY };
    } else {
      edgeSwipe = null;
    }
  }, { passive: true });

  window.addEventListener("touchend", (event) => {
    if (!edgeSwipe || event.changedTouches.length !== 1) return;
    const t = event.changedTouches[0];
    const dx = t.clientX - edgeSwipe.x;
    const dy = t.clientY - edgeSwipe.y;
    edgeSwipe = null;

    if (dx > 40 && Math.abs(dx) > Math.abs(dy) && !state.drawerOpen) {
      haptic();
      openDrawer();
    }
  }, { passive: true });

  // ── Keyboard shortcuts ─────────────────────────────────────────────────────
  window.addEventListener("keydown", (event) => {
    // The diagnostics dump covers the screen but is opened AFTER closeSongJump(), so songJumpOpen is
    // false and arrow keys would fall straight through to turnSong — moving the whole congregation
    // under an opaque overlay, on the director's iPad, with nothing on screen to explain it.
    if (diagnosticsOpen()) {
      if (event.key === "Escape") hideDiagnostics();
      event.preventDefault();
      return;
    }
    // Number entry only applies while the jump-to-song modal is open (matches native).
    if (state.songJumpOpen) {
      if (/^[0-9]$/.test(event.key)) { appendDigit(event.key); return; }
      if (event.key === "Backspace") { backspaceDraft(); return; }
      if (event.key === "Enter") { goToDraftSong(); return; }
      if (event.key === "Escape") { closeSongJump(); return; }
      return;
    }
    if (event.key === "Escape" && state.drawerOpen) { closeDrawer(); return; }
    if (event.key === "ArrowRight") turnSong(1);
    if (event.key === "ArrowLeft") turnSong(-1);
  });

  ["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
    document.addEventListener(eventName, updateFullscreenButton);
  });
};

// ── Init ──────────────────────────────────────────────────────────────────────
// ── Live follow via the relay (signovivo.com → director) ───────────────────────
// Standalone web only. Inside the native app, Multipeer already syncs; the offline
// file bundle has no network. Mirrors the proven test-rig logic: WebSocket push
// (seq-guarded) with reconnect + poll fallback, applied through renderPage().
const RELAY_BASE_RAW = "https://signovivo-sync.4j4982y8jp.workers.dev";
const RELAY_BASE = RELAY_BASE_RAW.startsWith("__RELAY")
  ? "https://signovivo-sync.4j4982y8jp.workers.dev"
  : RELAY_BASE_RAW.replace(/\/+$/, "");
// Relay room: production "alvernia-main" by default; the isolated canary channel
// "alvernia-staging" only for ?env=staging (Release Safety System, M1). Resolved by
// the pre-loaded svRelayRoom helper. Triple-guarded — lib present? known room? any
// throw? — so a resolver problem can never white-screen boot; it defaults to the
// live room, and only the exact "alvernia-staging" value is ever allowed through.
const RELAY_ROOM = (function resolveRelayRoomSafely() {
  try {
    const resolve =
      typeof globalThis !== "undefined" &&
      globalThis.svRelayRoom &&
      globalThis.svRelayRoom.resolveRelayRoom;
    const room = resolve ? resolve(location.search) : "alvernia-main";
    return room === "alvernia-staging" ? "alvernia-staging" : "alvernia-main";
  } catch (_) {
    return "alvernia-main";
  }
})();
const RELAY_LIVE_MAX_AGE_S = 90; // a director counts as "live" if its last update is this recent

// (The fleet readiness check-in was removed entirely on 2026-08-18 — dashboard, routes, roster and
//  all. It posted every 90s from every device so a pre-Mass page could show green lights, and held
//  choir phone numbers. isOfflineBundleReady below was its only consumer and is now unused; it is
//  kept because it encodes how to verify a cached bundle against the real caches, which is worth
//  more than the lines it costs if readiness reporting ever returns.)

// Report presence + cache state on load so the director's gated /fleet-dashboard shows which web
// devices are cached and ready. Anonymous by device — the old one-time "¿Quién usa este iPad?"
// self-ID prompt (name + role) was removed; it was more annoying than useful.
const scheduleFleetCheckin = () => {
  if (NATIVE_FILE_MODE) return;
};

const relay = {
  backoff: 500,
  manualClose: false,
  pollTimer: 0,
  ws: null,            // the live WebSocket — stored so connectRelay can guard against dupes
  heartbeatTimer: 0,   // 4s heartbeat interval id — cleared so it can't accumulate per socket
  reconnectTimer: 0,   // pending backoff reconnect timeout id — cleared so only ONE is scheduled
  lastMsgAt: 0,        // ts of the last WS message — read on foreground to detect a dead socket
  lastSeq: -1,
  browsing: false,   // user opted into manual browse (tap title → jump-to-song): pause auto-follow
  following: true,   // apply pushes until the user browses away
  appliedPage: null, // last page WE applied from the relay
  livePage: null,    // latest page the director is on (tracked even while browsing)
  hasDirector: false,
  clockOffsetMs: 0,  // (serverClock - deviceClock), calibrated from /state Date header (P2-CLOCKSKEW)
  healthTimer: 0,    // F3: independent time-driven relay-health watchdog (started once)
  cooldownUntil: 0,  // circuit breaker: no relay traffic at all until this timestamp (see tripRelayCooldown)
};

let relayPill = null;
const ensureRelayPill = () => {
  if (relayPill) return relayPill;
  const style = document.createElement("style");
  style.textContent =
    "#sv-live-pill{position:fixed;top:max(0.6rem,env(safe-area-inset-top,0px));" +
    "right:max(0.7rem,env(safe-area-inset-right,0px));z-index:46;border:0;padding:0;" +
    "width:8px;height:8px;border-radius:50%;display:none;cursor:pointer;" +
    "box-shadow:0 0 0 2px rgba(255,255,255,0.55),0 1px 3px rgba(0,0,0,.35);" +
    "user-select:none;-webkit-tap-highlight-color:transparent}" +
    "#sv-live-pill.is-live{background:#22c55e;animation:sv-pulse 1.7s ease-in-out infinite}" +
    "#sv-live-pill.is-resync{background:#f0c040}" +
    "@keyframes sv-pulse{0%,100%{opacity:1}50%{opacity:.35}}";
  document.head.appendChild(style);
  relayPill = document.createElement("button");
  relayPill.id = "sv-live-pill";
  relayPill.type = "button";
  relayPill.addEventListener("click", () => {
    if (!relayPill.classList.contains("is-resync") || relay.livePage == null) return;
    goLive();   // amber dot is also a "go live" affordance (same as the bar)
  });
  document.body.appendChild(relayPill);
  return relayPill;
};

const renderRelayPill = () => {
  const pill = ensureRelayPill();
  if (!relay.hasDirector) { pill.style.display = "none"; return; }
  pill.style.display = "block";
  pill.className = relay.following ? "is-live" : "is-resync";
  pill.setAttribute("aria-label", relay.following ? "En vivo con el director" : "Volver a en vivo");
};

// ── "Volver a en vivo" bar — shown while the user is browsing the songbook off-live ─────
let goLiveBar = null;
const ensureGoLiveBar = () => {
  if (goLiveBar) return goLiveBar;
  const style = document.createElement("style");
  style.textContent =
    "#sv-golive-bar{position:fixed;left:50%;transform:translateX(-50%);" +
    "bottom:max(1.1rem,env(safe-area-inset-bottom,0px));z-index:48;display:none;" +
    "align-items:center;gap:0.4rem;padding:0.7rem 1.3rem;border:0;border-radius:999px;" +
    "background:#22c55e;color:#fff;font:600 0.98rem/1 system-ui,-apple-system,sans-serif;" +
    "cursor:pointer;box-shadow:0 6px 20px rgba(0,0,0,.4);" +
    "-webkit-tap-highlight-color:transparent}" +
    "#sv-golive-bar.is-visible{display:flex;animation:sv-golive-in .2s ease}" +
    "@keyframes sv-golive-in{from{opacity:0;transform:translate(-50%,10px)}to{opacity:1;transform:translate(-50%,0)}}" +
    "#song-status{cursor:pointer}";
  document.head.appendChild(style);
  goLiveBar = document.createElement("button");
  goLiveBar.id = "sv-golive-bar";
  goLiveBar.type = "button";
  goLiveBar.textContent = "↩  Volver a en vivo";
  goLiveBar.addEventListener("click", () => goLive());
  document.body.appendChild(goLiveBar);
  return goLiveBar;
};
const showGoLiveBar = () => { if (relay.hasDirector) ensureGoLiveBar().classList.add("is-visible"); };
const hideGoLiveBar = () => { if (goLiveBar) goLiveBar.classList.remove("is-visible"); };

// Leave browse mode and snap back to the director's current page.
const goLive = () => {
  relay.browsing = false;
  relay.following = true;
  hideGoLiveBar();
  closeSongJump();
  if (relay.livePage != null) {
    relay.appliedPage = relay.livePage;
    if (state.currentPage !== relay.livePage) renderPage(relay.livePage, { pushToHistory: false });
  }
  renderRelayPill();
  haptic(12);
};

// ── Reconnect (parity control) — re-establish the live follow + resync to the director ─
// connectRelay / relayPollOnce are defined below; this only runs on a user tap, well after
// init, so the forward reference resolves fine.
const reconnectRelay = () => {
  haptic(12);
  // A HUMAN ASKING TO RETRY OUTRANKS THE COOLDOWN. RENDER_RETRY_COOLDOWN_MS paces a page that keeps
  // failing on its own; ⟳ is somebody standing there deciding to try again, usually right after
  // doing something that changed the answer (moving nearer the router, rejoining Wi-Fi). Making
  // them wait out a pacing window for a machine would make the one control they have feel broken.
  state.lastRenderFailure = null;
  // NATIVE: the web relay is intentionally OFF in the iPad shell (it syncs over the Multipeer
  // mesh via the bridge). So a ⟳ tap must NOT open a web-relay socket — ask the shell to
  // re-request the director's snapshot over the mesh instead.
  if (hasNativeBridge() || NATIVE_FILE_MODE) {
    postNativeBridge({ type: "resync" });
    return;
  }
  // WEB: reconnect the relay socket + force a resync to the director.
  relay.browsing = false;
  relay.following = true;
  relay.backoff = 500;
  relay.lastSeq = -1;   // accept the director's CURRENT seq even if it restarted (seq reset low)
  if (relay.ws) { try { relay.ws.close(); } catch {} relay.ws = null; }
  connectRelay();        // fresh WS; its open handler force-resyncs via relayPollOnce
  relayPollOnce(true);   // also resync now (covers the polling-only fallback path)
  hideGoLiveBar();
  renderRelayPill();
};

// The follower-sync decision (freshness-before-seq, clock-skew correction) lives in the
// unit-tested svSyncDecision lib. applyRelaySnapshot is a thin executor of its verdict:
// no ordering logic here, so the load-bearing "does a dead director stay live?" question
// is decided by node-tested code, identical in the browser and CI.
const applyRelaySnapshot = async (snap, { force = false } = {}) => {
  const lib = globalThis.svSyncDecision;
  // If the lib somehow failed to load, fall back to a conservative inline check so a
  // relay message can never throw at boot: reject bad pages, honor the seq de-dup, and
  // treat a snapshot as live only if fresh. (The lib is smoke-checked into every build.)
  if (!lib || typeof lib.decideRelaySnapshot !== "function") {
    if (!snap || !Number.isFinite(snap.page)) return;
    const hasPub = Number.isFinite(snap.seq) && snap.seq > 0;
    // FAIL CLOSED ON AN UNDATEABLE SNAPSHOT (2026-08-18).
    //
    // This read `!Number.isFinite(snap.ts) || <within window>` — so a snapshot with NO timestamp
    // was treated as FRESH and applied unconditionally. A snapshot that cannot prove its age is
    // exactly the one that must not be trusted: it is indistinguishable from something written
    // hours ago, and applying it puts a wrong song in front of the whole choir with no way for them
    // to know why. Every device jumping to a stale song 2 before the mesh corrected them to 372 on
    // 2026-08-18 is what this looks like from the loft.
    //
    // A missing ts now means NOT fresh. The cost of being wrong in this direction is a follower
    // that waits for the next update; the cost of being wrong in the other direction is confusion
    // during Mass, and confusion is the thing worth spending latency to avoid.
    const dateable = Number.isFinite(snap.ts);
    const ageS = dateable ? (Date.now() + (relay.clockOffsetMs || 0)) / 1000 - snap.ts : Infinity;
    const fresh = hasPub && dateable && ageS <= RELAY_LIVE_MAX_AGE_S;
    if (hasPub && !dateable) { try { console.warn("[sv] relay snapshot has no ts — refusing to apply"); } catch {} }
    if (!fresh) { relay.hasDirector = false; relay.browsing = false; relay.lastSeq = -1; hideGoLiveBar(); renderRelayPill(); return; }
    if (!force && snap.seq <= relay.lastSeq) { relay.hasDirector = true; renderRelayPill(); return; }
    relay.lastSeq = Math.max(relay.lastSeq, snap.seq);
    relay.hasDirector = true; relay.livePage = snap.page;
    if (relay.browsing) { renderRelayPill(); revealReader(); return; }
    try {
      console.info("[sv] page", snap.page, "via relay (age", Math.round(ageS) + "s, seq", snap.seq + ")");
    } catch {}
    relay.following = true; relay.appliedPage = snap.page;
    if (state.currentPage !== snap.page) renderPage(snap.page, { pushToHistory: false });
    renderRelayPill(); revealReader();
    return;
  }

  const d = lib.decideRelaySnapshot(snap, {
    lastSeq: relay.lastSeq,
    hasDirector: relay.hasDirector,
    browsing: relay.browsing,
    currentPage: state.currentPage,
    force,
    nowMs: Date.now(),
    clockOffsetMs: relay.clockOffsetMs || 0,
    maxAgeS: RELAY_LIVE_MAX_AGE_S,
  });
  if (d.action === "reject") return;

  // Apply the decided sync state. Single-book app: snap.bookId (kept on the wire for
  // build-373 compat) is always "standard", so the director's page renders straight
  // against the one book; renderPage clamps any out-of-range page.
  relay.hasDirector = d.hasDirector;
  relay.lastSeq = d.lastSeq;
  relay.browsing = d.browsing;
  if (d.livePage != null) relay.livePage = d.livePage;
  if (d.hideGoLiveBar) hideGoLiveBar();

  if (d.action === "follow") {
    // A congregation follower ALWAYS tracks the director on a new position, EVEN IF a
    // stray Safari swipe had nudged the page off (the old behavior stranded followers on
    // the amber dot — director on 367, follower frozen on 365). Peeking between moves is
    // fine; the next move pulls you home, or tap the dot to resync now.
    relay.following = true;
    relay.appliedPage = snap.page;
    if (d.renderPage != null) renderPage(d.renderPage, { pushToHistory: false });
  }
  if (d.renderPill) renderRelayPill();
  if (d.reveal) revealReader();  // idempotent — safe to call anytime
};

const relayStateUrl = () => RELAY_BASE + "/r/" + encodeURIComponent(RELAY_ROOM) + "/state";
const relayWsUrl = () => RELAY_BASE.replace(/^http/, "ws") + "/r/" + encodeURIComponent(RELAY_ROOM) + "/subscribe";

const stopRelayPolling = () => { if (relay.pollTimer) { clearInterval(relay.pollTimer); relay.pollTimer = 0; } };

// ── CIRCUIT BREAKER ──────────────────────────────────────────────────────────────────────────
//
// THE OUTAGE THIS EXISTS FOR (2026-08-18, and 2026-08-17 before it). Cloudflare's free plan allows
// 100,000 Worker requests a day. When the account crossed it, every request returned 429 — and this
// client answered by trying HARDER: the socket died, backoff climbed to its 8s cap and ALSO started
// a 4s /state poll, and then the 10s health timer reset backoff to 500ms and restarted both. About
// 20-25 requests a minute per device, ~30k/day each, four devices — over the daily cap on their own,
// forever. The outage fed itself and could not recover even after the traffic that started it
// stopped.
//
// A 429 is the server saying stop. Retrying through it is not resilience, it is a denial of service
// against your own account, and it takes signovivo.com down with the relay because both ride the
// same quota. So: when the relay says stop, STOP — for minutes, not milliseconds.
//
// Deliberately blunt. The relay is a convenience path; the mesh and BLE carry Mass, where there is
// no internet at all. Losing relay sync for ten minutes costs a web viewer some latency. Not losing
// it costs everyone the whole day.
const RELAY_COOLDOWN_MS = 10 * 60 * 1000;
const relayCooling = () => Date.now() < (relay.cooldownUntil || 0);
const tripRelayCooldown = (reason) => {
  if (relayCooling()) return;
  relay.cooldownUntil = Date.now() + RELAY_COOLDOWN_MS;
  stopRelayPolling();
  if (relay.reconnectTimer) { clearTimeout(relay.reconnectTimer); relay.reconnectTimer = 0; }
  try { relay.ws && relay.ws.close(); } catch {}
  relay.ws = null;
  try { console.warn("[sv] relay cooldown", reason, Math.round(RELAY_COOLDOWN_MS / 1000) + "s"); } catch {}
};
// Poll the relay for the director's current snapshot and apply it. Single-book + fully public:
// there is no geo header to read and no book to resolve — just fetch /state and apply the page.
const relayPollOnce = async (force = false) => {
  if (relayCooling()) return;
  try {
    // Time-box the /state fetch with an AbortController so a HUNG connection fails FAST.
    // navigator.onLine is TRUE on wifi-without-internet, so the fetch can hang indefinitely.
    const ac = typeof AbortController === "function" ? new AbortController() : null;
    const abortTimer = ac ? setTimeout(() => { try { ac.abort(); } catch {} }, 6000) : 0;
    let r;
    try {
      r = await fetch(relayStateUrl(), { cache: "no-store", signal: ac ? ac.signal : undefined });
    } finally {
      if (abortTimer) clearTimeout(abortTimer);
    }
    // 429 / 1027 is the account over its daily quota. Nothing this client does can fix that, and
    // every further request digs the hole deeper — including the ones that would tell us it is
    // fixed. Back all the way off and let the reset happen.
    if (r.status === 429) { tripRelayCooldown("429 daily limit"); return; }
    if (r.ok) {
      const recvMs = Date.now();
      const dateHeader = r.headers.get("date");
      const snap = await r.json();
      // P2-CLOCKSKEW: calibrate the client<->server clock offset BEFORE judging snapshot
      // freshness. A follower whose device clock is fast by >90s would otherwise treat
      // EVERY fresh snapshot as stale and never follow any director. Prefer the body `now`
      // field (server epoch seconds) — it's the reliable cross-origin channel; the HTTP
      // Date header is NOT CORS-safelisted, so it's a same-origin/dev fallback only.
      // Fail-safe: if neither is usable the previous offset is kept (0 by default →
      // identical to the pre-P2 behavior, so an old worker without `now` never regresses).
      const lib = globalThis.svSyncDecision;
      if (lib) {
        try {
          if (snap && Number.isFinite(snap.now) && typeof lib.clockOffsetFromServerNow === "function") {
            relay.clockOffsetMs = lib.clockOffsetFromServerNow(snap.now, recvMs, relay.clockOffsetMs || 0);
          } else if (typeof lib.clockOffsetFromDateHeader === "function") {
            relay.clockOffsetMs = lib.clockOffsetFromDateHeader(dateHeader, recvMs, relay.clockOffsetMs || 0);
          }
        } catch {}
      }
      await applyRelaySnapshot(snap, { force });
    }
  } catch {}
};
// Fallback polling (when the WS won't hold): force every tick so a stationary director
// still keeps the follower in sync.
// 4s -> 15s. A fallback that may run all day cannot poll at debug speed: 4s is 21,600 requests per
// device per day, and four devices exceed the entire account quota by themselves. 15s keeps a
// stationary director's followers in sync within one page turn's patience and costs a quarter as much.
const RELAY_POLL_MS = 15000;
const startRelayPolling = () => {
  if (relayCooling()) return;
  stopRelayPolling();
  relay.pollTimer = setInterval(() => relayPollOnce(true), RELAY_POLL_MS);
  relayPollOnce(true);
};

const connectRelay = () => {
  // F5: cancel any pending backoff reconnect on EVERY entry — BEFORE the dupe-guard return —
  // so an `online`/foreground/manual connect that finds a healthy socket doesn't leave a stale
  // reconnect scheduled to fire a redundant connect later. (Previously this ran only past the
  // guard, so the early-return path leaked the timer.) Keeps the "only ONE reconnect scheduled"
  // invariant on all paths.
  if (relay.reconnectTimer) { clearTimeout(relay.reconnectTimer); relay.reconnectTimer = 0; }
  // The account is over its daily quota; reconnecting is what put it there. Every path in — online,
  // foreground, manual, backoff — funnels through here, so one guard covers them all.
  if (relayCooling()) return;
  // Idempotent: if a socket is already CONNECTING (0) or OPEN (1), don't open a duplicate.
  // iOS fires `online` on network changes even while a socket is healthy, and the close-
  // handler's backoff reconnect can race with it — without this guard each call would stack
  // another live socket, each with its own 4s heartbeat + message handler.
  if (relay.ws && (relay.ws.readyState === 0 || relay.ws.readyState === 1)) return;
  relay.manualClose = false;
  // P2-POLL-GAP: keep a /state safety poll running THROUGH the connecting window instead
  // of going blind until the socket opens (or the 6s zombie-timeout fires). On a flaky
  // reconnect mid-Mass the socket can take seconds to open; without this the follower had
  // no sync source in that gap and could miss a director move. The open handler stops this
  // poll the instant the WS is confirmed live, so a healthy network pays only ~1 extra
  // /state fetch during the sub-second connect. startRelayPolling() self-clears first, so
  // it never stacks a second interval.
  startRelayPolling();
  let ws;
  try { ws = new WebSocket(relayWsUrl()); } catch { startRelayPolling(); return; }
  relay.ws = ws;
  relay.lastMsgAt = Date.now();
  // Zombie-CONNECTING guard: a flaky network can leave a socket stuck in readyState 0
  // (CONNECTING) with NO open AND NO close event — it just hangs. The dupe-guard above then
  // treats it as "already connecting" and blocks every future automatic reconnect (online /
  // foreground), so the follower silently stalls. Arm a 6s timeout: if the socket is still
  // CONNECTING when it fires, force-close it (that fires `close` → backoff reconnect) and fall
  // back to /state polling immediately so the follower keeps syncing. Cleared on BOTH open and
  // close below so it can't leak or fire late against a socket that already settled.
  let connectTimer = setTimeout(() => {
    connectTimer = 0;
    if (ws.readyState === 0) { try { ws.close(); } catch {} startRelayPolling(); }
  }, 6000);
  const clearConnectTimer = () => { if (connectTimer) { clearTimeout(connectTimer); connectTimer = 0; } };
  ws.addEventListener("open", () => {
    clearConnectTimer();
    // P2-POLL-GAP: the WS is live now — retire the connecting-window bridge poll so we
    // don't run BOTH a socket and a 4s /state poll. The heartbeat below + WS pushes take
    // over; if the socket later dies, connectRelay restarts the bridge poll again.
    stopRelayPolling();
    relay.backoff = 500;
    relay.lastMsgAt = Date.now();
    relayPollOnce(true);   // force-resync to the director's current page on (re)connect
    // Heartbeat: ping over the EXISTING socket every 4s. If it goes silent for 12s the
    // socket is a "zombie" (flaky cell can drop it dead with NO close event) — close it so
    // the reconnect + resync fires. Reuses the relay's ping->snapshot handler, so it's tiny
    // WS frames, not HTTP /state polls — cheap on weak cell, and each ping reply doubles as
    // a resync that catches any missed push within ~4s. Tracked on `relay` and cleared first
    // so a stale heartbeat from a prior socket can't accumulate.
    if (relay.heartbeatTimer) clearInterval(relay.heartbeatTimer);
    // Capture THIS socket's heartbeat id locally so neither the self-clear below nor the
    // close handler can reap a DIFFERENT (newer) socket's heartbeat: on a flaky reconnect the
    // new socket's open can fire BEFORE the old socket's close, so relay.heartbeatTimer may
    // already point at the new timer by the time the old close (or a stale self-clear tick)
    // runs. Always clearInterval(myHeartbeat); only null relay.heartbeatTimer when it's still us.
    let myHeartbeat = 0;
    myHeartbeat = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        clearInterval(myHeartbeat);
        if (relay.heartbeatTimer === myHeartbeat) relay.heartbeatTimer = 0;
        return;
      }
      // F2: silent past the window → zombie socket; close it. ALSO start /state polling as a
      // fallback — on iOS a dead socket's `close` event may NEVER fire, so relying on
      // close→reconnect alone can strand the follower. Polling keeps sync alive until a fresh
      // socket (re)connects; the open handler stops polling again once live.
      if (Date.now() - relay.lastMsgAt > 12000) { try { ws.close(); } catch {} startRelayPolling(); return; }
      try { ws.send("ping"); } catch {}
      // F1: a stray swipe / next-prev / arrow moved us off the director's page while still
      // "following" (ONLY the numpad jump sets relay.browsing). A STATIONARY director's ping-reply
      // is a duplicate seq → not re-rendered → the follower silently strands on the wrong page with
      // a green "en vivo" pill and no cue, until the director's NEXT move. Force a re-home poll so a
      // drifted follower snaps back within a heartbeat. No-op for an on-page follower (the common case).
      if (!relay.browsing && relay.hasDirector && relay.livePage != null && state.currentPage !== relay.livePage) {
        relayPollOnce(true);
      }
    }, 4000);
    relay.heartbeatTimer = myHeartbeat;
    ws.__svHeartbeat = myHeartbeat;
  });
  ws.addEventListener("message", (ev) => { relay.lastMsgAt = Date.now(); try { applyRelaySnapshot(JSON.parse(ev.data)).catch(() => {}); } catch {} });
  ws.addEventListener("error", () => {});
  ws.addEventListener("close", () => {
    clearConnectTimer();
    // Stop ONLY this socket's own heartbeat (ws.__svHeartbeat), never relay.heartbeatTimer
    // blindly: if a newer socket already opened and installed its heartbeat, an unconditional
    // clear here would kill the LIVE socket's heartbeat → zombie-socket detection silently lost.
    // Only null relay.heartbeatTimer when it still points at the timer that just died.
    if (ws.__svHeartbeat) {
      clearInterval(ws.__svHeartbeat);
      if (relay.heartbeatTimer === ws.__svHeartbeat) relay.heartbeatTimer = 0;
      ws.__svHeartbeat = 0;
    }
    // Only forget the socket if it's still the one that just closed — a newer connectRelay
    // may have already replaced relay.ws, and we must not clobber the live socket.
    if (relay.ws === ws) relay.ws = null;
    if (relay.manualClose) return;
    // Add ±30% JITTER so many followers don't reconnect in lockstep after a shared
    // network blip (thundering-herd on the worker). The first retry stays fast (the
    // 500ms floor), and the (re)open handler force-polls to resync. Backoff itself
    // remains the clean exponential base so the 8000ms cap + /state fallback are intact.
    // Clear any pending reconnect first so only ONE is ever scheduled.
    const delay = relay.backoff * (0.7 + Math.random() * 0.6);
    if (relay.reconnectTimer) clearTimeout(relay.reconnectTimer);
    relay.reconnectTimer = setTimeout(() => { relay.reconnectTimer = 0; connectRelay(); }, delay);
    relay.backoff = Math.min(relay.backoff * 2, 8000);
    if (relay.backoff >= 8000) startRelayPolling();   // WS truly won't hold -> /state fallback
  });
};

// OPT-IN, AND ONLY IN THE NATIVE SHELL. signovivo.com is UNTOUCHED by this gate: the relay is the
// only sync a web follower has, so switching it off there would delete the product. The check is
// deliberately written as "native AND not opted in" so the web path cannot be caught by it even by
// accident, and a test asserts that.
//
// What this DOES turn off is a native follower SUBSCRIBING to the relay, which is pure redundancy:
// that device already has the mesh and BLE, both of which work at Mass where there is no internet at
// all. It is not free redundancy either — it is what made every device jump to a stale song 2 on
// 2026-08-18 before the mesh corrected them, and its 4s /state poll is what exhausted the account's
// 100k daily Worker quota and took signovivo.com down WITH the relay.
//
// The director's relay PUBLISH is not gated. That is what feeds signovivo.com, it fires once per
// page turn, and it is the whole reason web followers exist.
const relaySubscribeEnabled = () => {
  const native = NATIVE_FILE_MODE || hasNativeBridge();
  if (!native) return true;   // signovivo.com — always on, never gated
  try {
    if (/[?&]relay=1/.test(location.search)) return true;
    return localStorage.getItem("sv.relaySubscribe") === "1";
  } catch { return false; }
};

const startRelayFollow = () => {
  if (!relaySubscribeEnabled()) {
    try { console.info("[sv] relay subscribe off (native default) — mesh + BLE carry sync"); } catch {}
    return;
  }
  if (hasNativeBridge() || NATIVE_FILE_MODE) return;  // native app / offline bundle: skip
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    relayPollOnce(true);   // resync on foreground
    // iOS can silently kill a backgrounded socket with NO close event — readyState stays
    // OPEN but the socket is dead, so the heartbeat never fires and we'd sit there frozen.
    // On foreground, if the socket is gone, not OPEN, or has been silent past the 12s
    // heartbeat window, force a fresh reconnect (fast backoff) to recover the live feed.
    if (!relay.ws || relay.ws.readyState !== WebSocket.OPEN || (Date.now() - relay.lastMsgAt > 12000)) {
      try { relay.ws && relay.ws.close(); } catch {}
      relay.ws = null;
      relay.backoff = 500;
      connectRelay();
    }
  });
  window.addEventListener("online", () => {
    relay.backoff = 500;
    // Symmetric with the visibilitychange recheck above: iOS can keep a DEAD socket at
    // readyState OPEN with NO close event after a network drop. connectRelay's dupe-guard
    // would then see the stale OPEN socket and return immediately — no new socket, no resync —
    // leaving the follower frozen until the 12s heartbeat-silence check finally trips. Tear a
    // stale/silent socket down FIRST so connectRelay opens a fresh one and force-resyncs.
    if (!relay.ws || relay.ws.readyState !== WebSocket.OPEN || (Date.now() - relay.lastMsgAt > 12000)) {
      try { relay.ws && relay.ws.close(); } catch {}
      relay.ws = null;
    }
    connectRelay();
  });
  // F3: an independent, TIME-DRIVEN health floor. Every other recovery is EVENT-driven (heartbeat,
  // `online`, `visibilitychange`) and iOS guarantees NONE of them fire — a socket can die silently on
  // a propped-up, foregrounded iPad whose wifi shows "connected" but is dropping packets. This
  // guaranteed 10s tick is the backstop: if the WS is gone / closing / closed, or OPEN-but-silent
  // past the window, tear it down and reconnect while keeping /state polling alive. Idempotent
  // (no-op when healthy); leaves a CONNECTING socket alone (the 6s connectTimer owns that case).
  if (!relay.healthTimer) {
    relay.healthTimer = setInterval(() => {
      if (relay.manualClose || relayCooling()) return;
      const s = relay.ws;
      const dead = !s || s.readyState >= 2 || (s.readyState === WebSocket.OPEN && Date.now() - relay.lastMsgAt > 15000);
      if (!dead) return;
      try { s && s.close(); } catch {}
      relay.ws = null;
      // DO NOT RESET THE BACKOFF HERE. This ran every 10s and slammed backoff back to 500ms, so the
      // exponential climb never survived one cycle — a socket that had earned an 8s wait was handed
      // a half-second one instead, forever, on exactly the paths where the far end was already
      // overloaded. Backoff resets on a SUCCESSFUL open, the only event that proves it is healthy.
      startRelayPolling();   // sync stays alive while a fresh socket re-establishes
      connectRelay();
    }, 10000);
  }
  relayPollOnce(true);   // snap to the director's current page (backup to initReader's awaited poll)
  if ("WebSocket" in window) connectRelay(); else startRelayPolling();
};

const initReader = async () => {
  // Single-book app. The inlined #pages-data carries { totalPages } for the standard book — just
  // enough to paint the first page instantly. The song index (~50 KB: titles, themes, intro
  // chords, jump-to-song) hydrates separately so it never blocks first paint. If #pages-data is
  // present we paint inline now and hydrate the song index in the background; otherwise we fetch
  // the manifest before first paint so totalPages/pages match.
  const inlinedPages = document.getElementById("pages-data");
  let manifest;
  if (inlinedPages) {
    // Inlined by build.mjs — parse defensively; a malformed blob must not abort boot.
    try { manifest = JSON.parse(inlinedPages.textContent); } catch (_) { manifest = {}; }
  } else {
    // No inlined page count (unusual — build.mjs inlines it). Fetch it, but on a non-ok
    // response / network error / bad JSON, degrade to the known standard count and keep
    // booting rather than aborting the whole reader (the totalPages fallback below
    // turns {} into STANDARD_TOTAL_PAGES, and the song index hydrates in the background).
    try {
      // Default cache mode (READER, not writer): must be servable from the SW cache offline.
      const response = await fetch(resolveAppPath(`/books/${BOOK_ID}/pages.json`));
      manifest = response.ok ? await response.json() : {};
    } catch (_) {
      manifest = {};
    }
  }
  // Only adopt a positive-integer totalPages — a missing / NaN / string value would NaN-stick the
  // whole reader. Fall back to the known standard count; never assign NaN/undefined.
  const manifestTotal = Number(manifest.totalPages);
  if (Number.isInteger(manifestTotal) && manifestTotal > 0) {
    state.totalPages = manifestTotal;
  } else {
    state.totalPages = STANDARD_TOTAL_PAGES;
  }
  state.currentPage = DEFAULT_START_PAGE;

  // Fold the full song index into state and refresh everything that depends on it.
  // Runs immediately if the index came inline with the manifest (offline / ?admin
  // build), otherwise in the background once pages.json lands (see below).
  const hydrateSongIndex = (data) => {
    if (!data || !data.songIndex) return;
    state.songIndex = [...data.songIndex].sort((left, right) => left.song - right.song);
    state.totalSongs = state.songIndex.length;
    state.themeIndex = data.themeIndex || [];
    state.songPageLookup = buildSongPageLookup(state.songIndex);
    // totalPages may have been painted from inline #pages-data and never reconciled with the
    // authoritative fetched pages.json. If they drift, adopt the fetched value (positive integer
    // only) and re-render so paging/padding stay correct.
    const dataTotal = Number(data.totalPages);
    if (Number.isInteger(dataTotal) && dataTotal > 0 && dataTotal !== state.totalPages) {
      state.totalPages = dataTotal;
      renderPage(state.currentPage, { pushToHistory: false });
    }
    renderStatus();
    renderActiveTab();
  };
  if (manifest.songIndex) hydrateSongIndex(manifest);
  updateBookLabel();
  renderDraft();
  renderStatus();
  updateFullscreenButton();
  hideLoadingIndicator();
  // Show the reader IMMEDIATELY — never block the congregation behind the big offline pre-cache.
  // The service worker caches every page in the background.
  setOfflineGateState({ visible: false });
  if (hasNativeBridge() || NATIVE_FILE_MODE) {
    // NATIVE: ask native for the real page BEFORE revealing anything, so a follower's WebView
    // (re)mount never shows a page number that was never confirmed real (see
    // firstNativePageSignal's comment above for the full "why"). bridge-ready is native's ONLY
    // channel for this, so it moves up here instead of firing at the very end of initReader —
    // posting it after the reveal was the bug: the correction always arrived after the wrong
    // page was already on screen. Bounded so a genuinely first-ever boot (nothing saved, no
    // director broadcasting yet) still reveals promptly rather than hanging behind a spinner.
    postNativeBridge({
      type: "bridge-ready",
      page: state.currentPage,
      totalPages: state.totalPages,
      book: state.currentBook,
    });
    if (firstNativePageSignal) {
      await Promise.race([
        firstNativePageSignal,
        new Promise((resolve) => setTimeout(resolve, FIRST_NATIVE_PAGE_TIMEOUT_MS)),
      ]);
    }
  } else {
    // Open directly on the director's current page if one is broadcasting (the relay state is
    // tiny — just a page number — so this barely delays first paint). Bounded by a short timeout
    // so a slow/dead relay can't block the reader.
    await Promise.race([relayPollOnce(true), new Promise((resolve) => setTimeout(resolve, 1500))]);
    if (!relay.hasDirector) renderPage(DEFAULT_START_PAGE, { pushToHistory: false });
  }
  // Fully public + single-book: reveal now — either a real page arrived (native) or there is
  // nothing further to wait for (web, no geo, no book gate).
  revealReader();
  // Hydrate the song index in the background if it wasn't inlined — keeps ~50 KB
  // off the critical first paint without losing titles, themes, or jump-to-song.
  if (!manifest.songIndex) {
    // Default cache mode ON PURPOSE — this hydration runs on EVERY web boot (build.mjs inlines
    // only {totalPages}), and it is the sole source of the song index. As a no-store request it
    // would hit the SW's network-only passthrough and reject offline, leaving songIndex empty:
    // every numpad jump would land on the LAST page (resolveSongPage's fallback), titles/browse/
    // prev-next would die — at Mass, on a device carrying a perfectly cached manifest it wrote
    // itself. Default mode flows through the SW's network-first branch: fresh online, cached
    // offline. no-store is reserved for the cache WRITERS (cacheAssetsNoStore/cacheSinglePage).
    fetch(resolveAppPath(`/books/${BOOK_ID}/pages.json`))
      .then((response) => response.json())
      .then(hydrateSongIndex)
      .catch((error) => console.warn("No se pudo cargar el índice de canciones", error));
  }
  // Background pre-cache for everyone — at 13 MB it's cheap enough to always do. Deferred until
  // AFTER the reader is revealed so it doesn't compete with first-paint fetches on weak connections.
  deferOfflinePrecache(state.totalPages);
  startRelayFollow();
  scheduleFleetCheckin(); // report readiness to the director's dashboard + offer the one-time picker
  // Search index is loaded lazily on first search-open (see activateTab) so it
  // never weighs down the follower's first paint.
  renderActiveTab();
  // bridge-ready (native shell) is posted earlier now, before the reveal gate above — see
  // firstNativePageSignal's comment. Posting it twice per mount would run native's bridge-ready
  // handler twice (AsyncStorage writes, a possible re-broadcast) for zero benefit.
};

// THREE INDEPENDENT NUMBERS, NEVER COLLAPSED INTO ONE.
//
// What was here: `nativeBuild || webBuild` — the native value REPLACED the web one, so a device
// showed a single bare number with nothing saying what it counted. That cost a full debugging
// session on 2026-08-05: a native app on build 393 and the web app on build 396 were
// indistinguishable on screen, and an iPad was mistaken for an iPhone for over an hour.
//
// It also hid defect D1 in plain sight. The native number is the SHELL's, and a shell can render a
// months-old songbook (that is the entire reason bookVersion exists) — so "395" could sit above a
// book from June and look perfectly current.
//
// Now every surface answers four questions at a glance — v1.0.4 (395-396-374) PAD:
//   395  which BINARY — reads "web" on the web, so the surface is always explicit
//   396  which WEB bundle — the OTA-updatable half; this is what a songbook update actually moves
//   374  which BOOK — pages in the bundle CURRENTLY MOUNTED, not what the shell believes
//   PAD  which DEVICE — injected by native, outside the parentheses; absent on web
//
// A mismatch is now readable rather than invisible: 395-396 means the web half was updated over the
// air past its binary, which is exactly what a working OTA looks like.
//
// DEVICE KIND IS NEVER GUESSED HERE. iPadOS 13+ reports itself as Macintosh to a WebView — no
// "iPad" in the UA, `navigator.platform` is "MacIntel" — so any browser-side check is a
// maxTouchPoints heuristic that can label an iPad wrongly, which is the exact error this is meant
// to eliminate. Only the OS's own answer is trusted (PdfReaderApp.tsx DEVICE_KIND). On the web the
// slot is simply absent, and "web" already says what that surface is.
const strip = (v) => (typeof v === "string" && v && v[0] !== "_" ? v : ""); // reject un-replaced tokens
const nativeBuild = String(window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || "");
const deviceKind = window.__SIGNO_VIVO_DEVICE_KIND === "PAD" || window.__SIGNO_VIVO_DEVICE_KIND === "PHN"
  ? window.__SIGNO_VIVO_DEVICE_KIND
  : "";
const webBuild = strip(BUILD_NUMBER);
const baseVersion = strip("1.0.4");
const bookPages = Number(STANDARD_TOTAL_PAGES) || 0;

// THE FORMAT IS FIXED AND POSITIONAL:  v1.0.4 (404-406-373)
//                                              │   │   └─ pages in the MOUNTED book
//                                              │   └───── web bundle build (the OTA-updatable half)
//                                              └───────── binary build (the shell; never moves OTA)
//
// Owner's call, 2026-08-06. Positional rather than suffixed (`404b · 406w · 373p`) because these
// three numbers get read aloud across a choir loft and compared between devices — "four-oh-four,
// four-oh-six, three-seventy-three" is one utterance, and two devices differ at a glance by which
// column disagrees. The slots are NEVER reordered and NEVER collapsed: the whole point is that the
// binary can sit still while the web build and the page count move, which is what a working OTA
// looks like from the outside.
//
// Missing values render as "—" rather than being dropped, so the reading stays positional. On the
// web there is no binary, so slot 1 reads "web".
const slot = (v) => (v ? String(v) : "—");
const buildLabel = [
  nativeBuild ? slot(nativeBuild) : "web",
  slot(webBuild),
].join("-");
// SONGS, NOT PAGES (owner, 2026-08-17). The third slot was a raw page count, which reads as "how
// long is this document" — a fact nobody in a choir loft needs. Song n IS page n in this book, so
// the number is identical and only the noun was wrong. Named rather than positional, because a
// bare integer in a triple is exactly what made it ambiguous.
const songCount = bookPages ? `${bookPages} cantos` : "";
// Device kind stays OUTSIDE the parentheses — it is not a build number, and putting it in a
// positional triple would make the triple mean different things on different devices.
const kindSuffix = deviceKind ? ` ${deviceKind}` : "";

if (appVersionLabel) {
  appVersionLabel.textContent = baseVersion
    ? `Versión ${baseVersion} (${buildLabel})${kindSuffix}${songCount ? ` · ${songCount}` : ""}`
    : `Versión ${buildLabel}${kindSuffix}${songCount ? ` · ${songCount}` : ""}`;
}
// Small always-visible badge — shown on BOTH web and native (rendered in the WebView, so it
// respects env(safe-area-inset-bottom) via viewport-fit=cover and never tucks under the iPhone home
// indicator).
const buildBadge = document.getElementById("build-badge");
if (buildBadge) {
  // Same shape as the settings line, with the leading "v" — this is the string someone reads out
  // over the phone, so the two surfaces must never disagree about what they are showing.
  buildBadge.textContent = baseVersion
    ? `v${baseVersion} (${buildLabel})${kindSuffix}${songCount ? ` · ${songCount}` : ""}`
    : `${buildLabel}${kindSuffix}${songCount ? ` · ${songCount}` : ""}`;
  syncBuildBadgeVisibility();

  // TAP THE BADGE TO READ THIS DEVICE'S CRUMB LOG. Native has captured up to 200 breadcrumbs across
  // restarts this whole time, and the viewer below has shipped in every build — but the ONE control
  // that connects them went away with the "¿Algo anda mal?" drawer in #342, which the commit itself
  // flagged as knowingly accepted. The consequence only became clear after a Mass went wrong: six
  // devices were each holding a written account of what they did, and there was no way to open it.
  //
  // The badge is the right home and costs nothing. It already answers "what is this device
  // holding?", and it is drawn on PAGE 1 ONLY (syncBuildBadgeVisibility), so during Mass — when
  // nobody is on page 1 — there is no new control anywhere near the music. No code to memorise, no
  // menu, no clutter on the 371 pages that matter.
  //
  // NATIVE SHELL ONLY: signovivo.com has no bridge to ask, so on the public web the badge stays
  // exactly what it was. Parity is preserved by doing nothing there rather than by showing an
  // empty dialog.
  if (hasNativeBridge() || NATIVE_FILE_MODE) {
    buildBadge.removeAttribute("aria-hidden");
    buildBadge.setAttribute("role", "button");
    buildBadge.setAttribute("tabindex", "0");
    buildBadge.setAttribute("aria-label", "Ver diagnóstico de este dispositivo");
    buildBadge.style.cursor = "pointer";
    // THE TAP NEVER WORKED. styles.css sets `pointer-events: none` on .build-badge so a label
    // floating over the music can never swallow a page-turn — correct for the label, fatal for the
    // button this block turns it into. cursor/role/tabindex/listener were all applied and the
    // element still could not receive a click, on ANY device. Reported 2026-08-17 as "the badge
    // does nothing"; the diagnostics viewer has been unreachable by tap since it shipped, which is
    // the same class of failure as #342 removing the drawer: the crumb log exists, the door doesn't.
    //
    // Re-enabled HERE rather than in the stylesheet so it is scoped to the native shell, where the
    // badge is genuinely a control. On signovivo.com it stays a pure label and keeps
    // pointer-events: none, so the web behaviour is unchanged.
    buildBadge.style.pointerEvents = "auto";
    // 8px text is a ~10px tall target. Pad the HIT AREA out to the 44px iOS minimum without
    // changing a pixel of what is drawn: transparent padding, negative margin to keep the glyph
    // exactly where it was. Someone hunting for a faint grey mark in the corner should not also
    // have to hit it precisely.
    buildBadge.style.padding = "17px";
    buildBadge.style.margin = "-17px";
    const askForDiagnostics = () => {
      haptic(12);
      // Native answers with a `diagnostics` event, which applyNativeSyncEvent hands to
      // showDiagnostics. If an older shell does not understand this message it is simply ignored —
      // the badge stays a label, which is what it was before.
      postNativeBridge({ type: "request-diagnostics" });
    };
    buildBadge.addEventListener("click", askForDiagnostics);
    buildBadge.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") { event.preventDefault(); askForDiagnostics(); }
    });
  }
}

// Screen wake lock (P7): keep the screen awake while the reader is open, so a follower's
// phone/iPad doesn't sleep and go dark mid-Mass (the native shell has expo-keep-awake;
// web followers on signovivo.com had nothing). Fully guarded — unsupported browsers and
// rejected requests are silent no-ops, and the lock is re-acquired when the tab returns
// to the foreground (the platform auto-releases it when the page is hidden). A nicety;
// it must never affect boot.
const initScreenWakeLock = () => {
  try {
    if (!("wakeLock" in navigator) || !navigator.wakeLock || typeof navigator.wakeLock.request !== "function") return;
    let sentinel = null;
    const acquire = async () => {
      try {
        if (document.visibilityState !== "visible" || sentinel) return;
        sentinel = await navigator.wakeLock.request("screen");
        sentinel.addEventListener("release", () => { sentinel = null; });
      } catch (_) { sentinel = null; /* not focused / not allowed — harmless */ }
    };
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") acquire();
    });
    acquire();
  } catch (_) { /* never let a wake-lock hiccup affect boot */ }
};

clearInitialUrl();
registerServiceWorker();
bindViewportMetrics();
bindReaderEvents();
initScreenWakeLock();
initReader().catch((error) => {
  console.error("No se pudo iniciar el lector", error);
  setLoading(true, "No se pudo cargar Signo Vivo.");
  revealReader();   // even on a boot failure, don't trap the user behind a blank white gate
});
// Absolute anti-trap safety net: the reader reveals immediately from initReader (single-book, fully
// public — nothing to wait for). This is a belt-and-suspenders backstop in case a boot-time throw
// somehow bypasses that reveal — force the (legacy) loading gate down so the user is never stranded
// behind a blank screen. Idempotent.
setTimeout(liftGateNow, 4000);

// ?selftest — the operator's GREEN/RED readiness card (Release Safety System, M1).
// Read-only (loads one image + GETs the relay /state on the CURRENT room) and gated
// strictly on the query param: without ?selftest this block is a no-op, and every
// layer swallows its own errors so a broken selftest can never break the reader it
// is testing. Runs after boot settles so it reads the real hydrated state.
// NOTE: gate on initialUrl.search (captured at line ~149), NOT live location.search —
// clearInitialUrl() strips the query via history.replaceState before this runs.
try {
  const selftestSearch = (typeof initialUrl !== "undefined" && initialUrl && initialUrl.search) || window.location.search || "";
  if (/(?:^|[?&])selftest(?:=|&|$)/.test(selftestSearch)) {
    setTimeout(() => {
      try {
        const st = globalThis.svSelftest;
        if (!st || typeof st.computeChecks !== "function") return;
        st.computeChecks({
          loadImage: (url) => new Promise((resolve) => {
            try {
              const im = new Image();
              im.onload = () => resolve(true);
              im.onerror = () => resolve(false);
              im.src = url;
            } catch (_) { resolve(false); }
          }),
          fetchImpl: (url) => fetch(url, { cache: "no-store" }),
          pageUrl: resolvePageSrc(1),
          totalPages: state.totalPages,
          relayBase: RELAY_BASE,
          relayRoom: RELAY_ROOM,
          buildNumber: BUILD_NUMBER,
          cacheVersion: CACHE_VERSION,
          isNativeFileMode: NATIVE_FILE_MODE,
          bridgeAvailable: state.nativeBridgeAvailable,
        }).then((result) => {
          try { st.renderCard(result, document); } catch (_) {}
        });
      } catch (_) { /* selftest must never break the reader */ }
    }, 1800);
  }
} catch (_) { /* selftest must never break the reader */ }
