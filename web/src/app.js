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
      try { window.__SV_LAST_ERROR = { where: where, msg: String((err && err.message) || err || ""), t: Date.now() }; } catch (_) {}
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
const STANDARD_TOTAL_PAGES = 371;
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
const SW_RELOAD_FLAG = "sv-sw-reload-pending";
const CACHE_VERSION = "__CACHE_VERSION__";
// Human-facing build number, baked from version.json at build time, shown as a small "v<NNN>" badge
// so signovivo.com always reveals which build it's on. Native injects its own version + overlay.
const BUILD_NUMBER = "__BUILD_NUMBER__";
const STATIC_CACHE = `signo-vivo-static-${CACHE_VERSION}`;
const PAGE_CACHE = `signo-vivo-pages-${CACHE_VERSION}`;
const OFFLINE_READY_KEY = `sv-offline-ready-${CACHE_VERSION}`;
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
// The book's manifests live under books/standard/. Cache them for the offline bundle.
const coreAssets = () => [
  ...SHELL_ASSETS,
  resolveAppPath(`/books/${BOOK_ID}/pages.json`),
  resolveAppPath(`/books/${BOOK_ID}/search-index.json`),
];

// ── Utilities ────────────────────────────────────────────────────────────────
// pdftoppm zero-pads page filenames to the WIDTH of the book's total page count
// (standard: 371 → page-001.webp). Derive the pad width from the live totalPages,
// falling back to the known standard count before the manifest lands.
const pagePadWidth = () => String(state.totalPages || STANDARD_TOTAL_PAGES || 0).length;
const pageFileName = (pageNumber) => {
  const padded = String(pageNumber).padStart(pagePadWidth(), "0");
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
    console.error("No se pudo hablar con la app nativa", error);
    return false;
  }
};

const formatVerifiedAt = (isoString) => {
  if (!isoString) return "";
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("es-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
};

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

const getCachedPageSet = async (cache) => {
  const keys = await cache.keys();
  return new Set(
    keys
      .map(extractCachedPageNumber)
      .filter((pageNumber) => Number.isFinite(pageNumber)),
  );
};

const ensureCoreAssetsCached = async () => {
  const cache = await caches.open(STATIC_CACHE);
  // Cache each core/book-manifest asset INDEPENDENTLY (Promise.allSettled), mirroring the SW
  // install's resilience — NOT an atomic cache.addAll(). With addAll, a single transient drop
  // or a 404 (e.g. search-index.json missing on a partially-deployed origin) rejects the whole
  // batch, aborting the offline-bundle prep before ANY pages cache → the book never precaches
  // and stays unusable offline, retry after retry hitting the same weak link. allSettled lets
  // the run proceed past one bad asset so the page-precache step still gets to run.
  await Promise.allSettled(
    coreAssets().map((asset) => cache.add(asset)),
  );
};

const cacheSinglePage = async (cache, pageNumber) => {
  const url = pageFileName(pageNumber);
  if (await cache.match(url)) return false;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`No se pudo descargar la página ${pageNumber}`);
  }
  await cache.put(url, response.clone());
  return true;
};

const ensureOfflineBundle = async (totalPages, onProgress) => {
  await ensureCoreAssetsCached();
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
  localStorage.setItem(OFFLINE_READY_KEY, "ready");
  fleetCheckin({ webCached: true }); // tell the readiness dashboard this iPad is fully cached
  await writeOfflineMetadata({
    version: CACHE_VERSION,
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
const deferOfflinePrecache = (totalPages) => {
  if (NATIVE_FILE_MODE || !("caches" in window)) return;
  if (offlinePrecacheStarted) return;
  const SETTLE_MS = 4000;   // base delay so first paint + relay handshake finish first
  const POLL_MS = 1500;     // how often we re-check the gate after the settle delay
  const kick = () => {
    if (offlinePrecacheStarted) return;
    offlinePrecacheStarted = true;
    ensureOfflineBundle(totalPages, () => {}).catch((error) => {
      offlinePrecacheStarted = false;   // failed → let a later trigger retry
      console.warn("Pre-cache offline incompleto:", error);
    });
  };
  const waitForGate = () => {
    if (gateLifted) { kick(); return; }
    setTimeout(waitForGate, POLL_MS);
  };
  setTimeout(waitForGate, SETTLE_MS);
};

const isOfflineBundleReady = async (totalPages) => {
  if (!("caches" in window)) return false;
  if (localStorage.getItem(OFFLINE_READY_KEY) !== "ready") return false;
  try {
    const metadata = await readOfflineMetadata();
    if (!metadata) return false;
    if (metadata.version !== CACHE_VERSION) return false;
    if (metadata.totalPages !== totalPages) return false;

    const staticCache = await caches.open(STATIC_CACHE);
    const coreMatches = await Promise.all(
      coreAssets().map((asset) => staticCache.match(asset)),
    );
    if (coreMatches.some((match) => !match)) return false;

    const pageCache = await caches.open(PAGE_CACHE);
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
const renderDirectorModeBadge = () => {
  const isDirector = state.nativeSyncRole === "director";
  // Drive the control layout: followers (web + any non-director native) get ⟳ resync + ♪;
  // a director gets ♪ + ⌕ search. Default "follower" so signovivo.com is right from boot.
  document.documentElement.dataset.role = isDirector ? "director" : "follower";
  if (!directorModeBadge) return;
  directorModeBadge.classList.toggle("is-hidden", !isDirector);
};

// ── Sync "working" spinner ──────────────────────────────────────────────────────
// A small top-center pill with a spinner, shown whenever the native mesh is actively trying to
// connect (searching / connecting) — so the ~4s handshake and any reconnect read as "working," not
// "frozen." Hidden the instant we're connected (or idle / self-directed). Native-only: it's driven
// by the mesh's state events; web relay followers use the relay pill instead.
let syncSpinnerEl = null;
const ensureSyncSpinner = () => {
  if (syncSpinnerEl) return syncSpinnerEl;
  const style = document.createElement("style");
  style.textContent =
    "#sv-sync-spinner{position:fixed;top:max(0.7rem,env(safe-area-inset-top,0px));left:50%;" +
    "transform:translateX(-50%);z-index:60;display:none;align-items:center;gap:0.5rem;" +
    "padding:0.5rem 0.95rem;border-radius:999px;background:rgba(26,26,46,0.9);color:#fff;" +
    "font:600 0.85rem/1 system-ui,-apple-system,sans-serif;box-shadow:0 4px 16px rgba(0,0,0,.4);" +
    "pointer-events:none;-webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px)}" +
    "#sv-sync-spinner.is-on{display:flex}" +
    "#sv-sync-spinner .sv-dot{width:14px;height:14px;border-radius:50%;" +
    "border:2px solid rgba(255,255,255,0.25);border-top-color:#fff;animation:sv-syncspin .7s linear infinite}" +
    "@keyframes sv-syncspin{to{transform:rotate(360deg)}}";
  document.head.appendChild(style);
  syncSpinnerEl = document.createElement("div");
  syncSpinnerEl.id = "sv-sync-spinner";
  syncSpinnerEl.setAttribute("role", "status");
  syncSpinnerEl.setAttribute("aria-live", "polite");
  syncSpinnerEl.innerHTML = '<span class="sv-dot"></span><span class="sv-label"></span>';
  document.body.appendChild(syncSpinnerEl);
  return syncSpinnerEl;
};
const setSyncWorking = (status) => {
  const working = status === "searching" || status === "connecting" || status === "resolving-conflict";
  const el = ensureSyncSpinner();
  if (working) {
    el.querySelector(".sv-label").textContent =
      status === "searching" ? "Buscando director…" : "Conectando…";
  }
  el.classList.toggle("is-on", working);
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
    "transform:translateX(-50%);z-index:70;display:none;max-width:min(92vw,34rem);" +
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

const applyNativeSyncEvent = async (payload) => {
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

  if (payload.type !== "sync-event") return;

  const event = payload.event || {};

  // Connection lifecycle from the native mesh → drive the "working" spinner.
  if (event.type === "state") {
    setSyncWorking(String(event.status || ""));
    return;
  }

  if (event.type === "page" && Number.isFinite(event.page)) {
    // Single-book app: ignore any event.book and just render the director's page.
    renderPage(event.page, { pushToHistory: false });
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

const renderPage = async (pageNumber, { pushToHistory = true, direction = 0 } = {}) => {
  const nextPage = clampPage(pageNumber);
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
      postNativeBridge({ type: "director-code", code });
      clearDraft();
      closeSongJump();
    } else {
      clearDraft();
      flashSongDisplay("Código no válido", "err");
    }
    return;
  }
  const targetPage = findSongPage(songNumber);
  renderPage(targetPage);
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
  renderPage(prevPage, { pushToHistory: false });
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
    const response = await fetch(resolveAppPath(`/books/${BOOK_ID}/search-index.json`), { cache: "no-store" });
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
      renderPage(state.songIndex[0].page, { direction });
      clearDraft();
      if (!keepOverlay) closeDrawer();
    }
    return;
  }
  const nextIndex = clampSongIndex(currentSongIndex + direction);
  if (nextIndex === currentSongIndex) return;
  renderPage(state.songIndex[nextIndex].page, { direction });
  clearDraft();
  if (!keepOverlay) closeDrawer();
};

const turnPage = (direction) => {
  const nextPage = clampPage(state.currentPage + direction);
  if (nextPage === state.currentPage) return;
  renderPage(nextPage, { direction });
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
  const searchFab = document.getElementById("search-fab");
  if (searchFab) searchFab.addEventListener("click", () => { haptic(); navigationDrawer.classList.add("as-dropdown"); openDrawer(); activateTab("buscar"); });

  // ⟳ resync fab (top-left, follower) — rejoin the live director + refresh the connection.
  // Spin the icon for ~1.1s on tap so it's obvious it's reconnecting/refreshing.
  const resyncFab = document.getElementById("resync-fab");
  if (resyncFab) resyncFab.addEventListener("click", () => {
    resyncFab.classList.add("is-spinning");
    window.setTimeout(() => resyncFab.classList.remove("is-spinning"), 1100);
    reconnectRelay();
  });
  // ⛶ fullscreen fab (bottom-right) — same toggle as the legacy hidden button
  if (fullscreenFab) fullscreenFab.addEventListener("click", () => { haptic(); toggleFullscreen().catch(() => {}); });

  // Director badge = tap-to-EXIT director mode (native only — the badge never shows on the web).
  // Confirm first so a mid-Mass mistap doesn't drop the director; the shell does the soft reset.
  if (directorModeBadge) directorModeBadge.addEventListener("click", () => {
    haptic();
    if (window.confirm("¿Salir del modo director?\n\nDejarás de dirigir y volverás a seguidor.")) {
      postNativeBridge({ type: "exit-director" });
    }
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
        renderPage(pageNum);
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
const RELAY_BASE_RAW = "__RELAY_BASE__";
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

// ── Fleet readiness check-in (signovivo.com PWA only — the native app reports itself) ─────────
// A device self-reports its cache + home-screen state so the director's gated /fleet-dashboard
// shows who's ready before Mass. A device sends ONLY a self-entered label — NEVER a phone number
// (phones live server-side in the gated roster). Fire-and-forget; failures are swallowed so this
// can never block or slow the reader.
const FLEET_LABEL_KEY = "svFleetLabel";
const FLEET_ROLE_KEY = "svFleetRole";
const FLEET_DEVICE_KEY = "svFleetDeviceId";
const FLEET_SKIP_KEY = "svFleetSkip";

const fleetDeviceId = () => {
  try {
    let id = localStorage.getItem(FLEET_DEVICE_KEY);
    if (!id) {
      id = window.crypto?.randomUUID?.()
        || "d-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
      localStorage.setItem(FLEET_DEVICE_KEY, id);
    }
    return id;
  } catch {
    return "anon-" + Math.random().toString(36).slice(2);
  }
};

// Rough count of cached page images across all page caches (immutable, so max across versions).
const countCachedPageImages = async () => {
  if (!("caches" in window)) return 0;
  try {
    const names = await caches.keys();
    let max = 0;
    for (const nm of names) {
      if (!nm.includes("pages")) continue;
      const c = await caches.open(nm);
      const keys = await c.keys();
      max = Math.max(max, keys.filter((r) => r.url.includes("/pages/")).length);
    }
    return max;
  } catch {
    return 0;
  }
};

let fleetCheckinInFlight = false;
const fleetCheckin = async (extra = {}) => {
  if (NATIVE_FILE_MODE) return; // the native app does its own check-in
  if (fleetCheckinInFlight) return;
  fleetCheckinInFlight = true;
  try {
    let label = "";
    let role = "";
    let webCached = false;
    try {
      label = localStorage.getItem(FLEET_LABEL_KEY) || "";
      role = localStorage.getItem(FLEET_ROLE_KEY) || "";
      webCached = localStorage.getItem(OFFLINE_READY_KEY) === "ready";
    } catch {}
    const totalPages = Number(state.totalPages) || STANDARD_TOTAL_PAGES;
    const pagesCached = await countCachedPageImages();
    const payload = {
      deviceId: fleetDeviceId(),
      label,
      role,
      surface: "web",
      webCached: webCached || (totalPages > 0 && pagesCached >= totalPages),
      pagesCached,
      totalPages,
      homeScreen: isStandaloneApp,
      cacheVersion: String(CACHE_VERSION || ""),
      ...extra,
    };
    await fetch(RELAY_BASE + "/fleet/checkin", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      keepalive: true,
    });
  } catch {
    /* offline / relay unreachable — presence just isn't reported; reader is unaffected */
  } finally {
    fleetCheckinInFlight = false;
  }
};

// One-time "¿Quién usa este iPad?" prompt. Choir members self-identify (name + role) for the
// dashboard; a Skip still checks in anonymously so the device count stays real.
const fleetPicker = document.getElementById("fleet-picker");
const fleetPickerName = document.getElementById("fleet-picker-name");
const fleetPickerRole = document.getElementById("fleet-picker-role");
const fleetPickerSave = document.getElementById("fleet-picker-save");
const fleetPickerSkip = document.getElementById("fleet-picker-skip");

const hideFleetPicker = () => fleetPicker?.classList.add("is-hidden");
const maybeShowFleetPicker = () => {
  if (NATIVE_FILE_MODE || !fleetPicker) return;
  try {
    if (localStorage.getItem(FLEET_LABEL_KEY) || localStorage.getItem(FLEET_SKIP_KEY) === "1") return;
  } catch {}
  fleetPicker.classList.remove("is-hidden");
  setTimeout(() => fleetPickerName?.focus(), 200);
};
fleetPickerSave?.addEventListener("click", () => {
  const name = (fleetPickerName?.value || "").trim().slice(0, 40);
  const role = fleetPickerRole?.value || "Cantor";
  if (!name) {
    fleetPickerName?.focus();
    return;
  }
  try {
    localStorage.setItem(FLEET_LABEL_KEY, name);
    localStorage.setItem(FLEET_ROLE_KEY, role);
  } catch {}
  hideFleetPicker();
  fleetCheckin({ label: name, role });
});
fleetPickerSkip?.addEventListener("click", () => {
  try {
    localStorage.setItem(FLEET_SKIP_KEY, "1");
  } catch {}
  hideFleetPicker();
  fleetCheckin(); // anonymous — device still appears on the dashboard, just unlabeled
});

// Report presence + cache state on load, then (once the gate has lifted) offer the picker.
const scheduleFleetCheckin = () => {
  if (NATIVE_FILE_MODE) return;
  fleetCheckin();
  const tryPicker = (n = 0) => {
    if (gateLifted) {
      maybeShowFleetPicker();
    } else if (n < 20) {
      setTimeout(() => tryPicker(n + 1), 1000);
    }
  };
  setTimeout(() => tryPicker(0), 2500);
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

const relayIsFreshLive = (snap) =>
  snap && Number.isFinite(snap.seq) && snap.seq > 0 &&
  (!snap.ts || (Date.now() / 1000) - snap.ts <= RELAY_LIVE_MAX_AGE_S);

const applyRelaySnapshot = async (snap, { force = false } = {}) => {
  // Number.isFinite rejects NaN (typeof NaN === "number" let it slip the old guard) →
  // a NaN page would clamp/render bogusly. Reject non-finite page outright.
  if (!snap || !Number.isFinite(snap.page)) return;
  // Single-book app: snap.bookId (kept on the wire for build-373 compat) is effectively always
  // "standard", so there is no book to switch — the director's page renders straight against the
  // one book. renderPage clamps any out-of-range page into range.
  // Ongoing pushes are de-duped / ordered by seq (Number.isFinite so a NaN seq can't
  // slip the guard). A FORCED resync (initial load, reconnect, foreground, or the safety
  // poll) must re-apply the director's CURRENT page even when the seq isn't newer — the
  // director may be sitting still — so it skips the seq guard.
  if (!force && Number.isFinite(snap.seq) && snap.seq > 0 && snap.seq <= relay.lastSeq) return;
  if (Number.isFinite(snap.seq)) relay.lastSeq = Math.max(relay.lastSeq, snap.seq);

  const hasPublished = Number.isFinite(snap.seq) && snap.seq > 0;
  // No director has ever published, OR the director's snapshot is older than the freshness
  // window (RELAY_LIVE_MAX_AGE_S, ~90s) → behave like a normal songbook. The staleness check
  // applies on the FORCED path too: a forced resync (boot poll, reconnect, foreground, polling
  // fallback) fires exactly when the WS is dead and we're scraping /state — marking a 90s-stale
  // director LIVE there (green pill + rendering its stale page) is the wrong call. force still
  // bypasses the seq guard above for a FRESH director sitting still; it does NOT resurrect a
  // stale one.
  if (!hasPublished || !relayIsFreshLive(snap)) {
    relay.hasDirector = false;
    // No live director → the "Volver a en vivo" bar would lie about going live (there's
    // nowhere to go). Hide it and drop browsing state so bar, pill, and browse flag stay
    // consistent: the user reverts to a normal songbook they can freely navigate.
    relay.browsing = false;
    hideGoLiveBar();
    renderRelayPill();
    return;
  }
  relay.hasDirector = true;
  relay.livePage = snap.page;

  // The user is intentionally browsing the songbook (tapped the title → jumped). Track the
  // director's latest page so "Volver a en vivo" lands on the current spot, but DON'T yank
  // them off their page.
  if (relay.browsing) { renderRelayPill(); revealReader(); return; }

  // A congregation follower should ALWAYS track the director. We only reach here for a
  // NEW director position (same-seq heartbeat pings are seq-guarded above), so the director
  // just moved — snap to it and (re-)engage following, EVEN IF a stray Safari swipe/scroll
  // had nudged the page off and "browsed away". The old behavior stranded followers on the
  // amber "tap to resync" dot, permanently out of sync (seen on video: director on 367,
  // follower frozen on 365). You can still peek between the director's moves — the next
  // move pulls you home; tap the dot to resync immediately.
  relay.following = true;
  relay.appliedPage = snap.page;
  if (state.currentPage !== snap.page) renderPage(snap.page, { pushToHistory: false });
  renderRelayPill();
  // Reveal once a follower is homed onto the director's page. Idempotent — safe to call anytime.
  revealReader();
};

const relayStateUrl = () => RELAY_BASE + "/r/" + encodeURIComponent(RELAY_ROOM) + "/state";
const relayWsUrl = () => RELAY_BASE.replace(/^http/, "ws") + "/r/" + encodeURIComponent(RELAY_ROOM) + "/subscribe";

const stopRelayPolling = () => { if (relay.pollTimer) { clearInterval(relay.pollTimer); relay.pollTimer = 0; } };
// Poll the relay for the director's current snapshot and apply it. Single-book + fully public:
// there is no geo header to read and no book to resolve — just fetch /state and apply the page.
const relayPollOnce = async (force = false) => {
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
    if (r.ok) await applyRelaySnapshot(await r.json(), { force });
  } catch {}
};
// Fallback polling (when the WS won't hold): force every tick so a stationary director
// still keeps the follower in sync.
const startRelayPolling = () => { stopRelayPolling(); relay.pollTimer = setInterval(() => relayPollOnce(true), 4000); relayPollOnce(true); };

const connectRelay = () => {
  // Idempotent: if a socket is already CONNECTING (0) or OPEN (1), don't open a duplicate.
  // iOS fires `online` on network changes even while a socket is healthy, and the close-
  // handler's backoff reconnect can race with it — without this guard each call would stack
  // another live socket, each with its own 4s heartbeat + message handler.
  if (relay.ws && (relay.ws.readyState === 0 || relay.ws.readyState === 1)) return;
  relay.manualClose = false;
  stopRelayPolling();
  // A reconnect is firing now (or was requested) — cancel any pending backoff timer so it
  // can't schedule a second connect on top of this one.
  if (relay.reconnectTimer) { clearTimeout(relay.reconnectTimer); relay.reconnectTimer = 0; }
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
      if (Date.now() - relay.lastMsgAt > 12000) { try { ws.close(); } catch {} return; }
      try { ws.send("ping"); } catch {}
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

const startRelayFollow = () => {
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
      const response = await fetch(resolveAppPath(`/books/${BOOK_ID}/pages.json`), { cache: "no-store" });
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
  // Open directly on the director's current page if one is broadcasting (the relay state is tiny —
  // just a page number — so this barely delays first paint). Bounded by a short timeout so a slow/
  // dead relay can't block the reader. The native app / offline bundle skip the relay (the native
  // bridge drives the page there).
  if (!hasNativeBridge() && !NATIVE_FILE_MODE) {
    await Promise.race([relayPollOnce(true), new Promise((resolve) => setTimeout(resolve, 1500))]);
  }
  if (!relay.hasDirector) renderPage(DEFAULT_START_PAGE, { pushToHistory: false });
  // Fully public + single-book: reveal the reader immediately (no geo, nothing to wait for).
  revealReader();
  // Hydrate the song index in the background if it wasn't inlined — keeps ~50 KB
  // off the critical first paint without losing titles, themes, or jump-to-song.
  if (!manifest.songIndex) {
    fetch(resolveAppPath(`/books/${BOOK_ID}/pages.json`), { cache: "no-store" })
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
  // bridge-ready is for the native shell.
  postNativeBridge({
    type: "bridge-ready",
    page: state.currentPage,
    totalPages: state.totalPages,
    book: state.currentBook,
  });
};

// Resolve the build to display: native injects __SIGNO_VINO_NATIVE_BUNDLE_VERSION; web bakes
// BUILD_NUMBER at build time. The `[0] !== "_"` guard rejects an un-replaced "__BUILD_NUMBER__" token.
const resolvedBuild =
  window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION ||
  (BUILD_NUMBER && BUILD_NUMBER[0] !== "_" ? BUILD_NUMBER : "");
if (appVersionLabel && resolvedBuild) {
  appVersionLabel.textContent = `Versión ${resolvedBuild}`;
}
// Small always-visible build badge — shown on BOTH web and native (rendered in the WebView, so it
// respects env(safe-area-inset-bottom) via viewport-fit=cover and never tucks under the iPhone home
// indicator). Just the number — the only person who reads it already knows what it means.
const buildBadge = document.getElementById("build-badge");
if (buildBadge && resolvedBuild) {
  buildBadge.textContent = resolvedBuild;
  buildBadge.classList.add("is-shown");
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
