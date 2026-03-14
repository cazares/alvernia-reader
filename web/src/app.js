const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const overlayControls = document.getElementById("overlay-controls");
const songStatus = document.getElementById("song-status");
const songDisplay = document.getElementById("song-display");
const numberpadGrid = document.getElementById("numberpad-grid");
const clearButton = document.getElementById("clear-button");
const backspaceButton = document.getElementById("backspace-button");
const goButton = document.getElementById("go-button");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const fullscreenButton = document.getElementById("fullscreen-button");
const prevCornerButton = document.getElementById("prev-corner");
const navigationNumberpad = document.getElementById("navigation-numberpad");
const searchToggle = document.getElementById("search-toggle");
const searchPanel = document.getElementById("search-panel");
const searchInput = document.getElementById("search-input");
const searchResults = document.getElementById("search-results");
const searchBack = document.getElementById("search-back");

const state = {
  totalPages: 1,
  totalSongs: 0,
  currentPage: 1,
  currentPageObjectUrl: "",
  songDraft: "",
  songIndex: [],
  pageHistory: [],
  searchIndexPages: [],
  overlayVisible: true,
  immersiveMode: false,
  loadingTimer: 0,
  pageLoadRequest: 0,
  prefetchedPages: new Set(),
  prefetchingPages: new Set(),
  touchStart: null,
  lastTouchEndedAt: 0,
};

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

const pageFileName = (pageNumber) => `/pages/page-${String(pageNumber).padStart(3, "0")}.jpg`;
const pageFileUrl = (pageNumber, retryToken = "") => retryToken
  ? `${pageFileName(pageNumber)}?reload=${retryToken}`
  : pageFileName(pageNumber);
const clampPage = (pageNumber) => Math.max(1, Math.min(pageNumber, state.totalPages));
const clampSongIndex = (index) => Math.max(0, Math.min(index, state.totalSongs - 1));
const getFullscreenElement = () => document.fullscreenElement || document.webkitFullscreenElement || null;
const isFullscreen = () => Boolean(getFullscreenElement());
const scheduleIdleWork = window.requestIdleCallback
  ? window.requestIdleCallback.bind(window)
  : (callback) => window.setTimeout(callback, 140);

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
  if (document.exitFullscreen) {
    return document.exitFullscreen();
  }

  if (document.webkitExitFullscreen) {
    return document.webkitExitFullscreen();
  }

  return null;
};

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

const clearInitialUrl = () => {
  if (!initialUrl.search) return;
  window.history.replaceState({}, "", initialUrl.pathname || "/");
};

const findSongIndexAtOrBeforePage = (pageNumber) => {
  let index = -1;
  for (let i = 0; i < state.songIndex.length; i += 1) {
    if (state.songIndex[i].page > pageNumber) break;
    index = i;
  }
  return index;
};

const findSongPage = (songNumber) => {
  if (songNumber <= 0) return 1;
  const exact = state.songIndex.find((entry) => entry.song === songNumber);
  if (exact) return exact.page;
  const next = state.songIndex.find((entry) => entry.song >= songNumber);
  return next ? next.page : state.totalPages;
};

const getCurrentSongNumber = () => {
  const index = findSongIndexAtOrBeforePage(state.currentPage);
  return index >= 0 ? state.songIndex[index].song : 0;
};

const getAdjacentSongPages = () => {
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  const pages = [];

  if (currentSongIndex < 0) {
    if (state.totalSongs > 0) {
      pages.push(state.songIndex[0].page);
    }
    return pages;
  }

  if (currentSongIndex > 0) {
    pages.push(state.songIndex[currentSongIndex - 1].page);
  }

  if (currentSongIndex < state.totalSongs - 1) {
    pages.push(state.songIndex[currentSongIndex + 1].page);
  }

  return [...new Set(pages)];
};

const prefetchSongPage = (pageNumber) => {
  if (pageNumber < 1 || pageNumber > state.totalPages) return;
  if (pageNumber === state.currentPage) return;
  if (state.prefetchedPages.has(pageNumber) || state.prefetchingPages.has(pageNumber)) return;

  state.prefetchingPages.add(pageNumber);
  scheduleIdleWork(async () => {
    try {
      const response = await fetch(pageFileName(pageNumber), { cache: "force-cache" });
      if (response.ok) {
        state.prefetchedPages.add(pageNumber);
      }
    } catch (error) {
      console.warn("No se pudo precargar la página", pageNumber, error);
    } finally {
      state.prefetchingPages.delete(pageNumber);
    }
  });
};

const renderStatus = () => {
  songStatus.textContent = `Canción ${getCurrentSongNumber()}`;
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  const hasPreviousPage = state.currentPage > 1;
  const hasNextSong = currentSongIndex < 0
    ? state.totalSongs > 0
    : currentSongIndex < state.totalSongs - 1;

  const hasHistory = state.pageHistory.length > 0;
  prevPageButton.disabled = !hasPreviousPage;
  nextPageButton.disabled = !hasNextSong;
  prevCornerButton.disabled = !hasHistory;
  prevPageButton.classList.toggle("is-unavailable", !hasPreviousPage);
  nextPageButton.classList.toggle("is-unavailable", !hasNextSong);
  prevCornerButton.classList.toggle("is-unavailable", !hasHistory);
};

const renderDraft = () => {
  songDisplay.value = state.songDraft;
};

const decodeImage = (src) => new Promise((resolve, reject) => {
  const loader = new Image();
  loader.decoding = "async";
  loader.onload = () => resolve(true);
  loader.onerror = () => reject(new Error("No se pudo decodificar la imagen"));
  loader.src = src;
});

const loadPageImage = async (pageNumber, retryToken = "") => {
  const response = await fetch(pageFileUrl(pageNumber, retryToken), { cache: "force-cache" });
  if (!response.ok) {
    throw new Error(`No se pudo cargar la página ${pageNumber}`);
  }

  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);

  try {
    await decodeImage(objectUrl);
    return objectUrl;
  } catch (error) {
    URL.revokeObjectURL(objectUrl);
    throw error;
  }
};

const renderPage = async (pageNumber, { pushToHistory = true } = {}) => {
  const nextPage = clampPage(pageNumber);
  const requestId = state.pageLoadRequest + 1;
  state.pageLoadRequest = requestId;
  scheduleLoadingIndicator();

  try {
    let nextPageUrl = "";

    try {
      nextPageUrl = await loadPageImage(nextPage);
    } catch (firstError) {
      console.warn("Primer intento falló al cargar la página", nextPage, firstError);
      nextPageUrl = await loadPageImage(nextPage, Date.now());
    }

    if (requestId !== state.pageLoadRequest) {
      if (nextPageUrl) URL.revokeObjectURL(nextPageUrl);
      return;
    }

    if (pushToHistory && state.currentPage > 0 && state.currentPage !== nextPage) {
      state.pageHistory.push(state.currentPage);
      if (state.pageHistory.length > 50) state.pageHistory.shift();
    }

    state.currentPage = nextPage;
    pageImage.src = nextPageUrl;
    pageImage.dataset.page = String(nextPage);
    if (state.currentPageObjectUrl) {
      URL.revokeObjectURL(state.currentPageObjectUrl);
    }
    state.currentPageObjectUrl = nextPageUrl;
    renderStatus();
    hideLoadingIndicator();
    getAdjacentSongPages().forEach(prefetchSongPage);
  } catch (error) {
    if (requestId !== state.pageLoadRequest) return;
    clearLoadingTimer();
    console.error("No se pudo cargar la página solicitada", nextPage, error);
    setLoading(true, "No se pudo cargar esta página.");
    setOverlayVisible(true);
  }
};

const setOverlayVisible = (visible) => {
  state.overlayVisible = visible;
  overlayControls.classList.toggle("is-hidden", !visible);
};

const updateFullscreenButton = () => {
  if (!supportsFullscreen) {
    fullscreenButton.classList.add("is-hidden");
    return;
  }

  fullscreenButton.classList.remove("is-hidden");
  if (canOfferPseudoFullscreen) {
    fullscreenButton.textContent = state.immersiveMode ? "⛶ Salir de pantalla completa" : "⛶ Pantalla completa";
    return;
  }

  fullscreenButton.textContent = isFullscreen() ? "⛶ Salir de pantalla completa" : "⛶ Pantalla completa";
};

const appendDigit = (digit) => {
  if (state.songDraft.length >= 4) return;
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

const goToDraftSong = () => {
  const songNumber = Number.parseInt(state.songDraft, 10);
  if (!Number.isFinite(songNumber)) return;
  renderPage(findSongPage(songNumber));
  clearDraft();
  setOverlayVisible(false);
};

const goBackInHistory = () => {
  if (state.pageHistory.length === 0) return;
  const prevPage = state.pageHistory.pop();
  renderPage(prevPage, { pushToHistory: false });
};

const normalizeText = (text) => text
  .normalize("NFD")
  .replace(/[\u0300-\u036f]/g, "")
  .toLowerCase();

const loadSearchIndex = async () => {
  try {
    const response = await fetch("/search-index.json", { cache: "no-store" });
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

const searchPages = (query) => {
  const normalizedQuery = normalizeText(query.trim());
  if (!normalizedQuery) return [];
  const words = normalizedQuery.split(/\s+/).filter(Boolean);
  const results = [];
  for (const entry of state.searchIndexPages) {
    const normalizedText = normalizeText(entry.text);
    if (words.every((word) => normalizedText.includes(word))) {
      results.push(entry);
    }
  }
  return results.slice(0, 30);
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
  for (const entry of results) {
    const songNum = getSongForPage(entry.page);
    const item = document.createElement("button");
    item.className = "search-result-item";
    item.type = "button";
    item.dataset.page = String(entry.page);

    const label = document.createElement("span");
    label.className = "search-result-song";
    label.textContent = songNum > 0 ? `Canción ${songNum}` : `Página ${entry.page}`;

    const snippet = document.createElement("span");
    snippet.className = "search-result-snippet";
    const lowerText = normalizeText(entry.text);
    const matchIdx = lowerText.indexOf(normalizedQuery.split(/\s+/)[0]);
    const start = Math.max(0, matchIdx - 40);
    const raw = entry.text.slice(start, start + 160).replace(/\s+/g, " ");
    snippet.textContent = (start > 0 ? "…" : "") + raw;

    item.appendChild(label);
    item.appendChild(snippet);
    searchResults.appendChild(item);
  }
};

const setSearchMode = (active) => {
  navigationNumberpad.classList.toggle("is-searching", active);
  if (active) {
    searchInput.value = "";
    searchResults.innerHTML = "";
    searchInput.focus();
  }
};

const handleSearchInput = () => {
  const query = searchInput.value;
  if (!query.trim()) {
    searchResults.innerHTML = "";
    return;
  }
  const results = searchPages(query);
  renderSearchResults(results, query);
};

const turnSong = (direction, { keepOverlay = false } = {}) => {
  if (direction === 0 || state.totalSongs === 0) return;
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);

  if (currentSongIndex < 0) {
    if (direction > 0) {
      renderPage(state.songIndex[0].page);
      clearDraft();
      setOverlayVisible(keepOverlay);
    }
    return;
  }

  const nextIndex = clampSongIndex(currentSongIndex + direction);
  if (nextIndex === currentSongIndex) return;
  renderPage(state.songIndex[nextIndex].page);
  clearDraft();
  setOverlayVisible(keepOverlay);
};

const toggleFullscreen = async () => {
  if (!supportsFullscreen) return;

  if (canOfferPseudoFullscreen) {
    state.immersiveMode = !state.immersiveMode;
    setOverlayVisible(!state.immersiveMode);
    updateFullscreenButton();
    return;
  }

  try {
    if (isFullscreen()) {
      await exitFullscreen();
    } else {
      await requestFullscreen();
    }
  } catch (error) {
    console.error("No se pudo cambiar la pantalla completa", error);
  } finally {
    updateFullscreenButton();
  }
};

const registerServiceWorker = async () => {
  if (!("serviceWorker" in navigator) || !window.isSecureContext) return;
  try {
    await navigator.serviceWorker.register("/sw.js");
  } catch (error) {
    console.error("No se pudo registrar el service worker", error);
  }
};

const bindReaderEvents = () => {
  numberpadGrid.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-digit]");
    if (!button) return;
    appendDigit(button.dataset.digit);
  });

  clearButton.addEventListener("click", clearDraft);
  backspaceButton.addEventListener("click", backspaceDraft);
  goButton.addEventListener("click", goToDraftSong);

  prevPageButton.addEventListener("click", () => {
    turnSong(-1, { keepOverlay: true });
  });

  nextPageButton.addEventListener("click", () => {
    turnSong(1, { keepOverlay: true });
  });

  prevCornerButton.addEventListener("click", () => {
    goBackInHistory();
  });

  searchToggle.addEventListener("click", () => {
    setSearchMode(true);
  });

  searchBack.addEventListener("click", () => {
    setSearchMode(false);
  });

  searchInput.addEventListener("input", handleSearchInput);

  searchResults.addEventListener("click", (event) => {
    const item = event.target.closest(".search-result-item[data-page]");
    if (!item) return;
    const pageNum = Number.parseInt(item.dataset.page, 10);
    if (!Number.isFinite(pageNum)) return;
    setSearchMode(false);
    renderPage(pageNum);
    setOverlayVisible(false);
  });

  fullscreenButton.addEventListener("click", () => {
    toggleFullscreen().catch((error) => {
      console.error("No se pudo activar la pantalla completa", error);
    });
  });

  viewerShell.addEventListener("click", (event) => {
    if (Date.now() - state.lastTouchEndedAt < 450) return;
    if (event.target !== viewerShell && event.target !== pageImage) return;
    if (event.detail > 1) return;
    setOverlayVisible(!state.overlayVisible);
  });

  viewerShell.addEventListener("touchstart", (event) => {
    if (event.touches.length !== 1) {
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
    const elapsed = Date.now() - state.touchStart.time;
    state.lastTouchEndedAt = Date.now();
    state.touchStart = null;

    if (Math.abs(deltaX) > 48 && Math.abs(deltaX) > Math.abs(deltaY)) {
      event.preventDefault();
      turnSong(deltaX < 0 ? 1 : -1);
      return;
    }

    if (Math.abs(deltaX) < 14 && Math.abs(deltaY) < 14 && elapsed < 360) {
      event.preventDefault();
      setOverlayVisible(!state.overlayVisible);
    }
  }, { passive: false });

  window.addEventListener("keydown", (event) => {
    if (/^[0-9]$/.test(event.key)) {
      appendDigit(event.key);
      return;
    }

    if (event.key === "Backspace") {
      backspaceDraft();
      return;
    }

    if (event.key === "Escape") {
      clearDraft();
      return;
    }

    if (event.key === "Enter") {
      goToDraftSong();
      return;
    }

    if (event.key === "ArrowRight") {
      turnSong(1);
    }

    if (event.key === "ArrowLeft") {
      turnSong(-1);
    }
  });

  ["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
    document.addEventListener(eventName, updateFullscreenButton);
  });
};

const initReader = async () => {
  const manifestResponse = await fetch("/pages.json", { cache: "no-store" });
  const manifest = await manifestResponse.json();
  state.totalPages = manifest.totalPages;
  state.songIndex = [...manifest.songIndex].sort((left, right) => left.song - right.song);
  state.totalSongs = state.songIndex.length;
  renderDraft();
  renderStatus();
  state.immersiveMode = canOfferPseudoFullscreen && isStandaloneApp;
  setOverlayVisible(!state.immersiveMode);
  updateFullscreenButton();
  renderPage(1, { pushToHistory: false });
  loadSearchIndex();
};

clearInitialUrl();
registerServiceWorker();
bindReaderEvents();
initReader().catch((error) => {
  console.error("No se pudo iniciar el lector", error);
  setLoading(true, "No se pudo cargar Nuestro Coro.");
});
