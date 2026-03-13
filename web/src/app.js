const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const overlayControls = document.getElementById("overlay-controls");
const installButton = document.getElementById("install-button");
const songStatus = document.getElementById("song-status");
const songDisplay = document.getElementById("song-display");
const numberpadGrid = document.getElementById("numberpad-grid");
const clearButton = document.getElementById("clear-button");
const backspaceButton = document.getElementById("backspace-button");
const goButton = document.getElementById("go-button");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const fullscreenButton = document.getElementById("fullscreen-button");

const state = {
  totalPages: 1,
  totalSongs: 0,
  currentPage: 1,
  songDraft: "",
  songIndex: [],
  deferredInstallPrompt: null,
  overlayVisible: true,
  immersiveMode: false,
  touchStart: null,
  lastTouchEndedAt: 0,
};

const initialUrl = new URL(window.location.href);
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
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

const manifestResponse = await fetch("./pages.json");
const manifest = await manifestResponse.json();
state.totalPages = manifest.totalPages;
state.songIndex = [...manifest.songIndex].sort((left, right) => left.song - right.song);
state.totalSongs = state.songIndex.length;

const pageFileName = (pageNumber) => `./pages/page-${String(pageNumber).padStart(3, "0")}.jpg`;
const clampPage = (pageNumber) => Math.max(1, Math.min(pageNumber, state.totalPages));
const clampSongIndex = (index) => Math.max(0, Math.min(index, state.totalSongs - 1));
const getFullscreenElement = () => document.fullscreenElement || document.webkitFullscreenElement || null;
const isFullscreen = () => Boolean(getFullscreenElement());

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

const preloadPage = (pageNumber) => {
  if (pageNumber < 1 || pageNumber > state.totalPages) return;
  const image = new Image();
  image.src = pageFileName(pageNumber);
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

const renderStatus = () => {
  songStatus.textContent = `Canción ${getCurrentSongNumber()}`;
  const currentSongIndex = findSongIndexAtOrBeforePage(state.currentPage);
  prevPageButton.disabled = currentSongIndex <= 0;
  nextPageButton.disabled = currentSongIndex >= state.totalSongs - 1;
};

const renderDraft = () => {
  songDisplay.value = state.songDraft;
};

const renderPage = (pageNumber) => {
  state.currentPage = clampPage(pageNumber);
  setLoading(true);
  pageImage.src = pageFileName(state.currentPage);
  pageImage.dataset.page = String(state.currentPage);
  renderStatus();
  preloadPage(state.currentPage + 1);
  preloadPage(state.currentPage - 1);
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

const updateInstallButton = () => {
  installButton.classList.toggle("is-hidden", isStandaloneApp);
};

const flashInstallButtonLabel = (label) => {
  if (!installButton) return;
  if (!installButton.dataset.defaultLabel) {
    installButton.dataset.defaultLabel = installButton.textContent;
  }
  installButton.textContent = label;
  window.setTimeout(() => {
    installButton.textContent = installButton.dataset.defaultLabel;
  }, 2800);
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
    await navigator.serviceWorker.register("./sw.js");
  } catch (error) {
    console.error("No se pudo registrar el service worker", error);
  }
};

const triggerInstall = async () => {
  if (state.deferredInstallPrompt) {
    state.deferredInstallPrompt.prompt();
    await state.deferredInstallPrompt.userChoice.catch(() => null);
    state.deferredInstallPrompt = null;
    updateInstallButton();
    return;
  }

  if (isIOS && !isStandaloneApp) {
    flashInstallButtonLabel("Safari > Compartir > Agregar");
    return;
  }

  flashInstallButtonLabel("Instálala desde el navegador");
};

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

fullscreenButton.addEventListener("click", () => {
  toggleFullscreen().catch((error) => {
    console.error("No se pudo activar la pantalla completa", error);
  });
});

installButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo iniciar la instalación", error);
  });
});

pageImage.addEventListener("load", () => {
  setLoading(false);
});

pageImage.addEventListener("error", () => {
  setLoading(true, "No se pudo cargar esta página.");
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

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstallPrompt = event;
  updateInstallButton();
});

window.addEventListener("appinstalled", () => {
  state.deferredInstallPrompt = null;
  installButton.classList.add("is-hidden");
});

["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
  document.addEventListener(eventName, updateFullscreenButton);
});

clearInitialUrl();
renderDraft();
renderStatus();
state.immersiveMode = canOfferPseudoFullscreen && isStandaloneApp;
setOverlayVisible(!state.immersiveMode);
updateFullscreenButton();
updateInstallButton();
renderPage(1);
registerServiceWorker();
