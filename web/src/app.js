const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const overlayControls = document.getElementById("overlay-controls");
const pageStatus = document.getElementById("page-status");
const pageDisplay = document.getElementById("page-display");
const numberpadGrid = document.getElementById("numberpad-grid");
const clearButton = document.getElementById("clear-button");
const backspaceButton = document.getElementById("backspace-button");
const goButton = document.getElementById("go-button");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const fullscreenButton = document.getElementById("fullscreen-button");

const state = {
  totalPages: 1,
  currentPage: 1,
  pageDraft: "",
  overlayVisible: true,
  touchStart: null,
  lastTouchEndedAt: 0,
};

const initialUrl = new URL(window.location.href);
const fullscreenTarget = document.documentElement;
const supportsFullscreen = Boolean(
  document.fullscreenEnabled
    || document.webkitFullscreenEnabled
    || fullscreenTarget.requestFullscreen
    || fullscreenTarget.webkitRequestFullscreen,
);

const manifestResponse = await fetch("./pages.json");
const manifest = await manifestResponse.json();
state.totalPages = manifest.totalPages;

const pageFileName = (pageNumber) => `./pages/page-${String(pageNumber).padStart(3, "0")}.jpg`;
const clampPage = (pageNumber) => Math.max(1, Math.min(pageNumber, state.totalPages));
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

const renderStatus = () => {
  pageStatus.textContent = `Pagina ${state.currentPage} de ${state.totalPages}`;
  prevPageButton.disabled = state.currentPage <= 1;
  nextPageButton.disabled = state.currentPage >= state.totalPages;
};

const renderDraft = () => {
  pageDisplay.value = state.pageDraft;
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
  fullscreenButton.textContent = isFullscreen() ? "⛶ Salir de pantalla completa" : "⛶ Pantalla completa";
};

const appendDigit = (digit) => {
  if (state.pageDraft.length >= 4) return;
  state.pageDraft = `${state.pageDraft}${digit}`;
  renderDraft();
};

const clearDraft = () => {
  state.pageDraft = "";
  renderDraft();
};

const backspaceDraft = () => {
  state.pageDraft = state.pageDraft.slice(0, -1);
  renderDraft();
};

const goToDraftPage = () => {
  const pageNumber = Number.parseInt(state.pageDraft, 10);
  if (!Number.isFinite(pageNumber)) return;
  renderPage(pageNumber);
  clearDraft();
  setOverlayVisible(false);
};

const turnPage = (direction) => {
  if (direction === 0) return;
  renderPage(state.currentPage + direction);
  clearDraft();
  setOverlayVisible(false);
};

const toggleFullscreen = async () => {
  if (!supportsFullscreen) return;

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

numberpadGrid.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-digit]");
  if (!button) return;
  appendDigit(button.dataset.digit);
});

clearButton.addEventListener("click", clearDraft);
backspaceButton.addEventListener("click", backspaceDraft);
goButton.addEventListener("click", goToDraftPage);

prevPageButton.addEventListener("click", () => {
  turnPage(-1);
});

nextPageButton.addEventListener("click", () => {
  turnPage(1);
});

fullscreenButton.addEventListener("click", () => {
  toggleFullscreen().catch((error) => {
    console.error("No se pudo activar la pantalla completa", error);
  });
});

pageImage.addEventListener("load", () => {
  setLoading(false);
});

pageImage.addEventListener("error", () => {
  setLoading(true, "No se pudo cargar esta pagina.");
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
    turnPage(deltaX < 0 ? 1 : -1);
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
    goToDraftPage();
    return;
  }

  if (event.key === "ArrowRight") {
    turnPage(1);
  }

  if (event.key === "ArrowLeft") {
    turnPage(-1);
  }
});

["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
  document.addEventListener(eventName, updateFullscreenButton);
});

clearInitialUrl();
renderDraft();
renderStatus();
setOverlayVisible(true);
updateFullscreenButton();
renderPage(1);
registerServiceWorker();
