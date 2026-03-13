const installGate = document.getElementById("install-gate");
const installGateCopy = document.getElementById("install-gate-copy");
const installGateSteps = document.getElementById("install-gate-steps");
const installGateButton = document.getElementById("install-gate-button");
const installGateNote = document.getElementById("install-gate-note");
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
  installCompleted: false,
};

const initialUrl = new URL(window.location.href);
const userAgent = navigator.userAgent;
const isIOS = /iphone|ipad|ipod/i.test(userAgent);
const isSafari = /safari/i.test(userAgent) && !/crios|fxios|edgios|opr\/|opera|duckduckgo/i.test(userAgent);
const isStandaloneApp = window.matchMedia("(display-mode: standalone)").matches
  || window.matchMedia("(display-mode: fullscreen)").matches
  || window.navigator.standalone === true;
const shouldShowInstallGate = !isStandaloneApp;
const fullscreenTarget = document.documentElement;
const nativeFullscreenSupported = Boolean(
  document.fullscreenEnabled
    || document.webkitFullscreenEnabled
    || fullscreenTarget.requestFullscreen
    || fullscreenTarget.webkitRequestFullscreen,
);
const canOfferPseudoFullscreen = isIOS && isStandaloneApp;
const supportsFullscreen = nativeFullscreenSupported || canOfferPseudoFullscreen;

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

const setInstallGateVisible = (visible) => {
  installGate.classList.toggle("is-hidden", !visible);
  viewerShell.classList.toggle("is-hidden", visible);
  overlayControls.classList.toggle("is-hidden", visible || !state.overlayVisible);
};

const updateInstallGateUi = () => {
  if (state.installCompleted) {
    installGateCopy.textContent = "Listo. Ahora abre Signo Vivo desde el ícono nuevo en tu pantalla de inicio.";
    installGateSteps.innerHTML = [
      "Busca el ícono Signo Vivo en tu pantalla de inicio.",
      "Tócalo para abrir la app.",
      "A partir de ahí se abrirá directo en modo app.",
    ].map((step) => `<li>${step}</li>`).join("");
    installGateButton.textContent = "Listo";
    installGateButton.disabled = true;
    installGateNote.textContent = "Si no ves el ícono todavía, espera un momento y vuelve a mirar.";
    return;
  }

  if (state.deferredInstallPrompt) {
    installGateCopy.textContent = "Toca el botón azul y acepta la instalación para guardar Signo Vivo como app.";
    installGateSteps.innerHTML = [
      "Toca Instalar Signo Vivo.",
      "Acepta el aviso del navegador.",
      "Abre Signo Vivo desde el ícono nuevo.",
    ].map((step) => `<li>${step}</li>`).join("");
    installGateButton.textContent = "⬇ Instalar Signo Vivo";
    installGateButton.disabled = false;
    installGateNote.textContent = "Solo hay que hacerlo una vez.";
    return;
  }

  if (isIOS && isSafari) {
    installGateCopy.textContent = "Haz esto una sola vez en Safari para dejar Signo Vivo como app en tu pantalla de inicio.";
    installGateSteps.innerHTML = [
      "Toca Compartir en Safari.",
      "Toca Agregar a pantalla de inicio.",
      "Abre Signo Vivo desde el ícono nuevo.",
    ].map((step) => `<li>${step}</li>`).join("");
    installGateButton.textContent = "⬇ Instalar Signo Vivo";
    installGateButton.disabled = false;
    installGateNote.textContent = "Después ya no uses el navegador. Abre siempre el ícono Signo Vivo.";
    return;
  }

  if (isIOS && !isSafari) {
    installGateCopy.textContent = "Primero abre este enlace en Safari. Desde otras apps o navegadores no es tan fácil instalarlo bien.";
    installGateSteps.innerHTML = [
      "Abre este enlace en Safari.",
      "En Safari toca Compartir.",
      "Luego toca Agregar a pantalla de inicio.",
    ].map((step) => `<li>${step}</li>`).join("");
    installGateButton.textContent = "Abrir en Safari";
    installGateButton.disabled = false;
    installGateNote.textContent = "Safari es la forma más segura para tu coro.";
    return;
  }

  installGateCopy.textContent = "Instala Signo Vivo y luego ábrelo como app desde tu dispositivo.";
  installGateSteps.innerHTML = [
    "Toca Instalar Signo Vivo.",
    "Acepta el aviso del navegador.",
    "Abre Signo Vivo desde el ícono nuevo.",
  ].map((step) => `<li>${step}</li>`).join("");
  installGateButton.textContent = "⬇ Instalar Signo Vivo";
  installGateButton.disabled = false;
  installGateNote.textContent = "La idea es usar siempre la app instalada.";
};

const flashInstallGateButton = (label) => {
  if (!installGateButton.dataset.defaultLabel) {
    installGateButton.dataset.defaultLabel = installGateButton.textContent;
  }
  installGateButton.textContent = label;
  window.setTimeout(() => {
    if (state.installCompleted) return;
    installGateButton.textContent = installGateButton.dataset.defaultLabel;
  }, 2600);
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
  if (!shouldShowInstallGate) {
    overlayControls.classList.toggle("is-hidden", !visible);
  }
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
    updateInstallGateUi();
    return;
  }

  if (isIOS && isSafari) {
    flashInstallGateButton("Usa Compartir de Safari");
    return;
  }

  if (isIOS && !isSafari) {
    flashInstallGateButton("Primero ábrelo en Safari");
    return;
  }

  flashInstallGateButton("Instálala desde tu navegador");
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

  fullscreenButton.addEventListener("click", () => {
    toggleFullscreen().catch((error) => {
      console.error("No se pudo activar la pantalla completa", error);
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

  ["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
    document.addEventListener(eventName, updateFullscreenButton);
  });
};

const initReader = async () => {
  const manifestResponse = await fetch("./pages.json");
  const manifest = await manifestResponse.json();
  state.totalPages = manifest.totalPages;
  state.songIndex = [...manifest.songIndex].sort((left, right) => left.song - right.song);
  state.totalSongs = state.songIndex.length;
  renderDraft();
  renderStatus();
  state.immersiveMode = canOfferPseudoFullscreen && isStandaloneApp;
  setInstallGateVisible(false);
  setOverlayVisible(!state.immersiveMode);
  updateFullscreenButton();
  renderPage(1);
};

installGateButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo iniciar la instalación", error);
  });
});

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstallPrompt = event;
  updateInstallGateUi();
});

window.addEventListener("appinstalled", () => {
  state.deferredInstallPrompt = null;
  state.installCompleted = true;
  updateInstallGateUi();
});

clearInitialUrl();
updateInstallGateUi();
registerServiceWorker();

if (shouldShowInstallGate) {
  setInstallGateVisible(true);
} else {
  bindReaderEvents();
  initReader().catch((error) => {
    console.error("No se pudo iniciar el lector", error);
    setLoading(true, "No se pudo cargar Signo Vivo.");
  });
}
