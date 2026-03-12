const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const topChrome = document.getElementById("top-chrome");
const bottomChrome = document.getElementById("bottom-chrome");
const jumpCta = document.getElementById("jump-cta");
const jumpForm = document.getElementById("jump-form");
const jumpInput = document.getElementById("jump-song");
const fullscreenGuard = document.getElementById("fullscreen-guard");
const resumeFullscreenButton = document.getElementById("resume-fullscreen");
const dismissFullscreenGuardButton = document.getElementById("dismiss-fullscreen-guard");
const pageStatus = document.getElementById("page-status");
const fullscreenButton = document.getElementById("fullscreen-button");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const installSheet = document.getElementById("install-sheet");
const installCopy = document.getElementById("install-copy");
const installSteps = document.getElementById("install-steps");
const confirmInstallButton = document.getElementById("confirm-install");
const dismissInstallButton = document.getElementById("dismiss-install");

const INSTALL_DISMISS_KEY = "alvernia-reader-install-dismissed";
const CHROME_HIDE_MS = 2400;

const state = {
  totalPages: 1,
  currentPage: 1,
  songIndex: [],
  deferredInstallPrompt: null,
  controlsVisible: false,
  chromeTimer: null,
  installTimer: null,
  stickyFullscreenWanted: false,
  userRequestedFullscreenExit: false,
  touchStart: null,
  lastTouchEndedAt: 0,
  appReady: false,
};

const initialUrl = new URL(window.location.href);
const initialSong = Number.parseInt(initialUrl.searchParams.get("song") ?? "", 10);
const initialPage = Number.parseInt(initialUrl.searchParams.get("page") ?? "", 10);
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
const isStandalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
const fullscreenTarget = document.documentElement;
const supportsFullscreen = Boolean(
  document.fullscreenEnabled
    || document.webkitFullscreenEnabled
    || fullscreenTarget.requestFullscreen
    || fullscreenTarget.webkitRequestFullscreen,
);

const getFullscreenElement = () => document.fullscreenElement || document.webkitFullscreenElement || null;
const isFullscreen = () => Boolean(getFullscreenElement());
const isEditingSong = () => document.activeElement === jumpInput;

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

const setButtonBusy = (button, label, busy) => {
  if (!button) return;
  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent;
  }
  button.textContent = busy ? label : button.dataset.defaultLabel;
};

const flashButtonLabel = (button, label) => {
  if (!button) return;
  const previous = button.textContent;
  button.textContent = label;
  window.setTimeout(() => {
    button.textContent = previous;
  }, 1400);
};

const manifestResponse = await fetch("./pages.json");
const manifest = await manifestResponse.json();
state.totalPages = manifest.totalPages;
state.songIndex = [...manifest.songIndex].sort((left, right) => left.page - right.page || left.song - right.song);

const pageFileName = (pageNumber) => `./pages/page-${String(pageNumber).padStart(3, "0")}.jpg`;
const clampPage = (pageNumber) => Math.max(1, Math.min(pageNumber, state.totalPages));

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

const findSongPage = (songNumber) => {
  if (songNumber <= 0) return 1;
  const exact = state.songIndex.find((entry) => entry.song === songNumber);
  if (exact) return exact.page;
  const next = state.songIndex.find((entry) => entry.song >= songNumber);
  return next ? next.page : state.totalPages;
};

const findSongAtOrBeforePage = (pageNumber) => {
  let match = null;
  for (const entry of state.songIndex) {
    if (entry.page > pageNumber) break;
    match = entry;
  }
  return match;
};

const clearInitialDeepLink = () => {
  if (!initialUrl.search) return;
  window.history.replaceState({ page: 1 }, "", initialUrl.pathname || "/");
};

const scheduleChromeHide = () => {
  window.clearTimeout(state.chromeTimer);
  if (!state.controlsVisible || installSheet.open || isEditingSong()) return;
  state.chromeTimer = window.setTimeout(() => {
    topChrome.classList.add("is-hidden");
    jumpCta.classList.add("is-hidden");
    bottomChrome.classList.add("is-hidden");
    state.controlsVisible = false;
  }, CHROME_HIDE_MS);
};

const setChromeVisible = (visible) => {
  window.clearTimeout(state.chromeTimer);
  state.controlsVisible = visible;
  topChrome.classList.toggle("is-hidden", !visible);
  jumpCta.classList.toggle("is-hidden", !visible);
  bottomChrome.classList.toggle("is-hidden", !visible);
  if (!visible) {
    jumpInput.blur();
    return;
  }
  scheduleChromeHide();
};

const setFullscreenGuardVisible = (visible) => {
  fullscreenGuard.classList.toggle("is-hidden", !visible);
};

const updateStatus = () => {
  const song = findSongAtOrBeforePage(state.currentPage);
  pageStatus.textContent = song
    ? `Cancion ${song.song} · Pagina ${state.currentPage}`
    : `Inicio · Pagina ${state.currentPage}`;
  prevPageButton.disabled = state.currentPage <= 1;
  nextPageButton.disabled = state.currentPage >= state.totalPages;
};

const updateFullscreenUi = () => {
  const active = isFullscreen();
  fullscreenButton.classList.toggle("is-hidden", !supportsFullscreen);
  fullscreenButton.textContent = active ? "Salir de pantalla completa" : "Pantalla completa";
};

const renderPage = (pageNumber) => {
  state.currentPage = clampPage(pageNumber);
  setLoading(true);
  pageImage.src = pageFileName(state.currentPage);
  pageImage.dataset.page = String(state.currentPage);
  updateStatus();
  preloadPage(state.currentPage + 1);
  preloadPage(state.currentPage + 2);
  preloadPage(state.currentPage - 1);
};

const sanitizeSongValue = (value) => value.replace(/\D+/g, "").slice(0, 4);

const goToSong = (songNumber) => {
  renderPage(findSongPage(songNumber));
  jumpInput.value = "";
  jumpInput.blur();
  if (isFullscreen()) {
    window.setTimeout(() => {
      setChromeVisible(false);
    }, 180);
    return;
  }
  scheduleChromeHide();
};

const turnPage = (direction) => {
  if (direction === 0) return;
  renderPage(state.currentPage + direction);
};

const dismissInstall = () => {
  window.localStorage.setItem(INSTALL_DISMISS_KEY, "1");
  if (installSheet.open) {
    installSheet.close();
  }
  scheduleChromeHide();
};

const installCopyText = () => {
  if (state.deferredInstallPrompt) {
    return "Toca Instalar y acepta el aviso del navegador para guardarla en tu pantalla de inicio.";
  }

  if (isIOS) {
    return "Haz esto una sola vez en Safari y luego abrira como una app facil de abrir.";
  }

  return "Abre este enlace en Safari o Chrome para instalar esta app en la pantalla de inicio.";
};

const installStepItems = () => {
  if (state.deferredInstallPrompt) {
    return [
      "Toca Instalar.",
      "Acepta el aviso del navegador.",
      "Abre Alvernia desde el icono nuevo en tu pantalla de inicio.",
    ];
  }

  if (isIOS) {
    return [
      "Abre este enlace en Safari.",
      "Toca Compartir en la barra del navegador.",
      "Toca Agregar a pantalla de inicio.",
    ];
  }

  return [
    "Abre este enlace en Safari o Chrome.",
    "Toca Instalar cuando el navegador lo ofrezca.",
    "Abre Alvernia desde el icono nuevo en tu pantalla de inicio.",
  ];
};

const updateInstallUi = () => {
  const dismissed = window.localStorage.getItem(INSTALL_DISMISS_KEY) === "1";
  const shouldOfferInstall = !isStandalone && (Boolean(state.deferredInstallPrompt) || isIOS);
  confirmInstallButton.classList.toggle("is-hidden", !state.deferredInstallPrompt);
  installCopy.textContent = installCopyText();
  installSteps.innerHTML = installStepItems().map((step) => `<li>${step}</li>`).join("");

  window.clearTimeout(state.installTimer);
  if (!dismissed && shouldOfferInstall && state.appReady && !installSheet.open && !isFullscreen()) {
    state.installTimer = window.setTimeout(() => {
      if (window.localStorage.getItem(INSTALL_DISMISS_KEY) === "1" || installSheet.open || isFullscreen()) {
        return;
      }
      installSheet.showModal();
    }, 1400);
  }
};

const disableStickyFullscreen = () => {
  state.stickyFullscreenWanted = false;
  state.userRequestedFullscreenExit = true;
  setFullscreenGuardVisible(false);
};

const triggerInstall = async () => {
  if (!state.deferredInstallPrompt) {
    updateInstallUi();
    if (!installSheet.open) {
      installSheet.showModal();
    }
    return;
  }

  state.deferredInstallPrompt.prompt();
  await state.deferredInstallPrompt.userChoice.catch(() => null);
  state.deferredInstallPrompt = null;
  if (installSheet.open) {
    installSheet.close();
  }
  updateInstallUi();
};

const registerServiceWorker = async () => {
  if (!("serviceWorker" in navigator) || !window.isSecureContext) return;
  try {
    await navigator.serviceWorker.register("./sw.js");
  } catch (error) {
    console.error("No se pudo registrar el service worker", error);
  }
};

const consumeTap = () => {
  setChromeVisible(!state.controlsVisible);
};

const toggleFullscreen = async ({ sourceButton = fullscreenButton, quiet = false } = {}) => {
  if (!supportsFullscreen) {
    return false;
  }

  try {
    setButtonBusy(sourceButton, "Abriendo...", true);
    state.stickyFullscreenWanted = true;
    state.userRequestedFullscreenExit = false;
    setFullscreenGuardVisible(false);
    await requestFullscreen();
    updateFullscreenUi();
    return true;
  } catch (error) {
    console.error("No se pudo cambiar la pantalla completa", error);
    if (!quiet) {
      flashButtonLabel(sourceButton, "No disponible");
    }
    if (state.stickyFullscreenWanted && !state.userRequestedFullscreenExit) {
      setFullscreenGuardVisible(true);
    }
    return false;
  } finally {
    setButtonBusy(sourceButton, "", false);
  }
};

const exitFullscreenByChoice = async ({ sourceButton = fullscreenButton } = {}) => {
  disableStickyFullscreen();
  if (!isFullscreen()) {
    setChromeVisible(true);
    updateFullscreenUi();
    return true;
  }

  try {
    setButtonBusy(sourceButton, "Saliendo...", true);
    await exitFullscreen();
    updateFullscreenUi();
    setChromeVisible(true);
    return true;
  } catch (error) {
    console.error("No se pudo salir de pantalla completa", error);
    flashButtonLabel(sourceButton, "No disponible");
    return false;
  } finally {
    setButtonBusy(sourceButton, "", false);
  }
};

const recoverFullscreen = () => {
  if (!state.stickyFullscreenWanted || state.userRequestedFullscreenExit || !supportsFullscreen) {
    return;
  }

  setFullscreenGuardVisible(true);
  window.setTimeout(() => {
    toggleFullscreen({ sourceButton: resumeFullscreenButton, quiet: true }).catch(() => {});
  }, 80);
};

const handleFullscreenButton = async () => {
  if (isFullscreen()) {
    await exitFullscreenByChoice({ sourceButton: fullscreenButton });
    return;
  }

  await toggleFullscreen({ sourceButton: fullscreenButton });
};

fullscreenButton.addEventListener("click", () => {
  handleFullscreenButton().catch((error) => {
    console.error("No se pudo activar la pantalla completa", error);
  });
});

resumeFullscreenButton.addEventListener("click", () => {
  state.userRequestedFullscreenExit = false;
  state.stickyFullscreenWanted = true;
  toggleFullscreen({ sourceButton: resumeFullscreenButton }).catch((error) => {
    console.error("No se pudo recuperar la pantalla completa", error);
  });
});

dismissFullscreenGuardButton.addEventListener("click", () => {
  disableStickyFullscreen();
  setChromeVisible(true);
});

confirmInstallButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo instalar", error);
  });
});

dismissInstallButton.addEventListener("click", dismissInstall);

prevPageButton.addEventListener("click", () => {
  turnPage(-1);
  setChromeVisible(true);
});

nextPageButton.addEventListener("click", () => {
  turnPage(1);
  setChromeVisible(true);
});

jumpInput.addEventListener("input", () => {
  const sanitized = sanitizeSongValue(jumpInput.value);
  if (jumpInput.value !== sanitized) {
    jumpInput.value = sanitized;
  }
});

jumpInput.addEventListener("focus", () => {
  setChromeVisible(true);
});

jumpInput.addEventListener("blur", () => {
  scheduleChromeHide();
});

jumpForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const songNumber = Number.parseInt(jumpInput.value, 10);
  if (!Number.isFinite(songNumber)) {
    jumpInput.focus();
    return;
  }
  goToSong(songNumber);
});

pageImage.addEventListener("load", () => {
  setLoading(false);
  if (!state.appReady) {
    state.appReady = true;
    updateInstallUi();
    setChromeVisible(true);
  }
});

pageImage.addEventListener("error", () => {
  setLoading(true, "No se pudo cargar esta pagina.");
});

viewerShell.addEventListener("click", (event) => {
  if (Date.now() - state.lastTouchEndedAt < 450) return;
  if (event.target !== viewerShell && event.target !== pageImage) return;
  if (event.detail > 1) return;
  consumeTap();
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
    consumeTap();
  }
}, { passive: false });

installSheet.addEventListener("click", (event) => {
  const rect = installSheet.getBoundingClientRect();
  const inside = rect.top <= event.clientY
    && event.clientY <= rect.top + rect.height
    && rect.left <= event.clientX
    && event.clientX <= rect.left + rect.width;
  if (!inside) {
    dismissInstall();
  }
});

window.addEventListener("keydown", (event) => {
  if (event.key === "ArrowRight") {
    turnPage(1);
  }
  if (event.key === "ArrowLeft") {
    turnPage(-1);
  }
  if (event.key.toLowerCase() === "f") {
    handleFullscreenButton().catch((error) => {
      console.error("No se pudo activar la pantalla completa", error);
    });
  }
  if (event.key.toLowerCase() === "g") {
    setChromeVisible(true);
    jumpInput.focus();
  }
});

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstallPrompt = event;
  updateInstallUi();
});

window.addEventListener("appinstalled", () => {
  if (installSheet.open) {
    installSheet.close();
  }
  state.deferredInstallPrompt = null;
  updateInstallUi();
});

["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
  document.addEventListener(eventName, () => {
    const active = isFullscreen();
    updateFullscreenUi();

    if (active) {
      state.userRequestedFullscreenExit = false;
      setFullscreenGuardVisible(false);
      window.setTimeout(() => {
        if (!isEditingSong()) {
          setChromeVisible(false);
        }
      }, 700);
      return;
    }

    if (state.stickyFullscreenWanted && !state.userRequestedFullscreenExit) {
      setChromeVisible(false);
      recoverFullscreen();
      return;
    }

    setFullscreenGuardVisible(false);
    setChromeVisible(true);
  });
});

updateInstallUi();
updateStatus();
updateFullscreenUi();
setChromeVisible(false);
renderPage(Number.isFinite(initialSong) ? findSongPage(initialSong) : Number.isFinite(initialPage) ? initialPage : 1);
clearInitialDeepLink();
registerServiceWorker();
