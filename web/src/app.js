const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const launchScreen = document.getElementById("launch-screen");
const launchCopy = document.getElementById("launch-copy");
const launchSteps = document.getElementById("launch-steps");
const launchFullscreenButton = document.getElementById("launch-fullscreen");
const launchWindowButton = document.getElementById("launch-window");
const launchInstallButton = document.getElementById("launch-install");
const launchContinueButton = document.getElementById("launch-continue");
const topChrome = document.getElementById("top-chrome");
const bottomChrome = document.getElementById("bottom-chrome");
const pageStatus = document.getElementById("page-status");
const openModalButton = document.getElementById("open-modal");
const shareButton = document.getElementById("share-button");
const windowButton = document.getElementById("window-button");
const installButton = document.getElementById("install-button");
const fullscreenButton = document.getElementById("fullscreen-button");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const modal = document.getElementById("go-modal");
const form = document.getElementById("go-form");
const songInput = document.getElementById("song-number");
const installSheet = document.getElementById("install-sheet");
const installCopy = document.getElementById("install-copy");
const installSteps = document.getElementById("install-steps");
const confirmInstallButton = document.getElementById("confirm-install");
const dismissInstallButton = document.getElementById("dismiss-install");

const INSTALL_DISMISS_KEY = "alvernia-reader-install-dismissed";
const LAUNCH_SEEN_KEY = "alvernia-reader-launch-seen";
const DOUBLE_TAP_WINDOW_MS = 260;
const CHROME_HIDE_MS = 2400;
const WINDOW_MODE = "reader";

const state = {
  totalPages: 1,
  currentPage: 1,
  songIndex: [],
  deferredInstallPrompt: null,
  controlsVisible: false,
  chromeTimer: null,
  lastTap: null,
  lastTapTimer: null,
  touchStart: null,
  lastTouchEndedAt: 0,
  appReady: false,
  launchVisible: false,
};

const initialUrl = new URL(window.location.href);
const launchMode = initialUrl.searchParams.get("mode");
const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
const isStandalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
const supportsShare = typeof navigator.share === "function";
const supportsClipboard = typeof navigator.clipboard?.writeText === "function";
const supportsWindowOpen = typeof window.open === "function";
const fullscreenTarget = document.documentElement;
const supportsFullscreen = Boolean(
  document.fullscreenEnabled
    || document.webkitFullscreenEnabled
    || fullscreenTarget.requestFullscreen
    || fullscreenTarget.webkitRequestFullscreen
);

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

const setButtonBusy = (button, label, busy) => {
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

const currentShareUrl = ({ windowMode = false } = {}) => {
  const url = new URL(window.location.href);
  const song = findSongAtOrBeforePage(state.currentPage);

  url.search = "";

  if (song) {
    url.searchParams.set("song", String(song.song));
  } else {
    url.searchParams.set("page", String(state.currentPage));
  }

  if (windowMode) {
    url.searchParams.set("mode", WINDOW_MODE);
  }

  return url.toString();
};

const updateHistory = () => {
  const shareUrl = currentShareUrl();
  if (window.location.href !== shareUrl) {
    window.history.replaceState({ page: state.currentPage }, "", shareUrl);
  }
};

const scheduleChromeHide = () => {
  window.clearTimeout(state.chromeTimer);
  if (!state.controlsVisible || modal.open || installSheet.open || state.launchVisible) return;
  state.chromeTimer = window.setTimeout(() => {
    topChrome.classList.add("is-hidden");
    bottomChrome.classList.add("is-hidden");
    state.controlsVisible = false;
  }, CHROME_HIDE_MS);
};

const setChromeVisible = (visible) => {
  state.controlsVisible = visible;
  topChrome.classList.toggle("is-hidden", !visible);
  bottomChrome.classList.toggle("is-hidden", !visible);
  scheduleChromeHide();
};

const updateStatus = () => {
  const song = findSongAtOrBeforePage(state.currentPage);
  pageStatus.textContent = song
    ? `Cancion ${song.song} · Pagina ${state.currentPage}`
    : `Intro · Pagina ${state.currentPage}`;
  prevPageButton.disabled = state.currentPage <= 1;
  nextPageButton.disabled = state.currentPage >= state.totalPages;
};

const updateFullscreenUi = () => {
  const active = isFullscreen();
  fullscreenButton.classList.toggle("is-hidden", !supportsFullscreen);
  fullscreenButton.textContent = active ? "Salir de pantalla completa" : "Pantalla completa";
  launchFullscreenButton.textContent = supportsFullscreen
    ? active ? "Salir de pantalla completa" : "Pantalla completa"
    : "Abrir lector";
};

const updateWindowUi = () => {
  windowButton.classList.toggle("is-hidden", !supportsWindowOpen || isStandalone);
  launchWindowButton.classList.toggle("is-hidden", !supportsWindowOpen);
};

const setLaunchVisible = (visible, { remember = true } = {}) => {
  state.launchVisible = visible;
  launchScreen.classList.toggle("is-hidden", !visible);

  if (!visible && remember) {
    window.sessionStorage.setItem(LAUNCH_SEEN_KEY, "1");
    setChromeVisible(true);
  }
};

const shouldShowLaunchScreen = () => {
  if (isStandalone) return false;
  if (launchMode === WINDOW_MODE) return false;
  return window.sessionStorage.getItem(LAUNCH_SEEN_KEY) !== "1";
};

const updateLaunchUi = () => {
  const steps = [];
  const actionReady = state.appReady;

  if (!actionReady) {
    launchCopy.textContent = "Preparando el manual para abrirlo sin distracciones...";
    steps.push(
      "Cargando la portada y el indice de canciones.",
      "En cuanto termine, podras abrirlo en pantalla completa o en otra ventana.",
    );
  } else {
    if (supportsFullscreen) {
      launchCopy.textContent = "Empieza en pantalla completa con un toque, o abre otra ventana con el mismo punto del manual.";
      steps.push(
        "Pantalla completa oculta las barras del navegador cuando tu navegador lo permite.",
        "Ventana nueva abre otra ventana o pestaña con la misma cancion o pagina.",
        "Como instalar te deja el icono en tu pantalla de inicio para abrirlo como app.",
      );
    } else {
      launchCopy.textContent = "Tu navegador no siempre deja entrar a pantalla completa desde la web, pero puedes abrir otra ventana o instalar el icono para sentirlo mas nativo.";
      steps.push(
        "Ventana nueva abre otra ventana o pestaña con el mismo punto del manual.",
        "Como instalar deja un icono en tu pantalla de inicio.",
        "Seguir aqui abre el lector normal en esta misma pantalla.",
      );
    }
  }

  launchSteps.innerHTML = steps.map((step) => `<li>${step}</li>`).join("");
  launchFullscreenButton.disabled = !actionReady;
  launchWindowButton.disabled = !actionReady;
  launchContinueButton.disabled = !actionReady;
  launchInstallButton.disabled = false;
  updateFullscreenUi();
  updateWindowUi();
};

const renderPage = (pageNumber, { syncUrl = true } = {}) => {
  state.currentPage = clampPage(pageNumber);
  setLoading(true);
  pageImage.src = pageFileName(state.currentPage);
  pageImage.dataset.page = String(state.currentPage);
  updateStatus();
  preloadPage(state.currentPage + 1);
  preloadPage(state.currentPage + 2);
  preloadPage(state.currentPage - 1);

  if (syncUrl) {
    updateHistory();
  }
};

const openModal = () => {
  songInput.value = "";
  setChromeVisible(true);
  modal.showModal();
  window.setTimeout(() => songInput.focus(), 30);
};

const closeModal = () => {
  if (modal.open) modal.close();
  scheduleChromeHide();
};

const openInstallSheet = () => {
  setChromeVisible(true);
  updateInstallUi();
  if (!installSheet.open) {
    installSheet.showModal();
  }
};

const closeInstallSheet = () => {
  if (installSheet.open) {
    installSheet.close();
  }
  scheduleChromeHide();
};

const goToSong = (songNumber) => {
  renderPage(findSongPage(songNumber));
  closeModal();
};

const turnPage = (direction) => {
  if (direction === 0) return;
  renderPage(state.currentPage + direction);
};

const dismissInstall = () => {
  window.localStorage.setItem(INSTALL_DISMISS_KEY, "1");
  closeInstallSheet();
};

const installCopyText = () => {
  if (state.deferredInstallPrompt) {
    return "Toca Instalar y acepta el aviso del navegador para guardarla en tu pantalla de inicio.";
  }

  if (isIOS) {
    return "Haz esto una sola vez en Safari y luego abrira como si fuera una app nativa.";
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

  installButton.classList.toggle("is-hidden", !shouldOfferInstall);
  shareButton.classList.toggle("is-hidden", !(supportsShare || supportsClipboard));
  confirmInstallButton.classList.toggle("is-hidden", !state.deferredInstallPrompt);
  installCopy.textContent = installCopyText();
  installSteps.innerHTML = installStepItems().map((step) => `<li>${step}</li>`).join("");

  if (!dismissed && shouldOfferInstall && state.appReady && !state.launchVisible && !installSheet.open) {
    window.setTimeout(() => {
      if (
        window.localStorage.getItem(INSTALL_DISMISS_KEY) !== "1"
        && !modal.open
        && !installSheet.open
        && !state.launchVisible
      ) {
        installSheet.showModal();
      }
    }, 900);
  }
};

const shareCurrentLocation = async () => {
  const song = findSongAtOrBeforePage(state.currentPage);
  const text = song
    ? `Abrir Alvernia Reader en la cancion ${song.song}`
    : "Abrir Alvernia Reader";
  const url = currentShareUrl();

  if (supportsShare) {
    await navigator.share({
      title: "Alvernia Reader",
      text,
      url,
    });
    return;
  }

  if (supportsClipboard) {
    await navigator.clipboard.writeText(url);
    flashButtonLabel(shareButton, "Copiado");
  }
};

const openCurrentLocationInNewWindow = () => {
  if (!supportsWindowOpen) return false;

  const popup = window.open(
    currentShareUrl({ windowMode: true }),
    "alvernia-reader-window",
    "popup=yes,resizable=yes,toolbar=no,menubar=no,width=1024,height=1366",
  );

  if (!popup) {
    flashButtonLabel(windowButton, "Bloqueada");
    flashButtonLabel(launchWindowButton, "Bloqueada");
    return false;
  }

  popup.focus?.();
  return true;
};

const triggerInstall = async () => {
  if (!state.deferredInstallPrompt) {
    openInstallSheet();
    return;
  }

  state.deferredInstallPrompt.prompt();
  await state.deferredInstallPrompt.userChoice.catch(() => null);
  state.deferredInstallPrompt = null;
  closeInstallSheet();
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

const consumeTap = (clientX, clientY) => {
  const now = Date.now();

  if (
    state.lastTap
    && now - state.lastTap.time <= DOUBLE_TAP_WINDOW_MS
    && Math.abs(clientX - state.lastTap.x) < 28
    && Math.abs(clientY - state.lastTap.y) < 28
  ) {
    window.clearTimeout(state.lastTapTimer);
    state.lastTap = null;
    openModal();
    return;
  }

  state.lastTap = { time: now, x: clientX, y: clientY };
  window.clearTimeout(state.lastTapTimer);
  state.lastTapTimer = window.setTimeout(() => {
    state.lastTap = null;
  }, DOUBLE_TAP_WINDOW_MS + 40);

  setChromeVisible(!state.controlsVisible);
};

const toggleFullscreen = async ({ sourceButton = fullscreenButton } = {}) => {
  if (!supportsFullscreen) {
    return false;
  }

  try {
    setButtonBusy(sourceButton, "Abriendo...", true);
    if (isFullscreen()) {
      await exitFullscreen();
    } else {
      await requestFullscreen();
    }
    updateFullscreenUi();
    return true;
  } catch (error) {
    console.error("No se pudo cambiar la pantalla completa", error);
    flashButtonLabel(sourceButton, "No disponible");
    return false;
  } finally {
    setButtonBusy(sourceButton, "", false);
  }
};

const continueIntoReader = ({ remember = true } = {}) => {
  setLaunchVisible(false, { remember });
  updateInstallUi();
};

const handleLaunchFullscreen = async () => {
  if (!state.appReady) return;
  if (supportsFullscreen) {
    await toggleFullscreen({ sourceButton: launchFullscreenButton });
  }
  continueIntoReader();
};

const handleLaunchWindow = () => {
  if (!state.appReady) return;
  const opened = openCurrentLocationInNewWindow();
  if (opened) {
    continueIntoReader();
  }
};

openModalButton.addEventListener("click", openModal);
shareButton.addEventListener("click", () => {
  shareCurrentLocation().catch((error) => {
    console.error("No se pudo compartir", error);
  });
});
windowButton.addEventListener("click", () => {
  openCurrentLocationInNewWindow();
});
installButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo instalar", error);
  });
});
fullscreenButton.addEventListener("click", () => {
  toggleFullscreen({ sourceButton: fullscreenButton }).catch((error) => {
    console.error("No se pudo activar la pantalla completa", error);
  });
});
confirmInstallButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo instalar", error);
  });
});
dismissInstallButton.addEventListener("click", dismissInstall);
launchFullscreenButton.addEventListener("click", () => {
  handleLaunchFullscreen().catch((error) => {
    console.error("No se pudo abrir en pantalla completa", error);
  });
});
launchWindowButton.addEventListener("click", handleLaunchWindow);
launchInstallButton.addEventListener("click", openInstallSheet);
launchContinueButton.addEventListener("click", () => {
  continueIntoReader();
});
prevPageButton.addEventListener("click", () => {
  turnPage(-1);
  setChromeVisible(true);
});
nextPageButton.addEventListener("click", () => {
  turnPage(1);
  setChromeVisible(true);
});

pageImage.addEventListener("load", () => {
  setLoading(false);
  if (!state.appReady) {
    state.appReady = true;
    updateLaunchUi();

    if (shouldShowLaunchScreen()) {
      setLaunchVisible(true, { remember: false });
    } else {
      continueIntoReader({ remember: false });
    }
  }
});
pageImage.addEventListener("error", () => setLoading(true, "No se pudo cargar esta pagina."));

viewerShell.addEventListener("click", (event) => {
  if (Date.now() - state.lastTouchEndedAt < 450) return;
  if (event.target !== viewerShell && event.target !== pageImage) return;
  if (event.detail > 1) return;
  consumeTap(event.clientX, event.clientY);
});

viewerShell.addEventListener("dblclick", (event) => {
  event.preventDefault();
  openModal();
});

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const songNumber = Number.parseInt(songInput.value, 10);
  if (!Number.isFinite(songNumber)) return;
  goToSong(songNumber);
});

modal.addEventListener("click", (event) => {
  const rect = modal.getBoundingClientRect();
  const inside = rect.top <= event.clientY
    && event.clientY <= rect.top + rect.height
    && rect.left <= event.clientX
    && event.clientX <= rect.left + rect.width;
  if (!inside) {
    closeModal();
  }
});

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
  if (event.key === "ArrowRight") turnPage(1);
  if (event.key === "ArrowLeft") turnPage(-1);
  if (event.key.toLowerCase() === "g") openModal();
  if (event.key.toLowerCase() === "f") {
    toggleFullscreen({ sourceButton: fullscreenButton }).catch((error) => {
      console.error("No se pudo activar la pantalla completa", error);
    });
  }
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
    turnPage(deltaX < 0 ? 1 : -1);
    return;
  }

  if (Math.abs(deltaX) < 14 && Math.abs(deltaY) < 14 && elapsed < 360) {
    consumeTap(touch.clientX, touch.clientY);
  }
}, { passive: true });

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstallPrompt = event;
  updateInstallUi();
  updateLaunchUi();
});

window.addEventListener("appinstalled", () => {
  closeInstallSheet();
  state.deferredInstallPrompt = null;
  updateInstallUi();
  updateLaunchUi();
});

["fullscreenchange", "webkitfullscreenchange"].forEach((eventName) => {
  document.addEventListener(eventName, () => {
    updateFullscreenUi();
    setChromeVisible(true);
  });
});

const initialSong = Number.parseInt(initialUrl.searchParams.get("song") ?? "", 10);
const initialPage = Number.parseInt(initialUrl.searchParams.get("page") ?? "", 10);

state.launchVisible = shouldShowLaunchScreen();
launchScreen.classList.toggle("is-hidden", !state.launchVisible);
updateInstallUi();
updateLaunchUi();
updateStatus();
setChromeVisible(false);
renderPage(
  Number.isFinite(initialSong)
    ? findSongPage(initialSong)
    : Number.isFinite(initialPage)
      ? initialPage
      : 1,
  { syncUrl: false },
);
updateHistory();
registerServiceWorker();
