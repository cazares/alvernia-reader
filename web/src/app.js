const viewerShell = document.getElementById("viewer-shell");
const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const topChrome = document.getElementById("top-chrome");
const bottomChrome = document.getElementById("bottom-chrome");
const pageStatus = document.getElementById("page-status");
const openModalButton = document.getElementById("open-modal");
const shareButton = document.getElementById("share-button");
const installButton = document.getElementById("install-button");
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
const DOUBLE_TAP_WINDOW_MS = 260;
const CHROME_HIDE_MS = 2400;

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
};

const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
const isStandalone = window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
const supportsShare = typeof navigator.share === "function";
const supportsClipboard = typeof navigator.clipboard?.writeText === "function";

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

const currentShareUrl = () => {
  const url = new URL(window.location.href);
  const song = findSongAtOrBeforePage(state.currentPage);
  url.search = "";

  if (song) {
    url.searchParams.set("song", String(song.song));
  } else {
    url.searchParams.set("page", String(state.currentPage));
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
  if (!state.controlsVisible || modal.open || installSheet.open) return;
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
  if (installSheet.open) {
    installSheet.close();
  }
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
  installSteps.innerHTML = installStepItems()
    .map((step) => `<li>${step}</li>`)
    .join("");

  if (!dismissed && shouldOfferInstall && !installSheet.open) {
    window.setTimeout(() => {
      if (
        window.localStorage.getItem(INSTALL_DISMISS_KEY) !== "1"
        && !modal.open
        && !installSheet.open
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
    shareButton.textContent = "Copiado";
    window.setTimeout(() => {
      shareButton.textContent = "Compartir";
    }, 1600);
  }
};

const triggerInstall = async () => {
  if (!state.deferredInstallPrompt) {
    installSheet.showModal();
    return;
  }

  state.deferredInstallPrompt.prompt();
  await state.deferredInstallPrompt.userChoice.catch(() => null);
  state.deferredInstallPrompt = null;
  if (installSheet.open) installSheet.close();
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

openModalButton.addEventListener("click", openModal);
shareButton.addEventListener("click", () => {
  shareCurrentLocation().catch((error) => {
    console.error("No se pudo compartir", error);
  });
});
installButton.addEventListener("click", () => {
  triggerInstall().catch((error) => {
    console.error("No se pudo instalar", error);
  });
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

pageImage.addEventListener("load", () => setLoading(false));
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
});

window.addEventListener("appinstalled", () => {
  if (installSheet.open) installSheet.close();
  state.deferredInstallPrompt = null;
  updateInstallUi();
});

const initialUrl = new URL(window.location.href);
const initialSong = Number.parseInt(initialUrl.searchParams.get("song") ?? "", 10);
const initialPage = Number.parseInt(initialUrl.searchParams.get("page") ?? "", 10);

updateInstallUi();
updateStatus();
setChromeVisible(true);
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
