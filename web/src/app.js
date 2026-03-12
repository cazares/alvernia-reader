const pageImage = document.getElementById("page-image");
const loading = document.getElementById("loading");
const openModalButton = document.getElementById("open-modal");
const modal = document.getElementById("go-modal");
const form = document.getElementById("go-form");
const songInput = document.getElementById("song-number");

let totalPages = 1;
let currentPage = 1;
let touchStart = null;

const manifestResponse = await fetch("./pages.json");
const { totalPages: manifestTotalPages, songIndex } = await manifestResponse.json();
totalPages = manifestTotalPages;

const pageFileName = (pageNumber) => `./pages/page-${String(pageNumber).padStart(3, "0")}.jpg`;

const setLoading = (active) => {
  loading.classList.toggle("is-hidden", !active);
};

const preloadPage = (pageNumber) => {
  if (pageNumber < 1 || pageNumber > totalPages) return;
  const image = new Image();
  image.src = pageFileName(pageNumber);
};

const renderPage = (pageNumber) => {
  currentPage = Math.max(1, Math.min(pageNumber, totalPages));
  setLoading(true);
  pageImage.src = pageFileName(currentPage);
  pageImage.dataset.page = String(currentPage);
  preloadPage(currentPage + 1);
  preloadPage(currentPage - 1);
};

const findSongPage = (songNumber) => {
  if (songNumber <= 0) return 1;
  const exact = songIndex.find((entry) => entry.song === songNumber);
  if (exact) return exact.page;
  const next = songIndex.find((entry) => entry.song >= songNumber);
  return next ? next.page : totalPages;
};

const openModal = () => {
  songInput.value = "";
  modal.showModal();
  setTimeout(() => songInput.focus(), 20);
};

const closeModal = () => {
  if (modal.open) modal.close();
};

const goToSong = (songNumber) => {
  renderPage(findSongPage(songNumber));
  closeModal();
};

const turnPage = (direction) => {
  if (direction === 0) return;
  renderPage(currentPage + direction);
};

openModalButton.addEventListener("click", openModal);
pageImage.addEventListener("load", () => setLoading(false));
pageImage.addEventListener("dblclick", openModal);

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const songNumber = Number.parseInt(songInput.value, 10);
  if (!Number.isFinite(songNumber)) return;
  goToSong(songNumber);
});

modal.addEventListener("click", (event) => {
  const rect = modal.getBoundingClientRect();
  const inside = rect.top <= event.clientY && event.clientY <= rect.top + rect.height
    && rect.left <= event.clientX && event.clientX <= rect.left + rect.width;
  if (!inside) {
    closeModal();
  }
});

window.addEventListener("keydown", (event) => {
  if (event.key === "ArrowRight") turnPage(1);
  if (event.key === "ArrowLeft") turnPage(-1);
  if (event.key.toLowerCase() === "g") openModal();
});

pageImage.addEventListener("touchstart", (event) => {
  if (event.touches.length !== 1) {
    touchStart = null;
    return;
  }
  const touch = event.touches[0];
  touchStart = { x: touch.clientX, y: touch.clientY, time: Date.now() };
}, { passive: true });

pageImage.addEventListener("touchend", (event) => {
  if (!touchStart || event.changedTouches.length !== 1) return;
  const touch = event.changedTouches[0];
  const deltaX = touch.clientX - touchStart.x;
  const deltaY = touch.clientY - touchStart.y;
  const elapsed = Date.now() - touchStart.time;
  touchStart = null;

  if (Math.abs(deltaX) > 48 && Math.abs(deltaX) > Math.abs(deltaY)) {
    turnPage(deltaX < 0 ? 1 : -1);
    return;
  }

  if (elapsed < 220) {
    openModal();
  }
}, { passive: true });

renderPage(1);
