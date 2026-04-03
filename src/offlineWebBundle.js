const OFFLINE_WEB_BUNDLE_VERSION = "140";

const OFFLINE_WEB_BUNDLE_ASSETS = {
  "app.js": require("../assets/offline-web/app.js"),
  "icon-192.png": require("../assets/offline-web/icon-192.png"),
  "icon-512.png": require("../assets/offline-web/icon-512.png"),
  "icon.png": require("../assets/offline-web/icon.png"),
  "index.html": require("../assets/offline-web/index.html"),
  "pages.json": require("../assets/offline-web/pages.json"),
  "search-index.json": require("../assets/offline-web/search-index.json"),
  "styles.css": require("../assets/offline-web/styles.css"),
};

module.exports = {
  OFFLINE_WEB_BUNDLE_VERSION,
  OFFLINE_WEB_BUNDLE_ASSETS,
};
