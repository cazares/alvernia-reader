const fs = require("fs");
const path = require("path");

const APP_ROOT = __dirname;
const VERSION_FILE = path.join(APP_ROOT, "version.json");
const DEFAULT_VERSION = {
  baseVersion: "1.0",
  buildNumber: 1,
};

const loadVersion = () => {
  try {
    const raw = fs.readFileSync(VERSION_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return {
      baseVersion: String(parsed?.baseVersion || DEFAULT_VERSION.baseVersion).trim(),
      buildNumber: Math.max(1, Number(parsed?.buildNumber || DEFAULT_VERSION.buildNumber) || 1),
    };
  } catch {
    return DEFAULT_VERSION;
  }
};

module.exports = ({ config }) => {
  const { baseVersion, buildNumber } = loadVersion();

  return {
    ...config,
    name: "SignoVivo",
    slug: "alvernia-reader",
    version: baseVersion,
    runtimeVersion: baseVersion,
    newArchEnabled: false,
    platforms: ["ios", "android"],
    icon: "./assets/icon.png",
    splash: {
      image: "./assets/splash.png",
      resizeMode: "contain",
      backgroundColor: "#000000",
    },
    updates: {
      enabled: false,
      checkAutomatically: "NEVER",
    },
    ios: {
      ...(config.ios || {}),
      supportsTablet: true,
      bundleIdentifier: "com.cazares.alvernia",
      buildNumber: String(buildNumber),
    },
    android: {
      ...(config.android || {}),
      package: "com.cazares.alvernia",
      versionCode: buildNumber,
    },
    extra: {
      ...(config.extra || {}),
      eas: {
        projectId: "8973a6b2-a2e5-4268-97ab-4a1b2c4cb555",
      },
    },
  };
};
