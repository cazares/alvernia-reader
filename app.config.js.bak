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
    name: "Nuestro Coro",
    slug: "alvernia-reader",
    version: baseVersion,
    runtimeVersion: baseVersion,
    newArchEnabled: false,
    platforms: ["ios", "android", "web"],
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
      bundleIdentifier: "com.cazares.alverniareader",
      buildNumber: String(buildNumber),
    },
    android: {
      ...(config.android || {}),
      package: "com.cazares.alverniareader",
      versionCode: buildNumber,
    },
    extra: {
      ...(config.extra || {}),
      eas: {
        projectId: "8f4aeff3-940f-4ec2-b82d-89b430f5c8be",
      },
    },
  };
};
