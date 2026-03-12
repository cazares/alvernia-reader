const fs = require("fs");
const path = require("path");

const APP_ROOT = __dirname;
const VERSION_FILE = path.join(APP_ROOT, "version.json");
const IOS_BUNDLE_ID = "com.cazares.mixterious";
const ANDROID_PACKAGE = "com.cazares.mixterious";
const ASSETS_DIR = path.join(APP_ROOT, "assets");
const IOS_ASSETS_DIR = path.join(APP_ROOT, "ios", "Mixterious", "Images.xcassets");

const resolveFirstExistingAsset = (candidates) => {
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      const relative = path.relative(APP_ROOT, candidate).split(path.sep).join("/");
      return relative.startsWith(".") ? relative : `./${relative}`;
    }
  }
  return "";
};

const ICON_RELATIVE_PATH = resolveFirstExistingAsset([
  path.join(ASSETS_DIR, "icon.png"),
  path.join(IOS_ASSETS_DIR, "AppIcon.appiconset", "AppIcon.png"),
]);
const ADAPTIVE_ICON_RELATIVE_PATH = resolveFirstExistingAsset([
  path.join(ASSETS_DIR, "adaptive-icon.png"),
  path.join(IOS_ASSETS_DIR, "AppIcon.appiconset", "AppIcon.png"),
]);
const SPLASH_RELATIVE_PATH = resolveFirstExistingAsset([
  path.join(ASSETS_DIR, "splash.png"),
  path.join(IOS_ASSETS_DIR, "SplashScreen.imageset", "SplashScreen.png"),
]);
const FAVICON_RELATIVE_PATH = resolveFirstExistingAsset([
  path.join(ASSETS_DIR, "favicon.png"),
  path.join(IOS_ASSETS_DIR, "AppIcon.appiconset", "AppIcon.png"),
]);
const NOTIFICATION_ICON_RELATIVE_PATH = resolveFirstExistingAsset([
  path.join(APP_ROOT, "icons", "notification-icon.png"),
  path.join(ASSETS_DIR, "notification-icon.png"),
  path.join(IOS_ASSETS_DIR, "AppIcon.appiconset", "AppIcon.png"),
]);
const DEFAULT_NOTIFICATION_COLOR = "#4a90e2";
const EFFECTIVE_NOTIFICATION_ICON_RELATIVE_PATH = NOTIFICATION_ICON_RELATIVE_PATH || ICON_RELATIVE_PATH;

const REQUIRED_ASSET_PATHS = [ICON_RELATIVE_PATH, ADAPTIVE_ICON_RELATIVE_PATH, SPLASH_RELATIVE_PATH, FAVICON_RELATIVE_PATH];

const DEFAULT_VERSION = {
  baseVersion: "1.0",
  buildNumber: 1,
};
const DEFAULT_OTA_CHANNEL = "production";
const OTA_STRICT_BUILD_PROFILES = new Set(["preview", "production"]);

const resolveOptionalString = (value) => {
  const text = String(value ?? "").trim();
  return text || "";
};

const parseBooleanEnv = (value, fallback = true) => {
  const text = resolveOptionalString(value).toLowerCase();
  if (!text) return fallback;
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
};

const resolveRuntimeVersion = (configuredRuntimeVersion, fallbackVersion) => {
  const runtimeFromEnv = resolveOptionalString(process.env.EXPO_RUNTIME_VERSION);
  if (runtimeFromEnv) return runtimeFromEnv;

  if (typeof configuredRuntimeVersion === "string" || typeof configuredRuntimeVersion === "number") {
    const runtimeFromConfig = resolveOptionalString(configuredRuntimeVersion);
    if (runtimeFromConfig) return runtimeFromConfig;
  }

  return resolveOptionalString(fallbackVersion) || DEFAULT_VERSION.baseVersion;
};

const loadVersion = () => {
  try {
    const raw = fs.readFileSync(VERSION_FILE, "utf8");
    const parsed = JSON.parse(raw);
    const baseVersion = String(parsed?.baseVersion || DEFAULT_VERSION.baseVersion).trim();
    const buildNumber = Math.max(1, Number(parsed?.buildNumber || DEFAULT_VERSION.buildNumber) || 1);
    return { baseVersion, buildNumber };
  } catch {
    return DEFAULT_VERSION;
  }
};

const assertRequiredAssets = () => {
  const missing = REQUIRED_ASSET_PATHS.filter((assetPath) => !assetPath);
  if (!missing.length) return;
  throw new Error("Missing required app assets (icon/adaptive icon/splash/favicon).");
};

const withRequiredPlugins = (plugins = []) => {
  let hasNotifications = false;
  let hasSecureStore = false;
  let hasExpoVideo = false;

  const nextPlugins = plugins.map((plugin) => {
    if (plugin === "expo-secure-store") {
      hasSecureStore = true;
      return plugin;
    }

    if (plugin === "expo-notifications") {
      hasNotifications = true;
      return [
        "expo-notifications",
        {
          icon: EFFECTIVE_NOTIFICATION_ICON_RELATIVE_PATH,
          color: DEFAULT_NOTIFICATION_COLOR,
          sounds: [],
        },
      ];
    }

    if (plugin === "expo-video") {
      hasExpoVideo = true;
      return ["expo-video", { supportsBackgroundPlayback: true }];
    }

    if (Array.isArray(plugin) && plugin[0] === "expo-secure-store") {
      hasSecureStore = true;
      return plugin;
    }

    if (Array.isArray(plugin) && plugin[0] === "expo-notifications") {
      hasNotifications = true;
      const options = plugin[1] && typeof plugin[1] === "object" ? plugin[1] : {};
      return [
        "expo-notifications",
        {
          ...options,
          icon: options.icon || EFFECTIVE_NOTIFICATION_ICON_RELATIVE_PATH,
          color: options.color || DEFAULT_NOTIFICATION_COLOR,
          sounds: Array.isArray(options.sounds) ? options.sounds : [],
        },
      ];
    }

    if (Array.isArray(plugin) && plugin[0] === "expo-video") {
      hasExpoVideo = true;
      const options = plugin[1] && typeof plugin[1] === "object" ? plugin[1] : {};
      return ["expo-video", { ...options, supportsBackgroundPlayback: true }];
    }

    return plugin;
  });

  if (!hasNotifications) {
    nextPlugins.push([
      "expo-notifications",
      {
        icon: EFFECTIVE_NOTIFICATION_ICON_RELATIVE_PATH,
        color: DEFAULT_NOTIFICATION_COLOR,
        sounds: [],
      },
    ]);
  }

  if (!hasSecureStore) {
    nextPlugins.push("expo-secure-store");
  }

  if (!hasExpoVideo) {
    nextPlugins.push(["expo-video", { supportsBackgroundPlayback: true }]);
  }

  return nextPlugins;
};

module.exports = ({ config }) => {
  assertRequiredAssets();
  const { baseVersion, buildNumber } = loadVersion();
  const existingInfoPlist = (config.ios && config.ios.infoPlist) || {};
  const currentBackgroundModes = Array.isArray(existingInfoPlist.UIBackgroundModes)
    ? existingInfoPlist.UIBackgroundModes
    : [];
  const existingExtra = config.extra && typeof config.extra === "object" ? config.extra : {};
  const existingEas = existingExtra.eas && typeof existingExtra.eas === "object" ? existingExtra.eas : {};
  const existingOta = existingExtra.ota && typeof existingExtra.ota === "object" ? existingExtra.ota : {};
  const existingUpdates = config.updates && typeof config.updates === "object" ? config.updates : {};
  const existingAndroidConfig = config.android && typeof config.android === "object" ? { ...config.android } : {};
  // Keep Android media permissions available for Play submission/device save behavior.
  if ("blockedPermissions" in existingAndroidConfig) {
    delete existingAndroidConfig.blockedPermissions;
  }
  const existingAndroidPermissions = Array.isArray(existingAndroidConfig.permissions)
    ? existingAndroidConfig.permissions.filter((permission) => resolveOptionalString(permission))
    : [];
  const easProjectId =
    resolveOptionalString(process.env.EXPO_PUBLIC_EAS_PROJECT_ID) ||
    resolveOptionalString(process.env.EAS_PROJECT_ID) ||
    resolveOptionalString(existingEas.projectId);
  const otaChannel =
    resolveOptionalString(process.env.EXPO_PUBLIC_OTA_CHANNEL) ||
    resolveOptionalString(existingOta.channel) ||
    DEFAULT_OTA_CHANNEL;
  const otaEnabled = parseBooleanEnv(process.env.EXPO_PUBLIC_OTA_ENABLED, true);
  const otaAutoCheckOnForeground = parseBooleanEnv(process.env.EXPO_PUBLIC_OTA_AUTO_CHECK, true);
  const easBuildProfile = resolveOptionalString(process.env.EAS_BUILD_PROFILE).toLowerCase();
  const shouldRequireProjectId = OTA_STRICT_BUILD_PROFILES.has(easBuildProfile) && otaEnabled;
  if (shouldRequireProjectId && !easProjectId) {
    throw new Error(
      `Missing EXPO_PUBLIC_EAS_PROJECT_ID for EAS build profile "${easBuildProfile}". ` +
        "Set EXPO_PUBLIC_EAS_PROJECT_ID before running preview/production builds."
    );
  }
  const updatesUrl =
    resolveOptionalString(process.env.EXPO_UPDATES_URL) ||
    resolveOptionalString(existingUpdates.url) ||
    (easProjectId ? `https://u.expo.dev/${easProjectId}` : "");
  const updatesEnabled = otaEnabled && Boolean(updatesUrl);
  const runtimeVersion = resolveRuntimeVersion(config.runtimeVersion, baseVersion);
  // App Store versioning:
  // - `version` maps to CFBundleShortVersionString (should be `n.n.n` style)
  // - `ios.buildNumber` is the monotonically increasing build number
  // We keep both in `extra` for displaying inside the app if desired.
  const appVersion = baseVersion;

  return {
    ...config,
    version: baseVersion,
    runtimeVersion,
    updates: {
      ...existingUpdates,
      ...(updatesUrl ? { url: updatesUrl } : {}),
      enabled: updatesEnabled,
      checkAutomatically: existingUpdates.checkAutomatically || "ON_LOAD",
      fallbackToCacheTimeout:
        typeof existingUpdates.fallbackToCacheTimeout === "number"
          ? existingUpdates.fallbackToCacheTimeout
          : 0,
    },
    icon: ICON_RELATIVE_PATH,
    splash: {
      ...(config.splash || {}),
      image: SPLASH_RELATIVE_PATH,
      resizeMode: (config.splash && config.splash.resizeMode) || "contain",
      backgroundColor: (config.splash && config.splash.backgroundColor) || "#000000",
    },
    notification: {
      ...(config.notification || {}),
      icon: (config.notification && config.notification.icon) || EFFECTIVE_NOTIFICATION_ICON_RELATIVE_PATH,
      color: (config.notification && config.notification.color) || DEFAULT_NOTIFICATION_COLOR,
    },
    plugins: withRequiredPlugins(config.plugins || []),
    ios: {
      ...(config.ios || {}),
      bundleIdentifier: IOS_BUNDLE_ID,
      buildNumber: String(buildNumber),
      icon: (config.ios && config.ios.icon) || ICON_RELATIVE_PATH,
      infoPlist: {
        ...existingInfoPlist,
        NSPhotoLibraryAddUsageDescription:
          existingInfoPlist.NSPhotoLibraryAddUsageDescription ||
          "Mixterious saves karaoke videos you create to your Photos library when you tap Save.",
        NSPhotoLibraryUsageDescription:
          existingInfoPlist.NSPhotoLibraryUsageDescription ||
          "Allow Mixterious to access your Photos library for saving karaoke videos.",
        NSUserNotificationsUsageDescription:
          existingInfoPlist.NSUserNotificationsUsageDescription ||
          "Mixterious can send optional notifications when background karaoke processing finishes, if you enable Background Notifications.",
        ITSAppUsesNonExemptEncryption:
          typeof existingInfoPlist.ITSAppUsesNonExemptEncryption === "boolean"
            ? existingInfoPlist.ITSAppUsesNonExemptEncryption
            : false,
        UIBackgroundModes: Array.from(new Set([...currentBackgroundModes, "audio"])),
      },
    },
    android: {
      ...existingAndroidConfig,
      package: existingAndroidConfig.package || ANDROID_PACKAGE,
      versionCode: buildNumber,
      permissions: Array.from(
        new Set([
          ...existingAndroidPermissions,
          "android.permission.FOREGROUND_SERVICE",
          "android.permission.FOREGROUND_SERVICE_MEDIA_PLAYBACK",
          "android.permission.WAKE_LOCK",
        ])
      ),
      adaptiveIcon: {
        ...(existingAndroidConfig.adaptiveIcon || {}),
        foregroundImage:
          (existingAndroidConfig.adaptiveIcon && existingAndroidConfig.adaptiveIcon.foregroundImage) ||
          ADAPTIVE_ICON_RELATIVE_PATH,
        backgroundColor:
          (existingAndroidConfig.adaptiveIcon && existingAndroidConfig.adaptiveIcon.backgroundColor) || "#000000",
      },
    },
    web: {
      ...(config.web || {}),
      favicon: (config.web && config.web.favicon) || FAVICON_RELATIVE_PATH,
    },
    extra: {
      ...existingExtra,
      eas: {
        ...existingEas,
        ...(easProjectId ? { projectId: easProjectId } : {}),
      },
      ota: {
        ...existingOta,
        channel: otaChannel,
        enabled: updatesEnabled,
        autoCheckOnForeground: otaAutoCheckOnForeground,
      },
      appVersion,
      appBuildNumber: buildNumber,
      apiBaseUrl: process.env.EXPO_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000",
      privacyPolicyUrl:
        process.env.EXPO_PUBLIC_PRIVACY_POLICY_URL ||
        existingExtra.privacyPolicyUrl ||
        "https://mixterious.co/privacy",
      termsOfUseUrl:
        process.env.EXPO_PUBLIC_TERMS_OF_USE_URL ||
        existingExtra.termsOfUseUrl ||
        "https://mixterious.co/terms",
      supportUrl:
        process.env.EXPO_PUBLIC_SUPPORT_URL ||
        existingExtra.supportUrl ||
        "https://mixterious.co/support",
    },
  };
};
