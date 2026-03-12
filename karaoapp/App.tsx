import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  AppState,
  Image,
  Keyboard,
  KeyboardAvoidingView,
  Linking,
  Modal,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  Share,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableWithoutFeedback,
  useWindowDimensions,
  View,
} from "react-native";
import type { StyleProp, TextStyle } from "react-native";
import { VideoView, useVideoPlayer } from "expo-video";
import type { VideoPlayer } from "expo-video";
import { WebView } from "react-native-webview";
import Constants from "expo-constants";
import { File as FileSystemFile, Paths as FileSystemPaths } from "expo-file-system";
import * as FileSystemLegacy from "expo-file-system/legacy";
import * as Haptics from "expo-haptics";
import * as MediaLibrary from "expo-media-library";
import * as Sharing from "expo-sharing";
import * as Application from "expo-application";
import * as Updates from "expo-updates";
import NetInfo from "@react-native-community/netinfo";
import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Notifications from "expo-notifications";
import { getApiBaseUrl, setApiBaseUrl } from "./src/apiBaseUrl";
import {
  detectBackendMode,
  shouldPreferDirectPlaybackForBaseUrl,
  shouldRequestYoutubeUploadForBaseUrl,
} from "./src/backendMode";
import {
  isBackendUnreachableMessage,
  isLikelyNetworkErrorText,
  LOCAL_BACKEND_UNREACHABLE_MESSAGE,
  looksLikeHtmlErrorPayload,
  resolveBackendUnreachableMessage,
} from "./src/backendErrorMessage";
import {
  isJobCancelled,
  isJobFailed,
  isJobInProgress,
  isJobSucceeded,
  isJobTerminal,
  shouldKeepProcessingModalOpen,
} from "./src/jobStatus";
import {
  buildCreateJobPayload,
} from "./src/createJobPayload";
import {
  buildStartJobRequestPayload,
} from "./src/jobStartPayload";
import { buildApplyVideoTuningRequest } from "./src/videoRetuneRequest";
import {
  canScheduleCompletionNotification,
  canWriteToPhotoLibrary,
  isPermissionGranted,
  shouldPromptForNotificationPermission,
  shouldPromptForPhotoLibraryWritePermission,
} from "./src/permissionFlow";
import { resolveOutputDownloadStrategy } from "./src/outputDownloadStrategy";
import {
  clampPreviewToFinalSeek,
  DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
  shouldArmPreviewToFinalSeek,
  shouldCarryPauseStateAcrossPreviewToFinal,
  shouldResetPlaybackOnSourceChange,
} from "./src/videoHandoff";
import { appendMediaRevisionToken, buildJobMediaRevisionToken } from "./src/mediaUrlVersioning";
import { resolveFailoverApiBaseUrl } from "./src/localApiHost";
import { shouldUsePreviewCompanionAudio } from "./src/previewAudioPolicy";
import { isPlaybackSourceReady, resolvePlaybackSource } from "./src/videoPlaybackPolicy";

const DEFAULT_BASE_URL = "http://127.0.0.1:8000";
const FALLBACK_BASE_URL = "http://127.0.0.1:8000";
const ELAPSED_TIMER_TICK_MS = 500;
const PRECISE_ELAPSED_TIMER_TICK_MS = 50;
const JOB_POLL_INTERVAL_ACTIVE_MS = 1200;
const JOB_POLL_INTERVAL_ACTIVE_NEAR_DONE_MS = 700;
const JOB_POLL_INTERVAL_BACKGROUND_MS = 5000;
const JOB_POLL_INTERVAL_ACTIVE_MAX_MS = 4200;
const JOB_POLL_INTERVAL_BACKGROUND_MAX_MS = 12000;
const JOB_POLL_UNCHANGED_STREAK_STEP = 0.2;
const JOB_POLL_UNCHANGED_STREAK_CAP = 2.5;
const JOB_POLL_INTERVAL_JITTER_RATIO = 0.12;
const VIDEO_TIME_UPDATE_INTERVAL_SEC = 0.25;
const VIDEO_SURFACE_REFRESH_BACKGROUND_GAP_MS = 2000;
const PRESET_EMBED_WAIT_RETRY_MS = 1200;
const PRESET_EMBED_WAIT_MAX_MS = 300000;
const VIDEO_EMBED_WEBVIEW_LOAD_TIMEOUT_MS = 12000;
const VIDEO_EMBED_PROCESSING_RELOAD_DELAY_MS = 5000;
const VIDEO_EMBED_MAX_PROCESSING_RETRIES_PER_VARIANT = 24;
const VIDEO_EMBED_MAX_RETRY_VARIANTS = 3;
const PROCESSING_MODAL_NO_JOB_GRACE_MS = 5000;
const PROCESSING_MODAL_STALE_JOB_AGE_MS = 45000;
const PROCESSING_MODAL_STALE_REFRESH_INTERVAL_MS = 10000;
const PROCESSING_MODAL_STALE_JOB_FORCE_CLOSE_MS = 240000;
const PROCESSING_MODAL_HARD_TIMEOUT_MS = 13 * 60 * 1000;
const GENERIC_ERROR_MESSAGE = "❌ Encountered an error";
const GENERIC_ERROR_FALLBACK_MESSAGE = "Something went wrong...";
const REQUEST_SUBMITTED_STATUS_MESSAGE = "Request submitted";
const REQUEST_RECEIVED_STATUS_MESSAGE = "Request received";
const PROCESSING_REQUEST_STATUS_MESSAGE = "Processing Request";
const VIDEO_LOADING_STATUS_MESSAGE = PROCESSING_REQUEST_STATUS_MESSAGE;
const VIDEO_LOAD_RECOVERY_MESSAGE = GENERIC_ERROR_MESSAGE;
const GLOBAL_OFFSET_STORAGE_KEY = "@global_offset_sec";
const MIX_LEVELS_STORAGE_KEY = "@mix_levels";
const ADVANCED_OPEN_STORAGE_KEY = "@advanced_open";
const ADVANCED_LAST_USED_OPEN_STORAGE_KEY = "@advanced_last_used_open_v1";
const ADVANCED_DEFAULT_MODE_STORAGE_KEY = "@advanced_default_mode_v1";
const ADVANCED_BUTTON_VISIBLE_STORAGE_KEY = "@advanced_button_visible";
const SHOW_TIMERS_STORAGE_KEY = "@show_timers_v1";
const BACKGROUND_VIDEO_PLAYBACK_STORAGE_KEY = "@background_video_playback_v1";
const AUTO_SAVE_GENERATED_VIDEOS_STORAGE_KEY = "@auto_save_generated_videos_v1";
const SEARCH_AUTO_CORRECT_STORAGE_KEY = "@search_auto_correct_v1";
const NOTIFICATIONS_ENABLED_STORAGE_KEY = "@notifications_enabled_v1";
const NOTIFICATIONS_FIRST_LAUNCH_PROMPTED_STORAGE_KEY = "@notifications_first_launch_prompted_v1";
const RECENT_NORMALIZED_SONGS_STORAGE_KEY = "@recent_normalized_songs_v1";
const SHOW_RECENT_NORMALIZED_SONGS_STORAGE_KEY = "@show_recent_normalized_songs_v1";
const DEFAULT_SHOW_TIMERS = true;
const RECENT_NORMALIZED_SONGS_MAX = 5;
const HISTORY_FEATURE_ENABLED = false;
const DEMO_QUERY_VALUE = "The Beatles - Let It Be";
const SMOKE_TEST_QUERY = String(process.env.EXPO_PUBLIC_SMOKE_TEST_QUERY || "").trim();
const SMOKE_TEST_VOCALS = (() => {
  const parsed = Number(process.env.EXPO_PUBLIC_SMOKE_TEST_VOCALS || "0");
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.min(150, Math.round(parsed)));
})();
const PRESET_QUERIES_EASTER_EGG_QUERY = "miguelkaraoke";
const PHOTO_LIBRARY_SETTINGS_REQUIRED_MESSAGE =
  "Photos/Media access is required to save videos, enable library permission in Settings";
const STEM_LABELS: Record<"vocals" | "bass" | "drums" | "other", string> = {
  vocals: "VOCALS",
  bass: "BASS",
  drums: "DRUMS",
  other: "OTHER",
};
type StemKey = keyof typeof STEM_LABELS;
const STEM_KEYS: StemKey[] = ["vocals", "bass", "drums", "other"];
const DEFAULT_MIX_LEVELS: { vocals: number; bass: number; drums: number; other: number } = {
  vocals: 100,
  bass: 100,
  drums: 100,
  other: 100,
};
const OFFSET_STEP_BUTTONS: Array<{ label: string; delta: number }> = [
  { label: "-1.00s", delta: -1 },
  { label: "-0.25s", delta: -0.25 },
  { label: "+0.25s", delta: 0.25 },
  { label: "+1.00s", delta: 1 },
];
type AdvancedDefaultMode = "collapsed" | "expanded" | "last_used";
const DEFAULT_ADVANCED_DEFAULT_MODE: AdvancedDefaultMode = "collapsed";
const DEFAULT_SHOW_ADVANCED_BUTTON = true;
const ADVANCED_DEFAULT_MODE_OPTIONS: Array<{ value: AdvancedDefaultMode; label: string }> = [
  { value: "collapsed", label: "Default: Collapsed" },
  { value: "expanded", label: "Default: Expanded" },
  { value: "last_used", label: "Use Last Used State" },
];

const parseAdvancedDefaultMode = (value: unknown): AdvancedDefaultMode => {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "collapsed" || normalized === "expanded" || normalized === "last_used") {
    return normalized;
  }
  return DEFAULT_ADVANCED_DEFAULT_MODE;
};

const resolveAdvancedOpenFromMode = (mode: AdvancedDefaultMode, lastUsedOpen: boolean): boolean => {
  if (mode === "expanded") return true;
  if (mode === "collapsed") return false;
  return lastUsedOpen;
};

const parseBoolFlag = (value: unknown): boolean => {
  const text = String(value ?? "").trim().toLowerCase();
  return text === "1" || text === "true" || text === "yes" || text === "on";
};

const resolveDemoMode = (): boolean => {
  try {
    const configuredFlag =
      (Constants.expoConfig?.extra?.demoMode as boolean | string | undefined) ??
      process.env.EXPO_PUBLIC_DEMO_MODE;
    if (configuredFlag !== undefined && configuredFlag !== null && String(configuredFlag).trim() !== "") {
      return parseBoolFlag(configuredFlag);
    }

    const runtimeFlags = (globalThis as any).__MIXTERIOUS_FLAGS__;
    if (runtimeFlags?.demoMode === true) return true;

    const href = (globalThis as any)?.location?.href;
    if (!href) return false;
    const params = new URL(href).searchParams;
    return parseBoolFlag(params.get("demo"));
  } catch {
    return false;
  }
};

// Configure notifications
Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowAlert: true,
    shouldPlaySound: true,
    shouldSetBadge: false,
    shouldShowBanner: true,
    shouldShowList: true,
  }),
});
const ENABLE_DEBUG_LOGS = typeof __DEV__ !== "undefined" ? __DEV__ : false;

const CLIENT_LOG_MAX_ENTRIES = 600;
const clientLogBuffer: string[] = [];

const _stringifyLogArg = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (value instanceof Error) return `${value.name}: ${value.message}`;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const appendClientLog = (level: "INFO" | "WARN", args: unknown[]) => {
  const ts = new Date().toISOString();
  const line = `${ts} [${level}] ${args.map(_stringifyLogArg).join(" ")}`.slice(0, 2000);
  clientLogBuffer.push(line);
  if (clientLogBuffer.length > CLIENT_LOG_MAX_ENTRIES) {
    clientLogBuffer.splice(0, clientLogBuffer.length - CLIENT_LOG_MAX_ENTRIES);
  }
};

const getClientLogSnapshot = (maxEntries = 250): string[] => {
  const take = Math.max(1, Math.min(maxEntries, CLIENT_LOG_MAX_ENTRIES));
  return clientLogBuffer.slice(-take);
};

const debugLog = (...args: unknown[]) => {
  appendClientLog("INFO", args);
  if (ENABLE_DEBUG_LOGS) {
    console.log(...args);
  }
};

const debugWarn = (...args: unknown[]) => {
  appendClientLog("WARN", args);
  if (ENABLE_DEBUG_LOGS) {
    console.warn(...args);
  }
};

const resolveApiBaseUrl = () => {
  const configured =
    Constants.expoConfig?.extra?.apiBaseUrl ||
    process.env.EXPO_PUBLIC_API_BASE_URL;
  if (configured) return configured.replace(/\/$/, "");

  return DEFAULT_BASE_URL;
};

const isLikelyPhysicalDevice = () => {
  const isDevice = (Constants as any)?.isDevice;
  const iosModel = String((Constants as any)?.platform?.ios?.model || "").toLowerCase();
  const deviceName = String((Constants as any)?.deviceName || "").toLowerCase();
  const simulatorHint = iosModel.includes("simulator") || deviceName.includes("simulator");
  if (isDevice === false) return false;
  if (isDevice === true) return !simulatorHint;
  return !simulatorHint;
};

const normalizeDisplayBaseVersion = (value: unknown): string => {
  const text = String(value ?? "").trim();
  if (!text) return "";
  const numericParts = text
    .split(".")
    .map((part) => part.trim())
    .filter((part) => /^\d+$/.test(part));
  if (numericParts.length >= 2) return numericParts.slice(0, 3).join(".");
  if (numericParts.length === 1) return numericParts[0];
  return text;
};

const APP_BASE_VERSION =
  normalizeDisplayBaseVersion(Constants.nativeApplicationVersion) ||
  normalizeDisplayBaseVersion(Constants.expoConfig?.version) ||
  normalizeDisplayBaseVersion(Constants.expoConfig?.extra?.appVersion as string | undefined) ||
  "1.0";

const APP_BUILD_NUMBER = String(
  Constants.nativeBuildVersion ||
    (Constants.expoConfig?.ios?.buildNumber as string | undefined) ||
    (Constants.expoConfig?.extra?.appBuildNumber as string | number | undefined) ||
    ""
).trim();

const APP_DISPLAY_VERSION = APP_BUILD_NUMBER
  ? `v${APP_BASE_VERSION} (${APP_BUILD_NUMBER})`
  : `v${APP_BASE_VERSION}`;
const APP_EMBED_CLIENT_ID =
  String(
    Application.applicationId ||
      (Constants.expoConfig?.ios?.bundleIdentifier as string | undefined) ||
      (Constants.expoConfig?.android?.package as string | undefined) ||
      "com.cazares.mixterious"
  )
    .trim()
    .toLowerCase();
const YOUTUBE_EMBED_ORIGIN = `https://${APP_EMBED_CLIENT_ID}`;
const OTA_UPDATE_CHECK_COOLDOWN_MS = 15 * 60 * 1000;
const OTA_AUTO_CHECK_DEFAULT = true;

const IDEMPOTENT_HTTP_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const RETRYABLE_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);
const RETRY_BASE_DELAY_MS = 140;
const RETRY_MAX_DELAY_MS = 900;

type FetchWithTimeoutPolicy = {
  maxAttempts?: number;
  retryOnStatuses?: ReadonlySet<number>;
};

const sleepMs = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const isRetryableNetworkError = (error: unknown): boolean => {
  const anyError = error as any;
  const name = String(anyError?.name || "").toLowerCase();
  const message = String(anyError?.message || "").toLowerCase();
  if (name === "aborterror") return true;
  return (
    message.includes("network request failed") ||
    message.includes("networkerror") ||
    message.includes("timeout") ||
    message.includes("timed out") ||
    message.includes("connection") ||
    message.includes("econn") ||
    message.includes("enotfound")
  );
};

const retryDelayMs = (attemptNumber: number) => {
  const capped = Math.min(RETRY_BASE_DELAY_MS * 2 ** Math.max(0, attemptNumber - 1), RETRY_MAX_DELAY_MS);
  const jitter = capped * 0.3 * Math.random();
  return Math.round(capped + jitter);
};

const fetchWithTimeout = async (
  url: string,
  options: RequestInit = {},
  timeoutMs = 12000,
  policy: FetchWithTimeoutPolicy = {}
) => {
  const method = String(options.method || "GET").toUpperCase();
  const isIdempotent = IDEMPOTENT_HTTP_METHODS.has(method);
  const maxAttempts = Math.max(
    1,
    Math.min(
      Number(policy.maxAttempts ?? (isIdempotent ? 2 : 1)) || 1,
      3
    )
  );
  const retryOnStatuses = policy.retryOnStatuses || RETRYABLE_HTTP_STATUSES;
  const boundedTimeoutMs = Math.max(1000, Number(timeoutMs) || 12000);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), boundedTimeoutMs);
    const startedAt = Date.now();
    try {
      const res = await fetch(url, { ...options, signal: controller.signal });
      const elapsedMs = Date.now() - startedAt;
      debugLog(`[net] ${method} ${url} -> ${res.status} (${elapsedMs}ms)`, {
        attempt,
        maxAttempts,
      });
      if (!res.ok && ENABLE_DEBUG_LOGS) {
        try {
          const body = await res.clone().text();
          debugWarn(`[net] ${method} ${url} non-OK body`, body.slice(0, 500));
        } catch {
          // ignore body read errors
        }
      }

      const shouldRetryStatus =
        isIdempotent && attempt < maxAttempts && retryOnStatuses.has(res.status);
      if (shouldRetryStatus) {
        const delayMs = retryDelayMs(attempt);
        debugWarn(`[net] retrying ${method} ${url} after status ${res.status}`, {
          attempt,
          delayMs,
        });
        await sleepMs(delayMs);
        continue;
      }
      return res;
    } catch (err: any) {
      const elapsedMs = Date.now() - startedAt;
      const shouldRetryError =
        isIdempotent &&
        attempt < maxAttempts &&
        isRetryableNetworkError(err);
      debugWarn(`[net] request failed ${url} (${elapsedMs}ms)`, {
        name: err?.name,
        message: err?.message,
        code: err?.code,
        attempt,
        maxAttempts,
        retrying: shouldRetryError,
      });
      if (shouldRetryError) {
        const delayMs = retryDelayMs(attempt);
        await sleepMs(delayMs);
        continue;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error("Network request failed");
};

const COOKIE_REFRESH_REQUIRED_MARKER = "COOKIE_REFRESH_REQUIRED";
const COOKIE_REFRESH_REQUIRED_HINT =
  "That source can't be downloaded right now, try a different song or paste a different source link";
const LYRICS_MISSING_HINTS = [
  "could not find synced lyrics for this track",
  "no synced lyrics found",
  "no lyrics found",
];
const AUDIO_MISSING_HINTS = [
  "could not find audio for this track",
  "no audio found",
  "step1 audio missing",
  "missing source audio",
];
const HOT_QUERY_SUGGESTIONS: Record<string, string> = {
  "let it be": "The Beatles - Let It Be",
  "beatles let it be": "The Beatles - Let It Be",
  "the beatles let it be": "The Beatles - Let It Be",
  "john frusciante god": "John Frusciante - God",
  "john frusciante the past recedes": "John Frusciante - The Past Recedes",
};
const QUERY_PREFILL_BUTTONS = [
  "Goo Goo Dolls - Iris",
  "Maná - Mariposa Traicionera",
  "Mazzy Star - Fade Into You",
  "Notorious B.I.G. - Sky's the Limit (feat. 112)",
  "Pearl Jam - Black",
  "Pink Floyd - Time",
  "Red Hot Chili Peppers - Under The Bridge",
  "Simon & Garfunkel - The Sound of Silence",
  "The Beatles - Let It Be",
  "The Eagles - Hotel California",
  "Vicente Fernández - Para Siempre",
];
const TOP_50_ALL_TIME_QUERIES = [
  "Queen - Bohemian Rhapsody",
  "The Beatles - Hey Jude",
  "Led Zeppelin - Stairway to Heaven",
  "Bob Dylan - Like a Rolling Stone",
  "Nirvana - Smells Like Teen Spirit",
  "John Lennon - Imagine",
  "Marvin Gaye - What's Going On",
  "The Beach Boys - Good Vibrations",
  "Aretha Franklin - Respect",
  "The Rolling Stones - (I Can't Get No) Satisfaction",
  "The Who - Baba O'Riley",
  "Eagles - Hotel California",
  "Michael Jackson - Billie Jean",
  "Prince - Purple Rain",
  "The Beatles - Let It Be",
  "The Beatles - A Day in the Life",
  "Fleetwood Mac - Dreams",
  "The Beatles - Yesterday",
  "Pink Floyd - Comfortably Numb",
  "Simon & Garfunkel - Bridge Over Troubled Water",
  "U2 - One",
  "Outkast - Hey Ya!",
  "The Clash - London Calling",
  "David Bowie - Heroes",
  "Jimi Hendrix - All Along the Watchtower",
  "Otis Redding - (Sittin' On) The Dock of the Bay",
  "Stevie Wonder - Superstition",
  "Guns N' Roses - Sweet Child O' Mine",
  "AC/DC - Back In Black",
  "The Doors - Light My Fire",
  "Janis Joplin - Piece of My Heart",
  "Whitney Houston - I Will Always Love You",
  "The Police - Every Breath You Take",
  "Journey - Don't Stop Believin'",
  "Oasis - Wonderwall",
  "Cyndi Lauper - Time After Time",
  "Radiohead - Creep",
  "Adele - Rolling in the Deep",
  "Tina Turner - What's Love Got to Do with It",
  "Elvis Presley - Can't Help Falling in Love",
  "Johnny Cash - Ring of Fire",
  "Elton John - Tiny Dancer",
  "Bruce Springsteen - Born to Run",
  "Madonna - Like a Prayer",
  "The Temptations - My Girl",
  "Frank Sinatra - My Way",
  "Bon Jovi - Livin' on a Prayer",
  "Etta James - At Last",
  "The Killers - Mr. Brightside",
  "The Beatles - Come Together",
];
const APP_HEADER_ICON = require("./ios/Mixterious/Images.xcassets/AppIcon.appiconset/AppIcon.png");
const PROCESSING_MODAL_DOT_MAX = 10;
const PROCESSING_MODAL_DOT_STEP_MIN_MS = 2000;
const PROCESSING_MODAL_DOT_STEP_MAX_MS = 3000;
const PROCESSING_MODAL_STAGES: Array<{ title: string; subtext: string }> = [
  { title: "Loading", subtext: REQUEST_SUBMITTED_STATUS_MESSAGE },
  { title: "Waiting", subtext: REQUEST_RECEIVED_STATUS_MESSAGE },
  { title: "Setting up", subtext: "Handling response" },
  { title: "Almost there", subtext: "Get ready to sing-a-long" },
  { title: "Thank you for your patience", subtext: "Just about done" },
  { title: "Finishing up", subtext: "Whoops, hit a snag" },
  { title: "Fixing snag", subtext: "Fallback plan" },
  { title: "Retrying", subtext: "Error encountered" },
  { title: "Investigating", subtext: "Resolving error" },
  { title: "Recovering", subtext: "Almost" },
  { title: "Continuing", subtext: "Nearly there" },
  { title: "Resolving", subtext: PROCESSING_REQUEST_STATUS_MESSAGE },
];
const PROCESSING_MODAL_STEPS_PER_STAGE = PROCESSING_MODAL_DOT_MAX + 1;
const PROCESSING_MODAL_TOTAL_STEPS = PROCESSING_MODAL_STAGES.length * PROCESSING_MODAL_STEPS_PER_STAGE;
const PROCESSING_MODAL_MAX_TICK = Math.max(0, PROCESSING_MODAL_TOTAL_STEPS - 1);

const extractYoutubeVideoId = (value: string | null | undefined): string | null => {
  const text = String(value || "").trim();
  if (!text) return null;
  const directMatch = text.match(/^[a-zA-Z0-9_-]{11}$/);
  if (directMatch) return directMatch[0];
  const patterns = [
    /[?&]v=([a-zA-Z0-9_-]{11})/i,
    /youtu\.be\/([a-zA-Z0-9_-]{11})/i,
    /youtube\.com\/embed\/([a-zA-Z0-9_-]{11})/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) return match[1];
  }
  return null;
};

type YoutubeEmbedMode = "origin" | "plain" | "nocookie";

const buildYoutubeEmbedUrl = (
  value: string | null | undefined,
  options?: { autoplay?: boolean; mode?: YoutubeEmbedMode }
): string | null => {
  const videoId = extractYoutubeVideoId(value);
  if (!videoId) return null;
  const mode = options?.mode || "origin";
  const autoplay = options?.autoplay === true ? "1" : "0";
  const mute = "0";
  const host = mode === "nocookie" ? "www.youtube-nocookie.com" : "www.youtube.com";
  const originParam =
    mode === "origin" ? `&origin=${encodeURIComponent(YOUTUBE_EMBED_ORIGIN)}` : "";
  return `https://${host}/embed/${videoId}?playsinline=1&autoplay=${autoplay}&mute=${mute}&rel=0&modestbranding=1&iv_load_policy=3&cc_load_policy=0${originParam}`;
};

const extractYoutubeUrlFromJob = (job: JobStatus | null | undefined): string | null => {
  if (!job) return null;
  const candidateJob = job as Record<string, any>;
  const candidates = [
    job.youtube_video_url,
    candidateJob.youtubeVideoUrl,
    candidateJob.youtube_url,
    candidateJob.youtubeUrl,
    candidateJob.result?.youtube_video_url,
    candidateJob.result?.youtubeVideoUrl,
    candidateJob.step5?.youtube_video_url,
    candidateJob.step5?.youtubeVideoUrl,
    candidateJob.step5_result?.youtube_video_url,
    candidateJob.step5_result?.youtubeVideoUrl,
    candidateJob.delivery?.youtube_video_url,
    candidateJob.delivery?.youtubeVideoUrl,
  ];
  for (const value of candidates) {
    const text = String(value || "").trim();
    if (!text) continue;
    if (extractYoutubeVideoId(text)) return text;
  }
  return null;
};

const buildYoutubeEmbedBridgeScript = () => `
(function () {
  if (window.__mixteriousEmbedBridgeInstalled) {
    return;
  }
  window.__mixteriousEmbedBridgeInstalled = true;

  function send(type, detail) {
    try {
      if (!window.ReactNativeWebView || !window.ReactNativeWebView.postMessage) return;
      window.ReactNativeWebView.postMessage(JSON.stringify({
        source: "mixterious-youtube-bridge",
        type: String(type || ""),
        detail: String(detail || "")
      }));
    } catch (err) {}
  }

  function isReadyVideoElement() {
    try {
      var video = document && document.querySelector ? document.querySelector("video") : null;
      if (!video) return false;
      var readyState = Number(video.readyState || 0);
      var duration = Number(video.duration || 0);
      var currentTime = Number(video.currentTime || 0);
      return readyState >= 2 || duration > 0 || currentTime > 0;
    } catch (err) {
      return false;
    }
  }

  function isElementVisible(node) {
    try {
      if (!node || !node.getBoundingClientRect) return false;
      var style = window.getComputedStyle ? window.getComputedStyle(node) : null;
      if (style) {
        if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity || "1") <= 0) {
          return false;
        }
      }
      var rect = node.getBoundingClientRect();
      return Boolean(rect && rect.width > 0 && rect.height > 0);
    } catch (err) {
      return false;
    }
  }

  function visiblePlayerErrorText() {
    try {
      if (!document || !document.querySelectorAll) return "";
      var selectors = [
        ".html5-video-player .ytp-error-content-wrap",
        ".html5-video-player .ytp-error-content",
        ".html5-video-player .ytp-error",
        "#player .ytp-error-content-wrap",
        "#player .ytp-error",
        ".ytp-error-content-wrap",
      ];
      var chunks = [];
      var seen = {};
      for (var i = 0; i < selectors.length; i += 1) {
        var nodes = document.querySelectorAll(selectors[i]);
        if (!nodes || !nodes.length) continue;
        for (var j = 0; j < nodes.length; j += 1) {
          var node = nodes[j];
          if (!node || !isElementVisible(node)) continue;
          var text = String(node.innerText || node.textContent || "").toLowerCase().trim();
          if (!text) continue;
          if (seen[text]) continue;
          seen[text] = true;
          chunks.push(text);
        }
      }
      return chunks.join(" ");
    } catch (err) {
      return "";
    }
  }

  function detect() {
    if (isReadyVideoElement()) {
      send("ready", "youtube_ready");
      return;
    }
    var errorText = visiblePlayerErrorText();
    if (!errorText) return;
    if (
      errorText.indexOf("we're processing this video") >= 0 ||
      errorText.indexOf("check back later") >= 0
    ) {
      send("processing", "youtube_processing");
      return;
    }
    if (
      errorText.indexOf("an error occurred") >= 0 ||
      errorText.indexOf("playback id") >= 0 ||
      errorText.indexOf("video unavailable") >= 0
    ) {
      send("unavailable", "youtube_unavailable");
    }
  }

  var checks = 0;
  var interval = setInterval(function () {
    checks += 1;
    detect();
    if (checks >= 90) {
      clearInterval(interval);
    }
  }, 1000);
  setTimeout(detect, 300);
  setTimeout(detect, 1200);
})();
true;
`;

type SourceSearchResult = {
  video_id: string;
  title: string;
  duration?: number | null;
  thumbnail?: string | null;
  uploader?: string | null;
};

const hasCookieRefreshMarker = (message?: string | null) =>
  String(message || "").toUpperCase().includes(COOKIE_REFRESH_REQUIRED_MARKER);

const sanitizeApiErrorMessage = (message?: string | null) => {
  const text = String(message || "").trim();
  if (!text) return "";
  const effectiveBaseUrl = getApiBaseUrl() || resolveApiBaseUrl();
  if (looksLikeHtmlErrorPayload(text)) {
    return resolveBackendUnreachableMessage(effectiveBaseUrl);
  }
  if (isLikelyNetworkErrorText(text)) {
    return resolveBackendUnreachableMessage(effectiveBaseUrl);
  }
  const markerIdx = text.toUpperCase().indexOf(COOKIE_REFRESH_REQUIRED_MARKER);
  if (markerIdx < 0) return text.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  const colonIdx = text.indexOf(":", markerIdx);
  const detail = colonIdx >= 0 ? text.slice(colonIdx + 1).trim() : "";
  return detail || COOKIE_REFRESH_REQUIRED_HINT;
};

const normalizeLooseQuery = (value: string) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const cleanQueryMetadataNoise = (value: string) =>
  String(value || "")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/\((?:[^)]*\b(?:official|lyrics?|audio|video|hd|4k)\b[^)]*)\)/gi, " ")
    .replace(/\b(?:official|lyrics?|audio|video|hd|4k|remastered)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const titleCaseWords = (value: string) =>
  String(value || "")
    .split(" ")
    .filter(Boolean)
    .map((token) => token[0].toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");

const normalizeResultQueryText = (title: string, uploader?: string | null): string => {
  const cleanTitle = cleanQueryMetadataNoise(title);
  const cleanUploader = cleanQueryMetadataNoise(String(uploader || ""));
  if (!cleanTitle) return cleanUploader;
  if (!cleanUploader) return cleanTitle;
  const titleLower = cleanTitle.toLowerCase();
  const uploaderLower = cleanUploader.toLowerCase();
  if (titleLower.startsWith(`${uploaderLower} -`)) return cleanTitle;
  if (titleLower.includes(uploaderLower)) return cleanTitle;
  return `${cleanUploader} - ${cleanTitle}`;
};

const extractQuotedQueryHint = (message?: string | null): string => {
  const text = String(message || "");
  const match = text.match(/query\s*:\s*['"]([^'"]+)['"]/i);
  if (match?.[1]) return match[1].trim();
  return "";
};

const isLyricsMissingMessage = (message?: string | null): boolean => {
  const lower = String(message || "").toLowerCase();
  if (!lower) return false;
  return LYRICS_MISSING_HINTS.some((hint) => lower.includes(hint));
};

const isAudioMissingMessage = (message?: string | null): boolean => {
  const lower = String(message || "").toLowerCase();
  if (!lower) return false;
  return AUDIO_MISSING_HINTS.some((hint) => lower.includes(hint));
};

const deriveSuggestedQuery = (rawQuery: string): string => {
  const normalized = normalizeLooseQuery(rawQuery);
  if (!normalized) return "";
  const hot = HOT_QUERY_SUGGESTIONS[normalized];
  if (hot) return hot;
  if (normalized.includes("-")) {
    const [leftRaw, ...rightParts] = normalized.split("-");
    const left = titleCaseWords(leftRaw || "");
    const right = titleCaseWords(rightParts.join("-"));
    const joined = [left, right].filter(Boolean).join(" - ").trim();
    return joined || "";
  }
  return "";
};

const formatDurationText = (durationSec?: number | null): string => {
  const duration = Number(durationSec);
  if (!Number.isFinite(duration) || duration <= 0) return "";
  const total = Math.round(duration);
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  return `${mins}:${secs.toString().padStart(2, "0")}`;
};

const isFatalMissingResourceMessage = (message?: string | null) => {
  const text = String(message || "").trim();
  if (!text) return false;
  return isLyricsMissingMessage(text) || isAudioMissingMessage(text);
};

const extractFatalMissingResourceMessage = (...messages: Array<string | null | undefined>) => {
  for (const message of messages) {
    const raw = String(message || "").trim();
    if (!raw) continue;
    if (isFatalMissingResourceMessage(raw)) return sanitizeApiErrorMessage(raw) || raw;
    const sanitized = sanitizeApiErrorMessage(raw);
    if (isFatalMissingResourceMessage(sanitized)) return sanitized || raw;
  }
  return "";
};

const parseApiErrorPayload = (raw: string) => {
  const text = String(raw || "").trim();
  if (!text) return "";
  try {
    const parsed = JSON.parse(text);
    if (typeof parsed === "string") return parsed;
    if (parsed && typeof parsed.detail === "string") return parsed.detail;
    if (parsed && typeof parsed.message === "string") return parsed.message;
  } catch {
    // Ignore JSON parse failures and fall back to raw text.
  }
  return text;
};

const getJobPollIntervalMs = (
  job: { status?: string | null; stage?: string | null; progress_percent?: number | null } | null,
  appIsActive: boolean
) => {
  if (!appIsActive) return JOB_POLL_INTERVAL_BACKGROUND_MS;
  const stage = String(job?.stage || "").trim().toLowerCase();
  const progress = Number(job?.progress_percent);
  const nearDone =
    stage.includes("step4") ||
    stage.includes("render") ||
    stage.includes("preview") ||
    stage.includes("step5") ||
    stage.includes("upload") ||
    (Number.isFinite(progress) && progress >= 90);
  return nearDone ? JOB_POLL_INTERVAL_ACTIVE_NEAR_DONE_MS : JOB_POLL_INTERVAL_ACTIVE_MS;
};

const withPollBackoffAndJitter = (baseMs: number, unchangedStreak: number, appIsActive: boolean) => {
  const boundedBase = Math.max(300, Math.round(baseMs));
  const streak = Math.max(0, Math.min(20, unchangedStreak));
  const boost = Math.min(JOB_POLL_UNCHANGED_STREAK_CAP, streak * JOB_POLL_UNCHANGED_STREAK_STEP);
  const scaled = Math.round(boundedBase * (1 + boost));
  const cap = appIsActive ? JOB_POLL_INTERVAL_ACTIVE_MAX_MS : JOB_POLL_INTERVAL_BACKGROUND_MAX_MS;
  const capped = Math.min(cap, scaled);
  const jitter = capped * JOB_POLL_INTERVAL_JITTER_RATIO * (Math.random() - 0.5) * 2;
  return Math.max(300, Math.round(capped + jitter));
};

const buildJobStatusUrl = (baseUrl: string, jobId: string, view: "full" | "poll" = "full") => {
  const suffix = view === "poll" ? "?view=poll" : "";
  return `${baseUrl}/jobs/${encodeURIComponent(jobId)}${suffix}`;
};

const STATUS_MESSAGE_SET = new Set([
  REQUEST_SUBMITTED_STATUS_MESSAGE,
  REQUEST_RECEIVED_STATUS_MESSAGE,
  PROCESSING_REQUEST_STATUS_MESSAGE,
]);

const isProgressStatusMessage = (message?: string | null) => {
  const text = String(message || "").trim();
  if (!text) return false;
  if (STATUS_MESSAGE_SET.has(text)) return true;
  if (text.startsWith("✅")) return true;
  if (text.startsWith("❌")) return false;
  const lower = text.toLowerCase();
  if (lower.includes("fail") || lower.includes("error")) return false;
  return (
    lower.includes("request submitted") ||
    lower.includes("request received") ||
    lower.includes("processing request") ||
    lower.includes("loading") ||
    lower.includes("processing") ||
    lower.includes("queued") ||
    lower.includes("starting")
  );
};

const withoutTrailingPeriod = (value?: string | null) =>
  String(value || "")
    .trim()
    .replace(/[.]+$/g, "");

const coerceUiStatusMessage = (message?: string | null) => {
  const text = withoutTrailingPeriod(message);
  if (!text) return REQUEST_RECEIVED_STATUS_MESSAGE;
  if (STATUS_MESSAGE_SET.has(text)) return text;
  const lower = text.replace(/^✅\s*/, "").toLowerCase();
  if (
    lower.includes("submit") ||
    lower.includes("start") ||
    lower.includes("create") ||
    lower.includes("queued") ||
    lower.includes("requesting")
  ) {
    return REQUEST_SUBMITTED_STATUS_MESSAGE;
  }
  if (
    lower.includes("receive") ||
    lower.includes("accepted") ||
    lower.includes("created")
  ) {
    return REQUEST_RECEIVED_STATUS_MESSAGE;
  }
  if (
    lower.includes("processing") ||
    lower.includes("loading") ||
    lower.includes("normalizing") ||
    lower.includes("retry") ||
    lower.includes("refresh") ||
    lower.includes("waiting") ||
    lower.includes("running") ||
    lower.includes("pending") ||
    lower.includes("checking")
  ) {
    return PROCESSING_REQUEST_STATUS_MESSAGE;
  }
  return REQUEST_RECEIVED_STATUS_MESSAGE;
};

const formatUiError = (message?: string | null) => {
  const text = withoutTrailingPeriod(sanitizeApiErrorMessage(message));
  if (!text) return GENERIC_ERROR_MESSAGE;
  if (isProgressStatusMessage(text)) return coerceUiStatusMessage(text);
  if (isBackendUnreachableMessage(text)) return text;
  return GENERIC_ERROR_MESSAGE;
};

const formatUiStatus = (message?: string | null) => {
  const text = withoutTrailingPeriod(sanitizeApiErrorMessage(message));
  if (!text) return REQUEST_RECEIVED_STATUS_MESSAGE;
  if (text.startsWith("❌")) return REQUEST_RECEIVED_STATUS_MESSAGE;
  return coerceUiStatusMessage(text);
};

const coerceNonNegativeInt = (raw: unknown, fallback = 0): number => {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return Math.max(0, Math.floor(fallback));
  return Math.max(0, Math.floor(parsed));
};

const cleanQuotedText = (value: string) =>
  String(value || "")
    .trim()
    .replace(/^["'`“”]+/, "")
    .replace(/["'`“”]+$/, "")
    .trim();

type NormalizedSong = {
  artist: string;
  title: string;
};

type RecentNormalizedSong = {
  query: string;
  artist: string;
  title: string;
};

const normalizeRecentNormalizedSongs = (raw: unknown): RecentNormalizedSong[] => {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: RecentNormalizedSong[] = [];
  raw.forEach((item) => {
    const row = item as Partial<RecentNormalizedSong> | null | undefined;
    const query = cleanQuotedText(String(row?.query || ""));
    const artist = cleanQuotedText(String(row?.artist || ""));
    const title = cleanQuotedText(String(row?.title || ""));
    if (!query || !artist || !title) return;
    const key = `${query.toLowerCase()}|${artist.toLowerCase()}|${title.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ query, artist, title });
  });
  return out.slice(0, RECENT_NORMALIZED_SONGS_MAX);
};

const buildPickSongForMePool = (recentSongs: RecentNormalizedSong[]): string[] => {
  const uniqueQueries: string[] = [];
  const seenQueries = new Set<string>();
  const combinedQueries = [...QUERY_PREFILL_BUTTONS, ...TOP_50_ALL_TIME_QUERIES];
  combinedQueries.forEach((raw) => {
    const value = String(raw || "").trim();
    const key = normalizeLooseQuery(value);
    if (!value || !key || seenQueries.has(key)) return;
    seenQueries.add(key);
    uniqueQueries.push(value);
  });
  if (!recentSongs.length) return uniqueQueries;

  const normalizedHistory = new Set<string>();
  recentSongs.forEach((song) => {
    const queryKey = normalizeLooseQuery(song.query);
    if (queryKey) normalizedHistory.add(queryKey);
    const artistTitleKey = normalizeLooseQuery(`${song.artist} - ${song.title}`);
    if (artistTitleKey) normalizedHistory.add(artistTitleKey);
  });
  const unseen = uniqueQueries.filter((query) => !normalizedHistory.has(normalizeLooseQuery(query)));
  return unseen.length ? unseen : uniqueQueries;
};

const splitArtistAndTitle = (rawQuery?: string | null): { artist: string; title: string } | null => {
  const text = cleanQuotedText(String(rawQuery || ""));
  if (!text) return null;

  const byMatch = text.match(/^(.+?)\s+\bby\b\s+(.+)$/i);
  if (byMatch) {
    const title = cleanQuotedText(byMatch[1] || "");
    const artist = cleanQuotedText(byMatch[2] || "");
    if (artist && title) return { artist, title };
  }

  const separators = [" - ", " – ", " — ", " | ", " : "];
  for (const separator of separators) {
    const idx = text.indexOf(separator);
    if (idx <= 0) continue;
    const left = cleanQuotedText(text.slice(0, idx));
    const right = cleanQuotedText(text.slice(idx + separator.length));
    if (!left || !right) continue;
    // Current normalization flow favors "Artist - Song Title" input.
    return { artist: left, title: right };
  }
  return null;
};

type TimingBreakdownHeadingParts =
  | {
      mode: "query";
      query: string;
    }
  | {
      mode: "normalized";
      songTitle: string;
      artist: string;
    };

const buildTimingBreakdownHeadingParts = (
  rawQuery?: string | null,
  normalizedSong?: NormalizedSong | null,
  options: { preferRawQuery?: boolean } = {}
): TimingBreakdownHeadingParts | null => {
  const { preferRawQuery = false } = options;
  const queryText = cleanQuotedText(String(rawQuery || ""));
  const normalizedArtist = cleanQuotedText(String(normalizedSong?.artist || ""));
  const normalizedTitle = cleanQuotedText(String(normalizedSong?.title || ""));
  if (!preferRawQuery && normalizedArtist && normalizedTitle) {
    return {
      mode: "normalized",
      songTitle: normalizedTitle,
      artist: normalizedArtist,
    };
  }
  if (!queryText) return null;
  return {
    mode: "query",
    query: queryText,
  };
};

type TimingBreakdownHeadingProps = {
  rawQuery?: string | null;
  normalizedSong?: NormalizedSong | null;
  preferRawQuery?: boolean;
};

const TimingBreakdownHeading = React.memo(function TimingBreakdownHeading({
  rawQuery,
  normalizedSong,
  preferRawQuery = false,
}: TimingBreakdownHeadingProps) {
  const parts = buildTimingBreakdownHeadingParts(rawQuery, normalizedSong, { preferRawQuery });
  return (
    <View style={styles.timingHeadingWrap}>
      <Text style={styles.modalTitle}>Timing Breakdown</Text>
      {parts?.mode === "query" ? (
        <Text style={styles.timingHeadingMeta}>Query: &quot;{parts.query}&quot;</Text>
      ) : null}
      {parts?.mode === "normalized" ? (
        <Text style={styles.timingHeadingMeta}>
          Normalized: &quot;{parts.songTitle}&quot; by{" "}
          <Text style={styles.timingHeadingArtist}>{parts.artist}</Text>
        </Text>
      ) : null}
    </View>
  );
});

const normalizeFileUri = (rawUri: unknown) => {
  const uri = String(rawUri || "").trim();
  if (!uri) return "";
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(uri)) return uri;
  if (uri.startsWith("/")) return `file://${uri}`;
  return uri;
};

const isLikelyIosSimulatorSaveError = (raw: unknown) => {
  const msg = String((raw as any)?.message || raw || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("simulator") ||
    msg.includes("iphonesimulator") ||
    msg.includes("core simulator")
  );
};

const isLikelyPhotoPermissionSaveError = (raw: unknown) => {
  const msg = String((raw as any)?.message || raw || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("not authorized") ||
    msg.includes("permission") ||
    msg.includes("denied") ||
    msg.includes("restricted")
  );
};

const formatElapsed = (seconds: number, precision = 1) => `${seconds.toFixed(precision)}s`;
const formatStage = (stage?: string | null) =>
  (stage || "starting").replace(/_/g, " ").replace(/source/gi, "source");

type TimingBreakdownMap = Record<string, number>;
type PipelineStepKey = "step0" | "step1" | "step2" | "step3" | "step4" | "step5" | "step6";
type PipelineChecklistStatus = "completed" | "active" | "pending";
type PipelineChecklistRow = {
  key: PipelineStepKey;
  label: string;
  elapsedMs: number | null;
  status: PipelineChecklistStatus;
};

const PIPELINE_CHECKLIST_STEPS: ReadonlyArray<{ key: PipelineStepKey; label: string }> = [
  { key: "step0", label: "Step 0: query" },
  { key: "step1", label: "Step 1: source" },
  { key: "step2", label: "Step 2: lyrics" },
  { key: "step3", label: "Step 3: split" },
  { key: "step4", label: "Step 4: merge" },
  { key: "step5", label: "Step 5: assemble" },
  { key: "step6", label: "Step 6: deliver" },
];

const stageContainsAny = (stage: string, needles: string[]) =>
  needles.some((needle) => stage.includes(needle));

const timingEntryMs = (value: unknown): number | null => {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return num;
};

const buildTimingLookup = (raw?: TimingBreakdownMap | null): Record<string, number> => {
  if (!raw || typeof raw !== "object") return {};
  const out: Record<string, number> = {};
  Object.entries(raw).forEach(([k, v]) => {
    const key = String(k || "").trim().toLowerCase();
    const elapsedMs = timingEntryMs(v);
    if (!key || elapsedMs == null) return;
    out[key] = elapsedMs;
  });
  return out;
};

const timingMatchesPattern = (key: string, pattern: string) => {
  const normalizedPattern = String(pattern || "").trim().toLowerCase();
  if (!normalizedPattern) return false;
  if (normalizedPattern.endsWith("*")) {
    return key.startsWith(normalizedPattern.slice(0, -1));
  }
  return key === normalizedPattern || key.startsWith(`${normalizedPattern}.`);
};

const sumTimingForPatterns = (timings: Record<string, number>, patterns: string[]): number | null => {
  let sum = 0;
  let found = false;
  Object.entries(timings).forEach(([key, value]) => {
    if (!patterns.some((pattern) => timingMatchesPattern(key, pattern))) return;
    sum += value;
    found = true;
  });
  return found ? sum : null;
};

const hasTimingForPatterns = (timings: Record<string, number>, patterns: string[]) =>
  sumTimingForPatterns(timings, patterns) != null;

const firstNonNullNumber = (...values: Array<number | null | undefined>) => {
  for (const value of values) {
    if (value != null) return value;
  }
  return null;
};

const resolvePipelineStatus = ({
  completed,
  active,
}: {
  completed: boolean;
  active: boolean;
}): PipelineChecklistStatus => {
  if (completed) return "completed";
  if (active) return "active";
  return "pending";
};

const buildPipelineChecklistRows = (
  job: JobStatus | null,
  {
    preflightQueryActive = false,
    preflightQueryElapsedMs = null,
  }: {
    preflightQueryActive?: boolean;
    preflightQueryElapsedMs?: number | null;
  } = {}
): PipelineChecklistRow[] => {
  const timings = buildTimingLookup(job?.timing_breakdown);
  const stage = String(job?.stage || "").trim().toLowerCase();
  const status = String(job?.status || "").trim().toLowerCase();
  const terminal = isJobTerminal(status);
  const succeeded = isJobSucceeded(status);
  const hasJob = Boolean(job?.id);
  const hasPreviewOutput = Boolean(job?.preview_output_url || (job?.output_is_preview && job?.output_url));
  const hasFinalOutput = Boolean(job?.final_output_url || (!job?.output_is_preview && job?.output_url));
  const previewOnlyOutput = hasPreviewOutput && !hasFinalOutput && !terminal;

  const queryElapsedMs = (() => {
    if (preflightQueryElapsedMs != null) {
      return Math.max(0, preflightQueryElapsedMs);
    }
    const startedAtSec = Number(job?.started_at);
    const createdAtSec = Number(job?.created_at);
    if (Number.isFinite(startedAtSec) && Number.isFinite(createdAtSec) && startedAtSec >= createdAtSec) {
      return Math.max(0, (startedAtSec - createdAtSec) * 1000);
    }
    return hasJob ? 0 : null;
  })();
  const queryCompleted = hasJob || preflightQueryElapsedMs != null;

  const sourceElapsedMs = firstNonNullNumber(
    sumTimingForPatterns(timings, [
      "step1.download_audio",
      "step1.search_source",
      "step1.download",
      "step1.resolve_source",
    ]),
    timingEntryMs(timings["step1.total"])
  );
  const lyricsElapsedMs = firstNonNullNumber(
    sumTimingForPatterns(timings, [
      "step1.fetch_lyrics",
      "step3.parse_lrc",
      "step3.write_final_csv",
      "step3.auto_offset",
    ]),
    timingEntryMs(timings["step3.total"])
  );
  const splitElapsedMs = firstNonNullNumber(
    sumTimingForPatterns(timings, [
      "step2.ensure_demucs_stems",
      "step2.resolve_source_audio",
    ]),
    timingEntryMs(timings["step2.total_stems"])
  );
  const mergeElapsedMs = firstNonNullNumber(
    sumTimingForPatterns(timings, [
      "step2.stems_mix*",
      "step2.full_mix*",
      "step2.write_mix_meta",
      "step2.total_full",
    ]),
    timingEntryMs(timings["step2.total"])
  );
  const assembleElapsedMs = firstNonNullNumber(
    sumTimingForPatterns(timings, [
      "step4.ffmpeg_attempt*",
      "step4.finalize",
      "step4.build_ffmpeg_cmd",
      "step4.total",
    ]),
    timingEntryMs(timings["step4.total"])
  );
  const deliverElapsedMs = firstNonNullNumber(
    timingEntryMs(timings["step5.total"]),
    sumTimingForPatterns(timings, ["step5.*"])
  );

  const sourceCompleted =
    succeeded ||
    hasTimingForPatterns(timings, ["step1.download_audio", "step1.total"]) ||
    stageContainsAny(stage, [
      "separate_audio",
      "mix_audio",
      "sync_lyrics",
      "render_video",
      "preview_ready",
      "complete",
      "upload",
      "final_render",
    ]);
  const lyricsCoreCompleted = hasTimingForPatterns(timings, ["step3.total", "step3.write_final_csv", "step3.auto_offset"]);
  const lyricsFetchObserved = hasTimingForPatterns(timings, ["step1.fetch_lyrics"]);
  const lyricsCompleted =
    succeeded ||
    lyricsCoreCompleted ||
    (terminal && lyricsFetchObserved) ||
    stageContainsAny(stage, ["complete", "upload", "final_render"]);
  const splitCompleted =
    succeeded ||
    hasTimingForPatterns(timings, ["step2.total", "step2.total_stems", "step2.total_full", "step2.write_mix_meta"]) ||
    stageContainsAny(stage, [
      "mix_audio",
      "render_video",
      "complete",
      "upload",
      "final_render",
    ]);
  const mergeCompleted =
    succeeded ||
    hasTimingForPatterns(timings, ["step2.total", "step2.total_full", "step2.write_mix_meta"]) ||
    stageContainsAny(stage, [
      "render_video",
      "complete",
      "upload",
      "final_render",
    ]);
  const assembleCompleted =
    succeeded ||
    hasTimingForPatterns(timings, ["step4.total"]) ||
    stageContainsAny(stage, ["complete", "upload"]) ||
    Boolean(job?.final_output_url) ||
    (terminal && Boolean(job?.output_url || job?.preview_output_url));
  const deliverCompleted = succeeded || stageContainsAny(stage, ["complete"]);

  const step0Active =
    !queryCompleted &&
    ((hasJob && !terminal && !sourceCompleted && !lyricsCompleted) || (!hasJob && preflightQueryActive));
  const earlyStageActive =
    hasJob &&
    !terminal &&
    (stage === "" ||
      stageContainsAny(stage, [
        "starting",
        "step1",
        "fetch_lyrics",
        "search_source",
        "download_audio",
        "retrying_no_cookie_recovery",
      ]));

  const step1Active = earlyStageActive && !sourceCompleted;
  const step2Active =
    !terminal &&
    !lyricsCompleted &&
    (earlyStageActive ||
      stageContainsAny(stage, ["separate_audio", "mix_audio", "sync_lyrics", "preview_ready", "render_video"]) ||
      (lyricsFetchObserved && !lyricsCoreCompleted) ||
      previewOnlyOutput);
  const splitTimingObserved = hasTimingForPatterns(timings, ["step2.ensure_demucs_stems", "step2.resolve_source_audio"]);
  const mergeTimingObserved = hasTimingForPatterns(timings, ["step2.stems_mix*", "step2.full_mix*", "step2.write_mix_meta"]);
  const mergePhaseObserved = stageContainsAny(stage, ["mix_audio"]) || mergeTimingObserved;
  const splitPhaseActive = stageContainsAny(stage, ["separate_audio", "convert_audio"]) && !mergePhaseObserved;
  const step3Active =
    !terminal &&
    !splitCompleted &&
    (splitPhaseActive || (splitTimingObserved && !mergePhaseObserved));
  const step4Active =
    !terminal &&
    !mergeCompleted &&
    mergePhaseObserved;
  const step5Active =
    !terminal &&
    !assembleCompleted &&
    (stageContainsAny(stage, ["render_video", "preview_ready", "final_render"]) ||
      previewOnlyOutput ||
      hasTimingForPatterns(timings, ["step4.ffmpeg_attempt*", "step4.build_ffmpeg_cmd", "step4.finalize"]));
  const step6Active = !terminal && !deliverCompleted && stageContainsAny(stage, ["upload"]);

  return [
    {
      key: "step0",
      label: "Step 0: query",
      elapsedMs: queryElapsedMs,
      status: resolvePipelineStatus({ completed: queryCompleted, active: step0Active }),
    },
    {
      key: "step1",
      label: "Step 1: source",
      elapsedMs: sourceElapsedMs,
      status: resolvePipelineStatus({ completed: sourceCompleted, active: step1Active }),
    },
    {
      key: "step2",
      label: "Step 2: lyrics",
      elapsedMs: lyricsElapsedMs,
      status: resolvePipelineStatus({ completed: lyricsCompleted, active: step2Active }),
    },
    {
      key: "step3",
      label: "Step 3: split",
      elapsedMs: splitElapsedMs,
      status: resolvePipelineStatus({ completed: splitCompleted, active: step3Active }),
    },
    {
      key: "step4",
      label: "Step 4: merge",
      elapsedMs: mergeElapsedMs,
      status: resolvePipelineStatus({ completed: mergeCompleted, active: step4Active }),
    },
    {
      key: "step5",
      label: "Step 5: assemble",
      elapsedMs: assembleElapsedMs,
      status: resolvePipelineStatus({ completed: assembleCompleted, active: step5Active }),
    },
    {
      key: "step6",
      label: "Step 6: deliver",
      elapsedMs: deliverElapsedMs,
      status: resolvePipelineStatus({ completed: deliverCompleted, active: step6Active }),
    },
  ];
};

const getPipelineTotalElapsedMs = (
  timingBreakdown: TimingBreakdownMap | null | undefined,
  rows: PipelineChecklistRow[]
): number | null => {
  const timings = buildTimingLookup(timingBreakdown);
  const explicit = timingEntryMs(timings["pipeline.total"]);
  if (explicit != null) return explicit;
  let sum = 0;
  let found = false;
  rows.forEach((row) => {
    if (row.elapsedMs == null) return;
    sum += row.elapsedMs;
    found = true;
  });
  return found ? sum : null;
};

const timingBreakdownSignature = (raw?: TimingBreakdownMap | null): string => {
  if (!raw || typeof raw !== "object") return "";
  const keys = Object.keys(raw).sort();
  return keys.map((k) => `${k}:${Number(raw[k])}`).join("|");
};

type JobStatus = {
  id: string;
  status: string;
  query: string;
  slug: string;
  created_at: number;
  started_at?: number | null;
  render_started_at?: number | null;
  render_finished_at?: number | null;
  finished_at?: number | null;
  cancelled_at?: number | null;
  stage?: string | null;
  last_message?: string | null;
  last_updated_at?: number | null;
  error?: string | null;
  output_path?: string | null;
  output_url?: string | null;
  youtube_video_url?: string | null;
  mix_audio_url?: string | null;
  output_is_preview?: boolean | null;
  progress_percent?: number | null;
  estimated_seconds_remaining?: number | null;
  preview_output_url?: string | null;
  final_output_url?: string | null;
  timing_breakdown?: TimingBreakdownMap | null;
};

type TabKey = "search" | "video" | "history" | "settings";
type NotificationPermissionStatus = "undetermined" | "denied" | "granted" | "provisional" | "ephemeral";
type MixLevelsState = { vocals: number; bass: number; drums: number; other: number };
type StartJobOptions = {
  renderOnly?: boolean;
  upload?: boolean;
  presetAutoOpen?: boolean;
  audioId?: string;
  audioUrl?: string;
  force?: boolean;
  reset?: boolean;
  queryOverride?: string;
  normalizedArtist?: string;
  normalizedTitle?: string;
  mixLevelsOverride?: MixLevelsState;
};

const isSameJobSnapshot = (left: JobStatus | null, right: JobStatus | null) => {
  if (!left || !right) return left === right;
  const leftTimingSig = timingBreakdownSignature(left.timing_breakdown);
  const rightTimingSig = timingBreakdownSignature(right.timing_breakdown);
  return (
    left.id === right.id &&
    left.status === right.status &&
    left.stage === right.stage &&
    left.last_message === right.last_message &&
    left.error === right.error &&
    left.output_url === right.output_url &&
    left.youtube_video_url === right.youtube_video_url &&
    left.mix_audio_url === right.mix_audio_url &&
    left.output_is_preview === right.output_is_preview &&
    left.last_updated_at === right.last_updated_at &&
    left.started_at === right.started_at &&
    left.finished_at === right.finished_at &&
    left.cancelled_at === right.cancelled_at &&
    left.progress_percent === right.progress_percent &&
    left.estimated_seconds_remaining === right.estimated_seconds_remaining &&
    left.preview_output_url === right.preview_output_url &&
    left.final_output_url === right.final_output_url &&
    leftTimingSig === rightTimingSig
  );
};

type ElapsedTimerTextProps = {
  startAt: number | null;
  endAt: number | null;
  placeholder: string;
  style?: StyleProp<TextStyle>;
  precision?: number;
  tickMs?: number;
};

const ElapsedTimerText = React.memo(function ElapsedTimerText({
  startAt,
  endAt,
  placeholder,
  style,
  precision = 1,
  tickMs = ELAPSED_TIMER_TICK_MS,
}: ElapsedTimerTextProps) {
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  useEffect(() => {
    if (!startAt || endAt) return;
    const timer = setInterval(() => {
      setNowMs(Date.now());
    }, tickMs);
    return () => clearInterval(timer);
  }, [startAt, endAt, tickMs]);

  useEffect(() => {
    setNowMs(Date.now());
  }, [startAt, endAt]);

  const elapsedSeconds = useMemo(() => {
    if (!startAt) return null;
    const end = endAt ?? nowMs;
    return Math.max(0, (end - startAt) / 1000);
  }, [startAt, endAt, nowMs]);

  return <Text style={style}>{elapsedSeconds !== null ? formatElapsed(elapsedSeconds, precision) : placeholder}</Text>;
});

const OutputVideo = React.memo(function OutputVideo({
  player,
  uri,
  fullscreen = false,
  nativeControls = true,
  onMissingUrl,
}: {
  player: VideoPlayer;
  uri: string;
  fullscreen?: boolean;
  nativeControls?: boolean;
  onMissingUrl?: () => void;
}) {
  const [videoError, setVideoError] = useState<string | null>(null);
  const requestedRecoveryRef = useRef(false);
  const recoveryAttemptsRef = useRef(0);
  const transientRetryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const formatPlaybackError = useCallback((raw: unknown): string => {
    if (!raw) return "Playback failed";
    if (typeof raw === "string") return raw;
    if (typeof raw === "object" && raw && "message" in (raw as Record<string, unknown>)) {
      const msg = String((raw as Record<string, unknown>).message || "").trim();
      if (msg) return msg;
    }
    try {
      return JSON.stringify(raw);
    } catch {
      return "Playback failed";
    }
  }, []);

  const classifyPlaybackError = useCallback((message: string): "transient_interrupt" | "not_found" | "other" => {
    const text = String(message || "").trim();
    const lower = text.toLowerCase();
    if (lower.includes("operation stopped")) {
      return "transient_interrupt";
    }
    if (lower.includes("not found") || lower.includes("requested url was not found")) {
      return "not_found";
    }
    return "other";
  }, []);

  const clearTransientRetryTimer = useCallback(() => {
    if (transientRetryTimerRef.current) {
      clearTimeout(transientRetryTimerRef.current);
      transientRetryTimerRef.current = null;
    }
  }, []);

  const scheduleTransientRetry = useCallback(() => {
    if (recoveryAttemptsRef.current >= 2) return false;
    const delayMs = recoveryAttemptsRef.current === 0 ? 250 : 600;
    recoveryAttemptsRef.current += 1;
    clearTransientRetryTimer();
    transientRetryTimerRef.current = setTimeout(() => {
      try {
        player.play();
      } catch {
        // ignore transient replay failures
      }
    }, delayMs);
    return true;
  }, [clearTransientRetryTimer, player]);

  useEffect(() => {
    const subscription = player.addListener("statusChange", (status) => {
      debugLog("[video] status change", status);
      if (!status.error) {
        recoveryAttemptsRef.current = 0;
        setVideoError((prev) => (prev ? null : prev));
        return;
      }

      const rawMsg = formatPlaybackError(status.error);
      const issue = classifyPlaybackError(rawMsg);
      if (issue === "transient_interrupt") {
        const retrying = scheduleTransientRetry();
        if (retrying) {
          debugWarn("[video] transient interruption; retrying playback", rawMsg);
          setVideoError(null);
          return;
        }
        setVideoError("Playback was interrupted, tap play to retry");
        if (!requestedRecoveryRef.current) {
          requestedRecoveryRef.current = true;
          onMissingUrl?.();
        }
        return;
      }
      if (issue === "not_found") {
        setVideoError("Video file was not found, refreshing video status");
        if (!requestedRecoveryRef.current) {
          requestedRecoveryRef.current = true;
          onMissingUrl?.();
        }
        return;
      }
      if (status.error) {
        debugWarn("[video] playback warning", rawMsg);
        setVideoError(rawMsg || "Playback failed");
        if (!requestedRecoveryRef.current) {
          requestedRecoveryRef.current = true;
          onMissingUrl?.();
        }
      }
    });
    return () => subscription.remove();
  }, [player, formatPlaybackError, classifyPlaybackError, onMissingUrl, scheduleTransientRetry]);

  useEffect(() => {
    try {
      player.play();
    } catch {
      // ignore autoplay failures; user can still press play manually
    }
  }, [player, uri]);

  useEffect(() => {
    return () => clearTransientRetryTimer();
  }, [clearTransientRetryTimer]);

  useEffect(() => {
    clearTransientRetryTimer();
    requestedRecoveryRef.current = false;
    recoveryAttemptsRef.current = 0;
    setVideoError(null);
    debugLog("[video] rendering", { uri });
  }, [uri, clearTransientRetryTimer]);

  return (
    <View style={[styles.videoContainer, fullscreen && styles.videoContainerFullscreen]}>
      <VideoView
        style={[styles.video, fullscreen && styles.videoFullscreen]}
        player={player}
        nativeControls={nativeControls}
        allowsFullscreen
        allowsPictureInPicture
        contentFit="contain"
      />
      {videoError && (
        <View style={{ position: 'absolute', top: 10, left: 10, right: 10, backgroundColor: 'rgba(255,0,0,0.8)', padding: 10, borderRadius: 5 }}>
          <Text style={{ color: '#ffe2e2', fontSize: 12 }}>{formatUiError(videoError)}</Text>
        </View>
      )}
    </View>
  );
});

const VideoPreloader = React.memo(function VideoPreloader({ uri }: { uri: string }) {
  const player = useVideoPlayer(uri, (player) => {
    player.muted = true;
    try {
      player.play();
      setTimeout(() => {
        try {
          player.pause();
        } catch {
          // ignore preload pause failures
        }
      }, 900);
    } catch {
      // ignore preload start failures
    }
  });

  return (
    <VideoView
      style={styles.hiddenVideoPreload}
      player={player}
      contentFit="contain"
    />
  );
});

const EMBED_VARIANT_ORDER: YoutubeEmbedMode[] = ["origin", "plain", "nocookie"];

const StockYoutubeEmbed = React.memo(function StockYoutubeEmbed({
  fullscreen = false,
  youtubeEmbedUrl = null,
  onReady,
  onFatalError,
}: {
  fullscreen?: boolean;
  youtubeEmbedUrl?: string | null;
  onReady?: () => void;
  onFatalError?: (message: string) => void;
}) {
  const [embedVariantIndex, setEmbedVariantIndex] = useState(0);
  const [reloadNonce, setReloadNonce] = useState(0);
  const [embedLoading, setEmbedLoading] = useState(Boolean(youtubeEmbedUrl));
  const [embedLoadError, setEmbedLoadError] = useState<string | null>(null);
  const [embedPlayable, setEmbedPlayable] = useState(false);
  const lastLoadFailedRef = useRef(false);
  const loadTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const recoveryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const fatalNotifiedRef = useRef(false);
  const readyNotifiedRef = useRef(false);
  const processingRetryCountRef = useRef(0);
  const bridgeScript = useMemo(() => buildYoutubeEmbedBridgeScript(), []);

  const clearLoadTimeout = useCallback(() => {
    if (!loadTimeoutRef.current) return;
    clearTimeout(loadTimeoutRef.current);
    loadTimeoutRef.current = null;
  }, []);

  const clearRecoveryTimer = useCallback(() => {
    if (!recoveryTimerRef.current) return;
    clearTimeout(recoveryTimerRef.current);
    recoveryTimerRef.current = null;
  }, []);

  const notifyFatalError = useCallback(
    (message: string) => {
      if (fatalNotifiedRef.current) return;
      fatalNotifiedRef.current = true;
      onFatalError?.(message);
    },
    [onFatalError]
  );

  const maxVariantIndex = Math.max(
    0,
    Math.min(VIDEO_EMBED_MAX_RETRY_VARIANTS, EMBED_VARIANT_ORDER.length) - 1
  );
  const embedVariant = EMBED_VARIANT_ORDER[Math.min(embedVariantIndex, maxVariantIndex)] || "origin";
  const resolvedEmbedUrl = useMemo(
    () => buildYoutubeEmbedUrl(youtubeEmbedUrl, { autoplay: true, mode: embedVariant }) || null,
    [embedVariant, youtubeEmbedUrl]
  );

  const advanceVariantOrFail = useCallback(
    (reason: string) => {
      clearLoadTimeout();
      clearRecoveryTimer();
      const nextVariant = embedVariantIndex + 1;
      lastLoadFailedRef.current = true;
      processingRetryCountRef.current = 0;
      if (nextVariant <= maxVariantIndex) {
        debugWarn("[youtube] retrying embed variant", { reason, nextVariant });
        setEmbedVariantIndex(nextVariant);
        setReloadNonce((prev) => prev + 1);
        setEmbedLoading(true);
        setEmbedLoadError(null);
        setEmbedPlayable(false);
        return;
      }
      setEmbedLoading(false);
      setEmbedLoadError(VIDEO_LOAD_RECOVERY_MESSAGE);
      setEmbedPlayable(false);
      notifyFatalError(VIDEO_LOAD_RECOVERY_MESSAGE);
    },
    [clearLoadTimeout, clearRecoveryTimer, embedVariantIndex, maxVariantIndex, notifyFatalError]
  );

  const armLoadTimeout = useCallback(() => {
    clearLoadTimeout();
    if (!resolvedEmbedUrl) return;
    loadTimeoutRef.current = setTimeout(() => {
      advanceVariantOrFail("load timeout");
    }, VIDEO_EMBED_WEBVIEW_LOAD_TIMEOUT_MS);
  }, [advanceVariantOrFail, clearLoadTimeout, resolvedEmbedUrl]);

  const scheduleProcessingReload = useCallback(
    (reason: string) => {
      if (readyNotifiedRef.current) return;
      if (recoveryTimerRef.current) return;
      const nextAttempt = processingRetryCountRef.current + 1;
      if (nextAttempt > VIDEO_EMBED_MAX_PROCESSING_RETRIES_PER_VARIANT) {
        advanceVariantOrFail(`${reason} exhausted`);
        return;
      }
      processingRetryCountRef.current = nextAttempt;
      debugWarn("[youtube] waiting for playable embed, reloading", {
        reason,
        attempt: nextAttempt,
        variant: embedVariant,
      });
      recoveryTimerRef.current = setTimeout(() => {
        recoveryTimerRef.current = null;
        if (readyNotifiedRef.current) return;
        setEmbedPlayable(false);
        setEmbedLoading(true);
        setEmbedLoadError(null);
        setReloadNonce((prev) => prev + 1);
      }, VIDEO_EMBED_PROCESSING_RELOAD_DELAY_MS);
    },
    [advanceVariantOrFail, embedVariant]
  );

  const handleBridgeMessage = useCallback(
    (event: any) => {
      const rawData = String(event?.nativeEvent?.data || "").trim();
      if (!rawData) return;
      let payload: any = null;
      try {
        payload = JSON.parse(rawData);
      } catch {
        return;
      }
      if (String(payload?.source || "") !== "mixterious-youtube-bridge") return;
      const signal = String(payload?.type || "").trim().toLowerCase();
      if (!signal) return;
      if (signal === "ready") {
        if (readyNotifiedRef.current) return;
        clearLoadTimeout();
        clearRecoveryTimer();
        readyNotifiedRef.current = true;
        processingRetryCountRef.current = 0;
        setEmbedPlayable(true);
        setEmbedLoading(false);
        setEmbedLoadError(null);
        onReady?.();
        return;
      }
      if (readyNotifiedRef.current) return;
      if (signal === "processing" || signal === "unavailable") {
        setEmbedPlayable(false);
        setEmbedLoading(true);
        setEmbedLoadError(null);
        scheduleProcessingReload(signal);
      }
    },
    [clearLoadTimeout, clearRecoveryTimer, onReady, scheduleProcessingReload]
  );

  const resetEmbedFromStart = useCallback(() => {
    fatalNotifiedRef.current = false;
    readyNotifiedRef.current = false;
    lastLoadFailedRef.current = false;
    processingRetryCountRef.current = 0;
    clearLoadTimeout();
    clearRecoveryTimer();
    setEmbedVariantIndex(0);
    setReloadNonce((prev) => prev + 1);
    setEmbedLoadError(null);
    setEmbedLoading(Boolean(youtubeEmbedUrl));
    setEmbedPlayable(false);
  }, [clearLoadTimeout, clearRecoveryTimer, youtubeEmbedUrl]);

  useEffect(() => {
    fatalNotifiedRef.current = false;
    readyNotifiedRef.current = false;
    lastLoadFailedRef.current = false;
    processingRetryCountRef.current = 0;
    clearLoadTimeout();
    clearRecoveryTimer();
    setEmbedVariantIndex(0);
    setReloadNonce(0);
    setEmbedLoadError(null);
    setEmbedLoading(Boolean(youtubeEmbedUrl));
    setEmbedPlayable(false);
  }, [clearLoadTimeout, clearRecoveryTimer, youtubeEmbedUrl]);

  useEffect(() => {
    if (!youtubeEmbedUrl || resolvedEmbedUrl) return;
    setEmbedLoading(false);
    setEmbedLoadError(VIDEO_LOAD_RECOVERY_MESSAGE);
    setEmbedPlayable(false);
    notifyFatalError(VIDEO_LOAD_RECOVERY_MESSAGE);
  }, [notifyFatalError, resolvedEmbedUrl, youtubeEmbedUrl]);

  useEffect(() => {
    return () => {
      clearLoadTimeout();
      clearRecoveryTimer();
    };
  }, [clearLoadTimeout, clearRecoveryTimer]);

  const usesOriginHeaders = embedVariant === "origin";
  const embedStatusText = formatUiStatus(PROCESSING_REQUEST_STATUS_MESSAGE);
  const embedErrorText = formatUiError(embedLoadError || GENERIC_ERROR_FALLBACK_MESSAGE);

  if (!resolvedEmbedUrl) {
    return (
      <View style={[styles.youtubeEmbedWrap, fullscreen && styles.youtubeEmbedWrapFullscreen]}>
        <View style={[styles.youtubeEmbed, fullscreen && styles.youtubeEmbedFullscreen, styles.youtubeEmbedFallback]}>
          <Text style={embedLoadError ? styles.error : styles.smallStatus}>
            {embedLoadError ? embedErrorText : embedStatusText}
          </Text>
          {embedLoadError ? (
            <Pressable
              style={styles.youtubeEmbedRetryButton}
              onPress={resetEmbedFromStart}
            >
              <Text style={styles.youtubeEmbedRetryButtonText}>Retry Request</Text>
            </Pressable>
          ) : null}
        </View>
      </View>
    );
  }

  return (
    <View style={[styles.youtubeEmbedWrap, fullscreen && styles.youtubeEmbedWrapFullscreen]}>
      <WebView
        key={`${resolvedEmbedUrl}|${reloadNonce}`}
        style={[
          styles.youtubeEmbed,
          fullscreen && styles.youtubeEmbedFullscreen,
          !embedPlayable && styles.youtubeEmbedHidden,
        ]}
        source={
          usesOriginHeaders
            ? {
                uri: resolvedEmbedUrl,
                headers: {
                  Referer: YOUTUBE_EMBED_ORIGIN,
                  Origin: YOUTUBE_EMBED_ORIGIN,
                },
              }
            : { uri: resolvedEmbedUrl }
        }
        originWhitelist={["https://*"]}
        mediaPlaybackRequiresUserAction={false}
        allowsInlineMediaPlayback
        allowsFullscreenVideo
        javaScriptEnabled
        domStorageEnabled
        scrollEnabled={false}
        injectedJavaScriptBeforeContentLoaded={bridgeScript}
        injectedJavaScript={bridgeScript}
        onMessage={handleBridgeMessage}
        onLoadStart={() => {
          lastLoadFailedRef.current = false;
          setEmbedPlayable(false);
          setEmbedLoading(true);
          setEmbedLoadError(null);
          clearRecoveryTimer();
          armLoadTimeout();
        }}
        onLoadEnd={() => {
          clearLoadTimeout();
          if (lastLoadFailedRef.current || readyNotifiedRef.current) return;
          setEmbedPlayable(false);
          setEmbedLoading(true);
          scheduleProcessingReload("waiting_for_playable_signal");
        }}
        onHttpError={(event: any) => {
          const code = Number(event?.nativeEvent?.statusCode);
          const desc = String(event?.nativeEvent?.description || "").trim();
          const reason = Number.isFinite(code) ? `http ${code}` : desc || "http error";
          advanceVariantOrFail(reason);
        }}
        onError={(event: any) => {
          const desc = String(event?.nativeEvent?.description || "").trim();
          const code = Number(event?.nativeEvent?.code);
          const reason = Number.isFinite(code) ? `webview ${code}` : desc || "webview error";
          advanceVariantOrFail(reason);
        }}
      />
      {embedLoading && !embedLoadError ? (
        <View style={styles.youtubeEmbedOverlay}>
          <ActivityIndicator size="small" color="#f2f4f8" />
          <Text style={styles.youtubeEmbedOverlayText}>{embedStatusText}</Text>
        </View>
      ) : null}
      {embedLoadError ? (
        <View style={styles.youtubeEmbedOverlay}>
          <Text style={[styles.youtubeEmbedOverlayText, styles.youtubeEmbedOverlayErrorText]}>
            {embedErrorText}
          </Text>
          <Pressable
            style={styles.youtubeEmbedRetryButton}
            onPress={resetEmbedFromStart}
          >
            <Text style={styles.youtubeEmbedRetryButtonText}>Retry Request</Text>
          </Pressable>
        </View>
      ) : null}
    </View>
  );
});

type PipelineChecklistProps = {
  job: JobStatus | null;
  startAt: number | null;
  endAt: number | null;
  emptyMessage: string;
  preflightQueryActive?: boolean;
  preflightQueryElapsedMs?: number | null;
  showTimers?: boolean;
  hideTotal?: boolean;
};

const PipelineChecklist = React.memo(function PipelineChecklist({
  job,
  startAt,
  endAt,
  emptyMessage,
  preflightQueryActive = false,
  preflightQueryElapsedMs = null,
  showTimers = true,
  hideTotal = false,
}: PipelineChecklistProps) {
  const rows = useMemo(
    () => buildPipelineChecklistRows(job, { preflightQueryActive, preflightQueryElapsedMs }),
    [job, preflightQueryActive, preflightQueryElapsedMs]
  );
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  useEffect(() => {
    if (!startAt || endAt) return;
    const timer = setInterval(() => {
      setNowMs(Date.now());
    }, ELAPSED_TIMER_TICK_MS);
    return () => clearInterval(timer);
  }, [startAt, endAt]);

  useEffect(() => {
    setNowMs(Date.now());
  }, [startAt, endAt, job?.id, job?.status, job?.stage, job?.last_updated_at, preflightQueryActive]);

  const wallClockElapsedMs = useMemo(() => {
    if (!startAt) return null;
    const end = endAt ?? nowMs;
    return Math.max(0, end - startAt);
  }, [startAt, endAt, nowMs]);

  const activeRowsCount = useMemo(
    () => rows.filter((row) => row.status === "active").length,
    [rows]
  );

  const completedRowsElapsedMs = useMemo(
    () =>
      rows.reduce((sum, row) => {
        if (row.elapsedMs == null) return sum;
        return sum + row.elapsedMs;
      }, 0),
    [rows]
  );

  const activeRowsCarryMs = useMemo(() => {
    if (wallClockElapsedMs == null || activeRowsCount <= 0) return 0;
    return Math.max(0, wallClockElapsedMs - completedRowsElapsedMs) / activeRowsCount;
  }, [wallClockElapsedMs, activeRowsCount, completedRowsElapsedMs]);

  const displayElapsedMs = useCallback(
    (row: PipelineChecklistRow): number | null => {
      if (row.status !== "active") return row.elapsedMs;
      if (wallClockElapsedMs == null) return row.elapsedMs;
      const base = Math.max(0, row.elapsedMs ?? 0);
      const live = base + activeRowsCarryMs;
      return live > 0 ? live : null;
    },
    [activeRowsCarryMs, wallClockElapsedMs]
  );

  const pipelineTotalMs = useMemo(
    () => getPipelineTotalElapsedMs(job?.timing_breakdown, rows),
    [job?.timing_breakdown, rows]
  );

  if (!job && !preflightQueryActive) {
    return <Text style={styles.modalMessage}>{emptyMessage}</Text>;
  }

  return (
    <View style={styles.pipelineChecklist}>
      <View style={styles.pipelineChecklistRows}>
        {rows.map((row) => {
          const rowElapsedMs = displayElapsedMs(row);
          return (
            <View
              key={row.key}
              style={[
                styles.pipelineChecklistRow,
                row.status === "completed" && styles.pipelineChecklistRowCompleted,
                row.status === "active" && styles.pipelineChecklistRowActive,
              ]}
            >
              <View style={styles.pipelineChecklistLabelWrap}>
                <View style={styles.pipelineChecklistLeading}>
                  {row.status === "active" ? (
                    <ActivityIndicator size="small" color="#a4d4ff" />
                  ) : (
                    <Text style={styles.pipelineChecklistLeadingEmoji}>
                      {row.status === "completed" ? "✅" : "⬜️"}
                    </Text>
                  )}
                </View>
                <Text style={styles.pipelineChecklistLabel}>{row.label}</Text>
              </View>
              {showTimers ? (
                <Text style={styles.pipelineChecklistValue}>
                  {rowElapsedMs != null
                    ? formatElapsed(rowElapsedMs / 1000)
                    : row.status === "completed"
                      ? "done"
                      : row.status === "active"
                        ? "running"
                        : "pending"}
                </Text>
              ) : null}
            </View>
          );
        })}
      </View>
      {showTimers && !hideTotal ? (
        <Text style={styles.pipelineTotalText}>
          Elapsed:{" "}
          {startAt ? (
            <ElapsedTimerText
              startAt={startAt}
              endAt={endAt}
              placeholder={pipelineTotalMs != null ? formatElapsed(pipelineTotalMs / 1000) : "0.0s"}
              style={styles.pipelineTotalValue}
            />
          ) : pipelineTotalMs != null ? (
            <Text style={styles.pipelineTotalValue}>{formatElapsed(pipelineTotalMs / 1000)}</Text>
          ) : (
            <Text style={styles.pipelineTotalValue}>0.0s</Text>
          )}
        </Text>
      ) : null}
    </View>
  );
});

type ProcessingModalProps = {
  visible: boolean;
  job: JobStatus | null;
  queryNormalizing?: boolean;
  queryPreflightElapsedMs?: number | null;
  startAt: number | null;
  endAt: number | null;
  showTimers?: boolean;
  onDismiss: () => void;
  onCancel: () => void;
};

const ProcessingModal = React.memo(function ProcessingModal({
  visible,
  job,
  queryNormalizing = false,
  queryPreflightElapsedMs = null,
  startAt,
  endAt,
  showTimers = true,
  onDismiss,
  onCancel,
}: ProcessingModalProps) {
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (!visible) {
      setTick(0);
      return;
    }
    setTick(0);
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let currentTick = 0;

    const scheduleNextTick = () => {
      if (cancelled) return;
      if (currentTick >= PROCESSING_MODAL_MAX_TICK) return;
      const delayMs =
        PROCESSING_MODAL_DOT_STEP_MIN_MS +
        Math.floor(
          Math.random() * (PROCESSING_MODAL_DOT_STEP_MAX_MS - PROCESSING_MODAL_DOT_STEP_MIN_MS + 1)
        );
      timer = setTimeout(() => {
        if (cancelled) return;
        if (currentTick >= PROCESSING_MODAL_MAX_TICK) return;
        currentTick += 1;
        setTick(currentTick);
        scheduleNextTick();
      }, delayMs);
    };

    scheduleNextTick();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [visible]);

  if (!visible) return null;
  const safeTick = Math.max(0, Math.min(PROCESSING_MODAL_MAX_TICK, tick));
  const dotCount = safeTick % PROCESSING_MODAL_STEPS_PER_STAGE;
  const stageIndex = Math.floor(safeTick / PROCESSING_MODAL_STEPS_PER_STAGE);
  const stage = PROCESSING_MODAL_STAGES[stageIndex] || PROCESSING_MODAL_STAGES[0];
  const liveStageText = formatStage(job?.stage || job?.status || "");
  const progressPercent = Number(job?.progress_percent);
  const animatedTitle = `${liveStageText || stage.title}${".".repeat(dotCount)}`;
  const animatedSubtext = String(job?.last_message || "").trim() || stage.subtext;

  return (
    <Modal visible transparent animationType="fade" onRequestClose={onDismiss}>
      <View style={styles.modalBackdrop}>
        <View style={[styles.modalCard, styles.processingScreenCard, styles.processingSpinnerCard]}>
          <ActivityIndicator size="large" color="#f2f4f8" />
          <Text style={styles.processingSpinnerTitle}>{animatedTitle}</Text>
          <ElapsedTimerText
            startAt={startAt}
            endAt={endAt}
            placeholder="00:00"
            style={styles.processingElapsedText}
          />
          {Number.isFinite(progressPercent) ? (
            <Text style={styles.modalStage}>Progress {Math.max(0, Math.round(progressPercent))}%</Text>
          ) : liveStageText ? (
            <Text style={styles.modalStage}>{liveStageText}</Text>
          ) : null}
          <Text style={styles.processingSpinnerBody}>
            <Text style={styles.processingSpinnerBracket}>[</Text>
            {animatedSubtext}
            <Text style={styles.processingSpinnerBracket}>]</Text>
          </Text>
          {showTimers ? (
            <View style={styles.processingChecklistWrap}>
              <PipelineChecklist
                job={job}
                startAt={startAt}
                endAt={endAt}
                preflightQueryActive={queryNormalizing}
                preflightQueryElapsedMs={queryPreflightElapsedMs}
                showTimers
                emptyMessage="Waiting for timing data..."
              />
            </View>
          ) : null}
          <View style={styles.processingActionsRow}>
            <Pressable style={[styles.modalBackButton, styles.processingActionButton]} onPress={onDismiss}>
              <Text style={styles.secondaryButtonText}>Dismiss</Text>
            </Pressable>
            <Pressable style={[styles.modalBackButton, styles.processingActionButton]} onPress={onCancel}>
              <Text style={styles.secondaryButtonText}>Cancel</Text>
            </Pressable>
          </View>
        </View>
      </View>
    </Modal>
  );
});

type TimingBreakdownModalProps = {
  visible: boolean;
  job: JobStatus | null;
  query: string;
  normalizedSong?: NormalizedSong | null;
  queryNormalizing?: boolean;
  queryPreflightElapsedMs?: number | null;
  loading?: boolean;
  startAt: number | null;
  endAt: number | null;
  showTimers?: boolean;
  onClose: () => void;
  onCancel: () => void;
};

const TimingBreakdownModal = React.memo(function TimingBreakdownModal({
  visible,
  job,
  query,
  normalizedSong,
  queryNormalizing = false,
  queryPreflightElapsedMs = null,
  loading = false,
  startAt,
  endAt,
  showTimers = true,
  onClose,
  onCancel,
}: TimingBreakdownModalProps) {
  const shouldSuppressExistingJob =
    loading && (!job?.id || isJobTerminal(String(job?.status || "").trim().toLowerCase()));
  const showPreflightQueryStep = queryNormalizing || shouldSuppressExistingJob;
  const headingQuery = showPreflightQueryStep ? query : job?.query || query;
  const checklistJob = showPreflightQueryStep ? null : job;
  const liveStageText = formatStage(job?.stage || job?.status || "");
  const progressPercent = Number(job?.progress_percent);

  if (!visible) return null;

  return (
    <View style={styles.timingScreenOverlay} pointerEvents="auto">
      <SafeAreaView style={styles.timingScreenSafeArea}>
        <View style={styles.timingScreenHeader}>
          <TimingBreakdownHeading
            rawQuery={headingQuery}
            normalizedSong={normalizedSong}
            preferRawQuery={queryNormalizing}
          />
          <ElapsedTimerText
            startAt={startAt}
            endAt={endAt}
            placeholder="0.00s"
            precision={2}
            tickMs={PRECISE_ELAPSED_TIMER_TICK_MS}
            style={styles.timingScreenElapsedText}
          />
          {Number.isFinite(progressPercent) ? (
            <Text style={styles.timingScreenStageText}>Progress {Math.max(0, Math.round(progressPercent))}%</Text>
          ) : liveStageText ? (
            <Text style={styles.timingScreenStageText}>{liveStageText}</Text>
          ) : null}
        </View>
        <ScrollView
          style={styles.timingScreenScroll}
          contentContainerStyle={styles.timingScreenScrollContent}
          showsVerticalScrollIndicator={false}
        >
          <PipelineChecklist
            job={checklistJob}
            startAt={startAt}
            endAt={endAt}
            preflightQueryActive={showPreflightQueryStep}
            preflightQueryElapsedMs={queryPreflightElapsedMs}
            showTimers={showTimers}
            hideTotal
            emptyMessage="No timing data yet, start a job to populate timings"
          />
        </ScrollView>
        <View style={styles.timingScreenActions}>
          <Pressable style={[styles.modalBackButton, styles.timingScreenActionButton]} onPress={onClose}>
            <Text style={styles.secondaryButtonText}>Hide</Text>
          </Pressable>
          {isJobInProgress(job?.status) ? (
            <Pressable style={[styles.modalBackButton, styles.timingScreenActionButton]} onPress={onCancel}>
              <Text style={styles.secondaryButtonText}>Cancel</Text>
            </Pressable>
          ) : null}
        </View>
      </SafeAreaView>
    </View>
  );
});

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("search");
  const { width, height } = useWindowDimensions();
  const isLandscape = width > height;
  const isDemoMode = useMemo(() => resolveDemoMode(), []);
  const queryInputRef = useRef<TextInput | null>(null);
  const primaryBaseUrl = useMemo(() => resolveApiBaseUrl(), []);
  const [baseUrl, setBaseUrl] = useState(primaryBaseUrl);
  const apiFailCountRef = useRef(0);
  const apiHealthCheckPromiseRef = useRef<Promise<boolean> | null>(null);
  useEffect(() => {
    debugLog("[app] baseUrl resolved", { baseUrl });
  }, [baseUrl]);
  const [query, setQuery] = useState(() => (isDemoMode ? DEMO_QUERY_VALUE : ""));
  const [globalOffsetSec, setGlobalOffsetSec] = useState(0);
  const [mixLevels, setMixLevels] = useState({ ...DEFAULT_MIX_LEVELS });
  const [job, setJob] = useState<JobStatus | null>(null);
  const [jobHistory, setJobHistory] = useState<JobStatus[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [processingVisible, setProcessingVisible] = useState(false);
  const [isQueryNormalizing, setIsQueryNormalizing] = useState(false);
  const [queryPreflightElapsedMs, setQueryPreflightElapsedMs] = useState<number | null>(null);
  const [timingModalVisible, setTimingModalVisible] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [advancedLastUsedOpen, setAdvancedLastUsedOpen] = useState(false);
  const [advancedDefaultMode, setAdvancedDefaultMode] = useState<AdvancedDefaultMode>(DEFAULT_ADVANCED_DEFAULT_MODE);
  const [showAdvancedButton, setShowAdvancedButton] = useState(DEFAULT_SHOW_ADVANCED_BUTTON);
  const [presetQueriesOpen, setPresetQueriesOpen] = useState(false);
  const [presetQueriesUnlocked, setPresetQueriesUnlocked] = useState(false);
  const progressLogRef = useRef("");
  const jobPollInFlightRef = useRef(false);
  const jobPollEtagRef = useRef<Record<string, string>>({});
  const jobPollUnchangedStreakRef = useRef(0);
  const [stopwatchStartAt, setStopwatchStartAt] = useState<number | null>(null);
  const [stopwatchEndAt, setStopwatchEndAt] = useState<number | null>(null);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);
  const [isExporting, setIsExporting] = useState(false);
  const [showPhotoSettingsAction, setShowPhotoSettingsAction] = useState(false);
  const [notificationsEnabled, setNotificationsEnabled] = useState(false);
  const [notificationPermissionStatus, setNotificationPermissionStatus] =
    useState<NotificationPermissionStatus>("undetermined");
  const [notificationsPreferenceReady, setNotificationsPreferenceReady] = useState(false);
  const [notificationsStatusMessage, setNotificationsStatusMessage] = useState<string | null>(null);
  const [otaStatusMessage, setOtaStatusMessage] = useState<string | null>(null);
  const [otaChecking, setOtaChecking] = useState(false);
  const [otaUpdateReady, setOtaUpdateReady] = useState(false);
  const [pendingNormalizedSong, setPendingNormalizedSong] = useState<NormalizedSong | null>(null);
  const [recentNormalizedSongs, setRecentNormalizedSongs] = useState<RecentNormalizedSong[]>([]);
  const [showRecentNormalizedSongs, setShowRecentNormalizedSongs] = useState(true);
  const [showTimers, setShowTimers] = useState(DEFAULT_SHOW_TIMERS);
  const [backgroundVideoPlaybackEnabled, setBackgroundVideoPlaybackEnabled] = useState(true);
  const [autoSaveGeneratedVideos, setAutoSaveGeneratedVideos] = useState(false);
  const [searchAutoCorrectEnabled, setSearchAutoCorrectEnabled] = useState(false);
  const [mixLevelEditorStem, setMixLevelEditorStem] = useState<StemKey | null>(null);
  const [mixLevelEditorValue, setMixLevelEditorValue] = useState("");
  const [normalizedSongByJobId, setNormalizedSongByJobId] = useState<Record<string, NormalizedSong>>({});
  const [videoPaused, setVideoPaused] = useState(false);
  const [videoIsPlaying, setVideoIsPlaying] = useState(false);
  const [videoSurfaceNonce, setVideoSurfaceNonce] = useState(0);
  const previewPlaybackTimeRef = useRef(0);
  const pendingPreviewToFinalSeekRef = useRef<number | null>(null);
  const shouldAutoplayAfterSourceChangeRef = useRef(true);
  const lastVideoSourceRef = useRef<{ outputUrl: string | null; isPreview: boolean }>({
    outputUrl: null,
    isPreview: false,
  });
  const [appIsActive, setAppIsActive] = useState(() => AppState.currentState === "active");
  const prevAppIsActiveRef = useRef(AppState.currentState === "active");
  const lastBackgroundedAtRef = useRef<number | null>(null);
  const notificationVideoOpenRef = useRef(false);
  const autoSaveHandledKeysRef = useRef<Set<string>>(new Set());
  const [isOnline, setIsOnline] = useState(true);
  const [apiReachable, setApiReachable] = useState<boolean | null>(null);
  const [, setApiLastError] = useState<string | null>(null);
  const [apiFailCount, setApiFailCount] = useState(0);
  const [sourcePickerVisible, setSourcePickerVisible] = useState(false);
  const [sourcePickerLoading, setSourcePickerLoading] = useState(false);
  const [sourcePickerError, setSourcePickerError] = useState<string | null>(null);
  const [sourceSearchResults, setSourceSearchResults] = useState<SourceSearchResult[]>([]);
  const [latestUploadedYoutubeUrl, setLatestUploadedYoutubeUrl] = useState<string | null>(null);
  const [youtubeEmbedReady, setYoutubeEmbedReady] = useState(false);
  const activeJobIdRef = useRef<string | null>(null);
  const pendingPresetVideoAutoOpenRef = useRef(false);
  const pendingEmbedWaitStartAtRef = useRef<number | null>(null);
  const pendingEmbedRefreshAttemptsRef = useRef(0);
  const missingEmbedHydrationAttemptsRef = useRef(0);
  const processingVisibleSinceRef = useRef<number | null>(null);
  const processingStaleRefreshAtRef = useRef(0);
  const sourceSearchRequestTokenRef = useRef(0);
  const globalOffsetPersistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mixLevelsPersistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const smokeTestStartedRef = useRef(false);
  const requestStartJobTokenRef = useRef(0);
  const exportActionInFlightRef = useRef(false);
  const mixLevelInputRef = useRef<TextInput | null>(null);
  const otaCheckInFlightRef = useRef(false);
  const otaLastCheckAtRef = useRef(0);
  // No local storage or admin settings in alpha.

  const backendMode = useMemo(() => detectBackendMode(baseUrl), [baseUrl]);
  const isSimpleLocalBackend = backendMode === "local_cli";
  const prefersDirectPlayback = useMemo(() => shouldPreferDirectPlaybackForBaseUrl(baseUrl), [baseUrl]);
  const shouldRequestYoutubeUpload = useMemo(() => shouldRequestYoutubeUploadForBaseUrl(baseUrl), [baseUrl]);
  const otaChannel = useMemo(() => {
    const runtimeChannel = String((Updates as any)?.channel || "").trim();
    if (runtimeChannel) return runtimeChannel;
    const configuredChannel = String((Constants.expoConfig?.extra as any)?.ota?.channel || "").trim();
    return configuredChannel || "production";
  }, []);
  const otaConfig = useMemo(() => {
    const otaExtra = (Constants.expoConfig?.extra as any)?.ota || {};
    const configuredEnabled = parseBoolFlag(otaExtra.enabled);
    const autoCheckOnForeground =
      otaExtra.autoCheckOnForeground === undefined
        ? OTA_AUTO_CHECK_DEFAULT
        : parseBoolFlag(otaExtra.autoCheckOnForeground);
    const nativeUpdatesEnabled = Platform.OS !== "web" && Boolean((Updates as any)?.isEnabled);
    return {
      enabled: configuredEnabled && nativeUpdatesEnabled,
      autoCheckOnForeground,
    };
  }, []);
  const applyDownloadedOtaUpdate = useCallback(() => {
    if (!otaConfig.enabled || !otaUpdateReady) return;
    setOtaStatusMessage(formatUiStatus("Restarting to apply update"));
    Updates.reloadAsync().catch(() => {
      setOtaStatusMessage(formatUiError("Unable to restart automatically, close and reopen Mixterious"));
    });
  }, [otaConfig.enabled, otaUpdateReady]);

  const checkForOtaUpdate = useCallback(
    async ({
      manual = false,
      reason = "manual",
    }: {
      manual?: boolean;
      reason?: string;
    } = {}) => {
      if (!otaConfig.enabled) {
        if (manual) {
          setOtaStatusMessage(formatUiStatus("OTA updates are unavailable for this build"));
        }
        return false;
      }
      if (otaUpdateReady) return false;
      if (otaCheckInFlightRef.current) return false;

      const now = Date.now();
      if (!manual && now - otaLastCheckAtRef.current < OTA_UPDATE_CHECK_COOLDOWN_MS) return false;

      otaCheckInFlightRef.current = true;
      if (manual) {
        setOtaChecking(true);
        setOtaStatusMessage(formatUiStatus("Checking for app updates"));
      }

      try {
        debugLog("[ota] checking for updates", {
          reason,
          channel: otaChannel,
          runtimeVersion: Updates.runtimeVersion,
        });
        const updateCheck = await Updates.checkForUpdateAsync();
        if (!updateCheck?.isAvailable) {
          if (manual) {
            setOtaStatusMessage(formatUiStatus("You're running the latest update"));
          }
          return false;
        }

        setOtaStatusMessage(formatUiStatus("Update found, downloading now"));
        await Updates.fetchUpdateAsync();
        setOtaUpdateReady(true);
        setOtaStatusMessage(formatUiStatus("Update downloaded, tap Apply Downloaded Update to restart"));

        if (manual) {
          Alert.alert(REQUEST_RECEIVED_STATUS_MESSAGE, PROCESSING_REQUEST_STATUS_MESSAGE, [
            { text: "Later", style: "cancel" },
            { text: "Restart", onPress: () => applyDownloadedOtaUpdate() },
          ]);
        }
        return true;
      } catch (err: any) {
        debugWarn("[ota] update check failed", {
          reason,
          message: err?.message || String(err),
        });
        if (manual) {
          setOtaStatusMessage(formatUiError("Unable to check for updates right now"));
        }
        return false;
      } finally {
        otaLastCheckAtRef.current = Date.now();
        otaCheckInFlightRef.current = false;
        if (manual) setOtaChecking(false);
      }
    },
    [applyDownloadedOtaUpdate, otaChannel, otaConfig.enabled, otaUpdateReady]
  );

  const focusQueryInput = useCallback((delayMs = 80) => {
    if (isDemoMode) return;
    setTimeout(() => {
      queryInputRef.current?.focus();
    }, delayMs);
  }, [isDemoMode]);

  const clearPendingPresetAutoOpenState = useCallback(() => {
    pendingPresetVideoAutoOpenRef.current = false;
    pendingEmbedWaitStartAtRef.current = null;
    pendingEmbedRefreshAttemptsRef.current = 0;
    missingEmbedHydrationAttemptsRef.current = 0;
  }, []);

  const isSnapshotForActiveJob = useCallback((jobId: string | null | undefined) => {
    const incoming = String(jobId || "").trim();
    if (!incoming) return false;
    const active = String(activeJobIdRef.current || "").trim();
    if (!active) return true;
    return active === incoming;
  }, []);

  useEffect(() => {
    const nextJobId = String(job?.id || "").trim();
    if (nextJobId) {
      activeJobIdRef.current = nextJobId;
      return;
    }
    const active = String(activeJobIdRef.current || "").trim();
    if (!active.startsWith("pending:")) {
      activeJobIdRef.current = null;
    }
  }, [job?.id]);

  const resetUiState = useCallback(
    ({ clearQuery = false }: { clearQuery?: boolean } = {}) => {
      requestStartJobTokenRef.current += 1;
      setLoading(false);
      setProcessingVisible(false);
      clearPendingPresetAutoOpenState();
      setIsQueryNormalizing(false);
      setQueryPreflightElapsedMs(null);
      setTimingModalVisible(false);
      setSaveStatus(null);
      setIsExporting(false);
      setShowPhotoSettingsAction(false);
      exportActionInFlightRef.current = false;
      activeJobIdRef.current = null;
      setError(null);
      setJob(null);
      setLatestUploadedYoutubeUrl(null);
      setStopwatchStartAt(null);
      setStopwatchEndAt(null);
      setSourcePickerVisible(false);
      setSourcePickerError(null);
      setSourcePickerLoading(false);
      setSourceSearchResults([]);
      setPendingNormalizedSong(null);
      setActiveTab("search");
      if (clearQuery) setQuery(isDemoMode ? DEMO_QUERY_VALUE : "");
      focusQueryInput();
    },
    [clearPendingPresetAutoOpenState, focusQueryInput, isDemoMode]
  );

  const activeNormalizedSong = useMemo<NormalizedSong | null>(() => {
    if (job?.id) return normalizedSongByJobId[job.id] || null;
    return pendingNormalizedSong;
  }, [job?.id, normalizedSongByJobId, pendingNormalizedSong]);

  const persistRecentNormalizedSongs = useCallback((rows: RecentNormalizedSong[]) => {
    AsyncStorage.setItem(
      RECENT_NORMALIZED_SONGS_STORAGE_KEY,
      JSON.stringify(rows.slice(0, RECENT_NORMALIZED_SONGS_MAX))
    ).catch(() => undefined);
  }, []);

  const rememberRecentNormalizedSong = useCallback(
    (song: RecentNormalizedSong) => {
      const queryText = cleanQuotedText(song.query);
      const artistText = cleanQuotedText(song.artist);
      const titleText = cleanQuotedText(song.title);
      if (!queryText || !artistText || !titleText) return;
      setRecentNormalizedSongs((prev) => {
        const next = [
          { query: queryText, artist: artistText, title: titleText },
          ...prev.filter((row) => {
            const sameQuery = row.query.toLowerCase() === queryText.toLowerCase();
            const sameSong =
              row.artist.toLowerCase() === artistText.toLowerCase() &&
              row.title.toLowerCase() === titleText.toLowerCase();
            return !sameQuery && !sameSong;
          }),
        ].slice(0, RECENT_NORMALIZED_SONGS_MAX);
        persistRecentNormalizedSongs(next);
        return next;
      });
    },
    [persistRecentNormalizedSongs]
  );

  const rememberNormalizedSongFromSource = useCallback(
    (rawQuery: string) => {
      const queryText = cleanQuotedText(String(rawQuery || ""));
      if (!queryText) return;
      fetchWithTimeout(
        `${baseUrl}/source/normalize?q=${encodeURIComponent(queryText)}`,
        {},
        8000
      )
        .then(async (res) => {
          if (!res.ok) return;
          const payload = (await res.json()) as {
            artist?: string;
            track?: string;
            title?: string;
            normalized_query?: string;
            display?: string;
          };
          const artist = cleanQuotedText(String(payload?.artist || ""));
          const title = cleanQuotedText(String(payload?.track || payload?.title || ""));
          if (!artist || !title) return;
          const normalizedQuery = cleanQuotedText(String(payload?.normalized_query || payload?.display || ""));
          rememberRecentNormalizedSong({
            query: normalizedQuery || `${artist} - ${title}`,
            artist,
            title,
          });
        })
        .catch(() => undefined);
    },
    [baseUrl, rememberRecentNormalizedSong]
  );

  useEffect(() => {
    if (!isDemoMode) return;
    setQuery(DEMO_QUERY_VALUE);
  }, [isDemoMode]);

  useEffect(() => {
    const loadPrefs = async () => {
      const [
        rawOffset,
        rawMixLevels,
        rawAdvancedOpen,
        rawAdvancedLastUsedOpen,
        rawAdvancedDefaultMode,
        rawAdvancedButtonVisible,
        rawRecentNormalizedSongs,
        rawShowRecentNormalizedSongs,
        rawShowTimers,
        rawBackgroundVideoPlaybackEnabled,
        rawAutoSaveGeneratedVideos,
        rawSearchAutoCorrectEnabled,
      ] = await Promise.all([
        AsyncStorage.getItem(GLOBAL_OFFSET_STORAGE_KEY),
        AsyncStorage.getItem(MIX_LEVELS_STORAGE_KEY),
        AsyncStorage.getItem(ADVANCED_OPEN_STORAGE_KEY),
        AsyncStorage.getItem(ADVANCED_LAST_USED_OPEN_STORAGE_KEY),
        AsyncStorage.getItem(ADVANCED_DEFAULT_MODE_STORAGE_KEY),
        AsyncStorage.getItem(ADVANCED_BUTTON_VISIBLE_STORAGE_KEY),
        AsyncStorage.getItem(RECENT_NORMALIZED_SONGS_STORAGE_KEY),
        AsyncStorage.getItem(SHOW_RECENT_NORMALIZED_SONGS_STORAGE_KEY),
        AsyncStorage.getItem(SHOW_TIMERS_STORAGE_KEY),
        AsyncStorage.getItem(BACKGROUND_VIDEO_PLAYBACK_STORAGE_KEY),
        AsyncStorage.getItem(AUTO_SAVE_GENERATED_VIDEOS_STORAGE_KEY),
        AsyncStorage.getItem(SEARCH_AUTO_CORRECT_STORAGE_KEY),
      ]);

      const offset = Number(rawOffset ?? "0");
      if (Number.isFinite(offset)) setGlobalOffsetSec(offset);

      if (rawMixLevels) {
        try {
          const parsed = JSON.parse(rawMixLevels) as Partial<{
            vocals: number;
            bass: number;
            drums: number;
            other: number;
          }>;
          setMixLevels((prev) => ({
            vocals: Number.isFinite(parsed.vocals) ? Math.max(0, Math.min(150, Math.round(parsed.vocals!))) : prev.vocals,
            bass: Number.isFinite(parsed.bass) ? Math.max(0, Math.min(150, Math.round(parsed.bass!))) : prev.bass,
            drums: Number.isFinite(parsed.drums) ? Math.max(0, Math.min(150, Math.round(parsed.drums!))) : prev.drums,
            other: Number.isFinite(parsed.other) ? Math.max(0, Math.min(150, Math.round(parsed.other!))) : prev.other,
          }));
        } catch {
          // ignore malformed mix prefs only
        }
      }

      const parsedAdvancedDefaultMode = parseAdvancedDefaultMode(rawAdvancedDefaultMode);
      const parsedAdvancedLastUsedOpen = (() => {
        const raw = rawAdvancedLastUsedOpen ?? rawAdvancedOpen;
        if (raw === "1" || raw === "0") return raw === "1";
        return false;
      })();
      const parsedShowAdvancedButton =
        rawAdvancedButtonVisible === "1" ? true : rawAdvancedButtonVisible === "0" ? false : DEFAULT_SHOW_ADVANCED_BUTTON;
      setAdvancedDefaultMode(parsedAdvancedDefaultMode);
      setAdvancedLastUsedOpen(parsedAdvancedLastUsedOpen);
      setShowAdvancedButton(parsedShowAdvancedButton);
      setAdvancedOpen(resolveAdvancedOpenFromMode(parsedAdvancedDefaultMode, parsedAdvancedLastUsedOpen));

      if (rawRecentNormalizedSongs) {
        try {
          const parsed = JSON.parse(rawRecentNormalizedSongs);
          setRecentNormalizedSongs(normalizeRecentNormalizedSongs(parsed));
        } catch {
          // ignore malformed recent song entries only
        }
      }
      if (rawShowRecentNormalizedSongs === "1" || rawShowRecentNormalizedSongs === "0") {
        setShowRecentNormalizedSongs(rawShowRecentNormalizedSongs === "1");
      }
      if (rawShowTimers === "1" || rawShowTimers === "0") {
        setShowTimers(rawShowTimers === "1");
      } else {
        setShowTimers(DEFAULT_SHOW_TIMERS);
      }
      if (rawBackgroundVideoPlaybackEnabled === "1" || rawBackgroundVideoPlaybackEnabled === "0") {
        setBackgroundVideoPlaybackEnabled(rawBackgroundVideoPlaybackEnabled === "1");
      }
      if (rawAutoSaveGeneratedVideos === "1" || rawAutoSaveGeneratedVideos === "0") {
        setAutoSaveGeneratedVideos(rawAutoSaveGeneratedVideos === "1");
      }
      if (rawSearchAutoCorrectEnabled === "1" || rawSearchAutoCorrectEnabled === "0") {
        setSearchAutoCorrectEnabled(rawSearchAutoCorrectEnabled === "1");
      }
    };
    loadPrefs().catch(() => undefined);
  }, []);

  useEffect(() => {
    if (globalOffsetPersistTimerRef.current) {
      clearTimeout(globalOffsetPersistTimerRef.current);
    }
    globalOffsetPersistTimerRef.current = setTimeout(() => {
      AsyncStorage.setItem(GLOBAL_OFFSET_STORAGE_KEY, String(globalOffsetSec)).catch(() => undefined);
      globalOffsetPersistTimerRef.current = null;
    }, 180);

    return () => {
      if (globalOffsetPersistTimerRef.current) {
        clearTimeout(globalOffsetPersistTimerRef.current);
        globalOffsetPersistTimerRef.current = null;
      }
    };
  }, [globalOffsetSec]);

  useEffect(() => {
    if (mixLevelsPersistTimerRef.current) {
      clearTimeout(mixLevelsPersistTimerRef.current);
    }
    mixLevelsPersistTimerRef.current = setTimeout(() => {
      AsyncStorage.setItem(MIX_LEVELS_STORAGE_KEY, JSON.stringify(mixLevels)).catch(() => undefined);
      mixLevelsPersistTimerRef.current = null;
    }, 180);

    return () => {
      if (mixLevelsPersistTimerRef.current) {
        clearTimeout(mixLevelsPersistTimerRef.current);
        mixLevelsPersistTimerRef.current = null;
      }
    };
  }, [mixLevels]);

  useEffect(() => {
    const value = advancedLastUsedOpen ? "1" : "0";
    AsyncStorage.setItem(ADVANCED_LAST_USED_OPEN_STORAGE_KEY, value).catch(() => undefined);
    // Keep legacy key in sync for backward compatibility with older builds.
    AsyncStorage.setItem(ADVANCED_OPEN_STORAGE_KEY, value).catch(() => undefined);
  }, [advancedLastUsedOpen]);

  useEffect(() => {
    AsyncStorage.setItem(ADVANCED_DEFAULT_MODE_STORAGE_KEY, advancedDefaultMode).catch(() => undefined);
  }, [advancedDefaultMode]);

  useEffect(() => {
    AsyncStorage.setItem(ADVANCED_BUTTON_VISIBLE_STORAGE_KEY, showAdvancedButton ? "1" : "0").catch(() => undefined);
  }, [showAdvancedButton]);

  useEffect(() => {
    AsyncStorage.setItem(SHOW_RECENT_NORMALIZED_SONGS_STORAGE_KEY, showRecentNormalizedSongs ? "1" : "0").catch(() => undefined);
  }, [showRecentNormalizedSongs]);

  useEffect(() => {
    AsyncStorage.setItem(SHOW_TIMERS_STORAGE_KEY, showTimers ? "1" : "0").catch(() => undefined);
  }, [showTimers]);

  useEffect(() => {
    AsyncStorage.setItem(
      BACKGROUND_VIDEO_PLAYBACK_STORAGE_KEY,
      backgroundVideoPlaybackEnabled ? "1" : "0"
    ).catch(() => undefined);
  }, [backgroundVideoPlaybackEnabled]);

  useEffect(() => {
    AsyncStorage.setItem(
      AUTO_SAVE_GENERATED_VIDEOS_STORAGE_KEY,
      autoSaveGeneratedVideos ? "1" : "0"
    ).catch(() => undefined);
  }, [autoSaveGeneratedVideos]);

  useEffect(() => {
    AsyncStorage.setItem(
      SEARCH_AUTO_CORRECT_STORAGE_KEY,
      searchAutoCorrectEnabled ? "1" : "0"
    ).catch(() => undefined);
  }, [searchAutoCorrectEnabled]);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      const [storedPreferenceRaw, firstLaunchPromptedRaw, currentPermission] = await Promise.all([
        AsyncStorage.getItem(NOTIFICATIONS_ENABLED_STORAGE_KEY),
        AsyncStorage.getItem(NOTIFICATIONS_FIRST_LAUNCH_PROMPTED_STORAGE_KEY),
        Notifications.getPermissionsAsync().catch(() => null),
      ]);

      let finalPermission = currentPermission;
      if (isSimpleLocalBackend) {
        AsyncStorage.setItem(NOTIFICATIONS_FIRST_LAUNCH_PROMPTED_STORAGE_KEY, "1").catch(() => undefined);
      } else if (!isDemoMode && firstLaunchPromptedRaw !== "1") {
        if (
          shouldPromptForNotificationPermission({
            enableNotifications: true,
            existingStatus: currentPermission?.status,
            canAskAgain: currentPermission?.canAskAgain,
          })
        ) {
          finalPermission = await Notifications.requestPermissionsAsync().catch(() => currentPermission);
        }
        AsyncStorage.setItem(NOTIFICATIONS_FIRST_LAUNCH_PROMPTED_STORAGE_KEY, "1").catch(() => undefined);
      }

      if (cancelled) return;

      setNotificationsEnabled(isSimpleLocalBackend ? false : storedPreferenceRaw === "1");
      setNotificationPermissionStatus(
        (finalPermission?.status as NotificationPermissionStatus | undefined) || "undetermined"
      );
      setNotificationsPreferenceReady(true);
    })().catch(() => {
      if (cancelled) return;
      setNotificationsPreferenceReady(true);
    });

    return () => {
      cancelled = true;
    };
  }, [isDemoMode, isSimpleLocalBackend]);

  useEffect(() => {
    if (isSimpleLocalBackend) return;
    if (!notificationsPreferenceReady) return;
    AsyncStorage.setItem(NOTIFICATIONS_ENABLED_STORAGE_KEY, notificationsEnabled ? "1" : "0").catch(() => undefined);
  }, [isSimpleLocalBackend, notificationsEnabled, notificationsPreferenceReady]);

  const adjustOffset = useCallback((delta: number) => {
    setGlobalOffsetSec((prev) => {
      const next = Math.round((prev + delta) * 100) / 100;
      return Math.max(-15, Math.min(15, next));
    });
  }, []);

  const adjustMixLevel = useCallback((stem: StemKey, delta: number) => {
    setMixLevels((prev) => {
      const nextValue = Math.max(0, Math.min(150, Math.round((prev[stem] ?? 100) + delta)));
      return { ...prev, [stem]: nextValue };
    });
  }, []);

  const openMixLevelEditor = useCallback((stem: StemKey) => {
    setMixLevelEditorStem(stem);
    setMixLevelEditorValue(String(mixLevels[stem] ?? 100));
  }, [mixLevels]);

  const closeMixLevelEditor = useCallback(() => {
    Keyboard.dismiss();
    setMixLevelEditorStem(null);
    setMixLevelEditorValue("");
  }, []);

  const updateMixLevelEditorValue = useCallback((raw: string) => {
    const digitsOnly = String(raw || "").replace(/[^0-9]/g, "").slice(0, 3);
    setMixLevelEditorValue(digitsOnly);
  }, []);

  const saveMixLevelEditorValue = useCallback(() => {
    if (!mixLevelEditorStem) return;
    const parsed = Number.parseInt(String(mixLevelEditorValue || "").trim(), 10);
    if (!Number.isFinite(parsed)) return;
    const clamped = Math.max(0, Math.min(150, Math.round(parsed)));
    setMixLevels((prev) => ({ ...prev, [mixLevelEditorStem]: clamped }));
    closeMixLevelEditor();
  }, [closeMixLevelEditor, mixLevelEditorStem, mixLevelEditorValue]);

  // Load job history from AsyncStorage on mount
  useEffect(() => {
    if (!HISTORY_FEATURE_ENABLED) return;
    const loadHistory = async () => {
      try {
        const stored = await AsyncStorage.getItem('@job_history');
        if (stored) {
          const parsed = JSON.parse(stored);
          if (Array.isArray(parsed)) {
            setJobHistory(parsed as JobStatus[]);
          } else {
            setJobHistory([]);
          }
        }
      } catch (e) {
        debugLog('[storage] failed to load history', { error: e });
      }
    };
    loadHistory();
  }, []);

  // Save job to history when it completes
  useEffect(() => {
    if (!job || !isJobTerminal(job.status)) return;
    if (HISTORY_FEATURE_ENABLED) {
      // Add to history (keep last 50 jobs).
      setJobHistory((prev) => [job, ...prev.filter((j) => j.id !== job.id)].slice(0, 50));
    }

    if (
      isSimpleLocalBackend ||
      !canScheduleCompletionNotification({
        notificationsEnabled,
        notificationPermissionStatus,
        appIsActive,
      })
    ) {
      return;
    }

    const notify = async () => {
      try {
        if (job.status === "succeeded") {
          await Notifications.scheduleNotificationAsync({
            content: {
              title: "🎤 Karaoke Ready!",
              body: `"${job.query}" is ready to view`,
              data: { jobId: job.id },
            },
            trigger: null, // show immediately
          });
        } else if (job.status === "failed") {
          await Notifications.scheduleNotificationAsync({
            content: {
              title: "❌ Job Failed",
              body: `"${job.query}" failed to process`,
              data: { jobId: job.id },
            },
            trigger: null,
          });
        }
      } catch (e) {
        debugLog("[notifications] failed to schedule", { error: e });
      }
    };

    notify().catch(() => undefined);
  }, [appIsActive, isSimpleLocalBackend, job?.id, job?.query, job?.status, notificationPermissionStatus, notificationsEnabled]);

  // Persist history whenever it changes.
  useEffect(() => {
    if (!HISTORY_FEATURE_ENABLED) return;
    AsyncStorage.setItem("@job_history", JSON.stringify(jobHistory)).catch(() => undefined);
  }, [jobHistory]);

  // Helper to resolve relative URLs to absolute
  const resolveUrl = useCallback((url: string | null | undefined) => {
    if (!url) return null;
    const trimmed = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
    if (!url.startsWith("http")) {
      return `${trimmed}${url}`;
    }
    if (!isSimpleLocalBackend) return url;
    try {
      const parsed = new URL(url);
      if (parsed.pathname.startsWith("/output/")) {
        return `${trimmed}${parsed.pathname}${parsed.search}${parsed.hash}`;
      }
    } catch {
      return url;
    }
    return url;
  }, [baseUrl, isSimpleLocalBackend]);

  useEffect(() => {
    const resolved = extractYoutubeUrlFromJob(job);
    if (!resolved) return;
    setLatestUploadedYoutubeUrl(resolved);
  }, [job]);

  const youtubeEmbedUrl = useMemo(
    () => buildYoutubeEmbedUrl(latestUploadedYoutubeUrl, { autoplay: true }),
    [latestUploadedYoutubeUrl]
  );

  useEffect(() => {
    setYoutubeEmbedReady(false);
  }, [youtubeEmbedUrl]);

  const videoMediaRevisionToken = useMemo(
    () => buildJobMediaRevisionToken(job, "video"),
    [job?.created_at, job?.finished_at, job?.id, job?.last_updated_at, job?.render_finished_at, job?.started_at]
  );
  const audioMediaRevisionToken = useMemo(
    () => buildJobMediaRevisionToken(job, "audio"),
    [job?.created_at, job?.finished_at, job?.id, job?.last_updated_at, job?.render_finished_at, job?.started_at]
  );
  const finalOutputUrl = useMemo(
    () => appendMediaRevisionToken(resolveUrl(job?.final_output_url), videoMediaRevisionToken ? `${videoMediaRevisionToken}:final` : null),
    [job?.final_output_url, resolveUrl, videoMediaRevisionToken]
  );
  const previewOutputUrl = useMemo(
    () => appendMediaRevisionToken(resolveUrl(job?.preview_output_url), videoMediaRevisionToken ? `${videoMediaRevisionToken}:preview` : null),
    [job?.preview_output_url, resolveUrl, videoMediaRevisionToken]
  );
  const legacyOutputUrl = useMemo(
    () => appendMediaRevisionToken(resolveUrl(job?.output_url), videoMediaRevisionToken ? `${videoMediaRevisionToken}:legacy` : null),
    [job?.output_url, resolveUrl, videoMediaRevisionToken]
  );
  const mixAudioUrl = useMemo(
    () => appendMediaRevisionToken(resolveUrl(job?.mix_audio_url), audioMediaRevisionToken ? `${audioMediaRevisionToken}:mix` : null),
    [job?.mix_audio_url, resolveUrl, audioMediaRevisionToken]
  );
  const suppressMutedPreview =
    !finalOutputUrl && Boolean(job?.output_is_preview) && (isJobFailed(job?.status) || isJobCancelled(job?.status));
  const effectivePreviewOutputUrl = suppressMutedPreview ? null : previewOutputUrl;
  const effectiveLegacyOutputUrl = suppressMutedPreview ? null : legacyOutputUrl;
  const playbackSource = useMemo(
    () =>
      resolvePlaybackSource({
        youtubeEmbedUrl: prefersDirectPlayback ? null : youtubeEmbedUrl,
        finalOutputUrl,
        previewOutputUrl: effectivePreviewOutputUrl,
        legacyOutputUrl: effectiveLegacyOutputUrl,
      }),
    [
      effectiveLegacyOutputUrl,
      effectivePreviewOutputUrl,
      finalOutputUrl,
      prefersDirectPlayback,
      youtubeEmbedUrl,
    ]
  );
  const outputUrl = playbackSource.kind === "direct" ? playbackSource.url : null;
  const videoSurfaceKey = useMemo(
    () => `${String(outputUrl || "none")}::${videoSurfaceNonce}`,
    [outputUrl, videoSurfaceNonce]
  );
  const isPreview = !finalOutputUrl && Boolean(effectivePreviewOutputUrl || (effectiveLegacyOutputUrl && job?.output_is_preview));
  const shouldUseCompanionAudio = useMemo(
    () =>
      shouldUsePreviewCompanionAudio({
        outputUrl,
        isPreview,
        companionAudioUrl: mixAudioUrl,
      }),
    [isPreview, mixAudioUrl, outputUrl]
  );
  const shouldMutePreviewAudio = shouldUseCompanionAudio;
  const targetVideoVolume = shouldMutePreviewAudio ? 0 : 1;
  const videoPlayer = useVideoPlayer(outputUrl ? { uri: outputUrl } : null, (player) => {
    player.staysActiveInBackground = backgroundVideoPlaybackEnabled;
    player.audioMixingMode = "doNotMix";
    player.showNowPlayingNotification = backgroundVideoPlaybackEnabled;
    player.muted = shouldMutePreviewAudio;
    player.volume = targetVideoVolume;
  });
  const companionAudioPlayer = useVideoPlayer(
    shouldUseCompanionAudio && mixAudioUrl ? { uri: mixAudioUrl } : null,
    (player) => {
      player.staysActiveInBackground = backgroundVideoPlaybackEnabled;
      player.audioMixingMode = "doNotMix";
      player.showNowPlayingNotification = false;
      player.muted = false;
      player.volume = 1;
    }
  );
  const rememberPlaybackTime = useCallback((value: unknown) => {
    const seconds = Number(value);
    if (!Number.isFinite(seconds) || seconds < 0) return;
    previewPlaybackTimeRef.current = seconds;
  }, []);
  const syncCompanionAudioToVideo = useCallback(
    (force = false) => {
      if (!shouldUseCompanionAudio || !outputUrl || !mixAudioUrl) return false;
      try {
        const videoTime = Number(videoPlayer.currentTime || 0);
        const audioTime = Number(companionAudioPlayer.currentTime || 0);
        if (!Number.isFinite(videoTime) || videoTime < 0) return false;
        if (!force && Number.isFinite(audioTime) && Math.abs(audioTime - videoTime) < 0.35) {
          return false;
        }
        companionAudioPlayer.currentTime = videoTime;
        return true;
      } catch {
        return false;
      }
    },
    [companionAudioPlayer, mixAudioUrl, outputUrl, shouldUseCompanionAudio, videoPlayer]
  );
  const applyPendingPreviewToFinalSeek = useCallback(
    (durationHint?: number) => {
      const pendingSeek = pendingPreviewToFinalSeekRef.current;
      if (pendingSeek === null) return false;
      const targetSeek = clampPreviewToFinalSeek({
        pendingSeek,
        durationHint,
        endBufferSec: DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
      });
      if (targetSeek === null) {
        pendingPreviewToFinalSeekRef.current = null;
        return false;
      }
      try {
        videoPlayer.currentTime = targetSeek;
        previewPlaybackTimeRef.current = targetSeek;
        pendingPreviewToFinalSeekRef.current = null;
        debugLog("[video] seamless handoff seek applied", { targetSeek, durationHint });
        return true;
      } catch {
        return false;
      }
    },
    [videoPlayer]
  );

  useEffect(() => {
    if (!outputUrl) return;
    try {
      videoPlayer.staysActiveInBackground = backgroundVideoPlaybackEnabled;
      videoPlayer.audioMixingMode = "doNotMix";
      videoPlayer.showNowPlayingNotification = backgroundVideoPlaybackEnabled;
      videoPlayer.muted = shouldMutePreviewAudio;
      videoPlayer.volume = targetVideoVolume;
      if (shouldUseCompanionAudio) {
        companionAudioPlayer.staysActiveInBackground = backgroundVideoPlaybackEnabled;
        companionAudioPlayer.audioMixingMode = "doNotMix";
        companionAudioPlayer.showNowPlayingNotification = false;
        companionAudioPlayer.muted = false;
        companionAudioPlayer.volume = 1;
      }
    } catch {
      // keep playback best-effort if native player rejects a setting
    }
  }, [
    backgroundVideoPlaybackEnabled,
    companionAudioPlayer,
    outputUrl,
    shouldMutePreviewAudio,
    shouldUseCompanionAudio,
    targetVideoVolume,
    videoPlayer,
  ]);

  useEffect(() => {
    if (appIsActive) return;
    if (!outputUrl) return;
    try {
      videoPlayer.staysActiveInBackground = backgroundVideoPlaybackEnabled;
      videoPlayer.showNowPlayingNotification = backgroundVideoPlaybackEnabled;
      if (!backgroundVideoPlaybackEnabled || videoPaused) {
        videoPlayer.pause();
        if (shouldUseCompanionAudio) {
          companionAudioPlayer.pause();
        }
        return;
      }
      videoPlayer.play();
      if (shouldUseCompanionAudio) {
        syncCompanionAudioToVideo(true);
        companionAudioPlayer.play();
      }
    } catch {
      // no-op: best-effort keepalive when screen is locked/dimmed.
    }
  }, [
    appIsActive,
    backgroundVideoPlaybackEnabled,
    companionAudioPlayer,
    outputUrl,
    shouldUseCompanionAudio,
    syncCompanionAudioToVideo,
    videoPaused,
    videoPlayer,
  ]);

  useEffect(() => {
    try {
      videoPlayer.timeUpdateEventInterval = VIDEO_TIME_UPDATE_INTERVAL_SEC;
    } catch {
      // no-op if native player rejects interval updates
    }
    const subscription = videoPlayer.addListener("timeUpdate", ({ currentTime }) => {
      rememberPlaybackTime(currentTime);
      if (shouldUseCompanionAudio) {
        syncCompanionAudioToVideo(false);
      }
    });
    return () => {
      subscription.remove();
      try {
        videoPlayer.timeUpdateEventInterval = 0;
      } catch {
        // no-op
      }
    };
  }, [rememberPlaybackTime, shouldUseCompanionAudio, syncCompanionAudioToVideo, videoPlayer]);

  useEffect(() => {
    const previousSource = lastVideoSourceRef.current;
    const sourceChanged = previousSource.outputUrl !== outputUrl;
    if (!sourceChanged) {
      lastVideoSourceRef.current = { outputUrl, isPreview };
      return;
    }

    if (outputUrl) {
      const carryPausedState = shouldCarryPauseStateAcrossPreviewToFinal({
        sourceChanged,
        previousIsPreview: previousSource.isPreview,
        nextIsPreview: isPreview,
        wasPaused: videoPaused,
      });
      shouldAutoplayAfterSourceChangeRef.current = !carryPausedState;
      if (carryPausedState) {
        debugLog("[video] seamless handoff preserving paused state");
      }
      pendingPreviewToFinalSeekRef.current = null;
      if (previousSource.isPreview && !isPreview && previousSource.outputUrl) {
        rememberPlaybackTime(videoPlayer.currentTime);
      }
      const handoffSeek = previewPlaybackTimeRef.current;
      const handoffInput = {
        sourceChanged,
        previousOutputUrl: previousSource.outputUrl,
        previousIsPreview: previousSource.isPreview,
        nextOutputUrl: outputUrl,
        nextIsPreview: isPreview,
        handoffSeek,
      };
      if (shouldArmPreviewToFinalSeek(handoffInput)) {
        pendingPreviewToFinalSeekRef.current = handoffSeek;
        debugLog("[video] seamless handoff armed", {
          from: previousSource.outputUrl,
          to: outputUrl,
          handoffSeek,
        });
      } else if (shouldResetPlaybackOnSourceChange(handoffInput)) {
        previewPlaybackTimeRef.current = 0;
        try {
          videoPlayer.pause();
          videoPlayer.currentTime = 0;
        } catch {
          // Best-effort reset; source load will continue even if native player rejects it.
        }
        try {
          companionAudioPlayer.pause();
          companionAudioPlayer.currentTime = 0;
        } catch {
          // no-op
        }
        debugLog("[video] source swap reset playback", {
          from: previousSource.outputUrl,
          to: outputUrl,
          previousIsPreview: previousSource.isPreview,
          nextIsPreview: isPreview,
        });
      }
    } else {
      previewPlaybackTimeRef.current = 0;
      pendingPreviewToFinalSeekRef.current = null;
      shouldAutoplayAfterSourceChangeRef.current = true;
    }

    lastVideoSourceRef.current = { outputUrl, isPreview };
  }, [companionAudioPlayer, isPreview, outputUrl, rememberPlaybackTime, videoPaused, videoPlayer]);

  useEffect(() => {
    const subscription = videoPlayer.addListener("sourceLoad", ({ duration }) => {
      applyPendingPreviewToFinalSeek(duration);
      if (shouldUseCompanionAudio) {
        syncCompanionAudioToVideo(true);
      }
    });
    return () => subscription.remove();
  }, [applyPendingPreviewToFinalSeek, shouldUseCompanionAudio, syncCompanionAudioToVideo, videoPlayer]);

  useEffect(() => {
    const subscription = companionAudioPlayer.addListener("sourceLoad", () => {
      syncCompanionAudioToVideo(true);
      try {
        if (videoPaused) {
          companionAudioPlayer.pause();
        } else {
          companionAudioPlayer.play();
        }
      } catch {
        // best-effort companion audio bootstrap
      }
    });
    return () => subscription.remove();
  }, [companionAudioPlayer, syncCompanionAudioToVideo, videoPaused]);

  useEffect(() => {
    const subscription = videoPlayer.addListener("playingChange", ({ isPlaying }) => {
      setVideoIsPlaying(Boolean(isPlaying));
    });
    return () => subscription.remove();
  }, [videoPlayer]);

  useEffect(() => {
    if (!outputUrl) {
      setVideoPaused(false);
      setVideoIsPlaying(false);
      shouldAutoplayAfterSourceChangeRef.current = true;
      try {
        companionAudioPlayer.pause();
        companionAudioPlayer.currentTime = 0;
      } catch {
        // no-op
      }
      return;
    }
    const shouldAutoplay = shouldAutoplayAfterSourceChangeRef.current;
    setVideoPaused(!shouldAutoplay);
    applyPendingPreviewToFinalSeek();
    try {
      videoPlayer.muted = shouldMutePreviewAudio;
      videoPlayer.volume = targetVideoVolume;
      if (shouldAutoplay) {
        videoPlayer.play();
      } else {
        videoPlayer.pause();
      }
      if (shouldUseCompanionAudio) {
        syncCompanionAudioToVideo(true);
        if (shouldAutoplay) {
          companionAudioPlayer.play();
        } else {
          companionAudioPlayer.pause();
        }
      } else {
        companionAudioPlayer.pause();
      }
    } catch {
      // ignore autoplay failures; user can still hit play.
    }
    shouldAutoplayAfterSourceChangeRef.current = true;
  }, [
    applyPendingPreviewToFinalSeek,
    companionAudioPlayer,
    outputUrl,
    shouldMutePreviewAudio,
    shouldUseCompanionAudio,
    syncCompanionAudioToVideo,
    targetVideoVolume,
    videoPlayer,
  ]);

  useEffect(() => {
    if (!outputUrl) return;
    try {
      const shouldForcePauseForBackgroundSetting =
        !backgroundVideoPlaybackEnabled && (!appIsActive || activeTab !== "video");
      if (videoPaused || shouldForcePauseForBackgroundSetting) {
        videoPlayer.pause();
        if (shouldUseCompanionAudio) {
          companionAudioPlayer.pause();
        }
      } else {
        videoPlayer.play();
        if (shouldUseCompanionAudio) {
          syncCompanionAudioToVideo(false);
          companionAudioPlayer.play();
        }
      }
    } catch {
      // playback toggles are best-effort.
    }
  }, [
    activeTab,
    appIsActive,
    backgroundVideoPlaybackEnabled,
    companionAudioPlayer,
    outputUrl,
    shouldUseCompanionAudio,
    syncCompanionAudioToVideo,
    videoPaused,
    videoPlayer,
  ]);

  const refreshVideoSurface = useCallback(
    (reason: string) => {
      if (!outputUrl) return;
      const wasPaused = Boolean(videoPaused);
      debugLog("[video] refreshing surface", { reason, outputUrl, wasPaused });
      setVideoSurfaceNonce((prev) => prev + 1);
      setTimeout(() => {
        try {
          if (shouldUseCompanionAudio) {
            syncCompanionAudioToVideo(true);
          }
          if (wasPaused) {
            videoPlayer.pause();
            if (shouldUseCompanionAudio) {
              companionAudioPlayer.pause();
            }
            return;
          }
          videoPlayer.play();
          if (shouldUseCompanionAudio) {
            companionAudioPlayer.play();
          }
        } catch {
          // best-effort refresh only
        }
      }, 120);
    },
    [companionAudioPlayer, outputUrl, shouldUseCompanionAudio, syncCompanionAudioToVideo, videoPaused, videoPlayer]
  );

  useEffect(() => {
    const subscription = Notifications.addNotificationResponseReceivedListener((response) => {
      const rawJobId = response.notification.request.content.data?.jobId;
      const jobId = String(rawJobId || "").trim();
      debugLog("[notifications] tapped", { jobId });
      if (!jobId) return;

      activeJobIdRef.current = jobId;
      notificationVideoOpenRef.current = true;
      fetchWithTimeout(buildJobStatusUrl(baseUrl, jobId, "full"), {}, 8000)
        .then((res) => (res.ok ? res.json() : null))
        .then((data) => {
          if (!data || !isSnapshotForActiveJob(String(data?.id || ""))) return;
          setJob((prev) => (isSameJobSnapshot(prev, data) ? prev : data));
        })
        .catch(() => undefined);

      setActiveTab("video");
      setTimeout(() => {
        refreshVideoSurface("notification_tap");
      }, 160);
    });

    return () => subscription.remove();
  }, [baseUrl, isSnapshotForActiveJob, refreshVideoSurface]);

  useEffect(() => {
    if (activeTab !== "video") return;
    if (!outputUrl) return;
    if (!notificationVideoOpenRef.current) return;
    notificationVideoOpenRef.current = false;
    refreshVideoSurface("notification_surface_recover");
  }, [activeTab, outputUrl, refreshVideoSurface]);

  useEffect(() => {
    if (job?.id) return;
    if (loading || isQueryNormalizing || processingVisible) return;
    setTimingModalVisible(false);
  }, [job?.id, isQueryNormalizing, loading, processingVisible]);

  useEffect(() => {
    if (showTimers) return;
    if (!timingModalVisible) return;
    setTimingModalVisible(false);
  }, [showTimers, timingModalVisible]);

  useEffect(() => {
    if (!mixLevelEditorStem) return;
    const timer = setTimeout(() => {
      mixLevelInputRef.current?.focus();
    }, 80);
    return () => clearTimeout(timer);
  }, [mixLevelEditorStem]);

  const refreshVideoJob = useCallback(async (): Promise<JobStatus | null> => {
    const targetJobId = String(job?.id || "").trim();
    if (!targetJobId) return null;
    if (!isSnapshotForActiveJob(targetJobId)) {
      debugWarn("[job] ignored refresh for stale job id", {
        targetJobId,
        activeJobId: activeJobIdRef.current || null,
      });
      return null;
    }
    try {
      const res = await fetchWithTimeout(buildJobStatusUrl(baseUrl, targetJobId, "full"), {}, 8000);
      if (!isSnapshotForActiveJob(targetJobId)) {
        return null;
      }
      if (!res.ok) {
        if (res.status === 404) {
          setError("This video is no longer available, generate it again");
        } else if (res.status >= 500 || res.status === 429) {
          setError(VIDEO_LOADING_STATUS_MESSAGE);
        }
        return null;
      }
      const data = (await res.json()) as JobStatus;
      if (!isSnapshotForActiveJob(data?.id)) {
        debugWarn("[job] dropped stale refresh snapshot", {
          snapshotJobId: data?.id || null,
          targetJobId,
          activeJobId: activeJobIdRef.current || null,
        });
        return null;
      }
      setJob((prev) => {
        if (prev?.id && prev.id !== data.id) {
          return prev;
        }
        return isSameJobSnapshot(prev, data) ? prev : data;
      });
      const hasFinalLikeOutput =
        Boolean(resolveUrl(data?.final_output_url)) ||
        Boolean(resolveUrl(data?.preview_output_url)) ||
        Boolean(resolveUrl(data?.output_url));
      const hasYoutubeEmbedUrl = Boolean(extractYoutubeUrlFromJob(data));
      if (isJobSucceeded(data?.status) && !hasFinalLikeOutput && !hasYoutubeEmbedUrl) {
        setError(VIDEO_LOAD_RECOVERY_MESSAGE);
      }
      return data;
    } catch {
      return null;
    }
  }, [baseUrl, isSnapshotForActiveJob, job?.id, resolveUrl]);

  // Debug: Log output URL when it changes
  useEffect(() => {
    if (outputUrl) {
      debugLog("[video] output url resolved", {
        outputUrl,
        finalOutputUrl,
        previewOutputUrl,
        legacyOutputUrl,
        isPreview,
        jobStatus: job?.status,
      });
    }
  }, [outputUrl, finalOutputUrl, previewOutputUrl, legacyOutputUrl, isPreview, job?.status]);
  // Memoize derived job status flags to prevent recomputation on every render
  const jobFlags = useMemo(
    () => ({
      jobInProgress: isJobInProgress(job?.status),
      jobSucceeded: isJobSucceeded(job?.status),
      jobFailed: isJobFailed(job?.status),
      jobCancelled: isJobCancelled(job?.status),
      jobTerminal: isJobTerminal(job?.status),
    }),
    [job?.status]
  );
  const { jobInProgress, jobSucceeded, jobFailed, jobCancelled, jobTerminal } = jobFlags;
  const jobLastUpdatedAtMs = useMemo(() => {
    const fromStatus = Number(job?.last_updated_at);
    if (Number.isFinite(fromStatus) && fromStatus > 0) return Math.floor(fromStatus * 1000);
    const fromStarted = Number(job?.started_at);
    if (Number.isFinite(fromStarted) && fromStarted > 0) return Math.floor(fromStarted * 1000);
    const fromCreated = Number(job?.created_at);
    if (Number.isFinite(fromCreated) && fromCreated > 0) return Math.floor(fromCreated * 1000);
    return null;
  }, [job?.created_at, job?.last_updated_at, job?.started_at]);
  const hasFinalLikeOutput = Boolean(finalOutputUrl) || (Boolean(legacyOutputUrl) && !isPreview && !jobInProgress);
  const autoSaveTargetKey = useMemo(() => {
    if (!jobSucceeded || !hasFinalLikeOutput || !outputUrl) return null;
    const jobId = String(job?.id || "").trim();
    if (jobId) return `job:${jobId}`;
    return `output:${outputUrl}`;
  }, [hasFinalLikeOutput, job?.id, jobSucceeded, outputUrl]);
  const playbackSourceReady = isPlaybackSourceReady(playbackSource, youtubeEmbedReady);
  const videoTabEnabled = playbackSource.kind !== "none";
  const shouldHoldPresetLoadingForEmbed = pendingPresetVideoAutoOpenRef.current && !playbackSourceReady;
  const shouldShowProcessingModal =
    processingVisible &&
    !timingModalVisible &&
    (loading || shouldKeepProcessingModalOpen(job?.status, Boolean(outputUrl)) || shouldHoldPresetLoadingForEmbed);
  const canSaveOrShare = hasFinalLikeOutput && !isDemoMode;
  const saveShareDisabled = !canSaveOrShare || isExporting;
  const mixLevelEditorSaveDisabled = !mixLevelEditorStem || !mixLevelEditorValue.trim();
  const mixLevelEditorStemLabel = mixLevelEditorStem ? STEM_LABELS[mixLevelEditorStem] : "Volume";
  const showInlineError = Boolean(error) && !isProgressStatusMessage(error);
  const showInlineStatus = Boolean(error) && isProgressStatusMessage(error);
  const strictLyricsFailure = useMemo(() => {
    const candidates = [
      String(error || ""),
      String(job?.error || ""),
      String(job?.last_message || ""),
    ].map((text) => text.trim()).filter(Boolean);
    const matched = candidates.find((text) => isLyricsMissingMessage(text));
    if (!matched) return null;

    const queryHint =
      extractQuotedQueryHint(matched) ||
      String(query || "").trim() ||
      String(job?.query || "").trim();
    const lookup = buildTimingLookup(job?.timing_breakdown);
    const step1Ms = timingEntryMs(lookup["step1.total"]);
    const pipelineMs = timingEntryMs(lookup["pipeline.total"]);
    const stopwatchMs =
      stopwatchStartAt && stopwatchEndAt ? Math.max(0, stopwatchEndAt - stopwatchStartAt) : null;
    const elapsedMs = stopwatchMs ?? step1Ms ?? pipelineMs ?? null;
    const suggestedQuery = deriveSuggestedQuery(queryHint);
    return {
      queryHint,
      elapsedMs,
      suggestedQuery,
    };
  }, [
    error,
    job?.error,
    job?.last_message,
    job?.query,
    job?.timing_breakdown,
    query,
    stopwatchStartAt,
    stopwatchEndAt,
  ]);
  const showSmartRecoveryActions = Boolean(showInlineError && strictLyricsFailure);
  const showGenericRetryButton = Boolean(showInlineError && !strictLyricsFailure);
  const showRetryVideoLoadButton = Boolean(
    job?.id && !videoTabEnabled && (jobSucceeded || pendingPresetVideoAutoOpenRef.current)
  );
  const retryVideoLoadDisabled = shouldShowProcessingModal || loading || isQueryNormalizing;
  const notificationsConfigured = notificationsEnabled && isPermissionGranted(notificationPermissionStatus);
  const notificationsSummary = notificationsStatusMessage
    ? notificationsStatusMessage
    : notificationsConfigured
      ? formatUiStatus("Background notifications enabled")
      : formatUiStatus("Background notifications disabled");
  const otaSummary = otaStatusMessage
    ? otaStatusMessage
    : otaConfig.enabled
      ? formatUiStatus(`OTA updates enabled on "${otaChannel}" channel.`)
      : formatUiStatus("OTA updates are unavailable for this build");

  useEffect(() => {
    if (activeTab !== "history") return;
    setActiveTab("search");
  }, [activeTab]);

  useEffect(() => {
    const subscription = AppState.addEventListener("change", (nextState) => {
      const active = nextState === "active";
      if (!active) {
        lastBackgroundedAtRef.current = Date.now();
      }
      setAppIsActive(active);
      debugLog("[app] state", { nextState, active });
    });
    return () => subscription.remove();
  }, []);

  useEffect(() => {
    if (!job?.id || !isJobInProgress(job.status)) {
      jobPollUnchangedStreakRef.current = 0;
    }
    if (job?.id && isJobTerminal(job.status)) {
      delete jobPollEtagRef.current[job.id];
    }
  }, [job?.id, job?.status]);

  useEffect(() => {
    if (!job?.id || !isJobInProgress(job?.status)) return;
    // Continue polling even when app is backgrounded for notifications
    const activeJobId = job.id;
    activeJobIdRef.current = activeJobId;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let latestSnapshot: JobStatus | null = job;

    const poll = async () => {
      if (cancelled || jobPollInFlightRef.current) return;
      if (!isSnapshotForActiveJob(activeJobId)) {
        cancelled = true;
        return;
      }
      debugLog("[poll] fetching job status", { jobId: activeJobId });
      jobPollInFlightRef.current = true;
      try {
        const headers: Record<string, string> = {};
        const previousEtag = jobPollEtagRef.current[activeJobId];
        if (previousEtag) {
          headers["If-None-Match"] = previousEtag;
        }

        const res = await fetchWithTimeout(
          buildJobStatusUrl(baseUrl, activeJobId, "poll"),
          { headers },
          8000
        );
        if (res.status === 304) {
          jobPollUnchangedStreakRef.current += 1;
          debugLog("[poll] not modified", {
            jobId: activeJobId,
            unchangedStreak: jobPollUnchangedStreakRef.current,
          });
          return;
        }
        if (!res.ok) {
          debugWarn("[poll] response not ok", { status: res.status });
          return;
        }
        const etag = res.headers.get("etag");
        if (etag) {
          jobPollEtagRef.current[activeJobId] = etag;
        }
        const data = (await res.json()) as JobStatus;
        if (cancelled || !isSnapshotForActiveJob(data?.id || activeJobId)) {
          debugWarn("[poll] stale snapshot ignored", {
            activeJobId,
            snapshotJobId: data?.id || null,
            status: data?.status || null,
          });
          cancelled = true;
          return;
        }
        debugLog("[poll] got job data", {
          status: data.status,
          stage: data.stage,
          progress: data.progress_percent,
        });
        latestSnapshot = data;
        const fatalMessage = extractFatalMissingResourceMessage(data.error, data.last_message);
        if (fatalMessage) {
          debugWarn("[poll] fatal missing resource detected", {
            jobId: data.id,
            status: data.status,
            stage: data.stage,
            fatalMessage,
          });
          const nowTs = Date.now() / 1000;
          setJob((prev) => {
            if (prev?.id && prev.id !== data.id) {
              return prev;
            }
            return {
              ...(prev || data),
              ...data,
              status: "failed",
              finished_at: data.finished_at ?? nowTs,
              error: fatalMessage,
              last_message: fatalMessage,
            };
          });
          setError(fatalMessage);
          clearPendingPresetAutoOpenState();
          setLoading(false);
          setProcessingVisible(false);
          setStopwatchEndAt((prev) => prev ?? Date.now());
          if (isJobInProgress(data.status)) {
            fetchWithTimeout(`${baseUrl}/jobs/${data.id}/cancel`, { method: "POST" }, 5000).catch(() => undefined);
          }
          jobPollUnchangedStreakRef.current = 0;
          cancelled = true;
          return;
        }
        setJob((prev) => {
          if (prev?.id && prev.id !== data.id) {
            debugWarn("[poll] blocked cross-job snapshot update", {
              prevJobId: prev.id,
              snapshotJobId: data.id,
            });
            return prev;
          }
          const isSame = isSameJobSnapshot(prev, data);
          debugLog("[poll] snapshot compare", { isSame });
          if (isSame) {
            jobPollUnchangedStreakRef.current += 1;
          } else {
            jobPollUnchangedStreakRef.current = 0;
          }
          return isSame ? prev : data;
        });
      } catch (e) {
        debugWarn("[poll] error", e);
        // ignore intermittent fetch errors
      } finally {
        jobPollInFlightRef.current = false;
        if (!cancelled) {
          const basePollMs = getJobPollIntervalMs(latestSnapshot ?? job ?? null, appIsActive);
          const nextPollMs = withPollBackoffAndJitter(
            basePollMs,
            jobPollUnchangedStreakRef.current,
            appIsActive
          );
          debugLog("[poll] scheduling next poll", {
            inMs: nextPollMs,
            appIsActive,
            unchangedStreak: jobPollUnchangedStreakRef.current,
          });
          timer = setTimeout(poll, nextPollMs);
        }
      }
    };

    // Start polling immediately
    poll().catch(() => undefined);

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      jobPollInFlightRef.current = false;
    };
  }, [clearPendingPresetAutoOpenState, isSnapshotForActiveJob, job?.id, job?.status, baseUrl, appIsActive]); // Continue monitoring even when app is backgrounded

  useEffect(() => {
    if (!job) return;
    const key = `${job.status}|${job.stage || ""}|${job.last_message || ""}|${job.error || ""}`;
    if (progressLogRef.current === key) return;
    progressLogRef.current = key;
    debugLog("[job] progress", {
      id: job.id,
      status: job.status,
      stage: job.stage,
      last_message: job.last_message,
      error: job.error,
      last_updated_at: job.last_updated_at,
    });
  }, [job?.status, job?.stage, job?.last_message, job?.error]); // Depend only on relevant fields, not entire job object

  useEffect(() => {
    const unsubscribe = NetInfo.addEventListener((state) => {
      setIsOnline(Boolean(state.isConnected));
      debugLog("[net] connectivity", {
        isConnected: state.isConnected,
        isInternetReachable: state.isInternetReachable,
        type: state.type,
      });
    });
    return () => unsubscribe();
  }, []);

  const checkApi = useCallback(
    // Mobile networks frequently have >3s cold-start latency (DNS/TLS/Radio wake).
    // Avoid showing "API unreachable" on a single slow health check; require consecutive failures.
    async (timeoutMs = 8000) => {
      if (apiHealthCheckPromiseRef.current) {
        return apiHealthCheckPromiseRef.current;
      }

      const runCheck = (async () => {
        if (!isOnline) {
          setApiReachable(false);
          setApiLastError("Offline");
          return false;
        }
        try {
          const res = await fetchWithTimeout(`${baseUrl}/health`, {}, timeoutMs);
          if (!res.ok) throw new Error(`Health check failed (${res.status})`);
          apiFailCountRef.current = 0;
          setApiFailCount(0);
          setApiReachable(true);
          setApiLastError(null);
          return true;
        } catch (err: any) {
          const message = err?.name === "AbortError" ? "Health check timed out" : err?.message;
          apiFailCountRef.current = apiFailCountRef.current + 1;
          const next = apiFailCountRef.current;
          setApiFailCount(next);
          // Only mark unreachable after 2 consecutive failures to prevent transient banners.
          if (next >= 2) {
            // Attempt automatic failover between the custom domain and the backup URL.
            const alt = resolveFailoverApiBaseUrl({
              currentBaseUrl: baseUrl,
              primaryBaseUrl,
              fallbackBaseUrl: FALLBACK_BASE_URL,
            });
            if (alt && alt !== baseUrl) {
              try {
                const altRes = await fetchWithTimeout(`${alt}/health`, {}, Math.max(5000, timeoutMs));
                if (altRes.ok) {
                  debugWarn("[net] failover", { from: baseUrl, to: alt, reason: message });
                  setBaseUrl(alt);
                  setApiReachable(true);
                  setApiLastError(null);
                  apiFailCountRef.current = 0;
                  setApiFailCount(0);
                  // Keep the runtime baseUrl in sync for modules that don't live in App.tsx.
                  setApiBaseUrl(alt);
                  return true;
                }
              } catch {
                // ignore alt probe errors
              }
            }
            setApiReachable(false);
          }
          setApiLastError(message || "Health check failed");
          return false;
        }
      })();

      apiHealthCheckPromiseRef.current = runCheck;
      try {
        return await runCheck;
      } finally {
        if (apiHealthCheckPromiseRef.current === runCheck) {
          apiHealthCheckPromiseRef.current = null;
        }
      }
    },
    [baseUrl, isOnline, primaryBaseUrl]
  );

  useEffect(() => {
    if (!otaConfig.autoCheckOnForeground) return;
    const timer = setTimeout(() => {
      checkForOtaUpdate({ reason: "app_launch_auto" }).catch(() => undefined);
    }, 1400);
    return () => clearTimeout(timer);
  }, [checkForOtaUpdate, otaConfig.autoCheckOnForeground]);

  // When the app returns to the foreground, refresh API reachability + job state quickly.
  useEffect(() => {
    const wasActive = prevAppIsActiveRef.current;
    prevAppIsActiveRef.current = appIsActive;
    if (wasActive || !appIsActive) return;
    const backgroundGapMs = lastBackgroundedAtRef.current ? Math.max(0, Date.now() - lastBackgroundedAtRef.current) : 0;
    lastBackgroundedAtRef.current = null;

    Notifications.getPermissionsAsync()
      .then((permissions) => {
        setNotificationPermissionStatus(
          (permissions?.status as NotificationPermissionStatus | undefined) || "undetermined"
        );
      })
      .catch(() => undefined);
    checkApi(2500).catch(() => undefined);
    refreshVideoJob().catch(() => undefined);
    if (otaConfig.autoCheckOnForeground) {
      checkForOtaUpdate({ reason: "app_foreground_auto" }).catch(() => undefined);
    }
    if (
      activeTab === "video" &&
      outputUrl &&
      backgroundGapMs >= VIDEO_SURFACE_REFRESH_BACKGROUND_GAP_MS
    ) {
      refreshVideoSurface("app_foreground");
    }
  }, [
    activeTab,
    appIsActive,
    checkApi,
    checkForOtaUpdate,
    otaConfig.autoCheckOnForeground,
    outputUrl,
    refreshVideoJob,
    refreshVideoSurface,
  ]);

  useEffect(() => {
    const timer = setTimeout(() => {
      checkApi().catch(() => undefined);
    }, 600);
    return () => clearTimeout(timer);
  }, [checkApi]);

  // Keep shared runtime base URL in sync (used by non-React modules).
  useEffect(() => {
    setApiBaseUrl(baseUrl);
  }, [baseUrl]);

  useEffect(() => {
    if (!job?.id) return;
    const shouldKeepWaitingForEmbed =
      pendingPresetVideoAutoOpenRef.current && jobSucceeded && !playbackSourceReady;
    if (jobTerminal) {
      setStopwatchEndAt((prev) => prev ?? Date.now());
      if (!shouldKeepWaitingForEmbed) {
        setProcessingVisible(false);
      }
    }
    if (!jobTerminal) return;
    // Job reached a terminal state: stop any "submitting" UI.
    setLoading(false);
    if (jobSucceeded) {
      if (shouldKeepWaitingForEmbed) {
        setError(VIDEO_LOADING_STATUS_MESSAGE);
      } else {
        clearPendingPresetAutoOpenState();
        setError(null);
      }
      Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success).catch(() => undefined);
      return;
    }
    if (jobFailed) {
      clearPendingPresetAutoOpenState();
      setYoutubeEmbedReady(false);
      Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error).catch(() => undefined);
      return;
    }
    clearPendingPresetAutoOpenState();
    Haptics.notificationAsync(Haptics.NotificationFeedbackType.Warning).catch(() => undefined);
  }, [
    clearPendingPresetAutoOpenState,
    job?.id,
    job?.status,
    jobTerminal,
    jobSucceeded,
    jobFailed,
    outputUrl,
    playbackSourceReady,
  ]);

  useEffect(() => {
    if (!processingVisible) {
      processingVisibleSinceRef.current = null;
      processingStaleRefreshAtRef.current = 0;
      return;
    }
    if (!processingVisibleSinceRef.current) {
      processingVisibleSinceRef.current = Date.now();
    }

    const timer = setInterval(() => {
      const now = Date.now();
      const openedAt = processingVisibleSinceRef.current ?? now;
      const elapsedMs = Math.max(0, now - openedAt);
      const waitingForPresetEmbed = pendingPresetVideoAutoOpenRef.current && !playbackSourceReady;
      const waitingForSubmit = loading || isQueryNormalizing;

      if (elapsedMs >= PROCESSING_MODAL_HARD_TIMEOUT_MS) {
        debugWarn("[processing] modal hard timeout", {
          elapsedMs,
          jobId: job?.id || null,
          status: job?.status || null,
        });
        clearPendingPresetAutoOpenState();
        setLoading(false);
        setProcessingVisible(false);
        setError(VIDEO_LOAD_RECOVERY_MESSAGE);
        return;
      }

      if (!job?.id && !waitingForSubmit && !waitingForPresetEmbed) {
        if (elapsedMs >= PROCESSING_MODAL_NO_JOB_GRACE_MS) {
          debugWarn("[processing] closing modal because no active job exists", { elapsedMs });
          clearPendingPresetAutoOpenState();
          setProcessingVisible(false);
        }
        return;
      }

      if (!jobInProgress) {
        if (!waitingForSubmit && !waitingForPresetEmbed) {
          setProcessingVisible(false);
        }
        return;
      }

      if (!jobLastUpdatedAtMs) return;
      const staleForMs = Math.max(0, now - jobLastUpdatedAtMs);
      if (staleForMs < PROCESSING_MODAL_STALE_JOB_AGE_MS) return;

      const sinceLastRefreshMs = Math.max(0, now - processingStaleRefreshAtRef.current);
      if (sinceLastRefreshMs >= PROCESSING_MODAL_STALE_REFRESH_INTERVAL_MS) {
        processingStaleRefreshAtRef.current = now;
        refreshVideoJob().catch(() => undefined);
      }

      if (staleForMs >= PROCESSING_MODAL_STALE_JOB_FORCE_CLOSE_MS) {
        debugWarn("[processing] stale job fallback close", {
          staleForMs,
          jobId: job?.id ?? null,
          status: job?.status ?? null,
          lastUpdatedAtMs: jobLastUpdatedAtMs,
        });
        clearPendingPresetAutoOpenState();
        setProcessingVisible(false);
        setLoading(false);
        setError(VIDEO_LOAD_RECOVERY_MESSAGE);
      }
    }, 1000);

    return () => clearInterval(timer);
  }, [
    clearPendingPresetAutoOpenState,
    isQueryNormalizing,
    job?.id,
    job?.status,
    jobInProgress,
    jobLastUpdatedAtMs,
    loading,
    processingVisible,
    refreshVideoJob,
    playbackSourceReady,
  ]);

  useEffect(() => {
    if (!pendingPresetVideoAutoOpenRef.current) return;
    if (!job?.id) return;
    if (!jobTerminal || !jobSucceeded) return;
    if (playbackSourceReady) return;
    const startedAt = pendingEmbedWaitStartAtRef.current ?? Date.now();
    pendingEmbedWaitStartAtRef.current = startedAt;
    const timer = setInterval(() => {
      const elapsedMs = Math.max(0, Date.now() - startedAt);
      if (elapsedMs >= PRESET_EMBED_WAIT_MAX_MS) {
        debugWarn("[video] embed wait timed out", {
          jobId: job.id,
          elapsedMs,
          attempts: pendingEmbedRefreshAttemptsRef.current,
        });
        clearPendingPresetAutoOpenState();
        setProcessingVisible(false);
        setLoading(false);
        setError(VIDEO_LOAD_RECOVERY_MESSAGE);
        clearInterval(timer);
        return;
      }

      if (!videoTabEnabled) {
        pendingEmbedRefreshAttemptsRef.current += 1;
        refreshVideoJob().catch(() => undefined);
      }
    }, PRESET_EMBED_WAIT_RETRY_MS);
    return () => clearInterval(timer);
  }, [
    clearPendingPresetAutoOpenState,
    job?.id,
    jobSucceeded,
    jobTerminal,
    playbackSourceReady,
    refreshVideoJob,
    videoTabEnabled,
  ]);

  useEffect(() => {
    if (!pendingPresetVideoAutoOpenRef.current) return;
    if (!job?.id) return;
    if (!jobTerminal) return;
    if (!jobSucceeded) {
      clearPendingPresetAutoOpenState();
      return;
    }
    if (!videoTabEnabled) return;
    setActiveTab("video");
    setVideoPaused(false);
  }, [clearPendingPresetAutoOpenState, job?.id, jobSucceeded, jobTerminal, videoTabEnabled]);

  useEffect(() => {
    if (!job?.id || !jobSucceeded || videoTabEnabled || pendingPresetVideoAutoOpenRef.current || prefersDirectPlayback) {
      missingEmbedHydrationAttemptsRef.current = 0;
      return;
    }
    if (missingEmbedHydrationAttemptsRef.current >= 4) {
      if (isProgressStatusMessage(error || "")) {
        setError(VIDEO_LOAD_RECOVERY_MESSAGE);
      }
      return;
    }
    const timer = setTimeout(() => {
      missingEmbedHydrationAttemptsRef.current += 1;
      refreshVideoJob()
        .then((snapshot) => {
          if (extractYoutubeUrlFromJob(snapshot)) {
            missingEmbedHydrationAttemptsRef.current = 0;
            setError(null);
            return;
          }
          if (missingEmbedHydrationAttemptsRef.current >= 4) {
            setError(VIDEO_LOAD_RECOVERY_MESSAGE);
          }
        })
        .catch(() => undefined);
    }, 1800);

    return () => clearTimeout(timer);
  }, [error, job?.id, jobSucceeded, prefersDirectPlayback, refreshVideoJob, videoTabEnabled]);

  useEffect(() => {
    if (!outputUrl) return;
    setActiveTab("video");
    setVideoPaused(false);
  }, [outputUrl]);

  useEffect(() => {
    if (!job || !jobFailed) return;
      const rawMessage = job.error || job.last_message || "Job failed";
    if (hasCookieRefreshMarker(rawMessage)) {
      setError(COOKIE_REFRESH_REQUIRED_HINT);
      return;
    }
    setError(sanitizeApiErrorMessage(rawMessage));
  }, [job, jobFailed, job?.error, job?.last_message]);

  const startJob = useCallback(
    async ({
      renderOnly = false,
      upload = false,
      presetAutoOpen = false,
      audioId,
      audioUrl,
      force = false,
      reset = false,
      queryOverride,
      normalizedArtist,
      normalizedTitle,
      mixLevelsOverride,
    }: StartJobOptions = {}) => {
      setError(null);
      setIsQueryNormalizing(false);
      clearPendingPresetAutoOpenState();
      const resolvedArtist = cleanQuotedText(String(normalizedArtist || ""));
      const resolvedTitle = cleanQuotedText(String(normalizedTitle || ""));
      const shouldAutoOpenPresetVideo = Boolean(presetAutoOpen);
      const resolvedSong =
        resolvedArtist && resolvedTitle
          ? ({ artist: resolvedArtist, title: resolvedTitle } as NormalizedSong)
          : null;
      if (resolvedSong) {
        setPendingNormalizedSong(resolvedSong);
      }

      const trimmedQuery = String(queryOverride ?? (isDemoMode ? DEMO_QUERY_VALUE : query)).trim();
      if (!trimmedQuery) {
        setError("Please enter a song query");
        setProcessingVisible(false);
        setStopwatchEndAt(Date.now());
        return;
      }
      if (!isOnline) {
        setError("You're offline, connect to the internet and try again");
        setProcessingVisible(false);
        setStopwatchEndAt(Date.now());
        return;
      }
      Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium).catch(() => undefined);
      setSaveStatus(null);
      setLatestUploadedYoutubeUrl(null);
      const previousActiveJobId = String(activeJobIdRef.current || "").trim() || null;
      activeJobIdRef.current = `pending:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 10)}`;
      if (shouldAutoOpenPresetVideo) {
        pendingPresetVideoAutoOpenRef.current = true;
        pendingEmbedWaitStartAtRef.current = Date.now();
        pendingEmbedRefreshAttemptsRef.current = 0;
      }
      setLoading(true);
      setProcessingVisible(true);
      setStopwatchStartAt(Date.now());
      setStopwatchEndAt(null);

      try {
        const createJobPayload = buildCreateJobPayload(
          trimmedQuery,
          "",
          force || reset
            ? {
                ...(force ? { force: true } : {}),
                ...(reset ? { reset: true } : {}),
              }
            : undefined
        );

        // Extra pipeline controls (not part of buildCreateJobPayload).
        const payload = buildStartJobRequestPayload({
          createJobPayload,
          renderOnly,
          upload: upload && shouldRequestYoutubeUpload,
          preview: true,
          offsetSec: globalOffsetSec,
          mixLevels,
          mixLevelsOverride,
          audioId,
          audioUrl,
        });

        debugLog("[job] start requested", {
          query: trimmedQuery,
          renderOnly,
          upload,
          force,
          reset,
          audioId: audioId || null,
          audioUrl: audioUrl || null,
          queryOverride: queryOverride || null,
        });

        setError(VIDEO_LOADING_STATUS_MESSAGE);
        const response = await fetchWithTimeout(
          `${baseUrl}/jobs`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          },
          30000
        );

        if (!response.ok) {
          const msg = parseApiErrorPayload(await response.text());
          throw new Error(msg || `Request failed (${response.status})`);
        }

        const jobData = await response.json();
        const newJobId = String(jobData?.id || "").trim();
        if (!newJobId) {
          throw new Error("Video request did not return a job id, retry");
        }
        if (resolvedSong && jobData?.id) {
          setNormalizedSongByJobId((prev) => ({
            ...prev,
            [String(jobData.id)]: resolvedSong,
          }));
        }

        // Set job state to trigger polling
        delete jobPollEtagRef.current[newJobId];
        jobPollUnchangedStreakRef.current = 0;
        activeJobIdRef.current = newJobId;
        setJob((prev) => (isSameJobSnapshot(prev, jobData) ? prev : jobData));
        setProcessingVisible(true);
        setError(VIDEO_LOADING_STATUS_MESSAGE);
      } catch (e: any) {
        activeJobIdRef.current = previousActiveJobId;
        const rawMessage = e?.name === "AbortError" ? "Request timed out, try again" : e?.message;
        const refreshRequired = hasCookieRefreshMarker(rawMessage);
        const message = refreshRequired ? COOKIE_REFRESH_REQUIRED_HINT : sanitizeApiErrorMessage(rawMessage);
        debugWarn("[job] start failed", rawMessage);
        clearPendingPresetAutoOpenState();
        setError(message || "Failed to start job");
        setIsQueryNormalizing(false);
        setProcessingVisible(false);
        setStopwatchEndAt(Date.now());
      } finally {
        setLoading(false);
      }
    },
    [
      query,
      isDemoMode,
      isOnline,
      baseUrl,
      clearPendingPresetAutoOpenState,
      globalOffsetSec,
      mixLevels,
      shouldRequestYoutubeUpload,
    ]
  );

  const isPresetQueriesEasterEgg = useCallback((value: string) => {
    return String(value || "").trim().toLowerCase() === PRESET_QUERIES_EASTER_EGG_QUERY;
  }, []);

  const unlockPresetQueries = useCallback(() => {
    setPresetQueriesUnlocked(true);
    setPresetQueriesOpen(true);
    setQuery("");
    setPendingNormalizedSong(null);
    clearPendingPresetAutoOpenState();
    setError(null);
    setIsQueryNormalizing(false);
    setQueryPreflightElapsedMs(null);
    setProcessingVisible(false);
    setTimingModalVisible(false);
    setStopwatchStartAt(null);
    setStopwatchEndAt(null);
    focusQueryInput(0);
  }, [clearPendingPresetAutoOpenState, focusQueryInput]);

  const toggleAdvancedPanel = useCallback(() => {
    setAdvancedOpen((prev) => {
      const next = !prev;
      setAdvancedLastUsedOpen(next);
      return next;
    });
  }, []);

  const setAdvancedDefaultModePreference = useCallback(
    (mode: AdvancedDefaultMode) => {
      setAdvancedDefaultMode(mode);
      if (!showAdvancedButton) return;
      setAdvancedOpen(resolveAdvancedOpenFromMode(mode, advancedLastUsedOpen));
    },
    [advancedLastUsedOpen, showAdvancedButton]
  );

  const requestStartJob = useCallback(
    (options: StartJobOptions = {}): boolean => {
      if (loading) return false;

      const trimmedQuery = String(options.queryOverride ?? (isDemoMode ? DEMO_QUERY_VALUE : query)).trim();
      if (!trimmedQuery) {
        setError("Please enter a song query");
        setStopwatchEndAt(Date.now());
        return false;
      }
      if (
        !options.renderOnly &&
        !options.audioId &&
        !options.audioUrl &&
        isPresetQueriesEasterEgg(trimmedQuery)
      ) {
        unlockPresetQueries();
        return false;
      }
      if (!isOnline) {
        setError("You're offline, connect to the internet and try again");
        setStopwatchEndAt(Date.now());
        return false;
      }
      Keyboard.dismiss();
      requestStartJobTokenRef.current += 1;
      setTimingModalVisible(showTimers);
      setQueryPreflightElapsedMs(null);
      setIsQueryNormalizing(false);

      const parsed = splitArtistAndTitle(trimmedQuery);
      const normalizedArtist = parsed?.artist || "";
      const normalizedTitle = parsed?.title || "";
      const normalizedQuery =
        normalizedArtist && normalizedTitle ? `${normalizedArtist} - ${normalizedTitle}` : "";

      if (normalizedArtist && normalizedTitle) {
        setPendingNormalizedSong({ artist: normalizedArtist, title: normalizedTitle });
        rememberRecentNormalizedSong({
          query: normalizedQuery || trimmedQuery,
          artist: normalizedArtist,
          title: normalizedTitle,
        });
      } else {
        setPendingNormalizedSong(null);
        rememberNormalizedSongFromSource(trimmedQuery);
      }

      startJob({
        ...options,
        queryOverride: trimmedQuery,
        ...(normalizedArtist && normalizedTitle
          ? {
              normalizedArtist,
              normalizedTitle,
            }
          : {}),
      }).catch(() => undefined);
      return true;
    },
    [
      isDemoMode,
      isPresetQueriesEasterEgg,
      isOnline,
      loading,
      query,
      rememberNormalizedSongFromSource,
      rememberRecentNormalizedSong,
      showTimers,
      startJob,
      unlockPresetQueries,
    ]
  );

  const pickSongForMe = useCallback(() => {
    if (loading) return;
    const pool = buildPickSongForMePool(recentNormalizedSongs);
    if (!pool.length) return;
    const selected = pool[Math.floor(Math.random() * pool.length)] || "";
    if (!selected) return;
    setQuery(selected);
  }, [loading, recentNormalizedSongs]);

  const applyCurrentVideoTuning = useCallback(() => {
    const request = buildApplyVideoTuningRequest({
      query: job?.query || query,
      mixLevels,
    });
    if (!request) {
      setError("Enter a song query before applying timing changes");
      return;
    }
    requestStartJob(request);
  }, [job?.query, mixLevels, query, requestStartJob]);

  useEffect(() => {
    if (!SMOKE_TEST_QUERY) return;
    if (smokeTestStartedRef.current) return;
    if (loading || isQueryNormalizing || !isOnline) return;
    smokeTestStartedRef.current = true;
    setQuery(SMOKE_TEST_QUERY);
    const presetMix: MixLevelsState = {
      vocals: SMOKE_TEST_VOCALS,
      bass: 100,
      drums: 100,
      other: 100,
    };
    setMixLevels(presetMix);
    requestStartJob({
      renderOnly: false,
      upload: !prefersDirectPlayback,
      presetAutoOpen: true,
      queryOverride: SMOKE_TEST_QUERY,
      mixLevelsOverride: presetMix,
    });
  }, [isOnline, isQueryNormalizing, loading, prefersDirectPlayback, requestStartJob]);

  const loadSourceSearchResults = useCallback(
    async (searchQuery: string) => {
      const requestToken = sourceSearchRequestTokenRef.current + 1;
      sourceSearchRequestTokenRef.current = requestToken;
      const trimmed = String(searchQuery || "").trim();
      if (!trimmed) {
        setSourceSearchResults([]);
        setSourcePickerError("Enter a query first");
        return;
      }
      setSourcePickerLoading(true);
      setSourcePickerError(null);
      setSourceSearchResults([]);
      try {
        const res = await fetchWithTimeout(
          `${baseUrl}/source/search-results?q=${encodeURIComponent(trimmed)}&limit=6`,
          {},
          15000
        );
        if (!res.ok) {
          const msg = parseApiErrorPayload(await res.text());
          throw new Error(msg || `Could not load source results (${res.status})`);
        }
        if (sourceSearchRequestTokenRef.current !== requestToken) {
          return;
        }
        const payload = (await res.json()) as { results?: Array<Record<string, unknown>> };
        const rows = Array.isArray(payload?.results) ? payload.results : [];
        const seenVideoIds = new Set<string>();
        const normalized: SourceSearchResult[] = [];
        for (const row of rows) {
          const videoId = String(row?.video_id || "").trim();
          const title = String(row?.title || "").trim();
          if (!videoId || !title || seenVideoIds.has(videoId)) continue;
          seenVideoIds.add(videoId);
          normalized.push({
            video_id: videoId,
            title,
            duration: Number.isFinite(Number(row?.duration)) ? Number(row?.duration) : null,
            thumbnail: String(row?.thumbnail || "").trim() || null,
            uploader: String(row?.uploader || "").trim() || null,
          });
        }
        if (sourceSearchRequestTokenRef.current !== requestToken) {
          return;
        }
        if (!normalized.length) {
          setSourcePickerError("No source matches found for this query");
        }
        setSourceSearchResults(normalized);
      } catch (err: any) {
        if (sourceSearchRequestTokenRef.current !== requestToken) {
          return;
        }
        const message = sanitizeApiErrorMessage(
          err?.name === "AbortError" ? "Source search timed out, please try again" : err?.message
        );
        setSourcePickerError(message || "Could not load source results");
      } finally {
        if (sourceSearchRequestTokenRef.current === requestToken) {
          setSourcePickerLoading(false);
        }
      }
    },
    [baseUrl]
  );

  const openSourcePicker = useCallback(() => {
    const searchQuery =
      String(strictLyricsFailure?.queryHint || "").trim() ||
      String(query || "").trim() ||
      String(job?.query || "").trim();
    setSourcePickerVisible(true);
    loadSourceSearchResults(searchQuery).catch(() => undefined);
  }, [job?.query, loadSourceSearchResults, query, strictLyricsFailure?.queryHint]);

  const applySuggestedQuery = useCallback(() => {
    const nextQuery = String(strictLyricsFailure?.suggestedQuery || "").trim();
    if (!nextQuery) return;
    setQuery(nextQuery);
    setError(null);
    focusQueryInput();
  }, [focusQueryInput, strictLyricsFailure?.suggestedQuery]);

  const chooseSourceResult = useCallback(
    (row: SourceSearchResult) => {
      const normalizedQuery = normalizeResultQueryText(row.title, row.uploader);
      const nextQuery = normalizedQuery || String(strictLyricsFailure?.queryHint || query || "").trim();
      if (nextQuery) setQuery(nextQuery);
      setSourcePickerVisible(false);
      setSourcePickerError(null);
      setSourceSearchResults([]);
      setError(null);
      requestStartJob({
        renderOnly: false,
        force: true,
        audioId: row.video_id,
        queryOverride: nextQuery,
      });
    },
    [query, requestStartJob, strictLyricsFailure?.queryHint]
  );

  const closeSourcePicker = useCallback(() => {
    sourceSearchRequestTokenRef.current += 1;
    setSourcePickerVisible(false);
    setSourcePickerError(null);
    setSourcePickerLoading(false);
    setSourceSearchResults([]);
  }, []);

  const retryJob = useCallback(() => {
    if (loading) return;
    resetUiState({ clearQuery: false });
    requestStartJob({ renderOnly: false });
  }, [loading, requestStartJob, resetUiState]);

  const retryVideoLoad = useCallback(async () => {
    if (!job?.id) return;
    clearPendingPresetAutoOpenState();
    pendingPresetVideoAutoOpenRef.current = true;
    pendingEmbedWaitStartAtRef.current = Date.now();
    pendingEmbedRefreshAttemptsRef.current = 0;
    missingEmbedHydrationAttemptsRef.current = 0;
    setYoutubeEmbedReady(false);
    setProcessingVisible(true);
    setError(VIDEO_LOADING_STATUS_MESSAGE);

    await checkApi(4000).catch(() => undefined);
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const snapshot = await refreshVideoJob();
      if (extractYoutubeUrlFromJob(snapshot)) {
        setError(null);
        return;
      }
      await sleepMs(500 + attempt * 300);
    }

    clearPendingPresetAutoOpenState();
    setLoading(false);
    setProcessingVisible(false);
    setError(VIDEO_LOAD_RECOVERY_MESSAGE);
  }, [checkApi, clearPendingPresetAutoOpenState, job?.id, refreshVideoJob]);

  const handleYoutubeEmbedReady = useCallback(() => {
    setYoutubeEmbedReady(true);
    if (pendingPresetVideoAutoOpenRef.current) {
      clearPendingPresetAutoOpenState();
      setLoading(false);
      setProcessingVisible(false);
    }
    setError((prev) => {
      if (!prev) return prev;
      const text = String(prev || "").trim();
      if (text === VIDEO_LOAD_RECOVERY_MESSAGE || isProgressStatusMessage(text)) return null;
      return prev;
    });
  }, [clearPendingPresetAutoOpenState]);

  const handleYoutubeEmbedFatalError = useCallback((message: string) => {
    const normalized = withoutTrailingPeriod(message) || VIDEO_LOAD_RECOVERY_MESSAGE;
    setYoutubeEmbedReady(false);
    clearPendingPresetAutoOpenState();
    setLoading(false);
    setProcessingVisible(false);
    setError(normalized);
  }, [clearPendingPresetAutoOpenState]);

  const cancelProcessing = () => {
    requestStartJobTokenRef.current += 1;
    clearPendingPresetAutoOpenState();
    const jobId = job?.id;
    if (jobId && jobInProgress) {
      fetchWithTimeout(
        `${baseUrl}/jobs/${jobId}/cancel`,
        { method: "POST" },
        5000
      ).catch(() => undefined);
    }
    debugLog("[job] cancel requested", { jobId });
    setProcessingVisible(false);
    setIsQueryNormalizing(false);
    setQueryPreflightElapsedMs(null);
    setIsExporting(false);
    setShowPhotoSettingsAction(false);
    exportActionInFlightRef.current = false;
    setJob((prev) =>
      prev
        ? {
            ...prev,
            status: "cancelled",
            cancelled_at: Date.now(),
            finished_at: Date.now(),
            error: "Cancelled by user",
          }
        : prev
    );
    setLoading(false);
    setError(GENERIC_ERROR_MESSAGE);
    setSaveStatus(null);
    setActiveTab("search");
    setStopwatchStartAt(null);
    setStopwatchEndAt(null);
  };

  const dismissProcessing = useCallback(() => {
    setProcessingVisible(false);
  }, []);

  const resetPipeline = () => {
    debugLog("[job] reset pipeline");
    resetUiState({ clearQuery: true });
  };

  const downloadOutputToCache = useCallback(
    async (filename: string) => {
      const downloadUrl = String(outputUrl || "").trim();
      if (!downloadUrl) throw new Error("No output URL");
      const legacyCacheRoot = (FileSystemLegacy as any).cacheDirectory;
      const strategy = resolveOutputDownloadStrategy({
        modernCacheDirectoryAvailable: Boolean(FileSystemPaths?.cache),
        modernDownloadFileAsyncAvailable: typeof (FileSystemFile as any).downloadFileAsync === "function",
        legacyCacheDirectoryAvailable: Boolean(legacyCacheRoot),
        legacyDownloadAsyncAvailable: typeof (FileSystemLegacy as any).downloadAsync === "function",
      });

      if (strategy === "modern") {
        const file = new FileSystemFile(FileSystemPaths.cache, filename);
        const download = await (FileSystemFile as any).downloadFileAsync(downloadUrl, file, { idempotent: true });
        const uri = normalizeFileUri(download?.uri || file?.uri || "");
        if (!uri) throw new Error("Downloaded file URI is empty");
        return uri;
      }

      if (strategy === "legacy") {
        const target = `${legacyCacheRoot}${filename}`;
        const download = await (FileSystemLegacy as any).downloadAsync(downloadUrl, target);
        const uri = normalizeFileUri(download?.uri || target);
        if (!uri) throw new Error("Downloaded file URI is empty");
        return uri;
      }

      throw new Error("File download API unavailable");
    },
    [outputUrl]
  );

  const handleShare = useCallback(async () => {
    if (exportActionInFlightRef.current) return;
    if (!outputUrl) {
      setShowPhotoSettingsAction(false);
      setSaveStatus(formatUiStatus("Nothing to share yet"));
      return;
    }
    setSaveStatus(null);
    setShowPhotoSettingsAction(false);
    exportActionInFlightRef.current = true;
    setIsExporting(true);
    try {
      const slug = job?.slug || "karao";
      const filename = `${slug}-${Date.now()}-share.mp4`;
      const shareUri = await downloadOutputToCache(filename);
      let lastShareError: any = null;

      const canNativeFileShare = await Sharing.isAvailableAsync().catch(() => false);
      if (canNativeFileShare) {
        try {
          await Sharing.shareAsync(shareUri, {
            mimeType: "video/mp4",
            dialogTitle: "Share Mixterious Karaoke",
          });
          setSaveStatus(formatUiStatus("Shared video"));
          Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => undefined);
          return;
        } catch (err: any) {
          lastShareError = err;
        }
      }

      const filePayloads: Array<Record<string, string>> = [
        { title: "Mixterious Karaoke", url: shareUri },
        { title: "Mixterious Karaoke", url: encodeURI(shareUri) },
        { title: "Mixterious Karaoke", message: shareUri, url: shareUri },
      ];

      let result: { action: string; activityType?: string | null } | null = null;
      for (const payload of filePayloads) {
        try {
          result = await Share.share(payload as any);
          break;
        } catch (err: any) {
          lastShareError = err;
        }
      }

      if (!result) {
        throw lastShareError || new Error("Share failed");
      }

      if (result.action === Share.sharedAction) {
        setSaveStatus(formatUiStatus("Shared video"));
      } else {
        setSaveStatus(formatUiStatus("Share cancelled"));
      }
      Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => undefined);
    } catch (err: any) {
      debugWarn("[share] failed", err?.message || err);
      setSaveStatus(formatUiError("Share failed"));
      Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error).catch(() => undefined);
    } finally {
      exportActionInFlightRef.current = false;
      setIsExporting(false);
    }
  }, [downloadOutputToCache, outputUrl, job?.slug]);

  const saveCurrentVideoToPhotos = useCallback(
    async (options: { autoSave?: boolean } = {}) => {
      const { autoSave = false } = options;
      if (exportActionInFlightRef.current) return "busy" as const;
      if (!outputUrl) {
        if (!autoSave) {
          setShowPhotoSettingsAction(false);
          setSaveStatus(formatUiStatus("Nothing to save yet"));
        }
        return "no_output" as const;
      }

      if (!autoSave) {
        setSaveStatus(null);
      }
      setShowPhotoSettingsAction(false);
      exportActionInFlightRef.current = true;
      setIsExporting(true);
      try {
        let permission = await MediaLibrary.getPermissionsAsync(true);
        if (shouldPromptForPhotoLibraryWritePermission(permission)) {
          permission = await MediaLibrary.requestPermissionsAsync(true);
        }
        const canWrite = canWriteToPhotoLibrary(permission);
        if (!canWrite) {
          setSaveStatus(
            formatUiError(
              autoSave ? `Auto-save needs Photos access. ${PHOTO_LIBRARY_SETTINGS_REQUIRED_MESSAGE}` : PHOTO_LIBRARY_SETTINGS_REQUIRED_MESSAGE
            )
          );
          setShowPhotoSettingsAction(true);
          if (!autoSave) {
            Haptics.notificationAsync(Haptics.NotificationFeedbackType.Warning).catch(() => undefined);
          }
          return "failed" as const;
        }

        const slug = job?.slug || "karao";
        const filename = `${slug}-${Date.now()}.mp4`;
        const localUri = await downloadOutputToCache(filename);
        const asset = await MediaLibrary.createAssetAsync(localUri);
        if (Platform.OS === "ios") {
          await MediaLibrary.createAlbumAsync("Mixterious", asset, false).catch(() => undefined);
        }
        setSaveStatus(formatUiStatus(autoSave ? "Auto-saved to Photos" : "Saved to Photos"));
        if (autoSaveTargetKey) {
          autoSaveHandledKeysRef.current.add(autoSaveTargetKey);
        }
        setShowPhotoSettingsAction(false);
        if (!autoSave) {
          Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success).catch(() => undefined);
        }
        return "saved" as const;
      } catch (err: any) {
        debugWarn("[save] failed", err?.message || err);
        if (Platform.OS === "ios" && isLikelyIosSimulatorSaveError(err)) {
          setSaveStatus(
            formatUiError(
              autoSave
                ? "Auto-save is unavailable on iOS Simulator, use a physical iPhone"
                : "Save is unavailable on iOS Simulator, use a physical iPhone"
            )
          );
          setShowPhotoSettingsAction(false);
        } else if (isLikelyPhotoPermissionSaveError(err)) {
          setSaveStatus(
            formatUiError(
              autoSave ? `Auto-save needs Photos access. ${PHOTO_LIBRARY_SETTINGS_REQUIRED_MESSAGE}` : PHOTO_LIBRARY_SETTINGS_REQUIRED_MESSAGE
            )
          );
          setShowPhotoSettingsAction(true);
        } else {
          setSaveStatus(formatUiError(autoSave ? "Auto-save failed" : "Save failed"));
          setShowPhotoSettingsAction(false);
        }
        if (!autoSave) {
          Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error).catch(() => undefined);
        }
        return "failed" as const;
      } finally {
        exportActionInFlightRef.current = false;
        setIsExporting(false);
      }
    },
    [autoSaveTargetKey, downloadOutputToCache, outputUrl, job?.slug]
  );

  const handleSave = useCallback(async () => {
    await saveCurrentVideoToPhotos({ autoSave: false });
  }, [saveCurrentVideoToPhotos]);

  useEffect(() => {
    if (!autoSaveTargetKey) return;
    if (!autoSaveGeneratedVideos || isDemoMode) {
      autoSaveHandledKeysRef.current.add(autoSaveTargetKey);
      return;
    }
    if (isExporting) return;
    if (autoSaveHandledKeysRef.current.has(autoSaveTargetKey)) return;
    autoSaveHandledKeysRef.current.add(autoSaveTargetKey);
    saveCurrentVideoToPhotos({ autoSave: true })
      .then((result) => {
        if (result === "busy") {
          autoSaveHandledKeysRef.current.delete(autoSaveTargetKey);
        }
      })
      .catch(() => undefined);
  }, [autoSaveGeneratedVideos, autoSaveTargetKey, isDemoMode, isExporting, saveCurrentVideoToPhotos]);

  const enableBackgroundNotifications = useCallback(async () => {
    if (isDemoMode) {
      setNotificationsEnabled(false);
      setNotificationsStatusMessage(formatUiError("Background notifications are disabled in demo mode"));
      return;
    }

    setNotificationsStatusMessage(null);

    try {
      const currentPermissions = await Notifications.getPermissionsAsync();
      let finalStatus = currentPermissions.status;

      if (
        shouldPromptForNotificationPermission({
          enableNotifications: true,
          existingStatus: currentPermissions.status,
          canAskAgain: currentPermissions.canAskAgain,
        })
      ) {
        const requestedPermissions = await Notifications.requestPermissionsAsync();
        finalStatus = requestedPermissions.status;
      }

      setNotificationPermissionStatus(finalStatus as NotificationPermissionStatus);

      if (isPermissionGranted(finalStatus)) {
        setNotificationsEnabled(true);
        setNotificationsStatusMessage(formatUiStatus("Background notifications enabled"));
      } else {
        setNotificationsEnabled(false);
        setNotificationsStatusMessage(
          formatUiError("Notifications remain disabled, enable notifications in iOS Settings to get background alerts")
        );
      }
    } catch (err: any) {
      debugWarn("[notifications] permission update failed", err?.message || err);
      setNotificationsEnabled(false);
      setNotificationsStatusMessage(formatUiError("Unable to update notifications right now"));
    }
  }, [isDemoMode]);

  const disableBackgroundNotifications = useCallback(() => {
    setNotificationsEnabled(false);
    setNotificationsStatusMessage(formatUiStatus("Background notifications disabled"));
  }, []);

  const resetAllSettingsToDefaults = useCallback(() => {
    Alert.alert(
      "Reset all settings?",
      "This restores the major app controls on this screen to their defaults.",
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Reset",
          style: "destructive",
          onPress: () => {
            disableBackgroundNotifications();
            setShowRecentNormalizedSongs(true);
            setShowTimers(DEFAULT_SHOW_TIMERS);
            setShowAdvancedButton(DEFAULT_SHOW_ADVANCED_BUTTON);
            setAdvancedDefaultMode(DEFAULT_ADVANCED_DEFAULT_MODE);
            setAdvancedLastUsedOpen(false);
            setAdvancedOpen(resolveAdvancedOpenFromMode(DEFAULT_ADVANCED_DEFAULT_MODE, false));
            setBackgroundVideoPlaybackEnabled(true);
            setAutoSaveGeneratedVideos(false);
            setSearchAutoCorrectEnabled(false);
            setGlobalOffsetSec(0);
            setMixLevels({ ...DEFAULT_MIX_LEVELS });
            setNotificationsStatusMessage(formatUiStatus("Settings restored to defaults"));
          },
        },
      ]
    );
  }, [disableBackgroundNotifications]);

  const openAppSettings = useCallback(() => {
    Linking.openSettings().catch(() => undefined);
  }, []);

  const switchTab = (tab: TabKey) => {
    if (tab === "history") {
      setActiveTab("search");
      return;
    }
    if (tab === "video" && !videoTabEnabled) return;
    setActiveTab(tab);
    if (tab === "search" && isProgressStatusMessage(error || "")) {
      setError(null);
    }
    if (tab === "settings") {
      Keyboard.dismiss();
      queryInputRef.current?.blur();
    }
    if (tab === "video") {
      if (outputUrl) {
        refreshVideoSurface("switch_tab_video");
      } else {
        setVideoPaused(false);
        try {
          videoPlayer.play();
        } catch {
          // best-effort autoplay on tab navigation
        }
      }
      if (jobInProgress && !outputUrl) {
        setProcessingVisible(true);
      }
    }
  };

  const toggleVideoPlayback = useCallback(() => {
    if (!outputUrl) return;
    setVideoPaused((prev) => !prev);
  }, [outputUrl]);

  const isLandscapeVideoOnly = activeTab === "video" && isLandscape;

  if (isLandscapeVideoOnly) {
    return (
      <View style={styles.safeLandscapeVideo}>
        <StatusBar hidden />
        {outputUrl ? (
          <OutputVideo
            key={`landscape:${videoSurfaceKey}`}
            player={videoPlayer}
            uri={outputUrl}
            fullscreen
            nativeControls
            onMissingUrl={() => {
              refreshVideoJob().catch(() => undefined);
            }}
          />
        ) : (
          <View style={styles.landscapeVideoPlaceholder}>
            <Text style={styles.placeholderText}>
              {jobFailed ? "Video generation failed." : "Waiting for the video..."}
            </Text>
          </View>
        )}
      </View>
    );
  }

  return (
    <SafeAreaView style={styles.safe}>
      <StatusBar hidden={false} barStyle="light-content" translucent={false} />
      <View style={styles.header}>
        <View style={styles.headerLeft}>
          <Image source={APP_HEADER_ICON} style={styles.headerAppIcon} />
          <Text style={styles.headerTitle}>Mixterious</Text>
        </View>
        <View style={styles.headerRight}>
          {showTimers ? (
            <ElapsedTimerText startAt={stopwatchStartAt} endAt={stopwatchEndAt} placeholder="—" style={styles.headerTimer} />
          ) : null}
          {outputUrl ? (
            <Pressable style={styles.headerPlaybackButton} onPress={toggleVideoPlayback}>
              <View style={styles.headerPlaybackGlyph}>
                {videoIsPlaying && !videoPaused ? (
                  <View style={styles.headerPauseGlyph}>
                    <View style={styles.headerPauseBar} />
                    <View style={styles.headerPauseBar} />
                  </View>
                ) : (
                  <View style={styles.headerPlayGlyph} />
                )}
              </View>
            </Pressable>
          ) : null}
        </View>
      </View>
      {!isOnline && (
        <View style={styles.offlineBanner}>
          <Text style={styles.offlineText}>No connection. Check your network.</Text>
        </View>
      )}
      {apiReachable === false && (
        <View style={styles.offlineBanner}>
          <Text style={styles.offlineText}>API unreachable at {baseUrl}. Check server or firewall.</Text>
        </View>
      )}
      {!appIsActive && jobInProgress && (
        <View style={styles.backgroundBanner}>
          <Text style={styles.backgroundText}>
            {notificationsConfigured
              ? "🎵 Processing in background... You'll get notified when ready!"
              : "🎵 Processing in background... Enable Background Notifications in Settings to get alerts when ready."}
          </Text>
        </View>
      )}
      {outputUrl && backgroundVideoPlaybackEnabled && activeTab !== "video" ? (
        <View style={styles.backgroundPlaybackHolder}>
          <VideoView
            key={`background:${videoSurfaceKey}`}
            style={styles.backgroundPlaybackView}
            player={videoPlayer}
            nativeControls={false}
            contentFit="contain"
          />
        </View>
      ) : null}

      <KeyboardAvoidingView
        style={[
          styles.content,
          activeTab === "video" && styles.contentVideoTab,
          activeTab === "settings" && styles.contentSettingsTab,
        ]}
        behavior={Platform.OS === "ios" ? "padding" : undefined}
      >
        {activeTab === "settings" ? (
          <View style={styles.feedbackTab}>
            <ScrollView
              style={styles.feedbackScroll}
              contentContainerStyle={[styles.feedbackScrollContent, styles.fixedWidthContentContainer]}
              keyboardShouldPersistTaps="handled"
              directionalLockEnabled
              alwaysBounceHorizontal={false}
              showsVerticalScrollIndicator={false}
              showsHorizontalScrollIndicator={false}
            >
              <View style={styles.settingsPanelContent}>
                <Text style={styles.label}>Settings</Text>
                <Text style={styles.searchHelper}>
                  Enjoying the app so far? We'd love to hear about your app experience with Mixterious
                </Text>
                <Pressable
                  style={styles.settingsContactLink}
                  onPress={() => Linking.openURL("mailto:contact@miguelengineer.com").catch(() => undefined)}
                >
                  <Text style={styles.settingsContactLinkText}>contact@miguelengineer.com</Text>
                </Pressable>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Background Notifications</Text>
                  <Text style={styles.settingsSectionBody}>
                    Get notified when your karaoke video is ready while Mixterious runs in the background.
                  </Text>
                  <Pressable
                    style={[styles.secondaryButton, !notificationsPreferenceReady && styles.primaryButtonDisabled]}
                    disabled={!notificationsPreferenceReady}
                    onPress={() => {
                      if (notificationsConfigured) {
                        disableBackgroundNotifications();
                        return;
                      }
                      enableBackgroundNotifications().catch(() => undefined);
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {notificationsConfigured ? "Disable Background Notifications" : "Enable Background Notifications"}
                    </Text>
                  </Pressable>
                  {notificationPermissionStatus === "denied" ? (
                    <Pressable style={styles.secondaryButton} onPress={openAppSettings}>
                      <Text style={styles.secondaryButtonText}>Open iOS Settings</Text>
                    </Pressable>
                  ) : null}
                  <Text style={notificationsSummary.startsWith("❌") ? styles.error : styles.smallStatus}>
                    {notificationsPreferenceReady
                      ? notificationsSummary
                      : formatUiStatus("Loading notification settings...")}
                  </Text>
                </View>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Background Video Playback</Text>
                  <Text style={styles.settingsSectionBody}>
                    Keep video playback running when Mixterious is in the background or when you leave the Video tab.
                  </Text>
                  <Pressable
                    style={styles.secondaryButton}
                    onPress={() => {
                      setBackgroundVideoPlaybackEnabled((prev) => !prev);
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {backgroundVideoPlaybackEnabled
                        ? "Disable Background Video Playback"
                        : "Enable Background Video Playback"}
                    </Text>
                  </Pressable>
                </View>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Auto-save Generated Videos</Text>
                  <Text style={styles.settingsSectionBody}>
                    Automatically save each completed karaoke video to Photos.
                  </Text>
                  <Pressable
                    style={styles.secondaryButton}
                    onPress={() => {
                      setAutoSaveGeneratedVideos((prev) => !prev);
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {autoSaveGeneratedVideos ? "Disable Auto-save" : "Enable Auto-save"}
                    </Text>
                  </Pressable>
                </View>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Interface</Text>
                  <Text style={styles.settingsSectionBody}>
                    Show or hide Recently Searched in Search.
                  </Text>
                  <Pressable
                    style={styles.secondaryButton}
                    onPress={() => {
                      setShowRecentNormalizedSongs((prev) => !prev);
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {showRecentNormalizedSongs ? "Hide Recent Songs Searched" : "Show Recent Songs Searched"}
                    </Text>
                  </Pressable>
                  <View style={styles.advancedSeparator} />
                  <Text style={styles.settingsSectionBody}>
                    Show or hide timer readouts across the app.
                  </Text>
                  <Pressable
                    style={styles.secondaryButton}
                    onPress={() => {
                      setShowTimers((prev) => !prev);
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {showTimers ? "Hide Timers" : "Show Timers"}
                    </Text>
                  </Pressable>
                  <View style={styles.advancedSeparator} />
                  <Text style={styles.settingsSectionBody}>
                    Show or hide the Advanced button in Search and Video.
                  </Text>
                  <Pressable
                    style={styles.secondaryButton}
                    onPress={() => {
                      setShowAdvancedButton((prev) => {
                        const next = !prev;
                        if (next) {
                          setAdvancedOpen(resolveAdvancedOpenFromMode(advancedDefaultMode, advancedLastUsedOpen));
                        }
                        return next;
                      });
                    }}
                  >
                    <Text style={styles.secondaryButtonText}>
                      {showAdvancedButton ? "Hide Advanced Button" : "Show Advanced Button"}
                    </Text>
                  </Pressable>
                  <Text style={styles.settingsSectionBody}>
                    Initial Advanced state when shown.
                  </Text>
                  <View style={styles.advancedModeChips}>
                    {ADVANCED_DEFAULT_MODE_OPTIONS.map((option) => {
                      const isSelected = advancedDefaultMode === option.value;
                      return (
                        <Pressable
                          key={option.value}
                          style={[styles.advancedModeChip, isSelected && styles.advancedModeChipSelected]}
                          onPress={() => setAdvancedDefaultModePreference(option.value)}
                        >
                          <Text
                            style={[
                              styles.advancedModeChipText,
                              isSelected && styles.advancedModeChipTextSelected,
                            ]}
                          >
                            {option.label}
                          </Text>
                        </Pressable>
                      );
                    })}
                  </View>
                </View>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Default Vocal / Instrument Volumes</Text>
                  <Text style={styles.settingsSectionBody}>
                    Set default vocals, bass, drums, and other percentages for new renders.
                  </Text>
                  {STEM_KEYS.map((stem) => (
                    <View key={stem} style={styles.mixGridRow}>
                      <View style={[styles.mixGridCell, styles.mixGridLabelCell, styles.mixGridLabelCellSettings]}>
                        <Text style={styles.mixGridLabel} numberOfLines={1} adjustsFontSizeToFit minimumFontScale={0.85}>
                          {STEM_LABELS[stem]}
                        </Text>
                      </View>
                      <Pressable
                        style={[
                          styles.mixButton,
                          styles.mixGridCell,
                          styles.mixGridButtonCell,
                          styles.mixGridButtonCellSettings,
                          isDemoMode && styles.mixButtonDisabled,
                        ]}
                        onPress={() => adjustMixLevel(stem, -10)}
                        disabled={isDemoMode}
                      >
                        <Text style={styles.tuningButtonText} numberOfLines={1}>-10%</Text>
                      </Pressable>
                      <Pressable
                        style={[
                          styles.mixGridCell,
                          styles.mixGridValueWrap,
                          styles.mixGridValueWrapSettings,
                          styles.mixGridValueTapTarget,
                          isDemoMode && styles.mixGridValueTapTargetDisabled,
                        ]}
                        onPress={() => openMixLevelEditor(stem)}
                        disabled={isDemoMode}
                        accessibilityRole="button"
                        accessibilityLabel={`Set ${STEM_LABELS[stem]} volume`}
                      >
                        <View style={styles.mixGridValueContent}>
                          <Text style={styles.mixGridValue} numberOfLines={1}>{mixLevels[stem]}%</Text>
                          <View style={styles.mixGridEditBadge}>
                            <Text style={styles.mixGridEditBadgeText}>EDIT</Text>
                          </View>
                        </View>
                      </Pressable>
                      <Pressable
                        style={[
                          styles.mixButton,
                          styles.mixGridCell,
                          styles.mixGridButtonCell,
                          styles.mixGridButtonCellSettings,
                          isDemoMode && styles.mixButtonDisabled,
                        ]}
                        onPress={() => adjustMixLevel(stem, 10)}
                        disabled={isDemoMode}
                      >
                        <Text style={styles.tuningButtonText} numberOfLines={1}>+10%</Text>
                      </Pressable>
                      <View style={[styles.mixGridCell, styles.mixGridSpacerCell, styles.mixGridSpacerCellSettings]} />
                    </View>
                  ))}
                  <Pressable
                    style={[styles.secondaryButton, isDemoMode && styles.primaryButtonDisabled]}
                    onPress={() => {
                      setMixLevels({ ...DEFAULT_MIX_LEVELS });
                    }}
                    disabled={isDemoMode}
                  >
                    <Text style={styles.secondaryButtonText}>Reset Defaults to 100%</Text>
                  </Pressable>
                </View>
                <View style={styles.settingsCard}>
                  <Text style={styles.settingsSectionTitle}>Reset Settings</Text>
                  <Text style={styles.settingsSectionBody}>
                    Restore all options on this screen to their defaults.
                  </Text>
                  <Pressable style={styles.secondaryButton} onPress={resetAllSettingsToDefaults}>
                    <Text style={styles.secondaryButtonText}>Reset All Settings to Defaults</Text>
                  </Pressable>
                </View>
              </View>
              <View style={styles.settingsVersionFooter}>
                <Text style={styles.settingsVersionText}>{APP_DISPLAY_VERSION}</Text>
              </View>
            </ScrollView>
          </View>
        ) : activeTab === "history" ? (
          <View style={styles.tabContent}>
            <Text style={styles.label}>Job History ({jobHistory.length})</Text>
            <ScrollView
              style={styles.historyList}
              showsVerticalScrollIndicator={false}
              showsHorizontalScrollIndicator={false}
            >
              {jobHistory.length === 0 ? (
                <Text style={styles.placeholderText}>No completed jobs yet</Text>
              ) : (
                jobHistory.map((historyJob) => (
                  <Pressable
                    key={historyJob.id}
                    style={styles.historyItem}
                    onPress={() => {
                      setJob(historyJob);
                      switchTab("video");
                    }}
                  >
                    <Text style={styles.historyQuery}>{historyJob.query}</Text>
                    <Text style={styles.historyStatus}>
                      {historyJob.status === "succeeded" ? "✓" : "✗"} {historyJob.status}
                    </Text>
                    {historyJob.created_at && (
                      <Text style={styles.historyDate}>
                        {new Date(historyJob.created_at * 1000).toLocaleString()}
                      </Text>
                    )}
                  </Pressable>
                ))
              )}
            </ScrollView>
            <Pressable
              style={styles.secondaryButton}
              onPress={() => {
                Alert.alert(
                  "Clear history?",
                  "This will remove completed jobs from this device",
                  [
                    { text: "Cancel", style: "cancel" },
                    {
                      text: "Clear",
                      style: "destructive",
                      onPress: () => {
                        AsyncStorage.removeItem("@job_history").catch(() => undefined);
                        setJobHistory([]);
                      },
                    },
                  ]
                );
              }}
            >
              <Text style={styles.secondaryButtonText}>Clear History</Text>
            </Pressable>
          </View>
        ) : activeTab === "search" ? (
          <TouchableWithoutFeedback onPress={Keyboard.dismiss} accessible={false}>
          <ScrollView
            style={styles.searchScroll}
            contentContainerStyle={[
              styles.searchTabContent,
              styles.searchScrollContent,
              styles.fixedWidthContentContainer,
            ]}
            keyboardShouldPersistTaps="handled"
            nestedScrollEnabled
            directionalLockEnabled
            alwaysBounceHorizontal={false}
            showsVerticalScrollIndicator={false}
            showsHorizontalScrollIndicator={false}
          >
            <Text style={styles.label}>Search for a sing-a-long video</Text>
            <Text style={styles.searchHelper}>
              Enter an artist and song title, then view the video you want
            </Text>
            {isDemoMode && (
              <View style={styles.demoModeBanner}>
                <Text style={styles.demoModeBannerText}>
                  Demo mode: limited controls are enabled for a consistent preview experience
                </Text>
              </View>
            )}
            <View style={styles.queryInputWrap}>
              <TextInput
                ref={queryInputRef}
                style={[styles.input, styles.searchInputProminent, styles.queryInput, isDemoMode && styles.inputDisabled]}
                value={query}
                onChangeText={setQuery}
                autoCapitalize="none"
                autoCorrect={searchAutoCorrectEnabled}
                spellCheck={searchAutoCorrectEnabled}
                editable={!isDemoMode}
                placeholder="Enter an artist and song title, then view the video you want"
                placeholderTextColor="#5c6472"
                returnKeyType="search"
                blurOnSubmit={false}
                onSubmitEditing={() => {
                  Keyboard.dismiss();
                }}
              />
              {!isDemoMode && query.trim().length > 0 ? (
                <Pressable
                  style={styles.queryClearButton}
                  onPress={() => {
                    setQuery("");
                    setPendingNormalizedSong(null);
                    setError(null);
                    focusQueryInput(0);
                  }}
                  hitSlop={8}
                  accessibilityRole="button"
                  accessibilityLabel="Clear search field"
                >
                  <Text style={styles.queryClearButtonText}>✕</Text>
                </Pressable>
              ) : null}
            </View>
            <View style={styles.searchQuickToggleRow}>
              <Pressable
                style={[
                  styles.quickToggleChip,
                  searchAutoCorrectEnabled && styles.quickToggleChipSelected,
                  isDemoMode && styles.quickToggleChipDisabled,
                ]}
                onPress={() => setSearchAutoCorrectEnabled((prev) => !prev)}
                disabled={isDemoMode}
                accessibilityRole="button"
                accessibilityLabel="Toggle keyboard auto-correct"
              >
                <Text
                  style={[
                    styles.quickToggleChipText,
                    searchAutoCorrectEnabled && styles.quickToggleChipTextSelected,
                  ]}
                >
                  Auto-correct: {searchAutoCorrectEnabled ? "On" : "Off"}
                </Text>
              </Pressable>
            </View>
            {!isDemoMode && showRecentNormalizedSongs && recentNormalizedSongs.length ? (
              <View style={styles.recentNormalizedSection}>
                <Text style={styles.recentNormalizedLabel}>Recently Searched</Text>
                <View style={styles.recentNormalizedChips}>
                  {recentNormalizedSongs.map((song) => {
                    const chipKey = `${song.query}|${song.artist}|${song.title}`;
                    const chipLabel = `${song.title} - ${song.artist}`;
                    const isSelected = query.trim().toLowerCase() === song.query.trim().toLowerCase();
                    return (
                      <Pressable
                        key={chipKey}
                        style={[
                          styles.recentNormalizedChip,
                          isSelected && styles.recentNormalizedChipSelected,
                        ]}
                        onPress={() => {
                          setQuery(song.query);
                          Keyboard.dismiss();
                          queryInputRef.current?.blur();
                        }}
                      >
                        <Text
                          style={[
                            styles.recentNormalizedChipText,
                            isSelected && styles.recentNormalizedChipTextSelected,
                          ]}
                        >
                          {chipLabel}
                        </Text>
                      </Pressable>
                    );
                  })}
                </View>
              </View>
            ) : null}
            {presetQueriesUnlocked ? (
              <View style={styles.quickQuerySection}>
                <View style={styles.quickQueryHeader}>
                  <Text style={styles.quickQueryLabel}>Preset queries</Text>
                  <Pressable
                    style={styles.quickQueryToggle}
                    onPress={() => setPresetQueriesOpen((prev) => !prev)}
                    disabled={isDemoMode}
                  >
                    <Text style={styles.quickQueryToggleText}>
                      {presetQueriesOpen ? "Hide" : "Show"}
                    </Text>
                  </Pressable>
                </View>
                {presetQueriesOpen ? (
                  <ScrollView
                    style={styles.quickQueryList}
                    contentContainerStyle={styles.quickQueryListContent}
                    nestedScrollEnabled
                    keyboardShouldPersistTaps="handled"
                    showsVerticalScrollIndicator={false}
                    showsHorizontalScrollIndicator={false}
                  >
                    {QUERY_PREFILL_BUTTONS.map((presetQuery) => {
                      const isSelected = query.trim().toLowerCase() === presetQuery.toLowerCase();
                      return (
                        <Pressable
                          key={presetQuery}
                          style={[
                            styles.quickQueryButton,
                            isSelected && styles.quickQueryButtonSelected,
                            isDemoMode && styles.quickQueryButtonDisabled,
                          ]}
                          onPress={() => {
                            setQuery(presetQuery);
                            Keyboard.dismiss();
                            queryInputRef.current?.blur();
                          }}
                          disabled={isDemoMode}
                        >
                          <Text
                            style={[
                              styles.quickQueryButtonText,
                              isSelected && styles.quickQueryButtonTextSelected,
                            ]}
                          >
                            {presetQuery}
                          </Text>
                        </Pressable>
                      );
                    })}
                  </ScrollView>
                ) : null}
              </View>
            ) : null}
            {isDemoMode ? (
              <View style={styles.demoCreateButtons}>
                <Pressable
                  style={[styles.primaryButton, loading && styles.primaryButtonDisabled]}
                  onPress={() => {
                    requestStartJob({ renderOnly: false, force: true, reset: true });
                  }}
                  disabled={loading}
                >
                  <Text style={styles.primaryButtonText}>
                    {loading ? "Starting..." : "Create Karaoke (no cache)"}
                  </Text>
                </Pressable>
                <Pressable
                  style={[styles.secondaryButton, styles.demoSecondaryAction, loading && styles.primaryButtonDisabled]}
                  onPress={() => {
                    requestStartJob({ renderOnly: false, force: false });
                  }}
                  disabled={loading}
                >
                  <Text style={styles.secondaryButtonText}>Create Karaoke (cached)</Text>
                </Pressable>
              </View>
            ) : (
              <Pressable
                style={[styles.primaryButton, loading && styles.primaryButtonDisabled]}
                onPress={() => {
                  requestStartJob({ renderOnly: false });
                }}
                disabled={loading}
              >
                <Text style={styles.primaryButtonText}>
                  {loading ? "Starting..." : "Create Karaoke"}
                </Text>
              </Pressable>
            )}
            <Pressable
              style={[styles.secondaryButton, (loading || isDemoMode) && styles.primaryButtonDisabled]}
              onPress={pickSongForMe}
              disabled={loading || isDemoMode}
            >
              <Text style={styles.secondaryButtonText}>Pick a Song For Me</Text>
            </Pressable>
            {showAdvancedButton ? (
              <>
                <Pressable style={styles.secondaryButton} onPress={toggleAdvancedPanel}>
                  <Text style={styles.secondaryButtonText}>{advancedOpen ? "Hide Advanced" : "Advanced"}</Text>
                </Pressable>
                {advancedOpen ? (
                  <View style={styles.tuningCard}>
                    <Text style={styles.tuningTitle}>
                      Offset lyrics by: {globalOffsetSec >= 0 ? "+" : ""}{globalOffsetSec.toFixed(2)}s
                    </Text>
                    <View style={styles.tuningButtonsRow}>
                      <Pressable style={styles.tuningButton} onPress={() => adjustOffset(-0.5)}>
                        <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>-0.50s</Text>
                      </Pressable>
                      <Pressable style={styles.tuningButton} onPress={() => adjustOffset(-0.25)}>
                        <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>-0.25s</Text>
                      </Pressable>
                      <Pressable style={styles.tuningButton} onPress={() => setGlobalOffsetSec(0)}>
                        <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>Reset</Text>
                      </Pressable>
                      <Pressable style={styles.tuningButton} onPress={() => adjustOffset(0.25)}>
                        <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>+0.25s</Text>
                      </Pressable>
                      <Pressable style={styles.tuningButton} onPress={() => adjustOffset(0.5)}>
                        <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>+0.50s</Text>
                      </Pressable>
                    </View>
                    <View style={styles.advancedSeparator} />
                    {STEM_KEYS.map((stem) => (
                      <View key={stem} style={styles.mixGridRow}>
                        <View style={[styles.mixGridCell, styles.mixGridLabelCell]}>
                          <Text style={styles.mixGridLabel} numberOfLines={1} adjustsFontSizeToFit minimumFontScale={0.85}>
                            {STEM_LABELS[stem]}
                          </Text>
                        </View>
                        <Pressable
                          style={[styles.mixButton, styles.mixGridCell, styles.mixGridButtonCell, isDemoMode && styles.mixButtonDisabled]}
                          onPress={() => adjustMixLevel(stem, -10)}
                          disabled={isDemoMode}
                        >
                          <Text style={styles.tuningButtonText} numberOfLines={1}>-10%</Text>
                        </Pressable>
                        <Pressable
                          style={[
                            styles.mixGridCell,
                            styles.mixGridValueWrap,
                            styles.mixGridValueTapTarget,
                            isDemoMode && styles.mixGridValueTapTargetDisabled,
                          ]}
                          onPress={() => openMixLevelEditor(stem)}
                          disabled={isDemoMode}
                          accessibilityRole="button"
                          accessibilityLabel={`Set ${STEM_LABELS[stem]} volume`}
                        >
                          <View style={styles.mixGridValueContent}>
                            <Text style={styles.mixGridValue} numberOfLines={1}>{mixLevels[stem]}%</Text>
                            <View style={styles.mixGridEditBadge}>
                              <Text style={styles.mixGridEditBadgeText}>EDIT</Text>
                            </View>
                          </View>
                        </Pressable>
                        <Pressable
                          style={[styles.mixButton, styles.mixGridCell, styles.mixGridButtonCell, isDemoMode && styles.mixButtonDisabled]}
                          onPress={() => adjustMixLevel(stem, 10)}
                          disabled={isDemoMode}
                        >
                          <Text style={styles.tuningButtonText} numberOfLines={1}>+10%</Text>
                        </Pressable>
                        <View style={[styles.mixGridCell, styles.mixGridSpacerCell]} />
                      </View>
                    ))}
                  </View>
                ) : null}
              </>
            ) : null}
            {showInlineError && <Text style={styles.error}>{formatUiError(error)}</Text>}
            {showSmartRecoveryActions && strictLyricsFailure && (
              <View style={styles.smartRecoveryCard}>
                <Text style={styles.smartRecoveryTitle}>
                  {REQUEST_RECEIVED_STATUS_MESSAGE}
                </Text>
                <Text style={styles.smartRecoveryBody}>
                  {PROCESSING_REQUEST_STATUS_MESSAGE}
                </Text>
              </View>
            )}
            {showInlineStatus && <Text style={styles.smallStatus}>{formatUiStatus(error)}</Text>}
            {showRetryVideoLoadButton ? (
              <Pressable
                style={[styles.secondaryButton, retryVideoLoadDisabled && styles.primaryButtonDisabled]}
                onPress={() => {
                  retryVideoLoad().catch(() => undefined);
                }}
                disabled={retryVideoLoadDisabled}
              >
                <Text style={styles.secondaryButtonText}>
                  {retryVideoLoadDisabled ? PROCESSING_REQUEST_STATUS_MESSAGE : "Retry Request"}
                </Text>
              </Pressable>
            ) : null}
            {!error && jobCancelled && (
              <Text style={styles.error}>{GENERIC_ERROR_MESSAGE}</Text>
            )}
            {showSmartRecoveryActions && strictLyricsFailure?.suggestedQuery ? (
              <Pressable style={styles.secondaryButton} onPress={applySuggestedQuery}>
                <Text style={styles.secondaryButtonText}>Use Suggested Query</Text>
              </Pressable>
            ) : null}
            {showSmartRecoveryActions ? (
              <>
                <Pressable style={styles.secondaryButton} onPress={() => focusQueryInput()}>
                  <Text style={styles.secondaryButtonText}>Edit Query</Text>
                </Pressable>
                <Pressable
                  style={[styles.secondaryButton, sourcePickerLoading && styles.primaryButtonDisabled]}
                  onPress={openSourcePicker}
                  disabled={sourcePickerLoading}
                >
                  <Text style={styles.secondaryButtonText}>
                    {sourcePickerLoading ? PROCESSING_REQUEST_STATUS_MESSAGE : "Pick Result"}
                  </Text>
                </Pressable>
              </>
            ) : null}
            {showGenericRetryButton && (
              <Pressable style={styles.secondaryButton} onPress={retryJob}>
                <Text style={styles.secondaryButtonText}>Try Again</Text>
              </Pressable>
            )}
            {outputUrl ? (
              <Pressable style={styles.secondaryButton} onPress={() => switchTab("video")}>
                <Text style={styles.secondaryButtonText}>View Video</Text>
              </Pressable>
            ) : null}
            {job?.id ? (
              <Pressable style={styles.secondaryButton} onPress={() => setTimingModalVisible(true)}>
                <Text style={styles.secondaryButtonText}>View Timing Details</Text>
              </Pressable>
            ) : null}
          </ScrollView>
          </TouchableWithoutFeedback>
        ) : (
          <View style={[styles.tabContent, styles.videoTabContent]}>
            <ScrollView
              style={styles.videoScroll}
              contentContainerStyle={[
                styles.videoScrollContent,
                isLandscape && styles.videoScrollContentLandscape,
                styles.fixedWidthContentContainer,
              ]}
              keyboardShouldPersistTaps="handled"
              directionalLockEnabled
              alwaysBounceHorizontal={false}
              showsVerticalScrollIndicator={false}
              showsHorizontalScrollIndicator={false}
            >
              <View style={[styles.videoSimpleLayout, isLandscape && styles.videoSimpleLayoutLandscape]}>
                {playbackSource.kind === "youtube" ? (
                  <StockYoutubeEmbed
                    fullscreen={isLandscape}
                    youtubeEmbedUrl={youtubeEmbedUrl}
                    onReady={handleYoutubeEmbedReady}
                    onFatalError={handleYoutubeEmbedFatalError}
                  />
                ) : outputUrl ? (
                  <OutputVideo
                    key={`inline:${videoSurfaceKey}`}
                    player={videoPlayer}
                    uri={outputUrl}
                    fullscreen={isLandscape}
                    nativeControls
                    onMissingUrl={() => {
                      refreshVideoJob().catch(() => undefined);
                    }}
                  />
                ) : (
                  <View style={styles.videoPlaceholder}>
                    <Text style={styles.placeholderText}>
                      {jobFailed ? "Video generation failed." : "Waiting for the video..."}
                    </Text>
                    {job?.id ? <Text style={styles.pipelineMeta}>Job: {job.id}</Text> : null}
                    {job?.status ? <Text style={styles.pipelineMeta}>Status: {job.status}</Text> : null}
                    {job?.stage ? <Text style={styles.pipelineMeta}>Stage: {formatStage(job.stage)}</Text> : null}
                    {job?.last_message ? <Text style={styles.pipelineMessage}>{sanitizeApiErrorMessage(job.last_message)}</Text> : null}
                    {job?.error ? <Text style={styles.pipelineError}>{sanitizeApiErrorMessage(job.error)}</Text> : null}
                  </View>
                )}

                <View style={styles.videoActionsStack}>
                  <Pressable
                    style={[styles.primaryButton, styles.videoActionButtonCompact, saveShareDisabled && styles.primaryButtonDisabled]}
                    onPress={handleSave}
                    disabled={saveShareDisabled}
                  >
                    <Text style={styles.primaryButtonText}>Save</Text>
                  </Pressable>
                  <Pressable
                    style={[styles.secondaryButton, styles.videoActionButtonCompact, saveShareDisabled && styles.primaryButtonDisabled]}
                    onPress={handleShare}
                    disabled={saveShareDisabled}
                  >
                    <Text style={styles.secondaryButtonText}>Share</Text>
                  </Pressable>
                  {showTimers ? (
                    <Pressable
                      style={[styles.secondaryButton, styles.videoActionButtonCompact]}
                      onPress={() => setTimingModalVisible(true)}
                    >
                      <Text style={styles.secondaryButtonText}>Timing Breakdown</Text>
                    </Pressable>
                  ) : null}
                  {isDemoMode ? (
                    <Text style={styles.smallStatus}>{formatUiStatus("Demo mode: Save and Share are disabled.")}</Text>
                  ) : (
                    <>
                      {saveStatus ? (
                        <Text style={saveStatus.startsWith("❌") ? styles.error : styles.smallStatus}>
                          {saveStatus}
                        </Text>
                      ) : null}
                      {showPhotoSettingsAction ? (
                        <Pressable style={styles.inlineSettingsButton} onPress={openAppSettings}>
                          <Text style={styles.inlineSettingsButtonText}>Open Settings</Text>
                        </Pressable>
                      ) : null}
                    </>
                  )}
                  {showAdvancedButton ? (
                    <>
                      <Pressable style={[styles.secondaryButton, styles.videoActionButtonCompact]} onPress={toggleAdvancedPanel}>
                        <Text style={styles.secondaryButtonText}>{advancedOpen ? "Hide Advanced" : "Advanced"}</Text>
                      </Pressable>
                      {advancedOpen ? (
                        <View style={styles.videoOffsetCard}>
                          <Text style={styles.tuningTitle}>
                            Offset lyrics by: {globalOffsetSec >= 0 ? "+" : ""}{globalOffsetSec.toFixed(2)}s
                          </Text>
                          <Text style={styles.videoOffsetHint}>
                            These timing and stem changes only affect the next render. Rebuild the video to apply them.
                          </Text>
                          <View style={styles.tuningButtonsRow}>
                            {OFFSET_STEP_BUTTONS.map((option) => (
                              <Pressable
                                key={`video-${option.label}`}
                                style={[styles.tuningButton, isDemoMode && styles.mixButtonDisabled]}
                                onPress={() => adjustOffset(option.delta)}
                                disabled={isDemoMode}
                              >
                                <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>
                                  {option.label}
                                </Text>
                              </Pressable>
                            ))}
                            <Pressable
                              style={[styles.tuningButton, isDemoMode && styles.mixButtonDisabled]}
                              onPress={() => setGlobalOffsetSec(0)}
                              disabled={isDemoMode}
                            >
                              <Text style={styles.offsetButtonText} numberOfLines={1} adjustsFontSizeToFit>
                                Reset
                              </Text>
                            </Pressable>
                          </View>
                          <View style={styles.advancedSeparator} />
                          {STEM_KEYS.map((stem) => (
                            <View key={`video-${stem}`} style={styles.mixGridRow}>
                              <View style={[styles.mixGridCell, styles.mixGridLabelCell]}>
                                <Text style={styles.mixGridLabel} numberOfLines={1} adjustsFontSizeToFit minimumFontScale={0.85}>
                                  {STEM_LABELS[stem]}
                                </Text>
                              </View>
                              <Pressable
                                style={[styles.mixButton, styles.mixGridCell, styles.mixGridButtonCell, isDemoMode && styles.mixButtonDisabled]}
                                onPress={() => adjustMixLevel(stem, -10)}
                                disabled={isDemoMode}
                              >
                                <Text style={styles.tuningButtonText} numberOfLines={1}>-10%</Text>
                              </Pressable>
                              <Pressable
                                style={[
                                  styles.mixGridCell,
                                  styles.mixGridValueWrap,
                                  styles.mixGridValueTapTarget,
                                  isDemoMode && styles.mixGridValueTapTargetDisabled,
                                ]}
                                onPress={() => openMixLevelEditor(stem)}
                                disabled={isDemoMode}
                                accessibilityRole="button"
                                accessibilityLabel={`Set ${STEM_LABELS[stem]} volume`}
                              >
                                <View style={styles.mixGridValueContent}>
                                  <Text style={styles.mixGridValue} numberOfLines={1}>{mixLevels[stem]}%</Text>
                                  <View style={styles.mixGridEditBadge}>
                                    <Text style={styles.mixGridEditBadgeText}>EDIT</Text>
                                  </View>
                                </View>
                              </Pressable>
                              <Pressable
                                style={[styles.mixButton, styles.mixGridCell, styles.mixGridButtonCell, isDemoMode && styles.mixButtonDisabled]}
                                onPress={() => adjustMixLevel(stem, 10)}
                                disabled={isDemoMode}
                              >
                                <Text style={styles.tuningButtonText} numberOfLines={1}>+10%</Text>
                              </Pressable>
                              <View style={[styles.mixGridCell, styles.mixGridSpacerCell]} />
                            </View>
                          ))}
                          <Pressable
                            style={[styles.primaryButton, styles.videoApplyTuningButton, (loading || isDemoMode) && styles.primaryButtonDisabled]}
                            onPress={applyCurrentVideoTuning}
                            disabled={loading || isDemoMode}
                          >
                            <Text style={styles.primaryButtonText}>
                              {loading ? "Rebuilding..." : "Rebuild Video With Current Timing + Mix"}
                            </Text>
                          </Pressable>
                        </View>
                      ) : null}
                    </>
                  ) : null}
                  <Pressable style={[styles.secondaryButton, styles.videoActionButtonCompact]} onPress={resetPipeline}>
                    <Text style={styles.secondaryButtonText}>Generate Another Video</Text>
                  </Pressable>
                  {error && !outputUrl ? (
                    <Text style={isProgressStatusMessage(error) ? styles.smallStatus : styles.error}>
                      {isProgressStatusMessage(error) ? formatUiStatus(error) : formatUiError(error)}
                    </Text>
                  ) : null}
                </View>
              </View>
            </ScrollView>
          </View>
        )}
      </KeyboardAvoidingView>

      <View style={styles.tabBarContainer}>
        <View style={styles.tabBar}>
          <Pressable
            style={[styles.tabButton, activeTab === "search" && styles.tabButtonActive]}
            onPress={() => switchTab("search")}
          >
            <Text style={styles.tabText}>Search</Text>
          </Pressable>
          <Pressable
            style={[styles.tabButton, activeTab === "video" && styles.tabButtonActive, !videoTabEnabled && styles.tabButtonDisabled]}
            onPress={() => switchTab("video")}
          >
            <Text style={styles.tabText}>Video</Text>
          </Pressable>
          <Pressable
            style={[styles.tabButton, activeTab === "settings" && styles.tabButtonActive]}
            onPress={() => switchTab("settings")}
          >
            <Text style={styles.tabText}>Settings</Text>
          </Pressable>
        </View>
      </View>

      <ProcessingModal
        visible={shouldShowProcessingModal}
        job={job}
        queryNormalizing={isQueryNormalizing}
        queryPreflightElapsedMs={queryPreflightElapsedMs}
        startAt={stopwatchStartAt}
        endAt={stopwatchEndAt}
        showTimers={showTimers}
        onDismiss={dismissProcessing}
        onCancel={cancelProcessing}
      />

      <TimingBreakdownModal
        visible={timingModalVisible}
        job={job}
        query={query}
        normalizedSong={pendingNormalizedSong}
        queryNormalizing={isQueryNormalizing}
        queryPreflightElapsedMs={queryPreflightElapsedMs}
        loading={loading}
        startAt={stopwatchStartAt}
        endAt={stopwatchEndAt}
        showTimers={showTimers}
        onClose={() => setTimingModalVisible(false)}
        onCancel={cancelProcessing}
      />

      <Modal visible={Boolean(mixLevelEditorStem)} transparent animationType="fade" onRequestClose={closeMixLevelEditor}>
        <View style={styles.modalBackdrop}>
          <View style={[styles.modalCard, styles.mixLevelEditorCard]}>
            <Text style={styles.modalTitle}>Set {mixLevelEditorStemLabel}</Text>
            <Text style={styles.modalMessage}>Enter a whole number from 0 to 150.</Text>
            <TextInput
              ref={mixLevelInputRef}
              style={styles.mixLevelEditorInput}
              value={mixLevelEditorValue}
              onChangeText={updateMixLevelEditorValue}
              keyboardType="number-pad"
              returnKeyType="done"
              maxLength={3}
              placeholder="0-150"
              placeholderTextColor="#667089"
              selectTextOnFocus
              onSubmitEditing={saveMixLevelEditorValue}
            />
            <View style={styles.mixLevelEditorActions}>
              <Pressable style={[styles.modalBackButton, styles.mixLevelEditorAction]} onPress={closeMixLevelEditor}>
                <Text style={styles.secondaryButtonText}>Cancel</Text>
              </Pressable>
              <Pressable
                style={[styles.primaryButton, styles.mixLevelEditorAction, mixLevelEditorSaveDisabled && styles.primaryButtonDisabled]}
                onPress={saveMixLevelEditorValue}
                disabled={mixLevelEditorSaveDisabled}
              >
                <Text style={styles.primaryButtonText}>Save</Text>
              </Pressable>
            </View>
          </View>
        </View>
      </Modal>

      <Modal visible={sourcePickerVisible} transparent animationType="fade" onRequestClose={closeSourcePicker}>
        <View style={styles.modalBackdrop}>
          <View style={[styles.modalCard, styles.searchResultsCard]}>
            <Text style={styles.modalTitle}>Pick Result</Text>
            <Text style={styles.modalMessage}>
              Choose a result to refine your request
            </Text>
            {sourcePickerError ? <Text style={styles.error}>{formatUiError(sourcePickerError)}</Text> : null}
            <ScrollView
              style={styles.searchResultsList}
              showsVerticalScrollIndicator={false}
              showsHorizontalScrollIndicator={false}
            >
              {sourceSearchResults.map((row) => (
                <Pressable
                  key={row.video_id}
                  style={styles.searchResultRow}
                  onPress={() => chooseSourceResult(row)}
                >
                  <Text style={styles.searchResultTitle}>{row.title}</Text>
                  <Text style={styles.searchResultMeta}>
                    {[row.uploader || ""].filter(Boolean).join("  •  ") ||
                      row.video_id}
                  </Text>
                </Pressable>
              ))}
              {!sourcePickerLoading && !sourceSearchResults.length && !sourcePickerError ? (
                <Text style={styles.smallStatus}>{formatUiStatus("No source results loaded yet")}</Text>
              ) : null}
              {sourcePickerLoading ? <Text style={styles.smallStatus}>{formatUiStatus("Loading source results...")}</Text> : null}
            </ScrollView>
            <Pressable style={styles.modalBackButton} onPress={closeSourcePicker}>
              <Text style={styles.secondaryButtonText}>Close</Text>
            </Pressable>
          </View>
        </View>
      </Modal>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: {
    flex: 1,
    backgroundColor: "#0f1115",
    width: "100%",
    overflow: "hidden",
    paddingTop: Platform.OS === "android" ? (StatusBar.currentHeight ?? 0) : 0,
  },
  header: {
    paddingHorizontal: 20,
    paddingTop: 8,
    paddingBottom: 12,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  headerLeft: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  headerAppIcon: {
    width: 28,
    height: 28,
    borderRadius: 7,
  },
  headerRight: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  headerTitle: { fontSize: 24, fontWeight: "800", color: "#f2f4f8", letterSpacing: 0.2 },
  headerPrefix: { fontSize: 16, color: "#8f98a8", fontWeight: "700" },
  headerTimer: { fontSize: 18, color: "#8f98a8", fontWeight: "700" },
  headerPlaybackButton: {
    width: 50,
    height: 44,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#344a70",
    backgroundColor: "#1d2534",
    alignItems: "center",
    justifyContent: "center",
    padding: 0,
  },
  headerPlaybackGlyph: {
    width: 18,
    height: 18,
    alignItems: "center",
    justifyContent: "center",
  },
  headerPauseGlyph: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
  },
  headerPauseBar: {
    width: 4,
    height: 14,
    borderRadius: 2,
    backgroundColor: "#f2f4f8",
  },
  headerPlayGlyph: {
    width: 0,
    height: 0,
    marginLeft: 1,
    borderTopWidth: 8,
    borderBottomWidth: 8,
    borderLeftWidth: 13,
    borderTopColor: "transparent",
    borderBottomColor: "transparent",
    borderLeftColor: "#f2f4f8",
  },
  offlineBanner: {
    marginHorizontal: 20,
    marginBottom: 10,
    paddingVertical: 8,
    paddingHorizontal: 12,
    backgroundColor: "#2a1d1d",
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#5a2a2a",
  },
  offlineText: { color: "#ffb4b4", fontSize: 12, textAlign: "center" },
  backgroundBanner: {
    marginHorizontal: 20,
    marginBottom: 10,
    paddingVertical: 8,
    paddingHorizontal: 12,
    backgroundColor: "#1d2a3a",
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#2a4a6a",
  },
  backgroundText: { color: "#f2f4f8", textAlign: "center", fontSize: 13 },
  content: { flex: 1, paddingHorizontal: 20, paddingBottom: 8, width: "100%", minWidth: 0, overflow: "hidden" },
  contentVideoTab: {
    paddingHorizontal: 0,
    paddingBottom: 0,
    overflow: "visible",
  },
  contentSettingsTab: {
    paddingHorizontal: 0,
  },
  tabContent: { flex: 1, gap: 8 },
  feedbackTab: {
    flex: 1,
  },
  feedbackScroll: {
    flex: 1,
    width: "100%",
  },
  feedbackScrollContent: {
    flexGrow: 1,
    paddingTop: 8,
    paddingBottom: 6,
  },
  settingsPanelContent: {
    flexGrow: 1,
    paddingHorizontal: 20,
    gap: 12,
  },
  settingsVersionFooter: {
    marginTop: "auto",
    alignSelf: "stretch",
    paddingTop: 8,
    paddingHorizontal: 20,
    paddingBottom: 2,
  },
  settingsVersionText: {
    color: "#7f8a99",
    fontSize: 11,
    fontWeight: "700",
    textAlign: "right",
  },
  fixedWidthContentContainer: {
    width: "100%",
    alignSelf: "stretch",
    minWidth: 0,
  },
  feedbackInput: {
    minHeight: 180,
    paddingTop: 12,
    paddingBottom: 12,
    lineHeight: 22,
  },
  settingsCard: {
    marginTop: 6,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#2a2f3a",
    backgroundColor: "#141821",
    paddingHorizontal: 12,
    paddingVertical: 12,
    gap: 8,
  },
  settingsSectionTitle: {
    color: "#dbe2ee",
    fontSize: 15,
    fontWeight: "800",
  },
  settingsSectionBody: {
    color: "#9ba5b6",
    fontSize: 13,
    lineHeight: 18,
  },
  settingsContactLink: {
    alignSelf: "flex-start",
    marginTop: -6,
    marginBottom: 2,
  },
  settingsContactLinkText: {
    color: "#8fb6ff",
    fontSize: 14,
    fontWeight: "700",
  },
  advancedModeChips: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 2,
  },
  advancedModeChip: {
    borderWidth: 1,
    borderColor: "#2f3a52",
    borderRadius: 999,
    backgroundColor: "#1b1f27",
    paddingVertical: 7,
    paddingHorizontal: 12,
  },
  advancedModeChipSelected: {
    borderColor: "#4b607f",
    backgroundColor: "#212838",
  },
  advancedModeChipText: {
    color: "#d3dae6",
    fontSize: 13,
    fontWeight: "600",
  },
  advancedModeChipTextSelected: {
    color: "#f2f4f8",
  },
  feedbackVersionDock: {
    position: "absolute",
    right: 2,
    bottom: 8,
  },
  feedbackVersionText: {
    color: "#6a7382",
    fontSize: 11,
    fontWeight: "600",
    textAlign: "right",
  },
  searchScroll: { flex: 1, width: "100%" },
  searchScrollContent: { flexGrow: 1, paddingBottom: 132 },
  searchTabContent: {
    paddingTop: 8,
    paddingBottom: 36,
    paddingHorizontal: 2,
    gap: 12,
  },
  demoModeBanner: {
    backgroundColor: "#202636",
    borderWidth: 1,
    borderColor: "#2f3a52",
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  demoModeBannerText: { color: "#c9d1d9", fontSize: 13, fontWeight: "600", textAlign: "center" },
  demoCreateButtons: { gap: 10 },
  demoSecondaryAction: { marginTop: 0 },
  videoActionsStack: { gap: 15, paddingTop: 8, paddingBottom: 28 },
  videoActionButtonCompact: {
    marginTop: 0,
  },
  videoProcessingCard: {
    marginTop: 0,
    alignSelf: "stretch",
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "#2f3a52",
    backgroundColor: "#171e2b",
    paddingHorizontal: 12,
    paddingVertical: 10,
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  videoProcessingTextWrap: {
    flex: 1,
    minWidth: 0,
    gap: 2,
  },
  videoProcessingTitle: {
    color: "#e4edf9",
    fontSize: 13,
    fontWeight: "700",
  },
  videoProcessingMeta: {
    color: "#9bb0cc",
    fontSize: 12,
    fontWeight: "600",
  },
  label: { color: "#c8cfda", fontSize: 34 / 2, marginTop: 8, fontWeight: "700" },
  searchHelper: { color: "#97a1b3", fontSize: 14, lineHeight: 20, marginTop: 2, marginBottom: 2 },
  input: {
    backgroundColor: "#1b1f27",
    color: "#f2f4f8",
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 12,
    fontSize: 18,
    borderWidth: 1,
    borderColor: "#2a2f3a",
  },
  queryInputWrap: {
    position: "relative",
    justifyContent: "center",
  },
  queryInput: {
    paddingRight: 50,
  },
  queryClearButton: {
    position: "absolute",
    right: 14,
    width: 30,
    height: 30,
    borderRadius: 15,
    borderWidth: 1,
    borderColor: "#44506a",
    backgroundColor: "#293249",
    alignItems: "center",
    justifyContent: "center",
  },
  queryClearButtonText: {
    color: "#f2f4f8",
    fontSize: 16,
    fontWeight: "800",
    lineHeight: 18,
  },
  searchQuickToggleRow: {
    marginTop: -2,
    marginBottom: 2,
    flexDirection: "row",
    alignItems: "center",
  },
  quickToggleChip: {
    borderWidth: 1,
    borderColor: "#2f3a52",
    borderRadius: 999,
    backgroundColor: "#1b1f27",
    paddingVertical: 6,
    paddingHorizontal: 12,
  },
  quickToggleChipSelected: {
    borderColor: "#4b607f",
    backgroundColor: "#212838",
  },
  quickToggleChipDisabled: {
    opacity: 0.6,
  },
  quickToggleChipText: {
    color: "#d3dae6",
    fontSize: 12,
    fontWeight: "700",
    letterSpacing: 0.25,
  },
  quickToggleChipTextSelected: {
    color: "#f2f4f8",
  },
  searchInputProminent: {
    fontSize: 20,
    fontWeight: "700",
    paddingVertical: 15,
    borderWidth: 2,
    borderColor: "#3a4253",
    marginTop: 8,
    marginBottom: 8,
  },
  recentNormalizedSection: {
    gap: 8,
    marginBottom: 2,
  },
  recentNormalizedLabel: {
    color: "#9aabc7",
    fontSize: 12,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.45,
  },
  recentNormalizedChips: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  recentNormalizedChip: {
    borderWidth: 1,
    borderColor: "#2f3a52",
    borderRadius: 999,
    backgroundColor: "#1b1f27",
    paddingVertical: 7,
    paddingHorizontal: 12,
  },
  recentNormalizedChipSelected: {
    borderColor: "#4b607f",
    backgroundColor: "#212838",
  },
  recentNormalizedChipText: {
    color: "#d3dae6",
    fontSize: 13,
    fontWeight: "600",
  },
  recentNormalizedChipTextSelected: {
    color: "#f2f4f8",
  },
  inputDisabled: {
    opacity: 0.9,
    backgroundColor: "#171b22",
  },
  quickQuerySection: {
    gap: 8,
    marginBottom: 4,
  },
  quickQueryHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    width: "100%",
    minWidth: 0,
  },
  quickQueryLabel: {
    color: "#a9b3c7",
    fontSize: 13,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.5,
    flexShrink: 1,
  },
  quickQueryToggle: {
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 8,
    backgroundColor: "#1b1f27",
    paddingVertical: 5,
    paddingHorizontal: 10,
  },
  quickQueryToggleText: {
    color: "#c9d1d9",
    fontSize: 12,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.45,
  },
  quickQueryList: {
    maxHeight: 170,
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 10,
    backgroundColor: "#141821",
    width: "100%",
    minWidth: 0,
  },
  quickQueryListContent: {
    paddingVertical: 8,
    paddingHorizontal: 8,
    gap: 8,
  },
  quickQueryButton: {
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 9,
    backgroundColor: "#1b1f27",
    paddingVertical: 10,
    paddingHorizontal: 12,
  },
  quickQueryButtonSelected: {
    borderColor: "#4b607f",
    backgroundColor: "#212838",
  },
  quickQueryButtonDisabled: {
    opacity: 0.45,
  },
  quickQueryButtonText: {
    color: "#d3dae6",
    fontSize: 15,
    fontWeight: "600",
  },
  quickQueryButtonTextSelected: {
    color: "#f2f4f8",
  },
  cookiesInput: {
    minHeight: 96,
    fontSize: 12,
    paddingTop: 10,
  },
  cookieHint: { color: "#f6c56a", marginTop: 2, fontSize: 12 },
  primaryButton: {
    backgroundColor: "#f2f4f8",
    paddingVertical: 14,
    borderRadius: 12,
    alignItems: "center",
    alignSelf: "stretch",
    minWidth: 0,
  },
  primaryButtonDisabled: { opacity: 0.4 },
  primaryButtonText: { color: "#0f1115", fontWeight: "800", fontSize: 19 },
  secondaryButton: {
    marginTop: 10,
    backgroundColor: "#1b1f27",
    paddingVertical: 12,
    borderRadius: 12,
    alignItems: "center",
    alignSelf: "stretch",
    borderWidth: 1,
    borderColor: "#2a2f3a",
    minWidth: 0,
  },
  uploadButton: {
    backgroundColor: "#1d2a1d",
    borderColor: "#2a5a2a",
  },
  secondaryButtonText: { color: "#c9d1d9", fontWeight: "700", fontSize: 18 },
  tuningCard: {
    marginTop: 10,
    marginHorizontal: 0,
    paddingHorizontal: 0,
    paddingVertical: 2,
    gap: 12,
    alignSelf: "stretch",
    width: "100%",
    minWidth: 0,
  },
  tuningTitle: { color: "#d5dbea", fontSize: 18, fontWeight: "800", paddingHorizontal: 0 },
  tuningButtonsRow: {
    flexDirection: "row",
    alignSelf: "stretch",
    width: "100%",
    gap: 8,
    minWidth: 0,
  },
  advancedSeparator: {
    height: 1,
    backgroundColor: "rgba(201, 209, 217, 0.22)",
    marginTop: 4,
    marginBottom: 6,
  },
  tuningButton: {
    backgroundColor: "#1b1f27",
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 8,
    minHeight: 44,
    minWidth: 0,
    flexGrow: 1,
    flexShrink: 1,
    flexBasis: 0,
    paddingVertical: 10,
    paddingHorizontal: 8,
    alignItems: "center",
    justifyContent: "center",
  },
  tuningButtonText: { color: "#c9d1d9", fontSize: 16, fontWeight: "700" },
  offsetButtonText: { color: "#c9d1d9", fontSize: 14, fontWeight: "700", includeFontPadding: false },
  mixAdjustButton: {
    paddingHorizontal: 6,
  },
  mixAdjustButtonDecrease: {
    backgroundColor: "#24181d",
    borderColor: "#6c3641",
  },
  mixAdjustButtonIncrease: {
    backgroundColor: "#17261c",
    borderColor: "#2f6a4a",
  },
  mixAdjustButtonContent: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 6,
  },
  mixAdjustButtonText: {
    fontSize: 15,
    fontWeight: "800",
  },
  mixAdjustButtonTextDecrease: {
    color: "#ffd5dc",
  },
  mixAdjustButtonTextIncrease: {
    color: "#c3f8d6",
  },
  mixAdjustArrowLeft: {
    width: 0,
    height: 0,
    borderTopWidth: 6,
    borderBottomWidth: 6,
    borderRightWidth: 9,
    borderTopColor: "transparent",
    borderBottomColor: "transparent",
    borderRightColor: "#ffd5dc",
  },
  mixAdjustArrowRight: {
    width: 0,
    height: 0,
    borderTopWidth: 6,
    borderBottomWidth: 6,
    borderLeftWidth: 9,
    borderTopColor: "transparent",
    borderBottomColor: "transparent",
    borderLeftColor: "#c3f8d6",
  },
  mixGridRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    minHeight: 46,
    width: "100%",
    minWidth: 0,
  },
  mixGridCell: {
    flex: 1,
    minHeight: 44,
    minWidth: 0,
    justifyContent: "center",
  },
  mixGridLabelCell: {
    flex: 1.35,
    paddingRight: 4,
  },
  mixGridLabelCellSettings: {
    flex: 1.22,
  },
  mixGridButtonCell: {
    flex: 0.95,
  },
  mixGridButtonCellSettings: {
    flex: 0.92,
  },
  mixGridSpacerCell: {
    flex: 0.35,
  },
  mixGridSpacerCellSettings: {
    flex: 0.15,
  },
  mixGridLabel: {
    color: "#c9d1d9",
    fontSize: 14,
    fontWeight: "700",
    textAlign: "left",
    includeFontPadding: false,
  },
  mixGridValueWrap: {
    flex: 1.2,
    alignItems: "center",
  },
  mixGridValueWrapSettings: {
    flex: 1.35,
    minWidth: 94,
    maxWidth: 106,
  },
  mixGridValueTapTarget: {
    borderWidth: 1,
    borderColor: "#4f7ec9",
    borderRadius: 8,
    backgroundColor: "#1d2737",
    minHeight: 44,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 8,
    paddingHorizontal: 8,
    shadowColor: "#4f7ec9",
    shadowOffset: { width: 0, height: 0 },
    shadowOpacity: 0.2,
    shadowRadius: 4,
    elevation: 1,
  },
  mixGridValueTapTargetPressed: {
    backgroundColor: "#23344c",
    borderColor: "#7fb0ff",
    transform: [{ scale: 0.98 }],
  },
  mixGridValueTapTargetDisabled: {
    opacity: 0.45,
  },
  mixGridValueContent: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 4,
  },
  mixGridValue: {
    color: "#dbe2ee",
    fontSize: 15,
    fontWeight: "800",
    textAlign: "center",
  },
  mixGridEditBadge: {
    borderWidth: 1,
    borderColor: "#8cb6ff",
    backgroundColor: "#2d4f86",
    borderRadius: 999,
    paddingHorizontal: 4,
    paddingVertical: 1,
  },
  mixGridEditBadgeText: {
    color: "#e5f0ff",
    fontSize: 9,
    fontWeight: "800",
    letterSpacing: 0.25,
  },
  mixButton: {
    backgroundColor: "#1b1f27",
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 8,
    minHeight: 44,
    minWidth: 0,
    paddingVertical: 9,
    paddingHorizontal: 8,
    alignItems: "center",
    justifyContent: "center",
  },
  mixButtonDisabled: {
    opacity: 0.45,
  },
  videoOffsetCard: {
    marginTop: 2,
    marginHorizontal: 0,
    paddingHorizontal: 0,
    paddingVertical: 2,
    gap: 12,
    alignSelf: "stretch",
    width: "100%",
    minWidth: 0,
  },
  videoOffsetHint: {
    color: "#9caac4",
    fontSize: 12,
    fontWeight: "600",
    lineHeight: 18,
  },
  videoApplyTuningButton: {
    marginTop: 2,
  },
  error: { color: "#ff6b6b", marginTop: 6 },
  smartRecoveryCard: {
    marginTop: 8,
    backgroundColor: "#261717",
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#5a2a2a",
    paddingHorizontal: 12,
    paddingVertical: 10,
    gap: 6,
  },
  smartRecoveryTitle: {
    color: "#ffd6d6",
    fontSize: 14,
    fontWeight: "700",
  },
  smartRecoveryBody: {
    color: "#ffb4b4",
    fontSize: 12,
    lineHeight: 18,
  },
  smallStatus: { color: "#f2f4f8", marginTop: 8, textAlign: "center", fontWeight: "600" },
  inlineSettingsButton: {
    marginTop: 4,
    alignSelf: "center",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  inlineSettingsButtonText: {
    color: "#8fb6ff",
    fontSize: 13,
    fontWeight: "700",
    textAlign: "center",
  },
  backgroundPlaybackHolder: {
    width: 1,
    height: 1,
    overflow: "hidden",
    opacity: 0,
    position: "absolute",
    left: -1000,
    top: -1000,
  },
  backgroundPlaybackView: {
    width: 1,
    height: 1,
  },
  hiddenVideoPreload: { width: 1, height: 1, opacity: 0, position: "absolute", left: -1000, top: -1000 },
  youtubeEmbedWrap: {
    alignItems: "center",
    alignSelf: "stretch",
    marginHorizontal: 0,
    marginBottom: 0,
    minHeight: 220,
    width: "100%",
    overflow: "hidden",
    backgroundColor: "#0b0d12",
    borderRadius: 0,
  },
  youtubeEmbedWrapFullscreen: {
    marginBottom: 0,
    minHeight: 0,
    borderRadius: 0,
    flex: 1,
  },
  youtubeEmbed: {
    width: "100%",
    aspectRatio: 16 / 9,
    backgroundColor: "#0b0d12",
  },
  youtubeEmbedHidden: {
    opacity: 0,
  },
  youtubeEmbedFallback: {
    alignItems: "center",
    justifyContent: "center",
  },
  youtubeEmbedFullscreen: {
    flex: 1,
    width: "100%",
    height: "100%",
    aspectRatio: undefined,
  },
  youtubeEmbedOverlay: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    justifyContent: "center",
    alignItems: "center",
    gap: 8,
    paddingVertical: 10,
    paddingHorizontal: 10,
    backgroundColor: "rgba(11, 13, 18, 0.92)",
  },
  youtubeEmbedOverlayText: {
    color: "#f2f4f8",
    fontSize: 13,
    fontWeight: "700",
    textAlign: "center",
  },
  youtubeEmbedOverlayErrorText: {
    color: "#ff8d8d",
  },
  youtubeEmbedRetryButton: {
    borderWidth: 1,
    borderColor: "#3a4458",
    borderRadius: 9,
    backgroundColor: "#1e2635",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  youtubeEmbedRetryButtonText: {
    color: "#f2f4f8",
    fontSize: 12,
    fontWeight: "700",
    textAlign: "center",
  },
  videoContainer: {
    alignItems: "center",
    alignSelf: "stretch",
    marginHorizontal: 0,
    marginBottom: 4,
    minHeight: 200,
    width: "100%",
    overflow: "hidden",
    backgroundColor: "#0b0d12",
  },
  videoContainerFullscreen: {
    marginHorizontal: 0,
    marginBottom: 0,
    minHeight: 0,
    flex: 1,
    width: "100%",
  },
  videoLandscapeLayout: {
    flex: 1,
  },
  videoLandscapePlayer: {
    flex: 1,
  },
  videoLandscapeControls: {
    position: "absolute",
    left: 10,
    right: 10,
    bottom: 10,
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  landscapeOverlayButton: {
    flex: 1,
    marginTop: 0,
    backgroundColor: "rgba(27, 31, 39, 0.9)",
  },
  videoPlaceholder: {
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#151922",
    borderWidth: 1,
    borderColor: "#2a2f3a",
    alignSelf: "stretch",
    marginHorizontal: 0,
    marginBottom: 4,
  },
  placeholderText: { color: "#f2f4f8" },
  pipelineMeta: { color: "#a1a9b7", fontSize: 12, marginTop: 6 },
  pipelineMessage: {
    color: "#c9d1d9",
    fontSize: 12,
    textAlign: "center",
    marginTop: 8,
    paddingHorizontal: 16,
  },
  pipelineError: {
    color: "#ff8d8d",
    fontSize: 12,
    textAlign: "center",
    marginTop: 8,
    paddingHorizontal: 16,
  },
  video: { width: "100%", aspectRatio: 16 / 9, backgroundColor: "#0b0d12" },
  videoFullscreen: {
    flex: 1,
    width: "100%",
    height: "100%",
    aspectRatio: undefined,
  },
  videoScroll: {
    flex: 1,
    width: "100%",
  },
  videoScrollContent: {
    gap: 8,
    paddingBottom: 20,
    flexGrow: 1,
  },
  videoSimpleLayout: {
    paddingTop: 8,
    paddingHorizontal: 0,
    width: "100%",
    alignSelf: "stretch",
  },
  videoSimpleLayoutLandscape: {
    flex: 1,
    paddingTop: 0,
  },
  videoTabContent: {
    flex: 1,
  },
  videoScrollContentLandscape: {
    paddingBottom: 0,
  },
  safeLandscapeVideo: {
    flex: 1,
    backgroundColor: "#000",
    width: "100%",
  },
  landscapeVideoPlaceholder: {
    flex: 1,
    backgroundColor: "#000",
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 20,
  },
  tabBarContainer: {
    backgroundColor: "#0b0d12",
    paddingBottom: 8,
    borderTopWidth: 1,
    borderTopColor: "#1b1f27",
    width: "100%",
    overflow: "hidden",
  },
  tabBar: {
    flexDirection: "row",
    paddingVertical: 12,
    paddingHorizontal: 12,
    backgroundColor: "#0b0d12",
    width: "100%",
    minWidth: 0,
  },
  tabButton: {
    flex: 1,
    paddingVertical: 10,
    alignItems: "center",
    borderRadius: 10,
  },
  tabButtonActive: { backgroundColor: "#1b1f27" },
  tabButtonDisabled: { opacity: 0.4 },
  tabText: { color: "#f2f4f8", fontWeight: "700", fontSize: 18 },
  historyList: {
    flex: 1,
    width: "100%",
    marginBottom: 12,
  },
  historyItem: {
    backgroundColor: "#151922",
    borderRadius: 12,
    padding: 16,
    marginBottom: 12,
    borderWidth: 1,
    borderColor: "#2f3a52",
  },
  historyQuery: {
    color: "#f2f4f8",
    fontSize: 17,
    fontWeight: "700",
    marginBottom: 6,
  },
  historyStatus: {
    color: "#8a92a3",
    fontSize: 13,
    marginBottom: 4,
  },
  historyDate: {
    color: "#5c6472",
    fontSize: 11,
  },
  modalBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.75)",
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  modalCard: {
    width: "100%",
    backgroundColor: "#151922",
    borderRadius: 16,
    padding: 20,
    alignItems: "center",
    gap: 12,
  },
  mixLevelEditorCard: {
    maxWidth: 420,
    alignItems: "stretch",
  },
  mixLevelEditorInput: {
    backgroundColor: "#1b1f27",
    color: "#f2f4f8",
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#2f3a52",
    fontSize: 24,
    fontWeight: "700",
    textAlign: "center",
    paddingVertical: 12,
    paddingHorizontal: 12,
  },
  mixLevelEditorActions: {
    width: "100%",
    flexDirection: "row",
    gap: 10,
    marginTop: 4,
  },
  mixLevelEditorAction: {
    flex: 1,
    marginTop: 0,
  },
  processingScreen: {
    flex: 1,
    justifyContent: "center",
    paddingVertical: 12,
  },
  processingScreenCard: {
    alignSelf: "center",
    width: "100%",
    maxWidth: 360,
    alignItems: "center",
  },
  processingSpinnerCard: {
    paddingVertical: 26,
  },
  processingSpinnerTitle: {
    color: "#f2f4f8",
    fontSize: 21,
    fontWeight: "800",
    textAlign: "center",
  },
  processingElapsedText: {
    color: "#dbe6ff",
    fontSize: 26,
    fontWeight: "800",
    textAlign: "center",
    marginTop: 2,
  },
  processingSpinnerBody: {
    color: "#f2f4f8",
    fontSize: 14,
    fontWeight: "600",
    textAlign: "center",
  },
  processingSpinnerBracket: {
    color: "#f2f4f8",
    fontWeight: "800",
  },
  processingActionsRow: {
    width: "100%",
    flexDirection: "row",
    gap: 10,
    marginTop: 4,
  },
  processingActionButton: {
    flex: 1,
    marginTop: 0,
  },
  modalBackButton: {
    marginTop: 6,
    backgroundColor: "#202636",
    paddingVertical: 10,
    paddingHorizontal: 18,
    borderRadius: 12,
    alignItems: "center",
    borderWidth: 1,
    borderColor: "#2f3a52",
    alignSelf: "stretch",
  },
  modalTitle: { color: "#f2f4f8", fontSize: 21, fontWeight: "800" },
  timingHeadingWrap: {
    width: "100%",
    marginBottom: 2,
    gap: 2,
  },
  timingHeadingLead: {
    color: "#f2f4f8",
    fontSize: 20,
    fontWeight: "800",
    lineHeight: 28,
  },
  timingHeadingMeta: {
    color: "#c7cfdb",
    fontSize: 18,
    fontWeight: "700",
    lineHeight: 24,
  },
  queryNormalizeHint: {
    color: "#8ea0bd",
    fontSize: 12,
    fontWeight: "600",
    marginTop: -2,
    marginBottom: 2,
  },
  timingHeadingArtist: {
    color: "#f2f4f8",
    fontWeight: "800",
  },
  modalMessage: { color: "#c7cfdb", textAlign: "center", fontSize: 16, fontWeight: "600" },
  modalStage: { color: "#8a92a3", fontSize: 13, textAlign: "center", fontStyle: "italic" },
  processingChecklistWrap: {
    width: "100%",
    marginTop: 6,
    paddingTop: 6,
    borderTopWidth: 1,
    borderTopColor: "#263246",
  },
  timingModalCard: {
    maxWidth: 520,
    alignItems: "stretch",
  },
  timingScreenOverlay: {
    ...StyleSheet.absoluteFillObject,
    zIndex: 40,
    elevation: 40,
    backgroundColor: "#0a1020",
  },
  timingScreenSafeArea: {
    flex: 1,
    paddingHorizontal: 18,
    paddingTop: 14,
    paddingBottom: 18,
  },
  timingScreenHeader: {
    gap: 6,
    marginBottom: 12,
  },
  timingScreenElapsedText: {
    color: "#f2f4f8",
    fontSize: 40,
    fontWeight: "900",
    letterSpacing: 0.3,
  },
  timingScreenStageText: {
    color: "#a8b9d7",
    fontSize: 14,
    fontWeight: "700",
  },
  timingScreenScroll: {
    flex: 1,
  },
  timingScreenScrollContent: {
    paddingBottom: 16,
  },
  timingScreenActions: {
    flexDirection: "row",
    gap: 10,
    marginTop: 12,
  },
  timingScreenActionButton: {
    flex: 1,
    marginTop: 0,
  },
  pipelineChecklist: {
    width: "100%",
    gap: 10,
  },
  pipelineChecklistRows: {
    width: "100%",
    gap: 8,
  },
  pipelineChecklistRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    borderWidth: 1,
    borderColor: "#2a2f3a",
    borderRadius: 10,
    backgroundColor: "#1b1f27",
    paddingVertical: 10,
    paddingHorizontal: 12,
  },
  pipelineChecklistRowCompleted: {
    borderColor: "#2e5a39",
    backgroundColor: "#16241a",
  },
  pipelineChecklistRowActive: {
    borderColor: "#3a6ea5",
    backgroundColor: "#1b2738",
  },
  pipelineChecklistLabel: {
    color: "#f2f4f8",
    fontSize: 14,
    fontWeight: "700",
  },
  pipelineChecklistLabelWrap: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  pipelineChecklistLeading: {
    width: 20,
    alignItems: "center",
    justifyContent: "center",
  },
  pipelineChecklistLeadingEmoji: {
    fontSize: 14,
  },
  pipelineChecklistValue: {
    color: "#c9d1d9",
    fontSize: 13,
    fontWeight: "700",
    marginLeft: 8,
  },
  pipelineTotalText: {
    color: "#b8c2d1",
    fontSize: 14,
    fontWeight: "600",
    textAlign: "center",
  },
  pipelineTotalValue: {
    color: "#f2f4f8",
    fontSize: 15,
    fontWeight: "800",
  },
  previewBanner: {
    backgroundColor: "#2a5a8f",
    paddingVertical: 8,
    paddingHorizontal: 12,
    borderRadius: 4,
    marginBottom: 8,
  },
  previewBannerText: { color: "#f2f4f8", fontSize: 13, textAlign: "center", fontWeight: "600" },
  searchResultsCard: {
    maxWidth: 440,
    maxHeight: "78%",
    alignItems: "stretch",
  },
  searchResultsList: {
    width: "100%",
    maxHeight: 360,
  },
  searchResultRow: {
    paddingVertical: 12,
    paddingHorizontal: 12,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#2a2f3a",
    backgroundColor: "#1b1f27",
    marginBottom: 8,
  },
  searchResultTitle: {
    color: "#f2f4f8",
    fontSize: 16,
    fontWeight: "700",
    marginBottom: 4,
  },
  searchResultMeta: {
    color: "#9ba5b6",
    fontSize: 13,
  },
  previewCard: {
    maxWidth: 400,
  },
  previewThumbnailContainer: {
    width: "100%",
    overflow: "hidden",
    borderRadius: 8,
    marginBottom: 12,
  },
  previewInfo: {
    width: "100%",
    gap: 6,
    marginBottom: 12,
  },
  previewTitle: {
    color: "#f2f4f8",
    fontSize: 16,
    fontWeight: "600",
    textAlign: "center",
  },
  previewDuration: {
    color: "#8a92a3",
    fontSize: 14,
    textAlign: "center",
  },
  previewEstimate: {
    color: "#f6c56a",
    fontSize: 13,
    textAlign: "center",
    fontStyle: "italic",
  },
  previewActions: {
    flexDirection: "row",
    gap: 12,
    width: "100%",
  },
  modalButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 12,
    alignItems: "center",
  },
  modalCancelButton: {
    backgroundColor: "#202636",
    borderWidth: 1,
    borderColor: "#2f3a52",
  },
  modalConfirmButton: {
    backgroundColor: "#4a90e2",
  },
});
