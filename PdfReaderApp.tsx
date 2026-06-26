// SignoVivo — native shell (build 332+).
//
// The entire reader UI now lives in a locally-bundled copy of signovivo.com,
// loaded into a WKWebView (via react-native-webview) from file://. This file is a
// thin native shell whose only jobs are the things the web cannot do:
//   1. Serve the bundled web app offline (file://…/WebBundle/index.html), preferring
//      a peer-pushed update in Documents/WebBundle if one has landed.
//   2. Bridge the offline iPad-to-iPad Multipeer mesh <-> the web app (page sync).
//   3. Validate director codes entered on the web numpad and drive director/follower role.
//   4. Publish the director's page to the Cloudflare relay so signovivo.com followers sync.
//   5. Pick the starting hymnal book from IP geolocation (Del Rio -> standard manual,
//      everywhere else -> hymns-4) and keep the screen awake.
//
// Everything else — rendering, paging, zoom, search, browse, song navigation — is the
// web app's job. The old 3,536-line FlatList/PDF reader was replaced wholesale.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AppState, Platform, StatusBar, StyleSheet, View } from "react-native";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { useKeepAwake } from "expo-keep-awake";
import * as FileSystem from "expo-file-system/legacy";
import WebView, { type WebViewMessageEvent } from "react-native-webview";

import {
  addNearbyDirectorSyncListener,
  denyDirectorTakeover,
  isNearbyDirectorSyncAvailable,
  primeNearbyPermissions,
  refreshNearbyDiscovery,
  requestCurrentSnapshot,
  resetNearbyDirectorSync,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
} from "./src/nearbyDirectorSync";
import { publishPageToRelay, TRANSMITTER_ACCESS_CODE } from "./src/directorRelaySync";
import { STORAGE_KEYS, type BookId } from "./src/offlineBooks";
import versionJson from "./version.json";

// ─────────────────────────────── Constants ──────────────────────────────────

const BUILD_VERSION = String((versionJson as { buildNumber?: number }).buildNumber ?? "");
const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";
const RELAY_ROOM = "alvernia-main";
const GEO_STATE_URL = `${RELAY_BASE}/r/${RELAY_ROOM}/state`;

// Fixed Multipeer session for the parish mesh (unchanged from the native reader).
const DIRECTOR_SESSION = "1234";

// Director codes — admin can force-takeover; the three regular codes start a director
// session (the Swift conflict logic lets the most-recent director win on contention).
const ADMIN_CODE = "8307343376";
const DIRECTOR_ACCESS_CODES = new Set<string>([
  ADMIN_CODE,
  "8304533367",
  "8307197000",
  "8303130470",
]);
// Secret numpad codes carried over from the native reader.
const SOFT_RESET_CODE = "744668486";
const FORCE_DIRECTOR_CODE = "50711";
// Last-director restore window: become director again on relaunch if we were one recently.
const DIRECTOR_RESTORE_WINDOW_MS = 24 * 60 * 60 * 1000;

const STORED_CODE_KEY = "director_access_code";
const DEFAULT_BOOK: BookId = "hymns-4";

type SyncRole = "off" | "director" | "follower";

const isBookId = (value: unknown): value is BookId => value === "standard" || value === "hymns-4";
const digitsOnly = (value: unknown) => String(value ?? "").replace(/[^0-9]/g, "");
const modeForBook = (book: BookId) => (book === "standard" ? "standard" : "nonStandard");
// Multipeer/relay carry both a canonical bookId and a legacy mode string; prefer bookId.
const bookFromSync = (bookId: unknown, mode: unknown): BookId => {
  if (isBookId(bookId)) return bookId;
  return String(mode ?? "") === "standard" ? "standard" : "hymns-4";
};

// ──────────────────────────────── App ───────────────────────────────────────

export default function App() {
  useKeepAwake("signovivo-reader");

  const webViewRef = useRef<WebView>(null);
  const [booted, setBooted] = useState(false);
  const [initialBook, setInitialBook] = useState<BookId>(DEFAULT_BOOK);
  // Bumped to force a fresh WebView mount (e.g. after a peer-pushed bundle update / soft reset).
  const [bundleUri, setBundleUri] = useState<string | null>(null);
  const [mountKey, setMountKey] = useState(0);

  const roleRef = useRef<SyncRole>("off");
  const explicitTransmitterRef = useRef(false);
  const currentPageRef = useRef(1);
  const totalPagesRef = useRef(0);
  const currentBookRef = useRef<BookId>(DEFAULT_BOOK);
  const webReadyRef = useRef(false);
  const pendingInjectRef = useRef<string[]>([]);
  const storedBookRef = useRef<BookId | null>(null);
  // Last page/book the DIRECTOR broadcast to us over the mesh (distinct from the web's own
  // page). Drives resync after a WebView reload / foreground for followers. Null until a
  // director snapshot has actually been received (a fresh-boot follower must keep its own page).
  const lastDirectorSnapshotRef = useRef<{ page: number; book: BookId } | null>(null);
  // Director re-broadcast heartbeat: keeps late joiners + the Cloudflare relay snapshot fresh.
  const directorHeartbeatRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const syncAvailable = useMemo(() => isNearbyDirectorSyncAvailable(), []);

  // ── Breadcrumb (lightweight crash forensics; survives a hard restart) ──────
  const breadcrumb = useCallback((tag: string) => {
    try {
      AsyncStorage.setItem("sv_bc", `${tag} @ ${Date.now()}`).catch(() => {});
    } catch {
      /* ignore */
    }
  }, []);

  // ── Native -> Web injection (queued until the web app signals bridge-ready) ──
  const injectEvent = useCallback((payload: Record<string, unknown>) => {
    const js =
      `window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent(${JSON.stringify(
        payload,
      )}); true;`;
    if (!webReadyRef.current || !webViewRef.current) {
      pendingInjectRef.current.push(js);
      return;
    }
    try {
      webViewRef.current.injectJavaScript(js);
    } catch {
      pendingInjectRef.current.push(js);
    }
  }, []);

  const flushPendingInjects = useCallback(() => {
    const ref = webViewRef.current;
    if (!ref) return;
    const queued = pendingInjectRef.current.splice(0, pendingInjectRef.current.length);
    for (const js of queued) {
      try {
        ref.injectJavaScript(js);
      } catch {
        /* drop */
      }
    }
  }, []);

  // ── Page broadcast: mesh (director) + relay (director or explicit transmitter) ──
  const broadcastPage = useCallback((page: number, book: BookId) => {
    const mode = modeForBook(book);
    const total = totalPagesRef.current;
    if (roleRef.current === "director") {
      sendNearbyDirectorPageUpdate(page, total, { mode, bookId: book }).catch(() => {});
    }
    if (roleRef.current === "director" || explicitTransmitterRef.current) {
      try {
        publishPageToRelay(page, total, { mode, bookId: book });
      } catch {
        /* network flaps are expected; next page change re-publishes */
      }
    }
  }, []);

  // ── Director re-broadcast heartbeat ─────────────────────────────────────────
  // Re-broadcasts the director's current page every 12s. Idempotent downstream
  // (latest-wins / seq-guarded), so it just helps freshly-joined or packet-dropping
  // followers catch up and keeps the relay snapshot's `ts` fresh for online followers.
  const stopDirectorHeartbeat = useCallback(() => {
    if (directorHeartbeatRef.current) {
      clearInterval(directorHeartbeatRef.current);
      directorHeartbeatRef.current = null;
    }
  }, []);

  const startDirectorHeartbeat = useCallback(() => {
    stopDirectorHeartbeat();
    directorHeartbeatRef.current = setInterval(() => {
      if (roleRef.current !== "director") return; // never run unless we're the director
      broadcastPage(currentPageRef.current, currentBookRef.current);
    }, 12000);
  }, [broadcastPage, stopDirectorHeartbeat]);

  // ── Become director ────────────────────────────────────────────────────────
  const becomeDirector = useCallback(
    async (code: string) => {
      if (!syncAvailable) {
        // No mesh on this device — still act as an online transmitter to the relay.
        explicitTransmitterRef.current = true;
        roleRef.current = "off";
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        return;
      }
      try {
        try {
          await startNearbyDirector(DIRECTOR_SESSION);
        } catch {
          // Mesh startup can transiently fail (permission race, radio warm-up).
          // Wait briefly and retry the start exactly once before giving up.
          await new Promise((r) => setTimeout(r, 2000));
          await startNearbyDirector(DIRECTOR_SESSION);
        }
        roleRef.current = "director";
        await AsyncStorage.multiSet([
          [STORAGE_KEYS.lastSyncRole, "director"],
          [STORAGE_KEYS.lastDirectorAt, String(Date.now())],
          [STORED_CODE_KEY, code],
        ]);
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat();
        breadcrumb("director");
      } catch {
        injectEvent({ type: "role", role: "none" });
      }
    },
    [syncAvailable, injectEvent, broadcastPage, breadcrumb, startDirectorHeartbeat],
  );

  const becomeFollower = useCallback(async () => {
    roleRef.current = "follower";
    explicitTransmitterRef.current = false;
    stopDirectorHeartbeat(); // a follower must never re-broadcast
    try {
      await AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower");
      if (syncAvailable) {
        try {
          await startNearbyFollower(DIRECTOR_SESSION);
        } catch {
          // Transient mesh startup failure (permission race, radio warm-up):
          // wait briefly and retry the start exactly once before giving up.
          await new Promise((r) => setTimeout(r, 2000));
          await startNearbyFollower(DIRECTOR_SESSION);
        }
      }
    } catch {
      /* ignore */
    }
    injectEvent({ type: "role", role: "follower" });
  }, [syncAvailable, injectEvent, stopDirectorHeartbeat]);

  // ── Soft reset (secret code 744668486) ─────────────────────────────────────
  const performSoftReset = useCallback(async () => {
    breadcrumb("soft-reset");
    stopDirectorHeartbeat();
    try {
      await resetNearbyDirectorSync();
    } catch {
      /* ignore */
    }
    try {
      await AsyncStorage.multiRemove([
        STORAGE_KEYS.lastSyncRole,
        STORAGE_KEYS.lastDirectorAt,
        STORED_CODE_KEY,
      ]);
    } catch {
      /* ignore */
    }
    roleRef.current = "off";
    explicitTransmitterRef.current = false;
    webReadyRef.current = false;
    pendingInjectRef.current = [];
    setMountKey((k) => k + 1); // remount the WebView from scratch
  }, [breadcrumb, stopDirectorHeartbeat]);

  // ── Director-code dispatch (codes entered on the web numpad) ────────────────
  const onDirectorCode = useCallback(
    (rawCode: unknown) => {
      const code = digitsOnly(rawCode);
      if (!code) {
        injectEvent({ type: "role", role: "none" });
        return;
      }
      if (code === SOFT_RESET_CODE) {
        performSoftReset();
        return;
      }
      if (code === FORCE_DIRECTOR_CODE || DIRECTOR_ACCESS_CODES.has(code)) {
        becomeDirector(code);
        return;
      }
      if (code === digitsOnly(TRANSMITTER_ACCESS_CODE)) {
        explicitTransmitterRef.current = true;
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        return;
      }
      // Unrecognized → tell the web it was wrong so it can surface "código incorrecto".
      injectEvent({ type: "role", role: "none" });
    },
    [injectEvent, performSoftReset, becomeDirector, broadcastPage],
  );

  // ── Web -> Native message router ───────────────────────────────────────────
  const handleMessage = useCallback(
    (event: WebViewMessageEvent) => {
      let msg: Record<string, unknown> | null = null;
      try {
        msg = JSON.parse(event.nativeEvent.data);
      } catch {
        return;
      }
      if (!msg || msg.channel !== "signovivo-native") return;

      switch (msg.type) {
        case "bridge-ready": {
          webReadyRef.current = true;
          if (typeof msg.page === "number") currentPageRef.current = msg.page;
          if (typeof msg.totalPages === "number") totalPagesRef.current = msg.totalPages;
          if (isBookId(msg.book)) currentBookRef.current = msg.book;
          flushPendingInjects();
          injectEvent({ type: "bridge-state", available: syncAvailable });
          // Always re-assert the current role to a freshly (re)loaded WebView so the web app's
          // numpad/role UI matches reality after a crash-reload or boot.
          injectEvent({ type: "role", role: roleRef.current === "off" ? "none" : roleRef.current });
          if (roleRef.current === "director") {
            // The director's own page is authoritative — just re-broadcast it.
            broadcastPage(currentPageRef.current, currentBookRef.current);
          } else if (roleRef.current === "follower" && lastDirectorSnapshotRef.current) {
            // A RELOADED follower (we already have a director snapshot) must resync to the
            // director's last-known page instead of showing the web's stale/default boot page.
            // The null-guard above is critical: a FRESH-BOOT follower that has never received a
            // director snapshot keeps the web's own boot page (geo/stored) — we do NOT override.
            const { page, book } = lastDirectorSnapshotRef.current;
            currentPageRef.current = page;
            currentBookRef.current = book;
            if (book !== msg.book) injectEvent({ type: "set-book", book });
            injectEvent({ type: "sync-event", event: { type: "page", page, book } });
          }
          break;
        }
        case "page-changed": {
          const page = Number(msg.page) || currentPageRef.current;
          currentPageRef.current = page;
          if (typeof msg.totalPages === "number" && msg.totalPages > 0) {
            totalPagesRef.current = msg.totalPages;
          }
          if (isBookId(msg.book)) currentBookRef.current = msg.book;
          // Persist per-book last page for restore.
          AsyncStorage.setItem(
            `${STORAGE_KEYS.lastPagePrefix}${currentBookRef.current}`,
            String(page),
          ).catch(() => {});
          broadcastPage(page, currentBookRef.current);
          break;
        }
        case "director-code":
          onDirectorCode(msg.code);
          break;
        case "book-changed": {
          if (isBookId(msg.book)) {
            currentBookRef.current = msg.book;
            storedBookRef.current = msg.book;
            AsyncStorage.setItem(STORAGE_KEYS.activeBookId, msg.book).catch(() => {});
            // A director switching books must move followers too.
            broadcastPage(currentPageRef.current, msg.book);
          }
          break;
        }
        default:
          break;
      }
    },
    [flushPendingInjects, injectEvent, syncAvailable, broadcastPage, onDirectorCode],
  );

  // ── Resolve the bundle URI (prefer a peer-pushed update in Documents) ───────
  const resolveBundleUri = useCallback(async (): Promise<string> => {
    const docDir = FileSystem.documentDirectory;
    if (docDir) {
      const docIndex = `${docDir}WebBundle/index.html`;
      try {
        const info = await FileSystem.getInfoAsync(docIndex);
        if (info.exists) return docIndex;
      } catch {
        /* fall through to bundled copy */
      }
    }
    const bundleDir = FileSystem.bundleDirectory || "";
    return `${bundleDir}WebBundle/index.html`;
  }, []);

  // ── Boot: restore book + role, resolve bundle, render WebView ───────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      breadcrumb("boot");
      let stored: BookId | null = null;
      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEYS.activeBookId);
        if (isBookId(raw)) stored = raw;
      } catch {
        /* ignore */
      }
      storedBookRef.current = stored;
      const startBook: BookId = stored ?? DEFAULT_BOOK;
      currentBookRef.current = startBook;

      const uri = await resolveBundleUri();
      if (cancelled) return;
      setInitialBook(startBook);
      setBundleUri(uri);
      setBooted(true);
    })();
    return () => {
      cancelled = true;
    };
  }, [breadcrumb, resolveBundleUri]);

  // ── Multipeer permissions + role bootstrap + event listener ─────────────────
  useEffect(() => {
    if (!syncAvailable) return;
    primeNearbyPermissions().catch(() => {});

    (async () => {
      try {
        const [lastRole, lastAtRaw] = await Promise.all([
          AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole),
          AsyncStorage.getItem(STORAGE_KEYS.lastDirectorAt),
        ]);
        const lastAt = Number(lastAtRaw) || 0;
        const recentlyDirector =
          lastRole === "director" && Date.now() - lastAt < DIRECTOR_RESTORE_WINDOW_MS;
        if (recentlyDirector) {
          const code = (await AsyncStorage.getItem(STORED_CODE_KEY)) || ADMIN_CODE;
          await becomeDirector(code);
        } else {
          await becomeFollower();
        }
      } catch {
        becomeFollower();
      }
    })();

    const sub = addNearbyDirectorSyncListener((event: Record<string, unknown>) => {
      const type = String(event?.type ?? "");
      switch (type) {
        case "page": {
          if (roleRef.current === "director") break; // ignore our own echoes
          const book = bookFromSync(event.bookId, event.mode);
          const page = Number(event.page) || currentPageRef.current;
          currentPageRef.current = page;
          // Remember the director's latest snapshot so a reloaded/foregrounded follower resyncs.
          lastDirectorSnapshotRef.current = { page, book };
          if (book !== currentBookRef.current) {
            currentBookRef.current = book;
            injectEvent({ type: "set-book", book });
          }
          injectEvent({ type: "sync-event", event: { type: "page", page, book } });
          break;
        }
        case "state": {
          injectEvent({
            type: "sync-event",
            event: {
              type: "state",
              status: event.status,
              role: event.role,
              message: event.message,
            },
          });
          break;
        }
        case "error": {
          if (String(event.code ?? "") === "DIRECTOR_CONFLICT") {
            stopDirectorHeartbeat(); // a newer director won; stop re-broadcasting
            becomeFollower(); // step down
          }
          break;
        }
        case "takeover-request": {
          // v1: a director auto-denies takeover requests. Admin-initiated force-takeover
          // rides the Swift conflict path (later token wins) when the admin starts director.
          if (roleRef.current === "director" && event.requestId != null) {
            denyDirectorTakeover(String(event.requestId)).catch(() => {});
          }
          break;
        }
        case "bundleUpdated": {
          // A peer pushed a newer web bundle; reload from Documents/WebBundle.
          (async () => {
            const uri = await resolveBundleUri();
            webReadyRef.current = false;
            pendingInjectRef.current = [];
            setBundleUri(uri);
            setMountKey((k) => k + 1);
          })();
          break;
        }
        default:
          break;
      }
    });

    return () => {
      try {
        sub.remove();
      } catch {
        /* ignore */
      }
      stopDirectorHeartbeat(); // no leaked interval on unmount / effect re-run
    };
  }, [
    syncAvailable,
    becomeDirector,
    becomeFollower,
    injectEvent,
    resolveBundleUri,
    stopDirectorHeartbeat,
  ]);

  // ── IP-geo book selection (first launch only; stored pref wins thereafter) ──
  useEffect(() => {
    if (!booted) return;
    if (storedBookRef.current) return; // user/device already has a book preference
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(GEO_STATE_URL, { cache: "no-store" });
        const hymnal = res.headers.get("X-Hymnal");
        const geoBook: BookId | null =
          hymnal === "standard" ? "standard" : hymnal === "nonstandard" ? "hymns-4" : null;
        if (cancelled || !geoBook || geoBook === currentBookRef.current) return;
        currentBookRef.current = geoBook;
        storedBookRef.current = geoBook;
        AsyncStorage.setItem(STORAGE_KEYS.activeBookId, geoBook).catch(() => {});
        injectEvent({ type: "set-book", book: geoBook });
      } catch {
        /* offline: keep the default/stored book */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [booted, injectEvent]);

  // ── Foreground: nudge mesh rediscovery + pull a fresh snapshot ──────────────
  useEffect(() => {
    if (!syncAvailable) return;
    const sub = AppState.addEventListener("change", (next) => {
      if (next !== "active") return;
      refreshNearbyDiscovery().catch(() => {});
      if (roleRef.current === "follower") {
        requestCurrentSnapshot().catch(() => {});
        // Re-assert the director's last-known snapshot immediately so the view is correct on
        // foreground while the fresh snapshot request round-trips over the mesh.
        if (lastDirectorSnapshotRef.current) {
          const { page, book } = lastDirectorSnapshotRef.current;
          currentPageRef.current = page;
          if (book !== currentBookRef.current) {
            currentBookRef.current = book;
            injectEvent({ type: "set-book", book });
          }
          injectEvent({ type: "sync-event", event: { type: "page", page, book } });
        }
      } else if (roleRef.current === "director") {
        broadcastPage(currentPageRef.current, currentBookRef.current);
      }
    });
    return () => sub.remove();
  }, [syncAvailable, broadcastPage, injectEvent]);

  // ── Global JS error trap (breadcrumb only; the web app owns its own UI) ──────
  useEffect(() => {
    const g = globalThis as unknown as {
      ErrorUtils?: { getGlobalHandler?: () => unknown; setGlobalHandler?: (h: unknown) => void };
    };
    const prev = g.ErrorUtils?.getGlobalHandler?.();
    g.ErrorUtils?.setGlobalHandler?.((error: unknown, isFatal?: boolean) => {
      breadcrumb(`jserror:${isFatal ? "fatal" : "soft"}`);
      if (typeof prev === "function") (prev as (e: unknown, f?: boolean) => void)(error, isFatal);
    });
  }, [breadcrumb]);

  const preloadScript = useMemo(
    () =>
      [
        "window.__SIGNO_VINO_NATIVE_FILE_MODE = true;",
        `window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION = ${JSON.stringify(BUILD_VERSION)};`,
        `window.__SIGNO_VINO_INITIAL_BOOK = ${JSON.stringify(initialBook)};`,
        "true;",
      ].join("\n"),
    [initialBook],
  );

  if (!booted || !bundleUri) {
    return <View style={styles.blank} />;
  }

  return (
    <View style={styles.root}>
      <StatusBar hidden />
      <WebView
        key={`webbundle-${mountKey}`}
        ref={webViewRef}
        source={{ uri: bundleUri }}
        originWhitelist={["*"]}
        allowFileAccess
        allowFileAccessFromFileURLs
        allowUniversalAccessFromFileURLs
        injectedJavaScriptBeforeContentLoaded={preloadScript}
        onMessage={handleMessage}
        onError={() => breadcrumb("webview-error")}
        onContentProcessDidTerminate={() => {
          breadcrumb("webview-terminated");
          webReadyRef.current = false;
          webViewRef.current?.reload();
        }}
        allowsInlineMediaPlayback
        mediaPlaybackRequiresUserAction={false}
        cacheEnabled={true}
        allowsLinkPreview={false}
        dataDetectorTypes="none"
        setSupportMultipleWindows={false}
        bounces={false}
        overScrollMode="never"
        scrollEnabled
        textInteractionEnabled={false}
        style={styles.web}
        {...(Platform.OS === "ios" ? { allowsBackForwardNavigationGestures: false } : {})}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#000" },
  blank: { flex: 1, backgroundColor: "#000" },
  web: { flex: 1, backgroundColor: "#000" },
});
