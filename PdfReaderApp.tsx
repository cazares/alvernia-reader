import { Asset } from "expo-asset";
import * as FileSystem from "expo-file-system/legacy";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ActivityIndicator, Pressable, StatusBar, StyleSheet, Text, View } from "react-native";
import { WebView, type WebViewMessageEvent } from "react-native-webview";

import {
  addNearbyDirectorSyncListener,
  isNearbyDirectorSyncAvailable,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
  stopNearbyDirectorSync,
} from "./src/nearbyDirectorSync";
import { OFFLINE_WEB_BUNDLE_ASSETS, OFFLINE_WEB_BUNDLE_VERSION } from "./src/offlineWebBundle";

const BRIDGE_CHANNEL = "signovivo-native";
const OFFLINE_READER_ENTRY = "index.html";
const OFFLINE_READER_SCRIPT = "app.bundle";
const OFFLINE_PAGE_ASSET_PATTERN = /^pages\/page-(\d+)\.jpg$/;
const OFFLINE_READER_HTML_CACHE = `${FileSystem.cacheDirectory}signovivo-offline-reader.html`;

type SyncRole = "off" | "director" | "follower";

const getReadableError = (error: unknown) => {
  if (error instanceof Error && error.message) return error.message;
  return "No se pudo preparar Signo Vino offline.";
};

const getBundledAssetUri = async (moduleId: number, { download = false } = {}) => {
  const asset = Asset.fromModule(moduleId);
  if (download) {
    await asset.downloadAsync();
  }
  const sourceUri = asset.localUri || asset.uri;
  if (!sourceUri) {
    throw new Error("No encontramos un recurso offline del lector.");
  }
  return sourceUri;
};

const readBundledTextAsset = async (relativePath: string) => {
  const moduleId = OFFLINE_WEB_BUNDLE_ASSETS[relativePath];
  if (!moduleId) {
    throw new Error(`Falta el recurso offline ${relativePath}.`);
  }
  const assetUri = await getBundledAssetUri(moduleId, { download: true });
  return FileSystem.readAsStringAsync(assetUri);
};

const buildOfflinePageMap = async () => {
  const offlinePages: Record<number, string> = {};

  const downloads = Object.entries(OFFLINE_WEB_BUNDLE_ASSETS)
    .filter(([relativePath]) => OFFLINE_PAGE_ASSET_PATTERN.test(relativePath))
    .map(async ([relativePath, moduleId]) => {
      const pageNumber = Number.parseInt(relativePath.match(OFFLINE_PAGE_ASSET_PATTERN)![1], 10);
      const asset = Asset.fromModule(moduleId);
      await asset.downloadAsync();
      if (asset.localUri) {
        offlinePages[pageNumber] = asset.localUri;
      }
    });

  await Promise.all(downloads);
  return offlinePages;
};

const buildOfflineReaderHtml = async () => {
  const [indexHtml, appJs, offlinePages] = await Promise.all([
    readBundledTextAsset(OFFLINE_READER_ENTRY),
    readBundledTextAsset(OFFLINE_READER_SCRIPT),
    buildOfflinePageMap(),
  ]);

  const nativeBootstrap = [
    `window.__SIGNO_VINO_NATIVE_FILE_MODE = true;`,
    `window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION = ${JSON.stringify(OFFLINE_WEB_BUNDLE_VERSION)};`,
    `window.OFFLINE_PAGES = ${JSON.stringify(offlinePages)};`,
  ].join("\n");

  return indexHtml
    .replace(/\s*<link rel="manifest"[^>]*>\n?/, "\n")
    .replace(/\s*<link rel="icon"[^>]*>\n?/, "\n")
    .replace(/\s*<link rel="apple-touch-icon"[^>]*>\n?/, "\n")
    .replace(
      '<script defer src="app.js"></script>',
      `<script>\n${nativeBootstrap}\n</script>\n    <script>\n${appJs}\n</script>`,
    );
};

const resolveOfflineReaderUri = async () => {
  const entryModule = OFFLINE_WEB_BUNDLE_ASSETS[OFFLINE_READER_ENTRY];
  if (!entryModule) {
    throw new Error("No encontramos el lector web offline.");
  }

  const asset = Asset.fromModule(entryModule);
  await asset.downloadAsync();
  const html = await buildOfflineReaderHtml();
  await FileSystem.writeAsStringAsync(OFFLINE_READER_HTML_CACHE, html);
  return OFFLINE_READER_HTML_CACHE;
};

export default function App() {
  const [readerUri, setReaderUri] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [reloadKey, setReloadKey] = useState(0);
  const webViewRef = useRef<WebView>(null);
  const syncRoleRef = useRef<SyncRole>("off");
  const currentPageRef = useRef(2);
  const totalPagesRef = useRef(0);
  const syncAvailable = isNearbyDirectorSyncAvailable();

  const injectNativeEvent = useCallback((payload: Record<string, unknown>) => {
    webViewRef.current?.injectJavaScript(
      `window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent(${JSON.stringify(payload)}); true;`,
    );
  }, []);

  const sendBridgeState = useCallback(() => {
    injectNativeEvent({
      type: "bridge-state",
      available: syncAvailable,
      role: syncRoleRef.current,
    });
  }, [injectNativeEvent, syncAvailable]);

  useEffect(() => {
    let cancelled = false;

    const loadReader = async () => {
      try {
        setErrorMessage("");
        setReaderUri(null);
        const nextUri = await resolveOfflineReaderUri();
        if (!nextUri) {
          throw new Error("No encontramos el lector web offline.");
        }
        if (!cancelled) {
          setReaderUri(nextUri);
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(getReadableError(error));
        }
      }
    };

    loadReader();
    return () => {
      cancelled = true;
    };
  }, [reloadKey]);

  useEffect(() => {
    if (!syncAvailable) return undefined;

    const subscription = addNearbyDirectorSyncListener((event) => {
      if (event.type === "state") {
        if (event.status === "idle") {
          syncRoleRef.current = "off";
        } else if (event.role === "director" || event.role === "follower") {
          syncRoleRef.current = event.role;
        }
      } else if (event.type === "error" && event.code === "DIRECTOR_CONFLICT") {
        syncRoleRef.current = "off";
      }

      injectNativeEvent({
        type: "sync-event",
        event,
      });
    });

    return () => {
      subscription.remove();
    };
  }, [injectNativeEvent, syncAvailable]);

  useEffect(() => () => {
    if (!syncAvailable) return;
    stopNearbyDirectorSync().catch(() => {});
  }, [syncAvailable]);

  const readAccessUrl = useMemo(() => {
    if (!readerUri?.startsWith("file://")) return undefined;
    return readerUri.replace(/[^/]+$/, "");
  }, [readerUri]);

  const handleWebMessage = useCallback(async (event: WebViewMessageEvent) => {
    let payload: Record<string, unknown> | null = null;
    try {
      payload = JSON.parse(event.nativeEvent.data);
    } catch {
      return;
    }

    if (!payload || payload.channel !== BRIDGE_CHANNEL) return;

    try {
      switch (payload.type) {
        case "bridge-ready":
          if (typeof payload.page === "number") currentPageRef.current = payload.page;
          if (typeof payload.totalPages === "number") totalPagesRef.current = payload.totalPages;
          sendBridgeState();
          break;
        case "page-changed":
          if (typeof payload.page === "number") currentPageRef.current = payload.page;
          if (typeof payload.totalPages === "number") totalPagesRef.current = payload.totalPages;
          if (syncRoleRef.current === "director") {
            await sendNearbyDirectorPageUpdate(currentPageRef.current, totalPagesRef.current);
          }
          break;
        case "sync-start-director":
          await startNearbyDirector(String(payload.sessionCode || ""));
          syncRoleRef.current = "director";
          sendBridgeState();
          break;
        case "sync-start-follower":
          await startNearbyFollower(String(payload.sessionCode || ""));
          syncRoleRef.current = "follower";
          sendBridgeState();
          break;
        case "sync-stop":
          await stopNearbyDirectorSync();
          syncRoleRef.current = "off";
          sendBridgeState();
          break;
        default:
          break;
      }
    } catch (error) {
      injectNativeEvent({
        type: "sync-event",
        event: {
          type: "error",
          code: "WEBVIEW_BRIDGE_FAILED",
          message: getReadableError(error),
          role: syncRoleRef.current,
        },
      });
    }
  }, [injectNativeEvent, sendBridgeState]);

  return (
    <View style={styles.screen}>
      <StatusBar hidden barStyle="light-content" />
      {readerUri ? (
        <WebView
          ref={webViewRef}
          allowingReadAccessToURL={readAccessUrl}
          allowsBackForwardNavigationGestures={false}
          allowsInlineMediaPlayback
          bounces={false}
          domStorageEnabled
          injectedJavaScriptBeforeContentLoaded={"window.__SIGNO_VINO_NATIVE_FILE_MODE = true; true;"}
          onLoadEnd={sendBridgeState}
          onMessage={handleWebMessage}
          originWhitelist={["*"]}
          setSupportMultipleWindows={false}
          source={{ uri: readerUri }}
          style={styles.webview}
        />
      ) : (
        <View style={styles.loadingShell}>
          <ActivityIndicator color="#93e4ff" size="large" />
          <Text style={styles.loadingTitle}>Preparando Signo Vino</Text>
          <Text style={styles.loadingText}>
            {errorMessage || "Abriendo la experiencia completa de signovivo.com para que funcione 100% offline."}
          </Text>
          {errorMessage ? (
            <Pressable onPress={() => setReloadKey((value) => value + 1)} style={styles.retryButton}>
              <Text style={styles.retryText}>Reintentar</Text>
            </Pressable>
          ) : null}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: "#030508",
  },
  webview: {
    flex: 1,
    backgroundColor: "#000000",
  },
  loadingShell: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 28,
    backgroundColor: "#060910",
    gap: 14,
  },
  loadingTitle: {
    color: "#ffffff",
    fontSize: 24,
    fontWeight: "700",
    textAlign: "center",
  },
  loadingText: {
    color: "#dbe4f7",
    fontSize: 16,
    lineHeight: 24,
    textAlign: "center",
    maxWidth: 420,
  },
  retryButton: {
    marginTop: 8,
    borderRadius: 999,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "#173463",
  },
  retryText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
});
