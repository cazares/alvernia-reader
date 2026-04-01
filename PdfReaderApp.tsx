import { Asset } from "expo-asset";
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

const OFFLINE_READER_MODULE = require("./web/dist/signo-vino-offline.html");
const BRIDGE_CHANNEL = "signovivo-native";

type SyncRole = "off" | "director" | "follower";

const getReadableError = (error: unknown) => {
  if (error instanceof Error && error.message) return error.message;
  return "No se pudo preparar el lector offline.";
};

const toInjectedScript = (payload: Record<string, unknown>) =>
  `window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent(${JSON.stringify(payload)}); true;`;

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
    webViewRef.current?.injectJavaScript(toInjectedScript(payload));
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

    const loadOfflineReader = async () => {
      try {
        setErrorMessage("");
        setReaderUri(null);
        const asset = Asset.fromModule(OFFLINE_READER_MODULE);
        await asset.downloadAsync();
        const nextUri = asset.localUri || asset.uri;
        if (!nextUri) {
          throw new Error("No encontramos el archivo del lector offline.");
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

    loadOfflineReader();

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
          decelerationRate="normal"
          domStorageEnabled
          onLoadEnd={sendBridgeState}
          onMessage={handleWebMessage}
          originWhitelist={["*"]}
          scrollEnabled
          source={{ uri: readerUri }}
          style={styles.webview}
        />
      ) : (
        <View style={styles.loadingShell}>
          <ActivityIndicator color="#93e4ff" size="large" />
          <Text style={styles.loadingTitle}>Preparando Signo Vino</Text>
          <Text style={styles.loadingText}>
            {errorMessage || "Cargando la misma experiencia del lector web, totalmente offline."}
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
    backgroundColor: "#000",
  },
  webview: {
    flex: 1,
    backgroundColor: "#000",
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
    color: "#d8dff0",
    fontSize: 16,
    lineHeight: 24,
    textAlign: "center",
    maxWidth: 420,
  },
  retryButton: {
    marginTop: 8,
    borderRadius: 999,
    paddingHorizontal: 20,
    paddingVertical: 12,
    backgroundColor: "#173463",
  },
  retryText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
});
