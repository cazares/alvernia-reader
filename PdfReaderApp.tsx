import { Asset } from "expo-asset";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ActivityIndicator, Pressable, StatusBar, StyleSheet, Text, View } from "react-native";
import ReactNativeBlobUtil from "react-native-blob-util";
import { WebView } from "react-native-webview";
import { OFFLINE_WEB_BUNDLE_ASSETS, OFFLINE_WEB_BUNDLE_VERSION } from "./src/offlineWebBundle";

const OFFLINE_BUNDLE_DIR = `${ReactNativeBlobUtil.fs.dirs.DocumentDir}/signovivo-offline-web`;
const OFFLINE_BUNDLE_VERSION_PATH = `${OFFLINE_BUNDLE_DIR}/.bundle-version`;
const OFFLINE_BUNDLE_BATCH_SIZE = 12;
const PAGE_ASSET_PREFIX = "pages/";

const getReadableError = (error: unknown) => {
  if (error instanceof Error && error.message) return error.message;
  return "No se pudo preparar el lector offline.";
};

const OFFLINE_BRIDGE_DISABLED_SCRIPT = [
  "window.__SIGNO_VINO_NATIVE_FILE_MODE = true;",
  "window.OFFLINE_PAGES = {};",
  "window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent({ type: 'bridge-state', available: false, role: 'off' });",
  "true;",
].join(" ");

export default function App() {
  const [readerUri, setReaderUri] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [reloadKey, setReloadKey] = useState(0);
  const [offlinePagesScript, setOfflinePagesScript] = useState(OFFLINE_BRIDGE_DISABLED_SCRIPT);
  const webViewRef = useRef<WebView>(null);

  useEffect(() => {
    let cancelled = false;

    const ensureParentDirectory = async (filePath: string) => {
      const parentPath = filePath.slice(0, filePath.lastIndexOf("/"));
      if (!parentPath) return;
      const exists = await ReactNativeBlobUtil.fs.exists(parentPath);
      if (!exists) {
        await ReactNativeBlobUtil.fs.mkdir(parentPath);
      }
    };

    const buildOfflineBundle = async () => {
      const bundleEntries = Object.entries(OFFLINE_WEB_BUNDLE_ASSETS);
      const pageBundleEntries = bundleEntries.filter(([relativePath]) => relativePath.startsWith(PAGE_ASSET_PREFIX));
      const coreBundleEntries = bundleEntries.filter(([relativePath]) => !relativePath.startsWith(PAGE_ASSET_PREFIX));

      const versionExists = await ReactNativeBlobUtil.fs.exists(OFFLINE_BUNDLE_VERSION_PATH);
      const currentVersion = versionExists
        ? await ReactNativeBlobUtil.fs.readFile(OFFLINE_BUNDLE_VERSION_PATH, "utf8")
        : "";
      const needsRefresh = currentVersion.trim() !== OFFLINE_WEB_BUNDLE_VERSION;

      if (needsRefresh) {
        const bundleDirExists = await ReactNativeBlobUtil.fs.exists(OFFLINE_BUNDLE_DIR);
        if (bundleDirExists) {
          await ReactNativeBlobUtil.fs.unlink(OFFLINE_BUNDLE_DIR);
        }
        await ReactNativeBlobUtil.fs.mkdir(OFFLINE_BUNDLE_DIR);

        for (let start = 0; start < coreBundleEntries.length; start += OFFLINE_BUNDLE_BATCH_SIZE) {
          const chunk = coreBundleEntries.slice(start, start + OFFLINE_BUNDLE_BATCH_SIZE);
          const chunkAssets = chunk.map(([, moduleId]) => Asset.fromModule(moduleId));

          await Promise.all(
            chunkAssets.map(async (asset) => {
              if (!asset.localUri && typeof asset.downloadAsync === "function") {
                await asset.downloadAsync();
              }
            }),
          );

          for (let index = 0; index < chunk.length; index += 1) {
            const [relativePath] = chunk[index];
            const asset = chunkAssets[index];
            const sourceUri = asset.localUri || asset.uri;
            if (!sourceUri) {
              throw new Error(`No encontramos el recurso offline ${relativePath}.`);
            }

            const destinationPath = `${OFFLINE_BUNDLE_DIR}/${relativePath}`;
            await ensureParentDirectory(destinationPath);
            await ReactNativeBlobUtil.fs.cp(sourceUri.replace(/^file:\/\//, ""), destinationPath);
          }
        }

        await ReactNativeBlobUtil.fs.writeFile(
          OFFLINE_BUNDLE_VERSION_PATH,
          OFFLINE_WEB_BUNDLE_VERSION,
          "utf8",
        );
      }

      const offlinePages = Object.fromEntries(
        pageBundleEntries.map(([relativePath, moduleId]) => {
          const asset = Asset.fromModule(moduleId);
          const sourceUri = asset.localUri || asset.uri;
          if (!sourceUri) {
            throw new Error(`No encontramos el recurso offline ${relativePath}.`);
          }

          const pageNumber = Number.parseInt(
            relativePath.replace(PAGE_ASSET_PREFIX, "").replace(/^page-/, "").replace(/\.jpg$/, ""),
            10,
          );

          return [pageNumber, sourceUri];
        }),
      );

      return {
        readerUri: `file://${OFFLINE_BUNDLE_DIR}/index.html`,
        bootScript: [
          "window.__SIGNO_VINO_NATIVE_FILE_MODE = true;",
          `window.OFFLINE_PAGES = ${JSON.stringify(offlinePages)};`,
          "window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent({ type: 'bridge-state', available: false, role: 'off' });",
          "true;",
        ].join(" "),
      };
    };

    const loadOfflineReader = async () => {
      try {
        setErrorMessage("");
        setReaderUri(null);
        const { readerUri: nextUri, bootScript } = await buildOfflineBundle();

        if (!cancelled) {
          if (!nextUri) {
            throw new Error("No encontramos el archivo del lector offline.");
          }
          setOfflinePagesScript(bootScript);
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

  const readAccessUrl = useMemo(() => {
    if (!readerUri?.startsWith("file://")) return undefined;
    return readerUri.replace(/[^/]+$/, "");
  }, [readerUri]);

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
          injectedJavaScriptBeforeContentLoaded={offlinePagesScript}
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
