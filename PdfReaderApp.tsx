import { Asset } from "expo-asset";
import {
  cacheDirectory,
  copyAsync,
  deleteAsync,
  documentDirectory,
  getInfoAsync,
  makeDirectoryAsync,
} from "expo-file-system/legacy";
import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, StatusBar, StyleSheet, Text, View } from "react-native";
import { WebView } from "react-native-webview";

import { OFFLINE_WEB_BUNDLE_ASSETS, OFFLINE_WEB_BUNDLE_VERSION } from "./src/offlineWebBundle";

const OFFLINE_READER_ENTRY = "index.html";
const OFFLINE_BUNDLE_PARENT = "signovivo-offline";

const getReadableError = (error: unknown) => {
  if (error instanceof Error && error.message) return error.message;
  return "No se pudo preparar Signo Vino offline.";
};

const ensureParentDirectory = async (fileUri: string) => {
  const directoryUri = fileUri.replace(/[^/]+$/, "");
  await makeDirectoryAsync(directoryUri, { intermediates: true });
};

const getBundleRootUri = () => {
  const baseDirectory = documentDirectory || cacheDirectory;
  if (!baseDirectory) {
    throw new Error("No encontramos almacenamiento local para el modo offline.");
  }

  return `${baseDirectory}${OFFLINE_BUNDLE_PARENT}/${OFFLINE_WEB_BUNDLE_VERSION}/`;
};

const stageOfflineBundle = async () => {
  const bundleRootUri = getBundleRootUri();
  const entryUri = `${bundleRootUri}${OFFLINE_READER_ENTRY}`;
  const existingEntry = await getInfoAsync(entryUri);
  if (existingEntry.exists) return entryUri;

  await deleteAsync(bundleRootUri, { idempotent: true }).catch(() => {});
  await makeDirectoryAsync(bundleRootUri, { intermediates: true });

  const entries = Object.entries(OFFLINE_WEB_BUNDLE_ASSETS);
  const batchSize = 12;

  for (let index = 0; index < entries.length; index += batchSize) {
    const batch = entries.slice(index, index + batchSize);
    await Promise.all(
      batch.map(async ([relativePath, moduleId]) => {
        const asset = Asset.fromModule(moduleId);
        await asset.downloadAsync();
        const sourceUri = asset.localUri || asset.uri;
        if (!sourceUri) {
          throw new Error(`No pudimos preparar el archivo offline: ${relativePath}`);
        }

        const destinationUri = `${bundleRootUri}${relativePath}`;
        await ensureParentDirectory(destinationUri);
        await copyAsync({
          from: sourceUri,
          to: destinationUri,
        });
      }),
    );
  }

  return entryUri;
};

export default function App() {
  const [readerUri, setReaderUri] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let cancelled = false;

    const loadReader = async () => {
      try {
        setErrorMessage("");
        setReaderUri(null);
        const nextUri = await stageOfflineBundle();
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

  const readAccessUrl = useMemo(() => {
    if (!readerUri?.startsWith("file://")) return undefined;
    return readerUri.replace(/[^/]+$/, "");
  }, [readerUri]);

  return (
    <View style={styles.screen}>
      <StatusBar hidden barStyle="light-content" />
      {readerUri ? (
        <WebView
          allowingReadAccessToURL={readAccessUrl}
          allowFileAccess
          allowFileAccessFromFileURLs
          allowUniversalAccessFromFileURLs
          allowsBackForwardNavigationGestures={false}
          allowsInlineMediaPlayback
          bounces={false}
          domStorageEnabled
          injectedJavaScriptBeforeContentLoaded={"window.__SIGNO_VINO_NATIVE_FILE_MODE = true; true;"}
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
            {errorMessage || "Copiando la experiencia completa de signovivo.com para que funcione 100% offline."}
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
