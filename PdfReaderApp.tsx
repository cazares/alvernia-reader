import { Asset } from "expo-asset";
import { useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Modal,
  Pressable,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import SignoVivoPdfNativeView from "./src/SignoVivoPdfNativeView";

const PDF_MODULE = require("./assets/alvernia_manual_2.pdf");

const getReadableError = (error: unknown) => {
  if (error instanceof Error && error.message) return error.message;
  return "No se pudo preparar el manual offline.";
};

const clampPage = (value: number, totalPages: number) => {
  if (totalPages < 1) return 1;
  return Math.max(1, Math.min(value, totalPages));
};

export default function App() {
  const [pdfUri, setPdfUri] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [reloadKey, setReloadKey] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [jumpDraft, setJumpDraft] = useState("");
  const [isJumpVisible, setIsJumpVisible] = useState(false);

  useEffect(() => {
    let cancelled = false;

    const loadPdf = async () => {
      try {
        setErrorMessage("");
        setPdfUri(null);
        const asset = Asset.fromModule(PDF_MODULE);
        await asset.downloadAsync();
        const nextUri = asset.localUri || asset.uri;
        if (!nextUri) {
          throw new Error("No encontramos el PDF offline.");
        }
        if (!cancelled) {
          setPdfUri(nextUri);
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(getReadableError(error));
        }
      }
    };

    loadPdf();
    return () => {
      cancelled = true;
    };
  }, [reloadKey]);

  const pageLabel = useMemo(() => `${currentPage} / ${totalPages}`, [currentPage, totalPages]);

  const openJumpModal = () => {
    setJumpDraft(String(currentPage));
    setIsJumpVisible(true);
  };

  const applyJump = () => {
    const numericValue = Number.parseInt(jumpDraft, 10);
    if (Number.isNaN(numericValue)) {
      setJumpDraft(String(currentPage));
      return;
    }

    setCurrentPage(clampPage(numericValue, totalPages));
    setIsJumpVisible(false);
  };

  return (
    <View style={styles.screen}>
      <StatusBar hidden barStyle="light-content" />
      {pdfUri ? (
        <>
          <SignoVivoPdfNativeView
            onDocumentLoaded={({ nativeEvent }) => {
              setTotalPages(nativeEvent.totalPages || 1);
              setCurrentPage(clampPage(nativeEvent.page || 1, nativeEvent.totalPages || 1));
            }}
            onError={({ nativeEvent }) => {
              setErrorMessage(nativeEvent.message || "No se pudo abrir el documento.");
            }}
            onPageChange={({ nativeEvent }) => {
              setTotalPages(nativeEvent.totalPages || 1);
              setCurrentPage(clampPage(nativeEvent.page || 1, nativeEvent.totalPages || 1));
            }}
            page={currentPage}
            source={pdfUri}
            style={styles.viewer}
          />
          <View pointerEvents="box-none" style={styles.overlay}>
            <View style={styles.controls}>
              <Pressable
                disabled={currentPage <= 1}
                onPress={() => setCurrentPage((value) => clampPage(value - 1, totalPages))}
                style={({ pressed }) => [
                  styles.navButton,
                  currentPage <= 1 && styles.navButtonDisabled,
                  pressed && currentPage > 1 && styles.navButtonPressed,
                ]}
              >
                <Text style={styles.navButtonText}>Anterior</Text>
              </Pressable>
              <Pressable onPress={openJumpModal} style={({ pressed }) => [styles.pageBadge, pressed && styles.pageBadgePressed]}>
                <Text style={styles.pageBadgeText}>{pageLabel}</Text>
              </Pressable>
              <Pressable
                disabled={currentPage >= totalPages}
                onPress={() => setCurrentPage((value) => clampPage(value + 1, totalPages))}
                style={({ pressed }) => [
                  styles.navButton,
                  currentPage >= totalPages && styles.navButtonDisabled,
                  pressed && currentPage < totalPages && styles.navButtonPressed,
                ]}
              >
                <Text style={styles.navButtonText}>Siguiente</Text>
              </Pressable>
            </View>
          </View>
          <Modal animationType="fade" onRequestClose={() => setIsJumpVisible(false)} transparent visible={isJumpVisible}>
            <View style={styles.modalBackdrop}>
              <View style={styles.modalCard}>
                <Text style={styles.modalTitle}>Ir a pagina</Text>
                <Text style={styles.modalText}>Escribe un numero entre 1 y {totalPages}.</Text>
                <TextInput
                  autoFocus
                  keyboardType="number-pad"
                  onChangeText={(value) => setJumpDraft(value.replace(/[^0-9]/g, ""))}
                  onSubmitEditing={applyJump}
                  selectTextOnFocus
                  style={styles.input}
                  value={jumpDraft}
                />
                <View style={styles.modalActions}>
                  <Pressable onPress={() => setIsJumpVisible(false)} style={({ pressed }) => [styles.modalButtonSecondary, pressed && styles.modalButtonPressed]}>
                    <Text style={styles.modalButtonSecondaryText}>Cancelar</Text>
                  </Pressable>
                  <Pressable onPress={applyJump} style={({ pressed }) => [styles.modalButtonPrimary, pressed && styles.modalButtonPressed]}>
                    <Text style={styles.modalButtonPrimaryText}>Ir</Text>
                  </Pressable>
                </View>
              </View>
            </View>
          </Modal>
        </>
      ) : (
        <View style={styles.loadingShell}>
          <ActivityIndicator color="#93e4ff" size="large" />
          <Text style={styles.loadingTitle}>Preparando SignoVivo</Text>
          <Text style={styles.loadingText}>
            {errorMessage || "Cargando el PDF offline con la ruta mas corta posible."}
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
  viewer: {
    flex: 1,
    backgroundColor: "#000",
  },
  overlay: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: "flex-end",
    paddingHorizontal: 18,
    paddingBottom: 18,
  },
  controls: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  navButton: {
    minWidth: 104,
    borderRadius: 999,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "rgba(7, 11, 18, 0.84)",
    borderWidth: 1,
    borderColor: "rgba(147, 228, 255, 0.24)",
  },
  navButtonDisabled: {
    opacity: 0.42,
  },
  navButtonPressed: {
    backgroundColor: "rgba(22, 46, 70, 0.96)",
  },
  navButtonText: {
    color: "#f5fbff",
    fontSize: 16,
    fontWeight: "700",
    textAlign: "center",
  },
  pageBadge: {
    flex: 1,
    borderRadius: 999,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "rgba(7, 11, 18, 0.88)",
    borderWidth: 1,
    borderColor: "rgba(255, 255, 255, 0.12)",
  },
  pageBadgePressed: {
    backgroundColor: "rgba(24, 33, 46, 0.96)",
  },
  pageBadgeText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
    textAlign: "center",
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
  modalBackdrop: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(0, 0, 0, 0.58)",
    paddingHorizontal: 20,
  },
  modalCard: {
    width: "100%",
    maxWidth: 420,
    borderRadius: 24,
    padding: 22,
    backgroundColor: "#0a1018",
    borderWidth: 1,
    borderColor: "rgba(147, 228, 255, 0.18)",
    gap: 14,
  },
  modalTitle: {
    color: "#ffffff",
    fontSize: 24,
    fontWeight: "700",
  },
  modalText: {
    color: "#dbe4f7",
    fontSize: 15,
    lineHeight: 22,
  },
  input: {
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(147, 228, 255, 0.24)",
    paddingHorizontal: 16,
    paddingVertical: 14,
    color: "#ffffff",
    fontSize: 24,
    fontWeight: "700",
    backgroundColor: "#04070c",
  },
  modalActions: {
    flexDirection: "row",
    justifyContent: "flex-end",
    gap: 12,
  },
  modalButtonSecondary: {
    borderRadius: 999,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "#17202d",
  },
  modalButtonPrimary: {
    borderRadius: 999,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "#1f4aa3",
  },
  modalButtonPressed: {
    opacity: 0.8,
  },
  modalButtonSecondaryText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
  modalButtonPrimaryText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
});
