import AsyncStorage from "@react-native-async-storage/async-storage";
import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  AppState,
  KeyboardAvoidingView,
  Modal,
  Platform,
  Pressable,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import Pdf, { type PdfRef } from "react-native-pdf";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import {
  addNearbyDirectorSyncListener,
  isNearbyDirectorSyncAvailable,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
  stopNearbyDirectorSync,
} from "./src/nearbyDirectorSync";

const ALVERNIA_PDF_ASSET = require("./assets/alvernia_manual_2.pdf");
const DIRECTOR_SYNC_STORAGE_KEY = "@alvernia-reader/director-sync";
const DIRECTOR_LONG_PRESS_MS = 1400;

type SyncRole = "off" | "director" | "follower";

const clampPage = (value: number, totalPages: number) => {
  const safeTotalPages = Math.max(1, totalPages || 1);
  if (!Number.isFinite(value)) return 1;
  return Math.min(Math.max(Math.trunc(value), 1), safeTotalPages);
};

const createDefaultSyncSessionCode = () => `CORO${Math.floor(1000 + Math.random() * 9000)}`;

const normalizeSyncSessionCode = (value = "") => value
  .toUpperCase()
  .replace(/[^A-Z0-9]/g, "")
  .slice(0, 12);

const PdfReaderApp = () => {
  const pdfRef = useRef<PdfRef | null>(null);

  const [activePage, setActivePage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");
  const [hintMessage, setHintMessage] = useState("");
  const [isPageJumpModalVisible, setIsPageJumpModalVisible] = useState(false);
  const [pageJumpValue, setPageJumpValue] = useState("");
  const [isSyncModalVisible, setIsSyncModalVisible] = useState(false);
  const [syncRole, setSyncRole] = useState<SyncRole>("off");
  const [syncSessionCode, setSyncSessionCode] = useState(createDefaultSyncSessionCode());
  const [syncStatusMessage, setSyncStatusMessage] = useState("");
  const [syncErrorMessage, setSyncErrorMessage] = useState("");
  const [isSyncBusy, setIsSyncBusy] = useState(false);
  const [isAppActive, setIsAppActive] = useState(true);

  const activePageRef = useRef(activePage);
  const syncSessionCodeRef = useRef(syncSessionCode);
  const usesNearbyDirectorSync = Platform.OS === "ios" && isNearbyDirectorSyncAvailable();

  useEffect(() => {
    activePageRef.current = activePage;
  }, [activePage]);

  useEffect(() => {
    syncSessionCodeRef.current = syncSessionCode;
  }, [syncSessionCode]);

  useEffect(() => {
    let isMounted = true;

    const loadPersistedSyncState = async () => {
      try {
        const rawValue = await AsyncStorage.getItem(DIRECTOR_SYNC_STORAGE_KEY);
        if (!rawValue || !isMounted) return;
        const parsed = JSON.parse(rawValue);
        const nextRole = parsed?.role === "director" || parsed?.role === "follower"
          ? parsed.role
          : "off";
        const nextSessionCode = normalizeSyncSessionCode(parsed?.sessionCode || "");

        if (nextRole !== "off") {
          setSyncRole(nextRole);
        }
        if (nextSessionCode) {
          setSyncSessionCode(nextSessionCode);
        }
      } catch {}
    };

    loadPersistedSyncState();

    return () => {
      isMounted = false;
    };
  }, []);

  useEffect(() => {
    AsyncStorage.setItem(DIRECTOR_SYNC_STORAGE_KEY, JSON.stringify({
      role: syncRole,
      sessionCode: syncSessionCode,
    })).catch(() => {});
  }, [syncRole, syncSessionCode]);

  useEffect(() => {
    const subscription = AppState.addEventListener("change", (nextState) => {
      setIsAppActive(nextState === "active");
    });
    return () => {
      subscription.remove();
    };
  }, []);

  const goToPage = useCallback((nextPage: number) => {
    const clampedPage = clampPage(nextPage, totalPages);
    setActivePage(clampedPage);
    setErrorMessage("");
    requestAnimationFrame(() => {
      pdfRef.current?.setPage(clampedPage);
    });
  }, [totalPages]);

  const openSyncModal = useCallback(() => {
    setSyncErrorMessage("");
    setIsSyncModalVisible(true);
  }, []);

  const openPageJumpModal = useCallback(() => {
    setPageJumpValue(String(activePageRef.current || 1));
    setIsPageJumpModalVisible(true);
  }, []);

  const closePageJumpModal = useCallback(() => {
    setIsPageJumpModalVisible(false);
  }, []);

  const appendPageJumpDigit = useCallback((digit: string) => {
    setPageJumpValue((currentValue) => {
      const nextValue = `${currentValue}${digit}`.replace(/\D/g, "").slice(0, 4);
      return nextValue.replace(/^0+(?=\d)/, "");
    });
  }, []);

  const backspacePageJumpDigit = useCallback(() => {
    setPageJumpValue((currentValue) => currentValue.slice(0, -1));
  }, []);

  const submitPageJump = useCallback(() => {
    const parsedPage = Number.parseInt(pageJumpValue, 10);
    const targetPage = clampPage(parsedPage, totalPages);

    if (!Number.isFinite(parsedPage) || parsedPage < 1) {
      setHintMessage("Escribe un número de página válido.");
      return;
    }

    goToPage(targetPage);
    setHintMessage(
      targetPage === parsedPage
        ? `Saltaste a la página ${targetPage}.`
        : `La página ${parsedPage} no existe. Fui a la ${targetPage}.`,
    );
    setIsPageJumpModalVisible(false);
  }, [goToPage, pageJumpValue, totalPages]);

  const closeSyncModal = useCallback(() => {
    setIsSyncModalVisible(false);
  }, []);

  const normalizeSyncCodeInput = useCallback((value: string) => {
    setSyncSessionCode(normalizeSyncSessionCode(value));
  }, []);

  useEffect(() => {
    if (!usesNearbyDirectorSync) return;

    const subscription = addNearbyDirectorSyncListener((event) => {
      if (event?.type === "page" && typeof event.page === "number") {
        goToPage(event.page);
        setHintMessage(`Director cambió a la página ${event.page}.`);
        setSyncErrorMessage("");
        return;
      }

      if (event?.type === "error") {
        const nextMessage = String(event.message || "La sincronización offline falló.");
        setSyncErrorMessage(nextMessage);
        if (event.code === "DIRECTOR_CONFLICT") {
          setSyncRole("off");
          setSyncStatusMessage("");
          setHintMessage("Había dos directores cerca. Esta iPad salió del modo director.");
        }
        return;
      }

      if (event?.type === "state") {
        const status = String(event.status || "");
        const sessionLabel = String(event.sessionCode || syncSessionCodeRef.current || "");
        const peerCount = Number(event.peerCount || 0);
        const directorCount = Number(event.directorCount || 0);
        const detail = String(event.message || "").trim();

        if (status === "connected") {
          setSyncErrorMessage("");
          if (event.role === "director") {
            setSyncStatusMessage(
              peerCount > 0
                ? `Director conectado con ${peerCount} iPad${peerCount === 1 ? "" : "s"}.`
                : `Director listo en ${sessionLabel}.`,
            );
          } else {
            setSyncStatusMessage(`Siguiendo al director en ${sessionLabel}.`);
          }
        } else if (status === "connecting") {
          setSyncStatusMessage("Conectando con iPads cercanas...");
        } else if (status === "searching") {
          setSyncStatusMessage(`Buscando director cerca de ${sessionLabel}...`);
        } else if (status === "advertising") {
          setSyncStatusMessage(`Director visible sin internet en ${sessionLabel}.`);
        } else if (status === "waiting-followers") {
          setSyncStatusMessage(`Director listo en ${sessionLabel}. Esperando seguidoras...`);
        } else if (status === "resolving-conflict") {
          setSyncStatusMessage(`Veo ${directorCount} directores cerca. Resolviendo conflicto...`);
        } else if (status === "idle") {
          setSyncStatusMessage("");
        }

        if (detail) {
          setHintMessage(detail);
        }
      }
    });

    return () => {
      subscription.remove();
    };
  }, [goToPage, usesNearbyDirectorSync]);

  const enableDirectorMode = useCallback(async () => {
    if (isSyncBusy) return;

    const nextSessionCode = normalizeSyncSessionCode(syncSessionCode) || createDefaultSyncSessionCode();
    setIsSyncBusy(true);
    setSyncSessionCode(nextSessionCode);

    try {
      if (!usesNearbyDirectorSync) {
        setSyncErrorMessage("La sincronización offline solo está disponible en iPad.");
        return;
      }

      await startNearbyDirector(nextSessionCode);
      setSyncRole("director");
      setSyncStatusMessage(`Director offline listo en ${nextSessionCode}.`);
      setSyncErrorMessage("");
      setHintMessage("Modo director offline activado. Funciona sin internet con iPads cercanas.");
      setIsSyncModalVisible(false);
    } catch (error) {
      setSyncErrorMessage(
        error instanceof Error ? error.message : "No se pudo activar el modo director.",
      );
    } finally {
      setIsSyncBusy(false);
    }
  }, [isSyncBusy, syncSessionCode, usesNearbyDirectorSync]);

  const enableFollowerMode = useCallback(async () => {
    if (isSyncBusy) return;

    const nextSessionCode = normalizeSyncSessionCode(syncSessionCode);
    if (!nextSessionCode) {
      setSyncErrorMessage("Ingresa un código de sesión.");
      return;
    }

    setIsSyncBusy(true);
    try {
      if (!usesNearbyDirectorSync) {
        setSyncErrorMessage("La sincronización offline solo está disponible en iPad.");
        return;
      }

      await startNearbyFollower(nextSessionCode);
      setSyncSessionCode(nextSessionCode);
      setSyncRole("follower");
      setSyncStatusMessage(`Buscando director cerca de ${nextSessionCode}...`);
      setSyncErrorMessage("");
      setHintMessage("Modo seguidor offline activado. Esta iPad buscará un director cercano sin internet.");
      setIsSyncModalVisible(false);
    } catch (error) {
      setSyncErrorMessage(
        error instanceof Error ? error.message : "No se pudo seguir la sesión.",
      );
    } finally {
      setIsSyncBusy(false);
    }
  }, [isSyncBusy, syncSessionCode, usesNearbyDirectorSync]);

  const disableSyncMode = useCallback(async () => {
    if (isSyncBusy) return;

    setIsSyncBusy(true);
    setSyncRole("off");
    setSyncStatusMessage("");
    setSyncErrorMessage("");
    setHintMessage("Sincronización desactivada.");

    try {
      await stopNearbyDirectorSync();
    } finally {
      setIsSyncModalVisible(false);
      setIsSyncBusy(false);
    }
  }, [isSyncBusy]);

  useEffect(() => {
    if (!usesNearbyDirectorSync || syncRole !== "director" || isLoading || errorMessage || !isAppActive) {
      return;
    }

    sendNearbyDirectorPageUpdate(activePage, totalPages).catch((error) => {
      setSyncErrorMessage(
        error instanceof Error
          ? error.message
          : "No se pudo enviar la página al resto del coro.",
      );
    });
  }, [activePage, errorMessage, isAppActive, isLoading, syncRole, totalPages, usesNearbyDirectorSync]);

  useEffect(() => () => {
    stopNearbyDirectorSync().catch(() => {});
  }, []);

  return (
    <GestureHandlerRootView style={styles.root}>
      <StatusBar animated hidden />

      <View style={styles.viewerLayer}>
        <Pdf
          ref={pdfRef}
          enableAnnotationRendering
          enablePaging
          fitPolicy={2}
          horizontal
          maxScale={4}
          minScale={1}
          onError={(error) => {
            setIsLoading(false);
            setErrorMessage(String(error || "No se pudo mostrar el PDF"));
          }}
          onLoadComplete={(numberOfPages) => {
            const safeTotalPages = numberOfPages || 0;
            const clampedPage = clampPage(activePageRef.current, safeTotalPages);

            setIsLoading(false);
            setTotalPages(safeTotalPages);
            setErrorMessage("");
            setHintMessage("");

            if (clampedPage !== activePageRef.current) {
              setActivePage(clampedPage);
              requestAnimationFrame(() => {
                pdfRef.current?.setPage(clampedPage);
              });
            }
          }}
          onPageChanged={(page, numberOfPages) => {
            setActivePage(page);
            setTotalPages(numberOfPages || 0);
          }}
          page={activePage}
          renderActivityIndicator={() => (
            <View style={styles.loadingIndicator}>
              <ActivityIndicator color="#ffffff" size="small" />
            </View>
          )}
          source={ALVERNIA_PDF_ASSET}
          spacing={0}
          style={styles.viewer}
        />
      </View>

      {isLoading ? (
        <View style={styles.loadingOverlay}>
          <ActivityIndicator color="#ffffff" size="small" />
        </View>
      ) : null}

      <Pressable
        accessibilityHint="Mantén presionado para abrir el modo oculto de sincronización"
        accessibilityLabel="Página actual"
        delayLongPress={DIRECTOR_LONG_PRESS_MS}
        onPress={openPageJumpModal}
        onLongPress={openSyncModal}
        style={styles.pageBadge}
      >
        <Text style={styles.pageBadgeText}>
          {activePage}
          {totalPages > 0 ? ` / ${totalPages}` : ""}
        </Text>
      </Pressable>

      {errorMessage ? (
        <View style={styles.errorPill}>
          <Text style={styles.errorText}>{errorMessage}</Text>
        </View>
      ) : null}

      {!errorMessage && hintMessage ? (
        <View style={styles.hintPill}>
          <Text style={styles.hintText}>{hintMessage}</Text>
        </View>
      ) : null}

      {!errorMessage && !hintMessage && syncRole !== "off" && syncStatusMessage ? (
        <View style={styles.syncPill}>
          <Text style={styles.syncText}>{syncStatusMessage}</Text>
        </View>
      ) : null}

      {!errorMessage && syncErrorMessage ? (
        <View style={styles.syncErrorPill}>
          <Text style={styles.errorText}>{syncErrorMessage}</Text>
        </View>
      ) : null}

      <Modal
        animationType="fade"
        onRequestClose={closePageJumpModal}
        transparent
        visible={isPageJumpModalVisible}
      >
        <View style={styles.modalBackdrop}>
          <View style={styles.modalCard}>
            <Text style={styles.modalTitle}>Ir a página</Text>
            <Text style={styles.syncDescription}>
              Toca el contador de página para abrir este teclado rápido. Mantén presionado ese mismo botón para Director.
            </Text>
            <Pressable onPress={backspacePageJumpDigit} style={styles.pageJumpDisplay}>
              <Text style={styles.pageJumpValue}>
                {pageJumpValue || "—"}
              </Text>
              <Text style={styles.pageJumpMeta}>
                {totalPages > 0 ? `de ${totalPages}` : "sin total todavía"}
              </Text>
            </Pressable>
            <View style={styles.numpadGrid}>
              {["1", "2", "3", "4", "5", "6", "7", "8", "9", "0"].map((digit) => (
                <Pressable
                  key={digit}
                  onPress={() => appendPageJumpDigit(digit)}
                  style={[
                    styles.numpadButton,
                    digit === "0" && styles.numpadZeroButton,
                  ]}
                >
                  <Text style={styles.numpadButtonText}>{digit}</Text>
                </Pressable>
              ))}
              <Pressable onPress={backspacePageJumpDigit} style={styles.numpadUtilityButton}>
                <Text style={styles.numpadUtilityText}>Borrar</Text>
              </Pressable>
              <Pressable onPress={submitPageJump} style={styles.numpadConfirmButton}>
                <Text style={styles.numpadConfirmText}>Ir</Text>
              </Pressable>
            </View>
            <Pressable onPress={closePageJumpModal} style={styles.syncCloseButton}>
              <Text style={styles.syncCloseText}>Cerrar</Text>
            </Pressable>
          </View>
        </View>
      </Modal>

      <Modal
        animationType="fade"
        onRequestClose={closeSyncModal}
        transparent
        visible={isSyncModalVisible}
      >
        <View style={styles.modalBackdrop}>
          <KeyboardAvoidingView
            behavior={Platform.OS === "ios" ? "padding" : undefined}
            style={styles.modalCard}
          >
            <Text style={styles.modalTitle}>Sincronizar iPads</Text>
            <Text style={styles.syncDescription}>
              Este lector solo muestra el PDF. Mantén presionado el contador de página para abrir este modo oculto.
            </Text>
            <TextInput
              autoCapitalize="characters"
              autoCorrect={false}
              onChangeText={normalizeSyncCodeInput}
              placeholder="Código de sesión"
              placeholderTextColor="#7a8daa"
              style={styles.modalInput}
              value={syncSessionCode}
            />
            <View style={styles.syncActionColumn}>
              <Pressable
                disabled={isSyncBusy}
                onPress={enableDirectorMode}
                style={[styles.modalConfirmButton, isSyncBusy && styles.disabledButton]}
              >
                <Text style={styles.modalConfirmText}>Entrar como director</Text>
              </Pressable>
              <Pressable
                disabled={isSyncBusy}
                onPress={enableFollowerMode}
                style={[styles.syncFollowerButton, isSyncBusy && styles.disabledButton]}
              >
                <Text style={styles.syncFollowerText}>Seguir director</Text>
              </Pressable>
              <Pressable
                disabled={isSyncBusy}
                onPress={disableSyncMode}
                style={[styles.modalCancelButton, isSyncBusy && styles.disabledButton]}
              >
                <Text style={styles.modalCancelText}>Apagar sincronización</Text>
              </Pressable>
            </View>
            <Pressable onPress={closeSyncModal} style={styles.syncCloseButton}>
              <Text style={styles.syncCloseText}>Cerrar</Text>
            </Pressable>
          </KeyboardAvoidingView>
        </View>
      </Modal>
    </GestureHandlerRootView>
  );
};

const styles = StyleSheet.create({
  root: {
    backgroundColor: "#000000",
    flex: 1,
  },
  viewerLayer: {
    flex: 1,
  },
  viewer: {
    backgroundColor: "#000000",
    flex: 1,
  },
  loadingIndicator: {
    alignItems: "center",
    justifyContent: "center",
  },
  loadingOverlay: {
    alignItems: "center",
    justifyContent: "center",
    left: 0,
    position: "absolute",
    right: 0,
    top: 42,
    zIndex: 2,
  },
  pageBadge: {
    backgroundColor: "rgba(18, 28, 44, 0.82)",
    borderRadius: 999,
    bottom: 16,
    left: 16,
    paddingHorizontal: 12,
    paddingVertical: 8,
    position: "absolute",
    zIndex: 6,
  },
  pageBadgeText: {
    color: "#ffffff",
    fontSize: 14,
    fontWeight: "700",
  },
  errorPill: {
    backgroundColor: "rgba(155, 21, 55, 0.92)",
    borderRadius: 10,
    bottom: 66,
    left: 12,
    paddingHorizontal: 10,
    paddingVertical: 8,
    position: "absolute",
    right: 12,
    zIndex: 5,
  },
  errorText: {
    color: "#ffffff",
    fontSize: 13,
    fontWeight: "600",
  },
  hintPill: {
    backgroundColor: "rgba(24, 41, 69, 0.92)",
    borderRadius: 10,
    bottom: 66,
    left: 12,
    paddingHorizontal: 10,
    paddingVertical: 8,
    position: "absolute",
    right: 12,
    zIndex: 4,
  },
  hintText: {
    color: "#ffffff",
    fontSize: 13,
    fontWeight: "500",
  },
  syncPill: {
    backgroundColor: "rgba(18, 90, 54, 0.92)",
    borderRadius: 10,
    bottom: 66,
    left: 12,
    paddingHorizontal: 10,
    paddingVertical: 8,
    position: "absolute",
    right: 12,
    zIndex: 4,
  },
  syncErrorPill: {
    backgroundColor: "rgba(155, 77, 21, 0.92)",
    borderRadius: 10,
    bottom: 108,
    left: 12,
    paddingHorizontal: 10,
    paddingVertical: 8,
    position: "absolute",
    right: 12,
    zIndex: 4,
  },
  syncText: {
    color: "#ffffff",
    fontSize: 13,
    fontWeight: "600",
  },
  modalBackdrop: {
    alignItems: "center",
    backgroundColor: "rgba(0,0,0,0.45)",
    flex: 1,
    justifyContent: "center",
    padding: 20,
  },
  modalCard: {
    alignSelf: "center",
    backgroundColor: "#ffffff",
    borderRadius: 14,
    maxWidth: 360,
    minWidth: 280,
    padding: 16,
    paddingBottom: 22,
    width: "72%",
  },
  modalTitle: {
    color: "#14233a",
    fontSize: 19,
    fontWeight: "700",
    marginBottom: 12,
  },
  syncDescription: {
    color: "#4a5f80",
    fontSize: 14,
    lineHeight: 20,
    marginBottom: 12,
  },
  modalInput: {
    backgroundColor: "#f3f6fb",
    borderColor: "#c8d5ea",
    borderRadius: 10,
    borderWidth: 1,
    color: "#14233a",
    fontSize: 20,
    fontWeight: "700",
    marginBottom: 12,
    paddingHorizontal: 12,
    paddingVertical: 12,
    textAlign: "center",
  },
  syncActionColumn: {
    gap: 10,
    marginTop: 4,
  },
  pageJumpDisplay: {
    alignItems: "center",
    backgroundColor: "#f3f6fb",
    borderColor: "#c8d5ea",
    borderRadius: 12,
    borderWidth: 1,
    marginBottom: 12,
    paddingHorizontal: 12,
    paddingVertical: 14,
  },
  pageJumpValue: {
    color: "#14233a",
    fontSize: 30,
    fontWeight: "800",
  },
  pageJumpMeta: {
    color: "#5e7592",
    fontSize: 13,
    fontWeight: "600",
    marginTop: 4,
  },
  numpadGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
    justifyContent: "center",
    marginTop: 4,
  },
  numpadButton: {
    alignItems: "center",
    backgroundColor: "#edf3fb",
    borderRadius: 12,
    justifyContent: "center",
    minHeight: 58,
    width: "30%",
  },
  numpadZeroButton: {
    width: "30%",
  },
  numpadButtonText: {
    color: "#102038",
    fontSize: 24,
    fontWeight: "800",
  },
  numpadUtilityButton: {
    alignItems: "center",
    backgroundColor: "#e8edf5",
    borderRadius: 12,
    justifyContent: "center",
    minHeight: 58,
    width: "30%",
  },
  numpadUtilityText: {
    color: "#20314f",
    fontSize: 15,
    fontWeight: "700",
  },
  numpadConfirmButton: {
    alignItems: "center",
    backgroundColor: "#0a84ff",
    borderRadius: 12,
    justifyContent: "center",
    minHeight: 58,
    width: "30%",
  },
  numpadConfirmText: {
    color: "#ffffff",
    fontSize: 18,
    fontWeight: "800",
  },
  modalCancelButton: {
    backgroundColor: "#e8edf5",
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  modalCancelText: {
    color: "#20314f",
    fontSize: 15,
    fontWeight: "700",
    textAlign: "center",
  },
  modalConfirmButton: {
    backgroundColor: "#0a84ff",
    borderRadius: 10,
    paddingHorizontal: 18,
    paddingVertical: 10,
  },
  modalConfirmText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "700",
    textAlign: "center",
  },
  syncFollowerButton: {
    backgroundColor: "#123a73",
    borderRadius: 10,
    paddingHorizontal: 18,
    paddingVertical: 10,
  },
  syncFollowerText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "700",
    textAlign: "center",
  },
  disabledButton: {
    opacity: 0.55,
  },
  syncCloseButton: {
    alignSelf: "center",
    marginTop: 14,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  syncCloseText: {
    color: "#57708f",
    fontSize: 14,
    fontWeight: "600",
  },
});

export default PdfReaderApp;
