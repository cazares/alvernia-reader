import AsyncStorage from "@react-native-async-storage/async-storage";
import Constants from "expo-constants";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { ALVERNIA_MANUAL_2_SONG_INDEX } from "./src/alverniaManual2SongIndex";
import {
  DEFAULT_DIRECTOR_HEARTBEAT_MS,
  createDefaultSyncSessionCode,
  createDirectorKey,
  normalizeSyncSessionCode,
  publishDirectorSyncState,
  releaseDirectorSyncState,
  readDirectorSyncState,
  resolveDirectorSyncEndpoint,
} from "./src/directorSync";
import {
  addNearbyDirectorSyncListener,
  isNearbyDirectorSyncAvailable,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
  stopNearbyDirectorSync,
} from "./src/nearbyDirectorSync";
import { clampPdfPage } from "./src/pdfReaderUrl";
import { findSongEntryOrNext } from "./src/songNavigation";

const ALVERNIA_PDF_ASSET = require("./assets/alvernia_manual_2.pdf");
const UNKNOWN_PAGE_MAX = 10000;
const DIRECTOR_SYNC_STORAGE_KEY = "@alvernia-reader/director-sync";
const DIRECTOR_SYNC_POLL_MS = 1500;
const DIRECTOR_LONG_PRESS_MS = 1400;
const MAX_SYNC_FAILURES_BEFORE_BACKOFF = 3;

type SyncRole = "off" | "director" | "follower";

type SongEntry = {
  page: number;
  song: number;
};

const PdfReaderApp = () => {
  const pdfRef = useRef<PdfRef | null>(null);
  const modalInputRef = useRef<TextInput | null>(null);

  const [activePage, setActivePage] = useState(1);
  const [totalPages, setTotalPages] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");
  const [hintMessage, setHintMessage] = useState("");
  const [isGoModalVisible, setIsGoModalVisible] = useState(false);
  const [isSyncModalVisible, setIsSyncModalVisible] = useState(false);
  const [modalInput, setModalInput] = useState("1");
  const [syncRole, setSyncRole] = useState<SyncRole>("off");
  const [syncSessionCode, setSyncSessionCode] = useState(createDefaultSyncSessionCode());
  const [syncDirectorKey, setSyncDirectorKey] = useState(createDirectorKey());
  const [syncStatusMessage, setSyncStatusMessage] = useState("");
  const [syncErrorMessage, setSyncErrorMessage] = useState("");
  const [isSyncBusy, setIsSyncBusy] = useState(false);
  const [isAppActive, setIsAppActive] = useState(true);

  const syncPollTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const syncHeartbeatTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const syncFailureCountRef = useRef(0);
  const syncRoleRef = useRef<SyncRole>("off");
  const syncSessionCodeRef = useRef(syncSessionCode);
  const syncDirectorKeyRef = useRef(syncDirectorKey);
  const activePageRef = useRef(activePage);
  const syncEndpoint = useMemo(
    () => resolveDirectorSyncEndpoint(Constants.expoConfig?.extra),
    [],
  );
  const usesNearbyDirectorSync = useMemo(
    () => Platform.OS === "ios" && isNearbyDirectorSyncAvailable(),
    [],
  );

  const songEntries = ALVERNIA_MANUAL_2_SONG_INDEX as readonly SongEntry[];

  useEffect(() => {
    activePageRef.current = activePage;
  }, [activePage]);

  useEffect(() => {
    syncRoleRef.current = syncRole;
    syncSessionCodeRef.current = syncSessionCode;
    syncDirectorKeyRef.current = syncDirectorKey;
  }, [syncDirectorKey, syncRole, syncSessionCode]);

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
        const nextDirectorKey = String(parsed?.directorKey || "").trim();

        if (nextRole !== "off") {
          setSyncRole(nextRole);
        }
        if (nextSessionCode) {
          setSyncSessionCode(nextSessionCode);
        }
        if (nextDirectorKey) {
          setSyncDirectorKey(nextDirectorKey);
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
      directorKey: syncDirectorKey,
    })).catch(() => {});
  }, [syncDirectorKey, syncRole, syncSessionCode]);

  useEffect(() => {
    const subscription = AppState.addEventListener("change", (nextState) => {
      setIsAppActive(nextState === "active");
    });
    return () => {
      subscription.remove();
    };
  }, []);

  useEffect(() => {
    if (!isGoModalVisible) return;
    const focusTimeout = setTimeout(() => {
      modalInputRef.current?.focus();
    }, 60);
    return () => {
      clearTimeout(focusTimeout);
    };
  }, [isGoModalVisible]);

  const goToPage = useCallback((value: number | string) => {
    const maxPage = totalPages > 0 ? totalPages : UNKNOWN_PAGE_MAX;
    const nextPage = clampPdfPage(value, 1, maxPage);
    setActivePage(nextPage);
    setModalInput(String(nextPage));
    setErrorMessage("");
    setHintMessage("");
    requestAnimationFrame(() => {
      pdfRef.current?.setPage(nextPage);
    });
  }, [totalPages]);

  const goToSong = useCallback((value: number | string) => {
    const rawInput = String(value || "").trim();
    const parsedInput = Number.parseInt(rawInput || "1", 10);
    if (Number.isFinite(parsedInput) && parsedInput <= 0) {
      goToPage(1);
      setModalInput("0");
      setErrorMessage("");
      setHintMessage("Mostrando la introduccion antes de la cancion 1.");
      return;
    }

    const requestedSong = clampPdfPage(parsedInput, 1, UNKNOWN_PAGE_MAX);
    const targetEntry = findSongEntryOrNext(songEntries, requestedSong);
    const exact = targetEntry?.song === requestedSong ? targetEntry : null;
    const maxPage = totalPages > 0 ? totalPages : UNKNOWN_PAGE_MAX;
    const targetPage = targetEntry
      ? targetEntry.page
      : clampPdfPage(requestedSong, 1, maxPage);

    setActivePage(targetPage);
    setModalInput(String(requestedSong));
    setErrorMessage("");

    if (!exact && targetEntry) {
      setHintMessage(`La cancion ${requestedSong} no existe. Saltamos a la cancion ${targetEntry.song}.`);
    } else if (!targetEntry) {
      setHintMessage("Indice de canciones no disponible. Usando numero de pagina.");
    } else {
      setHintMessage("");
    }

    requestAnimationFrame(() => {
      pdfRef.current?.setPage(targetPage);
    });
  }, [goToPage, songEntries, totalPages]);

  const openGoModal = useCallback(() => {
    setModalInput("");
    setIsGoModalVisible(true);
  }, []);

  const openSyncModal = useCallback(() => {
    setSyncErrorMessage("");
    setIsSyncModalVisible(true);
  }, []);

  const closeGoModal = useCallback(() => {
    setIsGoModalVisible(false);
  }, []);

  const closeSyncModal = useCallback(() => {
    setIsSyncModalVisible(false);
  }, []);

  const confirmGoModal = useCallback(() => {
    const trimmedValue = String(modalInput || "").trim();
    if (!trimmedValue) {
      setHintMessage("Ingresa un numero de cancion.");
      return;
    }
    goToSong(trimmedValue);
    setIsGoModalVisible(false);
  }, [goToSong, modalInput]);

  const normalizeSongInput = useCallback((value: string) => {
    setModalInput(value.replace(/\D+/g, "").slice(0, 4));
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

  const getFriendlySyncErrorMessage = useCallback((error: unknown, fallbackMessage: string) => {
    if (!(error instanceof Error)) return fallbackMessage;
    if ("code" in error && error.code === "DIRECTOR_CONFLICT") {
      return "Ya hay otro director usando ese código. Usa otro o espera a que salga.";
    }
    if ("code" in error && error.code === "DIRECTOR_TIMEOUT") {
      return "La sesión tardó demasiado en responder. Revisa la conexión e intenta otra vez.";
    }
    if ("code" in error && error.code === "DIRECTOR_NETWORK") {
      return "No hay conexión con la sesión del director. Revisa internet o la red local.";
    }
    return error.message || fallbackMessage;
  }, []);

  const publishCurrentDirectorPage = useCallback(async (pageNumber: number) => {
    const sessionCode = normalizeSyncSessionCode(syncSessionCodeRef.current);
    const directorKey = String(syncDirectorKeyRef.current || "").trim();

    if (syncRoleRef.current !== "director" || !sessionCode || !directorKey) {
      return;
    }

    try {
      const payload = await publishDirectorSyncState({
        endpoint: syncEndpoint,
        sessionCode,
        directorKey,
        page: pageNumber,
        totalPages,
      });
      syncFailureCountRef.current = 0;
      setSyncStatusMessage(`Director conectado en ${payload.session}.`);
      setSyncErrorMessage("");
    } catch (error) {
      syncFailureCountRef.current += 1;
      const message = getFriendlySyncErrorMessage(error, "No se pudo sincronizar.");
      setSyncErrorMessage(message);
      if (error instanceof Error && "code" in error && error.code === "DIRECTOR_CONFLICT") {
        setSyncRole("off");
        setSyncStatusMessage("");
        setHintMessage("Otro director tomó esa sesión. Elige otro código o espera.");
      }
    }
  }, [getFriendlySyncErrorMessage, syncEndpoint, totalPages]);

  const enableDirectorMode = useCallback(async () => {
    if (isSyncBusy) return;
    const nextSessionCode = normalizeSyncSessionCode(syncSessionCode) || createDefaultSyncSessionCode();
    const nextDirectorKey = String(syncDirectorKey || "").trim() || createDirectorKey();

    setIsSyncBusy(true);
    setSyncSessionCode(nextSessionCode);
    setSyncDirectorKey(nextDirectorKey);

    try {
      if (usesNearbyDirectorSync) {
        await startNearbyDirector(nextSessionCode);
        setSyncRole("director");
        setSyncStatusMessage(`Director offline listo en ${nextSessionCode}.`);
        setSyncErrorMessage("");
        setHintMessage("Modo director offline activado. Funciona sin internet con iPads cercanas.");
        setIsSyncModalVisible(false);
        return;
      }

      const payload = await publishDirectorSyncState({
        endpoint: syncEndpoint,
        sessionCode: nextSessionCode,
        directorKey: nextDirectorKey,
        page: activePageRef.current,
        totalPages,
      });

      setSyncRole("director");
      setSyncStatusMessage(`Modo director activo en ${payload.session}.`);
      setSyncErrorMessage("");
      setHintMessage("Modo director oculto activado. Tus cambios de página se sincronizan.");
      setIsSyncModalVisible(false);
    } catch (error) {
      setSyncErrorMessage(getFriendlySyncErrorMessage(error, "No se pudo activar el modo director."));
    } finally {
      setIsSyncBusy(false);
    }
  }, [getFriendlySyncErrorMessage, isSyncBusy, syncDirectorKey, syncEndpoint, syncSessionCode, totalPages, usesNearbyDirectorSync]);

  const enableFollowerMode = useCallback(async () => {
    if (isSyncBusy) return;
    const nextSessionCode = normalizeSyncSessionCode(syncSessionCode);
    if (!nextSessionCode) {
      setSyncErrorMessage("Ingresa un codigo de sesion.");
      return;
    }

    setIsSyncBusy(true);
    try {
      if (usesNearbyDirectorSync) {
        await startNearbyFollower(nextSessionCode);
        setSyncSessionCode(nextSessionCode);
        setSyncRole("follower");
        setSyncStatusMessage(`Buscando director cerca de ${nextSessionCode}...`);
        setSyncErrorMessage("");
        setHintMessage("Modo seguidor offline activado. Esta iPad buscará un director cercano sin internet.");
        setIsSyncModalVisible(false);
        return;
      }

      const payload = await readDirectorSyncState({
        endpoint: syncEndpoint,
        sessionCode: nextSessionCode,
      });

      setSyncSessionCode(nextSessionCode);
      setSyncRole("follower");
      setSyncStatusMessage(
        payload?.directorPresent
          ? `Siguiendo la sesión ${payload.session}.`
          : `Buscando director en ${nextSessionCode}...`,
      );
      setSyncErrorMessage("");
      setHintMessage(
        payload?.directorPresent
          ? "Modo seguidor activado. Esta iPad seguirá la página del director."
          : "Modo seguidor activado. Esperando a que un director entre a la sesión.",
      );
      setIsSyncModalVisible(false);

      if (payload?.directorPresent && payload?.page) {
        goToPage(payload.page);
      }
    } catch (error) {
      setSyncErrorMessage(getFriendlySyncErrorMessage(error, "No se pudo seguir la sesion."));
    } finally {
      setIsSyncBusy(false);
    }
  }, [getFriendlySyncErrorMessage, goToPage, isSyncBusy, syncEndpoint, syncSessionCode, usesNearbyDirectorSync]);

  const disableSyncMode = useCallback(async () => {
    if (isSyncBusy) return;
    const previousRole = syncRoleRef.current;
    const sessionCode = syncSessionCodeRef.current;
    const directorKey = syncDirectorKeyRef.current;

    setIsSyncBusy(true);
    if (syncHeartbeatTimeoutRef.current) {
      clearTimeout(syncHeartbeatTimeoutRef.current);
      syncHeartbeatTimeoutRef.current = null;
    }
    setSyncRole("off");
    setSyncStatusMessage("");
    setSyncErrorMessage("");
    setHintMessage("Sincronizacion desactivada.");
    if (syncPollTimeoutRef.current) {
      clearTimeout(syncPollTimeoutRef.current);
      syncPollTimeoutRef.current = null;
    }
    if (usesNearbyDirectorSync) {
      await stopNearbyDirectorSync();
    } else if (previousRole === "director") {
      try {
        await releaseDirectorSyncState({
          endpoint: syncEndpoint,
          sessionCode,
          directorKey,
        });
      } catch {
        setHintMessage("El director salió, pero la sesión se cerrará sola en unos segundos si quedó colgada.");
      }
    }
    setIsSyncModalVisible(false);
    setIsSyncBusy(false);
  }, [isSyncBusy, syncEndpoint, usesNearbyDirectorSync]);

  useEffect(() => {
    if (usesNearbyDirectorSync) {
      return;
    }
    if (syncRole !== "director" || isLoading || errorMessage || !isAppActive) {
      if (syncHeartbeatTimeoutRef.current) {
        clearTimeout(syncHeartbeatTimeoutRef.current);
        syncHeartbeatTimeoutRef.current = null;
      }
      return;
    }

    publishCurrentDirectorPage(activePage);
  }, [activePage, errorMessage, isAppActive, isLoading, publishCurrentDirectorPage, syncRole, usesNearbyDirectorSync]);

  useEffect(() => {
    if (usesNearbyDirectorSync) {
      return;
    }
    if (syncRole !== "director" || isLoading || errorMessage || !isAppActive) {
      return;
    }

    let cancelled = false;
    const heartbeat = async () => {
      try {
        await publishCurrentDirectorPage(activePageRef.current);
      } finally {
        if (!cancelled) {
          syncHeartbeatTimeoutRef.current = setTimeout(heartbeat, DEFAULT_DIRECTOR_HEARTBEAT_MS);
        }
      }
    };

    syncHeartbeatTimeoutRef.current = setTimeout(heartbeat, DEFAULT_DIRECTOR_HEARTBEAT_MS);

    return () => {
      cancelled = true;
      if (syncHeartbeatTimeoutRef.current) {
        clearTimeout(syncHeartbeatTimeoutRef.current);
        syncHeartbeatTimeoutRef.current = null;
      }
    };
  }, [errorMessage, isAppActive, isLoading, publishCurrentDirectorPage, syncRole, usesNearbyDirectorSync]);

  useEffect(() => {
    if (usesNearbyDirectorSync) {
      return;
    }
    if (syncRole !== "follower" || !isAppActive) {
      if (syncPollTimeoutRef.current) {
        clearTimeout(syncPollTimeoutRef.current);
        syncPollTimeoutRef.current = null;
      }
      return;
    }

    let cancelled = false;

    const pollDirector = async () => {
      try {
        const payload = await readDirectorSyncState({
          endpoint: syncEndpoint,
          sessionCode: syncSessionCodeRef.current,
        });

        if (cancelled) return;

        if (payload?.directorPresent && payload?.session) {
          setSyncStatusMessage(`Siguiendo la sesión ${payload.session}.`);
        } else {
          setSyncStatusMessage(`Esperando director en ${syncSessionCodeRef.current}...`);
          if (payload?.staleDirectorExpired) {
            setHintMessage("La sesión del director anterior expiró. Esperando un director nuevo.");
          }
        }

        if (payload?.directorPresent && payload?.page && payload.page !== activePageRef.current) {
          goToPage(payload.page);
          setHintMessage(`Director cambió a la página ${payload.page}.`);
        }

        syncFailureCountRef.current = 0;
        setSyncErrorMessage("");
      } catch (error) {
        if (!cancelled) {
          syncFailureCountRef.current += 1;
          setSyncErrorMessage(getFriendlySyncErrorMessage(error, "No se pudo sincronizar."));
          if (syncFailureCountRef.current >= MAX_SYNC_FAILURES_BEFORE_BACKOFF) {
            setSyncStatusMessage(`Reconectando a ${syncSessionCodeRef.current}...`);
            setHintMessage("La conexión está inestable. Seguiremos intentando en segundo plano.");
          }
        }
      } finally {
        if (!cancelled) {
          const nextDelay = syncFailureCountRef.current >= MAX_SYNC_FAILURES_BEFORE_BACKOFF
            ? DEFAULT_DIRECTOR_HEARTBEAT_MS
            : DIRECTOR_SYNC_POLL_MS;
          syncPollTimeoutRef.current = setTimeout(pollDirector, nextDelay);
        }
      }
    };

    pollDirector();

    return () => {
      cancelled = true;
      if (syncPollTimeoutRef.current) {
        clearTimeout(syncPollTimeoutRef.current);
        syncPollTimeoutRef.current = null;
      }
    };
  }, [getFriendlySyncErrorMessage, goToPage, isAppActive, syncEndpoint, syncRole, usesNearbyDirectorSync]);

  useEffect(() => {
    if (!usesNearbyDirectorSync) return;
    if (syncRole !== "director" || isLoading || errorMessage) return;
    sendNearbyDirectorPageUpdate(activePage, totalPages).catch((error) => {
      setSyncErrorMessage(getFriendlySyncErrorMessage(error, "No se pudo enviar la página al resto del coro."));
    });
  }, [activePage, errorMessage, getFriendlySyncErrorMessage, isLoading, syncRole, totalPages, usesNearbyDirectorSync]);

  useEffect(() => () => {
    if (syncHeartbeatTimeoutRef.current) {
      clearTimeout(syncHeartbeatTimeoutRef.current);
    }
    if (syncPollTimeoutRef.current) {
      clearTimeout(syncPollTimeoutRef.current);
    }
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
            const clampedPage = clampPdfPage(activePage, 1, safeTotalPages || 1);

            setIsLoading(false);
            setTotalPages(safeTotalPages);
            setErrorMessage("");
            setHintMessage("");

            if (clampedPage !== activePage) {
              setActivePage(clampedPage);
              requestAnimationFrame(() => {
                pdfRef.current?.setPage(clampedPage);
              });
            }
          }}
          onPageChanged={(page, numberOfPages) => {
            setActivePage(page);
            setTotalPages(numberOfPages || 0);
            if (!isGoModalVisible) {
              setModalInput("");
            }
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

      {!isGoModalVisible ? (
        <Pressable
          accessibilityHint="Abre el cuadro para ir a una canción"
          accessibilityLabel="Ir a canción"
          delayLongPress={DIRECTOR_LONG_PRESS_MS}
          onLongPress={openSyncModal}
          onPress={openGoModal}
          style={styles.jumpButton}
        >
          <Text style={styles.jumpButtonText}>Ir</Text>
        </Pressable>
      ) : null}

      <Modal
        animationType="fade"
        onRequestClose={closeGoModal}
        transparent
        visible={isGoModalVisible}
      >
        <View style={styles.modalBackdrop}>
          <KeyboardAvoidingView
            behavior={Platform.OS === "ios" ? "padding" : undefined}
            style={styles.modalCard}
          >
            <Text style={styles.modalTitle}>Ir a cancion</Text>
            <TextInput
              autoFocus
              blurOnSubmit={false}
              inputMode="numeric"
              keyboardType={Platform.OS === "ios" ? "number-pad" : "numeric"}
              maxLength={4}
              onChangeText={normalizeSongInput}
              onSubmitEditing={confirmGoModal}
              placeholder="Numero de cancion"
              placeholderTextColor="#7a8daa"
              ref={modalInputRef}
              returnKeyType="go"
              style={styles.modalInput}
              value={modalInput}
            />
            <View style={styles.modalButtonRow}>
              <Pressable onPress={closeGoModal} style={styles.modalCancelButton}>
                <Text style={styles.modalCancelText}>Cancelar</Text>
              </Pressable>
              <Pressable onPress={confirmGoModal} style={styles.modalConfirmButton}>
                <Text style={styles.modalConfirmText}>Ir</Text>
              </Pressable>
            </View>
          </KeyboardAvoidingView>
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
              Mantén presionado el botón Ir para abrir este modo oculto. En iPad usa conexión cercana sin internet.
            </Text>
            <TextInput
              autoCapitalize="characters"
              autoCorrect={false}
              onChangeText={normalizeSyncCodeInput}
              placeholder="Codigo de sesion"
              placeholderTextColor="#7a8daa"
              style={styles.modalInput}
              value={syncSessionCode}
            />
            <View style={styles.syncActionColumn}>
              <Pressable disabled={isSyncBusy} onPress={enableDirectorMode} style={[styles.modalConfirmButton, isSyncBusy && styles.disabledButton]}>
                <Text style={styles.modalConfirmText}>Entrar como director</Text>
              </Pressable>
              <Pressable disabled={isSyncBusy} onPress={enableFollowerMode} style={[styles.syncFollowerButton, isSyncBusy && styles.disabledButton]}>
                <Text style={styles.syncFollowerText}>Seguir director</Text>
              </Pressable>
              <Pressable disabled={isSyncBusy} onPress={disableSyncMode} style={[styles.modalCancelButton, isSyncBusy && styles.disabledButton]}>
                <Text style={styles.modalCancelText}>Apagar sincronizacion</Text>
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
  jumpButton: {
    alignItems: "center",
    backgroundColor: "rgba(10, 132, 255, 0.96)",
    borderRadius: 26,
    bottom: 16,
    height: 52,
    justifyContent: "center",
    position: "absolute",
    right: 16,
    width: 52,
    zIndex: 6,
  },
  jumpButtonText: {
    color: "#ffffff",
    fontSize: 17,
    fontWeight: "800",
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
    padding: 16,
    paddingBottom: 22,
    width: "72%",
    maxWidth: 360,
    minWidth: 280,
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
  modalButtonRow: {
    flexDirection: "row",
    gap: 10,
    justifyContent: "flex-end",
  },
  syncActionColumn: {
    gap: 10,
    marginTop: 4,
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
