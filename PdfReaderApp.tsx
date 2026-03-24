import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
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
import { clampPdfPage } from "./src/pdfReaderUrl";
import { findSongEntryOrNext } from "./src/songNavigation";

const ALVERNIA_PDF_ASSET = require("./assets/alvernia_manual_2.pdf");
const UNKNOWN_PAGE_MAX = 10000;

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
  const [modalInput, setModalInput] = useState("1");

  const songEntries = ALVERNIA_MANUAL_2_SONG_INDEX as readonly SongEntry[];

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

  const closeGoModal = useCallback(() => {
    setIsGoModalVisible(false);
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

      {!isGoModalVisible ? (
        <Pressable
          accessibilityHint="Abre el cuadro para ir a una canción"
          accessibilityLabel="Ir a canción"
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
});

export default PdfReaderApp;
