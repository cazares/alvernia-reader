import { useCallback, useMemo, useRef, useState } from "react";
import {
  FlatList,
  Image,
  Modal,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
  useWindowDimensions,
} from "react-native";
import { OFFLINE_WEB_BUNDLE_ASSETS } from "./src/offlineWebBundle";

const PAGE_ASSET_PREFIX = "pages/";

const PAGE_ENTRIES = Object.entries(OFFLINE_WEB_BUNDLE_ASSETS)
  .filter(([relativePath]) => relativePath.startsWith(PAGE_ASSET_PREFIX))
  .map(([relativePath, moduleId]) => ({
    pageNumber: Number.parseInt(
      relativePath.replace(PAGE_ASSET_PREFIX, "").replace(/^page-/, "").replace(/\.jpg$/, ""),
      10,
    ),
    moduleId,
  }))
  .sort((left, right) => left.pageNumber - right.pageNumber);

const TOTAL_PAGES = PAGE_ENTRIES.length;

const clampPage = (value: number) => Math.max(1, Math.min(TOTAL_PAGES, value));

export default function App() {
  const { width, height } = useWindowDimensions();
  const listRef = useRef<FlatList<(typeof PAGE_ENTRIES)[number]>>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [jumpModalVisible, setJumpModalVisible] = useState(false);
  const [jumpDraft, setJumpDraft] = useState("");

  const pageWidth = Math.max(width, 1);
  const pageHeight = Math.max(height - 148, 240);

  const currentEntry = useMemo(() => PAGE_ENTRIES[currentPage - 1] || PAGE_ENTRIES[0], [currentPage]);

  const goToPage = useCallback(
    (pageNumber: number, animated = true) => {
      const nextPage = clampPage(pageNumber);
      setCurrentPage(nextPage);
      listRef.current?.scrollToIndex({
        index: nextPage - 1,
        animated,
      });
    },
    [],
  );

  const handleMomentumEnd = useCallback(
    (event: { nativeEvent: { contentOffset: { x: number } } }) => {
      const nextIndex = Math.round(event.nativeEvent.contentOffset.x / pageWidth);
      setCurrentPage(clampPage(nextIndex + 1));
    },
    [pageWidth],
  );

  const handleJumpSubmit = useCallback(() => {
    const parsed = Number.parseInt(jumpDraft, 10);
    const nextPage = Number.isFinite(parsed) ? clampPage(parsed) : currentPage;
    setJumpModalVisible(false);
    setJumpDraft("");
    requestAnimationFrame(() => goToPage(nextPage));
  }, [currentPage, goToPage, jumpDraft]);

  const renderPage = useCallback(
    ({ item }: { item: (typeof PAGE_ENTRIES)[number] }) => (
      <View style={[styles.pageShell, { width: pageWidth, height: pageHeight }]}>
        <Image
          fadeDuration={0}
          resizeMode="contain"
          source={item.moduleId}
          style={[styles.pageImage, { width: pageWidth, height: pageHeight }]}
        />
      </View>
    ),
    [pageHeight, pageWidth],
  );

  return (
    <SafeAreaView style={styles.screen}>
      <StatusBar hidden barStyle="light-content" />

      <View style={styles.header}>
        <Text style={styles.title}>Signo Vino</Text>
        <Pressable onPress={() => setJumpModalVisible(true)} style={styles.pageBadge}>
          <Text style={styles.pageBadgeText}>{currentPage} / {TOTAL_PAGES}</Text>
        </Pressable>
      </View>

      <FlatList
        ref={listRef}
        data={PAGE_ENTRIES}
        getItemLayout={(_, index) => ({
          index,
          length: pageWidth,
          offset: pageWidth * index,
        })}
        horizontal
        initialNumToRender={1}
        initialScrollIndex={0}
        keyExtractor={(item) => String(item.pageNumber)}
        maxToRenderPerBatch={2}
        onMomentumScrollEnd={handleMomentumEnd}
        pagingEnabled
        removeClippedSubviews
        renderItem={renderPage}
        showsHorizontalScrollIndicator={false}
        style={styles.reader}
        windowSize={3}
      />

      <View style={styles.footer}>
        <Pressable
          disabled={currentPage <= 1}
          onPress={() => goToPage(currentPage - 1)}
          style={[styles.navButton, currentPage <= 1 && styles.navButtonDisabled]}
        >
          <Text style={styles.navButtonText}>Anterior</Text>
        </Pressable>

        <Text style={styles.footerLabel}>Página {currentEntry?.pageNumber || currentPage}</Text>

        <Pressable
          disabled={currentPage >= TOTAL_PAGES}
          onPress={() => goToPage(currentPage + 1)}
          style={[styles.navButton, currentPage >= TOTAL_PAGES && styles.navButtonDisabled]}
        >
          <Text style={styles.navButtonText}>Siguiente</Text>
        </Pressable>
      </View>

      <Modal
        animationType="fade"
        onRequestClose={() => setJumpModalVisible(false)}
        transparent
        visible={jumpModalVisible}
      >
        <View style={styles.modalBackdrop}>
          <View style={styles.modalCard}>
            <Text style={styles.modalTitle}>Ir a página</Text>
            <Text style={styles.modalText}>Escribe un número entre 1 y {TOTAL_PAGES}.</Text>
            <TextInput
              autoFocus
              inputMode="numeric"
              keyboardType="number-pad"
              onChangeText={(value) => setJumpDraft(value.replace(/[^0-9]/g, "").slice(0, 3))}
              onSubmitEditing={handleJumpSubmit}
              placeholder="Número de página"
              placeholderTextColor="#6f7b94"
              style={styles.input}
              value={jumpDraft}
            />
            <View style={styles.modalActions}>
              <Pressable onPress={() => setJumpModalVisible(false)} style={styles.secondaryButton}>
                <Text style={styles.secondaryButtonText}>Cancelar</Text>
              </Pressable>
              <Pressable onPress={handleJumpSubmit} style={styles.primaryButton}>
                <Text style={styles.primaryButtonText}>Ir</Text>
              </Pressable>
            </View>
          </View>
        </View>
      </Modal>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: "#030508",
  },
  header: {
    paddingHorizontal: 18,
    paddingTop: 10,
    paddingBottom: 8,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  title: {
    color: "#ffffff",
    fontSize: 20,
    fontWeight: "700",
  },
  pageBadge: {
    backgroundColor: "#173463",
    borderRadius: 999,
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  pageBadgeText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "700",
  },
  reader: {
    flex: 1,
  },
  pageShell: {
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#000000",
  },
  pageImage: {
    backgroundColor: "#000000",
  },
  footer: {
    paddingHorizontal: 16,
    paddingTop: 10,
    paddingBottom: 14,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
  },
  footerLabel: {
    color: "#dbe4f7",
    fontSize: 15,
    fontWeight: "600",
  },
  navButton: {
    minWidth: 112,
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 14,
    backgroundColor: "#173463",
    paddingHorizontal: 16,
    paddingVertical: 14,
  },
  navButtonDisabled: {
    opacity: 0.35,
  },
  navButtonText: {
    color: "#ffffff",
    fontSize: 16,
    fontWeight: "700",
  },
  modalBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0, 0, 0, 0.72)",
    alignItems: "center",
    justifyContent: "center",
    padding: 20,
  },
  modalCard: {
    width: "100%",
    maxWidth: 360,
    borderRadius: 22,
    backgroundColor: "#111827",
    padding: 20,
    gap: 14,
  },
  modalTitle: {
    color: "#ffffff",
    fontSize: 22,
    fontWeight: "700",
  },
  modalText: {
    color: "#dbe4f7",
    fontSize: 15,
    lineHeight: 22,
  },
  input: {
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "#243552",
    backgroundColor: "#0a1120",
    color: "#ffffff",
    fontSize: 24,
    fontWeight: "700",
    paddingHorizontal: 16,
    paddingVertical: 14,
  },
  modalActions: {
    flexDirection: "row",
    justifyContent: "flex-end",
    gap: 10,
  },
  secondaryButton: {
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: "#1c2433",
  },
  secondaryButtonText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "600",
  },
  primaryButton: {
    borderRadius: 12,
    paddingHorizontal: 18,
    paddingVertical: 12,
    backgroundColor: "#1d4ed8",
  },
  primaryButtonText: {
    color: "#ffffff",
    fontSize: 15,
    fontWeight: "700",
  },
});
