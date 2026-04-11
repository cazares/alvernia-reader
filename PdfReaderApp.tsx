import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Animated,
  Dimensions,
  FlatList,
  Image,
  Keyboard,
  Modal,
  PanResponder,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  TouchableWithoutFeedback,
  View,
  type KeyboardEvent,
  type ListRenderItemInfo,
  type ViewabilityConfig,
  type ViewToken,
} from "react-native";

import { ALVERNIA_MANUAL_2_SONG_INDEX } from "./src/alverniaManual2SongIndex";
import {
  addNearbyDirectorSyncListener,
  isNearbyDirectorSyncAvailable,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
  stopNearbyDirectorSync,
} from "./src/nearbyDirectorSync";
import { OFFLINE_WEB_BUNDLE_ASSETS, OFFLINE_WEB_BUNDLE_VERSION } from "./src/offlineWebBundle";

const TOTAL_PAGES = 368;
const START_PAGE = 2;
const DIRECTOR_SESSION = "1234"; // fixed session — only one director per session

const SONG_TO_PAGE = new Map<number, number>(
  ALVERNIA_MANUAL_2_SONG_INDEX.map(({ song, page }) => [song, page]),
);
const SORTED_SONGS = [...ALVERNIA_MANUAL_2_SONG_INDEX].sort((a, b) => a.song - b.song);

const resolveSongPage = (input: string): number => {
  const n = parseInt(input, 10);
  if (!Number.isInteger(n) || n <= 0) return 1;
  const exact = SONG_TO_PAGE.get(n);
  if (exact !== undefined) return exact;
  if (n < (SORTED_SONGS[0]?.song ?? 1)) return 1;
  if (n > (SORTED_SONGS[SORTED_SONGS.length - 1]?.song ?? TOTAL_PAGES)) return TOTAL_PAGES;
  const next = SORTED_SONGS.find((s) => s.song >= n);
  return next ? next.page : TOTAL_PAGES;
};

const PAGE_ASSETS = Array.from({ length: TOTAL_PAGES }, (_, i) => {
  const pageNum = i + 1;
  const key = `pages/page-${String(pageNum).padStart(3, "0")}.jpg`;
  return { page: pageNum, source: OFFLINE_WEB_BUNDLE_ASSETS[key] };
});

// Low-res thumbnails (150px wide) for the director song grid — prevents OOM crash
const THUMB_ASSETS = Array.from({ length: TOTAL_PAGES }, (_, i) => {
  const pageNum = i + 1;
  const key = `thumbs/thumb-${String(pageNum).padStart(3, "0")}.jpg`;
  return { page: pageNum, source: OFFLINE_WEB_BUNDLE_ASSETS[key] };
});

// ── Zoomable page ─────────────────────────────────────────────────────────────
function ZoomablePage({ source, width, height }: { source: number | undefined; width: number; height: number }) {
  const scale = useRef(new Animated.Value(1)).current;
  const currentScale = useRef(1);
  const lastDist = useRef<number | null>(null);

  const panResponder = useRef(
    PanResponder.create({
      onStartShouldSetPanResponder: (_, gs) => gs.numberActiveTouches === 2,
      onMoveShouldSetPanResponder: (_, gs) => gs.numberActiveTouches === 2,
      onPanResponderGrant: () => { lastDist.current = null; },
      onPanResponderMove: (evt) => {
        const touches = evt.nativeEvent.touches;
        if (touches.length < 2) return;
        const dx = touches[0].pageX - touches[1].pageX;
        const dy = touches[0].pageY - touches[1].pageY;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (lastDist.current === null) { lastDist.current = dist; return; }
        const delta = dist / lastDist.current;
        lastDist.current = dist;
        const next = Math.max(1, Math.min(6, currentScale.current * delta));
        currentScale.current = next;
        scale.setValue(next);
      },
      onPanResponderRelease: () => {
        if (currentScale.current < 1.05) {
          Animated.spring(scale, { toValue: 1, useNativeDriver: true, bounciness: 4 }).start();
          currentScale.current = 1;
        }
        lastDist.current = null;
      },
    }),
  ).current;

  useEffect(() => {
    scale.setValue(1);
    currentScale.current = 1;
  }, [scale]);

  return (
    <Animated.View
      style={{ width, height, justifyContent: "center", alignItems: "center", transform: [{ scale }] }}
      {...panResponder.panHandlers}
    >
      {source ? (
        <Image source={source} style={{ width, height }} resizeMode="contain" />
      ) : (
        <Text style={styles.missingText}>—</Text>
      )}
    </Animated.View>
  );
}

// ── Pulsing dot for director mode ─────────────────────────────────────────────
function PulsingDot({ color }: { color: string }) {
  const opacity = useRef(new Animated.Value(1)).current;

  useEffect(() => {
    const anim = Animated.loop(
      Animated.sequence([
        Animated.timing(opacity, { toValue: 0.25, duration: 900, useNativeDriver: true }),
        Animated.timing(opacity, { toValue: 1, duration: 900, useNativeDriver: true }),
      ]),
    );
    anim.start();
    return () => anim.stop();
  }, [opacity]);

  return <Animated.View style={[styles.syncDot, { backgroundColor: color, opacity }]} />;
}

// ── Main app ──────────────────────────────────────────────────────────────────
type SyncRole = "off" | "director" | "follower";

export default function App() {
  const listRef = useRef<FlatList>(null);
  const inputRef = useRef<TextInput>(null);
  const codeInputRef = useRef<TextInput>(null);
  const currentPageRef = useRef(START_PAGE);

  const [dims, setDims] = useState(() => Dimensions.get("window"));
  const [songModal, setSongModal] = useState(false);
  const [syncModal, setSyncModal] = useState(false);
  const [songInput, setSongInput] = useState("");
  const [codeInput, setCodeInput] = useState("");
  const [keyboardHeight, setKeyboardHeight] = useState(0);
  const [syncRole, setSyncRole] = useState<SyncRole>("off");
  const [gridVisible, setGridVisible] = useState(false);
  const [gridDensityIdx, setGridDensityIdx] = useState(0);
  const [searchVisible, setSearchVisible] = useState(false);
  const [searchText, setSearchText] = useState("");
  const [browseVisible, setBrowseVisible] = useState(false);
  const [browseTab, setBrowseTab] = useState<"todas" | "recientes">("todas");
  const [followerNotice, setFollowerNotice] = useState(false);
  const searchInputRef = useRef<TextInput>(null);
  const syncRoleRef = useRef<SyncRole>("off");
  const recentSongsRef = useRef<number[]>([]);
  const syncAvailable = isNearbyDirectorSyncAvailable();

  const GRID_DENSITY = [2, 3, 4] as const;
  const gridCols = GRID_DENSITY[gridDensityIdx];

  // Orientation handling
  useEffect(() => {
    const sub = Dimensions.addEventListener("change", ({ window }) => {
      setDims(window);
      setTimeout(() => {
        listRef.current?.scrollToIndex({ index: currentPageRef.current - 1, animated: false });
      }, 50);
    });
    return () => sub.remove();
  }, []);

  // Keyboard height
  useEffect(() => {
    const show = (e: KeyboardEvent) => setKeyboardHeight(e.endCoordinates.height);
    const hide = () => setKeyboardHeight(0);
    const s1 = Keyboard.addListener("keyboardWillShow", show);
    const s2 = Keyboard.addListener("keyboardWillHide", hide);
    const s3 = Keyboard.addListener("keyboardDidShow", show);
    const s4 = Keyboard.addListener("keyboardDidHide", hide);
    return () => { s1.remove(); s2.remove(); s3.remove(); s4.remove(); };
  }, []);

  // Keep ref in sync for use inside event callbacks
  useEffect(() => { syncRoleRef.current = syncRole; }, [syncRole]);

  // Sync listener
  useEffect(() => {
    if (!syncAvailable) return;
    const sub = addNearbyDirectorSyncListener((event: any) => {
      if (event.type === "page" && typeof event.page === "number") {
        // Follower: jump to director's page
        goToPage(event.page);
      } else if (event.type === "error" && event.code === "DIRECTOR_CONFLICT") {
        // A newer director took over — Swift already cleaned up transport.
        setSyncRole("follower");
        startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
      } else if (event.type === "state" && event.status === "connected" && syncRoleRef.current === "follower") {
        // Follower just connected to director — show notification
        setFollowerNotice(true);
        setTimeout(() => setFollowerNotice(false), 5000);
      }
    });
    return () => sub.remove();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [syncAvailable]);

  // Auto-start as follower on mount
  useEffect(() => {
    if (!syncAvailable) return;
    startNearbyFollower(DIRECTOR_SESSION).then(() => setSyncRole("follower")).catch(() => {});
    return () => { stopNearbyDirectorSync().catch(() => {}); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const goToPage = useCallback((page: number) => {
    const clamped = Math.max(1, Math.min(page, TOTAL_PAGES));
    currentPageRef.current = clamped;
    listRef.current?.scrollToIndex({ index: clamped - 1, animated: false });
  }, []);

  // Song modal
  const longPressedRef = useRef(false);
  const openSongModal = useCallback(() => {
    if (longPressedRef.current) { longPressedRef.current = false; return; }
    setSongModal(true);
  }, []);
  const closeSongModal = useCallback(() => {
    Keyboard.dismiss();
    setSongModal(false);
    setSongInput("");
    setKeyboardHeight(0);
  }, []);
  useEffect(() => {
    if (songModal) {
      setTimeout(() => inputRef.current?.focus(), 80);
    }
  }, [songModal]);

  const handleSongSubmit = useCallback(() => {
    const trimmed = songInput.trim();
    closeSongModal();
    if (trimmed !== "") {
      const n = parseInt(trimmed, 10);
      if (n > 0) recentSongsRef.current = [n, ...recentSongsRef.current.filter(s => s !== n)].slice(0, 20);
      goToPage(resolveSongPage(trimmed));
    }
  }, [songInput, goToPage, closeSongModal]);

  // Sync modal
  const openSyncModal = useCallback(() => {
    longPressedRef.current = true;
    setCodeInput("");
    setSyncModal(true);
    setTimeout(() => codeInputRef.current?.focus(), 50);
  }, []);
  const closeSyncModal = useCallback(() => {
    Keyboard.dismiss();
    setSyncModal(false);
    setCodeInput("");
  }, []);

  const handleBecomeDirector = useCallback(async () => {
    if (codeInput !== DIRECTOR_SESSION) {
      Alert.alert("Código incorrecto", "El código ingresado no es válido.");
      return;
    }
    closeSyncModal();
    try {
      // startNearbyDirector resets any existing session internally (no stopSync needed).
      // Skipping stopSync avoids a state:idle event that would race with setSyncRole("director").
      await startNearbyDirector(DIRECTOR_SESSION);
      setSyncRole("director");
    } catch {
      // If director fails, go back to follower
      startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
      setSyncRole("follower");
      Alert.alert("Error", "No se pudo iniciar el modo director.");
    }
  }, [codeInput, closeSyncModal]);

  const handleStopSync = useCallback(async () => {
    closeSyncModal();
    try {
      // startNearbyFollower resets internally — no stopSync needed.
      await startNearbyFollower(DIRECTOR_SESSION);
      setSyncRole("follower");
    } catch {
      setSyncRole("follower");
    }
  }, [closeSyncModal]);

  // Spotlight-style search
  const openSearch = useCallback(() => {
    setSearchText("");
    setSearchVisible(true);
    setBrowseVisible(false);
    setGridVisible(false);
    setTimeout(() => searchInputRef.current?.focus(), 80);
  }, []);
  const closeSearch = useCallback(() => {
    Keyboard.dismiss();
    setSearchVisible(false);
    setSearchText("");
  }, []);
  const handleSearchSubmit = useCallback(() => {
    const trimmed = searchText.trim();
    closeSearch();
    if (trimmed !== "") {
      const n = parseInt(trimmed, 10);
      if (n > 0) recentSongsRef.current = [n, ...recentSongsRef.current.filter(s => s !== n)].slice(0, 20);
      goToPage(resolveSongPage(trimmed));
    }
  }, [searchText, goToPage, closeSearch]);

  // Browse categories
  const openBrowse = useCallback(() => {
    setBrowseVisible(true);
    setSearchVisible(false);
    setGridVisible(false);
  }, []);
  const closeBrowse = useCallback(() => setBrowseVisible(false), []);
  const handleBrowseSongTap = useCallback((song: number) => {
    const page = resolveSongPage(String(song));
    goToPage(page);
    recentSongsRef.current = [song, ...recentSongsRef.current.filter(s => s !== song)].slice(0, 20);
    setBrowseVisible(false);
  }, [goToPage]);

  const handleGridButtonPress = useCallback(() => {
    setSearchVisible(false);
    setBrowseVisible(false);
    if (!gridVisible) {
      setGridVisible(true);
    } else {
      setGridDensityIdx((i) => (i + 1) % GRID_DENSITY.length);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [gridVisible]);

  const closeGrid = useCallback(() => setGridVisible(false), []);

  const handleGridSongTap = useCallback((song: number) => {
    const page = resolveSongPage(String(song));
    goToPage(page);
    recentSongsRef.current = [song, ...recentSongsRef.current.filter(s => s !== song)].slice(0, 20);
    setGridVisible(false);
  }, [goToPage]);

  const viewabilityConfig = useMemo<ViewabilityConfig>(
    () => ({ viewAreaCoveragePercentThreshold: 50 }),
    [],
  );

  const onViewableItemsChanged = useCallback(
    ({ viewableItems }: { viewableItems: ViewToken[] }) => {
      const first = viewableItems[0];
      if (!first?.item) return;
      const page = (first.item as typeof PAGE_ASSETS[0]).page;
      currentPageRef.current = page;
      // Director broadcasts page changes to all followers
      if (syncRole === "director") {
        sendNearbyDirectorPageUpdate(page, TOTAL_PAGES).catch(() => {});
      }
    },
    [syncRole],
  );

  const { width, height } = dims;

  const renderItem = useCallback(({ item }: ListRenderItemInfo<typeof PAGE_ASSETS[0]>) => (
    <View style={{ width, height, backgroundColor: "#000" }}>
      <ZoomablePage source={item.source} width={width} height={height} />
    </View>
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ), [width, height]);

  const keyExtractor = useCallback((item: typeof PAGE_ASSETS[0]) => String(item.page), []);
  const getItemLayout = useCallback((_: unknown, index: number) => ({
    length: width, offset: width * index, index,
  }), [width]);

  const availableHeight = height - keyboardHeight;
  const cardTop = Math.max(24, availableHeight / 2 - 80);

  return (
    <View style={styles.screen}>
      <StatusBar hidden />

      <FlatList
        key={`${width}x${height}`}
        ref={listRef}
        data={PAGE_ASSETS}
        renderItem={renderItem}
        keyExtractor={keyExtractor}
        horizontal
        pagingEnabled
        showsHorizontalScrollIndicator={false}
        getItemLayout={getItemLayout}
        initialScrollIndex={START_PAGE - 1}
        viewabilityConfig={viewabilityConfig}
        onViewableItemsChanged={onViewableItemsChanged}
        removeClippedSubviews
        maxToRenderPerBatch={3}
        windowSize={5}
        initialNumToRender={3}
      />

      {/* ── Song grid (thumbnails) — full-screen overlay ── */}
      {gridVisible && syncRole === "director" && (() => {
        const GAP = 6;
        const cellW = (width - GAP * (gridCols + 1)) / gridCols;
        const imgH = Math.round(cellW * 1.33);
        const labelH = 28;
        const cellH = imgH + labelH + GAP;

        return (
          <View style={styles.gridOverlay}>
            <FlatList
              data={SORTED_SONGS}
              keyExtractor={(item) => String(item.song)}
              numColumns={gridCols}
              key={`grid-${gridCols}`}
              windowSize={3}
              maxToRenderPerBatch={gridCols * 2}
              initialNumToRender={gridCols * 5}
              contentContainerStyle={{ padding: GAP / 2 }}
              renderItem={({ item }) => {
                const thumb = THUMB_ASSETS[item.page - 1];
                return (
                  <TouchableOpacity
                    style={{ width: cellW, margin: GAP / 2, backgroundColor: "#000", borderRadius: 6, overflow: "hidden" }}
                    onPress={() => handleGridSongTap(item.song)}
                    activeOpacity={0.7}
                  >
                    {thumb?.source ? (
                      <Image source={thumb.source} style={{ width: cellW, height: imgH }} resizeMode="cover" />
                    ) : (
                      <View style={{ width: cellW, height: imgH, backgroundColor: "#333" }} />
                    )}
                    <View style={{ height: labelH, backgroundColor: "#000", justifyContent: "center", alignItems: "center" }}>
                      <Text style={{ color: "#fff", fontSize: 14, fontWeight: "700", fontVariant: ["tabular-nums"] }}>{item.song}</Text>
                    </View>
                  </TouchableOpacity>
                );
              }}
            />
            {/* Bottom toolbar */}
            <View style={styles.gridToolbar}>
              <TouchableOpacity style={styles.gridToolbarBtn} onPress={() => setGridDensityIdx((i) => (i + 1) % GRID_DENSITY.length)} activeOpacity={0.8}>
                <Text style={styles.gridToolbarText}>⊞  {gridCols} col</Text>
              </TouchableOpacity>
              <TouchableOpacity style={styles.gridToolbarBtn} onPress={closeGrid} activeOpacity={0.8}>
                <Text style={styles.gridToolbarText}>✕  Cerrar</Text>
              </TouchableOpacity>
            </View>
          </View>
        );
      })()}

      {/* ── Browse overlay — director only ── */}
      {browseVisible && syncRole === "director" && (
        <View style={styles.gridOverlay}>
          {/* Tab bar */}
          <View style={styles.browseTabBar}>
            <TouchableOpacity style={[styles.browseTab, browseTab === "todas" && styles.browseTabActive]} onPress={() => setBrowseTab("todas")}>
              <Text style={[styles.browseTabText, browseTab === "todas" && styles.browseTabTextActive]}>Todas</Text>
            </TouchableOpacity>
            <TouchableOpacity style={[styles.browseTab, browseTab === "recientes" && styles.browseTabActive]} onPress={() => setBrowseTab("recientes")}>
              <Text style={[styles.browseTabText, browseTab === "recientes" && styles.browseTabTextActive]}>Recientes</Text>
            </TouchableOpacity>
          </View>
          {/* Content */}
          {browseTab === "todas" ? (
            <FlatList
              data={SORTED_SONGS}
              keyExtractor={(item) => `b-${item.song}`}
              renderItem={({ item }) => (
                <TouchableOpacity style={styles.browseRow} onPress={() => handleBrowseSongTap(item.song)} activeOpacity={0.6}>
                  <Text style={styles.browseSong}>{item.song}</Text>
                  <Text style={styles.browsePage}>p.{item.page}</Text>
                </TouchableOpacity>
              )}
              initialNumToRender={30}
              windowSize={5}
            />
          ) : (
            <FlatList
              data={recentSongsRef.current.map(s => SORTED_SONGS.find(x => x.song === s)).filter(Boolean) as typeof SORTED_SONGS}
              keyExtractor={(item) => `r-${item.song}`}
              renderItem={({ item }) => (
                <TouchableOpacity style={styles.browseRow} onPress={() => handleBrowseSongTap(item.song)} activeOpacity={0.6}>
                  <Text style={styles.browseSong}>{item.song}</Text>
                  <Text style={styles.browsePage}>p.{item.page}</Text>
                </TouchableOpacity>
              )}
              ListEmptyComponent={<Text style={styles.browseEmpty}>Sin canciones recientes</Text>}
            />
          )}
          {/* Close */}
          <View style={styles.gridToolbar}>
            <TouchableOpacity style={styles.gridToolbarBtn} onPress={closeBrowse} activeOpacity={0.8}>
              <Text style={styles.gridToolbarText}>✕  Cerrar</Text>
            </TouchableOpacity>
          </View>
        </View>
      )}

      {/* ── Search overlay (Spotlight-style) — director only ── */}
      {searchVisible && syncRole === "director" && (
        <TouchableWithoutFeedback onPress={closeSearch}>
          <View style={styles.searchOverlay}>
            <TouchableWithoutFeedback>
              <View style={styles.searchBar}>
                <TextInput
                  ref={searchInputRef}
                  style={styles.searchInput}
                  value={searchText}
                  onChangeText={(t) => setSearchText(t.replace(/[^0-9]/g, ""))}
                  onSubmitEditing={handleSearchSubmit}
                  placeholder="Buscar canción..."
                  placeholderTextColor="rgba(255,255,255,0.4)"
                  keyboardType="number-pad"
                  returnKeyType="go"
                  maxLength={4}
                  selectTextOnFocus
                />
                <TouchableOpacity style={styles.searchGoBtn} onPress={handleSearchSubmit} activeOpacity={0.7}>
                  <Text style={styles.searchGoBtnText}>Ir</Text>
                </TouchableOpacity>
              </View>
            </TouchableWithoutFeedback>
          </View>
        </TouchableWithoutFeedback>
      )}

      {/* Nav trigger — tap: song modal, long press: sync modal */}
      <TouchableOpacity
        style={[styles.cornerButton, styles.navTrigger]}
        onPress={openSongModal}
        onLongPress={syncAvailable ? openSyncModal : undefined}
        delayLongPress={500}
        activeOpacity={0.75}
        hitSlop={{ top: 12, bottom: 12, left: 12, right: 12 }}
      >
        <Text style={styles.navTriggerIcon}>♪</Text>
        <Text style={styles.navTriggerArrow}>›</Text>
        {syncRole === "director" && <PulsingDot color="#4a90e2" />}
        {syncRole === "follower" && <View style={[styles.syncDot, { backgroundColor: "#4cff91" }]} />}
      </TouchableOpacity>

      {/* Director buttons — top-right row: [Browse] [Grid] [Search] */}
      {syncRole === "director" && (
        <View style={styles.directorBar}>
          <TouchableOpacity style={styles.dirBtn} onPress={openBrowse} activeOpacity={0.75}>
            <Text style={styles.dirBtnIcon}>☰</Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.dirBtn} onPress={handleGridButtonPress} activeOpacity={0.75}>
            <Text style={styles.dirBtnIcon}>⊞</Text>
            {gridVisible && <Text style={styles.gridDensityBadge}>{gridCols}</Text>}
          </TouchableOpacity>
          <TouchableOpacity style={styles.dirBtn} onPress={openSearch} activeOpacity={0.75}>
            <Text style={styles.dirBtnIcon}>⌕</Text>
          </TouchableOpacity>
        </View>
      )}

      {/* Version label */}
      <Text style={styles.versionLabel} pointerEvents="none">1.0.{OFFLINE_WEB_BUNDLE_VERSION}</Text>

      {/* ── Song navigation modal ── */}
      <Modal visible={songModal} transparent animationType="fade" onRequestClose={closeSongModal} statusBarTranslucent>
        <TouchableWithoutFeedback onPress={closeSongModal}>
          <View style={styles.modalBackdrop}>
            <TouchableWithoutFeedback>
              <View style={[styles.inputCard, { top: cardTop }]}>
                <Text style={styles.inputLabel}>Ir a canción</Text>
                <TextInput
                  ref={inputRef}
                  style={styles.songInput}
                  value={songInput}
                  onChangeText={(t) => setSongInput(t.replace(/[^0-9]/g, ""))}
                  onSubmitEditing={handleSongSubmit}
                  placeholder="Número de canción"
                  placeholderTextColor="rgba(0,0,0,0.35)"
                  keyboardType="number-pad"
                  returnKeyType="go"
                  maxLength={4}
                  selectTextOnFocus
                />
                <View style={styles.modalButtons}>
                  <TouchableOpacity style={styles.cancelBtn} onPress={closeSongModal} activeOpacity={0.7}>
                    <Text style={styles.cancelText}>Cancelar</Text>
                  </TouchableOpacity>
                  <TouchableOpacity
                    style={[styles.goBtn, !songInput && styles.goBtnDisabled]}
                    onPress={handleSongSubmit}
                    activeOpacity={0.7}
                  >
                    <Text style={styles.goText}>Ir ›</Text>
                  </TouchableOpacity>
                </View>
              </View>
            </TouchableWithoutFeedback>
          </View>
        </TouchableWithoutFeedback>
      </Modal>

      {/* ── Director sync modal ── */}
      <Modal visible={syncModal} transparent animationType="fade" onRequestClose={closeSyncModal} statusBarTranslucent>
        <TouchableWithoutFeedback onPress={closeSyncModal}>
          <View style={styles.modalBackdrop}>
            <TouchableWithoutFeedback>
              <View style={[styles.inputCard, { top: cardTop }]}>
                <Text style={styles.inputLabel}>Sincronización de grupo</Text>

                {syncRole === "director" ? (
                  // Already in a role — show status + stop
                  <>
                    <Text style={styles.syncStatusText}>
                      {syncRole === "director" ? "📡 Eres el director" : "👁 Siguiendo al director"}
                    </Text>
                    <TouchableOpacity style={[styles.goBtn, { marginTop: 8 }]} onPress={handleStopSync} activeOpacity={0.7}>
                      <Text style={styles.goText}>Detener sincronización</Text>
                    </TouchableOpacity>
                  </>
                ) : (
                  // Not in a role — offer options
                  <>
                    <Text style={styles.syncHint}>Ingresa el código para tomar control:</Text>
                    <TextInput
                      ref={codeInputRef}
                      style={styles.songInput}
                      value={codeInput}
                      onChangeText={(t) => setCodeInput(t.replace(/[^0-9]/g, ""))}
                      onSubmitEditing={handleBecomeDirector}
                      placeholder="Código"
                      placeholderTextColor="rgba(0,0,0,0.35)"
                      keyboardType="number-pad"
                      returnKeyType="go"
                      maxLength={6}
                      secureTextEntry
                      selectTextOnFocus
                    />
                    <View style={styles.modalButtons}>
                      <TouchableOpacity style={styles.cancelBtn} onPress={closeSyncModal} activeOpacity={0.7}>
                        <Text style={styles.cancelText}>Cancelar</Text>
                      </TouchableOpacity>
                      <TouchableOpacity
                        style={[styles.goBtn, !codeInput && styles.goBtnDisabled]}
                        onPress={handleBecomeDirector}
                        activeOpacity={0.7}
                      >
                        <Text style={styles.goText}>Tomar control</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                )}
              </View>
            </TouchableWithoutFeedback>
          </View>
        </TouchableWithoutFeedback>
      </Modal>

      {/* ── Follower connection notification ── */}
      <Modal visible={followerNotice} transparent animationType="fade" statusBarTranslucent>
        <TouchableWithoutFeedback onPress={() => setFollowerNotice(false)}>
          <View style={styles.noticeBackdrop}>
            <View style={styles.noticeCard}>
              <Text style={styles.noticeEmoji}>📡</Text>
              <Text style={styles.noticeTitle}>Conectado al director</Text>
              <Text style={styles.noticeBody}>Tu iPad seguirá automáticamente los cambios de página del director. No necesitas hacer nada.</Text>
              <Text style={styles.noticeDismiss}>Toca para cerrar</Text>
            </View>
          </View>
        </TouchableWithoutFeedback>
      </Modal>
    </View>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#000" },
  missingText: { color: "rgba(255,255,255,0.15)", fontSize: 48 },

  cornerButton: {
    position: "absolute",
    top: 1.25,
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(26,26,46,0.38)",
    borderRadius: 14,
    paddingHorizontal: 18,
    paddingVertical: 13,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.4,
    shadowRadius: 5,
    elevation: 7,
    gap: 4,
  },
  navTrigger: { left: 1.25 },
  navTriggerIcon: { fontSize: 32, color: "#fff", lineHeight: 38 },
  navTriggerArrow: { fontSize: 36, color: "#7ec8f7", lineHeight: 38, fontWeight: "700" },

  // Director top-right button row
  directorBar: {
    position: "absolute",
    top: 1.25,
    right: 1.25,
    flexDirection: "row",
    gap: 6,
  },
  dirBtn: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(26,26,46,0.38)",
    borderRadius: 14,
    paddingHorizontal: 14,
    paddingVertical: 10,
    gap: 3,
  },
  dirBtnIcon: { fontSize: 26, color: "#fff", lineHeight: 30 },
  syncDot: {
    position: "absolute",
    top: 7,
    right: 7,
    width: 9,
    height: 9,
    borderRadius: 5,
  },

  versionLabel: {
    position: "absolute",
    bottom: 10,
    right: 12,
    fontSize: 10,
    color: "#aaa",
    fontVariant: ["tabular-nums"],
  },

  modalBackdrop: { flex: 1, backgroundColor: "rgba(0,0,0,0.55)" },
  inputCard: {
    position: "absolute",
    left: 32,
    right: 32,
    backgroundColor: "#fff",
    borderRadius: 16,
    padding: 20,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 8 },
    shadowOpacity: 0.3,
    shadowRadius: 16,
    elevation: 12,
    gap: 8,
  },
  inputLabel: {
    fontSize: 13,
    fontWeight: "600",
    color: "#555",
    textTransform: "uppercase",
    letterSpacing: 0.6,
    marginBottom: 4,
  },
  syncHint: { fontSize: 13, color: "#666", marginBottom: 4 },
  syncStatusText: { fontSize: 17, fontWeight: "600", color: "#222", textAlign: "center", paddingVertical: 8 },
  songInput: {
    backgroundColor: "#f4f4f4",
    color: "#111",
    fontSize: 28,
    fontWeight: "300",
    fontVariant: ["tabular-nums"],
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 10,
    textAlign: "center",
  },
  modalButtons: { flexDirection: "row", gap: 10, marginTop: 6 },
  cancelBtn: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 10,
    backgroundColor: "#f0f0f0",
    alignItems: "center",
  },
  cancelText: { color: "#555", fontSize: 16, fontWeight: "500" },
  goBtn: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 10,
    backgroundColor: "#1a1a2e",
    alignItems: "center",
  },
  goBtnDisabled: { backgroundColor: "#bbb" },
  goText: { color: "#fff", fontSize: 16, fontWeight: "700" },

  gridOverlay: {
    position: "absolute",
    top: 0, left: 0, right: 0, bottom: 0,
    backgroundColor: "#111",
    flexDirection: "column",
  },
  gridDensityBadge: { fontSize: 14, color: "#7ec8f7", fontWeight: "700", lineHeight: 38 },
  gridToolbar: {
    flexDirection: "row",
    backgroundColor: "rgba(0,0,0,0.85)",
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "rgba(255,255,255,0.15)",
  },
  gridToolbarBtn: {
    flex: 1,
    paddingVertical: 14,
    alignItems: "center",
    justifyContent: "center",
  },
  gridToolbarText: { color: "#fff", fontSize: 16, fontWeight: "600" },

  // Search overlay (Spotlight-style)
  searchOverlay: {
    position: "absolute",
    top: 0, left: 0, right: 0, bottom: 0,
    backgroundColor: "rgba(0,0,0,0.6)",
    justifyContent: "flex-start",
    paddingTop: 60,
    paddingHorizontal: 24,
  },
  searchBar: {
    flexDirection: "row",
    backgroundColor: "rgba(30,30,50,0.95)",
    borderRadius: 14,
    padding: 6,
    alignItems: "center",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 8 },
    shadowOpacity: 0.4,
    shadowRadius: 16,
    elevation: 12,
  },
  searchInput: {
    flex: 1,
    color: "#fff",
    fontSize: 24,
    fontWeight: "300",
    fontVariant: ["tabular-nums"],
    paddingHorizontal: 14,
    paddingVertical: 10,
  },
  searchGoBtn: {
    backgroundColor: "#3b82f6",
    borderRadius: 10,
    paddingHorizontal: 20,
    paddingVertical: 10,
  },
  searchGoBtnText: { color: "#fff", fontSize: 18, fontWeight: "700" },

  // Browse overlay
  browseTabBar: {
    flexDirection: "row",
    backgroundColor: "#1a1a2e",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(255,255,255,0.15)",
  },
  browseTab: {
    flex: 1,
    paddingVertical: 14,
    alignItems: "center",
  },
  browseTabActive: {
    borderBottomWidth: 3,
    borderBottomColor: "#3b82f6",
  },
  browseTabText: { color: "rgba(255,255,255,0.5)", fontSize: 15, fontWeight: "600" },
  browseTabTextActive: { color: "#fff" },
  browseRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 14,
    paddingHorizontal: 20,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(255,255,255,0.08)",
  },
  browseSong: { fontSize: 20, fontWeight: "700", color: "#fff", width: 60, fontVariant: ["tabular-nums"] },
  browsePage: { fontSize: 14, color: "rgba(255,255,255,0.4)" },
  browseEmpty: { color: "rgba(255,255,255,0.3)", fontSize: 16, textAlign: "center", marginTop: 60 },

  // Follower notification
  noticeBackdrop: { flex: 1, backgroundColor: "rgba(0,0,0,0.6)", justifyContent: "center", alignItems: "center" },
  noticeCard: {
    backgroundColor: "#fff",
    borderRadius: 20,
    padding: 28,
    marginHorizontal: 40,
    alignItems: "center",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 10 },
    shadowOpacity: 0.3,
    shadowRadius: 20,
    elevation: 16,
  },
  noticeEmoji: { fontSize: 48, marginBottom: 12 },
  noticeTitle: { fontSize: 20, fontWeight: "700", color: "#1a1a2e", marginBottom: 8 },
  noticeBody: { fontSize: 15, color: "#555", textAlign: "center", lineHeight: 22, marginBottom: 16 },
  noticeDismiss: { fontSize: 13, color: "#999" },
});
