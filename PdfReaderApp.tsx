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
  SectionList,
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
// @ts-ignore — Metro resolves JSON fine
import SONG_TITLES from "./assets/offline-web/song-titles.json";
// @ts-ignore
import SONG_SEARCH_INDEX from "./assets/offline-web/song-search-index.json";
// @ts-ignore
import PAGES_JSON from "./assets/offline-web/pages.json";

const TOTAL_PAGES = 368;
const START_PAGE = 2;
const DIRECTOR_SESSION = "1234"; // fixed session — only one director per session

const SONG_TO_PAGE = new Map<number, number>(
  ALVERNIA_MANUAL_2_SONG_INDEX.map(({ song, page }) => [song, page]),
);
const SORTED_SONGS = [...ALVERNIA_MANUAL_2_SONG_INDEX].sort((a, b) => a.song - b.song);

// Safe accent-insensitive normalizer (normalize may not exist on all Hermes builds)
const normalizeText = (s: string): string => {
  try {
    return s.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  } catch {
    return s.toLowerCase();
  }
};

// Build song→themes lookup from pages.json (all 312 songs are tagged)
const SONG_THEMES: Record<number, string[]> = {};
for (const entry of (PAGES_JSON as any).songIndex ?? []) {
  if (entry.themes?.length) SONG_THEMES[entry.song as number] = entry.themes;
}

// Extract a lyric snippet from raw OCR text.
// Starts at 30% to skip title/metadata. Strips chord tokens, stage directions,
// and any line that is essentially just the song title repeated.
const extractLyricSnippet = (raw: string, title: string): string => {
  const normTitle = normalizeText(title).replace(/[^a-z0-9 ]/g, "").trim();
  const start = Math.floor(raw.length * 0.30);
  const slice = raw.slice(start);
  const lines = slice.split("\n");

  const chordToken = /\b[A-G][#b]?(m|maj|sus|dim|aug|add)?[0-9]{0,2}(\/[A-G][#b]?)?\b/g;
  const skipLine = /^\s*(intro|coro|estrofa|puente|fin\s*$|bridge|verso|rev[\s\d]|capo|posible|\(coro\)|\(h\)|\(m\)|\(t\)|\(s\)|2\s*veces|dos\s*veces|[0-9]+\.)/i;

  const cleanLine = (t: string): string =>
    t.replace(chordToken, " ")
     .replace(/[_\-#:\\/*()[\]{}<>|^~`]/g, " ")
     .replace(/\s{2,}/g, " ")
     .trim();

  const realWords = (s: string) => s.split(/\s+/).filter((w) => /[a-záéíóúüñ]{2,}/i.test(w));

  // Collect ALL cleaned lyric lines — iOS numberOfLines={2} will truncate naturally
  const collected: string[] = [];
  for (const line of lines) {
    const t = line.trim();
    if (t.length < 6) continue;
    if (skipLine.test(t)) continue;
    const stripped = cleanLine(t);
    if (realWords(stripped).length < 2) continue;
    // Skip lines that are just the title repeated
    const normStripped = normalizeText(stripped).replace(/[^a-z0-9 ]/g, "").trim();
    if (normTitle.length > 4 && normStripped.includes(normTitle)) continue;
    collected.push(stripped);
  }
  return collected.join(" · ");
};

// Searchable song entries: title + full OCR text + theme tags + lyric snippet
const SEARCHABLE_SONGS = SORTED_SONGS.map((s) => {
  const title = (SONG_TITLES as Record<string, string>)[String(s.song)] ?? "";
  const fullText = (SONG_SEARCH_INDEX as Record<string, string>)[String(s.song)] ?? title;
  const normalized = normalizeText(fullText);
  const themes: string[] = SONG_THEMES[s.song] ?? [];
  const snippet = extractLyricSnippet(fullText, title);
  return { ...s, title, normalized, themes, snippet };
});

// Theme ID → display label mapping
const THEME_LABELS: Record<string, string> = {
  misa: "Parte de Misa",
  entrada: "Entrada",
  envio: "Despedida / Envío",
  eucaristia: "Eucaristía / Comunión",
  alabanza: "Alabanza",
  maria: "Virgen María",
  espiritu_santo: "Espíritu Santo",
  cuaresma: "Cuaresma / Semana Santa",
  resurreccion: "Pascua / Resurrección",
  navidad: "Navidad",
  adviento: "Adviento",
  comunidad: "Comunidad",
  fe: "Fe / Esperanza",
  sanacion: "Sanación / Perdón",
  paz: "Paz",
  mision: "Misión",
  ninos: "Niños",
  bodas: "Bodas",
  funerales: "Funerales",
  bautismo: "Bautismo",
  confirmacion: "Confirmación",
  primera_comunion: "Primera Comunión",
  procesion: "Procesión",
  santos: "Santos",
};

// Which theme IDs to surface as "Parte de Misa" subgroups when searched
const MISA_KEYWORDS: Record<string, string> = {
  gloria: "Gloria",
  santo: "Santo / Sanctus",
  sanctus: "Santo / Sanctus",
  aleluya: "Aleluya",
  alleluia: "Aleluya",
  cordero: "Cordero de Dios",
  piedad: "Piedad / Kyrie",
  kyrie: "Piedad / Kyrie",
  ofertorio: "Ofertorio",
  ofrenda: "Ofertorio",
  comunion: "Comunión",
  padre_nuestro: "Padre Nuestro",
};

type SearchResultItem =
  | { type: "header"; title: string; key: string }
  | { type: "song"; song: number; page: number; title: string; matchContext?: string; key: string };

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
  const followerBannerAnim = useRef(new Animated.Value(-120)).current;
  const lastFollowerNoticeRef = useRef(0);
  const followerDismissTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
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
        // Follower just connected to director — show banner (30s cooldown)
        const now = Date.now();
        if (now - lastFollowerNoticeRef.current > 30_000) {
          lastFollowerNoticeRef.current = now;
          showFollowerBanner();
        }
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

  // Follower banner
  const hideFollowerBanner = useCallback(() => {
    if (followerDismissTimer.current) { clearTimeout(followerDismissTimer.current); followerDismissTimer.current = null; }
    Animated.spring(followerBannerAnim, { toValue: -120, useNativeDriver: true, bounciness: 0, speed: 18 }).start(() => {
      setFollowerNotice(false);
    });
  }, [followerBannerAnim]);

  const showFollowerBanner = useCallback(() => {
    setFollowerNotice(true);
    followerBannerAnim.setValue(-120);
    Animated.spring(followerBannerAnim, { toValue: 0, useNativeDriver: true, bounciness: 6, speed: 14 }).start();
    followerDismissTimer.current = setTimeout(() => hideFollowerBanner(), 4500);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [followerBannerAnim, hideFollowerBanner]);

  const followerBannerPan = useRef(
    PanResponder.create({
      onMoveShouldSetPanResponder: (_, gs) => Math.abs(gs.dy) > 6,
      onPanResponderMove: (_, gs) => {
        if (gs.dy < 0) followerBannerAnim.setValue(gs.dy);
      },
      onPanResponderRelease: (_, gs) => {
        if (gs.dy < -30 || gs.vy < -0.5) {
          hideFollowerBanner();
        } else {
          Animated.spring(followerBannerAnim, { toValue: 0, useNativeDriver: true, bounciness: 4, speed: 16 }).start();
        }
      },
    }),
  ).current;

  // Spotlight-style keyword search
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

  // Compute grouped search results on every keystroke
  const searchSections = useMemo(() => {
    const q = searchText.trim();
    if (!q) return [];
    const normalizedQ = normalizeText(q);
    const words = normalizedQ.split(/\s+/).filter(Boolean);
    const isNumeric = /^\d+$/.test(q);
    const sections: { title: string; data: { song: number; page: number; title: string }[] }[] = [];
    const seenSongs = new Set<number>();

    const addSection = (title: string, songs: typeof SEARCHABLE_SONGS) => {
      const unique = songs.filter((s) => !seenSongs.has(s.song));
      if (unique.length === 0) return;
      unique.forEach((s) => seenSongs.add(s.song));
      sections.push({ title, data: unique });
    };

    // 1) Numeric → exact + prefix match at top
    if (isNumeric) {
      const n = parseInt(q, 10);
      const exact = SEARCHABLE_SONGS.find((s) => s.song === n);
      if (exact) addSection("Canción", [exact]);
      const prefix = SEARCHABLE_SONGS.filter((s) => s.song !== n && String(s.song).startsWith(q)).slice(0, 12);
      if (prefix.length > 0) addSection("Canciones", prefix);
      return sections;
    }

    const titleNorm = (s: typeof SEARCHABLE_SONGS[0]) => normalizeText(s.title);
    const matchesWords = (text: string) => words.every((w) => text.includes(w));

    // 2) Recents matching query (title or lyrics)
    const recentMatches = recentSongsRef.current
      .map((sn) => SEARCHABLE_SONGS.find((s) => s.song === sn))
      .filter((s): s is typeof SEARCHABLE_SONGS[0] =>
        !!s && (matchesWords(titleNorm(s)) || matchesWords(s.normalized)))
      .slice(0, 5);
    addSection("Recientes", recentMatches);

    // 3) Title matches
    const titleMatches = SEARCHABLE_SONGS.filter((s) => matchesWords(titleNorm(s))).slice(0, 15);
    addSection("Canciones", titleMatches);

    // 4) Lyric-only matches (not already in title section)
    const lyricMatches = SEARCHABLE_SONGS.filter((s) =>
      !seenSongs.has(s.song) && s.normalized && matchesWords(s.normalized)
    ).slice(0, 10);
    addSection("Letras", lyricMatches);

    // 5) Misa subgroup — match specific mass-part keywords in query
    const misaMatch = Object.entries(MISA_KEYWORDS).find(([kw]) =>
      normalizedQ.includes(kw) || kw.includes(normalizedQ)
    );
    if (misaMatch) {
      const [kw, label] = misaMatch;
      const misaSongs = SEARCHABLE_SONGS.filter((s) =>
        !seenSongs.has(s.song) &&
        (s.themes.includes("misa") || s.themes.includes("eucaristia") || s.themes.includes("entrada")) &&
        (titleNorm(s).includes(kw) || s.normalized.includes(kw))
      ).slice(0, 10);
      addSection(`Misa · ${label}`, misaSongs);
    }

    // 6) Theme tag match — if query matches a theme label or ID, surface tagged songs
    const themeEntry = Object.entries(THEME_LABELS).find(([id, label]) => {
      const normLabel = normalizeText(label);
      return normalizedQ.includes(id) || id.includes(normalizedQ) ||
             normLabel.includes(normalizedQ) || normalizedQ.includes(normLabel);
    });
    if (themeEntry) {
      const [themeId, themeLabel] = themeEntry;
      const themeSongs = SEARCHABLE_SONGS.filter((s) =>
        !seenSongs.has(s.song) && s.themes.includes(themeId)
      ).slice(0, 12);
      addSection(themeLabel, themeSongs);
    }

    return sections;
  }, [searchText]);

  const handleSearchResultTap = useCallback((song: number) => {
    const page = resolveSongPage(String(song));
    goToPage(page);
    recentSongsRef.current = [song, ...recentSongsRef.current.filter((s) => s !== song)].slice(0, 20);
    closeSearch();
  }, [goToPage, closeSearch]);

  const handleSearchSubmit = useCallback(() => {
    const firstSection = searchSections[0];
    if (firstSection && firstSection.data.length > 0) {
      handleSearchResultTap(firstSection.data[0].song);
    } else {
      const trimmed = searchText.trim();
      if (trimmed && /^\d+$/.test(trimmed)) {
        goToPage(resolveSongPage(trimmed));
        closeSearch();
      }
    }
  }, [searchSections, searchText, goToPage, closeSearch, handleSearchResultTap]);

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
  const isSmallScreen = Math.min(width, height) < 600;

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
        // rowH = cell content height + top/bottom margins (GAP/2 each = GAP total)
        const rowH = imgH + labelH + GAP;

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
              getItemLayout={(_, index) => ({
                length: rowH,
                offset: GAP / 2 + rowH * Math.floor(index / gridCols),
                index,
              })}
              renderItem={({ item }) => {
                const thumb = THUMB_ASSETS[item.page - 1];
                return (
                  <TouchableOpacity
                    style={{ width: cellW, margin: GAP / 2, backgroundColor: "#111", borderRadius: 6, overflow: "hidden" }}
                    onPress={() => handleGridSongTap(item.song)}
                    activeOpacity={0.7}
                  >
                    {thumb?.source ? (
                      <Image source={thumb.source} style={{ width: cellW, height: imgH }} resizeMode="cover" />
                    ) : (
                      <View style={{ width: cellW, height: imgH, backgroundColor: "#222" }} />
                    )}
                    <View style={{ height: labelH, backgroundColor: "#111", justifyContent: "center", alignItems: "center" }}>
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
          <View style={[styles.searchOverlay, { bottom: keyboardHeight }]}>
            <TouchableWithoutFeedback>
              <View style={styles.searchContainer}>
                <View style={styles.searchBar}>
                  <TextInput
                    ref={searchInputRef}
                    style={styles.searchInput}
                    value={searchText}
                    onChangeText={setSearchText}
                    onSubmitEditing={handleSearchSubmit}
                    placeholder="Buscar canción, palabra, parte de misa..."
                    placeholderTextColor="rgba(255,255,255,0.4)"
                    keyboardType="default"
                    returnKeyType="go"
                    autoCorrect={false}
                    autoCapitalize="none"
                    maxLength={40}
                    selectTextOnFocus
                  />
                  <TouchableOpacity style={styles.searchGoBtn} onPress={handleSearchSubmit} activeOpacity={0.7}>
                    <Text style={styles.searchGoBtnText}>Ir</Text>
                  </TouchableOpacity>
                </View>
                {searchSections.length > 0 && (
                  <SectionList
                    sections={searchSections}
                    keyExtractor={(item, i) => `sr-${item.song}-${i}`}
                    keyboardShouldPersistTaps="handled"
                    style={styles.searchResults}
                    contentContainerStyle={{ paddingBottom: 16 }}
                    stickySectionHeadersEnabled={false}
                    renderSectionHeader={({ section }) => (
                      <View style={styles.searchSectionHeader}>
                        <Text style={styles.searchSectionHeaderText}>{section.title}</Text>
                      </View>
                    )}
                    renderItem={({ item }) => {
                      const displayTitle = item.title || `Canción ${item.song}`;
                      const normalizedQ = normalizeText(searchText.trim());
                      const qWords = normalizedQ.split(/\s+/).filter(Boolean);
                      // Highlight matching words in title
                      const titleParts: { text: string; bold: boolean }[] = [];
                      if (qWords.length > 0 && !/^\d+$/.test(searchText.trim())) {
                        const normTitle = normalizeText(displayTitle);
                        let lastIdx = 0;
                        // Find first matching word position for highlight
                        const matchPositions: { start: number; end: number }[] = [];
                        for (const w of qWords) {
                          const idx = normTitle.indexOf(w);
                          if (idx !== -1) matchPositions.push({ start: idx, end: idx + w.length });
                        }
                        matchPositions.sort((a, b) => a.start - b.start);
                        for (const mp of matchPositions) {
                          if (mp.start < lastIdx) continue;
                          if (mp.start > lastIdx) titleParts.push({ text: displayTitle.slice(lastIdx, mp.start), bold: false });
                          titleParts.push({ text: displayTitle.slice(mp.start, mp.end), bold: true });
                          lastIdx = mp.end;
                        }
                        if (lastIdx < displayTitle.length) titleParts.push({ text: displayTitle.slice(lastIdx), bold: false });
                      }
                      const hasParts = titleParts.length > 0;
                      const snippet = SEARCHABLE_SONGS.find((s) => s.song === item.song)?.snippet ?? "";
                      return (
                        <TouchableOpacity
                          style={styles.searchResultRow}
                          onPress={() => handleSearchResultTap(item.song)}
                          activeOpacity={0.6}
                        >
                          <Text style={styles.searchResultNum}>{item.song}</Text>
                          <View style={styles.searchResultBody}>
                            <Text style={styles.searchResultTitle} numberOfLines={1}>
                              {hasParts ? titleParts.map((p, i) => (
                                <Text key={i} style={p.bold ? styles.searchHighlight : undefined}>{p.text}</Text>
                              )) : displayTitle}
                            </Text>
                            {!!snippet && (
                              <Text style={styles.searchResultSnippet} numberOfLines={2}>{"Letra: " + snippet}</Text>
                            )}
                          </View>
                          <Text style={styles.searchResultPage}>p.{item.page}</Text>
                        </TouchableOpacity>
                      );
                    }}
                  />
                )}
                {searchText.trim().length > 0 && searchSections.length === 0 && (
                  <View style={styles.searchEmpty}>
                    <Text style={styles.searchEmptyText}>Sin resultados</Text>
                  </View>
                )}
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

      {/* Director search button — top-right, same size as nav trigger */}
      {syncRole === "director" && (
        <TouchableOpacity
          style={[styles.cornerButton, styles.searchTrigger, isSmallScreen && styles.searchTriggerSmall]}
          onPress={openSearch}
          activeOpacity={0.75}
          hitSlop={{ top: 12, bottom: 12, left: 12, right: 12 }}
        >
          <Text style={styles.searchTriggerIcon}>⌕</Text>
        </TouchableOpacity>
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

      {/* ── Follower connection banner (slide-down, swipe-up to dismiss) ── */}
      {followerNotice && (
        <Animated.View
          style={[styles.noticeBanner, { transform: [{ translateY: followerBannerAnim }] }]}
          {...followerBannerPan.panHandlers}
        >
          <TouchableWithoutFeedback onPress={hideFollowerBanner}>
            <View style={styles.noticeBannerInner}>
              <Text style={styles.noticeBannerEmoji}>📡</Text>
              <View style={styles.noticeBannerText}>
                <Text style={styles.noticeBannerTitle}>Conectado al director</Text>
                <Text style={styles.noticeBannerBody}>Tu iPad seguirá automáticamente los cambios de página.</Text>
              </View>
            </View>
          </TouchableWithoutFeedback>
        </Animated.View>
      )}
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
  searchTrigger: { right: 1.25 },
  searchTriggerSmall: { paddingHorizontal: 14, paddingVertical: 10 },
  searchTriggerIcon: { fontSize: 32, color: "#7ec8f7", lineHeight: 38 },

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
  // bottom is overridden inline with keyboardHeight so results never go under keyboard
  searchOverlay: {
    position: "absolute",
    top: 0, left: 0, right: 0, bottom: 0,
    backgroundColor: "rgba(0,0,0,0.6)",
    justifyContent: "flex-start",
    paddingTop: 60,
    paddingHorizontal: 24,
    paddingBottom: 12,
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
  searchContainer: {
    flex: 1,
    // Ensures SectionList inside can grow to fill available space above keyboard
    overflow: "hidden",
  },
  searchGoBtn: {
    backgroundColor: "#3b82f6",
    borderRadius: 10,
    paddingHorizontal: 20,
    paddingVertical: 10,
  },
  searchGoBtnText: { color: "#fff", fontSize: 18, fontWeight: "700" },
  searchResults: {
    backgroundColor: "rgba(20,20,40,0.97)",
    borderRadius: 12,
    marginTop: 6,
    flex: 1,
  },
  searchSectionHeader: {
    paddingHorizontal: 16,
    paddingTop: 14,
    paddingBottom: 6,
  },
  searchSectionHeaderText: {
    fontSize: 12,
    fontWeight: "700",
    color: "rgba(255,255,255,0.45)",
    textTransform: "uppercase",
    letterSpacing: 0.8,
  },
  searchResultRow: {
    flexDirection: "row",
    alignItems: "center",
    minHeight: 48,
    paddingVertical: 12,
    paddingHorizontal: 16,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(255,255,255,0.08)",
    gap: 12,
  },
  searchHighlight: {
    color: "#7ec8f7",
    fontWeight: "700",
  },
  searchResultNum: {
    fontSize: 18,
    fontWeight: "800",
    color: "#3b82f6",
    width: 48,
    fontVariant: ["tabular-nums"] as any,
  },
  searchResultBody: {
    flex: 1,
    gap: 2,
  },
  searchResultTitle: {
    fontSize: 16,
    color: "#fff",
  },
  searchResultSnippet: {
    fontSize: 13,
    color: "rgba(255,255,255,0.45)",
    fontStyle: "italic",
  },
  searchResultPage: {
    fontSize: 13,
    color: "rgba(255,255,255,0.35)",
  },
  searchEmpty: {
    backgroundColor: "rgba(20,20,40,0.97)",
    borderRadius: 12,
    marginTop: 6,
    paddingVertical: 24,
    alignItems: "center",
  },
  searchEmptyText: {
    color: "rgba(255,255,255,0.35)",
    fontSize: 15,
  },

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

  // Follower notification banner
  noticeBanner: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    zIndex: 999,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.35,
    shadowRadius: 10,
    elevation: 20,
  },
  noticeBannerInner: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: "rgba(20,30,60,0.97)",
    margin: 10,
    borderRadius: 16,
    paddingVertical: 14,
    paddingHorizontal: 18,
    gap: 14,
  },
  noticeBannerEmoji: { fontSize: 28 },
  noticeBannerText: { flex: 1 },
  noticeBannerTitle: { fontSize: 15, fontWeight: "700", color: "#fff", marginBottom: 2 },
  noticeBannerBody: { fontSize: 13, color: "rgba(255,255,255,0.65)", lineHeight: 18 },

  // Small-screen (iPhone) director bar overrides
  directorBarSmall: { gap: 4 },
  dirBtnSmall: { paddingHorizontal: 9, paddingVertical: 7 },
  dirBtnIconSmall: { fontSize: 20, lineHeight: 24 },
  gridDensityBadgeSmall: { fontSize: 12, lineHeight: 24 },
});
