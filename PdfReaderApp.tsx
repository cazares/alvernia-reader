import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import AsyncStorage from "@react-native-async-storage/async-storage";
import {
  ActivityIndicator,
  Alert,
  Animated,
  Dimensions,
  FlatList,
  Image,
  InputAccessoryView,
  Keyboard,
  Modal,
  NativeModules,
  PanResponder,
  Pressable,
  SectionList,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  TouchableWithoutFeedback,
  View,
  type NativeSyntheticEvent,
  type KeyboardEvent,
  type ListRenderItemInfo,
  type NativeScrollEvent,
  type ViewabilityConfig,
  type ViewToken,
} from "react-native";
import * as Haptics from "expo-haptics";
import { useKeepAwake } from "expo-keep-awake";

import { ALVERNIA_MANUAL_2_SONG_INDEX } from "./src/alverniaManual2SongIndex";
import {
  addNearbyDirectorSyncListener,
  isNearbyDirectorSyncAvailable,
  resetNearbyDirectorSync,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
  stopNearbyDirectorSync,
} from "./src/nearbyDirectorSync";
import { OFFLINE_WEB_BUNDLE_ASSETS } from "./src/offlineWebBundle";
import { BOOKS, NON_STANDARD_BOOK_IDS, STORAGE_KEYS, clearAllBookState, getBook, validateOfflineBookAssets, type AppMode, type BookId } from "./src/offlineBooks";
// @ts-ignore — Metro resolves JSON fine
import SONG_TITLES from "./assets/offline-web/song-titles.json";
// @ts-ignore
import SONG_SEARCH_INDEX from "./assets/offline-web/song-search-index.json";
// @ts-ignore
import PAGES_JSON from "./assets/offline-web/pages.json";
// @ts-ignore — Metro resolves JSON fine
import VERSION_INFO from "./version.json";

const STANDARD_START_PAGE = 2;
const DIRECTOR_SESSION = "1234"; // fixed session — only one director per session
const SONG_MODAL_INPUT_MAX_LENGTH = 10;
const VISIBLE_BUILD_LABEL = `${VERSION_INFO.baseVersion}.${VERSION_INFO.buildNumber}`;

const normalizeDirectorDeviceName = (value: string): string => {
  let v = value || "";
  try {
    v = v.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  } catch {
    // Hermes builds may lack String.prototype.normalize; fall back to best-effort.
  }
  return v.toLowerCase().replace(/[^a-z0-9]/g, "");
};

const CHOIR_STANDARD_ACCESS = new Map<string, string>([
  ["8304699366", "Celia"],
  ["8305156458", "Yvonne"],
  ["8304699781", "Laura B"],
  ["8302128096", "Fredy"],
  ["8303130470", "Braulio (Original)"],
  ["8307197000", "Braulio (Personal)"],
  ["8307340943", "Catalina"],
  ["8307193848", "Rita"],
  ["8304883005", "Marisol"],
  ["8307655103", "Michelle"],
  ["8307197547", "Jesus"],
  ["83078840", "Hector y Adrian"],
]);

const DIRECTOR_ACCESS_CODES = new Set(["8303130470", "8307197000", "8771178844", "1234"]);

const normalizeAccessCode = (value: string): string => String(value || "").replace(/[^0-9]/g, "");

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

// Spanish text normalizer — removes tokens that are clearly not Spanish words
// (no vowels, contain digits, too short without being common words).
const SPANISH_KEEP_SHORT = new Set(["y","a","o","e","u","de","la","el","en","es","al","lo","le","su","no","si","mi","tu","se","me","te","yo","ya","he","oh","di","fe","ha","vi","os","un","ay"]);
const cleanSpanishText = (text: string): string => {
  const tokens = text.split(/\s+/).filter((tok) => {
    const bare = tok.replace(/[^a-záéíóúüñA-ZÁÉÍÓÚÜÑ]/g, "");
    if (!bare) return false;
    if (/^\d+[.,]?$/.test(tok)) return false; // purely numeric token (verse numbers, page refs)
    if (!/[aeiouáéíóúü]/i.test(bare)) return false; // no vowels — chord/noise
    if (bare.length <= 2 && !SPANISH_KEEP_SHORT.has(bare.toLowerCase())) return false;
    return true;
  });
  const result = tokens.join(" ").trim();
  if (!result) return "";
  return result.charAt(0).toUpperCase() + result.slice(1);
};

// Extract a lyric snippet from raw OCR text.
// Starts at 30% to skip title/metadata. Strips chord tokens, stage directions,
// and any line that is essentially just the song title repeated.
// Rule 1 — OCR text corrections applied before snippet extraction
const fixOcrText = (text: string): string =>
  text
    .replace(/\bSe[fh]i?or\b/g, "Señor")   // Sefor, Sefior, Sehor → Señor
    .replace(/\bSeAO[PR]\b/gi, "Señor")     // SeAOP, SeAOR → Señor
    .replace(/\bSeñof\b/g, "Señor")
    .replace(/\bJe sus\b/gi, "Jesús")
    .replace(/\bMarí a\b/g, "María")
    .replace(/\bcoraz[oó]6n\b/gi, "corazón")
    .replace(/\bcancién\b/gi, "canción");

const extractLyricSnippet = (raw: string, title: string): string => {
  const corrected = fixOcrText(raw);
  const normTitle = normalizeText(title).replace(/[^a-z0-9 ]/g, "").trim();
  const start = Math.floor(corrected.length * 0.30);
  const slice = corrected.slice(start);
  const lines = slice.split("\n");

  const chordToken = /\b[A-G][#b]?(m|maj|sus|dim|aug|add)?[0-9]{0,2}(\/[A-G][#b]?)?\b/g;
  // Rule 2 — skip entire lines that are performance/stage directions
  const skipLine = /^\s*(intro|coro|estrofa|puente|fin\s*$|bridge|verso|rev[\s\d]|capo|posible|\(coro\)|\(h\)|\(m\)|\(t\)|\(s\)|2\s*veces|dos\s*veces|veces\s*toda|solo\s*al\s*final|solista|guitarra|voces?|preparacion|opcional|para\s*arriba|para\s*abajo|inicio|al\s*coro|da\s*capo|bis\s*$|todos\s*$|hombres\s*$|mujeres\s*$|segunda\s*$|primera\s*$|segundo\s*$|repetir|cuantas\s*veces|con\s*guitarra|sin\s*guitarra|vuelta\s*$)/i;

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
  // Apply Spanish normalizer to remove OCR junk tokens
  return cleanSpanishText(collected.join(" · "));
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

// Ordered parts of the Mass for the Misa tab
const MISA_PARTS_ORDERED = [
  { label: "Entrada", keywords: ["entrada", "entrare", "venimos", "llegamos"] },
  { label: "Gloria", keywords: ["gloria"] },
  { label: "Aleluya", keywords: ["aleluya", "alleluia", "aleluia"] },
  { label: "Ofertorio", keywords: ["ofertorio", "ofrenda"] },
  { label: "Santo / Sanctus", keywords: ["santo", "sanctus", "hosanna"] },
  { label: "Cordero de Dios", keywords: ["cordero"] },
  { label: "Padre Nuestro", keywords: ["padre nuestro"] },
  { label: "Comunión", keywords: ["comunion"] },
  { label: "Paz", keywords: ["paz"] },
  { label: "Despedida / Envío", keywords: ["despedida", "envio"] },
];

// Pill colors — each Misa part gets a distinct hue cycling through this palette
const MISA_PILL_COLORS = [
  "#1a6b3a", // 1 Entrada       — forest green
  "#7b3f00", // 2 Gloria         — deep amber
  "#b5860a", // 3 Aleluya        — gold
  "#5c3317", // 4 Ofertorio      — dark brown
  "#1a3a6b", // 5 Santo          — navy
  "#6b1a1a", // 6 Cordero        — deep red
  "#1a5f6b", // 7 Padre Nuestro  — teal
  "#3a1a6b", // 8 Comunión       — deep violet
  "#2e6b1a", // 9 Paz            — mid green
  "#4a4a4a", // 10 Despedida     — charcoal
];

// Per-theme pill colors
const TEMA_PILL_COLORS: Record<string, string> = {
  alabanza:         "#7b4f00", // amber-brown
  fe:               "#1a4a7b", // steel blue
  eucaristia:       "#6b1a5f", // magenta-purple
  maria:            "#7b1a4a", // rose
  sanacion:         "#1a6b5a", // teal-green
  comunidad:        "#4a6b1a", // olive
  paz:              "#1a5f3a", // emerald
  mision:           "#6b4a1a", // sienna
  ninos:            "#1a6b6b", // cyan-teal
  bodas:            "#5f1a6b", // violet
  funerales:        "#3a3a3a", // dark grey
  bautismo:         "#1a3f6b", // cobalt
  confirmacion:     "#6b3a1a", // copper
  primera_comunion: "#5a1a6b", // deep purple
  procesion:        "#1a6b30", // jade
  santos:           "#6b5a1a", // ochre
};

// Per-temporada pill colors
const TEMPO_PILL_COLORS: Record<string, string> = {
  adviento:      "#4a1a6b", // deep purple
  navidad:       "#6b1a1a", // crimson
  cuaresma:      "#5a3a00", // dark bronze
  resurreccion:  "#1a6b2a", // bright green
  espiritu_santo:"#6b3a00", // burnt orange
};

const TIEMPO_GROUPS = [
  { id: "adviento",      label: "Adviento" },
  { id: "navidad",       label: "Navidad" },
  { id: "cuaresma",      label: "Cuaresma / Semana Santa" },
  { id: "resurreccion",  label: "Pascua / Resurrección" },
  { id: "espiritu_santo",label: "Pentecostés / Espíritu Santo" },
];

const TEMAS_EXCLUDED = new Set(["misa","entrada","envio","eucaristia","adviento","navidad","cuaresma","resurreccion","espiritu_santo"]);

// Static song sets per category (unordered — will be sorted inside component via sortSongs)
const MISA_TAB_SONGS_BY_PART = MISA_PARTS_ORDERED.map((part, i) => ({
  title: `Parte de Misa (${i + 1} de ${MISA_PARTS_ORDERED.length}): ${part.label}`,
  songs: SEARCHABLE_SONGS.filter((s) => part.keywords.some((kw) => s.normalized.includes(kw))).slice(0, 20),
})).filter((s) => s.songs.length > 0);

const TIEMPO_TAB_SONGS_BY_GROUP = TIEMPO_GROUPS.map((g) => ({
  title: g.label,
  id: g.id,
  songs: SEARCHABLE_SONGS.filter((s) => s.themes.includes(g.id)).slice(0, 20),
})).filter((s) => s.songs.length > 0);

const TEMAS_TAB_SONGS_BY_GROUP = Object.entries(THEME_LABELS)
  .filter(([id]) => !TEMAS_EXCLUDED.has(id))
  .map(([id, label]) => ({
    title: label,
    id,
    songs: SEARCHABLE_SONGS.filter((s) => s.themes.includes(id)).slice(0, 20),
  })).filter((s) => s.songs.length > 0);

type SearchSortMode = "best" | "az" | "number";

// Helper: sort a song array by sortMode
// For "best": score = title full match (10) + per-word title hits (3 each) + body occurrences (1 each), tiebreak by song#
function sortSongs(
  arr: typeof SEARCHABLE_SONGS,
  mode: SearchSortMode,
  queryWords: string[] = [],
): typeof SEARCHABLE_SONGS {
  const copy = [...arr];
  if (mode === "az") {
    copy.sort((a, b) => normalizeText(a.title).localeCompare(normalizeText(b.title)));
  } else if (mode === "number") {
    copy.sort((a, b) => a.song - b.song);
  } else if (mode === "best" && queryWords.length > 0) {
    const score = (s: typeof SEARCHABLE_SONGS[0]) => {
      const t = normalizeText(s.title);
      const body = s.normalized ?? "";
      const fullTitleMatch = queryWords.every((w) => t.includes(w)) ? 10 : 0;
      const wordTitleHits = queryWords.filter((w) => t.includes(w)).length * 3;
      const bodyHits = queryWords.reduce((acc, w) => {
        let count = 0;
        let idx = 0;
        while ((idx = body.indexOf(w, idx)) !== -1) { count++; idx += w.length; }
        return acc + count;
      }, 0);
      return fullTitleMatch + wordTitleHits + bodyHits;
    };
    copy.sort((a, b) => {
      const diff = score(b) - score(a);
      return diff !== 0 ? diff : a.song - b.song;
    });
  }
  return copy;
}

// Helper: find the Misa part label + index for a song
function getMisaPart(song: typeof SEARCHABLE_SONGS[0]): { label: string; idx: number } | null {
  for (let i = 0; i < MISA_PARTS_ORDERED.length; i++) {
    const part = MISA_PARTS_ORDERED[i];
    if (part.keywords.some((kw) => song.normalized.includes(kw))) {
      return { label: part.label, idx: i };
    }
  }
  return null;
}

const TIEMPO_IDS = new Set(TIEMPO_GROUPS.map((g) => g.id));
const MISA_CATEGORY_IDS = new Set(["misa", "entrada", "envio", "eucaristia"]);

// Which theme IDs to surface in keyword search results (misa subgroups)
const MISA_SEARCH_KEYWORDS: Record<string, string> = {
  gloria: "Gloria", santo: "Santo / Sanctus", sanctus: "Santo / Sanctus",
  aleluya: "Aleluya", alleluia: "Aleluya", cordero: "Cordero de Dios",
  piedad: "Piedad / Kyrie", kyrie: "Piedad / Kyrie",
  ofertorio: "Ofertorio", ofrenda: "Ofertorio", comunion: "Comunión",
};

const resolveSongPage = (input: string, totalPages: number, songToPage: Map<number, number>, sortedSongs: typeof SORTED_SONGS): number => {
  const n = parseInt(input, 10);
  if (!Number.isInteger(n) || n <= 0) return 1;
  const exact = songToPage.get(n);
  if (exact !== undefined) return exact;
  // Fallback behavior mirrors standard: clamp into the nearest next song, else last page.
  // For non-standard books (scanned PDFs), songToPage may be empty; we treat input as a page number.
  if (!sortedSongs.length) return Math.max(1, Math.min(n, totalPages));
  if (n < (sortedSongs[0]?.song ?? 1)) return 1;
  if (n > (sortedSongs[sortedSongs.length - 1]?.song ?? totalPages)) return totalPages;
  const next = sortedSongs.find((s) => s.song >= n);
  return next ? next.page : totalPages;
};

const buildPageAssets = (assets: Record<string, number>, totalPages: number) =>
  Array.from({ length: totalPages }, (_, i) => {
    const pageNum = i + 1;
    const key = `pages/page-${String(pageNum).padStart(3, "0")}.jpg`;
    return { page: pageNum, source: assets[key] };
  });

const buildThumbAssets = (assets: Record<string, number>, totalPages: number) =>
  Array.from({ length: totalPages }, (_, i) => {
    const pageNum = i + 1;
    const key = `thumbs/thumb-${String(pageNum).padStart(3, "0")}.jpg`;
    return { page: pageNum, source: assets[key] };
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

// ── Song numpad ───────────────────────────────────────────────────────────────
function SongNumpad({
  onDigit,
  onBackspace,
  onGo,
  goDisabled,
}: {
  onDigit: (d: string) => void;
  onBackspace: () => void;
  onGo: () => void;
  goDisabled: boolean;
}) {
  const rows: (string | null)[][] = [
    ["1", "2", "3"],
    ["4", "5", "6"],
    ["7", "8", "9"],
    [null, "0", "⌫"],
  ];
  const [pressedKey, setPressedKey] = useState<string | null>(null);
  const repeatDelayRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const repeatIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopBackspaceRepeat = useCallback(() => {
    if (repeatDelayRef.current) {
      clearTimeout(repeatDelayRef.current);
      repeatDelayRef.current = null;
    }
    if (repeatIntervalRef.current) {
      clearInterval(repeatIntervalRef.current);
      repeatIntervalRef.current = null;
    }
  }, []);

  useEffect(() => stopBackspaceRepeat, [stopBackspaceRepeat]);

  const fireHaptic = useCallback(() => {
    Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => {});
  }, []);

  const handlePressIn = useCallback((key: string, isBack: boolean, disabled: boolean) => {
    if (disabled) return;
    setPressedKey(key);
    fireHaptic();
    if (!isBack) return;
    onBackspace();
    stopBackspaceRepeat();
    repeatDelayRef.current = setTimeout(() => {
      repeatIntervalRef.current = setInterval(() => {
        onBackspace();
      }, 100);
    }, 500);
  }, [fireHaptic, onBackspace, stopBackspaceRepeat]);

  const handlePressOut = useCallback(() => {
    setPressedKey(null);
    stopBackspaceRepeat();
  }, [stopBackspaceRepeat]);

  return (
    <View style={numpadStyles.grid}>
      {rows.map((row, ri) => (
        <View key={ri} style={numpadStyles.row}>
          {row.map((key, ki) => {
            if (key === null) {
              return <View key={ki} style={numpadStyles.emptyKey} />;
            }
            const isGo = key === "Go";
            const isBack = key === "⌫";
            const disabled = isGo && goDisabled;
            return (
              <Pressable
                key={ki}
                style={[
                  numpadStyles.key,
                  isBack && numpadStyles.backKey,
                  isGo && numpadStyles.goKey,
                  disabled && numpadStyles.goKeyDisabled,
                  pressedKey === key && numpadStyles.keyPressed,
                  isBack && pressedKey === key && numpadStyles.backKeyPressed,
                  isGo && pressedKey === key && numpadStyles.goKeyPressed,
                ]}
                onPressIn={() => handlePressIn(key, isBack, disabled)}
                onPressOut={handlePressOut}
                onPress={() => {
                  if (isGo) { onGo(); }
                  else if (isBack) { return; }
                  else { onDigit(key); }
                }}
                disabled={disabled}
              >
                <Text style={[numpadStyles.keyText, isGo && numpadStyles.goKeyText]}>
                  {key}
                </Text>
              </Pressable>
            );
          })}
        </View>
      ))}
    </View>
  );
}

const numpadStyles = StyleSheet.create({
  grid: {
    marginTop: 16,
    gap: 12,
  },
  row: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "center",
  },
  key: {
    flex: 1,
    maxWidth: 108,
    minWidth: 72,
    height: 78,
    borderRadius: 12,
    backgroundColor: "#D1D5DB",
    alignItems: "center",
    justifyContent: "center",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.18,
    shadowRadius: 1.5,
    elevation: 2,
  },
  keyPressed: {
    backgroundColor: "#9CA3AF",
  },
  backKey: {
    // Subtle distinction from number keys (no bright blue / no heavy border).
    backgroundColor: "#E5E7EB",
    borderWidth: 1,
    borderColor: "rgba(0,0,0,0.06)",
  },
  backKeyPressed: {
    backgroundColor: "#D1D5DB",
  },
  emptyKey: {
    flex: 1,
    maxWidth: 108,
    minWidth: 72,
    height: 78,
  },
  goKey: {
    backgroundColor: "#3B82F6",
  },
  goKeyDisabled: {
    backgroundColor: "#93C5FD",
  },
  goKeyPressed: {
    backgroundColor: "#2563EB",
  },
  keyText: {
    fontSize: 33,
    fontWeight: "500",
    color: "#111827",
  },
  goKeyText: {
    fontSize: 18,
    fontWeight: "600",
    color: "#FFFFFF",
  },
});

// ── Main app ──────────────────────────────────────────────────────────────────
type SyncRole = "off" | "director" | "follower";

export default function App() {
  useKeepAwake("signovivo-reader");

  const listRef = useRef<FlatList>(null);
  const searchListRef = useRef<SectionList<any>>(null);
  const inputRef = useRef<TextInput>(null);
  const codeInputRef = useRef<TextInput>(null);
  const currentPageRef = useRef(STANDARD_START_PAGE);

  const [dims, setDims] = useState(() => Dimensions.get("window"));
  const [booted, setBooted] = useState(false);
  const [onboardingVisible, setOnboardingVisible] = useState(false);
  const [onboardingCode, setOnboardingCode] = useState("");
  const onboardingSubmittingRef = useRef(false);
  const standardLockedRef = useRef(false);
  const standardAccessNameRef = useRef<string | null>(null);
  const [mode, setMode] = useState<AppMode>("standard");
  const [activeBookId, setActiveBookId] = useState<BookId>("standard");
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
  const [searchTab, setSearchTab] = useState<"todas" | "misa" | "tiempo" | "temas" | "recientes">("todas");
  const [sortMode, setSortMode] = useState<SearchSortMode>("best");
  const [searchJumpDirection, setSearchJumpDirection] = useState<"up" | "down">("down");
  const [browseVisible, setBrowseVisible] = useState(false);
  const [browseTab, setBrowseTab] = useState<"todas" | "recientes">("todas");
  const [showSatellite, setShowSatellite] = useState(false);
  const [isSyncBootstrapped, setIsSyncBootstrapped] = useState(false);
  const [reconnectBusy, setReconnectBusy] = useState(false);
  const [reconnectMessage, setReconnectMessage] = useState("");
  const [isResettingApp, setIsResettingApp] = useState(false);
  const [resetCompleteVisible, setResetCompleteVisible] = useState(false);
  const [appResetKey, setAppResetKey] = useState(0);
  const [offlineAssetsError, setOfflineAssetsError] = useState<string | null>(null);
  const lastFollowerNoticeRef = useRef(0);
  const reconnectPressesRef = useRef<number[]>([]);
  const reconnectCancelledRef = useRef(false);
  const appResettingRef = useRef(false);
  const resetCompleteTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchInputRef = useRef<TextInput>(null);
  const syncRoleRef = useRef<SyncRole>("off");
  const recentSongsRef = useRef<number[]>([]);
  const pendingSyncPageRef = useRef<number | null>(null);
  const searchAccessoryId = "director-search-accessory";
  const syncAvailable = isNearbyDirectorSyncAvailable();
  const [, forceRerender] = useState(0);

  const activeBook = useMemo(() => getBook(activeBookId), [activeBookId]);
  const totalPages = activeBook.totalPages || 1;
  const startPage = mode === "standard" ? STANDARD_START_PAGE : 1;
  const pageAssets = useMemo(() => buildPageAssets(activeBook.assets, totalPages), [activeBook.assets, totalPages]);
  const thumbAssets = useMemo(() => buildThumbAssets(activeBook.assets, totalPages), [activeBook.assets, totalPages]);
  const isStandardMode = mode === "standard";

  // Safety: if bundled offline assets are missing/corrupt, fail closed with a recovery UI
  // instead of risking crashes or broken navigation.
  useEffect(() => {
    const validation = validateOfflineBookAssets(activeBook);
    if (validation.ok) {
      setOfflineAssetsError(null);
      return;
    }
    setOfflineAssetsError(
      `Faltan archivos offline del libro "${activeBook.title}". (${validation.sampleMissingKeys.join(", ")})`,
    );
  }, [activeBook]);

  // Per-book song index/search. Standard uses existing enriched index; non-standard may be empty (scanned PDFs).
  const songTitles = useMemo(() => (mode === "standard" ? (SONG_TITLES as any) : activeBook.songTitles) ?? {}, [mode, activeBook.songTitles]);
  const songSearchIndex = useMemo(() => {
    const raw = (mode === "standard" ? (SONG_SEARCH_INDEX as any) : activeBook.songSearchIndex) ?? [];
    // Some RN/Hermes builds can surface "iterator method is not callable" when a value has a bad @@iterator.
    // Avoid iterators by forcing a plain Array shape here.
    return Array.isArray(raw) ? raw : [];
  }, [mode, activeBook.songSearchIndex]);
  const bookSongToPage = useMemo(() => {
    if (mode === "standard") return SONG_TO_PAGE;
    const m = new Map<number, number>();
    // Avoid `for..of` to keep this resilient even if `songSearchIndex` has a broken iterator.
    for (let i = 0; i < songSearchIndex.length; i++) {
      const entry = songSearchIndex[i] as any;
      if (typeof entry?.song === "number" && typeof entry?.page === "number") m.set(entry.song, entry.page);
    }
    return m;
  }, [mode, songSearchIndex]);
  const bookSortedSongs = useMemo(() => {
    if (mode === "standard") return SORTED_SONGS;
    const copy: Array<{ song: number; page: number }> = [];
    // Avoid spreading iterators (Map#entries) to prevent "iterator method is not callable" crashes.
    bookSongToPage.forEach((page, song) => copy.push({ song, page }));
    copy.sort((a, b) => a.song - b.song);
    return copy;
  }, [mode, bookSongToPage]);

  const GRID_DENSITY = [2, 3, 4] as const;
  const gridCols = GRID_DENSITY[gridDensityIdx];

  const loadPersistedLaunchState = useCallback(async (isCancelled: () => boolean = () => false) => {
    try {
      const done = await AsyncStorage.getItem(STORAGE_KEYS.onboardingComplete);
      if (isCancelled()) return;
      if (done === "1") {
        const storedMode = (await AsyncStorage.getItem(STORAGE_KEYS.mode)) as AppMode | null;
        const storedBook = (await AsyncStorage.getItem(STORAGE_KEYS.activeBookId)) as BookId | null;
        const storedAccessName = await AsyncStorage.getItem(STORAGE_KEYS.standardAccessName);
        if (isCancelled()) return;
        // Sad path: onboardingComplete can be written even if mode fails to persist (AsyncStorage partial write).
        // Don't silently default to standard; force onboarding again so the user gets the right mode + UI.
        if (storedMode !== "standard" && storedMode !== "nonStandard") {
          setOnboardingVisible(true);
          setBooted(true);
          return;
        }
        standardLockedRef.current = storedMode === "standard" && !!storedAccessName;
        standardAccessNameRef.current = storedAccessName || null;
        const nextMode: AppMode = storedMode === "nonStandard" ? "nonStandard" : "standard";
        let nextBook: BookId = "standard";
        if (nextMode === "nonStandard") {
          if (storedBook && NON_STANDARD_BOOK_IDS.includes(storedBook)) nextBook = storedBook;
          else nextBook = NON_STANDARD_BOOK_IDS[Math.floor(Math.random() * NON_STANDARD_BOOK_IDS.length)]!;
        }
        currentPageRef.current = nextMode === "standard" ? STANDARD_START_PAGE : 1;
        setMode(nextMode);
        setActiveBookId(nextBook);
        setOnboardingVisible(false);
        setBooted(true);
      } else {
        // First launch — skip code prompt, boot directly as hymnal
        standardLockedRef.current = false;
        standardAccessNameRef.current = null;
        const firstBook: BookId = "hymns-4";
        await AsyncStorage.multiSet([
          [STORAGE_KEYS.onboardingComplete, "1"],
          [STORAGE_KEYS.mode, "nonStandard"],
          [STORAGE_KEYS.activeBookId, firstBook],
        ]).catch(() => {});
        await AsyncStorage.removeItem(STORAGE_KEYS.standardAccessName).catch(() => {});
        if (isCancelled()) return;
        currentPageRef.current = 1;
        setMode("nonStandard");
        setActiveBookId(firstBook);
        setOnboardingVisible(false);
        setBooted(true);
      }
    } catch {
      if (!isCancelled()) {
        setOnboardingVisible(true);
        setBooted(true);
      }
    }
  }, []);

  // Bootstrap persisted mode/book selection and onboarding completion.
  useEffect(() => {
    let cancelled = false;
    loadPersistedLaunchState(() => cancelled);
    return () => { cancelled = true; };
  }, [loadPersistedLaunchState]);

  if (offlineAssetsError) {
    return (
      <View style={[styles.screen, { padding: 24, justifyContent: "center" }]}>
        <Text style={{ color: "#fff", fontSize: 22, fontWeight: "700", marginBottom: 12 }}>
          Error de archivos offline
        </Text>
        <Text style={{ color: "#ddd", fontSize: 16, lineHeight: 22, marginBottom: 18 }}>
          {offlineAssetsError}
        </Text>
        <Text style={{ color: "#bbb", fontSize: 14, lineHeight: 20, marginBottom: 18 }}>
          Esto normalmente significa que la instalación está incompleta. Vuelve a instalar la app desde TestFlight/App
          Store o vuelve a compilar e instalar el build.
        </Text>
        <TouchableOpacity
          style={[styles.recoveryButton, { alignSelf: "flex-start" }]}
          onPress={() => {
            clearAllBookState()
              .catch(() => {})
              .finally(() => {
                const book: BookId = "hymns-4";
                AsyncStorage.multiSet([
                  [STORAGE_KEYS.onboardingComplete, "1"],
                  [STORAGE_KEYS.mode, "nonStandard"],
                  [STORAGE_KEYS.activeBookId, book],
                ]).catch(() => {});
                AsyncStorage.removeItem(STORAGE_KEYS.standardAccessName).catch(() => {});
                setAppResetKey((v) => v + 1);
                setActiveBookId(book);
                setMode("nonStandard");
                setOnboardingVisible(false);
                forceRerender((x) => x + 1);
              });
          }}
        >
          <Text style={styles.recoveryButtonText}>Restablecer ajustes</Text>
        </TouchableOpacity>
        <Text style={{ color: "#666", marginTop: 18, fontSize: 12 }}>{VISIBLE_BUILD_LABEL}</Text>
      </View>
    );
  }

  const enableStandardMode = useCallback(async (name: string) => {
    standardLockedRef.current = true;
    standardAccessNameRef.current = name;
    await AsyncStorage.multiSet([
      [STORAGE_KEYS.onboardingComplete, "1"],
      [STORAGE_KEYS.standardAccessName, name],
      [STORAGE_KEYS.mode, "standard"],
      [STORAGE_KEYS.activeBookId, "standard"],
    ]);
    setMode("standard");
    setActiveBookId("standard");
    setOnboardingVisible(false);
    setSongModal(false);
    currentPageRef.current = STANDARD_START_PAGE;
    setTimeout(() => {
      listRef.current?.scrollToIndex({ index: STANDARD_START_PAGE - 1, animated: false });
    }, 60);
  }, []);

  const enableNonStandardMode = useCallback(async () => {
    standardLockedRef.current = false;
    standardAccessNameRef.current = null;
    const nextBook: BookId = "hymns-4";
    await AsyncStorage.multiSet([
      [STORAGE_KEYS.onboardingComplete, "1"],
      [STORAGE_KEYS.mode, "nonStandard"],
      [STORAGE_KEYS.activeBookId, nextBook],
    ]);
    await AsyncStorage.removeItem(STORAGE_KEYS.standardAccessName);
    setMode("nonStandard");
    setActiveBookId(nextBook);
    setOnboardingVisible(false);
  }, []);

  const handleOnboardingContinue = useCallback(async () => {
    if (onboardingSubmittingRef.current) return;
    onboardingSubmittingRef.current = true;
    try {
      const code = normalizeAccessCode(onboardingCode);
      if (!code) {
        await enableNonStandardMode();
      } else {
        const name = CHOIR_STANDARD_ACCESS.get(code);
        if (!name) {
          Alert.alert("Código no reconocido", "Revisa el número e intenta de nuevo, o continúa como himnario.");
          return;
        }
        await enableStandardMode(name);
      }
    } catch {
      Alert.alert(
        "No se pudo guardar",
        "La app seguirá funcionando con estos ajustes por ahora. Intenta de nuevo si vuelve a aparecer.",
      );
    } finally {
      onboardingSubmittingRef.current = false;
    }
  }, [enableNonStandardMode, enableStandardMode, onboardingCode]);

  // Orientation handling
  useEffect(() => {
    const sub = Dimensions.addEventListener("change", ({ window }) => {
      setDims(window);
      setTimeout(() => {
        try {
          listRef.current?.scrollToIndex({ index: currentPageRef.current - 1, animated: false });
        } catch {
          // Ignore stale measurement scroll errors during rotations.
        }
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

  const goToPage = useCallback((page: number) => {
    const clamped = Math.max(1, Math.min(page, totalPages));
    currentPageRef.current = clamped;
    listRef.current?.scrollToIndex({ index: clamped - 1, animated: false });
  }, [totalPages]);

  useEffect(() => {
    if (pendingSyncPageRef.current === null) return;
    const page = pendingSyncPageRef.current;
    pendingSyncPageRef.current = null;
    setTimeout(() => goToPage(page), 60);
  }, [activeBookId, goToPage, mode, totalPages]);

  // Keep ref in sync for use inside event callbacks
  useEffect(() => { syncRoleRef.current = syncRole; }, [syncRole]);

  const bootstrapNearbySyncRole = useCallback(async (isCancelled: () => boolean = () => false) => {
    if (!syncAvailable) {
      setSyncRole("off");
      setIsSyncBootstrapped(false);
      return;
    }

    const rawName = await NativeModules.DirectorSyncModule?.getDeviceName?.().catch(() => "");
    if (isCancelled()) return;

    const normalizedName = normalizeDirectorDeviceName(rawName || "");
    const isBrauMaster = normalizedName === "braumaster";

    if (isBrauMaster) {
      try {
        await startNearbyDirector(DIRECTOR_SESSION);
        if (!isCancelled()) setSyncRole("director");
      } catch {
        await startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
        if (!isCancelled()) setSyncRole("follower");
      }
    } else {
      await startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
      if (!isCancelled()) setSyncRole("follower");
    }

    if (!isCancelled()) setIsSyncBootstrapped(true);
  }, [syncAvailable]);

  // Sync listener
  useEffect(() => {
    if (!syncAvailable) return;
    const sub = addNearbyDirectorSyncListener((event: any) => {
      if (event.type === "page" && typeof event.page === "number") {
        const incomingMode: AppMode | null =
          event.mode === "standard" || event.mode === "nonStandard" ? event.mode : null;
        const incomingBookId: BookId | null =
          incomingMode === "standard"
            ? "standard"
            : incomingMode === "nonStandard" && NON_STANDARD_BOOK_IDS.includes(event.bookId)
              ? event.bookId
              : null;

        if (standardLockedRef.current && (incomingMode !== "standard" || incomingBookId !== "standard")) {
          return;
        }

        if (incomingMode && incomingBookId && (incomingMode !== mode || incomingBookId !== activeBookId)) {
          pendingSyncPageRef.current = event.page;
          setMode(incomingMode);
          setActiveBookId(incomingBookId);
          AsyncStorage.multiSet([
            [STORAGE_KEYS.mode, incomingMode],
            [STORAGE_KEYS.activeBookId, incomingBookId],
          ]).catch(() => {});
          return;
        }
        goToPage(event.page);
      } else if (event.type === "error" && event.code === "DIRECTOR_CONFLICT") {
        // A newer director took over — Swift already cleaned up transport.
        setSyncRole("follower");
        startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
      } else if (event.type === "state" && event.status === "connected" && syncRoleRef.current === "follower") {
        // Follower just connected to director — show satellite emoji briefly (30s cooldown)
        const now = Date.now();
        if (now - lastFollowerNoticeRef.current > 30_000) {
          lastFollowerNoticeRef.current = now;
          setShowSatellite(true);
          setTimeout(() => setShowSatellite(false), 3000);
        }
      }
    });
    return () => sub.remove();
  }, [activeBookId, goToPage, mode, syncAvailable]);

  // Bootstrap sync role once nearby sync is available.
  // Brau MASTER should become director automatically; everyone else starts as follower.
  useEffect(() => {
    if (!syncAvailable) return;
    let cancelled = false;
    bootstrapNearbySyncRole(() => cancelled);

    return () => {
      cancelled = true;
      stopNearbyDirectorSync().catch(() => {});
    };
  }, [bootstrapNearbySyncRole, syncAvailable]);

  // Restore last page when entering a non-standard book (per-book saved state).
  useEffect(() => {
    if (!booted) return;
    if (onboardingVisible) return;
    if (mode !== "nonStandard") {
      currentPageRef.current = startPage;
      return;
    }
    let cancelled = false;
    const restore = async () => {
      const last = await AsyncStorage.getItem(`${STORAGE_KEYS.lastPagePrefix}${activeBookId}`).catch(() => null);
      if (cancelled) return;
      const p = Math.max(1, Math.min(parseInt(last || "1", 10) || 1, totalPages));
      setTimeout(() => goToPage(p), 60);
    };
    restore();
    return () => { cancelled = true; };
  }, [booted, onboardingVisible, mode, activeBookId, totalPages, startPage, goToPage]);

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

  const clearVolatileRuntimeState = useCallback(() => {
    Keyboard.dismiss();
    reconnectCancelledRef.current = true;
    reconnectPressesRef.current = [];
    pendingSyncPageRef.current = null;
    recentSongsRef.current = [];
    lastFollowerNoticeRef.current = 0;
    longPressedRef.current = false;
    currentPageRef.current = STANDARD_START_PAGE;
    setSongInput("");
    setCodeInput("");
    setKeyboardHeight(0);
    setSearchVisible(false);
    setSearchText("");
    setSearchTab("todas");
    setSortMode("best");
    setSearchJumpDirection("down");
    setBrowseVisible(false);
    setBrowseTab("todas");
    setGridVisible(false);
    setGridDensityIdx(0);
    setSongModal(false);
    setSyncModal(false);
  }, []);

  const performSoftAppReset = useCallback(async () => {
    if (appResettingRef.current) return;
    appResettingRef.current = true;
    reconnectCancelledRef.current = true;
    if (resetCompleteTimerRef.current) {
      clearTimeout(resetCompleteTimerRef.current);
      resetCompleteTimerRef.current = null;
    }
    setResetCompleteVisible(false);
    setIsResettingApp(true);
    setReconnectBusy(false);
    setReconnectMessage("");
    setSyncRole("off");
    setIsSyncBootstrapped(false);

    try {
      await resetNearbyDirectorSync().catch(() => stopNearbyDirectorSync().catch(() => null));
      clearVolatileRuntimeState();
      await loadPersistedLaunchState();
      setAppResetKey((v) => v + 1);
      await new Promise((resolve) => setTimeout(resolve, 120));
      await bootstrapNearbySyncRole();
      setResetCompleteVisible(true);
      resetCompleteTimerRef.current = setTimeout(() => {
        setResetCompleteVisible(false);
        resetCompleteTimerRef.current = null;
      }, 2200);
    } finally {
      setIsResettingApp(false);
      appResettingRef.current = false;
    }
  }, [bootstrapNearbySyncRole, clearVolatileRuntimeState, loadPersistedLaunchState]);

  const confirmResetApp = useCallback((source: "manual" | "reconnect" = "manual") => {
    if (isResettingApp) return;
    const title = source === "reconnect" ? "Volver a conectar" : "Restablecer app";
    const message = source === "reconnect"
      ? "Si todavia no conecta, podemos restablecer la app ahora mismo. Esto vuelve a empezar la conexion sin borrar cantos ni ajustes."
      : "Esto vuelve a empezar la app y la conexion desde cero. Tus cantos, ajustes y contenido no se borran.";
    Alert.alert(
      title,
      message,
      [
        { text: "Cancelar", style: "cancel" },
        { text: source === "reconnect" ? "Restablecer ahora" : "Restablecer app", onPress: () => { performSoftAppReset().catch(() => {}); } },
      ],
    );
  }, [isResettingApp, performSoftAppReset]);

  const handleSongSubmit = useCallback(async () => {
    const trimmed = songInput.trim();
    closeSongModal();
    if (!trimmed) return;
    if (trimmed === "744668486") {
      await performSoftAppReset();
      return;
    }
    const standardAccessName = CHOIR_STANDARD_ACCESS.get(normalizeAccessCode(trimmed));
    if (standardAccessName) {
      await enableStandardMode(standardAccessName);
      return;
    }
    const n = parseInt(trimmed, 10);
    if (n > 0) recentSongsRef.current = [n, ...recentSongsRef.current.filter(s => s !== n)].slice(0, 20);
    const page = resolveSongPage(trimmed, totalPages, bookSongToPage, bookSortedSongs as any);
    goToPage(page);
  }, [songInput, goToPage, closeSongModal, totalPages, bookSongToPage, bookSortedSongs, performSoftAppReset, enableStandardMode]);

  const switchBook = useCallback(async (nextId: BookId) => {
    if (mode !== "nonStandard") return;
    if (nextId === activeBookId) return;
    const curPage = currentPageRef.current;
    await AsyncStorage.setItem(`${STORAGE_KEYS.lastPagePrefix}${activeBookId}`, String(curPage));
    await AsyncStorage.setItem(STORAGE_KEYS.activeBookId, nextId);
    setActiveBookId(nextId);
    const last = await AsyncStorage.getItem(`${STORAGE_KEYS.lastPagePrefix}${nextId}`);
    const nextPage = Math.max(1, Math.min(parseInt(last || "1", 10) || 1, getBook(nextId).totalPages || 1));
    setTimeout(() => goToPage(nextPage), 50);
  }, [mode, activeBookId, goToPage]);

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
    if (!DIRECTOR_ACCESS_CODES.has(normalizeAccessCode(codeInput))) {
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

  const showConnectivityHelp = useCallback(() => {
    Alert.alert(
      "Revisa la conexión",
      "Activa Bluetooth y Wi-Fi, acepta permisos de red local si iOS los pide, y mantente cerca del director.",
    );
  }, []);

  const handleReconnectPress = useCallback(async () => {
    if (syncRoleRef.current === "director") return;
    const now = Date.now();
    reconnectPressesRef.current = reconnectPressesRef.current.filter((t) => now - t <= 25_000);
    reconnectPressesRef.current.push(now);

    if (reconnectPressesRef.current.length >= 3) {
      reconnectPressesRef.current = [];
      confirmResetApp("reconnect");
      return;
    }

    if (!syncAvailable) {
      showConnectivityHelp();
      return;
    }

    reconnectCancelledRef.current = false;
    setReconnectMessage("Reconectando con el director...");
    setReconnectBusy(true);
    try {
      await stopNearbyDirectorSync().catch(() => {});
      if (reconnectCancelledRef.current) return;
      await startNearbyFollower(DIRECTOR_SESSION);
      if (reconnectCancelledRef.current) return;
      setSyncRole("follower");
      setReconnectMessage("Listo.");
      setTimeout(() => {
        if (!reconnectCancelledRef.current) setReconnectBusy(false);
      }, 350);
    } catch {
      if (!reconnectCancelledRef.current) {
        setReconnectBusy(false);
        showConnectivityHelp();
      }
    }
  }, [confirmResetApp, showConnectivityHelp, syncAvailable]);

  const cancelReconnect = useCallback(() => {
    reconnectCancelledRef.current = true;
    setReconnectBusy(false);
    setReconnectMessage("");
    startNearbyFollower(DIRECTOR_SESSION).catch(() => {});
  }, []);

  // Spotlight-style keyword search
  const openSearch = useCallback(() => {
    setSearchVisible(true);
    setBrowseVisible(false);
    setGridVisible(false);
    setSearchJumpDirection("down");
    setTimeout(() => searchInputRef.current?.focus(), 80);
  }, []);
  const closeSearch = useCallback(() => {
    Keyboard.dismiss();
    setSearchVisible(false);
    setSearchJumpDirection("down");
  }, []);

  const activeSearchables = useMemo((): typeof SEARCHABLE_SONGS => {
    if (isStandardMode) return SEARCHABLE_SONGS;
    if (songSearchIndex.length > 0) {
      return songSearchIndex
        .map((raw: any) => {
          const song = Number(raw?.song || 0);
          const page = Number(raw?.page || 0);
          const title = String(raw?.title || (songTitles as any)?.[String(song)] || `Canción ${song || "?"}`);
          const snippet = String(raw?.lyrics || "");
          return {
            song: Number.isFinite(song) ? song : 0,
            page: Number.isFinite(page) ? page : 1,
            title,
            normalized: normalizeText(`${song}. ${title}`),
            themes: [],
            snippet,
          };
        })
        .filter((s: any) => Number.isFinite(s.song) && s.song > 0);
    }
    const pages = Math.max(1, totalPages || 1);
    return Array.from({ length: pages }, (_, i) => {
      const page = i + 1;
      const title = (songTitles as Record<string, string>)[String(page)] || `Página ${page}`;
      return {
        song: page,
        page,
        title,
        normalized: normalizeText(title),
        themes: [],
        snippet: "",
      };
    }) as unknown as typeof SEARCHABLE_SONGS;
  }, [isStandardMode, totalPages, songTitles, songSearchIndex]);

  const activeSearchablesBySong = useMemo(() => {
    const m = new Map<number, typeof SEARCHABLE_SONGS[0]>();
    for (const s of activeSearchables) m.set(s.song, s);
    return m;
  }, [activeSearchables]);

  // Compute grouped search results on every keystroke
  const searchSections = useMemo(() => {
    const q = searchText.trim();
    if (!q) return [];
    const normalizedQ = normalizeText(q);
    const words = normalizedQ.split(/\s+/).filter(Boolean);
    const isNumeric = /^\d+$/.test(q);
    const sections: { title: string; data: { song: number; page: number; title: string }[] }[] = [];
    const seenSongs = new Set<number>();

    const addSection = (title: string, songs: typeof activeSearchables) => {
      const unique = songs.filter((s) => !seenSongs.has(s.song));
      if (unique.length === 0) return;
      const sorted = sortSongs(unique, sortMode, words);
      sorted.forEach((s) => seenSongs.add(s.song));
      sections.push({ title, data: sorted });
    };

    // 1) Numeric → exact + prefix match at top
    if (isNumeric) {
      const n = parseInt(q, 10);
      const exact = activeSearchablesBySong.get(n);
      if (exact) addSection("Canción", [exact]);
      const prefix = activeSearchables.filter((s) => s.song !== n && String(s.song).startsWith(q)).slice(0, 12);
      if (prefix.length > 0) addSection("Canciones", prefix);
      return sections;
    }

    const titleNorm = (s: typeof SEARCHABLE_SONGS[0]) => normalizeText(s.title);
    const matchesWords = (text: string) => words.every((w) => text.includes(w));

    // 2) Recents matching query (title or lyrics)
    const recentMatches = recentSongsRef.current
      .map((sn) => activeSearchablesBySong.get(sn))
      .filter((s): s is typeof SEARCHABLE_SONGS[0] =>
        !!s && (matchesWords(titleNorm(s)) || matchesWords(s.normalized)))
      .slice(0, 5);
    addSection("Recientes", recentMatches);

    // 3) Title matches
    const titleMatches = activeSearchables.filter((s) => matchesWords(titleNorm(s))).slice(0, 15);
    addSection("Canciones", titleMatches);

    // 4) Lyric-only matches (not already in title section)
    const lyricMatches = activeSearchables.filter((s) =>
      !seenSongs.has(s.song) && s.normalized && matchesWords(s.normalized)
    ).slice(0, 10);
    addSection("Letras", lyricMatches);

    // 5) Misa subgroup — match specific mass-part keywords in query
    const misaMatch = Object.entries(MISA_SEARCH_KEYWORDS).find(([kw]) =>
      normalizedQ.includes(kw) || kw.includes(normalizedQ)
    );
    if (misaMatch) {
      const [kw, label] = misaMatch;
      const partIdx = MISA_PARTS_ORDERED.findIndex((p) => p.label === label || p.keywords.includes(kw));
      const header = partIdx >= 0
        ? `Parte de Misa (${partIdx + 1} de ${MISA_PARTS_ORDERED.length}): ${label}`
        : `Parte de Misa: ${label}`;
      const misaSongs = activeSearchables.filter((s) =>
        !seenSongs.has(s.song) &&
        (titleNorm(s).includes(kw) || s.normalized.includes(kw))
      ).slice(0, 10);
      addSection(header, misaSongs);
    }

    // 6) Theme tag match — if query matches a theme label or ID, surface tagged songs
    const themeEntry = Object.entries(THEME_LABELS).find(([id, label]) => {
      const normLabel = normalizeText(label);
      return normalizedQ.includes(id) || id.includes(normalizedQ) ||
             normLabel.includes(normalizedQ) || normalizedQ.includes(normLabel);
    });
    if (themeEntry) {
      const [themeId, themeLabel] = themeEntry;
      const themeSongs = activeSearchables.filter((s) =>
        !seenSongs.has(s.song) && s.themes.includes(themeId)
      ).slice(0, 12);
      addSection(themeLabel, themeSongs);
    }

    return sections;
  }, [searchText, sortMode, activeSearchables, activeSearchablesBySong, isStandardMode]);

  // Sorted song list for the "Todas" tab
  const sortedTodas = useMemo(() => {
    const arr = [...activeSearchables];
    if (sortMode === "az") arr.sort((a, b) => normalizeText(a.title).localeCompare(normalizeText(b.title)));
    else if (sortMode === "number") arr.sort((a, b) => a.song - b.song);
    // "best" keeps natural order
    return arr;
  }, [sortMode, activeSearchables]);

  // Sorted tab sections (re-sorted whenever sortMode changes)
  const sortedMisaSections = useMemo(() => {
    const seenMisa = new Set<number>();
    const sections = MISA_TAB_SONGS_BY_PART.map((part) => {
      const data = sortSongs(part.songs.filter((s) => activeSearchablesBySong.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
      data.forEach((s) => seenMisa.add(s.song));
      return { title: part.title, data };
    });
    const otros = sortSongs(activeSearchables.filter((s) => !seenMisa.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
    if (otros.length > 0) sections.push({ title: "Otros", data: otros });
    return sections;
  }, [sortMode, activeSearchables, activeSearchablesBySong]);

  const sortedTiempoSections = useMemo(() => {
    const seenTiempo = new Set<number>();
    const sections = TIEMPO_TAB_SONGS_BY_GROUP.map((g) => {
      const data = sortSongs(g.songs.filter((s) => activeSearchablesBySong.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
      data.forEach((s) => seenTiempo.add(s.song));
      return { title: g.title, data };
    });
    const otros = sortSongs(activeSearchables.filter((s) => !seenTiempo.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
    if (otros.length > 0) sections.push({ title: "Otros", data: otros });
    return sections;
  }, [sortMode, activeSearchables, activeSearchablesBySong]);

  const sortedTemasSections = useMemo(() => {
    const seenTemas = new Set<number>();
    const sections = TEMAS_TAB_SONGS_BY_GROUP.map((g) => {
      const data = sortSongs(g.songs.filter((s) => activeSearchablesBySong.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
      data.forEach((s) => seenTemas.add(s.song));
      return { title: g.title, data };
    });
    const otros = sortSongs(activeSearchables.filter((s) => !seenTemas.has(s.song)) as typeof SEARCHABLE_SONGS, sortMode);
    if (otros.length > 0) sections.push({ title: "Otros", data: otros });
    return sections;
  }, [sortMode, activeSearchables, activeSearchablesBySong]);

  const sortLabel = sortMode === "best" ? "Mejor" : sortMode === "az" ? "A→Z" : "#";
  const cycleSortMode = useCallback(() => {
    setSortMode((m) => m === "best" ? "az" : m === "az" ? "number" : "best");
  }, []);

  const navigateToSong = useCallback((song: number, afterNavigate?: () => void) => {
    const page = resolveSongPage(String(song), totalPages, bookSongToPage, bookSortedSongs as any);
    goToPage(page);
    recentSongsRef.current = [song, ...recentSongsRef.current.filter((s) => s !== song)].slice(0, 20);
    afterNavigate?.();
  }, [goToPage, totalPages, bookSongToPage, bookSortedSongs]);

  const handleSearchResultTap = useCallback((song: number) => {
    navigateToSong(song, closeSearch);
  }, [closeSearch, navigateToSong]);

  const handleSearchSubmit = useCallback(() => {
    const firstSection = searchSections[0];
    if (firstSection && firstSection.data.length > 0) {
      handleSearchResultTap(firstSection.data[0].song);
    } else {
      const trimmed = searchText.trim();
      if (trimmed && /^\d+$/.test(trimmed)) {
        goToPage(resolveSongPage(trimmed, totalPages, bookSongToPage, bookSortedSongs as any));
        closeSearch();
      }
    }
  }, [searchSections, searchText, goToPage, closeSearch, handleSearchResultTap, totalPages, bookSongToPage, bookSortedSongs]);

  const handleSearchJump = useCallback(() => {
    const hasQuery = searchText.trim().length > 0;
    const sections = hasQuery
      ? searchSections
      : (
          searchTab === "todas" ? [{ title: "", data: sortedTodas }] :
          searchTab === "misa" ? sortedMisaSections :
          searchTab === "tiempo" ? sortedTiempoSections :
          searchTab === "temas" ? sortedTemasSections :
          [{ title: "Recientes", data: recentSongsRef.current
            .map((sn) => activeSearchablesBySong.get(sn))
            .filter((s): s is typeof SEARCHABLE_SONGS[0] => !!s) }]
        );

    if (!sections.length) return;
    if (searchJumpDirection === "down") {
      const lastSectionIndex = sections.length - 1;
      const lastItemIndex = Math.max(0, sections[lastSectionIndex].data.length - 1);
      searchListRef.current?.scrollToLocation({
        sectionIndex: lastSectionIndex,
        itemIndex: lastItemIndex,
        animated: true,
        viewPosition: 1,
      });
      setSearchJumpDirection("up");
      return;
    }
    searchListRef.current?.scrollToLocation({
      sectionIndex: 0,
      itemIndex: 0,
      animated: true,
      viewPosition: 0,
    });
    setSearchJumpDirection("down");
  }, [
    searchJumpDirection,
    searchSections,
    searchTab,
    searchText,
    activeSearchablesBySong,
    sortedMisaSections,
    sortedTemasSections,
    sortedTiempoSections,
    sortedTodas,
  ]);

  const handleSearchListScroll = useCallback((event: NativeSyntheticEvent<NativeScrollEvent>) => {
    const y = event.nativeEvent.contentOffset.y;
    setSearchJumpDirection(y <= 20 ? "down" : "up");
  }, []);

  // Browse categories
  const openBrowse = useCallback(() => {
    setBrowseVisible(true);
    setSearchVisible(false);
    setGridVisible(false);
  }, []);
  const closeBrowse = useCallback(() => setBrowseVisible(false), []);
  const handleBrowseSongTap = useCallback((song: number) => {
    navigateToSong(song, closeBrowse);
  }, [closeBrowse, navigateToSong]);

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
    navigateToSong(song, closeGrid);
  }, [closeGrid, navigateToSong]);

  const viewabilityConfig = useMemo<ViewabilityConfig>(
    () => ({ viewAreaCoveragePercentThreshold: 50 }),
    [],
  );

  const onViewableItemsChanged = useCallback(
    ({ viewableItems }: { viewableItems: ViewToken[] }) => {
      const first = viewableItems[0];
      if (!first?.item) return;
      const page = (first.item as typeof pageAssets[0]).page;
      currentPageRef.current = page;
      if (mode === "nonStandard") {
        AsyncStorage.setItem(`${STORAGE_KEYS.lastPagePrefix}${activeBookId}`, String(page)).catch(() => {});
      }
      // Director broadcasts page changes to all followers
      if (syncRole === "director") {
        sendNearbyDirectorPageUpdate(page, totalPages, { mode, bookId: activeBookId }).catch(() => {});
      }
    },
    [syncRole, mode, activeBookId, totalPages],
  );

  const { width, height } = dims;
  const isSmallScreen = Math.min(width, height) < 600;

  const renderItem = useCallback(({ item }: ListRenderItemInfo<typeof pageAssets[0]>) => (
    <View style={{ width, height, backgroundColor: "#000" }}>
      <ZoomablePage source={item.source} width={width} height={height} />
    </View>
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ), [width, height]);

  const keyExtractor = useCallback((item: typeof pageAssets[0]) => String(item.page), []);
  const getItemLayout = useCallback((_: unknown, index: number) => ({
    length: width, offset: width * index, index,
  }), [width]);

  const availableHeight = height - keyboardHeight;
  const cardTop = Math.max(8, availableHeight / 2 - (songModal ? 340 : mode === "nonStandard" ? 260 : 80));

  if (!booted) {
    return <View style={styles.screen} />;
  }

  return (
    <View key={`app-reset-${appResetKey}`} style={styles.screen}>
      <StatusBar hidden />

      {onboardingVisible && (
        <View style={styles.cityOverlay}>
          <View style={styles.cityCard}>
            <Text style={styles.cityQuestion}>Código del coro</Text>
            <Text style={styles.citySubcopy}>
              Ingresa tu número autorizado para entrar al modo principal.
            </Text>
            <TextInput
              style={styles.cityInput}
              value={onboardingCode}
              onChangeText={(t) => setOnboardingCode(normalizeAccessCode(t))}
              onSubmitEditing={() => { handleOnboardingContinue().catch(() => {}); }}
              placeholder="Número o código"
              placeholderTextColor="rgba(255,255,255,0.35)"
              keyboardType="number-pad"
              returnKeyType="go"
              maxLength={10}
              secureTextEntry
              selectTextOnFocus
            />
            <TouchableOpacity style={styles.cityBtn} onPress={() => { handleOnboardingContinue().catch(() => {}); }} activeOpacity={0.8}>
              <Text style={styles.cityBtnText}>{onboardingCode ? "Entrar" : "Continuar como himnario"}</Text>
            </TouchableOpacity>
          </View>

          <Text style={styles.buildCorner}>{VISIBLE_BUILD_LABEL}</Text>
        </View>
      )}

      <FlatList
        key={`${width}x${height}`}
        ref={listRef}
        data={pageAssets}
        renderItem={renderItem}
        keyExtractor={keyExtractor}
        horizontal
        pagingEnabled
        showsHorizontalScrollIndicator={false}
        getItemLayout={getItemLayout}
        initialScrollIndex={Math.max(0, startPage - 1)}
        viewabilityConfig={viewabilityConfig}
        onViewableItemsChanged={onViewableItemsChanged}
        removeClippedSubviews
        maxToRenderPerBatch={3}
        windowSize={5}
        initialNumToRender={3}
      />

      {/* ── Song grid (thumbnails) — full-screen overlay ── */}
      {gridVisible && syncRole === "director" && isStandardMode && (() => {
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
                const thumb = thumbAssets[item.page - 1];
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
      {browseVisible && syncRole === "director" && isStandardMode && (
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

      {/* ── Search overlay — director only ── */}
      {searchVisible && (() => {
        const hasQuery = searchText.trim().length > 0;
        const normalizedQ = normalizeText(searchText.trim());
        const qWords = normalizedQ.split(/\s+/).filter(Boolean);

        const renderSongRow = (item: typeof SEARCHABLE_SONGS[0], highlightQuery: boolean) => {
          const displayTitle = item.title || `Canción ${item.song}`;
          const titleParts: { text: string; bold: boolean }[] = [];
          const snippetParts: { text: string; bold: boolean }[] = [];
          if (highlightQuery && qWords.length > 0 && !/^\d+$/.test(searchText.trim())) {
            const normTitle = normalizeText(displayTitle);
            let lastIdx = 0;
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

            const snippetText = `Letra: ${item.snippet}`;
            const normSnippet = normalizeText(snippetText);
            let snippetLastIdx = 0;
            const snippetMatchPositions: { start: number; end: number }[] = [];
            for (const w of qWords) {
              const idx = normSnippet.indexOf(w);
              if (idx !== -1) snippetMatchPositions.push({ start: idx, end: idx + w.length });
            }
            snippetMatchPositions.sort((a, b) => a.start - b.start);
            for (const mp of snippetMatchPositions) {
              if (mp.start < snippetLastIdx) continue;
              if (mp.start > snippetLastIdx) snippetParts.push({ text: snippetText.slice(snippetLastIdx, mp.start), bold: false });
              snippetParts.push({ text: snippetText.slice(mp.start, mp.end), bold: true });
              snippetLastIdx = mp.end;
            }
            if (snippetLastIdx < snippetText.length) snippetParts.push({ text: snippetText.slice(snippetLastIdx), bold: false });
          }
          const hasParts = titleParts.length > 0;
          const hasSnippetParts = snippetParts.length > 0;

          // Compute keyword tags
          const misaPart = getMisaPart(item);
          const tempoTag = item.themes.find((t) => TIEMPO_IDS.has(t));
          const temaTag = item.themes.find((t) => !TIEMPO_IDS.has(t) && !MISA_CATEGORY_IDS.has(t) && THEME_LABELS[t]);

          return (
            <TouchableOpacity
              key={`row-${item.song}`}
              style={styles.searchResultRow}
              onPress={() => handleSearchResultTap(item.song)}
              activeOpacity={0.6}
            >
              {/* Left: keyword pills */}
              <View style={styles.songRowKeywords}>
                {misaPart && (
                  <View style={[styles.pill, { backgroundColor: MISA_PILL_COLORS[misaPart.idx % MISA_PILL_COLORS.length] }]}>
                    <Text style={styles.pillText} numberOfLines={1}>
                      {`${misaPart.label} ${misaPart.idx + 1}/${MISA_PARTS_ORDERED.length}`}
                    </Text>
                  </View>
                )}
                {temaTag && (
                  <View style={[styles.pill, { backgroundColor: TEMA_PILL_COLORS[temaTag] ?? "#555" }]}>
                    <Text style={styles.pillText} numberOfLines={1}>{THEME_LABELS[temaTag]}</Text>
                  </View>
                )}
                {tempoTag && (
                  <View style={[styles.pill, { backgroundColor: TEMPO_PILL_COLORS[tempoTag] ?? "#5c4f7c" }]}>
                    <Text style={styles.pillText} numberOfLines={1}>{THEME_LABELS[tempoTag] ?? tempoTag}</Text>
                  </View>
                )}
              </View>
              {/* Center: song number + title + snippet */}
              <View style={styles.searchResultBody}>
                <Text style={styles.searchResultTitle} numberOfLines={1}>
                  <Text style={{ color: "#3b82f6", fontWeight: "800" }}>{item.song}. </Text>
                  {hasParts ? titleParts.map((p, pi) => (
                    <Text key={pi} style={p.bold ? styles.searchHighlight : undefined}>{p.text}</Text>
                  )) : displayTitle}
                </Text>
                {!!item.snippet && (
                  <Text style={styles.searchResultSnippet} numberOfLines={2}>
                    {hasSnippetParts
                      ? snippetParts.map((p, pi) => (
                          <Text key={pi} style={p.bold ? styles.searchHighlight : undefined}>{p.text}</Text>
                        ))
                      : `Letra: ${item.snippet}`}
                  </Text>
                )}
              </View>
              <Text style={styles.searchResultPage}>p.{item.page}</Text>
            </TouchableOpacity>
          );
        };

        // Tab content for when there's no query
        const recentSongEntries = recentSongsRef.current
          .map((sn) => activeSearchablesBySong.get(sn))
          .filter((s): s is typeof SEARCHABLE_SONGS[0] => !!s);
        const sortedRecentSongEntries = sortMode === "best" ? recentSongEntries : sortSongs(recentSongEntries, sortMode);
        const tabSections = !hasQuery ? (
          searchTab === "todas" ? [{title: "", data: sortedTodas}] :
          searchTab === "misa" ? sortedMisaSections :
          searchTab === "tiempo" ? sortedTiempoSections :
          searchTab === "temas" ? sortedTemasSections :
          /* recientes */ [{title: "Recientes", data: sortedRecentSongEntries}]
        ) : null;

        return (
          <TouchableWithoutFeedback onPress={closeSearch}>
            <View style={[styles.searchOverlay, { bottom: keyboardHeight }]}>
              <TouchableOpacity
                style={styles.searchOverlayTrigger}
                onPress={closeSearch}
                activeOpacity={0.8}
              >
                <Text style={styles.searchOverlayTriggerIcon}>⌕</Text>
              </TouchableOpacity>
              <TouchableWithoutFeedback>
                <View style={styles.searchContainer}>

                  {/* Search bar row */}
                  <View style={styles.searchBar}>
                    <TouchableOpacity style={styles.searchCloseBtn} onPress={closeSearch} activeOpacity={0.7}>
                      <Text style={styles.searchCloseBtnText}>←</Text>
                    </TouchableOpacity>
                    <TextInput
                      ref={searchInputRef}
                      style={styles.searchInput}
                      inputAccessoryViewID={searchAccessoryId}
                      value={searchText}
                      onChangeText={setSearchText}
                      onSubmitEditing={handleSearchSubmit}
                      placeholder="Buscar canción, parte de misa, tema..."
                      placeholderTextColor="rgba(255,255,255,0.4)"
                      keyboardType="default"
                      returnKeyType="go"
                      autoCorrect={false}
                      autoCapitalize="none"
                      maxLength={40}
                      selectTextOnFocus
                    />
                    {searchText.length > 0 && (
                      <TouchableOpacity style={styles.clearBtn} onPress={() => setSearchText("")} activeOpacity={0.7}>
                        <Text style={styles.clearBtnText}>✕</Text>
                      </TouchableOpacity>
                    )}
                    <TouchableOpacity style={styles.jumpBtn} onPress={handleSearchJump} activeOpacity={0.7}>
                      <Text style={styles.jumpBtnText}>{searchJumpDirection === "down" ? "↓" : "↑"}</Text>
                    </TouchableOpacity>
                    <TouchableOpacity style={styles.sortBtn} onPress={cycleSortMode} activeOpacity={0.7}>
                      <Text style={styles.sortBtnText}>{sortLabel}</Text>
                    </TouchableOpacity>
                  </View>

                  {/* Tabs — only when no query */}
                  {!hasQuery && (
                    <View style={styles.searchTabBar}>
                      {(["recientes","todas","misa","tiempo","temas"] as const).map((tab) => (
                        <TouchableOpacity
                          key={tab}
                          style={[styles.searchTabBtn, searchTab === tab && styles.searchTabBtnActive]}
                          onPress={() => setSearchTab(tab)}
                          activeOpacity={0.7}
                        >
                          <Text style={[styles.searchTabBtnText, searchTab === tab && styles.searchTabBtnTextActive]}>
                            {tab === "recientes" ? "Recientes" : tab === "todas" ? "Todas" : tab === "misa" ? "Misa" : tab === "tiempo" ? "Temporada" : "Temas"}
                          </Text>
                        </TouchableOpacity>
                      ))}
                    </View>
                  )}

                  {/* Results — keyword search */}
                  {hasQuery && searchSections.length > 0 && (
                    <SectionList
                      ref={searchListRef}
                      sections={searchSections}
                      keyExtractor={(item, i) => `sr-${item.song}-${i}`}
                      keyboardShouldPersistTaps="handled"
                      onScroll={handleSearchListScroll}
                      scrollEventThrottle={16}
                      style={styles.searchResults}
                      contentContainerStyle={{ paddingBottom: 16 }}
                      stickySectionHeadersEnabled={false}
                      renderSectionHeader={({ section }) => (
                        <View style={styles.searchSectionHeader}>
                          <Text style={styles.searchSectionHeaderText}>{section.title}</Text>
                        </View>
                      )}
                      renderItem={({ item }) => renderSongRow(item as typeof SEARCHABLE_SONGS[0], true)}
                    />
                  )}
                  {hasQuery && searchSections.length === 0 && (
                    <View style={styles.searchEmpty}>
                      <Text style={styles.searchEmptyText}>Sin resultados</Text>
                    </View>
                  )}

                  {/* Tab content — shown when no query */}
                  {!hasQuery && tabSections && (
                    tabSections[0]?.data.length === 0 && searchTab === "recientes" ? (
                      <View style={styles.searchEmpty}>
                        <Text style={styles.searchEmptyText}>Sin canciones recientes</Text>
                      </View>
                    ) : (
                      <SectionList
                        ref={searchListRef}
                        sections={tabSections}
                        keyExtractor={(item, i) => `tab-${item.song}-${i}`}
                        keyboardShouldPersistTaps="handled"
                        onScroll={handleSearchListScroll}
                        scrollEventThrottle={16}
                        style={styles.searchResults}
                        contentContainerStyle={{ paddingBottom: 16 }}
                        stickySectionHeadersEnabled={false}
                        renderSectionHeader={({ section }) =>
                          section.title ? (
                            <View style={styles.searchSectionHeader}>
                              <Text style={styles.searchSectionHeaderText}>{section.title}</Text>
                            </View>
                          ) : null
                        }
                        renderItem={({ item }) => renderSongRow(item as typeof SEARCHABLE_SONGS[0], false)}
                      />
                    )
                  )}

                </View>
              </TouchableWithoutFeedback>
              <InputAccessoryView nativeID={searchAccessoryId}>
                <TouchableOpacity style={styles.searchAccessoryBar} onPress={closeSearch} activeOpacity={0.8}>
                  <Text style={styles.searchAccessoryText}>Cerrar búsqueda</Text>
                </TouchableOpacity>
              </InputAccessoryView>
            </View>
          </TouchableWithoutFeedback>
        );
      })()}

      {/* Top-right button cluster */}
      {syncRole === "follower" && !searchVisible && !onboardingVisible && (
        <TouchableOpacity
          style={styles.reconnectButton}
          onPress={() => { handleReconnectPress().catch(() => {}); }}
          activeOpacity={0.75}
          hitSlop={{ top: 16, bottom: 16, left: 16, right: 16 }}
        >
          <Text style={styles.reconnectIcon}>↻</Text>
          <PulsingDot color="#4cff91" />
        </TouchableOpacity>
      )}

      <View style={styles.navCluster}>
        {/* Nav trigger — tap: song modal, long press: sync modal */}
        {(!searchVisible || syncRole !== "director") && (
          <TouchableOpacity
            style={styles.clusterBtn}
            onPress={openSongModal}
            onLongPress={openSyncModal}
            delayLongPress={500}
            activeOpacity={0.75}
            hitSlop={{ top: 16, bottom: 16, left: 16, right: 3 }}
          >
            <Text style={styles.navTriggerIcon}>♪</Text>
            <Text style={styles.navTriggerArrow}>›</Text>
            {syncRole === "director" && <PulsingDot color="#4a90e2" />}
            {syncRole === "follower" && <PulsingDot color="#4cff91" />}
            {syncRole === "follower" && showSatellite && <Text style={styles.satelliteEmoji}>🛰️</Text>}
          </TouchableOpacity>
        )}
        {syncRole === "director" && !searchVisible && (
          <TouchableOpacity
            style={styles.clusterBtn}
            onPress={openSearch}
            activeOpacity={0.75}
            hitSlop={{ top: 16, bottom: 16, left: 3, right: 16 }}
          >
            <Text style={styles.searchTriggerIcon}>⌕</Text>
          </TouchableOpacity>
        )}
      </View>

      {/* Version label */}
      <Text style={styles.versionLabel} pointerEvents="none">{VISIBLE_BUILD_LABEL}</Text>
      <Text style={styles.modeDebugLabel} pointerEvents="none">
        {isStandardMode ? "Modo: Principal" : "Modo: Himnarios"} · {getBook(activeBookId).title}
      </Text>

      {/* ── Song navigation modal ── */}
      <Modal visible={songModal} transparent animationType="fade" onRequestClose={closeSongModal} statusBarTranslucent>
        <TouchableWithoutFeedback onPress={closeSongModal}>
          <View style={styles.songModalBackdrop}>
            <TouchableWithoutFeedback>
              <View style={styles.songInputCard}>
                <Text style={styles.songInputLabel}>IR A CANTO</Text>
                <TextInput
                  ref={inputRef}
                  style={styles.songInput}
                  value={songInput}
                  onChangeText={(t) => setSongInput(t.replace(/[^0-9]/g, ""))}
                  onSubmitEditing={() => { handleSongSubmit().catch(() => {}); }}
                  placeholder="Número de canto"
                  placeholderTextColor="rgba(0,0,0,0.35)"
                  keyboardType="number-pad"
                  returnKeyType="go"
                  maxLength={SONG_MODAL_INPUT_MAX_LENGTH}
                  selectTextOnFocus
                  showSoftInputOnFocus={false}
                />
                {/* Custom in-app numpad */}
                <SongNumpad
                  onDigit={(d) => setSongInput((prev) => (prev.length < SONG_MODAL_INPUT_MAX_LENGTH ? prev + d : prev))}
                  onBackspace={() => setSongInput((prev) => prev.slice(0, -1))}
                  onGo={() => { handleSongSubmit().catch(() => {}); }}
                  goDisabled={!songInput}
                />
                <View style={styles.songActionButtons}>
                  <TouchableOpacity
                    style={[styles.songGoBtn, !songInput && styles.songGoBtnDisabled]}
                    onPress={() => { handleSongSubmit().catch(() => {}); }}
                    activeOpacity={0.7}
                    disabled={!songInput}
                  >
                    <Text style={styles.songGoText}>♪ Abrir Canto ♪</Text>
                  </TouchableOpacity>
                  <TouchableOpacity style={styles.songCancelBtn} onPress={closeSongModal} activeOpacity={0.7}>
                    <Text style={styles.songCancelText}>Cancelar</Text>
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
                      maxLength={10}
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
                <View style={styles.resetAppDivider} />
                <TouchableOpacity
                  style={[styles.resetAppButton, isResettingApp && styles.resetAppButtonDisabled]}
                  onPress={() => { confirmResetApp(); }}
                  activeOpacity={0.7}
                  disabled={isResettingApp}
                >
                  <Text style={styles.resetAppButtonText}>{isResettingApp ? "Restableciendo..." : "Restablecer app"}</Text>
                </TouchableOpacity>
              </View>
            </TouchableWithoutFeedback>
          </View>
        </TouchableWithoutFeedback>
      </Modal>

      {/* ── Reconnect blocking overlay ── */}
      <Modal visible={reconnectBusy} transparent animationType="fade" onRequestClose={cancelReconnect} statusBarTranslucent>
        <View style={styles.reconnectBackdrop}>
          <View style={styles.reconnectCard}>
            <ActivityIndicator size="large" color="#1a1a2e" />
            <Text style={styles.reconnectTitle}>{reconnectMessage || "Reconectando..."}</Text>
            <Text style={styles.reconnectBody}>Verificando Bluetooth, Wi-Fi local y la conexión con el director.</Text>
            <TouchableOpacity style={styles.reconnectCancelBtn} onPress={cancelReconnect} activeOpacity={0.75}>
              <Text style={styles.reconnectCancelText}>Cancelar</Text>
            </TouchableOpacity>
          </View>
        </View>
      </Modal>

      {/* ── Soft reset blocking overlay ── */}
      <Modal visible={isResettingApp} transparent animationType="fade" statusBarTranslucent>
        <View style={styles.resettingBackdrop}>
          <View style={styles.resettingCard}>
            <ActivityIndicator size="large" color="#0A84FF" />
            <Text style={styles.resettingTitle}>Restableciendo...</Text>
          </View>
        </View>
      </Modal>

      {resetCompleteVisible && (
        <View style={styles.resetCompleteBanner} pointerEvents="none">
          <Text style={styles.resetCompleteText}>Listo</Text>
        </View>
      )}

    </View>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#000" },
  missingText: { color: "rgba(255,255,255,0.15)", fontSize: 48 },
  recoveryButton: {
    backgroundColor: "#1f6feb",
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
  },
  recoveryButtonText: {
    color: "#fff",
    fontSize: 16,
    fontWeight: "700",
  },

  // clusterBtn: both nav and search buttons in navCluster — locked to same width
  clusterBtn: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(26,26,46,0.38)",
    borderRadius: 14,
    width: 96,
    height: 96,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.4,
    shadowRadius: 5,
    elevation: 7,
    gap: 4,
  },
  cornerButton: {
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
  navCluster: {
    position: "absolute",
    top: 1.25,
    right: 1.25,
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  reconnectButton: {
    position: "absolute",
    top: 1.25,
    left: 1.25,
    width: 96,
    height: 96,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(26,26,46,0.38)",
    borderRadius: 14,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.4,
    shadowRadius: 5,
    elevation: 7,
  },
  reconnectIcon: {
    fontSize: 58,
    color: "#fff",
    lineHeight: 76,
    fontWeight: "700",
  },
  navTriggerIcon: { fontSize: 54, color: "#fff", lineHeight: 76 },
  navTriggerArrow: { fontSize: 54, color: "#7ec8f7", lineHeight: 76, fontWeight: "700" },
  searchTriggerSmall: { paddingHorizontal: 16, paddingVertical: 12 },
  searchTriggerIcon: { fontSize: 64, color: "#7ec8f7", lineHeight: 76 },
  satelliteEmoji: {
    position: "absolute",
    top: -8,
    right: -8,
    fontSize: 16,
  },

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
  modeDebugLabel: {
    position: "absolute",
    bottom: 24,
    right: 12,
    fontSize: 10,
    color: "rgba(255,255,255,0.45)",
    fontVariant: ["tabular-nums"],
  },

  modalBackdrop: { flex: 1, backgroundColor: "rgba(0,0,0,0.55)" },
  songModalBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.55)",
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  inputCard: {
    position: "absolute",
    left: 32,
    right: 32,
    maxWidth: 460,
    alignSelf: "center",
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
  songInputCard: {
    width: "100%",
    maxWidth: 460,
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
  songInputLabel: {
    fontSize: 28,
    fontWeight: "800",
    color: "#1a1a2e",
    textTransform: "uppercase",
    textAlign: "center",
    lineHeight: 34,
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
  songActionButtons: { gap: 10, marginTop: 8 },
  songGoBtn: {
    paddingVertical: 27,
    borderRadius: 12,
    backgroundColor: "#0A84FF",
    alignItems: "center",
  },
  songGoBtnDisabled: { backgroundColor: "#9CA3AF" },
  songGoText: { color: "#FFFFFF", fontSize: 27, fontWeight: "800", textAlign: "center" },
  songCancelBtn: {
    paddingVertical: 27,
    borderRadius: 12,
    backgroundColor: "#f0f0f0",
    alignItems: "center",
  },
  songCancelText: { color: "#555", fontSize: 27, fontWeight: "700" },
  reconnectBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.62)",
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  reconnectCard: {
    width: "100%",
    maxWidth: 360,
    borderRadius: 16,
    backgroundColor: "#fff",
    padding: 22,
    alignItems: "center",
    gap: 12,
  },
  reconnectTitle: {
    color: "#111827",
    fontSize: 18,
    fontWeight: "800",
    textAlign: "center",
  },
  reconnectBody: {
    color: "#4B5563",
    fontSize: 14,
    lineHeight: 20,
    textAlign: "center",
  },
  reconnectCancelBtn: {
    marginTop: 4,
    paddingVertical: 14,
    paddingHorizontal: 28,
    borderRadius: 12,
    backgroundColor: "#f0f0f0",
  },
  reconnectCancelText: {
    color: "#555",
    fontSize: 17,
    fontWeight: "700",
  },
  resetAppDivider: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: "rgba(0,0,0,0.12)",
    marginTop: 12,
    marginBottom: 2,
  },
  resetAppButton: {
    paddingVertical: 14,
    borderRadius: 10,
    backgroundColor: "#0A84FF",
    alignItems: "center",
  },
  resetAppButtonDisabled: {
    backgroundColor: "#93C5FD",
  },
  resetAppButtonText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "800",
  },
  resettingBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.68)",
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  resettingCard: {
    width: "100%",
    maxWidth: 300,
    borderRadius: 16,
    backgroundColor: "#fff",
    padding: 24,
    alignItems: "center",
    gap: 14,
  },
  resettingTitle: {
    color: "#111827",
    fontSize: 20,
    fontWeight: "800",
    textAlign: "center",
  },
  resetCompleteBanner: {
    position: "absolute",
    top: 18,
    alignSelf: "center",
    paddingHorizontal: 18,
    paddingVertical: 10,
    borderRadius: 999,
    backgroundColor: "rgba(17,24,39,0.92)",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.3,
    shadowRadius: 10,
    elevation: 12,
  },
  resetCompleteText: {
    color: "#FFFFFF",
    fontSize: 15,
    fontWeight: "800",
  },

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
  searchOverlayTrigger: {
    position: "absolute",
    top: 0,
    right: 0,
    width: 120,
    height: 60,
    backgroundColor: "rgba(26,26,46,0.92)",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 20,
  },
  searchOverlayTriggerIcon: {
    fontSize: 44,
    color: "#7ec8f7",
    lineHeight: 48,
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
  searchCloseBtn: {
    width: 40,
    height: 40,
    borderRadius: 12,
    alignItems: "center",
    justifyContent: "center",
    marginLeft: 2,
    marginRight: 2,
  },
  searchCloseBtnText: {
    color: "#fff",
    fontSize: 26,
    fontWeight: "700",
    lineHeight: 28,
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
  jumpBtn: {
    backgroundColor: "rgba(255,255,255,0.12)",
    borderRadius: 8,
    width: 44,
    height: 44,
    alignItems: "center",
    justifyContent: "center",
    marginRight: 4,
  },
  jumpBtnText: {
    color: "#fff",
    fontSize: 20,
    fontWeight: "800",
    lineHeight: 22,
  },
  clearBtn: {
    width: 32,
    height: 32,
    borderRadius: 16,
    backgroundColor: "rgba(255,255,255,0.18)",
    alignItems: "center",
    justifyContent: "center",
    marginRight: 4,
  },
  clearBtnText: { color: "rgba(255,255,255,0.8)", fontSize: 14, fontWeight: "700" },
  sortBtn: {
    backgroundColor: "rgba(255,255,255,0.12)",
    borderRadius: 8,
    minWidth: 72,
    height: 44,
    paddingHorizontal: 12,
    alignItems: "center",
    justifyContent: "center",
  },
  sortBtnText: { color: "#7ec8f7", fontSize: 14, fontWeight: "700" },
  searchAccessoryBar: {
    backgroundColor: "#111827",
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "rgba(255,255,255,0.12)",
    paddingVertical: 14,
    alignItems: "center",
    justifyContent: "center",
  },
  searchAccessoryText: {
    color: "#fff",
    fontSize: 16,
    fontWeight: "700",
  },
  searchTabBar: {
    flexDirection: "row",
    backgroundColor: "rgba(20,20,40,0.97)",
    borderRadius: 10,
    marginTop: 6,
    overflow: "hidden",
  },
  searchTabBtn: {
    flex: 1,
    paddingVertical: 12,
    alignItems: "center",
  },
  searchTabBtnActive: {
    backgroundColor: "#3b82f6",
    borderRadius: 8,
  },
  searchTabBtnText: { color: "rgba(255,255,255,0.5)", fontSize: 14, fontWeight: "600" },
  searchTabBtnTextActive: { color: "#fff" },
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
    backgroundColor: "#F59E0B",
    color: "#1a1a2e",
    fontWeight: "700",
    borderRadius: 2,
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

  // Song row keyword pills (director row redesign)
  songRowKeywords: {
    width: 120,
    gap: 3,
    justifyContent: "center",
  },
  pill: {
    borderRadius: 6,
    paddingHorizontal: 5,
    paddingVertical: 2,
    alignSelf: "flex-start",
  },
  pillText: {
    fontSize: 10,
    color: "#fff",
    fontWeight: "700",
    opacity: 0.92,
  },

  // Follower notification banner (kept for reference, no longer rendered)
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

  // City unlock prompt
  cityOverlay: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "#05070B",
    justifyContent: "center",
    alignItems: "center",
    zIndex: 9999,
  },
  cityCard: {
    width: "80%",
    maxWidth: 400,
    gap: 20,
    alignItems: "stretch",
    backgroundColor: "rgba(15,23,42,0.70)",
    paddingVertical: 22,
    paddingHorizontal: 18,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    shadowColor: "#000",
    shadowOpacity: 0.45,
    shadowRadius: 24,
    shadowOffset: { width: 0, height: 10 },
    elevation: 6,
  },
  cityQuestion: {
    color: "#FFFFFF",
    fontSize: 22,
    fontWeight: "600",
    textAlign: "center",
    lineHeight: 30,
  },
  citySubcopy: {
    color: "rgba(255,255,255,0.65)",
    fontSize: 14,
    lineHeight: 20,
    textAlign: "center",
    marginTop: -8,
  },
  cityInput: {
    backgroundColor: "rgba(255,255,255,0.10)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.18)",
    borderRadius: 12,
    color: "#FFFFFF",
    fontSize: 28,
    fontWeight: "700",
    fontVariant: ["tabular-nums"],
    paddingHorizontal: 14,
    paddingVertical: 12,
    textAlign: "center",
  },
  buildCorner: {
    position: "absolute",
    right: 10,
    bottom: 8,
    color: "rgba(255,255,255,0.35)",
    fontSize: 10,
    fontWeight: "600",
  },
  cityBtn: {
    backgroundColor: "rgba(255,255,255,0.10)",
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: "center",
    marginTop: 4,
  },
  cityBtnText: {
    color: "#FFFFFF",
    fontSize: 17,
    fontWeight: "600",
  },
  sectionHeader: {
    color: "#555",
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 2,
    textTransform: "uppercase",
    textAlign: "center",
  },
  bookList: {
    gap: 8,
  },
  bookRow: {
    paddingVertical: 11,
    paddingHorizontal: 14,
    borderRadius: 10,
    backgroundColor: "#f3f4f6",
    borderWidth: 1,
    borderColor: "#e5e7eb",
  },
  bookRowSelected: {
    backgroundColor: "#1a1a2e",
    borderColor: "#1a1a2e",
  },
  bookRowText: {
    color: "#111827",
    fontSize: 15,
    fontWeight: "700",
    textAlign: "center",
  },
  bookRowTextSelected: {
    color: "#fff",
  },
});
