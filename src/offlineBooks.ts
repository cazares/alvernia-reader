import AsyncStorage from "@react-native-async-storage/async-storage";

// @ts-ignore
import STANDARD_PAGES_JSON from "../assets/standard/pages.json";
// @ts-ignore
import STANDARD_TITLES_JSON from "../assets/standard/song-titles.json";
// @ts-ignore
import STANDARD_SEARCH_JSON from "../assets/standard/song-search-index.json";

// Single-book app: the only book is the Alvernia manual ("standard").
export type AppMode = "standard";
export type BookId = "standard";

export type OfflineBook = {
  id: BookId;
  title: string;
  assets: Record<string, number>;
  /** PDF bundle resource name (no extension) — if set, pages are rendered on-demand */
  pdfSource?: string;
  totalPages: number;
  songTitles: Record<string, string>;
  songSearchIndex: Array<{ song: number; page: number; title?: string; normalized?: string; lyrics?: string }>;
};

export const BOOKS: OfflineBook[] = [
  {
    id: "standard",
    title: "Manual Alvernia",
    assets: {},
    pdfSource: "alvernia_manual_2",
    totalPages: Number((STANDARD_PAGES_JSON as any).totalPages ?? 368),
    songTitles: (STANDARD_TITLES_JSON as any) ?? {},
    songSearchIndex: (STANDARD_SEARCH_JSON as any) ?? [],
  },
];

export const getBook = (_id?: BookId): OfflineBook => BOOKS[0];

export type OfflineBookAssetsValidation = {
  ok: boolean;
  missingCount: number;
  sampleMissingKeys: string[];
};

export const validateOfflineBookAssets = (book: OfflineBook): OfflineBookAssetsValidation => {
  const totalPages = Math.max(0, Number(book.totalPages || 0) || 0);
  if (!totalPages) {
    return { ok: false, missingCount: 1, sampleMissingKeys: ["pages.json.totalPages"] };
  }
  if (book.pdfSource) {
    return { ok: true, missingCount: 0, sampleMissingKeys: [] };
  }
  const missing: string[] = [];
  const check = (key: string) => {
    if (!book.assets || typeof book.assets[key] !== "number") missing.push(key);
  };
  check(`pages/page-${String(1).padStart(3, "0")}.jpg`);
  check(`pages/page-${String(Math.min(totalPages, 2)).padStart(3, "0")}.jpg`);
  check(`pages/page-${String(totalPages).padStart(3, "0")}.jpg`);
  return { ok: missing.length === 0, missingCount: missing.length, sampleMissingKeys: missing.slice(0, 3) };
};

export const STORAGE_KEYS = {
  onboardingComplete: "sv.onboarding.complete",
  onboardingState: "sv.onboarding.state",
  onboardingCity: "sv.onboarding.city",
  standardAccessName: "sv.standard.accessName",
  mode: "sv.mode",
  activeBookId: "sv.book.active",
  // last known sync role (director/follower) for restart restore
  lastSyncRole: "sv.sync.lastRole",
  // timestamp (ms) of last time this device was director
  lastDirectorAt: "sv.sync.lastDirectorAt",
  // per-book saved position
  lastPagePrefix: "sv.book.lastPage.",
} as const;

export const clearAllBookState = async () => {
  const keys = await AsyncStorage.getAllKeys();
  const toClear = keys.filter((k) =>
    k === STORAGE_KEYS.onboardingComplete ||
    k === STORAGE_KEYS.onboardingState ||
    k === STORAGE_KEYS.onboardingCity ||
    k === STORAGE_KEYS.standardAccessName ||
    k === STORAGE_KEYS.mode ||
    k === STORAGE_KEYS.activeBookId ||
    k === STORAGE_KEYS.lastSyncRole ||
    k === STORAGE_KEYS.lastDirectorAt ||
    k.startsWith(STORAGE_KEYS.lastPagePrefix),
  );
  if (toClear.length) await AsyncStorage.multiRemove(toClear);
};
