import AsyncStorage from "@react-native-async-storage/async-storage";

import { OFFLINE_WEB_BUNDLE_ASSETS } from "./offlineWebBundle";
// @ts-ignore
import STANDARD_PAGES_JSON from "../assets/offline-web/pages.json";
// @ts-ignore
import STANDARD_TITLES_JSON from "../assets/offline-web/song-titles.json";
// @ts-ignore
import STANDARD_SEARCH_JSON from "../assets/offline-web/song-search-index.json";

// Generated static Metro asset maps for alternate books
// eslint-disable-next-line @typescript-eslint/no-var-requires
const H1 = require("./offlineBookAssets.hymns-1.js") as { BOOK_ID: string; ASSETS: Record<string, number> };
// eslint-disable-next-line @typescript-eslint/no-var-requires
const H2 = require("./offlineBookAssets.hymns-2.js") as { BOOK_ID: string; ASSETS: Record<string, number> };
// eslint-disable-next-line @typescript-eslint/no-var-requires
const H4 = require("./offlineBookAssets.hymns-4.js") as { BOOK_ID: string; ASSETS: Record<string, number> };

// @ts-ignore
import H1_PAGES_JSON from "../assets/offline-books/hymns-1/pages.json";
// @ts-ignore
import H2_PAGES_JSON from "../assets/offline-books/hymns-2/pages.json";
// @ts-ignore
import H4_PAGES_JSON from "../assets/offline-books/hymns-4/pages.json";
// @ts-ignore
import H4_TITLES_JSON from "../assets/offline-books/hymns-4/song-titles.json";
// @ts-ignore
import H4_SEARCH_JSON from "../assets/offline-books/hymns-4/song-search-index.json";

export type AppMode = "standard" | "nonStandard";
export type BookId = "standard" | "hymns-1" | "hymns-2" | "hymns-4";

export type OfflineBook = {
  id: BookId;
  title: string;
  assets: Record<string, number>;
  totalPages: number;
  // songIndex/search data is best-effort for scanned PDFs (may be empty)
  songTitles: Record<string, string>;
  songSearchIndex: Array<{ song: number; page: number; title?: string; normalized?: string; lyrics?: string }>;
};

export const BOOKS: OfflineBook[] = [
  {
    id: "standard",
    title: "Manual Alvernia",
    assets: OFFLINE_WEB_BUNDLE_ASSETS,
    totalPages: Number((STANDARD_PAGES_JSON as any).totalPages ?? 368),
    songTitles: (STANDARD_TITLES_JSON as any) ?? {},
    songSearchIndex: (STANDARD_SEARCH_JSON as any) ?? [],
  },
  {
    id: "hymns-1",
    title: "Himnos Evangélicos",
    assets: H1.ASSETS,
    totalPages: Number((H1_PAGES_JSON as any).totalPages ?? 0),
    songTitles: {},
    songSearchIndex: [],
  },
  {
    id: "hymns-2",
    title: "El Nuevo Himnario Evangélico",
    assets: H2.ASSETS,
    totalPages: Number((H2_PAGES_JSON as any).totalPages ?? 0),
    songTitles: {},
    songSearchIndex: [],
  },
  {
    id: "hymns-4",
    title: "Himnos de Sión",
    assets: H4.ASSETS,
    totalPages: Number((H4_PAGES_JSON as any).totalPages ?? 0),
    songTitles: (H4_TITLES_JSON as any) ?? {},
    songSearchIndex: (H4_SEARCH_JSON as any) ?? [],
  },
];

export const getBook = (id: BookId): OfflineBook => {
  const found = BOOKS.find((b) => b.id === id);
  return found ?? BOOKS[0];
};

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
  const missing: string[] = [];
  const check = (key: string) => {
    if (!book.assets || typeof book.assets[key] !== "number") missing.push(key);
  };
  // Validate a few sentinel pages to avoid heavy work on-device.
  check(`pages/page-${String(1).padStart(3, "0")}.jpg`);
  check(`pages/page-${String(Math.min(totalPages, 2)).padStart(3, "0")}.jpg`);
  check(`pages/page-${String(totalPages).padStart(3, "0")}.jpg`);
  return { ok: missing.length === 0, missingCount: missing.length, sampleMissingKeys: missing.slice(0, 3) };
};

// App Store submission hardening: keep non-standard mode to the single high-res book we ship.
export const NON_STANDARD_BOOK_IDS: BookId[] = ["hymns-4"];

export const STORAGE_KEYS = {
  onboardingComplete: "sv.onboarding.complete",
  onboardingState: "sv.onboarding.state",
  onboardingCity: "sv.onboarding.city",
  standardAccessName: "sv.standard.accessName",
  mode: "sv.mode",
  activeBookId: "sv.book.active",
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
    k.startsWith(STORAGE_KEYS.lastPagePrefix),
  );
  if (toClear.length) await AsyncStorage.multiRemove(toClear);
};
