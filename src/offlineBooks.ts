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
const H3 = require("./offlineBookAssets.hymns-3.js") as { BOOK_ID: string; ASSETS: Record<string, number> };
// eslint-disable-next-line @typescript-eslint/no-var-requires
const H4 = require("./offlineBookAssets.hymns-4.js") as { BOOK_ID: string; ASSETS: Record<string, number> };

// @ts-ignore
import H1_PAGES_JSON from "../assets/offline-books/hymns-1/pages.json";
// @ts-ignore
import H2_PAGES_JSON from "../assets/offline-books/hymns-2/pages.json";
// @ts-ignore
import H3_PAGES_JSON from "../assets/offline-books/hymns-3/pages.json";
// @ts-ignore
import H4_PAGES_JSON from "../assets/offline-books/hymns-4/pages.json";

// @ts-ignore
import H1_TITLES_JSON from "../assets/offline-books/hymns-1/song-titles.json";
// @ts-ignore
import H2_TITLES_JSON from "../assets/offline-books/hymns-2/song-titles.json";
// @ts-ignore
import H3_TITLES_JSON from "../assets/offline-books/hymns-3/song-titles.json";
// @ts-ignore
import H4_TITLES_JSON from "../assets/offline-books/hymns-4/song-titles.json";

// @ts-ignore
import H1_SEARCH_JSON from "../assets/offline-books/hymns-1/song-search-index.json";
// @ts-ignore
import H2_SEARCH_JSON from "../assets/offline-books/hymns-2/song-search-index.json";
// @ts-ignore
import H3_SEARCH_JSON from "../assets/offline-books/hymns-3/song-search-index.json";
// @ts-ignore
import H4_SEARCH_JSON from "../assets/offline-books/hymns-4/song-search-index.json";

export type AppMode = "standard" | "nonStandard";
export type BookId = "standard" | "hymns-1" | "hymns-2" | "hymns-3" | "hymns-4";

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
    songTitles: (H1_TITLES_JSON as any) ?? {},
    songSearchIndex: (H1_SEARCH_JSON as any) ?? [],
  },
  {
    id: "hymns-2",
    title: "El Nuevo Himnario Evangélico",
    assets: H2.ASSETS,
    totalPages: Number((H2_PAGES_JSON as any).totalPages ?? 0),
    songTitles: (H2_TITLES_JSON as any) ?? {},
    songSearchIndex: (H2_SEARCH_JSON as any) ?? [],
  },
  {
    id: "hymns-3",
    title: "Himnario Provisional",
    assets: H3.ASSETS,
    totalPages: Number((H3_PAGES_JSON as any).totalPages ?? 0),
    songTitles: (H3_TITLES_JSON as any) ?? {},
    songSearchIndex: (H3_SEARCH_JSON as any) ?? [],
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

export const NON_STANDARD_BOOK_IDS: BookId[] = ["hymns-1", "hymns-2", "hymns-3", "hymns-4"];

export const STORAGE_KEYS = {
  onboardingComplete: "sv.onboarding.complete",
  onboardingState: "sv.onboarding.state",
  onboardingCity: "sv.onboarding.city",
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
    k === STORAGE_KEYS.mode ||
    k === STORAGE_KEYS.activeBookId ||
    k.startsWith(STORAGE_KEYS.lastPagePrefix),
  );
  if (toClear.length) await AsyncStorage.multiRemove(toClear);
};
