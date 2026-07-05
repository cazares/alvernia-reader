// Single-book app: the only book is the Alvernia manual ("standard").
//
// Song data (index, titles, search) is NOT stored here. It is generated at build
// time by web/build.mjs from the canonical src/alverniaManual2SongIndex.js +
// assets/alvernia_manual_2.pdf into web/dist/books/standard/*.json, which both the
// web app and the native WebView load. This module now only carries the BookId type
// and the AsyncStorage key names used by the native shell.

export type BookId = "standard";

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
