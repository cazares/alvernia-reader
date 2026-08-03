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

  // ── Book-bundle resolution (defect D1 / D2) ───────────────────────────────
  // The cached boot DECISION: { uri, bookVersion, builtFromShellBuild }. Parsing two manifests on
  // the boot path is fine on one device and dangerous on eight identical aging iPads that cold-boot
  // together and could cross a timeout together (red team A7). The fast path re-uses the last
  // decision after one cheap existence check.
  bookResolved: "sv.book.resolved",
  // { bookVersion, mountedAt, provedAt, attempts } — written BEFORE a mount and proved on
  // bridge-ready. The only net that catches a bundle which kills the process before React renders,
  // which is the failure an old iPad under memory pressure actually produces.
  bookBoot: "sv.book.boot",
  // [{ bookVersion, failures, lastFailureAt }] — a COUNTER, not a tombstone.
  bookQuarantine: "sv.book.quarantine",
  // { setAt } — operator panic switch forcing the code-signed bundle. Auto-expires.
  bookForceBundled: "sv.book.forceBundled",
  // { bookVersion, at } — drives the loud, non-dismissible LIBRO ANTERIOR banner. A silent correct
  // recovery is worse than a loud one when eight devices do it at once and split the fleet across
  // two different songbooks (red team A4).
  bookReverted: "sv.book.reverted",

  // ── Downloader (M5) ───────────────────────────────────────────────────────
  // { bookVersion, totalPages, installedAt, source } — the book currently mounted.
  bookActive: "sv.book.active",
  // { bookVersion, ready, readyAt, error, … } — the staged download's state.
  bookStaged: "sv.book.staged",
  // ms of the last SUCCESSFUL /fleet/checkin. This is the live-internet proof the apply gate
  // depends on: true at practice, false inside the church by definition, needing no clock and no
  // Mass-schedule config.
  lastCheckinOkAt: "sv.book.lastCheckinOkAt",
  // { bookVersion, at } — when this device FIRST saw a given pointer, so the per-device stagger is
  // measured from discovery rather than from app launch.
  bookFirstSeen: "sv.book.firstSeen",
} as const;
