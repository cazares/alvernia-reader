// SignoVivo — native shell (build 332+).
//
// The entire reader UI now lives in a locally-bundled copy of signovivo.com,
// loaded into a WKWebView (via react-native-webview) from file://. This file is a
// thin native shell whose only jobs are the things the web cannot do:
//   1. Serve the bundled web app offline (file://…/WebBundle/index.html), preferring
//      a peer-pushed update in Documents/WebBundle if one has landed.
//   2. Bridge the offline iPad-to-iPad Multipeer mesh <-> the web app (page sync).
//   3. Validate director codes entered on the web numpad and drive director/follower role.
//   4. Publish the director's page to the Cloudflare relay so signovivo.com followers sync.
//   5. Keep the screen awake. (The app is SINGLE-BOOK and FULLY PUBLIC: there is exactly one book,
//      id="standard" (Manual Alvernia), served to everyone with no IP-geo gate. Any valid director
//      code is accepted. There is no second book, no book switching, and no geo selection anywhere.)
//
// Everything else — rendering, paging, zoom, search, browse, song navigation — is the
// web app's job. The old 3,536-line FlatList/PDF reader was replaced wholesale.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, AppState, Platform, StatusBar, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { useKeepAwake } from "expo-keep-awake";
import * as FileSystem from "expo-file-system/legacy";
import WebView, { type WebViewMessageEvent } from "react-native-webview";

import {
  addNearbyDirectorSyncListener,
  denyDirectorTakeover,
  isNearbyDirectorSyncAvailable,
  primeNearbyPermissions,
  refreshNearbyDiscovery,
  refreshDirectorBrowse,
  forceFollowerReconnectNow,
  requestCurrentSnapshot,
  resetNearbyDirectorSync,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
} from "./src/nearbyDirectorSync";
import {
  publishPageToRelay,
  setRelayPublishing,
  setRelayAuthErrorHandler,
  setRelayAuthOkHandler,
} from "./src/directorRelaySync";
import { STORAGE_KEYS, type BookId } from "./src/offlineBooks";
import {
  decideBundle,
  recordBundleFailure,
  clearBundleFailures,
  readAccessDirFor,
  nextHealAction,
  type BundleSource,
} from "./src/bookResolve";
import {
  parseBookUpdate,
  shouldStage,
  canApplyNow,
  stageBook,
  applyStagedBundle,
} from "./src/bookUpdate";
import versionJson from "./version.json";

// ─────────────────────────────── Constants ──────────────────────────────────

const BUILD_VERSION = String((versionJson as { buildNumber?: number }).buildNumber ?? "");

/**
 * Which KIND of device this is — "PAD" | "PHN" | "" (unknown).
 *
 * THE FRONTEND CANNOT WORK THIS OUT, which is why it is resolved here and injected. iPadOS 13+
 * reports itself as Macintosh to any WebView: `navigator.platform` is "MacIntel" and the UA carries
 * no "iPad" at all, so a browser-side check is a `maxTouchPoints` guess. `Platform.isPad` is the
 * OS answering about itself.
 *
 * This exists because on 2026-08-05 an iPad still on build 393 was read as the owner's iPhone for
 * over an hour: two native devices, and NOTHING anywhere — badge, check-in, or dashboard — could
 * tell them apart. Both surfaces now carry it: the badge (for whoever is holding the device) and
 * /fleet/checkin (for whoever is reading telemetry from somewhere else).
 *
 * Three letters, not two: "PH"/"PA" would be one edit apart, and this repo already learned that
 * lesson the hard way (MIN_CODE_DISTANCE, src/bookUpdate.js) when a proposed operator code sat one
 * digit from the soft-reset code. These get read aloud across a choir loft.
 */
const DEVICE_KIND: "PAD" | "PHN" | "" =
  Platform.OS === "ios" ? ((Platform as { isPad?: boolean }).isPad ? "PAD" : "PHN") : "";
const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";

// Hard ceiling on bundle resolution. Everything in resolveBundleUri touches the filesystem and it
// runs on the boot path, so a wedged I/O call must never be able to leave the app with no UI. The
// timeout mounts the code-signed bundle for THIS launch only and is deliberately non-sticky.
const RESOLVE_TIMEOUT_MS = 1500;
// Generous, because crossing it mounts a possibly-older book: it exists to guarantee the app is
// never a black rectangle, not to police slow disks.
const PREBOOT_TIMEOUT_MS = 12000;
// The operator panic switch expires on its own. Nothing else clears it, and a forced-baked device
// that nobody remembers forcing is an outage that looks like a mystery.
const FORCE_BUNDLED_TTL_MS = 24 * 60 * 60 * 1000;

// Fixed Multipeer session for the parish mesh (unchanged from the native reader).
const DIRECTOR_SESSION = "1234";

// Director entry. ONE code, in plain source, on purpose.
//
// This used to be a set of REAL DIRECTOR PHONE NUMBERS, baked at archive time out of a gitignored
// director-codes.private.json that release.sh swapped over a tracked empty file and restored under
// a trap. That machinery bought nothing: taking the role already requires physically holding a
// parish iPad, and the confirm dialog below — not the secrecy of the number — is what stops an
// accidental takeover. What it cost was real: an archive made without that file produced an IPA
// that installed perfectly and rejected every code, which is how the 2026-07-01 Mass outage
// happened, and it put four people's phone numbers one `git add` away from a public repo.
//
// Removed 2026-08-05 at the owner's call: "super overkill for what this app is and is meant to
// become (stay the same)." Nine iPads, one permanent director, no MDM, no threat model.
//
// Distance 8+ from every other numpad code (soft reset, book apply, force baked) so a single
// misread in poor light cannot wipe a device's role instead of granting it.
const DIRECTOR_CODE = "333444555";
// Secret numpad code carried over from the native reader.
const SOFT_RESET_CODE = "744668486";

// ── Songbook-update operator codes ──────────────────────────────────────────
//
// EVERY code must be Levenshtein distance >= MIN_CODE_DISTANCE from EVERY other code. These are
// read off a laminated card, in poor light, in a church, by someone under pressure. The originally
// proposed apply code was 744668487 — ONE DIGIT from SOFT_RESET_CODE (red team H4), so a single
// misread would have wiped the device's role instead of applying an update. Both of these sit at
// distance 9 from soft-reset and from each other; e2e/bookUpdate.test.mjs pins the rule.
const BOOK_APPLY_CODE = "265134902";
const BOOK_FORCE_BAKED_CODE = "907315268";

// BUILD-BAKED KILL SWITCH. A one-line source neuter, independent of the server, mirroring the
// SYNC_STRICT pattern. If the downloader ever misbehaves in the field, this turns it off in the
// NEXT build without needing the relay to be reachable — belt to the server flag's braces.
const SV_BOOK_DL_KILL = false;
// NEW-DIR-3: a director's mesh page arrives on a ~2s heartbeat. Treat a director as "live right now"
// only if we've heard from them within this window — otherwise the destructive "take control" confirm
// false-fires forever after any director ever broadcast (the ref used to be set-once-never-cleared).
const LIVE_DIRECTOR_WINDOW_MS = 8000;
// ── ONE DIRECTOR. ONLY A HUMAN MAKES ONE. ──────────────────────────────────────────────────────
// (Miguel, 2026-08-15, the night before Mass, after watching physical devices split: "only ever
// allow one and only one director." And 2026-07-02: "don't just auto-make anyone director — always
// ask, always.")
//
// Between 2026-08-05 and build 427 this file could mint a director BY ITSELF, twice over: a device
// that had directed moments before its app restarted resumed the role, and a device that had EVER
// directed claimed an "empty" seat on every boot after ~12-16s. Both decided the seat was empty from
// "no director page heard within 8s" — but a director the radio has DISCOVERED and not yet
// CONNECTED to sends no pages, and discovery alone can take 5-30s. So the automatic claim fired
// while a human was already directing, and because the mesh resolves two directors by NEWEST TOKEN
// WINS (DirectorSyncModule.swift, handleDirectorConflict), the automatic one demoted the human.
// The choir followed the wrong iPad, or half of each, until the next collision.
//
// So: no timer, no tally, no resume promotes this device. The role is taken ONLY by a hand on this
// device — the pill, or the director code on the numpad — through onDirectorCode → becomeDirector.
// A device that was directing when its app died comes back as a FOLLOWER and is TOLD to tap the
// pill. Two humans who both take the role converge on the newer one within a discovery cycle, and
// the one who lost is told so. That is the whole invariant, and e2e/singleDirector.test.mjs pins
// every clause of it against this source.
//
// The heartbeat ticks every second; this is how often it may touch AsyncStorage.
const DIRECTOR_STAMP_THROTTLE_MS = 20000;
// How many breadcrumbs to keep. Sized to cover a whole Mass plus the boot before it, while staying
// small enough that serialising the array on every crumb is free.
const BREADCRUMB_LIMIT = 200;
// The app is single-book: the only book is the standard (Alvernia) manual. Its id is pinned
// everywhere so the mesh/relay Snapshot's bookId stays a stable "standard" for backward compat.
const DEFAULT_BOOK: BookId = "standard";

type SyncRole = "off" | "director" | "follower";

const isBookId = (value: unknown): value is BookId => value === "standard";
const digitsOnly = (value: unknown) => String(value ?? "").replace(/[^0-9]/g, "");
// The relay/mesh Snapshot carries a legacy `mode` string alongside bookId; it is always "standard".
const modeForBook = (_book: BookId): "standard" => "standard";

// ──────────────────────────────── App ───────────────────────────────────────

export default function App() {
  useKeepAwake("signovivo-reader");

  const webViewRef = useRef<WebView>(null);
  const [booted, setBooted] = useState(false);
  const [initialBook, setInitialBook] = useState<BookId>(DEFAULT_BOOK);
  // Bumped to force a fresh WebView mount (e.g. after a peer-pushed bundle update / soft reset).
  const [bundleUri, setBundleUri] = useState<string | null>(null);
  const [mountKey, setMountKey] = useState(0);
  // Slice B (native crash recovery): when the WebView is confirmed dead — a (re)load that never
  // reaches `bridge-ready` after bounded remount attempts — show a NATIVE "Reintentar" view
  // instead of a black screen. The web's own boot-guard card can't run if the WebView PROCESS
  // itself is gone, so this is the native-layer floor: the app is ALWAYS recoverable.
  const [webDead, setWebDead] = useState(false);
  const bridgeWatchdogRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const remountAttemptsRef = useRef(0);

  // Which bundle is actually mounted right now. The watchdog closure has no other way to know what
  // it is about to abandon, and abandoning the wrong one (the code-signed floor) would leave the
  // device with nothing to fall back to.
  const activeBundleSourceRef = useRef<BundleSource | null>(null);
  const activeBookVersionRef = useRef<string | null>(null);
  // The watchdog is declared above the resolver (it is a dependency of the mount effect), and the
  // resolver depends on the quarantine writer, so the ladder reaches the resolver through a ref
  // rather than reordering ~500 lines of hooks. Assigned immediately after resolveBundleUri.
  const resolveBundleUriRef = useRef<((force?: "bundled") => Promise<string>) | null>(null);
  // Downloader state, kept in refs because fleetCheckin is a stable useCallback that must see the
  // CURRENT values without being re-created (re-creating it would restart the 90 s interval).
  const bookStageRef = useRef<string>("");
  const lastCheckinOkAtRef = useRef<number | null>(null);
  const lastPageTurnAtRef = useRef<number | null>(null);
  const coldBootAtRef = useRef<number>(Date.now());
  const lastKnownRoleRef = useRef<string | null>(null);
  // Live mesh peer count, latched from the Swift `state` event. This is the real "is a
  // rehearsal or Mass happening right now?" signal, and it is what throttles staging and
  // VETOES an apply — a guess would have been worse than nothing here.
  const meshPeerCountRef = useRef(0);
  const stagingInFlightRef = useRef(false);
  const onCheckinResponseRef = useRef<((body: unknown) => void) | null>(null);
  // applyStagedBook is declared far BELOW the numpad dispatch that invokes it. Referencing it
  // directly would capture the first render's copy (it is not in that useCallback's deps) and
  // sits in its temporal dead zone during the render pass. Same ref indirection as
  // resolveBundleUriRef, for the same reason.
  const applyStagedBookRef = useRef<(() => Promise<void>) | null>(null);
  // Declared as a ref for the same reason applyStagedBook is: the foreground listener and the
  // staging completion both need it, and both are defined above it.
  const autoApplyIfSafeRef = useRef<(() => Promise<void>) | null>(null);
  // MANUAL-ONLY BOOK UPDATES (owner decision, 2026-08-05). `pendingPointer` is what the last
  // check-in offered; `manualRefresh` is true ONLY while a ⟳ tap is being serviced. Staging reads
  // that flag, so a routine check-in records the offer and downloads nothing.
  const pendingPointerRef = useRef<{ bookVersion: string; base: string } | null>(null);
  const manualRefreshRef = useRef(false);
  const refreshBookNowRef = useRef<(() => Promise<void>) | null>(null);
  // LOUD self-heal (red team A4). A silent, correct recovery is WORSE than a loud one here: a
  // defect that only reproduces on the choir's hardware fires all eight watchdogs at once, every
  // device quietly drops to the previous songbook, and the fleet is now split across two books with
  // nobody aware. This banner survives restarts until an operator clears it.
  const [revertedBook, setRevertedBook] = useState<string | null>(null);

  const roleRef = useRef<SyncRole>("off");
  // Bumped at the top of every role-entry path. A become*() captures this at entry and bails
  // after each await if it's been superseded (e.g. a rapid director→soft-reset→director flip
  // inside the 2s mesh-start retry window), so a stale become* can't write roleRef late.
  const roleGenerationRef = useRef(0);
  const explicitTransmitterRef = useRef(false);
  const currentPageRef = useRef(1);
  const totalPagesRef = useRef(0);
  const currentBookRef = useRef<BookId>(DEFAULT_BOOK);
  const webReadyRef = useRef(false);
  const pendingInjectRef = useRef<string[]>([]);
  // Last page/book the DIRECTOR broadcast to us over the mesh (distinct from the web's own
  // page). Drives resync after a WebView reload / foreground for followers. Null until a
  // director snapshot has actually been received (a fresh-boot follower must keep its own page).
  const lastDirectorSnapshotRef = useRef<{ page: number; book: BookId; at: number } | null>(null);
  // Throttles the lastDirectorAt write from the 1s heartbeat. 0 = never written this session.
  const lastDirectorAtWrittenRef = useRef<number>(0);
  // True from the moment becomeDirector is entered until it settles. roleRef is only assigned after
  // the mesh has started (which can sleep 2s and retry); anything that must not race a director
  // start in flight reads this, not roleRef.
  const becomeDirectorInFlightRef = useRef(false);
  // One-shot: the transmitter notice must not re-fire when syncAvailable flips identity.
  const didTransmitterNoticeRef = useRef(false);
  // A count of how often this device has directed. DIAGNOSTIC ONLY — it decides nothing. (It used
  // to rank devices for an automatic seat claim; that path is gone, see the ONE DIRECTOR note above.)
  // Read-modify-write, best effort.
  const bumpDirectorSessions = useCallback(() => {
    AsyncStorage.getItem(STORAGE_KEYS.directorSessions)
      .then((raw) => AsyncStorage.setItem(STORAGE_KEYS.directorSessions, String((Number(raw || 0) || 0) + 1)))
      .catch(() => {});
  }, []);
  // Director re-broadcast heartbeats. Two cadences: a FAST mesh re-send (local, free) so a
  // dropped page-turn recovers in ~2s, and a SLOW relay keepalive that only refreshes the
  // Cloudflare snapshot's freshness (page CHANGES publish to the relay immediately anyway).
  const meshHeartbeatRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const relayHeartbeatRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // The role-restore bootstrap (auto-become director/follower from persisted state) must run
  // EXACTLY ONCE per session. The mesh-bootstrap effect's deps include become*/injectEvent
  // useCallbacks whose identities can change mid-session, re-running the effect — without this
  // guard a re-run would re-fire the bootstrap and re-promote / re-mint a director under us.
  const didBootstrapRef = useRef(false);

  const syncAvailable = useMemo(() => isNearbyDirectorSyncAvailable(), []);

  // ── Breadcrumb (lightweight crash forensics; survives a hard restart) ──────
  // A RING BUFFER, not a single value. This wrote one key and overwrote it every call, so exactly
  // ONE breadcrumb survived — the last one, which is almost never the interesting one. There is no
  // internet inside the church and no MDM on these iPads, so when a device misbehaves at Mass the
  // dbgLog telemetry below cannot reach the worker and nothing else is written down. "It froze"
  // stayed unanswerable forever.
  //
  // Kept deliberately small and dumb: a bounded array of strings in one AsyncStorage key, written
  // best-effort, read back by the diagnostics bridge. No timers, no flush logic, nothing that can
  // itself fail during Mass. The single-value key is still written so anything reading `sv_bc`
  // keeps working.
  const breadcrumbsRef = useRef<string[]>([]);
  // Until the previous session's crumbs are read back, persisting would OVERWRITE them with a
  // one-element array — destroying the history at exactly the moment it matters, since the first
  // crumb after a crash lands within milliseconds of boot. Buffer in memory, persist once loaded.
  const breadcrumbsLoadedRef = useRef(false);
  const breadcrumb = useCallback((tag: string) => {
    try {
      const next = breadcrumbsRef.current;
      next.push(`${new Date().toISOString()} ${tag}`);
      if (next.length > BREADCRUMB_LIMIT) next.splice(0, next.length - BREADCRUMB_LIMIT);
      AsyncStorage.setItem("sv_bc", `${tag} @ ${Date.now()}`).catch(() => {});
      if (breadcrumbsLoadedRef.current) {
        AsyncStorage.setItem(STORAGE_KEYS.breadcrumbs, JSON.stringify(next)).catch(() => {});
      }
    } catch {
      /* a diagnostic must never be able to break the thing it is diagnosing */
    }
  }, []);

  // Read back the previous session's crumbs, keeping anything logged during this boot AFTER them.
  // A session boundary is marked so a reader can see where the restart happened — the line before
  // it is usually the last thing that worked.
  useEffect(() => {
    let cancelled = false;
    AsyncStorage.getItem(STORAGE_KEYS.breadcrumbs)
      .then((raw) => {
        if (cancelled) return;
        let prev: string[] = [];
        try {
          const parsed = JSON.parse(raw || "[]");
          if (Array.isArray(parsed)) prev = parsed.filter((x) => typeof x === "string");
        } catch {
          /* a corrupt buffer is not worth failing over — start fresh */
        }
        const merged = [...prev, `${new Date().toISOString()} ── app start ──`, ...breadcrumbsRef.current];
        breadcrumbsRef.current = merged.slice(-BREADCRUMB_LIMIT);
        breadcrumbsLoadedRef.current = true;
        AsyncStorage.setItem(STORAGE_KEYS.breadcrumbs, JSON.stringify(breadcrumbsRef.current)).catch(() => {});
      })
      .catch(() => {
        // Could not read; allow persistence anyway so THIS session is at least recorded.
        breadcrumbsLoadedRef.current = true;
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // ── Remote sync telemetry → CF /log ─────────────────────────────────────────
  // The iPad↔iPhone sync is peer-to-peer Multipeer (no server), so we can't see it remotely. This
  // POSTs each device's sync LIFECYCLE (role chosen, connect/disconnect status, page sent/received)
  // to the worker's /log ring buffer, batched ~1s. Then `GET /log` reveals the whole handshake from
  // both sides — turning "follower stuck" guesses into a timeline. Best-effort: never blocks sync.
  const dbgDeviceRef = useRef<string>("?");
  const dbgBufferRef = useRef<Array<Record<string, unknown>>>([]);
  const dbgFlushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // TELEMETRY IS OPT-IN AND OFF BY DEFAULT (Miguel, 2026-08-18: "we only want to turn on the faucet
  // when we need water not suck up a whole lake").
  //
  // It has zero user value — it exists to debug the mesh — and it is what exhausted the account's
  // 100,000-request daily Worker quota twice, on 2026-08-17 and again on 2026-08-18, taking
  // signovivo.com down WITH the relay both times because they share one quota. It is also useless
  // at Mass, where followers have no internet and these POSTs are dropped on the floor.
  //
  // Turn it on deliberately for a debugging session via the ⌕ diagnostics dump or by setting
  // sv.telemetry, and it stays on only for that install.
  const telemetryEnabledRef = useRef(false);
  // Where breadcrumbs go. Empty = the Cloudflare worker; a LAN URL = scripts/log-sink.mjs running on
  // Miguel's Mac. During a debugging session the devices are metres from that machine on the same
  // wifi, so there is no reason for their telemetry to cross the internet — and every request that
  // does is drawn from the SAME 100,000/day account quota signovivo.com depends on. Pointing here
  // makes debugging cost the product nothing.
  //
  // TEMPORARY DEFAULT (Miguel, 2026-08-18): pre-fill the sink field with his Mac's LAN address so
  // the diagnostics dump opens with it already typed — he still flips telemetry ON himself, this
  // only saves re-typing an IP. ONLY EVER A PRE-FILL, never an override: the moment a device saves
  // ANY value (via the diagnostics dump), that saved value wins forever and this constant is never
  // consulted again — see the `?? DEFAULT_LOG_SINK` below.
  //
  // This IP is only valid on Miguel's home network and will go stale the moment his router hands
  // out a different lease. Delete this default after this weekend's testing is done; it is not a
  // permanent address to bake into the app.
  const DEFAULT_LOG_SINK = "http://192.168.1.197:8787";
  const logSinkRef = useRef<string>(DEFAULT_LOG_SINK);
  useEffect(() => {
    AsyncStorage.multiGet(["sv.telemetry", "sv.logSink"])
      .then((pairs) => {
        const map = Object.fromEntries(pairs);
        telemetryEnabledRef.current = map["sv.telemetry"] === "1";
        // AsyncStorage.multiGet resolves a MISSING key to null, never undefined — checking for
        // undefined here meant "never saved" was never true, so String(null) ran and the field
        // showed the literal text "null" on every device that had not explicitly saved a sink.
        // Distinguish "never saved" (null) from "saved as empty, meaning use the worker" ("") —
        // only the former falls back to the default; an explicit empty save must still stick.
        const saved = map["sv.logSink"];
        logSinkRef.current = (saved === null || saved === undefined ? DEFAULT_LOG_SINK : saved).replace(/\/+$/, "");
      })
      .catch(() => {});
  }, []);

  // ── TELEMETRY LEVELS ───────────────────────────────────────────────────────
  //
  // Off-by-default fixed the idle cost; this fixes the ON cost. Turning telemetry on used to mean
  // turning on EVERYTHING — a 1 Hz mesh heartbeat, every BLE packet, every discovery tick — which is
  // the firehose that exhausted a 100,000/day account quota twice in two days.
  //
  // Mirrors sync-worker/src/logBuffer.js exactly (LOG_LEVELS / levelForEvent); a test executes BOTH
  // and asserts they classify identically, because two copies of a rule drift the moment one moves.
  // The worker echoes logLevel on every POST /log, so `LOG_LEVEL` + `wrangler deploy` retunes the
  // fleet in ~20s with no TestFlight round trip.
  //
  // INFERRED FROM THE EVENT NAME rather than passed at the call site: a level argument that must be
  // remembered at ~100 call sites is one that will be forgotten, and forgetting would default to
  // the loudest setting — which is exactly the failure being fixed.
  const LOG_LEVELS = { off: 0, error: 1, warn: 2, info: 3, debug: 4 } as const;
  const levelForEvent = useCallback((event: string): number => {
    const e = String(event || "");
    if (/error|STORM|conflict|denied|fail|wedged|revoke|abort/i.test(e)) return LOG_LEVELS.error;
    if (/stale|retry|reconnect|rebuild|half-open|not-director|skip|cold/i.test(e)) return LOG_LEVELS.warn;
    if (/^(become|role|boot|resync|director|follower)[:.]?|ready|start|stop/i.test(e)) return LOG_LEVELS.info;
    return LOG_LEVELS.debug;
  }, []);
  // Starts OFF. Raised only by the policy the worker echoes back, so the resting state of a device
  // that never talks to the relay is silence.
  const logLevelRef = useRef<number>(LOG_LEVELS.off);
  const dbgFlush = useCallback(() => {
    const batch = dbgBufferRef.current;
    if (batch.length === 0) return;
    if (!telemetryEnabledRef.current) { dbgBufferRef.current = []; return; }

    // WHERE IT GOES DECIDES WHETHER IT MAY BE KEPT.
    //
    // To the Cloudflare worker: drop on failure. A queue that survives a Mass is a burst waiting to
    // fire the moment a device finds wifi — the same outage with a delay on it, drawn from the same
    // 100,000/day quota signovivo.com lives on.
    //
    // To the LAN sink (scripts/log-sink.mjs on Miguel's Mac): KEEP it. There is no quota to blow, and
    // this is the only way to see what happened at MASS, where the iPads join no network at all and
    // telemetry has simply never existed. Buffer through the whole run with no connectivity, rejoin
    // wifi afterwards, and the trace flushes to the Mac. Post-hoc telemetry from a no-network room.
    //
    // Bounded hard: the cap discards the OLDEST rows, because when a buffer overflows the interesting
    // part is what happened most recently, and an unbounded buffer on a device with no network is a
    // memory leak with a nice name.
    const sink = logSinkRef.current;
    dbgBufferRef.current = [];
    // TIME-BOX THE POST. On a wifi-off device this should fail fast (no route), but "should" is not
    // a bound — a hung request would sit on the flush timer with no signal for the whole test, which
    // is exactly the run we cannot afford to lose data from. 4s is generous for a LAN and short
    // enough that it never meaningfully delays the next flush cycle either way.
    const ac = typeof AbortController === "function" ? new AbortController() : undefined;
    const timer = ac ? setTimeout(() => ac.abort(), 4000) : null;
    fetch(`${sink || RELAY_BASE}/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(batch),
      signal: ac?.signal,
    })
      .then((r) => {
        // Adopt the flush cadence and level the sink (or worker) hands back, so either can retune
        // this device without a rebuild.
        r.json?.().then((j: { policy?: { logIntervalMs?: number; logLevel?: number } }) => {
          const p = j?.policy;
          if (p && Number.isFinite(p.logLevel)) logLevelRef.current = Number(p.logLevel);
        }).catch(() => {});
      })
      .catch(() => {
        if (!sink) return;   // worker: drop, per above
        const MAX = 5000;
        const merged = batch.concat(dbgBufferRef.current);
        dbgBufferRef.current = merged.length > MAX ? merged.slice(merged.length - MAX) : merged;
      })
      .finally(() => { if (timer) clearTimeout(timer); });
  }, []);
  const dbgLog = useCallback(
    (event: string, data?: Record<string, unknown>) => {
      try {
        // DROP BEFORE BUFFERING, not at flush. A buffer that fills at debug volume and is discarded
        // later still costs the memory and still exists to be accidentally flushed; the cheapest
        // request is the one never assembled.
        if (levelForEvent(event) > logLevelRef.current) return;
        dbgBufferRef.current.push({
          t: Date.now(),
          dev: dbgDeviceRef.current,
          role: roleRef.current,
          build: BUILD_VERSION,
          event,
          ...(data || {}),
        });
        if (dbgFlushTimerRef.current) clearTimeout(dbgFlushTimerRef.current);
        dbgFlushTimerRef.current = setTimeout(dbgFlush, 1000);
      } catch {
        /* ignore */
      }
    },
    [dbgFlush, levelForEvent],
  );
  // ── Fleet readiness check-in (native → the SAME /fleet dashboard as signovivo.com) ──
  // Reports this iPad's native build + role so the director sees who's ready before Mass. Reuses
  // the stable sv_devid; sends a self-entered label but NEVER a phone number. Best-effort.
  // ── Fleet readiness check-in — REMOVED (Miguel, 2026-08-18: "kill the fleet dashboard") ──
  //
  // It posted every 90 SECONDS from every device: 960/day each, ~3,840/day across the fleet — more
  // than the director's relay keepalive, spent so a pre-Mass page could show green lights. It also
  // stored a roster containing choir phone numbers, so deleting it is a privacy win as much as a
  // quota one.
  //
  // Kept as a no-op rather than deleted at ~6 call sites: those sites are inside offline-download
  // and cache-verification paths where an edit risks more than it saves, and this way the next
  // reader sees WHY nothing happens instead of finding a mysterious absence.
  // Stable per-install device id so the two devices are distinguishable in the log timeline.
  useEffect(() => {
    (async () => {
      try {
        let id = await AsyncStorage.getItem("sv_devid");
        if (!id) {
          id = Math.random().toString(36).slice(2, 8);
          await AsyncStorage.setItem("sv_devid", id);
        }
        dbgDeviceRef.current = id;
      } catch {
        dbgDeviceRef.current = "?";
      }
      dbgLog("boot", { syncAvailable });
      // Fleet identity: report readiness under whatever label this device ALREADY has. The
      // one-time "¿Quién usa este iPad?" Alert.prompt is gone.
      //
      // PR #270 removed this modal and its message says "(web PWA only — native does its own
      // check-in)" — but the NATIVE copy survived here and still fired on every fresh install,
      // putting a keyboard-bearing alert over the songbook on first launch. That is exactly the
      // friction #270 set out to delete, and it lands on all 6-8 iPads after any TestFlight
      // round. Same call as #270: choir members mostly tapped "Ahora no", so the labels were
      // rarely filled and were not worth the boot-path interruption.
      //
      // Devices labelled before this build KEEP their label (the key is still read, just never
      // written); new devices report anonymously by sv_devid, matching the web PWA's
      // "anonymous by device" behaviour (web/src/app.js:2986). sv_fleet_skip is now dead — it
      // only ever existed to suppress this prompt.
      // (sv_fleet_label was read here to name this device on the readiness dashboard; the dashboard
      // is gone, so the label has nowhere to go and the key is simply left in storage.)
    })();
  }, [dbgLog, syncAvailable]);
  // Re-report readiness every 90s while mounted — captures a follower who later becomes director
  // without touching the liturgy-critical sync callbacks.
  // (the 90s fleet check-in heartbeat was removed with the dashboard — see fleetCheckin above)

  // ── Native -> Web injection (queued until the web app signals bridge-ready) ──
  // Bound the pending-inject backlog: if the WebView never signals bridge-ready (broken/blank
  // bundle, crash-loop), the 2s heartbeat + page events would otherwise grow native heap without
  // limit. Drop the oldest queued inject once we exceed the cap — newer events are what matter.
  const queueInject = useCallback((js: string) => {
    if (pendingInjectRef.current.length > 100) pendingInjectRef.current.shift();
    pendingInjectRef.current.push(js);
  }, []);

  const injectEvent = useCallback(
    (payload: Record<string, unknown>) => {
      const js =
        `window.__signoVivoReceiveNativeEvent && window.__signoVivoReceiveNativeEvent(${JSON.stringify(
          payload,
        )}); true;`;
      if (!webReadyRef.current || !webViewRef.current) {
        queueInject(js);
        return;
      }
      try {
        webViewRef.current.injectJavaScript(js);
      } catch {
        queueInject(js);
      }
    },
    [queueInject],
  );

  const flushPendingInjects = useCallback(() => {
    const ref = webViewRef.current;
    if (!ref) return;
    const queued = pendingInjectRef.current.splice(0, pendingInjectRef.current.length);
    for (const js of queued) {
      try {
        ref.injectJavaScript(js);
      } catch {
        /* drop */
      }
    }
  }, []);

  /**
   * Take the Documents bundle out of play by RENAMING it — never deleting it.
   *
   * Two reasons it is a rename. It is the forensic evidence for *why* a bundle failed to boot, on a
   * device we will never have in front of us; and deleting ~27 MB of user-visible state on a
   * heuristic is exactly the irreversible move that should never be automatic. The boot sweep keeps
   * only the newest quarantined copy, so this cannot grow without bound.
   *
   * Declared here, above the watchdog, because the self-heal ladder calls it directly.
   */
  const quarantineDocumentsBundle = useCallback(
    async (bookVersion?: string | null) => {
      const docDir = FileSystem.documentDirectory || "";
      if (!docDir) return;
      const from = `${docDir}WebBundle`;
      const to = `${docDir}WebBundle.bad-${Date.now()}`;
      try {
        const info = await FileSystem.getInfoAsync(from);
        if (!info.exists) return;
        await FileSystem.moveAsync({ from, to });
        breadcrumb(`quarantine:${bookVersion || "unknown"}`);
      } catch {
        breadcrumb("quarantine-failed");
        return;
      }
      // The cached decision now points at a directory that no longer exists — drop it so the next
      // resolve runs the full table instead of trusting a stale answer.
      await AsyncStorage.removeItem(STORAGE_KEYS.bookResolved).catch(() => {});
      if (bookVersion) {
        try {
          const raw = await AsyncStorage.getItem(STORAGE_KEYS.bookQuarantine);
          const list = raw ? JSON.parse(raw) : [];
          await AsyncStorage.setItem(
            STORAGE_KEYS.bookQuarantine,
            JSON.stringify(recordBundleFailure(Array.isArray(list) ? list : [], bookVersion, Date.now())),
          );
        } catch {
          /* best-effort: a bookkeeping failure must not block the recovery itself */
        }
        await AsyncStorage.setItem(
          STORAGE_KEYS.bookReverted,
          JSON.stringify({ bookVersion, at: Date.now() }),
        ).catch(() => {});
        setRevertedBook(bookVersion);
      }
    },
    [breadcrumb],
  );

  // Slice B: watchdog for the bridge-ready handshake. Armed on every WebView (re)load; cleared
  // when bridge-ready arrives. If it fires, the bundle never booted (broken/blank/crash-loop) →
  // escalate: up to 2 bounded remounts (cheap; fixes a transient WKWebView wedge), then the
  // native fallback view. 6s is generous vs a ~1s local-bundle boot, so a slow device won't
  // false-fire into a spurious remount.
  const armBridgeWatchdog = useCallback(() => {
    if (bridgeWatchdogRef.current) clearTimeout(bridgeWatchdogRef.current);
    bridgeWatchdogRef.current = setTimeout(() => {
      bridgeWatchdogRef.current = null;
      if (webReadyRef.current) return; // bridge-ready arrived in time — healthy boot
      const attempt = remountAttemptsRef.current;
      const source = activeBundleSourceRef.current;
      breadcrumb(`bridge-timeout:${attempt}:${source || "?"}`);

      // THE SELF-HEAL LADDER (defect D2). The old behaviour was two bounded remounts of the SAME
      // uri and then a "Reintentar" button that remounted the same uri again — an unbounded
      // human-driven loop with no escape, which never once tried the code-signed copy sitting right
      // there on disk. Now: remount (a transient WKWebView wedge is still the cheapest hypothesis),
      // then abandon the bundle we just failed on and re-resolve, then the native floor.
      //
      // Crucially the Multipeer session is never torn down by any of this, so a follower keeps
      // following the director through the entire recovery.
      const { action, quarantineCurrent } = nextHealAction(attempt, source);
      remountAttemptsRef.current = attempt + 1;
      pendingInjectRef.current = []; // stale injects must not flush into a fresh page

      if (action === "remount") {
        setMountKey((k) => k + 1); // re-arms via the mount effect
        return;
      }
      if (action === "give-up") {
        // The floor is now guaranteed to be showing over a code-signed, read-only bundle that no
        // downloader can corrupt.
        setWebDead(true);
        return;
      }

      // fall-back: stop trusting this bundle, re-resolve, remount onto whatever the table picks.
      (async () => {
        try {
          if (quarantineCurrent) await quarantineDocumentsBundle(activeBookVersionRef.current);
          const next = await resolveBundleUriRef.current?.();
          if (next) {
            webReadyRef.current = false;
            setBundleUri(next);
          }
          setMountKey((k) => k + 1);
        } catch {
          setWebDead(true); // never leave the user with no UI because recovery itself threw
        }
      })();
    }, 6000);
  }, [breadcrumb, quarantineDocumentsBundle]);

  // Arm the watchdog whenever a WebView (re)mounts (initial boot + every mountKey remount). A
  // fresh mount hasn't handshaked yet, so reset webReadyRef; bridge-ready clears the timer.
  useEffect(() => {
    if (!booted || !bundleUri) return;
    // `webDead` is deliberately NOT a guard or a dependency any more. The floor is an overlay now,
    // so the WebView stays mounted underneath it and a Reintentar remount must still be WATCHED —
    // otherwise a retry onto a bundle that also fails would sit there forever with the ladder
    // disarmed. Keeping webDead out of the deps also stops the watchdog re-arming merely because
    // the overlay was shown or hidden.
    webReadyRef.current = false;
    armBridgeWatchdog();
    return () => {
      if (bridgeWatchdogRef.current) {
        clearTimeout(bridgeWatchdogRef.current);
        bridgeWatchdogRef.current = null;
      }
    };
  }, [mountKey, booted, bundleUri, armBridgeWatchdog]);

  // ── Relay-auth warning bridge ────────────────────────────────────────────────
  // The relay silently rejects a publish when the director's X-Director-Code is bad (401).
  // directorRelaySync latches it to one shot; here we forward that single event into the WebView so
  // the director SEES that every signovivo.com follower has gone dark, instead of the app looking
  // fine while the web congregation is frozen.
  useEffect(() => {
    setRelayAuthErrorHandler((status: number) => {
      injectEvent({ type: "relay-auth-error", status });
    });
    // ...and the other half: tell the WebView when publishing starts working again, so the banner
    // comes DOWN on its own. Without this it could only be dismissed by hand — a warning about a
    // problem that no longer exists, indistinguishable on screen from a live one.
    setRelayAuthOkHandler(() => {
      injectEvent({ type: "relay-auth-ok" });
    });
    return () => {
      setRelayAuthErrorHandler(null);
      setRelayAuthOkHandler(null);
    };
  }, [injectEvent]);

  // ── Page broadcast: mesh (director) + relay (director or explicit transmitter) ──
  const broadcastPage = useCallback((rawPage: number, book: BookId) => {
    // H4: never broadcast a non-positive page. The follower render-failed sentinel sets
    // currentPageRef to -1; if this device then becomes director before a real page lands, a raw
    // broadcast of -1 would clamp to page 1 on every follower — yanking the whole congregation.
    // Floor to a real page at the single choke point that feeds BOTH transports.
    const page = Number.isFinite(rawPage) && rawPage >= 1 ? rawPage : 1;
    const mode = modeForBook(book);
    const total = totalPagesRef.current;
    if (roleRef.current === "director") {
      dbgLog("page:send", { page, book });
      sendNearbyDirectorPageUpdate(page, total, { mode, bookId: book }).catch(() => {});
    }
    if (roleRef.current === "director" || explicitTransmitterRef.current) {
      try {
        publishPageToRelay(page, total, { mode, bookId: book });
      } catch {
        /* network flaps are expected; next page change re-publishes */
      }
    }
  }, []);

  // ── Director re-broadcast heartbeats ────────────────────────────────────────
  // MESH every 1s: a tiny local Multipeer re-send. Followers DE-DUPE a same-page re-send
  // (see the "page" case below), so this only does work when a page-turn packet was dropped —
  // and then the follower recovers within ~1s. A fast cadence is free on a LAN, and it also
  // feeds the follower's half-open watchdog (silence >3s ⇒ force reconnect).
  // RELAY every 12s: page CHANGES already publish to the relay immediately (broadcastPage),
  // so this only refreshes the snapshot's `ts` to keep signovivo.com followers "live"
  // (RELAY_LIVE_MAX_AGE_S=90). Kept slow so weak-cell web followers aren't pushed a frame
  // every couple seconds (they de-dupe renders, but it's still needless radio).
  const stopDirectorHeartbeat = useCallback(() => {
    if (meshHeartbeatRef.current) {
      clearInterval(meshHeartbeatRef.current);
      meshHeartbeatRef.current = null;
    }
    if (relayHeartbeatRef.current) {
      clearInterval(relayHeartbeatRef.current);
      relayHeartbeatRef.current = null;
    }
  }, []);

  const startDirectorHeartbeat = useCallback(() => {
    stopDirectorHeartbeat();
    meshHeartbeatRef.current = setInterval(() => {
      if (roleRef.current !== "director") return;
      const book = currentBookRef.current;
      // Keep "when was this device last directing" fresh, throttled hard because this ticks every
      // second and AsyncStorage is a real write. The boot path resumes only inside a short window
      // after the LAST heartbeat, not after the moment the role was taken — a director who started
      // at the beginning of Mass and crashed forty minutes in must still be inside the window.
      const nowMs = Date.now();
      if (nowMs - lastDirectorAtWrittenRef.current >= DIRECTOR_STAMP_THROTTLE_MS) {
        lastDirectorAtWrittenRef.current = nowMs;
        AsyncStorage.setItem(STORAGE_KEYS.lastDirectorAt, String(nowMs)).catch(() => {});
      }
      sendNearbyDirectorPageUpdate(currentPageRef.current, totalPagesRef.current, {
        mode: modeForBook(book),
        bookId: book,
      }).catch(() => {});
    }, 1000);
    relayHeartbeatRef.current = setInterval(() => {
      if (roleRef.current !== "director" && !explicitTransmitterRef.current) return;
      const book = currentBookRef.current;
      try {
        publishPageToRelay(currentPageRef.current, totalPagesRef.current, {
          mode: modeForBook(book),
          bookId: book,
        });
      } catch {
        /* network flaps are expected; the next tick / page change re-publishes */
      }
      // 12s -> 30s. This is ONLY a keepalive: every real page turn publishes immediately via
      // broadcastPage, so its sole job is keeping the snapshot inside RELAY_LIVE_MAX_AGE_S (90s) so
      // followers still count the director as live. At 12s it spent 7,200 requests a day to say
      // nothing had changed — 7% of the account's entire daily quota, on a keepalive.
      //
      // 30s, not 45s: at 45 a SINGLE lost publish reaches the 90s window and every follower declares
      // the director dead. 30 gives three attempts inside the window and still costs only 2,880/day.
    }, 30000);
  }, [stopDirectorHeartbeat]);

  // ── Become follower ──────────────────────────────────────────────────────────
  // Declared BEFORE becomeDirector: the live-takeover failure path in becomeDirector
  // falls back to becomeFollower, so it must be initialized first (avoids a TDZ in the
  // useCallback dependency array).
  const becomeFollower = useCallback(async () => {
    // Claim this role transition; a later flip bumps the generation and supersedes us.
    const myGen = ++roleGenerationRef.current;
    dbgLog("become:follower", { syncAvailable });
    roleRef.current = "follower";
    explicitTransmitterRef.current = false;
    stopDirectorHeartbeat(); // a follower must never re-broadcast
    // C3: stop publishing on step-down. Publishes are coalesced, so an in-flight one can drain a
    // final straggler frame AFTER we have stepped down. That used to be caught by the relay (no
    // code → 401 → never applied); /publish is open now, so the refusal has to happen HERE or the
    // ex-director's stale page lands on every web follower. becomeDirector re-enables it, so a
    // legitimate re-direct is unaffected.
    setRelayPublishing(false);
    try {
      await AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower");
      if (myGen !== roleGenerationRef.current) return; // superseded while persisting
      if (syncAvailable) {
        try {
          await startNearbyFollower(DIRECTOR_SESSION);
        } catch {
          // Transient mesh startup failure (permission race, radio warm-up):
          // wait briefly and retry the start exactly once before giving up.
          await new Promise((r) => setTimeout(r, 2000));
          if (myGen !== roleGenerationRef.current) return; // superseded during the retry sleep
          await startNearbyFollower(DIRECTOR_SESSION);
        }
      }
    } catch {
      /* ignore */
    }
    if (myGen !== roleGenerationRef.current) return; // superseded while the mesh was starting
    injectEvent({ type: "role", role: "follower" });
  }, [syncAvailable, injectEvent, stopDirectorHeartbeat]);

  // ── Become director ────────────────────────────────────────────────────────
  const becomeDirector = useCallback(
    async (code: string) => {
      // Claim this role transition; a later flip bumps the generation and supersedes us.
      const myGen = ++roleGenerationRef.current;
      becomeDirectorInFlightRef.current = true;
      // Allow this device to publish. BEFORE any broadcast, or the first frame is dropped by the
      // gate in directorRelaySync. No credential is involved — the relay stopped authorizing.
      setRelayPublishing(true);
      dbgLog("become:director", { wasFollower: roleRef.current === "follower", syncAvailable });
      if (!syncAvailable) {
        // No mesh on this device — still act as an online transmitter to the relay.
        explicitTransmitterRef.current = true;
        roleRef.current = "off";
        // H3: persist a NON-credential role breadcrumb (like the mesh director path does). The
        // transmitter role lives ONLY in the in-memory explicitTransmitterRef, so a NATIVE APP
        // restart (crash / memory-kill — not just a WebView reload) would lose it: the bootstrap
        // would read no "director" role, come back as a silent follower, and the relay heartbeat
        // would never restart → every signovivo.com follower frozen for the rest of Mass with no
        // 401 signal. Persisting "director" makes the boot resume prompt fire so the operator
        // resumes. This device has no mesh, so it can only ever be a relay transmitter.
        AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "director").catch(() => {});
        lastDirectorAtWrittenRef.current = Date.now();
        AsyncStorage.setItem(STORAGE_KEYS.lastDirectorAt, String(Date.now())).catch(() => {});
        bumpDirectorSessions();
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat(); // keep the relay snapshot fresh (guarded on explicitTransmitterRef)
        becomeDirectorInFlightRef.current = false;
        return;
      }
      // LIVE-DIRECTOR TAKEOVER: if we're entering a director code while currently a CONNECTED
      // follower, Swift's startDirector guard rejects with DIRECTOR_TAKEOVER_REQUIRED
      // (currentRole=="follower" && connectedDirectorPeer != nil). Drop the follower link FIRST
      // so the guard passes, then start as director (the most-recent token wins on contention).
      const wasFollower = roleRef.current === "follower";
      if (wasFollower) {
        stopDirectorHeartbeat(); // belt-and-suspenders; a follower shouldn't have one running
        try {
          await resetNearbyDirectorSync();
        } catch {
          /* best-effort drop; startDirector below still attempts the takeover */
        }
        if (myGen !== roleGenerationRef.current) { becomeDirectorInFlightRef.current = false; return; } // superseded while dropping the link
      }
      try {
        try {
          await startNearbyDirector(DIRECTOR_SESSION);
        } catch {
          // Mesh startup can transiently fail (permission race, radio warm-up).
          // Wait briefly and retry the start exactly once before giving up.
          await new Promise((r) => setTimeout(r, 2000));
          if (myGen !== roleGenerationRef.current) { becomeDirectorInFlightRef.current = false; return; } // superseded during the retry sleep
          await startNearbyDirector(DIRECTOR_SESSION);
        }
        if (myGen !== roleGenerationRef.current) { becomeDirectorInFlightRef.current = false; return; } // superseded while the mesh was starting
        roleRef.current = "director";
        // Record the role AND when. lastDirectorAt is what the boot path reads to tell a crash
        // twenty seconds ago from a cold start next Sunday; the heartbeat keeps it fresh from here.
        // No credential is stored — DIRECTOR_CODE is a constant in this file, not a secret.
        await AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "director");
        lastDirectorAtWrittenRef.current = Date.now();
        AsyncStorage.setItem(STORAGE_KEYS.lastDirectorAt, String(Date.now())).catch(() => {});
        bumpDirectorSessions();
        if (myGen !== roleGenerationRef.current) { becomeDirectorInFlightRef.current = false; return; } // superseded while persisting
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat();
        // SPLIT-BRAIN MITIGATION: a brand-new director's token must propagate fast so peers
        // (and any prior director) re-find it and converge, instead of waiting out the ~25s
        // browse cycle while both broadcast and followers flap. Kick an immediate re-browse.
        //
        // BROWSER ONLY. This called refreshNearbyDiscovery, which destroys the ADVERTISER first —
        // at the exact moment every follower's foundPeer has fired and their invites are in flight,
        // so those invites evaporated silently and each follower then waited out a timeout before
        // retrying. The same failure was already diagnosed and guarded on the follower side; the
        // director had no equivalent protection while doing it to everyone reaching for it.
        if (syncAvailable) refreshDirectorBrowse().catch(() => {});
        breadcrumb("director");
        becomeDirectorInFlightRef.current = false;
      } catch {
        // A live-takeover attempt that still failed (e.g. Swift DIRECTOR_TAKEOVER_REQUIRED raced
        // back, or a transient mesh error) must NOT strip a connected follower's UI to "none".
        if (wasFollower) {
          // We already dropped the follower link via resetNearbyDirectorSync above; re-join the
          // mesh as a follower so the device keeps syncing instead of stranding link-less.
          // (becomeFollower bumps the generation, so this also supersedes our own stale path.)
          if (myGen === roleGenerationRef.current) becomeFollower();
        } else {
          injectEvent({ type: "role", role: "none" });
        }
        // Released on EVERY exit. A single stuck `true` would wedge the resume guard forever —
        // the device would refuse to auto-resume for the rest of the session with no signal.
        becomeDirectorInFlightRef.current = false;
      }
    },
    [
      syncAvailable,
      injectEvent,
      broadcastPage,
      breadcrumb,
      startDirectorHeartbeat,
      stopDirectorHeartbeat,
      becomeFollower,
    ],
  );

  // ── Soft reset (secret code 744668486) ─────────────────────────────────────
  const performSoftReset = useCallback(async () => {
    breadcrumb("soft-reset");
    roleGenerationRef.current++; // supersede any in-flight become* from the prior role
    stopDirectorHeartbeat();
    try {
      await resetNearbyDirectorSync();
    } catch {
      /* ignore */
    }
    try {
      await AsyncStorage.removeItem(STORAGE_KEYS.lastSyncRole);
    } catch {
      /* ignore */
    }
    roleRef.current = "off";
    explicitTransmitterRef.current = false;
    webReadyRef.current = false;
    pendingInjectRef.current = [];
    // RIDER: re-resolve before remounting. A soft reset is a recovery action, so it must be able to
    // move OFF a bad bundle — remounting the same broken URI was one of the one-way doors that made
    // a bad Documents/WebBundle unrecoverable without reinstalling the app.
    try {
      const next = await resolveBundleUriRef.current?.();
      if (next) setBundleUri(next);
    } catch {
      /* keep the current URI rather than leaving the app with none */
    }
    setMountKey((k) => k + 1); // remount the WebView from scratch
  }, [breadcrumb, stopDirectorHeartbeat]);

  // ── Director-code dispatch (codes entered on the web numpad) ────────────────
  const onDirectorCode = useCallback(
    (rawCode: unknown) => {
      // NEW-DIR-2: do NOT bump roleGenerationRef here. A code entry is not yet a committed role
      // change — becomeDirector (:400), becomeFollower (:370), and performSoftReset (:479) each bump
      // on their OWN commit. Bumping up front superseded an in-flight boot becomeFollower: if the
      // confirm dialog below was CANCELLED while becomeFollower was in its retry-sleep, the follower
      // saw its generation superseded and returned WITHOUT establishing the mesh link → the device
      // was stranded as a link-less follower. With no bump, a Cancel leaves the follower intact.
      const code = digitsOnly(rawCode);
      if (!code) {
        injectEvent({ type: "role", role: "none" });
        return;
      }
      // Apply a downloaded songbook. Deliberately a CODE and not an ambient prompt: an automatic
      // modal on `ready` fires on seven devices at 12:04 with instruments in hand, and a persisted
      // flag makes that a certainty rather than a risk. A code is an unambiguous human act.
      if (code === BOOK_APPLY_CODE) {
        void applyStagedBookRef.current?.();
        return;
      }
      // Operator panic switch: force the code-signed bundle. Auto-expires (a forced device nobody
      // remembers forcing is its own outage), and takes effect on the next mount.
      if (code === BOOK_FORCE_BAKED_CODE) {
        void (async () => {
          await AsyncStorage.setItem(
            STORAGE_KEYS.bookForceBundled,
            JSON.stringify({ setAt: Date.now() }),
          ).catch(() => {});
          await AsyncStorage.removeItem(STORAGE_KEYS.bookResolved).catch(() => {});
          breadcrumb("force-baked");
          const next = await resolveBundleUriRef.current?.("bundled");
          webReadyRef.current = false;
          pendingInjectRef.current = [];
          if (next) setBundleUri(next);
          setMountKey((k) => k + 1);
          Alert.alert("Cancionero original", "Se restauró el cancionero incluido en el app.");
        })();
        return;
      }
      if (code === SOFT_RESET_CODE) {
        performSoftReset();
        return;
      }
      // Any valid director code may take the role. A valid code does NOT promote SILENTLY — confirm
      // first, so entering your code at Mass/practice never yanks the role from a director who is
      // already live (Miguel, 2026-07-02).
      const isDirectorCode = code === DIRECTOR_CODE;
      if (!isDirectorCode) {
        // Unrecognized → tell the web so it surfaces "código incorrecto".
        injectEvent({ type: "role", role: "none" });
        return;
      }
      // ALWAYS ask — a valid code never promotes silently (Miguel: "always ask, always").
      // Super-admin codes (Miguel) get a labeled prompt; everyone else is a plain director.
      // Best-effort heads-up: lastDirectorSnapshotRef is set whenever a director's page has arrived
      // over the mesh, so if it's set another device is (or was just) directing — warn before takeover.
      // NEW-DIR-3: only warn about taking over a director who is live RIGHT NOW (a fresh mesh
      // snapshot within the heartbeat window) — not one who directed earlier this session and left.
      // Previously this was `Boolean(lastDirectorSnapshotRef.current)`, a set-once-never-cleared flag,
      // so the scary red "Ya hay un director activo / Tomar el control" warning false-fired forever.
      const snap = lastDirectorSnapshotRef.current;
      const liveDirector =
        roleRef.current !== "director" &&
        Boolean(snap) &&
        Date.now() - (snap?.at ?? 0) < LIVE_DIRECTOR_WINDOW_MS;
      const title = liveDirector ? "⚠️ Ya hay un director activo" : "¿Dirigir el coro?";
      const body = liveDirector
        ? "Otro dispositivo está dirigiendo AHORA. Si continúas, tú tomas el control y todos te seguirán a ti."
        : "Los demás dispositivos seguirán tu página. Si otro director ya está activo, le quitarás el control.";
      Alert.alert(title, body, [
        // Cancel: do nothing — stay exactly as you were (a follower keeps following the real director).
        { text: "Cancelar", style: "cancel" },
        {
          text: liveDirector ? "Tomar el control" : "Sí, dirigir",
          style: liveDirector ? "destructive" : "default",
          onPress: () => becomeDirector(code),
        },
      ]);
    },
    [injectEvent, performSoftReset, becomeDirector],
  );

  // ── Web -> Native message router ───────────────────────────────────────────
  const handleMessage = useCallback(
    (event: WebViewMessageEvent) => {
      let msg: Record<string, unknown> | null = null;
      try {
        msg = JSON.parse(event.nativeEvent.data);
      } catch {
        return;
      }
      if (!msg || msg.channel !== "signovivo-native") return;

      switch (msg.type) {
        case "bridge-ready": {
          webReadyRef.current = true;
          // The recovery floor is dismissed HERE and nowhere else. Reintentar deliberately leaves
          // it up: a mount that has not handshaked is not a recovery, and hiding the only working
          // control before we know that would be the black-rectangle bug all over again.
          setWebDead(false);
          // Slice B: the web booted — disarm the watchdog and reset the remount budget so a
          // LATER crash gets its full escalation ladder again.
          if (bridgeWatchdogRef.current) {
            clearTimeout(bridgeWatchdogRef.current);
            bridgeWatchdogRef.current = null;
          }
          remountAttemptsRef.current = 0;

          // PROVE THE BUNDLE. Reaching bridge-ready is the only evidence that the bundle we chose
          // actually boots, so it is what clears the hard-crash counter and the quarantine strikes
          // against this bookVersion. Without the explicit reset (red team NI5), three transient
          // failures spread over months would blacklist a book that works fine today — permanently,
          // by content hash, on the device that can least afford it.
          void (async () => {
            const bookVersion = activeBookVersionRef.current;
            try {
              await AsyncStorage.setItem(
                STORAGE_KEYS.bookBoot,
                JSON.stringify({ bookVersion, mountedAt: Date.now(), provedAt: Date.now(), attempts: 0 }),
              );
              if (bookVersion) {
                const raw = await AsyncStorage.getItem(STORAGE_KEYS.bookQuarantine);
                const list = raw ? JSON.parse(raw) : [];
                if (Array.isArray(list) && list.length) {
                  await AsyncStorage.setItem(
                    STORAGE_KEYS.bookQuarantine,
                    JSON.stringify(clearBundleFailures(list, bookVersion)),
                  );
                }
              }
              // …AND RETIRE THE BANNER, for the same reason and at the same moment.
              //
              // The failure COUNTER was cleared here on a proven boot; the LIBRO ANTERIOR banner
              // was not, and its only other exit is an operator tapping it. So a device that
              // reverted once — then updated, healed, and booted cleanly every day since — kept
              // showing a permanent red warning about something that had stopped being true. It
              // survives app updates too (AsyncStorage does), so a 393-era revert was still on
              // screen after installing 395.
              //
              // The banner's job is "this device is silently on an older book, tell the director".
              // A proven boot of the CURRENT book is exactly the evidence that is no longer so.
              // It stays fully intact while the condition holds: nothing here fires unless the
              // bundle actually reached bridge-ready.
              await AsyncStorage.removeItem(STORAGE_KEYS.bookReverted).catch(() => {});
              setRevertedBook(null);
            } catch {
              /* best-effort */
            }
          })();

          // A3: a DIRECTOR/transmitter is authoritative for the page across a WebView reload. A
          // content-process reload boots the web to its DEFAULT page (2) and reports it here; adopting
          // that (and re-broadcasting below) would yank the WHOLE congregation to the boot page — the
          // single worst live-Mass bug. So a director/transmitter does NOT adopt the web's page here;
          // it re-asserts its OWN currentPageRef. Only a follower adopts the web's reported page.
          const isDirectorAuthority =
            roleRef.current === "director" || explicitTransmitterRef.current;
          if (!isDirectorAuthority && typeof msg.page === "number") currentPageRef.current = msg.page;
          if (typeof msg.totalPages === "number") totalPagesRef.current = msg.totalPages;
          if (isBookId(msg.book)) currentBookRef.current = msg.book;
          flushPendingInjects();
          injectEvent({ type: "bridge-state", available: syncAvailable });
          // Always re-assert the current role to a freshly (re)loaded WebView so the web app's
          // numpad/role UI matches reality after a crash-reload or boot. A transmitter-only device
          // has roleRef "off" but must still re-assert "director" (it keeps publishing to the relay),
          // otherwise a crash-reload would silently strip its director/transmitter UI to "none".
          const assertedRole =
            roleRef.current === "director" || explicitTransmitterRef.current
              ? "director"
              : roleRef.current === "off"
                ? "none"
                : roleRef.current;
          injectEvent({ type: "role", role: assertedRole });
          if (isDirectorAuthority) {
            // A3 FIX: re-drive the freshly-reloaded web to the director's/transmitter's REAL page
            // (currentPageRef, which we did NOT let the boot page clobber above), then (re)broadcast
            // THAT — never the web's boot page. This is what stops a reload from moving the congregation.
            injectEvent({
              type: "sync-event",
              event: { type: "page", page: currentPageRef.current, book: currentBookRef.current },
            });
            broadcastPage(currentPageRef.current, currentBookRef.current);
          } else if (roleRef.current === "follower" && lastDirectorSnapshotRef.current) {
            // A RELOADED follower (we already have a director snapshot) must resync to the
            // director's last-known page instead of showing the web's stale/default boot page.
            // The null-guard above is critical: a FRESH-BOOT follower that has never received a
            // director snapshot keeps the web's own boot page (geo/stored) — we do NOT override.
            const { page, book } = lastDirectorSnapshotRef.current;
            currentPageRef.current = page;
            currentBookRef.current = book;
            // ONE event only: the page sync-event carries `book` and the web handler switches +
            // renders atomically. A separate set-book first made the web jump to the book DEFAULT
            // page, then the page event raced it — followers could flash the default or wedge.
            injectEvent({ type: "sync-event", event: { type: "page", page, book } });
            // H1: the cached snapshot can be stale if the director moved during the reload window.
            // Actively pull the director's CURRENT page (like the foreground path does) instead of
            // waiting for the next 1s heartbeat. Best-effort.
            if (syncAvailable) requestCurrentSnapshot().catch(() => {});
          }
          break;
        }
        case "page-changed": {
          // A3: a director/transmitter must never broadcast the web's unsolicited BOOT render. On a
          // WebView (re)load the web boots to its default page and posts page-changed BEFORE bridge-ready
          // — i.e. while webReadyRef is still false. For a director/transmitter, ignore that pre-ready
          // boot render (bridge-ready re-asserts the real page). Followers are unaffected (they don't
          // broadcast) and a director's REAL page turns, which happen after bridge-ready, work normally.
          if ((roleRef.current === "director" || explicitTransmitterRef.current) && !webReadyRef.current) {
            break;
          }
          // Clamp: a buggy/tampered web bundle must not push a wild page to mesh followers.
          // Floor at 1; cap at totalPages when we have a positive count.
          let page = Math.max(1, Number(msg.page) || currentPageRef.current);
          if (totalPagesRef.current > 0) page = Math.min(page, totalPagesRef.current);
          currentPageRef.current = page;
          if (typeof msg.totalPages === "number" && msg.totalPages > 0) {
            totalPagesRef.current = msg.totalPages;
          }
          if (isBookId(msg.book)) currentBookRef.current = msg.book;
          // Persist per-book last page for restore.
          AsyncStorage.setItem(
            `${STORAGE_KEYS.lastPagePrefix}${currentBookRef.current}`,
            String(page),
          ).catch(() => {});
          broadcastPage(page, currentBookRef.current);
          break;
        }
        case "director-code":
          onDirectorCode(msg.code);
          break;
        // The web UI asking for the role directly, with no code typed. Routed through the SAME
        // handler as a typed code — so it gets the same confirmation, the same live-director
        // takeover warning, and the same becomeDirector path. The web bundle never learns
        // DIRECTOR_CODE; it asks, and the shell decides.
        //
        // This handler ships in the BINARY even though the button that sends it ships over the air,
        // because a web bundle that sends a message no shell understands is a button that does
        // nothing. Native first, web whenever.
        case "request-director":
          onDirectorCode(DIRECTOR_CODE);
          break;
        // ── Debug settings, editable from the diagnostics dump ──────────────────────────────────
        //
        // sv.telemetry ("1" | "") and sv.logSink (a LAN URL) live in AsyncStorage, which only the
        // shell can write — so without this they could only be set by rebuilding, which is absurd
        // for two strings whose whole purpose is to be changed during a debugging session.
        //
        // They take effect on the NEXT flush, no relaunch needed. Pointing sv.logSink at
        // scripts/log-sink.mjs on Miguel's Mac also means telemetry costs Cloudflare nothing, so
        // the debug level can be raised without competing with signovivo.com for the daily quota.
        case "set-debug-settings": {
          const sink = String((msg as Record<string, unknown>).logSink ?? "").trim().replace(/\/+$/, "");
          const tel = (msg as Record<string, unknown>).telemetry ? "1" : "";
          logSinkRef.current = sink;
          telemetryEnabledRef.current = tel === "1";
          AsyncStorage.multiSet([["sv.logSink", sink], ["sv.telemetry", tel]]).catch(() => {});
          dbgLog("debug:settings", { sink: sink || "(worker)", telemetry: tel === "1" });
          injectEvent({ type: "toast", text: tel === "1"
            ? `Telemetría activada -> ${sink || "Cloudflare"}`
            : "Telemetría desactivada." });
          break;
        }
        // ── The two panic switches, reachable without remembering a number ──────────────────────
        // These exist for the five minutes before Mass when something has already gone wrong: a
        // device holding a broken book, or one whose role is wedged. Requiring a memorised 9-digit
        // code in exactly that moment is the same defect the director button removed, and worse
        // here — becoming director is routine enough to learn, while these fire once a year, under
        // pressure, when nobody can look anything up.
        //
        // Both CONFIRM first (unlike the raw codes, which are already an unambiguous act by the
        // time they are typed) because a button is easier to hit than nine specific digits.
        // BOOK_FORCE_BAKED_CODE and SOFT_RESET_CODE still work and are unchanged.
        case "request-force-baked":
          Alert.alert(
            "¿Usar el cancionero original?",
            "Se descartará el cancionero descargado y volverá el que viene con el app. Úsalo si el actual no abre bien.",
            [
              { text: "Cancelar", style: "cancel" },
              // destructive, not the soft reset: THIS is the one that throws away the downloaded
              // songbook and remounts the WebView. A soft reset only re-establishes the connection.
              { text: "Sí, usar el original", style: "destructive", onPress: () => onDirectorCode(BOOK_FORCE_BAKED_CODE) },
            ],
          );
          break;
        // Hand the crumb buffer to the web layer to render. Native only captures and serves it —
        // the viewer is web, so it can be improved over the air without another binary.
        case "request-diagnostics":
          // book + pages are here on purpose: with no internet, "which songbook is this device
          // holding?" had no answer once the title-page stamp was deleted. This answers it, from
          // the device's own resolved state rather than from ink on a page that can disagree.
          injectEvent({
            type: "diagnostics",
            build: BUILD_VERSION,
            role: roleRef.current,
            book: activeBookVersionRef.current || "(incluido en el app)",
            pages: totalPagesRef.current || 0,
            lines: breadcrumbsRef.current.slice(-BREADCRUMB_LIMIT),
            // Echo the LIVE debug settings so the dump's fields show what is actually set rather
            // than whatever was typed last time. A settings box that lies is worse than none.
            logSink: logSinkRef.current,
            telemetry: telemetryEnabledRef.current,
          });
          break;
        case "request-soft-reset":
          Alert.alert(
            "¿Reparar este dispositivo?",
            "Se reinicia la conexión y este dispositivo deja de dirigir o seguir. No borra el cancionero. Úsalo si la sincronización se quedó atascada.",
            [
              { text: "Cancelar", style: "cancel" },
              { text: "Sí, reparar", onPress: () => onDirectorCode(SOFT_RESET_CODE) },
            ],
          );
          break;
        case "resync": {
          // A follower tapped the ⟳ button in the web UI. The web relay is off in the shell,
          // so do the NATIVE resync: re-request the director's current snapshot over the mesh
          // and re-assert the last-known one immediately (mirrors the foreground resync).
          // ⟳ IS ALSO THE BOOK UPDATE (owner decision, 2026-08-05). Fired without awaiting, so the
          // mesh resync below stays instant — a ~27 MB download must never make the button feel
          // broken. Useful whether or not a background check also runs: this is the "do it NOW"
          // affordance, and it is the only one that exists.
          void refreshBookNowRef.current?.();
          // If somehow stranded in "off" (e.g. a prior soft-reset left sync off while the web
          // still shows the follower ⟳), re-join the mesh as a follower first so ⟳ recovers it.
          if (roleRef.current === "off" && syncAvailable) becomeFollower();
          if (roleRef.current !== "director") {
            if (syncAvailable) {
              // ⟳ must also kick a fast re-browse: if the director vanished or a NEW director
              // took over (handoff), requestCurrentSnapshot alone can't help until we re-find
              // it. refreshNearbyDiscovery accelerates re-discovery so ⟳ actually recovers.
              // ⟳ MUST BREAK A WEDGED SESSION, not just re-browse. refreshNearbyDiscovery never
              // clears connectedDirectorPeer, and scheduleNextDiscoveryRefresh skips re-browsing
              // entirely while that field is set — so against the failure this button exists for it
              // did nothing at all (observed 2026-08-17: spinner animated, iPad stayed on song 59).
              forceFollowerReconnectNow().catch(() => {});
              refreshNearbyDiscovery().catch(() => {});
              requestCurrentSnapshot().catch(() => {});
            }
            if (lastDirectorSnapshotRef.current) {
              const { page, book } = lastDirectorSnapshotRef.current;
              currentPageRef.current = page;
              // Keep the ref in sync, but DON'T inject a separate set-book: the single page
              // sync-event below carries `book` and switches + renders atomically (no race / no
              // momentary book-default flash from a two-script set-book→page interleave).
              currentBookRef.current = book;
              injectEvent({ type: "sync-event", event: { type: "page", page, book } });
            }
          }
          break;
        }
        case "exit-director": {
          // Director tapped the badge to step down → become a FOLLOWER. (There is no auto-restore
          // anymore, so there is no stored director code/time to forget.)
          if (syncAvailable) {
            // Mesh device → become a FOLLOWER (not "off") so it immediately re-joins the mesh and
            // follows the NEW director, and the ⟳ resync stays live. (Using performSoftReset here
            // left the device in "off" → stranded on the default page with a dead resync — the
            // director-handoff bug.)
            becomeFollower();
          } else {
            // Transmitter-only device (no mesh): there is NO follower transport in the shell (the
            // relay socket is off; native follows via the mesh, which this device lacks). Routing
            // it through becomeFollower would show follower UI with a dead ⟳ that can never sync.
            // Instead drop cleanly to standalone "off" — the reader still works locally, and the
            // web shows no phantom follower UI. Re-entering a director/transmitter code re-arms it.
            roleGenerationRef.current++; // supersede any in-flight become*
            stopDirectorHeartbeat(); // stop publishing to the relay
            roleRef.current = "off";
            explicitTransmitterRef.current = false;
            injectEvent({ type: "role", role: "none" });
          }
          break;
        }
        case "render-failed": {
          // A follower's renderPage threw (offline / un-cached page). The mesh 'page' path
          // optimistically set currentPageRef.current = page BEFORE the web confirmed the render,
          // so currentPageRef now equals the FAILED page.
          //
          // NOTE: the native de-dupe this was written to escape is GONE (see the 'page' case) —
          // every packet is now forwarded, so a failed page is re-driven by the next beat whatever
          // this ref says, and the web paces the retry itself (svShouldPaceRender). The sentinel is
          // kept as belt-and-braces because it costs nothing and it keeps this ref honest about
          // what is actually on screen, which is the property whose absence caused that wedge.
          // Reset the ref to an impossible-page sentinel (pages floor at 1) so the next heartbeat's
          // page !== currentPageRef.current and re-fires. A sentinel keeps the ref a `number` so the
          // many broadcastPage / `Number(...) || currentPageRef.current` consumers stay sound.
          // FOLLOWERS ONLY (roleRef === "follower"): the sentinel re-drive exists purely for a mesh
          // follower whose currentPageRef was optimistically set by the mesh 'page' path. ANY broadcaster
          // must never reset to -1 — that includes BOTH a mesh director (roleRef "director") AND a
          // transmitter-only director (roleRef "off" + explicitTransmitterRef, whose relay heartbeat is
          // gated on explicitTransmitterRef, not roleRef). Resetting on a broadcaster would publish
          // page -1 → clamped to 1 → yank the whole congregation to page 1. Gate positively on
          // "follower" so every broadcaster role is excluded.
          if (roleRef.current === "follower") {
            currentPageRef.current = -1;
          }
          break;
        }
        default:
          break;
      }
    },
    [
      flushPendingInjects,
      injectEvent,
      syncAvailable,
      broadcastPage,
      onDirectorCode,
      becomeFollower,
      stopDirectorHeartbeat,
    ],
  );

  // ── Resolve which songbook bundle to boot ───────────────────────────────────
  //
  // The decision table itself lives in src/bookResolve.js (pure, node-tested — see
  // e2e/bookResolve.test.mjs). Everything here is I/O around it.
  //
  // WHAT THIS REPLACES: the old resolver returned Documents/WebBundle on mere existence, with no
  // version compare and no health check, and returned the baked path without even stat'ing it.
  // Nothing in the app ever deleted that directory, so an iPad that once took a mesh bundle push
  // rendered that book FOREVER while the badge showed the current build number — invisible to the
  // fleet dashboard and ineligible for a corrective push. Verified live on a simulator: a planted
  // stale Documents/WebBundle made build 383 silently render the previous songbook.

  /**
   * Boot sweep. Runs once per launch AFTER first paint, so it can never delay the reader.
   *
   * Keeps the newest quarantined bundle (forensics) and removes older ones, plus the legacy
   * `WebBundle_new-*` staging directories the Swift mesh rail creates. Nothing has ever swept
   * those: a process kill between unpack and swap orphans one permanently, and each is up to
   * ~27 MB on a device whose free space is the reason a download can fail in the first place.
   */
  const sweepStaleBundles = useCallback(async () => {
    const docDir = FileSystem.documentDirectory || "";
    if (!docDir) return;
    try {
      const entries = await FileSystem.readDirectoryAsync(docDir);
      const bad = entries.filter((n) => n.startsWith("WebBundle.bad-")).sort();
      const doomed = [
        ...bad.slice(0, Math.max(0, bad.length - 1)), // keep only the newest
        ...entries.filter((n) => n.startsWith("WebBundle_new-")), // legacy Swift orphans
        ...entries.filter((n) => n === "WebBundle.prev.tmp"), // interrupted swap scratch
      ];
      for (const name of doomed) {
        await FileSystem.deleteAsync(`${docDir}${name}`, { idempotent: true }).catch(() => {});
      }
      if (doomed.length) breadcrumb(`sweep:${doomed.length}`);
    } catch {
      /* best-effort housekeeping — never surfaced, never blocking */
    }
  }, [breadcrumb]);

  const readJsonFile = useCallback(async (uri: string): Promise<unknown | null> => {
    try {
      const info = await FileSystem.getInfoAsync(uri);
      if (!info.exists) return null;
      return JSON.parse(await FileSystem.readAsStringAsync(uri));
    } catch {
      return null; // missing OR unparseable are the same answer: we cannot identify this bundle
    }
  }, []);

  const readStored = useCallback(async (key: string, fallback: unknown): Promise<any> => {
    try {
      const raw = await AsyncStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch {
      return fallback;
    }
  }, []);

  const resolveBundleUri = useCallback(
    async (force?: "bundled"): Promise<string> => {
      const bundleDir = FileSystem.bundleDirectory || "";
      const bakedUri = `${bundleDir}WebBundle/index.html`;
      const docDir = FileSystem.documentDirectory || "";
      const docUri = `${docDir}WebBundle/index.html`;

      // TOTAL AND TIME-BOUNDED BY CONSTRUCTION (red team H1). Everything below touches the
      // filesystem, and this runs on the boot path: a slow or wedged I/O call must never be able to
      // strand the app with no UI at all. Whatever happens, we return a URI within 1.5 s.
      const decide = async (): Promise<string> => {
        // Fast path (red team A7): reuse the last decision after ONE cheap existence check.
        // Eight identical aging iPads cold-booting together must not all parse two manifests and
        // cross a timeout together into a correlated silent downgrade.
        if (!force) {
          const cached = await readStored(STORAGE_KEYS.bookResolved, null);
          if (cached?.uri) {
            try {
              const info = await FileSystem.getInfoAsync(cached.uri);
              if (info.exists) {
                activeBundleSourceRef.current = cached.uri === bakedUri ? "bundled" : "documents";
                // The slow path sets this (below); the fast path did not, so any cold boot that hit
                // the cache left activeBookVersion null. Two consequences, both of which read as
                // "the OTA is broken": /fleet/checkin omits bookVersion entirely (it is written
                // conditionally), so the dashboard cannot see which book the device holds; and
                // shouldStage's `already-active` check compares the pointer against null, never
                // matches, and re-downloads the whole 27 MB bundle — then re-applies it, remounting
                // the WebView — on EVERY cold boot. The value is already persisted here.
                activeBookVersionRef.current = cached.bookVersion ?? null;
                return cached.uri;
              }
            } catch {
              /* fall through to the full table */
            }
          }
        }

        const [bakedInfo, docInfo] = await Promise.all([
          FileSystem.getInfoAsync(bakedUri).catch(() => ({ exists: false })),
          FileSystem.getInfoAsync(docUri).catch(() => ({ exists: false })),
        ]);

        const forceRec = await readStored(STORAGE_KEYS.bookForceBundled, null);
        const forceBundled =
          force === "bundled" ||
          // Auto-expires (red team H4): nothing else clears it, and a panic switch nobody can see
          // and nobody remembers setting is its own outage.
          (!!forceRec?.setAt && Date.now() - Number(forceRec.setAt) < FORCE_BUNDLED_TTL_MS);

        const boot = await readStored(STORAGE_KEYS.bookBoot, null);
        const quarantine = await readStored(STORAGE_KEYS.bookQuarantine, []);

        const [docManifest, bakedManifest] = await Promise.all([
          docInfo.exists ? readJsonFile(`${docDir}WebBundle/bundle-manifest.json`) : Promise.resolve(null),
          bakedInfo.exists ? readJsonFile(`${bundleDir}WebBundle/bundle-manifest.json`) : Promise.resolve(null),
        ]);

        const decision = decideBundle({
          docExists: !!docInfo.exists,
          docManifest: docManifest as any,
          bakedManifest: bakedManifest as any,
          bakedExists: !!bakedInfo.exists,
          forceBundled,
          bootAttempts: Number(boot?.attempts || 0),
          bootProved: boot?.provedAt != null,
          quarantine: Array.isArray(quarantine) ? quarantine : [],
        });

        breadcrumb(`resolve:${decision.source}:${decision.reason}`);

        if (decision.quarantineDoc && docInfo.exists) {
          // Rename, never delete (global rule §18) — it is the forensic evidence for WHY it failed.
          await quarantineDocumentsBundle((docManifest as any)?.bookVersion);
        }

        if (decision.source === "none") {
          // Both bundles gone. There is no runtime remedy; say so in a breadcrumb and hand back the
          // baked path so the WebView's own failure is at least attributable.
          breadcrumb("FATAL:no-bundle-anywhere");
          activeBundleSourceRef.current = "bundled";
          return bakedUri;
        }

        const uri = decision.source === "documents" ? docUri : bakedUri;
        activeBundleSourceRef.current = decision.source;
        const chosenManifest = (decision.source === "documents" ? docManifest : bakedManifest) as any;
        activeBookVersionRef.current = chosenManifest?.bookVersion ?? null;
        await AsyncStorage.setItem(
          STORAGE_KEYS.bookResolved,
          JSON.stringify({
            uri,
            bookVersion: chosenManifest?.bookVersion ?? null,
            builtFromShellBuild: chosenManifest?.builtFromShellBuild ?? null,
          }),
        ).catch(() => {});
        return uri;
      };

      let settled = false;
      return Promise.race([
        decide().then((u) => {
          settled = true;
          return u;
        }).catch(() => {
          settled = true;
          activeBundleSourceRef.current = "bundled";
          return bakedUri;
        }),
        new Promise<string>((resolve) =>
          setTimeout(() => {
            if (settled) return;
            // NON-STICKY (red team A7): this mounts the baked bundle for THIS launch only. It must
            // never quarantine anything and never write sv_book_active — a slow disk is not a bad
            // bundle, and treating it as one would downgrade the whole fleet after one bad morning.
            breadcrumb("preboot-timeout");
            activeBundleSourceRef.current = "bundled";
            resolve(bakedUri);
          }, RESOLVE_TIMEOUT_MS),
        ),
      ]);
    },
    [breadcrumb, readJsonFile, readStored, quarantineDocumentsBundle],
  );
  resolveBundleUriRef.current = resolveBundleUri;

  // ── Boot: restore book + role, resolve bundle, render WebView ───────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      breadcrumb("boot");
      // Single-book app: the only book is standard (Alvernia).
      const startBook: BookId = "standard";
      currentBookRef.current = startBook;

      // THE HARD-CRASH COUNTER (§5.10b). The in-session watchdog cannot see a bundle that kills the
      // process before React renders — which is exactly the failure an old iPad under memory
      // pressure produces, and the one that used to be unrecoverable without reinstalling the app.
      // Increment BEFORE mounting and flush immediately, so a jetsam between here and first paint
      // is still counted. resolveBundleUri reads this and bails to the code-signed bundle at two.
      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEYS.bookBoot);
        const boot = raw ? JSON.parse(raw) : null;
        if (boot && boot.provedAt == null && boot.mountedAt) {
          const attempts = Number(boot.attempts || 0) + 1;
          await AsyncStorage.setItem(STORAGE_KEYS.bookBoot, JSON.stringify({ ...boot, attempts }));
          breadcrumb(`boot-unproved:${attempts}`);
        }
      } catch {
        /* a bookkeeping failure must never block boot */
      }

      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEYS.lastCheckinOkAt);
        if (raw) lastCheckinOkAtRef.current = Number(raw) || null;
      } catch {
        /* ignore */
      }

      // Restore the LIBRO ANTERIOR banner across restarts — it is non-dismissible until an operator
      // clears it precisely so that a silent fleet split cannot go unnoticed.
      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEYS.bookReverted);
        const rec = raw ? JSON.parse(raw) : null;
        if (rec?.bookVersion && !cancelled) setRevertedBook(String(rec.bookVersion));
      } catch {
        /* ignore */
      }

      const uri = await resolveBundleUri();
      if (cancelled) return;

      // Record the mount attempt BEFORE the WebView exists, so the counter above can see it.
      await AsyncStorage.setItem(
        STORAGE_KEYS.bookBoot,
        JSON.stringify({
          bookVersion: activeBookVersionRef.current,
          mountedAt: Date.now(),
          provedAt: null,
          attempts: 0,
        }),
      ).catch(() => {});

      setInitialBook(startBook);
      setBundleUri(uri);
      setBooted(true);

      // Boot sweep, AFTER first paint so it never delays the reader: drop stray quarantined copies
      // beyond the newest, and the legacy WebBundle_new-* orphans the Swift mesh rail leaves behind
      // (nothing has ever swept those, and each one is up to ~27 MB).
      void sweepStaleBundles();
    })();
    return () => {
      cancelled = true;
    };
  }, [breadcrumb, resolveBundleUri, sweepStaleBundles]);

  // ── The songbook downloader (M5) ───────────────────────────────────────────
  //
  // SHIPS DORMANT. The server sends no `bookUpdate` field until BOOK_UPDATE_VERSION is set in the
  // worker, so on every device today this code observes nothing and does nothing. That is what
  // lets it land without a rehearsal.
  //
  // The decision logic is all in src/bookUpdate.js (pure, 45 tests). Everything here is I/O.

  /** expo-file-system adapters, shaped for the injected `fs` the pure module expects. */
  const bookFs = useMemo(() => {
    const root = FileSystem.documentDirectory || "";
    const abs = (p: string) => `${root}${p}`;
    return {
      stat: async (p: string) => {
        const i: any = await FileSystem.getInfoAsync(abs(p));
        return i?.exists ? { size: Number(i.size || 0) } : null;
      },
      exists: async (p: string) => !!(await FileSystem.getInfoAsync(abs(p))).exists,
      mkdirp: async (p: string) => {
        await FileSystem.makeDirectoryAsync(abs(p), { intermediates: true }).catch(() => {});
      },
      rmrf: async (p: string) => {
        await FileSystem.deleteAsync(abs(p), { idempotent: true }).catch(() => {});
      },
      move: async (from: string, to: string) => FileSystem.moveAsync({ from: abs(from), to: abs(to) }),
      readJson: async (p: string) => JSON.parse(await FileSystem.readAsStringAsync(abs(p))),
      writeJson: async (p: string, v: unknown) => FileSystem.writeAsStringAsync(abs(p), JSON.stringify(v)),
      walkWithHashes: async (dir: string) => {
        // md5 comes from getInfoAsync because expo-file-system exposes md5 and not sha256. It is a
        // CORRUPTION check only — HTTPS to our own origin is the authenticity boundary.
        const out = new Map<string, { size: number; md5: string }>();
        const walk = async (rel: string) => {
          const names = await FileSystem.readDirectoryAsync(abs(rel)).catch(() => [] as string[]);
          for (const name of names) {
            const childRel = `${rel}/${name}`;
            const info: any = await FileSystem.getInfoAsync(abs(childRel), { md5: true });
            if (!info?.exists) continue;
            if (info.isDirectory) await walk(childRel);
            else out.set(childRel.slice(dir.length + 1), { size: Number(info.size || 0), md5: String(info.md5 || "") });
          }
        };
        await walk(dir);
        return out;
      },
    };
  }, []);

  const bookNet = useMemo(
    () => ({
      fetchJson: async (url: string) => {
        const r = await fetch(url, { cache: "no-store" as RequestCache });
        if (!r.ok) throw new Error(`http ${r.status}`);
        return r.json();
      },
      download: async (url: string, dest: string) => {
        const root = FileSystem.documentDirectory || "";
        const res = await FileSystem.downloadAsync(url, `${root}${dest}`);
        if (!res || (res.status && res.status >= 400)) throw new Error(`http ${res?.status}`);
      },
    }),
    [],
  );

  const setBookStage = useCallback((stage: string) => {
    bookStageRef.current = stage;
  }, []);

  /**
   * Handle a /fleet/checkin response. THE POINTER IS DATA, NOT AN INSTRUCTION — parseBookUpdate
   * validates the shape and pins the host to constants baked into the app, so a compromised or
   * buggy worker can never aim this device at another origin.
   */
  const onCheckinResponse = useCallback(
    (body: unknown) => {
      const pointer = parseBookUpdate(body);

      void (async () => {
        const staged = await readStored(STORAGE_KEYS.bookStaged, null);

        // THE ABORT IS A REAL REVOKE (red team NI6). Disarming the server only stops NEW arming; the
        // devices already sitting on a verified copy are the problem, and the person who knows is
        // not in the building. So: any check-in that does not name our staged version deletes it.
        if (staged?.bookVersion && (!pointer || pointer.bookVersion !== staged.bookVersion)) {
          breadcrumb(`staged-revoked:${staged.bookVersion}`);
          await AsyncStorage.removeItem(STORAGE_KEYS.bookStaged).catch(() => {});
          await bookFs.rmrf("WebBundleStaged");
          setBookStage("");
        }
        if (!pointer) return;

        // OWNER DECISION, 2026-08-05 (fourth amendment): IT UPDATES WHEN YOU OPEN IT, AND ⟳ FORCES
        // IT. Two triggers, one sentence each. No third path, and nothing decides on its own
        // whether now is a good moment.
        //
        // What was deleted to make this safe to run automatically was the GATING, not the
        // automation: canApplyNow went from seven conditions to two (both physics — nothing
        // downloaded, no bridge to swap under), and shouldStage lost the role veto, the stagger and
        // the cooldowns. Those were what made a working rollout and a dead one look identical,
        // because every one of them refused silently.
        //
        // `pendingPointer` is recorded so ⟳ can act on an offer seen earlier even if the tap's own
        // check-in cannot reach the relay — the button must work in a bad-signal parking lot.
        pendingPointerRef.current = pointer;

        if (stagingInFlightRef.current) return;
        const quarantine = await readStored(STORAGE_KEYS.bookQuarantine, []);
        const active = await readStored(STORAGE_KEYS.bookActive, null);
        const decision = shouldStage({
          killSwitch: SV_BOOK_DL_KILL,
          bookVersion: pointer.bookVersion,
          activeBookVersion: activeBookVersionRef.current,
          stagedBookVersion: staged?.bookVersion ?? null,
          stagedReady: !!staged?.ready,
          // Without this, `already-staged` outlives the apply gate's staleness TTL and the device
          // deadlocks: nothing applies it and nothing re-verifies it.
          stagedReadyAt: staged?.readyAt ?? null,
          quarantine: Array.isArray(quarantine) ? quarantine : [],
          webReady: webReadyRef.current,
          foreground: AppState.currentState === "active",
          role: roleRef.current,
          // firstSeenAt fed the client stagger, which was removed on 2026-08-03 — shouldStage no
          // longer reads it, and the AsyncStorage write that produced it went with this amendment.
          deviceId: dbgDeviceRef.current,
          now: Date.now(),
          minShellBuild: 1,
          shellBuild: Number(BUILD_VERSION) || 0,
        });
        if (!decision.stage) {
          if (decision.reason !== "already-active" && decision.reason !== "already-staged") {
            breadcrumb(`stage-skip:${decision.reason}`);
          }
          return;
        }

        stagingInFlightRef.current = true;
        setBookStage("downloading:0%");
        try {
          const rec = await stageBook({
            base: pointer.base,
            bookVersion: pointer.bookVersion,
            fs: bookFs,
            net: bookNet,
            now: () => Date.now(),
            activeTotalPages: Number(active?.totalPages || 0),
            shellBuild: Number(BUILD_VERSION) || 0,
            // Mesh peers connected means practice is happening: throttle to 1, never veto. The
            // practice room is the ONLY place these iPads have internet.
            concurrency: meshPeerCountRef.current > 0 ? 1 : 3,
            onProgress: (done, total) => setBookStage(`downloading:${Math.floor((done / total) * 100)}%`),
          });
          await AsyncStorage.setItem(STORAGE_KEYS.bookStaged, JSON.stringify(rec)).catch(() => {});
          setBookStage(rec.ready ? "ready" : `error:${rec.error}`);
          breadcrumb(rec.ready ? `staged-ready:${rec.bookVersion}` : `stage-failed:${rec.error}`);
          // Install it. Don't wait to be asked — canApplyNow decides WHEN, and if right now is a
          // Mass or a rehearsal it defers and the next foreground/check-in retries.
          if (rec.ready) await autoApplyIfSafeRef.current?.();
        } catch {
          setBookStage("error:unexpected");
        } finally {
          stagingInFlightRef.current = false;
        }
      })();
    },
    [breadcrumb, readStored, bookFs, bookNet, setBookStage],
  );
  onCheckinResponseRef.current = onCheckinResponse;

  /**
   * The swap itself. No dialogs, no prompts — the caller has already decided.
   */
  const performApplySwap = useCallback(
    async (staged: any) => {
      // Save the reader's place so nobody loses it across the swap.
      await AsyncStorage.setItem(
        `${STORAGE_KEYS.lastPagePrefix}${currentBookRef.current}`,
        String(currentPageRef.current),
      ).catch(() => {});
      const res = await applyStagedBundle({ fs: bookFs });
      breadcrumb(`apply:${res.stage}`);
      if (!res.ok) return false;
      await AsyncStorage.setItem(
        STORAGE_KEYS.bookActive,
        JSON.stringify({
          bookVersion: staged.bookVersion,
          totalPages: staged.totalPages,
          installedAt: Date.now(),
          source: "http",
        }),
      ).catch(() => {});
      await AsyncStorage.removeItem(STORAGE_KEYS.bookStaged).catch(() => {});
      await AsyncStorage.removeItem(STORAGE_KEYS.bookResolved).catch(() => {});
      setBookStage("active");
      const next = await resolveBundleUriRef.current?.();
      webReadyRef.current = false;
      pendingInjectRef.current = [];
      if (next) setBundleUri(next);
      setMountKey((k) => k + 1);
      return true;
    },
    [breadcrumb, bookFs, setBookStage],
  );

  /**
   * AUTOMATIC APPLY. A new songbook installs itself — that is the entire promise of the OTA, and
   * making a human type a secret code on eight personally-owned iPads is not "over the air", it is
   * manual work with extra steps.
   *
   * canApplyNow still governs WHEN, and every one of its gates is about timing, never about
   * consent: it refuses during a Mass or rehearsal (mesh peers connected, a director snapshot or
   * page turn in the last moments), on a cold boot, and on a `ready` flag stale enough to have come
   * from Saturday practice. So the swap lands in a quiet moment on its own, and if the moment is
   * never quiet it simply waits and tries again on the next check-in.
   *
   * Silent by design: no modal. A prompt firing on seven devices at 12:04 is the fleet-bricking
   * scenario this whole design avoids — the answer is to not ask, not to make someone type a code.
   */
  const autoApplyIfSafe = useCallback(async () => {
    const staged = await readStored(STORAGE_KEYS.bookStaged, null);
    if (!staged?.ready) return;
    const gate = canApplyNow({
      stagedReady: true,
      stagedReadyAt: staged?.readyAt ?? null,
      lastCheckinOkAt: lastCheckinOkAtRef.current,
      meshPeerConnected: meshPeerCountRef.current > 0,
      lastPageTurnAt: lastPageTurnAtRef.current,
      lastDirectorSnapshotAt: lastDirectorSnapshotRef.current?.at ?? null,
      role: roleRef.current,
      lastKnownRole: lastKnownRoleRef.current,
      coldBootAt: coldBootAtRef.current,
      webReady: webReadyRef.current,
      minShellBuild: Number(staged?.minShellBuild || 1),
      shellBuild: Number(BUILD_VERSION) || 0,
      now: Date.now(),
    });
    if (!gate.ok) {
      // Not a failure — just not yet. The next check-in tries again.
      breadcrumb(`auto-apply-waiting:${gate.reason}`);
      return;
    }
    breadcrumb("auto-apply:go");
    await performApplySwap(staged);
  }, [readStored, breadcrumb, performApplySwap]);
  autoApplyIfSafeRef.current = autoApplyIfSafe;

  /**
   * ⟳ — THE ONLY THING THAT UPDATES THE SONGBOOK (owner decision, 2026-08-05).
   *
   * One sentence: tap it, and the device reconnects and installs the latest book if there is one.
   *
   * It raises `manualRefresh` for the duration, so the check-in it fires is allowed to download —
   * a routine check-in is not. Staging then applies on completion through the existing path. The
   * flag is cleared in `finally`, so a thrown fetch can never leave the device silently
   * auto-updating afterwards.
   */
  const refreshBookNow = useCallback(async () => {
    if (manualRefreshRef.current) return; // already servicing a tap
    manualRefreshRef.current = true;
    breadcrumb("manual-refresh:start");
    try {
      // A pointer seen on an earlier routine check-in was recorded but deliberately not acted on.
      // Honour it now, so ⟳ works even if this tap's check-in cannot reach the relay.
      if (!stagingInFlightRef.current && pendingPointerRef.current) {
        await onCheckinResponseRef.current?.({ bookUpdate: pendingPointerRef.current });
      }
    } catch {
      breadcrumb("manual-refresh:network");
    } finally {
      manualRefreshRef.current = false;
      breadcrumb("manual-refresh:end");
    }
  }, [breadcrumb]);
  refreshBookNowRef.current = refreshBookNow;

  /**
   * MANUAL FORCE (numpad code). No longer required — the book installs itself — but kept as the
   * operator's override for the case where the timing gates keep deferring and someone wants it
   * NOW, between Masses. This one talks back, because a human deliberately asked.
   */
  const applyStagedBook = useCallback(async () => {
    const staged = await readStored(STORAGE_KEYS.bookStaged, null);
    const gate = canApplyNow({
      stagedReady: !!staged?.ready,
      stagedReadyAt: staged?.readyAt ?? null,
      lastCheckinOkAt: lastCheckinOkAtRef.current,
      meshPeerConnected: meshPeerCountRef.current > 0,
      lastPageTurnAt: lastPageTurnAtRef.current,
      lastDirectorSnapshotAt: lastDirectorSnapshotRef.current?.at ?? null,
      role: roleRef.current,
      lastKnownRole: lastKnownRoleRef.current,
      coldBootAt: coldBootAtRef.current,
      webReady: webReadyRef.current,
      minShellBuild: Number(staged?.minShellBuild || 1),
      shellBuild: Number(BUILD_VERSION) || 0,
      now: Date.now(),
    });
    breadcrumb(`apply-gate:${gate.reason}`);
    if (!gate.ok) {
      Alert.alert(
        gate.reason === "not-ready" ? "No hay libro nuevo" : "Ahora no",
        gate.reason === "not-ready"
          ? "Este iPad no tiene un cancionero nuevo descargado."
          : "Hay una Misa o ensayo en curso. Actualiza después.",
      );
      return;
    }

    Alert.alert("¿Actualizar el cancionero?", "El app se recargará con el libro nuevo.", [
      { text: "Cancelar", style: "cancel" },
      {
        text: "Actualizar",
        onPress: () => {
          void (async () => {
            // ONE implementation of the swap, shared with the automatic path. Two copies of a
            // sequence that rewrites the active bundle is how they drift.
            const ok = await performApplySwap(staged);
            if (!ok) Alert.alert("No se pudo actualizar", "El cancionero anterior sigue intacto.");
          })();
        },
      },
    ]);
  }, [readStored, breadcrumb, performApplySwap]);
  applyStagedBookRef.current = applyStagedBook;

  // ── Pre-boot watchdog (§5.10b) ─────────────────────────────────────────────
  //
  // While `!booted || !bundleUri` the app renders a plain black View, and the bridge watchdog
  // returns early in exactly that state — so a boot that never settles is a permanent black
  // rectangle with NO native floor at all and no way out but force-quitting. resolveBundleUri is
  // already time-bounded, but the boot effect also awaits AsyncStorage, and "should not hang" is
  // not a fallback strategy on hardware we never see.
  //
  // NON-STICKY BY DESIGN (red team A7). This mounts the code-signed bundle for THIS launch only.
  // It must never quarantine anything and never mark a book bad — eight identical aging iPads
  // cold-booting together on a slow morning must not be able to talk themselves into a
  // fleet-wide downgrade. The breadcrumb is deliberately distinct from `bridge-timeout` so a
  // slow-disk episode is never mistaken for a broken bundle when reading forensics later.
  useEffect(() => {
    if (booted) return;
    const t = setTimeout(() => {
      if (booted) return;
      breadcrumb("preboot-watchdog");
      const bundleDir = FileSystem.bundleDirectory || "";
      activeBundleSourceRef.current = "bundled";
      setBundleUri(`${bundleDir}WebBundle/index.html`);
      setBooted(true);
    }, PREBOOT_TIMEOUT_MS);
    return () => clearTimeout(t);
  }, [booted, breadcrumb]);

  // A device with NO mesh can still have been directing — becomeDirector's no-mesh branch makes it
  // an explicit relay transmitter and persists lastSyncRole="director", justifying that write with
  // "makes the boot resume prompt fire so the operator resumes". That prompt could never fire: the
  // whole bootstrap below sits behind `if (!syncAvailable) return`, so the persisted role was a
  // write nothing ever read. After a crash the device came back silent, with no relay heartbeat, and
  // every signovivo.com follower stayed frozen for the rest of Mass with no 401 to hint at it.
  //
  // It only NOTIFIES. Auto-resume is for the mesh path, where the live-director snapshot can prove
  // nobody else took over; there is no equivalent evidence here, and two relay transmitters publish
  // conflicting pages to every web follower.
  useEffect(() => {
    if (syncAvailable || didTransmitterNoticeRef.current) return;
    didTransmitterNoticeRef.current = true;
    AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole)
      .then((prev) => {
        if (prev !== "director") return;
        lastKnownRoleRef.current = "director";
        injectEvent({
          type: "toast",
          text: "Estabas transmitiendo cuando se cerró el app.",
          action: "resume-director",
        });
      })
      .catch(() => {});
  }, [syncAvailable, injectEvent]);

  // ── Multipeer permissions + role bootstrap + event listener ─────────────────
  useEffect(() => {
    if (!syncAvailable) return;
    primeNearbyPermissions().catch(() => {});

    // Role bootstrap: ONCE per session only. If this effect re-runs (its become*/injectEvent
    // useCallback deps changed identity mid-session), re-firing this would clobber an intentional
    // in-session role flip. The listener below still re-registers on every run; only this is one-shot.
    if (!didBootstrapRef.current) {
      didBootstrapRef.current = true;
      // A DEVICE ALWAYS BOOTS AS A FOLLOWER. Nothing here promotes it — see the ONE DIRECTOR note by
      // DIRECTOR_STAMP_THROTTLE_MS. Not a stale role from last Sunday, not a crash a minute ago, not
      // a tally of how often it has directed. Every one of those was tried between 2026-08-05 and
      // build 427 and each could mint a second director beside a human's, and win. The only thing
      // this block does with the persisted role is TELL the person: if this device was directing
      // when the app died, they get one toast pointing at the pill, and the seat stays empty until a
      // hand takes it. An empty seat for the seconds it takes to tap is the choir sitting on the
      // last page — which it already is. Two directors is the choir split.
      AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole)
        .then((prev) => {
          lastKnownRoleRef.current = prev ? String(prev) : null;
          if (prev === "director") {
            // Written back as follower so the toast fires once per crash, not on every boot forever.
            AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower").catch(() => {});
            // NO INSTRUCTIONS — THE NOTICE CARRIES THE BUTTON (owner, 2026-08-18: "really shitty
            // UX"). It used to say "toca el estado arriba a la izquierda", which had been WRONG
            // since build 435: the top-left status stopped taking the role then, and after
            // 2026-08-18 that corner is Salir/Director or ⟳ while the way in moved to the RIGHT.
            // So it sent whoever read it to the wrong control, for weeks, at the one moment they
            // were already flustered. A notice that describes a control is a promise about the UI
            // that rots as soon as the UI moves; carrying the control cannot rot.
            injectEvent({
              type: "toast",
              text: "Estabas dirigiendo cuando se cerró el app.",
              action: "resume-director",
            });
          }
        })
        .catch(() => {})
        .finally(() => {
          becomeFollower();
        });
    }

    const sub = addNearbyDirectorSyncListener((event: Record<string, unknown>) => {
      const type = String(event?.type ?? "");
      switch (type) {
        case "page": {
          if (roleRef.current === "director") break; // ignore our own echoes
          const book: BookId = "standard"; // single-book app; incoming bookId/mode is always standard
          const page = Number(event.page) || currentPageRef.current;
          dbgLog("mesh:page-recv", {
            page,
            book,
            dup: page === currentPageRef.current && book === currentBookRef.current,
          });
          // Remember the director's latest snapshot (with a timestamp — NEW-DIR-3) so a reloaded/
          // foregrounded follower resyncs, and so "is a director live RIGHT NOW?" can be judged by recency.
          lastDirectorSnapshotRef.current = { page, book, at: Date.now() };
          lastPageTurnAtRef.current = Date.now();
          // DUMB FOLLOWER (owner's rule, 2026-08-17): "STATELESS. DUMB FOLLOWERS, SIMPLE DIRECTOR."
          //
          // There used to be a de-dupe here — if `page === currentPageRef.current` we dropped the
          // packet without rendering, on the theory that a same-page re-send is just a keepalive.
          // That made the FOLLOWER decide whether to obey the director, using a REMEMBERED number.
          // A remembered number can drift from what is actually on the glass; when it does, the
          // follower ignores every re-assertion of the page it is already "on" and is wedged until
          // the director happens to turn to a DIFFERENT page.
          //
          // That is the bug reported 2026-08-17: director on song 16 backgrounds, followers are
          // walked to 14 and 19 by hand, director foregrounds still on 16 — and nobody comes back.
          // Turning to 15 fixed all of them instantly, which proves the mesh was healthy the whole
          // time and only the same-page re-assertion was being dropped. The iPhone recovered on its
          // own precisely because it DISCONNECTED and reconnected, and the connect path sends a
          // snapshot down a route this guard did not sit on. The bug rewarded a broken link.
          //
          // The check still exists — it just moved to the only layer that can see the truth. The
          // web's renderPage returns early when the page is unchanged AND the <img> is genuinely
          // showing it (app.js, "ALREADY ON THIS PAGE"). That guard is authoritative because it
          // inspects the rendered image rather than a number we hope still matches it, and it is
          // what actually prevents the 1 Hz re-render storm behind the 2026-08-06 crash — it
          // returns before the load request, the loading indicator, the src assignment and the
          // AsyncStorage write. Forwarding costs one bridge message per second per follower and
          // buys back the property that matters: the director's word always reaches the screen.
          currentPageRef.current = page;
          // Keep the ref in sync, but DON'T inject a separate set-book: the single page
          // sync-event below carries `book`, and the web handler switches the book + renders the
          // page atomically. The old set-book→page two-script sequence raced at the first
          // `await switchBook` — followers could land on the book DEFAULT page or wedge on a
          // rolled-back load. One event eliminates the race.
          if (book !== currentBookRef.current) currentBookRef.current = book;
          injectEvent({ type: "sync-event", event: { type: "page", page, book } });
          break;
        }
        case "state": {
          meshPeerCountRef.current = Number(event.peerCount) || 0;
          dbgLog("mesh:state", {
            status: event.status,
            srole: event.role,
            peers: event.peerCount,
            msg: event.message,
          });
          injectEvent({
            type: "sync-event",
            event: {
              type: "state",
              status: event.status,
              role: event.role,
              message: event.message,
            },
          });
          break;
        }
        case "error": {
          dbgLog("mesh:error", { code: event.code });
          if (String(event.code ?? "") === "DIRECTOR_CONFLICT") {
            stopDirectorHeartbeat(); // a newer director won; stop re-broadcasting
            becomeFollower(); // step down
            // C2: a demoted director never recorded a director snapshot (it WAS the director), so
            // its follower-resync fallbacks would no-op and it would keep showing its OWN stale page
            // until the winner next turns. Actively pull the winner's current page + re-scan so it
            // re-homes immediately. Best-effort.
            requestCurrentSnapshot().catch(() => {});
            refreshNearbyDiscovery().catch(() => {});
            // SAY SO. Until now the person who lost the seat got no signal at all — the pill just
            // read SIGUIENDO — so they kept turning pages that no longer went anywhere and, once
            // they noticed, took the role back, and the choir flipped a second time. Under ONE
            // DIRECTOR the only way this fires is another human deciding to direct; the loser must
            // know that, and that the choir now follows the other device.
            injectEvent({
              type: "toast",
              text: "Otro dispositivo tomó la dirección del coro. Este dispositivo ahora sigue.",
            });
          }
          break;
        }
        case "takeover-request": {
          // v1: a director auto-denies takeover requests. Admin-initiated force-takeover
          // rides the Swift conflict path (later token wins) when the admin starts director.
          if (roleRef.current === "director" && event.requestId != null) {
            denyDirectorTakeover(String(event.requestId)).catch(() => {});
          }
          break;
        }
        case "bundleUpdated": {
          // DELIBERATELY INERT. This used to re-resolve and REMOUNT THE WEBVIEW ON THE SPOT, with
          // no human gate and no timing check — the held M-F3 nightmare, live until now. A peer
          // arriving mid-Mass could swap the songbook out from under a singer mid-verse, on a
          // device in a building where nothing can be rolled back.
          //
          // The mesh bundle-push rail is retired at its receive boundary (DirectorSyncModule.swift,
          // four guards), so this event can no longer fire from a current build. It is kept as a
          // no-op rather than deleted because a peer on an OLDER build can still emit it, and a
          // silent no-op is the correct response to a rail we no longer honour.
          breadcrumb("mesh-bundleUpdated-ignored");
          break;
        }
        default:
          break;
      }
    });

    return () => {
      try {
        sub.remove();
      } catch {
        /* ignore */
      }
      stopDirectorHeartbeat(); // no leaked interval on unmount / effect re-run
    };
  }, [
    syncAvailable,
    becomeDirector,
    becomeFollower,
    injectEvent,
    resolveBundleUri,
    stopDirectorHeartbeat,
  ]);

  // ── Foreground: nudge mesh rediscovery + pull a fresh snapshot ──────────────
  // Always registered: a transmitter-only device (no mesh, syncAvailable false) becomes a
  // transmitter AFTER mount via a numpad code, so we can't gate the listener on the mount-time
  // syncAvailable value — it would never fire for that device. The listener body is fully
  // role-gated (every branch checks roleRef / explicitTransmitterRef) and mesh-only calls are
  // gated on syncAvailable, so this is harmless for a plain offline follower.
  useEffect(() => {
    const sub = AppState.addEventListener("change", (next) => {
      if (next !== "active") return;
      // IT UPDATES WHEN YOU OPEN IT (owner decision, 2026-08-05). The 90 s check-in timer only
      // ticks while the app is awake, so an iPad asleep when a book was published would otherwise
      // sit on the old one until someone happened to leave it open. Checking here means "open the
      // app" is the whole procedure, and ⟳ is the "do it NOW" force on top of it.
      //
      // This is safe to run unattended only because the GATING is gone, not because the automation
      // was ever the problem: canApplyNow is down to two physical impossibilities, so a device that
      // does nothing now really is a device that had nothing to do.
      void autoApplyIfSafeRef.current?.();
      // ONE refresh, not two. This was duplicated, and each call scheduled another discovery timer
      // without invalidating the previous one (DirectorSyncModule.scheduleNextDiscoveryRefresh) —
      // so every foreground DOUBLED the live timer population. Measured on the owner's iPhone:
      // 66 advertiser start/stop events per second, which is why the device that gets picked up and
      // put down all day was the one that could never complete a handshake. The Swift side now
      // invalidates properly and floors the churn at 2 s, but the duplicate had no reason to exist.
      if (syncAvailable) refreshNearbyDiscovery().catch(() => {});
      if (roleRef.current === "follower") {
        requestCurrentSnapshot().catch(() => {});
        // Re-assert the director's last-known snapshot immediately so the view is correct on
        // foreground while the fresh snapshot request round-trips over the mesh.
        if (lastDirectorSnapshotRef.current) {
          const { page, book } = lastDirectorSnapshotRef.current;
          currentPageRef.current = page;
          // Keep the ref in sync, but DON'T inject a separate set-book: the single page
          // sync-event below carries `book` and switches + renders atomically (no two-script
          // set-book→page race that could flash the book default on foreground).
          if (book !== currentBookRef.current) currentBookRef.current = book;
          injectEvent({ type: "sync-event", event: { type: "page", page, book } });
        }
      } else if (roleRef.current === "director") {
        // SIMPLE DIRECTOR: on every foreground, re-arm the beat and re-assert the page. The
        // broadcast below was already here; the restart is new.
        //
        // The 1 Hz mesh heartbeat is a JS setInterval, and a JS interval is not a guarantee — it
        // does not run while iOS has the process suspended, and nothing in this handler used to
        // put it back. Every other recovery path in the app re-arms itself on foreground (the
        // advertiser at DirectorSyncModule.swift handleAppDidBecomeActive, the follower watchdog,
        // discovery refresh); the one timer the whole choir depends on did not. startDirectorHeartbeat
        // calls stopDirectorHeartbeat first, so calling it again is idempotent and cannot leak an
        // interval.
        //
        // Belt and braces on purpose: the broadcast makes recovery immediate rather than up to a
        // second later, and the restarted beat is what keeps re-asserting if that one packet is
        // dropped. Neither alone is sufficient — a single packet can be lost, and a beat that
        // starts a second late still leaves a visible gap.
        startDirectorHeartbeat();
        broadcastPage(currentPageRef.current, currentBookRef.current);
      } else if (explicitTransmitterRef.current) {
        // Transmitter-only (no mesh): re-publish on foreground so the relay snapshot doesn't
        // stay stale after the device was backgrounded past the freshness window.
        broadcastPage(currentPageRef.current, currentBookRef.current);
      }
    });
    return () => sub.remove();
  }, [syncAvailable, broadcastPage, injectEvent, startDirectorHeartbeat]);

  // ── Global JS error trap (breadcrumb only; the web app owns its own UI) ──────
  useEffect(() => {
    const g = globalThis as unknown as {
      ErrorUtils?: { getGlobalHandler?: () => unknown; setGlobalHandler?: (h: unknown) => void };
    };
    const prev = g.ErrorUtils?.getGlobalHandler?.();
    g.ErrorUtils?.setGlobalHandler?.((error: unknown, isFatal?: boolean) => {
      breadcrumb(`jserror:${isFatal ? "fatal" : "soft"}`);
      if (typeof prev === "function") (prev as (e: unknown, f?: boolean) => void)(error, isFatal);
    });
  }, [breadcrumb]);

  const preloadScript = useMemo(
    () =>
      [
        "window.__SIGNO_VINO_NATIVE_FILE_MODE = true;",
        `window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION = ${JSON.stringify(BUILD_VERSION)};`,
        `window.__SIGNO_VIVO_DEVICE_KIND = ${JSON.stringify(DEVICE_KIND)};`,
        `window.__SIGNO_VINO_INITIAL_BOOK = ${JSON.stringify(initialBook)};`,
        "true;",
      ].join("\n"),
    [initialBook],
  );

  if (!booted || !bundleUri) {
    return <View style={styles.blank} />;
  }

  return (
    <View style={styles.root}>
      <StatusBar hidden />
      {/*
        LIBRO ANTERIOR (red team A4). The self-heal ladder is silent and correct, and that is the
        danger: a defect that only reproduces on the choir's hardware fires all eight watchdogs at
        once, every device quietly drops to the previous songbook, and the fleet is now split across
        two books with nobody aware — during Mass, with no internet and no way to find out. This
        banner is deliberately non-dismissible and survives restarts.
      */}
      {revertedBook ? (
        <TouchableOpacity
          style={styles.revertBanner}
          accessibilityRole="button"
          // Non-dismissible by ACCIDENT, clearable by an OPERATOR. The plan parks the clear action
          // inside DIAGNÓSTICO, which is not in this build — so without this the banner would stick
          // on the device forever with no way off it, which is its own small outage. A deliberate
          // tap plus a confirm is the same "an operator decided" bar, reachable today.
          onPress={() => {
            Alert.alert(
              "Cancionero anterior",
              "Este iPad volvió a una versión anterior del cancionero. Avísale al director antes de quitar el aviso.",
              [
                { text: "Dejar el aviso", style: "cancel" },
                {
                  text: "Ya avisé",
                  onPress: () => {
                    void AsyncStorage.removeItem(STORAGE_KEYS.bookReverted).catch(() => {});
                    setRevertedBook(null);
                    breadcrumb("reverted-banner-cleared");
                  },
                },
              ],
            );
          }}
        >
          <Text style={styles.revertBannerText}>LIBRO ANTERIOR · avísale al director</Text>
        </TouchableOpacity>
      ) : null}
      <WebView
        key={`webbundle-${mountKey}`}
        ref={webViewRef}
        source={{ uri: bundleUri }}
        // GRANT READ ACCESS TO THE BUNDLE DIRECTORY, NOT JUST index.html.
        //
        // Without this prop react-native-webview falls back to `request.URL` as the read-access
        // scope (RNCWebViewImpl.m: `readAccessUrl = allowingReadAccessToURL ? … : request.URL`),
        // and Apple's contract for loadFileURL:allowingReadAccessToURL: is that a FILE URL exposes
        // ONLY that file — a DIRECTORY URL is what exposes its contents. So the WebView could read
        // index.html and was denied styles.css, app.js and lib/*.js.
        //
        // The failure looks nothing like a permissions error: the page renders as raw unstyled
        // HTML with every hidden loader state visible at once, app.js never runs, `bridge-ready`
        // never arrives, and the watchdog concludes the bundle is broken — quarantining a bundle
        // whose 390 files are all byte-perfect on disk (verifyStaged proved that before the swap).
        // Observed on an iPad, 2026-08-05, after an OTA applied successfully.
        //
        // `allowFileAccessFromFileURLs` / `allowUniversalAccessFromFileURLs` do NOT cover this:
        // they relax same-origin for scripted reads (XHR/fetch), while <link> and <script src>
        // subresources are gated by the sandbox extension issued from the read-access URL.
        allowingReadAccessToURL={readAccessDirFor(bundleUri)}
        originWhitelist={["*"]}
        allowFileAccess
        allowFileAccessFromFileURLs
        allowUniversalAccessFromFileURLs
        injectedJavaScriptBeforeContentLoaded={preloadScript}
        onMessage={handleMessage}
        onError={() => breadcrumb("webview-error")}
        onContentProcessDidTerminate={() => {
          breadcrumb("webview-terminated");
          webReadyRef.current = false;
          pendingInjectRef.current = []; // drop stale queued injects so they don't flush into the fresh page
          // RIDER: a hard content-process crash must ESCALATE. This used to reload the same URI
          // without touching the counter, so a crash-loop on a bad bundle recycled forever and
          // never reached the ladder — the loop had no exit at all.
          remountAttemptsRef.current += 1;
          void (async () => {
            try {
              const next = await resolveBundleUriRef.current?.();
              if (next) setBundleUri(next);
            } catch {
              /* keep the current URI */
            }
            setMountKey((k) => k + 1); // full remount; the mount effect re-arms the watchdog
          })();
        }}
        allowsInlineMediaPlayback
        mediaPlaybackRequiresUserAction={false}
        cacheEnabled={true}
        allowsLinkPreview={false}
        dataDetectorTypes="none"
        setSupportMultipleWindows={false}
        bounces={false}
        overScrollMode="never"
        scrollEnabled
        textInteractionEnabled={false}
        style={styles.web}
        {...(Platform.OS === "ios" ? { allowsBackForwardNavigationGestures: false } : {})}
      />
      {/*
        The native recovery floor, now an OVERLAY rather than a replacement for the WebView.
        It used to be an early `return`, which made Reintentar unsafe: it cleared webDead and THEN
        remounted, so once resolution became async a hung filesystem call would remove the only UI
        on screen and leave a black rectangle with no way back (red team H1). As an overlay, the
        WebView is always mounted underneath and the floor stays visible until the fresh mount
        actually posts bridge-ready — which is the only real evidence that recovery worked.
      */}
      {webDead ? (
        <View style={styles.fallback}>
          <Text style={styles.fallbackTitle}>Signo Vivo se está recuperando</Text>
          <Text style={styles.fallbackMsg}>El app no cargó bien. Toca para reintentar.</Text>
          <TouchableOpacity
            style={styles.fallbackBtn}
            accessibilityRole="button"
            onPress={() => {
              breadcrumb("native-fallback-retry");
              remountAttemptsRef.current = 0;
              webReadyRef.current = false;
              pendingInjectRef.current = [];
              // Deliberately NOT setWebDead(false) here — bridge-ready clears it. Until the new
              // mount proves itself, the user keeps a working button instead of a black screen.
              void (async () => {
                try {
                  const next = await resolveBundleUriRef.current?.();
                  if (next) setBundleUri(next);
                } catch {
                  /* keep the current URI rather than leaving the app with none */
                }
                setMountKey((k) => k + 1);
              })();
            }}
          >
            <Text style={styles.fallbackBtnText}>Reintentar</Text>
          </TouchableOpacity>
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#000" },
  blank: { flex: 1, backgroundColor: "#000" },
  web: { flex: 1, backgroundColor: "#000" },
  // Slice B native recovery floor — an absolute overlay so the WebView underneath stays mounted
  // and there is never a frame with no UI at all.
  fallback: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: "#0d0d1a",
    alignItems: "center",
    justifyContent: "center",
    padding: 24,
  },
  // LIBRO ANTERIOR: deliberately loud and non-dismissible. pointerEvents="none" so it can never
  // swallow a page turn mid-Mass — being informative must not cost the choir a tap.
  revertBanner: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    zIndex: 10,
    backgroundColor: "#8a2f00",
    paddingVertical: 6,
    paddingHorizontal: 12,
  },
  revertBannerText: { color: "#fff", fontSize: 13, fontWeight: "700", textAlign: "center" },
  fallbackTitle: { color: "#fff", fontSize: 20, fontWeight: "700", textAlign: "center", marginBottom: 8 },
  fallbackMsg: { color: "#c8c8dc", fontSize: 16, textAlign: "center", marginBottom: 20 },
  fallbackBtn: { backgroundColor: "#3b6df6", paddingVertical: 13, paddingHorizontal: 28, borderRadius: 12 },
  fallbackBtnText: { color: "#fff", fontSize: 17, fontWeight: "600" },
});
