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
  requestCurrentSnapshot,
  resetNearbyDirectorSync,
  sendNearbyDirectorPageUpdate,
  startNearbyDirector,
  startNearbyFollower,
} from "./src/nearbyDirectorSync";
import { publishPageToRelay, setRelayPublishCode, setRelayAuthErrorHandler } from "./src/directorRelaySync";
import directorCodes from "./director-codes.json";
import { STORAGE_KEYS, type BookId } from "./src/offlineBooks";
import {
  decideBundle,
  recordBundleFailure,
  clearBundleFailures,
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

// Director entry. A real director number, baked at build from the gitignored
// director-codes.private.json — the committed director-codes.json is EMPTY, so no phone numbers
// live in this public repo. Any code in this set may take the director role (after confirming).
const STANDARD_DIRECTOR_CODES = new Set<string>(
  ((directorCodes as { standardDirectorCodes?: string[] }).standardDirectorCodes || []).map((c) =>
    String(c).replace(/[^0-9]/g, ""),
  ),
);
// Super-admin codes (Miguel) — a SUBSET of the standard codes, baked from the gitignored
// director-codes.private.json. Same power as any director code; the confirm dialog just labels it
// "super admin" so it's clear this number is taking the director role (never promoted silently).
const SUPER_ADMIN_CODES = new Set<string>(
  ((directorCodes as { superAdminCodes?: string[] }).superAdminCodes || []).map((c) =>
    String(c).replace(/[^0-9]/g, ""),
  ),
);
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
  const breadcrumb = useCallback((tag: string) => {
    try {
      AsyncStorage.setItem("sv_bc", `${tag} @ ${Date.now()}`).catch(() => {});
    } catch {
      /* ignore */
    }
  }, []);

  // ── Remote sync telemetry → CF /log ─────────────────────────────────────────
  // The iPad↔iPhone sync is peer-to-peer Multipeer (no server), so we can't see it remotely. This
  // POSTs each device's sync LIFECYCLE (role chosen, connect/disconnect status, page sent/received)
  // to the worker's /log ring buffer, batched ~1s. Then `GET /log` reveals the whole handshake from
  // both sides — turning "follower stuck" guesses into a timeline. Best-effort: never blocks sync.
  const dbgDeviceRef = useRef<string>("?");
  const dbgBufferRef = useRef<Array<Record<string, unknown>>>([]);
  const dbgFlushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dbgFlush = useCallback(() => {
    const batch = dbgBufferRef.current;
    if (batch.length === 0) return;
    dbgBufferRef.current = [];
    fetch(`${RELAY_BASE}/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(batch),
    }).catch(() => {
      /* best-effort telemetry */
    });
  }, []);
  const dbgLog = useCallback(
    (event: string, data?: Record<string, unknown>) => {
      try {
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
    [dbgFlush],
  );
  // ── Fleet readiness check-in (native → the SAME /fleet dashboard as signovivo.com) ──
  // Reports this iPad's native build + role so the director sees who's ready before Mass. Reuses
  // the stable sv_devid; sends a self-entered label but NEVER a phone number. Best-effort.
  const fleetLabelRef = useRef<string>("");
  const fleetCheckin = useCallback((extra?: Record<string, unknown>) => {
    const deviceId = dbgDeviceRef.current;
    if (!deviceId || deviceId === "?") return;
    fetch(`${RELAY_BASE}/fleet/checkin`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        deviceId,
        surface: "native",
        nativeBuild: Number(BUILD_VERSION) || 0,
        label: fleetLabelRef.current || "",
        // Native only knows director/follower — report "Director" or leave role blank so the
        // dashboard fills it from the seeded roster (never a wrong "Cantor" for a Bajo/Guitarrista).
        role: roleRef.current === "director" ? "Director" : "",
        ...(extra || {}),
        // Which BOOK this device holds. nativeBuild is the SHELL's number and can read "current"
        // over a songbook months old (D1), so without this the dashboard cannot answer the only
        // question the rollout cares about: did the update actually arrive?
        ...(activeBookVersionRef.current ? { bookVersion: activeBookVersionRef.current } : {}),
        ...(activeBundleSourceRef.current ? { bundleSource: activeBundleSourceRef.current } : {}),
        ...(bookStageRef.current ? { bookStage: bookStageRef.current } : {}),
        ...(extra || {}),
      }),
    })
      .then(async (r) => {
        if (!r.ok) return;
        // A SUCCESSFUL check-in is the live-internet proof the apply gate depends on. It is
        // recorded here and nowhere else, so it can never be faked by a cached response.
        const at = Date.now();
        lastCheckinOkAtRef.current = at;
        AsyncStorage.setItem(STORAGE_KEYS.lastCheckinOkAt, String(at)).catch(() => {});
        let body: unknown = null;
        try {
          body = await r.json();
        } catch {
          return;
        }
        onCheckinResponseRef.current?.(body);
      })
      .catch(() => {
        /* offline / relay unreachable — presence just isn't reported. Inside the church this is
           the NORMAL case and must stay completely silent: no error state, no UI. */
      });
  }, []);
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
      try {
        fleetLabelRef.current = (await AsyncStorage.getItem("sv_fleet_label")) || "";
      } catch {
        /* ignore */
      }
      fleetCheckin();
    })();
  }, [dbgLog, syncAvailable, fleetCheckin]);
  // Re-report readiness every 90s while mounted — captures a follower who later becomes director
  // without touching the liturgy-critical sync callbacks.
  useEffect(() => {
    const t = setInterval(() => fleetCheckin(), 90000);
    return () => clearInterval(t);
  }, [fleetCheckin]);

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
    return () => setRelayAuthErrorHandler(null);
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
    }, 12000);
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
    // C3: clear the relay publish code on step-down. directorRelaySync coalesces publishes and an
    // in-flight one can drain a final straggler frame AFTER we've stepped down; with no code it 401s
    // (rejected, never applied) instead of shoving the ex-director's stale page onto web followers.
    // becomeDirector re-sets the code, so a legit re-direct is unaffected.
    setRelayPublishCode("");
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
      // The relay authorizes this director's publishes with the exact code they entered.
      // Set BEFORE any broadcast.
      setRelayPublishCode(code);
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
        // re-enters their code (the code itself is never stored — no auto-resume of a credential).
        AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "director").catch(() => {});
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat(); // keep the relay snapshot fresh (guarded on explicitTransmitterRef)
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
        if (myGen !== roleGenerationRef.current) return; // superseded while dropping the link
      }
      try {
        try {
          await startNearbyDirector(DIRECTOR_SESSION);
        } catch {
          // Mesh startup can transiently fail (permission race, radio warm-up).
          // Wait briefly and retry the start exactly once before giving up.
          await new Promise((r) => setTimeout(r, 2000));
          if (myGen !== roleGenerationRef.current) return; // superseded during the retry sleep
          await startNearbyDirector(DIRECTOR_SESSION);
        }
        if (myGen !== roleGenerationRef.current) return; // superseded while the mesh was starting
        roleRef.current = "director";
        // Record the role (breadcrumb only). We deliberately do NOT persist the director code or a
        // timestamp — there is no auto-restore, so a credential must never sit in storage.
        await AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "director");
        if (myGen !== roleGenerationRef.current) return; // superseded while persisting
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat();
        // SPLIT-BRAIN MITIGATION: a brand-new director's token must propagate fast so peers
        // (and any prior director) re-find it and converge, instead of waiting out the ~25s
        // browse cycle while both broadcast and followers flap. Kick an immediate re-browse.
        if (syncAvailable) refreshNearbyDiscovery().catch(() => {});
        breadcrumb("director");
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
          Alert.alert("Cancionero original", "Se restauró el cancionero incluido en la app.");
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
      const isDirectorCode = STANDARD_DIRECTOR_CODES.has(code);
      if (!isDirectorCode) {
        // Unrecognized → tell the web so it surfaces "código incorrecto".
        injectEvent({ type: "role", role: "none" });
        return;
      }
      // ALWAYS ask — a valid code never promotes silently (Miguel: "always ask, always").
      // Super-admin codes (Miguel) get a labeled prompt; everyone else is a plain director.
      // Best-effort heads-up: lastDirectorSnapshotRef is set whenever a director's page has arrived
      // over the mesh, so if it's set another device is (or was just) directing — warn before takeover.
      const isSuperAdmin = SUPER_ADMIN_CODES.has(code);
      // NEW-DIR-3: only warn about taking over a director who is live RIGHT NOW (a fresh mesh
      // snapshot within the heartbeat window) — not one who directed earlier this session and left.
      // Previously this was `Boolean(lastDirectorSnapshotRef.current)`, a set-once-never-cleared flag,
      // so the scary red "Ya hay un director activo / Tomar el control" warning false-fired forever.
      const snap = lastDirectorSnapshotRef.current;
      const liveDirector =
        roleRef.current !== "director" &&
        Boolean(snap) &&
        Date.now() - (snap?.at ?? 0) < LIVE_DIRECTOR_WINDOW_MS;
      const title = liveDirector
        ? "⚠️ Ya hay un director activo"
        : isSuperAdmin
          ? "Super admin — ¿dirigir?"
          : "¿Dirigir el coro?";
      const body = liveDirector
        ? `Otro dispositivo está dirigiendo AHORA. Si continúas${isSuperAdmin ? " (super admin)" : ""}, tú tomas el control y todos te seguirán a ti.`
        : isSuperAdmin
          ? "Entrarás como director (super admin). Los demás dispositivos seguirán tu página."
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
        case "resync": {
          // A follower tapped the ⟳ button in the web UI. The web relay is off in the shell,
          // so do the NATIVE resync: re-request the director's current snapshot over the mesh
          // and re-assert the last-known one immediately (mirrors the foreground resync).
          // If somehow stranded in "off" (e.g. a prior soft-reset left sync off while the web
          // still shows the follower ⟳), re-join the mesh as a follower first so ⟳ recovers it.
          if (roleRef.current === "off" && syncAvailable) becomeFollower();
          if (roleRef.current !== "director") {
            if (syncAvailable) {
              // ⟳ must also kick a fast re-browse: if the director vanished or a NEW director
              // took over (handoff), requestCurrentSnapshot alone can't help until we re-find
              // it. refreshNearbyDiscovery accelerates re-discovery so ⟳ actually recovers.
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
          // so currentPageRef now equals the FAILED page. The 2s mesh heartbeat would then de-dupe
          // (page === currentPageRef.current) and never re-drive this follower → wedged forever.
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

        // Stagger is measured from when this device FIRST saw this pointer, not from launch.
        let firstSeen = await readStored(STORAGE_KEYS.bookFirstSeen, null);
        if (!firstSeen || firstSeen.bookVersion !== pointer.bookVersion) {
          firstSeen = { bookVersion: pointer.bookVersion, at: Date.now() };
          await AsyncStorage.setItem(STORAGE_KEYS.bookFirstSeen, JSON.stringify(firstSeen)).catch(() => {});
        }

        if (stagingInFlightRef.current) return;
        const quarantine = await readStored(STORAGE_KEYS.bookQuarantine, []);
        const active = await readStored(STORAGE_KEYS.bookActive, null);
        const decision = shouldStage({
          killSwitch: SV_BOOK_DL_KILL,
          bookVersion: pointer.bookVersion,
          activeBookVersion: activeBookVersionRef.current,
          stagedBookVersion: staged?.bookVersion ?? null,
          stagedReady: !!staged?.ready,
          quarantine: Array.isArray(quarantine) ? quarantine : [],
          webReady: webReadyRef.current,
          foreground: AppState.currentState === "active",
          role: roleRef.current,
          firstSeenAt: firstSeen.at,
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

    Alert.alert("¿Actualizar el cancionero?", "La app se recargará con el libro nuevo.", [
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

  // ── Multipeer permissions + role bootstrap + event listener ─────────────────
  useEffect(() => {
    if (!syncAvailable) return;
    primeNearbyPermissions().catch(() => {});

    // Role-restore bootstrap: ONCE per session only. If this effect re-runs (its become*/
    // injectEvent useCallback deps changed identity mid-session), re-firing this would re-promote
    // or re-mint a director under the live role — clobbering an intentional in-session role flip.
    // The listener below still re-registers on every run; only this restore is one-shot.
    if (!didBootstrapRef.current) {
      didBootstrapRef.current = true;
      // NO auto-director: a device ALWAYS boots as a follower. Becoming director requires explicitly
      // entering a director code, which then ASKS for confirmation (onDirectorCode). We never
      // silently restore a stale director role — that would step on whoever is actually directing
      // right now. (Miguel, 2026-07-02: "don't just auto-make anyone director — always ask, always.")
      //
      // NEW-DIR-1: but a SILENT demotion is its own hazard — if a DIRECTOR's app restarts mid-Mass it
      // drops to follower with no signal and nobody is directing (the 2026-07-01 outage class). Read the
      // prior role BEFORE becomeFollower() overwrites it; if this device was directing, surface a visible
      // prompt so the operator knows to RE-ENTER their code (the code is deliberately never stored, so we
      // cannot auto-resume). Intentional exit clears lastSyncRole, so this only fires after a crash/kill.
      AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole)
        .then((prev) => {
          lastKnownRoleRef.current = prev ? String(prev) : null;
          if (prev === "director") {
            Alert.alert(
              "Estabas dirigiendo",
              "La app se reinició y ahora sigues al director como los demás. Para volver a dirigir, reingresa tu código en el teclado (♪).",
              [{ text: "Entendido" }],
            );
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
          // De-dupe the 2s mesh heartbeat: if we're already on this page+book it's just a
          // keepalive re-send — do nothing (no redundant renderPage). A genuinely new page, a
          // book switch, or a recovered dropped packet (page differs from ours) still syncs.
          if (page === currentPageRef.current && book === currentBookRef.current) break;
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
      // EVERY FOREGROUND IS A BOOK CHECK — the CodePush shape. The 90 s check-in timer only ticks
      // while the app is awake, so an iPad that was asleep when the new book was published would
      // otherwise sit on the old one until someone happened to leave the app open. Checking here
      // means "open the app" is the entire user-facing procedure. autoApplyIfSafe covers the case
      // where a copy was already staged before the app was backgrounded.
      fleetCheckin();
      void autoApplyIfSafeRef.current?.();
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
        broadcastPage(currentPageRef.current, currentBookRef.current);
      } else if (explicitTransmitterRef.current) {
        // Transmitter-only (no mesh): re-publish on foreground so the relay snapshot doesn't
        // stay stale after the device was backgrounded past the freshness window.
        broadcastPage(currentPageRef.current, currentBookRef.current);
      }
    });
    return () => sub.remove();
  }, [syncAvailable, broadcastPage, injectEvent, fleetCheckin]);

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
          <Text style={styles.fallbackMsg}>La app no cargó bien. Toca para reintentar.</Text>
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
