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
//   5. Pick the starting hymnal book from IP geolocation (Del Rio -> standard manual,
//      everywhere else -> hymns-4) and keep the screen awake.
//
// Everything else — rendering, paging, zoom, search, browse, song navigation — is the
// web app's job. The old 3,536-line FlatList/PDF reader was replaced wholesale.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AppState, Platform, StatusBar, StyleSheet, Text, View } from "react-native";
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
import { publishPageToRelay, TRANSMITTER_ACCESS_CODE } from "./src/directorRelaySync";
import { STORAGE_KEYS, type BookId } from "./src/offlineBooks";
import versionJson from "./version.json";

// ─────────────────────────────── Constants ──────────────────────────────────

const BUILD_VERSION = String((versionJson as { buildNumber?: number }).buildNumber ?? "");
const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";
const RELAY_ROOM = "alvernia-main";
const GEO_STATE_URL = `${RELAY_BASE}/r/${RELAY_ROOM}/state`;

// Fixed Multipeer session for the parish mesh (unchanged from the native reader).
const DIRECTOR_SESSION = "1234";

// Director codes — admin can force-takeover; the three regular codes start a director
// session (the Swift conflict logic lets the most-recent director win on contention).
const ADMIN_CODE = "8307343376";
const DIRECTOR_ACCESS_CODES = new Set<string>([
  ADMIN_CODE,
  "8304533367",
  "8307197000",
  "8303130470",
]);
// Secret numpad codes carried over from the native reader.
const SOFT_RESET_CODE = "744668486";
const FORCE_DIRECTOR_CODE = "50711";
// Last-director restore window: become director again on relaunch if we were one recently.
const DIRECTOR_RESTORE_WINDOW_MS = 24 * 60 * 60 * 1000;

const STORED_CODE_KEY = "director_access_code";
const DEFAULT_BOOK: BookId = "hymns-4";

type SyncRole = "off" | "director" | "follower";

const isBookId = (value: unknown): value is BookId => value === "standard" || value === "hymns-4";
const digitsOnly = (value: unknown) => String(value ?? "").replace(/[^0-9]/g, "");
const modeForBook = (book: BookId) => (book === "standard" ? "standard" : "nonStandard");
// Multipeer/relay carry both a canonical bookId and a legacy mode string; prefer bookId.
const bookFromSync = (bookId: unknown, mode: unknown): BookId => {
  if (isBookId(bookId)) return bookId;
  return String(mode ?? "") === "standard" ? "standard" : "hymns-4";
};

// ──────────────────────────────── App ───────────────────────────────────────

export default function App() {
  useKeepAwake("signovivo-reader");

  const webViewRef = useRef<WebView>(null);
  const [booted, setBooted] = useState(false);
  const [initialBook, setInitialBook] = useState<BookId>(DEFAULT_BOOK);
  // Bumped to force a fresh WebView mount (e.g. after a peer-pushed bundle update / soft reset).
  const [bundleUri, setBundleUri] = useState<string | null>(null);
  const [mountKey, setMountKey] = useState(0);

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
  const storedBookRef = useRef<BookId | null>(null);
  // Last page/book the DIRECTOR broadcast to us over the mesh (distinct from the web's own
  // page). Drives resync after a WebView reload / foreground for followers. Null until a
  // director snapshot has actually been received (a fresh-boot follower must keep its own page).
  const lastDirectorSnapshotRef = useRef<{ page: number; book: BookId } | null>(null);
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
    })();
  }, [dbgLog, syncAvailable]);

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

  // ── Page broadcast: mesh (director) + relay (director or explicit transmitter) ──
  const broadcastPage = useCallback((page: number, book: BookId) => {
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
  // MESH every 2s: a tiny local Multipeer re-send. Followers DE-DUPE a same-page re-send
  // (see the "page" case below), so this only does work when a page-turn packet was
  // dropped — and then the follower recovers within ~2s. A fast cadence is free on a LAN.
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
    }, 2000);
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
      dbgLog("become:director", { wasFollower: roleRef.current === "follower", syncAvailable });
      if (!syncAvailable) {
        // No mesh on this device — still act as an online transmitter to the relay.
        explicitTransmitterRef.current = true;
        roleRef.current = "off";
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
        await AsyncStorage.multiSet([
          [STORAGE_KEYS.lastSyncRole, "director"],
          [STORAGE_KEYS.lastDirectorAt, String(Date.now())],
          [STORED_CODE_KEY, code],
        ]);
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
      await AsyncStorage.multiRemove([
        STORAGE_KEYS.lastSyncRole,
        STORAGE_KEYS.lastDirectorAt,
        STORED_CODE_KEY,
      ]);
    } catch {
      /* ignore */
    }
    roleRef.current = "off";
    explicitTransmitterRef.current = false;
    webReadyRef.current = false;
    pendingInjectRef.current = [];
    setMountKey((k) => k + 1); // remount the WebView from scratch
  }, [breadcrumb, stopDirectorHeartbeat]);

  // ── Director-code dispatch (codes entered on the web numpad) ────────────────
  const onDirectorCode = useCallback(
    (rawCode: unknown) => {
      // A fresh code entry is a new role-entry path: supersede any in-flight become* up front.
      // (The become*/performSoftReset paths below re-bump and capture their own generation.)
      roleGenerationRef.current++;
      const code = digitsOnly(rawCode);
      if (!code) {
        injectEvent({ type: "role", role: "none" });
        return;
      }
      if (code === SOFT_RESET_CODE) {
        performSoftReset();
        return;
      }
      if (code === FORCE_DIRECTOR_CODE || DIRECTOR_ACCESS_CODES.has(code)) {
        becomeDirector(code);
        return;
      }
      if (code === digitsOnly(TRANSMITTER_ACCESS_CODE)) {
        explicitTransmitterRef.current = true;
        injectEvent({ type: "role", role: "director" });
        broadcastPage(currentPageRef.current, currentBookRef.current);
        startDirectorHeartbeat(); // keep the relay snapshot fresh (guarded on explicitTransmitterRef)
        return;
      }
      // Unrecognized → tell the web it was wrong so it can surface "código incorrecto".
      injectEvent({ type: "role", role: "none" });
    },
    [injectEvent, performSoftReset, becomeDirector, broadcastPage, startDirectorHeartbeat],
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
          if (typeof msg.page === "number") currentPageRef.current = msg.page;
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
          if (roleRef.current === "director") {
            // The director's own page is authoritative — just re-broadcast it.
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
          }
          break;
        }
        case "page-changed": {
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
        case "book-changed": {
          if (isBookId(msg.book)) {
            currentBookRef.current = msg.book;
            storedBookRef.current = msg.book;
            AsyncStorage.setItem(STORAGE_KEYS.activeBookId, msg.book).catch(() => {});
            // A director switching books must move followers too. The web now sends the new
            // book's start page; adopt it so we don't broadcast the OLD book's page (e.g. 360)
            // onto a 51-page book where it would clamp wrong. Fall back to our last page only
            // when the web omitted it (non-finite).
            const startPage = Number(msg.page);
            const page = Number.isFinite(startPage) ? startPage : currentPageRef.current;
            currentPageRef.current = page;
            broadcastPage(page, msg.book);
          }
          break;
        }
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
          // Director tapped the badge to step down. Forget the director code/time so a relaunch
          // doesn't auto-restore director.
          AsyncStorage.multiRemove([STORAGE_KEYS.lastDirectorAt, STORED_CODE_KEY]).catch(() => {});
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

  // ── Resolve the bundle URI (prefer a peer-pushed update in Documents) ───────
  const resolveBundleUri = useCallback(async (): Promise<string> => {
    const docDir = FileSystem.documentDirectory;
    if (docDir) {
      const docIndex = `${docDir}WebBundle/index.html`;
      try {
        const info = await FileSystem.getInfoAsync(docIndex);
        if (info.exists) return docIndex;
      } catch {
        /* fall through to bundled copy */
      }
    }
    const bundleDir = FileSystem.bundleDirectory || "";
    return `${bundleDir}WebBundle/index.html`;
  }, []);

  // ── Boot: restore book + role, resolve bundle, render WebView ───────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      breadcrumb("boot");
      let stored: BookId | null = null;
      try {
        const raw = await AsyncStorage.getItem(STORAGE_KEYS.activeBookId);
        if (isBookId(raw)) stored = raw;
      } catch {
        /* ignore */
      }
      storedBookRef.current = stored;
      const startBook: BookId = stored ?? DEFAULT_BOOK;
      currentBookRef.current = startBook;

      const uri = await resolveBundleUri();
      if (cancelled) return;
      setInitialBook(startBook);
      setBundleUri(uri);
      setBooted(true);
    })();
    return () => {
      cancelled = true;
    };
  }, [breadcrumb, resolveBundleUri]);

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
      (async () => {
        try {
          const [lastRole, lastAtRaw] = await Promise.all([
            AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole),
            AsyncStorage.getItem(STORAGE_KEYS.lastDirectorAt),
          ]);
          const lastAt = Number(lastAtRaw) || 0;
          const recentlyDirector =
            lastRole === "director" && Date.now() - lastAt < DIRECTOR_RESTORE_WINDOW_MS;
          if (recentlyDirector) {
            const code = (await AsyncStorage.getItem(STORED_CODE_KEY)) || ADMIN_CODE;
            await becomeDirector(code);
          } else {
            await becomeFollower();
          }
        } catch {
          becomeFollower();
        }
      })();
    }

    const sub = addNearbyDirectorSyncListener((event: Record<string, unknown>) => {
      const type = String(event?.type ?? "");
      switch (type) {
        case "page": {
          if (roleRef.current === "director") break; // ignore our own echoes
          const book = bookFromSync(event.bookId, event.mode);
          const page = Number(event.page) || currentPageRef.current;
          dbgLog("mesh:page-recv", {
            page,
            book,
            dup: page === currentPageRef.current && book === currentBookRef.current,
          });
          // Remember the director's latest snapshot so a reloaded/foregrounded follower resyncs.
          lastDirectorSnapshotRef.current = { page, book };
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
          // A peer pushed a newer web bundle; reload from Documents/WebBundle.
          (async () => {
            const uri = await resolveBundleUri();
            webReadyRef.current = false;
            pendingInjectRef.current = [];
            setBundleUri(uri);
            setMountKey((k) => k + 1);
          })();
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

  // ── IP-geo book selection (first launch only; stored pref wins thereafter) ──
  useEffect(() => {
    if (!booted) return;
    if (storedBookRef.current) return; // user/device already has a book preference
    // The DIRECTOR'S book always wins over geo: if a director snapshot has already been adopted
    // (a follower synced to the director's page+book over the mesh before this async fetch
    // resolved), geo must NOT override it. storedBookRef stays null in that path (the mesh
    // listener doesn't set it), so this snapshot guard is the only thing that prevents geo from
    // ripping the follower onto the geo book's DEFAULT page mid-Mass.
    if (lastDirectorSnapshotRef.current) return;
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(GEO_STATE_URL, { cache: "no-store" });
        const hymnal = res.headers.get("X-Hymnal");
        const geoBook: BookId | null =
          hymnal === "standard" ? "standard" : hymnal === "nonstandard" ? "hymns-4" : null;
        // Re-check AFTER the await: a director snapshot can land while the fetch is in flight.
        if (cancelled || lastDirectorSnapshotRef.current) return;
        if (!geoBook || geoBook === currentBookRef.current) return;
        currentBookRef.current = geoBook;
        storedBookRef.current = geoBook;
        AsyncStorage.setItem(STORAGE_KEYS.activeBookId, geoBook).catch(() => {});
        injectEvent({ type: "set-book", book: geoBook });
      } catch {
        /* offline: keep the default/stored book */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [booted, injectEvent]);

  // ── Foreground: nudge mesh rediscovery + pull a fresh snapshot ──────────────
  // Always registered: a transmitter-only device (no mesh, syncAvailable false) becomes a
  // transmitter AFTER mount via a numpad code, so we can't gate the listener on the mount-time
  // syncAvailable value — it would never fire for that device. The listener body is fully
  // role-gated (every branch checks roleRef / explicitTransmitterRef) and mesh-only calls are
  // gated on syncAvailable, so this is harmless for a plain offline follower.
  useEffect(() => {
    const sub = AppState.addEventListener("change", (next) => {
      if (next !== "active") return;
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
  }, [syncAvailable, broadcastPage, injectEvent]);

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
          webViewRef.current?.reload();
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
      {/* Always-visible build badge — a NATIVE overlay (not web UI) so the running build number is
          legible on every screen and even survives a broken/blank WebView. Lets us confirm BOTH
          devices are on the SAME build (a build mismatch silently breaks director↔follower sync).
          pointerEvents="none" so it never intercepts a tap. */}
      {BUILD_VERSION ? (
        <Text style={styles.buildBadge} pointerEvents="none" allowFontScaling={false}>
          {`b${BUILD_VERSION}`}
        </Text>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#000" },
  blank: { flex: 1, backgroundColor: "#000" },
  web: { flex: 1, backgroundColor: "#000" },
  // Tiny dim build badge, bottom-right. Dark backing keeps it legible over white book pages.
  buildBadge: {
    position: "absolute",
    bottom: 6,
    right: 8,
    fontSize: 10,
    color: "rgba(255,255,255,0.5)",
    backgroundColor: "rgba(0,0,0,0.4)",
    paddingHorizontal: 5,
    paddingVertical: 1,
    borderRadius: 6,
    overflow: "hidden",
    fontVariant: ["tabular-nums"],
  },
});
