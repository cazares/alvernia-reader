> ⚠️ **CORRECTION BANNER (2026-07-09).** This map was written at HEAD 16244b25 / build 377. The branch has since been fast-forwarded to build 381 (d5075091). Landed since this map: **#269** capped the "Buscando director…" spinner so it never looks stuck; **#270 REMOVED the "¿Quién usa este iPad?" fleet self-ID modal entirely** (fleet check-in itself remains); **#271** simplified the sync spinner and renamed the songbook PDF (assets/alvernia_manual_2.pdf → assets/signo_vivo_371.pdf), touching web/src/app.js (−112 lines area), index.html, styles.css. Where this map contradicts current source, CURRENT SOURCE WINS — do not report the removed modal or old spinner behavior as findings.

# SignoVivo — WEB subsystem map (current state, native build 377)

Cartographer: "web". All anchors verified against the working tree at
`/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a` (HEAD 16244b25).
Files covered: `web/src/app.js` (3,638 lines — full), `web/src/index.html` (381), `web/src/styles.css` (2,402),
`web/src/sw.js` (237), `web/src/manifest.webmanifest`, `web/build.mjs` (715), `web/src/lib/{svRelayRoom,svSyncDecision,svSelftest}.js`.
The atlas/contracts docs describe build 370 — DO NOT trust their line numbers; this map supersedes them for the web side.

---

## 1. Architecture

### 1.1 Bundle shape
- **One plain, non-module script**: `web/src/app.js`. No bundler; `web/build.mjs` copies it with token replacement:
  - `__CACHE_VERSION__` → `<git-short-sha>-<8-hex content hash of app.js/sw.js/styles.css/index.html/manifest + build.mjs>` (build.mjs:10-28). Content-hashing exists because the bundle is built from the **working tree** — a SHA-only version collided across two deploys at the same HEAD.
  - `__RELAY_BASE__` → `process.env.ALVERNIA_RELAY_BASE` or `https://signovivo-sync.4j4982y8jp.workers.dev` (build.mjs:63-68).
  - `__BUILD_NUMBER__` → `version.json.buildNumber` (build.mjs:32-39).
  - app.js also self-defends against un-replaced tokens: `RELAY_BASE_RAW.startsWith("__RELAY")` fallback (app.js:2849-2852), `BUILD_NUMBER[0] !== "_"` guards (app.js:3545-3547, 2916).
- **Pre-app helper libs** loaded as `<script defer>` in document order BEFORE app.js (index.html:376-379): `lib/svRelayRoom.js`, `lib/svSelftest.js`, `lib/svSyncDecision.js`. Copied verbatim to `dist/lib/` (build.mjs:52-61). All three are UMD, ES5, dependency-free, never-throw; app.js has its own fallback if any fails to load.
- **Same bundle, two hosts**: served at signovivo.com (Cloudflare Pages, prod branch = main) AND loaded `file://` inside the native WKWebView. `NATIVE_FILE_MODE = window.__SIGNO_VINO_NATIVE_FILE_MODE || protocol === "file:"` (app.js:227). `resolveAppPath()` strips leading `/` in file mode (app.js:229-233). Page URLs are always **relative** (`books/standard/pages/page-NNN.webp`, app.js:258-263) so they resolve on both hosts.
- **Build pipeline for the book** (build.mjs:108-155, 562-666): `pdftoppm -r 115` PNG → `cwebp -q 60` (env-overridable `ALVERNIA_PDF_RENDER_DPI` / `ALVERNIA_PDF_WEBP_QUALITY`), plus a full OCR pipeline (`pdftotext`) deriving song titles (+7 manual `TITLE_OVERRIDES`, build.mjs:160-168), themes (weighted phrase/word scoring, build.mjs:363-553), key detection from chord lines, intro chords + capo, and harmonic complexity. Emits per-book `pages.json` (totalPages + songIndex + themeIndex), `search-index.json`, `song-titles.json`, `song-search-index.json` (build.mjs:633-649).
- **Single book, everywhere**: `BOOK_ID = "standard"`, `STANDARD_TOTAL_PAGES = 371` fallback (app.js:185-189). build.mjs still writes a single-entry `books.json` "registry" (build.mjs:678-686) and inlines it as `#books-data` (build.mjs:697-699) — **app.js never reads either** (see Oddities).
- Inlined boot data: `#pages-data` = `{ totalPages }` only (build.mjs:699) — the bare minimum for first paint; song index hydrates from `/books/standard/pages.json` in the background.
- `_headers`: `/books/*/pages/*` → `Cache-Control: public, max-age=31536000, immutable` (build.mjs:710-713). This immutability is load-bearing for sw.js's cross-version page-cache fallback.

### 1.2 svRelayRoom.js (relay room resolver — Release Safety M1)
- `resolveRelayRoom(search)`: `?env=staging` → `alvernia-staging`, else/any-error → `alvernia-main` (svRelayRoom.js:28-36). Whitelist of exactly two rooms. Consumed once at module-eval in app.js:2858-2869 with a triple guard (lib present? / exact string `"alvernia-staging"`? / try-catch) — only the exact staging value ever passes.

### 1.3 svSyncDecision.js (pure follower-sync decision — P2)
- `decideRelaySnapshot(snap, ctx)` returns `{action, hasDirector, lastSeq, browsing, livePage, renderPage, renderPill, reveal, hideGoLiveBar}` with actions `reject | demote | live-dup | browsing | follow` (svSyncDecision.js:51-139).
- **Ordering contract: FRESHNESS FIRST, seq de-dup second** (P2-SEQ, :87-101) — a stale/silent director is demoted even when its seq is a duplicate (the old inline order froze the green pill on a dead director at Wednesday practice).
- Fresh = `seq > 0` AND (`ts` absent OR `serverNow - ts <= maxAgeS` [90s]) where serverNow = `(nowMs + clockOffsetMs)/1000` (:71-85).
- **F4**: demote resets `lastSeq = -1` (:97) so a restarted director re-publishing from a low seq is followable.
- `force` bypasses the seq de-dup (:111) — used by boot poll / reconnect / foreground / polling fallback so a stationary director still re-homes a follower.
- Clock calibration helpers: `clockOffsetFromServerNow(body.now, …)` (preferred; CORS-safe) and `clockOffsetFromDateHeader` (same-origin fallback) (:149-176).

### 1.4 svSelftest.js (?selftest readiness card — M1)
- 5 checks, all timeout-bounded (6s): build identity, page 1 image actually loads, sane page count, relay GET `/state` on the CURRENT room (room name shown loudly), native bridge (applicable only in file mode) (svSelftest.js:39-106). `renderCard` builds a GREEN "✓ TODO BIEN" / RED "✗ REVISAR" overlay with createElement/textContent only (:109-168). Read-only by design; zero footprint without the query param.

---

## 2. Boot / reveal sequence (as of today — what remains of the geo gate)

Module-eval order in app.js:

1. **bootGuard IIFE** (app.js:12-47) — armed before anything can throw. Non-capturing `window "error"` + `unhandledrejection` → record `window.__SV_LAST_ERROR`, call `window.__svReportCrash` if registered, and — ONLY if `window.__svBooted !== true` — replace the screen with a "Signo Vivo se está recuperando / Reintentar" card (`#sv-crash-banner`, z 2147483600) that reloads on tap. Idempotent via `__svRecoveryShown`. Exposes `window.__svRecover`.
2. DOM refs (:50-130), state object (:144-180). All module-eval `localStorage` reads are try-guarded (:386, :396-401, comments cite the Wednesday white-screen).
3. Entry calls (:3584-3598): `clearInitialUrl()` → `registerServiceWorker()` → `bindViewportMetrics()` → `bindReaderEvents()` → `initScreenWakeLock()` → `initReader().catch(…)` → `setTimeout(liftGateNow, 4000)` anti-trap backstop.
4. `?selftest` block (:3600-3638) — gated on `initialUrl.search` (captured pre-`clearInitialUrl`), runs 1800ms after boot.

**The "geo gate" today**: `#geo-gate` (index.html:44-49) is purely a white full-page **loading screen** (legacy id kept to avoid churn; comment at index.html:41-43). CSS: fixed, z 150, spinner+caption fade in only after 500ms so fast loads show clean white (styles.css:70-119). `liftGateNow()` (app.js:59-66) adds `.is-hidden` once AND sets `window.__svBooted = true` (the bootGuard's "app is up" latch). `revealReader()` is a thin alias (:67). There is **no geo resolution, no book decision, no unlock** — all deleted in build 374.

**initReader** (app.js:3446-3541), step by step:
- Parse inlined `#pages-data` (defensive), else fetch `/books/standard/pages.json`, else `{}` (:3452-3468).
- Adopt `totalPages` only if positive integer, else 371 (:3471-3476). `state.currentPage = DEFAULT_START_PAGE = 2` (:215, :3477).
- `hydrateSongIndex(data)` (:3482-3498): sorts songIndex, builds `songPageLookup`, reconciles a drifted totalPages (re-render if changed), `renderStatus()` + `renderActiveTab()`. Runs inline only if the manifest carried `songIndex` (it doesn't in the normal web build — hydrates in background at :3520-3525).
- `setOfflineGateState({visible:false})` (:3507) — the offline gate never shows anymore (see Oddities).
- **Boot relay peek** (web only): `await Promise.race([relayPollOnce(true), 1500ms timeout])` (:3512-3514) so a live director's page is the FIRST page painted; if no director, `renderPage(2)` (:3515).
- `revealReader()` immediately (:3517) — nothing to wait for.
- `deferOfflinePrecache(state.totalPages)` (:3528), `startRelayFollow()` (:3529), `scheduleFleetCheckin()` (:3530), `renderActiveTab()` (:3533), `postNativeBridge({type:"bridge-ready", page, totalPages, book})` (:3535-3540).
- On any initReader throw: `setLoading(true, "No se pudo cargar Signo Vivo.")` + `revealReader()` (:3589-3593).

`clearInitialUrl()` (:669-678) strips the query via `history.replaceState` EXCEPT when it contains `env=staging` or `selftest` (a reload must not silently fall back to the live Mass room mid-canary-walk).

---

## 3. User-visible surfaces + controls (who sees what)

Role mechanism: `renderDirectorModeBadge()` (app.js:845-852) sets `document.documentElement.dataset.role = "director" | "follower"` from `state.nativeSyncRole` — the ONLY writer. It is only invoked from the native bridge `role` event (:959-968), so on pure web the attribute is **never set**; the base CSS is the follower layout (search-fab `display:none` at styles.css:2154; director rules keyed on `html[data-role="director"]` at styles.css:2123, 2155-2156).

| Surface | Element / created at | Who sees it | Behavior |
|---|---|---|---|
| Page viewer | `#page-image` (index.html:54) | everyone | swipe L/R turns page (app.js:2745-2749); img error → up to 4 `?retry=` reloads (app.js:70-77); slide animation classes (:1080-1086) |
| Loading gate | `#geo-gate` (index.html:44) | everyone, boot only | §2. Vestigial `.is-offline` styling (styles.css:113-117) is never applied by current JS |
| Build badge | `#build-badge` (index.html:51; app.js:3554-3558) | everyone | bottom-right "377…", z 200, pointer-events none (styles.css:2070-2085). Native injects `__SIGNO_VINO_NATIVE_BUNDLE_VERSION`; web uses baked BUILD_NUMBER (:3543-3547) |
| ⟳ resync fab | `#resync-fab` (index.html:85) | **followers only** (hidden for director, styles.css:2156) | top-LEFT. Tap → spin 1.1s + `reconnectRelay()` (app.js:2427-2432, 3154-3173). On native → bridges `{type:"resync"}` instead of touching the web relay |
| ♪ jump-to-song fab | `#song-jump-trigger` (index.html:91) | everyone | top-RIGHT; shifts 4.5rem left only when the director's ⌕ shows (styles.css:2122-2123). Opens the modal (app.js:2419). Hidden while drawer open via `body.sv-drawer-open` (styles.css:2159-2160) |
| ⌕ search fab | `#search-fab` (index.html:92) | **director only** (styles.css:2154-2155) | opens the drawer in `as-dropdown` mode + `activateTab("buscar")` (app.js:2422-2423). Dropdown CSS styles.css:1008-1050 (full-width panel, drawer chrome hidden) |
| ⛶ fullscreen fab | `#fullscreen-fab` (index.html:96) | web where fullscreen supported; hidden in NATIVE_FILE_MODE (app.js:1140-1150) | bottom-right; `toggleFullscreen()` (:2003-2017; iOS-standalone pseudo mode is a no-op :2005-2008) |
| Jump-to-song modal | `#song-jump-modal` (index.html:111-146) | everyone | numpad (digits/backspace incl. hold-to-repeat :2394-2411), "♪ Abrir Canto ♪", Cancelar. Also opened by tapping the song title `#song-status` (:2421). Physical keyboard digits/Enter/Escape work only while open (:2827-2832) |
| Drawer | `#navigation-drawer` (index.html:154) | everyone (but see below) | opened ONLY by left-edge swipe (<44px start, >40px right — viewer :2737-2742 and window-level :2801-2822) or the ⌕ fab (director). The visible pull-tab `#drawer-handle` is **display:none !important** (styles.css:2331) though JS still binds/toggles it (:2491-2494, 1122, 1135). Contains: "← Cerrar menú", prev/next SONG buttons + song title + intro-chords line, tab rail (Buscar ⛪Misa 🕐Recientes 📅Tiempo 🏷️Temas 🎵Tono 📋Todas), results pane. The Teclado/Explorar mode switch is CSS-retired (styles.css:2328); drawer is browse-only (`openDrawer` forces `switchDrawerMode("browse")` :1128) |
| Search UI | `#search-row/#search-input/#search-results` | via drawer Buscar tab or ⌕ dropdown | full-text search over search-index.json (lazy-loaded on first open, :2349, 1227-1247); theme-name shortcut (:1361-1374); sort toggle best/Nº/A–Z (:2556-2567); `<mark>` snippet highlighting (:1336-1353); keyboard arrows navigate results (:2534-2554) |
| Relay pill | `#sv-live-pill` (injected app.js:3078-3100) | web followers when a director is live | 8px dot, top-right, z 46. Green pulsing = following live; amber = browsing (tap → `goLive()`); hidden when no director (:3102-3108) |
| "Volver a en vivo" bar | `#sv-golive-bar` (injected :3111-3135) | web follower who numpad-jumped off the live page | bottom-center green pill button → `goLive()` (:3138-3149) |
| Sync spinner | `#sv-sync-spinner` (injected :859-890) | **native only** (driven by mesh `state` events :984-987) | "Buscando director… / Conectando…" top-center pill |
| Relay-auth warning | `#sv-relay-warn` (injected :898-937) | **native director only** (bridge `relay-auth-error` :974-977) | red assertive banner: relay rejected the director code → signovivo.com followers are dark. Dismissible, idempotent |
| Director badge | `#director-mode-badge` (index.html:55) | **native director only** | top-left "DIRECTOR / ✕ Salir"; tap → `confirm()` → bridge `{type:"exit-director"}` (:2438-2443) |
| Offline gate | `#offline-gate` (index.html:60-74) | **nobody today** — see Oddity O3 | spinner + rotating phrases + retry/continue; only call is `visible:false` (:3507) |
| Fleet picker | `#fleet-picker` (index.html:355-371; app.js:3008-3057) | web, once, ~2.5s after gate lift | "¿Quién usa este iPad?" name+role → localStorage + `/fleet/checkin`; Skip checks in anonymously |
| Help panel | `#help-panel` (index.html:278-348) | **unreachable on web** — see Oddity O4 | instructions + haptic toggle + "Versión N" label |
| Selftest card | `#sv-selftest-card` | operator with `?selftest` | §1.4 |
| Crash recovery card | `#sv-crash-banner` | anyone whose boot threw | §2 step 1 |

Numpad-panel stubs kept only so JS refs stay non-null: `#fullscreen-button`, `#prev-corner`, `#help-button`, `#display-clear` (index.html:207-212), `#search-cancel` (index.html:275). `#search-index` doesn't exist at all (`searchIndexButton` is null; all uses `?.`-guarded, e.g. app.js:108, 1899, 2656).

---

## 4. Flows

### 4.1 Page render pipeline
`renderPage(pageNumber, {pushToHistory, direction})` (app.js:1041-1111):
- `clampPage` guarantees an in-range INTEGER (floats/NaN would 404 as `page-2.7.webp`) (:274-279).
- Stale-request guard: `state.pageLoadRequest` monotonic id; a slower older load never commits (:1043, 1061, 1094).
- Preload into an OFF-DOM `Image` with a 3s timeout that **resolves "timeout" as success** (:1007-1028, 1071-1073); one retry with a cache-busting `?reload=<ts>` on error (:1053-1058). The live `<img>.src` is committed exactly once post-guard (:1069).
- History: last 50 pages (:1063-1066); `goBackInHistory` (:1220-1224).
- Posts `{type:"page-changed", page, totalPages, book}` to native on EVERY successful render (:1074-1079) — including relay/mesh-applied ones (native de-dupes).
- Failure path: "No se pudo cargar esta página." overlay + `{type:"render-failed", page, book}` to native so the mesh clears its optimistic currentPageRef and the next heartbeat re-drives the render (:1093-1110).
- Perf: adjacent-SONG prefetch (:730-744, 1089) + neighbor-page (+1/+2/−1) low-priority prefetch with `decode()` warm (:751-772, 1092).

### 4.2 Song navigation / draft / codes
- Draft numpad accepts up to **10 digits** (:1153-1159). `goToDraftSong` (:1184-1218): **≥5 digits = code**, routed to native `{type:"director-code", code}` when a bridge exists; on pure web it flashes "Código no válido" (:1190-1204, flash helper :830-841). ≤4 digits = song number → `resolveSongPage` (exact map hit, else first song ≥ N's page, else last page) (:700-709).
- Jump landing off the director's live page sets `relay.browsing = true` + shows the go-live bar (:1212-1217) — the ONLY user action that pauses auto-follow.
- `turnSong(±1)` moves between song start-pages (:1978-1994); `turnPage(±1)` by raw page (:1996-2000). Keyboard arrows = SONG; swipe = PAGE (:2835-2836 vs :2745-2749).
- Recientes: last 15 song numbers in localStorage (:404-418).

### 4.3 Relay-follow lifecycle (web follower)
State: `relay` object (app.js:3059-3075) — `backoff, manualClose, pollTimer, ws, heartbeatTimer, reconnectTimer, lastMsgAt, lastSeq, browsing, following, appliedPage, livePage, hasDirector, clockOffsetMs, healthTimer`.

- `startRelayFollow()` (:3394-3444) — skipped entirely when `hasNativeBridge() || NATIVE_FILE_MODE` (:3395). Wires:
  - `visibilitychange` → foreground: force poll + tear down a missing/non-OPEN/12s-silent socket and reconnect at 500ms backoff (:3396-3409).
  - `online` → same stale-socket teardown then `connectRelay()` (:3410-3422) (iOS can keep a dead socket at readyState OPEN with no close event).
  - **F3 health watchdog**: unconditional 10s `setInterval` — if ws is gone/closing/closed, or OPEN-but-silent >15s → teardown, `startRelayPolling()`, reconnect (:3429-3441). The only TIME-driven floor (iOS guarantees none of the event-driven paths fire).
  - Initial `relayPollOnce(true)` + `connectRelay()` (or polling-only if no WebSocket) (:3442-3443).
- `connectRelay()` (:3281-3392):
  - **F5**: clears any pending `reconnectTimer` before the dupe-guard early-return (:3287).
  - Dupe guard: readyState 0/1 → return (:3292).
  - **P2-POLL-GAP**: starts the 4s `/state` poll through the CONNECTING window; the open handler stops it (:3294-3301, 3323).
  - **Zombie-CONNECTING guard**: 6s timer force-closes a socket stuck at readyState 0 (:3313-3317).
  - `open`: reset backoff, `relayPollOnce(true)` force-resync, install a **per-socket** 4s heartbeat (id captured locally AND on `ws.__svHeartbeat` so an old socket's close can never reap a newer socket's heartbeat — :3333-3362). Heartbeat sends `"ping"`; **F2**: >12s silence → close + start polling (:3350); **F1**: while following, if `currentPage !== livePage` (stray swipe drift) force a re-home poll every beat (:3357-3359).
  - `message`: `lastMsgAt` stamp + `applyRelaySnapshot(JSON.parse(...))` (:3364).
  - `close`: clear own heartbeat only; `relay.ws = null` only if still us; jittered (±30%) exponential backoff 500ms→8000ms; at the 8000 cap also start `/state` polling (:3366-3391).
- `relayPollOnce(force)` (:3240-3276): GET `/r/<room>/state` with a 6s AbortController (navigator.onLine lies on wifi-without-internet); calibrates `relay.clockOffsetMs` from body `now` (preferred) or the Date header BEFORE judging freshness (P2-CLOCKSKEW); then `applyRelaySnapshot(snap, {force})`.
- `applyRelaySnapshot` (:3179-3232) is a thin executor of `svSyncDecision.decideRelaySnapshot` (§1.3); it carries a conservative inline fallback if the lib is missing (:3184-3198). On `follow` it always sets `following = true` — a stray swipe never permanently strands a follower (:3221-3229). `snap.bookId` is vestigial wire compat (comment :3212-3214).
- Polling fallback: `startRelayPolling()` = 4s interval of `relayPollOnce(true)` (:3279).
- `reconnectRelay()` (⟳ tap, :3154-3173): native → bridge `resync`; web → reset browsing/backoff/`lastSeq=-1`, close + reopen the socket, immediate forced poll.
- `goLive()` (:3138-3149): exit browse, snap to `relay.livePage`, hide bar, re-render pill.

### 4.4 PWA / offline / SW update flow
- `registerServiceWorker()` (app.js:2073-2106): skipped in file mode / insecure context. Registers `/sw.js` with `updateViaCache:"none"`.
  - **controllerchange reload semantics**: reload when the activation was flagged by THIS tab (`sessionStorage["sv-sw-reload-pending"]`) OR when a controller already existed at boot (`hadControllerAtBoot` — a true update that activated without our flag, e.g. a sibling tab skipped waiting). First install never reloads; `hasReloadedForUpdate` prevents loops (:2085-2096).
  - `wireServiceWorkerRegistration` (:2028-2071): immediately activates an already-`waiting` SW; on `updatefound`→`installed` (with an existing controller) posts SKIP_WAITING; **adaptive update polling**: 3s after foregrounding, backing off ×1.5 to a 60s ceiling while idle, snapped back to 3s on `visibilitychange` (:2053-2070); also `registration.update()` on `online`.
- **sw.js**:
  - Caches: `signo-vivo-static-<v>` / `signo-vivo-pages-<v>` (sw.js:2-3). Install precaches CORE_ASSETS individually via `Promise.allSettled` (never atomic) (:74-85).
  - **Gated takeover**: `skipWaitingIfShellReady()` — skipWaiting only if all CRITICAL_SHELL_ASSETS (`/`,`/index.html`,`/styles.css`,`/app.js`) are in the NEW static cache, or it's a genuine first install (:44-72). Same gate honored for the app.js-posted SKIP_WAITING message (:131-137). Prevents a shell-less (offline-installed) SW from wiping a fully-cached old one.
  - Activate: keeps the **2 newest page caches** (pages are immutable, so the previous version's full bundle keeps an offline follower alive right after a deploy) and deletes old static caches only once the new shell is verified cached (:87-129).
  - Fetch routing: page images (`pathname.includes("/pages/")`) → cache-first in PAGE_CACHE with `ignoreSearch` (so `?retry=`/`?reload=` recovery still hits the cached bare URL), then **cross-version page-cache fallback** (`matchAnyPageCache`, :152-161), then network (stored under the normalized bare URL) (:169-198). Shell paths (misnamed `NETWORK_FIRST_PATHS`) → **stale-while-revalidate** (:205-224). Everything else same-origin GET → cache-first + populate (:226-236).
- **Offline bundle (app.js side)**: `ensureOfflineBundle(totalPages)` (:577-613) — core assets via allSettled, then missing pages fetched `no-store` with concurrency 4 into PAGE_CACHE; sets `sv-offline-ready-<v>` + IndexedDB metadata (`signo-vivo-offline`/`bundle-status`/`current` = {version, totalPages, verifiedAt}); fires `fleetCheckin({webCached:true})`. Triggered only by `deferOfflinePrecache` (:620-639): waits 4s settle + polls `gateLifted` every 1.5s so first paint + relay handshake win the bandwidth; retries on failure via the reset flag. `isOfflineBundleReady` (:641-666) re-verifies flag + IDB metadata + core assets + page count ≥ total — **but nothing calls it anymore** (see Oddity O3).
- **Wake lock** (:3566-3582): `navigator.wakeLock.request("screen")`, re-acquired on foreground; fully guarded.
- manifest.webmanifest: standalone/fullscreen display, `orientation: any`, scope `/`, dark theme `#060a18`.

### 4.5 Native-bridge contract (web side)
- **Inbound dispatcher**: `window.__signoVivoReceiveNativeEvent = applyNativeSyncEvent` (app.js:1004; body :939-1002). Whole-body try/catch (M2 Slice C) — a throw would propagate back into `evaluateJavaScript`. Payload types:
  - `bridge-state` → `state.nativeBridgeAvailable` (:949-952)
  - `set-book` → **no-op** (legacy shell compat, :955)
  - `role` → `state.syncRole` + `state.nativeSyncRole` (`"none"`→`"off"`) + `renderDirectorModeBadge()` (:959-968)
  - `relay-auth-error` → `showRelayAuthWarning(status)` (:974-977)
  - `sync-event` → `event.type === "state"` drives the sync spinner; `event.type === "page"` (finite) → `renderPage(page, {pushToHistory:false})`; `event.book` ignored (:979-992)
- **Outbound** `postNativeBridge` (:309-321) — JSON on channel `"signovivo-native"` via `window.ReactNativeWebView.postMessage`. All 6 send sites:
  1. `page-changed` {page, totalPages, book} — every successful render (:1074)
  2. `render-failed` {page, book} — render error path (:1105)
  3. `director-code` {code} — 5-10 digit numpad entry (:1196)
  4. `exit-director` — director badge tap + confirm (:2441)
  5. `resync` — ⟳ tap in native mode (:3160)
  6. `bridge-ready` {page, totalPages, book} — end of initReader (:3535)

### 4.6 Fleet check-in + crash telemetry (web only)
- `fleetCheckin(extra)` (:2966-3004): POST `RELAY_BASE + /fleet/checkin` with `{deviceId, label, role, surface:"web", webCached, pagesCached, totalPages, homeScreen, cacheVersion}`. Never sends a phone number. Skipped in native mode; in-flight-deduped; failures swallowed. Called at boot (:3046-3057), after full precache (:607), and on picker save/skip (:3035, 3042).
- `countCachedPageImages` (:2948-2963): max page count across ALL caches whose name includes "pages" (old versions count — pages are immutable).
- Crash reporter `reportCrash` (:2908-2938): POST `RELAY_BASE + /log` `[{kind:"crash", dev, surface, build, where, msg≤300, stack≤600, url: pathname-only (never query — `?k=` dashboard secret must not leak), t}]`, `keepalive:true`; 30s signature de-dup + hard 20/session cap. Registered as `window.__svReportCrash` and flushes a pre-registration `__SV_LAST_ERROR` once (:2941-2945).

---

## 5. Contracts / invariants worth pinning

1. **Relay snapshot wire shape** consumed by web: `{ page:number, seq:number>0, ts:epoch-s (server publish), now:epoch-s (server clock, P2), bookId (vestigial) }`. Missing `ts` = treated fresh; missing `now` = keep previous clock offset (never regress vs old worker).
2. **Freshness-before-seq** is the load-bearing ordering (svSyncDecision.js:87-101) and it lives in node-tested code; `applyRelaySnapshot` must stay a mechanical executor (app.js:3179 comment).
3. `lastSeq` resets to −1 in exactly three places: demote (F4, lib), manual ⟳ (app.js:3167), and the inline fallback's demote (:3189).
4. Only ONE reconnect timer may ever be scheduled (F5, :3287, 3387-3388); heartbeats are strictly per-socket (`ws.__svHeartbeat`, :3339-3362, 3372-3376).
5. `relay.browsing` is set ONLY by a numpad jump off the live page (:1212-1217); every other manual navigation (drawer, swipe, arrows) leaves `following=true` and is re-homed by F1 within ~4s (:3357-3359).
6. Page images are immutable by filename; the SW's cross-version fallback + browser `immutable` header + `ignoreSearch` all depend on that (build.mjs:706-713, sw.js:6-14, 169-198).
7. All module-eval storage access must be try-guarded (white-screen class of failure; :2-6, 384-386, 394-401).
8. `?env=staging` / `?selftest` must survive `clearInitialUrl` (:669-678) — a mid-canary reload must not silently switch to the Mass room.
9. Native/web split: web relay code is dead in native mode at exactly three gates — `startRelayFollow` (:3395), boot peek (:3512), `reconnectRelay` (:3159); SW registration off in file mode (:2074); precache off in file mode (:622).
10. `renderPage` must never receive a non-integer (clampPage :274-279), and the live `<img>` src is committed only after the requestId guard (:1030-1039 comment).

### Persisted keys (complete)
| Store | Key | Purpose | Where |
|---|---|---|---|
| localStorage | `nc-sort-prefs` | index sort prefs | app.js:133-140 |
| localStorage | `sv-haptic` | haptics on/off | :381-390 |
| localStorage | `sv-tip` | numpad tip dismissed | :393-401 |
| localStorage | `sv-recientes` | last 15 song numbers | :404-418 |
| localStorage | `sv-offline-ready-<CACHE_VERSION>` | full-bundle flag (version-scoped) | :223, 606, 646, 2977 |
| localStorage | `svFleetLabel` / `svFleetRole` / `svFleetDeviceId` / `svFleetSkip` | fleet identity | :2877-2880 |
| sessionStorage | `sv-sw-reload-pending` | this-tab-requested SW activation → reload | :216, 2023, 2089-2094 |
| IndexedDB | `signo-vivo-offline` → `bundle-status`/`current` | {version, totalPages, verifiedAt} | :224-226, 333-378 |
| CacheStorage | `signo-vivo-static-<v>`, `signo-vivo-pages-<v>` | shell + pages | :221-222, sw.js:2-3 |

---

## 6. Vestigial remains of the deleted two-book system

- `#geo-gate` id + `geo-gate-*` CSS family, incl. never-applied `.is-offline` state (index.html:44; styles.css:66-119; app.js:57).
- `books.json`: still built (build.mjs:678-686), still inlined as `#books-data` (build.mjs:698), still in SHELL_ASSETS for caching (app.js:241) — **never read by any JS**.
- `state.currentBook` (always "standard", :176, 189) and the `book` field on every bridge message (:1078, 1108, 3539).
- `snap.bookId` on the relay wire — build-373 compat only (:3212-3214).
- Bridge `set-book` inbound no-op (:955).
- `updateBookLabel()` → `html[data-book="standard"]` for "any book-scoped CSS" (:1114-1116) — no such CSS exists in styles.css.
- `flashSongDisplay` err/ok styling incl. `is-ok` green path (:830-841; styles.css:2226-2241) — the "ok" variant (old unlock success) has no caller; only `is-err` is used (:1201).
- Drawer numpad panel + mode switch retired via CSS but fully wired in JS (`switchDrawerMode("numpad")` reachable only through a hidden button, :2472-2475; styles.css:2328).
- Hidden stub buttons `#fullscreen-button/#prev-corner/#help-button/#display-clear/#search-cancel` (index.html:207-212, 275) and null `#search-index`.
- sw.js comment still references the retired `signovivo.com?admin=1` preload flow (sw.js:91).

---

## 7. Oddities (smells for later audit lenses)

- **O1 — dead inline search-index path**: `loadSearchIndex` first checks `#search-data` (app.js:1228) but build.mjs never emits that tag (only `#books-data` + `#pages-data`, build.mjs:697-699). The inline branch is unreachable; search always fetches. Harmless, but the "offline / ?admin build" comment at app.js:3480-3481 describes a build that no longer exists.
- **O2 — `#books-data` produced but never consumed** (and `/books.json` cached for nothing) — pure vestige, see §6.
- **O3 — the entire offline-gate surface is dead code**: `setOfflineGateState` is called exactly once, with `visible:false` (app.js:3507). Consequently `LOADING_PHRASES` + phrase timer (:468-503), the `showAdminNote` param (destructured at :511, never used in the body), `#offline-admin-note`, `#offline-test-button` (no JS refs at all), and `isOfflineBundleReady` (:641-666 — zero callers) are all unreachable. The retry/continue click handlers (:2377-2383) are bound to UI that can never show. `readOfflineMetadata` is now only written, never meaningfully read.
- **O4 — help panel is unreachable on web**: its only opener `#help-button` is a `display:none` stub (index.html:210). The help instructions, the **haptic toggle**, and the "Versión N" label (app.js:3548-3550) are all invisible dead UI. (The haptic *pref* still works if it was ever set; there is just no UI to change it.) Also, several help texts describe retired UI ("franja oscura a la izquierda" = the hidden drawer handle; "escribe el número y toca ↵ Ir" = the retired drawer numpad).
- **O5 — `relay.appliedPage` is write-only** (writes at :3144, 3194, 3227; zero reads). Drift detection uses `livePage` vs `state.currentPage` instead.
- **O6 — near-duplicate role state**: `state.syncRole` (:177, written :961) vs `state.nativeSyncRole` (:179) — only the latter drives anything.
- **O7 — follower drawer navigation fights the director**: the drawer (edge-swipe) and swipe/arrow navigation do NOT set `relay.browsing`, so a live follower who browses that way is yanked back to the director's page within one 4s heartbeat (F1, :3357-3359). Only the ♪ numpad jump grants "browse peace" + the go-live bar. Deliberate per the F1 comment, but it makes the drawer a trap for a live follower — worth a UX/consistency lens.
- **O8 — `preloadImage` timeout counts as success**: a 3s-slow page resolves "timeout" and the src is committed anyway (:1023, 1071-1073) — the error/`render-failed` path only fires on a genuine decode error, not slowness. Intentional-looking, but it means "No se pudo cargar esta página" almost never shows on slow networks while the img may still be blank momentarily.
- **O9 — input-model inconsistency**: hardware arrow keys move by SONG (:2835-2836) while swipes move by PAGE (:2745-2749). Fine on touch iPads; surprising on desktop.
- **O10 — `NETWORK_FIRST_PATHS` misnomer** in sw.js (:32) — the handler is stale-while-revalidate (:204-224). Naming only, but it invites a wrong mental model during incident debugging.
- **O11 — stale size comments**: deferOfflinePrecache says "~13 MB" (app.js:615), build.mjs says ~28 MB at current settings (build.mjs:107); sw.js activate comment says "~34 MB / 370 pages" (sw.js:88-91). Nobody has reconciled these after the 371-page/115-DPI settle.
- **O12 — 4-digit song numbers silently resolve**: the code gate is ≥5 digits (:1190), so e.g. "9999" is treated as a song and lands on the LAST page via the `next ?? totalPages` fallback (:707-708) rather than erroring.
- **O13 — `data-role` unset at web boot**: `renderDirectorModeBadge` (the only writer of `html[data-role]`) never runs on pure web unless a native `role` event arrives (impossible on web). The follower layout works only because the base CSS is the follower layout; the comment at :849 ("Default 'follower' so signovivo.com is right from boot") overstates what the code does. Any future CSS keyed on `html[data-role="follower"]` would silently not apply on web.
- **O14 — `countCachedPageImages` counts foreign versions**: max across every cache whose name merely *contains* "pages" (:2955) — fine today (immutable pages), but a future non-page cache named "…pages…" would inflate fleet `pagesCached`.
- **O15 — boot-relay race can double-render**: initReader's raced `relayPollOnce` (1500ms, :3513) can resolve AFTER the timeout loses the race; `renderPage(2)` (:3515) and a late `follow`-decision `renderPage(director)` then both run. The requestId guard makes the last one win, so it self-heals — but page 2 can flash before snapping to the director.
- **O16 — heartbeat send-failure is silent**: `try { ws.send("ping") } catch {}` (:3351) — a throwing send (socket dying mid-frame) is swallowed; recovery then waits on the 12s silence window or the 10s F3 tick rather than reacting immediately. Consistent with the defensive style, just slower than it could be.

---

*End of map. Everything above cites current source; the atlas (docs/app-atlas.md) and contracts (docs/app-contracts.md) remain stale for this subsystem.*
