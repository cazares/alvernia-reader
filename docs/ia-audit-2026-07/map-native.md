> ⚠️ **CORRECTION BANNER (2026-07-09).** This map was written at HEAD 16244b25 / build 377. The branch has since been fast-forwarded to build 381 (d5075091). Landed since this map: **#269** capped the "Buscando director…" spinner so it never looks stuck; **#270 REMOVED the "¿Quién usa este iPad?" fleet self-ID modal entirely** (fleet check-in itself remains); **#271** simplified the sync spinner and renamed the songbook PDF (assets/alvernia_manual_2.pdf → assets/signo_vivo_371.pdf), touching web/src/app.js (−112 lines area), index.html, styles.css. Where this map contradicts current source, CURRENT SOURCE WINS — do not report the removed modal or old spinner behavior as findings.

# Native Subsystem Map — SignoVivo (build 377, HEAD 16244b25)

Cartographer: "native". All line numbers verified against CURRENT source at HEAD 16244b25.
Repo root: `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a`

Files covered:
- `PdfReaderApp.tsx` (1123 lines — the entire native shell)
- `src/nearbyDirectorSync.js` (104) — JS wrapper over the Swift Multipeer module
- `src/directorRelaySync.js` (129) — Cloudflare relay transmitter
- `src/offlineBooks.ts` (24) — BookId type + AsyncStorage key names
- `src/pdfReaderUrl.js` (31), `src/songNavigation.js` (43) — **dead code, no importers** (see Oddities)
- `ios/SignoVivo/DirectorSyncModule.swift` (1914) — Multipeer mesh + bundle distribution
- `ios/SignoVivo/DirectorSyncModuleBridge.m` (55) — RCT_EXTERN_MODULE declarations

---

## 1. Architecture

The native app is a thin WKWebView shell (PdfReaderApp.tsx:1-16). All reader UI lives in the web bundle (`web/src/`, built into `WebBundle/`), loaded from `file://`. Native's five jobs (PdfReaderApp.tsx:5-13):

1. Serve the bundled web app offline, preferring a peer-pushed update in `Documents/WebBundle` over the shipped bundle (`resolveBundleUri`, PdfReaderApp.tsx:813-826).
2. Bridge the iPad↔iPad Multipeer mesh ↔ web app.
3. Validate director codes entered on the web numpad; drive director/follower role.
4. Publish the director's page to the Cloudflare relay (`signovivo-sync.4j4982y8jp.workers.dev`, room `alvernia-main`) for signovivo.com followers.
5. Keep screen awake — via `useKeepAwake("signovivo-reader")` (PdfReaderApp.tsx:86), NOT the Swift `setIdleTimerDisabled` (which is exposed but never called — see Oddities).

Single-book invariants: `BookId = "standard"` only (src/offlineBooks.ts:9); `isBookId` accepts only "standard" (PdfReaderApp.tsx:78); `modeForBook` always returns "standard" (PdfReaderApp.tsx:81); relay payload hardcodes `bookId: "standard"` (src/directorRelaySync.js:120); mesh receive hardcodes book "standard" ignoring incoming bookId (PdfReaderApp.tsx:890). bookId/mode remain wire fields for backward compat only.

### Key constants
| Constant | Value | Anchor |
|---|---|---|
| RELAY_BASE | signovivo-sync.4j4982y8jp.workers.dev | PdfReaderApp.tsx:45, directorRelaySync.js:12 |
| RELAY_ROOM | alvernia-main | directorRelaySync.js:13 |
| DIRECTOR_SESSION (mesh session code) | "1234" fixed | PdfReaderApp.tsx:48 |
| STANDARD_DIRECTOR_CODES / SUPER_ADMIN_CODES | baked from gitignored director-codes.private.json into director-codes.json | PdfReaderApp.tsx:53-65 |
| SOFT_RESET_CODE | "744668486" | PdfReaderApp.tsx:67 |
| LIVE_DIRECTOR_WINDOW_MS | 8000 | PdfReaderApp.tsx:71 |
| Mesh heartbeat / relay heartbeat | 1s / 12s | PdfReaderApp.tsx:393-400 / 401-412 |
| Bridge watchdog | 6s, max 2 remounts, then native fallback | PdfReaderApp.tsx:307-321 |
| Relay publish timeout | 7000ms AbortController | directorRelaySync.js:50 |
| MPC serviceType | "signovivo", hgen="2" | DirectorSyncModule.swift:9,16 |
| Discovery refresh | 25s normal, 5s×6 early burst, 60s when thermally hot | DirectorSyncModule.swift:19-23, 1110-1114 |
| Invite timeout / hello / half-open watchdog | 30s / 8s / stale>3s checked every 1s | DirectorSyncModule.swift:24,26,33-34 |
| Snapshot probe / self-directed timeout | 1.5s / 10s | DirectorSyncModule.swift:40,42 |
| Mesh capacity | 7 followers × 2 sessions = 14 | DirectorSyncModule.swift:43-47 |
| Bundle-transfer watchdog | 90s | DirectorSyncModule.swift:111 |

---

## 2. Startup sequence

1. `useKeepAwake` (PdfReaderApp.tsx:86).
2. Device-id effect (PdfReaderApp.tsx:203-253): reads/creates `sv_devid`, logs `boot` to relay `/log`, one-time `Alert.prompt` "¿Quién usa este iPad?" for the fleet label (iOS only, skippable → `sv_fleet_skip`), then `fleetCheckin()`. Re-check-in every 90s (PdfReaderApp.tsx:256-259).
3. Boot effect (PdfReaderApp.tsx:829-846): breadcrumb "boot", pins book "standard", resolves bundle URI (Documents/WebBundle/index.html if exists, else `bundleDirectory/WebBundle/index.html`), `setBooted(true)`.
4. Mesh bootstrap effect (PdfReaderApp.tsx:849-985), only if `syncAvailable` (iOS + native module present, nearbyDirectorSync.js:6): `primeNearbyPermissions()` (:851 — Swift NWBrowser + MPC priming for the Local Network prompt, DirectorSyncModule.swift:530-581); one-shot role restore guarded by `didBootstrapRef` (:857): reads `STORAGE_KEYS.lastSyncRole`; if it was "director", shows the **"Estabas dirigiendo"** alert (NEW-DIR-1, :869-878 — re-enter your code prompt; codes are never persisted so no auto-resume); then **always** `becomeFollower()` (:881). Registers the `DirectorSyncEvent` listener (:885-968).
5. WebView mount arms the 6s bridge watchdog (mount effect :325-335); web posts `bridge-ready` when booted.

**Watchdog escalation (Slice B):** no `bridge-ready` in 6s → breadcrumb `bridge-timeout:N`, up to 2 remounts (`setMountKey`), then `setWebDead(true)` → native "Reintentar" screen (PdfReaderApp.tsx:307-321, 1051-1073). `bridge-ready` clears the timer and resets the remount budget (:642-646). `onContentProcessDidTerminate` (:1089-1095) reloads + re-arms the watchdog; `onError` only breadcrumbs (:1088).

**WebView setup** (PdfReaderApp.tsx:1078-1108): `file://` source, `originWhitelist=["*"]`, allowFileAccess/FromFileURLs/UniversalAccess, `injectedJavaScriptBeforeContentLoaded` = preloadScript (:1033-1042) setting globals `window.__SIGNO_VINO_NATIVE_FILE_MODE=true`, `__SIGNO_VINO_NATIVE_BUNDLE_VERSION`, `__SIGNO_VINO_INITIAL_BOOK` (note "VINO" spelling; INITIAL_BOOK is never read by the web — see Oddities). `bounces={false}`, no link preview, single window, text interaction off.

---

## 3. Bridge contract (web ↔ native)

Injection: `injectEvent` (PdfReaderApp.tsx:270-287) calls `window.__signoVivoReceiveNativeEvent(payload)` (web handler registered at web/src/app.js:1004); queued until `bridge-ready`, queue capped at 100 with oldest-drop (:265-268); flushed at :289-300.

### Web → native (`onMessage`, router PdfReaderApp.tsx:627-810; must have `channel:"signovivo-native"` :635)
| type | Sender (web) | Native handling |
|---|---|---|
| `bridge-ready` | web/src/app.js:3536 | :638-697. Disarm watchdog, reset remounts. A3: director/transmitter does NOT adopt web's boot page — re-asserts own `currentPageRef` via a `sync-event` page inject and re-broadcasts (:652-678). Re-asserts role ("director" for transmitter-only too, "none" when off, :663-669). A reloaded FOLLOWER with a stored director snapshot resyncs to it + `requestCurrentSnapshot()` (H1, :679-695). Fresh-boot follower keeps web's own page (null-guard). |
| `page-changed` | web/src/app.js:1075 | :698-723. Director/transmitter ignores pre-`bridge-ready` boot renders (:704). Clamps page ≥1 and ≤totalPages (:709-710). Persists `sv.book.lastPage.standard` (:717-720). Calls `broadcastPage`. |
| `director-code` | web/src/app.js:1196 | :724 → `onDirectorCode` (see §4). |
| `resync` (follower ⟳) | web/src/app.js:3160 | :727-753. If stranded "off" + mesh available → `becomeFollower()`. Non-director: `refreshNearbyDiscovery()` + `requestCurrentSnapshot()`, re-asserts last director snapshot as one atomic page sync-event. |
| `exit-director` | web/src/app.js:2441 | :754-776. Mesh device → `becomeFollower()` (rejoins mesh; persists lastSyncRole="follower" so the crash prompt won't fire). Transmitter-only → drop to "off": bump generation, stop heartbeat, clear transmitter flag, inject role "none" (:769-773). |
| `render-failed` | web/src/app.js:1106 | :777-795. FOLLOWER ONLY: set `currentPageRef=-1` sentinel so the heartbeat de-dupe re-fires. Never on a broadcaster (would publish page 1 to everyone). |

### Native → web (injected payloads)
| payload | Emitted at | Web handling |
|---|---|---|
| `{type:"bridge-state", available}` | PdfReaderApp.tsx:658 | web/src/app.js:949 |
| `{type:"role", role:"director"\|"follower"\|"none"}` | :449, :473, :508, :525, :573, :586, :669, :773 | drives html[data-role] UI |
| `{type:"sync-event", event:{type:"page", page, book}}` | :674-677, :690, :749, :911, :1008 | render page (book always carried; atomic switch) |
| `{type:"sync-event", event:{type:"state", status, role, message}}` | :921-929 | follower status UI |
| `{type:"relay-auth-error", status}` | :344 (handler set :342-347) | web/src/app.js:974 — director-visible warning |

**Not forwarded to web / dropped at default:** Swift events `memoryWarning`, `bundle-error`, `takeover-approved`, `takeover-denied` fall through the JS listener's `default: break` (PdfReaderApp.tsx:965-966). See Oddities.

---

## 4. Director/follower role state machine (JS side)

State: `roleRef` ∈ off|director|follower (:102), `explicitTransmitterRef` (transmitter-only director on a mesh-less device, :107), `roleGenerationRef` (:106 — every committed role transition bumps it; every `await` inside become* re-checks and bails if superseded).

**Entry (director code):** web numpad → `director-code` → `onDirectorCode` (PdfReaderApp.tsx:563-624). digitsOnly; empty → role "none"; `744668486` → `performSoftReset`; not in STANDARD_DIRECTOR_CODES → role "none" ("código incorrecto" on web). Valid code → **always a confirm Alert** (never silent promotion): title varies — "⚠️ Ya hay un director activo" when a mesh director snapshot arrived within 8s (NEW-DIR-3 liveness window :598-602), "Super admin — ¿dirigir?" for SUPER_ADMIN_CODES, else "¿Dirigir el coro?" (:603-621). Cancel = stay exactly as-is (deliberately no generation bump, NEW-DIR-2 :563-570). Confirm → `becomeDirector(code)`.

**becomeDirector** (:453-538): bump generation; `setRelayPublishCode(code)` FIRST (:459).
- No mesh (`!syncAvailable`): transmitter path (:461-476) — `explicitTransmitterRef=true`, roleRef stays "off", **persists lastSyncRole="director"** (H3, PR #267, :472 — non-credential breadcrumb so a native restart fires the resume prompt), inject role "director", broadcast, start heartbeats.
- Mesh: if currently a CONNECTED follower, drop the link first via `resetNearbyDirectorSync()` (:482-491) so Swift's DIRECTOR_TAKEOVER_REQUIRED guard passes; `startNearbyDirector("1234")` with one 2s-delayed retry (:493-501); on success set roleRef="director", persist lastSyncRole="director" (:506), inject role, broadcast, start heartbeats, kick `refreshNearbyDiscovery()` (split-brain fast converge :514), breadcrumb "director". On failure: ex-follower → `becomeFollower()` (re-join, :523); otherwise inject role "none" (:525).

**becomeFollower** (:419-450): bump generation, roleRef="follower", clear transmitter flag, stop heartbeats, **`setRelayPublishCode("")`** (C3 — an in-flight coalesced publish drains with no code → 401s instead of shoving a stale page, :430), persist lastSyncRole="follower", `startNearbyFollower("1234")` with one 2s retry, inject role "follower".

**Exit:** `exit-director` (§3). **Demotion:** mesh `error` event DIRECTOR_CONFLICT (:932-944) → stop heartbeat, `becomeFollower()`, then C2: `requestCurrentSnapshot()` + `refreshNearbyDiscovery()` so the demoted device re-homes to the winner's page immediately.

**Takeover request/deny:** web has no approve path; a native director **auto-denies** any `takeover-request` (v1, :946-952 → `denyDirectorTakeover`). Force-takeover happens instead via startDirector + Swift token conflict (newest token wins). `requestDirectorTakeover`/`approveDirectorTakeover` JS wrappers (nearbyDirectorSync.js:75-89) are exported but never called by the app.

**Soft reset** (:541-560): breadcrumb, bump generation, stop heartbeats, `resetNearbyDirectorSync()`, remove lastSyncRole, roleRef="off", clear transmitter flag, clear inject queue, remount WebView. NOTE: leaves the device in "off" until the user does something — the `resync` handler's "stranded off" rescue (:733) re-joins on ⟳.

**Persistence/restore:** only `sv.sync.lastRole` matters (offlineBooks.ts:19). Boot NEVER auto-restores director (Miguel 2026-07-02 "always ask"); it shows the NEW-DIR-1 alert and boots follower (:869-882). Codes are never stored.

---

## 5. Broadcast paths

`broadcastPage(rawPage, book)` (PdfReaderApp.tsx:350-369) is the single choke point: H4 floors non-finite/<1 pages to 1 (:355). Mesh send if roleRef==="director" (:358-361); relay publish if director OR transmitter (:362-368).

**Heartbeats** (:371-413): mesh re-send every **1s** (guard roleRef==="director"); relay keepalive every **12s** (guard director||transmitter) — page changes publish immediately, the 12s tick only refreshes snapshot freshness (`RELAY_LIVE_MAX_AGE_S=90` on the worker). Started in both becomeDirector paths, stopped on becomeFollower/soft-reset/exit/unmount (:976).

**Relay transmitter** (src/directorRelaySync.js): payload `{v:1, page≥1, totalPages≥0, mode, bookId:"standard", seq, ts}` (:111-123); `seq = max(seq+1, Date.now())` monotonic across restarts (:56-60); latest-wins coalescing — one request in flight, newest pending replaces (:124-128, drain :103-107); 7s AbortController ceiling so a black-holed captive-portal fetch can't wedge the coalescer (:50, :62-71); auth failure (401/403) fires the one-shot latched `relayAuthErrorHandler` (:87-96), re-armed on success or on a new `setRelayPublishCode` (:36-41); native forwards it into the web as `relay-auth-error` (PdfReaderApp.tsx:342-347).

**Foreground** (AppState effect, PdfReaderApp.tsx:993-1019): follower → `refreshNearbyDiscovery` + `requestCurrentSnapshot` + immediately re-assert last snapshot; director → re-broadcast; transmitter-only → re-publish to relay. Listener always registered (transmitter can appear after mount, :987-992).

---

## 6. Swift Multipeer module (DirectorSyncModule)

### JS-facing API (bridge .m lines 6-52; Swift impls)
| Method | Swift anchor | Notes |
|---|---|---|
| startDirector(sessionCode) | DirectorSyncModule.swift:353-386 | Rejects DIRECTOR_SESSION_INVALID; rejects **DIRECTOR_TAKEOVER_REQUIRED** if currently a connected follower (:365-368). resetTransport → role "director", mints token (:372), advertise+browse+refresh timer, `broadcastDirectorAnnounce`, emits state "advertising". |
| startFollower(sessionCode) | :388-411 | resetTransport → role "follower", advertise+browse, self-directed timer, emits "searching". |
| stop / resetForAppReset | :413-433 | Both resetTransport(emitState:true). |
| sendPageUpdate(page,totalPages,mode,bookId) | :647-697 | Director-only (DIRECTOR_ROLE_INVALID). Stores snapshot state ALWAYS (late joiners); no peers → emits "waiting-followers". Sends reliable per session, unreliable fallback; resolves deliveredPeers. |
| primePermissions | :530-581 | NWBrowser (the reliable iOS14+ Local Network prompt trigger) + throwaway MPC browser/advertiser, auto-torn-down after 6s. |
| refreshNearbyDiscovery | :586-604 | Re-arms 5s early burst, refreshDiscovery, restarts self-directed countdown. |
| requestCurrentSnapshot | :606-617 | forceFollowerHelloNow (hello → director replies with snapshot). |
| requestDirectorTakeover / approveDirectorTakeover / denyDirectorTakeover | :435-520 | approve demotes the approving director to follower (:481-490). JS only ever calls deny (auto-deny). |
| setIdleTimerDisabled | :621-631 | **Never called from JS.** |
| getDeviceName | :633-645 | **Never called from JS.** |

### Emitted events (all on `DirectorSyncEvent`)
- `state` {role, sessionCode, status, peerCount, directorCount, message} — deduped on (status, peerCount) unless a message is present (:1410-1429). Statuses: `advertising`, `searching`, `connecting`, `connected`, `waiting-followers`, `resolving-conflict` (:1483), `self-directed` (:1213), `idle` (:1405).
- `page` {page, totalPages, mode, bookId, sessionCode} (:1438-1443) — follower-side only, and ONLY from the connected director peer (:1859-1865).
- `error` {code, message, role, sessionCode} (:1431-1436) — codes: DIRECTOR_CONFLICT (:1546), DIRECTOR_START_FAILED (:1615), FOLLOWER_START_FAILED (:1690), BUNDLE_PACK_FAILED (:763), BUNDLE_SEND_FAILED (:783), BUNDLE_RECEIVE_FAILED (:1892).
- `takeover-request` {requestId, requesterName} (:1445-1453); `takeover-approved`/`takeover-denied` (:1455-1462).
- `bundleUpdated` {version} (:1043-1048) → JS remounts WebView from Documents (PdfReaderApp.tsx:954-963).
- `bundle-error` {stage} (many stages: timeout/pack/send/receive/receive-nil/docs/mkdir/open/header-*/archive-size-mismatch/file-*/index-too-small/swap-*) — **dropped by JS**.
- `memoryWarning` (:271-278) — **dropped by JS**.

### Mesh lifecycle & hardening (the M-F/H fixes)
- Discovery: advertiser carries discoveryInfo {session, role, hgen:"2", token (director)} (:1072-1085). Handshake gen 2 = follower self-invites; director only invites legacy (≤226, no hgen) followers (:1652-1663). Legacy director detection requires non-empty info without hgen (:1516-1526).
- Refresh loop (:1105-1195): early burst 5s×6 → 25s steady → 60s when thermal serious/critical; skipped while backgrounded. **Handshake protection**: never tear down transports mid-invite (:1139-1144). **Director hold-serving**: never churn transports while connected/discovered followers exist — but still re-announce token + run conflict tiebreak each cycle (:1154-1167, periodic split-brain reconvergence :1176-1182). Idle follower re-runs target selection each cycle (:1183-1190). Stale discovered directors pruned after 90s (:1349-1361).
- Follower connect (:1714-1729): set connectedDirectorPeer, cancel self-directed, pause discovery refresh, start hello timer (8s) + half-open watchdog, send hello, schedule 1.5s snapshot probe (:1338-1347), and **M-F1**: prime `lastFollowerPageReceivedAt` at connect so the watchdog is armed pre-first-page (:1723-1729). Director's announce also counts as a liveness beat (M-F1 belt-and-suspenders, :1795-1801).
- Half-open watchdog (:1246-1262): stale >3s → `forceFollowerReconnect` (:1274-1295): recreate MCSession, clear pointers, emit "searching – Reconectando…", **M-F5** immediate re-scan + fast burst + reconsiderFollowerTarget.
- Follower target selection (:1466-1541): picks the HIGHEST director token (the split-brain winner) with displayName tiebreak; single pending invite with 30s staleness retry; modern directors get self-invited via `browser.invitePeer`.
- Split-brain: token = 20-digit µs timestamp + UUID suffix (:1555-1562); `handleDirectorConflict` demotes the strictly-lower token via resetTransport + DIRECTOR_CONFLICT error (:1543-1549); announce broadcast on startDirector (:382), per-peer at .connected (:1741), every refresh cycle (:1160, :1177), and on foreground (:339). A director rejects invites from known directors (:1583-1594).
- Advertiser/browser launch failure (**M-F7**): exponential backoff 3s→30s for 5 failures, then 45s retry FOREVER (:1611-1631, :1686-1704). On foreground, counters >5 are reset and transports relaunched once (:302-311).
- Background/foreground (:284-341): background stops discovery/hello/watchdog timers but keeps sessions; foreground re-arms per role, and a director proactively re-pushes the snapshot to every connected peer + re-announces (:328-340).
- Capacity: 2 MCSessions × 7 followers (:43-47); sessions-full invite rejected (:1600-1601).
- Inbound payload guard: ≤8KB, JSON dict, protocol v must be 0 or 1 (:153-158, :1785-1786).
- **Security posture:** `encryptionPreference: .none` (:125, :1060, :1278) and `certificateHandler(true)` (:1910-1912) — unencrypted, unauthenticated mesh; session code fixed "1234".

### Web-bundle distribution (director → follower OTA over mesh)
Offer sent at .connected (:1743) with the director's CFBundleVersion (:97-99); follower requests iff offered > mine numerically and no transfer in flight (:717-730); director packs `WebBundle` into a length-prefixed archive off-main (:753-871, throwing FileHandle APIs to avoid uncatchable ObjC exceptions :851-869); follower receives via sendResource, installs with defense-in-depth (:879-1049): 4MB header bound (:926), whole-archive size check (:943-958), path-traversal rejection (:967-969), per-file size verification (:1004-1009), index.html >200 bytes floor (:1015-1019), atomic-ish swap with rollback (:1021-1041), then emits `bundleUpdated` → JS remounts pointing at Documents/WebBundle. 90s watchdog force-clears a stuck in-flight flag (:737-748).

---

## 7. User-visible surfaces rendered by NATIVE itself

1. **Fleet label prompt** — `Alert.prompt("¿Quién usa este iPad?", …)` once per install, "Ahora no"/"Guardar" (PdfReaderApp.tsx:220-245).
2. **Director confirm alert** — three variants incl. destructive "Tomar el control" (PdfReaderApp.tsx:603-621).
3. **"Estabas dirigiendo" boot alert** after a crash while directing (NEW-DIR-1, PdfReaderApp.tsx:871-878).
4. **Native dead-WebView fallback** — "Signo Vivo se está recuperando" + "Reintentar" button (PdfReaderApp.tsx:1051-1073, styles :1117-1122).
5. **Blank black view** pre-boot (:1044-1046). StatusBar hidden everywhere.
Everything else (banners, spinners, ⟳, numpad, status pills) is web UI driven by injected role/state events.

## 8. Telemetry & fleet
- JS `/log`: batched ~1s, `{t,dev(sv_devid),role,build,event,...}` (PdfReaderApp.tsx:139-177). Events: boot, become:follower, become:director, page:send, mesh:page-recv, mesh:state, mesh:error.
- Swift `/log`: per-event immediate POST, `dev` = real MPC displayName, `src:"swift"` (DirectorSyncModule.swift:174-191). Events: found, lost, invite:send/recv/accept/reject, session:connected/connecting/notConnected, refresh:hold-connecting/hold-serving, watchdog:half-open-reconnect.
- Fleet check-in: POST `/fleet/checkin` {deviceId, surface:"native", nativeBuild, label, role:"Director"|""} on boot + every 90s (PdfReaderApp.tsx:182-201, 256-259).

## 9. Persisted keys (AsyncStorage)
| Key | Written | Read |
|---|---|---|
| `sv_bc` (breadcrumb) | :133 | (forensics only; never read in-app) |
| `sv_devid` | :209 | :206 |
| `sv_fleet_label` / `sv_fleet_skip` | :238 / :230 | :218-219 |
| `sv.sync.lastRole` | follower :432, director :472/:506, removed on soft-reset :551 | boot restore :869 |
| `sv.book.lastPage.standard` | :717-720 | **never read by native** (web owns page restore) |
| `sv.sync.lastDirectorAt`, `sv.onboarding.*`, `sv.standard.accessName`, `sv.mode`, `sv.book.active` | declared in offlineBooks.ts:11-24 | **vestigial — never touched anywhere** |

---

## 10. Contracts / invariants (the load-bearing rules)

1. **Never broadcast the boot page.** A director/transmitter ignores pre-bridge-ready `page-changed` (:704) and re-asserts `currentPageRef` at bridge-ready (:652-678). (A3 — the "yank the congregation" class.)
2. **Never broadcast page <1.** Choke-point floor (:355); render-failed sentinel −1 is follower-only (:792-794). (H4)
3. **Valid director code never promotes silently** — Alert always (:589, :613). No auto-director at boot, ever (:859-862).
4. **Credentials never persist.** Only the role string persists; H3 persists "director" for transmitter-only so a native restart surfaces the resume prompt (:466-472).
5. **Role transitions are generation-guarded**; every await in become* re-checks `roleGenerationRef` (:421, :433, :441, :448, :455, :490, :499, :502, :507, :523). Code entry itself does NOT bump (NEW-DIR-2, :564-570).
6. **Step-down clears the relay code** so a draining coalesced publish 401s (C3, :426-430); a fresh success or code re-entry re-arms the one-shot auth warning (directorRelaySync.js:36-41, :88).
7. **One atomic page sync-event** (page carries book) — never set-book then page (:686-690, :744-749, :905-911, :1002-1008).
8. **Follower only honors pages from its connected director peer** (Swift :1859-1865); JS director ignores its own mesh echoes (:889).
9. **Snapshot is never sent with an empty book** — Swift withholds until a real page exists (:222-239).
10. **Higher token wins; approve-takeover demotes self; JS auto-denies takeover requests** (v1) (:946-952, Swift :1543-1549).
11. Bootstrap runs exactly once per session (`didBootstrapRef`, :126, :857).
12. Web bundle preference: Documents/WebBundle (peer-pushed) over shipped bundle (:813-826); install validates before swap and rolls back on failure.

---

## 11. Oddities (smells for later audit lenses)

1. **`__SIGNO_VINO_*` misspelling** ("VINO" not "VIVO") in all three injected globals (PdfReaderApp.tsx:1036-1038) — consistent with the web side (web/src/app.js:227, 2918, 3546) so it works, but it's a trap for future greps.
2. **`__SIGNO_VINO_INITIAL_BOOK` is injected but never read** by web/src/app.js — dead global (PdfReaderApp.tsx:1038).
3. **Dead files:** `src/pdfReaderUrl.js` and `src/songNavigation.js` have zero importers anywhere (checked repo-wide incl. tests/web/scripts) — leftovers from the pre-WebView native reader.
4. **Dead Swift API:** `setIdleTimerDisabled` (DirectorSyncModule.swift:621) and `getDeviceName` (:633) are bridged (.m:14-15, 39-41) but never called from JS. Idle-lock is expo useKeepAwake instead — MPC's "screen lock throttles sync" concern (:620) is only covered while the RN keep-awake holds.
5. **Silently dropped native events:** `bundle-error` (all ~15 stages), `memoryWarning`, `takeover-approved`, `takeover-denied` hit `default: break` in the JS listener (PdfReaderApp.tsx:965-966). A follower whose mesh OTA install fails gets NO surface at all; memory pressure is never acted upon (e.g., no cache trim / breadcrumb).
6. **Mesh is unencrypted and unauthenticated:** `encryptionPreference: .none` + `certificateHandler(true)` + fixed session code "1234" + self-minted director token means any device on the LAN speaking serviceType "signovivo" can join as a follower, receive the bundle, or mint a max-token and take over the congregation (token compare :1545 trusts the wire). Directors codes gate only the JS/relay layer, not the mesh.
7. **Comment drift on heartbeat cadence:** comments say "~2s heartbeat" (PdfReaderApp.tsx:68, :783) but the mesh heartbeat is 1s (:400). LIVE_DIRECTOR_WINDOW_MS rationale references the stale 2s number.
8. **Swift telemetry is unbatched:** every mesh event = one immediate URLSession POST to prod `/log` (DirectorSyncModule.swift:174-191), vs JS's 1s batching — chatty on churn (e.g. watchdog reconnect loops), and both layers POST to the PROD relay unconditionally (no build/env gate).
9. **Transmitter-only director reports blank fleet role:** fleetCheckin sends role "Director" only when roleRef==="director" (:195); a transmitter (roleRef "off" + explicitTransmitterRef) reports "" — dashboard shows the live web-transmitter as not directing.
10. **Follower watchdog vs a wedged director JS thread:** the 1s mesh heartbeat lives in RN JS (:393). If the director's JS stalls but Swift stays alive, all followers' 3s watchdogs (Swift :1256) will force-reconnect in a loop — the reconnect lands on the same silent director, churning every ~4-5s congregation-wide. The Swift director side has no self-heartbeat independent of JS (director_announce is also only sent from refresh cycles, ≥25s apart when serving).
11. **`startDirector` takeover flow briefly emits "idle" to the web:** becomeDirector's `resetNearbyDirectorSync()` (:487) → Swift resetTransport emits state "idle" (:1405) → injected into the web as a state event mid-takeover; possible momentary status flicker before "advertising".
12. **`sendPageUpdate` counts "delivered" on send-success, not delivery** (Swift :687-693) — deliveredPeers is best-effort optics only (unused by JS anyway).
13. **`sv.book.lastPage.standard` is written by native (:717-720) but never read by native** — restore is the web's job; harmless but write-only storage. Same for the wholly-vestigial keys in offlineBooks.ts:12-17, 21.
14. **JS `dbgLog` role field can lie for transmitters:** role is read from roleRef ("off") so a transmitter-director's telemetry shows role "off" (:164) — confusing when reading /log timelines.
15. **The relay heartbeat interval body double-guards but the mesh interval on a transmitter runs forever as a no-op** (:393-400 with roleRef never "director") — startDirectorHeartbeat starts BOTH intervals even on a mesh-less transmitter; the 1s mesh timer ticks uselessly for the whole Mass (tiny, but a wasted wakeup).
16. **`resync` rescue can race a Cancelled confirm:** `resync`'s stranded-off rescue calls becomeFollower (:733) which persists lastSyncRole="follower" — fine — but note soft-reset (roleRef "off") relies on this ⟳ path or an app restart to ever re-join; nothing else re-arms sync after soft reset.
