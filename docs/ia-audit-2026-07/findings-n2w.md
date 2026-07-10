# N2W — Bridge audit: native → web direction

> Lens: every native→web emission vs the web dispatcher `applyNativeSyncEvent` /
> `window.__signoVivoReceiveNativeEvent`. Audited at HEAD `d5075091` (build 381).
> All file:line anchors verified against CURRENT source (post-#269/#270/#271 — the
> map digests' anchors have drifted; these are re-verified).
>
> Emit-site inventory verified at this HEAD (PdfReaderApp.tsx):
> `bridge-state` :658 · `role` :449/:473/:508/:525/:573/:586/:669/:773 ·
> `sync-event page` :674-677/:690/:749/:911/:1008 · `sync-event state` :921-929 ·
> `relay-auth-error` :344. Web dispatcher handles bridge-state (:937), set-book no-op
> (:943), role (:947), relay-auth-error (:962), sync-event state/page (:972/:977).
> Every native emission has a web handler; the defects below are about FEEDBACK,
> BROWSING SEMANTICS, LOSSY ONE-SHOTS, and RETRY-LOOP side effects — not missing routes.

---

## N2W-01 (HIGH) — Invalid director code on native = total silence; the numpad closes and nothing ever happens

**Trigger/repro.** On any parish iPad (native shell): tap ♪, type a wrong 10-digit
director code (one digit off), tap Ir. The modal closes instantly and *nothing else
ever happens* — no error, no flash, no dialog. Same silence if the code is VALID but
`startNearbyDirector` fails twice (permission race / radio warm-up): the device
silently stays/returns follower.

**Code walk.**
- web/src/app.js:1183-1186 — in native mode (`NATIVE_FILE_MODE || hasNativeBridge()`),
  `goToDraftSong` posts `{type:"director-code", code}` then immediately `clearDraft();
  closeSongJump();`. No pending state, no timer, no response expectation.
- web/src/app.js:1187-1190 — the PURE-WEB branch *does* flash `"Código no válido"`
  via `flashSongDisplay` and keeps the numpad open. The affordance exists — it is just
  never used for the native round-trip.
- PdfReaderApp.tsx:584-587 — unrecognized code → `injectEvent({type:"role",
  role:"none"})`. The comment says «tell the web so it surfaces "código incorrecto"» —
  **that claim is false at current source**: the web's role handler
  (web/src/app.js:947-956) only sets `state.syncRole`/`state.nativeSyncRole` and calls
  `renderDirectorModeBadge()`. For a device that was already a follower, `data-role`
  stays "follower" and the badge stays hidden — a zero-pixel diff.
- PdfReaderApp.tsx:516-526 — valid code + confirmed, but mesh start failed twice:
  ex-follower silently `becomeFollower()` (injects role "follower" — again zero visible
  change); non-follower gets role "none" (same silence).

**User impact.** Director entry happens before every Mass, done by a volunteer on a
numpad with a 10-digit phone number. A mistype produces the exact symptom of "the app
is broken": modal vanishes, no confirm dialog appears. The user can't distinguish
(a) wrong code, (b) app hung, (c) dialog about to appear. The 2026-07-01 outage class
("no director all night") is one unnoticed failed promotion away.

**Fix approach.**
1. *Web-only interim (works against today's native):* when posting `director-code`,
   set `state.pendingDirectorCodeAt = Date.now()` and do NOT close the modal — show
   "Verificando…" on the numpad display. In the role handler (app.js:947), if a role
   event arrives while `pendingDirectorCodeAt` is fresh (<10s): `"none"` → flash
   "Código incorrecto" (reuse `flashSongDisplay`) and keep the modal open; `"director"`
   → close the modal (success); `"follower"` after a pending window (the silent
   mesh-fail fallback) → flash "No se pudo activar el modo director — intenta de
   nuevo". Clear the pending mark on any role event or timeout. Caveat: while the
   native confirm Alert is up, no role event arrives — so the pending state must not
   auto-flash on timeout alone; keep the modal open with the draft cleared and let the
   Alert resolution drive it (Confirm→role event; Cancel→nothing, user re-taps Cancelar).
   Simplest robust variant: keep the modal open, flash only on role:"none" within the
   window, and let Cancel/timeout just leave the reusable numpad visible.
2. *Proper fix (M3):* additive native event `{type:"director-code-result",
   ok:boolean, reason:"invalid"|"mesh-failed"|"cancelled"}` emitted from
   onDirectorCode/becomeDirector; web renders the right message per reason. Additive —
   old web bundles ignore the unknown type (verified: unknown payload.type falls
   through app.js:967 and returns).

**Acceptance criteria.** Wrong code → visible "Código incorrecto" on the still-open
numpad within 1s. Valid code + Cancel → numpad usable, no error shown. Valid code +
Confirm + mesh failure → visible non-lying error (NOT "código incorrecto").

**Test idea.** e2e (jsdom or the existing safe-web harness): stub
`window.ReactNativeWebView.postMessage`, submit a 10-digit draft, then call
`window.__signoVivoReceiveNativeEvent({type:"role",role:"none"})` and assert the
numpad is open and the display shows the error class. Regression: fire role:"none"
WITHOUT a pending submit (bridge-ready re-assert) and assert NO error flash.

---

## N2W-02 (HIGH) — Native follower browsing is yanked back within ~1s; the ♪ jump and drawer are dead affordances while a director is live

**Trigger/repro.** Director live on the mesh. A native follower taps ♪ and jumps to
song 45 (or swipes / uses the drawer). The page renders, then ≤1s later snaps back to
the director's page. Every navigation attempt is undone. No "Volver a en vivo" bar, no
explanation. On the web (signovivo.com), the SAME numpad jump enters an explicit
browsing mode with a go-live bar (app.js:1197-1205 — comment: "Jumping OFF the
director's live page = intentional browsing: pause auto-follow").

**Code walk (the yank loop).**
1. User browses to page X → `renderPage(X)` posts `page-changed` (app.js:1062-1067).
2. Native follower's page-changed handler adopts it: `currentPageRef.current = X`
   (PdfReaderApp.tsx:709-711 — the pre-ready gate at :704 applies only to
   director/transmitter).
3. Director's 1s mesh heartbeat re-sends director page P (PdfReaderApp.tsx:393-400).
4. Follower's mesh 'page' listener: dedupe `page === currentPageRef.current` FAILS
   (P ≠ X) → injects `sync-event page P` (PdfReaderApp.tsx:903-911).
5. Web dispatcher renders it unconditionally — `renderPage(event.page, ...)` with no
   browsing check of any kind (web/src/app.js:977-980).
6. `relay.browsing` can never protect: it's only set when `relay.hasDirector` is true
   (app.js:1200), and the relay is OFF in native mode (`startRelayFollow` early-returns,
   app.js:3329), so `relay.hasDirector` is always false; the go-live bar never shows.

**User impact.** The primary parish fleet (native iPads) offers followers ⟳ + ♪ as
their ONLY controls, and ♪'s core function (jump to a song) visibly self-reverts in
under a second whenever a director is live — i.e., during all of Mass. To an elderly
congregant this reads as "the iPad is broken". Web phones next to them behave
differently (browse + go-live bar), so the fleet is inconsistent.

**Fix approach (web-side, in the dispatcher — no native change needed).**
Maintain a mesh-side live-page mirror: in `applyNativeSyncEvent`'s page branch, always
record `meshLive.page = event.page` (and `meshLive.at`). Mark user-initiated
navigations (goToDraftSong, drawer taps, swipes — the same sites that would set
relay.browsing on web) as `meshBrowsing = true` when the target ≠ meshLive.page and a
mesh page has been received recently (within an 8s liveness window mirroring native's
LIVE_DIRECTOR_WINDOW_MS). While `meshBrowsing`: the page branch updates meshLive but
does NOT render; show the existing `showGoLiveBar()`. `goLive()` (and ⟳, which already
posts `resync`) clears `meshBrowsing` and renders meshLive.page.
IMPORTANT boot/reload guard: pushes at bridge-ready (fresh mount, no user navigation
since load) must render — initialize `meshBrowsing=false` on every load and only set it
from real user input handlers. Also clear it when no mesh page has arrived for >10s
(director gone → the follower's own navigation should stick).
Alternative/simpler product call: keep hard-follow but make it honest — disable
jump-to-other-page for native followers while mesh pages are fresh, with a toast
"Estás siguiendo al director". Either way the current silent tug-of-war must go.
NOTE ship vector: this is a web-only code change, but the affected devices are native
iPads — it reaches them at the NEXT native build (or mesh bundle push), not on Pages
deploy. Phones/PWA are unaffected (they already have relay browsing).

**Related known IDs (not duplicates).** `web-reader-browse-result-click-skips-relay-browsing-mode`
(OPEN) is the WEB-relay path missing the browsing flag on drawer taps; prior-art oddity
#1 covers F1-vs-browsing on web. THIS finding is the native/mesh path having no
browsing concept at all — a different dispatcher branch (app.js:977) and transport.
Fixing this should share the same user-navigation marking sites as the relay fix.

**Acceptance criteria.** With a live mesh director: follower jumps to a song → page
stays; go-live bar appears; director's next turn does NOT yank; tapping the bar or ⟳
returns to the director's current page. Fresh boot / WebView reload still snaps to the
director. Director gone >10s → follower navigation behaves standalone.

**Test idea.** Unit-test the decision (extract to a svSyncDecision-style pure helper:
`shouldApplyMeshPage({browsing, lastMeshAt, source})`); e2e: simulate
`__signoVivoReceiveNativeEvent` page pushes around a scripted numpad jump and assert
render vs bar.

---

## N2W-03 (MEDIUM) — relay-auth-error is a lossy one-shot: a WebView reload (or a pre-ready crash) permanently destroys the only "web congregation is dark" warning

**Trigger/repro.** Director's code gets rotated out of `TRANSMITTER_CODES` (the A1
rotation class) mid-session → first publish 401s → banner shows. Then the WebView
content process is reaped (memory pressure — it happens on the old iPads) → reload →
banner gone. Every subsequent publish keeps 401ing but the native latch never re-fires,
bridge-ready never re-asserts the warning, and the DOM banner was wiped. Variant (b):
the 401 fires while the WebView is mid-reload → event is QUEUED (webReadyRef false) →
`pendingInjectRef` is CLEARED by the terminate/remount path → the event is dropped
before delivery and the latch is already spent.

**Code walk.**
- src/directorRelaySync.js:89-96 — on 401/403, `authErrorNotified = true` then the
  handler fires ONCE. Re-arm happens ONLY on a subsequent `res.ok` (:88) or a new
  `setRelayPublishCode` (:36-41). A persistently-bad code never re-arms.
- PdfReaderApp.tsx:344 — the handler injects `{type:"relay-auth-error"}` exactly once.
- PdfReaderApp.tsx:1092 (`onContentProcessDidTerminate`), :315 (watchdog remount),
  :558 (soft reset), :959 (bundleUpdated) — all clear `pendingInjectRef`, so a queued
  relay-auth-error dies there.
- PdfReaderApp.tsx:638-696 — the bridge-ready handler re-asserts role and page but has
  NO relay-auth re-assert.
- web/src/app.js:887-925 — the banner is plain in-page DOM (`relayAuthWarningEl`);
  any reload starts from scratch; it is also user-dismissible (:918) with no persistent
  indicator behind it.

**User impact.** This banner is, by its own comment (app.js:881-885), "the ONLY signal
that the web congregation on signovivo.com has gone dark". After any crash-reload the
director's iPad looks completely healthy while every web follower is frozen for the
rest of Mass.

**Fix approach (native).** Track the latched state in the shell:
`relayAuthErrorRef = {status, at} | null`, set when the handler fires, cleared when a
new code is entered (becomeDirector) and on a successful publish (add an optional
`onPublishOk` callback to directorRelaySync, or expose `getAuthErrorState()` from the
lib). Then in the bridge-ready case (after the role assert, ~PdfReaderApp.tsx:669), if
the device is a broadcaster and `relayAuthErrorRef` is set,
`injectEvent({type:"relay-auth-error", status})` again. Web side needs no change
(showRelayAuthWarning is idempotent/re-revealing, :888-892).

**Acceptance criteria.** With a rejected code: banner visible → force
`onContentProcessDidTerminate` → after reload the banner re-appears without a new page
turn. New valid code entered → banner does not re-appear on reload.

**Test idea.** Contract e2e (source-pin style like the existing suite): assert the
bridge-ready block re-injects relay-auth-error when latched; unit-test directorRelaySync
latch re-fire behavior with a mocked fetch returning 401.

**majorUpdateIntersection.** M4's always-visible tri-state status pill is the right
permanent home for "relay dark" (a persistent state, not a dismissible banner); M3's
hello/welcome handshake is the natural carrier for re-asserted warning state.

---

## N2W-04 (MEDIUM) — render-failed sentinel + 1s mesh heartbeat = an unthrottled 1s failure loop that slams the drawer shut and re-renders the error overlay forever

**Trigger/repro.** Native follower whose current director page asset is missing or
corrupt on disk (the realistic path: a truncated/poisoned peer-pushed
Documents/WebBundle — the known "corrupt peer-bundle poisons every boot" class — or a
damaged install). Director sits on page P. Every ~1s, forever: render attempt → error
overlay → drawer forced closed.

**Code walk.**
1. web/src/app.js:1081-1097 — `renderPage` catch: `setLoading(true, "No se pudo cargar
   esta página.")` + `closeDrawer()` + posts `render-failed`.
2. PdfReaderApp.tsx:792-794 — native sets `currentPageRef.current = -1` (sentinel, by
   design so the heartbeat re-drives).
3. PdfReaderApp.tsx:393-400 — director's 1s mesh heartbeat re-sends P; follower dedupe
   at :903 fails (P ≠ -1) → re-inject → web re-fails → render-failed → -1 → … a tight
   ~1s loop with no backoff and no cap.
4. Escape is impossible: if the user navigates to a WORKING page X (currentPageRef
   becomes X via page-changed, :709-711), the next heartbeat (P ≠ X) re-injects P,
   which fails again → overlay + `closeDrawer()` again. The drawer is slammed shut
   every second; the follower cannot browse away from the failure.

**User impact.** A single bad page asset converts a follower iPad into a strobing
error screen for as long as the director stays on that page — and actively fights the
user's attempts to navigate anywhere else. The sentinel re-drive is the right idea for
transient failures (offline web PWA), but in file:// mode a missing file fails
deterministically, so the loop never converges.

**Fix approach.**
- *Web (biggest win, 2 lines):* in the renderPage catch, don't `closeDrawer()` when
  the failed render was native-pushed (`pushToHistory:false` from the dispatcher — or
  pass an explicit `source:"native"` option); and de-dupe the error path per page
  (track `lastFailedPage` + failure count; after N=3 failures of the SAME page, keep
  the overlay but stop re-posting render-failed for it so native's sentinel stops
  re-driving a deterministic failure). Show the existing overlay with a hint («Página
  no disponible en este dispositivo») instead of a bare error.
- *Native (belt):* cap sentinel re-drives — after render-failed for the same page K
  times, stop resetting to -1 until the incoming page CHANGES (track
  `lastRenderFailedPage`/count next to the sentinel logic at PdfReaderApp.tsx:777-795).
- Long-term this failure class disappears if the peer-bundle push is retired (Q5) or
  signed+validated per-file (M7).

**Acceptance criteria.** With one deliberately-missing page asset: follower shows the
error once, can open the drawer and navigate to other pages without interference, and
recovers automatically when the director moves to a renderable page.

**Test idea.** Web unit: renderPage with a stubbed always-failing loader called 5x for
the same page → assert closeDrawer called ≤1 time and render-failed posted ≤N times.
Native contract e2e: source-pin the re-drive cap.

---

## N2W-05 (LOW) — Dead injected global `__SIGNO_VINO_INITIAL_BOOK` + the load-bearing "VINO" typo + a false comment on the invalid-code path

**Code walk.**
- PdfReaderApp.tsx:1036-1038 — three globals injected via
  `injectedJavaScriptBeforeContentLoaded`, all spelled `__SIGNO_VINO_*` ("VINO", not
  "VIVO"). `__SIGNO_VINO_INITIAL_BOOK` (:1038) has exactly ONE occurrence repo-wide —
  the injection site; nothing consumes it (verified by grep across web/src, src,
  PdfReaderApp.tsx at this HEAD). It also drags the `initialBook` state +
  `setInitialBook` plumbing (:90, :839) and the preloadScript useMemo dependency
  (:1041) along as dead weight.
- web/src/app.js:227, :2903, :3480 — the web reads the OTHER two globals with the same
  typo, so the typo is load-bearing and self-consistent; it remains a grep trap
  (searching `SIGNO_VIVO` finds nothing).
- PdfReaderApp.tsx:585 — comment «Unrecognized → tell the web so it surfaces "código
  incorrecto"» describes web behavior that does not exist (see N2W-01) — a
  documentation trap for the next engineer.

**Fix approach.** Delete the `__SIGNO_VINO_INITIAL_BOOK` line + `initialBook`
state/plumbing (web never reads it; older bundles that DID read it are pre-374 and can
only appear paired with pre-374 shells, which inject it anyway). Keep FILE_MODE and
NATIVE_BUNDLE_VERSION under the existing names (renaming would break the
shipped-web/new-native pairing — do NOT fix the typo without a both-names transition,
which isn't worth it; instead add a one-line comment at both sites). Fix the :585
comment as part of N2W-01.

**Acceptance criteria.** Grep for `INITIAL_BOOK` returns zero hits; typecheck + the
native-entrypoint contract test pass.

---

## N2W-06 (LOW) — Swift `bundle-error` (15 stages) and `memoryWarning` die at `default: break`: a failed mesh OTA install has zero user surface AND zero telemetry

**Code walk.**
- PdfReaderApp.tsx:965-966 — the `DirectorSyncEvent` listener's `default: break`
  swallows every event type it doesn't know. Verified senders that hit it:
  `bundle-error` (DirectorSyncModule.swift:746 timeout, :764 pack, :784 send, :889
  install stages — ~15 distinct — :1893 receive, :1898 receive-nil) and
  `memoryWarning` (:274). Unlike `mesh:error` (PdfReaderApp.tsx:933) there is not even
  a `dbgLog` call, so `/log` timelines show a bundle transfer simply going quiet.
- Consequence pairing: the known pipeline finding (stale-Documents-bundle / re-download
  on every reconnect) means a follower can fail the same install on EVERY reconnect,
  ~30MB each time, forever — invisibly.

**User impact.** An iPad that keeps running an old web bundle "for no reason";
diagnosis requires physical access. Not Mass-breaking (rollback keeps the old bundle
working — that's why this is LOW), but it silently defeats the only update path that
reaches offline iPads and burns mesh bandwidth mid-Mass.

**Fix approach (native, small).** Add `case "bundle-error":` →
`dbgLog("bundle:error", {stage: event.stage})` + optionally inject a low-key web toast
event (additive `{type:"native-notice", kind:"bundle-error"}` — old bundles ignore
unknown types, verified). Add `case "memoryWarning":` → `dbgLog` + breadcrumb (cheap
forensics for the crash-recovery ladder). Do this only if Q5 keeps the mesh push;
if P-MESH retires bundle distribution, delete the Swift senders instead.

**Acceptance criteria.** A forced install failure (corrupt archive) produces a /log
entry with the stage; nothing user-visible beyond the optional toast; contract e2e
pins the case exists.

**majorUpdateIntersection.** M7 (mesh bundle sha256+signature work touches exactly
this code) and open decision Q5 (mesh retire-vs-keep) — sequence this WITH that work,
not before it.

---

## Verified-clean (checked, NOT findings — recorded so skeptics/re-hunts don't re-chase)

- **Role/page re-assert on reload:** every WebView reload path (terminate
  PdfReaderApp.tsx:1089-1094, watchdog remount :313-316 + mount effect :327,
  bundleUpdated :958-961, soft reset :557-559) resets `webReadyRef` and clears the
  queue; bridge-ready re-asserts bridge-state + role + (director) page or (follower)
  snapshot+requestCurrentSnapshot (:638-695). The web never comes back role-less:
  `renderDirectorModeBadge` defaults data-role to "follower" (app.js:849) and native
  re-asserts within one handshake.
- **A3 gate has no web-initiated-reload hole at current source:** the only
  `location.reload()` sites reachable in native mode are the bootGuard card (app.js:38,
  only reachable pre-`__svBooted`, i.e. before bridge-ready was ever posted — initReader
  posts bridge-ready at :3469 AFTER revealReader at :3451) and the dead offline-gate
  retry (:2370, unreachable — gate never shown). SW registration (the controllerchange
  reload) is hard-gated off in native (app.js:2062). So `webReadyRef` can't be
  stale-true across a page load, and the :704 pre-ready gate holds.
- **Handler-registration ordering:** `window.__signoVivoReceiveNativeEvent` is assigned
  at module eval (app.js:992), long before `bridge-ready` is posted (:3469); native
  queues until bridge-ready (PdfReaderApp.tsx:276-279), so no emission can race the
  listener's existence. The `window.__signoVivoReceiveNativeEvent && …` guard in
  injectEvent (:273) covers the residual window as a silent no-op.
- **Unknown-type forward-compat, both directions:** web ignores unknown `payload.type`
  (falls through app.js:967) and unknown `event.type`; native's web→native router
  ignores unknown `msg.type` (PdfReaderApp.tsx:797-798). Additive events are safe.
- **set-book vestige:** native never emits it at this HEAD; web no-ops it (app.js:943).
  Only an OLD (pre-374) shell paired with a NEW mesh-pushed web bundle would send it —
  no-op is the correct handling.
- **Backgrounding:** the AppState 'active' handler (PdfReaderApp.tsx:993-1019)
  re-asserts follower snapshot / re-broadcasts director / re-publishes transmitter, and
  a background-reaped content process funnels through terminate→bridge-ready — both
  freeze windows reconcile.
- **Injected globals re-inject on reload:** `injectedJavaScriptBeforeContentLoaded` is
  a WKUserScript — it runs on every (re)load, and NATIVE_FILE_MODE is double-guarded by
  the `file:` protocol check (app.js:227).
- **Echo-loop safety:** web posts page-changed on every render including native-pushed
  ones (app.js:1062); native's follower path updates the ref (no broadcast), director
  path re-broadcasts the same page (mesh followers dedupe at PdfReaderApp.tsx:903;
  relay coalesces). No loop.
- **startFollower rejection is effectively unreachable** (only rejects on empty session
  code; session is fixed "1234" — DirectorSyncModule.swift:388-411), so the web's
  "follower" role assert after becomeFollower is honest; async transport failures are
  covered by M-F7 retry-forever.

## Parking lot (speculative / dedupe notes — not findings)

- `sv.book.lastPage.standard` re-verified WRITE-ONLY at this HEAD (written
  PdfReaderApp.tsx:717-720; zero readers native or web — web always boots
  DEFAULT_START_PAGE, app.js:3411). Confirms the known-PARTIAL
  `new-director-dead-writes` row; no delta beyond confirmation.
- Every mesh-driven follower render persists lastPage to AsyncStorage (:717-720) — a
  disk write per director page-turn per follower. Harmless volume; fold into the
  dead-writes cleanup (P5/P8).
- `sync-event state` forwards Swift's Spanish `message` strings but the simplified
  spinner (post-#271) uses only `status` (app.js:972-975) — `message`/`role` are dead
  payload on the wire now. Candidate trim for M3's typed protocol.
- Old two-book shell (≤373) + new mesh-pushed web bundle could deliver page events
  with `book:"hymns-4"`; new web ignores event.book and renders that page number in
  the standard book. Theoretical only (fleet floor is 361→377+); subsumed by the known
  stale-Documents-bundle finding.
- The inject queue's oldest-drop (cap 100) can theoretically shed a role event while
  keeping heartbeat pages — already on record as "inject-queue drops by age not
  importance" (A.3 noted-unfixed); bridge-ready's role re-assert makes it moot in
  practice.
- `state.syncRole` (app.js:177) remains a dead duplicate of `nativeSyncRole` — known
  web oddity O5/O6.
- Native fresh-boot follower always shows page 2 until the mesh connects (initReader
  renders DEFAULT_START_PAGE at app.js:3449 because relay.hasDirector is always false
  in native mode; there is no native equivalent of the web's 1500ms boot relay peek).
  Expected discovery latency; only worth revisiting if mesh connect times regress.
