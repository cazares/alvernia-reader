# DIRNAT — Director experience, native iPad (portrait, live-Mass lens)

Audit lens: the DIRECTOR's end-to-end experience on the native app under live-Mass pressure.
All evidence verified at worktree HEAD `d5075091` (build 381 — NOTE: 4 commits ahead of the maps'
16244b25/377; commits #269-#271 touched the sync spinner, fleet self-ID web modal, and PDF rename —
all line numbers below re-verified against d5075091). Files: PdfReaderApp.tsx, web/src/app.js,
web/src/styles.css, web/src/index.html, src/directorRelaySync.js, src/nearbyDirectorSync.js,
ios/SignoVivo/DirectorSyncModule.swift, node_modules/react-native-webview/apple/RNCWebViewImpl.m
(read from the MAIN checkout /Users/cazares/src/alvernia-reader — the worktree has no node_modules).

Dedupe notes: the "am I live?" peer-count / status-pill base gap is PLANNED (major-update §6.5 M4
pill: director sees "● Dirigiendo — N conectados"; M7 DIAGNÓSTICO) — NOT re-reported. DIRNAT-06 and
DIRNAT-09 are deltas the plan does not cover. DIRNAT-02 is a delta on known-FIXED H3 (#267).

---

## DIRNAT-01 — Invalid/rejected director code on native gives ZERO feedback (silent dead-end at the numpad) — HIGH

Trigger/repro (native iPad): open ♪, type a 5+-digit code with one wrong digit, tap Ir.
The modal closes immediately and… nothing. No error, no dialog, no flash. The director cannot
tell "mistyped" from "still validating" from "I'm now director" (the only success signal is the
small top-left DIRECTOR badge appearing).

Code walk:
- web/src/app.js:1178-1191 (goToDraftSong): a >=5-digit code on native posts {type:'director-code'}
  then clearDraft(); closeSongJump(); — the feedback surface (the numpad display) is destroyed
  before validation even starts. flashSongDisplay("Código no válido","err") at :1189 runs ONLY on
  the pure-web else-branch.
- PdfReaderApp.tsx:583-587 (onDirectorCode): unrecognized code -> injectEvent({type:'role',
  role:'none'}), with a comment claiming "tell the web so it surfaces 'código incorrecto'" —
  that UI does not exist.
- web/src/app.js:947-955 (applyNativeSyncEvent, role case): role 'none' only sets
  state.nativeSyncRole='off' and hides the badge. No user-visible output anywhere.
- Same silent dead-end for the empty-code path (PdfReaderApp.tsx:572-575) and — worse — for the
  post-confirm mesh-start failure (PdfReaderApp.tsx:516-527, see DIRNAT-03).
- Two prior docs ASSUME the feedback exists (docs/app-hardening-plan.md:576 and :586 both say the
  web "shows 'código incorrecto'") — they describe imagined UI; fix the docs too.

Impact at Mass: the director-entry flow is THE critical pre-Mass action. A mistyped 10-digit
phone-number code (plausible: glass numpad, elderly volunteer, low light) fails silently; the
director may walk away believing they're live. Congregation never syncs; nobody knows why.

Fix approach:
1. Native: on the reject path emit a NEW additive bridge event
   {type:'director-code-result', ok:false, reason:'unknown-code'} (keep legacy role:'none' for old
   bundles). Also emit ok:true on committed promotion (alongside role:'director').
2. Web: on ok:false, keep/re-open the jump modal and flashSongDisplay("Código incorrecto —
   verifica el número","err"). Robust variant: in goToDraftSong's native path do NOT
   closeSongJump(); show "Verificando…" in the display; close on role:'director'/ok:true; flash
   error on ok:false; a 5s timeout closes quietly (covers the user cancelling the native confirm,
   which produces no event).
3. Interim web-only heuristic (ships via Pages; reaches iPads only at next native build/mesh
   push): set a pendingDirectorCode timestamp when posting director-code; if role:'none' arrives
   within ~4s, flash the error. Caveat: role:'none' is also injected by the DIRNAT-03 failure
   path — acceptable (it IS a failure), but then use generic copy ("No se pudo activar el modo
   director").

Acceptance: wrong code -> visible Spanish error within 1s, numpad still open for retry; valid
code + Cancelar -> no error shown; valid code + Sí -> badge appears.
Test idea: e2e — stub window.ReactNativeWebView, post a code, inject role:'none', assert the
error text renders; unit-test the pending-code timeout.

---

## DIRNAT-02 — H3/#267 DELTA: the persisted transmitter-director role is UNREADABLE — the resume prompt can never fire on the device class it was built for — HIGH

Known-finding delta: prior art marks relay-transmitter-only-role-lost-on-relaunch as FIXED-DG via
H3 PR #267. The fix writes the breadcrumb, but its only reader is gated so it never runs on a
transmitter-only device. The fix is dead code for its own scenario.

Code walk:
- Writer: PdfReaderApp.tsx:461-476 — the transmitter branch runs ONLY when !syncAvailable (no
  mesh module) and persists lastSyncRole='director' at :472 precisely so "the boot resume prompt
  fires" (comment :469-472).
- Reader: PdfReaderApp.tsx:849-850 — the bootstrap effect opens with `if (!syncAvailable) return;`.
  The ONLY read of STORAGE_KEYS.lastSyncRole (:869-882, the "Estabas dirigiendo" Alert) is inside
  this effect. syncAvailable = Platform.OS==='ios' && Boolean(NativeModules.DirectorSyncModule)
  (src/nearbyDirectorSync.js:6) — a per-install constant. A device that wrote the breadcrumb
  (!syncAvailable) is still !syncAvailable at next boot -> the prompt NEVER fires -> the exact
  "silent follower, web congregation frozen, no 401 signal" outage #267 was shipped to close.
  (The mesh-director path at :506 is fine — its reader runs.)
- Compounding: the transmitter-only exit-director path (PdfReaderApp.tsx:754-775) never clears/
  overwrites lastSyncRole (mesh exit goes through becomeFollower, which writes 'follower' at
  :432). Once the gate is fixed, an INTENTIONAL transmitter exit would leave a stale 'director'
  -> false "Estabas dirigiendo" prompt on every subsequent boot. Fix both together.
- Also: on a !syncAvailable boot, becomeFollower() is never invoked at all (same gate), so the
  device never persists 'follower' as a corrective either.

Real-world exposure: every parish iPad/iPhone has the Swift module, so today the class is Expo
Go / a build that failed to link the module — kept HIGH (not critical) because the shipped fix's
claimed coverage is wrong AND M7's acceptance test ("force-quit+relaunch the director -> visible
resume prompt") only exercises the mesh path, so device verification would pass while this stays
broken.

Fix approach: split the bootstrap out of the mesh effect: a new one-shot effect with NO
syncAvailable gate that (a) reads lastSyncRole, (b) shows the resume prompt if 'director',
(c) then calls becomeFollower() only when syncAvailable (a no-mesh device stays 'off' as today
but persists 'follower' to clear the breadcrumb). In the transmitter exit branch (:769-773) add
AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole,'follower') (or remove the key).
Acceptance: with the native module stubbed out — enter code, kill app, relaunch -> prompt fires;
exit-director, relaunch -> no prompt.
Test idea: extract the bootstrap decision into a pure helper; unit-test the
(syncAvailable x persistedRole) matrix. M7 device day adds the no-mesh variant if such a device
ever ships.

majorUpdateIntersection: M7 NEW-DIR-1 acceptance criteria — add the transmitter-restart case.

---

## DIRNAT-03 — After "Sí, dirigir" the activation is fire-and-forget: no progress indicator, and a failed start silently demotes the just-confirmed director — HIGH

Trigger/repro: enter a valid code, confirm "Sí, dirigir" (or the red "Tomar el control").
startNearbyDirector transiently fails twice (permission race, radio warm-up, Bluetooth off, or
Swift raced DIRECTOR_TAKEOVER_REQUIRED back). The user, who just explicitly confirmed a role
change, gets: nothing. No dialog, no banner. If they were a follower they silently remain one;
otherwise the web gets role:'none' (which renders nothing — DIRNAT-01).

Code walk:
- PdfReaderApp.tsx:492-501: start + one 2s-sleep retry -> up to ~2-4s (plus mesh startup) with
  zero "activating…" feedback; the DIRECTOR badge appears only after success (:508).
- PdfReaderApp.tsx:516-527 (catch): wasFollower ? becomeFollower() : injectEvent(role:'none') —
  deliberately non-stranding (good) but completely mute (bad). The comment explains the transport
  recovery, not the human's.
- Swift's own rejection copy never surfaces: DirectorSyncModule.swift:366 rejects with "Ya hay un
  director conectado. Solicita permiso para tomar control." — swallowed by the catch; and there
  is no "solicitar permiso" UI anywhere (takeover requests are auto-denied,
  PdfReaderApp.tsx:946-952; the Swift message is a dead instruction).
- Async Swift DIRECTOR_START_FAILED (DirectorSyncModule.swift:1615) reaches the JS listener's
  error case (PdfReaderApp.tsx:932-944) which only acts on DIRECTOR_CONFLICT — dropped.

Impact at Mass: the one moment the operator explicitly asserted control is the one moment the app
may silently refuse. They face the congregation believing they're live. The absence of a small
badge is not a failure signal an elderly volunteer will catch.

Fix approach (shell JS only, no Swift): in the catch at :516,
Alert.alert("No se pudo activar el modo director","Revisa que Bluetooth y Wi-Fi estén encendidos
e inténtalo de nuevo.",[{text:"Reintentar",onPress:()=>becomeDirector(code)},{text:"Cancelar"}])
(guard on myGen === roleGenerationRef.current before showing; Reintentar re-runs becomeDirector,
which re-bumps the generation). Optionally emit director-code-result ok:false reason:'mesh-start'
(pairs with DIRNAT-01's web surface). For the latency window, inject a transient
{type:'sync-event',event:{type:'state',status:'connecting'}} before starting so the existing
⟳-spin "still working" indicator (app.js:863-878) covers it — zero new UI.
Acceptance: with startDirector forced to reject twice — confirm -> visible Spanish error with
Reintentar within ~5s; device remains a functioning follower.
Test idea: unit-test the becomeDirector failure path with a mocked nativeModule (assert Alert
fired + role-event sequence).

majorUpdateIntersection: M7 native batch (DIAGNÓSTICO / panic buttons) touches these same shell
surfaces — fold in; M3's acked bridge later formalizes the result event.

---

## DIRNAT-06 — M4 DELTA: relay publishes failing with NETWORK errors (not 401/403) are invisible to the director — web congregation freezes with no director-side signal; the planned M4 pill doesn't cover this leg — HIGH

Trigger/repro: the director iPad's internet dies mid-Mass (hotspot drop, captive portal, carrier
blip — the plan doc itself says "The iPads have NO wi-fi inside the church",
docs/major-update-2026-07.md:26-27). Mesh keeps the iPads in sync; every relay publish now
throws/aborts. Web followers (personal phones, the old home-screen PWA iPad) freeze on the last
page and demote to "sin director" after 90s. The director's screen: nothing, all Mass.

Code walk:
- src/directorRelaySync.js:97-99: `catch {}` — network failures/aborts swallowed by design ("the
  next page change — or the heartbeat — re-publishes"). Self-heals for blips; for a persistent
  outage it is an infinite SILENT retry.
- src/directorRelaySync.js:87-96: the ONLY warning path is res.status === 401 || 403 (the
  relay-auth banner, app.js:887-925). The comment even asserts 5xx/429 are "transient" — a dead
  uplink is neither a 5xx nor transient.
- Planned-coverage check: M4's pill spec (docs/major-update-2026-07.md:368-371) gives the director
  "● Dirigiendo — N conectados" (peerCount) — mesh-only. DIAGNÓSTICO (:443-449) shows "relay
  status" but is a pre-Mass long-press screen, not a live signal. Followers get the red pill; the
  director — the only person who can fix it (toggle hotspot, move) — stays blind. This is the
  un-specced delta. (Related M4 implementation detail: peerCount is currently dropped at the
  bridge — PdfReaderApp.tsx:921-929 forwards status/role/message only.)

Fix approach: track publish outcomes in directorRelaySync.js (lastPublishOkAt epoch; add
setRelayHealthHandler alongside setRelayAuthErrorHandler). Native samples it on the existing 12s
relay heartbeat: if the last OK is older than ~45s while roleRef==='director' ||
explicitTransmitterRef, inject a one-shot latched (re-armed on recovery, mirroring
authErrorNotified) {type:'relay-health', ok:false}; web reuses the existing banner element with
different copy: "Sin internet — los teléfonos en signovivo.com no están recibiendo tu página.
Los iPads cercanos siguen bien." Auto-hide on ok:true (see DIRNAT-08 — today the banner never
auto-clears).
Acceptance: kill the uplink while directing -> banner within ~60s; restore -> banner clears
itself; mesh followers unaffected throughout.
Test idea: unit-test the health tracker (mock fetch reject; assert handler fires once, re-arms
after an ok).

majorUpdateIntersection: extend M4's status-pill spec — the director pill needs TWO inputs:
mesh peerCount AND relay publish-ack freshness (e.g. "● Dirigiendo — 5 iPads · web ✓/✗").

---

## DIRNAT-09 — A taken-over (demoted) director is never told; Swift's Spanish explanation strings are dead copy — their page then snaps under their fingers with no explanation — HIGH

Trigger/repro: director A is live. Miguel (or anyone with a code) confirms "Tomar el control" on
device B — the DESIGNED handoff mechanism (admin force-takeover "rides the Swift conflict path",
PdfReaderApp.tsx:947-948). On A: the badge silently vanishes, ⟳ appears, and moments later A's
page JUMPS to B's page (the C2 winner-snapshot pull). No dialog, no toast, no text. A keeps
swiping — swipes now move only A's own screen.

Code walk:
- PdfReaderApp.tsx:932-944 (error case): on DIRECTOR_CONFLICT -> stop heartbeat, becomeFollower(),
  pull winner snapshot. event.message — Swift's ready-made "Un nuevo director tomó el control.
  Este dispositivo cambió a modo seguidor." (DirectorSyncModule.swift:1546) — is never read,
  never injected, never shown.
- The state case (:914-930) DOES forward `message` to the web, but the web ignores it:
  web/src/app.js:972-975 uses only event.status to drive the ⟳ spinner. Swift's other
  director-relevant copy ("Cediendo el control al nuevo director...", swift:490) is dead too.
- Asymmetry: B's confirm dialog explicitly says "le quitarás el control" (PdfReaderApp.tsx:609-612)
  — the system knows a human is being displaced and tells only the displacer.
- Dedupe: held M-F6 is about the loser's FOLLOWERS (mesh redirect hint,
  docs/sync-reliability-audit-2026-07.md:65); C2 (#264) fixed the loser's PAGE sync. Neither gives
  the demoted human a notification. Not covered by M4's pill spec either (a pill flip is as silent
  as the badge flip).

Impact at Mass: the two-directors moment is precisely when the room is already confused; the
ex-director flailing (swiping, re-entering their code — which then triggers the red takeover
warning and can ping-pong control) makes it worse. One sentence on screen ends it.

Fix approach (shell JS only): in the DIRECTOR_CONFLICT branch, after becomeFollower():
Alert.alert("Otro director tomó el control","Este iPad ahora sigue al nuevo director. Si debes
dirigir tú, vuelve a entrar tu código en ♪.",[{text:"Entendido"}]). (A native Alert survives the
concurrent role churn and needs no web change; optionally also forward mesh `message` strings to
a small web toast — bigger, defer.)
Acceptance: two devices; B takes over; A shows the dialog within ~2s of the badge vanishing; A's
subsequent ⟳/follow behavior unchanged.
Test idea: fire a synthetic {type:'error',code:'DIRECTOR_CONFLICT'} through the listener; assert
Alert + becomeFollower ordering. Device-verify on the M7 2-device day.

majorUpdateIntersection: pairs with held M-F6 (followers' redirect) — implement in the same
M7/mesh batch; NEW-DIR-3's acceptance flow gains a "loser is informed" assertion.

---

## DIRNAT-04 — Exit-director confirm is window.confirm: hardcoded English "Ok"/"Cancel" buttons, empty title, and copy that's wrong for the transmitter path — MEDIUM

Trigger/repro: director taps the top-left DIRECTOR badge. An iOS system alert appears with
Spanish body text but buttons "Ok" and "Cancel" — react-native-webview hardcodes them:
node_modules/react-native-webview/apple/RNCWebViewImpl.m:1222-1231 (alertControllerWithTitle:@""
… actionWithTitle:@"Ok" … @"Cancel"). Caller: web/src/app.js:2426-2431.

Why it matters: the users are elderly Spanish-speaking volunteers; the bar is zero-training. The
single most destructive tap on the director's screen (stepping down mid-Mass) is gated by two
English words. Secondary: the copy "volverás a seguidor" is false on a transmitter-only device,
which drops to standalone "off" with no follower transport (PdfReaderApp.tsx:763-774).

Fix approach: replace window.confirm with a tiny in-page styled dialog (reuse the relay-warn
overlay pattern, app.js:887-925) with buttons "Seguir dirigiendo" (default) / "Salir del modo
director" (destructive), then post exit-director on confirm. Alternative: route through native
(exit-director-request -> Alert.alert with Spanish buttons) — needs a native build; prefer the
web dialog. shipVector caveat: the badge/dialog only exists inside the native shell, so this
web-only fix reaches directors at the next native build or mesh bundle push, not on Pages deploy.
Acceptance: badge tap -> fully-Spanish dialog; cancel keeps directing (badge stays, heartbeats
uninterrupted); confirm posts exactly one exit-director.
Test idea: e2e DOM test of the dialog wiring + a guard that `window.confirm(` has no callers.

---

## DIRNAT-05 — The 44px left-edge drawer zone hijacks the director's "previous page" swipe; an accidental drawer tap then broadcasts to the whole congregation — MEDIUM

Trigger/repro (portrait iPad, director): turning BACK a page is a rightward swipe. Held in
portrait, the natural thumb arc starts at the left bezel. Any rightward swipe starting at x<44
opens the DRAWER instead of turning back: web/src/app.js:2723-2730 (edge zone wins), :2733-2737
(page swipe requires startX>=44), plus the window-level duplicate :2789-2810. The top-left
DIRECTOR badge (left ≈ 9px, styles.css:154-158) sits inside this zone, widening the accidental
surface. Once the drawer is open, EVERY browse tap is live: drawer taps -> turnSong/renderPage ->
page-changed (app.js:1062-1067) -> broadcastPage (PdfReaderApp.tsx:721) -> the entire
congregation (mesh + relay) jumps with the director's exploration.

Impact: mid-Mass the director tries to flip back one page for a repeated refrain; instead a
drawer slides over their music; a hurried dismissal tap can land on a song row and yank every
device in the room. There is no un-broadcast browse for a director.

Fix approach (web):
1. For html[data-role="director"], shrink the edge zone (44 -> 24px) and/or require a longer
   travel (40 -> 80px) so a back-page swipe wins; the drawer handle + tap remain as entries.
2. Exclude touches starting on the DIRECTOR badge from both edge-swipe handlers
   (event.target.closest('#director-mode-badge') -> skip).
3. (Optional, parking lot) a director "vista previa" browse mode deferring broadcast until
   "Ir aquí".
Acceptance: as director, a rightward swipe starting 10-40px from the bezel turns the page back;
drawer still opens via handle/tap, and via edge swipe as follower.
Test idea: extract the gesture decision (startX, dx, dy, role -> action) into a pure helper and
unit-test the matrix.
shipVector caveat: web-only, but reaches director iPads only at the next native build/mesh push.

---

## DIRNAT-07 — Post-restart window bypasses the "Ya hay un director activo" takeover warning: the boot prompt urges re-entering the code before the device can know a successor is live — MEDIUM

Trigger/repro: director A crashes mid-song; B takes over and directs. A's app relaunches:
"Estabas dirigiendo … reingresa tu código en el teclado (♪)" (PdfReaderApp.tsx:869-882). A
obediently re-enters the code immediately. The red takeover warning requires a mesh snapshot
younger than 8s (PdfReaderApp.tsx:598-602, LIVE_DIRECTOR_WINDOW_MS :71) — but
lastDirectorSnapshotRef is null until the concurrent becomeFollower() finishes mesh discovery +
connect + receives B's first page (browse cycles can take longer than typing 10 digits). A gets
the calm "¿Dirigir el coro?" (:603-612), confirms, and unknowingly hijacks B — the token conflict
then demotes B, who (per DIRNAT-09) is never told. The boot prompt manufactures exactly the race
NEW-DIR-3 tried to de-fang, and M7's acceptance test covers only the calm case
(docs/major-update-2026-07.md:150).

Fix approach (native shell):
1. Copy (cheap): append "…Si otra persona ya está dirigiendo, avisa antes de entrar tu código."
2. Better: in onDirectorCode, when lastDirectorSnapshotRef.current === null AND the mesh is in
   searching/connecting (track the last state-event status in a ref), treat "unknown" differently
   from "no director": delay the promotion commit ~3s after confirm while requestCurrentSnapshot()
   races; if a snapshot lands in the grace window, re-prompt with the red warning instead of
   proceeding. Bounded so a genuinely solo director is delayed only once.
Acceptance (2-device, M7 day): A restarts while B directs; A re-enters the code within 10s of
boot -> A sees the RED warning (or a re-confirm), never the calm prompt. Solo restart -> calm
prompt, promotion works.
Test idea: unit-test the liveDirector/unknown decision helper across (snapshotAge x meshStatus).

majorUpdateIntersection: M7 NEW-DIR-3 acceptance list — add this hijack-window case.

---

## DIRNAT-08 — Relay-auth banner: "El relé" is jargon, gives no recovery action, and never auto-clears after recovery (keeps claiming followers are frozen when they're synced again) — LOW

Code walk: copy at web/src/app.js:912: "El relé rechazó el código de director. Los seguidores en
signovivo.com NO están sincronizados." — "relé" (an electrical relay) is meaningless to the
audience; no next step is offered. Lifecycle: directorRelaySync.js:88 re-arms the latch on a
successful publish, but nothing hides the banner — showRelayAuthWarning only ever adds is-on
(app.js:887-925); the sole remover is the manual × (:918). After a transient 401 (e.g. re-entered
code now works), the banner keeps asserting a live outage.

Fix approach (web-only; native reach at next build/push): copy -> "El servidor rechazó tu código
de director: los teléfonos en signovivo.com no están recibiendo tu página. Vuelve a entrar tu
código en ♪ o avisa a Miguel." Add hideRelayAuthWarning() and have native forward recovery
(inject {type:'relay-auth-error', status:0, recovered:true} when res.ok follows a notified
failure — one line next to authErrorNotified=false in directorRelaySync.js:88, routed through the
existing handler) -> web removes is-on.
Acceptance: 401 -> banner; next OK publish -> banner disappears by itself; copy contains an action.
Test idea: unit-test the doPublish latch/recovery firing sequence (fail -> handler(401);
ok -> handler(recovered)).

---

# Parking lot (speculative / product ideas — NOT findings)

- Director "peek" mode: no way to browse ahead without broadcasting (drawer, numpad, swipes all
  go live). A "vista previa -> Ir aquí" deferred-broadcast mode would fit next to M7's panic
  buttons. (Interacts with DIRNAT-05.)
- Re-entering your own code while already director re-runs the whole becomeDirector pipeline
  (mesh restart churn; the roleRef!=='director' guard only affects the warning copy). Cheap
  guard: same code while directing -> "Ya estás dirigiendo" toast, no-op.
- Restarted director lands on web boot page 2 (sv.book.lastPage.standard is written at
  PdfReaderApp.tsx:717-720 but never read; web boots to DEFAULT_START_PAGE=2, app.js:215/3411) —
  re-navigation friction stacked on re-entering a 10-digit code. Covered by planned M7/A3
  "broadcast the persisted last page"; noting the director-UX angle so the fix also RESTORES the
  reader's own view, not just the broadcast.
- setIdleTimerDisabled / getDeviceName are bridged but never called (map oddity): keep-awake
  rests solely on expo useKeepAwake (PdfReaderApp.tsx:86). Fine today; if expo-keep-awake ever
  regresses, the director's screen sleeping mid-Mass is the failure mode. Either call the Swift
  belt-and-suspenders on director entry or delete the dead bridge methods.
- Takeover request/approve/deny trio is dead surface (JS auto-denies at PdfReaderApp.tsx:946-952;
  wrappers in nearbyDirectorSync.js:75-97 never called; Swift's "Solicita permiso" message is a
  dead instruction) — retire the strings/wrappers or build the v2 flow; ties to open Q5.
- Fleet self-ID Alert.prompt ("¿Quién usa este iPad?") remains on native (PdfReaderApp.tsx:218-246)
  after #270 removed the web modal — intentional? The same UX argument applies to the native
  first-boot moment before Mass.
- No visual differentiation of super-admin after entry (badge says DIRECTOR for everyone) — fine
  today; note for M6 super-admin dashboard work.
