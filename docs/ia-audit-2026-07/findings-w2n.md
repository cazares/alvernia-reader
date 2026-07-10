# W2N — Bridge audit: web → native (lens findings, extended detail)

Auditor lens: every web-emitted postMessage on channel `signovivo-native` vs the native
handler in PdfReaderApp.tsx (router at :627-810 at HEAD d5075091), plus the surrounding
role machine, inject queue, and the Slice B bridge-ready watchdog / Reintentar view.

Ground truth verified at HEAD d5075091 (worktree of 16244b25, build 377).

Message inventory (verified complete, both directions):

| web emits (app.js) | native handles (PdfReaderApp.tsx) |
|---|---|
| bridge-ready :3469 | :638 OK |
| page-changed :1062 | :698 OK |
| render-failed :1093 | :777 OK |
| director-code :1184 | :724 OK |
| exit-director :2429 | :754 OK |
| resync :3094 | :727 OK |

No web-emitted type is unhandled; no native case lacks a web emitter. `book-changed`
(described by the stale contracts doc) no longer exists on either side — nothing to fix
beyond the already-known doc staleness. Envelope robustness is sound: handleMessage
JSON-parses defensively (:629-634), requires channel === "signovivo-native" (:635), and
non-object frames fall out safely. The echo loop is structurally closed: web posts
page-changed on EVERY render including native-injected ones (app.js:1062), but
broadcastPage (:350-369) is gated on roleRef==='director' || explicitTransmitterRef, so a
follower echo never re-enters mesh/relay. The render-failed <-> optimistic-ref pairing
(:777-795) correctly gates the -1 sentinel to followers only (H4 interplay verified).

---

## W2N-01 — Invalid director code (and failed director start) is COMPLETELY SILENT on native

Severity: high · Surface: cross · Ship: web-only (see caveat). Not in prior art — the
prior-art Q2 line (new-director-super-admin-label-gated-by-standard-set) ASSUMES a
"código incorrecto" surface exists; it does not.

### Trigger / repro
Native iPad: open the ♪ numpad, type a 5+ digit code NOT in the baked
STANDARD_DIRECTOR_CODES (fat-finger one digit of the real number), tap Go. The modal
closes and nothing whatsoever happens. Same silence if the code is valid but
becomeDirector's mesh start fails on a non-follower device (catch emits role:'none',
PdfReaderApp.tsx:525).

### Code walk
1. Web: goToDraftSong (app.js:1172-1206) — 5+ digits + bridge → posts
   {type:'director-code', code} then immediately clearDraft(); closeSongJump()
   (app.js:1184-1186). The numpad modal is gone before native replies.
2. Native: onDirectorCode (PdfReaderApp.tsx:563-624). Unrecognized code →
   injectEvent({type:'role', role:'none'}) (:584-587). The comment at :585 says
   "tell the web so it surfaces 'código incorrecto'" — that surface does not exist.
3. Web: applyNativeSyncEvent role branch (app.js:947-956) only stores
   state.nativeSyncRole and calls renderDirectorModeBadge() — which maps
   'none'→'off'→data-role="follower" (app.js:845-852). No user-visible output.
4. The only "Código no válido" flash in the codebase (flashSongDisplay, app.js:831-841,
   called at :1189) lives in the pure-web ELSE branch — it never runs when a bridge
   exists. And it writes into songDisplay inside the already-closed modal, so even
   reusing it verbatim would be invisible.

### User impact at Mass
The volunteer director mistypes one digit → believes the code "went through", no badge
appears, nobody follows. This is the UX half of the 2026-07-01 outage class (code
rejected → no director all night). It also compounds known-OPEN
coherence-director-codes-two-hand-synced-stores (non-release builds ship an EMPTY code
set → EVERY code silently rejected).

### Fix approach
Web-only, no wire change needed (works against current native builds):
1. In goToDraftSong, when posting director-code, record
   state.pendingDirectorCodeAt = Date.now().
2. In the role branch of applyNativeSyncEvent: if role:'none' arrives while
   Date.now() - pendingDirectorCodeAt < 10000, show a visible toast/banner (reuse the
   relay-warn banner pattern at app.js:887-925, amber not red):
   "Código no válido — verifica el número e inténtalo de nuevo." Then clear the mark.
   role:'director' also clears it.
3. Cancel on the native Alert sends nothing → the pending mark silently expires
   (correct: user chose to stay follower).
4. Later, native side, rides M3 typed-bridge work: add reason:
   'invalid-code'|'start-failed' to the role:'none' inject so the web can word the toast
   ("wrong code" vs "mesh couldn't start"). Also fix the false comment at
   PdfReaderApp.tsx:585.

Ship caveat: web-only reaches signovivo.com instantly but native devices only at the
next native build (bundled copy) or mesh bundle push — the audience that needs this IS
native, so include it in the next TestFlight/mesh-push.

### Acceptance criteria
- Native device, invalid 5+ digit code → visible Spanish error within ~1s; numpad reusable.
- Valid code + Cancel on the Alert → no error shown.
- Valid code + Confirm → badge appears, no error.
- Pure-web behavior unchanged (still flashes in the open modal path).

### Test idea
Unit-test the pending-window logic as a pure function (mirror svSyncDecision style);
e2e: call window.__signoVivoReceiveNativeEvent({type:'role', role:'none'}) inside and
outside the window and assert toast visibility.

---

## W2N-02 — DELTA on native-swift-stale-documents-bundle-masks-update / planned M7 rollback: the Reintentar ladder can never escape a broken Documents/WebBundle (bundleUri resolved once, never re-resolved)

Severity: high · Surface: native · Ship: native-build. Known-id:
native-swift-stale-documents-bundle-masks-update [OPEN-H]; planned work: major-update M7
"boot-watchdog auto-rollback to baked". Reporting ONLY the delta.

### Code walk
- resolveBundleUri (PdfReaderApp.tsx:813-826) prefers Documents/WebBundle/index.html
  unconditionally whenever it exists.
- It is called from exactly TWO places: boot (:837) and the mesh bundleUpdated event
  (:957). The bridge-watchdog remount path (:313-319) and the MANUAL Reintentar button
  (:1060-1067) only bump mountKey — the bundleUri state is REUSED as-is.
- Mesh OTA install validation (Swift) checks archive structure + index.html > 200B, but a
  bundle whose app.js is truncated/incompatible passes install yet never reaches
  bridge-ready.
- Result: corrupt-but-installed pushed bundle → watchdog fires at 6s → 2 remounts of the
  SAME uri → webDead → user taps Reintentar → same uri again → 6s → forever. The
  known-good SHIPPED bundle sits untouched next door. The copy ("La app no cargó bien.
  Toca para reintentar.") offers no escalation and the loop has no counter.

### User impact
A device stranded in a Reintentar loop mid-Mass with no user-recoverable path short of
deleting and reinstalling the app. This is the difference between "mesh OTA is risky"
(known) and "mesh OTA failure bricks the reader until manual intervention" (this delta).

### Fix approach (feed into M7's boot-watchdog rollback design)
1. Track consecutive bridge-ready failures per bundle uri (ref + AsyncStorage breadcrumb
   so it survives app restarts).
2. When the watchdog exhausts its remounts AND the current uri is the Documents copy:
   quarantine it (rename WebBundle → WebBundle.bad or delete), re-run resolveBundleUri(),
   and mount the shipped bundle — BEFORE showing Reintentar.
3. The manual Reintentar onPress must also re-run resolveBundleUri() rather than reusing
   stale state.
4. Copy upgrade: after ≥2 manual retries, append "Si sigue fallando, cierra la app por
   completo y ábrela de nuevo."

### Acceptance criteria
- Simulated broken Documents bundle (index.html present, app.js gutted): device
  auto-recovers to the shipped bundle within ~20s without user action; breadcrumb records
  the rollback.
- Healthy Documents bundle: behavior unchanged (still preferred).

### Test idea
Contract e2e pinning that resolveBundleUri is invoked from the retry/exhaustion paths
(source-grep pin like nearby-sync-contract); device test on the M7 2-device day.

---

## W2N-03 — Native resync (⟳) has no relay fallback: mesh down + internet up leaves native followers unrecoverable while every web phone syncs

Severity: high · Surface: native · Ship: native-build · Intersection: major-update M4
(sync-robustness remainder / status pill); complements, does not duplicate, P2-IDENTITY.

### Code walk
- Web deliberately disables relay-follow under the shell: startRelayFollow early-returns
  when hasNativeBridge() || NATIVE_FILE_MODE (app.js:3329); the ⟳ tap routes to
  postNativeBridge({type:'resync'}) instead (app.js:3088-3096).
- Native resync handler (PdfReaderApp.tsx:727-753) is MESH-ONLY: re-join if 'off' (:733),
  refreshNearbyDiscovery() + requestCurrentSnapshot() (:739-740), re-assert
  lastDirectorSnapshotRef (:742-750). No relay path — even though this same process has
  full relay reach (it PUBLISHES via directorRelaySync, RELAY_BASE at :45) and a
  read-only GET /r/alvernia-main/state costs one fetch.
- If Multipeer is impaired (BT/AWDL interference — the flakiness the whole M-F hardening
  wave exists for) while venue internet works, the parish iPads' ⟳ spins (fixed 1.1s
  theater, app.js:2416-2419) and recovers nothing, while congregants' phones on
  signovivo.com follow happily via relay.

### User impact
The parish iPads are the ONLY device class that cannot self-recover from mesh loss. At
Mass this reads as "the iPads are broken, the phones work" — with a placebo button.

### Fix approach
In the native resync handler (and optionally a slow background check while follower &&
not connected): after kicking the mesh refresh, fire a best-effort
fetch(RELAY_BASE + '/r/alvernia-main/state') with a short abort timeout; if the snapshot
is fresh (90s rule — logic already exists in web/src/lib/svSyncDecision.js; a simple
native check suffices), inject the page as a sync-event exactly like the mesh path (:749)
and update lastDirectorSnapshotRef. Guards: never while roleRef==='director' ||
explicitTransmitterRef; one-shot per tap (no polling loop — mesh stays primary; this is a
manual-recovery ramp, which also sidesteps two-transport fighting until M4 lands
transmitterId).

### Acceptance criteria
- Native follower, mesh dead, internet up, fresh relay snapshot: ⟳ lands the device on
  the director's page within ~2s.
- Mesh alive: unchanged (mesh answer wins; relay fetch redundant no-op).
- Director/transmitter devices never consume the relay snapshot.

### Test idea
Factor the handler's apply-snapshot into a testable unit; device test on M7 day
(BT off, wifi on).

---

## W2N-04 — becomeFollower failure wedge: role asserted before transport starts; a failed mesh start still tells the web "follower", and ⟳ can never repair it

Severity: medium · Surface: native · Ship: native-build

### Code walk
- becomeFollower (PdfReaderApp.tsx:419-450) sets roleRef.current="follower" at :423,
  BEFORE startNearbyFollower runs. If both start attempts throw (:436-443), the outer
  catch swallows (:445-447) and :449 still injects {type:'role', role:'follower'} — the
  web shows the follower layout as if all were well.
- The ⟳ repair path is gated the wrong way for this state: the resync handler re-runs
  becomeFollower only when roleRef==="off" (:733). Here roleRef is "follower", so it
  calls refreshNearbyDiscovery() — which NO-OPS in Swift when currentRole=="off"
  (guard self.currentRole != "off", ios/SignoVivo/DirectorSyncModule.swift:592), and
  Swift IS "off" because startFollower never completed. requestCurrentSnapshot →
  forceFollowerHelloNow with no transport → nothing.
- Trigger realism: Swift startFollower only rejects on an empty session code
  (DirectorSyncModule.swift:389-411), so the throw comes from the RN bridge layer
  (module wedge, early-boot race). Rare — but the retry-once design acknowledges it
  happens, and when it does the device is a permanent link-less "follower" recoverable
  only by the soft-reset code or an app kill.

### Fix approach
1. meshStartedRef set true only after startNearbyFollower resolves; cleared on
   reset/stop.
2. resync handler re-runs becomeFollower() when roleRef==="follower" &&
   !meshStartedRef.current (not just when "off").
3. Optionally, on total start failure emit a state-style sync-event so setSyncWorking
   shows "searching" honestly instead of implying a live link.

### Acceptance criteria
Force startNearbyFollower to reject twice (stub): a ⟳ tap afterwards re-attempts the
follower start; once it succeeds the device follows normally.

### Test idea
Unit-test the router with a mocked nearbyDirectorSync whose start rejects; assert the
subsequent resync calls startNearbyFollower again.

---

## W2N-05 — Transmitter-only exit-director never clears sv.sync.lastRole → false "Estabas dirigiendo" prompt after an intentional exit (H3/#267 follow-up gap)

Severity: medium · Surface: native · Ship: native-build · Intersection: direct follow-up
to PR #267 (H3); belongs on the M7 verify list.

### Code walk
- Transmitter path of becomeDirector persists lastSyncRole="director"
  (PdfReaderApp.tsx:472 — the H3 fix).
- Mesh-director exit routes through becomeFollower, which overwrites the key with
  "follower" (:432) — correct.
- The transmitter-only exit-director branch (:763-774) does NOT: it bumps the generation,
  stops the heartbeat, zeroes the refs, injects role:'none' — and leaves "director"
  sitting in AsyncStorage. (It also skips setRelayPublishCode(""), the C3 hygiene the
  becomeFollower path has — mostly harmless since the heartbeat is stopped, but an
  in-flight coalesced publish can still drain one authorized straggler frame after exit.)
- The boot bootstrap's own comment promises the opposite: "Intentional exit clears
  lastSyncRole, so this only fires after a crash/kill" (:868).

### Trigger / repro
Mesh-less device: enter code → confirm → exit via badge → confirm → later kill/relaunch
the app → "Estabas dirigiendo — La app se reinició…" fires though the exit was
deliberate.

### User impact
A misleading scary prompt that trains the operator to ignore it — eroding the exact
signal H3 shipped to prevent the frozen-web-congregation outage. Worst case a
non-director on a shared device re-enters a code because the app told them they "were
directing".

### Fix approach
In the transmitter exit branch add
AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower").catch(()=>{}) (mirroring
becomeFollower; or removeItem), plus setRelayPublishCode("") for C3 parity.

### Acceptance criteria
Transmitter enter→exit→relaunch: NO resume prompt. Transmitter enter→kill→relaunch:
prompt still fires.

### Test idea
Extend the #267 role-persistence contract e2e with the exit-then-relaunch sequence
against the transmitter branch.

---

## W2N-06 — DELTA on relay-no-transmitter-identity-two-publishers-ping-pong (M4): the "Ya hay un director activo" takeover warning is blind to relay-only directors

Severity: medium · Surface: native · Ship: native-build · Known-id:
relay-no-transmitter-identity-two-publishers-ping-pong [OPEN-H, planned M4] — this is its
unlisted user-facing angle at the code-entry moment.

### Code walk
onDirectorCode's live-director detection (PdfReaderApp.tsx:594-602) derives entirely from
lastDirectorSnapshotRef, which is set ONLY by mesh page events (:899). A director live
via the RELAY (transmitter-only device, or any director whose mesh doesn't reach this
iPad) produces no mesh snapshot here — so the confirm shows the benign "¿Dirigir el
coro?" (:606-607) instead of the red "⚠️ Ya hay un director activo" takeover warning, and
the new director walks straight into the M4 two-publisher ping-pong with the web
congregation. The dialog even asserts "Si otro director ya está activo, le quitarás el
control" (:612) — on the relay you DON'T cleanly take control (no tiebreak until M4); you
fight.

### Fix approach
Before showing the plain confirm, race a ~1.5s best-effort
GET RELAY_BASE/r/alvernia-main/state; if the snapshot is fresh (<90s), upgrade the dialog
to the live-director variant. Timeout/offline → current mesh-only heuristic (never block
code entry on the network). This is confirm-dialog copy-accuracy only; real arbitration
is M4's transmitterId (don't build tiebreak here).

### Acceptance criteria
Fresh relay snapshot from another publisher + no mesh signal: code entry shows the
destructive-styled takeover warning. Offline: dialogs behave exactly as today.

---

## W2N-07 — relay-auth-error is a one-shot latched EVENT delivered through a droppable queue: one cleared queue = the 401 warning is lost for the rest of Mass

Severity: medium · Surface: native · Ship: native-build · Intersection: M3 (typed/acked
bridge) and M4 (always-visible status pill) both subsume this; cheap standalone fix below.

### Code walk
- directorRelaySync.js latches the auth warning: fires the handler once per failure
  streak (authErrorNotified :34, :89-96), re-arming only on a later SUCCESS (:88) or a
  new code entry (:40). Every 401 during the latched streak is silent by design — the ONE
  fired event is load-bearing.
- Native forwards it via injectEvent({type:'relay-auth-error'}) (PdfReaderApp.tsx:
  342-347). If webReadyRef is false it lands in pendingInjectRef (:276-279), which is
  cleared WHOLESALE on watchdog remount (:315), soft reset (:558), bundleUpdated (:959),
  and content-process termination (:1092) — and can also age out of the 100-item
  drop-oldest cap (:266) under a mesh state-churn storm.
- Every other bridge-state message survives this because bridge-ready re-asserts it
  (role :669, page :674) — relay-auth-error is the only one-shot with no re-assert.

### Trigger / repro
Transmitter's WebView content-process dies (memory) → during the reload window the 12s
relay heartbeat publishes and 401s (rotated/retired code) → event queued → reload wedges
once → watchdog remount clears the queue → web boots, banner never shows, every
subsequent 401 is latched-silent. The director plays the whole Mass to a frozen
signovivo.com congregation — the precise failure the banner was built for.

### Fix approach
Treat auth failure as STATE, not an event: keep a relayAuthBrokenRef set by the handler
(:343), cleared on recovery (add a tiny success callback next to
setRelayAuthErrorHandler, or clear in setRelayPublishCode). In the bridge-ready case
(:638-696), if the ref is set and the device is a director authority, re-inject
{type:'relay-auth-error', status} after the role assert. Web side is already idempotent
(showRelayAuthWarning re-reveals without stacking, app.js:887-892).

### Acceptance criteria
Simulate: fire the auth handler while webReady false, then remount (queue cleared) — a
visible banner still appears after bridge-ready.

---

## W2N-08 — Mesh-less native "off" state wears follower clothes: dead ⟳ affordance + wrong Alert copy on transmitter-only devices

Severity: low · Surface: cross · Ship: native-build (copy fixes native; role mapping web)

### Code walk
- Transmitter-only exit drops to 'off' + injects role:'none' with the stated intent "the
  web shows no phantom follower UI" (PdfReaderApp.tsx:764-773).
- But the web has no third presentation: renderDirectorModeBadge maps everything
  non-director to data-role="follower" (app.js:845-852) — the device shows the full
  follower layout INCLUDING ⟳. Tapping it posts resync; native: roleRef==='off' &&
  !syncAvailable → no re-join, no mesh calls, no snapshot → nothing (:733-751). The fab
  still spins 1.1s (app.js:2416-2419). A placebo control on a device that by design has
  no follower transport.
- Related copy drift on the same device class: the becomeDirector confirm says "Los demás
  dispositivos seguirán tu página" (:611) — on transmitter-only, nearby iPads will NOT
  (no mesh); only web followers do. The exit confirm promises "volverás a seguidor"
  (app.js:2428) — it actually goes standalone-off.

### Fix approach
Add a real third state: native asserts role:'standalone' (additive; old web treats
unknown as follower — acceptable) or a followTransport:false flag on bridge-state; web
hides ⟳ for it. Adjust the two Alert strings on the !syncAvailable branch ("Los
seguidores en signovivo.com seguirán tu página" / "Dejarás de transmitir").

### Acceptance criteria
Transmitter-only device after exit shows no ⟳; mesh devices unchanged.

---

## W2N-09 — page-changed persists sv.book.lastPage.standard on every page turn "for restore" — but nothing anywhere reads it

Severity: low · Surface: native · Ship: native-build. Settles the prior-art "read-back
needs re-verify" oddity: verified WRITE-ONLY at HEAD.

### Code walk
PdfReaderApp.tsx:716-720 writes ${STORAGE_KEYS.lastPagePrefix}standard on every
page-changed (including every mesh-driven follower render). Repo-wide grep at HEAD: that
is the ONLY reference to lastPagePrefix besides its definition (src/offlineBooks.ts:23).
The comment "Persist per-book last page for restore" describes a restore that does not
exist — native boot (:829-846) never reads it, and the web always boots to
DEFAULT_START_PAGE (page 2).

### User impact
Debt + broken expectation: a device reopened mid-Mass shows page 2 until sync pulls it
home (followers recover via mesh; a standalone device just loses its place). One dead
AsyncStorage write per page turn on the hot path.

### Fix approach — pick one
(a) Implement the restore: on boot read the key and hand it to the web (best: include
page in the bridge-state inject and render it when no director is live; existing
bridge-ready ordering already lets a live director snapshot override it).
(b) Delete the write + key + comment.
Given the parish flow (devices are followers; the director gets the resume prompt), (b)
is the honest cheap option; (a) is nicer for standalone/practice.

---

## Parking lot (not findings)

- Duplicate broadcast after director reload: bridge-ready re-injects the director's page
  (:674-678) → web renders → posts page-changed same page → native re-broadcasts (no
  page-level de-dupe in the page-changed case). Harmless (followers de-dupe, relay
  coalesces); a one-line de-dupe in broadcastPage could ride the M4 transmitterId work.
- Pre-bridge-ready FOLLOWER page-changed adopts the web's boot render into currentPageRef
  and (dead-)persists it; benign today because bridge-ready re-syncs and the write is
  dead (W2N-09) — but any future lastPage restore must exclude pre-ready writes.
- NATIVE_FILE_MODE without a bridge (web/dist opened as plain file:// in a browser):
  relay-follow off AND postNativeBridge returns false → ⟳ fully dead. Nobody ships this.
- window.confirm dependence for exit-director (app.js:2428) relies on
  react-native-webview's WKUIDelegate confirm panel; node_modules absent in this worktree
  so unverified here, but the exit flow demonstrably worked in the 344-era fixes. Add to
  the M7 device checklist (a silent false would make the badge un-exitable).
- __SIGNO_VINO_INITIAL_BOOK injected (:1038) but never read web-side — dead global (also
  in the native map); delete with the next preloadScript touch.
- Telemetry role labels: dbgLog and fleet check-in report transmitter-directors as
  role 'off'/blank (:166, :195) — forensics/dashboard lens; already a map oddity.
- Fleet-label Alert.prompt vs resume-prompt vs code-confirm queueing at boot: iOS
  serializes RN alerts but ORDER is arbitrary on a slow boot; cosmetic.
- bridge-ready adopts msg.totalPages without the >0 guard that page-changed has
  (:655 vs :712) — covered by known-OPEN native-swift-bridge-ready-unclamped-total (M3
  coerce-and-clamp); do both fields there.
