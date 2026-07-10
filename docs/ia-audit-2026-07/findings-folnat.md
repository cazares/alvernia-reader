# Follower Experience — Native (FOLNAT) — audit lens detail file

> Lens: native FOLLOWER from cold launch to mirroring. HEAD `d5075091` (build **381**, version 1.0.4).
> All file:line anchors verified against CURRENT source at this HEAD (post-#269/#270/#271 — the
> "Buscando director…" pill is GONE; the sync-working signal is now the ⟳ fab's own 1.1s spin).
> Cross-checked against map-prior-art.md: nothing below re-reports a KNOWN-FINDING or HELD design
> (M-F2/M-F3/M-F6/C1/H2 respected); deltas name the known id in the title.
> (Note: intended path was findings-folnat.md; a tooling hook rejected that name — this file is the
> same content under lens-folnat.md.)

## Answers to the lens questions (context for the implementation doc)

- **Cold launch, no director:** Swift `startFollower` emits `searching` once (DirectorSyncModule.swift:408);
  web spins the ⟳ fab for a bounded 1.1s (app.js:861-878, #269/#271 "option A / fail-silent"), then a
  calm silent songbook. After 10s the self-directed timer fires (Swift:1197-1217) → status
  `self-directed` clears any spin. **No eternal spinner — this is now healthy.** No finding.
- **Mesh-vs-relay arbitration on a native follower:** there is none, by construction. The web relay is
  hard-disabled in the shell: `startRelayFollow` early-returns (app.js:3329), initReader's relay peek is
  skipped (app.js:3446), and the ⟳ tap is rerouted to a mesh-only native `resync` (app.js:3093-3095).
  The two sources cannot fight — but the mesh is the follower's ONLY transport (see FOLNAT-06).
- **Browse-away-then-return:** absent model — see FOLNAT-01 (headline finding).
- **Sleep/wake & background/foreground:** solid. expo `useKeepAwake` holds the screen
  (PdfReaderApp.tsx:86); background stops timers but keeps sessions (Swift:284-292); foreground re-arms
  per role with a fresh watchdog grace window + a one-shot transport relaunch if the failure counter
  maxed (Swift:294-341); JS foreground re-asserts the last snapshot + pulls a fresh one
  (PdfReaderApp.tsx:993-1019). No new finding.
- **Airplane-mode / no-wifi with the bundled book:** the book works fully offline (file:// bundle).
  Radios-off mesh failure is silent and indistinguishable from "no director" — folded into FOLNAT-02.
- **memoryWarning:** emitted by Swift (:271-278), silently dropped by JS — FOLNAT-07.
- **Takeover/handoff as seen by a follower:** followers re-target the highest token; the demoted
  sub-group blackout is HELD design M-F6 (not re-reported). The takeover ENTRY step has a silent
  failure mode — FOLNAT-03.

---

## FOLNAT-01 — Native follower has NO browse-away model: any local navigation is yanked back within ~1s, with full browse affordances still offered — HIGH

**Surface:** native (behavior lives in the shared web bundle + native heartbeat). **Ship:** web-only
(reaches iPads only at the next native build / mesh bundle push — say so in release notes).

### What the user experiences
Mid-Mass, a native follower (parish iPad) wants to peek at the next song or check a chord. The UI
offers three ways to browse: the ♪ jump modal, the left-edge-swipe drawer, and plain page swipes.
All of them work for exactly one heartbeat: within ~1 second the director's mesh heartbeat snaps the
page back. There is no amber pill, no "Volver a en vivo" bar, no way to opt into browsing, and no
indication of WHY the page moved. To an elderly congregant the songbook looks haunted; to anyone it
reads as a bug. Web followers on signovivo.com meanwhile DO get a sanctioned peek path (numpad jump
sets `relay.browsing`, shows the go-live bar, and the follow logic honors it).

### Exact code walk
1. Web renders any local navigation and unconditionally posts `page-changed`
   (web/src/app.js:1062-1067).
2. Native's `page-changed` handler updates `currentPageRef` for every role — the pre-bridge-ready
   ignore is director-only (PdfReaderApp.tsx:698-722, follower ref update at :709-711).
3. The director's mesh heartbeat re-sends its page every **1s** (PdfReaderApp.tsx:393-400).
4. The follower's mesh `page` listener de-dupes ONLY against `currentPageRef`
   (PdfReaderApp.tsx:903) — which now holds the browsed page — so the director's page is "new" and is
   injected (PdfReaderApp.tsx:911).
5. `applyNativeSyncEvent` renders it with **zero browsing check**: `renderPage(event.page, …)`
   (web/src/app.js:977-980). Compare the relay path, which consults `relay.browsing` via the
   unit-tested decision lib (app.js:3134-3143).
6. The one place browsing intent IS captured — the numpad jump (app.js:1200-1205) — is gated on
   `relay.hasDirector`, which is permanently `false` on native (relay disabled, app.js:3329), so the
   flag is never even set; and the mesh apply path would ignore it anyway.
7. `showGoLiveBar()` is also gated on `relay.hasDirector` (app.js:3068) — the bar can never appear
   on native.

Note the deliberate contrast with the web's own semantics after #263 F1: web followers are yanked in
~4s ONLY for un-flagged drift, and have a sanctioned browse mode. Native followers have neither.

### Prior-art check
`web-reader-browse-result-click-skips-relay-browsing-mode` (OPEN) is the WEB-relay half of this
class. The native/mesh half is unlisted anywhere. M4's planned tri-state pill is a status display —
it does not add a browse model.

### Proposed fix (web-only, mesh-aware browse mode)
- Introduce a transport-agnostic `browse` state in app.js (reuse `relay.browsing` or a new
  `syncBrowse` object) set by the same intentional-browse gestures the web honors (numpad jump off
  the live page; optionally drawer song taps), tracking `livePage` from EVERY applied sync source.
- In `applyNativeSyncEvent`'s `page` branch: always update `livePage` + `lastMeshPageAt`; if
  browsing, do NOT render — update the go-live affordance instead.
- Un-gate `showGoLiveBar()`/pill from `relay.hasDirector` by introducing
  `hasLiveDirector() = relay.hasDirector || meshDirectorFresh()` (mesh fresh = a mesh page/state
  event within ~8s, mirroring native's `LIVE_DIRECTOR_WINDOW_MS`).
- `goLive()` already renders `livePage` and clears browsing — it needs only the same un-gating.
- Native side needs NO change: when the web ignores an injected page it never posts `page-changed`,
  so `currentPageRef` stays on the browsed page and the 1s heartbeat keeps injecting — the web keeps
  ignoring it until go-live. (Cheap; events are already flowing. Optional polish: native could
  suppress re-injection, but it is not required for correctness.)
- Decide + document the un-flagged-drift rule for parity with web F1: a stray swipe (not via a
  flagged browse gesture) SHOULD still be yanked home — that is the intentional F1 semantic. So the
  fix is: flagged browse = honored + go-live bar; un-flagged drift = current yank behavior.

### Acceptance criteria
- Native follower, live director: numpad-jump to another song → page stays for ≥60s, go-live bar +
  amber affordance visible; heartbeats keep arriving (verify via /log `mesh:page-recv`).
- Tap "Volver a en vivo" → snaps to the director's CURRENT page (not the stale one at browse time).
- Director turns pages while follower browses → `livePage` tracks silently; go-live lands on the
  newest page.
- Stray swipe without the browse gesture → still re-homed by the next heartbeat (≤ ~1s).
- Web (signovivo.com) behavior unchanged (relay path untouched).

### Test idea
Extend the pure-decision pattern: extract the mesh-apply decision (browsing flag × page event) into
svSyncDecision or a sibling pure fn and unit-test: browsing+new-page → track-not-render;
not-browsing+new-page → render; go-live → render livePage. Plus a manual 2-device step on the M7
device day (this is bundled-web behavior, provable in one WebView with injected events too:
`window.__signoVivoReceiveNativeEvent({type:'sync-event',event:{type:'page',page:N}})` from Safari
inspector).

---

## FOLNAT-02 — Mesh start failure (Local Network denied / radios off) is invisible and indistinguishable from "no director": FOLLOWER_START_FAILED / DIRECTOR_START_FAILED are dropped by the JS listener — HIGH

**Surface:** native. **Ship:** native-build (the listener AND the bundled web copy ship together).

### What the user experiences
First launch, an elderly congregant taps "No permitir" on the iOS Local Network prompt (or ANY
launch with wifi+BT off / airplane mode). The app looks perfectly normal: songbook renders, ⟳ spins
1.1s when tapped, then silence. The device will NEVER sync, and nothing on any screen says why or
how to fix it (Settings → Privacidad → Red local). At Mass this reads as "her iPad is broken";
nobody can triage it without reading worker /log. The pre-Mass fleet dashboard shows the device
checked in (check-in is HTTP, unaffected), reinforcing false confidence.

### Exact code walk
- Swift emits the failure with explicit intent to surface it: `didNotStartBrowsingForPeers` →
  `emitError(code:"FOLLOWER_START_FAILED", …)` with the comment "Fires when Local Network permission
  is denied" (DirectorSyncModule.swift:1686-1691); advertiser twin `DIRECTOR_START_FAILED` with
  comment "Surface the failure to JS immediately so the director UI can warn the user"
  (Swift:1611-1616).
- The JS mesh listener's `error` case handles ONLY `DIRECTOR_CONFLICT`; every other code is
  telemetry-logged and dropped (PdfReaderApp.tsx:932-944; `dbgLog("mesh:error")` then nothing).
- The startup promise CANNOT catch this: `startFollower` resolves immediately
  (Swift:399-410) and the browser failure callback fires later/async, so `becomeFollower`'s
  2s-retry-once (PdfReaderApp.tsx:436-443) never sees permission denial.
- The last state the web ever saw is `searching` → a bounded 1.1s fab spin (app.js:863-878), then
  nothing. Identical presentation to "no director present".
- Regression context: the OLD native reader HAD a "director-mode error offers Settings deep link"
  behavior; its test was deleted as dead behavior when the WebView shell landed
  (e2e/permission-flow.test.mjs:82-86 comment block). Nothing replaced it.
- Swift's M-F7 45s-forever retry (Swift:1692-1704) and the foreground one-shot relaunch after the
  user fixes Settings (Swift:298-311) mean recovery WORKS — but only if the user somehow learns to
  open Settings, which nothing tells them.

### Prior-art check
`native-swift-onerror-no-recovery`, M-F7, and the permission-flow pins cover adjacent surface;
docs/audit-findings-raw.md:764 keeps the FOLLOWER_START_FAILED *emission* as a contract — no doc
anywhere covers the JS-side DROP or the missing user surface. M4's tri-state pill as designed
(heartbeat-freshness-driven, docs/major-update-2026-07.md:368-372) would show the SAME red
"Sin director" for this state — it cannot distinguish "can't sync" from "no director" without
consuming these error events, so this is a required input to M4, not a duplicate of it.

### Proposed fix
1. PdfReaderApp.tsx `error` case: on `FOLLOWER_START_FAILED`/`DIRECTOR_START_FAILED`, forward to the
   web: `injectEvent({type:"sync-event", event:{type:"error", code, message}})` (additive payload —
   old bundles ignore unknown event types via the try/catch whole-body guard, app.js:934).
2. Debounce/latch in native JS (one forward per role session, re-armed on a successful `connected`
   state) so the 45s retry loop doesn't spam.
3. Web: a dismissible banner (same pattern as `showRelayAuthWarning`, app.js:887-925):
   "Sin permiso de Red local — este iPad no puede seguir al director. Ajustes → Privacidad → Red
   local → Signo Vivo." Show only for followers (`data-role`), auto-hide on the next `connected`.
4. Optional native assist: `Linking.openSettings()` action button via a native Alert instead of (or
   in addition to) the web banner — restores the deleted Settings deep-link behavior.
5. Fold into M7's DIAGNÓSTICO screen as the permanent home; the banner is the interim floor.

### Acceptance criteria
- Deny Local Network on a fresh install → banner appears within ~10s (first browser-failure
  callback), names Settings path; ⟳ re-taps do not stack banners.
- Grant permission in Settings, foreground the app → transports relaunch (existing Swift:298-311),
  `connected` arrives, banner clears without user action.
- No director present but permission granted → NO banner (states distinguishable).

### Test idea
Unit-pin the JS forwarding (listener switch handles the two codes → injectEvent called once,
latched) with the existing source-regex style in e2e/nearby-sync-contract.test.mjs; device-verify
the end-to-end deny path on the M7 2-device day (add to its script — it currently has no
denied-permission step).

---

## FOLNAT-03 — Invalid director code on native is a silent no-op (modal closes, no feedback), while the code comments claim the web surfaces "código incorrecto" — HIGH

**Surface:** cross (native shell decision, web display). **Ship:** web-only (native reach at next
build; an optional native Alert fallback would be native-build).

### What the user experiences
The volunteer director types their 10-digit code on the numpad before Mass and fat-fingers one
digit. The modal closes and… nothing. No error, no confirm dialog, no role change. There is no way
to tell "code rejected" from "app didn't register the tap" from "I am now silently directing". The
correct-path signal is the appearance of the native confirm Alert — its ABSENCE is the only error
indicator. Same silence for a follower entering a code for takeover. This is the entry step of the
single most critical flow of every Mass.

### Exact code walk
- Web, native branch: 5+ digit input → `postNativeBridge({type:"director-code", code})` then
  `clearDraft(); closeSongJump()` — no pending-feedback state (app.js:1183-1186). The
  "Código no válido" flash exists ONLY in the pure-web else branch (app.js:1188-1189).
- Native: unrecognized code → `injectEvent({type:"role", role:"none"})` with the comment
  "Unrecognized → tell the web so it surfaces 'código incorrecto'" (PdfReaderApp.tsx:584-587) —
  a stale claim.
- Web `role` handler: stores the role and toggles the (already-hidden) director badge — no flash, no
  toast, nothing user-visible for `none` (app.js:947-956, renderDirectorModeBadge :845-852).

### Prior-art check
Nothing in the 117-finding index, the NEW-DIR set, or the sync audit covers rejected-code feedback;
`web-reader-draft-cap-blocks-legacy-11-digit-code` is about draft length, not feedback. New.

### Proposed fix (web-only)
- In `goToDraftSong`'s native branch, record `state.pendingDirectorCodeAt = Date.now()` before
  posting.
- In the `role` handler: if `payload.role === "none"` and a code was posted within the last ~5s,
  flash feedback. The numpad modal is closed by then, so use a transient toast (reuse the
  `flashSongDisplay` styling in a fixed-position toast, or re-open the modal pre-filled): "Código no
  válido — inténtalo de nuevo".
- Guard against false positives: `role:"none"` also arrives from transmitter exit-director
  (PdfReaderApp.tsx:773) and empty-code — the 5s pending window plus clearing the flag on any
  confirm-Alert-driven `role:"director"` handles both.
- Alternative/belt-and-suspenders (native-build): have `onDirectorCode` fire a lightweight
  `Alert.alert("Código no válido")` on rejection — zero web changes, but adds a native dialog; the
  web toast is the lower-friction primary fix.
- M3 (bridge v1, typed/acked) should carry an explicit `director-code-rejected` NACK instead of the
  overloaded `role:none` — note it in the M3 spec.

### Acceptance criteria
- Native device: enter a wrong 10-digit code → visible "Código no válido" within 1s; numpad
  re-entry path obvious.
- Enter a RIGHT code and cancel the confirm Alert → NO error toast (cancel ≠ rejection).
- Transmitter taps exit-director → NO error toast.

### Test idea
Unit-test the pending-window logic as a pure fn (posted-at, role-event, now → show/hide); manual
step in the pre-Mass checklist ("type a wrong code, expect the red flash").

---

## FOLNAT-04 — DELTA of `new-director-dead-writes-laststate-role-and-page`: a native follower restart always boots to page 2 — the persisted last page is write-only end to end — MEDIUM

**Surface:** cross. **Ship:** web-only (native reach at next build).

### What the user experiences (the unlisted user-facing angle)
Mid-Mass, a follower iPad's app restarts (crash, memory kill, accidental force-quit + relaunch). It
comes back on page 2 (the cover region), NOT where the congregation is, and stays there until the
mesh reconnects and the first director heartbeat lands (typically several seconds; up to tens of
seconds under discovery churn — and FOREVER if there is no live director, e.g. between songs at
practice, after which the user must re-navigate by hand). The known finding records the dead WRITE
as LOW code debt; this delta is the live-Mass symptom.

### Exact code walk
- Web always boots `state.currentPage = DEFAULT_START_PAGE` (= 2) with no persisted-page read
  (app.js:215, :3411, :3449); no localStorage page key exists (grep: only prefs/recientes/tip/
  haptic/offline-ready/fleet keys).
- Native dutifully persists `sv.book.lastPage.standard` on every page-changed
  (PdfReaderApp.tsx:717-720) and never reads it (offlineBooks.ts:23; no `getItem` of the prefix
  anywhere).
- `lastDirectorSnapshotRef` is in-memory only (PdfReaderApp.tsx:116) — an app restart loses it, so
  the bridge-ready follower-resync path (:679-695) has nothing to re-assert on first boot.

### Proposed fix (pick ONE owner; recommend the web)
Web-side restore, gated to native: on boot in `NATIVE_FILE_MODE`, read a
`localStorage["sv.page.last"]` written on every committed render, and use it instead of
`DEFAULT_START_PAGE` (app.js:3411/3449). WKWebView localStorage persists across app restarts for
file:// origins. A subsequently-arriving mesh snapshot still wins (H1/bridge-ready). Keep the web
PWA behavior unchanged (or adopt it there too — it only fills the gap before the relay peek). The
native `sv.book.lastPage.standard` write can then be deleted (finishes the known dead-write
cleanup). Alternative (native-build): inject the stored page via `preloadScript` as
`__SIGNO_VINO_INITIAL_PAGE` and read it in initReader — more moving parts, no added benefit.

### Acceptance criteria
- Native follower on page 250 → force-quit → relaunch offline (no director) → boots on 250.
- Relaunch WITH a live director on 260 → shows 250 momentarily then snaps to 260 (mesh wins).
- Fresh install → page 2 as today.

### Test idea
Pure unit for the boot-page pick (stored, valid-range, fallback) + an M7 device-day step.

---

## FOLNAT-05 — First-run friction: the native "¿Quién usa este iPad?" Alert.prompt survived #270's removal rationale and stacks with the Local Network permission prompt — LOW

**Surface:** native. **Ship:** native-build.

### What the user experiences
A freshly-installed/parish-reset iPad's first launch fires, back-to-back over the songbook: the iOS
Local Network permission dialog (from `primeNearbyPermissions()`, PdfReaderApp.tsx:851) and the
"¿Quién usa este iPad?" text-input prompt (PdfReaderApp.tsx:220-245) — two modal decisions before an
elderly user has seen a single page. #270 (2026-07-07) removed the identical web modal with the
rationale "more annoying than useful: choir members mostly tapped 'Ahora no'" — that rationale
applies verbatim to the native twin, which still ships. Worse timing: the dialog contention lands
exactly when the ONLY permission that matters (Local Network) needs the user's attention; a user
annoyed by prompt #2 is primed to dismiss/deny prompt #1 (the FOLNAT-02 trap).

### Exact code walk
- Native prompt: PdfReaderApp.tsx:216-246 (device-id effect, iOS-only, skippable → `sv_fleet_skip`).
- Permission prime fires from the mesh bootstrap effect on the same mount
  (PdfReaderApp.tsx:849-851).
- #270 commit 3db3a5ba explicitly scoped itself web-only ("Native iPads never showed this modal…
  web-PWA-only cleanup") — the native prompt was left by scope, not by decision, and no doc records
  choosing to keep it.

### Proposed fix
Remove the Alert.prompt (keep `fleetCheckin` anonymous, exactly like #270 did for web — the
dashboard already fills labels from the seeded roster for known devices, per the code comment at
PdfReaderApp.tsx:193-195). If a label path is still wanted, move it into M7's DIAGNÓSTICO screen as
a passive field. At minimum, defer it: don't prompt until after the first `connected` state or 24h,
so first-run has exactly one dialog (the permission).

### Acceptance criteria
Fresh install first launch shows ONLY the iOS Local Network dialog; fleet check-in still POSTs with
empty label; roster-matched devices still display named on the dashboard.

### Test idea
Source-pin (no `Alert.prompt` in the device-id effect) + dashboard smoke against a local wrangler.

---

## FOLNAT-06 — The mesh is a native follower's ONLY transport: total mesh failure never falls back to the relay the same iPad could reach — MEDIUM

**Surface:** native. **Ship:** multi (web decision logic + native state input; staging-room caveat).

### What the user experiences
Any native follower whose mesh path is broken — Local Network denied (FOLNAT-02), MPC radio wedge,
out of Multipeer range in a big/multi-room venue, or an iOS MPC regression — silently never syncs,
even when the device is on wifi with the relay reachable and carrying a live snapshot (any
director publishes to the relay: PdfReaderApp.tsx:362-368 — both mesh directors and transmitters).
The SAME iPad opened in Safari on signovivo.com would follow perfectly. The fleet is inconsistent:
the old-iPad web PWA follower has a more resilient transport than the flagship native app.

### Exact code walk
- Relay follow is hard-off in the shell: app.js:3329 (`startRelayFollow` early return), :3446
  (initReader peek skipped), :3093-3095 (⟳ rerouted to mesh-only native resync).
- Native's `resync` handler is mesh-only (PdfReaderApp.tsx:727-753).
- Prior art frames this gate as a SAFETY property ("relay fixes don't regress natives") and defers
  native staging-room entry to M7 — the gate itself is deliberate; the missing FALLBACK is the gap.
  It is unlisted as a finding; it intersects the open Q5 (P-MESH retire-vs-keep) decision directly.

### Why this is a real (bounded) gap, honestly framed
In-church iPads have no wifi, so the relay can't help there — the mesh remains primary and this
fallback is NOT a Mass-day silver bullet. It pays off at practice, at home, in permission-denied
recovery, and if Q5 ever retires the mesh it becomes the required baseline. It also creates the
FIRST possible mesh-vs-relay fight, so arbitration must ship with it (today none exists because the
relay is off).

### Proposed fix (design sketch — do NOT ship blind; pairs naturally with M4)
- Web-side: allow `startRelayFollow` in native when the mesh has been non-connected for >20s
  (native forwards mesh state already — track `lastMeshConnectedAt` from `sync-event` state events).
- Arbitration rule (single, explicit): a fresh mesh page/state (<8s) always wins and PAUSES relay
  apply; the relay applies only while mesh is stale — mirror it as a pure decision fn beside
  svSyncDecision so it's unit-tested (this answers "can the two fight" with a tested no).
- Respect the staging caveat: native has no `?env=` — the shell's relay room is `alvernia-main`
  only, so the follower fallback must use the same room resolution the shell trusts (constant), and
  M7's native staging entry should cover it.
- Battery note: keep the WS/poll machinery dormant until the mesh-stale trigger; tear down on mesh
  reconnect.

### Acceptance criteria
- Native follower with Local Network denied but wifi up: follows the director via relay within ~30s.
- Mesh reconnects → relay socket closes; mesh pages win immediately; no page ping-pong under a
  director publishing to both (heartbeat interleave test).
- Fully offline iPad: behavior unchanged (no relay attempts spamming timers — bounded backoff).

### Test idea
Extend the local wrangler harness (sync-worker/test) with a two-source decision unit suite:
sequences of (meshFresh, relaySnap) → applied source; plus an M7 device-day scenario (deny
permission on one iPad, confirm relay-follow, re-grant, confirm mesh takes back over).

---

## FOLNAT-07 — Swift `bundle-error` (~15 stages) and `memoryWarning` are silently dropped by the JS listener: a follower's failed mesh OTA has no surface anywhere — LOW

**Surface:** native. **Ship:** native-build.

### What the user experiences
During a mesh web-bundle push (director → followers), a follower whose 30MB transfer or install
fails sees nothing — not even telemetry beyond Swift's own dbgLog. The device silently keeps the old
bundle while the director believes the fleet updated; on the NEXT reconnect it re-downloads the
whole pack again (the re-download loop is pipeline-map O2 — not re-claimed here; this finding is the
missing surface/telemetry). Memory pressure (`memoryWarning`) is likewise never acted on — e.g. the
pending-inject queue and web-side caches are never trimmed pre-jetsam.

### Exact code walk
- JS listener `default: break` swallows `bundle-error`, `memoryWarning` (and the dead
  `takeover-approved/denied`) (PdfReaderApp.tsx:965-966).
- Swift emits `bundle-error` with a precise `stage` (timeout/pack/send/receive/header-*/
  archive-size-mismatch/index-too-small/swap-* …) exactly so failures are diagnosable
  (DirectorSyncModule.swift bundle install pipeline :879-1049; watchdog :737-748).
- `memoryWarning` emission: Swift:271-278.

### Proposed fix
Minimum (near-free): in the JS listener, `dbgLog("mesh:bundle-error", {stage})` and
`dbgLog("memoryWarning")` so /log timelines show them; breadcrumb on bundle-error. Better: forward
`bundle-error` to the DIRECTOR's UI in the M7 batch (the director is the actor who can retry /
decide), alongside the held M-F3 defer-install design and the planned sha256+signature work — the
follower stays silent by design (nothing actionable for a congregant). On `memoryWarning`: clear
`pendingInjectRef` overflow and skip the next heartbeat injections; optionally inject a web event so
the web can drop its in-memory page-image cache.

### Acceptance criteria
A forced bundle-install failure (corrupt archive in a dev build) produces a /log line with the stage
and a director-visible note; memoryWarning produces a /log breadcrumb.

### Test idea
Source-regex pin: listener switch contains cases for `bundle-error`/`memoryWarning` (mirrors the
nearby-sync-contract style).

---

## Parking lot (not findings — ideas / observations for later passes)

- **Two owners of the ⟳ fab's `is-spinning` class race:** the tap handler's own 1.1s timeout
  (app.js:2415-2418) and `setSyncWorking`'s timer (app.js:868-874) don't know about each other; a
  tap timeout can strip the class ~1s into a state-driven spin (and vice versa). Cosmetic —
  consolidate on `setSyncWorking` and have the tap call it.
- **"Se perdió el director" / `self-directed` ("Modo libre") messages are now invisible** — Swift
  still crafts them (DirectorSyncModule.swift:1679, :1213) but #271 removed the only display. The
  fail-silent choice was deliberate (#269 option A); M4's honest pill is the planned home. Just
  noting the Swift strings are currently dead weight on the wire.
- **`syncAvailable === false` on a real iPad (native module missing, e.g. a patch/no-op build):**
  bootstrap early-returns (PdfReaderApp.tsx:850), no role is ever injected, ⟳ becomes a total no-op
  — a completely sync-less songbook with zero signal. Root cause belongs to P5-PATCH-NOOP; a boot
  assert (`syncAvailable || breadcrumb+banner`) would be a cheap tripwire.
- **Wedged-director JS churn loop** (map-native oddity 10): follower-visible symptom post-#271 is a
  periodically re-spinning ⟳ fab + frozen page every ~4-5s. The fix (Swift-side director liveness
  independent of JS) belongs to the mesh/director lens; flagged here only for symptom attribution.
- **Fleet check-in waits behind the label prompt** (PdfReaderApp.tsx:216-251): first check-in is
  deferred until the user answers/skips; moot if FOLNAT-05's removal lands.
- **Follower drawer stays open while mesh pages render underneath** — harmless today; if FOLNAT-01's
  browse mode lands, opening the drawer should probably count as a browse gesture.
- **Native heartbeat comment drift** ("~2s" vs actual 1s, PdfReaderApp.tsx:68/:783 vs :400) —
  already in map-native oddity 7; FOLNAT-01's timing analysis used the real 1s.
