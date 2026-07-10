# FAILUX — Failure-mode UX audit (what the user actually sees)

> Lens: for each failure scenario, what does each surface DISPLAY, and can a non-technical
> Spanish-speaking user at Mass recover unaided?
> Verified at HEAD `d5075091` (build 381, post-#269/#270/#271). All file:line anchors are current.
> Dedupe base: map-prior-art.md KNOWN-FINDINGS + PLANNED-WORK (M0–M7, P1–P8). Every finding below
> is either absent from that list or an explicitly-named DELTA on a known/planned item.

Scenario coverage key (from the task): (a) relay unreachable at boot · (b) relay dies mid-Mass ·
(c) wifi-without-internet · (d) director killed → 90s demotion · (e) WebView crash floor ·
(f) missing/corrupt page image · (g) SW update mid-Mass · (h) storage unavailable ·
(i) relay 401 mid-Mass · (j) numpad jump to nonexistent song · (k) device clock badly wrong.

Scenarios traced and found ADEQUATELY handled or fully covered by known/planned work (NOT re-reported):
- (b) WS death → backoff → polling: sync genuinely continues via `/state` poll (app.js:3284, 3324,
  3364-3374); silence here is fine — the *pill* half is FAILUX-05.
- (d) web-follower 90s demotion (pill vanishes silently) and the native-follower "searching forever
  shows ~nothing" (post-#271 the spinner spins 1.1s per transition, app.js:861-878): both are exactly
  what M4's planned always-visible tri-state pill exists to fix (docs/major-update-2026-07.md:368-372)
  → only the DELTA not covered by that design is reported (FAILUX-05).
- (e) WebView crash: onContentProcessDidTerminate → reload → 6s watchdog → ≤2 remounts → native
  "Reintentar" floor (PdfReaderApp.tsx:1089-1094, 307-321, 1051-1073). Director heartbeats live in
  native timers and keep broadcasting the correct page through the whole ladder; on recovery A3/H1
  restore page + role. No new gap found (device verify owed = known M7).
- (g) SW deploy mid-Mass force-reload: known OPEN `offline-pwa-mid-mass-deploy-force-reload`,
  planned "update-ready chip" (major-update Ask 7). Traced, nothing new: controllerchange reload
  (app.js:2073-2084) + adaptive 3s→60s poll (app.js:2041-2058) will reload a visible tab ≤60s after
  deploy; post-reload the 1500ms relay peek re-homes the follower. No delta.
- (h) localStorage/IndexedDB unavailable: the module-eval reads are all guarded (#238) and IDB
  failures degrade (app.js:333-378, 386, 396-401, 606, 646). One unguarded latent read is in the
  parking lot (§PL-1).
- (k) follower-side clock skew: fixed (P2-CLOCKSKEW, deployed). The DIRECTOR-side clock hole is new
  → FAILUX-04.

---

## FAILUX-01 — Wrong director code on native = total silence (the promised "código incorrecto" surface does not exist)

**Severity:** high · **Surface:** cross · **shipVector:** multi

**Trigger / repro.** On a native iPad, open the ♪ numpad, type a 5+ digit code that is not in the
baked set (a typo of a real director number, or ANY code on a dev build whose committed
`director-codes.json` is empty), press Ir.

**Code walk.**
1. Web: `goToDraftSong()` sees 5+ digits + native bridge → posts `{type:"director-code", code}`,
   then **immediately** `clearDraft(); closeSongJump();` (web/src/app.js:1183-1186). The modal is
   gone before any verdict exists.
2. Native: `onDirectorCode` — `STANDARD_DIRECTOR_CODES.has(code)` false → comment says
   *"Unrecognized → tell the web so it surfaces 'código incorrecto'."* and injects
   `{type:"role", role:"none"}` (PdfReaderApp.tsx:584-587).
3. Web role handler: `applyNativeSyncEvent` type `"role"` only stores the role and toggles the tiny
   DIRECTOR badge (web/src/app.js:947-956). **There is no "código incorrecto" string anywhere in the
   web bundle** (`grep incorrecto web/src` → only the native comment matches; the only rejection
   flash, "Código no válido" at app.js:1189, is on the WEB-ONLY branch that never runs under the
   bridge).

**What the user sees:** modal closes, page unchanged, no badge, no message. Indistinguishable from
"it worked" minus the confirm dialog. The volunteer director retypes, or worse, assumes they are
directing. On a non-release build (empty baked set — known
`coherence-director-codes-two-hand-synced-stores`) EVERY code fails this silently, and this missing
surface is what makes that case undiagnosable in the field.

**Fix approach.**
- Native: on rejection inject a NEW additive event `{type:"director-code-rejected"}` (keep the
  legacy `role:none` for old bundles).
- Web: handle it in `applyNativeSyncEvent` → show a top-center toast reusing the relay-warn
  styling: **"Código incorrecto — inténtalo de nuevo"** (the modal is already closed; a toast beats
  re-opening it).
- Interim web-only heuristic (reaches iPads only at next native build anyway): remember
  `lastDirectorCodePostedAt`; if a `role:"none"` arrives within 3s of posting a director-code,
  flash the toast. Works against SHIPPED natives without a bridge change.
- Do NOT flash on every `role:"none"` unconditionally — it is also injected by exit-director
  (PdfReaderApp.tsx:773), becomeDirector's non-follower failure (:525), and empty-code (:573).

**Acceptance criteria.** Wrong code on native → visible Spanish rejection within 1s; correct code →
confirm Alert as today; exit-director produces no spurious "incorrecto".

**Test idea.** Bridge-contract e2e: simulate `role:none` within/outside the 3s window and assert
the toast; native unit: rejection path emits `director-code-rejected`.

**Major-update intersection:** M3 bridge v1 (typed/acked protocol) is the sanctioned home for a
proper NACK; this finding supplies the concrete UX requirement.

---

## FAILUX-02 — Confirmed "Sí, dirigir" can silently fail: mesh start failure demotes to follower with no error

**Severity:** high · **Surface:** native · **shipVector:** native-build

**Trigger / repro.** Enter a VALID code, tap "Sí, dirigir". `startNearbyDirector` throws twice
(permission race, radio warm-up, Bluetooth off, DIRECTOR_TAKEOVER_REQUIRED re-race).

**Code walk.** `becomeDirector` catch block (PdfReaderApp.tsx:516-527): if the device was a
follower → `becomeFollower()` (silent); else → `injectEvent({type:"role", role:"none"})` (silent,
see FAILUX-01: the web renders nothing for it). No `Alert.alert`, no toast. The operator has just
CONFIRMED a dialog that said "Los demás dispositivos seguirán tu página" — the app's last visible
statement is that they are now directing.

**What the user sees:** nothing. The only tell is the ABSENCE of the small DIRECTOR badge — a
negative signal a stressed volunteer at 6:55pm will not notice. They start Mass; no iPad follows;
the 2026-07-01 outage class recurs through a different door (NEW-DIR-1 covers app RESTART, not
failed promotion).

**Fix approach.** In the catch: `Alert.alert("No se pudo activar el modo director",
"Revisa que Bluetooth y la red local estén activados, y vuelve a intentar tu código.",
[{text:"Entendido"}])` — after the becomeFollower fallback so the device still re-joins the mesh.

**Acceptance criteria.** Force `startNearbyDirector` to reject twice → visible Alert; device ends
as connected follower; re-entering the code can still succeed.

**Test idea.** Unit-test the catch branch with a mocked rejecting `startNearbyDirector`; assert
Alert.alert called and becomeFollower invoked. Device-day: toggle Bluetooth off, enter code.

**Major-update intersection:** M7 native batch; the M4 director pill ("● Dirigiendo — N
conectados") is the structural fix, but the explicit error Alert is still needed at the moment of
failure.

---

## FAILUX-03 — Director-side relay publish failures (timeout / captive portal / 429 / 5xx) have zero surface — only 401/403 warns

**Severity:** high · **Surface:** native · **shipVector:** native-build

**Trigger / repro.** Scenario (c) on the TRANSMIT side: the director/transmitter device sits on
wifi that associates but doesn't route (captive portal, dead uplink), or the relay returns 429/5xx.

**Code walk.** `doPublish` (src/directorRelaySync.js:62-108):
- network throw / 7s abort → `catch {}` — silent by design (:97-99); on a DEAD uplink every
  re-publish also aborts, forever.
- `res.ok` false and not 401/403 (429, 5xx) → silently ignored (:89 condition).
- A2 rate-limit 429 is actually `200 {ok:true, rateLimited:true}` (sync-worker/src/index.ts:138-140)
  → counts as SUCCESS and even clears the auth latch (:88).
The only user-visible signal in the whole publish pipeline is the 401/403 banner
(PdfReaderApp.tsx:342-347 → app.js:887-925). The mesh keeps working, so the director sees iPads
following and reasonably believes everything is fine — while every phone on signovivo.com is frozen
for the rest of Mass.

**What the user sees:** nothing, anywhere, for the entire outage.

**Fix approach.** In directorRelaySync, count consecutive publish outcomes: 3 consecutive failures
(throw/abort/!ok, plus `rateLimited:true` and `ignored:true` bodies) → fire a NEW handler
`setRelayPublishHealthHandler("down")`; first success → `"up"`. Native forwards as additive
`{type:"relay-health", state}`; web shows a dismissible amber variant of `#sv-relay-warn`:
**"Sin conexión con el internet — los teléfonos en signovivo.com no están siguiendo. Los iPads sí
siguen."**, auto-hiding on `"up"`. (Consuming `ignored` here also gives the known-OPEN
`coherence-ignored-publish-response-never-consumed` its first surface.)

**Acceptance criteria.** Kill the transmitter's uplink (keep mesh) → banner within ~30s; restore →
clears on next successful publish; a 1-2-failure transient flap shows nothing.

**Test idea.** Node unit test of directorRelaySync with mocked fetch failing N times; assert
handler firing/clearing thresholds.

**Major-update intersection:** M4's director pill covers mesh `peerCount` only — the plan has no
relay-publish-health surface; fold this in as the director half of M4's "loud honest status".

---

## FAILUX-04 — Director clock >60s fast: worker collapses every publish to seq=0 → web congregation gets ~1 page per 90s, silently (scenario k)

**Severity:** high · **Surface:** worker · **shipVector:** worker-only

**Trigger / repro.** The transmitter device's clock is >60s ahead (old iPad, manual date, dead
NTP). Native seq = wall-clock ms (`nextSeq()` = `Math.max(seq+1, Date.now())`,
src/directorRelaySync.js:56-59).

**Code walk (sync-worker/src/index.ts).**
1. Sanitize: `incomingSeq > Date.now() + 60000` → collapsed to `0` (:146-149). A fast clock trips
   this on EVERY publish.
2. First publish of the evening: room stale (>90s) → `snapshotStale` true (:156-158) → guards
   skipped → accepted with server-assigned `seq = snapshot.seq + 1`, `ts = now` (:171-179). Room now
   FRESH.
3. Every subsequent publish (page turns AND the 12s heartbeat): collapsed to 0 again, room fresh →
   `!snapshotStale && incomingSeq === 0` → `{ok:true, ignored:true}` (:165-167). Ignored publishes
   never refresh `ts`, so the room ages out at ts+90s, the NEXT publish is accepted… cycle repeats.
**Net effect:** web followers get roughly one page update per ~90-102s, with the pill flapping
(demote at 90s, re-promote on the next accepted publish). Native mesh unaffected, so nothing looks
wrong to the director. Responses are all `{ok:true}` so even a future `ignored` consumer sees one
"clean" publish per cycle. No surface names the cause; the fleet dashboard has no clock column.

**Fix approach (worker).** Don't punish implausibly-FUTURE seqs with the ignore path: when the
sanitize clause trips on the too-high side, assign `seq = this.snapshot.seq + 1` **while still
applying the publish** (monotonicity preserved via the server counter; out-of-order STALE low seqs
are still ignored by the `<= snapshot.seq` guard). Optionally echo additive `{clockSkewed:true}` so
FAILUX-03's health counter can surface it to the director.

**Acceptance criteria.** Local wrangler harness: publisher whose seqs are `Date.now()+10min` →
every publish applies, `/state.page` tracks each turn, stored seq strictly increases; replaying an
OLD low seq while fresh is still ignored.

**Test idea.** Extend `sync-worker/test/a2.test.mjs` with a skewed-clock publisher sequence.

**Major-update intersection:** M4 P2-IDENTITY redesigns publisher arbitration (transmitterId);
fold this rule in there — but the worker-only fix is shippable now and independent.

---

## FAILUX-05 — DELTA on planned M4 pill: today the pill lies green through a TOTAL network loss, and the planned tri-state copy still conflates "sin conexión" with "sin director" (scenarios a, b, c, d)

**Severity:** high · **Surface:** web · **shipVector:** web-only

**Named planned item:** M4 "always-visible tri-state status pill" + 4s freshness-decay timer
(docs/major-update-2026-07.md:147, 368-372) — NOT started. Reported here: only what that design
does not yet state, plus the current-behavior trace the implementer needs.

**Code walk (current behavior).** `relay.hasDirector` is mutated ONLY when a snapshot is RECEIVED
(app.js:3123/3126/3149 via svSyncDecision.js:88-104). Every watchdog (F1-F3, health timer
:3364-3374) reconnects and polls but never touches the pill; `relayPollOnce`'s catch is empty
(:3209). So on wifi-dies / captive-portal / relay-down: no data arrives → no demotion → the 8px
pill keeps pulsing GREEN ("En vivo con el director", :3040-3041) on a frozen page for the rest of
Mass. At boot with the relay unreachable there is no pill at all (`!relay.hasDirector` →
`display:none`, :3038) — identical display to "director simply hasn't started", though the correct
user actions differ (fix the wifi vs wait).

**The DELTA the M4 design must add.**
1. The planned red state's copy is "○ Sin director — modo manual". A follower on dead wifi would
   read that as "the director stopped" and wait. Add a FOURTH distinguishable state (or distinct
   red copy) when the CLIENT cannot reach the relay (WS not OPEN AND last successful /state older
   than the decay window): **"○ Sin conexión — revisa el wifi"** vs **"○ El director no está
   transmitiendo"**. The client can already tell these apart (`relay.lastMsgAt` + tracking the last
   successful `relayPollOnce`).
2. The planned "4s freshness-decay timer" is described as making demotion reachable "on a healthy
   socket"; specify that it is driven by wall-time since last APPLIED snapshot so it also fires
   when NOTHING arrives (dead network) — otherwise the frozen-green bug survives M4.

**Acceptance criteria.** With a live director: kill the follower's uplink → within ~10s the pill
leaves green and shows the sin-conexión state; restore uplink → green within one poll/heartbeat.
Relay reachable + no publisher → sin-director state. Never green without a fresh applied snapshot.

**Test idea.** New pure lib (svPillState) unit-tested over (lastAppliedAt, wsState, lastPollOkAt) →
pill state, following the svSyncDecision pattern.

**Major-update intersection:** IS M4 Ask-3 — spec correction + current-behavior trace.

---

## FAILUX-06 — After a failed/timed-out page image, NOTHING can repaint the current page: ⟳, "Volver a en vivo", and force-resyncs all skip same-page renders (delta on P7-TIMEOUT-COMMIT / P7-IMG-RETRY)

**Severity:** medium · **Surface:** web · **shipVector:** web-only

**Named known items:** `perf-timeout-commit-hides-loading-indicator` (OPEN) and
`web-reader-live-img-error-retry` (OPEN) cover the commit-on-timeout and retry-race halves.
**Unlisted angle reported here: the recovery dead-end.**

**Code walk.**
1. On a hung network, `preloadImage` resolves `"timeout"` after 3s (app.js:1011) → renderPage
   commits the unloaded src, sets `state.currentPage = nextPage`, posts `page-changed` "success"
   (:1056-1067). The `<img>` error handler retries 4× with `?retry=N` (:70-77) — all fail during
   the outage; after that nothing retries again, and the src carries `?retry=4`.
2. Network returns. Every resync path refuses to re-render the CURRENT page: `decideRelaySnapshot`
   → `renderPage = ctx.currentPage !== snap.page ? snap.page : undefined` even under `force`
   (svSyncDecision.js:130); `goLive()` → same-page skip (:3079); F1 re-home fires only when pages
   DIFFER (:3291); the inline fallback likewise (:3129).
3. The follower shows a blank/stale image under a green pill; ⟳ spins and fixes the socket but not
   the pixels. The page repairs only when the DIRECTOR moves — a full hymn away — or if the user
   discovers "swipe away and back" (`pageImageMatches` fails on the `?retry=4` src :268-271, so
   re-entry does reload). Not a recoverable path for this congregation.

**Fix approach.** Add an image-health check to the force paths: in `goLive()` and in
`applyRelaySnapshot` when `force===true` and `currentPage === snap.page`, re-render anyway if
`!pageImage.complete || pageImage.naturalWidth === 0` (`isPageImageHealthy()` predicate), routed
through renderPage's stale-request guard. Optionally re-arm the `<img>` retry budget on `online`.

**Acceptance criteria.** Load page N with fetch blackholed → blank commit → restore network → tap ⟳
(or the amber dot) → page repaints without the director moving. Fast page-turns still never flash
an older page (requestId guard intact).

**Test idea.** Browser-driven offline→online toggle asserting `naturalWidth > 0` after ⟳; unit-test
the extracted force-path predicate.

**Major-update intersection:** none (P7 batch).

---

## FAILUX-07 — ⟳ resync gives identical fake-success feedback whether it worked or the network is dead

**Severity:** medium · **Surface:** web · **shipVector:** web-only

**Code walk.** The ⟳ fab's spin is purely cosmetic: `classList.add("is-spinning")` +
`setTimeout(remove, 1100)` (app.js:2416-2419), started BEFORE `reconnectRelay()` and independent of
any outcome. `reconnectRelay` (:3088-3107) fires connectRelay + a forced poll whose failures are
swallowed (:3209). On the native path it posts `{type:"resync"}` and equally learns nothing. During
an outage the follower's one recovery affordance animates convincingly, changes nothing, and
reports nothing — teaching them the button "does nothing" exactly when trust in it matters.

**Fix approach.** Make the spin outcome-driven with a bounded wait: keep spinning until either a
snapshot is successfully applied (`relayPollOnce` resolves ok) → stop + brief success state, or 4s
elapse → stop + small toast **"No hay conexión — revisa el wifi"**. Native: key off the next
`sync-event` within the window. Complements (doesn't depend on) the M4 pill.

**Acceptance criteria.** ⟳ with relay reachable → spin ends on apply (<4s), no toast. ⟳ with
network dead → spin ends at 4s + Spanish toast. No stacking on rapid taps.

**Test idea.** Wrap the outcome-wait in a pure promise/timeout helper and unit-test both races;
browser test with offline toggle.

**Major-update intersection:** M4 (status honesty family) — independently shippable.

---

## FAILUX-08 — Numpad jump to a nonexistent song silently lands on the LAST page (scenario j)

**Severity:** medium · **Surface:** web · **shipVector:** web-only

**Code walk.** `resolveSongPage` (app.js:700-709): no exact match → `songIndex.find(entry =>
entry.song >= normalized)`; a number beyond the last song finds nothing → returns
`state.totalPages`. `goToDraftSong` (:1193-1196) renders that page AND records the garbage number
in Recientes (`addToRecientes(songNumber)`). A follower typing a 2-4 digit typo past the last canto
is teleported to the book's final page with zero feedback — mid-Mass this reads as "the app jumped
somewhere random" — and is now ALSO off-live in browse mode (:1200-1205). An all-zeros draft
returns null and the modal closes silently (:1173-1174).

**Fix approach.** In `goToDraftSong`, before rendering: if the number exceeds the last indexed song
(or both lookup and find miss), keep the modal open and
`flashSongDisplay("No existe el canto NNN", "err")` (flash infra already in the modal :831-841).
Same flash for the zeros case. Only `addToRecientes` on an exact `songPageLookup` hit. Keep the
in-gap next-song fallback within range.

**Acceptance criteria.** Number greater than the last canto → modal stays open + red flash, page
unchanged, Recientes unchanged; real and in-gap numbers behave as today; 5+ digit codes unaffected.

**Test idea.** Extract a pure `resolveSongJump(draft, songIndex, totalPages)` verdict helper and
unit-test boundaries (0, 1, last, last+1, gap, 5-digit).

**Major-update intersection:** none.

---

## FAILUX-09 — The 401 relay-auth banner tells the director what broke but not what to DO, and never re-shows after dismissal while the failure persists (scenario i)

**Severity:** medium · **Surface:** cross · **shipVector:** multi

**Code walk.** Banner copy: "El relé rechazó el código de director. Los seguidores en signovivo.com
NO están sincronizados." (app.js:912) — accurate, but a volunteer has no idea the fix is "exit
director mode and re-enter a current code" (the code is memory-held: directorRelaySync.js:19,36-41).
The × dismisses it (app.js:918) and the native latch (`authErrorNotified`,
directorRelaySync.js:34,89-90) re-arms ONLY on a successful publish or a fresh code entry — with a
retired code neither ever happens, so after one dismissal the outage is signal-free for the rest of
Mass. The one-shot latch was deliberate (anti-spam); the gap is dismiss-vs-persistent-failure.

**Fix approach.**
- Copy (web, instant): append the action — "…NO están sincronizados. **Sal del modo director (toca
  la insignia DIRECTOR) y vuelve a entrar con un código vigente.**"
- Persistence: web-only — on ×, schedule a re-reveal in 60s unless a role change occurred; or
  native — re-fire the handler every Nth consecutive 401 (e.g. 25th) instead of a pure latch.

**Acceptance criteria.** Retired code mid-Mass → banner; dismiss → returns ≤60s while publishes
still 401; good code entered → never returns; normal page turns never show it.

**Test idea.** Unit: directorRelaySync every-Nth-401 counter with mocked fetch; web: re-reveal
timer with simulated events.

**Major-update intersection:** M4 status pill should absorb the persistent-state half (red director
pill = durable signal; banner = interrupt).

---

## FAILUX-10 — Boot-failure path shows a message with no retry affordance

**Severity:** low · **Surface:** web · **shipVector:** web-only

**Code walk.** `initReader().catch(...)` → `setLoading(true, "No se pudo cargar Signo Vivo.")` +
`revealReader()` (app.js:3523-3527). That renders the small `#loading` text div (index.html:59) —
no button, no instruction. The bootGuard's crash card DOES have a big "Reintentar" button
(app.js:36-38), but because initReader handled its own rejection and revealReader set `__svBooted`
(:59-66), the guard never takes over. The user's only recovery is knowing to pull-to-refresh /
relaunch — not zero-training.

**Fix approach.** In that catch, call the existing recovery card directly:
`window.__svRecover && window.__svRecover("init-reader", error)` BEFORE `revealReader()` (the card
early-returns once `__svBooted` is set), and skip revealReader on this path.

**Acceptance criteria.** Force initReader to reject → the "Signo Vivo se está recuperando /
Reintentar" card appears and reload works; normal boot unaffected.

**Test idea.** jsdom boot with poisoned `#pages-data` + failing fetch double; assert card presence.

**Major-update intersection:** none (M2 family polish).

---

## Parking lot (not findings — ideas / low-confidence / out-of-lens)

- **PL-1** `sessionStorage.getItem`/`removeItem` inside the `controllerchange` handler are unguarded
  (app.js:2077, 2082) — in a block-all-cookies browser they'd throw inside the listener; post-boot
  it's record-only, worst case a missed update-reload. Guard for symmetry with #238.
- **PL-2** `fleetDeviceId()`'s catch mints a NEW `anon-…` id per call (app.js:2876-2878) — a
  storage-blocked device shows up as a fresh ghost device on every check-in, inflating the fleet
  dashboard. Cache it in a module `let`.
- **PL-3** The native shell still carries the one-time "¿Quién usa este iPad?" `Alert.prompt`
  (PdfReaderApp.tsx:216-246) although #270's stated intent was removing the self-ID modal
  "entirely" (web side removed). Decide: prune native too, or document the asymmetry.
- **PL-4** Post-#271 the native "working" indicator is a 1.1s spin per state TRANSITION
  (app.js:861-878) — a mesh follower stuck in `searching` for minutes shows nothing after the first
  1.1s. Deliberate simplification; revisit when the M4 pill lands (wire `searching` to amber).
- **PL-5** Native follower whose page render keeps failing gets a 1s retry churn (mesh heartbeat ×
  render-failed sentinel) with the loading overlay flashing — consider calming the visual after N
  failures (message stays, spinner backs off).
- **PL-6** `initReader` boots on page 2 then snaps to the director when the 1500ms relay peek loses
  the race (app.js:3447-3449) — brief wrong-page flash at boot (map-web O15). Cosmetic.
- **PL-7** Keyboard arrows move by SONG while swipes move by PAGE (app.js:2823-2824 vs :2735) —
  external-keyboard iPad users skip pages unexpectedly. Rarely hit at Mass.
