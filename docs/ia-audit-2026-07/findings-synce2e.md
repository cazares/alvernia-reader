# SYNCE2E — Sync semantics end-to-end (both transports, both directions)

> Audit lens: full sync graph — native director → mesh + relay → web/native followers.
> Verified at HEAD `d5075091` (build 381). All file:line anchors re-checked against current source.
> Prior-art dedupe: map-prior-art.md consulted; nothing below re-reports a KNOWN-FINDINGS row or
> PLANNED-WORK item except where explicitly framed as a DELTA (SYNCE2E-02, SYNCE2E-09).

Ground-truth answers to the lens's standing questions (established from source, cited in findings):

- **Does a native follower consume BOTH mesh and relay?** NO. `startRelayFollow` early-returns in the
  native shell (`web/src/app.js:3329` — `if (hasNativeBridge() || NATIVE_FILE_MODE) return;`), and the
  native shell itself has no relay subscriber (`src/directorRelaySync.js` is publish-only). Native
  followers are mesh-only; there is no mesh-vs-relay conflict to resolve on native because the relay
  source simply doesn't exist there (→ SYNCE2E-08).
- **Publisher `ts` is server-stamped** (`sync-worker/src/index.ts:178`), so a skewed director clock
  cannot make the room look stale. But publisher `seq` is device wall-clock ms
  (`src/directorRelaySync.js:56-59`) and the worker's A2 sanitizer turns a fast clock into a rolling
  freeze (→ SYNCE2E-01).
- **DO cold-start/eviction does NOT reset seq** — snapshot is `storage.put` before broadcast
  (index.ts:180-184) and restored in the constructor under `blockConcurrencyWhile` (index.ts:121-128).
  No stranded-follower window from eviction.
- **Backgrounded director**: RN timers freeze; web followers demote at 90s (planned M4 pill covers the
  follower surface); on foreground the native shell immediately re-broadcasts
  (`PdfReaderApp.tsx:1010-1016`), so recovery is push-driven, not 12s-heartbeat-bound. Residual
  operator-awareness gap folded into the parking lot (M4 already plans the director-side pill).

---

## SYNCE2E-01 — Fast-clock director: A2 seq sanitizer freezes web followers to one page turn per ~90s (HIGH, worker-only)

**Trigger / repro.** Any authorized publisher whose device clock is >60s FAST (manual clock, broken
NTP, timezone fiddling on a parish iPad).

1. Device clock = server + 5 min. Room idle (stale).
2. First publish: `seq = Date.now()` (device) = serverNow+300s. Worker sanitizer
   (`sync-worker/src/index.ts:146-149`): `incomingSeq > Date.now() + 60000` → collapsed to **0**.
   Room is stale (`index.ts:156-158`), so the seq-0 gate is skipped and the publish is ACCEPTED with
   `seq = this.snapshot.seq + 1` (index.ts:177), `ts` = server now (index.ts:178). Room is now FRESH.
3. Every subsequent publish (every page turn + the 12s heartbeat,
   `PdfReaderApp.tsx:401-412`) is again sanitized to 0, and now hits the A2 seq-0 gate:
   `if (!snapshotStale && incomingSeq === 0) return { ok: true, ignored: true }` (index.ts:165-167).
   `ts` is only updated on accepted publishes, so the room stays "fresh" on the FIRST page until
   `nowSec - ts > 90` (index.ts:157-158).
4. At ~90s the room goes stale; the next heartbeat is accepted (one page update), re-freshens the
   room, and the cycle repeats.

**Observed behavior**: signovivo.com followers advance ONE page every ~90-102s while the director
turns freely; mesh iPads follow normally (mesh has no seq sanitizer) — silent web/native divergence
mid-Mass. The relay pill mostly stays green (fresh window), flickering demote/re-promote at each 90s
boundary. The director sees NOTHING: every rejected publish returns HTTP 200 `{ok:true,ignored:true}`,
which `directorRelaySync.doPublish` counts as success (`src/directorRelaySync.js:87-88` checks only
`res.ok`; the `ignored` field is never read — that non-consumption is known-OPEN P2-IDENTITY/M4, but
the worker-side trigger via clock skew is new).

**Code walk (exact):**
- `src/directorRelaySync.js:56-59` — `seqCounter = Math.max(seqCounter + 1, Date.now())` (device clock).
- `sync-worker/src/index.ts:147` — `incomingSeq > Date.now() + 60000 → incomingSeq = 0` (server clock).
- `index.ts:165-167` — fresh + seq 0 → `{ok:true, ignored:true}` (A2 gate, added #246).
- `index.ts:177-178` — stale-accept assigns `snapshot.seq + 1` and server `ts`.

**Related (folded in, not a separate finding):** a SLOW-clock replacement director (handoff to a
backup device whose clock lags) publishes `seq < snapshot.seq` and is `ignored` by the monotonic
guard (index.ts:168-170) until the dead director's snapshot ages out — up to ~90s of silent dead
air on the web surface while mesh iPads already follow the new director. That direction is bounded
and inherent to invariant #3 ("regressed seq accepted only after staleness"); the durable fix is
planned M4 transmitterId. The FAST direction above is unbounded (persists for the whole Mass) and
fixable worker-side today.

**Proposed fix.** In `publish()`, CLAMP an unreachably-high finite seq instead of zeroing it:

```ts
if (!Number.isFinite(incomingSeq) || incomingSeq < 0) incomingSeq = 0;
else if (incomingSeq > Date.now() + 60000) {
  // Fast-clock publisher: clamp to a sane monotonic value instead of collapsing to 0 —
  // zeroing turned every subsequent publish into an ignored "override attempt" (rolling freeze).
  incomingSeq = Math.max(this.snapshot.seq + 1, Date.now());
}
```

This preserves both A2 intents: Infinity/NaN/negative still collapse to 0 (poison guard), and the
room seq can never exceed serverNow+60s by more than one step, so a later honest director
(seq = serverNow wall-clock) is never blocked longer than today. Because each accepted publish now
updates `ts`, freshness works normally and page turns flow 1:1. Additive-only: no wire change, no
client change.

**Acceptance criteria.**
- Publisher sending seq = serverNow + 300_000 ms: first publish accepted; SECOND publish (higher
  device seq) also accepted with a page change visible in `/state` within one round-trip.
- Publisher sending seq = Infinity / -5 / NaN while a fresh director is live: still `ignored:true`
  (A2 regression guard).
- Existing a2.test.mjs cases stay green.

**Test idea.** Extend `sync-worker/test/a2.test.mjs` (local wrangler only): publish with
`seq: Date.now() + 300000`, then `seq: Date.now() + 301000` with a new page; assert `/state.page`
reflects the second publish immediately (today it does not until 90s pass).

---

## SYNCE2E-02 — DELTA on H3/#267 (relay-transmitter-only-role-lost-on-relaunch, marked FIXED-DG): the restore prompt is gated behind `syncAvailable`, so the fix cannot fire on the exact device class it targets (HIGH, native-build)

**Claimed fix (PR #267):** a transmitter-only director (`syncAvailable === false`) persists
`lastSyncRole = "director"` (`PdfReaderApp.tsx:461-476`, persist at :472) "so the existing boot
resume prompt fires" after a native restart.

**The bug:** the ONLY reader of `lastSyncRole` — the "Estabas dirigiendo" boot prompt
(`PdfReaderApp.tsx:869-882`) — lives inside the role-bootstrap effect that opens with:

```ts
useEffect(() => {
  if (!syncAvailable) return;   // PdfReaderApp.tsx:849-850
```

`syncAvailable = Platform.OS === "ios" && Boolean(nativeModule)` (`src/nearbyDirectorSync.js:6`,
memoized at `PdfReaderApp.tsx:128`). A transmitter-only device is BY DEFINITION `!syncAvailable`
(the transmitter branch in becomeDirector is only reachable at :461 `if (!syncAvailable)`). So on
the exact device class H3 protects, the effect early-returns and the prompt never fires: after a
crash/memory-kill the device boots as a silent nothing (the bootstrap `becomeFollower()` at :881
also never runs), the relay heartbeat never restarts, and every signovivo.com follower stays frozen
with no 401 signal — the original H3 outage, unchanged. The persist at :472 is a write with no
reachable reader.

**Secondary consequence:** M7's planned device-verify for H3 ("transmitter-direct, force-quit,
relaunch → expect prompt") cannot even be exercised on mesh-capable TestFlight iPads — on those,
`syncAvailable` is true and the transmitter branch is unreachable, so the verify would silently
test the mesh-director path instead and pass for the wrong reason.

**Practical exposure honestly stated:** production TestFlight builds ship the Swift module, so
`syncAvailable` is true fleet-wide today; `!syncAvailable` arises only if the module ever fails to
register (bad build, patch regression, non-iOS). But that is precisely the world H3 was written
for, and prior art rates it HIGH with status FIXED-DG — the status is wrong.

**Proposed fix.** Move the one-shot role-restore bootstrap OUT of the mesh-gated effect (or split
the gate): run the `lastSyncRole` read + prompt + `becomeFollower()` unconditionally at boot;
keep only `primeNearbyPermissions()` and the mesh event listener behind `syncAvailable`.
`becomeFollower` already internally gates its mesh start on `syncAvailable` (:434), so it is safe
to call on a mesh-less device.

**Acceptance criteria.** With the native module stubbed out (force `nativeModule = null` in a dev
build): enter a director code (transmitter path), force-quit, relaunch → "Estabas dirigiendo"
prompt appears. On a normal mesh build: behavior unchanged (prompt after director kill; no prompt
after clean exit).

**Test idea.** The bootstrap is a hook; minimally, an e2e source-contract test asserting the
`lastSyncRole` read is NOT inside the `if (!syncAvailable) return;` effect (regex-pin the restore
block appears before/outside the gate), until a proper RN test harness exists.

---

## SYNCE2E-03 — Native mesh follower cannot browse: the ♪ jump is yanked back within ~1s, with no browse mode and no "Volver a en vivo" — opposite of the web follower contract (HIGH, web-only*)

**Trigger / repro.** Parish iPad, follower, director live on the mesh. Follower taps ♪, jumps to
song N to peek at the next hymn. Within ~1 second the page snaps back to the director's page. Every
retry does the same. No bar, no pill, no explanation.

**Code walk.**
1. Web jump renders the target page (`web/src/app.js:1193-1194`) and posts `page-changed`;
   the native shell updates `currentPageRef` to the browsed page (`PdfReaderApp.tsx:709-711`).
2. The director's 1s mesh heartbeat (`PdfReaderApp.tsx:393-400`) delivers a `page` event; the
   follower's de-dupe `if (page === currentPageRef.current …) break` (:903) now MISSES (refs
   differ), so it re-injects the director's page (:904-911).
3. The web's mesh handler renders it UNCONDITIONALLY — `applyNativeSyncEvent` `page` branch:
   `renderPage(event.page, { pushToHistory: false })` (`web/src/app.js:977-980`). No `browsing`
   check of any kind.
4. The web's browsing machinery is relay-only: `relay.browsing` is set at
   `app.js:1200-1205` gated on `relay.hasDirector`, which is permanently false in the shell
   (`startRelayFollow` skipped, :3329). The go-live bar and amber pill never exist on native
   (`renderRelayPill` hides the pill when `!relay.hasDirector`, :3038).

**User impact.** The follower surface offers exactly two controls (⟳ and ♪, per the role CSS); one
of them is a 1-second trap whenever a director is live — i.e. during all of Mass. A web phone
follower doing the identical gesture gets a supported browse + "Volver a en vivo" bar. Elderly
users read the snap-back as "the iPad is broken." This cross-surface inconsistency is new surface:
prior art's `web-reader-browse-result-click-skips-relay-browsing-mode` covers only relay/web.

**Proposed fix (web bundle, no Swift).** Give the mesh path the same browsing contract the relay
path has, entirely in `applyNativeSyncEvent`:
- Track `mesh.livePage` from every incoming `page` event.
- Reuse the existing browse flag: when the ♪ jump lands off `mesh.livePage` in the shell, set
  `relay.browsing = true` (or a parallel `mesh.browsing`) and show the SAME go-live bar
  (`showGoLiveBar` / `goLive` already exist and are DOM-only).
- In the `page` branch: if browsing, update `livePage` + show the bar instead of `renderPage`.
- `goLive()` clears the flag and renders `livePage` (and can keep posting `resync` to native).
The 1s heartbeat makes eventual consistency trivial — the moment browsing clears, the next
heartbeat re-homes even if a turn was missed.

*shipVector caveat:* fix is in the shared web bundle, so it reaches signovivo.com instantly but the
parish iPads only at the next native build (bundled copy) or mesh bundle push.

**Acceptance criteria.** In the shell with a live mesh director: ♪ jump off the live page → page
stays, "Volver a en vivo" bar shows; director turns → follower does NOT move; tapping the bar
returns to the director's current page ≤1s. Follower with no director: ♪ jump behaves as today.
Reloaded follower (bridge-ready resync, `PdfReaderApp.tsx:679-694`) still snaps to the director.

**Test idea.** Unit-test the (to-be-extracted) mesh-page decision the same way svSyncDecision is
tested: `{browsing:true}` + page event → no render + bar flag; `{browsing:false}` → render.

---

## SYNCE2E-04 — Director promotion failures are SILENT on native: wrong code gives no feedback (the "código incorrecto" the code comments promise does not exist), and a valid code whose mesh start fails dies with no error after the user confirmed "Sí, dirigir" (HIGH, multi)

**Trigger / repro A (wrong code).** Native iPad, ♪ numpad, type a 5+ digit code with a typo, Ir.
Modal closes (`web/src/app.js:1183-1186` posts to native and closes unconditionally). Native:
`STANDARD_DIRECTOR_CODES.has(code)` false → `injectEvent({type:"role", role:"none"})`
(`PdfReaderApp.tsx:584-587` — its comment says "tell the web so it surfaces «código incorrecto»").
Web: the `role` handler (`web/src/app.js:947-956`) only sets `state.nativeSyncRole="off"` and hides
the DIRECTOR badge. **No error message exists anywhere on the native path** — the only
"Código no válido" flash is on the pure-web branch (`app.js:1189`). Outcome: the volunteer director
types their code before Mass, sees nothing, and has no way to distinguish "wrong code" from
"it worked" except noticing a tiny badge's absence.

**Trigger / repro B (valid code, mesh start fails).** Same numpad, correct code → confirm Alert
("¿Dirigir el coro?", `PdfReaderApp.tsx:613-621`) → user taps "Sí, dirigir" → `becomeDirector`:
`startNearbyDirector` throws twice (local-network permission revoked, radio wedge, Swift regression)
→ catch at :516-526 → non-follower case injects `role:"none"` → same silent nothing. The user
explicitly confirmed and believes they are directing; neither mesh nor relay is publishing
(role stays "off", `explicitTransmitterRef` false, `broadcastPage` gates at :358-368). Note the
device may have perfect internet — there is no degraded "publish to relay anyway" fallback either,
even though `setRelayPublishCode(code)` was already set at :459.

**User impact.** This is the single highest-stakes moment of the whole system (pre-Mass director
setup) failing silently, in front of the congregation. The 2026-07-01 outage class was exactly
"no director all night"; both paths above reproduce that outcome with zero signal.

**Proposed fix.**
- Web (shared bundle): on a `role` event of `"none"` arriving ≤10s after this client posted a
  `director-code`, flash a visible error ("Código incorrecto o no se pudo activar el modo director —
  intenta de nuevo") — track a `pendingDirectorCodeAt` timestamp at `app.js:1184`.
- Native: differentiate the two causes with an additive bridge payload —
  `{type:"role", role:"none", reason:"bad-code" | "mesh-failed"}` (old bundles ignore the extra
  field; additive-only contract holds). For `mesh-failed` on a device with a set publish code,
  offer the relay-only degradation: an Alert "No se pudo activar la señal local. ¿Transmitir solo a
  signovivo.com?" → set `explicitTransmitterRef.current = true` + `startDirectorHeartbeat()` (the
  transmitter machinery already exists at :461-476).
- Fix the stale comment at `PdfReaderApp.tsx:585`.

**Acceptance criteria.** Wrong code on native → visible Spanish error within 1s. Valid code +
forced mesh-start failure → visible error naming the failure + optional relay-only mode;
congregation web followers sync if accepted. Cancel on the confirm Alert → no error (note
`onDirectorCode`'s Cancel button does not inject `none`, so the pending-code window trick is safe).

**Test idea.** Web unit: simulate `postNativeBridge('director-code')` then `role:none` event →
error shown; `role:none` with no pending code → silent. Native: e2e contract test pinning the
`reason` field once added.

---

## SYNCE2E-05 — Forced /state polls bypass the seq guard entirely and can REWIND a follower onto an older snapshot for up to ~12s (MEDIUM, web-only)

**Trigger / repro.** Weak network (slow fetches — exactly when forced polls are most frequent).
1. A forced `/state` poll starts (poll-through-connect gap `app.js:3235`, WS-open resync `:3260`,
   F1 re-home `:3292`, foreground `:3332`, 4s fallback interval `:3213`). The fetch takes ~1-3s.
2. Meanwhile the director turns the page; the WS push applies it (`seq S2`, page P2), lastSeq=S2.
3. The stale poll resolves carrying the PRE-turn snapshot (S1<S2, P1) and is applied with
   `force:true`.

**Code walk.** `svSyncDecision.js:111`: `if (!ctx.force && snap.seq <= ctx.lastSeq)` — force
bypasses even STRICTLY OLDER seqs. Then `:116` keeps `lastSeq = max(...) = S2` but `:117` sets
`out.livePage = snap.page = P1` and `:130` renders P1. The follower visibly flips BACK to the old
page. Worse, `relay.livePage` now equals the stale page (`app.js:3152`), so the F1 drift re-home
(`app.js:3291-3293`, compares `state.currentPage !== relay.livePage`) sees NO drift and will not
correct it. Recovery waits for the director's next move or the 12s relay heartbeat republish (new
seq → follow branch re-renders) — up to ~12s on the wrong page under a green pill.

**Proposed fix.** In `decideRelaySnapshot`, force should re-apply EQUAL seqs (its purpose: re-home
onto a stationary director) but never STRICTLY OLDER ones:

```js
if (snap.seq <= ctx.lastSeq && (!ctx.force || snap.seq < ctx.lastSeq)) {
  out.action = "live-dup"; out.renderPill = true; return out;
}
```

(i.e. `!force && seq <= lastSeq` OR `force && seq < lastSeq` → live-dup.) F4's demote reset
(`lastSeq = -1`) is unaffected. The inline fallback in `applyRelaySnapshot` (`app.js:3124`) should
get the same guard.

**Acceptance criteria / test.** Add to `e2e/svSyncDecision.test.mjs`: `force:true, snap.seq=5,
ctx.lastSeq=9` → action `live-dup`, no renderPage; `force:true, seq=9, lastSeq=9` → follow with
renderPage (re-home preserved); `force:true, seq=10, lastSeq=9` → follow. All existing cases stay
green.

---

## SYNCE2E-06 — Step-down straggler publish goes out with an EMPTY code and triggers the scary red "El relé rechazó el código de director" banner on a device that just correctly stopped directing (MEDIUM, native-build)

**Trigger / repro.** Director on a weak link (in-flight publish + a queued `pending` page turn) is
demoted — mesh DIRECTOR_CONFLICT (`PdfReaderApp.tsx:934-943`) or exit-director — so
`becomeFollower` runs.

**Code walk.**
1. `becomeFollower` → `setRelayPublishCode("")` (C3, `PdfReaderApp.tsx:430`). That setter ALSO
   re-arms the one-shot auth latch: `authErrorNotified = false` (`src/directorRelaySync.js:36-41`).
2. The in-flight publish (sent earlier with the valid code) completes; its `finally` drains
   `pending` via `doPublish(next)` (`directorRelaySync.js:103-107`), which reads the NOW-EMPTY
   `relayPublishCode` at fetch time (:77).
3. Worker: empty `X-Director-Code` → `codeOk` false → 401 (`sync-worker/src/index.ts:782-787`).
   Exactly C3's intent — the stale frame is rejected.
4. But `doPublish` treats 401 as the "followers silently frozen" showstopper and, with the latch
   freshly re-armed, fires `relayAuthErrorHandler` (:89-96) → `injectEvent({type:"relay-auth-error"})`
   (`PdfReaderApp.tsx:342-347`, registered unconditionally at mount) → the web shows the fixed red
   alert "El relé rechazó el código de director. Los seguidores en signovivo.com NO están
   sincronizados." (`web/src/app.js:887-925`), latched on screen until dismissed.

**User impact.** The demoted (or voluntarily exited) volunteer — now a follower — gets a red
mid-Mass banner claiming their code was rejected and the congregation is out of sync. Both claims
are false: the WINNING director is publishing fine. Confusion + likely a panicked re-entry of the
code (which would fight the winner).

**Proposed fix.** In `doPublish` (or `publishPageToRelay`), if `relayPublishCode === ""` drop the
payload without fetching AND without warning — C3's goal ("rejected, never applied") is achieved
one hop earlier and the relay saves a guaranteed-401 round-trip:

```js
const doPublish = async (payload) => {
  if (!relayPublishCode) { inFlight = false; pending = null; return; }
  ...
```

(Also guard the drain: `if (pending && relayPublishCode)`.) Real 401s — a live director whose code
was rotated out — still warn, because their code is non-empty.

**Acceptance criteria.** Simulated step-down with a pending publish: no POST is issued after the
code clears; no banner. Live director with a retired code: banner still fires once (existing
behavior). e2e: unit-test directorRelaySync's coalescer with an injected fetch mock.

---

## SYNCE2E-07 — Fleet dashboard flags the REAL director "not ready" whenever ANY checked-in device carries a newer build — guaranteed false red during every canary window (MEDIUM, worker-only)

**Code walk.** `sync-worker/src/index.ts:433-434`:
`maxBuild = devices.reduce(max over ALL check-ins)` — including orphan/guest devices that match no
roster row (they're excluded from rows but not from maxBuild) — and
`latest = Math.max(maxBuild, MIN_SYNC_BUILD)`. The director row then requires equality with fleet
max: `bestBuild >= latest ? ok : "⚠ Director en build X (debe ser Y)"` (index.ts:462-464); the
footer repeats "Director debe estar en ${latest}" (:567).

**User impact.** The green-day runbook institutionalizes a canary build on the OLDEST iPad (a
follower) before wide rollout, and Miguel's own dev device checks in from anywhere with a higher
build (e.g. 381 vs fleet 377). During every such window the dashboard shows the actual director as
red "por contactar" with an instruction to update — right when the operator is doing pre-Mass
checks. Cry-wolf reds train the operator to ignore the dashboard, which is its whole value. Note
`MIN_SYNC_BUILD=361` (:403) is the real sync-compat floor; "director == fleet max" is a stricter
cosmetic rule that the canary ritual violates by design.

**Proposed fix.** Compute the director requirement from roster-matched, recently-seen devices only,
and demote the mismatch to WARN, e.g.: `latest = max(nativeBuild of devices seen <48h whose label
matches a roster person)`; director `>= MIN_SYNC_BUILD` → never red on build alone; director `<
latest` → amber "hay una build más nueva en la flota (canary?)". Alternatively annotate: if exactly
one device holds maxBuild and it isn't the director's, label it "canary detectada".

**Acceptance criteria.** Seed a roster + check-ins where director=377, one follower=381: director
row is amber (not red) with canary wording; all ≥ MIN_SYNC_BUILD stay "Listo".

**Test idea.** Pure-function extract of the row classifier (it's already deterministic on
`{roster, devices, nowSec}`) + node unit tests; runs in CI without a DO.

---

## SYNCE2E-08 — Native followers have NO relay fallback: mesh is a single point of failure for every parish iPad, while the same room's relay carries the identical pages (MEDIUM, multi — decision-sharpener for Q5)

**Code walk.** The consumption side of the relay is web-only:
`startRelayFollow` returns immediately in the shell (`web/src/app.js:3329`); the native shell never
opens `/subscribe` or polls `/state` (`src/directorRelaySync.js` is publish-only; no other relay
consumer in PdfReaderApp.tsx / src/). Consequences, all verified:

1. **Mesh-broken follower**: if Multipeer fails on a follower iPad (local-network permission
   revoked after an iOS update, radio wedge, a regression in the still-uncompiled #266 Swift), that
   iPad silently shows a normal songbook — since #269/#271 the sync spinner caps at ~1.1s
   (`app.js:861-878`), so "mesh broken" is visually identical to "no director yet". A web phone
   beside it follows fine off the relay.
2. **Transmitter-only director** (relay-only publisher, `PdfReaderApp.tsx:461-476`): drives ONLY
   web followers; every mesh iPad in the room stays frozen with no indication a director is live.
3. **Director mesh-start failure** (SYNCE2E-04 B): even with internet on both ends, iPads cannot be
   served at all.

**Why this is a finding and not a re-report.** The mesh-vs-relay stance ("iPads have no wifi in
church") is documented, and P-MESH/Q5 (retire-vs-keep) is planned; but no prior-art row states the
operational consequence that native followers are UNREACHABLE by the only redundant transport even
when they have connectivity (practice nights, tethered Masses — the fleet check-in itself proves
iPads often do have internet: it POSTs every 90s). This asymmetry is the strongest single input to
Q5: if native followers ALSO consumed the relay (mesh-preferred, relay as fallback exactly like web
— svSyncDecision already ships in the bundle), the mesh becomes a latency/offline optimization
instead of a single point of failure, and the retire decision gets cheap.

**Proposed fix approach (behind Q5).** In the shell, instead of the blanket skip at `app.js:3329`,
run relay-follow in a suppressed mode: connect WS/poll as on web, but apply snapshots ONLY when no
fresh mesh page has arrived within the mesh-liveness window (the shell already tracks mesh recency
via `lastDirectorSnapshotRef.at`, `PdfReaderApp.tsx:899`; bridge it down, or track last `sync-event`
page receipt web-side). Mesh events always win ties (they're sub-second; relay is ≥RTT). This is
the "svSyncDecision role" conflict resolution the lens asked about — today it is trivially "mesh
always, relay never"; the fix makes it "mesh if fresh, else relay".

**Acceptance criteria.** iPad with mesh disabled + internet: follows a native director via relay
≤4s per turn. iPad with mesh working: behavior identical to today (relay snapshots suppressed —
assert no double-renders/echo via the existing `page-changed` de-dupe). Offline church iPad:
unchanged.

---

## SYNCE2E-09 — DELTA on A2 (#246): the /publish token bucket can 429 a legitimate director scrubbing pages, silently dropping the FINAL resting page for up to ~12s (LOW, worker-only)

**Code walk.** Bucket: 15 burst, 2/s refill per IP (`sync-worker/src/index.ts:138-140`). The native
coalescer serializes publishes but each round-trip is ~150-300ms on good wifi, so a director
holding the page-forward arrow / rapidly swiping to hunt for a song can sustain ~3-6 publishes/s;
after ~5-8s of scrubbing the bucket empties and publishes 429 (`index.ts:816`). `doPublish`
deliberately treats 429 as transient and silent (`src/directorRelaySync.js:84-96` — only 401/403
warn), so if the LAST publish of the scrub (the page the director settles on) is the one rejected,
web followers sit on a mid-scrub page until the 12s relay heartbeat (`PdfReaderApp.tsx:401-412`)
republishes with a fresh seq. Mesh followers track every turn instantly (no mesh rate limit) —
another brief web/native divergence.

**Why a delta, not a re-report:** A2 is FIXED prior art; this is its legitimate-traffic edge, with
a concrete loss window and recovery bound, previously undocumented.

**Proposed fix (pick one, all cheap).**
- Raise the burst to ~30 and refill to 4/s (still orders of magnitude under flood scale; a
  page-hijack needs sustained volume, not 30 frames), OR
- Client-side: on a 429 response, keep the payload in `pending` (retry after ~1s) instead of
  dropping it — one small change in `doPublish`'s status handling, converts the 12s window to ~1s.

**Acceptance criteria.** Local wrangler: 25 publishes in 5s from one IP → final page visible in
`/state` within 2s of the last publish. Flood test from a2.test.mjs still hits 429.

---

## Parking lot (not findings — ideas / adjacent notes for later slices)

- **Backgrounded-director operator awareness**: foreground already re-broadcasts
  (`PdfReaderApp.tsx:1010-1016`); M4's director pill ("Dirigiendo — N conectados") should ALSO
  factor relay publish health (age of last `res.ok`), not just mesh peerCount, or a
  transmitter-only director's pill will be vacuously green. Export a `lastPublishOkAt` from
  directorRelaySync.
- **`{ignored:true}` / `rateLimited:true` consumption** is planned (M4); when built, wire it into
  the same pill (amber "el relé ignoró tu página — ¿otro director activo?").
- **Worker outer catch returns 200 EMPTY_SNAPSHOT for any route throw** (index.ts:821-826): on the
  /state path a transient DO error demotes followers for one poll cycle (self-heals via F4's
  `lastSeq=-1` reset); on /publish the inner try/catch (:811-815) already returns 500 so exposure
  is minimal. Left to the worker lens; noted because F4's reset is what makes recovery automatic —
  do not "fix" F4 without rechecking this path.
- **WS wire carries no `now`** — clock offset only calibrates via /state (fine today because polls
  run at boot/open/foreground; would matter if polls were ever removed).
- **MIN_SYNC_BUILD=361 is hand-maintained** (index.ts:403); any future mesh wire change must bump
  it manually — candidate for a build-time assert tied to the nearby-sync contract test.
- **`dbgLog("mesh:page-recv")` fires on EVERY incoming mesh page event including 1s-heartbeat dups**
  (`PdfReaderApp.tsx:892-896`, before the de-dupe break) — check JS dbgLog batching before
  Mass-scale telemetry lands.
- **Slow-clock handoff dead-air (~90s)** folded into SYNCE2E-01's detail; durable fix = M4
  transmitterId tiebreak.
