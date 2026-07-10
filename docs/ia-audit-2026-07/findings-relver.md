# RELVER — Release, versioning & compat seams as user-facing IA

> Audit lens: version/compat seams as EXPERIENCED reality (badges, dashboard, canary ritual,
> bundled-copy skew, cache rotation, rollback, wire-contract enforcement).
> HEAD at audit time: `d5075091` (build **381**, version 1.0.4) — NOTE: 4 commits PAST the
> cartographer maps (cut at 16244b25 / build 377). All line anchors below were re-verified
> against d5075091. Landed since the maps: #269 (spinner cap), #270 (web fleet self-ID modal
> REMOVED), #271 (spinner simplify + PDF rename), build-381 bump.
>
> Dedupe honored: nothing here re-reports a prior-art KNOWN-FINDING or PLANNED-WORK item
> except explicitly-labeled DELTAs (unlisted user-facing angle / materially worse).

---

## RELVER-01 [HIGH] Canary ritual's primary URL tests the WRONG bundle — `signovivo.com?env=staging` serves PROD content

**Trigger / repro.** Operator follows docs/pre-mass-checklist.md §A.3 verbatim: deploys the
canary with `STAGING=1 bash scripts/release.sh`, then points the oldest iPad at
"`signovivo.com?env=staging`, or the printed preview URL" (pre-mass-checklist.md:23-24 lists
the signovivo.com form FIRST). signovivo.com is Cloudflare Pages **prod branch `main`** —
it always serves the CURRENT PROD bundle. `?env=staging` only switches the **relay room**:

- `web/src/lib/svRelayRoom.js:28-36` — `resolveRelayRoom()` maps `?env=staging` →
  `alvernia-staging`. Nothing anywhere selects different *content* based on the param.
- `scripts/release.sh:33-36, 124-127` — the staging bundle goes to Pages branch `staging`
  (a *preview URL*), never to signovivo.com.
- The same misconception is in prose in docs/green-day-deploy-runbook.md:45 — "the
  `?env=staging` isolation is for the *bundle*" — it is for the ROOM, not the bundle.

**What the user experiences.** The canary walk runs entirely against the OLD (already-proven)
prod bundle joined to an empty staging room. Every checkbox passes trivially. The operator
"promotes" a build that was never executed on any device → the next prod deploy is the first
time the new bundle runs in front of the parish. This silently defeats the entire M1
release-safety gate — the exact "busted for everyone" class the gate was built after.

**Fix approach.**
1. Doc fix (instant): pre-mass-checklist.md §A.3 and green-day-deploy-runbook.md:45 must say
   the **preview URL printed by release.sh is the ONLY place the canary content exists**;
   `?env=staging` is the *room* pin you add ON TOP of the preview URL (open
   `https://staging.alvernia-reader.pages.dev/?env=staging` so the canary also stays off the
   Mass room). Remove `signovivo.com?env=staging` as a canary-content option.
2. Make the seam self-evident (RELVER-02 indicator + RELVER-11 badge marker) so a wrong URL
   is visually obvious.
3. Optional hardening: `STAGING=1 release.sh` should end by echoing the exact
   `https://staging.<project>.pages.dev/?env=staging` URL as one copy-paste line (today the
   operator has to fish it out of wrangler output).

**Acceptance criteria.** Checklist/runbook contain no instruction implying signovivo.com can
serve canary content; release.sh staging run ends with one canonical canary URL; a canary
device shows a visible staging marker (RELVER-02/-11).

**Test idea.** Assert in an e2e (source-pin style, like the existing contract tests) that
release.sh's staging branch is `staging` and that the checklist contains the pages.dev
preview host string — cheap drift guard for the ritual doc.

---

## RELVER-02 [HIGH] Staging-room membership is invisible on-device — a canary iPad left on `?env=staging` silently won't follow at Mass

**Trigger / repro.** During the Wed canary walk a device (per checklist: the OLDEST parish
iPad — exactly the one that returns to Mass duty) is pointed at a URL carrying `?env=staging`.
`clearInitialUrl()` **deliberately preserves** the param across reloads
(web/src/app.js:669-678, guard at :676) so the pin survives reload — good for the canary,
but there is **zero on-screen indication** of it: no pill, no badge suffix, nothing (grep
"staging" in app.js: only the resolver comments at :2842-2853 and the clearInitialUrl guard;
no UI). The build badge shows the same number as prod (see RELVER-11).

**What the user experiences.** Sunday: the iPad opens from the leftover Safari tab/bookmark,
joins `alvernia-staging`, shows the normal reader, and simply never follows the director.
⟳ resync re-syncs the *staging* room, so the on-screen recovery affordance doesn't help. To
everyone in the room the device is "broken"; the actual cause (a query param from four days
ago) is invisible and undiagnosable at 12:05 with the entrance hymn starting. (Home-screen
PWA relaunches are safe — manifest `start_url:"/"`, web/src/manifest.webmanifest:8 — the
risk vector is Safari tabs/bookmarks and any device deliberately left pinned.)

**Fix approach (web-only, tiny).** When `RELAY_ROOM !== "alvernia-main"` render a persistent
high-contrast chip (e.g. "MODO PRUEBA" next to the build badge, reusing the #build-badge
style block) and prefix the badge text (see RELVER-11). Optionally persist a timestamp with
the pin and auto-drop to the prod room after e.g. 24h with a one-line toast ("Volviendo al
modo normal"), converting the trap into a self-healing state. Note the fix reaches parish
iPads' *native* copies only at the next native build, but the native app can't reach staging
today anyway (native staging entry deferred to M7) — the at-risk surface is web/PWA, where a
Pages deploy lands instantly.

**Acceptance criteria.** Any device on a non-main room shows an always-visible marker; a
reload keeps the marker; the marker never renders on the prod room.

**Test idea.** Unit-test a pure `shouldShowRoomChip(room)` next to svRelayRoom; e2e source
pin that app.js wires it to the resolver output.

---

## RELVER-03 [MEDIUM] Fleet dashboard flags the real director "not ready" whenever ANY stored check-in has a higher build — no TTL, and dev/simulator runs feed it

**Code walk.**
- `sync-worker/src/index.ts:433-434` — `maxBuild` = max `nativeBuild` over **every stored
  check-in**, `latest = max(maxBuild, MIN_SYNC_BUILD)`.
- `:462-464` — the director row is `ok` only if `bestBuild >= latest`, else red
  "⚠ Director en build X (debe ser Y)".
- Check-ins have **no TTL**: `checkin()` (index.ts:250-289) keeps a 300-device MRU ring;
  `getFleet()` (:292-297) returns everything; the renderer's `ago()` shows staleness but
  `maxBuild` ignores it. Only `/fleet/reset` clears entries.
- Feeders of inflated builds: (a) the sanctioned canary ritual itself — checklist updates the
  oldest iPad FIRST, so from that moment the un-updated director is red for the whole
  adoption window; (b) any `npm run ios` — `preios` bumps version.json on every local run
  (package.json:8, known finding build-release-preios-hooks-bump), and the simulator app's
  fleet check-in has **no dev gate and posts to the PROD fleet DO**
  (PdfReaderApp.tsx:182-201 — hardcoded prod RELAY_BASE, no `__DEV__` check). One simulator
  boot at a locally-bumped build (e.g. 383) permanently makes the dashboard demand
  "debe ser 383" of a director who can never install it.

**What the user experiences.** Pre-Mass, the operator's readiness tool shows the actual,
perfectly-current director in red with an instruction that is impossible to satisfy —
either during every legitimate staged rollout, or forever after one stray dev run, until
someone remembers `/fleet/reset`. The tool cries wolf exactly when it's consulted.

**Fix approach (worker-only).** Compute `maxBuild` only over check-ins seen within a window
(e.g. 7 days): `maxBuild = max(nativeBuild) over devices with ts within 7d`, plus a muted
footnote row listing ignored stale devices. Optionally add a native-side `__DEV__` guard so
simulator runs never check in (native-build, rides any future build).

**Acceptance criteria.** A single stale or one-off high-build check-in does not change the
director row; a genuinely-updated fleet still raises the bar; `/fleet/reset` not required in
either case.

**Test idea.** Extract `computeFleetLatest(devices, nowSec)` as a pure function; unit-test
stale-entry exclusion (render logic — safe without a DO).

Parking-lot adjacent: `MIN_SYNC_BUILD=361` (index.ts:403) is a hand-maintained hardcode —
nothing forces a bump when a future build introduces a real sync floor; it silently decays
toward "everything green". Worth a comment-anchored checklist item in the release runbook.

---

## RELVER-04 [MEDIUM] (delta of `offline-pwa-fleet-webcached-false-green`) Web followers have NO version floor and no legible version anywhere on the dashboard

The known finding covers the *page-count/sticky-flag* inflation. The unlisted seam is
**shell version**: a web device's readiness is judged with no version dimension at all.

**Code walk.**
- Web check-in payload (web/src/app.js:2962-2971): `deviceId, surface, webCached,
  pagesCached, totalPages, homeScreen, cacheVersion` — **no human `buildNumber`** (the badge
  number, `BUILD_NUMBER`, is right there at :220 but not sent).
- Worker stores `cacheVersion` (index.ts:279) but **never renders or compares it** (only 2
  references in the file: the type at :74 and the store at :279).
- Dashboard readiness: `webReady = webCached && homeScreen` (index.ts:452), row `ok` at
  :467-468 — no floor analogous to `MIN_SYNC_BUILD`.

**What the user experiences.** The one parish device that MUST run web (the old iPad PWA,
too old for TestFlight) can sit on a months-old cached shell — e.g. one predating the #248
freshness-before-seq fix, i.e. the literal Wednesday green-pill-freeze bug — and the
dashboard says "Listo — web en caché". The operator has no way, from the dashboard, to know
what any web device runs; `cacheVersion` wouldn't help even if shown (opaque sha+hash a
human can't order or map to "v381").

**Fix approach (multi: web + worker, both instant-ship).**
1. web: add `buildNumber: BUILD_NUMBER` to the check-in payload (additive; worker `checkin()`
   whitelists fields, so add a capped numeric store).
2. worker: render it in the Web column ("✓ inicio · v381") and add a `MIN_WEB_BUILD` floor
   that demotes the row to warn "Recargar signovivo.com".
   Caveat: the PWA only re-checks-in when online, so the floor catches *stale-but-online*
   shells (the fixable case) — a fully-offline device just ages out via the "Visto" column.

**Acceptance criteria.** Every web row shows a human build; a web check-in below the floor
never renders "Listo".

**Test idea.** Pure-function extraction of the row classifier + unit tests (same harness as
RELVER-03).

---

## RELVER-05 [MEDIUM] (delta of `native-swift-stale-documents-bundle-masks-update`) No surface exposes WHICH web bundle a native device is running — and badge vs crash-telemetry use OPPOSITE version precedence

The known finding says a stale Documents/WebBundle runs "old web code under a new badge".
The unlisted user-facing angle: **nothing anywhere lets a human see the web-bundle identity
on a native device, and the two surfaces that do report a build disagree by design.**

**Code walk.**
- Badge: `resolvedBuild = window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || BUILD_NUMBER`
  (web/src/app.js:3479-3481) — on native, the injected SHELL build (PdfReaderApp.tsx:44,
  :1037, from version.json at Metro-bundle time) always wins; the web bundle's own baked
  `BUILD_NUMBER` is shadowed. Same for the help-panel "Versión" label (:3482-3484).
- Fleet: native check-in sends only `nativeBuild` (PdfReaderApp.tsx:191) — no web-bundle
  field exists on the wire.
- Crash telemetry: the OPPOSITE order — `BUILD_NUMBER` (web bundle) first, native injected
  as fallback (app.js:2901-2903). So for a device whose loaded web bundle ≠ shell (mesh-pushed
  Documents copy, or an alt-script archive with a stale ios/WebBundle — both known-open),
  the on-screen badge says one number and the dashboard crash panel's "Build" column says
  another. Nobody documented which surface means what; an operator diagnosing a crash will
  reasonably conclude the dashboard is buggy or the device lied.
- Consequence for the checklist: §B's "Same **version** as the others (bottom-right build
  label)" (pre-mass-checklist.md:49) is checked against the number that CANNOT reveal the
  known failure mode it exists to catch.

**Current concrete skew at HEAD** (illustrates the blindness): web builds 378-381 changed
follower-visible behavior (#269 spinner cap, #271 spinner simplify); a fleet iPad still on
native 377 renders the OLD spinner from its bundled copy while phones render the new one.
Diagnosable today only by comparing badge numbers across devices and knowing the
build→behavior mapping by heart.

**Fix approach.**
1. web (instant for PWA, next-build for native): when `__SIGNO_VINO_NATIVE_BUNDLE_VERSION`
   is present AND ≠ `BUILD_NUMBER`, render the badge as `381·w379` (shell·web) — one glance
   shows the skew that today is invisible. Also include `webBuild: BUILD_NUMBER` in the
   `bridge-ready` payload (app.js:3465-3474) so native learns it.
2. native (next build): forward `webBuild` in fleet check-in; dashboard renders
   "381 (web 379)" and flags mismatch.
3. Document the precedence rule (badge = shell-first, crash = web-first) or unify on
   "always report BOTH".

**majorUpdateIntersection.** M7's DIAGNÓSTICO screen and M4's status pill are the planned
homes for device-visible version truth (major-update-2026-07.md:448 promises "los demás
están en v374 … made visible on the device itself"); this finding defines exactly which
TWO numbers that surface must show. M5/M6 add a third (bookVersion) — design the badge
grammar once.

**Acceptance criteria.** A device whose loaded web bundle ≠ shell shows both numbers on the
badge; fleet dashboard shows both; crash rows and badges can be reconciled by rule.

---

## RELVER-06 [HIGH] (delta of `native-swift-stale-documents-bundle-masks-update` + held M-F3) Mesh OTA re-transfers the ~30 MB bundle and remounts the follower on EVERY reconnect — installed pack version is never persisted

**Code walk (all DirectorSyncModule.swift, device-gated/uncompiled like all recent Swift).**
- The version a follower compares against is `currentBundleVersion` = the APP's
  `CFBundleVersion` (:97-99) — a computed property that never changes at runtime.
- Director sends `bundle_offer` **on every peer connect** (:1743 → sendBundleOffer :705-713).
- Follower accepts when `offered > mine` where `mine = Int(currentBundleVersion)`
  (:717-722). After a successful install, the received pack's `headerVersion` (:937) is
  emitted in the `bundleUpdated` event (:1046) but **persisted nowhere** — `mine` is still
  the old shell build.
- So: follower on shell 377 receives the 381 bundle → installs → WebView remounts → any
  later reconnect (backgrounding, mesh churn, M-F1 watchdog kick) → director re-offers 381 →
  381 > 377 → the follower re-requests the SAME ~30 MB pack, streams it, re-installs,
  re-remounts.

**What the user experiences.** During any mixed-build window (the normal state for days
after a TestFlight rollout, per the checklist's own adoption model), every behind-by-one
follower iPad repeatedly: saturates the mesh with 30 MB transfers (starving 100-byte page
turns), then hard-remounts its WebView — mid-Mass, repeatedly. This is the held M-F3
disaster ("bundle install remounts follower mid-Mass") upgraded from *once* to *once per
reconnect, forever*, and it also keeps re-masking the update state RELVER-05 describes.

**Fix approach (native-build; belongs in the M7 batch).** Persist the installed pack's
`headerVersion` (UserDefaults, e.g. `sv.webBundle.installedVersion`) at the atomic-swap
success point; define
`effectiveBundleVersion = max(Int(CFBundleVersion), installedVersion)` and use it in
`handleBundleOffer` (:721) AND in the director-side advertised version — noting the
director packs only `Bundle.main/WebBundle` (:800-803), so a director should advertise the
SHIPPED version it can actually serve, not its effective/running one. Clear the persisted
key when CFBundleVersion exceeds it (TestFlight update supersedes the pushed copy — which is
also the cleanup hook the known stale-Documents finding wants).

**Acceptance criteria.** Two devices A(shell 381, director) and B(shell 377): first connect
transfers once; every subsequent reconnect transfers nothing; after B updates via TestFlight
to 381+, B's Documents copy is ignored/cleaned.

**Test idea.** Extend e2e/nearby-sync-contract.test.mjs source pins to require the
persisted-version read in handleBundleOffer (regex pin, same style as the existing
`v != protocolVersion` pin at :220-221). Behavioral proof needs the 2-device day.

**majorUpdateIntersection.** M7 lists "mesh bundle sha256+signature" and P-OTA may retire
mesh push entirely — if P-OTA wins, this dies with it; until then this is the sharpest edge
of the mesh-OTA seam and should ship in the same M7 native batch.

---

## RELVER-07 [LOW] Worker deploy identity is invisible — `/health` can't tell you what's live

**Code walk.** `/health` returns `{ok:true, service:"signovivo-sync", v:PROTOCOL_VERSION}`
(sync-worker/src/index.ts:585-587). `v` is the WIRE version (pinned =1 forever by the
additive-only contract) — it will never distinguish deploys. Worker deploys are manual
`npx wrangler deploy`, untied to version.json (map-pipeline O11; wrangler.jsonc has no
version var). The 2026-07-05 A2 deploy had to be verified by probing rate-limit *behavior*.

**What the user experiences.** Pre-Mass or mid-incident, the operator can confirm what web
build is on signovivo.com (badge) and what shell is on an iPad (badge), but has no way at
all to confirm which worker code is serving the room — the surface with instant fleet-wide
blast radius (see RELVER-08). Rollback verification (`wrangler rollback`) is equally blind.

**Fix approach (worker-only).** Bake an identifier at deploy: a `sync-worker/deploy.sh`
wrapper sets `DEPLOY_ID` (`git rev-parse --short HEAD` + UTC timestamp) via
`wrangler deploy --var DEPLOY_ID:$id`, echoed in `/health` as an ADDITIVE field
(`{ok, service, v, deploy}`) and printed in the fleet-dashboard header line.

**Acceptance criteria.** `curl /health` names the deployed commit; dashboard header shows it.

---

## RELVER-08 [MEDIUM] (delta of `test-suite-sync-worker-zero-tests` PARTIAL) The instant-fleet-wide ship vector has ZERO CI gate, and the relay wire shape is pinned by no test on any surface

The known finding records "no full vitest boundary suite" (security boundaries). The
unlisted release-seam angle: **the worker isn't in CI at all — not even its own typecheck —
while being the only surface that deploys to 100% of the fleet instantly.**

**Code walk.**
- `.github/workflows/ci.yml` contains no sync-worker step of any kind (grep `wrangler|
  sync-worker|working-directory`: zero hits). Root `npm run typecheck` is
  `tsc -p tsconfig.json` (package.json:16) — the RN app's tsconfig, not the worker's
  (sync-worker has its own `typecheck` script, never invoked by CI).
- The mesh wire has contract pins (e2e/nearby-sync-contract.test.mjs:26,57,220-221) but the
  RELAY snapshot `{v,page,totalPages,seq,ts}` + `/state`'s additive `now` is pinned by
  NOTHING that spans producer (index.ts:171-181 `next` construction, :758-763 `now`) and
  consumers (web/src/lib/svSyncDecision.js:79-93; src/directorRelaySync.js publisher).
- Failure shape is concrete and severe: svSyncDecision treats a MISSING `ts` as "live
  forever" — `hasFresh = hasPublished && (!isFiniteNum(snap.ts) || …)` (svSyncDecision.js:85).
  A worker refactor renaming/dropping `ts` (or `now`) typechecks nowhere in CI, deploys in
  one command, and instantly re-introduces the Wednesday dead-director-freeze class on every
  follower — with web and native code untouched and all green.

**Fix approach.**
1. CI: add `cd sync-worker && npm run typecheck` (zero new deps), and wire the EXISTING safe
   local harness (sync-worker/test/a2.test.mjs + run-a2.sh — refuses non-local bases, boots
   `wrangler dev`) into CI. That harness already exists and is undocumented; this is
   plumbing, not new test-writing.
2. Wire-shape pin: one shared fixture (e.g. e2e/relay-wire-contract.test.mjs) asserting (a)
   worker source constructs exactly the known keys (source-regex pin, house style), and (b)
   svSyncDecision yields "apply" for the canonical fixture and "demote" when ts is aged —
   consuming the SAME fixture object, so a field rename breaks CI on whichever side drifts.

**Acceptance criteria.** A PR renaming `ts` in index.ts fails CI; worker type errors fail CI.

**majorUpdateIntersection.** Overlaps planned P1-WORKER-UNIT/P1-HARNESS — but those are
scoped as *security-boundary/behavioral* suites; the delta here is (a) CI wiring of what
already exists and (b) the cross-surface SHAPE pin, neither explicitly in the plan. Also
directly serves major-update §5 ("additive-only … the promise the whole fleet's
compatibility rests on") which today is enforced only by comments.

---

## RELVER-09 [MEDIUM] #270 quietly killed roster matching for ALL web devices — the web-only follower now reads "No se ha visto — invitar" forever

**Code walk.**
- Since #270 (build 378 web), the web check-in payload carries **no `label` field at all**
  (web/src/app.js:2962-2971; the FLEET_LABEL_KEY plumbing was deleted).
- The dashboard maps devices→people by normalized-label equality
  (sync-worker/src/index.ts:443-446); a device with label "" can never match anyone.
- Coalescing keeps OLD labels alive server-side (`String(o.label ?? prev.label ?? "")`,
  index.ts:266) — so previously-labeled web devices still match TODAY, but any
  `/fleet/reset` ("for a fresh season", :299-303) permanently orphans every web device:
  there is no code path left that can ever set a web label again.
- Unmatched devices render in the "Sin coincidencia" orphan table as "(sin nombre)"
  (index.ts:507-515); the person row falls to `ds.length === 0` → red "No se ha visto —
  invitar" (:459-461).

**What the user experiences.** The parish's one web-only follower (the old iPad PWA — a
real, named roster device) shows on the dashboard as a permanently-red person ("never seen,
invite them") while the very same iPad sits three rows lower as an anonymous orphan reading
"✓ inicio". Contradictory readiness at the pre-Mass glance; the per-person web-readiness
branch (`webReady`, index.ts:452, :467-468) is now dead code for any post-reset fleet.

**Fix approach (worker-only render fix, instant).** Accept the new anonymous-web reality:
- Stop implying roster coverage for web: if a roster person has no matched device AND the
  fleet has anonymous web check-ins, don't claim "No se ha visto" — or better, drop the
  label-matching pretense for surface=web entirely and render web devices as first-class
  rows keyed by deviceId (with `ago`, cached state, and — per RELVER-04 — build).
- Alternative (heavier): roster gains an optional `deviceId` pin per person (seeded once by
  the operator from the orphan table), restoring named matching without any on-device prompt.

**Acceptance criteria.** After a `/fleet/reset` + one web check-in, the dashboard contains
no row that simultaneously claims a cached web device exists and that its person was never
seen; web devices are visible with readiness without requiring labels.

**Test idea.** Pure-render extraction + unit test: roster person with zero matched devices
+ one anonymous web device → no "invitar" contradiction.

---

## RELVER-10 [LOW] Self-ID UX is now skewed native-vs-web: the prompt judged "more annoying than useful" still ships on every iPad, and is now the ONLY label source

**Code walk.** #270 removed the web modal with the rationale that members mostly tapped
"Ahora no". The native twin — `Alert.prompt("¿Quién usa este iPad?", …)`
(PdfReaderApp.tsx:220-246) — still runs on first boot of every native install, persists
`sv_fleet_label` / `sv_fleet_skip`, and now supplies the only labels the dashboard's roster
matching (RELVER-09) can ever receive. Removing/aligning it requires a native build
(TestFlight), so the fleet shows this UX fork for as long as #270's rationale stands:
phones/PWA never ask; iPads still interrupt first boot with the exact prompt the team
decided against.

**Fix approach.** Decide the end state FIRST (ties to RELVER-09): if labels are dead, delete
the native Alert.prompt in the next native batch (M7 rides along) and key everything by
deviceId; if labels survive for iPads only, document that asymmetry in the checklist so the
operator knows only native devices can be name-matched.

**Acceptance criteria.** One documented rule for who gets asked what; no surface prompts for
data the dashboard no longer uses.

---

## RELVER-11 [LOW] Staging/canary bundle is badge-indistinguishable from prod — the operator can't confirm the canary content actually loaded

**Code walk.** `STAGING=1 release.sh` deliberately skips the bump and builds at the CURRENT
version (release.sh:38-44), so the canary badge renders the SAME `v<N>` as prod
(app.js:3488-3492). The only differing identity is `CACHE_VERSION`, visible solely on the
`?selftest` card as an opaque suffix (web/src/lib/svSelftest.js:45-51: "v381 · a1b2c3…") —
two hashes a human must diff by eye. Combined with RELVER-01 (wrong-URL trap) there is no
positive confirmation anywhere that the device under canary-walk is executing the new bytes.

**Fix approach (web-only, build-time).** In build.mjs, when the staging path is being built
(release.sh exports e.g. `SV_STAGING=1`; build.mjs consumes it), bake the badge token as
`<N>-prueba`. Badge then reads "381-prueba" — instant visual proof, and doubles as the
RELVER-02 room marker when paired with ?env=staging. Keep the prod path byte-identical.

**Acceptance criteria.** A staging deploy's badge is visually distinct; prod badge unchanged.

---

## RELVER-12 [MEDIUM] Checklist canary steps A.3.3–A.3.5 are impossible to execute on the staging canary — the walk cannot exercise director/mesh/restart at all

**Code walk.** pre-mass-checklist.md:23-30 instructs, on the staging WEB canary:
- "Become director on the canary" — on web, a 5+-digit code without a native bridge flashes
  "Código no válido" and dead-ends (web/src/app.js:1183-1190). Director promotion exists
  ONLY behind the native bridge.
- "a 2nd device follows the page over the mesh" — the mesh is native-only (Swift Multipeer);
  a web canary has no mesh surface whatsoever.
- "Restart test: force-quit the director iPad…" — presupposes the native director that step
  3 can't create; and the native app cannot load staging content at all (native staging
  entry explicitly deferred to M7; native transmitter pinned to prod room — prior-art
  oddity 3, not re-reported here).

**What the user experiences.** An operator honestly attempting the ritual hits "Código no
válido" and either (a) abandons the mesh/director checks — so the surfaces where every
historical outage lived (director restart, mesh sync) are validated by NOTHING before prod —
or (b) improvises with the native app, which silently publishes into the LIVE Mass room.
Either branch is a pre-Mass trap wearing a safety-procedure costume.

**Fix approach.** Rewrite §A.3 to state what a web canary CAN prove (boot, render, page
turn, relay follow in the staging room via a second browser tab on the preview URL +
`?env=staging`, ⟳, ?selftest card) and move director/mesh/restart items to a separate
"native canary (requires the new TestFlight build on the canary iPad — checks run against
the LIVE room, so do them at practice with the group present)" subsection. When M7 lands
native staging entry, fold them back.

**Acceptance criteria.** No checklist step is impossible on the device class it names.

---

# Parking lot (not findings — ideas / cleared checks / polish)

- **CLEARED: CACHE_VERSION rotation does NOT cause a silent re-download over parish wifi.**
  Verified end-to-end: after a deploy, `ensureOfflineBundle` fetches missing pages
  (app.js:566-575), the SW serves them from the PREVIOUS version's immutable page cache
  without touching the network (sw.js:184-185 matchAnyPageCache), and `cacheSinglePage`
  itself `cache.put`s the response into the NEW PAGE_CACHE (app.js:573) — a local
  copy-forward, not a re-download. The residual risk (device offline across 2+ deploys loses
  the only full cache) is the KNOWN `offline-pwa-page-cache-eviction-by-recency` — not
  re-reported.
- **CLEARED: web rollback path is sound.** Dashboard rollback rotates sw.js bytes → normal
  SW update → old shell in ~60s for online followers; rollback-web.sh path B rebuilds at
  GOOD_SHA so the badge number also reverts (visible). Residuals are known findings
  (mid-Mass force-reload; --commit-dirty artifact provenance). One nit: path B's
  `node web/build.mjs` runs with HEAD's node_modules against GOOD_SHA sources — fine today
  (build.mjs is dependency-light) but worth a comment in the script.
- `sv-offline-ready-<CACHE_VERSION>` localStorage keys and IDB metadata accumulate one per
  deploy, never pruned (app.js:223,:606) — bytes-trivial; fold a prune into any future
  cache-lifecycle batch (P3).
- release.sh header comment still promises a "native overlay (`b<N>`)" (release.sh:8) —
  no such overlay exists in PdfReaderApp.tsx; the only badge is web-rendered. Comment drift.
- Native fleet check-in reports `role:""` for a transmitter-director (PdfReaderApp.tsx:195
  checks `roleRef.current === "director"` only) — the dashboard can't distinguish an active
  transmitter from an idle follower. Touches the same dashboard-truth theme as RELVER-03/-04.
- `MIN_SYNC_BUILD` maintenance has no forcing function (see RELVER-03 tail).
- The `__SIGNO_VINO_*` (sic) globals are load-bearing on both sides (PdfReaderApp.tsx:1036-1038,
  app.js:227,:3480) — renaming to VIVO is a two-surface lockstep change; if ever done, do it
  additively (inject both names for one native generation).
- The ?selftest card is web-only by construction (query param impossible in the native
  file:// WebView) — M7's DIAGNÓSTICO should explicitly own the native equivalent, including
  the RELVER-05 dual-version display.
