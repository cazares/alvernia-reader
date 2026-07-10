# SignoVivo — IA & Interaction Audit Implementation Manual (2026-07)

_The execution manual for the July 2026 full-app Information-Architecture + native↔web interaction audit. Written for an autonomous engineer-agent (Opus 4.8 Max) executing with **zero prior context**. Everything you need is in this file, the companion artifacts under `docs/ia-audit-2026-07/`, and the repo itself._

## 0. Mission & provenance

**Mission.** Take SignoVivo — the parish songbook app (native iOS shell + signovivo.com web PWA + Cloudflare Worker sync relay) — from "hardened" to **nearly perfect** for live-Mass use: a volunteer director on a portrait iPad must always know they are live; elderly Spanish-speaking followers must never be stranded, misled, or confused; the operator's pre-Mass rituals must tell the truth.

**Provenance.** A ~130-agent audit (5 subsystem cartographers → 12 audit lenses → cross-lens dedupe → adversarial verification panels, 2 skeptics + tiebreaker on every high) ran against HEAD `d5075091` (build 381, 2026-07-09):

| Metric | Value |
|---|---|
| Raw findings | 115 |
| After dedupe | 73 |
| **Confirmed (this manual)** | **62** — 10 high · 26 medium · 26 low |
| Duplicates of already-tracked work | 8 (Appendix A — do not re-report, land with their tracked owners) |
| Refuted | 3 (Appendix B — do **not** "fix" these) |

Every confirmed finding survived an adversarial refutation pass with code citations re-verified at `d5075091`. **Line numbers are anchors, not gospel** — files will drift as waves land; re-verify every cite with Grep/Read immediately before editing (each finding's *Evidence* block tells you what to look for, not just where).

**Companion artifacts** (committed under `docs/ia-audit-2026-07/`):

| File | What it is |
|---|---|
| `audit-brief.md` | All 62 findings with verified evidence + recommendations, grouped by severity |
| `confirmed-findings.json` | Machine-readable record (incl. verifier notes, merge history) |
| `findings-<lens>.md` ×12 | Full per-lens write-ups: repro, code walks, fix sketches, parking-lot ideas |
| `map-native.md`, `map-web.md`, `map-worker.md`, `map-pipeline.md`, `map-prior-art.md` | Current-state subsystem maps (heed each file's correction banner) |

`map-prior-art.md` additionally indexes every PRIOR audit finding and planned-work item (the hardening plan, sync-reliability waves, major-update milestones M0–M7) so you never duplicate tracked work.

## 1. Ground rules — read before touching anything

### 1.1 Safety reds (absolute)

1. **NEVER run `npm run test:e2e` and NEVER execute `e2e/relay-sync.test.mjs`.** That file publishes to the **production** relay room `alvernia-main` and flips real followers' pages in church. Run individual safe test files only (§10 lists them).
2. **Never point any test, script, or manual probe at the prod relay room.** Use the staging room mechanics (`?env=staging`; see Wave 1 — and note finding RELVER-01: until fixed, `?env=staging` switches only the relay room, *not* the served bundle).
3. **No `--no-verify`, `--force` on main, or committed secrets.** `director-codes.private.json`, `roster.private.json`, `.dev.vars`, and anything with phone numbers stay out of git — the committed `director-codes.json` is intentionally EMPTY.
4. A red signal (failing test, linter, type error) is a real signal — investigate, never bypass. If a safe test file is red **before** your change, record it as baseline in the PR description and do not claim it.

### 1.2 Wire-compat doctrine (additive-only)

The field fleet runs **native builds 368–381**, each carrying an **old bundled copy of the web app**; signovivo.com updates instantly but native WebViews only pick up bundle changes at the next native release (or mesh OTA). Therefore:

- **Never remove or repurpose** an existing bridge message type/field (`signovivo-native` channel), relay wire field (`page/seq/ts/bookId/mode`), worker route, or localStorage/AsyncStorage key that an old peer might still emit or expect.
- **New capabilities = new, optional, silently-ignorable fields or message types.** Old web bundle + new native (and vice versa) must keep working at yesterday's feature level.
- Removals require the deprecation-window analysis in Workstream 6 (grep-proof of zero writers/readers **including what build-368's bundled web copy could still send**).

### 1.3 Working agreement (how to execute)

- **Worktree + branch per wave**: `git worktree add` a fresh tree off latest `main`; branch `dev-<wave-slug>`. Pull `origin/main` continuously; small PRs (1–3 per wave as each workstream chapter recommends); **commit + push after every green, logically-complete step** — never batch a day's work into one commit.
- **Merge-when-green is the default** for web/worker waves once §10 verification passes. **Exceptions that wait for Miguel**: anything tagged **DECISION-REQUIRED**, Workstream 4's §3 feature (ROLEWEB-02 web-director — approval-gated; WS4 §2's ROLEWEB-01/-07 fixes ship un-gated in Wave 4), and pushing the native release train to TestFlight/App Store.
- **Hunt → fix → re-hunt → verify, every wave.** After a wave's fixes: run a fresh adversarial review of the wave's full diff (`/code-review` at high effort, or equivalent), fix what it finds, then **re-hunt again**; iterate until a re-hunt comes back clean. Fixes create regressions a single pass misses — a clean *re-hunt*, not a green first pass, is the done signal.
- **Verify before you claim** (per repo doctrine): run the full available verification end-to-end AND cross-check an independent ground truth (browser preview for web, iPad simulator for native, `wrangler dev` probes for worker). Never report "fixed" on a single weak signal; surface every warning you accepted.
- **DECISION-REQUIRED protocol**: each chapter marks genuine product forks. Collect them per wave and present to Miguel in one batch (options + recommendation) *before* implementing that item; implement everything else meanwhile. Never resolve a fork by silent assumption.
- Update `docs/pre-mass-checklist.md` / `docs/green-day-deploy-runbook.md` in the same PR whenever a fix changes an operational ritual (Workstream 5 lists the exact edits).

### 1.4 Environment gotchas (hard-won; do not rediscover)

- **CocoaPods**: `pod install` crashes under the machine's Ruby 4.0.1 in the main checkout; workaround used historically: `cp ios/Pods/Manifest.lock ios/Podfile.lock` to satisfy the guard. Always `export LANG=en_US.UTF-8` before any `pod install` (Xcode 26 fmt/Folly post-install patch silently fails otherwise → `fmt` consteval archive error).
- **iOS archive/upload**: give the archive + export as ONE `&&` one-liner (`xcodebuild archive … && xcodebuild -exportArchive …`); upload via **Transporter**: `open -a Transporter <ipa>` (never suggest drag-and-drop). Local signing auto-mints distribution assets with `-allowProvisioningUpdates` — an Apple Development cert + 0 profiles is fine.
- **Version bumps**: `version.json` must be bumped **together with** `app.json`, `ios/SignoVivo/Info.plist`, and `project.pbxproj` — it feeds the web build badge (`__BUILD_NUMBER__`) and fleet `cacheVersion`. Use `scripts/bump-build.mjs` (read it first).
- **iPad PORTRAIT is the primary form factor** — verify every UI change in portrait first.
- The parish web-PWA iPad is **pre-iOS-16.4**: no TestFlight, no Web Push, no `navigator.wakeLock` — it uses Add-to-Home-Screen + Auto-Lock Never (Workstream 3 makes this in-app knowledge).

### 1.5 Ship vectors — how each change reaches users

| Vector | Mechanics | Latency to users |
|---|---|---|
| `web-only` | Merge to `main` → Cloudflare Pages (project `alvernia-reader`) auto-deploys signovivo.com. **Branch deploys are PREVIEW-only** — prod requires the merge. SW update polling picks it up on followers within minutes of a foreground. | Minutes (web/PWA); **native shells only at the next native train or mesh OTA** — sections flag when that matters |
| `worker-only` | `cd sync-worker && npx wrangler deploy` (secrets `RELAY_DIRECTOR_TOKEN` / `TRANSMITTER_CODES` / `FLEET_DASHBOARD_KEY` persist across deploys — never re-put casually) | Instant, fleet-wide — the scariest vector; always `wrangler dev` + `sync-worker/test/a2.test.mjs` first |
| `native-build` | Implemented per wave, **shipped batched** on the release train (§3): one build 382, canary ritual, then fleet | Days (TestFlight) |
| `multi` | Deploy order unless a finding says otherwise: **worker first** (additive-tolerant), **then web**, **then native train** | Mixed |

## 2. Architecture crash course (5 minutes)

One web bundle, two habitats: `web/src/app.js` (+`index.html`/`styles.css`/`sw.js`) is served at signovivo.com **and** embedded via `file://` inside the native shell's WKWebView (`PdfReaderApp.tsx`, repo root). The web side is a follower by default: it renders the single public 371-page songbook and mirrors the director through the relay (`sync-worker/src/index.ts`, one Durable Object room `alvernia-main`; WebSocket push + `/state` polling fallback; snapshot `{page, seq, ts, …}`; 90s liveness window). The native shell disables the web relay and instead drives the bundle over the `signovivo-native` postMessage bridge (inbound dispatcher `window.__signoVivoReceiveNativeEvent`); natively, devices sync over a Swift Multipeer mesh (`ios/…/DirectorSyncModule.swift`, 1s director heartbeat), and a native director *also* publishes to the relay (`src/directorRelaySync.js`, X-Director-Code header) so web followers track them. Roles: everyone boots follower; a 5+-digit code typed on the numpad promotes (native: mesh director via confirm dialog; web: currently a dead end — Workstream 4). `html[data-role]` gates director-only controls (⌕) vs follower controls (⟳). Read the five maps for depth; `map-prior-art.md` for what's already fixed/planned.

## 3. Release sequencing — the waves

Execute in this order. Each wave = one worktree/branch, 1–3 PRs (per its chapter's slicing), full §10 verification, adversarial re-hunt until clean, then merge/deploy per ship vector. **Wave 1 comes first because it repairs the release-safety ritual every later wave depends on.** Native-side changes are *implemented* in their waves but *ship together* on the release train.

| Wave | Theme | Findings | Ships via |
|---|---|---|---|
| **1** | Release truth — make the canary/staging/fleet rituals honest | RELVER-01, RELVER-02, RELVER-11, RELVER-07, SYNCE2E-07, VESTIG-02, RELVER-12 | web + worker |
| **2** | Director trust & feedback loop (Workstream 1) | DIRNAT-01, -03, -06, -09, -04, -05, SYNCE2E-06, FAILUX-07, FAILUX-09, N2W-03, DIRNAT-02, -07 | web now; native halves → train |
| **3** | Follower sync correctness & browse model (Workstream 2) | FOLNAT-01, -02, SYNCE2E-01, FOLNAT-04, SYNCE2E-05, FAILUX-06, N2W-04, W2N-04, PARITY-05, SYNCE2E-09, FAILUX-10, FOLWEB-12 | web + worker now; native halves → train |
| **4** | Web IA, onboarding, help & copy (Workstream 3) | IANAV-02, -07, FOLWEB-02, -03, -05, -07, IANAV-08, -09, -10, -12, FOLWEB-11, PARITY-10 | web |
| **5** | Fleet & version visibility (rest of Workstream 5) | RELVER-04, -05, -06, -09, FOLNAT-05, FOLNAT-07 | worker + web; native halves → train |
| **6** | Vestigial debt & comment drift (Workstream 6, minus VESTIG-02) | VESTIG-01, -03, -04, -05, -07, -08, -09, -11, -12, N2W-05 | web + worker; native halves → train |
| **🚂** | **Native release train** — build 382: all native halves from Waves 2–6, one TestFlight, canary ritual (as repaired by Wave 1), then fleet | (native halves listed above) | native-build |
| **7 — GATED** | Web-director emergency path (Workstream 4) — **only after Miguel approves the DECISION-REQUIRED register** | ROLEWEB-02 (ROLEWEB-01/-07 ship un-gated — see WS4 §2 and the * note) | web + worker |

*ROLEWEB-01's minimal messaging fix (honest copy for the dead end) may land in Wave 4 if Wave 7 is deferred — its chapter covers both variants.

Within Waves 1/5, the WS5 chapter's PR slicing is authoritative: execute WS5-PR1→PR2→PR3 in full during Wave 1 (their Wave-5-listed riders RELVER-04/-05/-09 land early); Wave 5 then consists only of WS5-PR4 (the native batch).

Wave-2/3 highs are the user-facing heart of this manual. If interrupted, ship completed waves rather than holding for the full program; every wave leaves the app strictly better.


---

## Workstream 1 — Director trust & feedback loop

> Repo: `(repo root)` · HEAD `d5075091` (build 381).
> All file:line anchors below were re-verified against this exact HEAD by the chapter author. If your
> working tree has moved past d5075091, re-verify each anchor with Grep before editing — the claims
> hold, but line numbers drift.

## 1. Workstream intro (WS1)

**Theme.** The director — a volunteer on a portrait iPad, in front of the congregation, zero training —
currently operates an instrument with almost no gauges. The app *acts* on their input (code entry,
"Sí, dirigir", exit, takeover) and *fails* on their behalf (mesh start failure, dead uplink, rejected
straggler publish, crash-reload) **silently**. Every finding in this workstream is one flavor of the
same defect: the system knows something the director needs to know at that exact second, and tells
them nothing — or worse, tells them something false (a spinning ⟳ that did nothing, a red banner
claiming their code was rejected when they just legitimately stepped down, English "Ok/Cancel"
buttons guarding the most destructive tap on the screen).

**Why it matters at Mass.** The director is the single point of control for the whole room: parish
iPads follow over the Multipeer mesh, and the congregation's personal phones follow signovivo.com via
the Cloudflare relay. Every silent failure here has congregation-wide blast radius, and the one
person who could fix it (re-enter the code, toggle the hotspot, stop swiping) is exactly the person
kept blind. The zero-training bar means the fix is never "they'll learn the quirk" — the screen must
say, in plain warm Spanish, what happened and what to do.

**How the findings interact.**

- **The director-code entry pipeline** (web numpad → `director-code` bridge message → native
  validation → confirm Alert → `becomeDirector`): DIRNAT-01 (reject is silent), DIRNAT-03 (post-confirm
  activation failure is silent), DIRNAT-07 (the boot resume prompt races the takeover warning). Fixes
  share one new additive bridge event (`director-code-result`) and one shared "Verificando…" web state.
- **The red warning banner** (`showRelayAuthWarning`, web/src/app.js:887-925) is the shared artery of
  four findings: FAILUX-09 (copy names no action; dismissal is permanent), SYNCE2E-06 (it false-fires
  on a legitimate step-down), N2W-03 (a WebView crash-reload destroys it permanently), DIRNAT-06 (it
  needs a second, network-outage variant). **Ordering rule: land SYNCE2E-06's empty-code guard before
  or with FAILUX-09's re-reveal timer** — a re-revealing *false* banner is worse than today's one-shot
  false banner. A web-side role guard (specified in SYNCE2E-06 step 4) belts this for old native shells.
- **The takeover pipeline**: DIRNAT-09 (the displaced director is never told) and DIRNAT-07 (the
  restart window manufactures accidental takeovers) are the two halves of the same two-director moment.
- **M4 status pill**: DIRNAT-06 *expands the planned M4 spec* (mesh peerCount AND relay publish health
  in one director surface). Several other fixes feed the future pill (DIRNAT-03's `connecting` state,
  DIRNAT-06's relay-health boolean, DIRNAT-09's conflict event) — the banner/Alert work below is the
  interrupt layer that ships now; the pill is the persistent layer that lands with M4.

**Not a single Swift line changes in this workstream.** Every native fix is shell JS
(`PdfReaderApp.tsx`, `src/directorRelaySync.js`) — simulator-verifiable, no M7 two-device gate
required to merge (though the mesh-adjacent behaviors get device-day spot-checks, noted per finding).

**WIRE COMPAT (applies to every fix below).** Native builds 368–381 with OLD bundled web copies stay
in the field. All bridge changes are additive-only: never remove/repurpose existing message fields;
new `payload.type` values are silently ignorable by old web bundles (verified: `applyNativeSyncEvent`
drops unknown types at the `if (payload.type !== "sync-event") return;` gate, web/src/app.js:967).
One trap to avoid is called out in N2W-03: do NOT overload the existing `relay-auth-error` type with
new semantics (e.g. `recovered:true`) — an old web bundle would false-show the banner. New semantics
⇒ new type.

**Recommended PR slicing (3 independently shippable PRs).**

| PR | Contents | Files | Ships via |
|---|---|---|---|
| **WS1-A — "Web: honest director & follower feedback"** | DIRNAT-01 (web half), DIRNAT-04, DIRNAT-05, FAILUX-07, FAILUX-09 (web half incl. banner copy + role guard), DIRNAT-06 part C (web banner variant) | `web/src/app.js`, `web/src/styles.css`, new `web/src/lib/svGestures.js` | Pages deploy (instant for phones; reaches iPads at next native build / mesh bundle push) |
| **WS1-B — "Shell: relay truth — publish health, straggler guard, warning persistence"** | SYNCE2E-06, N2W-03, DIRNAT-06 parts A+B, FAILUX-09 (native hook), DIRNAT-01 (native half) | `src/directorRelaySync.js`, `PdfReaderApp.tsx` | TestFlight build |
| **WS1-C — "Shell: role lifecycle alerts"** | DIRNAT-03, DIRNAT-09, DIRNAT-02, DIRNAT-07 | `PdfReaderApp.tsx`, new `src/roleBootstrap.js` | TestFlight build (can ride the same build as WS1-B; keep the PR separate for review) |

Repo rule reminder for WS1-B/C: bump `version.json` alongside `app.json`/`Info.plist`/`pbxproj` on
any native build (the bottom-right build label reads `version.json`).

Test-safety rule for the whole workstream: run test files **individually** (`node --test e2e/<file>`).
**NEVER `npm run test:e2e` and NEVER `e2e/relay-sync.test.mjs`** — that file publishes to the
PRODUCTION relay room and flips live followers' pages.

---

## 2. Findings (WS1)

#### DIRNAT-01 — Invalid/rejected director code on native gives ZERO feedback — numpad closes, nothing happens `high` `cross` `multi`

**Problem.** On the native iPad, a 5+-digit code typed into the ♪ numpad is posted to the shell and
the modal is immediately destroyed — before validation even starts. The shell rejects an unrecognized
code by injecting `{type:'role', role:'none'}`, whose web handler only hides the DIRECTOR badge; the
native comment claims the web "surfaces 'código incorrecto'", but that UI does not exist anywhere in
the bundle (`grep incorrecto web/src/` → zero hits). Two prior docs repeat the imagined UI.

**User impact at Mass.** A director who mistypes their 10-digit code pre-Mass (glass numpad, elderly
volunteer, low light) gets total silence — they cannot distinguish mistype from success-pending, may
walk to the ambo believing they are live, and the congregation never syncs.

**Evidence (verify before editing).**
- `web/src/app.js:1183-1186` — `goToDraftSong` native branch: `if (NATIVE_FILE_MODE || hasNativeBridge())` → posts `{type:'director-code'}` then `clearDraft(); closeSongJump();` — the feedback surface is destroyed before validation.
- `web/src/app.js:1189` — `flashSongDisplay("Código no válido", "err")` runs ONLY on the pure-web else branch.
- `PdfReaderApp.tsx:584-587` — unrecognized code → `injectEvent({type:'role', role:'none'})`; the comment at :585 ("tell the web so it surfaces 'código incorrecto'") describes UI that does not exist.
- `web/src/app.js:947-953` — role handler: `'none'` only sets `state.nativeSyncRole='off'` (:951) and re-renders the badge (:952). No error UI.
- `PdfReaderApp.tsx:613-621` — on a VALID code the native confirm Alert (title at :603-607, body :608-612) is the success surface; **Cancelar (:615) produces no bridge event at all** — the web can never distinguish "cancelled" from "still validating" today.
- `web/src/app.js:831-841` — `flashSongDisplay` infra (1600 ms auto-revert) already lives in the modal; reuse it.
- `docs/app-hardening-plan.md:576` and `:586` — prior docs assume the 'código incorrecto' UI exists; correct them in the same PR.

**Fix — step by step.**

*Native half (WS1-B, `PdfReaderApp.tsx`):*
1. In `onDirectorCode`, on the unrecognized-code path (:584-587), keep the legacy
   `injectEvent({type:'role', role:'none'})` (old bundles depend on it) and ADD, before it:
   `injectEvent({ type: "director-code-result", ok: false, reason: "unknown-code" });`
   Fix the stale comment at :585 to describe the new event.
2. Same additive event on the empty-code path (:572-575) with `reason: "empty"`.
3. In the confirm Alert (:613-621): valid code recognized → emit
   `injectEvent({ type: "director-code-result", ok: true })` immediately before `Alert.alert(...)`
   (semantics: "code accepted, native confirm now showing" — the web closes its numpad quietly).
   On the **Cancelar** button add an `onPress` that emits
   `injectEvent({ type: "director-code-result", ok: false, reason: "cancelled" })` so a new web
   bundle can close the pending state promptly instead of waiting out its timeout.
4. Wire compat: `director-code-result` is a NEW type → old web bundles drop it silently at
   app.js:967 (verified). `role:'none'` behavior is unchanged for old bundles.

*Web half (WS1-A, `web/src/app.js`):*
5. In `goToDraftSong`'s native branch (:1183-1186): do NOT `closeSongJump()` immediately. Instead:
   ```js
   postNativeBridge({ type: "director-code", code });
   clearDraft();
   state.pendingDirectorCodeAt = Date.now();
   songDisplay.textContent = "Verificando…";
   clearTimeout(pendingDirectorCodeTimer);
   pendingDirectorCodeTimer = setTimeout(() => {
     // Old shell + user cancelled the native confirm (no event ever arrives): close quietly.
     state.pendingDirectorCodeAt = 0;
     if (state.songJumpOpen) closeSongJump();
   }, 5000);
   ```
   (Declare `let pendingDirectorCodeTimer = 0;` module-level near `songDisplayFlashTimer`, :830.)
6. In `applyNativeSyncEvent` add a handler ABOVE the `sync-event` gate (:967), next to the
   `relay-auth-error` branch (:962):
   ```js
   if (payload.type === "director-code-result") {
     clearTimeout(pendingDirectorCodeTimer);
     state.pendingDirectorCodeAt = 0;
     if (payload.ok === false && payload.reason !== "cancelled") {
       if (!state.songJumpOpen) openSongJump();          // re-open for retry
       flashSongDisplay("Código incorrecto — revisa el número", "err");
     } else {
       if (state.songJumpOpen) closeSongJump();          // ok:true or cancelled → quiet close
     }
     return;
   }
   ```
7. Old-shell heuristic (covers native 368–381, which only send `role:'none'`): in the role handler
   (:947-953), before the state write, add:
   ```js
   if (payload.role === "none" && state.pendingDirectorCodeAt &&
       Date.now() - state.pendingDirectorCodeAt < 4000) {
     clearTimeout(pendingDirectorCodeTimer);
     state.pendingDirectorCodeAt = 0;
     if (!state.songJumpOpen) openSongJump();
     flashSongDisplay("Código incorrecto — revisa el número", "err");
   }
   ```
   Known ambiguity, accepted: on an old shell the DIRNAT-03 non-follower mesh-start failure also
   injects `role:'none'`; if that lands inside the 4 s window the flash says "Código incorrecto" for
   what was really an activation failure. Both are failures, and DIRNAT-03's fix (new shells) adds
   its own specific Alert — the ambiguity only exists on old shells and only in a ≲4 s race.
8. Success close: in the same role handler, `if (payload.role === "director") { clearTimeout(pendingDirectorCodeTimer); state.pendingDirectorCodeAt = 0; if (state.songJumpOpen) closeSongJump(); }`.
9. Update `docs/app-hardening-plan.md:576` and `:586` (and the `:863` mention) to reference the now-real
   `director-code-result` feedback instead of the imagined UI.

All user-visible strings: `"Verificando…"`, `"Código incorrecto — revisa el número"`.

**Acceptance criteria.**
- [ ] Native iPad, wrong 10-digit code → visible red flash "Código incorrecto — revisa el número" within 1 s, numpad still open for retry.
- [ ] Valid code → numpad shows "Verificando…" then closes quietly; native confirm Alert appears.
- [ ] Valid code + Cancelar on the Alert → numpad closes quietly within 5 s (instantly on new shells); NO error flash.
- [ ] Valid code + Sí → DIRECTOR badge appears; no error flash.
- [ ] Pure web (signovivo.com in Safari): behavior unchanged ("Código no válido" flash at :1189).
- [ ] Old web bundle + new shell: unchanged behavior (event silently dropped).

**Tests.** New `e2e/director-feedback-contract.test.mjs` (source-pin style like
`e2e/nearby-sync-contract.test.mjs`, run via `node --test e2e/director-feedback-contract.test.mjs`):
assert `PdfReaderApp.tsx` emits `director-code-result` with `ok: false` on the reject path and
`ok: true` before the confirm Alert; assert `web/src/app.js` contains the `director-code-result`
handler, the "Verificando…" pending state, and the `role === "none"` heuristic; assert the stale
"surfaces 'código incorrecto'" comment is gone. Manual (iPad portrait): run the four acceptance
flows above on a fleet iPad or the iPad simulator.

**Dependencies.** None to land the web half (the heuristic works against old shells). Native half
pairs naturally with DIRNAT-03 (same `onDirectorCode` region). Coordinate copy with DIRNAT-03.

---

#### DIRNAT-03 — After "Sí, dirigir" activation is fire-and-forget: no progress indicator for the 2–4 s+ mesh start, and a failed start silently demotes the just-confirmed director `high` `native` `native-build`

**Problem.** After the explicit confirm, `becomeDirector` starts the mesh with one 2 s-sleep retry and
zero "activating…" feedback; the DIRECTOR badge appears only on success. There are two silent failure
shapes: (a) the promise-rejection shape (a raced `DIRECTOR_TAKEOVER_REQUIRED`, session-invalid, or
bridge failure) lands in a catch that silently demotes the just-confirmed director; (b) the async
shape (Bluetooth off / Local Network denied) — Swift's `startDirector` resolves BEFORE advertising is
attempted, so the badge shows while `DIRECTOR_START_FAILED` fires later and is dropped by the JS
listener, which only handles `DIRECTOR_CONFLICT`. Shape (b) is the more deceptive: badge present,
mesh dead (the relay leg still publishes).

**User impact at Mass.** The one moment the operator explicitly asserted control is the one moment
the app may silently refuse — they face the congregation believing they are live, and either the
absent badge (shape a) or a present-but-lying badge (shape b) is the only signal. Parish iPads never
follow.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:492-501` — mesh start + one 2 s-sleep retry (sleep at :498); success feedback (badge via `role:'director'`) only at :508.
- `PdfReaderApp.tsx:516-527` — the catch: `wasFollower ? becomeFollower() : injectEvent({type:'role',role:'none'})` — no dialog, banner, or message.
- `PdfReaderApp.tsx:932-944` — JS mesh `error` listener acts only on `DIRECTOR_CONFLICT` (:934); every other code (incl. `DIRECTOR_START_FAILED`) is dbgLog'd and dropped.
- `ios/SignoVivo/DirectorSyncModule.swift:1611-1616` — `didNotStartAdvertisingPeer` emits `DIRECTOR_START_FAILED` (:1615) with the comment "so the director UI can warn the user" — dead copy today. **This fires AFTER `startDirector` already resolved at :384**, so it can never hit the :516 catch. (corrected — the finding's original framing implied the catch could see Bluetooth-off failures; it cannot.)
- `ios/SignoVivo/DirectorSyncModule.swift:1617-1629` — M-F7 backoff retries advertising forever (fast ×5, then every 45 s) — transients self-heal, so the warn must not imply permanence.
- `ios/SignoVivo/DirectorSyncModule.swift:366` — `DIRECTOR_TAKEOVER_REQUIRED` rejection ("Solicita permiso para tomar control") is swallowed by the catch, and no request-permission UI exists (takeover auto-denied at `PdfReaderApp.tsx:946-952`).
- `web/src/app.js:861-878` — `setSyncWorking` already spins the ⟳ fab on `searching`/`connecting`/`resolving-conflict` state events — the free "activating" indicator.

**Fix — step by step** (all in `PdfReaderApp.tsx`; no Swift changes).

1. **Latency window** — first line inside the `try` at :492 (before `startNearbyDirector`):
   ```ts
   injectEvent({ type: "sync-event", event: { type: "state", status: "connecting" } });
   ```
   At this moment the device is still non-director (`data-role="follower"`), so the ⟳ fab is visible
   and `setSyncWorking` spins it — zero new web UI. On success the role flip hides the fab.
2. **Shape (a), the catch (:516-527)** — after the existing recovery logic, show a retry Alert,
   guarded on the role generation so a superseding flip suppresses a stale dialog:
   ```ts
   } catch {
     if (myGen !== roleGenerationRef.current) return;
     if (wasFollower) becomeFollower();
     else injectEvent({ type: "role", role: "none" });
     Alert.alert(
       "No se pudo activar el modo director",
       "Revisa que Bluetooth y Wi-Fi estén encendidos e inténtalo de nuevo.",
       [
         { text: "Reintentar", onPress: () => becomeDirector(code) },
         { text: "Cancelar", style: "cancel" },
       ],
     );
   }
   ```
   (`becomeDirector` re-bumps the generation on entry, so Reintentar is race-safe. Keep the existing
   comment about non-stranding; extend it to note the Alert.)
3. **Shape (b), the dropped async error** — extend the mesh listener's `error` case (:932-944).
   Add a ref near the other refs (~:120): `const directorStartFailedNotifiedRef = useRef(false);`
   ```ts
   case "error": {
     dbgLog("mesh:error", { code: event.code });
     const errCode = String(event.code ?? "");
     if (errCode === "DIRECTOR_CONFLICT") {
       /* existing branch unchanged (see DIRNAT-09 for its new Alert) */
     } else if (errCode === "DIRECTOR_START_FAILED" && roleRef.current === "director") {
       // Latched once per director session: M-F7 retries forever and would otherwise spam.
       if (!directorStartFailedNotifiedRef.current) {
         directorStartFailedNotifiedRef.current = true;
         Alert.alert(
           "Los iPads cercanos no te reciben",
           "Revisa que Bluetooth y Wi-Fi estén encendidos. La app seguirá intentando conectar. Los teléfonos en signovivo.com sí reciben tu página.",
           [{ text: "Entendido" }],
         );
       }
     }
     break;
   }
   ```
   Copy note: the relay leg genuinely keeps working in this shape (broadcast gating at :358-368 is on
   `roleRef`, which IS `"director"`), so the last sentence is true and prevents a panic-restart.
4. Reset the latch on every role entry: `directorStartFailedNotifiedRef.current = false;` at the top
   of `becomeDirector` (:456 area) and `becomeFollower` (:421 area).
5. Optional pairing (skip if tight): also emit `director-code-result ok:false reason:'mesh-start'` in
   the catch — the DIRNAT-01 web surface would flash it. The native Alert is the primary surface; the
   extra event is additive and harmless.

**Acceptance criteria.**
- [ ] With `startDirector` forced to reject twice (dev stub): confirm → visible Spanish Alert with Reintentar within ~5 s; device remains a functioning follower; Reintentar re-runs the promotion.
- [ ] With Bluetooth+Wi-Fi off (device): confirm → badge appears AND "Los iPads cercanos no te reciben" Alert appears once (not repeatedly); radios back on → mesh recovers by itself (M-F7); no further Alert until the next director session.
- [ ] Between confirm and success, the ⟳ fab spins (connecting state).
- [ ] `Cancelar` on the failure Alert leaves the device in its recovered role (follower or none) with no further dialogs.

**Tests.** Extend `e2e/director-feedback-contract.test.mjs`: source-pin that the catch contains
`"No se pudo activar el modo director"` with a Reintentar action; that the error listener handles
`DIRECTOR_START_FAILED` gated on `roleRef.current === "director"` with a latch ref; that the
`connecting` state inject precedes `startNearbyDirector`. Manual (iPad portrait, device day): the
radios-off flow above — this cannot be proven in the simulator (Multipeer requires hardware).

**Dependencies.** Ships with WS1-C. DIRNAT-09 rewrites the same `error` case — implement together to
avoid merge conflicts. DIRNAT-01's native half touches the same `onDirectorCode`.

---

#### DIRNAT-06 — M4 DELTA: relay publishes failing with NETWORK errors (not 401/403) are invisible to the director — and the planned M4 pill spec covers mesh peerCount only `high` `cross` `multi`

**Problem.** `doPublish` swallows every network throw/abort in a bare `catch {}` and warns only on
HTTP 401/403 — a persistent uplink outage (hotspot drop, captive portal) is neither, so the retry
loop stays silent for the whole Mass while the mesh keeps parish iPads perfectly in sync and every
signovivo.com follower freezes. The planned M4 director pill ("● Dirigiendo — N conectados") is
specced from mesh peerCount only; the relay leg's live health is unspecced (DIAGNÓSTICO shows relay
status, but it is a pre-Mass long-press screen). This section is BOTH the immediate fix AND the
**expanded M4 spec** (mesh + relay health in one director-facing surface).

**User impact at Mass.** The director's screen looks perfect (mesh green) while phones and the old
home-screen-PWA iPad freeze on the last page and demote to "sin director" after 90 s. The one person
who could fix it (toggle hotspot, move a few pews) gets no signal for the entire Mass — the same
blast radius as the 401 class the banner was built for.

**Evidence (verify before editing).**
- `src/directorRelaySync.js:97-99` — bare `catch {}`: network failures/aborts swallowed; infinite silent retry during a persistent outage.
- `src/directorRelaySync.js:87-96` — the ONLY warning path is `res.status === 401 || 403`; the comment asserts 5xx/429 are "transient" — a dead uplink is neither.
- `PdfReaderApp.tsx:342-347` — the auth handler wiring (`injectEvent({type:'relay-auth-error'})`) is the only relay-failure signal in the app.
- `PdfReaderApp.tsx:401-412` — the existing 12 s relay heartbeat (the natural sampling cadence for publish health).
- `docs/major-update-2026-07.md:368-372` — planned M4 pill: director sees "● Dirigiendo — N conectados" (`peerCount`) — mesh-only.
- `docs/major-update-2026-07.md:441` — DIAGNÓSTICO (Ask 7 / §6.6) starts at :441 **(corrected — the finding cited :443-449)**; it is a pre-Mass long-press screen, not a live signal.
- `PdfReaderApp.tsx:921-929` — the bridge forwards only `status`/`role`/`message` on state events; Swift's `peerCount` (emitted on every state event, `ios/SignoVivo/DirectorSyncModule.swift:1425`) is dropped at the bridge.

**Fix — step by step.**

*Part A — publish-health tracking (WS1-B, `src/directorRelaySync.js`).*
1. Track server contact, distinct from auth success (a 401 means the server IS reachable — it must
   feed the auth banner, never the network banner):
   ```js
   let lastServerContactAt = 0;   // any resolved fetch (even 401/403/5xx): the uplink works
   export const getRelayPublishHealth = () => ({ lastServerContactAt });
   ```
   In `doPublish`, first line after `const res = await fetch(...)` resolves (i.e. inside the `try`,
   after :81): `lastServerContactAt = Date.now();`. The `catch {}` (:97-99) intentionally does NOT
   update it — that is the signal.
2. In `setRelayPublishCode` (:36-41), when the new code is non-empty, seed
   `lastServerContactAt = Date.now();` — a synthetic "contact" so the warning can only fire ≥45 s
   after a fresh director entry (grace period, no false alarm during the first seconds).

*Part B — native sampling + additive event (WS1-B, `PdfReaderApp.tsx`).*
3. Add a ref: `const relayHealthWarnedRef = useRef(false);`. Inside the existing 12 s relay
   heartbeat callback (:401-412), after the publish call:
   ```ts
   const { lastServerContactAt } = getRelayPublishHealth();
   const dark = lastServerContactAt > 0 && Date.now() - lastServerContactAt > 45000;
   if (dark && !relayHealthWarnedRef.current) {
     relayHealthWarnedRef.current = true;
     injectEvent({ type: "relay-health", ok: false });
   } else if (!dark && relayHealthWarnedRef.current) {
     relayHealthWarnedRef.current = false;
     injectEvent({ type: "relay-health", ok: true });   // auto-recovery signal
   }
   ```
   The heartbeat already only runs while director/transmitter (:402), so no extra role gate is
   needed. Reset `relayHealthWarnedRef.current = false` in `becomeFollower` and on exit-director.
   Wire compat: `relay-health` is a NEW type → old web bundles drop it silently (app.js:967).
4. Also forward `peerCount` additively on state events (:921-929): add
   `peerCount: Number(event.peerCount) || 0,` to the injected event object. Old web bundles ignore
   the extra field (their `state` handler reads only `event.status`, app.js:972-975). This is the
   M4 pill's mesh input, landed early so the pill needs no native change later.

*Part C — web banner variant (WS1-A, `web/src/app.js`).*
5. Generalize the banner: refactor `showRelayAuthWarning(status)` (:887-925) into
   `showDirectorWarning(kind)` with `kind ∈ {"code-rejected","no-internet"}` — same element, same
   styles, per-kind message text kept in a small map; add `hideDirectorWarning()` (removes `is-on`).
   Keep exported/behavioral compatibility: the `relay-auth-error` handler (:962) now calls
   `showDirectorWarning("code-rejected")` (copy per FAILUX-09).
6. Handle the new type next to it:
   ```js
   if (payload.type === "relay-health") {
     if (payload.ok === false) showDirectorWarning("no-internet");
     else if (currentDirectorWarningKind === "no-internet") hideDirectorWarning();
     return;
   }
   ```
   (Track `currentDirectorWarningKind` module-level so a relay recovery can never hide a
   code-rejected banner — the auth class has its own lifecycle, FAILUX-09/N2W-03.)
   Copy for `no-internet`:
   `"Sin internet — los teléfonos en signovivo.com no están recibiendo tu página. Los iPads cercanos siguen bien."`
   Auto-hide on `ok:true`; keep the × dismiss too.

*Part D — the EXPANDED M4 pill spec (spec-only here; implement in M4).* This replaces the M4 line
"the director sees '● Dirigiendo — N conectados' (peerCount)" (`docs/major-update-2026-07.md:368-372`):
- The director pill takes **two inputs** and renders both legs:
  `"● Dirigiendo — {N} iPads · web ✓"` / `"● Dirigiendo — {N} iPads · web ✗"`.
  - **Mesh leg**: `peerCount` from the state events forwarded in step 4.
  - **Relay leg**: a web-side boolean driven by the `relay-health` events from step 3 (`✗` while the
    latest event is `ok:false`; `✓` otherwise). Same 45 s freshness rule; no new wire messages needed
    beyond steps 3-4 — the pill is pure web rendering once this workstream lands.
- On a transmitter-only device (no mesh) the pill shows the relay leg only: `"● Dirigiendo — web ✓/✗"`.
- The banner from Part C remains the interrupt (appears/disappears); the pill is the persistent
  glance state. DIAGNÓSTICO (§6.6, :441) additionally shows "última publicación web: hace Xs" from
  `getRelayPublishHealth()`.
- Update `docs/major-update-2026-07.md` §6.3 (:368-372) with this two-input spec when M4 starts.

**Acceptance criteria.**
- [ ] Directing with the uplink killed (airplane-mode the hotspot, keep Bluetooth/Wi-Fi LAN up): banner "Sin internet — los teléfonos…" appears within ~60 s; mesh followers unaffected throughout.
- [ ] Restore the uplink: banner hides by itself within ~24 s (two heartbeats); no user action needed.
- [ ] A retired code (401 storm) shows the code-rejected banner, NEVER the no-internet banner (server contact is fresh).
- [ ] Fresh director entry with no internet: no banner before ~45 s (grace).
- [ ] Old web bundle + new shell: no banner, no errors (event dropped silently).
- [ ] State events now carry `peerCount` (verify via `?selftest` or a debug log) without changing old-bundle behavior.

**Tests.** New `e2e/relay-warning-lifecycle.test.mjs` (behavioral unit; import
`src/directorRelaySync.js` — it is dependency-free ESM — and **mock `globalThis.fetch` before any
call**; the module hardcodes the PROD relay URL, so an unmocked call would hit production): assert
`lastServerContactAt` updates on resolved fetches (incl. 401) and NOT on rejected fetches; assert the
seed on `setRelayPublishCode`. Source-pin the `relay-health` sampling block in `PdfReaderApp.tsx` and
the `peerCount` forwarding. Extend the same file with pins for the web `relay-health` handler + both
copy strings. Manual (iPad portrait): the two banner flows above on a real hotspot.

**Dependencies.** SYNCE2E-06 must land first or together (a stepped-down straggler must not pollute
health/auth signals). Part C shares the banner refactor with FAILUX-09 — implement the
`showDirectorWarning(kind)` refactor ONCE in WS1-A covering both findings.

---

#### DIRNAT-09 — A taken-over (demoted) director is never told: DIRECTOR_CONFLICT's ready-made Spanish message is dead copy, the badge silently vanishes, and their page snaps to the winner's under their fingers `high` `cross` `native-build`

**Problem.** Takeover-by-conflict is the DESIGNED handoff mechanism (admin force-takeover rides it).
The losing director's shell handles `DIRECTOR_CONFLICT` by stopping the heartbeat, demoting to
follower, and pulling the winner's snapshot — but never reads `event.message`, so Swift's ready-made
explanation ("Un nuevo director tomó el control…") is dead copy, the badge just vanishes, and the
page jumps under their fingers. Meanwhile the system explicitly warns the *displacer* ("le quitarás
el control") — it informs the wrong human.

**User impact at Mass.** The two-directors moment is precisely when the room is already confused; the
ex-director flails — keeps swiping (now local-only) or re-enters their code and triggers the red
warning / control ping-pong. One sentence on screen ends it.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:932-944` — `DIRECTOR_CONFLICT` branch (:934): stops heartbeat, `becomeFollower()`, pulls winner snapshot; `event.message` never read; nothing user-visible injected.
- `ios/SignoVivo/DirectorSyncModule.swift:1546` — Swift ships the explanation: "Un nuevo director tomó el control. Este dispositivo cambió a modo seguidor." (emitError :1431-1436) — dead copy; likewise "Cediendo el control al nuevo director..." at :490 (forwarded as a state `message` but dropped by the web).
- `web/src/app.js:972-975` — the web state handler uses only `event.status` for the ⟳ spinner; forwarded mesh `message` strings are ignored everywhere.
- `PdfReaderApp.tsx:612` — the displacer's confirm body says "le quitarás el control" **(corrected — the finding cited :609; :609 is the liveDirector variant "tú tomas el control…todos te seguirán a ti"; the asymmetry claim is unchanged)**.
- `PdfReaderApp.tsx:946-952` — admin force-takeover rides the conflict path (comment :947-948) — confirming this is a designed, expected flow, not an edge case.

**Fix — step by step** (shell JS only, `PdfReaderApp.tsx`, WS1-C).
1. In the `DIRECTOR_CONFLICT` branch (:934-943), after `becomeFollower()` and the snapshot pulls,
   add:
   ```ts
   Alert.alert(
     "Otro director tomó el control",
     "Este iPad ahora sigue al nuevo director. Si debes dirigir tú, vuelve a entrar tu código en ♪.",
     [{ text: "Entendido" }],
   );
   ```
   A native Alert survives the concurrent role churn and WebView re-renders; no web change needed.
   (Do not gate on `roleRef` — Swift only emits `DIRECTOR_CONFLICT` while its own role is director,
   `DirectorSyncModule.swift:1543-1548`, so the event is trustworthy by construction.)
2. Deliberately do NOT read `event.message` for the Alert body: Swift's string is correct today, but
   pinning UI to a mesh-delivered string couples copy to the Swift build; the hardcoded shell copy
   above is equivalent and versioned with the Alert. (If you disagree and prefer
   `String(event.message)` with the shell copy as fallback, that is also safe — both are additive.)
3. Defer (parking lot, do not build now): surfacing mesh state `message` strings as a small web
   toast — bigger surface, belongs with the M4 pill work.

**Acceptance criteria.**
- [ ] Two devices: A directs, B enters a code and confirms "Tomar el control" → A shows "Otro director tomó el control" within ~2 s of its badge vanishing; A now follows B (page snaps once, then tracks).
- [ ] A taps Entendido → normal follower behavior; ⟳ works; no repeat Alert.
- [ ] A re-enters their code after the Alert → the RED "Ya hay un director activo" warning appears (B is live) — the ping-pong now requires an informed, deliberate choice.
- [ ] Admin force-takeover (super-admin code on another device) produces the same informed demotion.

**Tests.** Extend `e2e/director-feedback-contract.test.mjs`: source-pin that the `DIRECTOR_CONFLICT`
branch contains `"Otro director tomó el control"` AFTER the `becomeFollower()` call (regex with
`[\s\S]*` ordering, house style). Manual (2-device day, iPad portrait): the takeover flow above —
Multipeer conflict cannot be simulated.

**Dependencies.** Same `error`-case rewrite as DIRNAT-03 step 3 — implement together in WS1-C.
Pairs with held M-F6 (followers' redirect hint) in the M7 batch; add a "loser is informed" assertion
to M7 NEW-DIR-3's acceptance flow (doc note, `docs/major-update-2026-07.md:150`).

---

#### DIRNAT-04 — Exit-director confirm is window.confirm: iOS system alert with hardcoded ENGLISH "Ok"/"Cancel" buttons and an empty title; "volverás a seguidor" is also false on the transmitter-only path `medium` `web` `web-only`

**Problem.** Tapping the DIRECTOR badge — the most destructive tap on the director's screen — runs
`window.confirm`, which react-native-webview presents as a UIAlertController with an empty title and
hardcoded English "Ok"/"Cancel" buttons regardless of locale. The Spanish body also promises
"volverás a seguidor", which is false on a transmitter-only device (it drops to standalone "off"
with no follower transport).

**User impact at Mass.** Elderly Spanish-speaking volunteers with a zero-training bar are gated by
two English words when stepping down mid-Mass; meaning is inferable from the body text (hence
medium), but it reads broken and invites the wrong tap.

**Evidence (verify before editing).**
- `web/src/app.js:2426-2431` — badge tap → `window.confirm("¿Salir del modo director?\n\nDejarás de dirigir y volverás a seguidor.")` (:2428) — the only confirm-dialog in the app.
- `node_modules/react-native-webview/apple/RNCWebViewImpl.m:1222-1230` — `runJavaScriptConfirmPanelWithMessage` presents `alertControllerWithTitle:@""` (:1224) with `actionWithTitle:@"Ok"` (:1225) / `@"Cancel"` (:1228) — English regardless of locale. **(corrected/precise — the finding cited :1225, which is inside the block; read from the MAIN checkout `/Users/cazares/src/alvernia-reader/node_modules/...` — this worktree has no node_modules.)**
- `PdfReaderApp.tsx:763-774` — transmitter-only exit drops to `roleRef="off"` + `role:'none'` with no follower transport — "volverás a seguidor" is untrue for that class. (Branch spans :763-774; the finding's :764 is within it.)
- `web/src/app.js:937-939` — `bridge-state` carries `available` (= native `syncAvailable`) into `state.nativeBridgeAvailable` — the web CAN distinguish the transmitter-only class for truthful copy.

**Fix — step by step** (WS1-A, `web/src/app.js` + `web/src/styles.css`).
1. Build a small in-page confirm dialog following the banner pattern (injected `<style>` + element,
   like `showRelayAuthWarning` :887-925, or static markup in `index.html` — either is fine; injected
   keeps the change single-file). Structure: fixed centered card, title, body, two buttons stacked
   full-width (portrait iPad, elderly fingers — minimum 44 px tap height):
   - Title: `"¿Salir del modo director?"`
   - Body (truthful for both device classes): `"Dejarás de dirigir en este iPad."` — optionally
     branch: if `state.nativeBridgeAvailable` append `" Este iPad volverá a seguir al director."`;
     else append `" Este iPad quedará en modo lectura."`
   - Button 1 (default, prominent): `"Seguir dirigiendo"` → close dialog, nothing else.
   - Button 2 (destructive red): `"Salir del modo director"` → `postNativeBridge({ type: "exit-director" })`, close dialog.
2. Replace the `window.confirm` call at :2426-2431 with `openExitDirectorDialog()`. Backdrop tap and
   Escape close = "Seguir dirigiendo" (safe default).
3. Post exactly ONE `exit-director` per confirm (disable the button on first tap until the dialog
   closes) — the shell's exit path is idempotent-ish but don't rely on it.
4. Ship-vector caveat (document in the PR body): this surface only exists inside the native shell,
   so the web-only fix reaches directors at the next native build or mesh bundle push — NOT on Pages
   deploy alone.

**Acceptance criteria.**
- [ ] Badge tap → fully-Spanish dialog with "Seguir dirigiendo" / "Salir del modo director"; no English anywhere.
- [ ] "Seguir dirigiendo" (and backdrop tap) → still directing: badge stays, mesh heartbeats uninterrupted, page broadcasts continue.
- [ ] "Salir del modo director" → exactly one `exit-director` posted; device becomes follower (mesh class) or read-only (transmitter class) exactly as today.
- [ ] `grep -n "window.confirm" web/src/app.js` → zero callers.

**Tests.** Extend `e2e/director-feedback-contract.test.mjs`: assert `web/src/app.js` has NO
`window.confirm(` callers; assert the dialog copy strings exist; assert exactly one code path posts
`{ type: "exit-director" }`. Manual (iPad portrait): badge tap flow above on a fleet iPad — confirm
buttons are comfortably tappable in portrait with the badge at top-left.

**Dependencies.** None. Land in WS1-A. (DIRNAT-05 excludes the badge from edge-swipe handlers —
unrelated code paths, no conflict.)

---

#### DIRNAT-05 — The 44 px left-edge drawer-swipe zone hijacks the director's "previous page" swipe in portrait; an accidental drawer open followed by any browse tap broadcasts to the whole congregation `medium` `web` `web-only`

**Problem.** Any rightward swipe starting <44 px from the left bezel opens the drawer (travel
threshold 40 px), while a page-turn swipe requires startX ≥ 44 (and 48 px travel) — so a director's
natural back-page thumb arc from the left bezel can never turn the page and always opens the drawer.
A window-level duplicate handler widens the trap, the DIRECTOR badge sits inside the zone, and every
drawer/browse tap broadcasts: drawer tap → `renderPage` → `page-changed` → `broadcastPage` → mesh +
relay. Neither handler consults `html[data-role]`.

**User impact at Mass.** Mid-Mass the director flips back for a repeated refrain; a drawer slides
over their music instead; a hurried dismissal tap on a song row yanks every device in the room. There
is no un-broadcast browsing for a director.

**Evidence (verify before editing).**
- `web/src/app.js:2725` — `startX < 44 && deltaX > 40` → `openDrawer()` wins; drawer wins at a LOWER travel threshold (40) than page turns (48).
- `web/src/app.js:2733` — page-turn branch hard-requires `startX >= 44`.
- `web/src/app.js:2789-2810` — window-level duplicate: touchstart arms on `clientX < 44` (:2792); touchend opens on `dx > 40` (:2806).
- `web/src/app.js:849` — `data-role` is set here and never read by any gesture code (verified by grep).
- `web/src/styles.css:154-157` — `.director-mode-badge` at top-left, `left: max(0.55rem, …)` (:157) ≈ 9 px, height 4 rem (:165) — inside the edge zone for the top strip. **(corrected — cite the block :154-157, not just :157; note the badge only enlarges the trap for swipes starting in that top strip.)**
- `web/src/app.js:1062-1067` → `PdfReaderApp.tsx:721` — every render posts `page-changed` → `broadcastPage` → the whole room follows the director's stray browse.
- `web/src/index.html:55` — badge element id is `director-mode-badge`.

**Fix — step by step** (WS1-A).
1. Create `web/src/lib/svGestures.js` — UMD, ES5, dependency-free, never-throw (match the style of
   `web/src/lib/svSyncDecision.js`) — exporting one pure function:
   ```js
   // decideEdgeGesture({ startX, dx, dy, role }) ->
   //   "drawer" | "page-next" | "page-prev" | null
   // Directors get a narrower drawer zone (24px) and a longer open-travel (80px)
   // so a natural back-page swipe from the bezel wins; followers keep 44/40
   // (the edge swipe is their ONLY drawer entry — do not shrink it for them).
   ```
   Decision table: `EDGE = role === "director" ? 24 : 44`; `TRAVEL = role === "director" ? 80 : 40`;
   drawer iff `startX < EDGE && dx > TRAVEL && |dx| > |dy|`; else page iff `|dx| > 48 && |dx| > |dy|
   && startX >= EDGE` (`dx < 0` → "page-next", else "page-prev"); else null.
2. Load it in `web/src/index.html` next to the other lib scripts (before app.js, `<script defer>`),
   and copy it in `web/build.mjs` alongside the existing `lib/` copies (follow how
   `svSyncDecision.js` is wired — same three touch points).
3. Rewire the `viewerShell` touchend (:2714-2740) to call the helper with
   `role: document.documentElement.dataset.role || "follower"` and act on the verdict (keep the
   existing `preventDefault` and `haptic()` calls per branch; keep a conservative inline fallback if
   the lib failed to load, mirroring app.js's other lib guards).
4. Same constants in the window-level handler (:2789-2810): arm on `clientX < (isDirector ? 24 : 44)`
   and open on `dx > (isDirector ? 80 : 40)`.
5. Exclude the badge from BOTH touchstart handlers (viewerShell :2700-2712 and window :2789-2797):
   ```js
   if (event.target && event.target.closest && event.target.closest("#director-mode-badge")) {
     state.touchStart = null; /* or edgeSwipe = null */ return;
   }
   ```
6. Do NOT change follower behavior (IANAV-02: the edge swipe is currently their only drawer entry).
7. Parking lot (do not build): a director "vista previa → Ir aquí" deferred-broadcast browse mode.

**Acceptance criteria.**
- [ ] As director (portrait): a rightward swipe starting 10–40 px from the bezel turns the page BACK; a slow deliberate drag >80 px from <24 px still opens the drawer.
- [ ] As follower: edge-swipe behavior byte-identical to today (44/40).
- [ ] Touches starting on the DIRECTOR badge never open the drawer or turn pages (they open the exit dialog only).
- [ ] Page-turn swipes in the middle of the screen unchanged for both roles.

**Tests.** New `e2e/svGestures.test.mjs` (`node --test e2e/svGestures.test.mjs`, plain unit like
`e2e/svSyncDecision.test.mjs`): matrix over role × startX ∈ {5,10,23,24,30,43,44,60} × dx ∈
{-60,39,41,48,79,81} × dy dominance — assert the table above, especially: director + startX 30 +
dx 60 → "page-prev" (the exact Mass gesture), follower same input → "drawer" would be wrong (startX
30 < 44 and dx 60 > 40 → "drawer" for follower — correct, unchanged). Manual (iPad portrait,
director role): thumb-arc back-page swipe from the bezel × 10 — expect 10/10 page turns, 0 drawers.

**Dependencies.** None hard. DIRNAT-04's dialog makes an accidental badge tap recoverable — land both
in WS1-A. Ship-vector caveat: reaches director iPads at the next native build / mesh bundle push.

---

#### SYNCE2E-06 — Step-down straggler publish drains with an EMPTY code, 401s by design (C3), and false-fires the red banner on a device that just correctly stopped directing `medium` `native` `native-build`

**Problem.** `becomeFollower` clears the relay publish code (C3) — and that setter ALSO re-arms the
one-shot auth latch. If a publish was in flight with a queued `pending` page turn, the `finally`
drain sends the queued payload reading the now-empty code at fetch time → guaranteed 401 → the
freshly re-armed latch fires the auth handler → the web shows the red "code rejected / followers not
synced" banner on a device that just legitimately stepped down. Both claims are false: the winning
director is publishing fine.

**User impact at Mass.** A director demoted by a mesh conflict (or who voluntarily exited) with a
page turn in flight gets a latched red mid-Mass banner inviting a panicked code re-entry that would
fight the winning director.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:430` — `becomeFollower` → `setRelayPublishCode("")` (C3).
- `src/directorRelaySync.js:36-41` — `setRelayPublishCode` ALSO re-arms the latch (`authErrorNotified = false` at :40).
- `src/directorRelaySync.js:100-107` — the `finally` drain (`doPublish(next)` at :106) sends the queued payload; the code is read at FETCH time (:77), now empty.
- `sync-worker/src/index.ts:782-787` — `codeOk` requires `code.length > 0` (:785) → empty code is a guaranteed 401 (:787).
- `src/directorRelaySync.js:89-96` — 401 + re-armed latch → handler fires.
- `PdfReaderApp.tsx:342-347` — the bridge forwards `relay-auth-error` with NO role guard.
- `web/src/app.js:962-963` → `:912` — the banner claims the code was rejected and signovivo.com followers are NOT synced.

**Fix — step by step.**
1. *(WS1-B, `src/directorRelaySync.js` — the root fix.)* Head-guard `doPublish` so an empty code
   drops the payload without fetching and without warning — C3's rejection intent achieved one hop
   earlier, and the relay saves a guaranteed-401 round trip:
   ```js
   const doPublish = async (payload) => {
     // Step-down guard: after becomeFollower()/exit-director the code is cleared (C3). A queued
     // straggler must be DROPPED here — not sent with an empty code to a guaranteed 401 that
     // false-fires the auth banner on a device that just correctly stopped directing.
     if (!relayPublishCode) { inFlight = false; pending = null; return; }
     inFlight = true;
     ...
   ```
   (The guard also covers the `finally` drain — the recursive `doPublish(next)` hits the same head
   guard and cleanly resets the coalescer state.)
2. Belt: in `publishPageToRelay` (:111), first line: `if (!relayPublishCode) return;` — don't even
   queue when there is no code. (Broadcast gates in the shell already make this rare; cheap belt.)
3. Verify no regression to the REAL 401 class: a live director whose code was rotated out has a
   non-empty code → still fetches → still 401s → still warns once. (This is FAILUX-09's territory;
   the guard must not touch it.)
4. *(WS1-A, `web/src/app.js` — defense-in-depth for OLD shells 368–381, which will keep false-firing
   until they update.)* In the banner show path (`showDirectorWarning("code-rejected")` /
   `showRelayAuthWarning`), suppress when this device is not currently a broadcaster:
   ```js
   if (state.nativeSyncRole !== "director") return;  // a follower has no publishes to reject
   ```
   Safe because: the transmitter-only class asserts `role:'director'` over the bridge
   (PdfReaderApp.tsx:663-669), so genuine broadcasters always have `nativeSyncRole === "director"`;
   a demoted/exited device flips to follower/off BEFORE the straggler 401 resolves in the common
   ordering. (A rare race where the 401 lands first is accepted — the native head-guard is the
   durable fix; this is only the old-shell belt.)

**Acceptance criteria.**
- [ ] Simulated step-down with a pending publish (unit): no POST issued after the code clears; no handler fire; coalescer state (inFlight/pending) fully reset.
- [ ] Live director with a retired (non-empty) code: banner still fires exactly once (existing behavior preserved).
- [ ] Exit-director / mesh-conflict demotion on device with rapid page turns in flight: NO red banner appears.
- [ ] Web belt: with an old shell simulated (inject `relay-auth-error` while `nativeSyncRole` is `"follower"`), no banner.

**Tests.** New `e2e/relay-warning-lifecycle.test.mjs` (shared with DIRNAT-06/N2W-03): import
`src/directorRelaySync.js` with **`globalThis.fetch` mocked before any call** (the module hardcodes
the PROD relay URL — an unmocked run would hit production). Cases: (1) code empty → publish → fetch
never called; (2) in-flight publish (slow mock) + `setRelayPublishCode("")` + queued pending →
resolve → second fetch never called, `pending` cleared; (3) non-empty retired code → 401 mock →
handler fires exactly once across a 401 burst; (4) ok response re-arms. Use fresh module state per
test via `await import("../src/directorRelaySync.js?case=" + n)`. Optionally extend
`sync-worker/test/a2.test.mjs` (LOCAL wrangler only, via `bash sync-worker/test/run-a2.sh`) with:
publish with empty `X-Director-Code` → 401. Manual: exit-director on an iPad while flipping pages
quickly — no banner.

**Dependencies.** **Must land before or with FAILUX-09's re-reveal timer** (a re-revealing false
banner would be a regression). DIRNAT-06's health tracking sits in the same `doPublish` — implement
both edits in one WS1-B pass.

---

#### FAILUX-07 — ⟳ resync gives identical fake-success feedback whether it worked or the network is dead `medium` `web` `web-only`

**Problem.** The ⟳ fab's spin is purely cosmetic: `is-spinning` is added before `reconnectRelay()`
and removed on a fixed 1100 ms timer, independent of any outcome. `reconnectRelay` never learns
whether the reconnect/poll succeeded (poll failures are swallowed), and the native path just posts
`{type:"resync"}` and returns. Since build ~378 the same fab also spins on native mesh state
transitions — likewise on a fixed timer, not outcome-driven.

**User impact at Mass.** During an outage the follower's single recovery affordance animates
convincingly, changes nothing, and reports nothing — training elderly users that the button does
nothing exactly when trust in it matters most.

**Evidence (verify before editing).**
- `web/src/app.js:2415-2420` — tap handler: `is-spinning` added (:2417), removed on a fixed 1100 ms timeout (:2418), before/regardless of `reconnectRelay()` (:2419).
- `web/src/app.js:3088-3107` — `reconnectRelay` returns void; native branch posts `{type:"resync"}` and returns (:3093-3095); web branch never inspects outcomes.
- `web/src/app.js:3209` — `relayPollOnce` swallows all failures in a bare `} catch {}`. **(note — the confirmed-findings JSON's own correction suggested "~3208"; re-verified at HEAD: the catch is exactly :3209, the original citation was right.)**
- `web/src/app.js:861-878` — `setSyncWorking` (builds 378–381) spins the same fab for mesh `searching`/`connecting` — also a fixed 1100 ms per transition; shares the `is-spinning` class (interference risk the fix must handle).
- `PdfReaderApp.tsx:727-752` — native resync handler re-requests/re-asserts but reports nothing back.

**Fix — step by step** (WS1-A, `web/src/app.js`).
1. Make `relayPollOnce` report: return `true` when `r.ok` and the snapshot was applied (i.e. reach
   the end of the `r.ok` block, :3186-3208), `false` from the catch (:3209) and non-ok statuses.
   (Signature stays compatible — all existing callers ignore the return.)
2. Rework the tap handler (:2415-2420) into an outcome-driven bounded wait:
   ```js
   let resyncPending = false;
   resyncFab.addEventListener("click", async () => {
     if (resyncPending) return;                       // no stacking on rapid taps
     resyncPending = true;
     resyncFab.classList.add("is-spinning");
     const ok = await runResyncWithOutcome(4000);     // see steps 3-4
     resyncFab.classList.remove("is-spinning");
     resyncPending = false;
     if (ok) {
       resyncFab.classList.add("is-ok");              // brief green success tick
       setTimeout(() => resyncFab.classList.remove("is-ok"), 800);
     } else {
       showResyncToast(hasNativeBridge() || NATIVE_FILE_MODE
         ? "Aún no se encuentra al director — la app sigue buscando"
         : "Sin conexión — revisa el wifi");
     }
   });
   ```
3. `runResyncWithOutcome(timeoutMs)`:
   - WEB path: run the existing `reconnectRelay()` web body, then `return await relayPollOnce(true)`
     raced against the timeout (a WS reconnect that applies a snapshot also counts — simplest
     reliable signal is the forced poll's boolean).
   - NATIVE path: post `{type:"resync"}`, then resolve `true` if a native `sync-event` page event is
     APPLIED within the window: set `state.resyncWaiter = resolve` and call it from
     `applyNativeSyncEvent`'s page branch (:977-980); on timeout resolve `false`. Note the native
     resync re-injects the last snapshot unconditionally when one exists (PdfReaderApp.tsx:742-750),
     so "director alive" reliably produces an event even when already on the right page.
4. Guard the spinner collision: in `setSyncWorking` (:863-878), first line
   `if (resyncPending) return;` — while a tap-initiated wait owns the fab (≤4 s), mesh transitions
   must not strip the class early.
5. `showResyncToast(msg)`: a minimal bottom-center pill (reuse the `#sv-golive-bar` styling pattern,
   :3048-3058: fixed, bottom, rounded, dark), auto-hide after 3 s, `aria-live="polite"`. No buttons.
6. es-MX copy (exact): `"Sin conexión — revisa el wifi"` (web), `"Aún no se encuentra al director — la app sigue buscando"` (native).

**Acceptance criteria.**
- [ ] Web follower, relay reachable: ⟳ spin ends when the snapshot applies (<4 s), brief success tick, no toast.
- [ ] Web follower, airplane mode: spin runs the full 4 s, then stops + toast "Sin conexión — revisa el wifi".
- [ ] Native follower with live director: spin ends on the re-asserted page event (<4 s), no toast.
- [ ] Native follower, no director broadcasting: spin ends at 4 s + toast "Aún no se encuentra al director — la app sigue buscando"; mesh keeps searching (no behavior change underneath).
- [ ] Rapid double/triple taps: one wait, no stacked spins, no stuck `is-spinning`.

**Tests.** New `e2e/resync-feedback.test.mjs` (source-pin): assert the fixed
`setTimeout(() => resyncFab.classList.remove("is-spinning"), 1100)` pattern is GONE from the tap
handler; assert `relayPollOnce` returns a boolean on both paths; assert both toast strings exist;
assert the `resyncPending` no-stacking guard and the `setSyncWorking` guard. Manual (iPad portrait +
a phone on signovivo.com): the four flows above.

**Dependencies.** None. Independent of the M4 pill (complements it). Land in WS1-A.

---

#### FAILUX-09 — The 401 relay-auth banner names the problem but not the action, and never re-shows after dismissal while the failure persists `medium` `cross` `multi`

**Problem.** The banner copy states that followers are not synced but gives no recovery action, and
it opens with "El relé" — electrical-relay jargon that means nothing to this audience. The × removes
it permanently: the native latch re-arms only on a successful publish or a fresh code entry, and with
a retired code neither ever happens — one dismissal buys a signal-free outage for the rest of Mass.
(Merged DIRNAT-08: the banner also never auto-clears after genuine recovery.)

**User impact at Mass.** A director with a retired code sees the banner once, dismisses it mid-panic,
and then has zero remaining signal while every web follower stays frozen. Even undismissed, it never
says WHAT to do.

**Evidence (verify before editing).**
- `web/src/app.js:912` — current copy: "El relé rechazó el código de director. Los seguidores en signovivo.com NO están sincronizados." — problem only, no action, banned jargon.
- `web/src/app.js:918` — × handler just removes `is-on`; re-reveal requires the event to fire again.
- `src/directorRelaySync.js:89-90` — `authErrorNotified` latches on the first 401/403; re-arms ONLY on `res.ok` (:88) or `setRelayPublishCode` (:36-41) — impossible with a retired code the banner never tells them to replace.
- `web/src/app.js:888-892` — `showRelayAuthWarning` is idempotent/re-revealing — the web half of persistence is nearly free.
- `web/src/app.js:962-963` — the `relay-auth-error` payload is the only trigger.

**Fix — step by step.**

*Web half (WS1-A — instant for new bundles).*
1. Replace the :912 copy entirely (kills "relé", adds the action; this is the `code-rejected` kind
   in DIRNAT-06's `showDirectorWarning(kind)` refactor — one refactor serves both findings):
   `"Tu código de director no fue aceptado: los teléfonos en signovivo.com no están recibiendo tu página. Toca DIRECTOR (arriba a la izquierda) para salir, y vuelve a entrar tu código en ♪. Si sigue igual, avisa a Miguel."`
2. Role guard on show (shared with SYNCE2E-06 step 4): `if (state.nativeSyncRole !== "director") return;`.
3. Re-reveal while the failure persists: track `codeRejectedActive = true` on show. On × (:918),
   schedule `setTimeout(() => { if (codeRejectedActive && state.nativeSyncRole === "director") relayAuthWarningEl.classList.add("is-on"); }, 60000)`.
   Clear `codeRejectedActive` (and cancel the timer, and hide the banner) when: (a) a
   `relay-auth-recovered` event arrives (step 5), or (b) the role changes away from director, or
   (c) a fresh `role:'director'` assert follows a new code entry — cover (b)+(c) in the role handler
   (:947-953): any role event while `codeRejectedActive` → if new role ≠ director, clear+hide; if
   new role = director AND a `director-code-result ok:true` was seen since the banner showed
   (DIRNAT-01 gives this signal), clear+hide.
   Simplest robust rule if that bookkeeping feels heavy: clear on ANY role event. A re-entered
   still-bad code re-fires the event chain anyway (native latch re-armed by `setRelayPublishCode`),
   so the banner legitimately returns.
4. Auto-clear on recovery (DIRNAT-08 half): on `relay-auth-recovered` (new type, step 5) →
   `hideDirectorWarning()` + clear state.

*Native half (WS1-B — makes persistence exact instead of heuristic).*
5. In `src/directorRelaySync.js`, add a recovery hook next to the auth handler (N2W-03 uses it too):
   ```js
   let relayRecoveredHandler = null;
   export const setRelayRecoveredHandler = (fn) => { relayRecoveredHandler = typeof fn === "function" ? fn : null; };
   ```
   In the `res.ok` branch (:87-88), fire it exactly on the failure→ok transition:
   ```js
   if (res && res.ok) {
     if (authErrorNotified && relayRecoveredHandler) {
       try { relayRecoveredHandler(); } catch {}
     }
     authErrorNotified = false;
   }
   ```
6. In `PdfReaderApp.tsx`, register it next to the auth handler (:342-347):
   `setRelayRecoveredHandler(() => { relayAuthErrorRef.current = null; injectEvent({ type: "relay-auth-recovered" }); });`
   **WIRE COMPAT — do NOT reuse `relay-auth-error` with a `recovered` flag**: old web bundles
   (368–381) call `showRelayAuthWarning(payload.status)` on ANY `relay-auth-error` payload — a
   recovery event on the old type would false-SHOW the banner. `relay-auth-recovered` is a new type,
   silently dropped by old bundles (app.js:967).
7. (Optional native alternative to the web timer — every-Nth 401 re-fire — is NOT needed once steps
   1–6 land; skip it to keep the latch semantics simple.)

**Acceptance criteria.**
- [ ] Retired code mid-Mass → banner with the new action copy; × → banner returns ≤60 s while publishes still 401.
- [ ] Director exits/steps down → banner (if showing) hides and never re-reveals.
- [ ] New valid code entered → banner clears and does not return; a NEW later failure warns again.
- [ ] Genuine recovery (code re-added server-side, publish succeeds): banner hides by itself (new shell) — no user action.
- [ ] Normal page turns never show it; the word "relé" appears nowhere in the bundle (`grep -rn "relé" web/src/` → 0).

**Tests.** Extend `e2e/relay-warning-lifecycle.test.mjs`: unit (mocked fetch) — 401 → handler once;
ok-after-401 → recovered handler exactly once; ok-without-prior-401 → recovered handler NOT called.
Source-pin: new copy string present, old "El relé rechazó" string absent, `relay-auth-recovered`
emitted from the shell and handled by the web, re-reveal timer + role-clear logic present. Manual
(iPad portrait): with a deliberately wrong code baked into a dev build config — banner → dismiss →
returns in ~60 s; enter the good code → gone for good.

**Dependencies.** **Requires SYNCE2E-06** (or at minimum its web role-guard, step 4 there) before
enabling the re-reveal timer. Shares the banner refactor with DIRNAT-06 part C and the shell hook
with N2W-03 — implement the three together (WS1-A + WS1-B).

---

#### N2W-03 — relay-auth-error is a lossy one-shot: a WebView crash-reload permanently destroys the only "web congregation is dark" warning `medium` `native` `native-build`

**Problem.** The auth-error latch lives in the RN runtime and fires the handler exactly once per
failure episode; the banner is plain in-page DOM. After any WebView content-process reload (common on
old iPads under memory pressure) the banner is wiped, `bridge-ready` re-asserts role and page but has
no relay-auth re-assert, and the latch never re-fires for a persistently-bad code. Variant: an
auth-error injected during the reload window is queued into `pendingInjectRef`, which every
terminate/remount path clears — dropped before delivery with the latch already spent.

**User impact at Mass.** By its own comment the banner is the ONLY signal that every signovivo.com
follower has gone dark on a rejected/rotated code. After a crash-reload the director's iPad looks
completely healthy while the web congregation stays frozen for the rest of Mass.

**Evidence (verify before editing).**
- `src/directorRelaySync.js:89-96` — latch set at :90, handler fires once; re-arm only on `res.ok` (:88) or a new `setRelayPublishCode` (:36-41).
- `PdfReaderApp.tsx:638-696` — `bridge-ready` re-asserts role (:663-669) and page (:670-695) but has NO relay-auth re-assert.
- `PdfReaderApp.tsx:1092` — `onContentProcessDidTerminate` clears `pendingInjectRef` — a queued auth-error dies here; also cleared at :315 (watchdog remount) and :959 (`bundleUpdated`).
- `web/src/app.js:886-925` — the banner is in-page DOM (`relayAuthWarningEl`), reload-wiped, user-dismissible (:918), no persistent state behind it; its comment (:881-885) confirms it is the only web-congregation-dark signal.
- `web/src/app.js:888-892` — `showRelayAuthWarning` is idempotent/re-revealing — the web side needs nothing for the re-assert to work.

**Fix — step by step** (WS1-B, `PdfReaderApp.tsx` + one hook in `src/directorRelaySync.js`).
1. Add a shell ref near the other refs (~:120):
   `const relayAuthErrorRef = useRef<{ status: number; at: number } | null>(null);`
2. Set it where the handler fires (:342-347):
   ```ts
   setRelayAuthErrorHandler((status: number) => {
     relayAuthErrorRef.current = { status, at: Date.now() };
     injectEvent({ type: "relay-auth-error", status });
   });
   ```
3. Clear it on the two genuine exits from the failure state:
   - New code entry: in `becomeDirector` immediately after `setRelayPublishCode(code)` (:459):
     `relayAuthErrorRef.current = null;`
   - Successful publish: via the `setRelayRecoveredHandler` hook installed in FAILUX-09 step 6
     (which already does `relayAuthErrorRef.current = null`). If FAILUX-09's native half is deferred,
     install the hook here — it is this finding's dependency too.
   - Also clear in `becomeFollower` (:419-450) and the transmitter exit branch (:763-774): a
     non-broadcaster has no publishes to warn about (pairs with SYNCE2E-06's semantics).
4. Re-assert at `bridge-ready`: in the handler, right after the role assert (:669):
   ```ts
   if ((roleRef.current === "director" || explicitTransmitterRef.current) && relayAuthErrorRef.current) {
     injectEvent({ type: "relay-auth-error", status: relayAuthErrorRef.current.status });
   }
   ```
   Web side needs nothing: `showRelayAuthWarning` re-reveals idempotently (:888-892), and this uses
   the EXISTING type with existing semantics — fully compatible with old bundles. This also cures the
   pending-inject drop variant: whatever was dropped, the next `bridge-ready` re-asserts from the ref.

**Acceptance criteria.**
- [ ] With a rejected code: banner visible → force a content-process kill (dev: simulator "Simulate Memory Warning" or a dev-menu reload) → after reload the banner re-appears WITHOUT a new page turn or a new 401.
- [ ] New valid code entered → reload → banner does NOT re-appear.
- [ ] Recovery (publish succeeds) → reload → banner does NOT re-appear.
- [ ] Follower/exited device → reload → no banner (ref cleared on demotion).

**Tests.** Extend `e2e/relay-warning-lifecycle.test.mjs` (source-pin, house style like the existing
bridge-contract pins): assert the `bridge-ready` case contains a `relayAuthErrorRef` re-inject gated
on director/transmitter; assert the ref is nulled in `becomeDirector`, `becomeFollower`, and the
exit-director transmitter branch; assert the handler registration writes the ref before injecting.
Manual (iPad portrait): the crash-reload flow above on a device (content-process kills are hard to
force in CI).

**Dependencies.** Shares the recovered-hook with FAILUX-09 (install once). Lands in WS1-B with
SYNCE2E-06 and DIRNAT-06. The M4 pill later becomes the persistent home for "relay dark"; M3's
hello/welcome handshake is the natural long-term carrier for re-asserted warning state — this fix is
forward-compatible with both (the re-assert simply moves into `welcome` when M3 lands).

---

#### DIRNAT-02 — H3/#267 DELTA: persisted transmitter-director role is unreadable — the boot resume prompt is gated on syncAvailable, which is false on exactly the device class that writes the breadcrumb `low` `native` `native-build`

**Problem.** The H3 fix (#267) persists `lastSyncRole='director'` on the transmitter-only path
(reachable only when `!syncAvailable`) precisely so the boot resume prompt fires — but the prompt's
ONLY reader sits inside an effect that opens with `if (!syncAvailable) return;`, and `syncAvailable`
is a per-install constant. The breadcrumb is write-only dead code for its own scenario. Compounding:
the transmitter-only exit path never clears the breadcrumb, so once the gate is fixed, an intentional
exit would leave a stale 'director' → false resume prompt every boot.

**User impact at Mass.** The exact outage #267 was shipped to close remains open on no-mesh devices:
a restarted transmitter-director comes back as a silent "off" with every signovivo.com follower
frozen and no 401 signal. Today no parish device is in this class (all have the Swift module), but
the fix's claimed coverage is wrong and M7's acceptance test only exercises the mesh path — device
verification would pass while this stays broken.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:472` — H3 writer: `AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "director")` in the transmitter branch, reachable only when `!syncAvailable` (:461); the comment (:465-471) says it exists so "the boot resume prompt fire[s]".
- `PdfReaderApp.tsx:850` — the bootstrap effect opens with `if (!syncAvailable) return;`; the ONLY repo-wide reader of `lastSyncRole` is the "Estabas dirigiendo" Alert at :869-882, inside it (grep: writes at :432, :472, :506; remove at :551; sole `getItem` at :869).
- `src/nearbyDirectorSync.js:6` — `syncAvailable` source: `Platform.OS === "ios" && Boolean(nativeModule)` — per-install constant, memoized once at `PdfReaderApp.tsx:128`. **(corrected — the finding's original evidence said repo-root `nearbyDirectorSync.js`; the file is under `src/`.)**
- `PdfReaderApp.tsx:763-774` — the transmitter-only exit branch has NO `lastSyncRole` write (mesh exit writes 'follower' via `becomeFollower` :432).

**Fix — step by step** (WS1-C, `PdfReaderApp.tsx` + new `src/roleBootstrap.js`).
1. Extract the decision into a pure helper, new file `src/roleBootstrap.js` (plain ESM, no RN
   imports, like `directorRelaySync.js`):
   ```js
   // decideRoleBootstrap(persistedRole, syncAvailable) ->
   //   { showResumePrompt: boolean, action: "become-follower" | "persist-follower" }
   export const decideRoleBootstrap = (persistedRole, syncAvailable) => ({
     showResumePrompt: persistedRole === "director",
     action: syncAvailable ? "become-follower" : "persist-follower",
   });
   ```
2. Split the bootstrap OUT of the mesh effect: delete the `didBootstrapRef` block from inside the
   `if (!syncAvailable) return;` effect (:857-883) and add a new standalone effect with NO gate:
   ```ts
   useEffect(() => {
     if (didBootstrapRef.current) return;
     didBootstrapRef.current = true;
     AsyncStorage.getItem(STORAGE_KEYS.lastSyncRole)
       .then((prev) => {
         const d = decideRoleBootstrap(prev, syncAvailable);
         if (d.showResumePrompt) {
           Alert.alert(
             "Estabas dirigiendo",
             "La app se reinició y ahora sigues al director como los demás. Para volver a dirigir, reingresa tu código en el teclado (♪).",
             [{ text: "Entendido" }],
           );
         }
       })
       .catch(() => {})
       .finally(() => {
         if (syncAvailable) becomeFollower();      // persists 'follower' itself (:432)
         else AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower").catch(() => {});
       });
   }, [becomeFollower, syncAvailable]);
   ```
   (The existing copy is reused verbatim; DIRNAT-07 appends one sentence to it — coordinate.) Keep
   the mesh effect's `primeNearbyPermissions()` + listener registration exactly as they are.
3. Clear the breadcrumb on intentional transmitter exit: in the `exit-director` transmitter branch
   (:763-774), alongside the ref flips, add
   `AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower").catch(() => {});`
4. Update the H3 comment (:465-471) — it currently claims the prompt fires; after this fix it will
   actually be true.

**Acceptance criteria.**
- [ ] Simulator with the Swift module stubbed out (`NativeModules.DirectorSyncModule` = undefined): enter a code (transmitter path), kill the app, relaunch → "Estabas dirigiendo" prompt fires.
- [ ] Same device: exit-director (badge), relaunch → NO prompt (breadcrumb cleared).
- [ ] Mesh device (normal fleet iPad): behavior unchanged — director crash-relaunch → prompt; intentional exit → no prompt; boot always lands as follower.
- [ ] The `(syncAvailable × persistedRole)` matrix passes in unit tests: (true,'director')→prompt+become-follower; (false,'director')→prompt+persist-follower; (·,'follower'|null)→no prompt.

**Tests.** New `e2e/role-bootstrap.test.mjs` (`node --test e2e/role-bootstrap.test.mjs`): unit the
`decideRoleBootstrap` matrix (4×2). Extend `e2e/director-feedback-contract.test.mjs`: source-pin
that the "Estabas dirigiendo" reader is NOT inside a `!syncAvailable`-gated effect (e.g. assert the
new effect exists without the gate and the transmitter exit branch writes `lastSyncRole`). Manual:
the simulator flows above (stub the module by temporarily renaming it in a dev build, or run in Expo
Go where the module is absent).

**Dependencies.** None hard; lands in WS1-C. Touches the same boot prompt as DIRNAT-07 (copy append)
— implement together. Doc note: add the transmitter (no-mesh) restart case to M7 NEW-DIR-1
acceptance criteria (`docs/major-update-2026-07.md:150`).

---

#### DIRNAT-07 — Post-restart window bypasses the "Ya hay un director activo" takeover warning: the boot resume prompt urges immediate code re-entry while lastDirectorSnapshotRef is still null `low` `native` `native-build`

**Problem.** Director A crashes; B takes over and keeps Mass going. A relaunches and is told to
re-enter their code — but the red takeover warning requires a mesh snapshot fresher than 8 s, and
`lastDirectorSnapshotRef` is null until mesh discovery + connect + B's first page lands, which can
take longer than typing 10 digits. A gets the calm "¿Dirigir el coro?" confirm and — following the
prompt in good faith — demotes B. The boot prompt manufactures exactly the race NEW-DIR-3 defanged,
and M7's acceptance test covers only the calm case.

**User impact at Mass.** Control ping-pong in front of the congregation: A hijacks B without meaning
to, B is (pre-DIRNAT-09) never told, and the room follows whoever won last. (Nuance kept honest: the
calm prompt's body does end with "Si otro director ya está activo, le quitarás el control." — a
plain-text warning at :612 — so "unknowingly" slightly overstates; the RED warning + explicit
"Tomar el control" button is the designed safeguard this window bypasses.)

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:872-876` — boot prompt (title "Estabas dirigiendo" :873, body with "reingresa tu código en el teclado (♪)" :874) fires straight from the AsyncStorage read; `becomeFollower()` only starts in `.finally` (:881) — mesh connect runs after.
- `PdfReaderApp.tsx:598-602` — `liveDirector` requires a snapshot younger than `LIVE_DIRECTOR_WINDOW_MS` (8000, :71); null snapshot → calm variant (:603-612).
- `PdfReaderApp.tsx:899` — the SOLE write of `lastDirectorSnapshotRef` is the mesh `page` listener — null until B's first heartbeat arrives.
- `PdfReaderApp.tsx:612` — the calm body's trailing plain-text warning ("…le quitarás el control.") — the only mitigation in the window.
- `docs/major-update-2026-07.md:150` — M7 NEW-DIR-3 acceptance tests only the calm no-live-director case; the hijack window is untested.

**Fix — step by step** (WS1-C, `PdfReaderApp.tsx`).
1. **Copy hardening (cheap, ship now).** Append one sentence to the boot prompt body (:874):
   `"La app se reinició y ahora sigues al director como los demás. Para volver a dirigir, reingresa tu código en el teclado (♪). Si otra persona ya está dirigiendo, avisa antes de entrar tu código."`
   (Coordinate with DIRNAT-02, which moves this Alert into the new ungated effect — apply the copy
   there.)
2. **DECISION-REQUIRED (Miguel) — the unknown-liveness grace window.** When the code confirm is
   reached while liveness is UNKNOWN (no snapshot AND mesh still searching/connecting), the shell
   can wait ~3 s for evidence before committing:
   - Track mesh status: add `const meshStatusRef = useRef("");` and set it in the state case
     (:914-930): `meshStatusRef.current = String(event.status ?? "");`
   - In `onDirectorCode`, compute `livenessUnknown = !snap && (meshStatusRef.current === "searching" || meshStatusRef.current === "connecting")`.
   - When `livenessUnknown`, the calm confirm's "Sí, dirigir" `onPress` routes through a grace probe
     instead of calling `becomeDirector(code)` directly: fire `requestCurrentSnapshot()` +
     `refreshNearbyDiscovery()`, then `setTimeout(3000)`; on expiry, if a fresh snapshot (<8 s) has
     landed, re-show the RED variant ("⚠️ Ya hay un director activo" / "Tomar el control"
     destructive / "Cancelar"); else proceed to `becomeDirector(code)`. Bound it: one probe per code
     entry; skip the probe entirely when a snapshot already exists or mesh is `connected`.
   - **Option A (recommended): ship step 1 (copy) in WS1-C now; implement the grace probe behind the
     M7 native batch**, because (i) it adds a 3 s promotion delay for a genuinely solo director on
     every cold-start entry in the unknown state — a live-Mass UX cost Miguel should sign off on, and
     (ii) its correctness depends on real Multipeer discovery timing that only the M7 two-device day
     can verify. **Option B: ship both now** — the delay is bounded, once, and only in the ambiguous
     state; the M7 day then just verifies. Recommendation: **A** — the copy plus DIRNAT-09's "loser is
     informed" Alert already reduce the harm from silent-hijack to visible-and-recoverable, and the
     probe deserves device-verified timing before it gates a promotion at Mass.
3. Doc note (either option): add the hijack-window case to M7 NEW-DIR-3's acceptance list
   (`docs/major-update-2026-07.md:150` / §8): "A restarts while B directs; A re-enters the code
   within 10 s of boot → A sees the RED warning (or a re-confirm), never a silent calm promotion."

**Acceptance criteria.**
- [ ] Boot prompt shows the extended copy (with the "avisa antes de entrar tu código" sentence).
- [ ] (If Option B) Two devices: A restarts while B directs; A enters the code within ~10 s of boot → A sees the RED "Ya hay un director activo" warning (possibly after the ≤3 s probe), never a silent calm promotion.
- [ ] (If Option B) Solo restart, no other director: calm prompt → confirm → promotion completes, delayed at most once by ≤3 s.
- [ ] Calm-case NEW-DIR-3 behavior (exit → wait >2 s → new code = calm prompt) unchanged.

**Tests.** Extend `e2e/director-feedback-contract.test.mjs`: source-pin the extended boot-prompt
copy; (Option B) pin the `livenessUnknown` probe (meshStatusRef tracking + the 3 s
`requestCurrentSnapshot` race + red re-prompt). If Option B, extract the liveness verdict into the
pure helper family (`src/roleBootstrap.js`: `decideLiveness(snapshotAgeMs, meshStatus, windowMs) ->
"live" | "unknown" | "none"`) and unit it in `e2e/role-bootstrap.test.mjs` across (snapshotAge ×
meshStatus). Manual (M7 two-device day, iPad portrait): the A/B restart-hijack flow above — this is
the one flow in the workstream that fundamentally requires two physical devices.

**Dependencies.** DIRNAT-02 (same Alert moves into the new bootstrap effect — apply the copy there);
DIRNAT-09 (the harm-reduction backstop: if a hijack still happens, B is now told). Ships in WS1-C.

---

## 3. Chapter close-out checklist (for the executor)

- [ ] All three PRs green on the safe test files: `node --test e2e/director-feedback-contract.test.mjs e2e/relay-warning-lifecycle.test.mjs e2e/svGestures.test.mjs e2e/role-bootstrap.test.mjs e2e/resync-feedback.test.mjs` (run individually if the multi-file form trips anything). Never the glob; never `e2e/relay-sync.test.mjs`.
- [ ] `grep -rn "relé" web/src/` returns nothing.
- [ ] WS1-B/C bumped `version.json` + `app.json` + `Info.plist` + `pbxproj` together.
- [ ] Wire-compat spot-check before merge: new web bundle against a 368-era shell simulation (no `director-code-result`, no `relay-health`, no `relay-auth-recovered` ever arrive) — every new web feature degrades to today's behavior or better (the DIRNAT-01 heuristic, the FAILUX-09 role-guard); new shell against the OLD bundled web copy — every new inject type is silently dropped (app.js:967 gate).
- [ ] The one **DECISION-REQUIRED (Miguel)**: DIRNAT-07 grace probe (Option A copy-only now vs Option B probe now) — do not implement Option B without the sign-off.
- [ ] Doc updates riding along: `docs/app-hardening-plan.md:576/:586/:863` (DIRNAT-01), `docs/major-update-2026-07.md` M4 pill spec §6.3 + M7 NEW-DIR-1/NEW-DIR-3 acceptance additions (DIRNAT-06, DIRNAT-02, DIRNAT-07, DIRNAT-09).


---

## Workstream 2 — Follower sync correctness & browse model

> Verified against HEAD `d5075091` (build 381). Every file:line in this chapter was re-read at this
> HEAD by the chapter author; citations marked **(corrected)** fix drift found in the audit records.
> Findings source: `docs/ia-audit-2026-07/confirmed-findings.json` + per-lens files in the same dir.

## 2.0 Orientation for the executor (read first — you have no other context)

**The system.** One web bundle (`web/src/` → built to `web/dist/`) runs in two places: (a)
signovivo.com (Cloudflare Pages, prod branch `main`) for phone/PWA followers, and (b) inside a
WKWebView in the native iOS shell (`PdfReaderApp.tsx` at repo root) on the parish iPads, loaded from
`file://` (a bundled copy, optionally overridden by a peer-pushed `Documents/WebBundle`). The web
detects the shell via `NATIVE_FILE_MODE` (web/src/app.js:227) and `hasNativeBridge()` (app.js:307).

**Two sync transports, strictly split today:**
- **Relay** (web followers only): director's shell publishes page numbers to a Cloudflare Worker
  (`sync-worker/src/index.ts`, one Durable Object per room, prod room `alvernia-main`) via
  `src/directorRelaySync.js`; web followers consume it over WS `/subscribe` + `/state` polls; the
  follow decision is the pure lib `web/src/lib/svSyncDecision.js`. The relay consumer is **hard-off
  in the shell**: `startRelayFollow` early-returns at app.js:3329.
- **Mesh** (native followers only): Multipeer (`ios/SignoVivo/DirectorSyncModule.swift`); the
  director's shell re-sends its page every **1s** (PdfReaderApp.tsx:393-400); follower shells inject
  pages into the web bundle via `window.__signoVivoReceiveNativeEvent` → `applyNativeSyncEvent`
  (app.js:927-990).

**WIRE COMPAT (non-negotiable, applies to every fix below).** Native builds 368–381 with their OLD
bundled web copies stay in the field indefinitely, and mesh bundle-push can pair an old shell with a
NEW web bundle (and, via a stale `Documents/WebBundle`, a new shell with an OLD bundle). Therefore:
additive-only. Never remove/rename/repurpose an existing bridge or relay message field. New bridge
payload types and new fields must be silently ignorable by old peers — verified safe channels:
unknown `payload.type` falls out of `applyNativeSyncEvent` (app.js:967) inside a whole-body
try/catch (app.js:934); unknown `event.type` inside `sync-event` likewise; native ignores unknown
web→native `msg.type` (PdfReaderApp.tsx:797-798); extra fields on known messages are unread by old
code. Worker responses keep their exact shape (`{ok, seq, ignored?, rateLimited?}`).

**Ship vectors.** `worker-only` = deploy sync-worker, effective for everyone instantly.
`web-only` = Pages deploy, instant on phones/PWA — but parish iPads only pick it up at the **next
native build** (their bundle is baked) or a mesh bundle push. `native-build` = TestFlight archive
(which also bakes the current `web/dist` — so land web changes first).

**Test safety (hard rules).**
- NEVER run `npm run test:e2e` (it globs `./e2e/*.test.mjs`, which includes
  `e2e/relay-sync.test.mjs` — that file **publishes to the PRODUCTION relay room** and would flip
  live followers' pages). NEVER run `e2e/relay-sync.test.mjs` by any means.
- Run individual safe files only: `node --test e2e/svSyncDecision.test.mjs`,
  `node --test e2e/svRelayRoom.test.mjs`, `node --test e2e/nearby-sync-contract.test.mjs`,
  `node --test e2e/permission-flow.test.mjs`.
- Worker tests: `bash sync-worker/test/run-a2.sh` — verified LOCAL-ONLY: it boots `wrangler dev`
  on 127.0.0.1 and `sync-worker/test/a2.test.mjs` throws at load unless `RELAY_TEST_BASE`/
  `RELAY_TEST_CODE` are set, and refuses any base matching `/signovivo|workers\.dev/`
  (a2.test.mjs:11-20). It reads `sync-worker/.dev.vars` (gitignored); if absent, create a local
  one with a dummy `TRANSMITTER_CODES=1234567890` — it only ever feeds local miniflare.
- Run no builds you don't need; a native archive is only required for PR-WS2-C verification.

## 2.1 Theme — why this workstream, why it matters at Mass

Every finding here is a way a **follower** ends up on the wrong page, or wedged, or lied to about
being live, or punished for looking around — with elderly congregants at a live Mass as the users
and "zero training" as the bar. The headline (FOLNAT-01) is that the flagship parish iPads have
**no browse-away model at all**: the same ♪ jump that the web sanctions with an amber pill and a
"Volver a en vivo" bar is, on native, a 1-second trap that snaps back on every heartbeat — the
songbook reads as haunted. Around it cluster the correctness holes: a fast device clock silently
freezes the whole web congregation to one page per ~90s (SYNCE2E-01); forced polls can visibly
rewind a follower under a green pill (SYNCE2E-05); a blank page after a wifi blip is unrepairable
by every recovery affordance (FAILUX-06); one bad page asset turns an iPad into a ~1s strobing
error loop that slams the drawer shut (N2W-04); and a crash-relaunch dumps a follower on page 2
(FOLNAT-04). The failure-honesty trio (FOLNAT-02, W2N-04, PARITY-05) covers followers that
*cannot* sync but look perfectly normal — denied Local Network permission, a wedged mesh start,
or a transmitter that exited director mode wearing follower clothes with a dead ⟳.

**How they interact.**
- FOLNAT-01 and N2W-04 (web half) both discipline the same code path — the mesh `page` branch of
  `applyNativeSyncEvent` — and should share one new pure decision helper (`decideMeshPage`,
  §FOLNAT-01 step 2 / §N2W-04 step 2). Land together.
- SYNCE2E-05 and FAILUX-06 both touch the follow decision/executor (`svSyncDecision.js` +
  `applyRelaySnapshot` + `goLive`). FAILUX-06's repaint hook relies on SYNCE2E-05's "force + equal
  seq still follows" behavior to fire on ⟳ against a stationary director — land SYNCE2E-05 first
  or together.
- FOLNAT-04 and FOLWEB-12 both edit the boot block at app.js:3446-3451; do them in one pass.
- FOLNAT-02's banner clears on the mesh `state:"connected"` event — the same event stream
  FOLNAT-01 consumes; no coupling beyond both reading `sync-event`s.
- PARITY-05 adds an additive `canFollow` field on the `role` bridge event; nothing else reads it.

**Recommended PR slicing (3 independently shippable PRs, in this order):**

| PR | Scope | Findings | Ship vector | Verify with |
|---|---|---|---|---|
| **PR-WS2-A** — "worker: publish correctness" | `sync-worker/src/index.ts` + `sync-worker/test/a2.test.mjs` | SYNCE2E-01, SYNCE2E-09 | worker-only (deploy `npx wrangler deploy` from `sync-worker/`) | `bash sync-worker/test/run-a2.sh` |
| **PR-WS2-B** — "web bundle: follower sync correctness + browse model" | `web/src/app.js`, `web/src/lib/svSyncDecision.js`, `web/src/styles.css` | SYNCE2E-05, FAILUX-06, FOLNAT-01, N2W-04 (web half), FOLNAT-04 (web half), FAILUX-10, FOLWEB-12 | web-only (Pages; iPads at next build) | `node --test e2e/svSyncDecision.test.mjs` + browser/manual |
| **PR-WS2-C** — "native shell: start-failure honesty + role truth" | `PdfReaderApp.tsx` (+ tiny web halves already in B's file set) | FOLNAT-02, W2N-04, PARITY-05, N2W-04 (native belt), FOLNAT-04 (delete dead write) | native-build (TestFlight; bakes B's bundle) | `node --test e2e/nearby-sync-contract.test.mjs e2e/permission-flow.test.mjs` + 2-device manual |

If the PR-WS2-B review feels too large, split it at the natural seam: B1 = SYNCE2E-05 + FAILUX-06
(decision-lib correctness, test-heavy, tiny diff) and B2 = the rest. A and B/C are independent;
**B must merge before C is archived** so the TestFlight build bakes the fixed bundle.

---

#### FOLNAT-01 — Native follower has no browse-away model: any local navigation is yanked back within ~1s by the director's mesh heartbeat, with full browse affordances still offered and no "Volver a en vivo" bar or pill ever shown `high` `native` `web-only`

**Problem.** The web bundle's browse machinery (`relay.browsing`, amber pill, "Volver a en vivo"
bar) is relay-gated, and the relay is permanently off in the shell — so on a parish iPad every
affordance still offers browsing (♪ jump modal, drawer, swipes) but the mesh `page` branch of
`applyNativeSyncEvent` renders the director's page unconditionally, and the director's 1s heartbeat
re-injects it because the follower's local browse updated native's `currentPageRef`. Web followers
doing the identical ♪ jump get a sanctioned peek; native followers get a 1-second trap. This is the
workstream centerpiece: it merged PARITY-02, N2W-02 and SYNCE2E-03.

**User impact at Mass.** A congregant who peeks at the next song or a chord mid-Mass is snapped
back within ~1 second, every time, with no explanation and no opt-in browse mode — to an elderly
user the songbook looks haunted/broken, and the fleet is inconsistent (the phone beside them
browses fine).

**Evidence (verify before editing).**
- web/src/app.js:977-980 — `applyNativeSyncEvent` `page` branch: `renderPage(event.page, { pushToHistory: false })` with zero browsing check (contrast the relay path, which consults `relay.browsing` via the decision lib at app.js:3134-3143).
- web/src/app.js:1197-1205 — the ONLY browse-intent capture (numpad jump in `goToDraftSong`) is gated on `relay.hasDirector` (:1200), permanently `false` in the shell.
- web/src/app.js:3329 — `startRelayFollow` early-returns in the shell (`hasNativeBridge() || NATIVE_FILE_MODE`); relay.hasDirector is initialized `false` at app.js:3006 and nothing on native ever sets it.
- web/src/app.js:3068 — `showGoLiveBar` gated on `relay.hasDirector`; app.js:3038 — `renderRelayPill` hides the pill when `!relay.hasDirector`. Bar and pill can never appear on native.
- PdfReaderApp.tsx:393-400 — director mesh heartbeat re-sends its page every 1000ms (interval literal at :400; the "2s" in comments at PdfReaderApp.tsx:372 and :900 is stale — trust the code) (corrected).
- PdfReaderApp.tsx:903 — follower mesh `page` listener de-dupes ONLY against `currentPageRef`; :904 sets the ref optimistically before injecting at :911.
- web/src/app.js:1062-1067 — every committed render posts `page-changed`; PdfReaderApp.tsx:709-711 adopts it into `currentPageRef` for followers (the pre-ready ignore at :704 is director/transmitter-only) — so a local browse makes the director's page look "new" to the de-dupe forever.
- web/src/app.js:3072-3083 — `goLive()` renders `relay.livePage` and clears browsing; app.js:3093-3095 — `reconnectRelay()`'s native branch posts `{type:"resync"}` and returns BEFORE the browse-state reset at :3098-3099 (a ⟳ tap on native would not exit a future browse mode — must be fixed as part of this).
- PdfReaderApp.tsx:71 — `LIVE_DIRECTOR_WINDOW_MS = 8000`, native's own "director is live right now" freshness window; mirror it web-side.
- web/src/index.html:360 — `lib/svSyncDecision.js` loads as a deferred script before app.js in BOTH contexts, so a new pure helper there is available to app.js and to node tests.

**Semantics to mirror (checked against the web relay path — do not invent).** On web, browse mode
is **sticky**: while `relay.browsing` is true, every new director page only updates `livePage`
(decision action `"browsing"`, svSyncDecision.js:121-126) — the director's NEXT page turn does
**not** re-engage follow. Re-engagement is explicit only: the "Volver a en vivo" bar (:3064), the
amber pill tap (:3028-3031), or ⟳ (`reconnectRelay` clears browsing, :3098). Only the **numpad
jump off the live page** grants browse mode (:1200-1205) — drawer taps and swipes do NOT (that gap
is the separate known-OPEN web finding `web-reader-browse-result-click-skips-relay-browsing-mode`;
un-flagged drift keeps getting re-homed, which on mesh means the ≤1s heartbeat yank — that is the
intended F1-parity semantic, keep it). When IANAV-02 step 1 (Wave 4) lands, its `noteIntentionalBrowse` helper closes that drawer/turnSong/turnPage gap — the helper must stay transport-agnostic (built on `hasLiveDirector()`, never bare `relay.*`). Mirror all of this exactly; no product fork there.

**Fix — step by step (all in the web bundle; native needs NO change — when the web ignores an
injected page it never posts `page-changed`, so `currentPageRef` stays on the browsed page and the
1s heartbeat keeps offering the live page until go-live).**

1. **Add a mesh live-page tracker + transport-agnostic liveness helper** in web/src/app.js next to
   the `relay` object (anchor: the `relay` state block ending at :3009):
   ```js
   // Mesh (native shell) live-director tracker — the mesh sibling of relay.livePage.
   // Only ever advances inside the shell (mesh events arrive via the bridge), inert on web.
   const mesh = { livePage: null, lastPageAt: 0 };
   const MESH_LIVE_WINDOW_MS = 8000; // mirrors native LIVE_DIRECTOR_WINDOW_MS (PdfReaderApp.tsx:71)
   const meshDirectorFresh = () =>
     mesh.livePage != null && Date.now() - mesh.lastPageAt < MESH_LIVE_WINDOW_MS;
   const hasLiveDirector = () => relay.hasDirector || meshDirectorFresh();
   const liveDirectorPage = () => (relay.hasDirector ? relay.livePage : mesh.livePage);
   ```
2. **Extract the mesh-apply decision as a pure function** in web/src/lib/svSyncDecision.js (same
   UMD object, add to the `api` at :178) so it is node-testable and shared with N2W-04:
   ```js
   /** What should the shell follower do with a native-injected page event?
    *  ctx: { browsing, failedPage, failedCount } → { track:boolean, render:boolean } */
   function decideMeshPage(page, ctx) {
     var out = { track: false, render: false };
     try {
       if (typeof page !== "number" || !isFinite(page) || !ctx) return out;
       out.track = true;                       // livePage always tracks an applied-or-ignored push
       if (ctx.browsing) return out;           // sanctioned peek: track silently, never yank
       // N2W-04: a page that has deterministically failed N times must stop re-rendering
       // (and stop re-posting render-failed) until the director moves to a different page.
       if (ctx.failedPage === page && ctx.failedCount >= 3) return out;
       out.render = true;
       return out;
     } catch (_) { return out; }
   }
   ```
   Do NOT add a same-page skip here: `renderPage` already short-circuits a healthy same-page render
   (app.js:1038) and re-fetches an unhealthy one — adding a skip would import FAILUX-06's dead-end
   onto native.
3. **Rewrite the `page` branch of `applyNativeSyncEvent`** (app.js:977-980) as a thin executor:
   ```js
   if (event.type === "page" && Number.isFinite(event.page)) {
     const lib = globalThis.svSyncDecision;
     const d = lib && typeof lib.decideMeshPage === "function"
       ? lib.decideMeshPage(event.page, {
           browsing: relay.browsing,
           failedPage: renderFailTracker.page,   // from N2W-04 (step 1 there); 0/0 until it lands
           failedCount: renderFailTracker.count,
         })
       : { track: true, render: !relay.browsing };   // conservative inline fallback
     if (d.track) { mesh.livePage = clampPage(event.page); mesh.lastPageAt = Date.now(); }
     if (!d.render) {
       if (relay.browsing) { showGoLiveBar(); renderRelayPill(); }  // keep return affordances fresh
       return;
     }
     renderPage(event.page, { pushToHistory: false, fromNativeSync: true });
   }
   ```
   (`fromNativeSync` and `renderFailTracker` are N2W-04 steps 1-2 — land it in the same PR,
   recommended; otherwise pass `{page:0,count:0}` literals and omit `fromNativeSync` until it does.)
4. **Make the browse-intent capture transport-agnostic** in `goToDraftSong` (app.js:1200-1205):
   ```js
   const liveNow = hasLiveDirector();
   const livePage = liveDirectorPage();
   if (liveNow && livePage != null && targetPage !== livePage) {
     relay.browsing = true;      // one browse flag for both transports (deliberate)
     relay.following = false;
     showGoLiveBar();
     renderRelayPill();
   }
   ```
   Race note (why this is safe without locks): `goToDraftSong`'s synchronous body sets the flag
   before any queued native inject can run (injects arrive as separate `evaluateJavaScript` tasks),
   and the user's `renderPage(targetPage)` bumped `state.pageLoadRequest`, so an older in-flight
   native render loses the requestId guard (app.js:1031/:1049).
5. **Un-gate the affordances** from `relay.hasDirector` → `hasLiveDirector()`:
   - app.js:3068: `const showGoLiveBar = () => { if (hasLiveDirector()) ensureGoLiveBar().classList.add("is-visible"); };`
   - app.js:3038 in `renderRelayPill`: `if (!hasLiveDirector()) { pill.style.display = "none"; return; }`
     (see DECISION-REQUIRED below for whether the pill shows on native at all).
   - app.js:3029 (pill click guard): use `liveDirectorPage() == null` instead of `relay.livePage == null`.
6. **Generalize `goLive()`** (app.js:3072-3083) to render whichever transport is live, and give
   native a freshness kick:
   ```js
   const goLive = () => {
     relay.browsing = false;
     relay.following = true;
     hideGoLiveBar();
     closeSongJump();
     const livePage = liveDirectorPage();
     if (livePage != null) {
       relay.appliedPage = livePage;
       if (state.currentPage !== livePage) renderPage(livePage, { pushToHistory: false });
     }
     // Shell: also ask native for the director's CURRENT snapshot — covers a director who
     // moved during the browse beyond what the 1s heartbeat has re-offered yet.
     if (hasNativeBridge() || NATIVE_FILE_MODE) postNativeBridge({ type: "resync" });
     renderRelayPill();
     haptic(12);
   };
   ```
7. **Fix the ⟳-while-browsing gap** in `reconnectRelay`'s native branch (app.js:3093-3095) — clear
   browse state BEFORE the early return so ⟳ keeps meaning "return to live" (mirrors web :3098-3106):
   ```js
   if (hasNativeBridge() || NATIVE_FILE_MODE) {
     relay.browsing = false;
     relay.following = true;
     hideGoLiveBar();
     renderRelayPill();
     postNativeBridge({ type: "resync" });
     return;
   }
   ```
   Without this, the resync re-inject (PdfReaderApp.tsx:749) would be swallowed by the browsing
   check from step 3 and ⟳ would look dead during a browse.
8. **No native change.** Explicitly do not "optimize" native to suppress re-injection while the web
   browses — the 1s re-offer is the mechanism that keeps `mesh.livePage` fresh for free, and
   ignored injects cost nothing.

**DECISION-REQUIRED (Miguel) — the live pill on parish iPads.** Un-gating `renderRelayPill` via
`hasLiveDirector()` makes the 8px status dot appear on native followers for the first time: green
pulsing while following a live mesh director, amber while browsing (tap = go live). Options:
(a) **full web parity** — pill shows green when live, amber when browsing (this is what the code in
step 5 does as written); (b) **amber-only on native** — extra guard so the pill renders in the shell
only while `relay.browsing` (no idle visual change on iPads; smallest blast radius); (c) no pill on
native, bar only. **Recommendation: (a)** — one mental model across the fleet, it is literally the
existing code path, and M4's planned tri-state pill will replace this dot on both surfaces anyway.
If (b) is chosen: in `renderRelayPill`, after the `hasLiveDirector()` check add
`if (!relay.hasDirector && !relay.browsing) { pill.style.display = "none"; return; }`.

**Acceptance criteria.**
- [ ] Shell follower, live mesh director: ♪ numpad-jump to another song → page stays put for ≥60s; "↩  Volver a en vivo" bar visible; heartbeats keep arriving (worker `/log` shows `mesh:page-recv` continuing).
- [ ] Director turns pages while the follower browses → follower does NOT move; tapping the bar (or the amber pill, per decision) lands on the director's CURRENT page ≤1s (not the page at browse time).
- [ ] ⟳ tap while browsing exits browse mode and re-homes ≤1s.
- [ ] Stray swipe WITHOUT the numpad jump → still re-homed by the next heartbeat (≤ ~1s) — unchanged F1-parity semantic.
- [ ] ♪ jump with NO live director (practice, mesh silent >8s) → plain navigation, no bar, page sticks.
- [ ] WebView crash-reload / app relaunch while browsing → snaps back to the director (bridge-ready resync wins; browse state resets on load) — acceptable and intended.
- [ ] signovivo.com relay behavior byte-identical (all changed sites are additive or shell-gated; `relay.hasDirector` path untouched).
- [ ] Director/transmitter devices: no behavior change (browse capture requires a live director signal which a director's own device does not receive — mesh `page` events are echo-ignored at PdfReaderApp.tsx:889).

**Tests.**
- Extend `e2e/svSyncDecision.test.mjs` with a `decideMeshPage` block: `{browsing:true}` → track,
  no render; `{browsing:false}` → render; `{failedPage:N, failedCount:3}` + page N → track, no
  render; page ≠ failedPage → render; non-finite page / null ctx → `{track:false, render:false}`
  and never throws.
- Add a source-pin to `e2e/nearby-sync-contract.test.mjs` (it already reads repo sources):
  app.js's `applyNativeSyncEvent` page branch matches `/decideMeshPage/`, and
  `reconnectRelay`'s native branch matches `/browsing = false/` before `postNativeBridge`.
- Manual (iPad portrait, 2 devices, no build needed for the web half — drive one WebView in Safari
  inspector): from the Mac, inject
  `window.__signoVivoReceiveNativeEvent({type:"sync-event",event:{type:"page",page:250}})`
  repeatedly at 1s; numpad-jump to another song; assert no yank + bar; tap the bar; assert render
  of the latest injected page. Then the real 2-device pass after PR-WS2-C's build.

**Dependencies.** Land with N2W-04's web half (shared `decideMeshPage` / `renderFailTracker`).
After SYNCE2E-05/FAILUX-06 in the same PR to avoid conflicts in `goLive`. Reaches iPads only via
PR-WS2-C's build — say so in the release notes.

---

#### FOLNAT-02 — Mesh start failure (Local Network denied, radios off) is invisible and indistinguishable from "no director" `high` `native` `native-build`

**Problem.** Swift emits `FOLLOWER_START_FAILED` / `DIRECTOR_START_FAILED` precisely so the user
can be warned (the emit comments say so), but the shell's mesh `error` listener handles only
`DIRECTOR_CONFLICT` and drops everything else; the startup promise can't catch it because Swift's
`startFollower` resolves immediately and the browser-failure callback fires async later. The old
native reader's "Settings deep link on director error" behavior was deleted in the WebView rewrite
with no replacement.

**User impact at Mass.** A congregant who taps "No permitir" on the first-run Local Network prompt
(or launches with radios off) gets a normal-looking songbook that will NEVER sync — the ⟳ spins
1.1s then silence, identical to "no director present" — while the fleet dashboard still shows the
device checked in (HTTP is unaffected), reinforcing false confidence.

**Evidence (verify before editing).**
- ios/SignoVivo/DirectorSyncModule.swift:1686-1704 — `didNotStartBrowsingForPeers`: comment "Fires when Local Network permission is denied"; the `FOLLOWER_START_FAILED` emit is at :1690, guarded `browser === self.browser, currentRole == "follower"` at :1689 (primer browser excluded) (corrected — the audit cited :1686, which is the delegate-method declaration).
- ios/SignoVivo/DirectorSyncModule.swift:1611-1616 — advertiser twin: "Surface the failure to JS immediately so the director UI can warn the user"; `DIRECTOR_START_FAILED` emit at :1615.
- ios/SignoVivo/DirectorSyncModule.swift:1692-1703 — M-F7: browser retries forever (fast backoff ×5, then every 45s) — so the failure event REPEATS ~every 45s; the JS forward must latch.
- PdfReaderApp.tsx:932-944 — the JS mesh `error` case acts only on `DIRECTOR_CONFLICT`; every other code is `dbgLog`ged and dropped.
- ios/SignoVivo/DirectorSyncModule.swift:388-411 — `startFollower` rejects only on an empty session code (:395-397) and otherwise resolves at :409 after merely *starting* the transports — so `becomeFollower`'s retry-once (PdfReaderApp.tsx:436-443) can never observe permission denial (corrected — the audit cited :399, inside the body).
- e2e/permission-flow.test.mjs:82-86 — comment block recording that the old "director-mode error offers Settings deep link" test was removed as dead behavior; nothing replaced the guidance (corrected — spans 82-86).
- ios/SignoVivo/DirectorSyncModule.swift:294-311 — `handleAppDidBecomeActive` one-shot transport relaunch after the user fixes permissions: recovery WORKS, but nothing ever tells the user to open Settings (corrected — function starts :294).
- web/src/app.js:927-990 — `applyNativeSyncEvent`: an unknown `event.type` inside `sync-event` falls through harmlessly (whole-body try at :934) → a new `error` event type is additive-safe against old bundles.
- web/src/app.js:887-925 — `showRelayAuthWarning`, the banner pattern to clone; web/src/app.js:972-975 — the mesh `state` handler, where the auto-clear hook goes.

**Fix — step by step.**
1. **Native: forward the failure, latched.** Add a ref near the other refs (PdfReaderApp.tsx:~126):
   `const meshStartFailedNotifiedRef = useRef(false);`. In the mesh listener's `error` case
   (PdfReaderApp.tsx:932-944), after the existing `DIRECTOR_CONFLICT` handling, add:
   ```ts
   const code = String(event.code ?? "");
   if (code === "FOLLOWER_START_FAILED" || code === "DIRECTOR_START_FAILED") {
     // Latch one forward per role session — Swift's 45s retry-forever re-emits this.
     if (!meshStartFailedNotifiedRef.current) {
       meshStartFailedNotifiedRef.current = true;
       injectEvent({ type: "sync-event", event: { type: "error", code } });
     }
   }
   ```
   (Additive: old bundles ignore the unknown event type — verified channel above.)
2. **Native: re-arm the latch** at the top of `becomeFollower` (after :423) and `becomeDirector`
   (after the generation claim, :456): `meshStartFailedNotifiedRef.current = false;` — a fresh role
   entry is a fresh attempt. Also re-arm in the mesh listener's `state` case when
   `String(event.status) === "connected"` (a recovered transport must be able to warn again later).
3. **Web: banner.** Clone the `showRelayAuthWarning` pattern (app.js:887-925) as
   `showMeshStartWarning(code)` / `hideMeshStartWarning()`: new element id `sv-mesh-warn`, amber
   (`background:#b45309`), dismissible ×, idempotent re-reveal, `role="alert"`. Copy, keyed on the
   code (plain es-MX, no jargon):
   - `FOLLOWER_START_FAILED`: **"Este iPad no puede conectarse con el director. Activa Wi-Fi y Bluetooth, y permite Red local en Ajustes → Privacidad → Red local → Signo Vivo."**
   - `DIRECTOR_START_FAILED`: **"No se pudo activar la señal para los otros iPads. Activa Wi-Fi y Bluetooth, y permite Red local en Ajustes → Privacidad → Red local → Signo Vivo."**
     > Note: DIRNAT-03 (Wave 2) already shows a native Alert for `DIRECTOR_START_FAILED` on a director — forward only `FOLLOWER_START_FAILED` to the web banner unless the double surface is deliberate; coordinate both notified-latches in the same switch case.
   Close-button aria-label: **"Cerrar aviso"** (same as the existing banner).
4. **Web: wire it** in `applyNativeSyncEvent` — add before the `state` handler (:972):
   ```js
   if (event.type === "error") {
     const code = String(event.code || "");
     if (code === "FOLLOWER_START_FAILED" || code === "DIRECTOR_START_FAILED") showMeshStartWarning(code);
     return;
   }
   ```
   and inside the existing `state` handler add the auto-clear:
   `if (String(event.status || "") === "connected") hideMeshStartWarning();`
5. **Optional (recommend deferring, consistent with #270's fewer-modals rationale):** a native
   `Alert.alert` with a "Abrir Ajustes" button calling `Linking.openSettings()` (add `Linking` to
   the react-native import at PdfReaderApp.tsx:19). The banner is the v1 floor; M7's DIAGNÓSTICO
   screen is the permanent home.

**Acceptance criteria.**
- [ ] Fresh install, deny Local Network → amber banner appears within ~10s naming the Settings path; repeated ⟳ taps and Swift's 45s retries do NOT stack or re-flash it.
- [ ] Grant the permission in Ajustes, foreground the app → transports relaunch (existing Swift :294-311), `connected` arrives, banner clears with no user action.
- [ ] Permission granted, no director present → NO banner (the two states are now distinguishable).
- [ ] Director device with radios off after confirming a code → the DIRECTOR variant appears.
- [ ] Old web bundle paired with this shell (mesh-pushed scenario): no banner, no error, no crash (event silently ignored).

**Tests.**
- `e2e/nearby-sync-contract.test.mjs` source pins: PdfReaderApp.tsx's error case matches
  `/FOLLOWER_START_FAILED/` and `/DIRECTOR_START_FAILED/` and injects a `sync-event` of type
  `error`; a latch ref exists (`/meshStartFailedNotifiedRef/`); app.js handles
  `event.type === "error"` → `/showMeshStartWarning/` and clears on `"connected"`.
- Manual (iPad portrait, the only real proof): Ajustes → Privacidad → Red local → toggle Signo
  Vivo OFF → relaunch app → expect banner; toggle ON → foreground → banner clears. Add this step
  to the M7 device-day script (it currently has no denied-permission step).

**Dependencies.** None in this workstream (independent of FOLNAT-01). Ships in PR-WS2-C. Its
banner surface should later fold into M4's tri-state pill work — note it in that spec.

---

#### SYNCE2E-01 — Fast-clock director: A2 seq sanitizer collapses every publish to seq=0, freezing web followers to one page turn per ~90s `high` `worker` `worker-only`

**Problem.** The client derives `seq` from the device wall clock (`Math.max(seqCounter+1,
Date.now())`), and the worker's A2 sanitizer collapses any `seq > serverNow+60s` to 0. For a
director whose clock is >60s fast, the FIRST publish is accepted (stale room bypasses the gate,
server assigns `snapshot.seq+1` and refreshes `ts`), but every subsequent publish is zeroed and hits
the fresh-room seq-0 gate → `{ok:true, ignored:true}` — and since `ts` only refreshes on ACCEPTED
publishes, the room stays "fresh" on that first page for ~90s. Result: one accepted page per
~90-102s, HTTP 200 throughout, so the client (which checks only `res.ok`) sees pure success.

**User impact at Mass.** A director with a manually-set or NTP-broken clock silently strands the
whole signovivo.com congregation on ~90s page cadence while the mesh iPads follow normally — no
error on any surface for the entire Mass.

**Evidence (verify before editing).**
- sync-worker/src/index.ts:146-149 — sanitizer: `!Number.isFinite || <0 || > Date.now()+60000` → `incomingSeq = 0` (the fast-clock case is lumped with the poison cases).
- sync-worker/src/index.ts:156-158 — staleness: `seq === 0 || nowSec - ts > RELAY_LIVE_MAX_AGE_S` (=90, :34).
- sync-worker/src/index.ts:165-167 — A2 seq-0 gate: fresh room + seq 0 → `{ok:true, ignored:true}`.
- sync-worker/src/index.ts:168-170 — monotonic guard (stale low seqs still ignored while fresh — must stay intact).
- sync-worker/src/index.ts:177-178 — accept path assigns `incomingSeq > 0 ? incomingSeq : snapshot.seq + 1` and server `ts` — the once-per-90s escape valve.
- src/directorRelaySync.js:56-59 — `nextSeq()` uses the DEVICE wall clock (:58).
- src/directorRelaySync.js:87-89 — client checks only `res.ok`; `ignored` is never read → 200 looks like success.
- sync-worker/test/a2.test.mjs:11-20 — the test file is LOCAL-ONLY by construction (throws without `RELAY_TEST_BASE`/`RELAY_TEST_CODE`; refuses `signovivo|workers.dev` bases); `run-a2.sh` boots local `wrangler dev` — safe to extend and run.

**Fix — step by step (worker only; zero wire change, zero client change).**
1. In `publish()` (sync-worker/src/index.ts:146-149), split the fast-clock case out of the poison
   collapse — clamp instead of zeroing:
   ```ts
   let incomingSeq = Number(input.seq ?? 0);
   if (!Number.isFinite(incomingSeq) || incomingSeq < 0) {
     incomingSeq = 0; // poison guard (NaN/Infinity-as-null/negative) — unchanged
   } else if (incomingSeq > Date.now() + 60000) {
     // Fast-clock publisher (device wall-clock >60s ahead): clamp to a sane monotonic value
     // instead of collapsing to 0 — zeroing turned EVERY subsequent publish into an ignored
     // "override attempt" while its own accepted predecessor kept the room fresh (a rolling
     // one-page-per-~90s freeze). Clamping keeps A2's intents: poison still zeroes, and the
     // stored seq can never run ahead of server time, so an honest later director is never
     // blocked longer than today.
     incomingSeq = Math.max(this.snapshot.seq + 1, Date.now());
   }
   ```
2. Leave :156-170 and :177-178 untouched. Walk the invariants once in the PR description:
   clamped seq > `snapshot.seq` always (max with `seq+1`) → passes the monotonic guard → accepted →
   `ts` refreshes → freshness works normally and page turns flow 1:1; two publishes in the same ms
   still increase by ≥1; the stored seq never exceeds serverNow+ε, so a later honest wall-clock
   director's seq wins immediately; Infinity/NaN arrive as JSON `null` → `?? 0` → 0 (poison path
   unchanged); the slow-clock handoff variant (seq < snapshot.seq, bounded ~90s) is deliberately
   NOT addressed here — its durable fix is M4's transmitterId.
3. Do not touch the client's non-consumption of `ignored` (known-OPEN, planned M4 P2-IDENTITY).

**Acceptance criteria.**
- [ ] Publisher whose seqs are `Date.now()+300s`: first publish accepted; SECOND publish (new page, higher device seq) also accepted — `/state.page` reflects it within one round-trip (today it freezes ~90s).
- [ ] Stored `/state.seq` stays ≤ server now + 60s at all times during the skewed sequence.
- [ ] `seq: -5` (and `seq: null`, the JSON form of Infinity/NaN) while a fresh director is live → still `{ignored:true}`, page unmoved (A2 poison guard regression).
- [ ] All existing a2.test.mjs cases stay green (baseline, seq-0 gate live+stale, flood).

**Tests.** Extend `sync-worker/test/a2.test.mjs` (run via `bash sync-worker/test/run-a2.sh` —
local wrangler only; create a local gitignored `sync-worker/.dev.vars` with a dummy
`TRANSMITTER_CODES=1234567890` if missing):
```js
test("A2-CLAMP: fast-clock director (seq >> now+60s) still turns pages 1:1", async () => {
  const room = "a2-fastclock";
  const skew = 300_000;
  await pub(room, { v: 1, page: 10, totalPages: 371, seq: Date.now() + skew });
  const r2 = await pub(room, { v: 1, page: 11, totalPages: 371, seq: Date.now() + skew + 1000 });
  assert.equal((await r2.json()).ignored, undefined, "second skewed publish must be ACCEPTED");
  const s = await state(room);
  assert.equal(s.page, 11, "page must advance immediately, not after ~90s");
  assert.ok(s.seq <= Date.now() + 60_000, "stored seq stays clamped near server time");
});
test("A2-CLAMP: poison/negative seq still ignored while a director is live", async () => {
  const room = "a2-poison";
  await pub(room, { v: 1, page: 10, totalPages: 371, seq: 7000 });
  for (const bad of [-5, null]) {           // null = what Infinity/NaN become over JSON
    const r = await pub(room, { v: 1, page: 99, totalPages: 371, seq: bad });
    assert.equal((await r.json()).ignored, true, `seq=${bad} must stay ignored while live`);
  }
  assert.equal((await state(room)).page, 10);
});
```
(While editing the file, fix its stale header comment: it says `RELAY_TEST_TOKEN` but the code
reads `RELAY_TEST_CODE`.) No manual device step needed; this is fully provable locally.

**Dependencies.** None. Ships in PR-WS2-A; deploy the worker before or independently of B/C.

---

#### FOLNAT-04 — Native follower app restart always boots to page 2: the persisted last page is write-only end to end `medium` `cross` `web-only`

**Problem.** Native dutifully persists `sv.book.lastPage.standard` to AsyncStorage on every
`page-changed` but nothing anywhere reads it, and the web always boots `state.currentPage =
DEFAULT_START_PAGE` (=2) with no persisted-page key of its own. `lastDirectorSnapshotRef` is
in-memory only, so a restarted follower has nothing to re-assert at bridge-ready either. The known
prior-art finding records the dead write as LOW code debt; this is its live-Mass symptom.

**User impact at Mass.** A follower iPad that crashes/relaunches mid-Mass comes back on page 2 (the
cover region), not where the congregation is, until the mesh reconnects and a heartbeat lands
(seconds to tens of seconds — forever if no director is live, e.g. at practice).

**Evidence (verify before editing).**
- web/src/app.js:215 — `DEFAULT_START_PAGE = 2`; app.js:3411 — `initReader` hard-sets `state.currentPage = DEFAULT_START_PAGE`; app.js:3446-3449 — in the shell the relay peek is skipped and `relay.hasDirector` is always false, so :3449 always renders page 2.
- web/src/app.js — no persisted-page localStorage key exists anywhere (grep: keys are `nc-sort-prefs` :136, haptic :386, tip :397, recientes :408, offline-ready :606, fleet device :2869).
- PdfReaderApp.tsx:717-720 — the AsyncStorage persist (template key at :718, prefix `sv.book.lastPage.` at src/offlineBooks.ts:23); repo-wide there is NO `getItem` of that prefix — write-only confirmed at HEAD.
- PdfReaderApp.tsx:116 — `lastDirectorSnapshotRef` is a `useRef` (lost on app restart); the bridge-ready follower resync (:679-695) explicitly null-guards it, so a fresh boot re-asserts nothing.
- web/src/app.js:1056-1067 — `renderPage`'s commit point (where the persist hook belongs).

**Fix — step by step (web-side restore, gated to the shell; then delete the dead native write).**
1. **Constant** (near :227): `const PAGE_RESTORE_KEY = "sv.page.last";`
2. **Persist on every committed render** — in `renderPage`, immediately after the `page-changed`
   post (:1067):
   ```js
   if (NATIVE_FILE_MODE) {
     try { localStorage.setItem(PAGE_RESTORE_KEY, String(nextPage)); } catch (_) {}
   }
   ```
   (Guarded per the #238 storage discipline. This tracks mesh-driven renders too — exactly what we
   want: the restore target is "where the congregation was", not "where the user last tapped".)
3. **Restore at boot** — in `initReader`, replace :3411 with:
   ```js
   state.currentPage = DEFAULT_START_PAGE;
   if (NATIVE_FILE_MODE) {
     // A relaunched parish iPad re-opens where it (or the director) last was, instead of the
     // cover region. A live director still wins via bridge-ready/heartbeat seconds later.
     try {
       const stored = Number(localStorage.getItem(PAGE_RESTORE_KEY));
       if (Number.isInteger(stored) && stored >= 1 && stored <= state.totalPages) {
         state.currentPage = stored;
       }
     } catch (_) {}
   }
   ```
4. **Render the restored page** — change :3449 from `renderPage(DEFAULT_START_PAGE, …)` to
   `renderPage(state.currentPage, { pushToHistory: false })`. (On web `state.currentPage` is still
   `DEFAULT_START_PAGE` here unless FOLWEB-12's block set a director page — behavior unchanged.
   Coordinate with FOLWEB-12, which edits the adjacent lines.)
5. **Environment assumption to verify on device (not provable in code):** WKWebView localStorage
   persists across app relaunches for `file://` origins with the default persistent data store —
   the manual step below proves it on the real fleet before relying on it.
6. **PR-WS2-C: delete the dead native write** — remove PdfReaderApp.tsx:716-720 (the
   `Persist per-book last page` comment + `AsyncStorage.setItem(lastPagePrefix…)`) and, once no
   reader remains, the `lastPagePrefix` entry in src/offlineBooks.ts:23. This finishes the known
   `new-director-dead-writes-laststate-role-and-page` cleanup. (Old shells keep writing their
   AsyncStorage key harmlessly; the two stores never interact.)

**Acceptance criteria.**
- [ ] Shell follower on page 250 → force-quit → relaunch with NO director → boots on 250 (not 2).
- [ ] Relaunch WITH a live director on 260 → shows 250 momentarily, snaps to 260 ≤ ~2s (mesh heartbeat wins).
- [ ] Fresh install → page 2 exactly as today.
- [ ] Stored page out of range after a book update (e.g. 999) → falls back to page 2.
- [ ] signovivo.com boot behavior byte-identical (key only written/read under `NATIVE_FILE_MODE`).

**Tests.**
- The boot-page pick is 4 lines of guarded logic; if you want it pinned, extract
  `pickBootPage(storedRaw, totalPages, fallback)` into `web/src/lib/svSyncDecision.js` (it is the
  workstream's grab-bag pure lib) and unit-test {null, "250", "999", "abc", "-1"} in
  `e2e/svSyncDecision.test.mjs`; otherwise a source-pin in `e2e/nearby-sync-contract.test.mjs`
  (`/sv\.page\.last/` appears in both a setItem and a getItem) prevents the write-only regression
  from recurring.
- Manual (iPad portrait, after PR-WS2-C's build): the two relaunch scenarios above, plus airplane
  mode relaunch at practice (no director) → page sticks.

**Dependencies.** Step 6 rides PR-WS2-C; steps 1-5 ride PR-WS2-B. Coordinate line-level with
FOLWEB-12 (same boot block). No functional dependency on other findings.

---

#### SYNCE2E-05 — Forced /state polls bypass the seq guard entirely and can REWIND a follower onto an older snapshot for up to ~12s under a green pill `medium` `web` `web-only`

**Problem.** `decideRelaySnapshot`'s de-dup guard is `if (!ctx.force && snap.seq <= ctx.lastSeq)` —
so a FORCED apply bypasses even strictly-older seqs. Every `/state` fetch in the app is forced
(WS-open resync, 4s fallback ticks, poll-through-connect, F1 re-home, foreground), a fetch can
take up to its 6s abort window, and a WS push can land mid-flight — so a stale poll body rewinds
the page. Worse, the stale apply overwrites `relay.livePage` with the OLD page, so the F1 drift
re-home sees no drift and cannot correct it; recovery waits for the director's next move or the
12s relay heartbeat's fresh seq.

**User impact at Mass.** On a slow network — exactly when forced polls are most frequent — a
follower can visibly flip BACK one page right after the director advances and sit there up to ~12s
with the "en vivo" pill green.

**Evidence (verify before editing).**
- web/src/lib/svSyncDecision.js:111 — `if (!ctx.force && snap.seq <= ctx.lastSeq)` — force bypasses strictly-older seqs; :116-117 then adopt `lastSeq`/`livePage`, and :130 renders the stale page.
- web/src/app.js:3260 — WS-open `relayPollOnce(true)` races in-flight WS pushes; also :3213 (4s forced fallback interval), :3235 (poll-through-connect), :3291-3292 (F1 re-home poll), :3332 (foreground) — every /state path is forced.
- web/src/app.js:3179 — the /state fetch has a 6s abort window (the race is real on slow links).
- web/src/app.js:3152 — the executor overwrites `relay.livePage` with the decided (stale) page, blinding the F1 re-home at :3291.
- web/src/app.js:3124 — the inline no-lib fallback has the same `!force &&` hole.
- web/src/lib/svSyncDecision.js:97 — F4's demote reset (`lastSeq = -1`) — must stay reachable and unaffected.

**Fix — step by step.** Force must re-apply EQUAL seqs (its purpose: re-home onto a stationary
director) but never STRICTLY OLDER ones.
1. svSyncDecision.js:111 — replace the guard:
   ```js
   // De-dup by seq. A FORCED resync re-applies an EQUAL seq (re-home onto a stationary
   // director) but never a STRICTLY OLDER one — a slow forced /state body that lost a race
   // to a newer WS push must not rewind the page (nor clobber livePage, which would blind
   // the F1 drift re-home).
   if (snap.seq <= ctx.lastSeq && (!ctx.force || snap.seq < ctx.lastSeq)) {
     out.action = "live-dup";
     out.renderPill = true;
     return out;
   }
   ```
   Resulting truth table: `!force && seq<=last` → live-dup (unchanged); `force && seq<last` →
   live-dup (NEW — no rewind, livePage untouched); `force && seq==last` → follow (re-home
   preserved); `seq>last` → follow.
2. Mirror in the inline fallback, app.js:3124:
   ```js
   if (snap.seq <= relay.lastSeq && (!force || snap.seq < relay.lastSeq)) { relay.hasDirector = true; renderRelayPill(); return; }
   ```
3. Nothing else changes: F4's demote path (svSyncDecision.js:88-101) runs before this guard, so a
   demoted room's `lastSeq=-1` reset still lets a restarted director's low seq follow.

**Acceptance criteria.**
- [ ] `force:true, snap.seq=5, ctx.lastSeq=9` → action `live-dup`, `renderPage` undefined, `livePage` NOT set.
- [ ] `force:true, seq=9, lastSeq=9` → `follow` with `renderPage` set when `currentPage` differs (re-home preserved — this is what ⟳/F1 rely on).
- [ ] `force:true, seq=10, lastSeq=9` → `follow`.
- [ ] All 21 existing svSyncDecision tests stay green.
- [ ] Browser sanity (DevTools, slow-3G throttle): with a live director turning pages, no backward page flips after WS reconnects.

**Tests.** Add the three cases above to `e2e/svSyncDecision.test.mjs` (pure, deterministic — use
the file's existing `ctx()` helper). Also assert on the `force && seq<last` case that
`d.livePage === undefined` (the F1-blinding half of the bug). Run
`node --test e2e/svSyncDecision.test.mjs` only.

**Dependencies.** None, but land in the same PR as (or before) FAILUX-06 — its repaint hook fires
from the follow branch this fix makes reachable under force+equal-seq.

---

#### FAILUX-06 — After a failed/timed-out page image, nothing can repaint the current page: ⟳, "Volver a en vivo", and force-resyncs all skip same-page renders `medium` `web` `web-only`

**Problem.** A hung fetch makes `preloadImage` resolve `"timeout"` as success, committing a
blank/stale `src` and updating `currentPage`; the `<img>`'s own error retry budget (4) is consumed
during the outage and never re-arms. When the network returns, every recovery path refuses to
re-render the CURRENT page: the decision lib sets `renderPage = undefined` when
`currentPage === snap.page` even under force, and `goLive`/the inline fallback have the same
same-page skip. The page repairs only when the director next MOVES. (Delta on the known-OPEN
P7-TIMEOUT-COMMIT / P7-IMG-RETRY pair — this is the recovery dead-end.)

**User impact at Mass.** A weak-wifi blip while turning to a hymn leaves a blank/stale image under
a green pill; tapping ⟳ or the amber dot fixes the socket but never the pixels — potentially for a
whole hymn. The workaround (swipe away and back) is real but undiscoverable for this congregation.

**Evidence (verify before editing).**
- web/src/app.js:1011 — the 3s preload timeout RESOLVES (`finish(resolve, "timeout")`) → renderPage commits the unloaded src, sets `state.currentPage`, posts `page-changed` (:1056-1067).
- web/src/app.js:70-77 — `<img>` error retry capped at 4 (`if (pageImgRetries++ >= 4) return;` at :73), src ends carrying `?retry=4`; the budget never re-arms.
- web/src/lib/svSyncDecision.js:130 — `renderPage = ctx.currentPage !== snap.page ? snap.page : undefined` EVEN under force.
- web/src/app.js:3079 — `goLive()` same-page skip; :3155-3162 — the executor renders only when `d.renderPage != null`; :3129 — inline fallback same-page skip.
- web/src/app.js:1038 — `renderPage`'s own health short-circuit (`pageImageMatches && complete && naturalWidth > 0`) — a broken image FAILS this check, so a re-invoked `renderPage` on the same page genuinely re-fetches (this is why the fix works); app.js:268-271 — `pageImageMatches` compares with `endsWith(pageFileName)`, so a `?retry=4` src also fails the match → reload.
- Native is NOT affected: the mesh path calls `renderPage` unconditionally (app.js:977-980), which self-heals via :1038 on the next inject/⟳ — do not add a same-page skip to the mesh branch (see FOLNAT-01 step 2).

**Fix — step by step.**
1. **Health predicate** (near `pageImageMatches`, app.js:~271):
   ```js
   const isPageImageHealthy = () => Boolean(pageImage && pageImage.complete && pageImage.naturalWidth > 0);
   ```
2. **Executor repaint hook** — in `applyRelaySnapshot`'s follow branch (:3155-3163):
   ```js
   if (d.action === "follow") {
     relay.following = true;
     relay.appliedPage = snap.page;
     if (d.renderPage != null) renderPage(d.renderPage, { pushToHistory: false });
     // Same page but the visible image never actually loaded (timeout-committed blank /
     // exhausted retries): a FORCED resync is the user's/system's explicit "repair" signal —
     // re-render through the requestId guard. No-op when the image is healthy.
     else if (force && !isPageImageHealthy()) renderPage(snap.page, { pushToHistory: false });
   }
   ```
3. **goLive repaint** (:3077-3080): change the condition to
   `if (state.currentPage !== livePage || !isPageImageHealthy()) renderPage(livePage, { pushToHistory: false });`
   (written against FOLNAT-01's generalized `goLive`; if landing standalone, the same edit applies
   to `relay.livePage`).
4. **Inline fallback** (:3129): same treatment —
   `if (state.currentPage !== snap.page || (force && !isPageImageHealthy())) renderPage(snap.page, { pushToHistory: false });`
5. **Re-arm the `<img>` retry budget on connectivity return** — inside the `if (pageImage)` block
   (:70-77): `window.addEventListener("online", () => { pageImgRetries = 0; });`
6. Note for reviewers: fast page-turns cannot flash older pages — every repaint routes through
   `renderPage`'s requestId guard (:1031/:1049), and SYNCE2E-05 guarantees the follow branch is
   reachable under force+equal-seq against a stationary director (the ⟳ case).

**Acceptance criteria.**
- [ ] DevTools: load page N with requests blackholed (offline after HTML load) → blank commit; restore network; tap ⟳ → page repaints without the director moving; `naturalWidth > 0`.
- [ ] Same repro, tap the amber dot / "Volver a en vivo" instead → repaints.
- [ ] Healthy image + forced poll → NO redundant re-render (no visible flicker; `renderPage` not re-invoked — verify via a temporary console count).
- [ ] Rapid director page turns during recovery → no stale-page flash (requestId guard).
- [ ] `window` `online` event resets the `<img>` retry budget (simulate: exhaust retries offline, go online, force one more error → retry fires).

**Tests.** The predicate and hooks are DOM-coupled; pin the decision-side precondition in
`e2e/svSyncDecision.test.mjs` (force+equal-seq → follow, from SYNCE2E-05) and add a source-pin in
`e2e/nearby-sync-contract.test.mjs` that app.js contains `isPageImageHealthy` wired into both
`goLive` and the follow branch. Manual browser steps above are the real proof; do them at
375×812 and iPad-portrait 834×1194 viewports.

**Dependencies.** SYNCE2E-05 (same functions; makes the force+equal-seq path reachable). Same PR.

---

#### N2W-04 — render-failed sentinel + 1s mesh heartbeat form an unthrottled ~1s failure loop that slams the drawer shut and blocks all navigation `medium` `cross` `multi`

**Problem.** `renderPage`'s catch fires `setLoading` + `closeDrawer()` + posts `render-failed` on
EVERY failed attempt with no per-page de-dupe or backoff; native answers `render-failed` by setting
`currentPageRef = -1` (sentinel) so the 1s heartbeat re-drives — correct for transient failures,
divergent forever for a deterministic one (missing/corrupt `file://` asset, e.g. the known corrupt
peer-pushed WebBundle class). Escape is impossible: navigating to a working page X sets the ref to
X, so the next heartbeat re-injects the failing page and slams the drawer shut again, every second.

**User impact at Mass.** One bad page asset turns a follower iPad into a strobing error screen that
actively fights the user's attempts to browse away, for as long as the director stays on that page.

**Evidence (verify before editing).**
- web/src/app.js:1081-1098 — renderPage catch: `setLoading(true, "No se pudo cargar esta página.")` (:1085) + `closeDrawer()` (:1086) + posts `render-failed` (:1093-1097) — every attempt, no de-dupe/backoff.
- PdfReaderApp.tsx:792-793 — `render-failed` sets `currentPageRef.current = -1` (gate `roleRef === "follower"` at :792, assignment at :793) (corrected — the audit's :792 spans both).
- PdfReaderApp.tsx:393-400 — 1000ms mesh heartbeat (in-code "2s" comments at :372/:780 are stale); the :903 de-dupe fails vs -1 → re-inject → re-fail → ~1s loop.
- PdfReaderApp.tsx:709-711 — escape blocked: user nav to X sets ref=X (:711), next heartbeat P≠X re-injects the failing page (corrected — clamp :709, ref-set :711).
- web/src/app.js:1041-1046 — the cache-buster retry also fails deterministically (an `error` event rejects; only the timeout path resolves) — the failure really is a throw, not a timeout-commit.

**Fix — step by step.**
1. **Web: per-page failure tracker** (module scope near renderPage):
   ```js
   // Deterministic-failure tamer: track consecutive render failures per page. After 3 failures
   // of the SAME page we stop re-posting render-failed (so native's -1 sentinel stops re-driving
   // a failure that will never succeed) and decideMeshPage stops re-rendering it (FOLNAT-01).
   const renderFailTracker = { page: 0, count: 0 };
   ```
2. **Web: renderPage catch** (:1081-1098) — replace the body's side effects:
   ```js
   } catch (error) {
     if (requestId !== state.pageLoadRequest) return;
     clearLoadingTimer();
     console.error("No se pudo cargar la página solicitada", nextPage, error);
     setLoading(true, "No se pudo cargar esta página.");
     if (renderFailTracker.page === nextPage) renderFailTracker.count += 1;
     else { renderFailTracker.page = nextPage; renderFailTracker.count = 1; }
     // A native-pushed render must not slam the drawer shut every heartbeat while the user
     // tries to browse away from a broken page; user-initiated renders still close it so the
     // error is visible behind the drawer.
     if (!fromNativeSync) closeDrawer();
     if (renderFailTracker.count <= 3) {
       postNativeBridge({ type: "render-failed", page: nextPage, book: state.currentBook });
     }
   }
   ```
   where `fromNativeSync` is a new renderPage option (`{ pushToHistory = true, direction = 0,
   fromNativeSync = false }`) passed as `true` ONLY by `applyNativeSyncEvent`'s page branch
   (FOLNAT-01 step 3's `renderPage(event.page, { pushToHistory: false, fromNativeSync: true })`).
   Do NOT infer from `pushToHistory` — goLive/history-back also pass false.
3. **Web: reset the tracker** (a) in renderPage's success path after the commit (:1056-1058):
   `if (renderFailTracker.page === nextPage) { renderFailTracker.page = 0; renderFailTracker.count = 0; }`
   — only the SAME page recovering clears it (success of a different page must not re-arm the
   loop); and (b) in `reconnectRelay`'s native branch (FOLNAT-01 step 7) so ⟳ = "try that page
   again": `renderFailTracker.page = 0; renderFailTracker.count = 0;`.
4. **Web: stop re-attempting at the dispatcher** — already handled by `decideMeshPage`'s
   `failedPage/failedCount >= 3` clause (FOLNAT-01 step 2): attempt 4+ of the same pushed page is
   skipped entirely, so the persistent error overlay stops flashing over a working page the user
   navigated to, and the follower quiesces until the director MOVES (new page ≠ failedPage →
   renders → recovery) or the user taps ⟳.
   Loop-termination walk (include in the PR description): heartbeat P → inject → fail#1..3 (posts
   render-failed → native -1 → re-drive) → fail#4 skipped at dispatcher → no render, no
   page-changed, no render-failed → native ref stays P (set optimistically at PdfReaderApp.tsx:904)
   → :903 de-dupe breaks every subsequent heartbeat → silence. Drawer usable throughout.
5. **Native belt (PR-WS2-C; protects old-bundle/new-shell pairings)** — cap sentinel re-drives:
   add `const renderFailRef = useRef({ page: 0, count: 0 });` and in the `render-failed` case
   (:777-795):
   ```ts
   const failedPage = Math.max(1, Number(msg.page) || 0);
   const t = renderFailRef.current;
   if (t.page === failedPage) t.count += 1; else { t.page = failedPage; t.count = 1; }
   if (roleRef.current === "follower" && t.count <= 3) currentPageRef.current = -1;
   ```
   and reset in the mesh `page` listener (before the de-dupe at :903):
   `if (Number(event.page) !== renderFailRef.current.page) renderFailRef.current = { page: 0, count: 0 };`
   Wire compat: message shapes unchanged; the web merely posts FEWER `render-failed`s (old shells
   handle each independently — fine), and the belt merely stops honoring the 4th+ (old bundles that
   keep posting are tamed shell-side).
6. **Copy.** Keep the existing overlay string **"No se pudo cargar esta página."** — the loop-stop
   is the fix; if a clearer hint is wanted for the deterministic case (count ≥ 3), use
   **"No se pudo cargar esta página en este dispositivo."** (optional).
7. Root cause (corrupt peer-pushed bundle) is out of scope here — it dies with M7 bundle
   signing / the Q5 mesh-push retire decision; this fix makes the failure survivable either way.

**Acceptance criteria.**
- [ ] With one deliberately-broken page asset (rename `page-250.webp` in a dev bundle) and the director sitting on 250: follower shows the error overlay, posts at most 3 `render-failed`s (watch `/log` or a console tap), then quiesces — no ~1s strobe.
- [ ] While quiesced, the drawer opens and STAYS open; navigation to other pages works with zero interference and no error overlay re-flash.
- [ ] Director moves to a renderable page → follower recovers automatically ≤1s.
- [ ] ⟳ tap re-attempts the failed page (budget reset) — a genuinely transient failure heals.
- [ ] Web-relay follower (signovivo.com) failure behavior unchanged apart from the 3-post cap (drawer still closes on user-initiated failures).

**Tests.**
- `e2e/svSyncDecision.test.mjs`: `decideMeshPage` failure-cap cases (shared with FOLNAT-01).
- `e2e/nearby-sync-contract.test.mjs` source pins: app.js catch contains `fromNativeSync` gating
  `closeDrawer` and a `renderFailTracker.count <= 3` gate on the `render-failed` post;
  PdfReaderApp.tsx `render-failed` case contains the capped sentinel (`/renderFailRef/`).
- Manual iPad-portrait step exactly as in acceptance (needs a dev build with a gutted asset —
  do it on the PR-WS2-C device day).

**Dependencies.** Web half lands WITH FOLNAT-01 (shared `decideMeshPage`/tracker/`fromNativeSync`).
Native belt rides PR-WS2-C. No dependency on the relay findings.

---

#### W2N-04 — becomeFollower failure wedge: role asserted to the web before the transport starts, and ⟳ cannot repair a follower whose mesh start failed `medium` `native` `native-build`

**Problem.** `becomeFollower` sets `roleRef = "follower"` before `startNearbyFollower` runs; if both
start attempts throw, the outer catch swallows and the code still injects `role:"follower"` — the
web shows a healthy follower UI. The ⟳ repair path is gated the wrong way for this state: the
resync handler re-runs `becomeFollower` only when `roleRef === "off"`, and the two mesh fallbacks
no-op in Swift because `currentRole` stayed `"off"` there (the start never completed).

**User impact at Mass.** If the RN-bridge start call fails twice (module wedge / early-boot race —
rare, but the retry-once design exists because it happens), the device is a permanent link-less
"follower" with normal-looking UI; ⟳ spins and does nothing; only the secret soft-reset code or an
app kill recovers it.

**Evidence (verify before editing).**
- PdfReaderApp.tsx:423 — `roleRef.current = "follower"` before any transport start.
- PdfReaderApp.tsx:436-443 — start + retry-once; :445-447 — outer catch swallows the double failure; :449 — `role:"follower"` still injected.
- PdfReaderApp.tsx:733 — resync handler re-runs `becomeFollower` only when `roleRef === "off"`.
- ios/SignoVivo/DirectorSyncModule.swift:592 — `refreshNearbyDiscovery` guards `currentRole != "off"` → no-op in this wedge; requestCurrentSnapshot (:606-617) calls `forceFollowerHelloNow` (:613), which guards `currentRole == "follower"` at :1322 → also a no-op (corrected — the audit's verifier note cited :718, which is `handleBundleOffer`'s unrelated guard; the real anchor is :1322).
- ios/SignoVivo/DirectorSyncModule.swift:388-411 — Swift `startFollower` itself only rejects on an empty session code, so the throw comes from the RN bridge layer; when it happens, Swift `currentRole` remains `"off"`.

**Fix — step by step (native only; no wire change).**
1. Add `const meshStartedRef = useRef(false);` near the other role refs (:~108).
2. In `becomeFollower` (:419-450): set `meshStartedRef.current = false;` at entry (right after
   :423), and set it `true` immediately after EACH `await startNearbyFollower(DIRECTOR_SESSION)`
   resolves (both the first attempt and the retry path, before the generation check that follows) —
   the outer catch leaves it false by construction.
3. Clear it in `performSoftReset` (after `resetNearbyDirectorSync()`, :546-549) and at the top of
   `becomeDirector`'s `wasFollower` teardown (:483-491) — any path that tears the follower
   transport down must drop the flag.
4. Re-gate the resync rescue (:733):
   ```ts
   if (syncAvailable &&
       (roleRef.current === "off" || (roleRef.current === "follower" && !meshStartedRef.current))) {
     becomeFollower();
   }
   ```
   (`becomeFollower` bumps the generation, so a tap-spam cannot stack starts; each ⟳ is a fresh
   bounded attempt.)
5. Optional honesty (cheap, existing wire): in the outer catch (:445-447), inject
   `{ type: "sync-event", event: { type: "state", status: "searching" } }` so the ⟳ fab's 1.1s
   spin grammar at least signals "still looking" instead of implying a live link.

**Acceptance criteria.**
- [ ] Stub `startNearbyFollower` to reject twice (dev build): device shows follower UI; a ⟳ tap afterwards re-runs `becomeFollower`; when the stub is un-broken the next ⟳ establishes the mesh and the device follows normally.
- [ ] Healthy boot: `meshStartedRef` true after start; ⟳ behaves exactly as today (no becomeFollower re-run — verify via dbgLog `become:follower` count).
- [ ] Soft reset (744668486) then ⟳: still rescues via the existing `roleRef === "off"` branch.
- [ ] Director entry after a wedged follower start: unaffected (becomeDirector path doesn't read the flag).

**Tests.**
- `e2e/nearby-sync-contract.test.mjs` source pins: `/meshStartedRef/` is set after
  `startNearbyFollower(` (regex with `[\s\S]*?` across the await), and the resync case's condition
  matches `/roleRef\.current === "follower" && !meshStartedRef\.current/`.
- No RN unit harness exists in this repo (a known gap) — the stub repro in acceptance is the
  functional proof; do it in the PR-WS2-C simulator pass.

**Dependencies.** None. Ships in PR-WS2-C. Complements FOLNAT-02 (that surfaces Swift-level start
failures; this repairs bridge-level ones — different layers of the same "silent dead follower"
class).

---

#### PARITY-05 — role:"none" collapses to FOLLOWER UI with a dead ⟳: a transmitter that exits director mode looks like a synced follower but follows nothing `medium` `cross` `multi`

**Problem.** The transmitter-only exit-director path injects `role:"none"` with a comment claiming
"the web shows no phantom follower UI" — but the web maps `"none"→"off"` and
`renderDirectorModeBadge` sets `data-role="follower"` for anything non-director; no "off"
presentation exists. Base CSS shows the ⟳ fab for all non-director layouts, and on this device the
⟳ is provably dead: the native resync handler skips the becomeFollower rescue and all mesh calls
when `syncAvailable` is false, and `lastDirectorSnapshotRef` is null on a device that WAS the
publisher.

**User impact at Mass.** After Miguel's transmitter phone/iPad exits director mode it renders as a
normal follower (⟳ + ♪) with NO follower transport — the page silently freezes while ⟳ visibly
"works" (1.1s spin). Handed to a congregant, it never follows.

**Evidence (verify before editing).**
- PdfReaderApp.tsx:754-774 — exit-director; the transmitter branch (:763-774) claims "the web shows no phantom follower UI" (:768) then injects `role:"none"` (:773).
- web/src/app.js:947-956 — role handler maps `"none"→"off"` (:951); :845-852 — `renderDirectorModeBadge` sets `data-role = isDirector ? "director" : "follower"` (:849) — no "off" presentation.
- web/src/styles.css:2149-2156 — role-based controls: the only ⟳ rule is `html[data-role="director"] .resync-fab { display: none; }` at :2156, i.e. ⟳ is visible by default for every non-director layout (corrected — the audit cited :2154, which is the adjacent `.search-fab` rule).
- PdfReaderApp.tsx:727-752 — resync handler: `roleRef "off"` + `syncAvailable false` skips the rescue (:733) and mesh calls (:735); web ⟳ spin is unconditional 1.1s theater (app.js:2416-2419).
- PdfReaderApp.tsx:663-669 — bridge-ready re-asserts `role:"none"` for an "off" device after a WebView reload — the fix must cover this path too or a crash-reload resurrects the phantom.
- Scope guard: for MESH devices stranded in "off" (post-soft-reset), ⟳ genuinely rescues via becomeFollower (:733) — the fix must NOT strip their follower presentation.

**Fix — step by step (additive bridge field so the web never guesses).**
1. **Native: tag the no-transport case.** In the exit-director transmitter branch (:773):
   `injectEvent({ type: "role", role: "none", canFollow: false });` and fix the stale comment at
   :768 (the web DOES render follower chrome today; after this fix it renders the "off" state).
   In the bridge-ready role re-assert (:669), carry the same truth across reloads:
   ```ts
   injectEvent({
     type: "role",
     role: assertedRole,
     ...(assertedRole === "none" && !syncAvailable ? { canFollow: false } : {}),
   });
   ```
   `canFollow:false` ⇔ "this shell has NO follower transport" (`!syncAvailable`). A mesh device's
   `role:"none"` (soft-reset, rejected code, empty code) never carries it → old behavior. Old
   bundles ignore the extra field (role handler reads only `payload.role`) — additive-safe.
2. **Web: a real "off" presentation.** State default near :179: `nativeCanFollow: true`. Role
   handler (:947-956): inside the role-string branch add
   `state.nativeCanFollow = payload.canFollow !== false;`. Then `renderDirectorModeBadge`
   (:845-852):
   ```js
   const isDirector = state.nativeSyncRole === "director";
   const isOffNoTransport =
     state.nativeSyncRole === "off" && state.nativeBridgeAvailable && state.nativeCanFollow === false;
   document.documentElement.dataset.role = isDirector ? "director" : isOffNoTransport ? "off" : "follower";
   ```
   (`nativeBridgeAvailable` is set by `bridge-state` at :938; pure web can never enter "off".)
3. **CSS** (styles.css, in the role block at :2149-2156):
   `html[data-role="off"] .resync-fab { display: none; }` — the ⟳ is the only lie; ♪ stays (it is
   the re-entry path for a new code). No other `data-role` selectors exist (verified), so "off"
   otherwise inherits the follower layout. (A "solo lectura" chip was considered and rejected for
   v1 — one fewer new UI element; the hidden ⟳ plus the badge's disappearance is the state change.)
4. **Copy.** No new strings (the state is the absence of a dead control). If the chip is ever
   wanted: **"Solo lectura"**.

**Acceptance criteria.**
- [ ] Transmitter-only device (dev build with the native module stubbed off, or a real tethered phone): enter code → confirm → exit via the DIRECTOR badge → NO ⟳ shown; ♪ still opens; re-entering a valid code re-promotes.
- [ ] Force a WebView reload in that state (backgrounding/memory) → still no ⟳ after bridge-ready.
- [ ] Mesh iPad after soft-reset (roleRef "off", syncAvailable true) → ⟳ still visible and still rescues via becomeFollower.
- [ ] Mesh follower types an empty/rejected code (`role:"none"` without canFollow) → presentation stays "follower" (no flicker to "off").
- [ ] Pure web (signovivo.com): byte-identical layout.

**Tests.**
- `e2e/nearby-sync-contract.test.mjs` source pins: exit-director transmitter branch matches
  `/canFollow:\s*false/`; bridge-ready assert carries the conditional field; app.js maps it
  (`/nativeCanFollow/`) and styles.css contains `html[data-role="off"] .resync-fab`.
- Manual iPad/iPhone-portrait pass per acceptance (transmitter exit is Miguel's own flow — verify
  on his device on the PR-WS2-C day).

**Dependencies.** None hard; ships in PR-WS2-C (native+web halves must pair, and the web half is
inert until a shell sends `canFollow:false`). Coordinate visually with M4's future pill (same
"frozen state" surface).

---

#### SYNCE2E-09 — /publish token bucket (15 burst, 2/s) can 429 a legitimate director scrubbing pages, silently dropping the FINAL resting page for up to ~12s `low` `worker` `worker-only`

**Problem.** Publishes are bucketed per IP at 15 burst / 2 per-second; the serialized client
coalescer sustains ~3-6 publishes/s while a director scrubs for a song, so ~5-8s of scrubbing
drains the bucket. The worker returns HTTP 429, which the client deliberately treats as silent and
does NOT re-queue — so if the rejected publish is the settle page, web followers sit on a mid-scrub
page until the 12s relay heartbeat republishes. (Delta on the FIXED A2 rate-limit work — this is
its legitimate-traffic edge.)

**User impact at Mass.** A director rapidly hunting for a song can strand signovivo.com followers
on a mid-scrub page for up to ~12s while the mesh iPads (no rate limit) already settled — a brief,
confusing web/native divergence.

**Evidence (verify before editing).**
- sync-worker/src/index.ts:138-140 — `rateLimited(ip, 15, 2)` on publish → `{ok:true, …, rateLimited:true}` from the DO; :816 — the HTTP handler maps it to `429 {ok:false, error:"rate_limited"}`.
- src/directorRelaySync.js:82-96 — 429 falls into the deliberate silent path (only 401/403 warn, comment at :82-86), and the rejected payload is NOT re-queued — `pending` is only refilled by a NEW page change (corrected — the audit cited :84 alone; the behavior spans :82-96 plus the coalescer at :124-128).
- PdfReaderApp.tsx:401-412 — the 12s relay heartbeat is the only recovery (`setInterval …, 12000` at :412).
- sync-worker/test/a2.test.mjs:138-149 — the flood test asserts `limited >= 10` and `notLimited >= 10` over 40 rapid publishes — the new bucket must keep both margins.

**Fix — step by step.** Raise the bucket worker-side (kept worker-only so it ships with PR-WS2-A;
the alternative — client-side re-queue of a 429'd payload with a ~1s retry — was considered and
rejected: it adds timer/state machinery to the liturgy-critical coalescer for a LOW-severity
window, and a native-build vector).
1. sync-worker/src/index.ts:138 — change to `if (this.rateLimited(ip, 25, 4)) {` and update the
   comment: a real director's worst case is the serialized coalescer at ~1/RTT (~3-6/s) while
   scrubbing; 25 burst / 4 per-second sustained absorbs a full song-hunt yet still throttles a
   flood (page-hijack needs sustained volume, not 25 frames).
2. Verify the flood-test margins hold with 25/4: 40 near-instant publishes → ~25 allowed, ~15
   limited → both `>= 10` assertions keep ≥5 headroom. Do NOT raise the burst to 30 — that leaves
   the `limited >= 10` assertion with zero margin and makes the test flaky.
3. Leave `/log` and `/fleet` buckets untouched (:200, elsewhere) — this finding is publish-only.

**Acceptance criteria.**
- [ ] Local wrangler: 25 publishes in 5s from one IP (distinct seqs/pages) → final page visible in `/state` within 2s of the last publish, zero 429s.
- [ ] The a2 flood test (40 rapid publishes) still produces ≥10 429s and ≥10 successes.
- [ ] No client change; response shapes unchanged.

**Tests.** Extend `sync-worker/test/a2.test.mjs` with a scrub-shaped case (25 sequential publishes
with increasing seq, small delay, assert last page lands and no 429), run via
`bash sync-worker/test/run-a2.sh`. Keep the existing flood test untouched as the abuse regression.

**Dependencies.** None; rides PR-WS2-A with SYNCE2E-01 (adjacent lines in `publish()`).

---

#### FAILUX-10 — Boot-failure path shows "No se pudo cargar Signo Vivo." with no retry affordance `low` `web` `web-only`

**Problem.** `initReader().catch(...)` renders only a small text line via `setLoading` and then
calls `revealReader()`, which sets `window.__svBooted` — after which the bootGuard's existing
"Reintentar" card (built for exactly this) can never take over; the catch also consumes the
rejection so `unhandledrejection` never fires. The user gets a dead-end label.

**User impact at Mass.** On a handled boot failure the user sees a text line with no button and no
instruction — recovery requires knowing to pull-to-refresh or relaunch, which is not zero-training.

**Evidence (verify before editing).**
- web/src/app.js:3523-3527 — `initReader().catch(...)` (call at :3523; catch body :3524-3527) → `setLoading(true, "No se pudo cargar Signo Vivo.")` + `revealReader()` (corrected — the audit's :3523 is the call line).
- web/src/app.js:438-442 — `setLoading` renders the plain `#loading` div (index.html:59) — no button.
- web/src/app.js:12-47 — bootGuard: `showRecovery` early-returns when `window.__svBooted === true` (:20); the Reintentar card is :29-39 (reload button :36-38); it is exported as `window.__svRecover` (:43).
- web/src/app.js:59-66 — `revealReader` → `liftGateNow` sets `__svBooted = true` (:65).
- web/src/app.js:3532 — `setTimeout(liftGateNow, 4000)` backstop ALSO sets `__svBooted` — a slow-failing initReader (>4s) would find the flag already set, so the fix must clear it.

**Fix — step by step.**
1. Replace the catch body (:3524-3527):
   ```js
   initReader().catch((error) => {
     console.error("No se pudo iniciar el lector", error);
     // Boot never completed: surface the bootGuard's "Reintentar" card (big reload button)
     // instead of a dead-end text line. __svRecover no-ops once __svBooted is set — and the
     // 4s liftGateNow backstop may have set it even though boot FAILED — so clear it first.
     if (typeof window.__svRecover === "function") {
       try { window.__svBooted = false; } catch (_) {}
       window.__svRecover("init-reader", error);
     } else {
       // bootGuard missing (should not happen — it is the first script block): keep the old floor.
       setLoading(true, "No se pudo cargar Signo Vivo.");
       revealReader();
     }
   });
   ```
   Notes: do NOT call `revealReader()` on the card path (the card owns the screen; leaving the
   flag false is correct — boot did not succeed). `__svRecover` also feeds `__svReportCrash`
   (:19), so handled boot failures now reach crash telemetry for free. The card's idempotence
   (`__svRecoveryShown`, :21) means a double failure cannot stack cards.
2. In the shell, this path coexists with the native watchdog: initReader failing means
   `bridge-ready` was never posted (:3469 is initReader's last line), so the native 6s watchdog
   (PdfReaderApp.tsx:307-321) will remount regardless — the web card may flash first; both lead to
   recovery. No native change.
3. Copy: no new strings — the card already says **"Signo Vivo se está recuperando"**, **"Algo no
   cargó bien. Toca para reintentar."**, button **"Reintentar"** (:34-36).

**Acceptance criteria.**
- [ ] Force `initReader` to reject early (dev: throw at the top) → the Reintentar card appears; the button reloads and a healthy boot proceeds.
- [ ] Force a LATE rejection (dev: `await new Promise(r=>setTimeout(r,5000)); throw …` inside initReader) → card still appears despite the 4s backstop having fired.
- [ ] Normal boot: no card, no behavior change; `__svBooted` true after reveal.
- [ ] Benign error AFTER a successful boot → still record-only (bootGuard hijack guard intact).

**Tests.** No jsdom harness exists in e2e/; add a source-pin to `e2e/nearby-sync-contract.test.mjs`
that the initReader catch matches `/__svRecover\("init-reader"/` and does not call `revealReader`
before it. Manual browser step per acceptance (throttle + poisoned `#pages-data` is the realistic
repro: corrupt the inlined JSON in a local copy of dist and block the network fallback).

**Dependencies.** None. Rides PR-WS2-B.

---

#### FOLWEB-12 — First visit mid-Mass can visibly flash page 1 → page 2 → director's page when the relay peek loses its 1.5s race `low` `web` `web-only`

**Problem.** The `<img>` ships hardcoded `page-001.webp`; boot races the relay `/state` peek
against 1500ms, and on loss commits `renderPage(2)` and lifts the gate — then the still-in-flight
peek (6s abort) resolves and snaps to the director's page. On a 1.5–6s RTT first visit the user
sees two page flashes before landing live (self-healing via the requestId guard, but visibly janky
at exactly the "arrive late, open the WhatsApp link" moment).

**User impact at Mass.** Two page flashes before landing on the director's page; a first-timer may
start swiping from the portada before being yanked.

**Evidence (verify before editing).**
- web/src/index.html:54 — hardcoded `src="books/standard/pages/page-001.webp"`.
- web/src/app.js:3446-3448 — the peek race (`Promise.race([relayPollOnce(true), 1500ms])`, await at :3447); :3449 — `renderPage(DEFAULT_START_PAGE)` commits when `relay.hasDirector` is still false; :3451 — gate lifts; :215 — `DEFAULT_START_PAGE = 2`.
- web/src/app.js:3174-3210 — `relayPollOnce` never rejects (whole-body catch :3209) and its fetch aborts at 6s (:3179) — an awaited peek is bounded.
- web/src/app.js:1031/:1049 — requestId guard (why the late snap self-heals today, and why holding is safe).
- web/src/app.js:3532 — `setTimeout(liftGateNow, 4000)` — the gate backstop that bounds how long the loader can visibly hold.

**Fix — step by step.** Never commit page 2 while the peek response is still in flight — hold the
boot loader until the in-flight peek settles (bounded by its own 6s abort) instead of extending the
arbitrary window.
1. Replace :3446-3448 with:
   ```js
   if (!hasNativeBridge() && !NATIVE_FILE_MODE) {
     // Boot peek: open directly on the director's page when one is live. Give it 1.5s for
     // free; if it is STILL in flight after that, keep the boot loader up until it settles
     // (≤6s via relayPollOnce's abort) rather than committing page 2 mid-flight — a late
     // follow decision would visibly flash 2 → director. relayPollOnce never rejects.
     const peek = relayPollOnce(true);
     const first = await Promise.race([
       peek.then(() => "settled"),
       new Promise((resolve) => setTimeout(() => resolve("pending"), 1500)),
     ]);
     if (first === "pending") await peek;
   }
   ```
2. Keep :3449 as the fallback render (per FOLNAT-04 it becomes `renderPage(state.currentPage, …)`;
   on web that is still page 2).
3. Trade-off to note in the PR: on a DEAD relay a first paint can now wait up to ~6s instead of
   1.5s; the 4s gate backstop (:3532) lifts the spinner at 4s, so worst case shows the hardcoded
   page 1 for ~2s before page 2 — rarer and calmer than the current 1→2→director triple-flash on a
   LIVE director. If Miguel ever reports slow dead-relay first paints, the lever is the abort at
   :3179, not this block.
4. No native impact (the whole block is skipped in the shell).

**Acceptance criteria.**
- [ ] DevTools route-delay `/state` by 3s with a LIVE director publishing: first page painted is the DIRECTOR's; the loader holds ~3s; never page 2 → snap.
- [ ] `/state` delayed 3s with NO director (stale snapshot): page 2 paints at ~3s, single commit.
- [ ] Relay unreachable (blackhole): page 2 paints by ~6s, gate spinner till 4s, no error.
- [ ] Fast relay (<1.5s), director live: unchanged instant open on the director's page.
- [ ] `state.pageLoadRequest` increments exactly once through boot in the live-director case (instrument temporarily).

**Tests.** No browser-automation harness exists in this repo's safe set; verify manually with
DevTools request blocking/delay per acceptance (do it at phone width — this is the WhatsApp-link
arrival flow). Guard the shape with a source-pin in `e2e/nearby-sync-contract.test.mjs`: initReader
matches `/first === "pending"[\s\S]*?await peek/` (or equivalent) so the hold isn't refactored away.

**Dependencies.** Coordinate line-level with FOLNAT-04 (same block). Rides PR-WS2-B.

---

## 2.2 Workstream-wide manual verification day (after PR-WS2-C's TestFlight build)

One iPad-portrait pass stitching the findings together (director device + follower iPad):
1. Deny Local Network on the follower → FOLNAT-02 banner → fix in Ajustes → banner clears.
2. Director live; follower ♪-jumps away → stays + bar (FOLNAT-01); director turns 3 pages; tap bar
   → lands on the newest; stray-swipe → yanked ≤1s; ⟳ during a browse → re-homes.
3. Force-quit the follower on page 250 → relaunch offline → boots on 250 (FOLNAT-04).
4. Gut one page asset in a dev bundle → director sits on it → no strobe, drawer usable, recovery
   on director move (N2W-04).
5. Transmitter phone: enter code → exit → no ⟳ (PARITY-05).
6. Web phone beside them: scrub-hunt a song for 8s → settle page lands ≤2s (SYNCE2E-09), set the
   director device clock +5 min → pages still flow 1:1 (SYNCE2E-01).


---

## Workstream 3 — Web IA, onboarding, help & copy

> Repo: `(repo root)` · HEAD `d5075091` (build 381).
> All file:line anchors below were re-verified by direct read at this HEAD. Files touched are almost
> exclusively `web/src/*` (app.js = 3572 lines, index.html = 363, styles.css = 2345, manifest.webmanifest = 35,
> sw.js = 237), plus one additive `sync-worker` change (FOLWEB-02) and new test files under `e2e/`.

## 1. Workstream intro (WS3)

**Theme.** The shared web bundle (served at signovivo.com AND bundled into the native shell) has a
discoverability and self-service hole: the entire browse/search IA hides behind an unmarked edge
swipe, the help panel is unreachable dead code describing retired UI, nothing on first open says
what the app is or that it auto-follows, and the operational rituals that keep the parish's one
mandatory web device alive (Add-to-Home-Screen, Auto-Lock → Nunca on the old pre-iOS-16.4 iPad)
exist only as oral tradition in Miguel's head. Around that core sit correctness-and-polish items:
the numpad silently teleports to page 371 on unknown numbers or a cold index, "Recientes" records
almost nothing a follower actually visits, ~500 lines of confirmed-dead UI ship in every bundle,
Spanish copy is inconsistent ("canto" vs "canción", missing accents, "relé" jargon), Android
installs hide the system clock, and a long-press on the score pops iOS's Copy/Save sheet over the
music.

**Why it matters at Mass.** The audience is an elderly, Spanish-speaking congregation with a
zero-training bar. Every gap here converts into a live-Mass failure mode: a follower who mishears a
song number gets teleported to the back of the book with no error AND silently dropped out of live
follow; a confused follower has no reachable "¿Cómo funciona?"; the old iPad goes dark at its
Auto-Lock interval mid-hymn because nobody re-set "Nunca"; a substitute operator can't provision a
replacement device without Miguel. None of these pages the developer — they just quietly degrade
the singing.

**How the findings interact.**

- **IANAV-02 is the keystone.** Giving followers a visible entry to the drawer makes FOLWEB-05's
  revived help honest (its "franja oscura a la izquierda" line becomes true again), and it must be
  designed jointly with **DIRNAT-05** (sibling workstream: the 44px left-edge swipe zone hijacks
  the director's previous-page swipe). The visible handle is precisely what lets DIRNAT-05 shrink
  or disable the invisible edge gesture for directors without stranding them.
- **Exposing browse without the browsing-flag fix is a regression.** The known-open prior-art item
  `web-reader-browse-result-click-skips-relay-browsing-mode` (this audit's IANAV-05, verdict
  duplicate-of-known) means any drawer/search navigation by a live web follower is boomeranged back
  by the F1 heartbeat every ~4s (app.js:3291). IANAV-02 therefore carries a small browse-intent
  helper as a hard prerequisite (Step 4) — land it here if the sync workstream hasn't already.
- **FOLWEB-05 (help) is the durable home for the guidance one-shots.** IANAV-07's first-run hint,
  FOLWEB-03's install steps and FOLWEB-02's Auto-Lock protocol each get a transient banner/card;
  the rewritten help panel repeats all three permanently.
- **IANAV-10's terminology decision gates all new copy.** Pick "canto" vs "canción" (decision below)
  before writing FOLWEB-05's help text, IANAV-07's hint, and IANAV-08's empty-state line, or the
  copy pass has to re-touch them.
- **IANAV-09 (dead-UI deletion) must land with/after FOLWEB-05**, because it deletes the hidden
  `#help-button` stub that FOLWEB-05 replaces — and its deletion list has null-crash landmines
  (unguarded DOM references in live functions) that are enumerated per-reference below.
- **FOLWEB-07 and IANAV-08 chain**: once the numpad refuses to navigate to nonexistent numbers,
  Recientes pollution by typed garbage disappears; IANAV-08 then adds dwell-based recording so
  followed songs finally appear.

**WIRE COMPAT (whole workstream).** Everything here is additive and web-shippable: no new bridge
message types, no changes to `page-changed` / `director-code` / `sync-event` shapes, no relay
protocol changes. Native builds 368–381 with old bundled web copies keep working unchanged;
signovivo.com (phones + the old-iPad PWA) picks fixes up on the next Pages deploy; parish native
iPads pick them up at the next native build (or mesh bundle push) — flagged per finding as "native
catches up at next build". The one exception: FOLWEB-02's fleet `wakeLock` field needs a
**sync-worker deploy** (additive — the worker's check-in sanitizer drops unknown fields, so old
web bundles + new worker and new bundle + old worker both degrade harmlessly).

**Recommended PR slicing (3 PRs, each independently shippable):**

| PR | Findings | Contents | Risk |
|---|---|---|---|
| **WS3-PR1 — Follower navigation entry + gesture guards** | IANAV-02 (+ browse-intent prerequisite) | Un-hide the drawer handle (pending decision), gate the window edge-swipe on the ♪ modal, browse-intent helper for search/drawer/swipe/arrow navigation | Medium — touches gesture code shared with DIRNAT-05; coordinate before merge |
| **WS3-PR2 — Guidance surfaces + dead-UI reconciliation** | FOLWEB-05, IANAV-07, FOLWEB-03, FOLWEB-02, IANAV-09 | Revive help behind a visible opener + rewrite content; first-run hint; A2HS install card; Auto-Lock banner + fleet `wakeLock` (worker sub-PR); delete the dead index panel / Teclado panel / stubs | Medium-high line count but mechanical; IANAV-09 has enumerated landmines + contract test |
| **WS3-PR3 — Copy pass + numpad/recientes/sort correctness + platform polish** | IANAV-10, FOLWEB-07, IANAV-08, IANAV-12, FOLWEB-11, PARITY-10 | Accents/terminology/banner copy; numpad hardening + index-hydrate retries; dwell-based Recientes; persisted segmented sort; manifest `display_override`; touch-callout suppression | Low — strings, small logic, CSS |

Order: PR1 and PR3 are independent (either first). PR2 last — its help copy documents PR1's chosen
entry, and IANAV-09 deletes the stub FOLWEB-05 replaces. If IANAV-09 feels too hot to ride with
PR2, split it out as WS3-PR2b using the same checklist; do NOT ship it before FOLWEB-05.

**Shared browser-preview setup (used by every "Tests" section below).**

1. One-time build (renders 371 PDF pages; needs `pdftoppm` + `cwebp`; several minutes):
   `node web/build.mjs`
2. Serve via the existing `.claude/launch.json` config `web-reader` (`npx serve -p 4000 web/dist`)
   with `preview_start` → http://localhost:4000.
3. Iteration loop — `build.mjs` wipes `dist/` and re-renders the PDF every run, so for shell-file
   edits copy instead of rebuilding:
   - `cp web/src/styles.css web/dist/styles.css`
   - `cp web/src/app.js web/dist/app.js` (the `__CACHE_VERSION__`/`__BUILD_NUMBER__`/`__RELAY_BASE__`
     tokens stay un-replaced; all three are guarded in code — `RELAY_BASE` falls back to the
     production worker, the build badge just hides)
   - `cp web/src/lib/*.js web/dist/lib/`
   - index.html edits (re-inline the two data blobs):
     `node -e 'const fs=require("fs");fs.writeFileSync("web/dist/index.html",fs.readFileSync("web/src/index.html","utf8").replace("</head>","  <script id=\"books-data\" type=\"application/json\">{\"default\":\"standard\",\"books\":{\"standard\":{\"label\":\"Manual Alvernia\",\"totalPages\":371}}}</script>\n  <script id=\"pages-data\" type=\"application/json\">{\"totalPages\":371}</script>\n</head>"))'`
4. ⚠️ A local preview joins the **production relay room read-only** (the web bundle has zero publish
   code) and fires one fleet check-in POST — harmless noise on the dashboard, never a page-flip
   risk. It also registers a service worker on localhost; use a hard reload (or
   `preview_eval` → `caches.keys().then(k=>Promise.all(k.map(x=>caches.delete(x))))` + unregister)
   when styles look stale.
5. Reset one-time hints between checks with `preview_eval`:
   `localStorage.removeItem("sv-hello");localStorage.removeItem("sv-a2hs-dismissed");localStorage.removeItem("sv-autolock-hint");location.reload()`
6. Touch-swipe simulation snippet (Chromium supports constructed TouchEvents) — used for the edge
   gesture checks:
   ```js
   (function swipe(x1,y1,x2,y2){const t=(x,y,id)=>new Touch({identifier:id,target:document.body,clientX:x,clientY:y});
     window.dispatchEvent(new TouchEvent("touchstart",{touches:[t(x1,y1,1)],bubbles:true}));
     window.dispatchEvent(new TouchEvent("touchend",{changedTouches:[t(x2,y2,1)],touches:[],bubbles:true}));})(10,400,120,405)
   ```

**Safe test commands.** Only ever run named files:
`node --test e2e/web-ia-contract.test.mjs e2e/svUiDecisions.test.mjs` (both new, defined below).
**NEVER run `npm run test:e2e` and NEVER run `e2e/relay-sync.test.mjs`** — the latter publishes to
the PRODUCTION relay room and flips live followers' pages.

---

## 2. Findings (WS3)

#### IANAV-02 — Entire browse/search IA is behind an invisible left-edge swipe; followers have no visible entry to the song list at all `high` `web` `web-only`

**Problem.** The drawer holds the app's whole browse IA — buscar / misa / recientes / tiempo /
temas / tono / todas — but its only follower-reachable opener is an undocumented left-edge swipe:
the pull-tab is hidden by CSS ("native parity"), the ⌕ fab is director-only, and the ♪ modal is a
numpad, not a list. The same 44px edge zone steals "previous page" swipes, and the window-level
copy of the gesture even fires while the ♪ modal is open, opening the drawer invisibly beneath it.
In a plain iOS Safari tab (the WhatsApp-link arrival path) the edge swipe collides with the system
back gesture and can navigate away from signovivo.com entirely.

**User impact at Mass.** Elderly followers can never discover search or the song lists — if they
mishear the announced number, the numpad is a dead end. The one surface that would teach the
gesture (help) is itself unreachable (FOLWEB-05).

**Evidence (verify before editing).**
- `web/src/styles.css:2330-2331` — `.drawer-handle { display: none !important; }` under the comment
  "Native parity: no left side-bar. The ♪ (upper-right) is the song navigator."
- `web/src/index.html:111-146` — the ♪ modal ("IR A CANTO") is numpad-only: display + digit grid +
  "♪ Abrir Canto ♪"/"Cancelar"; no list mode. (corrected — modal block spans :111-146, not :111-143)
- `web/src/index.html:78-80` + `web/src/app.js:2479-2482` — the handle still exists in DOM and its
  click listener is live; only the CSS hides it.
- `web/src/app.js:2723-2729` — viewer edge-swipe opens the drawer when `startX < 44 && deltaX > 40`;
  the page-turn branch at `:2733` hard-requires `startX >= 44` — the zones collide (DIRNAT-05).
- `web/src/app.js:2799-2810` — window-level duplicate edge handler; its fire condition at `:2806`
  (`dx > 40 && … && !state.drawerOpen`) has **no `state.songJumpOpen` check** (corrected — the
  guardless condition is at :2806; :2799 is where the `touchend` listener begins).
- `web/src/styles.css:2154-2155` — `.search-fab { display: none; }`, shown only under
  `html[data-role="director"]`; `web/src/app.js:2410-2411` — ⌕ opens the drawer as a search
  dropdown; `activateTab` (`:2327-2361`) has no role checks.
- `web/src/app.js:1116` — `openDrawer` hard-codes browse mode (the drawer numpad is retired).
- Overlay stacking for the modal-underlap bug: `.overlay-controls` z-index 4 (`styles.css:361-368`)
  vs `.song-jump-modal` z-index 200 (`styles.css:2163-2171`).

**Fix — step by step.**

> **DECISION-REQUIRED (Miguel) — which visible entry do followers get?**
> - **Option A (recommended): un-hide the existing drawer pull-tab.** One CSS deletion; already
>   styled + wired; full IA access (all 7 rails), not just search; gives directors a tap
>   alternative so DIRNAT-05 can shrink/disable their edge-swipe; makes the help text true again.
>   Cost: a permanent 24px strip on the left edge of the score.
> - **Option B: make ♪ a two-option sheet ("🔢 Número" / "📚 Lista").** Zero standing clutter, but
>   adds a tap to the number flow used mid-Mass and is a bigger diff (modal redesign + copy).
> - **Option C: show ⌕ for everyone.** Cheapest, but the ⌕ dropdown is search-only
>   (`as-dropdown.search-fullscreen` hides the tab rail and drawer chrome — `styles.css:1043-1046`),
>   so misa/recientes/temas/tono/todas stay hidden; insufficient alone, fine as a complement.
> Recommendation: **A**, with C optionally added later. Steps below assume A.

1. **Prerequisite — browse-intent routing** (skip only if the sync workstream has already landed the
   known finding `web-reader-browse-result-click-skips-relay-browsing-mode` / IANAV-05; check
   `git log` for it). Without this, a live web follower who taps a drawer song is yanked back by
   the F1 heartbeat every ~4s (`app.js:3286-3294`). Add one helper right after `goToDraftSong`
   (~`app.js:1207`), mirroring the numpad tail at `:1200-1205`:
   ```js
   // Any INTENTIONAL local navigation off the director's live page = browse mode:
   // pause auto-follow and surface the way back. No-op with no live director (and on
   // native, where the relay is disabled and relay.hasDirector is always false).
   const noteIntentionalBrowse = (targetPage) => {
     if (!hasLiveDirector() || liveDirectorPage() == null) return;
     if (targetPage === liveDirectorPage()) return; // transport-agnostic: FOLNAT-01's mesh tracker feeds these helpers on native
     if (targetPage === relay.livePage) return;
     relay.browsing = true;
     relay.following = false;
     showGoLiveBar();
     renderRelayPill();
   };
   ```
   Call it with the target page from: the search/drawer result tap (`app.js:2581`, after
   `renderPage(pageNum)` → `noteIntentionalBrowse(pageNum)`); `turnSong` (`:1971` and `:1979`,
   using `state.songIndex[...].page` as the target); `turnPage` (`:1987`, using `nextPage`).
   Arrow keys (`:2823-2824`) route through `turnSong` — covered. Then simplify the numpad tail
   (`:1200-1205`) to call the same helper. Forward references are safe: `relay`/`showGoLiveBar`
   are hoisted `const`s executed long before any tap.
2. **Un-hide the handle.** Delete `web/src/styles.css:2330-2331` (both the comment line and the
   rule). The handle renders at left-center, 24×64px, z-index 6 (`styles.css:372-395`) and already
   hides itself while the drawer is open (`app.js:1110` / `:1123`).
3. **Make the tap target elder-friendly** (optional but cheap). In the `.drawer-handle` rule
   (`styles.css:372-395`) bump `height: 64px` → `height: 88px` and `.drawer-handle-lines`
   `font-size: 14px` → `18px`. Do not widen past 28px — it overlays the score.
4. **Copy.** `web/src/index.html:78` — change `aria-label="Abrir controles"` to
   `aria-label="Abrir la lista de cantos"` (term pending IANAV-10's decision; if "canción" wins,
   use `"Abrir la lista de canciones"`).
5. **Gate the window-level edge handler on the modal.** At `web/src/app.js:2806` extend the
   condition:
   ```js
   if (dx > 40 && Math.abs(dx) > Math.abs(dy) && !state.drawerOpen && !state.songJumpOpen) {
   ```
   (The viewer-level handler at `:2725` needs no gate — the modal's full-screen backdrop, z-200,
   sits above `viewer-shell`, so its `touchstart` never fires while the modal is open.)
6. **DIRNAT-05 coordination (explicit dependency).** Do NOT change edge-zone geometry (44px /
   40px thresholds) in this PR — that is DIRNAT-05's change, scoped to
   `html[data-role="director"]`. State in the PR description: "With the handle visible, DIRNAT-05
   may shrink the director edge zone (44→24px) or disable the director edge-swipe entirely; the
   handle remains the deliberate entry." If DIRNAT-05 lands first and extracts a pure gesture
   decision helper (`startX, dx, dy, role → action`), route both `touchend` handlers here through
   it instead of duplicating conditions.
7. **Fix the stale comment** at `app.js:2408` ("Tapping the song title is the discoverable,
   deliberate entry…" — `#song-status` lives inside the drawer, `index.html:175`, so it cannot be
   the discovery path). Replace with: `// Tapping the song title (inside the drawer) also opens the jump modal.`

**Acceptance criteria.**
- [ ] A brand-new follower (no `data-role`, fresh localStorage) can reach the song list AND search
      within 2 taps of always-visible UI, with no gestures.
- [ ] With the ♪ modal open, a left-edge swipe does NOT open the drawer beneath it (verify
      `#overlay-controls` never gains `drawer-open` while `#song-jump-modal` is visible).
- [ ] With a live director (or simulated snapshot), tapping a drawer/search song shows the amber
      pill + "↩ Volver a en vivo" bar and the page is NOT re-homed 4s later.
- [ ] Director layout unchanged in this PR: badge visible, ⌕ visible, ♪ shifted left; the handle is
      visible for directors too (until DIRNAT-05 decides otherwise).
- [ ] Handle hidden while the drawer is open (existing behavior intact).

**Tests.**
- New `e2e/web-ia-contract.test.mjs` (static-source contract style, mirroring
  `e2e/permission-flow.test.mjs` — regex asserts on `web/src/*`; run with
  `node --test e2e/web-ia-contract.test.mjs`):
  - `styles.css` does NOT match `/\.drawer-handle\s*\{\s*display:\s*none/`.
  - `app.js` window-edge `touchend` block matches `/!state\.songJumpOpen/`.
  - `app.js` defines `noteIntentionalBrowse` and calls it in the `searchResults` click handler,
    `turnSong`, and `turnPage`.
- Browser preview (setup above): resize mobile (375×812). `preview_inspect` `.drawer-handle` —
  computed `display` must be `flex`, not `none`. `preview_click` `#drawer-handle` →
  `preview_inspect` `#overlay-controls` classList contains `drawer-open`; rails Buscar…Todas
  visible in `preview_snapshot`. Close the drawer, `preview_click` `#song-jump-trigger`, run the
  swipe-simulation snippet (10,400 → 120,405), then `preview_eval`
  `document.getElementById("overlay-controls").classList.contains("drawer-open")` → must be
  `false`. Simulate director: `preview_eval` `document.documentElement.dataset.role="director"`,
  re-inspect layout.

**Dependencies.** Hard: browse-intent routing (Step 1 — known prior-art item
`web-reader-browse-result-click-skips-relay-browsing-mode`, a.k.a. IANAV-05, possibly owned by the
sync workstream; land once, not twice). Cross-workstream: **DIRNAT-05** (design the entry and any
edge-zone change together; this PR adds the tap alternative DIRNAT-05 relies on). Related, not
blocking: FOLNAT-01 (native mesh browse model, other workstream) governs when browse actually works
on parish iPads; this finding's exposure reaches iPads only at the next native build anyway.
FOLWEB-05's help text documents whichever entry ships (land FOLWEB-05 after).

---

#### IANAV-07 — First-run web open explains nothing: no app identity, no "it auto-follows", no no-director state `medium` `web` `web-only`

**Problem.** A cold open of signovivo.com with no director live shows: loader → page 2 of the
scanned book + three unlabeled glyphs. No app name, no sentence saying the page follows the choir
automatically, and no status surface at all before Mass (the live pill is `display:none` whenever
there is no director). The one perfect one-liner exists only in the OG meta tag, shown by link
previews and never by the app.

**User impact at Mass.** A first-time congregant must infer "do nothing, it follows" from an 8px
green dot; pre-Mass there is nothing to infer from, and pages later change "by magic" — or ⟳ spins
1.1s and appears to do nothing.

**Evidence (verify before editing).**
- `web/src/app.js:215` — `DEFAULT_START_PAGE = 2`; `:3446-3448` — 1500ms relay boot peek; `:3449` —
  `if (!relay.hasDirector) renderPage(DEFAULT_START_PAGE, …)`.
- `web/src/app.js:3036-3042` — `renderRelayPill` sets `pill.style.display = "none"` when
  `!relay.hasDirector` (`:3038`) — zero status surface pre-Mass.
- `web/src/index.html:19` — `og:description` = "Manual de coro Signo Vivo — sigue al director en
  vivo desde tu teléfono." — never rendered in-app.
- Negative space (re-verified by grep): no first-run/onboarding string or flag exists anywhere in
  `web/src/` (`sv-hello` has zero hits; the only tip copy is the dead numpad tip,
  `index.html:214-217`).

**Fix — step by step.**

Scope guard: the *persistent* live/no-live signal is M4's planned tri-state pill — do NOT build it
here. This finding ships only the one-time first-run hint.

1. **Storage flag + gating.** In `web/src/app.js`, near the other pref keys (`~:393`):
   ```js
   const HELLO_KEY = "sv-hello";
   ```
   Add the pure decision to the new lib `web/src/lib/svUiDecisions.js` (created in this
   workstream; UMD/CommonJS-compatible like `svSyncDecision.js`, loaded via a `<script defer>` tag
   BEFORE app.js in `index.html:358-360`):
   ```js
   // svUiDecisions.js (excerpt)
   function shouldShowFirstRunHint({ native, dismissed }) {
     return !native && !dismissed;
   }
   ```
2. **Render helper** in app.js (pattern: `showRelayAuthWarning`, `app.js:887-925` — injected style +
   element, idempotent). Fixed bottom-center card, z-index 47 (below the go-live bar's 48), hidden
   under `body.sv-drawer-open`:
   ```js
   let helloEl = null;
   const maybeShowFirstRunHint = () => {
     let dismissed = true;
     try { dismissed = localStorage.getItem(HELLO_KEY) === "1"; } catch (_) {}
     const lib = globalThis.svUiDecisions;
     const show = lib && typeof lib.shouldShowFirstRunHint === "function"
       ? lib.shouldShowFirstRunHint({ native: NATIVE_FILE_MODE || hasNativeBridge(), dismissed })
       : false; // lib failed to load → skip the nicety, never break boot
     if (!show) return;
     try { localStorage.setItem(HELLO_KEY, "1"); } catch (_) {} // true one-time: mark on SHOW
     // …create #sv-hello card, appendChild, auto-remove after 30s…
   };
   ```
   Exact es-MX copy (title bold, one sentence, one button):
   - Title: **"Signo Vivo"**
   - Body: **"Cuando el coro cante, la página cambia sola. Toca ♪ para ir a un canto."**
   - Button: **"Entendido"**
   (If IANAV-10's decision lands on "canción", body becomes "…Toca ♪ para ir a una canción.")
3. **Call site.** In `initReader`, immediately after `revealReader()` (`app.js:3451`):
   `maybeShowFirstRunHint();`. Never `await` it; wrap the whole body in try/catch (house M2 rule:
   a nicety must never break boot).
4. **Suppression rules.** Native excluded (`NATIVE_FILE_MODE || hasNativeBridge()` — parish iPads
   are pre-configured); auto-hide after 30s; hide immediately if the user opens the drawer or the
   ♪ modal (add `document.body.classList.contains("sv-drawer-open")` CSS hide +
   remove-on-`openSongJump` is overkill — a simple CSS rule
   `body.sv-drawer-open #sv-hello{display:none}` suffices).
5. **Durable copy of the message** goes into FOLWEB-05's rewritten help (item 1 there) — that is
   the "missed the hint" fallback.

**Acceptance criteria.**
- [ ] Fresh profile (no localStorage), no director: after the reader reveals, the card shows the
      app name + the auto-follow sentence without any interaction; "Entendido" dismisses it.
- [ ] Reloading after (a) dismissal or (b) simply having seen it once → card never returns.
- [ ] Native file mode / bridged WebView never shows it (flag check is irrelevant there).
- [ ] Card never overlaps the "↩ Volver a en vivo" bar (z-47 vs z-48) and hides while the drawer
      is open.
- [ ] A storage-disabled browser (localStorage throws) boots normally (card may show every time —
      acceptable degraded mode; must never throw).

**Tests.**
- `e2e/svUiDecisions.test.mjs` (new; `createRequire` pattern copied from
  `e2e/svSyncDecision.test.mjs`): `shouldShowFirstRunHint` truth table — `{native:true}` → false,
  `{dismissed:true}` → false, `{native:false,dismissed:false}` → true.
- `e2e/web-ia-contract.test.mjs`: app.js matches `/sv-hello/` and calls `maybeShowFirstRunHint()`
  after `revealReader()` in `initReader`; `index.html` loads `lib/svUiDecisions.js` before
  `app.js`.
- Browser preview: `preview_eval` `localStorage.removeItem("sv-hello");location.reload()` →
  `preview_snapshot` shows the card text "Cuando el coro cante, la página cambia sola…";
  `preview_click` the "Entendido" button → card gone; reload → card does not return.

**Dependencies.** None blocking. Copy term gated by IANAV-10's canto/canción decision. The
persistent status half is explicitly deferred to M4's tri-state pill (do not duplicate). Ships
cleanly in WS3-PR2.

---

#### FOLWEB-02 — No wake-lock fallback and no Auto-Lock guidance on the exact device class the parish uses (pre-iOS-16.4 web-PWA follower) `medium` `web` `web-only`

**Problem.** `initScreenWakeLock` silently no-ops when `navigator.wakeLock` is absent — which is
every iOS < 16.4 device, i.e. exactly the old iPad that MUST run the web PWA because it can't get
TestFlight. Even where supported, `acquire()` swallows rejections (iOS Low Power Mode). The "set
Auto-Lock → Nunca" requirement exists only as oral tradition; nothing in the app or the fleet
dashboard surfaces it, and the fleet check-in payload has no wake-lock capability field so the
operator can't see which device is at risk.

**User impact at Mass.** The old iPad's screen goes dark at its Auto-Lock interval mid-Mass; an
elderly follower doesn't know the wake-and-reopen dance. If someone left Auto-Lock at "2 minutos",
it repeats all Mass.

**Evidence (verify before editing).**
- `web/src/app.js:3500-3516` — `initScreenWakeLock`; `:3502` returns silently when
  `!("wakeLock" in navigator)`; `:3509` swallows `acquire()` rejections (`sentinel = null`).
- `web/src/app.js:2951-2983` — `fleetCheckin`; the payload literal at `:2962-2971` has
  `webCached/pagesCached/totalPages/homeScreen/cacheVersion` but **no wake-lock field**
  (corrected — the payload object is at :2962-2971; the record's `:2960` cite is the `totalPages`
  computation two lines above).
- `sync-worker/src/index.ts:250-289` — **(new evidence)** `checkin()` sanitizes with an explicit
  per-field allowlist (`:272-280`); unknown fields are DROPPED, so "add wakeLock to the check-in"
  requires a worker change too. `FleetDevice` type at `:64-76`.
- `web/src/app.js:203-205` — `isStandaloneApp` (display-mode standalone/fullscreen or
  `navigator.standalone`); `:202` — `isIOS`. Both available for gating.

**Fix — step by step.**

1. **Pure predicate** in `web/src/lib/svUiDecisions.js`:
   ```js
   function shouldShowAutoLockHint({ wakeLockSupported, ios, standalone, native, dismissed }) {
     return !wakeLockSupported && ios && standalone && !native && !dismissed;
   }
   ```
   Gate on `standalone` deliberately: the target is the home-screen old iPad; casual Safari
   visitors aren't nagged (they get FOLWEB-03's install card instead).
2. **Surface it where the capability check fails.** In `initScreenWakeLock`
   (`web/src/app.js:3502`), replace the bare `return` with:
   ```js
   if (!("wakeLock" in navigator) || !navigator.wakeLock || typeof navigator.wakeLock.request !== "function") {
     maybeShowAutoLockHint();   // pre-16.4 iOS (the old-iPad PWA) — teach the Settings protocol once
     return;
   }
   ```
   `maybeShowAutoLockHint()` follows the exact same injected-banner pattern as IANAV-07's card
   (idempotent, try/catch everything, localStorage key `sv-autolock-hint`, set-on-dismiss this time
   — the protocol matters enough to re-show each session until acknowledged). Style it as an
   informational banner (navy `#1a2a4a`-style, NOT the red alert style of `#sv-relay-warn`), fixed
   top-center, dismiss button.
   Exact es-MX copy:
   - Body: **"Para que la pantalla no se apague durante la Misa: abre Ajustes → Pantalla y brillo → Bloqueo automático y elige “Nunca”."**
   - Button: **"Entendido"**
3. **Fleet capability field (web half — additive).** In the `fleetCheckin` payload
   (`app.js:2962-2971`) add:
   ```js
   wakeLock: "wakeLock" in navigator,
   ```
4. **Fleet capability field (worker half — separate additive deploy).** In
   `sync-worker/src/index.ts`:
   - `FleetDevice` type (`:64-76`): add `wakeLock?: boolean; // web only — Screen Wake Lock API available`
   - `checkin()` allowlist (after the `homeScreen` line at `:278`): add
     `if (o.wakeLock != null) entry.wakeLock = Boolean(o.wakeLock);`
   - Dashboard rendering is OPTIONAL here — coordinate with RELVER-04 (fleet Web-column rework,
     other workstream) so the column lands once. Minimal interim: nothing; the field is queryable.
   Wire compat: old bundles never send it (worker fine); new bundles against the old worker get the
   field silently dropped (fine).
5. **DECISION-REQUIRED (Miguel) — NoSleep-style muted-video fallback?** A tiny looping muted
   `<video>` while `relay.hasDirector` keeps pre-16.4 screens awake without Settings.
   **Recommendation: skip it.** Battery cost on an old device through a full Mass, autoplay-policy
   fragility across old Safari versions, and the banner + one-time Settings change already solve
   the fleet device durably. Revisit only if the device day shows the Settings protocol failing in
   practice.
6. Leave the `:3509` rejection swallow as-is (Low Power Mode denial is transient; visibilitychange
   re-acquires at `:3511-3513`). Optionally add `console.info("wakeLock denegado")` — no UI.

**Acceptance criteria.**
- [ ] On an iOS < 16.4 standalone launch (real old iPad on the next device day): the banner shows
      once per session until "Entendido", never blocks boot, and never appears again after
      dismissal.
- [ ] On iOS ≥ 16.4 / desktop / Android: nothing changes (wakeLock path untouched).
- [ ] Native file mode: never shows.
- [ ] Fleet check-in from a wake-lock-capable browser stores `wakeLock: true` on the worker (and
      `false` from the old iPad).
- [ ] A storage-disabled browser still boots (banner may re-show; must never throw).

**Tests.**
- `e2e/svUiDecisions.test.mjs`: `shouldShowAutoLockHint` truth table — supported→false,
  non-iOS→false, non-standalone→false, native→false, dismissed→false, the old-iPad tuple
  `{wakeLockSupported:false, ios:true, standalone:true, native:false, dismissed:false}`→true.
- `e2e/web-ia-contract.test.mjs`: app.js fleet payload matches `/wakeLock:\s*"wakeLock" in navigator/`;
  `sync-worker/src/index.ts` matches `/o\.wakeLock != null/` and `FleetDevice` contains `wakeLock?:`.
- Browser preview: desktop Chrome HAS `navigator.wakeLock`, so assert the NO-banner path
  (`preview_eval` `!!document.getElementById("sv-autolock-hint")` → false). To eyeball the banner,
  temporarily invert the predicate call in the working copy, `cp` to dist, reload, screenshot,
  revert. The pure-lib tests carry the real logic; the affirmative device check belongs on the M7
  device day with the actual old iPad (add it to that script).

**Dependencies.** None blocking. Worker sub-change coordinates with RELVER-04 (dashboard column,
other workstream). Long-term duplicate of the guidance goes into FOLWEB-05's help ("¿La pantalla se
apaga? Ajustes → …"). Ships in WS3-PR2 (+ a one-line worker PR).

---

#### FOLWEB-03 — Add-to-Home-Screen is guided nowhere in-app — the PWA install protocol is oral tradition `medium` `web` `web-only`

**Problem.** The bundle contains zero install-related strings or logic: no `beforeinstallprompt`
handler, no Share→Add-to-Home-Screen instructions, nothing. `isStandaloneApp` is computed but used
only for fleet reporting and the iOS pseudo-fullscreen gate. The manifest is fully install-ready —
only the guidance is missing, so WhatsApp-link arrivals stay in a Safari tab (no offline
resilience, browser chrome, back-swipe hazards) and provisioning a replacement old-iPad follower
depends entirely on Miguel remembering the ritual.

**User impact at Mass.** A congregant who arrives at a no-wifi church with the site un-cached in a
Safari tab gets nothing; if Miguel is unavailable nobody can provision a replacement follower
device.

**Evidence (verify before editing).**
- `web/src/app.js:203-205` — `isStandaloneApp` computed; its only uses are `:213`
  (`canOfferPseudoFullscreen`) and `:2968` (fleet `homeScreen`).
- Negative space (re-verified by grep): zero occurrences of `beforeinstallprompt`, `instalar`, or
  any A2HS string anywhere in `web/src/`.
- `web/src/index.html:15-30` — og:/twitter: share cards exist precisely for the WhatsApp-link
  arrival flow; no install prompt follows arrival.
- `web/src/manifest.webmanifest:1-35` — install-ready: `display: standalone`, 192/512 icons +
  maskable, `start_url: "/"`.

**Fix — step by step.**

1. **Pure predicate** in `web/src/lib/svUiDecisions.js`:
   ```js
   function shouldOfferInstall({ standalone, native, dismissed, hasDirector, selftest }) {
     return !standalone && !native && !dismissed && !hasDirector && !selftest;
   }
   ```
2. **Capture the Chromium prompt early.** At module scope in `app.js` (near the environment
   detection block, `~:200`):
   ```js
   let deferredInstallPrompt = null;
   window.addEventListener("beforeinstallprompt", (e) => {
     e.preventDefault();
     deferredInstallPrompt = e;   // consumed by the install card's "Instalar" button
   });
   ```
3. **Show the card after boot settles.** In `initReader` after `revealReader()` (with IANAV-07's
   hint; if both are eligible, show the first-run hint this session and the install card from the
   NEXT session — simplest: `if (helloEl) return;` inside the install check), schedule:
   ```js
   window.setTimeout(maybeShowInstallCard, 8000);   // never during the boot-critical window
   ```
   `maybeShowInstallCard()` evaluates the predicate with
   `{ standalone: isStandaloneApp, native: NATIVE_FILE_MODE || hasNativeBridge(), dismissed, hasDirector: relay.hasDirector, selftest: /(?:^|[?&])selftest(?:=|&|$)/.test(initialUrl.search) }`
   — if a director is live at the 8s mark, skip for the whole session (never interrupt a Mass).
   localStorage key: `sv-a2hs-dismissed` (set on dismissal).
4. **Two platform bodies, one card** (bottom-center card, same injected pattern; icon optional):
   - iOS Safari (`isIOS` true, no `deferredInstallPrompt`):
     Body: **"Instala Signo Vivo en tu teléfono: toca Compartir (el botón ⬆︎) y luego “Agregar a pantalla de inicio”. Así funciona sin internet en la iglesia."**
     Button: **"Entendido"** (dismiss).
   - Android/Chromium (`deferredInstallPrompt` captured):
     Body: **"Instala Signo Vivo en tu teléfono — funciona sin internet en la iglesia."**
     Buttons: **"Instalar"** → `deferredInstallPrompt.prompt()` (then dismiss regardless of
     outcome), **"Ahora no"** (dismiss).
   - Desktop / other (no iOS, no prompt): show nothing (predicate passes but there is no useful
     instruction — add `const canInstruct = isIOS || !!deferredInstallPrompt;` to the gate).
5. **Durable copy** goes into FOLWEB-05's help (item "Sin internet" points to the same steps), so
   the one-time card is not the only home of the ritual.

**Acceptance criteria.**
- [ ] First visit in a plain iOS-Safari-like context (non-standalone) with no director live: card
      appears ~8s after the reader is usable, with the Share→Agregar instructions; dismissing
      persists across reloads.
- [ ] Standalone launches, native file mode, `?selftest`, and any session with a live director at
      the 8s mark: card never appears.
- [ ] Chromium: the captured `beforeinstallprompt` drives a real install from the "Instalar"
      button.
- [ ] The card never traps input: it is dismissible, and page swipes/♪ still work with it visible.

**Tests.**
- `e2e/svUiDecisions.test.mjs`: `shouldOfferInstall` truth table (standalone→false, native→false,
  dismissed→false, hasDirector→false, selftest→false, the arrival tuple→true).
- `e2e/web-ia-contract.test.mjs`: app.js matches `/beforeinstallprompt/` and `/sv-a2hs-dismissed/`;
  the timeout call site exists in `initReader`.
- Browser preview: desktop Chromium fires `beforeinstallprompt` only when installability criteria
  are met (SW + manifest on localhost usually qualify) — `preview_eval`
  `localStorage.removeItem("sv-a2hs-dismissed")`, reload, wait ~9s, `preview_snapshot` → card with
  "Instalar" visible. For the iOS body, `preview_eval` a forced call:
  `window.__svDebugShowInstallCard && window.__svDebugShowInstallCard("ios")` — OR simply
  temporarily hardcode the branch in the working copy, `cp` to dist, screenshot, revert (do not
  ship a debug hook unless you want one; if you do, gate it on `?selftest`).

**Dependencies.** Copy term from IANAV-10's decision (uses no song word — unaffected).
Sequencing with IANAV-07's hint (Step 3 anti-stacking rule). Help revival (FOLWEB-05) hosts the
durable copy. Ships in WS3-PR2.

---

#### FOLWEB-05 — All in-app help is unreachable on web — the only opener is a display:none stub — and the help content describes retired UI `medium` `web` `web-only`

**Problem.** `#help-panel` ("¿Cómo funciona?"), the app's only haptics setting, and the "Versión N"
label are all openable ONLY via `#help-button` — a hidden compat stub (`display:none`,
`aria-hidden`, `tabindex="-1"`) buried in the retired numpad panel. The click wiring works; it just
can't be reached. The content is also stale: it tells users to tap a drawer handle that is
`display:none !important` and to use a "↵ Ir" button that no longer exists.

**User impact at Mass.** A confused follower has zero self-service path — swipes, the drawer, the
live dot, the go-live bar, offline behavior: none of it is explained anywhere reachable. This
compounds IANAV-02/FOLWEB-03: the app's entire interaction model is undocumented in the app.

**Evidence (verify before editing).**
- `web/src/index.html:210` — `#help-button` is a hidden stub inside the retired numpad panel
  (`#numpad-section`, `:207-212`).
- `web/src/app.js:2662-2665` — the live click wiring (only opener of `#help-panel`; grep confirms
  no other `is-hidden` remover targets it).
- `web/src/index.html:278-348` — the panel; stale items: `:300` "…o toca la franja oscura a la
  izquierda" (handle hidden by `styles.css:2331` — true again after IANAV-02 option A); `:313-314`
  "Ir a una canción / Escribe el número con el teclado y toca ↵ Ir" (retired drawer numpad; real
  control is the ♪ modal); `:285-287` "Toca las flechas grandes de arriba" (the arrows live inside
  the drawer, not "arriba").
- `web/src/index.html:341` — `#haptic-toggle` (only haptics UI); `:347` — `#app-version-label`,
  populated at `app.js:3482-3484`; haptic wiring `app.js:2680-2689` (corrected — the toggle
  listener block is :2680-2689; the detail file cited :2681-2685).
- `web/src/styles.css:1715-1725` — **(placement constraint)** `.help-panel` is
  `position:absolute; inset:0; z-index:10` INSIDE `.navigation-drawer` — it can only be seen while
  the drawer is open, so the opener must live in the drawer (or the panel must be re-parented).

**Fix — step by step.**

Decision: **resurrect** (the finding's own recommendation; retiring instead would force FOLWEB-02/03
to build their own permanent surfaces). Do the deletion half via IANAV-09 in the same PR.

1. **Visible opener in the drawer top bar.** `web/src/index.html:157-161`, add a sibling after
   `#drawer-close`:
   ```html
   <button class="drawer-help-btn" id="help-open" type="button" aria-label="Cómo funciona">?</button>
   ```
   CSS (`styles.css`, next to `.drawer-close-btn` at `:468`): make the bar a flex row and style the
   button ≥44px:
   ```css
   .drawer-top-bar { flex-shrink: 0; display: flex; align-items: stretch; }
   .drawer-close-btn { flex: 1 1 auto; width: auto; }
   .drawer-help-btn {
     flex: 0 0 auto; min-width: 52px; padding: 0.82rem 1rem;
     background: #0d1030; border: 0; border-bottom: 1px solid rgba(255,255,255,0.12);
     border-left: 1px solid rgba(255,255,255,0.12);
     color: rgba(255,255,255,0.82); font-size: 1.25rem; font-weight: 800;
     touch-action: manipulation; -webkit-tap-highlight-color: transparent; cursor: pointer;
   }
   .drawer-help-btn:active { background: rgba(255,255,255,0.13); color: #fff; }
   ```
2. **Rewire the opener.** In `app.js`: add `const helpOpenButton = document.getElementById("help-open");`
   next to the other refs (`~:111`), and change the listener at `:2662-2665` to bind on
   `helpOpenButton`. (The old `helpButton` const + stub are deleted by IANAV-09 — same PR; if you
   split the PRs, bind BOTH temporarily and let IANAV-09 remove the stub binding.)
3. **Rewrite the stale content** (`index.html:281-331`), matching build-381 UI and IANAV-02's
   entry. Replace the seven `help-item`s with (term shown for the recommended "canto"; swap if the
   IANAV-10 decision goes the other way):
   1. icon `📖` — **"Sigue al coro en vivo"** / "No tienes que hacer nada: cuando el director cambia
      de página, tu página cambia sola. El punto verde arriba indica que vas en vivo."
   2. icon `♪` — **"Ir a un canto"** / "Toca ♪, escribe el número y toca “♪ Abrir Canto ♪”."
   3. icon `◁` — **"Lista y búsqueda"** / "Toca la pestaña › del borde izquierdo (o desliza desde el
      borde) para ver todos los cantos y buscar por título, letra o tema."
   4. icon `👆` — **"Pasar página"** / "Desliza la pantalla hacia los lados para pasar página."
   5. icon `↩` — **"Volver a en vivo"** / "Si te alejas de la página del coro, toca “↩ Volver a en
      vivo” abajo para regresar."
   6. icon `⟳` — **"Reconectar"** / "Si crees que no estás sincronizado, toca ⟳ arriba a la
      izquierda."
   7. icon `📶` — **"Sin internet"** / "Después de la primera visita, Signo Vivo funciona sin
      internet. En tu teléfono: toca Compartir ⬆︎ y “Agregar a pantalla de inicio”."
   Keep the "Ajustes" separator + Vibración row + `#app-version-label` exactly as-is.
4. **Old-iPad line (FOLWEB-02 tie-in).** Append an eighth item ONLY if you want the Auto-Lock
   protocol durable in-app (recommended): icon `🔆` — **"¿La pantalla se apaga?"** / "En Ajustes →
   Pantalla y brillo → Bloqueo automático elige “Nunca” para la Misa."
5. **Close behavior sanity.** `#help-close` (`app.js:2667-2670`) just re-hides the panel — still
   correct. Verify the panel scrolls (`overflow-y:auto`, `styles.css:1720`) with the new content on
   a 375px viewport.

**Acceptance criteria.**
- [ ] Help reachable in ≤2 taps from the base reader (open drawer → "?" button), both roles.
- [ ] Every claim in the panel matches a control that exists at this build (no "↵ Ir", no
      arrows-"arriba", the drawer line matches IANAV-02's shipped entry).
- [ ] Haptics toggle and "Versión N" label are visible and functional inside the panel.
- [ ] Panel closes via ✕ and never blocks the drawer's other controls when hidden.

**Tests.**
- `e2e/web-ia-contract.test.mjs`:
  - `index.html` contains `id="help-open"` NOT inside a `display:none` context, and NO LONGER
    matches `/id="help-button"/` (after IANAV-09 lands).
  - Content-staleness pins: `index.html` must NOT match `/↵ Ir/` and must NOT match
    `/franja oscura/` unless `styles.css` lacks the `.drawer-handle{display:none` rule (simplest:
    assert both the rule is gone AND the new copy strings exist, e.g. `/Volver a en vivo/` inside
    the help panel and `/Agregar a pantalla de inicio/`).
  - app.js binds a click listener on `help-open`.
- Browser preview: open drawer → `preview_click` `#help-open` → `preview_snapshot` shows
  "¿Cómo funciona?" with the seven rewritten items; `preview_inspect` `#app-version-label` has
  non-empty text (build badge token caveat: with the cp-loop the version label may be empty —
  verify on a real `node web/build.mjs` output); toggle Vibración and confirm `aria-pressed` flips
  (`preview_inspect` `#haptic-toggle`).

**Dependencies.** After IANAV-02 (item 3's wording documents the shipped entry). With IANAV-09
(deletes the stub this replaces — same PR recommended). Hosts durable copy for IANAV-07, FOLWEB-03,
FOLWEB-02. Term from IANAV-10's decision. Ships in WS3-PR2.

---

#### FOLWEB-07 — Numpad jump silently lands on the LAST page for unknown song numbers — and for ALL numbers while the song index hasn't hydrated (incl. permanent if the one background fetch fails) `medium` `web` `web-only`

**Problem.** `resolveSongPage` falls through to `state.totalPages` (page 371) when no song ≥ N
exists — and, because `songIndex` starts `[]` on web and hydrates from ONE background fetch with no
`response.ok` check and no retry, EVERY number resolves to page 371 before hydration or forever
after a single failed fetch. The mis-jump also sets `relay.browsing = true`, silently dropping a
live follower out of auto-follow as a bonus surprise.

**User impact at Mass.** "I typed the song number and it took me to the back of the book" — the one
control followers are taught, silently broken; with a live director it additionally pauses their
follow. One transient pages.json failure breaks primary navigation for the whole session.

**Evidence (verify before editing).**
- `web/src/app.js:700-708` — `resolveSongPage`: exact map hit, else first song ≥ N, else
  `state.totalPages` (`:708`). No caller distinguishes found from guessed.
- `web/src/app.js:149` — `songIndex: []` initial; `:1178` — the ≥5-digit code gate (so every ≤4
  digit entry is treated as a song); `:1189` — codes at least get "Código no válido"; song numbers
  get nothing.
- `web/src/app.js:1193-1196` — `goToDraftSong` renders whatever came back and records the TYPED
  number into Recientes; `:1200-1205` — sets `relay.browsing = true` when the (wrong) page differs
  from the live page.
- `web/src/app.js:3454-3458` — the single background hydrate:
  `.then((response) => response.json())` (no `response.ok` check), `.catch(console.warn)`, no
  retry, no re-attempt on `online`/`visibilitychange`.
- `web/build.mjs:697-699` — the built index.html inlines only `{ totalPages }` (`#pages-data`), so
  the song index ALWAYS takes the background-fetch path on web.
- `web/src/sw.js:226-236` — pages.json is served cache-first once cached (app.js caches it in
  `coreAssets`, `app.js:247-251`), so the failure window is mostly first-visit / pre-precache —
  exactly the "arrive at church, open the link" moment.

**Fix — step by step.**

> **DECISION-REQUIRED (Miguel) — behavior for in-range gaps** (the book numbers 2–371 with ~56
> missing numbers; e.g. typed 258, nearest existing song is 262):
> - **Option A (recommended): snap to the next song but SAY so** — flash
>   "El canto 258 no existe — abriendo el 262" (ok-style), then navigate. Preserves today's
>   genuinely useful nearest-hit while killing the silence.
> - **Option B (strict): exact-only** — any non-exact number flashes "No existe el canto 258" and
>   stays put (the merged IANAV-04 position).
> Numbers beyond the last song (e.g. 400) error and stay put under BOTH options.
> Recommendation: **A** — a mistyped-but-close number at Mass usually wants the neighboring song,
> and the flash teaches the numbering. Steps below implement A; for B, replace step 3's snap
> branch with the error branch.

1. **Extract the decision as a pure function** in `web/src/lib/svUiDecisions.js`:
   ```js
   // draftNumber: normalized positive int (<5 digits — codes are routed before this).
   // Returns { action: "empty-index" } | { action: "exact", page }
   //       | { action: "snap", page, snapSong } | { action: "missing" }
   function decideSongJump({ draftNumber, indexLength, exactPage, nextEntry }) {
     if (!indexLength) return { action: "empty-index" };
     if (Number.isFinite(exactPage)) return { action: "exact", page: exactPage };
     if (nextEntry) return { action: "snap", page: nextEntry.page, snapSong: nextEntry.song };
     return { action: "missing" };
   }
   ```
2. **Rewire `goToDraftSong`** (`app.js:1193-1196`). Replace the
   `const targetPage = findSongPage(songNumber); renderPage(targetPage); addToRecientes(songNumber); closeSongJump();`
   block with the following (define the tiny inline fallback right above it, mirroring the
   `applyRelaySnapshot` lib-missing pattern at `app.js:3118` — if the lib somehow fails to load,
   behavior degrades to today's, never to a throw):
   ```js
   // Same decision inline, for the lib-failed-to-load case (M2 fail-soft rule).
   const fallbackDecideSongJump = ({ draftNumber, indexLength, exactPage, nextEntry }) => {
     if (!indexLength) return { action: "empty-index" };
     if (Number.isFinite(exactPage)) return { action: "exact", page: exactPage };
     if (nextEntry) return { action: "snap", page: nextEntry.page, snapSong: nextEntry.song };
     return { action: "missing" };
   };
   const d = (globalThis.svUiDecisions?.decideSongJump ?? fallbackDecideSongJump)({
     draftNumber: songNumber,
     indexLength: state.songIndex.length,
     exactPage: state.songPageLookup.get(songNumber),
     nextEntry: state.songIndex.find((e) => e.song >= songNumber) || null,
   });
   if (d.action === "empty-index") {
     flashSongDisplay("Cargando el índice de cantos…", "err");
     kickSongIndexHydrate();          // step 4 — re-attempt now
     return;                          // modal stays open, no navigation, nothing recorded
   }
   if (d.action === "missing") {
     flashSongDisplay(`No existe el canto ${songNumber}`, "err");
     return;                          // modal stays open
   }
   if (d.action === "snap") flashSongDisplay(`El canto ${songNumber} no existe — abriendo el ${d.snapSong}`, "ok");
   renderPage(d.page);
   addToRecientes(getSongForPage(d.page) || songNumber);  // record the RESOLVED song (IANAV-08 tie-in)
   closeSongJump();
   ```
   Notes: `flashSongDisplay` (`app.js:831-841`) auto-reverts after 1.6s — for the two error cases
   consider passing a longer revert by bumping the timeout to 3000ms for `"err"` kind (one-line
   change at `:840`; the demographic reads slowly). The "snap" flash renders inside the modal that
   is about to close — either show it BEFORE `closeSongJump()` via a 900ms delayed close, or
   simpler: keep the modal open ~900ms (`setTimeout(closeSongJump, 900)`) so the message is seen.
   Keep the ≥5-digit code path (`:1178-1192`) byte-identical.
3. **Never fall through to `totalPages`.** Leave `resolveSongPage` (`:700-708`) in place for other
   callers (`findSongPage` is used by nothing else at HEAD — verify with grep; if so, delete both
   `resolveSongPage`'s fall-through caller `findSongPage` and inline the decision), but
   `goToDraftSong` no longer uses it. Simplest safe move: keep the functions, route the modal
   through `decideSongJump` only.
4. **Harden the hydrate.** Replace `app.js:3454-3458` with a named, retrying kick:
   ```js
   let songIndexHydrateInFlight = false;
   const kickSongIndexHydrate = (attempt = 0) => {
     if (state.songIndex.length || songIndexHydrateInFlight) return;
     songIndexHydrateInFlight = true;
     fetch(resolveAppPath(`/books/${BOOK_ID}/pages.json`), { cache: "no-store" })
       .then((response) => {
         if (!response.ok) throw new Error(`pages.json HTTP ${response.status}`);
         return response.json();
       })
       .then(hydrateSongIndex)
       .catch((error) => {
         console.warn("No se pudo cargar el índice de cantos", error);
         if (attempt < 3) setTimeout(() => { songIndexHydrateInFlight = false; kickSongIndexHydrate(attempt + 1); }, 2000 * (attempt + 1));
       })
       .finally(() => { songIndexHydrateInFlight = false; });
   };
   if (!manifest.songIndex) {
     kickSongIndexHydrate();
     window.addEventListener("online", () => kickSongIndexHydrate());
     document.addEventListener("visibilitychange", () => {
       if (document.visibilityState === "visible") kickSongIndexHydrate();
     });
   }
   ```
   (`hydrateSongIndex` is defined inside `initReader` — either hoist `kickSongIndexHydrate` inside
   `initReader` next to it, which is simplest, or lift both to module scope. Keep the
   listeners-registered-once shape shown above; the in-flight + `songIndex.length` guards make
   repeat kicks free.)
5. **Recientes hygiene** is handled by step 2's "record the RESOLVED song" (typed garbage never
   stored) — this closes the IANAV-08 pollution half.

**Acceptance criteria.**
- [ ] With `songIndex` empty (block pages.json in DevTools): typing 145 + Abrir → "Cargando el
      índice de cantos…", page does NOT change, modal stays open; unblocking + retry (or waiting
      for the online/backoff re-kick) makes the same entry work without a reload.
- [ ] Hydrated: typing 400 (past the last song) → "No existe el canto 400", no navigation, nothing
      recorded in Recientes.
- [ ] Hydrated: typing 258 (a gap) → per decision A: navigates to song 262's page with the visible
      "abriendo el 262" flash; Recientes records 262, not 258.
- [ ] Live-follow behavior unchanged for valid jumps (browsing flag + bar still set when jumping
      off the live page); erroring entries never touch `relay.browsing`.
- [ ] The ≥5-digit code path is byte-identical (contract test below).

**Tests.**
- `e2e/svUiDecisions.test.mjs`: `decideSongJump` table — empty index → `empty-index`; exact (262) →
  `exact`; gap (258 → next 262) → `snap` with `snapSong: 262`; past-max (400, `nextEntry: null`) →
  `missing`.
- `e2e/web-ia-contract.test.mjs`: app.js hydrate block matches `/response\.ok/` and
  `/kickSongIndexHydrate/` and registers `online` + `visibilitychange` re-kicks; app.js no longer
  calls `addToRecientes(songNumber)` with the raw typed number in `goToDraftSong` (assert the
  resolved-song form).
- Browser preview: use DevTools-equivalent blocking via `preview_eval` is not possible — instead
  test the empty-index path by serving `web/src` semantics: temporarily rename
  `web/dist/books/standard/pages.json` → `.bak`, reload, open ♪, type 145 → assert the flash text
  via `preview_inspect` `#song-display` and that `#page-image`'s `data-page` did not change;
  restore the file, wait for a re-kick (or reload), repeat 145 → navigates. Then type 400 →
  "No existe el canto 400".

**Dependencies.** IANAV-08 (shares the resolved-song Recientes rule — land FOLWEB-07 first or
together). IANAV-10 (the strings above already follow the recommended "canto"; swap if the decision
differs). No cross-workstream blockers. Ships in WS3-PR3.

---

#### IANAV-08 — 'Recientes' records only typed numbers — followed and browsed songs never appear (and nonexistent numbers do) `low` `web` `web-only`

**Problem.** `addToRecientes` has exactly two call sites: the numpad jump and the search/drawer
result tap — and the tap records only when the tapped page EXACTLY equals a song's start page, so
interior lyric-page search hits record nothing. Relay/mesh-followed pages, drawer prev/next, swipes
and arrow keys never record. Meanwhile typed nonexistent numbers ARE stored and silently skipped at
render. The empty-state copy promises "las canciones que hayas visitado recientemente" — which the
code doesn't deliver.

**User impact at Mass.** The tab's natural use — "what did we sing at Mass?" — yields an empty tab,
because live-followed songs never register.

**Evidence (verify before editing).**
- `web/src/app.js:411-418` — `addToRecientes`; its only callers are `:1195` (numpad jump, raw typed
  number) and `:2584` (result tap — the `addToRecientes` call is at `:2584`; the exact-start-page
  `find` gate is at `:2583`). (corrected — the record cited :2583 for the call; the call itself is
  :2584.)
- `web/src/app.js:2143-2158` — `renderSongItem` sets `data-page` to the song's START page
  (`:2147`), so drawer-tab taps DO record via the `:2583-2584` path; the miss is search hits on
  interior pages (`:1308` sets `data-page` to the matched interior page) and all
  follow/swipe/arrow navigation. (This scopes the title's "browsed songs never appear" claim, per
  the verifier's correction.)
- `web/src/app.js:2188-2202` — `renderTabRecientes`: `:2195` empty-state copy; `:2199-2201`
  silently skips stored numbers not in the index.
- Relay/mesh applies render via `renderPage(...)` with no recording: `:3162` (relay follow),
  `:977-979` (native sync event).

**Fix — step by step.**

1. **Dwell reducer (pure)** in `web/src/lib/svUiDecisions.js` (testable per the finding's test
   idea):
   ```js
   // Song-boundary dwell: record a song after the reader stays on it dwellMs.
   // state: { songNum, sinceMs } | null. Returns { state, record } where record is a song
   // number to add exactly once, or null.
   function songDwellStep(prev, { songNum, nowMs, dwellMs = 20000 }) {
     if (!songNum || songNum <= 0) return { state: null, record: null };
     if (!prev || prev.songNum !== songNum) return { state: { songNum, sinceMs: nowMs, recorded: false }, record: null };
     if (!prev.recorded && nowMs - prev.sinceMs >= dwellMs) {
       return { state: { ...prev, recorded: true }, record: songNum };
     }
     return { state: prev, record: null };
   }
   ```
2. **Drive it from the render commit path.** In `app.js`, module scope near the Recientes block
   (`~:404`): `let songDwell = null;`. In `renderPage`'s success path, after `renderStatus()`
   (`:1075`), add `noteSongDwell();` and define:
   ```js
   const noteSongDwell = () => {
     const step = globalThis.svUiDecisions?.songDwellStep;
     if (!step) return;
     const apply = () => {
       const r = step(songDwell, { songNum: getSongForPage(state.currentPage), nowMs: Date.now() });
       songDwell = r.state;
       if (r.record) addToRecientes(r.record);
     };
     apply();
   };
   // One 5s ticker makes the dwell fire even when the page stops changing:
   window.setInterval(noteSongDwell, 5000);
   ```
   (Register the interval once at module scope, not inside `renderPage`.) Page flips WITHIN a song
   don't reset the timer (`prev.songNum === songNum` keeps `sinceMs`); crossing a song boundary
   restarts it; 20s dwell filters page-flip noise while catching every hymn actually sung
   (hymns run minutes).
3. **Keep both explicit-record call sites** (`:1195` — now recording the RESOLVED song per
   FOLWEB-07 step 2 — and `:2584`): an explicitly chosen song deserves immediate recording; the
   dwell path double-add is harmless (`addToRecientes` de-dupes by moving the number to the front,
   `:413-414`).
4. **Stop storing garbage** — already done by FOLWEB-07 (no navigation for unknown numbers; typed
   numbers never recorded raw). Optionally purge legacy garbage lazily: in `getRecientes()` no
   change needed — `renderTabRecientes` already skips unknowns (`:2199-2201`).
5. **Fix the copy to be true** (with IANAV-10's term): `app.js:2195` →
   **"Aquí aparecerán los cantos que hayas visto o cantado recientemente."**
   ("canción" variant: "…las canciones que hayas visto o cantado recientemente.")

**Acceptance criteria.**
- [ ] Simulated live follow across 3 songs (apply relay snapshots or use
      `window.__signoVivoReceiveNativeEvent({type:"sync-event",event:{type:"page",page:N}})` in a
      bridged context / direct `renderPage` calls in preview) with >20s on each: Recientes lists
      those 3 songs, most recent first.
- [ ] Flipping quickly through 10 songs (<20s each) records none of them (noise filter).
- [ ] A numpad jump and a drawer tap still record immediately.
- [ ] No stored number absent from the song index after a session (typed garbage path is closed).

**Tests.**
- `e2e/svUiDecisions.test.mjs`: `songDwellStep` — same song at t=0 and t=19s → no record; t=21s →
  records once; t=25s → no duplicate; song change resets `sinceMs`; songNum 0 → null state.
- `e2e/web-ia-contract.test.mjs`: app.js calls `noteSongDwell` in `renderPage`'s success path and
  registers exactly one 5s interval; empty-state copy string updated.
- Browser preview: `preview_eval` `localStorage.removeItem("sv-recientes")`; navigate to song 10's
  page via ♪; wait 21s (`Monitor`/manual); `preview_eval`
  `JSON.parse(localStorage.getItem("sv-recientes"))` → contains the resolved song; open drawer →
  Recientes tab shows it.

**Dependencies.** FOLWEB-07 first or together (resolved-song recording + no-garbage rule).
IANAV-10 term for the copy line. Ships in WS3-PR3.

---

#### IANAV-09 — Confirmed: index panel (incl. Easter computus), retired Teclado panel + stale tip, and stub buttons are all unreachable dead UI at HEAD `low` `web` `web-only`

**Problem.** A whole strongly-connected dead component ships in every bundle: the index panel
(6 renderers + sort tabs + liturgical-calendar computus, ~500 lines) is reachable only from chips
that the panel itself renders — a closed cycle with no live entry; the retired Teclado drawer panel
(with a tip telling users to tap a button that no longer exists) can never display; four hidden
stub buttons and a dead `#search-cancel` remain; and the `nc-sort-prefs` localStorage key persists
preferences that only the dead panel reads (and, at HEAD, only the dead panel could ever write).

**User impact at Mass.** None today — that's the point. It bloats every bundle (web AND the copy
inside every native build), keeps live-but-unfireable listeners, and each future edit risks waking
dead code (the audit itself kept tripping over it).

**Evidence (verify before editing).**
- `web/src/app.js:108` — `searchIndexButton = getElementById("search-index")` → null (`#search-index`
  has zero matches in index.html); its listener is null-guarded (`:2643-2651`).
- `web/src/app.js:1881-1914` — `renderIndexPanel`; only other entries: `drawerBack` click
  (`:2491-2494`) and the `searchClearButton` drill-down branch (`:2559-2561`) — both gated on state
  set exclusively at `:1918-1919` inside `activateSearchFromIndex`, which fires only from chips the
  index panel itself renders (`:2627-2640`). Closed cycle.
- `web/src/app.js:1485-1553` — `computeEaster` + `getLiturgicalSeason`, called only from
  `renderIndexThemesContent` (`:1570`); index renderers span `:1557-1878`.
- `web/src/styles.css:2328` — `.drawer-mode-switch { display: none; }`; `web/src/app.js:1116` —
  `openDrawer` forces browse mode (corrected — the `switchDrawerMode("browse")` call is exactly
  :1116), so `#numpad-panel` (`index.html:202-219`) can never display; its tip (`:214-217`) still
  says "toca ↵ Ir".
- `web/src/app.js:133-142` — `nc-sort-prefs` plumbing; the ONLY `saveSortPrefs()` call is `:2599`,
  inside the sort-tab click branch whose buttons render only inside the dead index panel — so at
  HEAD the key is never written either (new precision beyond the record).
- Stubs: `index.html:208-211` (`#fullscreen-button`, `#prev-corner`, `#help-button`,
  `#display-clear`), `index.html:275` (`#search-cancel`, listener optional-chained at
  `:2568-2572`).
- **Null-crash landmines (MUST handle — unguarded references in LIVE functions):**
  - `renderDraft` (`app.js:821-825`) references `displayClearButton` unconditionally (`:823`).
  - `renderStatus` (`:775-819`) references `prevCornerButton` unconditionally (`:783-784`,
    `:817-818`).
  - `updateFullscreenButton` (`:1128-1138`) references `fullscreenButton` unconditionally
    (`:1132`, `:1136`).
  - `switchDrawerMode` (`:2315-2324`) references `modeBtnNumpad`/`modeBtnBrowse` unconditionally
    (`:2319-2322`) and is called from LIVE paths (`openDrawer` `:1116`, result tap `:2586`).
  - `bindReaderEvents` binds unconditionally on `helpButton` (`:2662`), `tipDismissButton`
    (`:2673`), `fullscreenButton` (`:2692`), `prevCornerButton` (`:2448`), `modeBtnNumpad/Browse`
    (`:2460-2467`).
  - `clearSearch`/`renderIndexPanel`/`activateSearchFromIndex`/`handleSearchInput` reference
    `drawerBack` unconditionally (`:1884`, `:1919`, `:1932`).
  A missed one throws during `bindReaderEvents()`/boot → the boot guard's recovery card on every
  parish device. Delete references and elements TOGETHER, then run the smoke assertions.

**Fix — step by step.**

Ordered deletion checklist (do it in one commit so the tree never half-references):

1. **Index-panel subsystem (app.js):** delete `renderIndexPanel` (`:1881-1914`),
   `activateSearchFromIndex` (`:1916-1927`), `renderIndexTabContent` (`:1868-1878`) + the six
   `renderIndex*Content` renderers and their helpers used by nothing else — `makeSongButton`
   (`:1433-1450`), `renderSortTabs` (`:1453-1473`), `INDEX_TABS` (`:1475-1482`), `computeEaster` +
   `getLiturgicalSeason` (`:1485-1553`), `computeSongKeys`/`computeSongLengths` (`:1411-1430`),
   `STOPWORDS`/`getKeywordFreq`/`getTopKeywords`/`makeKeywordChip` (`:1749-1788`),
   `COMPLEXITY_LABELS` (`:1836-1840`), and the module caches `cachedSongKeys`/`cachedSongLengths`/
   `cachedKeywords` (`:191-193`). KEEP `SOLFEGE_MAP` + `renderTabTono` (`:2257-2286` — live) and
   `searchByTheme`/`renderThemeResults` (`:1349-1406` — live via `handleSearchInput`).
2. **Click-handler branches (app.js:2591-2640):** delete the sort-tab, index-tab, theme-chip and
   keyword-chip branches from the `searchResults` click handler (keep the `.search-result-item`
   branch). Delete the `searchIndexButton` listener (`:2643-2651`) and its const (`:108`).
3. **Drill-down state:** delete `state.indexVisible`/`state.indexTab`/`state.indexDrillDown`/
   `state.indexSortPrefs` (`:156`, `:163-171`), `PREFS_KEY`/`loadSortPrefs`/`saveSortPrefs`/
   `savedPrefs` (`:133-142`). Simplify: `searchClearButton` handler (`:2557-2565`) → always
   `clearSearch()`; `clearSearch` (`:1929-1937`) loses the `indexVisible`/`drawerBack`/
   `searchIndexButton` lines; `handleSearchInput` (`:1939-1944`) loses the index reset block;
   the `searchInput` blur handler (`:2501-2507`) drops `!state.indexVisible && !state.indexDrillDown`.
   Delete the `drawerBack` listener (`:2491-2494`), the `drawerBack` const (`:91`), the
   `#drawer-back` markup (`index.html:171-174`), and every `drawerBack.` reference (in
   `renderIndexPanel`/`activateSearchFromIndex` — already deleted — and `clearSearch` `:1932`).
   Add a one-time migration nicety (optional): `try { localStorage.removeItem("nc-sort-prefs"); } catch {}`.
4. **Teclado panel:** delete `#numpad-panel` (`index.html:202-219`) INCLUDING the stubs inside it
   and the tip; delete `.drawer-mode-switch` markup (`index.html:186-197`) + CSS (`styles.css:2328`
   and the `as-dropdown` reference at `:1045`); in app.js delete `modeBtnNumpad`/`modeBtnBrowse`
   consts (`:124-125`) + listeners (`:2460-2467`), `numpadTipWrap`/`tipDismissButton` consts
   (`:116-117`) + listener (`:2673-2677`) + the boot-time TIP block (`:392-401`, key `sv-tip`), and
   simplify `switchDrawerMode` (`:2315-2324`) to:
   ```js
   const switchDrawerMode = (mode) => {
     state.drawerMode = mode;
     navigationDrawer.classList.toggle("mode-browse", mode === "browse");
     if (mode === "browse") renderActiveTab();
   };
   ```
   (Or inline `renderActiveTab()` at the two call sites and delete the function + `state.drawerMode`;
   keep whichever diff is smaller.)
5. **Stub buttons + their live references:**
   - `#display-clear`: delete stub (in `:211`) + const (`:95`) + the `renderDraft` line (`:823`)
     — the modal has no clear button; the draft display just shows/clears text.
   - `#prev-corner`: delete stub (`:209`) + const (`:104`) + listener (`:2448-2451`) + the four
     `prevCornerButton` lines in `renderStatus` (`:783-784`, `:817-818`). KEEP `goBackInHistory` +
     `state.pageHistory` (history plumbing is used by `renderPage`; harmless, and a future "volver"
     control may want it) — or flag for a later pass; do NOT delete in this PR.
   - `#fullscreen-button`: delete stub (`:208`) + const (`:102`) + listener (`:2692-2697`) + the
     two lines in `updateFullscreenButton` (`:1132`, `:1136`) — the visible `#fullscreen-fab`
     branch remains.
   - `#help-button`: delete stub (`:210`) + const (`:111`) + rebind to FOLWEB-05's `#help-open`
     (same PR).
   - `#search-cancel`: delete stub (`index.html:275`) + optional-chained listener (`:2568-2572`) +
     const (`:119`).
6. **CSS sweep:** delete now-orphaned selectors — `.drawer-numpad-panel`/`#numpad-panel` block,
   `.numpad-tip*`, `.drawer-mode-*`, `.index-*` families (`index-layout`, `index-sidebar`,
   `index-tab-btn`, `index-sort-tab`, `index-theme-chip`, `index-keyword-chip`,
   `index-group-header`, `index-themes-grid`, `index-chip-*`, `index-content`), and
   `search-focused`/`index-visible` combos that referenced them (grep each selector before
   deleting; `index-group-header` is used ONLY by index renderers — verified — but re-grep after
   the app.js deletions).
7. **Smoke pass (non-negotiable):** boot the preview and exercise: open/close drawer, every rail
   tab, search with results + clear, ♪ jump (valid/invalid/code), help open/close, haptic toggle,
   fullscreen fab, swipes. Then run the contract test (below). The boot-guard recovery card
   appearing = you missed a landmine.

**Acceptance criteria.**
- [ ] `grep -c "renderIndexPanel\|computeEaster\|indexDrillDown\|nc-sort-prefs\|drawer-mode-switch\|numpad-panel"` over `web/src` returns 0.
- [ ] No unconditional reference to a deleted element remains (boot with a clean console; no
      recovery card).
- [ ] All LIVE surfaces still work: drawer rails, search (incl. ⇅ sort), theme search, ♪ modal,
      help (per FOLWEB-05), fullscreen fab, Recientes.
- [ ] Bundle shrinks (sanity: `wc -l web/src/app.js` drops by roughly 450-550 lines;
      index.html/styles.css shrink correspondingly).
- [ ] `nc-sort-prefs` and `sv-tip` are no longer read or written.

**Tests.**
- `e2e/web-ia-contract.test.mjs` — the dead-UI section:
  - `web/src/app.js` does NOT match any of: `renderIndexPanel`, `computeEaster`,
    `getLiturgicalSeason`, `indexDrillDown`, `nc-sort-prefs`, `sv-tip`.
  - `web/src/index.html` does NOT match: `id="numpad-panel"`, `id="help-button"`,
    `id="search-cancel"`, `id="prev-corner"`, `id="display-clear"`, `id="fullscreen-button"`,
    `drawer-mode-switch`, `↵ Ir`.
  - Landmine guard (the class of bug, not just instances): for every `getElementById("<id>")` in
    app.js, `index.html` must contain `id="<id>"` — write this as a loop extracting ids via regex;
    it catches ANY future stub deletion that leaves a dangling reference. (Allowlist ids created at
    runtime: `sv-live-pill`, `sv-golive-bar`, `sv-relay-warn`, `sv-crash-banner`, plus the new
    `sv-hello`/hint ids.)
- Browser preview: full smoke pass of step 7 with `preview_console_logs` level=error clean, and
  `preview_eval` `window.__SV_LAST_ERROR` → undefined after exercising every surface.

**Dependencies.** With/after FOLWEB-05 (help opener re-homed before the stub dies). After IANAV-12
if you keep them in different PRs (IANAV-12 replaces the `#search-sort-toggle` that lives in the
same header region — trivial either way, just avoid conflicting edits). Aligned with the P8
dead-code batch (other workstream): this component is ADDITIVE to P8's list — do not wait for P8.
Ships in WS3-PR2.

---

#### IANAV-10 — Spanish copy: missing accents on install surfaces, canto/canción inconsistency, 'relé' jargon in the director banner `low` `web` `web-only`

**Problem.** The OS-install-visible manifest description is missing four accents; the page image's
screen-reader alt is missing one; the app uses two words for the same concept ("canto" in the jump
modal universe, "canción" in the drawer/help); and the highest-stakes director banner leads with
"relé" — electrician's Spanish a volunteer director won't parse. Plus a vague "Tiempo" rail label
and a dead-end "Sin resultados." empty state.

**User impact at Mass.** Inconsistent terminology raises cognitive load for elderly users; missing
accents look unpolished exactly where the OS shows them; the banner's key noun fails the one person
who must act on it mid-Mass.

**Evidence (verify before editing).**
- `web/src/manifest.webmanifest:4` — `"Manual de coro Signo Vivo — navegacion rapida por numero de
  cancion."` (navegacion/rapida/numero/cancion — four missing accents).
- `web/src/index.html:54` — `alt="Pagina actual del manual"` (missing accent; screen-reader
  surface).
- Canto universe: `index.html:91` (`aria-label="Ir a canto"`), `:111` (modal aria), `:114`
  ("IR A CANTO"), `:122` ("Número de canto"), `:142` ("♪ Abrir Canto ♪").
  Canción universe: `index.html:165-181` ("Canción anterior/siguiente" + labels), `:175`
  ("Canción 0" status), `:227` ("Explorar canciones"), `:265` (column header "Canción"), `:313`
  ("Ir a una canción"); `app.js:793` (`Canción ${…}`), `:2154` (`Canción ${song.song}` fallback),
  `:2195` (Recientes empty state), `:2274` ("canción/es" in Tono headers), `:2183` + `:1373`
  ("canciones" empty states).
- `web/src/app.js:912` — "El relé rechazó el código de director. Los seguidores en signovivo.com NO
  están sincronizados."
- `web/src/index.html:231` — rail tab 📅 "Tiempo" (`data-tab="temporada"`), no aria-label.
- `web/src/app.js:1291` — "Sin resultados." (no guidance).

**Fix — step by step.**

> **DECISION-REQUIRED (Miguel) — canonical song term.**
> - **Option A (recommended): "canto"** — the Mexican-parish liturgical register (the choir "canta
>   los cantos de la Misa"), already the term of the primary control (IR A CANTO) and of the aria
>   labels; changing the drawer/help side is the smaller visible diff (the modal is what everyone
>   sees mid-Mass).
> - **Option B: "canción"** — more generic everyday Spanish; requires re-copying the modal
>   ("IR A CANCIÓN", "♪ Abrir Canción ♪") that native-parity screenshots and muscle memory already
>   use.
> Recommendation: **A ("canto")**. Coordinate with major-update §9 open decision #7 (Spanish copy
> for prompts/banners) so wording lands once. All steps below assume A.

1. **Accents (mechanical, zero risk):**
   - `manifest.webmanifest:4` → `"description": "Manual de coro Signo Vivo — navegación rápida por número de canto."`
   - `index.html:54` → `alt="Página actual del manual"`.
2. **Terminology sweep to "canto"** (es-MX, gender flips included). index.html:
   - `:165-167` → `aria-label="Canto anterior"`, label `Canto<br>Anterior`
   - `:179-181` → `aria-label="Canto siguiente"`, label `Siguiente<br>Canto`
   - `:175` → `Canto 0` (initial text; overwritten at boot)
   - `:227` → `aria-label="Explorar cantos"`
   - `:256` placeholder stays ("Título, letra o tema…" — no song word)
   - `:265` → `<span class="search-col-title">Canto</span>`
   - `:313` (or its FOLWEB-05 replacement) → "Ir a un canto"
   app.js:
   - `:779` → `` `Página ${state.currentPage}` `` (unchanged), `:793` → `` `Canto ${getCurrentSongNumber()}` ``
   - `:2154` → `` `Canto ${song.song}` ``
   - `:2183` → "No hay cantos con temas de misa asignados aún."
   - `:1373` → "Sin cantos etiquetados."
   - `:2195` → per IANAV-08 step 5 ("…los cantos…")
   - `:2274` → `` `— ${byKey[k].length} canto${byKey[k].length !== 1 ? "s" : ""}` `` and `:2282` →
     "…cantos"
   (If IANAV-09 already deleted `makeSongButton`/index renderers, their "Canción" fallbacks are
   gone — do this pass AFTER the deletion to avoid dead edits.)
3. **Director banner rewrite** (`app.js:912`). This string is shared with FAILUX-09 (other
   workstream: append the recovery action + re-show behavior). AUTHORITATIVE: FAILUX-09's Wave-2 string wins — if Wave 2 already landed, re-pin its string in the IANAV-10 test and skip this rewrite. Land ONE combined string here and
   tell that workstream it's done, or vice versa — never two edits:
   ```js
   msg.textContent = "No se pudo conectar con signovivo.com: el código de director fue rechazado. "
     + "Los teléfonos NO están siguiendo. Sal del modo director (toca DIRECTOR ✕ Salir) y vuelve a "
     + "entrar con un código vigente.";
   ```
   (Wire compat: pure string change; the `relay-auth-error` payload and latch logic are untouched.)
4. **"Tiempo" rail tab** (`index.html:231`): add `aria-label="Tiempo litúrgico"` to the button and
   keep the visible label "Tiempo" (smallest change; the group headers inside already say
   Adviento/Navidad/etc., which disambiguates). Optional visible rename to "Época" only if Miguel
   prefers — note it in the PR, don't block on it.
5. **Search empty state** (`app.js:1291`):
   ```js
   p.textContent = "Sin resultados. Prueba con menos palabras, o busca el número del canto con ♪.";
   ```
6. **Propagation note.** `CACHE_VERSION` hashes app.js/sw.js/styles.css/index.html/manifest
   (`web/build.mjs:10-28`), so every one of these edits busts the SW cache on deploy — returning
   PWAs pick the strings up on their next load-after-update. The OS-level manifest description on
   EXISTING installs refreshes only on Chromium's periodic manifest re-check (days) — fine.

**Acceptance criteria.**
- [ ] `grep -rn "navegacion\|numero de cancion\|Pagina actual" web/src/` → 0 hits.
- [ ] `grep -rn "Canción\|canción\|canciones" web/src/index.html web/src/app.js` returns ONLY
      hits inside comments (or zero) — one term everywhere user-visible.
- [ ] `grep -n "relé" web/src/app.js` → 0 hits; the banner names signovivo.com, states the
      consequence, and gives the recovery action.
- [ ] The 📅 tab exposes "Tiempo litúrgico" to screen readers.
- [ ] "Sin resultados." includes the guidance sentence.

**Tests.**
- `e2e/web-ia-contract.test.mjs` — copy section: assert the exact new manifest description string;
  assert `!/relé/.test(appJs)`; assert `!/Pagina actual/.test(indexHtml)`; assert
  `!/Canción/.test(indexHtml)` (term A) — write the assertions against whichever decision Miguel
  makes, and pin the full banner string verbatim so future edits are deliberate.
- Browser preview: `preview_snapshot` of the drawer (nav buttons read "Canto Anterior / Siguiente
  Canto"), the search header ("Canto" column), and the Tono tab headers; `preview_eval`
  `window.__signoVivoReceiveNativeEvent({type:"relay-auth-error",status:401})` → banner text shows
  the new copy (works in plain web too — the handler is un-gated).

**Dependencies.** Decision A/B FIRST (gates FOLWEB-05/IANAV-07/IANAV-08/FOLWEB-07 copy). Banner
string coordinates with FAILUX-09 (cross-workstream — single combined edit). Run AFTER IANAV-09's
deletion to avoid editing dead strings. Ships in WS3-PR3.

---

#### IANAV-12 — Search sort toggle: blind 3-state cycle with state-vs-action ambiguity, and it's the one sort preference NOT persisted `low` `web` `web-only`

**Problem.** The search-results sort control is a single button cycling "⇅ Mejor → ⇅ Nº → ⇅ A–Z"
whose label shows the CURRENT mode — users can't tell whether tapping applies the label or leaves
it. And persistence is inverted: the dead index panel's sort prefs have a localStorage key while
the LIVE `searchSortMode` resets to "best" every session.

**User impact at Mass.** A user who prefers number-order must blind-cycle the toggle every session;
"Mejor" (relevance) is abstract for the demographic.

**Evidence (verify before editing).**
- `web/src/app.js:2545` — `SEARCH_SORT_LABELS` constant; the cycle handler is `:2547-2554` with the
  label set at `:2552` (corrected — the record's :2545 cite is the labels constant; the logic is
  :2547-2554).
- `web/src/app.js:172` — `searchSortMode: "best"` hard init; no localStorage read/write anywhere
  for it (only the click handler writes state, `:2551`).
- `web/src/app.js:138-140` — `saveSortPrefs` persists only `indexSortPrefs` (`nc-sort-prefs`),
  which only the dead index panel reads (IANAV-09 deletes it).
- `web/src/index.html:266` — the single `#search-sort-toggle` button ("⇅ Mejor").
- Verified non-issue: theme-result sets DO honor the mode (`app.js:1379-1383`); list results sort
  pre-cap (`:1266-1283`).

**Fix — step by step.**

1. **Markup — segmented control.** Replace `web/src/index.html:266` with:
   ```html
   <div class="search-sort-seg" id="search-sort-seg" role="group" aria-label="Orden de resultados">
     <button type="button" data-sort="best" class="is-active" aria-pressed="true">Mejor</button>
     <button type="button" data-sort="num" aria-pressed="false">Nº</button>
     <button type="button" data-sort="az" aria-pressed="false">A–Z</button>
   </div>
   ```
2. **CSS** (styles.css, near the old `.search-sort-toggle` rules — grep `search-sort-toggle`,
   replace that block):
   ```css
   .search-sort-seg { display: flex; gap: 2px; border-radius: 10px; overflow: hidden;
     border: 1px solid rgba(255,255,255,0.18); }
   .search-sort-seg button { padding: 0.5rem 0.7rem; min-width: 3.2rem; border: 0;
     background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.75);
     font-size: 0.85rem; font-weight: 700; cursor: pointer;
     touch-action: manipulation; -webkit-tap-highlight-color: transparent; }
   .search-sort-seg button.is-active { background: rgba(90,150,255,0.35); color: #fff; }
   ```
3. **JS.** Delete the old block (`app.js:2544-2555`) and the `searchSortToggle` const (`:110`).
   Add:
   ```js
   const SEARCH_SORT_KEY = "sv-search-sort";
   const searchSortSeg = document.getElementById("search-sort-seg");
   const syncSearchSortSeg = () => {
     if (!searchSortSeg) return;
     searchSortSeg.querySelectorAll("button[data-sort]").forEach((b) => {
       const active = b.dataset.sort === (state.searchSortMode || "best");
       b.classList.toggle("is-active", active);
       b.setAttribute("aria-pressed", String(active));
     });
   };
   if (searchSortSeg) {
     searchSortSeg.addEventListener("click", (event) => {
       const btn = event.target.closest("button[data-sort]");
       if (!btn) return;
       haptic();
       state.searchSortMode = btn.dataset.sort;
       try { localStorage.setItem(SEARCH_SORT_KEY, state.searchSortMode); } catch {}
       syncSearchSortSeg();
       handleSearchInput();   // re-run the current query with the new sort
     });
   }
   ```
   And restore at boot — change the state init (`:172`) to:
   ```js
   searchSortMode: (() => {   // persisted; guarded (storage-disabled browsers boot fine)
     try { const v = localStorage.getItem("sv-search-sort"); return v === "num" || v === "az" ? v : "best"; }
     catch { return "best"; }
   })(),
   ```
   Call `syncSearchSortSeg()` once inside `bindReaderEvents` (after the listener) so the restored
   mode is highlighted on first open.
4. **Do not** migrate anything from `nc-sort-prefs` (dead panel's key; IANAV-09 removes it).

**Acceptance criteria.**
- [ ] The three modes render as separate buttons with the ACTIVE one visually distinct
      (`aria-pressed="true"`).
- [ ] Tapping "Nº" re-sorts current results by song number instantly; reload → "Nº" is still
      active and applied (persisted).
- [ ] Theme-result lists honor the selected mode (regression: `app.js:1379-1383` path).
- [ ] Storage-disabled browser: boots, control works session-only.

**Tests.**
- `e2e/web-ia-contract.test.mjs`: app.js matches `/sv-search-sort/` in BOTH a `getItem` (state
  init) and a `setItem` (click) context; index.html contains `search-sort-seg` with three
  `data-sort` buttons; app.js no longer matches `/SEARCH_SORT_LABELS/`.
- Browser preview: open drawer → Buscar → type "santo" → `preview_click`
  `#search-sort-seg button[data-sort="num"]` → `preview_snapshot`: results ordered by number and
  the Nº button highlighted; reload; reopen search → `preview_inspect`
  `#search-sort-seg button[data-sort="num"]` still `.is-active`.

**Dependencies.** Coordinate line-space with IANAV-09 (same `#search-col-header` region; land in
the same PR3 or rebase trivially). None otherwise. Ships in WS3-PR3.

---

#### FOLWEB-11 — Manifest display_override ['fullscreen', …] hides the status bar (clock/battery) on Android installs `low` `web` `web-only`

**Problem.** `display_override: ["fullscreen", "standalone"]` — Chromium honors the override list
first, so Android installs run true fullscreen with no system status bar; iOS ignores
`display_override`, making the regression Android-only. There is no benefit over standalone for a
static page viewer.

**User impact at Mass.** Congregants' personal Android phones lose the clock/battery/notification
edge during Mass, and fullscreen exit is non-obvious for elderly users.

**Evidence (verify before editing).**
- `web/src/manifest.webmanifest:5` — `"display_override": ["fullscreen", "standalone"],` with
  `"display": "standalone"` at `:6`.

**Fix — step by step.**

1. `web/src/manifest.webmanifest:5` → `"display_override": ["standalone"],`
   (equivalently delete the key — `display: "standalone"` at `:6` already covers intent; keeping
   the single-entry list is the minimal diff).
2. Propagation note for the PR description: fresh installs pick it up immediately; EXISTING Android
   installs update on Chromium's periodic manifest re-check (typically within a few days of an
   app launch). The manifest is hashed into `CACHE_VERSION` (`web/build.mjs:22`), so the SW-served
   copy busts on deploy.

**Acceptance criteria.**
- [ ] `manifest.webmanifest` contains no `"fullscreen"` token.
- [ ] A fresh Android/Chromium install (or DevTools → Application → Manifest) reports effective
      display mode `standalone`; the status bar is visible in the installed app.
- [ ] iOS standalone behavior unchanged (it never read the key).

**Tests.**
- `e2e/web-ia-contract.test.mjs`: read `web/src/manifest.webmanifest`, `JSON.parse`, assert
  `!manifest.display_override || !manifest.display_override.includes("fullscreen")` and
  `manifest.display === "standalone"`.
- Browser preview: `preview_eval`
  `fetch("manifest.webmanifest").then(r=>r.json()).then(m=>m.display_override)` → `["standalone"]`.
  (True install-mode verification needs a physical Android or Chrome's install flow — note it in
  the PR; the manifest assertion is the shippable check.)

**Dependencies.** None. Ships in WS3-PR3 (one line).

---

#### PARITY-10 — Long-press on the score image pops iOS's Copy/Save context menu on web/PWA while the native shell stays quiet — no touch-callout suppression on #page-image `low` `web` `web-only`

**Problem.** `#page-image` has no `-webkit-touch-callout`, `-webkit-user-drag`, or `user-select`
rules; the only ancestor guard is an UNPREFIXED `user-select: none` on `.viewer-shell`, which
Safari ignores and which wouldn't suppress the iOS image callout anyway. So a long-press on the
score in Safari/PWA opens the system sheet (Copiar / Guardar en Fotos / Compartir…) over the music,
and iPad touch-drag can lift the image as a drag item — while the native WKWebView is configured
quiet, so the same gesture behaves differently per context.

**User impact at Mass.** Elderly users who press-and-hold the page mid-hymn get a system sheet
covering the music on personal phones and the old-iPad PWA; it also lets anyone trivially save the
copyrighted scans from the public site.

**Evidence (verify before editing).**
- `web/src/styles.css:211-222` — the `#page-image` rule: no touch-callout / user-drag /
  user-select. Repo-wide grep of `web/src` confirms zero occurrences of `touch-callout` or
  `user-drag` anywhere.
- `web/src/styles.css:134-150` — the `user-select: none` at `:146` belongs to the `.viewer-shell`
  rule (starts `:134`), not `body`, and is unprefixed (corrected per verifier — Safari needs
  `-webkit-user-select`).
- `web/src/index.html:54` — the `<img>` has no `draggable` attribute.
- `web/src/index.html:5` — viewport keeps `user-scalable=yes, maximum-scale=5` (pinch-zoom for
  elderly readers must survive this fix).
- `PdfReaderApp.tsx:1099-1105` — native side already quiet: `allowsLinkPreview={false}` (`:1099`),
  `dataDetectorTypes="none"` (`:1100`), `textInteractionEnabled={false}` (`:1105`).

**Fix — step by step.**

1. **CSS.** In the `#page-image` rule (`web/src/styles.css:211-222`) add:
   ```css
   -webkit-touch-callout: none;   /* iOS long-press Copy/Save sheet */
   -webkit-user-select: none;     /* Safari needs the prefix */
   user-select: none;
   -webkit-user-drag: none;       /* iPad drag-lift of the image */
   ```
   (All four are supported back to iOS 15 — the old-iPad floor.)
2. **Belt for the shell.** In `.viewer-shell` (`:134-150`) add `-webkit-user-select: none;` next to
   the existing unprefixed `user-select: none` at `:146`.
3. **Markup.** `web/src/index.html:54`: add `draggable="false"` to the `#page-image` tag.
4. **Do NOT touch** the viewport meta (`index.html:5`) — pinch-zoom stays.

**Acceptance criteria.**
- [ ] On iOS Safari / home-screen PWA (device day): a 1-2s press-and-hold on the score produces
      nothing — no callout sheet, no drag ghost.
- [ ] Pinch-zoom still works (viewport untouched); swipes and the edge gesture behave exactly as
      before (touch-callout does not affect touch events).
- [ ] Native shell behavior unchanged (its quiet config is independent; the bundled web copy simply
      gains the same CSS at the next native build).

**Tests.**
- `e2e/web-ia-contract.test.mjs`: the `#page-image` CSS block matches
  `/-webkit-touch-callout:\s*none/` and `/-webkit-user-drag:\s*none/`; `index.html:54`'s img tag
  matches `/draggable="false"/`; viewport meta still matches `/user-scalable=yes/`.
- Browser preview: `preview_inspect` `#page-image` with
  `styles: ["-webkit-user-select", "user-select"]` → `none`; long-press cannot be simulated in
  desktop Chromium — the affirmative check is the device-day item plus the CSS assertions above.

**Dependencies.** None. Ships in WS3-PR3. (Copyright posture bonus noted in
`project_songbook_copyright_triage`: the public site currently serves ~92%-copyrighted scans;
suppressing casual save is a mitigation, not a fix — do not oversell it in the PR.)

---

## 3. Cross-reference summary

| Land order | Item | Blocking dependencies |
|---|---|---|
| 0 | Decisions: IANAV-02 entry (rec: handle), IANAV-10 term (rec: canto), FOLWEB-07 gap behavior (rec: snap+announce), FOLWEB-02 video fallback (rec: skip) | Miguel |
| 1 | WS3-PR1: IANAV-02 (+ browse-intent helper) | DIRNAT-05 coordination (design-compatible, not land-blocked); known browse-flag finding landed once |
| 1 (parallel) | WS3-PR3: IANAV-10, FOLWEB-07, IANAV-08, IANAV-12, FOLWEB-11, PARITY-10 | Term decision; FOLWEB-07 before/with IANAV-08 |
| 2 | WS3-PR2: FOLWEB-05, IANAV-07, FOLWEB-03, FOLWEB-02, IANAV-09 (+ worker one-liner for wakeLock) | PR1's entry choice (help copy); FOLWEB-05 before/with IANAV-09; FAILUX-09 string coordination |

Everything reaches signovivo.com (phones + old-iPad PWA) on the next Pages deploy; parish native
iPads inherit at the next native build/mesh bundle push — no native rebuild is REQUIRED by anything
in this workstream, and no bridge or relay message shapes change.


---

## Workstream 4 — The web-director emergency path

> **Scope.** Three confirmed findings from the 2026-07 IA audit: **ROLEWEB-02** (the feature: no
> web-director path exists — the native iPad is a single point of failure for the relay
> congregation), **ROLEWEB-01** (a VALID director code typed on signovivo.com flashes "Código no
> válido" — a misleading dead end in the exact emergency where a director reaches for the web), and
> **ROLEWEB-07** (`html[data-role]` never set on pure web + a false code comment).
>
> **Repo:** `(repo root)` — HEAD
> `d5075091`, build 381. Every `file:line` below was **re-verified against source at this HEAD** by
> the chapter author (drift notes in §5). Re-verify again before editing — this repo moves fast.
>
> **⛔ THE APPROVAL GATE — read before implementing anything in §3.**
> ROLEWEB-02 is **part feature**. It creates a second publisher surface for the live-Mass relay
> room from any phone browser. That is a product decision with real blast radius (two-publisher
> ping-pong, a new way to move the congregation's pages, a new thing a leaked code can do).
> **The ROLEWEB-02 feature ships ONLY behind Miguel's explicit approval** of the
> **DECISION-REQUIRED (Miguel)** forks in §3.1 — do not write a line of §3 code before he has
> answered them. The two fixes in §2 (ROLEWEB-01 copy fix, ROLEWEB-07 one-liner) are ordinary
> confirmed-bug fixes and need **no** product approval; ship them on the normal path.
>
> **Standing rules for the executor (from the audit brief + repo memory):**
> - **NEVER run `npm run test:e2e` and NEVER run `e2e/relay-sync.test.mjs`** — that test publishes
>   to the PRODUCTION relay room `alvernia-main` and flips live followers' pages. CI's safe
>   allowlist is `.github/workflows/ci.yml:58-70`; stay inside it.
> - **Additive-only wire compat.** Native builds 368–381 are in the field. Never remove/retype
>   `page`/`totalPages`/`seq`, never bump relay `v` or mesh `protocolVersion`
>   (docs/major-update-2026-07.md §5). Everything in this chapter is additive by construction.
> - All user-facing copy is **es-MX**; final wording is Miguel's call (major-update §9.7). Copy in
>   quotes below is the proposal.

---

## §1. Shared ground truth (verified at HEAD d5075091)

The facts every fix in this chapter builds on. `app.js` = `web/src/app.js`; `index.ts` =
`sync-worker/src/index.ts`; `PdfReaderApp.tsx` is at the repo root.

**The worker already authorizes code-bearing browsers.**
- `POST /r/:room/publish` accepts EITHER `Authorization: Bearer <RELAY_DIRECTOR_TOKEN>` OR an
  `X-Director-Code` header whose digits-only value is in the `TRANSMITTER_CODES` secret
  (index.ts:773-818; code extraction+strip at :782, `codeOk` at :785, 401 at :786-788;
  fail-closed set builder at :387-396 — no plaintext codes in the repo).
- CORS is `ALLOWED_ORIGINS: "*"` (sync-worker/wrangler.jsonc:20) and `X-Director-Code` is
  explicitly in `Access-Control-Allow-Headers` — the comment at index.ts:370-372 even names
  "signovivo.com" as an anticipated publisher. **A browser fetch can publish today with zero
  worker changes.**
- Publish gates (index.ts:131-185): per-IP token bucket 15 burst / 2 per s → `{ok:true,
  rateLimited:true}` → HTTP 429 (:138-140, :816). Seq sanitizer: non-finite / negative /
  `> Date.now()+60000` collapses to 0 (:146-149). Staleness: `seq===0 || nowSec - ts > 90`
  (:156-158, `RELAY_LIVE_MAX_AGE_S = 90` at :31-34). While FRESH: `seq=0` ignored (:165-167),
  `seq <= current` ignored (:168-170) — both return HTTP 200 `{ok:true, seq, ignored:true}`.
  While STALE: anything wins (takeover/reset). Accepted publish: `seq = incoming>0 ? incoming :
  current+1`, `ts` = SERVER seconds (:171-179), stored before broadcast (:180-183).
- `GET /r/:room/state` → snapshot + additive `now` (server epoch s) (:748-763). Never 500s to the
  browser; the outer catch returns 200 `EMPTY_SNAPSHOT` + CORS on any throw (:821-826).

**The native publisher this must mirror** (`src/directorRelaySync.js`, publish-only, 129 lines):
- Payload `{v:1, page≥1, totalPages≥0, mode, bookId:"standard", seq, ts}` (:111-123).
- `seq = Math.max(seqCounter+1, Date.now())` — wall-clock-ms scale, monotonic across restarts
  (:56-60). `ts = Math.floor(Date.now()/1000)` (:122).
- Latest-wins coalescing: one request in flight, newest pending replaces (:124-128, drain
  :100-108); 7s AbortController ceiling (:50, :67-71).
- 401/403 fires a one-shot latched `relayAuthErrorHandler` (:87-96); latch re-arms on a successful
  publish (:88) or a fresh `setRelayPublishCode` (:36-41). Network throws are swallowed (:97-99).
- Room is HARDCODED `alvernia-main` (:13) — the native transmitter has no staging entry (an M7
  item). The web side resolves its room properly (below).

**How the web resolves its relay room** (Release Safety M1, SHIPPED):
- `web/src/lib/svRelayRoom.js:28-36` — `?env=staging` → `alvernia-staging`, anything else / any
  error → `alvernia-main`. Two-room whitelist (:26).
- app.js consumes it once at module eval into the `RELAY_ROOM` const with a triple guard
  (app.js:2846-2857); `RELAY_BASE` at :2837-2840; `RELAY_LIVE_MAX_AGE_S = 90` mirror at :2858.
- `clearInitialUrl()` (app.js:669-678) deliberately KEEPS `?env=staging` / `?selftest` in the URL
  so a mid-canary reload can't silently fall back to the live Mass room.
- ⚠️ RELVER-01 (separate finding, already confirmed): `signovivo.com?env=staging` serves the PROD
  bundle — the param switches only the ROOM. Canary CONTENT lives only at the Pages preview URL
  from `STAGING=1 bash scripts/release.sh` (release.sh:33, :125; its usage text at :18 repeats the
  misconception — the manual script in §3.8 uses the preview URL).

**Native's director-entry confirm semantics — the parity bar** (PdfReaderApp.tsx:563-624,
`onDirectorCode`):
- Empty → `role:"none"` (:572-575). `744668486` → soft reset (:576-579). Unknown code →
  `role:"none"` + the FALSE comment "tell the web so it surfaces «código incorrecto»" (:584-588;
  no such web UI exists — that is confirmed finding DIRNAT-01, owned by another workstream).
- Valid code → **ALWAYS a confirm Alert, never a silent promotion** (:589-621; Miguel 2026-07-02
  "always ask, always"). Three variants: "⚠️ Ya hay un director activo" when a mesh director
  snapshot arrived within `LIVE_DIRECTOR_WINDOW_MS = 8000` ms (:598-607, const at :71) with
  destructive button "Tomar el control" and body "…tú tomas el control y todos te seguirán a ti"
  (:609); "Super admin — ¿dirigir?" for `SUPER_ADMIN_CODES` (:605-606); else "¿Dirigir el coro?"
  with body "…Si otro director ya está activo, le quitarás el control." (:612). Cancel = stay
  exactly as-is, no generation bump, no event (:614-615, NEW-DIR-2 comment :565-570).
- Codes are NEVER persisted (only the role string; :504-506, :465-472).
- Step-down clears the relay code so a draining coalesced publish 401s instead of shoving a stale
  page (C3, :426-430).

**Web role UI that already exists (dormant on pure web):**
- `renderDirectorModeBadge()` (app.js:845-852) writes `document.documentElement.dataset.role =
  isDirector ? "director" : "follower"` (:849) and toggles the `#director-mode-badge` button
  (index.html:55-57, boots `is-hidden`). Its ONLY caller is the native bridge `role` handler
  (app.js:947-956, call at :952) — never fires on pure web (ROLEWEB-07).
- Role CSS (styles.css:2149-2156): `.search-fab{display:none}` base (:2154);
  `html[data-role="director"] .search-fab{display:flex}` (:2155);
  `html[data-role="director"] .resync-fab{display:none}` (:2156); ♪ shifts left for the director's
  ⌕ (:2123). The ⌕ handler (app.js:2411) opens the drawer's Buscar tab — no role checks in its JS.
- Badge tap-to-exit (app.js:2426-2431): `window.confirm("¿Salir del modo director?\n\nDejarás de
  dirigir y volverás a seguidor.")` → `postNativeBridge({type:"exit-director"})` — a silent no-op
  on pure web today (`postNativeBridge` returns false without a bridge, :309-321).
- `showRelayAuthWarning(status)` (app.js:887-925): the red assertive banner "El relé rechazó el
  código de director. Los seguidores en signovivo.com NO están sincronizados." (:912), dismissible
  (:918), idempotent re-reveal (:888-891). Today triggered only via the native bridge
  `relay-auth-error` (:962-965) — but it is IN the web bundle, ready to reuse.

**Web follower machinery relevant to "stop following while directing":**
- `relay` state object (app.js:2993-3009; `hasDirector:false` init :3006; `manualClose` :2995).
- `startRelayFollow()` (:3328-3378) — `visibilitychange` (:3330-3343) and `online` (:3344-3356)
  handlers call `relayPollOnce(true)` / `connectRelay()` unconditionally; F3 10s health floor
  (:3363-3375) is a no-op when `relay.manualClose` (:3365) but `connectRelay()` RESETS
  `manualClose = false` (:3227). The follower pill renders only when `relay.hasDirector`
  (:3036-3042); go-live bar gated the same (:3068). `reconnectRelay()` web branch (:3097-3106)
  is the proven "resume following" sequence.
- The numpad: draft capped at 10 digits (:1141-1147, cap :1144); `goToDraftSong` (:1172-1206)
  routes ≥5-digit entries — native branch posts `{type:"director-code"}` (:1183-1186), pure-web
  else-branch flashes "Código no válido" (:1187-1190, flash at :1189, `flashSongDisplay`
  :831-841). ⚠️ nuance: `normalizeSongDraftNumber` (:691-698) runs the draft through `Number()`,
  so LEADING ZEROS are stripped before the length check — irrelevant for the real 10-digit NANP
  phone-number codes (never start with 0), but do not "fix" it in passing; native receives the
  same normalized string today and changing it would alter the native contract.
- Fleet check-in (web): `fleetCheckin(extra)` (:2951-2983) POSTs `{deviceId, surface:"web",
  webCached, pagesCached, totalPages, homeScreen, cacheVersion, ...extra}` (:2962-2971) — no
  label/role since #270 removed the self-ID picker; the worker's `checkin()` still accepts and
  sanitizes `label`/`role` ≤60 chars if present (index.ts:266-267).

**Two-publisher reality (the honest physics — from map-worker C2/C4 + findings-synce2e):**
Both the native transmitter and any web publisher use wall-clock-ms seqs. While the room is fresh,
higher seq wins and equal/lower returns `{ignored:true}` — which **no shipped client reads**
(directorRelaySync.js:87 checks only `res.ok`; known-open P2-IDENTITY/M4). So two live publishers
**ping-pong**: each later publish outbids the earlier, followers flip between the two pages, and
NEITHER publisher gets any signal (both see HTTP 200). There is no transmitterId until M4. A
slow-clock publisher is `ignored` until the other's snapshot ages out (≤90s); a >60s-fast clock
trips the A2 sanitizer freeze (SYNCE2E-01 — applies equally to a web phone). Design consequence:
**Phase 1 cannot arbitrate two live directors; it can only warn, be warned, and step down.** The
durable fix is M4's transmitterId + tiebreak.

---

## §2. The two fixes (ship on the normal path — no product approval needed)

Ship order: ROLEWEB-07 first (one line; ROLEWEB-02 later leans on `data-role` being trustworthy),
then ROLEWEB-01. Both are pure web-bundle changes; a Pages deploy reaches phones instantly and
parish iPads only at their next native build/bundle push — where both changes are inert (native
takes the bridge branch / native fires the role event anyway).

#### ROLEWEB-01 — A VALID director code typed on signovivo.com flashes "Código no válido" — misleading dead end in the forgot-iPad emergency `medium` `web` `web-only`

**Problem.** On pure web (no native bridge), ANY 5+-digit numpad entry falls into an else-branch
that flashes **"Código no válido"** for 1.6s and reverts. The code is never checked against
anything — the parish's real director code and random garbage get the same *credential verdict*,
when the truth is a *surface limitation* ("this device cannot direct"). The code's own comments
admit it (app.js:827-829 "nothing to unlock on web", :1180-1182 "a long code is meaningless"),
but the user-facing string blames the code.

**User impact at Mass.** The only realistic reason someone types a director code into
signovivo.com is the disaster drill: the parish iPad is dead/lost/left at home and the volunteer
director tries their code on a phone browser, minutes before Mass. The app tells them their
(correct) code is wrong → they retype → "invalid" again → they conclude they misremember it and
call Miguel or give up. Nothing anywhere on the web hints that directing requires the iPad app.
(Honest framing per the verifier: the web cannot direct today regardless, so this fix changes no
capability — it converts a lying dead end into an honest, actionable one. It is also the natural
entry point for §3's feature if approved.)

**Evidence (verify before editing).** All re-verified at HEAD d5075091:
- `web/src/app.js:1178` — the ≥5-digit length gate in `goToDraftSong` (function spans
  :1172-1206). *(corrected: the original finding cited :1178 as the fall-through; the pure-web
  else-branch is :1187-1190 and the flash call is :1189.)*
- `web/src/app.js:1187-1190` — pure-web else: `clearDraft(); flashSongDisplay("Código no válido",
  "err");` — numpad stays open.
- `web/src/app.js:831-841` — `flashSongDisplay`: 1.6s red flash on the numpad display, auto-revert
  (`is-err` styling + shake: styles.css ~2224-2241).
- `web/src/app.js:827-829` and `:1180-1182` — comments admitting the truth the copy doesn't say.
- `web/src/app.js:1183-1186` — native branch (bridge present): posts `{type:"director-code",
  code}` then `clearDraft()+closeSongJump()`. DO NOT touch (that path's zero-feedback problem is
  DIRNAT-01, another workstream).
- `web/src/app.js:1141-1147` — the draft cap is 10 digits, so "code-shaped" ⇔ exactly 10 digits.
- Reuse pattern for the new sheet: `showRelayAuthWarning` (app.js:887-925) — injected
  style+element, `is-on` class, idempotent, dismissible.

**Fix — step by step.**
1. **Add a small dismissible explainer sheet** in `web/src/app.js`, right after
   `showRelayAuthWarning` (after :925), following its exact injected-element pattern (module-level
   `let codeExplainEl = null;` + `const showCodeExplainSheet = () => {...}`):
   - Element id `#sv-code-sheet`, `role="alertdialog"`, `aria-live="polite"`, fixed,
     centered-top, z-index 210 (above the song-jump modal's 200 so it reads even if the modal is
     open), max-width `min(92vw, 30rem)`, dark card styling consistent with the numpad card.
   - Title (bold): **"Este código se usa en el iPad de la parroquia"**
   - Body: **"El sitio web solo puede seguir al director — no puede dirigir. Para dirigir el coro,
     abre la app Signo Vivo en el iPad de la parroquia y entra tu código ahí."**
   - One button: **"Entendido"** → removes `is-on`. Also dismiss on backdrop tap if you add one;
     keep it simple otherwise.
   - Idempotent re-reveal like :888-891. All copy es-MX; final wording Miguel (§9.7).
   - *(If §3 is approved, this same sheet later gains the emergency second action — build it as a
     two-slot layout now: message block + button row.)*
2. **Route code-shaped entries to the sheet** in `goToDraftSong`'s else-branch. Replace
   :1187-1190's body with:
   - `const digits = String(songNumber);` (already computed as `code` in the outer scope at
     :1179 — reuse it).
   - If `code.length >= 10` (i.e. exactly 10 under the cap): `clearDraft(); closeSongJump();
     showCodeExplainSheet();` — close the modal so the sheet is unobstructed.
   - Else (5–9 digits — not code-shaped, a typo): keep today's `clearDraft();
     flashSongDisplay("Código no válido", "err");` verbatim.
3. **Do not touch** the native branch (:1183-1186), the ≤4-digit song path (:1193-1205), or
   `flashSongDisplay` itself.
4. **Wire-compat statement:** pure client change in the shared bundle. Native builds 368–381 carry
   their own baked bundle and take the bridge branch anyway — when this bundle eventually reaches
   an iPad (next native build or mesh push), behavior there is unchanged. No relay/bridge message
   is added, removed, or altered.

**Acceptance criteria.**
- Pure web (no bridge): typing a real 10-digit director code → sheet appears with copy that (a)
  does NOT claim the code is invalid, (b) names the iPad app as the directing surface; numpad
  modal closed; "Entendido" dismisses; re-entry re-shows.
- Pure web: 5–9-digit entries still get the 1.6s "Código no válido" flash with the numpad open;
  ≤4-digit entries still resolve as songs (unchanged).
- Native shell (bridge present): 10-digit entry still posts `{type:"director-code"}` and closes
  the modal — byte-identical behavior.
- No new console errors; the sheet renders above the song-jump modal if both are ever visible.

**Tests.** (Never `npm run test:e2e`; never `e2e/relay-sync.test.mjs`.)
- **Unit (node, CI-safe):** extract the routing decision into the new pure lib this workstream
  adds anyway (see §3 step 1): `svWebDirector.decideCodeEntry({digits, hasBridge,
  nativeFileMode, enabled})` → `"song" | "bridge-code" | "flash-invalid" |
  "explain-ipad" | ...`. New file `e2e/svWebDirector.test.mjs` (node:test, pattern of
  `e2e/svRelayRoom.test.mjs`) pinning: `{len:10, web} → "explain-ipad"`, `{len:6, web} →
  "flash-invalid"`, `{len:10, bridge} → "bridge-code"`, `{len:3} → "song"`, junk inputs never
  throw. Add the file to the CI allowlist (`.github/workflows/ci.yml:58-70`).
  *(If ROLEWEB-01 ships before §3 is approved, ship the lib with only `decideCodeEntry` in it —
  it is the ROLEWEB-01 router; §3 extends the same file.)*
- **Manual:** open the local build (`node web/build.mjs`, serve `web/dist`) in a plain browser →
  walk the three entry classes above.
- `scripts/smoke-boot.mjs` stays green (it inspects the built bundle; run it locally).

**Dependencies.** None. Standalone; ship now. Interlocks: DIRNAT-01 (native zero-feedback — the
"Verificando…" flow on the bridge path) touches the same numpad surface — coordinate copy but do
not block on it. If ROLEWEB-02 is approved, this sheet is its entry point (§3 step 4).

---

#### ROLEWEB-07 — `html[data-role]` is never set on pure web and the code comment claims otherwise `low` `web` `web-only`

**Problem.** `renderDirectorModeBadge()` writes `document.documentElement.dataset.role` (:849) and
its comment says *"Default 'follower' so signovivo.com is right from boot"* (:847-848) — false.
The function's only call site is the native bridge `role` event handler (:952), which can never
fire on pure web, and `index.html:2` (`<html lang="es">`) carries no static attribute. On all of
signovivo.com the attribute is absent forever; the follower layout survives only because follower
is the unselected CSS base (director rules keyed at styles.css:2123, :2155, :2156).

**User impact at Mass.** None today — this is the classic works-on-iPad / broken-on-web trap: any
future `html[data-role="follower"]` CSS rule (the natural reading given the comment) or JS
attribute read would work in the native shell and silently break on the entire public web surface.
That skew class caused this app's worst outages. §3's feature also promotes `data-role` to a
load-bearing role signal on pure web, so it must become ground truth first.

**Evidence (verify before editing).** All re-verified at HEAD d5075091:
- `web/src/app.js:845-852` — `renderDirectorModeBadge`; the false comment at :847-848; the
  `dataset.role` write at :849. *(corrected vs original finding: the write is :849, not :847.)*
- `web/src/app.js:947-956` — the native `role` handler, sole caller (:952).
- `web/src/index.html:2` — no static `data-role` on `<html>`.
- `web/src/styles.css:2154` — `.search-fab{display:none}` is the unselected follower BASE rule;
  the director-keyed selectors are :2123, :2155, :2156. *(corrected: :2154 itself is the base
  rule, not a director selector.)*
- `web/src/app.js:179` — `state.nativeSyncRole` initializes to `"off"`, so calling the renderer at
  boot yields `"follower"` and keeps the badge hidden (`is-hidden` boots on the element,
  index.html:55; `classList.toggle("is-hidden", true)` is idempotent).

**Fix — step by step.**
1. In `initReader()` (app.js:3380), immediately after the `state.currentPage = DEFAULT_START_PAGE;`
   line (:3411 — i.e. once state is settled, before any render), add:
   ```js
   // Boot ground truth: html[data-role] is "follower" on EVERY surface from first paint.
   // The native shell re-asserts the real role over the bridge (role event → this same
   // renderer); pure web stays follower forever unless a web-director mode ever activates.
   renderDirectorModeBadge();
   ```
   Calling the existing renderer (rather than a raw `dataset.role = "follower"`) keeps ONE writer
   for the attribute and is automatically correct on native too (native boots follower and
   re-asserts its role at bridge-ready — PdfReaderApp.tsx:663-669).
2. Rewrite the now-true comment at :847-848 to: `// Drive the control layout: followers (web + any
   non-director native) get ⟳ + ♪; a director gets ♪ + ⌕. Set once at boot (initReader) and on
   every native role event — html[data-role] is reliable ground truth on every surface.`
3. Wire-compat: pure client, no messages touched; native 368–381 unaffected (their baked bundles
   don't change; when the bundle reaches them the boot value is "follower", which is what native
   boots as anyway).

**Acceptance criteria.**
- Pure web boot: `document.documentElement.getAttribute("data-role") === "follower"` before the
  reader reveals (initReader runs pre-reveal); the ⌕ fab stays hidden; ⟳ visible.
- Native shell: attribute flips to `"director"` only on a native `role:"director"` event
  (unchanged); back to `"follower"` on `follower`/`none`.
- The stale comment is gone.

**Tests.**
- **Source-contract (node, CI-safe):** in `e2e/svWebDirector.test.mjs` (or a tiny standalone
  `e2e/web-boot-role-contract.test.mjs`), read `web/src/app.js` and assert the `initReader` body
  (slice the source between `const initReader = async () => {` and `// Resolve the build`) matches
  `/renderDirectorModeBadge\(\)/` — pins the boot call so a refactor can't silently drop it.
  (The repo already uses source-contract pins where no DOM harness exists; `scripts/smoke-boot.mjs`
  is a static artifact inspector, no jsdom — extending it with the same regex against
  `dist/app.js` is an acceptable alternative.)
- **Manual:** local build in a plain browser → dev tools →
  `document.documentElement.dataset.role` === `"follower"` at load.

**Dependencies.** None. Ship first in this workstream — §3 requires it (its role UI keys off
`data-role`, and its guards read the same state).

---

## §3. ROLEWEB-02 — the web-director emergency path (feature design + Phase 1 implementation)

#### ROLEWEB-02 — No web-director path exists: the native iPad is a single point of failure for the relay congregation, though the worker already authorizes code-bearing browsers `medium` `cross` `multi`

**Problem.** There is no legitimate way to lead from the web. The web bundle contains zero publish
code (grep `publish` in `web/src/` → comments only); a valid director code entered on
signovivo.com is discarded (ROLEWEB-01). Yet the worker authorizes `X-Director-Code` from ANY
client (§1) — the server half of a web director already exists; only the client UI is absent. No
milestone in docs/major-update-2026-07.md plans one (M0–M7 checked; M4 at :147 plans the safety
prerequisite — transmitterId + two-publisher tiebreak — not a UI).

**User impact at Mass.** If the director's iPad fails mid-Mass (battery, crash, forgotten,
stolen), every relay follower — congregants' personal phones and the old web-PWA iPad — stops
receiving pages. Honest scope per the verifier: (a) followers do NOT freeze forever; staleness
demotion (#248, live) releases them to manual navigation after ≤90s — the loss is auto-follow, not
page access; (b) this is NOT the only recovery: any other parish iPad running the native app
accepts the director's memorable code today (always-follower boot + numpad promotion) and recovers
BOTH mesh and relay followers. The web-director mode is the resilience path for the
**no-native-device-reachable** case (director's iPad dead AND no other iPad at hand — e.g. a
practice night, a sub-choir Mass, a hardware fleet failure), and it recovers **relay followers
only**: mesh-only native iPads (no wifi in church) are physically out of reach of any web
director, and the design below says so to the user, honestly, at every step.

**Evidence (verify before editing).** All re-verified at HEAD d5075091:
- `sync-worker/src/index.ts:773-818` — /publish auth: `X-Director-Code` from any client
  (extraction :782, `codeOk` :785, 401 :786-788). *(corrected: original cited :777 — that's the
  comment above the block.)*
- `sync-worker/src/index.ts:370-372` — `X-Director-Code` CORS-allowlisted; comment names
  signovivo.com as a publisher.
- `sync-worker/wrangler.jsonc:20` — `ALLOWED_ORIGINS: "*"`.
- `web/src/app.js:1187-1190` — a valid long code on pure web is discarded with the error flash
  (:1189). *(corrected: original cited :1183 — that's the native branch.)*
- `docs/major-update-2026-07.md:147` — M4 plans transmitterId + tiebreak; no web-director UI
  anywhere in the doc.
- Coexistence physics: `src/directorRelaySync.js:56-60` (wall-clock seq) +
  `sync-worker/src/index.ts:156-179` (fresh/stale gates) + `directorRelaySync.js:87` (`ignored`
  never read) — see §1 "Two-publisher reality".

---

### §3.1 Product forks — every one is **DECISION-REQUIRED (Miguel)**

This feature ships ONLY after Miguel answers these. Record answers in the PR description.

**D1 — Should a web-director path exist at all?**
- *Option A — Build Phase 1 (emergency-only), land it with/after M4.* One more way Mass survives a
  dead iPad; the worker half already exists; web-only ship vector = instant rollback.
- *Option B — Don't build.* Rely on the existing fallback (any parish iPad + the memorable code).
  Cheapest; leaves the no-iPad-reachable case uncovered and ROLEWEB-01's honest copy as the end of
  the story.
- *Option C — Build, but hold behind a dark launch flag until M4's tiebreak is live.*
- **Recommendation: A with C's timing** — approve the design now; land the code inside the M4
  slice (shared transmitterId work, shared staging verification), keep it emergency-framed.
  Rationale: the 2026-07-01 outage class ("no director all night") is exactly one device failure
  away from recurring; this is the only fallback that works when zero native devices are usable.

**D2 — Who may direct from the web?**
- *Option A — Any code in `TRANSMITTER_CODES`* (i.e. any standard director code). Parity with
  native and with what the worker can enforce today; zero worker change.
- *Option B — A separate `WEB_DIRECTOR_CODES` secret* (worker change: second set union-checked at
  index.ts:783-785). Tighter, but a new secret to manage and a new failure mode ("my code works on
  the iPad but not on the web") during an emergency.
- **Recommendation: A.** The confirm sheet + emergency framing gate intent; the credential already
  gates capability. Note the A1 residual (major-update §10): the legacy committed code
  `12345678840` may still be in `TRANSMITTER_CODES` — a web UI makes any leaked code strictly more
  convenient to abuse, so **rotating that code out (hardening-plan Q3) should precede launch**.
- Related worker fact: the server cannot distinguish super-admin from standard codes (one flat
  secret), so the web sheet cannot offer a "super admin" label like native does (:605-606). Phase
  1 shows one generic confirm for all codes.

**D3 — Entry affordance at launch?**
- *Option A — Visible second action on the ROLEWEB-01 sheet* (10-digit code on pure web → sheet
  offers "explicar" + "dirigir en emergencia"). Discoverable in the exact moment of need — an
  emergency feature behind a secret handshake fails precisely when needed.
- *Option B — Same, but only when `?dir=1` / a localStorage flag is set* (dark launch soak).
- *Option C — Hidden long-press.* (Rejected: undiscoverable under stress, untrainable.)
- **Recommendation: A at launch, B during the staging soak** — ship one deploy with the button
  gated behind `?dir=1`, run the §3.8 script, then flip the const and redeploy. The flag is also
  the permanent kill-switch (§3.6).

**D4 — Does entering web-director mode warn about / demote a live native director?**
- Physics (§1): a web director CANNOT demote a native director (mesh is untouched; the native
  transmitter keeps publishing 200-ok and gets no signal pre-M4). Two live publishers ping-pong.
- *Option A — Warn-and-allow (mirror native's takeover confirm):* the entry sheet detects a FRESH
  room snapshot and shows the destructive-styled "Ya hay un director transmitiendo" variant;
  while directing, a lightweight /state poll detects being outbid and shows a step-down banner.
- *Option B — Hard-block entry while a fresh director exists.* Safer against ping-pong, but the
  freshness window is 90s: a just-died iPad looks "live" for up to 90s — blocking the emergency
  exactly when it's needed. (The heartbeat is 12s, so a live director is never stale; but a DEAD
  one stays fresh ≤90s.)
- **Recommendation: A** — warn, require the destructive confirm, never block. It mirrors native's
  own semantics (:598-621: warn on live, allow takeover). Post-M4, upgrade detection to
  transmitterId (§3.7).

**D5 — Resume after a reload/crash while web-directing?**
- Native persists a role breadcrumb and shows the "Estabas dirigiendo" prompt on boot; codes are
  never persisted (PdfReaderApp.tsx:465-472, :869-878).
- *Option A — No resume at all (Phase 1):* a reload boots follower; the director re-enters their
  code (≤15s on the taught numpad). In-memory state only; zero persistence.
- *Option B — sessionStorage breadcrumb + boot prompt* (web NEW-DIR-1 parity).
- **Recommendation: A for Phase 1** (smallest honest surface; the 12s-heartbeat gap before
  staleness gives ~90s to re-enter), B listed in Phase 2. Either way the CODE is never persisted.

**D6 — Copy.** All es-MX strings in §3.4/§3.5 need Miguel's confirmation (major-update §9.7).

---

### §3.2 Phase 1 — goals, non-goals, honest limits

**Goal.** A director whose iPad died opens signovivo.com on ANY phone, enters their 10-digit code
on the ♪ numpad, confirms deliberately, and within ~30s is driving every RELAY follower (phones +
web-PWA iPad) with visible director state, visible failure states, and a taught way back out.

**Non-goals (Phase 1).** No mesh reach (impossible from a browser); no arbitration of two live
directors (M4); no admin/super-admin distinction (D2); no drawer redesign (the director ⌕ already
works); no native code changes of any kind; no worker changes of any kind.

**Honest limits — stated to the user in-product, not buried here:**
1. **Mesh-only native iPads will NOT follow a web director.** The confirm sheet and the live chip
   both say so: "Los iPads del coro (sin internet) NO te seguirán — solo los teléfonos y tabletas
   en signovivo.com."
2. **Two live directors fight** (ping-pong) until M4; the web side warns on entry (fresh room) and
   while directing (outbid detection) — the native side stays blind pre-M4.
3. **A locked/backgrounded phone stops directing** (JS timers freeze → heartbeat stops → followers
   demote to manual after ≤90s — the honest failure mode, identical to a dead native director).
   Mitigations: the existing screen wake lock (app.js:3500-3516) + explicit copy + a foreground
   re-publish (§3.4 step 9).

---

### §3.3 Phase 1 — wire contract (all pre-existing; nothing added, nothing changed)

Endpoints (base `RELAY_BASE` app.js:2837-2840; room = the already-resolved `RELAY_ROOM` const
app.js:2846-2857 — NOT a new literal, so `?env=staging` and future practice-mode routing work for
free):

- **`POST {RELAY_BASE}/r/{RELAY_ROOM}/publish`**
  Headers: `Content-Type: application/json`, `X-Director-Code: <10 digits>` (worker strips
  non-digits :782; the numpad only produces digits).
  Body — byte-parity with `directorRelaySync.js:111-123`:
  ```json
  { "v": 1, "page": <int ≥1>, "totalPages": <int ≥0>, "mode": "standard",
    "bookId": "standard", "seq": <Math.max(prev+1, Date.now())>,
    "ts": <Math.floor(Date.now()/1000)> }
  ```
  Responses: `200 {ok:true, seq}` accepted · `200 {ok:true, seq, ignored:true}` fresh-room
  seq-gate (NOT an error; see §3.4 step 6) · `401 {ok:false,error:"unauthorized"}` bad/retired
  code · `429 {ok:false,error:"rate_limited"}` token bucket (15 burst / 2 per s per IP — 25/4 once SYNCE2E-09 lands,
  index.ts:138) · `400/413/500` per index.ts:789-815.
- **`GET {RELAY_BASE}/r/{RELAY_ROOM}/state`** → snapshot + `now` (index.ts:748-763) — used for
  the pre-entry freshness warning and the while-directing conflict poll.
- **Post-M4 additive rider:** add `"transmitterId": fleetDeviceId()` to the publish body. Safe to
  send EARLY: the current worker's `publish()` builds the stored snapshot from known fields only
  (index.ts:171-179), so an unknown input field is dropped harmlessly — but don't bother until the
  M4 worker stores/echoes it. Never remove/retype existing fields; never bump `v`
  (major-update §5).

**Additive-only compat with native builds 368–381:** this feature adds ZERO wire messages and ZERO
bridge messages. The only shared-bundle risk is the module running inside a FUTURE native shell
that embeds this bundle — prevented structurally: every entry point is gated on
`!hasNativeBridge() && !NATIVE_FILE_MODE` (the same gate the follower relay uses, app.js:3329), so
inside any native shell the web-director module is dead code and native's own
transmitter/director machinery remains the sole publisher.

---

### §3.4 Phase 1 — implementation, step by step

All web-bundle. New pure lib + `app.js` wiring. Verify every anchor before editing.

**Step 1 — New pure lib `web/src/lib/svWebDirector.js`** (UMD, ES5, dependency-free, never-throw —
copy the skeleton of `web/src/lib/svRelayRoom.js`). Exports (all pure; unit-tested in node):
- `decideCodeEntry({digits, hasBridge, nativeFileMode, enabled})` →
  `"song" | "bridge-code" | "flash-invalid" | "explain-ipad" | "offer-web-director"`
  (≤4 → song; ≥5 with bridge/file-mode → bridge-code; 5–9 pure web → flash-invalid; 10 pure web →
  `enabled ? "offer-web-director" : "explain-ipad"`). This is also ROLEWEB-01's router.
- `nextSeq(prev, nowMs)` → `Math.max((prev||0)+1, nowMs)` — mirror of directorRelaySync.js:56-60.
- `publishPayload({page, totalPages, seq, nowMs, transmitterId})` → the §3.3 body (transmitterId
  included only when non-empty; `mode`/`bookId` pinned `"standard"`).
- `roomIsFreshDirector({seq, ts, now}, nowMs, clockOffsetMs, maxAgeS)` → boolean — same freshness
  rule as `svSyncDecision` (seq>0 AND (ts absent OR skew-adjusted age ≤ maxAgeS)); used for the
  entry warning. Prefer the snapshot's own `now` for skew, falling back to `clockOffsetMs`.
- `outbid({stateSeq, myLastSeq})` → `stateSeq > myLastSeq` — the pre-M4 conflict heuristic:
  our seqs are wall-clock ms; any accepted snapshot with a seq above our last accepted publish
  means another publisher outbid us.
- `publishHealth({lastOkAt, nowMs, activeSinceMs})` → `"ok" | "stale"` (stale when active >45s and
  `nowMs - lastOkAt > 45000`) — drives the chip's failure state (mirrors the DIRNAT-06
  recommendation's thresholds).
Load it in `web/src/index.html` next to the other libs (`<script defer
src="lib/svWebDirector.js"></script>` beside svRelayRoom/svSelftest/svSyncDecision — find the
block near the end of index.html; verify `web/build.mjs` copies `lib/` verbatim — map-web cites
build.mjs:52-61 — so no build change should be needed).

**Step 2 — `webDirector` state + config (`web/src/app.js`).** Near the `relay` object (:2993):
```js
const WEB_DIRECTOR_ENABLED = true; // D3: kill-switch const — flip false + redeploy to disable
const webDirector = {
  active: false, code: "", seq: 0, lastOkAt: 0, activeSince: 0,
  inFlight: false, pending: null,        // latest-wins coalescer (mirror directorRelaySync)
  heartbeatTimer: 0, conflictTimer: 0,   // 12s keepalive; ~20s outbid poll
  authWarned: false,                     // one-shot 401 latch (mirror authErrorNotified)
};
```
During the D3 soak, derive `enabled` as `WEB_DIRECTOR_ENABLED && /(?:^|[?&])dir=1/.test(
initialUrl.search)`; drop the query check when Miguel approves general availability.

**Step 3 — Guard the follower machinery** (the "stop the follow loop" half). Three one-line
additive guards — do NOT restructure the functions:
- Top of `connectRelay()` (:3215, before the F5 clear): `if (webDirector.active) return;`
  (critical: :3227 resets `manualClose`, so guarding here is what makes the stop stick).
- Top of `relayPollOnce()` (:3174): `if (webDirector.active) return;` (the visibilitychange/online
  handlers at :3330-3356 call it unconditionally; while directing, a follower-snapshot apply would
  fight our own state).
- The F3 health tick body (:3364, after the `manualClose` check): `if (webDirector.active) return;`
The `visibilitychange`/`online` listeners themselves stay armed — their calls become no-ops via
the guards, and they resume working the instant `active` flips false. Belt-and-suspenders: F1
(:3291) and the go-live bar (:3068) are already inert because `relay.hasDirector` will be false.

**Step 4 — Entry UX (reuse the taught ♪ numpad path).** In `goToDraftSong`'s ≥5-digit web branch,
replace the ROLEWEB-01 routing with the lib call (`decideCodeEntry`); on `"offer-web-director"`,
show the ROLEWEB-01 sheet WITH the second action:
- Message block: same ROLEWEB-01 copy, plus one line: **"¿Tu iPad falló? Puedes dirigir desde este
  teléfono en emergencia — solo para los que siguen por internet."**
- Buttons: **"Entendido"** (dismiss) and **"Dirigir desde aquí (emergencia)"** → `
  beginWebDirectorEntry(code)`. The code travels in memory only — never rendered back, never
  stored.

**Step 5 — Always-confirm parity (native's semantics, §1).** `beginWebDirectorEntry(code)`:
1. Fetch `/state` (6s abort, mirror :3176-3185); compute
   `live = svWebDirector.roomIsFreshDirector(snap, Date.now(), relay.clockOffsetMs, 90)`.
   On fetch failure treat as `live = false` (an emergency must not be blocked by a flaky read —
   the confirm still says the room may have a director).
2. Show a CONFIRM sheet (same injected-element pattern; two buttons, never silent — parity with
   PdfReaderApp.tsx:589-621):
   - `live === false` → title **"¿Dirigir desde este teléfono? (emergencia)"**, body **"Los
     teléfonos y tabletas en signovivo.com seguirán tu página. Los iPads del coro (sin internet)
     NO te seguirán. Mantén la pantalla encendida mientras diriges."**, buttons **"Cancelar"** /
     **"Sí, dirigir"**.
   - `live === true` → title **"⚠️ Ya hay un director transmitiendo"**, body **"Otro dispositivo
     está transmitiendo AHORA. Si continúas, las páginas van a pelear entre los dos. Continúa SOLO
     si el iPad del director falló."**, buttons **"Cancelar"** / **"Tomar el control"**
     (destructive styling — red). This mirrors native's live-director variant (:603-609) with the
     web-appropriate freshness source (relay snapshot vs 8s mesh window).
   - **Cancel = stay exactly as-is** (follower keeps following) — parity with :614-615.
3. On confirm → `enterWebDirector(code)`.

**Step 6 — `enterWebDirector(code)` (promotion; validation and takeover are the SAME publish).**
There is no validate-only endpoint, and probing `/publish` with `seq:0` is NOT safe (on a stale
room the A2 rules make seq-0 a takeover/reset that would fake a live director for 90s —
index.ts:159-167 + :156-158). So the first real publish IS the credential check:
1. Set `webDirector.code = code`, `webDirector.seq = svWebDirector.nextSeq(0, Date.now())`;
   sheet shows **"Verificando código…"** with buttons disabled.
2. Publish the CURRENT page (`state.currentPage` — if they were following, that IS the live page,
   so followers don't jump): POST per §3.3 with a 7s AbortController (mirror
   directorRelaySync.js:50).
3. `401/403` → reset `code`/`active`, show **"El código no fue aceptado. Revisa el número e
   intenta de nuevo."** on the sheet; the device never left follower mode. (Garbage code = honest
   verdict at last — the worker is the validator.)
4. Network failure / abort → **"Sin conexión con signovivo.com. Revisa tu internet e intenta de
   nuevo."**; stay follower.
5. `200` (with or without `ignored:true` — while a fresh director is live our wall-clock seq
   normally wins, but if the very first frame is ignored the 12s heartbeat + next page turn will
   outbid; do NOT treat `ignored` as failure at entry, the D4 warning already covered the
   contention case) → **activate**:
   - `webDirector.active = true; webDirector.activeSince = Date.now();
     webDirector.lastOkAt = Date.now(); webDirector.authWarned = false;`
   - Stop the follower loop: `relay.manualClose = true;` close `relay.ws` if any (the close
     handler honors `manualClose`, :3314); `stopRelayPolling();` `relay.hasDirector = false;
     relay.browsing = false; relay.following = true;` `hideGoLiveBar(); renderRelayPill();`
     (pill hides — :3038).
   - Role UI for free: `state.nativeSyncRole = "director"; renderDirectorModeBadge();` →
     `html[data-role="director"]` → DIRECTOR badge shows (index.html:55-57), ⌕ appears (:2155 —
     and its drawer/Buscar handler :2411 has no role checks, so the web director gets search), ⟳
     hides (:2156), ♪ shifts (:2123).
   - Show the persistent chip (§ step 8) and start the heartbeat + conflict poll (steps 7, 10).
   - Fleet check-in: `fleetCheckin({ role: "Director (web)", label: "DIRECTOR WEB (emergencia)" })`
     — additive `extra` fields the worker already sanitizes (index.ts:266-267); see §3.9.

**Step 7 — Publishing pages (cadence = native parity).**
- **Immediate on page turn:** in `renderPage`'s success commit — right beside the existing
  `postNativeBridge({type:"page-changed",...})` block (:1074-1079 per map; verify locally) — add:
  `if (webDirector.active) publishWebDirectorPage(committedPage);`. Every navigation surface
  (numpad jump, swipes, drawer, search-result taps, ⌕) already funnels through `renderPage`, so
  the web director can drive from ANY of them — including search, which native directors have.
- **12s keepalive:** `webDirector.heartbeatTimer = setInterval(publish current page, 12000)` —
  refreshes `ts` so followers' 90s freshness holds (mirror PdfReaderApp.tsx:401-412). Guard the
  tick on `webDirector.active`.
- `publishWebDirectorPage(page)` implements the latest-wins coalescer + 7s abort, a line-for-line
  mirror of `doPublish`/`publishPageToRelay` (directorRelaySync.js:62-129), with:
  - `webDirector.seq = svWebDirector.nextSeq(webDirector.seq, Date.now())`,
  - on `res.ok` → `webDirector.lastOkAt = Date.now(); webDirector.authWarned = false;`,
  - on `401/403` once (`!authWarned`) → `authWarned = true;` + show the code-rejected banner (post-Wave-2 name: `showDirectorWarning("code-rejected")`) —
    the exact banner + copy the native director gets (:887-925), already in the bundle. A
    mid-Mass code rotation is therefore visible, not silent.
  - on network throw → swallow (next turn / heartbeat republishes) — the chip's staleness facet
    (step 8) is the durable signal, avoiding DIRNAT-06's silent-uplink-death trap on this new
    surface.
  - **Step-down parity (C3):** every publish reads `webDirector.code` at fetch time; `
    exitWebDirector` clears it BEFORE draining, and `doPublish` drops payloads when the code is
    empty (no fetch, no 401, no scary banner on a device that just correctly stepped down — this
    deliberately does directorRelaySync one better; see SYNCE2E-06 for why).

**Step 8 — The visible director state (the chip).** Inject once (`#sv-webdir-chip`, fixed
bottom-center above the safe-area, z-index 60, pointer-events none except a small "✕" affordance
is NOT included — exit is via the DIRECTOR badge, one taught path):
- OK state (green-tinted dark card): **"● EN DIRECTO desde este teléfono — los iPads sin internet
  no te siguen"** — the honesty requirement, permanently visible while directing.
- Stale state (red, when `svWebDirector.publishHealth(...) === "stale"`, evaluated on each
  heartbeat tick): **"⚠ Sin conexión — los seguidores NO están recibiendo tu página"**; auto-back
  to OK on the next successful publish.
- The chip is the web analog of M4's planned director pill; fold it into that pill when M4's spec
  lands (§3.10).

**Step 9 — Screen-lock / battery hazards (a phone, not a parish iPad).**
- The existing wake lock (app.js:3500-3516) already holds the screen on supported browsers
  (iOS 16.4+ Safari); the confirm copy says "Mantén la pantalla encendida" for the rest.
- Add a `visibilitychange` foreground hook (inside the web-director module): on return to visible
  while `active`, immediately `publishWebDirectorPage(state.currentPage)` — mirror of native's
  foreground re-broadcast (PdfReaderApp.tsx:993-1019). A brief background (notification, control
  center) then costs one round-trip, not a 12s gap.
- If the phone locks anyway: heartbeats stop → followers demote to manual after ≤90s (staleness,
  #248) — the honest, already-shipped failure mode. On unlock, the foreground hook re-freshens the
  room and followers re-attach on their next poll/push. No code needed beyond the hook; document
  it in the runbook line of §3.8.
- Battery: publishing is one ~300-byte POST per turn + per 12s — negligible; the wake lock is the
  battery cost. The confirm copy's "enchúfalo si puedes" line (optional, D6) covers it.

**Step 10 — Coexistence: detect being outbid (pre-M4 heuristic).**
`webDirector.conflictTimer = setInterval(~20s)`: raw `fetch(relayStateUrl())` (NOT
`relayPollOnce` — that's guarded off and must stay a follower-only path); if
`svWebDirector.outbid({stateSeq: snap.seq, myLastSeq: webDirector.seq})` → show a dismissible
warning banner (reuse the sheet pattern): **"⚠ Otro director está transmitiendo también. Las
páginas pueden brincar entre los dos. Si ya no debes dirigir tú, sal del modo director (toca
DIRECTOR ✕ Salir)."** with buttons **"Seguir dirigiendo"** / **"Salir del modo director"** (→
step 11). This is the web mirror of DIRNAT-09's "the loser must be told" lesson. Honest limits:
pre-M4 there is no transmitterId, so a false positive is possible only if something else publishes
a HIGHER seq (another wall-clock publisher — which IS the conflict case) — the heuristic is sound.
The NATIVE director in the ping-pong still gets no signal until M4's `{ignored:true}`/pill work.

**Step 11 — Stepping down.** Branch DIRNAT-04's exit dialog (after Wave 2 the raw `window.confirm` is gone — do not re-add it): in its destructive-button handler run `if (webDirector.active)
exitWebDirector(); else postNativeBridge({type:"exit-director"});`. Branch the dialog BODY too: when `webDirector.active`, use **"Este teléfono volverá a seguir al director."** (DIRNAT-04's native body "Este iPad quedará en modo lectura." is false here). `exitWebDirector()`:
1. `webDirector.code = ""` FIRST (in-flight drain drops, step 7), `active = false`; clear
   `heartbeatTimer`/`conflictTimer`; remove the chip; `authWarned = false`.
2. Role UI back: `state.nativeSyncRole = "off"; renderDirectorModeBadge();` (badge hides, ⌕
   hides, ⟳ returns).
3. Resume following: `relay.manualClose = false;` then call `reconnectRelay()` (:3088 — its web
   branch resets browsing/backoff/`lastSeq=-1`, reopens the socket, forces a poll; the step-3
   guards are inert now). Optional toast: **"Dejaste de dirigir — este teléfono vuelve a seguir al
   director."**
4. What the room sees: the last snapshot ages out; followers stay "live" on the final page up to
   90s, then demote to manual — IDENTICAL to a native director exiting (there is no un-publish
   primitive; do not invent one).
5. Reload/crash while active = implicit step-down (in-memory state only, D5): the phone boots
   follower; bootGuard/M2 recovery paths are untouched.

**Step 12 — Keep `?selftest` and boot honest.** No changes needed — verify only: the feature is
inert at boot (follower default, ROLEWEB-07's `data-role="follower"`), `?selftest` remains
read-only on the current room, and `scripts/smoke-boot.mjs` still passes (add the lib-file
presence check: `dist/lib/svWebDirector.js` exists and index.html references it — mirror how the
other libs are asserted, if they are; otherwise add both).

---

### §3.5 Phase 1 — acceptance criteria

- **Entry:** on pure web with a valid code, ♪ → 10 digits → sheet → "Dirigir desde aquí
  (emergencia)" → confirm → directing within ~30s of opening the site. With an INVALID code the
  flow ends at the honest 401 message; the device never leaves follower mode; no director UI ever
  appears.
- **Never silent:** no path promotes without the confirm sheet; Cancel leaves the follower
  exactly as it was (still following). With a fresh room snapshot the confirm is the destructive
  "⚠️ Ya hay un director transmitiendo" variant.
- **Directing:** every page navigation (numpad, swipe, drawer, search) reaches a second web
  follower in ≤4s (WS push ≈ instant; poll fallback ≤4s); a stationary web director keeps
  followers live indefinitely (12s heartbeat vs 90s window); the DIRECTOR badge + ⌕ show, ⟳
  hidden, chip visible with the mesh-honesty line.
- **Failure states:** code rotated mid-session → the red relay-auth banner (same as native) fires
  once; uplink dead >45s → chip flips to "⚠ Sin conexión…" and back on recovery; being outbid →
  conflict banner within ~20-40s.
- **Step-down:** badge tap → confirm → follower mode resumes (pill/bar behavior normal, ⟳ back);
  no straggler publish after the code clears (assert no POST fires); followers demote to manual
  ≤90s later if nobody else directs.
- **Isolation:** with `?env=staging`, ALL publishes/polls hit `alvernia-staging` only;
  `alvernia-main`'s `/state` never changes during the whole §3.8 script.
- **Native inertness:** in the native shell (bridge present), `decideCodeEntry` routes to
  `bridge-code`; grep-level assert that no web-director code path can run when
  `hasNativeBridge() || NATIVE_FILE_MODE`.
- **Regression floor:** all existing CI-allowlist tests green; `smoke-boot.mjs` green; follower
  behavior on signovivo.com byte-identical when the feature is not invoked.

### §3.6 Kill-switch / rollback

- **Ship vector is web-only** → the M1 lever applies: `scripts/rollback-web.sh` re-promotes the
  previous Pages deployment (~60s to online followers). Practice it before launch (major-update
  §7 STEP 6).
- **In-bundle kill-switch:** `WEB_DIRECTOR_ENABLED = false` + redeploy (same ~60s). During the D3
  soak the `?dir=1` gate means the GA bundle never exposed the button at all.
- **Credential-level:** rotating a code out of `TRANSMITTER_CODES` (wrangler secret) revokes that
  code everywhere — web AND native (blast radius: the native director using it loses relay
  publishing too and gets the red banner). Nuclear option only.
- **No worker rollback needed** — Phase 1 changes no worker code.

### §3.7 Phase 2 — full parity ambitions (sketch ONLY; defer; re-decide after Phase 1 soak)

Not designed here; listed so nobody smuggles them into Phase 1: transmitterId-driven deterministic
tiebreak UI (consume `{ignored:true}` + M4 identity; replace the seq heuristic); native-side
"web director active" signal (M4 pill facet); web NEW-DIR-1 resume prompt (D5-B); fleet-dashboard
first-class web-director row + history; practice-mode room picker UI; director panic buttons
("TODOS RE-SINCRONICEN") from the web; super-admin distinction (needs a worker code split, D2-B);
any mesh-bridging fantasy (a browser cannot Multipeer — permanently out).

### §3.8 Manual end-to-end verification script (for Miguel — staging only, ~20 min)

Prereqs: feature built behind `?dir=1` (D3 soak), a REAL director code on hand, two phones (A =
"emergency director", B = follower) + a laptop. **Never** point this at prod.

1. **Deploy staging:** `STAGING=1 SKIP_NATIVE=1 bash scripts/release.sh` → copy the printed Pages
   PREVIEW URL. ⚠️ RELVER-01: do NOT use `signovivo.com?env=staging` — that host serves the PROD
   bundle; the param only switches the room.
2. **Point both devices** at `<preview-url>/?env=staging&dir=1`. Verify on each: build badge shows
   the new build; reload keeps the params (clearInitialUrl :669-678). B behaves as a normal
   follower (no director → no pill).
3. **Prod isolation baseline (laptop):** open
   `https://signovivo-sync.4j4982y8jp.workers.dev/r/alvernia-main/state` — note `seq`/`page`.
   Re-check after every step below: **it must never change.**
4. **Honest dead-end (ROLEWEB-01):** on A WITHOUT `dir=1` (open `<preview-url>/?env=staging`):
   ♪ → real code → Ir → the "Este código se usa en el iPad…" sheet, NO director option. Back to
   the `&dir=1` URL for the rest.
5. **Bad code honest 401:** A: ♪ → `9999999999` → "Dirigir desde aquí (emergencia)" → confirm →
   "El código no fue aceptado…" — still a follower.
6. **Become web director:** A: ♪ → real code → emergency button → confirm sheet (plain variant —
   empty staging room) → "Sí, dirigir" → DIRECTOR badge + ⌕ visible, ⟳ gone, chip "● EN DIRECTO…
   los iPads sin internet no te siguen".
7. **Drive:** A jumps to song 25 (♪), swipes two pages, opens ⌕ → searches → taps a result. B
   follows every move ≤4s, green pill pulsing.
8. **Stationary hold:** wait 2 min without touching A. B stays live (heartbeat). Laptop:
   `/r/alvernia-staging/state` shows `ts` refreshing ~12s.
9. **Screen-lock hazard:** lock A for ~2 min → B demotes (pill hides, manual nav works) — the
   honest failure. Unlock A → foreground republish → B re-attaches ≤4s.
10. **Conflict (two directors):** on the laptop, publish once to the staging room with a second
    valid code via curl (or phone C repeating step 6). Within ~20-40s A shows "⚠ Otro director
    está transmitiendo también…". Watch B ping-pong (expected pre-M4). Step the intruder down.
11. **Step down:** A taps DIRECTOR ✕ Salir → confirm → ⟳ returns, chip gone; A follows again.
    ~90s later B shows no-director (manual). Re-check step 3's prod state one last time.
12. **Rollback drill:** run `scripts/rollback-web.sh` against the staging project once, end to
    end, so the lever is proven before GA.

Record PASS/FAIL per step in the PR. Any FAIL → stop, fix, rerun from step 1 (fresh deploy).

### §3.9 What the fleet dashboard should show

Phase 1 (client-only, ships with step 6): the check-in extra `{role:"Director (web)",
label:"DIRECTOR WEB (emergencia)"}` on entry and `{role:"", label:""}` on exit — the worker
already stores both (index.ts:266-267). Because dashboard rows are roster-name-matched
(index.ts:449-476) and this label matches nobody, the device lands in the ORPHAN table — visible
but not prominent.
**SHOULD (small additive worker follow-up, fold into M6's dashboard rework alongside RELVER-04's
web-build column):** a top-of-dashboard banner when any check-in seen <5 min has
`surface==="web" && role.startsWith("Director")`: **"⚠ DIRECTOR WEB ACTIVO (emergencia) — <label>,
visto hace <n>s"**. Display-only; no schema change. Do NOT build it in Phase 1 unless Miguel asks
— the orphan-row visibility is acceptable for an emergency feature.

### §3.10 Major-update intersections (map EVERY milestone — docs/major-update-2026-07.md)

| Milestone | Intersection with the web-director path |
|---|---|
| **M0 CI + boot-smoke** (SHIPPED — `.github/workflows/ci.yml`, `scripts/smoke-boot.mjs`) | `e2e/svWebDirector.test.mjs` joins the safe allowlist (ci.yml:58-70). smoke-boot gains the lib-presence check (§3.4 step 12). Feature must keep boot inert (follower default) so the smoke's boot assertions never see it. |
| **M1 staging channel** (SHIPPED — svRelayRoom, `?selftest`, `STAGING=1 release.sh`, `rollback-web.sh`) | The ENTIRE §3.8 verification runs in `alvernia-staging` via `?env=staging`; the room comes from the shared `RELAY_ROOM` const so no new resolver is needed. `rollback-web.sh` is the rollback lever (§3.6). Honor RELVER-01: canary content = the preview URL, never `signovivo.com?env=staging`. |
| **M2 crash-proofing (web)** | All new code follows the never-throw discipline (bare-catch fetches, guarded storage — there is none). A crash while directing → bootGuard recovery → boots FOLLOWER (never auto-resume a credentialed role, D5). `reportCrash("web-director", …)` breadcrumbs from the module's catches are welcome (best-effort). |
| **M3 bridge v1** | No bridge on pure web — structurally disjoint. Only touchpoint: DIRNAT-01's `director-code-result` envelope shares the numpad/sheet surface on NATIVE; coordinate copy so "Verificando…" reads the same on both surfaces. |
| **M4 sync robustness** | **The gating prerequisite (D1 recommendation: land Phase 1 inside/immediately after this slice).** transmitterId + two-publisher tiebreak (P2-IDENTITY) replaces §3.4-step-10's seq heuristic; the web director then sends `transmitterId: fleetDeviceId()` (§3.3 rider). `{ignored:true}` consumption gives the NATIVE side its first "someone else is publishing" signal — extend M4's director pill spec with it (overlaps DIRNAT-06's relay-health facet). M4's tri-state follower pill must add nothing for Phase 1 (the web director's chip is separate), but fold the chip into the pill system when M4's spec is implemented. |
| **M5 book out of bundle** | Once `bookVersion` exists, the transmitter stamps it on the heartbeat (§6.2 of the program doc) — add `bookVersion` to `publishPayload` then (additive). Nothing in Phase 1 blocks or is blocked by M5. |
| **M6 super-admin + distribution** | The finding's "natural companion": the admin surface is where a first-class kill-switch / force-takeover could live (Phase 2), and §3.9's dashboard banner belongs in M6's dashboard rework (same PR as RELVER-04's web-version column). |
| **M7 the 2-device day (native)** | ZERO native changes in Phase 1. But add to the M7 device-day script: (a) with the new bundle baked into the TestFlight build, verify the module is inert in the shell (enter a code → native confirm flow only); (b) the native+web two-director scenario — the native director's UX when a web director publishes (blind pre-M4; pill signal post-M4); (c) M7's native staging entry (directorRelaySync.js:13 hardcodes `alvernia-main`) finally lets the NATIVE side join staging drills too. |
| **§5 contracts** | Phase 1 sends the EXACT existing snapshot shape; never bumps `v`; transmitterId rides additively post-M4. The CI-allowlist hard contract (never the e2e glob) is restated in every Tests section of this chapter. |
| **§7 rollout recipe** | Add one line to STEP 2's canary walk once GA: "☐ web: código en un teléfono → dirigir en emergencia → un segundo teléfono sigue (staging)". |
| **§10 honest caveats / A1** | The legacy committed code `12345678840` possibly still in `TRANSMITTER_CODES` becomes MORE dangerous with a phone-friendly UI (D2). Rotate it out (hardening-plan Q3) BEFORE GA. |

**Tests (ROLEWEB-02 Phase 1 — consolidated).** (Never `npm run test:e2e`; never
`e2e/relay-sync.test.mjs`.)
- **Unit (node, CI):** `e2e/svWebDirector.test.mjs` — every §3.4-step-1 export: routing table
  (incl. `enabled` off → `explain-ipad`), `nextSeq` monotonic + wall-clock floor, payload shape
  parity (deep-equal against a fixture copied from directorRelaySync semantics), freshness
  matrix (fresh/stale/absent-ts/skew), `outbid` truth table, `publishHealth` thresholds, junk
  never throws.
- **Worker harness (local wrangler ONLY):** extend `sync-worker/test/a2.test.mjs` (runner
  `test/run-a2.sh`; the file refuses any base matching `/signovivo|workers\.dev/` — keep that
  guard): (a) publish with `X-Director-Code` + the §3.3 body → 200 + `/state` reflects it; (b)
  bad code → 401; (c) two-publisher interleave (wall-clock seqs) → documents last-write-wins;
  (d) `transmitterId` in the body is accepted and harmlessly dropped by the current worker
  (pre-M4 compat proof).
- **Source-contract (node, CI):** assert the three §3.4-step-3 guards exist (regex-pin
  `webDirector.active` inside `connectRelay`/`relayPollOnce`/the F3 tick) and that
  `beginWebDirectorEntry` is unreachable when `hasNativeBridge()` (pin the `decideCodeEntry`
  call site).
- **Manual:** §3.8, staging room, before ANY prod exposure.

**Dependencies (ROLEWEB-02).** Miguel's D1–D6 answers (hard gate) → ROLEWEB-07 (data-role ground
truth) → ROLEWEB-01 (the entry sheet) → M4 transmitterId+tiebreak (recommended co-landing; hard
requirement for lifting the "emergency-only" framing) → A1 code rotation before GA (§3.10 last
row). Worker changes: none for Phase 1.

---

## §4. Suggested landing order (whole workstream)

1. ROLEWEB-07 (one line + comment + contract test) — ship now.
2. ROLEWEB-01 (sheet + router lib + tests) — ship now; instantly de-lies the emergency path.
3. Take §3.1's D1–D6 to Miguel. **Stop here until answered.**
4. If approved: build Phase 1 behind `?dir=1`, run §3.8 in staging, soak, flip the gate with/after
   M4, add the §7-recipe line, rotate the A1 code first.

## §5. Evidence corrections found during re-verification (HEAD d5075091)

The findings JSON already self-corrected most anchors; this pass confirms those and adds:
1. **ROLEWEB-01** — confirmed as corrected: length gate :1178; web else-branch :1187-1190; flash
   :1189; comments :827-829 + :1180-1182; `flashSongDisplay` :831-841. NEW nuance for the
   executor: `normalizeSongDraftNumber` (app.js:691-698) strips leading zeros before the length
   gate (harmless for NANP codes; do not "fix" in passing — it would change the native contract).
2. **ROLEWEB-02** — confirmed as corrected: worker auth block :773-818 (comment :777-779, digit
   strip :782, `codeOk` :785, 401 :786-788); `X-Director-Code` CORS allowlist at index.ts:372 —
   and the comment at :370-372 explicitly names signovivo.com as an anticipated publisher
   (stronger than cited); app.js anchor is :1187-1190 (not :1183); major-update:147 is the M4 row
   as cited. userImpact overstatement ("no fallback surface") dropped per the verifier — any
   native fleet iPad + the memorable code is today's fallback, and staleness demotion (#248)
   releases followers to manual navigation.
3. **ROLEWEB-07** — confirmed as corrected: `dataset.role` write at :849 (comment :847-848); sole
   caller :952 (handler :947-956); `index.html:2` has no static attribute; director-keyed CSS at
   styles.css:2123/:2155/:2156 — the originally-cited :2154 is the follower BASE rule
   (`.search-fab{display:none}`), which supports the "unselected base" claim but is not a director
   selector (corrected).
4. **findings-roleweb.md:65** (lens write-up, ROLEWEB-02 sketch) cites "styles.css:2154-2156" for
   the free director CSS — precise anchors are :2155-2156 plus :2123 (corrected).
5. Confirmed unchanged at HEAD (no drift): directorRelaySync.js:13 room literal, :56-60 seq,
   :87-96 latch; index.ts:146-179 A2/staleness gates, :31-34 90s window; svRelayRoom.js:28-36;
   clearInitialUrl app.js:669-678; onDirectorCode PdfReaderApp.tsx:563-624 (confirm variants
   :603-621, 8s window :71/:598-602, Cancel :614-615); ci.yml allowlist :58-70;
   scripts/{release.sh:33+125, rollback-web.sh, smoke-boot.mjs} all present as described.


---

## Workstream 5 — Fleet, canary, versioning & release truth

> Repo: `(repo root)` · HEAD `d5075091` (build 381, v1.0.4).
> All file:line anchors below were re-verified against that HEAD on 2026-07-09. If your tree has moved,
> re-grep the quoted strings before editing — the quoted code is the anchor, the line number is a hint.

## 5.0 Workstream intro

**Theme: the operator must be able to TRUST what the fleet, the canary, and the badges say.**
SignoVivo's whole release-safety posture (M0/M1: staging channel → canary walk → promote → rollback)
assumes four surfaces tell the truth pre-Mass:

1. **The canary URL** — what bundle is the canary iPad actually executing? Today the checklist's primary
   URL executes the *prod* bundle (RELVER-01), the canary steps that matter are physically impossible on
   the web canary (RELVER-12), a correct canary shows a badge identical to prod (RELVER-11), and a device
   accidentally left pinned to the staging *room* looks completely normal (RELVER-02). Net effect: the
   ritual can pass green while proving nothing, and can leave a Sunday landmine behind.
2. **The fleet dashboard** — is each device ready? Today it cries wolf about the real director during
   every canary window (SYNCE2E-07), grants "Listo — web en caché" to a web shell of unknown age
   (RELVER-04), and since #270 contradicts itself about every web device ("No se ha visto — invitar" for
   the person, "✓ inicio" for the same iPad as an anonymous orphan — RELVER-09).
3. **The version badge** — what code runs on THIS device? Today the badge shadows the loaded web bundle
   behind the native shell number, while crash telemetry uses the *opposite* precedence (RELVER-05), so
   the two version surfaces legitimately disagree exactly when a stale bundle is live. The mesh OTA that
   creates that skew also re-transfers ~30 MB and remounts the follower on every reconnect (RELVER-06),
   and its failures are dropped on the JS floor (FOLNAT-07).
4. **The worker** — which relay code serves the room? `/health` returns a wire version pinned to 1
   forever; deploys are unverifiable (RELVER-07).

FOLNAT-05 (remove the native "¿Quién usa este iPad?" prompt) rides in this workstream because it is the
other half of #270's decision and it forces the same question RELVER-09 forces: **are self-entered labels
dead fleet-wide?** Land the dashboard's answer (RELVER-09) before or with the prompt removal.

**How the findings interact.**
- RELVER-01 + RELVER-12 rewrite the same checklist section (§A.3) — land as ONE docs PR.
- RELVER-11 (badge says `-prueba`) and RELVER-02 (chip says the *room* is pinned) are the two visual
  markers RELVER-01's rewritten ritual tells the operator to look for. Badge = which *bundle*;
  chip = which *room*. They are orthogonal seams; you need both.
- SYNCE2E-07 + RELVER-04 + RELVER-09 all edit `renderFleetDashboard` in `sync-worker/src/index.ts` —
  do them as one classifier refactor, one worker deploy.
- RELVER-07's deploy wrapper is how you *verify* that worker deploy.
- RELVER-05's web half (dual-number badge) instantly exposes the skew RELVER-06 creates; RELVER-06 and
  FOLNAT-07 both live in the mesh-OTA seam of `DirectorSyncModule.swift` and belong in the M7 native
  batch alongside the held M-F3 design (defer-install).

**WIRE COMPAT (non-negotiable, applies to every fix below).** Native builds 368–381 with their bundled
old web copies stay in the field indefinitely. Therefore: fleet check-in body changes are ADDITIVE ONLY
(new optional fields; never rename/remove/repurpose); the worker's `checkin()` must keep accepting the
old shapes (it whitelists fields — an old body simply lacks the new ones); dashboard logic must render
sanely when new fields are absent (absent ≠ error; absent = old client, usually the *interesting* state);
`/health` gains fields, never changes existing ones; the mesh `bundle_offer`/`bundle_request` wire keeps
its exact field names.

**Recommended PR slicing (four coherent deploys):**

| PR | Contents | Ship vector / deploy | Order |
|---|---|---|---|
| **WS5-PR1 "web: canary visibility + version truth"** | RELVER-02, RELVER-11, RELVER-04 (web half), RELVER-05 (web half) + the `scripts/release.sh` `SV_STAGING` export | Pages deploy (`SKIP_NATIVE=1 bash scripts/release.sh` when promoting) | first |
| **WS5-PR2 "worker: dashboard truth + deploy identity"** | SYNCE2E-07, RELVER-09, RELVER-04 (worker half), RELVER-07 | `bash sync-worker/deploy.sh` (new wrapper, RELVER-07) | with/after PR1 (worker tolerates either order; PR1-first means web devices start reporting `buildNumber` before the dashboard renders the column) |
| **WS5-PR3 "docs: canary ritual truth"** | RELVER-01, RELVER-12, plus every ritual edit listed in the per-finding *Tests* sections of PR1/PR2 items | docs only (no deploy) | after PR1 (docs reference the new `-prueba` badge and chip) |
| **WS5-PR4 "native M7 batch (device-gated)"** | RELVER-06 (+ held M-F3 alongside), FOLNAT-07, FOLNAT-05, RELVER-05 (native half) | TestFlight build via `bash scripts/release.sh`, verified on the 2-device day | M7 |

**Testing ground rules for this workstream.** NEVER run `npm run test:e2e` and NEVER run
`e2e/relay-sync.test.mjs` (it targets the production relay; the standing repo rule). Worker changes are
verified two ways: (a) pure-unit tests on the extracted classifier (`node --test`, no network), and
(b) behavioral checks against a LOCAL `wrangler dev` via the existing harness pattern
(`sync-worker/test/run-a2.sh` boots `wrangler dev` on :8787, exports `RELAY_TEST_BASE`/`RELAY_TEST_CODE`/
`RELAY_TEST_FLEET_KEY` from `.dev.vars`, runs `node --test`; the test files throw at load without those
vars and refuse any base matching `/signovivo|workers\.dev/` — copy that guard into every new worker test
file). The CI safe-subset lives at `.github/workflows/ci.yml:60-69` (named files only).

**DECISION-REQUIRED (Miguel) items in this workstream** (each also flagged inline):
- **D-WS5-1 (RELVER-09 / FOLNAT-05): label end-state.** Options: (a) labels die fleet-wide — remove the
  native prompt, dashboard pivots to deviceId-first, roster later gains an optional per-person `deviceId`
  pin (seeded once by you from the device table) to restore named matching; (b) labels survive
  native-only — keep the iPad prompt, document the asymmetry. **Recommendation: (a)** — #270's own data
  ("mostly tapped 'Ahora no'") applies verbatim to the native twin, and the deviceId pin is a
  once-per-season seeding step you already do for the roster.
- **D-WS5-2 (RELVER-02): 24 h auto-expiry of the `?env=staging` pin.** Options: chip only vs chip +
  auto-return to the prod room after 24 h with a toast. **Recommendation: chip + 24 h expiry** — the
  canary walk is Wed/Sat and Mass is Sun/Thu (≥ 3 days later), so an expiry can never fire mid-walk but
  always disarms the Sunday trap; there is no real multi-day-pin workflow.

---

## 5.1 Findings

#### RELVER-01 — Canary ritual's primary URL tests the WRONG bundle — `signovivo.com?env=staging` serves PROD content, `?env=staging` only switches the relay room `high` `cross` `web-only`

**Problem.** The pre-Mass checklist's canary step points the canary iPad at
`signovivo.com?env=staging` *first*. That host is Cloudflare Pages **prod branch `main`** and always
serves the current PROD bundle; `?env=staging` is consumed by exactly one thing — the relay-room
resolver, which pins the device to the `alvernia-staging` *room*. The staging *content* exists only on
the Pages preview branch `staging` (the URL `STAGING=1 release.sh` deploys to). The runbook and
release.sh's own usage text repeat the same misconception in prose.

**User impact at Mass.** The operator walks the canary against the old, already-proven prod bundle
joined to an empty staging room; every checkbox passes trivially; a never-executed build is promoted to
prod — the M1 release-safety gate is silently defeated and the first execution of the new bundle is in
front of the parish.

**Evidence (verify before editing).**
- `docs/pre-mass-checklist.md:23-24` — step A.3 points the canary "pointed at staging
  (`signovivo.com?env=staging`, or the printed preview URL)" — the wrong URL listed FIRST (URL literal on
  line 24; step starts line 23). *(corrected: original cite said :23 for the URL; it is on :24)*
- `web/src/lib/svRelayRoom.js:31` — `params.get("env") === "staging" ? STAGING_ROOM : PRODUCTION_ROOM` —
  the param selects only the relay room; repo-wide, nothing selects *content* by it.
- `scripts/release.sh:33` + `:125` — `DEPLOY_BRANCH="staging"` under `STAGING=1`, and the only deploy is
  `npx wrangler pages deploy web/dist --project-name alvernia-reader --branch "$DEPLOY_BRANCH"` — canary
  content exists only at the preview branch URL, never on signovivo.com. *(corrected: original cite said
  :34, which is an `else`)*
- `scripts/release.sh:18` — the usage header itself says "Prove a build here (signovivo.com?env=staging)
  before promoting to prod" — the misconception is baked into the tool's own help text. *(added — the
  strongest single cite)*
- `scripts/release.sh:129` — the 6/6 echo says "or signovivo.com?env=staging **once promoted**" — the only
  technically-correct phrasing in the repo; use it as the model.
- `docs/green-day-deploy-runbook.md:45` — "the `?env=staging` isolation is for the *bundle*, Step 2" —
  false; it is for the ROOM.
- Aggravating (ties to RELVER-11): `scripts/release.sh:38-41` — STAGING skips the version bump, so the
  canary badge shows the same `v<N>` as prod; even on the wrong URL nothing looks off.

**Fix — step by step.**
1. `scripts/release.sh:18` — rewrite the STAGING usage comment to:
   `#   Prove a build on the PREVIEW URL (staging.alvernia-reader.pages.dev/?env=staging) before promoting.`
   `#   signovivo.com always serves PROD; ?env=staging only pins the RELAY ROOM, never the content.`
2. `scripts/release.sh:127-131` (the `if [ "$STAGING" = "1" ]` final echo block) — replace the body so the
   script ends with one canonical copy-paste line. Keep the existing lines 130-131; replace 128-129 with:
   ```bash
   echo "==> 6/6  DONE — staging preview deployed. Prove it on the canary iPad at EXACTLY this URL"
   echo "         (preview CONTENT + staging ROOM — signovivo.com would serve the OLD prod bundle):"
   echo "           https://staging.alvernia-reader.pages.dev/?env=staging"
   echo "         Badge must read ${BUILD}-prueba (see RELVER-11). THEN promote to prod:"
   ```
   (`staging.<project>.pages.dev` is Cloudflare Pages' stable branch alias for the `staging` branch of
   project `alvernia-reader`; the per-deploy hash URL wrangler prints also works but is not stable.)
3. `docs/pre-mass-checklist.md:23-24` — change the canary-walk pointer to name the preview URL as the
   ONLY canary content source, `?env=staging` as the room pin ON TOP of it:
   `pointed at the staging preview URL printed by release.sh — `https://staging.alvernia-reader.pages.dev/?env=staging` — never signovivo.com (that host always serves PROD; the `?env=staging` part only keeps the canary off the live Mass room):`
   (The full §A.3 restructure — which checkbox can run where — is RELVER-12; land both in WS5-PR3 as one
   edit so §A.3 is rewritten once.)
4. `docs/green-day-deploy-runbook.md:44-45` — fix the parenthetical to:
   `(there is no per-room canary for worker *code*; `?env=staging` isolates the relay ROOM — the staging *bundle* is isolated by the Pages preview branch, Step 2)`.
5. `docs/green-day-deploy-runbook.md:81-86` (Step 2 verify block) — replace "Verify on the preview URL
   (or a canary device)" + line 86's "Point the canary at the staging room…" with one instruction naming
   the combined URL: `Verify on the canary device at https://staging.alvernia-reader.pages.dev/?env=staging (preview content + staging room in one URL).`

**Acceptance criteria.**
- [ ] `grep -rn "signovivo.com?env=staging" docs/ scripts/` returns only phrasings that (a) mark it as a
  *post-promotion* URL or (b) explicitly say it serves prod content — no instruction implies it can serve
  canary content.
- [ ] `STAGING=1 bash scripts/release.sh` ends by echoing the exact
  `https://staging.alvernia-reader.pages.dev/?env=staging` line (verify with `bash -n` + a dry read of the
  echo block; do NOT run a real deploy to test an echo).
- [ ] The checklist's canary step names the preview URL first and only.

**Tests.**
- Extend `scripts/smoke-boot.mjs`? No — wrong layer. Instead add a cheap ritual-drift guard to the CI
  safe subset: new file `e2e/release-ritual-docs.test.mjs` (source-regex style, mirrors
  `e2e/nearby-sync-contract.test.mjs` pins): (a) `scripts/release.sh` contains
  `staging.alvernia-reader.pages.dev/?env=staging` and `DEPLOY_BRANCH="staging"`; (b)
  `docs/pre-mass-checklist.md` contains the preview host string and does NOT match
  `/signovivo\.com\?env=staging[^ ]*`? — too brittle; instead assert the checklist contains the phrase
  `never signovivo.com`. Add the file to `.github/workflows/ci.yml:60-69`'s named list.
- Docs edits: steps 3-5 above ARE the docs edits (`docs/pre-mass-checklist.md`,
  `docs/green-day-deploy-runbook.md`).

**Dependencies.** Pair with RELVER-11 (badge `-prueba` gives the rewritten doc its "positive proof"
checkbox) and RELVER-02 (room chip); land the checklist text together with RELVER-12 in WS5-PR3.

---

#### RELVER-06 — Mesh OTA re-transfers the ~30 MB bundle and remounts the follower on EVERY reconnect — installed pack version never persisted `high` `native` `native-build`

**Problem.** A follower decides whether to accept a director's `bundle_offer` by comparing against
`currentBundleVersion`, which is the APP's `CFBundleVersion` — a computed property that never changes
after a mesh bundle install. The received pack's own `headerVersion` is parsed and emitted but persisted
nowhere. So after a successful install, "mine" is still the old shell build; the director re-sends
`bundle_offer` on every peer connect, and every reconnect (backgrounding, mesh churn, M-F1 watchdog
kicks) re-triggers the full ~30 MB transfer plus an unconditional WebView remount.

**User impact at Mass.** During any mixed-build window (the normal state for days after each TestFlight
rollout, per the checklist's own adoption model), every behind-by-one follower iPad repeatedly saturates
the mesh with 30 MB transfers — starving the ~100-byte page turns — and hard-remounts its WebView
mid-Mass. This is the held M-F3 disaster upgraded from *once* to *once per reconnect, forever*.

**Evidence (verify before editing).**
- `ios/SignoVivo/DirectorSyncModule.swift:97-99` — `currentBundleVersion` reads
  `Bundle.main.infoDictionary?["CFBundleVersion"]` only.
- `ios/SignoVivo/DirectorSyncModule.swift:717-728` — `handleBundleOffer`: `let mine =
  Int(currentBundleVersion) ?? 0` (:721); `guard offered > mine else { return }` (:722) — after an
  install, `mine` is still the shell build, so the same offer re-triggers. *(corrected: original cite
  :722 for the whole mechanism; accept-guard is :722, `mine` computed at :721, function spans 717-728)*
- `ios/SignoVivo/DirectorSyncModule.swift:937` — `let headerVersion = headerObj["version"] as? String ?? ""`
  — parsed; `grep -n "UserDefaults" ios/SignoVivo/DirectorSyncModule.swift` → zero hits: persisted nowhere.
- `ios/SignoVivo/DirectorSyncModule.swift:1043-1047` — `bundleUpdated` emit carries `"version":
  headerVersion` (:1046) — the value exists at the exact success moment, then is dropped.
- `ios/SignoVivo/DirectorSyncModule.swift:1743` — `self.sendBundleOffer(to: peerID)` inside the
  `.connected` handler — offered on every peer connect.
- `ios/SignoVivo/DirectorSyncModule.swift:800-806` — `packWebBundle` reads
  `Bundle.main.resourceURL/WebBundle` only — a director can only SERVE its shipped bundle (prior-art O3),
  which constrains what it may honestly advertise.
- `PdfReaderApp.tsx:954-962` — the `bundleUpdated` handler calls `setMountKey((k) => k + 1)`
  unconditionally — every re-transfer also blanks the follower mid-Mass.
- The atomic-swap success point (where to persist): `DirectorSyncModule.swift:1032-1041` — the
  `fm.moveItem(at: newDir, to: target)` success path, just before the emit at :1043.

**Fix — step by step.** This BUILDS ON the held M-F3 design (docs/sync-reliability-audit-2026-07.md:63:
"defer the install/remount to a safe moment (backgrounded/idle) or gate behind a confirm"). The two are
complementary, not rivals: **M-F3 gates WHEN the one legitimate install applies; RELVER-06 makes installs
happen at most ONCE per version.** Where this differs from M-F3: nothing conflicts — without this fix,
M-F3's deferral would merely reschedule an infinite loop; without M-F3, this fix still leaves ONE
mid-Mass remount per real update. Ship both in the same M7 Swift batch (WS5-PR4).
1. `DirectorSyncModule.swift` — add near `currentBundleVersion` (:97-99):
   ```swift
   private static let installedBundleVersionKey = "sv.webBundle.installedVersion"
   /// The newest web bundle this install can RUN: the shipped shell bundle or a
   /// mesh-installed pack, whichever is newer. Used ONLY on the accept side —
   /// the offer side advertises currentBundleVersion because packWebBundle can
   /// only serve Bundle.main's copy (it cannot re-pack Documents/WebBundle).
   private var effectiveBundleVersion: Int {
     let shell = Int(currentBundleVersion) ?? 0
     let installed = UserDefaults.standard.string(forKey: Self.installedBundleVersionKey)
       .flatMap(Int.init) ?? 0
     return max(shell, installed)
   }
   ```
2. Persist at the swap-success point: immediately after the `try fm.moveItem(at: newDir, to: target)`
   success (i.e., after :1041's rollback-guarded block completes, before the `bundleUpdated` emit at
   :1043), add:
   ```swift
   if Int(headerVersion) != nil {
     UserDefaults.standard.set(headerVersion, forKey: Self.installedBundleVersionKey)
   }
   ```
3. `handleBundleOffer` (:721) — change `let mine = Int(currentBundleVersion) ?? 0` to
   `let mine = effectiveBundleVersion`. Do NOT touch `sendBundleOffer` (:705-713): the director must keep
   advertising `currentBundleVersion` (the shipped pack it can actually serve — see the O3 evidence).
   Document that asymmetry in a comment on both functions.
4. Cleanup hook (also serves the known `native-swift-stale-documents-bundle-masks-update` finding): in
   the module init path (e.g. alongside the observer registration around :246), add a one-shot check:
   ```swift
   let shell = Int(currentBundleVersion) ?? 0
   let installed = UserDefaults.standard.string(forKey: Self.installedBundleVersionKey).flatMap(Int.init) ?? 0
   if installed > 0 && shell >= installed {
     UserDefaults.standard.removeObject(forKey: Self.installedBundleVersionKey)
     // Recommended rider (the adjacent known finding's fix — one line under the same guard):
     // delete Documents/WebBundle so resolveBundleUri falls back to the fresh shipped copy.
   }
   ```
   `>=` not `>`: when TestFlight delivers exactly the pushed version, the Documents copy is redundant.
   The `removeItem(Documents/WebBundle)` rider is the prior-art O1 fix — include it in the same M7 batch
   but call it out separately in the PR body (it changes which bundle loads after an app update).
5. JS side (`PdfReaderApp.tsx:954-962`): no change required — with steps 1-4 the redundant `bundleUpdated`
   events stop at the source. Optionally record `event.version` into a ref for RELVER-05's fleet
   `webBuild` forwarding (see that finding's native half).
6. WIRE COMPAT check: `bundle_offer`/`bundle_request` payload shapes unchanged; an OLD follower (368-381)
   talking to a NEW director sees identical bytes; a NEW follower talking to an OLD director simply stops
   re-downloading — no version of this change alters the wire.

**Acceptance criteria** (2-device day, per the M7 script):
- [ ] A (shell 38X+1, director) + B (shell 38X, follower): first connect transfers the pack ONCE and
  remounts once; force-disconnect/reconnect (background A, or toggle wifi) → NO new transfer, NO remount.
- [ ] After B updates via TestFlight to ≥ the pushed version: `sv.webBundle.installedVersion` is cleared
  on next launch (verify via a temporary dbgLog breadcrumb), and (if the rider shipped) Documents/WebBundle
  is gone and the shipped bundle loads.
- [ ] A director that itself received a pushed pack still advertises its SHELL version only.

**Tests.**
- Extend `e2e/nearby-sync-contract.test.mjs` (source-regex pins, same style as the
  `v != Self.protocolVersion` pin at :221):
  - `assert.match(swiftSource, /installedBundleVersionKey/)` — the persistence key exists;
  - pin that `handleBundleOffer` consults `effectiveBundleVersion` (extract the function body with the
    existing `match(/private func handleBundleOffer[\s\S]*?\n  \}/)` pattern and assert it contains
    `effectiveBundleVersion` and NOT `Int(currentBundleVersion)`);
  - pin that `sendBundleOffer` still uses `currentBundleVersion` (the deliberate asymmetry).
- Behavioral proof is device-gated → M7 2-device day (checklist above). Run the safe subset
  (`node --test e2e/nearby-sync-contract.test.mjs …`) — never the glob.
- Docs edits: `docs/pre-mass-checklist.md` — no ritual change (the fix removes a failure mode). Add one
  line to the M7 device-day script in `docs/major-update-2026-07.md`'s mesh-bundle section if that file
  hosts the script: "reconnect after a bundle push → assert no re-transfer".

**Dependencies.** Ship WITH held M-F3 (defer-install) and FOLNAT-07 (bundle-error surfacing) in WS5-PR4;
all three touch the same install pipeline and are verified by the same device-day scenario. Mooted if the
P-MESH/P-OTA decision retires mesh push — check that decision's status before starting.

---

#### RELVER-02 — Staging-room membership is invisible on-device — a canary iPad left on `?env=staging` silently never follows the director at Mass `medium` `web` `web-only`

**Problem.** `clearInitialUrl()` deliberately preserves `?env=staging` across reloads (correct for the
canary walk), but nothing anywhere renders the fact that a device is pinned to the staging relay room:
no chip, no badge suffix — grep "staging" in `web/src/app.js` finds only the resolver comments and the
clearInitialUrl guard. Worse, with no director in `alvernia-staging` the relay pill hides itself
entirely, so even the pill's absence reads as "normal".

**User impact at Mass.** Sunday: the oldest parish iPad (the checklist's designated canary) opens from a
leftover Safari tab/bookmark carrying the param, looks completely normal, and simply never follows the
live director; ⟳ re-syncs the *staging* room, so the on-screen recovery affordance cannot fix it. Nobody
can diagnose a 4-day-old query param during the entrance hymn.

**Evidence (verify before editing).**
- `web/src/app.js:676` — clearInitialUrl guard: `if (/(?:^|[?&])(env=staging|selftest)(?:=|&|$)/.test(initialUrl.search)) return;`
  — the pin survives reloads by design.
- `web/src/app.js:2846-2857` — `RELAY_ROOM` resolved once at module scope from `location.search` via the
  svRelayRoom helper (triple-guarded IIFE). *(corrected: original cite :3160 is `relay.following = true;`
  inside applyRelaySnap; the room binding anchors are the resolver here plus the URLs below)*
- `web/src/app.js:3168-3169` — `relayStateUrl`/`relayWsUrl` are built from `RELAY_ROOM` — every relay
  operation, including ⟳/pill resync, is room-bound. *(corrected, same note)*
- `web/src/app.js:3038` — `if (!relay.hasDirector) { pill.style.display = "none"; return; }` — in an
  empty staging room the pill is absent, so there is not even an indirect signal. *(added)*
- `web/src/manifest.webmanifest:8` — `"start_url": "/"` — home-screen PWA relaunches are safe; the risk
  vector is Safari tabs/bookmarks kept from the Wed canary walk.
- `web/src/index.html:51` + `web/src/styles.css:2070-2085` — the `#build-badge` element and style block
  the chip will sit beside.

**Fix — step by step.** (Web-only. Note the native shell loads from `file://` with no query string, so
`RELAY_ROOM` is always `alvernia-main` there — the chip can never appear on native; zero native risk.)
1. `web/src/lib/svRelayRoom.js` — add a pure helper next to `resolveRelayRoom` and export it:
   ```js
   // A device on any non-production room must SAY so on screen (RELVER-02).
   function shouldShowRoomChip(room) { return room !== PRODUCTION_ROOM; }
   ```
2. `web/src/index.html:51` — add a sibling of the badge:
   `<div class="room-chip" id="room-chip" hidden>MODO PRUEBA</div>`
   Exact copy: `"MODO PRUEBA"`.
3. `web/src/styles.css` (append near the `.build-badge` block at :2070) — high-contrast, always-visible,
   NOT pointer-events:none (make it tappable later if wanted, but no handler now):
   ```css
   .room-chip { position: fixed; bottom: max(6px, env(safe-area-inset-bottom, 0px));
     right: 56px; z-index: 200; padding: 2px 8px; border-radius: 6px;
     background: #b45309; color: #fff; font-size: 11px; font-weight: 700;
     letter-spacing: 0.04em; }
   .room-chip[hidden] { display: none; }
   ```
4. `web/src/app.js` — immediately after the `RELAY_ROOM` IIFE (:2857), wire it (guarded like everything
   at module scope):
   ```js
   try {
     const chip = document.getElementById("room-chip");
     const show = globalThis.svRelayRoom?.shouldShowRoomChip
       ? globalThis.svRelayRoom.shouldShowRoomChip(RELAY_ROOM)
       : RELAY_ROOM !== "alvernia-main";
     if (chip && show) chip.hidden = false;
   } catch (_) { /* the chip must never affect boot */ }
   ```
5. **D-WS5-2 (Miguel — recommended yes): 24 h auto-expiry.** Inside `resolveRelayRoomSafely`
   (app.js:2846-2857), after resolving `room === "alvernia-staging"`: read/write
   `localStorage["sv.stagingPinAt"]` (first-seen epoch ms). If `now - first > 24*3600*1000`: strip the
   param via `history.replaceState`, clear the key, set a module flag, and return `"alvernia-main"`;
   after boot settles, show a one-line toast with exact copy `"Volviendo al modo normal"` (reuse the
   flash/toast pattern; a `flashSongDisplay`-style transient is fine). When the param is absent, clear
   the key. All inside the existing try/catch — a storage failure must fall through to the pinned room,
   never to a throw.

**Acceptance criteria.**
- [ ] Open the reader with `?env=staging`: "MODO PRUEBA" chip visible next to the build badge; reload —
  still visible.
- [ ] Open without the param (prod room): chip absent; `document.getElementById("room-chip").hidden === true`.
- [ ] (If D-WS5-2 approved) With `sv.stagingPinAt` backdated > 24 h: boot lands on the prod room, URL is
  cleaned, toast "Volviendo al modo normal" appears once.
- [ ] Native file:// boot (or simulated `NATIVE_FILE_MODE`): chip never renders.

**Tests.**
- Extend `e2e/svRelayRoom.test.mjs` (pure unit, already in CI at `.github/workflows/ci.yml:67`):
  `shouldShowRoomChip("alvernia-main") === false`, `shouldShowRoomChip("alvernia-staging") === true`,
  hostile input never throws.
- Browser-verify per the M1 pattern (implementation-log): boot with/without the param, zero console
  errors — the boot path is the one thing that must never regress.
- Docs edits:
  - `docs/pre-mass-checklist.md` §A.3 (in the WS5-PR3 rewrite): add checkbox
    `☐ La canaria muestra el chip "MODO PRUEBA" (sala de prueba activa).` and an end-of-walk step:
    `Al terminar la caminata: quita ?env=staging (cierra la pestaña / borra el marcador) y confirma que el chip desaparece.`
  - `docs/pre-mass-checklist.md` §B (at-the-room): add bullet
    `☐ Ningún dispositivo muestra "MODO PRUEBA" (si lo muestra: abre signovivo.com sin parámetros).`
  - `docs/green-day-deploy-runbook.md` Step 2 verify list: add `- The "MODO PRUEBA" chip is visible (device is in the staging room).`

**Dependencies.** None hard. Pairs with RELVER-11 (the chip marks the ROOM; the `-prueba` badge marks the
BUNDLE — the rewritten RELVER-01/12 checklist tells the operator to check both). Land in WS5-PR1.

---

#### RELVER-04 — Web followers have NO version floor and no legible version anywhere on the dashboard `medium` `cross` `multi`

**Problem.** The web fleet check-in sends an opaque `cacheVersion` hash but not the human `BUILD_NUMBER`
that is baked three lines away; the worker stores `cacheVersion` and never renders or compares it; web
readiness is `webReady = webCached && homeScreen` with no version dimension — there is no analogue of
`MIN_SYNC_BUILD` for web shells.

**User impact at Mass.** The one parish device that MUST run web (the old iPad PWA, too old for
TestFlight) can sit on a months-old cached shell — e.g. one predating the build-377 P2-SEQ
dead-director-freeze fix — and the dashboard says "Listo — web en caché". The operator has no way to know
what any web device runs.

**Evidence (verify before editing).**
- `web/src/app.js:2962-2971` — check-in payload: `deviceId, surface, webCached, pagesCached, totalPages,
  homeScreen, cacheVersion` — no `buildNumber` (`cacheVersion` at :2969). *(corrected: :2969, not :2968/:2969-adjacent)*
- `web/src/app.js:220` — `const BUILD_NUMBER = "__BUILD_NUMBER__";` — baked and available.
- `sync-worker/src/index.ts:74` + `:279` — the only two `cacheVersion` references (type + store); never
  rendered, never compared.
- `sync-worker/src/index.ts:452` — `webReady = ds.some((d) => d.webCached && d.homeScreen)`; grants
  "Listo — web en caché" at `:467-468` — no version dimension.
- `sync-worker/src/index.ts:403` — `MIN_SYNC_BUILD = 361` applies only to `nativeBuild`.

**Fix — step by step.** (Two halves; both additive. Ship web half in WS5-PR1, worker half in WS5-PR2.
Order-independent by construction, but PR1-first means the column fills as devices reload.)
1. **Web** (`web/src/app.js:2962-2971`): add to the payload, guarded against the unreplaced-token dev
   case and staging's `-prueba` suffix (RELVER-11):
   ```js
   buildNumber: (BUILD_NUMBER && BUILD_NUMBER[0] !== "_") ? (parseInt(BUILD_NUMBER, 10) || 0) : 0,
   ```
   (`parseInt("381-prueba", 10) === 381`, so a staging canary reports its true number.)
2. **Worker type** (`sync-worker/src/index.ts:64-76`): add `buildNumber?: number; // web shell build (v-badge number)`
   to `FleetDevice`.
3. **Worker `checkin()`** (after :279): additive, clamped exactly like `nativeBuild` (:271-272):
   ```ts
   if (o.buildNumber != null)
     entry.buildNumber = Math.max(0, Math.min(Number(o.buildNumber) || 0, 1000000));
   ```
   Old bodies simply lack the field — `entry.buildNumber` stays undefined. WIRE COMPAT: never required.
4. **Worker floor** (next to :403):
   ```ts
   // Web shells older than this predate the P2-SEQ dead-director-freeze fix (shipped in build 377).
   // Bump alongside MIN_SYNC_BUILD when a future web fix becomes a real floor. (Hand-maintained —
   // see the pre-mass checklist §A note.)
   const MIN_WEB_BUILD = 377;
   ```
5. **Worker classifier** (inside the row map, :449-475; do this as part of the SYNCE2E-07 classifier
   extraction — one refactor): compute `const bestWebBuild = ds.reduce((m, d) => Math.max(m, Number(d.buildNumber) || 0), 0);`
   and split the current `webReady` branch (:467-468) three ways:
   - `webReady && bestWebBuild >= MIN_WEB_BUILD` → `cls "ok"`, action `` `Listo — web en caché · v${bestWebBuild}` ``
   - `webReady && bestWebBuild > 0` → `cls "warn"`, action `` `Recargar signovivo.com (v${bestWebBuild} < v${MIN_WEB_BUILD})` ``
   - `webReady` (no buildNumber reported — an OLD shell by definition once this ships) → `cls "warn"`,
     action `"Recargar signovivo.com (versión desconocida)"`
   Rationale for warn-not-red on the unknown case: during the deploy window every web device is
   version-unknown until it next opens; amber is the honest state and self-heals. The dashboard tolerates
   old check-in shapes by construction (absence ⇒ warn, never a crash).
6. **Web column cell** (:481-487): append the version when known —
   `webReady ? ("✓ inicio" + (bestWebBuild ? ` · v${bestWebBuild}` : "")) : …` (keep the other arms).
   Also render it in RELVER-09's new anonymous-web device table (same helper).
7. **Caveat to encode in a comment** (from the lens write-up): the PWA only re-checks-in when online, so
   the floor catches *stale-but-online* shells (the fixable case); a fully-offline device just ages out
   via the "Visto" column.

**Acceptance criteria.**
- [ ] A web check-in with `buildNumber: 381`, cached + home-screen → row "Listo — web en caché · v381",
  Web cell "✓ inicio · v381".
- [ ] Same with `buildNumber: 375` → amber "Recargar signovivo.com (v375 < v377)".
- [ ] Same with no `buildNumber` field (old-shape body) → amber "Recargar signovivo.com (versión
  desconocida)"; the dashboard renders without error.
- [ ] Native rows unaffected (no `buildNumber` on native check-ins; `nativeBuild` path untouched).

**Tests.**
- Pure unit (with the SYNCE2E-07 extraction): `sync-worker/test/fleet-rows.test.mjs` — the three web
  arms above as table-driven cases.
- Behavioral, local `wrangler dev` (a2 conventions — env-gated, refuses non-local bases): new
  `sync-worker/test/fleet-dashboard.test.mjs`, run by extending the `node --test` line in
  `sync-worker/test/run-a2.sh`: seed a roster via `POST /fleet/roster` (with `RELAY_TEST_FLEET_KEY`),
  POST `/fleet/checkin` bodies with/without `buildNumber`, GET `/fleet-dashboard?k=…`, assert the exact
  action strings above appear (mirror the Slice D dashboard assertion style at `a2.test.mjs:122-135`).
- Web half: no new web test file needed; the payload is fire-and-forget. Verify in the browser that
  `POST /fleet/checkin` carries `buildNumber` (network tab), per the M1 browser-verify pattern.
- Docs edits: `docs/pre-mass-checklist.md` §B version bullet (:49-50) — extend to:
  `Same version as the others (bottom-right build label; web devices also show on the dashboard as "✓ inicio · v<N>").`
  `docs/green-day-deploy-runbook.md` Step 3 "Verify prod" block — add
  `# fleet dashboard: web rows now show "· v<N>" after devices reload`.

**Dependencies.** Worker half rides the SYNCE2E-07 classifier extraction (same function) — land together
in WS5-PR2. Web half lands in WS5-PR1. RELVER-09's web-device table consumes the same version cell.

---

#### RELVER-05 — No surface exposes WHICH web bundle a native device runs — and badge vs crash telemetry use OPPOSITE version precedence `medium` `cross` `multi`

**Problem.** On a native device the badge prefers the injected SHELL build over the web bundle's own
baked `BUILD_NUMBER` (`resolvedBuild = window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || BUILD_NUMBER`), so
the loaded web bundle's identity is shadowed on every iPad. Crash telemetry uses the OPPOSITE precedence
(web `BUILD_NUMBER` first, native fallback). Fleet check-in sends only `nativeBuild`. So when a stale web
bundle is live (mesh-pushed Documents copy, stale `ios/WebBundle` from an alt-script archive — both
known-open), the on-screen badge and the dashboard crash panel's Build column legitimately disagree, and
no surface can reveal the skew.

**User impact at Mass.** The checklist's §B "same version" comparison is performed against the one number
that cannot reveal the stale-bundle failure mode it exists to catch; mid-incident, the badge and the
crash panel disagree and the operator concludes the tooling lies. Concrete skew at HEAD: web 378-381
changed follower-visible spinner behavior; a fleet iPad on native 377 renders the OLD spinner from its
bundled copy while phones render the new one — invisible except by memorized build→behavior mapping.

**Evidence (verify before editing).**
- `web/src/app.js:3479-3481` — badge precedence: `resolvedBuild = window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || (BUILD_NUMBER…)`
  — shell wins; help-panel "Versión" label at :3482-3484 uses the same value.
- `web/src/app.js:2901-2903` — crash precedence: `BUILD_NUMBER` first, `__SIGNO_VINO_NATIVE_BUNDLE_VERSION`
  as fallback — the exact opposite. *(corrected: block is 2901-2903; original cite :2903 is the fallback line)*
- `PdfReaderApp.tsx:191` — fleet check-in body has `nativeBuild` only (fetch body :188-197); no web-bundle
  field exists on the fleet wire.
- `PdfReaderApp.tsx:1033-1042` — `preloadScript` injects `__SIGNO_VINO_NATIVE_BUNDLE_VERSION = BUILD_VERSION`
  (:1037), i.e. the shell's version.json number compiled into the RN bundle — NOT the loaded web bundle's.
- `web/src/app.js:3469-3474` — `bridge-ready` payload: `{type, page, totalPages, book}` — no webBuild.
  *(corrected: payload spans 3469-3474, not 3465-3474)*
- `docs/pre-mass-checklist.md:49` — §B compares devices by "bottom-right build label".

**Fix — step by step.** (Web half ships in WS5-PR1 — reaches phones instantly, iPads at the next
build/bundle push; native half in WS5-PR4.)
1. **Web badge dual rendering** (`web/src/app.js:3479-3492`): replace the block with:
   ```js
   const nativeShellBuild = window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || "";
   const webBundleBuild = (BUILD_NUMBER && BUILD_NUMBER[0] !== "_") ? BUILD_NUMBER : "";
   const resolvedBuild = nativeShellBuild || webBundleBuild;
   // Precedence rule (documented): the BADGE is shell-first with the loaded web bundle
   // appended when it differs ("381·w379"); CRASH telemetry (reportCrash) is web-first,
   // because the crashing code IS the web bundle. Keep both comments in sync.
   const badgeText = nativeShellBuild && webBundleBuild && String(nativeShellBuild) !== String(webBundleBuild)
     ? `${nativeShellBuild}·w${webBundleBuild}`
     : String(resolvedBuild);
   ```
   Use `badgeText` for both the help-panel label (`Versión ${badgeText}`) and the badge (:3490). Pure web:
   `nativeShellBuild` empty → badge unchanged. Native, matched versions → badge unchanged. Only the skew
   state renders differently — which is the point.
2. **Cross-reference comment** at `web/src/app.js:2901` (crash precedence): add
   `// Web-first ON PURPOSE (opposite of the badge): the crashing code is the web bundle. See the badge block (~:3479) for the rule.`
3. **bridge-ready additive field** (`web/src/app.js:3469-3474`): add
   `webBuild: (BUILD_NUMBER && BUILD_NUMBER[0] !== "_") ? BUILD_NUMBER : "",`
   WIRE COMPAT: old shells (368-381) ignore unknown bridge-ready fields (the handler destructures known
   keys only) — verified safe.
4. **Native half (WS5-PR4)** (`PdfReaderApp.tsx`): in the `bridge-ready` bridge-message handler, store the
   new field: `webBuildRef.current = String(msg.webBuild || "")` (new `useRef("")`). In `fleetCheckin`
   (:188-197) add additively:
   ```ts
   ...(webBuildRef.current ? { webBuild: parseInt(webBuildRef.current, 10) || 0 } : {}),
   ```
5. **Worker (rides WS5-PR2 or WS5-PR4's worker follow-up)**: `FleetDevice` gains `webBuild?: number`;
   `checkin()` stores it clamped like `nativeBuild`; the native App cell (index.ts:480) renders
   `` `381 (web 379)` `` when `webBuild` differs from `nativeBuild`, plain `381` otherwise. Dashboard copy
   exactly: `381 (web 379)`. Absent field (all builds ≤ current) → plain number; old shapes tolerated.
6. **Document the grammar once** (this finding defines it for M4's pill / M7's DIAGNÓSTICO): shell number
   first, `·w<web>` suffix only on skew, `v` prefix only on pure-web surfaces. Put this sentence in the
   badge comment (step 1) — it is the single source the later milestones must follow.

**Acceptance criteria.**
- [ ] Pure web (signovivo.com): badge identical to today (`381`).
- [ ] Native, shell == loaded web bundle: badge identical to today.
- [ ] Native, shell 381 + loaded web bundle 379 (simulate: temporarily hand-edit the injected global in
  Safari web-inspector against a local file build, or stage a stale `ios/WebBundle`): badge reads
  `381·w379`; crash payload `build` reads `379`.
- [ ] After WS5-PR4: a native check-in carries `webBuild`; dashboard App cell reads `381 (web 379)` on
  skew; old-shape check-ins render a plain number.

**Tests.**
- `e2e/native-entrypoint.test.mjs` (already reads `PdfReaderApp.tsx` at :20): pin the additive check-in
  field — `assert.match(source, /webBuild/)` scoped to the fleetCheckin block.
- Worker behavioral: extend `sync-worker/test/fleet-dashboard.test.mjs` (from RELVER-04) with one
  check-in carrying `{nativeBuild: 381, webBuild: 379}` → dashboard contains `381 (web 379)`.
- Web: browser-verify the three badge states (the M1 pattern); no new unit file (DOM-coupled).
- Docs edits: `docs/pre-mass-checklist.md:49` — extend the §B bullet:
  `Same version as the others (bottom-right build label; if it reads "381·w379" the iPad is running an old web bundle — pull it aside).`

**Dependencies.** None hard for the web half (WS5-PR1). Native+worker halves ride WS5-PR4/PR2. RELVER-06
removes the biggest *creator* of the skew this makes visible — ship both so the badge stops needing to
show `·w` at all.

---

#### RELVER-09 — #270 quietly killed roster matching for ALL web devices — the web-only follower reads "No se ha visto — invitar" while its device sits cached in the orphan table `medium` `cross` `worker-only`

**Problem.** Since #270 (build 378 web) the web check-in payload carries no `label` field and no code
path can ever set one again; the dashboard maps devices to roster people by normalized-label equality, so
a label-less device can never match anyone. Server-side coalescing keeps OLD labels alive until
`/fleet/reset` ("for a fresh season") — after which every web device is permanently an anonymous
"(sin nombre)" orphan while its roster person renders red "No se ha visto — invitar".

**User impact at Mass.** At the pre-Mass glance the dashboard contradicts itself: the old-iPad-PWA person
shows permanently red "never seen, invite them" while the very same iPad appears rows below as an
anonymous orphan reading "✓ inicio" — the operator can't trust per-person readiness for anyone on web.

**Evidence (verify before editing).**
- `web/src/app.js:2962-2971` — post-#270 payload has no `label`; commit `3db3a5ba` (#270) deleted the
  modal + `FLEET_LABEL/ROLE/SKIP` keys and scoped itself web-only ("Native iPads never showed this
  modal… web-PWA-only cleanup").
- `sync-worker/src/index.ts:443-446` — `devicesFor(name)` matches by `normName(d.label) === normName(p.name)`;
  empty label can never match.
- `sync-worker/src/index.ts:266` — `label: String(o.label ?? prev.label ?? "")` — coalescing keeps old
  labels until reset (`resetFleet` :300-303) or ring-cap eviction (:281-287).
- `sync-worker/src/index.ts:459-461` — `ds.length === 0` → `cls "bad"` (:460), action
  `"No se ha visto — invitar"` (:461).
- `sync-worker/src/index.ts:507-515` — unmatched devices render in the "Sin coincidencia en la lista"
  orphan table as `(sin nombre)`.
- `sync-worker/src/index.ts:452` + `:467-468` — the per-person `webReady` branch is dead code for any
  post-reset fleet.

**Fix — step by step.** (Worker-only render fix, instant; the durable end-state is **D-WS5-1**.)
1. In `renderFleetDashboard` compute once, near :432:
   ```ts
   const anonWeb = devices.filter((d) => d.surface === "web" && !normName(d.label));
   ```
2. **First-class web-device table** (replaces web devices' appearance as accidental "orphans"): render —
   above the existing orphan table — a dedicated table for `anonWeb` when non-empty. Heading copy exactly:
   `Dispositivos signovivo.com (anónimos)` with columns `Dispositivo` (deviceId, first 8 chars,
   `escHtml`'d), `Web` (reuse the RELVER-04 cell: `✓ inicio · v381` / `caché, sin inicio` / `parcial`),
   `Visto` (`ago(d.ts)`), `Qué hacer` (the RELVER-04 web actions). Keep the existing orphan table for
   *labeled*-but-unmatched devices (typos/guests) by excluding `anonWeb` from it:
   change :507 to `const orphans = devices.filter((d) => !rosterNames.has(normName(d.label)) && !(d.surface === "web" && !normName(d.label)));`
3. **Stop the contradiction on person rows**: at :459-461, when the fleet has anonymous web check-ins,
   don't imply the person was never seen ANYWHERE. Replace the action with:
   ```ts
   action = anonWeb.length
     ? "No se ha visto (los dispositivos web ya no reportan nombre — ver tabla abajo)"
     : "No se ha visto — invitar";
   ```
   Keep `cls = "bad"` (the person's readiness genuinely is unknown), keep "invitar" when no anonymous web
   devices exist (a person who truly never onboarded).
4. Add a one-line note under the person table (`.sub` style):
   `Desde build 378 los dispositivos web se reportan sin nombre; aparecen abajo por dispositivo.`
5. **D-WS5-1 (Miguel)** — the heavier, durable option: `RosterPerson` gains optional
   `deviceIds?: string[]`; `putRoster()` sanitizes them (≤64 chars each, ≤4 per person); `devicesFor`
   matches by deviceId FIRST, label second. You seed the ids once from the new web-device table. This
   restores named per-person web readiness with no on-device prompt, and it is what makes FOLNAT-05's
   prompt removal permanent. Recommended, but it changes the roster-seeding ritual → decision first;
   implement in the M6 dashboard rework if approved.
6. WIRE COMPAT: pure render change; `checkin()` untouched; old and new check-in shapes both render.

**Acceptance criteria.**
- [ ] Local `wrangler dev`: `POST /fleet/reset` (with key) + one anonymous web check-in → dashboard has
  NO row that simultaneously claims a cached web device exists and that its person was "never seen —
  invite"; the web device appears under "Dispositivos signovivo.com (anónimos)" with `ago` + cache state
  (+ `v<N>` once RELVER-04 lands).
- [ ] A labeled-but-typo'd NATIVE device still lands in "Sin coincidencia en la lista".
- [ ] A roster with no anonymous web devices still renders "No se ha visto — invitar".

**Tests.**
- Pure unit: fold into `sync-worker/test/fleet-rows.test.mjs` (SYNCE2E-07's extraction) — the
  person-action arm: `(ds.length === 0, anonWebCount > 0)` → the no-contradiction string.
- Behavioral (`wrangler dev`, a2 conventions): extend `sync-worker/test/fleet-dashboard.test.mjs` —
  seed roster person "Rita" + anonymous web check-in → assert HTML contains
  `Dispositivos signovivo.com (anónimos)` and does NOT contain `No se ha visto — invitar`; then reset +
  roster-only → assert `invitar` returns.
- Docs edits: `docs/pre-mass-checklist.md` §B — add:
  `El iPad viejo (PWA) aparece en el tablero como dispositivo anónimo de signovivo.com — revisa su fila por dispositivo, no por persona.`

**Dependencies.** Land with SYNCE2E-07 + RELVER-04 (same `renderFleetDashboard` refactor, one worker
deploy — WS5-PR2). FOLNAT-05 must NOT land before this (it would push native devices into the same
contradiction). D-WS5-1 decides the durable model.

---

#### RELVER-12 — Checklist canary steps A.3.3–A.3.5 are impossible on the staging canary — director promotion, mesh follow, and the restart test cannot run on web `medium` `cross` `web-only`

**Problem.** §A.3 instructs, on the staging WEB canary: "Become director on the canary → a 2nd device
follows the page over the mesh", then a director force-quit/restart test. On web, a 5+-digit code without
a native bridge dead-ends at "Código no válido" — director promotion exists only behind the native
bridge; the mesh is native-only Swift Multipeer; and the native app cannot load staging content (native
staging entry is deferred to M7) while its relay publisher is hardcoded to the LIVE room.

**User impact at Mass.** An operator honestly following the safety ritual hits a dead end and either
skips the director/mesh/restart checks — so the surfaces where every historical outage lived get
validated by NOTHING before prod — or improvises with the native app, which silently publishes into the
live Mass room. A pre-Mass trap wearing a safety-procedure costume.

**Evidence (verify before editing).**
- `docs/pre-mass-checklist.md:27` — "☐ Become director on the canary → a 2nd device follows the page over
  the mesh." on a staging web canary.
- `docs/pre-mass-checklist.md:28-29` — the restart test presupposes the native director step 3 cannot
  create.
- `web/src/app.js:1189` — pure-web 5+-digit entry → `flashSongDisplay("Código no válido", "err")` (native
  branch :1183-1186; length gate :1178) — no web director promotion exists.
- `src/directorRelaySync.js:13` — `const RELAY_ROOM = "alvernia-main";` — a native improvisation
  publishes into the live room (known prior-art oddity; cited here as the hazard, not re-claimed).

**Fix — step by step.** (Docs-only; lands in WS5-PR3 as the same §A.3 rewrite RELVER-01 starts.)
1. Replace `docs/pre-mass-checklist.md` step A.3 (lines 23-32) with two subsections. Proposed text:
   ```markdown
   3. **Canary-walk on ONE device — the oldest iPad** (worst case first), in Safari at EXACTLY
      `https://staging.alvernia-reader.pages.dev/?env=staging` (preview CONTENT + staging ROOM —
      never signovivo.com: that host always serves PROD).
      **What the web canary CAN prove:**
      - ☐ Badge reads `<N>-prueba` and the "MODO PRUEBA" chip is visible (you ARE on the new bytes,
        in the test room).
      - ☐ App opens to the reader — a real page renders, **not a blank/white screen**.
      - ☐ Page-turn is snappy (< ~⅓ second warm).
      - ☐ `?selftest` appended → the readiness card is GREEN.
      - ☐ Relay follow works in the staging room: open the SAME preview URL in a second browser tab
        (laptop is fine), publish a page into `alvernia-staging` with the test client or a second
        canary phone, and confirm the iPad follows; ⟳ re-syncs it.
      - **Any box fails → STOP.** The group is still on the old build; nothing reached them.
      **What the web canary CANNOT prove (native-only — do at practice, on the LIVE room):**
      - Director promotion (code entry), mesh follow, and the director force-quit/restart test
        require the NEW TestFlight build on a real iPad. The native app has no staging entry yet
        (lands with M7) and publishes to the LIVE Mass room — so run these checks at Wed/Sat
        practice with the group present, right after the iPads take the TestFlight update
        (step 5), never alone on a Mass day.
   4. …(unchanged promote step)…
   ```
   Adjust the "restart test" sentence (old :28-29) into the native subsection verbatim, keeping the
   "(once the M7 build lands…)” resume-prompt note.
2. Add a closing line to §A: `When M7 lands native staging entry, fold the native checks back into the
   staging walk.` (mirrors the finding's fold-back clause).
3. `docs/green-day-deploy-runbook.md` Step 2 — the verify list already contains only web-provable items;
   add the same one-line CANNOT note pointing at practice for native checks.
4. Note for the relay-follow checkbox: publishing into `alvernia-staging` requires a publisher. Document
   the safe path: `sync-worker/test-client.html` with the Bearer token against the staging room, or
   `curl -s -X POST "$RELAY/r/alvernia-staging/publish" -H "X-Director-Code: <code>" -H 'Content-Type: application/json' -d '{"v":1,"page":42,"totalPages":371,"seq":'"$(date +%s000)"'}'`.
   Never the live room; never `e2e/relay-sync.test.mjs`.

**Acceptance criteria.**
- [ ] No checklist step is impossible on the device class it names (read-through: every ☐ under the web
  subsection is executable in Safari; every native check is explicitly routed to practice + TestFlight).
- [ ] The improvisation hazard is named in the doc ("publishes to the LIVE Mass room").

**Tests.**
- Extend `e2e/release-ritual-docs.test.mjs` (from RELVER-01): assert the checklist does NOT contain
  `Become director on the canary` (the impossible phrasing) and DOES contain `CANNOT` (the split exists).
- Docs edits: this finding IS the docs edit (`docs/pre-mass-checklist.md`,
  `docs/green-day-deploy-runbook.md`).

**Dependencies.** Same PR as RELVER-01 (one §A.3 rewrite — WS5-PR3), after WS5-PR1 ships the `-prueba`
badge (RELVER-11) + chip (RELVER-02) the new checkboxes reference. When M7 lands native staging entry,
revisit (fold-back clause).

---

#### SYNCE2E-07 — Fleet dashboard marks the REAL director "not ready" whenever ANY checked-in device carries a newer build — guaranteed false red during every canary window `medium` `worker` `worker-only`

**Problem.** `maxBuild` reduces over ALL stored check-ins — canary iPad, Miguel's dev phone, simulator
runs (every `npm run ios` bumps the build and checks in to the PROD fleet DO with no `__DEV__` gate),
unmatched guests — and the director row demands `bestBuild >= latest = max(maxBuild, MIN_SYNC_BUILD)`.
The real compat floor is `MIN_SYNC_BUILD = 361`; "director == fleet max" is a stricter cosmetic rule the
sanctioned canary ritual violates by design. Check-ins have no TTL, so one stray high-build check-in
poisons the row until `/fleet/reset`.

**User impact at Mass.** During every canary window — exactly the pre-Mass moments the dashboard exists
for — the actual, perfectly-current director shows red "⚠ Director en build 377 (debe ser 381)" and is
counted in "por contactar". Cry-wolf reds train the operator to ignore the dashboard.

**Evidence (verify before editing).**
- `sync-worker/src/index.ts:433-434` — `maxBuild = devices.reduce(max over ALL check-ins)`;
  `latest = Math.max(maxBuild, MIN_SYNC_BUILD)` — orphans/guests included (the orphan filter at :507 only
  affects the orphan table).
- `sync-worker/src/index.ts:462-464` — director row red unless `bestBuild >= latest`:
  `` `⚠ Director en build ${bestBuild || "—"} (debe ser ${latest})` ``.
- `sync-worker/src/index.ts:567` — footer repeats `Director debe estar en ${latest}`.
- `sync-worker/src/index.ts:403` — `MIN_SYNC_BUILD = 361` is the real floor.
- Feeders: `docs/pre-mass-checklist.md:23-33` institutionalizes a newer-build canary before the director
  updates; `PdfReaderApp.tsx:182-201` checks in to the hardcoded prod `RELAY_BASE` (:45) with no
  `__DEV__` guard; root `package.json:8` `preios` bumps the build on every local run.

**Fix — step by step.** (Worker-only; the classifier extraction here is the shared refactor RELVER-04/-09
build on.)
1. **Extract the row classifier to a pure module**: new `sync-worker/src/fleetRows.mjs` (dependency-free
   ESM) exporting `computeFleetLatest(devices, rosterNames, nowSec)` and
   `classifyPersonRow(person, ds, ctx)` → `{cls, action}` — move the logic of :433-434 and :449-475 there;
   `index.ts` imports and calls it. Add `sync-worker/src/fleetRows.d.mts` with the types and extend
   `sync-worker/tsconfig.json` `include` to `["src/**/*.ts", "src/**/*.d.mts"]` so `npm run typecheck`
   (in `sync-worker/`) still passes. (Wrangler's esbuild bundles the `.mjs` import fine.) If this
   plumbing fights you, fallback: keep the functions exported from `index.ts` and rely on the behavioral
   `wrangler dev` tests only — but the pure module is preferred (three findings share it, and it unit-tests
   in CI with no DO).
2. **Recency + roster filter for `latest`** in `computeFleetLatest`:
   ```js
   const RECENT_S = 48 * 3600; // what the fleet runs NOW; ages a Wed canary out by Fri
   const recentMatched = devices.filter((d) =>
     (nowSec - (Number(d.ts) || 0)) < RECENT_S && rosterNames.has(normName(d.label)));
   const maxBuild = recentMatched.reduce((m, d) => Math.max(m, Number(d.nativeBuild) || 0), 0);
   return Math.max(maxBuild, MIN_SYNC_BUILD);
   ```
   (Pass `normName` + `MIN_SYNC_BUILD` in via `ctx` or params to keep the module pure.) The roster-match
   filter drops the dev phone/simulator (label-less); 48 h is the canonical recommendation — note the
   RELVER-03 lens variant proposed 7 days; 48 h is preferred because it also ages the *roster-matched*
   canary iPad out before Sunday.
3. **Director never red on build alone**: replace :462-464 with:
   ```ts
   if (bestBuild >= MIN_SYNC_BUILD) {
     cls = "ok";
     action = bestBuild >= latest
       ? `Listo — build ${bestBuild}`
       : `Listo — build ${bestBuild} · hay una build más nueva (${latest}, ¿canary?)`;
     if (bestBuild < latest) cls = "warn";   // amber hint, never red, never "por contactar"
   } else {
     cls = "bad";
     action = `⚠ Director en build ${bestBuild || "—"} (mínimo ${MIN_SYNC_BUILD})`;
   }
   ```
   Exact operator-visible strings (match existing es style):
   `Listo — build 377 · hay una build más nueva (381, ¿canary?)` (warn) and
   `⚠ Director en build — (mínimo 361)` (bad).
4. **Footer** (:567): change the last sentence to `Director: build ≥ ${MIN_SYNC_BUILD}.` (drop the
   "debe estar en ${latest}" claim).
5. WIRE COMPAT: render-only; no check-in shape change; old shapes classify identically (missing
   `nativeBuild` → 0 → unchanged arms).

**Acceptance criteria.** (From the finding, verbatim behavior.)
- [ ] Seed roster + check-ins where director=377 (fresh), one roster follower=381 (fresh): director row is
  AMBER with the canary wording, counted in "por revisar", not "por contactar".
- [ ] All devices ≥ MIN_SYNC_BUILD and equal builds → everyone "Listo" (green), footer says
  `Director: build ≥ 361.`
- [ ] One label-less check-in at build 999 (simulated dev device): director row unaffected.
- [ ] One roster-matched check-in at 381 with `ts` 3 days old: director at 377 is plain green (the canary
  aged out).
- [ ] Director at 355 (< 361): red `⚠ Director en build 355 (mínimo 361)`.

**Tests.**
- Pure unit: new `sync-worker/test/fleet-rows.test.mjs` — table-driven over
  `{roster, devices, nowSec}` covering the five acceptance cases (no env gate needed — pure module, no
  network; run directly with `node --test sync-worker/test/fleet-rows.test.mjs`). Add the file to
  `sync-worker/test/run-a2.sh`'s `node --test` line, and optionally to the CI named list at
  `.github/workflows/ci.yml:60-69` (it needs no wrangler — safe in CI; full worker CI wiring is
  RELVER-08's scope, another workstream).
- Behavioral (`wrangler dev`): `sync-worker/test/fleet-dashboard.test.mjs` (shared with RELVER-04/-09):
  seed director 377 + follower 381 via `/fleet/roster` + `/fleet/checkin`, GET `/fleet-dashboard?k=…`,
  assert `hay una build más nueva` present and `debe ser` absent.
- Docs edits: `docs/pre-mass-checklist.md` §A.3 (WS5-PR3 rewrite) — add an interpretation note:
  `Durante la semana de canary es NORMAL que el director salga ámbar ("hay una build más nueva") — rojo significa build < 361.`

**Dependencies.** The classifier extraction is the base RELVER-04 and RELVER-09 build on — do this first
within WS5-PR2. Optional native rider (not this finding, note only): a `__DEV__` guard on
`PdfReaderApp.tsx:182-201`'s check-in would stop simulator pollution at the source — one line in WS5-PR4.

---

#### RELVER-07 — Worker deploy identity is invisible — `/health` returns only the frozen wire version, so nobody can confirm which worker code is live `low` `worker` `worker-only`

**Problem.** `/health` returns `{ok, service, v: PROTOCOL_VERSION}` and `PROTOCOL_VERSION` is pinned `=1`
forever by the additive-only wire contract — it can never distinguish deploys. Worker deploys are manual
`npx wrangler deploy`, untied to version.json; the 2026-07-05 A2 deploy had to be verified by probing
rate-limit *behavior*.

**User impact at Mass.** Pre-Mass or mid-incident the operator can read a badge for web and native but
has zero way to confirm which worker build serves the room — the surface with instant fleet-wide blast
radius; `wrangler rollback` verification is equally blind.

**Evidence (verify before editing).**
- `sync-worker/src/index.ts:585-587` — `/` and `/health` return
  `{ ok: true, service: "signovivo-sync", v: PROTOCOL_VERSION }`.
- `sync-worker/src/index.ts:29` — `const PROTOCOL_VERSION = 1;` (wire version; additive-only contract).
- `sync-worker/wrangler.jsonc:3` — "Deploy: `npx wrangler deploy`" (manual); vars block :17-21 contains
  only `ALLOWED_ORIGINS` — no deploy stamp anywhere. *(corrected: original cite :15 is the secrets
  comment; the manual-deploy comment is :3, vars are :17-21)*
- `grep -n "DEPLOY_ID\|GIT_SHA" sync-worker/src/index.ts` → zero hits.

**Fix — step by step.**
1. New `sync-worker/deploy.sh` (bash-only, mirrors `test/run-a2.sh` conventions; do NOT add an npm
   script — root `package.json` scripts are pinned by `e2e/repo-minimal-footprint.test.mjs`, and
   sync-worker's own scripts don't need to change):
   ```bash
   #!/usr/bin/env bash
   # Blessed worker deploy: stamps the deploy with an identity /health can echo.
   set -euo pipefail
   cd "$(dirname "$0")"
   SHA=$(git rev-parse --short HEAD 2>/dev/null || echo nogit)
   TS=$(date -u +%Y%m%dT%H%M%SZ)
   DEPLOY_ID="${SHA}-${TS}"
   echo "→ deploying signovivo-sync with DEPLOY_ID=${DEPLOY_ID}"
   npx wrangler deploy --var DEPLOY_ID:"${DEPLOY_ID}"
   echo "→ verify:  curl -s https://signovivo-sync.4j4982y8jp.workers.dev/health"
   echo "           expect: \"deploy\":\"${DEPLOY_ID}\""
   ```
2. `sync-worker/src/index.ts` — `Env` (:14-27) gains
   `/** Deploy stamp set by deploy.sh via --var; empty = deployed outside the wrapper. */`
   `DEPLOY_ID?: string;`
3. `/health` (:586) — ADDITIVE field:
   `return json({ ok: true, service: "signovivo-sync", v: PROTOCOL_VERSION, deploy: env.DEPLOY_ID || "" }, 200, cors);`
   WIRE COMPAT: nothing parses `/health` strictly (svSelftest checks the relay via `/state`; the runbook
   curls it for eyeballs) — additive is safe. `deploy: ""` is itself a signal: "deployed without the
   wrapper".
4. Fleet-dashboard header: pass the id through — `renderFleetDashboard(data, k, crashes, deployId = "")`
   and at the call site (:716-727 region) pass `env.DEPLOY_ID || ""`. Render in the `.sub` footer line
   (:567), appended: `` ` · relay ${escHtml(deployId || "sin identificar")}` ``. Operator-visible copy
   exactly: `· relay a1b2c3d-20260709T120000Z` (or `· relay sin identificar`).
5. A dirty-tree caveat comment in deploy.sh: the sha identifies HEAD, not uncommitted edits — same
   provenance caveat as Pages `--commit-dirty` (prior-art O12); commit before deploying.

**Acceptance criteria.**
- [ ] `bash sync-worker/deploy.sh` against LOCAL dev config → `curl :8787/health` shows a non-empty
  `deploy` matching the echoed id. (For prod: run the wrapper on the next real deploy; verify the same.)
- [ ] Plain `npx wrangler deploy` still works and `/health` shows `deploy:""` (fail-open).
- [ ] Dashboard footer shows `· relay <id>`.

**Tests.**
- Local `wrangler dev` recipe (a2 conventions): `wrangler dev` reads `.dev.vars` — add
  `DEPLOY_ID=dev-local` to `sync-worker/.dev.vars` (gitignored) and assert in
  `sync-worker/test/fleet-dashboard.test.mjs`: `GET /health` body has `ok:true` AND a string `deploy`
  field (tolerate empty for runners without the var); dashboard HTML contains `· relay`.
- Extend `sync-worker/test/a2.test.mjs`'s health check if one exists (baseline test hits publish/state
  only — add the `/health` shape assertion to the new dashboard test file instead; keep a2.test.mjs
  focused).
- Docs edits: `docs/green-day-deploy-runbook.md` Step 1 — change the deploy command from
  `cd sync-worker && npx wrangler deploy && cd ..` to `bash sync-worker/deploy.sh`, and add to the verify
  block: `curl -s …/health   # expect "deploy":"<the id deploy.sh just printed>"`. Also the rollback
  note: after `npx wrangler rollback`, `/health`'s `deploy` reverts to the previous id — that IS the
  rollback verification.

**Dependencies.** None. Rides WS5-PR2's worker deploy (stamp that very deploy with the first id).

---

#### RELVER-11 — Staging/canary bundle is badge-indistinguishable from prod — no positive confirmation the canary device is executing the new bytes `low` `web` `web-only`

**Problem.** `STAGING=1 release.sh` deliberately skips the version bump and builds at the CURRENT
version, so the canary badge renders the same `v<N>` as prod. The only differing identity is the opaque
`CACHE_VERSION` hash on the `?selftest` card — two hashes a human must diff by eye. Combined with
RELVER-01's wrong-URL trap, nothing anywhere positively confirms the canary device is executing the new
bytes.

**User impact at Mass.** The canary walk's first checkbox ("app opens to the reader") proves nothing
about WHICH bundle opened; a wrong URL or a stale SW cache passes the whole walk against old code.

**Evidence (verify before editing).**
- `scripts/release.sh:38-41` — STAGING path: "skip bump; build web at the CURRENT version".
- `web/src/app.js:3488-3492` — badge renders bare `resolvedBuild`, no channel marker.
- `web/src/lib/svSelftest.js:51` — the selftest "Versión" detail `"v" + buildLabel + " · " + cacheVersion`
  — the hash suffix is the only differing identity today.
- `web/build.mjs:32-39` + `:64-68` — `buildNumber` read from version.json and baked via
  `replaceAll("__BUILD_NUMBER__", buildNumber)`; no staging flag exists in build.mjs
  (`grep -n "STAGING" web/build.mjs` → none).
- Consumers of the baked token (all tolerate a `-prueba` suffix): badge :3479-3492 (string), crash
  telemetry :2901-2903 (string field — a canary crash self-identifies, a bonus), selftest :3562 (string),
  RELVER-04's new check-in field (uses `parseInt`, so `381-prueba` → `381` — by design).

**Fix — step by step.**
1. `web/build.mjs:32-39` — after computing `buildNumber`, append the channel token:
   ```js
   // RELVER-11: a staging/canary build must be visually distinct on the badge.
   // SV_STAGING=1 is exported ONLY by `STAGING=1 release.sh`; the prod path stays byte-identical.
   const badgeNumber = process.env.SV_STAGING === "1" ? `${buildNumber}-prueba` : buildNumber;
   ```
   and change :68 to `.replaceAll("__BUILD_NUMBER__", badgeNumber)`.
   Exact badge token: `381-prueba` (the operator-visible marker; es "prueba" = test).
2. `scripts/release.sh:31-33` — inside the `if [ "$STAGING" = "1" ]` block add `export SV_STAGING=1`
   (build step at :50 inherits it). Prod and `SKIP_NATIVE=1` paths never set it → byte-identical output
   (satisfies the "physically incapable" comment at :27-29).
3. `scripts/smoke-boot.mjs` compatibility check: the smoke test asserts NO unreplaced `__*__` tokens —
   a replaced `-prueba` value passes; CI never sets `SV_STAGING`, so CI artifacts stay pure-numeric.
   Verify smoke-boot doesn't assert the badge is numeric (it doesn't — it checks tokens and structure).
4. Interplay note (encode as a comment in build.mjs): the `-prueba` badge marks the BUNDLE channel;
   RELVER-02's chip marks the ROOM. A correct canary shows BOTH (`381-prueba` + "MODO PRUEBA"); a
   leftover Sunday device would show the chip but a normal badge (room pinned, prod bundle) — each marker
   catches the seam the other can't.

**Acceptance criteria.**
- [ ] `SV_STAGING=1 node web/build.mjs && grep -o '"[0-9]*-prueba"' web/dist/app.js` → the badge constant
  carries `-prueba`; opening the built dist shows badge `381-prueba` and selftest `v381-prueba · <hash>`.
- [ ] `node web/build.mjs` (no env) → `git diff`-level identical badge value to today (`381`).
- [ ] `bash -n scripts/release.sh` passes; the STAGING echo (RELVER-01) mentions the `-prueba` badge.

**Tests.**
- Extend `scripts/smoke-boot.mjs`? No behavior change on the prod path — instead add one case to
  `e2e/release-ritual-docs.test.mjs` (RELVER-01's file): source-pin that `web/build.mjs` contains
  `SV_STAGING` and `-prueba`, and `scripts/release.sh` exports `SV_STAGING=1` only inside the STAGING
  branch (regex: `/if \[ "\$STAGING" = "1" \][\s\S]*?export SV_STAGING=1/`).
- Manual verify (safe, no deploy): the two build commands in the acceptance criteria.
- Docs edits: `docs/pre-mass-checklist.md` §A.3 first checkbox (in the WS5-PR3 rewrite):
  `☐ Badge reads <N>-prueba…` (already in RELVER-12's proposed text);
  `docs/green-day-deploy-runbook.md` Step 2 verify list: add `- Badge bottom-right reads "<N>-prueba" — positive proof the canary bytes loaded.`

**Dependencies.** Ships in WS5-PR1 with RELVER-02 (the paired marker); referenced by the WS5-PR3 docs
(RELVER-01/-12). Note the badge on a native canary is unaffected (native injects the shell number —
RELVER-05), which is fine: native has no staging entry until M7.

---

#### FOLNAT-05 — Native first-run still shows the "¿Quién usa este iPad?" Alert.prompt that #270 removed from web as "more annoying than useful" — and it stacks with the critical Local Network permission dialog `low` `native` `native-build`

**Problem.** The native device-id mount effect still fires a one-time `Alert.prompt("¿Quién usa este
iPad?")` on iOS first launch, while `primeNearbyPermissions()` fires from the mesh bootstrap effect on
the same mount — so the iOS Local Network dialog and the label prompt contend back-to-back before the
user sees a page. #270 removed the identical web modal ("choir members mostly tapped 'Ahora no'") and
scoped itself web-only explicitly — the native twin was left by scope, not by decision. The first fleet
check-in is also delayed behind the prompt's resolution.

**User impact at Mass.** A fresh parish iPad's first launch presents two modal decisions back-to-back;
the annoyance prompt primes an elderly user to dismiss/deny the one dialog that matters (Local Network —
the FOLNAT-02 stranded-forever trap).

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:220-245` — the prompt block: guard `if (!label && !skip && Platform.OS === "ios")` at
  :220, `Alert.prompt(` at :222 ("¿Quién usa este iPad?", "Para el tablero del coro — una sola vez.",
  buttons "Ahora no"/"Guardar"). `grep -n "Alert.prompt" PdfReaderApp.tsx` → exactly one hit. *(corrected:
  original cite :220 is the guard; the call is :222)*
- `PdfReaderApp.tsx:251` — `fleetCheckin();` runs in the same async IIFE AFTER the prompt resolves — the
  first check-in waits on the modal.
- `PdfReaderApp.tsx:851` — `primeNearbyPermissions().catch(() => {});` in the mesh bootstrap effect on
  the same mount (gated only by `syncAvailable`, true on parish iPads).
- `web/src/app.js:2985-2987` — the web comment records the removal + rationale; commit `3db3a5ba` (#270)
  says "Native iPads never showed this modal… web-PWA-only cleanup" — left by scope.
- `PdfReaderApp.tsx:218` — `sv_fleet_label` is still read; the dashboard's roster matching (RELVER-09)
  is the only consumer of what the prompt collects.

**Fix — step by step.** (Native build — WS5-PR4. Blocked on **D-WS5-1**: this makes native labels die for
NEW installs, so RELVER-09's dashboard fix must land first, and the deviceId-pin option decides the
durable model.)
1. Delete the prompt block `PdfReaderApp.tsx:216-246` — but KEEP the `sv_fleet_label` READ:
   ```ts
   // Fleet identity: the one-time name prompt was removed (mirrors web #270 — "more annoying
   // than useful", and it contended with the Local Network permission dialog on first run).
   // A label saved by an older build keeps flowing so already-named iPads stay roster-matched;
   // new installs check in anonymously (the dashboard renders them by device — RELVER-09).
   try {
     fleetLabelRef.current = (await AsyncStorage.getItem("sv_fleet_label")) || "";
   } catch { /* ignore */ }
   ```
   This preserves roster matching for every already-answered iPad (important while D-WS5-1 is pending)
   and removes only the prompt. Leave `sv_fleet_skip` unread (dead key; harmless).
2. `fleetCheckin()` (:251) now runs without waiting on any modal — first check-in lands seconds earlier.
3. WIRE COMPAT: the check-in still sends `label: fleetLabelRef.current || ""` (:192) — same field, same
   shape; the worker's coalescing (:266) keeps old labels alive regardless.
4. If D-WS5-1 chooses "labels survive native-only": instead of deleting, DEFER the prompt until after the
   first `connected` state event (move it into the mesh listener behind a one-shot ref) so first-run has
   exactly one dialog. (Not recommended — see D-WS5-1.)
5. M7's DIAGNÓSTICO screen is the future home for a passive label field if naming ever returns.

**Acceptance criteria** (fresh install on the 2-device day):
- [ ] First launch shows ONLY the iOS Local Network dialog — no "¿Quién usa este iPad?".
- [ ] `POST /fleet/checkin` still fires (with `label:""` on a fresh install; with the old label on an
  upgraded install) — confirm on the local dashboard or /log.
- [ ] An iPad that answered the prompt on build ≤381 still shows named on the dashboard after updating.

**Tests.**
- `e2e/native-entrypoint.test.mjs` (reads `PdfReaderApp.tsx` at :20): add
  `assert.ok(!/Alert\.prompt\(/.test(source), "the first-run self-ID prompt stays removed (#270 parity)");`
  and keep a positive pin that `sv_fleet_label` is still read (upgrade-path guarantee):
  `assert.match(source, /sv_fleet_label/)`.
- Dashboard smoke against local `wrangler dev` (a2 conventions): an anonymous native check-in
  (`{deviceId, surface:"native", nativeBuild}`) renders in the unmatched table without error — fold into
  `sync-worker/test/fleet-dashboard.test.mjs`.
- Docs edits: `docs/pre-mass-checklist.md` — §B note (only if D-WS5-1(a)):
  `Los iPads nuevos ya no piden nombre; identifícalos en el tablero por dispositivo (o fija su deviceId en la lista).`
  No runbook change.

**Dependencies.** RELVER-09 (dashboard must stop depending on labels) lands BEFORE this; D-WS5-1 decides
the end state. Ships in WS5-PR4 (native batch). Bonus interaction: removing the prompt un-delays the
first check-in, making SYNCE2E-07's recency windows slightly more accurate on first boots.

---

#### FOLNAT-07 — Swift `bundle-error` (~15 diagnosable stages) and `memoryWarning` are silently dropped by the native JS listener: a follower's failed mesh OTA install has no surface or telemetry `low` `native` `native-build`

**Problem.** The DirectorSyncEvent listener's `default: break` swallows `bundle-error` and
`memoryWarning` (and the dead takeover-approved/denied). Swift instruments the bundle install pipeline
with a precise per-stage `bundle-error` (timeout/pack/send/receive/header-*/archive-size/index-too-small/
swap-*…) *specifically so failures are diagnosable*, and emits `memoryWarning` on
`UIApplication.didReceiveMemoryWarningNotification` — all of it dropped at the JS boundary, so `/log`
timelines never show it.

**User impact at Mass.** A follower whose 30 MB mesh transfer/install fails sees nothing and keeps the
old bundle while the director believes the fleet updated; post-Mass forensics via `/log` are blind to the
failure (only Swift's local dbgLog knows). Memory pressure is never even recorded before a jetsam kill.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:965-966` — `default: break` in the DirectorSyncEvent switch — the drop point.
- `ios/SignoVivo/DirectorSyncModule.swift:271-278` — `handleMemoryWarning` emits
  `{type:"memoryWarning", role}` (observer registered :246-250). *(corrected: emit body is :273-277
  within the :271-278 handler; original cite :271 was the handler start)*
- `ios/SignoVivo/DirectorSyncModule.swift` bundle-error emit sites — `:746` (stage "timeout"), `:764`
  ("pack"), `:784` ("send"), `:889` (install-pipeline stages via variable, doc comment :876), `:1893`
  ("receive"), `:1898` ("receive-nil"). *(corrected: original cite :879 → the pipeline emit is :889)*
- `PdfReaderApp.tsx:112` + `:266-267` — `pendingInjectRef` (the queue the finding's recommendation
  proposes clearing) is already capped at 100 entries.
- `e2e/nearby-sync-contract.test.mjs:180` — Swift's memoryWarning EMISSION is already pinned; no JS
  consumer is pinned anywhere.

**Fix — step by step.** (Native build — WS5-PR4, same batch as RELVER-06/M-F3: the events this surfaces
are exactly the ones that batch's device-day scenarios produce.)
1. `PdfReaderApp.tsx` — add two cases above `default:` (:965):
   ```ts
   case "bundle-error": {
     // The mesh OTA install pipeline failed at a named stage. Telemetry only — the
     // follower stays silent BY DESIGN (nothing a congregant can act on); the /log
     // timeline is how the operator diagnoses it post-hoc. RELVER-06 stops the loop;
     // held M-F3 gates the retry moment.
     dbgLog("mesh:bundle-error", { stage: String((event as { stage?: unknown }).stage ?? "?") });
     breadcrumb(`bundle-error:${String((event as { stage?: unknown }).stage ?? "?")}`);
     break;
   }
   case "memoryWarning": {
     dbgLog("mesh:memoryWarning", { role: event.role });
     break;
   }
   ```
   (Adjust the field access to the file's existing event typing style — the listener already reads
   `event.code`/`event.status` loosely.)
2. **Open disagreement with the finding's queue-clear recommendation.** The lens proposes clearing
   `pendingInjectRef` and skipping heartbeat injections on memoryWarning. Recommend AGAINST both: the
   queue is ≤100 small strings (:266-267 caps it) — clearing it recovers ~zero memory but can drop a
   queued ROLE inject (a real correctness cost: a director-role event lost while the WebView is booting),
   and page injects are re-driven by the 1 s heartbeat anyway only for pages — roles are not re-driven.
   Telemetry is the whole near-free win; take it and stop. If device-day profiling ever shows jetsam
   kills with the queue implicated, revisit with data.
3. **Director-side surfacing** (the actor who can retry): the director's own failure stages
   (timeout/pack/send — :746/:764/:784) now land in its `/log` timeline via step 1. Do NOT add a mid-Mass
   Alert — a background transfer failure is not actionable during Mass, and the follower keeps working on
   its old bundle by design. The passive "transferencia de himnario falló (etapa X)" line belongs on M7's
   DIAGNÓSTICO screen when it lands (note it in that spec).
4. WIRE COMPAT: consuming previously-dropped events is purely additive; old shells keep dropping them.

**Acceptance criteria.**
- [ ] Dev build with a forced install failure (corrupt archive — truncate the pack in
  `packWebBundle`'s temp file, or dev-hack a bad `headerLen`): the follower's `/log` (local `wrangler
  dev` or gated prod read) shows `mesh:bundle-error` with the correct stage; `sv_bc` breadcrumb contains
  `bundle-error:<stage>`.
- [ ] Simulator "Simulate Memory Warning": `/log` shows `mesh:memoryWarning` with the current role.
- [ ] No behavior change otherwise: page sync, role flows, and the happy-path bundle install are
  untouched (the new cases only log).

**Tests.**
- `e2e/native-entrypoint.test.mjs` (source pin, per the finding's test idea): assert the listener switch
  handles both events —
  `assert.match(source, /case "bundle-error"/); assert.match(source, /case "memoryWarning"/);`
- `e2e/nearby-sync-contract.test.mjs`: the Swift emission pins already exist (memoryWarning :180; add a
  `"type": "bundle-error"` match if absent) — keeps the producer/consumer pair pinned from both sides.
- Behavioral proof is device/simulator-gated → fold the two acceptance scenarios into the M7 device-day
  script.
- Docs edits: none to the pre-Mass ritual (telemetry-only). Add to the M7 device-day list (wherever
  WS5-PR4's PR body enumerates repros): "force a corrupt bundle install → /log shows the stage".

**Dependencies.** Ship in WS5-PR4 with RELVER-06 + held M-F3 (same pipeline, same device-day). No
worker/web dependency.

---

## 5.2 Cross-cutting docs ledger (single view of every ritual edit)

For WS5-PR3, apply to `docs/pre-mass-checklist.md`: the §A.3 rewrite (RELVER-01 + RELVER-12 texts above),
the `-prueba` badge checkbox (RELVER-11), the "MODO PRUEBA" chip checkbox + end-of-walk unpin step
(RELVER-02), the canary-week amber-director note (SYNCE2E-07), the §B web-version bullet (RELVER-04), the
§B skew-badge bullet (RELVER-05), the §B anonymous-web-device note (RELVER-09), and — post-D-WS5-1 — the
no-name-prompt note (FOLNAT-05).

To `docs/green-day-deploy-runbook.md`: fix :44-45 (RELVER-01), Step 1 deploy command → `bash
sync-worker/deploy.sh` + `/health deploy` verify (RELVER-07), Step 2 combined-URL + `-prueba` + chip
verify lines (RELVER-01/-11/-02), Step 3 fleet-dashboard web-version verify (RELVER-04).

`scripts/rollback-web.sh` needs no change (verified: read-only helper; paths A/B unaffected by anything
here) — but RELVER-07 gives its worker sibling (`wrangler rollback`) the verification it lacked.


---

## Workstream 6 — Vestigial debt, comment drift & naming

> Findings covered, in execution order: VESTIG-02, VESTIG-01, VESTIG-03, VESTIG-04, VESTIG-05,
> VESTIG-07, VESTIG-08, VESTIG-09, VESTIG-11, VESTIG-12, N2W-05.
> All evidence re-verified against HEAD `d5075091` (build 381) on 2026-07-09. Line numbers are
> exact at that commit; if your HEAD has moved, re-run every grep in this chapter before editing —
> the greps, not the line numbers, are the ground truth.

## 6.0 Why this workstream exists

**Theme.** In build 374 the entire two-book / geo / Sión / unlock system was deleted. What
survives is not dead *code* so much as dead *narrative*: comments, docs, defaults, and helper
files that confidently describe a system that no longer exists. In this project the "next
maintainer" is almost always a context-free engineer-agent working from exactly what the source
says. That makes false comments a **correctness risk, not a cosmetic one** — this audit found
three near-miss classes where the stale text actively invites a harmful edit:

- a worker comment that says the secret director codes "are hardcoded in this PUBLIC repo"
  (VESTIG-02 — invites re-committing real phone numbers, the exact leak class rotated out on
  2026-07-05);
- a Swift comment that justifies a still-load-bearing guard entirely with deleted mechanisms
  (VESTIG-04 — invites deleting the guard as "obsolete");
- a bridge contract whose identifiers contain a typo (`__SIGNO_VINO_*`, "wine") that a
  well-meaning cleanup would "fix" on one side and silently break version reporting fleet-wide
  (VESTIG-07).

Everything in this workstream is deliberately **zero-behavior-change**: comments and docs become
true, dead files and dead declarations disappear, and one dev-tool default path is repaired.
Nothing a director, follower, or congregant can observe changes. If any step here would change
runtime behavior, you have deviated — stop.

**Executor safety rails (apply to every finding below):**

- Repo root is the working directory; all paths are repo-relative.
- **NEVER run `npm run test:e2e`** — the glob includes `e2e/relay-sync.test.mjs`, which publishes
  to the **PRODUCTION** relay room `alvernia-main` and flips live followers' pages. Run only the
  named safe files listed per finding. The CI-safe set is: `e2e/repo-minimal-footprint.test.mjs`,
  `e2e/native-entrypoint.test.mjs`, `e2e/native-stability-config.test.mjs`,
  `e2e/offline-books-integrity.test.mjs`, `e2e/nearby-sync-contract.test.mjs`,
  `e2e/permission-flow.test.mjs`, `e2e/svRelayRoom.test.mjs`, `e2e/svSelftest.test.mjs`,
  `e2e/svSyncDecision.test.mjs`, plus `node scripts/smoke-boot.mjs` (needs poppler `pdftoppm`/
  `pdftotext` + `cwebp` locally) and the local-only worker harness
  `bash sync-worker/test/run-a2.sh` (boots a local `wrangler dev`; the test file itself refuses
  any non-local base URL).
- The same warning applies to `sync-worker/test-client.html` (VESTIG-12): **never point it at the
  production relay to "verify" anything** — its publish button writes to `alvernia-main` by
  default (`test-client.html:56`). Use `run-a2.sh`'s local relay.
- Swift is uncompiled in this environment. Only comment-only Swift changes are in scope here
  (VESTIG-04/-05); they must be provably comment-only (`git diff` shows no code lines touched).

## 6.1 Wire-compat doctrine for removals

The app is a **three-copy system**: (1) the web bundle at signovivo.com (redeploys in minutes),
(2) the *same* bundle baked into native shells — fleet iPads run builds in the 368–381 range and
upgrade slowly via TestFlight, and (3) mesh-pushed `Documents/WebBundle` copies of arbitrary
vintage sitting on iPads (the shell prefers them unconditionally at boot — `PdfReaderApp.tsx:813-826`).
Any identifier that crosses a boundary (bridge message type, injected global, worker route/field,
storage key) may still be emitted or expected by a copy you cannot instantly update.

Before deleting anything, classify it:

- **WIRE-SAFE** — no fielded peer (old native shell 368–381, old bundled/mesh-pushed web copy,
  deployed worker) can ever send it, expect it, or read it. Deletable outright once the grep
  proofs in the finding come back clean. (Examples in this workstream: never-imported repo files,
  never-referenced AsyncStorage key *declarations*, a dist artifact with zero readers.)
- **NEEDS-DEPRECATION-WINDOW** — some fielded artifact still emits or expects it. Then the fix is
  never a symmetrical delete: **keep a tolerant reader/stub with a dated comment, remove only the
  dead writer side**, and retire the stub when the fleet floor passes the relevant build. (Example
  here: the `__SIGNO_VINO_INITIAL_BOOK` writer in N2W-05; the `__SIGNO_VINO_*` names themselves in
  VESTIG-07.)

**Live shims you must NOT remove in this workstream** (they look vestigial; they are load-bearing
compat, all documented in-code): the web `set-book` no-op handler (`web/src/app.js:942-943` —
shells ≤ build 374 still inject `set-book`), the worker `/unlock` always-ok stub
(`sync-worker/src/index.ts:766-771` + `unlock` in the ROUTE regex at `:398` — pre-374 web copies
inside old shells still call it), the `mode`/`bookId` wire fields (pinned by
`e2e/nearby-sync-contract.test.mjs` and the additive-only invariant), and the `#geo-gate` element
naming (now the boot loader, kept deliberately).

**Grep-proof discipline.** Every deletion below lists the exact grep(s) proving zero
importers/readers. The executor MUST re-run them at their HEAD immediately before deleting. Any
hit outside the sites the step says it will remove = STOP and re-assess; this repo moves fast.

## 6.2 Recommended PR slicing

Two mechanical, individually-revertable PRs. Do not interleave them with feature work from other
workstreams.

| PR | Contents | Ships via | Findings |
|---|---|---|---|
| **WS6-A — "make the comments and docs true"** | Comment/doc rewrites + one dev-tool default-path fix. Zero shipped-behavior delta by construction. | Repo-only; web comments ride the next Pages deploy, native comments ride the next build, worker redeploy optional | VESTIG-02, VESTIG-01, VESTIG-03, VESTIG-04, VESTIG-05, VESTIG-07 (document option), N2W-05 leg 1 (the `:585` comment). VESTIG-02 executes early in Wave 1 per §3 — drop it from WS6-A when Wave 1 already landed it |
| **WS6-B — "delete the provably dead"** | File and declaration deletions, each with grep proofs re-run at commit time. Still zero user-observable change. | Repo-only + next web deploy (VESTIG-11) / next native build (VESTIG-08, -09, N2W-05 leg 2) | VESTIG-08, VESTIG-09, VESTIG-11, VESTIG-12 (per decision), N2W-05 leg 2 (INITIAL_BOOK deletion — see its sequencing decision) |

Why this split: WS6-A is reviewable as "text only — `git diff` shows no executable lines changed
except one argparse default"; WS6-B is reviewable as "deletions only — every deleted identifier
has a clean grep proof in the PR description". A revert of either PR is trivially safe. Both PRs
touch `sync-worker/README.md` (VESTIG-02 in A, VESTIG-12 in B) — land A first and rebase B.

---

#### VESTIG-02 — Worker comments + README still claim transmitter codes are "hardcoded in this PUBLIC repo" (false since build 368) and misdocument the required secret set `medium` `worker` `worker-only`

**Problem.** Two comments in `sync-worker/src/index.ts` and the README's Security section still
describe the pre-build-368 security model, in which director codes were plaintext in the repo.
Since build 368 the codes live ONLY in the `TRANSMITTER_CODES` Worker secret, fail-closed — the
stale comments directly contradict the invariant written at `index.ts:389`. The README further
claims `RELAY_DIRECTOR_TOKEN` is "the only write credential" when `/publish` also accepts
`X-Director-Code` (the path the native app actually uses), and `wrangler.jsonc` documents only one
of the three required secrets.

**Risk if left.** A maintainer taking the docs at face value either (a) re-commits real
phone-number codes "because they're already public" — re-opening the exact A1 leak class rotated
out on 2026-07-05 — or (b) provisions a fresh worker with only `RELAY_DIRECTOR_TOKEN`, which
401s every native director publish at the next Mass (fail-closed empty code set) and 401s the
fleet dashboard, both masked to followers as "no director".

**Evidence (verify before editing).**
- `sync-worker/src/index.ts:23-26` — `FLEET_DASHBOARD_KEY` docblock; the false clause "those are
  hardcoded in this PUBLIC repo, so gating PII behind them would expose every number" spans
  :24-25 (corrected: the finding cited :25 alone; the claim spans both lines).
- `sync-worker/src/index.ts:672-673` — fleet gate repeats it: "NOT the transmitter codes — those
  are hardcoded in this public repo, so gating PII behind them would expose every number to
  anyone reading the source."
- `sync-worker/src/index.ts:387-396` — ground truth: `validTransmitterCodes()` reads only
  `env.TRANSMITTER_CODES`, fail-closed; its own comment at :388-389 says the codes "must never
  live in this PUBLIC repo".
- `sync-worker/README.md:67-68` — "`RELAY_DIRECTOR_TOKEN` is the only write credential. It lives
  in the director app build + as a Worker secret." Both sentences are false at HEAD (corrected/
  addition: the second sentence is also stale — the native app authorizes via `X-Director-Code`,
  `src/directorRelaySync.js:16` and `:77`, not the Bearer token).
- `sync-worker/src/index.ts:777-787` — `/publish` auth: Bearer token OR `X-Director-Code` ∈
  `TRANSMITTER_CODES` (`codeOk` at :785).
- `sync-worker/wrangler.jsonc:15-16` — comment names only `RELAY_DIRECTOR_TOKEN`;
  `TRANSMITTER_CODES` and `FLEET_DASHBOARD_KEY` are undocumented.
- Adjacent same-file staleness, fold into this edit: `sync-worker/README.md:20` — example payload
  says `"totalPages":370`; the book is 371 (`STANDARD_TOTAL_PAGES = 371`, `web/src/app.js:188`).

**Fix — step by step.** (Comment/doc-only. No code lines change; redeploy optional.)

1. Replace the docblock clause at `index.ts:23-26` with:
   ```ts
   /** Secret gating the /fleet readiness dashboard + roster (which expose choir phone numbers). Set
    *  via `wrangler secret put FLEET_DASHBOARD_KEY`. Deliberately NOT a transmitter code — codes
    *  live only in the TRANSMITTER_CODES secret (never in this PUBLIC repo) and are held by several
    *  directors, so a page-turn credential must never double as a PII credential. */
   ```
2. Replace the fleet-gate comment at `index.ts:670-673` with:
   ```ts
   // Everything else under /fleet exposes choir phone numbers, so it is gated by the director
   // bearer token OR a DEDICATED SECRET (?k=SECRET for a browser, X-Fleet-Key for a script).
   // NOT the transmitter codes — those live only in the TRANSMITTER_CODES secret (see
   // validTransmitterCodes) and are held by several directors; a publish credential must never
   // unlock PII.
   ```
3. Rewrite the README Security section (`README.md:66-71`) to name both write credentials and all
   three secrets:
   ```md
   ## Security
   - Two write credentials authorize `POST /r/:room/publish` (see `src/index.ts`, publish auth):
     `Authorization: Bearer $RELAY_DIRECTOR_TOKEN` (scripts/testing) and
     `X-Director-Code: <code>` with the code listed in the `TRANSMITTER_CODES` secret
     (comma-separated; this is what the native app sends — see `src/directorRelaySync.js`).
     Both are Worker secrets; neither lives in this repo. **Never commit `.dev.vars`**
     (it's gitignored).
   - `FLEET_DASHBOARD_KEY` (third required secret) gates `/fleet*` reads and `GET|DELETE /log` —
     deliberately separate so a director code never unlocks phone-number PII.
   - Reads are public (a page number is not a secret). Worst-case abuse = a troll flipping the
     page; rotate the affected secret to cut them off.
   - Production: set `ALLOWED_ORIGINS` to `https://signovivo.com` in `wrangler.jsonc`.
   ```
4. Replace the `wrangler.jsonc:15-16` comment with:
   ```jsonc
   // Non-secret vars only. THREE required SECRETS — set each via `wrangler secret put`
   // (or .dev.vars locally), never here:
   //   RELAY_DIRECTOR_TOKEN — Bearer write credential (scripts/testing)
   //   TRANSMITTER_CODES    — comma-separated director codes; /publish fail-closes without it
   //   FLEET_DASHBOARD_KEY  — gates /fleet* + GET/DELETE /log; dashboard 401s without it
   ```
5. Same file while you are in it: fix `README.md:20` `"totalPages":370` → `"totalPages":371`.
6. `cd sync-worker && npm run typecheck` (`tsc --noEmit`) to prove the .ts edits are comment-only
   syntax-safe. Do NOT deploy as part of this PR — comments need no redeploy.

**Acceptance criteria.**
- [ ] `grep -rn "hardcoded in this" sync-worker/` → 0 hits.
- [ ] `grep -rn "only write credential" sync-worker/` → 0 hits.
- [ ] README Security section names `RELAY_DIRECTOR_TOKEN`, `TRANSMITTER_CODES`, and
      `FLEET_DASHBOARD_KEY`; wrangler.jsonc comment names all three.
- [ ] `grep -n "370" sync-worker/README.md` → 0 hits.
- [ ] `cd sync-worker && npm run typecheck` green; `git diff sync-worker/src/index.ts` shows
      comment lines only.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `bash sync-worker/test/run-a2.sh` (local wrangler dev only — proves the worker still
boots and publishes with `X-Director-Code` after the edit). Do NOT run `npm run test:e2e`; do NOT
touch `e2e/relay-sync.test.mjs`. `e2e/repo-minimal-footprint.test.mjs` does not pin worker files —
no update needed.

**Dependencies.** Workstream 5 (worker) edits `sync-worker/src/index.ts` code (e.g. SYNCE2E-01's
seq clamp around :147-177) — these comment blocks (:23-26, :670-673) are far from those lines, but
land WS6-A before or after Workstream 5's PR, never concurrently. VESTIG-12 (below, WS6-B) also
edits `sync-worker/README.md` — land this first; VESTIG-12 rebases.

---

#### VESTIG-01 — clean-header-boxes.py default `--in` still points at assets/alvernia_manual_2.pdf, deleted by the #271 rename — tool fires FileNotFoundError on next use `low` `cross` `multi`

**Problem.** PR #271 (`e4d1a014`, in build 381) renamed the songbook PDF to
`assets/signo_vivo_371.pdf` and updated every hardcoded consumer except one: the canonical
header-cleanup tool `scripts/clean-header-boxes.py` still defaults `--in` to the deleted path
(and shows it in its usage docstring). Project memory designates this exact script as the
instrument for future hymnal page edits, invoked without `--in`.

**Risk if left.** The next book-edit session opens with a guaranteed crash (pikepdf open of a
nonexistent file) — loud and quick to diagnose, but a certain stumble at the worst time (usually
a pre-Mass content fix).

**Evidence (verify before editing).**
- `scripts/clean-header-boxes.py:26` — `ap.add_argument("--in", dest="src",
  default=os.path.join(ROOT, "assets/alvernia_manual_2.pdf"))`.
- `scripts/clean-header-boxes.py:11` — usage docstring: `[--in assets/alvernia_manual_2.pdf]`.
- `ls assets/` at HEAD → only `signo_vivo_371.pdf` (no `alvernia_manual_2.pdf`).
- Contrast (already updated by #271, proving the miss): `web/build.mjs:675`,
  `scripts/check-book-consistency.mjs:22`, `e2e/eas-config.test.mjs:59`,
  `src/alverniaManual2SongIndex.js:1` — all say `signo_vivo_371.pdf`.

**Fix — step by step.** (Dev-tool default only; nothing ships. Not a deletion — no wire class.)

1. `scripts/clean-header-boxes.py:26`: change the default to
   `os.path.join(ROOT, "assets/signo_vivo_371.pdf")`.
2. `scripts/clean-header-boxes.py:11`: change the docstring example to
   `[--in assets/signo_vivo_371.pdf]`.
3. Optional (recommended, ~4 lines): immediately after `SRC = args.src` (:31), add a friendly
   existence check that lists available PDFs instead of a raw traceback:
   ```python
   if not os.path.exists(SRC):
       sys.exit(f"Input PDF not found: {SRC}\nAvailable: "
                + ", ".join(sorted(f for f in os.listdir(os.path.join(ROOT, "assets")) if f.endswith(".pdf"))))
   ```
4. Syntax-verify without needing the script's heavy deps (pikepdf/PIL may not be installed):
   `python3 -c "import ast; ast.parse(open('scripts/clean-header-boxes.py').read())"`.
   If pikepdf/Pillow ARE installed, `python3 scripts/clean-header-boxes.py --help` should print
   the new default. Do NOT run the script against the PDF — it edits in place by default (:32).

**Acceptance criteria.**
- [ ] `grep -rn "alvernia_manual_2" scripts/` → 0 hits.
- [ ] Repo-wide: `grep -rn "alvernia_manual_2" --include="*.py" --include="*.mjs" --include="*.ts" --include="*.tsx" --include="*.js" . | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"`
      → only `src/offlineBooks.ts:5` remains (that comment is VESTIG-09's; 0 hits once WS6-B lands).
- [ ] `ast.parse` syntax check passes.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** No test executes this script. Adjacent pins stay green untouched:
`node --test e2e/offline-books-integrity.test.mjs` (song index ↔ build contract) and
`node --test e2e/repo-minimal-footprint.test.mjs`. (`e2e/eas-config.test.mjs` pins the new PDF
path but is excluded from the CI safe set because it shells out — do not run it here.) Never
`npm run test:e2e`.

**Dependencies.** VESTIG-09 fixes the same stale path string in `src/offlineBooks.ts:5`
(different file, WS6-B) — no conflict, but the repo-wide acceptance grep only reaches zero after
both land. No other workstream touches this script.

---

#### VESTIG-03 — sw.js/app.js comments still describe the retired ?admin=1 operator preload flow (and say '370 pages') `low` `web` `web-only`

**Problem.** The service worker's activate-handler comment reads as ops guidance: it tells an
operator prepping a parish iPad to preload the manual "via signovivo.com?admin=1". No `?admin`
handling exists anywhere at HEAD — the flow died with the two-book removal; today the full
offline bundle precaches automatically after first paint. The same comment says "370 pages"
(book is 371), and a smaller `(offline / ?admin` ghost survives in an app.js comment.

**Risk if left.** An operator (or agent) preparing a new iPad per the comment visits
`?admin=1`, sees nothing special, and either assumes breakage or falsely assumes a full preload
happened — a wrong mental model of the one flow that guarantees offline readiness for Mass.

**Evidence (verify before editing).**
- `web/src/sw.js:88` — "We deliberately do NOT pre-fetch all 370 pages".
- `web/src/sw.js:90-91` — "the offline iPad still preloads the whole manual via
  signovivo.com?admin=1" (corrected: the clause spans :90-91; the finding cited :91).
- `grep -rn "admin" web/src/` → only these two comment sites plus the unrelated
  `offline-admin-note` element (`index.html:67`); zero `?admin` handling code.
- `web/src/app.js:3414` — "(offline / ?admin" in the song-index hydration comment (block
  :3413-3415).
- Ground truth for the replacement text: book is 371 pages (`STANDARD_TOTAL_PAGES = 371`,
  `web/src/app.js:188`); the current precache is `deferOfflinePrecache` (`web/src/app.js:621`,
  web-only, deferred after reveal) → `ensureOfflineBundle` (`web/src/app.js:577`), no query param.

**Fix — step by step.** (Comment-only; ships with the next Pages deploy, changes nothing at
runtime.)

1. Replace the first paragraph of the activate comment, `web/src/sw.js:88-91`, with:
   ```js
   // Clean up old-version caches. We deliberately do NOT pre-fetch all 371 pages during SW
   // install — that ~34 MB download froze first loads. Followers cache pages on demand
   // (cache-first handler below); the full offline bundle is precached automatically after
   // first paint by app.js (deferOfflinePrecache → ensureOfflineBundle) — no query param,
   // no operator step.
   ```
   Leave the rest of the comment block (:92-…, the two "deliberate departures") untouched.
2. `web/src/app.js:3414`: drop the ghost so :3413-3415 reads:
   ```js
   // Fold the full song index into state and refresh everything that depends on it.
   // Runs immediately if the index came inline with the manifest, otherwise in the
   // background once pages.json lands (see below).
   ```

**Acceptance criteria.**
- [ ] `grep -rn "admin=1\|?admin" web/src` → 0 hits (`offline-admin-note` does not match this
      pattern and stays).
- [ ] `grep -n "370" web/src/sw.js` → 0 hits.
- [ ] `git diff web/src` shows comment lines only.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `node scripts/smoke-boot.mjs` (builds the bundle and boots it — proves the sw.js edit
is not syntactically fatal), `node --test e2e/svSelftest.test.mjs e2e/svRelayRoom.test.mjs`.
Never `npm run test:e2e`; never `e2e/relay-sync.test.mjs`. Footprint test does not pin sw.js
content — no update needed.

**Dependencies.** None sharing these lines. (P8/prior-art owns the neighboring dead
offline-gate UI — do not expand into it here.)

---

#### VESTIG-04 — Swift nil-page-guard comment justifies a still-load-bearing guard entirely with DELETED behavior (hymns-4 / geo / bookFromSync) — invites a harmful cleanup `low` `native` `native-build`

**Problem.** `sendCurrentPageSnapshot`'s comment explains its nil-page guard entirely via
mechanisms deleted in build 374: a nil-page snapshot would ship `bookId=""` which "the follower's
JS (bookFromSync) hard-coerces to the DEFAULT book 'hymns-4', yanking a correctly geo-resolved
'standard' (Del Rio) follower". None of that exists — `bookFromSync`, `hymns-4`, and geo are all
gone. The guard itself is still correct and load-bearing: broadcasting a nil-page guess would
land newly-connected followers on page 1.

**Risk if left.** A maintainer who verifies the written rationale, finds every cited mechanism
deleted, and removes the "obsolete" guard reintroduces spurious page-1 snapshots to followers
joining mid-Mass.

**Evidence (verify before editing).**
- `ios/SignoVivo/DirectorSyncModule.swift:223-229` — the stale comment (corrected: the comment
  text spans :223-229; :222 cited by the finding is the function signature line).
- `ios/SignoVivo/DirectorSyncModule.swift:230-233` — the guard
  (`guard currentRole == "director", let page = currentPageNumber, let data = pagePayload(...)`)
  — KEEP byte-identical.
- Proof the rationale is dead: `grep -rn "bookFromSync\|hymns-4" web/src/ src/ PdfReaderApp.tsx`
  → 0 hits.
- Same block, :227, contains a "2s mesh heartbeat" claim — that is VESTIG-05's Swift site; this
  rewrite fixes both at once (heartbeat is 1s, `PdfReaderApp.tsx:400`).

**Fix — step by step.** (Comment-only Swift change — the only Swift class safe to ship without a
device/compile day. One rewrite serves VESTIG-04 AND VESTIG-05's Swift site.)

1. Replace the comment at `DirectorSyncModule.swift:223-229` with:
   ```swift
   // Send NOTHING until the director has an actual page. currentBookId/currentMode are only set
   // alongside currentPageNumber in sendPageUpdate, so a nil page means we'd broadcast an
   // empty-book/page-1 GUESS that yanks a newly-connected follower to page 1. The 1s mesh
   // heartbeat + 1.5s snapshot-probe + 8s hello all re-send the real snapshot the instant the
   // director navigates, so the nil window loses nothing. A page-1-with-empty-book guess is a
   // wrong guess — never broadcast it. (Historical: this guard once also protected the two-book
   // coercion — bookFromSync / "hymns-4" / geo — deleted in build 374.)
   ```
2. Confirm the guard at :230-233 and everything below is untouched:
   `git diff ios/SignoVivo/DirectorSyncModule.swift` must show only `//` lines changed.

**Acceptance criteria.**
- [ ] `grep -n "bookFromSync\|hymns-4\|geo-resolved\|Del Rio" ios/SignoVivo/DirectorSyncModule.swift` → 0 hits.
- [ ] `git diff ios/` contains only comment lines (no executable Swift changed).
- [ ] The guard condition string `guard currentRole == "director", let page = currentPageNumber`
      still present, byte-identical.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** No test compiles Swift here. Pin the adjacent wire contract:
`node --test e2e/nearby-sync-contract.test.mjs` and `node --test e2e/native-stability-config.test.mjs`
must stay green (they read source/config, not builds). Never `npm run test:e2e`. Footprint test —
not affected.

**Dependencies.** VESTIG-05 (same comment block at :227 — this step IS its Swift fix; do them in
one commit). M7's device day touches this file for mesh-bundle work — comment ride-along is fine;
avoid landing simultaneously with any Workstream that edits `DirectorSyncModule.swift` code.

---

#### VESTIG-05 — '2s mesh heartbeat' comment drift across 5 sites — actual cadence is 1s; NEW-DIR-3's 8s-window derivation cites the wrong number `low` `native` `native-build`

**Problem.** The director's mesh heartbeat fires every **1000 ms** (`setInterval(..., 1000)`,
`PdfReaderApp.tsx:393-400`), but five comments (plus one bonus site) still say "2s" — including
the stated derivation of the NEW-DIR-3 8-second live-director window at :68. Anyone re-tuning
liveness windows or reasoning about heartbeat timing during an incident from these comments is
off by 2x.

**Risk if left.** An engineer re-derives `LIVE_DIRECTOR_WINDOW_MS` (or debug-timeline timing
during a mid-Mass incident) from the written 2s cadence and miscalculates by a factor of two —
timing math is exactly the thing the next agent will trust comments for.

**Evidence (verify before editing).**
- Ground truth: `PdfReaderApp.tsx:391` — `startDirectorHeartbeat` (corrected: defined at :391,
  not :390/:394 as variously cited); mesh `setInterval` closes with `}, 1000);` at :400.
- Drift sites, exact current text verified:
  - `PdfReaderApp.tsx:68` — "a director's mesh page arrives on a ~2s heartbeat" (the NEW-DIR-3
    window derivation).
  - `PdfReaderApp.tsx:118` — "a dropped page-turn recovers in ~2s" (bonus site the original
    finding missed; confirmed at HEAD).
  - `PdfReaderApp.tsx:263` — "the 2s heartbeat + page events would otherwise grow native heap".
  - `PdfReaderApp.tsx:780` — "The 2s mesh heartbeat would then de-dupe".
  - `PdfReaderApp.tsx:900` — "De-dupe the 2s mesh heartbeat".
  - `ios/SignoVivo/DirectorSyncModule.swift:227` — "The 2s mesh heartbeat + 1.5s snapshot-probe"
    (fixed by VESTIG-04's block rewrite).
- **CAUTION — one "2s" is TRUE and must survive:** `PdfReaderApp.tsx:105` "inside the 2s
  mesh-start retry window" refers to the real `setTimeout(..., 2000)` start-retry sleeps
  (becomeFollower ~:440, becomeDirector ~:498), NOT the heartbeat (verified: both sleeps are
  2000 ms at HEAD). A blind `s/2s/1s/` would corrupt it.
- Adjacent dead work (do NOT restructure): `startDirectorHeartbeat` always starts the mesh timer;
  a transmitter-only director early-returns every tick at :394 — a harmless no-op 1 Hz timer.
  That region is owned by HELD design H2 (heartbeat-effect split); comment-only here.

**Fix — step by step.** (Comment-only; no `sed` over the whole file — edit each site by hand.)

1. `PdfReaderApp.tsx:68`: change "arrives on a ~2s heartbeat" → "arrives on a 1s heartbeat
   (startDirectorHeartbeat), so 8s ≈ 8 missed beats". Leave the rest of the :68-70 block as is.
2. `PdfReaderApp.tsx:118`: "recovers in ~2s" → "recovers in ~1s".
3. `PdfReaderApp.tsx:263`: "the 2s heartbeat" → "the 1s heartbeat".
4. `PdfReaderApp.tsx:780`: "The 2s mesh heartbeat" → "The 1s mesh heartbeat".
5. `PdfReaderApp.tsx:900`: "De-dupe the 2s mesh heartbeat" → "De-dupe the 1s mesh heartbeat".
6. `PdfReaderApp.tsx:105`: reword to keep it true AND make the acceptance grep clean:
   "inside the 2s mesh-start retry window" → "inside the 2s start-retry window (the
   setTimeout(…, 2000) retry sleeps)". Do not change the sleeps themselves.
7. Swift site :227 — covered by VESTIG-04 step 1 (verify it landed if executing separately).
8. Do NOT touch `startDirectorHeartbeat`'s structure, the interval values, or
   `LIVE_DIRECTOR_WINDOW_MS` — held design H2 owns any restructuring.

**Acceptance criteria.**
- [ ] `grep -rn "2s mesh\|2s heartbeat\|in ~2s" PdfReaderApp.tsx ios/SignoVivo/` → 0 hits (the
      third pattern catches the :118 site, which the first two don't match).
- [ ] `grep -n "2000" PdfReaderApp.tsx` still shows the two start-retry sleeps (unchanged).
- [ ] `grep -n "}, 1000)" PdfReaderApp.tsx` still shows the mesh heartbeat (unchanged).
- [ ] `npm run typecheck` green; `git diff PdfReaderApp.tsx` shows comment lines only.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `npm run typecheck`; `node --test e2e/native-entrypoint.test.mjs e2e/nearby-sync-contract.test.mjs`.
Never `npm run test:e2e`. Footprint test — not affected.

**Dependencies.** VESTIG-04 (shares the Swift :227 block — one commit). DIRNAT-07 (another
workstream) works in the `LIVE_DIRECTOR_WINDOW_MS` / boot-prompt area that the :68 comment
documents — comment-only here, but sequence the PRs rather than landing same-day on the same
lines. HELD design H2 owns `startDirectorHeartbeat` restructuring — out of scope.

---

#### VESTIG-07 — __SIGNO_VINO_* (sic, 'wine') misspelling is a load-bearing cross-file bridge contract — must be documented or renamed coordinated, never casually corrected `low` `cross` `multi`

**Problem.** Every native shell injects three globals spelled `__SIGNO_VINO_*` ("VINO" = wine,
not "VIVO"), and the web bundle reads two of them under the same misspelling. The typo is
self-consistent and therefore load-bearing: it IS the wire contract between every fielded shell
(builds 332–381+) and every web bundle. Nothing marks it as intentional, and a grep for
`SIGNO_VIVO` finds nothing — a classic trap for a tidying agent.

**Risk if left.** A well-meaning one-side spelling fix deploys instantly (web) or rides a build
(native) and silently degrades version reporting for every fielded shell: the version badge/label
would stop showing the native shell's build (falling back to the web bundle's baked
`BUILD_NUMBER` — plausible-but-wrong, worse than blank) and the crash-report `build` fallback
would lose its native source. `NATIVE_FILE_MODE` itself survives via the `file:` protocol OR, so
the breakage is silent, partial, and found only during a pre-Mass readiness check.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:1036-1038` — injects `__SIGNO_VINO_NATIVE_FILE_MODE` (:1036),
  `__SIGNO_VINO_NATIVE_BUNDLE_VERSION` (:1037), `__SIGNO_VINO_INITIAL_BOOK` (:1038 — dead; that
  one is N2W-05's to delete).
- `web/src/app.js:227` — `NATIVE_FILE_MODE = Boolean(window.__SIGNO_VINO_NATIVE_FILE_MODE ||
  window.location.protocol === "file:")`.
- `web/src/app.js:2901-2903` — crash-report payload's `build` field falls back to
  `__SIGNO_VINO_NATIVE_BUNDLE_VERSION` (corrected: this is the crash-report build field, NOT
  "fleet check-in version" as originally noted — fleet `nativeBuild` is reported native-side from
  `PdfReaderApp.tsx:191`).
- `web/src/app.js:3477-3481` — version label + build badge: `resolvedBuild` PREFERS the injected
  native global over baked `BUILD_NUMBER`.
- `web/build.mjs:30` — comment repeats the misspelled name (third file cementing the convention).
- Verified: zero hits for `SIGNO_VINO|SIGNO_VIVO` in `e2e/`, `src/`, `ios/` — no existing test
  pins these globals (the pin suggested below is net-new).

**Fix — step by step.** The fix is **DOCUMENT-in-place — NOT a rename.** A rename is
NEEDS-DEPRECATION-WINDOW by definition (old native builds 368–381 inject only the old names; old
bundled/mesh-pushed web copies read only the old names) and is specced here strictly as an
optional future step.

1. `PdfReaderApp.tsx` — insert immediately above line 1036 (inside the `preloadScript` array
   construction, or directly above the useMemo at :1033):
   ```ts
   // NOTE: "__SIGNO_VINO_*" (sic — "VINO", not "VIVO") is a historical typo that IS the wire
   // contract with every fielded web bundle (web/src/app.js reads these exact names at :227,
   // :2903, :3480). Do NOT correct the spelling unilaterally — a rename needs the dual-name
   // deprecation window described in docs/app-contracts.md ("load-bearing typo" note).
   ```
2. `web/src/app.js` — insert immediately above line 227:
   ```js
   // NOTE: "__SIGNO_VINO_*" (sic — "VINO") is a historical typo that IS the native<->web wire
   // contract; every fielded shell (builds 332-381+) injects these exact names from
   // PdfReaderApp.tsx preloadScript. Do NOT correct the spelling unilaterally — see the
   // "load-bearing typo" note in docs/app-contracts.md for the coordinated-rename recipe.
   ```
3. `web/build.mjs:30` — append `(sic — the VINO spelling is the wire contract; see
   web/src/app.js NATIVE_FILE_MODE note)` to the existing comment.
4. `docs/app-contracts.md` — add a dated addendum blockquote directly under the H1 (do NOT
   attempt to fix the rest of this doc — it describes build ~370 and its rewrite is owned by
   prior-art item P8):
   ```md
   > **[2026-07-09 addendum — load-bearing typo]** The native-injected globals are spelled
   > `__SIGNO_VINO_*` ("VINO", not "VIVO") on BOTH sides of the bridge. The typo IS the wire
   > contract for every fielded shell (builds 332–381+); never correct it unilaterally. A rename
   > must be a coordinated dual-name window: (1) web reads `__SIGNO_VIVO_* ?? __SIGNO_VINO_*`
   > first (web-only deploy, tolerant of both), (2) native injects BOTH names for >=2 builds,
   > (3) drop the old name only after the fleet floor (dashboard MIN_SYNC_BUILD) passes the
   > dual-inject build. (This doc otherwise predates the build-374 single-book removal — trust
   > current source over its line numbers and its two-book content.)
   ```
5. **Optional future step (do NOT execute in this workstream): coordinated rename.** Fold into
   M3 bridge v1. Sequence exactly as the addendum describes — web fallback-read ships BEFORE any
   native rename; native dual-injects for >=2 builds; retire the old name only after the fleet
   floor passes. Treat as its own PR with a device-day verification.
6. Optional hardening (cheap, recommended with this PR): extend
   `e2e/native-entrypoint.test.mjs`'s WebView-shell test (:19-32) with:
   ```js
   // The VINO (sic) globals are the wire contract with every fielded web bundle — pin them.
   assert.match(source, /__SIGNO_VINO_NATIVE_FILE_MODE/);
   assert.match(source, /__SIGNO_VINO_NATIVE_BUNDLE_VERSION/);
   assert.doesNotMatch(source, /__SIGNO_VIVO_/);
   ```
   (If N2W-05 leg 2 has NOT landed yet, do not pin against `__SIGNO_VINO_INITIAL_BOOK` either
   way — it is scheduled for deletion.)

**Acceptance criteria.**
- [ ] All three source sites (`PdfReaderApp.tsx` ~:1035, `web/src/app.js` ~:226,
      `web/build.mjs:30`) carry the do-not-rename note.
- [ ] `docs/app-contracts.md` carries the dated addendum.
- [ ] `grep -rn "__SIGNO_VIVO_" PdfReaderApp.tsx web/ src/ ios/ | grep -v node_modules` → 0 hits
      (no one "fixed" the spelling as part of this change).
- [ ] `npm run typecheck` green; `node scripts/smoke-boot.mjs` green (app.js edit is comment-only).
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `node --test e2e/native-entrypoint.test.mjs` (plus the optional new pins),
`node scripts/smoke-boot.mjs`. Never `npm run test:e2e`. Footprint test — not affected.

**Dependencies.** N2W-05 deletes the third injected global (`__SIGNO_VINO_INITIAL_BOOK`, :1038)
and shrinks the same `preloadScript` block — land VESTIG-07's comment in WS6-A, N2W-05's deletion
in WS6-B, and write the comment so it survives the deletion (it already names only the two live
globals). The optional rename belongs to M3 (typed bridge) — reference only.

---

#### VESTIG-08 — src/pdfReaderUrl.js + src/songNavigation.js(+.d.ts) are pre-WebView-era modules with zero importers repo-wide `low` `native` `native-build`

**Problem.** Two plausible-looking modules from the pre-build-332 native PDF-reader era —
`src/pdfReaderUrl.js` (page-URL clamp/normalizer) and `src/songNavigation.js` (binary-search
`findSongEntryOrNext`, with `songNavigation.d.ts`) — have zero importers anywhere. The WebView
rewrite stranded them; Metro never bundles them (it bundles imports only), so they have never
shipped since.

**Risk if left.** Pure maintainer tax: every audit and every agent that greps "song" or "page"
lands in these files and must re-prove them dead — this audit did exactly that, again.

**Evidence (verify before editing).**
- Files exist at HEAD: `src/pdfReaderUrl.js`, `src/songNavigation.js`, `src/songNavigation.d.ts`.
- Zero references: repo-wide grep (proof command below) returns nothing — not even
  self-references.
- (corrected) `e2e/repo-minimal-footprint.test.mjs` — the original evidence said the footprint
  test "pins npm scripts + devDeps, not src files". Incomplete: its third test (:98-105) DOES pin
  two src files required-PRESENT — `src/alverniaManual2SongIndex.js` and
  `src/alverniaManual2SongIndex.d.ts`. The three deletion targets here are NOT pinned (deletion
  is CI-safe), but this constrains the finding's optional fourth deletion — see step 4.
- Keep-alive facts for the neighbors: `web/build.mjs:563` reads
  `src/alverniaManual2SongIndex.js` as TEXT; `e2e/offline-books-integrity.test.mjs:17` pins it;
  the footprint test :100-101 pins both it and its `.d.ts`.

**Fix — step by step.**

1. **Grep proof (MUST re-run and be clean immediately before deleting):**
   ```bash
   grep -rn "pdfReaderUrl\|songNavigation" \
     --include="*.js" --include="*.ts" --include="*.tsx" --include="*.mjs" --include="*.json" . \
     | grep -v node_modules | grep -v "^\./docs/" | grep -v "^\./docs/ia-audit-2026-07/"
   ```
   Expected: **0 hits** (audit docs excluded). Any hit elsewhere = STOP.
2. **Wire-safety classification: WIRE-SAFE.** Never imported → never bundled by Metro → never
   shipped in any native build or web bundle; no fielded peer can reference a repo-local file.
   Deletable outright.
3. `git rm src/pdfReaderUrl.js src/songNavigation.js src/songNavigation.d.ts`
4. **Do NOT delete `src/alverniaManual2SongIndex.d.ts`** (the finding floated it as optional):
   it is pinned required-present by `e2e/repo-minimal-footprint.test.mjs:101`, so removing it
   costs a test edit for zero value. Skip. (If ever removed later, update the footprint test's
   :99-104 list in the same commit.)
5. Optional hardening (recommended): add the three deleted paths to the footprint test's
   "Unexpected leftover path" list (`e2e/repo-minimal-footprint.test.mjs:73-93`) so they cannot
   silently return:
   ```js
   "src/pdfReaderUrl.js",
   "src/songNavigation.js",
   "src/songNavigation.d.ts",
   ```
6. `npm run typecheck` (proves no type-graph dependency existed).

**Acceptance criteria.**
- [ ] Grep proof from step 1 re-run post-deletion → 0 hits; `ls src/` no longer lists the three
      files.
- [ ] `src/alverniaManual2SongIndex.js` and `.d.ts` still present (footprint test green).
- [ ] `npm run typecheck` green.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `node --test e2e/repo-minimal-footprint.test.mjs e2e/native-entrypoint.test.mjs
e2e/offline-books-integrity.test.mjs`; `npm run typecheck`. Never `npm run test:e2e`.
Footprint-inventory note: deletions do NOT require updating it (targets are unpinned), but
step 5 optionally updates it to pin them absent.

**Dependencies.** None — no other finding or workstream touches these files. Batch with
VESTIG-09 and N2W-05 leg 2 in WS6-B (same "native repo hygiene" commit family).

---

#### VESTIG-09 — offlineBooks.ts carries 6 dead STORAGE_KEYS (onboarding*/standardAccessName/mode/activeBookId/lastDirectorAt) plus a comment naming the deleted PDF path `low` `native` `native-build`

**Problem.** `src/offlineBooks.ts` declares **9** AsyncStorage key names (corrected — the
write-up said 8), of which only two have any consumer at HEAD: `lastSyncRole` (read+written by
the role-restore flow) and `lastPagePrefix` (written, never read — the known FOLNAT-04
write-only finding). The other **7** (corrected — title says 6; `lastDirectorAt` is also dead)
have zero readers AND zero writers repo-wide. The header comment also still cites the deleted
`assets/alvernia_manual_2.pdf`.

**Risk if left.** Every future storage/persistence change starts by re-deriving which of 9 keys
are real; the dead onboarding/mode/book keys also imply a two-book onboarding flow that no longer
exists, reinforcing the deleted-system narrative this workstream is scrubbing.

**Evidence (verify before editing).**
- `src/offlineBooks.ts:11-24` — 9 declared keys (corrected count): `onboardingComplete` (:12),
  `onboardingState` (:13), `onboardingCity` (:14), `standardAccessName` (:15), `mode` (:16),
  `activeBookId` (:17), `lastSyncRole` (:19), `lastDirectorAt` (:21), `lastPagePrefix` (:23).
- Consumers (repo-wide, verified): `STORAGE_KEYS.lastSyncRole` at `PdfReaderApp.tsx:432, :472,
  :506, :551, :869`; `STORAGE_KEYS.lastPagePrefix` at `PdfReaderApp.tsx:718` (write-only). The
  ONLY importer of the module is `PdfReaderApp.tsx:39`. Zero uses of the other 7 named keys and
  zero raw-string uses of their values anywhere (ts/tsx/js/mjs/swift).
- `src/offlineBooks.ts:5` — header comment cites `assets/alvernia_manual_2.pdf` (deleted in
  #271; same class as VESTIG-01).

**Fix — step by step.**

1. **Grep proofs (MUST re-run and be clean immediately before deleting):**
   ```bash
   grep -rn "STORAGE_KEYS\." --include="*.ts" --include="*.tsx" --include="*.js" --include="*.mjs" . \
     | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"
   # expected: ONLY PdfReaderApp.tsx lines 432/472/506/551/718/869, all .lastSyncRole or .lastPagePrefix
   grep -rn "sv\.onboarding\|sv\.mode\|sv\.book\.active\|sv\.standard\.accessName\|sv\.sync\.lastDirectorAt" \
     --include="*.ts" --include="*.tsx" --include="*.js" --include="*.mjs" --include="*.swift" . \
     | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"
   # expected: ONLY the src/offlineBooks.ts declaration lines this step deletes
   ```
2. **Wire-safety classification: WIRE-SAFE.** These are device-local AsyncStorage key NAMES.
   Deleting the *declarations* changes no wire message and no stored byte. Old builds keep their
   own compiled copies of the constants; nothing exchanges these names between devices.
3. Replace the `STORAGE_KEYS` block (`src/offlineBooks.ts:11-24`) with only the two live keys:
   ```ts
   export const STORAGE_KEYS = {
     // last known sync role (director/follower) for restart restore
     lastSyncRole: "sv.sync.lastRole",
     // per-book saved position — currently WRITE-ONLY (see finding FOLNAT-04 before changing)
     lastPagePrefix: "sv.book.lastPage.",
   } as const;
   ```
   If FOLNAT-04's PR-WS2-C (Wave 3) already removed `lastPagePrefix` (it deletes the write and the key), the replacement block keeps `lastSyncRole` ONLY.
4. Fix the header comment at :5: `assets/alvernia_manual_2.pdf` → `assets/signo_vivo_371.pdf`.
5. **Data-at-rest orphans: leave them.** Devices upgraded from pre-374 builds keep a few hundred
   bytes under `sv.onboarding.*`, `sv.mode`, `sv.book.active`, `sv.standard.accessName`,
   `sv.sync.lastDirectorAt` forever — harmless. An `AsyncStorage.multiRemove` boot sweep is a
   BEHAVIOR change and is out of scope for this zero-behavior workstream; if ever done later, it
   must never touch `sv.sync.lastRole` or `sv.book.lastPage.*`.
6. `npm run typecheck`.

**Acceptance criteria.**
- [ ] `grep -rn "onboarding\|standardAccessName\|lastDirectorAt\|activeBookId" src/ PdfReaderApp.tsx` → 0 hits.
- [ ] `grep -rn "alvernia_manual_2" src/` → 0 hits.
- [ ] `STORAGE_KEYS.lastSyncRole` and `.lastPagePrefix` untouched; their 6 PdfReaderApp.tsx call
      sites unchanged (`git diff PdfReaderApp.tsx` is empty for this finding).
- [ ] `npm run typecheck` green.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `npm run typecheck`; `node --test e2e/native-entrypoint.test.mjs
e2e/repo-minimal-footprint.test.mjs` (neither pins offlineBooks.ts contents — verified; no
inventory update needed). Never `npm run test:e2e`.

**Dependencies.** FOLNAT-04 (another workstream) will either make `lastPagePrefix` read at boot
or delete its dead write — this finding must NOT touch `lastPagePrefix` (the comment in step 3
points there). VESTIG-01 fixes the same stale PDF string in `scripts/` (WS6-A). No conflicts
otherwise.

---

#### VESTIG-11 — DELTA on new-web-dead-books-data-inline-blob: the books.json vestige is THREE sites — emitted dist file + inline blob + a dead per-device precache fetch `low` `web` `web-only`

**Problem.** The known P8 item records only the `#books-data` inline blob. At HEAD the vestige is
three sites: `web/build.mjs` (1) writes `dist/books.json` as a standalone artifact AND (2) inlines
the same registry as `#books-data`; and (3) `web/src/app.js` lists `"/books.json"` in
`SHELL_ASSETS`, so the offline precache fetches and caches it on every web device per cache
version — a file with zero readers (the app reads only `#pages-data`).

**Risk if left.** An implementer working the P8 item from its recorded anchor alone fixes
build.mjs and leaves the live precache reference behind — one dead network fetch + cache slot per
web device per version, forever, plus a boot-data registry that keeps implying multi-book
support.

**Evidence (verify before editing).**
- `web/build.mjs:678-686` — `booksManifest` construction (:680-685) + `fs.writeFileSync(...
  "books.json" ...)` at :686. NOTE the constraint: `defaultTotalPages` at :688 reads
  `booksManifest.books[DEFAULT_BOOK].totalPages` — the edit below must preserve it.
- `web/build.mjs:693, :698` — the `#books-data` comment + inline `<script id="books-data">`
  emission. `#pages-data` at :699 MUST STAY (read at `web/src/app.js:3386`).
- `web/src/app.js:241` — `"/books.json"` inside `SHELL_ASSETS` (:235-245); consumed by
  `coreAssets()` (:247-251) → `ensureCoreAssetsCached` (:553-564) → called from
  `ensureOfflineBundle` (:578).
- (corrected) The per-asset `Promise.allSettled` is the call at `app.js:561-563` (the finding
  cited :556, which is the explanatory comment above it). This is what makes removal order-safe:
  an old cached app.js requesting a no-longer-emitted `/books.json` just settles rejected,
  non-fatally.
- (corrected) "every device, every version" overstates: `deferOfflinePrecache` early-returns in
  `NATIVE_FILE_MODE` (`app.js:622`), so the dead fetch fires on WEB devices only (per cache
  version). Native shells never run this precache.
- `web/src/sw.js:22-31` — the SW's own `CORE_ASSETS` already excludes books.json (no sw.js edit
  needed).
- `scripts/smoke-boot.mjs:77` — asserts `books.json` exists in dist; MUST be updated in the same
  commit or the smoke gate fails (verifier addition, confirmed at HEAD).
- Zero readers proof: repo-wide grep for `books.json|books-data` hits ONLY the four lines this
  finding removes/updates (build.mjs:678/686/693/698, app.js:241, smoke-boot.mjs:77).

**Fix — step by step.** (All three sites + the smoke pin in ONE commit.)

1. **Grep proof (MUST re-run and be clean immediately before editing):**
   ```bash
   grep -rn "books\.json\|books-data" \
     --include="*.mjs" --include="*.js" --include="*.ts" --include="*.tsx" --include="*.html" . \
     | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"
   # expected hits ONLY: web/build.mjs:678,:686,:693,:698 · web/src/app.js:241 · scripts/smoke-boot.mjs:77
   ```
2. **Wire-safety classification: WIRE-SAFE.** No reader exists in any current or old bundle path
   that FAILS when the file disappears: the only fetcher is the `Promise.allSettled` precache
   (non-fatal by design, :555-563), and old cached app.js copies just settle rejected. No native
   or worker surface ever requests `/books.json`.
3. `web/build.mjs`: replace lines :678-688 (the whole `books.json — single-book registry` block
   plus the `defaultTotalPages` line) with:
   ```js
   const defaultTotalPages = standard.totalPages;
   ```
   Then in the `inlineScripts` block (:690-699): delete the `#books-data` line (:698) and its
   comment line (:693); keep `#pages-data` (:699) exactly as is. Update the block comment ("Inline
   TWO script tags" → "Inline ONE script tag").
4. `web/src/app.js:241`: delete the `"/books.json",` entry from `SHELL_ASSETS`.
5. `scripts/smoke-boot.mjs:77`: remove `"books.json"` from the shell-file list.
6. Rebuild + smoke: `node scripts/smoke-boot.mjs` (this both rebuilds dist and asserts boot
   integrity). Confirm a fresh `web/dist/` contains no `books.json`.

**Acceptance criteria.**
- [ ] Step 1 grep re-run → 0 hits repo-wide (excluding audit docs); `#pages-data` still present
      in build.mjs and still read at `app.js:3386`.
- [ ] Fresh build's `web/dist/` has no `books.json`; `dist/index.html` has `#pages-data` but no
      `#books-data`.
- [ ] `node scripts/smoke-boot.mjs` green (with its :77 list updated).
- [ ] `SHELL_ASSETS` still lists the 8 remaining shell assets; `ensureCoreAssetsCached` untouched.
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `node scripts/smoke-boot.mjs` (mandatory — it pinned the deleted artifact);
`node --test e2e/svSelftest.test.mjs e2e/repo-minimal-footprint.test.mjs`. Never
`npm run test:e2e`. Footprint-inventory note: the footprint test does not reference books.json —
only `scripts/smoke-boot.mjs:77` pins it, and step 5 updates that.

**Dependencies.** This IS the P8 `#books-data` purge item, widened to three anchors — mark P8
accordingly when done. No other workstream edits these exact lines; if another workstream lands
web deploys the same day, coordinate the Pages deploy order (this change is inert either way).

---

#### VESTIG-12 — sync-worker/test-client.html is three generations stale and reproduces the exact frozen-follower seq bug fixed in the real client `low` `worker` `worker-only`

**Problem.** The in-repo relay debugging page still implements the pre-#248 follower logic: a
seq-monotonic guard with NO freshness-first check — the exact P2-SEQ bug class fixed in the real
client (`7b3eda4c`/#248, now codified in `web/src/lib/svSyncDecision.js`). Its publish path is
Bearer-token-only, so the production auth path (`X-Director-Code`) cannot be exercised, and it
hardcodes `totalPages:370`. The README designates this page as the smoke-test tool.

**Risk if left.** During a live sync incident, the house debugging tool would freeze on a
stale-room takeover while real followers move — "confirming" a relay bug that does not exist and
misdirecting mid-Mass diagnosis. Worse, its publish button pointed at the prod relay (as the
README instructs) flips live followers in `alvernia-main`.

**Evidence (verify before editing).**
- `sync-worker/test-client.html:64` — `if(typeof s.seq === "number" && s.seq > 0 && s.seq <=
  lastSeq) return; // stale guard` — seq compare with no freshness-first rule (contrast
  `web/src/lib/svSyncDecision.js` ordering: freshness first, seq de-dupe second, demote resets
  lastSeq).
- `sync-worker/test-client.html:104` — publish sends only `Authorization: Bearer …`; the
  production `X-Director-Code` path (`sync-worker/src/index.ts:777-787`) is unreachable from it.
- `sync-worker/test-client.html:105` — `totalPages:370` (book is 371).
- `sync-worker/test-client.html:56` — default room is `alvernia-main` (PRODUCTION) — the
  prod-publish footgun (verifier addition, confirmed at HEAD).
- `sync-worker/README.md:44, :57` — the only references to the file repo-wide (verified);
  deleting the file orphans nothing else.
- Sanctioned harness exists: `sync-worker/test/a2.test.mjs` (throws unless pointed at a LOCAL
  relay; refuses `signovivo|workers.dev` bases) + `sync-worker/test/run-a2.sh` (boots local
  `wrangler dev`, exercises the real `X-Director-Code` auth).

**Fix — step by step.**

> **DECISION-REQUIRED (Miguel).** Two end-states; both resolve the debt.
> **Option B (recommended): delete the file** and point the README at the a2 harness. Rationale:
> the repo rule prefers deleting dead-behavior test surfaces over rewriting them green; the a2
> harness already exercises the REAL auth path against a local relay; and the page's
> prod-publishing default (room `alvernia-main`) is a standing footgun of the same class as the
> forbidden `relay-sync.test.mjs`. Recoverable from git history at any time (`git log --
> sync-worker/test-client.html`).
> **Option A: modernize in place** — keep a browser-visual debugging page. Pick A only if a
> point-and-click relay viewer is operationally wanted at Mass.

Option B (recommended):
1. **Grep proof (MUST re-run and be clean immediately before deleting):**
   ```bash
   grep -rn "test-client" --include="*.md" --include="*.mjs" --include="*.ts" --include="*.yml" --include="*.json" --include="*.html" . \
     | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"
   # expected: ONLY sync-worker/README.md:44 and :57
   ```
2. **Wire-safety classification: WIRE-SAFE.** The file is a repo-local dev page — never deployed
   (wrangler serves only `src/index.ts`), never referenced by any client or fielded device.
3. `git rm sync-worker/test-client.html`
4. Rewrite README's "Test it" section (:42-49) and the local-dev pointer (:57):
   ````md
   ## Test it (local relay, never production)

   ```bash
   bash sync-worker/test/run-a2.sh
   ```
   Boots a local `wrangler dev` (miniflare), runs `test/a2.test.mjs` against it (rate limits,
   seq=0 gate, X-Director-Code publish auth), then tears down. The test file refuses to run
   against any non-local base URL, so it can never touch the production room.
   ````
   In the "Local dev" section, replace "Point `test-client.html` at http://localhost:8787" with
   "Exercise it with `curl http://127.0.0.1:8787/r/test/state` or `bash test/run-a2.sh`."

Option A (only if chosen): port the freshness-before-seq rule into `applySnapshot` (mirror
`svSyncDecision.js`: stale `ts` older than 90s ⇒ treat as no-director and reset `lastSeq = -1`
BEFORE any seq comparison), add an `X-Director-Code` input used when the token field is empty,
change `totalPages:370` → `371` (:105), and change the default room (:56) to something non-prod
(e.g. `alvernia-staging`) so a stray click cannot flip live followers.

**Acceptance criteria.**
- [ ] (B) File gone; step 1 grep → 0 hits; README's test instructions name `run-a2.sh` and state
      the local-only guarantee. (A) The three code fixes verifiably present; default room is not
      `alvernia-main`.
- [ ] `bash sync-worker/test/run-a2.sh` passes locally (10/10) — the sanctioned harness works as
      documented.
- [ ] No wrangler deploy performed (repo-only change).
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `bash sync-worker/test/run-a2.sh` (local only). If Option A: manual local check
against `wrangler dev` — publish seq=high, wait >90s staleness, publish seq=low → the page must
FOLLOW (today it freezes). NEVER against production; never `npm run test:e2e`; never
`e2e/relay-sync.test.mjs`. Footprint test — does not pin sync-worker files.

**Dependencies.** VESTIG-02 (WS6-A) rewrites the SAME `sync-worker/README.md` — land VESTIG-02
first, rebase this. Workstream 5 (worker code) and M4 (transmitterId/tiebreak) change publish
semantics — whichever option is chosen should land BEFORE M4 so the tool cannot drift a fourth
generation.

---

#### N2W-05 — Dead injected global __SIGNO_VINO_INITIAL_BOOK (zero readers repo-wide) + load-bearing 'VINO' typo grep-trap + false code comment on the invalid-code path `low` `native` `native-build`

**Problem.** The native shell injects three globals; the third, `__SIGNO_VINO_INITIAL_BOOK`, has
zero readers repo-wide since the build-374 two-book deletion — it drags a `useState`, a setter
call, and a `useMemo` dependency along as dead plumbing. Separately, the comment on the
invalid-director-code reject path claims the web "surfaces 'código incorrecto'" — UI that has
never existed (the string appears nowhere in `web/src/app.js`). (The typo-contract half of this
finding is handled by VESTIG-07 — do not duplicate its comments here.)

**Risk if left.** The false :585 comment is a documentation trap: any engineer touching director
-code UX will assume error feedback exists and build on it (this exact assumption already
propagated into older planning docs — see DIRNAT-01). The dead global invites "what feeds this?"
archaeology on every bridge change.

**Evidence (verify before editing).**
- `PdfReaderApp.tsx:1038` — `window.__SIGNO_VINO_INITIAL_BOOK = ...` injection; the ONLY
  occurrence of `INITIAL_BOOK` repo-wide (grep-verified at HEAD).
- Dead plumbing that exists only to feed it: `PdfReaderApp.tsx:90`
  (`const [initialBook, setInitialBook] = useState<BookId>(DEFAULT_BOOK);`), `:839`
  (`setInitialBook(startBook)`), `:1041` (`[initialBook]` useMemo dep; the useMemo spans
  :1033-1042).
- `web/src/app.js:227` — web reads only the OTHER two `VINO` globals (`:227`, `:2903`, `:3480`);
  no `INITIAL_BOOK` read anywhere in web/src.
- `PdfReaderApp.tsx:585` — comment: `// Unrecognized → tell the web so it surfaces "código
  incorrecto".` — false: `grep -c "incorrecto" web/src/app.js` → 0; the web `role` handler
  (`web/src/app.js:947-956`) only sets `nativeSyncRole='off'` and re-renders the badge.
- Wire-history context for step 3: old web bundles (builds 332–373) DID read the global; they can
  only run inside (a) old shells, which inject it themselves, or (b) a stale pre-374 mesh-pushed
  `Documents/WebBundle` inside a FUTURE shell — possible because `resolveBundleUri`
  (`PdfReaderApp.tsx:813-826`) prefers Documents unconditionally.

**Fix — step by step.** Two independent legs.

*Leg 1 — the false comment (WS6-A, do now):*
1. Replace `PdfReaderApp.tsx:585` with:
   ```ts
   // Unrecognized → inject role:'none'. NOTE: the web shows NO error UI for this today — the
   // role handler just clears the badge (web/src/app.js role handler). Real feedback is
   // DIRNAT-01's director-code-result event; until that lands, a rejected code is silent.
   ```
2. Coordinate with DIRNAT-01 (its fix rewrites this same reject path): if DIRNAT-01 has already
   landed at your HEAD, verify its comment is accurate instead of applying step 1.

*Leg 2 — delete the dead global + plumbing (WS6-B, native build):*
3. **Wire-safety classification: NEEDS-DEPRECATION-WINDOW (writer-side removal).** This deletes
   the WRITER of a global that old (pre-374) web copies still read. Every old copy normally
   travels with an old shell that injects it — self-consistent — so the ONLY orphan case is a
   stale pre-374 `Documents/WebBundle` running inside a >=382 shell. That copy is already broken
   in worse ways (it calls the retired geo/unlock endpoints), so `undefined` initial book adds no
   NEW failure mode — but the clean retirement is to ship this deletion in the same native build
   as (or after) the M7 Documents-bundle version gate, which evicts stale pushed bundles
   entirely.

   > **DECISION-REQUIRED (Miguel): timing.**
   > (i) *Recommended:* hold leg 2 for the M7-adjacent native batch (M3 bridge cleanup is the
   > named natural vehicle) — zero residual risk, and this is a low-value deletion with no
   > user-visible payoff that justifies its own build.
   > (ii) *Ship in the next regular build now*, accepting the documented stale-Documents residual
   > (already-broken bundles only).
   > Either way, leg 1 (the comment) ships now in WS6-A.
4. **Grep proof (MUST re-run and be clean immediately before deleting):**
   ```bash
   grep -rn "INITIAL_BOOK\|initialBook" \
     --include="*.ts" --include="*.tsx" --include="*.js" --include="*.mjs" --include="*.html" . \
     | grep -v node_modules | grep -v "docs/\|docs/ia-audit-2026-07/"
   # expected: EXACTLY PdfReaderApp.tsx:90, :839, :1038, :1041 — the four sites this step deletes.
   # Any web/src hit = STOP (a reader came back).
   ```
5. Delete, in `PdfReaderApp.tsx`: the :1038 injection line; the :90 state line; the :839
   `setInitialBook(startBook);` call; change the useMemo dep at :1041 from `[initialBook]` to
   `[]`. Do NOT remove `DEFAULT_BOOK` (:74) or the `BookId` type — both have live consumers
   (`currentBookRef` at :110 etc.). Keep the :1036/:1037 injections (live — VESTIG-07).
6. `npm run typecheck`; `node --test e2e/native-entrypoint.test.mjs` (it pins
   `injectedJavaScriptBeforeContentLoaded` exists at :24 — still true with two globals).
7. Optional hardening (pairs with VESTIG-07 step 6): pin the preload contract to exactly the two
   live globals in `e2e/native-entrypoint.test.mjs`, adding
   `assert.doesNotMatch(source, /__SIGNO_VINO_INITIAL_BOOK/);` once leg 2 lands.

**Acceptance criteria.**
- [ ] Leg 1: `grep -n "código incorrecto" PdfReaderApp.tsx` → 0 hits; the replacement names
      DIRNAT-01.
- [ ] Leg 2: step 4 grep re-run → 0 hits repo-wide; `__SIGNO_VINO_NATIVE_FILE_MODE` and
      `__SIGNO_VINO_NATIVE_BUNDLE_VERSION` still injected (VESTIG-07 pins).
- [ ] `npm run typecheck` green; `node --test e2e/native-entrypoint.test.mjs` green.
- [ ] Deletion shipped per the timing decision in step 3 (recorded in the PR description).
- [ ] Grep proofs re-run and clean; no behavior change: targeted tests still pass.

**Tests.** `npm run typecheck`; `node --test e2e/native-entrypoint.test.mjs
e2e/nearby-sync-contract.test.mjs`. Never `npm run test:e2e`; never `e2e/relay-sync.test.mjs`.
Footprint test — not affected (no file inventory change).

**Dependencies.** VESTIG-07 (same `preloadScript` block — its do-not-rename comment must survive
this deletion; it already names only the two live globals). DIRNAT-01 (rewrites the :584-587
reject path — coordinate leg 1). RELVER-06 / M7 mesh-OTA + Documents-bundle version gate (the
recommended vehicle for leg 2). M3 bridge v1 (the named long-term home for all preload/bridge
contract cleanup).

---

## 6.3 Workstream exit checklist

- [ ] WS6-A merged: every grep in VESTIG-02/-01/-03/-04/-05/-07 acceptance and N2W-05 leg 1 is
      clean; `git diff` for A shows comments/docs + one argparse default only.
- [ ] WS6-B merged: every deletion's grep proof re-ran clean AT the merge commit; footprint +
      smoke-boot + typecheck + the named safe e2e files green.
- [ ] `sync-worker/README.md` reflects BOTH edits (VESTIG-02 then VESTIG-12) without conflict
      leftovers.
- [ ] The two DECISION-REQUIRED items (VESTIG-12 option; N2W-05 leg-2 timing) have recorded
      answers from Miguel in the PR descriptions.
- [ ] No `npm run test:e2e` and no `e2e/relay-sync.test.mjs` was ever executed; no
      `test-client.html` publish ever pointed at production.
- [ ] Follow-through pointers updated: mark the P8 `#books-data` item covered by VESTIG-11; note
      FOLNAT-04 still owns `lastPagePrefix`; note M3/M7 own the VINO rename and INITIAL_BOOK
      retirement respectively.


---

## 10. Verification doctrine (applies to every wave)

### 10.1 Safe automated tests

The safe suite is the set of **network-free node contract tests**. Before running ANY test file for the first time, Read it and confirm it makes no network calls to the prod relay; then run files individually (`node --test e2e/<file>` or the pattern the file's header documents):

- `e2e/svRelayRoom.test.mjs`, `e2e/svSyncDecision.test.mjs`, `e2e/svSelftest.test.mjs` — web sync-logic contracts
- `e2e/nearby-sync-contract.test.mjs` — native mesh JS contract
- `e2e/native-entrypoint.test.mjs`, `e2e/native-stability-config.test.mjs`, `e2e/permission-flow.test.mjs` — native config/flow pins; `e2e/eas-config.test.mjs` shells out and is excluded from CI — run it only when EAS config itself changes
- `e2e/offline-books-integrity.test.mjs`, `e2e/repo-minimal-footprint.test.mjs` — asset + repo inventory pins (deletions in Wave 6 may require updating the footprint allowlist — that is an *expected* edit, not a bypass)
- `sync-worker/test/a2.test.mjs` (+ `run-a2.sh`) — worker publish/seq/rate-limit harness against **local `wrangler dev`** only
- `scripts/smoke-boot.mjs` — boot smoke; `scripts/check-book-consistency.mjs` — book asset integrity
- ☢️ `e2e/relay-sync.test.mjs` — **NEVER. Publishes to the production relay room.** Do not run it, do not "fix" it into the suite, do not copy its pattern.

If any safe file is red before your change: record it as pre-existing baseline in the PR description; fix it only if your wave touches its subject.

### 10.2 Per-surface verification recipes

- **Web**: build with `node web/build.mjs` (read its header for output dir + flags), serve the built output locally, and drive a real browser preview. Minimum per wave: boot clean profile → follower flow renders; toggle `document.documentElement.dataset.role='director'` to verify role-gated controls; exercise the wave's changed flows; check the browser console for new errors; verify iPad-portrait viewport (~834×1194) AND iPhone-width (~390) layouts. For SW-affecting changes: verify update flow in a fresh profile (no stale caches) and confirm no mid-session self-reload regression.
- **Native**: `scripts/start-ios-simulator-local.sh` (read first) → boot the shell in the iPad simulator → verify bridge-ready lands, follower UI appears, and the wave's changed bridge messages flow (Xcode console / `console.log` via Safari Web Inspector attached to the WKWebView). Director-role paths need a real director code (private); anything requiring one goes on the §10.4 device checklist for Miguel instead of being faked.
- **Worker**: `cd sync-worker && npx wrangler dev` + `curl` probes against localhost only; extend `a2.test.mjs` per the wave. Never probe the deployed prod worker except read-only `GET /health`-class endpoints, and never `POST /r/alvernia-main/publish`.

### 10.3 The adversarial re-hunt (non-negotiable)

After each wave's fixes are green: run a fresh high-effort adversarial review over the wave's **entire diff** plus the invariants adjacent to it (the chapter's *Dependencies* lines name them). Fix real findings, then **re-hunt again**. Iterate until a hunt returns clean. History in this repo: a single-pass fix set shipped 18 problems that only the second hunt caught. The clean re-hunt — not the first green — is "done."

### 10.4 Miguel-assisted device checklist (batch at the release train)

Collect per wave, execute together pre-train: real-iPad director code entry (valid + invalid), takeover between two iPads, Local-Network-permission-denied recovery, old-iPad PWA wake/Auto-Lock behavior, staging-canary ritual per the repaired runbook, fleet dashboard truthfulness during a canary window.

### 10.5 Reporting per wave

Each wave's closing summary must state: findings shipped (IDs), PR links, verification evidence (which recipes ran, what they showed), re-hunt iterations to clean, deploy go-live timestamp in **Central Time** for anything that reached prod, and the wave's outstanding DECISION-REQUIRED items.


---

## Appendix A — Real, but already tracked (do NOT re-report; land with their owners)

Verified-real behaviors whose fixes already belong to tracked work items. When you reach the tracked item, its entry here adds the audit's fresh evidence.

| ID | What was found | Already tracked as / by |
|---|---|---|
| ROLEWEB-03 | Green "En vivo" pill never demotes when the follower's OWN network dies — false live signal (bare `catch{}` in `relayPollOnce`; `hasDirector` only changes on snapshot arrival) | Prior-art known finding (offline-freshness family) — fold into the M4 status-pill work |
| ROLEWEB-04 | Follower's entire live-status surface is an unlabeled 8×8px dot that is its own tap target | Planned M4 "always-visible tri-state status pill" (`docs/major-update-2026-07.md`) — M4 spec covers it |
| FOLWEB-06 | Offline readiness invisible; precache failure is console-only | Tracked offline-UX item in the hardening plan — implement with its owner |
| PARITY-04 | `?selftest` readiness card unreachable inside the native shell (query-param gate vs `file://` boot URL) | Tracked selftest/native-diagnostics item |
| W2N-02 | Reintentar/watchdog ladder never re-resolves the Documents/WebBundle → a stale OTA bundle survives retry | DELTA of tracked `native-swift-stale-documents-bundle-masks-update`; solve together with RELVER-06 (Wave 5) |
| W2N-06 | "Ya hay un director activo" takeover warning is blind to relay-only directors (`lastDirectorSnapshotRef` written only by the mesh page handler) | DELTA of tracked relay-transmitter-identity work (M4 family) |
| IANAV-05 | All non-numpad navigation (search taps, arrows, turnSong/turnPage) skips `relay.browsing` — go-live bar never offered | DELTA of tracked #263-F1 browse-mode finding — extend that fix to every navigation entry point |
| RELVER-08 | Worker (instant fleet-wide vector) has zero CI gate | Tracked `test-suite-sync-worker-zero-tests…` [H, PARTIAL] — a2 harness exists; CI wiring remains |

## Appendix B — Refuted (do NOT "fix" these)

| ID | Claim | Why it is wrong |
|---|---|---|
| FOLWEB-09 | First-load gate is blinding white in a dark church | The gate crossfades (160ms) directly into the full-screen **white songbook page image** — matching luminance. No dark shell exists at that moment; a dark gate would *add* a flash. |
| W2N-03 | Native ⟳ has no relay fallback when mesh is down but internet is up | Code claims accurate but the *fix* premise conflicts with the shell's deliberate relay-off design (web relay disabled in shell; mesh is the native transport). The real gap is covered by FOLNAT-02 / W2N-04 (mesh-failure visibility + repair), which ARE in scope. |
| W2N-05 | Transmitter-only exit-director leaves `lastRole` → false "Estabas dirigiendo" resume prompt | The only reader of `lastSyncRole` sits behind `if (!syncAvailable) return;` — on transmitter-only devices (`syncAvailable=false`) the prompt can never fire. (DIRNAT-02 addresses the *inverse*, real problem: the breadcrumb is unreadable exactly where it's written.) |

## Appendix C — DECISION-REQUIRED register (consolidated)

The 16 product forks the chapters marked **DECISION-REQUIRED (Miguel)**. Full context lives at the referenced chapter anchor; nothing below may be implemented on an assumption.

| # | Decision | Where | Writer's recommendation |
|---|---|---|---|
| 1 | DIRNAT-07 unknown-liveness grace window: copy-only now vs 3s liveness probe before the takeover confirm | WS1 · DIRNAT-07 | Copy-only now; probe in the device-gated native batch |
| 2 | Show the live relay pill on native parish iPads (web parity) or keep native chrome minimal | WS2 · FOLNAT-01 | Full web parity — one status language everywhere |
| 3 | Which visible browse entry do followers get (un-hide drawer handle vs new fab vs song-status tap) | WS3 · IANAV-02 | Un-hide the drawer handle |
| 4 | NoSleep-style muted-video wake fallback for pre-16.4 iPads | WS3 · FOLWEB-02 | Skip — battery/audio-session risk beats the win |
| 5 | Numpad behavior for in-range gap numbers (~56 unused numbers): snap to nearest + announce vs error | WS3 · FOLWEB-07 | Snap + announce |
| 6 | Canonical song term across all copy: "canto" vs "canción" | WS3 · IANAV-10 | "canto" |
| 7 | Build the web-director emergency path at all | WS4 · §3.1 | Yes — emergency-only Phase 1 |
| 8 | Who may become web-director | WS4 · §3.1 | Any valid director code |
| 9 | Web-director entry affordance | WS4 · §3.1 | Reuse the existing 5+-digit numpad path |
| 10 | A live native director exists: warn-and-allow vs block the web takeover | WS4 · §3.1 | Warn, never block |
| 11 | Web-director crash/reload resume semantics | WS4 · §3.1 | No auto-resume — re-enter the code |
| 12 | Final web-director copy set (confirm sheets, honesty chip) | WS4 · §3.1 | As drafted in the chapter |
| 13 | Fleet label end-state after #270: labels die fleet-wide + roster deviceId pin vs revive labels | WS5 · RELVER-09/FOLNAT-05 | Labels die; roster pins by deviceId |
| 14 | 24h auto-expiry for the staging-room pin on canary devices | WS5 · RELVER-02 | Yes |
| 15 | sync-worker/test-client.html: delete vs modernize (it defaults to the PROD room) | WS6 · VESTIG-12 | Delete — the a2 harness is the sanctioned tool |
| 16 | N2W-05 leg-2 (removing the native writer of the dead injected global): timing | WS6 · N2W-05 | Ride the native release train batch |


## Appendix D — Coverage notes & honest limits

- **Gap-hunt round skipped** (budget): the planned completeness-critic pass (a 13th+ lens hunting what the 12 missed) did not run. The 12 lenses covered: director UX (native), web role model, follower UX (native + web/PWA), cross-app parity, both bridge directions, sync end-to-end, failure-mode UX, IA/navigation/copy, release/versioning seams, vestigial debt. **Known un-swept angles** for an optional top-up audit: deep accessibility (VoiceOver/Dynamic Type), App Store listing vs reality, iPad multitasking/Split View, security posture re-check, director *training* materials.
- Verifier notes (`confirmed-findings.json`) occasionally narrow a finding's scope — read them before implementing any finding you find surprising.
- This manual freezes the audit at build 381. Re-run `git log --oneline <manual-merge-commit>..HEAD` before starting; if main moved into audited surfaces, re-verify affected findings' evidence first.

## Appendix E — Artifact index

- This manual: `docs/ia-audit-2026-07.md`
- Verified findings: `docs/ia-audit-2026-07/audit-brief.md` (human) · `confirmed-findings.json` (machine)
- Per-lens deep dives: `docs/ia-audit-2026-07/findings-{dirnat,roleweb,folnat,folweb,parity,n2w,w2n,synce2e,failux,ianav,relver,vestig}.md`
- Subsystem maps: `docs/ia-audit-2026-07/map-{native,web,worker,pipeline,prior-art}.md`
- Prior programs this manual interlocks with: `docs/app-hardening-plan.md`, `docs/sync-reliability-audit-2026-07.md`, `docs/major-update-2026-07.md`, `docs/audit-reconciliation-374.md`
