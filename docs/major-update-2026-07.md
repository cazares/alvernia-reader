# SignoVivo Major Update — the Release Safety System, and what ships on top of it

> **Why this document exists.** On a Wednesday practice the app was *completely busted for
> everyone* — the operator (Miguel, on bass) couldn't play the whole session, publicly, after being
> told there'd be no major issues. His stated fear is **"version incompatibility."** This update's
> organizing principle is therefore **not "zero bugs"** — we refuse to promise that, because that
> promise is exactly what caused the harm — but a **structural gate**: no change (web deploy, native
> bundle, or worker) can reach the whole choir without first being proven **GREEN on one device
> (Miguel's canary iPad) in an isolated staging channel**, and any bad change is **reversible in one
> command**. Everything else in this update — super-admin PDF upload, distribution, rock-solid sync,
> a real bridge, crash-proofing — is built to pass through that gate.
>
> **The honest promise:** *caught-on-canary + instant-rollback.* A bug can still reach the canary;
> the guarantee is that it **stops there**, one device Miguel is holding on a practice day, and can
> be undone in seconds. Say this plainly to the choir.
>
> **Provenance.** Designed by a 9-agent pass (1 grounding reader → 7 feature/system designers → 1
> synthesizer, all on Opus 4.8, reading real code at HEAD `f3d77193`, build 374). Read-only; every
> claim is grounded at a real `file:line`. This builds ON [`app-hardening-plan.md`](app-hardening-plan.md)
> (build-374 reconciled) and reuses its findings by ID (P1, P2-*, P3-*, A3, NEW-DIR-1/2/3, P-OTA,
> P-STAGING, P6-LOG, …). Line numbers need per-item re-verification before coding (plan §3.2).
>
> **Deployment ground truth this is designed for** (Our Lady of Guadalupe, Del Rio TX): Mass Sun
> 12:30pm + Thu Holy Hour; operator setup window is tight (12:00–12:15). ~6–8 singers (4–6 iPads,
> 2–4 iPhones), 3 player/singers, **exactly one** director (big iPad hotspotted off his iPhone).
> **The iPads have NO wi-fi inside the church** — page-turn sync runs over the **Multipeer mesh**
> (Bluetooth / peer-to-peer wifi), NOT the internet relay. The relay serves web followers and
> Miguel's tethered devices. **Practice is Wed + Sat, 1–2h, with wifi** — the only safe window to
> roll out and verify a new build.
>
> Created 2026-07-04.

---

## 1. North star + the honest promise

**North star:** *No change reaches the choir until it is proven green on one canary device in an
isolated channel, and every change is reversible in one command.*

That single sentence is the whole update. The features are downstream of it. Concretely it means
four structural guarantees, none of which is "we tested it and it's fine":

1. **A test that would have caught Wednesday.** A headless *boot smoke test* (`scripts/smoke-boot.mjs`)
   builds the real bundle, loads it, and asserts **page 1 renders**, `#pages-data` parses, and the
   native bridge handshake fires — in CI, on every PR. A bundle that is "busted for everyone" is red
   before it can merge.
2. **An isolated staging channel.** A change ships to a Cloudflare Pages **preview** branch and a
   **separate relay room** (`alvernia-staging` / `alvernia-practice`) that is a *different Durable
   Object instance* — so a broken canary **physically cannot** publish to a live-Mass follower.
3. **A canary that goes first.** Miguel's own (oldest) iPad updates and is verified **GREEN** via a
   one-tap self-test, on a **practice day**, before anyone else's device is touched.
4. **Version incompatibility as graceful degradation.** Every wire payload is **additive-only and
   forward-renderable**: an old follower + a new director *still see pages*, with a soft "update when
   you can" banner — never a hard break. This is the exact fear, answered by contract.

We do **not** claim zero regressions. We claim any regression is caught on the canary and reversible
in one command. (See §10 for what can still go wrong even with all of this.)

---

## 2. Deploy surfaces + the three-room model

The plan's deploy-surface concept (`app-hardening-plan.md` §2) is *more* central now, so re-state it
and add the room model this update introduces.

| Surface | Reaches | Latency | The safety lever |
|---|---|---|---|
| **worker** (`sync-worker/`) | everyone, server-side | instant | `wrangler rollback` / re-deploy |
| **web** (`web/src/*`, `build.mjs`) | signovivo.com followers instantly; **iPads only on next archive/OTA** | instant (web) / next build | Pages keeps every deployment → one-click re-promote |
| **native** (`PdfReaderApp.tsx`, `ios/**`) | parish iPads only | next TestFlight build | canary installs first; boot-watchdog auto-reverts to baked bundle |
| **book content** (NEW: R2 + version pointer) | everyone with wifi, no code deploy | next `/books/version` poll | old version stays in R2 → rollback = pointer flip |

**The three-room model (zero worker change).** Today production is one relay room `alvernia-main`.
The worker's route regex (`sync-worker/src/index.ts:338`) already accepts **any**
`[A-Za-z0-9_-]{1,64}` room name and spins up a fresh `SyncRoom` Durable Object by name. So:

- `alvernia-main` — **production / Mass.** The only room the group is on for a real liturgy.
- `alvernia-staging` — **canary.** Where a new build is proven on Miguel's one device.
- `alvernia-practice` — **group practice mode** (Wed/Sat), reachable via `?practice=1` / a Practice
  Mode toggle.

Each room is a separate DO instance — a hard wall. A director publishing in `alvernia-staging`
cannot move a follower in `alvernia-main`. **No worker code changes** to add rooms; only the client
picks the room name via a `getRelayRoom()` resolver (replacing the literal at `app.js:2765`,
`directorRelaySync.js:13`, and the native path). Pages branches mirror this: `main` = production
(signovivo.com), a preview branch = `*.alvernia-reader.pages.dev` that the group is never on.

**The offline church path is untouched by all of this.** Mesh page-turns proceed exactly as today;
staging/CI/rollback are *with-wifi practice-day tools* that never run during Mass. The version-compat
degradation (§1.4) is what protects an offline follower on an old build — it renders the director's
page from the mesh integer payload regardless of version skew.

---

## 3. Dependency order — build it un-break-able first, then add power

The seven designs are **not** independent features to pick from; they have a correct order. Each
layer makes the next one safe to add.

```
0. RELEASE SAFETY SYSTEM   ── the gate. CI + boot-smoke + staging room + rollback + additive version-compat.
                              Nothing below ships to the group until this skeleton exists.
        │
        ▼
1. BRIDGE (Ask 4) + CRASHES (Ask 5)  ── the fail-soft spine. A typed/acked bridge whose handshake lets
                              native re-assert authority (structural A3 fix), and error boundaries so a
                              shared-bundle bug becomes "banner + auto-recover", not a white screen.
        │
        ▼
2. BOOK OUT OF THE BUNDLE (Scenario 1)  ── move book content to R2 + a versioned pointer, gated by a
                              super-admin dashboard with a git-diff "additive-only" review.
        │
        ▼
3. DISTRIBUTION (Scenario 2)  ── atomic, hash-verified, no-half-books delivery of a new book version to
                              every device (web swap + offline mesh push), never mid-song.
        │
        ▼
4. SYNC ROBUSTNESS (Scenario 3)  ── idempotent full-state heartbeats, a loud honest status pill, and
                              auto-recovery; folds in P2-SEQ/IDENTITY/POLL/CLOCKSKEW + A3 + NEW-DIR-1/2/3.
        │
        ▼
5. THE RECIPE + DIAGNÓSTICO (Ask 7)  ── the Wed/Sat playbook, the LISTO/NO-LISTO readiness screen, Practice
                              Mode, and director panic buttons that tie it all together for the operator.
```

Why this order: the **safety system** must exist before anything ships (or a half-built feature
becomes its own footgun). The **bridge + crash layer** underpins sync *and* distribution (both send
messages and must never white-screen). **Book-out-of-bundle** must exist before **distribution** has
anything to distribute. **Sync** hardening is safest once the bridge handshake (its A3 fix) is in.
The **recipe/DIAGNÓSTICO** is the human glue that makes the whole thing usable in a 15-minute setup
window — some of it (the recipe doc, Practice Mode) can start *today* as process, and gets teeth as
the code lands.

---

## 4. Milestones (M0–M7)

Each milestone ships independently and is **provable before the group sees it**. Native items are
batched into **one 2-device day** (M7) because a Swift-mesh change can't be proven in a simulator.

| # | Ships (what the parish gets) | The gate that proves it safe | Surface |
|---|---|---|---|
| **M0** | CI (typecheck + safe e2e subset) + `scripts/smoke-boot.mjs` on every PR; the boot-smoke that would've caught Wednesday | A red check blocks merge; deliberately corrupt `pages.json` → smoke reds | dev/repo |
| **M1** | Staging channel: `?env=staging`/`?practice=1` room resolver + `STAGING=1` release path + `?selftest` GREEN/RED card + `scripts/rollback-web.sh` | Deploy to staging, confirm signovivo.com untouched; two devices sync in staging without affecting a prod follower; practice a one-command rollback | web + dev |
| **M2** | Crash-proofing (web): top-of-file `bootGuard`, guarded localStorage, `res.ok` fetch guards, recovery banner + bounded auto-reload; crash telemetry to a gated `/log` + fleet "Recent crashes" panel | Boot-smoke + a `localStorage-throws` unit test; force a synthetic crash → banner + one recovery, and it shows on the dashboard | web + worker |
| **M3** | Bridge v1 (web half): shared `bridge-protocol.js` (typed, validated, never-throws, acked), single web dispatcher, `hello`/`welcome` handshake stubs; legacy adapter so an un-updated native shell still works | `bridge-protocol.test.mjs` in CI (drops malformed, clamps ranges, A3 authority rule, AckTracker retries) | web |
| **M4** | Sync robustness (web + worker): freshness-before-seq (P2-SEQ), clock-skew offset (P2-CLOCKSKEW), poll-gap fix (P2-POLL-GAP), the always-visible tri-state status pill, transmitterId + two-publisher tiebreak (P2-IDENTITY relay half) | `syncDecision` pure fn unit-tested against the **local** relay harness (never the prod room); on staging, demotion now fires on a healthy socket (kills the "green pill on a dead director" freeze) | web + worker |
| **M5** | Book out of the bundle (Scenario 1), read-only first: R2 + `__book__` DO + `GET /books/version` + versioned page URLs; the baked book uploaded as bookVersion 1; readers prefer R2 online, fall back to baked offline | Online device fetches identical pages from R2; offline device unchanged; harness tests for the version pointer + fallback | worker + web |
| **M6** | Super-admin dashboard + distribution (Scenario 1 publish + Scenario 2 consume): PIN+password admin, client-side pdf.js render, git-diff additive-only review, atomic publish/rollback; `bookVersion` on the heartbeat + atomic hash-verified web swap + "actualizar" banner + fleet bookVersion column | Additive-only diff gate blocks any silent page change (typed confirmation to override); half-upload rejected; canary-walk a real N-1→N update on the oldest iPad, page-turn <300ms; rollback = one pointer flip | worker + web |
| **M7** | **The 2-device day (all native, one TestFlight build):** bridge native dispatcher + `hello`/`welcome` with native-computed authoritative page (**A3 fix**); WebView error boundary + content-process recovery; **NEW-DIR-1/2/3**; mesh payload gains `seq`/`epoch`/`transmitterId` (inside v=1); mesh bundle push gains per-file `sha256` + signature + super-admin arm; boot-watchdog auto-rollback to baked; native DIAGNÓSTICO + Practice Mode + panic buttons | On two physical iPads offline: director on page 250, force a WKWebView content-process reap → followers **stay on 250**, relay `/state.page` never becomes 1 or 2 (A3). Force-quit+relaunch the director → visible resume prompt, not a silent follower (NEW-DIR-1). A directs then exits, wait >2s, B enters a code → calm "¿Dirigir el coro?" not the red takeover warning (NEW-DIR-3). Corrupted mesh page → follower rejects it. Bricked Documents bundle → auto-reverts to baked. **Ship only in a Wed/Sat window, never near a Mass.** | native (TestFlight) + device-gated |

---

## 5. Cross-cutting contracts (the single source every slice must agree on)

**The one rule that makes version-incompatibility safe: every wire payload is ADDITIVE-ONLY and
FORWARD-RENDERABLE.** New fields are optional; existing fields (`page`, `totalPages`, `seq`) are
**never removed or retyped**; the mesh `protocolVersion` / relay `v` are **never bumped** (a bump
would break a mixed fleet — Swift drops payloads whose `v` differs). New capabilities ride *inside*
the existing envelope. Old clients ignore unknown fields; a version mismatch is a soft banner, never
a hard break. **This discipline must be enforced by the boot-smoke test and code review forever.**

Distinct version layers — never conflate them:

| Layer | What it versions | Where |
|---|---|---|
| `CACHE_VERSION` | the web **shell** (app.js/sw.js/css/html) | `build.mjs:10-28` (fix P3-CACHEVERSION: hash the whole `dist/` tree) |
| `bookVersion` | **book content** (pages + manifests) | R2 keys + `GET /books/version` + `__book__` DO |
| `CFBundleVersion` | the **native build** | `version.json` |
| `PROTOCOL_VERSION` (`v`) | the **relay wire** | `index.ts:29` — additive read on follower, never bumped |
| `BRIDGE_VERSION` (`bv`) | the **native↔web bridge** contract | `bridge-protocol.js` (new) |

Shared shapes:

- **Relay room names:** `alvernia-main` / `alvernia-staging` / `alvernia-practice` — isolated
  `SyncRoom` DO instances on the **same** worker (route regex `index.ts:338`, zero worker change).
  Resolved by `getRelayRoom()` reading `?env=staging` / `?practice=1` / `localStorage sv_practice`;
  unreadable/absent → `alvernia-main`.
- **Relay Snapshot** (`index.ts:36`, `EMPTY_SNAPSHOT :48`): keep `v/page/totalPages/mode/bookId/seq/ts`;
  **ADD** `bookVersion:string` and `transmitterId:string`, both `.slice(0,64)`, default `""`.
  `page`/`totalPages` never removed. `snap.v` becomes a **read** contract on the follower (soft
  update banner on mismatch, never a hard break).
- **Mesh page payload** (`DirectorSyncModule.swift:131`): **ADD** `seq:Int`, `epoch:String(≤64)`,
  `transmitterId:String(≤64)` **inside the existing v=1 envelope** — additive, old shells read
  `?? default`. `protocolVersion` (swift:11) **stays 1**. The existing `bundleVersion` (swift:139) is
  the parallel for book/bundle version. Mesh pack manifest (swift:800/831) extended `{path,len}` →
  `{path,len,sha256}` + a top-level signature verified before `installReceivedBundle` (swift:879);
  `bundle_offer` (swift:708) gated to super-admin authority + explicit arm.
- **Bridge envelope:** `{channel:"signovivo-native", bv, type, id, ack?, payload}`. `channel`
  **unchanged** (`app.js:177`) so a mismatched-build message still drops at the outer gate. ACKs only
  on `role` / `welcome` / `book-version-swap`, **not** page-turns. Missing `bv` ⇒ treated as legacy,
  tolerated, never fatal. `welcome` carries `authoritativePage` (native-computed — the A3 authority
  carrier) + `versionCompat: "ok"|"web-old"|"native-old"`.
- **Book addressing:** R2 keys `books/<bookVersion>/pages/page-NNN.<hash>.webp` +
  `books/<bookVersion>/{pages,search-index,song-titles,song-search-index,hashes}.json`, all immutable
  and version-scoped (subsumes P3-IMMUTABLE-PAGES). `bookVersion = bv_<first-16-hex of sha256 over the
  sorted per-page-hash + manifest-hash list>` (content-addressed, idempotent re-upload).
  `hashes.json = {bookVersion, totalPages, pages:{"1":"<sha256hex>",…}}` is the diff-engine baseline.
  `GET /books/version → {bookVersion, totalPages, publishedAt}` public, `no-cache`. **The baked bundle
  is kept FOREVER as the offline floor**; a 404 on a versioned URL falls back to baked.
- **Storage + telemetry:** `localStorage ACTIVE_BOOK_VERSION_KEY` + per-version page cache
  `signo-vivo-pages-book-<N>` (keep N and N-1 for instant rollback); `sv_practice` flag.
  `FleetDevice` + `fleetCheckin` gain `bookVersion` + `battery(0-100)` + `peerCount(0-64)` +
  `ready(bool)`, sanitized with the existing clamp (`index.ts:211-218`); old clients omit → shown as
  "—". Crash log to `/log`: `{kind:"crash", dev:<uuid>, build:<CACHE_VERSION>, bookVersion, where,
  msg, stack(≤600), url, t}` — **no name/phone**. `GET`/`DELETE /log` require `X-Fleet-Key` (header,
  not `?k=`); `POST` capped; native `dbgLog` **stops sending `UIDevice.current.name`**.
- **CI safe-test allowlist (hard contract):** named e2e files only (`offline-books-integrity`,
  `native-stability-config`, `native-entrypoint`, `repo-minimal-footprint`) + worker/appjs units +
  `scripts/smoke-boot.mjs`. The `npm run test:e2e` **glob is FORBIDDEN** in CI and any automation — it
  publishes to `alvernia-main` and flips live followers.
- **Native boot-watchdog:** `bridge-ready` within N seconds = healthy; **second-consecutive** timeout
  = "busted" → auto-revert `Documents/WebBundle` to the baked bundle, reload. A build-baked
  `SYNC_STRICT` boolean (default true) is a one-line kill-switch collapsing new adoption logic to
  today's freshness-only path.

---

## 6. The feature designs (implement from these)

Each subsection is implementable cold. Reused audit items are named by ID; open the plan for their
full fix text.

### 6.0 Release Safety System (the gate) — effort L

Six parts, each independently shippable:

**Part 1 — Staging (separate deploy + room).** `getRelayRoom()` resolver replaces the literal
`RELAY_ROOM` at `app.js:2765` (and `directorRelaySync.js:13`, native path): `?env=staging` →
`alvernia-staging`, else `alvernia-main`. A `STAGING=1 SKIP_NATIVE=1 bash scripts/release.sh` path
deploys `--branch staging` (preview URL), never prod, never TestFlight.

**Part 2 — Canary protocol.** A build is proven on **one** device (Miguel's oldest iPad) in staging
before anyone else's device is updated. Web: deploy `--branch staging`, open the staging URL /
`?env=staging`, run `?selftest`, confirm GREEN, then promote. Native: TestFlight installs to Miguel's
Apple ID first; he runs the offline self-test with a **second** physical device (the mesh requires 2
devices) before the choir pulls the update on the next practice day.

**Part 3 — Automated gates (what would've caught Wednesday).** `.github/workflows/ci.yml` (reuse
plan **P1-CI**): `npm ci`, `npm run typecheck`, the **safe** e2e subset (named files, **never** the
glob), plan **P1-HARNESS**/**P1-WORKER-UNIT**/**P1-APPJS-UNIT**, and the new **boot smoke test**
`scripts/smoke-boot.mjs`: runs `node web/build.mjs`, loads `dist/index.html` in jsdom, asserts
`#pages-data` parses to a positive-int `totalPages`, `pageFileName(1)` resolves to a file that
**exists** on disk at `dist/books/standard/pages/page-001.webp`, and the app posts a `bridge-ready`
with `channel:"signovivo-native"`. *Had this existed Wednesday, a bundle that couldn't render page 1
or post bridge-ready would have been red in CI.*

**Part 4 — Instant rollback.** Web: `scripts/rollback-web.sh` lists Pages deployments and prints the
one-line re-promote of the previous good deployment (the dashboard "Rollback to this deployment"
button is the instant path; keep the last-good `dist/` tarball as belt-and-suspenders). ~60s to
online followers via stale-while-revalidate. Native: the P-OTA boot-watchdog reverts a bricked
`Documents/WebBundle` to the App-Store-reviewed baked bundle on next launch, plus a baked-vs-Documents
version compare (fixes `native-swift-stale-documents-bundle-masks-update`).

**Part 5 — Version-compat as graceful degradation.** The worker already stamps `snap.v`
(`index.ts:29,124`) but no follower reads it. Add a soft check in `applyRelaySnapshot` (`app.js:3022`):
if `snap.v > OUR_PROTOCOL_VERSION`, still render `snap.page` and show a non-blocking banner (reuse the
`relay-auth-error` banner plumbing, `app.js:891`) — **never drop the page**. Add `bridgeVersion` to
the bridge `hello`/`welcome` (Ask 4); mismatch keeps page sync, logs skew, never hard-breaks.

**Part 6 — Pre-Mass verification ritual.** A `?selftest` route renders a big GREEN/RED card: page 1
renders, `totalPages` matches, relay `/state` 200 (on the *current* room, so `?env=staging&selftest`
tests staging), bridge-ready posted (native), build matches `version.json`. Plus
`docs/pre-mass-checklist.md` (see §7). *Note: `?selftest` replaces the dead/vestigial `?admin`.*

**Reuses:** P1-CI, P1-HARNESS, P1-WORKER-UNIT, P1-APPJS-UNIT, P-STAGING, P-OTA, P3-CACHEVERSION,
P2-SEQ, P2-CLOCKSKEW, A5, `native-swift-stale-documents-bundle-masks-update`.

### 6.1 Scenario 1 — Super-admin PDF upload + git-diff review — effort XL

Move book content **out of the baked bundle** into Cloudflare R2 + a versioned pointer; gate edits
behind a super-admin dashboard whose diff engine enforces *additive-only*.

- **Admin surface + server-side 2-factor auth.** New `web/src/admin.html` + `admin.js` (own bundle) at
  `signovivo.com/admin` — cosmetic-only; every privileged action hits the worker. Two new worker
  secrets `ADMIN_PIN` + `ADMIN_PASSWORD` (both required; modeled on the `FLEET_DASHBOARD_KEY` pattern,
  **not** on public transmitter codes). `POST /admin/session {pin,password}` → constant-time compare →
  an **httpOnly, Secure, SameSite=Strict** cookie holding `HMAC-SHA256(key=ADMIN_PASSWORD, {exp:+30min,
  nonce})` (a cookie, **not** `?k=`, per P6-FLEET-KEY). Every `/admin/*` route verifies it. Fail-closed
  if a secret is unset.
- **Storage.** `wrangler.jsonc` gains an R2 binding `BOOK_PAGES` (bucket `signovivo-books`) and a fixed
  `__book__` DO instance (same class-by-name trick as `__fleet__`) holding `book_current` +
  `book_versions`. `GET /books/version` (public, `no-cache`); `GET /books/:version/pages/*` +
  `/books/:version/*.json` (public, immutable — safe because version-scoped).
- **Client-side render engine** (`admin.js`, no server compute — works from a tethered iPad): vendored
  pdf.js (same-origin, no CDN) renders each page to canvas at the **same effective resolution the build
  uses** (`build.mjs:91` `PDF_RENDER_DPI=115`) → `canvas.convertToBlob({type:"image/webp",
  quality:0.60})` (matches `WEBP_QUALITY`). Regenerate the 4 manifests in-browser via a **shared module
  `web/src/bookManifest.js`** (the heuristics ported out of `build.mjs:156-536`, imported by *both*
  Node build and browser admin so outputs match).
- **The diff engine (the safety of Scenario 1).** Compute `sha256` of every rendered page and compare
  against the live book's `hashes.json`. Classify each page **identical / NEW / MODIFIED / REMOVED**
  and show a git-diff summary ("pages 1–371 identical ✓ · 372–373 NEW · 0 modified · 0 removed"). **If
  any MODIFIED or REMOVED: the publish button is DISABLED**, a red banner lists the changed pages with
  side-by-side thumbnails, and Miguel must type a confirmation phrase to override. Additive-only
  (all-identical + trailing NEW) publishes with one normal confirm. *This enforces "an edit is
  additive-only unless explicitly confirmed."*
- **Atomic publish + instant rollback.** Upload to a staging R2 prefix; `POST /admin/publish` verifies
  every expected object exists (rejects a half-upload), promotes staging → `books/<v>/`, then a **single
  `__book__` DO write** flips `book_current`. Old versions are **never deleted** → `POST /admin/rollback`
  is a pointer flip. Readers poll `/books/version` and transition atomically (pages are version-prefixed
  URLs, so no reader ever sees a mixed set).
- **Subsumes** P3-IMMUTABLE-PAGES (version-prefixed immutable URLs) and the **book-content half of
  P-OTA** (iPads read the same `/books/version` on practice-day wifi). The **baked bundle stays forever**
  as the offline floor. Native code/Swift is **not** touched by this scenario.

**Reuses:** P3-IMMUTABLE-PAGES, P3-CACHEVERSION, P-OTA, P-STAGING, P1-HARNESS, P1-WORKER-UNIT,
P6-FLEET-KEY.

### 6.2 Scenario 2 — Smart, atomic, hash-verified distribution — effort L

Get every device onto the new book version **atomically** — a device has *fully* version N or *fully*
N-1, **never a half-book** (a mixed book is a Wednesday-class failure).

- **bookVersion rides the existing heartbeat** (no new channel): add `bookVersion` to the relay
  `Snapshot` and the mesh `pagePayload`; the transmitter stamps it from its loaded manifest. The
  follower reads it in `applyRelaySnapshot` *after* the freshness/seq gate and sets
  `state.updateAvailable` — **passive advertisement that never blocks page rendering.** A version
  mismatch must never stop a follower from following (the page **number** is the sync unit, stable
  across a content re-render).
- **Atomic web swap:** stage version N into a **separate** cache `signo-vivo-pages-book-<N>` (never
  touch the active N-1 cache), fetch every page with the concurrency-4 loop, **SHA-256 verify each page
  against the manifest** (mismatch → discard + re-fetch, bounded), gate on **completeness** (manifest +
  every page 1..totalPages present), then a **single atomic flip** (`ACTIVE_BOOK_VERSION_KEY` = N) + a
  **data-swap** in the live DOM (re-hydrate manifest, re-render current page — no reload needed because
  page URLs are hash-keyed). Hard reload is the gated fallback. Evict N-1 only after N is confirmed
  rendering.
- **Offline church path (mesh):** primary route is *everyone updates at home/practice over wifi first*
  (the pre-practice diagnostic proves it). Straggler safety net: the super-admin/director device pushes
  version N over the mesh — reusing the existing `installReceivedBundle` atomic stage-then-swap
  (`swift:879`), hardened into the **safe** version of the flagged `native-swift-peer-bundle-
  unauthenticated-exec`: extend the pack manifest to carry per-file `sha256`, **verify every file's hash
  before the swap**, and **sign the manifest** so only a super-admin device produces an acceptable
  bundle. **Off by default; armed only by an explicit super-admin "enviar actualización" — never
  automatic mid-liturgy.**
- **"Download before practice" the group follows:** an in-app dismissible "Nueva versión — Actualizar"
  banner (wired to the currently-dead `setOfflineGateState` progress UI, `app.js:446`), plus a
  `bookVersion` column on the fleet dashboard flagging any device not on N in red (the straggler
  catcher).
- **Never swap mid-song/mid-Mass:** a `canApplyUpdateNow()` predicate (idle = `!relay.hasDirector` AND
  no recent page turn) gates both the data-swap and the reload fallback — which also fixes
  `perf/offline-mid-mass-deploy-force-reload` (`app.js:1999`).

**Reuses:** P3-IMMUTABLE-PAGES, P3-CACHEVERSION, P3-SW-LIFECYCLE, P5-DEVICE-GATED (`peer-bundle-exec`),
P-OTA.

### 6.3 Scenario 3 — Rock-solid connection, reconnection, desync detection — effort L

The single most robust choice: **every heartbeat carries the complete authoritative state**, so a
device that missed *any* packet self-corrects within one interval. No delta-only sync.

- **Idempotent full-state heartbeats.** The 1s mesh re-send and 12s relay re-publish already exist and
  already carry full state; add the fields that let a follower **detect desync deterministically**:
  `seq` + `transmitterId` + `epoch` (a per-director-session UUID so a seq reset on handoff is
  unambiguous), on both the mesh payload and the relay snapshot.
- **The desync rule (one pure function `syncDecision(local, hb)`, unit-tested, mirrored in Swift):**
  1. **Freshness first** (fixes **P2-SEQ**): `fresh = hb.seq>0 && (skewAdjustedNow - hb.ts) ≤ LIVE_MAX_AGE`.
     Not fresh → `{status:"manual", apply:false}` regardless of seq. *(This is the P2-SEQ fix — evaluate
     freshness/demotion BEFORE the seq guard; today `app.js:3033` returns at the seq guard first.)*
  2. **Epoch/identity** (fixes **P2-IDENTITY**): different `epoch`/`transmitterId` + fresh → a new
     director session (handoff); adopt wholesale even if `hb.seq < local.seq`. Tiebreak two
     simultaneously-fresh publishers: newest `ts`, then `transmitterId` lexicographic (mirror the worker
     so both ends agree).
  3. Same epoch, `hb.seq ≤ local.seq` → idempotent de-dupe (`{status:"live", apply:false}`); `hb.seq >
     local.seq` → **missed a page-turn** → `{status:"live", apply:true, page:hb.page}`, self-correct.
  4. Same seq but `hb.page !== local.appliedPage` (a dropped render / render-failed sentinel) →
     `{apply:true}`, re-home within one heartbeat.
- **Loud, correct status pill** on every device (replaces the 8px dot): tri-state **green "● En vivo
  con el director"** / **amber "◌ Reconectando…"** / **red "○ Sin director — modo manual"**, driven by
  heartbeat freshness; the director sees **"● Dirigiendo — N conectados"** (`peerCount`). A new 4s
  freshness-decay timer makes the demotion reachable on a healthy socket (today the only inbound is
  same-seq ping replies dropped at `:3033`).
- **Auto-recovery:** keep the mesh's existing 3s half-open watchdog (wire it to the amber pill); fix
  **P2-POLL-GAP** (`app.js:3108`) so `/state` polling stays alive until the WS is actually `OPEN`.
- **Fold in the native fixes (M7, device-gated):** **A3** (bridge-ready re-broadcast — don't broadcast a
  boot page; broadcast the persisted last page, and validate `msg.page`/`totalPages`), **NEW-DIR-1**
  (resume-director banner on restart), **NEW-DIR-2** (bump `roleGenerationRef` only after the confirm),
  **NEW-DIR-3** (clear/timestamp `lastDirectorSnapshotRef` so the takeover warning stops false-firing).

**Reuses:** P2-SEQ, P2-IDENTITY, P2-POLL-GAP, P2-CLOCKSKEW, A3, NEW-DIR-1/2/3.

### 6.4 Ask 4 — A real native↔web bridge — effort M

Replace the ad-hoc `postMessage` bridge with a typed, versioned, acked protocol + a single validating
dispatcher each side, in a **unit-tested pure module** (`web/src/bridge-protocol.js`) so the bridge
can't silently regress.

- **Envelope:** `{channel:"signovivo-native", bv, type, id, ack?, payload}`. `parseEnvelope(raw)` does
  JSON.parse + channel check + version compat + per-type schema validate and **never throws** (returns
  `{ok:false, reason}` → the dispatcher logs + drops). Each schema `validate` **coerces-and-clamps**
  (clampPage, sanitizeTotal, force `book="standard"`) — killing `native-swift-bridge-ready-unclamped-
  total` and `native-swift-page-changed-clamp-stale-total` at the boundary.
- **Single dispatcher** each side (`webDispatch` / `nativeDispatch`) replacing the if-chain
  (`app.js:867`) and switch (`PdfReaderApp.tsx:553`); handlers receive already-sanitized values. Old
  `set-book` no-op is dropped from the schema (an old shell sending it fails parse → logged, correct
  single-book behavior). A Phase-2 legacy adapter wraps envelope-less messages so an un-updated peer
  keeps working.
- **The A3 structural fix — native re-asserts authority on the handshake.** Web boots and sends `hello`
  `{bv, webBuild, bootPage(a proposal), totalPages}`. Native replies `welcome` (requiresAck) `{bv,
  nativeBuild, role, authoritativePage, totalPages, bookVersion, versionCompat}` where
  `authoritativePage` is computed from **native's own truth** (director → persisted `currentPageRef`,
  never a web boot value; follower with a snapshot → `lastDirectorSnapshotRef.page`; fresh follower with
  none → echoes `bootPage`, the only case the web boot page survives). On a WKWebView content-process
  reload, native re-sends `welcome` with its real page — **the congregation is never yanked to page 2.**
- **ACKs** on `role`/`welcome`/`book-version-swap` only (a bounded `AckTracker`: 1.5s timeout, 2 retries,
  then `onGiveUp` degrades — never blocks the liturgy). Page-turns are **not** acked (the 1s heartbeat is
  their reliability layer). The existing bounded pre-ready inject queue is retained (holds envelopes,
  flushes on the handshake).

**Reuses:** P1-APPJS-UNIT, A3, `native-swift-bridge-ready-unclamped-total`,
`native-swift-page-changed-clamp-stale-total`, P-RELEASE-ENGINE (generate the TS mirror at build).

### 6.5 Ask 5 — Prevent crashes / never white-screen — effort L

The top crash class is a throw during **module evaluation** of the shared `app.js` — it white-screens
*every* device at once (the most likely Wednesday cause). Confirmed trigger: unguarded top-level
`localStorage.getItem` at `app.js:331` and `:339` (throws in Safari private mode / storage-disabled) —
aborts the script tag so `initReader` and the 4s net at `:3335` never run.

- **Slice A (web, highest value): global fail-soft.** Insert a tiny `bootGuard` IIFE as the **first**
  executable code (before `app.js:2`), inlined so it survives even if the rest never evaluates: arms
  `window.onerror` + `unhandledrejection` → `recover()` (hide gates, `reportCrash`, show a Spanish
  recovery banner with a manual "Reintentar"). Guard the two white-gate-brick reads directly; add
  `res.ok` guards to the boot fetches (`app.js:3231/1153/3284`). Staged auto-recovery with **bounded**
  backoff (in-place re-render → soft reload, max 3 in 60s, then stop and leave the manual button — a
  reload loop is itself a "busted" state).
- **Slice B (native, M7): error boundary + WebView recovery.** A `WebViewErrorBoundary` around the
  `<WebView>` (remount via the existing `mountKey`, never render blank); harden the `ErrorUtils` handler;
  `onContentProcessDidTerminate` → reload + **re-assert via the versioned handshake, never a stale
  broadcast** (the A3 fix); if `bridge-ready` doesn't arrive in 4s, bounded remount then the native
  fallback view.
- **Slice C (web): defensive guards as a pattern** — `safeFetchJson`, `safeLS`, per-branch try/catch in
  the bridge handlers, render path → `__svRecover`.
- **Slice D (telemetry): Miguel SEES the crash.** *First* fix P6-LOG (gate `GET`/`DELETE /log` behind
  `X-Fleet-Key`, cap `POST`, no names). Then `reportCrash()` → `/log` `{kind:"crash", dev, build,
  bookVersion, where, msg, stack≤600, url, t}` (best-effort, debounced), and a "Recent crashes" panel on
  the fleet dashboard. Now "device X crashed at 12:05 on build Y" is a glance, not a guess.

**Reuses:** P5-ONERROR, P6-LOG, P6-FLEET-KEY, A3, `native-swift-bridge-ready-unclamped-total`, P-OBS.

### 6.6 Ask 7 — Rollout recipe + DIAGNÓSTICO + Practice Mode — effort L

- **DIAGNÓSTICO / ESTADO screen** (read-only local state, cannot break sync): opened by a **900ms
  long-press on the build-number label** (deliberately obscure). One glance: big **LISTO / NO LISTO**,
  app version, `bookVersion`, offline-ready (371/371), mesh peer count, relay status + live page, **¿En
  sync? SÍ/NO**, battery. `LISTO` iff version matches the group, offline-ready, mesh peer ≥ 1 (or only
  device up), battery ≥ 25%. It fetches the version pointer so an old build shows **"NO LISTO —
  actualiza, los demás están en v374"** — *version-incompatibility made visible on the device itself,
  pre-Mass.* Piggybacks the readiness onto the existing fleet check-in → dashboard "LISTOS: 6/7" header.
- **Practice Mode:** `?practice=1` / a toggle sets `getRelayRoom()` → `alvernia-practice` (same worker),
  pins an amber "MODO ENSAYO" banner so the room is always visible, and **auto-forces OFF** during
  Sun/Thu Mass windows (belt-and-suspenders). Mesh is unaffected (Bluetooth peers don't care about room
  names).
- **Director panic buttons** (reuse the existing broadcast path): **"TODOS RE-SINCRONICEN AHORA"**
  (force-republish the current snapshot) and **"PÁNICO: todos a mi página"** (same + clear followers'
  `browsing` state). Plus a web **"update ready" chip** replacing the silent force-reload (gated to not
  fire while following a live director).
- **Ops artifacts (zero code):** a printed QR taped by Miguel's gear that deep-links TestFlight (iPads)
  / signovivo.com (phones) for install/update.
- **Explicitly out of scope (gold-plating for a 7-person choir):** a remote-config service, a custom
  OTA server beyond P-OTA, per-device targeted rollouts, A/B percentages, an analytics pipeline.

**Reuses:** P-STAGING, P6-FLEET-KEY, P3-SW-LIFECYCLE, P1-APPJS-UNIT, B-DEPLOYWEB.

---

## 7. The rollout recipe (Wed/Sat) + the pre-practice checklist

> Ships as `docs/rollout-recipe.md` + a laminated one-pager. A stripped version is usable **today** as
> a manual process; it gets teeth as Practice Mode + DIAGNÓSTICO land.

**STEP 0 — never on a Mass day.** New builds roll out **only** Wed/Sat practice (wifi + low stakes),
**never** Sun/Thu.

**STEP 1 (day before, at home, wifi):** deploy to **staging** — Pages preview branch + relay room
`alvernia-practice`; build the `.ipa` locally (no EAS) and install on the **one canary iPad (the
oldest)**.

**STEP 2 (day before): canary-walk the one iPad, in Practice Mode:**
- ☐ App opens to the reader (no blank WebView).
- ☐ DIAGNÓSTICO reads **LISTO** (new build, bookVersion matches, offline-ready ✓).
- ☐ Page-turn latency < 300ms warm.
- ☐ Become director → page-turn propagates to a 2nd device over **mesh**; **force-quit the director iPad
  mid-page + relaunch → it must NOT silently demote or yank everyone to page 2** (A3 / NEW-DIR-1).
- ☐ A directs then exits, wait >2s, B enters a code → calm "¿Dirigir el coro?", not the red takeover
  warning (NEW-DIR-3).
- ☐ Follower ⟳ re-syncs.
- **Any box fails → STOP.** The group is still on the old build; nothing reached them.

**STEP 3 (practice day, ~1h before):** promote to prod **only if canary passed** — Pages `--branch
main` + TestFlight — and post the group message.

**STEP 4 (at the room, wifi present):** web followers reload (or tap the "update ready" chip); iPads
take the TestFlight update (or, once P-OTA lands, the wifi bundle swap). **Do iPad updates on wifi at
practice, never rely on mesh-push at Mass.** Open DIAGNÓSTICO on each device — everyone must read
**LISTO** and the **same** app + book version before practice starts.

**STEP 5 — straggler sweep:** open the fleet dashboard (`X-Fleet-Key` header); anyone below the max
version → poke them now while wifi is up.

**STEP 6 — rollback (always ready before you start):** web = `scripts/rollback-web.sh` re-promote (~30s);
worker = `wrangler rollback`; iPads = don't install the bad build (canary caught it) / P-OTA reverts to
baked. **Copy the rollback line into a note before Mass.**

**Group message (WhatsApp, Spanish):**
> 🎶 Coro: hay una actualización lista para HOY en el ensayo. Con wifi, abre la app y (iPad) actualiza
> en TestFlight / (teléfono) recarga signovivo.com. Revisa que abajo diga **LISTO** y la misma versión
> que los demás. Si dice NO LISTO o algo se ve raro, avísame ANTES de empezar. — Miguel

---

## 8. The 2-device day (M7)

Everything that touches the Swift mesh batches into **one** sanctioned session, two physical iPads on
one wifi, on a **practice day**: bridge native dispatcher + `hello`/`welcome` handshake (A3); WebView
error boundary + content-process recovery; **NEW-DIR-1/2/3**; mesh `seq`/`epoch`/`transmitterId`; mesh
bundle `sha256` + signature + super-admin arm; boot-watchdog auto-rollback; native DIAGNÓSTICO +
Practice Mode + panic buttons. Run the full canary-walk (§7 STEP 2) on the **oldest** iPad before the
TestFlight build touches the group. **Never ship a blind Swift change to the mesh.**

---

## 9. Decisions for Miguel (recommendations made; confirm)

1. **Director restart behavior** (NEW-DIR-1, the crux): **(a) a one-tap "Estabas dirigiendo —
   ¿continuar como director?" resume prompt on boot** *(recommended — respects "always ask" while making
   the demotion visible)* vs (c) accept silent demotion + rely on re-entering the code. Silent demotion
   of the one role the Mass depends on re-opens the 2026-07-01 outage class.
2. **Staging isolation:** **reuse the same prod worker with room `alvernia-staging`** *(recommended —
   zero infra, isolated DO)* vs a separate staging worker. The DO instance boundary is already a hard
   wall.
3. **Book version-pointer store:** **the `__book__` DO** *(recommended — reuses the class-by-name
   pattern, atomic flip, no new binding)* vs a KV namespace. You'll still add the **R2** binding for
   page storage.
4. **Diff strictness:** **exact per-page SHA-256, falling back to a tight structural threshold only if
   the render can't be made pixel-perfect** *(recommended)* — exact-only would block every legitimate
   upload if the render drifts one pixel; the additive-only gate stays hard either way.
5. **Boot-watchdog trigger:** **second-consecutive failed boot** *(recommended — avoids false-reverting
   a one-off slow cold start on an aging iPad)* vs first failure.
6. **Offline mesh book distribution:** **keep it a super-admin-armed, explicit straggler safety net**
   *(recommended)* — never automatic mid-Mass; the primary path is practice-day wifi.
7. **Confirm the Spanish copy** for the resume-director prompt, the "director tiene una versión más
   nueva" soft banner, and the recovery banner.

---

## 10. Honest caveats (what can still go wrong even with all of this)

- **We do NOT promise zero regressions** — only that any regression is caught on the canary iPad in
  staging and reversible in one command. A bug can still reach the canary; the guarantee is that it
  stops there.
- **The offline church mesh (Swift/Multipeer) is the church-critical path and CANNOT be tested in a
  simulator or in staging** — it needs two physical iPads. The safety system's structural guarantees are
  strongest on web/worker and **weaker (human-gated) on the mesh.** Every native mesh change carries an
  irreducible 2-device-day tax.
- **Solo-dev discipline is a single point of failure.** The whole gate depends on Miguel actually
  running the canary-walk on the oldest iPad before promoting, on a practice day, **every time.** The
  recipe forbids promoting near a Mass window in the strongest terms — honor it.
- **Book-out-of-bundle (Scenario 1/2) is XL and adds real infra** the worker doesn't have yet (R2, the
  `__book__` DO, admin auth, pdf.js). Until it lands, the book is still baked and a content fix still
  needs a full build. Don't start it until the safety system + bridge + crash layer are proven, or a
  half-built admin becomes its own footgun.
- **The additive-only wire contract is the promise the whole fleet's compatibility rests on.** If any
  future change removes/retypes `page`/`totalPages`/`seq` or bumps the mesh `protocolVersion`, the
  graceful-degradation guarantee breaks and a mixed fleet CAN hard-break. Enforce it forever (the
  boot-smoke test + code review), never assume it.
- **A1 residual (independent P0):** the committed director code `12345678840` may still be in
  `TRANSMITTER_CODES` (build-367 compat). The safety system does **not** fix this — until it's confirmed
  unused and rotated out, anyone reading the public repo has a full-privilege director code that can flip
  pages during Mass. Close it independently (plan §12 Q3).
- **Clock skew:** DIAGNÓSTICO and the version banner depend on device clocks; the P2-CLOCKSKEW offset
  reduces but doesn't fully eliminate clock-dependent misjudgments on the oldest iPads.

---

## 11. How this elevates the hardening plan

These edits fold the Major Update into [`app-hardening-plan.md`](app-hardening-plan.md) (applied
separately, kept surgical to avoid regressions in a doc that's already shipped):

- **New top-tier row A0** in the §0 dashboard, above A1: *"Release Safety System — the
  anti-busted-for-everyone gate (staging room + CI boot-smoke + one-command rollback + additive
  version-compat). Gates every other item."*
- **A "P0.5 — Release safety skeleton"** line in §1 between P0 and P1 (CI + `smoke-boot.mjs` + the
  staging resolver + `rollback-web.sh`) — it *is* P1-CI + P-STAGING made concrete and pulled earlier.
- **P-STAGING** is elevated from strategic-roadmap to a **P0.5 dependency** (the substrate, not
  "run after P1–P8").
- **P-OTA** gains the native boot-watchdog auto-rollback as its safety half (M7).
- **This document is the detailed spec** for the book-out-of-bundle work; the plan links here rather
  than duplicating it.

---

## Changelog

- **2026-07-04 — Major Update program.** Layered on the reconciled build-374 plan: the release-safety
  system made the gate for everything; seven designs woven into a 6-step dependency order
  (safety → bridge+crashes → book-out-of-bundle → distribution → sync → recipe/DIAGNÓSTICO); native
  items batched to one 2-device day (M7). Designed by a 9-agent pass (grounding → 7 designers →
  synthesizer) grounded at real file:line, HEAD `f3d77193`.

_End of spec. Grounded designs in the workflow run `wf_e9f49fa6-261`. The bug-fix backbone this reuses:
[`app-hardening-plan.md`](app-hardening-plan.md) + [`audit-reconciliation-374.md`](audit-reconciliation-374.md)._
