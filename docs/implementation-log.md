# SignoVivo — Implementation Log

> **Purpose.** A living, chronological record so any Claude Code tab (even a cold Opus) can pick up
> mid-stream: where we came from, where we're going, what's done, what's next, and the decisions still
> owed by Miguel. Read this + the three plan docs and you have full context. Append to the top of §"Log"
> as work lands. Keep it honest — record what actually shipped, what's deferred, and why.

## The arc so far (where we came from)

1. **Full audit** (build 370) → `docs/app-hardening-plan.md` + `docs/audit-findings-raw.md` +
   `docs/audit-findings-index.md` + `docs/app-atlas.md` + `docs/app-contracts.md`. 117 findings.
2. **Single-book refactor happened after the audit** (builds 371–374 removed the two-book/Sión/geo
   system, driven by a real 2026-07-01 Mass outage). → **Reconciliation** to build 374:
   `docs/audit-reconciliation-374.md` (25 findings moot, 14 new director-role findings). The plan doc
   was rewritten build-374-accurate.
3. **Major Update design** (post-Wednesday "busted for everyone" trauma) → `docs/major-update-2026-07.md`:
   the Release Safety System + 6 features (super-admin PDF upload, distribution, sync robustness, a
   typed bridge, crash-proofing, the rollout recipe/DIAGNÓSTICO), sequenced M0–M7.
4. **Now: implementation**, starting at M0 (the safety net). This log tracks it.

## The plan we are executing

`docs/major-update-2026-07.md` — milestones **M0 → M7**, built in a strict dependency order:
**safety system → bridge + crashes → book-out-of-bundle → distribution → sync → recipe/DIAGNÓSTICO.**
All native/mesh work batches into **one 2-device day (M7)**. The north star: *no change reaches the
choir until it's proven GREEN on one canary iPad in an isolated channel, and every change is reversible
in one command.* Not "zero bugs" — **caught-on-canary + instant-rollback.**

## Standing rules (see `~/.claude/CLAUDE.md` §7, §9 — global)

- **Commit early, commit often, push often — ALWAYS.** Every complete step → commit + push, so any tab
  resumes from git history, not memory.
- **Never assume on a genuine fork — ask Miguel first, before moving a millimeter.** Product/UX
  decisions, ambiguous scope, steps needing his hands (Cloudflare/secrets/devices), anything affecting
  the director/congregation → STOP and ask.
- **Zero regressions / zero risk of regressions is the top priority.** Never run `npm run test:e2e`
  (it publishes to the PROD relay room). Run named safe files only.

## How to verify locally (safe commands)

```bash
npm run typecheck                                   # green
node --test e2e/repo-minimal-footprint.test.mjs \
  e2e/native-entrypoint.test.mjs e2e/native-stability-config.test.mjs \
  e2e/offline-books-integrity.test.mjs e2e/nearby-sync-contract.test.mjs \
  e2e/permission-flow.test.mjs                       # 61/61 green (the CI safe subset)
SMOKE_SKIP_BUILD=1 node scripts/smoke-boot.mjs       # inspect existing web/dist (fast)
node scripts/smoke-boot.mjs                          # full: rebuild + assert (needs poppler/cwebp/sips)
```
Never run the bare `node --test e2e/*.test.mjs` glob or `e2e/relay-sync.test.mjs` — prod-mutating.

---

## Log (newest first)

### ⛳ SESSION WRAP (2026-07-04→05) — where a fresh tab picks up

**Shipped to `main` this session (11 PRs, all CI-gated; runtime changes browser-verified). NOTHING is
deployed yet — all in git, ready for the release ritual. signovivo.com / the worker / the iPads still
run their pre-session code.**
- Global rules (commit-often + never-assume) → `~/.claude/CLAUDE.md` §7/§9.
- **M0** CI + boot-smoke (#233) · **M1** staging channel complete (#234/#235/#237) · **M2-A** web
  crash-proofing (#238) · **A4** release PII trap (#239) · **A5/A1-repo** (#239) · **P8** README (#240)
  · **P7** wake-lock (#241) · **native A3+NEW-DIR-1/2/3** device-gated (#243).

**✅ A2 DONE (2026-07-05, PR #246 → `b9cc44b4`) — deploy-ready, NOT deployed.** Worker rate-limit
(per-IP token bucket on /publish 15/2·s, /fleet/checkin 10/1·s, /log 20/3·s → 429, fail-open) + seq=0
gate (reject seq=0 while a fresh director is live; honor it only as a stale reset). PROVEN 4/4 against a
local wrangler dev relay (`sync-worker/test/a2.test.mjs` + `run-a2.sh`). **All 5 criticals now handled
code-side.** DEPLOY = Miguel's `cd sync-worker && npx wrangler deploy` (reaches everyone) — pair with the
A1 secret rotation.

**▶ NEXT (in priority order):**
1. **P2 sync robustness** (web; wants the same harness pattern): P2-SEQ (freshness-before-seq so a dead
   director is demoted + the green-pill freeze ends), P2-CLOCKSKEW, P2-POLL-GAP. Extract the seq/freshness
   decision into a unit-tested lib (like svRelayRoom/svSelftest), then apply + browser-verify.
2. **M2 Slice C/D** (defensive guards mop-up; crash telemetry → needs P6-LOG X-Fleet-Key gate + worker deploy).

**⚠ NEEDS MIGUEL (I won't assume the timing/hardware):**
- ✅ Web deployed to signovivo.com + **build 375 cut & Delivered to TestFlight** (2026-07-05 ~00:43 CDT).
- **2-device day** to verify the native batch (build 375 / #243) BEFORE the choir updates — 4 repros in the
  PR body; target Wednesday practice.
- **A2 worker deploy** (`cd sync-worker && npx wrangler deploy`) + **A1 secret rotation**
  (`wrangler secret put TRANSMITTER_CODES` to drop `12345678840`, once no device depends on it) — both
  reach every follower; do on a green (non-Mass) day.

---

### NEXT BATCH — native director-role fixes ✅ DONE (2026-07-05, PR #243 → `77de2a7c`) — DEVICE-GATED, unshipped
All in `PdfReaderApp.tsx`. Interlocked (shared role state machine) + device-gated (only *provable* on a
2-device practice day + a TestFlight build). Implement carefully as ONE batch, typecheck-verify, ship on
a device day. **BLOCKED on one Miguel decision (NEW-DIR-1 mechanics) — see below.**

- **A3 (crit)** `~:566` bridge-ready: a director's WebView content-process reload makes the web boot to
  page 2 and native re-broadcasts it → whole congregation yanked to page 2. Fix: when `roleRef==='director'`
  (or `explicitTransmitterRef`), do NOT adopt `msg.page` from bridge-ready/first post-reload page-changed;
  re-assert native's own `currentPageRef` (inject a page sync-event + broadcast THAT). Guard the first
  post-reload page-changed with a `webReloadPendingRef`.
- **NEW-DIR-2 (high)** `:503` `onDirectorCode` bumps `roleGenerationRef` on EVERY code entry, before the
  confirm. If a boot `becomeFollower()` is mid-retry-sleep (`becomeDirector` retry at `:434`; becomeFollower
  has the symmetric pattern) and the user enters a code then Cancels, the in-flight becomeFollower sees its
  gen superseded and returns WITHOUT establishing the mesh link → stranded link-less follower. Fix: REMOVE
  the `:503` bump; `becomeDirector` (bumps at `:400`), `performSoftReset` (`:479`), and becomeFollower each
  already bump on commit, so a Cancel no longer supersedes an in-flight follower. (Verify becomeFollower's
  bump before removing.)
- **NEW-DIR-3 (med, SAFEST)** `:527-528` `liveDirector = Boolean(lastDirectorSnapshotRef.current) && ...` —
  the ref is set-once (mesh 'page' at `~:763`) and NEVER cleared, so the destructive red "⚠️ Ya hay un
  director activo / Tomar el control" warning false-fires forever after any director ever broadcast (e.g.
  Braulio directs then exits; 20 min later the real director enters their code and sees a scary phantom-
  takeover warning). Fix: store a timestamp with the snapshot and gate `liveDirector` on recency (~mesh
  heartbeat window), and/or clear it on DIRECTOR_CONFLICT / exit-director. Lowest-risk (only the confirm
  dialog's label/style, not the role transition).
- **NEW-DIR-1 (high) — ⛔ NEEDS MIGUEL'S DECISION** `:757` boot unconditionally `becomeFollower()` → a
  director whose app restarts silently drops to follower with no signal (re-opens the July-1 outage class).
  Miguel chose a **boot resume-prompt**. CONSTRAINT: the director code is DELIBERATELY not stored
  (`:440-442` "a credential must never sit in storage"), so the prompt CANNOT auto-re-become director. The
  fork: (a) boot prompt "Estabas dirigiendo — reingresa tu código para continuar" that reopens the code
  numpad (re-enter code; safest, keeps the no-stored-credential rule); (b) persist the code (encrypted,
  expo-secure-store) so the prompt can one-tap resume (more convenient, but stores a credential — violates
  the current principle); (c) a persistent visible banner only. **Ask Miguel before implementing.**

### P0 criticals — repo-side ones knocked out (2026-07-04, branch `dev-a4-a5-repo-p0s`)
- ✅ **A4 + B-RESTORE** (`scripts/release.sh`): crash-safe `trap cleanup_release EXIT INT TERM` around
  the director-codes PII swap — a Ctrl-C/crash/error mid-archive can no longer leave real phone numbers
  in the tracked file; restore always returns 0 (no more `set -e` abort). **Proven** in an isolated
  temp-dir sim (normal exit / `exit 1` / SIGINT all restore the file). Dev tool, no runtime surface.
- ✅ **A5 + A1-repo** (`e2e/relay-sync.test.mjs`): env-gated + disabled-by-default (throws at import
  unless RELAY_TEST_BASE/ROOM/CODE set), hard-refuses the live `alvernia-main` room, and the committed
  `12345678840` credential removed (gone from all runnable code; docs-only now).
- **Status of the 5 criticals:** A3 (pending native M7), A4 ✅, A5 ✅, **A1** repo-half ✅ / secret-half
  ⏳ needs Miguel (`wrangler secret put` to rotate 12345678840 out, once no device depends on it),
  **A2** (relay rate-limit) ⏳ worker change — build+test against P1 local harness, then a worker
  **deploy** that reaches every follower → do on a green weekday with Miguel's ok on timing.

### M2 — Web crash-proofing — ✅ Slice A DONE (2026-07-04, PR #238 → `c22af07d`)
- Guarded the two unguarded module-eval `localStorage` reads (`app.js` haptic + tip) — **the likely
  Wednesday white-screen cause** (they throw in a storage-disabled browser → whole script aborts).
- **Boot-guard net** (first executable code): an uncaught boot-time crash → "Reintentar" recovery card,
  never a white screen; gated on `__svBooted` so it never hijacks a working reader. Browser-verified on
  all 3 paths (normal / crash-not-booted / benign-post-boot).
- Resilient `pages.json` boot fetch (degrades to known page count, doesn't fail the reader).
- Remaining M2: Slice C (defensive-guard mop-up, web, safe), Slice D (crash telemetry → /log + fleet
  panel — **needs the P6-LOG `X-Fleet-Key` gate first + a worker deploy**), Slice B (native error
  boundary + WebView recovery → M7 2-device day).

### M1 — Staging channel — ✅ COMPLETE (2026-07-04, PRs #234/#235/#237)
Resolver (`?env=staging`) + `STAGING=1` deploy + `rollback-web.sh` + `?selftest` card + checklist.
Staging room live-proven against the real worker (`?env=staging&selftest` → relay conectado on
alvernia-staging), zero worker changes. All browser-verified.

### Decisions locked (2026-07-04, Miguel delegated to my recommendation)
1. **Cadence** → **autonomous on safe work**: build + merge web/worker/dev milestones on green CI,
   pause only for steps needing Miguel's hands (Cloudflare R2/secrets, physical devices) or genuine
   product/UX forks.
2. **Director restart (NEW-DIR-1)** → **boot resume-prompt** ("¿Continuar como director?"). Lands in M7.
3. **Staging isolation** → **same worker + `alvernia-staging` room** (separate DO, zero worker change).

### M1 — Staging channel — ✅ MOSTLY DONE (2026-07-04)
- ✅ **Relay-room resolver** (`?env=staging` → `alvernia-staging`, else `alvernia-main`) — PR #234 →
  `3d230c44`. `web/src/lib/svRelayRoom.js` (UMD, unit-tested), `<script defer>` before app.js; `build.mjs`
  copies `web/src/lib/*` → `dist/lib/`; `app.js:2765` triple-guarded (lib-presence + whitelist +
  try/catch) → can NEVER white-screen, defaults to `alvernia-main`. Native `directorRelaySync.js`
  intentionally unchanged (native staging = M7). Verified: unit 4/4, safe subset 65/65, typecheck,
  smoke, **browser boot renders page 2**, zero console errors.
- ✅ **`STAGING=1` release path + rollback helper + checklist** — PR #235 → `c900cc4a`.
  `STAGING=1 bash scripts/release.sh` = canary deploy to the isolated preview branch (no bump, no
  native, never prod); guarded so the default prod flow is byte-for-byte unchanged (`bash -n` + branch
  logic verified). `scripts/rollback-web.sh` = read-only break-glass undo. `docs/pre-mass-checklist.md`
  = the operator ritual. Dev/doc only, zero runtime surface.
- ⬜ **Remaining M1: the `?selftest` GREEN/RED readiness card** (touches the boot path → do it with the
  same browser-boot verification the resolver got; guard it so the normal `?`-less boot is 100%
  untouched). Then M1 is complete.

**Net: the staging + rollback safety loop is live** — deploy to an isolated channel (`STAGING=1`), prove
on the canary (`?env=staging`), promote only if green, undo in one step (`rollback-web.sh`). This is the
wall that was missing on Wednesday.

### M0 — Release Safety skeleton — ✅ DONE + MERGED (2026-07-04, PR #233 → main `da523294`)
The one part of the plan with **zero product forks and zero runtime risk** — dev/CI-only, cannot affect
any device. It is the gate every later milestone ships through, and the thing that would have caught the
Wednesday failure.

- ✅ **P1-RED fixed** (`e2e/repo-minimal-footprint.test.mjs`): added `deploy:web` to the script
  allowlist so the suite is green at HEAD (was red, masking regressions). Test-only; package.json
  untouched. `deploy:web`'s own footgun (no `--branch main`/bump) is deferred to P4 (B-DEPLOYWEB).
- ✅ **Boot smoke test** (`scripts/smoke-boot.mjs`): builds the bundle + asserts shell files, `#pages-data`
  positive-int totalPages, page 1 + last page render, **triple page-count consistency** (inline ===
  rendered === manifest — the song-370 "unreachable page" guard), native bridge markers survived, no
  unreplaced build tokens. Verified: passes on good dist, fails correctly on 3 broken bundles.
- ✅ **CI** (`.github/workflows/ci.yml`): macOS runner (for `sips`) + brew poppler/webp + node 22;
  `npm ci` → typecheck → 6 named safe e2e files → smoke. Never the glob/relay-sync; eas-config excluded.
- ⏳ **PR open + CI green + merge** — see PR link below. CI's first live run also proves `npm ci` + brew
  + build work in the runner.

**Deferred within M0 (noted, not yet done):** a guard test asserting the `test:e2e` glob never appears in
`.github/workflows/*` (belt-and-suspenders so no one re-adds the prod-mutating glob to CI).

### Next up (blocked on Miguel's decisions — DO NOT assume)
Before M1+, the following forks need Miguel's answers (from `docs/major-update-2026-07.md` §9). Ask
before proceeding:
1. **Director-restart UX** (NEW-DIR-1): boot resume-prompt (recommended) vs silent demotion.
2. **Staging isolation**: same worker + `alvernia-staging` room (recommended) vs separate worker.
3. **Book version store**: `__book__` DO (recommended) vs KV. (Also needs him to create the R2 bucket +
   set `ADMIN_PIN`/`ADMIN_PASSWORD` secrets when Scenario 1 starts.)
4. **Diff strictness**, **boot-watchdog trigger**, **offline mesh distribution** posture, **Spanish copy**.
Plus: **scope/sequencing** — confirm proceeding milestone-by-milestone, pausing at steps needing his
Cloudflare/secrets/physical-device involvement.

**M1 (next, mostly safe/web):** `getRelayRoom()` room resolver (`?env=staging`/`?practice=1`), `STAGING=1`
release path, `?selftest` GREEN/RED card, `scripts/rollback-web.sh`, `docs/pre-mass-checklist.md`.
