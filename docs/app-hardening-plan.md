# SignoVivo — App-Wide Hardening Plan

> **What this is.** A full-app audit turned into an executable plan another Claude Code tab can
> implement cold — reliability, robustness, security, performance, and cleanup, app-wide.
>
> **Reconciled to build 374** (HEAD `39b60313`, 2026-07-04). The audit was originally run against
> **build 370** (a *two-book, IP-geo-gated* app). Between then and now a real 2026-07-01 Mass outage
> (offline devices defaulted to the Sión floor book → book-scoped director code rejected → **no
> director all night**) forced a cascade of fixes (builds 371–374) that culminated in commits
> `f34d2bb0` / `9473e596` / `38132dd3` / `318d6f39` / `39b60313` **removing the entire two-book +
> Sión + IP-geo + geo-gate + `/unlock` + auto-director system**. The app is now a **single public
> book** ("standard" = the 371-page Alvernia manual), served to everyone with no geo and no book
> switching. A 14-agent reconciliation pass re-triaged all 117 original findings against build 374:
> **~25 are now MOOT** (retired by the refactor — see §9), 43 survive at moved line numbers, 16
> changed mechanism, 11 are partially moot, 25 survive as-is, and **14 NEW findings** were introduced
> by the refactor (a whole new director-role state machine that had never been audited).
>
> **Companion docs (all in `docs/`):**
> - `audit-reconciliation-374.md` — **the receipts for THIS reconciliation**: the full disposition
>   ledger (every original finding → SURVIVES/MOVED/CHANGED/PARTIALLY_MOOT/MOOT with build-374
>   line numbers) and the complete EVIDENCE/FIX text for all 14 new findings.
> - `audit-findings-raw.md` — the original unabridged 117 findings (build 370 line numbers).
> - `audit-findings-index.md` — one-line index of the original 117.
> - `app-atlas.md` — architecture navigation map (carries a build-374 correction banner at the top).
> - `app-contracts.md` — cross-subsystem contracts.
> - `sync-handoff-known-issues.md` — the prior (build 344/345) audit; this plan supersedes it.
>
> **Provenance & confidence.** Original findings + this reconciliation came from adversarial agent
> passes (delta-map → 9 per-dimension reconcilers → 3 new-surface finders → synthesizer, all on
> Opus 4.8, reading real code at HEAD). The security dimension additionally got a full skeptic pass
> in the original run. The orchestrator hand-verified the P0 tier (A1–A5) and A3 against build-374
> code directly (marked `[verified]`). Everything else is `[code-grounded]` — cites real current
> line numbers but **the implementing tab must open the cited code and confirm the mechanism before
> writing the fix** (§3). Line numbers are as of HEAD `39b60313`; the codebase is actively worked in
> parallel tabs, so re-locate with grep if a line has drifted.
>
> Last updated: 2026-07-04 (reconciled 370→374). Changelog at the foot of this file.

---

## 0. TL;DR — the dashboard

| # | Item | Sev | Surface | Ships via |
|---|------|-----|---------|-----------|
| **A1** | Committed director code `12345678840` (public master credential; now **fully unrestricted** — book-scoping is gone) | 🔴 crit | worker + repo | secret rotation + git |
| **A2** | Relay has zero rate limiting + `seq=0` bypass + one shared room → anyone can hijack/freeze the live Mass | 🔴 crit | worker | `wrangler deploy` |
| **A3** | Director WebView content-process reload broadcasts boot **page 2** to the whole congregation (mesh half survives the refactor) | 🔴 crit | native | TestFlight |
| **A4** | `release.sh` PII swap has no crash-safe cleanup → a Ctrl-C can git-commit real phone numbers | 🔴 crit | dev/release | git |
| **A5** | `npm run test:e2e` publishes real page-flips into the production room (and now asserts *deleted* two-book behavior) | 🔴 crit | tests | git |
| **NEW: director-role regressions** | Silent no-reprompt demotion (re-opens the 2026-07-01 outage class); gen-bump-before-confirm strands a link-less follower; a never-cleared ref false-fires the destructive "take control" warning | 🟠 high | native | TestFlight |
| **B-series** | ~23 highs: sync freeze/flap, cacheVersion staleness, immutable stale pages, white-gate boot brick, world-readable `/log` PII, false privacy policy, patch-noop blank app, deploy pipeline gaps, zero tests on the sync core | 🟠 high | mixed | mixed |
| **C/D-series** | med + low: robustness, perf (wake-lock!), dead code, test coverage, docs | 🟡🟢 | mixed | mixed |
| **RETIRED** | ~25 findings **MOOT** — geo-gate flashes, cross-book races, Sión cross-publish, `/unlock` brute-force oracle, "private manual not gated", geo oracle, 24h re-mint (see §9) | ⚪ | — | done by refactor |

**The single most important concept in this plan is the _deploy surface_ (§2).** The native iPads
run a **file:// copy of the web bundle baked at archive time**, so a web fix reaches signovivo.com
followers instantly but reaches the parish iPads **only on the next TestFlight build** (until the
OTA-bundle-refresh strategy P-OTA lands). Worker fixes reach everyone instantly. Plan the work around
this or you will "fix" a Mass-critical bug that never reaches the devices that matter.

---

## 1. Order of execution (recommended)

```
P0  Stop-the-bleeding      → A1 A2 A4 A5 now; A3 + director-role regressions in the next build
P1  Safe test harness + CI → kill the prod-mutating test, go green, add a unit seam (single-book matrix)
P2  Relay / sync robustness→ seq-guard demotion, transmitter identity, poll gap, clock skew  (web + worker + native)
P3  Offline / PWA update   → cacheVersion, immutable pages, SW lifecycle                      (web + build)
P4  Build / release harden → release.sh atomicity, deploy:web, alt scripts                    (dev/release)
P5  Native + director-role → the 3 new regressions + device-gated Swift items, batched         (TestFlight + 2-device day)
P6  Security / privacy     → rate limiting, /log PII, privacy policy (drop the geo claim)      (worker + native + store)
P7  Perf / polish          → wake lock, img-retry guard, precache contention, list rebuilds    (web)
P8  Dead code + docs        → residual dead fields, .d.ts/README/HANDOFF drift, purge          (web + repo)
────────────────────────────────────────────────────────────────────────────
Strategic roadmap (P-OTA, P-CI, P-OBS, P-MESH[stronger retire case], …)  — §11, run after/with P1-P8
```

P0 items are independent — do them in any order. Everything else assumes **P1 is done first** so you
have a safety net (right now there is effectively none: the only behavioral test mutates production
*and* asserts deleted behavior).

---

## 2. Deploy surfaces — memorize this

| Surface | Files | Reaches | Latency | How |
|---|---|---|---|---|
| **worker** | `sync-worker/src/index.ts`, `wrangler.jsonc`, secrets | **everyone** (server-side) | instant | `wrangler deploy` |
| **web** | `web/src/*`, `web/build.mjs` | **signovivo.com followers** instantly; **native iPads only on next archive** (bundle is baked) | instant (web) / next build (native) | `release.sh` web leg / Pages |
| **native** | `PdfReaderApp.tsx`, `ios/**`, `src/*.js\|ts`, `scripts/patch-*.js` | **parish iPads only** | next TestFlight build | `release.sh` full |
| **dev/release** | `scripts/*`, `package.json`, `e2e/*`, docs | nobody at runtime — safety/hygiene only | n/a | git |

**Consequences that shape the plan:**
- A3 and the three new director-role regressions are in `PdfReaderApp.tsx` → **native only**. They
  cannot be hot-fixed; they ride the next TestFlight build. Batch them together.
- Any web fix to `app.js` that must protect **iPad followers** is not truly shipped until either a
  TestFlight build or the **P-OTA** native-bundle-refresh strategy exists. This gap shadows every
  web-surface item and is P-OTA's core justification (§11).
- Worker fixes (rate limiting, code rotation, `/log`) are the highest-leverage because they protect
  every client the moment `wrangler deploy` returns.

---

## 3. Ground rules for the implementing tab

1. **Safety (hard):**
   - **Never run `npm run test:e2e`** until P1 neutralizes `e2e/relay-sync.test.mjs` — it publishes
     to the PRODUCTION relay room `alvernia-main` and flips live followers' pages. Run individual
     safe files with `node --test e2e/<file>.test.mjs`.
   - **Never deploy the worker or Pages during or right before a Mass** — a Pages deploy force-reloads
     every online follower tab within ~60s (P3 mid-Mass guard), and a worker deploy briefly drops
     WebSockets. Deploy on a green weekday.
   - **Never commit secrets or PII.** Real director codes (and now `superAdminCodes`) live only in
     gitignored `director-codes.private.json`; the roster lives only in gitignored
     `sync-worker/roster.private.json`. `git diff` for phone numbers before every commit (A4 is
     exactly this failure mode).
2. **Verify before you fix.** Line numbers are as of HEAD `39b60313`. Open the file, confirm the
   mechanism still holds, THEN fix. Don't fix a bug that isn't there.
3. **Verify before you claim.** Follow each item's acceptance criterion. Two-device sync items cannot
   be proven in a simulator — see §10.
4. **Hunt → fix → re-hunt → verify** (Miguel §11): after a batch, re-read the changed code
   adversarially for regressions your fixes introduced, then verify. A clean re-hunt is "done."
5. **Multi-tab safety (§4):** snapshot `git rev-parse HEAD` + `git status` at start; this repo is
   often worked in parallel tabs. Prefer a worktree. Never touch another tab's branch/worktree.
6. **No EAS** for Miguel's own builds — TestFlight + local `release.sh` only.
7. **Ship discipline:** small reviewed commits, one concern each. Web/worker fixes can ship the day
   they land (verify in a real browser first). Native fixes batch into one TestFlight build. Update
   `version.json` on every bump (the badge/fleet check-in read it). **Never ship red.**
8. **Device-gated items** (`[DEVICE-GATED]`) touch the Multipeer Swift mesh and CANNOT be proven
   without two physical iPads on one wifi. Batch them ALL into one sanctioned 2-device day (§10). Do
   not ship a blind Swift change to the mesh.

---

## P0 — Stop the bleeding (this week)

> A1/A2 ship server-side today. A4/A5 are repo-side today. A3 + the director-role regressions are
> native and ride the next build (their server-side blunting is P2-SEQ).

### A1 — Committed director code is a public master credential  🔴 `[verified]`
- **Surface:** worker (secret) + repo. **Files:** `e2e/relay-sync.test.mjs:21`, `TRANSMITTER_CODES`
  secret, `sync-worker/src/index.ts:327` (`validTransmitterCodes`).
- **Problem:** `const CODE = "12345678840"` is committed and still in the live `TRANSMITTER_CODES`
  secret (build-367 back-compat). **Post-refactor this is now MORE dangerous, not less:** book-scoping
  was deleted, so any valid code may publish anything into the single room — the old "only Sión-book
  pages" limit is gone. Anyone who reads the public repo has a full-privilege director code.
- **Failure scenario:** a curious parishioner POSTs page-flips to `alvernia-main` during Mass; every
  follower iPad and phone jumps to their chosen page.
- **Fix:**
  1. Confirm no real device still authenticates with `12345678840` (see open question Q3 — memory
     notes a residual build-367 device dependency; verify before removal).
  2. Remove `12345678840` from `TRANSMITTER_CODES` (`wrangler secret put TRANSMITTER_CODES`).
  3. Replace the test's hardcoded code with an env var + dedicated test room (P1).
  4. `git grep 12345678840` → zero committed occurrences remain.
- **Acceptance:** publish to `alvernia-main` with `X-Director-Code: 12345678840` → 401; a real
  director still publishes; `git grep` clean.

### A2 — Relay has no rate limiting; `seq=0` bypass; one shared room  🔴 `[verified]`
- **Surface:** worker. **Files:** `sync-worker/src/index.ts` publish path (~656-671), seq guard (~120-124).
- **Problem:** no throttle on `/publish`, `/fleet/checkin`, or `/log`. With a valid code (A1) an
  attacker floods `/publish`; `seq=0` is always accepted (bypasses the monotonic guard); a single
  global room `alvernia-main` means one hijacker reaches everyone. (`/unlock` is now a no-op stub, so
  it drops off the mutating-route list — but leave the stub in for build-373 client compat.)
- **Failure scenario:** page-flip flood holds the congregation on the wrong page; or `/fleet/checkin`
  spam evicts every real check-in so the pre-Mass dashboard shows nobody ready.
- **Fix (worker, defense-in-depth):**
  1. Per-IP + per-room token-bucket on all mutating routes (DO storage counter; a few writes/sec is
     plenty for a real director). Return 429.
  2. Gate `seq=0`: accept only as a genuine authenticated "reset" AND only when the current snapshot
     is already stale — never as a live override.
  3. Cap `/log` and `/fleet/checkin` body size + rate (P6 covers these fully).
- **Acceptance:** a 100-req/s publish burst gets 429s after the first few; a real director's
  ~1 flip/10s is never throttled; `seq=0` from an unauthenticated caller is rejected.

### A3 — Director WebView reload broadcasts the web's boot page 2 to everyone  🔴 `[verified]`
- **Surface:** native (TestFlight). **File:** `PdfReaderApp.tsx:566` (bridge-ready adopt) → `:582-584`
  (broadcast); `:603-615` (page-changed adopt+broadcast); persisted-but-unread `sv.book.lastPage.standard` at `:611-614`.
- **Problem:** the refactor killed A3's *crash-relaunch* half (auto-director-restore is gone — boot
  now always `becomeFollower`), but the **mesh/content-process-termination half is untouched and
  still live.** On the director iPad, any WKWebView content-process termination (routine under iOS
  memory pressure) or peer `bundleUpdated` remount reloads the web WITHOUT changing `roleRef` (stays
  `'director'`; `onContentProcessDidTerminate` at ~`:930` only sets `webReadyRef=false` + `reload()`).
  The reloaded web boots to `DEFAULT_START_PAGE=2` (relay.hasDirector is always false in native
  file-mode → `renderPage(2)`), posts `page-changed{page:2}` and `bridge-ready{page:2}`; native adopts
  (`currentPageRef.current = 2`, `:566`/`:605`) and, because `roleRef==='director'`, re-broadcasts over
  the mesh AND the relay (`:582-584`, `:615`). The comment "the director's own page is authoritative"
  is exactly wrong — the ref was just clobbered by the boot page.
- **Failure scenario:** mid-hymn on page 250, the director iPad's WKWebView process is reaped → reload
  → within ~2s every mesh follower iPad and every signovivo.com phone flips 250→2, and the director's
  own screen shows 2. **The single worst live-Mass failure in the audit.**
- **Fix (`PdfReaderApp.tsx`):** the native shell — not the web boot — is authoritative for the page
  across a reload. In BOTH `bridge-ready` and `page-changed`, when `roleRef.current==='director'` (or
  `explicitTransmitterRef`), do NOT adopt `msg.page`; instead re-assert native's `currentPageRef` by
  injecting `sync-event{type:'page', page: currentPageRef.current, book:'standard'}` into the freshly
  loaded web and broadcast THAT. Guard specifically the first post-reload `page-changed` (a
  `webReloadPendingRef` flag that suppresses the first inbound director page-changed and replaces it
  with `currentPageRef`). Cheapest correct variant: read back `sv.book.lastPage.standard` on the reload
  path (which also gives the dead write at `:611-614` a purpose — see `new-director-dead-writes`).
  Edge cases: followers must keep adopting `msg.page`; a genuinely fresh director still lands on the
  book default; the transmitter-only path needs the same suppression (its relay heartbeat reads
  `currentPageRef`).
- **Server-side interim blunt (ships now):** P2-SEQ hardens the web follower's demotion/authority path,
  reducing A3's blast radius on signovivo.com before the native build.
- **Acceptance:** two-iPad — director on page 250, background/foreground to force a WebView reap →
  follower iPad and a signovivo.com tab stay on 250, relay `/state.page` never becomes 1 or 2. Unit:
  mock `bridge-ready{page:2}` while `roleRef==='director'`, `currentPageRef=250` → `broadcastPage`
  called with 250, not 2.

### A4 — `release.sh` PII swap has no crash-safe cleanup  🔴 `[verified]`
- **Surface:** dev/release (git). **File:** `scripts/release.sh:41-48,59` (byte-identical to build 370).
- **Problem:** the script `cp`s gitignored `director-codes.private.json` (real phone numbers + now
  `superAdminCodes`) over the **tracked** `director-codes.json` at `:42`, archives (~10 min), then
  restores at `:48/:59`. There is **no `trap ... EXIT`**. A Ctrl-C, crash, or power loss between the
  swap and restore leaves real phone numbers in the tracked file; the next `git add -A` commits PII to
  the public repo. Two concurrent runs also race on `director-codes.committed.bak`.
- **Failure scenario:** Miguel starts a 7am pre-Mass build, the archive hangs, he Ctrl-C's to retry,
  then commits a version bump — real parishioner phone numbers land in public git history.
- **Fix:** `trap restore_codes EXIT INT TERM` right after the swap; make `restore_codes` idempotent
  and always return 0 (also fixes B-RESTORE); a concurrent-run lockfile (also fixes C-RMBUILD); a
  pre-commit hook that greps staged `director-codes.json` for digits and blocks.
- **Acceptance:** kill the script mid-archive → `git status` shows `director-codes.json` clean (empty
  arrays), no private numbers staged. Simulate missing private file → warns + restores cleanly.

### A5 — `npm run test:e2e` mutates prod AND asserts deleted behavior  🔴 `[verified]`
- **Surface:** tests (git). **Files:** `e2e/relay-sync.test.mjs:16-21,185-190`; `README.md`.
- **Problem:** the suite's only behavioral file hardcodes `BASE=` prod, `ROOM="alvernia-main"`,
  `CODE="12345678840"` and POSTs ~20 real page-flips; live followers obey them for ~90s. **New wrinkle
  from the refactor:** the "mode and bookId persisted" test at `:185-190` publishes
  `{mode:'nonStandard', bookId:'hymns-4'}` — *deleted concepts* — and stays green only because the DO
  echoes any string verbatim. So the test is now **both a prod-mutating hazard and a dead-behavior
  assertion** that would go falsely red if someone hardened the worker to reject non-`standard` bookId.
- **Fix:** neutralized by P1. Immediately: require `RELAY_TEST_ROOM` + `RELAY_TEST_CODE` env and
  `throw` if `ROOM` resolves to `alvernia-main`; change the dead assertion to single-book reality
  (`mode:'standard', bookId:'standard'`) or delete it; fix README to stop instructing the suite.
- **Acceptance:** `npm run test:e2e` with no env set skips/throws the relay file (no publish); `git
  grep -n "hymns-4\|nonStandard" e2e/relay-sync.test.mjs` → 0; README no longer references it.

---

## P1 — Safe test harness + CI (do this first, before P2–P8)

> The #1 leverage item. The pyramid is inverted: the only behavioral test mutates prod, the four
> Mass-critical components (`app.js` 3336 lines, `sync-worker` 710 lines, `sw.js`, `build.mjs`) have
> **zero tests**, and the suite is **red at HEAD**. The single-book refactor *shrinks* the test matrix
> (drop every Sión/book-scoping/X-Hymnal/unlock-oracle/geo case).

### P1-RED — Fix the red suite at HEAD  🟠 high `[verified]`
- **File:** `e2e/repo-minimal-footprint.test.mjs:15-27`. The exact-allowlist assertion was never
  updated for the `deploy:web` `package.json` script. Update the allowlist (re-check the other
  exact-match assertions for the same staleness). Confirm `node --test e2e/*.test.mjs` (minus the
  neutralized relay file) is green. **Do this before anything else** so "green" means something.

### P1-HARNESS — Local relay for behavioral sync tests  🟠 high
- Stand up `sync-worker` under `wrangler dev` (miniflare) with a **dedicated test room + test code**;
  rewrite `relay-sync.test.mjs` to hit `localhost` (or a gated staging env), never prod. Assert the
  **single-book** protocol: seq strictly-greater-wins, `seq=0` handling, 90s freshness,
  `{ignored:true}` on regressed seq, takeover-after-stale. **Drop** all Sión / book-scoping / X-Hymnal
  cases — deleted. This also gives P2/P6 a place to prove worker fixes.

### P1-WORKER-UNIT — First real worker tests  🟠 high `[verified: zero tests exist]`
- `sync-worker/` has NO test directory. Add `vitest` + `@cloudflare/vitest-pool-workers` (or
  miniflare) for the boundaries that now matter: publish auth (401 on bad code), seq monotonic +
  `seq=0`, fleet key gate (`/fleet` requires the key), CORS, rate limiting (A2). The worker boundary
  set **collapsed** to publish-auth + seq + fleet-key + CORS (unlock-oracle / Sión / geo cases gone).

### P1-APPJS-UNIT — Unit seam for the reader  🟠 high
- Extract pure logic into `web/src/relay-core.js` (or similar) testable under `node --test`:
  `clampPage`, `relayIsFreshLive`, the seq-guard predicate (the exact P2-SEQ decision), single-book
  `pageFileName`/`pagePadWidth` (guards `new-web-boot-padwidth-clamp-fallback-fragile`),
  `resolveSongPage`, `searchPages` ranking, numpad digit-routing. Start with the functions the P2
  fixes touch so those fixes land with tests. This is the seam P8/P-CONST modularization grows into
  (plain `module.exports` shims, concat at build — no bundler yet).

### P1-CI — GitHub Actions on PR  🟠 high `[no CI exists today]`
- `.github/workflows/ci.yml`: `npm ci` (guards lockfile installability), `npm run typecheck`, the
  **safe** e2e subset, the new worker + app unit tests, a `web/build.mjs` boot smoke (build succeeds,
  boots in jsdom, `#pages-data` present). Never run the prod-mutating file.
- **Delete/convert stale tests** (repo rule: prefer deleting dead-behavior tests):
  - `new-refactor-relay-test-asserts-dead-twobook` — the hymns-4/nonStandard assertion (folded into A5).
  - `new-refactor-contract-test-pins-dead-nonstandard-dts` (`nearby-sync-contract.test.mjs:23`) — pins
    the dead `nonStandard` union in the `.d.ts`; must land with the `.d.ts` narrowing (P8) or CI reddens.
  - `test-suite-permission-flow-dead-takeover-pin` — delete dead-flow assertions (keep live wire-contract pins).
  - `test-suite-vacuous-staleness-90s` — `includes("90")` can never fail; replace with a real harness assertion.
  - `test-suite-eas-config-banned-pathway` — delete EAS-enforcing assertions; guard the assets
    `app.json` actually references (`03_icon_1024x1024.png`, `04_splash_1668x2388.png`), currently unguarded.
  - `test-suite-song55-test-name-lies` — assert `clampPdfPage` behavior via the real module or delete.

---

## P2 — Relay / sync robustness (single-room, single-book)

> The sync layer is well-defended in its bones (zombie-CONNECTING timers, per-socket heartbeats, seq
> sanitization, durability-first publish). The refactor **retired an entire class of relay bugs**
> (cross-book switch race, Sión cross-publish, wrong-song-under-green-pill — all MOOT, §9). The
> residual holes cluster around **director page authority (A3)**, **staleness/takeover on a healthy
> socket**, **transmitter identity**, and **poll gaps / clock skew**.

### P2-SEQ — Seq guard runs before the freshness check → dead director never demoted; regressed-seq takeover ignored  🟠 high `[verified]`
- **Surface:** web (+ protects iPad followers on next build/OTA). **File:** `web/src/app.js:3033`
  (guard) vs `:3044` (freshness).
- **Problem:** `applyRelaySnapshot` returns at the seq guard (`!force && snap.seq <= relay.lastSeq`)
  BEFORE evaluating 90s freshness. Over a healthy WS the only inbound traffic after a director stops
  is the follower's own ping replies (`seq === lastSeq`), dropped at `:3033` — so the demotion branch
  that clears `relay.hasDirector` is unreachable. **Failure A:** director ends Mass and closes the
  app; every foregrounded, wifi-stable follower keeps a pulsing green "en vivo" pill pointing at a
  dead director indefinitely. **Failure B (worse):** the worker deliberately accepts a LOWER seq after
  90s staleness (takeover self-heal, `index.ts:120-124`), and native seqs are wall-clock ms
  (`directorRelaySync.js:56`), so a handoff to a slower-clock device publishes seqs BELOW the old
  director's → every WS push from the new director is dropped at `:3033` until wall-clock catches up.
  Followers on stands (never backgrounded, socket healthy) freeze under a green pill with no recovery
  but a manual ⟳.
- **Fix:** evaluate `hasPublished && relayIsFreshLive(snap)` BEFORE the seq guard — a stale snapshot
  must always run the demotion branch regardless of seq; apply the monotonic guard only to *fresh
  live* snapshots. Keep `force` bypassing the seq guard for a fresh director sitting still. (Exact
  logic to unit-test in P1-APPJS-UNIT; blunts A3's web-follower blast radius.)
- **Acceptance:** harness — publish seq 100 fresh, let it go >90s stale, publish seq 50: the follower
  accepts seq 50 (takeover) and, after staleness, drops the green pill. Unit-test the decision fn.
- **Dedup:** = `coherence-seq-regression-freezes-ws-followers`. Prior build-344 row fixed only the manual-⟳ path.

### P2-IDENTITY — No transmitter identity → two publishers ping-pong followers  🟠 high `[code-grounded]` `[partly DEVICE-GATED]`
- **Surface:** worker + native. **Files:** `src/directorRelaySync.js:56` (seq = wall-clock ms, no
  device id), `sync-worker/src/index.ts:120` publish.
- **Problem:** the relay protocol carries no transmitter identity. Two authorized publishers (two
  directors during a handoff, or a real director + anyone with the committed code A1) both publish
  into the one room; the worker's seq guard can't distinguish them, so followers flap. The worker
  returns `{ok:true, ignored:true}` on a dropped regressed-seq publish, but the native transmitter
  (`directorRelaySync.js`) **ignores the `ignored` flag** — a handoff director broadcasts into the
  void with no warning.
- **Fix:** (1) add `transmitterId` (stable per device, SecureStore) to the publish payload + snapshot;
  (2) worker: on a fresh live snapshot from a *different* transmitterId, apply an explicit tiebreak
  (newest token wins), stamp the winner, reject the loser with a distinct reason; (3) native: consume
  `{ignored:true}` and surface "another director is live" via the existing `showRelayAuthWarning`
  bridge.
- **Device-gated portion:** the mesh side of split-brain (two directors on Multipeer) needs 2 devices
  (§10). The relay side is testable in P1-HARNESS.
- **Dedup:** = `native-swift-relay-split-brain-no-tiebreak`, `coherence-ignored-publish-response`; prior HIGH#2, mesh half addressed in 345, relay half still open.

### P2-POLL-GAP — Polling stopped before the socket opens → repeating blind windows  🟡 med `[code-grounded]`
- **File:** `web/src/app.js:3108`. `connectRelay` kills `/state` polling before the WS opens; on
  WS-hostile parish/guest wifi this creates repeating ~6s blind windows. **Fix:** keep a low-rate
  `/state` poll alive until the socket is actually `OPEN`; only then stop it.

### P2-CLOCKSKEW — Follower with a fast clock never sees the director as live  🟡 med `[code-grounded]`
- **File:** `web/src/app.js:3018`. `relayIsFreshLive` compares the raw client clock to server `ts`
  with no offset; a device clock >90s fast treats every snapshot as stale. **Fix:** calibrate a
  client↔server offset from the WS snapshot/`/state` response and apply it in the freshness comparison.
  Also de-risks P2-SEQ Failure B.

### P2-lows (batch) `[code-grounded]`
- `web-reader-browse-result-click-skips-relay-browsing` (`app.js:2498`): tapping a song in browse/search
  never sets `relay.browsing`, so the next push yanks the follower back (numpad jump sets it right).
  Set `relay.browsing` on browse/search selection too.
- **Note — `P2-TRANSMITTER-RESTORE` is now subsumed** by the director-role work in P5: the transmitter/
  director role is no longer auto-persisted at all (the refactor deleted auto-restore), so "restore the
  transmitter role" is a *product decision* (open question Q1), not a straight bugfix. See P5.

_MOOT here (retired by refactor, §9): P2-SIÓN-ROOM, P2-WRONGSONG, P2-SWITCHBOOK-RACE, unlock-swallows-
switchbook, browsing-user-book-switched._

---

## P3 — Offline / PWA update correctness

> The SW/offline design is genuinely defensive (per-asset `allSettled` installs, shell-ready
> activation gating, cross-version page-cache fallback, native file:// correctly skips all SW paths).
> Two load-bearing assumptions are unenforced and cause **silent permanent staleness** — the worst
> kind for a Mass tool. The refactor did NOT touch this surface (all items survive; hymns-4 inputs drop).

### P3-CACHEVERSION — `cacheVersion` hashes only 5 shell files → book/version changes never bust caches  🟠 high `[verified]`
- **Surface:** build → web. **Files:** `web/build.mjs:22` (still hashes only the 5 shell files),
  `web/src/sw.js:1`.
- **Problem:** `cacheVersion` is a hash of `app.js`, `sw.js`, `styles.css`, `index.html`, `manifest`
  + `build.mjs`. The PDF, `assets/standard/*.json`, `src/alverniaManual2SongIndex.js`, and
  `version.json` are excluded. A deploy where only book data or `version.json` changed (and HEAD is
  unchanged — `release.sh` deploys `--commit-dirty=true`) produces a **byte-identical `sw.js`** →
  browsers never update → returning users keep the old app.js, badge, song index, and (page WebPs
  immutable+retained) stale page images forever. The comment at `:10-28` claims the opposite guarantee.
- **Fix:** include ALL deploy inputs in the hash — the PDF, every `assets/standard/*.json`, the
  song-index source, `version.json`, the generated manifests. Simplest robust approach: hash the final
  `dist/` tree (a manifest of content hashes) after build, not a hand-picked list. (Drop the deleted
  hymns-4.pdf + hymns-4 manifest inputs — single-book now.)
- **Acceptance:** change only `version.json` (or a song title) → `sw.js` `cacheVersion` changes → a
  returning browser fetches the new bundle. Add a `build.mjs` unit test.
- **Dedup:** = `build-release-cacheversion-hash-gap-frozen-manifests`.

### P3-IMMUTABLE-PAGES — In-place page-image revisions can never reach returning devices  🟠 high `[code-grounded]`
- **Surface:** build/web. **Files:** `web/src/sw.js:184`, `web/build.mjs` (`_headers`
  `max-age=31536000, immutable`), page WebPs served old-cache-first + cross-cache seeding.
- **Problem:** page images live at stable filenames (`page-NNN.webp`) marked immutable for a year and
  served cache-first from retained old caches. A re-rendered scan (fixed image, different
  `ALVERNIA_PDF_RENDER_DPI`/`QUALITY`) never reaches a device that cached the old bytes — and clients
  silently mix resolutions with no purge path.
- **Fix:** content-hash page-image filenames (`page-NNN.<hash>.webp`) so revised pages get new URLs;
  keep immutable caching (now safe). Coordinate with P3-CACHEVERSION and the song→page→image manifest.
  **The correct long-term fix for the "song N shows the wrong/old scan" class** (song-370 fire-drill
  memory). Single-book only — the hymns-4 half of the original finding is MOOT.
- **Dedup:** = `build-release-immutable-page-urls-vs-changed-bytes`.

### P3-SW-LIFECYCLE (batch, med) `[code-grounded]`
- `offline-pwa-partial-install-wedges-updates` (`sw.js:135`): a partially-failed install wedges update
  delivery until the NEXT deploy. Add install retry / don't `skipWaiting` on a partial install.
- `offline-pwa-page-cache-eviction-by-recency` (`sw.js:108`): `activate` evicts page caches by recency,
  not completeness — can delete the only *full* offline bundle. Evict by completeness/version.
- `offline-pwa-query-string-offline-navigation-fails` (`sw.js:219`): offline navigation with any query
  (`?k=`, `?retry=`) bypasses the cached shell → browser error page. Normalize navigation to the shell.
- `perf/offline-mid-mass-deploy-force-reload` (`app.js:1999`): a deploy during Mass force-reloads every
  online follower within ~60s. Add a "don't reload while actively following a live director" guard.

### P3-lows (batch)
- `offline-pwa-global-cache-match-version-mix` (`sw.js:59`): shell lookups use global `caches.match`;
  a retained old static cache can serve mixed-version shells. Scope lookups to the current cacheVersion.
- `offline-pwa-precache-no-in-session-retry` (`app.js:567`): one transient failure aborts precache for
  the session. Retry with backoff.
- `offline-pwa-dead-offline-gate-ui` (`app.js:446`): the offline-download UI + verification are dead
  code; no on-device readiness signal exists. Wire it up (real readiness) or delete it (P8).
- `offline-pwa-unawaited-sw-async` (`sw.js:128`): `clients.claim()` + page-image `cache.put` are
  fire-and-forget. `event.waitUntil` them.

---

## P4 — Build / release pipeline hardening

> Byte-identical to build 370 (`git diff 309a9afa..HEAD -- scripts/release.sh` is empty), so every
> original finding survives. The pipeline has the right lockstep *design* but **zero atomicity**.

### B-RESTORE — `restore_codes` aborts the script under `set -e` when the private file is missing  🟠 high `[verified]`
- **File:** `scripts/release.sh:48,59` + `set -euo pipefail`. `restore_codes()` is
  `[ -n "$RESTORE_CODES" ] && mv ...` — a missing private file makes `RESTORE_CODES` empty, the `&&`
  returns 1, and under `set -e` the bare call at `:59` **aborts AFTER a successful archive but BEFORE
  the IPA copy and web deploy** → version bumped, native archived, web NOT deployed: lockstep silently
  broken. **Fix:** make `restore_codes` always return 0 (`if [ -n … ]; then mv …; fi; return 0`) — the
  same function A4 hardens with a `trap`.

### B-DEPLOYWEB — `npm run deploy:web` is a footgun outside the blessed pipeline  🟠 high `[verified]`
- **File:** `package.json:15`. `deploy:web` runs `wrangler pages deploy` WITHOUT `--branch main` (→ a
  **PREVIEW** deploy from any non-main checkout, mistakable for prod) and WITHOUT a version bump.
  **Fix:** remove it, or make it `--branch main` + refuse on a non-main branch + require a clean bump;
  document that prod web ships only via `release.sh`.

### B-ALTSCRIPTS — Alt archive scripts bypass web rebuild, WebBundle sync, code baking  🟠 high `[code-grounded]`
- **Files:** `scripts/testflight-upload.sh:23`, `testflight-upload-transporter.sh`,
  `submit-appstore.sh`. These archive without rebuilding web, syncing `ios/WebBundle`, or baking
  director codes → an IPA with a **stale web bundle** and **no director codes**. Two divergent export
  plists can drift. **Fix:** route all archive paths through one hardened "release engine" (rebuild web
  → sync WebBundle → bake codes → archive); collapse to one export plist; or delete the redundant scripts.

### P4-med (batch) `[code-grounded]`
- `build-release-bump-first-no-rollback` (`release.sh:22`): version bump is step 1 with no rollback;
  any later failure leaves 4 manifests bumped/dirty and a retry double-bumps. Bump LAST or snapshot+restore via the `trap`.
- `build-release-songindex-range-unchecked` (`build.mjs`): the standard `songIndex` passes to
  `pages.json` with no page-range check against the rendered page count — the "unreachable song" class
  (song-370 fire-drill). Range-check the standard songIndex; fail the build. (The hymns-4 clause is deleted.)
- `build-release-check-book-consistency-coverage` (`check-book-consistency.mjs:30`): soft-skips
  (exit 0) without `pdfinfo` and never runs in `release.sh`. Hard-fail without pdfinfo (or a check that
  doesn't need it) and call it from `release.sh`.
- `build-release-rm-rf-build-concurrent-tabs` (`release.sh:49`): unconditional `rm -rf build`;
  concurrent tabs clobber in-flight archives. Add A4's lockfile.
- `build-release-podfile-lock-copy` (`release.sh:36`): `cp ios/Pods/Manifest.lock ios/Podfile.lock`
  neutralizes the CocoaPods consistency guard. See P-BUILDENV (§11) — retire once `pod install` under
  pinned Ruby works; until then document why it's there.

### P4-low (batch)
- `build-release-bump-silent-regex-skip` (`bump-build.mjs:34`): manifest syncs are silent-on-no-match
  regex edits + a dead `offlineWebBundle` sync block (file no longer exists). Make each sync **fail
  loudly** if its pattern is absent; delete the dead block. (= `new-refactor` cleanup.)
- `build-release-dist-wipe-before-preflight` (`build.mjs:41`): wipes `web/dist` before checking
  required tools → a missing tool leaves no deployable bundle mid-fire-drill. Preflight all tools first.
- `build-release-preios-hooks-bump` (`package.json:8`): `preios` bumps the build number on every local
  `npm run ios`, dirtying 4 tracked files. Gate behind an env flag or move into `release.sh` only.

---

## P5 — Native shell + Swift mesh + director-role regressions (batch for a 2-device day)

> The Swift mesh file is **completely untouched by the refactor** (`git diff` empty), so its
> device-gated items survive verbatim. The refactor's big new native surface is the **director-role
> state machine** (no-auto-director + always-confirm + super-admin) — three genuine NEW regressions
> that ride the next TestFlight build alongside A3.

**Prior-audit reconciliation (verified against build 374):**
- HIGH#1 (live takeover) — **FIXED** (build 370).
- HIGH#3 (set-book→page two-script race) — **FIXED** + now doubly MOOT (single book, no set-book).
- `native-swift-24h-restore-remints-stale-director`, `native-swift-geo-overrides-chosen-book` — **MOOT**
  (24h auto-restore and the geo book-selection effect were both deleted).

### NEW-DIR-1 — No-auto-director boot silently demotes a director on restart  🟠 high `[verified]` `[regression: 38132dd3]`
- **File:** `PdfReaderApp.tsx:757` (boot always `becomeFollower`); dead breadcrumb `lastSyncRole` at `:442`.
- **Problem:** the refactor removed auto-restore-director entirely to fix the 2026-07-01 outage — boot
  now UNCONDITIONALLY `becomeFollower()`, and `lastSyncRole` (written every `become*`) is **never read
  back**. "Always ask" is right for a *deliberate* relaunch, but it creates an outcome **identical to
  the outage it was meant to prevent**: if the director's iPad is killed by iOS (memory pressure / OOM
  / the ErrorUtils trap) and relaunches mid-Mass, it boots straight to a normal-looking follower with
  **no signal** that the role was lost. If the operator doesn't notice they've stopped broadcasting,
  the whole congregation freezes on the last page with no director.
- **Fix (product decision — open question Q1):** on boot, READ `lastSyncRole`; if `'director'`, surface
  a one-tap "Estabas dirigiendo — ¿continuar como director?" confirm (calls `becomeDirector` with the
  re-confirmed code), OR at minimum inject a persistent visible banner telling the ex-director they are
  now a follower and must re-enter their code. **Silent demotion of the one role the Mass depends on
  must never be invisible.** Do NOT silently auto-restore (respect "always ask").
- **Acceptance:** two-iPad — A director on page 100, force-quit + relaunch → A gets a visible
  prompt/banner, B/web are not left frozen with no director.

### NEW-DIR-2 — `roleGeneration` bump before the confirm strands a link-less follower  🟠 high `[code-grounded]` `[DEVICE-GATED]` `[regression: 9473e596]`
- **File:** `PdfReaderApp.tsx:503` (bump), `:384-386` (becomeFollower retry gen-check), `:627` (⟳ recovery gate).
- **Problem:** `onDirectorCode` increments `roleGenerationRef.current` at `:503` for EVERY code entry
  — before it's known whether `becomeDirector` (only inside the Alert `onPress` at `:545`) will run.
  Race: on a fresh boot, `becomeFollower()` hits a transient mesh startup failure and enters its
  `await setTimeout(…,2000)` retry (`:384`) with `roleRef='follower'`. During that 2s the operator
  types their code + enter → `:503` bumps the generation → the confirm Alert appears → operator taps
  **Cancelar** → `becomeDirector` never runs. The sleeping `becomeFollower` wakes, sees
  `myGen !== roleGenerationRef.current` (`:385`), and **returns without the second
  `startNearbyFollower`** — no mesh follower session is ever established. `roleRef` stays `'follower'`,
  the UI looks normal, the device receives ZERO director pages. ⟳ recovery only fires for
  `roleRef==='off'` (`:627`), so it can't rescue this.
- **Fix:** move the `roleGenerationRef` bump OUT of the top of `onDirectorCode` — bump only when a role
  transition is actually committed (inside `performSoftReset`, already at `:479`, and inside
  `becomeDirector`'s `onPress`, already at `:400`). The unrecognized-code and Cancel paths must not
  bump. Alternatively, have Cancel re-arm the follower (`becomeFollower()`).
- **Acceptance:** two-iPad (or a mocked `startNearbyFollower` that throws once): trigger boot
  `becomeFollower` into its 2s retry, fire `onDirectorCode` with a valid code, Cancel the Alert →
  assert the second `startNearbyFollower` still runs (mesh session established).

### NEW-DIR-3 — Never-cleared `lastDirectorSnapshotRef` false-fires the destructive takeover warning  🟠 high `[verified]` `[regression: 38132dd3]`
- **File:** `PdfReaderApp.tsx:527-528` (`liveDirector` heuristic), `:773` (the only writer, never cleared).
- **Problem:** the always-confirm flow computes
  `liveDirector = Boolean(lastDirectorSnapshotRef.current) && roleRef.current !== 'director'` to decide
  whether to show the alarming, red-destructive "⚠️ Ya hay un director activo / Otro dispositivo está
  dirigiendo AHORA / Tomar el control" dialog. But `lastDirectorSnapshotRef` is **set-once-never-
  cleared**: written at `:773` when any director mesh page arrives, and nulled nowhere (not on
  DIRECTOR_CONFLICT, exit-director, director-gone, or becomeFollower). So once this device has received
  even one director snapshot this session, the flag is stuck true. **Mass scenario:** Braulio directs
  the opening hymn then exits; 20 min later Miguel walks to that same iPad to lead the recessional and
  enters his code — instead of the calm "¿Dirigir el coro?", he gets the red "another director is live,
  take control" warning implying he'd yank the role from a phantom. A cautious operator cancels and
  delays the liturgy — the exact hesitation that produces a no-director gap.
- **Fix:** give the ref a timestamp and treat `liveDirector` as true only within the mesh freshness
  window (mirror `RELAY_LIVE_MAX_AGE_S` / the ~2s heartbeat); AND clear the ref (or its recency) on
  DIRECTOR_CONFLICT / exit-director / a `state` event with `peerCount 0`. Keep the resync call sites
  (`:585`, `:636`) reading the sticky ref unchanged.
- **Acceptance:** two-iPad — A directs one page then exits; wait >2s; on B enter a code → EXPECTED calm
  "¿Dirigir el coro?", not the takeover warning. Inverse: A actively broadcasting, B enters a code →
  takeover warning DOES show.
- **Note:** this is the same defect independently surfaced by both the director-role and
  refactor-coherence finders — high-confidence.

### P5-PATCH-NOOP — `patch-rn-webview.js` silently no-ops if react-native-webview changes → blank app shipped  🟠 high `[verified]`
- **File:** `scripts/patch-rn-webview.js:45`. If the target string isn't found (a webview version bump),
  the patch `console.warn`s and `process.exit(0)` — install "succeeds," archive proceeds, and file://
  loads fail → **blank WebView on every iPad**, discovered only at Mass. **Fix:** `exit(1)` when the
  pattern is missing UNLESS the already-patched marker is present (idempotent); assert the installed
  react-native-webview version equals the `package.json` pin. Same for `patch-hermes-thread.js`.
  (= P-BUILDENV fail-hard patches, §11.)
- **Acceptance:** bump the webview version locally → `npm install` FAILS loudly instead of shipping blind.

### P5-ONERROR — Failed initial file:// load leaves a permanent blank WebView with no retry  🟡 med `[code-grounded]`
- **File:** `PdfReaderApp.tsx:929`. `onError` only breadcrumbs; a transient failed initial load bricks
  the app until manual relaunch. **Fix:** retry the load (bounded backoff) + a retry affordance. Pair
  with the `onContentProcessDidTerminate` recovery (also A3's trigger).

### P5-med (JS-side, testable, batch) `[code-grounded]`
- `native-swift-zombie-director-on-storage-failure` (`PdfReaderApp.tsx:439`): an AsyncStorage failure
  during `becomeDirector` leaves a zombie director (mesh advertising, web shows "código incorrecto").
  Treat storage failure as non-fatal to role assumption; don't advertise a role you couldn't persist.
- `native-swift-bridge-ready-unclamped-total` (`:566`): `bridge-ready` trusts `msg.page`/`msg.totalPages`
  unvalidated; a bogus `totalPages` poisons the clamp. Validate/clamp on adoption. (Overlaps A3.)
- `native-swift-page-changed-clamp-stale-total` (`:603`): clamps against OLD `totalPagesRef` before
  adopting the message's own `totalPages`. Adopt totalPages first, then clamp.
- `native-swift-render-failed-residual-race` (`:687`): the `-1` render-failed sentinel + `becomeDirector`
  immediate-broadcast promotion-window race. Clear the `-1` sentinel on role change, or re-drive from
  storage on promotion.
- `new-director-super-admin-label-gated-by-standard-set` (`:516`) 🟢 low: a super-admin-only code not
  also in `STANDARD_DIRECTOR_CODES` is rejected as "código incorrecto." Either fold super-admin into
  the auth gate (`STANDARD_DIRECTOR_CODES.has(code) || SUPER_ADMIN_CODES.has(code)`) or add a bake-time
  assertion that `superAdminCodes ⊆ standardDirectorCodes` (open question Q2).
- `new-director-dead-writes` (`:442`, `:611-614`) 🟢 low: `lastSyncRole` + `sv.book.lastPage.standard`
  are written (every role change / every page turn) but never read — dead AsyncStorage churn on the
  liturgy-hot path + a maintenance trap. Either wire the persisted page into the A3/boot restore
  (preferred — gives it a purpose) or delete the writes and their misleading "for restore" comments.

### P5-DEVICE-GATED (Swift mesh — one 2-device day, §10) `[DEVICE-GATED]`
- `native-swift-peer-bundle-unauthenticated-exec` (`DirectorSyncModule.swift:717`): peer web-bundle push
  is **unauthenticated** → a malicious peer on the wifi can push arbitrary code into a follower's
  WebView (persistent). **Highest-severity native item.** Authenticate/verify the bundle (signature +
  version), OR — strongly consider — **retire peer bundle push entirely** (P-MESH decision; P-OTA makes
  it redundant). Disabling `bundle_offer`/`bundle_request` (`swift:699-1049`) behind a kill-switch is
  itself a Swift edit → schedule on the device day.
- `native-swift-stale-documents-bundle-masks-update` (`PdfReaderApp.tsx:713`): a peer-pushed
  `Documents/WebBundle` is preferred by existence with no version compare → a TestFlight update silently
  runs OLD web code while the badge shows the NEW build. Version-stamp + prefer the newer of (baked,
  pushed); clean stale pushes on app update. (Ties to P-OTA.)
- `native-swift-relay-split-brain-no-tiebreak` — mesh half of P2-IDENTITY.
- `native-swift-dbglog-device-name-pii` (`DirectorSyncModule.swift:177`) — see P6-LOG (stop sending device names).

### P5-low
- `web-reader-fullscreen-fab-noop-on-ios-pwa` (`app.js`): the ⛶ fab shows on iOS home-screen PWAs where
  `toggleFullscreen` is a no-op. Hide it there.

---

## P6 — Security / privacy (recalibrated: geo + `/unlock` deleted)

> Calibrated to a parish app (threat = pranksters/curious users — but a prankster flipping pages
> during Mass IS the worst-case product failure). **The refactor resolved four of the original items
> for free:** the "private manual not actually gated" concern is gone (the book is now *openly
> public* — a deliberate product decision), `/unlock` is a no-op stub, the director code is no longer
> persisted, and the `/health` geo oracle is deleted (see §9).

- **A1, A2** (P0) are the two showstoppers (committed master code; no rate limiting).
- **P6-LOG** `sync-worker/src/index.ts:515` + `DirectorSyncModule.swift:177` 🟠 high: `/log` is
  world-readable and un-capped; Swift `dbgLog` POSTs real `UIDevice.current.name` (leaks owner-renamed
  devices). Anyone can harvest parishioner names and wipe mid-Mass diagnostics. **Fix:** authenticate
  `/log` reads (dashboard key), cap size + rate, and **stop sending device names** from Swift (hash or
  drop). (Merges `native-swift-dbglog-device-name-pii`; the Swift half is `[DEVICE-GATED]` — next build.)
- **P6-PRIVACY-POLICY** `docs/privacy-policy.html:22` 🟠 high: still claims zero data collection / no
  internet / Keychain storage, while the app transmits device ids, self-entered names, and iOS device
  names to a server. Materially false → App Store review + legal risk. **Rewrite to reflect reality
  (device id, optional name, relay + diagnostic telemetry) — and now DROP the location/geo disclosure
  entirely, since IP-geo was deleted** (confirm no residual CF-header read first — open question Q4).
  Align `PrivacyInfo.xcprivacy`. Coordinate with P6-LOG so the policy can stay minimal.
- **P6-FLEET-KEY** `sync-worker/src/index.ts:575` 🟡 med: `FLEET_DASHBOARD_KEY` (gates choir phone
  numbers) is passed as `?k=` with Worker observability ON → the secret lands in CF logs/history/
  referrers. **Fix:** accept via header/cookie, not the query string; rotate the key.
- **P6-FLEET-CHECKIN** `:553` 🟡 med: `/fleet/checkin` is open + unthrottled → an attacker evicts real
  check-ins and blinds the pre-Mass dashboard. Rate-limit + light auth (A2's limiter) + cap `nativeBuild`.

_MOOT here (retired by refactor, §9): P6-UNLOCK-ORACLE, P6-STANDARD-GATE, P6-CODE-STORAGE, P6-HEALTH-GEO._

---

## P7 — Performance / polish (web; aging parish iPads)

> Steady-state perf is solid (115 DPI / q60 WebP → ~69 KB/page, ~25 MB for the single 371-page book;
> cache-first; neighbor prefetch makes director-driven +1 turns instant once warm). The refactor
> simplified this surface (single precache pool now — the second "floor" pool for hymns-4 is gone; the
> dueling gate-caption backstops are gone). The sharp edges are on the exact paths that matter at Mass.

- **P7-IMG-RETRY** `web/src/app.js:21` 🟠 high: the live `<img>` error-retry (now a module-level block)
  re-commits a captured OLD src with no stale-request guard → a delayed retry can overwrite the newly
  committed page and strand a follower on the wrong page. **Fix:** consult
  `state.pageLoadRequest` in the retry closure; drop the retry if superseded.
- **P7-TIMEOUT-COMMIT** `:983` 🟠 high: the 3s `preloadImage` timeout **commits an unloaded src**, then
  unconditionally hides the loader and updates the song title → the OLD page shown under the NEW song's
  status with no affordance. **Fix:** on timeout keep the loader, don't retitle, offer retry; only
  commit on a real decode.
- **P7-WAKELOCK** (`app.js` follow path) 🟡 med: web followers have no Screen Wake Lock → a
  signovivo.com iPad/phone auto-locks minutes into Mass and goes dark/desynced. **Fix:** request
  `navigator.wakeLock` while following a live director (re-acquire on `visibilitychange`). **Big
  real-world reliability win** for web followers without the native keep-awake.
- **P7-med:** hidden-drawer `todas` rebuild — the full list (315 rows + ~27 headers, per-item innerHTML)
  is rebuilt twice at boot + on every drawer open (`app.js:2154`). Build once + cache the fragment.
  Precache contention — the 4-way ~25 MB precache starts ~4-5s after gate lift with no yield to live
  page loads (`app.js:532`); pause/deprioritize precache while a live page load is in flight (single
  pool now — the deleted floor pool half is MOOT).
- **P7-low:** offline poll-storm decay (`app.js:3100` — the deleted 5s geo re-heal loop is gone; collapse
  the remaining retry loops into one backoff), SW update-poll backoff over a 3h Mass (`app.js:1971`),
  page-cache double retention (`sw.js` — keep one complete bundle), search per-keystroke
  renormalization + debounce (`app.js:1171`).

### P7/web-reader correctness lows (batch) `[code-grounded]`
- `web-reader-theme-substring-shadows-text-search` (`app.js:1273`): `searchByTheme` substring preempts
  full-text search, so "santo" returns Espíritu-Santo-themed songs and makes Santo/Sanctus (incl. Canto
  371 "Santo Español") **unreachable via search**. Make exact-title/number matches win over theme-substring.
- `web-reader-initreader-pagesjson-no-ok-no-retry` (`app.js:3229`): non-inlined `pages.json` boot fetch
  has no `response.ok` check and no retry → one transient failure leaves `totalPages=1`, follower clamped
  to page 1. Check `ok`, retry with backoff, fail visibly. (Related: `new-web-boot-padwidth-clamp-fallback`
  — seed `state.totalPages` to `STANDARD_TOTAL_PAGES` (371) instead of 1 so the pre-manifest fallback
  actually engages; `new-web-renderstatus-total-fallback-divergence` shares this root cause.)
- `web-reader-loadsearchindex-no-ok` (`app.js:1153`): no `response.ok` check on the search-index fetch.
- `web-reader-window-keydown-ignores-editable-targets` (`app.js:2737`): arrow keys while typing in
  search/fleet fields turn the song + close the drawer; Escape double-fires. Bail when `event.target` is editable.
- `web-reader-rendersongitem-innerhtml-unescaped` (`app.js:2067`): OCR-derived `song.title`/`song.key`
  built into DOM via innerHTML unescaped — a latent injection point. Escape or use `textContent`.
- `web-reader-draft-cap-blocks-11-digit-code` (`app.js:1068`): the numpad caps drafts at 10 digits +
  strips leading zeros, while the live secret holds an 11-digit code. **Mostly moot once A1 removes the
  11-digit code;** otherwise raise the cap to 11 and keep codes as strings (never `Number()`).

---

## P8 — Dead code purge + docs (single-book)

> The refactor already did most of the *book-related* dead-code purge (it deleted ~904 lines of app.js).
> What remains is residual dead fields, the type/doc drift the refactor itself introduced, and the
> original stale docs. Do it in small reviewed commits AFTER P1 (so tests hold behavior), each verified
> by the build smoke + e2e, updating the footprint allowlist in the same commit. Verify "dead" with grep
> before each delete.

- **Purge (residual):** `new-web-dead-books-data-inline-blob` (`build.mjs:681` — still injects a
  `#books-data` script app.js no longer reads; drop it, keep `#pages-data`); the dead offline-gate UI
  (`app.js:446`, `index.html`); dead state fields (`syncRole`, `relay.appliedPage`, `relay.manualClose`);
  the index-panel subsystem if still present; dead CSS; the `offlineWebBundle` block in `bump-build.mjs`;
  `new-director-dead-writes` (`PdfReaderApp.tsx:442`, `:611-614` — remove or wire into restore).
- **Type drift the refactor introduced:** `new-refactor-dts-appmode-drift` — `src/nearbyDirectorSync.d.ts:15,30`
  still declares `mode?: "standard" | "nonStandard"` after `offlineBooks.ts` narrowed `AppMode`/`BookId`
  to `"standard"`. Narrow the `.d.ts` **together with** the contract-test assertion at
  `e2e/nearby-sync-contract.test.mjs:23` (they must land in one commit or CI reddens).
- **README** (`README.md`) — wholesale stale/false: rewrite to the single public "standard" book,
  WKWebView-shell-over-web-bundle architecture, relay sync; remove the `npm run test:e2e` quick-start
  (it mutates prod); fix the absolute paths into a different worktree.
- **HANDOFF.md** (`new-refactor-handoff-doc-describes-deleted-app`, `HANDOFF.md:105`) — still documents
  the deleted two-book + IP-geo + geo-gate architecture as current; a cold tab will believe hymns-4/
  Sión/geo still exist and chase ghosts. Rewrite the stale sections to build-374 reality or delete the file.
- **Stale docs:** `docs/web-follower-relay-plan.md` marked "PROPOSED (not started)" but shipped; mark
  historical + de-Siónize residual comments. `docs/app-atlas.md` — carries a build-374 banner but its
  body still describes two-book internals; refresh opportunistically as you touch each subsystem.

---

## 9. Findings ledger — MOOT-by-refactor + dedup

### 9a. Retired by the 370→374 refactor (~25 findings, now MOOT)

These target code the single-book refactor **deleted**. Do NOT implement them; do NOT let a stale test
resurrect them. Full per-finding disposition (with the deletion evidence) is in
`audit-reconciliation-374.md`.

| Retired finding | Why moot |
|---|---|
| `relay-sion-code-cross-book-publish` | Sión code + book-scoping deleted |
| `relay-ws-message-interleaved-apply-during-book-switch` | no cross-book `switchBook` exists |
| `relay-unauthorized-follower-renders-wrong-song-live` | single fully-public book |
| `relay-unlock-swallows-failed-switchbook` | unlock+switchBook flow deleted |
| `relay-browsing-user-book-switched-mid-browse` | no cross-book switch remains |
| `web-reader-snapshot-during-switchbook-renders-old-totalpages` | switchBook interleave deleted |
| `web-reader-persisted-standard-boot-reveals-hardcoded-hymns4-img` | first-paint img now the correct book |
| `web-reader-hydratebookdata-keeps-numeric-prefetch-sets` | hydrateBookData / book-switch deleted |
| `web-reader-gate-caption-pingpong-and-false-offline` | geo-gate reveal machine deleted |
| `offline-pwa-persisted-standard-reveal-flash` | wrong-book-flash mechanism deleted |
| `perf-persisted-standard-boot-wrong-book-reveal` | hardcoded hymns-4 first-paint deleted |
| `perf-book-switch-stale-prefetch-sets` | mid-Mass book switch impossible now |
| `perf-gate-caption-flicker-dueling-backstops` | dueling reveal loops deleted |
| `secpriv-unlock-brute-force-oracle` | `/unlock` gutted to a no-op stub |
| `secpriv-standard-manual-not-actually-gated` | manual is now **openly public** (product decision made) |
| `secpriv-director-code-plaintext-asyncstorage` | code no longer persisted at all |
| `secpriv-health-geo-oracle` | `/health` geo echo deleted |
| `native-swift-24h-restore-remints-stale-director` | 24h auto-restore deleted |
| `native-swift-geo-overrides-chosen-book` | geo book-selection effect deleted |
| `coherence-wrong-book-flash-on-standard-boot` | two-book/geo-gate machine deleted |
| `coherence-ws-interleave-stale-totalpages-clamp` | book-switch async race deleted |
| `coherence-director-code-asyncstorage-not-securestore` | code no longer persisted |
| `P2-SIÓN-ROOM`, `P2-WRONGSONG` (plan items) | single-book makes both moot |
| `P3-IMMUTABLE-PAGES` hymns-4 half | hymns-4.pdf + manifests deleted (standard half survives as P3) |

**Watch out:** MOOT ≠ "the code is clean." Several vestiges remain harmless-but-present (`bookId`/`mode`
still on the relay wire for build-373 compat, the `#geo-gate` element hidden immediately, the `/unlock`
stub). Leave the wire fields for compat; the stub stays; P8 cleans the type/doc drift.

### 9b. New findings introduced by the refactor (14)

Full EVIDENCE/FIX text in `audit-reconciliation-374.md`. Placement in this plan:

| New finding | Sev | Plan home |
|---|---|---|
| `new-director-role-webview-reload-broadcasts-boot-page` | 🔴 crit | **A3** (it IS A3's surviving half) |
| `new-director-no-auto-restore-silent-demotion-outage` | 🟠 high | **P5 / NEW-DIR-1** |
| `new-refactor-stale-livedirector-warning` | 🟠 high | **P5 / NEW-DIR-3** |
| `new-director-code-gen-bump-before-confirm-strands-follower` | 🟠 high | **P5 / NEW-DIR-2** |
| `new-refactor-relay-test-asserts-dead-twobook` | 🟡 med | **A5 / P1** |
| `new-refactor-contract-test-pins-dead-nonstandard-dts` | 🟢 low | **P1 / P8** |
| `new-refactor-dts-appmode-drift` | 🟢 low | **P8** |
| `new-web-boot-padwidth-clamp-fallback-fragile` | 🟢 low | **P7** (seed `totalPages`) |
| `new-web-renderstatus-total-fallback-divergence` | 🟢 low | **P7** (same root) |
| `new-web-dead-books-data-inline-blob` | 🟢 low | **P8** |
| `new-director-super-admin-label-gated-by-standard-set` | 🟢 low | **P5** (open Q2) |
| `new-director-dead-writes-laststate-role-and-page` | 🟢 low | **P5 / P8** |
| `new-refactor-handoff-doc-describes-deleted-app` | 🟢 low | **P8** |

### 9c. Surviving dedup (original 117 → plan items)

The relay/coherence/perf/offline dimensions corroborate each other; key merges survive verbatim:
`A1/A5` (committed-code + prod-mutating-test cluster), `A3` (director reload), `A4` (release PII swap),
`P2-SEQ`, `P2-IDENTITY`, `P3-CACHEVERSION`, `P3-IMMUTABLE-PAGES`, `P6-LOG`, `P7-IMG-RETRY`, `P1-RED`.
The `WRONG-BOOK-FLASH` merge row **dissolved** (all its members are MOOT, §9a). Nothing was dropped —
full text of every finding is in `audit-findings-raw.md` (original) + `audit-reconciliation-374.md` (new).

---

## 10. Device-gated work — the 2-device test day

CANNOT be proven in a simulator; batch into ONE session with two physical iPads on one wifi:
- **A3** (director reload → follower must NOT jump to page 2), **NEW-DIR-2** (cancel-strands-follower),
  **NEW-DIR-3** (stale takeover warning), **P2-IDENTITY** (mesh split-brain half), **P5-DEVICE-GATED**
  (peer bundle exec, stale documents bundle), and any Swift edit (the peer-bundle kill-switch).
- **Test plan:** (1) live takeover A→B; (2) split-brain: start B while A live → only one director within
  ~1s, no flapping; (3) director vanish → B taps ⟳ → recovers; (4) background/foreground both roles
  mid-session; (5) **A3 repro:** force a WKWebView content-process reap on A mid-page → B must NOT jump
  to page 2; (6) **NEW-DIR-1 repro:** force-quit A (director) + relaunch → A shows a resume prompt/banner,
  not a silent follower; (7) **NEW-DIR-3 repro:** A directs then exits, wait >2s, B enters a code → calm
  "¿Dirigir el coro?" not the red takeover warning.
- **Decision to make on that day (P-MESH, §11):** does the mesh ever save a Mass the relay couldn't?
  If P-OBS data shows it never delivered a page the relay didn't, retire it.

---

## 11. Strategic roadmap (the 12 proposals, reconciled to single-book)

Full designs are in `audit-findings-raw.md` (strategy section); leverage order for a solo dev:

1. **P-CI / P-TEST (= P1)** — safe harness + CI. **Highest leverage.** Do first. **Matrix simplifies
   hard** post-refactor: drop every Sión/book-scoping/X-Hymnal/unlock-oracle/geo case; the worker
   boundary set collapses to publish-auth + seq + fleet-key + CORS.
2. **P-OTA** — native shell checks signovivo.com for a newer web-bundle version and atomically swaps a
   locally-cached copy (keeping file:// offline-first + rollback on failed boot). **Still the #1 product
   win after P1 — unchanged by the refactor.** It closes the "web fix doesn't reach iPads" gap that
   shadows every web-surface item, and directly retires `native-swift-stale-documents-bundle` and the
   peer-bundle-push (which P-MESH may delete anyway). Effort L; design carefully (signature/version
   check, boot-failure rollback to the baked bundle).
3. **P-RELEASE-ENGINE (= P4)** — idempotent, resumable release stages + `scripts/doctor.mjs` preflight
   (tools, LANG, Ruby, node_modules-vs-lock). Absorbs A4, B-RESTORE, B-ALTSCRIPTS, P4-*. Unchanged —
   `release.sh` is byte-identical to build 370; the code-baking step now also covers `superAdminCodes`.
4. **P-OBS** — lightweight client error/telemetry into the existing `/log` + fleet dashboard;
   director-visible follower health. **Gains urgency** as the input to the now-stronger P-MESH retire
   decision. Fix P6-LOG first so it doesn't leak device names.
5. **P-MESH — now a STRONGER retire candidate.** Single-book removes cross-book routing as a mesh
   justification, the mesh is device-gated + untouched (0 diff), and it still hosts the highest-severity
   native item (unauthenticated peer bundle exec). Write retire criteria BEFORE the device day and lean
   toward retire-or-kill-switch: if P-OBS shows the mesh never delivered a page the relay couldn't,
   delete the Swift module + Bonjour keys + JS wrappers (huge complexity dividend; the 2-device test tax
   disappears).
6. **P-CONST — smaller surface now.** The Sión code + book-registry constants are gone; the shared-
   constant set is now `RELAY_BASE` (×5 hardcoded copies), the freshness window, the room name, and the
   director-code stores. Codegen a single source at build.
7. **P-STAGING** — a preview Pages + second relay room/worker env + a pre-Mass canary-walk ritual (oldest
   fleet iPad; page-turn < ~300ms; A3 + NEW-DIR-1 repro). The per-book/per-parish-rooms sub-goal is no
   longer a near-term driver (single book, single room); multi-parish rooms move to a future note.
8. **P-BUILDENV** — fail-hard postinstall patches (P5-PATCH-NOOP), pinned toolchain (`.tool-versions`,
   Ruby 3.x for CocoaPods), `npm ci` reconciliation of the known node_modules drift, retire the
   Manifest.lock copy once real `pod install` works. Reject containerization (the macOS archive can't be
   containerized). Unchanged.
9. **P8-MODULARIZE** — dead-code purge (P8, now mostly done by the refactor for book code) + optional
   bundler-free concat-module split later (extend `build.mjs` to concat `web/src/lib/*.js`). Reject a
   framework/TS rewrite of the web app.
10. **P-COMPAT** — minimal older-iPad floor: `--viewport-height` needs a `100vh` fallback before the
    `100dvh` override (else layout collapse on Safari <15.4), guard the top-level localStorage reads
    (`app.js:331,339` — white-gate boot brick), add osVersion+model to fleet check-in to *measure* the
    floor. Reject a full a11y program (content is scanned hymnal images).

---

## 12. Appendix — open questions for Miguel (decide before "done")

1. **DIRECTOR RESTART BEHAVIOR (the crux — NEW-DIR-1).** The refactor removed auto-director to fix the
   2026-07-01 outage, but over-corrected: a director whose app restarts mid-Mass now silently becomes a
   follower with **no re-prompt**. Do you want (a) a boot-time "you were directing — resume?" confirm
   dialog, (b) a short (~2-min) re-validate-then-resume window, or (c) accept silent demotion and rely
   on re-entering the code? This is a product/UX call, not a pure bug. **Recommended: (a)** — a one-tap
   resume respects "always ask" while making the demotion visible.
2. **SUPER-ADMIN CODE BAKING (NEW-DIR / P5-low).** `SUPER_ADMIN_CODES` is currently a *subset* of
   `STANDARD_DIRECTOR_CODES`; a super-admin-only code (not also standard) is rejected as "código
   incorrecto." Intentional (super-admin is always also a standard director), or a footgun to guard at
   bake time with an assertion?
3. **A1 RESIDUAL.** Is `12345678840` still needed in `TRANSMITTER_CODES` for any build-367 device, or
   can it be rotated out entirely? (Memory notes a residual device dependency — confirm before removal.)
4. **PRIVACY POLICY SCOPE (P6-PRIVACY-POLICY).** With geo fully deleted, the rewrite should DROP the
   location disclosure entirely — confirm the app sends no residual geo/postal signal (worker geo block
   deleted; verify no leftover `request.cf` read) before certifying the new policy.
5. **P-MESH RETIRE.** Single-book makes the mesh harder to justify. Schedule P-OBS instrumentation now so
   the next device day can make the keep/retire call on real data, or keep the mesh indefinitely as a
   Mass-day safety net regardless of usage?

Also still open from the original audit (unchanged): **`android/`** (audit focused on iOS — prune if
dead), **App Store review risk** (now just the privacy policy, since the geo-gated-content risk is gone),
**multi-parish scalability** (one hardcoded room `alvernia-main`), **i18n** (hardcoded Spanish),
**data-loss UX** (a localStorage wipe silently resets prefs; the offline-readiness signal is dead code).
**RESOLVED by the refactor:** the "is the Del Rio manual private or public?" product decision — it is now
**openly public**.

---

## Changelog

- **2026-07-04 — Reconciled build 370 → 374.** A 14-agent pass (delta-map → 9 reconcilers → 3
  new-surface finders → synthesizer, on Opus 4.8) re-triaged all 117 findings against the single-book
  refactor: ~25 MOOT (§9a), 43 moved, 16 changed, 11 partially moot, 25 survive; 14 new findings added
  (§9b, a new director-role state machine). P2/P6/P7 pruned of book/geo items; P5 gained the three
  director-role regressions; §9 gained the MOOT ledger; §12 gained 5 decisions. Receipts in
  `docs/audit-reconciliation-374.md`.
- **2026-07-01 — Original plan** (build 370, HEAD `309a9afa`): 6-agent deep-read + 9-dimension
  adversarial hunt → 117 findings deduped into P0–P8 + a 12-item strategic roadmap.

_End of plan. New-finding evidence + full disposition ledger: `audit-reconciliation-374.md`. Original
evidence: `audit-findings-raw.md`. Architecture: `app-atlas.md`. Contracts: `app-contracts.md`._
