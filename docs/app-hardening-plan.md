# SignoVivo — App-Wide Hardening Plan

> **What this is.** A full-app audit (build 370, HEAD `309a9afa`, 2026-07-01) turned into an
> executable plan. A 6-agent deep-read mapped every subsystem; a 9-dimension adversarial hunt
> produced 117 findings (6 critical, 30 high, 46 medium, 35 low); a strategist proposed 12
> roadmap items. This document **deduplicates** those into ordered workstreams another Claude Code
> tab can implement cold — reliability, robustness, security, performance, and cleanup, app-wide.
>
> **Companion docs (read as needed, all in `docs/`):**
> - `app-atlas.md` — architecture navigation map (per-subsystem, with file:line anchors).
> - `app-contracts.md` — the 99 cross-subsystem contracts (change anything crossing a boundary → check here first).
> - `audit-findings-raw.md` — the full unabridged 117 findings (complete EVIDENCE/FIX text behind every item ID here).
> - `audit-findings-index.md` — one-line index of all 117.
> - `sync-handoff-known-issues.md` — the PRIOR audit (build 344/345). This plan supersedes it and records which of its deferred items are now fixed vs still open.
>
> **Provenance & confidence.** Findings came from Fable-5 finder agents. The **security** dimension
> got a full adversarial-skeptic pass (marked `[skeptic-CONFIRMED]`). The remaining skeptic passes
> were cut off by usage limits, so the orchestrator hand-verified the entire P0 tier and the
> load-bearing highs by reading the code directly — those are marked `[verified]`. Items marked
> `[code-grounded]` cite real line numbers but were not independently re-verified: **the implementing
> tab must open the cited code and confirm the mechanism before writing the fix** (§ Ground Rules).
>
> Last updated: 2026-07-01.

---

## 0. TL;DR — the dashboard

| # | Item | Sev | Surface | Ships via |
|---|------|-----|---------|-----------|
| **A1** | Committed live director code `12345678840` (master credential in public repo) | 🔴 crit | worker + repo | secret rotation + git |
| **A2** | Relay has zero rate limiting + `seq=0` bypass + one shared room → anyone can hijack/freeze the live Mass | 🔴 crit | worker | `wrangler deploy` |
| **A3** | Director WebView reload / app-relaunch broadcasts boot **page 2** to the whole congregation | 🔴 crit | native | TestFlight |
| **A4** | `release.sh` PII swap has no crash-safe cleanup → a Ctrl-C can git-commit real phone numbers | 🔴 crit | dev/release | git |
| **A5** | `npm run test:e2e` publishes real page-flips into the production room (README tells people to run it) | 🔴 crit | tests | git |
| **B-series** | 30 highs: sync freeze/flap, wrong-book flash, white-gate brick, cacheVersion staleness, `/unlock` brute-force oracle, false privacy policy, world-readable `/log` PII, patch-noop blank app, deploy pipeline gaps | 🟠 high | mixed | mixed |
| **C/D-series** | 46 med + 35 low: robustness, perf, dead code, test coverage, docs | 🟡🟢 | mixed | mixed |

**The single most important concept in this plan is the _deploy surface_ (§2).** Because the native
iPads run a **file:// copy of the web bundle baked at archive time**, a web fix reaches
signovivo.com followers instantly but reaches the parish iPads **only on the next TestFlight build**
(until the OTA-bundle-refresh strategy P-OTA lands). Worker fixes reach everyone instantly. Plan the
work around this or you will "fix" a Mass-critical bug that never reaches the devices that matter.

---

## 1. Order of execution (recommended)

```
P0  Stop-the-bleeding      → A1 A2 A4 A5 now; A3 in the next native build   (this week, before a Mass)
P1  Safe test harness + CI → kill prod-mutating tests, go green, add unit seam (unblocks everything)
P2  Relay / sync robustness→ the sync freeze/flap/wrong-page highs           (web + worker + native)
P3  Offline / PWA update   → cacheVersion, immutable pages, SW lifecycle     (web + build)
P4  Build / release harden → release.sh atomicity, deploy:web, alt scripts   (dev/release)
P5  Native shell + Swift   → device-gated items, batched for a 2-device day  (TestFlight)
P6  Security / privacy     → rate limiting, /log, /unlock, privacy policy    (worker + native + store)
P7  Perf / polish          → wake lock, precache contention, list rebuilds   (web)
P8  Dead code + docs        → purge ~25% of app.js, fix README, stale tests   (web + repo)
────────────────────────────────────────────────────────────────────────────
Strategic roadmap (P-OTA, P-CI, P-OBS, P-STAGING, P-CONST, P-MESH, …)  — §11, run after/with P1-P8
```

P0 items are independent — do them in any order. Everything else assumes **P1 is done first** so you
have a safety net (right now there is effectively none: the only behavioral test mutates production).

---

## 2. Deploy surfaces — memorize this

| Surface | Files | Reaches | Latency | How |
|---|---|---|---|---|
| **worker** | `sync-worker/src/index.ts`, `wrangler.jsonc`, secrets | **everyone** (server-side) | instant | `wrangler deploy` |
| **web** | `web/src/*`, `web/build.mjs` | **signovivo.com followers** instantly; **native iPads only on next archive** (bundle is baked) | instant (web) / next build (native) | `release.sh` web leg / Pages |
| **native** | `PdfReaderApp.tsx`, `ios/**`, `src/*.js|ts`, `scripts/patch-*.js` | **parish iPads only** | next TestFlight build | `release.sh` full |
| **dev/release** | `scripts/*`, `package.json`, `e2e/*`, docs | nobody at runtime — safety/hygiene only | n/a | git |

**Consequences that shape the plan:**
- A3 (director broadcasts page 2) is in `PdfReaderApp.tsx` → **native only**. It cannot be hot-fixed;
  it rides the next build. Until then, mitigate server-side if possible (A3's worker-side guard, §P2).
- Any web fix to `app.js` that must protect **iPad followers** is not truly shipped until either a
  TestFlight build or the **P-OTA** native-bundle-refresh strategy exists. Note this on every such item.
- Worker fixes (rate limiting, code rotation, `/log`, `/unlock`) are the highest-leverage because they
  protect every client the moment `wrangler deploy` returns.

---

## 3. Ground rules for the implementing tab

1. **Safety (hard):**
   - **Never run `npm run test:e2e`** until P1 neutralizes `e2e/relay-sync.test.mjs` — it publishes to
     the PRODUCTION relay room `alvernia-main` and flips live followers' pages. Run individual safe
     files with `node --test e2e/<file>.test.mjs`.
   - **Never deploy the worker or Pages during or right before a Mass** — a Pages deploy force-reloads
     every online follower tab within ~60s (finding C-DEPLOY-RELOAD), and a worker deploy briefly drops
     WebSockets. Deploy on a green weekday.
   - **Never commit secrets or PII.** Real director codes live only in gitignored
     `director-codes.private.json`; the roster lives only in gitignored `sync-worker/roster.private.json`.
     Check `git diff` for phone numbers before every commit (A4 is exactly this failure mode).
2. **Verify before you fix.** Every `[code-grounded]` item's line numbers are as of HEAD `309a9afa`.
   Open the file, confirm the mechanism still holds (build 370 has drifted from the prior audit — see
   the several "already fixed" notes below), THEN fix. Don't fix a bug that isn't there.
3. **Verify before you claim.** Follow the acceptance criterion on each item. For sync items that need
   two devices, you cannot prove them in a simulator — see §Device-gated.
4. **Hunt → fix → re-hunt → verify** (Miguel's §11): after a batch of fixes, re-read the changed code
   adversarially for regressions your fixes introduced, then verify. A clean re-hunt is the "done"
   signal, not a green first pass.
5. **Multi-tab safety (§4):** snapshot `git rev-parse HEAD` + `git status` at start; this repo is often
   worked in parallel tabs. Prefer a worktree. Don't touch another tab's branch/worktree.
6. **No EAS** for Miguel's own builds — TestFlight + local `release.sh` only.
7. **Ship discipline:** small reviewed commits, one concern each. Web/worker fixes can ship the day they
   land (verify in a real browser first). Native fixes batch into a single TestFlight build. Update
   `version.json` on every build bump; the badge/fleet check-in read it. **Never ship red.**
8. **Device-gated items** (marked `[DEVICE-GATED]`) touch the Multipeer Swift mesh and CANNOT be proven
   without two physical iPads on one wifi. Batch them ALL into one sanctioned 2-device test day (§10).
   Do not ship a blind Swift change to the mesh.

---

## P0 — Stop the bleeding (this week)

> Four of these ship server-side/repo-side today. A3 is native and rides the next build; its
> server-side half (P2-SEQ / P2-IDENTITY) can blunt it sooner.

### A1 — Committed live director code is a public master credential  🔴 `[verified][skeptic-CONFIRMED]`
- **Surface:** worker (secret) + repo. **Files:** `e2e/relay-sync.test.mjs:21`, `TRANSMITTER_CODES` secret, `sync-worker/src/index.ts:336-345`.
- **Problem:** `const CODE = "12345678840"` is committed in the repo and is still in the live
  `TRANSMITTER_CODES` secret (build-367 back-compat). `validTransmitterCodes()` accepts it, so anyone
  who reads the public repo has a full-privilege, **unrestricted** (not Sión-book-scoped) director code:
  they can publish into the live Mass room AND `/unlock` the "private" standard manual from anywhere.
- **Failure scenario:** a curious parishioner (or anyone who finds the repo) POSTs page-flips to
  `alvernia-main` during Mass; every follower iPad and phone jumps to their chosen page/book. They can
  also read the private Del Rio manual on cellular by unlocking with this code.
- **Fix:**
  1. Confirm no real device still authenticates with `12345678840` (grep the native baked codes; the
     Sión public code `1234567890` is separate and legitimate). Memory notes a residual build-367 device
     dependency — verify Braulio's/any transmitter's real code is provisioned before removal.
  2. Remove `12345678840` from `TRANSMITTER_CODES` (`wrangler secret put TRANSMITTER_CODES`).
  3. Replace the test's hardcoded code with an env var / dedicated **test room + test code** (see P1).
  4. `git grep 12345678840` → ensure zero committed occurrences remain.
- **Acceptance:** POST publish to `alvernia-main` with `X-Director-Code: 12345678840` returns 401; a real
  director still publishes fine; `git grep` is clean.

### A2 — Relay has no rate limiting; `seq=0` bypass; one shared room  🔴 `[verified][skeptic-CONFIRMED]`
- **Surface:** worker. **Files:** `sync-worker/src/index.ts` publish path (~746-800), seq guard (120-124).
- **Problem:** there is no throttle on `/publish`, `/unlock`, `/fleet/checkin`, or `/log`. Combined with
  a valid code (A1) or the public Sión code, an attacker floods `/publish`; `seq=0` is always accepted
  (bypasses the monotonic guard); a single global room `alvernia-main` means one hijacker reaches
  everyone. Even without a code, `/unlock` and `/log` and `/fleet/checkin` are open to abuse.
- **Failure scenario:** page-flip flood holds the congregation on the wrong page; or a `seq=0` spam loop
  makes every follower's page thrash; or `/fleet/checkin` spam evicts every real check-in so the pre-Mass
  dashboard shows nobody ready.
- **Fix (worker, defense-in-depth):**
  1. Add a per-IP + per-room token-bucket rate limit on all mutating routes (Durable Object storage or
     `caches`-based counter; a few writes/sec is plenty for a real director). Return 429.
  2. Gate `seq=0`: only accept `seq=0` as a genuine "reset" from an authenticated code AND only when the
     current snapshot is already stale — never as a live override.
  3. Cap `/log` and `/fleet/checkin` bodies and rate (P6 covers these in full).
  4. Consider a per-transmitter room token so a flood can't reach followers who joined via the real
     director (ties to P2-IDENTITY).
- **Acceptance:** a scripted 100-req/s publish burst gets 429s after the first few; a real director's
  ~1 flip/10s is never throttled; `seq=0` from an unauthenticated caller is rejected.

### A3 — Director reload / relaunch broadcasts the web's boot page 2 to everyone  🔴 `[verified]`
- **Surface:** native (TestFlight). **File:** `PdfReaderApp.tsx:553` (adopt) → `:569-571` (broadcast); boot restore `:727-751`; persisted-but-unread `sv.book.lastPage.<book>` written `:599`.
- **Problem:** on the director iPad, ANY WebView content-process termination (routine under iOS memory
  pressure), soft reset, `bundleUpdated` remount, or app relaunch makes the web boot to
  `DEFAULT_START_PAGE=2` and fire `bridge-ready{page:2}` / `page-changed{page:2}`. `handleMessage`
  adopts it (`currentPageRef.current = msg.page`, :553) and, because `roleRef==='director'`, broadcasts
  it over the mesh AND the relay (:571). The comment "the director's own page is authoritative" is
  exactly wrong — the ref was just clobbered by the boot page. App-relaunch is worse: the 24h
  role-restore auto-becomes director and broadcasts `currentPageRef` = the `useRef` initial value, then
  2; the per-book last page persisted at :599 is **never read back anywhere**.
- **Failure scenario:** mid-hymn on page 250, the director iPad's WKWebView process dies → reload →
  within ~2s every mesh follower iPad and every signovivo.com phone flips from 250 to 2, and the
  director's own screen shows 2. The music leader now has to re-find the hymn while the congregation is
  scattered. **This is the single worst live-Mass failure in the audit.**
- **Fix (`PdfReaderApp.tsx`):**
  1. On `bridge-ready` when `roleRef==='director'` **or** `explicitTransmitterRef` is set: do NOT adopt
     `msg.page` (:553); instead re-drive the web to the native-authoritative page via
     `injectEvent({type:'sync-event',event:{type:'page',page:currentPageRef.current,book:currentBookRef.current}})`
     (mirror the follower-resync branch at :572-584) and suppress adopting/broadcasting `page-changed`
     until the web echoes that page back.
  2. In the boot effect (:727-751), read `AsyncStorage['sv.book.lastPage.'+startBook]` into
     `currentPageRef` so the app-relaunch restore publishes the real last page, not 1 (`becomeDirector`
     at :776-777 broadcasts before the WebView loads).
  3. Edge cases: followers must keep adopting `msg.page` as today; a genuinely fresh director (no stored
     lastPage) still lands on the book default; the transmitter-only path (`roleRef 'off'` +
     `explicitTransmitterRef`) needs the same suppression because its relay heartbeat reads `currentPageRef`.
- **Server-side interim mitigation (worker, ships now):** because the native fix waits for a build, add
  a guard so a director's published page can't *regress to the book default* within the freshness
  window without an explicit reset flag — see **P2-SEQ** (this won't fully fix A3 but blunts the
  "yanked to page 2" blast radius for web followers immediately).
- **Acceptance:** simulator + browser follower on a DEV room: become director, go to page 40, trigger a
  WebView reload (dev-menu) → follower stays on 40, relay `/state.page` never becomes 1 or 2. Repeat with
  a full app relaunch inside the 24h window. Add a `handleMessage` unit test: `bridge-ready{page:2}` while
  `roleRef==='director'` must not change `currentPageRef` and must not publish page 2.

### A4 — `release.sh` PII swap has no crash-safe cleanup  🔴 `[verified][skeptic-CONFIRMED]`
- **Surface:** dev/release (git). **File:** `scripts/release.sh:40-59`.
- **Problem:** the script `cp`s gitignored `director-codes.private.json` (real phone numbers) over the
  **tracked** `director-codes.json` at :42, archives (~10 min), then restores at :58-59. There is **no
  `trap ... EXIT`**. A Ctrl-C, crash, or power loss between :42 and :59 leaves real phone numbers in the
  tracked file; the next `git add -A` commits PII to the public repo. Two concurrent `release.sh` runs
  also race on `director-codes.committed.bak`.
- **Failure scenario:** Miguel starts a 7am pre-Mass build, the archive hangs, he Ctrl-C's to retry, then
  commits a version bump — real parishioner phone numbers land in the public git history.
- **Fix (`scripts/release.sh`):**
  1. `trap restore_codes EXIT INT TERM` immediately after the swap, so cleanup runs on ANY exit.
  2. Make `restore_codes` idempotent and safe when the private file is missing (also fixes B-RESTORE, below).
  3. Guard against concurrent runs (lockfile; also fixes C-RMBUILD).
  4. Belt-and-suspenders: a pre-commit hook that greps staged `director-codes.json` for digits and blocks.
- **Acceptance:** kill the script mid-archive → `git status` shows `director-codes.json` clean (empty
  array), no private numbers staged. Simulate missing private file → script warns and restores cleanly.

### A5 — `npm run test:e2e` mutates the production relay room  🔴 `[verified][skeptic-CONFIRMED]`
- **Surface:** tests (git). **Files:** `e2e/relay-sync.test.mjs:16-21,111-244`; `README.md:17`.
- **Problem:** the suite's only behavioral file hardcodes `BASE=` the prod worker, `ROOM="alvernia-main"`,
  `CODE="12345678840"` and POSTs ~20 real page-flips (including a `bookId` switch to hymns-4). Live
  followers obey them and see a "live director" for ~90s after. README's quick-start tells users to run it.
- **Failure scenario:** anyone following the README near Mass time flips the whole congregation's pages.
- **Fix:** this is neutralized by **P1** (safe harness). Immediately: mark the file to **refuse to run
  against prod** — require `RELAY_TEST_ROOM` + `RELAY_TEST_CODE` env vars and `throw` if `ROOM` resolves
  to `alvernia-main`; fix README to stop instructing it. Full replacement in P1.
- **Acceptance:** `npm run test:e2e` with no env set skips/throws the relay file (doesn't publish); README
  no longer references it.

---

## P1 — Safe test harness + CI (do this first, before P2-P8)

> The strategist's #1 leverage item. Today the test pyramid is inverted: the only behavioral test
> mutates prod, the four Mass-critical components (`app.js` 3928 lines, `sync-worker` 818 lines,
> `sw.js`, `build.mjs`) have **zero tests**, and the suite is **red at HEAD** (masking every
> regression). This workstream builds the net that makes P2-P8 safe.

### P1-RED — Fix the red suite at HEAD  🟠 high `[verified]`
- **File:** `e2e/repo-minimal-footprint.test.mjs:15-27`. The exact-allowlist assertion was never updated
  for the `deploy:web` script added to `package.json`. Update the allowlist (and re-check the other
  exact-match assertions in that file for the same staleness). Confirm `node --test e2e/*.test.mjs` (minus
  the neutralized relay file) is green. **Do this before anything else** so "green" means something.

### P1-HARNESS — Local relay for behavioral sync tests  🟠 high `[strategy P1]`
- Stand up `sync-worker` under `wrangler dev` (miniflare) with a **dedicated test room** and **test code**;
  rewrite `relay-sync.test.mjs` to hit `localhost` (or a gated `sync-worker-staging` env), never prod.
  Assert the real protocol: seq strictly-greater-wins, `seq=0` handling, 90s freshness, book-scoping,
  `{ignored:true}` on regressed seq, takeover-after-stale. This also gives P2/P6 a place to prove
  worker fixes.

### P1-WORKER-UNIT — First real worker tests  🟠 high `[verified: zero tests exist]`
- `sync-worker/` has NO test directory. Add `vitest` + `@cloudflare/vitest-pool-workers` (or miniflare)
  unit tests for the security boundaries that currently have zero protection: Sión book-scoping (must
  reject non-hymns-4), `/unlock` gating (rejects Sión code, accepts real code), fleet PII gate (`/fleet`
  requires the key), rate limiting (P6), CORS. These guard the highest-value invariants server-side where
  fixes ship instantly.

### P1-APPJS-UNIT — Unit seam for the 3928-line reader  🟠 high `[strategy P1/P11]`
- Extract pure logic into testable units under `node --test` (+ jsdom where DOM is needed): `clampPage`,
  `resolveSongPage`, `searchPages` ranking, `applyRelaySnapshot`'s seq/freshness decision (the exact
  logic in P2-SEQ), numpad digit-routing (Canto 371, 5+-digit code path), `relayIsFreshLive`. Start with
  the functions the P2 fixes touch so those fixes land with tests. This is the seam the modularization
  (P8/P-CONST) grows into — no bundler required yet (plain `module.exports` shims, concat at build).

### P1-CI — GitHub Actions on PR  🟠 high `[strategy P2; note: no CI exists today]`
- Add `.github/workflows/ci.yml`: `npm ci` (guards lockfile installability), `npm run typecheck`, the
  **safe** e2e subset, the new worker + app unit tests, and a `web/build.mjs` smoke (build succeeds, boots
  in jsdom). Never run the prod-mutating file. This permanently enforces P1-RED and catches P2-P8
  regressions.
- **Delete/convert stale tests** here (repo rule: prefer deleting dead-behavior tests over rewriting green):
  - `test-suite-permission-flow-dead-takeover-pin` — pins `requestDirectorTakeover`/`approve` plumbing for
    a dismantled UX → delete the dead-flow assertions (keep any live wire-contract pins).
  - `test-suite-nearby-sync-regex-pins` — keep the cross-layer wire-contract pins (payload field names,
    Bonjour types), delete the implementation-shape pins that break on refactor.
  - `test-suite-vacuous-staleness-90s` — the `includes("90")` test can never fail; replace with a real
    harness assertion of the 90s contract (P1-HARNESS).
  - `test-suite-eas-config-banned-pathway` — EAS is banned for this project; delete the EAS-enforcing
    assertions, and instead guard the assets `app.json` ACTUALLY references (`03_icon_1024x1024.png`,
    `04_splash_1668x2388.png`), which are currently unguarded.
  - `test-suite-song55-test-name-lies` / `native-entrypoint` — the "clamps" test just greps a literal
    string; make it assert `clampPdfPage` behavior via the real module or delete it.

---

## P2 — Relay / sync robustness

> The sync layer is well-defended in its bones (zombie-CONNECTING timers, per-socket heartbeats, seq
> sanitization, durability-first publish, geo anti-brick fallbacks — several prior-audit items ARE fixed
> in build 370). The residual holes cluster around **director page authority**, **staleness/takeover on
> a healthy socket**, **transmitter identity**, and **async interleaving during book switches**.

### P2-SEQ — Seq guard runs before the freshness check → dead director never demoted; regressed-seq takeover ignored  🟠 high `[verified]`
- **Surface:** web (+ protects iPad followers only on next build/OTA). **File:** `web/src/app.js:3407` (guard) vs `:3418` (freshness).
- **Problem:** `applyRelaySnapshot` returns at the seq guard (`!force && snap.seq <= relay.lastSeq`)
  BEFORE evaluating 90s freshness. Over a healthy WS the only inbound traffic after a director stops is
  the follower's own ping replies (seq === lastSeq), dropped at :3407 — so the staleness branch that
  clears `relay.hasDirector` is unreachable. **Failure A:** director ends Mass and closes the app; every
  foregrounded, wifi-stable follower keeps a pulsing green "en vivo" pill pointing at a dead director
  indefinitely. **Failure B (worse):** the worker deliberately accepts a LOWER seq after 90s staleness
  (takeover self-heal, `index.ts:120-124`), and native seqs are wall-clock ms
  (`directorRelaySync.js:59`), so a handoff to a slower-clock device publishes seqs BELOW the old
  director's → every WS push from the new director is silently dropped at :3407 until wall-clock catches
  up. Followers on stands (never backgrounded, socket healthy) freeze under a green pill with no recovery
  but a manual ⟳.
- **Fix:** evaluate `hasPublished && relayIsFreshLive(snap)` BEFORE the seq guard — a stale snapshot must
  always run the demotion branch regardless of seq; only apply the monotonic guard to *fresh live*
  snapshots. Keep `force` bypassing the seq guard for a fresh director sitting still. (This is the exact
  logic to unit-test in P1-APPJS-UNIT.)
- **Interim value for A3:** hardening the demotion/authority path here reduces A3's web-follower blast
  radius before the native build.
- **Acceptance:** harness test — publish seq 100 fresh, then let it go >90s stale, then publish seq 50:
  the follower must accept seq 50 (takeover) and, after staleness, drop the green pill. Unit-test the
  decision function directly.
- **Dedup:** = `coherence-seq-regression-freezes-ws-followers`. Prior audit build-344 row only fixed the
  manual-⟳ path; the automatic WS path is still frozen.

### P2-IDENTITY — No transmitter identity → two publishers ping-pong followers with no arbitration  🟠 high `[code-grounded]` `[partly DEVICE-GATED]`
- **Surface:** worker + native. **Files:** `src/directorRelaySync.js:57-61` (seq = wall-clock ms, no
  device id), `sync-worker/src/index.ts` publish, `coherence-ignored-publish-response`.
- **Problem:** the relay protocol carries no transmitter identity. Two authorized publishers (e.g. a real
  director + anyone with the public Sión code, or two directors during a handoff) both publish into the
  one room; the worker's seq guard can't distinguish them, so followers flap between their pages. The
  worker returns `{ok:true, ignored:true}` when it drops a regressed-seq publish, but the native
  transmitter (`directorRelaySync.js:88`) **ignores the `ignored` flag** — a handoff director broadcasts
  into the void with no warning.
- **Fix:**
  1. Add a `transmitterId` (stable per device, e.g. from SecureStore) to the publish payload and snapshot.
  2. Worker: on a fresh live snapshot from a *different* transmitterId, apply an explicit tiebreak (newest
     token wins) and stamp the winner; reject the loser with a distinct reason.
  3. Native: consume `{ignored:true}` / the reject reason and surface "another director is live" to the
     transmitter UI (there's already a relay-auth warning bridge to reuse — `directorRelaySync` →
     `showRelayAuthWarning`).
- **Device-gated portion:** the mesh side of split-brain (two directors on Multipeer) needs 2 devices
  (§10). The relay side above is testable in the P1 harness.
- **Dedup:** = `native-swift-relay-split-brain-no-tiebreak`, `coherence-ignored-publish-response`; prior
  HIGH#2 (dual-director split-brain) — mesh half was addressed in 345, relay half is still open.

### P2-SIÓN-ROOM — Public Sión code can disrupt a standard Mass via the single shared room  🟠 high `[verified]`
- **Surface:** worker. **File:** `sync-worker/src/index.ts:795-799` (book-scope guard) + single-room design.
- **Problem:** the book-scope guard **works** — a Sión code CANNOT publish standard pages (returns 403).
  But there is ONE room `alvernia-main`, so a holder of the public in-repo Sión code can publish *hymns-4*
  pages into the room a standard Mass is using and yank every follower to the public book.
- **Fix:** separate rooms per book/context (e.g. `alvernia-standard` vs `alvernia-hymns4`), OR require the
  Sión code's publishes to target a distinct room that standard followers don't subscribe to. Coordinate
  with P2-IDENTITY and the multi-parish scalability question (§11 P-STAGING / rooms).
- **Acceptance:** harness — a Sión-code publish cannot change the page/book seen by a standard-room subscriber.

### P2-SWITCHBOOK-RACE — WS snapshot interleaves a cross-book `switchBook`, rendering against the OLD totalPages  🟠 high `[code-grounded]`
- **Surface:** web (+ iPad next build). **Files:** `web/src/app.js:3635` (un-awaited `applyRelaySnapshot`
  per WS message), `:3377` (render vs switch), `switchBook` sync `currentBook` write.
- **Problem:** each WS message calls `applyRelaySnapshot` without awaiting; a second push can interleave a
  cross-book switch at the first `await switchBook`, so a page is clamped/rendered against the OLD book's
  `totalPages` → follower stuck on the wrong page under a green live pill for up to ~12s. This is the
  web-relay surface of prior HIGH#3 (set-book→page two-script race).
- **Fix:** serialize snapshot application (a promise queue / generation guard keyed by `bookSwitchGeneration`)
  so a page event that arrives mid-switch either awaits the switch or is superseded; never clamp against a
  stale `totalPages`. Also fixes `native-swift-page-changed-clamp-stale-total` (`PdfReaderApp.tsx:590`
  clamps against OLD `totalPagesRef` before adopting the message's own totalPages — apply the message's
  totalPages first, then clamp).
- **Acceptance:** harness — interleave a `page{book:standard,page:300}` immediately after a
  `page{book:hymns-4}` switch; follower must land on standard 300, never a clamped-to-51 page.
- **Dedup:** = `relay-ws-message-interleaved-apply-during-book-switch`, `coherence-ws-interleave-stale-totalpages-clamp`.

### P2-WRONGSONG — Unauthorized follower renders the wrong hymns-4 song under a green pill  🟡 med `[code-grounded]`
- **File:** `web/src/app.js:3393/3377`. When a director broadcasts a standard-book page ≤ 51, an
  unauthorized (hymns-4-only) follower renders that page number in hymns-4 — a real but wrong song —
  under a green "en vivo" pill. **Fix:** a follower must not render a live page whose `bookId` it isn't
  authorized/loaded for; show a neutral "el director está en el manual" state instead. Ties to P2-SIÓN-ROOM.

### P2-POLL-GAP — Polling stopped before the socket opens → repeating ~6s blind windows on WS-hostile nets  🟡 med `[code-grounded]`
- **File:** `web/src/app.js:3585`. `connectRelay` kills `/state` polling before the WS opens; on networks
  where WebSockets never establish (some parish/guest wifi), this creates repeating blind windows. **Fix:**
  keep a low-rate `/state` poll alive until the socket is actually `OPEN`; only then stop it. Ties to the
  zombie-CONNECTING timeout already implemented.

### P2-CLOCKSKEW — Follower with a fast clock never sees the director as live  🟡 med `[code-grounded]`
- **File:** `web/src/app.js:3333`. `relayIsFreshLive` compares the raw client clock to the server `ts`
  with no offset calibration; a device clock >90s fast treats every snapshot as stale. **Fix:** calibrate a
  client↔server clock offset from the WS snapshot/`/state` response (server sends `ts`; client records
  receipt time) and apply it in the freshness comparison. This also de-risks P2-SEQ Failure B.

### P2-TRANSMITTER-RESTORE — Transmitter-only role never persisted → app relaunch silently stops publishing  🟡 med `[code-grounded]`
- **File:** `PdfReaderApp.tsx:409`. A transmitter-only (no-mesh) director's role isn't persisted; an app
  relaunch silently stops all relay publishing while the director thinks they're still live. **Fix:**
  persist + restore `explicitTransmitterRef` alongside the mesh role in the 24h restore path (coordinate
  with A3's boot-effect changes and native-swift-24h-restore, which wants the OPPOSITE for mesh — restore
  the transmitter relay role but re-validate before re-minting a mesh director).

### P2-lows (batch)
- `relay-unlock-swallows-failed-switchbook` (`app.js:3504`): `unlockStandard` reports "unlocked" + pins geo
  even when `switchBook` failed → device stuck on hymns-4 while claiming standard. Await + roll back on failure.
- `relay-browsing-user-book-switched-mid-browse` (`app.js:3377`): a director's cross-book move switches a
  deliberately-browsing follower's book despite the browsing guard. Extend the browsing guard to cover book.
- `web-reader-browse-result-click-skips-relay-browsing` (`app.js:2818`): tapping a song in browse/search
  never sets `relay.browsing`, so the next push yanks the follower back (numpad jump sets it correctly).
  Set `relay.browsing` on browse/search selection too.

---

## P3 — Offline / PWA update correctness

> The SW/offline design is genuinely defensive (per-asset `allSettled` installs, shell-ready activation
> gating, cross-version page-cache fallback, decode-guarded white gate, native file:// correctly skips
> all SW paths). But two load-bearing assumptions are unenforced, and they cause **silent permanent
> staleness** — the worst kind for a Mass tool.

### P3-CACHEVERSION — `cacheVersion` hashes only 5 shell files → book/version changes never bust caches  🟠 high `[verified]`
- **Surface:** build → web. **Files:** `web/build.mjs:10-28` vs `:574-579,673-683,779-791`; `web/src/sw.js:1`.
- **Problem:** `cacheVersion` is a hash of 5 `web/src` files + `build.mjs`. The PDFs,
  `assets/standard/*.json`, `src/alverniaManual2SongIndex.js`, and `version.json` are excluded. A deploy
  where only book data or `version.json` changed (and HEAD is unchanged — `release.sh` deploys with
  `--commit-dirty=true`, so dirty deploys are normal) produces a **byte-identical `sw.js`** → browsers
  never update → returning users keep the old app.js, old badge, old song index, and (because page WebPs
  are immutable+retained) stale page images forever at the same URLs. The comment at :16-20 claims the
  opposite guarantee.
- **Fix:** include ALL deploy inputs in the `cacheVersion` hash — the two PDFs, every
  `assets/standard/*.json`, the song-index source, `version.json`, and the generated per-book manifests.
  Simplest robust approach: hash the final `dist/` tree (or a manifest of content hashes) after build,
  not a hand-picked file list.
- **Acceptance:** change only `version.json` (or a song title) → `sw.js` `cacheVersion` changes → a
  returning browser fetches the new bundle. Add a `build.mjs` unit test asserting cacheVersion changes when
  book data changes.
- **Dedup:** = `build-release-cacheversion-hash-gap-frozen-manifests`.

### P3-IMMUTABLE-PAGES — In-place page-image revisions can never reach returning devices  🟠 high `[code-grounded]`
- **Surface:** build/web. **Files:** `web/src/sw.js:187`, `web/build.mjs:91-92,827-830` (`_headers`
  `max-age=31536000, immutable`), page WebPs served old-cache-first + cross-cache seeding.
- **Problem:** page images live at stable filenames (`page-NNN.webp`) marked immutable for a year and
  served cache-first from retained old caches. If a page image is re-rendered (fixed scan, different
  `ALVERNIA_PDF_RENDER_DPI`/`QUALITY`), the new bytes never reach a device that already cached the old
  ones — and different clients silently mix resolutions with no purge path.
- **Fix:** content-hash page-image filenames (`page-NNN.<hash>.webp`) or add a per-book content version to
  the path, so revised pages get new URLs; keep immutable caching (now safe). Coordinate with P3-CACHEVERSION
  and the manifest that maps song→page→image URL. **This is the correct long-term fix for the "song N shows
  the wrong/old scan" class** (relates to the song-370 fire-drill memory).
- **Dedup:** = `build-release-immutable-page-urls-vs-changed-bytes`.

### P3-SW-LIFECYCLE (batch, med) `[code-grounded]`
- `offline-pwa-partial-install-wedges-updates` (`sw.js:133`): a partially-failed install wedges update
  delivery until the NEXT deploy (precache never retried). Add install retry / don't `skipWaiting` on a
  partial install.
- `offline-pwa-page-cache-eviction-by-recency` (`sw.js:110`): `activate` evicts page caches by recency, not
  completeness — can delete the only *full* offline bundle while keeping a partial newer one. Evict by
  completeness/version, keep the most-complete bundle for the current cacheVersion.
- `offline-pwa-query-string-offline-navigation-fails` (`sw.js:222`): offline navigation with any query
  string (`?k=`, `?retry=`) bypasses the cached shell → browser error page. Normalize navigation requests to
  the shell regardless of query.
- `perf/offline-mid-mass-deploy-force-reload` (`app.js:2320`): a deploy during Mass force-reloads every
  online follower within ~60s. Add a "don't reload while actively following a live director" guard to the
  `SKIP_WAITING`→reload chain (defer the reload until the follower is idle / director goes stale).

### P3-lows (batch)
- `offline-pwa-global-cache-match-version-mix` (`sw.js:61`): shell lookups use global `caches.match`; a
  retained old static cache can serve mixed-version shells. Scope lookups to the current cacheVersion cache.
- `offline-pwa-precache-no-in-session-retry` (`app.js:694`): one transient failure aborts a book's precache
  for the whole session; floor precache completes with silent holes. Retry with backoff within the session.
- `offline-pwa-dead-offline-gate-ui` (`app.js:764`): the offline-download UI + bundle verification are dead
  code; no on-device offline-readiness signal exists. Either wire it up (real readiness) or delete it (P8) —
  don't leave a dead safety indicator.
- `offline-pwa-unawaited-sw-async` (`sw.js:130`): `clients.claim()` and page-image `cache.put` are
  fire-and-forget. `event.waitUntil` them.

---

## P4 — Build / release pipeline hardening

> The pipeline has the right lockstep *design* (one version source, web built before the native archive,
> prod pinned to `--branch main`) but **zero atomicity**: five tracked files mutated, PII swapped, and
> `Podfile.lock` overwritten with no crash safety, and several bypass paths that skip the invariants.

### B-RESTORE — `restore_codes` aborts the script under `set -e` when the private file is missing  🟠 high `[verified]`
- **File:** `scripts/release.sh:48,59` + `set -euo pipefail` (:16). `restore_codes()` is
  `[ -n "$RESTORE_CODES" ] && mv ...` — when the private file is missing, `RESTORE_CODES` is empty, the
  `&&` returns 1, and under `set -e` the bare call at :59 **aborts the script AFTER a successful archive
  but BEFORE the IPA copy (:60) and web deploy (:65)** → version bumped, native archived, web NOT deployed:
  lockstep silently broken. In the failure branches (:52,56) the same return-1 aborts before the diagnostic
  `tail -25` prints. **Fix:** make `restore_codes` always return 0 (`if [ -n … ]; then mv …; fi; return 0`);
  this is the same function A4 hardens with a `trap`.

### B-DEPLOYWEB — `npm run deploy:web` is a footgun outside the blessed pipeline  🟠 high `[verified]`
- **File:** `package.json:15`. `deploy:web` runs `wrangler pages deploy` WITHOUT `--branch main` (→ a
  **PREVIEW** deploy from any non-main checkout, easily mistaken for prod) and WITHOUT a version bump (ships
  a bundle whose badge/build number was never bumped — the by-hand path `release.sh` explicitly forbids,
  now one npm command away). **Fix:** either remove the script or make it `--branch main` + refuse to run on
  a non-main branch + require a clean bump; document that prod web ships only via `release.sh`.

### B-ALTSCRIPTS — Alt archive scripts bypass web rebuild, WebBundle sync, and code baking  🟠 high `[code-grounded]`
- **Files:** `scripts/testflight-upload.sh:23`, `scripts/testflight-upload-transporter.sh`,
  `scripts/submit-appstore.sh`. These archive without rebuilding web, without syncing `ios/WebBundle`, and
  without baking director codes → an IPA with a **stale web bundle** and **no standard-director entry**.
  Two divergent export plists (`scripts/export-options.plist` vs `ios/exportOptions.app-store.plist`) can
  drift. **Fix:** route all archive paths through one hardened function (the P-CI/P4 "release engine") that
  always rebuilds web → syncs WebBundle → bakes codes → archives; collapse to one export plist; or delete
  the redundant scripts.

### B-CACHEVERSION — (= P3-CACHEVERSION; the build-side owner of the fix.)

### B-FOOTPRINT-RED — (= P1-RED; the suite is red at HEAD.)

### P4-med (batch) `[code-grounded]`
- `build-release-bump-first-no-rollback` (`release.sh:22`): version bump is step 1 with no rollback; any
  later failure leaves 4 manifests bumped/dirty and a retry double-bumps. Bump LAST (just before deploy) or
  snapshot+restore on failure via the same `trap`.
- `build-release-hymns4-songindex-range-unchecked` (`build.mjs:689`): hymns-4 `songIndex` passes to
  `pages.json` with no page-range check (only the search index is validated). A page > rendered count sails
  past the end — the "unreachable song" class (song-370 fire-drill) for the OTHER book. Range-check both
  books' songIndex against rendered page count; fail the build.
- `build-release-check-book-consistency-coverage` (`check-book-consistency.mjs:30`): soft-skips (exit 0)
  without `pdfinfo`, guards only the standard book, and never runs in `release.sh`. Make it hard-fail
  without pdfinfo (or bundle a check that doesn't need it), guard both books, and call it from `release.sh`.
- `build-release-rm-rf-build-concurrent-tabs` (`release.sh:49`, `testflight-upload.sh:24`): unconditional
  `rm -rf build` at repo root; concurrent tabs clobber in-flight archives. Add a lockfile (same as A4's).
- `build-release-podfile-lock-copy` (`release.sh:36`): `cp ios/Pods/Manifest.lock ios/Podfile.lock` before
  every archive permanently neutralizes the CocoaPods consistency guard. See P-BUILDENV (§11) — retire the
  copy once `pod install` under pinned Ruby is proven; until then, document why it's there.

### P4-low (batch)
- `build-release-bump-silent-regex-skip` (`bump-build.mjs:42`): manifest syncs are silent-on-no-match regex
  edits + a dead `offlineWebBundle` sync block (the file no longer exists). Make each sync **fail loudly if
  its pattern is absent** (lockstep drift otherwise ships silently); delete the dead block.
- `build-release-dist-wipe-before-preflight` (`build.mjs:41`): wipes `web/dist` before checking required
  tools (`sips`/`pdftoppm`/`cwebp`) → a missing tool leaves no deployable bundle mid-fire-drill. Preflight
  all tools BEFORE wiping; stop hiding release progress behind `>/dev/null`.
- `build-release-preios-hooks-bump` (`package.json:8`): `preios`/`preios:mpad` bump the global build number
  on every local `npm run ios`, dirtying 4 tracked files per dev run. Gate the bump behind an env flag or
  move it into `release.sh` only.

---

## P5 — Native shell + Swift mesh (batch for a 2-device test day)

> Build 370 is well-hardened — most prior-audit deferred items LANDED (see below). The remaining native
> items split into **web-side/JS** (testable, ship on next build) and **Multipeer Swift** (device-gated).

**Prior-audit reconciliation (verified against build 370):**
- HIGH#1 (live takeover) — **FIXED.** `becomeDirector` drops the follower link first
  (`PdfReaderApp.tsx:420-429`) and no longer injects role "none".
- HIGH#3 (set-book→page two-script race) — **collapsed to one event** (`:583`, single atomic page
  sync-event). Residual web-relay interleave is P2-SWITCHBOOK-RACE.
- `render-failed -1 sentinel` — **primary fix landed** (`:689` gates the reset positively on
  `roleRef==='follower'`, excluding all broadcasters). Residual: a narrow promotion-window race (a
  follower that set `-1` then is promoted before a page event arrives) — MED, see below.

### P5-PATCH-NOOP — `patch-rn-webview.js` silently no-ops if react-native-webview changes → blank app shipped  🟠 high `[verified]`
- **File:** `scripts/patch-rn-webview.js:44-47`. If the target string isn't found (a webview version bump),
  the patch `console.warn`s and `process.exit(0)` — install "succeeds," archive proceeds, and file:// loads
  fail → **blank WebView on every iPad**, discovered only at Mass. **Fix:** `exit(1)` when the pattern is
  missing UNLESS the already-patched marker is present (idempotent); assert the installed
  react-native-webview version equals the `package.json` pin before patching. Same treatment for
  `patch-hermes-thread.js`. (= P-BUILDENV fail-hard patches, §11.)
- **Acceptance:** bump the webview version locally → `npm install` FAILS loudly instead of shipping blind.

### P5-ONERROR — Failed initial file:// load leaves a permanent blank WebView with no retry  🟡 med `[code-grounded]`
- **File:** `PdfReaderApp.tsx:989`. `onError` only breadcrumbs; a transient failed initial load bricks the
  app until manual relaunch. **Fix:** on initial-load error, retry the load (bounded backoff) and surface a
  retry affordance. Pair with `native-swift-onerror` and the WebView process-termination recovery
  (`onContentProcessDidTerminate` — which is also A3's trigger).

### P5-med (JS-side, testable, batch) `[code-grounded]`
- `native-swift-zombie-director-on-storage-failure` (`PdfReaderApp.tsx:442`): an AsyncStorage failure during
  `becomeDirector` leaves a zombie director (mesh advertising, web shows "código incorrecto", followers
  stranded). Treat storage failure as non-fatal to role assumption; don't advertise a role you couldn't persist.
- `native-swift-bridge-ready-unclamped-total` (`:553`): `bridge-ready` trusts `msg.page`/`msg.totalPages`
  unvalidated; a bogus `totalPages` poisons the clamp for later `page-changed`. Validate/clamp on adoption.
- `native-swift-page-changed-clamp-stale-total` (`:590`): clamps against OLD `totalPagesRef` before adopting
  the message's own `totalPages`. Adopt totalPages first, then clamp. (= P2-SWITCHBOOK-RACE native half.)
- `native-swift-render-failed-residual-race` (`:689`): the promotion-window race described above. Clear the
  `-1` sentinel on role change, or re-drive from storage on promotion.
- `native-swift-geo-overrides-chosen-book` (`:899`): the geo effect re-checks the director snapshot but not
  `storedBookRef` after its await → a book chosen mid-fetch can be overridden by geo. Re-check `storedBookRef`
  post-await.

### P5-DEVICE-GATED (Swift mesh — one 2-device day, §10) `[DEVICE-GATED]`
- `native-swift-peer-bundle-unauthenticated-exec` (`DirectorSyncModule.swift:717`): peer web-bundle push is
  **unauthenticated** → a malicious peer on the wifi can push arbitrary code into a follower's WebView
  (persistent). **Highest-severity native item.** Options: authenticate/verify the bundle (signature +
  version check), OR — strongly consider — **retire peer bundle push entirely** (P-MESH decision + P-OTA
  makes it redundant). Disabling `bundle_offer`/`bundle_request` (Swift `:699-1049`) behind a kill-switch is
  itself a Swift edit → schedule on the device day.
- `native-swift-stale-documents-bundle-masks-update` (`PdfReaderApp.tsx:711`): a peer-pushed
  `Documents/WebBundle` is never version-checked/cleaned → a TestFlight update silently runs OLD web code
  while the badge shows the NEW build. Version-stamp + prefer the newer of (baked bundle, pushed bundle);
  clean stale pushes on app update. (Ties to P-OTA.)
- `native-swift-relay-split-brain-no-tiebreak` — mesh half of P2-IDENTITY.
- `native-swift-24h-restore-remints-stale-director` (`:771`): the 24h auto-restore can re-mint a stale
  director the morning after Mass → split-brain + web flapping. Re-validate (recent activity / explicit
  confirm) before re-minting a *mesh* director on restore. (Careful: P2-TRANSMITTER-RESTORE wants to restore
  the *relay transmitter* role — restore publishing, gate mesh re-minting.)

### P5-low
- `web-reader-fullscreen-fab-noop-on-ios-pwa` (`app.js:2230`): the ⛶ fab shows on iOS home-screen PWAs where
  `toggleFullscreen` is a no-op. Hide it there.

---

## P6 — Security / privacy

> Calibrated to a parish app (threat = pranksters/curious users, not nation-states — but a prankster
> flipping pages during Mass IS the worst-case product failure). The security dimension got a full
> skeptic pass; all items below are `[skeptic-CONFIRMED]` unless noted.

- **A1, A2** (P0) are the two showstoppers (committed master code; no rate limiting).
- **P6-UNLOCK-ORACLE** `sync-worker/src/index.ts:731` 🟠 high: `/unlock` is an unthrottled yes/no oracle over
  phone-number-structured codes; the code comment's "isn't brute-forceable" is security-by-assertion (real
  codes cluster in the 830 area code, far below 10^10 entropy). **Fix:** rate-limit + lockout on `/unlock`
  (A2's limiter), and consider decoupling the "access" grant from the raw phone number (a per-device unlock
  token issued once, not the reusable code). 
- **P6-PRIVACY-POLICY** `docs/privacy-policy.html:22` 🟠 high: the App Store privacy policy claims zero data
  collection / no internet / Keychain storage, while the app transmits device ids, self-entered names,
  coarse geo, and iOS device names to a server. **Materially false → App Store review + legal risk.** Rewrite
  to reflect reality (fleet check-in, relay, geo-IP, `/log`); align `PrivacyInfo.xcprivacy`. Coordinate with
  P6-LOG (stop sending device names) so the policy can stay minimal.
- **P6-LOG** `sync-worker/src/index.ts:594` 🟠 high: `/log` is world-readable and un-capped; Swift posts
  device display names (`DirectorSyncModule.swift` dbgLog + `UIDevice.current.name`, which leaks
  owner-renamed devices even on iOS 16+). Anyone can harvest parishioner names and wipe mid-Mass
  diagnostics. **Fix:** authenticate `/log` reads (dashboard key), cap size + rate, and **stop sending
  device names** from Swift (hash or drop them). (Merges `native-swift-dbglog-device-name-pii`.)
- **P6-FLEET-KEY** `sync-worker/src/index.ts:654` 🟡 med: `FLEET_DASHBOARD_KEY` (gates choir phone numbers)
  is passed as `?k=` with Worker observability ON → the secret lands in CF logs/history/referrers. **Fix:**
  accept it via header/cookie, not the query string; rotate the key.
- **P6-FLEET-CHECKIN** `:632` 🟡 med: `/fleet/checkin` is open + unthrottled → an attacker evicts real
  check-ins and blinds the pre-Mass dashboard. Rate-limit + light auth (A2's limiter).
- **P6-STANDARD-GATE** `web/build.mjs:827` 🟡 med: the "private" Del Rio manual is served publicly and
  unauthenticated on signovivo.com — the geo/unlock gate is client-side cosmetics; the page WebPs are
  directly fetchable. **Decide the real posture:** if the manual must be private, gate the *asset* delivery
  behind an unlock token at the worker/edge (signed URLs); if "obscure but public" is acceptable, update the
  privacy/positioning docs to say so and stop calling it private. This is a product decision — surface it,
  don't silently pick.
- **P6-CODE-STORAGE** `PdfReaderApp.tsx:445` 🟢 low: the director access code (a real phone number) is
  persisted in plaintext AsyncStorage while `expo-secure-store` is installed and configured for exactly
  this. Move it to SecureStore/Keychain. (= `coherence-director-code-asyncstorage`.)
- **P6-HEALTH-GEO** `:565` 🟢 low: `/health` echoes the requester's geolocation with CORS `*` → any site a
  parish device visits can read its approximate location + Del-Rio membership. Restrict CORS on `/health` or
  drop the geo echo.

---

## P7 — Performance / polish (web; aging parish iPads)

> Steady-state perf is solid (115 DPI / q60 WebP → ~69 KB/page, 25 MB standard book, 3 MB hymns-4;
> cache-first; neighbor prefetch makes director-driven +1 turns instant once warm). The sharp edges are
> on the exact paths that matter during Mass.

- **P7-IMG-RETRY** `web/src/app.js:56` 🟠 high: the live `<img>` error-retry re-commits a captured OLD src
  with no stale-request guard → a delayed retry can overwrite the newly committed page and strand a follower
  on the wrong page. **Fix:** consult `state.pageLoadRequest`/`bookSwitchGeneration` in the retry closure;
  drop the retry if superseded. (= `web-reader-live-img-error-retry-bypasses-requestid-guard`.)
- **P7-TIMEOUT-COMMIT** `:1200` 🟠 high: the 3s `preloadImage` timeout **commits an unloaded src**, then
  unconditionally hides the loading indicator and updates the song title → the OLD page is shown under the
  NEW song's status with no affordance. **Fix:** on timeout, keep the loading indicator, don't update the
  title to the new song, and offer a retry; only commit on a real decode. (= `perf-timeout-commit-hides-loading-indicator`.)
- **P7-WAKELOCK** `:3665` 🟡 med: web followers have no Screen Wake Lock → a signovivo.com iPad/phone
  auto-locks minutes into Mass and goes dark/desynced. **Fix:** request a `navigator.wakeLock` while
  following a live director (re-acquire on `visibilitychange`); this is a big real-world reliability win for
  web followers who don't have the native keep-awake.
- **P7-PRECACHE-CONTENTION** `:642` 🟡 med: the 4-way ~25 MB precache (+ a second 4-way floor pool) starts
  ~4-5s after gate lift with no yield to live page loads → a cold boot during Mass has page turns fighting up
  to 8 concurrent downloads. **Fix:** pause/deprioritize precache while a live page load is in flight; lower
  concurrency on cold boot.
- **P7-TODAS-REBUILD** `:3808` 🟡 med: the full "todas" list (315 rows + ~27 headers, per-item innerHTML) is
  rebuilt into the laid-out off-screen drawer twice at boot, on every book switch, and every drawer open.
  **Fix:** build once + cache the DOM/fragment; rebuild only on book change; consider virtualization.
- **P7-DEPLOY-RELOAD** (= P3 mid-Mass reload guard.)
- **P7-low (batch):** offline poll storm battery (`:3576` — collapse the three overlapping retry loops into
  one backoff), SW update poll overhead over a 3h Mass (`:2284` — back off further / pause while following),
  page-cache double retention disk (`sw.js:110` — keep one complete bundle), book-switch stale prefetch sets
  (`:1254` — clear the number-keyed sets in `hydrateBookData`), search per-keystroke renormalization
  (`:1498` — precompute normalized index once + debounce), gate-caption dueling backstops (`:3878` — one
  reconcile owner for the caption).

### P7/web-reader correctness lows (batch) `[code-grounded]`
- `web-reader-theme-substring-shadows-text-search` (`:1593`): `searchByTheme` substring match preempts
  full-text search, so "santo" returns Espíritu-Santo-themed songs and makes Santo/Sanctus settings (incl.
  the new Canto 371 "Santo Español") **unreachable via search**. Make exact-title/number matches win over
  theme-substring, or require a theme prefix.
- `web-reader-initreader-pagesjson-no-ok-no-retry` (`:3716`): non-default `pages.json` boot fetch has no
  `response.ok` check and no retry → one transient failure leaves the session `totalPages=1`, follower
  clamped to page 1. Check `ok`, retry with backoff, fail visibly.
- `web-reader-loadsearchindex-stale-book-race` (`:1466`): no book/generation guard + no `ok` check → a book
  switch mid-fetch installs the OLD book's search index into the NEW book. Guard by generation; check `ok`.
- `web-reader-window-keydown-ignores-editable-targets` (`:3050`): arrow keys while typing in search/fleet
  fields turn the song and close the drawer; Escape double-fires. Bail when `event.target` is an input/editable.
- `web-reader-rendersongitem-innerhtml-unescaped` (`:2388`): OCR-derived `song.title`/`song.key` are built
  into DOM via innerHTML unescaped — an untrusted-data XSS boundary in every browse tab. Escape or use
  textContent. (Low because the data is repo-controlled today, but it's a latent injection point.)
- `web-reader-draft-cap-blocks-11-digit-code` / `coherence-numpad-code-cap` (`:1374`): the numpad caps drafts
  at 10 digits and strips leading zeros, while the worker contract says codes are 10-11 digits and the live
  secret holds an 11-digit code — the legacy code is unenterable on web and future leading-zero codes would
  corrupt. Once A1 removes the 11-digit code this is moot; otherwise raise the cap to 11 and keep codes as
  strings (never `Number()`).

---

## P8 — Dead code purge + docs

> Deleting dead code is pure risk-reduction and shrinks `app.js` ~20-25%. Do it in small reviewed commits
> AFTER P1 (so tests hold behavior), each verified by the build smoke + e2e, updating the footprint
> allowlist in the same commit. Verify "dead" with grep before each delete — some DOM stubs exist to keep
> old references valid, so delete JS+HTML+CSS together.

- **Purge** (from the maps' `stale_or_dead` inventories): the index-panel subsystem (`app.js:1645-2174` +
  its `bindReaderEvents` branches + `nc-sort-prefs` state + Easter computus), the never-visible offline-gate
  UI (`setOfflineGateState` visible-path, `LOADING_PHRASES`, `index.html:63-77`, matching CSS), dead state
  fields (`syncRole`, `relay.appliedPage`, `relay.manualClose`), dead files (`email.html`,
  `src/songNavigation.js*`, `src/pdfReaderUrl.js`, dead `offlineBooks.ts` exports + their JSON imports that
  bloat the RN bundle), dead CSS (`.director-sync-*`, `.index-*`, retired numpad styles), the dead
  `offlineWebBundle` block in `bump-build.mjs`.
- **README** (`README.md`) — wholesale stale/false: rewrite. It calls the app an offline PDF reader (it's a
  WKWebView shell over the web bundle since build 332), claims "no nearby sync / no local-network
  permissions" (contradicted by `Info.plist` + `DirectorSyncModule.swift`), links absolute paths into a
  DIFFERENT worktree, and its quick-start tells you to run the prod-mutating suite. Make it describe the real
  three-part architecture and point at `app-atlas.md`.
- **Stale docs:** `docs/web-follower-relay-plan.md` is marked "PROPOSED (not started)" but shipped; its §5
  security model ("writes require BOTH Bearer AND X-Director-Code") is false (worker accepts either); its
  line refs point at the deleted native reader. Mark it historical or update it.
- **Stale tests:** handled in P1-CI (delete dead-behavior pins, fix vacuous/lying test names).

---

## 9. Deduplication map (117 findings → plan items)

The 117 raw findings collapse to ~55 unique work items because dimensions corroborated each other. Key merges
(full IDs in `audit-findings-index.md`):

| Plan item | Merged raw findings |
|---|---|
| A1/A5 | secpriv-committed-live-transmitter-code, relay-e2e-tests-publish, build-release-e2e-suite-mutates, test-suite-relay-sync-mutates |
| A2 | secpriv-publish-flood-no-ratelimit, secpriv-fleet-checkin-open-abuse (partial) |
| A3 | relay-director-webview-reload-broadcasts-boot-page |
| A4 | build-release-director-codes-swap-no-trap, secpriv-release-pii-swap-window |
| P2-SEQ | relay-seq-guard-blocks-staleness-and-takeover, coherence-seq-regression-freezes-ws-followers |
| P2-IDENTITY | relay-no-transmitter-identity, native-swift-relay-split-brain-no-tiebreak, coherence-ignored-publish-response |
| P2-SWITCHBOOK-RACE | web-reader-snapshot-during-switchbook, relay-ws-message-interleaved-apply, coherence-ws-interleave, native-swift-page-changed-clamp |
| P3-CACHEVERSION | offline-pwa-cacheversion-misses-book-content, build-release-cacheversion-hash-gap |
| P3-IMMUTABLE-PAGES | offline-pwa-immutable-page-bytes-stale-forever, build-release-immutable-page-urls |
| WRONG-BOOK-FLASH (P2-WRONGSONG + reveal ordering) | web-reader-persisted-standard-boot-reveals-hymns4, offline-pwa-persisted-standard-reveal-flash, perf-persisted-standard-boot, coherence-wrong-book-flash |
| WHITE-GATE-BRICK (P3/P7 localStorage) | web-reader-unguarded-localstorage, offline-pwa-unguarded-localstorage |
| FLEET-WEBCACHED (P6/observability) | relay-fleet-webcached-inflated, offline-pwa-fleet-webcached-false-green, coherence-fleet-webcached |
| P6-LOG | secpriv-log-unauthenticated-pii-leak, native-swift-dbglog-device-name-pii |
| P7-IMG-RETRY | web-reader-live-img-error-retry, perf-live-img-retry-overwrites |
| B-FOOTPRINT-RED / P1-RED | build-release-footprint-test-red, test-suite-repo-footprint-red, coherence-footprint-test |

Everything not merged is a standalone item under its workstream above. **Nothing was dropped** — the full
text of every finding (including the ~35 lows and the ones summarized in batches) is in
`audit-findings-raw.md`, keyed by the same IDs.

---

## 10. Device-gated work — the 2-device test day

These CANNOT be proven in a simulator; batch them into ONE sanctioned session with two physical iPads on one
wifi (per `sync-handoff-known-issues.md`'s test plan, still valid):
- P2-IDENTITY (mesh split-brain half), P5-DEVICE-GATED (peer bundle exec, stale documents bundle, 24h
  re-mint), and any Swift edit (the peer-bundle kill-switch).
- **Test plan:** (1) live takeover A→B; (2) split-brain: start B while A live → only one director within
  ~1s, no flapping; (3) cross-book: A on standard 364, B follower on hymns-4 → B lands on standard 364; (4)
  director vanish → B taps ⟳ → recovers; (5) background/foreground both roles mid-session; (6) A3 repro:
  reload A's WebView mid-page → B must NOT jump to page 2.
- **Decision to make on that day (P-MESH, §11):** does the mesh ever save a Mass the relay couldn't? If
  P-OBS data shows the mesh never delivered a page the relay didn't, retire it (delete the Swift module +
  Bonjour keys + JS wrappers — huge complexity + the device-test tax disappears).

---

## 11. Strategic roadmap (the 12 proposals, mapped)

Full designs are in `audit-findings-raw.md` (strategy section) — summary + leverage order for a solo dev:

1. **P-CI / P-TEST (= P1)** — safe harness + CI. **Highest leverage.** Do first.
2. **P-OTA** — native shell checks signovivo.com for a newer web-bundle version and atomically swaps a
   locally-cached copy (keeping file:// offline-first + rollback on failed boot). **Kills most App Store
   releases** and closes the "web fix doesn't reach iPads" gap that shadows half this plan. Directly retires
   the risky peer-bundle-push (P5-DEVICE-GATED) and the stale-documents-bundle bug. Effort L; the biggest
   product win after P1. Design carefully (signature/version check, boot-failure rollback to baked bundle).
3. **P-RELEASE-ENGINE (= P4)** — idempotent, resumable release stages + a `scripts/doctor.mjs` preflight
   (tools, LANG, Ruby, node_modules-vs-lock). Absorbs A4, B-RESTORE, B-ALTSCRIPTS, P4-*.
4. **P-OBS** — lightweight client error/telemetry into the existing `/log` + fleet dashboard; director-visible
   follower health. Turns "is everyone on the right page?" from a guess into a signal, and feeds the P-MESH
   decision. Fix P6-LOG first so this doesn't leak PII.
5. **P-STAGING** — a preview Pages + second relay room/worker env, with a defined pre-Mass verification
   ritual (canary walk on the OLDEST fleet iPad; page-turn < ~300ms; A3 repro). Also the home for per-book /
   per-parish rooms (P2-SIÓN-ROOM, multi-parish scalability).
6. **P-CONST** — single source for constants shared across web/native/worker (Sión code, room, RELAY_BASE
   ×5 copies, freshness window), codegen'd at build. Kills the "hardcoded in 5 places" drift
   (`coherence-relay-base-five-hardcoded-copies`) and the two-hand-synced director-code stores
   (`coherence-director-codes-two-hand-synced`).
7. **P-BUILDENV** — fail-hard postinstall patches (P5-PATCH-NOOP), pinned toolchain (`.tool-versions`,
   Ruby 3.x for CocoaPods), `npm ci` reconciliation of the known node_modules drift, retire the
   Manifest.lock copy once real `pod install` works. Reject containerization (the risky leg is the macOS
   archive, which can't be containerized).
8. **P8-MODULARIZE** — dead-code purge now (P8); optional bundler-free concat-module split later (extend
   `build.mjs` to concat `web/src/lib/*.js`). Reject a framework/TS rewrite of the web app.
9. **P-MESH** — instrument (P-OBS), write decision criteria BEFORE looking at data, then keep/simplify/retire
   the Multipeer mesh on the device day.
10. **P-COMPAT** — minimal older-iPad floor: `--viewport-height` needs a `100vh` fallback before the `100dvh`
    override (currently NO fallback → layout collapse on Safari <15.4), guard the top-level localStorage
    reads (= WHITE-GATE-BRICK), add osVersion+model to fleet check-in to *measure* the floor. Reject a full
    a11y program (content is scanned hymnal images).

---

## 12. Appendix — what's NOT covered / open questions for Miguel

The completeness-critic pass didn't run (usage limits), so flag these surfaces for a follow-up look — none
blocked the plan, but decide before calling the app "done":
- **`android/`** — the audit focused on iOS (the parish device). Is Android shipped/relevant? If dead, prune it.
- **App Store review risk** — geo-gated + "private" content (P6-STANDARD-GATE) and the false privacy policy
  (P6-PRIVACY-POLICY) are the two review/legal exposures. Resolve before the next submission.
- **Multi-parish scalability** — one hardcoded room `alvernia-main`. If SignoVivo ever serves a second
  parish, rooms/tenancy is net-new work (P-STAGING/P2-SIÓN-ROOM lay groundwork).
- **i18n** — the app is Spanish with hardcoded strings; fine for now, note if a second language is ever wanted.
- **Data-loss UX** — a localStorage wipe (iOS eviction) silently resets unlock/geo/prefs; the offline-readiness
  signal is dead code (P3). Decide whether returning users need a "re-verifying…" state.
- **Product decision (P6-STANDARD-GATE):** is the Del Rio manual actually private, or obscure-but-public? This
  changes whether you build real asset-gating or just fix the docs. **This is Miguel's call — surface it.**

---

_End of plan. Full evidence/fix text for every item: `audit-findings-raw.md`. Architecture: `app-atlas.md`.
Contracts: `app-contracts.md`. Prior audit: `sync-handoff-known-issues.md`._
