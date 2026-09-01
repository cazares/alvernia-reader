> ⚠️ **CORRECTION BANNER (2026-07-09).** This map was written at HEAD 16244b25 / build 377. The branch has since been fast-forwarded to build 381 (d5075091). Landed since this map: **#269** capped the "Buscando director…" spinner so it never looks stuck; **#270 REMOVED the "¿Quién usa este iPad?" fleet self-ID modal entirely** (fleet check-in itself remains); **#271** simplified the sync spinner and renamed the songbook PDF (assets/alvernia_manual_2.pdf → assets/signo_vivo_371.pdf), touching web/src/app.js (−112 lines area), index.html, styles.css. Where this map contradicts current source, CURRENT SOURCE WINS — do not report the removed modal or old spinner behavior as findings.

# PRIOR-ART MAP — SignoVivo known findings + planned work (dedupe reference)

> Generated 2026-07-07 at HEAD `16244b25` (native build **377**, version 1.0.4), read-only.
> **Purpose:** every finding/issue already documented anywhere in this repo, with CURRENT status, so
> later audit lenses do NOT re-report known issues. Also: everything planned-but-unbuilt.
>
> **Source docs distilled:** docs/audit-reconciliation-374.md, docs/app-hardening-plan.md,
> docs/sync-reliability-audit-2026-07.md, docs/sync-handoff-known-issues.md,
> docs/audit-findings-index.md (117 originals), docs/audit-findings-raw.md, docs/implementation-log.md,
> docs/major-update-2026-07.md, docs/pre-mass-checklist.md, docs/green-day-deploy-runbook.md,
> plus `git log` through #268.
>
> ⚠️ **Line-number caveat for later agents:** the reconciliation doc's anchors are at HEAD `39b60313`
> (build 374, app.js=3336 lines). At current HEAD the files GREW again (app.js=3638,
> sync-worker/src/index.ts=828, PdfReaderApp.tsx=1123, DirectorSyncModule.swift=1913) because Waves
> 1–3 + M2 landed on top. Re-grep every anchor; do not trust ANY doc's line numbers, including the
> "reconciled" ones. docs/app-atlas.md + docs/app-contracts.md describe build 370 and are the
> stalest — atlas carries a correction banner but its body still describes two-book internals.

---

## 1. Architecture snapshot (current, verified at HEAD 16244b25)

- **One web bundle, three delivery surfaces.** `web/src/app.js` (3638 lines) + `web/src/index.html`
  + `web/src/sw.js` (237) + `web/src/lib/*.js` built by `web/build.mjs` (715) → served at
  signovivo.com (Cloudflare Pages, project `alvernia-reader`, **prod branch = main**; non-main deploy
  = preview only) AND baked into the native iOS shell as a `file://` bundle (`ios/WebBundle`,
  synced at archive time by `scripts/release.sh`). Native shell: `PdfReaderApp.tsx` (1123 lines, repo
  root) hosting a WKWebView; Swift Multipeer mesh in `ios/SignoVivo/DirectorSyncModule.swift` (1913).
- **Single public book.** `const BOOK_ID = "standard"` (web/src/app.js:185), 371 pages
  (`STANDARD_TOTAL_PAGES`). The whole two-book / Sión / IP-geo / `/unlock` / auto-director system was
  DELETED in builds 371–374 (2026-07-02) after a real 2026-07-01 Mass outage. `bookId`/`mode` remain
  on the wire as vestigial compat fields; `/unlock` is a no-op `{ok:true}` stub; `#geo-gate` is just
  a loader div. Boot is ALWAYS follower; director requires code entry + a confirm Alert.
- **Relay:** Cloudflare Worker `sync-worker/src/index.ts` (828 lines), prod
  `signovivo-sync.4j4982y8jp.workers.dev`, one Durable Object per room. Rooms: `alvernia-main`
  (prod/Mass), `alvernia-staging` (canary via `?env=staging`), `alvernia-practice` (planned).
  Resolver: `web/src/lib/svRelayRoom.js`, consumed triple-guarded at app.js:2853-2867. Native
  publisher: `src/directorRelaySync.js` (129 lines; `RELAY_ROOM="alvernia-main"` at :13 — native
  staging entry is deferred to M7). `PROTOCOL_VERSION=1` (index.ts:29). Snapshot
  `{v,page,totalPages,mode,bookId,seq,ts}` + additive `now` (server epoch s) on `/state`.
- **Mesh (church-critical, offline):** iPads have NO wifi in church; page turns go over Multipeer.
  Swift is UNCOMPILEABLE in this environment — all recent Swift changes (PR #266) are git-only and
  compile-unverified. The relay serves web followers + Miguel's tethered devices only.
- **Fleet/diagnostics:** `/fleet/checkin` + `/fleet-dashboard?k=SECRET` (or `X-Fleet-Key`) +
  `/log` (POST open + capped; GET/DELETE gated since #252). Crash telemetry `reportCrash()` → /log →
  dashboard "Fallos recientes" panel.
- **Auth:** `/publish` requires Bearer `RELAY_DIRECTOR_TOKEN` or `X-Director-Code` ∈
  `TRANSMITTER_CODES` env secret (fail-closed; **legacy master code 12345678840 rotated OUT
  2026-07-05** — now 4 real codes only). Native bakes codes from gitignored
  `director-codes.private.json` at release time (A4 trap now guards the swap). Rate-limited since A2:
  /publish 15/2s, /fleet/checkin 10/1s, /log 20/3s per-IP token bucket → 429, fail-open.

## 2. User-visible surfaces

- **Web follower (signovivo.com / home-screen PWA):** page image + relay pill (en vivo / stale),
  go-live bar, browse drawer (todas/recientes/tono/buscar), numpad song jump, ⟳ resync, wake-lock
  while following (#241), boot-guard "Reintentar" card on crash (#238), `?selftest` GREEN/RED card
  (#237), `?env=staging` canary channel (#234).
- **Native iPad (roles driven via `html[data-role]`):** follower = ⟳+♪; director = ♪+⌕. Director
  entry: numpad code → confirm Alert (takeover-aware, super-admin-labeled). Boot resume-prompt after
  crash/kill reads `lastSyncRole` (PdfReaderApp.tsx:869 — NEW-DIR-1 + H3, shipped #243/#267,
  device-unverified). WebView crash floor: bridge-ready watchdog → ≤2 remounts → native "Reintentar"
  view (#261).
- **Operator tooling:** fleet dashboard, pre-Mass checklist (docs/pre-mass-checklist.md), green-day
  deploy runbook, `STAGING=1 bash scripts/release.sh`, `scripts/rollback-web.sh`,
  `scripts/smoke-boot.mjs`, CI (`.github/workflows/ci.yml`, macOS runner, safe e2e subset only).

## 3. Flows (live semantics)

- **Publish:** native director → mesh broadcast + relay POST `/publish` (seq = wall-clock-ms
  monotonic via `Math.max(seq+1, Date.now())`, directorRelaySync.js:56-59). Worker accepts
  strictly-greater seq while fresh; accepts any seq after 90s staleness (takeover self-heal); seq=0
  only honored as a stale reset (A2). `{ok:true,ignored:true}` on regressed seq is STILL not consumed
  by the native transmitter (open, = P2-IDENTITY/M4).
- **Follow (web):** WS `/ws` + `/state` polling; decision logic extracted to pure
  `web/src/lib/svSyncDecision.js` (freshness-BEFORE-seq since #248 — fixes the Wednesday
  green-pill-on-dead-director freeze); clock-skew offset from `/state.now`; poll runs through the
  CONNECTING window; demote resets `lastSeq=-1` (#263 F4); 10s health watchdog + 12s zombie-close +
  heartbeat re-home of drifted followers (~4s) (#263 F1-F3).
- **Follow (native):** mesh `page` events force `book='standard'`; `startRelayFollow` early-returns
  in native file-mode (applyRelaySnapshot never runs on iPads — relay fixes don't regress natives).
- **Reload/restart:** WebView content-process reap → native re-asserts its own page for directors
  (A3 fix, #243); reloaded FOLLOWER pulls fresh snapshot on bridge-ready (#264 H1); native app
  restart → resume prompt (never silent auto-director; the code is deliberately never persisted).
- **Deploy:** `git merge ≠ deploy`. Worker = `wrangler deploy` (instant, everyone). Web =
  release.sh Pages deploy (phones instantly; **iPads only on next TestFlight archive** — the baked
  bundle gap that motivates P-OTA). Native = full release.sh + Transporter. Never deploy near Mass
  (Sun/Thu); builds roll out Wed/Sat practice only, canary-walked on the oldest iPad first.

## 4. Contracts / invariants (enforced or promised)

1. **Additive-only, forward-renderable wire.** Never remove/retype `page/totalPages/seq`; never bump
   relay `v` or mesh `protocolVersion` (=1); new fields optional with defaults. Mixed-fleet safety
   rests entirely on this (major-update §5).
2. **90s freshness window** (`RELAY_LIVE_MAX_AGE_S`), shared worker/web — now computed
   server-vs-server (publish ts vs `/state.now`), client clock factored out.
3. **seq=0 ⇒ no director live**; monotonic per session; regressed seq accepted only after staleness.
4. **No credential at rest:** director code never persisted; only a `lastSyncRole` breadcrumb.
5. **Always-ask director promotion:** valid code → confirm Alert; boot never silently promotes.
6. **Room isolation = DO instance isolation** (staging can't touch Mass).
7. **NEVER run `npm run test:e2e` / the e2e glob** — `e2e/relay-sync.test.mjs` is env-gated now
   (#239) but the rule stands; suite also historically red at HEAD.
8. **Release lockstep:** version.json + app.json + Info.plist + pbxproj bump together; signovivo.com
   == native == v<N> after a full release.

---

## (A) KNOWN-FINDINGS — every already-documented finding, with current status

Status legend: **FIXED-#PR** (merged; build/deploy noted) · **FIXED-DG** (fixed in git, DEVICE-GATED
verify still owed — Swift uncompiled here / needs the 2-device day) · **OPEN** (documented, unbuilt)
· **PARTIAL** · **MOOT-374** (target code deleted in the single-book refactor — do NOT re-report).
Original per-finding evidence: docs/audit-findings-raw.md (build-370 lines) + full re-triage text in
docs/audit-reconciliation-374.md §4. Dimension-tagged IDs are the canonical dedupe keys.

### A.1 Original 117-finding audit (build 370, reconciled to 374, statuses updated to HEAD 16244b25)

**relay-protocol (13)**
| ID | One-liner | Status |
|---|---|---|
| relay-director-webview-reload-broadcasts-boot-page [C] | Director WebView reload adopts web boot page 2 and re-broadcasts to whole congregation | **FIXED-DG** = A3, PR #243 (in build 375/377); verify on 2-device day |
| relay-seq-guard-blocks-staleness-and-takeover-on-ws-path [H] | seq guard ran before freshness → dead director never demoted; regressed-seq takeover ignored | **FIXED-#248** (P2-SEQ, svSyncDecision.js) + #263 F4 (demote resets lastSeq); live in 377 + deployed worker |
| relay-e2e-tests-publish-to-production-room [H] | test:e2e flips live followers' pages in alvernia-main | **FIXED-#239** (env-gated, refuses alvernia-main, hardcoded code removed) |
| relay-sion-code-cross-book-publish-into-live-room [H] | Public Sión code publishes hymns-4 into live room | **MOOT-374** |
| relay-no-transmitter-identity-two-publishers-ping-pong [H] | No txId; two authorized publishers ping-pong all web followers, no arbitration/warning | **OPEN** — planned M4 (P2-IDENTITY: transmitterId + worker tiebreak + consume `ignored`) |
| relay-ws-message-interleaved-apply-during-book-switch [M] | WS apply interleaves a cross-book switch | **MOOT-374** |
| relay-unauthorized-follower-renders-wrong-song-live [M] | Wrong hymns-4 song under green pill | **MOOT-374** |
| relay-polling-stopped-before-ws-opens-blind-windows [M] | connectRelay killed /state polling before WS open → ~6s blind windows | **FIXED-#248** (P2-POLL-GAP) |
| relay-follower-clock-skew-defeats-freshness [M] | >90s-fast clock ⇒ never sees director live | **FIXED-#248** + worker `/state now` deployed 2026-07-05 |
| relay-fleet-webcached-inflated-across-books [L] | Cross-book cache count fakes "Listo" | **MOOT-374**; residual nit OPEN: count still not version-scoped (mid-upgrade over-report, app.js countCachedPageImages max-across-versions) |
| relay-transmitter-only-role-lost-on-relaunch [L] | Relaunched transmitter silently stops publishing | **FIXED-DG** via H3 PR #267 (persist transmitter-director breadcrumb → re-enter prompt) |
| relay-unlock-swallows-failed-switchbook [L] | | **MOOT-374** |
| relay-browsing-user-book-switched-mid-browse [L] | | **MOOT-374** |

**web-reader (14)**
| ID | One-liner | Status |
|---|---|---|
| web-reader-snapshot-during-switchbook-renders-old-totalpages [H] | | **MOOT-374** |
| web-reader-unguarded-localstorage-kills-module-white-gate-forever [H] | Top-level localStorage reads throw in cookie-blocked browsers → permanent white screen (likely Wednesday cause) | **FIXED-#238** (M2-A: guards + first-code bootGuard + Reintentar card) |
| web-reader-persisted-standard-boot-reveals-hardcoded-hymns4-img [M] | | **MOOT-374** |
| web-reader-live-img-error-retry-bypasses-requestid-guard [M] | img error-retry re-commits captured old src, can revert visible page after a turn (dup: perf-live-img-retry) | **OPEN** — P7-IMG-RETRY |
| web-reader-theme-substring-shadows-text-search-santo [M] | searchByTheme substring preempts full-text: "santo" hides all Santo/Sanctus settings incl. Canto 371 | **OPEN** — P7 batch (app.js searchByTheme + handleSearchInput early-return) |
| web-reader-browse-result-click-skips-relay-browsing-mode [M] | Browse/search tap never sets relay.browsing → yanked back on next push (numpad jump does set it) | **OPEN** — note #263 F1 heartbeat-re-home changed drift semantics; re-verify interaction before reporting |
| web-reader-initreader-pagesjson-fetch-no-ok-no-retry [M] | Boot manifest fetch throw skipped startRelayFollow etc. | **FIXED-#238** (resilient boot fetch degrades to known count) |
| web-reader-loadsearchindex-stale-book-race-and-no-ok-check [L] | race MOOT; response.ok half | **PARTIAL/likely-FIXED** — M2 Slice C audit (#250) says inline+network search-index paths now guarded; re-verify the `response.ok` specifically |
| web-reader-window-keydown-ignores-editable-targets [L] | Arrows while typing turn the song + slam drawer; Escape double-fires | **OPEN** |
| web-reader-hydratebookdata-keeps-numeric-prefetch-sets [L] | | **MOOT-374** |
| web-reader-rendersongitem-innerhtml-unescaped-ocr-titles [L] | innerHTML with unescaped OCR titles (latent, current data clean) | **OPEN** |
| web-reader-draft-cap-blocks-legacy-11-digit-code [L] | 10-digit numpad cap + Number() leading-zero strip vs 11-digit code | **MOSTLY MOOT** — 11-digit code rotated out (A1); leading-zero corruption still latent |
| web-reader-gate-caption-pingpong-and-false-offline [L] | | **MOOT-374** |
| web-reader-fullscreen-fab-noop-on-ios-pwa [L] | ⛶ fab shown on iOS home-screen PWA where toggle is a no-op (exact parish fleet config) | **OPEN** |

**native-swift (12)**
| ID | One-liner | Status |
|---|---|---|
| native-swift-peer-bundle-unauthenticated-exec [H] | Peer web-bundle push unauthenticated → persistent arbitrary code in follower WebView. Highest-severity native item | **OPEN** — device-gated; M7 design = sha256+signature+super-admin arm; P-MESH may retire push entirely; related held M-F3 |
| native-swift-relay-split-brain-no-tiebreak [H] | Relay half of dual-director split-brain (mesh half fixed in 345) | **OPEN** — = P2-IDENTITY / M4 |
| native-swift-stale-documents-bundle-masks-update [H] | Peer-pushed Documents/WebBundle preferred by existence, never version-checked → TestFlight update runs OLD web code under NEW badge | **OPEN** — device-gated; retired by P-OTA design |
| native-swift-webview-patch-silent-noop [H] | patch-rn-webview.js exit(0) on pattern miss → file:// blank app shipped | **OPEN** — P5-PATCH-NOOP |
| native-swift-render-failed-sentinel-promote [H] | -1 sentinel broadcastable on promotion → congregation to page 1 | **FIXED-DG** #264 (H4: broadcastPage floors it) |
| native-swift-dbglog-device-name-pii [H] | Swift dbgLog posts UIDevice.name to /log | **PARTIAL** — /log reads gated + POST capped (#252, deployed); Swift stop-sending-names half **OPEN** device-gated (DirectorSyncModule.swift:~177) |
| native-swift-onerror-no-recovery [M] | Failed initial file:// load → permanent blank WebView | **LARGELY FIXED-DG** #261 (Slice B watchdog → remounts → Reintentar view); verify covers onError path too |
| native-swift-page-changed-clamp-stale-total [M→L] | Clamps against OLD totalPagesRef before adopting msg total | **OPEN** — M3 bridge coerce-and-clamp kills it at the boundary |
| native-swift-zombie-director-on-storage-failure [M] | AsyncStorage failure mid-becomeDirector → zombie director | **OPEN** — P5-med |
| native-swift-bridge-ready-unclamped-total [M] | bridge-ready trusts msg.page/totalPages unvalidated | **OPEN** — M3 bridge |
| native-swift-24h-restore-remints-stale-director [M] | | **MOOT-374** |
| native-swift-geo-overrides-chosen-book [L] | | **MOOT-374** |

**offline-pwa (13)**
| ID | One-liner | Status |
|---|---|---|
| offline-pwa-cacheversion-misses-book-content [H] | cacheVersion hashes only 5 shell files → book-data-only deploy never busts caches (dup: build-release-cacheversion) | **OPEN** — P3-CACHEVERSION (hash whole dist/ tree) |
| offline-pwa-immutable-page-bytes-stale-forever [H] | Stable page-NNN.webp names + 1-yr immutable + old-cache-first → revised scans never reach returning devices | **OPEN** — P3-IMMUTABLE-PAGES; long-term subsumed by M5 content-addressed R2 URLs |
| offline-pwa-fleet-webcached-false-green [H] | Sticky OFFLINE_READY_KEY + unscoped page count → dashboard false "Listo" | **PARTIAL** — cross-book half MOOT; sticky-flag + version-mix residual **OPEN** |
| offline-pwa-partial-install-wedges-updates [M] | Partial SW install wedges update delivery until NEXT deploy | **OPEN** |
| offline-pwa-page-cache-eviction-by-recency [M] | activate evicts by recency, can delete the only FULL offline bundle | **OPEN** |
| offline-pwa-query-string-offline-navigation-fails [M] | Offline nav with any ?query bypasses cached shell → browser error page | **OPEN** |
| offline-pwa-unguarded-localstorage-kills-boot [M] | dup of web-reader white-gate | **FIXED-#238** |
| offline-pwa-persisted-standard-reveal-flash [M] | | **MOOT-374** |
| offline-pwa-mid-mass-deploy-force-reload [M] | Deploy during Mass force-reloads every online follower ≤60s (dup: perf-mid-mass) | **OPEN** — planned "update-ready chip" (major-update Ask 7) |
| offline-pwa-precache-no-in-session-retry [L] | One transient failure aborts precache for the session | **OPEN** (floor-precache half MOOT) |
| offline-pwa-global-cache-match-version-mix [L] | Global caches.match can serve mixed-version shells | **OPEN** |
| offline-pwa-dead-offline-gate-ui [L] | Offline-download UI + verification is dead code | **OPEN** — P8 |
| offline-pwa-unawaited-sw-async [L] | clients.claim() + page cache.put fire-and-forget | **OPEN** |

**security-privacy (13, incl. 2 skeptic adds)**
| ID | One-liner | Status |
|---|---|---|
| secpriv-committed-live-transmitter-code [C] | 12345678840 committed + live | **FIXED** — repo half #239; secret ROTATED 2026-07-05 (real→200, legacy→401 verified) |
| secpriv-publish-flood-no-ratelimit [C] | No throttle + seq=0 bypass + one room | **FIXED-#246** (A2), deployed 2026-07-05 worker b2f67748, proven vs local wrangler + live |
| secpriv-unlock-brute-force-oracle [H] | | **MOOT-374** (/unlock is a stub) |
| secpriv-privacy-policy-materially-false [H] | Policy claims zero collection/no internet/Keychain while app sends device ids, names, telemetry | **OPEN** — P6-PRIVACY-POLICY (drop geo clause; verify no residual request.cf read = Q4) |
| secpriv-log-unauthenticated-pii-leak [H] | /log world-readable+wipeable; Swift posts device names | **PARTIAL** — GET/DELETE gated + POST 64KB cap #252 (deployed); Swift device-name half OPEN (device-gated) |
| secpriv-release-pii-swap-window [H] | release.sh PII swap uncrash-safe | **FIXED-#239** (A4 trap EXIT/INT/TERM, proven in temp-dir sim) |
| secpriv-fleet-key-in-url-logged [M] | FLEET_DASHBOARD_KEY via ?k= lands in CF logs/history | **OPEN** — ?k= still accepted (index.ts:627,676); X-Fleet-Key added but query path remains, no rotation recorded |
| secpriv-fleet-checkin-open-abuse [M] | Open/unthrottled checkin evicts real check-ins | **PARTIAL** — A2 rate-limits 10/1s; still unauthenticated |
| secpriv-standard-manual-not-actually-gated [M] | | **MOOT-374** (book deliberately public now) |
| secpriv-director-code-plaintext-asyncstorage [L] | | **MOOT-374** (code never persisted) |
| secpriv-health-geo-oracle [L] | | **MOOT-374** |
| secpriv-skeptic-new-1-cross-book-flap | | **MOOT-374** |
| secpriv-skeptic-new-2-e2e-prod-room-footgun [H] | | **FIXED-#239** |

**build-release (16)**
| ID | One-liner | Status |
|---|---|---|
| build-release-director-codes-swap-no-trap [C] | | **FIXED-#239** (=A4) |
| build-release-e2e-suite-mutates-prod-relay [C] | | **FIXED-#239** (=A5) |
| build-release-restore-codes-set-e-abort [H] | restore_codes returns 1 under set -e → dies post-archive pre-deploy | **FIXED-#239** (restore always returns 0) |
| build-release-cacheversion-hash-gap-frozen-manifests [H] | dup of P3-CACHEVERSION | **OPEN** |
| build-release-deploy-web-npm-script-trap [H] | `npm run deploy:web` = unbumped PREVIEW-or-prod deploy, no --branch main | **OPEN** — explicitly deferred at M0 to P4 (B-DEPLOYWEB) |
| build-release-alt-archive-scripts-bypass-invariants [H] | testflight-upload*.sh / submit-appstore.sh skip web rebuild + WebBundle sync + code baking | **OPEN** — re-check: #258 added an ASC-API-key auto-upload path; confirm whether alt scripts were consolidated |
| build-release-bump-first-no-rollback [M] | Bump is step 1, no rollback; retry double-bumps | **OPEN** |
| build-release-immutable-page-urls-vs-changed-bytes [M] | dup of P3-IMMUTABLE-PAGES | **OPEN** |
| build-release-hymns4-songindex-range-unchecked [M] | songIndex→pages.json no range check | **PARTIAL** — hymns-4 half MOOT; smoke-boot (#233) adds triple page-count consistency (inline==rendered==manifest); per-song range check still open |
| build-release-footprint-test-red-at-head [M] | Suite red masks regressions | **FIXED-#233** (P1-RED) + #260 (offline-books test repoint) |
| build-release-check-book-consistency-soft-skip-and-coverage [M] | Soft-skips without pdfinfo, never in release.sh | **PARTIAL** — guard repointed to canonical song index (87ee5a39/#260); soft-skip + not-in-release.sh unresolved (re-verify) |
| build-release-rm-rf-build-concurrent-tabs [M] | No lockfile; concurrent tabs clobber archives | **OPEN** |
| build-release-podfile-lock-copy-defeats-guard [M] | Manifest.lock→Podfile.lock cp neutralizes pod guard | **OPEN** — known env debt (Ruby 4.0.1 breaks pod install; see memory) |
| build-release-bump-silent-regex-skip-dead-block [L] | Silent-on-no-match regex syncs + dead offlineWebBundle block | **OPEN** |
| build-release-dist-wipe-before-tool-preflight [L] | dist wiped before tool checks | **OPEN** |
| build-release-preios-hooks-bump-on-every-local-run [L] | preios bumps build on every local run | **OPEN** |

**test-suite (13)**
| ID | One-liner | Status |
|---|---|---|
| test-suite-relay-sync-mutates-production-room [C] | | **FIXED-#239** |
| test-suite-repo-footprint-red-at-head [H] | | **FIXED-#233/#260** |
| test-suite-sync-worker-zero-tests-security-boundaries [H] | Worker had zero tests | **PARTIAL** — `sync-worker/test/a2.test.mjs` + run-a2.sh harness (10/10 vs wrangler dev) covers rate-limit/seq0/now/log-gate; no full vitest boundary suite (publish-auth/CORS/fleet-key) |
| test-suite-appjs-sync-core-zero-tests [H] | app.js sync core untested | **PARTIAL** — svSyncDecision.js extracted + e2e/svSyncDecision.test.mjs 19/19; svRelayRoom/svSelftest unit-tested; bulk of app.js still uncovered (P1-APPJS-UNIT open) |
| test-suite-eas-config-banned-pathway-stale-assets [M] | Enforces banned EAS path; real app.json assets unguarded | **OPEN** (excluded from CI) |
| test-suite-vacuous-staleness-90s-contract-unpinned [M] | `includes("90")` can't fail | **PARTIAL** — decision lib tests pin the logic; the vacuous assertion itself likely still present |
| test-suite-build-manifest-generation-untested [M] | build.mjs untested (song-370 class) | **PARTIAL** — smoke-boot triple-consistency guard (#233); no unit tests |
| test-suite-sw-cache-lifecycle-untested [M] | sw.js zero tests | **OPEN** |
| test-suite-nearby-sync-regex-pins-classification [L] | 29/29 source-regex pins | **OPEN** |
| test-suite-permission-flow-dead-takeover-pin [L] | Pins dismantled takeover plumbing | **OPEN** (P1 delete planned) |
| test-suite-song55-test-name-lies-about-clamping [L] | Greps literal "[55, 55]" | **OPEN** |
| test-suite-bump-build-lockstep-untested-directly [L] | | **OPEN** |
| test-suite-readme-instructs-prod-mutation-and-lies [L] | | **FIXED-#240** (P8 README rewrite) |

**perf (13)**
| ID | One-liner | Status |
|---|---|---|
| perf-live-img-retry-overwrites-new-page [H] | dup of web-reader img-retry | **OPEN** — P7-IMG-RETRY |
| perf-timeout-commit-hides-loading-indicator [H] | 3s preload timeout commits unloaded src + retitles → old page under new song title | **OPEN** — P7-TIMEOUT-COMMIT |
| perf-boot-precache-contention-with-live-turns [M] | Precache fights live page loads (single pool now) | **OPEN** (floor-pool half MOOT) |
| perf-persisted-standard-boot-wrong-book-reveal [M] | | **MOOT-374** |
| perf-hidden-drawer-todas-rebuilds [M] | 315-row innerHTML list rebuilt 2× boot + every drawer open | **OPEN** |
| perf-mid-mass-deploy-fleet-reload [M] | dup of offline-pwa mid-Mass reload | **OPEN** |
| perf-no-screen-wake-lock-web-followers [M] | | **FIXED-#241** (P7 wake-lock while following, re-acquire on visibilitychange) |
| perf-offline-poll-storm-battery [L] | Overlapping retry loops hammer a dead network | **PARTIAL** — 5s geo loop deleted (374); remaining loop collapse open |
| perf-sw-update-poll-mass-overhead [L] | 60s SW polls all Mass | **OPEN** |
| perf-page-cache-double-retention-disk [L] | Two retained versions ≈ disk pressure (halved by single book) | **OPEN** |
| perf-book-switch-stale-prefetch-sets [L] | | **MOOT-374** |
| perf-search-per-keystroke-renormalization [L] | Full index NFD+regex per keystroke, undebounced | **OPEN** |
| perf-gate-caption-flicker-dueling-backstops [L] | | **MOOT-374** |

**coherence (12)**
| ID | One-liner | Status |
|---|---|---|
| coherence-seq-regression-freezes-ws-followers [H] | | **FIXED-#248/#263** (= P2-SEQ + F4) |
| coherence-ignored-publish-response-never-consumed [H] | Native transmitter never reads `{ignored:true}` → handoff director broadcasts into the void | **OPEN** — M4 / P2-IDENTITY |
| coherence-fleet-webcached-inflated-cross-book [M] | | **PARTIAL** (cross-book MOOT; version-mix residual open) |
| coherence-footprint-test-red-and-deploy-web-preview-trap [M] | | **PARTIAL** — red half FIXED-#233; deploy:web trap OPEN (B-DEPLOYWEB) |
| coherence-director-codes-two-hand-synced-stores [M] | Native baked JSON vs worker secret, no consistency check; non-release builds ship EMPTY set | **OPEN** |
| coherence-wrong-book-flash-on-standard-boot [M] | | **MOOT-374** |
| coherence-ws-interleave-stale-totalpages-clamp [M] | | **MOOT-374** |
| coherence-numpad-code-cap-and-leading-zero-corruption [L] | | **MOSTLY MOOT** post-A1 (see web-reader-draft-cap) |
| coherence-relay-base-five-hardcoded-copies [L] | Relay origin hardcoded ×5, one override mechanism | **OPEN** — P-CONST |
| coherence-page-changed-clamps-before-totalpages-update [L] | | **OPEN** (dup native-swift clamp) |
| coherence-director-code-asyncstorage-not-securestore [L] | | **MOOT-374** |
| coherence-vacuous-90s-staleness-test [L] | | **PARTIAL** (see test-suite row) |

### A.2 The 14 NEW findings introduced by the 374 refactor (reconciliation §5; full text there)

| ID | Sev | Status |
|---|---|---|
| new-director-role-webview-reload-broadcasts-boot-page | CRIT | **FIXED-DG** #243 (= A3 surviving half) |
| new-director-no-auto-restore-silent-demotion-outage (NEW-DIR-1) | HIGH | **FIXED-DG** #243 — Miguel chose boot resume-prompt (a); lastSyncRole now READ back at PdfReaderApp.tsx:869 |
| new-director-code-gen-bump-before-confirm-strands-follower (NEW-DIR-2) | HIGH | **FIXED-DG** #243 |
| new-refactor-stale-livedirector-warning + new-director-livedirector-warning-never-clears (NEW-DIR-3, found twice) | HIGH/MED | **FIXED-DG** #243 |
| new-director-super-admin-label-gated-by-standard-set | LOW | **OPEN** — open question Q2 (super-admin-only code rejected as "código incorrecto") |
| new-director-dead-writes-laststate-role-and-page | LOW | **PARTIAL** — lastSyncRole now read (#243/#267); `sv.book.lastPage.standard` read-back status needs re-verify (A3 fix may consume it) |
| new-web-boot-padwidth-clamp-fallback-fragile | LOW | **OPEN** — P7 (seed totalPages=371 pre-manifest) |
| new-web-renderstatus-total-fallback-divergence | LOW | **OPEN** — same root |
| new-web-dead-books-data-inline-blob | LOW | **OPEN** — build.mjs:693-698 still injects `#books-data` nothing reads |
| new-refactor-relay-test-asserts-dead-twobook | MED | **FIXED-#239** |
| new-refactor-contract-test-pins-dead-nonstandard-dts | LOW | **OPEN** — nearby-sync-contract.test.mjs:23 still pins `"standard" \| "nonStandard"` |
| new-refactor-dts-appmode-drift | LOW | **OPEN** — src/nearbyDirectorSync.d.ts:15,30 still declare `nonStandard` |
| new-refactor-handoff-doc-describes-deleted-app | LOW | **FIXED 2026-09-01** — HANDOFF.md deleted from the repo root and `HANDOFF*.md` gitignored; pinned by `e2e/noStaleHandoff.test.mjs` |

### A.3 Sync-reliability audit 2026-07 (3-agent hunt; docs/sync-reliability-audit-2026-07.md)

**Wave 1 — web relay, ALL FIXED-#263 (browser-verified; live via build 377):**
F1 CRIT stray-swipe strands follower with green pill (heartbeat now re-homes ~4s) · F2 CRIT
12s-silence zombie-close now also starts polling · F3 HIGH 10s time-driven health watchdog ·
F4 HIGH demote resets lastSeq=-1 · F5 MED clear pending reconnect on connectRelay entry.

**Wave 2 — native bridge/role, ALL FIXED-DG #264 (typecheck+contract-e2e only; UNCOMPILED here):**
C2 CRIT demoted director kept stale page (now pulls winner's + re-scans) · H4 HIGH -1 sentinel
floor · C3 CRIT step-down straggler frame (becomeFollower clears publish code → straggler 401s) ·
H1 HIGH reloaded follower now requests current snapshot on bridge-ready.

**Wave 3 — mesh + bridge (Miguel: "do all now"; safe subset shipped, rest HELD):**
- **FIXED-DG #266 (Swift, uncompiled!):** M-F1 CRIT half-open watchdog disarmed until first page ·
  M-F5 MED forceFollowerReconnect fast-rediscovery burst · M-F7 MED advertiser/browser permanent
  give-up after 5 failures now retried.
- **FIXED-DG #267:** H3 HIGH transmitter-director role persisted → restart prompt (web congregation
  no longer frozen silently).
- **HELD with ready designs (see B.2):** M-F2, M-F3, M-F6, C1, H2.
- **Noted, unfixed (MED/LOW misc):** inject-queue drops by age not importance · corrupt peer-bundle
  poisons every boot (fallback re-loads it) · director foreground re-broadcasts before conflict
  resolves · async confirm dialog acts on stale role · web→native post has no retry.

### A.4 Prior build-344/345 handoff audit (docs/sync-handoff-known-issues.md — SUPERSEDED by the plan)

Fixed in 344: exit-director→becomeFollower; ⟳ re-joins from "off"; ⟳ refreshNearbyDiscovery; (2
geo/book rows now MOOT-374). Implemented in 345 (device-verify was owed): HIGH#1 live takeover
(later confirmed FIXED), HIGH#2 dual-director split-brain (mesh half done; **relay half = still-open
P2-IDENTITY**), HIGH#3 set-book→page race (fixed + doubly MOOT). Its MED/LOW mesh items
(advertiser accepts another director as follower; foreground re-snapshot; bootstrap-effect re-run —
partially = held H2; legacy nil-discoveryInfo wedge; µs randomToken; follower accepts page from any
peer; nil-page no-snapshot) remain **OPEN/unverified** unless covered by Waves 2-3 rows above.

### A.5 Also on record (not code findings, don't re-report)

- **feedback_never_run_full_e2e** — e2e glob publishes to prod relay (now env-gated but rule stands).
- **Songbook copyright triage** (docs/song-copyright-triage.md): ~92% copyrighted/orphan; public
  App Store listing not viable — product constraint, not a bug.
- **Build env debt** (memory): pod install crashes under Ruby 4.0.1; node_modules ahead of locks;
  Xcode 26 needs LANG=en_US.UTF-8 for pod install.
- **Header-box cleanup** shipped build 377 (#256/#257, 290 pages, pixel-verified). pdftotext bboxes
  unreliable on this PDF — classify by bbox HEIGHT.
- **Post-mortem owed**: song-370/build-325 fire drill (memory, agenda saved — never delivered).

---

## (B) PLANNED-WORK — designed/spec'd but unbuilt

### B.1 Major-update milestones M0–M7 (docs/major-update-2026-07.md; status per implementation-log)

| M | Scope | Status |
|---|---|---|
| M0 | CI + smoke-boot on every PR | ✅ DONE #233 (deferred within M0: guard test that the e2e glob never re-enters CI workflows) |
| M1 | Staging channel: `?env=staging` resolver, `STAGING=1` release, `?selftest` card, rollback-web.sh, checklist | ✅ DONE #234/#235/#237 (native staging entry deferred to M7) |
| M2 | Web crash-proofing A/C/D + native crash floor B | ✅ DONE #238/#250/#252/#253/#261 — Slice B device-verify owed; Swift stop-device-names half open |
| M3 | **Bridge v1 (web half):** typed/validated/acked `bridge-protocol.js`, single dispatcher, hello/welcome handshake, legacy adapter | ⬜ NOT STARTED |
| M4 | **Sync robustness remainder:** transmitterId + two-publisher tiebreak (P2-IDENTITY), consume `{ignored:true}`, always-visible tri-state status pill | ⬜ NOT STARTED (P2-SEQ/CLOCKSKEW/POLL-GAP halves already shipped #248) |
| M5 | **Book out of bundle:** R2 + `__book__` DO + `GET /books/version` + content-addressed page URLs; baked bundle = offline floor | ⬜ NOT STARTED — needs Miguel: R2 bucket + ADMIN_PIN/ADMIN_PASSWORD secrets |
| M6 | **Super-admin dashboard + distribution:** PIN+password, pdf.js render, git-diff additive-only review, atomic publish/rollback, bookVersion heartbeat + hash-verified web swap + fleet column | ⬜ NOT STARTED |
| M7 | **The 2-device day (all native, one TestFlight build):** verify #243 (A3, NEW-DIR-1/2/3) + Slice B + Wave 2 (C2/H4/C3/H1) + Wave 3 shipped (M-F1/F5/F7, H3); implement held items; bridge native half; mesh seq/epoch/transmitterId; mesh bundle sha256+signature; boot-watchdog auto-rollback to baked; native DIAGNÓSTICO + Practice Mode + panic buttons; Swift stop-sending-device-names | ⏳ **THE NEXT GATE** — blocked on Miguel running local `xcodebuild` first (all Wave-2/3 Swift is uncompiled) |

### B.2 Held Wave-3 designs (ready-to-implement, deliberately NOT shipped blind — do WITH build+devices)

| ID | Sev | Design |
|---|---|---|
| M-F2 | HIGH | Director never prunes half-open FOLLOWERS → dead peers hold 7-slot seats ("sessions-full"). Track lastHelloFromPeer; drop >20-25s silent |
| M-F3 | HIGH | Peer-pushed bundle install swaps live WebBundle + remounts EVERY follower mid-Mass. Defer install to idle/backgrounded or confirm-gate |
| M-F6 | MED | Director-conflict demotion hard-resets the loser's followers (sub-group blackout). Additive `redirect` hint before teardown |
| C1 | CRIT | injectJavaScript fire-and-forget; dropped page inject desyncs web-from-native and heartbeat de-dupe never re-drives. Track lastInjectedPageRef ≠ currentPageRef, re-inject on mismatch (flicker risk — device-test) |
| H2 | HIGH | Mesh-bootstrap effect's cleanup calls stopDirectorHeartbeat(); a future dep-add kills the heartbeat mid-Mass. Split listener into own effect; heartbeats owned by the role machine only |

### B.3 Hardening-plan batches never started (P-numbers = canonical plan IDs)

- **P1 remainder:** P1-HARNESS (local wrangler relay for behavioral sync tests), P1-WORKER-UNIT
  (vitest boundary suite), P1-APPJS-UNIT (extract pure reader logic), stale-test deletes
  (permission-flow takeover pin, eas-config, song55, vacuous-90s, nonStandard .d.ts pin).
- **P3 (all open):** CACHEVERSION, IMMUTABLE-PAGES, SW-LIFECYCLE batch (partial-install wedge,
  eviction-by-recency, query-string offline nav, mid-Mass reload guard), P3-lows.
- **P4 (all open):** B-DEPLOYWEB, B-ALTSCRIPTS, bump-first-no-rollback, songindex range-check,
  check-book-consistency hard-fail, rm-rf lockfile, podfile-lock retire, P4-lows.
- **P5 remainder:** PATCH-NOOP, zombie-director-storage, bridge-ready/page-changed clamps,
  render-failed residual race, super-admin auth-gate (Q2), dead-writes cleanup; device-gated Swift
  set (peer-bundle exec kill-switch/signing, stale-Documents-bundle version check).
- **P6 remainder:** PRIVACY-POLICY rewrite (+PrivacyInfo.xcprivacy), FLEET-KEY out of the query
  string + rotation, fleet-checkin light auth, Swift device-name PII.
- **P7 (mostly open):** IMG-RETRY, TIMEOUT-COMMIT, todas-rebuild caching, precache yield,
  theme-search shadowing, poll-storm collapse, SW-poll backoff, search debounce, padwidth seed,
  fullscreen-fab hide. (Wake-lock ✅ done.)
- **P8 (all open):** #books-data blob, dead offline-gate UI, dead state fields, .d.ts narrowing +
  contract-test one-commit land, HANDOFF.md rewrite/delete, app-atlas/app-contracts refresh,
  web-follower-relay-plan mark-historical, bump-build dead block.

### B.4 Strategic roadmap (plan §11, all unbuilt)

P-OTA (native shell pulls newer web bundle from signovivo.com, atomic swap + boot-failure rollback —
the #1 product win; closes the "web fix doesn't reach iPads" gap) · P-RELEASE-ENGINE (idempotent
release stages + doctor.mjs) · P-OBS (client telemetry → mesh keep/retire data) · **P-MESH retire
decision** (single-book strengthened the retire case; write criteria BEFORE the device day) ·
P-CONST (RELAY_BASE ×5 etc. codegen) · P-BUILDENV (fail-hard patches, pinned Ruby) · P8-MODULARIZE ·
P-COMPAT (100vh fallback pre-dvh, fleet osVersion/model).

### B.5 Open decisions owed by Miguel

- ~~Q1 director-restart UX~~ → DECIDED (boot resume-prompt) + implemented #243.
- ~~Q3 A1 residual~~ → DECIDED/DONE (rotated 2026-07-05).
- **Q2** super-admin ⊆ standard codes — intentional or bake-time assertion?
- **Q4** privacy-policy rewrite: certify no residual `request.cf` geo read.
- **Q5** P-MESH retire vs keep as Mass-day safety net.
- Major-update §9: **#3** book version store (`__book__` DO recommended) + R2 bucket + admin
  secrets; **#4** diff strictness (exact sha256 w/ structural fallback); **#5** boot-watchdog
  trigger (2nd consecutive failed boot); **#6** mesh book distribution = super-admin-armed only;
  **#7** Spanish copy for prompts/banners.
- Plus (plan §12 tail): android/ dir prune, multi-parish rooms, i18n, localStorage-wipe data-loss UX.

### B.6 Standing operational state (so nobody re-ships)

- Worker deployed 2026-07-05 10:20 PM CDT (`b2f67748`): A2 + /state now + /log gate + crash panel LIVE.
- Web + native build **377** live (signovivo.com + TestFlight) — carries P2/SliceC/SliceD web fixes
  + hymnal header-box cleanup. Do NOT re-run pages deploy / re-cut a build for that batch.
- A1 secret rotated. Rollbacks: `wrangler rollback` (worker), `scripts/rollback-web.sh` (web).
- **Everything native since build 377's #243 batch (Waves 2-3, Slice B) is git-only, UNCOMPILED,
  device-unverified** — Miguel must run local xcodebuild, then the Wednesday 2-device day.

---

## Oddities noticed while mapping (for later audit lenses to chase — not yet reported anywhere)

1. **Wave-1 F1 (heartbeat re-homes a drifted follower in ~4s) vs the still-open
   `browse-result-click-skips-relay-browsing` finding:** F1 makes any un-flagged navigation get
   yanked back in ~4s instead of on the director's next move — the browse/search-tap inconsistency
   is now MORE user-visible, and the interaction between F1's drift detection and `relay.browsing`
   is fresh, unaudited surface (web/src/app.js relay heartbeat region, ~app.js:2853+).
2. **`?k=` is still a first-class credential channel** (sync-worker/src/index.ts:627,676 and the
   dashboard even instructs `?fleet-dashboard?k=TU_CÓDIGO` at :683) after P6-LOG shipped — the
   fleet-key-in-URL finding was left half-addressed by design ("Miguel just appends ?k=SECRET"), and
   no key rotation is recorded anywhere post-observability.
3. **Native relay room is hardcoded** `alvernia-main` (src/directorRelaySync.js:13) while web has
   the staging resolver — a native canary director on staging web still publishes to PROD. Known
   (deferred to M7) but a live footgun for the canary-walk ritual in the meantime.
4. **Docs' own line anchors are 3 generations stale** (audit @370 → reconciliation @374/39b60313 →
   HEAD 16244b25 with app.js +302 lines, worker +118, PdfReaderApp +166). Any auditor citing a doc
   line without re-grepping will mis-anchor.
5. **The e2e "safe subset" count keeps drifting** (61/61 → 65/65 → 91/91 → 92/92 across log
   entries) with tests added out-of-band to ci.yml (svRelayRoom/svSelftest "had drifted out of
   ci.yml") — CI's guarded set is hand-maintained, the exact staleness class P1 was meant to kill.
6. **`new-director-dead-writes` is now half-alive:** `lastSyncRole` gained a reader
   (PdfReaderApp.tsx:869) but `sv.book.lastPage.standard` writes need re-verification — if the #243
   A3 fix didn't adopt the read-back variant, the page write is still dead and page-1 flashes on
   manual re-promote remain possible.
7. **HANDOFF.md still present at repo root** describing the deleted two-book architecture — a cold
   tab reading it before this map will chase ghosts (also flagged as a finding; listed here because
   it actively poisons future agent runs).
8. **A2 rate limiter is fail-open** by design — worth one adversarial look at whether the fail-open
   path can be forced deliberately (e.g. DO storage errors) to re-open the flood window.
9. **Wave-2/3 Swift shipped without ANY compile check** (environment cannot build Swift). PR #266
   touches the disaster-surface mesh; if xcodebuild fails Wednesday morning, the fallback is
   shipping 377 (which lacks M-F1/F5/F7 + H3) — nobody has written that contingency down.
