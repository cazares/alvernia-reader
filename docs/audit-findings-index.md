# ALL FINDINGS INDEX (117)

## relay-protocol
- [C] relay-director-webview-reload-broadcasts-boot-page — Director WebView reload/app restart clobbers currentPageRef with the web's boot page and broadcasts page 2 to the whole congregation (PdfReaderApp.tsx:553)
- [H] relay-seq-guard-blocks-staleness-and-takeover-on-ws-path — Web follower seq guard runs before the freshness check: a dead director is never demoted and a regressed-seq takeover is ignored over a healthy WebSocket (web/src/app.js:3407) [prior:build-344 fixed-table row 'Web: a restarted director (seq reset low) ignored as stale' — that fix is ⟳-manual only; the automatic WS path is still frozen]
- [H] relay-e2e-tests-publish-to-production-room — npm run test:e2e publishes real page flips into the production relay room alvernia-main with a still-valid code (e2e/relay-sync.test.mjs:21)
- [H] relay-sion-code-cross-book-publish-into-live-room — Public Sión code can publish hymns-4 snapshots into the live standard-Mass room, flipping every web follower's book (sync-worker/src/index.ts:796) [prior:HIGH#2 'Dual-director split-brain — Relay path has NO conflict resolution']
- [H] relay-no-transmitter-identity-two-publishers-ping-pong — No transmitter identity in the relay protocol: two authorized publishers ping-pong all web followers with no arbitration and no warning (src/directorRelaySync.js:59) [prior:MED 'Relay seq is wall-clock-ms, not device-unique → two transmitters collide → flapping']
- [M] relay-ws-message-interleaved-apply-during-book-switch — Un-awaited applyRelaySnapshot per WS message lets a second push interleave a cross-book switch, stranding the follower on the wrong page for up to 12s (web/src/app.js:3635)
- [M] relay-unauthorized-follower-renders-wrong-song-live — Unauthorized web follower renders the WRONG hymns-4 song under a green 'en vivo' pill when the director broadcasts a standard-book page ≤ 51 (web/src/app.js:3393)
- [M] relay-polling-stopped-before-ws-opens-blind-windows — connectRelay kills /state polling before the socket opens, creating repeating ~6s blind windows on networks where WebSockets never establish (web/src/app.js:3585) [prior:MED 'Web: zombie CONNECTING socket...' (the timeout itself was implemented; this is the residual polling gap it introduced)]
- [M] relay-follower-clock-skew-defeats-freshness — Follower with a fast device clock (>90s) never sees the director as live — relayIsFreshLive compares raw client clock to server ts with no offset calibration (web/src/app.js:3333)
- [M] relay-fleet-webcached-inflated-across-books — Fleet check-in claims webCached readiness from a cross-book page count — dashboard can show 'Listo' while dozens of standard pages are uncached (web/src/app.js:3144)
- [M] relay-transmitter-only-role-lost-on-relaunch — Transmitter-only (no-mesh) director role is never persisted or restored — an app relaunch silently stops all relay publishing (PdfReaderApp.tsx:409)
- [L] relay-unlock-swallows-failed-switchbook — unlockStandard swallows a failed switchBook: device reports 'unlocked' and pins geo while still displaying hymns-4 (web/src/app.js:3504)
- [L] relay-browsing-user-book-switched-mid-browse — A director's cross-book move switches a deliberately-browsing follower's book (rendering page 2) despite the browsing guard (web/src/app.js:3377)

## web-reader
- [H] web-reader-snapshot-during-switchbook-renders-old-totalpages — Relay/native page event arriving during an awaited switchBook renders against the OLD book's totalPages — follower stuck on wrong page under a green live pill (web/src/app.js:3377) [prior:HIGH#3 (set-book→page two-script race — same interleave-at-await-switchBook mechanism, web-relay surface)]
- [H] web-reader-unguarded-localstorage-kills-module-white-gate-forever — Two unguarded top-level localStorage reads (sv-haptic, sv-tip) crash module evaluation in cookie-blocked browsers — permanent white geo-gate with no backstops (web/src/app.js:436)
- [M] web-reader-persisted-standard-boot-reveals-hardcoded-hymns4-img — Persisted-standard boots reveal the gate by decoding the hardcoded hymns-4 <img> — wrong-book flash of up to ~3s, defeating the gate's core invariant (web/src/app.js:3773)
- [M] web-reader-live-img-error-retry-bypasses-requestid-guard — Live <img> error-retry re-commits a captured old src without consulting state.pageLoadRequest — can revert the visible page after a page turn (web/src/app.js:56)
- [M] web-reader-theme-substring-shadows-text-search-santo — searchByTheme substring match takes precedence over full-text search — searching "santo" returns Espíritu-Santo-themed songs and makes Santo/Sanctus settings (incl. the new Canto 371 "Santo Español") unreachable (web/src/app.js:1593)
- [M] web-reader-browse-result-click-skips-relay-browsing-mode — Tapping a song in the browse drawer / search results never sets relay.browsing — follower is silently yanked back to the director on the next push, unlike the numpad jump (web/src/app.js:2818)
- [M] web-reader-initreader-pagesjson-fetch-no-ok-no-retry — initReader's non-default pages.json boot fetch has no response.ok check and no retry — one transient failure leaves the session with totalPages=1 and a clamped-to-page-1 follower (web/src/app.js:3716)
- [L] web-reader-loadsearchindex-stale-book-race-and-no-ok-check — loadSearchIndex has no book/generation guard and no response.ok check — a book switch mid-fetch installs the OLD book's search index into the NEW book (web/src/app.js:1466)
- [L] web-reader-window-keydown-ignores-editable-targets — Window keydown handler never checks event.target — arrow keys while typing in the search input or fleet-picker name field turn the song AND close the drawer; Escape double-fires (web/src/app.js:3050)
- [L] web-reader-hydratebookdata-keeps-numeric-prefetch-sets — hydrateBookData clears prefetchedPageUrls but not the number-keyed state.prefetchedPages / prefetchingPages — adjacent-song prefetch silently disabled for colliding page numbers after a book switch (web/src/app.js:1254)
- [L] web-reader-rendersongitem-innerhtml-unescaped-ocr-titles — renderSongItem builds DOM via innerHTML with unescaped song.title/song.key — OCR-derived titles are an unescaped trust boundary in every browse tab (web/src/app.js:2388)
- [L] web-reader-draft-cap-blocks-legacy-11-digit-code — Numpad draft cap of 10 digits makes the still-live legacy 11-digit transmitter code (12345678840) unenterable on web, and Number() parsing would corrupt any future leading-zero code (web/src/app.js:1374)
- [L] web-reader-gate-caption-pingpong-and-false-offline — 8s backstop shows "Sin conexión — reintentando…" to ONLINE devices with merely-slow geo, and after 12s the two backstop loops flip the caption back and forth every ~1.5s (web/src/app.js:3878)
- [L] web-reader-fullscreen-fab-noop-on-ios-pwa — ⛶ fullscreen fab is shown on iOS home-screen PWAs where toggleFullscreen is an explicit no-op (web/src/app.js:2230)

## native-swift
- [H] native-swift-peer-bundle-unauthenticated-exec — Peer web-bundle push is unauthenticated → persistent arbitrary code in the follower WebView (ios/SignoVivo/DirectorSyncModule.swift:717) [DEV]
- [H] native-swift-relay-split-brain-no-tiebreak — HIGH#2 only half-fixed: relay path still has no transmitter tiebreak → web congregation flaps with two directors (src/directorRelaySync.js:57) [DEV] [prior:HIGH#2]
- [H] native-swift-stale-documents-bundle-masks-update — Peer-pushed Documents/WebBundle is never version-checked or cleaned → a TestFlight update silently runs OLD web code while the badge shows the NEW build (PdfReaderApp.tsx:711) [DEV]
- [H] native-swift-webview-patch-silent-noop — patch-rn-webview.js silently no-ops if react-native-webview changes → file:// never uses loadFileURL → blank app shipped (scripts/patch-rn-webview.js:44)
- [H] native-swift-render-failed-sentinel-promote — render-failed -1 sentinel can be broadcast on director promotion → whole congregation yanked to page 1 (PdfReaderApp.tsx:690) [DEV]
- [H] native-swift-dbglog-device-name-pii — Swift dbgLog leaks device owner names (UIDevice.name) to the public /log endpoint on every mesh event (ios/SignoVivo/DirectorSyncModule.swift:174)
- [M] native-swift-onerror-no-recovery — onError only breadcrumbs — a failed initial file:// load leaves a permanent blank WebView with no retry (PdfReaderApp.tsx:989)
- [M] native-swift-page-changed-clamp-stale-total — page-changed clamps against the OLD totalPagesRef before updating it → first turn after a book change can broadcast a wrongly-capped page (PdfReaderApp.tsx:590)
- [M] native-swift-zombie-director-on-storage-failure — AsyncStorage failure during becomeDirector leaves a zombie director (mesh advertising, web shows 'código incorrecto', followers stranded) (PdfReaderApp.tsx:442)
- [M] native-swift-bridge-ready-unclamped-total — bridge-ready trusts msg.page/msg.totalPages with no validation → a bogus totalPages poisons the clamp for all later page-changed events (PdfReaderApp.tsx:553)
- [M] native-swift-24h-restore-remints-stale-director — 24h director auto-restore can re-mint a stale director the morning after Mass → split-brain + web flapping (PdfReaderApp.tsx:771) [DEV] [prior:HIGH#2]
- [L] native-swift-geo-overrides-chosen-book — Geo effect re-checks the director snapshot but not storedBookRef after the await → a book chosen mid-fetch can be overridden by geo (PdfReaderApp.tsx:899)

## offline-pwa
- [H] offline-pwa-cacheversion-misses-book-content — cacheVersion hashes only 5 shell files — book-content changes can ship without busting any cache (web/build.mjs:22)
- [H] offline-pwa-immutable-page-bytes-stale-forever — In-place page-image revisions can never reach returning devices — old-cache-first + 1-year immutable + cross-cache seeding (web/src/sw.js:187)
- [H] offline-pwa-fleet-webcached-false-green — Fleet dashboard webCached can be a false green: sticky OFFLINE_READY_KEY + cross-book page counting (web/src/app.js:3144)
- [M] offline-pwa-partial-install-wedges-updates — A partially-failed SW install wedges update delivery until the NEXT deploy — precache is never retried (web/src/sw.js:133)
- [M] offline-pwa-page-cache-eviction-by-recency — activate evicts page caches by recency, not completeness — can delete the only full offline bundle (web/src/sw.js:110)
- [M] offline-pwa-query-string-offline-navigation-fails — Offline navigation with any query string bypasses the cached shell — browser error page instead of the app (web/src/sw.js:222)
- [M] offline-pwa-unguarded-localstorage-kills-boot — Top-level unguarded localStorage reads can kill all of app.js — permanent white gate (web/src/app.js:436)
- [M] offline-pwa-persisted-standard-reveal-flash — Persisted-standard boots can lift the gate on the hardcoded hymns-4 image — wrong-book flash (web/src/app.js:3773)
- [M] offline-pwa-mid-mass-deploy-force-reload — A deploy during Mass force-reloads every online follower tab within seconds (web/src/app.js:2320)
- [L] offline-pwa-precache-no-in-session-retry — One transient failure aborts a book's precache for the whole session; floor precache completes with silent holes (web/src/app.js:694)
- [L] offline-pwa-global-cache-match-version-mix — Shell lookups use global caches.match — a defensively-retained old static cache can serve mixed-version shells (web/src/sw.js:61)
- [L] offline-pwa-dead-offline-gate-ui — Offline-download UI and bundle verification are dead code — no on-device offline-readiness signal exists (web/src/app.js:764)
- [L] offline-pwa-unawaited-sw-async — clients.claim() and page-image cache.put are fire-and-forget in the SW (web/src/sw.js:130)

## security-privacy
- [C] secpriv-committed-live-transmitter-code — A full-privilege director code (12345678840) is committed in the repo and still accepted live — public master credential to hijack Mass + read the private manual (e2e/relay-sync.test.mjs:21)
- [C] secpriv-publish-flood-no-ratelimit — No rate limiting anywhere + seq=0 bypass + one shared room lets any internet user flood /publish and hold the live congregation on the wrong page/book (sync-worker/src/index.ts:754)
- [H] secpriv-unlock-brute-force-oracle — /unlock is an unthrottled yes/no code oracle over phone-number-structured codes — brute-forceable to recover a real director credential (sync-worker/src/index.ts:731)
- [H] secpriv-privacy-policy-materially-false — App Store privacy policy claims zero data collection / no internet / Keychain storage while the app transmits device ids, self-entered names, coarse geo, and iOS device names to a server (docs/privacy-policy.html:22)
- [H] secpriv-log-unauthenticated-pii-leak — /log is world-readable and un-capped; Swift posts device display names, so anyone can harvest parishioners' names and wipe mid-Mass diagnostics (sync-worker/src/index.ts:594)
- [H] secpriv-release-pii-swap-window — release.sh has no crash-safe cleanup around the director-codes PII swap — a Ctrl-C at 7am can leave real phone numbers in a tracked file and git-commit them to the public repo (scripts/release.sh:40)
- [M] secpriv-fleet-key-in-url-logged — FLEET_DASHBOARD_KEY (gates choir phone numbers) is passed as a ?k= query param with Worker observability ON — the secret lands in Cloudflare logs, history, and referrers (sync-worker/src/index.ts:654)
- [M] secpriv-fleet-checkin-open-abuse — /fleet/checkin is open and unthrottled — an attacker can evict every real check-in and blind the pre-Mass readiness dashboard (sync-worker/src/index.ts:632)
- [M] secpriv-standard-manual-not-actually-gated — The 'private' Del Rio manual is served publicly and unauthenticated on signovivo.com — the geo/unlock gate is client-side cosmetics (web/build.mjs:827)
- [L] secpriv-director-code-plaintext-asyncstorage — Director access code (a real phone number granting publish + unlock) is persisted in unencrypted AsyncStorage, not Keychain/SecureStore (PdfReaderApp.tsx:445)
- [L] secpriv-health-geo-oracle — /health echoes the requester's geolocation with CORS '*' — any site a parish device visits can read its approximate location and Del-Rio membership (sync-worker/src/index.ts:565)

## build-release
- [C] build-release-director-codes-swap-no-trap — No trap/EXIT handler around the director-codes PII swap — interrupted release leaves real phone numbers in the tracked file (scripts/release.sh:42)
- [C] build-release-e2e-suite-mutates-prod-relay — Blessed `npm run test:e2e` publishes real page flips to the PRODUCTION relay room with a valid director code (e2e/relay-sync.test.mjs:16)
- [H] build-release-restore-codes-set-e-abort — restore_codes returns 1 under set -e when the private file is missing — release dies AFTER a successful archive, before IPA copy and web deploy (scripts/release.sh:48)
- [H] build-release-cacheversion-hash-gap-frozen-manifests — cacheVersion excludes book data (PDFs, song index, assets/standard/*.json, version.json) while book manifests are served cache-first — a book-data-only dirty deploy never reaches returning followers (web/build.mjs:22)
- [H] build-release-deploy-web-npm-script-trap — `npm run deploy:web` omits --branch main and the version bump — one command produces an unbumped preview-or-prod deploy outside the blessed pipeline (package.json:15)
- [H] build-release-alt-archive-scripts-bypass-invariants — testflight-upload*.sh and submit-appstore.sh archive without rebuilding web, syncing ios/WebBundle, or baking director codes — they produce IPAs with a stale web bundle and NO standard-director entry (scripts/testflight-upload.sh:23)
- [M] build-release-bump-first-no-rollback — Version bump is step 1 with no rollback — any later failure leaves 4 manifests bumped/dirty and a retry double-bumps (scripts/release.sh:22)
- [M] build-release-immutable-page-urls-vs-changed-bytes — Page WebP bytes change with DPI/quality env vars or PDF edits at URLs marked immutable-for-a-year — no purge path, clients mix old and new page images (web/build.mjs:91)
- [M] build-release-hymns4-songindex-range-unchecked — hymns-4 songIndex passes through to pages.json with no page-range check — only the search index is validated against the rendered page count (web/build.mjs:689)
- [M] build-release-footprint-test-red-at-head — e2e suite is red at HEAD: repo-minimal-footprint's script allowlist was never updated for deploy:web (e2e/repo-minimal-footprint.test.mjs:15)
- [M] build-release-check-book-consistency-soft-skip-and-coverage — check-book-consistency soft-skips (exit 0) without pdfinfo, guards only the standard book, and never runs in release.sh (scripts/check-book-consistency.mjs:30)
- [M] build-release-rm-rf-build-concurrent-tabs — Every archive flow does `rm -rf build` at the repo root with no lock — concurrent tabs clobber each other's in-flight archives (scripts/release.sh:49)
- [M] build-release-podfile-lock-copy-defeats-guard — Unconditional `cp ios/Pods/Manifest.lock ios/Podfile.lock` before every archive permanently neutralizes the CocoaPods consistency guard (scripts/release.sh:36)
- [L] build-release-bump-silent-regex-skip-dead-block — bump-build.mjs manifest syncs are silent-on-no-match regex edits, plus a dead offlineWebBundle sync block (scripts/bump-build.mjs:42)
- [L] build-release-dist-wipe-before-tool-preflight — build.mjs wipes web/dist before checking any required tool, and release.sh hides its progress behind >/dev/null (web/build.mjs:41)
- [L] build-release-preios-hooks-bump-on-every-local-run — preios hooks bump the global build number on every local `npm run ios`, dirtying 4 tracked files per dev run (package.json:8)

## test-suite
- [C] test-suite-relay-sync-mutates-production-room — npm run test:e2e publishes ~20 real page flips to the live production relay room, and README instructs running it (e2e/relay-sync.test.mjs:17)
- [H] test-suite-repo-footprint-red-at-head — repo-minimal-footprint.test.mjs is FAILING at HEAD — the whole suite is red, masking every other regression (e2e/repo-minimal-footprint.test.mjs:15)
- [H] test-suite-sync-worker-zero-tests-security-boundaries — sync-worker (818 lines) has zero tests — the private-book security boundaries (Sión book-scoping, /unlock gating, fleet PII gate) are regression-unprotected (sync-worker/src/index.ts:746)
- [H] test-suite-appjs-sync-core-zero-tests — web/src/app.js (3,928 lines) has zero tests — the Mass-critical follower sync logic is only 'covered' by one vacuous grep (web/src/app.js:3407) [prior:known-issues MED (web zombie CONNECTING socket, stale-director pill inconsistency) — the untested code paths this finding wants covered]
- [M] test-suite-eas-config-banned-pathway-stale-assets — eas-config.test.mjs enforces the banned EAS pathway and validates a stale asset list — while the assets app.json ACTUALLY needs are unguarded (e2e/eas-config.test.mjs:54)
- [M] test-suite-vacuous-staleness-90s-contract-unpinned — The 'staleness' test cannot fail — the 90s director-liveness contract shared by worker and web client is effectively untested (e2e/relay-sync.test.mjs:380)
- [M] test-suite-build-manifest-generation-untested — web/build.mjs manifest generation (832 lines) has zero tests — the build-325 'unreachable song' fire-drill class is unguarded for hymns-4 and only hook-guarded for standard (web/build.mjs:739) [prior:project_postmortem_song370_firedrill (memory): the build-325 song-370 class this guards against]
- [M] test-suite-sw-cache-lifecycle-untested — web/src/sw.js (240 lines) has zero tests — the offline cache-retention rules that keep iPads working in a no-wifi church are unguarded (web/src/sw.js:14)
- [L] test-suite-nearby-sync-regex-pins-classification — nearby-sync-contract.test.mjs: 29/29 tests are source-regex pins — keep the cross-layer wire-contract pins, DELETE the implementation-shape pins (e2e/nearby-sync-contract.test.mjs:213)
- [L] test-suite-permission-flow-dead-takeover-pin — permission-flow's 'director takeover flow is wired' test pins a dismantled flow — requestDirectorTakeover/approveDirectorTakeover are exported but never called (e2e/permission-flow.test.mjs:38) [prior:docs/sync-handoff-known-issues.md HIGH#1 (takeover rejection — now fixed in code; this test pins the pre-fix plumbing)]
- [L] test-suite-song55-test-name-lies-about-clamping — native-entrypoint's 'song 55 → page 55, out-of-range clamps' test asserts none of that — it greps for the literal string '[55, 55]' (e2e/native-entrypoint.test.mjs:44)
- [L] test-suite-bump-build-lockstep-untested-directly — bump-build.mjs's silent-skip regex syncs have no direct test — lockstep drift is only caught if someone remembers to run the suite between bump and archive (scripts/bump-build.mjs:46)
- [L] test-suite-readme-instructs-prod-mutation-and-lies — README.md quick-start instructs running the prod-mutating suite and misdescribes the entire architecture (README.md:17)

## perf
- [H] perf-live-img-retry-overwrites-new-page — Live <img> error-retry has no stale-request guard — a delayed retry can overwrite the newly committed page and strand a follower on the wrong page (web/src/app.js:56)
- [H] perf-timeout-commit-hides-loading-indicator — 3s preload timeout commits an unloaded src, then unconditionally hides the loading indicator and updates the song title — old page shown under new-song status with no affordance (web/src/app.js:1200)
- [M] perf-boot-precache-contention-with-live-turns — 4-way ~25 MB precache (plus a second 4-way floor pool) starts 4-5s after gate lift with no yield to live page loads — cold-boot-during-Mass page turns fight up to 8 concurrent downloads (web/src/app.js:642)
- [M] perf-persisted-standard-boot-wrong-book-reveal — Persisted-standard boots lift the geo-gate on the hardcoded hymns-4 image before the standard page commits — wrong-manual flash on every returning Del Rio web boot (web/src/app.js:3773)
- [M] perf-hidden-drawer-todas-rebuilds — Full 'todas' browse list (315 song rows + ~27 headers, per-item innerHTML) rebuilt into the laid-out off-screen drawer twice at boot, on every book switch, and on every drawer open (web/src/app.js:3808)
- [M] perf-mid-mass-deploy-fleet-reload — A deploy landing while followers are live is picked up within ≤60s and auto-reloads every open follower tab mid-hymn — no 'live-following' guard on the SKIP_WAITING→reload chain (web/src/app.js:2320)
- [M] perf-no-screen-wake-lock-web-followers — Web followers have no Screen Wake Lock — a signovivo.com iPad/phone auto-locks minutes into Mass and goes dark/desynced until manually woken (web/src/app.js:3665)
- [L] perf-offline-poll-storm-battery — When the relay is unreachable, three overlapping retry loops (4s forced /state polls, 5s geo re-heal, ≤8s WS reconnect) hammer the dead network for the whole outage (web/src/app.js:3576)
- [L] perf-sw-update-poll-mass-overhead — SW update polling settles at 60s — ~180 forced sw.js network fetches (~2.1 MB) plus a radio wake per minute per device over a 3h Mass (web/src/app.js:2284)
- [L] perf-page-cache-double-retention-disk — Two retained page-cache versions × both books ≈ 56 MB steady (84 MB transient during refill) plus 1-year HTTP-cache copies — avoidable disk pressure on storage-tight iPads (web/src/sw.js:110)
- [L] perf-book-switch-stale-prefetch-sets — hydrateBookData clears prefetchedPageUrls but not the page-number-keyed prefetch Sets — adjacent-song warming silently disabled after a mid-Mass book switch (web/src/app.js:1254)
- [L] perf-search-per-keystroke-renormalization — searchPages re-normalizes the entire search index (354 entries, ~134 K chars NFD+regex+lowercase) on every un-debounced keystroke (web/src/app.js:1498)
- [L] perf-gate-caption-flicker-dueling-backstops — After 12s on an unresolved floor boot, the two gate-backstop loops alternately set and clear the 'Sin conexión' caption every ~1.5s (web/src/app.js:3878)

## coherence
- [H] coherence-seq-regression-freezes-ws-followers — Worker's designed seq regression on director takeover is not handled by the web follower's monotonic lastSeq guard (web/src/app.js:3407) [prior:Fixed-in-344 row '⟳ resets relay.lastSeq' (manual-only mitigation) + MED#5 (relay seq is wall-clock-ms)]
- [H] coherence-ignored-publish-response-never-consumed — Worker's {ok:true, ignored:true} publish rejection is invisible to the native transmitter — a handoff director broadcasts into the void with no warning (src/directorRelaySync.js:88) [prior:MED#5 (two transmitters collide) / HIGH#2 (relay path has no conflict resolution)]
- [M] coherence-fleet-webcached-inflated-cross-book — Web fleetCheckin's webCached fallback counts BOTH books' cached pages against one book's total, faking 'Listo — web en caché' on the dashboard (web/src/app.js:3144)
- [M] coherence-footprint-test-red-and-deploy-web-preview-trap — e2e suite is red at HEAD (repo-minimal-footprint allowlist missing deploy:web) and the new deploy:web script itself deploys a PREVIEW, not prod (e2e/repo-minimal-footprint.test.mjs:15)
- [M] coherence-director-codes-two-hand-synced-stores — Standard director codes live in two manually-synced stores (native baked JSON vs worker secret) with no consistency check, and every non-release native build ships an EMPTY set (PdfReaderApp.tsx:56)
- [M] coherence-wrong-book-flash-on-standard-boot — Hardcoded hymns-4 default <img> + reveal-before-render ordering lifts the gate onto the WRONG book on every persisted-standard and native-standard boot (web/src/index.html:57)
- [M] coherence-ws-interleave-stale-totalpages-clamp — Un-awaited applyRelaySnapshot per WS message + switchBook's synchronous currentBook write lets a second snapshot clamp the director's page against the OLD book's totalPages (web/src/app.js:3635)
- [L] coherence-numpad-code-cap-and-leading-zero-corruption — Web/native numpad caps codes at 10 digits and strips leading zeros, while the worker contract says codes are 10–11 digits and the live secret contains an 11-digit code (web/src/app.js:1374)
- [L] coherence-relay-base-five-hardcoded-copies — The relay origin is hardcoded in five places with only one override mechanism — a future relay move (planned sync.signovivo.com) can silently split director and followers onto different relays (web/src/index.html:36)
- [L] coherence-page-changed-clamps-before-totalpages-update — Native page-changed handler clamps against the OLD totalPagesRef before adopting the message's own totalPages, breaking the atomic page+totalPages contract (PdfReaderApp.tsx:590)
- [L] coherence-director-code-asyncstorage-not-securestore — director_access_code (a real phone number) is persisted in plaintext AsyncStorage while expo-secure-store is installed and configured for exactly this purpose (PdfReaderApp.tsx:445)
- [L] coherence-vacuous-90s-staleness-test — The only test pinning the shared 90s freshness contract is vacuous — `appJs.includes("90")` can never fail on a 3900-line file (e2e/relay-sync.test.mjs:380)
