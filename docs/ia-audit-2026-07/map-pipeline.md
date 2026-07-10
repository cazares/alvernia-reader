> ⚠️ **CORRECTION BANNER (2026-07-09).** This map was written at HEAD 16244b25 / build 377. The branch has since been fast-forwarded to build 381 (d5075091). Landed since this map: **#269** capped the "Buscando director…" spinner so it never looks stuck; **#270 REMOVED the "¿Quién usa este iPad?" fleet self-ID modal entirely** (fleet check-in itself remains); **#271** simplified the sync spinner and renamed the songbook PDF (assets/alvernia_manual_2.pdf → assets/signo_vivo_371.pdf), touching web/src/app.js (−112 lines area), index.html, styles.css. Where this map contradicts current source, CURRENT SOURCE WINS — do not report the removed modal or old spinner behavior as findings.

# SignoVivo — BUILD/RELEASE PIPELINE + TEST-PIN MAP (current state @ HEAD 16244b25, build 377, v1.0.4)

All paths relative to repo root `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a`.
Verified against CURRENT source; atlas/contract docs NOT trusted. Nothing was executed except read-only git/ls/grep.

---

## 1. ARCHITECTURE — one web bundle, two delivery rails

```
web/src/{index.html,app.js,sw.js,styles.css,manifest.webmanifest,lib/*.js}
        │
        ▼  node web/build.mjs   (token replace + PDF page render + manifest gen)
web/dist/  (gitignored: .gitignore:23)
        ├──► RAIL A (web):    npx wrangler pages deploy web/dist --project-name alvernia-reader
        │                     prod = Pages branch "main" → signovivo.com
        │                     canary = Pages branch "staging" (release.sh STAGING=1)
        └──► RAIL B (native): release.sh step 3: rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle
                              ios/WebBundle is GITIGNORED (.gitignore:8-9) but referenced in the
                              Xcode project as a blue-folder resource:
                                ios/SignoVivo.xcodeproj/project.pbxproj:19,38,59,208
                              → shipped INSIDE the IPA; WKWebView loads it from file://
```

Native load: `PdfReaderApp.tsx:813-826` (`resolveBundleUri`) — prefers `Documents/WebBundle/index.html`
(a peer-pushed mesh update) over the app-bundle copy `<bundleDir>WebBundle/index.html`. Boot effect at
`PdfReaderApp.tsx:829-846` sets `bundleUri`; book is hardcoded `"standard"` (line 834).

### Version wiring (single source: version.json)
- `version.json` = `{ baseVersion: "1.0.4", buildNumber: 377 }` — THE source of truth.
- `scripts/bump-build.mjs` (run by release.sh step 1 and by `preios`) propagates buildNumber+1 into:
  - `version.json` (lines 9-17)
  - `src/offlineWebBundle.js` (lines 20-28) — **DEAD CODE**: that file was deleted at build 304
    (commit 73174591); the `fs.existsSync` guard makes it a silent no-op.
  - `ios/SignoVivo.xcodeproj/project.pbxproj` `CURRENT_PROJECT_VERSION` (lines 31-36)
  - `ios/SignoVivo/Info.plist` `CFBundleVersion` (lines 39-47)
  - `app.json` `expo.ios.buildNumber` / `expo.android.versionCode` (lines 50-56, "best-effort")
- `app.config.js:24-77` re-reads version.json at expo-config time: `version`/`runtimeVersion` =
  baseVersion, `ios.buildNumber` = buildNumber, `updates: { enabled:false, checkAutomatically:"NEVER" }`.
- `PdfReaderApp.tsx:40,44` imports version.json → `BUILD_VERSION`, injected into the WebView as
  `window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION` (`PdfReaderApp.tsx:1037`, in `preloadScript` 1033-1042
  via `injectedJavaScriptBeforeContentLoaded`). Note: this is the NATIVE SHELL's build number, compiled
  into the RN JS bundle — NOT the version of the web bundle actually being displayed (see Oddity O1/O2).

### web/build.mjs token replacement (web/build.mjs)
- `__CACHE_VERSION__` = `<git short sha>-<sha256(app.js,sw.js,styles.css,index.html,manifest,build.mjs)[:8]>`
  (lines 10-28; content-hashed deliberately because builds come from the WORKING TREE — a SHA-only
  version would collide across two dirty deploys at the same HEAD). Replaced in `app.js` (consumed at
  `web/src/app.js:217`) and `sw.js` (consumed at `web/src/sw.js:1` → cache names).
- `__BUILD_NUMBER__` = `version.json.buildNumber` (lines 32-39, 68; consumed `web/src/app.js:220`,
  displayed as the `v<NNN>` badge; `app.js:3543-3546` prefers `__SIGNO_VINO_NATIVE_BUNDLE_VERSION`
  on native, with a `[0] !== "_"` guard against an unreplaced token).
- `__RELAY_BASE__` = `process.env.ALVERNIA_RELAY_BASE || "https://signovivo-sync.4j4982y8jp.workers.dev"`
  (line 63; consumed `web/src/app.js:2849`).
- `web/src/lib/*.js` copied VERBATIM — no token replacement (lines 47-61). They are dependency-free,
  node-unit-testable helpers loaded `<script defer>` BEFORE app.js (`web/src/index.html:373-379`:
  svRelayRoom, svSelftest, svSyncDecision, then app.js — deferred order matters).
- Inline JSON script tags injected into `dist/index.html` before `</head>` (lines 697-704):
  `#books-data` (single-book registry) + `#pages-data` (`{totalPages}` — the minimum to paint frame 1).
- `_headers` written at lines 710-713: `/books/*/pages/*` → `Cache-Control: public, max-age=31536000,
  immutable` (page WebPs are content-stable; updates ride the SW cache-version bump).
- Page rendering: `pdftoppm -png -r 115` → `cwebp -q 60` (lines 108-155); icons via macOS-only `sips`
  (lines 79-92) → **the web build only runs on macOS** (CI runner is macos-latest for this reason).
- Song metadata: regex-parses `[song, page]` pairs out of `src/alverniaManual2SongIndex.js`
  (lines 563-566), runs `pdftotext` over `assets/alvernia_manual_2.pdf`, derives titles/themes/keys/
  lyrics/complexity, emits per-book `pages.json`, `search-index.json`, `song-titles.json`,
  `song-search-index.json` (lines 633-649) + top-level `books.json` (680-686).
  `totalPages = rendered page count` (line 640) — cannot drift from the PDF by construction.

### SW cache strategy (web/src/sw.js:1-40)
- Cache names keyed on `CACHE_VERSION`; page caches KEPT across version bumps (immutable content),
  retaining the 2 newest (`PAGE_CACHES_TO_KEEP = 2`, line 14) so a follower who goes offline right
  after a deploy still has the previous full bundle. Shell assets are network-first; rollbacks
  propagate to online followers in ~60s (per rollback-web.sh:53).

---

## 2. USER-VISIBLE SURFACES

| Surface | Artifact | Version badge | Update path |
|---|---|---|---|
| signovivo.com (web/PWA) | Pages project `alvernia-reader`, prod branch `main` | `v<N>` from `__BUILD_NUMBER__` | every release.sh run (even SKIP_NATIVE=1); SW picks up in ~60s online |
| Native iPad app | IPA wrapping `ios/WebBundle` copy of web/dist | overlay `b<N>` from version.json via RN | TestFlight/App Store (manual install), OR mesh bundle-push (below) |
| Mesh bundle push | Director packs its SHIPPED `Bundle.main/WebBundle` → follower's `Documents/WebBundle` | follower still shows the SHELL's number | automatic when follower connects to a newer-build director |
| Staging canary | Pages branch `staging` (preview URL); relay room `alvernia-staging` only via `?env=staging` | same badge | `STAGING=1 bash scripts/release.sh` |

---

## 3. FLOWS

### 3a. Full lockstep release — `bash scripts/release.sh` (the ONLY blessed path, header lines 1-19)
1. `scripts/bump-build.mjs` → new buildNumber everywhere (release.sh:43-47).
2. `node web/build.mjs` — bakes v-badge + content-hashed cache version (line 50).
3. `rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle` (line 56) — **this is THE mechanism by which
   the native app acquires its bundled web copy.** Nothing else creates ios/WebBundle.
4. Native archive (lines 59-118):
   - `cleanup_release` trap armed on EXIT/INT/TERM BEFORE any mutation (lines 71-78) — restores the
     tracked `director-codes.json` and `ios/Podfile.lock` even on Ctrl-C mid-archive (PII guard, audit A4).
   - `cp ios/Pods/Manifest.lock ios/Podfile.lock` (line 80) — pod-guard workaround (Ruby 4.0.1 pod install broken).
   - Swaps gitignored `director-codes.private.json` (real phone numbers) over the committed-EMPTY
     `director-codes.json` (lines 82-88; consumed via `import directorCodes from "./director-codes.json"`
     at PdfReaderApp.tsx:38) — baked into the RN bundle for THIS archive only, then restored.
   - `xcodebuild ... clean archive` + `-exportArchive` with `ios/exportOptions.app-store.plist`
     (lines 90-97), IPA → `~/Desktop/SignoVivo-$BUILD.ipa` (line 99).
   - Optional hands-off TestFlight upload via `xcrun altool` when `scripts/asc-credentials.env`
     (gitignored) provides ASC_KEY_ID/ASC_ISSUER_ID (lines 105-117); else manual Transporter.
5. `npx wrangler pages deploy web/dist --project-name alvernia-reader --branch main --commit-dirty=true` (line 125).

Modes: `SKIP_NATIVE=1` = web-only refresh at a BUMPED version (still bumps + deploys prod).
`STAGING=1` (lines 30-47) = NO bump, NO native, deploy branch `staging` only — "physically incapable
of touching prod"; staging relay room selected client-side by `?env=staging` (web/src/lib/svRelayRoom.js).

### 3b. Web-only deploy — `npm run deploy:web` (package.json:14)
`node ./web/build.mjs && npx wrangler pages deploy web/dist --project-name alvernia-reader` —
**no `--branch` flag**: wrangler infers the current git branch, so from a dev branch this is a PREVIEW
deploy; from `main` it hits PROD without a version bump (badge stays stale). release.sh is explicit.

### 3c. Local native dev — `npm run ios` (package.json:8-9)
`preios` runs `check-book-consistency.mjs` + `bump-build.mjs` before EVERY `expo run:ios` —
i.e. every local simulator run bumps buildNumber and mutates version.json/pbxproj/Info.plist/app.json.

### 3d. Mesh (offline) bundle distribution — ios/SignoVivo/DirectorSyncModule.swift
- `currentBundleVersion` = the running app's `CFBundleVersion` (lines 97-99) — NOT the version of the
  web bundle actually installed/served.
- Director → new peer: `bundle_offer {version}` right after connect (lines 700-713; dispatch 1843-1849).
- Follower: requests iff `Int(offered) > Int(mine CFBundleVersion)` and no transfer in flight (715-728);
  watchdog-guarded in-flight flag (730-748).
- Director packs the SHIPPED `Bundle.main.resourceURL/WebBundle` (packWebBundle, lines 793-871) into a
  self-describing length-prefixed archive, streams via `MCSession.sendResource` (750-791).
- Follower unpacks to `Documents/WebBundle_new-<uuid>` with header-size bounds, path-traversal guard,
  per-file size verification, index.html floor >200 bytes, then atomic-ish swap into
  `Documents/WebBundle` (873-1053), emits `bundleUpdated`.
- JS side: `bundleUpdated` → re-resolve URI + remount WebView (PdfReaderApp.tsx:954-963).

### 3e. Sync-worker deploy (out-of-band)
`sync-worker/wrangler.jsonc`: name `signovivo-sync`, DO `SyncRoom` (sqlite migration v1),
deployed manually via `npx wrangler deploy`; `RELAY_DIRECTOR_TOKEN`/`TRANSMITTER_CODES` are secrets;
`ALLOWED_ORIGINS: "*"` still in vars with a "tighten in prod" TODO comment. No CI/CD for the worker.

### 3f. CI — .github/workflows/ci.yml (the ONLY CI)
- Triggers: every PR + push to main + manual. macOS runner (sips/poppler/cwebp needed), 25-min timeout,
  concurrency-cancel per ref.
- Steps: `brew install poppler webp` → `npm ci` → `npm run typecheck` → **named-file safe e2e subset**
  (9 files; NEVER the `test:e2e` glob — header comment reiterates the relay-sync prod hazard) →
  `node scripts/smoke-boot.mjs` (full fresh web build + artifact assertions).
- Excluded from CI: `e2e/relay-sync.test.mjs` (publishes to a relay) and `e2e/eas-config.test.mjs`
  (shells `npx expo config`, hangs, pins the banned EAS path — "slated for deletion in P1-CI").
- `scripts/check-book-consistency.mjs` is NOT in CI (only in `preios`) — see Oddity O7.

### 3g. Guards (what each checks)
- `scripts/smoke-boot.mjs` — the M0 "would this have caught Wednesday?" gate. Asserts on web/dist:
  shell files non-trivial (77-79); `#pages-data` parses to positive totalPages (84-102); page-001.webp
  and the LAST page exist non-empty (118, 151-155); **TRIPLE page-count consistency**
  inline == rendered == manifest (127-149, the build-325/song-370 class); native bridge markers
  `signovivo-native` + `bridge-ready` survived (159-162); NO unreplaced `__*__` tokens in app.js/sw.js
  (164-168); lib helpers shipped + wired (svRelayRoom defaults to `"alvernia-main"`, svSelftest,
  svSyncDecision `decideRelaySnapshot`) (173-186). `SMOKE_SKIP_BUILD=1` / `SMOKE_DIST` overrides.
- `scripts/check-book-consistency.mjs` — song index max page ≤ `pdfinfo` page count of
  `assets/alvernia_manual_2.pdf`; FAILS build if a song points past the end (lines 44-51); soft-warns
  if PDF has trailing unindexed pages; SKIPS (exit 0) if poppler missing (lines 33-43).

### 3h. Rollback mechanics
- Web: `scripts/rollback-web.sh` — READ-ONLY helper; lists Pages deployments, prints two paths:
  (A) dashboard "Rollback to this deployment" (instant, no rebuild; followers pick it up ~60s),
  (B) checkout good SHA → `node web/build.mjs` → `wrangler pages deploy --branch main`.
  wrangler 4.x has no `pages rollback` subcommand (header, lines 10-15). Verify in incognito (SW lag).
- Native: NO rollback path. TestFlight/App Store can only go forward; the mesh push also only moves
  followers FORWARD (`offered > mine`). A bad native build must be superseded by a higher build number.

---

## 4. CONTRACTS / INVARIANTS

1. **Lockstep invariant** (release.sh header): web + native are ONE artifact; `b<N>` overlay,
   the WebBundle in the IPA, and signovivo.com's `v<N>` must agree. Only release.sh preserves this.
2. `version.json` is the single version source; e2e pins enforce pbxproj/Info.plist/app.json agreement
   (native-entrypoint.test.mjs:54-65; eas-config.test.mjs:68-82).
3. OTA is hard-disabled everywhere: app.config.js `updates.enabled=false`, Expo.plist
   `EXUpdatesEnabled=false` (pinned by native-stability-config.test.mjs:22-31). The ONLY native web-content
   update channels are TestFlight/App Store and the mesh bundle push.
4. `totalPages` is derived from rendered output (web/build.mjs:640), and smoke-boot re-asserts the
   triple consistency — the whole "song N unreachable" class is guarded twice.
5. Wire fields `mode`/`bookId` are vestigial post-374 but MUST keep flowing (nearby-sync-contract pins
   them at every layer; relay persists them) — additive-only compat is the standing rule.
6. Relay room defaults to `alvernia-main`; ONLY `?env=staging` diverts (svRelayRoom.test.mjs), and the
   hostile-input default is the production room.
7. `npm run test:e2e` is forbidden (glob includes relay-sync + eas-config); CI runs named files only.
   NOTE: relay-sync.test.mjs is now env-gated — it THROWS at load without RELAY_TEST_BASE/ROOM/CODE and
   hard-refuses room `alvernia-main` (lines 30-45); the standing "publishes to prod with committed code"
   memory note describes the PRE-hardening state.
8. `package.json` scripts + devDeps are pinned EXACTLY by repo-minimal-footprint.test.mjs:15-34 —
   adding/renaming any npm script or devDep breaks CI until the pin is updated.
9. Committed `director-codes.json` must stay EMPTY (its own `_comment`); real codes exist only in the
   gitignored private file, baked in transiently by release.sh under a crash-safe trap.

---

## 5. WHAT EACH e2e FILE PINS (never run relay-sync; all others are static/source-regex or pure-unit)

| File | Pins |
|---|---|
| `eas-config.test.mjs` | eas.json `appVersionSource=local`, prod build store/channel; app identity (bundle id, slug, EAS projectId); required release assets exist; RESOLVED expo config version/runtimeVersion == version.json, updates disabled. ⚠ shells `npx expo config` (network/hang risk); excluded from CI; pins the EAS path Miguel banned. |
| `native-entrypoint.test.mjs` | entrypoint registration; app is a WebView shell (has `injectedJavaScriptBeforeContentLoaded`, `__signoVivoReceiveNativeEvent`; NO FlatList); song 55→page 55 in the canonical index; buildNumber > 0; pbxproj CURRENT_PROJECT_VERSION == version.json (all occurrences); Info.plist: exempt encryption, `_signovivo._tcp/_udp` Bonjour, LSMinimumSystemVersion 15.1, no NSAllowsArbitraryLoads; upside-down orientation on all devices; metro bundles html/bundle assetExts; Podfile props (newArch off, network inspector off); EXConstants wrapper in Podfile. |
| `native-stability-config.test.mjs` | newArch disabled (iOS pods + Android gradle); EX_DEV_CLIENT_NETWORK_INSPECTOR off; Expo.plist OTA disabled + no EXUpdatesURL; Info.plist RCTNewArchEnabled=false, no UIBackgroundModes. |
| `nearby-sync-contract.test.mjs` | The Multipeer WIRE CONTRACT across src/nearbyDirectorSync.js + .d.ts + ObjC bridge + Swift: page payload carries v/mode/bookId; requestCurrentSnapshot end-to-end (follower hello); promise-reject on unsupported platform; director snapshots on connect+hello; follower-is-sole-inviter (director only legacy-fallback invites); invite dedup (`pendingInvitePeer`); page state stored BEFORE empty-peers guard; 50-char MCPeerID cap; 1.5s one-shot follower snapshot probe (generation-guarded); discovery cadence 5s×6 burst → 25s steady, paused while connected, resumed on disconnect; reset generation guards on all timers; capacity 2 sessions × 7 followers; FOLLOWER_START_FAILED emission; memoryWarning event; emitState dedup; autoreleasepool in refreshDiscovery; protocol-version mismatch ignored; hello throttling. |
| `offline-books-integrity.test.mjs` | Canonical `src/alverniaManual2SongIndex.js`: parseable positive [song,page] pairs (regex identical to web/build.mjs), sane max page, and `assets/standard/` stays retired (2026-07-05 refactor, commit 87ee5a39). |
| `permission-flow.test.mjs` | Info.plist Multipeer permission keys (build-258 regression); `primePermissions` exists at JS/bridge/Swift with a retained, delegate-set browser (build-261 regression); director-takeover request/approve/deny wired through all layers. |
| `relay-sync.test.mjs` | Relay worker CONTRACT: health shape, 401s, 400 bad JSON, seq monotonic guard (stale/equal ignored), `seq=0` always-write mode, page clamped ≥1 + NaN coercion, mode/bookId persisted, dual-director higher-seq-wins, WS snapshot-on-connect / ping / live push, ts freshness (90s client constant), CORS/preflight. **DISABLED by default; throws without RELAY_TEST_* env; refuses room `alvernia-main`.** |
| `repo-minimal-footprint.test.mjs` | EXACT npm script list + EXACT devDeps; banned deps stay gone (expo-updates, react-native-pdf, …); web/src files exist; retired paths stay deleted (incl. `src/offlineWebBundle.d.ts`, `assets/standard/*.json`). |
| `svRelayRoom.test.mjs` | Pure unit: `resolveRelayRoom` → `alvernia-main` default / `alvernia-staging` only for `?env=staging`; never throws on hostile input; exported room constants. |
| `svSelftest.test.mjs` | Pure unit: `computeChecks` readiness card — page-1 load, relay reachability (incl. hang→timeout), totalPages validity, bridge check applicable only in native file mode, room shown loudly. |
| `svSyncDecision.test.mjs` | Pure unit: `decideRelaySnapshot` — reject malformed snapshots; demote seq≤0 or ts older than 90s (P2-SEQ "Wednesday freeze": stale duplicate seq must still demote); clock-skew offset helpers (P2-CLOCKSKEW). |

---

## 6. ODDITIES (smells for later audit lenses)

- **O1 — Stale peer-pushed bundle wins FOREVER after an app update.** `resolveBundleUri`
  (PdfReaderApp.tsx:813-826) unconditionally prefers `Documents/WebBundle`; NOTHING ever deletes or
  version-compares it against the shipped copy (no removeItem for WebBundle anywhere in PdfReaderApp.tsx;
  Swift only removes it transiently during a swap). Scenario: follower on build 377 receives a mesh-pushed
  380 bundle → later updates via TestFlight to 385 → the app still serves the OLD 380 web bundle from
  Documents, while `__SIGNO_VINO_NATIVE_BUNDLE_VERSION` (PdfReaderApp.tsx:1037) reports 385 — the badge
  LIES, and per O2 the mesh won't re-fix it. Disaster-class version-skew source.
- **O2 — Bundle-version compare uses the APP's CFBundleVersion, not the installed web bundle's version.**
  `currentBundleVersion` (DirectorSyncModule.swift:97-99) reads Info.plist. Two failure directions:
  (a) a follower that already received the director's newer bundle still reports its OLD app build →
  `offered > mine` stays true → it RE-DOWNLOADS the ~30 MB pack on every reconnect;
  (b) after an app update (O1) `mine` becomes ≥ offered, so the stale Documents copy is never refreshed.
  The header version inside the pack (`headerVersion`, .swift:988) is emitted but never persisted/consulted.
- **O3 — Director re-packs only the SHIPPED bundle.** `packWebBundle` reads `Bundle.main.resourceURL/WebBundle`
  (.swift:800-803), not Documents — a director that itself received a pushed newer bundle advertises a
  version (its CFBundleVersion) that can disagree with what it can actually serve.
- **O4 — Standalone archive scripts bypass the lockstep.** `scripts/testflight-upload.sh`,
  `scripts/testflight-upload-transporter.sh`, `scripts/submit-appstore.sh` run `xcodebuild archive`
  directly: NO bump, NO web rebuild, NO `web/dist → ios/WebBundle` sync, NO director-code bake/PII trap.
  An archive made through them ships whatever stale/absent `ios/WebBundle` is on disk, silently
  (and with the committed EMPTY director codes). Nothing asserts `ios/WebBundle == web/dist` at archive time.
- **O5 — `npm run deploy:web` has no `--branch`** (package.json:14): behavior depends on the current git
  branch (preview from dev branches, PROD from main) and skips the version bump — badge/version drift path.
  release.sh (line 125) is explicit; the npm script predates it.
- **O6 — Every `npm run ios` bumps the build number** (`preios`, package.json:8) — local dev runs mutate
  version.json/pbxproj/Info.plist/app.json, inflating the fleet-visible build counter and creating diff noise.
- **O7 — check-book-consistency.mjs is not in CI** — the song-index-vs-PDF page check runs only via `preios`
  (i.e., never on a web-only release path; release.sh doesn't call it either). smoke-boot covers page-count
  triple consistency but NOT "song points past the last page". It also exits 0 when poppler is missing.
- **O8 — bump-build.mjs lines 20-28 are dead** (`src/offlineWebBundle.js` deleted at build 304,
  commit 73174591) — harmless no-op, but misleading about how the native bundle version is kept in sync.
- **O9 — SKIP_NATIVE=1 institutionalizes web/native skew**: signovivo.com moves ahead of every installed
  native app's embedded bundle until the next TestFlight adoption or mesh push. Safe only while the
  relay/bridge wire contract stays strictly additive (the standing rule) — any breaking change on a
  web-only refresh strands the native fleet.
- **O10 — eas-config.test.mjs** pins the banned-for-personal-dev EAS path, is excluded from CI as
  hang-prone, and is "slated for deletion/conversion in P1-CI" (ci.yml header) — stale pin debt.
- **O11 — sync-worker `ALLOWED_ORIGINS: "*"`** still shipping with a "tighten in prod" comment
  (sync-worker/wrangler.jsonc vars); worker deploys are fully manual (`npx wrangler deploy`), no CI, no
  version tie to version.json — relay/client compat is enforced only by convention.
- **O12 — `--commit-dirty=true` on every Pages deploy** (release.sh:125): prod web deploys are legal from
  a dirty tree; the content-hash cache version (web/build.mjs:16-20) exists precisely because of this,
  but it means the deployed artifact may match NO commit — CLI rollback path B (rebuild a SHA) can be
  unable to reproduce the bad deploy for diagnosis.
- **O13 — eas.json commits ASC API key metadata** (`ascApiKeyId`, issuer id, an absolute local `.p8` path
  under /Users/cazares/Downloads). Not the private key itself, but identifier leakage + a machine-specific
  path in a tracked file.
- **O14 — memory-note drift**: the standing "relay-sync.test.mjs publishes to PROD with committed code"
  warning is now stale — current source is env-gated and refuses `alvernia-main` (relay-sync.test.mjs:30-45).
  The "NEVER run the full glob" rule still stands (eas-config hangs; relay-sync throws → red suite).
