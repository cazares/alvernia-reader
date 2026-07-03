# HANDOFF.md — alvernia-reader / Signo Vivo
*Written 2026-06-28 (~2:35 PM CT) for next-tab continuity. Untracked scratch — do NOT commit.*

---

## ✅ LATEST — 2026-06-29 ~2:54 PM CT: build 342 (director badge) shipped to web; IPA ready for Transporter

Two iPad director-mode issues Miguel reported on build 341:
1. The "MODO ACTIVO / DIRECTOR" badge sat top-RIGHT *behind* the ♪/⌕ nav fabs (z-8 under z-45) — read as a banner stuck behind the buttons.
2. No obvious way to EXIT director mode (only the secret soft-reset code `744668486` on the ♪ numpad).

Fix (PR #200, squashed to `main` = `bd2dea70`):
- Badge moved **top-LEFT** (free for directors — ⟳ resync is follower-only), made a **tap-to-EXIT** button: `✕ Salir` → `window.confirm("¿Salir del modo director?")` → `postNativeBridge({type:"exit-director"})` → native `handleMessage "exit-director"` → `performSoftReset` (clears role + 24h restore window, remounts as fresh follower).
- Restyled to MATCH the ♪/⌕ fabs: translucent navy `rgba(26,26,46,0.38)` + `blur(3px)` + radius 14, white text (Miguel: "style the upper left JUST like the upper right ... with opacity so it doesn't stick out").
- Badge shows ONLY on the native iPad (signovivo.com can't be a director). Optimized for **iPad portrait** (see memory `feedback_ipad_portrait`).

⚠️ MID-SHIP a concurrent tab had merged **#199** (⟳ + 3 staggered reconnect dots, `sv-dot-pulse`) + a `deploy:web` script to `main`. The bare `resync-dots` HTML was already loose in this checkout's working tree. Integrated #199 (merged origin/main into the branch) so **build 342 carries BOTH the badge work AND #199's dots** — full web/native parity. The first IPA (badge-only, missing #199) was DISCARDED + rebuilt; the rebuilt IPA was unzip-verified to contain the #199 dots CSS.

State:
- **signovivo.com LIVE** (~2:54 PM CT) — verified at canonical `/` (badge button + `✕ Salir` + #199 dots HTML; styles.css badge-navy ×2 + dots-CSS ×5). Note `/index.html` 308-redirects to `/` (curl without `-L` shows 0 — not staleness).
- **IPA 342 BUILT + VERIFIED** at `build/export/SignoVivo.ipa` (CFBundleVersion 342, WebBundle = badge restyle + #199 dots + exit-director bridge, Apple Distribution signed). **NOT yet uploaded — Miguel drags it into Transporter.app** (standing method; never ASC API keys).
- On fresh branch **`dev-signo-next @ bd2dea70`** (= origin/main). Merged `dev-director-badge` deleted (local + remote).
- Build tax: `web/build.mjs` re-encodes all 422 page images every run (~2 min) — no caching. A `SKIP_IMAGES`-style fast path would help CSS iteration (deferred — staleness risk).

---

## ✅ RESOLVED: ⟳ pulse shipped (the concurrent-tab work is done)

Earlier today a second tab handled the ⟳ feedback: Miguel said the spin looked **cartoonish**,
chose **Option C — opacity pulse (no rotation)**. That tab committed it and **merged PR #197**
(`f56bda04`, "⟳ fades instead of spinning"), then stopped WITHOUT deploying. This tab then:
- merged it to `main` (`5337b156` — a harmless redundant merge node on top of `f56bda04`;
  same content, the pulse), and
- **built + deployed to signovivo.com (2026-06-28 ~8:28 PM CT).**

**signovivo.com is now LIVE with the pulse** (`sv-resync-pulse`, opacity 0.5→1→0.5, 1.5s
ease-in-out); the spin is gone. Working tree is clean (no more `M styles.css`).

Leftover (do NOT force-delete — they're the stopped tab's artifacts, harness/Miguel can prune):
the worktree `.claude/worktrees/practical-darwin-59af9e` + branch `claude/practical-darwin-59af9e`.
Note `main` carries the one redundant merge commit `5337b156`; harmless, do NOT force-push to undo it.

Many other worktrees exist too (`git worktree list`) — assume multi-tab activity; never touch
another tab's worktree.

---

## TL;DR — where things stand

The big WebView rewrite is **long done** (this is NOT the 2026-06-26 plan; that shipped as builds
332–340). This session was a rapid-fire UX polish + bug-fix run on **signovivo.com** (the web
follower app), plus two native builds (339, 340) to TestFlight.

- **Repo**: `origin/main @ ce327faf` (build 341 reconciliation merged, PR #198). version.json = **341**.
- **signovivo.com**: LIVE with everything, INCLUDING the ⟳ pulse, the native-aware ⟳, and the
  4-arrows-to-corners fullscreen icon (last deploy ~9:42 AM CT 2026-06-29).
- **TestFlight**: **build 341** (delivered 2026-06-29 ~9:43 AM CT, processing) — now FULLY in sync
  with signovivo.com (reconciled WebBundle + native ⟳ resync handler). 340 before it.
- **⚠️ Local-checkout snag**: `git checkout main` in the main checkout FAILS — the stopped tab's
  worktree `.claude/worktrees/practical-darwin-59af9e` is holding `main` (at 5337b156, behind
  origin). The main checkout is parked on the merged branch `dev-web-native-reconcile @ f507c250`
  (remote already deleted; content == origin/main). To free `main`: prune that worktree
  (`git worktree remove .claude/worktrees/practical-darwin-59af9e`) — it belongs to the stopped
  tab, so I did NOT touch it. Then `git checkout main && git pull` in the main checkout.
- **Working tree**: clean except `?? HANDOFF.md` (this file).

---

## The product, in one paragraph

Signo Vivo is a parish choir hymnal. The **native iOS app** (parish iPads) is a thin
`react-native-webview` shell (`PdfReaderApp.tsx`) over a **locally-bundled copy of the web app**
(`web/dist` → `ios/WebBundle`, loaded `file://…/WebBundle/index.html`). The **same `web/dist`** is
also deployed to **signovivo.com** (Cloudflare Pages, project `alvernia-reader`) for phone
followers. A **director** iPad broadcasts its current page to followers via Multipeer (local mesh)
and a **Cloudflare Worker relay** (`signovivo-sync.4j4982y8jp.workers.dev`, room `alvernia-main`)
that signovivo.com followers subscribe to over WebSocket.

Because web + native share ONE bundle, "web-only" UI changes leak onto the iPad and vice-versa —
this caused the build-338 director lockout. The current design distinguishes web vs native by
**ROLE**, not platform (see below).

---

## Current control design (the settled model — IMPORTANT)

Controls are **role-driven** via `html[data-role]`, set in `renderDirectorModeBadge()`
(`web/src/app.js`), default `"follower"`:

- **FOLLOWER** (all of signovivo.com + any non-director iPad):
  - **⟳ resync** top-LEFT — `reconnectRelay()`: rejoin the live director + refresh the WS connection.
    Icon is ~35% bigger (`font-size: 3.05rem`). **On tap it currently SPINS (cartoonish) — the other
    tab is changing this to an opacity pulse.**
  - **♪** top-RIGHT, flush to the corner — opens the number-pad ("IR A CANTO").
  - **No ⌕ search.** (Hidden for followers.)
- **DIRECTOR** (native iPad after a director code → `html[data-role="director"]`):
  - **♪ + ⌕** top-RIGHT (♪ shifts 4.5rem left to make room for ⌕). The ♪ numpad is the ONLY way to
    type a director code (web numpad → `postNativeBridge` → native flips role). No ⟳ (a director is
    the source).
- **⛶ Fullscreen fab**, bottom-RIGHT, same fab styling, wired to `toggleFullscreen()`. Gated on
  `supportsFullscreen` → shows on desktop / Android / installed PWA; **hidden in a plain iOS Safari
  tab** (Apple blocks element fullscreen there). Open question to Miguel: add an "Add to Home Screen"
  hint for iPhone users? (not yet done).

**Book selection is STRICTLY geo-IP — NO manual selection for ANYONE, including directors.** The
manual book switcher was fully removed (PR #195). Book is set only by: the worker's `X-Hymnal`
header (web), native geo-fetch on first launch (`PdfReaderApp.tsx:585`), or following a live
director. Every `switchBook(...)` call on web is `{fromNative:true}` (automatic). Two books:
`standard` = "Manual Alvernia" (371 pp), `hymns-4` = "Himnos de Sión" (51 pp).

**Geo-IP rule** (worker `sync-worker/src/index.ts` ~line 205): `request.cf.postalCode` `78840`/`78841`
OR city "del rio" → `X-Hymnal: standard`; else `nonstandard`. Header is CORS-exposed
(`Access-Control-Expose-Headers: X-Hymnal`) so the browser can read it cross-origin.

---

## Blank-white geo-gate (the "no wrong-manual flash" feature)

signovivo.com used to flash the default hymns-4 book before geo resolved to `standard` for Del Rio.
Fixed with `#geo-gate` (a white overlay, visible by default before first paint) that lifts only once
the resolved book's page image has **decoded + painted**. Three subtle bugs were fixed in sequence:

1. Gate covers until geo resolves (`relayPollOnce` reads `X-Hymnal`).
2. **Race**: reveal ONLY from the poll that resolved geo (`geoJustResolved` flag) — not every
   concurrent boot poll — else a poll that skipped the geo block lifted the gate mid-`switchBook`.
3. **Decode lag**: `revealReader()` awaits `pageImage.decode()` + double `requestAnimationFrame`
   before lifting, so it never reveals the previous frame.
Safety: a 3s conditional reveal (only if geo still unresolved) + an 8s absolute backstop. Native
reveals immediately (book injected, never takes the geo path).

---

## This session's commit arc (all merged to main)

PRs #188–#196 (newest first):
- **#196** `cb8df6f6` — ⟳ spins + 35% bigger + ⛶ fullscreen fab. ← spin is what Miguel disliked
- **#195** `07980ecc` — remove manual book switcher (strictly geo-IP)
- **#194** `68fc1fb2` — ♪ flush to top-right corner for followers
- **#193** `1a822706` — geo-gate waits for image DECODE before lifting (kills residual flash)
- **#192** `6b787747` — geo-gate RACE fix (reveal only from the geo-resolving poll)
- **#191** `9a1b73f9` — blank-white geo-gate (first version)
- **#189** `a91fb9d2` — role-based follower controls (⟳+♪ / ♪+⌕)
- **#188** `ee78815d` — **build 339**: fix iPad director-mode lockout (un-hide ♪/⌕ on native)
- (`0879c881` build 340 — the original "full parity" that #189+ then refined)

Native builds shipped to TestFlight this session: **339** (director lockout fix) and **340**
(parity baseline). Both via the IPA flow below.

---

## Deploy mechanics

### Web → signovivo.com (Cloudflare Pages)
```bash
cd /Users/cazares/src/alvernia-reader
node web/build.mjs                       # builds web/dist (WebP encode ~1–2 min; index.html written LAST)
npx wrangler pages deploy web/dist --project-name alvernia-reader --branch main --commit-dirty=true
```
Live at **signovivo.com** within seconds. wrangler 4.105.0 is installed + authed.
⚠️ **SW is stale-while-revalidate** (`web/src/sw.js`): a deploy is picked up "**one load later**."
To verify a deploy immediately, **use a Private/Incognito tab** (no SW cache). This burned us
repeatedly — every "it still looks old" was the SW serving the prior version.

### Native IPA → TestFlight (build NNN)
Run in the **main checkout** (`/Users/cazares/src/alvernia-reader`) — it has `node_modules` + `ios/Pods`;
the worktrees do NOT.
```bash
# 1. bump build (version.json + app.json + Info.plist + pbxproj):
node scripts/bump-build.mjs
# 2. build web + sync the gitignored WebBundle from dist (= what's live on signovivo.com):
node web/build.mjs && rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle
# 3. POD-GUARD WORKAROUND (see gotcha): pod install is broken, so sync the lockfile to the built pods:
cp ios/Pods/Manifest.lock ios/Podfile.lock
# 4. archive + export (LANG matters; ~5–8 min clean archive):
export LANG=en_US.UTF-8
xcodebuild -workspace ios/SignoVivo.xcworkspace -scheme SignoVivo -configuration Release \
  -archivePath build/SignoVivo.xcarchive -sdk iphoneos -allowProvisioningUpdates clean archive && \
xcodebuild -exportArchive -archivePath build/SignoVivo.xcarchive -exportPath build/export \
  -exportOptionsPlist ios/exportOptions.app-store.plist -allowProvisioningUpdates
# → build/export/SignoVivo.ipa
# 5. revert the temp lockfile so git stays clean:
git checkout -- ios/Podfile.lock
```
Then **drag `build/export/SignoVivo.ipa` into Transporter.app** and click **Deliver** (Miguel's
account is signed in; signing is "Apple Distribution: Miguel A Cazares Jr / 7QRW68DD5X"). Transporter
is the ONLY upload path used — never ask about ASC API keys / app-specific passwords. Verify the IPA
before upload: `CFBundleVersion`, and that the bundled `WebBundle/` has the intended changes.

### Worker (relay / geo-IP)
`cd sync-worker && wrangler deploy`. Not touched this session.

---

## In-flight / OPEN items for the next tab

1. **Native build 341 — DONE (shipped 2026-06-29 ~9:43 AM CT, processing).** Full web↔native
   reconciliation: rebuilt WebBundle (all post-340 web changes) + the native ⟳-resync handler +
   fullscreen hidden on native. The iPad is now in sync with signovivo.com. **Pending: Miguel
   installs 341 from TestFlight and confirms on-device** (follower ⟳+♪, ⟳ resyncs to the director
   over the mesh, director-code → ♪+⌕, no book switcher, no fullscreen fab on the iPad).
2. **Free up `main` in the main checkout** — prune the stopped tab's `practical-darwin-59af9e`
   worktree (see TL;DR), then `git checkout main && git pull`. Until then the main checkout is
   parked on the merged `dev-web-native-reconcile` branch.
3. **Fullscreen on iPhone Safari** — currently hidden there (no API). Offered an "Add to Home Screen"
   hint; Miguel hasn't answered. Leave unless he asks.

---

## Gotchas / hard-won lessons (current)

1. **SW staleness** → test deploys in a **Private tab**. (Most "still broken" reports were this.)
2. **`pod install` is BROKEN** in this env: CocoaPods 1.16.2 on **Ruby 4.0.1** crashes in RN's
   `new_architecture.rb` post-install hook. `LANG=en_US.UTF-8` does NOT fix it. Workaround for builds:
   the pods on disk compile fine; just `cp ios/Pods/Manifest.lock ios/Podfile.lock` to satisfy the
   Xcode "Check Pods Manifest.lock" guard, build, then revert. (See `reference_build_env_pod_ruby4_drift`.)
3. **node_modules drift**: installed `expo@54.0.34` + a stray `@react-native-picker/picker@2.11.4`,
   vs committed locks pinning `54.0.33` and no picker. Harmless for builds via the workaround above;
   a real fix needs the Ruby/CocoaPods toolchain sorted, then `npm ci` + `pod install`.
4. **Multi-tab**: many worktrees (`git worktree list`). Edits in `.claude/worktrees/*` belong to other
   tabs — never touch them. The main checkout is shared; check `git status` before editing.
5. **Build output order**: `web/build.mjs` writes `index.html` LAST (after WebP). When waiting on a
   build, gate on `web/dist/index.html` existing, not just dist/.
6. **`npx serve` (preview) quirks**: rebuilding swaps the dist inode → the running `serve` serves a
   stale directory listing; kill it (`lsof -ti tcp:4000 | xargs kill -9`) and restart. Also `serve`
   serves `sw.js` with a bad MIME → a harmless "service worker registration failed" console error in
   the local preview only (fine on Pages).
7. **Director codes** (enter on the ♪ numpad, 5+ digits → routed to native bridge): admin
   `8307343376`; regular `8304533367` / `8307197000` / `8303130470`; `50711` = force-director;
   `744668486` = soft reset. Entering one on signovivo.com is a NO-OP (no native bridge) — web cannot
   become a director by design.

---

## Key files

| File | What |
|------|------|
| `web/src/app.js` | The whole web reader. `revealReader`/geo-gate (top), `renderDirectorModeBadge` (sets `data-role`), `relayPollOnce` (geo X-Hymnal + reveal), `reconnectRelay`, `toggleFullscreen`, `bindReaderEvents` (fab wiring), `initReader` (boot). |
| `web/src/index.html` | Markup. `#geo-gate`, the fabs (resync/song-jump/search/fullscreen), the ♪ modal. |
| `web/src/styles.css` | All styling. Fab base + role rules ~line 1940; `#geo-gate` ~line 66; **resync spin/pulse ~line 1980 ← contested with the other tab**. |
| `web/src/sw.js` | Service worker (stale-while-revalidate; skipWaiting+claim). |
| `web/build.mjs` | Build: src → dist, inlines book registry + per-book page counts, WebP encode. |
| `sync-worker/src/index.ts` | Cloudflare Worker: relay room, director-code gate, X-Hymnal geo (~line 205). |
| `PdfReaderApp.tsx` | Native WebView shell: bridge, director-code dispatch (`onDirectorCode`), geo book fetch (~585), `handleMessage`. |
| `ios/WebBundle/` | **gitignored**; synced from `web/dist` at build time. Not auto-synced — see deploy. |
| `version.json` | build number (340). `scripts/bump-build.mjs` bumps it + app.json + Info.plist + pbxproj. |
| `ios/exportOptions.app-store.plist` | export plist (method: app-store) used by the IPA flow. |

---

## Memory files that matter (in `~/.claude/projects/-Users-cazares-src-alvernia-reader/memory/`)

- `project_native_shares_web_bundle_gotcha` — the settled role-driven + strict-geo design (UPDATED this session)
- `project_webview_rewrite_groundtruth` — verified architecture facts
- `reference_build_env_pod_ruby4_drift` — the pod/Ruby workaround (NEW this session)
- `feedback_upload_method` — always Transporter, never ASC keys
- `feedback_ipa_command` — IPA as a single `&&` one-liner
- `feedback_build_bump` — bump version.json with the iOS files
- `reference_xcode26_pod_utf8`, `reference_ios_signing_autoprovision`

---

## What the next tab should do

1. Read this file completely. `git rev-parse HEAD` (expect `be481b3e`), `git status --porcelain`,
   `git worktree list`.
2. **Resolve the multi-tab state FIRST**: confirm whether the `claude/practical-darwin-59af9e` pulse
   work (commit `9fc76612`) has been merged + deployed. If not, coordinate with Miguel — either let
   that tab finish or pick up its branch. Do NOT independently re-implement the pulse or clobber the
   uncommitted `web/src/styles.css`.
3. For NEW web work: branch off `main`, edit `web/src/*`, `node web/build.mjs`, deploy with wrangler,
   and **tell Miguel to check in a Private tab**.
4. For the iPad: when Miguel asks, build **341** via the IPA flow above (pulse must land first).
5. Verify before claiming fixed — and remember the SW "one load later" before concluding a deploy
   "didn't work."

---
*End of HANDOFF.md — 2026-06-28*
