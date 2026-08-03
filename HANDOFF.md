# HANDOFF — SignoVivo / alvernia-reader

*Rewritten 2026-08-02 (~1:15 PM CT). Supersedes the 2026-06-28 (build 342) handoff entirely —
that one predated the whole caching architecture and would mislead a cold tab.*

---

## 0. TL;DR — state right now

| | State |
|---|---|
| **Repo HEAD** | `main` @ `3b4994f` — everything merged, no open PRs |
| **Web prod** | signovivo.com **v383**, 372-page stamped book, verified live |
| **Native** | `~/Desktop/SignoVivo-383.ipa` (44.9 MB, CFBundleVersion 383) → uploaded via Transporter; Miguel clicks **Deliver**, then adds build 383 to the choir group in App Store Connect |
| **Staging** | in sync with prod (372 book) |

The point of this session: **the director hands Miguel a new PDF; every device must actually show it.**
That now works on web, and the native build carrying the same book is in TestFlight.

---

## 1. What this session did

Started as "append a canary page 372 and ship it." Became an excavation of the whole distribution path.

### The bug found and fixed (PR #278)

`CACHE_VERSION` hashed only shell files — **never a byte of the book** — and the service worker
served previous-version page caches **before** the network. So a page revised **in place** under an
unchanged `page-NNN.webp` filename **never reached an already-cached device**: even the re-precache
was answered from the device's own stale cache, copying old bytes forward forever.

**Appending pages worked. Correcting one didn't.** Build 377 re-rendered ~290 pages in place; none
of those corrections ever arrived anywhere.

The fix, three parts:

1. **`BOOK_VERSION`** — `sha256(source PDF + render DPI + webp quality)`, stamped by `web/build.mjs`.
   Keys the page cache, offline-ready flag, and offline metadata **separately** from the shell's
   `CACHE_VERSION`. Shell deploys no longer re-download 25 MB; any book change starts a fresh cache
   and propagates because the token changes the emitted `sw.js` bytes.
2. **Network-before-previous-editions** — on a page-cache miss the SW races the network on TTFB
   (3 s), streams the winner, caches in the background, and only then falls back to a previous
   edition (tagged `X-SV-Prev-Edition`). Offline fetches reject in ms → church behavior unchanged.
3. **The reader/writer cache contract** — see §3.1. Most likely thing to be broken by accident.

Hardened through **7 adversarial rounds** (findings 9 → 7 → 5 → 4 → 3 → 1 → 1; 30 total, all fixed,
each verified live in a browser). Round 1 caught the fix re-shipping the original bug through the
precache's side door.

### Also shipped

- **Edition stamp on the title page** — `1 de agosto de 2026 · 372 páginas`, via
  `scripts/stamp-book-date.mjs`. Stamped **into the PDF**, so rendered pages, the AirDropped
  fallback copy, and printouts all inherit it. Anyone can now see *which* songbook a device renders.
- **`scripts/append-number-page.mjs`** (#282) — makes a canary/rehearsal page (giant number, appended).
- **`release.sh` book-consistency preflight** (#275) — the guard existed but had **never** run on the release path.
- **Native fleet prompt removed** (#276) — #270 removed the web copy only; the native `Alert.prompt`
  still fired on every fresh install. Found by *running* the app, not reading it.
- **Lockstep bump records** #281 (v382 web) and #283 (v383 web+native).

---

## 2. Rehearsal results — both update paths proven (2026-08-02 PM)

A full 372 → 373-appended update was exercised end to end:

- **Web OTA (staging only):** a client fully cached on the 372 book **converged on reload 1** — new
  SW took over, 373 pages cached, old cache retained as the offline fallback.
- **Native TestFlight motion (simulator):** build with 372 baked → install build with 373 baked
  **over** it (container preserved — the real TF update motion) → `♪999` flipped 372 → 373 cleanly.
- **🚨 The D1 trap, demonstrated live:** planting a stale `Documents/WebBundle` made the **same 373
  binary silently render the 372 book** — badge unchanged, no error. Deleting the folder recovered it.

Staging was restored to the 372 book; the 373 test book was never committed.

---

## 3. ⚠️ INVARIANTS — break these and the choir silently stops getting updates

### 3.1 The `no-store` reader/writer contract

**`{cache: "no-store"}` marks cache WRITERS and network PROBES only. Display READERS use the
default cache mode.**

- **Writers:** `cacheAssetsNoStore` (shell healer + strict manifest step), `cacheSinglePage`, the
  `?selftest` connectivity probes. `sw.js` passes these straight to the network — verified-fresh
  bytes or an honest failure.
- **Readers:** song-index hydration (runs every web boot), search-index load, boot manifest
  fallback. These go through the SW's network-first branch — fresh online, **cached offline**.

Put `no-store` on a reader and offline devices lose the song index: every numpad jump lands on the
**last page**, titles/browse/search die. That regression shipped and was caught in round 4.
`scripts/smoke-boot.mjs` pins all four halves — if a pin fails, do **not** loosen the pin.

### 3.2 Other load-bearing behavior

- **activate's page-cache keep policy scores unique page COVERAGE**, not raw entry count (legacy
  pre-347 caches hold `?retry=` duplicates and would otherwise win forever), and never keeps an
  empty cache.
- **Manifests must survive the deploy boundary** — they're in `CORE_ASSETS` (install) *and* salvaged
  in `activate` from a retiring static cache. `STATIC_CACHE` rotates every deploy; without both, an
  offline boot has pages but no song index.
- **Shell heal is transactional but best-effort**: zero fetches when complete; any gap refetches the
  *whole* shell (fetch-all-then-put-all, so it can't tear); failure warns and returns — **never**
  blocks the page precache. An icon must never outrank the book.
- **`CORE_ASSETS` (sw.js) and `SHELL_ASSETS` (app.js) must stay in step** — a mismatch makes the
  healer find a guaranteed gap after every deploy.

---

## 4. Deploy mechanics

- **Merging ≠ deployed.** Prod moves **only** when `scripts/release.sh` runs.
- `STAGING=1 bash scripts/release.sh` → isolated Pages preview branch. **Physically cannot touch
  prod** (implies no-bump + no-native). Verify at https://staging.alvernia-reader.pages.dev
- `SKIP_NATIVE=1 bash scripts/release.sh` → web-only prod deploy, no Xcode needed.
- `bash scripts/release.sh` → full lockstep: bump → web build → **native archive + export** → web
  prod deploy. IPA lands at `~/Desktop/SignoVivo-<N>.ipa`.
- **Cloudflare edge takes ~30–60 s to propagate.** A fresh deploy legitimately reads stale for a
  minute — check the unique deployment URL to distinguish propagation from failure.
- **Always commit the version bump afterward** (`version.json`, `app.json`, `Info.plist`, `pbxproj`)
  as a lockstep record — see #281/#283 for the pattern.
- **TestFlight upload:** `open -a Transporter ~/Desktop/SignoVivo-<N>.ipa`, then Miguel clicks
  **Deliver**. Never tell him to drag the file. (`scripts/asc-credentials.env` does not exist, so
  there is no automatic upload.)

### Gitignored files the build needs

- **`director-codes.private.json`** — real phone numbers; lives **only in the main checkout**.
  `release.sh` **warns but proceeds** without it, producing a build where nobody can become director
  (the build-371 "no director all night" outage class). **Copy it into the worktree before any archive.**
- The worktree guard **blocks cross-checkout writes**, and its `CLAUDE_ALLOW_SHARED_CHECKOUT=1`
  hatch is read from the hook's own environment — **Claude cannot do this copy.** Give Miguel the
  one-line `cp` and wait for confirmation.

---

## 5. Environment facts (these CORRECT older notes)

- ✅ **Xcode 26.6 installed and selected** (`/var/db/xcode_select_link` → Xcode). Miguel ran
  `sudo xcode-select -s /Applications/Xcode.app/Contents/Developer` on 2026-08-02.
- ✅ **`pod install` WORKS** (~12 s, 83 pods). The old "broken under Ruby 4.0.1" note was a **PATH
  artifact** — `pod` runs under system ruby **2.6.10**. Use `export LANG=en_US.UTF-8`.
- ✅ In a fresh worktree, `npm ci` + `pod install` yields `Manifest.lock` == committed
  `Podfile.lock`, so the `cp Manifest.lock Podfile.lock` workaround at `release.sh:80` is
  unnecessary there. The main checkout's `node_modules` are still drifted — **build from a fresh worktree.**
- A fresh worktree starts with **no** `node_modules`, `ios/Pods`, or `ios/WebBundle` (all gitignored).

---

## 6. Hard-won lessons

1. **Never modify `assets/` while a build may be running.** A mid-render PDF rewrite corrupted
   exactly one page (pure-white webp) and perfectly mimicked an app regression — an innocent commit
   was nearly blamed. Check the artifact before blaming code.
2. **Test through the real code path.** A check that manifests worked offline used a *default-mode*
   fetch while the app's reader used `no-store` — it validated an assumption, not the truth.
3. **The re-hunt is not optional.** Round 1's fix re-shipped the original bug; rounds 2–7 each found
   defects *in the previous round's fixes*.
4. **Run the app.** The native fleet prompt (#276) was invisible to every static check because the
   docs claimed it was already gone.
5. **`set -e` is suppressed inside the left operand of `||`** — a negative test written that way
   reports a false pass.

---

## 7. What is NOT done — pick up here

### 7.1 The one budgeted native build (main outstanding work)

Decided but unbuilt; all designed to ride **one** IPA:

- **D1: the `resolveBundleUri` fix.** `PdfReaderApp.tsx:~812` prefers `Documents/WebBundle`
  **unconditionally** — no version check — and nothing ever deletes it. **This trap is LIVE in build
  383** (demonstrated §2). A device that ever received a mesh push renders the old book forever,
  shows the new build number, and is ineligible for a corrective push (`offered > mine` fails).
- **«DIRIGIR»** — the decided director-access model; see `docs/director-access-design.md`. Native
  checks `utsname.machine`; on `iPad7,1`/`iPad7,2` (Braulio's iPad Pro 12.9" 2nd gen — nobody else
  has that model) it injects a flag and the web renders a one-tap **DIRIGIR** button driving the
  existing `becomeDirector` flow with a baked code supplied internally. Zero memory, nothing
  observable. The numpad code path stays as the written-down emergency lane (laminated card).
- **Three small floor repairs** cited in that doc: wrong-code silence, failed-mesh-start badge,
  silent demotion.

### 7.2 Open questions Miguel has not answered

From `docs/choir-pdf-distribution-plan.md` §11:

- **Annual refresh build?** The baked in-IPA book is the floor both self-heal layers land on and it
  freezes forever. A self-healing **director** would silently drop the whole choir to an old
  songbook. Recommendation: a routine ~annual build whose only content is a current `WebBundle`.
- The **trigger word** for the fallback protocol; **who the second operator is**; and whether
  **FOLWEB-07** (making out-of-range numpad entries error) needs a carve-out, since it would kill
  the `♪999` group check.

### 7.3 Spawned chip sessions

- `isOfflineBundleReady` dead-code chain → resolved in **#279** (became a real measurement).
- Icon precache payload → **#280** (~1.5 MB → ~0.6 MB).
- **Fleet-dashboard "Actualizando libro" state — may still be open.** Check
  `sync-worker/src/index.ts`: a device mid-migration reports `webCached=false, pagesCached=0` and
  may still render as "Onboarding pendiente", telling the operator to re-invite a healthy device.
  Deploying the worker is out-of-band (`cd sync-worker && npx wrangler deploy`) and needs Miguel's
  go-ahead — it is live prod infrastructure.

---

## 8. Orientation for a cold tab

- **What it is:** a Catholic parish choir songbook used LIVE during Mass. ~8 people, 6–8
  **personally-owned** iPads (no MDM), plus phones. One permanent director: **Braulio Figueroa**.
- **THERE IS NO INTERNET INSIDE THE CHURCH.** Page sync at Mass is a **Multipeer mesh**
  (Bluetooth / peer-to-peer wifi, native only). The Cloudflare relay needs internet and does **not**
  work there. Nothing can be pushed, rolled back, or looked up during Mass.
- **Stale beats blank. A blank page during Mass is the worst outcome.**
- **Architecture:** one web bundle (`web/src/`) deployed to signovivo.com (Cloudflare Pages) **and**
  copied into `ios/WebBundle` inside the IPA. The native app is a thin RN shell whose WKWebView
  loads that bundle from `file://`. `NATIVE_FILE_MODE` is injected by the shell — the SW is **not**
  registered on native.
- **Remote-loading signovivo.com in the WebView was tested and rejected:** offline cold launch gives
  `NSURLErrorDomain -1004` despite a full cache; service workers need both `WKAppBoundDomains` *and*
  `limitsNavigationsToAppBoundDomains`; `navigator.storage.persist()` returns false on iOS. **Keep `file://`.**
- **The mesh supports 14 followers** (two lazily-allocated MCSessions of 7), not 7 — an older doc
  claim of "zero headroom at 8 iPads" was wrong; corrected in `docs/director-access-design.md`.

### 🚫 Never do

- **Never run `npm run test:e2e`** — `e2e/relay-sync.test.mjs` publishes to the **PRODUCTION relay
  room** and would flip live followers' pages. Run the safe subset individually:
  `repo-minimal-footprint`, `native-entrypoint`, `native-stability-config`,
  `offline-books-integrity`, `nearby-sync-contract`, `permission-flow`, `svRelayRoom`, `svSelftest`,
  `svSyncDecision` (94 tests, green at HEAD).
- Never `git checkout` in the shared main checkout — other tabs live there.
- Never print or commit the contents of `director-codes.private.json`.

### Docs + memory

- `docs/choir-pdf-distribution-plan.md` — the OTA plan (13-agent design + 23-attack red team).
  Partly **superseded** by what shipped; its §2 native-downloader design is still the reference for
  the native book-update path.
- `docs/director-access-design.md` — the «DIRIGIR» decision.
- `docs/pre-mass-checklist.md`, `docs/ia-audit-2026-07.md`, `docs/app-hardening-plan.md`.
- Memory: `project_caching_layers_distribution` (six cache layers, what's actually armed, the native
  trap, deploy + rehearsal history), `reference_build_env_pod_ruby4_drift` (toolchain — RESOLVED),
  `feedback_never_run_full_e2e`, `feedback_upload_method`, `project_offline_director_outage_build371`.

---

## 9. The operator flow this session created

When the director hands Miguel a new PDF:

```bash
# 1. replace assets/signo_vivo_372.pdf with the new book, then stamp it:
node scripts/stamp-book-date.mjs --pdf assets/signo_vivo_372.pdf
# 2. rehearse on the isolated preview:
STAGING=1 bash scripts/release.sh      # → staging.alvernia-reader.pages.dev
# 3. promote:
SKIP_NATIVE=1 bash scripts/release.sh  # web-only, no Xcode needed
```

Every online device picks it up on next open — **including corrections to existing pages**, which
never worked before this session. The title-page date confirms which edition a device holds.

**The fleet check, no internet required:** the director jumps to the last page (or `♪ → 999 →
Abrir`). Every device showing the current stamp/number is current; anything else is stale. The build
badge in the corner is the **shell's** number and cannot answer this.
