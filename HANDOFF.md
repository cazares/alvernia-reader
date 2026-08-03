# HANDOFF — SignoVivo / alvernia-reader

*Rewritten 2026-08-03 (~11:15 AM CT). Supersedes the 2026-08-02 handoff. That one was written
before the native OTA work landed and lists M1/M2/M4/M5 as outstanding — they are shipped.*

---

## 0. TL;DR — state right now

| | State |
|---|---|
| **Repo HEAD** | `main` @ `043b298` — everything merged, **no open PRs** |
| **Web prod** | signovivo.com **v384**, **373-page** book, verified live. ⚠️ v384 predates M1, so prod serves **NO `bundle-manifest.json` yet** — see §6.2 |
| **Native / TestFlight** | still **build 383**, carrying the **372-page** book. **It has none of the work below.** An IPA sits uploaded-but-undelivered in Transporter — **do NOT tell Miguel to Deliver it, see §6.0** |
| **The one budgeted build** | **NOT CUT YET.** That is the next real step — see §6.1 |
| **Downloader** | shipped **DORMANT**. Changes nothing on any device until deliberately armed |

Two sessions of work landed: a production OTA rehearsal (#285) and the native OTA capability
(#286). A parallel chip session landed #287.

---

## 1. What the last session did

### 1a. Shipped a real 373-page book to production (#285)

Appended a canary page **373** and refreshed the title-page edition stamp
(`2 de agosto de 2026 · 373 páginas`), then deployed **web-only** to signovivo.com at
**2026-08-02 11:08:19 PM CT**. No IPA, no TestFlight — `SKIP_NATIVE=1`.

Propagation was **proven, not assumed**: a browser was primed against signovivo.com *before* the
deploy until it held the complete 372 book (372/372 pages cached — the exact device state the
pre-#278 bug stranded forever). After the deploy, **one reload**:

- inline `totalPages` 372 → 373
- new page cache 373/373, zero missing; **previous edition retained** as the offline fallback
- cached `pages.json` → 373 with 315 songs, i.e. the song index survived the deploy boundary

Then driven as the director drives it: `♪ → 999` lands on a page reading **373**; `♪ → 371` opens
"371. Santo Español"; the browse index lists all 315 songs.

### 1b. Built the native OTA capability (#286) — M1, M2, M4, M5

The plan is `docs/choir-pdf-distribution-plan.md`. **M3 (MODO EMERGENCIA / DIAGNÓSTICO) and
DIRIGIR were deliberately deferred** by Miguel to keep the unproven surface small.

- **M1** — `bundle-manifest.json` over the finished `web/dist`, plus the **additive-only gate** on
  the *publish* path (`release.sh`) as well as CI. Makes "which songbook is this device rendering?"
  answerable at all.
- **M2** — the **D1** stale-bundle fix and the **D2** self-heal ladder. Worth the build on its own.
  Verified on a simulator, not just in tests.
- **M4** — the Multipeer peer bundle-push **retired** at its receive boundary (Miguel's answer to
  the plan's open Q4: RETIRE).
- **M5** — `src/bookUpdate.js`, the HTTPS downloader, **shipped dormant**.

### 1c. #287 (a parallel chip session, already merged)

`fix(sw): never cache the SPA fallback as a page image, and heal slots already poisoned`.
Cloudflare Pages answers a *missing* page image with `200 text/html` (the SPA fallback), and the
service worker would store that HTML under the page's URL — permanently breaking that page number
on that device, because the page branch is cache-first with no revalidation. Fixed + healed.

---

## 2. ⚠️ INVARIANTS — break these and the choir silently stops getting updates

### 2.1 The `no-store` reader/writer contract

**`{cache: "no-store"}` marks cache WRITERS and network PROBES only. Display READERS use the
default cache mode.** Put `no-store` on a reader and offline devices lose the song index: every
numpad jump lands on the **last page**, titles/browse/search die. That regression shipped once and
was caught in round 4 of #278. `scripts/smoke-boot.mjs` pins all four halves — if a pin fails, do
**not** loosen the pin.

### 2.2 ADDITIVE-ONLY: the songbook only ever GROWS AT THE END

Existing page numbers are permanent and the bytes behind them never change. That is what keeps
every stale offline copy valid forever — an AirDropped PDF, an SW page cache, a previous-edition
fallback cache, a staged native download. **It has been violated once in production** (build 377 /
PR #257 re-rendered ~290 pages in place; nothing fired).

`scripts/additive-gate.mjs` now enforces it on **both** CI and `release.sh`. Read its header before
touching it — every rule in it is load-bearing and each has a story.

### 2.3 The page-URL pad width is FROZEN at 3

`web/build.mjs` and `web/src/app.js` both declare `PAGE_PAD_WIDTH = 3`. It used to be derived from
the page count (`String(totalPages).length`), which is stable only inside one decade of digits: the
first book to reach 1000 pages would have renamed **every** page image at once — `page-001.webp` →
`page-0001.webp` — invalidating the entire installed base in a single deploy. `smoke-boot` pins both
sides and the gate fails on any change.

### 2.4 Other load-bearing behavior

- `activate`'s page-cache keep policy scores unique page **coverage**, not raw entry count.
- **Manifests must survive the deploy boundary** — in `CORE_ASSETS` (install) *and* salvaged in
  `activate`. `STATIC_CACHE` rotates every deploy.
- Shell heal is transactional but best-effort; it must **never** block the page precache.
- `CORE_ASSETS` (sw.js) and `SHELL_ASSETS` (app.js) must stay in step.

---

## 3. Deploy mechanics

- **Merging ≠ deployed.** Prod moves **only** when `scripts/release.sh` runs.
- `STAGING=1 bash scripts/release.sh` → isolated Pages preview. **Physically cannot touch prod.**
- `SKIP_NATIVE=1 bash scripts/release.sh` → web-only prod deploy, no Xcode needed. **It now leaves
  `ios/WebBundle` untouched** (it used to rewrite it), so a later hand-run Xcode build cannot bake a
  bundle no archive in this pipeline produced.
- `bash scripts/release.sh` → full lockstep: bump → web build → **native archive + export** → web
  prod deploy. IPA lands at `~/Desktop/SignoVivo-<N>.ipa`.
- **`release.sh` now runs publish gates** before deploying: `smoke-boot`, `additive-gate`,
  `check-book-consistency`. If one reds, **do not bypass it** — read what it says.
- **Cloudflare edge propagation measured at ~3 minutes** (older notes say 30–60 s — optimistic). The
  unique deployment URL is current immediately; the alias lags. Check the deployment URL to tell
  propagation from failure.
- Always commit the version bump afterward as a lockstep record (see #281/#283/#285).
- **TestFlight:** `open -a Transporter ~/Desktop/SignoVivo-<N>.ipa`, then Miguel clicks **Deliver**.
  Never tell him to drag the file. There are no ASC API creds.
- **The worker deploys separately**: `cd sync-worker && npx wrangler deploy`. It is live prod
  infrastructure — get Miguel's go-ahead.

### Gitignored files the build needs

- **`director-codes.private.json`** — real phone numbers; lives **only in the main checkout**.
  `release.sh` **warns but proceeds** without it, producing a build where nobody can become director
  (the build-371 "no director all night" outage class). **Copy it into the worktree before any
  archive.** The worktree guard blocks cross-checkout writes and Claude cannot do this copy — give
  Miguel the one-line `cp` and wait for confirmation.
- `ios/WebBundle/` is gitignored and materialized only by `release.sh`. A fresh worktree has none;
  `cp -R web/dist ios/WebBundle` after a build if you need to run on a simulator.

---

## 4. Environment facts (verified 2026-08-03, in a fresh worktree)

- ✅ **Xcode 26.6 installed and selected.** The plan doc's §3.1 and its OPEN RISK 3 call the
  toolchain a blocker — **that is STALE**. `xcodebuild -version` works and an IPA was cut.
- ✅ **`npm ci` works and leaves `package-lock.json` untouched.** Same for `sync-worker/`.
- ✅ **`pod install` works** (84 deps / 83 pods, `export LANG=en_US.UTF-8`) and yields
  `Manifest.lock` **identical** to the committed `ios/Podfile.lock` — so the `cp Manifest.lock
  Podfile.lock` workaround in `release.sh` is unnecessary in a fresh worktree.
- ✅ A Debug simulator build takes ~2.5 min cold, ~20 s incremental. **Debug needs Metro**
  (`npx expo start`); there is no embedded JS bundle.
- ✅ The web build renders 373 pages in ~4 min and is **deterministic** — two consecutive builds
  produce byte-identical output and the same `bookVersion`.

---

## 5. Hard-won lessons

1. **A gate that compares an artifact to itself proves nothing.** The first committed
   `web/manifest-baseline.json` was generated from a `web/dist` whose `page-005.webp` had been
   hand-mutated by the gate's own mutation test. The file was restored; the manifest was not
   regenerated. It claimed 97945 bytes for a 97944-byte page and **read green for four commits**,
   because it was only ever compared against the manifest it came from. Production's own bytes
   settled it. The gate now re-hashes every file a manifest lists.
2. **CI renders with a different encoder than the build Mac.** `ci.yml` installs poppler/webp
   **unpinned**: pdftoppm 26.07.0 (CI) vs 26.04.0 (Mac) changed exactly **one page of 373**. Hence
   `--allow-renderer-drift`, which CI passes and `release.sh` deliberately does not — byte-identity
   is skipped only when the renderers actually differ; every renderer-independent invariant (page
   removed, book shrank, song moved, pad width) still fires.
3. **Run the app.** D1 was invisible to every static check for months. It was only ever caught by
   planting a stale bundle on a simulator. `scripts/sim-bundle-lab.mjs` makes that one command.
4. **A "clean" helper that silently finds nothing is worse than no helper.** `sim-bundle-lab
   --state clean` guessed the AsyncStorage path, deleted directories that did not exist, and
   reported success — so every later run inherited the previous run's state. It now searches for the
   store and reports what it wiped.
5. **HTTP 200 is not evidence a file exists.** Cloudflare Pages serves the SPA fallback for
   unmatched paths, so a missing `page-NNN.webp` returns `200 text/html`. Check magic bytes.
6. **Never modify `assets/` while a build may be running.** A mid-render PDF rewrite corrupted
   exactly one page and perfectly mimicked an app regression.
7. **`set -e` is suppressed inside the left operand of `||`** — a negative test written that way
   reports a false pass.

---

## 6. What is NOT done — pick up here

### 6.0 ⚠️ Do NOT Deliver build 383

`~/Desktop/SignoVivo-383.ipa` was uploaded to Transporter in an earlier session and Miguel
never clicked **Deliver**. Leave it that way unless he says otherwise.

Clicking Deliver would push build 383 to the choir, and 383:

- carries the **372-page** book, not the 373-page one that is live on the web
- contains **none** of M1/M2/M4/M5
- still has the **D1 stale-bundle trap live** — a device that once took a mesh push renders
  that old songbook forever while displaying the current build number

Delivering it now would also make the choir do **two** TestFlight installs (383, then the
budgeted build) instead of one, and the first would hand them a known trap. Recommendation
given to Miguel on 2026-08-03 and he chose to go straight to the budgeted build: **skip 383
entirely**; §6.1 supersedes it.

*Note "Deliver" is the BINARY upload, not the OTA. The OTA is what the budgeted build makes
possible afterwards — one last TestFlight round, then never again.*

### 6.1 THE ONE BUDGETED NATIVE BUILD (the main outstanding work)

Everything it needs to carry is **already merged**. What remains is cutting it:

```bash
# in a FRESH worktree off origin/main, with director-codes.private.json copied in (§3)
npm ci && (cd ios && LANG=en_US.UTF-8 pod install)
bash scripts/release.sh                              # bump + web build + native archive + web prod deploy
open -a Transporter ~/Desktop/SignoVivo-<N>.ipa      # Miguel clicks DELIVER
```

Then add the build to the choir group in App Store Connect.

**Ask Miguel before running this** — it deploys web prod as a side effect and spends the budgeted
TestFlight round.

### 6.2 Arming the downloader — the rollout, in order

It ships **dormant**: `sync-worker/wrangler.jsonc` has `BOOK_UPDATE_VERSION: ""`, so the
`bookUpdate` field never appears in any `/fleet/checkin` response and no client has anything to act
on. Merging changed nothing on any device.

0. ⚠️ **PROD HAS NO MANIFEST YET.** v384 was deployed before M1 existed, so
   `https://signovivo.com/bundle-manifest.json` currently returns `200 text/html` (the Pages SPA
   fallback — see lesson 5; a 200 is not evidence the file exists). **A web deploy from current
   `main` must happen before any of this works.** `bash scripts/release.sh` does it as part of
   §6.1; `SKIP_NATIVE=1 bash scripts/release.sh` does it web-only.
1. Then read the target from `https://signovivo.com/bundle-manifest.json` → `bookVersion`. Confirm
   it is real JSON, not the HTML fallback.
2. Read the device id from the fleet dashboard's new **Libro** column.
3. Set `BOOK_UPDATE_VERSION` + `BOOK_UPDATE_DEVICES="<that ONE id>"`, then
   `cd sync-worker && npx wrangler deploy`.
4. Watch that one device's Libro cell go `downloading:NN%` → `ready` → `active`.
5. **A HUMAN opens a named new song and confirms it out loud.** No hash can catch a
   wrong-but-well-formed PDF; this is the only check independent of the build.
6. Only then fleet-wide: `BOOK_UPDATE_DEVICES="*"` **and** `BOOK_UPDATE_ALLOW_FLEET="yes"` (both
   required — one alone does nothing), redeploy.

**Abort:** set `BOOK_UPDATE_VERSION` back to `""` and redeploy. ~20 seconds, no iPad needed. Devices
already holding a staged copy delete it on the next check-in that stops naming their version.

**Apply on the device** is the numpad code `265134902`. `907315268` forces the code-signed bundle
(auto-expires after 24 h). Both are Levenshtein distance 9 from `SOFT_RESET_CODE` and from each
other; a test reads them from source and fails if a future code lands within 3.

### 6.3 Deferred by choice — M3 and DIRIGIR

- **MODO EMERGENCIA + DIAGNÓSTICO + the native strip** (plan M3). Miguel chose "numpad code only,
  no permanent chrome" for this build and asked to see a mockup of the strip before it ships. Note
  DIAGNÓSTICO was also where the plan put "clear the LIBRO ANTERIOR banner" — as a stopgap the
  banner itself is tappable-with-confirm.
- **«DIRIGIR»** — `docs/director-access-design.md`. Native checks `utsname.machine`; on
  `iPad7,1`/`iPad7,2` (Braulio's iPad Pro 12.9" 2nd gen) it injects a flag and the web renders a
  one-tap DIRIGIR button driving the existing `becomeDirector` flow.

### 6.4 Open questions Miguel has not answered

From `docs/choir-pdf-distribution-plan.md` §11. Answered so far: **Q4 = retire the mesh push**,
**Q6 = do NOT bundle the PDF into Resources**, **Q7 = moot (toolchain is fine)**. Still open:

- **Q1 — annual refresh build?** The baked in-IPA book is the floor both self-heal layers land on
  and it freezes forever. Recommendation: a routine ~annual build whose only content is a current
  `WebBundle`.
- **Q2 — FOLWEB-07 vs the `♪999` canary.** Making out-of-range numpad entries error would kill the
  zero-network group book check.
- **Q3 — who is the second operator**, and do you want a phone-operable arm/abort on the dashboard?
- **Q5 — the trigger word** for the fallback protocol.
- **Q8/Q9/Q10** — fleet concurrency (default 2), where the apply affordance lives, and how loud a
  stale-book mismatch should be to the whole choir.

### 6.5 Possibly still open

Fleet-dashboard "Actualizando libro" state — a device mid-migration reports `webCached=false,
pagesCached=0` and may render as "Onboarding pendiente", telling the operator to re-invite a healthy
device. Check `sync-worker/src/index.ts`.

---

## 7. Orientation for a cold tab

- **What it is:** a Catholic parish choir songbook used LIVE during Mass. ~8 people, 6–8
  **personally-owned** iPads (no MDM), plus phones. One permanent director: **Braulio Figueroa**.
- **THERE IS NO INTERNET INSIDE THE CHURCH.** Page sync at Mass is a **Multipeer mesh** (native
  only). The Cloudflare relay needs internet and does **not** work there. Nothing can be pushed,
  rolled back, or looked up during Mass.
- **Stale beats blank. A blank page during Mass is the worst outcome.**
- **Architecture:** one web bundle (`web/src/`) deployed to signovivo.com (Cloudflare Pages) **and**
  copied into `ios/WebBundle` inside the IPA. The native app is a thin RN shell whose WKWebView
  loads that bundle from `file://`. `NATIVE_FILE_MODE` is injected by the shell — the SW is **not**
  registered on native.
- **Remote-loading signovivo.com in the WebView was tested and rejected** (offline cold launch gives
  `NSURLErrorDomain -1004` despite a full cache). **Keep `file://`.**
- **The mesh supports 14 followers** (two lazily-allocated MCSessions of 7).

### 🚫 Never do

- **Never run `npm run test:e2e`** — `e2e/relay-sync.test.mjs` publishes to the **PRODUCTION relay
  room** and would flip live followers' pages. Run the named safe subset — it is the list in
  `.github/workflows/ci.yml` (currently **14 files**). It reports **226 tests** with a built
  `web/dist` and **208 without** — `e2e/sw-page-cache.test.mjs` needs the bundle. Green at HEAD.
- Never `git checkout` in the shared main checkout — other tabs live there.
- Never print or commit the contents of `director-codes.private.json`.
- Never hand-edit a `bundle-manifest.json`. Regenerate it with `node web/build.mjs`.

### Key files added by the last session

| File | What it is |
|---|---|
| `src/bookResolve.js` (+`.d.ts`) | the boot decision table — which bundle to load. Pure, 23 tests |
| `src/bookUpdate.js` (+`.d.ts`) | the downloader: stage / verify / canApplyNow / apply. Pure, 47 tests |
| `sync-worker/src/bookArming.js` | who gets told about a new book. Pure, 18 tests |
| `scripts/additive-gate.mjs` | the additive-only gate. Read its header |
| `scripts/compare-book-renders.mjs` | which pages actually changed between two PDFs, at build settings |
| `scripts/sim-bundle-lab.mjs` | plant a bundle state on a simulator: `--state legacy\|valid\|broken\|clean` |
| `web/manifest-baseline.json` | the gate's reference point. **Bump it in its OWN commit** |

### Docs + memory

- `docs/choir-pdf-distribution-plan.md` — the OTA plan (13-agent design + 23-attack red team).
  **Its §3.1 environment facts and OPEN RISK 3 are stale** (the toolchain is fine now), and
  M1/M2/M4/M5 in its §8 build order are DONE.
- `docs/director-access-design.md`, `docs/pre-mass-checklist.md`, `docs/app-hardening-plan.md`.
- Memory: `project_caching_layers_distribution`, `feedback_never_run_full_e2e`,
  `feedback_upload_method`, `project_offline_director_outage_build371`.

---

## 8. The operator flow, when the director hands Miguel a new PDF

```bash
# 1. replace assets/signo_vivo_372.pdf with the new book, then stamp it:
node scripts/stamp-book-date.mjs --pdf assets/signo_vivo_372.pdf
#    (it REFUSES to double-stamp. If the book is already stamped, rebuild page 1 from an
#     unstamped copy first: qpdf --empty --pages <unstamped>.pdf 1 <this>.pdf 2-z -- clean.pdf)

# 2. prove it is additive BEFORE publishing:
node scripts/compare-book-renders.mjs --a <old>.pdf --b assets/signo_vivo_372.pdf

# 3. rehearse on the isolated preview:
STAGING=1 bash scripts/release.sh      # → staging.alvernia-reader.pages.dev

# 4. promote:
SKIP_NATIVE=1 bash scripts/release.sh  # web-only, no Xcode needed
```

Every online **web** device picks it up on next open. **iPads get it only once the budgeted build
(§6.1) has shipped and the downloader is armed (§6.2)** — until then a web deploy does not reach
them at all.

**The fleet check, no internet required:** the director jumps to the last page (`♪ → 999 → Abrir`).
Every device showing the current number is current; anything else is stale. The build badge in the
corner is the **shell's** number and cannot answer this.
