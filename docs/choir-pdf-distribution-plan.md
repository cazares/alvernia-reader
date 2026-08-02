# SignoVivo — Choir PDF Distribution: one last build, then never TestFlight again

> **Goal.** Today a new songbook PDF from the music director requires a new IPA and a TestFlight
> install on every iPad, and the choir finds TestFlight confusing enough that it does not reliably
> happen. This plan spends **ONE more native build** on capabilities so that every subsequent PDF
> reaches every iPad by **opening the app with internet** — at home or at Wed/Sat practice — with a
> human tap to apply, and never a TestFlight round again. That one build buys five things: (1) the
> D1/D2 boot-resolution fixes, which are worth the build on their own; (2) a TypeScript book
> downloader that stages over HTTPS and applies only on an explicit human action; (3) **MODO
> EMERGENCIA**, a native-drawn songbook viewer that keeps following the director when the web layer
> is dead; (4) a two-line neutering of the peer bundle-push rail; (5) a build-baked kill switch so
> all of it can be turned off in source without another build.
>
> **Created 2026-08-01.** Written against HEAD `67d7ad1`, build 381, branch
> `claude/choir-pdf-distribution-da2897`. Every claim about current behavior is cited at a real
> `file:line`, verified at that HEAD. This builds on [`major-update-2026-07.md`](major-update-2026-07.md)
> §5/§6.2/§6.6 (the atomic hash-verified distribution design), [`app-hardening-plan.md`](app-hardening-plan.md)
> (P-OTA, P-MESH, P-BUILDENV), [`ia-audit-2026-07.md`](ia-audit-2026-07.md) (RELVER-01/04/05/06,
> FOLWEB-07), [`sync-reliability-audit-2026-07.md`](sync-reliability-audit-2026-07.md) (M-F3), and the
> operator ritual in [`pre-mass-checklist.md`](pre-mass-checklist.md). Where those docs already
> specify something, this plan cites them rather than restating them.
>
> **Provenance.** 5-dimension grounded source map → 3 independent designs → 1 synthesis → a 23-attack
> red team across three lenses (no-internet-at-Mass, correlated failure, human factors under stress).
> Every red-team mitigation that survived is folded into §5–§6 below, not left in a footnote. The
> ones that are only partially answered are marked **OPEN RISK** in §7.

---

## 1. What is being built, in one paragraph

`web/build.mjs` gains a `bundle-manifest.json` emitted over the finished `web/dist` — a content-hash
`bookVersion`, per-file size + sha256 + md5, `totalPages`, and the shell build it was baked from. That
file ships **inside the IPA** (via the existing `rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle`
at `scripts/release.sh:56`) and **on signovivo.com**, so the baked book and the served book can never
disagree by construction. The native shell learns which book it should have from the **response** to
the `POST /fleet/checkin` it already makes on foreground and every 90 s
(`PdfReaderApp.tsx:182-201`, `:251`, `:257`) — no new endpoint, no new poll, and a failed check is a
structural no-op, which is the only correct behavior in a building with no internet. When armed, a new
TypeScript module `src/bookUpdate.ts` downloads the missing files into
`Documents/WebBundleStaged/`, verifies each one, and stops. Nothing swaps until a human acts, and the
apply path re-checks that the device has had **real internet in the last 5 minutes** — which is true at
practice and false inside the church by definition. `resolveBundleUri` (`PdfReaderApp.tsx:813-826`) is
rewritten to stop trusting `Documents/WebBundle` on mere existence, the bridge watchdog
(`PdfReaderApp.tsx:307-321`) learns to escalate into a known-good bundle instead of retrying a broken
one, and the peer bundle-push rail is disabled at its receive boundary.

---

## 2. Why this is hard

**There is no internet inside the church during Mass. None. Not for any device.** That single fact
removes every remedy this codebase has. `scripts/rollback-web.sh`, the staging canary, the fleet
dashboard, the relay, the crash reporter (`web/src/app.js:2916` POSTs to `RELAY_BASE + "/log"`) — all
of them are **practice-day tools only**. Whatever state a device walks in with at 12:00 is the state
it dies with at 13:45. Multipeer mesh page-turn sync is the only thing that works in that room, which
is why the native app exists at all (`ios/SignoVivo/DirectorSyncModule.swift:44` — MCSession caps at
8 peers total including the director, so at 8 iPads there is exactly zero headroom).

**An OTA mechanism is a correlated-failure machine.** Eight iPads are the same model, on the same iOS,
running the same bundle, foregrounded within the same two minutes. Today a bad bundle can only reach
one device at a time, by hand. After this ships, one server string and one `wrangler deploy` fan out to
all eight with no stagger and no cap. Every decision below that looks paranoid — the human apply gate,
the live-internet veto, the per-device arming, the K-at-a-time server queue, the untouchable
code-signed floor — exists because the failure mode this feature introduces is *the whole choir at
once, in the one place where nothing can be fixed*.

**And the invariant is fragile.** The additive-only rule (§4, decision 7) is what keeps every stale
offline copy valid forever, and it has **already been violated once in production**: build 377 / PR
#257 re-rendered ~290 pages in place. The gate that would have caught it is installed on the PR path
(`.github/workflows/ci.yml:72`) and **absent from the publish path** — `scripts/release.sh` runs
`node web/build.mjs` (`:50`) then `npx wrangler pages deploy` (`:125`) and never calls
`scripts/smoke-boot.mjs`.

---

## 3. Ground truth — verified facts and defects

### 3.1 Environment

| Fact | Evidence |
|---|---|
| No internet at Mass; mesh is the only sync rail | `DirectorSyncModule.swift:44`; `major-update-2026-07.md` header |
| 6–8 iPads, ~8 people; MCSession caps at 8 peers **total** | `DirectorSyncModule.swift:44` |
| `web/dist` = **27,818,447 bytes / 389 files**; largest single file 1,120,503 B (`icon.png`) | measured at HEAD `67d7ad1` |
| 372 rendered pages; `assets/signo_vivo_372.pdf` = 14,856,339 B | `ls web/dist/books/standard/pages \| wc -l`; `ls -la assets/*.pdf` |
| Cloudflare Pages rejects a single asset > 25 MiB (26,214,400 B) | Pages platform limit — **this is why a monolithic `.pack` is undeliverable from the origin this project has** |
| Xcode.app is NOT installed (`xcode-select -p` → CommandLineTools, macOS SDKs only) | verified this session |
| `node_modules` absent from **both** this worktree and `/Users/cazares/src/alvernia-reader/` | `ios/Podfile:1` resolves expo through it, so `pod install` fails before it reaches the Ruby crash |
| `pod install` broken under Ruby 4.0.1; `release.sh:84` works around it with `cp ios/Pods/Manifest.lock ios/Podfile.lock` — valid ONLY while the pod set never changes | `pod env` → Ruby 4.0.1; `release.sh:84` |
| `ios/Pods/Manifest.lock` ≠ committed `ios/Podfile.lock` (Expo 54.0.34 vs .33, stray RNCPicker 2.11.4) | `diff <(git show HEAD:ios/Podfile.lock) ios/Pods/Manifest.lock` |
| `director-codes.private.json` is gitignored and exists ONLY in the main checkout; `release.sh` **warns but proceeds** without it → a build where nobody can become director (this caused a real Mass outage) | `project_offline_director_outage_build371` |
| TestFlight upload is manual via Transporter (no ASC API creds) | `pre-mass-checklist.md:38` |
| `ios/WebBundle/` is gitignored and materialized ONLY by `release.sh:56` | `.gitignore:9`; `git ls-files ios/WebBundle` → 0 files |

### 3.2 The three defects this plan must answer

**D1 — `resolveBundleUri` prefers `Documents/WebBundle` unconditionally, forever.**

```
PdfReaderApp.tsx:813-826
  const docIndex = `${docDir}WebBundle/index.html`;
  try { const info = await FileSystem.getInfoAsync(docIndex); if (info.exists) return docIndex; }
  catch { /* fall through to bundled copy */ }
  const bundleDir = FileSystem.bundleDirectory || "";
  return `${bundleDir}WebBundle/index.html`;          // ← no existence check on this branch
```

No version compare, no hash, no manifest, no boot-health check. Once that directory exists it wins
forever, and **nothing anywhere ever deletes it** — the only writer is the Swift atomic swap
(`DirectorSyncModule.swift:1021-1042`) and the entire JS layer touches `expo-file-system` on three
read-only lines (`PdfReaderApp.tsx:814`, `:818`, `:824`). An iPad that once received a mesh bundle
push and then installs a newer TestFlight build keeps loading the **old** web bundle forever while the
badge shows the **new** build number (`PdfReaderApp.tsx:1037` injects `BUILD_VERSION` — the shell's
`version.json` number — and `web/src/app.js:2903` prefers that injected global over the web bundle's
own baked number). It is also permanently ineligible for a corrective mesh push, because
`handleBundleOffer` compares `offered > mine` where `mine` is the **shell's** CFBundleVersion
(`DirectorSyncModule.swift:717-721`, `:97-99`). And it is invisible to the fleet dashboard, which
reports only `nativeBuild` + `role` (`PdfReaderApp.tsx:185-191`) while the web layer's own check-in is
disabled in native file mode (`web/src/app.js:2952`). Tracked as
`native-swift-stale-documents-bundle-masks-update` [OPEN-H] in
[`app-hardening-plan.md:613-616`](app-hardening-plan.md); fix sketched as RELVER-06 in
[`ia-audit-2026-07.md:4754-4767`](ia-audit-2026-07.md).

> **The one-way door.** No code path in the app ever re-resolves `bundleUri` after a failure — not the
> watchdog (`:307-321`), not `Reintentar` (`:1060-1067`), not `performSoftReset` (`:541-560`), not
> `onContentProcessDidTerminate` (`:1089-1095`). Once `Documents/WebBundle` exists and is broken, the
> only recovery is delete-and-reinstall the app, which is TestFlight, which is the exact thing this
> project exists to eliminate. **Shipping a downloader without the D1 fix converts this from rare to
> universal**, because a downloader makes `Documents/WebBundle` the normal state of every device.

**D2 — the boot watchdog retries the SAME bundle, then parks.**

```
PdfReaderApp.tsx:307-321
  if (remountAttemptsRef.current < 2) { remountAttemptsRef.current += 1; setMountKey(k => k+1); }
  else { setWebDead(true); }                                   // 6000 ms
```

Two bounded remounts of the identical URI, then a native Spanish "Reintentar" floor
(`:1051-1073`). It never falls back to the code-signed in-IPA copy. `Reintentar` resets
`remountAttemptsRef` to 0 against the same URI (`:1060-1067`) — an unbounded human-driven loop with no
escape. `onContentProcessDidTerminate` reloads without incrementing the counter (`:1089-1095`), so a
hard crash-loop never escalates at all. Reported as W2N-02 in
[`ia-audit-2026-07/findings-w2n.md:128-132`](ia-audit-2026-07/findings-w2n.md).

Also: `PdfReaderApp.tsx:1044-1046` renders a plain black `View` while `!booted || !bundleUri`, and the
watchdog effect returns early in exactly that state (`:326`). A boot that never settles is a permanent
black rectangle with **no native floor at all**.

**D3 — the `_headers` immutable rule is INERT in production. Leave it that way.**

Cloudflare Pages rejects any `_headers` path with more than one `*`, and `/books/*/pages/*` has two,
so Pages drops the rule at parse time and serves `public, max-age=0, must-revalidate`
(`web/build.mjs:706-712`, restated at `web/src/sw.js:18-21`; corrected in PR #273 and deliberately left
inert per [`app-hardening-plan.md:419-422`](app-hardening-plan.md)). **This is correct for this
design** — every staged GET revalidates against the origin, which is what a downloader wants. 372
revalidating GETs on practice wifi is seconds. **Do not propose arming it.**

### 3.3 Machinery that already exists and must be reused, not rewritten

| What | Where | Reuse as |
|---|---|---|
| `installReceivedBundle` — staging dir, header bounds, whole-archive size equality, path-traversal reject, streamed 1 MiB copy on the **throwing** FileHandle API, per-file on-disk size verify, `index.html > 200 B` floor, three-move atomic swap **with rollback** | `DirectorSyncModule.swift:879-1049` (swap at `:1021-1042`) | **The authoritative model for §5.7.** The TS apply path mirrors its order and its `fail(stage:)` vocabulary (`:886-891`, 17 stages) exactly. Do not invent a second sequence. |
| `packWebBundle` + the self-describing pack format `[4-byte BE len][JSON header][file bytes]`, header additive-extensible (reader only reads `files` + `version`, ignores `v`) | `:794-871`, `:932-937` | Retained on disk, unused by this plan (§16.1 of the synthesis — the 25 MiB Pages cap makes a monolithic pack undeliverable). Kept intact so a future R2 rail can adopt it unchanged. |
| `fleetCheckin(extra?)` — spreads an arbitrary object into the payload | `PdfReaderApp.tsx:182-201` | The book-version reporting channel. Three new fields cost zero plumbing. |
| `breadcrumb()` → AsyncStorage key `sv_bc`; `dbgLog`/`dbgFlush` → `RELAY_BASE + /log` | `:131-137`, `:147-177` | Durable forensics for which bundle was loaded and which one failed. |
| `checkin()` sanitize-and-clamp + `{...prev}` merge; `getByName("__fleet__")` singleton-DO trick | `sync-worker/src/index.ts:250-289`, `:649` | Additive by construction — old clients simply omit the new fields. |
| `setOfflineGateState` progress/ready/retry UI, fully wired to real DOM, **zero live callers** (`:2366` and `:3441` both pass `visible:false`) | `web/src/app.js:505-533` | Already assigned as the update-banner host by [`major-update-2026-07.md:336-338`](major-update-2026-07.md). |
| `isOfflineBundleReady(totalPages)` — cache-count-vs-totalPages verifier. **Now wired**: it backs `fleetCheckin`'s `webCached` claim, which previously trusted the never-cleared `OFFLINE_READY_KEY` flag and so reported evicted iPads as green | `web/src/app.js:788`, called at `:3126` | The web-side completeness gate — extend it (don't fork it) when a bundle version enters the readiness claim. |
| `svRelayRoom.js` / `svSyncDecision.js` pattern — pure UMD lib, node-tested, in the CI **named** list, with an inline fallback in `app.js` | `web/src/lib/` | The shape every new pure decision function must follow. |
| `scripts/smoke-boot.mjs` check() harness + page-count cross-check; `scripts/check-book-consistency.mjs` (pdfinfo vs song index, wired into `npm run preios`) | — | The additive-only gate lives here, and must also be moved onto the **publish** path. |
| `cleanup_release` + `trap ... EXIT INT TERM` idiom | `scripts/release.sh:70-88` | Any new mutating release step folds into this trap. |

---

## 4. Decisions — SETTLED. Not up for relitigation.

These are the maintainer's decisions. A future session may implement them better; it may not reopen
them without being asked.

| # | Decision | One-line rationale |
|---|---|---|
| 1 | **ONE more native build / TestFlight round is budgeted and accepted.** Spend it on capabilities so no further build is ever needed for a new PDF. | The whole project exists to make TestFlight the last resort instead of the delivery mechanism. |
| 2 | **NO EAS, ever, for Miguel's own apps.** If `expo-updates` were used it would have to be self-hosted. | Standing global rule; also codified at [`app-hardening-plan.md:134`](app-hardening-plan.md). |
| 3 | **Ship the downloader DORMANT.** Enable it over the air on ONE iPad, prove it, then fleet-wide. | "A controlled amount." Also the only way to observe a failure before it is fleet-wide. |
| 4 | **Updates STAGE automatically but APPLY only on EXPLICIT HUMAN ACTION.** Never unattended. | An iPad catching a hotspot in the parking lot at 11:55 and swapping its bundle is the nightmare, and there is no remedy at Mass. |
| 5 | **The D1 `resolveBundleUri` fix is MANDATORY.** | A downloader makes `Documents/WebBundle` the normal state of every device, so D1 goes from occasional to universal. |
| 6 | **Fallback layers L1→L4 in priority order** (see §6): app self-heals into the shipped in-IPA bundle; a NATIVE-drawn emergency control at the current song; the PDF pre-positioned by AirDrop at practice; printed music. Plus a **trigger word** so falling back is one command, not eight judgment calls — and it must be **rehearsed**. | L1 is the only layer that costs zero human actions and keeps mesh sync alive. Distribution *during* the emergency is the actual bug — the Keynote AirDrop reached "a person or two." |
| 7 | **ADDITIVE-ONLY: the songbook only ever GROWS AT THE END. Existing page numbers are permanent.** | It is what keeps every stale offline copy — AirDropped PDF, cached page images, old SW caches — valid forever. Violated once already (build 377 / PR #257). |
| 8 | **Canary page 372 is committed and verified purely additive** (all 371 pre-existing images byte-identical; only `page-372.webp` is new, 9,228 B). It doubles as a book-version check: ♪ → `999` → Abrir lands on the device's own last page, because `resolveSongPage` sends out-of-range numbers to `state.totalPages` (`web/src/app.js:700-711`). | Today this is the ONLY ground truth for which book a device has — the badge shows the shell build and cannot be trusted (D1). |

**Two deliberate reversals of prior art**, flagged here so no future session thinks they were misread:

- [`major-update-2026-07.md:340-342`](major-update-2026-07.md) wires `canApplyUpdateNow()` as an
  **automatic apply-when-idle trigger**. This plan demotes it to a **pure veto** on top of a human tap.
  Decision 4 is explicit and all three independent designs reached the same conclusion.
- [`major-update-2026-07.md:460-461`](major-update-2026-07.md) declares per-device targeted rollouts
  **out of scope as gold-plating**. This plan ships them anyway, because here they cost two `wrangler`
  vars and a `.includes()`, and because the ability to disarm the entire fleet in 20 seconds without
  touching an iPad is worth more than the machinery costs when there is no remedy at Mass.

**`expo-updates` is REJECTED**, and this is a finding, not a preference: the songbook lives in a
blue-folder Copy-Bundle-Resources directory inside code-signed `Bundle.main`
(`ios/SignoVivo.xcodeproj/project.pbxproj:38`, `:208`; read at `DirectorSyncModule.swift:802-803` and
`PdfReaderApp.tsx:824-825`). An `expo-updates` payload is the Metro JS bundle plus Metro-registered
assets. **It structurally cannot deliver a new songbook**, it would cost ~6 new CocoaPods (defeating
the `Manifest.lock` copy workaround), it would require hand-editing `AppDelegate.swift`'s Release
`bundleURL()`, and `ios/SignoVivo/Supporting/Expo.plist:11` already declares
`EXUpdatesRuntimeVersion 1.1` while `app.config.js:32` resolves to `1.0.4` — a silent
never-delivers-an-update trap. It would spend the one budgeted build and leave the original problem
unsolved. Do not revisit without new information.

---

## 5. Design

### 5.0 Shape, and why

- **Per-file HTTP, not a pack.** `web/dist` is 27,818,447 B and Pages caps a single asset at
  26,214,400 B. A monolithic `.pack` cannot be served from the origin this project actually has, and
  would require R2 (M5, NOT STARTED, blocked on provisioning) plus a Worker stream route to ship bytes
  that `npx wrangler pages deploy web/dist` (`release.sh:125`) already publishes at their natural URLs.
  Largest single file is 1.1 MB.
- **TypeScript, not Swift.** Xcode is not installed and `node_modules` is absent from both checkouts.
  Swift written now ships **uncompiled and device-unverified** on a one-shot build, on top of an
  already-uncompiled Wave-2/3 Swift backlog. The TS path is typechecked by CI and node-testable today.
  Swift's total footprint in this plan is **four guard lines** (§5.9). If the toolchain is restored and
  Swift becomes device-testable, porting the downloader to Swift later is mechanical.
- **The Swift installer is reused in DESIGN and VOCABULARY, not in code.** §5.7 mirrors
  `installReceivedBundle`'s order and its `fail(stage:)` taxonomy exactly. Stated here as a deliberate
  departure from the brief's "reuse `installReceivedBundle`" instruction, with the 25 MiB cap and the
  missing toolchain as the reasons.

### 5.1 Identity — `bundle-manifest.json`

**File:** `web/build.mjs`. New function `emitBundleManifest(distDir)`, called at the end of the
top-level build **after** `dist` is fully written (after the `inlineScripts` write at `:697-704`), NOT
inside `buildBook` — the manifest must cover the whole `dist`, shell files included.

```jsonc
{
  "schema": 1,
  "bookVersion": "bv_<first 16 hex of sha256 over sorted \"<path>:<sha256>\" lines>",
  "totalPages": 372,
  "builtFromShellBuild": 382,        // version.json buildNumber at build time
  "minShellBuild": 382,              // lowest shell that may run this bundle
  "generatedAt": "2026-08-01T…Z",
  "sourcePdfSha256": "<sha256 of assets/signo_vivo_372.pdf>",
  "sourcePdfPages": 372,             // from pdfinfo, NOT from the render
  "songIndexDigest": "<sha256 of JSON.stringify(songIndex)>",
  "firstSong": 1, "lastSong": 372,
  "renderer": { "pdftoppm": "…", "cwebp": "…" },
  "files": [ { "p": "books/standard/pages/page-372.webp", "n": 9228, "h": "<sha256>", "m": "<md5>" }, … ]
}
```

Rules:

1. **`files` is produced by WALKING `dist`**, never from an in-memory list. A manifest that can omit a
   file is a manifest whose completeness gate proves nothing (red team A6/NI7). Exclude only
   `bundle-manifest.json` itself.
2. `node:crypto` is already imported (`web/build.mjs:3`) and sha256-over-file-bytes already exists
   (`:21-27`). **Zero new dependencies.**
3. `h` (sha256) derives `bookVersion` and drives the CI additive gate. `m` (md5) is what the **device**
   verifies, because `expo-file-system/legacy` exposes `getInfoAsync(uri, { md5: true })` and not
   sha256. Document in the file header that **md5 is a corruption check; HTTPS to our own origin is the
   authenticity boundary.** No pod, no CryptoKit, no Swift.
4. Thread `bookVersion` into the existing inline `#pages-data` blob (`web/build.mjs:699`), which
   `web/src/app.js:3386-3390` already parses. **Do NOT touch `books.json` or `#books-data`** — verified
   dead: the only repo-wide references are a precache string (`web/src/app.js:241`) and a smoke-test
   existence assert (`scripts/smoke-boot.mjs:77`).
5. Because `release.sh:56` does `rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle`, the baked
   in-IPA bundle carries the identical manifest for free. **The baked book and the served book can
   never disagree.**

**Why a content hash and not a monotonic integer:** idempotent republish, and rollback becomes a plain
inequality. The genuine benefit of an integer — that "has the shell caught up?" is *decidable* — is
preserved by carrying `builtFromShellBuild` / `minShellBuild` as separate integers in the
`version.json` series. One field per question; three forked version namespaces was the root disease
(§3.2 D1), and the cure is not a fourth universal field.

### 5.2 Storage layout and keys

Under `FileSystem.documentDirectory`:

| Path | Meaning | Writer |
|---|---|---|
| `WebBundle/` | the ACTIVE downloaded copy | `applyStagedBundle()` only |
| `WebBundleStaged/` | stage in progress or verified-unapplied. **Stable name** so it survives an app kill | `stageBook()` |
| `WebBundleStaged/.stage.json` | `{bookVersion, startedAt}` — written FIRST, before any file | `stageBook()` |
| `WebBundle.prev/` | the last **PROVEN-GOOD** Documents bundle. **Kept permanently** (red team NI2) | `applyStagedBundle()` |
| `WebBundle.bad-<epochms>/` | quarantine. **RENAMED, NEVER DELETED** (global rule §18). Newest one kept; the boot sweep deletes older ones | self-heal ladder |

Immutable floor, never written by anything: `${FileSystem.bundleDirectory}WebBundle/` — code-signed,
read-only, and verified to contain all 372 page WebPs.

> **Deliberate improvement over the Swift original**, which deletes `WebBundle_old` unconditionally at
> `DirectorSyncModule.swift:1042`: `WebBundle.prev` is kept **until boot proof**, and then kept
> **permanently** as the middle rung of the self-heal ladder. One extra ~27 MB is trivial; a fallback
> that lands on a book one version old instead of years old is not (red team NI2/A4).

AsyncStorage keys (all JSON):

| Key | Shape |
|---|---|
| `sv_book_active` | `{bookVersion, totalPages, installedAt, source: "baked" \| "http"}` |
| `sv_book_staged` | `{bookVersion, done, total, ready, readyAt, error}` |
| `sv_book_boot` | `{bookVersion, mountedAt, provedAt, attempts}` |
| `sv_book_quarantine` | `[{bookVersion, failures, lastFailureAt}]` — a **counter, not a tombstone** (red team NI5) |
| `sv_book_resolved` | `{uri, bookVersion, builtFromShellBuild}` — the **cached** boot decision (red team A7) |
| `sv_book_force_bundled` | `{setAt}` — operator panic switch; **auto-expires** (red team H4) |
| `sv_book_reverted` | `{bookVersion, at}` — drives the loud native "LIBRO ANTERIOR" banner (red team A4) |
| `sv_lastpage` | `<int>` — for L2 and for resume-in-place |

Reused: `sv_devid` (`PdfReaderApp.tsx:206`) is the per-device rollout target. Build-baked kill switch
in `PdfReaderApp.tsx`: `const SV_BOOK_DL_KILL = false;` — a one-line source neuter, independent of the
server, mirroring the `SYNC_STRICT` pattern from
[`major-update-2026-07.md:213-215`](major-update-2026-07.md).

### 5.3 Server side — the dormant flag IS the rollout control

**File:** `sync-worker/wrangler.jsonc`, add to `vars`:

```jsonc
"BOOK_UPDATE_VERSION": "",        // "" = dormant
"BOOK_UPDATE_DEVICES": "",        // "" = nobody | "<sv_devid>[,<sv_devid>]" | "*"
"BOOK_UPDATE_ALLOW_FLEET": "",    // "*" is IGNORED unless this is exactly "yes" (red team A2)
"BOOK_UPDATE_BASE": "https://signovivo.com",
"BOOK_UPDATE_CONCURRENCY": "2"    // max devices told to stage at once when "*" is honored
```

**File:** `sync-worker/src/index.ts`, inside `POST /fleet/checkin` (`:651-667`), after
`fleet.checkin(...)` returns. The `__fleet__` DO already holds every device's last-seen record
(`:649`), so once `bookVersion` is stored the handler can make the fan-out a **server decision, not a
broadcast**:

```
armed = BOOK_UPDATE_DEVICES.split(",").filter(Boolean)
if (!BOOK_UPDATE_VERSION) → no bookUpdate field, ever
if (armed includes this deviceId) → hand out bookUpdate
else if (armed includes "*" && BOOK_UPDATE_ALLOW_FLEET === "yes")
     → hand out bookUpdate ONLY if fewer than BOOK_UPDATE_CONCURRENCY devices are
       currently mid-stage (bookStage starts with "downloading" and ts < 15 min old)
       AND this device's bookVersion !== BOOK_UPDATE_VERSION
result.bookUpdate = { bookVersion, base: BOOK_UPDATE_BASE }
```

`"*"` then means *roll the fleet two at a time, automatically* instead of *hit all eight at once* — and
the 20-second abort is preserved. Both `BOOK_UPDATE_*` strings empty ⇒ the field never appears ⇒ the
client has nothing to act on. **That is decision 3, and it costs two strings.**

Also in `checkin()` (`:250-289`), following the existing clamp idiom exactly:

```ts
if (o.bookVersion  != null) entry.bookVersion  = String(o.bookVersion).slice(0, 32);
if (o.bundleSource != null) entry.bundleSource = String(o.bundleSource).slice(0, 16);
if (o.bookStage    != null) entry.bookStage    = String(o.bookStage).slice(0, 40);
```

Add a **`Libro`** column to `renderFleetDashboard` (`:425-568`) showing `bookVersion` + `bookStage`,
amber `versión desconocida` for clients that omit it (the RELVER-04 shape,
[`ia-audit-2026-07.md:4947-4948`](ia-audit-2026-07.md)). **Without this, decision 3's "prove it on ONE
iPad first" has nothing to observe** — a downloader failing silently on seven iPads looks identical to
one that succeeded.

`publish()` builds its `Snapshot` field-by-field (`:171-179`) and **silently drops unknown keys**, so
nothing rides the relay heartbeat without a worker deploy. Sequence the worker deploy strictly before
any client that reads a new field back.

The worker currently has **zero CI coverage** (`.github/workflows/ci.yml:58-69` lists nine named e2e
files, none under `sync-worker/`). Add `sync-worker/test/a2.test.mjs` and the new book-pointer test to
the **named** list. **Never the `npm run test:e2e` glob** — it publishes to the live Mass room
(`major-update-2026-07.md:209-211`).

### 5.4 Discovery — no new endpoint, no new poll

**File:** `PdfReaderApp.tsx:182-201`. `fleetCheckin` currently discards the response
(`.catch(() => {})`). Change to `.then(r => r.json()).then(onCheckinResponse).catch(() => {})`. The
existing foreground call (`:251`) and 90 s interval (`:257`) ARE the scheduler.

`onCheckinResponse(resp)` — **the pointer is data, not an instruction.** Validate before acting; any
violation ⇒ silent ignore + `dbgLog` + zero state change:

- `bookVersion` must match `/^bv_[0-9a-f]{16}$/`.
- `base`'s host must be one of **two constants baked into the app** (`signovivo.com`,
  `alvernia-reader.pages.dev`). **Never a host taken from the response.** A compromised or buggy worker
  can never redirect the fetch to an arbitrary origin.

Then also, on **every** response (red team NI6/A6 — the abort must be a real revoke, not just a
disarm):

- if `resp.bookUpdate` is **absent**, or its `bookVersion` differs from `sv_book_staged.bookVersion`
  → clear `sv_book_staged` and delete `WebBundleStaged/`. A retracted update dies on any device that
  touches wifi once before Mass.
- record `sv_lastCheckinOkAt = Date.now()` — this is the **live-internet proof** the apply gate needs
  (§5.6).

Stage iff **all** hold: `!SV_BOOK_DL_KILL`; `bookVersion !== sv_book_active.bookVersion`; not
blacklisted (3 lifetime failures) and not failed today; `webReadyRef.current === true` (**never on the
boot path** — §5.10b); app foregrounded; `roleRef.current !== "director"`; and the **stagger** has
elapsed: `delayMs = (hash(sv_devid) % 20) * 60_000` since this device first saw this `bookVersion`
(red team A2 — identical devices otherwise arrive within the same 20 seconds).

**Staging and applying are different questions and must not share a veto set** (red team NI4/H2). The
practice room is the ONLY place these iPads have internet, and the mesh is up for essentially all of
it — a "no mesh peer connected" staging veto means the downloader never runs where it is needed.
Therefore:

| Predicate | STAGING (writing bytes nothing reads) | APPLY (swapping the live bundle) |
|---|---|---|
| `roleRef.current === "director"` | veto | veto, never overridable |
| director snapshot within 10 min | veto | veto |
| any mesh **peer connected** | *throttle*, not veto: concurrency 3 → 1 | veto |
| mesh page event in last 30 s | pause downloads 30 s | veto (page turn within 60 s) |
| free disk below threshold | veto | veto |
| bridge not ready | veto | veto |
| `minShellBuild > BUILD_VERSION` | veto | veto |
| successful check-in within last 5 min | not required | **required** (§5.6) |

A partially-staged directory is inert by construction (§5.5g), so staging during practice risks
nothing but bandwidth — and bandwidth is a throttle, not a gate.

**With no internet the fetch simply rejects. A failed version check is a structural no-op — never an
error state, never UI.** Inside the church every check-in fails by definition; a design that surfaced
that would show a permanent error mid-Mass.

### 5.5 Stage — `src/bookUpdate.ts`

New pure module (~250 lines) with `FileSystem` and `AsyncStorage` **injected**, so node can test it.
`stageBook({ base, bookVersion, fs, storage })`, in exactly this order:

1. `GET {base}/bundle-manifest.json?v={bookVersion}`. If `manifest.bookVersion !== bookVersion` →
   abort, retry next check-in. **Stale-CDN-edge guard — this is why a staged dir can never mix two
   versions.**
2. `manifest.minShellBuild > BUILD_VERSION` → abort with `error: "shell-too-old"`, surfaced on the
   dashboard as *"Actualiza en TestFlight"*. A book that needs native code this shell lacks is never
   downloaded.
3. **Disk precheck** (there is none anywhere in the pipeline today):
   `getFreeDiskStorageAsync() > 2 × Σ files[].n + 50 MB` (~106 MB today). Short → `error: "disk"`,
   nothing touched. If the API is unavailable at implementation time, skip it — the mid-download throw
   path is already non-destructive.
4. If `WebBundleStaged/.stage.json` exists with the same `bookVersion` → **RESUME**. Else
   `deleteAsync(WebBundleStaged, { idempotent: true })` and write `.stage.json` FIRST.
5. For each entry at concurrency 3 (1 while a mesh peer is connected): skip if the file exists at
   **exact** size `n`; else `downloadAsync` → verify size → `getInfoAsync({ md5: true })` vs `m`. On
   mismatch delete that one file and retry once; a second failure aborts with `error: "file-hash"` and
   the path recorded. Persist `sv_book_staged.done` every ~20 files so the dashboard shows
   `downloading:37%`.
6. **Completeness gate** — mirrors `DirectorSyncModule.swift:952-1019` in the same order:
   - every manifest entry present at exact size, every md5 matches;
   - `index.html` > 200 bytes (same floor, same rationale as `:1012-1017`);
   - **`books/standard/pages/page-NNN.webp` present for every N in `1..totalPages`** — not just first
     and last (red team NI7);
   - `manifest.files.length` equals the actual file count under `WebBundleStaged/`;
   - `totalPages >= sv_book_active.totalPages` — **decision 7 enforced on-device: a book that SHRANK
     is rejected outright**, because it would invalidate every cached page, every prior SW cache, and
     every AirDropped copy.
7. Only now `sv_book_staged = { ready: true, readyAt: Date.now(), … }`.

> **`ready: true` is the ONLY thing that makes a staged directory eligible for promotion, and
> `resolveBundleUri` has no code path that reads `WebBundleStaged/`. A partially-staged or
> failed-verification bundle is inert by construction — there is no route from a failed check to a
> swap.** Files present on resume are RE-VERIFIED rather than trusted from a bookkeeping list, so the
> state is self-correcting with zero persisted bookkeeping.

`sv_book_staged.ready` **expires**: if `Date.now() - readyAt > 12 h` with no confirming check-in, revert
to `ready: false` and force a re-verify (red team A1 — a `ready` flag persisted from Saturday practice
must not be applicable in the church on Sunday).

**Deferred optimization, deliberately not in v1:** because the invariant is additive-only, the staged
dir could be seeded by `copyAsync` from the active bundle for every file whose size+md5 already
matches, cutting a one-new-page update from 27 MB of network to ~100 KB. It needs transient disk
headroom and merge semantics. Build it only if practice wifi proves slow. Full re-download works
identically from **any** device state — devices in the field carry baked bundles from builds 368–381
plus mesh pushes of unknown vintage — and has no base-version matrix.

### 5.6 The apply gate — human trigger, predicate veto

**There is no automatic modal.** The synthesis proposed an `Alert.alert` on `ready`; the red team killed
it three separate ways (a persisted `ready` flag fires it in the church at 12:04 on seven devices at
once; the director's iPad boots as a *follower* so the role veto is disarmed on the one device it was
written to protect; and a modal is a demand for a decision handed to eight people holding
instruments). Replace it with an **affordance**, never a demand:

- **The native strip.** A thin, permanently visible native bar along the bottom edge (React Native, so
  it survives a dead web layer) that normally shows the current **song number** — earning its screen
  space every day, and therefore familiar on the day it matters. When a stage is `ready` **and**
  `canApplyNow()` currently passes, it additionally reads `Cancionero nuevo listo — tocar para
  actualizar`. Tapping asks for one confirm. A person can ignore it forever. Verify it does not
  intercept the edge-swipe region that opens the drawer (`web/src/app.js:2699-2790`).
- **DIAGNÓSTICO** (§6, L2) also exposes *Aplicar actualización* for the operator.
- **A numpad code**, `BOOK_APPLY_CODE`, beside the existing `SOFT_RESET_CODE` (`PdfReaderApp.tsx:67`,
  dispatched at `:576`). **Constraint: every code must be Levenshtein distance ≥ 3 from every other
  code**, unit-tested in the named CI list. The synthesis's proposed `744668487` is **rejected** — it
  is one digit from `SOFT_RESET_CODE = "744668486"`, read off a printed sheet in poor light (red team
  H4). Same constraint for `BOOK_FORCE_BAKED_CODE`.

`canApplyNow(ctx)` — a pure function in `src/bookUpdate.ts`, node-tested, in the named CI list,
following the shipped `svSyncDecision.js` pattern. **It can only ever say NO.** Vetoes:

1. **No successful `POST /fleet/checkin` within the last 5 minutes.** This is the load-bearing one. It
   proves real, sustained internet; it is true at practice and **false inside the church by
   definition**; it needs no clock, no calendar, and no Mass-schedule config. A parking-lot hotspot
   usually fails it too.
2. Any mesh peer connected.
3. A page turn within 60 s.
4. A director snapshot within 10 min.
5. `roleRef.current === "director"` — never overridable. The director's iPad is the last device on
   earth that may swap.
6. Cold-boot cool-down: refuse for 90 minutes after a cold boot on a device whose last recorded role
   was director.
7. WebView not bridge-ready; `minShellBuild > BUILD_VERSION`.

On refusal: `Alert.alert("Ahora no", "Hay una Misa o ensayo en curso. Actualiza después.")`, change
nothing.

**ALSO REQUIRED:** delete the unconditional auto-apply at `PdfReaderApp.tsx:954-963` — the mesh
`bundleUpdated` handler currently re-resolves and remounts on the spot with no human gate. That is the
held M-F3 nightmare ([`sync-reliability-audit-2026-07.md:63`](sync-reliability-audit-2026-07.md)), live
today.

### 5.7 Apply — `applyStagedBundle()`

The only function that touches the live bundle. Mirrors `DirectorSyncModule.swift:1021-1042`.

1. Re-run the completeness gate defensively (a file could have been evicted since staging).
2. `AsyncStorage.setItem("sv_lastpage", String(currentPageRef.current))`.
3. Set `swapping = true`; render a native `Actualizando…` View with **no WebView mounted** — an
   unmounted WebView cannot lazily 404 a page image mid-swap.
4. Write `sv_book_boot = { bookVersion, mountedAt: Date.now(), provedAt: null, attempts: 0 }` **before**
   the swap.
5. Swap — **renames only, so it moves zero bytes and cannot fail on disk space**:
   - `deleteAsync(WebBundle.prev.tmp, { idempotent: true })`
   - if `WebBundle` exists → `moveAsync(WebBundle → WebBundle.prev.tmp)`  · failure ⇒ `swap-aside`, abort, nothing changed
   - `moveAsync(WebBundleStaged → WebBundle)` · failure ⇒ move `.tmp` back, `swap-in`, abort
6. **Do not delete the old copy.** On boot proof (§5.8) `WebBundle.prev.tmp` is promoted to
   `WebBundle.prev` and the previous `WebBundle.prev` is removed — so exactly one proven-good previous
   bundle is retained, permanently.
7. `setBundleUri(await resolveBundleUri()); setMountKey(k => k + 1)` — the existing remount path,
   unchanged.
8. The preload script (`PdfReaderApp.tsx:1033-1042`) additionally injects
   `window.__SIGNO_VIVO_RESUME_PAGE` from `sv_lastpage`, so nobody loses their place.

**No mid-swap resume logic, deliberately.** Killed between the two moves, `Documents/WebBundle` is
absent, `resolveBundleUri` falls through to the code-signed in-IPA bundle, and the app boots into a
working songbook with mesh sync intact. The operator taps apply again. A phase-fenced ledger would
guard a window measured in milliseconds with code that runs in the boot path — the riskiest code in the
app. The manual retry is the cheaper correctness guarantee.

**Boot sweep** (runs once at launch, after first paint): delete stray `WebBundle.prev.tmp/`, any
`WebBundleStaged/` whose `.stage.json` disagrees with `sv_book_staged`, `WebBundle.bad-*` beyond the
newest one, and legacy `WebBundle_new-*` orphans from the Swift rail (nothing sweeps those today and
each is up to 27 MB — a process kill between `DirectorSyncModule.swift:899` and any exit path orphans
one forever).

### 5.8 Prove — on `bridge-ready` from the new mount

At `PdfReaderApp.tsx:638`:

```
sv_book_active   = { bookVersion, totalPages, installedAt: Date.now(), source: "http" }
sv_book_boot.provedAt = Date.now();  sv_book_boot.attempts = 0     // explicit reset (red team NI5)
sv_book_resolved = { uri, bookVersion, builtFromShellBuild }        // the cached boot decision
promote WebBundle.prev.tmp → WebBundle.prev  (dropping the older prev)
fleetCheckin({ bookVersion, bundleSource: "documents", bookStage: "active" })
```

`bookVersion` reported to the dashboard is **re-read from `bundle-manifest.json` in the bundle actually
mounted**, never from `sv_book_active` — a filesystem clobber must be able to change what the dashboard
says (red team A5). The existing `remountAttemptsRef.current = 0` reset in that handler is unchanged.

### 5.9 D1 fix — rewrite `resolveBundleUri`. MANDATORY. The single riskiest edit in the plan.

Current behavior is quoted in §3.2. The new function is **total and time-bounded by construction**:
wrap it in `Promise.race([resolve, timeout(1500) → bundledUri])` so no filesystem hang can ever strand
it (red team H1).

`resolveBundleUri(force?: "bundled")`:

1. **Fast path (red team A7):** read `sv_book_resolved`. If present and one cheap `getInfoAsync` on its
   `uri` succeeds, return it. Manifest parsing then happens on the **apply** path, where a slow device
   costs a spinner instead of a silent downgrade. Fall through to the full table only when the record
   is missing or its `uri` no longer exists.
2. `bundledUri = ${FileSystem.bundleDirectory}WebBundle/index.html` — **STAT IT** (closes the unchecked
   return at `:824-825`). Missing ⇒ breadcrumb `FATAL:no-baked-bundle` and boot straight into MODO
   EMERGENCIA (§6 L2).
3. `force === "bundled"` OR `sv_book_force_bundled` set and unexpired ⇒ return `bundledUri`.
4. `sv_book_boot.provedAt === null && sv_book_boot.attempts >= 2` ⇒ quarantine `Documents/WebBundle`,
   return `bundledUri` (the hard-crash net, §5.10b).
5. If `Documents/WebBundle/index.html` exists, decide with the manifests:
   - read `Documents/WebBundle/bundle-manifest.json` and `${bundleDir}WebBundle/bundle-manifest.json`;
   - **missing or unparseable Documents manifest ⇒ return `bundledUri`.** Every mesh-pushed
     `Documents/WebBundle` in the field has no manifest, so **one rule retires D1 across the entire
     installed base on first launch of the new build** — no migration, no UserDefaults, no
     CFBundleVersion compare, no Swift. This is strictly better than RELVER-06's key-based eviction,
     which only evicts copies the mesh path wrote; a downloader that writes Documents without that key
     would leave D1 half-fixed.
   - `docManifest.bookVersion === bakedManifest.bookVersion` ⇒ return `bundledUri` (the shell caught
     up; the Documents copy is redundant — RELVER-06's `>=` rider, generalized);
   - `bakedManifest.builtFromShellBuild >= docManifest.builtFromShellBuild` ⇒ return `bundledUri`.
     **PREFER KNOWN-GOOD OVER NEWER:** a TestFlight install always wins, and the device silently
     re-downloads the newer book at the next check-in with internet.
   - `docManifest.bookVersion` blacklisted in `sv_book_quarantine` ⇒ return `bundledUri`;
   - else return the Documents uri.
6. Fall through ⇒ `bundledUri`.

Set `activeBundleSourceRef.current = "documents" | "prev" | "bundled"` on **every** return — the
watchdog closure has no other way to know what is loaded. Cache the outcome into `sv_book_resolved`.

### 5.10 D2 fix — the L1 self-heal ladder

**File:** `PdfReaderApp.tsx`, inside the existing 6000 ms `armBridgeWatchdog` (`:307-321`):

| Attempt | Action |
|---|---|
| 0 | remount the same URI (transient WKWebView wedge — today's behavior) |
| 1 | if source is `documents`: `moveAsync(WebBundle → WebBundle.bad-<ts>)` (**renamed, never deleted** — it is the forensic evidence for *why* it failed), increment that `bookVersion`'s failure counter, `setBundleUri(await resolveBundleUri())`, remount. The resolver now returns `WebBundle.prev` if one exists and is not blacklisted, else `bundledUri`. |
| 2 | if source was `prev`: fall to `bundledUri` and remount. |
| 3 | `setWebDead(true)` — the native floor, now guaranteed to be showing over a code-signed, read-only bundle a downloader can never corrupt, and now escalating into MODO EMERGENCIA rather than a dead-end Reintentar. |

Recovery ≈ 13–19 s, **zero human actions**, and **the Multipeer session is never torn down, so a
follower keeps following through the entire self-heal.**

Four required riders, each one line, each closing a verified one-way door:

- `Reintentar` (`:1060-1067`) must `await resolveBundleUri()` before remounting — and must **not clear
  `webDead`** until the fresh mount actually posts `bridge-ready` (`:638`). Today it clears the floor
  first, which under an async resolve would remove the only UI on screen (red team H1).
- **Never set `bundleUri` to null on any post-boot path**, so the unwatchdogged `!bundleUri` black
  screen (`:1044-1046` + the watchdog's early return at `:326`) is unreachable after first paint.
- `onContentProcessDidTerminate` (`:1089-1095`) must re-resolve **and increment
  `remountAttemptsRef`** — today it does neither, so a hard crash-reload loop never escalates.
- `performSoftReset` (`:541-560`) must re-resolve.

**Make the self-heal LOUD** (red team A4): on any fall-back rung, persist
`sv_book_reverted = { bookVersion, at }` and render a persistent, non-dismissible native banner —
**`LIBRO ANTERIOR · avísale al director`** — that survives restarts until an operator clears it in
DIAGNÓSTICO. A silent correct recovery is worse than a loud one when eight devices do it together. If
`roleRef.current === "director"` when the ladder fires, additionally `Alert` *"Tu cancionero volvió a
una versión anterior — pasa la dirección a otro iPad"*: a director must never be silently demoted to a
stale book.

**Quarantine is a counter, not a tombstone** (red team NI5): store
`{bookVersion, failures, lastFailureAt}`; blacklist only at **3 lifetime failures**; allow one re-stage
attempt per calendar day below that; count an attempt only once the boot effect has actually run and
mounted; and require failures to be **consecutive with no intervening proved boot**. A transient
jetsam under memory pressure must not permanently blacklist the current book on the oldest iPad.

#### 5.10b The hard-crash variant and the pre-boot black screen

- The in-session timer cannot see a bundle that kills the process before React renders — the failure an
  old iPad actually produces. In the boot effect (`:826-846`), read `sv_book_boot`: if
  `provedAt === null && mountedAt` is set, `attempts += 1` and **flush before mounting**.
  `attempts >= 2` ⇒ quarantine before mounting anything (step 4 of §5.9). One AsyncStorage read; the
  only net that catches jetsam.
- Two rules for the pre-boot window: **(a)** discovery and staging NEVER run before `setBooted(true)`
  and never before `bridge-ready`; **(b)** a pre-boot watchdog (12 s, generous) forces
  `bundleUri = bundledUri` and boots anyway if the resolve has not settled. That fallback is
  **explicitly NON-STICKY** — it may mount the baked bundle for this launch but must never quarantine,
  never write `sv_book_active`, and must retry the Documents copy next launch — and it records a
  distinct breadcrumb `preboot-timeout`, never `bridge-timeout`, so a slow-disk episode is never
  mistaken for a bad bundle (red team A7).

### 5.11 Stale-book behavior on the mesh — stop the clamp fight

**This is the most likely real-world failure and it is currently silent.** A device that missed an
update has 371 pages; the director's has 372+. The director turns to 372. The mesh listener sets
`currentPageRef = 372` and injects; `clampPage` (`web/src/app.js:274-279`) silently pins it to
`state.totalPages = 371`; `renderPage` posts `page-changed {page: 371}`; the native handler writes
`currentPageRef = 371` (`:698-723`). One second later the director's heartbeat re-sends 372, the
de-dupe compares `372 !== 371`, and **the cycle repeats forever, for the entire duration of every new
song** — including overwriting the singer's own manual navigation every second, which makes the iPad
unusable as a manual reader precisely when it most needs to be one.

The fix costs no new wire fields: the director's `totalPages` is **already on the payload**
(`DirectorSyncModule.swift:133-141`, forwarded by `emitPage` at `:1438-1441`) and the JS `case "page"`
throws it away. In `PdfReaderApp.tsx`'s `case "page"`, when
`Number(event.totalPages) > totalPagesRef.current`:

1. do **not** inject a sync event for a page beyond my `totalPages` — freeze on my last page instead of
   fighting the singer's navigation every second;
2. suppress the mesh re-drive so **manual navigation works** on a stale device;
3. raise a persistent native banner: *"Cancionero desactualizado — el director va en la página 372 y
   tu copia llega a 371. Usa MODO EMERGENCIA."*

Correspondingly, in `applyNativeSyncEvent` (`web/src/app.js:983`), an out-of-range **director** page
must become a visible state (*"No tengo la página N — libro viejo"*) rather than routing through
`clampPage`. Keep the clamp for local user input only. Inject the resolved bundle's `totalPages` and
`bookVersion` into the web layer via the existing preload script (`PdfReaderApp.tsx:1033-1042`).

Bound the render-failure loop too (red team NI7): after **3 consecutive** `render-failed` events for
the same page, stop re-driving and show a native banner naming the page (*"Falta la página 137 en este
iPad — MODO EMERGENCIA"*) instead of an invisible ~6 s retry storm.

### 5.12 Mesh bundle push — disable it at the receive boundary

Four guards behind one build constant defaulting OFF, **not two**. The synthesis proposed guarding
`sendBundleOffer` (`:705`) and `handleBundleOffer` (`:717`); the red team showed that leaves two live
paths: `handleBundleRequest` (`:753`) still packs and streams, and
`didFinishReceivingResourceWithName` (`:1885`) has **no role guard, no in-flight guard, and never
inspects `resourceName`** — it hands any MCSession resource straight to `installReceivedBundle`, which
move-asides and then unconditionally deletes the old copy (`:1042`).

```swift
// :1885, before dispatching to installReceivedBundle
guard SV_MESH_BUNDLE_PUSH_ENABLED else { bundleTransferInFlight = false; return }
guard bundleTransferInFlight,
      currentRole == "follower",
      resourceName.hasPrefix("webbundle-") else { bundleTransferInFlight = false; return }
```

plus the same build-constant guard at `:705`, `:717`, and `:753`.

**Mesh PAGE-TURN sync — the entire reason the native app exists — is untouched.**

What those four lines delete: the **book-regression vector** (`packWebBundle` sources
`Bundle.main/WebBundle` at `:802-803`, so a director on a newer *shell* would overwrite a follower's
newer HTTP-installed *book*); the **repeat-transfer bug** (`mine` never advances after an install and
`sendBundleOffer` fires on every `.connected` at `:1743`, so a follower re-pulls the full 27 MB on
every reconnect forever, competing with the 1 s page heartbeat on a fleet with zero peer headroom); and
**unauthenticated remote code execution** (every MCSession is `encryptionPreference: .none` at `:125`,
`:1060`, `:1278`, with an auto-accepting certificate handler at `:1910`).

Pin **all four** guards with source-regex assertions in `e2e/nearby-sync-contract.test.mjs` — the only
automated safety net available for Swift in an environment with no Xcode — and keep that file in the
named CI list.

This closes open question **Q5 (P-MESH)** in the RETIRE direction that
[`app-hardening-plan.md:609-611`](app-hardening-plan.md) already leaned. The straggler net that mesh
push was pretending to be is L3, which actually works with no internet.

### 5.13 Observability

`fleetCheckin`'s existing `extra` spread (`:182-201`) carries three new fields:

| Field | Source |
|---|---|
| `bookVersion` | re-read from the **mounted** bundle's manifest at `bridge-ready` — never `BUILD_VERSION`, never `sv_book_active` |
| `bundleSource` | `"documents" \| "prev" \| "bundled"` |
| `bookStage` | `idle \| downloading:37% \| ready \| error:disk \| error:file-hash \| error:shell-too-old \| quarantined:<bv> \| reverted:<bv> \| active` |

Wire the currently-dropped Swift `bundle-error` events into `dbgLog`: add `case "bundle-error"` before
the `default: break` at `:965-966` and declare the event in `src/nearbyDirectorSync.d.ts:17-32` (two
lines). Even with mesh push disabled, that 17-stage taxonomy stays the shared error vocabulary.

The badge still lies — it renders the shell's `BUILD_VERSION` and overrides the web bundle's own baked
number (`PdfReaderApp.tsx:1037`; `web/src/app.js:2903`, `:3479-3491`). RELVER-05's dual badge
(`382·w<bv>`, shown only when they disagree,
[`ia-audit-2026-07.md:5026-5028`](ia-audit-2026-07.md)) is the cheap fix; DIAGNÓSTICO covers it either
way.

### 5.14 Build and CI guards — protect the floor everything stands on

1. **`scripts/release.sh`, immediately after `:56`** — hard assertion: `ios/WebBundle/index.html`
   exists and is > 200 B; `ios/WebBundle/bundle-manifest.json` parses; `page-001.webp` and
   `page-<totalPages>.webp` present; page count matches `web/dist`. `ios/WebBundle/` is gitignored and
   materialized only by that one line — **a silent copy miss ships an IPA with NO in-IPA fallback,
   which makes L1 nonexistent and hands the WebView a path to nothing.** There is no runtime remedy for
   this and there should not be one; it is a release-pipeline defect and the pipeline is where it must
   fail loudly.
2. **`scripts/release.sh:52-57`** — move the `ios/WebBundle` sync out from under the `if STAGING`
   guard, so `SKIP_NATIVE=1` can no longer rewrite the tracked WebBundle tree **and deploy PROD** from
   a bundle no IPA was ever archived from.
3. **Move the additive-only gate onto the PUBLISH path** (red team A3). In `release.sh`, after
   `node web/build.mjs` (`:50`) and **before** `npx wrangler pages deploy` (`:125`), run
   `node scripts/smoke-boot.mjs` with `SMOKE_SKIP_BUILD=1` plus the additive diff, and `exit 1` on
   failure. The script already has `set -euo pipefail` + the `cleanup_release` trap to do this
   crash-safely. Today the gate exists only at `.github/workflows/ci.yml:72` — on the PR path, not the
   path the artifact actually leaves the building on.
4. **The additive gate itself**, in `scripts/smoke-boot.mjs` (already the last CI step). Diff the new
   `bundle-manifest.json` against the baseline **as committed**: `git show HEAD:web/manifest-baseline.json`,
   **never the working tree**, so a same-commit baseline update cannot self-approve — require the
   baseline bump to be its own commit. Assert: every `page-NNN.webp` in the baseline exists in the new
   manifest with an **identical sha256**; no page path disappears; `totalPages` never decreases;
   `manifest.files.length` equals the real file count; pages `1..totalPages` all present by name.
   Allowlist shell files as expected-to-change. Any MODIFIED/REMOVED reds the gate, overridable only by
   `ADDITIVE_OVERRIDE="<typed phrase>"`. **This is the guard build 377 / PR #257 needed.**
   **CRITICAL:** diff against the committed baseline, never a fresh CI re-render — `ci.yml:50` runs an
   unpinned `brew install poppler webp`, so a renderer difference between the runner and the build Mac
   would produce different bytes for identical PDF input and red the gate for a non-defect (or mask a
   real change). Record `pdftoppm`/`cwebp` versions in the manifest and red the gate on a change, so a
   toolchain drift is a loud explicit override rather than 372 silent hash changes.
5. **Content correctness, not just integrity** (red team A6). Every hash in this design measures the
   artifact against itself, so a *wrong PDF* passes every check. Extend
   `scripts/check-book-consistency.mjs` (already validates the song index against the PDF via
   `pdfinfo`) to assert `rendered page count === pdfinfo page count === manifest.totalPages`, and wire
   it into `release.sh` alongside the additive gate. Assert in the additive gate that every song number
   present in the baseline still maps to the **same page** — that catches a wrong-but-longer PDF that a
   page-hash diff would just report as "372 modified, 40 new."
6. **Freeze the page-URL pad width NOW** at a fixed 3+ (or move to explicit 4). Page URLs are a
   function of `totalPages` (`web/src/app.js:257-262`), so crossing 999 → 1000 renames **every** image
   and silently voids the additive-only invariant every offline copy depends on. 627 pages of headroom,
   but changing the scheme later is itself a full-book rename.
7. **New tests, all added to the NAMED allowlist in `.github/workflows/ci.yml:58-69`:**
   `e2e/bookUpdate.test.mjs` (stage gate ordering, `canApplyNow` vetoes, the `resolveBundleUri`
   decision table, code Levenshtein distance), the extended `e2e/nearby-sync-contract.test.mjs`
   (four Swift guards), `sync-worker/test/` (the arming logic and the K-at-a-time queue), and a
   song-index parity test between MODO EMERGENCIA and the web app (§6 L2).
   **Never the `npm run test:e2e` glob** — it publishes to the live Mass room.

---

## 6. Fallback layers L1–L4

| | Covers | Human actions | Mesh sync survives? | Must be true BEFORE Mass |
|---|---|---|---|---|
| **L1** — app self-heals into a known-good bundle (`prev`, then the code-signed in-IPA copy) | the web bundle boots broken, blank, or crash-loops | **0** | **YES** — the Multipeer session is never torn down | The IPA actually contains `WebBundle` (§5.14.1 assertion); `sv_book_resolved`/manifests intact |
| **L2** — **MODO EMERGENCIA**, a native-drawn viewer that STILL FOLLOWS | the web layer is dead *or* alive-but-wrong (blank, wrong book, stuck) | 1 tap (or 0 — it is the automatic floor) | **YES** — the mesh listener is native (`PdfReaderApp.tsx:885+`) and keeps updating `currentPageRef` | The strip is visible and the choir has done the 20-second drill at practice |
| **L3** — the songbook PDF pre-positioned by AirDrop, in **"On My iPad"** (never iCloud Drive) | the app will not launch at all | ~3 (open Files → open PDF → jump to the song bookmark) | no | The PDF was AirDropped **at practice**, with per-song bookmarks, and its presence was re-checked in Part B |
| **L4** — **printed music** for the Mass's ~6 songs | the iPad is dead, lost, or drained | 0 devices | no | Printed on Saturday as a named step in the routine |

**L1 is ranked first because it costs zero human actions and preserves sync. L2 is ranked above the
AirDropped PDF for the same reason** — it is the only *manual* layer that still follows the director.

### L2 — MODO EMERGENCIA, in detail

Verified: `assets/signo_vivo_372.pdf` is **not** in the Resources build phase (Resources = `Expo.plist`,
`Images.xcassets`, `SplashScreen.storyboard`, `PrivacyInfo.xcprivacy`, `WebBundle`). But the in-IPA
`WebBundle` **does** contain all 372 page WebPs. **So the emergency viewer needs no PDF, no new
resource, and no IPA size increase.**

A pure React Native component (**no WebView**):

```tsx
<Image source={{ uri: `${resolvedBookDir}books/standard/pages/page-${pad(n)}.webp` }} resizeMode="contain" />
```

- **`resolvedBookDir` resolves per-file: active → `WebBundle.prev` → baked.** Never a hardcoded
  `bundleDirectory` (red team NI2/A4) — decision 1 freezes the baked book forever, so a viewer pinned
  to it would silently show a years-old songbook on a device whose active bundle is perfectly current.
- **It speaks SONG numbers, never page numbers** (red team H3). The choir's entire vocabulary is song
  numbers — the reader's status line renders `${song}. ${title}` (`web/src/app.js:791`) and never shows
  a page number. Today song ≈ page across almost the whole book (exactly one divergence: song 347 →
  page 346), so a rehearsal would work flawlessly and prove nothing; the first appended multi-page song
  permanently offsets every song after it. The data is already in the IPA:
  `${bundleDirectory}WebBundle/books/standard/pages.json` carries `songIndex[{song, page, title}]`,
  readable with the `expo-file-system` already imported and one `JSON.parse`. Resolve exactly the way
  `resolveSongPage` does (`web/src/app.js:700-711`) and render the same `212. Título` header the normal
  reader shows, so the screen looks like the app the choir knows. **CI asserts the two indexes resolve
  identically for every song.**
- **Discoverable and reversible.** A large, always-visible **`Volver`** button — the mode must be
  exitable in one tap by someone who entered it by accident. Entry is the permanently visible native
  strip (§5.6) plus a confirm tap, **not** a hidden 900 ms long-press overlapping the swipe surface.
- It prints the rendered book's `bookVersion` + `totalPages` on screen, so the operator can **see**
  which book they are looking at.
- **Director-down indicator** (red team H6): the native layer already receives the 1 s mesh heartbeat.
  Render a calm indicator that goes stale after ~20 s with no director packet — *"director sin
  señal"*. That converts an ambiguous frozen page into an unambiguous shared fact and removes the need
  for anyone to speak into a live microphone mid-Mass. When the **director's own** device lands in the
  native floor, it should broadcast a `director-down` hint over the still-live mesh session so
  followers self-announce.

The same long-press panel doubles as **DIAGNÓSTICO** — read-only, works with **no internet**, which is
the only place it matters: shell build, active `bookVersion` + `totalPages`, source, staged version,
quarantine reason and count, `sv_book_force_bundled` state, free disk, and two buttons: *"Usar la
versión de la app"* and *"Aplicar actualización"*. This is the durable replacement for the
numpad-`999` canary — which matters because **FOLWEB-07** ([`ia-audit-2026-07.md:2950`](ia-audit-2026-07.md))
is queued to make out-of-range entries error and stay put, silently killing that trick (see §11 Q2).

Any mode a device can be stuck in must be **visible without a secret gesture**: a small persistent
amber marker beside the build badge whenever `sv_book_force_bundled` is set or the active bundle was
reverted, mirrored as a distinct dashboard state (*"fijado a la versión de la app"*) rather than just an
old `bookVersion`. `sv_book_force_bundled` **auto-expires** after 14 days or on the next successful
`bridge-ready` from the baked bundle, whichever is later — a panic switch must not become permanent
policy by accident.

### L3 — the AirDropped PDF, made usable

Not just "AirDrop the PDF." Three specifics, or it fails on the day (red team H7):

1. **Generate it with real PDF bookmarks, one per song, labeled `212 — Título`,** from the same
   `songIndex` the app uses, in `web/build.mjs`, so it can never drift from the book. iOS Files exposes
   the outline — "song 212" becomes two taps instead of scrubbing a 372-page PDF.
2. **Name it so a stressed person finds it:** `CANCIONERO — abrir si la app falla.pdf`. Store in **"On
   My iPad"**, never iCloud Drive — a placeholder is useless with no internet.
3. **Re-verify its presence in Part B of the checklist every Mass**, so the ritual does not decay
   silently.

### L4 — printed music, not a set list

A list of six song numbers and titles is a list of pointers, not music. **Print the actual music for
the Mass's ~6 songs**, as a named step in the Saturday routine.

### The trigger word

One word the director says (or a gesture she already makes — holding up the iPad) meaning *"the strip,
MODO EMERGENCIA, estamos en la canción N."* Requirements: it must be sayable while singing; its literal
meaning must match the layer it invokes (**"PAPEL" is a bad choice** — it means *paper* and points at
L4); the script says **canción N, never página N**; and it is **drilled for 20 seconds at every
practice**, as a line item in `pre-mass-checklist.md` Part B — not rehearsed once, months earlier. A
protocol that lives only in someone's memory is not a fallback.

---

## 7. Red team findings and mitigations

23 attacks across three lenses. Ordered most severe first. **OPEN RISK** marks the ones that are only
partially answered.

| # | Attack | Severity | Answered by |
|---|---|---|---|
| A1 | `sv_book_staged.ready` is **persisted**, so the "structurally cannot fire during Mass" claim is false — seven followers see an apply prompt at 12:04 and tap it | fleet-bricking | §5.6: no ambient modal at all; the **live-internet veto** (successful check-in within 5 min) is true at practice and false in the church by definition; §5.5: `ready` expires after 12 h |
| A2 | `"*"` + zero-jitter 90 s check-in ⇒ 8 devices × concurrency 3 = 24 concurrent GETs, ~216 MB on one AP, right before Mass; the director's iPad stages too (role is `off` until a code is entered) | degraded Mass | §5.3: server-side K-at-a-time queue + `BOOK_UPDATE_ALLOW_FLEET`; §5.4: deterministic 0–20 min stagger from `sv_devid`; §5.4 throttle table |
| A3 | The additive-only gate is on the **PR** path, not the **publish** path; `release.sh` never runs `smoke-boot.mjs` — and a same-commit baseline lets the gate diff against itself | fleet-bricking | §5.14.3–4: gate moved into `release.sh` before `wrangler pages deploy`; baseline read via `git show HEAD:`; renderer versions recorded and gated |
| A5 | §5.12's guards, as originally scoped, missed `handleBundleRequest` (`:753`) and `didFinishReceivingResourceWithName` (`:1885`) — which has **no role guard** and never inspects `resourceName` | degraded Mass | §5.12: guard at the **receive boundary**, four guards not two, all regex-pinned; §5.8: dashboard `bookVersion` re-read from the mounted bundle so a clobber is visible |
| NI2 | Decision 1 freezes the baked book forever; every L1/L2 fallback then lands on a book that decays for years. A self-healing **director** silently drops the whole choir's new songs | fleet-bricking | §5.2/§5.7: `WebBundle.prev` kept permanently as the middle rung; §6 L2: per-file resolution active → prev → baked; §5.10: director-specific Alert. **OPEN RISK — see below** |
| NI3 | 11:52 in the parking lot: the device boots as a *follower* (role auto-restore is refused), so the `role !== director` veto is disarmed on the one device it protects | fleet-bricking | §5.6: live-internet veto; no ambient modal; 90-minute cold-boot cool-down for a last-known-director device; apply reachable only from the strip/DIAGNÓSTICO |
| NI1 | A stale device is dragged back to its last page by the 1 s heartbeat, forever, and cannot even navigate manually — on exactly the songs the update was published for | degraded Mass | §5.11: consume the `totalPages` already on the mesh payload; freeze instead of fight; suppress re-drive; loud native banner |
| H1 | `Reintentar` clears `webDead` and then awaits a now-async `resolveBundleUri` — the recovery UI disappears; if any I/O hangs the iPad is a black rectangle | fleet-bricking | §5.10 riders: keep the floor rendered until `bridge-ready`; never null `bundleUri` post-boot; §5.9: `Promise.race` 1500 ms so the resolver is total |
| NI4 / H2 | Staging and applying share a veto set; the mesh is up for the entire practice, which is the only window with internet ⇒ the downloader never runs where it is needed | degraded Mass | §5.4: split predicate table — staging throttles on mesh activity, apply keeps the full veto set |
| NI5 | A transient jetsam on the oldest iPad permanently blacklists the **current** book by content hash — it can never receive that book again | degraded Mass | §5.10: quarantine is a counter (3 lifetime failures), one retry/day, consecutive-only, explicit `attempts` reset on prove, surfaced in DIAGNÓSTICO |
| NI6 | The 20-second abort disarms *discovery*, not the six devices already sitting at `ready` — and the only person who knows is not in the building | degraded Mass | §5.4: server-authoritative staged state — absent/mismatched `bookUpdate` clears `sv_book_staged` and deletes the staged dir; §5.5: 12 h expiry |
| A4 | A defect that only reproduces on the choir's hardware fires all 8 watchdogs at once; the self-heal works perfectly and silently splits the fleet onto two different books | degraded Mass | §5.10: loud non-dismissible `LIBRO ANTERIOR` banner + `sv_book_reverted`; §5.11: out-of-range director page becomes a visible state, not a clamp |
| A6 / NI7 | Every integrity check measures the artifact against itself: a **wrong PDF**, or a manifest that omits a file, passes everything and installs cleanly on all 8 | degraded Mass | §5.1.1 walk-the-tree manifest; §5.5.6 numeric page assertions; §5.14.5 `sourcePdfSha256` + `pdfinfo` cross-check + song→page stability; §10 requires a human to open a **named new song** before a second device is armed |
| A7 | §5.9 moves manifest parsing onto the boot path; 8 identical aging iPads cold-boot together and cross the timeout together ⇒ correlated silent downgrade with no defect to find | degraded Mass | §5.9.1 cached `sv_book_resolved` fast path; §5.10b non-sticky pre-boot fallback with a distinct `preboot-timeout` breadcrumb; threshold raised to 12 s |
| H3 | MODO EMERGENCIA addressed by **page**, while the choir speaks **song** — and today song ≈ page, so the rehearsal certifies a protocol that breaks the first time a multi-page song is appended | degraded Mass | §6 L2: song numbers only, resolved from the in-IPA `pages.json` the same way `resolveSongPage` does; CI parity assertion |
| H5 | The emergency mode has two entrances and **zero exits**, entered by a hidden long-press that overlaps the page-swipe surface | degraded Mass | §6 L2: always-visible strip + confirm to enter; large `Volver` to exit; verify no edge-swipe conflict |
| H4 | `BOOK_FORCE_BAKED_CODE = 744668487` is one digit from `SOFT_RESET_CODE = 744668486`; nothing ever clears the flag and nothing displays it | degraded Mass | §5.6: Levenshtein ≥ 3, unit-tested; §6: auto-expiry; visible amber marker + distinct dashboard state |
| NI8 | Every "which book does this iPad have?" instrument needs the relay; inside the church there is none, and DIAGNÓSTICO is a per-device obscure gesture | degraded Mass | §10 Part B: **one director-driven, zero-network group check** — director jumps to the last page; every device showing the canary is current, every device showing a song is stale. Ten seconds, no internet, no maintainer |
| H6 | The trigger word requires the director to notice a failure and speak into a live mic mid-Mass; "PAPEL" points at the wrong layer | degraded Mass | §6: `director sin señal` staleness indicator + `director-down` mesh hint (nobody has to speak); word chosen to match the layer; 20-second drill every practice |
| H7 | L3 assumes a PDF found and navigated under stress, with no index and no shared way to say *where*; L4 is a list of pointers, not music | degraded Mass | §6 L3/L4: per-song PDF bookmarks generated from the same index, self-describing filename, presence re-checked every Mass; L4 redefined as printed **music** |
| A-swap | A crash between the two `moveAsync` calls | benign | §5.7: no Documents/WebBundle ⇒ `resolveBundleUri` falls through to the baked bundle ⇒ working songbook, mesh intact, zero taps. Plus the boot sweep |
| A-disk | An install transiently needs ~3× the bundle; there is no free-space check anywhere today | low | §5.5.3 disk precheck; §5.7 renames-only swap moves zero bytes |
| H8 | Every arm/abort/rollback control needs one specific person with a laptop, terminal, wrangler creds and internet | degraded Mass | Partially — see OPEN RISK below |

### OPEN RISKS — not fully closed by this plan

1. **The baked floor decays (NI2).** `WebBundle.prev` fixes the common case, but a device that has
   **never** successfully applied still falls all the way to the build-382 book, and every device does
   after a fresh TestFlight install. Decision 1 says one more build; realistically the baked floor
   should be refreshed roughly **annually** with a routine build whose only content is a current
   `WebBundle`. **See §11 Q1.**
2. **Single-operator dependency (H8).** Arm, abort, and rollback are all `wrangler.jsonc` + `npx
   wrangler deploy`. This plan mitigates the *choir-facing* half (nothing a singer needs during Mass is
   behind a numeric code) but not the operator half. The real fix is a token-gated arm/abort toggle on
   the existing `FLEET_DASHBOARD_KEY` page (`sync-worker/src/index.ts:674-688`) so it works from a
   phone with no terminal — plus one other person with a laminated card, who has executed it once at
   practice **without Miguel in the room**. **See §11 Q3.**
3. **The toolchain blocker (§8 M0).** Nothing in this plan is buildable today. Restoring
   `node_modules` from the committed locks reproduces a **different pod set** than build 381 actually
   compiled against, so the first archive is a genuinely new, unproven configuration on a fleet with no
   remedy at Mass.
4. **Everything native since build 377 is git-only, uncompiled and device-unverified.** The one
   budgeted TestFlight build carries all of it *plus* this plan. A compile failure has no written
   contingency ([`ia-audit-2026-07/map-prior-art.md:448-450`](ia-audit-2026-07/map-prior-art.md)).
5. **The additive-only invariant is already historically violated** (build 377 / PR #257 re-rendered
   ~290 pages). Every cached copy from before that build is already inconsistent; the gate protects the
   future, not the past.
6. **Correlated content error survives.** No amount of hashing catches a wrong-but-well-formed PDF.
   §10's human "open song N and confirm out loud" step is the only check independent of the build, and
   it is a **human** check.

---

## 8. Build order

Each milestone is independently shippable and green. **M0 is a hard gate and is independently
verifiable**, so if the toolchain turns out to be a swamp, nothing else has been wasted — and the
`expo-updates` question is already answered (§4) so it cannot become the swamp.

| M | What | Gate — how you know it is done | Risk |
|---|---|---|---|
| **M0** | **TOOLCHAIN. Nothing else starts.** Install Xcode.app; `xcode-select -s`; restore `node_modules` (**explicit permission required** — lockfile-mutating, §12 of the global rules); resolve the `Podfile.lock` vs `Manifest.lock` drift deliberately; `export LANG=en_US.UTF-8`; get `pod install` to run for real **or** confirm the `cp ios/Pods/Manifest.lock ios/Podfile.lock` workaround still holds | `bash scripts/release.sh` with `SKIP_NATIVE=0` produces an IPA **on HEAD UNCHANGED**, and it installs and boots on a simulator. No source edits until this passes. | **Blocking.** `xcodebuild -version` currently errors; `node_modules` absent from both checkouts |
| **M1** | `emitBundleManifest` in `web/build.mjs`; `bookVersion` into `#pages-data`; `release.sh` WebBundle assertions; the `SKIP_NATIVE` sync fix; additive gate in `smoke-boot.mjs`; gate moved onto the publish path; `check-book-consistency` extension; pad-width freeze; commit `web/manifest-baseline.json` | CI green; `node scripts/smoke-boot.mjs` passes; deliberately corrupting one page WebP reds the gate; `https://<preview>/bundle-manifest.json` serves | Web-only, no native risk. **Immediately valuable — it makes "which book is this" answerable at all** |
| **M2** | `resolveBundleUri` rewrite; the four re-resolve riders; `sv_book_resolved` fast path; boot-attempt counter; pre-boot watchdog; boot sweep; the self-heal ladder + `LIBRO ANTERIOR` banner | `e2e/bookUpdate.test.mjs` decision-table tests green; simulator: a hand-planted broken `Documents/WebBundle` self-heals with zero taps (§9) | **This is the D1/D2 fix. It is worth the native build on its own, even if the downloader is never enabled.** Ship it first inside the build |
| **M3** | MODO EMERGENCIA + DIAGNÓSTICO + the native strip; song-index parity test; the mesh stale-book handling (§5.11) | Simulator: kill the web layer, confirm the emergency viewer renders and **still follows** a simulated director page event | Native, self-contained, no server dependency. Highest value per line in the plan |
| **M4** | The four Swift mesh guards + source-regex pins | `e2e/nearby-sync-contract.test.mjs` green; grep confirms all four sites guarded | 4 lines. Closes Q5 |
| **M5** | `src/bookUpdate.ts` (`stageBook`, `canApplyNow`, `applyStagedBundle`); `fleetCheckin` response handling; worker vars + arming logic + K-queue; dashboard `Libro` column; `bundle-error` wiring | Local end-to-end against `wrangler dev` + a local static server (§9); worker tests in the named CI list | **Ships DORMANT** — both `BOOK_UPDATE_*` strings empty |
| **M6** | Checklist updates: RELVER-01 canary-URL correction, the director last-page group check, the trigger-word drill, the AirDrop ritual, the new codes | `docs/pre-mass-checklist.md` updated and printed | Doc-only |
| **M7** | **The single TestFlight build.** `director-codes.private.json` present (§10). Transporter upload | Build installs on the oldest iPad; badge shows the new number; `♪ 999` lands on 372 | One shot |
| **M8** | Practice day: arm ONE iPad, prove staging → apply → prove → dashboard, rehearse the trigger word + MODO EMERGENCIA with the group | Dashboard `Libro` column flips; a named new song opens and the director confirms it out loud | The real gate |
| **M9** | Fleet-wide: `BOOK_UPDATE_DEVICES="*"` + `BOOK_UPDATE_ALLOW_FLEET="yes"`, redeploy | All devices report the new `bookVersion` | **No further native build is ever required for a new PDF** |

---

## 9. Local test plan — simulator + local server, zero choir devices, zero production

**Absolute rules:** never `npm run test:e2e` (it publishes to the PROD relay room `alvernia-main` and
would flip live followers' pages). Never point a test client at `signovivo-sync.*.workers.dev` or
`signovivo.com`. `sync-worker/test/run-a2.sh` already refuses any base matching
`/signovivo|workers\.dev/` — keep that guard.

```bash
cd /Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a

# ── 0. Toolchain (M0) ─────────────────────────────────────────────────────────
xcodebuild -version && xcrun simctl list devices available | head
npm ci                                        # ASK FIRST — lockfile-mutating
export LANG=en_US.UTF-8 && (cd ios && pod install)   # or the Manifest.lock workaround

# ── 1. Build + gates (M1) ─────────────────────────────────────────────────────
node web/build.mjs
node scripts/smoke-boot.mjs                   # page-count cross-check + additive gate
node scripts/check-book-consistency.mjs
node -e "const m=require('./web/dist/bundle-manifest.json');console.log(m.bookVersion,m.totalPages,m.files.length)"

# negative test: the additive gate MUST go red
cp web/dist/books/standard/pages/page-005.webp /tmp/p5.bak
printf 'x' >> web/dist/books/standard/pages/page-005.webp
node scripts/smoke-boot.mjs; echo "expect NON-ZERO exit: $?"
cp /tmp/p5.bak web/dist/books/standard/pages/page-005.webp

# ── 2. Pure-module tests (M2/M5) ──────────────────────────────────────────────
node --test e2e/bookUpdate.test.mjs
node --test e2e/nearby-sync-contract.test.mjs
npx tsc --noEmit

# ── 3. Local origin standing in for signovivo.com ─────────────────────────────
npx --yes http-server web/dist -p 8788 --cors -c-1     # ANY static server; -c-1 = no cache
curl -s http://127.0.0.1:8788/bundle-manifest.json | head -c 200

# ── 4. Local worker, never prod ───────────────────────────────────────────────
cd sync-worker && npx wrangler dev --port 8787 --var BOOK_UPDATE_VERSION:bv_<hash> \
  --var BOOK_UPDATE_DEVICES:<sim-sv_devid> --var BOOK_UPDATE_BASE:http://127.0.0.1:8788
curl -s -XPOST localhost:8787/fleet/checkin -H 'content-type: application/json' \
  -d '{"deviceId":"<sim-sv_devid>","surface":"native","nativeBuild":382}' | jq .
bash sync-worker/test/run-a2.sh                        # env-gated behavioral harness
```

**Simulator.** Build and run with the iOS Simulator tooling (or `xcodebuild -scheme SignoVivo -sdk
iphonesimulator`), pointing `RELAY_BASE` at `http://127.0.0.1:8787` via a debug-only override. Then
drive the scenarios by manipulating the app's container directly:

```bash
APP=$(xcrun simctl get_app_container booted com.mysupertech.signovivo data)
open "$APP/Documents"
```

| Scenario | How to force it | Expected |
|---|---|---|
| **D1** — stale Documents bundle | `cp -R web/dist "$APP/Documents/WebBundle"` then **delete** its `bundle-manifest.json`. Relaunch | Boots the **baked** bundle; breadcrumb records the eviction; dashboard shows `bundleSource: bundled` |
| **D2** — broken bundle self-heal | With a Documents bundle present, truncate its `app.js` to 0 bytes. Relaunch | 6 s watchdog → remount → quarantine to `WebBundle.bad-*` → mount prev/baked → `LIBRO ANTERIOR` banner. **Zero taps.** `WebBundle.bad-*` still on disk |
| **Hard-crash net** | Replace `index.html` with a script that calls `while(true){}` before bridge-ready, twice | `sv_book_boot.attempts` reaches 2 → quarantine **before** mounting |
| **No baked bundle** | Rename `WebBundle` inside the built `.app` | `FATAL:no-baked-bundle` breadcrumb; boots MODO EMERGENCIA, not a black screen |
| **Staging** | Arm the local worker for the sim's `sv_devid`; foreground the app | `sv_book_staged` progresses; dashboard shows `downloading:NN%` |
| **Half-stage inert** | Kill the app mid-download, relaunch | Resume re-verifies; `ready` stays false; **`WebBundle` untouched** |
| **Corrupt download** | Serve one page byte-modified from the local origin | `error:file-hash`, one retry, abort. No swap |
| **Missing page** | Delete one page from the local origin's `dist` | Completeness gate fails on the `1..totalPages` assertion. No swap |
| **Shrunk book** | Serve a manifest with `totalPages: 300` | Rejected — decision 7 |
| **Apply veto** | Simulate a director snapshot / mesh peer / a check-in older than 5 min | *"Ahora no"*, nothing changes |
| **Apply happy path** | All vetoes clear, tap the strip | `Actualizando…`, swap, remount, `bridge-ready`, `sv_book_active` written, `prev` promoted, resume at `sv_lastpage` |
| **Mid-swap crash** | Kill the app between the two moves (breakpoint or an injected throw) | Relaunch boots the **baked** bundle; sweep cleans `.tmp`; apply again works |
| **Server revoke** | Clear `BOOK_UPDATE_VERSION` on the local worker, foreground | `sv_book_staged` cleared, `WebBundleStaged/` deleted |
| **Stale-book mesh** | Two sims: one with 372 pages, one with 371; drive a page-372 event | Stale device freezes on 371 with the banner, **manual navigation works**, no 1 s fight |
| **MODO EMERGENCIA follows** | Kill the web layer on a follower sim; send mesh page events | The native viewer advances with the director |
| **Song-index parity** | CI test, plus manually enter a song number in both viewers | Identical page for every song |

---

## 10. Rollout — the EXISTING ritual, corrected

This extends [`pre-mass-checklist.md`](pre-mass-checklist.md). It does not replace it. **The one hard
rule stands: new builds go out Wed/Sat practice only, never on a Mass day (Sun/Thu), and never deploy a
fix during Mass** (`pre-mass-checklist.md:7-9`, `:65`).

### Prerequisite — `director-codes.private.json`

**Run `release.sh` from the MAIN checkout `/Users/cazares/src/alvernia-reader/`, or copy the file
there first.** It is gitignored and exists only in the main checkout. `release.sh` **warns and
proceeds** without it, producing a build where **nobody can become director** — this caused a real Mass
outage (build 371). Before the archive, verify the file is present and non-empty. Treat its absence as
a hard stop, not a warning.

### Correction to the existing canary step (RELVER-01 / RELVER-12)

`pre-mass-checklist.md:24` points the canary at `signovivo.com?env=staging`. **That proves nothing** —
`?env=staging` selects the relay **room** (`web/src/lib/svRelayRoom.js:31`), not content, and nothing
repo-wide selects content by it. Use the printed preview URL (`staging.alvernia-reader.pages.dev/?env=staging`).
And note what staging **cannot** prove: `release.sh:52-53` **skips the `ios/WebBundle` sync** under
`STAGING=1`, and the native app has no staging entry at all — `src/directorRelaySync.js:13` hardcodes
the LIVE `alvernia-main` room. **So the one-iPad proof cannot ride the web staging canary. It runs at
Wed/Sat practice, on the live room, on the new TestFlight build, with exactly one `deviceId` armed.**

### A. Publishing a new songbook PDF (Wed/Sat, at home, with wifi)

1. Drop the new PDF at `assets/signo_vivo_<N>.pdf`; confirm it is **additive** (old pages unchanged,
   new songs appended).
2. `bash scripts/release.sh` — this now runs the additive gate and the consistency check **before**
   deploying, and hard-fails on a non-additive book unless `ADDITIVE_OVERRIDE` is typed.
3. Verify `https://signovivo.com/bundle-manifest.json` serves the new `bookVersion` and the expected
   `totalPages`.
4. Commit the new `web/manifest-baseline.json` **as its own commit**.
5. Regenerate and re-AirDrop the bookmarked `CANCIONERO — abrir si la app falla.pdf` at the next
   practice (L3), and print the music for the coming Sunday's ~6 songs (L4).

### B. Arming — one iPad first (decision 3)

1. Get the target `sv_devid` from the fleet dashboard (arm the **OLDEST** iPad — worst case first).
2. Set `BOOK_UPDATE_VERSION` and `BOOK_UPDATE_DEVICES` in `sync-worker/wrangler.jsonc`;
   `npx wrangler deploy`. Leave `BOOK_UPDATE_ALLOW_FLEET` empty.
3. Watch the dashboard `Libro` column: `idle` → `downloading:NN%` → `ready`.
4. On the iPad, tap the strip → confirm. Column flips to the new `bookVersion` with
   `bundleSource: documents`.
5. **The human content check, and it is not optional:** open a **named NEW song by number** that the
   director confirms **out loud** is correct. Hashes verify integrity; only a person verifies
   *correctness* (red team A6).
6. Long-press → DIAGNÓSTICO to confirm on-device with no network.
7. **Rehearse with the group, once, ~5 minutes:** the trigger word, entering MODO EMERGENCIA from the
   strip, `Volver`, and where the AirDropped PDF lives. Then a 20-second drill at **every** subsequent
   practice.

### C. Fleet-wide

`BOOK_UPDATE_DEVICES="*"` **and** `BOOK_UPDATE_ALLOW_FLEET="yes"`, redeploy. The worker hands the
update to **two devices at a time**. Send the already-drafted Spanish message
([`major-update-2026-07.md:506-508`](major-update-2026-07.md)):

> 🎶 Coro: hay una actualización lista para HOY en el ensayo. Con wifi, abre la app y (iPad) actualiza
> en TestFlight / (teléfono) recarga signovivo.com. Revisa que abajo diga **LISTO** y la misma versión
> que los demás.

### D. Abort and rollback

| Situation | Command | Effect |
|---|---|---|
| Abort a rollout | `BOOK_UPDATE_DEVICES=""` + `npx wrangler deploy` | ~20 s. Disarms discovery **and revokes staged copies** on any device that touches wifi once (§5.4). No app change, no physical access |
| Roll back the book | `bash scripts/rollback-web.sh`, then point `BOOK_UPDATE_VERSION` at the OLD `bookVersion` and redeploy | Devices re-stage the older version by plain **inequality** — the client never assumes newer-is-better and treats the server as the sole authority |
| Kill it entirely | `SV_BOOK_DL_KILL = true` in `PdfReaderApp.tsx` | Requires a build. The source-level last resort |
| A bad web deploy | `bash scripts/rollback-web.sh` | Unchanged, existing ritual |
| Worker | `cd sync-worker && npx wrangler rollback` | Unchanged, existing ritual |

### E. At the room, 12:00–12:15 — additions to Part B

Keep every existing item. Add:

- ☐ **The zero-network group book check (10 seconds, director-driven).** The director's first act after
  becoming director: **jump to the LAST page of the book** (`♪ 999 → Abrir` — out-of-range routes to
  each device's own `state.totalPages`, `web/src/app.js:700-711`). Every device showing the canary
  digits is current; **every device showing a song page instead is stale** — pull it aside or swap it
  for a spare **before** Mass. No internet, no server, no maintainer.
- ☐ No device shows the amber `LIBRO ANTERIOR` banner or the `fijado a la versión de la app` marker.
- ☐ `CANCIONERO — abrir si la app falla.pdf` is present in **On My iPad** on every device.
- ☐ The printed music for today's ~6 songs is on the stand.
- ☐ 20-second drill: trigger word → strip → MODO EMERGENCIA → `Volver`.

---

## 11. Open questions for Miguel

1. **Is "one more build" really final, or one more build *this year*?** The baked in-IPA bundle is the
   floor L1 and L2 stand on, and it freezes at build 382's book forever. `WebBundle.prev` covers
   devices that have applied at least once, but a fresh TestFlight install still lands on a
   progressively older floor. Do you want a **routine ~annual refresh build** whose only content is a
   current `WebBundle`, or do you accept a floor that decays? (OPEN RISK 1.)
2. **FOLWEB-07 vs the `♪ 999` canary.** FOLWEB-07 is queued to make out-of-range numpad entries error
   and stay put ([`ia-audit-2026-07.md:2950`](ia-audit-2026-07.md)), which would silently kill the
   zero-network group book check in §10.E. Three options: (a) carve out an explicit exception for
   out-of-range → last page; (b) land DIAGNÓSTICO + the `Libro` column first and let FOLWEB-07 proceed;
   (c) add a dedicated "ir a la última página" affordance. Which?
3. **Who is the second operator, and do you want the phone-operable arm/abort?** Every control today
   needs your laptop, terminal, wrangler creds and internet. Adding a token-gated arm/abort toggle to
   the existing `FLEET_DASHBOARD_KEY` page (`sync-worker/src/index.ts:674-688`) is maybe an hour of
   worker work and makes an abort possible from your phone. And who gets the laminated card and
   executes the drill **without you in the room**? (OPEN RISK 2.)
4. **Confirm Q5 / P-MESH is closed in the RETIRE direction.** §5.12 disables peer bundle push with four
   guards. [`app-hardening-plan.md:609-611`](app-hardening-plan.md) already leaned that way. Confirm,
   so no future PR reintroduces the book-regression vector — or say keep-and-fix and accept three Swift
   fixes in a language that cannot be compiled here.
5. **The trigger word.** It must be sayable while singing, must point at MODO EMERGENCIA (not paper),
   and ideally is a gesture you already make. "PAPEL" is rejected in §6. What is it?
6. **Add `assets/signo_vivo_372.pdf` to the Resources build phase?** +14.8 MB IPA, no pod change, no
   code change, so any working iPad can AirDrop the exact songbook to a dead one without Files.app
   spelunking. Worth it, or keep the IPA small and rely on the pre-positioned copy?
7. **`node_modules` restoration (M0) needs your explicit go-ahead.** `npm ci` restores the committed
   pins (Expo 54.0.33, no RNCPicker), which is **not** the pod set build 381 compiled against
   (54.0.34 + RNCPicker 2.11.4). The first archive after restoration is a genuinely new configuration.
   Do you want to (a) `npm ci` and accept the new config, (b) reconstruct the 54.0.34 tree to match
   build 381 exactly, or (c) deliberately re-baseline both lockfiles as part of M0?
8. **Fleet concurrency.** `BOOK_UPDATE_CONCURRENCY` defaults to 2. On practice wifi with 8 iPads, is
   two at a time right, or do you want strictly one at a time on the first fleet pass?
9. **Where does the apply affordance live, exactly?** §5.6 proposes a permanently visible native strip
   showing the current song number, which doubles as the MODO EMERGENCIA entrance. That is new
   permanent chrome on an iPad in portrait. Do you want to see a mockup before M3, or is a thin bottom
   strip fine?
10. **Should a `bookVersion` mismatch be surfaced to the whole choir, or only the operator?** §5.11's
    stale-book banner is per-device and blunt (*"Cancionero desactualizado…"*). It is honest and it is
    also visible to a singer mid-Mass. Keep it loud, or make it a small marker plus a dashboard row?
