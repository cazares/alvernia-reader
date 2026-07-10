# VESTIG lens — post-374 vestigial debt & naming (detail file)

> Audit lens: remnants of the deleted two-book / geo / Sión / unlock system + dead UI + naming debt.
> Verified at HEAD `d5075091` (build **381**, 2026-07-08) — NOTE this is 4 commits past the maps'
> HEAD 16244b25/377; PRs #269–271 landed in between (spinner cap, web fleet-modal removal, sync-spinner
> simplification + **PDF rename `assets/alvernia_manual_2.pdf` → `assets/signo_vivo_371.pdf`**).
> Current file sizes: web/src/app.js = 3572 lines, PdfReaderApp.tsx = 1123, sync-worker/src/index.ts = 828.
> Every anchor below re-verified against this HEAD.
>
> Dedupe honored: nothing here re-reports prior-art KNOWN-FINDINGS or PLANNED-WORK (P8 already owns:
> #books-data inline blob, dead offline-gate UI incl. isOfflineBundleReady, dead state fields
> syncRole/relay.appliedPage/relay.manualClose, index-panel subsystem, bump-build offlineWebBundle block,
> HANDOFF/atlas/contracts drift, .d.ts nonStandard narrowing). Where a finding touches a known id it is
> explicitly a DELTA and names the id.

---

## VESTIG-01 — clean-header-boxes.py default input points at the deleted PDF (HARMFUL-CAN-FIRE)

**Class:** (a) HARMFUL-CAN-FIRE (deterministic tool failure) · **Severity:** low · **Ship:** repo-only edit (no deploy; output feeds both web and native book pipelines → recorded as `multi`).

**Trigger/repro:** PR #271 (`e4d1a014`, in build 381) renamed the songbook PDF to
`assets/signo_vivo_371.pdf` — verified: `ls assets/` shows only `signo_vivo_371.pdf`. But the canonical
header-cleanup tool still defaults to the old path:

- `scripts/clean-header-boxes.py:26` — `ap.add_argument("--in", dest="src", default=os.path.join(ROOT, "assets/alvernia_manual_2.pdf"))`
- `scripts/clean-header-boxes.py:11` — usage docstring shows `[--in assets/alvernia_manual_2.pdf]`

Run `python3 scripts/clean-header-boxes.py` with no `--in` (exactly how the shipped 290-page cleanup
was run per project memory) → the open of a nonexistent file raises. This is the tool project memory
designates for future header cleanups ("pixel-verify edits" gotcha), so the next hymnal-cleanup session
hits it first thing. Everything else was updated in #271 (`web/build.mjs:675`,
`scripts/check-book-consistency.mjs:22`, `e2e/eas-config.test.mjs:59`, the song-index header
`src/alverniaManual2SongIndex.js:1`) — only this script and one comment (see VESTIG-09) were missed.

**Fix:** change both lines to `assets/signo_vivo_371.pdf`. Optionally fail with a friendly message
listing `assets/*.pdf` when the input is missing.

**Acceptance:** `python3 scripts/clean-header-boxes.py --help` shows the new default;
`grep -rn alvernia_manual_2 scripts/` returns nothing.

**Test idea:** a trivial e2e assertion that every `assets/*.pdf` path mentioned under `scripts/` and
`web/build.mjs` exists on disk (would also have caught the #271 rename in the other 4 files had they
been missed).

---

## VESTIG-02 — Worker comments + README still claim transmitter codes are "hardcoded in this PUBLIC repo" (false since build 368) and misdocument the secret set

**Class:** (b) CONFUSING-DEBT (security-model misdirection) · **Severity:** medium · **Ship:** worker-only (comments/docs; redeploy optional).

**Evidence (all current HEAD):**
- `sync-worker/src/index.ts:25` — FLEET_DASHBOARD_KEY docblock: transmitter codes "…are hardcoded in
  this PUBLIC repo, so gating PII behind them would expose every number."
- `sync-worker/src/index.ts:672` — fleet gate comment repeats it verbatim: "NOT the transmitter codes —
  those are hardcoded in this public repo…"
- Both are FALSE at HEAD: `validTransmitterCodes` (index.ts:387-396) reads ONLY the `TRANSMITTER_CODES`
  secret, fail-closed, zero hardcoded codes; index.ts:389 itself says "the real phone-number director
  codes must never live in this PUBLIC repo." The two stale comments directly contradict the line-389
  invariant.
- `sync-worker/README.md:67` — "`RELAY_DIRECTOR_TOKEN` is the only write credential." False:
  `X-Director-Code` ∈ TRANSMITTER_CODES also authorizes /publish (index.ts:780-787).
- `sync-worker/wrangler.jsonc:15-16` — "RELAY_DIRECTOR_TOKEN is a SECRET — set via wrangler secret put"
  names only ONE of the THREE required secrets. `TRANSMITTER_CODES` (publish fail-closed without it)
  and `FLEET_DASHBOARD_KEY` (dashboard 401s without it) are undocumented here.

**Why medium, not low:** two concrete failure modes for a maintainer taking the docs at face value:
(1) "codes are already public" invites committing real codes to the repo — re-opening the exact A1
class that was rotated out on 2026-07-05; (2) a fresh worker deploy provisioned per wrangler.jsonc/README
sets only RELAY_DIRECTOR_TOKEN → /publish fail-closed rejects every native director at the next Mass
(silent — the outer catch masks worker errors as EMPTY_SNAPSHOT).

**Fix:** rewrite index.ts:23-26 and :670-673 to state the current model ("codes live ONLY in the
TRANSMITTER_CODES secret; the fleet key is separate so a transmitter code never unlocks PII");
fix README.md:67 to name both write credentials; list all three secrets in wrangler.jsonc's comment.

**Acceptance:** `grep -rn "hardcoded in this" sync-worker/` → 0 hits; README names both /publish
credentials; wrangler.jsonc comment names all three secrets.

**Test idea:** none needed (comments); optionally a README lint that greps for the retired claim.

---

## VESTIG-03 — sw.js / app.js comments still describe the retired `?admin=1` preload flow (and "370 pages")

**Class:** (b) CONFUSING-DEBT (describes a retired OPERATOR workflow) · **Severity:** low · **Ship:** web-only (comment-only).

**Evidence:**
- `web/src/sw.js:88` — "We deliberately do NOT pre-fetch all **370** pages" (book is 371;
  `STANDARD_TOTAL_PAGES = 371` at app.js:188).
- `web/src/sw.js:91` — "the offline iPad still preloads the whole manual via **signovivo.com?admin=1**."
  There is NO ?admin handling anywhere at HEAD (`grep -n 'admin' web/src/app.js web/src/sw.js` → only
  these comments); the admin preload flow died with the two-book refactor. Today the precache is the
  automatic `ensureOfflineBundle` path (app.js:~617+), no query param involved.
- `web/src/app.js:3414` — "(offline / ?admin" in the search-index comment, same ghost.

**Failure mode:** this comment is written as ops guidance. Someone preparing a new parish iPad per the
comment browses to `signovivo.com?admin=1`, sees nothing special, and either assumes breakage or assumes
the iPad is now preloaded when only lazy caching ran. (The real flow needs no action — which is exactly
what the comment should say.)

**Fix:** rewrite sw.js:87-91 to describe the current automatic deferred precache; fix "370"→"371" (or
"all pages"); drop the `?admin` clause in app.js:3414.

**Acceptance:** `grep -rn 'admin=1\|?admin' web/src` → 0 hits.

---

## VESTIG-04 — Swift nil-page-guard comment justifies a load-bearing guard with DELETED behavior (hymns-4 / geo / bookFromSync)

**Class:** (b) CONFUSING-DEBT (can induce a harmful "cleanup") · **Severity:** low · **Ship:** native-build (comment-only, rides any next build).

**Evidence:** `ios/SignoVivo/DirectorSyncModule.swift:222-229` (`sendCurrentPageSnapshot`): the comment
explains that sending a nil-page snapshot would ship `bookId=""/mode=""` which "the follower's JS
(bookFromSync) hard-coerces to the DEFAULT book 'hymns-4', yanking a correctly geo-resolved 'standard'
(Del Rio) follower off its book onto hymns-4 page 1."

At HEAD: `bookFromSync` does not exist in web/src (grep → 0), there is no hymns-4, no geo, no book
coercion. The guard itself (`guard currentRole == "director", let page = currentPageNumber`) is STILL
correct and load-bearing — an empty/nil-page snapshot is a wrong guess that could flash page 1 — but
its entire written rationale is dead. A maintainer verifying the rationale ("none of this exists
anymore") could plausibly delete the guard during cleanup, reintroducing a spurious page-1 snapshot to
newly-connected followers.

**Fix:** rewrite the comment with the current rationale: "never broadcast a snapshot before the director
has a real page; a nil-page guess would land followers on page 1. (Historical: this once also protected
the deleted two-book coercion.)" Keep the guard byte-identical.

**Acceptance:** comment no longer references hymns-4/geo/bookFromSync; guard logic unchanged
(`git diff` shows comment-only change). Note: Swift is uncompiled in this environment — comment-only
changes are the only safe class to ship without the device day.

---

## VESTIG-05 — "2s mesh heartbeat" comment drift (actual: 1s) across 5 sites; NEW-DIR-3's window rationale cites the wrong cadence

**Class:** (b) CONFUSING-DEBT · **Severity:** low · **Ship:** native-build (comment-only) — group with VESTIG-04.

**Evidence:** actual mesh heartbeat interval is **1000 ms** — `PdfReaderApp.tsx:392-400`
(`meshHeartbeatRef.current = setInterval(..., 1000)`). Comments claiming 2s:
- `PdfReaderApp.tsx:68` — "a director's mesh page arrives on a ~2s heartbeat" — this is the stated
  basis for the NEW-DIR-3 8s live-director recency window: the window reads as ~4 beats of a 2s cadence;
  at 1s it's 8 beats. Not a bug (more generous), but the documented derivation is wrong, so anyone
  re-tuning the window from the comment miscalculates by 2×.
- `PdfReaderApp.tsx:263` — "the 2s heartbeat + page events would otherwise grow native heap…"
- `PdfReaderApp.tsx:780` — "The 2s mesh heartbeat would then de-dupe…"
- `PdfReaderApp.tsx:900` — "De-dupe the 2s mesh heartbeat…"
- `ios/SignoVivo/DirectorSyncModule.swift:227` — "The 2s mesh heartbeat + 1.5s snapshot-probe + 8s hello…"
  (same stale block as VESTIG-04).

**Adjacent dead work (same function, note or fix together):** `startDirectorHeartbeat`
(PdfReaderApp.tsx:391-400) unconditionally starts the 1s mesh timer; for a transmitter-only director
(roleRef `off` + explicitTransmitterRef, no mesh) every tick early-returns at :394 — a useless 1 Hz
timer for the whole Mass. Harmless but confusing; either guard the mesh timer or add a comment saying
the no-op is deliberate. CAUTION: restructuring this function is adjacent to HELD design H2
(heartbeat-effect split) — prefer comment-only until H2 lands.

**Fix:** s/2s/1s/ at the five sites; restate the :68 derivation ("8s ≈ 8 beats of the 1s heartbeat").

**Acceptance:** `grep -rn '2s mesh\|2s heartbeat' PdfReaderApp.tsx ios/` → 0 hits.

---

## VESTIG-06 — `__SIGNO_VINO_INITIAL_BOOK` is a dead injected global, plus the native state that exists only to feed it

**Class:** (c) INERT dead code with a REMOVAL TRAP · **Severity:** low · **Ship:** native-build.

**Evidence:**
- Injection: `PdfReaderApp.tsx:1038` (`preloadScript`), dep at :1041.
- Feeding plumbing that exists ONLY for it: `const [initialBook, setInitialBook] = useState<BookId>(DEFAULT_BOOK)`
  at :90; `setInitialBook(startBook)` at :839 (always `"standard"`); `initialBook` appears nowhere else.
- Web reader: ZERO reads. `grep -n 'SIGNO_VINO' web/src/app.js` → only `__SIGNO_VINO_NATIVE_FILE_MODE`
  (:227) and `__SIGNO_VINO_NATIVE_BUNDLE_VERSION` (:2903, :3477-3480). Reads of INITIAL_BOOK were
  removed in `318d6f39` (the two-book deletion).

**Wire-safety classification — needs-deprecation-window, NOT free:** old web bundles DID read this
global (present since build 332, per `git log -S`). Old native shells ship their own matching old web
copy, so shell-side removal can't hurt THEM. The one skew that matters: `resolveBundleUri`
(PdfReaderApp.tsx:813-826) prefers a mesh-pushed `Documents/WebBundle` unconditionally — a stale
pre-374 pushed bundle running inside a FUTURE shell that stopped injecting the global would boot its
two-book logic with `undefined` initial book. That scenario is already broken in worse ways (it calls
the retired geo endpoints), and the OPEN finding `native-swift-stale-documents-bundle-masks-update`
plus M7's bundle version-check will eliminate it. **Recommendation:** remove the :1038 injection, the
:90/:839 state, and the :1041 dep in the SAME native build that ships the Documents-bundle version
check (M7), not before. Keep `__SIGNO_VINO_NATIVE_FILE_MODE` / `_NATIVE_BUNDLE_VERSION` — both live.

**Acceptance:** `grep -rn 'INITIAL_BOOK' PdfReaderApp.tsx web/src` → 0; typecheck green; boot smoke
(scripts/smoke-boot.mjs) green; device boot verified on the M7 day.

**Test idea:** extend e2e/native-entrypoint pins to assert the preload script injects exactly the two
live globals (freezes the contract, catches drift both directions).

---

## VESTIG-07 — the `__SIGNO_VINO_*` (sic) misspelling is a LOAD-BEARING cross-file contract — document it or rename it coordinated, never casually

**Class:** (b) CONFUSING-DEBT / naming · **Severity:** low · **Ship:** multi (if renamed); document-only is repo-only.

**Evidence:** "VINO" (wine) for "VIVO" on both sides of the bridge:
- Native injects: `PdfReaderApp.tsx:1036` (`__SIGNO_VINO_NATIVE_FILE_MODE`), :1037
  (`__SIGNO_VINO_NATIVE_BUNDLE_VERSION`), :1038 (INITIAL_BOOK, see VESTIG-06).
- Web reads: `web/src/app.js:227` (NATIVE_FILE_MODE — gates SW registration, relay follow mode, asset
  path resolution via `resolveAppPath`), :2903 (fleet check-in version), :3477-3480 (version badge).
- `web/build.mjs:30` comment repeats the misspelled name.

**Why it must never be a casual one-side rename:** a web-only deploy renaming the read side breaks
nothing catastrophically today (app.js:227 ORs `location.protocol === "file:"`, so FILE_MODE survives)
but silently degrades the version badge and fleet `nativeBuild` reporting for EVERY fielded native
shell (368–381 all inject the old name; a renamed web bundle mesh-pushed to an old shell reports blank
build numbers to the fleet dashboard, sabotaging the pre-Mass readiness check). A native-only rename
breaks the same things for the bundled copy.

**Recommendation (two acceptable end-states):**
1. **Document (cheapest, do now):** one comment at each of PdfReaderApp.tsx:1035 and app.js:226 —
   "`VINO` is a historical typo; it is the wire contract, do NOT correct the spelling unilaterally."
2. **Coordinated rename (fold into M3 bridge v1):** web reads `__SIGNO_VIVO_* ?? __SIGNO_VINO_*`
   (deploys instantly, tolerant of both); native injects BOTH names for ≥2 builds; drop the old name
   only when the fleet floor (dashboard MIN_SYNC_BUILD) passes the dual-inject build.

**Acceptance (option 1):** both sites carry the do-not-rename comment. (Option 2): grep shows the web
fallback-read landed BEFORE any native rename; e2e/native-entrypoint updated in the same commit.

---

## VESTIG-08 — `src/pdfReaderUrl.js` + `src/songNavigation.js`(+`.d.ts`) are pre-WebView-era modules with zero importers

**Class:** (c) INERT dead code · **Severity:** low · **Ship:** repo-only delete (recorded native-build; nothing ships).

**Evidence:** repo-wide grep (`grep -rn 'pdfReaderUrl\|songNavigation'` over all js/ts/tsx/mjs excluding
node_modules) → zero references outside the files themselves. Contents confirm provenance: pdfReaderUrl.js
is a PDF page URL clamp/normalizer (`clampPdfPage`, `normalizePdfUrl`, PDF_PAGE_MAX=10000) and
songNavigation.js a binary-search `findSongEntryOrNext` — both from the pre-build-332 native PDF-reader
app; the WebView rewrite (`de934699`) left them stranded. Not pinned by any e2e (repo-minimal-footprint
pins npm scripts + devDeps, not src files; grep of e2e/ → 0 hits). `src/songNavigation.d.ts` and (verify)
`src/alverniaManual2SongIndex.d.ts` fall with them if nothing imports the types — `tsc` is the arbiter.

**Wire-safety:** absolute — never bundled (Metro only bundles imports), never served.

**Fix:** `git rm src/pdfReaderUrl.js src/songNavigation.js src/songNavigation.d.ts`; run typecheck; if
`alverniaManual2SongIndex.d.ts` also has zero type-importers, remove it too (the `.js` STAYS —
build.mjs:563 reads it as text and e2e/offline-books-integrity.test.mjs:17 pins it).

**Acceptance:** typecheck + CI safe subset green; grep → 0.

---

## VESTIG-09 — offlineBooks.ts: 6 dead STORAGE_KEYS + a stale comment naming the deleted PDF path

**Class:** (c) INERT + (b) CONFUSING-DEBT (comment) · **Severity:** low · **Ship:** native-build (types/comments only).

**Evidence:** `src/offlineBooks.ts:11-24` declares 8 keys; only TWO have any consumer at HEAD
(all in PdfReaderApp.tsx): `lastSyncRole` (:432, :472, :506, :551, :869) and `lastPagePrefix` (:718,
write-only). Dead with zero readers AND zero writers repo-wide (grep): `onboardingComplete`,
`onboardingState`, `onboardingCity` (:12-14), `standardAccessName` (:15), `mode` (:16),
`activeBookId` (:17), `lastDirectorAt` (:21). Also the header comment `src/offlineBooks.ts:4-5` still
cites `assets/alvernia_manual_2.pdf` (renamed in #271 — same class as VESTIG-01).

**Wire-safety:** keys are device-local AsyncStorage names — deleting the *declarations* is absolutely
wire-safe. Data-at-rest note: devices upgraded from pre-374 builds keep orphaned values under
`sv.onboarding.*`, `sv.mode`, `sv.book.active`, `sv.standard.accessName`, `sv.sync.lastDirectorAt`
forever (a few hundred bytes; harmless). An optional one-time `AsyncStorage.multiRemove` cleanup could
ride any native build — do NOT remove `sv.sync.lastRole` or `sv.book.lastPage.*`.

**Fix:** delete the 6 dead entries; fix the :4-5 comment to `assets/signo_vivo_371.pdf`; optionally add
the multiRemove sweep in the boot effect (guarded, fire-and-forget).

**Acceptance:** typecheck green; `grep -rn 'onboarding\|standardAccessName\|lastDirectorAt' src PdfReaderApp.tsx` → 0.

---

## VESTIG-10 — the entire help subsystem (instructions + the ONLY haptic-pref control) is stranded behind a display:none stub

**Class:** (b) CONFUSING-DEBT / dead UI with a product fork · **Severity:** low · **Ship:** web-only (reaches native iPads at next native build / mesh push).

**Evidence chain (all current HEAD):**
- The only opener is a stub: `web/src/index.html:210` `<button id="help-button" … style="display:none" tabindex="-1">`.
  No CSS ever reveals it (grep styles.css 'help-button' → 0; `html[data-role]` rules don't touch it).
- The panel it opens is real, translated, and fully wired: `#help-panel` index.html:278-346
  ("¿Cómo funciona?" — swipe/drawer/numpad/search/back instructions) + the **only UI for the haptic
  preference** `#haptic-toggle` at :341 (pref `sv-haptic`, app.js:381-390 — permanently stuck at
  default "on" since the toggle is unreachable; moot on iOS Safari which lacks navigator.vibrate, live
  on Android web).
- Wiring alive: app.js:111-113 (lookups), :2662-2670 (open/close listeners that can never fire).
- Double-stale content: the help text instructs "toca la franja oscura a la izquierda"
  (index.html:300) — that strip is `#drawer-handle`, itself `display: none !important`
  (styles.css:2331, still JS-wired at app.js:88).
- index.html:203-206 documents the stub PATTERN ("kept hidden only so existing JS references stay
  valid; the web reader no longer surfaces them") but nothing acknowledges that hiding #help-button
  strands a whole live panel + a setting.

**User impact:** the app's bar is zero-training; the one in-app instruction surface is unreachable on
every surface (web, PWA, native — same bundle, no role reveals it). A congregant who can't find the
edge-swipe drawer has no discoverable path to learn it.

**Fix (product fork — flag to Miguel, do not assume):**
- (i) *Resurface:* give the drawer a visible "?" row (or a "¿Cómo funciona?" row at the bottom of the
  `todas` tab) that calls the existing opener logic; fix the "franja oscura" line to describe edge-swipe
  only; keep the haptic toggle. ~10 lines.
- (ii) *Complete the retirement:* delete #help-panel, #haptic-toggle, help CSS, app.js:111-113 +
  :2662-2670, and the `sv-haptic` read (or keep haptic hardwired). Aligns with the documented
  "minimal reader" intent.
Either resolves the debt; shipping neither leaves ~90 lines of unreachable UI that every future audit
re-discovers.

**Acceptance:** (i) help panel reachable by tap on web + native follower + director roles; (ii) grep
'help-panel\|haptic' web/src → 0 and footprint/CI green.

---

## VESTIG-11 — DELTA on `new-web-dead-books-data-inline-blob`: the books.json vestige is THREE sites, not one (emitted file + inline blob + per-device precache)

**Class:** (c) INERT dead code, delta scoping · **Severity:** low · **Ship:** web-only.

**Known finding covers:** build.mjs injecting the `#books-data` inline script (P8: "drop it, keep
#pages-data"). **Unlisted sites discovered:**
1. `web/build.mjs:686` — also WRITES `dist/books.json` as a standalone artifact.
2. `web/src/app.js:241` — `"/books.json"` sits in `SHELL_ASSETS`, so `ensureCoreAssetsCached`
   (app.js:553-564) fetches + caches it into STATIC_CACHE on **every device, every version** — a dead
   network fetch and cache slot for a file with zero readers (`grep -n 'books.json' web/src/app.js` →
   only :241; sw.js → 0 hits; sw.js's own CORE_ASSETS list at sw.js:22-31 does NOT include it).
3. `web/build.mjs:698` — the known inline `#books-data` blob (context :678-704, "single-book registry").

**Removal ordering is safe in any order** (worth stating so the implementer doesn't over-engineer):
`ensureCoreAssetsCached` uses `Promise.allSettled` per-asset (app.js:556-563, deliberately non-atomic),
so an old cached app.js requesting a no-longer-emitted /books.json just settles rejected; and the dead
`isOfflineBundleReady` completeness check that enumerates coreAssets (app.js:~656) has zero callers
(P8-known). Land all three in one commit + update scripts/smoke-boot.mjs if it asserts dist contents.

**Acceptance:** `grep -rn 'books.json\|books-data' web/src web/build.mjs` → only #pages-data remains;
smoke-boot green; a fresh build's dist/ has no books.json.

---

## VESTIG-12 — sync-worker/test-client.html is three generations stale and reproduces the exact frozen-follower bug fixed in prod

**Class:** (b) CONFUSING-DEBT (incident-debugging tool that lies) · **Severity:** low · **Ship:** repo-only (recorded worker-only; not deployed).

**Evidence:**
- `sync-worker/test-client.html:64-65` — `if(typeof s.seq === "number" && s.seq > 0 && s.seq <= lastSeq) return;`
  — a seq guard with NO freshness-first check: the P2-SEQ class fixed in the real client in `7b3eda4c`
  (#248). Against a stale-room takeover (regressed seq), the test client freezes on the old page while
  real followers move — during a live incident this tool would "confirm" a relay bug that isn't there.
- `:104` — publish is Bearer-token-only; the production auth path (X-Director-Code ∈ TRANSMITTER_CODES,
  index.ts:782-787) cannot be exercised at all.
- `:105` — `totalPages:370` (book is 371) — same stale count as `sync-worker/README.md:20`.

**Fix:** port the svSyncDecision freshness-before-seq rule (or literally import the lib), add an
X-Director-Code field next to the token field, bump 370→371. Alternatively DELETE the file and document
`sync-worker/test/a2.test.mjs` + `run-a2.sh` as the sanctioned harness (repo rule prefers deleting
dead-behavior test surfaces).

**Acceptance:** manual vs local wrangler dev: publish seq=high, then takeover seq=low after staleness →
test client follows (today it freezes); publish via a code (no token) succeeds.

---

# Classification ledger (lens-mandated, non-finding)

Wire-compat shims — **INERT-KEEP**, all documented in-code, do NOT remove yet:
- `set-book` no-op handler, app.js:942-943 — LOAD-BEARING: shells built ≤ #243 (368–374) still inject
  `set-book`, and a mesh-pushed NEW web bundle can run inside such a shell. Keep until fleet floor ≥375.
- Worker `/unlock` always-ok stub, index.ts:766-770 + `unlock` in ROUTE regex :398 — for pre-374 web
  copies still fielded inside old shells / stale Documents bundles. Cheap (returns before any DO work).
  Suggested retirement telemetry: count hits via the existing /log breadcrumb pattern; retire when 0
  for a month AND fleet MIN_SYNC_BUILD ≥ 371.
- `mode`/`bookId` wire fields everywhere (worker index.ts:40-41/:175-176, src/directorRelaySync.js:115-120
  with in-code compat comment, Swift :137-138/:677-678, app.js:3146 comment) — pinned by the
  additive-only invariant + nearby-sync-contract e2e. KEEP flowing.
- `#geo-gate` naming (index.html:43-47, app.js:53-66, styles.css:70-119) — deliberately retained as the
  boot loader with an explanatory comment; renaming = churn across 3 files for zero user value. INERT.

Verified GONE at HEAD (no action; listed so later lenses stop hunting): `X-Hymnal` (web + worker),
`PUBLIC_FLOOR_BOOK`, `precachePublicFloor`, `unlockStandard`, `bookFromSnap`/`bookFromSync` (code; one
Swift comment remains = VESTIG-04), `book-changed`, `SION_DIRECTOR_CODE` / any 5-digit Sión code,
`svGeoBook` / `svStandardUnlock` / `standardUnlocked` (code side; orphaned localStorage values persist
on pre-374-upgraded devices — harmless, optional cleanup could ride any web deploy).

Verified anchors for PLANNED work (P8 owns these; provided so the implementer doesn't re-derive):
- **Index-panel subsystem is present and 100% unreachable** (~460 lines): INDEX_TABS app.js:1470-1482,
  computeEaster :1485, getLiturgicalSeason :1496-1553, renderIndex*Content :1557-1866,
  renderIndexTabContent :1868, renderIndexPanel :1881, activateSearchFromIndex :1916. Reachability
  proof: its opener `#search-index` does NOT exist in index.html (app.js:108 getElementById → null;
  listener attach at :2644 uses `?.`); remaining invokers `#drawer-back` (:2493) and `#search-clear`
  (:2559-2562) are `is-hidden` until index state is entered — circular. The `buscar` tab renders only
  a type-to-search hint (renderTabBuscar :2301-2312), never the panel. The Easter computus is NOT
  independently dead — it dies with the panel.
- **isOfflineBundleReady zero callers confirmed** (def app.js:641; only other mention is comment :604).
- **`sv.book.lastPage.standard` still write-only at 381** (prior art asked for re-verify): write
  PdfReaderApp.tsx:715-718; no `getItem(lastPagePrefix…)` anywhere.

# Ideas parking lot (speculative — NOT findings)

- Compat-shim ledger: a `docs/wire-compat.md` table (shim → who needs it → retirement condition) for
  set-book / /unlock / INITIAL_BOOK / mode/bookId / VINO globals, so each future lens stops re-deriving
  wire-safety. Cheap, high leverage for this multi-agent workflow.
- Asset-path existence lint (see VESTIG-01 test idea) covering scripts/ + build.mjs + e2e pins.
- `prevCornerButton`/`displayClearButton` stub state updates run on every page render
  (app.js:783-784, :817-818, :823) — dead DOM work per page turn; fold into whichever P8 commit deletes
  or keeps the stubs.
- `src/alverniaManual2SongIndex.js` filename still brands the old manual — a rename touches
  build.mjs:563, e2e/offline-books-integrity.test.mjs:17, the .d.ts; churn likely exceeds value, but if
  M5 (book out of bundle) rewrites this pipeline anyway, rename then.
- `sync-worker/README.md:20` example `totalPages:370` + endpoint-table omissions (map-worker documented;
  likely another lens's README finding — noted here only because VESTIG-02 edits the same file).
- Numpad ≥5-digit director-code routing (app.js:1173-1194) is deliberate + documented post-Sión; the
  adjacent "4-digit garbage lands on the last page via next-song fallback" is an interaction-lens item,
  not vestigial.
- Native fleet self-ID `Alert.prompt` (PdfReaderApp.tsx:216-249) SURVIVES #270 by design (#270 removed
  only the web modal; commit message says native keeps its own check-in) — do not report as leftover.
