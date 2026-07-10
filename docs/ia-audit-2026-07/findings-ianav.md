# IA / Navigation / Discoverability audit — detailed findings (IANAV)

> Lens: information architecture, navigation & discoverability of the shared UI.
> Verified at HEAD `d5075091` (build 381) in worktree
> `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a`.
> File sizes at this HEAD: web/src/app.js = 3572 lines, index.html = 363, styles.css = 2345,
> PdfReaderApp.tsx ~ 1123. All file:line anchors below are from THIS worktree, not the stale docs.
> Dedupe: checked against map-prior-art.md (KNOWN-FINDINGS + PLANNED-WORK, incl. the build-381
> correction banner). Where a finding is a delta on a known-open item, the known id is named in
> the title and only the delta is claimed.

---

## IANAV-01 — Director-code entry outcome is SILENT on native: invalid code and failed promotion give zero feedback

**Severity: high · Surface: cross (native+web seam) · Ship: multi**

### Trigger / repro
1. On the parish iPad, open the ♪ modal, type a 10-digit director code with one wrong digit, tap "♪ Abrir Canto ♪".
2. The modal closes (web posts `director-code` and immediately closes itself) — and then **nothing happens at all**. No alert, no flash, no toast.
3. Variant B: type the CORRECT code, confirm "Sí, dirigir" in the native Alert, but `becomeDirector` fails (transient mesh/storage error). The device silently reverts to follower/none — the only cue is the *absence* of the small translucent DIRECTOR badge.

### Code walk
- `web/src/app.js:1178-1191` (`goToDraftSong`): a 5+-digit entry on native posts `{type:"director-code", code}` then `clearDraft(); closeSongJump();` — the numpad display that could show feedback is already gone by the time native answers.
- `PdfReaderApp.tsx:583-587` (`onDirectorCode`): unrecognized code → `injectEvent({ type: "role", role: "none" })` with the comment *"Unrecognized → tell the web so it surfaces 'código incorrecto'."* **That surfacing does not exist.** `grep -rn "incorrecto" web/src/` → zero hits; the only occurrence in the repo is that Swift-side comment itself.
- `web/src/app.js:947-956` (role handler in `applyNativeSyncEvent`): stores the role and calls `renderDirectorModeBadge()` — which for role `none` just keeps `data-role="follower"` and keeps the badge hidden (`app.js:845-852`). No message path whatsoever.
- `PdfReaderApp.tsx:516-526` (`becomeDirector` catch): a failed promotion after the user already confirmed the Alert either silently re-runs `becomeFollower()` or injects `role:"none"` — again no user-visible error.

### Why it matters at Mass
This is the exact outage family as 2026-07-01 ("no director all night"): the volunteer director believes the code "didn't take" (or worse, believes it DID take), gets no diagnostic, and the congregation follows nobody. Memory note `project_director_codes_are_phone_numbers_security_debt` still lists "device-test Braulio's director entry" as owed — a mistyped phone-number code is the most likely real-world input, and today it is indistinguishable from the app ignoring you.

### Fix approach
1. **Web (reaches phones instantly, iPads next build):** in the `role` handler, when the previous posted message was a `director-code` and the answering role event is `none`, surface an explicit dismissible banner or reuse the jump-modal flash: reopen/flash "Código incorrecto — revísalo e inténtalo de nuevo". Cleanest: don't close the modal on native code entry; keep it open in a "checking" state and let the role event either close it (director) or flash the error in the `#song-display` (the `flashSongDisplay` helper at app.js:831-841 already exists). Requires correlating request→response: add a `pendingDirectorCode` flag set at app.js:1184, cleared by the next `role` event.
2. **Native (robust, needs TestFlight):** in `onDirectorCode`'s unrecognized branch, ALSO `Alert.alert("Código incorrecto", "Ese código no es de director. Revísalo e inténtalo de nuevo.")`; in `becomeDirector`'s catch, `Alert.alert("No se pudo activar el modo director", "Inténtalo de nuevo.")`. Native alerts work even if the WebView is wedged.
3. Longer term this is exactly the kind of typed/acked message M3 (bridge v1) is for — a `director-code` should get an explicit ack/nack with a reason.

### Acceptance criteria
- Typing a wrong 5+-digit code on a native device produces a visible Spanish error within 1s.
- A failed promotion after confirm produces a visible error (not just a missing badge).
- A correct code still flows unchanged (confirm Alert → badge appears).

### Test idea
- e2e (web half): simulate `window.__signoVivoReceiveNativeEvent({type:"role", role:"none"})` right after posting a `director-code` and assert the error surface appears.
- Device day (M7): fat-finger a real code on the iPad; verify the alert.

---

## IANAV-02 — The entire browse/search IA is behind an invisible edge-swipe; followers have no visible entry at all

**Severity: high · Surface: web (shared bundle) · Ship: web-only (iPads at next native build)**

### Trigger / repro
Open signovivo.com (or the native app as a follower). Visible controls: page scan, ⟳ (top-left), ♪ (top-right), build number. The ♪ modal is a **numpad only** — number in, song out. There is no visible way to reach the song LIST: buscar / misa / recientes / tiempo / temas / tono / todas all live in the left drawer, and the drawer opens ONLY via a left-edge swipe nobody is told about.

### Code walk
- `web/src/styles.css:2331`: `.drawer-handle { display: none !important; }` — comment says *"Native parity: no left side-bar. The ♪ (upper-right) is the song navigator."* But ♪ only accepts numbers; it is not a browse entry.
- `web/src/index.html:78-80`: the handle still exists in DOM and is JS-wired (`app.js:2479-2482`), it's just hidden.
- Drawer openers at HEAD: edge swipe on viewer (`app.js:2723-2730`, start < 44px + 40px right), window-level edge swipe (`app.js:2789-2810`), hidden handle click, and the ⌕ fab — which is **director-only** (`styles.css:2154-2155`: `.search-fab { display:none }`, shown only under `html[data-role="director"]`). A follower's complete set of drawer entries = the secret swipe.
- The one thing that would teach the swipe — the help panel — is itself unreachable (IANAV-03).
- `app.js:2408-2409`: comment claims "Tapping the song title is the discoverable, deliberate entry" — but `#song-status` lives INSIDE the drawer (index.html:175), so it can't be the discovery path; the comment describes a retired layout.

### Gesture conflict matrix (verified)
| Gesture | Zone | Result | Conflict |
|---|---|---|---|
| swipe right, start x<44 | viewer | opens drawer | steals "previous page" swipe near the left edge (page-turn requires startX>=44, app.js:2733) |
| swipe right, start x<44 | anywhere (window handler) | opens drawer | fires even while the ♪ modal is open (no `songJumpOpen` check at app.js:2799-2810) → drawer opens hidden beneath the modal |
| left-edge swipe in a REGULAR iOS Safari tab | left edge | browser back-gesture territory | Safari's history-swipe can intercept/compete — for congregants on personal phones in a plain tab (not the installed PWA), the only drawer entry is unreliable |
| swipe left inside drawer | drawer | closes | OK (vertical-scroll guard at app.js:2755) |

### Why it matters
Elderly congregants can't discover an unmarked edge gesture; the director announces song numbers so the numpad flow survives, but anyone wanting a title, the recientes list, or search (e.g., at practice, or when the number was misheard) hits a dead end. The asymmetry is also unexplained: directors get a visible ⌕ that opens the same drawer (`app.js:2411`), followers get nothing.

### Fix approach
Pick one visible, low-clutter entry for followers:
1. **Un-hide the drawer handle** (delete styles.css:2331; it's already styled + wired), or
2. Make ♪ open a small two-option sheet ("🔢 Número" / "📚 Lista"), or
3. Show the ⌕ fab for everyone (cheapest: change the role-gating CSS; the drawer-as-dropdown search already works for any role — `activateTab("buscar")` has no role checks).
Also: gate the window-level edge handler on `!state.songJumpOpen`, and consider raising the page-turn dead zone only when the drawer affordance is visible.

### Acceptance criteria
- A brand-new follower can reach the song list and search within 2 taps of visible UI, no gestures.
- Edge swipe no longer opens the drawer beneath the open ♪ modal.

### Test idea
DOM test: with default role, assert some visible element (offsetParent != null) opens the drawer. Manual: iPhone Safari plain-tab check that the chosen entry works without gestures.

---

## IANAV-03 — Help panel is unreachable (its only opener is a hidden stub) and its content is stale

**Severity: medium · Surface: web (shared bundle) · Ship: web-only**

### Trigger / repro
There is no way, on web or native, to open "¿Cómo funciona?". The panel also contains the ONLY haptics setting and a "Versión" label — all dead.

### Code walk
- Opener: `web/src/index.html:210` — `#help-button` is `aria-hidden style="display:none" tabindex="-1"` inside the retired numpad panel; its click listener is live (`app.js:2662-2665`) but can never fire. `#help-close` (app.js:2667), `#haptic-toggle` (app.js:2684-2689), `#app-version-label` (populated at app.js:3482-3484) are all inside `#help-panel` (index.html:278-348) — unreachable.
- Content drift (if ever revived):
  - index.html:314: "Escribe el número con el teclado y toca **↵ Ir**" — the button is now "♪ Abrir Canto ♪" (index.html:142) and the drawer numpad is retired (styles.css:2327-2328).
  - index.html:300: "Desliza desde el borde izquierdo **o toca la franja oscura a la izquierda**" — the dark strip (drawer handle) is `display:none !important` (styles.css:2331).
  - index.html:285: "Toca las flechas grandes **de arriba**" — the arrows are inside the drawer, not "arriba" on the main screen.

### Why it matters
Six unlabeled glyph controls (⟳ ♪ ⌕ ⛶ ✕Salir, amber/green dot) and two hidden gestures with ZERO textual documentation anywhere in the product, for an elderly-first audience. The one surface built to explain them is dead code shipped to every device.

### Fix approach
Decide: revive or delete. Recommended: revive as a small "?" entry inside the drawer top bar (next to "← Cerrar menú"), rewrite the 3 stale items to match current UI (♪ modal, edge swipe OR the new visible entry from IANAV-02, fab meanings incl. ⟳/dot/go-live bar), keep the haptic toggle. If deleting instead: remove #help-panel, #help-button, haptic toggle dead pref, and the `sv-haptic` setting surface goes with it.

### Acceptance criteria
- Help is reachable from visible UI on both roles, and every claim in it matches build-381 UI.
- Or: the panel + opener + listeners are fully removed (no orphan listeners).

### Test idea
Smoke-boot style assertion: every element with a click listener bound in `bindReaderEvents` is not permanently `display:none` (catches this whole class: help-button, mode buttons, stubs).

---

## IANAV-04 — Nonexistent song numbers silently open the WRONG page (next song, or the last page) with no feedback

**Severity: medium · Surface: web (shared bundle) · Ship: web-only**

### Trigger / repro
The book has 315 songs numbered 2–371 with ~56 missing numbers (verified from `src/alverniaManual2SongIndex.js`: gaps at 83, 138, 149, 170, 185, 202, 217, 234, 246, 250-251, 257-261, 264-265, 268-269, 273-275, 278-281, 283-284, 286-289, 291-293, 297, 299, 305, 310-311, 315-316, 321-322, 325-326, 329, 331, 337, 346, 356, 360-361, 366).
- Type `258` (announced as 285, misheard) → app silently opens song 262's page.
- Type `400` or a 4-digit typo (`1234`) → app silently opens the LAST page (371).
- Type `1` → silently opens song 2.

### Code walk
- `web/src/app.js:700-709` (`resolveSongPage`): exact lookup, else `state.songIndex.find((entry) => entry.song >= normalized)` → next song's page, else `state.totalPages`. No caller distinguishes "found" from "guessed".
- `web/src/app.js:1193-1196` (`goToDraftSong`): renders whatever came back and records the TYPED number into recientes (`addToRecientes(songNumber)` at :1195) — so a nonexistent number **pollutes Recientes** too: `renderTabRecientes` (app.js:2199-2202) then can't find it in songIndex and silently skips it.
- Contrast: a 5+-digit entry on web DOES get feedback ("Código no válido", app.js:1189) — the numpad has an error language; it just isn't used for the far more common case.

### Why it matters
At Mass the number is announced verbally; a mishear or fat-finger lands the follower on a wrong song with zero cue that anything went wrong (and the go-live bar shows, implying the jump "worked"). Elderly users won't infer "that number doesn't exist".

### Fix approach
In `goToDraftSong`, check `state.songPageLookup.has(songNumber)` first. On miss: `flashSongDisplay("No existe el canto N", "err")` and KEEP the modal open (don't navigate, don't touch recientes). Keep the >=5-digit code path unchanged. Optionally offer nearest song behind an explicit second tap — but silent guessing must go. Skip `addToRecientes` for numbers not in the index either way.

### Acceptance criteria
- Typing a missing/out-of-range number never navigates and shows a clear Spanish message.
- Recientes never contains numbers that don't exist in the song index.

### Test idea
Unit-extract the draft-resolution decision (number → {exact|missing|code}) and test the gaps (258→missing, 262→exact, 400→missing, 12345→code); DOM test that the modal stays open on miss.

---

## IANAV-05 — DELTA on `web-reader-browse-result-click-skips-relay-browsing-mode` (+ #263 F1): every non-numpad navigation is now yanked back within ~4s, silently

**Severity: high · Surface: web · Ship: web-only**

Known-open core (not re-reported): browse/search taps don't set `relay.browsing`. This report is the **user-facing delta created by Wave-1 F1**, which prior-art oddity #1 explicitly left to this lens to verify. Verified at HEAD:

### Code walk (current)
- `web/src/app.js:1200-1205`: ONLY the ♪ numpad jump sets `relay.browsing = true` + `showGoLiveBar()`. `showGoLiveBar` has exactly one caller (:1203).
- Search-result tap: `app.js:2574-2588` → `renderPage(pageNum)` only. Drawer prev/next: `turnSong` `app.js:1966-1982` → `renderPage` only. Viewer swipe: `turnPage` `app.js:1984-1988`. Keyboard arrows: `app.js:2823-2824`. None touch `relay.browsing`.
- F1 heartbeat (`app.js:3286-3293`): every 4s, if `!relay.browsing && relay.hasDirector && state.currentPage !== relay.livePage` → force re-home poll → `renderPage(livePage)`.

### The delta (what changed vs. the originally-filed finding)
Pre-#263 the mismatch meant "your tap gets undone on the director's NEXT page turn" (minutes, maybe never). Post-#263 it is a **4-second boomerang loop**: a live follower who opens the drawer and taps a song in "Todas" reads it for ~4s, is snapped back with no message, taps again, is snapped back again — indefinitely. The green "en vivo" pill stays green throughout (they ARE following), the "Volver a en vivo" bar never appears (numpad-path only), and nothing on screen explains why the app is "fighting" them. Two identical intents → opposite outcomes depending on entry point.

### Fix approach
Route ALL intentional local navigation through one helper that mirrors goToDraftSong's tail: if `relay.hasDirector && targetPage !== relay.livePage` → `relay.browsing = true; relay.following = false; showGoLiveBar(); renderRelayPill();`. Call it from the search-result tap (:2581), `turnSong` (:1971/:1979), `turnPage` (:1987), and the arrow-key path. F1 then correctly leaves them alone, and the amber pill + bar give the way back — the F4/demote path already resets `browsing` when the director goes stale, so no strand risk. ~15-line web-only change; should land BEFORE M4's status pill, which will otherwise render a confusing "en vivo" state during the boomerang.

### Acceptance criteria
- With a live director: tapping any song in the drawer/search keeps you there, shows the amber pill + "Volver a en vivo" bar; heartbeats do NOT re-home you.
- Numpad, search, drawer arrows, swipe, and keyboard arrows all behave identically.

### Test idea
DOM test that a `.search-result-item` click sets `relay.browsing` when `relay.hasDirector && livePage !== targetPage`; extend the svSyncDecision-adjacent unit coverage for the browsing flag.

---

## IANAV-06 — Web "Código no válido" is a misleading dead end for a real director code (and for 5-digit song typos)

**Severity: medium · Surface: web · Ship: web-only**

### Trigger / repro
1. The real director (or Miguel testing) opens signovivo.com on a phone and types their actual director code → "Código no válido". The code IS valid — the platform is wrong. Nothing tells them director mode only exists in the iPad app; natural next steps (retype, doubt the code, call Miguel) all waste pre-Mass minutes.
2. A follower fat-fingers a 5th digit onto a song number → same "Código no válido", confusing because they never entered a "código".

### Code walk
- `web/src/app.js:1178-1191`: any >=5-digit entry on pure web → `flashSongDisplay("Código no válido", "err")`. One string for two distinct failure modes (wrong platform for a REAL code; song-number typo).
- 1.6s auto-revert (`app.js:837-840`) is short for the demographic.

### Fix approach
Split the message: since songs cap at 3 digits, treat 4-digit entries as song typos → route to IANAV-04's "No existe el canto NNNN". For >=5 digits on web: "El modo director solo funciona en el iPad del coro" (accurate, actionable; the numpad-code affordance is already parish-public). Consider lengthening the flash to ~3s.

### Acceptance criteria
- A >=5-digit entry on web names the real reason (wrong platform), not "invalid code".
- 4-digit typos get the song-not-found treatment, not code language.

### Test idea
DOM test of the two draft lengths on a `hasNativeBridge()===false` build; string assertions.

---

## IANAV-07 — First-run web open: nothing says what the app is, that it auto-follows, or that no director is live

**Severity: medium · Surface: web · Ship: web-only · Intersects M4**

### Trigger / repro
Cold open of signovivo.com with no director broadcasting: loader ("Cargando Signo Vivo…") → page 2 of the scanned book + three unlabeled glyphs. No app name on screen, no "sigues al coro en vivo", no indicator that nothing is live right now (the pill is `display:none` when `!relay.hasDirector`, app.js:3038). When a director IS live, the page changes by itself — magic with no explanation; when they aren't, ⟳ spins 1.1s (app.js:2416-2419) and appears to do nothing.

### Code walk
- `web/src/app.js:215` `DEFAULT_START_PAGE = 2`; `:3449` renders it when no director after the 1500ms boot peek (`:3446-3448`).
- `renderRelayPill` `:3036-3042`: pill hidden entirely unless `hasDirector`.
- The only onboarding copy ever written (numpad tip, index.html:215) is stranded in the retired Teclado panel (IANAV-09) and referenced a retired button anyway.
- The OG meta description (index.html:19) says exactly the right thing — "sigue al director en vivo desde tu teléfono" — but only link previews ever show it.

### Fix approach
M4 already plans the always-visible tri-state status pill — that covers the persistent state signal; don't rebuild it. The residual gap claimed here is the **one-time first-run hint**: a small dismissible line (localStorage flag, e.g. `sv-hello`) on first open — "Signo Vivo · Cuando el coro cante, la página cambia sola. ♪ para ir a un canto." Ship with (or fold into) M4's pill work.

### Acceptance criteria
- A first-time visitor with no director sees, without interacting: the app name + one sentence stating it follows the choir automatically; dismissed state persists.

### Test idea
DOM boot test with relay stubbed to no-director: hint visible; set flag → hidden on next boot.

---

## IANAV-08 — "Recientes" misses most actually-visited songs (and records nonexistent numbers)

**Severity: low · Surface: web · Ship: web-only**

### Code walk
`addToRecientes` callers: numpad jump (`app.js:1195`) and search-result tap — the latter only when the tapped page is EXACTLY a song's start page (`app.js:2583-2584`, `state.songIndex.find((s) => s.page === pageNum)`; search hits on interior lyric pages record nothing). Never called from: drawer prev/next (`turnSong`, :1966-1982), swipes (`turnPage`), arrow keys, or relay/mesh-applied pages. Empty-state copy promises "las canciones que hayas visitado recientemente" (app.js:2195). Plus the IANAV-04 note: typed-but-nonexistent numbers ARE recorded then silently skipped at render (:2199-2202).

### Why it matters
The tab's most valuable use — "what did we sing at Mass?" — records nothing, because followed pages never register. What it actually is is "numbers you typed".

### Fix approach
Record on song-boundary dwell instead of on input method: in `renderPage`'s commit path, resolve `getSongForPage(page)` and add after a short dwell (15-30s on the same song, timer reset on page change) — captures followed songs and browsing alike without page-flip noise. Drop the `addToRecientes` call for unmatched numbers (IANAV-04).

### Acceptance criteria
- Following a director across 3 songs populates Recientes with those 3 songs (after dwell).
- Nonexistent typed numbers never appear in storage.

### Test idea
Unit-test a small `noteSongVisited(page, nowMs)` reducer (dwell logic) extracted for the purpose.

---

## IANAV-09 — Confirmed-dead UI shipped in every bundle: index panel (incl. Easter computus), retired Teclado panel + stale tip, hidden stubs

**Severity: low · Surface: web · Ship: web-only**

Answers the tasked question "is any of the index panel still user-reachable?" — **No.** Verified reachability at HEAD:

- `#search-index` does not exist in index.html (`searchIndexButton = getElementById("search-index")` → null, app.js:108; all uses optional-chained).
- `renderIndexPanel` (app.js:1881) callers: `drawerBack` click (:2491-2494) — but `#drawer-back` is unhidden ONLY by `activateSearchFromIndex` (:1919), which is reachable only from chips INSIDE the rendered index panel (:2627-2640); `searchClearButton` when `state.indexDrillDown` (:2559-2562) — same circular gate (:1918); and the null `searchIndexButton` (:2643-2651).
  → the whole subsystem is a strongly-connected dead component: `getLiturgicalSeason` + `computeEaster` (:1480-1553), the index content renderers (:1557-1880), the index-tab/sort-tab/chip click branches (:2591-2640).
- Retired Teclado half: `.drawer-mode-switch { display:none }` (styles.css:2328) + drawer always opens in browse (`openDrawer` → `switchDrawerMode("browse")`, app.js:1116) → `#numpad-panel` (index.html:202-219) can never display. Its tip banner still says "toca **↵ Ir**" (a button that no longer exists) and the `sv-tip` dismissed-flag plumbing (app.js:393-399, 2673-2677) guards an element that can't show. Mode buttons wired (:2460-2467) but invisible.
- `#search-cancel` stub (index.html:275) + listener (:2568-2572); the four hidden stub buttons index.html:208-211 (incl. `#help-button`, see IANAV-03).
- Persistence inversion: `nc-sort-prefs` (PREFS_KEY, app.js:133-139) persists sort prefs used ONLY by the dead index panel, while the LIVE search sort (`state.searchSortMode`, ⇅ toggle :2545-2554) is not persisted at all (IANAV-12).

Prior-art note: P8 lists other dead surfaces (#books-data, offline-gate UI, dead state fields) but NOT this component — additive inventory, aligned with the P8 cleanup batch.

### Fix approach
Delete the component (~500 lines app.js + HTML/CSS blocks + `nc-sort-prefs` persistence); fold into P8. Keep `SOLFEGE_MAP`/rail renderers (live). After deletion, re-check the known theme-search shadowing item (P7) since `searchByTheme` survives.

### Acceptance criteria
- No unreachable UI branches remain in app.js for index/Teclado; smoke boot green; `nc-sort-prefs` no longer written.

---

## IANAV-10 — Spanish copy: accents missing in installable-surface strings, canto/canción inconsistency, jargon in the director banner

**Severity: low · Surface: web · Ship: web-only · Intersects major-update §9 decision #7 (Spanish copy)**

Systematic string sweep (user-visible only):

1. **Missing accents** (OS install/link surfaces): `web/src/manifest.webmanifest:4` "navegacion rapida por numero de cancion" → "navegación rápida por número de canción"; `web/src/index.html:54` `alt="Pagina actual del manual"` → "Página" (screen readers).
2. **Same concept, two words**: modal says **canto** ("IR A CANTO" index.html:114, "♪ Abrir Canto ♪" :142, aria "Ir a canto" :91); drawer/help say **canción** ("Canción anterior/siguiente" :165-181, "Ir a una canción" :313, `Canción ${n}` app.js:793). Pick one term everywhere (native alerts say "¿Dirigir el coro?" — coro/canto register fits the parish).
3. **Jargon**: director relay-auth banner (app.js:912): "El relé rechazó el código de director…" — "relé" is electrician's Spanish. Suggest: "No se pudo conectar con signovivo.com: el código de director fue rechazado. Los teléfonos NO están siguiendo."
4. **Vague tab label**: rail tab "Tiempo" 📅 (index.html:231) for liturgical seasons — reads as time/weather; consider "Época" or fuller aria-label.
5. **Terse empty state**: "Sin resultados." (app.js:1291) — add guidance: "Sin resultados. Prueba con menos palabras o busca el número con ♪."

### Fix approach
Single copy-pass PR; items 1-2 mechanical. Coordinate with Miguel's pending §9 #7 decision so wording lands once.

### Acceptance criteria
- One canonical term for song in user-facing strings; accents fixed; banner copy actionable.

---

## IANAV-11 — The live/amber dot is an 8×8px button layered on the ♪ fab's corner

**Severity: low · Surface: web · Ship: web-only · Intersects M4 (status pill redesign)**

### Code walk
- `app.js:3016-3019`: `#sv-live-pill` fixed at `top:0.6rem; right:0.7rem; width:8px; height:8px; z-index:46` — geometrically INSIDE the ♪ fab's 64px square (`.song-jump-fab` at `top/right 0.55rem`, 4rem, z-index 45, styles.css:2094-2122). Reads as a notification dot ON the ♪ button.
- In amber state it is a functional "go live" button (`app.js:3028-3031`) — an 8px tap target for this demographic; a miss opens the numpad modal instead (the fab underneath). While the drawer is open the fabs hide (`body.sv-drawer-open`, styles.css:2159-2160) but the dot remains — a floating context-free dot.
- The green bar is the primary affordance, but it only appears on the numpad browse path (IANAV-05), so the amber dot is sometimes the ONLY visible way back.

### Fix approach
Don't grow the dot in isolation — M4's always-visible tri-state pill replaces this surface. Interim web-only patch if M4 slips: 44px invisible hit area, move it clear of the fab, hide it under `body.sv-drawer-open`.

### Acceptance criteria
- Amber-state tap target >= 44px; dot not visually attached to the ♪ fab; hidden with the fabs when the drawer covers them.

---

## IANAV-12 — Search sort toggle: state-vs-action ambiguity, and the live preference is the one that ISN'T persisted

**Severity: low · Surface: web · Ship: web-only**

### Code walk
- `app.js:2545-2554`: `⇅ Mejor → ⇅ Nº → ⇅ A–Z` cycle; the label shows the CURRENT mode, so users can't tell if tapping applies the label or leaves it. Blind 3-state cycling is hard for the demographic; "Mejor" (relevance) is abstract.
- `state.searchSortMode` is session-only — never saved; meanwhile `saveSortPrefs` (app.js:138-140) persists `indexSortPrefs`, which only the dead index panel reads (IANAV-09). A user who always wants "Nº" re-taps twice every session.
- Verified NON-issue (do not report): theme-result sets DO honor the toggle (renderThemeResults app.js:1377-1383).

### Fix approach
Persist `searchSortMode`; render as three tiny segmented buttons (`Mejor | Nº | A–Z`, active highlighted) instead of a blind cycle — the dead index panel's `renderSortTabs` pattern can donate the markup.

### Acceptance criteria
- Chosen sort survives reload; current mode visually distinct from the alternatives.

---

## Parking lot (ideas / non-findings — separate decision needed)

- **Boot flash page 2 → snap to director** when the 1500ms relay peek loses the race (app.js:3446-3449 vs late `applyRelaySnapshot`); could hold the loader until the race settles (map-web O15).
- **Landscape on `orientation: any`** (manifest:11): landscape CSS caps page height (styles.css:2041-2057); phone-landscape renders small but correct; no defect.
- **Keyboard arrows move by SONG while swipes move by PAGE** (app.js:2823-2824 vs 1984-1988) — desktop-only inconsistency (map-web O9); prior art tracks the editable-target half separately.
- **A11y semantics**: rail tabs `role="tab"` but `aria-selected` never updates (activateTab :2340-2342 toggles class only); `#search-results role="list"` contains buttons without listitem roles; no `aria-expanded` on drawer openers.
- **Build badge overlaps the ⛶ fab zone** bottom-right on fullscreen-capable platforms (styles.css:2070-2085 vs 2127-2131) — pointer-events:none, purely visual.
- **Stale comment** app.js:2408 (song-title-as-discoverable-entry describes a retired layout) — fix alongside IANAV-02.
- **searchPages 30-result cap** (app.js:1283) has no "showing best 30" indicator.
- **"Canción 0"** possible in drawer status for pre-song pages (app.js:793 + getCurrentSongNumber=0) — cosmetic.
- **Director numpad shows the code in cleartext while typing** (codes are phone numbers) — mask after 5+ digits; security debt tracked elsewhere.
