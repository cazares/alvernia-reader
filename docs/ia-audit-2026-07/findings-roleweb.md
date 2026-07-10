# ROLEWEB — Role model & director IA on the pure-web surface

Audit lens: role information architecture on signovivo.com (no native bridge).
HEAD at audit time: **d5075091** (build 381, two commits past the cartographer maps' 16244b25 / build 377 — all line anchors below re-verified against d5075091).

## Ground-truth established for this lens

- `html[data-role]` is written in exactly ONE place: `renderDirectorModeBadge` (web/src/app.js:845-852), whose only call site is the native bridge `role` event handler (app.js:952). On pure web the attribute is **never set**; the follower layout works only because follower is the CSS base (styles.css:2150-2156 gate the director variants on `html[data-role="director"]`).
- The pre-374 `goToDraftSong -> unlockStandard -> POST /unlock` chain is **gone from the web bundle**. `grep unlock web/src/` finds only comments (app.js:828, :1181; styles.css:2224). Today a 5+-digit numpad entry on pure web hits app.js:1178-1190: no bridge -> `flashSongDisplay("Código no válido", "err")`, numpad stays open. Nothing is POSTed anywhere; the worker's `/unlock` (sync-worker/src/index.ts:766-771) is a live always-`{ok:true}` stub with **zero shipped callers** — kept deliberately for pre-374 native shells (documented compat, not re-reported).
- The worker's `/publish` accepts `X-Director-Code` from ANY origin (index.ts:777-788; `ALLOWED_ORIGINS: "*"` wrangler.jsonc:20) — so the *capability* for a browser to direct exists server-side today; only the web UI is absent (zero publish code anywhere in web/src, verified by grep).
- Director-flavored artifacts shipped in the web bundle: the `#director-mode-badge` button (index.html:55-57, boots `is-hidden`), its tap-to-exit `window.confirm` handler (app.js:2426-2431), the ⌕ `search-fab` (index.html:92, display gated to director), and the 10-digit numpad draft cap kept "for director / secret codes" (app.js:1141-1147). On pure web the badge/exit/⌕ can never activate (no role event) — dead but *inert*; the dangerous artifact is the code path's error copy (ROLEWEB-01).

---

## ROLEWEB-01 — A VALID director code on signovivo.com flashes "Código no válido" — misleading dead end in the exact emergency where a director reaches for the web

**Severity: high · Surface: web · Ship: web-only**

### Trigger / repro
1. Open signovivo.com in any browser (no native bridge).
2. Tap ♪ -> type a real 10-digit director code -> Ir.
3. The numpad display flashes **"Código no válido"** in red for 1.6s (app.js:831-841), then reverts. Retyping repeats it forever.

### Code walk
- app.js:1178-1190 (`goToDraftSong`): any 5+-digit entry branches on `NATIVE_FILE_MODE || hasNativeBridge()`. Web falls to the else: `clearDraft(); flashSongDisplay("Código no válido", "err");`.
- The copy is a *credential verdict* ("your code is invalid") for what is actually a *surface limitation* ("this device cannot direct"). The code was never checked against anything — the same message fires for a garbage code and for the parish's real code.
- The comment at app.js:827-829 / :1181-1182 shows the intent ("nothing to unlock on web") but the user-facing string doesn't say that.

### User impact at Mass
The one realistic reason anyone types a director code into signovivo.com is the disaster drill: the parish iPad is dead/lost/left at home, the volunteer director opens the website on their phone and tries their code. The app tells them their code is wrong. Predictable spiral for a zero-training volunteer: retype -> "invalid" -> assume they misremembered the code -> call Miguel / give up, minutes before Mass. No hint exists anywhere on web that directing requires the iPad app.

### Fix approach
In the web else-branch, replace the flash with honest, actionable Spanish copy. The 1.6s flash is too small for this message; reuse the flash for short feedback but for a 10-digit-looking entry show a small dismissible sheet/toast, e.g.:
> **"Este código se usa en el iPad de la parroquia."**
> "El sitio web solo puede seguir al director, no dirigir. Abre la app SignoVivo en el iPad para dirigir."

Keep "Código no válido" only for entries that are 5-9 digits (not code-shaped). Purely client-side; no worker change. Note: the native bundle also carries this file, but native devices take the bridge branch, so the change is genuinely web-reaching via a Pages deploy.

### Acceptance criteria
- Typing a 10-digit number on pure web produces copy that (a) does not claim the code is invalid, (b) names the iPad app as the directing surface.
- 5-9-digit entries still get a short "no válido"-class flash; <=4 digits still resolve as songs.
- Native path (bridge present) unchanged — still posts `{type:"director-code"}`.

### Test idea
Unit-extract the branch (draft length -> action enum) into a pure helper à la svSyncDecision and pin: `{web, len10} -> "explain-ipad"`, `{web, len6} -> "invalid-flash"`, `{native, len10} -> "post-bridge"`, `{*, len4} -> "song"`.

---

## ROLEWEB-02 — No web-director path at all: the native iPad is a single point of failure for the entire relay congregation, even though the worker already authorizes code-bearing browsers

**Severity: high · Surface: cross · Ship: multi**

### The hole
There is no legitimate way to lead from the web. If the director's iPad fails at Mass (battery, crash, forgotten, stolen), every relay follower (personal phones, the old web-PWA iPad) freezes on the last published page for the rest of Mass. No fallback surface exists. This is an *absence*, but it is the operational hole that turns any single-device failure into a congregation-wide outage.

### Evidence that it's cheap-ish, not moonshot
- sync-worker/src/index.ts:777-788: `/publish` authorizes via `X-Director-Code` header — the SAME memorable codes the director already knows — from any client; CORS is `*` (wrangler.jsonc:20). A browser `fetch` can publish **today** with zero worker changes.
- web/src/app.js contains zero publish code (grep `publish` -> comments only) and the follower relay machinery (room, seq, snapshot shape) is already all present client-side (app.js:2836-2869, 3168-3169).
- ROLEWEB-01's code entry point is already numpad-shaped for 10-digit codes (app.js:1141-1147) — a web-director mode has a natural, already-taught entry gesture.

### Why it's gated as `multi`, not web-only
Shipping a second publisher surface before transmitter identity exists would sharpen the known-open two-publisher ping-pong (`relay-no-transmitter-identity-two-publishers-ping-pong`, still open per prior-art map; `{ignored:true}` never consumed). A safe web-director needs: transmitterId + tiebreak (planned, M4), publish-rejection surfacing on web (the web analog of `relay-auth-error`), and a deliberate "EN DIRECTO desde la web" state so a follower phone can't become a director by accident.

### Fix approach (design sketch)
1. After M4's transmitterId lands: on pure web, a valid 10-digit code (the ROLEWEB-01 sheet gains a second line: "¿Dirigir desde aquí en emergencia?") enters a **web-director mode**: sets `html[data-role="director"]` via the existing `renderDirectorModeBadge` state (giving badge + ⌕ for free — the CSS already exists, styles.css:2154-2156), stops the relay follow loop, and publishes page-changes with `X-Director-Code` + transmitterId, seq = wall-clock-ms (matches directorRelaySync.js semantics).
2. Surface 401 (`unauthorized`) inline — reuse `showRelayAuthWarning` (app.js:887) which is already in the bundle and today native-only.
3. Explicitly out of scope: mesh (web can't Multipeer) — native followers in mesh-only mode won't follow a web director; document that limitation in the mode's copy ("los iPads del coro no seguirán; solo teléfonos y web").

### Acceptance criteria
- A director with a valid code and no iPad can drive all relay followers from a phone browser within ~30s of opening signovivo.com, with a visible director state and a visible failure state on 401.
- An invalid code cannot enter the mode (server 401 -> mode exits with honest copy).
- Two publishers (native + web) resolve deterministically via the M4 tiebreak, verified on the staging room.

### majorUpdateIntersection
Depends on and extends **M4** (transmitterId + two-publisher tiebreak; status pill). Natural companion to **M6**'s admin surface. Not itself listed anywhere in docs/major-update-2026-07.md — net-new milestone candidate and a Miguel product decision (does he WANT web-directing? Surface the fork).

---

## ROLEWEB-03 — Green "En vivo" pill never demotes when the FOLLOWER's own network dies — false live signal over a frozen page (delta beyond M4's stated demotion case)

**Severity: high · Surface: web · Ship: web-only**

### Trigger / repro
1. Web follower is live (green pulsing dot, following the director).
2. The phone's wifi/cell drops (or church wifi loses upstream — `navigator.onLine` can stay true).
3. Page freezes on the last applied snapshot. The pill keeps pulsing **green ("En vivo con el director")** indefinitely — hours, if the network never returns.

### Code walk — every demotion path requires a RECEIVED snapshot
- `renderRelayPill` (app.js:3036-3042) renders purely from `relay.hasDirector` / `relay.following`.
- `relay.hasDirector = false` is set ONLY inside `applyRelaySnapshot` (inline fallback app.js:3123; lib path via svSyncDecision "demote") — i.e. only when a snapshot **arrives** and is judged stale.
- On network death no snapshot arrives: `relayPollOnce` swallows every failure in bare `catch {}` (app.js:3174-3210, the catch at :3209) — no counter, no state change. The WS `close` handler (app.js:3300-3325) reconnects with backoff but never touches `hasDirector` or the pill. F2/F3 watchdogs (app.js:3284, 3363-3375) tear down zombie sockets and start polls — all *recovery*, no *status honesty*. `setSyncWorking` (app.js:863-878) is native-mesh-only.
- Net: the connection-health machinery already KNOWS the link is dead (F3's `dead` predicate, app.js:3367) but no surface reflects it.

### User impact
The one status cue the follower has actively lies during the most common real-world failure (connectivity blips in a masonry church). An elderly congregant sees green + a page that stopped matching the singing, and has no cue that tapping ⟳ would help.

### Relationship to planned work (why this is a reportable delta)
M4 (docs/major-update-2026-07.md:147) plans the "always-visible tri-state status pill" and verifies "demotion now fires on a healthy socket (kills the 'green pill on a dead director' freeze)" — that is the **dead-director-on-healthy-link** case. This finding is the mirror: **healthy-director-on-dead-link**, driven client-side with no snapshot at all. The M4 acceptance list does not mention it; if the tri-state pill is implemented as a pure function of received snapshots it will ship with this hole intact.

### Fix approach
Add a client-link facet to pill state: reuse F3's existing `dead` computation (app.js:3367) plus N consecutive `relayPollOnce` failures (add a counter in the catch) to flip the pill to a "reconectando" visual (gray/hollow, `aria-label="Reconectando…"`); restore on the next successfully applied snapshot. Do NOT clear `relay.hasDirector`/`livePage` (rejoin should still know the last live page); presentation-state only.

### Acceptance criteria
- Airplane-mode a following device: pill leaves green within <=15s; page untouched.
- Network back: pill returns to green (or hidden if the director has meanwhile gone stale) without a reload.
- Dead-director-on-healthy-socket behavior (M4's case) unchanged.

### Test idea
Extend svSyncDecision (or a sibling pure lib) with `pillState({hasDirector, following, linkDead})` and unit-pin the combos; drive `linkDead` from the F3 predicate + failure counter in a jsdom test with stubbed fetch failures.

---

## ROLEWEB-04 — The follower's entire "am I live?" surface is an unlabeled 8×8-pixel dot that is also its own tap target (delta/spec input to M4's planned tri-state pill)

**Severity: medium · Surface: web · Ship: web-only**

### Code walk
- The pill is literally 8px × 8px: `width:8px;height:8px;border-radius:50%` (app.js:3016-3023), fixed top-right.
- Its meaning is color-only: green pulsing = following, amber = browsing, `display:none` = no director (app.js:3038-3040). Text exists only in `aria-label` (:3041) — invisible to a sighted elderly user.
- The amber dot is itself the "go live" **tap target** (click handler app.js:3028-3031) — 8px against Apple's 44pt minimum; the go-live bar (:3060-3066) only appears for numpad-initiated browsing (see ROLEWEB-06), so for every other off-live situation the 8px dot is the only interactive status object.
- "No director" and "app has no live feature at all" are indistinguishable: pill hidden (:3038), page sits on DEFAULT_START_PAGE=2 (app.js:215, :3449). First-time visitors arriving before Mass (drawn by the og:description promise "sigue al director en vivo desde tu teléfono", index.html:19) get zero indication that live-follow exists or will start.

### User impact
The parish's stated audience is elderly congregants on phones and iPads. A 1.5mm colored dot with no words is not a status system for them. The three states users need distinguished ("estás en vivo" / "estás navegando, vuelve aquí" / "aún no hay director") collapse into pulse-color trivia and an absence.

### Relationship to planned work
M4 explicitly plans an "always-visible tri-state status pill" — this finding does NOT re-report that plan; it reports the user-facing spec angles the plan line doesn't list: (1) visible TEXT ("EN VIVO" / "Reconectando" / "Esperando al director"), not color-only; (2) >=44px tap target; (3) an explicit waiting state before the first publish; (4) portrait-iPad + phone placement that doesn't collide with the ⟳/♪ fabs (styles.css:2119-2126).

### Acceptance criteria
- Every relay state renders a labeled surface legible at arm's length (>=13pt text), including a pre-director waiting state.
- All interactive status affordances >=44×44 px.

### Test idea
Snapshot/DOM assertions per state in jsdom (label text + computed size), plus a manual portrait-iPad pass on the parish device.

---

## ROLEWEB-05 — Search and title-browse are role-locked away from web followers on a fully public book: ⌕ is director-only CSS, the drawer handle is display:none, and the only opener left is an undocumented touch edge-swipe

**Severity: medium · Surface: web · Ship: web-only**

### Code walk — every path into search/browse on pure web
1. ⌕ `search-fab` -> `display:flex` only under `html[data-role="director"]` (styles.css:2154-2155); pure web never sets the attribute (app.js:845-852, sole caller :952) -> **never visible**.
2. `#drawer-handle` -> `display:none !important` (styles.css:2331, "Native parity: no left side-bar") yet still JS-wired (app.js:2479-2482) -> **dead**.
3. Left-edge touch swipe -> viewerShell handler (app.js:2723-2729) + window-level edge swipe (:2806-2809) -> works, but is undiscoverable (no visual hint anywhere) and **touch-only**: a desktop/laptop visitor has NO path into the drawer at all (keyboard handler has no drawer-open binding, app.js:2812-2825).
4. The ♪ modal (the one discoverable navigator, index.html:90, app.js:2407-2409) is numeric-only — it requires already knowing the song number.

The drawer behind those locked doors contains the full Buscar tab (full-text + title search, index.html:227-260) with zero role checks in its JS — the gating is accidental-by-CSS, not a security posture: post-374 the book is fully public.

### User impact
A congregant or visitor who knows a song's NAME ("Pescador de Hombres") but not its number cannot find it: no visible search, no visible index, and on desktop literally no route. The capability exists in the shipped bundle on every device; IA hides it behind a role that pure web can never attain.

### Fix approach
Decide the follower nav story deliberately rather than by CSS residue. Minimal version: add a "Buscar / Índice" entry inside the ♪ modal (already the taught gesture) that opens the existing drawer's Buscar tab (`navigationDrawer.classList.add("as-dropdown"); openDrawer(); activateTab("buscar")` — exactly the director ⌕ handler at app.js:2411, minus the role gate). Must land together with ROLEWEB-06's browsing fix, or search becomes a live-yank trap for followers.

### Acceptance criteria
- On pure web (touch AND desktop) a user can reach title/full-text search in <=2 discoverable taps from the reader.
- Director layout unchanged; native unaffected until its next bundled build.

### Test idea
jsdom: with no `data-role`, assert the ♪ modal exposes the search entry and invoking it activates the `buscar` tab; keep a DOM assertion that `search-fab` stays hidden without `data-role="director"`.

---

## ROLEWEB-06 — DELTA on known `web-reader-browse-result-click-skips-relay-browsing-mode`: Wave-1's F1 re-home turned it from "yanked at the director's next move" into "yanked within <=4 seconds, unexplained" — drawer/search browsing while live is now effectively impossible

**Severity: medium · Surface: web · Ship: web-only**

### Known base (not re-reported)
docs/audit-findings-index.md:24 — drawer/search-result taps never set `relay.browsing`, unlike the numpad jump. Still true at HEAD: the search-result click handler calls `renderPage(pageNum)` with no browsing/go-live-bar logic (app.js:2574-2589); only `goToDraftSong` sets `relay.browsing`/`showGoLiveBar()` (app.js:1200-1205). Swipes/arrows likewise (F1 comment: "ONLY the numpad jump sets relay.browsing", app.js:3287).

### The delta (new since the finding was filed)
Wave-1 F1 (#263) added a heartbeat re-home: every 4s ping tick, if `!relay.browsing && state.currentPage !== relay.livePage`, force-poll and snap home (app.js:3286-3293). Consequence: a live follower who edge-swipes the drawer open and taps a search result now gets **~<=4s** on the chosen page before being silently snapped back — previously they kept it until the director's next actual page turn. There is no cue: no amber pill transition, no "Volver a en vivo" bar (browsing was never set), the page just teleports. Combined with ROLEWEB-05, the only rich-browse surface on web is now a booby trap during exactly the period it would be used (Mass). Prior-art map explicitly flagged the F1↔browsing interaction as fresh unaudited surface — this is that audit.

### Fix approach
Unify intent detection: extract the numpad jump's tail (app.js:1197-1205) into `enterBrowseIfOffLive(targetPage)` and call it from the search-result click handler (app.js:2577-2587); for drawer prev/next buttons (app.js:2437-2445 via `turnSong`) either include behind a drawer-originated flag or accept F1 for raw swipes and fix only explicit song-picks (picking a named song = deliberate browse).

### Acceptance criteria
- Live follower taps a search result off the live page -> amber pill + "Volver a en vivo" bar appear; page HOLDS (no F1 yank at the next heartbeat); tapping the bar returns to live.
- Follower who taps the result equal to the live page stays green/following (mirror of app.js:1198-1200 semantics).

### Test idea
Unit: with `browsing=true` set by the new helper, assert the F1 predicate (`!relay.browsing && …`) is false; jsdom click-through of a `.search-result-item` asserting `relay.browsing===true` and the bar's `is-visible`.

---

## ROLEWEB-07 — `html[data-role]` is never set on pure web, and the code's own comment claims otherwise — one base-CSS reshuffle away from breaking the follower layout

**Severity: low · Surface: web · Ship: web-only**

### Code walk
- `renderDirectorModeBadge` (app.js:845-852) writes `document.documentElement.dataset.role = isDirector ? "director" : "follower"` and its comment says: *"Default 'follower' so signovivo.com is right from boot"* (app.js:847-848).
- The function's ONLY call site is the native `role` event (app.js:952). No boot path calls it. On signovivo.com the attribute is absent forever.
- The follower layout survives only because follower styles are the unselected CSS base (styles.css:2150-2156). Any future rule written as `html[data-role="follower"]` (the natural reading given the comment and the existing director selectors), or any JS that branches on the attribute, silently misbehaves on the entire public web surface — while working perfectly in the native shell where the event always fires. That works-on-iPad / broken-on-web skew is this app's worst debugging class.

### Fix approach
One line at boot (top of `initReader` or beside the `directorModeBadge` lookup): `document.documentElement.dataset.role = "follower";` — making the comment true and the attribute reliable ground truth on every surface.

### Acceptance criteria
- On pure web at boot, `document.documentElement.getAttribute("data-role") === "follower"` before first paint settles; flips to `director` only on a native role event (unchanged).

### Test idea
jsdom boot test asserting the attribute; fold into ROLEWEB-05/06's harness.

---

## Parking lot (speculative / adjacent — NOT findings)

- **`/unlock` stub retirement date**: the always-ok stub (index.ts:766-771) exists solely for pre-374 clients. Once the fleet dashboard shows no check-ins below build 374, delete the route. Needs fleet data, not a code fix.
- **`state.syncRole` dead duplicate** of `state.nativeSyncRole` (app.js:177-179, written :949, never read elsewhere) — fold into any app.js cleanup PR (map-web O6).
- **4-digit garbage lands on the LAST page**: `resolveSongPage` falls through to `state.totalPages` when no song >= the number exists (app.js:700-708) — a mistyped code fragment (first 4 digits of a director code) silently opens page 371. Song-jump lens territory (map-web O12), noted because the numpad doubles as the code-entry surface.
- **Exit-director `window.confirm`** (app.js:2428): fine on web (never shown); a custom in-page confirm would be styleable for the portrait iPad — only if a native-side report shows the system dialog is jarring.
- **og/twitter descriptions promise live-following** (index.html:19,29) that a pre-Mass visitor cannot see any evidence of — resolved automatically if M4's waiting-state pill (ROLEWEB-04) ships.
- **Numpad 10-digit cap on web** (app.js:1144) is now consistent with 10-digit phone-number codes (legacy 11-digit code retired per prior-art); the old [L] finding `web-reader-draft-cap-blocks-legacy-11-digit-code` appears moot at HEAD — worth a one-line status update in the findings index rather than a new finding.
