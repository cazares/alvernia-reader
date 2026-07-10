# FOLWEB — Follower experience, web PWA (audit lens findings)

> Lens: web/PWA follower end-to-end. Verified against CURRENT HEAD `d5075091` (build 381,
> post-#269/#270/#271 — fleet picker REMOVED, sync spinner simplified). All file:line anchors are
> current source, re-verified by direct read, not doc anchors.
> Files: `web/src/app.js` (3572 lines), `web/src/index.html` (363), `web/src/styles.css` (2345),
> `web/src/sw.js` (237), `web/src/manifest.webmanifest` (35).
> Dedupe: checked every finding against the prior-art map KNOWN-FINDINGS + PLANNED-WORK.
> Items marked **DELTA** intentionally report only the unlisted angle of a known/planned item.
> shipVector note: all findings are 'web-only' — they ship instantly to phones/PWA via Pages;
> the native shell's bundled copy picks them up at the next native build (harmless there: these
> surfaces are either web-relay-only or shared UI that native also benefits from).

---

## FOLWEB-01 — Total connectivity loss never demotes the green "En vivo" pill; follower silently strands behind a live-looking indicator  (HIGH, web-only)

**DELTA on planned M4 status pill** (docs/major-update-2026-07.md:368-372) — the plan's motivating
scenario ("dead director on a healthy socket") is already FIXED (#248/#263). The **unlisted trigger**
is the inverse: a healthy director and a follower whose DEVICE lost connectivity. Nothing anywhere
documents that today's pill lies green in that case.

**Trigger / repro.** Web follower (phone on parish LTE / church wifi) is following live —
green pulsing dot, `relay.hasDirector=true`, `relay.following=true`. The device's network dies
(wifi drop, LTE dead spot, airplane-mode fat-finger, or the worker unreachable at the network
layer). From that moment:

1. The WS closes (or zombies) → F2/F3/close-handler reconnect loops run forever
   (app.js:3300-3325, 3363-3375) — correct.
2. `relayPollOnce` fetches fail and are swallowed whole: `} catch {}` (app.js:3209). **No code path
   touches `relay.hasDirector` / `relay.following` on failure.**
3. Demotion exists ONLY inside `applyRelaySnapshot` → `svSyncDecision.decideRelaySnapshot`, which
   runs **only when a snapshot arrives** (app.js:3113-3166). No snapshot ⇒ no decision ⇒ no demote.
4. `renderRelayPill` (app.js:3036-3042) therefore keeps rendering `is-live` (green, pulsing,
   `aria-label="En vivo con el director"`).

**User impact at Mass.** The page freezes on the director's last position while the indicator keeps
asserting "live". A 70-year-old holds a frozen page through two hymns believing they're following.
There is no cue to tap ⟳ (and even ⟳ would spin 1.1s and "succeed" visually — see FOLWEB-10).
Recovery is automatic once the network returns, but the *lie during the outage* is the defect.

**Code walk (the hole).**
- `relay.lastMsgAt` is already maintained (WS message stamp app.js:3298, open stamp :3259) but is
  only consulted for socket-teardown decisions, never for pill state.
- There is no `lastGoodSnapshotAt` — a timestamp of the last *successfully applied/judged* snapshot
  (WS or poll) — which is the datum a staleness decay needs.

**Proposed fix (small, shippable before M4).**
1. Stamp `relay.lastSnapshotAt = Date.now()` at the two success points: WS message apply
   (app.js:3298) and poll-body apply (app.js:3207).
2. In the existing F3 health interval (app.js:3364-3374, already a guaranteed 10s tick), add a
   presentation decay: if `relay.hasDirector && Date.now() - relay.lastSnapshotAt > 30_000`, switch
   the pill to a distinct "reconnecting" visual (e.g. `is-stale`: grey/hollow, no pulse,
   `aria-label="Reconectando…"`), WITHOUT clearing `relay.hasDirector` (sync logic untouched, purely
   presentational). After `RELAY_LIVE_MAX_AGE_S` (90s) optionally fully demote (`hasDirector=false`,
   `lastSeq=-1`, hide pill) so state converges with what a successful poll would have decided.
3. On the next applied snapshot the normal decision path repaints — no new state machine.
Keep the threshold ≥2 poll intervals + jitter so a single slow fetch never flickers the pill.

**Acceptance criteria.**
- Following live, then cut network (airplane mode): within ≤30s the pill visibly stops claiming
  "En vivo"; page stays put; no reload.
- Restore network: pill returns to green within one heartbeat/poll (≤5s) and page re-homes.
- A slow-but-working network (3-4s RTT polls) never leaves the green state.

**Test idea.** Unit-test the decay as a pure function (feed `{hasDirector, lastSnapshotAt, now}` →
pill state) in the svSyncDecision style; browser-verify with DevTools offline toggle.

**majorUpdateIntersection:** M4 §6.3 "tri-state status pill + 4s freshness-decay timer" subsumes
this; this finding is the minimal pre-M4 web-only version and the missing requirement (decay must
fire on *no data*, not just on stale data).

---

## FOLWEB-02 — No wake-lock fallback and no Auto-Lock guidance on the exact device class the parish uses (pre-iOS-16.4 web PWA follower)  (HIGH, web-only)

**Context.** The one congregation device that CANNOT run the native app is the old iPad running the
signovivo.com home-screen PWA (project memory: "TestFlight needs iOS 16+ … no wake-lock
pre-iOS-16.4. Set Auto-Lock Never" — an ORAL protocol). Prior art marks
`perf-no-screen-wake-lock-web-followers` **FIXED-#241**, but the fix only covers browsers that
implement the Screen Wake Lock API (Safari/WebKit ≥16.4). This is the unlisted residual half.

**Code walk.** `initScreenWakeLock` (app.js:3500-3516):
- `if (!("wakeLock" in navigator) || …) return;` (:3502) — silent no-op on the old iPad. No
  fallback, no message, no flag anywhere. The screen sleeps at the device's Auto-Lock interval
  mid-Mass and the follower goes DARK.
- Even where supported, `acquire()` swallows rejections (`catch (_) { sentinel = null; }` :3509) —
  e.g. iOS Low Power Mode denies the lock — again with zero surface.
- Nothing in the UI, help (unreachable anyway — FOLWEB-05), or fleet check-in mentions Auto-Lock.
  `fleetCheckin` (app.js:2951-2981) doesn't report wake-lock capability, so the operator's
  dashboard can't flag the at-risk device.

**User impact.** Mid-Mass the old iPad's screen turns off; an elderly follower doesn't know the
wake + re-open dance re-syncs it. If Auto-Lock was left at "2 minutes" by whoever last touched
Settings, this repeats every 2 minutes all Mass.

**Proposed fix.**
1. **Detect + surface once:** where :3502 returns today, if `NATIVE_FILE_MODE` is false and the UA
   is iOS, show a one-time dismissible banner (localStorage-flagged, reuse the relay-warn styling
   pattern app.js:880-935): "Para que la pantalla no se apague durante la Misa: Ajustes → Pantalla y
   brillo → Bloqueo automático → **Nunca**." Gate on `isStandaloneApp` (app.js:203-205) so casual
   Safari visitors aren't nagged — the standalone old iPad is the target.
2. **Optional fallback:** a muted, tiny looping inline `<video>` (NoSleep.js technique) kept playing
   while `relay.hasDirector` — works on iOS ≥10. Weigh battery vs. benefit; the banner alone fixes
   the fleet device if Settings are set once.
3. **Report capability to fleet:** add `wakeLock: boolean` to the check-in payload
   (app.js:2960-2972) so /fleet-dashboard can permanently mark the old iPad "needs Auto-Lock Nunca".

**Acceptance criteria.** On an iOS <16.4 Safari standalone launch, the hint shows exactly once, is
dismissible, never blocks boot (try/catch like everything else). On iOS ≥16.4 nothing changes.

**Test idea.** Extract `shouldShowAutoLockHint(navigatorLike, standalone, dismissedFlag)` as a pure
function + unit tests; manual verify on the actual old iPad on the 2-device day.

---

## FOLWEB-03 — Add-to-Home-Screen is guided NOWHERE in-app; the PWA install protocol is oral tradition  (MEDIUM, web-only)

**Evidence.** Zero install-related strings or logic in the bundle: no occurrence of
"inicio"/"instalar"/"Compartir"/`beforeinstallprompt` anywhere in web/src (grep clean; the only
"pantalla" hits are page-swipe help text, index.html:293). `isStandaloneApp` (app.js:203-205) is
computed but used only for fleet `homeScreen` reporting (app.js:2968) and the iOS pseudo-fullscreen
gate (app.js:212-213). The manifest is install-ready (manifest.webmanifest:1-35, maskable icon,
standalone) — but no UI ever tells a phone user the app is installable or why (offline in a no-wifi
church + fullscreen + faster launch).

**User impact.** The documented old-iPad protocol (Safari → share → "Agregar a pantalla de inicio" →
Auto-Lock Nunca) lives only in Miguel's memory. Congregants who open the WhatsApp-shared link (the
og: tags at index.html:15-30 exist precisely for that share flow) stay in a Safari tab: no offline
resilience if they arrive at church before caching, Safari chrome eats screen, and the back-swipe
can exit the app (see FOLWEB-08). If Miguel is unavailable, nobody can provision a replacement.

**Proposed fix.**
1. iOS Safari, NOT standalone, NOT native: after boot settles (and never during a live follow —
   gate on `!relay.hasDirector` or delay ~30s), show a one-time dismissible card: icon + "Instala
   Signo Vivo: toca **Compartir** (⬆︎) y luego **Agregar a pantalla de inicio** — funciona sin
   internet." localStorage `sv-a2hs-dismissed`. Reuse the injected-banner pattern.
2. Android/Chromium: capture `beforeinstallprompt`, stash it, surface the same card whose button
   calls `prompt()`.
3. Keep it out of native file mode (`NATIVE_FILE_MODE` guard) and out of `?selftest`/staging runs.

**Acceptance criteria.** First visit in an iOS Safari tab shows the card once after the reader is
usable; installed/standalone launches never see it; dismissing persists; a live director suppresses
it until idle.

**Test idea.** Pure function `shouldOfferInstall({standalone, native, dismissed, hasDirector, ua})`
+ unit tests; manual check on iPhone Safari + Chrome Android emulation.

---

## FOLWEB-04 — The live dot physically overlaps the ♪ fab, and its amber "tap to re-sync" affordance is an untappable 8px target  (MEDIUM, web-only)

**DELTA on planned M4** ("Loud, correct status pill … **replaces the 8px dot**",
docs/major-update-2026-07.md:368-370). The replacement is planned/unbuilt; what NO doc records is
that the current dot is (a) rendered **on top of the ♪ button**, and (b) an 8×8px tap target.

**Code walk / geometry.**
- Pill: `position:fixed; top:max(0.6rem,safe); right:max(0.7rem,safe); width:8px; height:8px;
  z-index:46` (app.js:3016-3023) → spans ~11-19px from the right edge, ~10-18px from the top.
- ♪ fab: `position:fixed; top:max(0.55rem,safe); right:max(0.55rem,safe); width:4rem; height:4rem;
  z-index:45` (styles.css:2094-2102, 2122) → spans ~9-73px from the right edge.
- ⇒ the dot sits fully INSIDE the fab's top-right corner, painted above it (46 > 45). It reads as a
  mysterious badge ON the ♪ button, not an app-level "you are live" status. When the drawer opens
  the fab hides (`body.sv-drawer-open .song-jump-fab{display:none}`, styles.css:2159-2160) but the
  dot stays — the "badge" detaches from its apparent host.
- Tap affordance: the amber state is clickable → `goLive()` (app.js:3028-3031), but any tap missing
  the 8px circle lands on the 64px ♪ fab underneath and opens the jump modal instead — the opposite
  of the user's intent. WCAG minimum target is 44px; parish users are elderly.

**Mitigation already present.** The amber case is always accompanied by the bottom
"↩ Volver a en vivo" bar (app.js:3068, shown at :1203), which is the real recovery path — so this
is comprehension/mislead, not a stranding.

**Proposed fix (pre-M4 stopgap, or fold into M4).** Move the pill left of the ♪ fab
(`right: calc(max(0.55rem, safe) + 4.5rem)` — same offset as the director's ♪ shift,
styles.css:2123) and give it a labeled capsule form (`● En vivo` / `◌ …`) with ≥44px hit area; or
drop the pill's click handler and let the bar own the tap affordance. If M4 lands soon, implement
the tri-state pill there and delete this dot entirely.

**Acceptance criteria.** No fixed element overlaps another interactive element's hit area at
375/768/1024px widths, portrait + landscape; the live indicator has a text label; any clickable
status element is ≥44px.

**Test idea.** Playwright/manual: `document.elementFromPoint()` at the pill's center and ±20px
returns the intended element; visual check both roles.

**majorUpdateIntersection:** M4 replaces the dot outright; this delta is the interim collision fix
and a layout constraint M4's pill must respect (don't re-park the new pill inside the fab).

---

## FOLWEB-05 — Every word of in-app help is unreachable on web: the only opener is a `display:none` stub (and the help content is stale)  (MEDIUM, web-only)

**Evidence.**
- `#help-panel` with the full "¿Cómo funciona?" instructions, the haptics toggle, and the
  "Versión N" label: index.html:278-348.
- Its ONLY opener is `#help-button` — a hidden compat stub: `style="display:none"` aria-hidden
  (index.html:210). The click wiring exists and works (app.js:2662-2669); it just can't be reached.
- Not covered by prior art: P8 lists the dead *offline-gate* UI, `#books-data`, HANDOFF.md etc., but
  not the help panel.
- The content is also stale where it matters: "toca la franja oscura a la izquierda"
  (index.html:300) describes `#drawer-handle`, which is `display:none !important`
  (styles.css:2331); "Escribe el número … toca ↵ Ir" (index.html:314) describes the retired drawer
  numpad (the real control is "♪ Abrir Canto ♪").

**User impact.** A confused follower at Mass has zero self-service path: no explanation of swipes,
the drawer gesture, the dot, the go-live bar — nothing. The haptic preference has no UI to change
it (the stored pref still applies, app.js hapticToggle wiring :2681-2685 unreachable). This
compounds FOLWEB-03/08: the app's entire interaction model is undocumented *in the app*.

**Proposed fix.** Decide the product intent, then do ONE of:
- (a) **Resurrect (recommended):** add a small "?" entry (e.g. a row in the jump modal or drawer top
  bar) that opens `#help-panel`; REWRITE the stale items (swipe-to-open-drawer, ♪ modal, live
  dot/bar semantics, offline capability, A2HS pointer — one panel can satisfy FOLWEB-03/06's
  "where do I learn this" need); keep it follower-first.
- (b) **Retire:** delete panel + wiring + haptic toggle so dead UI stops accreting — but then
  FOLWEB-03/06 need their own small surfaces.

**Acceptance criteria.** Help reachable in ≤2 taps from the base reader; every claim in it matches a
control that exists at build 38x; version label visible (useful for phone-side support).

**Test idea.** e2e DOM check: the opener is visible and opens the panel; content test pinning that
help text references only live selectors/gestures.

---

## FOLWEB-06 — Offline readiness is invisible to the follower; precache failure is silent (user-facing DELTA on `offline-pwa-dead-offline-gate-ui`)  (MEDIUM, web-only)

Known finding `offline-pwa-dead-offline-gate-ui` [L, OPEN → P8] frames this as *dead code to
delete*. The unlisted **user-facing angle**: with (or after deleting) that dead UI, the app has NO
surface that tells the person holding the device whether it will survive the no-wifi church.

**Code walk.**
- Precache runs silently: `deferOfflinePrecache` → `ensureOfflineBundle(totalPages, () => {})` — the
  progress callback is a no-op (app.js:629); success writes a flag + IDB metadata and reports to the
  *operator* fleet dashboard only (`fleetCheckin({webCached:true})`, app.js:606-607).
- Failure is console-only: `console.warn("Pre-cache offline incompleto:", error)` (app.js:631); and
  (known, not re-reported) there is no in-session retry trigger after the flag reset (:630).
- The verification function `isOfflineBundleReady` (app.js:641-666) has ZERO callers.
- The `?selftest` card is operator tooling behind a query param; the fleet dashboard is behind a
  secret. The USER has nothing.

**User impact.** The congregant who installed the PWA at home cannot tell whether the 371 pages
finished caching before they left. If the precache died at page 180 they discover it at Mass as
"No se pudo cargar esta página" on later pages — with the director live and no network to recover.
For the old-iPad fleet device, a substitute operator (not Miguel) has no way to verify Mass-readiness.

**Proposed fix (lightweight, NOT a resurrection of the old gate).**
1. On successful `ensureOfflineBundle` completion, one-time toast: "✓ Signo Vivo ya funciona sin
   internet en este dispositivo" (flagged per CACHE_VERSION so re-verifications stay silent).
2. On failure, a quiet retriable chip: "Descarga sin internet incompleta — toca para reintentar"
   wired to the precache kick (fixes the no-retry gap as a side effect; coordinate with known
   `offline-pwa-precache-no-in-session-retry`).
3. Put the durable answer in the help panel (FOLWEB-05a): a "¿Funciona sin internet?" row calling
   `isOfflineBundleReady(state.totalPages)` — giving that dead function its first real caller.
4. P8's deletion pass should then remove only the old `#offline-gate` DOM, keeping these.

**Acceptance criteria.** Fresh profile + throttled network: exactly one success signal when caching
completes; simulated fetch failure mid-precache surfaces the retry chip; airplane-mode relaunch
then works end-to-end (pages 1→371) with no cue ever blocking a live follow.

**majorUpdateIntersection:** P8 (constrains what P8 may delete); M5/M6 add *operator* book-version
visibility but nothing user-facing.

---

## FOLWEB-07 — Numpad jump fails silently to the LAST page: unknown song numbers, and ANY song number typed before the index hydrates  (MEDIUM, web-only)

**Code walk.**
- `resolveSongPage` (app.js:700-709): exact map hit → page; else *first song ≥ N*; else
  **`state.totalPages`** — the last page of the book (371, an arbitrary end page).
- (a) Typo: songs ≤4 digits are ALWAYS treated as songs (code gate is ≥5 digits, app.js:1178). A
  follower who fat-fingers "412" (no such song) is silently teleported to page 371 AND — because
  371 ≠ livePage — dropped out of live-follow into browse mode (app.js:1200-1205). Two surprises
  for one typo, no error message (contrast: a 5-digit entry at least flashes "Código no válido",
  app.js:1189).
- (b) Empty-index window: on web `songIndex` starts `[]` (app.js:149) and hydrates from a
  BACKGROUND fetch of pages.json (app.js:3454-3459). Until it lands, `songPageLookup` is empty ⇒
  **every** song number resolves to `state.totalPages`. On a slow first visit a user who
  immediately taps ♪ and types a valid "145" lands on page 371.
- (c) Hydration failure sticks all session: the background fetch has no `response.ok` check and no
  retry — `.then(r => r.json()).catch(console.warn)` (app.js:3455-3458). One transient failure
  (first visit, flaky LTE, captive portal) leaves songIndex empty forever ⇒ the ♪ modal (the
  PRIMARY navigation control) silently sends every entry to page 371 and all drawer tabs list
  nothing. (The BOOT manifest path was hardened in #238; this background hydrate was not. On
  second visits pages.json is normally in caches — coreAssets app.js:247-251 — so the window is
  mostly first-visit.)

**User impact.** "I typed the song number and it took me to the back of the book" — undermines the
one control followers are taught. At Mass with a live director, (a) additionally pauses their
auto-follow.

**Proposed fix.**
1. In `goToDraftSong`: if `state.songIndex.length === 0` → flash "Cargando índice…" (reuse
   `flashSongDisplay`, app.js:831) and re-kick the hydrate; do NOT navigate.
2. No exact match and no `next` → flash "No existe el canto N", keep the modal open; never fall
   through to `totalPages`. (Keep the *first song ≥ N* behavior for in-range gaps — genuinely
   useful.)
3. Harden the hydrate: check `response.ok`, retry ×3 with backoff, re-attempt on
   `visibilitychange`/`online` while `songIndex` is empty.

**Acceptance criteria.** With an empty index, no numpad entry navigates; with a hydrated index,
out-of-range numbers show an error and stay put; a blocked-then-restored pages.json fetch recovers
without reload.

**Test idea.** Unit-test `resolveSongPage`+wrapper for `{empty index, gap number, >max number}`;
e2e: block `**/pages.json`, type 145, assert page unchanged + message; unblock, assert recovery.

---

## FOLWEB-08 — Song browse/search is undiscoverable for followers: drawer is edge-swipe-only (handle hidden), ⌕ is director-only, and the edge swipe collides with Safari's back gesture  (MEDIUM, web-only)

**Evidence.**
- The drawer contains the ONLY search-by-title/lyrics UI plus Misa/Recientes/Temas/Tono/Todas rails
  (index.html:224-272).
- Openers: (1) left-edge swipe — start <44px, move >40px right (viewer handler app.js:2725-2730;
  window-level app.js:2789-2810); (2) the ⌕ fab — `display:none` for followers, director-only
  (styles.css:2154-2155); (3) `#drawer-handle` — JS-wired but `display:none !important`
  (styles.css:2331, comment "Native parity: no left side-bar"). For every follower the drawer is
  reachable ONLY by an invisible gesture that no reachable text documents (the help that describes
  it is itself unreachable — FOLWEB-05).
- Safari-tab conflict: in a normal Safari tab (the WhatsApp-link arrival path), a left-edge
  rightward swipe is the system BACK gesture. With history (arrived via link/search), Safari
  navigates AWAY from signovivo.com; the app's touchend handler often never fires. In standalone
  mode there's no history so the gesture works — but nobody was told to install (FOLWEB-03).

**User impact.** A follower who knows the song's NAME but not its number (the normal case for
elderly congregants) has no path: ♪ is number-only. They are locked out of 6 of the app's 7
navigation surfaces. Worst case in a Safari tab: attempting the secret gesture exits the site
mid-Mass.

**Product note.** The hidden handle is deliberate "native parity" (styles.css:2330) — but parity
preserved a discoverability hole on both surfaces rather than solving it.

**Proposed fix (smallest first).**
1. Give followers the ⌕ fab too (drop the `html[data-role]` gate at styles.css:2154-2155; it opens
   the drawer in dropdown/search mode exactly as for directors, app.js:2410-2411). Search is
   read-only navigation — no director privilege involved. Coordinate with the known OPEN
   `web-reader-browse-result-click-skips-relay-browsing-mode` finding so a follower tapping a
   search result gets the browsing/bar treatment instead of a 4s F1 yank (fix together, don't
   duplicate).
2. And/or re-show a slim `#drawer-handle` (delete styles.css:2331 — JS wiring is already alive,
   app.js:1110/1123).
3. Mention the gesture in the resurrected help (FOLWEB-05).

**Acceptance criteria.** A first-time follower can reach search + song lists via a visible control
in ≤2 taps, on iPhone Safari tab AND standalone; director layout unchanged unless deliberately
unified.

**Test idea.** e2e: with no `data-role` set, assert a visible opener exists and opens the drawer;
regression-check director layout with `data-role="director"`.

---

## FOLWEB-09 — First-load screen is full-brightness WHITE in a dark-church, dark-themed app  (LOW, web-only)

**Evidence.** `#geo-gate{background:#fff}` (styles.css:70-79), dark-navy spinner/caption on white
(styles.css:96-111); comment: "a fast load still shows clean white" (styles.css:66-69). Everything
else is committed dark: `background:#000` on html/body (styles.css:41), `theme-color #060a18`
(index.html:6), manifest `background_color #060a18` (manifest.webmanifest:9) — so a standalone
LAUNCH paints a dark splash, flashes WHITE at first paint, then reveals the dark shell.

**User impact.** Evening Mass in a dim nave: every newly-opened phone flashes a full-screen white
beacon for the load duration (up to the 4s backstop, app.js:3532). Plus a perceived-quality
dark→white→dark flicker on every cold launch.

**Proposed fix.** Restyle the gate to the dark palette (bg `#060a18`, light spinner/caption) —
~6 lines of CSS; keep the 500ms delayed-fade. Check caption contrast (≥4.5:1).

**Acceptance criteria.** Cold standalone launch shows dark splash → dark loader → reader, no white
frame; crash/selftest overlays unaffected.

---

## FOLWEB-10 — ⟳ resync spins a fixed 1.1s regardless of outcome — false "success" feedback when offline  (LOW, web-only)

**Evidence.** The fab handler adds `is-spinning` and removes it on a hard 1100ms timer
(app.js:2415-2419); `reconnectRelay` (app.js:3088-3107) has no success/failure signal back to the
button. Offline, the poll fails silently (app.js:3209) and the WS won't open — but the user saw the
standard "it worked" animation. (Native reuses the same spin via `setSyncWorking`, app.js:863-878:
spin = working, stop = done — so stopping IS the app's success grammar.)

**User impact.** The one manual recovery control confirms success it can't know. Combined with
FOLWEB-01's green pill this completes the illusion of a healthy link on a dead network.

**Proposed fix.** On web, keep spinning until an applied snapshot arrives (`relay.lastSnapshotAt`
advances — piggyback on FOLWEB-01's stamp) or a ~6s cap; on cap-without-data flash a small "Sin
conexión" hint near the fab (or flip FOLWEB-01's stale pill immediately). Cheap version:
`if (!navigator.onLine)` show the hint instantly.

**Acceptance criteria.** Offline ⟳ tap visibly does NOT report success; online tap behaves as today.

---

## FOLWEB-11 — Manifest `display_override: ["fullscreen", …]` hides the status bar (clock/battery/notifications) on Android installs  (LOW, web-only)

**Evidence.** manifest.webmanifest:5 — `"display_override": ["fullscreen", "standalone"]` with
`"display": "standalone"` (:6). Chromium honors the override list first ⇒ Android installs run true
FULLSCREEN: no clock/battery, notification shade needs an extra system swipe. iOS ignores
`display_override`, so the asymmetry hits exactly the congregants' personal Android phones — the
least-supported cohort.

**User impact.** Elderly users lose the system clock/battery during Mass and can feel "trapped"
(fullscreen exit is non-obvious); no benefit over standalone for a static page viewer.

**Proposed fix.** Drop `"fullscreen"` from display_override (keep `["standalone"]` or delete the
key). One-line manifest change; propagates to installed PWAs on the next Chromium manifest
re-check, fresh installs immediately.

**Acceptance criteria.** New Android install shows the status bar; iOS behavior unchanged.

---

## FOLWEB-12 — First visit mid-Mass can flash page 1 → page 2 → director's page (the boot race is visible)  (LOW, web-only)

**Evidence.** The `<img>` ships hardcoded `src="books/standard/pages/page-001.webp"`
(index.html:54). Boot: relay peek raced against a 1500ms timer (app.js:3446-3448); if the poll
loses the race, `renderPage(DEFAULT_START_PAGE=2)` runs (app.js:3449, :215) and the gate lifts
(:3451) — then the late-resolving poll's `follow` decision renders the director's page seconds
later. On a slow-cell first visit: white gate → page 1/2 → snap to director. Self-healing (the
requestId guard makes the last render win) but visibly janky exactly at the "arrive late to Mass,
open the link" moment this flow exists for.

**User impact.** Two page flashes before landing live; a first-timer may start swiping from the
portada before the snap yanks them. Cosmetic, frequency-bounded (needs >1.5s relay RTT).

**Proposed fix.** Only render page 2 when the peek has definitively settled: extend the race window
modestly (~2.5s, still under the 4s gate backstop) OR track the in-flight peek promise and, when it
loses the race, keep the gate's spinner visible until it settles (≤6s abort) before committing
page 2. Either way, never commit page 2 while a peek response is already streaming.

**Acceptance criteria.** With a 3s-delayed /state (devtools throttling) and a live director, the
first page painted is the director's (or the loader holds ≤3s) — never page 2 → snap. With no
director, page 2 paints by ~2.5s.

**Test idea.** Playwright with route-delayed `**/state`; instrument `state.pageLoadRequest` and
assert a single commit.

---

# Parking lot (ideas / below the reporting bar)

- **`-webkit-text-size-adjust` absent** (no hit in styles.css): add `-webkit-text-size-adjust:100%`
  to the html/body reset per the client-site playbook — free polish; UI is mostly images so impact
  is small.
- **Known-finding interaction watch** (already in prior art, for the implementer): F1's ~4s re-home
  makes the drawer/swipe "browse without `relay.browsing`" inconsistency more visible — fix
  `web-reader-browse-result-click-skips-relay-browsing-mode` together with FOLWEB-08's ⌕ exposure
  or followers will get yanked out of search results.
- **Desktop keyboard model** (map O9): arrows move by SONG, swipes by PAGE; digits only while the
  modal is open (app.js:2813-2825). Low value for the parish.
- **Heartbeat send-failure swallowed** (map O16): `try{ws.send("ping")}catch{}` app.js:3285 — could
  tear down immediately instead of waiting the 12s window; marginal next to F2/F3.
- **`countCachedPageImages` counts old-version caches** — residual of the known PARTIAL
  fleet-webcached finding; version-scope when P3-CACHEVERSION lands.
- **Maskable icon is the plain square 512** (manifest.webmanifest:23-27): if the artwork has no
  safe-zone padding, Android maskable crops it. Verify the asset once.
- **"Código no válido" wording** for 5+ digit entries on web could say "Ese número no existe" —
  "código" is a native/director concept the web follower was never told about (fold into
  FOLWEB-07's messaging pass).
- **`og:image` → /icon.png** (index.html:21): icon.png is in SHELL_ASSETS (app.js:242) so it should
  ship; worth one manual check that the WhatsApp share card renders (it is the acquisition flow).
- **Offline nav with a query string** is known OPEN (`offline-pwa-query-string-…`); manifest
  `start_url:"/"` is clean so home-screen launches are safe from it (verified).
- **html/body `overflow:hidden` both axes** (styles.css:44-45) is the deliberate
  html-is-not-the-scroller pattern (fixed-viewport app, inner scrollers use `overflow-y:auto` +
  `overscroll-behavior:contain`, styles.css:923-926, 978-981) — checked against the §14
  root-scroller rules; NOT a defect here since nothing scrolls the root by design.
- **dvh fallback**: `--viewport-height:100dvh` (styles.css:24) would be invalid on pre-dvh Safari,
  but `bindViewportMetrics` overwrites it with px at boot (app.js:288-299) — safe; P-COMPAT already
  tracks formalizing this.
