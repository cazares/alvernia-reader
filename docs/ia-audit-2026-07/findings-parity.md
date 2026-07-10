# PARITY lens — same web bundle, two contexts (signovivo.com vs native WKWebView file://)

> Audit lens: cross-app parity. HEAD `d5075091` (build 381, version 1.0.4) — 3 commits past the
> cartographer maps' 16244b25/377 (#269 spinner cap, #270 fleet-modal removal, #271 spinner
> simplify + PDF rename). **All line anchors below verified against d5075091.**
> app.js = 3572 lines, PdfReaderApp.tsx = 1123, index.html = 363, styles.css = 2345.

Method: enumerated every user-visible control in `web/src/index.html` + every JS-injected surface
in `web/src/app.js`, then diffed presence/behavior/copy across (a) pure web, (b) native WKWebView,
(c) roles follower/director/transmitter/"off". Dedupe-checked against map-prior-art.md's
KNOWN-FINDINGS + PLANNED-WORK.

Context-gate inventory (all verified current): `NATIVE_FILE_MODE` app.js:227; native skips —
SW registration :2062, offline precache :622, fleet check-in :2952+:2989, relay follow :3329,
boot relay peek :3446, web relay resync :3093, fullscreen fab :1131. Native WebView props
(PdfReaderApp.tsx:1078-1108): `bounces={false}`, `allowsBackForwardNavigationGestures:false`,
`textInteractionEnabled={false}`, `allowsLinkPreview={false}` — edge-swipe/pinch parity is
structurally sound; divergences found are behavioral, below.

---

## PARITY-01 (HIGH, native) — Wrong director code on a native iPad gives ZERO feedback; the native comment says the web "surfaces 'código incorrecto'" but no such surface exists

**Trigger/repro:** On a native iPad, open ♪, type a 5+-digit code that is NOT in the baked
director set (e.g. one digit of a real 10-digit code mistyped), tap "♪ Abrir Canto ♪".
The modal closes and *nothing else happens*. No alert, no flash, no toast.

**Code walk:**
1. `goToDraftSong()` (web/src/app.js:1172-1192): for >=5 digits in native mode it posts
   `{type:"director-code", code}` then immediately `clearDraft(); closeSongJump();`
   (app.js:1184-1186) — the numpad display that could flash feedback is already gone.
2. Native `onDirectorCode` (PdfReaderApp.tsx:563-588): unrecognized code →
   `injectEvent({ type: "role", role: "none" })` with the comment *"Unrecognized → tell the web
   so it surfaces 'código incorrecto'"* (:584-586).
3. Web role handler (app.js:947-956): stores `state.syncRole`, maps `"none"→"off"`, calls
   `renderDirectorModeBadge()` (:845-852) — which only toggles the DIRECTOR badge and
   `html[data-role]`. **No error message is ever rendered.** The contract the native comment
   relies on was never implemented.
4. Git archaeology: the pre-rewrite native app showed
   `Alert.alert("Código incorrecto", "El código ingresado no es válido.")` — removed in the
   WKWebView rewrite `de934699` (build 332), which introduced the role:"none" + comment pattern.
   The feedback has been silently missing since build 332.

**User impact:** the exact 2026-07-01 outage shape: a volunteer director (or a director whose
app just crash-restarted and got the "Estabas dirigiendo… reingresa tu código" prompt,
PdfReaderApp.tsx:869-882) retypes their 10-digit code, fat-fingers one digit, and gets silence.
They cannot distinguish "wrong code" from "app broken" from "already director". At Mass, with
the congregation waiting, this is a derailment-class confusion. Note the boot resume prompt
explicitly tells them to re-enter the code — into a flow that swallows typos silently.

**Ambiguity to handle in the fix:** role:"none" is also injected for: empty code
(PdfReaderApp.tsx:572-574), failed becomeDirector on a non-follower (:525), and transmitter
exit-director (:773). So web cannot blindly flash "código incorrecto" on every role:"none".

**Proposed fix (pick one, prefer A, optionally +B):**
- (A) Native (robust, self-contained): in `onDirectorCode`, on `!isDirectorCode`, show
  `Alert.alert("Código incorrecto", "El código ingresado no es válido. Revísalo e inténtalo de nuevo.")`
  before/instead of the role:"none" inject (mirrors the pre-332 behavior; native Alert needs no
  bridge contract).
- (B) Bridge (additive wire): inject a dedicated `{type:"director-code-rejected"}` event; web
  handler shows a brief centered toast (reuse `flashSongDisplay` styling but as an overlay, since
  the modal is closed) — additive-only, old bundles ignore it (wire invariant preserved).
- Do NOT re-purpose role:"none" (see ambiguity above).

**Acceptance criteria:** wrong 5+-digit code on native → visible Spanish error within 1s; valid
code still shows the confirm Alert; empty/cancel paths unchanged; pure-web behavior unchanged.

**Test idea:** e2e source-pin: assert PdfReaderApp.tsx's `!isDirectorCode` branch contains an
`Alert.alert` (or the new event type), and app.js handles `director-code-rejected`. Device test
on the 2-device day: typo a code, expect the alert.

**shipVector:** native-build (fix A is native; fix B's web half only reaches iPads at the next
native build anyway). **majorUpdateIntersection:** M3 bridge v1 (typed/acked messages) is the
structural home for (B); M7 device-day should verify.

---

## PARITY-02 (HIGH, native) — Native follower cannot browse at all: every manual navigation is yanked back within ~1s with no feedback — the same ♪ jump that web deliberately supports with "Volver a en vivo"

**Trigger/repro:** Native iPad follower, mesh-connected to a live director. Congregant taps ♪,
jumps to song 250 to peek at the next hymn (or swipes a page, or uses the drawer's
"Siguiente Canción"). Page renders, then snaps back to the director's page within ~1s.
Repeat forever. No pill, no bar, no message explains why.

**Code walk (the yank loop):**
1. Web renders the jumped page → posts `page-changed` on EVERY render (app.js renderPage).
2. Native `page-changed` handler sets `currentPageRef.current = page`
   (PdfReaderApp.tsx:709-711) — a follower's `broadcastPage` call is a role-gated no-op (:358-368).
3. Director's mesh heartbeat re-sends its page every 1000ms
   (startDirectorHeartbeat, PdfReaderApp.tsx:391-400).
4. Follower mesh `page` event de-dupes ONLY against `currentPageRef`
   (PdfReaderApp.tsx:900-903): browsed page != director page → not a dup →
   `injectEvent({type:"sync-event", event:{type:"page", …}})` (:911).
5. Web `applyNativeSyncEvent` renders the director's page unconditionally (app.js:987-990,
   `renderPage(event.page, {pushToHistory:false})`). Back to square one, <=1s after the jump.

**Web-context contrast (same bundle, same button):** `goToDraftSong` grants explicit browse
mode on the relay path — `relay.browsing = true; relay.following = false; showGoLiveBar();`
(app.js:1197-1206) — with the amber pill + green "Volver a en vivo" bar to return. The web's
non-numpad navigation yank (~4s via F1) is a KNOWN open finding
(`web-reader-browse-result-click-skips-relay-browsing-mode`); this finding is its unlisted
NATIVE sibling, where even the sanctioned numpad path has no browse mode and the yank is 4x
faster. The old-iPad web-PWA follower sitting in the same pew behaves differently from the
parish native iPads — two devices, same UI, different rules.

**User impact:** the ♪ fab is a misleading affordance for every native follower: it appears to
work, then silently reverts. Elderly congregants read this as "the app is broken" or fight it
repeatedly during Mass. There is no way at all for a native follower to look ahead at the next
song's page while connected.

**Proposed fix:** implement browse mode in the shared bundle for the native sync path, mirroring
the relay design (all in web/src/app.js, so ONE mental model):
- Add `meshBrowse = {browsing:false, livePage:null}`; in `applyNativeSyncEvent`'s `page` branch,
  always update `livePage`; if `browsing`, suppress the render.
- Set `browsing=true` from the same `goToDraftSong` numpad-jump path when
  `targetPage !== meshBrowse.livePage`; reuse `showGoLiveBar()`/`goLive()` (generalize them off
  the `relay` object; they're pure DOM). `goLive()` for mesh renders `meshBrowse.livePage`.
- CAUTION: while browsing, the web still posts `page-changed` for the browsed page, so native's
  `currentPageRef` follows the browse — the heartbeat keeps injecting (good: keeps `livePage`
  fresh). Verify the render-failed sentinel (:792-794) and A3 director paths untouched (browse
  mode must be follower-only: gate on data-role !== "director").
- Decide policy for swipe/drawer nav (recommend: same as web today — only numpad jump grants
  browse peace; document it).

**Acceptance criteria:** native follower numpad-jumps off the live page → stays there, go-live
bar shown; tapping the bar or ⟳ returns to the director's live page <=1s; director page turns
while browsing update `livePage` silently; director/transmitter behavior byte-identical.

**Test idea:** unit-extract the decision (mirror svSyncDecision) so node tests cover
browsing/live-dup/follow for the mesh path; 2-device day: jump-away + return, crash-reload while
browsing (bridge-ready resync wins → returns live, acceptable).

**shipVector:** native-build (code lives in the web bundle but only reaches the affected devices
via the next TestFlight archive — a Pages deploy alone does NOT fix any native iPad).
**majorUpdateIntersection:** M4's always-visible tri-state status pill is the natural surface
for "estás navegando / en vivo" on native; land together or sequence this first.

---

## PARITY-03 (MEDIUM, web) — A VALID director code typed on signovivo.com flashes "Código no válido" — misleading copy in the exact disaster-fallback moment

**Trigger/repro:** director's iPad dies mid-Mass; they grab any phone, open signovivo.com,
tap ♪, type their real (valid) director code → red flash **"Código no válido"**.

**Code walk:** `goToDraftSong` (app.js:1172-1192): >=5 digits with no native bridge →
`flashSongDisplay("Código no válido", "err")` (:1188-1189). The comment (:1180-1182) admits the
truth: *"WEB: … a long code is meaningless"* — the code isn't invalid, the CAPABILITY is absent
on web (relay publish is native-only by architecture).

**User impact:** the message asserts the code is wrong. A stressed volunteer concludes they
forgot the code (or that it was rotated) and burns the outage window double-checking digits,
instead of grabbing a device with the app. Copy actively misdirects during the highest-stress
recovery flow this product has.

**Proposed fix:** change the flash to capability copy, e.g.
`"Dirigir solo está disponible en la app del iPad"` (fits the flash style). Optionally lengthen
the flash for this branch (flash helper app.js:831-841). No behavior change.

**Acceptance criteria:** web long-code entry flashes the capability message; native long-code
routing unchanged; song numbers (<=4 digits) unchanged.

**Test idea:** source pin if desired; visual check in browser preview.

**shipVector:** web-only (instant via Pages; the native bundle never hits this branch, so the
TestFlight lag is irrelevant). **majorUpdateIntersection:** none — but if a web-transmitter path
ever ships (M4+ identity work), this branch is where it mounts.

---

## PARITY-04 (MEDIUM, native) — The ?selftest readiness card is unreachable in the native shell, the one context its "Puente nativo" check was built for

**Trigger/repro:** operator wants the GREEN/RED pre-Mass card on a parish iPad (the devices
that actually serve Mass). There is no way to invoke it: the card is gated strictly on the URL
query (`app.js:3542-3543`, regex over `initialUrl.search`), and the shell loads
`file://…/WebBundle/index.html` with no query and no injected flag
(resolveBundleUri PdfReaderApp.tsx:813-826; preloadScript :1033-1042 injects only
FILE_MODE/BUNDLE_VERSION/INITIAL_BOOK).

**Code walk:** svSelftest check 5 (web/src/lib/svSelftest.js:92-100) is explicitly
`"Puente nativo … applicable only inside the iPad shell"` — on web it renders "—"
(applicable:false). So the bridge check is dead in BOTH contexts: web = not applicable,
native = unreachable. The canary-walk ritual (M1) can validate signovivo.com but cannot produce
the readiness card on the primary fleet; bridge health before Mass is checked by nobody.

**User impact:** operator/IA gap, not congregant-facing: the pre-Mass checklist's one-glance
device check silently doesn't exist where it matters most; a broken bridge/bundle on an iPad is
discovered live instead.

**Proposed fix (two options, recommend A):**
- (A) web-only trigger: reserve a diagnostic numpad code (e.g. `55555`) in `goToDraftSong`'s
  >=5-digit branch — checked BEFORE the native director-code post — that runs the existing
  selftest block (extract the IIFE body at app.js:3536-3572 into `runSelftest()`); works
  identically on web and native. Reaches iPads at the next native build (bundled copy).
- (B) native flag: preloadScript adds `window.__SIGNO_VINO_SELFTEST_AVAILABLE` + a gesture —
  needs native change; more moving parts.
**Careful:** the chosen code must never reach the relay or native role machinery, and must be
documented next to SOFT_RESET_CODE (744668486) to avoid collision.

**Acceptance criteria:** typing the diagnostic code on a native iPad shows the card with
"Puente nativo: conectado"; on web shows the card with "Puente nativo: —"; ?selftest URL path
unchanged.

**Test idea:** extend e2e/svSelftest.test.mjs to pin the trigger; manual on-device check M7.

**shipVector:** web-only mechanically, but effective on iPads only at the next native build —
schedule before the M7 2-device day so the day itself can use it.
**majorUpdateIntersection:** direct extension of M1 (selftest) and a tool FOR M7 verification.

---

## PARITY-05 (MEDIUM, native) — role:"none" renders as FOLLOWER UI with a dead ⟳: a transmitter that exits director mode looks like a follower but follows nothing

**Trigger/repro:** transmitter-only device (no mesh — Miguel's tethered phone/iPad,
`syncAvailable === false`) is directing to the relay; operator taps the DIRECTOR badge →
"✕ Salir" → confirm. Native drops to `roleRef "off"` and injects `{type:"role", role:"none"}`
(PdfReaderApp.tsx:754-774), with the comment *"the web shows no phantom follower UI"* (:768).

**Code walk (the comment is wrong):** web maps `"none" → "off"` (app.js:951) →
`renderDirectorModeBadge` sets `html[data-role] = isDirector ? "director" : "follower"`
(app.js:846-849) — there is NO "off" presentation. Base CSS shows the ⟳ resync fab for
everything non-director (styles.css:2154-2156). So the device shows the standard FOLLOWER
layout (⟳ + ♪). Tapping ⟳: web bridges `{type:"resync"}` (native mode, app.js:3093+) → native
resync handler (PdfReaderApp.tsx:727-752): `roleRef "off"` + `syncAvailable false` skips the
becomeFollower rescue (:733), skips all mesh calls (:735), `lastDirectorSnapshotRef` is null on
a device that was the publisher → **nothing happens**. The fab spins ~1.1s (false "working"
feedback) and no-ops. The shell has NO follower transport on this device (web relay is off in
file:// mode), so the "follower" UI is a lie: the page is frozen wherever they left it.

(Scope note: for MESH devices stranded in "off" — e.g. post-soft-reset — ⟳ genuinely rescues
via becomeFollower (:733); this finding is scoped to `!syncAvailable` devices only.)

**User impact:** narrow population (transmitter operators) but a deceptive state: after exiting
director mode the device looks like every other synced follower while silently static. If handed
to a congregant, it never follows.

**Proposed fix:** give "off" a real presentation: `renderDirectorModeBadge` sets
`data-role="off"` when `state.nativeSyncRole === "off"` AND a native bridge exists (pure web
must stay "follower"); CSS: `html[data-role="off"] .resync-fab { display:none; }` (or a truthful
"solo lectura" chip). Keep the mesh-device rescue: only apply the "off" presentation when native
reports no follower transport — cleanest is a new additive bridged field (e.g. role event gains
`canFollow:false` on the transmitter exit path) so the web doesn't guess. Update the stale
native comment either way.

**Acceptance criteria:** after transmitter exit-director: no ⟳ (or truthful standalone chip);
♪ still works; re-entering a code still promotes; pure-web layout byte-identical; mesh-device
"off" ⟳-rescue preserved.

**Test idea:** contract e2e pinning the none→off→data-role mapping + the new rule; device check
on the transmitter phone.

**shipVector:** native-build effectively (the affected device is native by definition; additive
bridge field is native+web). **majorUpdateIntersection:** M4's tri-state status pill would make
the frozen state visible too — same surface, coordinate.

---

## PARITY-06 (MEDIUM, cross) — The follower's entire index/browse surface hides behind an invisible edge swipe; only the director gets a visible entry point to the same drawer

**Trigger/repro:** any follower (web or native) wants the song list / themes / search /
prev-next-song buttons. The ONLY opener is a left-edge swipe (start <44px from edge, >40px
rightward — viewer handler app.js:2723-2731 + window-level :2786-2812). The visible pull-tab is
globally killed: `.drawer-handle { display: none !important; }` (styles.css:2331) while JS still
binds it (app.js:88 + open/close wiring). The help text explaining the gesture
("Desliza desde el borde izquierdo o toca la franja oscura a la izquierda", index.html:300 —
which also references the now-invisible handle) is itself unreachable (PARITY-08).
The director, by contrast, gets a big visible ⌕ fab into the same drawer
(app.js:2411, styles.css:2155).

**Context wrinkle (web only):** in a regular Safari TAB with back-history (arrived via a link),
iOS's own back-swipe owns the left edge and can shadow the gesture; in the home-screen PWA and
the native WKWebView (`allowsBackForwardNavigationGestures:false`, PdfReaderApp.tsx:1107) the
gesture is reliable. So discoverability is zero everywhere and mechanics vary by context.

**User impact:** for a zero-training congregation, prev/next-song buttons, Recientes, Temas and
search effectively don't exist. Followers are locked to swipe-by-page + numpad-by-number.
(The "Keynote-style" choice is documented in code (:2739) — deliberate != discoverable; the
role asymmetry is the parity defect: the surface exists for everyone, the AFFORDANCE only for
the director.)

**Proposed fix:** minimum: resurrect the pull-tab (delete styles.css:2331; the component is
fully styled+wired at styles.css:372-415 and JS-live) — a thin visible tab restores
discoverability in both contexts with zero new code. Optional: show ⌕ for followers too (drop
the director-only CSS gate) — but reconcile with PARITY-02's yank first, or the drawer is a
trap on native. Sequence: fix PARITY-02, then open this up.

**Acceptance criteria:** a first-time user can find the song list unaided on both surfaces;
director layout unchanged (verify the tab doesn't collide with the top-left DIRECTOR badge —
badge styles.css:154-157 vs handle mid-left).

**Test idea:** visual verification both contexts, portrait iPad.

**shipVector:** web-only (instant for phones/PWA; iPads at next native build).
**majorUpdateIntersection:** none.

---

## PARITY-07 (LOW, native) — Native still shows the first-boot "¿Quién usa este iPad?" prompt that #270 removed from web as "more annoying than useful"

**Code walk:** PR #270 (3db3a5ba, 2026-07-07) deleted the web self-ID modal with the rationale
"choir members mostly tapped 'Ahora no'… more annoying than useful"; web now checks in
anonymously. The native shell still runs the SAME question as an iOS `Alert.prompt` on first
boot (PdfReaderApp.tsx:216-246, "¿Quién usa este iPad? / Para el tablero del coro — una sola
vez."), persisting `sv_fleet_label`/`sv_fleet_skip`.

**Why it matters beyond consistency:** the fleet dashboard's roster matching is label-equality
(sync-worker/src/index.ts:443-446): after #270, WEB devices can never match a roster name
(always anonymous), so per-person readiness rows now depend entirely on NATIVE labels — meaning
naively removing the native prompt (full parity) would leave `devicesFor(name)` empty for
everyone and every roster row "No se ha visto — invitar" (index.ts:458-461). The two surfaces
are half-migrated around one dashboard.

**Proposed fix:** decide the label strategy once: (a) drop the native prompt too and make the
dashboard device-centric (labels seeded from roster.private mapping), or (b) keep native prompts
and mark roster rows as native-readiness-only. Don't leave the asymmetry undocumented.

**Acceptance criteria:** first-boot experience matches the chosen policy on both surfaces;
dashboard rows remain meaningful for the director.

**shipVector:** native-build (+ worker if the dashboard pivots → multi).
**majorUpdateIntersection:** M6 super-admin dashboard rebuilds this view — fold the decision in.

---

## PARITY-08 (LOW, cross) — Help panel (instructions + haptics toggle + "Versión" label) is unreachable in BOTH contexts; its copy also describes retired UI

**Code walk:** the panel's only opener `#help-button` is a hidden stub
(index.html:210, `style="display:none" aria-hidden`), yet app.js fully wires it
(:111-112, :2661-2670) and populates `#app-version-label` ("Versión N", app.js:3482-3484).
Panel content (index.html:278-348) includes the haptics toggle (`sv-haptic` pref is live code,
app.js:381-390) and instructions referencing the invisible drawer handle ("toca la franja
oscura", index.html:300) and the retired drawer numpad ("Escribe el número… toca ↵ Ir", :314).
Haptics themselves are `navigator.vibrate` (app.js:387-389) — unsupported on every iOS browser,
so the toggle governs a no-op on the entire parish fleet anyway.

**User impact:** a zero-training app ships with zero reachable instructions in either context;
plus ~70 lines of dead dialog + a dead settings control as debt.

**Proposed fix:** decide: (a) delete the panel + bindings + haptic toggle (haptics are iOS-dead),
or (b) resurface help behind a small "?" affordance (e.g. drawer top bar) with copy rewritten to
the CURRENT UI (♪ modal, edge swipe or restored handle per PARITY-06, ⟳ meaning). If PARITY-06
restores the handle, (b) becomes cheap and the copy nearly true again.

**Acceptance criteria:** no unreachable interactive UI remains in index.html; any surfaced help
copy matches shipped controls.

**shipVector:** web-only (native at next build). **majorUpdateIntersection:** P8 dead-code batch
is the natural bucket — note P8 lists other dead UI but NOT this panel.

---

## PARITY-09 (LOW, native) — DELTA of known `native-swift-stale-documents-bundle-masks-update`: NO on-device surface reveals which WEB bundle a native iPad is actually running

**Known core (not re-reported):** peer-pushed Documents/WebBundle preferred forever + version
compare uses the app's CFBundleVersion → stale web code under a new badge (OPEN, device-gated).
**This delta = the unlisted user-facing angle:** even in the healthy case there is no way to
READ the loaded web bundle's identity on-device, so the known failure class is undiagnosable in
the field:
- The build badge prefers the injected SHELL number:
  `resolvedBuild = window.__SIGNO_VINO_NATIVE_BUNDLE_VERSION || BUILD_NUMBER` (app.js:3477-3486)
  — in native it ALWAYS shows the shell build.
- The baked web `BUILD_NUMBER`/`CACHE_VERSION` are displayed nowhere reachable in native: the
  "Versión" label sits in the unreachable help panel (PARITY-08), and ?selftest (which shows
  build + cacheVersion, svSelftest.js:45-54) is unreachable in the shell (PARITY-04).

So when skew is suspected (mesh-pushed stale bundle; alt-script archive that skipped the
WebBundle sync — both known-OPEN), the support question "what does the badge say?" cannot detect
it: the badge answers for the shell, not the content.

**Proposed fix:** one line of glass: badge shows `shell·web` when they differ, e.g. `381·w379`
(compare the injected global vs baked BUILD_NUMBER at app.js:3477-3486; when equal, render just
the number as today). Ships in the web bundle → any FUTURE skew becomes self-announcing (the
stale bundle prints its own number).

**Acceptance criteria:** web badge unchanged; native badge unchanged in lockstep; visibly
composite when skewed. **Test idea:** unit-pin the resolver; simulate by setting the global in a
browser console.

**shipVector:** web-only mechanically; lands on iPads at the next native build (fine — it
protects every build AFTER that one). **majorUpdateIntersection:** P-OTA/M5 make bundle-skew a
first-class state; this is the cheap precursor gauge.

---

## PARITY-10 (LOW, web) — Long-press on the score pops iOS's image context menu on web/PWA; the native shell is quiet — divergent and disruptive mid-Mass

**Code walk:** `#page-image` (styles.css:211-222) has no `-webkit-touch-callout: none` and no
`-webkit-user-drag: none`; the shell's `user-select:none` (styles.css:146) does NOT suppress the
iOS image callout. On iOS Safari / home-screen PWA (the old-iPad follower + every personal
phone), a long-press on the page image opens the system context menu (Copiar / Guardar en
Fotos / Compartir…) covering the music; on iPad, touch-drag can also lift the image as a drag
item. The native WKWebView side is configured quiet (`textInteractionEnabled={false}`,
`allowsLinkPreview={false}`, `dataDetectorTypes="none"`, PdfReaderApp.tsx:1099-1105), so the two
contexts behave differently for the same hold-the-page gesture elderly users make while reading.

**Proposed fix:** `#page-image { -webkit-touch-callout: none; -webkit-user-drag: none; user-select: none; }`
(+ `draggable="false"` on the img). Zero functional loss; copyright posture arguably improves.

**Acceptance criteria:** long-press on the page produces nothing on iOS Safari/PWA; swipes and
pinch-zoom unaffected (meta viewport stays user-scalable, index.html:5 — keep it for elderly
zooming).

**shipVector:** web-only (instant where it matters: phones + the web-PWA iPad).
**majorUpdateIntersection:** none.

---

# Parking lot (not findings — speculative / covered-elsewhere / trivial)

- **Transmitter-director reports blank role to fleet** (PdfReaderApp.tsx:193-195 maps only
  `roleRef==="director"`→"Director"; explicitTransmitter → ""): display-only column
  (worker index.ts:512) — cosmetic; telemetry-lens territory. Same family: JS dbg telemetry logs
  transmitters as role "off".
- **`sv.book.lastPage.standard` still write-only** — verified at d5075091: single occurrence
  PdfReaderApp.tsx:717-720 (write), zero reads. Confirms the prior-art PARTIAL residual on
  `new-director-dead-writes`.
- **Dead global `__SIGNO_VINO_INITIAL_BOOK`** (PdfReaderApp.tsx:1038) — injected, never read;
  plus the `SIGNO_VINO` (vs VIVO) misspelling both sides — rename only with a both-sides-
  tolerant transition.
- **Comment drift:** index.html:50 says the badge is "web only" (shows on both, app.js:3486+);
  app.js:2097 says edge-swipe is "right edge" (it's left); app.js initReader comment still says
  scheduleFleetCheckin "offer[s] the one-time picker" (removed in #270).
- **Numpad tip is dead UI with stale copy** ("toca ↵ Ir", index.html:214-217) inside the
  CSS-retired numpad panel — P8 bundle.
- **Web fleet check-in gates on NATIVE_FILE_MODE only** (app.js:2952), not hasNativeBridge —
  fine while the shell is file://-only; would double-report if it ever loads https.
- **4-digit garbage songs resolve to the LAST page** (map-web O12) — same both contexts;
  interaction-lens item.
- **Edge-swipe vs Safari back-gesture** — detailed under PARITY-06; only bites in a
  history-laden Safari tab.
- **Haptics is a fleet-wide no-op** (navigator.vibrate unsupported on iOS) — folded into
  PARITY-08's delete-or-resurface decision.
