# SignoVivo full-app UX/robustness audit — verified brief

Adversarially-verified output of the 12-lens audit. Source of truth: `docs/ia-audit-2026-07/confirmed-findings.json`. Every finding below carries verdict **CONFIRMED**; citations were re-verified against current source (HEAD d5075091, build 381 — the branch fast-forwarded past build 377 via #269–#271). Full per-lens write-ups live in the `findings-<lens>.md` files referenced per entry.

## Stats

| Metric | Value |
|---|---|
| Lenses run | 12 |
| Raw findings | 115 |
| After dedupe | 73 |
| **Confirmed** | **62** |
| Duplicate of known/planned | 8 |
| Refuted | 3 |
| Unverified | 0 |
| By severity | critical 0 · high 10 · medium 26 · low 26 |

## Critic's coverage assessment

_(none provided — the `criticAssessment` field is empty in the verified output)_

## Confirmed findings

Grouped by severity (critical → low), then by surface (cross → native → web → worker).

## HIGH (10)

### Surface: cross

#### DIRNAT-01 — Invalid/rejected director code on native gives ZERO feedback — numpad closes, nothing happens (native comment claims the web shows 'código incorrecto'; that UI does not exist)

- **Lens:** dirnat
- **Ship vector:** multi
- **Evidence:**
  - `web/src/app.js:1183` — Native path: posts {type:'director-code'} then clearDraft()+closeSongJump() — the feedback surface is destroyed before validation; the 'Código no válido' flash at :1189 runs only on the pure-web else-branch
  - `PdfReaderApp.tsx:584` — Unrecognized code → injectEvent({type:'role',role:'none'}); comment at :585 claims the web 'surfaces código incorrecto'
  - `web/src/app.js:947` — role handler: role 'none' only sets state.nativeSyncRole='off' and hides the badge — no error UI anywhere
  - `docs/app-hardening-plan.md:576` — Prior docs (also :586) assume the web shows 'código incorrecto' — imagined UI; correct the docs with the fix
- **User impact:** A director who mistypes their 10-digit code pre-Mass gets total silence — the modal just closes. They cannot distinguish mistype from success-pending; the only success signal is a small badge appearing. They may face the congregation believing they are live while nothing syncs.
- **Recommendation:** Native: emit an additive {type:'director-code-result', ok:false} on the reject path (keep legacy role:'none'). Web: keep the jump modal open showing 'Verificando…' until a result arrives; flash 'Código incorrecto' on failure, close on role:'director'; 5s quiet timeout covers the native-confirm Cancel (which produces no event). Interim web-only heuristic: flash if role:'none' arrives within ~4s of sending a code.
- **Major-update intersection:** M3 typed/acked bridge: the director-code-result message belongs in the new envelope instead of overloading role:'none'.
- **Merged from:** FOLNAT-03, PARITY-01, N2W-01, W2N-01, SYNCE2E-04, FAILUX-01, IANAV-01
- **Verifier notes:** All evidence holds at HEAD. app.js:1183-1186: native branch posts {type:'director-code'} then clearDraft()+closeSongJump() — the numpad is destroyed before validation; the 'Código no válido' flash at :1189 runs only in the pure-web else branch. PdfReaderApp.tsx:584-587 rejects an unrecognized code with injectEvent({type:'role',role:'none'}) and the :585 comment claims the web 'surfaces código incorrecto' — but grep confirms the string 'incorrecto' appears nowhere in web/src/app.js; the role handler (app.js:947-956) for 'none' only sets nativeSyncRole='off' and re-renders the badge. Not a duplicate: prior art (Q2 super-admin gating, zombie-director P5, audit-reconciliation-374 §) all CITE 'có
- **Evidence corrections (verifier):** Minor userImpact nuance: on a VALID code the native shell shows a prominent confirm Alert ('¿Dirigir el coro?', PdfReaderApp.tsx:603-609) before promotion, so the success signal is that Alert (then the badge) — not only 'a small badge appearing'. The zero-feedback claim applies to the reject path (and to Cancel on the Alert, which also emits no event — the recommendation's 5s quiet timeout correctly covers this). All file:line citations are accurate at HEAD. | Citations verified accurate at current HEAD d5075091 (build 381; the branch fast-forwarded past build 377 via #269-#271 but none of the cited lines moved): web/src/app.js:1183-1190 (native branch), :1189 (web-only flash), :947-953 (role handler); PdfReaderApp.tsx:584-587 (reject path, comment at :585); docs/app-hardening-plan.md:576 and :586 (also :863) reference the imagined 'código incorrecto' UI.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### DIRNAT-06 — M4 DELTA: relay publishes failing with NETWORK errors (not 401/403) are invisible to the director — web congregation freezes with no director-side signal, and the planned M4 pill spec covers mesh peerCount only

- **Lens:** dirnat
- **Ship vector:** multi
- **Evidence:**
  - `src/directorRelaySync.js:97` — doPublish catch {} — network failures/aborts swallowed; an infinite silent retry during a persistent uplink outage (hotspot drop, captive portal)
  - `src/directorRelaySync.js:89` — The only warning path is res.status 401||403 (relay-auth banner); comment asserts 5xx/429 are 'transient' — a dead uplink is neither
  - `docs/major-update-2026-07.md:368` — Planned M4 pill gives the director '● Dirigiendo — N conectados' from mesh peerCount only; DIAGNÓSTICO (:443-449) shows relay status but is a pre-Mass long-press screen — the live relay-leg health is unspecced
  - `PdfReaderApp.tsx:921` — Related M4 implementation detail: Swift emits peerCount on every state event but the bridge forwards only status/role/message
- **User impact:** Mesh keeps the iPads in sync so the director's screen looks perfect, while every web follower (personal phones, the old home-screen PWA iPad) freezes on the last page and demotes to 'sin director' after 90s. The one person who could fix it (toggle hotspot, move) gets no signal for the entire Mass — same blast radius as the 401 class the banner was built for.
- **Recommendation:** Track publish outcomes in directorRelaySync.js (lastPublishOkAt + setRelayHealthHandler mirroring the auth-error latch). Native samples on the existing 12s relay heartbeat: last OK older than ~45s while director/transmitter → inject one-shot {type:'relay-health', ok:false}; web reuses the banner element with copy 'Sin internet — los teléfonos en signovivo.com no están recibiendo tu página. Los iPads cercanos siguen bien.' Auto-hide on recovery.
- **Major-update intersection:** Extend M4's status-pill spec: the director pill needs two inputs — mesh peerCount AND relay publish-ack freshness (e.g. '● Dirigiendo — 5 iPads · web ✓/✗').
- **Merged from:** FAILUX-03
- **Verifier notes:** All evidence holds at HEAD: directorRelaySync.js:97-99 swallows every network throw/abort in a bare catch{}, and the only director-facing warning path is res.status 401/403 (:89-92, wired to injectEvent({type:'relay-auth-error'}) at PdfReaderApp.tsx:343-345 — the ONLY relay-failure signal in the app, added deliberately auth-scoped in commit 52d9c934). A persistent uplink outage (hotspot drop/captive portal) produces neither an ok response (which would re-arm at :88) nor a 401, so the infinite retry loop stays silent for the whole Mass while mesh keeps the director's iPad fleet looking perfect and web followers (congregants' phones + the old home-screen PWA iPad — documented real Mass users; 
- **Evidence corrections (verifier):** docs/major-update-2026-07.md DIAGNÓSTICO section (Ask 7 / §6.6) starts at line 441, not 443-449 as cited; all other file:line citations verified accurate at HEAD.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### DIRNAT-09 — A taken-over (demoted) director is never told: DIRECTOR_CONFLICT's ready-made Spanish message is dead copy, the badge silently vanishes, and their page snaps to the winner's under their fingers

- **Lens:** dirnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:934` — DIRECTOR_CONFLICT branch: stops heartbeat, becomeFollower(), pulls winner snapshot — event.message is never read or surfaced; nothing user-visible is injected
  - `ios/SignoVivo/DirectorSyncModule.swift:1546` — Swift already ships the explanation: 'Un nuevo director tomó el control. Este dispositivo cambió a modo seguidor.' — dead copy (as is 'Cediendo el control…' at :490)
  - `web/src/app.js:972` — Web's state handler uses only event.status for the ⟳ spinner — the forwarded mesh `message` strings are ignored everywhere
  - `PdfReaderApp.tsx:609` — Asymmetry: the new director's confirm says 'le quitarás el control' — the system knows a human is being displaced and informs only the displacer
- **User impact:** Takeover-by-conflict is the DESIGNED handoff mechanism (admin force-takeover rides it, :947-948). The displaced director experiences: badge gone, ⟳ appears, page jumps by itself. Mid-Mass they flail — keep swiping (now local-only), or re-enter their code and trigger the red warning / control ping-pong. One sentence on screen ends the confusion. Distinct from held M-F6 (loser's FOLLOWERS) and fixed C2 (loser's page sync).
- **Recommendation:** Shell JS only: in the DIRECTOR_CONFLICT branch, after becomeFollower(), Alert.alert('Otro director tomó el control', 'Este iPad ahora sigue al nuevo director. Si debes dirigir tú, vuelve a entrar tu código en ♪.', [Entendido]). Optionally later surface mesh state `message` strings as a small web toast.
- **Major-update intersection:** Implement alongside held M-F6 (followers' redirect hint) in the same M7/mesh batch; add a 'loser is informed' assertion to NEW-DIR-3's acceptance flow.
- **Verifier notes:** All four evidence points hold at HEAD. PdfReaderApp.tsx:932-944: the DIRECTOR_CONFLICT branch reads only event.code, never event.message, and produces zero user-visible output — becomeFollower() (PdfReaderApp.tsx:419-450) just injects {type:"role"} which silently re-renders the badge (web/src/app.js:947-956). Swift ships a ready-made Spanish explanation via emitError (DirectorSyncModule.swift:1431-1436, :1546) that JS drops; the :490 "Cediendo el control…" emitState message is likewise dropped by web/src/app.js:972-975 (setSyncWorking uses only event.status). The :609 asymmetry is accurate — only the displacer is informed. Not a duplicate: prior art's C2 (fixed) covers the demoted director's
- **Evidence corrections (verifier):** Minor: the quoted string 'le quitarás el control' is at PdfReaderApp.tsx:612, not :609 (609 is the liveDirector variant 'tú tomas el control...todos te seguirán a ti'); same Alert body, asymmetry claim unchanged. Also note the :490 'Cediendo el control...' string is technically forwarded to the web as a state message but dropped there (app.js:972-975 uses only event.status) — still effectively dead copy.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### RELVER-01 — Canary ritual's primary URL tests the WRONG bundle — signovivo.com?env=staging serves PROD content, ?env=staging only switches the relay room

- **Lens:** relver
- **Ship vector:** web-only
- **Evidence:**
  - `docs/pre-mass-checklist.md:23` — Step A.3 lists 'signovivo.com?env=staging' FIRST as where to point the canary iPad — that host is Pages prod branch main and always serves the current PROD bundle
  - `web/src/lib/svRelayRoom.js:31` — ?env=staging maps only to relay room alvernia-staging; nothing anywhere selects different CONTENT based on the param
  - `scripts/release.sh:34` — STAGING=1 deploys web/dist to Pages branch 'staging' (preview URL) — the canary content exists ONLY at the preview URL, never on signovivo.com
  - `docs/green-day-deploy-runbook.md:45` — Repeats the misconception in prose: 'the ?env=staging isolation is for the *bundle*' — it is for the ROOM
- **User impact:** The operator walks the canary against the OLD, already-proven prod bundle joined to an empty staging room; every checkbox passes trivially and a never-executed build is promoted to prod — the M1 release-safety gate is silently defeated, re-opening the 'busted for everyone at Mass' class it was built to prevent.
- **Recommendation:** Fix both docs to state the release.sh-printed preview URL is the ONLY canary content source and must be opened as https://staging.<project>.pages.dev/?env=staging (room pin ON TOP of preview content); remove signovivo.com?env=staging as a canary option; have STAGING=1 release.sh echo the exact combined URL as one copy-paste line; pair with RELVER-02/-11 visual markers.
- **Major-update intersection:** Corrects the operational half of M1 (staging channel, DONE) — the code is fine, the documented ritual around it is wrong; also affects how M7's native staging entry should be documented when it lands.
- **Verifier notes:** All evidence holds at HEAD. pre-mass-checklist.md:23-24 lists signovivo.com?env=staging as the FIRST canary target; svRelayRoom.js:31 (and app.js:2842-2853) confirm ?env=staging only selects the relay room alvernia-staging — no code anywhere selects different content by the param; release.sh deploys the STAGING=1 bundle only to Pages preview branch 'staging' (lines 33, 125), so canary content never appears on signovivo.com (Pages prod branch = main); green-day-deploy-runbook.md:45 verbatim states the isolation 'is for the *bundle*' when it is for the room. Aggravating: STAGING=1 skips the version bump (release.sh:39-41), so the canary iPad's build badge is identical for prod-vs-staging bundl
- **Evidence corrections (verifier):** scripts/release.sh: cite line 33 (DEPLOY_BRANCH="staging") and line 125 (wrangler pages deploy --branch "$DEPLOY_BRANCH"), not line 34 (an `else`). Also add scripts/release.sh:18 as a FIFTH evidence item — the script's own usage text repeats the misconception ('Prove a build here (signovivo.com?env=staging) before promoting to prod'); line 129 is the correct phrasing ('once promoted') and should be the model for the fix. pre-mass-checklist.md: the URL literal is on line 24 (step A.3 starts at 23). | Minor anchor fixes: pre-mass-checklist.md — the step starts at line 23 but the URL itself is on line 24. scripts/release.sh — the staging branch assignment is line 33 (DEPLOY_BRANCH="staging"), the actual Pages deploy is line 125; ADD line 18 as stronger evidence (usage comment literally says "Prove a build here (signovivo.com?env=staging) before promoting to prod") and note line 129's "once promoted" qualifier as the only technically-correct phrasing, absent from the checklist. svRelayRoom.js:31 and green-day-deploy-runbook.md:45 cited correctly.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

### Surface: native

#### DIRNAT-03 — After 'Sí, dirigir' activation is fire-and-forget: no progress indicator for the 2-4s+ mesh start, and a failed start silently demotes the just-confirmed director

- **Lens:** dirnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:516` — becomeDirector catch: wasFollower ? becomeFollower() : injectEvent(role:'none') — a confirmed promotion that fails (mesh start rejected twice) produces no dialog, banner, or message
  - `PdfReaderApp.tsx:498` — startNearbyDirector + one 2s-sleep retry — up to ~2-4s+ with zero 'activating…' feedback; the DIRECTOR badge appears only after success (:508)
  - `PdfReaderApp.tsx:934` — JS mesh 'error' listener acts only on DIRECTOR_CONFLICT — Swift's async DIRECTOR_START_FAILED (DirectorSyncModule.swift:1615) and its Spanish message are dropped
  - `ios/SignoVivo/DirectorSyncModule.swift:366` — DIRECTOR_TAKEOVER_REQUIRED rejection says 'Solicita permiso para tomar control' — swallowed by the catch, and no request-permission UI exists (takeover auto-denied, PdfReaderApp.tsx:946-952)
- **User impact:** The one moment the operator explicitly asserted control (tapped Sí/Tomar el control) is the one moment the app may silently refuse — Bluetooth off, permission race, radio warm-up. They believe they are directing; the absent badge is the only (subtle) negative signal. Congregation never follows.
- **Recommendation:** In the catch at :516 show Alert.alert('No se pudo activar el modo director', 'Revisa que Bluetooth y Wi-Fi estén encendidos e inténtalo de nuevo.', [Reintentar → becomeDirector(code), Cancelar]) guarded on the role generation. For the latency window, inject a transient state:'connecting' sync-event before starting so the existing ⟳ 'still working' spin (app.js:863-878) covers it — zero new web UI.
- **Major-update intersection:** Fold into the M7 native batch (DIAGNÓSTICO/panic buttons touch the same shell surfaces); M3's acked bridge later formalizes a director-code-result event.
- **Merged from:** FAILUX-02
- **Verifier notes:** All cited evidence holds at HEAD. PdfReaderApp.tsx:516-527's catch silently demotes a just-confirmed director (becomeFollower() or role:'none' injection) with no Alert/banner/message; :494-508 shows the only role feedback is injected after success, leaving the ~2s+ retry window with zero 'activating' indicator; the JS mesh 'error' listener (:932-944) handles only DIRECTOR_CONFLICT, dropping Swift's DIRECTOR_START_FAILED (DirectorSyncModule.swift:1615, whose own comment says it exists 'so the director UI can warn the user'); and DIRECTOR_TAKEOVER_REQUIRED's 'solicita permiso' remedy (Swift:366) has no UI since takeover requests are auto-denied (PdfReaderApp.tsx:946-952). Not in map-prior-art.
- **Evidence corrections (verifier):** Minor precision note (citations themselves are correct): the two failure shapes differ — Swift startDirector only REJECTS synchronously (DIRECTOR_TAKEOVER_REQUIRED / session-invalid), so the :516 silent-demote path is the takeover-race shape; Bluetooth-off / Local-Network-denied RESOLVES the promise and surfaces later as the async DIRECTOR_START_FAILED (Swift:1615, from didNotStartAdvertisingPeer) — in that shape the DIRECTOR badge DOES appear while the mesh is dead (M-F7's backoff retries forever and may recover). Both shapes are silent to the user; a fix should cover both (the :516 Alert for the reject shape, plus surfacing DIRECTOR_START_FAILED in the :934 listener for the async shape). | PdfReaderApp.tsx:516 catch is unreachable via Bluetooth/permission/radio failures — those emit async DIRECTOR_START_FAILED (DirectorSyncModule.swift:1615) AFTER startDirector already resolved at :384; the :498 retry sleep only runs after a promise rejection, which requires the DIRECTOR_TAKEOVER_REQUIRED race (:366) surviving a completed resetTransport (:1394/:1398) or a bridge failure. The :934 listener-drops-DIRECTOR_START_FAILED citation is accurate, but its consequence is a shown-badge-with-dead-advertising state (relay path at :358-368 still works; M-F7 retry at Swift :1617-1629 self-heals transients), not silent demotion. | Minor nuances, citations otherwise exact at HEAD: (1) the Swift :366 DIRECTOR_TAKEOVER_REQUIRED rejection is normally pre-empted — PdfReaderApp.tsx:482-491 drops the follower link via resetNearbyDirectorSync() before startDirector, so that guard only fires on a race (the :516-518 comment acknowledges this); the swallowed-rejection point stands. (2) The finding's 'the absent badge is the only negative signal' describes the promise-rejection path; on the async DIRECTOR_START_FAILED path (DirectorSyncModule.swift:1611-1616) the badge is PRESENT while advertising is dead, since emitError fires only when currentRole=='director' — an even more deceptive failure mode the fix should also cover (handle DIRECTOR_START_FAILED in the :932 listener, not just the catch).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### FOLNAT-01 — Native follower has no browse-away model: any local navigation is yanked back within ~1s by the director's mesh heartbeat, with full browse affordances (jump modal, drawer, swipes) still offered and no 'Volver a en vivo' bar or pill ever shown

- **Lens:** folnat
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:977` — applyNativeSyncEvent 'page' branch calls renderPage(event.page) with zero browsing check — unlike the relay path which consults relay.browsing via svSyncDecision (app.js:3134-3143)
  - `web/src/app.js:1200` — The only browsing-intent capture (numpad jump) is gated on relay.hasDirector, which is permanently false on native (relay disabled), so the flag is never set; showGoLiveBar (app.js:3068) has the same gate — the bar can never appear on native
  - `PdfReaderApp.tsx:393` — Director mesh heartbeat re-sends its page every 1s
  - `PdfReaderApp.tsx:903` — Follower mesh 'page' listener de-dupes only against currentPageRef, which the follower's local browse just updated via page-changed (:709-711) — so the director's page is 'new' and is injected, yanking the user back
  - `web/src/app.js:1062` — Every local render unconditionally posts page-changed to native, updating currentPageRef to the browsed page
- **User impact:** A congregant on a parish iPad who peeks at the next song or a chord mid-Mass gets snapped back within ~1 second, every time, with no explanation and no opt-in browse mode — the songbook looks haunted/broken. Web followers on signovivo.com get a sanctioned peek (amber pill + 'Volver a en vivo'); native followers get none, yet the UI still offers all the browse controls.
- **Recommendation:** Web-only fix: make the browse state transport-agnostic — set it from the same intentional-browse gestures web honors, track livePage from applied mesh events in applyNativeSyncEvent (update-not-render while browsing), and un-gate the go-live bar/pill from relay.hasDirector via a hasLiveDirector() that also counts a fresh mesh event (<8s). Native needs no change: an ignored inject never posts page-changed, so the heartbeat keeps offering the live page until go-live. Keep the F1 semantic: un-flagged stray swipes still get yanked home. Full design + acceptance criteria in the detail file.
- **Major-update intersection:** M4 sync-robustness: the planned always-visible tri-state pill should gain the amber 'browsing' state and this go-live model should land as part of M4's follow-semantics slice; the mesh half mirrors the still-open web-reader-browse-result-click-skips-relay-browsing finding. Web-only ships instantly to phones but reaches iPads only at the next native build / mesh bundle push.
- **Merged from:** PARITY-02, N2W-02, SYNCE2E-03
- **Verifier notes:** Every evidence line verified at HEAD. app.js:977-980 renders mesh page events with no browsing check (relay path consults relay.browsing via svSyncDecision at app.js:3134-3143). Relay init early-returns on native (app.js:3329 `if (hasNativeBridge() || NATIVE_FILE_MODE) return;`), so relay.hasDirector (init false, app.js:3006) is permanently false in the shell — the numpad browse-intent block (app.js:1200), showGoLiveBar (app.js:3068), and the relay pill (app.js:3038 hides when !hasDirector) can never activate on native. Director mesh heartbeat fires every 1000ms (PdfReaderApp.tsx:393-400); the follower listener de-dupes only against currentPageRef (PdfReaderApp.tsx:903), which the follower's
- **Evidence corrections (verifier):** All cited file:line anchors are accurate at HEAD. Minor additions for the implementer: the relay-disable gate that makes relay.hasDirector permanently false on native is app.js:3329; relay.hasDirector is initialized false at app.js:3006; the PdfReaderApp.tsx:903 comment says "2s mesh heartbeat" but the actual interval is 1000ms (PdfReaderApp.tsx:400).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/lens-folnat.md`

#### FOLNAT-02 — Mesh start failure (Local Network denied, radios off) is invisible and indistinguishable from 'no director': FOLLOWER_START_FAILED / DIRECTOR_START_FAILED are silently dropped by the native JS listener, and the old Settings-deep-link behavior was deleted without replacement

- **Lens:** folnat
- **Ship vector:** native-build
- **Evidence:**
  - `ios/SignoVivo/DirectorSyncModule.swift:1686` — didNotStartBrowsingForPeers emits FOLLOWER_START_FAILED with the comment 'Fires when Local Network permission is denied' — explicit intent to surface it (advertiser twin DIRECTOR_START_FAILED at :1611-1616 says 'so the director UI can warn the user')
  - `PdfReaderApp.tsx:932` — The JS mesh listener's 'error' case handles ONLY DIRECTOR_CONFLICT; every other code is dbgLog'd and dropped — no user surface anywhere
  - `ios/SignoVivo/DirectorSyncModule.swift:399` — startFollower resolves immediately; the browser-failure callback fires async later, so becomeFollower's 2s retry-once (PdfReaderApp.tsx:436-443) can never catch permission denial
  - `e2e/permission-flow.test.mjs:82` — Comment block records that the old reader's 'director-mode error offers Settings deep link' test was removed as dead behavior when the WebView shell landed — nothing replaced the guidance
  - `ios/SignoVivo/DirectorSyncModule.swift:298` — Foreground one-shot transport relaunch after the user fixes permissions exists (recovery works) — but nothing ever tells the user to open Settings
- **User impact:** A congregant who taps 'No permitir' on the first-run Local Network prompt (or launches with radios off) gets a perfectly normal-looking songbook that will NEVER sync: the ⟳ fab spins 1.1s then silence, identical to 'no director present'. The device is effectively stranded for sync with no user-discoverable recovery path, while the fleet dashboard still shows it checked in (HTTP unaffected), reinforcing false confidence.
- **Recommendation:** Forward FOLLOWER_START_FAILED/DIRECTOR_START_FAILED to the web as an additive sync-event (latched once per role session, re-armed on 'connected'), and show a dismissible follower banner in the showRelayAuthWarning pattern naming the fix: 'Ajustes → Privacidad → Red local → Signo Vivo' (auto-clear on next connected). Optionally add a native Alert with Linking.openSettings(). Add a denied-permission step to the M7 device-day script.
- **Major-update intersection:** Required input to M4's tri-state status pill: as designed (heartbeat-freshness-driven) the pill would show the same red 'Sin director' for this state and cannot distinguish 'can't sync — fix Settings' from 'no director'. Permanent home is M7's planned native DIAGNÓSTICO / LISTO-NO-LISTO screen.
- **Verifier notes:** All five evidence points hold at HEAD: Swift emits FOLLOWER_START_FAILED (DirectorSyncModule.swift:1690) / DIRECTOR_START_FAILED (:1615) explicitly to warn the user, but PdfReaderApp.tsx:932's error case handles only DIRECTOR_CONFLICT and drops everything else (repo-wide grep: no other consumer exists). startFollower resolves synchronously (:388-411) so becomeFollower's retry-once (PdfReaderApp.tsx:436-443) cannot catch the async permission-denial callback. The old Settings-deep-link UI was deleted (e2e/permission-flow.test.mjs:83-87 comment) with no replacement, and no Linking.openSettings exists in the shell. Impact is stronger than claimed in one respect: web/src/app.js:3329 startRelayFol
- **Evidence corrections (verifier):** Minor only: the FOLLOWER_START_FAILED emit is at DirectorSyncModule.swift:1690 (the :1686 citation is the didNotStartBrowsingForPeers declaration); DIRECTOR_START_FAILED emit is at :1615. startFollower is declared at :388 (cited :399 is inside its body). All notes otherwise accurate. | Minor line refinements only: startFollower spans DirectorSyncModule.swift:388-410 with the immediate resolve at :409 (cited :399 is inside the function body but before the resolve); the JS error case spans PdfReaderApp.tsx:932-944; becomeFollower's retry-once is at PdfReaderApp.tsx:432-442; the removed Settings-deep-link comment block is at e2e/permission-flow.test.mjs:83-87; the foreground one-shot relaunch is handleAppDidBecomeActive at DirectorSyncModule.swift:293-310. All citations otherwise exact.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/lens-folnat.md`

#### RELVER-06 — DELTA of native-swift-stale-documents-bundle-masks-update + held M-F3: mesh OTA re-transfers the ~30MB bundle and remounts the follower on EVERY reconnect — installed pack version never persisted

- **Lens:** relver
- **Ship vector:** native-build
- **Evidence:**
  - `ios/SignoVivo/DirectorSyncModule.swift:98` — currentBundleVersion = the APP's CFBundleVersion — a computed property that never changes after a mesh bundle install
  - `ios/SignoVivo/DirectorSyncModule.swift:722` — handleBundleOffer accepts when offered > Int(currentBundleVersion) — after installing the director's pack, 'mine' is still the old shell build, so the same offer re-triggers a full transfer
  - `ios/SignoVivo/DirectorSyncModule.swift:937` — the received pack's headerVersion is parsed (and emitted in bundleUpdated at :1046) but persisted NOWHERE
  - `ios/SignoVivo/DirectorSyncModule.swift:1743` — bundle_offer is sent on every peer connect — so every reconnect (backgrounding, mesh churn, M-F1 watchdog kicks) restarts a ~30MB transfer + WebView remount
- **User impact:** During any mixed-build window (the normal state for days after each TestFlight rollout), every behind-by-one follower iPad repeatedly saturates the mesh with 30MB transfers (starving page turns) and hard-remounts its WebView mid-Mass — the held M-F3 disaster upgraded from once to once-per-reconnect.
- **Recommendation:** Persist the installed pack's headerVersion (UserDefaults) at the atomic-swap success point; compare against max(CFBundleVersion, installedVersion) in handleBundleOffer; clear the key when CFBundleVersion exceeds it (which also gives the known stale-Documents finding its cleanup hook). Add a nearby-sync-contract source pin. Device-gated — ship in the M7 native batch.
- **Major-update intersection:** Belongs in M7's mesh-bundle work (sha256+signature) and is mooted if the P-MESH/P-OTA decision retires mesh push; until that decision it is the sharpest edge of the mesh-OTA seam.
- **Verifier notes:** All cited evidence holds at HEAD: DirectorSyncModule.swift:97-99 computes currentBundleVersion from CFBundleVersion only; :717-722 accepts any offer > the shell build (installed pack version persisted nowhere — zero UserDefaults usage in the module); :937/:1043-1047 parse and emit headerVersion without storing it; :1743 re-sends bundle_offer on every peer .connected. Verified beyond the finding: PdfReaderApp.tsx:954-962 remounts the WebView unconditionally on bundleUpdated (no version guard), so each re-transfer also blanks the follower mid-Mass. Not a duplicate: prior-art's native-swift-stale-documents-bundle-masks-update is the load-time masking direction and held M-F3 is the once-per-inst
- **Evidence corrections (verifier):** Citations accurate. Minor precision notes: currentBundleVersion spans :97-99; the bundleUpdated emit spans :1043-1047 (version field at :1046). Add supporting evidence: PdfReaderApp.tsx:954-962 — the bundleUpdated handler calls setMountKey((k)=>k+1) with no version comparison, confirming the unconditional remount half of the claim.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

### Surface: web

#### IANAV-02 — Entire browse/search IA is behind an invisible left-edge swipe; followers have no visible entry to the song list at all

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/styles.css:2331` — .drawer-handle { display:none !important } — comment says 'the ♪ is the song navigator', but ♪ is a numpad only
  - `web/src/app.js:2725` — Drawer opens only via edge swipe (start <44px, move >40px right); the same zone steals 'previous page' right-swipes (page turn requires startX>=44 at :2733)
  - `web/src/styles.css:2154` — .search-fab display:none for followers; ⌕ (the only visible drawer entry) is director-only via html[data-role=director]
  - `web/src/app.js:2799` — Window-level edge-swipe handler has no songJumpOpen check — opens the drawer hidden beneath the open ♪ modal
- **User impact:** Elderly followers can never discover buscar/misa/recientes/tiempo/temas/tono/todas: the only entry is an unmarked gesture that also collides with iOS Safari's back-swipe in a plain browser tab, and the help panel that would teach it is itself unreachable (IANAV-03).
- **Recommendation:** Give followers one visible entry: un-hide the already-wired drawer handle, OR show the ⌕ fab for all roles (activateTab('buscar') has no role checks), OR make ♪ offer 'Número / Lista'. Also gate the window-level edge handler on !state.songJumpOpen.
- **Merged from:** ROLEWEB-05, FOLWEB-08, PARITY-06
- **Verifier notes:** Every evidence claim holds at current HEAD. (1) web/src/styles.css:2331 has `.drawer-handle { display: none !important; }` with the comment "Native parity: no left side-bar. The ♪ (upper-right) is the song navigator" — but the ♪ modal (index.html:111-143, "IR A CANTO") is verifiably numpad-only: draft display + digit grid + Abrir/Cancelar, no list or browse mode (the old Teclado/Explorar switch is retired via `.drawer-mode-switch { display:none }` at styles.css:2328, and openDrawer at app.js:1114 hard-codes browse mode). (2) For followers the ONLY drawer entries are gestures: viewerShell edge-swipe at app.js:2725 (startX<44, deltaX>40) and the window-level handler at app.js:2806; the visible
- **Evidence corrections (verifier):** Evidence item 4: the guardless condition is at web/src/app.js:2806 (handler spans 2797-2810), not :2799. Evidence item 1 supporting cites: ♪ modal markup index.html:111-143; mode-switch retirement styles.css:2328; openDrawer forces browse mode app.js:1114-1116. Drawer-under-modal stacking: .overlay-controls z-index 4 (styles.css ~366) vs .song-jump-modal z-index 200 (styles.css ~2166). | Minor: the missing songJumpOpen guard in the window-level handler is the condition at app.js:2806 (the touchend handler cited at :2799 is where the listener begins). All other file:line citations are exact at current HEAD.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

### Surface: worker

#### SYNCE2E-01 — Fast-clock director: A2 seq sanitizer collapses every publish to seq=0, freezing web followers to one page turn per ~90s while mesh iPads follow normally

- **Lens:** synce2e
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/src/index.ts:147` — incomingSeq > Date.now()+60000 is collapsed to 0 (A2 sanitizer)
  - `sync-worker/src/index.ts:165` — fresh room + seq 0 → {ok:true, ignored:true}; ts only refreshed on ACCEPTED publishes, so the room stays 'fresh' on the first page ~90s
  - `sync-worker/src/index.ts:177` — stale-accept assigns snapshot.seq+1 and server ts — the once-per-90s escape valve
  - `src/directorRelaySync.js:58` — seq = Math.max(seqCounter+1, Date.now()) uses the DEVICE wall clock
  - `src/directorRelaySync.js:87` — client checks only res.ok; the ignored field is never read → HTTP 200 looks like success to the director
- **User impact:** A director whose device clock is >60s fast (manual clock / broken NTP) silently strands the whole signovivo.com congregation: phones advance one page per ~90s with a mostly-green pill while parish iPads (mesh) turn normally. No error on any surface for the entire Mass.
- **Recommendation:** In publish(), clamp an unreachably-high FINITE seq to Math.max(snapshot.seq+1, Date.now()) instead of zeroing it; keep zeroing only for NaN/Infinity/negative (poison guard intact). Worker-only, additive, no client change. Add a2.test.mjs case: seq=now+300s then now+301s → second page visible immediately.
- **Major-update intersection:** M4 (P2-IDENTITY) plans consuming {ignored:true} + transmitterId, which would surface/arbitrate this class; the worker-side clamp is independent and shippable now. The folded-in slow-clock handoff variant is durably fixed only by M4's transmitterId.
- **Merged from:** FAILUX-04
- **Verifier notes:** All citations verified at HEAD and the control flow behaves exactly as claimed. sync-worker/src/index.ts:147 collapses any seq > server Date.now()+60000 to 0; a director clock >60s fast trips this on every publish since directorRelaySync.js:58 derives seq from the device wall clock. Line 165 then returns {ok:true, ignored:true} for seq=0 while the room is fresh, and because ts (line 178) is written only on ACCEPTED publishes, freshness (lines 156-158, RELAY_LIVE_MAX_AGE_S=90 at line 34) persists ~90s after each acceptance — yielding exactly one accepted snapshot per ~90-102s (the stale-takeover branch at line 177). The director sees nothing: the HTTP handler (index.ts:812-814) returns 200 an
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-synce2e.md`

## MEDIUM (26)

### Surface: cross

#### ROLEWEB-02 — No web-director path exists: the native iPad is a single point of failure for the whole relay congregation, though the worker already authorizes code-bearing browsers

- **Lens:** roleweb
- **Ship vector:** multi
- **Evidence:**
  - `sync-worker/src/index.ts:777` — /publish authorizes via X-Director-Code header (the memorable codes the director already knows) from ANY client — a browser fetch can publish today with zero worker changes
  - `sync-worker/wrangler.jsonc:20` — ALLOWED_ORIGINS '*' — CORS does not block a browser publisher
  - `web/src/app.js:1183` — Web bundle contains zero publish code (grep 'publish' → comments only); a valid code entered on web is discarded with an error flash instead of granting anything
  - `docs/major-update-2026-07.md:147` — M4 plans transmitterId + two-publisher tiebreak — the safety prerequisite; no milestone anywhere plans a web-director UI
- **User impact:** If the director's iPad fails at Mass (battery, crash, forgotten), every relay follower — personal phones and the old web-PWA iPad — freezes on the last published page for the rest of Mass. There is no fallback surface a director with valid credentials can use.
- **Recommendation:** Design a deliberate emergency web-director mode AFTER M4's transmitterId lands: valid 10-digit code on web (via ROLEWEB-01's new sheet) → sets html[data-role=director] (badge + ⌕ CSS already exist), stops the follow loop, publishes with X-Director-Code + transmitterId, surfaces 401 via the already-bundled showRelayAuthWarning (app.js:887). Copy must state mesh-only native followers won't follow. This is a product fork — surface to Miguel before building.
- **Major-update intersection:** Depends on M4 (transmitterId + tiebreak; publish-rejection surfacing); natural companion to M6's admin surface; not itself planned anywhere — net-new milestone candidate.
- **Verifier notes:** All evidence verifies at HEAD: worker /publish accepts X-Director-Code from any client (sync-worker/src/index.ts:782-787) with the header CORS-allowlisted (index.ts:372) and ALLOWED_ORIGINS '*' (wrangler.jsonc:20); the web bundle has zero publish code and a valid long code in a pure browser flashes 'Código no válido' (web/src/app.js:1177-1191); no milestone in docs/major-update-2026-07.md (M0-M7, §9 decisions) plans a web-director UI, and map-prior-art.md has no covering item — not a duplicate. However the impact claim 'no fallback surface a director with valid credentials can use' is overstated: director codes are memorable and work on ANY native fleet iPad via the song-jump numpad, so the 
- **Evidence corrections (verifier):** web/src/app.js citation should be the long-code routing block at 1177-1191 (the 'Código no válido' flash is line 1189, not 1183). Worker auth block is sync-worker/src/index.ts:777-787 (codeOk at 785); note CORS also allowlists X-Director-Code at index.ts:372, strengthening the 'browser can publish today' claim. userImpact should drop 'There is no fallback surface a director with valid credentials can use' — any native fleet iPad accepts the director's memorable code today and recovers BOTH mesh and relay followers; the web-director mode is a resilience enhancement for the no-native-device-reachable case (relay-only), not the sole recovery path. | index.ts citation should be 782-786 (code extraction + codeOk check; :777 is the comment above). userImpact correction: 'no fallback surface a director with valid credentials can use' is false — any other parish iPad running the native app is a fallback today (always-follower boot + code-entry promotion); and followers do not freeze for the rest of Mass — staleness demotion (#248, live) releases them to manual navigation, so the loss is auto-follow, not page access.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-roleweb.md`

#### FOLNAT-04 — DELTA of new-director-dead-writes-laststate-role-and-page: a native follower app restart always boots to page 2 — the persisted last page (sv.book.lastPage.standard) is write-only end to end and the web has no page restore either

- **Lens:** folnat
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:3411` — initReader always sets state.currentPage = DEFAULT_START_PAGE (=2, :215) and renders it when no relay director (:3449); no persisted-page localStorage key exists anywhere in app.js
  - `PdfReaderApp.tsx:717` — Native persists sv.book.lastPage.standard on every page-changed but never reads it (offlineBooks.ts:23; no getItem of the prefix repo-wide)
  - `PdfReaderApp.tsx:116` — lastDirectorSnapshotRef is in-memory only — lost on app restart, so bridge-ready's follower resync (:679-695) has nothing to re-assert on first boot
- **User impact:** A follower iPad that crashes/relaunches mid-Mass comes back on page 2 (cover region), not where the congregation is, until the mesh reconnects and a heartbeat lands (seconds to tens of seconds; forever if no director is live, e.g. at practice) — disorienting for elderly users. The known finding records only the dead write as LOW code debt; this is the unlisted user-facing symptom.
- **Recommendation:** Web-side restore gated to NATIVE_FILE_MODE: persist the committed page to localStorage on every render and boot from it instead of DEFAULT_START_PAGE; a mesh snapshot still wins via the existing H1/bridge-ready path. Then delete the dead native write, finishing the known cleanup.
- **Major-update intersection:** Adjacent to M7's A3 fold-in ('broadcast the persisted last page') which is director-side only; this covers the follower half. Web-only reaches iPads at the next native build.
- **Merged from:** W2N-09
- **Verifier notes:** All evidence holds at HEAD: web initReader hard-sets DEFAULT_START_PAGE=2 (app.js:215,3411) and in NATIVE_FILE_MODE skips the relay boot-poll so it always renders page 2 (:3446-3449); no persisted-page localStorage key exists in app.js (all localStorage uses are prefs/tips/recientes/offline/fleet). The sole sv.book.lastPage.* usage repo-wide is the AsyncStorage.setItem in page-changed (PdfReaderApp.tsx:717-720, prefix src/offlineBooks.ts:23) — no getItem anywhere, so the write is dead end-to-end. lastDirectorSnapshotRef (PdfReaderApp.tsx:116) is a useRef, and the bridge-ready resync path explicitly null-guards it for fresh boots, so a restarted follower has nothing to re-assert until mesh di
- **Evidence corrections (verifier):** Minor: the AsyncStorage.setItem persist call is at PdfReaderApp.tsx:717-720 (template-string key at :718); cited :717 is the call start — effectively correct. All other citations accurate at HEAD.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/lens-folnat.md`

#### PARITY-05 — role:'none' collapses to FOLLOWER UI with a dead ⟳ — a transmitter that exits director mode looks like a synced follower but follows nothing (native comment 'no phantom follower UI' is false)

- **Lens:** parity
- **Ship vector:** multi
- **Evidence:**
  - `PdfReaderApp.tsx:768` — Transmitter exit-director path claims 'the web shows no phantom follower UI' then injects role:'none'
  - `web/src/app.js:951` — Web maps 'none'→'off', then renderDirectorModeBadge (:846-849) sets data-role to 'follower' for anything non-director — no 'off' presentation exists
  - `web/src/styles.css:2154` — Base CSS shows the ⟳ resync fab for all non-director layouts — the 'off' device displays full follower chrome
  - `PdfReaderApp.tsx:733` — resync handler: roleRef 'off' + syncAvailable false skips the becomeFollower rescue and all mesh calls; lastDirectorSnapshotRef null → ⟳ tap does nothing (spins 1.1s as false feedback)
- **User impact:** After Miguel's transmitter phone/iPad exits director mode it renders as a normal follower (⟳ + ♪) but has NO follower transport in the shell — the page silently freezes and ⟳ visibly 'works' while doing nothing. Handed to a congregant, it never follows.
- **Recommendation:** Give 'off' a real presentation: data-role='off' when a native bridge exists and role is off, CSS hides ⟳ (or shows a 'solo lectura' chip) — preserving the mesh-device ⟳ rescue path by keying on a new additive bridged field (e.g. canFollow:false on the transmitter exit path). Fix the stale native comment.
- **Major-update intersection:** M4's tri-state status pill would surface the frozen state too — coordinate the two on the same surface.
- **Merged from:** W2N-08
- **Verifier notes:** All evidence holds at HEAD. PdfReaderApp.tsx:754-774 (transmitter-only exit-director) injects role:'none' with a comment claiming no phantom follower UI; web/src/app.js:947-953 maps 'none'→nativeSyncRole 'off' and renderDirectorModeBadge (:846-849) sets data-role='follower' for anything non-director — no 'off' presentation exists, so the device renders full follower chrome (⟳ visible per styles.css:2149-2156, hidden only for director). The ⟳ is provably dead on this device: the web tap handler (app.js:2416-2418, 3088-3095) spins 1.1s unconditionally and posts {type:'resync'}; the native handler (PdfReaderApp.tsx:727-752) skips becomeFollower and all mesh calls when syncAvailable is false, an
- **Evidence corrections (verifier):** styles.css citation: the exact ⟳ rule is line 2156 (`html[data-role="director"] .resync-fab { display: none; }` — i.e., visible by default for all non-director layouts); cited line 2154 is the adjacent .search-fab rule in the same role-based-controls block (2149-2156). All other file:line citations are accurate at HEAD.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-parity.md`

#### N2W-04 — render-failed sentinel + 1s mesh heartbeat form an unthrottled ~1s failure loop that slams the drawer shut every second and blocks all navigation (deterministic-failure case, e.g. corrupt peer-pushed bundle)

- **Lens:** n2w
- **Ship vector:** multi
- **Evidence:**
  - `web/src/app.js:1086` — renderPage catch: setLoading overlay + closeDrawer() + posts render-failed — fires on EVERY failed attempt with no per-page de-dupe or backoff
  - `PdfReaderApp.tsx:792` — render-failed sets currentPageRef=-1 sentinel so the heartbeat re-drives (correct for transient failures, never converges for a missing file:// asset)
  - `PdfReaderApp.tsx:393` — 1s mesh heartbeat re-sends the director page; dedupe at :903 fails vs -1 → re-inject → re-fail → repeat every ~1s forever
  - `PdfReaderApp.tsx:709` — Escape impossible: user navigates to working page X → currentPageRef=X → next heartbeat (P≠X) re-injects the failing page → overlay + closeDrawer again
- **User impact:** A single bad/missing page asset (the known corrupt peer-pushed WebBundle class) turns a follower iPad into a strobing error screen that actively fights the user's attempts to browse away — every second the drawer is forced closed and the error overlay re-renders, for as long as the director stays on that page.
- **Recommendation:** Web: don't closeDrawer when the failed render was native-pushed (pushToHistory:false / explicit source flag), and after N=3 failures of the SAME page stop re-posting render-failed so the sentinel stops re-driving a deterministic failure (keep the overlay with a clearer hint). Native belt: cap sentinel re-drives per page until the incoming page changes (track lastRenderFailedPage/count at PdfReaderApp.tsx:777-795). Root cause disappears if peer-bundle push is retired (Q5) or signed/validated per-file (M7).
- **Major-update intersection:** M7 mesh-bundle signing + Q5 retire decision eliminate the realistic trigger; the web-side loop-taming is independent and safe to ship first. Related known: 'corrupt peer-bundle poisons every boot' (A.3 noted-unfixed) — this is the live-UX consequence loop, not the boot poisoning.
- **Verifier notes:** All four evidence legs hold at HEAD: app.js:1086 catch fires setLoading+closeDrawer+render-failed on every attempt with no per-page de-dupe/backoff; PdfReaderApp.tsx:792-794 resets currentPageRef to -1 for followers; the mesh heartbeat is 1000ms (PdfReaderApp.tsx:393-400, code comments saying '2s' are stale) and the :903 de-dupe fails vs -1, so a deterministic per-page failure (missing file:// asset in a corrupt peer-pushed WebBundle — the cache-buster retry at app.js:1043-1046 also fails; only the timeout path resolves, an error event rejects) loops at ~1s forever. Escape is blocked as claimed: page-changed sets currentPageRef=X (:711), the next heartbeat P≠X re-injects the failing page and
- **Evidence corrections (verifier):** Minor: PdfReaderApp.tsx:393 is the setInterval line and the interval is 1000ms — the in-code comments (PdfReaderApp.tsx:372, :780 'The 2s mesh heartbeat') are stale, not the finding. Sentinel assignment is at :793 (gate at :792); escape-path ref-set is at :711 (clamp at :709). All cited files/lines otherwise accurate at HEAD.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-n2w.md`

#### FAILUX-09 — The 401 relay-auth banner names the problem but not the action, and never re-shows after dismissal while the failure persists

- **Lens:** failux
- **Ship vector:** multi
- **Evidence:**
  - `web/src/app.js:912` — Copy states followers are NOT synced but gives no recovery action (exit director + re-enter a current code); × dismisses permanently (:918)
  - `src/directorRelaySync.js:89` — authErrorNotified latch re-arms only on a successful publish or fresh code entry — with a retired code neither happens, so one dismissal = signal-free outage for the rest of Mass
- **User impact:** Director with a retired code sees the banner once, dismisses it mid-panic, and then has zero remaining signal while every web follower stays frozen. Even undismissed, the banner never says WHAT to do.
- **Recommendation:** Copy (web, instant): append 'Sal del modo director (toca la insignia DIRECTOR) y vuelve a entrar con un código vigente.' Persistence: re-reveal 60s after dismissal while the failure persists (web timer), or native re-fires the handler every Nth consecutive 401 instead of a pure latch.
- **Major-update intersection:** M4 status pill should absorb the persistent-state half (red director pill = durable signal; banner = interrupt).
- **Merged from:** DIRNAT-08
- **Verifier notes:** Evidence holds exactly as cited at current HEAD. (1) web/src/app.js:912 — banner copy states only the problem ("El relé rechazó el código de director. Los seguidores en signovivo.com NO están sincronizados.") with no recovery instruction; the × handler at :918 just removes the is-on class, and re-reveal requires showRelayAuthWarning to fire again (app.js:888-891, triggered only via the 'relay-auth-error' payload at app.js:963). (2) src/directorRelaySync.js:89-90 — authErrorNotified latches true on the first 401/403 and re-arms ONLY on a successful publish (:88, impossible with a retired code) or setRelayPublishCode (:40, requires the director to re-enter a code — which the banner never tells
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-failux.md`

#### RELVER-04 — DELTA of offline-pwa-fleet-webcached-false-green: web followers have NO version floor and no legible version anywhere on the dashboard

- **Lens:** relver
- **Ship vector:** multi
- **Evidence:**
  - `web/src/app.js:2969` — web check-in sends opaque cacheVersion but NOT the human BUILD_NUMBER (baked at :220) — no human-orderable version ever leaves a web device
  - `sync-worker/src/index.ts:279` — cacheVersion is stored but never rendered or compared — only 2 references in the whole file (type + store)
  - `sync-worker/src/index.ts:467` — webReady = webCached && homeScreen with no version dimension — no analogue of MIN_SYNC_BUILD exists for web
- **User impact:** The one parish device that MUST run web (the old iPad PWA, too old for TestFlight) can sit on a months-old cached shell — e.g. one predating the #248 dead-director-freeze fix — and the dashboard says 'Listo — web en caché'; the operator has no way to know what any web device runs.
- **Recommendation:** Add buildNumber to the web check-in payload (additive), render it in the Web column ('✓ inicio · v381'), and add a MIN_WEB_BUILD floor that demotes stale-but-online shells to 'Recargar signovivo.com'. Known finding covers page-count inflation; this delta is the missing version dimension.
- **Major-update intersection:** M6 plans a fleet bookVersion column — add the web shell build column in the same dashboard rework so version truth lands once.
- **Verifier notes:** Evidence holds at HEAD. (1) web/src/app.js fleetCheckin payload (~2960-2970) sends cacheVersion (an opaque hash) but never BUILD_NUMBER; the only buildNumber emission at app.js:3562 is the ?selftest debug card, not the check-in. (2) sync-worker/src/index.ts has exactly two cacheVersion references (type at :74, store at :279) — it is never rendered on the dashboard nor compared. (3) The dashboard grants 'Listo — web en caché' from webReady = webCached && homeScreen (defined index.ts:452, used :467) with no version dimension; MIN_SYNC_BUILD (:403) applies only to nativeBuild. Not a duplicate: undefined/map-prior-art.md's offline-pwa-fleet-webcached-false-green [H, PARTIAL] covers the sticky OF
- **Evidence corrections (verifier):** index.ts:467 is where webReady grants the 'Listo — web en caché' state; the webReady = webCached && homeScreen definition is at index.ts:452. app.js cacheVersion-in-payload is at ~:2968 (payload block 2951-2970); BUILD_NUMBER baked at :220 is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

#### RELVER-05 — DELTA of native-swift-stale-documents-bundle-masks-update: no surface exposes WHICH web bundle a native device runs — and badge vs crash telemetry use OPPOSITE version precedence

- **Lens:** relver
- **Ship vector:** multi
- **Evidence:**
  - `web/src/app.js:3480` — badge resolvedBuild prefers native-injected SHELL version over the web bundle's own BUILD_NUMBER — the loaded web bundle's identity is shadowed on every native device
  - `web/src/app.js:2903` — crash telemetry uses the OPPOSITE precedence (web BUILD_NUMBER first, native fallback) — badge and dashboard crash-panel Build column disagree exactly when a stale bundle is live, undocumented
  - `PdfReaderApp.tsx:191` — native fleet check-in sends only nativeBuild (shell) — no web-bundle field exists on the fleet wire
  - `docs/pre-mass-checklist.md:49` — §B tells the operator to compare devices by the bottom-right build label — the number that cannot reveal the stale-web-bundle failure mode it exists to catch
- **User impact:** A pre-Mass human comparing badges cannot detect the known stale-bundle failure (old web code under a new badge); worse, when a crash happens the dashboard's Build column and the device badge legitimately disagree, making the operator distrust the tooling mid-incident. Current HEAD skew (web 381 spinner changes vs native-377 bundled copies) is invisible except by memorized build→behavior mapping.
- **Recommendation:** Web: render the badge as shell·web (e.g. '381·w379') whenever the injected native version differs from BUILD_NUMBER, and include webBuild in the bridge-ready payload; native (next build): forward webBuild in fleet check-in and render both on the dashboard; document the precedence rule for crash rows.
- **Major-update intersection:** Defines the exact version grammar M7's DIAGNÓSTICO screen and M4's status pill must display (major-update-2026-07.md:448 already promises device-visible version truth); M5/M6 add bookVersion as a third number — design once.
- **Merged from:** PARITY-09
- **Verifier notes:** All four citations verified at HEAD: badge (app.js:3479-3481) prefers native-injected shell version (PdfReaderApp.tsx:1037 injects BUILD_VERSION) over web BUILD_NUMBER, while crash telemetry (app.js:2900-2903) uses the opposite precedence (BUILD_NUMBER first, native fallback) — so badge and dashboard crash-row build genuinely disagree exactly when a stale web bundle is live. Fleet check-in (PdfReaderApp.tsx:186-197) sends only nativeBuild with no web-bundle field, and docs/pre-mass-checklist.md:48-50 directs the operator to compare the bottom-right badge — the one number that cannot reveal the stale-bundle failure. The parent root-cause finding (native-swift-stale-documents-bundle-masks-upda
- **Evidence corrections (verifier):** Minor anchor precision only: badge precedence block is app.js:3479-3481 (3480 cited is within it); crash precedence is app.js:2900-2903 (2903 is the fallback line); fleet check-in nativeBuild is PdfReaderApp.tsx:191 within the 186-197 fetch body; checklist version bullet is docs/pre-mass-checklist.md:48-50. All citations substantively correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

#### RELVER-09 — #270 (build 378 web) quietly killed roster matching for ALL web devices — the web-only follower reads 'No se ha visto — invitar' while its device sits cached in the orphan table

- **Lens:** relver
- **Ship vector:** worker-only
- **Evidence:**
  - `web/src/app.js:2962` — post-#270 the web check-in payload carries no label field at all and no code path can ever set one again
  - `sync-worker/src/index.ts:445` — dashboard maps devices to roster people by normalized-label equality — a label-less device can never match anyone
  - `sync-worker/src/index.ts:266` — coalescing (o.label ?? prev.label) keeps OLD labels alive until /fleet/reset ('for a fresh season') — after which every web device is permanently an anonymous '(sin nombre)' orphan
  - `sync-worker/src/index.ts:460` — an unmatched roster person renders red 'No se ha visto — invitar'; the per-person webReady branch (:452,:467) is dead code for any post-reset fleet
- **User impact:** At the pre-Mass glance the dashboard contradicts itself: the old-iPad PWA person shows permanently red 'never seen, invite them' while the very same iPad appears rows below as an anonymous orphan reading '✓ inicio' — the operator can't trust per-person readiness for anyone on web.
- **Recommendation:** Worker-only render fix: stop implying roster coverage for surface=web — render web devices as first-class deviceId-keyed rows (with ago/cached/build per RELVER-04) and drop the 'invitar' claim when anonymous web check-ins exist; or (heavier) let the roster pin an optional deviceId per person, seeded from the orphan table.
- **Major-update intersection:** M6's fleet dashboard rework should adopt the deviceId-first model; decide jointly with RELVER-10 whether labels are dead fleet-wide.
- **Verifier notes:** All cited behavior verified at HEAD d5075091: web check-ins carry no label and no code path can set one (app.js:2962-2971; #270 deleted the modal + FLEET_LABEL keys), dashboard matches devices to roster people only by normalized label equality (index.ts:443-446), label coalescing (index.ts:266) keeps old labels only until fleet reset/ring-cap eviction, and an unmatched roster person renders red 'No se ha visto — invitar' (index.ts:459-461) while the same device sits in the orphan table (index.ts:507). #270 touched only web/src files — the worker dashboard was never adjusted, so per-person web readiness (webReady branch :452/:467) is dead for all future web check-ins and the dashboard self-co
- **Evidence corrections (verifier):** All citations accurate; the 'invitar' string is at sync-worker/src/index.ts:461 (line 460 cited is the cls='bad' assignment immediately above — same block).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

#### RELVER-12 — Checklist canary steps A.3.3–A.3.5 are impossible on the staging canary — director promotion, mesh follow, and the restart test cannot run on web

- **Lens:** relver
- **Ship vector:** web-only
- **Evidence:**
  - `docs/pre-mass-checklist.md:27` — instructs 'Become director on the canary → a 2nd device follows the page over the mesh' on a staging WEB canary — the mesh is native-only and native cannot load staging content (deferred to M7)
  - `web/src/app.js:1189` — on web a 5+-digit code without a native bridge dead-ends at 'Código no válido' — director promotion exists only behind the native bridge
  - `docs/pre-mass-checklist.md:28` — the restart test presupposes the native director that step 3 cannot create; improvising with the native app publishes into the LIVE Mass room (native room hardcoded — known, not re-reported)
- **User impact:** An operator honestly following the safety ritual hits a dead end and either skips the director/mesh/restart checks (so the surfaces where every historical outage lived get validated by NOTHING before prod) or improvises with the native app against the live Mass room — a pre-Mass trap wearing a safety-procedure costume.
- **Recommendation:** Rewrite §A.3 into 'what a web canary proves' (boot, render, page turn, staging-room relay follow across two browser tabs, ⟳, ?selftest) and a separate 'native canary' subsection that states its checks require the new TestFlight build and run against the LIVE room at practice; fold back when M7 lands native staging entry.
- **Major-update intersection:** M7 delivers native staging entry + the 2-device day — the rewritten checklist is the operational contract those verifications should be run against.
- **Verifier notes:** All evidence verified at HEAD: pre-mass-checklist.md:27-30 instructs director promotion + mesh follow + a director-restart test on a canary "pointed at staging (signovivo.com?env=staging)", but the staging deploy (STAGING=1 release.sh, checklist step 2) is web-only — no native build exists at that stage. On web, a 5+-digit code without a native bridge dead-ends at flashSongDisplay("Código no válido") (web/src/app.js:1178-1190, exact line 1189), so web cannot become director; the mesh is native-only Swift Multipeer; and no publisher can exist for the staging room since the native app lacks staging entry (deferred to M7) and hardcodes RELAY_ROOM="alvernia-main" (src/directorRelaySync.js:13) — 
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

### Surface: native

#### N2W-03 — relay-auth-error is a lossy one-shot: a WebView crash-reload permanently destroys the only 'web congregation is dark' warning (latch never re-arms, bridge-ready never re-asserts, banner DOM wiped)

- **Lens:** n2w
- **Ship vector:** native-build
- **Evidence:**
  - `src/directorRelaySync.js:89` — authErrorNotified latches on first 401/403 and the handler fires ONCE; re-arm only on a later res.ok (:88) or new setRelayPublishCode (:36-41) — a persistently-bad code never warns again
  - `PdfReaderApp.tsx:638` — bridge-ready handler re-asserts role and page but has NO relay-auth re-assert — a reloaded WebView never re-learns the warning
  - `PdfReaderApp.tsx:1092` — onContentProcessDidTerminate clears pendingInjectRef — a relay-auth-error queued during the reload window (webReadyRef false) is dropped before delivery with the latch already spent
  - `web/src/app.js:887` — Banner is in-page DOM (relayAuthWarningEl), wiped by any reload and user-dismissible (:918) with no persistent state behind it
- **User impact:** The banner is by its own comment (app.js:881-885) the ONLY signal that every signovivo.com follower has gone dark on a rejected/rotated code. After any WebView crash-reload (common on old iPads under memory pressure) the director's device looks healthy while the web congregation stays frozen for the rest of Mass.
- **Recommendation:** Native shell tracks the latched auth-error state (set when the handler fires; cleared on new code entry and on a successful publish via a small onPublishOk hook or getAuthErrorState() in directorRelaySync). At bridge-ready, if the device is a broadcaster and the state is set, re-inject {type:'relay-auth-error', status}. Web side needs nothing — showRelayAuthWarning is already idempotent/re-revealing (app.js:888-892).
- **Major-update intersection:** M4's tri-state status pill is the permanent home for a persistent 'relay dark' state (vs a dismissible banner); M3's hello/welcome handshake is the natural carrier for re-asserted warning state.
- **Merged from:** W2N-07
- **Verifier notes:** All cited behavior verified at HEAD. The auth-error latch (directorRelaySync.js:34, set :90) lives in the RN runtime and survives WebView reloads; it re-arms only on res.ok (:88) or a new code entry (:36-41), so a persistently-bad code fires the handler exactly once. The bridge-ready handler (PdfReaderApp.tsx:638-696) re-asserts role/page/bridge-state but has no relay-auth re-assert; onContentProcessDidTerminate (PdfReaderApp.tsx:1089-1095) clears pendingInjectRef, dropping any in-flight auth-error inject. The banner (web/src/app.js:886-925) is reload-wiped, dismissible DOM with no backing state, and its own comment (:881-885) confirms it is the only web-congregation-dark signal. Not in map-
- **Evidence corrections (verifier):** Minor: the latch is set at directorRelaySync.js:90 (condition checked at :89 as cited); handler registration is PdfReaderApp.tsx:342-347. All other citations accurate at HEAD.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-n2w.md`

#### W2N-04 — becomeFollower failure wedge: role asserted to the web before the transport starts, and ⟳ cannot repair a follower whose mesh start failed

- **Lens:** w2n
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:423` — roleRef set to 'follower' before startNearbyFollower runs
  - `PdfReaderApp.tsx:449` — After both start attempts throw (swallowed at :445), role:'follower' is still injected — web shows healthy follower UI
  - `PdfReaderApp.tsx:733` — resync re-runs becomeFollower only when roleRef==='off' — this wedge has roleRef 'follower'
  - `ios/SignoVivo/DirectorSyncModule.swift:592` — refreshNearbyDiscovery guards currentRole != 'off' — Swift is 'off' because startFollower never completed, so the ⟳ fallback no-ops
- **User impact:** If the RN-bridge start call fails twice (module wedge / early-boot race — rare, but the retry-once design exists because it happens), the device is a permanent link-less 'follower' with normal-looking UI; ⟳ does nothing; only the secret soft-reset code or an app kill recovers it.
- **Recommendation:** Track transport truth in a meshStartedRef set only after startNearbyFollower resolves; make the resync handler re-run becomeFollower when roleRef==='follower' && !meshStartedRef. Optionally emit a searching-state sync-event on total failure so the web spinner is honest.
- **Major-update intersection:** M4's always-visible tri-state status pill would make this wedge visible; the resync-gate fix is independent and cheap.
- **Verifier notes:** All four citations verified at current HEAD: roleRef set to 'follower' at PdfReaderApp.tsx:423 before transport start; double startNearbyFollower failure swallowed at :445 with role:'follower' still injected at :449; the ⟳ resync handler at :733 only re-runs becomeFollower when roleRef==='off' (this wedge is 'follower'); and both fallback repairs no-op in Swift because currentRole stays 'off' — refreshNearbyDiscovery guards it at DirectorSyncModule.swift:592 and forceFollowerHelloNow guards currentRole=='follower' at :718. The AppState foreground handler (:994-1017) uses only those same two dead paths, so no code path repairs the wedge short of soft-reset or app kill. Not in map-prior-art.md
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-w2n.md`

#### SYNCE2E-06 — Step-down straggler publish drains with an EMPTY code, 401s by design (C3), and false-fires the red 'El relé rechazó el código de director' banner on a device that just correctly stopped directing

- **Lens:** synce2e
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:430` — becomeFollower → setRelayPublishCode('') (C3)
  - `src/directorRelaySync.js:40` — setRelayPublishCode ALSO re-arms the one-shot auth latch (authErrorNotified=false)
  - `src/directorRelaySync.js:103` — finally-drain sends the queued pending payload reading the now-empty code at fetch time (:77) → guaranteed 401 → :89-96 fires the auth handler
  - `web/src/app.js:912` — banner text claims the code was rejected and signovivo.com followers are NOT synced — both false after a legitimate demotion/exit
- **User impact:** A director demoted by a mesh conflict (or who voluntarily exited) with a page turn in flight gets a latched red mid-Mass banner saying their code was rejected and the congregation is out of sync — inviting a panicked code re-entry that would fight the winning director.
- **Recommendation:** In doPublish, drop the payload without fetching (and without warning) when relayPublishCode is empty — achieves C3's rejection intent one hop earlier; also guard the pending drain. Real 401s (retired code on a live director) still warn.
- **Major-update intersection:** M4's director-side status pill will surface publish health; this false-alarm suppression is independent and should land first so the pill's inputs are trustworthy.
- **Verifier notes:** Full chain verified at HEAD: becomeFollower (PdfReaderApp.tsx:430) clears the publish code via setRelayPublishCode(""), which ALSO re-arms the one-shot auth latch (directorRelaySync.js:36-41). The doPublish finally-drain (directorRelaySync.js:100-107) sends any pending payload reading relayPublishCode at fetch time (:77) — now empty — and the worker 401s an empty code by construction (sync-worker/src/index.ts:782-787, codeOk requires code.length>0). The 401 plus re-armed latch fires the handler (:89-96); the native bridge (PdfReaderApp.tsx:342-347) forwards relay-auth-error with NO role guard, and web/src/app.js:962→887 shows the red banner at :912 claiming the code was rejected and follower
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-synce2e.md`

### Surface: web

#### DIRNAT-04 — Exit-director confirm is window.confirm: iOS system alert with hardcoded ENGLISH 'Ok'/'Cancel' buttons and an empty title; copy 'volverás a seguidor' is also false on the transmitter-only path

- **Lens:** dirnat
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:2428` — Badge tap → window.confirm('¿Salir del modo director?…') — the only confirm-dialog in the app
  - `node_modules/react-native-webview/apple/RNCWebViewImpl.m:1225` — react-native-webview presents UIAlertController with title:@"" and hardcoded actionWithTitle:@"Ok" / @"Cancel" — English regardless of locale (verified in the main checkout's node_modules)
  - `PdfReaderApp.tsx:764` — Transmitter-only exit drops to standalone 'off' with no follower transport — the dialog's 'volverás a seguidor' promise is untrue for that class
- **User impact:** The most destructive tap on the director's screen (stepping down mid-Mass) is gated by two English words shown to elderly Spanish-speaking volunteers whose bar is zero-training. Meaning is inferable from the Spanish body text, hence medium.
- **Recommendation:** Replace window.confirm with a small in-page styled dialog (reuse the relay-warn overlay pattern) with Spanish buttons 'Seguir dirigiendo' (default) / 'Salir del modo director' (destructive), posting exit-director on confirm. Note: this surface only exists inside the native shell, so the web-only fix reaches directors at the next native build or mesh bundle push — not on Pages deploy alone.
- **Verifier notes:** All evidence holds at current HEAD. web/src/app.js:2428 uses raw window.confirm for the director-badge exit (native-only surface). react-native-webview's iOS confirm handler (RNCWebViewImpl.m, runJavaScriptConfirmPanelWithMessage) presents a UIAlertController with title:@"" and hardcoded English "Ok"/"Cancel" actions (no NSLocalizedString on the iOS branch — only macOS localizes). PdfReaderApp.tsx exit-director case (lines 754–775) confirms the transmitter-only path drops to role "off"/"none" with no follower transport, so the dialog's "volverás a seguidor" is false for that device class. No matching item in map-prior-art.md (only a generic future "i18n" plan-tail bullet). Medium severity is
- **Evidence corrections (verifier):** RNCWebViewImpl.m: the confirm panel is at lines 1222–1230 in the main checkout's node_modules ("Ok" at 1225, "Cancel" at 1228, empty title at 1224) — cited line 1225 is within the block, essentially accurate. PdfReaderApp.tsx: the transmitter-only branch spans 763–774 (case starts at 754); cited line 764 is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### DIRNAT-05 — The 44px left-edge drawer-swipe zone hijacks the director's 'previous page' swipe in portrait; an accidental drawer open followed by any browse tap broadcasts to the whole congregation

- **Lens:** dirnat
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:2725` — startX<44 + dx>40 → openDrawer wins; the page-turn branch at :2733 requires startX>=44 — a natural back-page thumb swipe from the left bezel opens the drawer instead
  - `web/src/app.js:2792` — Window-level duplicate edge-swipe handler widens the same trap to touches outside viewerShell
  - `web/src/styles.css:157` — The DIRECTOR badge sits at left≈9px inside the edge zone, enlarging the accidental surface
  - `PdfReaderApp.tsx:721` — Every drawer/browse render posts page-changed → broadcastPage → mesh + relay: a director's stray drawer tap yanks every device in the room
- **User impact:** Mid-Mass, the director flips back for a repeated refrain; a drawer slides over their music instead; a hurried dismissal tap on a song row jumps the entire congregation. There is no un-broadcast browsing for a director.
- **Recommendation:** For html[data-role="director"]: shrink the edge zone (44→24px) and/or raise the open-travel threshold (40→80px) so back-page swipes win; exclude touches starting on #director-mode-badge from both edge handlers. Extract the gesture decision (startX,dx,dy,role→action) into a pure helper and unit-test it. Same native-reach caveat as DIRNAT-04.
- **Verifier notes:** All four citations hold at HEAD. app.js:2725 gives the drawer priority for any touch starting <44px from the left edge with >40px rightward travel, and the page-turn branch at :2733 hard-requires startX>=44 — so a director's natural back-page swipe from the left bezel can never turn the page and always opens the drawer (drawer even wins at a LOWER travel threshold, 40px vs 48px). The window-level duplicate at :2789-2810 (clientX<44, dx>40) widens the trap. Neither handler consults html[data-role] (set at :849, never read in gesture code). The broadcast chain is real: drawer song-row tap → renderPage (app.js:2581) → page-changed post (app.js:1063) → PdfReaderApp.tsx:721 broadcastPage → mesh+r
- **Evidence corrections (verifier):** styles.css citation: the .director-mode-badge rule block begins at ~line 154 (left: max(0.55rem, ...) at :157) — cite the block, not just :157. Also note the badge sits at the TOP-left corner (top ≈9px, height 4rem), so it only enlarges the accidental surface for swipes starting in that top strip — a minor contributor, not the main trap.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### ROLEWEB-01 — A VALID director code typed on signovivo.com flashes "Código no válido" — misleading dead end in the forgot-iPad emergency

- **Lens:** roleweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:1178` — goToDraftSong: any 5+-digit entry with no native bridge falls to flashSongDisplay("Código no válido","err") — the code is never checked against anything; a real director code and garbage get the same 'invalid credential' verdict
  - `web/src/app.js:831` — flashSongDisplay: 1.6s red flash on the numpad display, then auto-revert — no explanation, no pointer to the iPad app
  - `web/src/app.js:827` — Comment admits the truth ("nothing to unlock on web") but the user-facing string claims the CODE is invalid, not the SURFACE
- **User impact:** The only realistic reason someone types a director code into the website is the emergency: the parish iPad is dead/lost and the volunteer director tries their code on a phone browser. The app tells them their (correct) code is wrong — they retype, conclude they misremember it, and give up minutes before Mass. Nothing on web hints that directing requires the iPad app.
- **Recommendation:** For 10-digit (code-shaped) entries on pure web, replace the flash with honest actionable Spanish copy in a small dismissible toast/sheet: 'Este código se usa en el iPad de la parroquia — el sitio web solo puede seguir al director.' Keep the short 'no válido' flash for 5-9 digit non-code-shaped entries. Pure client change; native takes the bridge branch and is unaffected.
- **Major-update intersection:** Standalone copy fix, ship now; if a web-director mode ever ships (see ROLEWEB-02 / M4), this branch becomes its entry point.
- **Merged from:** PARITY-03, IANAV-06
- **Verifier notes:** Verified at current HEAD: goToDraftSong (web/src/app.js:1172-1192) routes any 5+-digit entry with no native bridge to flashSongDisplay("Código no válido","err") at :1189 with zero validation — a correct director code and garbage get the same 'invalid' flash (1.6s auto-revert, :831-841), and the comment at :1181-1182 admits the limitation is the surface, not the code. Not in prior art (closest item, web-reader-draft-cap-blocks-legacy-11-digit-code, is a different cap/leading-zero issue). However, severity 'high' is inflated: web cannot direct regardless (only native publishes via src/directorRelaySync.js), so honest copy changes nothing functionally — the harm is confusion/wasted retries in a
- **Evidence corrections (verifier):** web/src/app.js:1178 is the 5+-digit length gate; the misleading flash call is at :1189 (web branch :1187-1190). flashSongDisplay is at :831 and the admitting comment at :827-828 as cited. | First evidence item cites web/src/app.js:1178 for the fall-through; line 1178 is the 5+-digit length gate — the pure-web branch and flashSongDisplay call are at lines 1187-1190 (flash at 1189). Other citations (831, 827) are accurate.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-roleweb.md`

#### FOLWEB-02 — No wake-lock fallback and no Auto-Lock guidance on the exact device class the parish uses (pre-iOS-16.4 web-PWA follower) — residual of FIXED #241

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:3502` — initScreenWakeLock silently no-ops when navigator.wakeLock is absent (all iOS <16.4 — the old-iPad PWA follower); no fallback, no message, no flag
  - `web/src/app.js:3509` — even where supported, acquire() swallows rejections (e.g. iOS Low Power Mode) with zero surface
  - `web/src/app.js:2960` — fleetCheckin payload has no wakeLock capability field — operator dashboard cannot flag the at-risk device
- **User impact:** The old iPad (the one device that must use the web PWA) sleeps at its Auto-Lock interval mid-Mass and goes dark; the 'set Auto-Lock Never' requirement exists only as oral tradition, surfaced nowhere in the app.
- **Recommendation:** Where :3502 returns, on iOS standalone show a one-time dismissible banner: 'Ajustes → Pantalla y brillo → Bloqueo automático → Nunca'; optionally add a NoSleep-style muted-video fallback while following; add wakeLock:boolean to fleet check-in.
- **Verifier notes:** All three evidence points hold at HEAD: app.js:3502 silently returns when navigator.wakeLock is absent (all iOS <16.4, i.e. the parish's mandatory web-PWA old iPad); app.js:3509 swallows acquire() rejections with no surfacing; the fleetCheckin payload (app.js:2962-2971) has no wake-lock capability field. Not a duplicate: map-prior-art.md:252 marks the wake-lock item FIXED-#241 with no residual caveat, and neither the hardening plan (P7-WAKELOCK, closed) nor docs/pre-mass-checklist.md documents the pre-16.4 gap or any Auto-Lock-Never guidance — that protocol lives only in the operator's session memory, exactly as the finding claims. Impact is real (the one device that cannot run the native ap
- **Evidence corrections (verifier):** Fleet check-in citation: the payload object is constructed at web/src/app.js:2962-2971 (the cited :2960 is the fleetCheckin declaration two lines above — same function, minor offset). Wake-lock citations :3502 and :3509 are exact. | Third evidence line: the fleetCheckin payload literal is at web/src/app.js:2962-2971 (function starts at 2951); line 2960 is the totalPages computation. Substance of the claim (no wakeLock field) is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### FOLWEB-03 — Add-to-Home-Screen is guided nowhere in-app — the PWA install protocol is oral tradition

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:203` — isStandaloneApp is computed but used only for fleet homeScreen reporting (:2968) and the iOS pseudo-fullscreen gate — never to offer install; no beforeinstallprompt handler, no install strings anywhere in web/src
  - `web/src/index.html:16` — og:/twitter: share cards exist for the WhatsApp-link arrival flow, but arrivals stay in a Safari tab with no prompt to install
  - `web/src/manifest.webmanifest:6` — manifest is fully install-ready (standalone, maskable icon) — the capability exists, only the guidance is missing
- **User impact:** Congregants arriving via shared link never learn the app installs and works offline; provisioning a replacement old-iPad follower depends entirely on Miguel remembering the Safari→Share→Add-to-Home-Screen→Auto-Lock ritual.
- **Recommendation:** One-time dismissible install card: iOS Safari non-standalone gets Share→'Agregar a pantalla de inicio' instructions; Android captures beforeinstallprompt and offers a real install button. Suppress while a director is live, in native mode, and after dismissal.
- **Verifier notes:** All evidence verified at HEAD: isStandaloneApp (web/src/app.js:203-205) is used only at :213 (iOS pseudo-fullscreen gate) and :2968 (fleet homeScreen report); no beforeinstallprompt handler or install-guidance string exists anywhere in web/src; index.html has og share cards but no install prompt; manifest.webmanifest is fully install-ready (standalone, 192/512 icons). Not in map-prior-art.md (offline-pwa items cover SW lifecycle/cache issues, not install guidance). Impact is real, not speculative: the Add-to-Home-Screen provisioning ritual for the old-iPad follower exists only as oral tradition/session memory, and WhatsApp-link arrivals stay in a Safari tab with no path to the offline-capabl
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### FOLWEB-05 — All in-app help is unreachable on web — the only opener is a display:none stub — and the help content describes retired UI

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/index.html:210` — #help-button, the sole opener of #help-panel, is a hidden compat stub (display:none, aria-hidden)
  - `web/src/app.js:2662` — click wiring for help open/close + haptic toggle exists and works — it just cannot be reached
  - `web/src/index.html:300` — help text says 'toca la franja oscura a la izquierda' — that handle is display:none !important (styles.css:2331); :314 describes the retired drawer numpad
- **User impact:** A confused follower has zero self-service path: swipes, the hidden drawer gesture, the live dot/bar, offline behavior — none of it is explained anywhere reachable; the haptics setting and version label are also stranded.
- **Recommendation:** Either resurrect (recommended): add a visible '?' entry (jump modal or drawer top bar) and rewrite the stale items — one panel can also host FOLWEB-03/06's guidance; or fully retire the panel so dead UI stops accreting. Not covered by P8's dead-UI list.
- **Merged from:** IANAV-03, PARITY-08, VESTIG-10
- **Verifier notes:** All evidence holds at HEAD: #help-button (index.html:210) is a display:none/aria-hidden/tabindex=-1 compat stub in the hidden numpad-stub block, and app.js's helpButton click handler (app.js:2662-2665) is the ONLY code that removes is-hidden from #help-panel (grep confirms no other opener), so the help panel, its haptic toggle (index.html:341), and #app-version-label (index.html:347) are unreachable. The panel's content is also stale: 'toca la franja oscura a la izquierda' (index.html:300) points at .drawer-handle which is display:none !important (styles.css:2331), and the numpad item describes the retired drawer numpad. Not in undefined/map-prior-art.md — the P8 dead-UI list covers the offl
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### FOLWEB-07 — Numpad jump silently lands on the LAST page for unknown song numbers — and for ALL numbers while the song index hasn't hydrated (incl. permanent if the one background fetch fails)

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:708` — resolveSongPage falls through to state.totalPages (page 371) when no song ≥ N exists — silent teleport to the back of the book
  - `web/src/app.js:1200` — the mis-jump also sets relay.browsing=true, dropping a live follower out of auto-follow as a bonus surprise
  - `web/src/app.js:3455` — songIndex hydrates from ONE background fetch with no response.ok check and no retry (.catch(console.warn)) — songIndex starts [] (:149), so pre-hydration or post-failure EVERY entry resolves to the last page
- **User impact:** Typing a wrong number — or a valid number too early on a slow first visit — teleports the user to page 371 with no error and pauses their live follow; one transient pages.json failure breaks the primary navigation control for the whole session.
- **Recommendation:** Never fall through to totalPages: flash 'No existe el canto N' (keep first-song-≥N for in-range gaps); if songIndex is empty flash 'Cargando índice…' and re-kick the hydrate; harden the hydrate with response.ok + retries + online/visibilitychange re-attempts.
- **Merged from:** FAILUX-08, IANAV-04
- **Verifier notes:** All three evidence points hold at current HEAD (d5075091, build 381). resolveSongPage (web/src/app.js:700-708) returns state.totalPages when no song >= N exists or when songIndex is empty — silent jump to page 371 with no error. goToDraftSong (app.js:1193-1205) then sets relay.browsing=true/following=false since the mis-resolved page differs from the live page, pausing auto-follow. On the public web build, build.mjs (line 699) inlines only { totalPages }, so songIndex ALWAYS hydrates via the single background fetch at app.js:3455-3458, which has no response.ok check and no retry (.catch(console.warn)); songIndex starts [] (app.js:149) and the numpad is reachable with no empty-index guard (op
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### SYNCE2E-05 — Forced /state polls bypass the seq guard entirely and can REWIND a follower onto an older snapshot for up to ~12s under a green pill

- **Lens:** synce2e
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/lib/svSyncDecision.js:111` — `if (!ctx.force && snap.seq <= ctx.lastSeq)` — force bypasses even STRICTLY OLDER seqs and renders the stale page (:117,:130)
  - `web/src/app.js:3260` — WS-open force-resync races in-flight WS pushes; also :3213 4s forced fallback ticks, :3292 F1 re-home, :3235 poll-through-connect
  - `web/src/app.js:3152` — stale forced apply overwrites relay.livePage with the OLD page, so the F1 drift re-home (:3291) sees no drift and cannot correct it
- **User impact:** On a slow network (when forced polls are most frequent), a follower can visibly flip BACK one page right after the director advances and sit there up to ~12s (until the relay heartbeat's new seq) with the 'en vivo' pill green.
- **Recommendation:** In decideRelaySnapshot, force should re-apply EQUAL seqs (re-home purpose) but never strictly older: treat `force && snap.seq < ctx.lastSeq` as live-dup. Mirror the guard in the inline fallback (app.js:3124). Add cases to e2e/svSyncDecision.test.mjs (force+older → live-dup; force+equal → follow).
- **Major-update intersection:** M4 formalizes syncDecision testing against the local relay harness — add the force-rewind case to that suite.
- **Verifier notes:** All three evidence citations verified at HEAD: svSyncDecision.js:111 lets force bypass even strictly-older seqs and the follow branch (:116-117,:130) renders the stale page; app.js:3152 overwrites relay.livePage with the old page so the F1 re-home (:3291) sees no drift and cannot correct; all /state paths (WS-open :3260, 4s fallback :3213, poll-through-connect :3235, F1 :3292) are forced, and a /state fetch has up to a 6s abort window during which a newer WS push can land first, making the rewind race real. WS ping replies are force=false and same-seq (live-dup, no render), so the only recovery is a new seq: the native director's 12s relay heartbeat (PdfReaderApp.tsx:401-412) publishes a fre
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-synce2e.md`

#### FAILUX-06 — After a failed/timed-out page image, nothing can repaint the current page — ⟳, 'Volver a en vivo', and force-resyncs all skip same-page renders (delta on P7-TIMEOUT-COMMIT/P7-IMG-RETRY)

- **Lens:** failux
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:1011` — 3s preload timeout RESOLVES as success → blank/stale src committed, currentPage updated, page-changed posted (known half)
  - `web/src/app.js:73` — <img> error retry capped at 4 — all consumed during the outage; nothing ever retries again
  - `web/src/lib/svSyncDecision.js:130` — renderPage = currentPage !== snap.page ? page : undefined EVEN under force — ⟳/goLive/F1/poll all refuse to re-render the current (broken) page after network returns
- **User impact:** Weak-wifi blip while turning to a hymn: follower shows a blank/stale image under a green pill; when the network returns, tapping ⟳ or the amber dot fixes the socket but never the pixels. The page only repairs when the director next MOVES — potentially a whole hymn. Workaround (swipe away and back) is real but undiscoverable for this congregation.
- **Recommendation:** Add an image-health check to the force paths: in goLive() and applyRelaySnapshot when force && currentPage===snap.page, re-render if !pageImage.complete || naturalWidth===0, routed through renderPage's requestId guard. Optionally re-arm the <img> retry budget on window 'online'.
- **Verifier notes:** All three citations verified at HEAD: app.js:1011 timeout-resolves-as-success commits a blank/stale src and updates currentPage (known P7-TIMEOUT-COMMIT); app.js:73 caps <img> error retries at 4 per session with no reset (on 'online' or otherwise); svSyncDecision.js:130 sets renderPage=undefined when currentPage===snap.page regardless of force (force only bypasses seq de-dup at line 111). Every recovery affordance hits a same-page skip: goLive() at app.js:3079, the executor guard at app.js:3162, the no-lib inline fallback at app.js:3129, and ⟳/reconnectRelay + poll/WS-reopen all funnel through the decision lib. Meanwhile renderPage's own health check (app.js:1038, complete && naturalWidth>0)
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-failux.md`

#### FAILUX-07 — ⟳ resync gives identical fake-success feedback whether it worked or the network is dead

- **Lens:** failux
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:2416` — Spin is cosmetic: is-spinning added before reconnectRelay() and removed on a fixed 1100ms timer, independent of any outcome
  - `web/src/app.js:3088` — reconnectRelay never learns or reports whether the reconnect/poll succeeded; poll failures are swallowed (:3209); native resync path equally outcome-blind
- **User impact:** During an outage the follower's single recovery affordance animates convincingly, changes nothing, and reports nothing — training elderly users that the button does nothing exactly when trust in it matters most.
- **Recommendation:** Make the spin outcome-driven: keep spinning until a snapshot is successfully applied (stop, brief success state) or 4s elapse (stop + small toast 'No hay conexión — revisa el wifi'). Native path keys off the next sync-event within the window.
- **Major-update intersection:** M4 status-honesty family, but independently shippable.
- **Merged from:** FOLWEB-10
- **Verifier notes:** Verified at current HEAD d5075091 (build 381, not the 377 the finding was written against — code unchanged in the relevant paths). The ⟳ handler (web/src/app.js:2415-2420) adds is-spinning and removes it on a fixed 1100ms setTimeout before/regardless of any outcome; reconnectRelay (app.js:3088) returns void, never learns whether the WS reopened or a snapshot applied; relayPollOnce swallows all fetch failures in a bare catch{} (app.js:~3208); the native branch just posts {type:"resync"} and returns. The build-381 setSyncWorking addition (app.js:861-877) reuses the spinner for native mesh status events but is also capped at a fixed 1100ms per transition and does not make a tapped resync outcom
- **Evidence corrections (verifier):** At HEAD d5075091 the citations are: spin add/remove web/src/app.js:2417-2418 (handler 2415-2420); reconnectRelay app.js:3088 (unchanged); swallowed poll failure is the bare `} catch {}` closing relayPollOnce at app.js:~3208 (finding said :3209 — off by one line at current HEAD). New since the finding: setSyncWorking app.js:861-877 (builds 378-381) spins the same fab for native mesh searching/connecting states, fixed 1100ms per transition — partial native mitigation but still not outcome-driven for a tap.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-failux.md`

#### IANAV-07 — First-run web open explains nothing: no app identity, no 'it auto-follows', no no-director state

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:215` — DEFAULT_START_PAGE=2; cold boot with no director renders a bare book page + unlabeled glyphs (:3449)
  - `web/src/app.js:3038` — renderRelayPill hides the pill entirely when !hasDirector — pre-Mass there is no status surface at all
  - `web/src/index.html:19` — The perfect one-liner ('sigue al director en vivo desde tu teléfono') exists only in og: meta — never shown in the app
- **User impact:** A first-time congregant mid-Mass must understand 'do nothing, it follows' with zero cues; today the strongest signal is an 8px green dot, and before Mass there is nothing — pages later change 'by magic' or ⟳ spins and appears to do nothing.
- **Recommendation:** One-time dismissible first-run hint ('Cuando el coro cante, la página cambia sola. ♪ para ir a un canto', localStorage-flagged); rely on M4's planned tri-state pill for the persistent live/no-live signal rather than rebuilding it.
- **Major-update intersection:** M4's always-visible tri-state status pill covers the persistent state signal; this finding claims only the residual one-time first-run hint — ship them together.
- **Verifier notes:** All evidence verified at HEAD: app.js:215 DEFAULT_START_PAGE=2 with app.js:3449 rendering a bare page when !relay.hasDirector; app.js:3038 renderRelayPill sets display:none when no director (zero status surface pre-Mass); index.html:19 og:description holds the explanatory one-liner but it is never shown in-app; no first-run/onboarding UI exists anywhere in app.js. Prior art's M4 tri-state pill covers only the pill-visibility half, which the finding already defers to M4 — the first-run hint is a novel, undocumented gap. Impact is real for the parish context (elderly Spanish-speaking first-time users, zero-training bar, documented PWA follower deployment). Medium severity is appropriate.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

#### RELVER-02 — Staging-room membership is invisible on-device — a canary iPad left on ?env=staging silently never follows the director at Mass

- **Lens:** relver
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:676` — clearInitialUrl deliberately preserves ?env=staging across reloads (correct for canary) but no UI anywhere indicates the device is in the staging room — grep 'staging' in app.js finds only resolver comments, zero indicator
  - `web/src/manifest.webmanifest:8` — start_url:'/' means home-screen relaunches are safe; the risk vector is Safari tabs/bookmarks kept from the Wed canary walk on the oldest parish iPad
  - `web/src/app.js:3160` — resync (and the whole relay lifecycle) operates on the resolved RELAY_ROOM — the on-screen recovery affordance re-syncs the STAGING room, so it cannot fix the symptom
- **User impact:** Sunday: the canary iPad opens from a leftover ?env=staging tab, looks completely normal, and simply never follows the live director; the ⟳ button doesn't help; nobody in the room can diagnose a 4-day-old query param during the entrance hymn.
- **Recommendation:** Web-only: when RELAY_ROOM !== 'alvernia-main' render a persistent high-contrast 'MODO PRUEBA' chip next to the build badge; optionally persist the pin timestamp and auto-return to the prod room after ~24h with a toast. Unit-test a pure shouldShowRoomChip(room).
- **Major-update intersection:** Extends M1's staging channel with the missing operator affordance; M4's planned always-visible status pill is the natural permanent home for the room indicator.
- **Verifier notes:** All evidence verified at HEAD. app.js:676 deliberately preserves ?env=staging across reloads; RELAY_ROOM is resolved once from location.search at app.js:2846-2864 and every relay endpoint (relayStateUrl/relayWsUrl app.js:3168-3169) is bound to it; grep confirms ZERO staging UI anywhere in the web bundle — the only room display is the ?selftest card (svSelftest.js:76-86), which requires a separate query param and is dismissible. The finding actually UNDERSTATES the symptom: with no director in alvernia-staging, renderRelayPill hides the pill entirely (app.js:3038 — display:none when !relay.hasDirector) and boot silently renders DEFAULT_START_PAGE (app.js:3449), so the claimed ⟳ affordance isn
- **Evidence corrections (verifier):** Evidence note 3 cites app.js:3160 for the resync path; at current HEAD the resync tap handler is app.js:3028-3031 (pill click → relayPollOnce(true)) and the room-bound URLs are relayStateUrl/relayWsUrl at app.js:3168-3169, with RELAY_ROOM resolved at app.js:2846-2864. Additionally the pill itself is hidden when the (staging) room has no director — app.js:3038 — so the on-screen recovery affordance is absent, not just ineffective. Notes 1 (app.js:676) and 2 (manifest.webmanifest:8) are exact. | Third evidence item cites app.js:3160 (that line is `relay.following = true;` inside applyRelaySnap). The correct anchors for "resync operates on the staging room" are: ⟳ fab handler app.js:2415-2418 → reconnect() app.js:3085-3103 → relayStateUrl/relayWsUrl at app.js:3168-3169, built from RELAY_ROOM resolved at app.js:2846-2857. Substance of the note is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

### Surface: worker

#### SYNCE2E-07 — Fleet dashboard marks the REAL director 'not ready' whenever ANY checked-in device (canary iPad, Miguel's dev phone, unmatched guests) carries a newer build — guaranteed false red during every canary window

- **Lens:** synce2e
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/src/index.ts:433` — maxBuild reduces over ALL check-ins (orphan/guest devices included); latest = max(maxBuild, MIN_SYNC_BUILD)
  - `sync-worker/src/index.ts:464` — director row: bestBuild < latest → red '⚠ Director en build X (debe ser Y)' sorted to 'por contactar'
  - `sync-worker/src/index.ts:567` — footer repeats 'Director debe estar en {latest}' — a stricter rule than the real MIN_SYNC_BUILD compat floor (:403)
- **User impact:** The green-day runbook's canary-on-the-oldest-iPad ritual (and any dev-device check-in) makes the dashboard cry wolf about the actual director during exactly the pre-Mass windows it exists for, training the operator to distrust it.
- **Recommendation:** Compute the director requirement from roster-matched recently-seen devices only; director ≥ MIN_SYNC_BUILD should never be red on build alone — demote 'behind fleet max' to an amber 'hay una build más nueva (¿canary?)' hint. Extract the row classifier to a pure function + node unit tests.
- **Major-update intersection:** M6 adds a fleet bookVersion column to this same dashboard — fold the classifier extraction in then, or earlier as a standalone worker deploy.
- **Merged from:** RELVER-03
- **Verifier notes:** All three citations verified at HEAD: sync-worker/src/index.ts:433-434 computes latest = max over ALL check-ins (orphans/guests included — the orphan filter at :507 only affects the orphans table, not maxBuild); :462-464 flags the director row red ("por contactar", counted in the bad chip) whenever bestBuild < latest even though the real compat floor is MIN_SYNC_BUILD=361 (:403); :567 repeats the stricter rule in the footer. The trigger scenario is documented ritual, not speculation: docs/pre-mass-checklist.md:23-33 prescribes canary-walking every new build on ONE oldest iPad BEFORE fleet promotion, so every canary window guarantees a legit device with a newer build than the director — and t
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-synce2e.md`

#### VESTIG-02 — Worker comments + README still claim transmitter codes are 'hardcoded in this PUBLIC repo' (false since build 368) and misdocument the required secret set

- **Lens:** vestig
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/src/index.ts:25` — FLEET_DASHBOARD_KEY docblock: transmitter codes 'are hardcoded in this PUBLIC repo, so gating PII behind them would expose every number' — false at HEAD
  - `sync-worker/src/index.ts:672` — Fleet gate repeats the false claim verbatim; directly contradicts index.ts:389 ('codes must never live in this PUBLIC repo') and validTransmitterCodes (:387-396) which reads only the TRANSMITTER_CODES secret, fail-closed
  - `sync-worker/README.md:67` — 'RELAY_DIRECTOR_TOKEN is the only write credential' — false: X-Director-Code ∈ TRANSMITTER_CODES also authorizes /publish (index.ts:780-787)
  - `sync-worker/wrangler.jsonc:15` — Secrets comment names only RELAY_DIRECTOR_TOKEN; TRANSMITTER_CODES (publish fail-closed without it) and FLEET_DASHBOARD_KEY (dashboard 401s) are undocumented
- **User impact:** Two maintainer traps: 'codes are already public' invites re-committing real phone-number codes (the exact A1 class rotated out 2026-07-05); and a fresh worker provisioned per these docs sets only RELAY_DIRECTOR_TOKEN, so every native director's /publish 401s at the next Mass — masked as 'no director' by the outer EMPTY_SNAPSHOT catch.
- **Recommendation:** Rewrite index.ts:23-26 and :670-673 to state the current secret-only model; fix README.md:67 to name both write credentials; list all three required secrets in wrangler.jsonc's comment. Comment/doc-only; redeploy optional.
- **Verifier notes:** All four evidence points hold at HEAD: index.ts:24-25 and :672-673 both claim transmitter codes are "hardcoded in this PUBLIC repo," directly contradicting validTransmitterCodes (index.ts:387-396) which reads only the TRANSMITTER_CODES secret fail-closed with a comment saying codes "must never live in this PUBLIC repo"; sync-worker/README.md:67 falsely states RELAY_DIRECTOR_TOKEN is the only write credential while /publish also accepts X-Director-Code (index.ts:780-787) — the path the native app actually uses; and wrangler.jsonc:15-16 documents only RELAY_DIRECTOR_TOKEN, so a worker provisioned per these docs 401s every native director publish (empty code set, fail-closed) and 401s the fleet
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

## LOW (26)

### Surface: cross

#### VESTIG-01 — clean-header-boxes.py default --in still points at assets/alvernia_manual_2.pdf, deleted by the #271 rename — tool fires FileNotFoundError on next use

- **Lens:** vestig
- **Ship vector:** multi
- **Evidence:**
  - `scripts/clean-header-boxes.py:26` — argparse default = assets/alvernia_manual_2.pdf; `ls assets/` at HEAD d5075091 shows only signo_vivo_371.pdf (renamed in PR #271 / e4d1a014)
  - `scripts/clean-header-boxes.py:11` — Usage docstring also shows the deleted path
  - `web/build.mjs:675` — Contrast: build pipeline WAS updated to signo_vivo_371.pdf in #271, as were check-book-consistency.mjs:22 and e2e/eas-config.test.mjs:59 — only this script was missed
- **User impact:** The canonical header-cleanup tool (per project memory, the designated instrument for future hymnal page edits) crashes when run without --in, exactly how the shipped 290-page cleanup was invoked. Loud failure, quick to diagnose — but a guaranteed stumble at the start of the next book-edit session.
- **Recommendation:** Update both lines to assets/signo_vivo_371.pdf. Optional: add an e2e lint asserting every assets/*.pdf path referenced under scripts/ and web/build.mjs exists on disk. Repo-only edit, no deploy.
- **Verifier notes:** Verified at HEAD d5075091: scripts/clean-header-boxes.py:26 defaults --in to assets/alvernia_manual_2.pdf and the line-11 docstring shows the same path; `ls assets/` confirms only signo_vivo_371.pdf exists (renamed in #271 / e4d1a014). The #271 commit message itself enumerates exactly 3 updated hardcoded-path sites — web/build.mjs (line 675 confirmed), scripts/check-book-consistency.mjs:22, e2e/eas-config.test.mjs:59 — and omits clean-header-boxes.py, so the miss is definitive, not a misread. Running the script without --in will fail loudly when pikepdf/pdftoppm can't open the missing file. Not in undefined/map-prior-art.md (its correction banner notes the #271 rename but lists no finding ab
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

#### VESTIG-07 — __SIGNO_VINO_* (sic, 'wine') misspelling is a load-bearing cross-file bridge contract — must be documented or renamed coordinated, never casually corrected

- **Lens:** vestig
- **Ship vector:** multi
- **Evidence:**
  - `PdfReaderApp.tsx:1036` — Native injects __SIGNO_VINO_NATIVE_FILE_MODE (:1036) and __SIGNO_VINO_NATIVE_BUNDLE_VERSION (:1037)
  - `web/src/app.js:227` — Web reads the misspelled names: NATIVE_FILE_MODE gate (:227, ORed with file:// protocol), fleet check-in version (:2903), version badge (:3477-3480)
  - `web/build.mjs:30` — Comment repeats the misspelled name — a third file cements the typo as convention
- **User impact:** A well-meaning one-side spelling fix silently blanks the version badge and fleet nativeBuild reporting for every fielded shell (368-381 all inject the old name), degrading the pre-Mass readiness dashboard; FILE_MODE itself survives via the file:// fallback.
- **Recommendation:** Cheapest now: add a 'historical typo, this IS the wire contract, do not rename unilaterally' comment at PdfReaderApp.tsx:1035 and app.js:226. If renaming: fold into M3 bridge v1 — web reads __SIGNO_VIVO_* ?? __SIGNO_VINO_* first (web-only deploy), native injects BOTH names for >=2 builds, drop old name after the fleet floor passes the dual-inject build.
- **Major-update intersection:** M3 bridge v1 (typed bridge-protocol.js + hello/welcome handshake) is the natural vehicle for the coordinated rename.
- **Verifier notes:** Verified at HEAD: PdfReaderApp.tsx:1036-1038 injects __SIGNO_VINO_NATIVE_FILE_MODE, __SIGNO_VINO_NATIVE_BUNDLE_VERSION, and (missed by the finder) __SIGNO_VINO_INITIAL_BOOK; web/src/app.js reads the misspelled names at :227 (NATIVE_FILE_MODE gate, ORed with file: protocol so it survives), :2903, and :3480 (version label + build badge); web/build.mjs:30 repeats the name in a comment. Not in map-prior-art.md. A one-sided spelling correction would silently blank the version badge and crash-report build attribution on every fielded native shell — a real, if low-stakes, footgun for the pre-Mass readiness workflow. Severity low is honest.
- **Evidence corrections (verifier):** app.js:2903 is the crash-report payload's build field, not "fleet check-in version" — no fleet nativeBuild reporting in app.js reads this global, so the userImpact's "fleet nativeBuild reporting" clause is wrong; affected surfaces are the version label/build badge (app.js:3480-3493) and crash-report build (app.js:2901-2903). Also add PdfReaderApp.tsx:1038 (__SIGNO_VINO_INITIAL_BOOK) as a third injected global sharing the typo, read at web/src/app.js (grep __SIGNO_VINO_INITIAL_BOOK).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

### Surface: native

#### DIRNAT-02 — H3/#267 DELTA: persisted transmitter-director role is unreadable — the boot resume prompt is gated on syncAvailable, which is false on exactly the device class that writes the breadcrumb

- **Lens:** dirnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:472` — H3 writer: transmitter branch (only reachable when !syncAvailable) persists lastSyncRole='director' so 'the boot resume prompt fires'
  - `PdfReaderApp.tsx:850` — The bootstrap effect opens with `if (!syncAvailable) return;` — the ONLY reader of lastSyncRole (:869-882 'Estabas dirigiendo' Alert) is inside it; syncAvailable is a per-install constant (nearbyDirectorSync.js:6), so the prompt can never fire on that device
  - `PdfReaderApp.tsx:769` — Compounding: the transmitter-only exit-director path never clears/overwrites lastSyncRole (mesh exit writes 'follower' via becomeFollower :432) — once the gate is fixed, intentional exit would leave a stale 'director' → false resume prompt every boot
- **User impact:** The exact outage #267 was shipped to close remains open on no-mesh devices: a restarted transmitter-director comes back as a silent 'off' with no resume prompt — every signovivo.com follower frozen for the rest of Mass with no 401 signal. Today no parish device is in this class (all have the Swift module), but the fix's claimed coverage is wrong and M7's acceptance test only exercises the mesh path, so device verification would pass while this stays broken.
- **Recommendation:** Split the role-restore bootstrap into its own one-shot effect with NO syncAvailable gate: read lastSyncRole, show the resume prompt if 'director', then becomeFollower() only when syncAvailable (persist 'follower' either way to clear the breadcrumb). Also write 'follower' in the transmitter exit branch (:769-773). Unit-test the (syncAvailable × persistedRole) matrix via an extracted pure helper.
- **Major-update intersection:** M7 NEW-DIR-1 acceptance criteria test only the mesh restart — add the transmitter (no-mesh) restart case.
- **Merged from:** SYNCE2E-02
- **Verifier notes:** All three evidence legs hold at HEAD. (1) The H3 write at PdfReaderApp.tsx:472 sits in the becomeDirector branch reachable only when !syncAvailable (:461). (2) The ONLY reader of lastSyncRole in the repo is the 'Estabas dirigiendo' bootstrap at :869-882, inside an effect that opens `if (!syncAvailable) return;` (:850); syncAvailable is a per-install constant (useMemo [] at :128, Platform.OS+Boolean(nativeModule) at src/nearbyDirectorSync.js:6), so the resume prompt is statically unreachable on exactly the device class that writes the breadcrumb — H3's transmitter half is write-only dead code, and its own comment (':470 makes the boot resume prompt fire') is false. (3) The transmitter exit-di
- **Evidence corrections (verifier):** Citations accurate with two path/anchor fixes: syncAvailable constant is defined at src/nearbyDirectorSync.js:6 (not repo-root nearbyDirectorSync.js) and memoized once at PdfReaderApp.tsx:128; the transmitter exit branch is :763-774 (roleGenerationRef bump at :769). H3 writer at :472, gated bootstrap at :850, reader/Alert at :869-882 — all as claimed. | Evidence citations are accurate at HEAD. Minor path fix: the syncAvailable constant is src/nearbyDirectorSync.js:6 (not repo-root nearbyDirectorSync.js:6); the constant is captured in PdfReaderApp.tsx:128 (useMemo with empty deps). The transmitter exit branch is :763-774 (cited :769 is within it). The mesh-exit "follower" overwrite is via becomeFollower at :432 as cited.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### DIRNAT-07 — Post-restart window bypasses the 'Ya hay un director activo' takeover warning: the boot resume prompt urges immediate code re-entry while lastDirectorSnapshotRef is still null, so a restored director can hijack their successor with the calm prompt

- **Lens:** dirnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:874` — Boot prompt: 'Estabas dirigiendo … reingresa tu código en el teclado (♪)' — fires while becomeFollower() is still discovering/connecting the mesh
  - `PdfReaderApp.tsx:599` — The red warning requires a mesh snapshot <8s old (LIVE_DIRECTOR_WINDOW_MS :71); the ref is null until connect + first director page — mesh browse/connect can take longer than typing 10 digits
  - `docs/major-update-2026-07.md:150` — M7's NEW-DIR-3 acceptance criterion tests only the calm no-live-director case — the hijack window is untested
- **User impact:** Director A crashes, B takes over and keeps Mass going; A relaunches, follows the prompt, re-enters the code before the mesh has connected, sees the calm '¿Dirigir el coro?' and unknowingly demotes B (who, per DIRNAT-09, is never told). Control ping-pong in front of the congregation.
- **Recommendation:** Cheap: extend the boot-prompt copy with 'Si otra persona ya está dirigiendo, avisa antes de entrar tu código.' Better: when lastDirectorSnapshotRef is null AND mesh status is searching/connecting, treat liveness as UNKNOWN — after the confirm, race a ~3s requestCurrentSnapshot() grace window and re-prompt with the red warning if a snapshot lands; bounded so a solo director is delayed once.
- **Major-update intersection:** Add the hijack-window case to M7's NEW-DIR-3 acceptance list (currently calm-case only).
- **Verifier notes:** Mechanism is real at HEAD. (1) The boot resume prompt (PdfReaderApp.tsx:872-876) fires immediately from AsyncStorage read, while becomeFollower() only starts in .finally(:881) — mesh browse/connect runs asynchronously afterward. (2) lastDirectorSnapshotRef is set ONLY on receipt of a mesh 'page' event (:899, sole write); until connect + first director heartbeat it stays null, so the liveDirector check (:599-602, 8s window at :71) evaluates false and the code-entry Alert shows the calm '¿Dirigir el coro?' variant instead of the red 'Ya hay un director activo' destructive one. Multipeer discovery+connect plausibly exceeds the time to type a 10-digit code, especially when the boot prompt is act
- **Evidence corrections (verifier):** All citations hold at HEAD. Minor precision: the boot-prompt title 'Estabas dirigiendo' is at PdfReaderApp.tsx:873 (body with 'reingresa tu código' at :874); becomeFollower() kicks off in .finally() at :881. liveDirector check spans :598-602; sole lastDirectorSnapshotRef write is :899 (mesh listener). Also note :612 — the calm prompt already contains a plain-text takeover warning, which the finding's userImpact ('unknowingly demotes B') slightly overstates.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-dirnat.md`

#### FOLNAT-05 — Native first-run still shows the '¿Quién usa este iPad?' Alert.prompt that PR #270 removed from web as 'more annoying than useful' — and it stacks with the critical Local Network permission dialog at the same mount

- **Lens:** folnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:220` — One-time Alert.prompt('¿Quién usa este iPad?') in the device-id effect (iOS, skippable) — still shipping at HEAD
  - `PdfReaderApp.tsx:851` — primeNearbyPermissions() fires from the mesh bootstrap effect on the same mount — the iOS Local Network dialog and the label prompt contend at first launch
  - `web/src/app.js:2986` — Web comment records the removal rationale; commit 3db3a5ba scoped itself web-only ('Native iPads never showed this modal') — the native twin was left by scope, not by decision
- **User impact:** A fresh parish iPad's first launch presents two modal decisions back-to-back before the user sees a page; the annoyance prompt primes an elderly user to dismiss/deny the one dialog that matters (Local Network — the FOLNAT-02 trap). #270's own data says choir members mostly tapped 'Ahora no', so the prompt collects little.
- **Recommendation:** Remove the Alert.prompt and keep fleetCheckin anonymous, exactly as #270 did for web (the dashboard already fills labels from the seeded roster). If a label path is still wanted, move it to M7's DIAGNÓSTICO screen or defer it past the first 'connected' state so first-run has exactly one dialog.
- **Major-update intersection:** M7 DIAGNÓSTICO screen is the natural future home for device labeling.
- **Merged from:** PARITY-07, RELVER-10
- **Verifier notes:** All evidence holds at current HEAD d5075091 (build 381): the native Alert.prompt('¿Quién usa este iPad?') still ships in the device-id mount effect (PdfReaderApp.tsx:220-245, call at :222), and primeNearbyPermissions() fires from the mesh bootstrap effect on the same mount (:851, gated only by syncAvailable which is true on native iOS), so a fresh install does get the label prompt and the iOS Local Network dialog contending at first launch. Commit 3db3a5ba (#270) confirms the asymmetry is by scope, not decision — its message explicitly says 'this is a web-PWA-only cleanup' and 'Native iPads never showed this modal', referring only to the web modal while leaving the native twin untouched; the
- **Evidence corrections (verifier):** PdfReaderApp.tsx:220 is the guard condition (!label && !skip && Platform.OS === 'ios'); the Alert.prompt call itself is at PdfReaderApp.tsx:222 (block :220-245). HEAD is now build 381 (d5075091), not 377 — the finding survives the fast-forward. Note also fleetCheckin() at :251 awaits the prompt's resolution, so the first fleet check-in is delayed behind the modal.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/lens-folnat.md`

#### FOLNAT-07 — Swift bundle-error (~15 diagnosable stages) and memoryWarning are silently dropped by the native JS listener: a follower's failed mesh OTA install has no surface or telemetry, and memory pressure is never acted on

- **Lens:** folnat
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:965` — default: break in the DirectorSyncEvent listener swallows bundle-error, memoryWarning (and the dead takeover-approved/denied)
  - `ios/SignoVivo/DirectorSyncModule.swift:271` — memoryWarning emitted to JS on UIApplication.didReceiveMemoryWarningNotification — never consumed
  - `ios/SignoVivo/DirectorSyncModule.swift:879` — Bundle install pipeline emits bundle-error with a precise stage (header/size/traversal/index/swap...) specifically so failures are diagnosable — all stages dropped
- **User impact:** A follower whose 30MB mesh bundle transfer/install fails sees nothing and keeps the old bundle while the director believes the fleet updated; JS-side /log timelines never show the failure (only Swift's own unbatched dbgLog), making post-Mass forensics blind. Memory pressure never trims the pending-inject queue or web caches before jetsam.
- **Recommendation:** Minimum: dbgLog + breadcrumb both events in the JS listener (near-free). Better, in the M7 batch alongside the held M-F3 defer-install design and the planned bundle signing: surface bundle-error to the DIRECTOR (the actor who can retry); keep the follower silent by design. On memoryWarning, clear pendingInjectRef and skip the next heartbeat injections.
- **Major-update intersection:** Belongs in M7's mesh bundle sha256+signature + M-F3 (deferred install) batch; the repeated re-download loop itself is pipeline-map O2 (not re-claimed here).
- **Merged from:** N2W-06
- **Verifier notes:** Verified at HEAD: PdfReaderApp.tsx:965-966 default:break drops all unhandled DirectorSyncEvent types; Swift emits memoryWarning (DirectorSyncModule.swift:274, observer :248) and bundle-error with distinct stages at :746/:764/:784/:889/:1893/:1898; repo-wide grep shows zero JS consumers (only an e2e contract test asserting Swift emits memoryWarning). Not in map-prior-art.md — adjacent bundle items (M-F3 held defer-install, unauthenticated peer-bundle exec, corrupt-bundle boot) are documented but this JS-boundary telemetry drop is not. Impact real: install failures are Swift-instrumented specifically for diagnosability yet invisible to /log timelines. Low severity is honest.
- **Evidence corrections (verifier):** memoryWarning emit is at ios/SignoVivo/DirectorSyncModule.swift:274 (observer registered :248), not :271. Install-pipeline bundle-error emit is at :889 (doc comment :876), not :879; additional bundle-error emit sites at :746 (timeout), :764 (pack), :784 (send), :1893/:1898 (receive). PdfReaderApp.tsx:965 citation is exact.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/lens-folnat.md`

#### N2W-05 — Dead injected global __SIGNO_VINO_INITIAL_BOOK (zero readers repo-wide) + load-bearing 'VINO' typo grep-trap + false code comment on the invalid-code path

- **Lens:** n2w
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:1038` — __SIGNO_VINO_INITIAL_BOOK injected via injectedJavaScriptBeforeContentLoaded — single occurrence repo-wide (grep-verified); drags initialBook state/setInitialBook plumbing (:90, :839) and the preloadScript memo dep (:1041) as dead weight
  - `web/src/app.js:227` — Web reads the other two globals under the same 'VINO' (not VIVO) spelling — typo is load-bearing/self-consistent but a grep trap
  - `PdfReaderApp.tsx:585` — Comment claims the web 'surfaces código incorrecto' for role:'none' — behavior does not exist (see N2W-01); documentation trap
- **User impact:** No direct user impact; engineer-facing debt that misleads future bridge work (a grep for SIGNO_VIVO finds nothing; the :585 comment asserts feedback that was never built).
- **Recommendation:** Delete the INITIAL_BOOK injection + initialBook plumbing. Do NOT rename the VINO globals (would break shipped-web/new-native pairing) — add a one-line typo-is-intentional comment at both sites instead. Correct the :585 comment as part of the N2W-01 fix.
- **Major-update intersection:** Natural to fold into M3 bridge v1 cleanup (typed protocol replaces ad-hoc globals/comments).
- **Merged from:** VESTIG-06
- **Verifier notes:** All three legs verified at HEAD: (1) __SIGNO_VINO_INITIAL_BOOK is injected at PdfReaderApp.tsx:1038 and has zero readers repo-wide — web/src reads only the NATIVE_FILE_MODE (app.js:227) and NATIVE_BUNDLE_VERSION (app.js:2903, :3480) globals — dragging dead plumbing at :90, :839, :1041; (2) the VINO (not VIVO) spelling is consistent across both native injections and all web readers, so it is load-bearing and must not be renamed unilaterally (shipped-web/new-native pairing); (3) the PdfReaderApp.tsx:585 comment claims the web surfaces "código incorrecto" for role:'none', but the web role handler (web/src/app.js:947-956) only updates badge state and the string "incorrecto" appears nowhere in ap
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-n2w.md`

#### VESTIG-04 — Swift nil-page-guard comment justifies a still-load-bearing guard entirely with DELETED behavior (hymns-4 / geo / bookFromSync) — invites a harmful cleanup

- **Lens:** vestig
- **Ship vector:** native-build
- **Evidence:**
  - `ios/SignoVivo/DirectorSyncModule.swift:226` — Comment block (:222-229) says a nil-page snapshot would be coerced by 'bookFromSync' to default book 'hymns-4', yanking a 'geo-resolved standard (Del Rio)' follower — bookFromSync/hymns-4/geo all deleted; grep of web/src → 0 hits
  - `ios/SignoVivo/DirectorSyncModule.swift:230` — The guard itself (currentRole==director, non-nil page) remains correct: broadcasting a nil-page guess would land newly-connected followers on page 1
- **User impact:** None directly — but a maintainer who verifies the rationale, finds every cited mechanism gone, and deletes the 'obsolete' guard reintroduces spurious page-1 snapshots to joining followers mid-Mass.
- **Recommendation:** Comment-only rewrite: state the current rationale (never broadcast before the director has a real page) and mark the two-book story as historical. Keep the guard byte-identical. Safe to ship without compile verification (comment-only); ride the next native build.
- **Major-update intersection:** Same file M7's device day touches; bundle the comment fix into that build.
- **Verifier notes:** Comment at ios/SignoVivo/DirectorSyncModule.swift:222-229 verified at HEAD: it justifies the nil-page guard entirely via bookFromSync/hymns-4/geo mechanisms, all deleted in build 374 (grep of web/src returns 0 hits for bookFromSync and hymns-4). The guard at :230-233 remains correct and load-bearing (prevents broadcasting an empty-book/page-1 guess to newly connected peers). map-prior-art.md's 'nil-page no-snapshot' entry (line 325) covers the guard's behavior as an old MED/LOW mesh item, not the stale-comment maintainer-trap this finding raises — not a duplicate. Severity low is honest for a comment-only fix; the recommended comment rewrite with byte-identical guard is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

#### VESTIG-05 — '2s mesh heartbeat' comment drift across 5 sites — actual cadence is 1s; NEW-DIR-3's 8s-window derivation cites the wrong number

- **Lens:** vestig
- **Ship vector:** native-build
- **Evidence:**
  - `PdfReaderApp.tsx:400` — Ground truth: meshHeartbeatRef setInterval(..., 1000) — 1s
  - `PdfReaderApp.tsx:68` — 'a director's mesh page arrives on a ~2s heartbeat' — the stated basis for the NEW-DIR-3 8s live-director recency window; re-tuning from this comment miscalculates by 2x
  - `PdfReaderApp.tsx:263` — 'the 2s heartbeat...' (also :780, :900)
  - `ios/SignoVivo/DirectorSyncModule.swift:227` — 'The 2s mesh heartbeat + 1.5s snapshot-probe...' — same stale block as VESTIG-04
  - `PdfReaderApp.tsx:394` — Adjacent dead work: startDirectorHeartbeat always starts the mesh timer; transmitter-only directors early-return every tick — a useless 1Hz timer all Mass. Prefer comment-only until HELD design H2 (heartbeat-effect split) lands
- **User impact:** None at runtime; misleads any engineer tuning liveness windows or debugging heartbeat timing during an incident (timing math off by 2x).
- **Recommendation:** s/2s/1s/ at the five sites and restate the :68 derivation (8s ≈ 8 beats of the 1s heartbeat). Do NOT restructure startDirectorHeartbeat now — that region is owned by held design H2.
- **Major-update intersection:** Touches the exact function held Wave-3 design H2 will split; land comments now, structure with H2 on the M7 device day.
- **Verifier notes:** Ground truth verified: PdfReaderApp.tsx:392-400 sets the mesh heartbeat interval to 1000ms, while comments at PdfReaderApp.tsx:68, 263, 780, 900 (plus an unlisted sixth site at :118 "recovers in ~2s") and ios/SignoVivo/DirectorSyncModule.swift:227 all state "2s". Line 68 is the explicit derivation basis for LIVE_DIRECTOR_WINDOW_MS=8000, so any re-tune from that comment is off by 2x. The adjacent dead-work note also holds: startDirectorHeartbeat starts the mesh timer unconditionally and :393 early-returns every tick for transmitter-only directors. Not a duplicate: map-prior-art H2 (heartbeat-effect split, line 363) covers lifecycle ownership, not this comment drift; no prior-art item document
- **Evidence corrections (verifier):** startDirectorHeartbeat is defined at PdfReaderApp.tsx:390 (not :394); the `}, 1000)` interval close is at :400 as cited. All other file:line citations accurate at HEAD. Bonus drift site not in the finding: PdfReaderApp.tsx:118 ("dropped page-turn recovers in ~2s") should also become ~1s.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

#### VESTIG-08 — src/pdfReaderUrl.js + src/songNavigation.js(+.d.ts) are pre-WebView-era modules with zero importers repo-wide

- **Lens:** vestig
- **Ship vector:** native-build
- **Evidence:**
  - `src/pdfReaderUrl.js:1` — PDF page URL clamp/normalizer (clampPdfPage, PDF_PAGE_MAX=10000) from the pre-build-332 native PDF reader; repo-wide grep (js/ts/tsx/mjs, ex-node_modules) → zero references
  - `src/songNavigation.js:1` — Binary-search findSongEntryOrNext, same era, zero importers; songNavigation.d.ts falls with it
  - `e2e/repo-minimal-footprint.test.mjs:88` — Footprint test pins npm scripts + devDeps, not src files — deletion won't redden CI; no e2e references either
- **User impact:** None (never bundled — Metro only bundles imports). Pure maintainer confusion: two plausible-looking song/page modules that every auditor must re-prove dead.
- **Recommendation:** git rm src/pdfReaderUrl.js src/songNavigation.js src/songNavigation.d.ts; run typecheck; also remove src/alverniaManual2SongIndex.d.ts if tsc confirms no type-importers (the .js stays — build.mjs:563 reads it as text, e2e/offline-books-integrity.test.mjs:17 pins it). Wire-safe, repo-only.
- **Verifier notes:** Evidence fully holds at HEAD: repo-wide grep (js/ts/tsx/mjs/json/md, ex-node_modules) finds zero importers of src/pdfReaderUrl.js and src/songNavigation.js(+.d.ts) — only self-references and the audit's own docs; PdfReaderApp.tsx imports only nearbyDirectorSync/directorRelaySync/offlineBooks. e2e/repo-minimal-footprint.test.mjs never mentions them, so deletion is CI-safe. Recommendation caveats are also accurate: web/build.mjs:563 reads src/alverniaManual2SongIndex.js as text and e2e/offline-books-integrity.test.mjs:17 pins it, so that .js must stay. Not in map-prior-art.md (the related test-suite-song55-test-name-lies-about-clamping item is about a stale test, not these dead modules). Sever
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

#### VESTIG-09 — offlineBooks.ts carries 6 dead STORAGE_KEYS (onboarding*/standardAccessName/mode/activeBookId/lastDirectorAt) plus a comment naming the deleted PDF path

- **Lens:** vestig
- **Ship vector:** native-build
- **Evidence:**
  - `src/offlineBooks.ts:11` — Of 8 declared keys (:11-24), only lastSyncRole (PdfReaderApp.tsx:432/472/506/551/869) and lastPagePrefix (:718, write-only) have any consumer; the other 6 have zero readers AND zero writers repo-wide
  - `src/offlineBooks.ts:5` — Header comment still cites assets/alvernia_manual_2.pdf — deleted by the #271 rename (same class as VESTIG-01)
- **User impact:** Maintainer confusion only; plus a few hundred bytes of orphaned AsyncStorage (sv.onboarding.*, sv.mode, sv.book.active, sv.standard.accessName, sv.sync.lastDirectorAt) persisting forever on devices upgraded from pre-374 builds.
- **Recommendation:** Delete the 6 dead entries and fix the comment to assets/signo_vivo_371.pdf (wire-safe — device-local key names). Optional: one-time AsyncStorage.multiRemove sweep on boot; never touch sv.sync.lastRole or sv.book.lastPage.*.
- **Verifier notes:** Verified at HEAD: of the 8 STORAGE_KEYS in src/offlineBooks.ts:11-24, only lastSyncRole (PdfReaderApp.tsx:432/472/506/551/869) and lastPagePrefix (PdfReaderApp.tsx:718, write-only) have any consumer; repo-wide grep (ts/tsx/js/mjs/swift/m) finds zero readers and zero writers for the others. The header comment at :5 cites assets/alvernia_manual_2.pdf, which no longer exists (assets/ contains signo_vivo_371.pdf; web/build.mjs:675 builds from it). Not in map-prior-art.md. Low severity is honest — maintainer confusion plus orphaned AsyncStorage keys on pre-374 upgraded devices; recommendation is wire-safe since these are device-local key names.
- **Evidence corrections (verifier):** Minor count error: the title enumerates 7 dead keys but says "6". lastDirectorAt (sv.sync.lastDirectorAt, offlineBooks.ts:21) is itself declared-only with zero consumers, so the dead set is 7 keys: onboardingComplete, onboardingState, onboardingCity, standardAccessName, mode, activeBookId, AND lastDirectorAt. The recommendation should delete all 7 (still never touch lastSyncRole or lastPagePrefix).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

### Surface: web

#### ROLEWEB-07 — html[data-role] is never set on pure web and the code comment claims otherwise — one [data-role="follower"] selector away from a web-only layout break

- **Lens:** roleweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:847` — Comment: "Default 'follower' so signovivo.com is right from boot" — false; renderDirectorModeBadge's only call site is the native role event (:952), which never fires on web, so the attribute is absent forever
  - `web/src/styles.css:2154` — Follower layout survives only because it is the unselected CSS base; any future html[data-role="follower"] rule or JS attribute read would work in the native shell and silently break on all of signovivo.com
- **User impact:** No user impact today; classic works-on-iPad/broken-on-web trap for the next engineer — the skew class that caused this app's worst outages.
- **Recommendation:** Set document.documentElement.dataset.role='follower' once at boot (top of initReader), making the comment true and the attribute reliable ground truth on every surface.
- **Verifier notes:** Verified at HEAD: renderDirectorModeBadge (app.js:845-852) is the only place html dataset.role is written, and its sole call site is app.js:952 inside the native bridge 'role' message handler — which never fires on pure web. index.html:2 has no static data-role. So on signovivo.com the attribute is absent forever, making the comment at app.js:847-848 ("Default 'follower' so signovivo.com is right from boot") factually false. Follower layout works today only because all current CSS selectors are director-only (styles.css:2123, 2155-2156) and follower is the unselected base. No match in map-prior-art.md (role items there cover director regressions, not this). Impact is honestly rated: zero tod
- **Evidence corrections (verifier):** app.js: the dataset.role assignment is at line 849 (847-848 is the false comment). styles.css: the director-only selectors are at lines 2123, 2155, and 2156 (line 2154 is within the preceding comment block).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-roleweb.md`

#### FOLWEB-11 — Manifest display_override ['fullscreen', …] hides the status bar (clock/battery) on Android installs

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/manifest.webmanifest:5` — display_override:['fullscreen','standalone'] — Chromium honors the list first, so Android installs run true fullscreen; iOS ignores it, making the regression Android-only
- **User impact:** Congregants' personal Android phones lose the system clock/battery/notification edge during Mass and fullscreen exit is non-obvious for elderly users; no benefit over standalone for a static page viewer.
- **Recommendation:** Drop 'fullscreen' from display_override (display:'standalone' already covers intent). One line; propagates on the next manifest re-check for existing installs.
- **Verifier notes:** Verified at HEAD: web/src/manifest.webmanifest:5 has display_override:["fullscreen","standalone"] alongside display:"standalone". Chromium honors display_override in order, so Android installs run true fullscreen (status bar hidden); iOS ignores display_override, making this Android-only exactly as claimed. Git history (added in early "Refine navigation stage and install affordance" commit) shows no deliberate rationale for fullscreen — boilerplate, not intent. Not a duplicate: the only prior-art fullscreen item (web-reader-fullscreen-fab-noop-on-ios-pwa) is the iOS ⛶ FAB no-op, a distinct issue. Impact is real but minor (congregants' personal Android phones only; parish iPads unaffected) — 
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### FOLWEB-12 — First visit mid-Mass can visibly flash page 1 → page 2 → director's page when the relay peek loses its 1.5s race

- **Lens:** folweb
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/index.html:54` — the <img> ships hardcoded src=page-001.webp
  - `web/src/app.js:3447` — boot peek raced against 1500ms; on loss renderPage(2) commits (:3449) and the gate lifts (:3451), then the late follow decision snaps to the director seconds later (requestId guard self-heals)
- **User impact:** Arriving late to Mass on a slow connection shows two page flashes before landing on the director's page; a first-timer may start swiping from the portada before being yanked.
- **Recommendation:** Don't commit page 2 while the peek response is still in flight: extend the race modestly (~2.5s, under the 4s backstop) or hold the loader until the in-flight peek settles (≤6s abort) before falling back to page 2.
- **Verifier notes:** Verified at HEAD: index.html:54 hardcodes page-001.webp; app.js:3447 races the boot relay peek against 1500ms, and on loss line 3449 commits renderPage(2) (DEFAULT_START_PAGE=2, app.js:215) un-awaited and line 3451 lifts the gate — so page 1 shows until page 2 lands, then the still-in-flight peek (6s abort, app.js:3179) resolves and applyRelaySnapshot (app.js:3155-3162) snaps to the director's page, the requestId guard (app.js:1031/1049) cancelling any stale page-2 render. The 1→2→director double-flash occurs exactly when the relay /state fetch takes 1.5–6s, plausible on a cold cell connection at a late Mass arrival. No prior-art duplicate: the flash entries in map-prior-art.md are MOOT-374 
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-folweb.md`

#### PARITY-10 — Long-press on the score image pops iOS's Copy/Save context menu on web/PWA while the native shell stays quiet — no touch-callout suppression on #page-image

- **Lens:** parity
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/styles.css:211` — #page-image has no -webkit-touch-callout:none / -webkit-user-drag:none; body user-select:none (:146) does not suppress the iOS image callout
  - `PdfReaderApp.tsx:1099` — Native side is configured quiet (allowsLinkPreview={false}, dataDetectorTypes='none', textInteractionEnabled={false}) — same gesture, different outcome per context
- **User impact:** Elderly users on personal phones or the old-iPad PWA who press-and-hold the page mid-Mass get a system sheet (Copiar/Guardar en Fotos/Compartir) covering the music; parish native iPads never show it. Also lets anyone trivially save copyrighted scans from the public site.
- **Recommendation:** Add -webkit-touch-callout:none; -webkit-user-drag:none; user-select:none to #page-image (+ draggable="false"). Keep the viewport user-scalable (index.html:5) so pinch-zoom for elderly readers is untouched.
- **Verifier notes:** Evidence holds at HEAD. #page-image (web/src/styles.css:211-222) has no -webkit-touch-callout, -webkit-user-drag, or user-select rules; a repo-wide grep of web/src finds zero occurrences of touch-callout or user-drag anywhere, no draggable="false" on the img (web/src/index.html:54), and no contextmenu/preventDefault suppression in the web JS. The only ancestor guard is unprefixed `user-select: none`, which (a) Safari only honors with the -webkit- prefix and (b) even prefixed does not suppress the iOS long-press image callout — only -webkit-touch-callout does. Native side is confirmed quiet: PdfReaderApp.tsx:1099 allowsLinkPreview={false}, :1100 dataDetectorTypes="none", :1105 textInteraction
- **Evidence corrections (verifier):** The `user-select: none` at web/src/styles.css:146 belongs to the .viewer-shell rule (starts line 134), not body — same non-suppression conclusion applies, and note it is unprefixed so Safari likely ignores it entirely.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-parity.md`

#### FAILUX-10 — Boot-failure path shows 'No se pudo cargar Signo Vivo.' with no retry affordance

- **Lens:** failux
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:3523` — initReader catch → setLoading text only + revealReader(); revealReader sets __svBooted so the bootGuard's Reintentar card (which exists for exactly this, :36-38) can never take over
- **User impact:** On a handled boot failure the user gets a small text line with no button and no instruction — recovery requires knowing to pull-to-refresh or relaunch.
- **Recommendation:** In that catch, invoke the existing recovery card (window.__svRecover('init-reader', error)) BEFORE revealReader() so the 'Reintentar' card with its reload button appears; skip revealReader on this path.
- **Verifier notes:** Verified at HEAD: web/src/app.js:3523-3527 catch shows only setLoading(true, "No se pudo cargar Signo Vivo.") — a text label with no button (setLoading, app.js:438-442) — then revealReader() → liftGateNow (app.js:59-67) sets window.__svBooted=true, which makes bootGuard's showRecovery early-return (app.js:20), so the existing Reintentar card (app.js:29-39) can never take over. (Minor nuance: the card wouldn't fire on this path even without the flag, since .catch() consumes the rejection and unhandledrejection never dispatches — the dead-end conclusion holds either way.) Not a duplicate: map-prior-art.md's related items (web-reader-unguarded-localstorage... FIXED-#238 and web-reader-initreade
- **Evidence corrections (verifier):** Line 3523 is the initReader() call; the catch body is at app.js:3524-3527. The Reintentar card cited at ":36-38" is in web/src/app.js lines 36-38 (bootGuard IIFE), not a separate file.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-failux.md`

#### IANAV-08 — 'Recientes' records only typed numbers — followed and browsed songs never appear (and nonexistent numbers do)

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:1195` — addToRecientes called only from the numpad jump and (conditionally) search tap; never from turnSong/turnPage/arrows/relay-applied pages
  - `web/src/app.js:2583` — Search tap records only if the tapped page EXACTLY equals a song's start page — interior lyric-page hits record nothing
  - `web/src/app.js:2195` — Empty-state copy promises 'las canciones que hayas visitado recientemente' — mismatch with actual behavior ('numbers you typed')
- **User impact:** The natural use — reviewing what was sung at Mass — yields an empty tab, because live-followed songs never register; typo'd nonexistent numbers are stored and silently skipped at render.
- **Recommendation:** Record on song-boundary dwell in renderPage's commit path (e.g. 15-30s on the same song) regardless of input method; stop recording numbers absent from the index.
- **Verifier notes:** All cited behavior verified at HEAD: addToRecientes has exactly two call sites (app.js:1195 numpad jump, app.js:2584 searchResults click); relay-applied pages, turnSong/turnPage/arrow-swipe navigation never record. Typed nonexistent numbers ARE stored (resolveSongPage:700-708 never fails, falls back to next/last page, then 1195 records the raw typed number) and are silently skipped at render (renderTabRecientes 2199-2201). Lyric-search hits on interior pages set data-page to the interior page (1308) so the exact start-page match at 2583 records nothing. Empty-state copy at 2195 mismatches actual behavior. No prior-art duplicate (map-prior-art.md and audit-findings-raw.md mention Recientes on
- **Evidence corrections (verifier):** Title overstates "browsed songs never appear": drawer-tab song taps (renderSongItem, app.js:2147 sets data-page = song start page) route through the searchResults click handler and DO record via app.js:2584. Accurate scope: relay-followed songs, arrow/swipe page turns, and interior-page lyric-search hits never record; drawer song taps and numpad jumps do. All file:line citations (1195, 2583/2584, 2195) are correct at HEAD.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

#### IANAV-09 — Confirmed: index panel (incl. Easter computus), retired Teclado panel + stale tip, and stub buttons are all unreachable dead UI at HEAD

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:1881` — renderIndexPanel reachable only via #search-index (absent from index.html → null, :108) or via drawer-back/indexDrillDown, which only the index panel itself unhides (:1918-1919) — circular, dead; drags computeEaster/getLiturgicalSeason (:1480-1553) and ~500 lines of renderers with it
  - `web/src/styles.css:2328` — .drawer-mode-switch display:none + openDrawer forces browse mode (app.js:1116) → #numpad-panel and its tip ('toca ↵ Ir', a retired button) can never display
  - `web/src/app.js:138` — Persistence inversion: nc-sort-prefs persists prefs read ONLY by the dead index panel, while the live ⇅ search sort is session-only
- **User impact:** No user impact today (that's the point — it's confirmed unreachable); it bloats every bundle, keeps live-but-unfireable listeners, and persists a preference key that drives nothing.
- **Recommendation:** Delete the component (app.js index renderers + HTML/CSS blocks + nc-sort-prefs + Teclado panel/tip/stubs) as part of the P8 dead-code batch; this answers the atlas question definitively — none of it is user-reachable.
- **Major-update intersection:** Aligned with (additive to) the P8 cleanup batch, which lists other dead surfaces but not this component.
- **Verifier notes:** All three evidence legs hold at HEAD. (1) #search-index has 0 matches in web/src/index.html so searchIndexButton (app.js:108) is null and its listener is null-guarded (app.js:2645); the only other paths to renderIndexPanel (app.js:1881) — the drawer-back click (2492) and the searchClearButton indexDrillDown branch (2559) — depend on state set exclusively at app.js:1918-1919 inside activateSearchFromIndex, which fires only from chips rendered by the index panel itself (2627-2638 ← 1592-1601/1781-1789): a closed cycle with no live entry. computeEaster/getLiturgicalSeason (1485-1552) are called only from renderIndexThemesContent (1570). (2) styles.css:2328 hides .drawer-mode-switch ("Retire the
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

#### IANAV-10 — Spanish copy: missing accents on install surfaces, canto/canción inconsistency, 'relé' jargon in the director banner

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/manifest.webmanifest:4` — 'navegacion rapida por numero de cancion' — four missing accents on an OS-install-visible string
  - `web/src/index.html:54` — alt='Pagina actual del manual' — missing accent (screen readers)
  - `web/src/index.html:114` — Modal universe says 'canto' (IR A CANTO / Abrir Canto) while drawer+help say 'canción' (:165-181, :313; app.js:793) — two words for the same concept
  - `web/src/app.js:912` — 'El relé rechazó el código de director…' — electrician's jargon in the highest-stakes director banner
- **User impact:** Inconsistent terminology raises cognitive load for elderly users; missing accents look unpolished in install prompts; the relay-auth banner's key term ('relé') won't be understood by the volunteer director who most needs it.
- **Recommendation:** One copy-pass PR: fix accents, pick one canonical song term, rewrite the banner ('No se pudo conectar con signovivo.com: el código fue rechazado. Los teléfonos NO están siguiendo.'), improve 'Sin resultados.' with guidance, reconsider the vague 'Tiempo' rail label.
- **Major-update intersection:** Coordinate with major-update §9 open decision #7 (Spanish copy for prompts/banners) so wording lands once.
- **Verifier notes:** All cited strings verified verbatim at current HEAD: manifest.webmanifest:4 has four missing accents on the install-visible description; index.html:54 alt="Pagina actual del manual" lacks the accent (screen-reader surface); the jump modal consistently says "canto" (index.html:91/114/122/142) while the drawer/help/status say "canción" (index.html:165-181/313, app.js:793); and app.js:912 shows the exact "El relé rechazó el código de director…" banner — a role=alert director-facing message whose key noun a volunteer director won't understand. Not a duplicate: map-prior-art.md B.5 item "#7 Spanish copy for prompts/banners" is an open decision about copy for NEW major-update prompts, not a docume
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

#### IANAV-12 — Search sort toggle: blind 3-state cycle with state-vs-action ambiguity, and it's the one sort preference NOT persisted

- **Lens:** ianav
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/app.js:2545` — '⇅ Mejor → ⇅ Nº → ⇅ A–Z' cycle; label shows the CURRENT mode so users can't tell whether tapping applies the label or changes away from it
  - `web/src/app.js:138` — saveSortPrefs persists only the dead index panel's prefs; the live state.searchSortMode resets to 'best' every session
- **User impact:** A user who prefers number-order must blind-cycle the toggle every session; 'Mejor' (relevance) is abstract for the demographic. (Verified non-issue: theme results DO honor the toggle — app.js:1377-1383.)
- **Recommendation:** Persist searchSortMode and render three segmented buttons (Mejor | Nº | A–Z, active highlighted) instead of a blind cycle.
- **Verifier notes:** Both evidence points hold at HEAD: the search sort toggle (web/src/app.js:2547-2554) is a blind 3-state cycle whose label renders the CURRENT mode (SEARCH_SORT_LABELS[next] at :2552), creating genuine state-vs-action ambiguity; and persistence is asymmetric — saveSortPrefs (:138-140) writes only state.indexSortPrefs under "nc-sort-prefs", while state.searchSortMode is hard-initialized to "best" (:172) and never read from or written to localStorage (only writes: click handler :2551), so it resets every session. The finding's self-caveat is accurate (theme results honor the mode at :1379; list results sort pre-cap at :1266-1277). Not in undefined/map-prior-art.md (no sort/search-toggle entries
- **Evidence corrections (verifier):** Line 2545 is the SEARCH_SORT_LABELS constant; the cycle logic itself is the click handler at web/src/app.js:2547-2554 (label set at :2552). One narrative nit: calling the index panel "dead" is imprecise — indexSortPrefs is live and persisted (:165-171, saved at :2599); the point that stands is that searchSortMode is the only sort preference NOT persisted.
- **Full write-up:** `/private/tmp/claude-501/-Users-cazares-src-alvernia-reader--claude-worktrees-jolly-almeida-aef83a/f325daa5-8881-4788-b844-8526753208fd/scratchpad/findings-ianav.md`

#### RELVER-11 — Staging/canary bundle is badge-indistinguishable from prod — no positive confirmation the canary device is executing the new bytes

- **Lens:** relver
- **Ship vector:** web-only
- **Evidence:**
  - `scripts/release.sh:41` — STAGING=1 deliberately skips the bump and builds at the CURRENT version — the canary badge renders the same v<N> as prod
  - `web/src/lib/svSelftest.js:51` — the only differing identity is the opaque CACHE_VERSION hash suffix on the ?selftest card — two hashes a human must diff by eye
  - `web/src/app.js:3490` — build badge renders resolvedBuild with no channel marker
- **User impact:** Even an operator on the CORRECT preview URL cannot visually confirm the canary content loaded (vs a cached prod shell) — compounding RELVER-01's wrong-URL trap; the walk's first checkbox ('app opens to the reader') proves nothing about WHICH bundle opened.
- **Recommendation:** Have release.sh export a staging flag that build.mjs consumes to bake the badge token as '<N>-prueba'; doubles as the RELVER-02 room marker when paired with ?env=staging; prod path stays byte-identical.
- **Major-update intersection:** Completes M1's staging channel affordances.
- **Verifier notes:** All three citations verified at HEAD: release.sh:38-41 builds the staging canary at the CURRENT (unbumped) version.json build number; app.js:3488-3491 renders the badge as bare resolvedBuild with no channel marker (and no staging token exists anywhere in web/src or web/build.mjs — ?env=staging only swaps the relay room); svSelftest.js:51 exposes the CACHE_VERSION hash as the sole differing identity, requiring an eyeball diff against a prod hash the operator must already know. So a canary bundle is visually indistinguishable from prod, and the canary-walk's first checkbox cannot prove which bundle loaded. map-prior-art.md marks M1 (staging channel/selftest) DONE and tracks distinct adjacent i
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

#### VESTIG-03 — sw.js/app.js comments still describe the retired ?admin=1 operator preload flow (and say '370 pages')

- **Lens:** vestig
- **Ship vector:** web-only
- **Evidence:**
  - `web/src/sw.js:91` — 'the offline iPad still preloads the whole manual via signovivo.com?admin=1' — no ?admin handling exists anywhere at HEAD (grep: only comments); precache is now the automatic deferred ensureOfflineBundle
  - `web/src/sw.js:88` — 'all 370 pages' — book is 371 (STANDARD_TOTAL_PAGES, app.js:188)
  - `web/src/app.js:3414` — '(offline / ?admin' — same ghost in the search-index comment
- **User impact:** Written as ops guidance: an operator prepping a parish iPad per this comment visits ?admin=1, sees nothing, and either assumes breakage or falsely assumes a full preload happened.
- **Recommendation:** Rewrite sw.js:87-91 to describe the current automatic precache, fix 370→371, drop the ?admin clause at app.js:3414. Comment-only.
- **Verifier notes:** All evidence holds at HEAD: web/src/sw.js:88-91 claims "all 370 pages" and directs an operator to preload via "signovivo.com?admin=1", but no ?admin handling exists anywhere in the codebase (app.js has zero URLSearchParams usage; the only non-comment "admin" hits are an unrelated offline-admin-note element in index.html:67/styles.css:318). The book is 371 pages (STANDARD_TOTAL_PAGES = 371, web/src/app.js:188). app.js:3414 carries the same "(offline / ?admin" ghost. Not documented in undefined/map-prior-art.md. Comment-only fix; low severity is honest — the sw.js comment reads as real ops guidance and would send an iPad-prepping operator to a nonexistent flow.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

#### VESTIG-11 — DELTA on new-web-dead-books-data-inline-blob: the books.json vestige is THREE sites — emitted dist file + inline blob + a dead per-device precache fetch

- **Lens:** vestig
- **Ship vector:** web-only
- **Evidence:**
  - `web/build.mjs:686` — UNLISTED: also writes dist/books.json as a standalone artifact (known finding covers only the #books-data inline blob at :698)
  - `web/src/app.js:241` — UNLISTED: '/books.json' in SHELL_ASSETS → ensureCoreAssetsCached (:553-564) fetches+caches it on every device/version; zero readers (only reference in app.js is this list; sw.js CORE_ASSETS :22-31 excludes it)
  - `web/src/app.js:556` — Removal is order-safe: precache is Promise.allSettled per-asset, so an old cached app.js requesting a no-longer-emitted books.json just settles rejected
- **User impact:** One dead network fetch + cache slot per device per version; a P8 implementer working from the recorded anchor alone would fix build.mjs and leave the live precache reference behind.
- **Recommendation:** Land all three sites in one commit (build.mjs:686 emission, :698 inline blob, app.js:241 SHELL_ASSETS entry); update smoke-boot if it asserts dist contents. Wire-safe in any order.
- **Major-update intersection:** This IS the P8 #books-data purge item — this finding widens its scope to three anchors.
- **Verifier notes:** All three sites verified at HEAD: build.mjs:686 emits dist/books.json; app.js:241 lists "/books.json" in SHELL_ASSETS and ensureCoreAssetsCached (app.js:553-564) cache.add()s it per device/version; repo-wide grep shows zero readers (only #pages-data is parsed at app.js:3386; sw.js CORE_ASSETS:22-31 excludes books.json). Not a duplicate: prior-art item new-web-dead-books-data-inline-blob (map-prior-art.md:288, P8 at :382) records only the build.mjs:693-698 inline-blob site — the dist emission and the live per-device dead precache fetch are unlisted, so an implementer working from the recorded anchor would leave the fetch behind. Bonus: scripts/smoke-boot.mjs:77 does assert books.json in dist,
- **Evidence corrections (verifier):** Add supporting site: scripts/smoke-boot.mjs:77 asserts "books.json" exists in dist — must be updated in the same commit as the build.mjs:686 emission removal or the smoke gate fails.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

### Surface: worker

#### SYNCE2E-09 — DELTA on A2 (#246): the /publish token bucket (15 burst, 2/s) can 429 a legitimate director scrubbing pages, silently dropping the FINAL resting page for up to ~12s

- **Lens:** synce2e
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/src/index.ts:138` — rateLimited(ip, 15, 2) on publish; :816 returns 429
  - `src/directorRelaySync.js:84` — 429 deliberately silent (only 401/403 warn) and the rejected payload is not retried — recovery waits for the 12s heartbeat (PdfReaderApp.tsx:401-412)
- **User impact:** A director rapidly swiping to hunt for a song (~3-6 publishes/s through the serialized coalescer) can exhaust the bucket in ~5-8s; if the settle-page publish is the one rejected, web followers sit on a mid-scrub page up to ~12s while mesh iPads (no rate limit) already settled — a brief, confusing web/native divergence.
- **Recommendation:** Either raise the bucket to ~30 burst / 4 per-second (still far under flood scale), or client-side re-queue the payload into `pending` on a 429 (~1s retry) so the resting page always lands. Verify vs local wrangler: 25 publishes in 5s → final page in /state within 2s; a2 flood test still 429s.
- **Verifier notes:** All cited code holds at HEAD: publish is bucketed at 15 burst / 2 per-sec per-IP (index.ts:138) and returns HTTP 429 (index.ts:816); the client coalescer treats 429 as silently transient (directorRelaySync.js:83-96) and never re-queues the rejected payload — `pending` is only refilled by a NEW page change, so a 429'd final settle-page publish is recovered only by the 12s relay heartbeat (PdfReaderApp.tsx:400-412). The serialized coalescer publishes at ~1/RTT (~3-6/s), so a director scrubbing for a song for ~5-8s can drain the bucket, and the 1s un-throttled Multipeer mesh means native iPads settle while web followers sit on a mid-scrub page for up to ~12s. Not a duplicate: map-prior-art.md l
- **Evidence corrections (verifier):** directorRelaySync.js citation should be lines 83-96 (429 silence + no re-queue; the explanatory comment is at 84-85), not line 84 alone. Heartbeat citation PdfReaderApp.tsx:400-412 is correct (setInterval 12000 at :412).
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-synce2e.md`

#### RELVER-07 — Worker deploy identity is invisible — /health returns only the frozen wire version, so nobody can confirm which worker code is live

- **Lens:** relver
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/src/index.ts:586` — /health returns {ok, service, v:PROTOCOL_VERSION}; v is the wire version pinned =1 forever by the additive-only contract — it can never distinguish deploys
  - `sync-worker/wrangler.jsonc:15` — worker deploys are manual `wrangler deploy`, untied to version.json — the 2026-07-05 A2 deploy had to be verified by probing rate-limit behavior
- **User impact:** Pre-Mass or mid-incident the operator can read a badge for web and native but has zero way to confirm which worker build serves the room — the surface with instant fleet-wide blast radius; wrangler rollback verification is equally blind.
- **Recommendation:** Deploy wrapper sets DEPLOY_ID (short sha + UTC ts) via --var; echo it additively in /health ({ok,service,v,deploy}) and in the fleet-dashboard header.
- **Major-update intersection:** Trivial to fold into M6's worker/dashboard work; also makes the M1 rollback story verifiable for the worker leg.
- **Verifier notes:** Evidence holds at HEAD. sync-worker/src/index.ts:585-586 shows /health (and /) returning only {ok:true, service:"signovivo-sync", v:PROTOCOL_VERSION}, and PROTOCOL_VERSION=1 is defined at index.ts:29 with the additive-only compat contract (map-prior-art.md invariant #96) pinning it — so it can never distinguish deploys. A grep of the entire worker source finds no DEPLOY_ID, GIT_SHA, or any build/deploy stamp; wrangler.jsonc confirms manual `npx wrangler deploy` (comment at line 3) with only ALLOWED_ORIGINS in vars — nothing ties a deploy to version.json. Prior art corroborates the operational pain: the 2026-07-05 A2 deploy (worker b2f67748) was 'proven vs local wrangler + live' probing (map-
- **Evidence corrections (verifier):** index.ts:586 citation is exact. wrangler.jsonc:15 is slightly off — line 15 is the RELAY_DIRECTOR_TOKEN secret comment; the manual-deploy comment is line 3 ("Deploy: `npx wrangler deploy`") and the vars block (no deploy id) is lines 17-21. The substance of the note is correct.
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-relver.md`

#### VESTIG-12 — sync-worker/test-client.html is three generations stale and reproduces the exact frozen-follower seq bug fixed in the real client

- **Lens:** vestig
- **Ship vector:** worker-only
- **Evidence:**
  - `sync-worker/test-client.html:64` — seq<=lastSeq guard with NO freshness-first check (:64-65) — the P2-SEQ class fixed in 7b3eda4c/#248; freezes on stale-room takeover while real followers move
  - `sync-worker/test-client.html:104` — Publish is Bearer-only — the production X-Director-Code auth path (index.ts:782-787) cannot be exercised
  - `sync-worker/test-client.html:105` — totalPages:370 (book is 371)
- **User impact:** During a live sync incident, the house debugging page would 'confirm' a relay freeze that is actually its own stale client logic — misdirecting mid-Mass diagnosis.
- **Recommendation:** Either port the svSyncDecision freshness-before-seq rule + add an X-Director-Code field + fix 371, or delete the file and document sync-worker/test/a2.test.mjs + run-a2.sh as the sanctioned harness (repo rule prefers deleting dead-behavior test surfaces). Repo-only, no deploy.
- **Major-update intersection:** M4 (transmitterId + tiebreak) changes publish semantics; whichever option is chosen should land before/with M4 so the tool doesn't drift a fourth generation.
- **Verifier notes:** All evidence holds at HEAD: test-client.html:64-65 has the seq<=lastSeq guard with no freshness-first check (the exact P2-SEQ class fixed for the real client in 7b3eda4c/#248 via svSyncDecision.js, per map-prior-art.md item relay-seq-guard-blocks-staleness-and-takeover-on-ws-path, which covers only the real client — the stale test page is not a documented known item); :104 publish is Bearer-only so the X-Director-Code path (index.ts:782) can't be exercised; :105 hardcodes totalPages:370 vs the actual 371. Impact is real, not speculative: sync-worker/README.md:44,57 designates test-client.html as the smoke-test tool, so during a sync incident it would freeze on stale-room takeover due to its 
- **Full write-up:** `/Users/cazares/src/alvernia-reader/.claude/worktrees/jolly-almeida-aef83a/undefined/findings-vestig.md`

## Duplicates of known/planned work (8)

Real behavior, verified at HEAD, but already covered by a documented finding or planned milestone — not re-filed.

- **ROLEWEB-03** (roleweb) — Green "En vivo" pill never demotes when the follower's OWN network dies — false live signal over a frozen page (mirror case M4 doesn't list) → duplicates planned work item M4 (always-visible tri-state status pill, docs/major-update-2026-07.md §6.3, NOT STARTED)
- **ROLEWEB-04** (roleweb) — Follower's entire live-status surface is an unlabeled 8×8px dot that is also its own tap target — illegible for the elderly congregation, and 'no director yet' renders as nothing at all → duplicates planned work item M4 (always-visible tri-state status pill — explicitly replaces the 8px dot, incl. the no-director state)
- **FOLWEB-06** (folweb) — Offline readiness is invisible to the follower and precache failure is console-only (user-facing delta on offline-pwa-dead-offline-gate-ui) → duplicates known OPEN items offline-pwa-dead-offline-gate-ui [L] (P8) + offline-pwa-precache-no-in-session-retry [L] in map-prior-art.md
- **PARITY-04** (parity) — ?selftest readiness card is unreachable inside the native shell — the one context its 'Puente nativo' check exists for; the bridge check is therefore dead everywhere → duplicates deliberately-deferred planned work — ?selftest was scoped web-only in M1 by design; the native readiness check is planned M7 work
- **W2N-02** (w2n) — DELTA on native-swift-stale-documents-bundle-masks-update: Reintentar/watchdog ladder never re-resolves bundleUri, so a broken Documents/WebBundle is an infinite Reintentar loop with no fallback to the shipped bundle → duplicates planned work item M7 boot-watchdog auto-rollback to baked (acceptance test: 'Bricked Documents bundle → auto-reverts to baked')
- **W2N-06** (w2n) — DELTA on relay-no-transmitter-identity (M4): the 'Ya hay un director activo' takeover warning is blind to relay-only directors → duplicates known OPEN item relay-no-transmitter-identity-two-publishers-ping-pong [H] / native-swift-relay-split-brain-no-tiebreak [H] — planned M4 (P2-IDENTITY)
- **IANAV-05** (ianav) — DELTA on web-reader-browse-result-click-skips-relay-browsing-mode + #263 F1: all non-numpad navigation now boomerangs back in ~4s with zero explanation → duplicates known OPEN item web-reader-browse-result-click-skips-relay-browsing-mode [M] in map-prior-art.md (which already notes the #263 F1 drift-semantics change)
- **RELVER-08** (relver) — DELTA of test-suite-sync-worker-zero-tests: the instant-fleet-wide ship vector has ZERO CI gate (not even typecheck), and the relay wire shape is pinned by no test on any surface → duplicates known finding test-suite-sync-worker-zero-tests-security-boundaries [H, PARTIAL] — remedies are the planned P1-HARNESS + P1-WORKER-UNIT items

## Unverified (0)

_(none — every non-duplicate, non-refuted finding was verified)_

## Refuted (3)

- **FOLWEB-09** (folweb) — First-load gate is full-brightness white in a dark-church, dark-themed app → REFUTED: premise wrong — the gate crossfades directly into full-brightness white scanned sheet music, so the app's steady state during Mass IS a white screen; the white gate is a deliberate, documented design choice
- **W2N-03** (w2n) — Native resync (⟳) has no relay fallback — when the mesh is down but internet is up, native followers cannot recover while every web phone syncs → REFUTED: scenario counterfactual for the parish — documented ground truth: the church iPads have NO wi-fi; the mesh is their transport and the relay serves only web followers and tethered devices
- **W2N-05** (w2n) — Transmitter-only exit-director never clears sv.sync.lastRole — false 'Estabas dirigiendo' prompt after an intentional exit (H3/#267 gap) → REFUTED: the false prompt cannot occur — the only lastSyncRole reader is gated on syncAvailable, which is false on exactly the (transmitter-only) device class that takes the non-clearing write/exit paths

_Full verifier reasoning for duplicates and refutations is preserved verbatim in `confirmed-findings.json` (`duplicatesOfKnown[].verifierNotes`, `refuted[].verifierNotes`)._
