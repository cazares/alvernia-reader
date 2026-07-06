# Sync reliability audit — 2026-07 (connect / disconnect / reconnect / sync / resync)

Adversarial 3-agent hunt of the whole sync surface, commissioned for "ultimate robustness,
just-works-Apple-style." Three subsystems audited independently: the **web relay** (phones),
the **native↔web bridge + role state machine**, and the **native Multipeer mesh** (the offline
church transport). This is the map + status. Findings are ranked by "can it silently break sync
during Mass with no auto-recovery."

**Legend:** ✅ shipped · ⏳ pending Miguel's call · severity CRIT/HIGH/MED/LOW · verifiability.

---

## ✅ WAVE 1 — web relay (PR #263 `d7fc0938`, merged, browser-verified) — ships on next `pages deploy`

Web relay path is **follower-only** (app.js never publishes; directing is 100% native), so none
of these can affect a director.

| ID | Sev | Fix | Status |
|----|-----|-----|--------|
| F1 | CRIT | Stray swipe/next-prev/arrow strands a follower off-page with a GREEN pill, no recovery until the director's next move. Heartbeat now force-re-homes a drifted follower in ~4s. | ✅ |
| F2 | CRIT | 12s-silence zombie-close now also starts /state polling (iOS may never fire `close`). | ✅ |
| F3 | HIGH | Independent time-driven health watchdog (10s) — the guaranteed recovery floor when no event fires. | ✅ |
| F4 | HIGH | Demote resets `lastSeq=-1` so a restarted low-seq director is followable (was rejected forever). Unit-tested. | ✅ |
| F5 | MED | Clear pending reconnect on every `connectRelay` entry (before the dupe-guard). | ✅ |

## ✅ WAVE 2 — native bridge / role (PR #264 `d688b926`, merged, typecheck + contract e2e) — next native build, Wed device-test

| ID | Sev | Fix | Status |
|----|-----|-----|--------|
| C2 | CRIT | Demoted director (DIRECTOR_CONFLICT) kept its own stale page; now pulls the winner's page + re-scans. | ✅ |
| H4 | HIGH | `-1` render-failed sentinel could yank the whole congregation to page 1; `broadcastPage` now floors it. | ✅ |
| C3 | CRIT | Step-down straggler relay frame; `becomeFollower` now clears the publish code (straggler 401s). | ✅ |
| H1 | HIGH | Reloaded follower re-asserted a cached snapshot but never pulled fresh; `bridge-ready` now requests current. | ✅ |

---

## ⏳ WAVE 3 — native Multipeer MESH + structural bridge (PENDING Miguel's decision)

**Why pending, not shipped:** (a) the Swift mesh **cannot be compiled in this environment** (no Xcode
build in the loop) — an uncompiled Swift change risks breaking the build, discovered only at the next
release archive; (b) the mesh is the **disaster surface** (offline church-critical, no internet
fallback); (c) some bridge items are **structural refactors** (higher regression risk). These want a
local `xcodebuild` to confirm compilation + focused Wednesday device-testing before they're trusted.

### Native mesh (`DirectorSyncModule.swift`) — Swift, uncompileable here
| ID | Sev | Finding | Fix size |
|----|-----|---------|----------|
| M-F1 | CRIT | Half-open follower watchdog is disarmed until the FIRST page ever arrives (`lastFollowerPageReceivedAt > 0` gate at :1254). A follower that joins a director who hasn't turned a page yet has NO half-open recovery. Fix: prime the liveness clock at `.connected` and/or stamp it on the periodic `director_announce` (director already broadcasts it) so "silence since connect" is watched. | SMALL-MED |
| M-F2 | HIGH | Director never prunes a half-open FOLLOWER → dead followers keep 7-slot seats and can block a real follower's reconnect ("sessions-full"). Fix: track `lastHelloFromPeer`, drop peers silent > ~20-25s. | MED |
| M-F3 | HIGH | A peer-pushed bundle install swaps the live `WebBundle` dir + remounts EVERY follower mid-Mass (unprompted, coordinated reload). Fix: defer the install/remount to a safe moment (backgrounded/idle) or gate behind a confirm. | MED |
| M-F5 | MED | `forceFollowerReconnect` doesn't kick a fast rediscovery if the director was already pruned → slow recovery. Fix: fast-refresh burst on reconnect. | SMALL |
| M-F6 | MED | Director-conflict demotion hard-resets the loser's followers (no snapshot handoff) → sub-group blackout. Fix: additive `redirect` hint before teardown. | MED |
| M-F7 | MED | Advertiser/browser give up permanently after 5 failures; only a foreground event re-arms — a foregrounded director whose radio hiccups stays dark. Fix: slow last-resort retry + reset the counter on manual reconnect. | SMALL |

### Bridge / role (`PdfReaderApp.tsx`) — compileable but structural/higher-risk
| ID | Sev | Finding | Fix size |
|----|-----|---------|----------|
| C1 | CRIT | `injectJavaScript` is fire-and-forget; a silently-dropped page inject desyncs web from native, and the heartbeat de-dupe (`page === currentPageRef`) never re-drives it. Fix: track `lastInjectedPageRef` distinct from `currentPageRef`, re-inject on mismatch. | MED |
| H2 | HIGH | The mesh-bootstrap effect's cleanup calls `stopDirectorHeartbeat()`; if that effect ever re-runs for a live director (a future refactor that adds a state dep), it kills the heartbeat mid-Mass. Fix: split the listener into its own effect; own heartbeats only in the role machine. | MED |
| H3 | HIGH | A transmitter-only director isn't persisted; a native app restart (not just WebView reload) comes back as a silent follower → web congregation frozen with no 401 signal. Fix: persist a non-credential `wasTransmitter` breadcrumb → the existing NEW-DIR "re-enter code" prompt. | MED |
| M1-M4, L1 | MED/LOW | inject-queue drops by age not importance; corrupt peer-bundle poisons every boot (fallback re-loads it); director foreground re-broadcasts before conflict resolves; async confirm dialog acts on stale role; web→native post has no retry. | mixed |

**Recommendation:** M-F1 + M-F2 (the bidirectional half-open gap) and C1/H3 (silent-freeze classes)
are the highest-value Wave-3 items. All are additive to the wire contract. But given the
can't-compile + disaster-surface constraints, do them with a local build to confirm compilation and
extra Wednesday device-testing — not shipped blind.
