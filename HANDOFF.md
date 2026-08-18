# SignoVivo — session handoff, 2026-08-18 ~10:50 AM CT

Overnight session (2026-08-17 evening → 2026-08-18 late morning). This file is for a **cold tab**
with zero context. Read it before touching anything.

## TL;DR

- **70 commits**, all on branch `claude/cloudflare-429-degradation-95d17a`, all pushed, tree clean.
  **NOT merged to main.** That was a deliberate choice, not an oversight — see "Why this isn't
  merged" below.
- Build **457** is installed and hardware-verified working on all 4 of Miguel's physical test
  devices right now. The headline bug (director takes the role → a wrong page flashes before
  correcting ~10s later) is **fixed and confirmed live** ("omg it FINALLY WORKED").
- `signovivo.com` and the Cloudflare relay are both **429** (daily Workers quota) and will stay that
  way until the quota resets at **7:00 PM CT tonight**. This is expected, not a regression — see
  below. Do not spend time on it before then.
- Two IPAs (**453, 454**) never reached TestFlight — Apple's daily upload cap ("wait 1 day"),
  unrelated to Cloudflare. Both are **stale/superseded** by everything through 457. Don't upload
  them; when ready, just re-run `bash scripts/release.sh` fresh.
- A local telemetry log sink (`scripts/log-sink.mjs`) may still be running on this Mac, port 8787.

## Why this isn't merged to main

Miguel's global rules auto-ship green work to main by default. I overrode that default here,
per his own rule that a "product/UX decision... that materially affects the director or the
congregation" gets asked about rather than assumed. This branch touches:

- Director-conflict/takeover logic (`becomeDirector`, the mesh page-broadcast race fix)
- BLE beacon seq/nonce handling and self-healing
- The relay quota circuit breaker and the **removal of the entire fleet dashboard** (routes, DO
  RPCs, both clients' check-in calls — gone, not just disabled)
- A full corner-cluster UI rework (director controls, the crossed-out ⟳, safe-area placement)

70 commits, one continuous overnight session, zero external review, shipping straight to a
choir's live-Mass sync tool. That's squarely the kind of fork Miguel's rules say to surface rather
than decide unilaterally. **Merging to main is the next action, pending his go-ahead** — the branch
is fully tested and pushed and ready the moment he says so.

## The headline fix (why tonight mattered)

Two live hardware test runs (director on song 372; followers on 100/200/300) each showed a
**wrong page flash instantly**, then a correction to 372 about ~10s later:
- Run 1: flashed **song 2**
- Run 2: flashed **page 1** (the cover)

Different wrong value each run → not a fixed stale cache, a **race**. Root cause: `currentPageRef`
(a native-side mirror of the WebView's page) is fed asynchronously by an earlier bridge message.
The WebView renders a jump **instantly** (pure client-side draw, no native round trip needed), so
tapping "Ser Director" right after a jump could fire the first broadcast before that async message
landed — sending whatever page was true a *moment ago* (the WebView's documented boot-default page
2, or the pre-jump page) to the whole choir.

**Fix** (commit `aa68c9e`): the web's true page now rides *with* the `request-director` tap itself
(`postNativeBridge({ type: "request-director", currentPage: state.currentPage })`), threaded
through `onDirectorCode` → the native confirm `Alert` (safe across the wait — it's modal, so the
WebView can't navigate again while it's open) → `becomeDirector`, which corrects the mirror from
the fresh value **before** any broadcast path reads it.

Verified: 417 tests, `xcodebuild` `BUILD SUCCEEDED`, then confirmed live on all 4 devices.

**Not re-measured**: raw convergence latency (the sub-1s stretch goal). What's confirmed is
*correctness* — no more wrong-page flash. Whether the ~10s settle time itself got faster from
tonight's BLE self-healing work is unconfirmed. See `docs/convergence-theories-2026-08-18.md` if
that's still wanted — Theory 0 there (relay vs. mesh) was disproven overnight; the beacon/mesh-race
theories are what's left, and most are now fixed (see below).

## Everything else that shipped tonight (roughly chronological)

1. **UI overhaul of the top corner controls** — crossed-out ⟳ when nobody's directing (shape-based,
   not color-based, so it survives colorblindness), a "¿Eres Braulio?" self-filtering shortcut,
   `☆ Ser Director` / `Salir Director` squares, safe-area clamp added *then reverted same day*
   (confirmed on device to clip content under the status bar — see commit `fbe9789`), optical
   centering of the ⟳ glyph, several rounds of pixel nudges per Miguel's live feedback.
2. **BLE bugs found and fixed**: a second seq guard that had been silently rejecting every new
   director's pages since build 448 (`e6a4049`); no contention/abstain handling when two
   advertisers exist (`90d27a6`); unbounded advertised seq (same commit); self-healing
   advertise/scan that re-asserts if the radio silently drops (`44ed9f2`); radios pre-warmed at
   boot instead of on first use (`5903972`); a director destroying its own advertiser at the exact
   moment followers were inviting it (`fb7b467` — this was the leading theory for the ~10s gap and
   is now fixed, independent of tonight's headline race fix above).
3. **Cloudflare quota**: root-caused a *self-sustaining* outage (a 429 made the client retry
   *harder*: backoff climbed to 8s and ALSO started 4s polling, then a 10s health timer reset
   backoff to 500ms and restarted both). Fixed with a circuit breaker, telemetry made opt-in +
   leveled (off/error/warn/info/debug, default off), the fleet dashboard **deleted entirely**
   (was costing ~3,840 req/day + storing choir phone numbers), a 10k/day non-essential quota
   reservation so signovivo.com always has ~90k/day for itself. Self-inflicted traffic went from
   **117% of the daily cap to ~0.05%** in real-world terms.
4. **A local debug log sink** (`scripts/log-sink.mjs`) — runs on Miguel's own Mac so telemetry
   costs Cloudflare nothing even at debug level. Wired into the diagnostics dump (Ir a Canto →
   `999` → Guardar) so the sink URL + telemetry toggle are settable on-device, pre-filled with a
   *temporary* default IP (`192.168.1.197`) that **should be deleted after this weekend** — it's
   only valid on Miguel's home network.
5. **`el app`, not `la app`** — global Spanish house-style rule, now in memory
   (`feedback_el_app_not_la_app.md`) and pinned by a test.
6. **Every code change bumps the build number, no exceptions** — Miguel's rule, now in memory
   (`feedback_every_code_change_bumps_build.md`) after I got caught reusing a build number twice
   in one night for different bits, including for local-only dev installs.

## Current state, precisely

```
branch:  claude/cloudflare-429-degradation-95d17a
HEAD:    aa68c9e (pushed, tree clean)
ahead of origin/main by: 70 commits
version.json: buildNumber 457
```

**On the 4 physical devices** (iPhone, mPad, iPad2 "Rita y Alfredo Varela", iPad de Braulio — all
local dev-signed installs via `devicectl`/`ios-deploy`, NOT via TestFlight): build 457, badge reads
`v457-457` (native/web numbers match, no split). All confirmed working.

**Brau MASTER** is a *different* physical iPad from "iPad de Braulio" (different device IDs, different
iOS versions — 17.7.11 vs 16.5). It was **not physically present** at any point tonight. Don't assume
it's the same device as anything else, and don't chase it unless Miguel says he has it.

**TestFlight**: last successful upload was build **452** (verified `IN_BETA_TESTING`). Builds 453 and
454 both hit Apple's daily upload cap (error 90382, "wait 1 day") and only exist as local archives —
`~/Desktop/SignoVivo-453.ipa` and `-454.ipa`. **Both are stale** (superseded by everything through
457). Don't try to upload them; cut a fresh one instead.

**`release.sh`** now does the whole pipeline in one call including TestFlight distribution
(`scripts/testflight-distribute.mjs`, added tonight after build 452 got stuck at "Ready to Submit" —
the old script stopped right after upload and left group-attach + beta-review-submit as a manual
step nobody did). It also **waits for Apple to register the build** before trying to attach it
(452's replacement bug — a race between upload finishing and ASC indexing it).

## Gotchas for whoever picks this up

- **`ios/WebBundle` is gitignored and only synced by `release.sh`'s explicit step**
  (`rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle`). A plain `xcodebuild build` does **not**
  refresh it. I burned two install cycles tonight before catching this — a native rebuild with a
  web-only change looked successful but was silently wrapping the *old* web bundle. If you're doing
  local dev installs and touched anything in `web/src/`, sync WebBundle by hand first.
- **`AsyncStorage.multiGet` resolves a missing key to `null`, never `undefined`.** Caused a real
  bug tonight (a settings field displaying the literal text `"null"`). Fixed, but worth remembering
  as a class of mistake in this codebase.
- **Never run simulators during a live mesh/BLE test.** A sim director (newest token) hijacks every
  physical follower into a connecting↔notConnected loop — this is a *standing* project memory
  (`feedback_no_sims_during_live_tests.md`), not new tonight, but it's exactly the kind of thing
  that's easy to forget at 3 AM.
- **`devicectl` (iOS 17+) and `ios-deploy` (iOS <17, via `brew install ios-deploy`) are two separate
  install paths.** iPad2 and iPad de Braulio are iOS 16.x and invisible to `devicectl` entirely —
  they need `ios-deploy` over USB. iPhone/mPad are iOS 26.6 and only reachable via `devicectl`
  (works over wifi once paired, shows as "available (paired)" or "connected").
- **The `log-sink.mjs` process may still be running** — `lsof -i :8787` to check, PID was 29889 at
  last check. Harmless to leave running (listens on Miguel's LAN only); kill it if you don't need
  it.
- **8 other worktrees exist on this repo from other tabs** (`git worktree list` shows them). Per
  standing multi-tab rules, none of them are mine to touch — this handoff only concerns
  `alvernia-stress-test-verify-c393b9`.

## Deferred, not forgotten

**Task #7** (still pending): director-handoff social conflict resolution (what happens when two
devices both think they're director) + auto-resume-role-on-startup. Deliberately not started
tonight — it's the most dangerous remaining surface (who directs), needs a rested morning and real
devices, not a 3 AM decision. Full context is in the task itself (`TaskList`/`TaskGet` id 7).

## Pointers

- `docs/convergence-theories-2026-08-18.md` — five theories for the original ~10s gap, most now
  fixed; Theory 0 (relay vs mesh) disproven on hardware overnight.
- Memory: `feedback_el_app_not_la_app.md`, `feedback_every_code_change_bumps_build.md` (both new
  tonight), plus the standing ones referenced above.
- `scripts/log-sink.mjs`, `scripts/testflight-distribute.mjs` — both new tonight, both documented
  in their own file headers.

## Next-tab kickoff prompt

See the message accompanying this handoff for the copy-paste block to start a fresh tab.
