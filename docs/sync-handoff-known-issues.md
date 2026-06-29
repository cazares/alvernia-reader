# Director-sync handoff — known issues & follow-up

_From a 2-agent exhaustive audit of the multi-device sync (2026-06-29), triggered by a reported
director-handoff bug. This tracks what was FIXED in build 344 vs. what's DEFERRED (needs 2-device
on-hardware testing before touching the delicate Multipeer/Swift mesh)._

## ✅ Fixed in build 344 (testable: typecheck + browser)

| Area | Fix | File |
|---|---|---|
| **Reported bug** — exit director → stuck on default page, ⟳ dead | `exit-director` now → `becomeFollower` (role "follower", re-joins mesh, follows new director) instead of `performSoftReset` (role "off") | `PdfReaderApp.tsx` exit-director case |
| ⟳ resync stranded in "off" | resync re-joins as follower if "off" before requesting snapshot | `PdfReaderApp.tsx` resync case |
| ⟳ can't re-find a dropped/changed director | resync now also calls `refreshNearbyDiscovery()` (fast re-browse) | `PdfReaderApp.tsx` resync case |
| Web: transient geo-switch failure pins the WRONG book forever | `relayGeoBookApplied` set only AFTER `switchBook` confirms; else next poll retries | `web/src/app.js` relayPollOnce geo block |
| Web: a restarted director (seq reset low) ignored as "stale" | ⟳ resets `relay.lastSeq = -1` so the director's current seq is accepted | `web/src/app.js` reconnectRelay |

## ⛔ DEFERRED — need 2-device on-hardware testing (do NOT rush untested Swift mesh changes)

### HIGH
1. **No live-director takeover via the numpad.** Grab Device B (a connected follower) while Device A is still directing, enter the code → Swift rejects `DIRECTOR_TAKEOVER_REQUIRED` (`DirectorSyncModule.swift:264-266`), JS retries/fails → injects role `"none"`, stripping B's UI. Takeover only works if A first disappears. **Fix:** in `onDirectorCode`/`becomeDirector`, if connected as a follower, `resetNearbyDirectorSync()` first (drop the follower link) then `startNearbyDirector` (wins on newer token); and never inject role "none" on this rejection (`PdfReaderApp.tsx:321-350`, `becomeDirector` ~225-264).
2. **Dual-director split-brain (~25 s).** A 2nd director started while the 1st is still live → both broadcast (mesh + relay) until the loser's 25 s browser cycle catches the higher token → followers see pages **flapping**. Relay path has NO conflict resolution. **Fix:** new director broadcasts an explicit "taking over, token=Tb" control msg so the loser demotes immediately; add relay-side transmitter identity + tiebreak.
3. **`set-book`→`page` two-script race.** Native injects `set-book` (web jumps to the book's DEFAULT page) then `page` as two separate WebView scripts; they interleave at the first `await switchBook` → follower can momentarily land on the book default, or get stuck on the wrong book if the load rolls back. **Fix:** collapse to ONE event — the `page` sync-event already carries `book` and switches atomically (`PdfReaderApp.tsx:551-553` et al.); stop emitting the separate `set-book`.

### MED
- Advertiser accepts ANOTHER director as a "follower" (no role check) → cross-connected directors (`swift ~1342`).
- Director foreground doesn't re-snapshot existing followers → silent half-dead mesh after iOS suspend (`swift 215-240` / `PdfReaderApp.tsx:647-673`).
- Mesh-bootstrap effect can re-run and re-promote/re-mint a director mid-session (`PdfReaderApp.tsx:511-614`) → gate behind a `didBootstrapRef`.
- Legacy-director branch wedges "connecting" forever if `discoveryInfo` arrives nil (treat missing `hgen` as modern + self-invite) (`swift ~1284`).
- Relay `seq` is wall-clock-ms, not device-unique → two transmitters collide → flapping (`directorRelaySync.js:20-28`).
- Web: zombie `CONNECTING` socket can block automatic reconnects (`app.js connectRelay` dupe-guard); web ⟳ already nulls the socket so it's mitigated there, but `online`/foreground reconnects can stall → add a CONNECTING timeout.
- Web: stale director hides go-live bar + pill inconsistently (`app.js ~2820`) → drive bar/pill/browsing from one reconcile fn.

### LOW
- `randomToken()` is a µs timestamp, not random → equal tokens = no conflict resolution (`swift ~1322`) → add UUID suffix.
- Advertiser/browser silently stop retrying after 5 failures (`swift ~1356`) → surface a "followers can't see you" state.
- Follower accepts `page` packets from ANY peer, not just `connectedDirectorPeer` (`swift ~1557`).
- Director with `currentPageNumber == nil` sends no snapshot on follower-join (`swift ~161`) → fall back to page 1.
- Web dead state: `appliedPage` never read, `manualClose` never set true.

## Test plan for the deferred work (requires 2 physical iPads/iPhones on the same wifi)
1. Live takeover: A directing on song 100 → grab B (follower) → enter code → B should take over, A should demote + follow B. (#1)
2. Split-brain: start B as director without cancelling A → confirm only ONE director broadcasts within ~1 s, no flapping. (#2)
3. Cross-book: A on standard song 364, B follower on hymns-4 → A drives → B should land on standard 364, never the book default. (#3)
4. Director vanish: A directing → kill A → B (follower) taps ⟳ → should recover when A's replacement appears.
5. Background/foreground both roles mid-session.
