# OTA follow-ups — deferred from the 2026-08-04 root-cause session

Build 395 fixed the three defects that made the songbook OTA a permanent no-op. This file records
what was found and **deliberately not fixed**, so it is not rediscovered from scratch.

Each item carries an honest verification status. An adversarial audit (36 agents, five independent
lenses) produced the raw list; some of its claims were overstated and are corrected here. **Do not
treat an unverified row as a known bug** — re-derive it before acting.

## Fixed in 395 — context for what follows

| # | Defect | Commit |
|---|---|---|
| A | `stageBook` never wrote `bundle-manifest.json`, so every applied bundle was unidentifiable and `decideBundle` rule 3 evicted it. **The root cause.** | `4b96646` |
| C | `stale-ready` deadlock: past the 12 h TTL a device could neither apply nor re-stage, and neither state emitted a breadcrumb | `765e25c` |
| F | `resolveBundleUri`'s cached fast path never restored `activeBookVersionRef` | `f764f1d` |

A fourth change (rule 7 `>=` → `<` + `generatedAt` tie-break) was **reverted** in `90a2d37`; see
that commit for why the justification was false. The equal-shell case is load-bearing.

---

## Verified by hand — real, deferred

### 1. A cold launch never retries a pending apply

**Status: VERIFIED.** `autoApplyIfSafe` runs from exactly two places: after a successful stage
(`PdfReaderApp.tsx:1441`) and on `AppState` `"change"` → `active` (`:1776`). A cold launch starts
*already* active and fires no transition, so a device that boots holding a ready staged bundle does
not attempt the swap until someone backgrounds and re-foregrounds the app.

The audit claimed "the apply is never retried" — **that is wrong**, `:1776` makes every foreground a
retry, which is the intended CodePush shape. The gap is only the cold-launch edge.

*Fix sketch:* call `autoApplyIfSafeRef.current?.()` once after `webReady` first flips true.
Deliberately not done in 395: it runs on the boot path, which is the riskiest code in the app, and
the existing foreground path already covers the realistic case.

### 2. `shouldStage` returning `already-staged` short-circuits before any apply attempt

**Status: VERIFIED.** `onCheckinResponse` returns at `:1413-1418` whenever `decision.stage` is
false, so the 90-second check-in timer never drives an apply. Combined with item 1, the only apply
triggers are "just finished downloading" and "user foregrounded the app". Low impact given the
foreground path, but it means the check-in loop is not self-healing on its own.

### 3. `sv.book.forceBundled` is not cleared by a successful apply

**Status: VERIFIED.** `performApplySwap` (`:1456-1486`) clears `sv.book.staged` and
`sv.book.resolved` but not `sv.book.forceBundled`. While that record is inside its
`FORCE_BUNDLED_TTL_MS` window, `resolveBundleUri` (`:1144-1148`) keeps forcing the baked bundle, so
an OTA that lands is reverted on the next resolve.

Narrow: the switch auto-expires and is only set by a deliberate operator action. But if someone uses
the panic switch and then an OTA arrives the same day, it silently loses.

### 4. `bookStageRef` does not survive a relaunch, so a blank Libro cell proves nothing

**Status: VERIFIED.** `bookStageRef` is a plain `useRef` (`:153`) reset to `""` on every launch, and
`:278` only sends `bookStage` when non-empty. A blank **Libro** column therefore cannot distinguish
"never started" from "relaunched since". This matters because that column is the primary instrument
for diagnosing a rollout — it was the first thing reached for on 2026-08-03 and it would have
misled.

### 5. A right-size / wrong-bytes file wedges staging permanently — **highest severity open item**

**Status: VERIFIED 2026-08-04 (both halves read directly).** Two individually-reasonable behaviours
combine into a trap with no exit:

- `bookNet.download` (`PdfReaderApp.tsx:1351-1355`) throws only on `status >= 400`. Any 200 carrying
  a wrong body — a captive portal, a truncated proxy response, a CDN error page — is written to disk
  as though it were the file.
- `stageBook`'s resume path (`src/bookUpdate.js:352-360`) re-fetches a file **only when its size
  differs** from the manifest: `if (!info || Number(info.size) !== Number(f.n))`. Size matches,
  bytes wrong → never re-fetched, on this attempt or any future one.

`verifyStaged` then fails on md5 forever. Every retry re-walks 390 files, re-hashes ~27 MB, fails on
the same file, and reports `error:verify`. The device never converges and never self-heals — and
because `stageBook` returns `ready:false`, nothing is ever applied, so from outside it looks exactly
like the bug 395 just fixed.

*Fix sketch:* on a `verify` failure, delete the files named in `verdict.problems[]` before
returning, so the resume path is forced to re-fetch them. A blanket `rmrf(stagedDir)` after two
consecutive verify failures is the cheap version and cannot be got wrong.

This one is worth doing next. It is the only open item that produces a permanently stuck device
from an ordinary transient network fault.

---

## Audit-confirmed, NOT independently re-verified

Re-derive before acting on any of these.

- **The free-disk guard is dead code** — `stageBook` accepts `freeDiskBytes` but its only caller
  never passes it (`:1423-1435`), so the disk check never runs on a device.
- **A failed `Promise.all` leaves orphan download workers running** after `stageBook` has returned
  and `stagingInFlightRef` has been cleared, racing the next attempt on the same directory.
- **`stagingInFlightRef` is checked two awaits before it is set** (`:1394` vs `:1420`), so
  overlapping check-ins can start two `stageBook` runs against one directory.
- **The revoke path does not check `stagingInFlightRef`** (`:1379`), so one pointer-less check-in
  can delete a download that is actively in flight.
- **Worker `checkin()` sticky-merges `bookVersion` / `bookStage` / `bundleSource`**, so
  `decideBookUpdate` and the dashboard both see a high-water mark of everything a device ever
  reported rather than its current state.

---

## The lesson worth keeping

All three defects fixed in 395 lived on **seams between components that were individually tested and
individually green**. `stageBook` had a suite. `decideBundle` had a suite. Nothing fed one's output
into the other's input, and the bug lived exactly there for three TestFlight builds while every
server-side probe passed — correctly.

`e2e/bookOtaSeam.test.mjs` exists to hold that boundary. Its rule: **never hand-construct a
`docManifest`.** Read what `stageBook` actually left on disk, the way `PdfReaderApp.tsx:1153-1156`
does. A test that builds its own manifest is testing `decideBundle` again — which already passed,
and which never caught this.
