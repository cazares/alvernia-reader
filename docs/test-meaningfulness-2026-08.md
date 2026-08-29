# How we know these tests are good — 2026-08-29

Written after Miguel asked two questions in a row: *"how can you be sure they're good tests?"* and
then *"write more meaningful tests that actually prove the app works as intended."*

The first question is the harder one, and it has only one honest answer: **break the code on purpose
and check that the tests notice.** A green test run and a useless test look identical from the
outside, and this repo has been on the wrong side of that identity more than once.

---

## 1. What we found

Every source-text assertion in `e2e/` that looked decorative was checked by measurement — apply a
real regression to the real source, re-run the named test, record whether it goes red. **Of 25
checked, 19 stayed GREEN.** Eleven of those were high severity.

Here is what was slipping through, silently:

| What could break with nobody noticing | Consequence |
|---|---|
| The publish rate limit deleted | The only abuse control on an open, unauthenticated endpoint |
| The four-way director predicate flipped `\|\|` → `&&` | A follower hangs up on its own director — this was the round-5 fix of the previous campaign |
| `parseInboundPayload` force-unwrapping | Any peer in range crashes the app with one malformed packet |
| A late joiner's catch-up snapshot `.reliable` → `.unreliable` | That iPad silently stays on the wrong page |
| The songbook index truncated to 10 of 317 entries | 307 songs stop resolving |
| Every song remapped to page 1 | Every song jump and search result lands on page 1 |
| `clampPage` losing its upper bound | A jump past the end sticks on a 404 |
| `assets/songbook.pdf` truncated to zero bytes | The highest-stakes file in the repo |
| The two director fabs overlapping by 2rem | Unusable controls on the director's iPad |
| The BLE unchanged-page early return deleted | `advertSeq` climbs ~5,400 times a Mass; the advert restarts once a second |

Each was invisible for the same reason: **the assertion looked for a string, and the string still
existed somewhere else in the file.**

## 2. The three shapes this takes

Worth knowing by name, because they recur and they all look like coverage.

**A string-offset comparison standing in for containment.**
`assert.ok(src.indexOf(a) < src.indexOf(b))` passes *because* `a` was deleted — `indexOf` returns
`-1`, and `-1` sorts before everything. The assertion is satisfied by the absence of the thing it
is checking for.

**A window that silently becomes the whole file.** `src.slice(src.indexOf(start), src.indexOf(end))`
runs to EOF when `end` is missing, so a match anywhere in the file satisfies it. Five separate files
broke this way. The end marker is often a decorative comment banner: rewording
`// ── Sync "working" indicator` — a change with no behavioural meaning at all — disarmed a test that
otherwise had teeth. Character-count windows (`slice(i, i + 400)`) fail in both directions: an
inserted comment pushes the target out and reddens a correct build.

**A rule re-implemented inside the test.** The test computes the answer with its own copy of the
logic, so the source can be deleted outright and the assertions still pass. `transportBackoff` had
already diagnosed this in its own header and fixed it one level down, while the level above — a
hand-written model of the settle mechanism, fed a constant hardcoded in the test file — stayed.

## 3. What was built

**`scripts/mutation-sweep.mjs`** — derives syntax-preserving mutations from a source file, runs the
tests against each in a `mktemp` mirror, reports the survivors. Comment- and string-aware, so the
prose these files are mostly made of is not mutated. Nothing in the working tree is ever written to.

```bash
node scripts/mutation-sweep.mjs --source web/src/lib/svSyncDecision.js --test e2e/svSyncDecision.test.mjs
```

Survivors are **questions, not verdicts** — mutation testing produces equivalent mutants, changes
that alter the text without altering behaviour. The number to drive down is survivors on
load-bearing lines, not the percentage.

**`scripts/verify-behavioural-guards.mjs`** — replays all 19 regressions and requires each to redden
**the named test**, scoped with `--test-name-pattern` so a sibling assertion cannot take the credit.
Runs in 5 seconds; wired into CI. A SKIP is a failure there: a mutation whose pattern has drifted
looks exactly like coverage.

**`e2e/svSyncSystem.test.mjs`** — a discrete-event simulation of the whole relay path. It imports and
runs the real modules (`src/directorRelaySync.js` through a stubbed `fetch`,
`sync-worker/src/publishSeq.js`, `web/src/lib/svSyncDecision.js`) as a director, a relay room and N
followers on a virtual clock, with per-device clock skew, loss, jitter-driven reordering, handover
and restarts. The unit tests each cover one module alone; every bug that reached the congregation
lived in the seam between two modules that were each correct by themselves.

**`e2e/svSyncBoundaries.test.mjs`** — the gaps the sweep found in the two best-tested modules in the
repo. Chiefly the freshness window: *"a director is live for 90 seconds"* is decided independently
in three places that never speak to each other, and nothing checked they drew the line in the same
second.

**`e2e/swiftRules.test.mjs`** — the Swift mesh rules compiled and executed rather than grepped.
`e2e/helpers/swift-harness.mjs` slices the real function bodies out of `DirectorSyncModule.swift`,
compiles them against a minimal MultipeerConnectivity stand-in that decides nothing, and runs them.

## 4. Two rules that carried most of the weight

**Prove the harness can express failure before trusting anything it says.** `svSyncSystem` opens with
three vacuity guards — the simulated follower can demote, can follow, and the simulated relay can
refuse. `seq` is epoch milliseconds and `ts` is epoch seconds; a `ts` in the wrong unit is fresh
forever, so every freshness assertion in the file would pass while testing nothing. Fast, green, and
blind to the exact class of bug it exists to catch.

**Never let a test skip quietly.** A suite that stops running is indistinguishable from one that
passes. `swiftRules` fails rather than skips when the toolchain is missing, when an extraction anchor
has moved, and when the extracted code no longer compiles.

## 5. Measurements

Mutation coverage, before and after, measured with the sweeper:

| Module | Before | After |
|---|---|---|
| `sync-worker/src/publishSeq.js` | 79% | **89%** |
| `web/src/lib/svSyncDecision.js` | 72% | **83%** |
| `src/directorRelaySync.js` | 44% | **71%** |
| `DirectorSyncModule.swift` (extracted regions) | — | **81%** |

Suite: **570 pass / 0 fail / 2 skipped**, up from 517 at the branch point. Worker 60/60.
`verify-behavioural-guards` 19/19 caught. The three pre-existing guard scripts still 16/16, 11/11 and
6/6.

CI no longer runs a hand-maintained allowlist of 45 filenames. It enumerates `e2e/` and
`sync-worker/test/` and excludes exactly three files, each for a stated reason. A new test file runs
in CI the moment it lands rather than the moment somebody remembers to add a line — the earlier list
had silently stopped running 24 files, including all four that were red at the time.

## 6. A bug the simulation found

**A director that steps down still published one more page.** Publishes are coalesced latest-wins, so
a page turned while a request is in flight sits in `pending`; the drain in `doPublish`'s `finally`
posts it after the step-down. The gate existed only at `publishPageToRelay`'s entry.

`src/directorRelaySync.js:45-56` describes this exact failure and states the refusal moved to a local
gate. The drain went around it.

It was survivable by accident while `/publish` required a code — the straggler came back 401 and was
never applied. `/publish` has been open since 2026-08-06, so it now *succeeds*: the ex-director's page
lands on every signovivo.com follower after the congregation has been handed to somebody else, and
wins outright whenever the handover is quick enough that the new director has not published a higher
seq yet. Fixed on both paths.

## 7. What is still open

- **`e2e/pillWording.test.mjs` — the live stylesheet contradicts a test's claim.** `styles.css:2317`
  sets `display: none` for `html[data-role="follower"] .director-mode-badge.is-following
  .director-mode-badge-exit`, which wins on specificity for exactly the follower role the test is
  about, while the test asserted the control is dimmed rather than hidden. The test now reflects the
  cascade as it actually computes. **Which behaviour Miguel wants is a product decision, not a test
  decision** — flagging rather than choosing.

- **Handover latency is unbounded in clock skew.** Measured in simulation: director A stops, director
  B takes over with a −45 s clock, and nine consecutive publishes are refused — roughly 45 seconds of
  web followers frozen on A's page. `publishSeq.js:53-56` knowingly accepts this (a slow clock is left
  alone so it cannot gain an unearned advantage). It is capped by `maxAgeS` at 90 s. Worth a decision
  rather than a silent pin.

- **Not reachable from a test runner**, and still covered only by source anchors plus hardware runs:
  anything needing real MultipeerConnectivity or CoreBluetooth delivery, `DispatchQueue` timing, or a
  real radio. Concretely, `reserveSlot`'s expiry timer and the ABA token check inside it, the
  advertiser restart closure, and app.js's WebSocket transport loop (heartbeat, zombie/health
  watchdogs, backoff ladder, re-home budget, circuit breaker) — all of which live inline with no
  extraction point.

- **`e2e/eas-config.test.mjs`** has one test red outside CI (`npx expo config` with no
  `node_modules`). Pre-existing and environmental; the file stays excluded from CI for the reason
  already documented there.

## 8. How to keep this true

```bash
# Does this test file actually catch anything?
node scripts/mutation-sweep.mjs --source <source> --test <test>

# Do the 19 known regressions still redden the tests that name them?
node scripts/verify-behavioural-guards.mjs
```

When you add a test, mutate the thing it claims to protect and watch it go red. If it does not, the
test is decoration — and decoration is worse than nothing, because it occupies the space where a real
guard would have gone.
