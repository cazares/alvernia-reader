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

Mutation coverage, both columns measured with the **same, corrected** instrument — the "before"
column against the branch point `f2dffe4`:

| Module | Before | After |
|---|---|---|
| `sync-worker/src/publishSeq.js` | 31/41 = 76% | **37/41 = 90%** |
| `web/src/lib/svSyncDecision.js` | 70/104 = 67% | **86/104 = 83%** |
| `src/directorRelaySync.js` | 8/63 = **13%** | **46/65 = 71%** |
| `DirectorSyncModule.swift` (extracted regions) | 20/52 = 38% | **42/52 = 81%** |

> **An earlier version of this page published different numbers, and they were wrong.** The sweeper
> was scoring syntax-invalid mutants as "caught" — the parser was doing the tests' work — so every
> figure was inflated. `src/directorRelaySync.js` was at **13%**, not the 44% first reported. Section
> 9 has the details. The corrected numbers are above; the improvements are larger, not smaller.

Suite: **573 pass / 0 fail / 2 skipped**, up from 517 at the branch point. Worker 60/60.
`verify-behavioural-guards` 19/19 caught. The three pre-existing guard scripts still 16/16, 11/11 and
6/6.

CI no longer runs a hand-maintained allowlist of filenames. It enumerates `e2e/` and
`sync-worker/test/` and excludes exactly three files, each for a stated reason. A new test file runs
in CI the moment it lands rather than the moment somebody remembers to add a line.

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

## 9. The re-hunt — what this campaign broke

Fixes create bugs. This repo's own measured rate is that about a third of each round's findings are
defects in the previous round's fixes, so the work above was re-hunted adversarially: nine agents
looking for what it broke, then one independent skeptic per finding whose default was to refute.
**48 findings, 32 survived refutation.** The ones that mattered:

**The measuring instrument was inflating its own scores.** `mutation-sweep` scored *syntax-invalid*
mutants as "caught" — the exact false pass its header swore was impossible. The token scan restarts
at every byte, so at the second `=` of `===` it still saw `==` and emitted `a =!= b`; on Swift it
turned `-> Int` into `->= Int`. Neither compiles; the test process died on the parse error, and a
non-zero exit was scored identically to a real assertion failure. Its `node --check` guard was inert
too — it ran only for statement deletions, and for a bare `.js` under Node 22's automatic module
detection a file failing *both* parses still exits 0.

Every mutant is now parse-checked (`.mjs` then `.cjs` for JS, `xcrun swiftc -parse` for Swift) and a
failure is excluded from scoring rather than credited. Proven by construction: a test asserting only
`typeof clamp === "function"` now scores **0%** where it scored 11%; a real behavioural test of the
same function scores 75%. Four of the sixteen operator pairs were also unreachable — a table that
read like coverage and was not.

**`assertHarnessFidelity()` did not exist.** `sv-sync-sim.mjs`'s header named it twice as the only
thing keeping the harness from becoming fiction. Nobody had written it — this campaign's own thesis,
turned back on it. It exists now, and reads the relay's field assembly out of *both*
`sync-worker/src/index.ts` and the harness so drift on either side fails.

**The black-hole test could not catch its own outage.** It was three regexes plus an assertion
(`aborted === false`) that is satisfied *more* easily when the wiring breaks. It now intercepts the
abort timer instead of waiting it out and observes the drain — verified red on all three regressions
that reinstate the whole-Mass freeze.

**One repair would have blocked the repo's normal workflow.** The songbook check pinned
`assets/songbook.pdf` to `web/manifest-baseline.json`, which only a *production release* writes. Any
legitimate song splice would have redded CI with no way to green it before merge — and because
`verify-behavioural-guards` refuses to run on a red baseline, that one assertion would have taken all
nineteen guards with it. It now checks properties of the tree instead.

The rest were defects in the rewritten tests themselves: `fabLayout` was blind to `@media` (a
media-query override overlapped the two director controls by 2rem with 14/14 green); `pillWording`'s
cascade resolver swallowed its own deliberate throws, so a `:not()` or a `>` combinator vanished
rather than failing loudly; `nearby-sync` compared occurrence *counts* rather than the property;
`relayQuotaGuards`' new "no else branch" assertion was structurally incapable of firing. All fixed
and proven red-then-green, each also checked against a behaviour-neutral edit so it does not cry
wolf.

**What that says about the method.** Every one of these was found by the same discipline the campaign
is about — break it and see — applied to the campaign itself. None would have been visible from a
green run, and the two most serious were in the tools that produce the evidence. If you take one
thing from this document: measure the instrument before believing the measurement.

## 10. Rounds 3 and 4 — where text-based testing hits its ceiling

The re-hunt was itself re-hunted. Round 3 looked for what round 2's fixes broke and confirmed **24
defects**; round 4 fixed them. The distribution is the interesting part: a third were in the CSS
resolvers `fabLayout` and `pillWording`, and they **alternated between false negatives and false
positives** — a `!important` or an `#id` override slipping through, and a whitespace change or an
added comment reddening a correct build.

That alternation is a diagnosis, not a list of bugs. It is what a model too weak for its question
looks like from the outside. Those two files were being asked *"what does the browser actually
compute for this element?"*, and answering it properly means implementing the cascade — specificity
across a rule's whole selector list, `!important` as an origin rather than a specificity, custom
properties inheriting down the real ancestor chain, functional pseudos, at-rules. Every feature
modelled is another chance to be confidently wrong, and a confidently wrong "no match" is exactly
the defect being fixed.

**So round 4 stopped modelling and started refusing.** Where a reader cannot decide something
soundly it now calls `assert.fail`, naming the construct, the selector and the `styles.css` line.
A refusal is sound in a way a guess is not: it converts every false negative into a failure that
sends a person to look, and it writes the ceiling into the file instead of leaving it to be
rediscovered. Each file now says in its own header what it cannot decide and what would — for the
CSS ones, a real browser calling `getComputedStyle` on the live document; for the Swift ones, the
module on a device.

That is the honest end of this road. A test file can read source soundly, execute a module, or
compile and run extracted Swift — this branch does all three. What it cannot do is *be* the runtime.
Where the question is genuinely "what does the browser render", the next real step is a headless
browser in CI, not a better parser.

**A red from these files now has two meanings.** Either the code is wrong, or the file met a
construct it will not score — the message says which. That is deliberate. Every one of round 3's
defects here was a silence.
