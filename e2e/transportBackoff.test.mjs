import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

// THE M-F7 BACKOFF LADDER, WHICH HAD NEVER CLIMBED A SINGLE RUNG.
//
// DirectorSyncModule documents the intent plainly: "fast exponential backoff for the first 5
// failures … then a SLOW 45 s last-resort retry FOREVER — never give up permanently", and
// handleAppDidBecomeActive has a `> 5` branch that gives a device whose Local Network permission was
// just granted in Settings one more shot. Neither has ever run.
//
// Originally because the counter was reset on ATTEMPT, inside the same function the retry calls back
// into: it oscillated 0↔1 forever and a permission denial meant a 3 s teardown/rebuild loop for the
// whole Mass. Then, after that was fixed with a settle timer, because the settle scheduled by an
// attempt that ALREADY FAILED still fired during the wait once the delay grew past it — the count
// sawtoothed 1,2,3,reset,1,2,3,reset and stopped at the third rung.
//
// The arithmetic is replayed here rather than only grepped, because "the counter increments" and
// "the ladder actually reaches its last-resort tier" are different claims and only the second one
// matters.
//
// A third re-hunt found three ways the ladder could be broken again with this whole file still green:
// deleting the token guard inside noteTransportSettled (the half of the anti-sawtooth mechanism that
// makes the other half mean anything), turning `advertiserFailureCount += 1` into `= 1`, and swapping
// the two branches of invalidatePendingSettle so each transport cancelled the other's settle. All
// three were silences, not false alarms, so the fix is to read those three facts out of the Swift and
// feed them into the replay — and to refuse loudly wherever the source stops being readable.

const MODULE = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

// THE LADDER IS READ OUT OF THE SWIFT, NOT RESTATED HERE.
//
// This was a hand-copied JS constant fed only into the test's own model, so the entire M-F7 retry
// could have been deleted from DirectorSyncModule and every test in the repo would still have been
// green — a model of a ladder that no longer exists, proving a property of itself. The whole point
// of replaying the arithmetic instead of grepping for it is that "the ladder reaches its last-resort
// tier" is a claim about the shipped code.
const ladder = () => {
  const m = MODULE.match(
    /advertiserFailureCount <= (\d+)\s*\?\s*min\(([\d.]+) \* pow\(([\d.]+), Double\(self\.advertiserFailureCount - 1\)\), ([\d.]+)\)\s*:\s*([\d.]+)/,
  );
  assert.ok(m, "could not read the advertiser backoff ladder out of DirectorSyncModule.swift");
  const [, fastCount, base, factor, cap, lastResort] = m.map(Number);
  return { fastCount, base, factor, cap, lastResort };
};

const L = ladder();
const delayFor = (count) =>
  count <= L.fastCount ? Math.min(L.base * Math.pow(L.factor, count - 1), L.cap) : L.lastResort;

// WHAT THIS FILE IS AND IS NOT. It reads Swift as text and replays arithmetic in JavaScript. It is
// not a Swift compiler and it is not a device: the only thing that can truly decide "does the
// advertiser reach the 45 s tier" is DirectorSyncModule running on hardware with Local Network
// permission denied, with the failure counter logged. So every structural reader below is written to
// REFUSE — assert.fail, naming the construct and the line in DirectorSyncModule.swift — the moment
// it meets a shape it cannot score soundly, rather than quietly returning a "no" it has not earned.
// Round 3's findings in this file were all silence: a mechanism deleted, a `+= 1` turned into `= 1`,
// two branches swapped, and the suite stayed green. A loud refusal at least says which line to read.

const lineOf = (idx) => MODULE.slice(0, idx).split("\n").length;

// A copy of the module with string literals and comments blanked out to spaces of the SAME LENGTH,
// so every index still lines up with MODULE. Brace matching and structural regexes run against this;
// messages and line numbers still come from MODULE. Without it a brace inside a comment or a string
// silently mis-bounds a body — and a mis-bounded body is exactly how a presence grep starts matching
// text that has nothing to do with the code it claims to be reading.
assert.ok(!/"""|#"/.test(MODULE),
  "DirectorSyncModule.swift now contains a multi-line or raw Swift string literal. This file blanks " +
  "ordinary literals to keep its brace matching sound and cannot do that for a raw one — teach it " +
  "the new form before trusting anything below.");
const blank = (m) => m.replace(/[^\n]/g, " ");
const SCAN = MODULE
  .replace(/"(?:[^"\\\n]|\\.)*"/g, blank)
  .replace(/\/\*[\s\S]*?\*\//g, blank)
  .replace(/\/\/[^\n]*/g, blank);

/** Index of the `}` closing the `{` at `open`, or a refusal. */
const closeOf = (text, open, what) => {
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    if (text[i] === "{") depth += 1;
    else if (text[i] === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return assert.fail(`${what}: the block opened here is never closed — refusing to read past it.`);
};

/**
 * Slice a Swift declaration's body by BRACE MATCHING, not by a character count and not to the next
 * comment banner. A count-based window drifts the moment somebody adds a line above it, and a window
 * whose end marker was deleted silently runs to EOF and matches anything in the file.
 */
const bodyOf = (decl) => {
  const start = SCAN.indexOf(decl);
  assert.ok(start > 0, `${decl} is gone from DirectorSyncModule.swift`);
  const open = SCAN.indexOf("{", start);
  assert.ok(open > start, `could not find the opening brace of ${decl}`);
  const end = closeOf(SCAN, open, `${decl} (line ${lineOf(start)})`);
  return SCAN.slice(open + 1, end);
};

/**
 * Read a Swift `if <cond> { … } else { … }` out of `text`, or refuse. Returns the two branch bodies
 * plus whatever text surrounds the statement, so a caller can check that nothing ELSE in the
 * function touches the state it is about to reason over.
 *
 * Refusing here is the whole point. Round 3's defect in invalidatePendingSettle was that the three
 * assertions over its body were pure presence greps — swap the two branches and every substring is
 * still present, so the advertiser's failure bumped the BROWSER's token and the file stayed green.
 * A presence grep cannot express "this token belongs to that branch"; reading the branches apart can.
 */
const ifElse = (text, cond, what) => {
  const m = new RegExp(`\\bif\\s+${cond}\\s*\\{`).exec(text);
  if (!m) {
    assert.fail(`${what}: no \`if ${cond} { … }\` here any more. This file reads the two transports ` +
      `apart by that branch; it cannot score whatever replaced it. Read it by hand.`);
  }
  const openYes = m.index + m[0].length - 1;
  const closeYes = closeOf(text, openYes, what);
  const em = /^\s*else\s*\{/.exec(text.slice(closeYes + 1));
  if (!em) {
    assert.fail(`${what}: \`if ${cond}\` is no longer followed by a plain \`else { … }\` (an ` +
      `\`else if\`, a guard, or an early return would all land here). Refusing to guess which ` +
      `transport the second path belongs to.`);
  }
  const openNo = closeYes + 1 + em[0].length - 1;
  const closeNo = closeOf(text, openNo, what);
  return {
    yes: text.slice(openYes + 1, closeYes),
    no: text.slice(openNo + 1, closeNo),
    rest: text.slice(0, m.index) + text.slice(closeNo + 1),
  };
};

// THE MODEL'S ONE FREE VARIABLE IS READ OUT OF THE SWIFT.
//
// invalidatePendingSettle was previously covered only by presence greps for its CALL SITES, so its
// body could be emptied to `_ = isAdvertiser` — declaration intact, both call sites intact — and
// every test in this file stayed green while the device went straight back to the 1,2,3,reset
// sawtooth. The behaviour the function has to have is that it BUMPS the attempt token of the
// transport that failed, which is what makes the already-doomed settle fail its `token ==` guard.
// So the replay below takes that fact from the shipped source instead of from a hand-set boolean:
// gut the body and the model sawtooths, and the ladder assertions go red the way the device did.
//
// Round 3 then showed the presence greps could not tell the two transports apart: swapping the two
// branches of the one-line if leaves all three substrings in place, so the advertiser's failure bumps
// the BROWSER's token, the advertiser's doomed settle stays armed, the sawtooth is back, and the file
// stayed green. The branches are now read apart, and the advertiser replay below is driven by the
// ADVERTISER branch specifically.
//
// Every structural read below is LAZY, and deliberately so: a refusal is a first-class outcome here,
// and one construct this file cannot score should fail the test that asks about it, not blind the
// other nine by throwing while the module is still loading.
const lazy = (f) => {
  let value;
  let done = false;
  return () => {
    if (!done) { value = f(); done = true; }
    return value;
  };
};

const INVALIDATE = lazy(() => {
  const parsed = ifElse(bodyOf("private func invalidatePendingSettle"), "isAdvertiser",
    "invalidatePendingSettle");
  if (parsed.rest.trim() !== "") {
    assert.fail("invalidatePendingSettle now does something besides its `if isAdvertiser { … } else " +
      `{ … }\` — leftover statements: \`${parsed.rest.trim()}\`. If that addition is harmless, ` +
      "teach this file about it; it will not assume so on its own, because the last two rounds of " +
      "this function's regressions both looked harmless.");
  }
  return parsed;
});
const SWIFT_INVALIDATES_ON_FAILURE = () => /\badvertiserAttemptToken\s*&\+=\s*1/.test(INVALIDATE().yes);

// The settle window is Swift's too — a duplicated `settleSeconds = 10` in this file would keep the
// model at 10 s while the shipped constant moved, which is the same class of lie as the ladder was.
const SETTLE_SECONDS = (() => {
  const m = MODULE.match(
    /private static let transportSettleSeconds:\s*TimeInterval\s*=\s*([\d.]+)/,
  );
  assert.ok(m, "could not read transportSettleSeconds out of DirectorSyncModule.swift");
  return Number(m[1]);
})();

// THE OTHER HALF OF THE ANTI-SAWTOOTH MECHANISM, WHICH NOTHING WAS READING.
//
// invalidatePendingSettle bumping the attempt token only MEANS anything because noteTransportSettled's
// scheduled block re-checks `self.advertiserAttemptToken == token` before zeroing the counter. Round 3
// deleted that equality check — so a settle scheduled by an attempt that had already failed fired
// anyway and zeroed the counter — and every test in this file stayed green. That is the whole M-F7
// bug, restored, invisibly. The gate is now read structurally and fed into the replay, so removing it
// sawtooths the model exactly as it sawtoothed the device.
const SETTLED = lazy(() => {
  const parsed = ifElse(bodyOf("private func noteTransportSettled"), "isAdvertiser",
    "noteTransportSettled");
  if (/FailureCount|AttemptToken/.test(parsed.rest)) {
    assert.fail("noteTransportSettled touches a failure counter or an attempt token OUTSIDE its " +
      "`if isAdvertiser { … } else { … }`. This file reads the two transports apart by that branch " +
      "and cannot say which one the stray statement belongs to — read it by hand.");
  }
  return parsed;
});

/**
 * Does this branch of noteTransportSettled zero its counter ONLY when the attempt token still
 * matches? Returns false when the structure is recognisable and the gate is simply missing; refuses
 * when the structure is not something this file can score.
 */
const settleGate = (branch, tokenVar, counterVar, what) => {
  if (!new RegExp(`let\\s+token\\s*=\\s*(self\\.)?${tokenVar}\\b`).test(branch)) {
    assert.fail(`${what}: no \`let token = ${tokenVar}\` — \`token\` no longer names this ` +
      `transport's attempt, so this file cannot tell what the scheduled block compares. Read it by hand.`);
  }
  const resets = [...branch.matchAll(new RegExp(`\\b${counterVar}\\s*=\\s*0\\b`, "g"))];
  if (resets.length !== 1) {
    assert.fail(`${what}: expected exactly one \`${counterVar} = 0\`, found ${resets.length}. ` +
      `Refusing to guess which one the settle timer runs.`);
  }
  const resetIdx = resets[0].index;
  const guards = [...branch.matchAll(/\bguard\b/g)];
  if (guards.length === 0) {
    return { ok: false, why: `nothing guards \`${counterVar} = 0\` at all` };
  }
  if (guards.length !== 1) {
    assert.fail(`${what}: ${guards.length} \`guard\`s in this branch. Refusing to guess which one ` +
      `gates \`${counterVar} = 0\`.`);
  }
  const gIdx = guards[0].index;
  if (gIdx > resetIdx) return { ok: false, why: `\`${counterVar} = 0\` runs BEFORE the guard` };
  const em = /\belse\s*\{/.exec(branch.slice(gIdx));
  if (!em) assert.fail(`${what}: the guard has no \`else { … }\` this file can bound. Read it by hand.`);
  const condition = branch.slice(gIdx + "guard".length, gIdx + em.index);
  const closeIdx = closeOf(branch, gIdx + em.index + em[0].length - 1, what);
  if (closeIdx > resetIdx) {
    assert.fail(`${what}: \`${counterVar} = 0\` now sits INSIDE the guard's else block, which is ` +
      `the opposite of a gate. Read it by hand.`);
  }
  let depth = 0;
  for (let i = closeIdx + 1; i < resetIdx; i++) {
    if (branch[i] === "{") depth += 1;
    else if (branch[i] === "}" && --depth < 0) {
      return { ok: false, why: `\`${counterVar} = 0\` has escaped the scope the guard returns from` };
    }
  }
  const ok = new RegExp(`self\\.${tokenVar}\\s*==\\s*token\\b`).test(condition);
  return { ok, why: ok ? "" : `the guard does not compare \`self.${tokenVar}\` against the attempt's token` };
};

const ADV_SETTLE_GATE = lazy(() => settleGate(SETTLED().yes, "advertiserAttemptToken",
  "advertiserFailureCount", "noteTransportSettled's advertiser branch"));
const BRW_SETTLE_GATE = lazy(() => settleGate(SETTLED().no, "browserAttemptToken",
  "browserFailureCount", "noteTransportSettled's browser branch"));

// THE COUNTER'S STEP IS READ OUT OF THE SWIFT, NOT ASSUMED TO BE +1.
//
// Round 3 changed `advertiserFailureCount += 1` to `= 1` — the ladder pinned to its first rung
// forever, a failing radio retrying every 3 s for the whole Mass, which is the ORIGINAL outage this
// file documents — and nothing noticed, because the increment was never read out of the source. The
// replay now takes its step from here, so the pinned counter shows up as a ladder that never climbs.
const counterStep = (body, counterVar, what) => {
  const muts = [...body.matchAll(new RegExp(`\\bself\\.${counterVar}\\s*(\\+=|-=|=(?!=))\\s*([^\\n;]+)`, "g"))];
  if (muts.length !== 1) {
    assert.fail(`${what}: expected exactly one mutation of ${counterVar}, found ${muts.length}. ` +
      `Refusing to guess which one sets the rung the backoff delay is computed from.`);
  }
  const [, op, rhs] = muts[0];
  const operand = rhs.trim();
  if (op === "+=" && operand === "1") return { accumulates: true, step: 1 };
  if (op === "=" && /^\d+$/.test(operand)) return { accumulates: false, pinnedTo: Number(operand) };
  assert.fail(`${what}: \`${counterVar} ${op} ${operand}\` is a form this file cannot replay — it ` +
    `models a counter that either accumulates by a literal or is pinned to one. Read it by hand.`);
};

const ADV_STEP = lazy(() => counterStep(
  bodyOf("func advertiser(_ advertiser: MCNearbyServiceAdvertiser, didNotStartAdvertisingPeer"),
  "advertiserFailureCount", "didNotStartAdvertisingPeer"));
const BRW_STEP = lazy(() => counterStep(
  bodyOf("func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers"),
  "browserFailureCount", "didNotStartBrowsingForPeers"));

test("the ladder the model replays is the one the Swift actually computes", () => {
  // Pins the shape too, so a change to the constants is a deliberate, visible edit rather than a
  // silent drift the model would happily follow into nonsense.
  assert.deepEqual(L, { fastCount: 5, base: 3, factor: 2, cap: 30, lastResort: 45 },
    "the backoff constants changed — update the expectations here and confirm the intent");
  // The browser must use the same ladder; the file's comment says "same as the advertiser".
  assert.match(MODULE, /browserFailureCount <= 5\s*\?\s*min\(3\.0 \* pow\(2\.0, Double\(self\.browserFailureCount - 1\)\), 30\.0\)\s*:\s*45\.0/,
    "the browser ladder has drifted from the advertiser's");
});

/// How long after a launch attempt the didNotStart callback lands. MPC reports a Local Network
/// permission denial essentially at once, so this is small — but it is NOT zero, and the gap matters:
/// a settle window shorter than it would fire before the failure it is supposed to be cancelled by.
const FAILURE_LATENCY = 0.1;

/**
 * Replay a transport that fails on every attempt.
 * `invalidateOnFailure` models whether a failure cancels the settle its attempt scheduled; it
 * defaults to what invalidatePendingSettle's body in the Swift actually does, and the settle window
 * defaults to the Swift's transportSettleSeconds.
 *
 * The clock is advanced explicitly and the settle is given a chance to fire at EVERY point the real
 * main queue would reach it — both while waiting for didNotStart and during the backoff wait that
 * follows. The earlier model only checked it during the backoff wait, applying the failure (and its
 * token bump) unconditionally first, which made the whole replay blind to transportSettleSeconds on
 * the shipped path: the Swift constant could be set to 0 — a transport pronounced healthy the
 * instant it launches, so the counter can never leave rung 1 — and every test here stayed green. The
 * sawtooth is a RELATIONSHIP between that constant and the ladder delays, so a model that cannot
 * feel the constant is not testing the relationship.
 *
 * Two more of its free variables are now the Swift's too, both because round 3 removed the thing
 * they stand for and watched nothing happen: whether noteTransportSettled's scheduled block still
 * checks the attempt token before zeroing the counter (`settleChecksToken`), and by how much a
 * failure moves the counter (`step`). Delete the token check, or pin the counter with `= 1`, and the
 * ladder assertions below go red the way the device did.
 */
const replay = ({
  invalidateOnFailure = SWIFT_INVALIDATES_ON_FAILURE(),
  settleChecksToken = ADV_SETTLE_GATE().ok,
  step = ADV_STEP(),
  settleSeconds = SETTLE_SECONDS,
  rounds = 8,
} = {}) => {
  let count = 0;
  let token = 0;
  let settle = null;
  const counts = [];
  // Run the main queue forward to `now`: a settle whose deadline has passed fires, and zeroes the
  // counter, unless a newer attempt has already superseded its token — which the settle only notices
  // because noteTransportSettled re-reads the token inside the block. Without that guard the settle
  // is unconditional, so an already-doomed attempt zeroes the counter anyway.
  const advanceTo = (now) => {
    if (settle && settle.at <= now && (!settleChecksToken || settle.token === token)) {
      count = 0; // the settle fired and declared the transport healthy
      settle = null;
    }
  };
  const attempt = (at) => {
    token += 1;
    settle = { at: at + settleSeconds, token };
  };
  const fail = (at) => {
    advanceTo(at); // the settle may already have fired while we waited for didNotStart
    count = step.accumulates ? count + step.step : step.pinnedTo;
    if (invalidateOnFailure) token += 1;
    counts.push(count);
    return at + delayFor(count);
  };
  attempt(0);
  let next = fail(FAILURE_LATENCY);
  for (let i = 0; i < rounds; i++) {
    advanceTo(next); // …and it may fire during the backoff wait, which is the original sawtooth
    attempt(next);
    next = fail(next + FAILURE_LATENCY);
  }
  return counts;
};

test("the settle window and the ladder are in the relationship the fix assumes", () => {
  // Both halves of the M-F7 bug live in this one comparison, so it is asserted directly rather than
  // left implicit in the replay: the window has to outlast the failure callback (or the counter can
  // never leave the first rung at all, and invalidatePendingSettle protects nothing), and some rung
  // has to grow past the window (or the settle from an already-doomed attempt could never fire
  // during the wait, and the sawtooth this file exists to prevent could not happen). Moving
  // transportSettleSeconds outside that band is a real design change and should be a visible one.
  assert.ok(SETTLE_SECONDS > FAILURE_LATENCY,
    `transportSettleSeconds is ${SETTLE_SECONDS}s, no longer than the ${FAILURE_LATENCY}s a ` +
    "didNotStart takes to land — every launch is pronounced healthy before it can be reported " +
    "failed, so the counter is pinned to rung 1 and the whole ladder is dead again");
  const rungs = [1, 2, 3, 4, 5, 6].map(delayFor);
  assert.ok(rungs.some((d) => d > SETTLE_SECONDS),
    `no rung of the ladder (${rungs.join(", ")}) outlasts the ${SETTLE_SECONDS}s settle window — ` +
    "the sawtooth this file models can no longer occur, so confirm the change was deliberate");
});

test("invalidatePendingSettle bumps the attempt token of the transport that FAILED", () => {
  // Named separately from the replay so an emptied body reports itself in one line instead of only
  // as a strange arithmetic failure downstream. The operator matters, not just the operands: the
  // guard inside noteTransportSettled is `self.advertiserAttemptToken == token`, so anything short
  // of actually CHANGING the token leaves the doomed settle armed.
  //
  // And so does the ASSOCIATION, which is what round 3 found missing: the three assertions here were
  // presence greps over the whole body, so swapping the two branches — the advertiser's failure
  // bumping the browser's token and vice versa — left every substring in place and every test green,
  // with both doomed settles still armed. Each branch is now checked for its own token and, just as
  // importantly, for the ABSENCE of the other's.
  assert.match(INVALIDATE().yes, /\badvertiserAttemptToken\s*&\+=\s*1/,
    "the isAdvertiser branch does not bump the advertiser's attempt token — its settle stays armed");
  assert.doesNotMatch(INVALIDATE().yes, /\bbrowserAttemptToken\b/,
    "the isAdvertiser branch touches the BROWSER's token — an advertiser failure is cancelling the " +
    "wrong transport's settle, so the advertiser ladder sawtooths and the browser's is silenced");
  assert.match(INVALIDATE().no, /\bbrowserAttemptToken\s*&\+=\s*1/,
    "the else branch does not bump the browser's attempt token — its settle stays armed");
  assert.doesNotMatch(INVALIDATE().no, /\badvertiserAttemptToken\b/,
    "the else branch touches the ADVERTISER's token — the two transports are crossed");
  assert.ok(SWIFT_INVALIDATES_ON_FAILURE());
});

test("noteTransportSettled zeroes the counter ONLY for the attempt it was scheduled by", () => {
  // The other half of M-F7, and the half nothing was reading. invalidatePendingSettle bumping the
  // token is inert unless the scheduled block re-checks it: without the `==` guard a settle armed by
  // an attempt that has already failed fires anyway, zeroes the counter mid-wait, and the ladder is
  // back to 1,2,3,reset. Round 3 deleted exactly that comparison and this file stayed green.
  assert.ok(ADV_SETTLE_GATE().ok,
    `the advertiser's settle no longer honours its attempt token — ${ADV_SETTLE_GATE().why}. A settle ` +
    "scheduled by an attempt that has since failed will zero advertiserFailureCount anyway, and the " +
    "sawtooth this whole file exists to prevent is back on the device.");
  assert.ok(BRW_SETTLE_GATE().ok,
    `the browser's settle no longer honours its attempt token — ${BRW_SETTLE_GATE().why}.`);
});

test("a failure ACCUMULATES on the counter instead of pinning it to one rung", () => {
  // `advertiserFailureCount = 1` in place of `+= 1` reads as a harmless simplification and is the
  // original outage restored: the ladder never leaves rung one, so a radio that cannot start is torn
  // down and rebuilt every 3 s for the whole Mass. Round 3 made that edit and every test passed,
  // because the increment was never read out of the Swift. It now drives the replay as well.
  assert.ok(ADV_STEP().accumulates,
    `didNotStartAdvertisingPeer pins advertiserFailureCount to ${ADV_STEP().pinnedTo} instead of ` +
    "incrementing it — the backoff delay is computed from that counter, so the ladder is stuck on " +
    "its first rung forever and the 45 s last-resort tier is unreachable again");
  assert.ok(BRW_STEP().accumulates,
    `didNotStartBrowsingForPeers pins browserFailureCount to ${BRW_STEP().pinnedTo} instead of ` +
    "incrementing it — the browser ladder is stuck on its first rung");
});

test("a failed attempt cancels the settle it scheduled, or the ladder sawtooths forever", () => {
  // What the old version of this test missed: both replays were hand-parameterised, so the whole
  // assertion was a statement about a boolean literal written three lines above it. It is now
  // SWIFT_INVALIDATES_ON_FAILURE that drives the second replay, so gutting invalidatePendingSettle's
  // body — which is exactly what regressed on the device — reddens this test.
  const broken = replay({ invalidateOnFailure: false });
  assert.ok(Math.max(...broken) <= 3,
    "the pre-fix model should stall at the third rung — if not, this test no longer models the bug");

  const shipped = replay();
  assert.ok(Math.max(...shipped) > 5,
    "the failure count still never exceeds 5 — the '> 5' foreground permission-recovery branch stays dead code");
  assert.deepEqual(shipped.slice(0, 7), [1, 2, 3, 4, 5, 6, 7],
    "the ladder no longer climbs monotonically — a failure is not cancelling the settle it scheduled");
});

test("the ladder reaches the 45 s last-resort tier and stays there", () => {
  const counts = replay();
  const delays = counts.map(delayFor);
  assert.deepEqual(delays.slice(0, 6), [3, 6, 12, 24, 30, 45], "the documented ladder is not what is computed");
  assert.ok(delays.slice(6).every((d) => d === 45), "the last-resort tier is not sustained");
});

test("the counter is reset by a transport that SURVIVES, never by launching one", () => {
  // The original bug: startAdvertising/startBrowsing zeroed the counter, and the retry path calls
  // straight back into them.
  const adv = MODULE.slice(MODULE.indexOf("private func startAdvertising"), MODULE.indexOf("/// How long a transport must stay up"));
  assert.doesNotMatch(adv, /advertiserFailureCount = 0/,
    "startAdvertising zeroes the counter again — the retry path will reset it before it can climb");
  assert.match(adv, /noteTransportSettled\(advertiser: true\)/, "no settle scheduled for the advertiser");

  const brw = MODULE.slice(MODULE.indexOf("private func startBrowsing"), MODULE.indexOf("// Adaptive discovery refresh"));
  assert.doesNotMatch(brw, /browserFailureCount = 0/, "startBrowsing zeroes the counter again");
  assert.match(brw, /noteTransportSettled\(advertiser: false\)/, "no settle scheduled for the browser");

  // The reset belongs to the settle, and only the settle.
  const settleStart = MODULE.indexOf("private func noteTransportSettled");
  const settle = MODULE.slice(settleStart, MODULE.indexOf("private func startBrowsing", settleStart));
  assert.ok(settle.length > 0, "could not bound noteTransportSettled");
  assert.match(settle, /advertiserFailureCount = 0/, "the settle no longer resets the advertiser counter");
  assert.match(settle, /browserFailureCount = 0/, "the settle no longer resets the browser counter");
});

test("both didNotStart handlers invalidate their pending settle", () => {
  for (const [marker, end, flag] of [
    ["didNotStartAdvertisingPeer error", "// MARK: - MCNearbyServiceBrowserDelegate", "true"],
    ["didNotStartBrowsingForPeers error", "// MARK: - MCSessionDelegate", "false"],
  ]) {
    const body = MODULE.slice(MODULE.indexOf(marker), MODULE.indexOf(end, MODULE.indexOf(marker)));
    assert.ok(body.length > 0, `could not bound ${marker}`);
    assert.match(body, new RegExp(`invalidatePendingSettle\\(advertiser: ${flag}\\)`),
      `${marker} leaves its settle armed — the counter will be zeroed mid-wait`);
  }
});

test("both didNotStart handlers run on the main queue", () => {
  // They mutate the failure counters and read currentRole, which every other delegate callback in
  // this file touches only on main.
  for (const [marker, end] of [
    ["func advertiser(_ advertiser: MCNearbyServiceAdvertiser, didNotStartAdvertisingPeer", "// MARK: - MCNearbyServiceBrowserDelegate"],
    ["func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers", "// MARK: - MCSessionDelegate"],
  ]) {
    const start = MODULE.indexOf(marker);
    assert.ok(start > 0, `${marker} is gone`);
    const body = MODULE.slice(start, MODULE.indexOf(end, start));
    const brace = body.indexOf("{");
    assert.match(body.slice(brace, brace + 400), /DispatchQueue\.main\.async/,
      `${marker} mutates shared state off MPC's delegate queue`);
  }
});
