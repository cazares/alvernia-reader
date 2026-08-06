import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

// The resume logic lives inside a React effect in PdfReaderApp.tsx and cannot be imported without a
// React Native runtime. These tests pin the DECISION RULE as pure logic, plus the source invariants
// that make the shipped version match it. That is deliberately less than end-to-end — what it buys
// is that the three guards can never be quietly dropped, and the guard that matters most (do not
// resume while somebody else is directing) has an executable definition rather than a comment.

const SRC = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const num = (name) => {
  const m = SRC.match(new RegExp(`const ${name} = ([^;]+);`));
  assert.ok(m, `${name} not found in PdfReaderApp.tsx`);
  // eslint-disable-next-line no-new-func
  return Function(`return (${m[1]})`)();
};

const RESUME_WINDOW = num("DIRECTOR_RESUME_WINDOW_MS");
const SETTLE = num("DIRECTOR_RESUME_SETTLE_MS");
const LIVE = num("LIVE_DIRECTOR_WINDOW_MS");
const THROTTLE = num("DIRECTOR_STAMP_THROTTLE_MS");

// A mirror of the shipped rule, and mirrors drift: the first version of this omitted the
// explicitTransmitterRef guard entirely, so deleting that guard from the app failed nothing. The
// test below pins the mirror against the real source so a new guard cannot appear on one side only.
const shouldResume = ({ prevRole, lastDirectorAt, now, roleNow, otherDirectorAt, transmitter = false, inFlight = false }) => {
  if (prevRole !== "director") return false;
  if (!(lastDirectorAt > 0) || now - lastDirectorAt > RESUME_WINDOW) return false;
  if (roleNow === "director" || transmitter || inFlight) return false;
  if (otherDirectorAt && now - otherDirectorAt < LIVE) return false; // two directors is worse
  return true;
};

test("the mirror above still lists every guard the shipped code applies", () => {
  const start = SRC.indexOf("resumeTimerRef.current = setTimeout");
  const body = SRC.slice(start, SRC.indexOf("}, DIRECTOR_RESUME_SETTLE_MS)", start));
  // Scope to the EARLY-RETURN guard specifically. Matching the whole callback let the
  // explicitTransmitterRef mutation slip through, because that symbol also appears further down in
  // the success-toast check — the assertion was satisfied by an entirely different use.
  const entry = body.slice(body.indexOf("if ("), body.indexOf("return;"));
  for (const [name, re] of [
    ["roleRef director", /roleRef\.current === "director"/],
    ["explicitTransmitterRef", /explicitTransmitterRef\.current/],
    ["becomeDirector in flight", /becomeDirectorInFlightRef\.current/],
  ]) {
    assert.match(entry, re, `the resume's early-return lost its ${name} guard`);
  }
  assert.match(body, /DIRECTOR_RESUME_WINDOW_MS/, "no freshness re-check when the timer fires");
  assert.match(body, /LIVE_DIRECTOR_WINDOW_MS/, "no live-other-director guard");
});

test("the success toast is conditional on the role ACTUALLY changing", () => {
  // becomeDirector never rejects — it catches its own failures and falls back to becomeFollower — so
  // an unconditional `.then(toast)` announces "you got the choir back" to a device sitting there as
  // a follower with nobody directing. The single most dangerous shape in this file: the one signal
  // that would send a human to fix it instead says everything is fine.
  const at = SRC.indexOf("void becomeDirector(DIRECTOR_CODE).then(");
  assert.ok(at > 0, "the resume no longer calls becomeDirector");
  // Slice to the end of the .then callback, not to the first "});" after the toast — the toast sits
  // inside an if/else, so that landmark fell short of the failure branch.
  const then = SRC.slice(at, SRC.indexOf("}, DIRECTOR_RESUME_SETTLE_MS)", at));
  const cond = then.slice(then.indexOf("if ("), then.indexOf("injectEvent"));
  assert.match(cond, /roleRef\.current === "director"/, "toast does not check the resulting role");
  assert.match(then, /standDown\(/, "no failure branch — a failed resume says nothing at all");
});

test("the device follows first, so it is never role-less while the mesh settles", () => {
  // The resume decision is deferred by DIRECTOR_RESUME_SETTLE_MS. Without becomeFollower() running
  // immediately, the device spends that whole window in "off": not directing, and not following the
  // director who may already have taken over — a silent hole with the choir on screen.
  const boot = SRC.slice(SRC.indexOf("didBootstrapRef.current = true"),
    SRC.indexOf("const sub = addNearbyDirectorSyncListener"));
  const fin = boot.slice(boot.indexOf(".finally("));
  assert.match(fin, /becomeFollower\(\)/, "boot no longer falls back to follower");
});

test("a device already acting as a relay transmitter does not resume", () => {
  assert.equal(shouldResume({ ...base, transmitter: true }), false);
});

test("a becomeDirector already in flight blocks the resume", () => {
  // roleRef is not assigned until AFTER the mesh start (which can sleep 2s and retry), so a takeover
  // confirmed by hand one second into the settle window is still pre-roleRef when the timer fires.
  assert.equal(shouldResume({ ...base, inFlight: true }), false);
});

const NOW = 1_700_000_000_000;
const base = { prevRole: "director", lastDirectorAt: NOW - 20_000, now: NOW, roleNow: "follower", otherDirectorAt: null };

test("a director who crashed seconds ago resumes", () => {
  assert.equal(shouldResume(base), true);
});

test("a device that was never director does not resume", () => {
  assert.equal(shouldResume({ ...base, prevRole: "follower" }), false);
  assert.equal(shouldResume({ ...base, prevRole: null }), false);
});

test("a cold start long after directing does not resume", () => {
  assert.equal(shouldResume({ ...base, lastDirectorAt: NOW - RESUME_WINDOW - 1 }), false);
  // Exactly at the boundary still resumes — the window is inclusive.
  assert.equal(shouldResume({ ...base, lastDirectorAt: NOW - RESUME_WINDOW }), true);
});

test("a missing timestamp does not resume", () => {
  // Devices that directed before this shipped have lastSyncRole but no lastDirectorAt. They must
  // fall through to the prompt, not resume on a 0 that reads as 1970.
  assert.equal(shouldResume({ ...base, lastDirectorAt: 0 }), false);
  assert.equal(shouldResume({ ...base, lastDirectorAt: NaN }), false);
});

test("NEVER resume while another device is directing", () => {
  // The failure this prevents is two directors publishing conflicting pages to the same mesh —
  // strictly worse than the frozen-choir problem the resume exists to fix.
  assert.equal(shouldResume({ ...base, otherDirectorAt: NOW - 1000 }), false);
  // A director who stopped broadcasting longer ago than the heartbeat window is gone, not live.
  assert.equal(shouldResume({ ...base, otherDirectorAt: NOW - LIVE - 1 }), true);
});

test("do not resume if the role was already retaken by hand", () => {
  assert.equal(shouldResume({ ...base, roleNow: "director" }), false);
});

test("the settle window outlasts mesh discovery AND the live-director window", () => {
  // This test previously asserted only `SETTLE >= 2000` while its own comment said settle must not
  // be shorter than the live window — and the shipped 3500 violated that, green, for hours. The
  // guard it feeds reads lastDirectorSnapshotRef, which is only ever written by a mesh `page` event
  // requiring a fully connected session, so deciding before discovery completes means deciding on
  // evidence that cannot exist yet: resume anyway, win on a newer token, and silently demote the
  // person who deliberately took over.
  assert.ok(SETTLE > LIVE, `settle ${SETTLE}ms must exceed the live-director window ${LIVE}ms`);
  // Swift's own budget, read from the module rather than restated here so it cannot drift.
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const selfDirected = Number(swift.match(/selfDirectedTimeoutSeconds: TimeInterval = ([\d.]+)/)[1]) * 1000;
  assert.ok(SETTLE >= selfDirected,
    `settle ${SETTLE}ms is under Swift's selfDirectedTimeout ${selfDirected}ms — a follower has not ` +
    `even concluded there is no director by then`);
  assert.ok(RESUME_WINDOW >= 60_000, "resume window must survive a slow device reboot");
  assert.ok(THROTTLE < RESUME_WINDOW, "the timestamp must refresh well inside the resume window");
});

test("the shipped source still applies the core guards", () => {
  // Scope to the RESUME block only. Slicing from the first lastDirectorAt match reached the whole
  // rest of the file, so LIVE_DIRECTOR_WINDOW_MS matched its OTHER use (the takeover warning) and
  // deleting the guard here failed nothing. Caught by mutation-testing this very assertion.
  const start = SRC.indexOf("didBootstrapRef.current = true");
  const block = SRC.slice(start, SRC.indexOf("const sub = addNearbyDirectorSyncListener", start));
  assert.match(block, /DIRECTOR_RESUME_WINDOW_MS/, "freshness guard missing");
  assert.match(block, /DIRECTOR_RESUME_SETTLE_MS/, "mesh-settle guard missing");
  assert.match(block, /LIVE_DIRECTOR_WINDOW_MS/, "live-other-director guard missing");
  assert.match(block, /becomeDirector\(DIRECTOR_CODE\)/, "resume never actually takes the role");
});

test("a soft reset cancels a pending resume", () => {
  // Otherwise the operator wipes the role and a timer promotes the device three seconds later.
  const start = SRC.indexOf("const performSoftReset");
  const reset = SRC.slice(start, SRC.indexOf("}, [", start));
  assert.match(reset, /resumeTimerRef\.current\)/, "performSoftReset does not clear the resume timer");
});

test("the boot path no longer blocks the screen with a modal", () => {
  const start = SRC.indexOf("didBootstrapRef.current = true");
  const boot = SRC.slice(start, SRC.indexOf("const sub = addNearbyDirectorSyncListener", start));
  assert.ok(!/Alert\.alert/.test(boot), "a blocking Alert is back on the boot path — use a notice");
});
