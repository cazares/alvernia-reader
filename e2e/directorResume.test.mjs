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

// Mirror of the shipped decision. Kept in one place so the assertions below read as intent.
const shouldResume = ({ prevRole, lastDirectorAt, now, roleNow, otherDirectorAt }) => {
  if (prevRole !== "director") return false;
  if (!(lastDirectorAt > 0) || now - lastDirectorAt > RESUME_WINDOW) return false;
  if (roleNow === "director") return false; // someone re-took it by hand while we settled
  if (otherDirectorAt && now - otherDirectorAt < LIVE) return false; // two directors is worse
  return true;
};

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

test("the settle window is long enough for the mesh to out-race it", () => {
  // The decision is made SETTLE ms after boot, and a live director broadcasts every ~1s. If settle
  // were shorter than the live window, a peer could still be undiscovered when we decide and we
  // would resume into a second director.
  assert.ok(SETTLE >= 2000, `settle ${SETTLE}ms is too short for mesh discovery`);
  assert.ok(RESUME_WINDOW >= 60_000, "resume window must survive a slow device reboot");
  assert.ok(THROTTLE < RESUME_WINDOW, "the timestamp must refresh well inside the resume window");
});

test("the shipped source still applies all three guards", () => {
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
