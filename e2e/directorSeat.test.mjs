import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const SRC = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");
const KEYS = fs.readFileSync("src/offlineBooks.ts", "utf8");
const num = (n) => Number(SRC.match(new RegExp(`const ${n} = (\\d+)`))[1]);

// ── Claiming an empty seat ────────────────────────────────────────────────────
// Nothing is enrolled. The iPad that keeps directing is the one that claims an empty seat, ranked
// by how often it has done so. It can never take an OCCUPIED seat — that guard is separate and
// covered in directorResume.test.mjs.

test("eligibility is habit, not recency — no time window on the habitual path", () => {
  // A window silently expires over a summer with no Mass, so the iPad that ran every Sunday for a
  // year would stop claiming the seat exactly when everyone had forgotten the manual path.
  const m = SRC.match(/const habitual = ([^;]+);/);
  assert.ok(m, "the habitual path is gone");
  assert.ok(!/DIRECTOR_RESUME_WINDOW_MS|Date\.now/.test(m[1]),
    `habitual eligibility is time-limited: ${m[1]}`);
  assert.match(m[1], /sessions > 0/, "eligibility is not based on having directed before");
});

test("a device that has never directed never claims a seat", () => {
  assert.match(SRC, /if \(!crashFresh && !habitual\) return;/, "ineligible devices are not turned away");
});

test("more experience claims sooner, and the spread is bounded", () => {
  const MAX = num("HABIT_MAX_EXTRA_MS"), STEP = num("HABIT_STEP_MS"), SETTLE = num("DIRECTOR_RESUME_SETTLE_MS");
  const extra = (sessions) => Math.max(0, MAX - sessions * STEP);
  // The regular director must beat a one-off substitute by enough that the substitute sees a live
  // director and stands down, rather than both claiming and fighting.
  assert.ok(extra(1) - extra(40) >= 2000, "the regular director barely out-ranks a one-time one");
  assert.equal(extra(1000), 0, "experience never stops helping");
  // ...but nobody waits absurdly long: the choir is sitting on the last page either way.
  assert.ok(SETTLE + MAX <= 20000, `worst-case claim is ${(SETTLE + MAX) / 1000}s`);
  // Monotonic: more sessions is never worse.
  for (let n = 1; n < 40; n += 1) assert.ok(extra(n) >= extra(n + 1), `not monotonic at ${n}`);
});

test("a crash resume is not penalised by the ranking", () => {
  // A crash is a continuation of a role already held, not a claim competing with other devices.
  const m = SRC.match(/const extraWait = crashFresh\s*\?\s*0/);
  assert.ok(m, "a crashed director now waits behind the experience ranking");
});

test("the tally is incremented wherever the role is actually taken", () => {
  // Both paths — mesh director and relay-only transmitter — or the ranking is wrong for one of them.
  assert.match(KEYS, /directorSessions: "sv\.sync\.directorSessions"/, "no storage key for the tally");
  assert.equal((SRC.match(/bumpDirectorSessions\(\);/g) || []).length, 2,
    "the tally is not bumped on both director paths");
});

test("upgrading devices are safe: absent tally means no claim", () => {
  // lastDirectorAt and the tally are both new. On the first boot of this build every device reads 0
  // and nothing auto-claims; it starts working after the first manual direct.
  const m = SRC.match(/const sessions = Number\(([^)]+)\)/);
  assert.match(m[1], /sessRaw \|\| 0/, "a missing tally does not read as 0");
});

// ── The pill ──────────────────────────────────────────────────────────────────

test("the pill shows nobody-directing ONLY on the mesh's own verdict", () => {
  // Inferring it from silence would light the warning during the ~10s of boot discovery on every
  // device every Sunday, and a warning that cries wolf is one nobody reads by the third week.
  const fn = APP.slice(APP.indexOf("const syncPillState"), APP.indexOf("const renderDirectorModeBadge"));
  assert.match(fn, /lastMeshStatus === "self-directed"/, "nobody-state is guessed, not reported");
  assert.match(fn, /return "following";\s*\};/, "the default is not the safe one");
});

test("the pill never appears on the public web", () => {
  const fn = APP.slice(APP.indexOf("const renderDirectorModeBadge"), APP.indexOf("// ── Sync \"working\""));
  assert.match(fn, /NATIVE_FILE_MODE \|\| hasNativeBridge\(\)/, "not gated to the native shell");
});

test("tapping the pill acts on whatever it is showing", () => {
  const h = APP.slice(APP.indexOf("if (directorModeBadge) directorModeBadge.addEventListener"));
  assert.match(h, /syncPillState\(\)/, "the tap ignores the displayed state");
  assert.match(h, /exit-director/, "a director cannot step down");
  assert.match(h, /request-director/, "a follower cannot take the role");
});
