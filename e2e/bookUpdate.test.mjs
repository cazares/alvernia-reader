// src/bookUpdate.js — the songbook downloader.
//
// Every veto here is the only thing standing between "a new PDF reaches the choir" and "eight
// iPads swap their songbook during Mass, in a building with no internet and no remedy". The tests
// that matter most are the ones asserting a REFUSAL.
import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import {
  parseBookUpdate,
  staggerDelayMs,
  shouldStage,
  verifyStaged,
  canApplyNow,
  levenshtein,
  stageBook,
  applyStagedBundle,
  ALLOWED_HOSTS,
  STAGED_READY_TTL_MS,
  LIVE_INTERNET_WINDOW_MS,
  ROOM_ACTIVE_WINDOW_MS,
  MIN_CODE_DISTANCE,
} from "../src/bookUpdate.js";

const BV = "bv_0123456789abcdef";

// ─── The pointer is DATA, not an instruction ────────────────────────────────

test("accepts a well-formed pointer on an allowed host", () => {
  assert.deepEqual(parseBookUpdate({ bookUpdate: { bookVersion: BV, base: "https://signovivo.com" } }), {
    bookVersion: BV,
    base: "https://signovivo.com",
  });
});

test("REFUSES a host the app was not built to trust", () => {
  // A compromised or simply buggy worker must never be able to aim a device at another origin.
  for (const base of [
    "https://evil.example.com",
    "https://signovivo.com.evil.example",
    "https://not-signovivo.com",
    "http://signovivo.com", // plain http
    "//signovivo.com",
    "",
  ]) {
    assert.equal(parseBookUpdate({ bookUpdate: { bookVersion: BV, base } }), null, `accepted ${base}`);
  }
});

test("the allowlist is a baked constant, not something read from a response", () => {
  assert.deepEqual(ALLOWED_HOSTS, ["signovivo.com", "alvernia-reader.pages.dev"]);
});

test("REFUSES a malformed bookVersion", () => {
  for (const v of ["", "bv_", "bv_XYZ", "bv_0123456789ABCDEF", "bv_0123456789abcde", "../../etc"]) {
    assert.equal(parseBookUpdate({ bookUpdate: { bookVersion: v, base: "https://signovivo.com" } }), null, v);
  }
});

test("an absent bookUpdate field is the DORMANT case and yields null, never a throw", () => {
  for (const r of [null, undefined, {}, { bookUpdate: null }, "nope", 42]) {
    assert.equal(parseBookUpdate(r), null);
  }
});

// ─── Stagger: eight identical devices must not arrive together ──────────────

test("stagger is deterministic per device and inside the window", () => {
  const a = staggerDelayMs("device-a");
  assert.equal(a, staggerDelayMs("device-a"), "must be reproducible — no RNG");
  assert.ok(a >= 0 && a < 20 * 60_000);
});

test("stagger actually SPREADS devices rather than mapping them all to one slot", () => {
  const slots = new Set(Array.from({ length: 40 }, (_, i) => staggerDelayMs(`ipad-${i}`)));
  assert.ok(slots.size >= 8, `only ${slots.size} distinct slots — the AP would still be hammered`);
});

// ─── shouldStage ────────────────────────────────────────────────────────────

// No `role` key: it is not a parameter of either gate any more (owner decision, 2026-08-03).
const stageCtx = (over = {}) => ({
  killSwitch: false, bookVersion: BV, activeBookVersion: "bv_ffffffffffffffff",
  stagedBookVersion: null, stagedReady: false, stagedReadyAt: null, quarantine: [], webReady: true,
  foreground: true, firstSeenAt: 0, deviceId: "d", now: 21 * 60_000,
  minShellBuild: 1, shellBuild: 384, ...over,
});

test("stages in the ordinary case", () => {
  assert.equal(shouldStage(stageCtx()).stage, true);
});

test("REFUSES on the boot path — staging must never compete with the reader for I/O", () => {
  assert.equal(shouldStage(stageCtx({ webReady: false })).reason, "not-web-ready");
});

test("EVERY role downloads, the DIRECTOR included (owner decision, 2026-08-03)", () => {
  // Reverses "the DIRECTOR's iPad never downloads". Role is no longer an input, so a role that
  // somehow reaches this function must be ignored rather than obeyed — an update that skips the
  // device leading the room splits the fleet across two songbooks.
  for (const role of ["director", "follower", "off", null, undefined]) {
    const r = shouldStage(stageCtx({ role }));
    assert.equal(r.stage, true, `role ${String(role)} was refused: ${r.reason}`);
    assert.equal(r.reason, "ok");
  }
});

test("the build-baked kill switch stops everything, first — for the director too", () => {
  assert.equal(shouldStage(stageCtx({ killSwitch: true })).reason, "kill-switch");
  assert.equal(shouldStage(stageCtx({ killSwitch: true, role: "director" })).reason, "kill-switch");
});

test("the NON-role staging gates still refuse a director's iPad like anyone else's", () => {
  // Requirement of the same decision: roles came out, TIMING and CAPABILITY did not.
  const d = (over) => shouldStage(stageCtx({ role: "director", ...over })).reason;
  assert.equal(d({ webReady: false }), "not-web-ready");
  assert.equal(d({ foreground: false }), "background");
  assert.equal(d({ minShellBuild: 999 }), "shell-too-old");
  assert.equal(d({ quarantine: [{ bookVersion: BV, failures: 3 }] }), "quarantined");
  assert.equal(d({ deviceId: "slow", firstSeenAt: 0, now: 0 }), "stagger-waiting");
});

test("REFUSES a book this shell is too old to run", () => {
  assert.equal(shouldStage(stageCtx({ minShellBuild: 999 })).reason, "shell-too-old");
});

test("REFUSES a quarantined book — it is not re-downloaded to fail again", () => {
  assert.equal(
    shouldStage(stageCtx({ quarantine: [{ bookVersion: BV, failures: 3 }] })).reason,
    "quarantined",
  );
});

test("does not re-stage what is already active or already staged-and-ready", () => {
  assert.equal(shouldStage(stageCtx({ activeBookVersion: BV })).reason, "already-active");
  assert.equal(
    shouldStage(stageCtx({ stagedBookVersion: BV, stagedReady: true, stagedReadyAt: 21 * 60_000 })).reason,
    "already-staged",
  );
});

test("REFUSES a bookVersion that is not one — the staging gate re-checks, it does not trust its caller", () => {
  // Previously unpinned by ANY test: parseBookUpdate is the only other place this is validated, and
  // a second caller (a cached pointer, a future admin path) could hand this gate anything at all.
  // `bad-version` must beat every gate below it, including the ones that would otherwise say yes.
  for (const v of ["", "bv_", "bv_XYZ", "bv_0123456789ABCDEF", "bv_0123456789abcde", "../../etc", null, undefined]) {
    assert.equal(shouldStage(stageCtx({ bookVersion: v })).reason, "bad-version", `accepted ${String(v)}`);
  }
  // And it outranks a version that happens to equal the active/staged one, so a malformed string
  // can never be reported as "already-active" and quietly look like a healthy device.
  assert.equal(shouldStage(stageCtx({ bookVersion: "", activeBookVersion: "" })).reason, "bad-version");
});

test("REFUSES before the stagger clock has even STARTED", () => {
  // Previously unpinned. firstSeenAt is null until this device has written down that it saw this
  // pointer; staging before that would mean `now - null === now`, an enormous number, which sails
  // past every stagger delay — i.e. all eight iPads download in the same second. The exact
  // correlated-failure this whole mechanism exists to prevent (Rule 3).
  assert.equal(shouldStage(stageCtx({ firstSeenAt: null })).reason, "stagger-not-started");
  assert.equal(shouldStage(stageCtx({ firstSeenAt: undefined })).reason, "stagger-not-started");
  // Not merely falsy: 0 is a legitimate first-seen timestamp and must NOT be refused.
  assert.equal(shouldStage(stageCtx({ firstSeenAt: 0 })).stage, true);
});

test("waits out its stagger slot", () => {
  assert.equal(shouldStage(stageCtx({ deviceId: "slow", firstSeenAt: 0, now: 0 })).stage, false);
});

// ─── M1: a stale-ready copy must RECOVER, not dead-end ──────────────────────

test("M1 — a STALE-READY staged copy is re-staged instead of dead-ending forever", () => {
  // The trap, in full: the device stages at practice (the only window with internet) and is vetoed
  // from applying by the live mesh. Twelve hours later canApplyNow says `stale-ready` — forever —
  // while shouldStage said `already-staged` — forever. A verified 27 MB copy that can never be
  // used and can never be refreshed, on a device the fleet dashboard reports as "ready" while it
  // renders the OLD book. Neither gate could break the tie alone.
  const staged = { stagedBookVersion: BV, stagedReady: true, stagedReadyAt: 0 };
  const expiredNow = STAGED_READY_TTL_MS + 1;

  // 1. The apply gate refuses it (unchanged, correct).
  assert.equal(canApplyNow(applyCtx({ ...staged, lastCheckinOkAt: expiredNow, now: expiredNow })).reason, "stale-ready");
  // 2. The staging gate now lets it be refreshed — THIS is the fix.
  const d = shouldStage(stageCtx({ ...staged, firstSeenAt: 0, now: expiredNow }));
  assert.equal(d.stage, true, `still dead-ended: ${d.reason}`);
  assert.equal(d.reason, "restage-expired", "the breadcrumb must say WHY it is downloading again");
  // 3. And once re-staged (a fresh readyAt), the device converges: the apply gate opens.
  const fresh = canApplyNow(applyCtx({
    stagedBookVersion: BV, stagedReady: true, stagedReadyAt: expiredNow,
    lastCheckinOkAt: expiredNow, now: expiredNow,
  }));
  assert.equal(fresh.ok, true, fresh.reason);
});

test("M1 — a MISSING stagedReadyAt fails CLOSED in both gates, never skips the TTL check", () => {
  // An un-ageable record is not a fresh record. It cannot be proven safe, so it is treated as
  // expired: refused by the apply gate, and re-verified by the staging gate so it gets a real
  // readyAt and stops being un-ageable.
  assert.equal(canApplyNow(applyCtx({ stagedReadyAt: null })).reason, "stale-ready");
  assert.equal(canApplyNow(applyCtx({ stagedReadyAt: undefined })).reason, "stale-ready");
  const d = shouldStage(stageCtx({ stagedBookVersion: BV, stagedReady: true, stagedReadyAt: null }));
  assert.equal(d.stage, true, "an un-ageable staged record must not read as already-staged");
  assert.equal(d.reason, "restage-expired");
});

test("M1 — a record ONE MILLISECOND inside the TTL is still honoured on both sides", () => {
  // The recovery must not become a re-download treadmill: the boundary belongs to "fresh".
  const at = 1000;
  const edge = at + STAGED_READY_TTL_MS; // exactly at the limit — not yet expired
  assert.equal(shouldStage(stageCtx({ stagedBookVersion: BV, stagedReady: true, stagedReadyAt: at, now: edge })).reason, "already-staged");
  assert.equal(canApplyNow(applyCtx({ stagedReadyAt: at, lastCheckinOkAt: edge, now: edge })).ok, true);
  // One millisecond later, both flip.
  assert.equal(shouldStage(stageCtx({ stagedBookVersion: BV, stagedReady: true, stagedReadyAt: at, now: edge + 1 })).reason, "restage-expired");
  assert.equal(canApplyNow(applyCtx({ stagedReadyAt: at, lastCheckinOkAt: edge + 1, now: edge + 1 })).reason, "stale-ready");
});

test("M1 — the recovery does not weaken any gate BELOW already-staged", () => {
  // An expired record earns a re-verify, not a bypass. If it ever earned one, an expired copy
  // would download on the boot path, in the background, and inside its stagger slot.
  const expired = { stagedBookVersion: BV, stagedReady: true, stagedReadyAt: 0, now: STAGED_READY_TTL_MS + 1 };
  assert.equal(shouldStage(stageCtx({ ...expired, killSwitch: true })).reason, "kill-switch");
  assert.equal(shouldStage(stageCtx({ ...expired, activeBookVersion: BV })).reason, "already-active");
  assert.equal(shouldStage(stageCtx({ ...expired, minShellBuild: 999 })).reason, "shell-too-old");
  assert.equal(shouldStage(stageCtx({ ...expired, quarantine: [{ bookVersion: BV, failures: 3 }] })).reason, "quarantined");
  assert.equal(shouldStage(stageCtx({ ...expired, webReady: false })).reason, "not-web-ready");
  assert.equal(shouldStage(stageCtx({ ...expired, foreground: false })).reason, "background");
  assert.equal(shouldStage(stageCtx({ ...expired, firstSeenAt: null })).reason, "stagger-not-started");
  assert.equal(shouldStage(stageCtx({ ...expired, deviceId: "slow", firstSeenAt: expired.now })).reason, "stagger-waiting");
});

// ─── canApplyNow — it can only ever say NO ──────────────────────────────────

const applyCtx = (over = {}) => ({
  stagedReady: true, stagedReadyAt: 1000, lastCheckinOkAt: 1000, meshPeerConnected: false,
  lastPageTurnAt: null, lastMeshPageAt: null, webReady: true, minShellBuild: 1, shellBuild: 384,
  now: 2000, ...over,
});

test("allows an apply at practice: real internet, no mesh, nobody singing", () => {
  assert.equal(canApplyNow(applyCtx()).ok, true);
});

test("THE LOAD-BEARING VETO: no successful check-in in 5 minutes means no internet means NO", () => {
  // True at practice; FALSE INSIDE THE CHURCH BY DEFINITION. No clock, no calendar, no config.
  const r = canApplyNow(applyCtx({ lastCheckinOkAt: 1000, now: 1000 + LIVE_INTERNET_WINDOW_MS + 1 }));
  assert.equal(r.ok, false);
  assert.equal(r.reason, "no-live-internet");
  assert.equal(canApplyNow(applyCtx({ lastCheckinOkAt: null })).reason, "no-live-internet");
});

test("a ready flag from Saturday practice EXPIRES before Sunday Mass", () => {
  const r = canApplyNow(applyCtx({ stagedReadyAt: 0, lastCheckinOkAt: STAGED_READY_TTL_MS + 1, now: STAGED_READY_TTL_MS + 1 }));
  assert.equal(r.reason, "stale-ready");
});

test("EVERY role applies, the DIRECTOR included (owner decision, 2026-08-03)", () => {
  // Reverses "the DIRECTOR may never apply — the one device the room depends on". Role is not an
  // input any more, so passing one must not change the answer.
  for (const role of ["director", "follower", "off", null, undefined]) {
    const r = canApplyNow(applyCtx({ role }));
    assert.equal(r.ok, true, `role ${String(role)} was refused: ${r.reason}`);
  }
});

test("no DIRECTOR-DERIVED field is an input any more (owner decision, 2026-08-03)", () => {
  // `lastDirectorSnapshotAt` asked "is a DIRECTOR live?". It is gone as an input and passing it
  // must change nothing. Its 10-minute WINDOW is not gone — it lives on as `lastMeshPageAt` /
  // `room-active`, which asks "did a page move in this room?" and is fed by every role's page
  // turns (H1/H2). This test proves the identity question was retired; the H1 tests above prove
  // the timing question was kept. Deleting either half is a regression.
  const r = canApplyNow(applyCtx({ lastDirectorSnapshotAt: 1999, now: 2000 }));
  assert.equal(r.ok, true, r.reason);
});

test("the 90-minute director cold-boot cooldown is GONE, at every age since boot", () => {
  // Reverses NI3's cooldown: a device whose last role was director used to refuse for 90 minutes
  // after any cold boot — the veto that survived even when the live role read "follower".
  for (const coldBootAt of [2000, 1999, 1000, 0, null]) {
    const r = canApplyNow(applyCtx({ role: "follower", lastKnownRole: "director", coldBootAt }));
    assert.equal(r.ok, true, `coldBootAt ${String(coldBootAt)} was refused: ${r.reason}`);
  }
});

test("a connected mesh peer means a rehearsal or Mass is happening: NO — on the director's iPad too", () => {
  assert.equal(canApplyNow(applyCtx({ meshPeerConnected: true })).reason, "mesh-peer");
  assert.equal(canApplyNow(applyCtx({ meshPeerConnected: true, role: "director" })).reason, "mesh-peer");
});

test("a page turn in the last minute means someone is SINGING: NO", () => {
  assert.equal(canApplyNow(applyCtx({ lastPageTurnAt: 1990, now: 2000 })).reason, "recent-page-turn");
});

// ─── H1: the room-activity window, restored role-neutrally ──────────────────

test("H1 — page movement in this room within 10 MINUTES vetoes an apply, for EVERY role", () => {
  // The regression this pins: the deleted `director-active` veto was NOT a role veto. It read a
  // timestamp written by the mesh page-RECEIVE handler, so it fired on FOLLOWERS — the devices that
  // receive a director's page — and never on a director. Deleting it alongside the real role vetoes
  // silently cut the in-room quiet window an apply must clear from 10 minutes to 60 seconds on
  // EVERY device, which is not what the owner asked for.
  const now = 30 * 60_000;
  const nineMinutesAgo = now - 9 * 60_000;
  for (const role of ["director", "follower", "off", null, undefined]) {
    const r = canApplyNow(applyCtx({
      role, lastMeshPageAt: nineMinutesAgo, lastCheckinOkAt: now, stagedReadyAt: now, now,
    }));
    assert.equal(r.ok, false, `role ${String(role)} was allowed to swap 9 minutes into a live room`);
    assert.equal(r.reason, "room-active");
  }
});

test("H1 — the window is TEN minutes, not sixty seconds", () => {
  // Pins the duration itself. A 60 s window here would pass every other assertion in this file and
  // still be the exact regression: the gap between two songs routinely exceeds a minute.
  assert.equal(ROOM_ACTIVE_WINDOW_MS, 10 * 60_000);
  const now = 30 * 60_000;
  // 61 s of quiet clears `recent-page-turn` but must NOT clear the room window.
  assert.equal(
    canApplyNow(applyCtx({ lastMeshPageAt: now - 61_000, lastPageTurnAt: now - 61_000, lastCheckinOkAt: now, stagedReadyAt: now, now })).reason,
    "room-active",
  );
  // The boundary belongs to "still active".
  assert.equal(
    canApplyNow(applyCtx({ lastMeshPageAt: now - (ROOM_ACTIVE_WINDOW_MS - 1), lastCheckinOkAt: now, stagedReadyAt: now, now })).reason,
    "room-active",
  );
  // Ten minutes of silence and the room is finally quiet.
  assert.equal(
    canApplyNow(applyCtx({ lastMeshPageAt: now - ROOM_ACTIVE_WINDOW_MS, lastCheckinOkAt: now, stagedReadyAt: now, now })).ok,
    true,
  );
});

test("H1 — the room window keys on ACTIVITY, never on identity", () => {
  // Two devices, identical room history, opposite roles: same answer. And a device that has heard
  // nothing is not vetoed by it — a null clock is silence, not a permanent lockout.
  const now = 30 * 60_000;
  const busy = { lastMeshPageAt: now - 60_000, lastCheckinOkAt: now, stagedReadyAt: now, now };
  assert.equal(canApplyNow(applyCtx({ ...busy, role: "director" })).reason, "room-active");
  assert.equal(canApplyNow(applyCtx({ ...busy, role: "follower" })).reason, "room-active");
  assert.equal(canApplyNow(applyCtx({ ...busy, lastMeshPageAt: null })).ok, true);
});

test("H1 — a page turn in the last MINUTE is still reported as the sharper reason", () => {
  // Both windows fire during a song. `recent-page-turn` is the more specific diagnosis and must
  // win the breadcrumb, or every in-Mass deferral would read as the vaguer `room-active`.
  const now = 30 * 60_000;
  assert.equal(
    canApplyNow(applyCtx({ lastPageTurnAt: now - 5_000, lastMeshPageAt: now - 5_000, lastCheckinOkAt: now, stagedReadyAt: now, now })).reason,
    "recent-page-turn",
  );
});

test("the NON-role apply gates still refuse a director's iPad like anyone else's", () => {
  // The roles came out; TIMING and CAPABILITY did not. If any of these ever say ok, the removal
  // went further than the owner asked for.
  const d = (over) => canApplyNow(applyCtx({ role: "director", ...over })).reason;
  assert.equal(d({ stagedReady: false }), "not-ready");
  assert.equal(d({ stagedReadyAt: 0, lastCheckinOkAt: STAGED_READY_TTL_MS + 1, now: STAGED_READY_TTL_MS + 1 }), "stale-ready");
  assert.equal(d({ stagedReadyAt: null }), "stale-ready");
  assert.equal(d({ lastCheckinOkAt: null }), "no-live-internet");
  assert.equal(d({ meshPeerConnected: true }), "mesh-peer");
  assert.equal(d({ lastPageTurnAt: 1990 }), "recent-page-turn");
  assert.equal(d({ lastMeshPageAt: 1990 }), "room-active");
  assert.equal(d({ webReady: false }), "bridge-not-ready");
  assert.equal(d({ minShellBuild: 99999 }), "shell-too-old");
});

test("refuses when the bridge is not ready or the shell is too old", () => {
  assert.equal(canApplyNow(applyCtx({ webReady: false })).reason, "bridge-not-ready");
  assert.equal(canApplyNow(applyCtx({ minShellBuild: 99999 })).reason, "shell-too-old");
});

test("an empty context never throws and never says yes", () => {
  for (const c of [undefined, null, {}]) assert.equal(canApplyNow(c).ok, false);
});

// ─── The completeness gate ──────────────────────────────────────────────────

const mkManifest = (pages = 3) => ({
  bookVersion: BV,
  totalPages: pages,
  pagePadWidth: 3,
  files: [
    { p: "index.html", n: 500, h: "h", m: "m0" },
    ...Array.from({ length: pages }, (_, i) => ({
      p: `books/standard/pages/page-${String(i + 1).padStart(3, "0")}.webp`,
      n: 100, h: "h", m: `m${i + 1}`,
    })),
  ],
});
const mkDisk = (man) => new Map(man.files.map((f) => [f.p, { size: f.n, md5: f.m }]));

test("a complete staged bundle verifies", () => {
  const m = mkManifest();
  assert.deepEqual(verifyStaged(m, mkDisk(m), 3), { ok: true, problems: [] });
});

test("FIRES: a file with the right size but the WRONG BYTES", () => {
  const m = mkManifest();
  const d = mkDisk(m);
  d.set("books/standard/pages/page-002.webp", { size: 100, md5: "corrupted" });
  const v = verifyStaged(m, d, 3);
  assert.equal(v.ok, false);
  assert.ok(v.problems.some((p) => p.startsWith("md5:")));
});

test("FIRES: a missing MIDDLE page, not just the endpoints (NI7)", () => {
  const m = mkManifest(5);
  const d = mkDisk(m);
  d.delete("books/standard/pages/page-003.webp");
  const v = verifyStaged(m, d, 5);
  assert.equal(v.ok, false);
  assert.ok(v.problems.some((p) => p.startsWith("pages:") || p.startsWith("missing:")));
});

test("FIRES: a truncated index.html — the blank-app failure", () => {
  const m = mkManifest();
  const d = mkDisk(m);
  d.set("index.html", { size: 12, md5: "m0" });
  assert.ok(verifyStaged(m, d, 3).problems.includes("index-too-small"));
});

test("FIRES: an unexplained extra file on disk", () => {
  const m = mkManifest();
  const d = mkDisk(m);
  d.set("stowaway.js", { size: 1, md5: "x" });
  assert.ok(verifyStaged(m, d, 3).problems.some((p) => p.startsWith("count:")));
});

test("FIRES: a book that SHRANK — additive-only, enforced on the device", () => {
  const m = mkManifest(2);
  assert.ok(verifyStaged(m, mkDisk(m), 373).problems.some((p) => p.startsWith("shrank:")));
});

test("a null manifest is never quietly ok", () => {
  assert.equal(verifyStaged(null, new Map(), 0).ok, false);
});

// ─── Operator codes must not be confusable under stress ─────────────────────

test("levenshtein is correct", () => {
  assert.equal(levenshtein("744668486", "744668486"), 0);
  assert.equal(levenshtein("744668486", "744668487"), 1);
  assert.equal(levenshtein("abc", "xyz"), 3);
});

test("the rejected apply code was ONE digit from soft-reset — that is why the rule exists", () => {
  // Red team H4: 744668487 vs SOFT_RESET_CODE 744668486, read off a laminated card in poor light.
  assert.ok(levenshtein("744668486", "744668487") < MIN_CODE_DISTANCE);
});

// ─── stageBook end to end, against a fake filesystem ────────────────────────

const md5 = (s) => crypto.createHash("md5").update(s).digest("hex");

const fakeFs = (initial = {}) => {
  const files = new Map(Object.entries(initial));
  return {
    files,
    stat: async (p) => (files.has(p) ? { size: files.get(p).length } : null),
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(`${p}/`)),
    mkdirp: async () => {},
    rmrf: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(`${p}/`)) files.delete(k); },
    move: async (from, to) => {
      for (const k of [...files.keys()]) {
        if (k === from || k.startsWith(`${from}/`)) { files.set(to + k.slice(from.length), files.get(k)); files.delete(k); }
      }
    },
    readJson: async (p) => { if (!files.has(p)) throw new Error("enoent"); return JSON.parse(files.get(p)); },
    writeJson: async (p, v) => { files.set(p, JSON.stringify(v)); },
    walkWithHashes: async (dir) => {
      const out = new Map();
      for (const [k, v] of files) if (k.startsWith(`${dir}/`)) out.set(k.slice(dir.length + 1), { size: v.length, md5: md5(v) });
      return out;
    },
  };
};

const serverBundle = (pages = 2) => {
  const body = { "index.html": "x".repeat(500) };
  for (let i = 1; i <= pages; i += 1) body[`books/standard/pages/page-${String(i).padStart(3, "0")}.webp`] = `page${i}-bytes`;
  const manifest = {
    bookVersion: BV, totalPages: pages, pagePadWidth: 3, minShellBuild: 1,
    files: Object.entries(body).map(([p, v]) => ({ p, n: v.length, h: "sha", m: md5(v) })),
  };
  return { body, manifest };
};

const fakeNet = ({ manifest, body, failOn = null }) => ({
  fetchJson: async () => manifest,
  download: async (url, dest) => {
    const rel = url.replace("https://signovivo.com/", "");
    if (failOn === rel) throw new Error("net");
    if (!(rel in body)) throw new Error("404");
    fsRef.files.set(dest, body[rel]);
  },
});
let fsRef;

test("stageBook downloads, verifies, and reports ready", async () => {
  const { body, manifest } = serverBundle(3);
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: fakeNet({ manifest, body }), now: () => 111, shellBuild: 384, activeTotalPages: 2,
  });
  assert.equal(rec.ready, true, JSON.stringify(rec));
  assert.equal(rec.totalPages, 3);
});

test("stageBook REFUSES when the CDN hands back a different edition", async () => {
  const { body, manifest } = serverBundle(2);
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: "bv_aaaaaaaaaaaaaaaa", fs: fsRef,
    net: fakeNet({ manifest, body }), shellBuild: 384,
  });
  assert.equal(rec.ready, false);
  assert.equal(rec.error, "version-mismatch");
});

test("stageBook is a silent NO-OP when offline — never an error state, never UI", async () => {
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: { fetchJson: async () => { throw new Error("offline"); }, download: async () => {} },
    shellBuild: 384,
  });
  assert.equal(rec.ready, false);
  assert.equal(rec.error, "network");
});

test("stageBook refuses to fill a disk it cannot afford", async () => {
  const { body, manifest } = serverBundle(2);
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: fakeNet({ manifest, body }), shellBuild: 384, freeDiskBytes: 10,
  });
  assert.equal(rec.error, "disk");
});

test("a partial download NEVER reports ready — a failed stage cannot reach a swap", async () => {
  const { body, manifest } = serverBundle(3);
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: fakeNet({ manifest, body, failOn: "books/standard/pages/page-002.webp" }),
    shellBuild: 384, concurrency: 1,
  });
  assert.equal(rec.ready, false);
  assert.equal(rec.error, "download");
});

test("stageBook refuses a book that shrank, on the device", async () => {
  const { body, manifest } = serverBundle(2);
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: fakeNet({ manifest, body }), shellBuild: 384, activeTotalPages: 373,
  });
  assert.equal(rec.ready, false);
  assert.equal(rec.error, "verify");
});

// ─── The swap ───────────────────────────────────────────────────────────────

test("apply swaps by rename and keeps the previous bundle", async () => {
  const fs = fakeFs({ "WebBundle/index.html": "old", "WebBundleStaged/index.html": "new" });
  const r = await applyStagedBundle({ fs });
  assert.deepEqual(r, { ok: true, stage: "done" });
  assert.equal(fs.files.get("WebBundle/index.html"), "new");
  assert.equal(fs.files.get("WebBundle.prev.tmp/index.html"), "old", "the previous book must survive");
});

test("apply works on a device that has no Documents bundle yet", async () => {
  const fs = fakeFs({ "WebBundleStaged/index.html": "new" });
  assert.equal((await applyStagedBundle({ fs })).ok, true);
  assert.equal(fs.files.get("WebBundle/index.html"), "new");
});

test("a failed swap-in ROLLS BACK, leaving the old book live", async () => {
  const fs = fakeFs({ "WebBundle/index.html": "old", "WebBundleStaged/index.html": "new" });
  let calls = 0;
  const realMove = fs.move;
  fs.move = async (a, b) => { calls += 1; if (calls === 2) throw new Error("boom"); return realMove(a, b); };
  const r = await applyStagedBundle({ fs });
  assert.equal(r.ok, false);
  assert.equal(r.stage, "swap-in");
  assert.equal(fs.files.get("WebBundle/index.html"), "old", "rollback must restore the live bundle");
});

test("a failed swap-aside changes NOTHING", async () => {
  const fs = fakeFs({ "WebBundle/index.html": "old", "WebBundleStaged/index.html": "new" });
  fs.move = async () => { throw new Error("boom"); };
  const r = await applyStagedBundle({ fs });
  assert.equal(r.stage, "swap-aside");
  assert.equal(fs.files.get("WebBundle/index.html"), "old");
  assert.equal(fs.files.get("WebBundleStaged/index.html"), "new");
});

// ── The codes as actually shipped ───────────────────────────────────────────
// Read from the source, so adding a confusable code in future reds this test.
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const readSrc = (rel) => fs.readFileSync(path.join(APP_ROOT, rel), "utf8");
/** Comments are prose — they legitimately DISCUSS roles. Only executable text is evidence. */
const stripComments = (s) => s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
/** Executable slice of `src` from `start` up to (not including) the next `end` after it. */
const region = (src, start, end) => {
  const a = src.indexOf(start);
  assert.ok(a > -1, `source marker vanished — this test is stale, not passing: ${start}`);
  const b = src.indexOf(end, a + start.length);
  assert.ok(b > a, `end marker vanished after ${start}: ${end}`);
  return stripComments(src.slice(a, b));
};

test("every shipped operator code is >= MIN_CODE_DISTANCE from every other", () => {
  const src = fs.readFileSync(path.resolve(new URL("..", import.meta.url).pathname, "PdfReaderApp.tsx"), "utf8");
  const codes = [...src.matchAll(/^const ([A-Z_]*CODE) = "(\d+)";/gm)].map((m) => ({ name: m[1], code: m[2] }));
  assert.ok(codes.length >= 3, `expected the operator codes in source, found ${codes.length}`);
  for (let i = 0; i < codes.length; i += 1) {
    for (let j = i + 1; j < codes.length; j += 1) {
      const d = levenshtein(codes[i].code, codes[j].code);
      assert.ok(
        d >= MIN_CODE_DISTANCE,
        `${codes[i].name} and ${codes[j].name} are only ${d} apart — one misread off a laminated card in poor light would fire the wrong one`,
      );
    }
  }
});

test("the downloader ships DORMANT and the kill switch is off-by-source", () => {
  const src = fs.readFileSync(path.resolve(new URL("..", import.meta.url).pathname, "PdfReaderApp.tsx"), "utf8");
  assert.match(src, /const SV_BOOK_DL_KILL = false;/, "the build-baked kill switch must exist");
  const wrangler = fs.readFileSync(
    path.resolve(new URL("..", import.meta.url).pathname, "sync-worker/wrangler.jsonc"), "utf8",
  );
  // The shipped config must arm nobody. This is what lets M5 land without a rehearsal.
  assert.match(wrangler, /"BOOK_UPDATE_VERSION":\s*""/, "BOOK_UPDATE_VERSION must ship empty");
  assert.match(wrangler, /"BOOK_UPDATE_DEVICES":\s*""/, "BOOK_UPDATE_DEVICES must ship empty");
  assert.match(wrangler, /"BOOK_UPDATE_ALLOW_FLEET":\s*""/, "fleet arming must ship disabled");
});

// ── The gates as actually WIRED, read from PdfReaderApp.tsx ─────────────────
//
// These exist because the reviewer of the 2026-08-03 amendment demonstrated the hole: a ONE-LINE
// early return in PdfReaderApp.tsx — `if (roleRef.current === "director") return;` above either
// call site — restores the whole director exemption, changes no gate, and leaves every pure-logic
// test in this file green. bookUpdate.js being role-free proves nothing on its own if the caller
// decides who gets to ask.

test("NO role-derived early return guards the shouldStage / canApplyNow call sites", () => {
  const src = readSrc("PdfReaderApp.tsx");
  // Everything from the top of each gate-calling function down to the point where its verdict is
  // consumed. A role check anywhere in that stretch is the exemption, reintroduced.
  const callSites = {
    "onCheckinResponse → shouldStage": region(src, "const onCheckinResponse = useCallback(", "if (!decision.stage) {"),
    "autoApplyIfSafe → canApplyNow": region(src, "const autoApplyIfSafe = useCallback(", 'breadcrumb("auto-apply:go")'),
    "applyStagedBook → canApplyNow": region(src, "const applyStagedBook = useCallback(", "if (!gate.ok) {"),
    "AppState active → autoApplyIfSafe": region(src, 'AppState.addEventListener("change"', 'roleRef.current === "follower"'),
  };
  // The identity of the device, in every spelling it has ever had in this file.
  const roleTells = [
    /\broleRef\b/,
    /\blastKnownRole\b/,
    /\bexplicitTransmitterRef\b/,
    /\blastDirectorSnapshotRef\b/,
    /["']director["']/,
    /\bcoldBootAtRef\b/,
  ];
  for (const [name, body] of Object.entries(callSites)) {
    assert.ok(body.length > 120, `${name}: region collapsed to ${body.length} chars — markers drifted`);
    for (const tell of roleTells) {
      assert.doesNotMatch(
        body,
        tell,
        `${name} consults ${tell} — that is the director exemption coming back through the caller. `
          + "Every role stages and applies (owner decision, 2026-08-03).",
      );
    }
  }
  // Belt and braces: no role-shaped KEY may be handed to either gate, even as dead data. The
  // context types dropped these fields; a stray one here is someone re-growing them.
  const gateArgs = `${callSites["onCheckinResponse → shouldStage"]}${callSites["autoApplyIfSafe → canApplyNow"]}${callSites["applyStagedBook → canApplyNow"]}`;
  for (const key of ["role:", "lastKnownRole:", "coldBootAt:", "lastDirectorSnapshotAt:"]) {
    assert.ok(!gateArgs.includes(key), `a gate is being passed \`${key}\` again`);
  }
});

test("H3 — the foreground refreshes mesh state BEFORE it considers applying", () => {
  // The apply used to run on the line above refreshNearbyDiscovery(), so both in-room gates read a
  // peer count latched before the device went to sleep — reliably quiet, reliably wrong, at exactly
  // the moment a device rejoins a live room. An iPad unlocked mid-song would swap its bundle and
  // remount the WebView underneath the singer.
  const src = readSrc("PdfReaderApp.tsx");
  const fg = region(src, 'AppState.addEventListener("change"', 'roleRef.current === "follower"');

  // 1. The apply must not be invoked directly from the foreground handler at all. Its single entry
  //    point there is the scheduler, and the scheduler is reached only AFTER rediscovery.
  assert.doesNotMatch(
    fg,
    /autoApplyIfSafeRef/,
    "H3: the foreground calls autoApplyIfSafe directly again — it runs before the mesh can answer, "
      + "so mesh-peer and room-active both read stale-quiet exactly when a device rejoins a live room",
  );
  const discovery = fg.indexOf("refreshNearbyDiscovery");
  const schedule = fg.indexOf("scheduleForegroundApply");
  assert.ok(discovery > -1, "the foreground no longer kicks mesh rediscovery");
  assert.ok(schedule > -1, "the foreground no longer schedules an apply at all");
  assert.ok(discovery < schedule, "H3: the apply is scheduled before rediscovery is even kicked");
  // Chained off discovery, not fired alongside it. `.finally` so a rejected browse still converges.
  assert.match(fg, /refreshNearbyDiscovery\(\)[\s\S]{0,60}\.finally\(scheduleForegroundApply\)/);

  // 2. Reordering alone is not enough: refreshNearbyDiscovery resolves before the Swift `state`
  //    event has updated meshPeerCountRef, so an immediate apply would still read the stale count.
  const sched = region(src, "const scheduleForegroundApply = useCallback(", "}, []);");
  assert.match(sched, /setTimeout\(/, "the apply must be DEFERRED, not merely moved down a line");
  assert.match(sched, /FOREGROUND_APPLY_SETTLE_MS/, "it must wait for mesh state to actually land");
  assert.match(sched, /AppState\.currentState !== "active"/, "re-check at FIRE time — no background swaps");
  assert.match(sched, /clearTimeout\(foregroundApplyTimerRef\.current\)/, "a foreground flap must re-arm, not stack");
  assert.doesNotMatch(sched, /roleRef|["']director["']/, "the scheduler must not know who we are");

  // 3. And the timer must not survive the listener it belongs to.
  const teardown = region(src, "return () => {\n      sub.remove();", "};");
  assert.match(teardown, /clearTimeout\(foregroundApplyTimerRef\.current\)/, "a bundle swap must never stay armed against a torn-down component");
});

test("H2 — a DIRECTOR's own page turn stamps the room-activity clocks", () => {
  const src = readSrc("PdfReaderApp.tsx");
  // The web 'page-changed' case is the ONE place a locally-originated page turn is observed for
  // every role — a director (whose own mesh echoes it ignores), a transmitter-only device with no
  // mesh at all, and a plain "off" reader. Before this, both clocks were fed only by the mesh
  // page-RECEIVE case, which a director breaks out of first: the gates were dead code on the one
  // device the owner's decision newly exposed to a mid-song swap.
  const ownTurn = region(src, 'case "page-changed"', 'case "director-code"');
  assert.match(ownTurn, /noteRoomPageActivity\(\)/, "a device's OWN page turn no longer stamps the clocks");
  assert.match(ownTurn, /broadcastPage\(/, "region markers drifted — this is not the page-turn case");
  // The mesh RECEIVE side must keep stamping too, or followers lose both gates instead.
  const meshRecv = region(src, 'case "page": {', 'case "state": {');
  assert.match(meshRecv, /noteRoomPageActivity\(\)/, "the mesh page-receive case no longer stamps the clocks");

  // One writer, both clocks, no role in it.
  const writer = region(src, "const noteRoomPageActivity = useCallback(", "}, []);");
  assert.match(writer, /lastPageTurnAtRef\.current\s*=/, "the 60 s clock is not being written");
  assert.match(writer, /lastMeshPageAtRef\.current\s*=/, "the 10 min clock is not being written");
  assert.doesNotMatch(writer, /roleRef|["']director["']/, "the room-activity writer must not know who we are");

  // Both clocks must reach canApplyNow, or writing them changes nothing.
  for (const site of ["const autoApplyIfSafe = useCallback(", "const applyStagedBook = useCallback("]) {
    const body = region(src, site, "});");
    assert.match(body, /lastPageTurnAt:\s*lastPageTurnAtRef\.current/, `${site} drops the 60 s clock`);
    assert.match(body, /lastMeshPageAt:\s*lastMeshPageAtRef\.current/, `${site} drops the 10 min clock`);
  }

  // ROLE TRANSITIONS: these are ROOM facts, not ROLE facts, and are deliberately never cleared. A
  // follower that becomes director carries its timestamps across, which is correct — clearing them
  // would OPEN both gates at the instant the room got busier (someone just took the podium). They
  // need no reset because they expire against the wall clock on their own.
  assert.doesNotMatch(
    stripComments(src),
    /last(PageTurn|MeshPage)AtRef\.current\s*=\s*(null|undefined|0)\b/,
    "something now CLEARS a room-activity clock — that opens an apply gate on a role transition",
  );
  // Fed from exactly two places (the shared writer), and the writer is called from exactly two.
  const calls = stripComments(src).match(/noteRoomPageActivity\(\)/g) || [];
  assert.equal(calls.length, 2, `expected 2 stamp sites (own turn + mesh receive), found ${calls.length}`);
});

test("M1 — the app actually HANDS the staged record's age to shouldStage", () => {
  // Found by mutation probe: deleting this ONE line puts the whole stale-ready dead end back with
  // every pure-logic M1 test still green, because those tests supply the context themselves and can
  // never see the wiring. The gate is only as good as what the caller feeds it.
  //
  // It is also not a harmless omission in the other direction: an absent stagedReadyAt reads as
  // EXPIRED (fail closed), so shouldStage would answer `restage-expired` on every single check-in —
  // a full re-verify walk of 372 files, every 90 seconds, on eight iPads sharing one parish AP.
  const body = region(readSrc("PdfReaderApp.tsx"), "const onCheckinResponse = useCallback(", "if (!decision.stage) {");
  assert.match(body, /stagedReady:\s*!!staged\?\.ready/, "the staging gate is not told whether a copy is ready");
  assert.match(
    body,
    /stagedReadyAt:\s*staged\?\.readyAt\s*\?\?\s*null/,
    "shouldStage is no longer told HOW OLD the staged copy is — `already-staged` becomes permanent "
      + "and a copy that expired before it could be applied can never be refreshed",
  );
});

test("M1 — an ALREADY-STAGED copy still gets an apply attempt on every check-in", () => {
  // The stale-ready dead end reached from the other side, found in the re-hunt. The apply used to
  // be attempted only in the seconds after a download completed — at practice, where the mesh is up
  // and `mesh-peer` vetoes by definition. A device whose gates all opened later (peers gone, ten
  // quiet minutes, internet still live, app never backgrounded so no foreground event ever fires)
  // sailed past the `already-staged` early return every 90 s without ever asking.
  const body = region(readSrc("PdfReaderApp.tsx"), "if (!decision.stage) {", "stagingInFlightRef.current = true;");
  assert.match(
    body,
    /decision\.reason === "already-staged"[\s\S]{0,40}autoApplyIfSafeRef/,
    "a staged-and-ready copy is never offered to canApplyNow outside the post-download window — "
      + "the device can hold a verified songbook it never installs",
  );
  // And it must stay a REQUEST, not a decision: no gate may be re-implemented at this call site.
  assert.doesNotMatch(body, /roleRef|meshPeerCountRef|["']director["']/);
});

test("src/bookUpdate.js contains no role logic at all, in either gate", () => {
  // The floor under every test above: whatever the caller does, the gates themselves cannot know.
  const code = stripComments(readSrc("src/bookUpdate.js"));
  assert.doesNotMatch(code, /\brole\b/i, "`role` is back in the downloader's executable source");
  assert.doesNotMatch(code, /\bdirector\b/i, "`director` is back in the downloader's executable source");
});

test("every veto reason of both gates is named in the header amendment's authoritative list", () => {
  // The header claims to be EXHAUSTIVE. An authoritative list that is wrong is worse than no list:
  // the previous one omitted bridge-not-ready and the whole of shouldStage, and told readers
  // lastPageTurnAt "is always null on the director's iPad" — which H2 made false. This makes the
  // claim self-enforcing: add a gate without documenting it and the suite goes red.
  const src = readSrc("src/bookUpdate.js");
  const header = region(src.replace(/\/\/ ?/g, ""), "WHAT DID NOT CHANGE", "The apply sequence deliberately mirrors");
  assert.ok(header.length > 300, "the header amendment block is missing or was gutted");

  const bodyOf = (name) => {
    const a = src.indexOf(`export const ${name} = (ctx) => {`);
    assert.ok(a > -1, `${name} is gone`);
    const b = src.indexOf("\n};", a);
    return stripComments(src.slice(a, b));
  };
  const reasons = new Set();
  for (const name of ["shouldStage", "canApplyNow"]) {
    for (const m of bodyOf(name).matchAll(/"([a-z][a-z0-9-]*)"/g)) reasons.add(m[1]);
  }
  reasons.delete("ok");
  assert.ok(reasons.size >= 18, `only extracted ${reasons.size} reasons — the extractor drifted, not the code`);
  for (const r of reasons) {
    // Whole-token match: "bridge-not-ready" must not satisfy a missing "not-ready".
    assert.match(
      header,
      new RegExp(`(?<![a-z-])${r}(?![a-z-])`),
      `\`${r}\` is a live gate reason that the header's "exhaustive" list never names`,
    );
  }
});
