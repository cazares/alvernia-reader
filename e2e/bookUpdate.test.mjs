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

test("a per-deployment Pages subdomain is REFUSED — which is why --base rollback does not work", () => {
  // Documents a real limitation rather than a wish. scripts/ota-arm.sh's header claims pointing
  // BOOK_UPDATE_BASE at an immutable per-deployment URL is "what makes ROLLBACK possible at all".
  // It is not: ALLOWED_HOSTS matches by exact equality, so these are rejected — and a rejected
  // pointer is the REVOKE path (PdfReaderApp.tsx), which DELETES a staged bundle. If someone later
  // makes rollback work by allowing subdomains, this test fails and forces them to notice that the
  // revoke semantics and the ota-arm.sh guard both have to move with it.
  for (const base of [
    "https://8f7cbc00.alvernia-reader.pages.dev",
    "https://staging.alvernia-reader.pages.dev",
  ]) {
    assert.equal(parseBookUpdate({ bookUpdate: { bookVersion: BV, base } }), null, `accepted ${base}`);
  }
});

test("scripts/ota-arm.sh's --base guard lists exactly ALLOWED_HOSTS — they must not drift", () => {
  // The guard is a duplicated list by necessity (shell cannot import the JS constant). Duplication
  // is safe only while something proves the copies agree; without this, the shell list silently
  // rots and the guard starts passing bases the client will revoke on.
  const sh = fs.readFileSync(new URL("../scripts/ota-arm.sh", import.meta.url), "utf8");
  const branch = /^\s*(signovivo\.com\|alvernia-reader\.pages\.dev)\)\s*;;\s*$/m.exec(sh);
  assert.ok(branch, "ota-arm.sh has no `signovivo.com|alvernia-reader.pages.dev)` case branch");
  assert.deepEqual(branch[1].split("|"), ALLOWED_HOSTS);
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

const stageCtx = (over = {}) => ({
  killSwitch: false, bookVersion: BV, activeBookVersion: "bv_ffffffffffffffff",
  stagedBookVersion: null, stagedReady: false, quarantine: [], webReady: true,
  foreground: true, role: "follower", firstSeenAt: 0, deviceId: "d", now: 21 * 60_000,
  minShellBuild: 1, shellBuild: 384, ...over,
});

test("stages in the ordinary case", () => {
  assert.equal(shouldStage(stageCtx()).stage, true);
});

test("REFUSES on the boot path — staging must never compete with the reader for I/O", () => {
  assert.equal(shouldStage(stageCtx({ webReady: false })).reason, "not-web-ready");
});

test("EVERY role downloads, the DIRECTOR included (owner decision, 2026-08-03)", () => {
  // Inverted, not deleted. The old rule refused the director outright, so that iPad could never
  // receive a book by ANY path. If a future change reintroduces a role veto in shouldStage, this
  // goes red. Role is passed explicitly to pin that it is IGNORED, not merely absent.
  for (const role of ["director", "follower", "off", null, undefined]) {
    const r = shouldStage(stageCtx({ role }));
    assert.equal(r.stage, true, `role=${String(role)} must stage`);
    assert.equal(r.reason, "ok", `role=${String(role)} must not be vetoed`);
  }
});

test("the build-baked kill switch stops everything, first", () => {
  assert.equal(shouldStage(stageCtx({ killSwitch: true, role: "follower" })).reason, "kill-switch");
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
  assert.equal(shouldStage(stageCtx({ stagedBookVersion: BV, stagedReady: true })).reason, "already-staged");
});

test("NO client stagger — an armed device downloads immediately (owner decision, 2026-08-03)", () => {
  // Inverted. The old rule delayed by hash(deviceId) % 20 minutes, invisibly, so a working rollout
  // was indistinguishable from a dead one for up to 19 minutes. BOOK_UPDATE_CONCURRENCY in the
  // worker caps concurrent downloads instead — same protection, server-tunable, no build to change.
  for (const deviceId of ["slow", "fast", "zzdiag", ""]) {
    const r = shouldStage(stageCtx({ deviceId, firstSeenAt: 0, now: 0 }));
    assert.equal(r.stage, true, `deviceId=${deviceId} must stage immediately`);
    assert.equal(r.reason, "ok");
  }
  // firstSeenAt is no longer an input at all — a device that has never seen the pointer before
  // still stages on the very first check-in.
  assert.equal(shouldStage(stageCtx({ firstSeenAt: null, now: 0 })).stage, true);
});

// ─── canApplyNow — it can only ever say NO ──────────────────────────────────

const applyCtx = (over = {}) => ({
  stagedReady: true, stagedReadyAt: 1000, lastCheckinOkAt: 1000, meshPeerConnected: false,
  lastPageTurnAt: null, lastDirectorSnapshotAt: null, role: "follower", lastKnownRole: null,
  coldBootAt: null, webReady: true, minShellBuild: 1, shellBuild: 384, now: 2000, ...over,
});

test("allows an apply at practice: real internet, no mesh, not directing", () => {
  assert.equal(canApplyNow(applyCtx()).ok, true);
});

test("the live-internet veto is GONE (owner decision, 2026-08-05: NO LOGIC AROUND DELIVERY)", () => {
  // Was "THE LOAD-BEARING VETO": no successful check-in within 5 minutes meant no internet meant NO.
  // Removed with every other deferring condition. A device applies whatever it has finished
  // downloading, whenever it next foregrounds, regardless of how long ago it last reached the relay.
  assert.equal(canApplyNow(applyCtx({ lastCheckinOkAt: 1000, now: 1000 + LIVE_INTERNET_WINDOW_MS + 1 })).ok, true);
  assert.equal(canApplyNow(applyCtx({ lastCheckinOkAt: null })).ok, true);
});

test("the stale-ready expiry is GONE — an old staged copy still applies", () => {
  // Was: a `ready` flag older than 12 h was refused so a copy staged at Saturday practice would not
  // apply on Sunday (red team A1). ACCEPTED CONSEQUENCE of the 2026-08-05 decision: it applies now,
  // however old it is. shouldStage still re-stages a stale copy, which is the real re-verification.
  assert.equal(canApplyNow(applyCtx({ stagedReadyAt: 0, now: STAGED_READY_TTL_MS * 100 })).ok, true);
});

test("EVERY role applies, the DIRECTOR included (owner decision, 2026-08-03)", () => {
  for (const role of ["director", "follower", "off", null, undefined]) {
    assert.equal(canApplyNow(applyCtx({ role })).ok, true, `role=${String(role)} must apply`);
  }
});

test("the NON-role gates still refuse a director's iPad exactly like anyone else's", () => {
  // The other half of the owner decision: removing the ROLE veto must not have removed any TIMING
  // or CAPABILITY veto. If any of these ever answers ok, the removal went further than asked.
  const asDirector = (over) => canApplyNow(applyCtx({ role: "director", ...over }));
  // Only the two PHYSICAL impossibilities survive: nothing downloaded, and no bridge to swap under.
  assert.equal(asDirector({ stagedReady: false }).reason, "not-ready");
  assert.equal(asDirector({ webReady: false }).reason, "bridge-not-ready");
  // Every DEFERRING gate is gone — a director's iPad applies under all of these.
  assert.equal(asDirector({ meshPeerConnected: true }).ok, true);
  assert.equal(asDirector({ minShellBuild: 99999 }).ok, true);
  assert.equal(asDirector({ lastCheckinOkAt: null }).ok, true);
  // Staging side, same principle.
  assert.equal(shouldStage(stageCtx({ role: "director", webReady: false })).reason, "not-web-ready");
  assert.equal(shouldStage(stageCtx({ role: "director", foreground: false })).reason, "background");
  assert.equal(shouldStage(stageCtx({ role: "director", minShellBuild: 999 })).reason, "shell-too-old");
});

test("the mesh-peer defer is GONE — an update lands even mid-rehearsal", () => {
  assert.equal(canApplyNow(applyCtx({ meshPeerConnected: true })).ok, true);
});

test("AN UPDATE NEVER WAITS FOR THE USER TO STOP TOUCHING THE DEVICE (owner decision, 2026-08-03)", () => {
  // THE bug from the user's side: typing a song number to check whether the update arrived IS a
  // page turn, which re-armed a 60-second lockout every single time. Looking at it prevented it.
  // A page turn one millisecond ago must not defer the apply.
  assert.equal(canApplyNow(applyCtx({ lastPageTurnAt: 1999, now: 2000 })).ok, true);
  assert.equal(canApplyNow(applyCtx({ lastPageTurnAt: 2000, now: 2000 })).ok, true);
  // Nor may a live director in the room, nor a device that was directing before a restart.
  assert.equal(canApplyNow(applyCtx({ lastDirectorSnapshotAt: 1999, now: 2000 })).ok, true);
  assert.equal(
    canApplyNow(applyCtx({ lastKnownRole: "director", coldBootAt: 1999, now: 2000 })).ok,
    true,
  );
  // No reason string anywhere may still describe a wait-for-quiet veto.
  for (const ctx of [
    { lastPageTurnAt: 1999 },
    { lastDirectorSnapshotAt: 1999 },
    { lastKnownRole: "director", coldBootAt: 1999 },
  ]) {
    const r = canApplyNow(applyCtx({ ...ctx, now: 2000 }));
    assert.ok(
      !["recent-page-turn", "director-active", "director-cold-boot-cooldown"].includes(r.reason),
      `a wait-for-quiet veto came back: ${r.reason}`,
    );
  }
});

test("NO gate defers an update any more — mesh-peer was the last survivor and it is gone", () => {
  // mesh-peer was kept through the 2026-08-03 amendment because a connected peer means a rehearsal
  // or Mass is running and the whole room would blink at once. The 2026-08-05 decision removed it
  // anyway, knowingly: that blink is now an ACCEPTED CONSEQUENCE. This test exists so the gate
  // cannot creep back without someone deleting an explicit assertion that it stays gone.
  for (const over of [
    { meshPeerConnected: true },
    { lastCheckinOkAt: null },
    { stagedReadyAt: 0, now: STAGED_READY_TTL_MS * 100 },
    { minShellBuild: 99999 },
    { lastPageTurnAt: 2000, now: 2000 },
    { lastDirectorSnapshotAt: 2000, now: 2000 },
    { coldBootAt: 2000, now: 2000 },
  ]) {
    assert.equal(canApplyNow(applyCtx(over)).ok, true, `deferred on ${JSON.stringify(over)}`);
  }
});

test("NI3 is CLOSED BY DECISION, not by a cooldown — a device that was directing still updates", () => {
  // The old NI3 answer was a 90-minute cold-boot cooldown, on the reasoning that role auto-restore
  // is refused on boot so `role` reads "follower" on the device that was directing 30 seconds ago.
  // Under the 2026-08-03 decision there is nothing left to protect: every role updates, and no gate
  // may hold an update waiting for the room to go quiet. Kept as a test so the cooldown cannot
  // return silently.
  const r = canApplyNow(applyCtx({ role: "follower", lastKnownRole: "director", coldBootAt: 1000, now: 2000 }));
  assert.equal(r.ok, true);
  assert.notEqual(r.reason, "director-cold-boot-cooldown");
});

test("that cooldown expires, so the device is not locked out forever", () => {
  const r = canApplyNow(applyCtx({
    role: "follower", lastKnownRole: "director", coldBootAt: 0,
    lastCheckinOkAt: 91 * 60_000, stagedReadyAt: 91 * 60_000, now: 91 * 60_000,
  }));
  assert.equal(r.ok, true);
});

test("refuses ONLY when there is nothing to apply or no bridge to apply it under", () => {
  // The complete surviving list. Both are physics, not policy: you cannot apply a book you have not
  // finished downloading, and swapping the bundle under a WebView that never signalled ready renders
  // a permanent blank screen — the worst outcome in this app.
  assert.equal(canApplyNow(applyCtx({ stagedReady: false })).reason, "not-ready");
  assert.equal(canApplyNow(applyCtx({ webReady: false })).reason, "bridge-not-ready");
  // shell-too-old moved out: shouldStage refuses the DOWNLOAD, which is the right place for it.
  assert.equal(canApplyNow(applyCtx({ minShellBuild: 99999 })).ok, true);
  assert.equal(shouldStage(stageCtx({ minShellBuild: 99999 })).reason, "shell-too-old");
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
    // Strip the ?v=<bookVersion> cache-buster the downloader appends (src/bookUpdate.js). A real
    // origin keys on the path and ignores the query; this fake has to do the same or every
    // download 404s. The assertion that the buster is actually SENT lives in its own test below —
    // swallowing it here must not mean nobody checks for it.
    const rel = url.replace("https://signovivo.com/", "").replace(/\?v=[^&]*$/, "");
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

test("every downloaded file carries the ?v= cache-buster", async () => {
  // Cloudflare holds these assets at s-maxage=604800 and a book update rewrites files at UNCHANGED
  // paths, so a bare URL can be served the PREVIOUS bytes: verifyStaged fails on md5, the bundle is
  // evicted, ~27 MB is pulled again, and the retry gets the same stale bytes. Forever.
  //
  // This lives in its own test because fakeNet STRIPS the query to resolve its body map — without
  // an explicit assertion, deleting the cache-buster from src/bookUpdate.js would keep every other
  // test in this file green. The fix is only as durable as the thing that notices it is gone.
  const { body, manifest } = serverBundle(3);
  fsRef = fakeFs();
  const seen = [];
  const net = fakeNet({ manifest, body });
  const spy = { ...net, download: async (url, dest) => { seen.push(url); return net.download(url, dest); } };
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV, fs: fsRef,
    net: spy, now: () => 111, shellBuild: 384, activeTotalPages: 2,
  });
  assert.equal(rec.ready, true, JSON.stringify(rec));
  assert.ok(seen.length > 0, "nothing was downloaded — the assertion below would pass vacuously");
  const bare = seen.filter((u) => !u.includes(`?v=${BV}`));
  assert.deepEqual(bare, [], `these downloaded without a cache-buster: ${bare.join(", ")}`);
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
