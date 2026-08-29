import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// WHY THIS FILE EXISTS. scripts/analyze-promotion-capture.mjs verdicts multi-device hardware
// captures against the build 469-474 fix signatures. Its fixture is a faithful subset of the REAL
// 2026-08-24 capture that caught the phantom-beacon bug live: every follower rendered
// `nonce=4ca1 page=7` while NO device in the room was broadcasting it (root-caused in PR #380).
//
// The second half of this file pins the findings of an adversarial audit (2026-08-24, 139 agents,
// 12 confirmed) that broke the tool's FIRST design. That design asked "was there a ble:page-send
// for page P near this sighting?", which is wrong in both directions: a page-send fires once per
// page turn (BlePageBeacon.swift:156) while the advertisement is continuous state, so a healthy
// stable director read as a ghost, AND a stale advertisement was explained away by the very
// page-send its ex-director logged while alive. Adding ONE legitimate line to the real fixture
// flipped it from FAIL to PASS. Every one of those is a live regression test now — a verdict
// machine that can be talked out of its own headline finding is worse than no tool at all.

const TOOL = "scripts/analyze-promotion-capture.mjs";
const FIXTURE = "e2e/fixtures/console-capture-ghost7.txt";

const run = (file) => {
  try {
    return { out: execFileSync("node", [TOOL, file], { encoding: "utf8" }), code: 0 };
  } catch (e) {
    return { out: e.stdout ?? "", code: e.status };
  }
};

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "capture-"));
const clock = (s) => {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${(s % 60).toFixed(6).padStart(9, "0")}`;
};
const line = (t, msg) => `default\t${typeof t === "number" ? clock(t) : t}-0500\tSignoVivo\t${msg}`;
const write = (name, lines) => {
  const p = path.join(tmp, name);
  fs.writeFileSync(p, lines.join("\n") + "\n");
  return p;
};
const T0 = 18000; // 05:00:00, matching the real capture's hour
const verdictsOf = (s) => s.slice(s.indexOf("══ VERDICTS ══")).trim();

const real = run(FIXTURE);

// ── The real capture ──────────────────────────────────────────────────────────────────────────
test("the real ghost-page-7 capture FAILs, naming the page nobody broadcast", () => {
  assert.equal(real.code, 1, "the capture that shipped PR #380 must exit non-zero");
  assert.match(real.out, /page\(s\) rendered with nobody broadcasting them: 7 \(nobody in the pool ever broadcast it\)/);
});

test("the FAIL names the user-visible symptom — which devices RENDERED the ghost", () => {
  // The old design only flagged the advertiser line. What actually went wrong at Mass is that three
  // iPads showed the wrong song, and that is what the report must say.
  for (const d of ["iPad-4CEFD0", "iPad-4E3E79", "iPad-91B171"]) {
    assert.match(real.out, new RegExp(`RENDERED page=7 on ${d}`));
  }
});

test("the genuine promotion broadcast is NOT flagged, and is attributed to its broadcaster", () => {
  assert.match(real.out, /advert nonce=1e3f page=213 .*\(matches iPhone-3C85E1 broadcasting page 213\)/);
});

test("Console's 4x line duplication collapses to single events", () => {
  assert.match(real.out, /iPad-4E3E79->iPhone-3C85E1: worst unanswered streak=0, eventually answered/);
  assert.match(real.out, /duplicate line\(s\) collapsed/);
});

test("the promotion is detected and convergence measured for every follower", () => {
  assert.match(real.out, /iPhone-3C85E1\s+PROMOTION/);
  for (const d of ["iPad-91B171", "iPad-4CEFD0", "iPad-4E3E79"]) {
    assert.match(real.out, new RegExp(`${d}: 8\\.\\d\\ds`), `${d} should converge in ~8s`);
  }
});

// ── Input paths ───────────────────────────────────────────────────────────────────────────────
test("the same evidence as JSONL yields the same verdicts as Console text", () => {
  // A device that ran at Mass was never attached to Console; its evidence exists only in its own
  // persisted unified log. That path must not be a second, weaker analyzer.
  const rows = [];
  for (const l of fs.readFileSync(FIXTURE, "utf8").split("\n")) {
    const m = /(\d{2}):(\d{2}):(\d{2})\.(\d{6}).*SignoVivo\t(follower|director|off) (\S+) (\S+)(.*)$/.exec(l);
    if (!m) continue;
    const [, hh, mm, ss, us, role, dev, event, rest] = m;
    const row = { t: new Date(2026, 7, 24, +hh, +mm, +ss, 0).getTime() + +us / 1000, dev, role, event, src: "swift", build: null };
    for (const kvp of rest.trim().split(/\s+/)) {
      const i = kvp.indexOf("=");
      if (i > 0) row[kvp.slice(0, i)] = kvp.slice(i + 1);
    }
    rows.push(JSON.stringify(row));
  }
  assert.ok(rows.length > 50, `expected a full JSONL conversion, got ${rows.length}`);
  const r = run(write("capture.jsonl", rows));
  assert.equal(r.code, 1, "the ghost must still be caught through the JSONL path");
  assert.equal(verdictsOf(r.out), verdictsOf(real.out), "input shape must not change the answer");
});

test("mixing Console text and JSONL in one run is refused, not silently misread", () => {
  const p = write("mixed.txt", [
    JSON.stringify({ t: Date.UTC(2026, 7, 24, 10, 0, 0), dev: "iPad-A", role: "follower", event: "ble:page-apply", page: "7" }),
    line(T0, "director iPhone-B ble:page-send coldRadio=false page=213 seq=1"),
  ]);
  assert.equal(run(p).code, 2, "a mixed pool must abort, not produce a verdict");
});

test("unparseable input exits 2 rather than reporting a clean bill of health", () => {
  assert.equal(run(write("junk.txt", ["not a log", "%%%", ""])).code, 2);
});

// ── Audit regressions: the ghost check must model STATE, not one-shot events ───────────────────
const heartbeat = (from, count, dev = "iPad-D") =>
  Array.from({ length: count }, (_, i) => line(from + i * 5, `director ${dev} refresh:hold-serving connected=1 discovered=1`));

test("AUDIT: a stable director's page-send scrolling out of the window is NOT a ghost", () => {
  // Finding 5 (critical false-fail). BlePageBeacon.swift:156 emits page-send exactly once per page
  // turn; a director sitting on one page for ten minutes logs it once, ten minutes ago. The old
  // design reported the healthiest possible fleet as a phantom beacon, burning a hardware night.
  const p = write("stable.txt", [
    line(T0, "director iPad-D ble:page-send coldRadio=false page=7 seq=1"),
    ...heartbeat(T0 + 5, 120),
    line(T0 + 540, "follower iPad-F ble:new-advertiser nonce=aa11 page=7"),
    line(T0 + 540.1, "follower iPad-F ble:page-apply page=7 seq=1"),
  ]);
  const r = run(p);
  assert.equal(r.code, 0, "a stable live director must not FAIL");
  assert.match(r.out, /PASS\s+every rendered page traces to a device that was actually broadcasting it/);
});

test("AUDIT: an ex-director's own earlier legit send cannot explain away its stale beacon", () => {
  // Findings 2/3/4 (critical false-pass) — the exploit that broke the first design. A PR #380
  // stale advertisement is BY CONSTRUCTION the leftover of a page its ex-director genuinely
  // published, and therefore genuinely logged. One prepended line flipped the real capture to PASS.
  const p = write("exdir.txt", [
    line(T0 + 10.1, "director iPad-EXDIR ble:page-send coldRadio=false page=7 seq=1"),
    ...fs.readFileSync(FIXTURE, "utf8").split("\n").filter(Boolean),
  ]);
  const r = run(p);
  assert.doesNotMatch(verdictsOf(r.out), /PASS\s+every rendered page traces/, "must never PASS the ghost check");
  assert.match(r.out, /INCONCLUSIVE\s+page\(s\) 7 arrived after their broadcaster went quiet/);
  assert.match(r.out, /iPad-EXDIR/, "must name the device whose silence created the ambiguity");
});

test("AUDIT: force-quit while directing, then relaunching as a follower, FAILs", () => {
  // The exact hardware scenario PR #380 exists for, and one Miguel runs by hand.
  const p = write("relaunch.txt", [
    line(T0, "director iPad-EX ble:page-send coldRadio=false page=7 seq=1"),
    line(T0 + 5, "director iPad-EX refresh:hold-serving connected=1 discovered=1"),
    line(T0 + 20, "follower iPad-EX ble:boot-stop-stale"),
    line(T0 + 30, "follower iPad-F ble:new-advertiser nonce=bb22 page=7"),
    line(T0 + 30.1, "follower iPad-F ble:page-apply page=7 seq=1"),
  ]);
  const r = run(p);
  assert.equal(r.code, 1);
  assert.match(r.out, /its broadcaster explicitly stopped, yet the page kept arriving/);
});

test("AUDIT: a page still arriving after a CLEAN demote FAILs", () => {
  const p = write("demote-ad.txt", [
    line(T0, "director iPad-EX ble:page-send coldRadio=false page=7 seq=1"),
    line(T0 + 10, "follower iPad-EX ble:stop-publishing"),
    line(T0 + 30, "follower iPad-F ble:page-apply page=7 seq=1"),
  ]);
  assert.equal(run(p).code, 1);
});

test("AUDIT: a page rendered from an already-known advertiser is still checked", () => {
  // Finding 1 (critical false-pass): ble:new-advertiser fires only when the NONCE changes
  // (BlePageBeacon.swift:400), so later pages under a known nonce have no advertiser line at all.
  // The old check iterated advertiser lines and never looked at what was rendered.
  const p = write("no-advert.txt", [
    line(T0, "director iPad-D ble:page-send coldRadio=false page=213 seq=1"),
    ...heartbeat(T0 + 5, 6),
    line(T0 + 20, "follower iPad-F ble:page-recv page=7 rssi=-40 seq=9"),
    line(T0 + 20.1, "follower iPad-F ble:page-apply page=7 seq=9"),
  ]);
  const r = run(p);
  assert.equal(r.code, 1, "a rendered page with no advertiser line must still be checked");
  assert.match(r.out, /RENDERED page=7 on iPad-F/);
});

test("AUDIT: with no director captured at all, nothing is attributable — INCONCLUSIVE, not FAIL", () => {
  // Indicting every broadcast in the room because the director simply was not in the pool would be
  // the false-FAIL twin of the bug above.
  const p = write("nodirector.txt", [
    line(T0, "follower iPad-F ble:new-advertiser nonce=dd44 page=7"),
    line(T0 + 0.1, "follower iPad-F ble:page-apply page=7 seq=1"),
  ]);
  const r = run(p);
  assert.equal(r.code, 0);
  assert.match(r.out, /INCONCLUSIVE\s+no device in the pool was ever a publishing director/);
});

// ── Audit regressions: eviction ───────────────────────────────────────────────────────────────
test("471's failure signature — an unanswered invite hammer with no eviction — FAILs", () => {
  const p = write("hammer.txt", [0, 1, 2, 3, 4].map((i) =>
    line(T0 + i, "follower iPad-A invite:send to=iPad-B")));
  const r = run(p);
  assert.equal(r.code, 1);
  assert.match(r.out, /FAIL\s+invite hammering: iPad-A->iPad-B/);
});

test("471's fix signature — eviction inside the streak — PASSes", () => {
  const p = write("evict.txt", [
    line(T0, "director iPad-B refresh:hold-serving connected=1 discovered=1"),
    line(T0 + 1, "follower iPad-B found peer=iPad-A prole=follower"),
    line(T0 + 2, "follower iPad-A invite:send to=iPad-B"),
    line(T0 + 2.5, "follower iPad-A invite:send to=iPad-B"),
    line(T0 + 2.9, "follower iPad-A director:evict-stale peer=iPad-B streak=2"),
  ]);
  const r = run(p);
  assert.equal(r.code, 0, "an evicted 2-streak is the fix working");
  assert.match(r.out, /\[evicted /);
});

test("AUDIT: one stray evict line cannot whitewash an unbounded hammering run", () => {
  // Finding 6 (high false-pass). DirectorSyncModule.swift:2358 clears the streak counter on
  // eviction, so evict -> rediscover -> hammer again is the live regression; the old check excused
  // a run if an evict for that pair existed ANYWHERE in the capture.
  const p = write("evict-then-hammer.txt", [
    line(T0, "follower iPad-A invite:send to=iPad-B"),
    line(T0 + 0.5, "follower iPad-A invite:send to=iPad-B"),
    line(T0 + 0.9, "follower iPad-A director:evict-stale peer=iPad-B streak=2"),
    ...Array.from({ length: 40 }, (_, i) => line(T0 + 10 + i * 0.5, "follower iPad-A invite:send to=iPad-B")),
  ]);
  const r = run(p);
  assert.equal(r.code, 1, "40 unanswered retries after an evict is the 471 symptom, not a pass");
  assert.match(r.out, /eviction is not capping it/);
});

test("AUDIT: a demotion nobody re-targeted is INCONCLUSIVE, never PASS", () => {
  // Findings 7 and 10 (high false-pass). The 471 path needs BOTH a demotion AND a follower actually
  // re-targeting the ex-director. An empty runs map is the normal shape when the hammering
  // followers were simply not in the pool — the exact capture that must not read as healthy.
  const p = write("demote-quiet.txt", [
    line(T0, "director iPad-D ble:page-send coldRadio=false page=7 seq=1"),
    line(T0 + 5, "follower iPad-D found peer=iPad-F prole=follower"),
  ]);
  const r = run(p);
  assert.equal(r.code, 0);
  assert.match(r.out, /INCONCLUSIVE\s+a demotion occurred but no follower ever re-targeted the ex-director/);
  assert.doesNotMatch(r.out, /PASS\s+\d+ demoted ex-director/);
});

// ── Audit regressions: clock skew and boot-stop ────────────────────────────────────────────────
test("AUDIT: an unanswered retry does not steal an older receive and invent skew", () => {
  // Finding 8 (high false-fail). Nearest-match over ±30s with no consumption let a lone retry pair
  // with a much older, already-explained receive, reporting tens of seconds of skew that is not
  // there — and skew is a FAIL.
  const p = write("skew.txt", [
    line(T0, "follower iPad-A invite:send to=iPad-B"),
    line(T0 + 0.2, "director iPad-B invite:recv from=iPad-A"),
    line(T0 + 0.4, "follower iPad-A session:connected peer=iPad-B"),
    line(T0 + 22, "follower iPad-A invite:send to=iPad-B"), // retry, no receive of its own
  ]);
  const r = run(p);
  assert.equal(r.code, 0, "a normal unanswered retry must not be reported as clock skew");
  assert.match(r.out, /causal order intact/);
});

test("AUDIT: boot-stop-stale is checked by membership, not by counting", () => {
  // Findings 11 and 12. `off ? ble:boot-stop-stale` lines are real (localPeerID is nil before the
  // module starts); counting let one pad the tally so a real device's absence passed, and in the
  // other direction produced "missing on: (none)" while refusing to PASS.
  const p = write("bootstop.txt", [
    line(T0, "off ? ble:boot-stop-stale"),
    line(T0 + 1, "follower iPad-A ble:boot-stop-stale"),
    line(T0 + 2, "follower iPad-B found peer=iPad-A prole=follower"),
  ]);
  const r = run(p);
  assert.match(r.out, /INCONCLUSIVE\s+ble:boot-stop-stale not seen on: iPad-B/,
    "the '?' pseudo-device must not stand in for a real one");
  assert.doesNotMatch(r.out, /not seen on: \(none\)/);
});

test("AUDIT: every real device logging boot-stop-stale is a PASS", () => {
  const p = write("bootstop-ok.txt", [
    line(T0, "off ? ble:boot-stop-stale"),
    line(T0 + 1, "follower iPad-A ble:boot-stop-stale"),
    line(T0 + 2, "follower iPad-B ble:boot-stop-stale"),
  ]);
  assert.match(run(p).out, /PASS\s+every device \(2\) logged ble:boot-stop-stale/);
});

test("AUDIT: a send and an apply in the SAME instant is a healthy exchange, not a ghost", () => {
  // THE ZERO-LENGTH SPAN. A publish span whose open and close land on the same timestamp used to be
  // discarded (`t > since`), so a director whose ble:page-send is the newest row in the pool left no
  // span at all — and the ghost check then reported the page the follower had just CORRECTLY applied
  // as "rendered with nobody broadcasting it". That is the normal shape of a short capture that ends
  // on a page turn, and it is exactly the shape of this repo's own sv-log-2026-08-18.jsonl.
  //
  // A capture that FAILS the ghost check reads as PR #380 regressing, which costs a hardware night.
  const p = write("same-instant.txt", [
    line(T0, "director mPad ble:page-send page=372 seq=1"),
    line(T0, "follower iPad-A ble:page-apply page=372 seq=1"),
  ]);
  const r = run(p);
  assert.doesNotMatch(r.out, /nobody in the pool ever broadcast it/,
    "a page broadcast in the same instant it was applied is still called a ghost");
  assert.doesNotMatch(verdictsOf(r.out), /FAIL/,
    "the healthiest possible BLE exchange still fails the capture");
});

test("AUDIT: the real ghost fixture still FAILS — the instrument is not blunted", () => {
  // The counterweight to the test above: widening the span rule must not stop the tool catching a
  // page that genuinely nobody was broadcasting. This is the captured page-7 ghost from hardware.
  assert.equal(real.code, 1, "the real ghost capture no longer fails");
  assert.match(real.out, /page\(s\) rendered with nobody broadcasting them: 7/,
    "the page-7 ghost is no longer named");
});
