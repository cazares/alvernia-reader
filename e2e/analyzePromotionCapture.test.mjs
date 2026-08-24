import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// WHY THIS FILE EXISTS. scripts/analyze-promotion-capture.mjs is the tool that verdicts
// multi-device Console captures against the build 469-474 fix signatures. Its fixture here is a
// faithful subset of the REAL 2026-08-24 hardware capture (mPad + Rita's + Braulio's iPads +
// iPhone) that caught the phantom-beacon bug live: every follower applied `nonce=4ca1 page=7`
// that NO device in the room ever logged sending, milliseconds before the genuine promotion to
// page 213. PR #380 root-caused it (stale bluetoothd advertisement surviving a force-quit
// director). If the analyzer ever stops flagging that capture, it has stopped catching the one
// bug it was built to catch — these tests pin it to the real evidence, Console's 2-4x
// line-duplication artifact included.

const TOOL = "scripts/analyze-promotion-capture.mjs";
const FIXTURE = "e2e/fixtures/console-capture-ghost7.txt";

const run = (file) => {
  try {
    return { out: execFileSync("node", [TOOL, file], { encoding: "utf8" }), code: 0 };
  } catch (e) {
    return { out: e.stdout, code: e.status };
  }
};

const real = run(FIXTURE);

test("the real ghost-page-7 capture FAILs, naming the phantom nonce", () => {
  assert.equal(real.code, 1, "the capture that shipped PR #380 must exit non-zero");
  assert.match(real.out, /nonce=4ca1 page=7 .*GHOST: no device logged sending this page/);
  assert.match(real.out, /FAIL\s+ghost page\(s\) applied: nonce=4ca1 page=7/);
});

test("the genuine promotion broadcast is NOT flagged as a ghost", () => {
  assert.match(real.out, /nonce=1e3f page=213 .*explained by a logged ble:page-send/);
});

test("Console's 4x line duplication collapses to single events", () => {
  // mPad's invite:send appears 4x in the fixture with microsecond-jittered timestamps (the
  // Console GUI artifact, copied verbatim from the real capture). One invite, answered.
  assert.match(real.out, /iPad-4E3E79->iPhone-3C85E1: worst unanswered run=0, eventually answered/);
  assert.match(real.out, /Console duplicates collapsed/);
});

test("no demotion in the capture => eviction is inconclusive, never a pass", () => {
  assert.match(real.out, /INCONCLUSIVE\s+no demotion in capture — 471's eviction path was not exercised/);
});

test("absence of ble:boot-stop-stale is inconclusive per device, not silent", () => {
  assert.match(real.out, /BOOT-STOP-STALE/);
  assert.match(real.out, /INCONCLUSIVE\s+ble:boot-stop-stale missing on:/);
});

test("the promotion is detected and convergence is measured for every follower", () => {
  assert.match(real.out, /iPhone-3C85E1\s+PROMOTION/);
  for (const d of ["iPad-91B171", "iPad-4CEFD0", "iPad-4E3E79"]) {
    assert.match(real.out, new RegExp(`${d}: 8\\.\\d\\ds`), `${d} should converge in ~8s (BLE scan latency)`);
  }
});

// ── Synthetic captures for the paths the real fixture cannot exercise ─────────────────────────
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "capture-"));
const line = (t, msg) => `default\t${t}-0500\tSignoVivo\t${msg}`;
const write = (name, lines) => {
  const p = path.join(tmp, name);
  fs.writeFileSync(p, lines.join("\n") + "\n");
  return p;
};

test("471's failure signature — an unanswered invite hammer with no eviction — FAILs", () => {
  // 5 sends 500ms apart (outside the dedup window: these are genuine retries, the exact 300-700ms
  // cadence of the pre-471 bug), never connected, no director:evict-stale anywhere.
  const p = write("hammer.txt", [0, 1, 2, 3, 4].map((i) =>
    line(`06:00:0${i}.000000`, "follower iPad-A invite:send to=iPad-B")));
  const r = run(p);
  assert.equal(r.code, 1);
  assert.match(r.out, /FAIL\s+invite hammering with no eviction: iPad-A->iPad-B \(run of 5\)/);
});

test("the same evidence as JSONL yields the same verdicts as Console text", () => {
  // A device that was never attached to Console can still be read afterwards from its own
  // persisted unified log (sudo log collect → logarchive-to-jsonl). That path is the ONLY one
  // available for a device that ran at Mass, so it must not be a second, weaker analyzer: same
  // evidence in, same verdicts out. Built from the Console fixture so the two can never drift.
  const rows = [];
  for (const line of fs.readFileSync(FIXTURE, "utf8").split("\n")) {
    const m = /(\d{2}):(\d{2}):(\d{2})\.(\d{6}).*SignoVivo\t(follower|director|off) (\S+) (\S+)(.*)$/.exec(line);
    if (!m) continue;
    const [, hh, mm, ss, us, role, dev, event, rest] = m;
    // 2026-08-24 local, the real capture's date, + the line's time-of-day.
    const base = new Date(2026, 7, 24, +hh, +mm, +ss, 0).getTime() + +us / 1000;
    const row = { t: base, dev, role, event, src: "swift", build: null };
    for (const kvp of rest.trim().split(/\s+/)) {
      const i = kvp.indexOf("=");
      if (i > 0) row[kvp.slice(0, i)] = kvp.slice(i + 1);
    }
    rows.push(JSON.stringify(row));
  }
  assert.ok(rows.length > 50, `expected a full JSONL conversion, got ${rows.length} rows`);
  const p = write("capture.jsonl", rows);
  const r = run(p);

  assert.equal(r.code, 1, "the ghost must still be caught through the JSONL path");
  assert.match(r.out, /nonce=4ca1 page=7 .*GHOST/);
  assert.match(r.out, /nonce=1e3f page=213 .*explained by a logged ble:page-send/);
  // Verdict lines must match the Console run exactly — the input shape must not change the answer.
  const verdicts = (s) => s.slice(s.indexOf("══ VERDICTS ══")).trim();
  assert.equal(verdicts(r.out), verdicts(real.out));
});

test("mixing Console text and JSONL in one run is refused, not silently misread", () => {
  // Their timestamps live on different axes (seconds-of-day vs epoch); pooling them would put
  // events ~57 years apart on one timeline and make every cross-device check meaningless.
  const p = write("mixed.txt", [
    JSON.stringify({ t: Date.UTC(2026, 7, 24, 10, 0, 0), dev: "iPad-A", role: "follower", event: "ble:page-apply", page: "7" }),
    line("05:03:02.877849", "director iPhone-B ble:page-send coldRadio=false page=213 seq=1"),
  ]);
  const r = run(p);
  assert.equal(r.code, 2, "a mixed pool must abort, not produce a verdict");
});

test("471's fix signature — eviction after 2 failures — PASSes and ends the run", () => {
  const p = write("evict.txt", [
    line("06:00:00.000000", "follower iPad-A invite:send to=iPad-B"),
    line("06:00:00.500000", "follower iPad-A invite:send to=iPad-B"),
    line("06:00:00.900000", "follower iPad-A director:evict-stale peer=iPad-B streak=2"),
  ]);
  const r = run(p);
  assert.equal(r.code, 0, "an evicted 2-streak is the fix working, not a failure");
  assert.match(r.out, /PASS\s+director:evict-stale fired 1x, no hammering runs/);
});
