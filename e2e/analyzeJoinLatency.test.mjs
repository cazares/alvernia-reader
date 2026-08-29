import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

// THE ANALYZER IS AN INSTRUMENT, AND A LYING INSTRUMENT COSTS A HARDWARE NIGHT.
//
// scripts/pull-device-trace.sh runs analyze-join-latency on every collected trace, so whatever it
// prints is the first thing read after a capture — and a 🔴 NEVER CONVERGED banner is exactly the
// kind of result that sends someone back to the loft to re-test a mesh that was never broken.
// These pin the two ways it used to be wrong about a HEALTHY capture.

const ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const SCRIPT = path.join(ROOT, "scripts", "analyze-join-latency.mjs");

const run = (rows) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sv-join-"));
  const file = path.join(dir, "capture.jsonl");
  fs.writeFileSync(file, rows.map((r) => JSON.stringify(r)).join("\n") + "\n");
  try {
    const stdout = execFileSync("node", [SCRIPT, file], { encoding: "utf8" });
    return { code: 0, stdout };
  } catch (err) {
    return { code: err.status, stdout: String(err.stdout || "") };
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
};

// A director who takes the role on page 10 and then simply keeps directing — six page turns over
// five minutes — while both followers receive every page within 200 ms. There is nothing wrong
// here; this is what a good rehearsal looks like.
const healthyRehearsal = () => {
  const T = 1787060000000;
  const pages = [10, 50, 101, 203, 300, 372];
  const rows = [{ t: T, dev: "mPad", role: "director", event: "become:director" }];
  pages.forEach((page, i) => {
    const at = T + i * 30000;
    rows.push({ t: at, dev: "mPad", role: "director", event: "page:send", page });
    for (const dev of ["iPad1", "iPad2"]) {
      // iPad2's telemetry stops partway through — a device that left, lost power, or (at Mass, the
      // normal case) was never on a network able to deliver the rest of its rows.
      if (dev === "iPad2" && i >= 4) continue;
      rows.push({ t: at + 200, dev, role: "follower", event: "mesh:page-recv", page });
    }
  });
  return rows;
};

test("a healthy rehearsal is not reported as slow just because the director kept directing", () => {
  const { code, stdout } = run(healthyRehearsal());
  // Latency was measured from the assert to the LAST page in the window, so it reported however
  // long the director had been directing — 150.2 s here — and flagged every follower "slow".
  assert.doesNotMatch(stdout, /slow/, "a 0.2 s mesh is still being reported as slow");
  assert.match(stdout, /cold join .*median 0\.2s/, "the cold-join number is not measuring the first asserted page");
  assert.equal(code, 0, "a healthy capture must not exit non-zero");
});

test("a follower whose telemetry ends early is not accused of never converging", () => {
  const { code, stdout } = run(healthyRehearsal());
  // iPad2 received every page it was present for, promptly. Judging it against the director's FINAL
  // page turned "this device stopped reporting" into "this device never reached the page" — a red
  // banner and exit 1 on a follower that was in perfect sync the whole time it was there.
  assert.doesNotMatch(stdout, /NEVER CONVERGED/, "an early-ending follower is still being called wedged");
  assert.doesNotMatch(stdout, /🔴/, "a healthy capture still prints the red banner");
  assert.equal(code, 0);
});

test("page-turn latency is reported from the SEND, not from the role assert", () => {
  const { stdout } = run(healthyRehearsal());
  // The number that actually answers "how fast does a page reach a follower" — the one to compare
  // against the project's standard of "longer than a few seconds is a failure".
  assert.match(stdout, /page turns \(send → recv\).*median 0\.20s/,
    "no per-page-turn measurement — the only number offered still spans the whole window");
});

test("a genuinely wedged follower is still caught", () => {
  // The instrument must not be blunted: a follower that receives OTHER pages but never the
  // director's is the real wedge this tool exists to surface.
  const T = 1787060000000;
  const rows = [
    { t: T, dev: "mPad", role: "director", event: "become:director" },
    { t: T + 1000, dev: "mPad", role: "director", event: "page:send", page: 59 },
    { t: T + 1200, dev: "iPad1", role: "follower", event: "mesh:page-recv", page: 59 },
    // iPad2 is stuck on an older page and never receives 59.
    { t: T + 1300, dev: "iPad2", role: "follower", event: "mesh:page-recv", page: 12 },
    { t: T + 5000, dev: "iPad2", role: "follower", event: "mesh:page-recv", page: 12 },
  ];
  const { code, stdout } = run(rows);
  assert.match(stdout, /NEVER CONVERGED/, "a real wedge is no longer reported");
  assert.equal(code, 1, "a real wedge must still exit non-zero");
});

test("the director and the assert time come from the SAME row", () => {
  // These were read from different rows — director from the last page:send, assert from the first —
  // so a window holding two directors named one and timed the other.
  const T = 1787060000000;
  const rows = [
    { t: T, dev: "oldPad", role: "director", event: "page:send", page: 5 },
    { t: T + 60000, dev: "mPad", role: "director", event: "become:director" },
    { t: T + 61000, dev: "mPad", role: "director", event: "page:send", page: 100 },
    { t: T + 61300, dev: "iPad1", role: "follower", event: "mesh:page-recv", page: 100 },
  ];
  const { stdout } = run(rows);
  assert.match(stdout, /director: mPad/, "the wrong device is named as director");
  // Cold join is 1.3 s — mPad's own assert to the first receipt of mPad's first page. The old
  // split-row read timed from oldPad's earlier send and reported 61.3 s for the same capture.
  assert.match(stdout, /cold join .*median 1\.3s/, "latency is timed from a different device's row");
  assert.doesNotMatch(stdout, /61\.3s/, "the assert time is still coming from the other director's row");
});

test("Central time is a real zone conversion, not a hardcoded summer offset", () => {
  // A fixed -5 h is CDT: every 'CT' label printed between early November and early March was an
  // hour late, and these timestamps are read back to place a failure inside a Mass.
  const src = fs.readFileSync(SCRIPT, "utf8");
  assert.doesNotMatch(src, /5 \* 3600 \* 1000/, "the hardcoded CDT offset is back");
  assert.match(src, /timeZone: "America\/Chicago"/, "no explicit zone conversion");
  const resync = fs.readFileSync(path.join(ROOT, "scripts", "analyze-resync.mjs"), "utf8");
  assert.doesNotMatch(resync, /5 \* 3600 \* 1000/, "analyze-resync still hardcodes the CDT offset");
  assert.match(resync, /timeZone: "America\/Chicago"/, "analyze-resync has no explicit zone conversion");
});

test("a follower that joined LATE is not reported as wedged", () => {
  // The mirror of the bug above, and a regression the first fix introduced. Judging every device
  // against the director's FIRST page made any device that arrived after that turn read as NEVER
  // CONVERGED — a red banner and exit 1 on a follower that then tracked every page it was present
  // for. Judging against the LAST page (what that replaced) false-flagged the opposite case. The
  // honest question is whether the director's pages reached it while it was there.
  const T = 1787060000000;
  const pages = [10, 50, 101, 203, 300, 372];
  const rows = [{ t: T, dev: "mPad", role: "director", event: "become:director" }];
  pages.forEach((page, i) => {
    const at = T + i * 30000;
    rows.push({ t: at, dev: "mPad", role: "director", event: "page:send", page });
    rows.push({ t: at + 200, dev: "iPad1", role: "follower", event: "mesh:page-recv", page });
    // iPad3 shows up halfway through and then tracks perfectly.
    if (i >= 3) rows.push({ t: at + 250, dev: "iPad3", role: "follower", event: "mesh:page-recv", page });
  });
  const { code, stdout } = run(rows);
  assert.match(stdout, /iPad3.*joined late — tracked 3\/6 turns/,
    "a late joiner is not described as a late joiner");
  assert.doesNotMatch(stdout, /NEVER CONVERGED/, "a late joiner is still being called wedged");
  assert.equal(code, 0, "a late joiner still exits non-zero");
});

test("the wedge verdict is per-page-turn, not presence at assert time", () => {
  // A follower present the whole time that never once receives a page the director turned to IS
  // wedged, and must still be caught — the change above must not blunt that.
  const T = 1787060000000;
  const rows = [
    { t: T, dev: "mPad", role: "director", event: "become:director" },
    { t: T + 1000, dev: "mPad", role: "director", event: "page:send", page: 59 },
    { t: T + 1200, dev: "iPad1", role: "follower", event: "mesh:page-recv", page: 59 },
    // iPad2 keeps receiving an OLD page and never the director's — the real wedge.
    { t: T + 1300, dev: "iPad2", role: "follower", event: "mesh:page-recv", page: 12 },
    { t: T + 9000, dev: "iPad2", role: "follower", event: "mesh:page-recv", page: 12 },
  ];
  const { code, stdout } = run(rows);
  assert.match(stdout, /never a page the director turned to/, "the real wedge is no longer described");
  assert.equal(code, 1, "a real wedge must still exit non-zero");
});

test("a follower that converges once and then wedges is NOT a green run", () => {
  // The false GREEN that judging on "has at least one successful sample" introduced. One page early
  // on cleared the verdict for the rest of the capture — on the precise failure this tool's own
  // header names: "one iPad sitting on song 11 while the director and everyone else are on 372".
  // A false green on an instrument is worse than the false red it replaced, because nobody looks
  // again.
  const T = 1787060000000;
  const pages = [10, 50, 101, 203, 300, 372];
  const rows = [{ t: T, dev: "mPad", role: "director", event: "become:director" }];
  pages.forEach((page, i) => {
    const at = T + i * 30000;
    rows.push({ t: at, dev: "mPad", role: "director", event: "page:send", page });
    rows.push({ t: at + 200, dev: "iPad1", role: "follower", event: "mesh:page-recv", page });
    // iPad2 gets the first page, then keeps re-receiving it while the director moves on. It is
    // demonstrably present the whole time — it is logging — and demonstrably not following.
    rows.push({ t: at + 300, dev: "iPad2", role: "follower", event: "mesh:page-recv", page: i === 0 ? page : 10 });
  });
  const { code, stdout } = run(rows);
  assert.match(stdout, /iPad2.*WEDGED — present for \d+ turn\(s\) it never received/,
    "a follower that stopped following is not reported as wedged");
  assert.doesNotMatch(stdout, /✅/, "a capture containing a wedged follower still prints the green tick");
  assert.equal(code, 1, "a wedged follower no longer exits non-zero");
});

test("presence is bracketed, so late joiners and early leavers are still not misses", () => {
  // The three cases have to be told apart by the SAME rule, or fixing one re-breaks another — which
  // is exactly what happened twice. A turn counts as missed only if the device logged something
  // before it AND after it.
  const src = fs.readFileSync(SCRIPT, "utf8");
  assert.match(src, /firstRowAt <= s\.t && lastRowAt > s\.t/,
    "presence is no longer bracketed — a late joiner or an early leaver will be called wedged again");
});
