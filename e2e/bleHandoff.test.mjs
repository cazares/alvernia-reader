import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const BEACON = fs.readFileSync("ios/SignoVivo/BlePageBeacon.swift", "utf8");
const MODULE = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

// A missing marker must FAIL, not silently produce a slice from -1 that happens to contain the
// string being looked for. EVERY two-marker read in this file goes through here — the helper was
// added for the one site that had already burned us and left the other eight raw, which is how the
// same slice-to-EOF failure survived one test below it. (Single-marker reads are open-ended by
// design and cannot mis-bound; they still assert their start exists via indexOf returning >= 0
// wherever a match is required.)
/**
 * The body of a Swift `func <name>`, brace-matched. Whole-file `assert.match` is how most of the
 * guards in this file went decorative: a byte-identical line elsewhere satisfied the regex while
 * the copy that mattered was deleted. Scope to the function that actually implements the rule.
 */
const braceBlock = (src, at, what) => {
  const open = src.indexOf("{", at);
  assert.ok(open >= at && open !== -1, `no block found for ${what}`);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}") { depth--; if (depth === 0) return src.slice(open, i + 1); }
  }
  assert.fail(`unbalanced braces in ${what}`);
};

const swiftFunc = (name, src) => {
  const at = src.indexOf(`func ${name}(`);
  assert.ok(at > 0, `func ${name} is gone`);
  return braceBlock(src, at, `func ${name}`);
};

const slice = (from, to, src = BEACON) => {
  const a = src.indexOf(from);
  const b = src.indexOf(to, a + 1);
  assert.ok(a >= 0, `slice start marker is gone: ${from}`);
  assert.ok(b > a, `slice end marker is gone or precedes the start: ${to}`);
  return src.slice(a, b);
};

// WHY THIS FILE EXISTS. BLE is the only path that can show a follower the right page BEFORE the
// mesh finishes its ~10s first handshake, and it never once did — because the seq guard exists in
// TWO layers and only one of them knew about advertising sessions.
//
// Swift cannot run here, so this models both guards and plays a director handover through them.
// That is stronger than grepping for the fix: the model FAILS on the old logic and passes on the
// new, so it is testing the behaviour rather than the presence of a line.

// ── The two guards, transliterated ────────────────────────────────────────────
const CONTENTION_WINDOW = 4;
const makeBeacon = () => ({ nonce: "", seq: -1, recent: new Map() });
const beaconRecv = (b, adv, now = 0) => {
  // Contention: two live advertisers => abstain entirely.
  b.recent.set(adv.nonce, now);
  for (const [n, t] of b.recent) if (now - t > CONTENTION_WINDOW) b.recent.delete(n);
  if (b.recent.size > 1) return null;
  if (adv.nonce !== b.nonce) { b.nonce = adv.nonce; b.seq = -1; }   // new advertiser => rebase
  if (!(adv.seq > b.seq)) return null;                              // monotonic WITHIN a session
  b.seq = adv.seq;
  return { page: adv.page, seq: adv.seq, nonce: adv.nonce };
};

// DirectorSyncModule.onPage. `rebases` models the fix; false reproduces what shipped in 448-452.
const makeModule = () => ({ appliedSeq: -1, appliedNonce: "" });
const moduleRecv = (m, hit, rebases) => {
  if (!hit) return null;
  if (rebases && hit.nonce !== m.appliedNonce) { m.appliedNonce = hit.nonce; m.appliedSeq = -1; }
  if (!(hit.seq > m.appliedSeq)) return null;   // the guard that silently ate every page
  m.appliedSeq = hit.seq;
  return hit.page;
};

// A director advertises pages; seq ALWAYS restarts at 0 per launch (bleSeq = 0 in Swift).
const run = (rebases) => {
  const b = makeBeacon(), m = makeModule();
  const rendered = [];
  let clock = 0;
  const advertise = (nonce, pages) => pages.forEach((page, i) => {
    clock += 1;
    const out = moduleRecv(m, beaconRecv(b, { nonce, seq: i + 1, page }, clock), rebases);
    if (out !== null) rendered.push(out);
  });
  advertise("aaaa", [10, 11, 12, 13, 14]);   // director A: seq 1..5
  const afterA = rendered.length;
  clock += CONTENTION_WINDOW + 1;            // A goes quiet: no contention
  advertise("bbbb", [372]);                  // director B takes over: seq restarts at 1
  return { afterA, rendered, sawNewDirector: rendered.length > afterA };
};

test("a new director's page reaches the follower over BLE", () => {
  const r = run(true);
  assert.equal(r.afterA, 5, "the first director's pages did not render");
  assert.ok(r.sawNewDirector, "the NEW director's page never rendered — BLE cannot cover the mesh gap");
  assert.equal(r.rendered.at(-1), 372, `last rendered page was ${r.rendered.at(-1)}, expected 372`);
});

test("THE BUG: without the module-level rebase, the handover is silently dropped", () => {
  // Proves this test can fail. Under the shipped logic the beacon rebased and handed up seq 1,
  // the module compared 1 > 5, and the page vanished with no error and no log — identical from the
  // outside to BLE being switched off. It stayed dropped until the new director's seq climbed past
  // the old one, i.e. for about as many page turns as the previous director had made.
  const r = run(false);
  assert.equal(r.afterA, 5, "the first director's pages did not render even in the broken model");
  assert.equal(r.sawNewDirector, false,
    "the broken model rendered the handover — the model no longer reproduces the bug it guards");
});

test("both layers carry the nonce, so neither can drift from the other", () => {
  // The 448 fix landed in the beacon only. The contract is now explicit in the callback signature,
  // which is what makes a repeat of that half-landing a compile error rather than a silent regression.
  assert.match(BEACON, /var onPage: \(\(Int, Int, String\) -> Void\)\?/,
    "onPage no longer carries the nonce — a caller can go back to guessing across sessions");
  assert.match(BEACON, /onPage\?\(parsed\.page, parsed\.seq, parsed\.nonce\)/, "the nonce is not passed through");
  assert.match(BEACON, /if parsed\.nonce != lastSeenNonce/, "the beacon no longer rebases on a new advertiser");

  assert.match(MODULE, /private var bleAppliedNonce = ""/, "the module tracks no advertising session");
  assert.match(MODULE, /onPage = \{ \[weak self\] page, seq, nonce in/, "the module ignores the nonce again");
  const handler = slice("self.bleBeacon.onPage = {", "A FOLLOWER MUST NEVER ADVERTISE", MODULE);
  assert.match(handler, /if nonce != self\.bleAppliedNonce \{[\s\S]*?self\.bleAppliedSeq = -1/,
    "the module does not reset its seq on a new advertiser — the exact half-landing this fixes");
  // Order matters: rebase must happen BEFORE the monotonic guard, or it changes nothing. Presence
  // of BOTH operands is asserted first: a bare `a < b` on offsets reports -1 for a deleted marker,
  // and -1 is less than everything, so the check would pass precisely because the code went missing.
  const rebaseAt = handler.indexOf("bleAppliedSeq = -1");
  const monotonicAt = handler.indexOf("guard seq > self.bleAppliedSeq");
  assert.ok(rebaseAt >= 0, "the rebase that clears the seq floor on a new advertiser is gone");
  assert.ok(monotonicAt >= 0, "the within-session monotonic guard is gone");
  assert.ok(rebaseAt < monotonicAt,
    "the rebase runs AFTER the monotonic guard, so the guard still rejects the first page");
});

test("the safety rules that made BLE dangerous in 444 are still in place", () => {
  // BLE renders without a handshake, so it has no freshness guarantee of its own. Two rules keep it
  // safe, and a rebase must not quietly remove either.
  const handler = slice("self.bleBeacon.onPage = {", "A FOLLOWER MUST NEVER ADVERTISE", MODULE);
  assert.match(handler, /guard !self\.lastKnownBookId\.isEmpty/,
    "BLE can render a page number with no known book — the one unrecoverable failure in this app");
  assert.match(handler, /guard seq > self\.bleAppliedSeq/,
    "the within-session monotonic guard is gone — a stale packet can drag a follower backwards");
  // Legacy 2-field advertisements must stay rejected: pre-448 devices never stop advertising.
  // Since the HMAC tag (#374) the only accepted shape is the 4-field "SV<nonce>.<seq>.<page>.<tag>";
  // the pre-tag 3-field form is rejected too (a 448-455 device can't prove it knows the session code).
  // PIN THE REJECTION, not the count. `guard parts.count == 4` also matches a LENIENT parse that
  // falls through to `else { return (nonce, seq, page) }` — i.e. accepts the pre-tag 3-field form
  // unverified, which is the build-444 wrong-song failure exactly. Measured: that mutation left all
  // 15 tests in this file green.
  assert.match(
    swiftFunc("parse", BEACON),
    /guard parts\.count == 4,[^\n]*else \{ return nil \}/,
    "a malformed or pre-tag advertisement is no longer REJECTED — 444's frozen page renders again",
  );
  assert.doesNotMatch(BEACON, /parts\.count == 2\b/, "the 2-field legacy format must not be parsed");
  assert.doesNotMatch(BEACON, /parts\.count == 3\b/, "the 3-field pre-tag format must not be parsed");
  // And a device that stops directing must stop advertising.
  // BOTH call sites, named. This was a whole-file substring search with two call sites, so deleting
  // the one that carries the fix — resetTransport, the actual build-444 remedy — left it green while
  // a device that directed song 357 and changed role kept broadcasting 357 forever.
  assert.equal(
    (MODULE.match(/bleBeacon\.stopPublishing\(\)/g) || []).length, 2,
    "a bleBeacon.stopPublishing() call site was added or removed — name the new one here",
  );
  assert.match(swiftFunc("resetTransport", MODULE), /bleBeacon\.stopPublishing\(\)/,
    "resetTransport no longer silences the beacon — this IS the build-444 ghost");
});


test("TWO LIVE ADVERTISERS: render NEITHER, rather than flapping or trusting the wrong one", () => {
  // The edge case the nonce rebase OPENED. Before it, the stale seq guard suppressed the second
  // advertiser by accident; after it, alternating packets each rebase the floor and every one
  // applies — the follower ping-pongs between two pages, several times a second.
  //
  // The tempting fix, preferring whichever advertiser was seen first, is WORSE: a backgrounded or
  // crashed director keeps advertising, and stickiness would pin every follower to its dead page
  // permanently. That is the 444 wrong-song flash turned into a wrong-song hold.
  //
  // BLE carries no authority — nothing in the packet proves the sender is the director — so the
  // only safe answer under contention is silence. It costs a few seconds of acceleration and the
  // mesh resolves the conflict by token anyway.
  const b = makeBeacon(), m = makeModule();
  const rendered = [];
  let clock = 0;
  for (let i = 1; i <= 6; i++) {           // A and B interleaved, both live
    clock += 1;
    for (const [nonce, page] of [["aaaa", 100], ["bbbb", 372]]) {
      const out = moduleRecv(m, beaconRecv(b, { nonce, seq: i, page }, clock), true);
      if (out !== null) rendered.push(out);
    }
  }
  assert.deepEqual(rendered, [100],
    `contested advertisers rendered ${JSON.stringify(rendered)} — expected at most the first, then silence`);

  // And once the loser goes quiet, the survivor is adopted rather than being locked out forever.
  clock += CONTENTION_WINDOW + 1;
  const out = moduleRecv(m, beaconRecv(b, { nonce: "bbbb", seq: 7, page: 372 }, clock), true);
  assert.equal(out, 372, "after contention clears, the remaining advertiser is still ignored");

  // …AND the Swift must actually implement what the model above describes. Everything above this
  // line exercises a JS transliteration, so every Swift mutation left it green — including deleting
  // the abstain-under-contention `return` outright, and deleting the whole post-contention cooldown
  // (the 2026-08-18 hardening added after a live packet capture).
  const didDiscover = swiftFunc("centralManager", BEACON);
  const contentionAt = didDiscover.indexOf("if recentNonces.count > 1 {");
  assert.ok(contentionAt > 0, "the contention branch is gone — contested packets are applied");
  // Brace-matched, not indexOf("}") — the first closing brace belongs to the nested contention LOG,
  // so a naive slice stops before the `return` and passes on a branch that no longer abstains.
  assert.match(braceBlock(didDiscover, contentionAt, "contention branch"), /\breturn\b/,
    "a contested packet no longer abstains — followers ping-pong between two advertisers");
  const cooldownAt = didDiscover.indexOf("if now - lastContentionAt < Self.contentionCooldown {");
  assert.ok(cooldownAt > 0,
    "the post-contention cooldown is gone — a rival slower than contentionWindow resumes rendering");
  assert.match(braceBlock(didDiscover, cooldownAt, "cooldown branch"), /\breturn\b/,
    "the cooldown no longer abstains");
});

test("the advertised seq is bounded by page turns, not by wall-clock seconds", () => {
  // sendPageUpdate runs at 1 Hz from the director heartbeat. Passing ITS counter meant the number
  // in a fixed-size BLE name grew all Mass — ~5400 after 90 minutes and never resetting — in a
  // 31-byte advertisement already carrying a 128-bit service UUID.
  //
  // WHAT THE OLD ASSERTION MISSED. It was a bare offset comparison,
  // `body.indexOf("guard page != lastPublishedPage") < body.indexOf("advertSeq += 1")`. Delete the
  // early return — the exact regression this test names — and the left indexOf returns -1, and
  // -1 < 30 is true, so the assertion passed BECAUSE the code it was looking for was gone. Presence
  // is asserted first now, and the operator with it: an early return that no longer returns, or a
  // `==` where a `!=` belongs, is the same bug written differently.
  assert.match(BEACON, /private var advertSeq = 0/, "the beacon does not own its seq");
  assert.match(BEACON, /func publish\(page: Int\)/, "publish still takes a caller-supplied seq");
  const body = slice("func publish(page: Int)", "\n  }");
  assert.match(body, /guard\s+page\s*!=\s*lastPublishedPage\s+else\s*\{\s*return\s*\}/,
    "publish() no longer early-returns on an unchanged page — the 1 Hz sendPageUpdate heartbeat " +
      "now bumps advertSeq and tears down the advertiser once per second, ~5400 times a Mass");
  const guardAt = body.indexOf("guard page != lastPublishedPage");
  const bumpAt = body.indexOf("advertSeq += 1");
  const rememberAt = body.indexOf("lastPublishedPage = page");
  assert.ok(guardAt >= 0 && bumpAt >= 0 && rememberAt >= 0,
    `publish() lost a required statement (guard ${guardAt}, bump ${bumpAt}, remember ${rememberAt})`);
  assert.ok(guardAt < bumpAt,
    "seq increments even when the page has not changed — that is the unbounded growth again");
  assert.ok(guardAt < rememberAt,
    "the remembered page is written before the guard reads it, so the guard can never fire");
  // Nothing may reach the air before that guard, or the teardown/restart storm returns even with
  // the seq held still. The guard has to be the FIRST executable statement in the function.
  const preamble = body.slice(body.indexOf("{") + 1, guardAt).replace(/\/\/[^\n]*/g, "").trim();
  assert.equal(preamble, "",
    `publish() does work before the unchanged-page guard: ${JSON.stringify(preamble)}`);
  assert.match(MODULE, /publish\(page: self\.currentPageNumber \?\? page\.intValue\)/,
    "the module still passes its own seq");
  assert.doesNotMatch(MODULE, /private var bleSeq/, "the module's per-second counter is back");
  // A new session must restart the count, or a re-promoted director resumes a stale ladder.
  const stop = BEACON.slice(BEACON.indexOf("func stopPublishing()"));
  assert.match(stop.slice(0, stop.indexOf("\n  }")), /advertSeq = 0/, "stopPublishing does not reset the seq");
});


test("BLE self-heals if the radio goes quiet without telling us", () => {
  // THE FAILURE CLASS: both sides can stop working with no callback, no error, and no way to notice
  // from inside the app. A director's advertisement is only ever (re)started by publish(), which
  // early-returns on an unchanged page — so a director that went dark stayed dark until it happened
  // to turn a page. A follower's scan is guarded by a LOCAL bool, so if iOS stopped the scan the
  // guard refused to restart it and the device was deaf while believing it was listening.
  assert.match(BEACON, /func ensureAdvertising\(\)/, "nothing re-asserts a lost advertisement");
  assert.match(BEACON, /func ensureScanning\(\)/, "nothing re-asserts a lost scan");

  // Must ask the FRAMEWORK, not our own memory — the local flag is exactly what lies here.
  const adv = BEACON.slice(BEACON.indexOf("func ensureAdvertising()"));
  assert.match(adv.slice(0, adv.indexOf("\n  }")), /!p\.isAdvertising/,
    "ensureAdvertising trusts a local flag instead of CBPeripheralManager.isAdvertising");
  const scan = BEACON.slice(BEACON.indexOf("func ensureScanning()"));
  const scanBody = scan.slice(0, scan.indexOf("\n  }"));
  assert.match(scanBody, /!c\.isScanning/,
    "ensureScanning trusts the local isScanning bool — the very flag that goes stale");
  assert.match(scanBody, /isScanning = false/,
    "the stale local flag is not cleared, so scanIfReady's guard still blocks the restart");

  // And both must be DRIVEN by something that ticks regardless of page changes.
  assert.match(MODULE, /bleBeacon\.ensureAdvertising\(\)/, "the director never re-asserts advertising");
  assert.match(MODULE, /bleBeacon\.ensureScanning\(\)/, "the follower never re-asserts scanning");
  // The follower hook belongs in the watchdog, which runs while HUNTING — the state that needs it.
  const wd = MODULE.slice(MODULE.indexOf("private func startFollowerWatchdog"));
  assert.match(wd.slice(0, 2500), /bleBeacon\.ensureScanning\(\)/,
    "ensureScanning is not on the follower watchdog, so it does not run while hunting");
});

test("a follower scans continuously — BLE is never switched off by connecting", () => {
  // If connecting to the mesh stopped the scan, BLE would cover the first gap and then be dead for
  // every later one — a wedged session, a director restart, a follower that drops. It is the
  // fallback precisely for the moments the mesh is not working.
  //
  // This used to assert that stopScanning() is called NOWHERE, which enforced the property by
  // enforcing that the scan is literally never stoppable — so every device, including a director
  // that can never consume a BLE page, ran an allowDuplicates scan for the life of the process. The
  // real invariant is narrower and is what is pinned now: nothing on a CONNECTION path stops the
  // scan, and any path that leaves the device following re-arms it.
  const connectionPaths = [
    slice("case .connected:", "case .connecting:", MODULE),
    slice("private func reconsiderFollowerTarget", "private func handleDirectorConflict", MODULE),
    slice("private func forceFollowerReconnect", "private func sendFollowerHelloIfNeeded", MODULE),
    MODULE.slice(
      MODULE.indexOf("didReceiveInvitationFromPeer peerID"),
      MODULE.indexOf("func advertiser(_ advertiser: MCNearbyServiceAdvertiser, didNotStartAdvertisingPeer"),
    ),
  ];
  for (const path of connectionPaths) {
    assert.ok(path.length > 0, "a connection path could not be located — the slice markers drifted");
    assert.doesNotMatch(path, /bleBeacon\.stopScanning\(\)/,
      "a connection path stops the scan — BLE would stop covering everything after the first connection");
  }
  // The ONLY place it may be stopped is the full transport teardown, which every role transition
  // runs — and which is immediately followed by beginFollowing() re-arming the scan for a follower.
  const stops = MODULE.match(/bleBeacon\.stopScanning\(\)/g) || [];
  assert.equal(stops.length, 1, "stopScanning is called from somewhere other than resetTransport");
  const reset = slice("private func resetTransport", "// MARK: - Event emission", MODULE);
  assert.match(reset, /bleBeacon\.stopScanning\(\)/, "the one stop is not the transport teardown");
});

test("every path that ends as a follower re-arms the BLE scan and the watchdog", () => {
  // THE DRIFT THIS CLOSES. approveDirectorTakeover set currentRole = "follower" and started the mesh
  // transports, but never gave the beacon a session code, never started scanning, never started the
  // BLE health timer and never started the follower watchdog — so handing over control produced a
  // follower with no BLE and no automatic recovery of any kind. It only looked fine because nothing
  // stopped the scan left running from that device's previous follower stint.
  const follow = slice("private func beginFollowing", "@objc(stop:rejecter:)", MODULE);
  for (const required of [
    "bleBeacon.sessionCode = normalizedSessionCode",
    "bleBeacon.startScanning()",
    "startBleHealthTimer()",
    "startFollowerWatchdog()",
  ]) {
    assert.ok(follow.includes(required), `beginFollowing no longer does: ${required}`);
  }
  // Both entry points must go through it, or they can drift apart again.
  for (const entry of ["func startFollower", "func approveDirectorTakeover"]) {
    const idx = MODULE.indexOf(entry);
    assert.ok(idx > 0, `${entry} is gone`);
    const body = MODULE.slice(idx, idx + 2000);
    assert.match(body, /beginFollowing\(sessionCode:/,
      `${entry} builds its own follower state instead of using beginFollowing`);
  }
});


test("neither radio is created on the critical path", () => {
  // THE SUB-1s REQUIREMENT LIVES OR DIES HERE. CBPeripheralManager was created lazily inside
  // publish(), so CoreBluetooth's power-on (~200-500ms, more if it prompts for permission) sat
  // between "Braulio taps Ser Director" and "the page is on the air" — the exact moment the beacon
  // exists to make fast. Both radios are now brought up at startup, in BOTH roles: a follower needs
  // the central to hear the first advertisement, and the peripheral too, because any follower may
  // become the director a second later.
  // Its BODY, not its name. Gutting primeRadios to create only the central — breaking the test's own
  // stated reason, "any follower may become the director a second later" — left every assertion here
  // green.
  const prime = swiftFunc("primeRadios", BEACON);
  assert.match(prime, /central\s*==\s*nil/, "primeRadios no longer warms the CENTRAL — a follower goes deaf on boot");
  assert.match(prime, /peripheral\s*==\s*nil/,
    "primeRadios no longer warms the PERIPHERAL — becoming director pays the cold start this exists to remove");
  assert.match(MODULE, /bleBeacon\.primeRadios\(\)/, "primeRadios is never called");
  const follower = MODULE.slice(MODULE.indexOf("func startFollower("));
  const fEnd = MODULE.indexOf("\n  func ", MODULE.indexOf("func startFollower(") + 1);
  assert.match(MODULE.slice(MODULE.indexOf("func startFollower("), fEnd), /primeRadios\(\)/,
    "a follower does not warm its radios, so becoming director pays a cold start");
  const director = MODULE.slice(MODULE.indexOf("func startDirector("));
  assert.match(director.slice(0, 2000), /primeRadios\(\)/,
    "a director that never followed first still pays a cold start");

  // And the trace must be able to PROVE it, or "it should be fast" is just a claim.
  assert.match(BEACON, /"coldRadio": cold/, "nothing reports when a publish hit a cold radio");
  assert.match(BEACON, /ble:on-air/, "nothing measures request -> on-air latency");
});


test("BLE health does not depend on the mesh it is a fallback FOR", () => {
  // ensureScanning lived on the follower watchdog, which stops on disconnect, on backgrounding and
  // on every transport reset. So the fast path went deaf at exactly the moments the slow path was
  // already failing — which is when it was the only thing still working. A fallback coupled to the
  // health of the thing it backs up is not a fallback.
  assert.match(MODULE, /private func startBleHealthTimer\(\)/, "BLE has no independent health timer");
  const t = MODULE.slice(MODULE.indexOf("private func startBleHealthTimer()"));
  const body = t.slice(0, t.indexOf("\n  private func"));
  assert.match(body, /ensureAdvertising\(\)/, "a director never re-asserts its beacon");
  assert.match(body, /ensureScanning\(\)/, "a follower never re-asserts its scan");
  // It must NOT be gated on sessions, peers or connection state — that is the whole point.
  assert.doesNotMatch(body, /connectedDirectorPeer|mcSessions|discoveredDirectors/,
    "the BLE health timer is gated on mesh state, which re-couples it to what it backs up");
  // Torn down only with the transport itself.
  const reset = MODULE.slice(MODULE.indexOf("private func resetTransport"));
  assert.match(reset.slice(0, 1200), /bleHealthTimer\?\.invalidate\(\)/, "the timer leaks past a reset");
});

test("losing the director resumes HUNTING, with the pulse still running", () => {
  // The disconnect path stopped the follower watchdog, so the 0.5 Hz retry, the BLE scan self-heal
  // and the wedged-session escalation all died at the moment reconnection began. Recovery fell back
  // to one retry and then the 5-12s discovery cadence. Same mistake as forceFollowerReconnect, in a
  // second location — which is why the test asserts the behaviour rather than one call site.
  // ALL THREE connected -> hunting TRANSITIONS, not whichever one an ambiguous anchor happened to
  // land on. The marker below appears twice (lostPeer and .notConnected), and slicing from its first
  // occurrence silently re-pointed this test at a different branch the moment the other one changed.
  // Each transition must restore the same set, which is the actual invariant — the file has now
  // forgotten a different member of it three separate times.
  const transitions = [
    ["lostPeer", slice("func browser(_ browser: MCNearbyServiceBrowser, lostPeer", "func browser(_ browser: MCNearbyServiceBrowser, didNotStart", MODULE)],
    [".notConnected", slice("EVICT A REPEATEDLY-FAILING TARGET", "} else if self.currentRole == \"director\" {", MODULE)],
    ["forceFollowerReconnect", slice("private func forceFollowerReconnect", "private func sendFollowerHelloIfNeeded", MODULE)],
  ];
  for (const [name, body] of transitions) {
    const code = body.replace(/\/\/.*$/gm, "");
    assert.match(code, /startFollowerWatchdog\(\)/, `${name} resumes hunting without the retry pulse`);
    assert.doesNotMatch(code, /stopFollowerWatchdog\(\)/, `${name} stops the pulse on the path that begins reconnecting`);
    assert.match(code, /followerHuntingSince = Date\(\)\.timeIntervalSince1970/,
      `${name} does not restart the wedged-session clock, so escalation never fires after a drop`);
    assert.match(code, /resumeDiscoveryRefreshAfterDisconnect\(\)/,
      `${name} leaves the discovery refresh paused — pauseDiscoveryRefreshWhileConnected promises it restarts on a drop`);
  }
});

// ── The model that justifies leaving the deafness gap OPEN ───────────────────────────────────
// A future session WILL look at "a re-entering follower is BLE-deaf until the next page turn" and
// reach for the same cure. This runs the two candidate cures against the ghost that actually exists
// on this hardware, so the answer is reproducible rather than a claim in a comment.
const runGuard = ({ redeliverOnReset, wipeOnReset }) => {
  const b = { nonce: "", seq: -1, redeliver: false, recent: new Map(), contendedAt: -Infinity };
  const recv = (adv, now) => {
    b.recent.set(adv.nonce, now);
    for (const [n, t] of b.recent) if (now - t > CONTENTION_WINDOW) b.recent.delete(n);
    if (b.recent.size > 1) { b.contendedAt = now; return null; }
    if (now - b.contendedAt < CONTENTION_WINDOW) return null;
    if (adv.nonce !== b.nonce) { b.nonce = adv.nonce; b.seq = -1; }
    const ok = adv.seq > b.seq || (b.redeliver && adv.seq === b.seq);
    if (!ok) return null;
    b.redeliver = false;
    b.seq = Math.max(b.seq, adv.seq);
    return adv.page;
  };
  // A device force-quit while directing song 357: bluetoothd keeps broadcasting a validly-tagged,
  // FROZEN packet with no in-process owner (recorded on hardware 2026-08-19).
  const ghost = { nonce: "G", seq: 4, page: 357 };
  let t = 0;
  const applied = [];
  for (let i = 0; i < 3; i++) { const p = recv(ghost, ++t); if (p !== null) applied.push(p); }
  // …the mesh has since corrected the screen to the real page. Now a role change asks for a refresh.
  if (wipeOnReset) { b.nonce = ""; b.seq = -1; b.recent.clear(); b.contendedAt = -Infinity; }
  if (redeliverOnReset) b.redeliver = true;
  const afterReset = recv(ghost, ++t);
  return { firstSightings: applied, afterReset };
};

test("BOTH candidate cures for the deafness gap re-apply a frozen ghost page", () => {
  // Blunt cure: forget the advertiser and the seq floor.
  assert.equal(runGuard({ wipeOnReset: true }).afterReset, 357,
    "the model no longer reproduces the blunt cure's failure — re-check it before trusting this file");
  // Narrow cure: allow ONE re-delivery of an EQUAL seq from the SAME nonce, never an older one.
  // A frozen ghost's seq sits exactly AT the baseline, so this describes it precisely.
  assert.equal(runGuard({ redeliverOnReset: true }).afterReset, 357,
    "the narrow cure no longer re-applies the ghost — if this is genuinely fixed, the gap can close");
  // And with NO reset — what ships — the ghost stays suppressed after its first sighting.
  assert.equal(runGuard({}).afterReset, null, "the shipped guard leaks a frozen page");
});

test("the BLE scan baseline is never reset on a role change", () => {
  // A resetScanBaseline() was written to cure a real gap — a device re-entering follower mode while
  // the SAME director advertises an unchanged nonce+seq is BLE-deaf until the next page turn — and
  // then removed, because every version of it re-armed the build-444 wrong-song failure.
  //
  // Blunt version (wipe nonce + seq): the first advertisement heard after any role change is applied
  // whatever its age. Narrow version (allow ONE re-delivery of an EQUAL seq from the same nonce): no
  // better against the case that matters, because a device force-quit while directing leaves
  // bluetoothd broadcasting a frozen, validly-tagged page (recorded on hardware 2026-08-19) whose
  // seq sits exactly AT the baseline — "equal seq from the advertiser I was already tracking"
  // describes that ghost precisely. Modelled: suppressed packet after packet, then re-delivered the
  // instant a role change asks for a refresh.
  //
  // A stationary director and a ghost are byte-identical in a BLE advertisement, so the distinction
  // the cure needs does not exist. The mesh stays authoritative; the gap stays open on purpose.
  assert.ok(!/func resetScanBaseline\s*\(/.test(BEACON), "resetScanBaseline is back — it re-arms the 444 ghost window");
  assert.doesNotMatch(BEACON, /redeliverCurrentPage/, "the one-shot re-delivery is back — a frozen ghost matches it exactly");
  const reset = slice("private func resetTransport", "// MARK: - Event emission", MODULE);
  assert.doesNotMatch(reset, /bleAppliedSeq = -1/,
    "resetTransport drops the module's BLE seq floor — the first packet after a role change will be applied whatever its age");
});

test("a director does not scan — and cannot be made to by a radio restart", () => {
  // centralManagerDidUpdateState fires again on every bluetoothd restart and Control Center toggle,
  // and used to call scanIfReady() with no notion of role — silently resuming an allowDuplicates
  // packet-rate scan on the director for the rest of the Mass, which is the whole drain that
  // stopping the scan exists to prevent.
  assert.match(BEACON, /private var wantsScanning = false/, "there is no record of scan INTENT");
  const ready = slice("private func scanIfReady", "func resumeOnForeground");
  assert.match(ready, /guard wantsScanning/, "scanIfReady scans regardless of whether we want to");
  const ensure = slice("func ensureScanning", "private func startAdvertisingIfReady");
  assert.match(ensure, /guard wantsScanning/, "the 1 Hz self-heal re-arms a deliberately stopped scan");
  // Bounded by a marker that is ASSERTED to exist. This used to end at a doc-comment that the
  // resetScanBaseline revert deleted, so indexOf returned -1, the slice ran to end-of-file, and the
  // assertion below passed by finding `wantsScanning = false` somewhere else entirely — a test
  // reporting green for a property it had stopped checking, which is the failure this whole file
  // exists to prevent.
  const stop = slice("func stopScanning", "/// NOT PROVIDED, DELIBERATELY");
  assert.match(stop, /wantsScanning = false/, "stopping the scan does not clear the intent");
});
