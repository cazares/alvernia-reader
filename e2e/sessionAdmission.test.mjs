import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

// ONE DIRECTOR, ONE FOLLOWER, ONE SESSION — ENFORCED, NOT ASSUMED.
//
// maxFollowersPerSession went 7 -> 1 (#370) because Multipeer full-meshes every member of a
// session: with two followers in one MCSession they are connected to EACH OTHER, which is the
// phantom-page-source exposure behind "three followers converged on the same wrong song". That
// change bought structural isolation, and the module's own comment says the peer-not-director branch
// "should now be unreachable in practice — if it ever fires again, the 1-follower-per-session
// topology has a hole."
//
// It had one. MCSession.connectedPeers omits peers still shaking hands, so for the ~1 s a handshake
// takes the session reads as empty — and at pre-Mass setup every follower self-invites off the same
// advertisement burst. All of them were admitted to mcSessions[0].
//
// These pin the reservation that closes it, INCLUDING the two ways the first draft of the
// reservation was itself wrong (found by re-hunting the fix).
//
// A third re-hunt found the pins themselves wrong in three ways, all in how they read the director
// predicate out of the Swift: one regression it could not see, and two behaviour-neutral edits it
// reported as regressions. Those are fixed below, and the reader that got them wrong now refuses
// out loud instead of guessing whenever the source takes a shape it cannot score.

const MODULE = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

const fn = (name, end) => {
  const start = MODULE.indexOf(name);
  assert.ok(start > 0, `${name} is gone`);
  const stop = MODULE.indexOf(end, start);
  assert.ok(stop > start, `could not bound ${name}`);
  return MODULE.slice(start, stop);
};

test("occupancy counts peers still shaking hands, not just connected ones", () => {
  const body = fn("private func availableSessionForNewFollower", "private func pagePayload");
  assert.match(body, /connectedPeers\.count \+ admissionCount/,
    "occupancy is back to connectedPeers alone — simultaneous joiners will share a session again");
});

test("the slot is claimed BEFORE the invitation is answered", () => {
  // Reserving after invitationHandler(true, …) would leave the same window open.
  const accept = fn('self.dbgLog("invite:accept", ["from": peerID.displayName])', "invite:reject");
  const reserveIdx = accept.indexOf("reserveSlot");
  const answerIdx = accept.indexOf("invitationHandler(true");
  assert.ok(reserveIdx > 0, "the accept path no longer reserves a slot");
  assert.ok(reserveIdx < answerIdx, "the slot is reserved AFTER answering — the race is still open");
});

test("ONLY a completed handshake releases the reservation, and only for its own session", () => {
  // Releasing on .notConnected as well looked like tidy bookkeeping and was a second ABA door: a
  // failed handshake is exactly when a stale or duplicate terminal callback is most likely (MPC
  // delivers them for torn-down sessions, and forceFollowerReconnect and resetTransport rebuild
  // sessions routinely), so it handed the slot away while a RETRY's handshake was still in flight.
  // The reservation lapses on its own tokened expiry instead, which nothing outside the class can
  // trigger early.
  const body = fn("func session(_ session: MCSession, peer peerID: MCPeerID, didChange", "case .connected:");
  assert.match(body, /if state == \.connected \{ self\.releaseSlot\(peerID, in: session\) \}/,
    "a failed handshake frees the slot again, or the release ignores which session it came from");
  assert.doesNotMatch(body, /state != \.connecting/, "the release is back on every terminal state");

  const rel = fn("private func releaseSlot", "/// The session to answer");
  assert.match(rel, /held\.session === session/,
    "releaseSlot ignores the session, so a callback for a torn-down one frees a live reservation");
});

test("a peer already connected here is admitted back into THAT session, not a new one", () => {
  // A follower whose link went half-open on its side re-invites while the director still counts it
  // as connected. Without this it was handed a different session and became a member of two at
  // once — burning a session and putting two peers where the topology permits one. The legacy
  // invite path has always guarded this; the accept path never did.
  const admit = fn("private func sessionForAdmitting", "private func availableSessionForNewFollower");
  assert.match(admit, /mcSessions\.first\(where: \{ \$0\.connectedPeers\.contains\(peerID\) \}\)/,
    "an already-connected peer can still be routed into a second session");
  // …and it must be checked BEFORE falling through to a fresh session.
  assert.ok(admit.indexOf("connectedPeers.contains(peerID)") < admit.indexOf("availableSessionForNewFollower()"),
    "the already-connected check runs after a new session has been chosen");
});

// ── REGRESSIONS IN THE FIRST DRAFT OF THE FIX ────────────────────────────────────────────────

test("a retrying follower re-uses its session instead of consuming a new one each time", () => {
  // A follower re-invites every inviteRetryAfter (2.5 s). When reservations were held per SESSION,
  // the peer's own slot made the session it had already been offered look full, so each retry was
  // answered with a newly created session. One follower held four or five at once and a couple of
  // them exhausted maxSessions (12) — after which the director answered every invitation, from
  // every device, with "sessions-full". A worse outage than the bug being fixed.
  const admit = fn("private func sessionForAdmitting", "private func availableSessionForNewFollower");
  assert.match(admit, /pendingAdmissions\[peerID\]/, "sessionForAdmitting no longer checks for an existing reservation");
  assert.match(admit, /mcSessions\.contains\(where: \{ \$0 === held\.session \}\)/,
    "a reservation pointing at a torn-down session would be reused");
  // Both admission paths must go through it.
  assert.ok(MODULE.includes("if let session = self.sessionForAdmitting(peerID) {"),
    "the accept path picks a fresh session per retry instead of re-using the peer's own");
  const legacy = fn("let isLegacyFollower", "// Modern follower");
  assert.match(legacy, /sessionForAdmitting\(peerID\)/,
    "the legacy invite path still allocates a session per foundPeer — it re-fires on every browser restart");
});

test("an expired reservation cannot cancel a NEWER one for the same peer", () => {
  // ABA. The expiry fires on a schedule; a peer that connects, drops and re-invites inside the
  // window would have its new reservation released by its old timer, freeing a slot whose handshake
  // is still in flight — re-opening the double-admission this mechanism exists to prevent.
  const body = fn("private func reserveSlot", "private func releaseSlot");
  assert.match(body, /admissionToken &\+= 1/, "reservations are no longer tokened");
  assert.match(body, /self\.pendingAdmissions\[peerID\]\?\.token == token/,
    "the expiry no longer checks it is releasing the reservation it created");
  assert.match(body, /resetGeneration == generation/, "the expiry survives a transport reset");
});

test("a reservation is per peer, so it can never be double-released or double-held", () => {
  assert.match(MODULE, /private var pendingAdmissions: \[MCPeerID: \(session: MCSession, token: Int\)\]/,
    "reservations are no longer keyed by peer — a peer can hold slots in several sessions at once");
  const release = fn("private func releaseSlot", "/// The session to answer");
  assert.match(release, /pendingAdmissions\.removeValue\(forKey: peerID\)/);
});

test("a transport reset clears every outstanding reservation", () => {
  const reset = fn("private func resetTransport", "// MARK: - Event emission");
  assert.match(reset, /pendingAdmissions = \[:\]/,
    "reservations survive a role change — the next session would start out looking occupied");
});

// ── THE FOLLOWER HANGING UP ON ITS OWN DIRECTOR ──────────────────────────────────────────────

test("a lostPeer mid-handshake does not make the follower reject its real director", () => {
  // The .connected guard asks "is this peer a director?" three ways, and its comment claims the
  // token/info clauses cover the case where "lostPeer can clear discoveredDirectors moments before
  // .connected lands for that same peer". They do not: lostPeer clears discoveredDirectorInfo in
  // the SAME breath, and reconsiderFollowerTarget then clears pendingInvitePeer because its target
  // just vanished. All three go false together, for the peer we deliberately invited — and the
  // handler answers a successful handshake with cancelConnectPeer, hanging up on the real director.
  //
  // A lapsed advertisement is routine; this file says so itself ("fires more often the more iPads
  // are in the room"), so the busiest room loses this race most often.
  // Bounded by the binding and its guard, and by nothing about the predicate's own text: round 3
  // showed that spelling the first clause into the marker made a behaviour-neutral edit — a pair of
  // parentheses around the whole predicate — report the binding as "gone".
  const guardBlock = fn("let isDirector", "guard isDirector");
  assert.match(guardBlock, /weInvitedAsDirector\(peerID\)/,
    "the guard still trusts only what the browser can currently see — a lostPeer race rejects the director");

  // The record must survive what lostPeer wipes: it is keyed off OUR decision, not discovery.
  const lost = fn("func browser(_ browser: MCNearbyServiceBrowser, lostPeer", "func browser(_ browser: MCNearbyServiceBrowser, didNotStart");
  assert.doesNotMatch(lost, /invitedDirector = nil/,
    "lostPeer clears the invite record too — it is back to being as forgetful as discovery");

  // Bounded, so it can never vouch for an unrelated later connection.
  const helper = fn("private func weInvitedAsDirector", "private func handleDirectorConflict");
  assert.match(helper, /Self\.inviteTimeout/, "the invite record is unbounded in time");

  // Cleared on a transport reset, like every other piece of per-relationship state.
  const reset = fn("private func resetTransport", "// MARK: - Event emission");
  assert.match(reset, /invitedDirector = nil/, "the invite record survives a role change");
});

// Strip Swift comments so nothing a human writes ABOUT the predicate is mistaken for the predicate.
// Block comments first, then line comments, so a `//` living inside a `/* … */` cannot orphan half
// of it.
const stripComments = (s) => s.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/\/\/[^\n]*/g, " ");

// Whitespace carries no meaning inside a Swift boolean expression, so remove all of it: the
// comparison is then blind to indentation, to where the author chose to break the lines, and to the
// spacing around ==. None of these clauses contain a string literal with a space in it, so nothing
// meaningful is lost.
const squash = (s) => s.replace(/\s+/g, "");

// Drop redundant wrapping parens, but only when the opening one really closes at the very end —
// `(a)||(b)` must not be mistaken for a parenthesised whole.
const unwrap = (s) => {
  let t = s;
  for (;;) {
    if (!t.startsWith("(") || !t.endsWith(")")) return t;
    let depth = 0;
    let closesAtEnd = true;
    for (let i = 0; i < t.length; i++) {
      if (t[i] === "(") depth += 1;
      else if (t[i] === ")") {
        depth -= 1;
        if (depth === 0 && i < t.length - 1) {
          closesAtEnd = false;
          break;
        }
      }
    }
    if (!closesAtEnd) return t;
    t = t.slice(1, -1);
  }
};

const lineOf = (idx) => MODULE.slice(0, idx).split("\n").length;

// WHAT THIS FILE IS AND IS NOT. It reads Swift as text. It is not a Swift compiler and cannot decide
// what an arbitrary expression evaluates to; the only thing that could is building the module and
// exercising the two admission paths on a device (or in an XCTest with a fake MCSession). So every
// helper below is written to REFUSE — assert.fail, naming the construct, the binding and the line in
// DirectorSyncModule.swift — the moment it meets a shape it cannot score soundly, rather than
// returning a confident answer it has not earned. A refusal is a failing test that says "a human has
// to look at line N". A wrong "no match" is the outage this file exists to prevent, dressed up as a
// passing suite.

// The only Swift line-continuation shapes these predicates use: a line that OPENS with a binary
// operator continues the one above it, and so does a line that ENDS with one.
// (`)` and `]` are here so a dangling closer on its own line is read as part of the expression
// rather than as the start of the next statement — otherwise the bracket-balance refusal below fires
// on a perfectly ordinary re-wrap.)
const CONTINUES_LINE = /^(\|\||&&|\?\?|[-+*/<>=!]=|[-+*/<>]|\.|\)|\])/;
const ENDS_OPEN = /(\|\||&&|\?\?|[-+*/<>=!]=|[-+*/<>]|=|,|\(|\[)$/;

const balanced = (s, open, close) => {
  let depth = 0;
  for (const ch of s) {
    if (ch === open) depth += 1;
    else if (ch === close) {
      depth -= 1;
      if (depth < 0) return false;
    }
  }
  return depth === 0;
};

// Read the CONDITION of a `guard`, and refuse unless it is exactly the bound identifier.
//
// Round 3 found the previous version located `guard <name>` by substring alone. That marker survives
// intact when a conjunct is appended TO THE GUARD ITSELF — `guard peerIsKnownDirector &&
// self.mcSessions.isEmpty else {` genuinely narrows when a peer is admitted, and the extracted
// predicate does not change one character, so the whole comparison below went on passing. The guard
// is now read as a statement: everything between `guard` and the brace it opens must be the
// identifier and nothing else, or this refuses.
const guardOn = (name) => {
  const marker = `guard ${name}`;
  const idx = MODULE.indexOf(marker);
  assert.ok(idx > 0, `\`${marker}\` is gone from DirectorSyncModule.swift`);
  const open = MODULE.indexOf("{", idx);
  assert.ok(open > idx, `\`${marker}\` (line ${lineOf(idx)}) opens no block — cannot read its condition`);
  const head = squash(stripComments(MODULE.slice(idx, open)));
  if (head !== `guard${name}else`) {
    assert.fail(
      `the guard at DirectorSyncModule.swift:${lineOf(idx)} is \`${MODULE.slice(idx, open).trim()}\`, ` +
      `not a bare \`guard ${name} else\`. Something other than ${name} now decides whether the peer ` +
      `is admitted, and this test scores ${name}'s binding only — it cannot say what the extra ` +
      `condition does. Score it by hand, or build the module and exercise the path.`);
  }
  return idx;
};

// Read the EXPRESSION a name is bound to: from the `=` of the LAST `let <name> =` before the guard,
// forward across continuation lines only. Anchoring on the guard matters — `peerIsKnownDirector` is
// bound twice in this file, and the other one (the director branch's two-clause split-brain check)
// is a different predicate that must NOT be dragged into this comparison.
//
// Round 3 found the previous version returned MODULE.slice(binding, guard) — every character between
// the two, not the expression. Any behaviour-neutral statement standing between them (a dbgLog line,
// a local, a blank block) became part of the "predicate" and the comparison failed, accusing the
// shipped code of inverting a check it had not touched. That is the same class of lie the round
// before it fixed, pointed the other way.
const boundExpression = (name, guardIdx) => {
  const declRe = new RegExp(`let\\s+${name}\\s*=`, "g");
  let decl = null;
  for (let m; (m = declRe.exec(MODULE)) !== null; ) {
    if (m.index >= guardIdx) break;
    decl = m;
  }
  assert.ok(decl, `${name} is no longer a plain let-binding before its guard`);
  const from = decl.index + decl[0].length;
  const declLine = lineOf(decl.index);
  const lines = MODULE.slice(from, guardIdx).split("\n");
  const taken = [lines[0]];
  for (let i = 1; i < lines.length; i++) {
    const here = squash(stripComments(lines[i]));
    if (here === "") { taken.push(lines[i]); continue; } // a comment or blank inside the OR chain
    const prev = squash(stripComments(taken[taken.length - 1]));
    if (!CONTINUES_LINE.test(here) && !ENDS_OPEN.test(prev)) break;
    taken.push(lines[i]);
  }
  const expr = squash(stripComments(taken.join("\n")));
  const where = `\`let ${name}\` at DirectorSyncModule.swift:${declLine}`;
  assert.ok(expr !== "", `${where} binds nothing this test can read`);
  if (/[{}]/.test(expr)) {
    assert.fail(`${where} contains a closure or a trailing closure. This test compares operands as ` +
      `text and cannot say what a closure returns — score it by hand, or build the module.`);
  }
  if (!balanced(expr, "(", ")") || !balanced(expr, "[", "]")) {
    assert.fail(`${where} did not close its brackets inside the lines this test could attribute to ` +
      `it: ${expr}. Either the expression uses a continuation shape not modelled here, or the ` +
      `statement really is malformed. Refusing to score a half-read expression.`);
  }
  for (const lit of expr.match(/"(?:[^"\\]|\\.)*"/g) || []) {
    if (/[|&]/.test(lit)) {
      assert.fail(`${where} contains the string literal ${lit}, which holds an operator character. ` +
        `This test splits the predicate on \`||\` as raw text and would split inside that literal. ` +
        `Refusing to score it.`);
    }
  }
  return expr;
};

test("both director predicates stay in agreement", () => {
  // The accept path and the .connected path have drifted apart before; the file's own comments say
  // they must not. Same four clauses, both places — and, load-bearing, joined by OR.
  //
  // The oldest version only grepped for the four clause substrings, which says nothing about how
  // they are combined: joining them with && leaves every substring intact, so the whole point of the
  // four-way predicate could be inverted with the test still green. And an inverted predicate is
  // exactly the outage above — a follower needing all four to be true hangs up on its real director
  // the moment a lostPeer clears discoveredDirectors and discoveredDirectorInfo together. So the
  // operator itself is asserted: the right-hand side is split on ||, and it must yield exactly these
  // four operands and contain no && at all.
  //
  // What THAT version missed is that it compared the operands as raw source text, so anything
  // behaviour-neutral broke it: a comment written inside the OR chain, a re-wrap of the lines, a
  // pair of clarifying parentheses. Worse, a comment that merely mentioned "&&" — the likeliest
  // comment to write next to a predicate whose whole point is that it is NOT a conjunction — made
  // the test fail with an actively false accusation that the shipped code had been inverted. A test
  // that lies about which way the bug went is worse than no test. Comments and whitespace are
  // therefore normalised away before anything is compared, and the operator check counts real ||
  // tokens in the stripped source rather than searching text a human may have written.
  //
  // Round 3 found three more holes in THAT, two of them the same lie in the other direction. The
  // guard was located by substring, so a conjunct appended to the guard itself was invisible; the
  // "predicate" was every character between the binding and the guard, so an unrelated statement
  // standing between them reddened the test; and unwrap ran per-operand AFTER the split, so parens
  // around the WHOLE predicate — behaviour-neutral in Swift — landed on the first and last operands
  // and reddened it too. The first two are fixed in guardOn/boundExpression above, which now refuse
  // out loud rather than guess. The third is fixed here: the RHS is unwrapped before it is split, and
  // again per operand.
  const CLAUSES = [
    "self.pendingInvitePeer == peerID",
    "self.weInvitedAsDirector(peerID)",
    "self.discoveredDirectors[peerID] != nil",
    'self.discoveredDirectorInfo[peerID]?["role"] == "director"',
  ].map(squash);

  const seen = [];
  for (const name of ["isDirector", "peerIsKnownDirector"]) {
    const rhs = unwrap(boundExpression(name, guardOn(name)));
    assert.ok(!rhs.includes("&&"),
      `${name} is a CONJUNCTION — a follower now needs every clause true at once, so a lostPeer ` +
      `mid-handshake makes it cancelConnectPeer on its own director. Predicate: ${rhs}`);
    assert.equal((rhs.match(/\|\|/g) || []).length, CLAUSES.length - 1,
      `${name} no longer joins exactly ${CLAUSES.length} clauses with ||: ${rhs}`);
    const operands = rhs.split("||").map(unwrap);
    for (const o of operands) {
      if (!balanced(o, "(", ")") || !balanced(o, "[", "]")) {
        assert.fail(`splitting ${name} on \`||\` produced the unbalanced fragment \`${o}\`, so one ` +
          `of its \`||\` is nested inside a call or a subscript. This test scores a flat OR chain ` +
          `and cannot work out what a nested one evaluates to — score it by hand. Predicate: ${rhs}`);
      }
    }
    assert.deepEqual([...operands].sort(), [...CLAUSES].sort(),
      `${name} does not OR the four director clauses — got: ${rhs}`);
    seen.push(operands.slice().sort().join("||"));
  }
  assert.equal(seen[0], seen[1],
    "the accept path and the .connected path disagree about what makes a peer a director");
});
