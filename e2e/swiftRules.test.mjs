// The Swift mesh rules, COMPILED AND EXECUTED — not grepped.
//
// WHY THIS FILE EXISTS. On 2026-08-29 every claimed-decorative Swift assertion in this repo was
// checked by measurement: apply a real regression to ios/SignoVivo/*.swift, re-run the named test,
// see whether it goes red. These went straight through, green:
//
//   • the four-way "is this peer my director?" predicate flipped from `||` to `&&` — the round-5 fix
//     of the previous campaign, the thing that stops a follower hanging up on its own director
//   • a late joiner's catch-up snapshot downgraded from .reliable to .unreliable
//   • parseInboundPayload rewritten to force-unwrap and crash on malformed JSON
//   • the MCPeerID display-name cap removed at the site that actually feeds MCPeerID
//   • the BLE unchanged-page early return deleted, so advertSeq climbs ~5,400 times a Mass
//
// Every one of those was "caught" by a regex that still matched a string somewhere else in a
// 2,802-line file. The lesson is not that somebody wrote a sloppy pattern; it is that a regex
// cannot see behaviour, and the next four regexes will fail the same way.
//
// So this box compiles Swift, and these tests run it. e2e/helpers/swift-harness.mjs slices the real
// function bodies out of the shipped source, compiles them against a minimal MultipeerConnectivity
// stand-in, and executes them. Nothing here is transliterated — the bytes between
// `private func sessionForAdmitting` and its closing brace are the bytes that ship. Change the
// logic and these tests change with it. Change its SHAPE so the extractor cannot find it and they
// FAIL LOUDLY, which is the property the source-text tests never had.
//
// WHAT IS SHIMMED: identity, membership and construction (MCSession is a class, so `===` means what
// it means in production), plus recorders for the actions a decision triggers. The shim decides
// NOTHING. The moment it starts encoding a rule, the rule has moved out of the code under test and
// into the harness — which is exactly the failure this file exists to end.

import test from "node:test";
import assert from "node:assert/strict";

import {
  readSwift, extractDecl, extractExpression, extractConst, runSwift, swiftAvailable, MPC_SHIM,
} from "./helpers/swift-harness.mjs";

const SW = readSwift("ios/SignoVivo/DirectorSyncModule.swift");
const FILE = "ios/SignoVivo/DirectorSyncModule.swift";

// A missing toolchain is reported, never silently skipped — a suite that quietly stops running is
// indistinguishable from one that passes, which is the whole subject of this file.
const HAVE_SWIFT = swiftAvailable();
const needSwift = () =>
  assert.ok(HAVE_SWIFT, "no Swift toolchain (xcrun swiftc) on this machine — these tests did NOT run");

/** Parse the harness's `key: value` output lines into an object. */
const parse = (out) =>
  Object.fromEntries(out.trim().split("\n").filter(Boolean).map((l) => {
    const i = l.indexOf(":");
    return [l.slice(0, i).trim(), l.slice(i + 1).trim()];
  }));

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 1. SESSION ADMISSION — the simultaneous-joiner race.
// ════════════════════════════════════════════════════════════════════════════════════════════════

const ADMISSION_FNS = [
  "private func admissionCount(",
  "private func releaseSlot(",
  "private func sessionForAdmitting(",
  "private func availableSessionForNewFollower(",
];

function admissionProgram(body) {
  return `${MPC_SHIM}
final class Admission {
  static let maxFollowersPerSession = ${extractConst(SW, "maxFollowersPerSession", { file: FILE })}
  static let maxSessions = ${extractConst(SW, "maxSessions", { file: FILE })}
  var localPeerID: MCPeerID?
  var mcSessions: [MCSession] = []
  var pendingAdmissions: [MCPeerID: (session: MCSession, token: Int)] = [:]
  var admissionToken = 0

${ADMISSION_FNS.map((a) => extractDecl(SW, a, { file: FILE })).join("\n\n")}

  // reserveSlot's other half is a DispatchQueue.asyncAfter expiry, which has no meaning without a
  // run loop. The reservation itself — the part every routing decision below reads — is this.
  func reserve(_ p: MCPeerID, in s: MCSession) {
    admissionToken &+= 1
    pendingAdmissions[p] = (session: s, token: admissionToken)
  }

  func drive() {
${body}
  }
}
Admission().drive()
`;
}

test("six followers inviting at the same instant each get their OWN session", () => {
  // THE BUG THIS IS ABOUT. `connectedPeers` omits every peer still shaking hands, so before the
  // reservation existed, simultaneous joiners all read the same session as empty and were all
  // routed into it — restoring the full-mesh topology that #370 removed. With
  // maxFollowersPerSession at 1, the correct answer is one session each and no sharing.
  //
  // The existing guard for this is `assert.match(body, /connectedPeers\.count \+ admissionCount/)`,
  // which is satisfied by the expression still being written down. This runs it.
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    var assigned: [Int] = []
    for i in 0..<6 {
      let p = MCPeerID(displayName: "f\\(i)")
      guard let s = sessionForAdmitting(p) else { assigned.append(-1); continue }
      reserve(p, in: s)
      assigned.append(s.label)
    }
    print("assigned:", assigned.map(String.init).joined(separator: ","))
    print("distinct:", Set(assigned).count)
    print("sessions:", mcSessions.count)
  `), { label: "admission-burst" }));

  assert.equal(out.distinct, "6",
    `six simultaneous joiners were routed into ${out.distinct} session(s) instead of 6 — ` +
    "the handshake-aware occupancy check is not holding, and the full mesh is back");
  assert.ok(!out.assigned.includes("-1"), "a follower was refused a session outright");
  assert.equal(out.sessions, "6");
});

test("a follower retrying its invitation goes back into the SAME session every time", () => {
  // A follower re-invites every inviteRetryAfter until it connects. With reservations held per
  // SESSION rather than per PEER, that peer's own reservation made the session it had already been
  // offered look full — so every retry was answered with a NEWLY CREATED session. One follower held
  // four or five at once, and two retrying followers exhausted maxSessions until the director
  // answered every invitation with "sessions-full".
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    let r = MCPeerID(displayName: "retrier")
    var got: [Int] = []
    for _ in 0..<5 {
      guard let s = sessionForAdmitting(r) else { got.append(-1); continue }
      reserve(r, in: s)
      got.append(s.label)
    }
    print("distinct:", Set(got).count)
    print("sessions:", mcSessions.count)
  `), { label: "admission-retry" }));

  assert.equal(out.distinct, "1", "a retrying follower was handed a different session each time");
  assert.equal(out.sessions, "1",
    `five retries from ONE follower created ${out.sessions} sessions — a handful of retrying ` +
    "followers would exhaust maxSessions and the director would refuse everyone");
});

test("a half-open follower re-inviting is returned to the session it is already connected to", () => {
  // A follower whose link went half-open on its side re-invites while the director still counts it
  // as connected. Without the already-CONNECTED branch it was handed a different session and became
  // a member of two at once, burning a session and putting two peers where the topology allows one.
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    let p = MCPeerID(displayName: "halfopen")
    guard let s1 = sessionForAdmitting(p) else { print("same: NO-SESSION"); return }
    s1.connectedPeers = [p]
    guard let s2 = sessionForAdmitting(p) else { print("same: NO-SESSION"); return }
    print("same:", s1 === s2)
    print("sessions:", mcSessions.count)
  `), { label: "admission-halfopen" }));

  assert.equal(out.same, "true", "a half-open follower was put into a SECOND session while still connected to the first");
  assert.equal(out.sessions, "1");
});

test("a stale terminal callback cannot free a reservation made after it", () => {
  // The ABA. MPC delivers terminal callbacks for sessions that have been torn down, and
  // resetTransport rebuilds sessions routinely. An unconditional release-by-peer freed whatever
  // reservation the peer held AT THAT MOMENT — including a newer one whose handshake was still in
  // flight, which re-opens the double-admission the reservation exists to close.
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    let p = MCPeerID(displayName: "aba")
    guard let live = sessionForAdmitting(p) else { print("kept: NO-SESSION"); return }
    reserve(p, in: live)
    let tornDown = MCSession(peer: localPeerID!, securityIdentity: nil, encryptionPreference: .none)
    releaseSlot(p, in: tornDown)
    print("kept:", pendingAdmissions[p] != nil)
    releaseSlot(p, in: live)
    print("freed:", pendingAdmissions[p] == nil)
  `), { label: "admission-aba" }));

  assert.equal(out.kept, "true",
    "a terminal callback for a torn-down session freed the reservation held in the LIVE one");
  assert.equal(out.freed, "true", "the matching release did not free the reservation");
});

test("occupancy counts reserved slots, not just connected peers", () => {
  // The occupancy expression is `connectedPeers.count + admissionCount(session)`, and
  // admissionCount is the half that does the work: connectedPeers omits every peer still shaking
  // hands, which is exactly the window in which simultaneous joiners all read a session as empty.
  //
  // Asserted directly because the burst test above cannot distinguish a broken admissionCount from
  // a working one — with maxFollowersPerSession at 1, ANY non-zero count fills a session, so a
  // count that started at 1 instead of 0 produces the same six-session answer for the wrong reason.
  // A mutation sweep of the Swift found exactly that: `reduce(0)` → `reduce(1)` survived.
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    let s = MCSession(peer: localPeerID!, securityIdentity: nil, encryptionPreference: .none)
    let other = MCSession(peer: localPeerID!, securityIdentity: nil, encryptionPreference: .none)
    mcSessions = [s, other]
    print("emptyIsZero:", admissionCount(s))
    for i in 0..<3 { reserve(MCPeerID(displayName: "p\\(i)"), in: s) }
    print("threeReserved:", admissionCount(s))
    print("otherSessionUnaffected:", admissionCount(other))
  `), { label: "admission-count" }));

  assert.equal(out.emptyIsZero, "0", "a session with no reservations reports non-zero occupancy");
  assert.equal(out.threeReserved, "3", "three reservations are not counted as three");
  assert.equal(out.otherSessionUnaffected, "0",
    "reservations in one session are counted against another — sessions would fill each other up");
});

test("past maxSessions the director refuses rather than opening another one", () => {
  // The ceiling is what stops a burst of retrying followers from allocating sessions without bound.
  // Once it is reached, sessionForAdmitting must return nil so the invitation is answered with a
  // refusal — a director that kept creating sessions would eventually exhaust MPC's own limits and
  // fail in a much less legible way.
  needSwift();
  const out = parse(runSwift(admissionProgram(`
    localPeerID = MCPeerID(displayName: "director")
    let cap = Admission.maxSessions
    var created = 0
    for i in 0..<(cap + 5) {
      let p = MCPeerID(displayName: "f\\(i)")
      guard let s = sessionForAdmitting(p) else { continue }
      reserve(p, in: s)
      created += 1
    }
    print("cap:", cap)
    print("admitted:", created)
    print("sessions:", mcSessions.count)
  `), { label: "admission-cap" }));

  assert.equal(out.sessions, out.cap,
    `the director opened ${out.sessions} sessions against a ceiling of ${out.cap}`);
  assert.equal(out.admitted, out.cap,
    "more followers were admitted than there are sessions — the ceiling is not being enforced");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 2. THE FOUR-WAY DIRECTOR PREDICATE — the round-5 fix, executed.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("a follower still recognises its director when the browser has forgotten it", () => {
  // THE ROUND-5 BUG, AND WHY FOUR CLAUSES. The first three can all be false at once for the RIGHT
  // peer: lostPeer clears discoveredDirectors AND discoveredDirectorInfo together, and
  // reconsiderFollowerTarget then clears pendingInvitePeer because its target vanished. When that
  // interleaves with a .connected callback, a follower drops the very director it just invited —
  // and the code comment that used to sit here claimed the three clauses could not go false
  // together. The fourth clause records what WE decided rather than what the browser can still see.
  //
  // The existing guard greps for the four clause substrings inside the block. Changing `||` to `&&`
  // — which makes the predicate require all four, so this scenario returns false and the follower
  // hangs up on its director — leaves every one of those substrings intact and every assertion
  // green. That was verified by mutation. This compiles the expression instead.
  needSwift();
  const predicate = extractExpression(SW, "let isDirector = self.pendingInvitePeer == peerID", { file: FILE, minContinuations: 3 });
  const weInvited = extractDecl(SW, "private func weInvitedAsDirector(", { file: FILE });

  const out = parse(runSwift(`${MPC_SHIM}
final class Mesh {
  static let inviteTimeout: TimeInterval = ${extractConst(SW, "inviteTimeout", { file: FILE })}
  var pendingInvitePeer: MCPeerID?
  var discoveredDirectors: [MCPeerID: String] = [:]
  var discoveredDirectorInfo: [MCPeerID: [String: String]] = [:]
  var invitedDirector: (peer: MCPeerID, at: TimeInterval)?

${weInvited}

  func evaluate(_ peerID: MCPeerID) -> Bool {
${predicate}
    return isDirector
  }

  func drive() {
    // The exact interleaving: we invited this peer as the director, then lostPeer wiped both
    // discovery maps and reconsiderFollowerTarget cleared pendingInvitePeer. Only our own memory
    // of the invitation is left.
    let director = MCPeerID(displayName: "director-ipad")
    invitedDirector = (peer: director, at: Date().timeIntervalSince1970)
    pendingInvitePeer = nil
    discoveredDirectors = [:]
    discoveredDirectorInfo = [:]
    print("lostPeerMidHandshake:", evaluate(director))

    // A peer we never invited and never discovered is NOT our director — the predicate must not
    // simply return true for everyone, or it would accept another follower's connection.
    print("strangerRejected:", !evaluate(MCPeerID(displayName: "some-follower")))

    // Each clause alone is sufficient.
    let byPending = MCPeerID(displayName: "p1"); pendingInvitePeer = byPending
    print("byPendingInvite:", evaluate(byPending))
    pendingInvitePeer = nil
    let byDiscovery = MCPeerID(displayName: "p2"); discoveredDirectors[byDiscovery] = "tok"
    print("byDiscovery:", evaluate(byDiscovery))
    let byRole = MCPeerID(displayName: "p3"); discoveredDirectorInfo[byRole] = ["role": "director"]
    print("byRole:", evaluate(byRole))

    // The invite memory is TIME-BOUNDED, and the bound is tested AT ITS EDGE. A window that has
    // quietly grown to cover the whole Mass would make the fourth clause "this peer is my director
    // because I once invited it", which is how an ex-director keeps being accepted after a handover.
    // The grace above inviteTimeout exists for a slow .connected, so both sides of it matter.
    let stale = MCPeerID(displayName: "stale")
    let window = Mesh.inviteTimeout + 5
    invitedDirector = (peer: stale, at: Date().timeIntervalSince1970 - (window - 2))
    print("insideWindowStillCounts:", evaluate(stale))
    invitedDirector = (peer: stale, at: Date().timeIntervalSince1970 - (window + 2))
    print("justOutsideWindowExpires:", !evaluate(stale))
    invitedDirector = (peer: stale, at: Date().timeIntervalSince1970 - (window + 600))
    print("staleInviteExpires:", !evaluate(stale))

    // The memory is about ONE peer. A different peer must not inherit it.
    invitedDirector = (peer: stale, at: Date().timeIntervalSince1970)
    print("inviteIsPeerSpecific:", !evaluate(MCPeerID(displayName: "someone-else")))
  }
}
Mesh().drive()
`, { label: "director-predicate" }));

  assert.equal(out.lostPeerMidHandshake, "true",
    "a follower whose browser forgot the director mid-handshake now rejects it — this is the " +
    "`||` → `&&` regression, and it makes a follower hang up on its own director");
  assert.equal(out.strangerRejected, "true", "the predicate accepts a peer that is not a director at all");
  assert.equal(out.byPendingInvite, "true", "the pendingInvitePeer clause does not work on its own");
  assert.equal(out.byDiscovery, "true", "the discoveredDirectors clause does not work on its own");
  assert.equal(out.byRole, "true", "the advertised-role clause does not work on its own");
  assert.equal(out.insideWindowStillCounts, "true",
    "an invite two seconds inside its own window has already expired — a slow .connected callback " +
    "would be rejected, which is the case the grace period exists for");
  assert.equal(out.justOutsideWindowExpires, "true",
    "the invite window has grown past its bound — an ex-director stays acceptable long after handover");
  assert.equal(out.staleInviteExpires, "true",
    "the invite memory never expires — a peer invited an hour ago is still treated as the director");
  assert.equal(out.inviteIsPeerSpecific, "true",
    "the invite memory applies to the wrong peer — any device connecting during the window is taken " +
    "for the director");
});

test("the accept-path predicate and the .connected predicate are the SAME four clauses", () => {
  // Two copies of one rule, in two files' worth of delegate code, with a comment on each saying
  // "these must not drift apart". Nothing enforced it. Compare the extracted text with whitespace
  // normalised, so a reformat is fine and a changed clause is not.
  const a = extractExpression(SW, "let peerIsKnownDirector = self.pendingInvitePeer == peerID", { file: FILE, minContinuations: 3 });
  const b = extractExpression(SW, "let isDirector = self.pendingInvitePeer == peerID", { file: FILE, minContinuations: 3 });
  const norm = (s) => s.replace(/^\s*let\s+\w+\s*=\s*/, "").replace(/\s+/g, " ").trim();
  assert.equal(norm(a), norm(b),
    "the accept-path and .connected director predicates have drifted apart — one of them will " +
    "admit or drop a peer the other would not");
  assert.equal((norm(a).match(/\|\|/g) || []).length, 3,
    "the predicate is no longer a four-clause disjunction");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 3. THE DIRECTOR TIEBREAK, AND THE TOKEN ORDERING IT DEPENDS ON.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("when two directors meet, the NEWER one wins and the older demotes — exactly once", () => {
  // Split-brain resolution. The rule is a lexicographic compare of two tokens, and it is only
  // correct because randomToken's zero-padded microsecond prefix makes lexicographic order equal
  // chronological order. Two properties, in two functions, and neither is executed by anything
  // today: singleDirector.test.mjs greps for `if otherToken > currentDirectorToken`.
  //
  // The guards matter as much as the comparison. An empty token on either side must NOT demote
  // anyone: a device that has not minted a token yet would otherwise lose to every string, and a
  // follower would demote itself over a conflict it is not part of.
  needSwift();
  const conflict = extractDecl(SW, "private func handleDirectorConflict(", { file: FILE });
  const randomToken = extractDecl(SW, "private static func randomToken(", { file: FILE });

  const out = parse(runSwift(`${MPC_SHIM}
final class Conflict {
  var currentRole = "off"
  var currentDirectorToken = ""
  var demotions = 0
  var errors: [String] = []

${conflict}

${randomToken}

  // Recorders, not decisions: the rule under test is which of these gets called, not what they do.
  func emitError(code: String, message: String) { errors.append(code) }
  func resetTransport(emitState: Bool) { demotions += 1 }

  func drive() {
    // THE ORDERING PROPERTY THE WHOLE TIEBREAK RESTS ON: minted later => sorts later.
    //
    // Spaced by 1.5 ms so every token genuinely lands in a different microsecond. Minted back to
    // back they can share one, and then the random UUID suffix decides the comparison — which is
    // correct behaviour (two devices cannot mint the same token) but makes an ordering assertion a
    // coin flip. A flaky test in this suite would be worse than no test: it teaches people that red
    // means "run it again".
    var tokens: [String] = []
    for _ in 0..<12 { tokens.append(Conflict.randomToken()); usleep(1500) }
    print("tokensSortedByAge:", tokens == tokens.sorted())

    // Distinctness needs NO spacing — that is exactly the case the UUID suffix exists for. Minted
    // in a tight loop these will collide on the microsecond, and must still never compare equal:
    // two equal tokens deadlock the tiebreak, neither side demotes, and the choir sees two directors.
    var burst: [String] = []
    for _ in 0..<200 { burst.append(Conflict.randomToken()) }
    print("tokensAllDistinct:", Set(burst).count == burst.count)
    print("tokensFixedWidthPrefix:", Set(burst.map { $0.split(separator: "-")[0].count }).count == 1)

    // What the zero-padding actually buys, tested directly rather than inferred. Without it a
    // timestamp that crosses a digit-count boundary sorts BACKWARDS as a string — "9999999" is
    // lexicographically greater than "10000000" — so an older director would out-rank a newer one
    // for as long as the two straddled that boundary.
    let lower = String(format: "%020lld", Int64(999_999))
    let higher = String(format: "%020lld", Int64(1_000_000))
    print("paddingSurvivesDigitBoundary:", lower < higher)
    print("unpaddedWouldHaveInverted:", !("999999" < "1000000"))

    // A director meeting a NEWER director demotes.
    currentRole = "director"; currentDirectorToken = tokens[2]; demotions = 0; errors = []
    handleDirectorConflict(with: tokens[9])
    print("olderDemotes:", demotions == 1 && errors == ["DIRECTOR_CONFLICT"])

    // A director meeting an OLDER one holds its seat.
    currentRole = "director"; currentDirectorToken = tokens[9]; demotions = 0; errors = []
    handleDirectorConflict(with: tokens[2])
    print("newerHolds:", demotions == 0)

    // Equal tokens must not demote EITHER side — but randomToken's UUID suffix means two devices
    // can never actually mint the same one, which is the property that stops a mutual demotion.
    currentRole = "director"; currentDirectorToken = tokens[2]; demotions = 0
    handleDirectorConflict(with: tokens[2])
    print("equalHolds:", demotions == 0)

    // A FOLLOWER never demotes on somebody else's conflict.
    currentRole = "follower"; currentDirectorToken = tokens[2]; demotions = 0
    handleDirectorConflict(with: tokens[9])
    print("followerUnaffected:", demotions == 0)

    // An empty token on either side is not a valid comparison.
    currentRole = "director"; currentDirectorToken = ""; demotions = 0
    handleDirectorConflict(with: tokens[9])
    print("emptyOwnTokenHolds:", demotions == 0)
    currentRole = "director"; currentDirectorToken = tokens[2]; demotions = 0
    handleDirectorConflict(with: "")
    print("emptyOtherTokenHolds:", demotions == 0)
  }
}
Conflict().drive()
`, { label: "director-conflict" }));

  assert.equal(out.tokensSortedByAge, "true",
    "tokens minted later do not sort later — the newest director no longer reliably wins, and " +
    "which device holds the choir is decided by string luck");
  assert.equal(out.tokensAllDistinct, "true", "two tokens compared equal — neither side would demote, and the split-brain deadlocks");
  assert.equal(out.tokensFixedWidthPrefix, "true",
    "the timestamp prefix is not fixed width, so lexicographic order stops matching chronological order");
  assert.equal(out.paddingSurvivesDigitBoundary, "true",
    "the zero-padded format no longer keeps string order equal to numeric order across a digit-count boundary");
  assert.equal(out.unpaddedWouldHaveInverted, "true",
    "this control assertion is meant to demonstrate that WITHOUT padding the comparison inverts; " +
    "if it fails, the demonstration is wrong and the test above proves less than it claims");
  assert.equal(out.olderDemotes, "true", "the older director did not step down for the newer one");
  assert.equal(out.newerHolds, "true", "the NEWER director demoted — the tiebreak is inverted");
  assert.equal(out.equalHolds, "true", "equal tokens demoted a director");
  assert.equal(out.followerUnaffected, "true", "a follower demoted itself over two other devices' conflict");
  assert.equal(out.emptyOwnTokenHolds, "true", "a director with no token yet demoted to any string");
  assert.equal(out.emptyOtherTokenHolds, "true", "an empty peer token demoted a live director");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 4. THE SESSION CODE — one string, two radios.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("the session code normalises identically for the mesh and for BLE", () => {
  // normalizeSessionCode's output is simultaneously the mesh discoveryInfo session key and the
  // input to the BLE auth tag. If two devices normalise the same code differently they simply never
  // see each other, on either radio, with no error anywhere. It is asserted by nothing today.
  needSwift();
  const out = runSwift(`${MPC_SHIM}
enum Code {
  static let maxSessionCodeLength = ${extractConst(SW, "maxSessionCodeLength", { file: FILE })}

${extractDecl(SW, "private static func normalizeSessionCode(", { file: FILE })}

  static func drive() {
    for raw in ["alvernia", "ALVERNIA", "  alvernia  ", "al-ver-nia", "al ver nia",
                "alvernia-main-2026", "álvernia", "a", "", "12345678901234567890"] {
      print("[\\(raw)] -> [\\(normalizeSessionCode(raw))]")
    }
  }
}
Code.drive()
`, { label: "session-code" }).trim().split("\n");

  const map = Object.fromEntries(out.map((l) => {
    const m = l.match(/^\[(.*)\] -> \[(.*)\]$/);
    return [m[1], m[2]];
  }));

  // Case, spacing and punctuation must all collapse to the same key: a person typing the code on
  // one iPad and another typing it with different capitalisation have to land in the same room.
  for (const variant of ["alvernia", "ALVERNIA", "  alvernia  ", "al-ver-nia", "al ver nia"]) {
    assert.equal(map[variant], "ALVERNIA",
      `"${variant}" normalises to "${map[variant]}" instead of "ALVERNIA" — two iPads typing the ` +
      "same code would join different rooms and never see each other");
  }
  const cap = Number(extractConst(SW, "maxSessionCodeLength", { file: FILE }));
  assert.equal(map["alvernia-main-2026"].length, cap, `the ${cap}-character cap is not applied`);
  assert.equal(map["12345678901234567890"], "123456789012", "digits are dropped or the cap moved");
  assert.equal(map[""], "", "an empty code did not stay empty");
  // Punctuation and whitespace are what must not survive: they are the characters a person actually
  // types by accident, and the ones that would change the byte length of the BLE local name.
  for (const [raw, got] of Object.entries(map)) {
    assert.ok(!/[\s\-_.,:;/\\]/.test(got), `"${raw}" left punctuation or whitespace in "${got}"`);
  }

  // A DELIBERATE PIN ON CURRENT BEHAVIOUR, NOT AN ENDORSEMENT. The filter is `isLetter || isNumber`,
  // which is true for the whole Unicode letter category — so an accented character survives and
  // "álvernia" normalises to "ÁLVERNIA", not "LVERNIA". Both iPads run this same function, so they
  // agree and nothing breaks today. It is worth knowing because the normalised code goes into the
  // BLE local name, where the budget is bytes and not characters: "Á" costs two. If somebody later
  // tightens this to ASCII, this assertion is the place that says the change was deliberate.
  assert.equal(map["álvernia"], "ÁLVERNIA",
    "non-ASCII letters no longer survive normalisation — that may well be an improvement, but it " +
    "changes the session key every existing device computes, so both radios must be rolled together");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// 5. THE TRANSPORT BACKOFF LADDER — run, not replayed.
// ════════════════════════════════════════════════════════════════════════════════════════════════

test("the advertiser retry ladder climbs and then retries forever at the last-resort tier", () => {
  // M-F7: fast exponential backoff for the first five failures, then a slow 45 s retry FOREVER. The
  // "forever" half is the point — a foregrounded director whose radio hiccups past the ceiling would
  // otherwise stay dark until a foreground transition that may never come during a long Mass.
  //
  // e2e/transportBackoff.test.mjs currently regexes the five constants out of the Swift and then
  // recomputes the ladder in JavaScript. That is better than hardcoding it, and it is still a model:
  // the arithmetic being verified is the test's own. This compiles and runs the actual expression.
  needSwift();
  const ladder = extractExpression(SW, "let delay = self.advertiserFailureCount <= 5", { file: FILE, minContinuations: 2 });
  const out = parse(runSwift(`import Foundation
final class Backoff {
  var advertiserFailureCount = 0
  func delayFor(_ n: Int) -> Double {
    advertiserFailureCount = n
${ladder}
    return delay
  }
  func drive() {
    let d = (1...12).map { delayFor($0) }
    print("ladder:", d.prefix(8).map { String($0) }.joined(separator: ","))
    print("monotonicToCap:", zip(d.prefix(5), d.prefix(5).dropFirst()).allSatisfy { $0 <= $1 })
    print("neverGivesUp:", d.suffix(6).allSatisfy { $0 > 0 && $0.isFinite })
    print("settlesConstant:", Set(d.suffix(6)).count == 1)
    print("firstIsShort:", d[0] <= 5.0)
  }
}
Backoff().drive()
`, { label: "backoff" }));

  const ladder8 = out.ladder.split(",").map(Number);
  assert.deepEqual(ladder8.slice(0, 6), [3, 6, 12, 24, 30, 45],
    `the retry ladder is ${out.ladder} — a director whose radio fails now recovers on a different ` +
    "schedule than the one the mesh timings were tuned against");
  assert.equal(out.monotonicToCap, "true", "the ladder does not increase — a failing radio is retried at a constant rate");
  assert.equal(out.neverGivesUp, "true",
    "the ladder stops producing a retry — a director whose advertiser failed stays dark for the rest of Mass");
  assert.equal(out.settlesConstant, "true", "the last-resort tier is not constant");
  assert.equal(out.firstIsShort, "true", "the first retry is slow — a transient radio hiccup now costs seconds of blackout");
});
