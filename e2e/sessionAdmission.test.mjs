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

test("the reservation is released on every terminal handshake state", () => {
  const body = fn("func session(_ session: MCSession, peer peerID: MCPeerID, didChange", "case .connected:");
  assert.match(body, /if state != \.connecting \{ self\.releaseSlot\(peerID\) \}/,
    "a completed or failed handshake no longer frees its slot — sessions will look permanently full");
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
