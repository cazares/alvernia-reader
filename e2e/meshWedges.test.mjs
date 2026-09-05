// Two ways a follower could sit on the wrong page "with the director plainly visible" until a human
// tapped the resync control — the exact complaint the wedged-session escalation was added to remove.
//
// 1. THE ESCALATION COULD NEVER FIRE WHILE INVITES KEPT FAILING. The follower watchdog rebuilds the
//    MCSession when `now - followerHuntingSince > followerWedgedSeconds` (20 s). But the .notConnected
//    branch — entered for a drop from connected AND for every FAILED OUTBOUND INVITE (its condition is
//    `connectedDirectorPeer == peerID || pendingInvitePeer == peerID`) — re-stamped followerHuntingSince
//    unconditionally. With the invite retried every 2.5 s and timing out at 8 s, the clock was reset every
//    ≤8 s and never reached 20. reconsiderFollowerTarget already has the right rule for arming this clock:
//    `if followerHuntingSince == 0 { stamp }` — arm it, do not restart it. This branch now follows it: a
//    drop from connected still stamps (the connected path zeroes the clock), a failed invite while already
//    hunting does not.
//
// 2. THE WINNER OF A DIRECTOR CONFLICT REJECTED THE LOSER FOREVER. A and B both direct; token tiebreak
//    demotes A; A re-advertises as role=follower and invites B. B's director accept path refused it as
//    "peer-is-director" because discoveredDirectors[A] — recorded when A WAS a director — was never
//    cleared: B never refreshes while serving (hold-serving), and a role=follower re-sighting only ADDED
//    A to discoveredFollowers. A retried every 2.5 s, indefinitely. Now a peer re-sighted advertising
//    role=follower is removed from the director records (its advertised role changed, so the old record
//    is stale by definition), and the accept-path check also requires the director sighting to be
//    FRESH (browserHealthySeconds) — defence in depth against a record nobody cleared.
//
// Structural pins on the Swift; each defect is re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const M = readFileSync(join(ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

const braceBlock = (src, at, what) => {
  const open = src.indexOf("{", at);
  assert.ok(open !== -1, `no block for ${what}`);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}") { depth--; if (depth === 0) return src.slice(open, i + 1); }
  }
  assert.fail(`unbalanced braces in ${what}`);
};

test("a failed invite does not restart the wedged-session clock — only arm it if it is not running", () => {
  // The .notConnected follower branch: from the stale-director eviction to the watchdog restart.
  const evict = M.indexOf('self.dbgLog("director:evict-stale"');
  assert.notEqual(evict, -1, "the eviction log moved");
  const watchdog = M.indexOf("self.startFollowerWatchdog()", evict);
  assert.notEqual(watchdog, -1, "the watchdog restart moved");
  const branch = M.slice(evict, watchdog);
  assert.doesNotMatch(branch, /^\s*self\.followerHuntingSince = Date\(\)\.timeIntervalSince1970\s*$/m,
    "the hunting clock is re-stamped unconditionally on every failed invite — the 20 s wedge rebuild can never fire while invites keep failing");
  assert.match(branch, /if self\.followerHuntingSince == 0 \{\s*self\.followerHuntingSince = Date\(\)\.timeIntervalSince1970\s*\}/,
    "the clock must be ARMED (only if zero), the same rule reconsiderFollowerTarget already uses");
});

test("a peer re-sighted as a FOLLOWER is no longer remembered as a director", () => {
  const found = M.indexOf('} else if role == "follower", self.currentRole == "director" {');
  assert.notEqual(found, -1, "foundPeer's follower branch moved");
  const branch = braceBlock(M, found, "foundPeer follower branch");
  for (const rec of ["discoveredDirectors", "discoveredDirectorInfo", "discoveredDirectorSeenAt"]) {
    assert.match(branch, new RegExp(`self\\.${rec}\\.removeValue\\(forKey: peerID\\)`),
      `a demoted director re-advertising as follower keeps its ${rec} record — the winner rejects it as "peer-is-director" forever`);
  }
});

test("the director's invite check treats a director sighting as evidence only while it is FRESH", () => {
  const at = M.indexOf("let peerIsKnownDirector =");
  assert.notEqual(at, -1, "the peer-is-director check moved");
  const stmt = M.slice(at, M.indexOf("if peerIsKnownDirector", at));
  // The exact comparison, not the token: a skeptic showed that `<` → `>` (fresh record ACCEPTED =
  // split-brain re-opened, stale record REFUSED = the original wedge) left a bare /browserHealthySeconds/
  // pin green. Pin the predicate text and the missing-timestamp default (-.infinity → "never seen" =
  // infinitely old = never refused on the timestamp alone).
  assert.match(stmt, /&& directorSeenAgo < Self\.browserHealthySeconds/,
    "the freshness comparison is not `directorSeenAgo < browserHealthySeconds` — a stale record still refuses, or a fresh one is accepted");
  const ago = M.slice(M.lastIndexOf("let directorSeenAgo", at), at);
  assert.match(ago, /self\.discoveredDirectorSeenAt\[peerID\] \?\? -\.infinity/,
    "directorSeenAgo must read the DIRECTOR sighting time and treat a missing stamp as infinitely old");
});
