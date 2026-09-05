// A director may hold its transports steady on the strength of a CONNECTED peer, or of a RECENT
// sighting — never of a memory.
//
// The discovery refresh timer exists to rebuild a browser or advertiser that has gone deaf. The
// "hold-serving" guard skips that rebuild while the director is forming or serving connections, because
// tearing down a live advertiser drops every connected follower. Its condition was
//
//     !self.allConnectedPeers.isEmpty || !self.discoveredFollowers.isEmpty
//
// and discoveredFollowers is a Set with NO timestamps. MPC's lostPeer is not reliable (invites evaporate
// "with no callback" elsewhere in this file), so one follower found once at 11:50 and never reported lost
// kept the hold alive for the rest of Mass — a director whose browser or advertiser had since gone deaf
// never rebuilt either, and never noticed. The follower side already solved this exact problem for
// directors (discoveredDirectorSeenAt + browserHealthySeconds): hold on a sighting only while it is fresh.
// A follower that was sighted but has not connected within 20 s is a handshake that did not happen, and
// refreshing is the right call. Connected peers still hold unconditionally — a live session is proof.
//
// Structural pins (Swift cannot run here without the MPC shim); the defect is re-injected by
// scripts/verify-behavioural-guards.mjs and the NAMED test must go red.
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

test("follower sightings are timestamped, like director sightings already are", () => {
  assert.match(M, /private var discoveredFollowerSeenAt: \[MCPeerID: TimeInterval\] = \[:\]/,
    "there is no timestamp for follower sightings — a stale one is indistinguishable from a fresh one");
  // Stamped where the sighting is recorded (foundPeer's follower branch)…
  const found = M.indexOf('} else if role == "follower", self.currentRole == "director" {');
  assert.notEqual(found, -1, "foundPeer's follower branch moved");
  const branch = braceBlock(M, found, "foundPeer follower branch");
  assert.match(branch, /self\.discoveredFollowerSeenAt\[peerID\] = Date\(\)\.timeIntervalSince1970/,
    "a follower sighting is recorded without a timestamp");
  // …and removed wherever the sighting is forgotten.
  assert.match(M, /self\.discoveredFollowers\.remove\(peerID\)\s*\n\s*self\.discoveredFollowerSeenAt\.removeValue\(forKey: peerID\)/,
    "lostPeer forgets the follower but keeps its timestamp");
  const clears = (M.match(/discoveredFollowers(?: = \[\]|\.removeAll\(\))/g) || []).length;
  const stampClears = (M.match(/discoveredFollowerSeenAt(?: = \[:\]|\.removeAll\(\))/g) || []).length;
  assert.equal(stampClears, clears, `discoveredFollowers is cleared in ${clears} place(s) but its timestamps in ${stampClears} — a stale stamp survives a reset`);
});

test("the serving-director hold requires a CONNECTED peer or a RECENT sighting, never a bare memory", () => {
  const at = M.indexOf('self.dbgLog("refresh:hold-serving"');
  assert.notEqual(at, -1, "the hold-serving branch is gone");
  // The condition is the `if` immediately above the log — bounded by the previous statement's newline.
  const head = M.slice(M.lastIndexOf("if self.currentRole == \"director\"", at), at);
  assert.doesNotMatch(head, /!self\.discoveredFollowers\.isEmpty/,
    "the hold still fires on the bare presence of a follower sighting — one stale sighting pins a deaf advertiser for the rest of Mass");
  // Three kinds of evidence hold the transport: a CONNECTED peer, a HANDSHAKE IN FLIGHT (pendingAdmissions,
  // tokened and self-expiring — added after a skeptic showed a same-peer re-invite losing its handshake to
  // this tick), or a RECENT sighting. Never a bare memory.
  assert.match(head, /!self\.allConnectedPeers\.isEmpty \|\| !self\.pendingAdmissions\.isEmpty \|\| self\.hasRecentFollowerSighting\(\)/,
    "the hold must be: connected peer OR recent sighting");
  const helperAt = M.indexOf("private func hasRecentFollowerSighting()");
  assert.notEqual(helperAt, -1, "hasRecentFollowerSighting is missing");
  const helper = braceBlock(M, helperAt, "hasRecentFollowerSighting");
  assert.match(helper, /discoveredFollowerSeenAt\.values\.max\(\)/, "the helper must read the newest sighting");
  assert.match(helper, /Self\.browserHealthySeconds/, "the helper must use the same freshness window the follower side uses");
});
