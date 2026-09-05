// The serving-director hold must protect a HANDSHAKE IN FLIGHT, not only a fresh sighting.
//
// 7d2f37b made the hold require a RECENT follower sighting (discoveredFollowerSeenAt). A skeptic showed
// the gap: foundPeer fires once per browser generation, so a follower connected for >20 s that drops
// and re-invites with the SAME peer id carries only its stale stamp — the next refresh tick read "no
// recent sighting", tore down the advertiser and browser, and the in-flight invite evaporated. One
// bounded retry cycle (~3–5 s), but a real one, and exactly what the hold exists to prevent.
//
// Two closures: (1) the hold also honours pendingAdmissions — the tokened, self-expiring (inviteTimeout
// + 2 s) reservation every accepted invite holds until it connects; (2) invite:accept stamps the
// sighting, because an invite IS a sighting. Plus the comparison itself is pinned literally: `<` → `>=`
// (hold ONLY on stale sightings) left the earlier structural test green.
// Each is re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const M = readFileSync(join(ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

test("the hold honours an in-flight handshake (pendingAdmissions), a connection, or a fresh sighting", () => {
  const at = M.indexOf('self.dbgLog("refresh:hold-serving"');
  assert.notEqual(at, -1, "the hold-serving log moved");
  const cond = M.slice(M.lastIndexOf('if self.currentRole == "director",', at), at);
  assert.match(cond, /!self\.allConnectedPeers\.isEmpty \|\| !self\.pendingAdmissions\.isEmpty \|\| self\.hasRecentFollowerSighting\(\)/,
    "the hold ignores pendingAdmissions — a re-inviting follower's handshake is torn down by the refresh tick");
});

test("accepting an invite stamps the follower sighting — an invite IS a sighting", () => {
  const at = M.indexOf('self.dbgLog("invite:accept"');
  assert.notEqual(at, -1, "the invite:accept log moved");
  const block = M.slice(at, M.indexOf("self.reserveSlot(peerID, in: session)", at));
  assert.match(block, /self\.discoveredFollowerSeenAt\[peerID\] = Date\(\)\.timeIntervalSince1970/,
    "invite:accept does not refresh the sighting stamp — a same-peer re-invite after 20 s is invisible to the hold");
});

test("the freshness comparison is `age < browserHealthySeconds`, not its inverse", () => {
  const at = M.indexOf("private func hasRecentFollowerSighting()");
  assert.notEqual(at, -1, "hasRecentFollowerSighting moved");
  const body = M.slice(at, M.indexOf("\n  }", at));
  assert.match(body, /return Date\(\)\.timeIntervalSince1970 - newest < Self\.browserHealthySeconds/,
    "the comparison is not `now - newest < browserHealthySeconds` — a hold on STALE sightings refreshes every 5 s while followers are handshaking");
});

test("configureTransport clears the three director records as a unit", () => {
  const at = M.indexOf("private func configureTransport()");
  assert.notEqual(at, -1, "configureTransport moved");
  const body = M.slice(at, M.indexOf("\n  }", at));
  for (const rec of ["discoveredDirectors = [:]", "discoveredDirectorInfo = [:]", "discoveredDirectorSeenAt = [:]"]) {
    assert.ok(body.includes(rec), `configureTransport no longer clears ${rec.split(" ")[0]} — the freshness check's records drift apart`);
  }
});
