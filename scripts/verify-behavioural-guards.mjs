#!/usr/bin/env node
/**
 * Replays the 19 regressions that were PROVEN to slip past this repo's tests, and requires each one
 * to redden the test that names it.
 *
 * WHAT HAPPENED. On 2026-08-29 every source-text assertion in e2e/ that looked decorative was
 * checked by measurement rather than by argument: apply a real regression to the real source, re-run
 * the named test, record whether it goes red. Nineteen stayed GREEN. Among them:
 *
 *   • the publish rate limit — the ONLY abuse control on an open, unauthenticated endpoint — deleted
 *   • the four-way director predicate flipped from `||` to `&&`, which makes a follower hang up on
 *     its own director (this was the round-5 fix of the previous campaign)
 *   • parseInboundPayload rewritten to force-unwrap, so any peer in range crashes the app with one
 *     malformed packet
 *   • a late joiner's catch-up snapshot downgraded from .reliable to .unreliable
 *   • the songbook index truncated from 317 entries to 10, and separately every song remapped to
 *     page 1
 *   • clampPage's upper bound removed, so a song jump past the end sticks on a 404
 *   • assets/songbook.pdf truncated to zero bytes, with app.json pointing at a renamed icon
 *   • the two director fabs overlapping by 2rem on the iPad
 *
 * Each was invisible for the same reason: an assertion looked for a STRING, and the string still
 * existed somewhere else in the file. This script is the standing proof that the repairs work, and
 * the thing that will notice if a future edit quietly undoes one.
 *
 * SAME CONTRACT as its siblings (verify-smoke-guards, verify-sw-page-cache-guards,
 * verify-director-rescue-guards): mutate a COPY, rerun the matching test, require RED. A SKIP is a
 * FAILURE — it means the mutation's pattern no longer matches the source, so that mutation has
 * silently stopped testing anything, and a silent stop is indistinguishable from coverage.
 *
 * Scoped with --test-name-pattern so each mutation must redden THE NAMED TEST, not merely something
 * in the same file. A file-level pass would let an unrelated sibling assertion take the credit.
 *
 * Usage: node scripts/verify-behavioural-guards.mjs [repoRoot]
 * Exits non-zero if any mutation is MISSED, SKIPPED, or names a test that does not exist.
 *
 * TO CHECK THAT THIS SCRIPT ITSELF STILL HAS TEETH — worth doing after any edit to it, because a
 * broken guard and a working guard produce identical output on a healthy tree:
 *
 *   cp scripts/verify-behavioural-guards.mjs /tmp/drift.mjs
 *   # in /tmp/drift.mjs, append " BUT RENAMED" to any one of the testName strings below
 *   node /tmp/drift.mjs "$PWD"
 *
 * It must refuse with "NAME MATCHES NOTHING" and exit non-zero. If it instead reports 19 CAUGHT,
 * the ran-detection in runNamed() is broken and every number this script prints is meaningless.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const REPO = path.resolve(process.argv[2] || process.cwd());

/** Files a mutation may touch. Copied once, restored between mutations from an in-memory pristine. */
const FILES = [
  "PdfReaderApp.tsx",
  "app.json",
  "ios/SignoVivo/BlePageBeacon.swift",
  "ios/SignoVivo/DirectorSyncModule.swift",
  "ios/SignoVivo/DirectorSyncModuleBridge.m",
  "src/nearbyDirectorSync.js",
  "scripts/release.sh",
  "src/alverniaManual2SongIndex.js",
  "sync-worker/src/index.ts",
  "web/src/app.js",
  "web/src/styles.css",
];

const sub = (a, b) => (s) => s.replace(a, b);

// Each entry: [description, file, mutate, testFile, testName]
// `testName` is matched with --test-name-pattern, so the NAMED test must be the one that fails.
const MUTATIONS = [
  ["the publish rate limit — the only abuse control on an open endpoint — is deleted",
   "sync-worker/src/index.ts",
   sub("    if (this.rateLimited(ip, 15, 2)) {", "    if (false) {"),
   "e2e/relayPublishGate.test.mjs", "publish is still rate limited"],

  ["the four-way director predicate becomes a conjunction — a follower hangs up on its own director",
   "ios/SignoVivo/DirectorSyncModule.swift",
   (s) => {
     const at = s.indexOf("let isDirector = self.pendingInvitePeer == peerID");
     if (at < 0) return s;
     const end = s.indexOf('== "director"', at);
     if (end < 0) return s;
     const block = s.slice(at, end + '== "director"'.length);
     return s.slice(0, at) + block.replace(/\|\|/g, "&&") + s.slice(at + block.length);
   },
   "e2e/sessionAdmission.test.mjs", "both director predicates stay in agreement"],

  ["parseInboundPayload force-unwraps — one malformed packet from any peer crashes the app",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub(`    guard data.count > 0, data.count <= Self.maxInboundPayloadBytes else { return nil }
    guard let obj = try? JSONSerialization.jsonObject(with: data) else { return nil }
    return obj as? [String: Any]`,
       `    let obj = try! JSONSerialization.jsonObject(with: data)
    return (obj as! [String: Any])`),
   "e2e/nearby-sync-contract.test.mjs", "parseInboundPayload gracefully rejects invalid JSON"],

  ["a peer connecting to the director no longer gets an immediate page snapshot",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub(`          self.sendDirectorAnnounce(to: peerID)
          self.sendCurrentPageSnapshot(to: peerID, via: session)`,
       `          self.sendDirectorAnnounce(to: peerID)`),
   "e2e/nearby-sync-contract.test.mjs", "director immediately snapshots on connect and on hello"],

  ["the catch-up snapshot for a late joiner becomes best-effort and can be dropped",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub(`    do {
      try session.send(data, toPeers: [peerID], with: .reliable)`,
       `    do {
      try session.send(data, toPeers: [peerID], with: .unreliable)`),
   "e2e/nearby-sync-contract.test.mjs", "sendCurrentPageSnapshot uses reliable delivery"],

  ["the MCPeerID display-name cap is removed at the site that feeds the real peer identity",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    let peerName = String(rawName.prefix(50))", "    let peerName = rawName"),
   "e2e/nearby-sync-contract.test.mjs", "caps peer display name to 50 chars"],

  ["invalidatePendingSettle becomes a no-op — the retry ladder sawtooths and never climbs",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    if isAdvertiser { advertiserAttemptToken &+= 1 } else { browserAttemptToken &+= 1 }",
       "    _ = isAdvertiser"),
   "e2e/transportBackoff.test.mjs", "a failed attempt cancels the settle it scheduled"],

  ["a mesh-page precondition returns to the BLE path — the fallback dies exactly when the mesh breaks",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("      guard seq > self.bleAppliedSeq else { return }",
       "      guard self.lastKnownTotalPages != 0 else { return }\n      guard seq > self.bleAppliedSeq else { return }"),
   "e2e/dumbFollowerResync.test.mjs", "BLE renders standalone again"],

  ["the BLE unchanged-page early return is deleted — advertSeq climbs ~5,400 times a Mass",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    guard page != lastPublishedPage else { return }\n", ""),
   "e2e/bleHandoff.test.mjs", "advertised seq is bounded by page turns"],

  ["clampPage loses its upper bound — a song jump past the end sticks on a 404",
   "web/src/app.js",
   sub("  return Math.max(1, Math.min(n, total));", "  return Math.max(1, n);"),
   "e2e/native-entrypoint.test.mjs", "song index resolves correctly"],

  ["renderPage consults the pacing rule and ignores it — the 1 Hz retry storm returns",
   "web/src/app.js",
   sub("  if (!userInitiated && svShouldPaceRender(state.lastRenderFailure, nextPage, Date.now())) return;",
       "  if (!userInitiated && svShouldPaceRender(state.lastRenderFailure, nextPage, Date.now())) { /* ignored */ }"),
   "e2e/renderRetryStorm.test.mjs", "actually CALLS the shared rule inside renderPage"],

  ["the director pill ships to the public web, where there is no mesh and no role to take",
   "web/src/app.js",
   sub("  const inShell = NATIVE_FILE_MODE || hasNativeBridge();", "  const inShell = true;"),
   "e2e/directorButton.test.mjs", "the pill is the only thing that asks for the role"],

  ["the two director fabs overlap by 2rem on the iPad",
   "web/src/styles.css",
   sub("  --fab-slot2: calc(var(--fab-size) + var(--fab-gap));", "  --fab-slot2: calc(var(--fab-size) - 2rem);"),
   "e2e/fabLayout.test.mjs", "never overlap"],

  ["the songbook index is truncated — 300+ songs no longer resolve",
   "src/alverniaManual2SongIndex.js",
   (s) => {
     const open = s.indexOf("[");
     const close = s.lastIndexOf("]");
     if (open < 0 || close < open) return s;
     const pairs = [...s.slice(open, close).matchAll(/\[\s*\d+\s*,\s*\d+\s*\]/g)].map((m) => m[0]);
     if (pairs.length < 20) return s;
     return s.slice(0, open + 1) + "\n  " + pairs.slice(0, 10).join(", ") + ",\n" + s.slice(close);
   },
   "e2e/offline-books-integrity.test.mjs", "canonical standard song index"],

  ["every song in the index is remapped to page 1",
   "src/alverniaManual2SongIndex.js",
   (s) => s.replace(/\[\s*(\d+)\s*,\s*(\d+)\s*\]/g, (m, song) => `[${song}, 1]`),
   "e2e/offline-books-integrity.test.mjs", "canonical standard song index"],

  ["app config points its icon at a file that does not exist",
   "app.json",
   sub('"icon": "./assets/03_icon_1024x1024.png"', '"icon": "./assets/03_icon_RENAMED.png"'),
   "e2e/eas-config.test.mjs", "Release assets required by app config"],

  ["a stale native page mirror is trusted over the web's fresh one at takeover",
   "PdfReaderApp.tsx",
   sub('      if (typeof knownCurrentPage === "number" && knownCurrentPage > 0) {',
       '      if (typeof knownCurrentPage === "number" && knownCurrentPage < 0) {'),
   "e2e/relayQuotaGuards.test.mjs", "a stale mirror is only ever a fallback"],

  ["a notice starts telling people where a control is, which is the rot that outlives the layout",
   "PdfReaderApp.tsx",
   sub(`        const noticeText = "Hay una versión más reciente del app disponible. Puedes " +
          "actualizar ahora, o mientras tanto usar signovivo.com desde cualquier navegador.";`,
       `        const noticeText = "Hay una versión más reciente del app disponible. Toca el " +
          "botón arriba a la izquierda para actualizar ahora.";`),
   "e2e/noticesCarryControls.test.mjs", "no notice tells anyone where a control is"],

  ["the release script's build-number collision guard is inverted",
   "scripts/release.sh",
   sub('  if [ -e "$IPA_OUT" ] && [ "${ALLOW_REUSED_BUILD:-0}" != "1" ]; then',
       '  if [ ! -e "$IPA_OUT" ] && [ "${ALLOW_REUSED_BUILD:-0}" != "1" ]; then'),
   "e2e/buildNumberGuard.test.mjs", "refuses a number whose IPA already exists"],
  ["a forced handover rescue keeps the OLD seq floor, so the follower freezes on one page for the rest of the Mass",
   "web/src/lib/svSyncDecision.js",
   sub("    out.lastSeq = snap.seq;", "    out.lastSeq = Math.max(ctx.lastSeq, snap.seq);"),
   "e2e/svSyncDecision.test.mjs", "handover: the page turn AFTER a forced rescue still renders"],

  ["broadcastPage floors an invalid page to 1 again — a confident wrong page to the whole fleet",
   "PdfReaderApp.tsx",
   sub("    const page = rawPage;",
       "    const page = Number.isFinite(rawPage) && rawPage >= 1 ? rawPage : 1;"),
   "e2e/broadcastNeverWrongPage.test.mjs",
   "broadcastPage refuses an invalid page instead of flooring it to page 1"],

  ["the 100ms director heartbeat loses its own guard and hands a -1 mirror straight to Swift",
   "PdfReaderApp.tsx",
   sub("      if (!Number.isFinite(currentPageRef.current) || currentPageRef.current < 1) return;\n", ""),
   "e2e/broadcastNeverWrongPage.test.mjs",
   "the director heartbeat refuses an invalid page before sending it"],

  ["render-failed blanks the mirror during becomeDirector's await window again",
   "PdfReaderApp.tsx",
   sub('if (roleRef.current === "follower" && !becomeDirectorInFlightRef.current) {',
       'if (roleRef.current === "follower") {'),
   "e2e/broadcastNeverWrongPage.test.mjs",
   "render-failed does not blank the mirror while this device is becoming director"],

  ["the revoke condition is narrowed so a MISSING pointer no longer revokes — a withdrawn book stays installed",
   "PdfReaderApp.tsx",
   sub("(!pointer || pointer.bookVersion !== staged.bookVersion)",
       "(pointer && pointer.bookVersion !== staged.bookVersion)"),
   "e2e/otaRevokeIsTotal.test.mjs", "a revoke deletes a STAGED copy"],

  ["the parked-pointer clear becomes conditional — one tap can resurrect the withdrawn book",
   "PdfReaderApp.tsx",
   sub("          pendingPointerRef.current = null;",
       "          if (staged?.bookVersion) pendingPointerRef.current = null;"),
   "e2e/otaRevokeIsTotal.test.mjs", "a revoke clears the PARKED pointer that ⟳ replays offline"],

  ["the mid-flight disarm check is gone — a revoked download finishes and installs anyway",
   "PdfReaderApp.tsx",
   sub("          if (stagingDisarmedRef.current) {",
       "          if (stagingInFlightRef.current === null) {"),
   "e2e/otaRevokeIsTotal.test.mjs", "a revoke stops a download already in flight"],

  ["an internal worker error returns the DESTRUCTIVE 200-without-bookUpdate instead of 503",
   "sync-worker/src/index.ts",
   sub('          return json({ ok: false, error: "arming_unavailable" }, 503, cors);',
       '          return json({ ok: false, error: "arming_unavailable" }, 200, cors);'),
   "e2e/otaRevokeIsTotal.test.mjs", "an internal worker error cannot impersonate a revoke"],

  ["BLE parse() goes lenient and accepts the pre-tag 3-field form — the build-444 wrong song, re-armed",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("guard parts.count == 4, let seq", "guard parts.count >= 3, let seq"),
   "e2e/bleHandoff.test.mjs", "the safety rules that made BLE dangerous in 444 are still in place"],

  ["resetTransport stops silencing the beacon — a device that directed song 357 broadcasts it forever",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    // as a follower. That ghost advertisement is what build 444 rendered as song 357.\n    bleBeacon.stopPublishing()\n",
       "    // as a follower. That ghost advertisement is what build 444 rendered as song 357.\n"),
   "e2e/bleHandoff.test.mjs", "the safety rules that made BLE dangerous in 444 are still in place"],

  ["a contested BLE packet is applied instead of abstained — followers ping-pong between advertisers",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("      lastContentionAt = now\n      for nonce in recentNonces.keys { contestedNonces.insert(nonce) }   // everyone on the air is suspect\n      return\n    }",
       "      lastContentionAt = now\n      for nonce in recentNonces.keys { contestedNonces.insert(nonce) }   // everyone on the air is suspect\n    }"),
   "e2e/bleHandoff.test.mjs", "TWO LIVE ADVERTISERS: render NEITHER, rather than flapping or trusting the wrong one"],

  ["the post-contention cooldown is deleted — a rival slower than contentionWindow resumes rendering",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    if now - lastContentionAt < Self.contentionCooldown {\n      lastAppliedPage = -1\n      contestedNonces.insert(parsed.nonce)\n      return\n    }\n", ""),
   "e2e/bleHandoff.test.mjs", "TWO LIVE ADVERTISERS: render NEITHER, rather than flapping or trusting the wrong one"],

  ["primeRadios warms only the central — a follower that becomes director pays the cold start",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("  func primeRadios() {\n    if peripheral == nil { peripheral = CBPeripheralManager(delegate: self, queue: .main) }\n",
       "  func primeRadios() {\n"),
   "e2e/bleHandoff.test.mjs", "neither radio is created on the critical path"],

  ["the BLE seq floor goes back to ONE shared baseline — a frozen ghost re-qualifies and holds a stale song",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    guard parsed.seq > floor else { return }",
       "    guard parsed.seq > (seenSeqByNonce[lastSeenNonce]?.seq ?? -1) else { return }"),
   "e2e/dumbFollowerResync.test.mjs",
   "a scanner keeps a seq floor PER ADVERTISER, so a new director is never mis-ordered"],

  ["the per-advertiser floor stops being recorded before abstention — contention throws away the ghost evidence",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    seenSeqByNonce[parsed.nonce] = (seq: max(priorSeq, parsed.seq), at: now)", ""),
   "e2e/bleHandoff.test.mjs",
   "both layers carry the nonce, so neither can drift from the other"],

  ["the BLE applied-floor loses precedence — a director's own contested packets raise the floor against it (the #395 regression)",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    if let applied = appliedSeqByNonce[parsed.nonce] {\n      floor = applied\n    } else if",
       "    if false, let applied = appliedSeqByNonce[parsed.nonce] {\n      floor = applied\n    } else if"),
   "e2e/bleHandoff.test.mjs", "both layers carry the nonce, so neither can drift from the other"],

  ["the foreground handler calls the advertiser-destroying refresh for a serving director again",
   "PdfReaderApp.tsx",
   sub('        (roleRef.current === "director" ? refreshDirectorBrowse() : refreshNearbyDiscovery()).catch(() => {});',
       '        refreshNearbyDiscovery().catch(() => {});'),
   "e2e/foregroundKeepsAdvertiser.test.mjs",
   "the foreground handler gives a director the browser-only refresh, never the advertiser-destroying one"],

  ["refreshNearbyDiscovery destroys a live advertiser while serving — the Swift guard is removed",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub('      if self.currentRole == "director", !self.allConnectedPeers.isEmpty {\n        self.refreshBrowserOnly()\n      } else {\n        self.refreshDiscovery()\n      }\n',
       '      self.refreshDiscovery()\n'),
   "e2e/foregroundKeepsAdvertiser.test.mjs",
   "refreshNearbyDiscovery itself refuses to destroy a live advertiser while serving followers"],

  ["the takeover confirm broadcasts the page captured when the dialog OPENED — the choir holds a stale page for a song",
   "PdfReaderApp.tsx",
   sub("          onPress: () => becomeDirector(code, pageAtConfirm() ?? restoredDirectorPageRef.current),",
       "          onPress: () => becomeDirector(code, knownCurrentPage ?? restoredDirectorPageRef.current),"),
   "e2e/takeoverPageAtConfirm.test.mjs",
   "the takeover dialog snapshots the mirror when it OPENS and resolves the page at CONFIRM"],

  ["pageAtConfirm's comparison inverts — a page turned during the dialog is dropped and a lagging mirror wins",
   "PdfReaderApp.tsx",
   sub("        return live !== mirrorAtOpen && Number.isFinite(live) && live > 0 ? live : knownCurrentPage;",
       "        return live === mirrorAtOpen && Number.isFinite(live) && live > 0 ? live : knownCurrentPage;"),
   "e2e/takeoverPageAtConfirm.test.mjs",
   "pageAtConfirm prefers a mirror that MOVED during the dialog, keeps the captured page otherwise, never the sentinel"],

  ["a director's page turns stop refreshing lastDirectorPage — the crash-resume page is the promotion-time page again",
   "PdfReaderApp.tsx",
   sub('          if (roleRef.current === "director") {\n            AsyncStorage.setItem(STORAGE_KEYS.lastDirectorPage, String(page)).catch(() => {});\n          }\n', ''),
   "e2e/crashResumeRestoresPage.test.mjs",
   "a director's page turns keep lastDirectorPage fresh, so the restored page is the crash-time page"],

  ["bootstrap stops driving the web to the restored page — 'Volver a dirigir' broadcasts the boot default again",
   "PdfReaderApp.tsx",
   sub("            const restored = restoredDirectorPageRef.current;", "            const restored = undefined;"),
   "e2e/crashResumeRestoresPage.test.mjs",
   "bootstrap drives the web to the restored page, so 'Volver a dirigir' carries it instead of the boot default"],

  ["the 30s relay heartbeat loses its own guard and hands a -1 mirror to publishPageToRelay, which floors it to page 1",
   "PdfReaderApp.tsx",
   sub("      if (!Number.isFinite(currentPageRef.current) || currentPageRef.current < 1) return;\n      try {\n",
       "      try {\n"),
   "e2e/broadcastNeverWrongPage.test.mjs",
   "the relay heartbeat refuses an invalid page before publishing it"],

  ["a superseded becomeDirector releases the in-flight claim unconditionally — the newer call's render-failed gate is re-armed",
   "PdfReaderApp.tsx",
   sub("          if (myGen !== roleGenerationRef.current) { if (becomeDirectorInFlightRef.current === myGen) becomeDirectorInFlightRef.current = 0; return; } // superseded during the retry sleep",
       "          if (myGen !== roleGenerationRef.current) { becomeDirectorInFlightRef.current = 0; return; } // superseded during the retry sleep"),
   "e2e/becomeDirectorInFlightGeneration.test.mjs",
   "every release of the in-flight claim is guarded by === myGen"],

  ["testflight-distribute attaches external groups by default again — the choir gets the build before hardware testing",
   "scripts/testflight-distribute.mjs",
   sub("    return allowExternal === true && wanted !== null;", "    return true;"),
   "e2e/testflightNoExternalGroups.test.mjs",
   "by default only INTERNAL groups are selected — an external group is never attached implicitly"],

  ["--allow-external alone reaches EVERY external group instead of only a named one",
   "scripts/testflight-distribute.mjs",
   sub("    return allowExternal === true && wanted !== null;", "    return allowExternal === true;"),
   "e2e/testflightNoExternalGroups.test.mjs",
   "only --allow-external reaches an external group, and it still requires naming it"],

  ["a soft reset leaves relay publishing ENABLED — a queued frame drains to every web follower from a device that just reset",
   "PdfReaderApp.tsx",
   sub("    // becomeFollower does this for the step-down path; the reset path did not.\n    setRelayPublishing(false);\n",
       "    // becomeFollower does this for the step-down path; the reset path did not.\n"),
   "e2e/stepDownStopsPublishing.test.mjs",
   "performSoftReset disables relay publishing before it tears the role down"],

  ["a soft reset parks the device in role 'off' with every transport down while the web shows the follower UI",
   "PdfReaderApp.tsx",
   sub("    void becomeFollower();\n", ""),
   "e2e/stepDownStopsPublishing.test.mjs",
   "performSoftReset re-enters follower mode after the remount instead of stranding the device 'off'"],

  ["a failed takeover that was not previously following leaves publishing enabled with no director role",
   "PdfReaderApp.tsx",
   sub("          explicitTransmitterRef.current = false;\n          setRelayPublishing(false);\n          injectEvent({ type: \"role\", role: \"none\" });\n",
       "          explicitTransmitterRef.current = false;\n          injectEvent({ type: \"role\", role: \"none\" });\n"),
   "e2e/stepDownStopsPublishing.test.mjs",
   "becomeDirector's non-follower failure path disables relay publishing"],

  ["the serving-director hold fires on the bare presence of a follower sighting again — one stale sighting pins a deaf advertiser for the rest of Mass",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("             !self.allConnectedPeers.isEmpty || !self.pendingAdmissions.isEmpty || self.hasRecentFollowerSighting() {\n",
       "             !self.allConnectedPeers.isEmpty || !self.pendingAdmissions.isEmpty || !self.discoveredFollowers.isEmpty {\n"),
   "e2e/holdServingRequiresRecentSighting.test.mjs",
   "the serving-director hold requires a CONNECTED peer or a RECENT sighting, never a bare memory"],

  ["follower sightings stop being timestamped — every sighting is forever fresh",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("        self.discoveredFollowers.insert(peerID)\n        self.discoveredFollowerSeenAt[peerID] = Date().timeIntervalSince1970\n",
       "        self.discoveredFollowers.insert(peerID)\n"),
   "e2e/holdServingRequiresRecentSighting.test.mjs",
   "follower sightings are timestamped, like director sightings already are"],

  ["a failed invite restarts the wedge clock again — the 20 s session rebuild can never fire while invites keep failing",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("          if self.followerHuntingSince == 0 { self.followerHuntingSince = Date().timeIntervalSince1970 }\n",
       "          self.followerHuntingSince = Date().timeIntervalSince1970\n"),
   "e2e/meshWedges.test.mjs",
   "a failed invite does not restart the wedged-session clock — only arm it if it is not running"],

  ["a peer re-sighted as a follower keeps its director records — the conflict winner rejects the loser forever",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("        self.discoveredDirectors.removeValue(forKey: peerID)\n        self.discoveredDirectorInfo.removeValue(forKey: peerID)\n        self.discoveredDirectorSeenAt.removeValue(forKey: peerID)\n", ""),
   "e2e/meshWedges.test.mjs",
   "a peer re-sighted as a FOLLOWER is no longer remembered as a director"],

  ["the peer-is-director check loses its freshness requirement — a stale record refuses a legitimate follower",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("          && directorSeenAgo < Self.browserHealthySeconds\n", ""),
   "e2e/meshWedges.test.mjs",
   "the director's invite check treats a director sighting as evidence only while it is FRESH"],

  ["the post-gate reconciliation is deleted — a fresh director reveals on the cover with state on page 2 and broadcasts song 2",
   "web/src/app.js",
   sub("    if (!firstNativePageArrived && renderedPage() !== state.currentPage) {\n      await renderPage(renderedPage(), { pushToHistory: false, notifyNative: false });\n    }\n", ""),
   "e2e/bootStateMatchesScreen.test.mjs",
   "after the native reveal gate, a screen that disagrees with state is RENDERED so state, mirror and badge agree"],

  ["the reconciliation only assigns state instead of rendering — native's mirror keeps the boot default and the badge is never re-synced",
   "web/src/app.js",
   sub("      await renderPage(renderedPage(), { pushToHistory: false, notifyNative: false });\n", "      state.currentPage = renderedPage();\n"),
   "e2e/bootStateMatchesScreen.test.mjs",
   "after the native reveal gate, a screen that disagrees with state is RENDERED so state, mirror and badge agree"],

  ["the page handler stops recording the arrival — the reconciliation cannot tell a director's page from the static boot image",
   "web/src/app.js",
   sub("      firstNativePageArrived = true;\n", ""),
   "e2e/bootStateMatchesScreen.test.mjs",
   "the sync-event page handler records that a native page arrived, so a real page is never overridden by the reconciliation"],

  ["renderedPage() stops understanding a ?reload= retry token — a retried page reads as state instead of the screen",
   "web/src/app.js",
   sub("/page-(\\d+)\\.webp(?:\\?.*)?$/", "/page-(\\d+)\\.webp$/"),
   "e2e/bootStateMatchesScreen.test.mjs",
   "renderedPage() reads the page from the <img> itself — the static boot image is page 1"],

  ["a version-mismatch stage failure stops counting toward quarantine — a stale arm pointer re-fetches the manifest every check-in forever",
   "PdfReaderApp.tsx",
   sub("            (rec.error === \"cannot-outrank-baked-shell\" || rec.error === \"version-mismatch\") &&\n",
       "            rec.error === \"cannot-outrank-baked-shell\" &&\n"),
   "e2e/stageFailureQuarantine.test.mjs",
   "a version-mismatch stage failure counts toward quarantine, not just cannot-outrank-baked-shell"],

  ["the already-on-this-page return stops invalidating an in-flight render — a mis-tap correction commits the wrong page over the right one",
   "web/src/app.js",
   sub("    state.pageLoadRequest += 1;\n    if (pageImage.classList.contains(\"is-loading\")) hideLoadingIndicator();\n", ""),
   "e2e/renderPageInFlightCancel.test.mjs",
   "a request for the page already on screen invalidates a render still in flight for another page"],

  ["release.sh stops honouring SKIP_OTA_ARM — a release meant to stay unarmed arms the whole fleet and redeploys the worker",
   "scripts/release.sh",
   sub("  if [ \"${SKIP_OTA_ARM:-0}\" = \"1\" ]; then\n", "  if [ \"${SKIP_OTA_ARM:-0}\" = \"never\" ]; then\n"),
   "e2e/releaseSkipOtaArm.test.mjs",
   "SKIP_OTA_ARM=1 leaves the worker untouched: no rewrite, no deploy, and it says so"],

  ["the OTA arm's fail-soft fallback is deleted — a failed worker deploy aborts a release that already reached TestFlight and prod",
   "scripts/release.sh",
   sub("      || echo \"         ⚠ OTA arm/deploy failed — songbook update AND the native-build nudge will NOT reach devices until this is fixed and re-run\"\n",
       "      || true\n"),
   "e2e/otaStandingFleetArm.test.mjs",
   "a failed OTA arm/deploy does not abort the release — it must fail soft and say so"],

  ["the boot reconciliation posts page-changed to native again — a director whose remount re-assert lands after the gate adopts the cover and broadcasts it",
   "web/src/app.js",
   sub("      await renderPage(renderedPage(), { pushToHistory: false, notifyNative: false });\n",
       "      await renderPage(renderedPage(), { pushToHistory: false });\n"),
   "e2e/bootStateMatchesScreen.test.mjs",
   "the boot reconciliation never posts page-changed to native — a director's late remount would adopt and broadcast the cover"],

  ["renderPage ignores notifyNative — every render, the reconciliation included, reaches native as a page-changed",
   "web/src/app.js",
   sub("    if (notifyNative) {\n      postNativeBridge({\n        type: \"page-changed\",\n",
       "    {\n      postNativeBridge({\n        type: \"page-changed\",\n"),
   "e2e/bootStateMatchesScreen.test.mjs",
   "the boot reconciliation never posts page-changed to native — a director's late remount would adopt and broadcast the cover"],

  ["the hold stops honouring a handshake in flight — a re-inviting follower's handshake is torn down by the refresh tick",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("             !self.allConnectedPeers.isEmpty || !self.pendingAdmissions.isEmpty || self.hasRecentFollowerSighting() {\n",
       "             !self.allConnectedPeers.isEmpty || self.hasRecentFollowerSighting() {\n"),
   "e2e/holdServingHandshake.test.mjs",
   "the hold honours an in-flight handshake (pendingAdmissions), a connection, or a fresh sighting"],

  ["invite:accept stops stamping the sighting — a same-peer re-invite after 20 s is invisible to the hold",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("          // then read \"no recent sighting\" and refreshed the advertiser under this very handshake.\n          self.discoveredFollowerSeenAt[peerID] = Date().timeIntervalSince1970\n",
       "          // then read \"no recent sighting\" and refreshed the advertiser under this very handshake.\n"),
   "e2e/holdServingHandshake.test.mjs",
   "accepting an invite stamps the follower sighting — an invite IS a sighting"],

  ["hasRecentFollowerSighting's comparison inverts — the director holds ONLY on stale sightings and refreshes every 5 s while followers handshake",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    return Date().timeIntervalSince1970 - newest < Self.browserHealthySeconds\n",
       "    return Date().timeIntervalSince1970 - newest >= Self.browserHealthySeconds\n"),
   "e2e/holdServingHandshake.test.mjs",
   "the freshness comparison is `age < browserHealthySeconds`, not its inverse"],

  ["configureTransport stops clearing discoveredDirectorSeenAt — the three director records drift apart",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    discoveredDirectorSeenAt = [:]   // the three director records clear as a unit, everywhere\n", ""),
   "e2e/holdServingHandshake.test.mjs",
   "configureTransport clears the three director records as a unit"],

  ["the peer-is-director freshness comparison inverts — a fresh director is ACCEPTED as a follower (split-brain), a stale record still refuses",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("          && directorSeenAgo < Self.browserHealthySeconds\n", "          && directorSeenAgo > Self.browserHealthySeconds\n"),
   "e2e/meshWedges.test.mjs",
   "the director's invite check treats a director sighting as evidence only while it is FRESH"],

  ["the already-directing failure exit leaves the heartbeats running and roleRef 'director' while the web shows 'none'",
   "PdfReaderApp.tsx",
   sub("          stopDirectorHeartbeat();\n          roleRef.current = \"off\";\n          explicitTransmitterRef.current = false;\n          setRelayPublishing(false);\n          injectEvent({ type: \"role\", role: \"none\" });\n",
       "          setRelayPublishing(false);\n          injectEvent({ type: \"role\", role: \"none\" });\n"),
   "e2e/directorCatchExitNeutral.test.mjs",
   "a failed takeover by a device that was already directing stops the heartbeats and mirrors role off"],

  ["the reset confirmation goes back to saying the device stops directing OR following — it re-enters follower mode",
   "PdfReaderApp.tsx",
   sub("Se reinicia la conexión: este dispositivo deja de dirigir y vuelve a buscar al director.",
       "Se reinicia la conexión y este dispositivo deja de dirigir o seguir."),
   "e2e/directorCatchExitNeutral.test.mjs",
   "the reset confirmation describes what a soft reset now does: back to following, not neutral"],

  ["a quarantined pointer writes a stage-skip breadcrumb on every 4-minute check-in again — the forensics ring fills with a settled state",
   "PdfReaderApp.tsx",
   sub("            decision.reason !== \"already-staged\" &&\n            decision.reason !== \"quarantined\"\n",
       "            decision.reason !== \"already-staged\"\n"),
   "e2e/stageFailureQuarantine.test.mjs",
   "a quarantined pointer is a settled state — no stage-skip breadcrumb per check-in for the life of the install"],

  // ── 2026-09-05 three-device hardware test: two directors for 45 s, and a dead advertisement replayed ──

  ["the takeover no longer announces the new token to the director it replaces — 45 s of two directors again",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("      self.sendControlPayload([\n        \"v\": Self.protocolVersion,\n        \"type\": \"director_announce\",\n        \"token\": token,\n      ], to: oldDirector)\n", ""),
   "e2e/takeoverAnnounce.test.mjs",
   "takeoverDirector announces the minted token to the connected director BEFORE becoming director"],

  ["beginDirecting mints its own token — the announced token and the advertised one differ, so the replaced director never demotes",
   "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    currentDirectorToken = token\n", "    currentDirectorToken = Self.randomToken()\n"),
   "e2e/takeoverAnnounce.test.mjs",
   "takeoverDirector announces the minted token to the connected director BEFORE becoming director"],

  ["the JS takeover path goes back to the plain director start — the announcing takeover is never called",
   "PdfReaderApp.tsx",
   sub("const startAsDirector = wasFollower ? takeoverNearbyDirector : startNearbyDirector;",
       "const startAsDirector = startNearbyDirector;"),
   "e2e/takeoverAnnounce.test.mjs",
   "the takeover is exported to JavaScript and the JS path no longer drops the link first"],

  ["takeoverDirector is dropped from the bridge .m — JS sees undefined and silently falls back to drop-then-start",
   "ios/SignoVivo/DirectorSyncModuleBridge.m",
   sub("RCT_EXTERN_METHOD(takeoverDirector:(NSString *)sessionCode\n                  resolver:(RCTPromiseResolveBlock)resolve\n                  rejecter:(RCTPromiseRejectBlock)reject)\n\n", ""),
   "e2e/nativeBridgeExports.test.mjs",
   "every Swift @objc method is declared in the bridge .m"],

  ["the BLE newcomer grace is deleted — a dead director's replayed advertisement renders on its first packet again",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    if appliedSeqByNonce[parsed.nonce] == nil,\n       let first = firstHeardByNonce[parsed.nonce], now - first.at < Self.newAdvertiserGrace {\n      return\n    }\n", ""),
   "e2e/bleHandoff.test.mjs",
   "THE 2026-09-05 REPLAY: a dead director's advertisement re-emitted at its successor's start must not render"],

  ["a contested newcomer is judged against its LAST-seen seq again — the surviving live director's page waits for the next turn",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("    } else if contestedNonces.contains(parsed.nonce) {\n      floor = firstSeen\n",
       "    } else if contestedNonces.contains(parsed.nonce) {\n      floor = priorSeq\n"),
   "e2e/bleHandoff.test.mjs",
   "THE 2026-09-05 REPLAY: a dead director's advertisement re-emitted at its successor's start must not render"],

  ["contention stops marking the advertisers on the air — a frozen ghost heard under contention re-qualifies as an uncontested newcomer",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("      for nonce in recentNonces.keys { contestedNonces.insert(nonce) }   // everyone on the air is suspect\n", ""),
   "e2e/bleHandoff.test.mjs",
   "THE 2026-09-05 REPLAY: a dead director's advertisement re-emitted at its successor's start must not render"],

  ["an uncontested newcomer must climb too — a lone brand-new director is refused until its first page turn",
   "ios/SignoVivo/BlePageBeacon.swift",
   sub("      floor = firstSeen - 1\n", "      floor = firstSeen\n"),
   "e2e/bleHandoff.test.mjs",
   "THE 2026-09-05 REPLAY: a dead director's advertisement re-emitted at its successor's start must not render"],

];

// ── run ─────────────────────────────────────────────────────────────────────────────────────────
const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "sv-behavguard-"));
const TESTS = [...new Set(MUTATIONS.map((m) => m[3]))];
const HELPERS = fs.existsSync(path.join(REPO, "e2e/helpers"))
  ? fs.readdirSync(path.join(REPO, "e2e/helpers")).map((f) => `e2e/helpers/${f}`)
  : [];
// Copy the whole tracked tree: these tests read assets, fixtures and sibling sources, and a
// hand-maintained file list is the kind of thing that silently rots into a skip.
const TRACKED = execFileSync("git", ["-C", REPO, "ls-files", "-z"], { encoding: "utf8" })
  .split("\0").filter(Boolean);
for (const f of [...TRACKED, ...HELPERS]) {
  const from = path.join(REPO, f);
  let st;
  try { st = fs.lstatSync(from); } catch { continue; }
  if (!st.isFile()) continue;   // `.claude/worktrees/*` is a committed gitlink, not a file
  const to = path.join(ROOT, f);
  fs.mkdirSync(path.dirname(to), { recursive: true });
  fs.copyFileSync(from, to);
}

// Snapshot EVERY file any mutation touches — the hand-maintained FILES list alone is not enough. When
// a new mutation targeted a file absent from that literal, restore() never put it back, so the first
// mutation's edit leaked into the second mutation's `before`, whose pattern then found nothing and was
// reported as SKIP (2026-09-04, scripts/testflight-distribute.mjs). Deriving the set from MUTATIONS
// makes that impossible; FILES is kept as a superset for files a test reads without mutating.
const SNAPSHOT = [...new Set([...FILES, ...MUTATIONS.map((m) => m[1])])];
const orig = Object.fromEntries(SNAPSHOT.map((f) => [f, fs.readFileSync(path.join(ROOT, f), "utf8")]));
const restore = () => SNAPSHOT.forEach((f) => fs.writeFileSync(path.join(ROOT, f), orig[f]));

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * Run one named test. Returns { ran, green }.
 *
 * `ran` IS NOT OPTIONAL BOOKKEEPING. `--test-name-pattern` that matches nothing exits ZERO with an
 * empty plan:
 *
 *     TAP version 13
 *     1..0
 *     # Subtest: e2e/svSyncBoundaries.test.mjs
 *     ok 1 - e2e/svSyncBoundaries.test.mjs
 *
 * The `ok 1` there is the FILE, not a test. So a name that drifts — a test renamed, a typo, an em
 * dash turned into a hyphen — makes the baseline look green AND every mutation look caught, while
 * running nothing at all. This script would then report a confident "19 CAUGHT" having verified
 * zero of them, which is precisely the silent-nothing failure it exists to detect. Verified against
 * node 22: the zero-match case is distinguishable only by the absence of a Subtest line naming the
 * test.
 */
function runNamed(testFile, testName) {
  let out = "";
  let green = true;
  try {
    // The reporter is PINNED to tap. The first version of this read the default reporter's output,
    // which makes the check quietly dependent on a node upgrade changing its default — and on how
    // that reporter escapes a test name containing `#` or a backslash. The TAP plan line is a
    // documented part of the format and needs no name matching at all.
    out = execFileSync("node", ["--test", "--test-reporter=tap", "--test-name-pattern", esc(testName), testFile],
      { cwd: ROOT, encoding: "utf8", stdio: "pipe" });
  } catch (e) {
    green = false;
    out = `${e.stdout ?? ""}${e.stderr ?? ""}`;
  }
  // TWO SIGNALS, BOTH REQUIRED.
  //
  // A pattern that matches nothing emits an EMPTY PLAN (`1..0`) and exits zero — so without the
  // first check a drifted test name would make the baseline look green and every mutation look
  // caught while running none of them.
  //
  // But the plan check ALONE is not enough either, and that was a real regression in an earlier
  // version of this file: when a mutation breaks the test file at module scope (a syntax error, a
  // failing top-level assertion, a bad import) the run aborts before any test executes. TAP then
  // emits a failing subtest named after the FILE and no `1..0` at all, so `ran` was true, `green`
  // was false, and the mutation was scored CAUGHT — crediting the tests for what is really a
  // loader error. So the named test must ALSO appear as a subtest of its own.
  const emptyPlan = /^\s*1\.\.0\s*$/m.test(out);
  const namedSubtest = out.split("\n").some(
    (l) => l.startsWith("# Subtest:") && l.slice("# Subtest:".length).includes(testName),
  );
  return { ran: !emptyPlan && namedSubtest, green };
}

// BASELINE FIRST. A red baseline makes every mutation look "caught" and reports a perfect score
// while proving nothing — which is exactly what happened the first time the sibling harness ran.
let baselineBad = 0;
for (const [, , , testFile, testName] of MUTATIONS) {
  const { ran, green } = runNamed(testFile, testName);
  if (!ran) {
    console.error(`✖ NAME MATCHES NOTHING: ${testFile} :: "${testName}"`);
    console.error("  The test has been renamed or removed. Every mutation below would report CAUGHT");
    console.error("  while running nothing at all.");
    baselineBad++;
  } else if (!green) {
    console.error(`✖ BASELINE RED: ${testFile} :: ${testName}`);
    baselineBad++;
  }
}
if (baselineBad) {
  console.error(`\n${baselineBad} test(s) are already failing or unmatched. Fix them before trusting any result here.`);
  fs.rmSync(ROOT, { recursive: true, force: true });
  process.exit(1);
}
console.log(`baseline: all ${MUTATIONS.length} named tests green — the mutations below are meaningful\n`);

let missed = 0, skipped = 0;
for (const [name, file, mutate, testFile, testName] of MUTATIONS) {
  restore();
  const before = fs.readFileSync(path.join(ROOT, file), "utf8");
  const after = mutate(before);
  if (after === before) {
    console.log(`SKIP    ${name}\n        (pattern drifted in ${file} — update this mutation)`);
    skipped++;
    continue;
  }
  fs.writeFileSync(path.join(ROOT, file), after);
  const { ran, green } = runNamed(testFile, testName);
  // A mutation that made the named test stop MATCHING proves nothing either — it is a MISSED wearing
  // a pass, for the same reason the baseline check above rejects an unmatched name.
  const caught = ran && !green;
  console.log(`${caught ? "CAUGHT" : "MISSED"}  ${name}`);
  if (!caught) {
    console.log(ran
      ? `        ${testFile} :: "${testName}" stayed GREEN with this regression live`
      : `        ${testFile} :: "${testName}" did not run at all under the mutation`);
    missed++;
  }
}
restore();
fs.rmSync(ROOT, { recursive: true, force: true });

console.log(`\n${missed} MISSED, ${skipped} SKIPPED, ${MUTATIONS.length - missed - skipped} CAUGHT.`);
if (missed || skipped) {
  console.error("\nA regression that reaches the congregation would not be caught. Tighten the named assertions.");
  process.exit(1);
}
