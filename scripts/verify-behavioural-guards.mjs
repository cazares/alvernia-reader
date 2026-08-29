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
 * Exits non-zero if any mutation is MISSED or SKIPPED.
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

const orig = Object.fromEntries(FILES.map((f) => [f, fs.readFileSync(path.join(ROOT, f), "utf8")]));
const restore = () => FILES.forEach((f) => fs.writeFileSync(path.join(ROOT, f), orig[f]));

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
function runNamed(testFile, testName) {
  try {
    execFileSync("node", ["--test", "--test-name-pattern", esc(testName), testFile],
      { cwd: ROOT, stdio: "pipe" });
    return true;   // green
  } catch {
    return false;  // red
  }
}

// BASELINE FIRST. A red baseline makes every mutation look "caught" and reports a perfect score
// while proving nothing — which is exactly what happened the first time the sibling harness ran.
let baselineBad = 0;
for (const [, , , testFile, testName] of MUTATIONS) {
  if (!runNamed(testFile, testName)) {
    console.error(`✖ BASELINE RED: ${testFile} :: ${testName}`);
    baselineBad++;
  }
}
if (baselineBad) {
  console.error(`\n${baselineBad} test(s) are already failing. Fix them before trusting any result here.`);
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
  const caught = !runNamed(testFile, testName);
  console.log(`${caught ? "CAUGHT" : "MISSED"}  ${name}`);
  if (!caught) {
    console.log(`        ${testFile} :: "${testName}" stayed GREEN with this regression live`);
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
