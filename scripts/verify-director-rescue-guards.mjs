#!/usr/bin/env node
/**
 * Proves the director/rescue source-text guards have teeth.
 *
 * Same contract as scripts/verify-smoke-guards.mjs and
 * scripts/verify-sw-page-cache-guards.mjs: mutate a COPY of the sources, rerun the
 * matching e2e files, and require each mutation to turn the suite RED. A guard that
 * merely quotes the current source passes forever and catches nothing — and a passing
 * test looks identical either way.
 *
 * WHY THIS EXISTS. The director/rescue tests assert on SOURCE TEXT — `assert.match(slice, /literal/)`
 * — because the logic lives inside a React effect that cannot be imported without a React Native
 * runtime. That style is uniquely easy to write wrong: a regex that merely quotes the current source
 * passes forever and catches nothing, and a green run looks identical either way.
 *
 * Measured 2026-08-06, the first time this was run: 13 of 13 mutations slipped past all three test
 * files. Every guard those tests claimed to enforce — the director button not appearing on the
 * public web, the panic switches confirming before firing, the crumb buffer being bounded — was
 * decorative. The tests were written the same night as the features and reviewed by the same eyes,
 * which is exactly how this happens.
 *
 * A SKIP is a failure too: it means a mutation's pattern no longer matches the source, so that
 * mutation has silently stopped testing anything.
 *
 * Usage: node scripts/verify-director-rescue-guards.mjs [repoRoot]
 * Exits non-zero if any mutation is MISSED.
 */
import { execSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const REPO = path.resolve(process.argv[2] || process.cwd());
const FILES = ["PdfReaderApp.tsx", "web/src/app.js", "web/src/index.html", "web/src/styles.css", "src/offlineBooks.ts", "ios/SignoVivo/DirectorSyncModule.swift"];
const TESTS = ["e2e/directorButton.test.mjs", "e2e/singleDirector.test.mjs", "e2e/rescueAndDiagnostics.test.mjs", "e2e/pillWording.test.mjs"];

const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "sv-guardmut-"));
for (const rel of [...FILES, ...TESTS]) {
  fs.mkdirSync(path.join(ROOT, path.dirname(rel)), { recursive: true });
  fs.copyFileSync(path.join(REPO, rel), path.join(ROOT, rel));
}
const orig = Object.fromEntries(FILES.map((f) => [f, fs.readFileSync(path.join(ROOT, f), "utf8")]));
const restore = () => FILES.forEach((f) => fs.writeFileSync(path.join(ROOT, f), orig[f]));

const sub = (a, b) => (s) => s.replace(a, b);

// Two mutations were DELETED on 2026-08-06 with the "¿Algo anda mal?" block they targeted — the
// public-web gate and the re-collapse. A mutation whose pattern no longer exists is reported as a
// SKIP, and a SKIP is a failure here for exactly the reason this file exists: it looks like
// coverage while testing nothing. Retargeting them at absent markup would have been worse.
const MUTATIONS = [
  ["the sync PILL no longer gated on the native shell (appears on public signovivo.com)", "web/src/app.js",
   sub("  if (!inShell) {\n    directorModeBadge.classList.add(\"is-hidden\");\n    return;\n  }", "")],
  ["a rotated DIRECTOR_CODE leaks into the public web bundle", "web/src/app.js",
   sub("const openSongJump = () => {", 'const LEAKED = "918273645";\nconst openSongJump = () => {')],
  // ── ONE DIRECTOR (2026-08-15). The four resume/habit mutations that lived here were DELETED with
  //    the machinery they mutated (a mutation with no target reports SKIP, which reads as coverage).
  //    These pin the invariant that replaced it: nothing but a human's confirm mints a director.
  ["the boot path no longer follows — becomeFollower() dropped", "PdfReaderApp.tsx",
   sub(".finally(() => {\n          becomeFollower();\n        });", ".finally(() => {});")],
  ["an AUTOMATIC director claim returns to the boot path", "PdfReaderApp.tsx",
   sub(".finally(() => {\n          becomeFollower();\n        });",
       ".finally(() => {\n          becomeFollower();\n          void becomeDirector(DIRECTOR_CODE);\n        });")],
  ["a persisted director role is silently RESTORED at boot instead of just announced", "PdfReaderApp.tsx",
   sub('if (prev === "director") {\n            // Written back as follower',
       'if (prev === "director") {\n            void becomeDirector(DIRECTOR_CODE);\n            // Written back as follower')],
  ["the ex-director toast fires on every boot forever (lastSyncRole never written back)", "PdfReaderApp.tsx",
   // Text drifted 2026-08-18 ("Estabas dirigiendo." -> "...cuando se cerró el app.") when the notice
   // stopped describing a control and started carrying one. Anchored on the setItem + injectEvent
   // PAIR rather than the copy, so a future wording change cannot silently disarm this again.
   // Anchored on the write ALONE. It was anchored on the write plus the injectEvent that follows it,
   // which broke the moment a comment was added between them — a mutation that cannot apply is a
   // guard proving nothing, and it fails silently as a SKIP. The write is what the guard is about.
   sub('            AsyncStorage.setItem(STORAGE_KEYS.lastSyncRole, "follower").catch(() => {});\n', '')],
  ["the demoted director is not told another device took the seat", "PdfReaderApp.tsx",
   sub('              text: "Otro dispositivo tomó la dirección del coro. Este dispositivo ahora sigue.",',
       '              text: "",')],
  ["the rediscovery kick after becoming director dropped — two directors wait out a browse cycle", "PdfReaderApp.tsx",
   // Renamed 2026-08-18: refreshNearbyDiscovery -> refreshDirectorBrowse, because the old call
   // destroyed the ADVERTISER as its first act — at the exact moment every follower was inviting it.
   sub('        if (syncAvailable) refreshDirectorBrowse().catch(() => {});\n        breadcrumb("director");',
       '        breadcrumb("director");')],
  ["the Swift tiebreak inverted — the OLDER director demotes and the newest never wins", "ios/SignoVivo/DirectorSyncModule.swift",
   sub("    if otherToken > currentDirectorToken {", "    if otherToken < currentDirectorToken {")],
  ["soft-reset fires BEFORE its confirmation dialog", "PdfReaderApp.tsx",
   sub('case "request-soft-reset":\n          Alert.alert(', 'case "request-soft-reset":\n          onDirectorCode(SOFT_RESET_CODE);\n          Alert.alert(')],
  ["force-baked fires BEFORE its confirmation dialog", "PdfReaderApp.tsx",
   sub('case "request-force-baked":\n          Alert.alert(', 'case "request-force-baked":\n          onDirectorCode(BOOK_FORCE_BAKED_CODE);\n          Alert.alert(')],
  ["request-director bypasses onDirectorCode's takeover confirmation", "PdfReaderApp.tsx",
   // Threaded through a knownPage extraction 2026-08-18 (aa68c9e) so the web's true page rides
   // with the tap instead of trusting the async-fed currentPageRef mirror. Anchored on the
   // onDirectorCode call itself, not the whole case block, so future changes to the knownPage
   // extraction above it don't silently disarm this mutation as a SKIP.
   sub('onDirectorCode(DIRECTOR_CODE, knownPage);', 'void becomeDirector(DIRECTOR_CODE, knownPage); if (0) onDirectorCode(DIRECTOR_CODE, knownPage);')],
  ["crumb merge order swapped — the NEW session's crumbs get dropped", "PdfReaderApp.tsx",
   sub("const merged = [...prev, `${new Date().toISOString()} ── app start ──`, ...breadcrumbsRef.current];",
       "const merged = [...breadcrumbsRef.current, `${new Date().toISOString()} ── app start ──`, ...prev];")],
  ["BREADCRUMB_LIMIT set to 1 — one crumb survives, the exact bug the buffer replaced", "PdfReaderApp.tsx",
   sub("const BREADCRUMB_LIMIT = 200;", "const BREADCRUMB_LIMIT = 1;")],
  ["ring-buffer trim neutered", "PdfReaderApp.tsx",
   sub("next.splice(0, next.length - BREADCRUMB_LIMIT)", "next.splice(0, 0) || next.splice(0, next.length - BREADCRUMB_LIMIT)")],
  ["-webkit-user-select dropped (the property iOS WKWebView actually honours)", "web/src/styles.css",
   sub("  -webkit-user-select: text;\n", "")],
];

// BASELINE FIRST. If the pristine suite is red, every mutation trivially "fails" and this script
// reports 0 MISSED while proving nothing. Measured: that is exactly what happened on the first run
// after the assertions were rewritten, and it looked like total success.
try {
  execSync(`node --test ${TESTS.join(" ")}`, { cwd: ROOT, stdio: "pipe" });
  console.log("baseline: suite green — mutations below are meaningful\n");
} catch {
  console.error("✖ BASELINE IS RED. Fix the suite before trusting any result here:\n");
  try { execSync(`node --test ${TESTS.join(" ")}`, { cwd: ROOT, stdio: "inherit" }); } catch {}
  process.exit(1);
}

let missed = 0, skipped = 0;
for (const [name, file, fn] of MUTATIONS) {
  restore();
  const before = fs.readFileSync(path.join(ROOT, file), "utf8");
  const after = fn(before);
  if (after === before) { console.log(`SKIP    ${name}  (pattern drifted — update this mutation)`); skipped++; continue; }
  fs.writeFileSync(path.join(ROOT, file), after);
  let caught = false;
  try { execSync(`node --test ${TESTS.join(" ")}`, { cwd: ROOT, stdio: "pipe" }); } catch { caught = true; }
  console.log(`${caught ? "CAUGHT" : "MISSED "} ${name}`);
  if (!caught) missed++;
}
restore();
fs.rmSync(ROOT, { recursive: true, force: true });
console.log(`\n${missed} MISSED, ${skipped} SKIPPED, ${MUTATIONS.length - missed - skipped} CAUGHT.`);
if (missed || skipped) { console.error("\nGuards without teeth — tighten the assertions above."); process.exit(1); }
