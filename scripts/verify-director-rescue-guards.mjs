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
const TESTS = ["e2e/directorButton.test.mjs", "e2e/directorResume.test.mjs", "e2e/rescueAndDiagnostics.test.mjs", "e2e/pillWording.test.mjs"];

const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), "sv-guardmut-"));
for (const rel of [...FILES, ...TESTS]) {
  fs.mkdirSync(path.join(ROOT, path.dirname(rel)), { recursive: true });
  fs.copyFileSync(path.join(REPO, rel), path.join(ROOT, rel));
}
const orig = Object.fromEntries(FILES.map((f) => [f, fs.readFileSync(path.join(ROOT, f), "utf8")]));
const restore = () => FILES.forEach((f) => fs.writeFileSync(path.join(ROOT, f), orig[f]));

const sub = (a, b) => (s) => s.replace(a, b);

const MUTATIONS = [
  ["the sync PILL no longer gated on the native shell (appears on public signovivo.com)", "web/src/app.js",
   sub("  if (!inShell) {\n    directorModeBadge.classList.add(\"is-hidden\");\n    return;\n  }", "")],
  ["rescue block no longer gated on the shell (panic switches on the public web)", "web/src/app.js",
   sub('rescueWrap.classList.toggle("is-hidden", !inShell)', 'rescueWrap.classList.toggle("is-hidden", false)')],
  ["a rotated DIRECTOR_CODE leaks into the public web bundle", "web/src/app.js",
   sub("const openSongJump = () => {", 'const LEAKED = "918273645";\nconst openSongJump = () => {')],
  ["the explicitTransmitterRef resume guard deleted", "PdfReaderApp.tsx",
   sub("      explicitTransmitterRef.current ||\n", "")],
  ["the becomeDirector-in-flight guard deleted", "PdfReaderApp.tsx",
   sub("      becomeDirectorInFlightRef.current\n", "      false\n")],
  ["the settle window dropped back under the live-director window", "PdfReaderApp.tsx",
   sub("const DIRECTOR_RESUME_SETTLE_MS = 12000;", "const DIRECTOR_RESUME_SETTLE_MS = 3500;")],
  ["the resume success toast fires without checking the role actually changed", "PdfReaderApp.tsx",
   sub('if (roleRef.current === "director" || explicitTransmitterRef.current) {\n                injectEvent({',
       'if (true) {\n                injectEvent({')],
  ["resume no longer follows first — becomeFollower() dropped", "PdfReaderApp.tsx",
   sub(".finally(() => {\n          becomeFollower();\n        });", ".finally(() => {});")],
  ["soft-reset fires BEFORE its confirmation dialog", "PdfReaderApp.tsx",
   sub('case "request-soft-reset":\n          Alert.alert(', 'case "request-soft-reset":\n          onDirectorCode(SOFT_RESET_CODE);\n          Alert.alert(')],
  ["force-baked fires BEFORE its confirmation dialog", "PdfReaderApp.tsx",
   sub('case "request-force-baked":\n          Alert.alert(', 'case "request-force-baked":\n          onDirectorCode(BOOK_FORCE_BAKED_CODE);\n          Alert.alert(')],
  ["request-director bypasses onDirectorCode's takeover confirmation", "PdfReaderApp.tsx",
   sub('case "request-director":\n          onDirectorCode(DIRECTOR_CODE);', 'case "request-director":\n          void becomeDirector(DIRECTOR_CODE); if (0) onDirectorCode(DIRECTOR_CODE);')],
  ["rescue block left EXPANDED between visits", "web/src/app.js",
   sub('if (rescueActions) rescueActions.classList.add("is-hidden");', 'if (rescueActions && false) rescueActions.classList.add("is-hidden");')],
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
