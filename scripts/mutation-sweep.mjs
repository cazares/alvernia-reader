#!/usr/bin/env node
/**
 * mutation-sweep — measure whether a test file can actually FAIL.
 *
 * A green test run and a useless test look identical from the outside. The only way to tell them
 * apart is to break the source on purpose and check that the suite notices. This repo already does
 * that by hand in seven `scripts/verify-*-guards.mjs` files, each carrying a bespoke list of
 * hand-written mutations. Those lists are valuable — they encode "this specific invariant matters" —
 * but they only ever test what somebody remembered to write down, and a mutation whose pattern
 * drifts out of the source reports SKIP, which reads exactly like coverage.
 *
 * This does the mechanical half automatically: it derives mutations from the source itself, so
 * coverage cannot silently drift, and it reports every mutant the tests did NOT catch.
 *
 * WHY IT WAS WORTH BUILDING. Measured on 2026-08-06 the first time the hand-written harness ran:
 * 13 of 13 mutations slipped past all three test files — every guard those tests claimed to enforce
 * was decorative. Measured again during the build-475 campaign: three freshly written tests could
 * not fail at all (a backoff ladder re-implemented inside the test; a string-offset check mistaken
 * for containment; a slice whose end marker had been deleted, so the "window" ran to EOF and any
 * match anywhere satisfied it). Both times the tests were written the same night as the feature and
 * read by the same eyes, which is how this always happens.
 *
 *   node scripts/mutation-sweep.mjs --source <file> --test <file> [--test <file>…] [options]
 *
 * Options:
 *   --source <path>     File to mutate. Repeatable. (.js .mjs .cjs .ts .tsx .swift .m .html .css)
 *   --test <path>       Test file to run. Repeatable. Passed to `node --test`.
 *   --cmd "<shell>"     Run this instead of `node --test <tests>` (for non-node suites).
 *   --lines A-B         Only mutate within this line range of each source. Repeatable.
 *   --only <regex>      Only run mutants whose description matches.
 *   --limit N           Cap the number of mutants (deterministic prefix after shuffling by hash).
 *   --jobs N            Parallel mirrors. Default: min(8, cpus-1).
 *   --timeout MS        Per-mutant test timeout. Default 120000.
 *   --json              Emit machine-readable JSON instead of the human report.
 *   --fail-under PCT    Exit non-zero if the caught percentage is below PCT. Default: report only.
 *   --quiet             Only print survivors and the summary.
 *
 * Exit codes: 0 = ran cleanly, 1 = baseline red / bad usage / below --fail-under.
 *
 * SURVIVORS ARE NOT ALL BUGS. Mutation testing produces *equivalent mutants* — changes that alter
 * the text without altering observable behaviour (a clamp that was already unreachable, a bound
 * nothing crosses in practice, a log string). A survivor is a QUESTION: "would a real regression
 * here be caught?" Triage each one. The number to drive down is survivors on load-bearing lines,
 * not the raw percentage.
 */
import { execSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// ── argv ────────────────────────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const opt = { sources: [], tests: [], lines: [], jobs: 0, limit: 0, timeout: 120000 };
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  const next = () => argv[++i];
  if (a === "--source") opt.sources.push(next());
  else if (a === "--test") opt.tests.push(next());
  else if (a === "--cmd") opt.cmd = next();
  else if (a === "--lines") opt.lines.push(next());
  else if (a === "--only") opt.only = new RegExp(next(), "i");
  else if (a === "--limit") opt.limit = Number(next());
  else if (a === "--jobs") opt.jobs = Number(next());
  else if (a === "--timeout") opt.timeout = Number(next());
  else if (a === "--fail-under") opt.failUnder = Number(next());
  else if (a === "--json") opt.json = true;
  else if (a === "--quiet") opt.quiet = true;
  else if (a === "--help" || a === "-h") { console.log(fs.readFileSync(new URL(import.meta.url), "utf8").split("*/")[0]); process.exit(0); }
  else { console.error(`unknown argument: ${a}`); process.exit(1); }
}
if (!opt.sources.length || (!opt.tests.length && !opt.cmd)) {
  console.error("usage: mutation-sweep.mjs --source <file> [--source …] --test <file> [--test …]");
  console.error("       (or --cmd \"<shell command>\" instead of --test)");
  process.exit(1);
}

const REPO = execSync("git rev-parse --show-toplevel", { encoding: "utf8" }).trim();
const rel = (p) => path.relative(REPO, path.resolve(p));
const SOURCES = opt.sources.map(rel);
const TESTS = opt.tests.map(rel);
const RUN_CMD = opt.cmd || `node --test ${TESTS.map((t) => JSON.stringify(t)).join(" ")}`;
const JOBS = Math.max(1, opt.jobs || Math.min(8, Math.max(1, os.cpus().length - 1)));

for (const s of SOURCES) {
  if (!fs.existsSync(path.join(REPO, s))) { console.error(`no such source: ${s}`); process.exit(1); }
}

// ── lexer: which byte offsets are CODE (not comment, not string) ────────────────────────────────
//
// This matters more here than in most codebases. These files are majority comment by line count —
// publishSeq.js is 60 lines of prose around 20 of logic — and a mutation inside a comment is
// guaranteed to survive while proving nothing. Scanning them as code would bury every real survivor
// under hundreds of noise entries and make the percentage meaningless.
//
// Handles // and /* */ comments, '…' "…" `…` strings, Swift """…""" strings, and JS regex literals.
// Regex detection uses the standard previous-significant-token heuristic: a `/` opens a regex unless
// the last thing before it could end an expression (identifier, number, `)`, `]`, `}`). Getting this
// wrong is not cosmetic — a regex like /["']/ read as code would flip the string state and desync
// every offset after it in the file.
function codeMask(src, ext) {
  const swift = ext === ".swift";
  const mask = new Uint8Array(src.length); // 1 = code
  let i = 0;
  let lastSig = ""; // last significant code char, for the regex heuristic
  const KEYWORD_BEFORE_REGEX = /(?:^|[^\w$])(return|typeof|instanceof|in|of|new|delete|void|throw|case|do|else|yield|await)$/;
  while (i < src.length) {
    const c = src[i], c2 = src[i + 1];
    // line comment
    if (c === "/" && c2 === "/") { while (i < src.length && src[i] !== "\n") i++; continue; }
    // block comment (Swift's nest; JS's do not — nesting harmlessly over-consumes nothing in JS
    // because `/*` inside a JS block comment is not special and the first `*/` still closes it)
    if (c === "/" && c2 === "*") {
      let depth = 1; i += 2;
      while (i < src.length && depth > 0) {
        if (swift && src[i] === "/" && src[i + 1] === "*") { depth++; i += 2; continue; }
        if (src[i] === "*" && src[i + 1] === "/") { depth--; i += 2; continue; }
        i++;
      }
      continue;
    }
    // Swift multiline string
    if (swift && src.startsWith('"""', i)) {
      i += 3;
      while (i < src.length && !src.startsWith('"""', i)) { if (src[i] === "\\") i++; i++; }
      i += 3; lastSig = '"'; continue;
    }
    // strings
    if (c === '"' || c === "'" || c === "`") {
      const q = c; i++;
      while (i < src.length && src[i] !== q) { if (src[i] === "\\") i++; i++; }
      i++; lastSig = q; continue;
    }
    // regex literal (JS family only)
    if (!swift && c === "/") {
      const before = src.slice(0, i);
      const trimmed = before.replace(/\s+$/, "");
      const prev = trimmed[trimmed.length - 1] || "";
      const startsRegex = !prev || !/[\w$)\]]/.test(prev) || KEYWORD_BEFORE_REGEX.test(trimmed);
      if (startsRegex) {
        i++;
        let inClass = false;
        while (i < src.length) {
          const d = src[i];
          if (d === "\\") { i += 2; continue; }
          if (d === "[") inClass = true;
          else if (d === "]") inClass = false;
          else if (d === "/" && !inClass) { i++; break; }
          else if (d === "\n") break; // unterminated — bail rather than eat the file
          i++;
        }
        while (i < src.length && /[gimsuyvd]/.test(src[i])) i++;
        lastSig = "/"; continue;
      }
    }
    mask[i] = 1;
    if (!/\s/.test(c)) lastSig = c;
    i++;
  }
  return mask;
}

const lineOf = (src, idx) => {
  let n = 1;
  for (let i = 0; i < idx; i++) if (src[i] === "\n") n++;
  return n;
};

// ── mutation operators ──────────────────────────────────────────────────────────────────────────
//
// EVERY OPERATOR HERE IS SYNTAX-PRESERVING BY CONSTRUCTION. Flipping `>` to `>=` cannot make a file
// stop parsing, so a mutant is never "caught" merely because the source became invalid — which would
// be a false pass that flatters the tests. The one operator that can break syntax (line deletion) is
// restricted to whole single statements and, for JS, validated with `node --check` before it counts.
//
// Ordered by yield. Boundary and connective flips find real off-by-ones and wrong-operator bugs;
// literal flips find inverted guards; identifier swaps (max/min, first/last) find the clamp-direction
// class of bug that this repo has shipped more than once.
const TOKEN_MUTATIONS = [
  // Relational boundaries — the off-by-one class. `seq > nowMs` vs `seq >= nowMs` decides whether a
  // director exactly on the server's clock is clamped.
  [">=", ">"], [">=", "<"], ["<=", "<"], ["<=", ">"], [">", ">="], [">", "<"], ["<", "<="], ["<", ">"],
  // Equality — inverted guards.
  ["===", "!=="], ["!==", "==="], ["==", "!="], ["!=", "=="],
  // Connectives — the "all three clauses go false together" class that produced a follower hanging
  // up on its own director.
  ["&&", "||"], ["||", "&&"],
  // Assignment/compound arithmetic direction.
  ["+=", "-="], ["-=", "+="],
];

const WORD_MUTATIONS = [
  ["true", "false"], ["false", "true"],
  ["Math.max", "Math.min"], ["Math.min", "Math.max"],
  ["Math.floor", "Math.ceil"], ["Math.ceil", "Math.floor"],
  ["isFinite", "isNaN"],
  ["first", "last"], ["last", "first"],
  ["push", "unshift"],
];

function generate(src, ext, ranges) {
  const mask = codeMask(src, ext);
  const inRange = (line) => !ranges.length || ranges.some(([a, b]) => line >= a && line <= b);
  const out = [];
  const seen = new Set();
  const add = (start, end, replacement, kind) => {
    const line = lineOf(src, start);
    if (!inRange(line)) return;
    const before = src.slice(start, end);
    if (before === replacement) return;
    const key = `${start}:${end}:${replacement}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ start, end, line, before, after: replacement, kind, text: src.slice(0, start) + replacement + src.slice(end) });
  };

  // Token operators. Longest-match first so `>=` is never mutated as `>`.
  const byLen = [...TOKEN_MUTATIONS].sort((a, b) => b[0].length - a[0].length);
  for (let i = 0; i < src.length; i++) {
    if (!mask[i]) continue;
    for (const [from, to] of byLen) {
      if (!src.startsWith(from, i)) continue;
      // Don't treat `=>`, `<=` inside `<<=`, or the `=` of `>=` in `>>=` as a comparison.
      if (from === ">" && src[i + 1] === "=") break;
      if (from === "<" && src[i + 1] === "=") break;
      if (from === "=" ) break;
      if ((from === ">" || from === "<") && (src[i - 1] === "=" || src[i - 1] === "<" || src[i - 1] === ">")) break;
      if (from === ">" && src[i - 1] === "=") break; // the `>` of `=>`
      add(i, i + from.length, to, "operator");
      break; // longest match wins; don't also emit the shorter overlapping one
    }
  }

  // Whole-word identifier/literal swaps.
  for (const [from, to] of WORD_MUTATIONS) {
    const re = new RegExp(`(^|[^\\w$.])${from.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w$])`, "g");
    let m;
    while ((m = re.exec(src))) {
      const start = m.index + m[1].length;
      if (!mask[start]) continue;
      // `Math.max` needs the dot to be part of the match; the guard above excludes a leading `.`
      // so `foo.max` is not mutated, but `Math.max` must be. Re-allow when the word itself has one.
      add(start, start + from.length, to, "identifier");
    }
  }

  // Numeric literal perturbation. Off-by-one on a bound, and 0/1 flips on a sentinel. Skips the
  // digits of an identifier and anything inside a longer number.
  const numRe = /(^|[^\w$.])(\d+)(?![\w.])/g;
  let nm;
  while ((nm = numRe.exec(src))) {
    const start = nm.index + nm[1].length;
    if (!mask[start]) continue;
    const lit = nm[2];
    const n = Number(lit);
    if (!Number.isFinite(n)) continue;
    add(start, start + lit.length, String(n + 1), "number");
    if (n !== 0) add(start, start + lit.length, "0", "number");
  }

  // Unary `!` removal — inverts a guard without touching its shape.
  for (let i = 0; i < src.length - 1; i++) {
    if (!mask[i] || src[i] !== "!") continue;
    if (src[i + 1] === "=" || src[i - 1] === "=" || src[i - 1] === "!") continue;
    if (!/[\w$([]/.test(src[i + 1])) continue;
    add(i, i + 1, "", "negation");
  }

  // Statement deletion — "what if this guard simply weren't here". Restricted to a line that is a
  // complete statement on its own (balanced brackets, ends in `;` or `}`) so the file still parses.
  const lines = src.split("\n");
  let off = 0;
  for (const raw of lines) {
    const start = off;
    off += raw.length + 1;
    const t = raw.trim();
    if (!t || t.startsWith("//") || t.startsWith("*") || t.startsWith("/*")) continue;
    if (!mask[start + raw.length - Math.max(1, t.length)] && !mask[start + raw.indexOf(t)]) continue;
    if (!/[;}]$/.test(t)) continue;
    const bal = (s, o, c) => [...s].filter((ch) => ch === o).length === [...s].filter((ch) => ch === c).length;
    if (!bal(t, "(", ")") || !bal(t, "{", "}") || !bal(t, "[", "]")) continue;
    // Never delete a declaration — the next line would reference a name that no longer exists, so
    // the mutant dies of a ReferenceError rather than of the behaviour change we meant to test.
    if (/^(export\s+)?(const|let|var|function|class|import|type|interface|enum|struct|func|private|public|internal|@)\b/.test(t)) continue;
    if (/^(}|\)|\];?|\}\);?)$/.test(t)) continue;
    add(start + raw.indexOf(t), start + raw.indexOf(t) + t.length, "", "delete-statement");
  }

  return out;
}

// ── mirror ──────────────────────────────────────────────────────────────────────────────────────
// A COPY, NEVER THE REAL TREE. `cp` once refused a restore in this repo as "identical (not copied)"
// and left a production file mutated; the next test run was the only thing that caught it. Every
// mutation here happens inside a mktemp mirror that is deleted at the end, so the worktree is never
// written to at all and a crash mid-run leaves nothing behind to restore.
const TRACKED = execSync("git ls-files -z", { cwd: REPO, encoding: "utf8" }).split("\0").filter(Boolean);
function makeMirror(tag) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `sv-mutsweep-${tag}-`));
  for (const f of TRACKED) {
    const from = path.join(REPO, f);
    // Not every `git ls-files` entry is a regular file. This repo has a GITLINK committed at
    // `.claude/worktrees/determined-carson-439745` (mode 160000, from the build-265 era) — it lists
    // like a file and is a directory on disk, so copyFileSync throws ENOTSUP. That same entry is
    // what breaks the Cloudflare Pages deploy at Checkout ("No url found for submodule path"), so it
    // may vanish one day; skipping anything that is not a regular file is right either way.
    let st;
    try { st = fs.lstatSync(from); } catch { continue; } // deleted-but-tracked
    if (!st.isFile()) continue;
    const to = path.join(dir, f);
    fs.mkdirSync(path.dirname(to), { recursive: true });
    fs.copyFileSync(from, to);
  }
  // Uncommitted work counts: the point is to test the tests AS THEY ARE NOW, not as they were at
  // HEAD. Overlay anything modified or untracked-but-relevant on top of the tracked copy.
  const dirty = execSync("git status --porcelain -z", { cwd: REPO, encoding: "utf8" })
    .split("\0").filter(Boolean)
    .map((l) => l.slice(3))
    .filter((f) => f && !f.endsWith("/"));
  for (const f of dirty) {
    const from = path.join(REPO, f);
    let st;
    try { st = fs.lstatSync(from); } catch { continue; }
    if (!st.isFile()) continue;
    const to = path.join(dir, f);
    fs.mkdirSync(path.dirname(to), { recursive: true });
    fs.copyFileSync(from, to);
  }
  return dir;
}

function runTests(dir, timeout) {
  return new Promise((resolve) => {
    const child = spawn("sh", ["-c", RUN_CMD], { cwd: dir, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    const timer = setTimeout(() => { try { child.kill("SIGKILL"); } catch { /* already gone */ } }, timeout);
    child.stdout.on("data", (d) => { out += d; });
    child.stderr.on("data", (d) => { out += d; });
    child.on("close", (code) => { clearTimeout(timer); resolve({ ok: code === 0, out }); });
    child.on("error", () => { clearTimeout(timer); resolve({ ok: false, out }); });
  });
}

// ── run ─────────────────────────────────────────────────────────────────────────────────────────
const log = (...a) => { if (!opt.json) console.log(...a); };
const ranges = opt.lines.map((r) => r.split("-").map(Number));

const pristine = Object.fromEntries(SOURCES.map((s) => [s, fs.readFileSync(path.join(REPO, s), "utf8")]));
let mutants = [];
for (const s of SOURCES) {
  const ext = path.extname(s);
  for (const m of generate(pristine[s], ext, ranges)) {
    const desc = `${s}:${m.line}  ${JSON.stringify(m.before)} → ${JSON.stringify(m.after)}`;
    if (opt.only && !opt.only.test(desc)) continue;
    mutants.push({ ...m, source: s, desc });
  }
}
// Deterministic order: by file, then line, then offset. A stable order makes two runs comparable,
// which matters when --limit is in play.
mutants.sort((a, b) => a.source.localeCompare(b.source) || a.start - b.start || a.after.localeCompare(b.after));
if (opt.limit > 0) mutants = mutants.slice(0, opt.limit);

const baseDir = makeMirror("base");
// BASELINE FIRST. If the pristine suite is already red, every mutant "fails" too and the sweep
// reports a perfect score while proving nothing. That exact false success happened here on
// 2026-08-06, and it is indistinguishable from real coverage in the output.
const baseline = await runTests(baseDir, opt.timeout);
if (!baseline.ok) {
  console.error("✖ BASELINE IS RED — every mutant would look 'caught'. Fix the suite first.\n");
  console.error(baseline.out.split("\n").slice(-40).join("\n"));
  fs.rmSync(baseDir, { recursive: true, force: true });
  process.exit(1);
}
const baseTests = (baseline.out.match(/^# pass (\d+)/m) || [])[1];
log(`baseline: green${baseTests ? ` (${baseTests} passing)` : ""} — ${mutants.length} mutants over ${SOURCES.length} source file(s), ${JOBS} jobs\n`);

// `node --check` is only meaningful for the JS family; a deleted statement is the only operator that
// can produce an unparseable file, and a mutant that merely fails to parse must not be counted as
// caught — that would credit the tests for the compiler's work.
const checkable = (s) => [".js", ".mjs", ".cjs"].includes(path.extname(s));

const results = [];
const queue = [...mutants];
const workers = Array.from({ length: JOBS }, (_, w) => (async () => {
  const dir = w === 0 ? baseDir : makeMirror(`w${w}`);
  while (queue.length) {
    const m = queue.shift();
    if (!m) break;
    const target = path.join(dir, m.source);
    fs.writeFileSync(target, m.text);
    let verdict;
    if (m.kind === "delete-statement" && checkable(m.source)) {
      try { execSync(`node --check ${JSON.stringify(target)}`, { stdio: "pipe" }); }
      catch { verdict = "unparseable"; }
    }
    if (!verdict) {
      const r = await runTests(dir, opt.timeout);
      verdict = r.ok ? "SURVIVED" : "caught";
    }
    fs.writeFileSync(target, pristine[m.source]);
    results.push({ ...m, verdict });
    if (!opt.quiet && !opt.json && verdict === "SURVIVED") console.log(`SURVIVED  ${m.desc}`);
    else if (!opt.quiet && !opt.json) process.stdout.write(".");
  }
  if (w !== 0) fs.rmSync(dir, { recursive: true, force: true });
})());
await Promise.all(workers);
fs.rmSync(baseDir, { recursive: true, force: true });

const survived = results.filter((r) => r.verdict === "SURVIVED");
const caught = results.filter((r) => r.verdict === "caught");
const unparseable = results.filter((r) => r.verdict === "unparseable");
const scored = survived.length + caught.length;
const pct = scored ? Math.round((caught.length / scored) * 100) : 0;

if (opt.json) {
  console.log(JSON.stringify({
    sources: SOURCES, tests: TESTS, cmd: RUN_CMD,
    total: results.length, caught: caught.length, survived: survived.length, unparseable: unparseable.length,
    caughtPct: pct,
    survivors: survived.map((s) => ({ source: s.source, line: s.line, before: s.before, after: s.after, kind: s.kind })),
  }, null, 2));
} else {
  console.log(`\n\n${caught.length} caught, ${survived.length} SURVIVED${unparseable.length ? `, ${unparseable.length} unparseable (not scored)` : ""} — ${pct}% of ${scored} scored mutants caught.`);
  if (survived.length) {
    // Grouped by line so a single under-tested function reads as one problem, not twelve.
    const byLine = new Map();
    for (const s of survived) {
      const k = `${s.source}:${s.line}`;
      if (!byLine.has(k)) byLine.set(k, []);
      byLine.get(k).push(`${JSON.stringify(s.before)}→${JSON.stringify(s.after)}`);
    }
    console.log(`\nSurvivors by line — each is a QUESTION, not automatically a bug (see the header on`);
    console.log(`equivalent mutants). Ask of each: would a real regression here be caught?\n`);
    for (const [k, v] of [...byLine.entries()].sort()) console.log(`  ${k.padEnd(48)} ${v.join("  ")}`);
  }
}
if (opt.failUnder != null && pct < opt.failUnder) {
  console.error(`\n✖ ${pct}% caught is below --fail-under ${opt.failUnder}`);
  process.exit(1);
}
