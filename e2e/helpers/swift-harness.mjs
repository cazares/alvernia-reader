// swift-harness — compile and RUN the real Swift decision logic from ios/SignoVivo/*.swift.
//
// WHY. Nearly every assertion this repo makes about its Swift is a regex over the source. Measured
// on 2026-08-29 by applying real regressions and re-running the suite, those regexes let all of
// this through, silently:
//
//   • the four-way "is this peer my director?" predicate flipped from `||` to `&&` — the round-5 fix
//     of the previous campaign, the one that stops a follower hanging up on its own director
//   • a late joiner's catch-up snapshot downgraded from .reliable to .unreliable
//   • parseInboundPayload rewritten to force-unwrap and crash on malformed JSON
//   • the 50-character MCPeerID display-name cap removed at the site that actually feeds MCPeerID
//
// Each of those is a regex matching a string that still exists somewhere else in a 2,800-line file.
// The category of defect is not "somebody wrote a sloppy regex"; it is that a regex cannot see
// BEHAVIOUR, and four different people writing four more regexes will produce the same result.
//
// THE APPROACH. This box compiles and runs Swift (verified: `xcrun swiftc -O t.swift -o t && ./t`).
// So instead of describing the Swift, this EXTRACTS THE REAL FUNCTION BODIES VERBATIM, compiles
// them against a minimal shim that stands in for MultipeerConnectivity, and runs them.
//
// Nothing is transliterated. The bytes between `private func sessionForAdmitting` and its closing
// brace are the bytes that ship. Change the logic and the compiled behaviour changes with it; change
// its shape so the extractor can no longer find it and the test FAILS rather than quietly stopping.
//
// WHAT THIS CANNOT DO. It runs pure decision logic, not the framework. Anything that depends on
// real MultipeerConnectivity or CoreBluetooth delivery, on DispatchQueue timing, or on a real radio
// is out of reach here and stays covered by source anchors and by hardware runs. The shim's job is
// to be the smallest possible stand-in — if it ever starts encoding a decision, the decision has
// moved out of the code under test and into the harness, which is the failure this file exists to
// avoid.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

export const readSwift = (rel) => fs.readFileSync(path.join(REPO, rel), "utf8");

/** Walk forward from `openIdx` (the index of a `{`) to its matching `}`, skipping over comments and
 *  string literals. A naive depth counter gets this wrong on the first brace inside a comment or a
 *  `"\(interpolation)"`, and silently returns a truncated body — which would compile to something
 *  that is not the code under test. */
function matchBrace(src, openIdx) {
  let depth = 0;
  let i = openIdx;
  while (i < src.length) {
    const c = src[i], c2 = src[i + 1];
    if (c === "/" && c2 === "/") { while (i < src.length && src[i] !== "\n") i++; continue; }
    if (c === "/" && c2 === "*") {
      let d = 1; i += 2;                       // Swift block comments nest
      while (i < src.length && d > 0) {
        if (src[i] === "/" && src[i + 1] === "*") { d++; i += 2; continue; }
        if (src[i] === "*" && src[i + 1] === "/") { d--; i += 2; continue; }
        i++;
      }
      continue;
    }
    if (src.startsWith('"""', i)) {
      i += 3;
      while (i < src.length && !src.startsWith('"""', i)) { if (src[i] === "\\") i++; i++; }
      i += 3; continue;
    }
    if (c === '"') {
      i++;
      while (i < src.length && src[i] !== '"') { if (src[i] === "\\") i++; i++; }
      i++; continue;
    }
    if (c === "{") depth++;
    else if (c === "}") { depth--; if (depth === 0) return i; }
    i++;
  }
  return -1;
}

/**
 * Extract one whole declaration — signature and body — by an anchor that appears in its signature.
 *
 * Throws rather than returning empty. An extractor that silently yields "" would compile a program
 * missing the code under test, and every assertion about it would then be meaningless in exactly
 * the way this whole harness exists to prevent.
 */
export function extractDecl(src, anchor, { file = "the Swift source" } = {}) {
  const at = src.indexOf(anchor);
  if (at < 0) throw new Error(`swift-harness: could not find ${JSON.stringify(anchor)} in ${file} — ` +
    "the declaration has been renamed or reshaped, so this test is no longer running the code it names");
  const brace = src.indexOf("{", at);
  if (brace < 0) throw new Error(`swift-harness: no body found after ${JSON.stringify(anchor)} in ${file}`);
  const close = matchBrace(src, brace);
  if (close < 0) throw new Error(`swift-harness: unbalanced braces after ${JSON.stringify(anchor)} in ${file}`);
  return src.slice(at, close + 1);
}

/**
 * Extract a multi-line EXPRESSION — an assignment whose right-hand side continues onto following
 * lines. Used for the ones that are not functions: the four-way director predicate and the
 * transport backoff ladder, both of which live inline inside large delegate methods.
 *
 * Bounded STRUCTURALLY, by continuation-operator lines (`||`, `&&`, `?`, `:`), never by a character
 * count. Five separate test files in this repo broke by slicing a fixed number of characters and
 * silently running to EOF, and the whole point of extracting an expression is that the operators
 * come with it — an extractor that dropped a `||` would compile something the author never wrote.
 */
export function extractExpression(src, anchor, { file = "the Swift source", minContinuations = 1 } = {}) {
  const at = src.indexOf(anchor);
  if (at < 0) throw new Error(`swift-harness: could not find ${JSON.stringify(anchor)} in ${file} — ` +
    "the expression has moved or been reshaped, so this test is no longer running the code it names");
  const lineStart = src.lastIndexOf("\n", at) + 1;
  const lines = src.slice(lineStart).split("\n");
  const out = [lines[0]];
  let continuations = 0;
  for (let i = 1; i < lines.length; i++) {
    const t = lines[i].trim();
    if (!/^(\|\||&&|\?|:|\+|-)/.test(t)) break;
    out.push(lines[i]);
    continuations++;
  }
  if (continuations < minContinuations) {
    throw new Error(`swift-harness: ${JSON.stringify(anchor)} in ${file} has ${continuations} continuation ` +
      `line(s), expected at least ${minContinuations} — the expression has been rewritten, and this test ` +
      "would otherwise silently check a fragment of it");
  }
  return out.join("\n");
}

/** Extract a `static let NAME = <value>` / `let NAME = <value>` initialiser, so a constant is read
 *  from the source rather than restated here. A constant copied into a test is a constant that can
 *  drift, and this repo has shipped that too. */
export function extractConst(src, name, { file = "the Swift source" } = {}) {
  const re = new RegExp(`(?:private\\s+)?(?:static\\s+)?let\\s+${name}\\s*(?::[^=]+)?=\\s*([^\\n]+)`);
  const m = src.match(re);
  if (!m) throw new Error(`swift-harness: could not find the constant ${name} in ${file}`);
  return m[1].trim();
}

/**
 * Compile and run a Swift program; return its stdout.
 *
 * A COMPILE FAILURE IS A TEST FAILURE, never a skip. If the extracted code stops compiling against
 * the shim, either the code changed shape or the shim drifted — and both mean this file is no
 * longer testing what it claims. A harness that skips on a build error is indistinguishable from a
 * harness that passes.
 */
export function runSwift(source, { label = "swift-rules" } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `sv-${label}-`));
  const src = path.join(dir, "main.swift");
  const bin = path.join(dir, "main");
  fs.writeFileSync(src, source);
  try {
    try {
      execFileSync("xcrun", ["swiftc", "-O", "-swift-version", "5", src, "-o", bin], { stdio: "pipe" });
    } catch (e) {
      const out = `${e.stdout ?? ""}${e.stderr ?? ""}`;
      // Point at the emitted file so the failure is debuggable: the interesting line numbers are in
      // the generated program, not in any file a person wrote.
      const kept = path.join(os.tmpdir(), `sv-${label}-FAILED.swift`);
      fs.writeFileSync(kept, source);
      throw new Error(`the extracted Swift did not compile (source kept at ${kept}):\n${out.slice(0, 4000)}`);
    }
    try {
      return execFileSync(bin, { encoding: "utf8" });
    } catch (e) {
      // A Swift trap prints its reason to stderr and exits non-zero. Surfacing only the exit status
      // turns "index out of range on line 12 of the generated program" into "Command failed", which
      // is the difference between a two-minute fix and an afternoon.
      const out = `${e.stdout ?? ""}${e.stderr ?? ""}`.trim();
      const kept = path.join(os.tmpdir(), `sv-${label}-CRASHED.swift`);
      fs.writeFileSync(kept, source);
      throw new Error(`the extracted Swift compiled but crashed (source kept at ${kept}):\n${out.slice(0, 4000) || "(no output)"}`);
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/** Is a Swift toolchain available? Used only to give a clear message on a box without Xcode — never
 *  to skip silently. */
export function swiftAvailable() {
  try { execFileSync("xcrun", ["swiftc", "--version"], { stdio: "pipe" }); return true; }
  catch { return false; }
}

// ── the MultipeerConnectivity shim ──────────────────────────────────────────────────────────────
//
// The smallest thing the extracted admission code can compile against. It provides IDENTITY
// (MCSession is a class, so `===` means what it means in production), MEMBERSHIP
// (connectedPeers), and CONSTRUCTION. It decides nothing: there is no logic here to get right,
// which is the property that keeps this from becoming another re-implementation.
export const MPC_SHIM = `
import Foundation

final class MCPeerID: Hashable {
  let displayName: String
  init(displayName: String) { self.displayName = displayName }
  static func == (a: MCPeerID, b: MCPeerID) -> Bool { a === b }
  func hash(into h: inout Hasher) { h.combine(ObjectIdentifier(self)) }
}

enum MCEncryptionPreference { case none, optional, required }

final class MCSession {
  var connectedPeers: [MCPeerID] = []
  weak var delegate: AnyObject?
  let label: Int
  private static var counter = 0
  init(peer: MCPeerID, securityIdentity: [Any]?, encryptionPreference: MCEncryptionPreference) {
    MCSession.counter += 1
    self.label = MCSession.counter
  }
}
`;
