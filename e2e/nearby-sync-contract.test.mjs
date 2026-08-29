import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// The native UI is now a thin react-native-webview shell (PdfReaderApp.tsx); the old
// FlatList reader and its reconnect-overlay / AppState / memory-tuning / onboarding
// machinery are gone. What remains — and what this file pins — is the Multipeer offline
// sync WIRE CONTRACT across the JS wrapper, its .d.ts, the ObjC bridge, and the Swift
// engine. Those layers are KEPT (and extended), so these assertions still guard real
// failure modes. Every assertion that read PdfReaderApp.tsx for removed reader UI was
// deleted (dead-behavior); restore from git history if that UI ever returns.

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const jsSyncSource = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.js"), "utf8");
const dtsSyncSource = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.d.ts"), "utf8");
const swiftSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
const bridgeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModuleBridge.m"), "utf8");

// A COMMENT IS NOT CODE, AND THIS FILE USED TO TREAT IT AS BOTH. Round 3 found that the windows
// below were cut out of the RAW Swift, so a comment could satisfy an assertion or break one on an
// edit that cannot change a single byte of the compiled binary: `// note: never use try! here`
// inside parseInboundPayload tripped its own force-try ban, and `// note: not .unreliable — MPC may
// drop it` inside sendCurrentPageSnapshot's do-block tripped the best-effort ban. In the other
// direction a cap deleted but left behind in a trailing comment still counted as a cap. So every
// STRUCTURAL search below runs against `swiftCode`: the same source with comments blanked to spaces,
// offsets and line breaks preserved, so a slice taken at these offsets still lines up with the file.
//
// maskSwift lexes strings so a `//` inside a literal cannot start a fake comment. Where it meets a
// construct it cannot lex soundly — a `"""` multi-line string, a `#"raw"#` literal, a nested block
// comment, anything unterminated — it REFUSES by name and line rather than guessing, because a
// masker that quietly mis-lexes turns every assertion downstream into a confident wrong answer. The
// thing that would settle any of this properly is a Swift parser (or the compiler); this is a
// lexer's worth of certainty and it should stop where a lexer's certainty stops.
function maskSwift(src, { strings = false, label = "DirectorSyncModule.swift" } = {}) {
  const out = src.split("");
  const lineAt = (i) => src.slice(0, i).split("\n").length;
  const blank = (from, to) => {
    for (let k = from; k < to && k < out.length; k += 1) out[k] = out[k] === "\n" ? "\n" : " ";
  };
  for (let j = 0; j < src.length; ) {
    if (src.startsWith('"""', j)) {
      assert.fail(`${label}:${lineAt(j)}: a """ multi-line string literal — this file's masker cannot lex one, so it refuses to score the source rather than mis-read it`);
    }
    if (src[j] === "#" && src[j + 1] === '"') {
      assert.fail(`${label}:${lineAt(j)}: a #"raw"# string literal — this file's masker cannot lex one, so it refuses to score the source rather than mis-read it`);
    }
    if (src[j] === '"') {
      let e = j + 1;
      while (e < src.length && src[e] !== '"' && src[e] !== "\n") e += src[e] === "\\" ? 2 : 1;
      assert.ok(src[e] === '"', `${label}:${lineAt(j)}: unterminated string literal — refusing to score the source`);
      if (strings) blank(j + 1, e);
      j = e + 1;
      continue;
    }
    if (src[j] === "/" && src[j + 1] === "/") {
      const nl = src.indexOf("\n", j);
      const e = nl < 0 ? src.length : nl;
      blank(j, e);
      j = e;
      continue;
    }
    if (src[j] === "/" && src[j + 1] === "*") {
      const e = src.indexOf("*/", j + 2);
      assert.ok(e > 0, `${label}:${lineAt(j)}: unterminated block comment — refusing to score the source`);
      assert.ok(src.slice(j + 2, e).indexOf("/*") < 0,
        `${label}:${lineAt(j)}: a NESTED block comment (legal in Swift) — this file's masker cannot lex one, so it refuses to score the source rather than mis-read it`);
      blank(j, e + 2);
      j = e + 2;
      continue;
    }
    j += 1;
  }
  return out.join("");
}

// Comments blanked, string literals intact: assertions here legitimately match protocol text like
// `"type": "hello"`, so only the commentary is removed. Computed on FIRST USE, not at import, so a
// refusal reddens the handful of tests that actually depend on the masked view instead of taking
// the whole file — and the other 30-odd assertions keep reporting.
let swiftCodeCache = null;
const swiftCodeOf = () => (swiftCodeCache ??= maskSwift(swiftSource));

// Swift cannot be executed here, so several assertions below are source-anchored. That is only
// honest if the window they read is the ONE function under test — this repo has repeatedly shipped
// whole-file regexes that stayed green because an identical call in an UNRELATED function satisfied
// them. These two helpers cut the window on STRUCTURE (a declaration, a `case` label, the member's
// own closing brace at 2-space indent) and both refuse to return a window unless BOTH endpoints
// were actually located — a missing end marker must fail loudly, never silently widen to EOF.
function memberBody(src, declRe, label) {
  const start = src.search(declRe);
  assert.ok(start > 0, `${label}: declaration not found — the member this test guards is gone or renamed`);
  const rel = src.slice(start).indexOf("\n  }");
  assert.ok(rel > 0, `${label}: could not find the member's closing brace at 2-space indent`);
  return src.slice(start, start + rel + 4);
}

function between(src, startRe, endRe, label) {
  const start = src.search(startRe);
  assert.ok(start > 0, `${label}: start marker not found`);
  const rel = src.slice(start + 1).search(endRe);
  assert.ok(rel > 0, `${label}: end marker not found after the start marker`);
  return src.slice(start, start + 1 + rel);
}

// Below, the MCPeerID test needs to read the ARGUMENT of a specific call and the SCOPE that call
// sits in, rather than counting occurrences across the file. These four helpers do that on
// structure only — balanced parentheses, the enclosing member declaration, the member's own closing
// brace — and every one of them fails loudly rather than returning a widened or empty window.

// Text between the parentheses that open at `openIdx`. Swift string interpolation (`\(x)`) nests
// real parens, so a balanced-depth walk is required; a naive indexOf(")") would truncate.
function parenArgs(src, openIdx) {
  let depth = 0;
  for (let i = openIdx; i < src.length; i += 1) {
    if (src[i] === "(") depth += 1;
    else if (src[i] === ")") {
      depth -= 1;
      if (depth === 0) return src.slice(openIdx + 1, i);
    }
  }
  return null;
}

// Identifiers that are real CODE in an expression. Plain string-literal text is dropped so a word
// inside "…" cannot masquerade as a capped local; interpolated segments are kept because they are
// code.
function exprIdentifiers(expr) {
  const code = expr.replace(
    /"(?:[^"\\]|\\[^(]|\\\([^)]*\))*"/g,
    (lit) => (lit.match(/\\\([^)]*\)/g) || []).join(" "),
  );
  return code.match(/[A-Za-z_]\w*/g) || [];
}

// End of the Swift statement whose right-hand side starts at `from`. Swift has no statement
// terminator, so the end is a newline — but only one that is not inside brackets and not followed by
// a `.method()` continuation, the two ways a binding legitimately spans lines. Leading whitespace is
// skipped first, so `let x =` followed by a newline continues rather than ending empty.
function swiftStatementEnd(src, from) {
  let i = from;
  while (i < src.length && /\s/.test(src[i])) i += 1;
  let depth = 0;
  for (; i < src.length; i += 1) {
    const c = src[i];
    if (c === "(" || c === "[" || c === "{") depth += 1;
    else if (c === ")" || c === "]" || c === "}") {
      if (depth === 0) return i;
      depth -= 1;
    } else if (c === "\n" && depth === 0) {
      const rest = src.slice(i + 1);
      const nextIdx = rest.search(/\S/);
      if (nextIdx < 0 || rest[nextIdx] !== ".") return i;
    }
  }
  return src.length;
}

// Locals in `body` that were bound to a 50-char-capped value, by whatever name the source chose.
// Derived from the source, never hard-coded, so renaming the local is behaviour-neutral here.
//
// Round 3 broke this helper in BOTH directions with one character class. It read
// `(?:let|var)\s+(\w+)\s*=\s*[^\n]*\.prefix\(50\)`, and `[^\n]*` is exactly one physical line: it
// spanned the trailing comment, so `let peerName = rawName // was String(rawName.prefix(50))`
// registered peerName as capped when the cap had been DELETED — the priming peer's MCPeerID then
// took an uncapped UIDevice.name and the whole file stayed green. And it stopped at the newline, so
// wrapping the very same binding across two lines — behaviour-neutral — made the local look
// uncapped and reddened the test. Both are gone: the body handed in is comment-masked, and the
// binding is read to the end of its STATEMENT rather than the end of its line.
function cappedLocals(body) {
  const bare = maskSwift(body, { strings: true, label: "member body" });
  const out = new Set();
  const re = /(?:let|var)\s+(\w+)\s*=/g;
  for (let m; (m = re.exec(bare)); ) {
    const stmt = bare.slice(m.index, swiftStatementEnd(bare, m.index + m[0].length));
    if (/\.prefix\(50\)/.test(stmt)) out.add(m[1]);
  }
  return out;
}

// The body of the member (func/var/init) that lexically contains `idx`.
function enclosingMemberBody(src, idx, label) {
  const declRe = /\n  (?:(?:private|public|internal|fileprivate|open|static|override|final|lazy)\s+)*(?:func|var|init|subscript)\b/g;
  let declIdx = -1;
  for (let m; (m = declRe.exec(src)) && m.index < idx; ) declIdx = m.index;
  assert.ok(declIdx > -1, `${label}: could not locate the member that contains this call`);
  const rel = src.slice(declIdx).indexOf("\n  }");
  assert.ok(rel > 0, `${label}: could not find that member's closing brace at 2-space indent`);
  const end = declIdx + rel + 4;
  assert.ok(idx < end, `${label}: the call sits outside the member body the scan found`);
  return src.slice(declIdx, end);
}

// Whether `name` is a member of this type that caps a value to 50 chars AND hands that capped value
// back to its caller — i.e. whether passing `name` to MCPeerID is safe. Three answers, not two:
// "capped", "uncapped", and "unknown" for a name this file has no business ruling on (not a member
// of the type at all — a free function, a type name, a property). A helper that collapses "I cannot
// tell" into "no" is how a confidently wrong failure gets shipped, so the caller refuses on unknown
// instead of accusing the source of dropping a cap it may well still have.
function memberVerdict(src, name) {
  const decl = new RegExp(
    `\\n  (?:(?:private|public|internal|fileprivate|open|static|final|lazy)\\s+)*(?:var|func)\\s+${name}\\b`,
  );
  const start = src.search(decl);
  if (start < 0) return "unknown";
  const rel = src.slice(start).indexOf("\n  }");
  assert.ok(rel > 0,
    `${name} is declared as a member but its body has no closing brace at 2-space indent — refusing to rule on whether it caps its name`);
  const body = src.slice(start, start + rel + 4);
  const capped = cappedLocals(body);
  if (capped.size === 0) return "uncapped";
  const returns = body.match(/\n\s*return\s+[^\n]+/g) || [];
  return returns.some((r) => [...capped].some((c) => new RegExp(`\\b${c}\\b`).test(r)))
    ? "capped"
    : "uncapped";
}

test("nearby sync page updates include mode and book identity", () => {
  assert.match(jsSyncSource, /sendPageUpdate\(\s*page,\s*totalPages,\s*String\(context\.mode/);
  assert.match(jsSyncSource, /String\(context\.bookId/);
  assert.match(dtsSyncSource, /context\?: \{ mode\?: "standard" \| "nonStandard"; bookId\?: string \}/);
  assert.match(bridgeSource, /mode:\(NSString \*\)mode/);
  assert.match(bridgeSource, /bookId:\(NSString \*\)bookId/);
  assert.match(swiftSource, /"v": Self\.protocolVersion/);
  assert.match(swiftSource, /"mode": mode/);
  assert.match(swiftSource, /"bookId": bookId/);
});

test("JS can force-request a current snapshot from the director", () => {
  assert.match(jsSyncSource, /requestCurrentSnapshot/);
  assert.match(dtsSyncSource, /requestCurrentSnapshot/);
  assert.match(bridgeSource, /requestCurrentSnapshot/);
  assert.match(swiftSource, /func requestCurrentSnapshot/);
  // Implementation must send a follower hello (director responds with snapshot).
  assert.match(swiftSource, /forceFollowerHelloNow/);
  assert.match(swiftSource, /"type": "hello"/);
});

test("unsupported-platform sync entrypoints reject through promises instead of throwing synchronously", () => {
  assert.match(jsSyncSource, /return Promise\.reject\(new Error\("La sincronización offline solo está disponible en iPad\."\)\);/);
  assert.doesNotMatch(jsSyncSource, /throw new Error\("La sincronización offline solo está disponible en iPad\."\);/);
});

// A follower that joins mid-Mass has no page until the director tells it one. Two independent
// paths cover that: the director pushes a snapshot the moment a peer connects, and it answers every
// follower "hello" with one. They are belt-and-suspenders for each other, so BOTH must be pinned
// separately — which is exactly what the old assertions failed to do.
test("happy path: director immediately snapshots on connect and on hello", () => {
  // What the old assertions missed: `/case \.connected:[\s\S]*director[\s\S]*sendCurrentPageSnapshot/`
  // spans the whole 2800-line file, so deleting the snapshot from the .connected director branch
  // was invisible — the third term was still satisfied by the hello handler ~66 lines further
  // down. The two paths now get two disjoint, structurally-bounded windows, so losing either one
  // reddens on its own.
  const connectedArm = between(
    swiftSource, /\n      case \.connected:/, /\n      case \.connecting:/, "session(_:peer:didChange:) .connected arm");
  const dirIdx = connectedArm.search(/self\.currentRole == "director"/);
  assert.ok(dirIdx > -1, "the .connected arm no longer has a director branch");
  const directorOnConnect = connectedArm.slice(dirIdx);
  assert.match(directorOnConnect, /self\.sendCurrentPageSnapshot\(to: peerID, via: session\)/,
    "a peer connecting to the director no longer gets an immediate page snapshot — a late joiner sits on the wrong page until its next hello tick (8s)");

  const helloHandler = between(
    swiftSource, /if type == "hello" \{/, /\n      if type == "/, "didReceive hello handler");
  assert.match(helloHandler, /self\.currentRole == "director"/,
    "the hello handler no longer checks that WE are the director before answering");
  assert.match(helloHandler, /self\.sendCurrentPageSnapshot\(to: peerID, via: session\)/,
    "the director no longer answers a follower hello with a snapshot — the follower's own recovery path is dead");
});

test("happy path: takeover approved/denied messages are handled only by follower", () => {
  assert.match(swiftSource, /if type == "takeover_approved"[\s\S]*currentRole == "follower"/);
  assert.match(swiftSource, /if type == "takeover_denied"[\s\S]*currentRole == "follower"/);
});

test("happy path: sendPageUpdate includes protocol version and mode/bookId fields", () => {
  assert.match(swiftSource, /"v": Self\.protocolVersion/);
  assert.match(swiftSource, /"mode": mode/);
  assert.match(swiftSource, /"bookId": bookId/);
});

test("happy path: discovery refresh cadence uses early burst then steady refresh", () => {
  assert.match(swiftSource, /earlyRefreshCycleCount/);
  assert.match(swiftSource, /earlyRefreshInterval/);
  assert.match(swiftSource, /discoveryRefreshInterval/);
});

test("soft app reset clears native sync transport and guards stale callbacks", () => {
  assert.match(jsSyncSource, /resetNearbyDirectorSync/);
  assert.match(jsSyncSource, /nativeModule\.resetForAppReset/);
  assert.match(dtsSyncSource, /resetNearbyDirectorSync/);
  assert.match(bridgeSource, /resetForAppReset/);
  assert.match(swiftSource, /resetGeneration = UUID\(\)/);
  assert.match(swiftSource, /guard self\.mcSessions\.contains\(where: \{ \$0 === session \}\) else \{ return \}/);
});

// Root cause of 1-director/10-follower failures: both sides calling invitePeer simultaneously
// created duplicate MCSession objects per pair. Fix: director never invites — followers are
// the sole inviters; director only accepts via advertiser delegate.
test("director does not eagerly call invitePeer in browser:foundPeer (legacy fallback allowed)", () => {
  const browserFoundPeerBlock = swiftSource.match(
    /func browser\(_ browser: MCNearbyServiceBrowser, foundPeer[\s\S]*?(?=\n  func )/
  )?.[0] ?? "";
  assert.ok(browserFoundPeerBlock.length > 0, "browser:foundPeer delegate must exist");
  // Directors may keep an immediate fallback invite for legacy followers (build ≤226) that wait
  // to be invited. Modern followers self-invite, so the director must not invite them here.
  assert.match(browserFoundPeerBlock, /Modern follower: it will self-invite us/);
  assert.match(browserFoundPeerBlock, /guard !self\.allConnectedPeers\.contains\(peerID\) else \{ return \}/);
  assert.match(browserFoundPeerBlock, /\.invitePeer\(/);
});

test("follower invite ownership lives in reconsiderFollowerTarget", () => {
  const reconsiderBlock = swiftSource.match(
    /private func reconsiderFollowerTarget\(\)[\s\S]*?(?=\n  private func )/
  )?.[0] ?? "";
  assert.ok(reconsiderBlock.length > 0, "reconsiderFollowerTarget must exist");
  assert.match(reconsiderBlock, /browser\?\.invitePeer\(capturedTarget, to: capturedSession/);
  assert.match(reconsiderBlock, /Modern director: we initiate/);
});

// Peer display names longer than 63 chars cause an ObjC exception in MCPeerID init.
test("Swift caps peer display name to 50 chars before creating MCPeerID", () => {
  // What the old single-line assertion missed: prefix(50) occurs at TWO sites — the throwaway
  // permission-priming peer and stablePeerName, which feeds the real archived MCPeerID. A
  // whole-file /prefix\(50\)/ is satisfied by either, so removing the cap at the site that
  // actually matters stayed green while a device whose UIDevice.name exceeds 63 chars would throw
  // the very ObjC exception this test is named after. Now the cap is asserted INSIDE
  // stablePeerName, and it must be the value the display name is actually built from.
  const stable = memberBody(swiftCodeOf(), /private var stablePeerName: String \{/, "stablePeerName");
  const capped = stable.match(/let\s+(\w+)\s*=\s*String\(\s*\w+(?:\.\w+)*\.prefix\(50\)\s*\)/);
  assert.ok(capped,
    "stablePeerName no longer caps the device name with prefix(50) — a long UIDevice.name will throw in MCPeerID init");
  const returned = stable.match(/\n\s*return\s+(.+)/)?.[1] ?? "";
  assert.ok(returned.length > 0, "stablePeerName has no return statement");
  assert.ok(returned.includes(capped[1]),
    `stablePeerName returns ${returned.trim()}, which does not use the capped name \`${capped[1]}\` — the cap is computed and thrown away`);

  // Every MCPeerID the module mints must be fed by a capped name, not just one of them.
  //
  // What the previous version of THIS assertion missed: it compared the NUMBER of prefix(50)
  // occurrences to the NUMBER of MCPeerID(displayName:) sites. An occurrence count is not the
  // property the message claimed. Two capped locals feeding a single MCPeerID plus one site fed by
  // a raw UIDevice.name satisfies the equality exactly — and a device whose name exceeds 63 chars
  // still throws the ObjC exception this test is named after. The property is now checked per SITE:
  // the expression each MCPeerID is handed must cap inline, or name a local capped in the same
  // member, or name a member (stablePeerName, above) that caps and returns the capped value.
  const siteRe = /MCPeerID\(\s*displayName:/g;
  const sites = [];
  for (let m; (m = siteRe.exec(swiftCodeOf())); ) sites.push(m.index);
  assert.ok(sites.length > 0, "no MCPeerID is created anywhere — the mesh cannot start");

  for (const siteIdx of sites) {
    const args = parenArgs(swiftCodeOf(), swiftCodeOf().indexOf("(", siteIdx));
    assert.ok(args !== null, "unbalanced MCPeerID(displayName:) call — its argument cannot be read");
    const expr = args.replace(/^\s*displayName:\s*/, "").trim();
    const where = `MCPeerID(displayName: ${expr})`;
    if (/\.prefix\(50\)/.test(expr)) continue; // capped inline, at the site itself
    const scope = enclosingMemberBody(swiftCodeOf(), siteIdx, where);
    const locals = cappedLocals(scope);
    // Score every identifier the expression is built from. "unknown" is its own answer: an
    // identifier that is neither a local of this member nor a member of this type is something this
    // file cannot follow, and reporting a missing cap on that basis would be a guess dressed as a
    // finding. Refuse instead, by name — still red, but red for the true reason.
    const idents = exprIdentifiers(expr);
    const declaredHere = (id) => new RegExp(`(?:let|var)\\s+${id}\\b`).test(scope);
    const verdicts = idents.map((id) =>
      locals.has(id) ? "capped"
        : declaredHere(id) ? "uncapped"       // a local of this member, and cappedLocals did not find a cap on it
          : memberVerdict(swiftCodeOf(), id));
    if (verdicts.includes("capped")) continue;
    const unknown = idents.filter((id, i) => verdicts[i] === "unknown");
    assert.ok(unknown.length === 0,
      `${where}: this file cannot decide whether ${unknown.join(", ")} carries a 50-char cap — ` +
      "neither a capped local of the enclosing member nor a member of this type. Refusing to guess; " +
      "what would settle it is compiling the module and reading the display name that actually reaches MCPeerID init.");
    assert.fail(
      `${where} is handed a name that nothing caps to 50 chars — on a device whose UIDevice.name exceeds 63 chars this throws the ObjC exception in MCPeerID init`);
  }
});

// sendPageUpdate must store the new page state BEFORE the early-return guard that checks
// for connected peers — otherwise late-joining followers get a nil snapshot.
test("Swift stores page state before the empty-peers guard in sendPageUpdate", () => {
  const sendPageUpdateBlock = swiftSource.match(
    /func sendPageUpdate[\s\S]*?(?=\n  func |\n  \/\/ MARK)/
  )?.[0] ?? "";
  assert.ok(sendPageUpdateBlock.length > 0, "sendPageUpdate must exist");
  const storeIdx = sendPageUpdateBlock.search(/currentPageNumber\s*=/);
  const guardIdx = sendPageUpdateBlock.search(/guard\s*!connected\.isEmpty/);
  assert.ok(storeIdx > -1, "must store currentPageNumber in sendPageUpdate");
  assert.ok(guardIdx > -1, "must have guard for empty connected peers");
  assert.ok(storeIdx < guardIdx, "state must be stored BEFORE the empty-peers guard");
});

// Dedup guard prevents multiple in-flight invitations to the same peer (reconsiderFollowerTarget).
test("Swift deduplicates in-flight invitations with pendingInvitePeer guard", () => {
  assert.match(swiftSource, /if let pending = pendingInvitePeer/);
  assert.match(swiftSource, /if pending == target/);
  assert.match(swiftSource, /if discoveredDirectors\[pending\] != nil/);
});

test("late joiners receive immediate snapshots from the director", () => {
  assert.match(swiftSource, /sendCurrentPageSnapshot\(to: peerID, via: session\)/);
  assert.match(swiftSource, /if type == "hello"[\s\S]*sendCurrentPageSnapshot\(to: peerID, via: session\)/);
});

// Belt-and-suspenders: MPC can drop the first reliable send right at .connected, so the
// director's proactive snapshot AND the follower's first hello can both vanish. A one-shot
// ~1.5s probe re-requests the snapshot if no page has arrived, so a joining/reconnecting
// follower snaps to the director's current page fast instead of waiting a full hello tick (8s).
test("late joiner: follower schedules a one-shot snapshot-recovery probe on connect", () => {
  assert.match(swiftSource, /followerSnapshotProbeDelay/);
  assert.match(swiftSource, /private func scheduleFollowerSnapshotProbe\(\)/);
  const probeBlock = swiftSource.match(/private func scheduleFollowerSnapshotProbe\(\)[\s\S]*?\n  \}/)?.[0] ?? "";
  assert.ok(probeBlock.length > 0, "scheduleFollowerSnapshotProbe must exist");
  // Must be generation-guarded (a reset cancels it) and gated on "no page received yet".
  assert.match(probeBlock, /self\.resetGeneration == generation/);
  assert.match(probeBlock, /lastFollowerPageReceivedAt == 0/);
  assert.match(probeBlock, /forceFollowerHelloNow\(\)/);
  // Must be invoked on the .connected follower path, after the hello timer starts.
  const connectedBlock = swiftSource.match(
    /case \.connected:[\s\S]*?self\.startFollowerHelloTimer\(\)[\s\S]*?scheduleFollowerSnapshotProbe\(\)/
  )?.[0] ?? "";
  assert.ok(connectedBlock.length > 0, "probe must be scheduled on .connected after startFollowerHelloTimer");
});

test("discovery cadence keeps early burst then steady 12-second refreshes", () => {
  // 25 -> 12 and burst 6 -> 12 cycles, build 433. A follower that lost the director waited a FULL
  // refresh interval before looking again — that was the 14 s recovery measured when a
  // backgrounded director returned. Not lower than ~5 s: each refresh tears down and rebuilds both
  // transports, so past that you disrupt discovery more than you perform it.
  assert.match(swiftSource, /private static let discoveryRefreshInterval: TimeInterval = 12/);
  assert.match(swiftSource, /private static let earlyRefreshInterval: TimeInterval = 5/);
  assert.match(swiftSource, /private static let earlyRefreshCycleCount = 12/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining = Self\.earlyRefreshCycleCount/);
  assert.match(swiftSource, /earlyRefreshCyclesRemaining > 0/);
});

test("timers are generation-guarded so stale callbacks cannot survive reset", () => {
  assert.match(swiftSource, /let generation = resetGeneration/);
  assert.match(swiftSource, /self\.resetGeneration == generation/);
  assert.match(swiftSource, /DispatchQueue\.main\.asyncAfter\(deadline: \.now\(\) \+ Self\.followerRetryDelay\)[\s\S]*self\.resetGeneration == generation/);
});

test("each follower gets its OWN session — no follower-to-follower cross-connect", () => {
  // Miguel, 2026-08-18: "nuke peer sharing and any peer connections... one director to one
  // follower... that setup times N followers". MCSession connects every member of ONE session to
  // every OTHER member — not a choice this codebase made, Apple's framework behavior — so any
  // session with more than 2 peers lets followers see each other's traffic at the protocol level.
  // A prior measured incident (2026-08-16, comment near "22 follower-to-follower connections")
  // documented this exact cross-connect causing a follower to misidentify ANOTHER FOLLOWER as its
  // director. maxFollowersPerSession=1 makes that structurally impossible: a session with exactly
  // 2 members (director + one follower) has no other follower in it to cross-connect with.
  assert.match(swiftSource, /private static let maxFollowersPerSession = 1/,
    "maxFollowersPerSession is not 1 — followers can still cross-connect within a shared session");
  const m = swiftSource.match(/private static let maxSessions = (\d+)/);
  assert.ok(m, "maxSessions constant missing");
  assert.ok(Number(m[1]) >= 8,
    `maxSessions=${m?.[1]} is too low for real fleet capacity now that each session holds only 1 follower`);
  assert.match(swiftSource, /availableSessionForNewFollower\(\)/);
});

test("FOLLOWER_START_FAILED: browser failure emits error to JS so the follower can recover", () => {
  assert.match(swiftSource, /FOLLOWER_START_FAILED/);
  // Must guard against the priming browser — only the real browser emits.
  assert.match(swiftSource, /browser === self\.browser.*currentRole == "follower"[\s\S]{0,60}FOLLOWER_START_FAILED/);
});

test("native emits memory-warning event to JS on iOS memory pressure", () => {
  assert.match(swiftSource, /UIApplication\.didReceiveMemoryWarningNotification/);
  assert.match(swiftSource, /"type": "memoryWarning"/);
});

test("follower pauses discovery refresh timer when connected to director", () => {
  assert.match(swiftSource, /pauseDiscoveryRefreshWhileConnected\(\)/);
  assert.match(swiftSource, /private func pauseDiscoveryRefreshWhileConnected\(\)/);
  // Pause must be called on the .connected path before startFollowerHelloTimer
  const connectedBlock = swiftSource.match(
    /case \.connected:[\s\S]*?self\.startFollowerHelloTimer\(\)/
  )?.[0] ?? "";
  assert.ok(connectedBlock.includes("pauseDiscoveryRefreshWhileConnected"), "pause must be called on connect");
});

test("follower resumes discovery fast-burst on disconnect so late director is found quickly", () => {
  assert.match(swiftSource, /resumeDiscoveryRefreshAfterDisconnect\(\)/);
  assert.match(swiftSource, /private func resumeDiscoveryRefreshAfterDisconnect\(\)/);
  // Resume must fire before startSelfDirectedTimer on .notConnected path
  const disconnBlock = swiftSource.match(
    /case \.notConnected:[\s\S]*?self\.startSelfDirectedTimer\(\)/
  )?.[0] ?? "";
  assert.ok(disconnBlock.includes("resumeDiscoveryRefreshAfterDisconnect"), "resume must be called on disconnect");
});

test("native state emissions are deduplicated to cut JS bridge churn", () => {
  assert.match(swiftSource, /lastEmittedStatus/);
  assert.match(swiftSource, /lastEmittedPeerCount/);
  // Dedup guard must appear inside emitState, not just as a property declaration
  const emitFn = swiftSource.match(
    /private func emitState\(status: String[\s\S]*?\n  \}/
  )?.[0] ?? "";
  assert.ok(emitFn.includes("lastEmittedStatus"), "dedup guard must be inside emitState");
});

test("refreshDiscovery wraps MPC object churn in autoreleasepool", () => {
  const refreshFn = swiftSource.match(
    /private func refreshDiscovery\(\)[\s\S]*?\n  \}/
  )?.[0] ?? "";
  assert.ok(refreshFn.includes("autoreleasepool"), "refreshDiscovery must use autoreleasepool");
});

test("edge case: protocolVersion mismatch is ignored (v != 0 and != protocolVersion)", () => {
  assert.match(swiftSource, /if v != 0, v != Self\.protocolVersion \{ return \}/);
});

// parseInboundPayload is the ONLY thing standing between the app and an arbitrary byte string
// handed to it by any device in Bluetooth/AWDL range. A malformed or absurdly large packet must
// return nil, not trap.
test("edge case: parseInboundPayload gracefully rejects invalid JSON", () => {
  // The old assertions were three whole-file scans, and the source has more than one of each:
  // `try? JSONSerialization.jsonObject` also appears in an unrelated decoder further down, and
  // `return nil` appears in availableSessionForNewFollower 30 lines EARLIER. Replacing this entire
  // body with `try!` / `as!` — which turns any stranger's malformed packet into a crash and drops
  // the size guard — left the test green. The window is now the function's own body.
  //
  // Round 3: the window was cut out of the raw source, so writing `// note: never use try! here`
  // inside this function tripped the force-try ban below — a comment failing a test about compiled
  // behaviour. The window is comment-masked now, which also stops a commented-out `return nil` from
  // padding the rejection-path count.
  const body = memberBody(swiftCodeOf(), /private func parseInboundPayload\(/, "parseInboundPayload");

  assert.match(body, /data\.count\s*<=\s*Self\.maxInboundPayloadBytes/,
    "the payload size guard is gone — a pathological packet is handed straight to JSONSerialization");
  assert.match(body, /try\?\s*JSONSerialization\.jsonObject/,
    "the JSON parse is no longer an optional try — malformed JSON now throws instead of returning nil");
  assert.match(body, /as\?\s*\[String\s*:\s*Any\]/,
    "the dictionary cast is no longer conditional — a JSON array or scalar would trap");
  assert.doesNotMatch(body, /try\s*!/,
    "force-try in parseInboundPayload: any peer in range can crash the app with one bad packet");
  assert.doesNotMatch(body, /as\s*!/,
    "force-cast in parseInboundPayload: a well-formed JSON array from a peer would crash the app");

  // Both rejection paths (oversized, unparseable) must actually bail rather than fall through.
  const bails = (body.match(/return nil/g) || []).length;
  assert.ok(bails >= 2,
    `parseInboundPayload has only ${bails} nil-returning rejection path(s) — the size guard and the parse failure each need one`);
});

test("edge case: followerHello is throttled when pages are being received", () => {
  assert.match(swiftSource, /lastFollowerPageReceivedAt/);
  assert.match(swiftSource, /now - lastFollowerPageReceivedAt < Self\.followerHelloInterval \* 2/);
});

test("edge case: followerHello is throttled when a hello was sent recently", () => {
  assert.match(swiftSource, /lastFollowerHelloAt/);
  assert.match(swiftSource, /now - lastFollowerHelloAt < Self\.followerHelloInterval/);
});

test("edge case: self-directed timer only fires when follower has no connectedDirectorPeer and no pendingInvitePeer", () => {
  const timerBlock = swiftSource.match(/private func startSelfDirectedTimer\(\)[\s\S]*?\}\n\s*\}\n\s*\}\n/)?.[0] ?? "";
  assert.ok(timerBlock.length > 0, "startSelfDirectedTimer must exist");
  assert.match(timerBlock, /currentRole == "follower"/);
  assert.match(timerBlock, /connectedDirectorPeer == nil/);
  assert.match(timerBlock, /pendingInvitePeer == nil/);
});

test("edge case: follower disconnect schedules a retry after followerRetryDelay", () => {
  assert.match(swiftSource, /followerRetryDelay/);
  assert.match(swiftSource, /DispatchQueue\.main\.asyncAfter\(deadline: \.now\(\) \+ Self\.followerRetryDelay\)/);
  assert.match(swiftSource, /reconsiderFollowerTarget\(\)/);
});

test("edge case: requestCurrentSnapshot is exposed on the ObjC bridge", () => {
  assert.match(bridgeSource, /requestCurrentSnapshot/);
});

test("edge case: requestCurrentSnapshot only sends when follower is connected to a director", () => {
  assert.match(swiftSource, /private func forceFollowerHelloNow\(\)/);
  assert.match(swiftSource, /connectedDirectorPeer/);
  assert.match(swiftSource, /session\.connectedPeers\.contains/);
});

test("edge case: director sendCurrentPageSnapshot uses reliable delivery", () => {
  // The catch-up snapshot is a ONE-SHOT: unlike the page heartbeat, nothing resends it on its own.
  // Sent best-effort it can simply vanish, and the late-joining iPad stays on the wrong page.
  //
  // What the old assertions missed: `with: .reliable` appears at six sites and `with: .unreliable`
  // at two, all matched against the whole file, so flipping the ONE send inside this function to
  // .unreliable was invisible. The window is now the function body, and the two ordering modes are
  // checked in their own halves of it — reliable on the primary send, unreliable only as the catch
  // fallback — so a swap in either direction reddens.
  //
  // Round 3: the do/catch split was computed over raw source, so a comment inside the do-block that
  // merely MENTIONED the other ordering mode — `// note: not .unreliable — MPC may drop it`, the
  // most natural thing a maintainer would write here — landed inside the primary half and failed the
  // best-effort ban. Comments are masked out before the split now.
  const body = memberBody(swiftCodeOf(), /private func sendCurrentPageSnapshot\(/, "sendCurrentPageSnapshot");

  const doIdx = body.search(/\bdo\s*\{/);
  const catchIdx = body.search(/\}\s*catch\b/);
  assert.ok(doIdx > -1, "the primary snapshot send is no longer wrapped in do/catch");
  assert.ok(catchIdx > doIdx, "the catch fallback after the primary send is gone");
  const primary = body.slice(doIdx, catchIdx);
  const fallback = body.slice(catchIdx);

  assert.match(primary, /session\.send\([^)]*with:\s*\.reliable\)/,
    "the primary catch-up snapshot is no longer sent .reliable — MPC may drop it and leave a late-joining follower on the wrong page");
  assert.doesNotMatch(primary, /\.unreliable/,
    "the primary send has been downgraded to best-effort delivery");
  assert.match(fallback, /session\.send\([^)]*with:\s*\.unreliable\)/,
    "the .unreliable catch fallback is gone — a throwing reliable send now drops the snapshot entirely");
});

// ── The peer bundle-push rail is RETIRED (plan §5.12 / Q4, red team A5) ──────
//
// This rail streamed a director's own WebBundle to a follower over Multipeer. It is the ONLY
// writer of Documents/WebBundle and therefore the sole source of the D1 stale-bundle trap, and it
// compares CFBundleVersion — the SHELL's number, which says nothing about which book either device
// holds — so it can push a songbook BACKWARDS.
//
// These pin the guards at their SOURCE, because a disabled feature with no test is a feature that
// quietly comes back. The receive side is what matters: a peer running an older build still offers
// and still sends, so guarding only the send side would leave the rail wide open.
const appSource = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

test("peer bundle push is GONE — not disabled, not reachable, no receiver", () => {
  // Replaces five tests that pinned the guards on a DISABLED rail. Build 434 deleted the subsystem
  // outright, and the guards went with it. This asserts the stronger property.
  //
  // WHY IT IS A SECURITY TEST, not tidiness. From the project's own audit
  // (docs/audit-findings-raw.md:240): any device in range advertising role=director on the
  // hard-coded public session code could push an arbitrary web bundle onto every follower iPad —
  // no auth, no signature, no consent — into a WebView with originWhitelist ['*'] and
  // allowFileAccess, surviving reboot. It shipped for many builds behind ONE boolean. It was also
  // the only writer of Documents/WebBundle (the directory the boot resolver prefers forever) and
  // could push a book BACKWARDS, since its check compared CFBundleVersion rather than the book.
  //
  // Comments are stripped first: the tombstone doc-block deliberately NAMES what was removed so a
  // future reader understands why, and that explanation must not trip its own test.
  const code = swiftSource
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .split("\n").filter((l) => !l.trim().startsWith("//") && !l.trim().startsWith("///")).join("\n");

  // If any of these names come back, the mechanism came back with them.
  for (const gone of [
    "meshBundlePushEnabled", "bundleTransferInFlight", "bundleTransferGeneration",
    "sendBundleOffer", "handleBundleOffer", "handleBundleRequest",
    "packWebBundle", "installReceivedBundle", "beginBundleTransfer",
    "bundle_offer", "bundle_request",
  ]) {
    assert.ok(!code.includes(gone), `peer bundle push is back: ${gone} reappeared in the mesh module`);
  }

  // The two MCSessionDelegate resource methods must REMAIN (protocol requirements) and must not
  // keep what a peer sends. An empty didStart plus a delete in didFinish is the correct posture:
  // before, a stranger's archive was written to the container before anything rejected it.
  assert.match(swiftSource, /didStartReceivingResourceWithName[\s\S]{0,200}\{\}/,
    "didStartReceivingResource must exist with an EMPTY body");
  const fin = swiftSource.slice(swiftSource.indexOf("didFinishReceivingResourceWithName"));
  assert.match(fin.slice(0, 400), /removeItem\(at: localURL\)/,
    "didFinishReceivingResource must DELETE anything a peer sent");
});

test("the JS bundleUpdated handler no longer auto-remounts the WebView", () => {
  // It used to re-resolve and remount ON THE SPOT with no human gate and no timing check — a peer
  // arriving mid-Mass could swap the songbook out from under a singer mid-verse.
  const handler = appSource.slice(appSource.indexOf('case "bundleUpdated"'), appSource.indexOf('case "bundleUpdated"') + 900);
  assert.doesNotMatch(handler, /setMountKey/, "bundleUpdated must not remount");
  assert.doesNotMatch(handler, /setBundleUri/, "bundleUpdated must not swap the bundle");
  assert.match(handler, /mesh-bundleUpdated-ignored/, "it should leave a breadcrumb that it was ignored");
});

test("BLE contention cooldown outlasts a single sliding-window gap, not just one packet", () => {
  // Live hardware capture (2026-08-18), verified via Xcode Console on a real device mid-repro:
  // nonce af9b/page 99 and nonce 5990/page 50 were BOTH broadcasting; the existing abstain logic
  // correctly suppressed page-apply WHILE both were within the 4s contentionWindow, but the moment
  // af9b's last-seen timestamp aged out of that window, page 50 (also wrong) was trusted and
  // applied on the very next packet. Nothing had proven af9b actually stopped — only that it
  // hadn't been re-heard recently enough. This pins the fix: a SUSTAINED cooldown after
  // contention, not just the first uncontested packet.
  const bleSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  assert.match(bleSource, /private var lastContentionAt: TimeInterval = 0/,
    "lastContentionAt tracking is gone — the cooldown has nothing to measure from");
  assert.match(bleSource, /private static let contentionCooldown: TimeInterval = 4\.0/,
    "contentionCooldown constant is gone or changed unexpectedly");
  assert.match(bleSource, /lastContentionAt = now/,
    "contention detection no longer stamps lastContentionAt — the cooldown would never arm");
  assert.match(bleSource, /now - lastContentionAt < Self\.contentionCooldown/,
    "the cooldown check is gone — a rival can win the instant its sliding-window entry expires");
});

test("BLE page broadcasts are HMAC-bound to the session code — a lone broadcaster must prove it knows the code", () => {
  // Hardened 2026-08-18 after THREE live hardware captures (Xcode Console, real devices) each
  // showed a "ghost" page (nonce+page matching no song any device had actually navigated to) get
  // applied even after: the MCSession one-follower-per-session fix, the contention-cooldown fix,
  // and an explicit force-quit of all 4 devices. A bare nonce proves nothing about WHO sent it —
  // any broadcaster in range that can format "SV<nonce>.<seq>.<page>" was trusted. This closes that
  // gap with zero added latency: the tag is a local HMAC comparison, not a round trip, so BLE keeps
  // its sub-second reaction time while a scanner now refuses any packet whose tag doesn't verify
  // against the session code it was itself given.
  const bleSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "BlePageBeacon.swift"), "utf8");
  assert.match(bleSource, /var sessionCode: String = ""/,
    "sessionCode is gone — there is nothing left to bind the tag to");
  assert.match(bleSource, /HMAC<SHA256>\.authenticationCode/,
    "the HMAC tag computation is gone — broadcasts are unauthenticated again");
  assert.match(bleSource, /guard parts\.count == 4/,
    "parse() no longer requires the tag field — untagged packets would be accepted");
  assert.match(bleSource, /tag == Self\.authTag\(sessionCode: sessionCode,/,
    "parse() no longer verifies the tag — any well-formed packet would be trusted again");

  const nativeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");
  const setSites = (nativeSource.match(/self\.bleBeacon\.sessionCode = normalizedSessionCode/g) || []).length;
  assert.strictEqual(setSites, 2,
    "expected bleBeacon.sessionCode to be set from BOTH startDirector and startFollower — found " + setSites);
});

test("a follower never accepts a mesh invite from another follower", () => {
  // Confirmed on real hardware 2026-08-18: a follower's MCSession connected to a FELLOW FOLLOWER
  // (tagged session:peer-not-director), which then silently wedged reconnection to the real
  // director for 40+ seconds — reconsiderFollowerTarget's old guard treated "anything connected"
  // as "connected to the director" and never retried. Root cause: didReceiveInvitationFromPeer's
  // follower branch accepted ANY invite unconditionally, with no check on who was asking.
  const nativeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

  // 1) accept-time: the follower branch must verify the inviter before accepting.
  assert.match(nativeSource, /A FOLLOWER MUST NEVER ACCEPT AN INVITE FROM ANOTHER FOLLOWER/,
    "the accept-time verification comment/guard is gone — followers may accept invites from anyone again");
  assert.match(nativeSource, /guard peerIsKnownDirector else \{\s*\n\s*self\.dbgLog\("invite:reject", \["from": peerID\.displayName, "why": "not-a-director"\]\)/,
    "the follower-side invite accept no longer verifies the inviter is a known director");

  // 2) connect-time cleanup: a non-director peer that lands in the session must be dropped, not just logged.
  assert.match(nativeSource, /self\.dbgLog\("session:peer-not-director", \["peer": peerID\.displayName\]\)\s*\n\s*\/\/ ACTIVELY DROP IT/,
    "session:peer-not-director no longer disconnects the bogus peer — it can wedge reconnection again");
  assert.match(nativeSource, /session\.cancelConnectPeer\(peerID\)/,
    "cancelConnectPeer call is gone — a rejected peer will linger in session.connectedPeers");

  // 3) retry guard: must check the actual director connection, not just "is anything connected".
  assert.match(nativeSource, /guard connectedDirectorPeer == nil else \{ emitState\(status: "connected"\); return \}/,
    "reconsiderFollowerTarget reverted to the connectedPeers.isEmpty guard — a stray peer can wedge retry again");
  assert.doesNotMatch(nativeSource, /guard session\.connectedPeers\.isEmpty else \{ emitState\(status: "connected"\); return \}/,
    "the old, wedge-prone connectedPeers.isEmpty guard is back");
});

test("a stale ex-director is evicted after repeated rejected invites, not retargeted forever", () => {
  // Confirmed on real 4-device hardware (2026-08-19): a device that demoted itself from director
  // back to follower stayed in every OTHER follower's discoveredDirectors (only lostPeer clears
  // it, and the demoted device never left range) — and its stale token, being the most recent,
  // kept sorting first. Two followers spent 90+ seconds firing invite:send at it every 300-700ms,
  // each rejected (why=not-a-director) and immediately retried, while the REAL, live director sat
  // undiscovered nearby the whole time. The rejection round-trip is far faster than a genuine
  // timeout, so the normal retry-after-timeout backoff never engaged.
  const nativeSource = fs.readFileSync(path.join(APP_ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

  assert.match(nativeSource, /private var invalidDirectorStreak: \[MCPeerID: Int\] = \[:\]/,
    "the per-peer failure streak tracker is gone — a stale director can be retargeted forever again");
  assert.match(nativeSource, /private static let invalidDirectorEvictThreshold = 2/,
    "the eviction threshold constant is gone or changed unexpectedly");
  assert.match(nativeSource, /if streak >= Self\.invalidDirectorEvictThreshold \{/,
    "the eviction check on repeated connect failure is gone");
  assert.match(nativeSource, /self\.discoveredDirectors\.removeValue\(forKey: peerID\)\s*\n\s*self\.discoveredDirectorInfo\.removeValue\(forKey: peerID\)\s*\n\s*self\.discoveredDirectorSeenAt\.removeValue\(forKey: peerID\)/,
    "eviction no longer clears all three discoveredDirector* dicts for the stale peer");
  assert.match(nativeSource, /self\.invalidDirectorStreak\.removeValue\(forKey: peerID\)\s*\n\s*self\.followerHuntingSince = 0/,
    "a successful connect no longer clears the failure streak — a peer that briefly failed once could get wrongly evicted later");
});
