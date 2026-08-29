import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");

// WHY THIS FILE EXISTS. A boot notice read "Estabas dirigiendo. Toca el estado arriba a la izquierda
// para volver a dirigir." The top-left status stopped taking the role in build 435 and the sentence
// was never updated, so for weeks it sent whoever read it to the wrong control — at the one moment
// they were already flustered, having just had the app die on them mid-Mass. On 2026-08-18 the way
// in moved to the top-RIGHT, making it wronger still.
//
// The lesson is not "keep the text in sync". It is that a notice describing where a control lives is
// a promise about the layout, and this layout moves. Notices carry the control instead.

// WHAT THE OLD ASSERTION MISSED. It matched one shape — `type: "toast",\n text: "…"` — and there are
// six toast sites in four shapes. It saw three. The three it could not see are the ones that matter:
// two of them are `text: noticeText`, and the sentence lives in a const one line above, which is
// exactly where the freshness nudge's copy is written. Putting the old directional sentence back
// into that const left the file GREEN. A scanner that silently skips half its inputs is worse than
// no scanner, because it reports a clean sweep it never made. So the extractor now resolves every
// shape the source uses — literal, template, same-line ternary, and an identifier followed back to
// its declaration — and the count of resolved texts must equal the count of toast sites, so a new
// shape fails loudly instead of being skipped.

// WHAT THE FIRST REWRITE STILL MISSED. Following `text: noticeText` back to its declaration reads
// only the initializer, and a sentence is just as easily added afterwards — `noticeText += "Toca el
// estado arriba a la izquierda."`, or a reassignment on a branch. That copy ships to the choir and
// the old extractor never looked at it. Every write to the identifier inside the block that declares
// it is scanned now, not just the first. And the site count is pinned against an independent count
// of the source's own toast literals, so a seventh notice cannot arrive in a shape the extractor
// cannot see and be skipped in silence — the sweep either covers everything or it goes red.

// ── Reading the source at STRUCTURE level ───────────────────────────────────────────────────────
// Strings, templates and comments are skipped whole, so a brace or comma inside prose (this file's
// subject matter is prose) can never be mistaken for syntax. Nothing here re-implements the app;
// it only decides which characters are code.
const skipLiteral = (src, i) => {
  const quote = src[i];
  for (let j = i + 1; j < src.length; j++) {
    if (src[j] === "\\") { j++; continue; }
    if (src[j] === quote) return j + 1;
  }
  throw new Error(`unterminated ${quote} literal at offset ${i}`);
};

// Blank out every literal, comment and nested bracket group, preserving offsets, so an indexOf on
// the result can only match structure belonging to THIS level. Used instead of a character-count
// window: a window has to guess how far to look, and guesses wrong every time a comment is added.
const skeleton = (body) => {
  const out = body.split("");
  const blank = (from, to) => { for (let k = from; k < to && k < out.length; k++) out[k] = out[k] === "\n" ? "\n" : " "; };
  let depth = 0;
  for (let j = 0; j < body.length; ) {
    const c = body[j];
    if (c === '"' || c === "'" || c === "`") { const e = skipLiteral(body, j); blank(j, e); j = e; continue; }
    if (c === "/" && body[j + 1] === "/") { const nl = body.indexOf("\n", j); const e = nl < 0 ? body.length : nl; blank(j, e); j = e; continue; }
    if (c === "/" && body[j + 1] === "*") { const e = body.indexOf("*/", j); if (e < 0) throw new Error("unterminated comment"); blank(j, e + 2); j = e + 2; continue; }
    if (c === "{" || c === "(" || c === "[") { if (depth > 0) out[j] = " "; depth++; j++; continue; }
    if (c === "}" || c === ")" || c === "]") { depth--; if (depth > 0) out[j] = " "; j++; continue; }
    if (depth > 0) out[j] = " ";
    j++;
  }
  return out.join("");
};

// From a position inside an object literal to the `}` that closes it. Structural endpoint, not a
// count of characters, and it throws rather than running to EOF if the brace is missing.
const objectBody = (src, from) => {
  let depth = 0;
  for (let j = from; j < src.length; ) {
    const c = src[j];
    if (c === '"' || c === "'" || c === "`") { j = skipLiteral(src, j); continue; }
    if (c === "/" && src[j + 1] === "/") { const nl = src.indexOf("\n", j); j = nl < 0 ? src.length : nl; continue; }
    if (c === "/" && src[j + 1] === "*") { const e = src.indexOf("*/", j); if (e < 0) throw new Error("unterminated comment"); j = e + 2; continue; }
    if (c === "{" || c === "(" || c === "[") { depth++; j++; continue; }
    if (c === "}" && depth === 0) return src.slice(from, j);
    if (c === "}" || c === ")" || c === "]") { depth--; j++; continue; }
    j++;
  }
  throw new Error(`object starting at ${from} is never closed`);
};

// Every string a fragment of source can produce: quoted literals, and the static halves of a
// template with its ${…} holes removed (a hole cannot contain a directional word without one of
// the words also appearing in some string, and we would rather under-claim than invent text).
const literalsIn = (code) => {
  const found = [];
  for (let j = 0; j < code.length; ) {
    const c = code[j];
    if (c === '"' || c === "'" || c === "`") {
      const e = skipLiteral(code, j);
      found.push(code.slice(j + 1, e - 1).replace(/\$\{[^}]*\}/g, " "));
      j = e;
      continue;
    }
    if (c === "/" && code[j + 1] === "/") { const nl = code.indexOf("\n", j); j = nl < 0 ? code.length : nl; continue; }
    if (c === "/" && code[j + 1] === "*") { const e = code.indexOf("*/", j); j = e < 0 ? code.length : e + 2; continue; }
    j++;
  }
  return found;
};

const lineOf = (src, index) => src.slice(0, index).split("\n").length;

// Two offset-preserving views of the whole file. `comments: true` blanks comments, so prose quoting
// `type: "toast"` cannot invent a notice that does not exist; `strings: true` additionally blanks
// the INSIDE of every literal (the quotes stay), so an identifier search can only ever match code.
// Offsets are preserved in both, which is the whole point: a line number or a slice taken from one
// view means the same thing in the original.
const maskOut = (src, { strings = false, comments = false }) => {
  const out = src.split("");
  const blank = (from, to) => { for (let k = from; k < to && k < out.length; k++) out[k] = out[k] === "\n" ? "\n" : " "; };
  for (let j = 0; j < src.length; ) {
    const c = src[j];
    if (c === '"' || c === "'" || c === "`") { const e = skipLiteral(src, j); if (strings) blank(j + 1, e - 1); j = e; continue; }
    if (c === "/" && src[j + 1] === "/") { const nl = src.indexOf("\n", j); const e = nl < 0 ? src.length : nl; if (comments) blank(j, e); j = e; continue; }
    if (c === "/" && src[j + 1] === "*") { const e = src.indexOf("*/", j); if (e < 0) throw new Error("unterminated comment"); if (comments) blank(j, e + 2); j = e + 2; continue; }
    j++;
  }
  return out.join("");
};

// From an offset inside a block to the `}` that closes it. Structural endpoint: the scope an
// identifier's writes can live in ends where its declaring block ends, not some number of lines on.
const blockEnd = (bare, from) => {
  let depth = 0;
  for (let j = from; j < bare.length; j++) {
    const c = bare[j];
    if (c === "{" || c === "(" || c === "[") depth++;
    else if (c === "}" || c === ")" || c === "]") { if (depth === 0) return j; depth--; }
  }
  return bare.length;
};

// End of the statement starting at `from`: the first `;` at this bracket depth, or the close of the
// enclosing block if the statement is the last one and unterminated.
const statementEnd = (bare, from) => {
  let depth = 0;
  for (let j = from; j < bare.length; j++) {
    const c = bare[j];
    if (c === "{" || c === "(" || c === "[") depth++;
    else if (c === "}" || c === ")" || c === "]") { if (depth === 0) return j; depth--; }
    else if (c === ";" && depth === 0) return j;
  }
  return bare.length;
};

// Every string the identifier can be holding by the time a notice reads it: its declaration's
// initializer AND every later write to it — `x = …`, `x += …` — inside the block that declares it.
// Reading only the declaration is how appended copy ships unread.
const identifierStrings = (src, bare, ident, beforeIdx) => {
  const declRe = new RegExp(`(?:const|let|var)\\s+${ident}\\s*=(?![=>])`, "g");
  let declIdx = -1;
  for (const d of bare.matchAll(declRe)) { if (d.index < beforeIdx) declIdx = d.index; }
  if (declIdx < 0) return null;
  const scopeEnd = blockEnd(bare, declIdx);
  const writeRe = new RegExp(`\\b${ident}\\s*\\+?=(?![=>])`, "g");
  const strings = [];
  let writes = 0;
  for (const w of bare.slice(declIdx, scopeEnd).matchAll(writeRe)) {
    const at = declIdx + w.index + w[0].length;
    strings.push(...literalsIn(src.slice(at, statementEnd(bare, at))));
    writes++;
  }
  return { strings, writes, declLine: lineOf(src, declIdx) };
};

// One entry per `type: "toast"` in the source, with `text` resolved or left null. Null is the
// interesting case: it means a notice shipped in a shape nothing reads.
const toastSites = (src) => {
  const bare = maskOut(src, { strings: true });
  const sites = [];
  for (const m of src.matchAll(/type:\s*(["'`])toast\1/g)) {
    const site = { line: lineOf(src, m.index), text: null, how: "unresolved" };
    sites.push(site);
    const body = objectBody(src, m.index);
    const skel = skeleton(body);
    const key = skel.search(/(^|[\s,{])text\s*:/);
    if (key < 0) continue;                              // a toast with no text at all
    const colon = skel.indexOf(":", key);
    const comma = skel.indexOf(",", colon);
    const expr = body.slice(colon + 1, comma < 0 ? body.length : comma);
    let strings = literalsIn(expr);
    let how = "inline";
    if (!strings.length) {
      // `text: someIdent` — follow it back to EVERY write to that identifier in the block that
      // declares it, not just the initializer. This is the shape the old matcher could not see, and
      // the one both mutations used: first a directional sentence written into the const, then the
      // same sentence appended a line later.
      const ident = expr.trim();
      if (!/^[A-Za-z_$][\w$]*$/.test(ident)) continue;
      const resolved = identifierStrings(src, bare, ident, m.index);
      if (!resolved) continue;
      strings = resolved.strings;
      how = `${ident} declared at line ${resolved.declLine}, ${resolved.writes} write(s) scanned`;
    }
    if (!strings.length) continue;
    site.text = strings.join(" | ");
    site.how = how;
  }
  return sites;
};

test("no notice tells anyone where a control is", () => {
  // Read the source with its comments blanked out, so prose that happens to quote a notice cannot
  // invent a site, and so the independent count below counts shipped copy rather than commentary.
  const CODE = maskOut(NATIVE, { comments: true });
  const sites = toastSites(CODE);

  // THE SWEEP MUST BE COMPLETE BEFORE IT MEANS ANYTHING. Three counts have to agree: the toast
  // objects the extractor found, the file's own `"toast"` literals (an independent count that does
  // not go through the extractor at all), and the six notices that exist today. A seventh notice, or
  // one written in a shape `type: "toast"` does not describe, breaks the agreement and reddens here
  // — instead of being skipped in silence the way three of the six were for weeks.
  const literalCount = (CODE.match(/(["'`])toast\1/g) || []).length;
  assert.equal(sites.length, literalCount,
    `the extractor found ${sites.length} toast sites but the source has ${literalCount} "toast" literals — ` +
    "a notice ships in a shape this scanner cannot see; teach it that shape before shipping it");
  assert.equal(sites.length, 6,
    `${sites.length} toast sites, not the 6 this sweep was written against — if a notice was added, ` +
    "confirm the extractor resolves its text and update this count; if one was deleted, say so here");
  const unread = sites.filter((s) => s.text === null).map((s) => `line ${s.line}`);
  assert.deepEqual(unread, [],
    `the scanner cannot read the text of ${unread.join(", ")} — an unread notice is an unchecked one, so teach the extractor that shape before shipping it`);
  // Directional words are the tell. Any of them means the sentence is describing the layout.
  const DIRECTIONAL = /\b(arriba|abajo|izquierda|derecha|esquina|superior|inferior)\b/i;
  for (const s of sites) {
    assert.doesNotMatch(s.text, DIRECTIONAL,
      `the notice at line ${s.line} (${s.how}) describes a position: "${s.text}" — carry the control instead, or it rots when the layout moves`);
  }
});

test("the resume notice carries a button, and that button opens the GATE", () => {
  // Losing the role unexpectedly is the moment someone most needs one tap, not a scavenger hunt.
  assert.match(NATIVE, /text: "Estabas dirigiendo cuando se cerró el app\.",\s*\n\s*action: "resume-director"/,
    "the crash-resume notice no longer carries its action");
  assert.match(NATIVE, /text: "Estabas transmitiendo cuando se cerró el app\.",\s*\n\s*action: "resume-director"/,
    "the relay-transmitter notice no longer carries its action");

  // It must open the gate, NOT take the role: the typed word is the only thing standing between a
  // flustered tap and a second director. request-director keeps exactly one call site.
  const handler = APP.slice(APP.indexOf('if (payload.type === "toast")'), APP.indexOf('if (payload.type === "diagnostics")'));
  assert.match(handler, /resume-director/, "the toast action is unwired");
  assert.match(handler, /requestRoleGate\(\)/, "the action does not open the gate");
  assert.doesNotMatch(handler, /postNativeBridge/, "the toast posts to the bridge directly, bypassing the gate");
  assert.equal((APP.match(/type: "request-director"/g) || []).length, 1,
    "a second request-director site exists — every extra one is an ungated hole through #role-gate");
});

test("a device still boots as a FOLLOWER — the notice informs, it does not promote", () => {
  // The seat stays empty until a hand takes it. Auto-promotion on a persisted role was tried between
  // 2026-08-05 and build 427 and each version could mint a second director beside a human's and win.
  // An empty seat is the choir on the last page; two directors is the choir split.
  assert.match(NATIVE, /\.finally\(\(\) => \{\s*\n\s*becomeFollower\(\);/,
    "the boot path no longer forces follower — a persisted role could promote this device");
  const boot = NATIVE.slice(NATIVE.indexOf("if (!didBootstrapRef.current)"), NATIVE.indexOf("const sub = addNearbyDirectorSyncListener"));
  assert.doesNotMatch(boot, /becomeDirector\(/, "the boot path can promote to director without a human");
});

test('Spanish copy uses "el app", never "la app"', () => {
  // House style, Miguel 2026-08-18, global. Not a grammar debate — "la app" is common usage
  // elsewhere and is still wrong in this product's voice. Cheap to enforce, easy to reintroduce
  // by habit, so it is pinned rather than remembered.
  const WEB = fs.readFileSync("web/src/app.js", "utf8");
  const SELFTEST = fs.readFileSync("web/src/lib/svSelftest.js", "utf8");
  for (const [name, src] of [["PdfReaderApp.tsx", NATIVE], ["web/src/app.js", WEB], ["svSelftest.js", SELFTEST]]) {
    assert.doesNotMatch(src, /\bla app\b/i, `${name} says "la app" — this product writes "el app"`);
  }
});
