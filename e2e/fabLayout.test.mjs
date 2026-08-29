import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const CSS = fs.readFileSync("web/src/styles.css", "utf8");

// Every top corner control is `position:fixed`, 4rem square, offset from its edge by the same
// gutter. Overlap is therefore pure arithmetic — and pure arithmetic is exactly the kind of thing
// that silently breaks when someone widens a fab or nudges a gutter, because nothing renders in CI.
//
// Two pairs share a corner:
//   top-LEFT   ⟳ resync fab  +  the sync pill      (follower / nobody states)
//   top-RIGHT  ⌕ search fab  +  ♪ song-jump fab    (director state)
//
// The left pair collided the moment the pill was made visible to followers: both sat at the same
// offset, and until then they could never be on screen together — the old badge showed only for the
// DIRECTOR, and ⟳ is hidden for exactly that role.

// The cluster is DERIVED from four :root variables now, so this reads them and works out the same
// ladder the browser will. That is strictly stronger than the old literal arithmetic: changing a
// size in one place used to leave the offsets stale — which is exactly how ⌕ and Ir a Canto ended up
// 0.1rem apart, and the pill 0.15rem from ⟳, both on 2026-08-17.
//
// WHAT THE PREVIOUS VERSION MISSED, and it was measured: it only ever pattern-matched the NAME
// `var(--fab-slot2)` and substituted the test's own JS constant `SIZE + GAP` for it. The calc() body
// of --fab-slot2 was never read, so rewriting it to `calc(var(--fab-size) - 2rem)` — which puts ♪
// 2.00rem INSIDE ⌕ on the director's iPad — left all fourteen tests green. Everything below now
// evaluates the stylesheet's own expressions: the variables, the calc()s, the max() against the
// safe-area inset. The numbers the browser resolves are the numbers under test.

// AND WHAT ROUND 3 FOUND, which is the reason for the shape of everything below. The hand-written
// resolvers were wrong in both directions at once: they missed real overlaps (an `!important`, an
// `#id` override, a variable redefined on an ancestor, a `@charset` line at the top of the file) and
// they invented false ones (a quote-style change inside the director's attribute selector). That
// pattern — false negatives and false positives from the same code — means the resolver was being
// asked a question it is not equipped to answer. A test file cannot become a browser, and every
// further CSS feature modelled by hand is another chance to be confidently wrong; a confidently
// wrong "this does not apply" reads exactly like a pass.
//
// So the resolvers below REFUSE rather than guess. Anything they cannot score soundly — !important,
// an id/attribute/pseudo-qualified selector, a functional pseudo, a selector inside a condition, a
// ladder variable declared anywhere but the one :root rule, a construct the parser does not model —
// fails loudly, naming the construct, the selector and the styles.css line. THE CEILING, stated once
// so nobody has to rediscover it: this file resolves a stylesheet, not a rendered page. What would
// actually settle any of these questions is a real browser computing the cascade over the real DOM —
// load web/src/index.html, set html[data-role], and read getComputedStyle(el).right. Until something
// does that, a refusal here is the honest answer and a green is only a claim about the two selector
// tiers named below.

const PX_PER_REM = 16;   // the page never overrides the root font-size; px only appears as nudges

// Comments are neutralised for VALUE parsing only. They carry braces-free prose today, but a brace
// or a stray `--fab-size` inside one would otherwise be read as code.
//
// Blanked rather than deleted — every character is replaced by a space and every newline kept — so
// an offset into CSS_NC is the same offset into CSS. That is what lets every refusal below name the
// styles.css LINE it is refusing about. A refusal nobody can locate is barely better than silence.
const CSS_NC = CSS.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, " "));
const lineOf = (idx) => CSS.slice(0, Math.max(0, idx)).split("\n").length;

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const matchBrace = (s, open) => {
  let depth = 0;
  for (let i = open; i < s.length; i++) {
    if (s[i] === "{") depth++;
    else if (s[i] === "}" && --depth === 0) return i;
  }
  return -1;
};
const matchParen = (s, open) => {
  let depth = 0;
  for (let i = open; i < s.length; i++) {
    if (s[i] === "(") depth++;
    else if (s[i] === ")" && --depth === 0) return i;
  }
  return -1;
};
const matchBracket = (s, open) => {
  let depth = 0;
  for (let i = open; i < s.length; i++) {
    if (s[i] === "[") depth++;
    else if (s[i] === "]" && --depth === 0) return i;
  }
  return -1;
};
// A selector LIST split on its real commas — the ones outside quotes, parens and brackets. Splitting
// on every comma tears `:is(.song-jump-fab, .x)` into two fragments that parse as neither selector,
// which turns a rule that genuinely moves a control into two pieces nobody scores.
const splitSelectorList = (prelude) => {
  const out = [];
  let cur = "", depth = 0, q = null;
  for (let k = 0; k < prelude.length; k++) {
    const c = prelude[k];
    if (q) { cur += c; if (c === "\\") cur += prelude[++k] ?? ""; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'") { q = c; cur += c; continue; }
    if (c === "(" || c === "[") depth++;
    else if (c === ")" || c === "]") depth--;
    else if (c === "," && depth === 0) { out.push(cur); cur = ""; continue; }
    cur += c;
  }
  out.push(cur);
  return out.map((s) => s.trim()).filter(Boolean);
};

// The first `;` between `from` and `to` that is not inside quotes, parens or brackets.
const topLevelSemi = (s, from, to) => {
  let depth = 0, q = null;
  for (let k = from; k < to; k++) {
    const c = s[k];
    if (q) { if (c === "\\") k++; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'") { q = c; continue; }
    else if (c === "(" || c === "[") depth++;
    else if (c === ")" || c === "]") depth--;
    else if (c === ";" && depth === 0) return k;
  }
  return -1;
};

// Split a rule body into its declarations, structurally: on top-level `;`, with parens counted so
// the commas and nested calls inside a calc()/max()/env() value can never split it.
//
// WHAT THE PREVIOUS VERSION MISSED: it read declarations with one regex per property whose match
// CONSUMED the terminating `;` — `/(?:^|;)\s*(left|right)\s*:\s*([^;]+);/g`. In a body with two
// consecutive declarations (`right: …; left: auto;`) the separator the second one needed had
// already been eaten, so only the FIRST was ever seen: the "last one wins" cascade below was a
// no-op, and a rule that set the same property twice read the value the browser discards.
//
// ROUND 3 found `!important` was never even tokenised: the flag was simply part of the value string,
// so an `!important` added to the unscoped `.song-jump-fab` rule — which in a browser beats the
// director-scoped override outright, putting ♪ exactly on top of ⌕ — left every test green. It is
// split off here so callers can REFUSE on it rather than resolve a cascade they cannot compute.
// `at` is the declaration's absolute offset in styles.css, for the same reason.
const declarations = (body, base = 0) => {
  const raw = [];
  let depth = 0, q = null, start = 0;
  for (let k = 0; k < body.length; k++) {
    const c = body[k];
    if (q) { if (c === "\\") k++; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'") q = c;
    else if (c === "(") depth++;
    else if (c === ")") depth--;
    else if (c === ";" && depth === 0) { raw.push({ text: body.slice(start, k), at: base + start }); start = k + 1; }
  }
  raw.push({ text: body.slice(start), at: base + start });
  return raw.flatMap(({ text, at }) => {
    const i = text.indexOf(":");
    if (i < 0) return [];
    const prop = text.slice(0, i).trim();
    if (!prop) return [];
    let value = text.slice(i + 1).trim();
    const bang = /!\s*important\s*$/i.exec(value);
    const important = Boolean(bang);
    if (bang) value = value.slice(0, bang.index).trim();
    // Custom properties are case-SENSITIVE; regular properties are not.
    return [{
      prop: prop.startsWith("--") ? prop : prop.toLowerCase(),
      value,
      important,
      at: at + (text.length - text.trimStart().length),
    }];
  });
};

// Conditional group rules hold ordinary style rules — :root overrides included — so the parser has
// to walk into them. @keyframes is deliberately NOT walked: its inner blocks are keyframe selectors
// (`0%`, `to`), not style rules, and reading them as such would invent selectors that do not exist.
const GROUP_AT_RULES = new Set(["media", "supports", "container", "layer", "scope"]);

// Every rule, as { selectors[], body, context[] }, where `context` is the chain of at-rule preludes
// the rule sits inside (empty for a top-level rule). Structural: preludes are read up to their own
// opening brace and bodies to the matching close, so an added declaration or a reflowed rule cannot
// shift anything out of a window.
//
// WHAT THE PREVIOUS VERSION MISSED: it dropped every prelude starting with `@` and never looked
// inside, so a @media block was invisible to the whole file. cssVar's message promised that a
// @media override would be caught, and it could not be: appending
// `@media (max-width: 400px) { :root { --fab-slot2: calc(var(--fab-size) - 3rem); } }` puts ♪ three
// rem INSIDE ⌕ on a phone, and all fourteen tests stayed green. styles.css already ships two @media
// blocks that redefine :root variables, so this is a live edit path.
//
// ROUND 3 broke it with one line of perfectly ordinary CSS: a STATEMENT at-rule. `@charset "UTF-8";`
// or `@import url(…);` or `@layer base, components;` ends in a semicolon, not a block, so the old
// loop read everything from the file start to the next `{` as one prelude and every rule after it
// was off by one — `--fab-gutter is declared 0 times in :root` and the whole ladder collapsed.
// Statement at-rules are consumed to their `;` here. A top-level `;` that is NOT one is refused
// loudly rather than skipped, because it means this parser is reading something it does not model.
const parseRules = (css, context = [], base = 0) => {
  const out = [];
  let i = 0;
  while (i < css.length) {
    const open = css.indexOf("{", i);
    if (open < 0) break;
    const semi = topLevelSemi(css, i, open);
    if (semi >= 0) {
      const stmt = css.slice(i, semi).trim();
      assert.ok(stmt.startsWith("@"),
        `styles.css:${lineOf(base + i)}: a top-level \`;\` that is not a statement at-rule ends ` +
        `"${stmt.slice(0, 60)}" — this parser does not model that construct, and reading past it ` +
        "would mis-attribute every rule after it. A real browser's CSS parser is what settles this.");
      i = semi + 1;
      continue;
    }
    const close = matchBrace(css, open);
    if (close < 0) break;
    const prelude = css.slice(i, open).trim();
    const body = css.slice(open + 1, close);
    if (prelude.startsWith("@")) {
      const at = (/^@([a-zA-Z-]+)/.exec(prelude) || [])[1];
      if (at && GROUP_AT_RULES.has(at.toLowerCase())) {
        out.push(...parseRules(body, [...context, prelude], base + open + 1));
      }
    } else {
      out.push({
        selectors: splitSelectorList(prelude),
        body,
        context,
        at: base + i,
        bodyAt: base + open + 1,
      });
    }
    i = close + 1;
  }
  return out;
};
const RULES = parseRules(CSS_NC);
assert.ok(RULES.length > 100, `only ${RULES.length} CSS rules parsed — the stylesheet did not parse`);
assert.ok(RULES.some((r) => r.context.length),
  "no rule was found inside an @media/@supports block — the parser stopped walking into them, and " +
  "a conditional override of a ladder variable would go unnoticed again");

// Every :root declaration of --<name>, from ANYWHERE in the stylesheet, each tagged with the
// at-rule context it sits in.
const declsOf = (name) =>
  RULES.filter((r) => r.selectors.includes(":root"))
    .flatMap((r) => declarations(r.body, r.bodyAt)
      .filter((d) => d.prop === `--${name}`)
      .map((d) => ({ value: d.value, context: r.context, important: d.important, at: d.at })));

// Every declaration of --<name> ANYWHERE in the stylesheet, as line numbers — read off the raw text
// rather than the rule tree, so it also sees the places the tree deliberately does not walk into.
const declLinesAnywhere = (name) => {
  const re = new RegExp(`(?:^|[;{}\\s])--${escapeRe(name)}\\s*:`, "g");
  const lines = [];
  for (let m; (m = re.exec(CSS_NC)); ) lines.push(lineOf(m.index));
  return lines;
};

// The single value of a ladder variable — or a loud failure. The ladder below is one set of
// numbers, so it is only derivable while each variable it touches has exactly one value that
// applies everywhere. A second declaration, at top level OR inside any condition, makes every
// number downstream of it wrong on some device, and this is the assertion that says so.
//
// ROUND 3: it only ever looked at :root. Custom properties INHERIT, so redefining --fab-slot2 on
// `main.app-shell` — the real ancestor of all three fabs, index.html:50 — moves the geometry on
// screen while the :root value this reads never changes. Every declaration in the file is counted
// now, and anything beyond the one :root declaration is refused rather than resolved: which one
// wins at the fab is a cascade-plus-inheritance question over the real DOM tree, and the only thing
// that answers it soundly is a real browser computing the cascade.
const varValue = (name) => {
  const everywhere = declLinesAnywhere(name);
  assert.equal(everywhere.length, 1,
    `--${name} is declared ${everywhere.length} times in styles.css (line${everywhere.length === 1 ? "" : "s"} ` +
    `${everywhere.join(", ") || "none"}) — this resolver reads the single :root declaration and nothing ` +
    "else, and custom properties inherit, so a redefinition on html/body/main.app-shell or any other " +
    "ancestor of the fabs changes the computed offset on the device while this test keeps reading " +
    ":root. Resolving which declaration reaches the fab needs a real browser computing the cascade " +
    "over the real DOM.");
  assert.doesNotMatch(CSS_NC, new RegExp(`@property\\s+--${escapeRe(name)}\\b`),
    `--${name} has an @property rule, which can give it an initial-value this resolver never reads`);

  const decls = declsOf(name);
  assert.equal(decls.length, 1,
    `--${name} is declared ${decls.length} times in :root` +
    (decls.length > 1
      ? ` (${decls.map((d) => d.context.length ? d.context.join(" / ") : "top level").join("; ")})`
      : "") +
    " — the ladder is only derivable while each variable has exactly one value everywhere, so a " +
    "conditional override has to be resolved per condition rather than ignored");
  assert.equal(decls[0].context.length, 0,
    `--${name} is declared only inside ${decls[0].context.join(" / ")} — outside that condition it ` +
    "falls back to nothing and every offset derived from it collapses");
  assert.ok(!decls[0].important,
    `--${name} carries !important at styles.css:${lineOf(decls[0].at)} — this resolver does not rank ` +
    "cascade tiers, so it cannot say what that beats; a real browser can");
  return decls[0].value;
};

// Evaluate a CSS length the way the browser would, in rem. Anything it does not understand is a
// hard failure, never a silent 0 — a resolver that shrugs at an unfamiliar expression is how a
// "derived" test quietly stops deriving anything.
const evalLength = (expr, seen = new Set()) => {
  const s = String(expr).trim();
  const fn = /^([a-zA-Z-]+)\s*\(/.exec(s);
  if (fn) {
    const open = s.indexOf("(");
    const close = matchParen(s, open);
    if (close === s.length - 1) {
      const inner = s.slice(open + 1, close);
      const args = [];
      let depth = 0, cur = "";
      for (const c of inner) {
        if (c === "(") depth++;
        if (c === ")") depth--;
        if (c === "," && depth === 0) { args.push(cur); cur = ""; continue; }
        cur += c;
      }
      args.push(cur);
      const name = fn[1].toLowerCase();
      if (name === "calc") return evalLength(inner, seen);
      if (name === "max") return Math.max(...args.map((a) => evalLength(a, seen)));
      if (name === "min") return Math.min(...args.map((a) => evalLength(a, seen)));
      if (name === "var") {
        const varName = args[0].trim().replace(/^--/, "");
        assert.ok(!seen.has(varName), `--${varName} is defined in terms of itself`);
        const decls = declsOf(varName);
        if (!decls.length) {
          assert.ok(args.length > 1, `--${varName} is used but never defined in :root`);
          return evalLength(args.slice(1).join(","), seen);
        }
        // varValue(), not "the last declaration wins": a variable this ladder actually reads must
        // have one value everywhere, and the check has to reach the ones reached INDIRECTLY too —
        // --fab-size is only ever touched through --fab-slot2's calc().
        return evalLength(varValue(varName), new Set([...seen, varName]));
      }
      // env(): modelled at its FALLBACK, i.e. a device with no notch. Every anchor wraps env() in a
      // max() against the gutter, and the inset only ever pushes a control FURTHER from the edge —
      // so the fallback is the tightest case, which is the one where controls collide.
      if (name === "env") return args.length > 1 ? evalLength(args.slice(1).join(","), seen) : 0;
      assert.fail(`cannot resolve CSS function ${name}() in "${s}" — teach this resolver, do not skip it`);
    }
  }
  // Top-level + / - terms.
  const terms = [];
  let depth = 0, cur = "", sign = 1;
  for (const c of s) {
    if (c === "(") depth++;
    if (c === ")") depth--;
    if (depth === 0 && (c === "+" || c === "-") && cur.trim() !== "") {
      terms.push({ sign, text: cur.trim() });
      cur = ""; sign = c === "+" ? 1 : -1;
      continue;
    }
    cur += c;
  }
  if (cur.trim()) terms.push({ sign, text: cur.trim() });
  if (terms.length > 1) return terms.reduce((n, t) => n + t.sign * evalLength(t.text, seen), 0);

  const leaf = terms.length ? terms[0] : { sign: 1, text: s };
  const num = /^(-?\d*\.?\d+)(rem|px)?$/.exec(leaf.text);
  assert.ok(num, `cannot resolve CSS length "${leaf.text}" (from "${s}")`);
  if (num[2] === "px") return leaf.sign * (Number(num[1]) / PX_PER_REM);
  assert.ok(num[2] === "rem" || Number(num[1]) === 0,
    `"${leaf.text}" has no unit — only 0 may be unitless here`);
  return leaf.sign * Number(num[1]);
};

const cssVar = (name) => evalLength(varValue(name));

const GUTTER = cssVar("fab-gutter");
const GUTTER_TOP = cssVar("fab-gutter-top");
const SIZE = cssVar("fab-size");
const GAP = cssVar("fab-gap");
// Vestigial since 2026-08-18: the DIRECTOR status content-hugs around a lone ☆, so no position is
// derived from this width. Still read so the ladder fails loudly if someone re-introduces a fixed
// status width without re-deriving the offsets beside it.
const STATUS_W = cssVar("fab-status-w");

const fabWidth = () => SIZE;

// slot2 is a calc() of the above — and it is READ, not recomputed, because a hand-edited calc that
// disagrees with the variables is precisely the regression this file exists to catch.
const SLOT2 = cssVar("fab-slot2");
// THERE IS NO SLOT 3 any more (2026-08-18). The empty-seat state was a text pill sitting third in
// the follower row; it is now the ⟳ button itself, crossed out. Both roles are two controls wide,
// which also retires the whole class of bug where slot 3 measured a control that had moved out of
// the row — it did exactly that for a day, floating "Nadie dirige" 2.33rem clear of its neighbour.

const near = (a, b, why) =>
  assert.ok(Math.abs(a - b) < 1e-9, `${why} — got ${a}rem, expected ${b}rem`);

const DIRECTOR = 'html[data-role="director"]';

// ── Selectors, read structurally rather than as strings ──────────────────────────────────────────
//
// ROUND 3 hit this resolver from both sides at once, which is the signature of a matcher that is
// guessing. It compared the scoped tier to the EXACT string `html[data-role="director"] .song-jump-fab`,
// so rewriting that selector with single quotes and a double space — byte-different, browser-identical
// — dropped the rule out of the scoped tier and reddened the file for nothing. And it collected only
// selectors ENDING literally in `.song-jump-fab`, so `#song-jump-trigger { right: … }` (the element's
// real id, index.html:144, specificity 10000) or `.song-jump-fab:not(.never)` moved the fab on the
// device and were invisible here.
//
// The repair is NOT more CSS modelling. Every extra construct scored by hand is another chance to be
// confidently wrong, and a confidently wrong "no match" is the exact defect above. So: selectors are
// broken into compounds and compounds into simple selectors, structurally, so that whitespace and
// quote style cannot matter; anything outside that small grammar is recorded as UNMODELLED, and a
// rule that moves this control through an unmodelled shape is REFUSED by name. A refusal says "look
// here"; silence says nothing at all.

// A selector as { compounds[], combinators[] }, or null if it cannot be tokenised.
const splitSelector = (sel) => {
  const tokens = [];
  let cur = "", depth = 0, q = null;
  const endCompound = () => { if (cur.trim()) { tokens.push({ t: "c", v: cur.trim() }); cur = ""; } };
  for (let k = 0; k < sel.length; k++) {
    const c = sel[k];
    if (q) { cur += c; if (c === "\\") cur += sel[++k] ?? ""; else if (c === q) q = null; continue; }
    if (c === '"' || c === "'") { q = c; cur += c; continue; }
    if (c === "(" || c === "[") { depth++; cur += c; continue; }
    if (c === ")" || c === "]") { depth--; cur += c; continue; }
    if (depth === 0 && /\s/.test(c)) {
      endCompound();
      if (tokens.length && tokens[tokens.length - 1].t === "c") tokens.push({ t: "k", v: " " });
      continue;
    }
    if (depth === 0 && (c === ">" || c === "+" || c === "~")) {
      endCompound();
      if (tokens.length && tokens[tokens.length - 1].t === "k") tokens[tokens.length - 1].v = c;
      else if (tokens.length) tokens.push({ t: "k", v: c });
      else return null;                      // a leading combinator: not a selector this reads
      continue;
    }
    cur += c;
  }
  endCompound();
  if (depth !== 0 || q) return null;
  while (tokens.length && tokens[tokens.length - 1].t === "k") tokens.pop();
  if (!tokens.length) return null;
  const compounds = [], combinators = [];
  for (let k = 0; k < tokens.length; k++) {
    if (k % 2 === 0) { if (tokens[k].t !== "c") return null; compounds.push(tokens[k].v); }
    else { if (tokens[k].t !== "k") return null; combinators.push(tokens[k].v); }
  }
  return { compounds, combinators };
};

// One compound (`html[data-role="director"]`, `.song-jump-fab:not(.x)`) split into the simple
// selectors this file is willing to score, plus everything it is NOT — which is what gets refused.
const parseCompound = (c) => {
  const out = { type: null, classes: [], ids: [], attrs: [], unmodelled: [] };
  let s = c, first = true;
  while (s.length) {
    let m;
    if (first && (m = /^(?:\*|[a-zA-Z][\w-]*)/.exec(s))) { out.type = m[0].toLowerCase(); s = s.slice(m[0].length); first = false; continue; }
    first = false;
    if ((m = /^\.[\w-]+/.exec(s))) { out.classes.push(m[0].slice(1)); s = s.slice(m[0].length); continue; }
    if ((m = /^#[\w-]+/.exec(s))) { out.ids.push(m[0].slice(1)); s = s.slice(m[0].length); continue; }
    if (s[0] === "[") {
      const end = matchBracket(s, 0);
      if (end < 0) { out.unmodelled.push(s); break; }
      const raw = s.slice(0, end + 1);
      // name, optional operator + quoted-or-bare value. A case-sensitivity flag (`i`/`s`) is NOT
      // modelled — matching rules differ per flag, so it is refused rather than guessed at.
      const a = /^\[\s*([\w-]+)\s*(?:([~^|$*]?=)\s*(?:"([^"]*)"|'([^']*)'|([^\]\s"']+))\s*([iIsS])?\s*)?\]$/.exec(raw);
      if (!a || a[6]) out.unmodelled.push(raw);
      else out.attrs.push(a[2] ? `[${a[1]}${a[2]}"${a[3] ?? a[4] ?? a[5]}"]` : `[${a[1]}]`);
      s = s.slice(end + 1);
      continue;
    }
    if (s[0] === ":") {
      const m2 = /^::?[\w-]+/.exec(s);
      if (!m2) { out.unmodelled.push(s); break; }
      let raw = m2[0];
      s = s.slice(m2[0].length);
      if (s[0] === "(") {
        const end = matchParen(s, 0);
        if (end < 0) { out.unmodelled.push(raw + s); break; }
        raw += s.slice(0, end + 1);
        s = s.slice(end + 1);
      }
      out.unmodelled.push(raw);
      continue;
    }
    out.unmodelled.push(s);
    break;
  }
  return out;
};

// A canonical form for a compound: order-independent, quote-independent, whitespace-independent. Two
// compounds a browser cannot tell apart produce the same string here, which is what stops a reformat
// of `html[data-role='director']  .song-jump-fab` from moving an assertion.
const canonCompound = (c) => {
  const p = parseCompound(c);
  return (p.type || "") +
    [...p.classes].sort().map((x) => `.${x}`).join("") +
    [...p.ids].sort().map((x) => `#${x}`).join("") +
    [...p.attrs].sort().join("") +
    [...p.unmodelled].sort().map((x) => `?${x}`).join("");
};
// The same, for a whole selector: combinators kept (a descendant is not a child), everything else
// normalised. null when it cannot be tokenised — callers refuse on that rather than assume no match.
const canonSelector = (sel) => {
  const s = splitSelector(sel);
  if (!s) return null;
  return s.compounds.map(canonCompound)
    .reduce((acc, c, i) => (i ? `${acc}${s.combinators[i - 1]}${c}` : c), "");
};

// The ids the real markup gives to the elements carrying a class. An id selector outranks anything
// scoped by attribute, so a stylesheet that positions the fab through its id has to be seen.
const HTML_SRC = fs.readFileSync("web/src/index.html", "utf8");
const idsForClass = (cls) => {
  const found = [];
  for (const m of HTML_SRC.matchAll(/<[a-zA-Z][^>]*>/g)) {
    const cl = /\sclass\s*=\s*"([^"]*)"/.exec(m[0]);
    if (!cl || !cl[1].trim().split(/\s+/).includes(cls)) continue;
    const id = /\sid\s*=\s*"([^"]*)"/.exec(m[0]);
    found.push(id ? id[1] : null);
  }
  return found;
};

// A control's resolved distance from the screen edge it is anchored to, in rem, for an element in a
// given role scope. Two tiers are scored, and ONLY two: the bare `key` rule, and `scope key`. The
// scoped tier outranks the unscoped one exactly as an added attribute selector outranks nothing, and
// within a tier the last declaration wins, as the cascade does.
//
// WHAT THE PREVIOUS VERSION MISSED: it matched `rule.selectors.includes(selector)` — an exact string
// match — so any role-scoped override such as `html[data-role="director"] .search-fab { right: … }`
// was simply invisible, and a ⌕/♪ collision introduced that way stayed green.
//
// Everything that is not one of those two tiers is now REFUSED by name, with its styles.css line:
// an `!important` (which beats both tiers regardless of specificity, and is how round 3 put ♪ exactly
// on top of ⌕ with all 22 assertions green), an id or attribute or pseudo-qualified selector, a
// selector inside an @media/@layer condition, a selector this file cannot tokenise at all. None of
// those can be scored soundly by hand, and a hand-rolled "no match" on one of them is a lie that
// reads as a pass. What would actually answer the question is a real browser computing the cascade
// on the real DOM — load the page and read getComputedStyle(el).right.
const edgeOffset = (key, scope) => {
  assert.match(key, /^\.[\w-]+$/, `edgeOffset only resolves a bare class selector, not \`${key}\``);
  const cls = key.slice(1);
  const carriers = idsForClass(cls);
  assert.ok(carriers.length,
    `no element in web/src/index.html carries class \`${cls}\` — it was renamed, or the control is ` +
    "injected at runtime, in which case this resolver cannot see the id an override could use");
  const ids = carriers.filter(Boolean);
  const mentions = (text) => text.includes(`.${cls}`) || ids.some((id) => text.includes(`#${id}`));
  // Could this selector's subject be our element? Class or id in the LAST compound — plus, because
  // a functional pseudo like `:is(.song-jump-fab, .x)` is deliberately not parsed, any unmodelled
  // chunk that so much as names the control. That errs toward refusing, which is the safe direction.
  const touches = (sel) => {
    const p = parseCompound(sel.compounds[sel.compounds.length - 1]);
    return p.classes.includes(cls) || p.ids.some((id) => ids.includes(id)) || p.unmodelled.some(mentions);
  };
  const isBareKey = (compound) => {
    const p = parseCompound(compound);
    return !p.type && !p.ids.length && !p.attrs.length && !p.unmodelled.length &&
      p.classes.length === 1 && p.classes[0] === cls;
  };
  const scopeCanon = scope ? canonCompound(scope) : null;
  if (scope) {
    const s = splitSelector(scope);
    assert.ok(s && s.compounds.length === 1, `the scope \`${scope}\` is not a single compound selector`);
  }

  const unscoped = [], scoped = [];
  let targeted = false;
  for (const rule of RULES) {
    const parsed = rule.selectors.map((raw) => ({ raw, sel: splitSelector(raw) }));
    const touching = parsed.filter((p) => p.sel && touches(p.sel));
    if (touching.length) targeted = true;
    const edges = declarations(rule.body, rule.bodyAt).filter((d) => d.prop === "left" || d.prop === "right");
    if (!edges.length) continue;
    // A rule that positions something and carries a selector this file cannot even tokenise, while
    // naming this control, is not a rule anyone may assume is irrelevant.
    const opaque = parsed.filter((p) => !p.sel && mentions(p.raw));
    assert.equal(opaque.length, 0,
      `styles.css:${lineOf(rule.at)}: \`${opaque.map((p) => p.raw).join(", ")}\` sets an edge and names ` +
      `${key}, but this file cannot tokenise that selector — a real browser computing the cascade is ` +
      "what would say whether it moves this control");
    if (!touching.length) continue;
    assert.equal(rule.context.length, 0,
      `\`${touching.map((p) => p.raw).join(", ")}\` (styles.css:${lineOf(rule.at)}) is repositioned inside ` +
      `${rule.context.join(" / ")} — this test resolves one unconditional layout, so that condition ` +
      "needs its own derivation");
    for (const d of edges) {
      assert.ok(!d.important,
        `styles.css:${lineOf(d.at)}: \`${touching.map((p) => p.raw).join(", ")}\` sets \`${d.prop}: ` +
        `${d.value} !important\` on ${key}. !important is a cascade ORIGIN, not specificity — it beats ` +
        `both tiers this file scores, including \`${scope || "(scoped)"} ${key}\`, so the offset it ` +
        "resolves would not be the offset on the device. Nothing short of a real browser computing " +
        "the cascade can rank these soundly; remove the !important or derive this layout in a browser.");
    }
    for (const p of touching) {
      const { compounds, combinators } = p.sel;
      const last = compounds[compounds.length - 1];
      if (compounds.length === 1 && isBareKey(last)) { unscoped.push(...edges); continue; }
      if (compounds.length === 2 && combinators[0] === " " && isBareKey(last) &&
          scopeCanon && canonCompound(compounds[0]) === scopeCanon) { scoped.push(...edges); continue; }
      assert.fail(
        `styles.css:${lineOf(rule.at)}: \`${p.raw}\` sets ${[...new Set(edges.map((e) => e.prop))].join("/")} ` +
        `on ${key}, and this file will not score it. It is neither the bare \`${key}\` rule nor ` +
        `\`${scope || "(no scope given)"} ${key}\`, and ranking an id, attribute, pseudo-class or ` +
        "extra-compound selector against those two is a specificity computation this file refuses to " +
        "guess at — a wrong 'this does not apply' is exactly how an overlap ships green. Derive the " +
        "ladder for that state in a real browser (getComputedStyle on the live element), or drop the " +
        "override.");
    }
  }
  assert.ok(targeted, `no rule targets \`${key}\` — it was renamed or deleted`);

  const anchor = {};
  for (const d of [...unscoped, ...scoped]) anchor[d.prop] = d.value;
  // `left: auto` beside `right: <length>` is a reset, not a second anchor — but exactly one of the
  // two must resolve to a length, or the control is not positioned the way this arithmetic assumes.
  const anchored = ["left", "right"].filter((e) => anchor[e] !== undefined && anchor[e] !== "auto");
  assert.equal(anchored.length, 1,
    `${key} resolves to ${anchored.length} edge anchors (${JSON.stringify(anchor)}) — exactly one of ` +
    "left/right must be a length for its distance from an edge to mean anything");
  return { edge: anchored[0], rem: evalLength(anchor[anchored[0]]) };
};

test("the corner cluster is uniformly spaced, by construction", () => {
  // Every neighbour pair must be exactly --fab-gap apart. This is the property the owner asked for
  // ("uniformly separated") and the one hand-computed offsets kept breaking. SLOT2 is the
  // stylesheet's own calc() evaluated, so this compares the ladder against its primitives instead of
  // comparing the test's arithmetic against itself.
  near(SLOT2, SIZE + GAP,
    `--fab-slot2 does not resolve to one control plus one gap (--fab-size ${SIZE}rem + --fab-gap ${GAP}rem)`);
  assert.ok(GAP >= 0.25, `gap ${GAP}rem is too tight to read as separate controls`);
  assert.ok(GUTTER >= 0.75, `gutter ${GUTTER}rem crowds the screen edge`);
});

test("the derived offsets are what the stylesheet actually uses", () => {
  // A literal rem offset creeping back in is the regression this whole change exists to stop.
  const from = CSS.indexOf(".song-jump-fab,"), to = CSS.indexOf("/* ── Role-based controls");
  // Both anchors must EXIST. The previous version sliced from ".sync-exit-fab {", which was deleted
  // with the ✕ — indexOf returned -1, the slice came back empty, and the assertion below passed
  // against "" on every run. Pin the anchors so a rename fails loudly instead of going quiet.
  assert.ok(from > 0 && to > from, "the cluster block anchors moved — this test was scanning nothing");
  const cluster = CSS.slice(from, to);
  assert.doesNotMatch(cluster, /\+\s*\d+(\.\d+)?rem\)/,
    "a hard-coded rem offset is back in the cluster — derive it from --fab-size/--fab-gap instead");
});

// A control's horizontal extent, measured inward from the edge it is anchored to. Both director
// fabs are right-anchored, so both are measured from the right edge and compared directly.
const span = (offsetRem, w) => ({ near: offsetRem, far: offsetRem + w });

test("the two DIRECTOR fabs (top-right ⌕ and ♪) never overlap", () => {
  const w = fabWidth();
  // Neither position is assumed any more. Both are read out of the stylesheet and resolved — which
  // is the whole repair: the old version hard-coded ⌕ at the gutter and substituted its own
  // SIZE + GAP for ♪'s offset, so rewriting --fab-slot2's calc() to `var(--fab-size) - 2rem` moved ♪
  // 2.00rem on top of ⌕ on a real iPad while this test went on computing a comfortable 0.50rem.
  // Both are resolved IN THE DIRECTOR'S STATE, which is the only state where these two are on
  // screen together — so a director-scoped override of either one is part of the answer instead of
  // being invisible to it.
  const searchAt = edgeOffset(".search-fab", DIRECTOR);
  const songAt = edgeOffset(".song-jump-fab", DIRECTOR);
  assert.equal(searchAt.edge, "right", "⌕ is no longer right-anchored — this comparison assumes one edge");
  assert.equal(songAt.edge, "right", "the director's ♪ is no longer right-anchored");

  const search = span(searchAt.rem, w);
  const songJump = span(songAt.rem, w);
  const gap = songJump.near - search.far;
  assert.ok(gap >= 0,
    `⌕ and ♪ overlap by ${(-gap).toFixed(2)}rem — ⌕ spans ${search.near.toFixed(2)}–${search.far.toFixed(2)}rem ` +
    `from the right edge and ♪ starts at ${songJump.near.toFixed(2)}rem`);
  // A zero gap is touching, not overlapping — still wrong for a thumb on a moving iPad.
  assert.ok(gap >= 0.25, `⌕ and ♪ are only ${gap.toFixed(2)}rem apart — too tight to tap reliably`);
  // And the separation must be the ladder's gap, not merely "some clearance". A slot that drifts
  // wider is the same bug as one that drifts narrower: the row stops reading as evenly spaced.
  near(gap, GAP, "⌕ and ♪ are not exactly one --fab-gap apart");
});

test("the follower row is exactly two controls — ⟳ and Ir a Canto", () => {
  // WHAT REPLACED THE OVERLAP TESTS. Two tests here used to check that a text pill beside ⟳ did not
  // collide with it, and then that it did not float clear of it either. Neither can happen now
  // because no such pill exists in any state: the DIRECTOR's status owns slot 1 (⟳ is hidden for
  // that role), a following follower is a DOT ON the fab, and an empty seat is the fab crossed out.
  // Asserting on a pill that cannot render would be a test that can never fail.
  assert.match(CSS, /html\[data-role="follower"\] \.director-mode-badge\.is-nobody \{[^}]*display:\s*none/,
    "the empty-seat pill is back in the follower row — the crossed-out ⟳ is the indicator now");
  assert.doesNotMatch(CSS, /--fab-slot3/,
    "slot 3 is back; the follower row is two controls, and a third slot is how the ladder drifted before");
});

test("nobody-directing crosses out the ⟳, in shape and not only in colour", () => {
  // The property worth pinning is that this survives without colour. Red/green is the commonest
  // colour blindness, the loft is a distance read, and the 10px dot this replaced was a poor alarm.
  // A 62px cross is none of those things — but only while it is actually drawn as a cross.
  const block = CSS.slice(CSS.indexOf('html[data-mesh="nobody"] .resync-fab::after'));
  const body = block.slice(0, block.indexOf("}") + 1);
  assert.match(body, /to bottom right/, "the ⟳ has no top-left→bottom-right diagonal");
  assert.match(body, /to bottom left/, "the ⟳ has no bottom-left→top-right diagonal — that is a slash, not an X");
  assert.match(body, /pointer-events:\s*none/,
    "the X eats the tap, so the button under it can no longer open its own explanation");
  // Driven by the mesh verdict, not by a class someone might set independently of it.
  assert.match(CSS, /html\[data-mesh="nobody"\] \.resync-fab \{/,
    "the crossed-out state is not driven by data-mesh — it can then disagree with the pill");
});

test("the director keeps the pill flush left, where ⟳ is hidden", () => {
  // ⟳ is display:none for the director, so shifting the pill there would just waste the corner.
  assert.match(CSS, /html\[data-role="director"\] \.resync-fab \{ display: none/,
    "⟳ is no longer hidden for the director — the pill must then clear it in BOTH roles");
  const line = CSS.split("\n").find((l) => l.includes('[data-role="director"] .director-mode-badge'));
  assert.ok(!line || !/calc\(/.test(line), "the director's pill is being shifted for a fab that is not there");
});

// ── The follower DOT sits ON the ⟳ fab, deliberately ──────────────────────────
//
// Owner, 2026-08-17: "the green dot should be 30% smaller and it should be *part of* the resync
// button, upper left of the resync button." So the dot now overlaps ⟳ on purpose — it is a badge on
// that button meaning "this button's link is alive", not a separate indicator.
//
// The overlap test above says nothing about the dot: it measures the two right-anchored DIRECTOR
// fabs, and the dot is a left-anchored follower badge. Correct, but silent — without the assertions
// below, someone could delete the dot's placement entirely and every test would stay green. Pin the
// intent, not just the absence of a collision.
test("the follower dot is a badge ON the ⟳ fab, not a control beside it", () => {
  const rule = CSS.slice(CSS.indexOf('html[data-role="follower"] .director-mode-badge.is-following'));
  const body = rule.slice(0, rule.indexOf("}"));

  const off = body.match(/left:\s*calc\([^;]*?\+\s*([\d.]+)rem/);
  assert.ok(off, "the dot must set its own left, or it inherits the pill's 5.15rem and floats away");
  const inset = Number(off[1]);
  const w = fabWidth();
  assert.ok(inset > 0 && inset < w,
    `the dot's ${inset}rem inset puts it outside the ${w}rem fab — it is meant to sit on its corner`);

  const size = body.match(/width:\s*([\d.]+)rem/);
  assert.ok(size, "the dot must declare a width");
  // 0.75rem was the old size; the owner asked for 30% smaller. Guard the band, not the exact value,
  // so a deliberate nudge does not fail while a reversion does.
  const d = Number(size[1]);
  assert.ok(d >= 0.45 && d <= 0.60, `dot is ${d}rem — expected ~0.525rem (30% down from 0.75rem)`);

  // It must draw ABOVE the fab it sits on, or the badge is hidden behind the button.
  const badge = CSS.slice(CSS.indexOf(".director-mode-badge {"));
  const badgeZ = Number((badge.slice(0, badge.indexOf("}")).match(/z-index:\s*(\d+)/) || [])[1]);
  const fabBlock = CSS.slice(CSS.indexOf(".search-fab,"), CSS.indexOf(".resync-fab {"));
  const fabZ = Number((fabBlock.match(/z-index:\s*(\d+)/) || [])[1]);
  assert.ok(badgeZ > fabZ, `dot z-index ${badgeZ} must beat the fab's ${fabZ} or it renders underneath`);
});

test("the top fabs are square, and stay square if padding is ever added", () => {
  const block = CSS.slice(CSS.indexOf(".search-fab,"), CSS.indexOf(".search-fab {"));
  // Both axes now read the SAME variable, which is a stronger guarantee than two literals that
  // happened to match — they cannot drift apart at all.
  assert.match(block, /width:\s*var\(--fab-size\)/, "fab width is not --fab-size");
  assert.match(block, /height:\s*var\(--fab-size\)/, "fab height is not --fab-size");
  // width==height only stays true under content-box if nothing gains padding or a border later.
  assert.match(block, /box-sizing:\s*border-box/, "border-box is what keeps it square under padding");
  assert.match(block, /aspect-ratio:\s*1\s*\/\s*1/, "aspect-ratio pins squareness independently");
});


test("the sync pill is NOT inside a stacking context that outranks the fabs", () => {
  // THE BUG THIS EXISTS FOR (found 2026-08-18, shipped for weeks). #viewer-shell sets both
  // `isolation: isolate` and `contain: content`, each of which creates a stacking context. While
  // the pill lived inside that SECTION, its z-index:46 was scoped INSIDE it — and #viewer-shell
  // itself sits at z-index:auto, so the ⟳ fab's z-index:45, a sibling of the SECTION, painted above
  // the entire subtree. The follower's green "you are synced" dot was rendering BEHIND the button
  // it is a badge on. It looked like a faint tint, not a missing dot, which is why it survived.
  //
  // A comment in the CSS asserted that `position: fixed` had fixed this. It had not: `fixed`
  // escapes normal-flow positioning, never an ancestor's stacking context. Only moving out does.
  // z-index alone cannot be asserted here — the numbers were already correct and still lost.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  const shellStart = HTML.indexOf('<section class="viewer-shell"');
  const shellEnd = HTML.indexOf("</section>", shellStart);
  assert.ok(shellStart > 0 && shellEnd > shellStart, "#viewer-shell moved — re-derive this test");
  const insideShell = HTML.slice(shellStart, shellEnd);
  assert.ok(!insideShell.includes('id="director-mode-badge"'),
    "the sync pill is back inside #viewer-shell, whose isolation/contain traps its z-index below the fabs");

  // And the trap itself is still there, so the constraint above is still load-bearing. If someone
  // removes isolation/contain, this test should be re-read rather than silently kept.
  const shellCss = CSS.slice(CSS.indexOf(".viewer-shell {"));
  const shellBody = shellCss.slice(0, shellCss.indexOf("}"));
  assert.ok(/isolation:\s*isolate/.test(shellBody) || /contain:\s*(content|paint|strict)/.test(shellBody),
    "#viewer-shell no longer creates a stacking context — this guard may be obsolete; re-read it");
});


test("ADDITIVE: Ir a Canto stays top-RIGHT, and the new controls take empty corners", () => {
  // THE PROPERTY THE OWNER CARES MOST ABOUT (2026-08-18): "this is to make all these changes
  // additive not scramble shit up and confuse the fuck out of everyone". Six people already know
  // where these buttons are. Ir a Canto spent 2026-08-17 in the top-LEFT cluster and moving it back
  // is the whole point — a rearrangement costs more than any layout gains.
  //
  //   FOLLOWER : ⟳(L1)          …    ★ Ser Director(R2) · Ir a Canto (flush R)
  //   DIRECTOR : ☆(L1)          …        Ir a Canto(R2) · ⌕ (flush R)
  //
  // Ir a Canto is flush right in BOTH roles and never moves. Everything added sits one slot to its
  // left, in space that was empty, and the two occupants of that slot (★ Ser Director for a
  // follower, ⌕ for a director) are never on screen together. Same for left slot 1: ⟳ for a
  // follower, the ☆ status for a director.
  // ROUND 3 found the same false positive here as in edgeOffset, one layer down: this looked its
  // rules up with CSS.indexOf on a literal that had to include the brace and the exact spacing, so
  // rewriting `html[data-role="director"] .song-jump-fab` with single quotes and a double space —
  // byte-different, browser-identical — reddened this test over nothing. The lookup is structural
  // now: every rule whose selector list canonically contains this selector, bodies joined on `;` so
  // no `[^;]*` can match across a boundary. That is also strictly stronger, because an anchor moved
  // into the shared `.song-jump-fab, .search-fab, …` block still counts as declared.
  const rule = (sel) => {
    const want = canonSelector(sel);
    assert.ok(want, `\`${sel}\` is not a selector this file can read structurally`);
    const bodies = RULES.filter((r) => r.selectors.some((x) => canonSelector(x) === want)).map((r) => r.body);
    assert.ok(bodies.length, `${sel} is gone`);
    return bodies.join(";");
  };
  assert.match(rule(".song-jump-fab"), /right:\s*max\(var\(--fab-gutter\)/,
    "Ir a Canto left the top-right corner — that is a relearn for the whole choir, not an addition");
  assert.match(rule('html[data-role="director"] .song-jump-fab'), /right:\s*calc\(/,
    "the director's Ir a Canto no longer steps left of ⌕ — they will overlap in that corner");
  assert.match(rule(".search-fab"), /right:\s*max\(var\(--fab-gutter\)/, "⌕ is no longer flush right");
  // ★ Ser Director sits one slot LEFT of Ir a Canto (owner moved it there 2026-08-18), sharing that
  // slot with ⌕ — which only a director sees, so they never collide.
  assert.match(rule(".become-director-pill"), /right:\s*calc\([^;]*--fab-slot2/,
    "★ Ser Director is not one slot left of Ir a Canto — it must not push Ir a Canto out of the corner");
  assert.match(rule('html[data-role="director"] .director-mode-badge'), /left:\s*max\(var\(--fab-gutter\)/,
    "the ☆ status left flush-left, where ⟳ is hidden and the corner is free");

  // BOTH role buttons are SQUARE, like every other control in the row (owner, 2026-08-18: "try hard
  // to keep buttons square, scale contents to get them square when necessary"). They briefly
  // content-hugged; squareness won, and the TYPE shrinks to fit instead of the box growing — which
  // is why their font-size sits below Ir a Canto's even though they share its treatment.
  // Located structurally, for the same reason as `rule()` above: the previous version searched for
  // the literal `".become-director-pill,\nhtml[data-role=\"director\"] .director-mode-badge {"`,
  // newline and all, so putting the two selectors on one line would have said "the shared block is
  // gone" about a block that had not moved.
  const sharedRule = RULES.find((r) =>
    r.selectors.some((x) => canonSelector(x) === canonSelector(".become-director-pill")) &&
    r.selectors.some((x) => canonSelector(x) === canonSelector('html[data-role="director"] .director-mode-badge')));
  assert.ok(sharedRule, "the shared role-button block is gone — the two can now drift apart");
  const body = sharedRule.body;
  assert.match(body, /width:\s*var\(--fab-size\)/, "the role buttons are no longer --fab-size wide");
  assert.match(body, /height:\s*var\(--fab-size\)/, "the role buttons are no longer --fab-size tall");
  assert.match(body, /aspect-ratio:\s*1\s*\/\s*1/, "nothing pins squareness independently of the two lengths");
  // The word that sets the floor. If this ever exceeds Ir a Canto's size, "Director" overflows the
  // square — the exact reason these carry their own, smaller size.
  //
  // Ir a Canto's size is the LAST font-size declared across every rule that selects `.song-jump-fab`
  // — the shared fab block sets 4.65rem and the typography block after it sets 1.03rem, both one
  // class deep, so source order decides, exactly as the stylesheet's own comment says. Reading it
  // this way is also what stops `CSS.indexOf(".song-jump-fab {\n")` from landing on whichever rule
  // happens to be formatted with a newline first — which, when the base rule was reflowed onto
  // several lines, made this test CRASH on a null match rather than fail on anything real.
  const remOf = (sel, prop) => {
    const want = canonSelector(sel);
    const hits = RULES.filter((r) => r.selectors.some((x) => canonSelector(x) === want))
      .flatMap((r) => declarations(r.body, r.bodyAt).filter((d) => d.prop === prop));
    assert.ok(hits.length, `no \`${prop}\` is declared on \`${sel}\``);
    const last = hits[hits.length - 1];
    const m = /^([\d.]+)rem$/.exec(last.value);
    assert.ok(m, `\`${sel}\` sets \`${prop}: ${last.value}\` at styles.css:${lineOf(last.at)}, which is ` +
      "not a plain rem — this comparison only reads plain rem, and a browser is what resolves anything else");
    return Number(m[1]);
  };
  const roleSize = Number(body.match(/font-size:\s*([\d.]+)rem/)[1]);
  const irSize = remOf(".song-jump-fab", "font-size");
  assert.ok(roleSize < irSize,
    `role type ${roleSize}rem >= Ir a Canto's ${irSize}rem — "Director" is longer than "Canto" and will overflow`);
});

test("the two role controls never share the screen", () => {
  // ★ Ser Director is follower-only and the DIRECTOR status is director-only. If both could render,
  // the left cluster would hold two role controls saying different things about the same role.
  assert.match(CSS, /html\[data-shell="native"\]\[data-role="follower"\] \.become-director-pill \{ display: flex/,
    "★ Ser Director is not gated to a follower in the native shell — it can show for a director, or on the public web");
  assert.match(CSS, /html\[data-role="director"\] \.resync-fab \{ display: none/,
    "⟳ shows for a director again — it would then collide with the DIRECTOR status in left slot 1");
});


test("the top edge and the side edges are independently tunable", () => {
  // SPLIT ON 2026-08-18. One --fab-gutter drove top, left AND right, so "nudge the row up 5px" also
  // pulled both clusters horizontally inward — a change to one axis silently moved the other. Every
  // top: anchor reads --fab-gutter-top and every left:/right: anchor reads --fab-gutter.
  assert.ok(GUTTER_TOP > 0 && GUTTER > 0, "a gutter went to zero — controls would touch the bezel");
  assert.doesNotMatch(CSS, /top:\s*max\(var\(--fab-gutter\)/,
    "a top: anchor is back on --fab-gutter — raising the row will drag the clusters sideways again");
  // THE SAFE-AREA INSET MUST NOT BE CLAMPED — reverted 2026-08-18, same day, after a real device
  // regression. A clamp was tried here first: capping the inset's contribution on the theory that
  // cross-OS placement differences meant it was "winning by an unbounded amount". Confirmed on
  // mPad running this exact build in TestFlight: with the clamp, Salir Director rendered UNDER the
  // status bar / TestFlight banner, clipped and half-hidden. A safe-area inset exists so content
  // clears exactly that case; capping it reintroduces the overlap it was already preventing, in
  // exchange for fixing a merely cosmetic inconsistency. Pin the uncapped form so the clamp cannot
  // quietly return.
  assert.doesNotMatch(CSS, /min\(env\(safe-area-inset-top/,
    "the safe-area inset is capped again — confirmed on device to clip content under the status bar");
  assert.match(CSS, /top:\s*max\(var\(--fab-gutter-top\), env\(safe-area-inset-top, 0px\)\)/,
    "the top anchor is not the uncapped max() form — safe-area protection is weakened or gone");
  // Every fixed top control must share ONE top edge, or the row stops reading as a band.
  const tops = [...CSS.matchAll(/top:\s*max\(var\((--fab-gutter[a-z-]*)\)/g)].map((m) => m[1]);
  assert.ok(tops.length >= 3, `only ${tops.length} top anchors found — the cluster shrank or a selector moved`);
  assert.equal(new Set(tops).size, 1, `top anchors disagree: ${[...new Set(tops)].join(", ")}`);
});

test("★ Ser Director keeps the star on line 1, which is what makes it narrow", () => {
  // Owner, 2026-08-18: the star rides beside "Ser" rather than standing left of the whole stack.
  // Line 1 is the SHORT line, so the star fills width that was already empty instead of adding a
  // column — the pill went 103px -> 82px on that change alone. A future tidy that pulls the star
  // back out of .become-director-l1 silently pays that width back.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  const l1 = HTML.slice(HTML.indexOf('class="become-director-l1"'));
  assert.match(l1.slice(0, l1.indexOf("</span>") + 7), /become-director-star/,
    "the star left line 1 — it now needs its own column and the pill gets wider again");
  const rule = CSS.slice(CSS.indexOf(".become-director-l1 {"));
  assert.match(rule.slice(0, rule.indexOf("}")), /display:\s*flex/,
    "line 1 is not a flex row, so the star and Ser will not sit on one baseline");
});

test("the whole corner cluster hides together, not most of it", () => {
  // This listed only ♪ and ⌕, so opening the search dropdown left the role controls and ⟳ floating
  // over a panel that covers the page. Worse than untidy: the sync status is a CONTROL now — tapping
  // it leaves the role — so a stray tap while reading search results drops the choir's director.
  //
  // Asserted as a SET rather than a line, because the failure mode is omission: every control added
  // later is hidden only if someone remembers to come back here.
  const rule = CSS.slice(CSS.indexOf("body.sv-drawer-open"));
  const block = rule.slice(0, rule.indexOf("}") + 1);
  for (const sel of [".song-jump-fab", ".search-fab", ".resync-fab",
                     ".become-director-pill", ".director-mode-badge"]) {
    assert.ok(block.includes(`body.sv-drawer-open ${sel}`),
      `${sel} stays visible over the drawer — the cluster must hide as one`);
  }
  assert.match(block, /display: none !important/, "the drawer no longer hides the cluster at all");
});
