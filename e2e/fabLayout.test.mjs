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

const PX_PER_REM = 16;   // the page never overrides the root font-size; px only appears as nudges

// Comments are stripped for VALUE parsing only. They carry braces-free prose today, but a brace or
// a stray `--fab-size` inside one would otherwise be read as code.
const CSS_NC = CSS.replace(/\/\*[\s\S]*?\*\//g, "");

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

// Split a rule body into its declarations, structurally: on top-level `;`, with parens counted so
// the commas and nested calls inside a calc()/max()/env() value can never split it.
//
// WHAT THE PREVIOUS VERSION MISSED: it read declarations with one regex per property whose match
// CONSUMED the terminating `;` — `/(?:^|;)\s*(left|right)\s*:\s*([^;]+);/g`. In a body with two
// consecutive declarations (`right: …; left: auto;`) the separator the second one needed had
// already been eaten, so only the FIRST was ever seen: the "last one wins" cascade below was a
// no-op, and a rule that set the same property twice read the value the browser discards.
const declarations = (body) => {
  const raw = [];
  let depth = 0, cur = "";
  for (const c of body) {
    if (c === "(") depth++;
    else if (c === ")") depth--;
    else if (c === ";" && depth === 0) { raw.push(cur); cur = ""; continue; }
    cur += c;
  }
  raw.push(cur);
  return raw.flatMap((d) => {
    const i = d.indexOf(":");
    if (i < 0) return [];
    const prop = d.slice(0, i).trim();
    if (!prop) return [];
    // Custom properties are case-SENSITIVE; regular properties are not.
    return [{ prop: prop.startsWith("--") ? prop : prop.toLowerCase(), value: d.slice(i + 1).trim() }];
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
const parseRules = (css, context = []) => {
  const out = [];
  let i = 0;
  while (i < css.length) {
    const open = css.indexOf("{", i);
    if (open < 0) break;
    const close = matchBrace(css, open);
    if (close < 0) break;
    const prelude = css.slice(i, open).trim();
    const body = css.slice(open + 1, close);
    if (prelude.startsWith("@")) {
      const at = (/^@([a-zA-Z-]+)/.exec(prelude) || [])[1];
      if (at && GROUP_AT_RULES.has(at.toLowerCase())) out.push(...parseRules(body, [...context, prelude]));
    } else {
      out.push({ selectors: prelude.split(",").map((s) => s.trim()).filter(Boolean), body, context });
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
    .flatMap((r) => declarations(r.body)
      .filter((d) => d.prop === `--${name}`)
      .map((d) => ({ value: d.value, context: r.context })));

// The single value of a ladder variable — or a loud failure. The ladder below is one set of
// numbers, so it is only derivable while each variable it touches has exactly one value that
// applies everywhere. A second declaration, at top level OR inside any condition, makes every
// number downstream of it wrong on some device, and this is the assertion that says so.
const varValue = (name) => {
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
const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// A control's resolved distance from the screen edge it is anchored to, in rem, for an element in a
// given role scope. Every rule whose selector ENDS in `key` targets this control, so all of them are
// considered: the unscoped ones first, then the ones scoped to `scope`, which outrank them exactly
// as an added attribute selector outranks nothing. Within a tier the last declaration wins, as the
// cascade does.
//
// WHAT THE PREVIOUS VERSION MISSED: it matched `rule.selectors.includes(selector)` — an exact string
// match — so any role-scoped override such as `html[data-role="director"] .search-fab { right: … }`
// was simply invisible, and a ⌕/♪ collision introduced that way stayed green. Anything that moves
// this control from a state the caller did not name, or from inside an @media condition, now fails
// loudly rather than being skipped: the numbers below describe ONE layout, and a control that sits
// somewhere else in some other state has to be re-derived there, not ignored here.
const edgeOffset = (key, scope) => {
  const tail = new RegExp(`(?:^|[\\s>+~])${escapeRe(key)}$`);
  const unscoped = [], scoped = [];
  let targeted = false;
  for (const rule of RULES) {
    const mine = rule.selectors.filter((s) => tail.test(s));
    if (!mine.length) continue;
    targeted = true;
    const edges = declarations(rule.body).filter((d) => d.prop === "left" || d.prop === "right");
    if (!edges.length) continue;
    assert.equal(rule.context.length, 0,
      `\`${mine.join(", ")}\` is repositioned inside ${rule.context.join(" / ")} — this test resolves ` +
      "one unconditional layout, so that condition needs its own derivation");
    if (mine.includes(key)) unscoped.push(...edges);
    else if (scope && mine.includes(`${scope} ${key}`)) scoped.push(...edges);
    else assert.fail(
      `\`${mine.join(", ")}\` moves ${key} in a state this test does not model (scope: ${scope || "none"}) ` +
      "— derive the ladder for that state too, or this comparison is about a layout the device never shows");
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
  const rule = (sel) => {
    const i = CSS.indexOf(sel);
    assert.ok(i > 0, `${sel} is gone`);
    return CSS.slice(i, CSS.indexOf("}", i) + 1);
  };
  assert.match(rule(".song-jump-fab { "), /right:\s*max\(var\(--fab-gutter\)/,
    "Ir a Canto left the top-right corner — that is a relearn for the whole choir, not an addition");
  assert.match(rule('html[data-role="director"] .song-jump-fab'), /right:\s*calc\(/,
    "the director's Ir a Canto no longer steps left of ⌕ — they will overlap in that corner");
  assert.match(rule(".search-fab {"), /right:\s*max\(var\(--fab-gutter\)/, "⌕ is no longer flush right");
  // ★ Ser Director sits one slot LEFT of Ir a Canto (owner moved it there 2026-08-18), sharing that
  // slot with ⌕ — which only a director sees, so they never collide.
  assert.match(rule(".become-director-pill {"), /right:\s*calc\([^;]*--fab-slot2/,
    "★ Ser Director is not one slot left of Ir a Canto — it must not push Ir a Canto out of the corner");
  assert.match(rule('html[data-role="director"] .director-mode-badge'), /left:\s*max\(var\(--fab-gutter\)/,
    "the ☆ status left flush-left, where ⟳ is hidden and the corner is free");

  // BOTH role buttons are SQUARE, like every other control in the row (owner, 2026-08-18: "try hard
  // to keep buttons square, scale contents to get them square when necessary"). They briefly
  // content-hugged; squareness won, and the TYPE shrinks to fit instead of the box growing — which
  // is why their font-size sits below Ir a Canto's even though they share its treatment.
  const shared = CSS.slice(CSS.indexOf(".become-director-pill,\nhtml[data-role=\"director\"] .director-mode-badge {"));
  const body = shared.slice(0, shared.indexOf("}") + 1);
  assert.ok(body.length > 20, "the shared role-button block is gone — the two can now drift apart");
  assert.match(body, /width:\s*var\(--fab-size\)/, "the role buttons are no longer --fab-size wide");
  assert.match(body, /height:\s*var\(--fab-size\)/, "the role buttons are no longer --fab-size tall");
  assert.match(body, /aspect-ratio:\s*1\s*\/\s*1/, "nothing pins squareness independently of the two lengths");
  // The word that sets the floor. If this ever exceeds Ir a Canto's size, "Director" overflows the
  // square — the exact reason these carry their own, smaller size.
  const roleSize = Number(body.match(/font-size:\s*([\d.]+)rem/)[1]);
  const irBlock = CSS.slice(CSS.indexOf(".song-jump-fab {\n"));
  const irSize = Number(irBlock.slice(0, irBlock.indexOf("}")).match(/font-size:\s*([\d.]+)rem/)[1]);
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
