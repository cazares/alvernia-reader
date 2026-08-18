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

// The cluster is DERIVED from four :root variables now, so this reads them and recomputes the same
// ladder the browser will. That is strictly stronger than the old literal arithmetic: changing a
// size in one place used to leave the offsets stale — which is exactly how ⌕ and Ir a Canto ended up
// 0.1rem apart, and the pill 0.15rem from ⟳, both on 2026-08-17.
const rootVar = (name) => {
  const m = CSS.match(new RegExp(`--${name}:\\s*([\\d.]+)rem`));
  assert.ok(m, `--${name} is not defined in :root — the ladder cannot be derived`);
  return Number(m[1]);
};
const GUTTER = rootVar("fab-gutter");
const GUTTER_TOP = rootVar("fab-gutter-top");
const SIZE = rootVar("fab-size");
const GAP = rootVar("fab-gap");
// Vestigial since 2026-08-18: the DIRECTOR status content-hugs around a lone ☆, so no position is
// derived from this width. Still read so the ladder fails loudly if someone re-introduces a fixed
// status width without re-deriving the offsets beside it.
const STATUS_W = rootVar("fab-status-w");

const fabWidth = () => SIZE;

// slot2 / slot3 are calc()s of the above; recompute rather than re-read, so a hand-edited calc that
// disagrees with the variables shows up as an overlap here.
const SLOT2 = SIZE + GAP;
// THERE IS NO SLOT 3 any more (2026-08-18). The empty-seat state was a text pill sitting third in
// the follower row; it is now the ⟳ button itself, crossed out. Both roles are two controls wide,
// which also retires the whole class of bug where slot 3 measured a control that had moved out of
// the row — it did exactly that for a day, floating "Nadie dirige" 2.33rem clear of its neighbour.

// Resolve a rule's horizontal offset. Now that the ladder is calc()s of :root variables, map the
// variable NAMES to the numbers above rather than parsing a literal that no longer exists.
const offsetOf = (selector) => {
  const lines = CSS.split("\n");
  let best = 0;
  for (let i = 0; i < lines.length; i++) {
    if (!lines[i].includes(selector)) continue;
    const block = lines.slice(i, i + 8).join("\n");
    const body = block.slice(0, block.indexOf("}") + 1 || undefined);
    const m = body.match(/(?:left|right):\s*calc\((.*)\)/);
    if (!m) continue;
    // Sum every term AFTER the gutter: the offsets are expressed as variables now, so resolve the
    // variables rather than pattern-matching one shape. A literal rem still counts.
    const tail = m[1].slice(m[1].indexOf(")") + 1);
    let n = 0;
    if (/var\(--fab-slot2\)/.test(tail)) n += SLOT2;
    if (/var\(--fab-slot3\)/.test(tail)) n += SLOT3;
    n += (tail.match(/var\(--fab-size\)/g) || []).length * SIZE;
    n += (tail.match(/var\(--fab-gap\)/g) || []).length * GAP;
    n += (tail.match(/var\(--fab-status-w\)/g) || []).length * STATUS_W;
    for (const lit of tail.match(/\+\s*([\d.]+)rem/g) || []) n += Number(lit.replace(/[^\d.]/g, ""));
    best = Math.max(best, n);
  }
  return best;
};

test("the corner cluster is uniformly spaced, by construction", () => {
  // Every neighbour pair must be exactly --fab-gap apart. This is the property the owner asked for
  // ("uniformly separated") and the one hand-computed offsets kept breaking.
  assert.equal(SLOT2 - SIZE, GAP, "slot 2 does not sit one gap after a control");
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

const span = (extraOffset, w) => ({ near: GUTTER + extraOffset, far: GUTTER + extraOffset + w });

test("the two DIRECTOR fabs (top-right ⌕ and ♪) never overlap", () => {
  const w = fabWidth();
  const search = span(0, w);                                              // ⌕ flush to the corner
  const songJump = span(offsetOf('[data-role="director"] .song-jump-fab'), w);
  const gap = songJump.near - search.far;
  assert.ok(gap >= 0, `⌕ and ♪ overlap by ${(-gap).toFixed(2)}rem`);
  // A zero gap is touching, not overlapping — still wrong for a thumb on a moving iPad.
  assert.ok(gap >= 0.25, `⌕ and ♪ are only ${gap.toFixed(2)}rem apart — too tight to tap reliably`);
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
// The overlap test above still passes, because offsetOf() takes the MAX offset across matching
// rules and the is-nobody PILL still carries its 5.15rem shift. That is correct but silent: without
// the assertions below, someone could delete the dot's placement entirely and every test would stay
// green. Pin the intent, not just the absence of a collision.
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
