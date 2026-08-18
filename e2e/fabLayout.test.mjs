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

const GUTTER = 0.55; // max(0.55rem, env(safe-area-inset-*)) — the safe-area term only grows it
const rem = (s) => Number(s);

const fabWidth = () => {
  const block = CSS.slice(CSS.indexOf(".search-fab,"), CSS.indexOf(".search-fab {"));
  const m = block.match(/width:\s*([\d.]+)rem/);
  assert.ok(m, "could not read the shared fab width");
  return rem(m[1]);
};

// `right: calc(max(0.55rem, …) + Xrem)` → the fab's near edge sits X + gutter from the corner.
// Reads the rule whether it is written on one line or spread over several — the first version only
// checked the selector's own line, so a multi-line rule read as "no offset" and the test claimed an
// overlap that did not exist. A layout test that cannot parse the layout is worse than none.
const offsetOf = (selector) => {
  // Scans EVERY rule whose selector matches, not just the first. Build 435 added a second rule for
  // the same selector (`...director-mode-badge.is-following`, which collapses a follower's pill to a
  // dot) ABOVE the rule carrying the horizontal offset — so first-match returned the dot's block,
  // found no `+ Xrem`, and reported a phantom overlap with ⟳. It also only reads left/right, since
  // a VERTICAL calc() in a sibling rule is not a horizontal offset and must never be read as one.
  const lines = CSS.split("\n");
  let best = 0;
  for (let i = 0; i < lines.length; i++) {
    if (!lines[i].includes(selector)) continue;
    const block = lines.slice(i, i + 8).join("\n");
    const body = block.slice(0, block.indexOf("}") + 1 || undefined);
    const m = body.match(/(?:left|right):\s*calc\([^;]*?\+\s*([\d.]+)rem/);
    if (m) best = Math.max(best, rem(m[1]));
  }
  return best; // no calc() → flush against the gutter
};

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

test("the top-LEFT ⟳ fab and the sync pill never overlap", () => {
  const w = fabWidth();
  const resync = span(0, w);                                              // ⟳ flush to the corner
  const pill = span(offsetOf('[data-role="follower"] .director-mode-badge'), w * 0); // pill starts here
  const gap = pill.near - resync.far;
  assert.ok(gap >= 0, `the pill sits ${(-gap).toFixed(2)}rem UNDER the ⟳ fab`);
  assert.ok(gap >= 0.25, `the pill is only ${gap.toFixed(2)}rem from ⟳`);
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
  const block = CSS.slice(CSS.indexOf(".search-fab,"), CSS.indexOf(".resync-fab {"));
  const w = block.match(/width:\s*([\d.]+)rem/);
  const h = block.match(/height:\s*([\d.]+)rem/);
  assert.ok(w && h && w[1] === h[1], `fab is ${w?.[1]}x${h?.[1]}rem — not square`);
  // width==height only stays true under content-box if nothing gains padding or a border later.
  assert.match(block, /box-sizing:\s*border-box/, "border-box is what keeps it square under padding");
  assert.match(block, /aspect-ratio:\s*1\s*\/\s*1/, "aspect-ratio pins squareness independently");
});
