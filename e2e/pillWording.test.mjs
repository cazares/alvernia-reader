import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const CSS = fs.readFileSync("web/src/styles.css", "utf8");

// EXECUTE the pill's render decision. A presence check for `NATIVE_FILE_MODE || hasNativeBridge()`
// passes even when the early return that USES it is deleted — the const stays behind and satisfies
// the regex while the pill renders on signovivo.com anyway. Measured: that mutation slipped past.
const renderPill = (nativeFileMode, hasBridge, role, meshStatus = "", lastPageAgoMs = 1e9) => {
  const start = APP.indexOf("const SYNC_PILL = {");
  const end = APP.indexOf("// ── Sync \"working\"");
  const body = APP.slice(start, end);
  let hidden = null;
  // setAttribute on BOTH: renderDirectorModeBadge now also relabels #resync-fab, because while
  // nobody directs the pill is display:none and the crossed-out ⟳ is the only thing on screen — so
  // the announcement has to live there or the state goes silent to a screen reader. The fallback
  // stub stands in for that button, and a bare {textContent} threw "setAttribute is not a function".
  const titleEl = { textContent: "", setAttribute() {} };
  const actionEl = { textContent: "", setAttribute() {} };
  const badge = {
    classList: {
      remove: (...c) => { if (c.includes("is-hidden")) hidden = false; },
      add: (c) => { if (c === "is-hidden") hidden = true; },
    },
    setAttribute() {},
  };
  new Function("NATIVE_FILE_MODE", "hasNativeBridge", "state", "directorModeBadge", "document", "Date",
    `${body}; renderDirectorModeBadge();`,
  )(nativeFileMode, () => hasBridge,
    { nativeSyncRole: role },
    badge,
    { documentElement: { dataset: {} }, getElementById: (id) => {
        if (id === "sync-pill-title") return titleEl;
        if (id === "role-toggle") return { classList: { toggle() {} } };
        if (id === "role-toggle-label") return { textContent: "" };
        // The directing state is now shown by a SEPARATE ✕ control and an inert DIRECTOR status,
        // rather than by relabelling one button. Stub them so this file keeps testing the WORDING.
        if (id === "role-exit-x" || id === "role-status-pill" || id === "sync-exit-fab"
            || id === "become-director-fab" || id === "role-gate" || id === "role-gate-input"
            || id === "role-gate-confirm") {
          return { classList: { toggle() {}, remove() {}, add() {} }, dataset: {}, addEventListener() {} };
        }
        return actionEl;
      } },
    { now: () => lastPageAgoMs });
  return { hidden, title: titleEl.textContent, action: actionEl.textContent };
};

test("the pill never renders on the public web", () => {
  // No mesh there, no role to take — it would describe a room the viewer is not in.
  assert.equal(renderPill(false, false, "follower").hidden, true, "the pill RENDERED on signovivo.com");
  assert.equal(renderPill(true, false, "follower").hidden, false, "the pill is hidden inside the shell");
  assert.equal(renderPill(false, true, "follower").hidden, false, "the pill is hidden when the bridge exists");
});

// Read the rendered strings only — the surrounding comments deliberately DISCUSS the words we
// rejected, so grepping the whole file for them reports a leak that isn't there.
const states = () => {
  const start = APP.indexOf("const SYNC_PILL = {");
  const block = APP.slice(start, APP.indexOf("};", start));
  const out = {};
  for (const m of block.matchAll(/(\w+):\s*\{[^}]*title: "([^"]*)"[^}]*action: "([^"]*)"/g)) {
    out[m[1]] = { title: m[2], action: m[3] };
  }
  return out;
};

test("all three states are present; the pill reports STATE, it no longer takes the role", () => {
  // WHAT CHANGED (build 435). The pill used to BE the role control, so every state had to carry an
  // action or it looked inert. The role now lives behind the IR A CANTO modal (★ Dirigir), because
  // a permanently visible become-director button on six choir iPads is a split-brain generator and
  // the choir is non-technical. So the pill is now a readout, and a follower is a bare dot.
  //
  // "SIGUIENDO" was removed on purpose: it was a CLAIM, and it lied — two iPads sat stranded on
  // song 59 for minutes still reading SIGUIENDO. A dot is just a light.
  const s = states();
  assert.deepEqual(Object.keys(s).sort(), ["directing", "following", "nobody"]);
  for (const [name, v] of Object.entries(s)) {
    assert.ok(v.title.length > 0, `${name} has no title`);
  }
  // The one state that still earns WORDS is "nobody is driving" — the moment a singer must notice.
  assert.match(s.nobody.title, /nadie/i, "the no-director state must still say so in words");
  assert.equal(s.following.action, "", "following must be a bare dot — no action, no claim");
});

test("no state promises an approval that does not exist", () => {
  // Tapping makes you director immediately. Nothing grants it and the current director is never
  // asked, so "Pedir"/"Solicitar"/"request" would have someone waiting for a reply that never comes.
  for (const [name, v] of Object.entries(states())) {
    const text = `${v.title} ${v.action}`;
    assert.ok(!/pedir|solicit|request/i.test(text),
      `${name} uses approval language ("${text}") for something nothing approves`);
  }
});

test("no label promises a dialog the app may not show", () => {
  // THE MISTAKE THIS REPLACES. An earlier version asserted following → "Tomar el control", matching
  // the takeover button. But which confirm appears is decided NATIVELY at tap time by `liveDirector`
  // — an 8s window on lastDirectorSnapshotRef — while the pill's SIGUIENDO uses a 15s window on mesh
  // page events, deliberately longer so a radio hiccup does not flash NADIE DIRIGE mid-hymn.
  //
  // Between 8s and 15s the two disagree and the dialog reads "¿Dirigir el coro?", so the label was
  // wrong exactly when someone was reading it. A label may only claim what is invariant.
  const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const pillWindow = Number(APP.match(/const DIRECTOR_SEEN_WINDOW_MS = (\d+)/)[1]);
  const dialogWindow = Number(NATIVE.match(/const LIVE_DIRECTOR_WINDOW_MS = (\d+)/)[1]);
  assert.notEqual(pillWindow, dialogWindow,
    "the windows now agree — if that is deliberate, a label MAY name the dialog; re-read this test");
  for (const [name, v] of Object.entries(states())) {
    assert.ok(!/Tomar el control/.test(v.action),
      `${name} names the takeover button, which only appears inside a ${dialogWindow}ms window`);
  }
});

test("the role control names the role, and is not a bare verb", () => {
  // These two concerns followed the control to its new home rather than being deleted: an earlier
  // label "Tomar" failed because it was a bare verb ("take WHAT?"), and both paths to directing must
  // describe the same destination. The control is now #role-toggle inside the IR A CANTO modal.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  // ONE HOME PER DIRECTION, and as of 2026-08-18 entering is VISIBLE rather than buried.
  // ★ Ser Director moved out of the IR A CANTO keypad and into a top-left pill, because Braulio
  // forgets the procedure most weeks and a control hidden inside a modal named "IR A CANTO" is not
  // findable by someone who has forgotten there is a procedure at all. The empty seat that causes
  // is what produced the 2026-07-01 no-director-all-night outage.
  //
  // Visibility does NOT weaken the role: #role-gate is what protects it, not obscurity. Tapping this
  // opens the red "ESTO CAMBIA LA PÁGINA DE TODOS" wall and you must type the word.
  assert.match(HTML, /id="become-director-pill"/, "the entry to the role is gone entirely");
  assert.doesNotMatch(HTML, /id="role-toggle"/,
    "the keypad copy is back — two homes for one act is the redundancy that was just removed");
  // Leaving is the DIRECTOR status itself: it reads the role you hold, and tapping it asks whether
  // you meant to stop. A separate ✕ lived beside it briefly — two controls for one act, and the ✕
  // said nothing about which role you were in.
  assert.match(APP, /exit-director/, "nothing can leave the role any more");
  assert.doesNotMatch(HTML, /id="sync-exit-fab"/, "the separate ✕ is back — the status owns the exit");
  assert.doesNotMatch(HTML, /id="role-exit-x"/, "an exit is back in the keypad — it must live only in the top bar");
  assert.doesNotMatch(HTML, /id="role-status-pill"/, "a DIRECTOR status is back in the keypad");
  // Sentence case, not caps: all-caps is fine for a one-time alarm, distracting on a label you live
  // with. The two labels are set in app.js.
  // The control no longer relabels itself. While directing it is REPLACED by an inert DIRECTOR
  // status plus a separate ✕ — one element wearing two meanings is what made "Salir de director"
  // ambiguous on a control that also ENTERS the role, and what let a mis-tap drop the choir's
  // director mid-Mass. So the key says one thing, always.
  // "Ser Director" names the ROLE. "Tomar" failed as a bare verb ("take WHAT?") and
  // "Convertirme en director" was right but too long for the square.
  //
  // WHAT THE OLD ASSERTION MISSED. It looped over two string literals written on the line above —
  // ["Ser Director", "DIRECTOR"] — and checked that they contain "Director". Both do, by
  // construction, so the loop could not fail no matter what index.html and app.js said; relabelling
  // the button "Tomar" left it green. The labels are now READ OUT of the markup that renders them,
  // and the extractor must find a non-empty label before anything is asserted about it.
  //
  // An `assert.match(HTML, /Ser Director/)` stood beside that loop and is gone rather than kept: the
  // markup splits those two words across sibling spans, so the only contiguous "Ser Director" in the
  // file is in the comments above. It passed on prose, not on the button.
  const visibleText = (html, marker, tag) => {
    const at = html.indexOf(marker);
    assert.ok(at > 0, `${marker} is gone from index.html — the control it labels no longer exists`);
    const open = html.lastIndexOf(`<${tag}`, at);
    assert.ok(open >= 0, `${marker} is not inside a <${tag}> — this extractor is reading the wrong element`);
    // Structural end: the </tag> that closes THIS element, counting nested opens of the same tag.
    // Not a character count, and it throws rather than running to EOF if the close is missing.
    let depth = 0;
    let close = -1;
    for (let i = open; i < html.length; i++) {
      if (html.startsWith(`<${tag}`, i)) depth++;
      else if (html.startsWith(`</${tag}`, i)) { depth--; if (depth === 0) { close = i; break; } }
    }
    assert.ok(close > open, `<${tag}> carrying ${marker} is never closed`);
    return html.slice(open, close)
      .replace(/<!--[\s\S]*?-->/g, " ")     // the comments in this markup discuss rejected wording
      .replace(/<[^>]*>/g, " ")             // tags become gaps, so "…>Ser" does not read as one word
      .replace(/&#\d+;|&\w+;/g, " ")        // ☆ and friends are decoration, not a word
      .replace(/[^\p{L}\s]/gu, " ")
      .replace(/\s+/g, " ")
      .trim();
  };

  const entry = visibleText(HTML, 'id="become-director-pill"', "button");
  assert.ok(entry.length > 0, "the entry control renders no text at all — a blank square teaches nothing");
  assert.match(entry, /\bdirector\b/i,
    `the entry control reads "${entry}" — it must name the ROLE, the way "Tomar" did not`);
  assert.ok(entry.split(" ").length >= 2,
    `the entry control reads "${entry}" — one word is the bare-verb failure again; say what it acts on`);
  // The screen-reader path is a separate string and rots separately; it must name the role too.
  //
  // WHAT THE OLD ASSERTION MISSED. It sliced from the marker to END OF FILE and took the first
  // aria-label it ran into. The very next element in the markup is the ⟳ resync fab, which carries
  // "Volver a sincronizar con el director" — so deleting the pill's own aria-label left the check
  // reading the FAB's label, finding "director" in it, and passing while a blind director heard
  // nothing about the button under their thumb. The slice is now bounded to the pill's own open tag.
  const openTagOf = (html, marker, tag) => {
    const at = html.indexOf(marker);
    assert.ok(at > 0, `${marker} is gone from index.html — the control it labels no longer exists`);
    const open = html.lastIndexOf(`<${tag}`, at);
    assert.ok(open >= 0, `${marker} is not inside a <${tag}> — this extractor is reading the wrong element`);
    // Structural end of the OPEN TAG: the first ">" that is not inside a quoted attribute value.
    // Not a character count, and it fails rather than running to EOF if the tag is never closed.
    let quote = null;
    let close = -1;
    for (let i = open; i < html.length; i++) {
      const ch = html[i];
      if (quote) { if (ch === quote) quote = null; continue; }
      if (ch === '"' || ch === "'") { quote = ch; continue; }
      if (ch === ">") { close = i; break; }
    }
    assert.ok(close > at, `the <${tag}> carrying ${marker} never closes its open tag`);
    return html.slice(open, close + 1);
  };
  const aria = openTagOf(HTML, 'id="become-director-pill"', "button").match(/aria-label="([^"]*)"/);
  assert.ok(aria, "the entry control lost its own aria-label — the ⟳ fab's label next to it is not a substitute");
  assert.match(aria[1], /\bdirector\b/i, `the spoken label "${aria[1]}" does not name the role`);

  // Leaving names the role as well — a bare ✕ said nothing about which role you were in, which is
  // the ambiguity that dropped the choir's director mid-Mass. A STATUS may be one word (it reports
  // rather than acts), so only the role noun is required here.
  const exit = visibleText(HTML, 'class="director-exit-label"', "span");
  assert.match(exit, /\bdirector\b/i, `the exit control reads "${exit}" — it must say which role you are leaving`);
  const directing = states().directing.title;
  assert.ok(typeof directing === "string" && directing.length > 0, "the directing state lost its title");
  assert.match(directing, /\bdirector\b/i,
    `the directing status reads "${directing}" — the status the director lives with must name the role`);
});

test("taking the role is gated on a typed word, stepping down is not", () => {
  // Asymmetric friction, matched to consequence. Taking the role changes the page for EVERY device
  // in the loft; stepping down is recoverable. The word is "braulio", not a real director code:
  // everyone watches Braulio type his code, so secrecy was never the threat model — carelessness is.
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  assert.match(HTML, /id="role-gate"/, "the take-the-role gate is gone");
  assert.match(HTML, /ESTO CAMBIA LA P/, "the gate no longer warns that it changes everyone's page");
  assert.match(APP, /ROLE_GATE_WORD = "braulio"/, "the typed-word gate is gone");
  assert.match(APP, /roleGateConfirm\.disabled =/, "Confirmar must stay disabled until the word matches");
  assert.match(APP, /¿Salir de director\?/, "stepping down must still confirm, plainly");
});

test("LEGACY (pre-435): both ways of taking the role named the same outcome", { skip: "the pill no longer takes the role — see the two tests above" }, () => {
  // Following and nobody differ in SITUATION, not in result: either way you end up directing. The
  // title (SIGUIENDO vs NADIE DIRIGE) and the tint carry the difference; the action states the
  // destination, which never varies.
  const s = states();
  const plain = (a) => a.replace(/[✕▶]\s*/, "").trim();
  assert.equal(plain(s.following.action), plain(s.nobody.action),
    "the two paths to directing describe different outcomes, but the outcome is identical");
  // Must name the ROLE, not just a verb — that is what "Tomar" lacked.
  assert.match(plain(s.nobody.action), /director/i, "the action does not name the role being taken");
});

test("LEGACY (pre-435): no pill label is a bare verb", { skip: "the pill no longer carries an action — see 'the role control names the role'" }, () => {
  // The specific failure being guarded: "Tomar" reads as "take" with nothing taken.
  for (const [name, v] of Object.entries(states())) {
    const plain = v.action.replace(/[✕▶]\s*/, "").trim();
    assert.ok(plain.split(/\s+/).length >= 2 || /Salir/.test(plain),
      `${name} action "${plain}" is a bare verb — say what it acts on`);
  }
});

// ── A small cascade resolver ────────────────────────────────────────────────────────────────────
// THE STANDING RULE FOR EVERYTHING BELOW, and the lesson of round 3: a test file cannot become a
// browser, so it must not pretend to be one. Every extra CSS feature modelled here is another chance
// to be confidently wrong, and a confidently wrong "no rule decides this" is exactly the failure the
// resolver exists to prevent — it reads as reassurance while the shipped pixels say the opposite.
// So the resolver REFUSES, loudly and by name, on every construct it cannot score soundly:
// !important, a selector shape whose specificity it cannot compute, an id or attribute it has not
// read out of the document, a functional pseudo, a combinator, a statement at-rule, a conditional
// rule. A refusal names the construct, the selector and the styles.css line, so the next reader
// knows where to look. What would actually ANSWER any of these questions is a real browser
// computing the cascade on the real document — nothing in this file is a substitute for that, and
// where it refuses, that is the tool to reach for.
//
// Every rule in the sheet, in source order, with its at-rule context. Rules are bounded by their
// own braces, never by a character count.
//
// WHAT THE OLD VERSION MISSED. Comments were collapsed to a single space, which erased the newlines
// inside them, so every `line` reported here ran ~148 lines short of the real styles.css — and a
// failure message that points at the wrong line sends the next reader to the wrong rule, which is
// worse than no line at all. Each comment now leaves its newlines behind, so line numbers are
// counted against the ORIGINAL text while the comment's CONTENT still cannot be read as CSS.
//
// WHAT ROUND 3 FOUND. The selector list was split on EVERY comma with no awareness of parentheses,
// so a single `:is(a, b)` or `:not(.x, .y)` was torn into two malformed halves and the whole file
// died on a parse error instead of returning a verdict. Commas are now split at paren depth zero
// only, and the functional pseudo that survives the split is refused by name further down.

// A refusal raised while PARSING the sheet, before any test has run. Collected rather than thrown at
// import time so it surfaces as a named test failure with a readable message instead of a module
// that fails to load.
const SHEET_REFUSALS = [];

// Split on a separator that appears at paren/bracket depth zero and outside quotes. Used for the
// commas between selectors and for the semicolons between declarations, both of which can legally
// appear inside `:is(a, b)` or `url(data:...;base64,...)`.
const splitTopLevel = (text, sep) => {
  const out = [];
  let buf = "", depth = 0, quote = null;
  for (const ch of text) {
    if (quote) { buf += ch; if (ch === quote) quote = null; continue; }
    if (ch === '"' || ch === "'") { quote = ch; buf += ch; continue; }
    if (ch === "(" || ch === "[") depth++;
    else if (ch === ")" || ch === "]") depth--;
    else if (ch === sep && depth === 0) { out.push(buf); buf = ""; continue; }
    buf += ch;
  }
  out.push(buf);
  return out;
};

const cssRules = (() => {
  const src = CSS.replace(/\/\*[\s\S]*?\*\//g, (c) => ` ${"\n".repeat((c.match(/\n/g) || []).length)}`);
  const lineAt = (i) => src.slice(0, i).split("\n").length;
  const out = [];
  const conds = [];
  let buf = "";
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    // A STATEMENT at-rule — `@import "x";`, `@charset "utf-8";`, `@layer a, b;` — never opens a
    // block, so this brace-counting loop would carry its text forward and glue it onto the front of
    // the NEXT rule's selector list, scoring a selector that does not exist anywhere. Refuse.
    if (c === ";") {
      SHEET_REFUSALS.push(`line ${lineAt(i)}: styles.css contains the statement "${`${buf.trim()};`.trim()}", and this resolver does not model statement at-rules — it would fold that text into the next rule's selector list. Teach it that statement, or check the cascade in a real browser.`);
      buf = "";
      continue;
    }
    if (c === "{") {
      const prelude = buf.trim();
      buf = "";
      if (prelude.startsWith("@")) { conds.push(prelude); continue; }
      const end = src.indexOf("}", i);
      if (end <= i) { SHEET_REFUSALS.push(`line ${lineAt(i)}: the rule "${prelude}" is never closed`); break; }
      out.push({
        selectors: splitTopLevel(prelude, ",").map((s) => s.trim()).filter(Boolean),
        body: src.slice(i + 1, end),
        order: out.length,
        conds: [...conds],
        line: lineAt(i),
      });
      i = end;
      continue;
    }
    if (c === "}") { conds.pop(); continue; }
    buf += c;
  }
  if (conds.length) SHEET_REFUSALS.push("styles.css has an unbalanced at-rule block");
  return out;
})();

// Split a selector into its compounds and combinators, respecting parens, brackets and quotes.
//
// WHAT ROUND 3 FOUND. The old version padded `>`, `+` and `~` with spaces ANYWHERE in the string,
// including inside `:has(> .x)` and `:nth-child(2n + 1)`, and then split on every run of whitespace
// — so a functional pseudo containing a combinator or a space came out as two broken fragments that
// no longer parsed as written. Both the padding and the split now happen at depth zero only.
const selectorParts = (sel) => {
  const parts = [];
  let buf = "", depth = 0, quote = null;
  const flush = () => { if (buf.trim()) parts.push(buf.trim()); buf = ""; };
  for (const ch of sel.trim()) {
    if (quote) { buf += ch; if (ch === quote) quote = null; continue; }
    if (ch === '"' || ch === "'") { quote = ch; buf += ch; continue; }
    if (ch === "(" || ch === "[") { depth++; buf += ch; continue; }
    if (ch === ")" || ch === "]") { depth--; buf += ch; continue; }
    if (depth === 0 && /\s/.test(ch)) { flush(); continue; }
    if (depth === 0 && /[>+~]/.test(ch)) { flush(); parts.push(ch); continue; }
    buf += ch;
  }
  flush();
  return parts;
};

// A compound like `html[data-role="follower"]`, parsed strictly: anything this does not understand
// throws instead of being silently treated as "no constraint", which is how a matcher starts
// matching more than it should. A functional pseudo is read WHOLE, with balanced parens, so
// `:is(:not(.a), .b)` arrives downstream as one named construct to refuse rather than as a parse
// error at a stray bracket — the refusal message is the useful half.
const parseCompound = (text) => {
  const part = { tag: null, ids: [], classes: [], attrs: [], pseudos: [] };
  const token = /^(?:([a-zA-Z][\w-]*)|#([\w-]+)|\.([\w-]+)|\[([^\]]+)\]|(\*))/;
  let rest = text;
  while (rest.length) {
    if (rest[0] === ":") {
      const name = /^(::?[\w-]+)/.exec(rest);
      if (!name) throw new Error(`cannot parse the selector fragment "${text}" at "${rest}"`);
      let take = name[1].length;
      if (rest[take] === "(") {
        let depth = 0, closed = false;
        for (let i = take; i < rest.length; i++) {
          if (rest[i] === "(") depth++;
          else if (rest[i] === ")") { depth--; if (depth === 0) { take = i + 1; closed = true; break; } }
        }
        if (!closed) throw new Error(`the pseudo "${name[1]}(" in "${text}" never closes its parenthesis`);
      }
      part.pseudos.push(rest.slice(0, take));
      rest = rest.slice(take);
      continue;
    }
    const m = token.exec(rest);
    if (!m) throw new Error(`cannot parse the selector fragment "${text}" at "${rest}"`);
    if (m[1]) part.tag = m[1].toLowerCase();
    else if (m[2]) part.ids.push(m[2]);
    else if (m[3]) part.classes.push(m[3]);
    else if (m[4]) part.attrs.push(m[4]);
    // `*` is the one thing that really IS "no constraint" — it matches every element and adds
    // nothing to specificity, so it is recorded as nothing rather than assumed to be nothing.
    rest = rest.slice(m[0].length);
  }
  return part;
};

// The RESTING state only: a compound carrying :hover/:active/::before describes a moment or a
// generated box, not the pill sitting on screen, so it is excluded rather than counted as a match.
//
// WHAT THE OLD VERSION MISSED. It excluded EVERY pseudo — `if (c.pseudos.length) return false` — so
// a rule was allowed to vanish from the cascade for reasons this resolver had never actually
// decided. Measured: appending `… .director-mode-badge-exit:not(.gone) { display: flex }` un-hides
// the follower action in a real browser and the whole file stayed green, because :not() dropped the
// rule on the floor. Only the two kinds below are a SOUND non-match; everything else now refuses.
const DECIDABLY_ABSENT = (p) =>
  // A pseudo-ELEMENT styles a generated box, never this element, so it cannot decide our display.
  p.startsWith("::")
  // A transient state pseudo describes a finger on the glass, not the resting pill on the wall.
  || /^:(hover|active|focus|focus-visible|focus-within)$/.test(p);

// The two pseudos this resolver genuinely DOES model, evaluated against the fixture rather than
// assumed. :root is the document root, which in this modelled chain is the one <html>. :disabled is
// read straight out of index.html: none of the modelled elements carries the attribute.
//
// THE CEILING ON :disabled, written down so it is not rediscovered: this is a STATIC read of the
// markup. If app.js ever set `el.disabled = true` on one of these elements at runtime the model
// would be wrong and nothing here would notice — only a browser computing the cascade on the live
// document can settle that. It stays modelled because `button:disabled` (styles.css:108) matches
// the badge's tag, and refusing it would redden every cascade answer in this file over a rule that
// has never applied.
const MODELLED_PSEUDO = {
  ":root": (el) => el.tag === "html",
  ":disabled": (el) => "disabled" in el.attrs,
};

// Three answers, and the third is never quietly folded into the second: NO (this compound cannot
// describe this element), YES, and UNKNOWN (a constraint this resolver does not model).
const NO = "no", YES = "yes", UNKNOWN = "unknown";

const attrName = (a) => {
  const eq = a.indexOf("=");
  return (eq < 0 ? a : a.slice(0, eq)).replace(/[~|^$*]$/, "").trim();
};

// Everything in this compound that this resolver cannot score, named for the failure message.
const unmodelledIn = (el, text) => {
  const c = parseCompound(text);
  const out = [];
  for (const p of c.pseudos) if (!(p in MODELLED_PSEUDO)) out.push(`the pseudo "${p}"`);
  for (const a of c.attrs) {
    const n = attrName(a);
    if (!el.knownAttrs.has(n)) out.push(`the attribute "${n}", which index.html does not set on <${el.tag}> and this fixture does not model at runtime`);
    else if (/[~|^$*]=/.test(a)) out.push(`the attribute matcher in "[${a}]"`);
  }
  if (c.ids.length && !("id" in el)) out.push(`an id selector against a fixture element that never declared its id`);
  return out.length ? out : [`something this resolver does not model`];
};

const compoundVerdict = (el, text) => {
  const c = parseCompound(text);
  // One decidably-absent pseudo is enough: it excludes the rule from the resting cascade no matter
  // what else the compound carries (`.btn:hover:not(.is-active)` is simply not hovered right now).
  if (c.pseudos.some(DECIDABLY_ABSENT)) return NO;
  if (c.tag && c.tag !== el.tag) return NO;
  // Tags, ids and static attributes are READ OUT of index.html (see the document reader below), so
  // their absence is a fact about the document rather than a hole in the fixture. A fixture element
  // that never declared its id cannot decide an id selector and says so.
  if (c.ids.length) {
    if (!("id" in el)) return UNKNOWN;
    if (c.ids.some((id) => id !== el.id)) return NO;
  }
  if (c.classes.some((cl) => !el.classes.includes(cl))) return NO;
  for (const a of c.attrs) {
    const n = attrName(a);
    // An attribute this element's open tag never carried MIGHT still be set by app.js at runtime.
    // "Absent from the markup" is not "absent from the DOM", so it is UNKNOWN, never NO.
    if (!el.knownAttrs.has(n)) return UNKNOWN;
    const eq = a.indexOf("=");
    if (eq < 0) { if (!(n in el.attrs)) return NO; continue; }
    // [x~="a"] / [x^="a"] / [x*="a"] / [x|="a"] / [x$="a"] are substring and token matchers, none
    // of which this resolver computes.
    if (/[~|^$*]=/.test(a)) return UNKNOWN;
    const want = a.slice(eq + 1).trim().replace(/^["']|["']$/g, "");
    if (el.attrs[n] !== want) return NO;
  }
  for (const p of c.pseudos) {
    if (!(p in MODELLED_PSEUDO)) return UNKNOWN;
    if (!MODELLED_PSEUDO[p](el)) return NO;
  }
  return YES;
};

// The yes/no face of the same question, for the ordered walk: UNKNOWN throws rather than returning
// the false that the old code would have handed back.
const compoundMatches = (el, text) => {
  const v = compoundVerdict(el, text);
  if (v === UNKNOWN) {
    throw new Error(`compound "${text}" carries ${unmodelledIn(el, text).join(" and ")}, which this resolver does not model`);
  }
  return v === YES;
};

// Specificity, computed only for shapes it can compute EXACTLY. A functional pseudo does not
// contribute a flat class-worth: :where() contributes nothing at all, and :is()/:not()/:has() each
// contribute the maximum specificity of their argument list. Guessing "one class" for those is the
// kind of quiet arithmetic error that hands the cascade to the wrong declaration, so it refuses.
const specificity = (sel) => {
  let ids = 0, cls = 0, els = 0;
  for (const compound of selectorParts(sel)) {
    if (/^[>+~]$/.test(compound)) {
      throw new Error(`cannot score the specificity of "${sel}": it uses the combinator "${compound}"`);
    }
    const c = parseCompound(compound);
    for (const p of c.pseudos) {
      if (p.includes("(")) {
        throw new Error(`cannot score the specificity of "${sel}": the functional pseudo "${p}" contributes the specificity of its ARGUMENT (:where() none, :is()/:not()/:has() the maximum of theirs), which this resolver does not compute`);
      }
    }
    ids += c.ids.length;
    cls += c.classes.length + c.attrs.length + c.pseudos.filter((p) => !p.startsWith("::")).length;
    els += (c.tag ? 1 : 0) + c.pseudos.filter((p) => p.startsWith("::")).length;
  }
  return ids * 10000 + cls * 100 + els;
};

// `chain` is the element and its modelled ancestors, outermost first. Descendant combinators only:
// a selector using > + or ~ throws, because this resolver does not model sibling or direct-child
// relationships and a wrong "no match" is indistinguishable from a real absence.
//
// The one thing it CAN decide about such a selector is its subject. Combinators only ADD
// constraints, so if the rightmost compound does not match the element, the whole selector cannot
// either, whatever sits to the left of the arrow — that is a sound no, and it keeps the sheet's
// unrelated `>` and `~` rules quiet. If the subject DOES match, the answer is genuinely unknown and
// the selector is thrown at the caller instead of being reported as an absence.
const selectorMatches = (sel, chain) => {
  const parts = selectorParts(sel);
  const subject = parts[parts.length - 1];
  if (subject === undefined) throw new Error(`selector "${sel}" is empty`);
  if (/^[>+~]$/.test(subject)) throw new Error(`selector "${sel}" ends in a combinator`);
  // A descendant-or-child selector requires every one of its compounds to match somewhere on this
  // element's ancestor path. If one of them matches nowhere — decided on structure alone, never on
  // a pseudo this resolver cannot read — the selector is absent and no modelling is needed to say
  // so. That is what lets `.resync-dots span:nth-child(2)` stay quiet without pretending to know
  // what :nth-child means. Sibling combinators are excluded because their compounds describe
  // siblings, which this chain does not model at all.
  if (!parts.some((p) => /^[+~]$/.test(p))) {
    for (const compound of parts.filter((p) => !/^[>+~]$/.test(p))) {
      if (chain.every((el) => compoundVerdict(el, compound) === NO)) return false;
    }
  }
  if (!compoundMatches(chain[chain.length - 1], subject)) return false;
  const combinator = parts.find((p) => /^[>+~]$/.test(p));
  if (combinator) {
    throw new Error(`selector "${sel}" uses "${combinator}", which this resolver does not model, and its subject DOES match this element`);
  }
  let i = chain.length - 2;
  for (let p = parts.length - 2; p >= 0; p--) {
    while (i >= 0 && !compoundMatches(chain[i], parts[p])) i--;
    if (i < 0) return false;
    i--;
  }
  return true;
};

// Every rule that applies to this element, ordered the way a browser orders them. Media-conditional
// rules are refused rather than guessed at, so a rule moving inside a @media fails loudly.
//
// WHAT THE OLD VERSION MISSED. It wrapped selectorMatches in `try { … } catch { ok = false }`,
// which turned every deliberate "I cannot decide this" into the exact wrong answer the throw exists
// to prevent: a silent "no match". An undecidable selector must redden the file and name itself, so
// that whoever added it either teaches this resolver or hand-checks the cascade in a browser.
//
// WHAT ROUND 3 FOUND. The per-rule loop stopped at the FIRST matching selector of the comma list and
// scored the whole rule at THAT selector's specificity. A browser scores a rule at the HIGHEST
// specificity among the selectors of its list that match, so a rule whose list happened to open with
// a low-specificity match was under-scored, the wrong declaration won, and resolve() returned a
// value the browser never uses. Measured: appending `.director-mode-badge-exit,
// html[data-role="follower"] .app-shell .director-mode-badge.is-following .director-mode-badge-exit
// { display: flex }` un-hides the follower action in a real browser and the file stayed green. Every
// matching selector of a rule is now scored and the maximum is kept.
const matchingRules = (chain) => {
  assert.equal(SHEET_REFUSALS.length, 0, `this resolver cannot read styles.css:\n${SHEET_REFUSALS.join("\n")}`);
  const hits = [];
  for (const r of cssRules) {
    if (r.conds.some((c) => c.startsWith("@keyframes"))) continue;
    const matched = [];
    for (const sel of r.selectors) {
      try {
        if (selectorMatches(sel, chain)) matched.push({ sel, spec: specificity(sel) });
      } catch (e) {
        assert.fail(`line ${r.line}: ${e.message} — this resolver refuses to guess, because a wrong "no match" is indistinguishable from a real absence. Teach it that selector, or compute the cascade in a real browser.`);
      }
    }
    if (!matched.length) continue;
    assert.deepEqual(r.conds, [], `line ${r.line}: "${matched[0].sel}" now matches from inside ${r.conds.join(" ")} — this resolver cannot decide a conditional rule`);
    const best = matched.reduce((a, b) => (b.spec > a.spec ? b : a));
    hits.push({ ...r, sel: best.sel, spec: best.spec });
  }
  return hits.sort((a, b) => a.spec - b.spec || a.order - b.order);
};

// Specificity, then source order — and a later declaration inside the same rule beats an earlier
// one, which is why the last comparison is >=. There is deliberately no !important branch here:
// resolve() refuses rather than ranking one, because important declarations are ordered by ORIGIN
// (author / user / user-agent, and cascade layers invert within them) and this resolver reads one
// author sheet with no idea what else the document loads.
const beats = (a, b) => {
  if (!b) return true;
  if (a.spec !== b.spec) return a.spec > b.spec;
  return a.order >= b.order;
};

const resolve = (prop, chain) => {
  let winner = null;
  for (const r of matchingRules(chain)) {
    for (const d of splitTopLevel(r.body, ";")) {
      const colon = d.indexOf(":");
      if (colon < 0) continue;
      if (d.slice(0, colon).trim() !== prop) continue;
      const raw = d.slice(colon + 1).trim();
      if (/!\s*important$/i.test(raw)) {
        assert.fail(`line ${r.line}: "${r.sel}" declares ${prop}: ${raw} — this resolver refuses to score !important, whose ordering depends on the origin and layer of every sheet the document loads, not just this one. Compute the cascade in a real browser and pin the answer here.`);
      }
      const cand = { value: raw, spec: r.spec, order: r.order, line: r.line, sel: r.sel };
      if (beats(cand, winner)) winner = cand;
    }
  }
  return winner;
};

// ── The document the cascade is resolved against ────────────────────────────────────────────────
// index.html with every HTML comment blanked to same-length spaces. Offsets and newlines survive, so
// indexes and line numbers still line up, while commented-out markup cannot be read as markup.
//
// WHAT ROUND 3 FOUND. The ancestor walk scanned the raw file, so open tags inside `<!-- ... -->`
// counted as real ancestry — wrapping an unrelated block in a comment, a behaviour-neutral edit,
// moved the modelled chain and the assertion moved with it. This markup is full of comments that
// contain example markup, so that was not a hypothetical.
const HTML_DOC = fs.readFileSync("web/src/index.html", "utf8")
  .replace(/<!--[\s\S]*?-->/g, (c) => c.replace(/[^\n]/g, " "));

// The attributes of one open tag, `class` and `id` lifted out because the matcher treats them
// separately. Quoted values are consumed whole, so a value containing a space or a ">" is safe.
const attrsOf = (tagText) => {
  const inner = tagText.replace(/^<[a-zA-Z][\w-]*/, "").replace(/\/?>$/, "");
  const out = {};
  for (const m of inner.matchAll(/([a-zA-Z_:][\w:.-]*)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+)))?/g)) {
    if (!m[1]) continue;
    out[m[1]] = m[2] ?? m[3] ?? m[4] ?? "";
  }
  return out;
};

// The element whose open tag contains `at`, as a fixture element: its tag, id, classes and the
// complete set of attributes index.html gives it. `knownAttrs` is the honest boundary — an
// attribute outside it may still be set by app.js at runtime, so the matcher refuses on it rather
// than reporting an absence.
const elementAt = (at, what) => {
  const start = HTML_DOC.lastIndexOf("<", at);
  assert.ok(start >= 0, `${what} is not inside any tag in index.html`);
  // The first ">" at or after the tag start that is not inside a quoted attribute value. Requiring
  // it to sit at or beyond `at` is also what proves `at` really is inside THIS open tag.
  let quote = null, end = -1;
  for (let i = start; i < HTML_DOC.length; i++) {
    const ch = HTML_DOC[i];
    if (quote) { if (ch === quote) quote = null; continue; }
    if (ch === '"' || ch === "'") { quote = ch; continue; }
    if (ch === ">") { end = i; break; }
  }
  assert.ok(end >= at, `the tag carrying ${what} never closes its open tag — this reader is misreading index.html`);
  const name = /^<([a-zA-Z][\w-]*)/.exec(HTML_DOC.slice(start, end + 1));
  assert.ok(name, `${what} is not inside an element open tag in index.html`);
  const attrs = attrsOf(HTML_DOC.slice(start, end + 1));
  const classes = (attrs.class || "").split(/\s+/).filter(Boolean);
  const id = attrs.id ?? null;
  delete attrs.class;
  delete attrs.id;
  return { tag: name[1].toLowerCase(), id, classes, attrs, knownAttrs: new Set(Object.keys(attrs)), tagStart: start };
};

const docElement = (marker) => {
  const at = HTML_DOC.indexOf(marker);
  assert.ok(at > 0, `${marker} is gone from index.html, or lives only inside a comment — the cascade fixture describes an element that does not exist`);
  return elementAt(at, marker);
};

// The still-open ancestors at a byte offset, outermost first. Void and self-closed elements never
// push; a close tag pops only when it matches the top of the stack, so a stray `</div>` cannot
// unwind the document.
const VOID = new Set(["meta", "link", "br", "img", "input", "hr", "source", "path", "circle", "area", "base", "col", "embed", "track", "wbr"]);
const docAncestors = (offset) => {
  const stack = [];
  for (const m of HTML_DOC.slice(0, offset).matchAll(/<(\/?)([a-zA-Z][\w-]*)((?:"[^"]*"|'[^']*'|[^>"'])*?)(\/?)>/g)) {
    const tag = m[2].toLowerCase();
    if (m[1]) { if (stack.length && stack[stack.length - 1].tag === tag) stack.pop(); continue; }
    if (VOID.has(tag) || m[4] === "/") continue;
    stack.push(elementAt(m.index + 1, `<${tag}> at offset ${m.index}`));
  }
  return stack;
};

// The runtime half of the modelled state, kept separate from the document because index.html cannot
// show it: renderDirectorModeBadge() writes data-role onto <html>, and the native shell writes
// data-shell. These two names are therefore MODELLED (present, with these values); every other
// attribute on <html> — data-mesh among them — is not, and the matcher refuses on it.
const RUNTIME_HTML_ATTRS = { "data-role": "follower", "data-shell": "native" };
// The badge's runtime classes. index.html ships it `is-hidden`; the state under test is the one a
// follower sees while someone else directs.
const FOLLOWING_BADGE_CLASSES = ["director-mode-badge", "is-following"];

const withRuntime = (el) => el.tag !== "html" ? el : {
  ...el,
  attrs: { ...el.attrs, ...RUNTIME_HTML_ATTRS },
  knownAttrs: new Set([...el.knownAttrs, ...Object.keys(RUNTIME_HTML_ATTRS)]),
};

const BADGE_DOC = docElement('id="director-mode-badge"');
const badgeChain = [
  ...docAncestors(BADGE_DOC.tagStart).map(withRuntime),
  { ...BADGE_DOC, classes: FOLLOWING_BADGE_CLASSES },
];

// WHAT ROUND 3 FOUND. This leaf was hardcoded as `{ tag: "span" }` for BOTH children, while
// index.html:107 ships `<strong class="director-mode-badge-title" id="sync-pill-title">`. A fixture
// that disagrees with the document produces exactly the confident wrong answer the resolver refuses
// to produce on its own, and in both directions at once: a `strong.…` rule is missed, and a
// `span.…` rule is falsely matched. The tag, the id and the attributes now come out of the document;
// only the class the caller names is checked against it, so a rename fails loudly.
const childChain = (cls, id) => {
  const leaf = docElement(`id="${id}"`);
  assert.ok(leaf.classes.includes(cls),
    `#${id} in index.html carries classes [${leaf.classes.join(" ")}] — the cascade fixture asks for .${cls}, so the two disagree and every answer below would be decided against an element that does not exist`);
  return [...badgeChain, leaf];
};

test("the resolver can read every rule in styles.css", () => {
  // A construct the parser cannot take apart is not a green light; it is a rule whose declarations
  // silently never entered the cascade. Surfaced here as its own failure so the message is readable
  // rather than a module that refuses to load.
  assert.equal(SHEET_REFUSALS.length, 0, SHEET_REFUSALS.join("\n"));
  assert.ok(cssRules.length > 100, `only ${cssRules.length} rules parsed out of a 3000-line sheet — this parser is skipping the file`);
});

test("the modelled ancestor chain is the one index.html actually builds", () => {
  // The resolver above answers "no rule decides this" by walking THIS chain, so a chain that has
  // drifted from the markup produces exactly the confident wrong "no" the resolver now refuses to
  // produce on its own. Wrap the badge in one new <div class="…"> and every `.that-div .badge` rule
  // silently stops existing as far as the cascade tests are concerned.
  //
  // The chain is now READ OUT of index.html rather than written down, so it cannot drift — which
  // means this test can no longer compare it against itself. It is compared against the shape
  // spelled out below instead, so restructuring the markup fails here, loudly, once, rather than
  // quietly changing what every cascade answer in this file means.
  const shape = (e) => [e.tag + (e.id ? `#${e.id}` : ""), ...[...e.classes].sort()].join(".");
  assert.deepEqual(badgeChain.map(shape), [
    "html",
    "body",
    "main.app-shell",
    "button#director-mode-badge.director-mode-badge.is-following",
  ], "index.html no longer builds the ancestry this file resolves the cascade against");

  // Both children hang off the badge itself. Their TAGS are the half round 3 caught being wrong, so
  // they are pinned by name: the title is a <strong>, the action is a <span>.
  const under = (id) => [...docAncestors(docElement(`id="${id}"`).tagStart), docElement(`id="${id}"`)].map(shape);
  assert.deepEqual(under("sync-pill-title"), [
    "html",
    "body",
    "main.app-shell",
    "button#director-mode-badge.director-mode-badge.is-hidden",
    "strong#sync-pill-title.director-mode-badge-title",
  ], "the pill title moved, or changed element — the cascade fixture below describes a different document");
  assert.deepEqual(under("sync-pill-action"), [
    "html",
    "body",
    "main.app-shell",
    "button#director-mode-badge.director-mode-badge.is-hidden",
    "span#sync-pill-action.director-mode-badge-exit",
  ], "the pill action moved, or changed element — the cascade fixture below describes a different document");

  // And the runtime half is modelled, not read: <html> in the file carries neither attribute.
  assert.equal(badgeChain[0].attrs["data-role"], "follower");
  assert.equal(badgeChain[0].attrs["data-shell"], "native");
});

test("FINDING: the following action is HIDDEN, not dimmed — the dot design won and this test was wrong", () => {
  // WHAT THIS TEST USED TO CLAIM, and why it was believed. Hiding the action made the pill look
  // inert, so the rule was written to dim it instead: `.director-mode-badge.is-following
  // .director-mode-badge-exit { opacity: 0.62 }` (still in the sheet). The test sliced to the FIRST
  // rule mentioning that selector and asserted it was not display:none — and a first match cannot
  // see an override. Two screens further down, `html[data-role="follower"]
  // .director-mode-badge.is-following .director-mode-badge-exit { display: none }` sets exactly the
  // opposite, at higher specificity, for exactly the role this state can only ever occur in
  // (renderDirectorModeBadge writes data-role="follower" whenever nativeSyncRole !== "director",
  // and syncPillState can only return "following" in that same case).
  //
  // So the assertion had been passing while the shipped behaviour was its opposite, for as long as
  // the follower pill has been collapsing to a breathing green dot on the ⟳ fab — the 2026-08-17
  // decision that SIGUIENDO was a claim that lied. The dot is deliberate and documented in both the
  // sheet and app.js; the old assertion is the stale half. This test now pins the DOT, resolved
  // through the whole cascade, so the first-match blindness cannot come back.
  //
  // ROUND 3 found the resolver underneath it scoring rules at the wrong specificity (first matching
  // selector of a list rather than the highest) and modelling the title as a <span> when it ships as
  // a <strong>; both are fixed above, so these answers are now decided against the real document.
  const exit = resolve("display", childChain("director-mode-badge-exit", "sync-pill-action"));
  const title = resolve("display", childChain("director-mode-badge-title", "sync-pill-title"));
  assert.ok(exit, "nothing in the sheet decides the follower action's display any more");
  assert.equal(exit.value, "none",
    `the follower action resolves to display:${exit.value} (line ${exit.line}) — it must not render beside a 0.6rem dot`);
  assert.ok(title, "nothing in the sheet decides the follower title's display any more");
  assert.equal(title.value, "none",
    `the follower title resolves to display:${title.value} (line ${title.line}) — SIGUIENDO is back on screen`);

  // The vestigial opacity rule must be seen to LOSE, not merely to exist: this is the exact shape
  // the old slice could not read.
  const dimmer = matchingRules(childChain("director-mode-badge-exit", "sync-pill-action"))
    .find((r) => /opacity/.test(r.body));
  if (dimmer) {
    assert.ok(exit.spec > dimmer.spec,
      `the dimming rule at line ${dimmer.line} now outranks the hide at line ${exit.line} — read the cascade before believing either`);
  }

  // And the badge itself is the dot. If this collapses back to a text pill the two assertions above
  // stop describing anything a person sees, so they are pinned together.
  const radius = resolve("border-radius", badgeChain);
  const width = resolve("width", badgeChain);
  assert.ok(radius && /50%/.test(radius.value), `the follower badge is not round (border-radius ${radius && radius.value}) — it is a pill again, not a status light`);
  assert.ok(width && /^0\.\d+rem$/.test(width.value), `the follower badge resolves to width ${width && width.value} — a dot is sub-rem; a control is not`);
});
