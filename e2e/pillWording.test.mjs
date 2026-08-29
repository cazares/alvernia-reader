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
  const aria = HTML.slice(HTML.indexOf('id="become-director-pill"')).match(/aria-label="([^"]*)"/);
  assert.ok(aria, "the entry control lost its aria-label");
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
// Every rule in the sheet, in source order, with its at-rule context. Rules are bounded by their
// own braces, never by a character count.
const cssRules = (() => {
  const src = CSS.replace(/\/\*[\s\S]*?\*\//g, " ");
  const out = [];
  const conds = [];
  let buf = "";
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    if (c === "{") {
      const prelude = buf.trim();
      buf = "";
      if (prelude.startsWith("@")) { conds.push(prelude); continue; }
      const end = src.indexOf("}", i);
      assert.ok(end > i, `the rule "${prelude}" is never closed`);
      out.push({
        selectors: prelude.split(",").map((s) => s.trim()).filter(Boolean),
        body: src.slice(i + 1, end),
        order: out.length,
        conds: [...conds],
        line: src.slice(0, i).split("\n").length,
      });
      i = end;
      continue;
    }
    if (c === "}") { conds.pop(); continue; }
    buf += c;
  }
  assert.equal(conds.length, 0, "styles.css has an unbalanced at-rule block");
  return out;
})();

// A compound like `html[data-role="follower"]`, parsed strictly: anything this does not understand
// throws instead of being silently treated as "no constraint", which is how a matcher starts
// matching more than it should.
const parseCompound = (text) => {
  const part = { tag: null, ids: [], classes: [], attrs: [], pseudos: [] };
  const token = /^(?:([a-zA-Z][\w-]*)|#([\w-]+)|\.([\w-]+)|\[([^\]]+)\]|(::?[\w-]+(?:\([^)]*\))?))/;
  let rest = text;
  while (rest.length) {
    const m = token.exec(rest);
    if (!m) throw new Error(`cannot parse the selector fragment "${text}" at "${rest}"`);
    if (m[1]) part.tag = m[1];
    else if (m[2]) part.ids.push(m[2]);
    else if (m[3]) part.classes.push(m[3]);
    else if (m[4]) part.attrs.push(m[4]);
    else part.pseudos.push(m[5]);
    rest = rest.slice(m[0].length);
  }
  return part;
};

// The RESTING state only: a compound carrying :hover/:active/::before describes a moment, not the
// pill sitting on screen, so it is excluded rather than counted as a match.
const compoundMatches = (el, text) => {
  const c = parseCompound(text);
  if (c.pseudos.length) return false;
  if (c.tag && c.tag !== el.tag) return false;
  if (c.ids.some((id) => id !== el.id)) return false;
  if (c.classes.some((cl) => !el.classes.includes(cl))) return false;
  return c.attrs.every((a) => {
    const eq = a.indexOf("=");
    if (eq < 0) return a in el.attrs;
    const name = a.slice(0, eq).trim();
    const want = a.slice(eq + 1).trim().replace(/^["']|["']$/g, "");
    return el.attrs[name] === want;
  });
};

const specificity = (sel) => {
  let ids = 0, cls = 0, els = 0;
  for (const compound of sel.split(/\s+/)) {
    const c = parseCompound(compound);
    ids += c.ids.length;
    cls += c.classes.length + c.attrs.length + c.pseudos.filter((p) => !p.startsWith("::")).length;
    els += (c.tag ? 1 : 0) + c.pseudos.filter((p) => p.startsWith("::")).length;
  }
  return ids * 10000 + cls * 100 + els;
};

// `chain` is the element and its modelled ancestors, outermost first. Descendant combinators only:
// a selector using > + or ~ throws, because this resolver does not model sibling or direct-child
// relationships and a wrong "no match" is indistinguishable from a real absence.
const selectorMatches = (sel, chain) => {
  if (/[>+~]/.test(sel)) throw new Error(`selector "${sel}" uses a combinator this resolver cannot decide`);
  const parts = sel.split(/\s+/).filter(Boolean);
  if (!compoundMatches(chain[chain.length - 1], parts[parts.length - 1])) return false;
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
const matchingRules = (chain) => {
  const hits = [];
  for (const r of cssRules) {
    if (r.conds.some((c) => c.startsWith("@keyframes"))) continue;
    for (const sel of r.selectors) {
      let ok = false;
      try { ok = selectorMatches(sel, chain); } catch { ok = false; }
      if (!ok) continue;
      assert.deepEqual(r.conds, [], `line ${r.line}: "${sel}" now matches from inside ${r.conds.join(" ")} — this resolver cannot decide a conditional rule`);
      hits.push({ ...r, sel, spec: specificity(sel) });
      break;
    }
  }
  return hits.sort((a, b) => a.spec - b.spec || a.order - b.order);
};

// !important first, then specificity, then source order — and a later declaration inside the same
// rule beats an earlier one, which is why the last comparison is >=.
const beats = (a, b) => {
  if (!b) return true;
  if (a.important !== b.important) return a.important;
  if (a.spec !== b.spec) return a.spec > b.spec;
  return a.order >= b.order;
};

const resolve = (prop, chain) => {
  let winner = null;
  for (const r of matchingRules(chain)) {
    for (const d of r.body.split(";")) {
      const colon = d.indexOf(":");
      if (colon < 0) continue;
      if (d.slice(0, colon).trim() !== prop) continue;
      const raw = d.slice(colon + 1).trim();
      const cand = {
        value: raw.replace(/\s*!important$/, ""),
        important: /!important$/.test(raw),
        spec: r.spec, order: r.order, line: r.line, sel: r.sel,
      };
      if (beats(cand, winner)) winner = cand;
    }
  }
  return winner;
};

const FOLLOWER_HTML = { tag: "html", id: null, classes: [], attrs: { "data-role": "follower", "data-shell": "native" } };
const FOLLOWING_BADGE = { tag: "button", id: "director-mode-badge", classes: ["director-mode-badge", "is-following"], attrs: {} };
// <body> carries no class in the resting state — the drawer-open rule that hides the whole cluster
// is a different moment, and modelling body explicitly is what keeps it from matching by accident.
const badgeChain = [FOLLOWER_HTML, { tag: "body", id: null, classes: [], attrs: {} }, FOLLOWING_BADGE];
const childChain = (cls, id) => [...badgeChain, { tag: "span", id, classes: [cls], attrs: {} }];

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
  const exit = resolve("display", childChain("director-mode-badge-exit", "sync-pill-action"));
  const title = resolve("display", childChain("director-mode-badge-title", "sync-pill-title"));
  assert.ok(exit, "nothing in the sheet decides the follower action's display any more");
  assert.equal(exit.value, "none",
    `the follower action resolves to display:${exit.value} (line ${exit.line}) — it must not render beside a 0.6rem dot`);
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
