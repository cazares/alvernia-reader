import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const SRC = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");

// ── ONE DIRECTOR. ONLY A HUMAN MAKES ONE. ─────────────────────────────────────
// (Miguel, 2026-08-15, the night before Mass, after watching physical devices split between two
// directors: "only ever allow one and only one director." And 2026-07-02: "don't just auto-make
// anyone director — always ask, always.")
//
// Between 2026-08-05 and build 427 PdfReaderApp.tsx could mint a director by itself — a crash-resume
// and a "habitual" seat claim — each deciding the seat was empty from "no director page heard in
// 8s". A director the radio has discovered but not yet connected to sends no pages, so the automatic
// claim fired beside a human's, carried the NEWER token, and the mesh (newest wins) demoted the
// human. These tests pin the source so that cannot come back with a comment claiming it is safe.
//
// Source-level pins, like the rest of e2e/: they read PdfReaderApp.tsx and web/src/app.js and
// assert on the shape of the code, because the thing under test is "which paths can call
// becomeDirector at all", and that is a property of the source.

// The bootstrap effect: from its comment banner to the mesh event listener registration.
const bootstrap = () => {
  const start = SRC.indexOf("// ── Multipeer permissions + role bootstrap + event listener");
  const end = SRC.indexOf("const sub = addNearbyDirectorSyncListener", start);
  assert.ok(start > 0 && end > start, "cannot find the role bootstrap block");
  return SRC.slice(start, end);
};

// Every call of becomeDirector( — not its definition, not a dependency-array mention.
const becomeDirectorCallSites = () => {
  const lines = SRC.split("\n");
  return lines
    .map((l, i) => ({ l, n: i + 1 }))
    .filter(({ l }) => /becomeDirector\(/.test(l))
    .filter(({ l }) => !/const becomeDirector\s*=/.test(l))
    .filter(({ l }) => !/^\s*becomeDirector,\s*$/.test(l));
};

test("the boot path never calls becomeDirector — a device always boots as a follower", () => {
  const b = bootstrap();
  assert.doesNotMatch(b, /becomeDirector\(/, "the bootstrap promotes the device on its own");
  assert.match(b, /becomeFollower\(\);/, "the bootstrap does not even follow");
});

test("no timer, tally or persisted role can promote this device (the machinery is gone)", () => {
  for (const gone of [
    "resumeTimerRef",
    "DIRECTOR_RESUME_SETTLE_MS",
    "DIRECTOR_RESUME_WINDOW_MS",
    "HABIT_MAX_EXTRA_MS",
    "HABIT_STEP_MS",
    "crashFresh",
    "habitual",
  ]) {
    assert.ok(!SRC.includes(gone), `${gone} is back in PdfReaderApp.tsx — an automatic director path is returning`);
  }
  // The tally may still be COUNTED (it is a diagnostic) but it must not be READ to decide anything.
  const reads = SRC.match(/AsyncStorage\.getItem\(STORAGE_KEYS\.directorSessions\)/g) || [];
  // The one legitimate read is the read-modify-write inside bumpDirectorSessions.
  assert.ok(reads.length <= 1, `directorSessions is read ${reads.length}x — only the bump may read it`);
});

test("becomeDirector is reachable from exactly one place: the human confirm in onDirectorCode", () => {
  const sites = becomeDirectorCallSites();
  assert.equal(
    sites.length, 1,
    `expected exactly one becomeDirector( call site, found ${sites.length}:\n` + sites.map((s) => `  ${s.n}: ${s.l.trim()}`).join("\n"),
  );
  const [only] = sites;
  assert.match(only.l, /onPress:\s*\(\)\s*=>\s*becomeDirector\(code\)/, `the one call site is not the confirm-dialog onPress: ${only.l.trim()}`);
  // …and that dialog lives inside onDirectorCode.
  const fnStart = SRC.indexOf("const onDirectorCode = useCallback(");
  const fnEnd = SRC.indexOf("[injectEvent, performSoftReset, becomeDirector]", fnStart);
  assert.ok(fnStart > 0 && fnEnd > fnStart, "cannot find onDirectorCode");
  const lineOffset = SRC.slice(0, fnStart).split("\n").length;
  const lineEnd = SRC.slice(0, fnEnd).split("\n").length;
  assert.ok(only.n >= lineOffset && only.n <= lineEnd, `the call site (line ${only.n}) is outside onDirectorCode (${lineOffset}-${lineEnd})`);
});

test("onDirectorCode is fed only by human actions: the numpad code and the pill", () => {
  const callers = SRC.split("\n")
    .map((l, i) => ({ l, n: i + 1 }))
    .filter(({ l }) => /onDirectorCode\(/.test(l) && !/const onDirectorCode/.test(l));
  // Each caller must be one of: the bridge "director-code" (typed), "request-director" (pill tap),
  // or a rescue confirm passing a NON-director code (soft reset / force baked).
  const allowed = [
    /onDirectorCode\(msg\.code\)/,
    /onDirectorCode\(DIRECTOR_CODE\)/,
    /onDirectorCode\(BOOK_FORCE_BAKED_CODE\)/,
    /onDirectorCode\(SOFT_RESET_CODE\)/,
  ];
  for (const c of callers) {
    assert.ok(allowed.some((re) => re.test(c.l)), `unexpected onDirectorCode caller at ${c.n}: ${c.l.trim()}`);
  }
  // The DIRECTOR_CODE caller is the pill's request-director bridge case, and nothing else.
  const pill = callers.filter((c) => /onDirectorCode\(DIRECTOR_CODE\)/.test(c.l));
  assert.equal(pill.length, 1, "DIRECTOR_CODE is passed to onDirectorCode from more than one place");
  const before = SRC.split("\n").slice(pill[0].n - 12, pill[0].n).join("\n");
  assert.match(before, /case "request-director":/, "the DIRECTOR_CODE call is not the request-director bridge case");
});

test("a device that was directing when it died comes back as a FOLLOWER and is told to tap the pill", () => {
  const b = bootstrap();
  assert.match(b, /prev === "director"/, "the persisted role is not even consulted for the toast");
  assert.match(b, /Estabas dirigiendo\. Toca el estado arriba a la izquierda para volver a dirigir\./, "no toast points the ex-director at the pill");
  // and the flag is written back so the toast fires once per crash, not forever
  assert.match(b, /setItem\(STORAGE_KEYS\.lastSyncRole, "follower"\)/, "lastSyncRole is not cleared after the toast");
});

test("when the mesh demotes this director, it steps down AND the person is told", () => {
  const start = SRC.indexOf('if (String(event.code ?? "") === "DIRECTOR_CONFLICT") {');
  assert.ok(start > 0, "no DIRECTOR_CONFLICT handler");
  const block = SRC.slice(start, SRC.indexOf('case "takeover-request"', start));
  assert.match(block, /stopDirectorHeartbeat\(\);/, "keeps broadcasting after losing the seat");
  assert.match(block, /becomeFollower\(\);/, "does not step down");
  assert.match(block, /requestCurrentSnapshot\(\)/, "does not re-home onto the winner's page");
  assert.match(block, /Otro dispositivo tomó la dirección del coro/, "the demoted human is not told");
});

test("a new director forces immediate rediscovery so two directors converge in seconds, not a browse cycle", () => {
  const start = SRC.indexOf("const becomeDirector = useCallback(");
  const end = SRC.indexOf("// ── Soft reset", start);
  const fn = SRC.slice(start, end);
  assert.match(fn, /roleRef\.current = "director";[\s\S]*refreshNearbyDiscovery\(\)/, "no rediscovery kick after becoming director");
});

test("the Swift mesh still resolves two directors deterministically (newest token wins) — the JS relies on it", () => {
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  assert.match(swift, /private func handleDirectorConflict\(with otherToken: String\)/);
  assert.match(swift, /if otherToken > currentDirectorToken \{[\s\S]*?emitError\(code: "DIRECTOR_CONFLICT"/, "the lower token no longer demotes");
  assert.match(swift, /String\(format: "%020lld", Int64\(Date\(\)\.timeIntervalSince1970 \* 1_000_000\)\)/, "the token no longer orders by time");
});

// ── The pill (moved from directorSeat.test.mjs, still true) ──────────────────

test("the pill shows nobody-directing ONLY on the mesh's own verdict", () => {
  // Inferring it from silence would light the warning during the ~10s of boot discovery on every
  // device every Sunday, and a warning that cries wolf is one nobody reads by the third week.
  const fn = APP.slice(APP.indexOf("const syncPillState"), APP.indexOf("const renderDirectorModeBadge"));
  assert.match(fn, /lastMeshStatus === "self-directed"/, "nobody-state is guessed, not reported");
  assert.match(fn, /return "following";\s*\};/, "the default is not the safe one");
});

test("the pill never appears on the public web", () => {
  const fn = APP.slice(APP.indexOf("const renderDirectorModeBadge"), APP.indexOf("// ── Sync \"working\""));
  assert.match(fn, /NATIVE_FILE_MODE \|\| hasNativeBridge\(\)/, "not gated to the native shell");
});

test("the pill REPORTS state, and is the ONLY way to drop the role", () => {
  // WHAT CHANGED, twice. In 435 the pill stopped acting entirely: both directions moved into the
  // IR A CANTO modal behind a typed-word gate, because a permanently visible become-director
  // control on six choir iPads is a split-brain generator.
  //
  // On 2026-08-18 the owner moved the EXIT back onto the pill: "tapping DIRECTOR should give you
  // the modal if you wanna quit being a director", and then "no X pill needed anymore, remove plz".
  // The status is now also the way out. That is one element with two meanings, which is normally
  // the bug — it is right here because both meanings are the SAME FACT: it reads the role you hold,
  // and tapping asks whether you meant to stop holding it. The label can never lie about the act.
  //
  // The asymmetry is the invariant, not the count of controls:
  //   TAKING  the role changes the page on every device in the loft -> ONE site, behind the gate.
  //   DROPPING it is recoverable                                    -> reachable without hunting.
  const h = APP.slice(APP.indexOf("if (directorModeBadge) directorModeBadge.addEventListener"), APP.indexOf("songCancelButton.addEventListener"));

  // UNCHANGED AND NON-NEGOTIABLE. The pill is the control most likely to be tapped by accident, so
  // a take-the-role path here would be a second, UNGATED hole straight through the typed-word gate.
  // directorButton.test.mjs pins the matching half: exactly ONE site asks for the role.
  assert.ok(!/request-director/.test(h), "the pill can TAKE the role — that path must live only behind the gate");

  // The exit lives here now, and nowhere else — the separate ✕ fab was removed with it.
  assert.match(h, /exit-director/, "the pill no longer drops the role, and the ✕ is gone — nothing can");
  assert.match(h, /window\.confirm\("¿Salir de director\?"\)/,
    "the exit is unconfirmed — a stray tap on the status would drop the choir's director mid-Mass");
  const HTML = fs.readFileSync("web/src/index.html", "utf8");
  assert.doesNotMatch(HTML, /id="sync-exit-fab"/, "a second exit is back — one act, one control");

  // While NOT directing there is nothing to drop, so the tap teaches instead of acting.
  assert.match(h, /openSongJump\(\)/, "the pill should point at the control instead of acting");
});

