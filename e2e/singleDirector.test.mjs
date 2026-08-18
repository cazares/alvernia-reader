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

test("a device that was directing when it died comes back as a FOLLOWER and is handed the way back", () => {
  const b = bootstrap();
  assert.match(b, /prev === "director"/, "the persisted role is not even consulted for the notice");
  // THE NOTICE CARRIES THE CONTROL, it does not describe one. This asserted the old sentence
  // "Toca el estado arriba a la izquierda…" — which had been WRONG since build 435, when the
  // top-left status stopped taking the role, and which this test happily pinned in place. A test
  // that freezes a sentence cannot notice the sentence has become a lie; e2e/noticesCarryControls
  // now bans directional wording outright, which is the property that actually matters.
  assert.match(b, /text: "Estabas dirigiendo cuando se cerró el app\."/, "the ex-director is not told what happened");
  assert.match(b, /action: "resume-director"/, "the notice carries no way back — it is an announcement, not a control");
  // and the flag is written back so the notice fires once per crash, not forever
  assert.match(b, /setItem\(STORAGE_KEYS\.lastSyncRole, "follower"\)/, "lastSyncRole is not cleared after the notice");
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

test("a new director re-browses immediately — WITHOUT going invisible to the followers reaching for it", () => {
  const start = SRC.indexOf("const becomeDirector = useCallback(");
  const end = SRC.indexOf("// ── Soft reset", start);
  const fn = SRC.slice(start, end);

  // The kick must still happen: a brand-new director has to find rivals fast, or two of them sit
  // split for a whole browse cycle while followers flap between them.
  assert.match(fn, /roleRef\.current = "director";[\s\S]*refreshDirectorBrowse\(\)/,
    "no rediscovery kick after becoming director");

  // AND IT MUST BE BROWSER-ONLY. This used refreshNearbyDiscovery, which destroys the ADVERTISER as
  // its first act — fired at the exact moment every follower's foundPeer had triggered and their
  // invites were in flight. Multipeer drops an invite to a vanished advertiser SILENTLY, so each
  // follower then waited out a timeout before retrying: most of the observed ~10s convergence.
  //
  // The follower side already had this exact protection ("NEVER tear down the advertiser/browser
  // while a connection is actively being established"); the director had none while doing it to
  // everyone trying to reach it. Asserting the ABSENCE matters as much as the presence — the old
  // call is one autocomplete away and fails nothing else.
  assert.doesNotMatch(fn, /refreshNearbyDiscovery\(\)/,
    "becomeDirector destroys its own advertiser again — every in-flight follower invite dies silently");

  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const browserOnly = swift.slice(swift.indexOf("private func refreshBrowserOnly()"));
  // Comments stripped: the body necessarily EXPLAINS that it leaves the advertiser alone, so a raw
  // scan for "advertiser" fails on the rationale rather than on the code. Same trap that made an
  // earlier test assert against its own explanation.
  const body = browserOnly.slice(0, browserOnly.indexOf("\n  }"))
    .replace(/\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");
  assert.ok(body.length > 20, "refreshBrowserOnly is gone");
  assert.doesNotMatch(body, /advertiser\??\./,
    "refreshBrowserOnly touches the advertiser — that is the whole thing it exists not to do");
  assert.match(body, /startBrowsing\(\)/, "it does not restart browsing, so it refreshes nothing");
  // Ghost peers still have to be cleared: an MCPeerID dies with the browser that found it.
  assert.match(body, /discoveredDirectors\.removeAll\(\)/,
    "stale peers survive the browser that found them — invites to them evaporate");
});

test("becoming director does not immediately tear down the browser it just created", () => {
  // startDirector calls startBrowsing directly; becomeDirector then kicks a re-browse milliseconds
  // later for split-brain convergence. Without a guard that kick destroys a browser that is already
  // as fresh as a refresh could make it, discarding peers it may have just found — churn during the
  // exact window every follower is trying to reach this device.
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const sb = swift.slice(swift.indexOf("private func startBrowsing()"));
  assert.match(sb.slice(0, sb.indexOf("\n  }")), /lastRefreshAt = Date\(\)\.timeIntervalSince1970/,
    "a fresh browser does not stamp the refresh clock, so an immediate re-browse is not throttled");
  // The throttle it relies on has to still be there, and be long enough to cover the kick.
  const throttle = Number(swift.match(/minRefreshInterval: TimeInterval = ([\d.]+)/)[1]);
  assert.ok(throttle >= 1.0, `minRefreshInterval ${throttle}s is too short to absorb the startup kick`);
  assert.match(swift, /guard now - lastRefreshAt >= Self\.minRefreshInterval else \{ return \}/,
    "the refresh throttle is gone — every caller now has to know how old the browser is");
});

test("the invite retry cadence is NOT the invite timeout", () => {
  // They were the same number, so a silently-dropped invite — no delegate callback, which is exactly
  // what a restarted advertiser produces — cost a full 8s of a follower doing nothing before
  // anything retried. Two different questions: how long Multipeer holds the invite open, and how
  // long WE wait before concluding it died.
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const timeout = Number(swift.match(/inviteTimeout: TimeInterval = ([\d.]+)/)[1]);
  const retry = Number(swift.match(/inviteRetryAfter: TimeInterval = ([\d.]+)/)[1]);
  assert.ok(retry < timeout, `retry ${retry}s must be shorter than the invite timeout ${timeout}s`);
  assert.ok(retry >= 1.5, `retry ${retry}s is too eager — invites need room to complete normally`);
  assert.match(swift, /if elapsed < Self\.inviteRetryAfter/,
    "the retry decision reads inviteTimeout again — that is the conflation this fixed");
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



test("the hunting pulse survives a reconnect, instead of dying when it is needed most", () => {
  // forceFollowerReconnect used to stopFollowerWatchdog() "and restart cleanly on the next
  // .connected" — but it runs precisely when we are NOT connected, so the 0.5 Hz pulse that retries
  // the handshake died at the moment it was needed and stayed dead until a connection it existed to
  // help produce. It also drives the BLE scan self-heal, so stopping it went deaf as well as blind.
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const fn = swift.slice(swift.indexOf("private func forceFollowerReconnect(staleFor"));
  const body = fn.slice(0, fn.indexOf("\n  }"));
  assert.match(body, /startFollowerWatchdog\(\)/, "the reconnect path leaves the follower without a pulse");
  assert.doesNotMatch(body.replace(/\/\/.*$/gm, ""), /stopFollowerWatchdog\(\)/,
    "the watchdog is stopped again on the one path that most needs it running");
});

test("a follower that can SEE a director but never connects rebuilds its session", () => {
  // Retrying an invite cannot fix a wedged MCSession: same session, same result, forever — a
  // follower stuck at "connecting" with the director plainly visible. This is the escalation the
  // resync button performs, reached without needing a human to notice.
  const swift = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
  const wd = swift.slice(swift.indexOf("private func startFollowerWatchdog"));
  const body = wd.slice(0, wd.indexOf("\n  private func"));
  assert.match(body, /watchdog:wedged-rebuild/, "no escalation from retrying invites to rebuilding the session");
  assert.match(body, /forceFollowerReconnect\(staleFor:/, "the escalation does not actually rebuild anything");

  // Gated on a director being IN SIGHT — otherwise the problem is discovery and a rebuild is churn.
  assert.match(body, /!self\.discoveredDirectors\.isEmpty/,
    "it rebuilds even with no director discovered, which churns for no reason");
  // And bounded: the clock must reset, or this fires every tick forever.
  assert.match(body, /self\.followerHuntingSince = Date\(\)\.timeIntervalSince1970/,
    "the hunting clock is not reset on escalation — this would rebuild the session every 0.5s");

  const wedged = Number(swift.match(/followerWedgedSeconds: TimeInterval = ([\d.]+)/)[1]);
  const retry = Number(swift.match(/inviteRetryAfter: TimeInterval = ([\d.]+)/)[1]);
  assert.ok(wedged > retry * 4,
    `wedged threshold ${wedged}s must leave room for several invite retries (${retry}s each) first`);

  // The clock must stop on success, or a connected follower eventually rebuilds for no reason.
  assert.match(swift, /self\.followerHuntingSince = 0/, "the hunting clock never clears on connect");
});
