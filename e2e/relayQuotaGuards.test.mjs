import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const WORKER = fs.readFileSync("sync-worker/src/index.ts", "utf8");
const WCONF = fs.readFileSync("sync-worker/wrangler.jsonc", "utf8");

// WHY THIS FILE EXISTS. Cloudflare's free plan gives the ACCOUNT 100,000 Worker/Pages-Function
// requests a day, and signovivo.com shares that number with the relay. Twice — 2026-08-17 and again
// on 2026-08-18 — non-essential traffic ate the whole thing and took the SITE down with it.
//
// The 2026-08-18 failure was self-sustaining: a 429 killed the WebSocket, backoff climbed to its 8s
// cap and ALSO started a 4s /state poll, and then the 10s health timer reset backoff to 500ms and
// restarted both. ~20-25 requests/minute/device, four devices, over the daily cap on their own,
// forever. The outage fed itself and could not recover even after the traffic that started it had
// stopped.

test("THE ARITHMETIC: the old polling config exceeded the daily quota by itself", () => {
  // This is the test that would have caught it before a human did. It is arithmetic, not a guess.
  const DAILY_CAP = 100_000, DEVICES = 4;
  const perDevicePerDay = (pollMs) => Math.floor((24 * 60 * 60 * 1000) / pollMs);
  // 4s = 21,600/device/day = 86,400 across four devices: 86% of the ENTIRE account quota spent on
  // fallback polling alone, before the site, the WebSocket reconnects, the 10s health timer or any
  // telemetry. Not over the cap by itself — the first draft of this test claimed it was, and the
  // arithmetic said otherwise — but it left so little headroom that anything else finished the job.
  const old4s = perDevicePerDay(4000) * DEVICES;
  assert.ok(old4s > DAILY_CAP * 0.75,
    `the 4s poll spent ${old4s}/day, under 75% of the cap — recheck this test, not the config`);
  const current = Number(APP.match(/const RELAY_POLL_MS = (\d+)/)[1]);
  assert.ok(perDevicePerDay(current) * DEVICES < DAILY_CAP / 2,
    `a ${current}ms poll across ${DEVICES} devices spends ${perDevicePerDay(current) * DEVICES}/day — ` +
    "more than half the account's entire quota on fallback polling alone");
});

test("a 429 stops the client instead of making it try harder", () => {
  assert.match(APP, /if \(r\.status === 429\) \{ tripRelayCooldown/,
    "a 429 no longer trips the breaker — the client will retry through the outage that caused it");
  const cooldown = Number(APP.match(/const RELAY_COOLDOWN_MS = (\d+) \* 60 \* 1000/)[1]);
  assert.ok(cooldown >= 5, `a ${cooldown}-minute cooldown is too short to let a quota window recover`);
  // Every outbound path must honour it, or the breaker leaks.
  for (const [what, re] of [
    ["polling", /const relayPollOnce = async \(force = false\) => \{\s*\n\s*if \(relayCooling\(\)\) return;/],
    ["startRelayPolling", /const startRelayPolling = \(\) => \{\s*\n\s*if \(relayCooling\(\)\) return;/],
    ["connectRelay", /if \(relayCooling\(\)\) return;/],
    ["health timer", /if \(relay\.manualClose \|\| relayCooling\(\)\) return;/],
  ]) assert.match(APP, re, `${what} ignores the cooldown, so the breaker leaks`);
});

test("the health timer no longer hands back the backoff the socket earned", () => {
  // It reset backoff to 500ms every 10s, so the exponential climb never survived one cycle — on
  // exactly the paths where the far end was already overloaded.
  const t = APP.slice(APP.indexOf("relay.healthTimer = setInterval"));
  const body = t.slice(0, t.indexOf("}, 10000);"));
  assert.doesNotMatch(body, /relay\.backoff = 500/,
    "the health timer resets backoff again — exponential backoff cannot survive it");
});

test("signovivo.com is NEVER gated — only the native shell's redundant subscribe is", () => {
  // THE LINE THAT MUST NOT MOVE. The relay is the ONLY sync a web follower has; gating it there
  // would delete the product. A native follower already has the mesh and BLE, both of which work at
  // Mass where there is no internet at all.
  const fn = APP.slice(APP.indexOf("const relaySubscribeEnabled"));
  const body = fn.slice(0, fn.indexOf("\n};"));
  assert.match(body, /if \(!native\) return true;/,
    "the web path can now be gated off — signovivo.com would lose sync entirely");
  assert.ok(body.indexOf("if (!native) return true;") < body.indexOf("localStorage"),
    "the web short-circuit is not first, so a storage failure could disable signovivo.com");
});

test("telemetry is opt-in and drops rather than queues", () => {
  assert.match(NATIVE, /if \(!telemetryEnabledRef\.current\) \{ dbgBufferRef\.current = \[\]; return; \}/,
    "telemetry sends by default again — it has no user value and it caused two outages");
  // And OFF must also clear the buffer, or a device accumulates rows it will never send.
  assert.match(NATIVE, /telemetryEnabledRef = useRef\(false\)/, "telemetry does not default to off");
  const flush = NATIVE.slice(NATIVE.indexOf("const dbgFlush = useCallback"), NATIVE.indexOf("const dbgLog = useCallback"));
  // RETENTION DEPENDS ON THE DESTINATION. Failing to the Cloudflare worker must DROP — a queue that
  // survives a Mass is a burst waiting for the next wifi, from the same quota signovivo.com lives on.
  // Failing to the LAN sink must KEEP: no quota exists there, and buffering through a no-network run
  // then flushing afterwards is the only way to see what happened at Mass, where the iPads join
  // nothing at all and telemetry has never existed.
  assert.match(flush, /if \(!sink\) return;/, "a failed send to the WORKER is retried/queued — that is the outage");
  assert.match(flush, /const MAX = 5000;/, "the local buffer is unbounded — a memory leak on a device with no network");
  assert.match(flush, /merged\.slice\(merged\.length - MAX\)/,
    "overflow discards the NEWEST rows; when a buffer overflows the recent end is the interesting one");
});

test("the worker RESERVES quota for signovivo.com, and never counts the product's own traffic", () => {
  assert.match(WORKER, /async spendNonEssential\(day: string, cap: number\)/, "no budget meter exists");
  assert.match(WORKER, /getByName\("__budget__"\)\.spendNonEssential/, "the gate never spends from the budget");
  assert.match(WORKER, /"Retry-After": "3600"/, "a refused request does not tell the caller when to return");

  // The essential paths must be OUTSIDE the counted set. /r/ carries state, subscribe and publishes
  // — the actual sync — and must never be refused or even metered.
  const gate = WORKER.slice(WORKER.indexOf("const NON_ESSENTIAL ="));
  const set = gate.slice(0, gate.indexOf(";"));
  for (const essential of ["/r/", "/health"]) {
    assert.ok(!set.includes(essential), `${essential} is counted as non-essential — the product would be refused`);
  }
  assert.ok(set.includes("/log"), "/log is not metered, so it can starve the site again");
  // /fleet is not metered because it no longer EXISTS — the readiness dashboard was removed on
  // 2026-08-18. It posted every 90s from every device (~3,840/day, more than the director's relay
  // keepalive) so a pre-Mass page could show green lights, and it held a roster of choir phone
  // numbers. Deleting a surface beats budgeting for it.
  assert.doesNotMatch(WORKER, /url\.pathname === "\/fleet/,
    "a /fleet route is back — it was removed for cost AND because it served phone numbers");
  assert.doesNotMatch(WORKER, /renderFleetDashboard\(/, "the dashboard renderer is back");
  const NATIVE_SRC = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  assert.doesNotMatch(NATIVE_SRC, /setInterval\(\(\) => fleetCheckin\(\), \d+\)/,
    "the 90s fleet check-in heartbeat is back — that was ~3,840 requests a day for a status page");
  assert.doesNotMatch(NATIVE_SRC, /fetch\(`\$\{RELAY_BASE\}\/fleet\/checkin`/,
    "the native app posts fleet check-ins again");
  assert.doesNotMatch(APP, /fetch\(RELAY_BASE \+ "\/fleet\/checkin"/,
    "the web app posts fleet check-ins again");

  // The reservation has to leave the great majority for the product.
  const cap = Number(WCONF.match(/"NONESSENTIAL_DAILY_MAX": "(\d+)"/)[1]);
  assert.ok(cap > 0 && cap <= 25_000,
    `a ${cap}/day non-essential budget leaves too little of the 100k daily quota for signovivo.com`);
});

test("telemetry levels exist, default OFF, and a typo goes QUIET not loud", async () => {
  const m = await import("../sync-worker/src/logBuffer.js");
  assert.deepEqual(Object.keys(m.LOG_LEVELS), ["off", "error", "warn", "info", "debug"]);
  assert.equal(m.logLevel({}), 0, "the default is not OFF — two outages came from unrequested telemetry");
  assert.equal(m.logLevel({ LOG_LEVEL: "bogus" }), 0,
    "a level typo falls back to a NOISY default; it must go quiet — a lost debugging session beats a spent quota");
  assert.equal(m.logLevel({ LOG_LEVEL: "debug" }), 4);
  assert.equal(m.logLevel({ LOG_LEVEL: "ERROR" }), 1, "levels are case-sensitive");
  assert.equal(m.logLevel({ LOG_LEVEL: "2" }), 2, "a numeric level is rejected");
});

test("the native classifier and the worker classifier cannot drift", async () => {
  // Two copies of one rule drift the moment either moves, and the drift is silent: the fleet would
  // filter by one table while the operator tuned the other. So both are EXECUTED and compared.
  const m = await import("../sync-worker/src/logBuffer.js");
  const src = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const body = src.slice(src.indexOf("const levelForEvent = useCallback"), src.indexOf("}, []);", src.indexOf("const levelForEvent")));
  const inner = body.slice(body.indexOf("{") + 1);
  const LOG_LEVELS = { off: 0, error: 1, warn: 2, info: 3, debug: 4 };
  const nativeLevelFor = new Function("LOG_LEVELS", "event", inner.replace(/:\s*number/g, ""));

  const SAMPLES = [
    "mesh:error", "refresh:STORM", "watchdog:wedged-rebuild", "session:peer-not-director",
    "invite:retry", "ble:page-send", "ble:rebase", "become:director", "boot", "resync:force-reconnect",
    "page:send", "session:connected", "refresh:peers-cleared", "ble:contention", "watchdog:half-open-reconnect",
  ];
  for (const e of SAMPLES) {
    assert.equal(nativeLevelFor(LOG_LEVELS, e), m.levelForEvent(e),
      `"${e}" classifies differently in the app than in the worker — the tables have drifted`);
  }
  // And the classification must be USEFUL: the per-second chatter has to be the level you turn off.
  assert.equal(m.levelForEvent("ble:page-send"), LOG_LEVELS.debug, "1 Hz BLE chatter is not debug-level");
  assert.equal(m.levelForEvent("mesh:error"), LOG_LEVELS.error, "errors are not error-level");
});

test("dbgLog drops below-level events BEFORE buffering them", () => {
  const src = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const fn = src.slice(src.indexOf("const dbgLog = useCallback"));
  const body = fn.slice(0, fn.indexOf("[dbgFlush"));
  assert.match(body, /if \(levelForEvent\(event\) > logLevelRef\.current\) return;/, "dbgLog ignores levels");
  assert.ok(body.indexOf("levelForEvent(event)") < body.indexOf("dbgBufferRef.current.push"),
    "events are buffered first and filtered later — the cheapest request is the one never assembled");
});

test("the relay keepalive is a keepalive, not a poll", () => {
  const src = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const hb = src.slice(src.indexOf("relayHeartbeatRef.current = setInterval"));
  const ms = Number(hb.match(/\}, (\d+)\);/)[1]);
  const perDay = Math.floor((24 * 60 * 60 * 1000) / ms);
  assert.ok(perDay < 3000,
    `a ${ms}ms relay keepalive spends ${perDay}/day saying nothing changed — page turns already publish`);
  // But it must stay comfortably inside the liveness window, or followers declare the director dead.
  const APPJS = fs.readFileSync("web/src/app.js", "utf8");
  const liveS = Number(APPJS.match(/const RELAY_LIVE_MAX_AGE_S = (\d+)/)[1]);
  assert.ok(ms / 1000 < liveS / 2,
    `a ${ms / 1000}s keepalive against a ${liveS}s liveness window leaves no margin for one lost publish`);
});

test("a never-saved sink resolves to the default, not the string \"null\"", () => {
  // AsyncStorage.multiGet resolves a MISSING key to null, never undefined. A check for undefined
  // is therefore never true on a fresh device, so the "never saved" branch never runs and
  // String(null) executes instead — producing the literal text "null" in the sink field. Confirmed
  // on device 2026-08-18.
  const NATIVE_SRC = fs.readFileSync("PdfReaderApp.tsx", "utf8");
  const line = NATIVE_SRC.slice(NATIVE_SRC.indexOf("logSinkRef.current = (saved"));
  const stmt = line.slice(0, line.indexOf(";") + 1);
  assert.match(stmt, /saved === null/, "the null case from AsyncStorage is not handled");
  assert.doesNotMatch(stmt, /String\(saved\)/,
    "String(saved) is back — on the null branch that produces the literal text \"null\"");
});

/**
 * Index of the closing bracket that balances the one at openIdx, or -1 if they never balance. Lets
 * an assertion lift a whole `if (...)` condition, or a whole `{ ... }` consequent, out of the source
 * by STRUCTURE — surviving reformatting and an added clause — instead of quoting today's text or
 * slicing a fixed number of characters.
 */
const matchBracket = (src, openIdx, open, close) => {
  let depth = 0;
  for (let i = openIdx; i < src.length; i += 1) {
    if (src[i] === open) depth += 1;
    else if (src[i] === close) {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
};
const matchParen = (src, openIdx) => matchBracket(src, openIdx, "(", ")");
const matchBrace = (src, openIdx) => matchBracket(src, openIdx, "{", "}");

/**
 * The same source with every comment and the inside of every string literal blanked out, offsets
 * preserved. Structural searches run against this, so a brace inside a string or the word `else`
 * inside a comment can neither hide a branch nor invent one, while a slice taken at these offsets
 * still reads the real text out of the original.
 */
const maskOut = (src) => {
  const out = src.split("");
  const blank = (from, to) => { for (let k = from; k < to && k < out.length; k += 1) out[k] = out[k] === "\n" ? "\n" : " "; };
  for (let j = 0; j < src.length; ) {
    const c = src[j];
    if (c === '"' || c === "'" || c === "`") {
      let e = j + 1;
      while (e < src.length && src[e] !== c) e += src[e] === "\\" ? 2 : 1;
      blank(j + 1, e);
      j = e + 1;
      continue;
    }
    if (c === "/" && src[j + 1] === "/") { const nl = src.indexOf("\n", j); const e = nl < 0 ? src.length : nl; blank(j, e); j = e; continue; }
    if (c === "/" && src[j + 1] === "*") { const e = src.indexOf("*/", j); if (e < 0) throw new Error("unterminated comment"); blank(j, e + 2); j = e + 2; continue; }
    j += 1;
  }
  return out.join("");
};

/**
 * becomeDirector's body, bounded by DECLARATIONS on both ends — the next `const … = useCallback`
 * after it. Both endpoints are asserted, because a slice whose end marker has been renamed returns
 * -1 and quietly becomes the whole rest of the file, at which point every `indexOf` ordering check
 * in here starts measuring some other function.
 */
const becomeDirectorBody = () => {
  const start = NATIVE.indexOf("const becomeDirector = useCallback");
  assert.ok(start > 0, "becomeDirector is gone");
  const end = NATIVE.indexOf("const performSoftReset = useCallback", start);
  assert.ok(end > start, "becomeDirector is no longer followed by performSoftReset — re-anchor this slice");
  return NATIVE.slice(start, end);
};

test("the director's first broadcast uses the WEB's true page, not a lagging native mirror", () => {
  // THE RACE THIS CLOSES, confirmed on hardware 2026-08-18 across two separate test runs, each
  // showing a DIFFERENT wrong page (song 2, then page 1) before self-correcting to the director's
  // real page ~10s later. The web renders a jump INSTANTLY — no native round trip needed to draw a
  // page — but tells native asynchronously. becomeDirector used to broadcast currentPageRef.current,
  // a native-side mirror fed by that async message, with zero verification. Tap Ser Director right
  // after a jump and the mirror is still holding whatever was true a MOMENT AGO: the WebView's boot
  // default (2) on a fresh install, or the page before the jump (1, the cover) on a warm one — both
  // observed, which is exactly what a race against an evolving mirror produces, not a fixed bug.
  const WEB = fs.readFileSync("web/src/app.js", "utf8");
  const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");

  // 1. The web sends its OWN true page the instant the tap happens — before any native round trip.
  assert.match(WEB, /postNativeBridge\(\{ type: "request-director", currentPage: state\.currentPage \}\)/,
    "the request no longer carries the web's true page");

  // 2. Native extracts it and threads it through onDirectorCode rather than discarding it.
  const caseBlock = NATIVE.slice(NATIVE.indexOf('case "request-director"'), NATIVE.indexOf('case "request-director"') + 400);
  assert.match(caseBlock, /msg\.currentPage/, "the native handler no longer reads the page off the message");
  assert.match(caseBlock, /onDirectorCode\(DIRECTOR_CODE, knownPage\)/,
    "onDirectorCode is called without the page — it reverts to trusting the native mirror alone");

  // 3. The confirm dialog's callback closes over it rather than dropping it. The dialog is a native
  //    modal Alert, so nothing on the WebView can navigate again while it is open — the page
  //    captured at the ORIGINAL tap is still correct by the time the human taps "Sí, dirigir".
  // (The call may fall back to the restored crash-resume page when the web sent no page, so this
  //  matches the argument rather than the whole expression — pinning `becomeDirector(code,
  //  knownCurrentPage)` exactly went stale the moment that `?? restoredDirectorPageRef` fallback
  //  landed, and a red test that guards a property the code still HAS teaches people to ignore it.)
  assert.match(NATIVE, /onPress: \(\) => becomeDirector\(code, knownCurrentPage\b/,
    "the confirm dialog no longer passes the known page through to becomeDirector");

  // 4. becomeDirector corrects the mirror BEFORE anything downstream can read it — every broadcast
  //    path (no-mesh transmitter, mesh, both heartbeats) reads currentPageRef.current rather than
  //    asking the web again, so the fix has to land here, once, ahead of the first read.
  //    (The old bound here was `const becomeFollower`, which is declared ABOVE becomeDirector and so
  //     never appears again — indexOf returned -1 and the "body" was the whole rest of the file
  //     minus one character. Bounded on the next declaration now, with both ends asserted.)
  const body = becomeDirectorBody();
  const fixIdx = body.indexOf("currentPageRef.current = knownCurrentPage");
  assert.ok(fixIdx > 0, "becomeDirector no longer corrects the mirror from the known page");
  const firstBroadcastIdx = body.indexOf("broadcastPage(currentPageRef.current");
  assert.ok(firstBroadcastIdx > fixIdx,
    "a broadcast reads currentPageRef.current BEFORE the known-page correction runs — the race is still open");
});

test("a stale mirror is only ever a fallback, never silently trusted over a fresh value", () => {
  // With a known-fresh page supplied, the stale mirror must never win. Without one (an older web
  // bundle, or no bridge payload), the mirror is still the correct fallback — this must not regress
  // into REQUIRING a fresh value to broadcast at all, which would break becomeDirector for any
  // caller that cannot supply one.
  //
  // WHAT THE OLD ASSERTION MISSED. It called `applyKnownPage`, a four-line model of the guard
  // written in this file, and imported nothing. The truth table it checked was the truth table of
  // the TEST, so inverting the real guard in becomeDirector to `knownCurrentPage < 0` — which ships
  // the lagging mirror on the director's very first broadcast, the whole outage — left all four
  // assertions green. The guard is now LIFTED OUT OF PdfReaderApp.tsx and executed: the same table,
  // run against the statement the app actually ships.
  //
  // AND WHAT THE REWRITE ITSELF MISSED, twice. First, its "no else branch" check scanned
  // body.slice(closeIdx, assignIdx) — the text between the condition's `)` and the assignment inside
  // the consequent, which in `if (…) { assign; } else { … }` is always just " { ". The else it
  // claimed to catch lies AFTER the assignment, so the assertion was structurally incapable of
  // firing on the one regression it named. The whole if-statement is bounded by brace matching now,
  // and the check is on what FOLLOWS the consequent's closing brace. Second, the rewrite dropped the
  // old model's `applyKnownPage(2, undefined) === 2` case, the only thing that asserted the fallback
  // path is a NO-OP; testing the condition alone cannot see a branch that rewrites the mirror. So
  // the table below asserts the value the mirror is left holding, not merely which inputs pass.
  const body = becomeDirectorBody();
  const bare = maskOut(body);
  const assignIdx = bare.indexOf("currentPageRef.current = knownCurrentPage");
  assert.ok(assignIdx > 0, "becomeDirector no longer corrects the mirror from the known page at all");
  const ifIdx = bare.lastIndexOf("if (", assignIdx);
  assert.ok(ifIdx >= 0, "the mirror correction is not guarded — a bogus page would be broadcast as-is");
  const openIdx = bare.indexOf("(", ifIdx);
  const closeIdx = matchParen(bare, openIdx);
  assert.ok(closeIdx > openIdx, "the freshness guard's condition never closes its parentheses");
  assert.ok(closeIdx < assignIdx, "the correction sits outside the guard that is supposed to gate it");
  const cond = body.slice(openIdx + 1, closeIdx);
  assert.match(cond, /knownCurrentPage/,
    `the freshness guard no longer tests the page it was handed: ${cond}`);

  // The consequent, bounded by STRUCTURE at both ends: from the first thing after the condition to
  // the brace (or the semicolon, if someone drops the braces) that closes it. Both endpoints are
  // asserted, so a slice that failed to find its end can never silently become the rest of the
  // function.
  const bodyStart = closeIdx + 1 + bare.slice(closeIdx + 1).search(/\S/);
  assert.ok(bodyStart > closeIdx, "the freshness guard has no consequent at all");
  let consequentEnd;
  if (bare[bodyStart] === "{") {
    const closeBrace = matchBrace(bare, bodyStart);
    assert.ok(closeBrace > bodyStart, "the freshness guard's consequent block never closes");
    consequentEnd = closeBrace + 1;
  } else {
    const semi = bare.indexOf(";", bodyStart);
    assert.ok(semi > bodyStart, "the freshness guard's brace-less consequent never ends");
    consequentEnd = semi + 1;
  }
  assert.ok(consequentEnd > assignIdx, "the mirror correction is not inside the guard's consequent");
  assert.doesNotMatch(bare.slice(consequentEnd), /^\s*else\b/,
    "an else branch now runs when no fresh page was supplied — the mirror fallback must be a no-op, " +
    "because that is the branch every caller that cannot supply a fresh page still takes");

  // AND WHAT ROUND 3 FOUND, third time round. Executing a slice of TSX is only sound if the
  // environment tolerates everything the real function may call. It did not: the statement ran with
  // exactly two names in scope, so adding one ordinary line inside the consequent —
  // `breadcrumb("took web page");`, a dbgLog, a setState — threw ReferenceError, and the catch below
  // turned that into "the mirror correction no longer runs", a confident and wrong accusation
  // against an edit that cannot change the property at all. Two changes fix it, in opposite
  // directions:
  //
  // FIRST, the CONDITION must be scoreable from the page alone, or this file refuses. A truth table
  // is only worth reading if the thing under test is the guard and not a stub: if the condition
  // starts consulting a name this file cannot supply (`isFreshPage(...)`, a ref, a feature flag),
  // a no-op stand-in would make it evaluate falsy and the table would "catch" a regression that
  // isn't there. Refuse by name instead. What would actually answer it is rendering becomeDirector
  // in a real React runtime with the real surrounding state, which no source-reading test can do.
  const condIdents = [
    ...new Set(
      maskOut(cond).replace(/\.\s*[A-Za-z_$][\w$]*/g, " ").match(/[A-Za-z_$][\w$]*/g) || [],
    ),
  ].filter((id) => !["typeof", "true", "false", "null", "undefined", "NaN", "Infinity",
    "in", "instanceof", "void"].includes(id));
  const SCOREABLE = ["knownCurrentPage", "Number", "Math", "String", "Boolean",
    "isFinite", "isNaN", "parseInt", "parseFloat"];
  const unscoreable = condIdents.filter((id) => !SCOREABLE.includes(id));
  assert.deepEqual(unscoreable, [],
    `the freshness guard now consults ${unscoreable.join(", ")}, which this file cannot supply: ` +
    `\`${cond.trim()}\`. Refusing to score a truth table against a stubbed condition — what would ` +
    "settle it is rendering becomeDirector in a real React runtime with the real surrounding state.");

  // SECOND, the CONSEQUENT may contain anything, so the environment tolerates anything. Unknown
  // names resolve through a proxy: real globals stay real (`Number.isFinite` must work), the two
  // modelled bindings stay modelled, and everything else becomes a no-op function — so logging,
  // breadcrumbs and setState inside the branch are harmless, exactly as they are to the property.
  // A no-op that was supposed to compute the page still shows up: the mirror ends holding undefined
  // and the table below says so.
  const stmt = body.slice(ifIdx, consequentEnd);
  const noop = () => {};
  const stubbed = new Set();
  const afterGuard = (knownCurrentPage) => {
    const currentPageRef = { current: 2 };
    const modelled = { currentPageRef, knownCurrentPage };
    const env = new Proxy(modelled, {
      has: (_t, k) => k !== Symbol.unscopables,
      get: (t, k) => {
        if (k === Symbol.unscopables) return undefined;
        if (k in t) return t[k];
        if (typeof k === "string" && k in globalThis) return globalThis[k];
        stubbed.add(String(k));
        return noop;
      },
      set: (t, k, v) => { t[k] = v; return true; },
    });
    try {
      new Function("__env", `with (__env) { ${stmt} }`)(env);
    } catch (e) {
      assert.fail(`the lifted freshness guard will not run at all (${e.message}): ${stmt}`);
    }
    return currentPageRef.current;
  };

  // Every name the branch used that this file stubbed out. Named in each failure below, because a
  // wrong number here is far likelier to be a stub that should have computed something than a
  // genuine inversion of the guard, and the reader should not have to guess which.
  const stubs = () => (stubbed.size ? ` [names stubbed to no-ops while running it: ${[...stubbed].join(", ")}]` : "");

  assert.equal(afterGuard(372), 372, "a fresh known page did not override the stale mirror — the lagging page ships" + stubs());
  assert.equal(afterGuard(1), 1, "page 1 is a real page (the cover) and must be able to override the mirror" + stubs());
  assert.equal(afterGuard(undefined), 2,
    "no known page, and the mirror did not survive — the fallback must be a NO-OP, or becomeDirector " +
    "breaks for every caller (an older web bundle, any path with no bridge payload) that has none to give" + stubs());
  assert.equal(afterGuard(0), 2, "a page of 0 was trusted — that is not a real page" + stubs());
  assert.equal(afterGuard(-5), 2, "a negative page was trusted" + stubs());
  assert.equal(afterGuard(NaN), 2, "NaN was trusted as a page" + stubs());
  assert.equal(afterGuard("372"), 2, "a string off the bridge payload was trusted without a type check" + stubs());
});
