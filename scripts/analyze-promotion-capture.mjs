#!/usr/bin/env node
/**
 * analyze-promotion-capture — verdict a multi-device hardware capture of a promote/demote/
 * re-promote test against the fix signatures of builds 469-474.
 *
 * WHY THIS EXISTS. The 2026-08-19 marathon (builds 469-472) established that this bug family is
 * only ever settled by SIMULTANEOUS multi-device captures — and then every capture got analyzed by
 * hand, line by line, in chat. The 4-way capture that exposed the phantom-beacon bug (nonce=4ca1
 * page=7, root-caused in PR #380) took a full manual pass to notice that NO device in the room was
 * broadcasting the page every follower rendered. That is a mechanical check. This tool makes it one
 * command.
 *
 * INPUT. Two shapes, because there are two ways to get a capture off a device and neither is
 * optional. Xcode Console TEXT is what you get live in the room. JSONL (scripts/logarchive-to-jsonl,
 * fed by `sudo log collect --device-udid`) is what you get AFTERWARDS from a device's own persisted
 * unified log — the only option for a device never attached to Console, which at Mass is every
 * device. Multiple files are pooled; device identity comes from the message itself, so a single
 * concatenated paste works. Stdin works too. The two shapes may not be mixed in one run: their
 * timestamps are on different axes (seconds-of-day vs epoch) and pooling them is refused, not
 * silently misread. The Console GUI renders the same line 2-4x with microsecond-jittered stamps;
 * identical (device, message) pairs within 100 ms collapse to one event. Genuine retries here are
 * 300-700 ms apart, so dedup cannot eat a real retry.
 *
 * ── THE GHOST CHECK, and why it models STATE rather than events ──────────────────────────────
 *
 * The first version of this tool asked "was there a `ble:page-send` for page P near the time page P
 * was seen?". That is wrong in BOTH directions, and an adversarial audit caught it flipping the
 * real ghost capture to PASS by adding one legitimate line:
 *
 *   - BlePageBeacon.swift:156 `guard page != lastPublishedPage else { return }` — a page-send is
 *     emitted EXACTLY ONCE per page turn. The advertisement it starts is continuous STATE that
 *     outlives it by minutes. A director sitting on one page for ten minutes logs one send, ten
 *     minutes ago, so any capture window that misses it reported the HEALTHIEST possible fleet as
 *     a phantom beacon. FALSE FAIL, burning a hardware night.
 *   - A PR #380 stale advertisement is BY CONSTRUCTION the leftover of a page its ex-director
 *     genuinely published while alive — and therefore genuinely logged a page-send for. Pool that
 *     device's log (which pull-device-trace.sh explicitly tells you to do) and the very bug this
 *     tool exists to catch explains itself away. FALSE PASS on the headline check.
 *
 * So the question is not "did someone send this page once?" but "is anyone ACTUALLY BROADCASTING
 * this page right now?". Each device's publish state is replayed from its own log:
 *
 *     ble:page-send page=P   -> publishing P        (BlePageBeacon.swift:170)
 *     ble:on-air             -> publishing          (:217)
 *     ble:stop-publishing    -> NOT publishing      (:242, and :238 resets lastPublishedPage)
 *     ble:boot-stop-stale    -> NOT publishing      (:347, build 472's active cancel)
 *     role is not "director" -> NOT publishing      (demoted, or relaunched as a follower)
 *
 * A rendered page is EXPLAINED when some device held "director + publishing + that page" within
 * ±2 s of it. The stable-director case now passes (the span is still open ten minutes later) and
 * the force-quit ex-director now fails (relaunching flips it to follower, closing the span) —
 * which is exactly the asymmetry the bug has and the old time-window could not express.
 *
 * WHAT THIS CANNOT DO, stated plainly because a verdict machine that overclaims is worse than none:
 * a receiver's log identifies an advertiser only by nonce, never by sender, and the sending half of
 * a ghost is by definition a process that is no longer logging. So attribution is inference, not
 * proof. If no device in the pool was ever a publishing director, nothing can be attributed at all
 * and the check reports INCONCLUSIVE rather than indicting every broadcast in the room.
 *
 * ── THE OTHER CHECKS ─────────────────────────────────────────────────────────────────────────
 *
 *   INVITE HAMMERING / EVICTION (470 + 471): per (follower, target), streaks of `invite:send` with
 *     no `session:connected` within 5 s. Build 471's bug was 90+ s of 300-700 ms rejected retries at
 *     a demoted ex-director; `director:evict-stale` must cap any streak at 2 failures. An eviction
 *     only excuses a streak it actually lands INSIDE — DirectorSyncModule.swift:2358 deletes the
 *     streak counter on eviction, so evict -> rediscover -> hammer again is the natural regression,
 *     and a stray evict line elsewhere in the capture must not whitewash it. A streak long enough
 *     that eviction demonstrably is not capping it FAILs regardless of how many evict lines exist.
 *     A demotion nobody re-targeted did not exercise the path: INCONCLUSIVE, never PASS.
 *
 *   BOOT-STOP-STALE (472): `ble:boot-stop-stale` on launch proves the stale-ad cancel is live on
 *     that device. Checked by MEMBERSHIP per device, never by counting — lines logged before
 *     localPeerID is set carry the literal device "?" and must not pad the tally.
 *
 *   OTA/RELOAD (474): the restage-loop bug (PR #381) lives in device breadcrumbs, not os_log. Its
 *     tokens are surfaced if present; their absence proves nothing and says so.
 *
 *   CLOCK AGREEMENT: every cross-device comparison assumes the devices agree on the time. Free in a
 *     live Console stream (one host renders every line), NOT free across separately-collected
 *     archives. An invite:send on A and its invite:recv on B are one causal pair, so the deltas
 *     measure agreement directly. Each receive is consumed by at most one send, or an unanswered
 *     retry steals an older receive and invents tens of seconds of skew that is not there.
 *     One-directional by nature: only followers send invites (DirectorSyncModule.swift:2032), so
 *     these samples can only catch a follower clock running slow. Reported honestly as such.
 *
 * SILENCE IS NOT SUCCESS (house rule, same as analyze-join-latency): a scenario the capture did not
 * exercise is INCONCLUSIVE, never a pass. At Mass the followers have no network; a silent fleet once
 * read as a flawless one.
 *
 * Also reported: role transitions and convergence — for each promotion, how long every other device
 * took to render the new director's page. `iPad-prime` is the permission-priming pseudo-peer
 * (DirectorSyncModule.primePermissions) and is ignored throughout.
 *
 * Exit code: 1 if any check FAILs, 2 on unusable input, else 0 (INCONCLUSIVE does not fail — it
 * names what to capture next time instead).
 */
import fs from "node:fs";

const ROLE = /^(follower|director|off) (\S+) (.+)$/;
const TS = /(\d{2}):(\d{2}):(\d{2})\.(\d{1,6})/;
const DEDUP_MS = 100;
const ANSWER_S = 5;
const PUBLISH_TOL_S = 2;
const SKEW_WINDOW_S = 5;
const EVICT_GRACE_S = 5;
// The fix evicts after 2 consecutive failures, so a healthy streak cannot exceed ~3 before the
// eviction lands. Past this, eviction is demonstrably not capping it whatever the log claims.
const STREAK_UNCAPPED = 5;
// How long a device's own log may go quiet before its broadcast stops being assumed to continue.
// A serving director logs refresh:hold-serving every few seconds, so a minute of nothing is a
// strong hint the process died — but only a hint, which is why it downgrades rather than indicts.
const SILENCE_GRACE_S = 60;
const PRIME = "iPad-prime";

const inputs = process.argv.slice(2);
const raw = inputs.length
  ? inputs.map((f) => fs.readFileSync(f, "utf8")).join("\n")
  : fs.readFileSync(0, "utf8");

// ── Parse ─────────────────────────────────────────────────────────────────────
const events = [];
for (const line of raw.split(/\r?\n/)) {
  const s = line.trim();
  if (!s) continue;

  // JSONL row: {t: epoch ms, dev, role, event, ...kv} — logarchive-to-jsonl's emitted shape.
  if (s.startsWith("{")) {
    let row;
    try { row = JSON.parse(s); } catch { continue; }
    if (!Number.isFinite(row?.t) || !row.dev || !row.role || !row.event) continue;
    const rest = Object.entries(row)
      .filter(([k]) => !["t", "dev", "role", "event", "src", "build"].includes(k))
      .map(([k, v]) => `${k}=${v}`);
    const msg = [row.event, ...rest].join(" ");
    events.push({ t: row.t / 1000, role: row.role, device: row.dev, msg, full: `${row.role} ${row.dev} ${msg}` });
    continue;
  }

  const ts = TS.exec(line);
  if (!ts) continue;
  const t = +ts[1] * 3600 + +ts[2] * 60 + +ts[3] + +ts[4].padEnd(6, "0") / 1e6;
  let msg = null;
  const proc = line.indexOf("SignoVivo");
  if (proc >= 0) msg = line.slice(proc + "SignoVivo".length).replace(/^[\s\t]+/, "");
  if (!msg || !ROLE.test(msg)) {
    const m = /(?:^|\s)((?:follower|director|off) \S+ .+)$/.exec(line.slice(ts.index));
    msg = m ? m[1] : null;
  }
  if (!msg) continue;
  const rm = ROLE.exec(msg);
  if (!rm) continue;
  events.push({ t, role: rm[1], device: rm[2], msg: rm[3], full: msg });
}
events.sort((a, b) => a.t - b.t);

// Mixing shapes would put seconds-of-day and epoch-seconds events ~57 years apart on one axis.
const shapes = new Set(events.map((e) => (e.t > 86400 * 2 ? "jsonl" : "console")));
if (shapes.size > 1) {
  console.error("✖ Refusing to mix Console-text and JSONL inputs in one run: their timestamps are on different axes (seconds-of-day vs epoch) and every cross-device comparison would be nonsense. Analyze each shape separately.");
  process.exit(2);
}

const lastSeen = new Map();
const evs = events.filter((e) => {
  const k = `${e.device} ${e.full}`;
  const prev = lastSeen.get(k);
  lastSeen.set(k, e.t);
  return !(prev !== undefined && (e.t - prev) * 1000 < DEDUP_MS);
});

if (!evs.length) {
  console.error("No parseable SignoVivo log lines found in the input (expected Xcode Console text or logarchive-to-jsonl JSONL).");
  process.exit(2);
}
if (evs[evs.length - 1].t - evs[0].t > 12 * 3600) {
  console.error("WARNING: capture spans >12h of clock — if it crossed midnight, ordering and all timing analysis below are wrong (timestamps carry no date).");
}

const kv = (msg, key) => (new RegExp(`\\b${key}=(\\S+)`).exec(msg) || [])[1];
const pad = (n, w = 2) => String(n).padStart(w, "0");
const clock = (t) => {
  // JSONL carries epoch seconds; Console text carries seconds-of-day. Render both as local
  // wall-clock (the host's zone — Central here), because every timestamp Miguel reads must be CT.
  if (t > 86400 * 2) {
    const d = new Date(t * 1000);
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
  }
  return `${pad(Math.floor(t / 3600))}:${pad(Math.floor((t % 3600) / 60))}:${(t % 60).toFixed(3).padStart(6, "0")}`;
};

// ── Extract facts ─────────────────────────────────────────────────────────────
const devices = new Set(evs.filter((e) => e.device !== "?").map((e) => e.device));
const endT = evs[evs.length - 1].t;

// PUBLISH SPANS — the intervals during which a device was actually broadcasting a page. See the
// header: this is the whole basis of the ghost check, and the reason it models state not events.
// A span ends for one of two very different reasons, and conflating them is what let the audit's
// exploit through. STOPPED means the device SAID it stopped (stop-publishing, boot-stop-stale, or a
// role change) — an advertisement outliving that is precisely PR #380. SILENT means the device
// simply stopped logging, which is genuinely ambiguous: a force-quit director looks identical to a
// director whose log was merely not captured any further. The first indicts; the second can only
// raise a question, and says so.
const spans = [];
const byDevice = new Map();
for (const e of evs) {
  if (e.device === "?") continue;
  if (!byDevice.has(e.device)) byDevice.set(e.device, []);
  byDevice.get(e.device).push(e);
}
for (const [dev, list] of byDevice) {
  let role = null, publishing = false, page = null, since = null, prevT = null;
  const close = (t, closedBy) => {
    // `>=`, NOT `>`. A publish span that opens and closes at the same instant is still a real
    // instant of broadcasting, and dropping it made the analyzer accuse a HEALTHY capture of a ghost
    // page. It happens whenever the director's ble:page-send is the newest row in the pool — which
    // is the normal shape of a short capture that ends on a page turn: `since`, `prevT` and `endT`
    // are all that same timestamp, so the final close is a zero-length span and no span is recorded
    // at all. explainedBy() then finds nobody broadcasting the page the follower just correctly
    // applied and exits 1.
    //
    // Reproduced on this repo's own sv-log-2026-08-18.jsonl — a director ble:page-send of page 372
    // and a follower ble:page-apply of page 372 in the SAME millisecond, the healthiest possible BLE
    // exchange — which FAILED as "nobody in the pool ever broadcast 372". The next hardware capture
    // would have read as PR #380 regressing.
    //
    // Safe: explainedBy() already widens by ±PUBLISH_TOL_S on both sides, so a point span still
    // covers renders around it, and the real ghost fixture (console-capture-ghost7) still fails
    // byte-identically.
    if (publishing && page != null && role === "director" && since != null && t >= since) {
      spans.push({ device: dev, page, from: since, to: t, closedBy });
    }
  };
  for (const e of list) {
    // A director with any peer at all logs refresh:hold-serving every few seconds
    // (DirectorSyncModule.swift:1446), so a long hole in a device's own log means the process is
    // most likely gone. Its broadcast must not be assumed to continue across that hole.
    if (prevT != null && e.t - prevT > SILENCE_GRACE_S) {
      close(prevT + SILENCE_GRACE_S, "silent");
      // END THE PUBLISHING STATE, not just the span. This advanced `since` while leaving
      // publishing/role/page loaded, so the NEXT state change closed a second span from the instant
      // the device's log resumed — zero-length, and therefore silently dropped while close() still
      // required `t > since`. Once close() started recording point spans (which it must, or a
      // director whose page-send is the newest row in a short capture leaves no span at all and its
      // followers are accused of rendering a ghost), that phantom became a real span — and
      // explainedBy widens every span by ±PUBLISH_TOL_S, so it granted a 4-second alibi anchored at
      // exactly the moment a force-quit ex-director's log comes back as a FOLLOWER. That is when a
      // stale advertisement is most expected to be read, so the alibi covered the very burst the
      // ghost check exists to catch: a false GREEN on the tool's headline finding.
      //
      // Nothing was broadcasting across a silence that long by assumption — that is what the grace
      // means — so the state must be cleared, not carried.
      publishing = false;
      page = null;
      since = e.t;
    }
    let pub = publishing, pg = page;
    if (e.msg.startsWith("ble:page-send")) { pub = true; pg = kv(e.msg, "page") ?? pg; }
    else if (e.msg.startsWith("ble:on-air")) pub = true;
    else if (e.msg.startsWith("ble:stop-publishing") || e.msg.startsWith("ble:boot-stop-stale")) pub = false;
    // A device that is not currently a director is not a legitimate publisher, whatever it last
    // sent. This is what closes the span of a force-quit ex-director that relaunches as a follower.
    if (e.role !== "director") pub = false;
    if (since == null) since = e.t;

    if (pub !== publishing || pg !== page || e.role !== role) {
      close(e.t, "stopped");
      role = e.role; publishing = pub; page = pg; since = e.t;
    }
    prevT = e.t;
  }
  // Still publishing when its log ends: alive to the end of the capture, or silently gone.
  const tail = prevT + SILENCE_GRACE_S;
  close(Math.min(endT, tail), tail < endT ? "silent" : "capture-end");
}
spans.sort((a, b) => a.from - b.from);

const everDirected = evs.some((e) => e.role === "director" && e.device !== "?");
const explainedBy = (page, t) =>
  spans.find((s) => s.page === page && s.to >= t - PUBLISH_TOL_S && s.from <= t + PUBLISH_TOL_S);

// What a follower actually RENDERED — the user-visible symptom, and so the primary evidence. The
// advertiser lines are supporting: BlePageBeacon.swift:400 logs one only when the NONCE changes, so
// a page rendered from an already-known advertiser has no advertiser line at all.
const applies = evs.filter((e) => e.msg.startsWith("ble:page-apply"))
  .map((e) => ({ t: e.t, device: e.device, page: kv(e.msg, "page") }))
  .filter((a) => a.page != null);
const advertisers = new Map(); // "nonce|page" -> {nonce, page, first, receivers:Set}
for (const e of evs) {
  if (!e.msg.startsWith("ble:new-advertiser")) continue;
  const nonce = kv(e.msg, "nonce"), page = kv(e.msg, "page");
  if (page == null) continue;
  const k = `${nonce}|${page}`;
  const a = advertisers.get(k) ?? { nonce, page, first: e.t, receivers: new Set() };
  a.first = Math.min(a.first, e.t);
  a.receivers.add(e.device);
  advertisers.set(k, a);
}

const evicts = evs.filter((e) => e.msg.startsWith("director:evict-stale"));
const bootStops = new Set(evs.filter((e) => e.msg.startsWith("ble:boot-stop-stale") && e.device !== "?").map((e) => e.device));

// Role transitions: a device's own lines flipping follower<->director.
const transitions = [];
const roleNow = new Map();
for (const e of evs) {
  if (e.device === "?" || e.role === "off") continue;
  const prev = roleNow.get(e.device);
  if (prev && prev !== e.role) {
    transitions.push({ t: e.t, device: e.device, kind: e.role === "director" ? "PROMOTION" : "DEMOTION" });
  }
  roleNow.set(e.device, e.role);
}
const demotions = transitions.filter((t) => t.kind === "DEMOTION");

// CLOCK AGREEMENT. Each receive is consumed by at most one send: an unanswered retry would
// otherwise steal an older, unrelated receive and invent tens of seconds of skew out of nothing.
const recvPool = evs
  .filter((e) => e.msg.startsWith("invite:recv"))
  .map((e, i) => ({ i, t: e.t, device: e.device, from: kv(e.msg, "from") }));
const consumed = new Set();
const skews = [];
for (const s of evs) {
  if (!s.msg.startsWith("invite:send")) continue;
  const to = kv(s.msg, "to");
  if (!to || to === PRIME) continue;
  const cand = recvPool
    .filter((r) => !consumed.has(r.i) && r.device === to && r.from === s.device && Math.abs(r.t - s.t) < SKEW_WINDOW_S)
    .sort((a, b) => Math.abs(a.t - s.t) - Math.abs(b.t - s.t))[0];
  if (!cand) continue;
  consumed.add(cand.i);
  skews.push({ pair: `${s.device}->${to}`, delta: cand.t - s.t });
}
const skewMin = skews.reduce((w, x) => (x.delta < w.delta ? x : w), { pair: "", delta: Infinity });
const skewMax = skews.reduce((w, x) => (x.delta > w.delta ? x : w), { pair: "", delta: -Infinity });

// INVITE STREAKS. A streak is consecutive unanswered sends; an answered send closes it but must
// never erase it — 471's whole bug was one long unanswered streak, and a later successful reconnect
// masking it would report the failure as healthy.
const connects = evs.filter((e) => e.msg.startsWith("session:connected"))
  .map((e) => ({ t: e.t, device: e.device, peer: kv(e.msg, "peer") }));
const runs = new Map(); // "dev->target" -> {worst, first, last, open, openFrom, everAnswered, worstFrom, worstTo}
for (const e of evs) {
  if (!e.msg.startsWith("invite:send")) continue;
  const target = kv(e.msg, "to");
  if (!target || target === PRIME) continue;
  const key = `${e.device}->${target}`;
  const answered = connects.some((c) => c.device === e.device && c.peer === target && c.t >= e.t && c.t - e.t <= ANSWER_S);
  const r = runs.get(key) ?? { worst: 0, first: e.t, last: e.t, open: 0, openFrom: e.t, everAnswered: false, worstFrom: e.t, worstTo: e.t };
  if (answered) { r.open = 0; r.everAnswered = true; }
  else {
    if (r.open === 0) r.openFrom = e.t;
    r.open++;
    if (r.open > r.worst) { r.worst = r.open; r.worstFrom = r.openFrom; r.worstTo = e.t; }
    else if (r.open === r.worst) r.worstTo = e.t;
  }
  r.last = e.t;
  runs.set(key, r);
}

// ── Report ────────────────────────────────────────────────────────────────────
const verdicts = [];
const say = (s) => console.log(s);
say(`Devices seen: ${[...devices].join(", ") || "(none)"}`);
say(`Events: ${evs.length} after dedup (${events.length - evs.length} duplicate line(s) collapsed)`);
if (skews.length) {
  const bad = skewMin.delta < -0.05;
  say(`Clock agreement: ${skews.length} invite pair(s), delta ${skewMin.delta.toFixed(3)}s..${skewMax.delta.toFixed(3)}s` +
    (bad ? `  ← SKEWED: a receive precedes its own send (${skewMin.pair}); cross-device timings below are unreliable`
         : "  (causal order intact; note only follower→director pairs exist, so a fast director clock is not observable)"));
  if (bad) verdicts.push({ v: "FAIL", what: `device clocks disagree by ~${Math.abs(skewMin.delta).toFixed(1)}s — re-collect the archives together, or trust only per-device findings` });
} else if (devices.size > 1) {
  say("Clock agreement: UNVERIFIED — no invite send/recv pair spans two devices in this capture");
}
say("");
say("Role transitions:");
if (!transitions.length) say("  (none — no promotion or demotion in this capture)");
for (const tr of transitions) say(`  ${clock(tr.t)}  ${tr.device}  ${tr.kind}`);

say("\n── WHO WAS BROADCASTING WHAT (publish spans) ──");
if (!spans.length) say("  (no device held director+publishing at any point in this capture)");
for (const s of spans) say(`  ${s.device} page=${s.page}  ${clock(s.from)} → ${clock(s.to)}`);

// GHOST PAGE
say("\n── GHOST PAGE (469 HMAC / 472 stale-ad cancel) ──");
const badApplies = applies.filter((a) => !explainedBy(a.page, a.t));
const badAds = [...advertisers.values()].filter((a) => !explainedBy(a.page, a.first));
for (const a of advertisers.values()) {
  const ok = explainedBy(a.page, a.first);
  say(`  advert nonce=${a.nonce} page=${a.page} first=${clock(a.first)} seen-by=${a.receivers.size}` +
    (ok ? `  (matches ${ok.device} broadcasting page ${ok.page})` : "  ← UNEXPLAINED: nobody was broadcasting that page"));
}
if (!advertisers.size) say("  (no ble:new-advertiser lines)");
const rendered = new Map();
for (const a of badApplies) rendered.set(`${a.device}|${a.page}`, a);
for (const a of rendered.values()) say(`  RENDERED page=${a.page} on ${a.device} at ${clock(a.t)} ← UNEXPLAINED: nobody was broadcasting that page`);
// An unexplained page splits by WHY no live broadcaster covers it. Only the first two indict.
const unexplainedPages = [...new Set([...badApplies.map((a) => a.page), ...badAds.map((a) => a.page)])];
const silentCulprits = new Map(); // page -> device that went quiet still broadcasting it
const indicted = [];
for (const page of unexplainedPages) {
  const forPage = spans.filter((s) => s.page === page);
  const silent = forPage.find((s) => s.closedBy === "silent");
  if (!forPage.length) indicted.push(`${page} (nobody in the pool ever broadcast it)`);
  else if (forPage.some((s) => s.closedBy === "stopped")) indicted.push(`${page} (its broadcaster explicitly stopped, yet the page kept arriving)`);
  else if (silent) silentCulprits.set(page, silent.device);
  else indicted.push(`${page} (broadcast only outside the window it was seen in)`);
}
if (!everDirected) {
  verdicts.push({ v: "INCONCLUSIVE", what: "no device in the pool was ever a publishing director — no broadcast can be attributed, so a ghost cannot be told from a legitimate page. Capture the director too." });
} else if (indicted.length) {
  verdicts.push({ v: "FAIL", what: `page(s) rendered with nobody broadcasting them: ${indicted.join("; ")} — the PR #380 stale-advertisement signature (assumes all ${devices.size} device(s) in the room are in this pool; a missing one could be the broadcaster)` });
} else if (silentCulprits.size) {
  verdicts.push({ v: "INCONCLUSIVE", what: `page(s) ${[...silentCulprits.keys()].join(", ")} arrived after their broadcaster went quiet without ever saying it stopped (${[...new Set(silentCulprits.values())].join(", ")}). Force-quit while directing looks exactly like a log that simply ends — confirm whether that device was killed, and re-collect it if not.` });
} else if (applies.length || advertisers.size) {
  verdicts.push({ v: "PASS", what: `every rendered page traces to a device that was actually broadcasting it (${applies.length} render(s), ${advertisers.size} advertisement(s))` });
} else {
  verdicts.push({ v: "INCONCLUSIVE", what: "no BLE page was rendered or advertised in this capture — the ghost path was not exercised" });
}

// INVITE HAMMERING / EVICTION
say("\n── INVITE HAMMERING / EVICTION (470 / 471) ──");
const hammered = [];
for (const [key, r] of runs) {
  const [dev, target] = key.split("->");
  // An eviction only excuses the streak it actually lands inside: the fix clears the streak counter
  // (DirectorSyncModule.swift:2358), so evict → rediscover → hammer again is the live regression.
  const inRun = evicts.find((e) => e.device === dev && e.msg.includes(target) &&
    e.t >= r.worstFrom && e.t <= r.worstTo + EVICT_GRACE_S);
  say(`  ${key}: worst unanswered streak=${r.worst}${r.everAnswered ? ", eventually answered" : ", never answered"}` +
    ` (${clock(r.first)}..${clock(r.last)})${inRun ? `  [evicted ${clock(inRun.t)}]` : ""}`);
  if (r.worst >= STREAK_UNCAPPED) hammered.push(`${key} (streak of ${r.worst} — eviction is not capping it)`);
  else if (r.worst >= 3 && !inRun) hammered.push(`${key} (streak of ${r.worst}, no eviction inside the streak)`);
}
for (const e of evicts) say(`  ${clock(e.t)}  ${e.device}  ${e.msg}`);
// The 471 path needs BOTH a demotion AND a follower actually re-targeting the ex-director.
const exercised = demotions.filter((d) => evs.some((e) =>
  e.msg.startsWith("invite:send") && kv(e.msg, "to") === d.device && e.t > d.t));
if (hammered.length) verdicts.push({ v: "FAIL", what: `invite hammering: ${hammered.join("; ")}` });
else if (exercised.length) verdicts.push({ v: "PASS", what: `${exercised.length} demoted ex-director(s) were re-targeted and the retargeting stopped within threshold${evicts.length ? ` (director:evict-stale fired ${evicts.length}x)` : ""}` });
else if (demotions.length) verdicts.push({ v: "INCONCLUSIVE", what: "a demotion occurred but no follower ever re-targeted the ex-director — 471's eviction path was not exercised (were the followers in the pool?)" });
else verdicts.push({ v: "INCONCLUSIVE", what: "no demotion in capture — 471's eviction path was not exercised" });

// BOOT-STOP-STALE — membership per device, never a count.
say("\n── BOOT-STOP-STALE (472) ──");
for (const d of devices) say(`  ${d}: ${bootStops.has(d) ? "ble:boot-stop-stale seen" : "not seen (pre-472 build, or launch not captured)"}`);
const missingBoot = [...devices].filter((d) => !bootStops.has(d));
verdicts.push(devices.size && !missingBoot.length
  ? { v: "PASS", what: `every device (${devices.size}) logged ble:boot-stop-stale on launch` }
  : { v: "INCONCLUSIVE", what: `ble:boot-stop-stale not seen on: ${missingBoot.join(", ") || "(no devices in capture)"}` });

// OTA / RELOAD
say("\n── OTA / RELOAD (474) ──");
const otaTokens = raw.match(/staged-ready|auto-apply|resolve:bundled|webview-terminated/g) ?? [];
say(otaTokens.length
  ? `  ${otaTokens.length} OTA/reload token(s) present — inspect manually: ${[...new Set(otaTokens)].join(", ")}`
  : "  no OTA/reload tokens in os_log (expected — the restage loop lives in device breadcrumbs; pull build badge → diagnostics to verify PR #381)");
verdicts.push({ v: otaTokens.length ? "FAIL" : "INCONCLUSIVE", what: otaTokens.length ? "OTA/reload activity visible during capture" : "restage loop not verifiable from os_log — check device breadcrumbs" });

// CONVERGENCE
say("\n── CONVERGENCE per promotion ──");
for (const tr of transitions.filter((x) => x.kind === "PROMOTION")) {
  const span = spans.find((s) => s.device === tr.device && s.to >= tr.t - 1);
  if (!span) { say(`  ${tr.device} promoted ${clock(tr.t)} — never broadcast a page`); continue; }
  say(`  ${tr.device} promoted ${clock(tr.t)}, page=${span.page}:`);
  for (const d of devices) {
    if (d === tr.device) continue;
    const hit = applies.find((a) => a.device === d && a.page === span.page && a.t >= span.from - PUBLISH_TOL_S);
    say(`    ${d}: ${hit ? `${(hit.t - span.from).toFixed(2)}s` : "NEVER rendered that page — worth shouting about"}`);
  }
}

say("\n══ VERDICTS ══");
for (const { v, what } of verdicts) say(`  ${v.padEnd(12)} ${what}`);
process.exit(verdicts.some((x) => x.v === "FAIL") ? 1 : 0);
