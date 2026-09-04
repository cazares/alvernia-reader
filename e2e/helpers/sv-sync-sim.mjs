// sv-sync-sim — a discrete-event simulation of the whole relay sync path.
//
// WHY THIS EXISTS. Almost every test in this repo asserts on SOURCE TEXT: it proves the code still
// contains a string, not that the system behaves. That style has failed here repeatedly and
// silently — 13 of 13 hand-written mutations once slipped past three whole test files, and the
// build-475 campaign produced three brand-new tests that could not fail at all. The unit tests that
// DO execute real code each cover one module in isolation, so the bugs that actually reached the
// congregation lived in the SEAMS: a seq the relay clamped one way and the follower judged another,
// a freshness window measured in seconds on one side and milliseconds on the other, a director
// restart that every module handled correctly on its own and that still stranded every follower.
//
// This wires the real modules together and runs a Mass.
//
// ── WHAT IS REAL AND WHAT IS SIMULATED ──────────────────────────────────────────────────────────
//
// REAL, imported and executed — no logic is re-stated here:
//   • src/directorRelaySync.js         the transmitter: seq generation, latest-wins coalescing,
//                                      the in-flight guard, the auth-error latch. Driven through a
//                                      stubbed global fetch(), so the actual module runs.
//   • sync-worker/src/publishSeq.js    decidePublish — the relay's accept/clamp/refuse rule.
//   • web/src/lib/svSyncDecision.js    decideRelaySnapshot + clockOffsetFromServerNow — the
//                                      follower's decision and its clock calibration.
//
// SIMULATED — transport and the mechanical execution of a decision, never a decision itself:
//   • the network (delay, loss, reordering, partition), the Durable Object's field assembly and
//     WebSocket fan-out, and the follower's application of a verdict it did not make.
//
// THE RULE THIS FILE LIVES BY: **the simulation never decides anything.** The moment a harness
// re-implements the logic it is testing, it passes forever — that is exactly how a backoff ladder
// re-stated in JS left a test green while the entire Swift retry loop was deleted. Every branch
// below either calls a real module or copies a MECHANICAL step (a field assignment, a clamp) from a
// named file:line, and `assertHarnessFidelity()` in svSyncSystem.test.mjs pins those copies against
// the real sources so this file cannot drift into fiction.
//
// Time is virtual. Nothing sleeps, a whole Mass runs in milliseconds, and every run is reproducible
// from its seed.

import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { decidePublish } from "../../sync-worker/src/publishSeq.js";

const require = createRequire(import.meta.url);
const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const svSyncDecision = require(path.join(REPO, "web/src/lib/svSyncDecision.js"));
const { decideRelaySnapshot, clockOffsetFromServerNow } = svSyncDecision;

/** Mirrors RELAY_LIVE_MAX_AGE_S. It is declared THREE times in production — sync-worker/src/index.ts,
 *  web/src/app.js, and as svSyncDecision's default — and the whole system is only coherent while all
 *  three agree. assertHarnessFidelity() proves they still do. */
export const RELAY_LIVE_MAX_AGE_S = 90;
export const PROTOCOL_VERSION = 1;

/** The relay's empty room. Mirrors EMPTY_SNAPSHOT, sync-worker/src/index.ts:101. */
const emptySnapshot = () => ({ v: PROTOCOL_VERSION, page: 0, totalPages: 0, mode: "", bookId: "", seq: 0, ts: 0 });

// ── deterministic randomness ────────────────────────────────────────────────────────────────────
// Seeded so a failure is reproducible from the seed printed in the assertion message. A flaky
// distributed-systems test that cannot be replayed is worse than no test.
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── virtual clock + event queue ─────────────────────────────────────────────────────────────────
class Scheduler {
  constructor(startMs) {
    this.now = startMs;
    this.queue = [];
    this.seq = 0;
  }
  /** Schedule work `deltaMs` from now. Ties break by insertion order so a run is deterministic. */
  at(deltaMs, fn, label = "") {
    this.queue.push({ t: this.now + Math.max(0, deltaMs), i: this.seq++, fn, label });
  }
  /** Run every event due at or before `untilMs`, advancing the clock to each in turn. */
  runUntil(untilMs) {
    for (;;) {
      this.queue.sort((a, b) => a.t - b.t || a.i - b.i);
      if (!this.queue.length || this.queue[0].t > untilMs) break;
      const e = this.queue.shift();
      this.now = e.t;
      e.fn();
    }
    this.now = Math.max(this.now, untilMs);
  }
}

// ── the relay room ──────────────────────────────────────────────────────────────────────────────
//
// A mechanical mirror of the SyncRoom Durable Object. The ONE decision it makes — whether a publish
// is applied and with what seq — is delegated to the real decidePublish(). Everything else here is
// field assembly copied from sync-worker/src/index.ts:185-198 and the fan-out from :335-344.
class Relay {
  constructor(sim) {
    this.sim = sim;
    this.snapshot = emptySnapshot();
    this.sockets = new Set();
    this.publishLog = [];   // every attempt, applied or not — the relay's own view of the Mass
  }

  /** Mirrors SyncRoom.publish(), sync-worker/src/index.ts:158. */
  publish(input, nowMs) {
    const decision = decidePublish({
      rawSeq: input.seq,
      nowMs,
      snapshotSeq: this.snapshot.seq,
      snapshotTs: this.snapshot.ts,
      maxAgeS: RELAY_LIVE_MAX_AGE_S,
    });
    if (!decision.apply) {
      // NOTE THE SHAPE: `ok: true` on a REFUSED publish. This is the response that let a fast device
      // clock freeze the whole web congregation while the director's pill stayed green — the
      // transmitter reads a resolved 200 as success and says nothing. Preserved exactly, because a
      // simulation that reported failure here would hide the bug it exists to catch.
      this.publishLog.push({ t: nowMs, page: input.page, seq: input.seq, applied: false, reason: decision.reason });
      return { status: 200, body: { ok: true, seq: decision.seq, ignored: true } };
    }
    const next = {
      v: PROTOCOL_VERSION,
      page: Math.max(1, Math.min(Number(input.page ?? this.snapshot.page) || 1, 100000)),
      totalPages: Math.max(0, Math.min(Number(input.totalPages ?? this.snapshot.totalPages) || 0, 100000)),
      mode: String(input.mode ?? this.snapshot.mode ?? "").slice(0, 64),
      bookId: String(input.bookId ?? this.snapshot.bookId ?? "").slice(0, 64),
      seq: decision.seq,
      ts: Math.floor(nowMs / 1000),
    };
    this.snapshot = next;
    this.publishLog.push({ t: nowMs, page: next.page, seq: next.seq, applied: true, reason: decision.reason });
    this.broadcast(next);
    return { status: 200, body: { ok: true, seq: next.seq } };
  }

  /** Mirrors the /state route, sync-worker/src/index.ts:630-646 — including the additive `now`
   *  (server epoch SECONDS) that is the ONLY channel a follower can calibrate its clock from. */
  getState(nowMs) {
    return { ...this.snapshot, now: Math.floor(nowMs / 1000) };
  }

  subscribe(socket) {
    this.sockets.add(socket);
    // "Land a fresh follower on the right page immediately" — index.ts:304. The snapshot is sent on
    // accept, before any publish. A follower joining an empty room therefore gets seq 0.
    socket.deliver(this.snapshot);
  }
  unsubscribe(socket) { this.sockets.delete(socket); }
  broadcast(snapshot) { for (const s of this.sockets) s.deliver(snapshot); }

  /** Mirrors webSocketMessage, sync-worker/src/index.ts:313 — a follower's "ping"/"resync" is
   *  answered with the snapshot AS IT IS AT REPLY TIME.
   *
   *  That timing is the whole point, not a detail. It means a director who died an hour ago keeps
   *  arriving on every healthy socket forever, with an unchanged seq, four times a minute. If a
   *  follower's seq de-dup ran before its freshness check, every one of those replies would be
   *  discarded as a duplicate and never reach the staleness test — which is exactly how the green
   *  "en vivo" pill stayed lit on a dead director at Wednesday practice. A simulation that captured
   *  the snapshot when the ping was ENQUEUED could not express that bug at all. */
  ping(socket) { socket.deliver(this.snapshot); }
}

// ── the network ─────────────────────────────────────────────────────────────────────────────────
//
// One place where delay, loss and reordering happen, so a test can turn each on independently.
// Reordering is not an extra feature bolted on: it falls out of per-message jitter, which is how it
// happens in the real world. A message is delivered at now + latency, and two messages sent 5 ms
// apart with 200 ms of jitter routinely arrive backwards.
class Network {
  constructor(sim, opts = {}) {
    this.sim = sim;
    this.latencyMs = opts.latencyMs ?? 20;
    this.jitterMs = opts.jitterMs ?? 0;
    this.lossRate = opts.lossRate ?? 0;
    this.partitioned = new Set();   // device ids that currently receive/send nothing
    this.dropped = 0;
    this.delivered = 0;
  }
  isDown(deviceId) { return this.partitioned.has(deviceId); }
  partition(deviceId) { this.partitioned.add(deviceId); }
  heal(deviceId) { this.partitioned.delete(deviceId); }

  /** Deliver `fn` after a sampled latency, or drop it. Returns true if it was scheduled. */
  send(deviceId, fn, label = "") {
    if (this.isDown(deviceId)) { this.dropped++; return false; }
    if (this.sim.rand() < this.lossRate) { this.dropped++; return false; }
    this.delivered++;
    this.sim.at(this.latencyMs + Math.floor(this.sim.rand() * (this.jitterMs + 1)), fn, label);
    return true;
  }
}

// ── the follower ────────────────────────────────────────────────────────────────────────────────
//
// A signovivo.com browser. Its ONE decision — what to do with a snapshot — is made by the real
// decideRelaySnapshot(). Everything below the call is the mechanical executor, copied field for
// field from applyRelaySnapshot in web/src/app.js:3925-3972, whose own docstring calls it "a thin
// executor of its verdict: no ordering logic here".
export class Follower {
  constructor(sim, { id, skewMs = 0, startPage = 1 }) {
    this.sim = sim;
    this.id = id;
    this.skewMs = skewMs;            // device clock = true time + skew
    this.socket = null;

    // Mirrors the `relay` state object, web/src/app.js:3759-3778.
    this.lastSeq = -1;
    this.hasDirector = false;
    this.browsing = false;
    this.following = true;
    this.appliedPage = null;
    this.livePage = null;
    this.currentPage = startPage;
    this.clockOffsetMs = 0;

    // Observation, not state. What a person in the pews would have seen.
    this.pageHistory = [];           // every page this follower actually rendered, in order
    this.revealed = false;           // has the reader ever been revealed (the page is visible)?
    this.pillHistory = [];           // "live" | "resync" | "hidden", appended on every render
    this.demotions = 0;
    this.rejects = 0;
  }

  deviceNow() { return this.sim.now + this.skewMs; }

  /** Mirrors applyRelaySnapshot, web/src/app.js:3887. */
  apply(snap, { force = false } = {}) {
    const wasLive = this.hasDirector;
    const d = decideRelaySnapshot(snap, {
      lastSeq: this.lastSeq,
      hasDirector: this.hasDirector,
      browsing: this.browsing,
      currentPage: this.currentPage,
      force,
      nowMs: this.deviceNow(),
      clockOffsetMs: this.clockOffsetMs,
      maxAgeS: RELAY_LIVE_MAX_AGE_S,
    });
    if (d.action === "reject") { this.rejects++; return d; }

    this.hasDirector = d.hasDirector;
    this.lastSeq = d.lastSeq;
    this.browsing = d.browsing;
    if (d.livePage != null) this.livePage = d.livePage;
    if (d.hideGoLiveBar) this.goLiveBarVisible = false;

    if (d.action === "follow") {
      this.following = true;
      this.appliedPage = snap.page;
      if (d.renderPage != null) this.renderPage(d.renderPage);
    }
    if (d.renderPill) this.renderPill();
    if (d.reveal) this.revealed = true;
    if (d.action === "demote") this.demotions++;

    // "A DEMOTION MAY BE OUR OWN CLOCK, AND ONLY /state CAN TELL US" — app.js:3959. One poll on the
    // live→dead transition recalibrates the offset and re-promotes. Self-limiting: wasLive is false
    // on the second pass, so it cannot loop.
    if (wasLive && !this.hasDirector) this.pollState({ force: true });
    return d;
  }

  renderPage(page) { this.currentPage = page; this.pageHistory.push(page); }
  renderPill() { this.pillHistory.push(!this.hasDirector ? "hidden" : this.following ? "live" : "resync"); }

  /** The user tapped a song title and jumped. app.js sets relay.browsing on that gesture. */
  browseTo(page) { this.browsing = true; this.following = false; this.renderPage(page); }

  /** "Volver a en vivo" — mirrors goLive(), web/src/app.js:3841. */
  goLive() {
    this.browsing = false;
    this.following = true;
    this.goLiveBarVisible = false;
    if (this.livePage != null) {
      this.appliedPage = this.livePage;
      if (this.currentPage !== this.livePage) this.renderPage(this.livePage);
    }
    this.renderPill();
  }

  /** Open the WebSocket. The relay pushes the current snapshot on accept (index.ts:304). */
  connect(relay, network) {
    this.relay = relay;
    this.network = network;
    const socket = {
      id: this.id,
      deliver: (snapshot) => {
        // A snapshot is serialized on the wire, so a follower can never see the relay's live object.
        // Passing the reference would let a later publish retroactively change a message already
        // "in flight" — the simulation would silently deliver the future.
        const frozen = JSON.parse(JSON.stringify(snapshot));
        network.send(this.id, () => { if (this.socket === socket) this.apply(frozen); }, `ws→${this.id}`);
      },
    };
    this.socket = socket;
    relay.subscribe(socket);
  }

  disconnect() {
    if (this.socket && this.relay) this.relay.unsubscribe(this.socket);
    this.socket = null;
    if (this.heartbeat) { this.heartbeat.stopped = true; this.heartbeat = null; }
  }

  /** The 4 s WebSocket heartbeat (web/src/app.js) — a "ping" the relay answers with the current
   *  snapshot. Re-arms itself on the virtual scheduler, so a whole Mass of heartbeats costs no
   *  real time. */
  startHeartbeat(intervalMs = 4000) {
    const h = { stopped: false, sent: 0 };
    this.heartbeat = h;
    const tick = () => {
      if (h.stopped || this.socket !== h.socket) return;
      h.sent++;
      const socket = this.socket;
      this.network.send(this.id, () => { if (this.socket === socket) this.relay.ping(socket); }, `ping←${this.id}`);
      this.sim.at(intervalMs, tick, `hb:${this.id}`);
    };
    h.socket = this.socket;
    this.sim.at(intervalMs, tick, `hb:${this.id}`);
    return h;
  }

  /** Inject a frame straight into the follower's message handler, bypassing the relay. The relay
   *  cannot produce a malformed snapshot — it assembles every field itself — so a hostile or
   *  corrupted frame (a truncated proxy response, an old worker, a future protocol version) can
   *  only be modelled by putting it on the wire directly. */
  receiveRaw(snap, opts) { return this.apply(snap, opts); }

  /** Mirrors reconnectRelay()'s web branch, web/src/app.js:3857 — the ⟳ control. Note lastSeq = -1:
   *  without it a director that restarted with a lower seq reads as a duplicate forever. */
  reconnect() {
    this.disconnect();
    this.browsing = false;
    this.following = true;
    this.lastSeq = -1;
    this.connect(this.relay, this.network);
    this.pollState({ force: true });
  }

  /** Mirrors relayPollOnce, web/src/app.js:4021-4069 — including the clock calibration that only
   *  this path performs. A WebSocket snapshot carries no `now`, so a follower that never polls can
   *  never correct its own clock. */
  pollState({ force = false } = {}) {
    if (!this.relay || !this.network) return;
    this.pollCount = (this.pollCount || 0) + 1;
    // WHAT WE KNEW WHEN THIS POLL LEFT — mirrors relayPollOnce, web/src/app.js:4027. A forced poll runs
    // concurrently with the live socket; if a push lands while the /state body is in flight, the body
    // is older than what we now show, and force would rewind the follower one page. The real code drops
    // the FORCE only and still applies normally (app.js:4070-4075), so the de-dup handles it and a
    // genuinely newer snapshot is still honoured. Without this copy, an 8%-loss sweep of sim-mass.mjs
    // rendered [46, 49, 46, 49] on a follower — a defect the real app does not have.
    const preSeq = this.lastSeq;
    this.network.send(this.id, () => {
      const body = this.relay.getState(this.sim.now);
      this.network.send(this.id, () => {
        const recvMs = this.deviceNow();
        if (Number.isFinite(body.now)) {
          this.clockOffsetMs = clockOffsetFromServerNow(body.now, recvMs, this.clockOffsetMs);
        }
        const lostRace = force && Number.isFinite(body.seq) && this.lastSeq > preSeq && body.seq < this.lastSeq;
        this.apply(body, { force: force && !lostRace });
      }, `state-res→${this.id}`);
    }, `state-req←${this.id}`);
  }
}

// ── the director ────────────────────────────────────────────────────────────────────────────────
//
// Drives the REAL src/directorRelaySync.js. That module is a singleton with module-level state
// (seqCounter, inFlight, pending, publishEnabled), so each director gets its own instance via a
// cache-busting import specifier — two directors sharing one seqCounter would be a fiction, and the
// handover tests exist precisely to exercise two independent seq streams.
//
// Getting the real module rather than a re-statement is the point: `nextSeq`'s
// `Math.max(seqCounter + 1, Date.now())` is the source of the fast-clock wedge, and the latest-wins
// coalescer is itself load-bearing ("a burst of page turns on a weak link never queues a backlog").
let directorInstances = 0;
export async function makeDirector(sim, { id, skewMs = 0, totalPages = 400 }) {
  const mod = await import(`../../src/directorRelaySync.js?sim=${++directorInstances}`);
  const director = {
    id,
    skewMs,
    totalPages,
    mod,
    publishedPages: [],   // every page this director TRIED to publish, in order
    deviceNow: () => sim.now + skewMs,
    stepDown() { mod.setRelayPublishing(false); },
    goLive() { mod.setRelayPublishing(true); },
    /** Turn to `page` and publish it. Date.now() is scoped to this device for the duration of the
     *  synchronous call, which is the whole window in which the module reads it (nextSeq + ts). */
    turnTo(page) {
      this.publishedPages.push(page);
      sim.withDeviceClock(skewMs, () => mod.publishPageToRelay(page, totalPages, { mode: "reader" }));
    },
  };
  return director;
}

// ── the simulation ──────────────────────────────────────────────────────────────────────────────
export class Sim extends Scheduler {
  constructor(opts = {}) {
    // A fixed, ordinary wall-clock start. Not 0: the seq rule compares seqs against epoch
    // milliseconds, so a simulation starting at 0 would exercise a universe the code never sees.
    super(opts.startMs ?? 1_787_000_000_000);
    this.rand = mulberry32(opts.seed ?? 1);
    this.seed = opts.seed ?? 1;
    this.relay = new Relay(this);
    this.network = new Network(this, opts);
    this.followers = [];
    this.directors = [];
    this._installed = false;
  }

  /** Run `fn` with Date.now() reporting a device's skewed clock. Scoped and synchronous: the real
   *  transmitter reads Date.now() only while building a payload, so nothing outside this window
   *  ever sees a patched clock. */
  withDeviceClock(skewMs, fn) {
    const real = Date.now;
    const at = this.now + skewMs;
    Date.now = () => at;
    try { return fn(); } finally { Date.now = real; }
  }

  /** Install the fetch stub the real transmitter publishes through. Every request becomes a network
   *  event, so a publish is subject to the same loss and latency as everything else. */
  install() {
    if (this._installed) return;
    this._realFetch = globalThis.fetch;
    this._installed = true;
    globalThis.fetch = (url, init = {}) => {
      const u = String(url);
      const deviceId = this._publishingAs || "director";
      return new Promise((resolve, reject) => {
        if (init.signal?.aborted) { reject(new Error("aborted")); return; }
        const body = init.body ? JSON.parse(init.body) : {};
        const sent = this.network.send(deviceId, () => {
          if (!u.includes("/publish")) { resolve({ ok: true, status: 200, json: async () => this.relay.getState(this.now) }); return; }
          const res = this.relay.publish(body, this.now);
          // The response travels back over the same lossy link. A lost RESPONSE is not a lost
          // publish — the relay already applied it — and the transmitter must not conclude failure.
          const back = this.network.send(deviceId, () => {
            resolve({ ok: res.status >= 200 && res.status < 300, status: res.status, json: async () => res.body });
          }, "publish-res");
          if (!back) reject(new Error("response lost"));
        }, "publish-req");
        if (!sent) reject(new Error("request lost"));
      });
    };
  }

  uninstall() {
    if (!this._installed) return;
    globalThis.fetch = this._realFetch;
    this._installed = false;
  }

  async addDirector(opts) {
    this.install();
    const d = await makeDirector(this, opts);
    d.goLive();
    this.directors.push(d);
    return d;
  }

  addFollower(opts) {
    const f = new Follower(this, opts);
    f.connect(this.relay, this.network);
    this.followers.push(f);
    return f;
  }

  /** Advance exactly `ms` of virtual time, letting every message that lands inside that window be
   *  delivered and every promise it unblocks resolve.
   *
   *  ADVANCE EXACTLY, NEVER "UNTIL QUIET". An earlier version drained the queue to quiescence, which
   *  is incoherent once anything re-arms itself: the 4 s heartbeat schedules its own successor, so
   *  "drain until empty" ran ten virtual minutes forward on every call. Tests that meant to observe
   *  a follower five seconds after a page turn were silently observing it ten minutes later, with
   *  the director long since aged out — and the failures looked exactly like real demotion bugs.
   *  Two of the tests in svSyncSystem.test.mjs failed that way before this was fixed. Use idle() to
   *  skip time on purpose. */
  async settle(ms = 5000) {
    const target = this.now + ms;
    // ONE EVENT AT A TIME, FLUSHING PROMISES BETWEEN EACH. Batch-processing the window and only
    // then flushing microtasks does not work here, because the transmitter's coalescer drains from
    // a `finally` that runs as a promise continuation: the response lands, the continuation posts
    // the queued page, and it schedules relative to whatever the clock says at that moment. Batch
    // the window first and the clock has already jumped to the far edge, so the drain is scheduled
    // past it and simply never happens — a 100-page burst delivered its first page and lost the
    // other ninety-nine, which reads as a coalescer bug rather than a harness one.
    for (let guard = 0; guard < 100000; guard++) {
      await Promise.resolve();
      await new Promise((r) => setImmediate(r));
      this.queue.sort((a, b) => a.t - b.t || a.i - b.i);
      if (!this.queue.length || this.queue[0].t > target) break;
      const e = this.queue.shift();
      this.now = e.t;
      e.fn();
    }
    this.now = Math.max(this.now, target);
  }

  /** Advance virtual time WITHOUT delivering anything new — used to age a snapshot past the
   *  freshness window while the director is silent. */
  async idle(ms) {
    this.runUntil(this.now + ms);
    await new Promise((r) => setImmediate(r));
  }
}

// ── fidelity ────────────────────────────────────────────────────────────────────────────────────
//
// The harness copies a handful of MECHANICAL steps out of production (a clamp, a constant, a field
// assignment). Those copies are the one way this file can become fiction: production changes, the
// simulation keeps passing, and the green run now proves something about a system that no longer
// exists. These checks read the real sources and fail when a copy drifts.
//
// They are deliberately anchored on structure rather than on a character offset — five separate
// tests in this repo broke by slicing a fixed number of characters and silently running to EOF.
export function readSource(rel) {
  return fs.readFileSync(path.join(REPO, rel), "utf8");
}

/** The freshness window is declared in three independent places and the system is only coherent
 *  while they agree. If the relay thinks a director is live for 90 s and a follower for 60 s, there
 *  is a 30 s hole in which the relay accepts page turns that every follower discards as stale. */
export function freshnessWindows() {
  const worker = readSource("sync-worker/src/index.ts").match(/const RELAY_LIVE_MAX_AGE_S\s*=\s*(\d+)/);
  const web = readSource("web/src/app.js").match(/const RELAY_LIVE_MAX_AGE_S\s*=\s*(\d+)/);
  const lib = readSource("web/src/lib/svSyncDecision.js").match(/isFiniteNum\(ctx\.maxAgeS\)\s*\?\s*ctx\.maxAgeS\s*:\s*(\d+)/);
  return {
    worker: worker && Number(worker[1]),
    web: web && Number(web[1]),
    libDefault: lib && Number(lib[1]),
    harness: RELAY_LIVE_MAX_AGE_S,
  };
}

/**
 * Compare the harness's copy of the relay's field assembly against the real Durable Object.
 *
 * The harness delegates the one DECISION (decidePublish) but hand-copies the MECHANICAL step that
 * follows it: the page floor, the totalPages floor, the 64-character caps, and the server ts stamp.
 * Those four are load-bearing — the page floor is what stops a follower's -1 render sentinel
 * reaching the congregation as page 1 — and a hand copy is exactly the thing that drifts while
 * every test stays green.
 *
 * Rather than compare either side against a literal written here (which would drift with neither),
 * this pulls the numbers out of BOTH and compares them to each other. A change on either side that
 * is not made on the other fails.
 */
export function relayFieldContract() {
  const worker = readSource("sync-worker/src/index.ts");
  const harness = readSource("e2e/helpers/sv-sync-sim.mjs");

  const block = (src, startMarker, endMarker, what) => {
    const a = src.indexOf(startMarker);
    const b = a >= 0 ? src.indexOf(endMarker, a) : -1;
    if (a < 0 || b < 0) return null;   // caller asserts; a missing marker must never widen to EOF
    return src.slice(a, b);
  };
  // Bounded by the assignment itself and the very next statement — structural on both sides, and
  // both endpoints are checked by the caller.
  const workerBlock = block(worker, "const next: Snapshot = {", "await this.ctx.storage.put", "worker");
  const harnessBlock = block(harness, "const next = {", "this.snapshot = next;", "harness");

  const shape = (b) => b && ({
    // Every numeric literal in the block, in order: the clamp bounds and the length caps.
    numbers: [...b.matchAll(/\b(\d+)\b/g)].map((m) => Number(m[1])),
    // The relay must stamp its OWN clock, in seconds. A block that echoed the client's ts would
    // make a fast-clocked director permanently fresh and a slow one permanently stale.
    stampsOwnSeconds: /ts:\s*Math\.floor\((?:Date\.now\(\)|nowMs)\s*\/\s*1000\)/.test(b),
    echoesClientTs: /ts:\s*[^,\n]*input\.ts/.test(b),
  });

  return { workerBlock, harnessBlock, worker: shape(workerBlock), harness: shape(harnessBlock) };
}

/**
 * THE FUNCTION THIS FILE'S HEADER NAMES as the only thing keeping the harness from becoming fiction.
 *
 * It was referenced twice in prose and never written — caught by an adversarial re-hunt of this very
 * campaign, which is a tidy demonstration of the thesis: a claimed guard that does not exist is
 * indistinguishable, from the outside, from one that does.
 *
 * Called from the first test in svSyncSystem.test.mjs so it runs before anything relies on the
 * harness being faithful.
 */
export function assertHarnessFidelity(assert) {
  const w = freshnessWindows();
  assert.equal(typeof w.worker, "number", "RELAY_LIVE_MAX_AGE_S is no longer readable from sync-worker/src/index.ts");
  assert.equal(w.harness, w.worker, "the harness's freshness window has drifted from the worker's");

  const c = relayFieldContract();
  assert.ok(c.workerBlock, "could not locate the worker's snapshot assembly (const next: Snapshot = {…}) — " +
    "the harness can no longer be checked against it, so treat every relay assertion as unverified");
  assert.ok(c.harnessBlock, "could not locate the harness's snapshot assembly");
  assert.deepEqual(c.harness.numbers, c.worker.numbers,
    `the harness clamps with ${JSON.stringify(c.harness.numbers)} but the worker clamps with ` +
    `${JSON.stringify(c.worker.numbers)} — the simulation is testing a relay that does not ship`);
  assert.equal(c.worker.stampsOwnSeconds, true, "the worker no longer stamps its own clock in seconds");
  assert.equal(c.harness.stampsOwnSeconds, true, "the harness no longer stamps its own clock in seconds");
  assert.equal(c.worker.echoesClientTs, false, "the worker now echoes the client's ts — freshness stops meaning anything");
  assert.equal(c.harness.echoesClientTs, false, "the harness echoes the client's ts");

  // The two behaviours the whole dead-director test rests on, read out of the worker rather than
  // assumed: a socket is handed the snapshot when it is accepted, and a ping is answered with the
  // snapshot AS OF THE REPLY, which is what keeps a dead director arriving forever.
  const worker = readSource("sync-worker/src/index.ts");
  const accept = worker.indexOf("this.ctx.acceptWebSocket(server);");
  assert.ok(accept > 0, "the worker no longer accepts websockets where this harness expects");
  assert.match(worker.slice(accept, accept + 400), /server\.send\(JSON\.stringify\(this\.snapshot\)\)/,
    "the worker no longer pushes the snapshot on accept — a fresh follower would land on no page, " +
    "and the harness's subscribe() would be simulating a behaviour that does not exist");
  const wsMsg = worker.indexOf("async webSocketMessage(");
  const wsEnd = worker.indexOf("async webSocketClose(");
  assert.ok(wsMsg > 0 && wsEnd > wsMsg, "could not bound the worker's webSocketMessage handler");
  assert.match(worker.slice(wsMsg, wsEnd), /ws\.send\(JSON\.stringify\(this\.snapshot\)\)/,
    "a ping is no longer answered with the CURRENT snapshot — the dead-director scenario the " +
    "harness models cannot occur, so that test would pass without exercising anything");

  // /state's `now` is the only channel a follower can calibrate its clock from.
  assert.match(worker, /\{\s*\.\.\.snapshot,\s*now:\s*Math\.floor\(Date\.now\(\)\s*\/\s*1000\)\s*\}/,
    "/state no longer returns `now` in epoch seconds — clock calibration has no source, and the " +
    "harness's pollState is simulating a field the worker does not send");
}

/** Every field the decision module produces must be consumed by the caller that executes it.
 *  A field added to svSyncDecision and never read by app.js is a decision that silently does
 *  nothing — the exact shape of the bug that left `forceFollowerReconnectNow` implemented in Swift,
 *  absent from the bridge, and quietly resolving to null on every ⟳ tap for months. */
export function decisionContract() {
  const lib = readSource("web/src/lib/svSyncDecision.js");
  const app = readSource("web/src/app.js");
  const produced = new Set([...lib.matchAll(/\bout\.([A-Za-z_$][\w$]*)\s*=/g)].map((m) => m[1]));
  // Bound the executor by the two structural markers around it, and assert both exist — a slice
  // whose end marker has been deleted runs to EOF and would match anything anywhere in the file.
  const start = app.indexOf("const d = lib.decideRelaySnapshot(snap, {");
  const end = app.indexOf("const relayStateUrl =");
  const executor = start >= 0 && end > start ? app.slice(start, end) : "";
  const consumed = new Set([...executor.matchAll(/\bd\.([A-Za-z_$][\w$]*)/g)].map((m) => m[1]));
  return { produced: [...produced].sort(), consumed: [...consumed].sort(), executorFound: start >= 0 && end > start };
}
