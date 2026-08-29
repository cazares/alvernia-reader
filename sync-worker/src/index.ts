/// <reference types="@cloudflare/workers-types" />
//
// SignoVivo Sync — featherweight relay (Path D).
// Director POSTs the current page; every signovivo.com browser gets it pushed.
// One Durable Object instance per "room" (one church = one room: "alvernia-main").
//
// Endpoints (see README.md):
//   GET  /r/:room/subscribe  (WebSocket)  — followers; receive current snapshot + every update
//   GET  /r/:room/state                   — followers; poll fallback / initial paint (tiny JSON)
//   POST /r/:room/publish    (open)       — director; set the current page, fan out to subscribers
//
import { DurableObject } from "cloudflare:workers";
// Pure arming decision, kept in plain JS so node --test can cover it without a worker runtime.
// See sync-worker/test/bookArming.test.mjs — this is the only logic here that fans out to every
// iPad at once, and the worker otherwise has no coverage at all.
// @ts-expect-error - plain JS module with no .d.ts; the shape is asserted by its tests
import { decideBookUpdate } from "./bookArming.js";
// Diagnostic ring-buffer sizing + run-length folding. Plain JS for the same reason as above:
// this is the instrument every mesh diagnosis depends on, so it gets real tests.
// @ts-expect-error - plain JS module with no .d.ts; the shape is asserted by its tests
import { foldLogEntries, logIntervalMs, logLevel, LOG_RATE_BURST, LOG_RATE_PER_SEC } from "./logBuffer.js";

export interface Env {
  SYNC_ROOM: DurableObjectNamespace<SyncRoom>;
  /** Secret the director must send as `Authorization: Bearer <token>`. Set via `wrangler secret put`. */
  RELAY_DIRECTOR_TOKEN: string;
  /** Comma-separated allow-list for CORS on /state. "*" allows all (fine — no credentials). */
  ALLOWED_ORIGINS?: string;
  /** Gates reading and wiping the diagnostic log (GET/DELETE /log), which holds sync breadcrumbs.
   *  Named for the fleet dashboard it used to guard; that dashboard was removed on 2026-08-18 but
   *  this credential still has a job, so it stays. Renaming it would mean rotating a secret to fix
   *  a comment. */
  FLEET_DASHBOARD_KEY?: string;

  /** Daily budget for NON-ESSENTIAL requests (telemetry, fleet dashboard). Default 10000.
   *  See nonEssentialBudget() — this is a reservation for signovivo.com, not a rate limit. */
  NONESSENTIAL_DAILY_MAX?: string;

  /** How often a device should FLUSH its batched telemetry, in ms. Echoed on every POST /log
   *  response so the fleet can be throttled — or silenced with "0" — WITHOUT a TestFlight build.
   *
   *  THE KILL SWITCH THAT DID NOT EXIST. On 2026-08-17 this account tripped Cloudflare's
   *  free-plan Workers cap (100,000 requests/day): it served 99,428 requests, of which
   *  signovivo-sync was 87,258 (87.8%), and BOTH signovivo.com and this Worker returned
   *  429/1027 for hours. Every other site on the account combined for ~12,000. There was no
   *  way to stop the traffic short of shipping a build, so it simply ran until UTC midnight.
   *
   *  Default 15000. Sized by replaying that day's captures through
   *  scripts/telemetry-budget-sim.mjs: 15 s batching is a 4.8x cut with 100% of rows still
   *  delivered (87,258 -> ~18,000/day). Raising it cuts further; "0" stops the fleet dead. */
  LOG_INTERVAL_MS?: string;

  /** Telemetry verbosity the fleet should log at: off | error | warn | info | debug (default off).
   *  Echoed on POST /log, so this + `wrangler deploy` retunes every device in ~20s. */
  LOG_LEVEL?: string;

  // ── Songbook OTA arming (docs/choir-pdf-distribution-plan.md §5.3) ─────────
  // SHIPPED DORMANT: with BOOK_UPDATE_VERSION empty the `bookUpdate` field never appears in any
  // response, so no client can act on it. Arming requires editing wrangler.jsonc and redeploying,
  // and reaching the WHOLE fleet requires TWO independent vars — see bookArming.js.
  /** "" = dormant. Otherwise the bv_<16hex> every armed device should converge on. */
  BOOK_UPDATE_VERSION?: string;
  /** "" = nobody | "<deviceId>[,<deviceId>]" | "*" (only honoured with BOOK_UPDATE_ALLOW_FLEET). */
  BOOK_UPDATE_DEVICES?: string;
  /** Must be exactly "yes" before "*" means anything. */
  BOOK_UPDATE_ALLOW_FLEET?: string;
  /** Origin the bundle is fetched from. The CLIENT also allowlists hosts; this is not trusted alone. */
  BOOK_UPDATE_BASE?: string;
  /** Max devices mid-download at once under "*". Keeps 8 iPads off one parish access point. */
  BOOK_UPDATE_CONCURRENCY?: string;

  /** Latest shipped native build number, echoed on /ota/checkin as `latestNativeBuild` so a
   *  behind device can prompt its own "update the app" nudge. Purely informational — never gates
   *  or blocks anything, unlike BOOK_UPDATE_*. Kept in lockstep by release.sh; see wrangler.jsonc. */
  LATEST_NATIVE_BUILD?: string;
}

const PROTOCOL_VERSION = 1;

// Past which age (seconds) a snapshot counts as "no active director" — matches the web
// follower's RELAY_LIVE_MAX_AGE_S. Build 332+ directors re-publish every ~12s, so a live
// director never goes stale; a stale snapshot means the director is gone.
const RELAY_LIVE_MAX_AGE_S = 90;

type Snapshot = {
  v: number;
  page: number;
  totalPages: number;
  mode: string;
  bookId: string;
  /** Monotonic per director session. seq===0 means "no director live yet". */
  seq: number;
  /** Unix seconds of last publish — clients use freshness to detect a live director. */
  ts: number;
};

const EMPTY_SNAPSHOT: Snapshot = {
  v: PROTOCOL_VERSION,
  page: 0,
  totalPages: 0,
  mode: "",
  bookId: "",
  seq: 0,
  ts: 0,
};

/** A single device's OTA-relevant state — deliberately narrower than the old FleetDevice.
 *  No label, no role, no phone-adjacent field can ever reach this shape. */
type OtaDevice = { deviceId: string; bookVersion?: string; bookStage?: string; ts: number };

export class SyncRoom extends DurableObject<Env> {
  private snapshot: Snapshot = EMPTY_SNAPSHOT;

  // ── Per-IP rate limiting (A2) ──────────────────────────────────────────────
  // A token bucket per client IP, held in THIS DO instance's memory. A room DO is a
  // singleton, so this bucket sees EVERY request to that room/resource → a real global
  // limit (unlike a per-Worker-isolate Map, which each isolate would see only a slice of).
  // FAIL-OPEN by contract: a real director/device runs far below these limits, and blocking
  // a legit director mid-Mass is far worse than an attacker slipping a few requests through,
  // so ANY error (or a missing IP) → allowed.
  private rateBuckets = new Map<string, { tokens: number; last: number }>();
  private rateLimited(ip: string, capacity: number, refillPerSec: number): boolean {
    try {
      if (!ip) return false; // no IP to bucket on — never block
      const now = Date.now();
      // Bound memory against an IP-spray flood: if the map grows huge, reset it. Buckets refill in
      // seconds, so the per-IP sustained limit re-applies at once; only a transient burst could slip.
      if (this.rateBuckets.size > 20000) this.rateBuckets.clear();
      let b = this.rateBuckets.get(ip);
      if (!b) {
        b = { tokens: capacity, last: now };
        this.rateBuckets.set(ip, b);
      }
      b.tokens = Math.min(capacity, b.tokens + ((now - b.last) / 1000) * refillPerSec);
      b.last = now;
      if (b.tokens < 1) return true; // no tokens → limited
      b.tokens -= 1;
      return false;
    } catch {
      return false; // fail-open: never block a real director on a rate-limiter bug
    }
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Restore the latest page on wake (covers hibernation / eviction).
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<Snapshot>("snapshot");
      if (stored) this.snapshot = stored;
    });
  }

  /** RPC: director publishes a new page state. Latest-wins, stale-guarded. */
  async publish(
    input: Partial<Snapshot>,
    ip = "",
  ): Promise<{ ok: true; seq: number; ignored?: boolean; rateLimited?: boolean }> {
    // A2 rate limit: a real director publishes ~1 page turn every few seconds plus a 12s heartbeat —
    // far under 2/sec. Cap 15 (burst) + 2/sec sustained stops a flood (page-hijack) without ever
    // touching a legitimate director. Checked first so a flood is cheap to reject.
    if (this.rateLimited(ip, 15, 2)) {
      return { ok: true, seq: this.snapshot.seq, rateLimited: true };
    }
    // Sanitize seq before it touches the guard. A non-finite (Infinity/NaN), negative, or
    // unreachably-high seq would poison the room: Infinity serializes as null and, since every
    // finite seq is <= Infinity, would block every future director for the whole live window.
    // Collapse any such value to 0 so the `incomingSeq > 0 ? … : this.snapshot.seq + 1` branch
    // below assigns a sane monotonic seq instead.
    let incomingSeq = Number(input.seq ?? 0);
    if (!Number.isFinite(incomingSeq) || incomingSeq < 0) {
      incomingSeq = 0;
    } else if (incomingSeq > Date.now() + 60000) {
      // CLAMP A FAST CLOCK, DO NOT COLLAPSE IT TO THE RESERVED VALUE.
      //
      // The native transmitter's seq IS the device's wall clock in ms (directorRelaySync.js:
      // `Math.max(seqCounter + 1, Date.now())`). So a director iPad whose clock runs more than a
      // minute ahead of Cloudflare's — automatic date/time off, or drift on a device that was long
      // powered down — trips this bound on EVERY publish. Folding that to 0 then met the seq=0 gate
      // below, which rejects 0 while the snapshot is fresh, and the two rules wedged each other: the
      // first publish landed (stale snapshot lets 0 through) and refreshed ts, which then made every
      // page turn and every keepalive for the next 90 s "ignored", after which exactly one more
      // landed. Web followers advanced roughly once per 90 seconds while the app reported ok:true on
      // every publish and the pill stayed green — the silent frozen-congregation failure, with no
      // banner and no breadcrumb anywhere.
      //
      // 0 is a RESERVED value meaning "no director has published"; a real seq must never be folded
      // into it. Clamping into the window keeps the director monotonic under the SERVER's clock, and
      // the snapshot.seq + 1 floor keeps two publishes clamped in the same server millisecond from
      // colliding with the <= guard.
      incomingSeq = Math.max(Date.now() + 60000, this.snapshot.seq + 1);
    }
    // The seq guard stops a burst of page turns on a weak link from arriving out of order
    // and rewinding the song — but ONLY while a director is actively live (fresh snapshot).
    // If nobody has published within the live window the snapshot is stale (no active
    // director), so a NEW director may take over regardless of seq. This also self-heals a
    // poisoned / wrongly-scaled seq left behind by a gone director (otherwise a too-high
    // stale seq would silently block every future director from ever transmitting).
    const nowSec = Math.floor(Date.now() / 1000);
    const snapshotStale =
      this.snapshot.seq === 0 || nowSec - this.snapshot.ts > RELAY_LIVE_MAX_AGE_S;
    // A2 seq=0 gate: seq=0 must NOT bypass the monotonic guard while a director is live. A legit live
    // director always sends a real (>0) monotonic seq (native transmitters use wall-clock ms); a
    // seq=0 — or an invalid seq we collapsed to 0 above — arriving while a FRESH director is
    // broadcasting is malformed or an override attempt, so reject it. seq=0 is only honored as a
    // takeover/reset when the snapshot is already stale (no active director), which the branch below
    // and the next-seq assignment already handle.
    if (!snapshotStale && incomingSeq === 0) {
      return { ok: true, seq: this.snapshot.seq, ignored: true };
    }
    if (!snapshotStale && incomingSeq > 0 && incomingSeq <= this.snapshot.seq) {
      return { ok: true, seq: this.snapshot.seq, ignored: true };
    }
    const next: Snapshot = {
      v: PROTOCOL_VERSION,
      page: Math.max(1, Math.min(Number(input.page ?? this.snapshot.page) || 1, 100000)),
      totalPages: Math.max(0, Math.min(Number(input.totalPages ?? this.snapshot.totalPages) || 0, 100000)),
      mode: String(input.mode ?? this.snapshot.mode ?? "").slice(0, 64),
      bookId: String(input.bookId ?? this.snapshot.bookId ?? "").slice(0, 64),
      seq: incomingSeq > 0 ? incomingSeq : this.snapshot.seq + 1,
      ts: Math.floor(Date.now() / 1000),
    };
    // Persist first, then cache, then broadcast (durability before fan-out).
    await this.ctx.storage.put("snapshot", next);
    this.snapshot = next;
    this.broadcast(next);
    return { ok: true, seq: next.seq };
  }

  /** RPC: current state for poll fallback / first paint. */
  async getState(): Promise<Snapshot> {
    return this.snapshot;
  }

  /** RPC: append diagnostic breadcrumbs to a capped ring buffer (debug telemetry only — no sync
   *  data). Devices POST their Multipeer sync lifecycle here so it can be read back remotely. */
  async appendLog(
    entries: unknown[],
    ip = "",
  ): Promise<{ ok: true; total: number; rateLimited?: boolean }> {
    // SIZED FOR A ROOM, NOT A PAIR. The old limit bucketed by IP at 3/sec sustained, and a whole
    // fleet leaves through ONE NAT — six iPads flushing once a second had over half their
    // telemetry refused, with ok:true and no device-side retry, so the log looked calm precisely
    // when it was losing the evidence. And 600 shared entries is ~100 seconds at fleet scale, most
    // of it keepalive that evicts the one disconnect explaining why the choir stopped following.
    // Both ceilings and the run-length fold live in logBuffer.js so node --test can cover them.
    if (this.rateLimited(ip, LOG_RATE_BURST, LOG_RATE_PER_SEC)) {
      return { ok: true, total: 0, rateLimited: true };
    }
    const existing = (await this.ctx.storage.get<unknown[]>("dbglog")) ?? [];
    const next = foldLogEntries(existing, entries, Math.floor(Date.now() / 1000));
    await this.ctx.storage.put("dbglog", next);
    return { ok: true, total: next.length };
  }

  /** RPC: read the diagnostic ring buffer. */
  async readLog(): Promise<unknown[]> {
    return (await this.ctx.storage.get<unknown[]>("dbglog")) ?? [];
  }

  /** RPC: clear the diagnostic ring buffer. */
  /** Count one non-essential request against today's budget and return the new total.
   *
   *  Deliberately counts BEFORE the caller decides, so the answer is "you are the Nth request today"
   *  and a burst cannot slip through on a stale read. Keyed by UTC date because that is the boundary
   *  Cloudflare resets the account quota on, so the two windows line up exactly.
   *
   *  One key, overwritten daily. Old days are dropped rather than accumulated: this is a meter, not
   *  an audit log, and a growing history in a DO is another thing to pay for. */
  async spendNonEssential(day: string, cap: number): Promise<number> {
    const rec = (await this.ctx.storage.get<{ day: string; n: number }>("budget")) ?? { day, n: 0 };
    if (rec.day !== day) { rec.day = day; rec.n = 0; }
    rec.n += 1;
    // Stop writing once well past the cap: the answer does not change, and a refused request should
    // not keep paying for storage to say so.
    if (rec.n <= cap + 100) await this.ctx.storage.put("budget", rec);
    return rec.n;
  }

  /** RPC: record a device's OTA-relevant state. NARROW ON PURPOSE.
   *
   *  This restores what the deleted fleet dashboard's /fleet/checkin used to carry for free
   *  (2026-08-18: "kill the fleet dashboard" removed the ONLY delivery path a device had for
   *  learning about a new songbook, because arming was piggybacked on that endpoint's response —
   *  see decideBookUpdate's call site in the pre-removal history). Restored as its OWN endpoint,
   *  its OWN Durable Object instance ("__ota__", never "__fleet__"), and its OWN storage shape —
   *  deviceId, bookVersion, bookStage, ts, nothing else — so there is no path back to storing a
   *  label, a role, or anything PII-adjacent under this name. If a caller sends more fields, they
   *  are silently dropped, not stored. */
  async otaCheckin(input: unknown, ip = ""): Promise<{ ok: true }> {
    if (this.rateLimited(ip, 10, 1)) return { ok: true };
    const o = (input && typeof input === "object" ? input : {}) as Record<string, unknown>;
    const deviceId = String(o.deviceId ?? "").slice(0, 64);
    if (!deviceId) return { ok: true };
    const devices = (await this.ctx.storage.get<Record<string, OtaDevice>>("ota_devices")) ?? {};
    const entry: OtaDevice = { deviceId, ts: Math.floor(Date.now() / 1000) };
    if (o.bookVersion != null) entry.bookVersion = String(o.bookVersion).slice(0, 32);
    if (o.bookStage != null) entry.bookStage = String(o.bookStage).slice(0, 40);
    devices[deviceId] = entry;
    // Same ring-cap idiom as the old fleet store: bounded, newest-first.
    const kept = Object.values(devices).sort((a, b) => b.ts - a.ts).slice(0, 300);
    const pruned: Record<string, OtaDevice> = {};
    for (const d of kept) pruned[d.deviceId] = d;
    await this.ctx.storage.put("ota_devices", pruned);
    return { ok: true };
  }

  /** RPC: every device's OTA state, for decideBookUpdate's fleet-wide throttle count. */
  async getOtaDevices(): Promise<OtaDevice[]> {
    const devices = (await this.ctx.storage.get<Record<string, OtaDevice>>("ota_devices")) ?? {};
    return Object.values(devices);
  }

  async clearLog(): Promise<{ ok: true }> {
    await this.ctx.storage.put("dbglog", []);
    return { ok: true };
  }

  // (The fleet readiness RPCs — putRoster / checkin / getFleet / resetFleet — were removed with the
  //  dashboard on 2026-08-18. Rows already in the "__fleet__" instance are unreachable but not
  //  deleted; removing code does not remove data.)

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    // Hibernatable accept: the DO can evict from memory while sockets stay open.
    this.ctx.acceptWebSocket(server);
    // Land a fresh follower on the right page immediately.
    try {
      server.send(JSON.stringify(this.snapshot));
    } catch {
      /* ignore */
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Followers are read-only; support an explicit resync/ping, ignore everything else. */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message === "string" && (message === "resync" || message === "ping")) {
      try {
        ws.send(JSON.stringify(this.snapshot));
      } catch {
        /* ignore */
      }
    }
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
    try {
      ws.close();
    } catch {
      /* ignore */
    }
  }

  async webSocketError(): Promise<void> {
    /* hibernation cleans up; nothing to do */
  }

  private broadcast(snapshot: Snapshot): void {
    const data = JSON.stringify(snapshot);
    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(data);
      } catch {
        /* a dead socket will be reaped by the runtime */
      }
    }
  }
}

// ──────────────────────────────── Worker ────────────────────────────────────

function corsHeaders(origin: string | null, env: Env): Record<string, string> {
  const list = (env.ALLOWED_ORIGINS || "*").split(",").map((s) => s.trim());
  let allow = "*";
  if (!list.includes("*")) {
    allow = origin && list.includes(origin) ? origin : list[0];
  }
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    // X-Director-Code is required so the web app (inside the native file:// WebView, or
    // signovivo.com) can POST /publish without the preflight stripping the auth header.
    "Access-Control-Allow-Headers": "Authorization,Content-Type",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
  };
}

function json(data: unknown, status: number, cors: Record<string, string>): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...cors },
  });
}


// There is no transmitter/director code any more. TRANSMITTER_CODES and its lookup were deleted
// on 2026-08-06 along with the /publish gate that used them — see the publish route below. The
// secret can be removed with `npx wrangler secret delete TRANSMITTER_CODES`; leaving it set is
// harmless, nothing reads it.

const ROUTE = /^\/r\/([A-Za-z0-9_-]{1,64})\/(subscribe|publish|state|unlock)$/;

// ── Fleet dashboard rendering (server-side; gated) ───────────────────────────
// Followers must be ≥ this native build to sync reliably with a current director (357 = connect
// fix, 361 = fast half-open recovery). The director must be on the FLEET-latest build.
const MIN_SYNC_BUILD = 361;

const escHtml = (s: unknown): string =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

// Normalize a name for matching a device's self-entered label to a roster person.
const normName = (s: unknown): string =>
  String(s ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

// (renderFleetDashboard and its tel: helper were removed with the dashboard on 2026-08-18. They
//  rendered choir phone numbers into HTML, so there is no version of this worth keeping around
//  "just in case".)

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const cors = corsHeaders(origin, env);

    // Single-book, fully-public app: everyone gets the standard manual. No IP-geo, no book
    // selection. Outer guard: any unexpected throw below (DO eviction, RPC transport error,
    // runtime hiccup) must STILL return CORS — otherwise the web client's fetch() rejects.
    try {
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: cors });
      }

      if (url.pathname === "/" || url.pathname === "/health") {
        return json({ ok: true, service: "signovivo-sync", v: PROTOCOL_VERSION }, 200, cors);
      }

      // Diagnostic telemetry sink. Devices POST their Multipeer sync lifecycle (role chosen, peer
      // found, invite, connect/disconnect, page sent/received) here; GET reads the ring buffer back
      // so the whole director↔follower handshake can be inspected remotely. DELETE clears it.
      // A single fixed debug DO holds the buffer (separate from any sync room).
      // ── QUOTA RESERVATION FOR signovivo.com ──────────────────────────────────────────────
      //
      // Cloudflare's free plan allows 100,000 Worker/Pages-Function requests a DAY, ACCOUNT-WIDE.
      // signovivo.com and this relay share that one number, so diagnostics can and did starve the
      // product: on 2026-08-17 and again on 2026-08-18 telemetry exhausted the quota and took the
      // site down with it. Cloudflare offers no way to partition a quota, so it is partitioned here.
      //
      // Non-essential traffic (telemetry, fleet dashboard) gets NONESSENTIAL_DAILY_MAX — 10,000 by
      // default, 10% — and is refused beyond it. Essential traffic (the site, /state, /subscribe,
      // director publishes) is NEVER counted and NEVER refused, so the remaining ~90,000 is reserved
      // for the thing people actually use.
      //
      // 10,000 is sized from both sides: a heavy Mass day of web followers is 10-20k requests
      // (an offline download alone is ~389), so 90k is deep headroom; and 10k buys ~10 hours of
      // 4-device telemetry at the 15s flush interval, which is longer than any debugging session.
      //
      // The counter lives in a DO and is touched ONLY by non-essential paths, so metering the
      // faucet costs the product nothing.
      const NON_ESSENTIAL = url.pathname === "/log" || url.pathname === "/ota/checkin";
      if (NON_ESSENTIAL) {
        const cap = Math.max(0, Number(env.NONESSENTIAL_DAILY_MAX ?? "10000") || 0);
        const day = new Date().toISOString().slice(0, 10);   // UTC, same boundary Cloudflare resets on
        const spent = await env.SYNC_ROOM.getByName("__budget__").spendNonEssential(day, cap);
        if (spent > cap) {
          return json(
            {
              error: "non-essential daily budget exhausted",
              spent,
              cap,
              day,
              note: "Reserved so signovivo.com always has quota. Raise NONESSENTIAL_DAILY_MAX to lift it.",
            },
            503,
            { "Retry-After": "3600" },
          );
        }
      }

      // ── OTA arming pointer — restored 2026-08-18 ────────────────────────────────────────
      //
      // The ONLY delivery path a device has for learning about a new songbook. Deliberately its
      // own route, its own DO instance ("__ota__"), its own narrow storage shape — see otaCheckin
      // above for why. POST-only: a device reports {deviceId, bookVersion?, bookStage?,
      // nativeBuild?} and gets back {bookUpdate?, nativeBuildConfirmedLatest?}. Absent bookUpdate
      // is not an error, it is the dormant default (see bookArming.js) — the client must not
      // treat a network failure the same as an explicit "no update", so this fails soft: any
      // error here still returns 200 with the informational fields omitted rather than
      // surfacing a 500 the client would have no correct way to react to.
      if (url.pathname === "/ota/checkin") {
        if (request.method !== "POST") return json({ ok: false, error: "method_not_allowed" }, 405, cors);
        if (Number(request.headers.get("content-length") || 0) > 4 * 1024) {
          return json({ ok: false, error: "payload_too_large" }, 413, cors);
        }
        const ota = env.SYNC_ROOM.getByName("__ota__");
        let body: unknown = null;
        try { body = await request.json(); } catch { body = null; }
        const ip = request.headers.get("CF-Connecting-IP") || "";
        let bookUpdate: { bookVersion: string; base: string } | null = null;
        try {
          await ota.otaCheckin(body, ip);
          const deviceId = String((body as Record<string, unknown>)?.deviceId ?? "").slice(0, 64);
          if (deviceId && String(env.BOOK_UPDATE_VERSION || "").trim()) {
            const devices = await ota.getOtaDevices();
            const me = devices.find((d) => d.deviceId === deviceId) || { deviceId, ts: 0 };
            bookUpdate = decideBookUpdate(env, me, devices, Math.floor(Date.now() / 1000));
          }
        } catch {
          // A 503 IS THE ONLY SAFE FAILURE HERE — 200 IS A DESTRUCTIVE ONE.
          //
          // This used to swallow the error into bookUpdate = null and still return 200 {ok:true},
          // reasoning that it "fails soft" so "the client must not treat a network failure the same
          // as an explicit no update". The client contract is exactly inverted from that assumption:
          // an OK response that does not name the device's staged version IS the explicit revoke
          // (onCheckinResponse deletes STORAGE_KEYS.bookStaged and rmrf's WebBundleStaged), while a
          // non-ok response is the one shape it already ignores without touching any state.
          //
          // So the shape this catch produced was the single most destructive one available: a
          // transient Durable Object storage error or an eviction mid-call destroyed a verified
          // 27 MB staged copy on every armed device that happened to check in during the blip, and
          // re-downloaded it — presenting as "the OTA never sticks on that iPad".
          //
          // Only a decideBookUpdate call that actually RAN and returned null may keep producing the
          // 200-without-bookUpdate revoke shape; that is a real disarm and must still work.
          return json({ ok: false, error: "arming_unavailable" }, 503, cors);
        }
        // NATIVE BUILD FRESHNESS — see wrangler.jsonc's LATEST_NATIVE_BUILD comment. Fails toward
        // "not confirmed" (the field is simply omitted, never sent as false): a device on a
        // binary from before this field existed sends no nativeBuild at all and Number(undefined)
        // is NaN, so it silently gets no flag either way — which is correct, since that binary
        // has no code to interpret one. Any parse failure or mismatch also omits the flag rather
        // than risk a false "you're current" from a malformed request.
        let nativeBuildConfirmedLatest: true | undefined;
        try {
          const latest = Number(env.LATEST_NATIVE_BUILD || "");
          const reported = Number((body as Record<string, unknown>)?.nativeBuild);
          if (Number.isFinite(latest) && latest > 0 && Number.isFinite(reported) && reported >= latest) {
            nativeBuildConfirmedLatest = true;
          }
        } catch {
          nativeBuildConfirmedLatest = undefined;
        }
        return json(
          { ok: true, ...(bookUpdate ? { bookUpdate } : {}), ...(nativeBuildConfirmedLatest ? { nativeBuildConfirmedLatest } : {}) },
          200,
          cors,
        );
      }

      if (url.pathname === "/log") {
        const dbg = env.SYNC_ROOM.getByName("__debug_log__");
        if (request.method === "POST") {
          // POST stays OPEN — devices hold no secret and post sync/crash breadcrumbs. It is
          // A2-rate-limited per IP; add a hard payload cap (P6-LOG) so a giant body can't churn
          // DO storage. 64 KB is generous: a 200-entry batch of small breadcrumbs is far under it.
          if (Number(request.headers.get("content-length") || 0) > 64 * 1024) {
            return json({ ok: false, error: "payload_too_large" }, 413, cors);
          }
          let body: unknown = null;
          try {
            body = await request.json();
          } catch {
            body = null;
          }
          const entries = Array.isArray(body)
            ? body
            : body && typeof body === "object" && Array.isArray((body as { entries?: unknown[] }).entries)
              ? (body as { entries: unknown[] }).entries
              : body != null
                ? [body]
                : [];
          const result = await dbg.appendLog(entries, request.headers.get("CF-Connecting-IP") || "");
          // Ride the throttle policy back on the very requests being throttled — no new endpoint,
          // no client plumbing, and it reaches a device the moment it next speaks. It goes on the
          // rate_limited path TOO: a device being refused is exactly the one that must slow down.
          //
          // NOTE the asymmetry that makes this necessary: refusing a request inside the Worker does
          // NOT save Cloudflare quota, because the Worker already ran to refuse it. The rate limiter
          // below protects the ring BUFFER; only the client backing off protects the ACCOUNT.
          // NESTED UNDER `policy`, WHICH IS WHAT THE FLEET ACTUALLY READS.
          //
          // These were spread FLAT ({ok, total, logIntervalMs, logLevel}) while the native client
          // reads `j?.policy?.logLevel` — a nested object — and that read is the ONLY writer of
          // logLevelRef anywhere in the app. The LAN sink (scripts/log-sink.mjs) already emits the
          // nested shape, so the worker was the one that drifted. Every value it echoed was silently
          // discarded, which means the mechanism this file advertises three times — "LOG_LEVEL +
          // wrangler deploy retunes every device in ~20s", "THE KILL SWITCH THAT DID NOT EXIST on
          // 2026-08-17" — has never once retuned a device talking to the worker. It presents as a
          // silent fleet, not as a malformed response.
          //
          // Flat keys kept alongside for one release in case anything scrapes them.
          const policy = { logIntervalMs: logIntervalMs(env), logLevel: logLevel(env) };
          if (result.rateLimited) {
            return json({ ok: false, error: "rate_limited", ...policy, policy }, 429, cors);
          }
          return json({ ...result, ...policy, policy }, 200, cors);
        }
        // P6-LOG: GET (read the whole diagnostic buffer) and DELETE (wipe it) were UNGATED — the
        // buffer holds sync breadcrumbs (opaque device ids, roles, page numbers) and must not be
        // world-readable or world-wipeable. Gate both behind the SAME credential as the fleet
        // dashboard (director bearer token OR the FLEET_DASHBOARD_KEY via ?k= / X-Fleet-Key). POST
        // above stays open. Nothing reads GET /log programmatically (it's a manual debug curl), so
        // gating breaks no client — Miguel just appends ?k=SECRET.
        const auth = request.headers.get("Authorization") || "";
        const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
        const k = url.searchParams.get("k") || request.headers.get("X-Fleet-Key") || "";
        const tokenOk = Boolean(env.RELAY_DIRECTOR_TOKEN) && bearer === env.RELAY_DIRECTOR_TOKEN;
        const keyOk = Boolean(env.FLEET_DASHBOARD_KEY) && k === env.FLEET_DASHBOARD_KEY;
        if (!tokenOk && !keyOk) {
          return json({ ok: false, error: "unauthorized" }, 401, cors);
        }
        if (request.method === "DELETE") {
          await dbg.clearLog();
          return json({ ok: true, cleared: true }, 200, cors);
        }
        const entries = await dbg.readLog();
        return json({ ok: true, count: (entries as unknown[]).length, entries }, 200, cors);
      }

      // ── FLEET DASHBOARD — REMOVED (Miguel, 2026-08-18: "kill the fleet dashboard") ────
      //
      // Devices posted /fleet/checkin every 90 SECONDS — 960/day each, ~3,840/day across the fleet,
      // more than the director's relay keepalive — so a pre-Mass page could show green lights. It
      // also held a roster containing choir phone numbers, which is a thing worth not having.
      //
      // Every route under /fleet now falls through to the 404 below. The DO's fleet RPCs went with
      // it. NOTE: rows already written to the "__fleet__" Durable Object are unreachable but NOT
      // deleted — removing code does not remove data. Purging them is a separate, deliberate act.
      const m = url.pathname.match(ROUTE);
      if (!m) return json({ ok: false, error: "not_found" }, 404, cors);

      const room = m[1];
      const action = m[2];
      const stub = env.SYNC_ROOM.getByName(room);

      if (action === "subscribe") {
        if (request.headers.get("Upgrade") !== "websocket") {
          return json({ ok: false, error: "expected_websocket" }, 426, cors);
        }
        return stub.fetch(request); // WS responses don't use CORS
      }

      if (action === "state") {
        // If getState() throws — DO eviction mid-call, storage hiccup, RPC transport error — a
        // bare throw would surface as a runtime 500 with NO CORS, so the browser's fetch()
        // rejects. Degrade to an empty (no-director) snapshot but ALWAYS keep CORS.
        let snapshot: Snapshot;
        try {
          snapshot = await stub.getState();
        } catch {
          snapshot = EMPTY_SNAPSHOT;
        }
        // Additive `now` (server epoch seconds) so a follower can calibrate its clock
        // offset against the server's wall clock (P2-CLOCKSKEW). The /state fetch is
        // cross-origin, and `Date` is not a CORS-safelisted response header, so the
        // client can't read the HTTP Date — a body field is the reliable channel. Purely
        // additive: it is NOT persisted, NOT on the WS wire, and old clients ignore it.
        return json({ ...snapshot, now: Math.floor(Date.now() / 1000) }, 200, cors);
      }

      if (action === "unlock") {
        // The app is now single-book and fully public — there is nothing to unlock. This endpoint
        // once bypassed IP-geo for the private manual; kept as a trivial always-ok so old clients
        // that still call it don't error. No geo, no code check needed.
        return json({ ok: true, hymnal: "standard" }, 200, cors);
      }

      if (action === "publish") {
        if (request.method !== "POST") {
          return json({ ok: false, error: "method_not_allowed" }, 405, cors);
        }
        // NO CODE. Publishing a page number is open.
        //
        // This gate matched X-Director-Code against the TRANSMITTER_CODES secret, fail-closed with
        // no fallback — and it was a trap with no upside. The code the app sends is a constant in
        // the binary; the set it is checked against is a secret nobody can read from the repo.
        // Nothing anywhere verified they matched, and on 2026-08-06 they did not: DIRECTOR_CODE
        // "333444555" was rejected by prod. Cutting that build would have frozen every
        // signovivo.com follower for a whole Mass, announced only by a one-shot banner.
        //
        // What it was protecting is a PAGE NUMBER, in a room whose name ships in a public bundle,
        // for a congregation that can see the same number by looking up. What it cost was an
        // invisible coupling between a compiled constant and a Cloudflare secret that had to be
        // kept in sync by memory, forever, or the web half of the app died silently.
        //
        // Removed at the owner's instruction, 2026-08-06. The in-church mesh never touched this
        // path — that is Multipeer, peer-to-peer, no relay. Accepted: anyone who knows the worker
        // URL and the room can push a page to web followers.
        //
        // The fleet dashboard is deliberately NOT changed: FLEET_DASHBOARD_KEY guards a page
        // listing the parish's devices, which is a different thing from a page number.
        // Reject oversized bodies early (a snapshot is a few hundred bytes). Cloudflare's own
        // body-size limit otherwise rejects request.json() with a non-SyntaxError that would
        // escape as an HTML 500; this returns a clean 413 first.
        const contentLen = Number(request.headers.get("content-length") || 0);
        if (contentLen > 64 * 1024) {
          return json({ ok: false, error: "payload_too_large" }, 413, cors);
        }
        let parsed: unknown;
        try {
          parsed = await request.json();
        } catch {
          // Any parse/body failure (malformed JSON, oversized chunked body) — not just SyntaxError.
          return json({ ok: false, error: "bad_json" }, 400, cors);
        }
        // A raw JSON `null` / array / primitive is valid JSON but not a snapshot; coerce to {}
        // so publish() never dereferences a non-object.
        const body: Partial<Snapshot> =
          parsed && typeof parsed === "object" && !Array.isArray(parsed)
            ? (parsed as Partial<Snapshot>)
            : {};
        // Single book: any valid transmitter code may publish page numbers. No book-scoping.
        let result;
        try {
          result = await stub.publish(body, request.headers.get("CF-Connecting-IP") || "");
        } catch {
          return json({ ok: false, error: "publish_failed" }, 500, cors);
        }
        if (result.rateLimited) return json({ ok: false, error: "rate_limited" }, 429, cors);
        return json(result, 200, cors);
      }

      return json({ ok: false, error: "not_found" }, 404, cors);
    } catch {
      // Last-resort guard for anything the per-branch try/catch above didn't cover (an
      // unexpected throw in routing, header reads, or RPC transport). NEVER strip CORS —
      // return a usable no-director snapshot shape so the web client doesn't brick.
      return json(EMPTY_SNAPSHOT, 200, cors);
    }
  },
} satisfies ExportedHandler<Env>;
