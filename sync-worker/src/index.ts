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
import { foldLogEntries, LOG_RATE_BURST, LOG_RATE_PER_SEC } from "./logBuffer.js";

export interface Env {
  SYNC_ROOM: DurableObjectNamespace<SyncRoom>;
  /** Secret the director must send as `Authorization: Bearer <token>`. Set via `wrangler secret put`. */
  RELAY_DIRECTOR_TOKEN: string;
  /** Comma-separated allow-list for CORS on /state. "*" allows all (fine — no credentials). */
  ALLOWED_ORIGINS?: string;
  /** Secret gating the /fleet readiness dashboard + roster (which expose choir phone numbers). Set
   *  via `wrangler secret put FLEET_DASHBOARD_KEY`. This one STAYS: it guards a page listing the
   *  parish's devices and phone numbers, which is a different thing from a page number. */
  FLEET_DASHBOARD_KEY?: string;

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

// ── Fleet readiness (device pre-Mass check-in) ───────────────────────────────
// A device self-reports (personId/label + role + native build and/or web-cache status) so the
// director can see, before Mass, which iPads are ready and who to poke. Kept in ONE fixed DO
// instance ("__fleet__"), separate from any sync room. NO phone numbers ever come from a device —
// devices send only a self-entered label; phones live server-side in the roster (seeded once,
// gated dashboard only) so the PUBLIC repo never carries anyone's number.
type FleetDevice = {
  deviceId: string; // stable per-install id (localStorage / AsyncStorage uuid)
  label: string; // self-entered name ("Rita")
  role: string; // Cantor | Cantor/Guitarrista | Bajo/Guitarrón | Director
  surface: "web" | "native";
  /** "PAD" | "PHN" — native only, resolved by the OS (Platform.isPad), never guessed from a UA. */
  deviceKind?: string;
  nativeBuild?: number; // native only
  webCached?: boolean; // web only — offline bundle fully cached
  pagesCached?: number; // web only
  totalPages?: number;
  homeScreen?: boolean; // web only — added to Home Screen (iOS keeps the cache past 7 days)
  cacheVersion?: string;
  // Which BOOK this device holds — distinct from nativeBuild, which is the SHELL's number and can
  // read "current" over a songbook months old (defect D1). This is the only field that answers
  // "did the update actually arrive?"
  bookVersion?: string;
  bundleSource?: string; // "baked" | "documents" — where the mounted bundle came from
  bookStage?: string; // "active" | "downloading:37%" | "ready" | "error:disk" …
  ts: number; // last check-in (unix seconds)
};

type RosterPerson = {
  id: string;
  name: string;
  role: string;
  phones: string[]; // server-side ONLY — never leaves the gated dashboard
  director?: boolean;
};

// ────────────────────────────── Durable Object ──────────────────────────────

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
    if (!Number.isFinite(incomingSeq) || incomingSeq < 0 || incomingSeq > Date.now() + 60000) {
      incomingSeq = 0;
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
  async clearLog(): Promise<{ ok: true }> {
    await this.ctx.storage.put("dbglog", []);
    return { ok: true };
  }

  // ── Fleet readiness RPCs (all on the fixed "__fleet__" instance) ──

  /** RPC: seed/replace the roster (names + phones + roles). Gated at the worker layer. Phones are
   *  stored ONLY here — the public repo and device pings never carry them. Sanitized + capped. */
  async putRoster(people: unknown[]): Promise<{ ok: true; count: number }> {
    const clean: RosterPerson[] = (Array.isArray(people) ? people : [])
      .slice(0, 200)
      .map((p) => {
        const o = (p && typeof p === "object" ? p : {}) as Record<string, unknown>;
        return {
          id: String(o.id ?? "").slice(0, 64),
          name: String(o.name ?? "").slice(0, 80),
          role: String(o.role ?? "").slice(0, 60),
          phones: (Array.isArray(o.phones) ? o.phones : [])
            .slice(0, 6)
            .map((x) => String(x).slice(0, 40)),
          director: Boolean(o.director),
        };
      })
      .filter((p) => p.id || p.name);
    await this.ctx.storage.put("roster", clean);
    return { ok: true, count: clean.length };
  }

  /** RPC: a device reports its readiness. OPEN at the worker layer (devices hold no secret), so
   *  every field is sanitized + length/range-capped here the same way publish() guards its input. */
  async checkin(input: unknown, ip = ""): Promise<{ ok: true; total: number; rateLimited?: boolean }> {
    // A2: cap check-in spam per IP so an attacker can't churn/evict real device readiness entries.
    if (this.rateLimited(ip, 10, 1)) {
      return { ok: true, total: 0, rateLimited: true };
    }
    const o = (input && typeof input === "object" ? input : {}) as Record<string, unknown>;
    const deviceId = String(o.deviceId ?? "").slice(0, 64);
    if (!deviceId) return { ok: true, total: 0 }; // can't key a device with no id — drop it
    const devices =
      (await this.ctx.storage.get<Record<string, FleetDevice>>("fleet_devices")) ?? {};
    const prev = devices[deviceId] || ({} as Partial<FleetDevice>);
    const surface: "web" | "native" =
      o.surface === "native" ? "native" : o.surface === "web" ? "web" : prev.surface || "web";
    const entry: FleetDevice = {
      ...prev,
      deviceId,
      label: String(o.label ?? prev.label ?? "").slice(0, 60),
      role: String(o.role ?? prev.role ?? "").slice(0, 60),
      surface,
      ts: Math.floor(Date.now() / 1000),
    };
    if (o.nativeBuild != null)
      entry.nativeBuild = Math.max(0, Math.min(Number(o.nativeBuild) || 0, 1000000));
    if (o.webCached != null) entry.webCached = Boolean(o.webCached);
    if (o.pagesCached != null)
      entry.pagesCached = Math.max(0, Math.min(Number(o.pagesCached) || 0, 100000));
    if (o.totalPages != null)
      entry.totalPages = Math.max(0, Math.min(Number(o.totalPages) || 0, 100000));
    if (o.homeScreen != null) entry.homeScreen = Boolean(o.homeScreen);
    if (o.cacheVersion != null) entry.cacheVersion = String(o.cacheVersion).slice(0, 40);
    // WHICH KIND of native device. `surface` separates native from web; this separates two NATIVE
    // devices, which nothing here could do before — deviceId is an opaque 6-char random, so an iPad
    // on an old build was read as the owner's iPhone for an hour on 2026-08-05 while diagnosing a
    // live rollout. Whitelisted rather than clamped: only the two values the client can send are
    // stored, so a malformed or hostile body cannot put arbitrary text on the dashboard.
    if (o.deviceKind === "PAD" || o.deviceKind === "PHN") entry.deviceKind = o.deviceKind;
    // Book identity, following the existing clamp idiom exactly. WITHOUT THESE the rollout's
    // "prove it on ONE iPad first" step has nothing to observe: a downloader failing silently on
    // seven iPads looks identical to one that succeeded. Additive by construction — an older
    // client simply omits them and the dashboard renders "versión desconocida".
    if (o.bookVersion != null) entry.bookVersion = String(o.bookVersion).slice(0, 32);
    if (o.bundleSource != null) entry.bundleSource = String(o.bundleSource).slice(0, 16);
    if (o.bookStage != null) entry.bookStage = String(o.bookStage).slice(0, 40);
    devices[deviceId] = entry;
    // Ring-cap: keep the 300 most-recently-seen devices so storage can't grow unbounded.
    const kept = Object.values(devices)
      .sort((a, b) => b.ts - a.ts)
      .slice(0, 300);
    const pruned: Record<string, FleetDevice> = {};
    for (const d of kept) pruned[d.deviceId] = d;
    await this.ctx.storage.put("fleet_devices", pruned);
    return { ok: true, total: kept.length };
  }

  /** RPC: roster (with phones) + all device check-ins, newest first. Gated at the worker layer. */
  async getFleet(): Promise<{ roster: RosterPerson[]; devices: FleetDevice[] }> {
    const roster = (await this.ctx.storage.get<RosterPerson[]>("roster")) ?? [];
    const devices =
      (await this.ctx.storage.get<Record<string, FleetDevice>>("fleet_devices")) ?? {};
    return { roster, devices: Object.values(devices).sort((a, b) => b.ts - a.ts) };
  }

  /** RPC: wipe device check-ins (roster kept). For testing / a fresh season. */
  async resetFleet(): Promise<{ ok: true }> {
    await this.ctx.storage.put("fleet_devices", {});
    return { ok: true };
  }

  /** WebSocket subscribe — must go through fetch() for the upgrade. */
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

// Only digits survive into a tel: href (defensive — the value is server-side roster data).
const telHref = (phone: string): string => "tel:" + String(phone).replace(/[^0-9+]/g, "");

function renderFleetDashboard(
  data: { roster: RosterPerson[]; devices: FleetDevice[] },
  k: string,
  crashes: Array<Record<string, unknown>> = [],
): string {
  const roster = Array.isArray(data.roster) ? data.roster : [];
  const devices = Array.isArray(data.devices) ? data.devices : [];
  const nowSec = Math.floor(Date.now() / 1000);
  const maxBuild = devices.reduce((m, d) => Math.max(m, Number(d.nativeBuild) || 0), 0);
  const latest = Math.max(maxBuild, MIN_SYNC_BUILD);

  const ago = (ts: number): string => {
    const s = Math.max(0, nowSec - (Number(ts) || 0));
    if (s < 90) return "ahora";
    if (s < 3600) return `${Math.round(s / 60)} min`;
    if (s < 86400) return `${Math.round(s / 3600)} h`;
    return `${Math.round(s / 86400)} d`;
  };
  const devicesFor = (name: string): FleetDevice[] => {
    const n = normName(name);
    return n.length === 0 ? [] : devices.filter((d) => normName(d.label) === n);
  };

  type Row = { cls: "ok" | "warn" | "bad"; sort: number; html: string };
  const rows: Row[] = roster.map((p) => {
    const ds = devicesFor(p.name);
    const bestBuild = ds.reduce((m, d) => Math.max(m, Number(d.nativeBuild) || 0), 0);
    const webReady = ds.some((d) => d.webCached && d.homeScreen);
    const webNoHome = ds.some((d) => d.webCached && !d.homeScreen);
    const webPartial = ds.some((d) => !d.webCached && (Number(d.pagesCached) || 0) > 0);
    const lastTs = ds.reduce((m, d) => Math.max(m, Number(d.ts) || 0), 0);

    let cls: "ok" | "warn" | "bad" = "bad";
    let action = "Onboarding pendiente";
    if (ds.length === 0) {
      cls = "bad";
      action = "No se ha visto — invitar";
    } else if (p.director) {
      if (bestBuild >= latest) { cls = "ok"; action = `Listo — build ${bestBuild}`; }
      else { cls = "bad"; action = `⚠ Director en build ${bestBuild || "—"} (debe ser ${latest})`; }
    } else if (bestBuild >= MIN_SYNC_BUILD) {
      cls = "ok"; action = `Listo — app ${bestBuild}`;
    } else if (webReady) {
      cls = "ok"; action = "Listo — web en caché";
    } else if (webNoHome) {
      cls = "warn"; action = "Agregar a inicio";
    } else if (webPartial) {
      cls = "warn"; action = "Reabrir para cachear";
    } else if (bestBuild > 0) {
      cls = "bad"; action = `Actualizar app (${bestBuild} &lt; ${MIN_SYNC_BUILD})`;
    }
    const sort = (cls === "bad" ? 0 : cls === "warn" ? 1 : 2) - (p.director ? 0.5 : 0);
    const phoneHtml = (p.phones || [])
      .map((ph) => `<a href="${escHtml(telHref(ph))}">${escHtml(ph)}</a>`)
      .join(" ") || '<span class="muted">—</span>';
    const nativeCell = bestBuild > 0 ? String(bestBuild) : '<span class="muted">—</span>';
    // LIBRO — which SONGBOOK this person's device holds. The App column is the SHELL's build
    // number and can read "current" over a book months old (defect D1), so it cannot answer
    // "did the update arrive?". Amber "versión desconocida" for a client that omits the field,
    // which is every client older than this build.
    const bookDev = ds.find((d) => d.bookVersion) || null;
    const stage = String(bookDev?.bookStage || "");
    const libroCell = !bookDev
      ? '<span class="muted">versión desconocida</span>'
      : stage.startsWith("downloading") || stage === "ready"
        ? `<span class="tag">${escHtml(stage)}</span> ${escHtml(String(bookDev.bookVersion).slice(3, 11))}`
        : `${escHtml(String(bookDev.bookVersion).slice(3, 11))}${bookDev.bundleSource === "documents" ? ' <span class="muted">(descargado)</span>' : ""}`;
    const webCell = webReady
      ? "✓ inicio"
      : webNoHome
        ? "caché, sin inicio"
        : webPartial
          ? "parcial"
          : '<span class="muted">—</span>';
    const html = `<tr class="${cls}">
      <td>${escHtml(p.name)}${p.director ? ' <span class="tag">Director</span>' : ""}</td>
      <td class="muted">${escHtml(p.role)}</td>
      <td>${nativeCell}</td>
      <td>${libroCell}</td>
      <td>${webCell}</td>
      <td>${ds.length ? ago(lastTs) : '<span class="muted">nunca</span>'}</td>
      <td>${phoneHtml}</td>
      <td class="act">${action}</td>
    </tr>`;
    return { cls, sort, html };
  });
  rows.sort((a, b) => a.sort - b.sort);

  const ready = rows.filter((r) => r.cls === "ok").length;
  const warn = rows.filter((r) => r.cls === "warn").length;
  const bad = rows.filter((r) => r.cls === "bad").length;

  // Devices whose typed label matched no roster person (typo / guest) — surfaced so nobody is lost.
  const rosterNames = new Set(roster.map((p) => normName(p.name)));
  const orphans = devices.filter((d) => !rosterNames.has(normName(d.label)));
  const orphanHtml = orphans.length
    ? `<h3>Sin coincidencia en la lista (${orphans.length})</h3><table><thead><tr><th>Etiqueta</th><th>Rol</th><th>Origen</th><th>App</th><th>Web</th><th>Visto</th></tr></thead><tbody>${orphans
        .map(
          (d) =>
            `<tr><td>${escHtml(d.label) || '<span class="muted">(sin nombre)</span>'}</td><td class="muted">${escHtml(d.role)}</td><td>${escHtml(d.surface)}${d.deviceKind ? ` <b>${escHtml(d.deviceKind)}</b>` : ""}</td><td>${d.nativeBuild ? escHtml(d.nativeBuild) : '<span class="muted">—</span>'}</td><td>${d.webCached ? (d.homeScreen ? "✓ inicio" : "caché") : '<span class="muted">—</span>'}</td><td>${ago(Number(d.ts) || 0)}</td></tr>`,
        )
        .join("")}</tbody></table>`
    : "";

  // Recent crashes panel (M2 Slice D) — "device X crashed at 12:05 on build Y" at a glance.
  // Newest first, capped. `rx` (server receive epoch-seconds, stamped by appendLog) is used for
  // the timestamp so a skewed device clock can't misplace it; falls back to the client `t` (ms).
  const crashList = (Array.isArray(crashes) ? crashes : [])
    .filter((c) => c && typeof c === "object")
    .sort((a, b) => (Number(b.rx) || Number(b.t) / 1000 || 0) - (Number(a.rx) || Number(a.t) / 1000 || 0))
    .slice(0, 25);
  const crashHtml = crashList.length
    ? `<h3>Fallos recientes (${crashList.length})</h3><table><thead><tr><th>Cuándo</th><th>Dispositivo</th><th>Origen</th><th>Build</th><th>Dónde</th><th>Mensaje</th></tr></thead><tbody>${crashList
        .map((c) => {
          const whenSec = Number(c.rx) || Math.floor(Number(c.t) / 1000) || 0;
          return `<tr class="bad"><td>${ago(whenSec)}</td><td class="muted">${escHtml(c.dev) || "—"}</td><td>${escHtml(c.surface) || "web"}</td><td>${escHtml(c.build) || '<span class="muted">—</span>'}</td><td>${escHtml(c.where) || "—"}</td><td class="muted">${escHtml(c.msg) || "—"}</td></tr>`;
        })
        .join("")}</tbody></table>`
    : "";

  const kq = k ? `?k=${encodeURIComponent(k)}` : "";
  return `<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="20${kq ? `;url=/fleet-dashboard${kq}` : ""}">
<title>Signo Vivo — estado del coro</title>
<style>
:root{color-scheme:dark}
body{margin:0;background:#0f1216;color:#e8edf2;font:15px/1.5 -apple-system,system-ui,sans-serif;padding:16px}
h1{font-size:20px;font-weight:600;margin:0 0 2px}
.sub{color:#8b96a3;font-size:13px;margin-bottom:14px}
.chips{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.chip{padding:6px 12px;border-radius:999px;font-size:13px;font-weight:600}
.chip.ok{background:#12351f;color:#5fd48a}.chip.warn{background:#3a2f10;color:#e0b64a}.chip.bad{background:#3a1518;color:#f0788a}
table{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:18px}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #1e252d;vertical-align:top}
th{color:#8b96a3;font-weight:600}
tr.bad td{background:rgba(240,120,138,.06)}tr.warn td{background:rgba(224,182,74,.06)}
td.act{font-weight:600}
tr.ok td.act{color:#5fd48a}tr.warn td.act{color:#e0b64a}tr.bad td.act{color:#f0788a}
.muted{color:#5b6673}.tag{background:#243b57;color:#8fc0f0;padding:1px 6px;border-radius:4px;font-size:11px}
a{color:#7fb3f0;text-decoration:none}
h3{font-size:14px;color:#8b96a3;margin:18px 0 6px}
</style></head><body>
<h1>Estado del coro — Signo Vivo</h1>
<div class="sub">Actualiza cada 20 s · el iPad reporta cuando tiene internet · orden: pendientes primero</div>
<div class="chips">
<span class="chip ok">${ready} listos</span>
<span class="chip warn">${warn} por revisar</span>
<span class="chip bad">${bad} por contactar</span>
</div>
<table><thead><tr><th>Persona</th><th>Rol</th><th>App</th><th>Libro</th><th>signovivo.com</th><th>Visto</th><th>Teléfono</th><th>Qué hacer</th></tr></thead>
<tbody>${rows.map((r) => r.html).join("") || '<tr><td colspan="7" class="muted">Sin lista cargada todavía.</td></tr>'}</tbody></table>
${orphanHtml}
${crashHtml}
<div class="sub">Listo = app ≥ ${MIN_SYNC_BUILD} · o signovivo.com en caché + agregado a inicio (iOS conserva la caché). Director debe estar en ${latest}.</div>
</body></html>`;
}

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
          if (result.rateLimited) return json({ ok: false, error: "rate_limited" }, 429, cors);
          return json(result, 200, cors);
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

      // Fleet readiness — device pre-Mass check-in + gated director dashboard. Phones live ONLY in
      // the fixed "__fleet__" DO + this dashboard; a device only ever POSTs a self-entered label.
      if (
        url.pathname === "/fleet" ||
        url.pathname === "/fleet.json" ||
        url.pathname === "/fleet-dashboard" ||
        url.pathname.startsWith("/fleet/")
      ) {
        const fleet = env.SYNC_ROOM.getByName("__fleet__");

        // Device check-in is OPEN (devices hold no secret). checkin() sanitizes + caps every field.
        if (url.pathname === "/fleet/checkin") {
          if (request.method !== "POST") {
            return json({ ok: false, error: "method_not_allowed" }, 405, cors);
          }
          if (Number(request.headers.get("content-length") || 0) > 16 * 1024) {
            return json({ ok: false, error: "payload_too_large" }, 413, cors);
          }
          let body: unknown = null;
          try {
            body = await request.json();
          } catch {
            body = null;
          }
          const result = await fleet.checkin(body, request.headers.get("CF-Connecting-IP") || "");
          if (result.rateLimited) return json({ ok: false, error: "rate_limited" }, 429, cors);

          // Book-update pointer, riding the response to a call the client ALREADY makes on
          // foreground and every 90 s. No new endpoint, no new poll, and — critically — a failed
          // check is a structural no-op, which is the only correct behaviour in a building with no
          // internet. See sync-worker/src/bookArming.js for why arming takes two independent vars.
          let bookUpdate: { bookVersion: string; base: string } | null = null;
          try {
            const deviceId = String((body as Record<string, unknown>)?.deviceId ?? "").slice(0, 64);
            if (deviceId && String(env.BOOK_UPDATE_VERSION || "").trim()) {
              const { devices } = await fleet.getFleet();
              const me = devices.find((d) => d.deviceId === deviceId) || { deviceId };
              bookUpdate = decideBookUpdate(env, me, devices, Math.floor(Date.now() / 1000));
            }
          } catch {
            // A failure to compute the pointer must never break presence reporting.
            bookUpdate = null;
          }
          return json(bookUpdate ? { ...result, bookUpdate } : result, 200, cors);
        }

        // Everything else under /fleet exposes choir phone numbers, so it is gated by the director
        // bearer token OR a DEDICATED SECRET (?k=SECRET for a browser, X-Fleet-Key for a script).
        // NOT the transmitter codes — those are hardcoded in this public repo, so gating PII behind
        // them would expose every number to anyone reading the source.
        const auth = request.headers.get("Authorization") || "";
        const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
        const k = url.searchParams.get("k") || request.headers.get("X-Fleet-Key") || "";
        const tokenOk = Boolean(env.RELAY_DIRECTOR_TOKEN) && bearer === env.RELAY_DIRECTOR_TOKEN;
        const keyOk = Boolean(env.FLEET_DASHBOARD_KEY) && k === env.FLEET_DASHBOARD_KEY;
        const isDash = url.pathname === "/fleet-dashboard" || url.pathname === "/fleet/dashboard";
        if (!tokenOk && !keyOk) {
          if (isDash) {
            return new Response(
              `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><body style="background:#0f1216;color:#e8edf2;font:16px system-ui;padding:24px"><h2>Estado del coro</h2><p>Agrega tu c&oacute;digo al final del enlace:</p><p><code>/fleet-dashboard?k=TU_C&Oacute;DIGO</code></p></body>`,
              { status: 401, headers: { "Content-Type": "text/html; charset=utf-8", ...cors } },
            );
          }
          return json({ ok: false, error: "unauthorized" }, 401, cors);
        }

        if (url.pathname === "/fleet/roster" && request.method === "POST") {
          if (Number(request.headers.get("content-length") || 0) > 128 * 1024) {
            return json({ ok: false, error: "payload_too_large" }, 413, cors);
          }
          let body: unknown = null;
          try {
            body = await request.json();
          } catch {
            body = null;
          }
          const people = Array.isArray(body)
            ? body
            : body && typeof body === "object"
              ? (body as { people?: unknown[] }).people
              : [];
          return json(await fleet.putRoster(Array.isArray(people) ? people : []), 200, cors);
        }
        if (url.pathname === "/fleet/reset" && request.method === "POST") {
          return json(await fleet.resetFleet(), 200, cors);
        }
        if (isDash) {
          const data = await fleet.getFleet();
          // Recent crashes (M2 Slice D): read the gated debug buffer and surface kind:"crash"
          // entries on the dashboard. Best-effort — a debug-log hiccup must not break the roster.
          let crashes: Array<Record<string, unknown>> = [];
          try {
            const log = await env.SYNC_ROOM.getByName("__debug_log__").readLog();
            crashes = (Array.isArray(log) ? log : [])
              .filter((e) => Boolean(e) && typeof e === "object" && (e as { kind?: unknown }).kind === "crash")
              .map((e) => e as Record<string, unknown>);
          } catch {
            crashes = [];
          }
          return new Response(renderFleetDashboard(data, k, crashes), {
            status: 200,
            headers: { "Content-Type": "text/html; charset=utf-8", ...cors },
          });
        }
        if (url.pathname === "/fleet" || url.pathname === "/fleet.json") {
          return json(await fleet.getFleet(), 200, cors);
        }
        return json({ ok: false, error: "not_found" }, 404, cors);
      }

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
