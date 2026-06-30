/// <reference types="@cloudflare/workers-types" />
//
// SignoVivo Sync — featherweight relay (Path D).
// Director POSTs the current page; every signovivo.com browser gets it pushed.
// One Durable Object instance per "room" (one church = one room: "alvernia-main").
//
// Endpoints (see README.md):
//   GET  /r/:room/subscribe  (WebSocket)  — followers; receive current snapshot + every update
//   GET  /r/:room/state                   — followers; poll fallback / initial paint (tiny JSON)
//   POST /r/:room/publish    (auth)       — director; set the current page, fan out to subscribers
//
import { DurableObject } from "cloudflare:workers";

export interface Env {
  SYNC_ROOM: DurableObjectNamespace<SyncRoom>;
  /** Secret the director must send as `Authorization: Bearer <token>`. Set via `wrangler secret put`. */
  RELAY_DIRECTOR_TOKEN: string;
  /** Comma-separated allow-list for CORS on /state. "*" allows all (fine — no credentials). */
  ALLOWED_ORIGINS?: string;
  /** Comma-separated transmitter access codes accepted via the `X-Director-Code` header.
   *  Defaults to the legacy transmitter code plus the four director codes if unset.
   *  Gates who may publish (page numbers only). The legacy "12345678840" stays until
   *  native build 332 is confirmed live on TestFlight, then it is removed (Phase 7). */
  TRANSMITTER_CODES?: string;
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

// ────────────────────────────── Durable Object ──────────────────────────────

export class SyncRoom extends DurableObject<Env> {
  private snapshot: Snapshot = EMPTY_SNAPSHOT;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Restore the latest page on wake (covers hibernation / eviction).
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<Snapshot>("snapshot");
      if (stored) this.snapshot = stored;
    });
  }

  /** RPC: director publishes a new page state. Latest-wins, stale-guarded. */
  async publish(input: Partial<Snapshot>): Promise<{ ok: true; seq: number; ignored?: boolean }> {
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
    "Access-Control-Allow-Headers": "Authorization,Content-Type,X-Director-Code",
    // Expose X-Hymnal so browser fetch() can READ it cross-origin (otherwise it's hidden).
    "Access-Control-Expose-Headers": "X-Hymnal",
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

const ROUTE = /^\/r\/([A-Za-z0-9_-]{1,64})\/(subscribe|publish|state)$/;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const cors = corsHeaders(origin, env);

    // IP geolocation → which hymnal book the follower should default to.
    // The parish is Del Rio, TX + its cross-border sister city Ciudad Acuña, Coahuila
    // (our families straddle the border), so BOTH sides — plus the immediate surroundings
    // within a radius — get the standard manual (alvernia_manual_2). Everywhere else
    // defaults to hymns-4 ("Himnos de Sión"), which is PUBLIC. The standard manual is
    // PRIVATE to Del Rio / our church, so this gate stays tight to the border region
    // (radius is well short of Eagle Pass ≈80 km and San Antonio ≈234 km). IP geo is
    // approximate & ISP-dependent, so we OR several signals: physical radius (primary),
    // exact ZIP, city name, and the Acuña MX postal block. Sent on every response; the
    // web app reads X-Hymnal on its first /state fetch. Tune STANDARD_RADIUS_KM to taste.
    const postal = String((request.cf?.postalCode as string) || "");
    const cityLc = String((request.cf?.city as string) || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[̀-ͯ]/g, ""); // strip accents: "acuña" → "acuna"
    const country = String((request.cf?.country as string) || "").toUpperCase();
    const lat = Number(request.cf?.latitude ?? NaN);
    const lon = Number(request.cf?.longitude ?? NaN);

    // Haversine distance from Del Rio center; Ciudad Acuña's center is ≈7 km away, so a
    // single radius covers both sides of the border without reaching the next parishes.
    const DR_LAT = 29.3627;
    const DR_LON = -100.8968;
    const STANDARD_RADIUS_KM = 45;
    const kmFromDelRio = (la: number, lo: number): number => {
      const R = 6371;
      const toRad = (d: number) => (d * Math.PI) / 180;
      const dLat = toRad(la - DR_LAT);
      const dLon = toRad(lo - DR_LON);
      const a =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(DR_LAT)) * Math.cos(toRad(la)) * Math.sin(dLon / 2) ** 2;
      return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
    };
    const distKm =
      Number.isFinite(lat) && Number.isFinite(lon) ? kmFromDelRio(lat, lon) : Infinity;

    const isStandard =
      distKm <= STANDARD_RADIUS_KM || // primary: physical radius (both sides of the border)
      postal === "78840" ||
      postal === "78841" || // Del Rio, TX ZIPs (fallback if coords missing)
      cityLc === "del rio" ||
      cityLc === "ciudad acuna" ||
      cityLc === "acuna" || // Ciudad Acuña, MX
      (country === "MX" && /^262\d\d$/.test(postal)); // Acuña postal block 26200–26299
    cors["X-Hymnal"] = isStandard ? "standard" : "nonstandard";

    // Outer guard: by here `cors` (incl. X-Hymnal) is fully built off request geo and never
    // touches a Durable Object, so it's safe to emit no matter what the routing/DO logic does.
    // Any unexpected throw below (DO eviction, RPC transport error, runtime hiccup) must STILL
    // return CORS + X-Hymnal — otherwise the web client's fetch() rejects, geo never resolves,
    // and a fresh / geo-failed device bricks on the loader instead of falling to the Sión floor.
    try {
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: cors });
      }

      if (url.pathname === "/" || url.pathname === "/health") {
        // Echo the REQUESTER's own geo + the resulting book decision. Lets a device hit
        // this URL in a browser and see precisely why it got standard vs nonstandard
        // (diagnoses ISP geo-routing that lands a Del Rio device outside the radius).
        return json(
          {
            ok: true,
            service: "signovivo-sync",
            v: PROTOCOL_VERSION,
            geo: {
              city: String((request.cf?.city as string) || ""),
              region: String((request.cf?.region as string) || ""),
              postal,
              country,
              lat: Number.isFinite(lat) ? lat : null,
              lon: Number.isFinite(lon) ? lon : null,
              kmFromDelRio: Number.isFinite(distKm) ? Math.round(distKm * 10) / 10 : null,
              hymnal: cors["X-Hymnal"],
            },
          },
          200,
          cors,
        );
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
        // The web client's geo resolution lives ENTIRELY in this response's X-Hymnal header
        // (read off /state). If getState() throws — DO eviction mid-call, storage hiccup,
        // RPC transport error — a bare throw would surface as a runtime 500 with NO CORS and
        // NO X-Hymnal, so the browser's fetch() rejects, geo never resolves, and a fresh /
        // geo-failed device bricks on the loader. Degrade to an empty (no-director) snapshot
        // but ALWAYS keep CORS + X-Hymnal so geo still resolves and the Sión floor holds.
        let snapshot: Snapshot;
        try {
          snapshot = await stub.getState();
        } catch {
          snapshot = EMPTY_SNAPSHOT;
        }
        return json(snapshot, 200, cors);
      }

      if (action === "publish") {
        if (request.method !== "POST") {
          return json({ ok: false, error: "method_not_allowed" }, 405, cors);
        }
        // Authorized by EITHER the bearer token (scripts/testing) OR a valid
        // transmitter access code in X-Director-Code (the native app — matches the
        // memorable director codes already used in-app).
        const auth = request.headers.get("Authorization") || "";
        const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
        const code = (request.headers.get("X-Director-Code") || "").replace(/[^0-9]/g, "");
        const validCodes = new Set(
          (
            env.TRANSMITTER_CODES ||
            // legacy transmitter code + the four director codes (admin 8307343376 + 3 regular).
            // Legacy "12345678840" removed in Phase 7 once build 332 is confirmed on TestFlight.
            "12345678840,8307343376,8304533367,8307197000,8303130470"
          )
            .split(",")
            .map((c) => c.trim())
            .filter(Boolean),
        );
        const tokenOk = Boolean(env.RELAY_DIRECTOR_TOKEN) && token === env.RELAY_DIRECTOR_TOKEN;
        const codeOk = code.length > 0 && validCodes.has(code);
        if (!tokenOk && !codeOk) {
          return json({ ok: false, error: "unauthorized" }, 401, cors);
        }
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
        let result;
        try {
          result = await stub.publish(body);
        } catch {
          return json({ ok: false, error: "publish_failed" }, 500, cors);
        }
        return json(result, 200, cors);
      }

      return json({ ok: false, error: "not_found" }, 404, cors);
    } catch {
      // Last-resort guard for anything the per-branch try/catch above didn't cover (an
      // unexpected throw in routing, header reads, or RPC transport). NEVER strip CORS /
      // X-Hymnal — return a usable no-director snapshot shape so the web client's geo still
      // resolves off X-Hymnal and falls to the public Sión floor instead of bricking.
      return json(EMPTY_SNAPSHOT, 200, cors);
    }
  },
} satisfies ExportedHandler<Env>;
