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
   *  Defaults to "12345678840" if unset. Gates who may publish (page numbers only). */
  TRANSMITTER_CODES?: string;
}

const PROTOCOL_VERSION = 1;

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
    const incomingSeq = Number(input.seq ?? 0);
    // Ignore out-of-order / duplicate / replayed packets so the song can't rewind.
    if (incomingSeq > 0 && incomingSeq <= this.snapshot.seq) {
      return { ok: true, seq: this.snapshot.seq, ignored: true };
    }
    const next: Snapshot = {
      v: PROTOCOL_VERSION,
      page: Math.max(1, Number(input.page ?? this.snapshot.page) || 1),
      totalPages: Math.max(0, Number(input.totalPages ?? this.snapshot.totalPages) || 0),
      mode: String(input.mode ?? this.snapshot.mode ?? ""),
      bookId: String(input.bookId ?? this.snapshot.bookId ?? ""),
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

const ROUTE = /^\/r\/([A-Za-z0-9_-]{1,64})\/(subscribe|publish|state)$/;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const cors = corsHeaders(origin, env);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({ ok: true, service: "signovivo-sync", v: PROTOCOL_VERSION }, 200, cors);
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
      const snapshot = await stub.getState();
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
        (env.TRANSMITTER_CODES || "12345678840").split(",").map((c) => c.trim()).filter(Boolean),
      );
      const tokenOk = Boolean(env.RELAY_DIRECTOR_TOKEN) && token === env.RELAY_DIRECTOR_TOKEN;
      const codeOk = code.length > 0 && validCodes.has(code);
      if (!tokenOk && !codeOk) {
        return json({ ok: false, error: "unauthorized" }, 401, cors);
      }
      let body: Partial<Snapshot>;
      try {
        body = (await request.json()) as Partial<Snapshot>;
      } catch {
        return json({ ok: false, error: "bad_json" }, 400, cors);
      }
      const result = await stub.publish(body);
      return json(result, 200, cors);
    }

    return json({ ok: false, error: "not_found" }, 404, cors);
  },
} satisfies ExportedHandler<Env>;
