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
  /** Secret gating the /fleet readiness dashboard + roster (which expose choir phone numbers). Set
   *  via `wrangler secret put FLEET_DASHBOARD_KEY`. Deliberately NOT a transmitter code — those are
   *  hardcoded in this PUBLIC repo, so gating PII behind them would expose every number. */
  FLEET_DASHBOARD_KEY?: string;
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
  nativeBuild?: number; // native only
  webCached?: boolean; // web only — offline bundle fully cached
  pagesCached?: number; // web only
  totalPages?: number;
  homeScreen?: boolean; // web only — added to Home Screen (iOS keeps the cache past 7 days)
  cacheVersion?: string;
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

  /** RPC: append diagnostic breadcrumbs to a capped ring buffer (debug telemetry only — no sync
   *  data). Devices POST their Multipeer sync lifecycle here so it can be read back remotely. */
  async appendLog(entries: unknown[]): Promise<{ ok: true; total: number }> {
    const existing = (await this.ctx.storage.get<unknown[]>("dbglog")) ?? [];
    const rx = Math.floor(Date.now() / 1000);
    const stamped = entries
      .slice(0, 200)
      .map((e) => (e && typeof e === "object" ? { rx, ...(e as object) } : { rx, v: e }));
    const next = [...existing, ...stamped].slice(-600); // ring buffer: keep the last 600 entries
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
  async checkin(input: unknown): Promise<{ ok: true; total: number }> {
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

// The valid transmitter / director access codes — gates BOTH /publish (directing) and /unlock
// (authorized access to the private standard manual). Single source of truth so the two paths
// never drift. Override via env.TRANSMITTER_CODES (comma-separated).
function validTransmitterCodes(env: Env): Set<string> {
  return new Set(
    (
      env.TRANSMITTER_CODES ||
      // legacy transmitter code + the four director codes (admin 8307343376 + 3 regular).
      "12345678840,8307343376,8304533367,8307197000,8303130470"
    )
      .split(",")
      .map((c) => c.trim())
      .filter(Boolean),
  );
}

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
            `<tr><td>${escHtml(d.label) || '<span class="muted">(sin nombre)</span>'}</td><td class="muted">${escHtml(d.role)}</td><td>${escHtml(d.surface)}</td><td>${d.nativeBuild ? escHtml(d.nativeBuild) : '<span class="muted">—</span>'}</td><td>${d.webCached ? (d.homeScreen ? "✓ inicio" : "caché") : '<span class="muted">—</span>'}</td><td>${ago(Number(d.ts) || 0)}</td></tr>`,
        )
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
<table><thead><tr><th>Persona</th><th>Rol</th><th>App</th><th>signovivo.com</th><th>Visto</th><th>Teléfono</th><th>Qué hacer</th></tr></thead>
<tbody>${rows.map((r) => r.html).join("") || '<tr><td colspan="7" class="muted">Sin lista cargada todavía.</td></tr>'}</tbody></table>
${orphanHtml}
<div class="sub">Listo = app ≥ ${MIN_SYNC_BUILD} · o signovivo.com en caché + agregado a inicio (iOS conserva la caché). Director debe estar en ${latest}.</div>
</body></html>`;
}

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

      // Diagnostic telemetry sink. Devices POST their Multipeer sync lifecycle (role chosen, peer
      // found, invite, connect/disconnect, page sent/received) here; GET reads the ring buffer back
      // so the whole director↔follower handshake can be inspected remotely. DELETE clears it.
      // A single fixed debug DO holds the buffer (separate from any sync room).
      if (url.pathname === "/log") {
        const dbg = env.SYNC_ROOM.getByName("__debug_log__");
        if (request.method === "POST") {
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
          const result = await dbg.appendLog(entries);
          return json(result, 200, cors);
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
          return json(await fleet.checkin(body), 200, cors);
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
          return new Response(renderFleetDashboard(data, k), {
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

      if (action === "unlock") {
        // Authorized ACCESS unlock for the PRIVATE standard manual. A valid director code lets a
        // parish device render standard from ANY network — including cellular / wrong-geo ISPs the
        // IP-geo radius can't place in Del Rio. Read-only: it only VALIDATES the code (no publish,
        // no DO state), so the public (no code) still can't reach standard. Codes are 10–11 digits,
        // so the yes/no oracle isn't brute-forceable over HTTP, and they already gate /publish — no
        // new secret is exposed here.
        if (request.method !== "POST") {
          return json({ ok: false, error: "method_not_allowed" }, 405, cors);
        }
        const code = (request.headers.get("X-Director-Code") || "").replace(/[^0-9]/g, "");
        const codeOk = code.length > 0 && validTransmitterCodes(env).has(code);
        return json(
          { ok: codeOk, hymnal: codeOk ? "standard" : "nonstandard" },
          codeOk ? 200 : 403,
          cors,
        );
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
        const validCodes = validTransmitterCodes(env);
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
