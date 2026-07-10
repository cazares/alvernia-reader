> ⚠️ **CORRECTION BANNER (2026-07-09).** This map was written at HEAD 16244b25 / build 377. The branch has since been fast-forwarded to build 381 (d5075091). Landed since this map: **#269** capped the "Buscando director…" spinner so it never looks stuck; **#270 REMOVED the "¿Quién usa este iPad?" fleet self-ID modal entirely** (fleet check-in itself remains); **#271** simplified the sync spinner and renamed the songbook PDF (assets/alvernia_manual_2.pdf → assets/signo_vivo_371.pdf), touching web/src/app.js (−112 lines area), index.html, styles.css. Where this map contradicts current source, CURRENT SOURCE WINS — do not report the removed modal or old spinner behavior as findings.

# Subsystem Map — SYNC WORKER (`sync-worker/`)

**Scope:** `sync-worker/src/index.ts` (828 lines, entire file), `sync-worker/wrangler.jsonc`, `sync-worker/README.md`, `sync-worker/test-client.html`, plus `sync-worker/test/a2.test.mjs` + `run-a2.sh` (discovered — README doesn't mention them).
**HEAD state:** post-374 single-book worker. Last worker commits: `348d3d6f` (Slice D crash panel), `6d58baf8` (P6-LOG gate), `7b3eda4c` (P2 follower robustness), `b9cc44b4` (A2 rate limit + seq=0 gate), `318d6f39` (two-book/geo system removal).
All line numbers verified against current source in this worktree.

---

## 1. Architecture

- **Cloudflare Worker** `signovivo-sync` (`wrangler.jsonc:5`), entry `src/index.ts` (`wrangler.jsonc:6`), compat date `2025-06-01` (`:7`), observability enabled (`:8`). Prod URL: `signovivo-sync.4j4982y8jp.workers.dev` (hardcoded in clients: `src/directorRelaySync.js:12`, `PdfReaderApp.tsx:45`, `web/src/app.js:2851`). No custom-domain route yet (commented out, `wrangler.jsonc:23-24`; README.md:59-64 describes it as "later").
- **One Durable Object class, three logical instance families**, all the same class `SyncRoom` bound as `SYNC_ROOM` (`wrangler.jsonc:10-12`; migration `v1` `new_sqlite_classes` `:13` — SQLite-backed storage):
  1. **Sync rooms** — one per church, prod room `alvernia-main`; addressed via `env.SYNC_ROOM.getByName(room)` (`index.ts:739`).
  2. **`__debug_log__`** — fixed instance holding the diagnostic ring buffer (`index.ts:594`, also read by the dashboard at `:716`).
  3. **`__fleet__`** — fixed instance holding roster (with phone numbers) + device check-ins (`index.ts:649`).
- **Env / credentials** (`index.ts:14-27`):
  - `RELAY_DIRECTOR_TOKEN` — secret; `Authorization: Bearer` write credential (scripts/testing).
  - `TRANSMITTER_CODES` — secret; comma-separated director codes accepted via `X-Director-Code`.
  - `FLEET_DASHBOARD_KEY` — secret; gates `/fleet*` reads and `GET|DELETE /log`.
  - `ALLOWED_ORIGINS` — plain var, **still `"*"` in prod config** (`wrangler.jsonc:20`).
- **Constants:** `PROTOCOL_VERSION = 1` (`index.ts:29`); `RELAY_LIVE_MAX_AGE_S = 90` (`:31-34`, matches `web/src/app.js:2870`); `MIN_SYNC_BUILD = 361` (`:400-403`).
- **Snapshot wire shape** (`index.ts:36-46`): `{ v, page, totalPages, mode, bookId, seq, ts }`. `mode` and `bookId` are **vestigial post-374** — stored, echoed, sliced to 64 chars, but read by no route for authz/routing. `EMPTY_SNAPSHOT` (`:48-56`) has `seq:0, ts:0` = "no director live".

## 2. User-visible surfaces (every route)

Routing lives entirely in the default `fetch` handler (`index.ts:571-828`). Everything is wrapped in an outer try/catch (`:580`, `:821-826`) that returns **200 + `EMPTY_SNAPSHOT` + CORS** on any unexpected throw (see Oddity O1).

| Route | Method | Auth | Request | Response | Anchor |
|---|---|---|---|---|---|
| any | OPTIONS | none | — | 204 + CORS | `:581-583` |
| `/`, `/health` | GET | none | — | `{ok:true, service:"signovivo-sync", v:1}` (no geo — post-374) | `:585-587` |
| `/log` | POST | **open** (rate-limited 20 burst / 3/s per IP inside DO; 64 KB content-length cap → 413) | array \| `{entries:[...]}` \| single object (`:608-614`) | `{ok:true,total}` or 429 `{ok:false,error:"rate_limited"}` | `:593-617` |
| `/log` | GET | Bearer token **or** `?k=`/`X-Fleet-Key` == `FLEET_DASHBOARD_KEY`; else 401 | — | `{ok:true, count, entries}` (whole ring buffer) | `:625-638` |
| `/log` | DELETE | same gate | — | `{ok:true, cleared:true}` | `:633-635` |
| `/fleet/checkin` | POST | **open** (rate-limited 10 burst / 1/s per IP; 16 KB cap → 413; non-POST → 405) | device self-report (sanitized in `checkin()`) | `{ok:true,total}` or 429 | `:652-668` |
| `/fleet/roster` | POST | fleet gate (token or key), 128 KB cap | array \| `{people:[...]}` | `{ok:true,count}` | `:690-706` |
| `/fleet/reset` | POST | fleet gate | — | `{ok:true}` (wipes check-ins, keeps roster) | `:707-709` |
| `/fleet-dashboard`, `/fleet/dashboard` | GET | fleet gate; unauthorized gets a **401 HTML help page** telling you to append `?k=` | — | server-rendered Spanish HTML dashboard, meta-refresh 20 s with `?k=` re-embedded | `:679-685`, `:710-727`, renderer `:425-569` |
| `/fleet`, `/fleet.json` | GET | fleet gate | — | raw JSON `{roster (INCLUDING phones[]), devices[]}` newest-first | `:728-730` |
| `/r/:room/subscribe` | GET (WS) | none | must have `Upgrade: websocket` else 426 | 101 upgrade; snapshot pushed immediately on connect | `:741-746`, DO `:305-322` |
| `/r/:room/state` | GET | none | — | snapshot + additive `now` (server epoch s, P2-CLOCKSKEW); DO failure degrades to `EMPTY_SNAPSHOT`, never 500 | `:748-764` |
| `/r/:room/unlock` | any | **none** | — | **always** `{ok:true, hymnal:"standard"}` — post-374 no-op stub kept so old clients don't error | `:766-771` |
| `/r/:room/publish` | POST | Bearer `RELAY_DIRECTOR_TOKEN` **or** `X-Director-Code` (digits-only, `:782`) ∈ `TRANSMITTER_CODES`; else 401 | JSON partial snapshot; 64 KB cap → 413; bad JSON → 400; non-object → `{}` | `{ok:true, seq}` \| `{ok:true, seq, ignored:true}` \| 429 \| 500 `publish_failed` | `:773-818` |
| anything else | — | — | — | 404 `{ok:false,error:"not_found"}` | `:735`, `:731`, `:820` |

**Room-name grammar:** `/^\/r\/([A-Za-z0-9_-]{1,64})\/(subscribe|publish|state|unlock)$/` (`index.ts:398`). Underscore is allowed → `__fleet__` and `__debug_log__` are addressable as rooms (Oddity O2).

**CORS** (`corsHeaders`, `index.ts:361-376`): with `ALLOWED_ORIGINS="*"` (current prod), allow is always `*`. If a list were set: matching origin echoed, **non-matching origin gets `list[0]` (soft-deny, not a rejection)** (`:364-366`). Allowed headers `Authorization, Content-Type, X-Director-Code` (`:372` — X-Director-Code allowed so the file:// native WebView can publish through preflight). `Vary: Origin`, Max-Age 86400. **No `Access-Control-Expose-Headers` at all — X-Hymnal fully gone.**

## 3. Flows

### 3a. Director publish → follower fan-out
1. Director device (native WebView or signovivo.com in director role) POSTs `/r/alvernia-main/publish` with `X-Director-Code` (the app path — `src/directorRelaySync.js:77`) or Bearer token (scripts/test-client). Client seq is **wall-clock ms scale**: `seqCounter = Math.max(seqCounter+1, Date.now())` (`src/directorRelaySync.js:52-59`).
2. Worker validates auth fail-closed (`validTransmitterCodes` `index.ts:387-396` — **empty set if the `TRANSMITTER_CODES` secret is unset; NO hardcoded fallback codes exist at HEAD**), caps body, parses, coerces, and RPCs `stub.publish(body, CF-Connecting-IP)` (`:812`).
3. DO `publish()` (`:131-185`):
   - Per-IP token bucket first: 15 burst / 2 per sec (`:138`) → `{ok:true, rateLimited:true}` → worker maps to 429 (`:816`).
   - **Seq sanitize** (`:146-149`): `Number(input.seq ?? 0)`; non-finite / negative / `> Date.now()+60000` collapses to 0 (anti-poison: an `Infinity` seq would otherwise block every future director).
   - **Staleness** (`:156-158`): stale ⟺ `seq===0` (never published) OR `now - ts > 90s`.
   - Gates while FRESH: `seq=0` → ignored (`:165-167`, A2 anti-override); `seq <= current` → ignored (`:168-170`, anti-rewind). While STALE: anything goes — takeover/reset, self-heals a poisoned seq.
   - Accepted snapshot (`:171-179`): `page` clamped [1, 100000], `totalPages` [0, 100000], `mode`/`bookId` sliced to 64 chars, `seq = incoming>0 ? incoming : current+1`, `ts = now` (server clock, seconds).
   - **Durability order:** `storage.put("snapshot")` → memory cache → `broadcast()` (`:180-184`).
4. `broadcast()` sends the JSON to every socket from `ctx.getWebSockets()` (`:347-356`) — works across hibernation.

### 3b. Follower subscribe / resync / poll
- WS connect → DO `fetch()` (`:306-322`): non-upgrade → 426; `ctx.acceptWebSocket(server)` (`:314`) = **hibernatable accept** — the DO can be evicted from memory while sockets stay open; snapshot sent immediately (`:317`).
- Constructor (`:121-128`) restores the snapshot from storage inside `blockConcurrencyWhile` on every wake → page state survives hibernation/eviction. (`rateBuckets` do NOT — in-memory only, reset on wake; fail-open by design.)
- Inbound WS messages: only string `"resync"` or `"ping"` → resend snapshot; everything else silently ignored (followers are read-only) (`:325-333`).
- Poll fallback: `GET /r/:room/state` → `getState()` (`:188-190`) with catch → `EMPTY_SNAPSHOT`; response adds `now` (epoch s) for client clock-skew calibration — additive only, **not** persisted, **not** on the WS wire (`:758-763`).

### 3c. Telemetry (`/log`) — Multipeer breadcrumbs + crash reports
- Native batches sync-lifecycle breadcrumbs to POST `/log` (~1 s batches, `PdfReaderApp.tsx:139-151`); web posts crash telemetry (debounced, session-capped, strips query/hash so `?k=` can't leak — `web/src/app.js:2896-2931`).
- DO `appendLog` (`:194-211`): rate-limited, batch sliced to 200 entries, each stamped `rx` (server receive epoch-seconds), ring buffer keeps **last 600** entries under storage key `dbglog`.
- GET/DELETE gated since P6-LOG (`6d58baf8`): buffer contains device ids/roles/pages — not world-readable/wipeable (`:619-638`).

### 3d. Fleet readiness check-in + dashboard
- Devices POST `/fleet/checkin` openly (native: `PdfReaderApp.tsx:185`; web: `web/src/app.js:2993`). `checkin()` (`:250-289`) sanitizes: `deviceId` (required, ≤64 chars — drops entry if empty `:257`), `label`/`role` ≤60, `nativeBuild` clamped [0, 1e6], `pagesCached`/`totalPages` clamped, booleans coerced; merges over previous entry per deviceId; ring-caps to the **300 most-recently-seen devices** (`:281-287`).
- `putRoster()` (`:228-246`): ≤200 people, ≤6 phones each (≤40 chars), phones live **only** server-side (never in the public repo or device pings — design note `:58-63`).
- Dashboard (`renderFleetDashboard` `:425-569`): matches devices to roster people by **normalized-name equality** (`normName` `:414-420`: lowercase, NFD accent-strip, alnum). Readiness logic per row (`:449-475`):
  - `latest = max(highest reported nativeBuild, MIN_SYNC_BUILD)` (`:433-434`).
  - **Director**: ok only if `bestBuild >= latest` (must be on the fleet-latest build); else red (`:462-464`).
  - **Follower**: ok if `bestBuild >= MIN_SYNC_BUILD (361)`; ok if web cached + homeScreen; warn if cached w/o home-screen or partial cache; red if build < 361 or never seen.
  - Sort: bad → warn → ok, directors floated up (`:476`, `:499`).
- Orphan devices (label matches nobody) get their own table (`:506-515`). Crash panel (Slice D): reads `__debug_log__`, filters `kind === "crash"`, newest-first (by `rx`, fallback `t/1000`), cap 25 (`:713-722`, `:520-531`); best-effort — log failure can't break the roster (`:715-721`).

### 3e. Local test harness (undocumented in README)
- `sync-worker/test/run-a2.sh`: boots `wrangler dev` on :8787, pulls the first digit code from `.dev.vars` `TRANSMITTER_CODES` + `FLEET_DASHBOARD_KEY`, runs `node --test test/a2.test.mjs`, tears down. Test file **throws at load** unless `RELAY_TEST_BASE`+`RELAY_TEST_CODE` are set and **refuses any base matching `/signovivo|workers\.dev/`** (`a2.test.mjs:11-20`) — safe by construction, unlike `e2e/relay-sync.test.mjs` (which hits prod — never run). Coverage: baseline publish, seq=0 gate fresh/stale, `/state now`, P6-LOG gates, Slice D crash-on-dashboard, publish flood → 429s. `package.json` has **no `test` script** (`dev/deploy/tail/typecheck` only) — the harness is bash-only.

## 4. Contracts / invariants (what other subsystems rely on)

- **C1.** `seq === 0` ⟺ "no director live yet"; clients stay in free-browse (README:22-23; `EMPTY_SNAPSHOT :54`).
- **C2.** While a snapshot is fresh (< 90 s), seq is strictly monotonic; equal/lower/zero publishes return `{ok:true, ignored:true}` and do NOT broadcast. After 90 s staleness ANY seq (incl. lower/0) wins — takeover + poison-self-heal (`:156-170`).
- **C3.** Live directors heartbeat ~every 12 s (`:32-33`, `PdfReaderApp.tsx:378`), so a snapshot older than 90 s genuinely means "director gone". Web mirrors with `RELAY_LIVE_MAX_AGE_S = 90` (`web/src/app.js:2870`) and uses `/state`'s `now` + `clockOffsetMs` for freshness (`web/src/app.js:3188`).
- **C4.** Client-generated seqs are wall-clock-ms scale (`directorRelaySync.js:58`); server-assigned takeover seqs are `current+1` (counter scale). The seq upper sanity bound is `Date.now()+60000` (`index.ts:147`) — couples seq validity to server wall-clock ms.
- **C5.** `/state` and the outer catch NEVER return a CORS-less error to the browser: every branch degrades to a snapshot-shaped 200 with CORS (`:748-757`, `:821-826`) — the web client must never brick on a worker hiccup.
- **C6.** Auth matrix: `/publish` = token OR code (fail-closed if `TRANSMITTER_CODES` unset); `/fleet*` reads + `GET|DELETE /log` = token OR `FLEET_DASHBOARD_KEY`; `POST /log` + `/fleet/checkin` = open but rate-limited + size-capped; reads of room state = fully public. **Transmitter codes deliberately do NOT gate fleet PII** (`:670-673`).
- **C7.** `bookId`/`mode` are wire-compat vestiges: still sent (`directorRelaySync.js:118-121` hardcodes `bookId:'standard'`), still stored/echoed by the DO (`:175-176`), read by nobody for decisions. `/unlock` is an unauthenticated always-ok stub; **no current client calls it** (only comments reference it: `web/src/app.js:828,1193`).
- **C8.** Rate limiting is **fail-open by contract** (`:95-97, :101, :117`): any limiter error or missing IP → allow. Blocking a real director mid-Mass is deemed worse than letting a flood through.
- **C9.** Persistence keys in DO storage: `snapshot` (rooms), `dbglog` (debug instance, ring 600), `roster` + `fleet_devices` (fleet instance, ring 300).
- **C10.** Durability before fan-out: storage.put precedes broadcast (`:180-183`).

## 5. Mismatches — README / test-client / comments vs. real code

- **M1. README endpoint table is badly incomplete** (README.md:10-17): lists only subscribe/state/publish. Missing: `/health`, `/log` (POST/GET/DELETE), the entire `/fleet*` family, and `/r/:room/unlock`.
- **M2. README:67-68 "RELAY_DIRECTOR_TOKEN is the only write credential" — FALSE at HEAD.** `X-Director-Code` ∈ `TRANSMITTER_CODES` also authorizes `/publish` (`index.ts:777-786`), and `POST /log` + `/fleet/checkin` are open writes. The security section (README:66-71) predates A2/P6/fleet entirely (no mention of rate limits, fleet key, PII gating).
- **M3. README:71 "Production: set ALLOWED_ORIGINS to https://signovivo.com" — never done**; `wrangler.jsonc:20` ships `"*"`. Note tightening is non-trivial: the native WKWebView loads from `file://` (Origin `null`), which is exactly why `X-Director-Code` is in Allow-Headers (`index.ts:370-372`).
- **M4. README:20 example payload says `"totalPages":370`; the book is 371 pp** (clients and a2 tests use 371). test-client.html:105 also hardcodes `totalPages:370`.
- **M5. README:22-23 describes seq as purely monotonic** — omits the 90 s stale-takeover bypass, the seq=0 fresh-gate, and seq sanitization (all behavior the clients depend on).
- **M6. test-client.html publishes with Bearer token ONLY** (`:104`) — it cannot exercise the production auth path (`X-Director-Code`), which is what the actual app uses.
- **M7. test-client's stale guard can freeze on takeover**: `applySnapshot` ignores any `seq <= lastSeq` (`test-client.html:64`) with no freshness-first check. After a stale-room takeover the server can legitimately assign a LOWER seq (counter-scale `current+1` vs previous wall-clock-ms scale) — the real web client fixed this class in P2 (`7b3eda4c`, freshness-before-seq); the test rig did not.
- **M8. README's "Test it" section (44-49) only describes the manual test-client** — no mention of `test/a2.test.mjs` / `run-a2.sh`, the only safe automated worker tests in the repo.
- **M9. Stale comment inside index.ts itself**: `:24-25` and `:672-673` claim transmitter codes "are hardcoded in this PUBLIC repo" — that was true pre-368; at HEAD `validTransmitterCodes` (`:387-396`) is fail-closed with **zero** plaintext codes anywhere in the repo. The comment's *conclusion* (don't gate PII behind codes) still stands, but its premise is stale and could mislead a future editor.
- **M10. `wrangler.jsonc:15-16` comment lists only `RELAY_DIRECTOR_TOKEN` as the secret** — `TRANSMITTER_CODES` and `FLEET_DASHBOARD_KEY` are also required secrets (fail-closed features without them).
- **M11. X-Hymnal remnants:** none in `sync-worker/` code (fully removed — no Expose-Headers, no geo). Survives only in stale docs: `HANDOFF.md:106-123` + `HANDOFF.md:243` ("X-Hymnal geo (~line 205)" — describes deleted code), plus historical mentions in `docs/app-hardening-plan.md` and the audit docs (which correctly document the removal).

## 6. Oddities (smells for later audit lenses)

- **O1. Outer catch masks failures as "no director" and 200s everything** (`index.ts:821-826`): ANY unexpected throw — including in `/publish` auth or the `/fleet` PII gate — returns **200 + `EMPTY_SNAPSHOT` + CORS** regardless of route. A publisher gets a snapshot-shaped body with no `ok` field and HTTP 200; a fleet request that throws mid-gate returns a snapshot instead of 401/500. Deliberate for `/state` resilience, but it makes real worker bugs invisible (clients see "no director") and returns the wrong shape on non-snapshot routes.
- **O2. Fixed internal DO instances share the public room namespace**: the room regex (`:398`) permits underscores, so `/r/__fleet__/state`, `/r/__fleet__/publish` (with any valid code), and `/r/__debug_log__/subscribe` all resolve to the SAME DO instances holding fleet PII / logs. Storage keys don't collide (`snapshot` vs `roster`/`fleet_devices`/`dbglog`) so no data leaks today, but a valid-code holder can write a `snapshot` key into the fleet DO and anyone can hold WebSockets open on it. Namespace-segregation smell.
- **O3. CORS soft-deny**: a non-allowlisted Origin gets `list[0]` echoed rather than a denial (`:364-366`). Moot while `ALLOWED_ORIGINS="*"`, a footgun the day it's tightened.
- **O4. Content-length-header-trusting size caps** (`:599`, `:656`, `:692`, `:792-793`): a chunked request with no `content-length` bypasses every early 413. `/publish` still dies at JSON parse → 400, but `/log` would parse and store — and **individual `/log` entries are size-unbounded** (only the batch count is sliced to 200, `:206`); one giant string entry reaches the ring buffer. Cloudflare's own body limits are the real backstop.
- **O5. Rate-limit state is per-DO-instance memory** (`:98`): hibernation (likely, since sockets are hibernatable) resets all buckets; the `> 20000` map-flood guard **clears every bucket including the attacker's** (`:105`). Both accepted under the documented fail-open philosophy — but the flood protection is best-effort, not durable.
- **O6. Seq-scale asymmetry**: server sanitization rejects seqs `> Date.now()+60000` (`:147`) — a director device with a clock > 60 s fast gets every seq collapsed to 0 and (while another director is fresh) **all its publishes ignored**; after a stale takeover the room can sit at counter-scale seq (e.g. 1), which any wall-clock-scale director instantly outbids. Works, but the two seq scales coexisting is fragile reasoning territory.
- **O7. `/unlock` is a live, unauthenticated, always-ok route** (`:766-771`) that no shipped client calls — pure attack-surface/compat vestige. Fine today; pin it (audit reconciliation already recommends a regression test so nobody re-adds auth semantics to it silently).
- **O8. Fleet key travels in the URL** (`?k=` — browser history, server logs) and is re-embedded into the meta-refresh URL (`:533-536`); `/fleet.json` then hands out **choir phone numbers as JSON with CORS `*`** to any key holder (`:728-730`). Accepted design (dedicated secret, PII never in repo), but it's the single most sensitive read in the worker guarded by a query param.
- **O9. Check-in identity is honor-system**: `deviceId` and `label` are client-chosen (`:256`, `:266`); anyone can overwrite another device's readiness entry or impersonate a roster name on the dashboard (matching is by normalized label, `:443-446`). Open-by-design; the dashboard is advisory.
- **O10. Crash entries share the 600-entry ring with sync breadcrumbs** (`:208`): a chatty Multipeer session can evict crash reports before Miguel views the dashboard. Also `appendLog` rewrites the full array per POST — write amplification, fine at parish scale.
- **O11. `renderFleetDashboard` "Actualizar app" row builds HTML with `&lt;` pre-escaped inside a template literal (`:474`) while other branches rely on `escHtml`** — inconsistent escaping discipline (no injection found: `action` strings are static + numbers, names/roles/phones go through `escHtml`; crash fields all escaped `:528`).
- **O12. `getFleet`/`putRoster`/`resetFleet`/log RPCs have no instance guard** — nothing stops a future code path from calling `checkin()` on a page room or `publish()` on `__fleet__`; correctness rests purely on the worker's routing discipline (`:594`, `:649`, `:739`).
