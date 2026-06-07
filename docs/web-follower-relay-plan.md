# Web-Follower Sync — "Featherweight Relay" Implementation Plan

> **Status:** PROPOSED (not started) · **Owner:** Miguel · **Drafted:** 2026-06-06
> **Goal:** let people who open **signovivo.com in a phone browser** follow the
> director live, seamlessly — given the church has only **weak cell and no shared
> WiFi/router**.
>
> This is the authoritative plan for the work. Re-read the **Changelog** at the
> bottom at session start and on resumption (per `~/.claude/CLAUDE.md` §6).

---

## 0. TL;DR

The director's native app **cannot** be a local server to *browser* followers
under "phones-only, no WiFi, no reliable internet" — browsers physically cannot
open a socket to a peer without either the internet or a shared LAN, and you've
ruled out the shared LAN. (Multipeer works today only because native apps can
build their *own* WiFi/BT network — a power browsers never get.)

But the real goal — "let signovivo.com browser users follow along" — is very
achievable with a **featherweight cloud relay**, because **the website already
caches all 370 page images offline** (`web/src/sw.js`). The network therefore
only has to carry the *page number* (~50 bytes every several seconds), which
weak cell handles fine even when it can't stream anything heavy.

```
Director's app  ──(tiny HTTP POST: "page 142")──▶  Cloudflare relay  ──(push)──▶  every signovivo.com browser
   (also keeps Multipeer for native followers, untouched)                          (renders page 142 from its offline cache)
```

- ✅ Zero-touch for followers (just open signovivo.com — better than the QR you'd tolerate)
- ✅ Unlimited followers (no Multipeer ~7 cap)
- ✅ Survives weak cell (tiny payloads + content already cached)
- ✅ Multipeer path stays 100% intact — relay is purely **additive**, zero risk to what works today
- ⚠️ Fails only in true dead-zones (no signal at all) — that's a later job for an App Clip (see §11)

> **UPDATE (2026-06-06): cell coverage in the church is GOOD.** Both "average
> Joseph" and the director have service. This **retires the weak-cell / dead-zone
> risk entirely** — Path D is now a standard, low-risk real-time relay; no
> church-trip gate. **Canonical requirement:** *Joseph pulls out his phone, opens
> signovivo.com (no app, no login), and it always shows the song the director is
> currently on, updating live as the director advances.*

---

## 1. Why not "director as local server" (the honest constraint)

For a **browser** follower, here is every live channel and why your constraints block it:

| Channel a browser *can* use | Needs | Your constraint | Result |
|---|---|---|---|
| Cloud / internet relay | any internet (even a sliver) | weak cell | 🟡 **works if payload is tiny** ← this plan |
| Local WebSocket/HTTP to director's phone | shared WiFi/LAN (router or hotspot-join) | "phones only," won't join WiFi | ❌ blocked |
| Web Bluetooth | Android + pairing tap | iOS Safari never supported it | ❌ blocked |
| WebRTC peer-to-peer | a network route + signaling | no shared net, no internet | ❌ no route to build on |

**Key insight:** the *heavy* part (song images) is already offline on every
follower's phone. Only the *pointer* (which page) needs the network. That single
fact is what makes weak cell sufficient.

---

## 2. Architecture

```
┌─────────────────────────┐         ┌───────────────────────────────┐         ┌──────────────────────────┐
│   DIRECTOR (native app)  │         │   Cloudflare Worker + Durable  │         │  FOLLOWER (signovivo.com │
│                          │         │   Object  "SyncRoom"           │         │  in any phone browser)   │
│  page change ──────────┐ │  HTTPS  │                                │   WS    │                          │
│  broadcastDirectorPage()│─┼────────▶ POST /r/:room/publish (auth)   │  push   │  renderPage(page,…) from │
│        ├─ Multipeer (as is) │       │   → store latest + seq         │────────▶│  offline image cache     │
│        └─ relay publish  │ │        │ GET  /r/:room/subscribe  (WS)  │         │  + 📡 "siguiendo" badge  │
│                          │ │        │ GET  /r/:room/state  (poll fb) │◀────────│  WS↦poll fallback, resync│
└─────────────────────────┘ │        └───────────────────────────────┘  GET    └──────────────────────────┘
   (native followers keep    │                latest state only
    syncing over Multipeer ──┘                (page,totalPages,mode,bookId,seq,ts)
    exactly as today)
```

**Three pieces of work:**

1. **Relay service** — a new Cloudflare Worker + Durable Object. The DO holds one
   "room" = the church's live page state, accepts authenticated writes from the
   director, fans out pushes to all subscribed browsers. Uses the **WebSocket
   Hibernation API** so it costs ~nothing while idle.
2. **Director publishes** — `PdfReaderApp.tsx` calls a new `publishDirectorPageToRelay()`
   alongside the existing Multipeer broadcast, at the 4 sites that already call
   `sendNearbyDirectorPageUpdate`.
3. **Web subscribes** — `web/src/app.js` connects to the relay on load (on
   *standalone* signovivo.com, independent of the in-app bridge), and applies
   updates via the existing `renderPage(page, { pushToHistory: false })`.

---

## 3. Protocol & data model

**One payload shape, reused everywhere (mirrors the existing Multipeer payload):**

```jsonc
{ "v": 1, "page": 142, "totalPages": 370, "mode": "standard", "bookId": "standard", "seq": 87, "ts": 1733500000 }
```

- `seq` — monotonic counter per director session. Followers **ignore any update
  with `seq` ≤ last applied** → out-of-order / duplicate / replayed packets can't
  rewind the song. Resets when a director (re)starts a session.
- `ts` — unix seconds. Used to decide "is a director live right now?" (e.g. ts
  within last 90s) and to show "weak signal / director paused" hints.

**Endpoints (Worker):**

| Method | Path | Who | Purpose |
|---|---|---|---|
| `POST` | `/r/:room/publish` | director (auth) | set latest page state, broadcast to subscribers |
| `GET`  | `/r/:room/subscribe` | followers | WebSocket upgrade; on open → current state, then each update |
| `GET`  | `/r/:room/state` | followers | plain JSON latest state (initial paint **+ poll fallback**) |

- `:room` — single fixed room for the one church: **`alvernia-main`**. (Multi-choir
  rooms later via `?room=` in a shared link — you said taps/links are fine.)
- **Reads are public** (it's just a page number). **Writes require auth** (§5).

---

## 4. Weak-cell resilience (the part that actually matters)

| Side | Strategy |
|---|---|
| **Director → relay** | **HTTP POST** (not a held socket) with 1 retry + backoff. **Latest-wins coalescing:** if a send is in flight and the page changes again, drop the stale one and send only the newest. Fire-and-forget — never blocks the director UI. Runs *in parallel* with Multipeer, so native followers are unaffected if the relay POST fails. |
| **Relay → follower** | **WebSocket** primary (cheap via hibernation, instant push, low battery). **Exponential-backoff reconnect.** On *every* (re)connect, immediately `GET /state` to resync (covers any missed update). If WS can't open after 2 tries → **fall back to polling `GET /state` every ~4s.** Payload is ~50 bytes either way. |
| **Both** | Followers **hold the last page** on disconnect (people sing the same song for a while, so a brief drop is invisible). UI shows a subtle "señal débil — reconectando…" hint, **never an error**. |

This layering (WS → poll, with resync-on-connect and seq-guarded apply) is what
lets it degrade gracefully on the exact flaky-cell network you described.

---

## 5. Security & abuse

- **Reads:** open. A church page number is not a secret.
- **Writes:** `POST /publish` requires **both**:
  - `Authorization: Bearer <RELAY_DIRECTOR_TOKEN>` — a Worker secret, embedded in
    the native app build (not in the web bundle).
  - `X-Director-Code: <hash(accessCode)>` — proves the human passed the existing
    `DIRECTOR_ACCESS_CODES` gate. So extracting the binary token alone isn't enough.
- **Blast radius if abused:** worst case a troll flips the page number; no data,
  no PII, no money. Mitigate by rotating the token in an app update. (Hardening
  later: per-director signed tokens from a tiny auth endpoint.)
- **CORS:** allow the `https://signovivo.com` origin for `/state`; WS origin-check.
- **Connection caps:** basic per-IP WS cap in the DO to blunt connection floods.

---

## 6. File-by-file changes

### New
| Path | What |
|---|---|
| `sync-worker/wrangler.toml` | Cloudflare Worker + DO binding config |
| `sync-worker/src/index.ts` | Worker router + `SyncRoom` Durable Object (publish/subscribe/state, hibernation, seq, caps) |
| `sync-worker/test/relay.test.mjs` | node `--test`: publish→broadcast, seq ordering, auth rejection, poll fallback |
| `src/directorRelaySync.js` | native relay publisher: `publishDirectorPageToRelay(page,total,{mode,bookId})` — reachability check, retry+backoff, latest-wins coalesce, auth headers, token from `expo-secure-store`/build config |

### Edited
| Path | Change |
|---|---|
| `PdfReaderApp.tsx` | Add `broadcastDirectorPage(page,total,ctx)` wrapper that calls **both** `sendNearbyDirectorPageUpdate(...)` (unchanged) **and** `publishDirectorPageToRelay(...)`. Replace the 4 existing call sites (`:1315`, `:1492`, `:1670`, `:2158`) with the wrapper. Publishing is active only while the device is the director. |
| `web/src/app.js` | Add relay-follow: on load (standalone web, **independent of `nativeSyncAvailable`**) `GET /state`; if a director is live, auto-follow and apply each update via the existing `renderPage(page,{pushToHistory:false})` (same entry point used at `:805`). Add `state.relay*` fields + a 📡 "siguiendo al director" badge mirroring the existing follower UI, plus a "explorar libremente / volver a sincronizar" toggle. WS + poll fallback + reconnect. |
| `web/src/sw.js` | Add the relay origin to a **network-only bypass** (never cache `/r/...`). Bump `CACHE_VERSION`. |
| `web/build.mjs` | Inject the relay base URL (e.g. `https://sync.signovivo.com`) as a build-time constant. No bundler change (keep relay code inline in `app.js` to match the no-build setup). |

### Infra (needs your Cloudflare/DNS access — see §10)
- DNS: `sync.signovivo.com` → the Worker (or start on `*.workers.dev`).
- Worker secret: `RELAY_DIRECTOR_TOKEN`.

---

## 7. Milestones (each independently shippable)

| # | Milestone | Output | Gate |
|---|---|---|---|
| **M0** | **De-risk weak cell** | Throwaway: minimal relay + a 1-page web subscriber; test latency/reliability on a throttled + lossy connection (and ideally the actual church) | Prove tiny pushes survive your real network **before** building the rest |
| **M1** | Relay service | Worker + `SyncRoom` DO deployed; unit/integration tests green | publish→subscribe works, auth enforced |
| **M2** | Director publishes | Native app posts to relay alongside Multipeer, behind director mode | Multipeer still works identically; relay receives pages |
| **M3** | Web subscribes | signovivo.com auto-follows live director; 📡 badge; reconnect + poll fallback | Two phones: one director, one browser, in sync |
| **M4** | Hardening | seq ordering, resync-on-connect, "weak signal" UX, connection caps, director-live detection | Survives forced disconnects/throttling in tests |
| **M5** | Field test at Mass | Real-world validation; iterate | Followers stay synced through a real service |

**Recommended first action (updated 2026-06-06):** Church coverage is confirmed
good, so the weak-cell question is no longer existential → **M0 is now optional**
(keep it only as a quick local throttled smoke test; no church trip needed as a
gate). **Recommended start: the walking skeleton — M1 + a thin slice of M2 + M3**
— one real follower following the real director, which doubles as the field test.

---

## 8. Testing (per CLAUDE.md §11 — e2e first)

- **e2e/integration** (matches existing `e2e/*.test.mjs`, node `--test`): spin the
  Worker locally (`wrangler dev`/Miniflare), POST a publish, assert a WS subscriber
  receives it; assert a stale `seq` is ignored; assert unauthorized publish is 401;
  assert poll `GET /state` returns latest.
- **Weak-cell sim:** throttle + drop packets; assert reconnect, resync-on-connect,
  latest-wins, no rewind.
- **Regression:** confirm Multipeer native sync is byte-for-byte unchanged (relay
  is additive). Run `npm run typecheck` + existing e2e.
- **Manual (M5):** real Mass.

---

## 9. Cost

One church, ~weekly hour, tens–low-hundreds of followers, ~50-byte messages,
WS Hibernation (DO sleeps between page changes). Effectively **pennies/month**,
almost certainly within Cloudflare's free tier. (Matches the OTA/Cloudflare
direction already in the memory notes.)

---

## 10. Open questions (need your input before/within M1)

1. **Where is signovivo.com hosted, and is its DNS on Cloudflare?** The relay can
   be an independent Worker on `sync.signovivo.com` regardless of where the static
   site lives (cross-origin WS/fetch is fine) — but I need Cloudflare account + DNS
   access to create the subdomain and set the secret. If DNS isn't on Cloudflare,
   we can start on `*.workers.dev` and CNAME later.
2. **Single room vs per-choir rooms?** Plan assumes one fixed room (`alvernia-main`)
   = zero-touch. Multi-room is a small add (`?room=` link) if you ever want it.
3. **Auto-follow vs one-tap follow?** Plan auto-follows when a director is live,
   with an easy "browse freely" toggle. Confirm that's the seamless behavior you want.
4. **How many web followers, and who?** **[RESOLVED 2026-06-06]** ~8–10 choir
   devices + ~16–30 congregation ≈ **24–40 total** → far above the ~5 hotspot cap,
   so hotspot-local (§12) is **ruled out**; **Path D relay confirmed.** Still open:
   is the director iPad **cellular or WiFi-only**? If WiFi-only, the iPhone hotspot
   is load-bearing for the director's relay leg.

---

## 11. Future: true dead-zone fallback (Path A — App Clip)

When there is *zero* signal, no relay can help. The complement is an **iOS App
Clip**: scan a QR → an instant, no-install native slice joins the director's
**offline Multipeer mesh**. Out of scope for this plan; noted so the relay is
designed to coexist (the web client can later detect "no signal" and surface the
App Clip QR). Android's equivalent (Instant Apps) is deprecated, so Android stays
relay-only.

---

## 12. Update — director uplink via iPhone hotspot (2026-06-06)

**New fact:** the director iPad is always tethered to the director's iPhone
Personal Hotspot (and Miguel's iPhone is a backup hotspot). This **hardens the
director's leg** of Path D — and is *load-bearing* if the iPad is WiFi-only (no
SIM), since the hotspot is then its only route to the relay.

**What it changes vs. doesn't:**

```
Director iPad ──hotspot──▶ iPhone cell ──▶ relay     ← now solid + backup  ✅
Relay ──▶ internet ──▶ each follower's OWN cell       ← UNCHANGED; M0 must still prove this  ❓
```

Followers cannot ride the hotspot at congregation scale: Apple caps Personal
Hotspot at ~5 devices, and the iPad already takes a slot. So the follower
downlink (weak cell) is untouched, and **M0 still gates the project.**

**Resilience this adds (Murphy's Law §1):**
- Director uplink is flappy by nature → relay publish stays fire-and-forget with
  retry + latest-wins (§4). If the hotspot drops: Multipeer still carries native
  followers, and web followers hold last page until it returns.
- Add a director-screen **"sync web: en vivo / sin conexión"** indicator so Miguel
  can see at a glance whether browser-followers are being reached.

**Small-group alternative this OPENS (gated on §10.4 follower count):**
> **[RULED OUT 2026-06-06]** Actual scale is ~24–40 followers — far past the ~5
> hotspot cap. This local-only option is not viable here; kept for the record.

If the people who need signovivo.com are just the **choir (~≤4)**, everyone can
join the director's hotspot (QR-to-join) and sync against a **local server on the
iPad — zero cloud, zero internet** (the original "local server" instinct, now
viable). Caps at ~5 devices, adds a one-time QR join, serves over local HTTP (not
signovivo.com HTTPS). Worth it only for a genuine dead-zone + tiny group; for
anything congregation-sized, Path D (relay) remains the answer.

---

## Changelog

- **2026-06-06** — Initial plan drafted. Chose Path D (featherweight relay) over
  local-server (impossible for browsers under phones-only/no-WiFi) and App Clip
  (heavier, dead-zone-only). Grounded integration points in code:
  `sendNearbyDirectorPageUpdate` call sites `PdfReaderApp.tsx:{1315,1492,1670,2158}`,
  web apply fn `renderPage` `web/src/app.js:805`, director gate `PdfReaderApp.tsx:93`.
  Awaiting answers to §10 before M1.
- **2026-06-06 (update)** — Recorded that the director iPad is always tethered to
  the iPhone hotspot (+ Miguel's iPhone as backup). Hardens the director→relay leg
  (load-bearing if the iPad is WiFi-only); does **not** change the follower
  downlink (Apple's ~5-device hotspot cap rules out congregation-scale tethering).
  Added director uplink-health indicator + graceful-degrade requirement, and a
  small-group hotspot-local alternative (§12) gated on follower count (§10.4).
- **2026-06-06 (resolved §10.4)** — Scale is ~8–10 choir + ~16–30 congregation ≈
  **24–40 web followers**, far above the ~5 hotspot cap → hotspot-local ruled out,
  **Path D relay confirmed**. Note: choir alone (~8–10) already nudges Multipeer's
  ~7/session cap, so the relay is also the natural scalable home for web followers;
  Multipeer stays as the offline-capable transport for the native choir.
- **2026-06-06 (coverage clarified)** — Church cell coverage is **good** (both
  Joseph and the director have service), correcting the earlier "weak cell"
  assumption. **Weak-cell/dead-zone risk retired** → no church-trip gate; M0
  demoted to optional smoke test. Canonical UX locked: Joseph opens signovivo.com
  → always sees the director's current song, live. Plan to start the **walking
  skeleton** once Cloudflare access (§10.1) is confirmed.
- **2026-06-07 (M1 DEPLOYED + PROVEN)** — Relay built in `sync-worker/` (Worker +
  `SyncRoom` DO, WebSocket hibernation, publish/subscribe/state, seq guard, bearer
  auth). Deployed to Cloudflare "Miguels Account" at
  `https://signovivo-sync.4j4982y8jp.workers.dev`. Smoke tests PASS: authed publish,
  state persistence, 401 on unauth, and **live WebSocket push verified end-to-end**
  (publish → push). Token set as Worker secret `RELAY_DIRECTOR_TOKEN` (mirror in
  gitignored `sync-worker/.dev.vars`). §10.1 resolved (wrangler authed via home dir).
  Next: M2 (native director publish hook at the 4 `sendNearbyDirectorPageUpdate`
  sites) + M3 (signovivo.com subscriber calling `renderPage`). Prod hardening TODO:
  set `ALLOWED_ORIGINS=https://signovivo.com`; optional custom domain sync.signovivo.com.
