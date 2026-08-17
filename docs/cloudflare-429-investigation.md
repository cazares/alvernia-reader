# Cloudflare 429 / 1027 — traffic investigation handoff

> **Status: OPEN. Production is degraded right now.**
> Written 2026-08-17 ~2:50 PM CT for a cold tab. Everything below is measured unless marked
> *hypothesis*.

## The symptom

```
https://signovivo.com/                                   -> HTTP 429
https://signovivo-sync.4j4982y8jp.workers.dev/health     -> HTTP 429
```

Earlier the same afternoon signovivo.com returned Cloudflare **error 1027** (account-level
limit / suspension) before settling into 429s. Both the Pages site and the Worker are
affected, which points at an **account-wide** limit rather than one project.

**Impact:** web followers on signovivo.com cannot load the app at all. Native iPads are
unaffected — they sync over the Multipeer mesh and render the book baked into their own
binary, neither of which touches Cloudflare.

Miguel reports the account has been "reaching its limit lately" — so this predates today,
and today's activity is probably an accelerant rather than the whole cause.

## Known traffic sources, with measured rates

Every native device and every web client talks to the same Worker
(`RELAY_BASE = https://signovivo-sync.4j4982y8jp.workers.dev`).

| caller | endpoint | rate | notes |
|---|---|---|---|
| **`dbgLog` (Swift)** | `POST /log` | **one POST per mesh event** | `DirectorSyncModule.swift:194`. Fire-and-forget, no batching, no throttle. The big one — see below. |
| `dbgLog` (JS shell) | `POST /log` | per event | `PdfReaderApp.tsx:351` |
| fleet check-in | `POST /fleet/checkin` | every **90 s** per device | `PdfReaderApp.tsx:468` |
| director relay heartbeat | `POST /r/<room>/publish` | every **12 s** while directing | `PdfReaderApp.tsx:729` |
| **web follower polling** | `GET /r/<room>/state` | every **4 s** per web client | `web/src/app.js:3836` — fallback path when the WebSocket is not carrying |
| stress-capture (dev tool) | `GET /log` | every **5 s** per running capture | `scripts/stress-capture.mjs`. **Stopped 2026-08-17 ~2:50 PM.** Ran for much of the day. |

### `dbgLog` is the prime suspect

It POSTs on **every** mesh event: `found`, `lost`, `session:*`, `invite:*`, `refresh:hold-*`.
Discovery is chatty — a single follower logged **~150 `found` events in two minutes** while
hunting for a director (measured 2026-08-17, build 431). With 5–7 devices all discovering
each other, that is easily **several POSTs per second sustained**, and it gets *worse* the
worse the mesh is behaving.

There is no batching, no sampling, and no kill switch. A device in a bad discovery loop
becomes a traffic generator.

### A contributor that was mine, now fixed but NOT yet shipped

The BLE page beacon added in build 433 published on every call to `sendPageUpdate` — and the
director's **mesh heartbeat calls that once per second**, not once per page turn. Measured
over a 33-minute run:

```
280 real page turns  ->  907 ble:page-send + 2436 ble:page-recv
                     ~= 1.7 extra relay POSTs per second, continuously
```

Fixed in the working tree (`BlePageBeacon.swift`: publish and log only when the page actually
changes) and compiled, but **builds 433 and 434 are on TestFlight with the noisy version**.
Anyone running those is still generating it.

## What to check first

1. **Cloudflare dashboard → the actual limit.** Which product tripped — Workers requests/day,
   Pages builds, or something account-level? 1027 usually means account-level. This decides
   whether the fix is traffic reduction or a plan change.
2. **Worker analytics → top talkers.** Break requests down by path. If `/log` dominates,
   `dbgLog` is confirmed and the fix is obvious. If `/r/<room>/state` dominates, it is web
   followers polling every 4 s.
3. **Is anything OUTSIDE the fleet hitting it?** The Worker is public. `/log` accepts POSTs
   with no auth (`sync-worker/src/index.ts:639`). A scanner or a stuck client anywhere on the
   internet could be hammering it. Check source IPs — and note the rate limiter buckets by IP
   while a whole fleet shares one NAT, so fleet traffic looks like a single abusive client.
4. **Count the real fleet.** `/fleet.json` listed **~50 device entries** on 2026-08-17, most of
   them long-dead peer identities. If any old builds are still checking in every 90 s, that is
   background load nobody remembers.

## Candidate fixes, cheapest first

- **Sample or batch `dbgLog`.** It is a diagnostic, not a product feature. Buffer events and
  POST once every 5–10 s, or drop the high-volume ones (`found`, `refresh:hold-*`) entirely.
  Likely the single biggest win.
- **Add a kill switch.** A server-side flag the client honours would let this be turned off
  without a TestFlight round-trip. There is currently no way to stop a device logging.
- **Back off web polling.** 4 s per client is aggressive for a page number; 10 s would be
  invisible to a singer.
- **Auth `/log`.** It is unauthenticated and public.
- **Ship the BLE throttle** (already written) so 433/434 devices stop the 1.7/s.

## Context worth knowing

- **The mesh does not need Cloudflare.** At Mass the follower iPads are on **no network at
  all** — only the director has cellular. So a throttled Worker does not break Mass; it breaks
  web followers and all telemetry. See `project_mass_network_reality` in project memory.
- **Telemetry is how every bug this week was found.** Do not delete it wholesale — throttle it.
  Losing `session:peer-not-director` and `mesh:page-recv` would have made the build-430 fix
  unverifiable.
- `scripts/stress-capture.mjs` + `scripts/stress-analyze.mjs` are the tools for reading this
  data. `stress-analyze` scores a capture; see its header for the criteria.
