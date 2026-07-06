# Green-day deploy runbook — ship the 2026-07-05 batch

> ## ✅ STATUS: DEPLOYED (2026-07-05)
> - **Step 1 (worker) — DONE 10:20 PM CDT** by `wrangler deploy` (version `b2f67748`). Verified: `/state now`
>   present, `/log` gated (401), POST /log open. A2 + P2-CLOCKSKEW + P6-LOG + crash panel are LIVE.
> - **Steps 2-3 (web → signovivo.com) — DONE via build 377** (a concurrent tab bumped to 377 and deployed;
>   377 descends from #255, so the P2/SliceC/SliceD web fixes are live — verified `svSyncDecision.js`/
>   `reportCrash` served). **Do NOT re-run** the pages deploy.
> - **Step 4 (native/TestFlight) — build 377 already Delivered** with the fixes baked in. **Do NOT cut a new
>   build** — just update the iPads to 377 and run the 2-device day (#243 repros + Slice B crash floor) at
>   Wednesday practice.
> - **Step 5 (A1 rotation) — DONE 2026-07-05** (Miguel confirmed all directors on 375+). Legacy `12345678840`
>   dropped from prod `TRANSMITTER_CODES`; real code → 200, legacy → 401 verified. Reversible.
>
> The steps below are retained as the record + for future deploys.

---


**What this ships** (all merged to `main`, all currently **git-only — NOT live**):

| Change | PR | Surface | Reaches |
|---|---|---|---|
| P2-SEQ freeze fix + P2-POLL-GAP | #248 | web bundle | congregation phones (pages deploy) + iPads (next TestFlight) |
| P2-CLOCKSKEW (client offset) | #248 | web bundle | ″ (inert until the worker `now` field is live) |
| P2-CLOCKSKEW (`/state now`) | #248 | worker | everyone on `wrangler deploy` |
| A2 rate-limit + seq=0 gate | #246 | worker | everyone on `wrangler deploy` |
| P6-LOG `/log` read gate + POST cap | #252 | worker | everyone on `wrangler deploy` |
| Slice D dashboard "Recent crashes" panel | #253 | worker | the gated dashboard |
| Slice C bridge guard + Slice D `reportCrash()` | #250/#253 | web bundle | phones (pages deploy) + iPads (next TestFlight) |

> **Golden rule (why nothing is live yet): `git merge ≠ deploy`.** Nothing reaches a
> device until an explicit `wrangler` command below. Do this on a **green (non-Mass) day** —
> next event is Wednesday practice. Every change is additive + backward-compatible (old
> bundle ↔ new worker and vice-versa), so the order is safe either way; the order below
> activates P2-CLOCKSKEW soonest.

All commands run from the repo root on the latest `main` (`git checkout main && git pull`).

---

## Step 1 — Deploy the worker (A2 + P6-LOG + `/state now` + dashboard panel)

One worker, all rooms — this is all-or-nothing (there is no per-room canary for worker
*code*; the `?env=staging` isolation is for the *bundle*, Step 2). Every change here is
additive and fail-open, and was proven **10/10** against local `wrangler dev`.

```bash
cd sync-worker && npx wrangler deploy && cd ..
```

**Verify immediately (independent ground truth):**
```bash
# 1) health + protocol
curl -s https://signovivo-sync.4j4982y8jp.workers.dev/health
# 2) P2-CLOCKSKEW: /state must now include a numeric "now" (server epoch seconds)
curl -s https://signovivo-sync.4j4982y8jp.workers.dev/r/alvernia-main/state | grep -o '"now":[0-9]*'
# 3) P6-LOG: GET /log must be 401 WITHOUT the key, 200 WITH it
curl -s -o /dev/null -w '%{http_code}\n' https://signovivo-sync.4j4982y8jp.workers.dev/log            # expect 401
curl -s -o /dev/null -w '%{http_code}\n' 'https://signovivo-sync.4j4982y8jp.workers.dev/log?k=YOUR_FLEET_KEY'  # expect 200
```
- `now` present → clock-skew correction will activate the moment the new bundle ships.
- `401` then `200` → the diagnostic buffer is no longer world-readable.

**Rollback (worker):** instant — `cd sync-worker && npx wrangler rollback` (or redeploy the
prior commit). The worker keeps prior versions.

---

## Step 2 — Canary the web bundle on the isolated staging channel

Proves the new bundle (P2-SEQ freeze fix, poll-gap, bridge guard, `reportCrash`) on an
**isolated Pages preview branch** that physically cannot touch signovivo.com.

```bash
STAGING=1 bash scripts/release.sh
```
This builds the current-version bundle and deploys it to the `staging` preview branch — **no
version bump, no native archive, prod untouched.** It prints a preview URL.

**Verify on the preview URL (or a canary device):**
- Reader boots, page 1 renders (no white screen).
- Append `?selftest` → the readiness card is **GREEN**.
- The green "en vivo" pill follows a director and, when the director stops, **demotes within
  ~90 s** instead of freezing (that's the P2-SEQ fix).
- Point the canary at the staging room to keep it off the live Mass room while testing.

---

## Step 3 — Promote the web bundle to production (signovivo.com)

Web-only prod refresh at a freshly bumped build (no native archive):

```bash
SKIP_NATIVE=1 bash scripts/release.sh
```
Deploys to Pages branch `main` = **signovivo.com**. Bumps `version.json` so the `v<N>` badge,
cache version, and manifest all agree.

**Verify prod:**
```bash
curl -s https://signovivo.com/ | grep -o 'lib/svSyncDecision.js'   # new lib shipped
# open signovivo.com on a phone: reader boots, ?selftest = GREEN, badge = the new v<N>
```

**Rollback (web):** `bash scripts/rollback-web.sh` (read-only; lists deployments + prints the
one-click dashboard rollback). Cloudflare Pages keeps every deployment → instant, no rebuild.

---

## Step 4 — Native build (iPads) — pair with the 2-device day

The web bundle changes reach the iPads only in a new TestFlight build (the native app wraps
the baked bundle). Build 375 (#243 native director fixes) is **already Delivered** but
**device-unverified**. Cut the next native build only after the **2-device day** confirms the
#243 native batch (4 repros in the PR body):

```bash
bash scripts/release.sh    # full: bump + web(prod) + native IPA + deploy prod
# then: open -a Transporter ~/Desktop/SignoVivo-<N>.ipa   → click DELIVER
```
This also re-runs Steps 2-3's prod web deploy at the same build, keeping signovivo.com ==
native == `v<N>`.

---

## Step 5 — A1 secret rotation (do once, when safe)

Drop the legacy compat code `12345678840` from the transmitter secret, **only once no device
depends on it** (verify on the fleet dashboard that all directors are on a build that uses the
real codes):

```bash
cd sync-worker && npx wrangler secret put TRANSMITTER_CODES   # paste the real codes WITHOUT 12345678840
```
Reversible: re-put the secret with the old value if a device still needs it.

---

## Stamp the go-live (house rule)

After each prod step, record the exact **Central-time** go-live in the deploy summary and in
`docs/implementation-log.md` (merge/deploy timestamp = the official moment; note live-confirm
separately if you polled).

## Still pending after this batch
- **M2 Slice B** (native `onContentProcessDidTerminate` / `ErrorUtils`) — device-gated.
- **P6-LOG native half** (`DirectorSyncModule.swift:177`, stop sending device names) —
  device-gated. (The web `dev` id is already an opaque 6-char id, not a name.)
