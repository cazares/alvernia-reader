# Why followers take ~10s to converge when a director is first taken

**Status:** theories for testing, 2026-08-18. Nothing here is confirmed on hardware.
**Scope:** the delay happens ONLY when a director is first taken. Once converged, page turns are
instant. This is a one-time cost paid before Mass — unless a device drops and must re-join mid-Mass.

## What was measured

| Run | Build | Result |
|---|---|---|
| A | 451 | director on 372, followers on 100/200/300. **One follower converged in 1–2s; the other two took 10–20s.** |
| B | 452 | Same shape. iPads lag ~10s on first becoming director. |
| Baseline | ~441–445 | 8.6s median, 9.4s worst, 4/4 converged |

Build 452 contains **zero native changes** vs 448/451 (no `.swift`/`.m`/`.h` differs since the 448
release commit), so neither run measures new mesh code. The tail has now survived builds 439, 441,
446 and 448.

**The shape is the evidence.** One device fast, the rest slow, run after run. That is not a slow
handshake — a slow handshake makes everyone slow. It is a race that one device wins.

## The timing constants that actually exist

From `ios/SignoVivo/DirectorSyncModule.swift`. Several land near 10s, which is why guessing from the
symptom alone cannot separate them:

| Constant | Value |
|---|---|
| `inviteTimeout` | **8s** |
| `followerRetryDelay` | 2s |
| `selfDirectedTimeoutSeconds` | **10s** |
| `discoveryRefreshInterval` | 12s |
| `earlyRefreshInterval` | 5s |
| `browserHealthySeconds` | 20s |
| `followerHelloInterval` | 8s |
| `followerWatchdogInterval` | 0.5s |

`inviteTimeout + followerRetryDelay` = **10s exactly**. So does `selfDirectedTimeoutSeconds`. Two
different mechanisms predict the same number; only a trace separates them.

---

## Theory 1 — The director destroys its own advertiser while followers are inviting it

**This is the leading theory. It is a self-inflicted race, and the code already describes the
symptom in its own words.**

Sequence:

1. `becomeDirector` calls `startNearbyDirector()` — the device begins advertising.
2. Followers' browsers fire `foundPeer` and each calls `browser.invitePeer(...)`.
3. **Immediately after**, `becomeDirector` calls `refreshNearbyDiscovery()`
   ([PdfReaderApp.tsx:841](../PdfReaderApp.tsx#L841)), commented *"SPLIT-BRAIN MITIGATION … Kick an
   immediate re-browse."*
4. That lands in Swift `refreshDiscovery()`, whose first act is
   `advertiser?.stopAdvertisingPeer(); advertiser = nil` — then it rebuilds.
5. Any follower invite in flight at that instant **evaporates silently**. Multipeer logs
   `Cannot find peer with idString […] in the peers dictionary` and returns nothing: no error, no
   delegate callback, no session.
6. The follower sits at `connecting` for the full `inviteTimeout` (8s). The refresh is *held* during
   that window ([DirectorSyncModule.swift:1287](../ios/SignoVivo/DirectorSyncModule.swift#L1287)).
7. The next refresh cycle calls `reconsiderFollowerTarget()`, which retries. **Total ≈ 8–12s.**

The device that converges in 1–2s is simply the one whose invite fired outside the teardown window.

`refreshDiscovery()`'s own comment describes this outcome verbatim:

> *an invite succeeded only when it happened to fire in the same generation that discovered the
> target — which is why exactly one device would follow while the rest sat there, and why it looked
> maddeningly intermittent.*

That comment was written about the ghost-peer bug fixed in 444, which cleared the **follower's**
stale maps on the follower's own refresh. It does not cover this case: here the follower's map is
not stale in the usual sense — the target genuinely vanishes mid-invite because the **director** tore
its advertiser down.

**The split-brain mitigation and the follower convergence delay are the same line of code.**

- **Predicted signature:** convergence times CLUSTER near 8–10s rather than scattering. Device logs
  show `invite:send` followed by silence, then a second `invite:send` ~8–10s later that succeeds.
  The fast device shows one `invite:send` and an immediate `.connected`.
- **Experiment:** grep a device trace for `invite:send` and `refresh:peers-cleared` around a role
  change and compare timestamps. Cheapest possible confirmation — the log lines already exist.
- **Fix sketch:** do not tear down the advertiser on the director at the moment it starts serving.
  Either delay the split-brain re-browse by a couple of seconds, or make `refreshDiscovery()` rebuild
  the **browser only** when the caller's purpose is re-discovery rather than re-advertisement. The
  director needs to *find* other directors; it does not need to stop *being findable* to do that.

## Theory 2 — `selfDirectedTimeoutSeconds` (10s) gates the follower's reaction

A follower runs a 10s timer before declaring `self-directed`. `startSelfDirectedTimer` only re-hunts
when it fires, and it is explicitly guarded to not fire while `pendingInvitePeer != nil`.

- **Predicted signature:** convergence clusters at 10s measured **from follower boot**, not from the
  moment the role was taken. That is the distinguishing test.
- **Experiment:** take the director role at two very different offsets after the followers boot —
  ~2s and ~30s. If the delay tracks boot time, it is this. If it tracks the role change, it is not.
- **Note:** this is cheap to rule out and worth doing early precisely because it predicts the same
  10s as Theory 1 from a completely different cause.

## Theory 3 — `browserHealthySeconds = 20` suppresses the refresh that would rescue the invite

Build 446 added a hold: a follower that has seen *a* director recently refuses to refresh discovery
for up to 20s. If its recent sighting is the **old** director, it waits out the hold before hunting
the new one — and the retry in Theory 1 step 7 is exactly a refresh.

**This "fix" may be causing the tail it was meant to cure.** It shipped in 446 and the number did not
move; it is the right order of magnitude for a 10–20s tail.

- **Predicted signature:** the tail is *worse* when a previous director existed in the same session
  than on a cold start with no prior director at all.
- **Experiment:** measure convergence (a) with no device having ever directed this session, and
  (b) immediately after a different device relinquished the role. Compare.

## Theory 4 — AWDL bring-up when followers have no network

At Mass, followers join **no network** and the director is on cellular, so Multipeer runs over AWDL
peer-to-peer (memory `project_mass_network_reality`). AWDL discovery duty-cycles and is markedly
slower than infrastructure Wi-Fi.

- **Predicted signature:** all-on-LAN is fast; Wi-Fi-off is slow, uniformly, for every follower.
  Uniform slowness would argue AGAINST Theory 1, whose signature is bimodal.
- **Experiment:** run the identical test twice — all devices on home Wi-Fi, then follower Wi-Fi
  **off** entirely. **This is the run that predicts Mass; every other number is optimistic.**
- **Capture note:** with Wi-Fi off the followers cannot send telemetry at all — `dbgLog` is
  fire-and-forget with no queue. Pull device logs over a cable, not the relay.

## Theory 5 — The BLE beacon should be masking this and is not

`BlePageBeacon` exists as a connectionless second channel: the director advertises the page number,
followers scan and render it without any Multipeer session. If it were working, the page would land
in ~1s regardless of how long the mesh took, and this whole delay would be invisible to the choir.

It is not masking it. So either the beacon does not start promptly on becoming director, or
followers are not scanning, or the rendered page is gated behind mesh state.

- **Why this matters most practically:** it is the only theory whose fix helps *even if every other
  theory is also true*. It attacks the symptom the choir actually experiences rather than the
  mechanism.
- **Experiment:** with the mesh deliberately broken (airplane mode on the follower, Bluetooth on),
  take the director role and see whether the page updates at all.

---

## What not to do

**Do not tune another constant.** Builds 446 (`browserHealthySeconds`) and 448 (watchdog 1.0s → 0.5s)
both tried that and neither moved the number. 448 in particular optimized the wrong half: the 0.5s
watchdog accelerates *retrying a handshake with a director already seen*, and a follower whose invite
evaporated is not retrying — it is sitting in `inviteTimeout`.

Capture a trace that shows **why** a slow device is slow before changing any value.

## Test protocol

1. **Control** — all devices on home Wi-Fi, clean starts. Take the role, time each follower.
2. **Mass-realistic** — follower Wi-Fi **off**, director on cellular. Same test. The number that matters.
3. **Spread** — take and drop the role ~10 times, plot the convergence times. Clustering near 8–10s
   points at Theory 1; a roughly uniform 0–12s scatter points at the browse cycle instead.
4. **Boot-offset** — vary the delay between follower boot and role acquisition to test Theory 2.
5. Capture a device trace at the moment a slow run reproduces, and read `invite:send` timestamps first.

**Never run simulators during a measurement.** A sim director (newest token) hijacks every physical
follower into a `connecting → notConnected` loop — observed 3× on 2026-08-15
(memory `feedback_no_sims_during_live_tests`). Kill and shut down all simulators first.
