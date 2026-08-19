import Foundation
import MultipeerConnectivity
import Network
import os
import React
import UIKit

@objc(DirectorSyncModule)
final class DirectorSyncModule: RCTEventEmitter, MCNearbyServiceAdvertiserDelegate, MCNearbyServiceBrowserDelegate, MCSessionDelegate {
  private static let serviceType = "signovivo"
  private static let eventName = "DirectorSyncEvent"
  private static let protocolVersion = 1

  /// PEER BUNDLE PUSH IS GONE — removed entirely in build 434, not merely disabled.
  ///
  /// This rail let a director stream its own `WebBundle` to a follower over Multipeer. It shipped
  /// behind `meshBundlePushEnabled = false` for many builds, which meant a single boolean was the
  /// only thing standing between a stranger in range and persistent code execution on the choir's
  /// iPads. From the project's own audit:
  ///
  ///   "Any device in Bluetooth/Wi-Fi range that advertises role=director on the fixed public
  ///    session code can push an arbitrary web bundle onto every follower iPad ... no auth, no
  ///    signature, no user consent ... the injected HTML/JS runs in a WebView with
  ///    originWhitelist ['*'] + allowUniversalAccessFromFileURLs + allowFileAccess, persisting
  ///    across relaunches."
  ///
  /// It was also the ONLY writer of `Documents/WebBundle` — the directory the boot resolver
  /// prefers forever — and it could push a book BACKWARDS, because its version check compared
  /// CFBundleVersion (the shell's number), which says nothing about which book either device
  /// holds. Books ship over HTTPS with manifest verification instead.
  ///
  /// Deleting the mechanism rather than the switch is the actual fix. `didFinishReceivingResource`
  /// now discards anything a peer sends.
  // Handshake generation — incremented whenever the who-invites-whom rule changes.
  // Advertised in discoveryInfo so peers can see it before connecting.
  // Build ≤226: no "hgen" key → legacy (director initiates).
  // Build ≥310: hgen=2 → modern (follower initiates; director only invites legacy peers).
  private static let handshakeGeneration = "2"
  private static let maxSessionCodeLength = 12
  /// Normal (steady-state) discovery refresh interval.
  // 25 -> 12. A follower that lost the director waited up to a FULL refresh interval before it even
  // looked again; that is the 14 s recovery measured on 2026-08-17 when a backgrounded director
  // returned. Halving it halves the blind window. Not lower: each refresh tears down and rebuilds
  // both transports, so past ~5 s you disrupt discovery more than you perform it.
  private static let discoveryRefreshInterval: TimeInterval = 12
  /// Fast refresh interval used for the first N cycles after starting, so followers find
  /// a late-arriving director within a few seconds rather than up to 25 s.
  private static let earlyRefreshInterval: TimeInterval = 5
  private static let earlyRefreshCycleCount = 12          // 12 × 5 s = 60 s burst window (was 6/30 s)
  // 30 -> 8. THE SINGLE BIGGEST SOURCE OF MEASURED STALENESS. Every failed handshake sat in a
  // penalty box for the full timeout before the follower could even retry:
  //     11:07:53  invite:send
  //     11:08:24  session:notConnected   <- 31 s gone
  // Four consecutive failures cost ~120 s while the device sat on the wrong song. A HEALTHY
  // handshake completes in ~1 s (measured on three iPads), so 8 s is 8x headroom and caps the same
  // four failures at ~32 s. Not lower: a too-short timeout abandons handshakes that would have
  // completed on a marginal link, which trades staleness for churn.
  private static let inviteTimeout: TimeInterval = 8
  /// How long WE wait before assuming an invite died and re-issuing it. NOT the same number as
  /// inviteTimeout, which is how long MULTIPEER holds the invite open.
  ///
  /// They were the same value, so a silently-dropped invite — one that never fires a delegate
  /// callback at all, which is exactly what happens when the target's advertiser restarts — cost a
  /// full 8 seconds of a follower doing nothing before anything retried. That is most of a 10s
  /// convergence, spent waiting out a worst case rather than noticing a failure.
  private static let inviteRetryAfter: TimeInterval = 2.5
  /// How long a follower may hunt, WITH a director in sight, before rebuilding its session.
  ///
  /// Retrying an invite cannot fix a wedged MCSession — same broken session, same result, forever.
  /// The human escape hatch for that is the resync button; this is the same escalation without
  /// needing someone to notice. Deliberately long: a rebuild is disruptive, and normal convergence
  /// must be given room to finish first.
  private static let followerWedgedSeconds: TimeInterval = 20
  /// How recently the browser must have seen a director for it to count as WORKING. Below this,
  /// a scheduled refresh is skipped rather than destroying live discovery. Comfortably longer than
  /// the 5 s early-refresh tick so a healthy hunt is never interrupted, and short enough that a
  /// browser which has genuinely gone deaf is rebuilt within one extra cycle.
  private static let browserHealthySeconds: TimeInterval = 20
  private static let followerRetryDelay: TimeInterval = 2
  private static let followerHelloInterval: TimeInterval = 8
  /// Fast half-open watchdog. The director re-sends the current page every ~1s (mesh heartbeat), so
  /// a CONNECTED follower that hasn't heard ANYTHING in this window is almost certainly on a
  /// half-open link (the director dropped us on its side but MPC hasn't declared .notConnected yet —
  /// which can otherwise take 30-90s, stranding the follower on the wrong song). At that point we
  /// force an immediate reconnect. 3s = ~3 missed heartbeats: tight, but tolerant of 1-2 dropped
  /// packets so a momentary blip doesn't churn the connection.
  private static let followerStaleReconnectSeconds: TimeInterval = 3.0
  /// On foreground a follower must PROVE its session is alive rather than assume it. See the note
  /// in handleAppDidBecomeActive.
  private static let foregroundVerifySeconds: TimeInterval = 2.0
  /// 1.0 -> 0.5 s (owner, 2026-08-17: "it needs to feel snappy").
  ///
  /// This tick does two jobs and both want to be fast. While CONNECTED it is the half-open
  /// watchdog; while HUNTING it re-attempts the handshake, which is where the felt latency lives —
  /// live telemetry measured 4/4 followers converging in 4.6-9.4 s against a standard of "longer
  /// than a few seconds is a failure".
  ///
  /// Safe to halve because the tick does no work of its own: reconsiderFollowerTarget returns
  /// immediately when connected, and while an invite is inside its window it emits "connecting" and
  /// returns WITHOUT issuing a parallel invite. Doubling the rate doubles the number of times we ask
  /// "should I act yet?", not the number of invites. It never touches the transports — a fast tick
  /// that restarted the browser is the discovery storm, and a test forbids it.
  private static let followerWatchdogInterval: TimeInterval = 0.5
  /// One-shot snapshot-recovery probe delay. MPC can drop the first reliable send right at
  /// .connected, so if the director's proactive snapshot AND the follower's first hello both
  /// land in that fragile window, the follower would otherwise wait a full followerHelloInterval
  /// (8 s) for the next hello. This probe re-requests the snapshot ~1.5 s after connect when no
  /// page has arrived yet, so a joining/reconnecting follower snaps to the director's page fast.
  private static let followerSnapshotProbeDelay: TimeInterval = 1.5
  /// Seconds a follower waits for a director before entering self-directed mode.
  private static let selfDirectedTimeoutSeconds: TimeInterval = 10
  private static let maxInboundPayloadBytes = 8 * 1024
  /// ONE FOLLOWER PER SESSION, ON PURPOSE (Miguel, 2026-08-18: "nuke peer sharing and any peer
  /// connections... it should be one director to one follower... that setup times N followers").
  ///
  /// MCSession is not a star topology by choice — it is Apple's framework behavior: every peer
  /// joined to the SAME MCSession object is directly connected to EVERY OTHER peer in it (full
  /// mesh), not just to whoever invited them. There is no "broadcast-only" mode. The comment this
  /// replaced ("7 followers in one session still all cross-connect") already documented this as a
  /// known, unfixed property — followers really could see each other's traffic at the protocol
  /// level, not just the director's.
  //
  // Hunted down (2026-08-18) as the likely cause of a repeated, hard-to-pin bug: a follower whose
  // OWN device navigated to a song WHILE nobody was directing showed up as a phantom page source
  // for the OTHER followers once a new director took over — three followers converged on the
  // SAME wrong song (matching whichever device had most recently touched a page), self-corrected
  // after ~10s once the real director's session stabilized. Consistent with cross-session/
  // cross-peer bleed that a strict 1-follower-per-session topology makes structurally impossible:
  // a session with exactly 2 members (director + one follower) has no OTHER follower in it to
  // bleed from.
  //
  // Was 7 (Apple's per-session hard cap is 8 peers including local, so 7 followers/session).
  private static let maxFollowersPerSession = 1
  /// Was 2 (7-per-session × 2 = 14). Raised to keep real fleet capacity headroom (measured fleet:
  /// 1 director + ~6-8 followers) now that each session holds only 1 follower instead of 7.
  private static let maxSessions = 12

  private var localPeerID: MCPeerID?
  /// Director uses up to maxSessions instances; follower uses exactly one (mcSessions[0]).
  private var mcSessions: [MCSession] = []
  private var advertiser: MCNearbyServiceAdvertiser?
  private var browser: MCNearbyServiceBrowser?
  private var currentRole = "off"
  private var currentSessionCode = ""
  private var currentDirectorToken = ""
  private var discoveredDirectors: [MCPeerID: String] = [:]
  private var discoveredDirectorSeenAt: [MCPeerID: TimeInterval] = [:]
  /// Full discoveryInfo keyed by peer — used to detect legacy directors (no "hgen" key).
  private var discoveredDirectorInfo: [MCPeerID: [String: String]] = [:]
  private var discoveredFollowers: Set<MCPeerID> = []
  /// Full discoveryInfo for discovered followers — used to detect legacy followers (no "hgen" key).
  private var discoveredFollowerInfo: [MCPeerID: [String: String]] = [:]
  private var pendingInvitePeer: MCPeerID?
  private var pendingInviteTimestamp: TimeInterval = 0
  /// Consecutive failed-connect count per candidate director, so a STALE entry in
  /// discoveredDirectors can be evicted instead of re-targeted forever.
  ///
  /// Confirmed on real 4-device hardware (2026-08-19): a device that demoted itself from director
  /// back to follower stayed in every OTHER follower's discoveredDirectors (only lostPeer clears
  /// that dict, and the demoted device never left range) — and its token, being the most recent, kept
  /// SORTING FIRST. Two followers spent 90+ seconds firing invite:send at it every 300-700ms, each
  /// invite rejected in didReceiveInvitationFromPeer (why=not-a-director) and immediately retried,
  /// while the REAL, live, foregrounded director sat undiscovered nearby the entire time. The
  /// rejection round-trip is far faster than a genuine timeout, so the normal retry-after-timeout
  /// backoff never engaged — this is a distinct failure mode from a slow/dead peer, not the same
  /// bug the backoff already covers.
  private var invalidDirectorStreak: [MCPeerID: Int] = [:]
  private static let invalidDirectorEvictThreshold = 2
  private var connectedDirectorPeer: MCPeerID?
  private var discoveryRefreshTimer: Timer?
  private var earlyRefreshCyclesRemaining: Int = 0
  private var selfDirectedTimer: Timer?
  private var followerHelloTimer: Timer?
  private var followerWatchdogTimer: Timer?
  private var lastFollowerHelloAt: TimeInterval = 0
  private var lastFollowerPageReceivedAt: TimeInterval = 0
  /// When this follower started hunting with nothing connected. 0 means "not hunting".
  private var followerHuntingSince: TimeInterval = 0
  /// Keeps the BLE radios alive independently of every mesh timer. See startBleHealthTimer.
  private var bleHealthTimer: Timer?
  private var currentPageNumber: Int?
  private var currentTotalPages: Int = 0
  private var currentMode = ""
  private var currentBookId = ""
  private var resetGeneration = UUID()
  private var pendingTakeoverRequests: [String: MCPeerID] = [:]
  private var pendingTakeoverTimers: [String: Timer] = [:]
  /// Dedup guard: skip emitting state events whose status and peerCount haven't changed.
  private var lastEmittedStatus: String = ""
  private var lastEmittedPeerCount: Int = -1
  /// Exponential back-off counters for repeated advertiser/browser launch failures.
  private var advertiserFailureCount: Int = 0
  private var browserFailureCount: Int = 0
  /// Current device thermal state — used to back off discovery refresh rate when hot.
  private var thermalState: ProcessInfo.ThermalState = .nominal
  /// When backgrounded, avoid churny discovery timers (which can exacerbate memory/CPU pressure).
  private var appIsActive: Bool = true
  /// Keeps the process alive briefly after backgrounding so the advertiser survives a glance at a
  /// notification. See beginBackgroundGrace.
  private var backgroundTaskID: UIBackgroundTaskIdentifier = .invalid
  /// MEASUREMENT PROBE (build 433). A connectionless BLE page channel running ALONGSIDE the mesh,
  /// observing only — it renders nothing and changes no behaviour. It exists to answer the one
  /// question every proposed rewrite depends on: how fast does a page reach a follower when there
  /// is no handshake to fail? Pair `ble:page-send` with `ble:page-recv` in stress-analyze.
  private let bleBeacon = BlePageBeacon()
  /// Book context last seen from a MESH page. BLE carries only a page number, so it renders only
  /// once we know which book that number refers to.
  private var lastKnownTotalPages = 0
  private var lastKnownMode = ""
  /// THE ONLY BOOK. Pinned, not discovered.
  ///
  /// This was "" until a MESH page arrived, and the BLE handler refused to apply anything while it
  /// was empty — so the connectionless channel could only ever work AFTER the handshake channel had
  /// already succeeded. That inverted the whole point of having it. BLE needs no browse, no invite,
  /// no session and no peer identity, which is to say none of the machinery that failed all day on
  /// 2026-08-17; and it is the faster one on the numbers that matter (measured across five devices:
  /// BLE 0.10-0.18 s median, 0.83 s WORST, versus a mesh worst case of 112 s). Gating it behind the
  /// mesh meant the reliable path was switched off exactly when the unreliable one broke.
  ///
  /// The gate guarded a real ambiguity once: with two books a bare page number was meaningless, and
  /// guessing wrong yanked a follower onto the wrong hymnal. That ended on 2026-07-02 — the app has
  /// been single-book since, pinned in PdfReaderApp.tsx ("the only book is the standard (Alvernia)
  /// manual"), in web/src/app.js (`currentBook: "standard"`), and in the manifest, which ships one
  /// book of 372 pages. A page number is now unambiguous, so the guard protects nothing and costs
  /// the whole fallback.
  ///
  /// Still updated from mesh pages below — if a second book ever returns, this is the one line to
  /// revisit, and the BLE payload would need to carry the book id before it could stay ungated.
  /// REVERTED 2026-08-17, same night it shipped. This was pinned to "standard" so the BLE channel
  /// could render without waiting for a mesh page — correct in principle (BLE has no handshake and
  /// is the faster path) and WRONG as shipped, because BLE has no freshness guarantee.
  ///
  /// Observed on the owner's fleet within minutes of build 444: every device rendered song 357,
  /// then corrected to 101. A cached/leftover BLE advertisement from an earlier session was applied
  /// because the monotonic guard cannot reject it — `bleSeq` restarts at 0 on every director launch
  /// and a follower's `bleAppliedSeq` starts at -1, so the FIRST reading after launch always passes
  /// whatever its age. The mesh then corrected it (see "mesh is authoritative" below), which is why
  /// it presented as a flash of the wrong song rather than a stuck one.
  ///
  /// Persisting the seq does not fix it: seq is PER-DEVICE, so a follower that had applied 1743
  /// from one director would reject a different director starting at 13, forever. Making BLE safe
  /// to render first needs a director-scoped nonce with a freshness window, in a payload iOS limits
  /// to two fields — a real design, not a one-line pin.
  ///
  /// So the gate returns: empty until a mesh page establishes context. BLE stays a fast follower of
  /// the mesh rather than an independent source. A wrong song on screen in front of a congregation
  /// is worse than a slow one.
  /// Pinned to the single book so BLE can render WITHOUT waiting for a mesh page.
  ///
  /// Build 444 tried this and had to be reverted the same night: devices flashed song 357 before
  /// correcting to 101. The cause was NOT the pin — it was that BlePageBeacon.stopPublishing()
  /// existed but was never called, so a device that had once directed advertised its last page
  /// forever, and followers read that ghost. resetTransport now stops the beacon on every role
  /// change, becoming a follower stops it explicitly, and the advertisement carries a per-session
  /// nonce so a scanner rebases instead of mis-ordering seqs across sessions. Legacy two-field
  /// advertisements (builds 433-445, which never stop) are rejected outright.
  ///
  /// With no stale beacon possible, the reason for the mesh-first gate is gone — and the gate was
  /// costing the entire fallback. BLE has no handshake, which is where EVERY failure found on
  /// 2026-08-17 lived, and it is the faster path on the number that matters: 0.83 s worst case
  /// across five devices, against a mesh worst case of 112 s.
  ///
  /// Still updated from mesh pages below; if a second book ever returns, this pin is wrong and the
  /// BLE payload must carry a book id.
  private var lastKnownBookId = "standard"
  private var bleLastSeenSeq = -1
  private var bleAppliedSeq = -1
  /// The advertising session bleAppliedSeq belongs to. WITHOUT THIS, BLE NEVER HELPED ANYONE.
  /// bleSeq restarts at 0 on every director launch, so a follower holding bleAppliedSeq = 50 from a
  /// previous director rejected seq 1, 2, 3 … from the next one and stayed silent for ~50 page turns
  /// — indistinguishable from BLE being switched off, which is how it looked on the fleet.
  /// BlePageBeacon already rebased on a new nonce internally (build 448); this second guard, one
  /// layer up, never learned about nonces. The fix was half-landed for four builds.
  private var bleAppliedNonce = ""

  // ── Telemetry batching state (build 436) ─────────────────────────────────────
  /// Serial queue guarding `logBuffer` + `logSuspended`. dbgLog is called from MCSession and
  /// MCNearbyServiceBrowser delegate callbacks, which arrive on arbitrary threads.
  private let logQueue = DispatchQueue(label: "com.cazares.signovivo.dbglog")
  private var logBuffer: [[String: Any]] = []
  private var logSuspended = false
  /// Timer + interval are touched on MAIN ONLY (Timer needs a run loop, and keeping the interval
  /// main-only avoids a second lock). The relay can retune both — see applyLogPolicy.
  private var logFlushTimer: Timer?
  private var logFlushInterval: TimeInterval = 15

  /// The running app's build number. Rides on every outbound page payload so a follower can see
  /// which shell its director is on; it no longer gates any transfer.
  private var currentBundleVersion: String {
    Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "0"
  }

  /// after install succeeds/fails. Prevents duplicate requests from repeated offers.

  // MARK: - Convenience

  private var allConnectedPeers: [MCPeerID] {
    mcSessions.flatMap { $0.connectedPeers }
  }

  /// Returns an existing session with room, or creates a new one (up to maxSessions).
  private func availableSessionForNewFollower() -> MCSession? {
    if let s = mcSessions.first(where: { $0.connectedPeers.count < Self.maxFollowersPerSession }) {
      return s
    }
    guard mcSessions.count < Self.maxSessions, let peerID = localPeerID else { return nil }
    let s = MCSession(peer: peerID, securityIdentity: nil, encryptionPreference: .none)
    s.delegate = self
    mcSessions.append(s)
    return s
  }

  private func pagePayload(page: Int, totalPages: Int) -> Data? {
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "page",
      "page": max(1, page),
      "totalPages": max(0, totalPages),
      "mode": currentMode,
      "bookId": currentBookId,
      "bundleVersion": currentBundleVersion,
    ]
    return try? JSONSerialization.data(withJSONObject: payload)
  }

  private func helloPayload() -> Data? {
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "hello",
      "ts": Int(Date().timeIntervalSince1970),
    ]
    return try? JSONSerialization.data(withJSONObject: payload)
  }

  private func parseInboundPayload(_ data: Data) -> [String: Any]? {
    // Protect against pathological payload sizes (should never happen for our protocol).
    guard data.count > 0, data.count <= Self.maxInboundPayloadBytes else { return nil }
    guard let obj = try? JSONSerialization.jsonObject(with: data) else { return nil }
    return obj as? [String: Any]
  }

  private func sendControlPayload(_ obj: [String: Any], to peerID: MCPeerID) {
    guard let data = try? JSONSerialization.data(withJSONObject: obj) else { return }
    for session in mcSessions {
      if session.connectedPeers.contains(peerID) {
        try? session.send(data, toPeers: [peerID], with: .reliable)
        return
      }
    }
  }

  // MARK: - Remote sync telemetry (CF /log)
  //
  // BATCHED since build 436. Breadcrumbs are buffered and flushed together on a timer instead of
  // one HTTP POST per mesh event.
  //
  // WHY: on 2026-08-17 this took production down. The account tripped Cloudflare's free-plan
  // Workers cap (100,000 requests/day) and BOTH signovivo.com and the relay returned 429/1027 for
  // hours. Measured that day:
  //
  //     account-wide      99,428 / 100,000     <- the cap, hit at ~14:00 CT
  //     signovivo-sync    87,258   (87.8%)
  //     ~10 other sites   ~12,000  (23 workers, 39 zones, 9 Pages projects)
  //
  // One POST per event has no ceiling: it scales with how badly the mesh is behaving, so the
  // instrument shouts loudest exactly when something is wrong. One follower hunting for a director
  // logged ~150 `found` events in two minutes.
  //
  // WHY BATCHING AND NOT SAMPLING: replaying that day's real captures through
  // scripts/telemetry-budget-sim.mjs —
  //
  //     batch 15s                          4.8x fewer requests   100% of rows kept
  //     batch 15s + coalesce               4.8x                    51%
  //     batch 30s + coalesce + sampling    8.6x                    34%
  //
  // Sampling saves ~1% more and costs two thirds of the evidence, because the request count is set
  // by how many WINDOWS contain an event, not by how many events. Telemetry is how every mesh bug
  // this week was found — including the follower-takes-a-follower-for-the-director bug that broke
  // sync from build 381 through 429. It gets throttled here, never dropped.
  //
  // Best-effort throughout; never blocks or affects sync.

  /// Flush early when a burst fills the buffer, so a storm still reports promptly instead of
  /// waiting out the timer. Well under the relay's 200-entry LOG_MAX_BATCH.
  private static let logFlushThreshold = 60
  /// Hard ceiling on the buffer. AT MASS THE FOLLOWERS ARE ON NO NETWORK AT ALL — every flush
  /// fails and nothing drains — so an unbounded buffer would grow for the whole hour. Oldest rows
  /// are dropped first: the newest breadcrumbs are the ones that explain a failure.
  private static let logBufferCap = 300

  /// EVERY BREADCRUMB ALSO GOES TO THE DEVICE'S OWN LOG.
  ///
  /// Until 2026-08-17 this module produced ZERO local output — no os_log, no NSLog, no print. Every
  /// breadcrumb it has ever written went to Cloudflare and nowhere else. Two consequences, and the
  /// second one is the expensive one:
  ///
  ///   1. When the relay is unreachable the fleet is MUTE. That is not hypothetical: the account
  ///      tripped its Cloudflare daily cap that afternoon and every device went silent for hours,
  ///      mid-investigation.
  ///   2. AT MASS THE FOLLOWERS ARE ON NO NETWORK AT ALL. So in the one setting the whole app
  ///      exists for, no follower has ever been able to say what it did. The handoff records the
  ///      cost plainly — "the fix cannot be confirmed in the field" — and a day was spent inferring
  ///      causes from an absence of evidence that was guaranteed by construction.
  ///
  /// os_log fixes both for free. It needs no network, no relay, no key and no UI: attach the device
  /// and `log stream --predicate 'subsystem == "com.cazares.signovivo"'`, or pull a sysdiagnose
  /// AFTER a Mass and read the whole account offline. It costs zero Cloudflare requests, so it is
  /// unaffected by the telemetry batching, and it survives whatever the quota is doing.
  ///
  /// `.public` is deliberate. os_log redacts interpolated strings by default, which would render
  /// every peer name and page number as <private> — a log that cannot be read is the problem this
  /// is solving, not a solution to it. The values are opaque peer ids, roles and page numbers on
  /// the owner's own devices; there is nothing here that is not already on the screen.
  private static let deviceLog = Logger(subsystem: "com.cazares.signovivo", category: "mesh")

  private func dbgLog(_ event: String, _ data: [String: Any] = [:]) {
    // Build the payload on the CALLING thread, exactly as the unbatched version did, so the role /
    // peer name / build recorded are the ones true at the moment of the event rather than at flush.
    var payload: [String: Any] = [
      "t": Int(Date().timeIntervalSince1970 * 1000),
      "dev": localPeerID?.displayName ?? "?",
      "role": currentRole,
      "src": "swift",
      "build": currentBundleVersion,
      "event": event,
    ]
    payload.merge(data) { _, new in new }

    // Local first, and UNCONDITIONALLY — before the batching queue, before the suspend check, and
    // outside anything the relay or the kill switch can turn off. Silencing the fleet's network
    // telemetry must never also blind the device in front of you.
    let extras = data
      .map { "\($0.key)=\($0.value)" }
      .sorted()
      .joined(separator: " ")
    // .notice, NOT .info. os_log's .info and .debug levels are MEMORY-ONLY by default: they are
    // visible in a live Console.app stream but are never written to the persistent store, so
    // `log collect` and `sysdiagnose` return nothing. Build 438 shipped this as .info, which made
    // the whole channel useless for the exact case it was built for — reading a device AFTER the
    // fact, offline, when it was never on a network. Confirmed by converting a real 440 archive:
    // zero com.cazares.signovivo entries. .notice is the lowest level that persists by default.
    Self.deviceLog.notice(
      "\(self.currentRole, privacy: .public) \(self.localPeerID?.displayName ?? "?", privacy: .public) \(event, privacy: .public) \(extras, privacy: .public)"
    )

    logQueue.async { [weak self] in
      guard let self, !self.logSuspended else { return }
      self.logBuffer.append(payload)
      if self.logBuffer.count > Self.logBufferCap {
        self.logBuffer.removeFirst(self.logBuffer.count - Self.logBufferCap)
      }
      if self.logBuffer.count >= Self.logFlushThreshold { self.flushLogOnQueue() }
    }
    ensureLogFlushTimer()
  }

  /// Starts the repeating flush timer once. Main-only (Timer needs a run loop).
  private func ensureLogFlushTimer() {
    DispatchQueue.main.async { [weak self] in
      guard let self, self.logFlushTimer == nil, self.logFlushInterval > 0 else { return }
      self.logFlushTimer = Timer.scheduledTimer(
        withTimeInterval: self.logFlushInterval, repeats: true
      ) { [weak self] _ in
        self?.logQueue.async { self?.flushLogOnQueue() }
      }
    }
  }

  /// MUST be called on `logQueue`. Drains the buffer into one POST.
  private func flushLogOnQueue() {
    guard !logSuspended, !logBuffer.isEmpty else { return }
    let batch = logBuffer
    logBuffer.removeAll(keepingCapacity: true)
    guard let url = URL(string: "https://signovivo-sync.4j4982y8jp.workers.dev/log"),
          let body = try? JSONSerialization.data(withJSONObject: batch) else { return }
    var req = URLRequest(url: url)
    req.httpMethod = "POST"
    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
    req.httpBody = body
    // The batch is NOT re-queued on failure. At Mass every POST fails (no network) and retrying
    // would rebuild the unbounded backlog this change exists to prevent.
    URLSession.shared.dataTask(with: req) { [weak self] data, _, _ in
      guard let self, let data,
            let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
      else { return }
      self.applyLogPolicy(obj)
    }.resume()
  }

  /// Adopt the flush interval the relay asks for. THE KILL SWITCH THAT DID NOT EXIST on 2026-08-17:
  /// with the fleet already fielded there was no way to stop the traffic that had taken production
  /// down, so it simply ran until UTC midnight. Now `LOG_INTERVAL_MS` in the Worker retunes every
  /// device on its next flush — and "0" silences them — with no TestFlight round-trip.
  private func applyLogPolicy(_ response: [String: Any]) {
    guard let ms = (response["logIntervalMs"] as? NSNumber)?.doubleValue else { return }
    let next = max(0, ms / 1000)

    logQueue.async { [weak self] in
      guard let self else { return }
      let suspend = (next == 0)
      guard suspend != self.logSuspended else { return }
      self.logSuspended = suspend
      if suspend { self.logBuffer.removeAll(keepingCapacity: false) }
    }

    guard next > 0 else { return }
    DispatchQueue.main.async { [weak self] in
      guard let self, abs(next - self.logFlushInterval) > 0.001 else { return }
      self.logFlushInterval = next
      self.logFlushTimer?.invalidate()
      self.logFlushTimer = nil
      self.ensureLogFlushTimer()
    }
  }

  /// DIRECTOR → every currently-connected peer. Announces this device's director token so any
  /// connected peer that is ALSO directing (split-brain) can demote immediately via
  /// handleDirectorConflict, instead of waiting up to a full ~25 s browser-refresh cycle to
  /// notice the higher token through discovery. Best-effort reliable control send per session.
  private func broadcastDirectorAnnounce() {
    guard currentRole == "director", !currentDirectorToken.isEmpty else { return }
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "director_announce",
      "token": currentDirectorToken,
    ]
    guard let data = try? JSONSerialization.data(withJSONObject: payload) else { return }
    for session in mcSessions where !session.connectedPeers.isEmpty {
      try? session.send(data, toPeers: session.connectedPeers, with: .reliable)
    }
  }

  /// DIRECTOR → a single peer. Same intent as broadcastDirectorAnnounce, but targeted at one
  /// freshly-connected peer (e.g. another director that just cross-connected) so it demotes fast.
  private func sendDirectorAnnounce(to peerID: MCPeerID) {
    guard currentRole == "director", !currentDirectorToken.isEmpty else { return }
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "director_announce",
      "token": currentDirectorToken,
    ]
    sendControlPayload(payload, to: peerID)
  }

  private func sendCurrentPageSnapshot(to peerID: MCPeerID, via session: MCSession) {
    // Send NOTHING until the director has an actual page (and therefore a real book context).
    // currentBookId/currentMode are only set alongside currentPageNumber in sendPageUpdate, so a
    // nil page means we'd ship bookId=""/mode="" — which the follower's JS (bookFromSync) hard-
    // coerces to the DEFAULT book "hymns-4", yanking a correctly geo-resolved "standard" (Del Rio)
    // follower off its book onto hymns-4 page 1. The 2s mesh heartbeat + 1.5s snapshot-probe + 8s
    // hello all re-pull the real snapshot the instant the director navigates, so the nil window
    // loses nothing. A page-1-with-empty-book guess is a wrong guess — never broadcast it.
    guard currentRole == "director", let page = currentPageNumber,
          let data = pagePayload(page: page, totalPages: currentTotalPages) else {
      return
    }
    do {
      try session.send(data, toPeers: [peerID], with: .reliable)
    } catch {
      try? session.send(data, toPeers: [peerID], with: .unreliable)
    }
  }

  override init() {
    super.init()
    thermalState = ProcessInfo.processInfo.thermalState
    appIsActive = UIApplication.shared.applicationState != .background
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(handleMemoryWarning),
      name: UIApplication.didReceiveMemoryWarningNotification,
      object: nil
    )
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(handleAppDidEnterBackground),
      name: UIApplication.didEnterBackgroundNotification,
      object: nil
    )
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(handleAppDidBecomeActive),
      name: UIApplication.didBecomeActiveNotification,
      object: nil
    )
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(handleThermalStateChange),
      name: ProcessInfo.thermalStateDidChangeNotification,
      object: nil
    )
  }

  @objc private func handleMemoryWarning() {
    DispatchQueue.main.async {
      self.sendEvent(withName: Self.eventName, body: [
        "type": "memoryWarning",
        "role": self.currentRole,
      ] as [String: Any])
    }
  }

  @objc private func handleThermalStateChange() {
    thermalState = ProcessInfo.processInfo.thermalState
  }

  /// THE DIRECTOR GOING DARK IS THE WHOLE FAILURE. iOS suspends a backgrounded app and
  /// MCNearbyServiceAdvertiser dies with it, so the director simply stops existing as far as every
  /// follower is concerned — there is no client-side fix, because the peer they are hunting for is
  /// genuinely not there. Measured on five devices 2026-08-17: the director was backgrounded for
  /// ~2.5 minutes and a follower logged ~150 `found` events in that window, every one of them for a
  /// FELLOW FOLLOWER and never once for the director. It reconnected 14 s after the director
  /// returned. The followers were behaving perfectly the entire time.
  ///
  /// Apple provides no background mode for MultipeerConnectivity, so "advertise forever" is not
  /// available. What IS available is ~30 seconds of continued execution via a background task,
  /// which covers the cases that actually happen to a director mid-Mass: glancing at a
  /// notification, a Control Center pull, a brief app switch. Inside that window the process keeps
  /// running, so the advertiser keeps advertising and no follower ever notices.
  ///
  /// It does NOT survive a real phone call — this is a cellular iPad — and it is not meant to.
  /// That case needs a BLE background transport, which is a separate piece of work.
  ///
  /// DIRECTOR ONLY — build 431 applied this to every role and that was a REGRESSION. A follower
  /// backgrounding now stayed alive ~30 s with a live MCSession while handleAppDidEnterBackground
  /// had already stopped its watchdog, hello timer and discovery refresh, then got killed abruptly
  /// when iOS reclaimed the task. The old fast-suspend path was a clean break that reconnected on
  /// return. Measured on one device across the 430 -> 431 boundary in a single session:
  ///
  ///                  invites   connected   dropped   pages received
  ///     build 430          8          16        11            2,185
  ///     build 431         16          10        18              423
  ///
  /// Twice the invites, more drops than connects, a fifth of the pages. The trace shows the shape
  /// exactly: bg:grace-begin, 26 s of limbo, bg:grace-expired, then four session:notConnected in
  /// the same second.
  ///
  /// A follower going quiet for 30 s costs nobody anything — it reconnects when it comes back. The
  /// DIRECTOR going quiet strands the entire choir, which is the only reason this exists. Role, not
  /// device class: there is no iPhone/iPad branching anywhere in this repo and there must not be.
  private func beginBackgroundGrace() {
    guard currentRole == "director", backgroundTaskID == .invalid else { return }
    backgroundTaskID = UIApplication.shared.beginBackgroundTask(withName: "signovivo-mesh") { [weak self] in
      // Expiry handler: iOS is reclaiming us. End the task or the app is killed outright.
      self?.dbgLog("bg:grace-expired")
      self?.endBackgroundGrace()
    }
    dbgLog("bg:grace-begin", ["role": currentRole])
  }

  private func endBackgroundGrace() {
    guard backgroundTaskID != .invalid else { return }
    UIApplication.shared.endBackgroundTask(backgroundTaskID)
    backgroundTaskID = .invalid
  }

  @objc private func handleAppDidEnterBackground() {
    appIsActive = false
    beginBackgroundGrace()
    // Flush telemetry NOW. Timers do not fire once suspended, so whatever is still buffered would
    // otherwise sit there until the app returns — and the breadcrumbs explaining a backgrounded
    // director (bg:grace-begin/expired, the 431 regression trace) are precisely the ones written
    // in this window. The background-task grace above is what keeps us alive long enough to send.
    logQueue.async { [weak self] in self?.flushLogOnQueue() }
    // Keep existing MCSession connections as-is, but stop any periodic churn.
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = nil
    cancelSelfDirectedTimer()
    stopFollowerHelloTimer()
    stopFollowerWatchdog()
  }

  @objc private func handleAppDidBecomeActive() {
    appIsActive = true
    endBackgroundGrace()
    guard currentRole != "off" else { return }

    // BLE recovers on foreground far more cleanly than the mesh does, because there is no session
    // to rebuild — the director simply advertises again and the follower simply scans again, and
    // the CURRENT page is in the packet. No invite, no handshake, nothing that can half-succeed.
    bleBeacon.resumeOnForeground()

    // Fix: the advertiser/browser silently stop retrying after 5 consecutive launch failures
    // (e.g. Local Network permission was denied, then granted in Settings). Give them ONE more
    // shot on foreground — the user may have just fixed permissions. Reset the counter so the
    // back-off ladder starts fresh, then relaunch whichever transport had given up.
    if advertiserFailureCount > 5 {
      advertiserFailureCount = 0
      advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
      startAdvertising()
    }
    if browserFailureCount > 5 {
      browserFailureCount = 0
      browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
      startBrowsing()
    }

    if currentRole == "follower" {
      if connectedDirectorPeer != nil {
        pauseDiscoveryRefreshWhileConnected()
        startFollowerHelloTimer()
        // VERIFY, DO NOT ASSUME. A full grace reset trusts `connectedDirectorPeer` on a session
        // that may have died while we slept — and a follower that wakes holding a DEAD session has
        // also just cancelled its own discovery (pauseDiscoveryRefreshWhileConnected above), so
        // nothing is looking for anyone. That is the shape of the two iPads found stuck on song 59
        // on 2026-08-17, still reading SIGUIENDO.
        //
        // Instead of a full 3 s grace, leave only foregroundVerifySeconds of it. A live director's
        // 1 Hz heartbeat refreshes this within one beat, so a healthy link is unaffected; a dead one
        // trips the watchdog ~2 s after wake, which clears the stale peer and resumes discovery.
        // Bounded staleness on wake instead of unbounded.
        lastFollowerPageReceivedAt = Date().timeIntervalSince1970
          - max(0, Self.followerStaleReconnectSeconds - Self.foregroundVerifySeconds)
        startFollowerWatchdog()
        sendFollowerHelloIfNeeded()
      } else {
        startDiscoveryRefreshTimer()
        startSelfDirectedTimer()
      }
    } else if currentRole == "director" {
      // RE-ADVERTISE IMMEDIATELY WHEN WE CAME BACK TO AN EMPTY ROOM. If the suspend outlasted the
      // background grace, iOS tore our advertiser down and followers have been searching for a peer
      // that no longer exists. Waiting up to discoveryRefreshInterval (25 s) for the next refresh
      // tick to rebuild it is the difference measured on 2026-08-17 between a 14 s recovery and a
      // ~1 s one, with the whole choir stranded for the gap.
      //
      // ONLY when there is nobody left. Tearing down a LIVE advertiser drops it out from under
      // already-connected followers and aborts any in-flight invite — that is the outage the
      // STABILITY PROTECTION block in scheduleNextDiscoveryRefresh exists to prevent, and this must
      // not reintroduce it. With peers still attached the advertiser is demonstrably working, so
      // there is nothing to fix.
      if allConnectedPeers.isEmpty {
        dbgLog("advertiser:foreground-restart")
        advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
        startAdvertising()
      }
      startDiscoveryRefreshTimer()
      // After an iOS suspend, MPC may have silently dropped reliable sends to existing followers,
      // leaving them stuck on a stale page (half-dead mesh). Proactively re-push the current
      // snapshot to EVERY connected peer — not just newly-connecting ones — so they re-sync on
      // resume. Re-announce our token too in case a split-brain formed while we were backgrounded.
      for session in mcSessions where !session.connectedPeers.isEmpty {
        for peerID in session.connectedPeers {
          sendCurrentPageSnapshot(to: peerID, via: session)
        }
      }
      broadcastDirectorAnnounce()
    }
  }

  deinit {
    NotificationCenter.default.removeObserver(self)
  }

  override static func requiresMainQueueSetup() -> Bool { false }

  override func supportedEvents() -> [String]! { [Self.eventName] }

  // MARK: - JS-exposed methods

  @objc(startDirector:resolver:rejecter:)
  func startDirector(
    _ sessionCode: String,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    let normalizedSessionCode = Self.normalizeSessionCode(sessionCode)
    guard !normalizedSessionCode.isEmpty else {
      reject("DIRECTOR_SESSION_INVALID", "Ingresa un código de sesión válido.", nil)
      return
    }
    DispatchQueue.main.async {
      if self.currentRole == "follower", self.connectedDirectorPeer != nil {
        reject("DIRECTOR_TAKEOVER_REQUIRED", "Ya hay un director conectado. Solicita permiso para tomar control.", nil)
        return
      }
      self.resetTransport(emitState: false)
      self.currentRole = "director"
      self.currentSessionCode = normalizedSessionCode
      self.currentDirectorToken = Self.randomToken()
      self.bleBeacon.sessionCode = normalizedSessionCode
      self.bleBeacon.primeRadios()   // belt-and-braces: a director that never passed through startFollower
      self.configureTransport()
      self.startBleHealthTimer()
      self.startAdvertising()
      self.startBrowsing()
      self.startDiscoveryRefreshTimer()
      // Split-brain mitigation: tell every peer still connected through this device that we are
      // now the director carrying this token, so a peer that is ALSO directing demotes at once
      // via handleDirectorConflict rather than waiting for the ~25 s browser cycle to catch the
      // higher token. resetTransport() above tears down prior sessions, so this is usually a
      // no-op at this instant; the per-peer announce on .connected (below) covers the live case.
      self.broadcastDirectorAnnounce()
      self.emitState(status: "advertising")
      resolve(["role": "director", "sessionCode": normalizedSessionCode])
    }
  }

  @objc(startFollower:resolver:rejecter:)
  func startFollower(
    _ sessionCode: String,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    let normalizedSessionCode = Self.normalizeSessionCode(sessionCode)
    guard !normalizedSessionCode.isEmpty else {
      reject("DIRECTOR_SESSION_INVALID", "Ingresa un código de sesión válido.", nil)
      return
    }
    DispatchQueue.main.async {
      self.resetTransport(emitState: false)
      self.currentRole = "follower"
      self.currentSessionCode = normalizedSessionCode
      self.bleBeacon.sessionCode = normalizedSessionCode
      // BLE PROBE — scan while following. Observes and logs only; the page is NOT applied to the
      // UI, so a bad reading cannot move anybody off the director's page.
      self.bleBeacon.log = { [weak self] ev, data in self?.dbgLog(ev, data) }
      // RENDER THE BLE PAGE. Measured on 2026-08-17 (build 433): BLE delivered a page 9 seconds
      // before the mesh session existed, and kept delivering sub-second throughout — median
      // 0.10-0.18 s, worst case 0.83 s across all five devices, versus a mesh worst case of 112 s.
      // The mesh keeps the fast path (0.02 s median); BLE is the floor that makes staleness
      // bounded, because there is no handshake to fail.
      //
      // TWO SAFETY RULES. (1) Never render without a known book — a page number in the wrong book
      // is unrecoverable, so this no-ops until a mesh page has told us which book we are in.
      // (2) Monotonic: never apply an older seq than one already applied, so a stale advertisement
      // cannot drag a follower backwards.
      self.bleBeacon.onPage = { [weak self] page, seq, nonce in
        guard let self = self, self.currentRole == "follower" else { return }
        self.bleLastSeenSeq = seq
        // A NEW ADVERTISER RESETS THIS GUARD TOO. seq is per-session and restarts at 0, so comparing
        // it across directors is meaningless — the beacon rebases for exactly this reason and now
        // says so out loud by handing us the nonce.
        //
        // Safe to trust a nonce change today in a way it was not when this guard was written:
        // parse() rejects the legacy 2-field format outright, so pre-448 devices (the ones that
        // never stopped advertising) cannot reach here at all, and 448 wired stopPublishing() into
        // resetTransport() so a device that stops directing stops advertising. The stale-advertiser
        // case that produced the 444 wrong-song flash is closed on both sides.
        if nonce != self.bleAppliedNonce {
          self.bleAppliedNonce = nonce
          self.bleAppliedSeq = -1
          self.dbgLog("ble:rebase", ["nonce": nonce, "page": page, "seq": seq])
        }
        // Unreachable now that the book is pinned above, and kept deliberately: if a future
        // change ever blanks the id, refusing to render a page whose book is unknown is still the
        // right call. `ble:skip-no-book` appearing in telemetry would mean that regression.
        guard !self.lastKnownBookId.isEmpty else {
          self.dbgLog("ble:skip-no-book", ["page": page, "seq": seq])
          return
        }
        guard seq > self.bleAppliedSeq else { return }
        self.bleAppliedSeq = seq
        self.dbgLog("ble:page-apply", ["page": page, "seq": seq])
        self.emitPage(page: max(1, page), totalPages: self.lastKnownTotalPages,
                      mode: self.lastKnownMode, bookId: self.lastKnownBookId, src: "ble")
      }
      // A FOLLOWER MUST NEVER ADVERTISE. Belt-and-braces alongside resetTransport: whatever path
      // reached this role, stop publishing before we start listening.
      self.bleBeacon.stopPublishing()
      // Warm BOTH radios now, not when they are first needed. A follower may become the director a
      // second later, and creating the peripheral inside publish() put CoreBluetooth's power-on
      // between the tap and the page reaching the air.
      self.bleBeacon.primeRadios()
      self.bleBeacon.startScanning()
      self.configureTransport()
      self.startBleHealthTimer()
      // Start the 1 Hz pulse NOW, not on .connected — hunting is exactly when it has work to do.
      self.startFollowerWatchdog()
      self.startAdvertising()
      self.startBrowsing()
      self.startDiscoveryRefreshTimer()
      self.startSelfDirectedTimer()
      self.emitState(status: "searching")
      resolve(["role": "follower", "sessionCode": normalizedSessionCode])
    }
  }

  @objc(stop:rejecter:)
  func stop(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      self.resetTransport(emitState: true)
      resolve(["stopped": true])
    }
  }

  @objc(resetForAppReset:rejecter:)
  func resetForAppReset(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      self.resetTransport(emitState: true)
      resolve(["reset": true])
    }
  }

  @objc(requestDirectorTakeover:rejecter:)
  func requestDirectorTakeover(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "follower", let director = self.connectedDirectorPeer else {
        reject("DIRECTOR_TAKEOVER_NO_DIRECTOR", "No hay director conectado.", nil)
        return
      }
      let requestId = Self.randomToken()
      let payload: [String: Any] = [
        "v": Self.protocolVersion,
        "type": "takeover_request",
        "requestId": requestId,
        "requesterName": self.localPeerID?.displayName ?? "seguidor",
      ]
      self.sendControlPayload(payload, to: director)
      resolve(["requestId": requestId])
    }
  }

  @objc(approveDirectorTakeover:resolver:rejecter:)
  func approveDirectorTakeover(
    _ requestId: String,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "director" else {
        reject("DIRECTOR_TAKEOVER_ROLE_INVALID", "Solo el director puede aprobar.", nil)
        return
      }
      guard let requester = self.pendingTakeoverRequests[requestId] else {
        reject("DIRECTOR_TAKEOVER_NOT_FOUND", "Solicitud no encontrada.", nil)
        return
      }
      self.pendingTakeoverRequests[requestId] = nil
      self.pendingTakeoverTimers[requestId]?.invalidate(); self.pendingTakeoverTimers.removeValue(forKey: requestId)
      let payload: [String: Any] = [
        "v": Self.protocolVersion,
        "type": "takeover_approved",
        "requestId": requestId,
      ]
      self.sendControlPayload(payload, to: requester)

      let sessionCode = self.currentSessionCode
      self.resetTransport(emitState: false)
      self.currentRole = "follower"
      self.currentSessionCode = sessionCode
      self.configureTransport()
      self.startAdvertising()
      self.startBrowsing()
      self.startDiscoveryRefreshTimer()
      self.startSelfDirectedTimer()
      self.emitState(status: "searching", message: "Cediendo el control al nuevo director...")
      resolve(nil)
    }
  }

  @objc(denyDirectorTakeover:resolver:rejecter:)
  func denyDirectorTakeover(
    _ requestId: String,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "director" else {
        reject("DIRECTOR_TAKEOVER_ROLE_INVALID", "Solo el director puede rechazar.", nil)
        return
      }
      guard let requester = self.pendingTakeoverRequests[requestId] else {
        resolve(nil)
        return
      }
      self.pendingTakeoverRequests[requestId] = nil
      self.pendingTakeoverTimers[requestId]?.invalidate(); self.pendingTakeoverTimers.removeValue(forKey: requestId)
      let payload: [String: Any] = [
        "v": Self.protocolVersion,
        "type": "takeover_denied",
        "requestId": requestId,
      ]
      self.sendControlPayload(payload, to: requester)
      resolve(nil)
    }
  }

  // Held strongly so they survive long enough for iOS to surface the permission dialog.
  private var primingBrowser: MCNearbyServiceBrowser?
  private var primingAdvertiser: MCNearbyServiceAdvertiser?
  private var primingPeerID: MCPeerID?
  // NWBrowser is the reliable iOS 14+ trigger for the Local Network permission prompt and
  // for registering the app toggle under Settings > Privacy > Local Network.
  private var primingNWBrowser: NWBrowser?

  @objc(primePermissions:rejecter:)
  func primePermissions(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      // NWBrowser is the guaranteed way to trigger the iOS 14+ Local Network permission
      // dialog. MCNearbyServiceBrowser alone does NOT reliably surface the prompt or
      // register the toggle in Settings > Privacy > Local Network on all iOS versions.
      let nwParams = NWParameters()
      nwParams.includePeerToPeer = true
      let nwBrowser = NWBrowser(
        for: .bonjour(type: "_\(Self.serviceType)._tcp", domain: nil),
        using: nwParams
      )
      nwBrowser.stateUpdateHandler = { _ in }
      nwBrowser.browseResultsChangedHandler = { _, _ in }
      nwBrowser.start(queue: .main)
      self.primingNWBrowser = nwBrowser

      // MPC priming: start a real browser+advertiser with a delegate so iOS also
      // records the Bluetooth usage for the Bluetooth permission path.
      let rawDisplay = UIDevice.current.name.isEmpty ? "signovivo" : UIDevice.current.name
      let displayName = String(rawDisplay.prefix(50))
      let peerID = MCPeerID(displayName: "\(displayName)-prime")
      self.primingPeerID = peerID

      let browser = MCNearbyServiceBrowser(peer: peerID, serviceType: Self.serviceType)
      browser.delegate = self
      browser.startBrowsingForPeers()
      self.primingBrowser = browser

      let advertiser = MCNearbyServiceAdvertiser(peer: peerID, discoveryInfo: ["session": "prime", "role": "prime"], serviceType: Self.serviceType)
      advertiser.delegate = self
      advertiser.startAdvertisingPeer()
      self.primingAdvertiser = advertiser

      DispatchQueue.main.asyncAfter(deadline: .now() + 6.0) { [weak self] in
        guard let self = self else { return }
        self.primingNWBrowser?.cancel()
        self.primingNWBrowser = nil
        self.primingBrowser?.stopBrowsingForPeers()
        self.primingBrowser?.delegate = nil
        self.primingBrowser = nil
        self.primingAdvertiser?.stopAdvertisingPeer()
        self.primingAdvertiser?.delegate = nil
        self.primingAdvertiser = nil
        self.primingPeerID = nil
      }
      resolve(nil)
    }
  }

  // Lightweight discovery refresh exposed to JS — restarts browser+advertiser without
  // tearing down existing MCSession connections. Used for the first tap on the reconnect
  // button so followers can re-find the director without dropping the current session.
  /// The split-brain kick from becomeDirector. Browser-only ON PURPOSE — see refreshBrowserOnly.
  /// A brand-new director must find rivals fast; it must NOT stop being findable to do it, because
  /// every follower is inviting it at exactly that moment.
  @objc(refreshDirectorBrowse:rejecter:)
  func refreshDirectorBrowse(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole != "off" else { resolve(nil); return }
      self.earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
      self.refreshBrowserOnly()
      self.scheduleNextDiscoveryRefresh()
      resolve(nil)
    }
  }

  @objc(refreshNearbyDiscovery:rejecter:)
  func refreshNearbyDiscovery(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole != "off" else { resolve(nil); return }
      // Reset to early-burst mode so the next few cycles are fast (5 s each).
      self.earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
      self.refreshDiscovery()
      self.scheduleNextDiscoveryRefresh()
      // Restart the self-directed countdown so the user gets a fresh 10 s window
      // after a manual refresh before the "Modo libre" label re-appears.
      if self.currentRole == "follower", self.connectedDirectorPeer == nil {
        self.startSelfDirectedTimer()
      }
      resolve(nil)
    }
  }

  /// What ⟳ SHOULD do. `refreshNearbyDiscovery` restarts the transports but never clears
  /// `connectedDirectorPeer`, so against a wedged session it is a no-op — which is exactly what
  /// Miguel saw on 2026-08-17: the spinner animated and the iPad stayed on song 59, because
  /// scheduleNextDiscoveryRefresh skips re-browsing entirely while that field is non-nil. A human
  /// tapping ⟳ has already decided they are not synced; honour that and tear the session down.
  @objc(forceFollowerReconnectNow:rejecter:)
  func forceFollowerReconnectNow(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "follower" else { resolve(nil); return }
      self.dbgLog("resync:force-reconnect", [:])
      self.earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
      self.forceFollowerReconnect(staleFor: 0)
      resolve(nil)
    }
  }

  @objc(requestCurrentSnapshot:rejecter:)
  func requestCurrentSnapshot(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      autoreleasepool {
        self.forceFollowerHelloNow()
        resolve(nil)
      }
    }
  }

  // Prevents iOS from auto-locking the screen during active sync sessions. MPC is throttled
  // when the screen locks, which silently breaks director-follower connectivity mid-rehearsal.
  @objc(setIdleTimerDisabled:resolver:rejecter:)
  func setIdleTimerDisabled(
    _ disabled: Bool,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      UIApplication.shared.isIdleTimerDisabled = disabled
      resolve(nil)
    }
  }

  @objc(getDeviceName:rejecter:)
  func getDeviceName(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    let deviceName = UIDevice.current.name.trimmingCharacters(in: .whitespacesAndNewlines)
    if !deviceName.isEmpty {
      resolve(deviceName)
      return
    }
    let host = ProcessInfo.processInfo.hostName
    resolve(host.hasSuffix(".local") ? String(host.dropLast(6)) : host)
  }

  @objc(sendPageUpdate:totalPages:mode:bookId:resolver:rejecter:)
  func sendPageUpdate(
    _ page: NSNumber,
    totalPages: NSNumber,
    mode: String,
    bookId: String,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "director" else {
        reject("DIRECTOR_ROLE_INVALID", "Solo el director puede enviar páginas.", nil)
        return
      }
      // Always store state so late-joining followers receive the correct snapshot.
      self.currentPageNumber = max(1, page.intValue)
      self.currentTotalPages = max(0, totalPages.intValue)
      self.currentMode = mode
      self.currentBookId = bookId
      // BLE PROBE — published before the connected-peers guard on purpose. The mesh gives up here
      // when nobody is attached; a beacon does not care whether anyone is listening, which is
      // exactly the property being measured.
      // The beacon owns its own seq now. This used to pass bleSeq, which THIS function bumps once
      // per second via the director heartbeat rather than once per page turn — an ever-growing
      // number inside a fixed-size advertisement.
      self.bleBeacon.log = { [weak self] ev, data in self?.dbgLog(ev, data) }
      self.bleBeacon.publish(page: self.currentPageNumber ?? page.intValue)
      // The heartbeat reaches here once a second whether or not the page moved, which makes it the
      // natural place to re-assert an advertisement that stopped without us asking. publish() alone
      // cannot do it: it early-returns on an unchanged page, so a director that went dark would stay
      // dark until the next page turn.
      self.bleBeacon.ensureAdvertising()
      let connected = self.allConnectedPeers
      guard !connected.isEmpty else {
        self.emitState(status: "waiting-followers")
        resolve(["deliveredPeers": 0])
        return
      }
      let payload: [String: Any] = [
        "v": Self.protocolVersion,
        "type": "page",
        "page": max(1, page.intValue),
        "totalPages": max(0, totalPages.intValue),
        "mode": mode,
        "bookId": bookId,
        "bundleVersion": self.currentBundleVersion,
      ]
      guard let data = try? JSONSerialization.data(withJSONObject: payload) else {
        resolve(["deliveredPeers": 0])
        return
      }
      var delivered = 0
      // Send through each session to its own peers
      for session in self.mcSessions where !session.connectedPeers.isEmpty {
        if (try? session.send(data, toPeers: session.connectedPeers, with: .reliable)) != nil {
          delivered += session.connectedPeers.count
        } else {
          try? session.send(data, toPeers: session.connectedPeers, with: .unreliable)
        }
      }
      self.emitState(status: "connected")
      resolve(["deliveredPeers": delivered])
    }
  }

  // MARK: - Transport setup

  /// This device's peer name, STABLE for the life of the install.
  ///
  /// It used to be minted fresh on every call to configureTransport — `UIDevice.name` plus a random
  /// UUID prefix — and configureTransport runs on EVERY role transition (startDirector,
  /// startFollower, approveDirectorTakeover). So a device became a stranger to the whole mesh every
  /// time it changed role.
  ///
  /// MEASURED on the owner's fleet, build 440, from mPad's own unified log:
  ///
  ///     21:06:26  advertising as iPad-92A6C5
  ///     21:08:07  advertising as iPad-CF034B     <- same iPad, two minutes later
  ///     21:08:34  advertising as iPad-AE6CD6     <- and again
  ///
  ///     11 distinct peer identities for a 4-device fleet
  ///
  /// That is fatal for sync, because a follower tracks its director by MCPeerID
  /// (connectedDirectorPeer, discoveredDirectors). WHEN THE DIRECTOR RENAMES ITSELF IT SILENTLY
  /// ABANDONS EVERY FOLLOWER: they go on hunting a peer that no longer exists while a "new" director
  /// they have never seen appears beside it. It is also what produced the apparent split-brain in
  /// that capture — iPad-92A6C5 and iPad-AE6CD6 are the SAME iPad, not two directors.
  ///
  /// The random suffix itself is NOT the mistake and must stay: since iOS 16, UIDevice.name returns
  /// a generic "iPad"/"iPhone" to apps without a special entitlement, so every device in the loft
  /// would advertise the same displayName and collide. It only has to be stable, so it is generated
  /// once and persisted.
  private var stablePeerName: String {
    let key = "sv.peerNameSuffix"
    let defaults = UserDefaults.standard
    let suffix: String
    if let saved = defaults.string(forKey: key), !saved.isEmpty {
      suffix = saved
    } else {
      suffix = String(UUID().uuidString.prefix(6))
      defaults.set(suffix, forKey: key)
    }
    let rawName = UIDevice.current.name.isEmpty ? "SignoVivo" : UIDevice.current.name
    // 63 is MCPeerID's hard limit; leave room for the "-XXXXXX" suffix.
    let peerName = String(rawName.prefix(50))
    return "\(peerName)-\(suffix)"
  }

  /// The archived MCPeerID for this install.
  ///
  /// A stable NAME is not sufficient. Apple's documentation is explicit that an MCPeerID's identity
  /// is the object, not its displayName — two MCPeerIDs created independently with the same name are
  /// not the same peer to Multipeer's bookkeeping — and that a peer ID must be ARCHIVED to be stable
  /// across launches. resetTransport() sets localPeerID = nil and runs at the top of startDirector
  /// and approveDirectorTakeover, so an in-memory cache alone would still mint a new peer on exactly
  /// the transitions this is meant to survive.
  ///
  /// Falls back to a fresh peer on any archive failure rather than refusing to start: a device that
  /// cannot read its own UserDefaults must still be able to join the mesh, and a fresh identity is
  /// the pre-existing behaviour, not a regression.
  private func loadOrCreatePeerID() -> MCPeerID {
    let key = "sv.peerID.v1"
    let defaults = UserDefaults.standard
    if let data = defaults.data(forKey: key),
       let restored = try? NSKeyedUnarchiver.unarchivedObject(ofClass: MCPeerID.self, from: data) {
      return restored
    }
    let created = MCPeerID(displayName: stablePeerName)
    if let data = try? NSKeyedArchiver.archivedData(withRootObject: created, requiringSecureCoding: true) {
      defaults.set(data, forKey: key)
    }
    return created
  }

  private func configureTransport() {
    // The SAME peer, every time. Reconfiguring the transport must not change who this device IS —
    // only how it is talking.
    let peerID = localPeerID ?? loadOrCreatePeerID()
    localPeerID = peerID

    // Create first session (director will lazily create second when first fills up)
    let firstSession = MCSession(peer: peerID, securityIdentity: nil, encryptionPreference: .none)
    firstSession.delegate = self
    mcSessions = [firstSession]

    discoveredDirectors = [:]
    discoveredDirectorInfo = [:]
    discoveredFollowers = []
    discoveredFollowerInfo = [:]
    pendingInvitePeer = nil
    connectedDirectorPeer = nil
  }

  private func startAdvertising() {
    guard (currentRole == "director" || currentRole == "follower"), let peerID = localPeerID else { return }
    var discoveryInfo: [String: String] = [
      "session": currentSessionCode,
      "role": currentRole,
      "hgen": Self.handshakeGeneration,  // absent on build ≤226 → legacy peer
    ]
    if currentRole == "director" { discoveryInfo["token"] = currentDirectorToken }
    let adv = MCNearbyServiceAdvertiser(peer: peerID, discoveryInfo: discoveryInfo, serviceType: Self.serviceType)
    adv.delegate = self
    adv.startAdvertisingPeer()
    advertiser = adv
    advertiserFailureCount = 0
  }

  private func startBrowsing() {
    guard let peerID = localPeerID else { return }
    let b = MCNearbyServiceBrowser(peer: peerID, serviceType: Self.serviceType)
    b.delegate = self
    b.startBrowsingForPeers()
    browser = b
    browserFailureCount = 0
    // A BRAND-NEW BROWSER IS ALREADY AS FRESH AS A REFRESH CAN MAKE IT.
    //
    // startDirector calls startBrowsing directly, and becomeDirector then kicks a re-browse
    // milliseconds later for split-brain convergence — which tore that browser down and rebuilt it,
    // discarding any peer it had already found, to achieve a state it was already in. Stamping the
    // refresh clock here makes the existing minRefreshInterval throttle suppress that automatically,
    // for every caller, instead of each one having to know how old the browser is.
    lastRefreshAt = Date().timeIntervalSince1970
  }

  // Adaptive discovery refresh: fast burst for the first earlyRefreshCycleCount cycles,
  // then settle into the normal interval. This means a follower finds a late-arriving
  // director within earlyRefreshInterval seconds rather than up to discoveryRefreshInterval.
  private func startDiscoveryRefreshTimer() {
    discoveryRefreshTimer?.invalidate()
    earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
    scheduleNextDiscoveryRefresh()
  }

  private func scheduleNextDiscoveryRefresh() {
    // FIX 1 — THE TIMER LEAK THAT CAUSED THE DISCOVERY STORM (measured 2026-08-17).
    //
    // This assigned `discoveryRefreshTimer` WITHOUT invalidating the previous timer. A
    // Timer.scheduledTimer retains itself in the run loop, so overwriting the property does not
    // stop the old one — it only loses the handle. The orphan still fires, and its callback
    // schedules another. Ten call sites reach this function and only startDiscoveryRefreshTimer
    // invalidated first, so the live timer population DOUBLED on every overlapping schedule.
    //
    // The biggest doubler was foregrounding: refreshNearbyDiscovery schedules without
    // invalidating, and PdfReaderApp.tsx called it TWICE per foreground. Pick a device up, put it
    // down, repeat, and the rate compounds.
    //
    // Measured on the owner's iPhone from its own unified log: 66 advertiser start/stop events per
    // SECOND, sustained — ~33 full teardown/rebuild cycles a second against an intended one every
    // 5-12 s. An MCSession invite cannot complete when the advertiser it must answer on lives ~15
    // ms, which is exactly the "handshake fails, delivery is fine" signature recorded in issue
    // #352. That issue holds EIGHT disproved theories — discovery backoff, the M-F1 clock, a render
    // -layer bug, radio duty-cycling, AWDL/iOS incompatibility — every one of them about the
    // device. It was a missing invalidate(), and it explains why the iPhone was worst: it is the
    // device that gets picked up and put down all day.
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = nil

    let generation = resetGeneration
    let interval: TimeInterval
    if earlyRefreshCyclesRemaining > 0 {
      interval = Self.earlyRefreshInterval
    } else if thermalState == .serious || thermalState == .critical {
      // Device is hot — slow discovery churn to 60 s to reduce CPU/radio pressure.
      interval = 60
    } else {
      interval = Self.discoveryRefreshInterval
    }
    discoveryRefreshTimer = Timer.scheduledTimer(withTimeInterval: interval, repeats: false) { [weak self] _ in
      DispatchQueue.main.async {
        autoreleasepool {
          guard let self = self, self.currentRole != "off", self.resetGeneration == generation else { return }
          if !self.appIsActive {
            self.scheduleNextDiscoveryRefresh()
            return
          }
          if self.earlyRefreshCyclesRemaining > 0 { self.earlyRefreshCyclesRemaining -= 1 }
          // Don't restart advertiser/browser while a follower is stably connected —
          // the churn disrupts MPC without any benefit since we already have a director.
          if self.currentRole == "follower", self.connectedDirectorPeer != nil {
            self.scheduleNextDiscoveryRefresh()
            return
          }
          // HANDSHAKE PROTECTION (fixes the follower-stuck bug): NEVER tear down the advertiser/
          // browser while a connection is actively being established. refreshDiscovery() stops+
          // restarts BOTH transports — that aborts the in-flight invite AND makes this device's
          // advertiser vanish, firing a spurious lostPeer on the peer, so the MCSession never reaches
          // .connected and the follower loops searching↔connecting forever (confirmed via /log).
          // While a handshake is in progress, hold the transport STEADY and let it finish. Escape
          // hatches: the invite's 30s timeout (follower) and lostPeer (peer truly left) both resume
          // normal refresh, so a genuinely dead peer can't wedge us here.
          if self.currentRole == "follower", self.pendingInvitePeer != nil,
             Date().timeIntervalSince1970 - self.pendingInviteTimestamp < Self.inviteTimeout {
            self.dbgLog("refresh:hold-connecting", ["target": self.pendingInvitePeer?.displayName ?? ""])
            self.reconsiderFollowerTarget() // maintain/re-issue the invite on the LIVE browser
            self.scheduleNextDiscoveryRefresh()
            return
          }

          // STABILITY PROTECTION FOR THE FOLLOWER'S BROWSER — the mirror of the director's rule
          // below, and the fix for slow convergence measured on the owner's fleet with build 445:
          // followers DID converge (the ghost-peer fix worked) but took 10-20+ s, against a
          // standard of "longer than a few seconds is a failure".
          //
          // refreshDiscovery() now clears every discovered peer, because an MCPeerID dies with the
          // browser that found it. That is correct and it stopped the ghost invites. But it runs on
          // a fixed 5-12 s tick REGARDLESS of whether the browser is working, so a follower that had
          // found the director and was about to invite it got its discovery wiped and started over.
          // Convergence became "however many cycles until an invite happens to fit inside one
          // window" — 5 s, 10 s, 20 s. Exactly the numbers observed.
          //
          // The rule is the same one the director already follows: DO NOT RESTART A TRANSPORT THAT
          // IS DEMONSTRABLY WORKING. A browser that produced a sighting seconds ago is not wedged,
          // and restarting it destroys the only progress the follower has. The refresh exists to
          // recover a browser that has gone deaf; it should fire when that is actually in evidence.
          //
          // Bounded on purpose: the hold lasts only while sightings keep arriving. If the director
          // truly disappears, sightings stop, this guard lapses within browserHealthySeconds and the
          // normal refresh resumes — so a genuinely dead browser still gets rebuilt.
          if self.currentRole == "follower", self.connectedDirectorPeer == nil,
             let newest = self.discoveredDirectorSeenAt.values.max(),
             Date().timeIntervalSince1970 - newest < Self.browserHealthySeconds {
            self.dbgLog("refresh:hold-browsing", ["seenAgo": Int(Date().timeIntervalSince1970 - newest)])
            self.reconsiderFollowerTarget() // act on what the LIVE browser already found
            self.scheduleNextDiscoveryRefresh()
            return
          }
          // STABILITY PROTECTION for the director: do NOT tear down the advertiser while we are
          // forming OR serving connections. Tearing it down aborts a connecting follower's invite AND
          // drops the advertiser out from under ALREADY-CONNECTED followers — observed via /log: the
          // connection died ~30s in when the director's 25s refresh tore down its advertiser (follower
          // fired lostPeer → session went notConnected). A running advertiser/browser keeps finding
          // new peers without a restart, so new followers still self-invite the live advertiser. We
          // STILL run the split-brain announce/conflict here so two directors converge. Only churn the
          // transport when the director is fully idle (no followers at all → discovery may be wedged).
          if self.currentRole == "director",
             !self.allConnectedPeers.isEmpty || !self.discoveredFollowers.isEmpty {
            self.dbgLog("refresh:hold-serving", [
              "connected": self.allConnectedPeers.count,
              "discovered": self.discoveredFollowers.count,
            ])
            self.broadcastDirectorAnnounce()
            for (_, token) in self.discoveredDirectors where !token.isEmpty {
              self.handleDirectorConflict(with: token)
              guard self.currentRole == "director" else { break }
            }
            self.scheduleNextDiscoveryRefresh()
            return
          }
          self.refreshDiscovery()
          // Split-brain re-convergence (periodic). broadcastDirectorAnnounce/handleDirectorConflict
          // otherwise only fire on foreground, startDirector, and the one-shot foundPeer/.connected
          // events — so a SINGLE dropped announce (MPC's fragile first-send-at-connected window) can
          // leave two directors split forever, with followers obeying the wrong one. Re-announce our
          // token to every connected peer each cycle so a dropped announce is retried, and re-run the
          // token tiebreak against every still-discovered director so two directors that briefly lost
          // sight of each other re-converge instead of staying split.
          if self.currentRole == "director" {
            self.broadcastDirectorAnnounce()
            for (_, token) in self.discoveredDirectors where !token.isEmpty {
              self.handleDirectorConflict(with: token)
              // handleDirectorConflict may demote us (resetTransport → role "off"); stop if so.
              guard self.currentRole == "director" else { break }
            }
          } else if self.currentRole == "follower", self.connectedDirectorPeer == nil {
            // The refresh tick rebuilt discovery but, on its own, never re-drives target selection.
            // A follower whose invite was silently rejected (sessions full / director refreshed its
            // advertiser) would otherwise sit at "connecting" for a full inviteTimeout with no retry,
            // since a steadily-visible director never re-fires foundPeer. Re-run selection here so a
            // stale invite is retried (or a newly-best director adopted) every cycle.
            self.reconsiderFollowerTarget()
          }
          self.scheduleNextDiscoveryRefresh()
        }
      }
    }
  }

  private func startSelfDirectedTimer() {
    selfDirectedTimer?.invalidate()
    let generation = resetGeneration
    selfDirectedTimer = Timer.scheduledTimer(withTimeInterval: Self.selfDirectedTimeoutSeconds, repeats: false) { [weak self] _ in
      DispatchQueue.main.async {
        autoreleasepool {
          // Also guard pendingInvitePeer: don't fire while mid-handshake with a director.
          guard let self = self,
                self.resetGeneration == generation,
                self.currentRole == "follower",
                self.connectedDirectorPeer == nil,
                self.pendingInvitePeer == nil else { return }
          // Restart fast burst so a director who comes online right now is found within 5 s.
          self.earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
          self.refreshDiscovery()
          self.scheduleNextDiscoveryRefresh()
          self.emitState(status: "self-directed")
        }
      }
    }
  }

  private func cancelSelfDirectedTimer() {
    selfDirectedTimer?.invalidate()
    selfDirectedTimer = nil
  }

  private func startFollowerHelloTimer() {
    followerHelloTimer?.invalidate()
    let generation = resetGeneration
    lastFollowerHelloAt = 0
    lastFollowerPageReceivedAt = 0
    followerHelloTimer = Timer.scheduledTimer(withTimeInterval: Self.followerHelloInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        autoreleasepool {
          guard let self = self, self.resetGeneration == generation else { return }
          self.sendFollowerHelloIfNeeded()
        }
      }
    }
  }

  private func stopFollowerHelloTimer() {
    followerHelloTimer?.invalidate()
    followerHelloTimer = nil
  }

  /// Fast half-open watchdog — see followerStaleReconnectSeconds. Runs only while a follower is
  /// connected; forces a reconnect the moment the director's heartbeat stream goes silent.
  /// BLE IS THE SUB-1s PATH, SO ITS HEALTH MUST NOT DEPEND ON THE MESH.
  ///
  /// ensureScanning lived on the follower watchdog, which stops on disconnect, on backgrounding and
  /// on every transport reset — so the fast path went deaf at exactly the moments the slow path was
  /// already struggling, which is when it was the only thing still working. Coupling a fallback to
  /// the health of the thing it is a fallback FOR defeats the point of having one.
  ///
  /// This timer runs for BOTH roles, from configureTransport until resetTransport, regardless of
  /// sessions, peers or connection state. It costs two bool reads a second.
  private func startBleHealthTimer() {
    bleHealthTimer?.invalidate()
    let generation = resetGeneration
    bleHealthTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        guard let self = self, self.resetGeneration == generation, self.appIsActive else { return }
        if self.currentRole == "director" { self.bleBeacon.ensureAdvertising() }
        else if self.currentRole == "follower" { self.bleBeacon.ensureScanning() }
      }
    }
  }

  private func startFollowerWatchdog() {
    followerWatchdogTimer?.invalidate()
    let generation = resetGeneration
    followerWatchdogTimer = Timer.scheduledTimer(withTimeInterval: Self.followerWatchdogInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        autoreleasepool {
          guard let self = self, self.resetGeneration == generation, self.appIsActive,
                self.currentRole == "follower" else { return }

          // BLE IS THE FAST PATH AND MUST NOT GO DEAF QUIETLY. If iOS stops the scan without a
          // callback, the beacon's own `isScanning` bool stays true and its guard refuses to restart
          // it — a follower deaf for the rest of the session while believing it is listening. This
          // asks CoreBluetooth for the truth every tick, which costs a bool read.
          self.bleBeacon.ensureScanning()

          // LAST RESORT: rebuild the session when retrying the invite is provably not working.
          //
          // Everything above this retries the HANDSHAKE. None of it can fix a wedged MCSession —
          // the same broken session produces the same result no matter how many invites go into it,
          // which is how a follower sits at "connecting" indefinitely with the director plainly
          // visible. That is the state the resync button exists for; this reaches it without
          // needing a human to notice, once, after a deliberately generous window.
          //
          // Gated on a director actually being IN SIGHT: if nothing is discovered the problem is
          // discovery, and rebuilding the session would churn for no reason. The clock resets on
          // every escalation, so this is bounded to one rebuild per window and cannot storm.
          if self.connectedDirectorPeer == nil, self.followerHuntingSince > 0,
             !self.discoveredDirectors.isEmpty {
            let hunting = Date().timeIntervalSince1970 - self.followerHuntingSince
            if hunting > Self.followerWedgedSeconds {
              self.dbgLog("watchdog:wedged-rebuild", [
                "huntingSec": Int(hunting), "visibleDirectors": self.discoveredDirectors.count,
              ])
              self.followerHuntingSince = Date().timeIntervalSince1970
              self.forceFollowerReconnect(staleFor: hunting)
              return
            }
          }

          // NOT CONNECTED: retry the handshake every tick until we are (owner's idea, 2026-08-17 —
          // "sleep 1s, setTimerRepeating every 1s until synced").
          //
          // This timer already ran at 1 Hz; it just did nothing unless already connected, so the
          // one state that needed a fast pulse — still hunting — was the one state it sat out.
          //
          // The dead time it removes is real and measured in the constants: a failed handshake
          // waits out inviteTimeout (8 s) and then followerRetryDelay (2 s) before anything tries
          // again, so ~10 s of a 10-20 s convergence was a follower doing nothing at all.
          //
          // Safe at 1 Hz because reconsiderFollowerTarget is idempotent by construction: it returns
          // immediately when already connected, and while an invite is inside its timeout window it
          // emits "connecting" and returns WITHOUT issuing a parallel invite. So this pulses the
          // retry without ever spamming invitePeer.
          //
          // Deliberately NOT a 1 Hz transport restart — that is the discovery storm (66 events/sec)
          // and the cause of the slow convergence in the first place. The browser stays up and
          // accumulates sightings; only the decision to act on them is re-evaluated.
          guard self.connectedDirectorPeer != nil else {
            self.reconsiderFollowerTarget()
            return
          }

          guard self.lastFollowerPageReceivedAt > 0 else { return }
          let stale = Date().timeIntervalSince1970 - self.lastFollowerPageReceivedAt
          if stale > Self.followerStaleReconnectSeconds {
            self.forceFollowerReconnect(staleFor: stale)
          }
        }
      }
    }
  }

  private func stopFollowerWatchdog() {
    followerWatchdogTimer?.invalidate()
    followerWatchdogTimer = nil
  }

  /// Half-open recovery: MPC still thinks we're connected, but the director stopped sending (it
  /// dropped us on its side). Recreate our MCSession so allConnectedPeers no longer contains the
  /// dead director — otherwise reconsiderFollowerTarget's "already connected" guard would block the
  /// re-invite. The advertiser/browser keep running (the director is usually still discovered), so
  /// we re-invite and reconnect within ~1-2s instead of sitting stale for 30-90s.
  private func forceFollowerReconnect(staleFor: Double) {
    guard currentRole == "follower", let peerID = localPeerID else { return }
    dbgLog("watchdog:half-open-reconnect", ["staleFor": Int(staleFor)])
    for s in mcSessions { s.delegate = nil; s.disconnect() }
    let fresh = MCSession(peer: peerID, securityIdentity: nil, encryptionPreference: .none)
    fresh.delegate = self
    mcSessions = [fresh]
    connectedDirectorPeer = nil
    pendingInvitePeer = nil
    lastFollowerPageReceivedAt = 0
    followerHuntingSince = Date().timeIntervalSince1970
    stopFollowerHelloTimer()
    // RESTART the watchdog, do not stop it. It used to stop here "and restart cleanly on the next
    // .connected" — but this function runs precisely when we are NOT connected, so the 0.5 Hz pulse
    // that retries the handshake died at the exact moment it was needed, and stayed dead until a
    // connection it was supposed to help produce. Since 2026-08-18 it also drives the BLE scan
    // self-heal, so stopping it here went deaf as well as blind. startFollowerWatchdog invalidates
    // any existing timer first, so restarting is idempotent.
    startFollowerWatchdog()
    startSelfDirectedTimer()
    emitState(status: "searching", message: "Reconectando con el director...")
    // M-F5: the watchdog can fire because the director genuinely LEFT (not just a data half-open),
    // in which case reconsiderFollowerTarget alone stalls (discoveredDirectors may be empty). Kick
    // an immediate re-scan + arm the fast 5 s discovery burst so a returning/other director is
    // re-found in seconds instead of drifting into the slow ~25 s cadence.
    earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
    refreshDiscovery()
    reconsiderFollowerTarget()
  }

  private func sendFollowerHelloIfNeeded() {
    guard currentRole == "follower", let session = mcSessions.first else { return }
    guard let directorPeer = connectedDirectorPeer else { return }
    guard session.connectedPeers.contains(directorPeer) else { return }

    let now = Date().timeIntervalSince1970
    // If we've received a page recently, avoid spamming.
    if lastFollowerPageReceivedAt > 0, now - lastFollowerPageReceivedAt < Self.followerHelloInterval * 2 {
      return
    }
    if lastFollowerHelloAt > 0, now - lastFollowerHelloAt < Self.followerHelloInterval {
      return
    }
    guard let data = helloPayload() else { return }
    do {
      try session.send(data, toPeers: [directorPeer], with: .reliable)
      lastFollowerHelloAt = now
    } catch {
      // Best-effort: don't emit an error for transient MPC send failures.
    }
  }

  /// Force an immediate "hello" from follower to director to request a fresh snapshot.
  /// Used on app foreground to recover quickly from background-induced desync.
  private func forceFollowerHelloNow() {
    guard currentRole == "follower", let session = mcSessions.first else { return }
    guard let directorPeer = connectedDirectorPeer else { return }
    guard session.connectedPeers.contains(directorPeer) else { return }
    guard let data = helloPayload() else { return }
    do {
      try session.send(data, toPeers: [directorPeer], with: .reliable)
      lastFollowerHelloAt = Date().timeIntervalSince1970
    } catch {
      // Best-effort.
    }
  }

  /// Schedule a one-shot snapshot re-request shortly after connecting. If the director's proactive
  /// snapshot AND the follower's first hello were both dropped in the fragile just-connected window,
  /// this recovers the director's current page in ~followerSnapshotProbeDelay seconds instead of
  /// waiting for the next followerHello tick. Generation-guarded so a reset cancels it.
  private func scheduleFollowerSnapshotProbe() {
    let generation = resetGeneration
    DispatchQueue.main.asyncAfter(deadline: .now() + Self.followerSnapshotProbeDelay) { [weak self] in
      guard let self = self, self.resetGeneration == generation else { return }
      guard self.currentRole == "follower" else { return }
      // Only probe if no page has arrived since we connected — otherwise the snapshot already landed.
      guard self.lastFollowerPageReceivedAt == 0 else { return }
      self.forceFollowerHelloNow()
    }
  }

  // ── Discovery churn: hard floor + loop alarm ─────────────────────────────────
  //
  // Fix 1 (the invalidate above) removes the KNOWN driver of the storm. These two are the safety
  // net for the unknown ones, at the owner's instruction: throttle it anyway, and make it shout.
  // Ten call sites can reach refreshDiscovery, several from JS, so "no caller misbehaves" is not a
  // property this code can assume — Murphy's Law applies to our own call graph.

  /// No caller may tear down the transports faster than this, ever. 2 s is far below the intended
  /// 5-12 s cadence so it never interferes with correct behaviour, and far above the ~15 ms
  /// observed during the storm, so it converts a pathological loop into a survivable one: a
  /// handshake gets whole seconds to complete instead of milliseconds.
  private static let minRefreshInterval: TimeInterval = 2.0
  private var lastRefreshAt: TimeInterval = 0

  /// Loop alarm. Refreshes are DESIGNED to happen every 5-12 s, so more than 8 attempts inside 10 s
  /// cannot be legitimate — it means something is driving this in a loop.
  private static let refreshStormWindow: TimeInterval = 10
  private static let refreshStormThreshold = 8
  private var refreshAttemptTimes: [TimeInterval] = []
  /// Latched so the alarm reports ONCE per episode rather than 33 times a second — an alarm that
  /// floods the log is indistinguishable from the fault it is reporting. Re-arms when things calm.
  private var refreshStormReported = false

  /// Re-browse WITHOUT going invisible.
  ///
  /// THE BUG THIS EXISTS FOR. becomeDirector kicks refreshNearbyDiscovery() the instant it starts
  /// serving, to make a split brain converge fast. That lands in refreshDiscovery(), whose first act
  /// is to destroy the advertiser — at precisely the moment every follower's foundPeer has fired and
  /// their invites are in flight. Those invites evaporate silently.
  ///
  /// This codebase already diagnosed the mirror-image case on the FOLLOWER side and guarded it:
  /// "NEVER tear down the advertiser/browser while a connection is actively being established …
  /// makes this device's advertiser vanish, firing a spurious lostPeer on the peer, so the MCSession
  /// never reaches .connected". The director had no equivalent protection while doing the same thing
  /// to everyone trying to reach it.
  ///
  /// The split-brain purpose only ever needed the BROWSER — finding other directors. Stopping being
  /// findable was never part of the intent, just a side effect of reusing the full refresh.
  private func refreshBrowserOnly() {
    guard currentRole != "off" else { return }
    let now = Date().timeIntervalSince1970
    guard now - lastRefreshAt >= Self.minRefreshInterval else { return }
    lastRefreshAt = now
    autoreleasepool {
      browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
      // Same reasoning as refreshDiscovery: an MCPeerID is meaningful only to the browser instance
      // that found it, so everything discovered by the old browser is a ghost. The advertiser is
      // untouched, so peers mid-invite to US keep their target.
      let forgotten = discoveredDirectors.count + discoveredFollowers.count
      discoveredDirectors.removeAll(); discoveredDirectorSeenAt.removeAll()
      discoveredDirectorInfo.removeAll()
      discoveredFollowers.removeAll(); discoveredFollowerInfo.removeAll()
      if forgotten > 0 { dbgLog("refresh:browser-only", ["forgotten": forgotten]) }
      startBrowsing()
    }
  }

  private func refreshDiscovery() {
    guard currentRole != "off" else { return }
    let now = Date().timeIntervalSince1970

    // ALARM — count every ATTEMPT, including ones the throttle below will drop. Counting only the
    // ones that got through would hide the loop behind the very guard that contains it, which is
    // how a throttle turns a loud bug into a silent one.
    refreshAttemptTimes.append(now)
    refreshAttemptTimes.removeAll { now - $0 > Self.refreshStormWindow }
    if refreshAttemptTimes.count > Self.refreshStormThreshold {
      if !refreshStormReported {
        refreshStormReported = true
        dbgLog("refresh:STORM", [
          "attempts": refreshAttemptTimes.count,
          "windowSec": Int(Self.refreshStormWindow),
          "role": currentRole,
        ])
      }
    } else if refreshAttemptTimes.count <= 2 {
      refreshStormReported = false
    }

    // THROTTLE — silent by design. The alarm above is the signal; logging every dropped call would
    // reproduce the flood at the telemetry layer.
    guard now - lastRefreshAt >= Self.minRefreshInterval else { return }
    lastRefreshAt = now

    autoreleasepool {
      advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
      browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil

      // A REMEMBERED PEER DIES WITH THE BROWSER THAT FOUND IT.
      //
      // An MCPeerID is only meaningful to the MCNearbyServiceBrowser instance that discovered it.
      // The new browser created below starts with an EMPTY peers dictionary, so every peer still
      // sitting in these maps is a ghost — and reconsiderFollowerTarget invites straight out of
      // discoveredDirectors (:invitePeer, "Pick the HIGHEST token"). Inviting a ghost through a
      // browser that never saw it does not fail loudly; Multipeer just logs
      //
      //     Cannot find peer with idString [2gbyj11r6mftw] in the peers dictionary.
      //
      // ...and the invite evaporates. No error, no delegate callback, no session. Which is exactly
      // the symptom that survived six builds: discovery works perfectly, every peer is found at
      // -37 dBm, and nothing ever connects.
      //
      // The maps used to be pruned only at 90 s, more than SEVEN browser generations. That made
      // connecting a race — an invite succeeded only when it happened to fire in the same
      // generation that discovered the target — which is why exactly one device would follow while
      // the rest sat there, and why it looked maddeningly intermittent.
      //
      // Clearing costs nothing: browsing restarts on the next line and a live peer is re-found in
      // well under a second, whereas a stale one can never be connected to at all. connectedPeers
      // is unaffected — an established MCSession is independent of the browser that introduced it,
      // so a follower already attached to its director does not drop here.
      let forgotten = discoveredDirectors.count + discoveredFollowers.count
      discoveredDirectors.removeAll(); discoveredDirectorSeenAt.removeAll()
      discoveredDirectorInfo.removeAll()
      discoveredFollowers.removeAll(); discoveredFollowerInfo.removeAll()
      if forgotten > 0 { dbgLog("refresh:peers-cleared", ["forgotten": forgotten]) }

      startAdvertising()
      startBrowsing()
    }
  }

  /// Stop the discovery refresh timer while a follower is stably connected to a director.
  /// Restarts automatically (with a fast burst) when the connection drops.
  private func pauseDiscoveryRefreshWhileConnected() {
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = nil
  }

  private func resumeDiscoveryRefreshAfterDisconnect() {
    earlyRefreshCyclesRemaining = Self.earlyRefreshCycleCount
    scheduleNextDiscoveryRefresh()
  }

  private func resetTransport(emitState shouldEmitState: Bool) {
    resetGeneration = UUID()
    discoveryRefreshTimer?.invalidate(); discoveryRefreshTimer = nil
    bleHealthTimer?.invalidate(); bleHealthTimer = nil
    cancelSelfDirectedTimer()
    stopFollowerHelloTimer()
    stopFollowerWatchdog()
    advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
    browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
    for s in mcSessions { s.disconnect(); s.delegate = nil }
    mcSessions = []
    // STOP THE BEACON. resetTransport runs on every role change, and until now it never touched
    // BlePageBeacon — so a device that had directed kept broadcasting its last page forever, even
    // as a follower. That ghost advertisement is what build 444 rendered as song 357.
    bleBeacon.stopPublishing()
    localPeerID = nil
    discoveredDirectors = [:]; discoveredDirectorSeenAt = [:]; discoveredDirectorInfo = [:]
    discoveredFollowers = []; discoveredFollowerInfo = [:]
    pendingInvitePeer = nil; pendingInviteTimestamp = 0; connectedDirectorPeer = nil
    pendingTakeoverTimers.values.forEach { $0.invalidate() }
    pendingTakeoverRequests = [:]; pendingTakeoverTimers = [:]
    advertiserFailureCount = 0; browserFailureCount = 0
    currentRole = "off"; currentSessionCode = ""; currentDirectorToken = ""
    lastFollowerHelloAt = 0
    lastFollowerPageReceivedAt = 0
    currentPageNumber = nil; currentTotalPages = 0
    currentMode = ""; currentBookId = ""
    lastEmittedStatus = ""; lastEmittedPeerCount = -1
    if shouldEmitState { emitState(status: "idle") }
  }

  // MARK: - Event emission

  private func emitState(status: String, message: String? = nil) {
    let peerCount = allConnectedPeers.count
    // Skip redundant emissions when nothing meaningful changed (no message, same status/peerCount).
    if (message == nil || message!.isEmpty),
       status == lastEmittedStatus,
       peerCount == lastEmittedPeerCount {
      return
    }
    lastEmittedStatus = status
    lastEmittedPeerCount = peerCount
    sendEvent(withName: Self.eventName, body: [
      "type": "state",
      "role": currentRole,
      "sessionCode": currentSessionCode,
      "status": status,
      "peerCount": peerCount,
      "directorCount": discoveredDirectors.count,
      "message": message ?? "",
    ] as [String: Any])
  }

  private func emitError(code: String, message: String) {
    sendEvent(withName: Self.eventName, body: [
      "type": "error", "code": code, "message": message,
      "role": currentRole, "sessionCode": currentSessionCode,
    ] as [String: Any])
  }

  /// `src` NAMES WHICH CHANNEL DELIVERED THE PAGE — "mesh" or "ble".
  ///
  /// Both arrive at the web layer through this one event, so from JS they were indistinguishable.
  /// That is why the 2026-08-18 "every device jumped to song 2, then reached 372 ten seconds later"
  /// could not be attributed: nothing recorded whether song 2 came over BLE, the mesh, or the relay.
  /// A page that appears in front of the choir with no record of where it came from is the hardest
  /// class of bug to close, and it is one field to fix.
  private func emitPage(page: Int, totalPages: Int, mode: String, bookId: String, src: String) {
    sendEvent(withName: Self.eventName, body: [
      "type": "page", "page": page, "totalPages": totalPages,
      "mode": mode, "bookId": bookId, "sessionCode": currentSessionCode, "src": src,
    ] as [String: Any])
  }

  private func emitTakeoverRequest(requestId: String, requesterName: String) {
    sendEvent(withName: Self.eventName, body: [
      "type": "takeover-request",
      "requestId": requestId,
      "requesterName": requesterName,
      "role": currentRole,
      "sessionCode": currentSessionCode,
    ] as [String: Any])
  }

  private func emitTakeoverDecision(type: String, requestId: String) {
    sendEvent(withName: Self.eventName, body: [
      "type": type,
      "requestId": requestId,
      "role": currentRole,
      "sessionCode": currentSessionCode,
    ] as [String: Any])
  }

  // MARK: - Connection logic

  private func reconsiderFollowerTarget() {
    guard currentRole == "follower", let session = mcSessions.first else { return }
    // DEFENSE IN DEPTH, not the primary guard anymore. This used to read
    // `session.connectedPeers.isEmpty` — "anything connected means we're done" — which silently
    // wedged for 40+s on real hardware (2026-08-18) when a non-director peer occupied the session:
    // connectedDirectorPeer was never set, but this guard still bailed every cycle because the slot
    // wasn't empty, so a live director sitting in discoveredDirectors never got invited. The accept
    // side (didReceiveInvitationFromPeer) and the .connected handler (session:peer-not-director,
    // now actively disconnecting) are what should keep connectedPeers accurate; this checks the
    // thing that actually matters.
    guard connectedDirectorPeer == nil else { emitState(status: "connected"); return }

    // Pick the HIGHEST token — the one that SURVIVES director-conflict resolution
    // (handleDirectorConflict demotes the strictly-lower token). Selecting ascending here would
    // make a follower target the loser during every split-brain/handoff window, then eat an extra
    // reconnect cycle when that director demotes. Descending converges the follower directly onto
    // the winning director. (Equal tokens are impossible — randomToken() carries a UUID suffix —
    // but keep the deterministic displayName tiebreak for total ordering.)
    let sorted = discoveredDirectors.sorted {
      $0.value == $1.value ? $0.key.displayName < $1.key.displayName : $0.value > $1.value
    }
    guard let target = sorted.first?.key else {
      pendingInvitePeer = nil; emitState(status: "searching"); return
    }
    if sorted.count > 1 {
      emitState(status: "resolving-conflict", message: "Hay varios directores cercanos. Eligiendo uno automáticamente.")
    }
    // Don't issue parallel invites while we already have one in flight.
    if let pending = pendingInvitePeer {
      if pending == target {
        // If the invite is still within its timeout window, wait for it.
        // After the window the director may have refreshed its advertiser and silently
        // rejected the invite without firing notConnected — force a fresh attempt.
        let elapsed = Date().timeIntervalSince1970 - pendingInviteTimestamp
        if elapsed < Self.inviteRetryAfter {
          emitState(status: "connecting")
          return
        }
        dbgLog("invite:retry", ["to": pending.displayName, "afterSec": Int(elapsed)])
        // Invite likely stale; fall through to retry below.
        pendingInvitePeer = nil
      } else if discoveredDirectors[pending] != nil {
        // Different pending target is still visible — finish that handshake first.
        emitState(status: "connecting")
        return
      } else {
        // Pending peer disappeared; clear stale state and proceed with a fresh invite.
        pendingInvitePeer = nil
      }
    }
    // A director was found — cancel the self-directed fallback; we're about to connect.
    cancelSelfDirectedTimer()

    // A genuine legacy (build ≤226) director always advertises a NON-EMPTY discoveryInfo
    // (it still carries "session"/"role" — that's how we matched the session code above) but
    // omits "hgen". A target whose stored info is EMPTY means discoveryInfo arrived nil — that
    // is NOT a legacy signal, and waiting for an invite from a modern director that will never
    // send one wedges the follower at "connecting" forever. So only the present-but-no-hgen
    // case is treated as legacy; a missing/empty info falls through to the modern self-invite.
    let targetInfo = discoveredDirectorInfo[target] ?? [:]
    let isLegacyDirector = !targetInfo.isEmpty && targetInfo["hgen"] == nil
    if isLegacyDirector {
      // Build ≤226 director: it will call invitePeer on us immediately upon foundPeer.
      // Do NOT self-invite — that causes the double-invite race that breaks the connection.
      // Just set pendingInvitePeer and wait for its incoming invitation.
      pendingInvitePeer = target
      pendingInviteTimestamp = Date().timeIntervalSince1970
      emitState(status: "connecting")
      return
    }

    // Modern director: we initiate. No race possible — it won't invite us.
    pendingInvitePeer = target
    pendingInviteTimestamp = Date().timeIntervalSince1970
    emitState(status: "connecting")
    let capturedTarget = target
    let capturedSession = session
    DispatchQueue.main.async { [weak self] in
      guard let self = self, self.currentRole == "follower",
            self.pendingInvitePeer == capturedTarget,
            !self.allConnectedPeers.contains(capturedTarget) else { return }
      self.dbgLog("invite:send", ["to": capturedTarget.displayName])
      self.browser?.invitePeer(capturedTarget, to: capturedSession, withContext: nil, timeout: Self.inviteTimeout)
    }
  }

  private func handleDirectorConflict(with otherToken: String) {
    guard currentRole == "director", !otherToken.isEmpty, !currentDirectorToken.isEmpty else { return }
    if otherToken > currentDirectorToken {
      emitError(code: "DIRECTOR_CONFLICT", message: "Un nuevo director tomó el control. Este dispositivo cambió a modo seguidor.")
      resetTransport(emitState: false)
    }
  }

  private static func normalizeSessionCode(_ value: String) -> String {
    String(value.uppercased().filter { $0.isLetter || $0.isNumber }.prefix(maxSessionCodeLength))
  }

  private static func randomToken() -> String {
    // The µs timestamp prefix preserves the "newer director wins" ordering used by
    // handleDirectorConflict (lexicographic compare of a fixed-width zero-padded number).
    // The UUID suffix guarantees two tokens minted in the same microsecond can never be
    // exactly equal — equal tokens would deadlock conflict resolution (neither side > the other).
    let micros = String(format: "%020lld", Int64(Date().timeIntervalSince1970 * 1_000_000))
    return "\(micros)-\(UUID().uuidString)"
  }

  // MARK: - MCNearbyServiceAdvertiserDelegate

  func advertiser(
    _ advertiser: MCNearbyServiceAdvertiser,
    didReceiveInvitationFromPeer peerID: MCPeerID,
    withContext context: Data?,
    invitationHandler: @escaping (Bool, MCSession?) -> Void
  ) {
    DispatchQueue.main.async {
      // Note: do NOT guard advertiser === self.advertiser here.
      // refreshDiscovery replaces self.advertiser every few seconds; any in-flight
      // invitation that arrives on the old advertiser would be silently rejected,
      // leaving the follower's pendingInvitePeer stuck indefinitely.
      // The role guard below is sufficient to reject invites during/after reset.
      self.dbgLog("invite:recv", ["from": peerID.displayName])
      guard self.currentRole == "director" || self.currentRole == "follower" else {
        self.dbgLog("invite:reject", ["why": "role-off"])
        invitationHandler(false, nil); return
      }
      if self.currentRole == "director" {
        // Never accept another DIRECTOR as a "follower" — that produces two cross-connected
        // directors fighting over the same followers (split-brain). If the inviting peer is one
        // we've discovered advertising role=director, reject and let token-based conflict
        // resolution (handleDirectorConflict) decide which one demotes instead.
        let peerIsKnownDirector = self.discoveredDirectors[peerID] != nil
          || self.discoveredDirectorInfo[peerID]?["role"] == "director"
        if peerIsKnownDirector {
          self.dbgLog("invite:reject", ["from": peerID.displayName, "why": "peer-is-director"])
          invitationHandler(false, nil)
          return
        }
        // Route incoming follower to a session with room
        if let session = self.availableSessionForNewFollower() {
          self.dbgLog("invite:accept", ["from": peerID.displayName])
          invitationHandler(true, session)
        } else {
          self.dbgLog("invite:reject", ["from": peerID.displayName, "why": "sessions-full"])
          invitationHandler(false, nil) // all sessions full (> maxSessions followers)
        }
      } else {
        // A FOLLOWER MUST NEVER ACCEPT AN INVITE FROM ANOTHER FOLLOWER.
        //
        // This branch used to accept unconditionally — "a follower only ever gets invited by the
        // director" was an assumption, not something enforced here. It is not actually true:
        // followers advertise too (so a device that recently WAS director, or briefly advertised
        // as one during a promotion race, can still have a stale/in-flight invite land on another
        // follower), and nothing on the accept side checked who was asking. Confirmed on real
        // hardware 2026-08-18: a follower's session connected to ANOTHER follower
        // (`session:peer-not-director`), which silently wedged reconsiderFollowerTarget for 40+s
        // (see its own comment) because a non-director peer occupying the session looked
        // identical to being connected to the director.
        //
        // Same three-way "is this peer actually the director" check used at .connected and by
        // reconsiderFollowerTarget, so all three places cannot drift out of agreement.
        let peerIsKnownDirector = self.pendingInvitePeer == peerID
          || self.discoveredDirectors[peerID] != nil
          || self.discoveredDirectorInfo[peerID]?["role"] == "director"
        guard peerIsKnownDirector else {
          self.dbgLog("invite:reject", ["from": peerID.displayName, "why": "not-a-director"])
          invitationHandler(false, nil)
          return
        }
        self.dbgLog("invite:accept", ["from": peerID.displayName, "as": "follower"])
        invitationHandler(true, self.mcSessions.first)
      }
    }
  }

  func advertiser(_ advertiser: MCNearbyServiceAdvertiser, didNotStartAdvertisingPeer error: Error) {
    // Surface the failure to JS immediately so the director UI can warn the user.
    // This fires when Local Network permission is denied or the radio is unavailable.
    if currentRole == "director" {
      emitError(code: "DIRECTOR_START_FAILED", message: error.localizedDescription)
    }
    advertiserFailureCount += 1
    // M-F7: fast exponential backoff for the first 5 failures (transient radio/thermal hiccup or a
    // permission race), then a SLOW 45 s last-resort retry FOREVER — never give up permanently. A
    // foregrounded director whose radio hiccups past the ceiling would otherwise stay dark until a
    // foreground transition that may never come during a long foregrounded Mass. A genuine
    // permanent permission denial just keeps failing harmlessly every 45 s.
    let delay = advertiserFailureCount <= 5
      ? min(3.0 * pow(2.0, Double(advertiserFailureCount - 1)), 30.0)
      : 45.0
    DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [weak self] in
      guard let self = self, advertiser === self.advertiser, self.currentRole != "off" else { return }
      self.advertiser?.stopAdvertisingPeer(); self.advertiser?.delegate = nil; self.advertiser = nil
      self.startAdvertising()
    }
  }

  // MARK: - MCNearbyServiceBrowserDelegate

  func browser(_ browser: MCNearbyServiceBrowser, foundPeer peerID: MCPeerID, withDiscoveryInfo info: [String: String]?) {
    DispatchQueue.main.async {
      guard browser === self.browser else { return }
      guard let sessionCode = info?["session"], sessionCode == self.currentSessionCode else { return }
      let role = info?["role"] ?? ""
      self.dbgLog("found", ["peer": peerID.displayName, "prole": role])

      if role == "director" {
        let token = info?["token"] ?? peerID.displayName
        self.discoveredDirectors[peerID] = token
        self.discoveredDirectorSeenAt[peerID] = Date().timeIntervalSince1970
        self.discoveredDirectorInfo[peerID] = info ?? [:]
        if self.currentRole == "director" {
          self.handleDirectorConflict(with: token)
        } else if self.currentRole == "follower" {
          self.reconsiderFollowerTarget()
        }
      } else if role == "follower", self.currentRole == "director" {
        self.discoveredFollowers.insert(peerID)
        self.discoveredFollowerInfo[peerID] = info ?? [:]
        guard !self.allConnectedPeers.contains(peerID) else { return }
        let isLegacyFollower = info?["hgen"] == nil  // build ≤226: no hgen → director must invite
        if isLegacyFollower {
          // Legacy follower sits and waits — invite it immediately (it will never self-invite).
          guard let session = self.availableSessionForNewFollower() else { return }
          self.browser?.invitePeer(peerID, to: session, withContext: nil, timeout: Self.inviteTimeout)
        }
        // Modern follower: it will self-invite us; no action needed from director side.
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, lostPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      guard browser === self.browser else { return }
      self.dbgLog("lost", ["peer": peerID.displayName])
      self.discoveredDirectors.removeValue(forKey: peerID)
      self.discoveredDirectorSeenAt.removeValue(forKey: peerID)
      self.discoveredDirectorInfo.removeValue(forKey: peerID)
      self.discoveredFollowers.remove(peerID)
      self.discoveredFollowerInfo.removeValue(forKey: peerID)
      if self.currentRole == "follower" {
        if self.connectedDirectorPeer == peerID {
          // THE BROWSER LOSING SIGHT OF A PEER IS NOT THE SESSION DROPPING, and treating it as one
          // wedged the follower PERMANENTLY. Discovery and the MCSession are independent
          // subsystems: a director's advertisement lapses routinely under radio congestion, or
          // whenever that device restarts its own advertiser — which every peer does on its refresh
          // cycle, so this fires more often the more iPads are in the room.
          //
          // Clearing connectedDirectorPeer while the data path is still up is unrecoverable. The
          // reconsiderFollowerTarget() below returns immediately (its first guard bails when the
          // session is non-empty, emitting "connected"), so nothing ever restores the reference.
          // From then on every page arriving over that live session is dropped by the guard in
          // didReceive, the half-open watchdog is disarmed because it requires the same reference,
          // hellos stop, and the discovery-refresh timer was already paused at .connected. Even the
          // eventual real .notConnected cannot recover it: that path is gated on
          // connectedDirectorPeer == peerID, the very field this cleared. The device follows
          // nothing, forever, while its pill still reads SIGUIENDO and ⟳ does nothing — the
          // "half the devices synced" report, and a `lost` with no `session:notConnected` beside it
          // in the relay log is its signature.
          //
          // .notConnected already guards this exact case a few lines below (allConnectedPeers).
          // This is that guard's missing twin.
          if self.allConnectedPeers.contains(peerID) {
            self.dbgLog("lost:session-still-live", ["peer": peerID.displayName])
            return
          }
          self.connectedDirectorPeer = nil; self.pendingInvitePeer = nil
          self.emitState(status: "searching", message: "Se perdió el director. Buscando otro cercano.")
        }
        self.reconsiderFollowerTarget()
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers error: Error) {
    // Guard against priming browser failures — only the real browser emits to JS.
    // Fires when Local Network permission is denied or the radio is unavailable.
    if browser === self.browser, currentRole == "follower" {
      emitError(code: "FOLLOWER_START_FAILED", message: error.localizedDescription)
    }
    browserFailureCount += 1
    // M-F7: same as the advertiser — fast backoff for 5, then a slow 45 s retry forever so a
    // follower whose radio hiccups past the ceiling keeps trying to find the director instead of
    // going permanently dark on a foregrounded device.
    let delay = browserFailureCount <= 5
      ? min(3.0 * pow(2.0, Double(browserFailureCount - 1)), 30.0)
      : 45.0
    DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [weak self] in
      guard let self = self, browser === self.browser, self.currentRole != "off" else { return }
      self.browser?.stopBrowsingForPeers(); self.browser?.delegate = nil; self.browser = nil
      self.startBrowsing()
    }
  }

  // MARK: - MCSessionDelegate

  func session(_ session: MCSession, peer peerID: MCPeerID, didChange state: MCSessionState) {
    DispatchQueue.main.async {
      guard self.mcSessions.contains(where: { $0 === session }) else { return }
      let stateName = state == .connected ? "connected" : (state == .connecting ? "connecting" : "notConnected")
      self.dbgLog("session:\(stateName)", ["peer": peerID.displayName])
      switch state {
      case .connected:
        if self.currentRole == "follower" {
          // AN MCSession IS A GROUP, NOT A WIRE — and this line used to forget that.
          //
          // The director admits up to maxFollowersPerSession peers into ONE session, and Multipeer
          // then connects every member to every other member. So a follower's .connected fires for
          // the director AND for each of its fellow followers, in whatever order the radio settles.
          // Assigning connectedDirectorPeer unconditionally meant the LAST peer to connect became
          // "the director" — usually another follower. Measured on four devices, 2026-08-16: one
          // follower reassigned it twice in 524 ms, ending up pointed at an iPhone that was itself
          // only a follower.
          //
          // Everything downstream is gated on that field, so the damage is total and silent. Real
          // pages from the real director are dropped by the guard in didReceive (peer mismatch);
          // three seconds of silence trips the half-open watchdog; it tears the session down and
          // re-invites; the same race runs again. That is a ~4 s reconnect loop that never
          // converges, with the pill still reading SIGUIENDO the entire time. In one 45-minute
          // capture: 22 follower-to-follower connections, 45 forced reconnects, and heartbeat
          // delivery of 519 / 29 / 4 across three followers — the "half the devices synced" report,
          // reproduced exactly.
          //
          // It needs THREE devices to appear at all: with a single follower there is no second
          // follower to cross-connect with, which is why every two-device test ever run passed, and
          // why this survived from build 381 through 429 untouched.
          //
          // UPDATE 2026-08-18: maxFollowersPerSession dropped 7 -> 1 (see its own comment) — a
          // session now has exactly 2 members (director + one follower), so there is structurally
          // no OTHER follower to receive a spurious .connected for. This guard stays as
          // defense-in-depth (it costs nothing and the discoveredDirectors/token checks are cheap
          // to keep correct), but its "session:peer-not-director" branch should now be unreachable
          // in practice — if it ever fires again, the 1-follower-per-session topology has a hole.
          //
          // So: only the peer we deliberately invited, or one we have seen advertising role=director,
          // may claim the slot. Anything else is a peer sharing our session and is ignored here.
          // Same three-way predicate the advertiser already uses to recognise a director, so the two
          // places cannot drift: the peer we invited, one carrying a director token, or one whose
          // discoveryInfo says role=director. The token/info clauses matter because lostPeer can
          // clear discoveredDirectors moments before .connected lands for that same peer.
          let isDirector = self.pendingInvitePeer == peerID
            || self.discoveredDirectors[peerID] != nil
            || self.discoveredDirectorInfo[peerID]?["role"] == "director"
          guard isDirector else {
            self.dbgLog("session:peer-not-director", ["peer": peerID.displayName])
            // ACTIVELY DROP IT — logging and returning here used to be the whole handler. That left
            // a rejected-but-still-transport-connected peer sitting in session.connectedPeers
            // forever, which made reconsiderFollowerTarget's `connectedPeers.isEmpty` guard (its own
            // comment covers this in full) believe the follower was done and stop retrying the real
            // director. Disconnecting it here means that guard's premise is actually true again:
            // if anything is in connectedPeers, it is the director.
            session.cancelConnectPeer(peerID)
            return
          }
          self.connectedDirectorPeer = peerID; self.pendingInvitePeer = nil
          self.invalidDirectorStreak.removeValue(forKey: peerID)
          self.followerHuntingSince = 0   // connected: stop the wedged-session countdown
          self.cancelSelfDirectedTimer()
          self.pauseDiscoveryRefreshWhileConnected()
          self.startFollowerHelloTimer()
          self.startFollowerWatchdog()
          self.sendFollowerHelloIfNeeded()
          self.scheduleFollowerSnapshotProbe()
          // M-F1: prime the half-open liveness clock at connect so the watchdog measures
          // "silence since connect", not "silence since the first page ever". Without this, the
          // watchdog's `lastFollowerPageReceivedAt > 0` gate leaves it DISARMED for a follower
          // that connects during the director's brief pre-first-page window — a half-open link
          // there would never trigger a reconnect. A LIVE director's 1s mesh page-heartbeat keeps
          // this fresh (well under the 3s stale threshold), so this never false-fires.
          self.lastFollowerPageReceivedAt = Date().timeIntervalSince1970
        } else if self.currentRole == "director" {
          // If this freshly-connected peer is one we've discovered advertising as a director, we
          // have a split-brain: resolve it now via token tiebreak instead of waiting for the
          // browser cycle. handleDirectorConflict demotes US (reset) if the peer's token is
          // higher; otherwise the announce below nudges the peer to demote itself.
          if let peerToken = self.discoveredDirectors[peerID], !peerToken.isEmpty {
            self.handleDirectorConflict(with: peerToken)
            // If the conflict demoted us, resetTransport already ran and emitted DIRECTOR_CONFLICT
            // to JS — we're no longer a director, so stop here (the session is gone).
            guard self.currentRole == "director" else { return }
          }
          self.sendDirectorAnnounce(to: peerID)
          self.sendCurrentPageSnapshot(to: peerID, via: session)
        }
        self.emitState(status: "connected")
      case .connecting:
        break
      case .notConnected:
        if self.currentRole == "follower",
           (self.connectedDirectorPeer == peerID || self.pendingInvitePeer == peerID) {
          // Guard: if the peer is still connected via a parallel MPC path (e.g. a
          // legacy director invited us at the same moment we invited it, causing a
          // double-invite race), don't treat the failed outbound invite as a
          // disconnection — just clear the pending state and stay connected.
          if self.allConnectedPeers.contains(peerID) {
            self.pendingInvitePeer = nil
            return
          }
          self.connectedDirectorPeer = nil; self.pendingInvitePeer = nil
          self.stopFollowerHelloTimer()
          // EVICT A REPEATEDLY-FAILING TARGET (see invalidDirectorStreak's own comment for the
          // hardware trace this closes). Without this, reconsiderFollowerTarget's highest-token
          // sort just re-selects the exact same stale entry every cycle — a peer that keeps
          // rejecting us is exactly as persistent in discoveredDirectors as one that keeps
          // accepting us, and nothing here previously told them apart.
          let streak = (self.invalidDirectorStreak[peerID] ?? 0) + 1
          if streak >= Self.invalidDirectorEvictThreshold {
            self.dbgLog("director:evict-stale", ["peer": peerID.displayName, "streak": streak])
            self.discoveredDirectors.removeValue(forKey: peerID)
            self.discoveredDirectorInfo.removeValue(forKey: peerID)
            self.discoveredDirectorSeenAt.removeValue(forKey: peerID)
            self.invalidDirectorStreak.removeValue(forKey: peerID)
          } else {
            self.invalidDirectorStreak[peerID] = streak
          }
          // HUNTING RESUMES HERE, so the hunting pulse must too. This stopped the watchdog — the
          // 0.5 Hz retry, the BLE scan self-heal and the wedged-session escalation all went with
          // it — leaving reconnection to one retry at followerRetryDelay and then the 5-12s
          // discovery cadence. Same mistake as forceFollowerReconnect, second location.
          self.followerHuntingSince = Date().timeIntervalSince1970
          self.startFollowerWatchdog()
          self.resumeDiscoveryRefreshAfterDisconnect()
          // Give the director a grace window to reconnect before going self-directed.
          self.startSelfDirectedTimer()
          self.emitState(status: "searching", message: "Reconectando con el director...")
          let generation = self.resetGeneration
          DispatchQueue.main.asyncAfter(deadline: .now() + Self.followerRetryDelay) { [weak self] in
            guard let self = self, self.resetGeneration == generation else { return }
            self.reconsiderFollowerTarget()
          }
        } else if self.currentRole == "director" {
          self.emitState(status: self.allConnectedPeers.isEmpty ? "waiting-followers" : "connected")
        }
      @unknown default:
        break
      }
    }
  }

  func session(_ session: MCSession, didReceive data: Data, fromPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      guard self.mcSessions.contains(where: { $0 === session }) else { return }
      guard let payload = self.parseInboundPayload(data) else { return }
      guard let type = payload["type"] as? String else { return }
      let v = payload["v"] as? Int ?? 0
      if v != 0, v != Self.protocolVersion { return }

      if type == "hello" {
        if self.currentRole == "director" {
          self.sendCurrentPageSnapshot(to: peerID, via: session)
        }
        return
      }

      if type == "director_announce" {
        // M-F1 (belt-and-suspenders): a FOLLOWER treats its connected director's periodic announce
        // as a liveness beat, so the half-open watchdog stays armed even if page heartbeats are
        // momentarily missed. Only the connected director's announce counts (not a stray one).
        if self.currentRole == "follower", let dp = self.connectedDirectorPeer, dp == peerID {
          self.lastFollowerPageReceivedAt = Date().timeIntervalSince1970
        }
        // Another device just declared itself director with this token. If WE are also a
        // director, resolve the split-brain immediately: the lower token demotes to follower.
        guard self.currentRole == "director" else { return }
        guard let token = payload["token"] as? String, !token.isEmpty else { return }
        self.handleDirectorConflict(with: token)
        return
      }

      if type == "takeover_request" {
        guard self.currentRole == "director" else { return }
        guard let requestId = payload["requestId"] as? String, !requestId.isEmpty else { return }
        let requesterName = (payload["requesterName"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
        let displayName = (requesterName?.isEmpty == false) ? requesterName! : peerID.displayName
        self.pendingTakeoverRequests[requestId] = peerID
        self.emitTakeoverRequest(requestId: requestId, requesterName: displayName)
        // Auto-expire stale requests after 30 s so pendingTakeoverRequests can't grow unbounded.
        let ttlTimer = Timer.scheduledTimer(withTimeInterval: 30, repeats: false) { [weak self] _ in
          DispatchQueue.main.async {
            guard let self = self else { return }
            self.pendingTakeoverRequests.removeValue(forKey: requestId)
            self.pendingTakeoverTimers.removeValue(forKey: requestId)
          }
        }
        self.pendingTakeoverTimers[requestId] = ttlTimer
        return
      }

      if type == "takeover_approved" {
        guard self.currentRole == "follower" else { return }
        guard let requestId = payload["requestId"] as? String, !requestId.isEmpty else { return }
        self.emitTakeoverDecision(type: "takeover-approved", requestId: requestId)
        return
      }

      if type == "takeover_denied" {
        guard self.currentRole == "follower" else { return }
        guard let requestId = payload["requestId"] as? String, !requestId.isEmpty else { return }
        self.emitTakeoverDecision(type: "takeover-denied", requestId: requestId)
        return
      }

      guard type == "page" else { return }
      guard let page = payload["page"] as? Int else { return }
      let totalPages = payload["totalPages"] as? Int ?? 0
      let mode = payload["mode"] as? String ?? ""
      let bookId = payload["bookId"] as? String ?? ""
      if self.currentRole == "follower" {
        // Only honor pages from the director we're actually connected to. A stray "page" from
        // any other peer (e.g. a second director cross-connected during a split-brain window)
        // must not yank the follower onto a foreign page.
        guard let directorPeer = self.connectedDirectorPeer, directorPeer == peerID else { return }
        self.lastFollowerPageReceivedAt = Date().timeIntervalSince1970
        // Remember the BOOK the director is in. The BLE beacon carries only a page number, and a
        // page number applied to the wrong book is the one unrecoverable failure in this app — so
        // BLE reuses this context and never invents one.
        self.lastKnownTotalPages = max(0, totalPages)
        self.lastKnownMode = mode
        self.lastKnownBookId = bookId
        // Mesh is authoritative; don't re-apply older BLE from the SAME advertiser. A different
        // advertiser still rebases in onPage above — otherwise this line would re-arm the exact
        // bug it sits next to.
        self.bleAppliedSeq = self.bleLastSeenSeq
        self.emitPage(page: max(1, page), totalPages: max(0, totalPages), mode: mode, bookId: bookId, src: "mesh")
      }
    }
  }

  func session(_ session: MCSession, didReceive stream: InputStream, withName streamName: String, fromPeer peerID: MCPeerID) {}

  // PEER BUNDLE TRANSFER IS REMOVED. These two stay because MCSessionDelegate requires them, and
  // an EMPTY body is now the correct behaviour: ignore any file a peer tries to send us. Before,
  // a stranger's archive was written to disk before anything rejected it.
  func session(_ session: MCSession, didStartReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, with progress: Progress) {}

  func session(_ session: MCSession, didFinishReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, at localURL: URL?, withError error: Error?) {
    // Delete anything a peer sent rather than leaving it in the container.
    if let localURL = localURL { try? FileManager.default.removeItem(at: localURL) }
  }
  func session(_ session: MCSession, didReceiveCertificate certificate: [Any]?, fromPeer peerID: MCPeerID, certificateHandler: @escaping (Bool) -> Void) {
    certificateHandler(true)
  }
}
