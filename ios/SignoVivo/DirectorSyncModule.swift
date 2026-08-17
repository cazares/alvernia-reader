import Foundation
import MultipeerConnectivity
import Network
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
  private static let followerWatchdogInterval: TimeInterval = 1.0
  /// One-shot snapshot-recovery probe delay. MPC can drop the first reliable send right at
  /// .connected, so if the director's proactive snapshot AND the follower's first hello both
  /// land in that fragile window, the follower would otherwise wait a full followerHelloInterval
  /// (8 s) for the next hello. This probe re-requests the snapshot ~1.5 s after connect when no
  /// page has arrived yet, so a joining/reconnecting follower snaps to the director's page fast.
  private static let followerSnapshotProbeDelay: TimeInterval = 1.5
  /// Seconds a follower waits for a director before entering self-directed mode.
  private static let selfDirectedTimeoutSeconds: TimeInterval = 10
  private static let maxInboundPayloadBytes = 8 * 1024
  /// MCSession hard limit is 8 peers total (including local). Director occupies 1 slot → 7 followers/session.
  private static let maxFollowersPerSession = 7
  /// Two sessions → up to 14 followers simultaneously.
  private static let maxSessions = 2

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
  private var connectedDirectorPeer: MCPeerID?
  private var discoveryRefreshTimer: Timer?
  private var earlyRefreshCyclesRemaining: Int = 0
  private var selfDirectedTimer: Timer?
  private var followerHelloTimer: Timer?
  private var followerWatchdogTimer: Timer?
  private var lastFollowerHelloAt: TimeInterval = 0
  private var lastFollowerPageReceivedAt: TimeInterval = 0
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
  private var bleSeq = 0
  /// Book context last seen from a MESH page. BLE carries only a page number, so it renders only
  /// once we know which book that number refers to.
  private var lastKnownTotalPages = 0
  private var lastKnownMode = ""
  private var lastKnownBookId = ""
  private var bleLastSeenSeq = -1
  private var bleAppliedSeq = -1

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
  // Fire-and-forget POST of the Multipeer connection lifecycle so the peer-to-peer handshake can be
  // inspected remotely (no Xcode needed). `dev` is the real peer displayName so the iPad/iPhone are
  // identifiable. Best-effort; never blocks or affects sync.
  private func dbgLog(_ event: String, _ data: [String: Any] = [:]) {
    var payload: [String: Any] = [
      "t": Int(Date().timeIntervalSince1970 * 1000),
      "dev": localPeerID?.displayName ?? "?",
      "role": currentRole,
      "src": "swift",
      "build": currentBundleVersion,
      "event": event,
    ]
    payload.merge(data) { _, new in new }
    guard let url = URL(string: "https://signovivo-sync.4j4982y8jp.workers.dev/log"),
          let body = try? JSONSerialization.data(withJSONObject: [payload]) else { return }
    var req = URLRequest(url: url)
    req.httpMethod = "POST"
    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
    req.httpBody = body
    URLSession.shared.dataTask(with: req).resume()
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
      self.configureTransport()
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
      self.bleBeacon.onPage = { [weak self] page, seq in
        guard let self = self, self.currentRole == "follower" else { return }
        self.bleLastSeenSeq = seq
        guard !self.lastKnownBookId.isEmpty else {
          self.dbgLog("ble:skip-no-book", ["page": page, "seq": seq])
          return
        }
        guard seq > self.bleAppliedSeq else { return }
        self.bleAppliedSeq = seq
        self.dbgLog("ble:page-apply", ["page": page, "seq": seq])
        self.emitPage(page: max(1, page), totalPages: self.lastKnownTotalPages,
                      mode: self.lastKnownMode, bookId: self.lastKnownBookId)
      }
      self.bleBeacon.startScanning()
      self.configureTransport()
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
      self.bleSeq += 1
      self.bleBeacon.log = { [weak self] ev, data in self?.dbgLog(ev, data) }
      self.bleBeacon.publish(page: self.currentPageNumber ?? page.intValue, seq: self.bleSeq)
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

  private func configureTransport() {
    let rawName = UIDevice.current.name.isEmpty ? UUID().uuidString : UIDevice.current.name
    let peerName = String(rawName.prefix(50)) // MCPeerID displayName hard limit is 63 chars
    let peerID = MCPeerID(displayName: "\(peerName)-\(UUID().uuidString.prefix(6))")
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
  private func startFollowerWatchdog() {
    followerWatchdogTimer?.invalidate()
    let generation = resetGeneration
    followerWatchdogTimer = Timer.scheduledTimer(withTimeInterval: Self.followerWatchdogInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        autoreleasepool {
          guard let self = self, self.resetGeneration == generation, self.appIsActive,
                self.currentRole == "follower", self.connectedDirectorPeer != nil,
                self.lastFollowerPageReceivedAt > 0 else { return }
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
    stopFollowerHelloTimer()
    stopFollowerWatchdog() // restarts cleanly on the next .connected
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

  private func refreshDiscovery() {
    guard currentRole != "off" else { return }
    autoreleasepool {
      // Prune directors that haven't been seen by the browser in over 90 s (stale MPC state).
      let now = Date().timeIntervalSince1970
      let stale = discoveredDirectorSeenAt.filter { now - $0.value > 90 }.map { $0.key }
      for key in stale {
        discoveredDirectors.removeValue(forKey: key)
        discoveredDirectorSeenAt.removeValue(forKey: key)
        discoveredDirectorInfo.removeValue(forKey: key)
        discoveredFollowers.remove(key)
        discoveredFollowerInfo.removeValue(forKey: key)
      }
      advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
      browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
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
    cancelSelfDirectedTimer()
    stopFollowerHelloTimer()
    stopFollowerWatchdog()
    advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
    browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
    for s in mcSessions { s.disconnect(); s.delegate = nil }
    mcSessions = []
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

  private func emitPage(page: Int, totalPages: Int, mode: String, bookId: String) {
    sendEvent(withName: Self.eventName, body: [
      "type": "page", "page": page, "totalPages": totalPages,
      "mode": mode, "bookId": bookId, "sessionCode": currentSessionCode,
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
    guard session.connectedPeers.isEmpty else { emitState(status: "connected"); return }

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
        if elapsed < Self.inviteTimeout {
          emitState(status: "connecting")
          return
        }
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
          invitationHandler(false, nil) // all sessions full (>14 followers)
        }
      } else {
        // Follower accepts from director into its single session
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
            return
          }
          self.connectedDirectorPeer = peerID; self.pendingInvitePeer = nil
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
          self.stopFollowerWatchdog()
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
        self.bleAppliedSeq = self.bleLastSeenSeq   // mesh is authoritative; don't re-apply older BLE
        self.emitPage(page: max(1, page), totalPages: max(0, totalPages), mode: mode, bookId: bookId)
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
