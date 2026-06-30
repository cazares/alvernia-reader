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
  // Handshake generation — incremented whenever the who-invites-whom rule changes.
  // Advertised in discoveryInfo so peers can see it before connecting.
  // Build ≤226: no "hgen" key → legacy (director initiates).
  // Build ≥310: hgen=2 → modern (follower initiates; director only invites legacy peers).
  private static let handshakeGeneration = "2"
  private static let maxSessionCodeLength = 12
  /// Normal (steady-state) discovery refresh interval.
  private static let discoveryRefreshInterval: TimeInterval = 25
  /// Fast refresh interval used for the first N cycles after starting, so followers find
  /// a late-arriving director within a few seconds rather than up to 25 s.
  private static let earlyRefreshInterval: TimeInterval = 5
  private static let earlyRefreshCycleCount = 6           // 6 × 5 s = 30 s burst window
  private static let inviteTimeout: TimeInterval = 30
  private static let followerRetryDelay: TimeInterval = 2
  private static let followerHelloInterval: TimeInterval = 8
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

  // MARK: - Web-bundle distribution

  /// The running app's build number — used as the web-bundle version. Followers learn the
  /// director's version passively (it rides on every outbound page payload) and via the
  /// proactive `bundle_offer` control message sent right after a peer connects.
  private var currentBundleVersion: String {
    Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "0"
  }

  /// Guards a follower so it requests + receives at most one bundle transfer at a time.
  /// Set true when a `bundle_request` is sent (and on `didStartReceivingResource`), cleared
  /// after install succeeds/fails. Prevents duplicate requests from repeated offers.
  private var bundleTransferInFlight = false
  /// Generation token for the bundle-transfer watchdog. Incremented every time a transfer is
  /// marked in-flight; the watchdog captures the current value and only fires if it still
  /// matches (i.e. no newer transfer started and nothing cleared the flag in between). Guards
  /// against a director vanishing mid-transfer and leaving bundleTransferInFlight stuck forever.
  private var bundleTransferGeneration: Int = 0
  /// How long a single bundle transfer may stay in-flight before the watchdog force-clears it.
  private static let bundleTransferWatchdogSeconds: TimeInterval = 90

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

  @objc private func handleAppDidEnterBackground() {
    appIsActive = false
    // Keep existing MCSession connections as-is, but stop any periodic churn.
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = nil
    cancelSelfDirectedTimer()
    stopFollowerHelloTimer()
  }

  @objc private func handleAppDidBecomeActive() {
    appIsActive = true
    guard currentRole != "off" else { return }

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
        sendFollowerHelloIfNeeded()
      } else {
        startDiscoveryRefreshTimer()
        startSelfDirectedTimer()
      }
    } else if currentRole == "director" {
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

  // MARK: - Web-bundle distribution (peer-to-peer)

  /// DIRECTOR → peer, sent right after the peer connects (alongside the page snapshot).
  /// Advertises the director's bundle version so an offline follower on an older build can
  /// pull the updated web bundle over the mesh. Uses the same reliable control-send helper
  /// as the takeover_* messages.
  private func sendBundleOffer(to peerID: MCPeerID) {
    guard currentRole == "director" else { return }
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "bundle_offer",
      "version": currentBundleVersion,
    ]
    sendControlPayload(payload, to: peerID)
  }

  /// FOLLOWER side. On receiving a `bundle_offer`, compare versions numerically; if the
  /// director's is newer and no transfer is already in flight, request the bundle once.
  private func handleBundleOffer(version offeredVersion: String, from peerID: MCPeerID) {
    guard currentRole == "follower" else { return }
    guard !bundleTransferInFlight else { return }
    let offered = Int(offeredVersion) ?? 0
    let mine = Int(currentBundleVersion) ?? 0
    guard offered > mine else { return }
    beginBundleTransfer()
    let payload: [String: Any] = [
      "v": Self.protocolVersion,
      "type": "bundle_request",
      "version": String(mine),
    ]
    sendControlPayload(payload, to: peerID)
  }

  /// FOLLOWER side. Marks a bundle transfer in-flight and arms a watchdog so the flag can never
  /// stick forever if the director vanishes mid-transfer and MPC never delivers a terminal
  /// didFinishReceivingResource. Idempotent for repeated arming (e.g. request then
  /// didStartReceivingResource) — each call bumps the generation so only the latest watchdog acts.
  /// Must be called on the main queue.
  private func beginBundleTransfer() {
    bundleTransferInFlight = true
    bundleTransferGeneration += 1
    let generation = bundleTransferGeneration
    DispatchQueue.main.asyncAfter(deadline: .now() + Self.bundleTransferWatchdogSeconds) { [weak self] in
      guard let self = self else { return }
      // Only act if this is still the same transfer AND it never completed/cleared.
      guard self.bundleTransferGeneration == generation, self.bundleTransferInFlight else { return }
      self.bundleTransferInFlight = false
      self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": "timeout"] as [String: Any])
    }
  }

  /// DIRECTOR side. On receiving a `bundle_request`, pack the running app's WebBundle and
  /// stream it to the requesting peer via MCSession.sendResource (NOT subject to the 8 KB
  /// control-payload cap). Cleans up the temp file on completion.
  private func handleBundleRequest(from peerID: MCPeerID) {
    guard currentRole == "director" else { return }
    let version = currentBundleVersion
    guard let session = mcSessions.first(where: { $0.connectedPeers.contains(peerID) }) else { return }
    // Packing touches the filesystem (~30 MB of assets) — do it off the main thread, then
    // hop back to the session for the actual sendResource call.
    DispatchQueue.global(qos: .utility).async { [weak self] in
      guard let self = self else { return }
      guard let packedURL = self.packWebBundle(version: version) else {
        DispatchQueue.main.async {
          self.emitError(code: "BUNDLE_PACK_FAILED", message: "No se pudo empaquetar el contenido para enviar.")
          self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": "pack"] as [String: Any])
        }
        return
      }
      DispatchQueue.main.async {
        guard self.currentRole == "director", session.connectedPeers.contains(peerID) else {
          try? FileManager.default.removeItem(at: packedURL)
          return
        }
        session.sendResource(
          at: packedURL,
          withName: "webbundle-\(version).pack",
          toPeer: peerID,
          withCompletionHandler: { [weak self] error in
            // Always clean up the temp pack once the transfer settles.
            try? FileManager.default.removeItem(at: packedURL)
            if let error = error {
              DispatchQueue.main.async {
                guard let self = self else { return }
                self.emitError(code: "BUNDLE_SEND_FAILED", message: error.localizedDescription)
                self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": "send"] as [String: Any])
              }
            }
          }
        )
      }
    }
  }

  /// Packs the app's shipped `WebBundle` blue-folder resource into a single self-describing
  /// archive with no external dependency:
  ///   [4-byte big-endian UInt32 header length][header JSON UTF-8][file0 bytes][file1 bytes]...
  /// header JSON = { "v":1, "version":"<ver>", "files":[ {"path":"index.html","len":1234}, ... ] }
  /// `path` is the POSIX-relative path (forward slashes) within the WebBundle dir.
  /// File bytes are streamed via FileHandle so we never hold the whole ~30 MB in memory.
  /// Returns the temp-file URL, or nil on any failure.
  private func packWebBundle(version: String) -> URL? {
    let fm = FileManager.default
    guard let resourceURL = Bundle.main.resourceURL else { return nil }
    let sourceDir = resourceURL.appendingPathComponent("WebBundle", isDirectory: true)
    var isDir: ObjCBool = false
    guard fm.fileExists(atPath: sourceDir.path, isDirectory: &isDir), isDir.boolValue else { return nil }

    // Enumerate regular files only, recording each file's POSIX-relative path + byte length.
    guard let enumerator = fm.enumerator(at: sourceDir, includingPropertiesForKeys: [.isRegularFileKey, .fileSizeKey]) else {
      return nil
    }
    struct PackEntry { let url: URL; let path: String; let len: Int }
    var entries: [PackEntry] = []
    let basePath = sourceDir.standardizedFileURL.path
    while let element = enumerator.nextObject() as? URL {
      let resourceValues = try? element.resourceValues(forKeys: [.isRegularFileKey, .fileSizeKey])
      guard resourceValues?.isRegularFile == true else { continue }
      let fullPath = element.standardizedFileURL.path
      // Derive the relative path under WebBundle; skip anything outside the base (shouldn't happen).
      guard fullPath.hasPrefix(basePath + "/") else { continue }
      var relPath = String(fullPath.dropFirst(basePath.count + 1))
      relPath = relPath.replacingOccurrences(of: "\\", with: "/")
      guard !relPath.isEmpty else { continue }
      let len = resourceValues?.fileSize ?? 0
      entries.append(PackEntry(url: element, path: relPath, len: len))
    }
    guard !entries.isEmpty else { return nil }
    // Deterministic order so the manifest matches the byte stream exactly.
    entries.sort { $0.path < $1.path }

    let manifest: [String: Any] = [
      "v": Self.protocolVersion,
      "version": version,
      "files": entries.map { ["path": $0.path, "len": $0.len] },
    ]
    guard let headerData = try? JSONSerialization.data(withJSONObject: manifest) else { return nil }
    guard headerData.count <= Int(UInt32.max) else { return nil }

    let destURL = fm.temporaryDirectory.appendingPathComponent("webbundle-\(version).pack")
    try? fm.removeItem(at: destURL)
    guard fm.createFile(atPath: destURL.path, contents: nil) else { return nil }
    guard let writer = try? FileHandle(forWritingTo: destURL) else { return nil }
    defer { try? writer.close() }

    // [4-byte big-endian header length]
    var headerLen = UInt32(headerData.count).bigEndian
    let lenBytes = withUnsafeBytes(of: &headerLen) { Data($0) }
    // Use the THROWING FileHandle API: the non-throwing write(_:)/readData(ofLength:) raise an
    // uncatchable Objective-C NSFileHandleOperationException on I/O failure (e.g. disk full),
    // which Swift do/catch and try? cannot trap → process crash. Route every I/O failure through
    // cleanup + nil instead.
    do {
      try writer.write(contentsOf: lenBytes)
      // [header JSON]
      try writer.write(contentsOf: headerData)
      // [file bytes, in manifest order] — streamed in chunks so memory stays flat.
      for entry in entries {
        let reader = try FileHandle(forReadingFrom: entry.url)
        defer { try? reader.close() }
        while true {
          // read(upToCount:) returns Data? — nil/empty both mean EOF.
          guard let chunk = try reader.read(upToCount: 1024 * 1024), !chunk.isEmpty else { break }
          try writer.write(contentsOf: chunk)
        }
      }
    } catch {
      try? writer.close()
      try? fm.removeItem(at: destURL)
      return nil
    }
    return destURL
  }

  /// FOLLOWER side. Unpacks a received `.pack` file into a unique `Documents/WebBundle_new-<uuid>`
  /// temp dir, validates it contains `index.html`, then atomically-ish swaps it into
  /// `Documents/WebBundle`. On
  /// success emits `{type:"bundleUpdated", version:...}`; on any failure emits `{type:"bundle-error"}`.
  /// Always clears `bundleTransferInFlight`. Reads are FileHandle-sliced so we don't load the
  /// whole archive into memory.
  private func installReceivedBundle(at localURL: URL) {
    let fm = FileManager.default
    // This runs on a background utility queue, but bundleTransferInFlight is otherwise only ever
    // touched on the main queue (beginBundleTransfer, the offer guard, the watchdog, resetTransport,
    // the resource delegates). Marshal the clear back to main to avoid an unsynchronized write.
    defer { DispatchQueue.main.async { self.bundleTransferInFlight = false } }

    func fail(_ stage: String, cleanup newDir: URL?) {
      if let newDir = newDir { try? fm.removeItem(at: newDir) }
      DispatchQueue.main.async {
        self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": stage] as [String: Any])
      }
    }

    guard let docs = fm.urls(for: .documentDirectory, in: .userDomainMask).first else {
      fail("docs", cleanup: nil); return
    }
    // Unique per-install temp dir so two installs completing close together (director resend /
    // takeover window) can't collide on a single fixed extraction path. Every exit path below —
    // success AND fail() — removes THIS dir.
    let newDir = docs.appendingPathComponent("WebBundle_new-\(UUID().uuidString)", isDirectory: true)
    try? fm.removeItem(at: newDir)
    do {
      try fm.createDirectory(at: newDir, withIntermediateDirectories: true)
    } catch {
      fail("mkdir", cleanup: newDir); return
    }

    guard let reader = try? FileHandle(forReadingFrom: localURL) else {
      fail("open", cleanup: newDir); return
    }

    // [4-byte big-endian header length]
    // Use the THROWING read(upToCount:) — the non-throwing readData(ofLength:) raises an
    // uncatchable Objective-C NSFileHandleOperationException on I/O failure (e.g. disk full),
    // crashing the process. Route any read failure through fail() instead.
    guard let lenData = try? reader.read(upToCount: 4), lenData.count == 4 else {
      try? reader.close(); fail("header-len", cleanup: newDir); return
    }
    let headerLen = lenData.withUnsafeBytes { rawBuffer -> UInt32 in
      var value: UInt32 = 0
      withUnsafeMutableBytes(of: &value) { $0.copyBytes(from: rawBuffer) }
      return UInt32(bigEndian: value)
    }
    // Bound the declared header length BEFORE allocating. A corrupt/malicious pack could claim a
    // ~4 GB header → a multi-GB contiguous alloc → memory warning → jetsam kill (blank app). 4 MB
    // is generous for a file manifest of a ~30 MB bundle.
    guard headerLen > 0, headerLen <= 4 * 1024 * 1024 else {
      try? reader.close(); fail("header-len-bounds", cleanup: newDir); return
    }

    // [header JSON]
    guard let headerData = try? reader.read(upToCount: Int(headerLen)),
          headerData.count == Int(headerLen),
          let headerObj = try? JSONSerialization.jsonObject(with: headerData) as? [String: Any],
          let files = headerObj["files"] as? [[String: Any]] else {
      try? reader.close(); fail("header-parse", cleanup: newDir); return
    }
    let headerVersion = headerObj["version"] as? String ?? ""

    // Cheap up-front sanity check: the received file size must equal the 4-byte length prefix +
    // the header JSON + the sum of every declared file length. A grossly truncated or oversized
    // archive is rejected here before we extract a single byte. (Per-file size is re-verified
    // exactly below; this catches whole-archive corruption early.)
    var declaredFileBytes = 0
    var declaredOverflow = false
    for fileEntry in files {
      let len = (fileEntry["len"] as? Int) ?? 0
      guard len >= 0 else { declaredOverflow = true; break }
      let (sum, overflow) = declaredFileBytes.addingReportingOverflow(len)
      if overflow { declaredOverflow = true; break }
      declaredFileBytes = sum
    }
    let receivedSize = (try? fm.attributesOfItem(atPath: localURL.path)[.size] as? Int) ?? nil
    if !declaredOverflow, let receivedSize = receivedSize {
      let expectedSize = 4 + Int(headerLen) + declaredFileBytes
      guard receivedSize == expectedSize else {
        try? reader.close(); fail("archive-size-mismatch", cleanup: newDir); return
      }
    }

    // [file bytes] — slice sequentially; the reader's offset advances as we go.
    for fileEntry in files {
      guard let relPath = fileEntry["path"] as? String, !relPath.isEmpty,
            let len = fileEntry["len"] as? Int, len >= 0 else {
        try? reader.close(); fail("file-entry", cleanup: newDir); return
      }
      // Reject path traversal / absolute paths defensively (we control the sender, but Murphy).
      let components = relPath.split(separator: "/").map(String.init)
      guard !components.isEmpty, !components.contains(".."), !relPath.hasPrefix("/") else {
        try? reader.close(); fail("file-path", cleanup: newDir); return
      }
      let destFileURL = newDir.appendingPathComponent(relPath)
      let destFileDir = destFileURL.deletingLastPathComponent()
      do {
        try fm.createDirectory(at: destFileDir, withIntermediateDirectories: true)
      } catch {
        try? reader.close(); fail("file-mkdir", cleanup: newDir); return
      }
      guard fm.createFile(atPath: destFileURL.path, contents: nil),
            let fileWriter = try? FileHandle(forWritingTo: destFileURL) else {
        try? reader.close(); fail("file-create", cleanup: newDir); return
      }
      var remaining = len
      var ioFailed = false
      while remaining > 0 {
        let toRead = min(remaining, 1024 * 1024)
        // Throwing read/write so a disk-full / I/O error routes through fail() instead of
        // crashing via an uncatchable NSFileHandleOperationException.
        guard let chunk = try? reader.read(upToCount: toRead) else { ioFailed = true; break }
        if chunk.isEmpty { break } // truncated archive
        do {
          try fileWriter.write(contentsOf: chunk)
        } catch {
          ioFailed = true; break
        }
        remaining -= chunk.count
      }
      try? fileWriter.close()
      guard !ioFailed else {
        try? reader.close(); fail("file-write", cleanup: newDir); return
      }
      guard remaining == 0 else {
        try? reader.close(); fail("file-truncated", cleanup: newDir); return
      }
      // Verify the extracted file's on-disk size matches the manifest exactly. A short/garbage
      // file would otherwise silently brick the follower's WebView once swapped in.
      let onDiskSize = (try? fm.attributesOfItem(atPath: destFileURL.path)[.size] as? Int) ?? nil
      guard onDiskSize == len else {
        try? reader.close(); fail("file-size-mismatch", cleanup: newDir); return
      }
    }
    try? reader.close()

    // Validate: the unpacked dir must contain a non-trivial index.html. A zero/tiny index.html
    // would brick the follower into a blank WebView, so require it to clear a small floor.
    let indexURL = newDir.appendingPathComponent("index.html")
    let indexSize = (try? fm.attributesOfItem(atPath: indexURL.path)[.size] as? Int) ?? nil
    guard let indexBytes = indexSize, indexBytes > 200 else {
      fail("index-too-small", cleanup: newDir); return
    }

    // Atomic-ish swap: move current WebBundle aside, move new into place, then drop the old.
    let target = docs.appendingPathComponent("WebBundle", isDirectory: true)
    let oldDir = docs.appendingPathComponent("WebBundle_old", isDirectory: true)
    try? fm.removeItem(at: oldDir)
    if fm.fileExists(atPath: target.path) {
      do {
        try fm.moveItem(at: target, to: oldDir)
      } catch {
        fail("swap-aside", cleanup: newDir); return
      }
    }
    do {
      try fm.moveItem(at: newDir, to: target)
    } catch {
      // Roll back: restore the old bundle if we moved it aside.
      if fm.fileExists(atPath: oldDir.path) {
        try? fm.moveItem(at: oldDir, to: target)
      }
      fail("swap-in", cleanup: newDir); return
    }
    try? fm.removeItem(at: oldDir)

    DispatchQueue.main.async {
      self.sendEvent(withName: Self.eventName, body: [
        "type": "bundleUpdated",
        "version": headerVersion,
      ] as [String: Any])
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
    bundleTransferInFlight = false
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
      guard self.currentRole == "director" || self.currentRole == "follower" else {
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
          invitationHandler(false, nil)
          return
        }
        // Route incoming follower to a session with room
        if let session = self.availableSessionForNewFollower() {
          invitationHandler(true, session)
        } else {
          invitationHandler(false, nil) // all sessions full (>14 followers)
        }
      } else {
        // Follower accepts from director into its single session
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
    // Stop retrying after 5 consecutive failures — permission is likely permanently denied
    // for this session. The user must toggle it in Settings and restart.
    guard advertiserFailureCount <= 5 else { return }
    let delay = min(3.0 * pow(2.0, Double(advertiserFailureCount - 1)), 30.0)
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
      self.discoveredDirectors.removeValue(forKey: peerID)
      self.discoveredDirectorSeenAt.removeValue(forKey: peerID)
      self.discoveredDirectorInfo.removeValue(forKey: peerID)
      self.discoveredFollowers.remove(peerID)
      self.discoveredFollowerInfo.removeValue(forKey: peerID)
      if self.currentRole == "follower" {
        if self.connectedDirectorPeer == peerID {
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
    guard browserFailureCount <= 5 else { return }
    let delay = min(3.0 * pow(2.0, Double(browserFailureCount - 1)), 30.0)
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
      switch state {
      case .connected:
        if self.currentRole == "follower" {
          self.connectedDirectorPeer = peerID; self.pendingInvitePeer = nil
          self.cancelSelfDirectedTimer()
          self.pauseDiscoveryRefreshWhileConnected()
          self.startFollowerHelloTimer()
          self.sendFollowerHelloIfNeeded()
          self.scheduleFollowerSnapshotProbe()
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
          self.sendBundleOffer(to: peerID)
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

      if type == "bundle_offer" {
        guard let version = payload["version"] as? String else { return }
        self.handleBundleOffer(version: version, from: peerID)
        return
      }

      if type == "bundle_request" {
        self.handleBundleRequest(from: peerID)
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
        self.emitPage(page: max(1, page), totalPages: max(0, totalPages), mode: mode, bookId: bookId)
      }
    }
  }

  func session(_ session: MCSession, didReceive stream: InputStream, withName streamName: String, fromPeer peerID: MCPeerID) {}

  func session(_ session: MCSession, didStartReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, with progress: Progress) {
    // A web-bundle transfer has begun. Mark in-flight (and re-arm the watchdog) so repeated
    // bundle_offers don't double-request and a stalled transfer can't wedge the flag forever.
    DispatchQueue.main.async {
      // Ignore resources arriving on a stale/removed session, or when we're not a follower —
      // otherwise a phantom transfer would re-arm the watchdog and bump the generation. didStart
      // hasn't set the flag yet, so a plain early-return is correct here (unlike didFinish, which
      // deliberately clears the flag on stale sessions).
      guard self.mcSessions.contains(where: { $0 === session }), self.currentRole == "follower" else { return }
      self.beginBundleTransfer()
    }
  }

  func session(_ session: MCSession, didFinishReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, at localURL: URL?, withError error: Error?) {
    DispatchQueue.main.async {
      guard self.mcSessions.contains(where: { $0 === session }) else {
        self.bundleTransferInFlight = false
        return
      }
      if let error = error {
        self.emitError(code: "BUNDLE_RECEIVE_FAILED", message: error.localizedDescription)
        self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": "receive"] as [String: Any])
        self.bundleTransferInFlight = false
        return
      }
      guard let localURL = localURL else {
        self.sendEvent(withName: Self.eventName, body: ["type": "bundle-error", "stage": "receive-nil"] as [String: Any])
        self.bundleTransferInFlight = false
        return
      }
      // Unpack + validate + swap off the main thread (filesystem-heavy); installReceivedBundle
      // hops back to main for every sendEvent and clears bundleTransferInFlight when done.
      let capturedURL = localURL
      DispatchQueue.global(qos: .utility).async { [weak self] in
        self?.installReceivedBundle(at: capturedURL)
      }
    }
  }
  func session(_ session: MCSession, didReceiveCertificate certificate: [Any]?, fromPeer peerID: MCPeerID, certificateHandler: @escaping (Bool) -> Void) {
    certificateHandler(true)
  }
}
