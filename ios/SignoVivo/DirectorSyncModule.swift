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
  private var discoveredFollowers: Set<MCPeerID> = []
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

  private func sendCurrentPageSnapshot(to peerID: MCPeerID, via session: MCSession) {
    guard currentRole == "director", let page = currentPageNumber, let data = pagePayload(page: page, totalPages: currentTotalPages) else {
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
    discoveredFollowers = []
    pendingInvitePeer = nil
    connectedDirectorPeer = nil
  }

  private func startAdvertising() {
    guard (currentRole == "director" || currentRole == "follower"), let peerID = localPeerID else { return }
    var discoveryInfo: [String: String] = ["session": currentSessionCode, "role": currentRole]
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

  private func refreshDiscovery() {
    guard currentRole != "off" else { return }
    autoreleasepool {
      // Prune directors that haven't been seen by the browser in over 90 s (stale MPC state).
      let now = Date().timeIntervalSince1970
      let stale = discoveredDirectorSeenAt.filter { now - $0.value > 90 }.map { $0.key }
      for key in stale {
        discoveredDirectors.removeValue(forKey: key)
        discoveredDirectorSeenAt.removeValue(forKey: key)
        discoveredFollowers.remove(key)
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
    discoveredDirectors = [:]; discoveredDirectorSeenAt = [:]; discoveredFollowers = []
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

    let sorted = discoveredDirectors.sorted {
      $0.value == $1.value ? $0.key.displayName < $1.key.displayName : $0.value < $1.value
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
    pendingInvitePeer = target
    pendingInviteTimestamp = Date().timeIntervalSince1970
    browser?.invitePeer(target, to: session, withContext: nil, timeout: Self.inviteTimeout)
    emitState(status: "connecting")
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
    String(format: "%020lld", Int64(Date().timeIntervalSince1970 * 1_000_000))
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
        if self.currentRole == "director" {
          self.handleDirectorConflict(with: token)
        } else if self.currentRole == "follower" {
          self.reconsiderFollowerTarget()
        }
      } else if role == "follower", self.currentRole == "director" {
        self.discoveredFollowers.insert(peerID)
        // Modern followers initiate the connection themselves. For legacy followers
        // (build ≤226) that wait to be invited, the director falls back after 1 s.
        guard !self.allConnectedPeers.contains(peerID) else { return }
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
          guard let self = self, self.currentRole == "director" else { return }
          guard !self.allConnectedPeers.contains(peerID),
                self.discoveredFollowers.contains(peerID),
                let session = self.availableSessionForNewFollower() else { return }
          self.browser?.invitePeer(peerID, to: session, withContext: nil, timeout: Self.inviteTimeout)
        }
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, lostPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      guard browser === self.browser else { return }
      self.discoveredDirectors.removeValue(forKey: peerID)
      self.discoveredDirectorSeenAt.removeValue(forKey: peerID)
      self.discoveredFollowers.remove(peerID)
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
        } else if self.currentRole == "director" {
          self.sendCurrentPageSnapshot(to: peerID, via: session)
        }
        self.emitState(status: "connected")
      case .connecting:
        break
      case .notConnected:
        if self.currentRole == "follower",
           (self.connectedDirectorPeer == peerID || self.pendingInvitePeer == peerID) {
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
        self.lastFollowerPageReceivedAt = Date().timeIntervalSince1970
        self.emitPage(page: max(1, page), totalPages: max(0, totalPages), mode: mode, bookId: bookId)
      }
    }
  }

  func session(_ session: MCSession, didReceive stream: InputStream, withName streamName: String, fromPeer peerID: MCPeerID) {}
  func session(_ session: MCSession, didStartReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, with progress: Progress) {}
  func session(_ session: MCSession, didFinishReceivingResourceWithName resourceName: String, fromPeer peerID: MCPeerID, at localURL: URL?, withError error: Error?) {}
  func session(_ session: MCSession, didReceiveCertificate certificate: [Any]?, fromPeer peerID: MCPeerID, certificateHandler: @escaping (Bool) -> Void) {
    certificateHandler(true)
  }
}
