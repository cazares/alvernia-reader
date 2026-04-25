import Foundation
import MultipeerConnectivity
import React

@objc(DirectorSyncModule)
final class DirectorSyncModule: RCTEventEmitter, MCNearbyServiceAdvertiserDelegate, MCNearbyServiceBrowserDelegate, MCSessionDelegate {
  private static let serviceType = "signovivo"
  private static let eventName = "DirectorSyncEvent"
  private static let protocolVersion = 1
  private static let maxSessionCodeLength = 12
  private static let discoveryRefreshInterval: TimeInterval = 25
  private static let inviteTimeout: TimeInterval = 30
  private static let followerRetryDelay: TimeInterval = 2
  private static let followerHelloInterval: TimeInterval = 8
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
  private var discoveredFollowers: Set<MCPeerID> = []
  private var pendingInvitePeer: MCPeerID?
  private var connectedDirectorPeer: MCPeerID?
  private var discoveryRefreshTimer: Timer?
  private var followerHelloTimer: Timer?
  private var lastFollowerHelloAt: TimeInterval = 0
  private var lastFollowerPageReceivedAt: TimeInterval = 0
  private var currentPageNumber: Int?
  private var currentTotalPages: Int = 0
  private var currentMode = ""
  private var currentBookId = ""
  private var resetGeneration = UUID()

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

  @objc(primePermissions:rejecter:)
  func primePermissions(
    _ resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      // Briefly start browsing to trigger the iOS Local Network permission dialog,
      // then stop immediately. This fires on first app launch regardless of mode.
      let tempPeerID = MCPeerID(displayName: UIDevice.current.name.isEmpty ? "signovivo" : UIDevice.current.name)
      let browser = MCNearbyServiceBrowser(peer: tempPeerID, serviceType: Self.serviceType)
      browser.startBrowsingForPeers()
      DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
        browser.stopBrowsingForPeers()
      }
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
      self.currentPageNumber = max(1, page.intValue)
      self.currentTotalPages = max(0, totalPages.intValue)
      self.currentMode = mode
      self.currentBookId = bookId
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
    let peerName = UIDevice.current.name.isEmpty ? UUID().uuidString : UIDevice.current.name
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
  }

  private func startBrowsing() {
    guard let peerID = localPeerID else { return }
    let b = MCNearbyServiceBrowser(peer: peerID, serviceType: Self.serviceType)
    b.delegate = self
    b.startBrowsingForPeers()
    browser = b
  }

  private func startDiscoveryRefreshTimer() {
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = Timer.scheduledTimer(withTimeInterval: Self.discoveryRefreshInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async { self?.refreshDiscovery() }
    }
  }

  private func startFollowerHelloTimer() {
    followerHelloTimer?.invalidate()
    lastFollowerHelloAt = 0
    lastFollowerPageReceivedAt = 0
    followerHelloTimer = Timer.scheduledTimer(withTimeInterval: Self.followerHelloInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async { self?.sendFollowerHelloIfNeeded() }
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
    advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
    browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
    startAdvertising()
    startBrowsing()
  }

  private func resetTransport(emitState shouldEmitState: Bool) {
    resetGeneration = UUID()
    discoveryRefreshTimer?.invalidate(); discoveryRefreshTimer = nil
    stopFollowerHelloTimer()
    advertiser?.stopAdvertisingPeer(); advertiser?.delegate = nil; advertiser = nil
    browser?.stopBrowsingForPeers(); browser?.delegate = nil; browser = nil
    for s in mcSessions { s.disconnect(); s.delegate = nil }
    mcSessions = []
    localPeerID = nil
    discoveredDirectors = [:]; discoveredFollowers = []
    pendingInvitePeer = nil; connectedDirectorPeer = nil
    currentRole = "off"; currentSessionCode = ""; currentDirectorToken = ""
    lastFollowerHelloAt = 0
    lastFollowerPageReceivedAt = 0
    currentPageNumber = nil; currentTotalPages = 0
    currentMode = ""; currentBookId = ""
    if shouldEmitState { emitState(status: "idle") }
  }

  // MARK: - Event emission

  private func emitState(status: String, message: String? = nil) {
    sendEvent(withName: Self.eventName, body: [
      "type": "state",
      "role": currentRole,
      "sessionCode": currentSessionCode,
      "status": status,
      "peerCount": allConnectedPeers.count,
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
    pendingInvitePeer = target
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
      guard advertiser === self.advertiser else {
        invitationHandler(false, nil); return
      }
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
    DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
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
        if self.currentRole == "director" {
          self.handleDirectorConflict(with: token)
        } else if self.currentRole == "follower" {
          self.reconsiderFollowerTarget()
        }
      } else if role == "follower", self.currentRole == "director" {
        self.discoveredFollowers.insert(peerID)
        // Only invite if not already connected in any session
        guard !self.allConnectedPeers.contains(peerID) else { return }
        if let session = self.availableSessionForNewFollower() {
          self.browser?.invitePeer(peerID, to: session, withContext: nil, timeout: Self.inviteTimeout)
        }
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, lostPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      guard browser === self.browser else { return }
      self.discoveredDirectors.removeValue(forKey: peerID)
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
    DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
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
          self.startFollowerHelloTimer()
          self.sendFollowerHelloIfNeeded()
        } else if self.currentRole == "director" {
          self.sendCurrentPageSnapshot(to: peerID, via: session)
        }
        self.emitState(status: "connected")
      case .connecting:
        break
      case .notConnected:
        if self.currentRole == "follower", self.connectedDirectorPeer == peerID {
          self.connectedDirectorPeer = nil; self.pendingInvitePeer = nil
          self.stopFollowerHelloTimer()
          self.emitState(status: "searching", message: "El director se desconectó. Reconectando...")
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
