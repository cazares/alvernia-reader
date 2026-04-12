import Foundation
import MultipeerConnectivity
import React

@objc(DirectorSyncModule)
final class DirectorSyncModule: RCTEventEmitter, MCNearbyServiceAdvertiserDelegate, MCNearbyServiceBrowserDelegate, MCSessionDelegate {
  private static let serviceType = "signovivo"
  private static let eventName = "DirectorSyncEvent"
  private static let maxSessionCodeLength = 12
  /// How often to restart browser+advertiser to keep discovery fresh (especially offline/Bluetooth)
  private static let discoveryRefreshInterval: TimeInterval = 25
  /// Invitation timeout — long enough for Bluetooth negotiation
  private static let inviteTimeout: TimeInterval = 30
  /// Delay before follower retries after disconnect
  private static let followerRetryDelay: TimeInterval = 2

  private var localPeerID: MCPeerID?
  private var mcSession: MCSession?
  private var advertiser: MCNearbyServiceAdvertiser?
  private var browser: MCNearbyServiceBrowser?
  private var currentRole = "off"
  private var currentSessionCode = ""
  private var currentDirectorToken = ""
  private var discoveredDirectors: [MCPeerID: String] = [:]
  private var discoveredFollowers: Set<MCPeerID> = []
  private var pendingInvitePeer: MCPeerID?
  private var connectedDirectorPeer: MCPeerID?
  /// Periodic timer that restarts browser+advertiser to keep discovery alive offline
  private var discoveryRefreshTimer: Timer?

  override static func requiresMainQueueSetup() -> Bool {
    false
  }

  override func supportedEvents() -> [String]! {
    [Self.eventName]
  }

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
      resolve([
        "role": "director",
        "sessionCode": normalizedSessionCode,
      ])
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
      resolve([
        "role": "follower",
        "sessionCode": normalizedSessionCode,
      ])
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

  @objc(sendPageUpdate:totalPages:resolver:rejecter:)
  func sendPageUpdate(
    _ page: NSNumber,
    totalPages: NSNumber,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    DispatchQueue.main.async {
      guard self.currentRole == "director" else {
        reject("DIRECTOR_ROLE_INVALID", "Solo el director puede enviar páginas.", nil)
        return
      }

      guard let session = self.mcSession else {
        reject("DIRECTOR_SESSION_MISSING", "La sesión del director no está disponible.", nil)
        return
      }

      guard !session.connectedPeers.isEmpty else {
        self.emitState(status: "waiting-followers")
        resolve(["deliveredPeers": 0])
        return
      }

      let payload: [String: Any] = [
        "type": "page",
        "page": max(1, page.intValue),
        "totalPages": max(0, totalPages.intValue),
      ]

      do {
        let data = try JSONSerialization.data(withJSONObject: payload)
        try session.send(data, toPeers: session.connectedPeers, with: .reliable)
        self.emitState(status: "connected")
        resolve(["deliveredPeers": session.connectedPeers.count])
      } catch {
        // Reliable send failed — try unreliable as fallback (better than nothing)
        if let data = try? JSONSerialization.data(withJSONObject: payload) {
          try? session.send(data, toPeers: session.connectedPeers, with: .unreliable)
        }
        resolve(["deliveredPeers": 0])
      }
    }
  }

  // MARK: - Transport setup

  private func configureTransport() {
    let peerName = UIDevice.current.name.isEmpty ? UUID().uuidString : UIDevice.current.name
    let peerID = MCPeerID(displayName: "\(peerName)-\(UUID().uuidString.prefix(6))")
    // encryptionPreference: .none ensures both WiFi AND Bluetooth are used simultaneously
    let session = MCSession(peer: peerID, securityIdentity: nil, encryptionPreference: .none)
    session.delegate = self

    localPeerID = peerID
    mcSession = session
    discoveredDirectors = [:]
    discoveredFollowers = []
    pendingInvitePeer = nil
    connectedDirectorPeer = nil
  }

  private func startAdvertising() {
    guard (currentRole == "director" || currentRole == "follower"), let peerID = localPeerID else { return }
    var discoveryInfo: [String: String] = [
      "session": currentSessionCode,
      "role": currentRole,
    ]
    if currentRole == "director" {
      discoveryInfo["token"] = currentDirectorToken
    }
    let advertiser = MCNearbyServiceAdvertiser(peer: peerID, discoveryInfo: discoveryInfo, serviceType: Self.serviceType)
    advertiser.delegate = self
    advertiser.startAdvertisingPeer()
    self.advertiser = advertiser
  }

  private func startBrowsing() {
    guard let peerID = localPeerID else { return }
    let browser = MCNearbyServiceBrowser(peer: peerID, serviceType: Self.serviceType)
    browser.delegate = self
    browser.startBrowsingForPeers()
    self.browser = browser
  }

  /// Periodically restart browser + advertiser to keep discovery alive.
  /// MultipeerConnectivity's Bonjour layer can go stale after ~30s in offline
  /// (no-internet) environments, especially over Bluetooth. Restarting forces
  /// a fresh mDNS/BLE announcement cycle without dropping existing MCSession connections.
  private func startDiscoveryRefreshTimer() {
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = Timer.scheduledTimer(withTimeInterval: Self.discoveryRefreshInterval, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        self?.refreshDiscovery()
      }
    }
  }

  private func refreshDiscovery() {
    guard currentRole != "off" else { return }
    // Stop and restart browser+advertiser — keeps mDNS/BLE announcements fresh
    // The MCSession stays alive so existing connections are NOT dropped.
    advertiser?.stopAdvertisingPeer()
    advertiser?.delegate = nil
    advertiser = nil

    browser?.stopBrowsingForPeers()
    browser?.delegate = nil
    browser = nil

    startAdvertising()
    startBrowsing()
  }

  private func resetTransport(emitState shouldEmitState: Bool) {
    discoveryRefreshTimer?.invalidate()
    discoveryRefreshTimer = nil

    advertiser?.stopAdvertisingPeer()
    advertiser?.delegate = nil
    advertiser = nil

    browser?.stopBrowsingForPeers()
    browser?.delegate = nil
    browser = nil

    mcSession?.disconnect()
    mcSession?.delegate = nil
    mcSession = nil

    localPeerID = nil
    discoveredDirectors = [:]
    discoveredFollowers = []
    pendingInvitePeer = nil
    connectedDirectorPeer = nil
    currentRole = "off"
    currentSessionCode = ""
    currentDirectorToken = ""

    if shouldEmitState {
      emitState(status: "idle")
    }
  }

  // MARK: - Event emission

  private func emitState(status: String, message: String? = nil) {
    let payload: [String: Any] = [
      "type": "state",
      "role": currentRole,
      "sessionCode": currentSessionCode,
      "status": status,
      "peerCount": mcSession?.connectedPeers.count ?? 0,
      "directorCount": discoveredDirectors.count,
      "message": message ?? "",
    ]
    sendEvent(withName: Self.eventName, body: payload)
  }

  private func emitError(code: String, message: String) {
    let payload: [String: Any] = [
      "type": "error",
      "code": code,
      "message": message,
      "role": currentRole,
      "sessionCode": currentSessionCode,
    ]
    sendEvent(withName: Self.eventName, body: payload)
  }

  private func emitPage(page: Int, totalPages: Int) {
    let payload: [String: Any] = [
      "type": "page",
      "page": page,
      "totalPages": totalPages,
      "sessionCode": currentSessionCode,
    ]
    sendEvent(withName: Self.eventName, body: payload)
  }

  // MARK: - Connection logic

  private func reconsiderFollowerTarget() {
    guard currentRole == "follower", let session = mcSession else { return }
    guard session.connectedPeers.isEmpty else {
      emitState(status: "connected")
      return
    }

    let sortedPeers = discoveredDirectors.sorted { lhs, rhs in
      if lhs.value == rhs.value {
        return lhs.key.displayName < rhs.key.displayName
      }
      return lhs.value < rhs.value
    }

    guard let target = sortedPeers.first?.key else {
      pendingInvitePeer = nil
      emitState(status: "searching")
      return
    }

    if sortedPeers.count > 1 {
      emitState(status: "resolving-conflict", message: "Hay varios directores cercanos. Eligiendo uno automáticamente.")
    }

    // Always allow re-invite (clear stale pending state)
    pendingInvitePeer = target
    browser?.invitePeer(target, to: session, withContext: nil, timeout: Self.inviteTimeout)
    emitState(status: "connecting")
  }

  private func handleDirectorConflict(with otherToken: String) {
    guard currentRole == "director" else { return }
    guard !otherToken.isEmpty, !currentDirectorToken.isEmpty else { return }

    if otherToken > currentDirectorToken {
      emitError(code: "DIRECTOR_CONFLICT", message: "Un nuevo director tomó el control. Este dispositivo cambió a modo seguidor.")
      resetTransport(emitState: false)
    }
  }

  private static func normalizeSessionCode(_ value: String) -> String {
    let filtered = value.uppercased().filter { $0.isLetter || $0.isNumber }
    return String(filtered.prefix(maxSessionCodeLength))
  }

  private static func randomToken() -> String {
    let microseconds = Int64(Date().timeIntervalSince1970 * 1_000_000)
    return String(format: "%020lld", microseconds)
  }

  // MARK: - MCNearbyServiceAdvertiserDelegate

  func advertiser(
    _ advertiser: MCNearbyServiceAdvertiser,
    didReceiveInvitationFromPeer peerID: MCPeerID,
    withContext context: Data?,
    invitationHandler: @escaping (Bool, MCSession?) -> Void
  ) {
    DispatchQueue.main.async {
      guard (self.currentRole == "director" || self.currentRole == "follower"),
            let session = self.mcSession else {
        invitationHandler(false, nil)
        return
      }
      invitationHandler(true, session)
    }
  }

  func advertiser(
    _ advertiser: MCNearbyServiceAdvertiser,
    didNotStartAdvertisingPeer error: Error
  ) {
    DispatchQueue.main.async {
      // Don't reset everything — just try to re-advertise after a short delay
      DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
        guard let self = self, self.currentRole != "off" else { return }
        self.advertiser?.stopAdvertisingPeer()
        self.advertiser?.delegate = nil
        self.advertiser = nil
        self.startAdvertising()
      }
    }
  }

  // MARK: - MCNearbyServiceBrowserDelegate

  func browser(
    _ browser: MCNearbyServiceBrowser,
    foundPeer peerID: MCPeerID,
    withDiscoveryInfo info: [String : String]?
  ) {
    DispatchQueue.main.async {
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
        guard let session = self.mcSession,
              !session.connectedPeers.contains(peerID) else { return }
        self.browser?.invitePeer(peerID, to: session, withContext: nil, timeout: Self.inviteTimeout)
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, lostPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      self.discoveredDirectors.removeValue(forKey: peerID)
      self.discoveredFollowers.remove(peerID)
      if self.currentRole == "follower" {
        if self.connectedDirectorPeer == peerID {
          self.connectedDirectorPeer = nil
          self.pendingInvitePeer = nil
          self.emitState(status: "searching", message: "Se perdió el director. Buscando otro cercano.")
        }
        self.reconsiderFollowerTarget()
      }
    }
  }

  func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers error: Error) {
    DispatchQueue.main.async {
      // Don't reset — just retry after a short delay
      DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
        guard let self = self, self.currentRole != "off" else { return }
        self.browser?.stopBrowsingForPeers()
        self.browser?.delegate = nil
        self.browser = nil
        self.startBrowsing()
      }
    }
  }

  // MARK: - MCSessionDelegate

  func session(_ session: MCSession, peer peerID: MCPeerID, didChange state: MCSessionState) {
    DispatchQueue.main.async {
      switch state {
      case .connected:
        if self.currentRole == "follower" {
          self.connectedDirectorPeer = peerID
          self.pendingInvitePeer = nil
        }
        self.emitState(status: "connected")
      case .connecting:
        break // don't emit — avoids spamming JS during negotiation
      case .notConnected:
        if self.currentRole == "follower", self.connectedDirectorPeer == peerID {
          self.connectedDirectorPeer = nil
          self.pendingInvitePeer = nil
          self.emitState(status: "searching", message: "El director se desconectó. Reconectando...")
          // Retry after short delay — gives Bluetooth/WiFi time to stabilize
          DispatchQueue.main.asyncAfter(deadline: .now() + Self.followerRetryDelay) { [weak self] in
            self?.reconsiderFollowerTarget()
          }
        } else if self.currentRole == "director" {
          self.emitState(status: session.connectedPeers.isEmpty ? "waiting-followers" : "connected")
        }
      @unknown default:
        break
      }
    }
  }

  func session(_ session: MCSession, didReceive data: Data, fromPeer peerID: MCPeerID) {
    DispatchQueue.main.async {
      guard
        let payload = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
        let type = payload["type"] as? String,
        type == "page",
        let page = payload["page"] as? Int
      else {
        return // silently ignore malformed data — don't spam error events
      }

      let totalPages = payload["totalPages"] as? Int ?? 0
      if self.currentRole == "follower" {
        self.emitPage(page: page, totalPages: totalPages)
      }
    }
  }

  func session(
    _ session: MCSession,
    didReceive stream: InputStream,
    withName streamName: String,
    fromPeer peerID: MCPeerID
  ) {}

  func session(
    _ session: MCSession,
    didStartReceivingResourceWithName resourceName: String,
    fromPeer peerID: MCPeerID,
    with progress: Progress
  ) {}

  func session(
    _ session: MCSession,
    didFinishReceivingResourceWithName resourceName: String,
    fromPeer peerID: MCPeerID,
    at localURL: URL?,
    withError error: Error?
  ) {}

  func session(
    _ session: MCSession,
    didReceiveCertificate certificate: [Any]?,
    fromPeer peerID: MCPeerID,
    certificateHandler: @escaping (Bool) -> Void
  ) {
    certificateHandler(true)
  }
}
