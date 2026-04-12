import Foundation
import MultipeerConnectivity
import React

@objc(DirectorSyncModule)
final class DirectorSyncModule: RCTEventEmitter, MCNearbyServiceAdvertiserDelegate, MCNearbyServiceBrowserDelegate, MCSessionDelegate {
  private static let serviceType = "signovivo"
  private static let eventName = "DirectorSyncEvent"
  private static let maxSessionCodeLength = 12

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
  private var reinviteTimer: Timer?

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
      self.startReinviteTimer()
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
        reject("DIRECTOR_SEND_FAILED", "No se pudo enviar la página a los dispositivos conectados.", error)
      }
    }
  }

  private func configureTransport() {
    let peerName = UIDevice.current.name.isEmpty ? UUID().uuidString : UIDevice.current.name
    let peerID = MCPeerID(displayName: "\(peerName)-\(UUID().uuidString.prefix(6))")
    let session = MCSession(peer: peerID, securityIdentity: nil, encryptionPreference: .none)
    session.delegate = self

    localPeerID = peerID
    mcSession = session
    discoveredDirectors = [:]
    pendingInvitePeer = nil
    connectedDirectorPeer = nil
  }

  /// Director: periodically re-invite any discovered followers that haven't connected yet.
  /// This fixes flaky discovery where the 3rd+ device is found but the initial invite times out.
  private func startReinviteTimer() {
    reinviteTimer?.invalidate()
    reinviteTimer = Timer.scheduledTimer(withTimeInterval: 5.0, repeats: true) { [weak self] _ in
      DispatchQueue.main.async {
        self?.reinviteDisconnectedFollowers()
      }
    }
  }

  private func reinviteDisconnectedFollowers() {
    guard currentRole == "director", let session = mcSession else { return }
    let connected = Set(session.connectedPeers)
    for follower in discoveredFollowers {
      if !connected.contains(follower) {
        browser?.invitePeer(follower, to: session, withContext: nil, timeout: 10)
      }
    }
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

  private func resetTransport(emitState shouldEmitState: Bool) {
    reinviteTimer?.invalidate()
    reinviteTimer = nil

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

    guard pendingInvitePeer != target else { return }
    pendingInvitePeer = target
    browser?.invitePeer(target, to: session, withContext: nil, timeout: 10)
    emitState(status: "connecting")
  }

  private func handleDirectorConflict(with otherToken: String) {
    guard currentRole == "director" else { return }
    guard !otherToken.isEmpty, !currentDirectorToken.isEmpty else { return }

    // The device that became director most recently (largest timestamp token) wins.
    // This means a device that enters the code after an existing director always
    // takes over — the old director is demoted to follower automatically.
    if otherToken > currentDirectorToken {
      emitError(code: "DIRECTOR_CONFLICT", message: "Un nuevo director tomó el control. Este dispositivo cambió a modo seguidor.")
      resetTransport(emitState: false) // JS will restart as follower; skip idle event
    }
  }

  private static func normalizeSessionCode(_ value: String) -> String {
    let filtered = value.uppercased().filter { $0.isLetter || $0.isNumber }
    return String(filtered.prefix(maxSessionCodeLength))
  }

  private static func randomToken() -> String {
    // Zero-padded microsecond timestamp so newer directors always have a
    // lexicographically larger token — the newest director always wins.
    let microseconds = Int64(Date().timeIntervalSince1970 * 1_000_000)
    return String(format: "%020lld", microseconds)
  }

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
      self.emitState(status: "connecting")
    }
  }

  func advertiser(
    _ advertiser: MCNearbyServiceAdvertiser,
    didNotStartAdvertisingPeer error: Error
  ) {
    DispatchQueue.main.async {
      self.emitError(code: "DIRECTOR_ADVERTISE_FAILED", message: "No se pudo anunciar este dispositivo al resto del coro.")
      self.resetTransport(emitState: true)
    }
  }

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
        // Director found a follower advertising — track and invite them.
        self.discoveredFollowers.insert(peerID)
        guard let session = self.mcSession,
              !session.connectedPeers.contains(peerID) else { return }
        self.browser?.invitePeer(peerID, to: session, withContext: nil, timeout: 10)
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
      self.emitError(code: "DIRECTOR_BROWSE_FAILED", message: "No se pudieron buscar dispositivos cercanos.")
      self.resetTransport(emitState: true)
    }
  }

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
        self.emitState(status: "connecting")
      case .notConnected:
        if self.currentRole == "follower", self.connectedDirectorPeer == peerID {
          self.connectedDirectorPeer = nil
          self.emitState(status: "searching", message: "El director se desconectó. Buscando otra vez.")
          self.reconsiderFollowerTarget()
        } else if self.currentRole == "director" {
          self.emitState(status: session.connectedPeers.isEmpty ? "waiting-followers" : "connected")
        }
      @unknown default:
        self.emitError(code: "DIRECTOR_SESSION_UNKNOWN", message: "La sesión cambió a un estado desconocido.")
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
        self.emitError(code: "DIRECTOR_PAYLOAD_INVALID", message: "Llegó un mensaje inválido desde otro dispositivo.")
        return
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
