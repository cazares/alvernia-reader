import CoreBluetooth
import CryptoKit
import Foundation

/// BlePageBeacon — a CONNECTIONLESS page channel over Bluetooth LE.
///
/// WHY THIS EXISTS. Every sync failure measured on 2026-08-17 was a HANDSHAKE failure, not a
/// delivery failure. A follower would discover the director immediately (~150 `found` events in two
/// minutes) and then fail to establish an MCSession four times in a row, each attempt burning up to
/// `inviteTimeout` = 30 s. The device was never lost and never out of range — it was stuck in a
/// penalty box. Tuning the timeout shrinks that window; it cannot remove it, because the thing that
/// fails IS the handshake.
///
/// A BLE advertisement has no handshake. The page number travels in the advertisement packet
/// itself, so a scanner reads the CURRENT page the moment it sees the beacon — whether it just
/// launched, just woke, or has been stranded for a minute. There is no session to establish, no
/// invite to time out, and no connection to go half-open. Staleness is bounded by scan latency
/// rather than by connection setup.
///
/// It is also a SEPARATE RADIO. Every failure we have measured lives in AWDL/peer-to-peer Wi-Fi;
/// when that is congested or duty-cycled, Bluetooth is unaffected. That is what makes this a real
/// backup rather than a second door into the same room.
///
/// ── iOS PAYLOAD LIMITS (the design constraint) ──────────────────────────────────────────────
/// Unlike Android, iOS permits exactly TWO advertisement fields: `CBAdvertisementDataLocalNameKey`
/// and `CBAdvertisementDataServiceUUIDsKey`. No manufacturer data, no service data. So the payload
/// is encoded into the local name as a compact ASCII string:
///
///     "SV<seq>.<page>"      e.g. "SV1743.59"
///
/// `seq` is monotonic so a scanner can ignore a stale or cached reading, and so two directors
/// resolve by last-write-wins exactly as the mesh already does.
///
/// ⚠️ BACKGROUNDING: when the advertising app is backgrounded, iOS DROPS the local name entirely
/// and moves the service UUID into the overflow area. So this channel carries the page only while
/// the director is foregrounded — the same limitation the mesh has. It does NOT solve the
/// backgrounded-director problem on its own; encoding the page into rotating service UUIDs would,
/// and is deliberately not attempted here until the latency question is answered.
///
/// THIS IS A MEASUREMENT PROBE FIRST. It emits `ble:page-send` and `ble:page-recv` through the same
/// telemetry the mesh uses, so `scripts/stress-analyze.mjs` can pair them and report real
/// director→follower latency alongside the mesh numbers. Ship it, measure it, and only then decide
/// whether it becomes the primary channel.
final class BlePageBeacon: NSObject {
  /// One fixed service UUID identifies a SignoVivo beacon. Scanners filter on it, which is both a
  /// power optimisation and what makes background scanning possible at all.
  private static let serviceUUID = CBUUID(string: "5194B0C0-5E00-4A11-9F30-51670A0DE5CB")
  private static let namePrefix = "SV"

  /// Called with (page, seq) whenever a scan observes a NEWER reading than the last one.
  /// (page, seq, nonce). THE NONCE IS PART OF THE CONTRACT, not a detail. seq is monotonic only
  /// WITHIN one advertising session and restarts at 0 on every director launch, so any consumer that
  /// compares seqs across sessions is wrong. This class rebases itself on a new nonce — but it used
  /// to keep that knowledge private while the caller kept its OWN seq guard, which then rejected
  /// every page from a new director whose seq had not yet climbed past the old one. Handing the
  /// nonce out makes that mistake impossible to repeat silently.
  var onPage: ((Int, Int, String) -> Void)?
  /// Telemetry sink — wired to DirectorSyncModule's dbgLog so this shares one log stream.
  var log: ((String, [String: Any]) -> Void)?

  /// The session/book code, set by DirectorSyncModule from the SAME `sessionCode` passed to
  /// startDirector/startFollower — known to every device in the room before mesh ever connects.
  ///
  /// Used as an HMAC key so a scanner can verify a broadcast was minted by someone who knows this
  /// session's code, WITHOUT any round trip: the check is a local hash comparison, so it costs
  /// nothing on the sub-second path this beacon exists for. Hardened 2026-08-18 after repeated
  /// live-hardware captures kept showing "ghost" pages (nonce+page pairs matching nothing any
  /// device had actually navigated to) getting applied — a bare nonce proves nothing about WHO
  /// sent it. This does not prove the sender is the CURRENT director (any device that has ever
  /// known this session's code, including a stale one, can still mint a valid tag) — it proves the
  /// sender is a SignoVivo device that was told this session's code, which rules out every other
  /// class of broadcaster in the room and every other book's session.
  var sessionCode: String = ""

  private var peripheral: CBPeripheralManager?
  private var central: CBCentralManager?
  private var pendingAdvert: String?
  private var lastSeenSeq = -1
  private var lastSeenNonce = ""
  private var lastPublishedPage = -1
  /// When the current pendingAdvert was requested, so the trace can show request -> on-air latency.
  private var publishRequestedAt: TimeInterval = 0
  /// Identifies THIS advertising session. Minted fresh every time publishing (re)starts.
  ///
  /// `seq` alone cannot order readings across sessions: it restarts at 0 on every director launch
  /// and it is PER-DEVICE, so a follower holding "last applied seq 1743" would reject a different
  /// director starting at 13, forever. The nonce lets a scanner notice "this is a different
  /// advertiser than the one I was tracking" and reset its baseline instead of ignoring it.
  private var sessionNonce = BlePageBeacon.newNonce()
  private static func newNonce() -> String { String(UUID().uuidString.prefix(4)).lowercased() }
  private var lastAppliedPage = -1
  private var isScanning = false

  /// The advertised seq, owned HERE and incremented once per real page change.
  ///
  /// It used to be the caller's counter, which the director's mesh heartbeat bumps ONCE PER SECOND
  /// whether the page moved or not. Over a 90-minute Mass that reaches ~5400 and keeps climbing,
  /// lengthening a name that already shares a 31-byte advertisement with a 128-bit service UUID.
  /// A number that grows forever inside a fixed-size packet is a bug with a fuse on it. Bounded by
  /// page turns instead, this stays in the low hundreds for any real session.
  private var advertSeq = 0

  /// Nonces heard recently, for contention detection. BLE carries no authority — nothing in an
  /// advertisement proves the sender is the real director — so when two advertisers disagree the
  /// only safe answer is to render NEITHER and let the mesh, which does have authority, decide.
  private var recentNonces: [String: TimeInterval] = [:]
  private static let contentionWindow: TimeInterval = 4.0

  /// When contention was last observed — separate from recentNonces, which only tracks nonces seen
  /// WITHIN contentionWindow. Hardened 2026-08-18 after a live capture on real hardware showed the
  /// exact gap this closes: two devices (nonce af9b/page 99, nonce 5990/page 50 — a genuine
  /// contention event) were both broadcasting, and the moment af9b's timestamp aged out of the
  /// 4s sliding window, page 50 was trusted and applied on the very next packet — even though
  /// nothing proved af9b had actually stopped, only that it hadn't been HEARD again recently
  /// enough. A rival that's still alive but has a slightly slower beacon interval than
  /// contentionWindow can "win" the instant its opponent's last-seen timestamp expires.
  ///
  /// This requires a SUSTAINED clean read after contention clears, not just the first uncontested
  /// packet, before trusting BLE again — giving a genuinely-stopped ghost time to actually go
  /// silent, while a still-live one keeps re-triggering contention and re-arming the cooldown.
  private var lastContentionAt: TimeInterval = 0
  /// How long to keep abstaining after contention was last seen, even once only one nonce remains
  /// in the window. Same magnitude as contentionWindow on purpose — a rival needs to have missed
  /// roughly a full window's worth of chances to be re-heard before it's trusted as gone, not just
  /// one lucky gap.
  private static let contentionCooldown: TimeInterval = 4.0

  /// Power both radios up BEFORE they are needed, so neither pays a cold start on the critical path.
  ///
  /// CBPeripheralManager was created lazily inside publish(), which put CoreBluetooth's power-on
  /// (~200-500ms, more when it triggers the permission prompt) directly between "Braulio taps Ser
  /// Director" and "the page is on the air" — the one moment the whole beacon exists to make fast.
  /// Called from BOTH roles at startup: a follower needs the central warm to hear the first
  /// advertisement, and it needs the PERIPHERAL warm too, because any follower may become the
  /// director a second later.
  ///
  /// Creating a manager does not advertise or scan; it only brings the radio up. Advertising still
  /// requires pendingAdvert, scanning still requires an explicit startScanning.
  func primeRadios() {
    if peripheral == nil { peripheral = CBPeripheralManager(delegate: self, queue: .main) }
    if central == nil { central = CBCentralManager(delegate: self, queue: .main) }
  }

  // MARK: - Director side

  /// Publish a page. Safe to call before Bluetooth is powered on — the value is held and advertised
  /// as soon as the radio is ready, so a director that taps the pill and turns a page immediately
  /// does not lose the first update.
  func publish(page: Int) {
    // PUBLISH ONLY ON A REAL CHANGE. The director's mesh heartbeat calls sendPageUpdate once per
    // SECOND, not once per page turn — so the first cut of this bumped seq, restarted the
    // advertiser and emitted telemetry every second forever. Measured over a 33-minute run: 280
    // real page turns produced 907 ble:page-send and 2436 ble:page-recv, about 1.7 relay POSTs per
    // second of pure noise, and a Bonjour advertiser tearing itself down 1 Hz for no reason.
    // An advertisement is STATE, not an event: if the page has not moved there is nothing to say.
    guard page != lastPublishedPage else { return }
    lastPublishedPage = page
    advertSeq += 1
    let seq = advertSeq
    let tag = Self.authTag(sessionCode: sessionCode, nonce: sessionNonce, seq: seq, page: page)
    let name = "\(Self.namePrefix)\(sessionNonce).\(seq).\(page).\(tag)"
    pendingAdvert = name
    let cold = peripheral == nil
    if peripheral == nil { peripheral = CBPeripheralManager(delegate: self, queue: .main) }
    publishRequestedAt = Date().timeIntervalSince1970
    startAdvertisingIfReady()
    // `cold` should be false in normal operation — primeRadios runs at startup. If it is ever true
    // the radio was not warm and this publish paid a power-on delay, which is exactly the cost this
    // beacon exists to avoid. Worth seeing in a trace rather than guessing.
    log?("ble:page-send", ["page": page, "seq": seq, "coldRadio": cold])
  }

  /// Re-assert advertising if it has stopped for any reason other than us stopping it.
  ///
  /// startAdvertisingIfReady is reachable from exactly three places — publish(), resumeOnForeground()
  /// and the power-on callback — and publish() early-returns when the page has not changed. So a
  /// director that loses its advertisement mid-session while FOREGROUNDED (radio hiccup, iOS
  /// reclaiming the peripheral) never re-arms until it happens to turn a page. Silent, and invisible
  /// from the device itself: it still believes it is publishing.
  ///
  /// Called from the director's 1 Hz heartbeat. Cheap and idempotent: a bool check, and a restart
  /// only when iOS says we are genuinely not advertising.
  func ensureAdvertising() {
    guard let p = peripheral, p.state == .poweredOn, pendingAdvert != nil, !p.isAdvertising else { return }
    log?("ble:readvertise", [:])
    startAdvertisingIfReady()
  }

  /// Re-assert scanning on the same principle, and trust CBCentralManager over our own flag.
  ///
  /// scanIfReady() guards on `!isScanning`, a local bool. If iOS stops the scan without telling us,
  /// that bool stays true and the guard refuses to restart it — the follower is deaf for the rest of
  /// the session while believing it is listening. `central.isScanning` is the actual state, so this
  /// asks the framework rather than our memory of it.
  func ensureScanning() {
    guard let c = central, c.state == .poweredOn, !c.isScanning else { return }
    if isScanning { log?("ble:rescan", [:]) }   // only interesting when we THOUGHT we were scanning
    isScanning = false
    scanIfReady()
  }

  private func startAdvertisingIfReady() {
    guard let p = peripheral, p.state == .poweredOn, let name = pendingAdvert else { return }
    // Restarting is required: iOS gives no way to mutate a live advertisement in place. This is
    // cheap here precisely BECAUSE there are no connections — nothing can be dropped by a restart,
    // which is the failure the mesh's advertiser has to guard against.
    if p.isAdvertising { p.stopAdvertising() }
    p.startAdvertising([
      CBAdvertisementDataLocalNameKey: name,
      CBAdvertisementDataServiceUUIDsKey: [Self.serviceUUID],
    ])
    // THE NUMBER THAT MATTERS on the director side: how long the page took to reach the air after
    // it was requested. With warm radios this is sub-millisecond; a large value means something put
    // a cold radio on the critical path again.
    if publishRequestedAt > 0 {
      let ms = Int((Date().timeIntervalSince1970 - publishRequestedAt) * 1000)
      log?("ble:on-air", ["ms": ms])
      publishRequestedAt = 0
    }
  }

  /// STOP ADVERTISING. This existed from the first cut and was NEVER CALLED — dead code, while the
  /// beacon it was meant to switch off advertised forever.
  ///
  /// That is the bug behind build 444's wrong-song flash. A device that had been director on song
  /// 357 published it, later stopped directing, and kept broadcasting 357 through role changes and
  /// through resetTransport, which never touched this class. Followers on 444 (where BLE was
  /// ungated) read that ghost advertisement and rendered 357 before the mesh corrected them to 101.
  ///
  /// Resetting lastPublishedPage matters as much as stopping: publish() early-returns when the page
  /// has not changed, so a device that stops directing on 357 and is later re-promoted while still
  /// on 357 would otherwise never re-advertise at all.
  ///
  /// A fresh nonce marks the next session as a new advertiser, so scanners rebase their seq
  /// tracking rather than filtering the new session out as "older".
  func stopPublishing() {
    pendingAdvert = nil
    lastPublishedPage = -1
    advertSeq = 0
    sessionNonce = Self.newNonce()
    peripheral?.stopAdvertising()
    log?("ble:stop-publishing", [:])
  }

  // MARK: - Follower side

  func startScanning() {
    if central == nil { central = CBCentralManager(delegate: self, queue: .main) }
    scanIfReady()
  }

  private func scanIfReady() {
    guard let c = central, c.state == .poweredOn, !isScanning else { return }
    isScanning = true
    // allowDuplicates is ESSENTIAL. Without it iOS reports each peripheral once and a later page
    // change is never delivered — the probe would measure "first sighting" and report a beautiful
    // number that means nothing. It costs battery, which is why this scans only while following.
    c.scanForPeripherals(
      withServices: [Self.serviceUUID],
      options: [CBCentralManagerScanOptionAllowDuplicatesKey: true]
    )
    log?("ble:scan-start", [:])
  }

  /// Foreground recovery. iOS may have torn down advertising/scanning while suspended; both sides
  /// simply start again. There is no session to rebuild and no handshake to redo — the follower's
  /// very next sighting carries the CURRENT page, so it is correct immediately rather than after a
  /// negotiation. This is the property the mesh cannot offer.
  func resumeOnForeground() {
    if pendingAdvert != nil { startAdvertisingIfReady() }
    if isScanning {
      isScanning = false          // force scanIfReady past its guard
      central?.stopScan()
      scanIfReady()
    }
  }

  func stopScanning() {
    isScanning = false
    central?.stopScan()
  }

  /// Truncated HMAC-SHA256(key: sessionCode, msg: nonce|seq|page). 4 hex chars (16 bits) is a
  /// deliberate space/security tradeoff: this rides in a BLE local name with an already-tight
  /// payload budget, and the threat model is "keep out devices that don't know the session code",
  /// not "resist a targeted forger" — the session code itself is not a high-value secret (it is
  /// shared with every device in the room). CryptoKit's HMAC is deterministic across processes,
  /// unlike Swift's default Hashable (which is randomized per launch and would silently reject
  /// every packet from any other process, including this one on a scan vs. advertise call from two
  /// different launches).
  private static func authTag(sessionCode: String, nonce: String, seq: Int, page: Int) -> String {
    let key = SymmetricKey(data: Data(sessionCode.utf8))
    let msg = "\(nonce)|\(seq)|\(page)"
    let mac = HMAC<SHA256>.authenticationCode(for: Data(msg.utf8), using: key)
    return mac.compactMap { String(format: "%02x", $0) }.joined().prefix(4).description
  }

  /// "SVa3f9.1743.59.ab12" -> (nonce a3f9, seq 1743, page 59). Returns nil for anything that is
  /// not ours, INCLUDING a well-formed packet whose tag does not verify against our own
  /// sessionCode — that covers unrelated Bluetooth devices, a different book's session running
  /// nearby, and (structurally) anything that isn't a SignoVivo device that was told this session's
  /// code.
  ///
  /// The LEGACY two-field form ("SV1743.59") and the pre-tag three-field form ("SVa3f9.1743.59")
  /// are both deliberately REJECTED rather than accepted with a synthetic tag. Builds 433-445
  /// shipped a beacon that is never switched off, so any device still running one is broadcasting a
  /// page frozen at whatever it last directed — possibly hours stale. Ignoring old formats is what
  /// makes it safe to render from BLE at all.
  private func parse(_ name: String) -> (nonce: String, seq: Int, page: Int)? {
    guard name.hasPrefix(Self.namePrefix) else { return nil }
    let body = name.dropFirst(Self.namePrefix.count)
    let parts = body.split(separator: ".")
    guard parts.count == 4, let seq = Int(parts[1]), let page = Int(parts[2]) else { return nil }
    let nonce = String(parts[0])
    let tag = String(parts[3])
    guard tag == Self.authTag(sessionCode: sessionCode, nonce: nonce, seq: seq, page: page) else {
      log?("ble:tag-mismatch", ["nonce": nonce, "page": page])
      return nil
    }
    return (nonce, seq, page)
  }
}

extension BlePageBeacon: CBPeripheralManagerDelegate {
  func peripheralManagerDidUpdateState(_ p: CBPeripheralManager) {
    guard p.state == .poweredOn else {
      log?("ble:peripheral-state", ["state": p.state.rawValue])
      return
    }
    if pendingAdvert != nil {
      startAdvertisingIfReady()
      return
    }
    // ACTIVELY CANCEL A STALE ADVERTISEMENT FROM A PRIOR PROCESS. Found on hardware 2026-08-19:
    // a follower rendered a validly-HMAC-tagged page from a nonce that NEITHER of the 4 test
    // devices ever logged sending in that session. resetTransport() already calls
    // stopPublishing() defensively at boot (before any role is chosen), but `peripheral` is still
    // nil at that exact call — created lazily, only inside publish()/primeRadios() — so
    // `peripheral?.stopAdvertising()` was a silent no-op that never reached CoreBluetooth at all.
    // A device that was FORCE-QUIT while directing (rather than cleanly demoted) never runs
    // stopPublishing() with a live peripheral either, so bluetoothd can be left holding an
    // advertisement set tied to this app's identity with no in-process owner left to cancel it.
    // This handler now fires on every power-on (primeRadios runs at the top of both
    // startDirector and startFollower) and, whenever we do NOT intend to be advertising, tells
    // CoreBluetooth so explicitly — closing the one path that could survive an unclean exit.
    p.stopAdvertising()
    log?("ble:boot-stop-stale", [:])
  }
}

extension BlePageBeacon: CBCentralManagerDelegate {
  func centralManagerDidUpdateState(_ c: CBCentralManager) {
    if c.state == .poweredOn { scanIfReady() } else { log?("ble:central-state", ["state": c.state.rawValue]) }
  }

  func centralManager(
    _ central: CBCentralManager,
    didDiscover peripheral: CBPeripheral,
    advertisementData: [String: Any],
    rssi RSSI: NSNumber
  ) {
    guard let name = advertisementData[CBAdvertisementDataLocalNameKey] as? String,
          let parsed = parse(name) else { return }
    // A DIFFERENT ADVERTISER RESETS THE BASELINE. seq is monotonic only WITHIN one advertising
    // session — it restarts at 0 on every director launch and it is per-device — so comparing seqs
    // across sessions is meaningless. Without this, a follower holding "last seen 1743" would
    // silently ignore a brand-new director starting at 1, forever, and look exactly like the mesh
    // being broken.
    // CONTENTION => ABSTAIN. Two advertisers means either a split brain or a stale beacon, and
    // nothing in a BLE packet says which one is the real director. Rendering the newest packet
    // ping-pongs the follower between two pages; preferring the incumbent pins it to a possibly
    // dead one, which is the 444 wrong-song failure made permanent. Both are worse than showing
    // nothing for a few seconds: BLE only ever buys time before the mesh arrives, and the mesh
    // resolves director conflicts by token within seconds. So when it is ambiguous, say nothing.
    let now = Date().timeIntervalSince1970
    recentNonces[parsed.nonce] = now
    recentNonces = recentNonces.filter { now - $0.value <= Self.contentionWindow }
    if recentNonces.count > 1 {
      if lastAppliedPage != -1 { log?("ble:contention", ["advertisers": recentNonces.count]) }
      lastAppliedPage = -1        // so the next uncontested reading logs as a fresh apply
      lastContentionAt = now
      return
    }
    // COOLDOWN AFTER CONTENTION (2026-08-18, hardened after a live capture — see
    // lastContentionAt's own comment for the exact packet trace this closes). The window above
    // only proves a rival hasn't been HEARD recently; it does not prove it has stopped. Keep
    // abstaining for a full cooldown period after contention was last seen, even though only one
    // nonce remains in the window — a still-live rival with a slower beacon interval than
    // contentionWindow will simply re-trigger contention and re-arm this, while a genuinely-dead
    // one goes quiet and the cooldown naturally expires.
    if now - lastContentionAt < Self.contentionCooldown {
      lastAppliedPage = -1
      return
    }

    if parsed.nonce != lastSeenNonce {
      lastSeenNonce = parsed.nonce
      lastSeenSeq = -1
      lastAppliedPage = -1
      log?("ble:new-advertiser", ["nonce": parsed.nonce, "page": parsed.page])
    }
    // Monotonic guard WITHIN a session: a cached or out-of-order packet must never move a follower
    // backward. Across sessions the nonce check above has already rebased us.
    guard parsed.seq > lastSeenSeq else { return }
    lastSeenSeq = parsed.seq
    // Log only when the PAGE moves. A scan with allowDuplicates reports every advertisement packet,
    // and one relay POST per packet is how a diagnostic turns into a denial of service against your
    // own worker.
    if parsed.page != lastAppliedPage {
      lastAppliedPage = parsed.page
      log?("ble:page-recv", ["page": parsed.page, "seq": parsed.seq, "rssi": RSSI.intValue])
    }
    onPage?(parsed.page, parsed.seq, parsed.nonce)
  }
}
