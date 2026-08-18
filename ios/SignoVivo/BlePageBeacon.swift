import CoreBluetooth
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
  var onPage: ((Int, Int) -> Void)?
  /// Telemetry sink — wired to DirectorSyncModule's dbgLog so this shares one log stream.
  var log: ((String, [String: Any]) -> Void)?

  private var peripheral: CBPeripheralManager?
  private var central: CBCentralManager?
  private var pendingAdvert: String?
  private var lastSeenSeq = -1
  private var lastSeenNonce = ""
  private var lastPublishedPage = -1
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

  // MARK: - Director side

  /// Publish a page. Safe to call before Bluetooth is powered on — the value is held and advertised
  /// as soon as the radio is ready, so a director that taps the pill and turns a page immediately
  /// does not lose the first update.
  func publish(page: Int, seq: Int) {
    // PUBLISH ONLY ON A REAL CHANGE. The director's mesh heartbeat calls sendPageUpdate once per
    // SECOND, not once per page turn — so the first cut of this bumped seq, restarted the
    // advertiser and emitted telemetry every second forever. Measured over a 33-minute run: 280
    // real page turns produced 907 ble:page-send and 2436 ble:page-recv, about 1.7 relay POSTs per
    // second of pure noise, and a Bonjour advertiser tearing itself down 1 Hz for no reason.
    // An advertisement is STATE, not an event: if the page has not moved there is nothing to say.
    guard page != lastPublishedPage else { return }
    lastPublishedPage = page
    let name = "\(Self.namePrefix)\(sessionNonce).\(seq).\(page)"
    pendingAdvert = name
    if peripheral == nil { peripheral = CBPeripheralManager(delegate: self, queue: .main) }
    startAdvertisingIfReady()
    log?("ble:page-send", ["page": page, "seq": seq])
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

  /// "SVa3f9.1743.59" -> (nonce a3f9, seq 1743, page 59). Returns nil for anything that is not ours.
  ///
  /// The LEGACY two-field form ("SV1743.59") is deliberately REJECTED rather than accepted with a
  /// synthetic nonce. Builds 433-445 shipped a beacon that is never switched off, so any device
  /// still running one is broadcasting a page frozen at whatever it last directed — possibly hours
  /// stale. Those are exactly the advertisements that produced the wrong-song flash, and during a
  /// mixed-build window they will be in the air. Ignoring the old format is what makes it safe to
  /// render from BLE at all.
  private func parse(_ name: String) -> (nonce: String, seq: Int, page: Int)? {
    guard name.hasPrefix(Self.namePrefix) else { return nil }
    let body = name.dropFirst(Self.namePrefix.count)
    let parts = body.split(separator: ".")
    guard parts.count == 3, let seq = Int(parts[1]), let page = Int(parts[2]) else { return nil }
    return (String(parts[0]), seq, page)
  }
}

extension BlePageBeacon: CBPeripheralManagerDelegate {
  func peripheralManagerDidUpdateState(_ p: CBPeripheralManager) {
    if p.state == .poweredOn { startAdvertisingIfReady() } else { log?("ble:peripheral-state", ["state": p.state.rawValue]) }
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
    onPage?(parsed.page, parsed.seq)
  }
}
