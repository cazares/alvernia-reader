import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

// A LIVE TAKEOVER MUST TELL THE DIRECTOR IT REPLACES — over the wire that still connects them.
//
// Hardware, 2026-09-05, build 480, three devices: the middle iPad tapped "Tomar el control" while
// connected to the iPhone director. Both were director for 45 s. The JS side dropped the follower
// link FIRST (so startDirector's DIRECTOR_TAKEOVER_REQUIRED guard would pass), then started as
// director — so Swift's split-brain announce ran with no session left to travel over, and the phone
// could learn of the newer token only through browser discovery. On real AWDL the browser does not
// re-report a peer whose role flipped, and the hold-serving guard kept the browser alive on a
// recent sighting, so the phone found out at `refresh:peers-cleared` 12:01:12 — 41 s after the
// takeover — and demoted 100 ms later. Half a minute of two directors and two BLE advertisers.
//
// The fix mints the token first, sends `director_announce` with it to connectedDirectorPeer while
// the session is up, waits for the reliable send to flush, then becomes director with THAT token.
// The receiver's handler has run handleDirectorConflict on that message since long before build
// 472, so a mixed fleet demotes just the same.

const SWIFT = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");
const BRIDGE = fs.readFileSync("ios/SignoVivo/DirectorSyncModuleBridge.m", "utf8");
const JS = fs.readFileSync("src/nearbyDirectorSync.js", "utf8");
const APP = fs.readFileSync("PdfReaderApp.tsx", "utf8");

const braceBlock = (src, at, what) => {
  const open = src.indexOf("{", at);
  assert.ok(open >= at && open !== -1, `no block found for ${what}`);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}") { depth--; if (depth === 0) return src.slice(open, i + 1); }
  }
  assert.fail(`unbalanced braces in ${what}`);
};
const swiftFunc = (name, src = SWIFT) => {
  const at = src.indexOf(`func ${name}(`);
  assert.ok(at > 0, `func ${name} is gone`);
  return braceBlock(src, at, `func ${name}`);
};

test("takeoverDirector announces the minted token to the connected director BEFORE becoming director", () => {
  const body = swiftFunc("takeoverDirector");
  // The token exists before anything is sent, and the announce carries THAT token to THAT peer.
  const mint = body.indexOf("let token = Self.randomToken()");
  assert.ok(mint > 0, "the takeover no longer mints its token up front — nothing to announce");
  const announce = body.search(/sendControlPayload\(\[[\s\S]*?"type": "director_announce",[\s\S]*?"token": token,[\s\S]*?\], to: oldDirector\)/);
  assert.ok(announce > mint, "the takeover no longer announces the new token to the director it replaces");
  assert.match(body, /guard self\.currentRole == "follower", let oldDirector = self\.connectedDirectorPeer else \{/,
    "the announce is no longer addressed to the CONNECTED director");
  // The teardown (beginDirecting) is deferred until after the send has had time to leave the radio —
  // an immediate resetTransport disconnects the session under the queued frame.
  const deferred = body.indexOf("asyncAfter(deadline: .now() + Self.takeoverAnnounceFlushSeconds)");
  assert.ok(deferred > announce, "becoming director is no longer deferred past the announce — the frame dies with the session");
  const flush = /takeoverAnnounceFlushSeconds: TimeInterval = ([0-9.]+)/.exec(SWIFT);
  assert.ok(flush && Number(flush[1]) >= 0.2, "the flush window is too short for a reliable frame to leave before the session is torn down");
  const deferredBlock = braceBlock(body, deferred, "deferred start");
  // ...and the deferred start is generation-guarded, so a role change inside the window wins.
  assert.match(deferredBlock, /guard self\.resetGeneration == generation, self\.currentRole == "follower" else \{[\s\S]*?reject\("DIRECTOR_TAKEOVER_SUPERSEDED"/,
    "the deferred start no longer yields to a role change inside the flush window — Swift becomes a director JS is not driving");
  assert.ok(body.indexOf("let generation = self.resetGeneration") > announce, "the generation is not captured before the wait");
  assert.match(deferredBlock, /self\.beginDirecting\(sessionCode: normalizedSessionCode, token: token\)/,
    "the director start no longer uses the token that was announced");
  // Nothing is torn down before the announce: resetTransport lives only inside beginDirecting.
  assert.doesNotMatch(body, /resetTransport\(/, "takeoverDirector tears the transport down itself — before the announce can leave");
  assert.match(swiftFunc("beginDirecting"), /currentDirectorToken = token\b/,
    "beginDirecting mints its own token — the announced one and the advertised one differ, so the old director keeps directing");
});

test("startDirector still refuses a connected follower — the takeover is the only door", () => {
  const body = swiftFunc("startDirector");
  assert.match(body, /if self\.currentRole == "follower", self\.connectedDirectorPeer != nil \{\s*reject\("DIRECTOR_TAKEOVER_REQUIRED"/,
    "startDirector lets a connected follower promote without announcing — the 45 s split-brain path is open again");
  assert.match(body, /self\.beginDirecting\(sessionCode: normalizedSessionCode, token: Self\.randomToken\(\)\)/,
    "startDirector no longer shares beginDirecting — the two director starts can drift");
});

test("the replaced director demotes on the announce it receives", () => {
  // The other half of the handshake is the receiver, unchanged and pinned here because the whole
  // fix depends on it: a director_announce reaching a director runs the token tiebreak, and the
  // tiebreak demotes the OLDER token.
  const receive = SWIFT.slice(SWIFT.indexOf('if type == "director_announce" {'));
  const handler = braceBlock(receive, 0, "director_announce handler");
  assert.match(handler, /guard self\.currentRole == "director" else \{ return \}/);
  assert.match(handler, /self\.handleDirectorConflict\(with: token\)/,
    "a director no longer resolves the conflict on an announce — the takeover's message is ignored");
  const conflict = swiftFunc("handleDirectorConflict");
  assert.match(conflict, /if otherToken > currentDirectorToken \{[\s\S]*resetTransport\(emitState: false\)/,
    "the older director no longer steps down for a newer token");
});

test("a director re-browses the moment a follower drops, so a taker on an older build is found within seconds", () => {
  // The announce is sent by the TAKER, so it protects only when the taker runs 481+. A 472 taker still
  // drops its link and starts directing silently; the replaced director's only signal is that drop.
  // A browser-only refresh right there re-reports the peer with role=director in a second or two,
  // instead of at the next ~25 s full rebuild (41 s on hardware, 2026-09-05).
  const at = SWIFT.indexOf("case .notConnected:");
  assert.ok(at > 0, "the .notConnected handler is gone");
  const handler = SWIFT.slice(at, SWIFT.indexOf("@unknown default:", at));
  assert.ok(handler.length > 500 && handler.length < 12000, "the .notConnected slice is mis-bounded");
  const directorAt = handler.indexOf('} else if self.currentRole == "director" {');
  assert.ok(directorAt > 0, "the director branch of .notConnected is gone");
  assert.match(braceBlock(handler, directorAt + 1, "director .notConnected branch"), /self\.refreshBrowserOnly\(\)/,
    "a director no longer re-browses when a follower drops — a pre-481 taker is found only at the next full rebuild");
  // ...and refreshBrowserOnly is still browser-ONLY and rate-limited, or this call would churn the advertiser.
  const refresh = swiftFunc("refreshBrowserOnly");
  assert.doesNotMatch(refresh, /advertiser\?\.stopAdvertisingPeer|startAdvertising\(\)/, "refreshBrowserOnly touches the advertiser");
  assert.match(refresh, /guard now - lastRefreshAt >= Self\.minRefreshInterval else \{ return \}/, "refreshBrowserOnly lost its rate limit");
});

test("the takeover is exported to JavaScript and the JS path no longer drops the link first", () => {
  assert.match(BRIDGE, /RCT_EXTERN_METHOD\(takeoverDirector:\(NSString \*\)sessionCode\s*resolver:\(RCTPromiseResolveBlock\)resolve\s*rejecter:\(RCTPromiseRejectBlock\)reject\)/,
    "takeoverDirector is not declared in the bridge .m — JS sees undefined and falls back to drop-then-start");
  const wrapperAt = JS.indexOf("export const takeoverNearbyDirector = async (sessionCode) => {");
  assert.ok(wrapperAt > 0, "the JS wrapper for the takeover is gone");
  assert.match(braceBlock(JS, wrapperAt, "takeoverNearbyDirector"), /return nativeModule\.takeoverDirector\(sessionCode\);/,
    "the JS wrapper does not call the native takeover");
  const at = APP.indexOf("const becomeDirector = useCallback(");
  assert.ok(at > 0, "becomeDirector is gone");
  const become = APP.slice(at, APP.indexOf("    [\n      syncAvailable,", at));
  assert.ok(become.length > 2000 && become.length < 20000, "the becomeDirector slice is mis-bounded");
  assert.doesNotMatch(become, /resetNearbyDirectorSync\(/,
    "becomeDirector drops the follower link before starting again — the replaced director is never told");
  assert.match(become, /const startAsDirector = wasFollower \? takeoverNearbyDirector : startNearbyDirector;/,
    "a former follower no longer goes through the announcing takeover");
  assert.match(become, /await startAsDirector\(DIRECTOR_SESSION\);[\s\S]*await startAsDirector\(DIRECTOR_SESSION\);/,
    "the retry does not use the same (takeover-aware) start");
});
