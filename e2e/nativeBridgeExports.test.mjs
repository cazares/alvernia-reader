import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

// A SWIFT METHOD NOBODY DECLARED IN THE .m DOES NOT EXIST TO JAVASCRIPT.
//
// This app runs the old architecture (app.json newArchEnabled:false), where RCT_EXTERN_MODULE
// exports a Swift method to JS ONLY if the .m also carries a matching RCT_EXTERN_METHOD. Miss one
// and there is no error anywhere: the module still loads, every other method works, and the missing
// one is simply `undefined` on NativeModules.DirectorSyncModule.
//
// forceFollowerReconnectNow shipped that way. It is what ⟳ calls to tear down a wedged MCSession —
// the one thing ⟳ can do that re-browsing cannot, since scheduleNextDiscoveryRefresh skips
// re-browsing entirely while connectedDirectorPeer is set. The JS wrapper guards its call with
// `typeof nativeModule.X === "function"`, written as a fallback for an OLDER shell, so the guard
// matched EVERY shell ever built and returned null. From the loft that is: tap ⟳, the spinner
// animates, the iPad stays on song 59. It was live in every build from the ⟳ rebuild until now.
//
// Three files have to agree, and nothing but this test makes them.

const ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const SWIFT = fs.readFileSync(path.join(ROOT, "ios/SignoVivo/DirectorSyncModule.swift"), "utf8");
const BRIDGE = fs.readFileSync(path.join(ROOT, "ios/SignoVivo/DirectorSyncModuleBridge.m"), "utf8");
const JS = fs.readFileSync(path.join(ROOT, "src/nearbyDirectorSync.js"), "utf8");

/** Every `@objc(selector:with:parts:)` in the Swift module — i.e. everything it intends to export. */
const swiftSelectors = () =>
  [...SWIFT.matchAll(/@objc\(([A-Za-z0-9_:]+)\)/g)].map((m) => m[1]).filter((s) => s.includes(":"));

/** Every RCT_EXTERN_METHOD declaration in the bridge, as full selectors. */
const bridgeSelectors = () =>
  [...BRIDGE.matchAll(/RCT_EXTERN_METHOD\(([\s\S]*?)\)\s*(?=\n\s*(?:\/\/|RCT_EXTERN_METHOD|@end))/g)].map((m) => {
    // "startDirector:(NSString *)sessionCode\n resolver:(RCTPromiseResolveBlock)resolve\n rejecter:…"
    // -> "startDirector:resolver:rejecter:"
    const labels = [...m[1].matchAll(/([A-Za-z0-9_]+)\s*:\s*\(/g)].map((x) => x[1]);
    return labels.join(":") + ":";
  });

test("every Swift @objc method is declared in the bridge .m", () => {
  const swift = swiftSelectors();
  const bridge = bridgeSelectors();
  assert.ok(swift.length > 10, "the Swift selector scan found almost nothing — the regex has drifted");
  assert.ok(bridge.length > 10, "the bridge selector scan found almost nothing — the regex has drifted");

  const missing = swift.filter((s) => !bridge.includes(s));
  assert.deepEqual(missing, [],
    `Swift exports these, the .m does not declare them, so JS sees undefined: ${missing.join(", ")}`);
});

test("the bridge declares nothing Swift does not implement", () => {
  // The mirror mistake: a declaration with no implementation is an unrecognized-selector crash the
  // first time JS calls it, rather than a silent no-op.
  const swift = swiftSelectors();
  const orphans = bridgeSelectors().filter((s) => !swift.includes(s));
  assert.deepEqual(orphans, [],
    `the .m declares methods Swift does not implement: ${orphans.join(", ")}`);
});

test("every native method the JS wrapper calls is actually exported", () => {
  // src/nearbyDirectorSync.js is the only caller, and its `typeof … === "function"` guards mean a
  // missing export degrades to silence instead of throwing — so the wrapper cannot report this.
  const called = new Set([...JS.matchAll(/nativeModule\.([A-Za-z0-9_]+)/g)].map((m) => m[1]));
  const exported = new Set(bridgeSelectors().map((s) => s.split(":")[0]));
  const missing = [...called].filter((name) => !exported.has(name));
  assert.deepEqual(missing, [],
    `the JS wrapper calls these but the bridge does not export them, so they silently no-op: ${missing.join(", ")}`);
});

test("the ⟳ wedge-breaker specifically is reachable from JS", () => {
  // Named on its own because this is the one that shipped broken, and because the generic checks
  // above would still pass if someone deleted the method from all three files together.
  assert.match(SWIFT, /@objc\(forceFollowerReconnectNow:rejecter:\)/, "Swift no longer implements it");
  assert.match(BRIDGE, /RCT_EXTERN_METHOD\(forceFollowerReconnectNow:/, "the bridge no longer exports it");
  assert.match(JS, /nativeModule\.forceFollowerReconnectNow\(\)/, "the JS wrapper no longer calls it");
  // And it must actually tear the session down, not just re-browse.
  const impl = SWIFT.slice(SWIFT.indexOf("func forceFollowerReconnectNow"), SWIFT.indexOf("@objc(requestCurrentSnapshot"));
  assert.match(impl, /forceFollowerReconnect\(staleFor: 0\)/,
    "⟳ no longer forces a reconnect — against a wedged session it is a no-op again");
});
