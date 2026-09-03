// A serving director's foreground must not tear down its own live advertiser.
//
// Mid-Mass the director pulls Control Center or glances at a notification and comes back. Two
// things fire: UIApplication.didBecomeActive → handleAppDidBecomeActive in Swift, which DELIBERATELY
// skips the advertiser restart when followers are connected ("tearing down a LIVE advertiser drops it
// out from under already-connected followers and aborts any in-flight invite"); and, milliseconds
// later, RN's AppState "active" in PdfReaderApp.tsx — which called refreshNearbyDiscovery() for EVERY
// role, before the role branches. That lands on refreshDiscovery(), whose first act is
// `advertiser?.stopAdvertisingPeer(); advertiser = nil` plus a browser teardown and a wipe of the
// discovered-peer maps. Every follower's browser fires lostPeer for the director, any invite in
// flight evaporates with no callback, and the whole choir re-handshakes and holds the stale page at
// the exact moment the director came back to turn one.
//
// becomeDirector had this identical call and was fixed by switching to the browser-only
// refreshDirectorBrowse (PdfReaderApp.tsx, "BROWSER ONLY. This called refreshNearbyDiscovery, which
// destroys the ADVERTISER first"). The foreground path kept the advertiser-destroying variant.
//
// Two pins: the JS caller routes a director to the browser-only refresh, and — defence in depth, so
// no future caller can reintroduce this — the Swift entry point itself refuses to destroy a live
// advertiser while serving. Both are re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");
const MODULE = readFileSync(join(ROOT, "ios", "SignoVivo", "DirectorSyncModule.swift"), "utf8");

/** A slice bounded by two markers that are each ASSERTED to exist — never runs to end-of-file. */
const between = (src, from, to, what) => {
  const a = src.indexOf(from);
  assert.notEqual(a, -1, `${what}: start marker missing — ${from}`);
  const b = src.indexOf(to, a);
  assert.notEqual(b, -1, `${what}: end marker missing — ${to}`);
  return src.slice(a, b);
};

/** Brace-matched body of a Swift func. indexOf("}") finds the NESTED brace; this does not. */
const swiftFunc = (name) => {
  const at = MODULE.indexOf(`func ${name}(`);
  assert.notEqual(at, -1, `func ${name} is gone`);
  const open = MODULE.indexOf("{", at);
  let depth = 0;
  for (let i = open; i < MODULE.length; i++) {
    if (MODULE[i] === "{") depth++;
    else if (MODULE[i] === "}") { depth--; if (depth === 0) return MODULE.slice(open, i + 1); }
  }
  assert.fail(`unbalanced braces in func ${name}`);
};

test("the foreground handler gives a director the browser-only refresh, never the advertiser-destroying one", () => {
  // The handler is bounded by its own two load-bearing comments/statements, both asserted present.
  const handler = between(APP, "ONE refresh, not two.", "requestCurrentSnapshot().catch", "AppState active handler");
  assert.doesNotMatch(
    handler,
    /if \(syncAvailable\) refreshNearbyDiscovery\(\)/,
    "the foreground still calls the full refresh for every role — a serving director tears down its own advertiser",
  );
  assert.match(
    handler,
    /roleRef\.current === "director"\s*\?\s*refreshDirectorBrowse\(\)\s*:\s*refreshNearbyDiscovery\(\)/,
    "the foreground refresh must route a director to refreshDirectorBrowse, exactly as becomeDirector already does",
  );
});

test("refreshNearbyDiscovery itself refuses to destroy a live advertiser while serving followers", () => {
  const body = swiftFunc("refreshNearbyDiscovery");
  assert.match(
    body,
    /if self\.currentRole == "director", !self\.allConnectedPeers\.isEmpty \{\s*self\.refreshBrowserOnly\(\)/,
    "the Swift entry point has no serving-director guard — any caller can drop the whole choir with one refresh",
  );
  // And the destructive path must be the ELSE, not an unconditional call alongside the guard.
  const guardAt = body.search(/if self\.currentRole == "director", !self\.allConnectedPeers\.isEmpty/);
  const fullAt = body.indexOf("self.refreshDiscovery()");
  assert.ok(guardAt !== -1 && fullAt !== -1 && guardAt < fullAt, "the guard must come before the full refresh");
  const betweenThem = body.slice(guardAt, fullAt);
  assert.match(betweenThem, /\} else \{/, "refreshDiscovery() must be reachable only through the else branch");
});
