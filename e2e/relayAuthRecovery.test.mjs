import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const SYNC = fs.readFileSync("src/directorRelaySync.js", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const APP = fs.readFileSync("web/src/app.js", "utf8");

// The banner warns that every signovivo.com follower has gone dark. It was shown on failure and
// removable ONLY by tapping its ×, so a relay that started working again kept shouting — and on
// 2026-08-06 it was still on screen long after the deploy that fixed it. On screen, a stale warning
// is indistinguishable from a live one, which is how people learn to ignore warnings.

// Execute the real publish-result branch so a changed condition changes the RESULT.
const onPublishResult = ({ ok, status, alreadyNotified }) => {
  const src = SYNC.slice(SYNC.indexOf("if (res && res.ok) {"), SYNC.indexOf("} else if (res && (res.status === 401"));
  let recovered = false;
  let notified = alreadyNotified;
  new Function("res", "relayAuthOkHandler", "setNotified", "getNotified", `
    let authErrorNotified = getNotified();
    ${src.replace(/authErrorNotified = false;/, "authErrorNotified = false; setNotified(false);")}
    }
  `)({ ok, status }, () => { recovered = true; }, (v) => { notified = v; }, () => alreadyNotified);
  return { recovered, notified };
};

test("a successful publish AFTER a failure signals recovery", () => {
  assert.equal(onPublishResult({ ok: true, alreadyNotified: true }).recovered, true,
    "recovery is never announced, so the banner can only be dismissed by hand");
});

test("it does NOT fire on every successful publish", () => {
  // A director who never had a problem turns pages all Mass. Announcing recovery each time would
  // post an event per page turn for a condition that never occurred.
  assert.equal(onPublishResult({ ok: true, alreadyNotified: false }).recovered, false,
    "recovery fires without a preceding failure — noise on every page turn");
});

test("the signal is carried across the bridge and lands on the banner", () => {
  assert.match(SYNC, /export const setRelayAuthOkHandler/, "no recovery handler to register");
  assert.match(NATIVE, /setRelayAuthOkHandler\(\(\) => \{/, "native never registers it");
  assert.match(NATIVE, /injectEvent\(\{ type: "relay-auth-ok" \}\)/, "native never forwards it");
  assert.match(APP, /payload\.type === "relay-auth-ok"/, "the web ignores the recovery event");
  assert.match(APP, /hideRelayAuthWarning\(\)/, "nothing takes the banner down");
});

test("both handlers are unregistered together on teardown", () => {
  // Leaving one registered after unmount means a callback into a torn-down tree.
  const eff = NATIVE.slice(NATIVE.indexOf("setRelayAuthErrorHandler((status: number)"));
  const cleanup = eff.slice(eff.indexOf("return () => {"), eff.indexOf("}, [injectEvent]);"));
  assert.match(cleanup, /setRelayAuthErrorHandler\(null\)/, "error handler leaks past unmount");
  assert.match(cleanup, /setRelayAuthOkHandler\(null\)/, "ok handler leaks past unmount");
});

test("a throwing recovery handler cannot break the publish", () => {
  // This runs inside the publish path. A diagnostic must never take down the thing it reports on.
  const blk = SYNC.slice(SYNC.indexOf("if (authErrorNotified) {"), SYNC.indexOf("} else if (res && (res.status === 401"));
  assert.match(blk, /try \{/, "the recovery callback is not guarded");
  assert.match(blk, /catch/, "a throwing handler would propagate into the publish");
});
