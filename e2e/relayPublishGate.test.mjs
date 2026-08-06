import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
const SRC = fs.readFileSync("src/directorRelaySync.js", "utf8");
const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");

test("no credential is sent to the relay any more", () => {
  assert.ok(!SRC.includes('"X-Director-Code"'), "still sending a director code header");
  assert.ok(!/relayPublishCode/.test(SRC), "the code variable survives");
});

test("a non-director cannot publish — checked locally, before the payload", () => {
  // The relay used to 401 a straggler frame from an ex-director. /publish is open now, so this
  // refusal has to happen on the device or that stale page lands on every web follower.
  const fn = SRC.slice(SRC.indexOf("export const publishPageToRelay"), SRC.indexOf("const payload = {"));
  assert.match(fn, /if \(!publishEnabled\) return;/, "publish is not gated locally");
});

test("stepping down disables publishing, becoming director enables it", () => {
  // Anchor on `= useCallback` — plain "const becomeDirector" also matches
  // becomeDirectorInFlightRef, which sits hundreds of lines earlier and silently inverted the slice.
  const fnBody = (name) => {
    const at = NATIVE.indexOf(`const ${name} = useCallback`);
    assert.ok(at > 0, `${name} not found`);
    return NATIVE.slice(at, NATIVE.indexOf("  }, [", at));
  };
  assert.match(fnBody("becomeFollower"), /setRelayPublishing\(false\)/, "step-down leaves publishing on");
  assert.match(fnBody("becomeDirector"), /setRelayPublishing\(true\)/, "becoming director never enables publishing");
});

test("the worker no longer gates publish on a secret", () => {
  const W = fs.readFileSync("sync-worker/src/index.ts", "utf8");
  assert.ok(!/TRANSMITTER_CODES/.test(W.replace(/\/\/.*|\/\*[\s\S]*?\*\//g, "")), "code check survives in live code");
  // The dashboard key MUST remain — it guards phone numbers, not a page number.
  assert.match(W, /FLEET_DASHBOARD_KEY/, "the dashboard key was removed too — it guards PII");
});

test("publish is still rate limited, which is now the only abuse control", () => {
  const W = fs.readFileSync("sync-worker/src/index.ts", "utf8");
  const pub = W.slice(W.indexOf("async publish("), W.indexOf("Sanitize seq"));
  assert.match(pub, /this\.rateLimited\(/, "publish lost its rate limit while also losing its auth");
});
