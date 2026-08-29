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

// Reads the `{ … }` body that opens at or after `from`, matching braces, so a check on a guard's
// body cannot be satisfied by text belonging to some later statement.
const bracedBodyAfter = (src, from, what) => {
  const open = src.indexOf("{", from);
  assert.ok(open > from, `${what} has no body`);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}" && --depth === 0) return src.slice(open + 1, i);
  }
  assert.fail(`${what} has unbalanced braces`);
};

test("publish is still rate limited, which is now the only abuse control", () => {
  // /publish carries no credential any more, so the token bucket is the entire abuse story: without
  // it, anyone who knows the room id can drive every web follower's page at will, mid-Mass.
  //
  // The old assertion sliced from `async publish(` to "Sanitize seq" — a marker that does not exist
  // anywhere in this file. indexOf returned -1, so the "slice" was really the whole worker from
  // publish to the end, and the unrelated limiters on /log and /join sat inside it. Deleting
  // publish's own limiter left the test green. Now both endpoints must be found, the window is
  // publish's own preamble (it ends at the decidePublish call publish is built around), the bucket's
  // capacity and refill are pinned so widening the limit to uselessness also fails, and the guard
  // must actually return rather than compute a boolean and drop it.
  const W = fs.readFileSync("sync-worker/src/index.ts", "utf8");
  const start = W.indexOf("async publish(");
  assert.ok(start > 0, "async publish( is gone — there is nothing left to anchor this window on");
  const end = W.indexOf("const decision = decidePublish", start);
  assert.ok(end > start, "publish no longer calls decidePublish — the window would have run to EOF");
  const pub = W.slice(start, end);

  const gate = pub.match(/if\s*\(\s*this\.rateLimited\(\s*ip\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*\)/);
  assert.ok(gate, "publish lost its rate limit while also losing its auth — /publish is now wide open");
  assert.equal(Number(gate[1]), 15, "the burst capacity moved — a bigger bucket is a weaker gate");
  assert.equal(Number(gate[2]), 2, "the sustained refill moved — a faster refill is a weaker gate");

  const body = bracedBodyAfter(pub, gate.index, "the publish rate-limit guard");
  assert.match(body, /\breturn\b/,
    "the rate-limit result is computed and then ignored — a flood still publishes");
  assert.doesNotMatch(body, /storage\.put|this\.broadcast\(/,
    "a rate-limited publish still persists or fans out the page");
});
