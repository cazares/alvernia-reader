import test from "node:test";
import assert from "node:assert/strict";

import {
  isBackendUnreachableMessage,
  isLikelyNetworkErrorText,
  LOCAL_BACKEND_UNREACHABLE_MESSAGE,
  REMOTE_BACKEND_UNREACHABLE_MESSAGE,
  resolveBackendUnreachableMessage,
} from "../src/backendErrorMessage.js";

test("resolveBackendUnreachableMessage keeps local guidance for loopback backends", () => {
  assert.equal(
    resolveBackendUnreachableMessage("http://127.0.0.1:8000"),
    LOCAL_BACKEND_UNREACHABLE_MESSAGE
  );
  assert.equal(
    resolveBackendUnreachableMessage("http://192.168.1.42:8000"),
    LOCAL_BACKEND_UNREACHABLE_MESSAGE
  );
});

test("resolveBackendUnreachableMessage uses remote guidance for hosted backends", () => {
  assert.equal(
    resolveBackendUnreachableMessage("https://api.miguelendpoint.com"),
    REMOTE_BACKEND_UNREACHABLE_MESSAGE
  );
});

test("isLikelyNetworkErrorText catches common fetch/network failures", () => {
  assert.equal(isLikelyNetworkErrorText("Network request failed"), true);
  assert.equal(isLikelyNetworkErrorText("Failed to fetch"), true);
  assert.equal(
    isLikelyNetworkErrorText("The Internet connection appears to be offline"),
    true
  );
  assert.equal(isLikelyNetworkErrorText("Request failed (500)"), false);
});

test("isBackendUnreachableMessage matches both local and remote variants", () => {
  assert.equal(isBackendUnreachableMessage(LOCAL_BACKEND_UNREACHABLE_MESSAGE), true);
  assert.equal(isBackendUnreachableMessage(REMOTE_BACKEND_UNREACHABLE_MESSAGE), true);
  assert.equal(isBackendUnreachableMessage("Something else"), false);
});
