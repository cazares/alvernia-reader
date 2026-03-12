import test from "node:test";
import assert from "node:assert/strict";

import {
  isDirectSourceOverrideRejected,
  stripDirectSourceOverride,
} from "../src/jobStartApiPolicy.js";

test("detects backend rejection of direct source overrides", () => {
  assert.equal(
    isDirectSourceOverrideRejected(
      "direct audio_url/audio_id overrides are disabled in server-download-only mode"
    ),
    true
  );
  assert.equal(isDirectSourceOverrideRejected("some other backend error"), false);
});

test("stripDirectSourceOverride removes direct source fields and preserves other payload", () => {
  const payload = stripDirectSourceOverride({
    query: "Artist - Song",
    audio_id: "abc123",
    audio_url: "https://example.com/audio.m4a",
    vocals: 20,
    offset_sec: 0.5,
  });

  assert.deepEqual(payload, {
    query: "Artist - Song",
    vocals: 20,
    offset_sec: 0.5,
  });
});
