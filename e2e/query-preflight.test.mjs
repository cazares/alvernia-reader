import test from "node:test";
import assert from "node:assert/strict";

import { resolveQueryPreflightResult } from "../src/queryPreflight.js";

test("resolveQueryPreflightResult promotes normalized query and source id", () => {
  const result = resolveQueryPreflightResult({
    rawQuery: "red hot chili peppers the zephyr song lyrics",
    normalizedPayload: {
      artist: "Red Hot Chili Peppers",
      track: "The Zephyr Song",
      normalized_query: "Red Hot Chili Peppers - The Zephyr Song",
      video_id: "0fcRa5Z6LmU",
    },
  });

  assert.deepEqual(result, {
    query: "Red Hot Chili Peppers - The Zephyr Song",
    audioId: "0fcRa5Z6LmU",
    normalizedSong: {
      artist: "Red Hot Chili Peppers",
      title: "The Zephyr Song",
    },
  });
});

test("resolveQueryPreflightResult preserves explicit source choice", () => {
  const result = resolveQueryPreflightResult({
    rawQuery: "red hot chili peppers the zephyr song lyrics",
    explicitAudioId: "manualVideo123",
    normalizedPayload: {
      artist: "Red Hot Chili Peppers",
      track: "The Zephyr Song",
      normalized_query: "Red Hot Chili Peppers - The Zephyr Song",
      video_id: "0fcRa5Z6LmU",
    },
  });

  assert.deepEqual(result, {
    query: "Red Hot Chili Peppers - The Zephyr Song",
    audioId: "manualVideo123",
    normalizedSong: {
      artist: "Red Hot Chili Peppers",
      title: "The Zephyr Song",
    },
  });
});

test("resolveQueryPreflightResult can suppress resolved audio ids when backend disallows overrides", () => {
  const result = resolveQueryPreflightResult({
    rawQuery: "red hot chili peppers the zephyr song lyrics",
    allowResolvedAudioId: false,
    normalizedPayload: {
      artist: "Red Hot Chili Peppers",
      track: "The Zephyr Song",
      normalized_query: "Red Hot Chili Peppers - The Zephyr Song",
      video_id: "0fcRa5Z6LmU",
    },
  });

  assert.deepEqual(result, {
    query: "Red Hot Chili Peppers - The Zephyr Song",
    audioId: null,
    normalizedSong: {
      artist: "Red Hot Chili Peppers",
      title: "The Zephyr Song",
    },
  });
});

test("resolveQueryPreflightResult falls back to raw query when normalization is incomplete", () => {
  const result = resolveQueryPreflightResult({
    rawQuery: "mystery song words",
    normalizedPayload: {
      display: "Mystery Song",
    },
  });

  assert.deepEqual(result, {
    query: "mystery song words",
    audioId: null,
    normalizedSong: null,
  });
});
