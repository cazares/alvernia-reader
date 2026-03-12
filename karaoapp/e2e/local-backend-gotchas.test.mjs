import test from "node:test";
import assert from "node:assert/strict";

import {
  detectBackendMode,
  shouldPreferDirectPlaybackForBaseUrl,
  shouldRequestYoutubeUploadForBaseUrl,
} from "../src/backendMode.js";
import { isPlaybackSourceReady, resolvePlaybackSource } from "../src/videoPlaybackPolicy.js";

test("loopback api uses local cli mode and direct playback", () => {
  assert.equal(detectBackendMode("http://127.0.0.1:8000"), "local_cli");
  assert.equal(detectBackendMode("http://localhost:8000"), "local_cli");
  assert.equal(detectBackendMode("http://192.168.1.25:8000"), "local_cli");
  assert.equal(detectBackendMode("http://10.0.0.12:8000"), "local_cli");
  assert.equal(shouldPreferDirectPlaybackForBaseUrl("http://127.0.0.1:8000"), true);
  assert.equal(shouldRequestYoutubeUploadForBaseUrl("http://127.0.0.1:8000"), false);
});

test("remote api keeps youtube upload mode", () => {
  assert.equal(detectBackendMode("https://mixterioso.example"), "remote");
  assert.equal(shouldPreferDirectPlaybackForBaseUrl("https://mixterioso.example"), false);
  assert.equal(shouldRequestYoutubeUploadForBaseUrl("https://mixterioso.example"), true);
});

test("playback source falls back to direct output when youtube url is absent", () => {
  const source = resolvePlaybackSource({
    youtubeEmbedUrl: null,
    finalOutputUrl: "http://127.0.0.1:8000/output/final.mp4",
    previewOutputUrl: null,
    legacyOutputUrl: null,
  });
  assert.deepEqual(source, {
    kind: "direct",
    url: "http://127.0.0.1:8000/output/final.mp4",
  });
  assert.equal(isPlaybackSourceReady(source, false), true);
});

test("local direct playback accepts nested output urls from output temp", () => {
  const source = resolvePlaybackSource({
    youtubeEmbedUrl: null,
    finalOutputUrl: "http://127.0.0.1:8000/output/temp/final.mp4",
    previewOutputUrl: null,
    legacyOutputUrl: null,
  });
  assert.deepEqual(source, {
    kind: "direct",
    url: "http://127.0.0.1:8000/output/temp/final.mp4",
  });
  assert.equal(isPlaybackSourceReady(source, false), true);
});

test("youtube playback still waits for embed readiness", () => {
  const source = resolvePlaybackSource({
    youtubeEmbedUrl: "https://www.youtube.com/watch?v=abc123",
    finalOutputUrl: "http://127.0.0.1:8000/output/final.mp4",
    previewOutputUrl: null,
    legacyOutputUrl: null,
  });
  assert.equal(source.kind, "youtube");
  assert.equal(isPlaybackSourceReady(source, false), false);
  assert.equal(isPlaybackSourceReady(source, true), true);
});
