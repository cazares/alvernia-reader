import test from "node:test";
import assert from "node:assert/strict";

import { isMutedPreviewOutputUrl, shouldUsePreviewCompanionAudio } from "../src/previewAudioPolicy.js";

test("isMutedPreviewOutputUrl detects muted preview artifacts", () => {
  assert.equal(isMutedPreviewOutputUrl("http://127.0.0.1:8000/output/song.preview.muted.mp4"), true);
  assert.equal(isMutedPreviewOutputUrl("/output/song.preview.muted.mp4?cache=1"), true);
  assert.equal(isMutedPreviewOutputUrl("/output/song.preview.mp4"), false);
  assert.equal(isMutedPreviewOutputUrl("/output/song.mp4"), false);
});

test("shouldUsePreviewCompanionAudio only enables companion audio for muted preview playback", () => {
  assert.equal(
    shouldUsePreviewCompanionAudio({
      outputUrl: "http://127.0.0.1:8000/output/song.preview.muted.mp4",
      isPreview: true,
      companionAudioUrl: "http://127.0.0.1:8000/files/mixes/song.m4a",
    }),
    true
  );
  assert.equal(
    shouldUsePreviewCompanionAudio({
      outputUrl: "http://127.0.0.1:8000/output/song.preview.mp4",
      isPreview: true,
      companionAudioUrl: "http://127.0.0.1:8000/files/mixes/song.m4a",
    }),
    false
  );
  assert.equal(
    shouldUsePreviewCompanionAudio({
      outputUrl: "http://127.0.0.1:8000/output/song.preview.muted.mp4",
      isPreview: false,
      companionAudioUrl: "http://127.0.0.1:8000/files/mixes/song.m4a",
    }),
    false
  );
});
