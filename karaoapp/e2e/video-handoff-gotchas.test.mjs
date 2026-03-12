import test from "node:test";
import assert from "node:assert/strict";

import {
  clampPreviewToFinalSeek,
  DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
  shouldArmPreviewToFinalSeek,
  shouldCarryPauseStateAcrossPreviewToFinal,
  shouldResetPlaybackOnSourceChange,
} from "../src/videoHandoff.js";

test("clampPreviewToFinalSeek clamps to zero for negative seek and keeps finite positives", () => {
  assert.equal(clampPreviewToFinalSeek({ pendingSeek: -0.1, durationHint: 10 }), null);
  assert.equal(clampPreviewToFinalSeek({ pendingSeek: 0, durationHint: 10 }), 0);
  assert.equal(clampPreviewToFinalSeek({ pendingSeek: 2.5, durationHint: 10 }), 2.5);
});

test("clampPreviewToFinalSeek honors end buffer even when custom buffer is negative", () => {
  const withNegativeBuffer = clampPreviewToFinalSeek({
    pendingSeek: 99,
    durationHint: 10,
    endBufferSec: -4,
  });
  assert.equal(withNegativeBuffer, 10);
});

test("clampPreviewToFinalSeek defaults to configured end buffer when duration is known", () => {
  const clamped = clampPreviewToFinalSeek({
    pendingSeek: 20,
    durationHint: 20,
  });
  assert.equal(clamped, 20 - DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC);
});

test("shouldArmPreviewToFinalSeek requires both URLs, preview->final transition, and positive seek", () => {
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 5,
    }),
    true
  );
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 5,
    }),
    false
  );
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "preview.mp4",
      previousIsPreview: false,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 5,
    }),
    false
  );
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: true,
      handoffSeek: 5,
    }),
    false
  );
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 0,
    }),
    false
  );
});

test("shouldCarryPauseStateAcrossPreviewToFinal only carries pause for preview->final source swaps", () => {
  assert.equal(
    shouldCarryPauseStateAcrossPreviewToFinal({
      sourceChanged: true,
      previousIsPreview: true,
      nextIsPreview: false,
      wasPaused: true,
    }),
    true
  );
  assert.equal(
    shouldCarryPauseStateAcrossPreviewToFinal({
      sourceChanged: true,
      previousIsPreview: true,
      nextIsPreview: false,
      wasPaused: false,
    }),
    false
  );
  assert.equal(
    shouldCarryPauseStateAcrossPreviewToFinal({
      sourceChanged: false,
      previousIsPreview: true,
      nextIsPreview: false,
      wasPaused: true,
    }),
    false
  );
  assert.equal(
    shouldCarryPauseStateAcrossPreviewToFinal({
      sourceChanged: true,
      previousIsPreview: false,
      nextIsPreview: false,
      wasPaused: true,
    }),
    false
  );
});

test("shouldResetPlaybackOnSourceChange does not reset when there is no new source or no source change", () => {
  assert.equal(
    shouldResetPlaybackOnSourceChange({
      sourceChanged: false,
      previousOutputUrl: "preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 5,
    }),
    false
  );

  assert.equal(
    shouldResetPlaybackOnSourceChange({
      sourceChanged: true,
      previousOutputUrl: "final.mp4",
      previousIsPreview: false,
      nextOutputUrl: "",
      nextIsPreview: false,
      handoffSeek: 5,
    }),
    false
  );

  assert.equal(
    shouldResetPlaybackOnSourceChange({
      sourceChanged: true,
      previousOutputUrl: "preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "final.mp4",
      nextIsPreview: false,
      handoffSeek: 0,
    }),
    true
  );
});
