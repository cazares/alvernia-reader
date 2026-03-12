import test from "node:test";
import assert from "node:assert/strict";

import {
  clampPreviewToFinalSeek,
  DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
  shouldArmPreviewToFinalSeek,
  shouldCarryPauseStateAcrossPreviewToFinal,
} from "../src/videoHandoff.js";

test("clampPreviewToFinalSeek caps seek near end of final video", () => {
  const clamped = clampPreviewToFinalSeek({
    pendingSeek: 14.99,
    durationHint: 15,
    endBufferSec: DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
  });
  assert.ok(Math.abs(Number(clamped) - 14.85) < 1e-9);
});

test("clampPreviewToFinalSeek passes through seek when no duration hint is known", () => {
  const clamped = clampPreviewToFinalSeek({ pendingSeek: 37.5, durationHint: undefined });
  assert.equal(clamped, 37.5);
});

test("clampPreviewToFinalSeek returns null for invalid or negative seek inputs", () => {
  assert.equal(clampPreviewToFinalSeek({ pendingSeek: undefined, durationHint: 90 }), null);
  assert.equal(clampPreviewToFinalSeek({ pendingSeek: -1, durationHint: 90 }), null);
});

test("shouldArmPreviewToFinalSeek arms only for preview-to-final transitions with a positive seek", () => {
  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "https://cdn.example/preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "https://cdn.example/final.mp4",
      nextIsPreview: false,
      handoffSeek: 42.1,
    }),
    true
  );

  assert.equal(
    shouldArmPreviewToFinalSeek({
      previousOutputUrl: "https://cdn.example/preview.mp4",
      previousIsPreview: true,
      nextOutputUrl: "https://cdn.example/final.mp4",
      nextIsPreview: false,
      handoffSeek: 0,
    }),
    false
  );
});

test("shouldCarryPauseStateAcrossPreviewToFinal carries pause only on preview-to-final source swaps", () => {
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
});
