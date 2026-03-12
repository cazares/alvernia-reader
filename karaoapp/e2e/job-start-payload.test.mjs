import test from "node:test";
import assert from "node:assert/strict";

import { buildStartJobRequestPayload, hasCustomStemMix } from "../src/jobStartPayload.js";

test("buildStartJobRequestPayload preserves fine offset even without a custom stem mix", () => {
  const payload = buildStartJobRequestPayload({
    createJobPayload: { query: "Artist - Song", idempotency_key: "abc123" },
    offsetSec: 1.25,
    mixLevels: { vocals: 100, bass: 100, drums: 100, other: 100 },
  });

  assert.equal(payload.query, "Artist - Song");
  assert.equal(payload.offset_sec, 1.25);
  assert.equal(payload.render_only, false);
  assert.equal("vocals" in payload, false);
  assert.equal("bass" in payload, false);
  assert.equal("drums" in payload, false);
  assert.equal("other" in payload, false);
});

test("buildStartJobRequestPayload includes custom stem levels and trims direct source ids", () => {
  const payload = buildStartJobRequestPayload({
    createJobPayload: { query: "Artist - Song" },
    mixLevels: { vocals: 10, bass: 100, drums: 95, other: 105 },
    audioId: "  abc123xyz  ",
  });

  assert.equal(payload.offset_sec, 0);
  assert.equal(payload.audio_id, "abc123xyz");
  assert.equal(payload.vocals, 10);
  assert.equal(payload.bass, 100);
  assert.equal(payload.drums, 95);
  assert.equal(payload.other, 105);
});

test("buildStartJobRequestPayload clamps offset and still includes levels for render-only requests", () => {
  const payload = buildStartJobRequestPayload({
    createJobPayload: { query: "Artist - Song" },
    renderOnly: true,
    offsetSec: 99,
    mixLevels: { vocals: 100, bass: 100, drums: 100, other: 100 },
    audioUrl: "  https://example.com/audio.m4a  ",
  });

  assert.equal(payload.render_only, true);
  assert.equal(payload.offset_sec, 15);
  assert.equal(payload.audio_url, "https://example.com/audio.m4a");
  assert.equal(payload.vocals, 100);
  assert.equal(payload.bass, 100);
  assert.equal(payload.drums, 100);
  assert.equal(payload.other, 100);
});

test("buildStartJobRequestPayload includes preview when requested for first-playable mobile flow", () => {
  const payload = buildStartJobRequestPayload({
    createJobPayload: { query: "Artist - Song" },
    preview: true,
  });

  assert.equal(payload.preview, true);
  assert.equal(payload.render_only, false);
});

test("hasCustomStemMix only trips when a level actually changes", () => {
  assert.equal(hasCustomStemMix({ vocals: 100, bass: 100, drums: 100, other: 100 }), false);
  assert.equal(hasCustomStemMix({ vocals: 80, bass: 100, drums: 100, other: 100 }), true);
});
