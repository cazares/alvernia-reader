import test from "node:test";
import assert from "node:assert/strict";

import { buildApplyVideoTuningRequest } from "../src/videoRetuneRequest.js";

test("buildApplyVideoTuningRequest enables fast render-only rebuild for the current video", () => {
  const mixLevels = { vocals: 80, bass: 100, drums: 95, other: 110 };
  const request = buildApplyVideoTuningRequest({
    query: "Red Hot Chili Peppers - Californication",
    mixLevels,
  });

  assert.deepEqual(request, {
    renderOnly: true,
    presetAutoOpen: true,
    queryOverride: "Red Hot Chili Peppers - Californication",
    mixLevelsOverride: mixLevels,
  });
});

test("buildApplyVideoTuningRequest returns null when there is no usable query", () => {
  assert.equal(buildApplyVideoTuningRequest({ query: "   ", mixLevels: { vocals: 100 } }), null);
});
