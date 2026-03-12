import test from "node:test";
import assert from "node:assert/strict";

import {
  isJobInProgress,
  isJobSucceeded,
  isJobTerminal,
  normalizeJobStatus,
  shouldKeepProcessingModalOpen,
} from "../src/jobStatus.js";

test("normalizes completed and complete statuses as success", () => {
  assert.equal(normalizeJobStatus("completed"), "succeeded");
  assert.equal(normalizeJobStatus("complete"), "succeeded");
  assert.equal(isJobSucceeded("completed"), true);
  assert.equal(isJobTerminal("completed"), true);
});

test("processing modal closes once output is available even if backend still reports running", () => {
  assert.equal(shouldKeepProcessingModalOpen("running", false), true);
  assert.equal(shouldKeepProcessingModalOpen("running", true), false);
});

test("end-to-end status flow reaches terminal state and exits loading", () => {
  const snapshots = [
    { status: "queued", hasOutputUrl: false },
    { status: "running", hasOutputUrl: false },
    { status: "completed", hasOutputUrl: true },
  ];

  let processingVisible = true;
  for (const snapshot of snapshots) {
    if (!shouldKeepProcessingModalOpen(snapshot.status, snapshot.hasOutputUrl)) {
      processingVisible = false;
    }
  }

  const final = snapshots[snapshots.length - 1];
  assert.equal(isJobInProgress(final.status), false);
  assert.equal(isJobTerminal(final.status), true);
  assert.equal(processingVisible, false);
});
