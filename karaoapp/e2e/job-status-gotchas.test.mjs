import test from "node:test";
import assert from "node:assert/strict";

import {
  isJobCancelled,
  isJobFailed,
  isJobInProgress,
  isJobSucceeded,
  isJobTerminal,
  normalizeJobStatus,
  shouldKeepProcessingModalOpen,
} from "../src/jobStatus.js";

test("normalizeJobStatus is case-insensitive and trims whitespace", () => {
  assert.equal(normalizeJobStatus("  COMPLETED  "), "succeeded");
  assert.equal(normalizeJobStatus("  FAILED "), "failed");
  assert.equal(normalizeJobStatus("  CANCELED "), "cancelled");
  assert.equal(normalizeJobStatus("  In_Progress "), "running");
});

test("normalizeJobStatus maps running-family states consistently", () => {
  assert.equal(normalizeJobStatus("queued"), "queued");
  assert.equal(normalizeJobStatus("running"), "running");
  assert.equal(normalizeJobStatus("processing"), "running");
  assert.equal(normalizeJobStatus("starting"), "running");
  assert.equal(normalizeJobStatus("partial_ready"), "running");
});

test("normalizeJobStatus returns unknown for empty and unsupported values", () => {
  assert.equal(normalizeJobStatus(""), "unknown");
  assert.equal(normalizeJobStatus("mystery_state"), "unknown");
  assert.equal(normalizeJobStatus(undefined), "unknown");
});

test("in-progress and terminal predicates stay mutually consistent", () => {
  const inProgress = ["queued", "running", "processing", "in_progress"];
  for (const status of inProgress) {
    assert.equal(isJobInProgress(status), true, status);
    assert.equal(isJobTerminal(status), false, status);
  }

  const terminal = ["succeeded", "completed", "complete", "failed", "error", "cancelled", "canceled"];
  for (const status of terminal) {
    assert.equal(isJobInProgress(status), false, status);
    assert.equal(isJobTerminal(status), true, status);
  }
});

test("result predicates detect only their own normalized family", () => {
  assert.equal(isJobSucceeded("completed"), true);
  assert.equal(isJobSucceeded("failed"), false);

  assert.equal(isJobFailed("error"), true);
  assert.equal(isJobFailed("cancelled"), false);

  assert.equal(isJobCancelled("canceled"), true);
  assert.equal(isJobCancelled("running"), false);
});

test("shouldKeepProcessingModalOpen closes immediately once output exists", () => {
  const statuses = [
    "queued",
    "running",
    "processing",
    "succeeded",
    "failed",
    "cancelled",
    "unknown",
    "",
  ];
  for (const status of statuses) {
    assert.equal(shouldKeepProcessingModalOpen(status, true), false, status);
  }
});

test("shouldKeepProcessingModalOpen keeps modal only for in-progress states when no output exists", () => {
  assert.equal(shouldKeepProcessingModalOpen("queued", false), true);
  assert.equal(shouldKeepProcessingModalOpen("running", false), true);
  assert.equal(shouldKeepProcessingModalOpen("processing", false), true);
  assert.equal(shouldKeepProcessingModalOpen("complete", false), false);
  assert.equal(shouldKeepProcessingModalOpen("failed", false), false);
  assert.equal(shouldKeepProcessingModalOpen("unknown", false), false);
});

test("status timeline transitions from queued to terminal without predicate contradictions", () => {
  const snapshots = ["queued", "running", "partial_ready", "complete"];
  let seenTerminal = false;
  for (const status of snapshots) {
    const inProgress = isJobInProgress(status);
    const terminal = isJobTerminal(status);
    if (seenTerminal) {
      assert.equal(inProgress, false, `status ${status} should not re-enter progress`);
    }
    if (terminal) {
      seenTerminal = true;
    }
    assert.equal(!(inProgress && terminal), true, `status ${status} cannot be both`);
  }
  assert.equal(seenTerminal, true);
});
