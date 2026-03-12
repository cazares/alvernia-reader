import test from "node:test";
import assert from "node:assert/strict";

import {
  canAcknowledgeEmbedReady,
  canAutoOpenVideoTab,
  canSelectVideoTab,
  hasPendingVideoMismatch,
  resolveExpectedPendingVideoId,
  shouldHoldPresetLoadingForEmbed,
  shouldKeepWaitingForEmbedAfterSuccess,
  shouldRefreshEmbedWhileWaiting,
} from "../src/presetSessionGates.js";

test("resolveExpectedPendingVideoId resolves mapped ID for pending session job", () => {
  const resolved = resolveExpectedPendingVideoId({
    pendingSessionJobId: "job-123",
    resolvedVideoIdByJobId: { "job-123": "abc123def45" },
  });
  assert.equal(resolved, "abc123def45");
});

test("resolveExpectedPendingVideoId trims job id and video id values", () => {
  const resolved = resolveExpectedPendingVideoId({
    pendingSessionJobId: "  job-abc  ",
    resolvedVideoIdByJobId: { "job-abc": "  IKeeYvrexlU  " },
  });
  assert.equal(resolved, "IKeeYvrexlU");
});

test("resolveExpectedPendingVideoId returns null when mapping is missing or invalid", () => {
  assert.equal(
    resolveExpectedPendingVideoId({
      pendingSessionJobId: "job-1",
      resolvedVideoIdByJobId: null,
    }),
    null
  );
  assert.equal(
    resolveExpectedPendingVideoId({
      pendingSessionJobId: "",
      resolvedVideoIdByJobId: { "job-1": "abc123def45" },
    }),
    null
  );
  assert.equal(
    resolveExpectedPendingVideoId({
      pendingSessionJobId: "job-1",
      resolvedVideoIdByJobId: { "job-2": "abc123def45" },
    }),
    null
  );
});

test("hasPendingVideoMismatch is false when no pending session job exists", () => {
  assert.equal(
    hasPendingVideoMismatch({
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    false
  );
});

test("hasPendingVideoMismatch flags unresolved expected video for pending session", () => {
  assert.equal(
    hasPendingVideoMismatch({
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "abc123def45",
    }),
    true
  );
});

test("hasPendingVideoMismatch flags unresolved active video for pending session", () => {
  assert.equal(
    hasPendingVideoMismatch({
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "",
    }),
    true
  );
});

test("hasPendingVideoMismatch detects mismatched video IDs", () => {
  assert.equal(
    hasPendingVideoMismatch({
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    true
  );
});

test("hasPendingVideoMismatch is false when pending and active IDs match", () => {
  assert.equal(
    hasPendingVideoMismatch({
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    false
  );
});

test("shouldHoldPresetLoadingForEmbed only holds while auto-open flow is active", () => {
  assert.equal(
    shouldHoldPresetLoadingForEmbed({
      pendingPresetVideoAutoOpen: false,
      videoTabEnabled: false,
      youtubeEmbedReady: false,
      hasPendingVideoMismatch: true,
    }),
    false
  );
});

test("shouldHoldPresetLoadingForEmbed holds when video tab is not yet enabled", () => {
  assert.equal(
    shouldHoldPresetLoadingForEmbed({
      pendingPresetVideoAutoOpen: true,
      videoTabEnabled: false,
      youtubeEmbedReady: true,
      hasPendingVideoMismatch: false,
    }),
    true
  );
});

test("shouldHoldPresetLoadingForEmbed holds when embed has not reported ready", () => {
  assert.equal(
    shouldHoldPresetLoadingForEmbed({
      pendingPresetVideoAutoOpen: true,
      videoTabEnabled: true,
      youtubeEmbedReady: false,
      hasPendingVideoMismatch: false,
    }),
    true
  );
});

test("shouldHoldPresetLoadingForEmbed holds when active video mismatches pending expectation", () => {
  assert.equal(
    shouldHoldPresetLoadingForEmbed({
      pendingPresetVideoAutoOpen: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      hasPendingVideoMismatch: true,
    }),
    true
  );
});

test("shouldHoldPresetLoadingForEmbed clears when tab is enabled, embed is ready, and no mismatch", () => {
  assert.equal(
    shouldHoldPresetLoadingForEmbed({
      pendingPresetVideoAutoOpen: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      hasPendingVideoMismatch: false,
    }),
    false
  );
});

test("shouldKeepWaitingForEmbedAfterSuccess returns false unless auto-open + success are active", () => {
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: false,
      jobSucceeded: true,
      videoTabEnabled: false,
      youtubeEmbedReady: false,
    }),
    false
  );
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: false,
      videoTabEnabled: false,
      youtubeEmbedReady: false,
    }),
    false
  );
});

test("shouldKeepWaitingForEmbedAfterSuccess waits while tab disabled or embed not ready", () => {
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: false,
      youtubeEmbedReady: true,
      pendingSessionJobId: "",
    }),
    true
  );
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: false,
      pendingSessionJobId: "",
    }),
    true
  );
});

test("shouldKeepWaitingForEmbedAfterSuccess does not require ID match when no pending job is tracked", () => {
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    false
  );
});

test("shouldKeepWaitingForEmbedAfterSuccess waits when pending job ID exists but expected/active IDs are unresolved", () => {
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "abc123def45",
    }),
    true
  );
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "",
    }),
    true
  );
});

test("shouldKeepWaitingForEmbedAfterSuccess waits on mismatched IDs and clears on match", () => {
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    true
  );
  assert.equal(
    shouldKeepWaitingForEmbedAfterSuccess({
      pendingPresetVideoAutoOpen: true,
      jobSucceeded: true,
      videoTabEnabled: true,
      youtubeEmbedReady: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    false
  );
});

test("shouldRefreshEmbedWhileWaiting refreshes if video tab is disabled", () => {
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: false,
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    true
  );
});

test("shouldRefreshEmbedWhileWaiting skips refresh when tab is enabled and no pending job exists", () => {
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: true,
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    false
  );
});

test("shouldRefreshEmbedWhileWaiting refreshes unresolved or mismatched pending sessions", () => {
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "abc123def45",
    }),
    true
  );
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "",
    }),
    true
  );
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    true
  );
  assert.equal(
    shouldRefreshEmbedWhileWaiting({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    false
  );
});

test("canAutoOpenVideoTab is true only when enabled and matching expectations", () => {
  assert.equal(
    canAutoOpenVideoTab({
      videoTabEnabled: false,
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    false
  );
  assert.equal(
    canAutoOpenVideoTab({
      videoTabEnabled: true,
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "",
    }),
    true
  );
  assert.equal(
    canAutoOpenVideoTab({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "",
      activeYoutubeVideoId: "abc123def45",
    }),
    false
  );
  assert.equal(
    canAutoOpenVideoTab({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    false
  );
  assert.equal(
    canAutoOpenVideoTab({
      videoTabEnabled: true,
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
      activeYoutubeVideoId: "IKeeYvrexlU",
    }),
    true
  );
});

test("canAcknowledgeEmbedReady rejects stale and unresolved ready callbacks", () => {
  assert.deepEqual(
    canAcknowledgeEmbedReady({
      activeVideoId: "",
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "abc123def45",
    }),
    { accept: false, reason: "missing_active_video_id" }
  );
  assert.deepEqual(
    canAcknowledgeEmbedReady({
      activeVideoId: "abc123def45",
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "",
    }),
    { accept: false, reason: "expected_video_unresolved" }
  );
  assert.deepEqual(
    canAcknowledgeEmbedReady({
      activeVideoId: "abc123def45",
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
    }),
    { accept: false, reason: "stale_video_ready" }
  );
});

test("canAcknowledgeEmbedReady accepts ready events for matching or unbound sessions", () => {
  assert.deepEqual(
    canAcknowledgeEmbedReady({
      activeVideoId: "abc123def45",
      pendingSessionJobId: "",
      expectedPendingVideoId: "",
    }),
    { accept: true, reason: "ready" }
  );
  assert.deepEqual(
    canAcknowledgeEmbedReady({
      activeVideoId: "IKeeYvrexlU",
      pendingSessionJobId: "job-1",
      expectedPendingVideoId: "IKeeYvrexlU",
    }),
    { accept: true, reason: "ready" }
  );
});

test("canSelectVideoTab requires tab enabled and no request in flight", () => {
  assert.equal(canSelectVideoTab({ videoTabEnabled: true, presetRequestInFlight: false }), true);
  assert.equal(canSelectVideoTab({ videoTabEnabled: false, presetRequestInFlight: false }), false);
  assert.equal(canSelectVideoTab({ videoTabEnabled: true, presetRequestInFlight: true }), false);
});
