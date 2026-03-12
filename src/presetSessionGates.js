const clean = (value) => String(value || "").trim();

export const resolveExpectedPendingVideoId = ({
  pendingSessionJobId,
  resolvedVideoIdByJobId,
} = {}) => {
  const jobId = clean(pendingSessionJobId);
  if (!jobId || !resolvedVideoIdByJobId || typeof resolvedVideoIdByJobId !== "object") {
    return null;
  }
  const videoId = clean(resolvedVideoIdByJobId[jobId]);
  return videoId || null;
};

export const hasPendingVideoMismatch = ({
  pendingSessionJobId,
  expectedPendingVideoId,
  activeYoutubeVideoId,
} = {}) => {
  const jobId = clean(pendingSessionJobId);
  if (!jobId) return false;
  const expected = clean(expectedPendingVideoId);
  const active = clean(activeYoutubeVideoId);
  if (!expected || !active) return true;
  return expected !== active;
};

export const shouldHoldPresetLoadingForEmbed = ({
  pendingPresetVideoAutoOpen,
  videoTabEnabled,
  youtubeEmbedReady,
  hasPendingVideoMismatch: pendingVideoMismatch,
} = {}) =>
  Boolean(
    pendingPresetVideoAutoOpen &&
      (!videoTabEnabled || !youtubeEmbedReady || Boolean(pendingVideoMismatch))
  );

export const shouldKeepWaitingForEmbedAfterSuccess = ({
  pendingPresetVideoAutoOpen,
  jobSucceeded,
  videoTabEnabled,
  youtubeEmbedReady,
  pendingSessionJobId,
  expectedPendingVideoId,
  activeYoutubeVideoId,
} = {}) => {
  if (!pendingPresetVideoAutoOpen || !jobSucceeded) return false;
  if (!videoTabEnabled || !youtubeEmbedReady) return true;
  const hasPendingSessionJob = Boolean(clean(pendingSessionJobId));
  if (!hasPendingSessionJob) return false;
  const expected = clean(expectedPendingVideoId);
  const active = clean(activeYoutubeVideoId);
  if (!expected || !active) return true;
  return expected !== active;
};

export const shouldRefreshEmbedWhileWaiting = ({
  videoTabEnabled,
  pendingSessionJobId,
  expectedPendingVideoId,
  activeYoutubeVideoId,
} = {}) => {
  if (!videoTabEnabled) return true;
  const hasPendingSessionJob = Boolean(clean(pendingSessionJobId));
  if (!hasPendingSessionJob) return false;
  const expected = clean(expectedPendingVideoId);
  const active = clean(activeYoutubeVideoId);
  if (!expected || !active) return true;
  return expected !== active;
};

export const canAutoOpenVideoTab = ({
  videoTabEnabled,
  pendingSessionJobId,
  expectedPendingVideoId,
  activeYoutubeVideoId,
} = {}) => {
  if (!videoTabEnabled) return false;
  const hasPendingSessionJob = Boolean(clean(pendingSessionJobId));
  if (!hasPendingSessionJob) return true;
  const expected = clean(expectedPendingVideoId);
  const active = clean(activeYoutubeVideoId);
  if (!expected || !active) return false;
  return expected === active;
};

export const canAcknowledgeEmbedReady = ({
  activeVideoId,
  pendingSessionJobId,
  expectedPendingVideoId,
} = {}) => {
  const active = clean(activeVideoId);
  if (!active) {
    return { accept: false, reason: "missing_active_video_id" };
  }
  const hasPendingSessionJob = Boolean(clean(pendingSessionJobId));
  const expected = clean(expectedPendingVideoId);
  if (hasPendingSessionJob && !expected) {
    return { accept: false, reason: "expected_video_unresolved" };
  }
  if (expected && active !== expected) {
    return { accept: false, reason: "stale_video_ready" };
  }
  return { accept: true, reason: "ready" };
};

export const canSelectVideoTab = ({
  videoTabEnabled,
  presetRequestInFlight,
} = {}) => Boolean(videoTabEnabled && !presetRequestInFlight);
