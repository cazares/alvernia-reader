export const DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC = 0.15;

const toFiniteNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
};

export const clampPreviewToFinalSeek = ({
  pendingSeek,
  durationHint,
  endBufferSec = DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC,
} = {}) => {
  const seek = toFiniteNumber(pendingSeek);
  if (seek === null || seek < 0) return null;

  const duration = toFiniteNumber(durationHint);
  if (duration === null || duration <= 0) {
    return Math.max(0, seek);
  }

  const configuredEndBuffer = toFiniteNumber(endBufferSec);
  const endBuffer = Math.max(
    0,
    configuredEndBuffer === null ? DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC : configuredEndBuffer
  );
  const maxAllowedSeek = Math.max(0, duration - endBuffer);
  return Math.max(0, Math.min(seek, maxAllowedSeek));
};

export const shouldArmPreviewToFinalSeek = ({
  previousOutputUrl,
  previousIsPreview,
  nextOutputUrl,
  nextIsPreview,
  handoffSeek,
} = {}) => {
  const seek = toFiniteNumber(handoffSeek);
  if (!previousOutputUrl || !nextOutputUrl) return false;
  if (!previousIsPreview || nextIsPreview) return false;
  if (seek === null || seek <= 0) return false;
  return true;
};

export const shouldResetPlaybackOnSourceChange = ({
  sourceChanged,
  previousOutputUrl,
  previousIsPreview,
  nextOutputUrl,
  nextIsPreview,
  handoffSeek,
} = {}) => {
  if (!sourceChanged) return false;
  if (!nextOutputUrl) return false;
  return !shouldArmPreviewToFinalSeek({
    previousOutputUrl,
    previousIsPreview,
    nextOutputUrl,
    nextIsPreview,
    handoffSeek,
  });
};

export const shouldCarryPauseStateAcrossPreviewToFinal = ({
  sourceChanged,
  previousIsPreview,
  nextIsPreview,
  wasPaused,
} = {}) => Boolean(sourceChanged && previousIsPreview && !nextIsPreview && wasPaused);
