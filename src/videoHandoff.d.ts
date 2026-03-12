export const DEFAULT_PREVIEW_TO_FINAL_END_BUFFER_SEC: number;

export type ClampPreviewToFinalSeekInput = {
  pendingSeek?: unknown;
  durationHint?: unknown;
  endBufferSec?: unknown;
};

export function clampPreviewToFinalSeek(input?: ClampPreviewToFinalSeekInput): number | null;

export type PreviewToFinalSeekArmingInput = {
  previousOutputUrl?: string | null;
  previousIsPreview?: boolean;
  nextOutputUrl?: string | null;
  nextIsPreview?: boolean;
  handoffSeek?: unknown;
};

export function shouldArmPreviewToFinalSeek(input?: PreviewToFinalSeekArmingInput): boolean;

export type CarryPauseAcrossPreviewToFinalInput = {
  sourceChanged?: boolean;
  previousIsPreview?: boolean;
  nextIsPreview?: boolean;
  wasPaused?: boolean;
};

export function shouldCarryPauseStateAcrossPreviewToFinal(
  input?: CarryPauseAcrossPreviewToFinalInput
): boolean;
