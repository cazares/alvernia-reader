export function resolveExpectedPendingVideoId(input?: {
  pendingSessionJobId?: string | null;
  resolvedVideoIdByJobId?: Record<string, string | null | undefined> | null;
}): string | null;

export function hasPendingVideoMismatch(input?: {
  pendingSessionJobId?: string | null;
  expectedPendingVideoId?: string | null;
  activeYoutubeVideoId?: string | null;
}): boolean;

export function shouldHoldPresetLoadingForEmbed(input?: {
  pendingPresetVideoAutoOpen?: boolean;
  videoTabEnabled?: boolean;
  youtubeEmbedReady?: boolean;
  hasPendingVideoMismatch?: boolean;
}): boolean;

export function shouldKeepWaitingForEmbedAfterSuccess(input?: {
  pendingPresetVideoAutoOpen?: boolean;
  jobSucceeded?: boolean;
  videoTabEnabled?: boolean;
  youtubeEmbedReady?: boolean;
  pendingSessionJobId?: string | null;
  expectedPendingVideoId?: string | null;
  activeYoutubeVideoId?: string | null;
}): boolean;

export function shouldRefreshEmbedWhileWaiting(input?: {
  videoTabEnabled?: boolean;
  pendingSessionJobId?: string | null;
  expectedPendingVideoId?: string | null;
  activeYoutubeVideoId?: string | null;
}): boolean;

export function canAutoOpenVideoTab(input?: {
  videoTabEnabled?: boolean;
  pendingSessionJobId?: string | null;
  expectedPendingVideoId?: string | null;
  activeYoutubeVideoId?: string | null;
}): boolean;

export function canAcknowledgeEmbedReady(input?: {
  activeVideoId?: string | null;
  pendingSessionJobId?: string | null;
  expectedPendingVideoId?: string | null;
}): { accept: boolean; reason: string };

export function canSelectVideoTab(input?: {
  videoTabEnabled?: boolean;
  presetRequestInFlight?: boolean;
}): boolean;
