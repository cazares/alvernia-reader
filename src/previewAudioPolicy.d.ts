export function isMutedPreviewOutputUrl(url?: string | null): boolean;

export function shouldUsePreviewCompanionAudio(args?: {
  outputUrl?: string | null;
  isPreview?: boolean;
  companionAudioUrl?: string | null;
}): boolean;
