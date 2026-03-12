export const isMutedPreviewOutputUrl = (url = null) =>
  /\.preview\.muted\.mp4(?:$|[?#])/.test(String(url || "").trim());

export const shouldUsePreviewCompanionAudio = ({
  outputUrl = null,
  isPreview = false,
  companionAudioUrl = null,
}) => Boolean(isPreview && isMutedPreviewOutputUrl(outputUrl) && String(companionAudioUrl || "").trim());
