export const resolvePlaybackSource = ({
  youtubeEmbedUrl = null,
  finalOutputUrl = null,
  previewOutputUrl = null,
  legacyOutputUrl = null,
}) => {
  const youtube = String(youtubeEmbedUrl || "").trim();
  if (youtube) {
    return { kind: "youtube", url: youtube };
  }

  const direct =
    String(finalOutputUrl || "").trim() ||
    String(previewOutputUrl || "").trim() ||
    String(legacyOutputUrl || "").trim();
  if (direct) {
    return { kind: "direct", url: direct };
  }

  return { kind: "none", url: null };
};

export const isPlaybackSourceReady = (source, youtubeEmbedReady) => {
  if (!source || source.kind === "none") return false;
  if (source.kind === "direct") return Boolean(source.url);
  return Boolean(source.url) && Boolean(youtubeEmbedReady);
};
