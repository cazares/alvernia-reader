const cleanText = (value) =>
  String(value || "")
    .trim()
    .replace(/^["'`“”]+/, "")
    .replace(/["'`“”]+$/, "")
    .trim();

const buildNormalizedSong = (payload = {}) => {
  const artist = cleanText(payload?.artist);
  const title = cleanText(payload?.track || payload?.title);
  if (!artist || !title) return null;
  return { artist, title };
};

export const resolveQueryPreflightResult = ({
  rawQuery,
  normalizedPayload,
  explicitAudioId,
  explicitAudioUrl,
  allowResolvedAudioId = true,
} = {}) => {
  const query = cleanText(rawQuery);
  const normalizedSong = buildNormalizedSong(normalizedPayload);
  const normalizedQuery = cleanText(
    normalizedPayload?.normalized_query || normalizedPayload?.display
  );
  const normalizedVideoId = cleanText(normalizedPayload?.video_id);
  const requestedAudioId = cleanText(explicitAudioId);
  const requestedAudioUrl = cleanText(explicitAudioUrl);
  const hasExplicitSource = Boolean(requestedAudioId || requestedAudioUrl);

  if (!normalizedSong) {
    return {
      query,
      audioId: requestedAudioId || null,
      normalizedSong: null,
    };
  }

  return {
    query: normalizedQuery || `${normalizedSong.artist} - ${normalizedSong.title}`,
    audioId: hasExplicitSource
      ? requestedAudioId || null
      : allowResolvedAudioId
        ? normalizedVideoId || null
        : null,
    normalizedSong,
  };
};
