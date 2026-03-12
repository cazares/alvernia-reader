export type QueryPreflightNormalizedSong = {
  artist: string;
  title: string;
};

export type QueryPreflightResult = {
  query: string;
  audioId: string | null;
  normalizedSong: QueryPreflightNormalizedSong | null;
};

export function resolveQueryPreflightResult(args?: {
  rawQuery?: string;
  normalizedPayload?: {
    artist?: string;
    track?: string;
    title?: string;
    normalized_query?: string;
    display?: string;
    video_id?: string;
  } | null;
  explicitAudioId?: string;
  explicitAudioUrl?: string;
  allowResolvedAudioId?: boolean;
}): QueryPreflightResult;
