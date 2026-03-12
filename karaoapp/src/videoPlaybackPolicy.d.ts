export type PlaybackSource =
  | { kind: "youtube"; url: string }
  | { kind: "direct"; url: string }
  | { kind: "none"; url: null };

export function resolvePlaybackSource(args?: {
  youtubeEmbedUrl?: string | null;
  finalOutputUrl?: string | null;
  previewOutputUrl?: string | null;
  legacyOutputUrl?: string | null;
}): PlaybackSource;

export function isPlaybackSourceReady(
  source: PlaybackSource | null | undefined,
  youtubeEmbedReady: boolean
): boolean;
