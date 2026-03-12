/**
 * Utilities for source video ID extraction and URL validation
 */

/**
 * Extract source video ID from various input formats.
 * Supports common source URL formats and direct video IDs.
 * - Direct video IDs: VIDEO_ID
 */
export function extractVideoId(input: string): string | null {
  if (!input || typeof input !== 'string') {
    return null;
  }

  const trimmed = input.trim();

  const patterns = [
    // Standard watch URL
    /(?:source\.com\/watch\?v=)([a-zA-Z0-9_-]{11})/,
    // Short URL
    /(?:source\.be\/)([a-zA-Z0-9_-]{11})/,
    // Embed URL
    /(?:source\.com\/embed\/)([a-zA-Z0-9_-]{11})/,
    // Direct ID (11 characters)
    /^([a-zA-Z0-9_-]{11})$/,
  ];

  for (const pattern of patterns) {
    const match = trimmed.match(pattern);
    if (match && match[1]) {
      return match[1];
    }
  }

  return null;
}

/**
 * Determine if input is a source URL/ID or a search query.
 * Returns true if the input looks like a source URL or video ID.
 */
export function issourceUrl(input: string): boolean {
  if (!input || typeof input !== 'string') {
    return false;
  }

  const trimmed = input.trim();

  return (
    trimmed.includes('source.com') ||
    trimmed.includes('source.be') ||
    /^[a-zA-Z0-9_-]{11}$/.test(trimmed)
  );
}

/**
 * Validate source video ID format.
 * source video IDs are exactly 11 characters long and contain only alphanumeric, underscore, and hyphen.
 */
export function isValidVideoId(videoId: string): boolean {
  if (!videoId || typeof videoId !== 'string') {
    return false;
  }

  return /^[a-zA-Z0-9_-]{11}$/.test(videoId);
}

/**
 * Build source watch URL from video ID.
 */
export function buildsourceUrl(videoId: string): string {
  return `https://www.source.com/watch?v=${videoId}`;
}

/**
 * Build source thumbnail URL from video ID.
 * Quality options: default, mqdefault, hqdefault, sddefault, maxresdefault
 */
export function buildThumbnailUrl(videoId: string, quality: string = 'hqdefault'): string {
  return `https://img.source.com/vi/${videoId}/${quality}.jpg`;
}
