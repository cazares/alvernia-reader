/**
 * Utilities for YouTube video ID extraction and URL validation
 */

/**
 * Extract YouTube video ID from various input formats.
 * Supports:
 * - Full URLs: https://www.youtube.com/watch?v=VIDEO_ID
 * - Short URLs: https://youtu.be/VIDEO_ID
 * - Embed URLs: https://www.youtube.com/embed/VIDEO_ID
 * - Direct video IDs: VIDEO_ID
 */
export function extractVideoId(input: string): string | null {
  if (!input || typeof input !== 'string') {
    return null;
  }

  const trimmed = input.trim();

  const patterns = [
    // Standard watch URL
    /(?:youtube\.com\/watch\?v=)([a-zA-Z0-9_-]{11})/,
    // Short URL
    /(?:youtu\.be\/)([a-zA-Z0-9_-]{11})/,
    // Embed URL
    /(?:youtube\.com\/embed\/)([a-zA-Z0-9_-]{11})/,
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
 * Determine if input is a YouTube URL/ID or a search query.
 * Returns true if the input looks like a YouTube URL or video ID.
 */
export function isYouTubeUrl(input: string): boolean {
  if (!input || typeof input !== 'string') {
    return false;
  }

  const trimmed = input.trim();

  return (
    trimmed.includes('youtube.com') ||
    trimmed.includes('youtu.be') ||
    /^[a-zA-Z0-9_-]{11}$/.test(trimmed)
  );
}

/**
 * Validate YouTube video ID format.
 * YouTube video IDs are exactly 11 characters long and contain only alphanumeric, underscore, and hyphen.
 */
export function isValidVideoId(videoId: string): boolean {
  if (!videoId || typeof videoId !== 'string') {
    return false;
  }

  return /^[a-zA-Z0-9_-]{11}$/.test(videoId);
}

/**
 * Build YouTube watch URL from video ID.
 */
export function buildYouTubeUrl(videoId: string): string {
  return `https://www.youtube.com/watch?v=${videoId}`;
}

/**
 * Build YouTube thumbnail URL from video ID.
 * Quality options: default, mqdefault, hqdefault, sddefault, maxresdefault
 */
export function buildThumbnailUrl(videoId: string, quality: string = 'hqdefault'): string {
  return `https://img.youtube.com/vi/${videoId}/${quality}.jpg`;
}
