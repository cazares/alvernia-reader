/**
 * Unit tests for YouTube extraction utilities
 */

import { extractVideoId, isYouTubeUrl, isValidVideoId, buildYouTubeUrl } from '../lib/youtubeUtils';
import { YouTubeExtractor } from '../lib/youtubeExtractor';

describe('YouTube Utils', () => {
  describe('extractVideoId', () => {
    test('extracts video ID from full URL', () => {
      const id = extractVideoId('https://www.youtube.com/watch?v=dQw4w9WgXcQ');
      expect(id).toBe('dQw4w9WgXcQ');
    });

    test('extracts video ID from short URL', () => {
      const id = extractVideoId('https://youtu.be/dQw4w9WgXcQ');
      expect(id).toBe('dQw4w9WgXcQ');
    });

    test('extracts video ID from embed URL', () => {
      const id = extractVideoId('https://www.youtube.com/embed/dQw4w9WgXcQ');
      expect(id).toBe('dQw4w9WgXcQ');
    });

    test('recognizes direct video ID', () => {
      const id = extractVideoId('dQw4w9WgXcQ');
      expect(id).toBe('dQw4w9WgXcQ');
    });

    test('returns null for invalid input', () => {
      expect(extractVideoId('not a youtube url')).toBeNull();
      expect(extractVideoId('')).toBeNull();
      expect(extractVideoId('https://placeholder.invalid')).toBeNull();
    });

    test('handles whitespace', () => {
      const id = extractVideoId('  https://youtu.be/dQw4w9WgXcQ  ');
      expect(id).toBe('dQw4w9WgXcQ');
    });
  });

  describe('isYouTubeUrl', () => {
    test('recognizes YouTube URLs', () => {
      expect(isYouTubeUrl('https://www.youtube.com/watch?v=dQw4w9WgXcQ')).toBe(true);
      expect(isYouTubeUrl('https://youtu.be/dQw4w9WgXcQ')).toBe(true);
      expect(isYouTubeUrl('dQw4w9WgXcQ')).toBe(true);
    });

    test('rejects non-YouTube URLs', () => {
      expect(isYouTubeUrl('https://placeholder.invalid')).toBe(false);
      expect(isYouTubeUrl('not a url')).toBe(false);
      expect(isYouTubeUrl('')).toBe(false);
    });
  });

  describe('isValidVideoId', () => {
    test('validates correct video ID format', () => {
      expect(isValidVideoId('dQw4w9WgXcQ')).toBe(true);
      expect(isValidVideoId('abc-_123456')).toBe(true);
    });

    test('rejects invalid video ID format', () => {
      expect(isValidVideoId('tooshort')).toBe(false);
      expect(isValidVideoId('waytooooolong')).toBe(false);
      expect(isValidVideoId('invalid@char')).toBe(false);
      expect(isValidVideoId('')).toBe(false);
    });
  });

  describe('buildYouTubeUrl', () => {
    test('builds correct URL from video ID', () => {
      const url = buildYouTubeUrl('dQw4w9WgXcQ');
      expect(url).toBe('https://www.youtube.com/watch?v=dQw4w9WgXcQ');
    });
  });
});

describe('YouTube Extractor', () => {
  const extractor = new YouTubeExtractor();

  // Use a well-known, stable test video
  const TEST_VIDEO_ID = 'dQw4w9WgXcQ'; // Rick Astley - Never Gonna Give You Up

  describe('extract', () => {
    test('extracts metadata successfully', async () => {
      const metadata = await extractor.extract(TEST_VIDEO_ID);

      expect(metadata.videoId).toBe(TEST_VIDEO_ID);
      expect(metadata.audioUrl).toBeTruthy();
      expect(metadata.audioUrl).toMatch(/^https?:\/\//);
      expect(metadata.title).toBeTruthy();
      expect(metadata.duration).toBeGreaterThan(0);
      expect(metadata.thumbnail).toBeTruthy();
      expect(metadata.format.container).toBeTruthy();
      expect(metadata.expiresAt).toBeGreaterThan(Date.now());
    }, 30000); // 30s timeout for network request

    test('rejects invalid video ID', async () => {
      await expect(extractor.extract('invalid')).rejects.toThrow();
    });

    test('rejects empty input', async () => {
      await expect(extractor.extract('')).rejects.toThrow();
    });
  });

  describe('caching', () => {
    test('uses cache on second request', async () => {
      // First request
      const start1 = Date.now();
      const metadata1 = await extractor.extract(TEST_VIDEO_ID);
      const duration1 = Date.now() - start1;

      // Second request (should be cached)
      const start2 = Date.now();
      const metadata2 = await extractor.extract(TEST_VIDEO_ID);
      const duration2 = Date.now() - start2;

      // Cached request should be much faster (at least 10x)
      expect(duration2).toBeLessThan(duration1 / 10);

      // Metadata should be identical
      expect(metadata2).toEqual(metadata1);
    }, 30000);

    test('cache stats are accurate', async () => {
      extractor.clearCache();

      const statsBefore = extractor.getCacheStats();
      expect(statsBefore.size).toBe(0);

      await extractor.extract(TEST_VIDEO_ID);

      const statsAfter = extractor.getCacheStats();
      expect(statsAfter.size).toBe(1);
      expect(statsAfter.maxSize).toBeGreaterThan(0);
    }, 30000);
  });

  describe('search', () => {
    test('throws not implemented error', async () => {
      await expect(extractor.search('test query')).rejects.toThrow('CLIENT_SEARCH_NOT_AVAILABLE');
    });
  });
});
