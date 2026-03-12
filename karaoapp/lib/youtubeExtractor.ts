/**
 * YouTube metadata extraction via API (React Native safe)
 */

import { extractVideoId, isValidVideoId } from './youtubeUtils';
import { getBaseUrl } from '../src/config';

export interface YouTubeMetadata {
  videoId: string;
  title: string;
  duration: number;
  thumbnail: string;
  audioUrl: string;
  format: {
    container: string; // m4a, webm, etc.
    quality: string;
    codec: string;
  };
  expiresAt: number; // Unix timestamp
}

export class YouTubeExtractor {
  private cache: Map<string, YouTubeMetadata> = new Map();
  private readonly CACHE_TTL = 3600 * 1000; // 1 hour
  private readonly MAX_CACHE_SIZE = 100;

  /**
   * Extract audio metadata and stream URL from YouTube video.
   * Uses API extraction to avoid Node-only runtime dependencies on iOS/Android.
   *
   * @param input - YouTube URL, video ID, or plain-text query
   * @returns YouTubeMetadata with audio URL and details
   * @throws Error if extraction fails
   */
  async extract(input: string): Promise<YouTubeMetadata> {
    const normalizedInput = String(input || '').trim();
    if (!normalizedInput) {
      throw new Error('Query is required');
    }
    const directVideoId = extractVideoId(normalizedInput);
    const searchParam = directVideoId || normalizedInput;

    // Check cache first when the input already includes a canonical video ID.
    if (directVideoId && isValidVideoId(directVideoId)) {
      const cached = this.getFromCache(directVideoId);
      if (cached) {
        console.log('[extractor] cache hit:', directVideoId);
        return cached;
      }
    }

    console.log('[extractor] fetching metadata:', searchParam);

    try {
      const startTime = Date.now();
      const baseUrl = getBaseUrl();
      const response = await fetch(
        `${baseUrl}/youtube/audio-url?q=${encodeURIComponent(searchParam)}`,
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      );

      if (!response.ok) {
        throw new Error(`Server returned ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      const fetchTime = Date.now() - startTime;
      console.log(`[extractor] metadata fetched in ${fetchTime}ms`);

      const resolvedVideoId = String(data.video_id || directVideoId || '');
      if (!resolvedVideoId || !isValidVideoId(resolvedVideoId)) {
        throw new Error('Server did not return a valid video ID');
      }

      const audioUrl = data.audio_url || data.url || '';
      if (!audioUrl) {
        throw new Error('No valid audio URL found');
      }

      const duration = Number(data.duration || 0);
      const expiresAt =
        typeof data.expires_at === 'number'
          ? Math.round(data.expires_at * 1000)
          : Date.now() + 3600 * 1000;

      // Use proxy download endpoint to avoid IP-lock issues
      // YouTube URLs can only be downloaded from the same IP that requested them
      const proxyAudioUrl = `${baseUrl}/youtube/proxy-download?q=${encodeURIComponent(resolvedVideoId)}`;

      const metadata: YouTubeMetadata = {
        videoId: resolvedVideoId,
        title: data.title || 'Unknown Title',
        duration: Number.isFinite(duration) ? duration : 0,
        thumbnail: data.thumbnail || '',
        audioUrl: proxyAudioUrl,
        format: {
          container: data.format?.container || 'm4a',
          quality: data.format?.quality || 'audio',
          codec: data.format?.codec || 'unknown',
        },
        expiresAt,
      };

      console.log('[extractor] extraction successful:', {
        videoId: resolvedVideoId,
        title: metadata.title,
        duration: metadata.duration,
        container: metadata.format.container,
        expiresIn: Math.round((expiresAt - Date.now()) / 1000 / 60) + 'min',
      });

      this.saveToCache(resolvedVideoId, metadata);
      return metadata;

    } catch (error: any) {
      console.error('[extractor] extraction failed:', {
        input: searchParam,
        error: error?.message,
        stack: error?.stack,
      });
      throw new Error(`YouTube extraction failed: ${error?.message || 'Unknown error'}`);
    }
  }

  /**
   * Validate if cached URL is still valid (not expired).
   */
  private getFromCache(videoId: string): YouTubeMetadata | null {
    const cached = this.cache.get(videoId);
    if (!cached) {
      return null;
    }

    const now = Date.now();
    const bufferTime = 5 * 60 * 1000; // 5 minute buffer before expiry

    if (now >= cached.expiresAt - bufferTime) {
      console.log('[extractor] cache expired:', videoId);
      this.cache.delete(videoId);
      return null;
    }

    return cached;
  }

  /**
   * Save metadata to cache with size limit.
   */
  private saveToCache(videoId: string, metadata: YouTubeMetadata): void {
    this.cache.set(videoId, metadata);

    // Limit cache size - remove oldest entries
    if (this.cache.size > this.MAX_CACHE_SIZE) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey) {
        this.cache.delete(firstKey);
      }
    }
  }

  /**
   * Clear all cached metadata.
   */
  clearCache(): void {
    console.log('[extractor] clearing cache');
    this.cache.clear();
  }

  /**
   * Get cache statistics.
   */
  getCacheStats(): { size: number; maxSize: number } {
    return {
      size: this.cache.size,
      maxSize: this.MAX_CACHE_SIZE,
    };
  }

  /**
   * Search YouTube and return video IDs.
   * Client search is delegated to server search endpoints.
   */
  async search(query: string): Promise<string[]> {
    console.log('[extractor] client search not implemented, use server');
    throw new Error('CLIENT_SEARCH_NOT_AVAILABLE');
  }
}

// Singleton instance
export const youtubeExtractor = new YouTubeExtractor();
