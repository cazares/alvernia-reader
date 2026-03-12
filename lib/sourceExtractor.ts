/**
 * source metadata extraction via API (React Native safe)
 */

import { extractVideoId, isValidVideoId } from './sourceUtils';
import { getBaseUrl } from '../src/config';
import AsyncStorage from '@react-native-async-storage/async-storage';

export interface sourceMetadata {
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

interface PersistedSourceExtractorCacheV1 {
  version: 1;
  entries: Array<{ videoId: string; metadata: sourceMetadata }>;
  queryToVideoId: Array<[string, string]>;
}

export class SourceExtractor {
  private cache: Map<string, sourceMetadata> = new Map();
  private queryToVideoId: Map<string, string> = new Map();
  private readonly CACHE_TTL = 3600 * 1000; // 1 hour
  private readonly MAX_CACHE_SIZE = 100;
  private readonly STORAGE_KEY = '@source_extractor_cache_v1';
  private cacheHydrated = false;
  private hydratePromise: Promise<void> | null = null;

  /**
   * Extract audio metadata and stream URL from source video.
   * Uses API extraction to avoid Node-only runtime dependencies on iOS/Android.
   *
   * @param input - source URL, video ID, or plain-text query
   * @returns sourceMetadata with audio URL and details
   * @throws Error if extraction fails
   */
  async extract(input: string): Promise<sourceMetadata> {
    await this.ensureHydrated();

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
    // Query cache: remember last resolved ID for repeated plain-text searches.
    if (!directVideoId) {
      const cached = this.getFromQueryCache(normalizedInput);
      if (cached) {
        console.log('[extractor] cache hit (query):', normalizedInput);
        return cached;
      }
    }

    console.log('[extractor] fetching metadata:', searchParam);

    try {
      const startTime = Date.now();
      const response = await fetch(
        `${getBaseUrl()}/source/audio-url?q=${encodeURIComponent(searchParam)}`,
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
          : Date.now() + this.CACHE_TTL;

      // Use proxy download endpoint to avoid IP-lock issues.
      // Source URLs can only be downloaded from the same IP that requested them.
      const proxyAudioUrl = `${getBaseUrl()}/source/proxy-download?q=${encodeURIComponent(resolvedVideoId)}`;

      const metadata: sourceMetadata = {
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

      this.saveToCache(resolvedVideoId, metadata, false);
      this.rememberQueryCache(normalizedInput, resolvedVideoId, true);
      return metadata;

    } catch (error: any) {
      console.error('[extractor] extraction failed:', {
        input: searchParam,
        error: error?.message,
        stack: error?.stack,
      });
      throw new Error(`source extraction failed: ${error?.message || 'Unknown error'}`);
    }
  }

  /**
   * Validate if cached URL is still valid (not expired).
   */
  private getFromCache(videoId: string): sourceMetadata | null {
    const cached = this.cache.get(videoId);
    if (!cached) {
      return null;
    }

    const now = Date.now();
    const bufferTime = 5 * 60 * 1000; // 5 minute buffer before expiry

    if (now >= cached.expiresAt - bufferTime) {
      console.log('[extractor] cache expired:', videoId);
      this.cache.delete(videoId);
      this.removeQueryAliasesForVideoId(videoId, false);
      this.persistCacheAsync();
      return null;
    }

    return cached;
  }

  /**
   * Save metadata to cache with size limit.
   */
  private saveToCache(videoId: string, metadata: sourceMetadata, persist: boolean = true): void {
    this.cache.set(videoId, metadata);

    // Limit cache size - remove oldest entries
    if (this.cache.size > this.MAX_CACHE_SIZE) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey) {
        this.cache.delete(firstKey);
        this.removeQueryAliasesForVideoId(firstKey, false);
      }
    }
    if (persist) {
      this.persistCacheAsync();
    }
  }

  private normalizeQueryCacheKey(input: string): string {
    return String(input || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ');
  }

  private getFromQueryCache(input: string): sourceMetadata | null {
    const key = this.normalizeQueryCacheKey(input);
    if (!key) {
      return null;
    }
    const videoId = this.queryToVideoId.get(key);
    if (!videoId) {
      return null;
    }
    const cached = this.getFromCache(videoId);
    if (cached) {
      this.queryToVideoId.delete(key);
      this.queryToVideoId.set(key, videoId);
      return cached;
    }
    this.queryToVideoId.delete(key);
    this.persistCacheAsync();
    return null;
  }

  private rememberQueryCache(input: string, videoId: string, persist: boolean = true): void {
    const key = this.normalizeQueryCacheKey(input);
    if (!key || !isValidVideoId(videoId)) {
      return;
    }
    this.queryToVideoId.delete(key);
    this.queryToVideoId.set(key, videoId);
    while (this.queryToVideoId.size > this.MAX_CACHE_SIZE) {
      const firstKey = this.queryToVideoId.keys().next().value;
      if (!firstKey) {
        break;
      }
      this.queryToVideoId.delete(firstKey);
    }
    if (persist) {
      this.persistCacheAsync();
    }
  }

  private removeQueryAliasesForVideoId(videoId: string, persist: boolean = true): void {
    let changed = false;
    for (const [queryKey, cachedVideoId] of this.queryToVideoId.entries()) {
      if (cachedVideoId === videoId) {
        this.queryToVideoId.delete(queryKey);
        changed = true;
      }
    }
    if (persist && changed) {
      this.persistCacheAsync();
    }
  }

  private isMetadataFresh(metadata: sourceMetadata): boolean {
    if (!metadata || !isValidVideoId(String(metadata.videoId || '').trim())) {
      return false;
    }
    const expiresAt = Number(metadata.expiresAt || 0);
    if (!Number.isFinite(expiresAt) || expiresAt <= 0) {
      return false;
    }
    const bufferTime = 5 * 60 * 1000; // 5 minute buffer before expiry
    return Date.now() < expiresAt - bufferTime;
  }

  private async ensureHydrated(): Promise<void> {
    if (this.cacheHydrated) {
      return;
    }
    if (!this.hydratePromise) {
      this.hydratePromise = this.hydrateFromStorage();
    }
    await this.hydratePromise;
  }

  private async hydrateFromStorage(): Promise<void> {
    try {
      const raw = await AsyncStorage.getItem(this.STORAGE_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw) as Partial<PersistedSourceExtractorCacheV1>;
      if (!parsed || parsed.version !== 1) {
        return;
      }
      const entries = Array.isArray(parsed.entries) ? parsed.entries : [];
      for (const entry of entries) {
        const videoId = String(entry?.videoId || '').trim();
        const metadata = entry?.metadata as sourceMetadata | undefined;
        if (!isValidVideoId(videoId) || !metadata || String(metadata.videoId || '').trim() !== videoId) {
          continue;
        }
        if (!this.isMetadataFresh(metadata)) {
          continue;
        }
        this.saveToCache(videoId, metadata, false);
      }
      const aliases = Array.isArray(parsed.queryToVideoId) ? parsed.queryToVideoId : [];
      for (const pair of aliases) {
        if (!Array.isArray(pair) || pair.length !== 2) {
          continue;
        }
        const queryKey = this.normalizeQueryCacheKey(String(pair[0] || ''));
        const videoId = String(pair[1] || '').trim();
        if (!queryKey || !isValidVideoId(videoId) || !this.cache.has(videoId)) {
          continue;
        }
        this.queryToVideoId.delete(queryKey);
        this.queryToVideoId.set(queryKey, videoId);
        while (this.queryToVideoId.size > this.MAX_CACHE_SIZE) {
          const firstKey = this.queryToVideoId.keys().next().value;
          if (!firstKey) {
            break;
          }
          this.queryToVideoId.delete(firstKey);
        }
      }
    } catch {
      // Ignore storage issues and fall back to in-memory cache only.
    } finally {
      this.cacheHydrated = true;
    }
  }

  private persistCacheAsync(clear: boolean = false): void {
    if (clear) {
      AsyncStorage.removeItem(this.STORAGE_KEY).catch(() => undefined);
      return;
    }
    if (!this.cacheHydrated) {
      return;
    }
    const entries: Array<{ videoId: string; metadata: sourceMetadata }> = [];
    for (const [videoId, metadata] of this.cache.entries()) {
      if (!this.isMetadataFresh(metadata)) {
        continue;
      }
      entries.push({ videoId, metadata });
    }
    const validVideoIds = new Set(entries.map((entry) => entry.videoId));
    const queryToVideoId: Array<[string, string]> = [];
    for (const [queryKey, videoId] of this.queryToVideoId.entries()) {
      if (!validVideoIds.has(videoId)) {
        continue;
      }
      queryToVideoId.push([queryKey, videoId]);
    }
    const payload: PersistedSourceExtractorCacheV1 = {
      version: 1,
      entries,
      queryToVideoId,
    };
    AsyncStorage.setItem(this.STORAGE_KEY, JSON.stringify(payload)).catch(() => undefined);
  }

  /**
   * Clear all cached metadata.
   */
  clearCache(): void {
    console.log('[extractor] clearing cache');
    this.cache.clear();
    this.queryToVideoId.clear();
    this.persistCacheAsync(true);
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
   * Search source and return video IDs.
   * Client search is delegated to server search endpoints.
   */
  async search(query: string): Promise<string[]> {
    console.log('[extractor] client search not implemented, use server');
    throw new Error('CLIENT_SEARCH_NOT_AVAILABLE');
  }
}

// Singleton instance
export const sourceExtractor = new SourceExtractor();
