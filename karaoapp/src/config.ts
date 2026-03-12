/**
 * Configuration for client-first source download strategy
 */

export type DownloadStrategy = 'local_first' | 'server_fallback' | 'server_only';

export interface DownloadConfig {
  strategy: DownloadStrategy;
  enableClientExtraction: boolean;
  enableServerFallback: boolean;
  maxClientRetries: number;
  clientTimeout: number;
  fallbackThreshold: number; // After N failures, force server
}

export const DOWNLOAD_CONFIG: DownloadConfig = {
  strategy: 'server_only', // Server downloads via Webshare proxy, streams to client
  enableClientExtraction: false,
  enableServerFallback: false,
  maxClientRetries: 3,
  clientTimeout: 60000,
  fallbackThreshold: 2,
};

import { ensureApiBaseUrl, getApiBaseUrl } from './apiBaseUrl';

// Base URL for API (supports both legacy and newer env names).
// NOTE: This is initialized at module-load but can be changed at runtime by the app (failover).
const DEFAULT_PRIMARY_API_BASE_URL = 'http://127.0.0.1:8000';
export const PRIMARY_API_BASE_URL =
  process.env.EXPO_PUBLIC_API_BASE_URL ||
  process.env.EXPO_PUBLIC_API_URL ||
  DEFAULT_PRIMARY_API_BASE_URL;

// Backup host (direct backup URL) for when the custom domain is down.
export const FALLBACK_API_BASE_URL = 'http://127.0.0.1:8000';

// Initialize runtime base URL once.
ensureApiBaseUrl(PRIMARY_API_BASE_URL);

export const getBaseUrl = (): string => getApiBaseUrl() || DEFAULT_PRIMARY_API_BASE_URL;

/**
 * Remote kill switch - can force server_only if client breaks.
 * Fetches configuration from server to allow runtime changes.
 */
export async function getRemoteConfig(): Promise<Partial<DownloadConfig>> {
  try {
    const response = await fetch(`${getBaseUrl()}/config/download-strategy`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      console.warn('[config] Failed to fetch remote config:', response.status);
      return {};
    }

    const data = await response.json();
    console.log('[config] Remote config fetched:', data);
    return data;
  } catch (error) {
    console.warn('[config] Error fetching remote config:', error);
    return {}; // Use defaults if remote config unavailable
  }
}

/**
 * Get effective configuration by merging local defaults with remote overrides.
 */
export async function getEffectiveConfig(): Promise<DownloadConfig> {
  const remoteConfig = await getRemoteConfig();
  return {
    ...DOWNLOAD_CONFIG,
    ...remoteConfig,
  };
}

/**
 * Success criteria for Phase 1 rollout
 */
export const SUCCESS_CRITERIA = {
  successRate: 0.90, // 90%+ successful downloads
  clientExtractionRate: 0.80, // 80%+ use client extraction (Phase 1)
  medianLatencyMs: 15000, // <15s time-to-first-play
  maxBatteryDrain: 0.05, // <5% battery for 3-min song
  maxFileSizeMB: 50, // Support up to 50MB audio files
};

/**
 * Phase targets for progressive improvement
 */
export const PHASE_TARGETS = {
  phase1: {
    clientSuccessRate: { min: 0.80, target: 0.90 },
    serverFallbackRate: { max: 0.20, target: 0.10 },
    timeline: 'Weeks 1-4',
  },
  phase2: {
    clientSuccessRate: { min: 0.90, target: 0.95 },
    serverFallbackRate: { max: 0.10, target: 0.05 },
    timeline: 'Months 2-3',
  },
  phase3: {
    clientSuccessRate: { min: 0.95, target: 0.98 },
    serverFallbackRate: { max: 0.05, target: 0.02 },
    timeline: 'Months 4-6',
  },
};
