/**
 * Client-side metrics tracking for download operations
 */

import { getBaseUrl } from '../src/config';
import Constants from 'expo-constants';

export interface DownloadMetrics {
  total_attempts: number;
  successful_downloads: number;
  client_success: number;
  server_fallback: number;
  failures: number;

  average_duration_ms: number;
  average_file_size_bytes: number;

  provider_stats: {
    [provider: string]: {
      attempts: number;
      successes: number;
      failures: number;
    };
  };

  error_types: {
    [error: string]: number;
  };
}

export interface DownloadEvent {
  type: 'attempt' | 'success' | 'failure';
  provider?: string;
  durationMs?: number;
  fileSizeBytes?: number;
  errorType?: string;
  timestamp: number;
}

class MetricsCollector {
  private metrics: DownloadMetrics = {
    total_attempts: 0,
    successful_downloads: 0,
    client_success: 0,
    server_fallback: 0,
    failures: 0,
    average_duration_ms: 0,
    average_file_size_bytes: 0,
    provider_stats: {},
    error_types: {},
  };

  private events: DownloadEvent[] = [];
  private readonly MAX_EVENTS = 100;
  private lastSyncTime: number = Date.now();
  private readonly SYNC_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

  /**
   * Record a download attempt
   */
  recordAttempt(provider: string = 'unknown'): void {
    this.metrics.total_attempts++;

    // Initialize provider stats if needed
    if (!this.metrics.provider_stats[provider]) {
      this.metrics.provider_stats[provider] = { attempts: 0, successes: 0, failures: 0 };
    }
    this.metrics.provider_stats[provider].attempts++;

    this.events.push({
      type: 'attempt',
      provider,
      timestamp: Date.now(),
    });

    console.log('[metrics] attempt recorded:', {
      provider,
      total: this.metrics.total_attempts,
    });
  }

  /**
   * Record a successful download
   */
  recordSuccess(provider: string, durationMs: number, fileSizeBytes: number): void {
    this.metrics.successful_downloads++;

    if (provider === 'ytdl-core') {
      this.metrics.client_success++;
    } else if (provider === 'server') {
      this.metrics.server_fallback++;
    }

    // Update provider stats
    if (!this.metrics.provider_stats[provider]) {
      this.metrics.provider_stats[provider] = { attempts: 0, successes: 0, failures: 0 };
    }
    this.metrics.provider_stats[provider].successes++;

    // Update running averages
    const total = this.metrics.successful_downloads;
    this.metrics.average_duration_ms =
      (this.metrics.average_duration_ms * (total - 1) + durationMs) / total;
    this.metrics.average_file_size_bytes =
      (this.metrics.average_file_size_bytes * (total - 1) + fileSizeBytes) / total;

    this.events.push({
      type: 'success',
      provider,
      durationMs,
      fileSizeBytes,
      timestamp: Date.now(),
    });

    console.log('[metrics] success recorded:', {
      provider,
      durationMs: Math.round(durationMs),
      fileSizeMB: (fileSizeBytes / 1024 / 1024).toFixed(2),
      successRate: this.getSuccessRate().toFixed(1) + '%',
    });

    // Auto-sync metrics if interval passed
    this.autoSync();
  }

  /**
   * Record a download failure
   */
  recordFailure(provider: string, errorType: string): void {
    this.metrics.failures++;

    // Update provider stats
    if (!this.metrics.provider_stats[provider]) {
      this.metrics.provider_stats[provider] = { attempts: 0, successes: 0, failures: 0 };
    }
    this.metrics.provider_stats[provider].failures++;

    // Update error types
    this.metrics.error_types[errorType] = (this.metrics.error_types[errorType] || 0) + 1;

    this.events.push({
      type: 'failure',
      provider,
      errorType,
      timestamp: Date.now(),
    });

    console.log('[metrics] failure recorded:', {
      provider,
      errorType,
      failureRate: ((this.metrics.failures / this.metrics.total_attempts) * 100).toFixed(1) + '%',
    });

    // Auto-sync metrics if interval passed
    this.autoSync();
  }

  /**
   * Get current metrics snapshot
   */
  getMetrics(): DownloadMetrics {
    return { ...this.metrics };
  }

  /**
   * Calculate overall success rate
   */
  getSuccessRate(): number {
    return this.metrics.total_attempts > 0
      ? (this.metrics.successful_downloads / this.metrics.total_attempts) * 100
      : 0;
  }

  /**
   * Calculate client-side success rate (of successful downloads)
   */
  getClientSuccessRate(): number {
    return this.metrics.successful_downloads > 0
      ? (this.metrics.client_success / this.metrics.successful_downloads) * 100
      : 0;
  }

  /**
   * Calculate server fallback rate (of successful downloads)
   */
  getFallbackRate(): number {
    return this.metrics.successful_downloads > 0
      ? (this.metrics.server_fallback / this.metrics.successful_downloads) * 100
      : 0;
  }

  /**
   * Get recent events
   */
  getRecentEvents(count: number = 10): DownloadEvent[] {
    return this.events.slice(-count);
  }

  /**
   * Auto-sync metrics to server if interval has passed
   */
  private autoSync(): void {
    const now = Date.now();
    if (now - this.lastSyncTime >= this.SYNC_INTERVAL_MS) {
      this.sendToServer();
      this.lastSyncTime = now;
    }
  }

  /**
   * Send metrics to server for monitoring
   */
  async sendToServer(): Promise<void> {
    try {
      console.log('[metrics] sending to server...');
      const appVersion =
        Constants.expoConfig?.version ||
        (Constants.expoConfig?.extra?.appVersion as string | undefined) ||
        '1.2.0.1';

      const payload = {
        metrics: this.getMetrics(),
        derived: {
          success_rate: this.getSuccessRate(),
          client_success_rate: this.getClientSuccessRate(),
          fallback_rate: this.getFallbackRate(),
        },
        device: {
          platform: 'ios',
          app_version: appVersion,
          timestamp: Date.now(),
        },
        recent_events: this.getRecentEvents(20),
      };

      const response = await fetch(`${getBaseUrl()}/metrics/download`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        console.warn('[metrics] server returned non-OK:', response.status);
        return;
      }

      console.log('[metrics] successfully sent to server');

      // Trim events after successful sync
      if (this.events.length > this.MAX_EVENTS) {
        this.events = this.events.slice(-this.MAX_EVENTS);
      }

    } catch (error) {
      console.warn('[metrics] failed to send to server:', error);
      // Don't throw - metrics are non-critical
    }
  }

  /**
   * Reset all metrics (useful for testing)
   */
  reset(): void {
    this.metrics = {
      total_attempts: 0,
      successful_downloads: 0,
      client_success: 0,
      server_fallback: 0,
      failures: 0,
      average_duration_ms: 0,
      average_file_size_bytes: 0,
      provider_stats: {},
      error_types: {},
    };
    this.events = [];
    console.log('[metrics] reset complete');
  }

  /**
   * Get summary statistics as formatted string
   */
  getSummary(): string {
    const successRate = this.getSuccessRate();
    const clientRate = this.getClientSuccessRate();
    const fallbackRate = this.getFallbackRate();

    return [
      `📊 Download Metrics Summary`,
      `─────────────────────────`,
      `Total Attempts: ${this.metrics.total_attempts}`,
      `Successes: ${this.metrics.successful_downloads}`,
      `Failures: ${this.metrics.failures}`,
      `Success Rate: ${successRate.toFixed(1)}%`,
      ``,
      `Client Success: ${this.metrics.client_success} (${clientRate.toFixed(1)}%)`,
      `Server Fallback: ${this.metrics.server_fallback} (${fallbackRate.toFixed(1)}%)`,
      ``,
      `Avg Duration: ${(this.metrics.average_duration_ms / 1000).toFixed(1)}s`,
      `Avg File Size: ${(this.metrics.average_file_size_bytes / 1024 / 1024).toFixed(1)} MB`,
    ].join('\n');
  }
}

// Singleton instance
export const metricsCollector = new MetricsCollector();

/**
 * Initialize metrics collection with periodic syncing
 */
export function initMetrics(): void {
  console.log('[metrics] initializing...');

  // Send metrics every 5 minutes
  setInterval(() => {
    metricsCollector.sendToServer();
  }, 5 * 60 * 1000);

  // Log summary every 10 downloads
  let downloadCount = 0;
  const originalRecordSuccess = metricsCollector.recordSuccess.bind(metricsCollector);
  metricsCollector.recordSuccess = (provider: string, duration: number, size: number) => {
    originalRecordSuccess(provider, duration, size);
    downloadCount++;
    if (downloadCount % 10 === 0) {
      console.log(metricsCollector.getSummary());
    }
  };

  console.log('[metrics] initialized');
}
