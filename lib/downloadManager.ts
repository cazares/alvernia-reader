/**
 * Download manager for client-side YouTube audio downloads
 * Manages download state, progress tracking, and file persistence
 */

import * as FileSystem from 'expo-file-system/legacy';
import type { sourceMetadata } from './sourceExtractor';
import { sourceExtractor } from './sourceExtractor';
import { categorizeError, DownloadError, getUserFriendlyErrorMessage } from './downloadErrors';
import { metricsCollector } from './downloadMetrics';

export type DownloadState =
  | 'queued'
  | 'resolving'
  | 'downloading'
  | 'validating'
  | 'ready'
  | 'failed'
  | 'cancelled';

export interface DownloadTask {
  id: string;
  videoId: string;
  query: string;
  state: DownloadState;
  progress: number; // 0-100
  filePath?: string;
  error?: string;
  errorType?: string; // Categorized error type
  metadata?: sourceMetadata;
  provider?: string; // Which extractor was used
  createdAt: number;
  startedAt?: number;
  completedAt?: number;
}

export type ProgressCallback = (progress: number, state: DownloadState, task: DownloadTask) => void;

export class DownloadManager {
  private tasks: Map<string, DownloadTask> = new Map();
  private downloadDir: string;
  private activeDownloads: Map<string, FileSystem.DownloadResumable> = new Map();

  constructor() {
    const baseDir = FileSystem.cacheDirectory || FileSystem.documentDirectory;
    this.downloadDir = `${baseDir}audio/`;
  }

  /**
   * Ensure download directory exists
   */
  private async ensureDownloadDir(): Promise<void> {
    try {
      // Create the directory if needed.
      try {
        await FileSystem.makeDirectoryAsync(this.downloadDir, { intermediates: true });
        console.log('[download-mgr] ensured download directory:', this.downloadDir);
      } catch (mkdirError: any) {
        // Only ignore errors when the directory already exists.
        const info = await FileSystem.getInfoAsync(this.downloadDir);
        if (!info.exists) {
          throw mkdirError;
        }
        console.log('[download-mgr] download directory already exists:', this.downloadDir);
      }
    } catch (error) {
      console.error('[download-mgr] failed to create download directory:', error);
      throw error;
    }
  }

  /**
   * Start a new download task.
   *
   * @param query - YouTube URL or video ID
   * @param onProgress - Callback for progress updates
   * @returns DownloadTask that can be monitored
   */
  async startDownload(
    query: string,
    onProgress?: ProgressCallback
  ): Promise<DownloadTask> {
    await this.ensureDownloadDir();

    const taskId = `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const task: DownloadTask = {
      id: taskId,
      videoId: '',
      query,
      state: 'queued',
      progress: 0,
      createdAt: Date.now(),
    };

    this.tasks.set(taskId, task);
    console.log('[download-mgr] task created:', taskId);

    // Run download pipeline asynchronously
    this.runDownloadPipeline(task, onProgress).catch(error => {
      console.error('[download-mgr] pipeline error:', error);
      task.state = 'failed';
      task.error = error.message;
      onProgress?.(0, 'failed', task);
    });

    return task;
  }

  /**
   * Main download pipeline with multiple stages
   */
  private async runDownloadPipeline(
    task: DownloadTask,
    onProgress?: ProgressCallback
  ): Promise<void> {
    // Record attempt at start
    metricsCollector.recordAttempt();

    try {
      // Stage 1: Resolve metadata
      await this.stageResolveMetadata(task, onProgress);

      // Stage 2: Download audio
      await this.stageDownloadAudio(task, onProgress);

      // Stage 3: Validate file
      await this.stageValidateFile(task, onProgress);

      // Stage 4: Mark ready
      task.state = 'ready';
      task.progress = 100;
      task.completedAt = Date.now();
      onProgress?.(100, 'ready', task);

      const duration = (task.completedAt - task.startedAt!) / 1000;
      console.log(`[download-mgr] ✓ task completed in ${duration.toFixed(1)}s:`, task.id);

      // Record success metrics
      if (task.filePath) {
        try {
          const fileInfo = await FileSystem.getInfoAsync(task.filePath);
          const fileSize = fileInfo.exists && 'size' in fileInfo ? (fileInfo.size || 0) : 0;
          const durationMs = task.completedAt - task.startedAt!;

          metricsCollector.recordSuccess(
            task.provider || 'unknown',
            durationMs,
            fileSize
          );
        } catch (error) {
          console.warn('[download-mgr] could not read file stats:', error);
        }
      }

    } catch (error) {
      console.error('[download-mgr] task failed:', task.id, error);

      // Categorize error for better handling
      const categorizedError = categorizeError(error);
      task.state = 'failed';
      task.error = getUserFriendlyErrorMessage(categorizedError);
      task.errorType = categorizedError.type;

      console.error('[download-mgr] error categorized:', {
        type: categorizedError.type,
        retryable: categorizedError.retryable,
        message: task.error,
      });

      // Record failure metrics
      metricsCollector.recordFailure(
        task.provider || 'unknown',
        categorizedError.type
      );

      throw categorizedError;
    }
  }

  /**
   * Stage 1: Resolve YouTube metadata
   */
  private async stageResolveMetadata(
    task: DownloadTask,
    onProgress?: ProgressCallback
  ): Promise<void> {
    task.state = 'resolving';
    task.startedAt = Date.now();
    task.progress = 10;
    onProgress?.(10, 'resolving', task);

    console.log('[download-mgr] resolving:', task.query);
    const input = task.query.trim();
    if (!input) {
      throw new Error('Please enter a song query');
    }

    // Extract metadata (server-side via proxy)
    const metadata = await sourceExtractor.extract(input);

    task.metadata = metadata;
    task.provider = 'server-proxy';
    task.videoId = metadata.videoId;
    task.progress = 30;
    onProgress?.(30, 'resolving', task);

    console.log('[download-mgr] resolved:', {
      videoId: metadata.videoId,
      title: metadata.title,
      provider: 'server-proxy',
    });
  }

  /**
   * Stage 2: Download audio file
   */
  private async stageDownloadAudio(
    task: DownloadTask,
    onProgress?: ProgressCallback
  ): Promise<void> {
    task.state = 'downloading';
    task.progress = 30;
    onProgress?.(30, 'downloading', task);

    if (!task.metadata) {
      throw new Error('No metadata available for download');
    }

    const filename = `${Date.now()}_${task.videoId}.m4a`;
    const filePath = `${this.downloadDir}${filename}`;

    console.log('[download-mgr] starting download to:', filePath);

    // Create resumable download with proper headers for YouTube
    const downloadResumable = FileSystem.createDownloadResumable(
      task.metadata.audioUrl,
      filePath,
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
          'Accept': '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
        },
      },
      (downloadProgress) => {
        const { totalBytesWritten, totalBytesExpectedToWrite } = downloadProgress;

        // Calculate progress (30% to 90% of total progress)
        const downloadPercent =
          totalBytesExpectedToWrite > 0
            ? totalBytesWritten / totalBytesExpectedToWrite
            : 0;

        const progress = 30 + Math.min(Math.round(downloadPercent * 60), 60);
        task.progress = progress;

        // Log progress at 25% intervals
        if (progress % 25 === 0) {
          const mb = (totalBytesWritten / 1024 / 1024).toFixed(1);
          const totalMb = (totalBytesExpectedToWrite / 1024 / 1024).toFixed(1);
          console.log(`[download-mgr] progress: ${progress}% (${mb}/${totalMb} MB)`);
        }

        onProgress?.(progress, 'downloading', task);
      }
    );

    // Store for potential cancellation
    this.activeDownloads.set(task.id, downloadResumable);

    try {
      const result = await downloadResumable.downloadAsync();

      if (!result) {
        throw new Error('Download failed - no result returned');
      }

      task.filePath = result.uri;
      console.log('[download-mgr] download complete:', result.uri);

    } finally {
      // Clean up active download reference
      this.activeDownloads.delete(task.id);
    }
  }

  /**
   * Stage 3: Validate downloaded file
   */
  private async stageValidateFile(
    task: DownloadTask,
    onProgress?: ProgressCallback
  ): Promise<void> {
    task.state = 'validating';
    task.progress = 90;
    onProgress?.(90, 'validating', task);

    if (!task.filePath) {
      throw new Error('No file path to validate');
    }

    console.log('[download-mgr] validating file:', task.filePath);

    // Validate file exists and has reasonable size
    let fileInfo;
    try {
      fileInfo = await FileSystem.getInfoAsync(task.filePath);
    } catch (error) {
      throw new Error('Downloaded file does not exist or is not accessible');
    }

    if (!fileInfo.exists) {
      throw new Error('Downloaded file does not exist');
    }

    const fileSize = 'size' in fileInfo ? (fileInfo.size || 0) : 0;
    const fileSizeMB = (fileSize / 1024 / 1024).toFixed(2);

    if (fileSize < 1000) {
      throw new Error(`Downloaded file is too small (${fileSize} bytes)`);
    }

    console.log('[download-mgr] file validated:', {
      size: `${fileSizeMB} MB`,
      path: task.filePath,
    });
  }

  /**
   * Get task by ID
   */
  getTask(taskId: string): DownloadTask | undefined {
    return this.tasks.get(taskId);
  }

  /**
   * Get all tasks
   */
  getAllTasks(): DownloadTask[] {
    return Array.from(this.tasks.values());
  }

  /**
   * Cancel a download task
   */
  async cancelDownload(taskId: string): Promise<void> {
    const task = this.tasks.get(taskId);
    if (!task) {
      console.warn('[download-mgr] task not found for cancellation:', taskId);
      return;
    }

    console.log('[download-mgr] cancelling task:', taskId);

    // Cancel active download if exists
    const activeDownload = this.activeDownloads.get(taskId);
    if (activeDownload) {
      try {
        await activeDownload.pauseAsync();
      } catch (error) {
        console.warn('[download-mgr] error pausing download:', error);
      }
      this.activeDownloads.delete(taskId);
    }

    // Update task state
    task.state = 'cancelled';
    task.error = 'Cancelled by user';

    // Clean up partial file
    if (task.filePath) {
      try {
        await FileSystem.deleteAsync(task.filePath, { idempotent: true });
        console.log('[download-mgr] cleaned up partial file:', task.filePath);
      } catch (error) {
        console.warn('[download-mgr] error cleaning up file:', error);
      }
    }
  }

  /**
   * Clean up old completed/failed tasks
   */
  async cleanupOldTasks(maxAge: number = 3600000): Promise<void> {
    const now = Date.now();
    const toDelete: string[] = [];

    for (const [taskId, task] of this.tasks.entries()) {
      const age = now - task.createdAt;
      if (age > maxAge && (task.state === 'ready' || task.state === 'failed')) {
        toDelete.push(taskId);
      }
    }

    for (const taskId of toDelete) {
      this.tasks.delete(taskId);
    }

    if (toDelete.length > 0) {
      console.log(`[download-mgr] cleaned up ${toDelete.length} old tasks`);
    }
  }

  /**
   * Clean up all downloaded files
   */
  async cleanupDownloadDir(): Promise<void> {
    try {
      // Just try to delete - idempotent flag means it won't fail if doesn't exist
      await FileSystem.deleteAsync(this.downloadDir, { idempotent: true });
      await this.ensureDownloadDir();
      console.log('[download-mgr] cleaned up download directory');
    } catch (error) {
      console.error('[download-mgr] error cleaning up directory:', error);
    }
  }
}

// Singleton instance
export const downloadManager = new DownloadManager();
