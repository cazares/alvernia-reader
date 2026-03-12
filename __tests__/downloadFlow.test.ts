/**
 * Integration tests for download flow
 */

import { downloadManager } from '../lib/downloadManager';
import * as FileSystem from 'expo-file-system';

// Test configuration
const TEST_VIDEO_ID = 'dQw4w9WgXcQ'; // Rick Astley - Never Gonna Give You Up
const DOWNLOAD_TIMEOUT = 60000; // 60s

describe('Download Flow', () => {
  afterEach(async () => {
    // Clean up after each test
    await downloadManager.cleanupOldTasks(0);
  });

  describe('Full download cycle', () => {
    test('completes full download successfully', async () => {
      let progressUpdates = 0;
      const progressStates: string[] = [];

      const task = await downloadManager.startDownload(
        TEST_VIDEO_ID,
        (progress, state) => {
          progressUpdates++;
          if (!progressStates.includes(state)) {
            progressStates.push(state);
          }
        }
      );

      // Wait for completion
      while (
        task.state !== 'ready' &&
        task.state !== 'failed' &&
        task.state !== 'cancelled'
      ) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Assertions
      expect(task.state).toBe('ready');
      expect(task.filePath).toBeTruthy();
      expect(task.provider).toBeTruthy();
      expect(task.videoId).toBe(TEST_VIDEO_ID);
      expect(task.metadata).toBeTruthy();
      expect(task.metadata?.title).toBeTruthy();

      // Verify progress updates happened
      expect(progressUpdates).toBeGreaterThan(0);

      // Verify state transitions
      expect(progressStates).toContain('resolving');
      expect(progressStates).toContain('downloading');
      expect(progressStates).toContain('ready');

      // Verify file exists and is valid
      if (task.filePath) {
        const fileInfo = await FileSystem.getInfoAsync(task.filePath);
        expect(fileInfo.exists).toBe(true);
        expect(fileInfo.size).toBeGreaterThan(1000);

        const fileSizeMB = ((fileInfo.size || 0) / 1024 / 1024).toFixed(2);
        console.log(`✓ Downloaded ${fileSizeMB} MB using ${task.provider}`);
      }
    }, DOWNLOAD_TIMEOUT);

    test('handles invalid video ID gracefully', async () => {
      const task = await downloadManager.startDownload('invalid_id');

      while (
        task.state !== 'ready' &&
        task.state !== 'failed' &&
        task.state !== 'cancelled'
      ) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      expect(task.state).toBe('failed');
      expect(task.error).toBeTruthy();
      expect(task.errorType).toBeTruthy();
    }, DOWNLOAD_TIMEOUT);
  });

  describe('Task management', () => {
    test('can retrieve task by ID', async () => {
      const task = await downloadManager.startDownload(TEST_VIDEO_ID);

      const retrieved = downloadManager.getTask(task.id);
      expect(retrieved).toBe(task);
      expect(retrieved?.id).toBe(task.id);
    });

    test('can list all tasks', async () => {
      const task1 = await downloadManager.startDownload(TEST_VIDEO_ID);
      const task2 = await downloadManager.startDownload(TEST_VIDEO_ID);

      const allTasks = downloadManager.getAllTasks();
      expect(allTasks.length).toBeGreaterThanOrEqual(2);
      expect(allTasks.some(t => t.id === task1.id)).toBe(true);
      expect(allTasks.some(t => t.id === task2.id)).toBe(true);
    });

    test('can cancel download', async () => {
      const task = await downloadManager.startDownload(TEST_VIDEO_ID);

      // Cancel immediately
      await downloadManager.cancelDownload(task.id);

      // Wait a moment for cancellation to propagate
      await new Promise(resolve => setTimeout(resolve, 1000));

      expect(task.state).toBe('cancelled');
      expect(task.error).toContain('cancel');
    }, 30000);
  });

  describe('Error handling', () => {
    test('categorizes errors correctly', async () => {
      const task = await downloadManager.startDownload('');

      while (task.state !== 'failed' && task.state !== 'ready') {
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      expect(task.state).toBe('failed');
      expect(task.errorType).toBeTruthy();
    }, 30000);

    test('provides user-friendly error messages', async () => {
      const task = await downloadManager.startDownload('invalid');

      while (task.state !== 'failed' && task.state !== 'ready') {
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      expect(task.state).toBe('failed');
      expect(task.error).toBeTruthy();
      expect(task.error).not.toMatch(/Error:/); // Should be user-friendly
    }, 30000);
  });

  describe('Provider fallback', () => {
    test('attempts fallback on client failure', async () => {
      // This test is hard to write without mocking, but we can at least
      // verify the task structure supports provider tracking
      const task = await downloadManager.startDownload(TEST_VIDEO_ID);

      while (
        task.state !== 'ready' &&
        task.state !== 'failed' &&
        task.state !== 'cancelled'
      ) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Should have a provider recorded (either ytdl-core or server)
      expect(task.provider).toBeTruthy();
      expect(['ytdl-core', 'server']).toContain(task.provider);
    }, DOWNLOAD_TIMEOUT);
  });

  describe('Cleanup', () => {
    test('cleans up old tasks', async () => {
      const task = await downloadManager.startDownload(TEST_VIDEO_ID);

      // Wait for completion
      while (task.state !== 'ready' && task.state !== 'failed') {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      const beforeCount = downloadManager.getAllTasks().length;

      // Clean up tasks older than 0ms (all tasks)
      await downloadManager.cleanupOldTasks(0);

      const afterCount = downloadManager.getAllTasks().length;
      expect(afterCount).toBeLessThan(beforeCount);
    }, DOWNLOAD_TIMEOUT);
  });
});
