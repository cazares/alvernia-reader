/**
 * Error categorization and handling for download operations
 */

export enum DownloadErrorType {
  EXTRACTOR_FAILED = 'extractor_failed',
  NETWORK_FAILED = 'network_failed',
  URL_EXPIRED = 'url_expired',
  DOWNLOAD_FAILED = 'download_failed',
  VALIDATION_FAILED = 'validation_failed',
  STORAGE_FULL = 'storage_full',
  TIMEOUT = 'timeout',
  CANCELLED = 'cancelled',
  UNKNOWN = 'unknown',
}

export class DownloadError extends Error {
  constructor(
    public type: DownloadErrorType,
    message: string,
    public details?: any,
    public retryable: boolean = false
  ) {
    super(message);
    this.name = 'DownloadError';
  }
}

/**
 * Categorize an error to determine type and whether it's retryable
 */
export function categorizeError(error: any): DownloadError {
  const message = error?.message || String(error);
  const lowerMessage = message.toLowerCase();

  // URL expiration errors
  if (lowerMessage.includes('expire') || lowerMessage.includes('403')) {
    return new DownloadError(
      DownloadErrorType.URL_EXPIRED,
      'Download URL expired',
      { originalError: message },
      true // Retryable - can get fresh URL
    );
  }

  // Network errors
  if (
    lowerMessage.includes('network') ||
    lowerMessage.includes('timeout') ||
    lowerMessage.includes('connection') ||
    lowerMessage.includes('econnrefused') ||
    lowerMessage.includes('enotfound')
  ) {
    return new DownloadError(
      DownloadErrorType.NETWORK_FAILED,
      'Network connection failed',
      { originalError: message },
      true // Retryable
    );
  }

  // Timeout errors
  if (lowerMessage.includes('timeout') || lowerMessage.includes('timed out')) {
    return new DownloadError(
      DownloadErrorType.TIMEOUT,
      'Operation timed out',
      { originalError: message },
      true // Retryable
    );
  }

  // Storage errors
  if (
    lowerMessage.includes('storage') ||
    lowerMessage.includes('space') ||
    lowerMessage.includes('disk full') ||
    lowerMessage.includes('enospc')
  ) {
    return new DownloadError(
      DownloadErrorType.STORAGE_FULL,
      'Not enough storage space',
      { originalError: message },
      false // Not retryable without user action
    );
  }

  // Validation errors
  if (
    lowerMessage.includes('too small') ||
    lowerMessage.includes('invalid') ||
    lowerMessage.includes('corrupt')
  ) {
    return new DownloadError(
      DownloadErrorType.VALIDATION_FAILED,
      'Downloaded file is invalid',
      { originalError: message },
      true // Retryable - might be transient
    );
  }

  // Extraction errors
  if (
    lowerMessage.includes('extract') ||
    lowerMessage.includes('provider') ||
    lowerMessage.includes('youtube') ||
    lowerMessage.includes('video unavailable')
  ) {
    return new DownloadError(
      DownloadErrorType.EXTRACTOR_FAILED,
      'Could not extract audio URL',
      { originalError: message },
      true // Retryable with fallback provider
    );
  }

  // Cancellation
  if (lowerMessage.includes('cancel')) {
    return new DownloadError(
      DownloadErrorType.CANCELLED,
      'Download cancelled by user',
      { originalError: message },
      false // Not retryable
    );
  }

  // Unknown/generic error
  return new DownloadError(
    DownloadErrorType.UNKNOWN,
    message || 'An unknown error occurred',
    { originalError: message },
    false // Default to not retryable for unknown errors
  );
}

/**
 * Get user-friendly error message for display
 */
export function getUserFriendlyErrorMessage(error: DownloadError): string {
  const typeMessages: Record<DownloadErrorType, string> = {
    [DownloadErrorType.EXTRACTOR_FAILED]:
      'Unable to find audio source. Please try again or use a different video.',
    [DownloadErrorType.NETWORK_FAILED]:
      'Network connection lost. Please check your internet connection and try again.',
    [DownloadErrorType.URL_EXPIRED]:
      'Download link expired. Retrying with fresh link...',
    [DownloadErrorType.DOWNLOAD_FAILED]:
      'Download failed. Please try again.',
    [DownloadErrorType.VALIDATION_FAILED]:
      'Downloaded file is corrupted. Please try again.',
    [DownloadErrorType.STORAGE_FULL]:
      'Not enough storage space. Please free up space and try again.',
    [DownloadErrorType.TIMEOUT]:
      'Operation timed out. Please check your connection and try again.',
    [DownloadErrorType.CANCELLED]:
      'Download cancelled.',
    [DownloadErrorType.UNKNOWN]:
      'An unexpected error occurred. Please try again.',
  };

  return typeMessages[error.type] || error.message;
}

/**
 * Determine if an error should trigger a retry
 */
export function shouldRetry(error: DownloadError, attemptNumber: number, maxAttempts: number): boolean {
  // Don't retry if max attempts reached
  if (attemptNumber >= maxAttempts) {
    return false;
  }

  // Only retry if error is marked as retryable
  return error.retryable;
}

/**
 * Calculate exponential backoff delay for retries
 */
export function getRetryDelay(attemptNumber: number, baseDelay: number = 1000): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, max 10s
  const delay = Math.min(baseDelay * Math.pow(2, attemptNumber - 1), 10000);

  // Add jitter (±20%) to prevent thundering herd
  const jitter = delay * 0.2 * (Math.random() * 2 - 1);

  return Math.round(delay + jitter);
}
