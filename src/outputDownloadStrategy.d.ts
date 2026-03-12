export type OutputDownloadStrategy = "modern" | "legacy" | "unavailable";

export function resolveOutputDownloadStrategy(options?: {
  modernCacheDirectoryAvailable?: boolean;
  modernDownloadFileAsyncAvailable?: boolean;
  legacyCacheDirectoryAvailable?: boolean;
  legacyDownloadAsyncAvailable?: boolean;
}): OutputDownloadStrategy;
