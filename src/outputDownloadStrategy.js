export const resolveOutputDownloadStrategy = ({
  modernCacheDirectoryAvailable = false,
  modernDownloadFileAsyncAvailable = false,
  legacyCacheDirectoryAvailable = false,
  legacyDownloadAsyncAvailable = false,
} = {}) => {
  if (modernCacheDirectoryAvailable && modernDownloadFileAsyncAvailable) {
    return "modern";
  }
  if (legacyCacheDirectoryAvailable && legacyDownloadAsyncAvailable) {
    return "legacy";
  }
  return "unavailable";
};
