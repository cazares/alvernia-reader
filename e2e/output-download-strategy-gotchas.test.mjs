import test from "node:test";
import assert from "node:assert/strict";

import { resolveOutputDownloadStrategy } from "../src/outputDownloadStrategy.js";

test("resolveOutputDownloadStrategy covers full capability matrix deterministically", () => {
  const bools = [false, true];
  for (const modernCacheDirectoryAvailable of bools) {
    for (const modernDownloadFileAsyncAvailable of bools) {
      for (const legacyCacheDirectoryAvailable of bools) {
        for (const legacyDownloadAsyncAvailable of bools) {
          const result = resolveOutputDownloadStrategy({
            modernCacheDirectoryAvailable,
            modernDownloadFileAsyncAvailable,
            legacyCacheDirectoryAvailable,
            legacyDownloadAsyncAvailable,
          });

          if (modernCacheDirectoryAvailable && modernDownloadFileAsyncAvailable) {
            assert.equal(result, "modern");
            continue;
          }
          if (legacyCacheDirectoryAvailable && legacyDownloadAsyncAvailable) {
            assert.equal(result, "legacy");
            continue;
          }
          assert.equal(result, "unavailable");
        }
      }
    }
  }
});
