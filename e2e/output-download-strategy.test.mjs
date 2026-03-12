import test from "node:test";
import assert from "node:assert/strict";

import { resolveOutputDownloadStrategy } from "../src/outputDownloadStrategy.js";

test("output download strategy prefers modern Expo file API when both modern and legacy APIs are available", () => {
  const strategy = resolveOutputDownloadStrategy({
    modernCacheDirectoryAvailable: true,
    modernDownloadFileAsyncAvailable: true,
    legacyCacheDirectoryAvailable: true,
    legacyDownloadAsyncAvailable: true,
  });
  assert.equal(strategy, "modern");
});

test("output download strategy falls back to legacy API when modern API is unavailable", () => {
  const strategy = resolveOutputDownloadStrategy({
    modernCacheDirectoryAvailable: true,
    modernDownloadFileAsyncAvailable: false,
    legacyCacheDirectoryAvailable: true,
    legacyDownloadAsyncAvailable: true,
  });
  assert.equal(strategy, "legacy");
});

test("output download strategy reports unavailable when no download API is usable", () => {
  const strategy = resolveOutputDownloadStrategy({
    modernCacheDirectoryAvailable: false,
    modernDownloadFileAsyncAvailable: false,
    legacyCacheDirectoryAvailable: false,
    legacyDownloadAsyncAvailable: false,
  });
  assert.equal(strategy, "unavailable");
});
