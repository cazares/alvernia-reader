import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeSyncSessionCode,
  publishDirectorSyncState,
  readDirectorSyncState,
  releaseDirectorSyncState,
} from "../src/directorSync.js";

test("normalizeSyncSessionCode strips punctuation and caps length", () => {
  assert.equal(normalizeSyncSessionCode(" coro-uno 2026 !!! "), "COROUNO2026");
});

test("publishDirectorSyncState sanitizes outgoing page values", async () => {
  const originalFetch = globalThis.fetch;
  let requestBody = null;

  globalThis.fetch = async (_url, options) => {
    requestBody = JSON.parse(options.body);
    return {
      ok: true,
      json: async () => ({
        session: "CORO1234",
        page: 1,
        totalPages: 0,
        updatedAt: "2026-03-29T00:00:00.000Z",
      }),
    };
  };

  try {
    await publishDirectorSyncState({
      endpoint: "https://signovino.com/director-sync",
      sessionCode: "coro1234",
      directorKey: "director-1",
      page: -99,
      totalPages: -5,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.deepEqual(requestBody, {
    directorKey: "director-1",
    page: 1,
    totalPages: 0,
  });
});

test("readDirectorSyncState converts network failures into friendly errors", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("socket hang up");
  };

  try {
    await assert.rejects(
      () => readDirectorSyncState({
        endpoint: "https://signovino.com/director-sync",
        sessionCode: "CORO1234",
      }),
      (error) => error?.code === "DIRECTOR_NETWORK",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("releaseDirectorSyncState returns null for empty session codes", async () => {
  assert.equal(await releaseDirectorSyncState({
    endpoint: "https://signovino.com/director-sync",
    sessionCode: "",
    directorKey: "director-1",
  }), null);
});
