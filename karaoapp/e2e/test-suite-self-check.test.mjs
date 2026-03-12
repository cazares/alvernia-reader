import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

import { normalizeJobStatus } from "../src/jobStatus.js";
import { resolveOutputDownloadStrategy } from "../src/outputDownloadStrategy.js";
import { canAcknowledgeEmbedReady, canSelectVideoTab } from "../src/presetSessionGates.js";

const thisDir = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(thisDir, "..");

const CRITICAL_TEST_FILES = [
  "e2e/create-job-payload.test.mjs",
  "e2e/create-job-payload-gotchas.test.mjs",
  "e2e/eas-config.test.mjs",
  "e2e/job-status-flow.test.mjs",
  "e2e/job-status-gotchas.test.mjs",
  "e2e/permission-flow.test.mjs",
  "e2e/permission-flow-gotchas.test.mjs",
  "e2e/output-download-strategy.test.mjs",
  "e2e/output-download-strategy-gotchas.test.mjs",
  "e2e/preset-session-gates.test.mjs",
  "e2e/video-handoff.test.mjs",
  "e2e/video-handoff-gotchas.test.mjs",
];

const countMatches = (text, pattern) => (String(text || "").match(pattern) || []).length;

test("meta suite: critical test files exist and contain runnable tests", () => {
  for (const relativePath of CRITICAL_TEST_FILES) {
    const fullPath = path.resolve(appRoot, relativePath);
    assert.equal(fs.existsSync(fullPath), true, `Missing required test file: ${relativePath}`);
    const source = fs.readFileSync(fullPath, "utf8");
    const testCount = countMatches(source, /\btest\s*\(/g);
    assert.ok(testCount >= 1, `Expected at least one test() in ${relativePath}`);
  }
});

test("meta suite: aggregate test footprint meets minimum density and assertion depth", () => {
  let totalTestBlocks = 0;
  let totalAssertions = 0;

  for (const relativePath of CRITICAL_TEST_FILES) {
    const fullPath = path.resolve(appRoot, relativePath);
    const source = fs.readFileSync(fullPath, "utf8");
    totalTestBlocks += countMatches(source, /\btest\s*\(/g);
    totalAssertions += countMatches(source, /\bassert\./g);
  }

  // Guardrails: prevent suite erosion over time.
  assert.ok(totalTestBlocks >= 50, `Expected >= 50 tests, found ${totalTestBlocks}`);
  assert.ok(totalAssertions >= 180, `Expected >= 180 assertions, found ${totalAssertions}`);
});

test("meta suite: oracle cases detect simple mutant behaviors", () => {
  const normalizeJobStatusMutant = () => "running";
  const jobCases = [
    ["completed", "succeeded"],
    ["failed", "failed"],
    ["cancelled", "cancelled"],
    ["queued", "queued"],
    ["wat", "unknown"],
  ];
  let killedJobMutant = false;
  for (const [input, expected] of jobCases) {
    assert.equal(normalizeJobStatus(input), expected, `oracle mismatch for ${input}`);
    if (normalizeJobStatusMutant(input) !== expected) {
      killedJobMutant = true;
    }
  }
  assert.equal(killedJobMutant, true, "job status cases failed to detect mutant");

  const strategyMutant = () => "unavailable";
  const strategyCases = [
    [{ modernCacheDirectoryAvailable: true, modernDownloadFileAsyncAvailable: true }, "modern"],
    [
      {
        modernCacheDirectoryAvailable: false,
        modernDownloadFileAsyncAvailable: false,
        legacyCacheDirectoryAvailable: true,
        legacyDownloadAsyncAvailable: true,
      },
      "legacy",
    ],
  ];
  let killedStrategyMutant = false;
  for (const [input, expected] of strategyCases) {
    assert.equal(resolveOutputDownloadStrategy(input), expected);
    if (strategyMutant(input) !== expected) {
      killedStrategyMutant = true;
    }
  }
  assert.equal(killedStrategyMutant, true, "download strategy cases failed to detect mutant");

  const readyMutant = () => ({ accept: true, reason: "ready" });
  const staleCase = {
    activeVideoId: "IKeeYvrexlU",
    pendingSessionJobId: "job-1",
    expectedPendingVideoId: "abc123def45",
  };
  assert.deepEqual(canAcknowledgeEmbedReady(staleCase), {
    accept: false,
    reason: "stale_video_ready",
  });
  assert.notDeepEqual(readyMutant(staleCase), canAcknowledgeEmbedReady(staleCase));

  const selectMutant = ({ videoTabEnabled }) => Boolean(videoTabEnabled);
  const selectCase = { videoTabEnabled: true, presetRequestInFlight: true };
  assert.equal(canSelectVideoTab(selectCase), false);
  assert.notEqual(selectMutant(selectCase), canSelectVideoTab(selectCase));
});

test("meta suite: runner can execute critical specs in a subprocess", () => {
  const run = spawnSync(
    process.execPath,
    [
      "--test",
      "./e2e/job-status-gotchas.test.mjs",
      "./e2e/preset-session-gates.test.mjs",
      "./e2e/create-job-payload-gotchas.test.mjs",
    ],
    {
      cwd: appRoot,
      env: process.env,
      encoding: "utf8",
      timeout: 120000,
    }
  );

  if (run.status !== 0) {
    const stderr = String(run.stderr || "").trim();
    const stdout = String(run.stdout || "").trim();
    assert.fail(
      `Subprocess test runner failed with code ${run.status}\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`
    );
  }
});
