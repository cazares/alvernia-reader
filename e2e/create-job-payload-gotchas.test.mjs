import test from "node:test";
import assert from "node:assert/strict";

import {
  buildCookieCandidates,
  buildCreateJobPayload,
  normalizeNetscapeCookies,
} from "../src/createJobPayload.js";

const COOKIE_HEADER = "# Netscape HTTP Cookie File";
const COOKIE_ROW_A = ".youtube.com\tTRUE\t/\tTRUE\t0\tSID\tabc123";
const COOKIE_ROW_B = ".youtube.com\tTRUE\t/\tTRUE\t0\tSAPISID\tdef456";

test("buildCookieCandidates splits multi-block cookie payload by Netscape header", () => {
  const raw = [
    COOKIE_HEADER,
    COOKIE_ROW_A,
    "",
    COOKIE_HEADER,
    COOKIE_ROW_B,
  ].join("\n");
  const candidates = buildCookieCandidates(raw);
  assert.equal(candidates.length, 2);
  assert.equal(candidates[0], `${COOKIE_HEADER}\n${COOKIE_ROW_A}`);
  assert.equal(candidates[1], `${COOKIE_HEADER}\n${COOKIE_ROW_B}`);
});

test("buildCookieCandidates splits cookie payload by separator when header split is unavailable", () => {
  const raw = [COOKIE_ROW_A, "---", COOKIE_ROW_B].join("\n");
  const candidates = buildCookieCandidates(raw);
  assert.equal(candidates.length, 2);
  assert.equal(candidates[0], COOKIE_ROW_A);
  assert.equal(candidates[1], COOKIE_ROW_B);
});

test("buildCookieCandidates removes duplicate cookie candidates", () => {
  const raw = [COOKIE_ROW_A, "---", COOKIE_ROW_A].join("\n");
  const candidates = buildCookieCandidates(raw);
  assert.deepEqual(candidates, [COOKIE_ROW_A]);
});

test("buildCookieCandidates ignores candidate chunks without valid cookie rows", () => {
  const raw = [
    COOKIE_HEADER,
    "# comment only",
    "",
    COOKIE_HEADER,
    COOKIE_ROW_A,
  ].join("\n");
  const candidates = buildCookieCandidates(raw);
  assert.deepEqual(candidates, [`${COOKIE_HEADER}\n${COOKIE_ROW_A}`]);
});

test("buildCookieCandidates returns empty list for empty input", () => {
  assert.deepEqual(buildCookieCandidates(""), []);
  assert.deepEqual(buildCookieCandidates("   "), []);
});

test("normalizeNetscapeCookies normalizes newlines and trims whitespace", () => {
  const raw = `\r\n${COOKIE_HEADER}\r\n${COOKIE_ROW_A}\r\n`;
  assert.equal(normalizeNetscapeCookies(raw), `${COOKIE_HEADER}\n${COOKIE_ROW_A}`);
});

test("buildCreateJobPayload trims query and cookie text", () => {
  const payload = buildCreateJobPayload("  The Beatles - Let It Be  ", `\n${COOKIE_ROW_A}\n`);
  assert.equal(payload.query, "The Beatles - Let It Be");
  assert.equal(payload.source_cookies_netscape, COOKIE_ROW_A);
});

test("buildCreateJobPayload honors snake_case and camelCase idempotency overrides", () => {
  const snake = buildCreateJobPayload("Song", "", { idempotency_key: "custom-a" });
  assert.equal(snake.idempotency_key, "custom-a");

  const camel = buildCreateJobPayload("Song", "", { idempotencyKey: "custom-b" });
  assert.equal(camel.idempotency_key, "custom-b");
});

test("buildCreateJobPayload supports no_parallel from camelCase and snake_case options", () => {
  const snake = buildCreateJobPayload("Song", "", { no_parallel: true });
  assert.equal(snake.no_parallel, true);

  const camel = buildCreateJobPayload("Song", "", { noParallel: true });
  assert.equal(camel.no_parallel, true);
});

test("buildCreateJobPayload parses yt_search_n from numeric strings and ignores invalid values", () => {
  const positive = buildCreateJobPayload("Song", "", { yt_search_n: "12" });
  assert.equal(positive.yt_search_n, 12);

  const nonPositive = buildCreateJobPayload("Song", "", { yt_search_n: "0" });
  assert.equal("yt_search_n" in nonPositive, false);

  const invalid = buildCreateJobPayload("Song", "", { yt_search_n: "NaN" });
  assert.equal("yt_search_n" in invalid, false);
});

test("buildCreateJobPayload keeps force/reset flags opt-in only", () => {
  const enabled = buildCreateJobPayload("Song", "", { force: true, reset: true });
  assert.equal(enabled.force, true);
  assert.equal(enabled.reset, true);

  const disabled = buildCreateJobPayload("Song", "", { force: false, reset: false });
  assert.equal("force" in disabled, false);
  assert.equal("reset" in disabled, false);
});

test("buildCreateJobPayload always emits randomized idempotency key if none provided", () => {
  const first = buildCreateJobPayload("Song A", "");
  const second = buildCreateJobPayload("Song A", "");
  assert.match(String(first.idempotency_key || ""), /^ios-\d+-[a-z0-9]{10}$/i);
  assert.match(String(second.idempotency_key || ""), /^ios-\d+-[a-z0-9]{10}$/i);
  assert.notEqual(first.idempotency_key, second.idempotency_key);
});
