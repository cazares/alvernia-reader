import test from "node:test";
import assert from "node:assert/strict";

import { buildCreateJobPayload, normalizeNetscapeCookies } from "../src/createJobPayload.js";

test("normalizeNetscapeCookies trims and normalizes line endings", () => {
  const raw = " \r\n# Netscape HTTP Cookie File\r\n.source.com\tTRUE\t/\tTRUE\t0\tSID\tabc123\r\n";
  const normalized = normalizeNetscapeCookies(raw);
  assert.equal(
    normalized,
    "# Netscape HTTP Cookie File\n.source.com\tTRUE\t/\tTRUE\t0\tSID\tabc123"
  );
});

test("buildCreateJobPayload includes cookies only when provided", () => {
  const base = buildCreateJobPayload("Artist - Song", "");
  assert.equal(base.query, "Artist - Song");
  assert.match(String(base.idempotency_key || ""), /^ios-\d+-[a-z0-9]{10}$/i);

  const withCookies = buildCreateJobPayload("Artist - Song", ".source.com\tTRUE\t/\tTRUE\t0\tSID\txyz");
  assert.equal(withCookies.query, "Artist - Song");
  assert.match(String(withCookies.idempotency_key || ""), /^ios-\d+-[a-z0-9]{10}$/i);
  assert.equal(withCookies.source_cookies_netscape, ".source.com\tTRUE\t/\tTRUE\t0\tSID\txyz");
});

test("buildCreateJobPayload includes retry options when provided", () => {
  const payload = buildCreateJobPayload("The Beatles - Let It Be", "", {
    force: true,
    reset: true,
    no_parallel: true,
    yt_search_n: 12,
  });
  assert.equal(payload.query, "The Beatles - Let It Be");
  assert.match(String(payload.idempotency_key || ""), /^ios-\d+-[a-z0-9]{10}$/i);
  assert.equal(payload.force, true);
  assert.equal(payload.reset, true);
  assert.equal(payload.no_parallel, true);
  assert.equal(payload.yt_search_n, 12);
});
