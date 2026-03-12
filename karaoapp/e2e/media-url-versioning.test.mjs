import test from "node:test";
import assert from "node:assert/strict";

import { appendMediaRevisionToken, buildJobMediaRevisionToken } from "../src/mediaUrlVersioning.js";

test("buildJobMediaRevisionToken prefers the latest meaningful job timestamp", () => {
  const token = buildJobMediaRevisionToken(
    {
      id: "job-123",
      render_finished_at: 12.345,
      finished_at: 11.111,
      last_updated_at: 10.0,
    },
    "video"
  );

  assert.equal(token, "job-123:12345:video");
});

test("appendMediaRevisionToken appends revision to relative media urls", () => {
  const url = appendMediaRevisionToken("/output/song.mp4", "job-123:12345:video");
  assert.equal(url, "/output/song.mp4?mixterious_rev=job-123%3A12345%3Avideo");
});

test("appendMediaRevisionToken preserves existing query and hash", () => {
  const url = appendMediaRevisionToken(
    "https://example.com/output/song.mp4?existing=1#frag",
    "job-456:777:audio"
  );

  assert.equal(
    url,
    "https://example.com/output/song.mp4?existing=1&mixterious_rev=job-456%3A777%3Aaudio#frag"
  );
});

test("appendMediaRevisionToken leaves empty or missing revisions alone", () => {
  assert.equal(appendMediaRevisionToken("/output/song.mp4", ""), "/output/song.mp4");
  assert.equal(appendMediaRevisionToken("", "abc"), null);
});

test("same media path gets a new revision when a retune job changes", () => {
  const firstToken = buildJobMediaRevisionToken(
    {
      id: "job-1",
      render_finished_at: 10.1,
    },
    "video"
  );
  const secondToken = buildJobMediaRevisionToken(
    {
      id: "job-2",
      render_finished_at: 11.2,
    },
    "video"
  );

  const firstUrl = appendMediaRevisionToken("/output/shared/song.mp4", firstToken);
  const secondUrl = appendMediaRevisionToken("/output/shared/song.mp4", secondToken);

  assert.notEqual(firstToken, secondToken);
  assert.notEqual(firstUrl, secondUrl);
  assert.match(String(secondUrl), /mixterious_rev=job-2%3A11200%3Avideo/);
});

test("appendMediaRevisionToken replaces an older revision value", () => {
  const url = appendMediaRevisionToken(
    "/output/song.mp4?mixterious_rev=job-1%3A10100%3Avideo&existing=1",
    "job-9:22222:video"
  );

  assert.equal(
    url,
    "/output/song.mp4?mixterious_rev=job-9%3A22222%3Avideo&existing=1"
  );
});
