import test from "node:test";
import assert from "node:assert/strict";

import { buildPdfPageUrl, clampPdfPage, normalizePdfUrl } from "../src/pdfReaderUrl.js";

test("clampPdfPage keeps values in valid bounds", () => {
  assert.equal(clampPdfPage("7"), 7);
  assert.equal(clampPdfPage("0"), 1);
  assert.equal(clampPdfPage("-9"), 1);
  assert.equal(clampPdfPage("15000"), 10000);
  assert.equal(clampPdfPage("not-a-number"), 1);
});

test("normalizePdfUrl strips existing hash fragments", () => {
  assert.equal(
    normalizePdfUrl("https://example.com/file.pdf#page=9"),
    "https://example.com/file.pdf"
  );
});

test("buildPdfPageUrl appends a safe #page anchor", () => {
  assert.equal(
    buildPdfPageUrl({ pdfUrl: "https://example.com/file.pdf", page: "4" }),
    "https://example.com/file.pdf#page=4"
  );
  assert.equal(
    buildPdfPageUrl({ pdfUrl: "https://example.com/file.pdf#page=2", page: "0" }),
    "https://example.com/file.pdf#page=1"
  );
});
