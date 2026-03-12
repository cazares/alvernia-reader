import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const infoPlistPath = path.resolve(
  import.meta.dirname,
  "..",
  "ios",
  "Mixterious",
  "Info.plist"
);

test("iOS Info.plist allows local dev HTTP loads for simulator/native smoke testing", () => {
  const source = fs.readFileSync(infoPlistPath, "utf8");

  assert.match(source, /<key>NSAppTransportSecurity<\/key>/);
  assert.match(source, /<key>NSAllowsArbitraryLoads<\/key>\s*<true\/>/);
  assert.match(source, /<key>NSAllowsLocalNetworking<\/key>\s*<true\/>/);
});
