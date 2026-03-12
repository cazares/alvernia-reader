import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appTsx = fs.readFileSync(path.join(__dirname, "..", "App.tsx"), "utf8");
const packageJson = JSON.parse(
  fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8")
);

test("mobile app no longer references backend rating sync endpoints", () => {
  assert.equal(appTsx.includes("/rating/state"), false);
  assert.equal(appTsx.includes("/rating/mark"), false);
  assert.equal(appTsx.includes("/rating/progress"), false);
});

test("mobile app no longer depends on expo store review plumbing", () => {
  assert.equal(appTsx.includes("expo-store-review"), false);
  assert.equal(appTsx.includes("StoreReview"), false);
  assert.equal("expo-store-review" in (packageJson.dependencies || {}), false);
});
