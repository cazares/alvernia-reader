import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const SOURCE = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

test("onboarding modal exists with required Spanish strings", () => {
  assert.match(SOURCE, /Detectando tu iPad/);
  assert.match(SOURCE, /Este build usará modo principal para los iPads autorizados y modo himnarios para el resto\./);
  assert.match(SOURCE, /Dispositivo detectado:/);
  assert.match(SOURCE, /Continuar/);
});

test("allowlist uses explicit device names and no city gate", () => {
  assert.match(SOURCE, /const DEVICE_ALLOWLIST = new Set/);
  assert.match(SOURCE, /Platform\.OS === "ios" && Platform\.isPad && DEVICE_ALLOWLIST\.has\(normalizedName\)/);
  assert.match(SOURCE, /Brau 3 🎶 😎/);
  assert.match(SOURCE, /Brau MASTER/);
  assert.match(SOURCE, /Ipad 2 Caty y Raul Leal/);
  assert.match(SOURCE, /Ipad 2 Rita y Alfredo Varela/);
  assert.match(SOURCE, /iPad de Adrian/);
  assert.match(SOURCE, /iPad de Braulio/);
  assert.match(SOURCE, /mPad/);
  assert.doesNotMatch(SOURCE, /isDelRioMatch/);
  assert.doesNotMatch(SOURCE, /setOnboardingState/);
  assert.doesNotMatch(SOURCE, /setOnboardingCity/);
});

test("non-standard build removes the IR A LIBRO section entirely", () => {
  assert.doesNotMatch(SOURCE, /IR A LIBRO/);
  assert.doesNotMatch(SOURCE, /styles\\.bookList/);
  assert.doesNotMatch(SOURCE, /switchBook\\(id\\)/);
});

test("reset code 744668486 is intercepted before normal navigation", () => {
  assert.match(SOURCE, /trimmed === \"744668486\"/);
  assert.match(SOURCE, /performColdBootReset/);
});

test("city onboarding storage failures stay inside the submit handler", () => {
  assert.match(SOURCE, /await AsyncStorage\.multiSet\(\[/);
  assert.match(SOURCE, /catch \{\n\s+Alert\.alert\(/);
  assert.match(SOURCE, /onboardingSubmittingRef\.current = false/);
});
