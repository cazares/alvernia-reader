import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const APP_ROOT = path.resolve(import.meta.dirname, "..");

test("nearby director sync bridge stays native-only", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "src", "nearbyDirectorSync.js"), "utf8");

  assert.match(source, /NativeModules\.DirectorSyncModule/);
  assert.match(source, /Platform\.OS === "ios"/);
  assert.match(source, /startDirector/);
  assert.match(source, /startFollower/);
  assert.doesNotMatch(source, /fetch\(/);
  assert.doesNotMatch(source, /https?:\/\//);
});

test("app source no longer includes remote or songbook sync paths", () => {
  const source = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

  assert.doesNotMatch(source, /resolveDirectorSyncEndpoint/);
  assert.doesNotMatch(source, /publishDirectorSyncState/);
  assert.doesNotMatch(source, /readDirectorSyncState/);
  assert.doesNotMatch(source, /releaseDirectorSyncState/);
  assert.doesNotMatch(source, /ALVERNIA_MANUAL_2_SONG_INDEX/);
  assert.doesNotMatch(source, /findSongEntryOrNext/);
});
