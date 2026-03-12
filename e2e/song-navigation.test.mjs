import test from "node:test";
import assert from "node:assert/strict";

import { ALVERNIA_MANUAL_2_SONG_INDEX } from "../src/alverniaManual2SongIndex.js";
import { findSongAtOrBeforePage, findSongEntryOrNext } from "../src/songNavigation.js";

const SONGS_BY_PAGE = [...ALVERNIA_MANUAL_2_SONG_INDEX].sort(
  (left, right) => left.page - right.page || left.song - right.song,
);

test("hardcoded Alvernia index contains unique song numbers", () => {
  const uniqueSongs = new Set(ALVERNIA_MANUAL_2_SONG_INDEX.map((entry) => entry.song));

  assert.equal(ALVERNIA_MANUAL_2_SONG_INDEX.length, 312);
  assert.equal(uniqueSongs.size, ALVERNIA_MANUAL_2_SONG_INDEX.length);
});

test("findSongEntryOrNext resolves exact song matches from the hardcoded PDF", () => {
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 52), { song: 52, page: 52 });
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 249), { song: 249, page: 329 });
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 347), { song: 347, page: 346 });
});

test("findSongEntryOrNext jumps forward when a song number is missing in the PDF", () => {
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 1), { song: 2, page: 2 });
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 83), { song: 84, page: 84 });
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 366), { song: 367, page: 367 });
  assert.deepEqual(findSongEntryOrNext(ALVERNIA_MANUAL_2_SONG_INDEX, 999), { song: 368, page: 368 });
});

test("findSongAtOrBeforePage keeps the current song aligned with real PDF pages", () => {
  assert.equal(findSongAtOrBeforePage(SONGS_BY_PAGE, 1), null);
  assert.deepEqual(findSongAtOrBeforePage(SONGS_BY_PAGE, 2), { song: 2, page: 2 });
  assert.deepEqual(findSongAtOrBeforePage(SONGS_BY_PAGE, 149), { song: 148, page: 148 });
  assert.deepEqual(findSongAtOrBeforePage(SONGS_BY_PAGE, 329), { song: 249, page: 329 });
  assert.deepEqual(findSongAtOrBeforePage(SONGS_BY_PAGE, 346), { song: 347, page: 346 });
});
