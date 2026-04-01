export type SongEntry = {
  page: number;
  song: number;
};

export const findSongEntryOrNext: (
  entries: readonly SongEntry[],
  requestedSong: number,
) => SongEntry | null;

export const findSongAtOrBeforePage: (
  entries: readonly SongEntry[],
  activePage: number,
) => SongEntry | null;
