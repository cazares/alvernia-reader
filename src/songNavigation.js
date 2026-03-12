export const findSongEntryOrNext = (entries = [], requestedSong) => {
  if (!entries.length) return null;

  let low = 0;
  let high = entries.length - 1;
  let candidateIndex = entries.length - 1;

  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const current = entries[middle]?.song ?? Number.POSITIVE_INFINITY;

    if (current >= requestedSong) {
      candidateIndex = middle;
      high = middle - 1;
    } else {
      low = middle + 1;
    }
  }

  return entries[candidateIndex] ?? null;
};

export const findSongAtOrBeforePage = (entries = [], activePage = 1) => {
  if (!entries.length) return null;

  let low = 0;
  let high = entries.length - 1;
  let candidate = null;

  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const current = entries[middle]?.page ?? Number.POSITIVE_INFINITY;

    if (current <= activePage) {
      candidate = entries[middle] ?? null;
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }

  return candidate;
};
