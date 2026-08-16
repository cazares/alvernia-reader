// sync-worker/src/logBuffer.js — the diagnostic ring buffer, sized for a ROOM.
//
// Every device in the building POSTs its Multipeer lifecycle here (role taken, peer found, invite,
// connect, disconnect, page received). `GET /log` reads it back, and it is the ONLY way to see both
// sides of a handshake — there is no internet in the church and no console on an iPad.
//
// Two defects made it blind at exactly the moment it mattered, a six-device diagnosis, and both
// were invisible from the Mac because a dropped breadcrumb looks identical to one that never
// happened:
//
//   1. THE RATE LIMIT BUCKETS BY IP, AND A FLEET SHARES ONE. Six iPads leave through the same NAT,
//      each flushing about once a second, so the fleet presented ~6 POST/s from a single address
//      against a 3/s sustained limit. More than half of the telemetry was refused — with ok:true,
//      no device-side retry, and no marker in the log. The limit now scales to how many devices are
//      in a room rather than how many one person could carry.
//
//   2. 600 SHARED ENTRIES IS ABOUT 100 SECONDS AT FLEET SCALE, and nearly all of it was keepalive.
//      Measured on one real 3-device window: 148 identical `found` rows for a peer already known,
//      and 271 `mesh:page-recv` rows that all said "still on the same page". Those evict the single
//      `session:notConnected` that explains why the choir stopped following.
//
// So consecutive identical events from one device collapse into a single row carrying a count and a
// first/last timestamp. Nothing diagnostic is lost: those rows were only ever evidence that a
// heartbeat was arriving, and `n` across (t0 → t) states that more precisely than 271 copies did.
//
// Sizing. The entry ceiling keeps the buffer readable; the BYTE ceiling is the load-bearing one,
// because a Durable Object value is hard-capped at 128 KiB and an oversized write fails outright —
// which would discard the whole log instead of its oldest rows. That failure mode is why the byte
// trim exists at all, and why it is a loop rather than a single slice.

export const LOG_RATE_BURST = 100;
export const LOG_RATE_PER_SEC = 20;
export const LOG_MAX_ENTRIES = 4000;
export const LOG_MAX_BYTES = 100 * 1024;
/** Cap on how many entries one POST may contribute, so a single batch cannot dominate the buffer. */
export const LOG_MAX_BATCH = 200;

/** The subject keys that distinguish two otherwise-identical rows. */
const SUBJECT_KEYS = ["page", "peer", "to", "from", "target", "status", "dup", "code"];

/**
 * Identity of a row for run-length collapsing: same device, same event, same subject.
 *
 * Returns "" for anything without BOTH a device and an event, and "" never matches "" — an
 * unrecognised shape is always kept verbatim rather than folded into its neighbour. A future event
 * that adds a new subject key simply stops collapsing, which fails toward more detail, never toward
 * silently merging two distinct events.
 */
export const logSignature = (entry) => {
  if (!entry || typeof entry !== "object") return "";
  const { dev, event } = entry;
  if (typeof dev !== "string" || typeof event !== "string") return "";
  return dev + " " + event + " " + SUBJECT_KEYS.map((k) => (entry[k] === undefined ? "" : String(entry[k]))).join("|");
};

/**
 * Fold a batch of incoming entries onto the existing buffer, then apply both ceilings.
 *
 * @param existing  the stored buffer (oldest first)
 * @param incoming  this POST's entries, already parsed
 * @param rx        server receive time, epoch SECONDS — stamped so a device with a wrong clock is
 *                  still orderable against the others
 * @returns the new buffer to store
 */
export const foldLogEntries = (existing, incoming, rx) => {
  const out = Array.isArray(existing) ? existing.slice() : [];
  const batch = (Array.isArray(incoming) ? incoming : []).slice(0, LOG_MAX_BATCH);

  // CONSECUTIVE PER DEVICE, not consecutive globally. Real traffic INTERLEAVES - five followers
  // beating at 1 Hz arrive as ipad-1, ipad-2, ipad-3, ipad-4, ipad-5, ipad-1, ... so no two
  // adjacent rows ever match and a naive "fold into the previous row" folds nothing at all. That
  // version passed every small unit test here and then collapsed ZERO of a simulated ten-minute
  // six-device run - the only scale that matters. So each device folds into ITS OWN last row:
  // ipad-1 heartbeats accumulate on ipad-1 row, and a count is never attributed to the wrong iPad.
  // Any other event from that device in between ends the run, because that row becomes its last
  // one and the signature no longer matches.
  const lastIdxByDev = new Map();
  for (let i = out.length - 1; i >= 0; i -= 1) {
    const d = out[i] && out[i].dev;
    if (typeof d === "string" && !lastIdxByDev.has(d)) lastIdxByDev.set(d, i);
  }

  for (const raw of batch) {
    const entry = raw && typeof raw === "object" ? { rx, ...raw } : { rx, v: raw };
    const sig = logSignature(entry);
    const dev = typeof entry.dev === "string" ? entry.dev : null;
    const idx = dev === null ? undefined : lastIdxByDev.get(dev);
    const prev = idx === undefined ? null : out[idx];
    if (prev && sig !== "" && logSignature(prev) === sig) {
      // t0 is pinned on the FIRST fold only, so the pair (t0, t) always spans every occurrence.
      if (prev.t0 === undefined) prev.t0 = prev.t;
      prev.t = entry.t;
      prev.rx = entry.rx;
      prev.n = (typeof prev.n === "number" ? prev.n : 1) + 1;
      continue;
    }
    out.push(entry);
    if (dev !== null) lastIdxByDev.set(dev, out.length - 1);
  }

  let capped = out.slice(-LOG_MAX_ENTRIES);
  // Drop ~10% of the oldest per pass rather than one row at a time: JSON.stringify over a few
  // thousand rows is the expensive part, and this bounds it to a handful of passes.
  while (capped.length > 1 && JSON.stringify(capped).length > LOG_MAX_BYTES) {
    capped = capped.slice(Math.max(1, Math.floor(capped.length * 0.1)));
  }
  return capped;
};
