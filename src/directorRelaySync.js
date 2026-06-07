// SignoVivo — relay transmitter.
//
// Publishes the director's current page to the Cloudflare relay so signovivo.com
// followers (the congregation on the web) stay in sync. The "transmitter" is the
// internet-connected device that runs this — the director by default, or any
// device that enters TRANSMITTER_ACCESS_CODE. Page numbers only; no PII.
//
// Fire-and-forget with latest-wins coalescing so a burst of page turns on a weak
// link never queues a backlog — only the newest page is sent. Mirrors the relay's
// proven payload shape ({v,page,totalPages,mode,bookId,seq,ts}).

const RELAY_BASE = "https://signovivo-sync.4j4982y8jp.workers.dev";
const RELAY_ROOM = "alvernia-main";
const PROTOCOL_VERSION = 1;

// Memorable transmitter access code (same style as the director codes already in
// the app). Sent as X-Director-Code; the relay authorizes publishes with it.
export const TRANSMITTER_ACCESS_CODE = "12345678840";

let seqCounter = 0;
let inFlight = false;
let pending = null; // newest payload queued while a publish is in flight

const nextSeq = () => {
  // Monotonic and roughly time-based, so it stays ahead even across app restarts.
  seqCounter = Math.max(seqCounter + 1, Date.now());
  return seqCounter;
};

const doPublish = async (payload) => {
  inFlight = true;
  try {
    await fetch(`${RELAY_BASE}/r/${RELAY_ROOM}/publish`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Director-Code": TRANSMITTER_ACCESS_CODE,
      },
      body: JSON.stringify(payload),
    });
  } catch {
    // Network flaps are expected (weak signal); the next page change re-publishes.
  } finally {
    inFlight = false;
    if (pending) {
      const next = pending;
      pending = null;
      doPublish(next);
    }
  }
};

export const publishPageToRelay = (page, totalPages = 0, context = {}) => {
  const payload = {
    v: PROTOCOL_VERSION,
    page: Math.max(1, Number(page) || 1),
    totalPages: Math.max(0, Number(totalPages) || 0),
    mode: String(context.mode || ""),
    bookId: String(context.bookId || ""),
    seq: nextSeq(),
    ts: Math.floor(Date.now() / 1000),
  };
  if (inFlight) {
    pending = payload; // latest-wins
    return;
  }
  doPublish(payload);
};
