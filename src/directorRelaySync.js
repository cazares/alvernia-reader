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

// Hard ceiling on a single publish. On parish wifi that associates but doesn't
// route (captive portal / black-holed TCP) a bare fetch() never settles, so
// without this the request hangs forever, inFlight stays true, and every later
// page turn (and the 12s heartbeat) only overwrites `pending` and returns —
// freezing the whole web congregation on the last sent page for the rest of
// Mass, with no self-heal even after connectivity returns. Aborting guarantees
// the finally below always runs and the coalescer drains.
const PUBLISH_TIMEOUT_MS = 7000;

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
  // AbortController so a hung/half-open socket can't wedge the coalescer forever.
  // Guard typeof in case an exotic runtime lacks it — degrade to a plain fetch
  // rather than throwing on a missing global.
  const controller =
    typeof AbortController !== "undefined" ? new AbortController() : null;
  const timer = controller
    ? setTimeout(() => controller.abort(), PUBLISH_TIMEOUT_MS)
    : null;
  try {
    await fetch(`${RELAY_BASE}/r/${RELAY_ROOM}/publish`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Director-Code": TRANSMITTER_ACCESS_CODE,
      },
      body: JSON.stringify(payload),
      signal: controller ? controller.signal : undefined,
    });
  } catch {
    // Network flaps and aborts are expected (weak signal / timeout); the next
    // page change — or the heartbeat — re-publishes once inFlight clears below.
  } finally {
    if (timer) clearTimeout(timer);
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
