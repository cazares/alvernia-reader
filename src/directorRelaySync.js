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

// The credential sent as X-Director-Code so the relay authorizes this device's publishes. It's set
// to the code the director actually entered — a real director number — via setRelayPublishCode()
// when they become director. No code is hardcoded here; any valid transmitter code may publish.
let relayPublishCode = "";

// One-time bridge to the director's UI when the relay REJECTS this device's publish. A resolved
// fetch() is NOT proof of success: the relay authorizes every publish by X-Director-Code, so an
// unknown/retired code (HTTP 401) comes back !ok WITHOUT throwing. Left unchecked that failure is
// swallowed silently — the director's
// app shows nothing while EVERY signovivo.com follower freezes on the last page for the whole Mass
// (the relay is the only sync path to web phones; the Multipeer mesh doesn't reach them). The native
// shell registers a handler here that surfaces a visible warning in the WebView. Latched so a burst
// of page turns — each re-publishing and re-failing — can't spam it; the latch clears on the next
// successful publish (or a new code entry) so a genuine later failure can warn again.
let relayAuthErrorHandler = null;
export const setRelayAuthErrorHandler = (fn) => {
  relayAuthErrorHandler = typeof fn === "function" ? fn : null;
};
let authErrorNotified = false;

export const setRelayPublishCode = (code) => {
  relayPublishCode = String(code || "").replace(/[^0-9]/g, "");
  // A fresh code entry is a NEW attempt: re-arm the one-time auth warning so, if this code is
  // also rejected, the director is told again (and a corrected code that now works never warns).
  authErrorNotified = false;
};

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
    const res = await fetch(`${RELAY_BASE}/r/${RELAY_ROOM}/publish`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Director-Code": relayPublishCode,
      },
      body: JSON.stringify(payload),
      signal: controller ? controller.signal : undefined,
    });
    // A resolved fetch only means the round-trip completed — inspect the status. Auth rejections
    // (401 unauthorized code, or a 403) are exactly the "followers silently frozen" showstopper:
    // persistent, the director's fault to fix, and never self-healing. Warn ONCE. Other
    // non-ok statuses (5xx server blips, 429) are transient and already covered by the re-publish
    // loop, so we don't cry wolf on them.
    if (res && res.ok) {
      authErrorNotified = false; // recovered — re-arm so a future failure can warn again
    } else if (res && (res.status === 401 || res.status === 403) && !authErrorNotified) {
      authErrorNotified = true;
      try {
        if (relayAuthErrorHandler) relayAuthErrorHandler(res.status);
      } catch {
        // The warning bridge must never break the publish/coalesce loop.
      }
    }
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
    // Single-book app: always "standard". Kept in the payload for relay Snapshot
    // backward-compat ({v,page,totalPages,mode,bookId,seq,ts}) — old clients still
    // read this field. No longer sourced from a per-book context.
    bookId: "standard",
    seq: nextSeq(),
    ts: Math.floor(Date.now() / 1000),
  };
  if (inFlight) {
    pending = payload; // latest-wins
    return;
  }
  doPublish(payload);
};
