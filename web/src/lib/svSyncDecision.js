/**
 * svSyncDecision — the pure "what should a follower do with this relay snapshot?"
 * decision, extracted from applyRelaySnapshot so it can be unit-tested in node.
 *
 * P2 sync robustness. The old inline logic ran the seq de-dup guard BEFORE the
 * freshness check, so a director who had gone silent kept re-delivering the SAME
 * seq on every WebSocket heartbeat reply — that duplicate was dropped by the seq
 * guard and never reached the staleness check, freezing the green "en vivo" pill on
 * a dead director forever (observed at Wednesday practice). This module evaluates
 * FRESHNESS FIRST so a stale director is always demoted, regardless of seq
 * (P2-SEQ), and folds in a server-calibrated clock offset so a follower whose
 * device clock is skewed doesn't judge every fresh snapshot stale (P2-CLOCKSKEW).
 *
 * PURE: no DOM, no timers, no side effects. It reads a snapshot + the follower's
 * current sync state and returns a decision object; the caller (applyRelaySnapshot)
 * executes the render / pill / reveal side effects. That keeps the load-bearing
 * ordering logic testable and identical across the browser and node.
 *
 * Decision.action is one of:
 *   "reject"   — malformed snapshot (non-finite page); do nothing.
 *   "demote"   — no live director (never published, or stale beyond maxAgeS):
 *                behave like a normal songbook. Caller: hasDirector=false,
 *                browsing=false, hide the go-live bar, re-render the pill.
 *   "live-dup" — director is live but this seq is a duplicate/older (they haven't
 *                moved): nothing new to render, but they ARE still live. Caller:
 *                keep hasDirector=true, re-render the pill.
 *   "browsing" — director is live and moved, but the user is manually browsing:
 *                track livePage for "volver a en vivo" but DON'T yank their page.
 *   "follow"   — director is live and moved: snap the follower to snap.page.
 *
 * The returned object also carries the next lastSeq/hasDirector/browsing/livePage
 * and boolean side-effect flags (renderPill, reveal, hideGoLiveBar, renderPage) so
 * the caller stays a thin, mechanical executor with no ordering decisions of its own.
 *
 * UMD: browser global (window.svSyncDecision) AND require()-able in node. ES5,
 * dependency-free, and must NEVER throw — a thrown exception here would abort a
 * relay message handler mid-Mass. Any surprise collapses to a safe "reject".
 */
(function (root) {
  function isFiniteNum(n) {
    return typeof n === "number" && isFinite(n);
  }

  /**
   * @param {object} snap  the relay snapshot ({ page, seq, ts, ... }).
   * @param {object} ctx   follower state + knobs:
   *   { lastSeq, hasDirector, browsing, currentPage, force,
   *     nowMs, clockOffsetMs, maxAgeS }
   * @returns {object} decision (see module docstring).
   */
  function decideRelaySnapshot(snap, ctx) {
    // Defaults mirror "no change" so a caller can blindly apply every field.
    var out = {
      action: "reject",
      hasDirector: ctx ? ctx.hasDirector : false,
      lastSeq: ctx ? ctx.lastSeq : -1,
      browsing: ctx ? ctx.browsing : false,
      livePage: undefined, // set only when the director's live page advances
      renderPage: undefined, // page to render (follow branch only)
      renderPill: false,
      reveal: false,
      hideGoLiveBar: false,
    };
    try {
      if (!ctx) return out;
      if (!snap || !isFiniteNum(snap.page)) {
        out.action = "reject";
        return out;
      }

      var maxAgeS = isFiniteNum(ctx.maxAgeS) ? ctx.maxAgeS : 90;
      var offsetMs = isFiniteNum(ctx.clockOffsetMs) ? ctx.clockOffsetMs : 0;
      var nowMs = isFiniteNum(ctx.nowMs) ? ctx.nowMs : 0;
      // Server-corrected "now" in seconds. offsetMs = (serverClock - deviceClock),
      // calibrated from the /state response Date header, so a device whose clock is
      // fast/slow still measures snapshot age against the SERVER's clock.
      var nowServerS = (nowMs + offsetMs) / 1000;

      var hasPublished = isFiniteNum(snap.seq) && snap.seq > 0;
      // A director counts as live only if it has published AND its last update is
      // within the freshness window. snap.ts is the server publish time (epoch s);
      // if absent (older wire shape), treat as fresh so we never regress to worse.
      var fresh =
        hasPublished &&
        (!isFiniteNum(snap.ts) || nowServerS - snap.ts <= maxAgeS);

      // ---- P2-SEQ: FRESHNESS FIRST, before any seq de-dup ------------------
      if (!fresh) {
        out.action = "demote";
        out.hasDirector = false;
        out.browsing = false;
        // F4: reset the seq de-dup floor when the director is demoted (stale / gone). A director
        // that RESTARTS its app re-publishes from a LOW seq; once the old snapshot ages out of the
        // freshness window, that low seq must be followable. Without this reset, `lowSeq <= lastSeq`
        // would classify the restarted director as a duplicate ("live-dup") forever on a healthy
        // socket, stranding every follower until a manual ⟳ (which is the only other lastSeq reset).
        out.lastSeq = -1;
        out.hideGoLiveBar = true;
        out.renderPill = true;
        return out;
      }

      // Director is fresh-live. Confirm liveness regardless of what happens next.
      out.hasDirector = true;

      // De-dup ongoing pushes by seq: a duplicate or older seq means the director
      // hasn't moved — nothing to re-render — but they're still live (confirmed
      // above). A FORCED resync (boot poll, reconnect, foreground, /state fallback)
      // re-applies even a duplicate seq so a director sitting still still re-homes
      // the follower onto their page.
      if (!ctx.force && snap.seq <= ctx.lastSeq) {
        out.action = "live-dup";
        out.renderPill = true;
        return out;
      }
      out.lastSeq = Math.max(ctx.lastSeq, snap.seq);
      out.livePage = snap.page;

      // The user is intentionally browsing (tapped a song → jumped). Track the
      // director's page for "volver a en vivo" but do NOT yank them off their page.
      if (ctx.browsing) {
        out.action = "browsing";
        out.renderPill = true;
        out.reveal = true;
        return out;
      }

      // Normal follower: snap to the director's page.
      out.action = "follow";
      out.renderPage = ctx.currentPage !== snap.page ? snap.page : undefined;
      out.renderPill = true;
      out.reveal = true;
      return out;
    } catch (_) {
      // Never throw out of a relay handler. On any surprise, do nothing.
      out.action = "reject";
      return out;
    }
  }

  /**
   * Calibrate the client<->server clock offset from a /state response's Date header.
   * Returns (serverClockMs - deviceClockMs), or the previous offset if the header is
   * missing/unparseable. Kept here so it is unit-tested alongside the freshness math.
   * @param {string|null} dateHeader  the HTTP Date header value.
   * @param {number} deviceNowMs      Date.now() captured at response receipt.
   * @param {number} prevOffsetMs     the offset to keep if the header is unusable.
   */
  function clockOffsetFromDateHeader(dateHeader, deviceNowMs, prevOffsetMs) {
    try {
      var prev = isFiniteNum(prevOffsetMs) ? prevOffsetMs : 0;
      if (!dateHeader || !isFiniteNum(deviceNowMs)) return prev;
      var serverMs = Date.parse(dateHeader);
      if (!isFiniteNum(serverMs)) return prev;
      return serverMs - deviceNowMs;
    } catch (_) {
      return isFiniteNum(prevOffsetMs) ? prevOffsetMs : 0;
    }
  }

  /**
   * Calibrate the client<->server clock offset from the /state body's `now` field
   * (server epoch SECONDS). This is the reliable cross-origin channel — unlike the
   * HTTP Date header, a JSON body field survives CORS. Returns (server - device) in ms,
   * or the previous offset if `now` is absent/invalid (old worker → no correction, which
   * is exactly the pre-P2 behavior). @param {number} serverNowSec  body.now (epoch s).
   */
  function clockOffsetFromServerNow(serverNowSec, deviceNowMs, prevOffsetMs) {
    try {
      var prev = isFiniteNum(prevOffsetMs) ? prevOffsetMs : 0;
      if (!isFiniteNum(serverNowSec) || serverNowSec <= 0 || !isFiniteNum(deviceNowMs)) return prev;
      return serverNowSec * 1000 - deviceNowMs;
    } catch (_) {
      return isFiniteNum(prevOffsetMs) ? prevOffsetMs : 0;
    }
  }

  var api = {
    decideRelaySnapshot: decideRelaySnapshot,
    clockOffsetFromDateHeader: clockOffsetFromDateHeader,
    clockOffsetFromServerNow: clockOffsetFromServerNow,
  };
  try {
    root.svSyncDecision = api;
  } catch (_) {
    /* setting a global should never break boot */
  }
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
