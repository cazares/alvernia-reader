/**
 * svRenderPace — should renderPage attempt this page, or is it still failing?
 *
 * THE FAILURE THIS EXISTS FOR. A follower whose render fails posts `render-failed` to the native
 * shell, which resets its page reference to an impossible sentinel so the director's very next
 * heartbeat re-drives this device. That recovery is deliberate and load-bearing: without it the
 * heartbeat's own de-dupe ("you are already on page P") would drop every re-send and strand the
 * follower on the error overlay forever, while the director faithfully re-sent the right page.
 *
 * What it lacked was a floor. When a page genuinely cannot render — not cached with no network,
 * which describes every follower in a church, or a cache slot poisoned with an HTML error body —
 * the sentinel and the 1 Hz heartbeat close into a loop: fail, reset, re-drive, fail. Each pass
 * built two off-DOM Images with their own timeout timers, scheduled a loading indicator, and posted
 * back across the bridge. Once a second, for as long as the director stayed on that song, on an
 * eight-year-old iPad. That is the several-minute follower death, and renderPage's same-page early
 * return cannot stop it because that guard deliberately falls through for a failed load.
 *
 * So retries are PACED, never stopped. The distinction matters: stopping would turn a transient
 * failure into a permanent one, which is the bug the sentinel was added to fix in the first place.
 *
 * PURE: no DOM, no timers, no side effects — the caller passes `now`. Extracted here for the same
 * reason as svSyncDecision: logic that decides whether a follower renders at all should be
 * executable by a test, not merely quoted by one. An earlier version of its test re-implemented
 * this rule instead of importing it, and a mutation that deleted the shipped guard outright passed
 * every assertion.
 */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  else root.svRenderPace = api;
})(typeof self !== "undefined" ? self : this, function () {
  /** Default pace. Long enough to blunt a 1 Hz storm, short enough that a page which becomes
   *  loadable is back on screen while the same song is still being sung. */
  const RENDER_RETRY_COOLDOWN_MS = 5000;

  /**
   * @param failure  {{page:number, at:number}|null} the most recent failed render, or null
   * @param page     the page renderPage was just asked for
   * @param now      epoch ms
   * @param cooldown optional override, for tests
   * @returns true if this attempt should be skipped (the page is still in its pacing window)
   */
  const shouldPaceRender = (failure, page, now, cooldown) => {
    if (!failure) return false;
    if (failure.page !== page) return false; // a DIFFERENT page from the director is never delayed
    const window = typeof cooldown === "number" ? cooldown : RENDER_RETRY_COOLDOWN_MS;
    const elapsed = now - failure.at;
    // A clock that jumps backwards must not pace forever: a negative elapsed means the memo is in
    // the future, which is not evidence of anything, so allow the attempt.
    if (!(elapsed >= 0)) return false;
    return elapsed < window;
  };

  return { RENDER_RETRY_COOLDOWN_MS, shouldPaceRender };
});
