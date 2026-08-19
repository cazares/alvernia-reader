/**
 * svRelayRoom — resolves which relay room this device joins.
 *
 * Release Safety System (M1). A device can point at an ISOLATED staging room so a
 * canary build is proven without touching the live-Mass room. Each room is a
 * separate Durable Object instance on the same worker, so a broken canary
 * physically cannot reach a live follower.
 *
 *   (no param)     -> alvernia-main     (production / Mass — the default)
 *   ?env=staging   -> alvernia-staging  (canary channel; isolated DO)
 *
 * DEFENSIVE BY CONTRACT: this runs before the app boots (loaded as an external
 * <script defer> ahead of app.js). It must NEVER throw and must ALWAYS return a
 * known room name — a wrong room is recoverable, a thrown exception at boot is a
 * white screen. Any error / unknown input collapses to the production room. If
 * this file fails to load at all, app.js's own guard still falls back to
 * alvernia-main. (Native staging entry is a separate M7 concern; the native
 * transmitter still targets alvernia-main.)
 *
 * UMD: usable as a browser global (window.svRelayRoom) AND require()-able in node
 * for unit tests. Kept dependency-free and ES5 so it runs anywhere, early.
 */
(function (root) {
  var PRODUCTION_ROOM = "alvernia-main";
  var STAGING_ROOM = "alvernia-staging";
  var KNOWN = { "alvernia-main": true, "alvernia-staging": true };

  function resolveRelayRoom(search) {
    try {
      var params = new URLSearchParams(search || "");
      var room = params.get("env") === "staging" ? STAGING_ROOM : PRODUCTION_ROOM;
      return KNOWN[room] === true ? room : PRODUCTION_ROOM;
    } catch (_) {
      return PRODUCTION_ROOM;
    }
  }

  var api = {
    resolveRelayRoom: resolveRelayRoom,
    PRODUCTION_ROOM: PRODUCTION_ROOM,
    STAGING_ROOM: STAGING_ROOM,
  };
  try {
    root.svRelayRoom = api;
  } catch (_) {
    /* setting a global should never break boot */
  }
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
