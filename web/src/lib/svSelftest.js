/**
 * svSelftest — the ?selftest GREEN/RED readiness card (Release Safety System, M1).
 *
 * The canary-walk tool: on `signovivo.com?selftest` (or `?env=staging&selftest`) the
 * operator gets a one-glance card proving the device can actually work — page 1
 * really loads, the page count is sane, the relay answers on the CURRENT room, and
 * (on native) the bridge is up. Read-only by design: it only loads an image and GETs
 * the relay /state — it can never mutate anything or flip anyone's page.
 *
 * DEFENSIVE BY CONTRACT: only ever invoked when ?selftest is present (zero footprint
 * otherwise); every entry point swallows its own errors (a broken selftest must never
 * break the reader it is testing). All dependencies are injected so the check logic
 * unit-tests in node with no DOM.
 *
 * UMD: window.svSelftest in the browser, module.exports in node.
 */
(function (root) {
  var DEFAULT_TIMEOUT_MS = 6000;

  function withTimeout(promise, ms, fallback) {
    return new Promise(function (resolve) {
      var settled = false;
      var timer = setTimeout(function () {
        if (!settled) { settled = true; resolve(fallback); }
      }, ms);
      Promise.resolve(promise).then(
        function (v) { if (!settled) { settled = true; clearTimeout(timer); resolve(v); } },
        function () { if (!settled) { settled = true; clearTimeout(timer); resolve(fallback); } }
      );
    });
  }

  /**
   * Runs every readiness check. Never rejects; each check resolves ok:true/false.
   * deps: { loadImage(url)->Promise<bool>, fetchImpl(url)->Promise<Response-like>,
   *         pageUrl, totalPages, relayBase, relayRoom, buildNumber, cacheVersion,
   *         isNativeFileMode, bridgeAvailable, timeoutMs? }
   */
  function computeChecks(deps) {
    var d = deps || {};
    var timeoutMs = typeof d.timeoutMs === "number" ? d.timeoutMs : DEFAULT_TIMEOUT_MS;
    var checks = [];

    // 1. Build identity (informational; red only if wholly absent)
    var buildLabel = String(d.buildNumber || "");
    if (buildLabel && buildLabel.charAt(0) === "_") buildLabel = "dev"; // unreplaced token in a source run
    checks.push(Promise.resolve({
      id: "version",
      label: "Versión",
      ok: buildLabel.length > 0,
      detail: "v" + (buildLabel || "?") + (d.cacheVersion && String(d.cacheVersion).charAt(0) !== "_" ? " · " + d.cacheVersion : ""),
      applicable: true,
    }));

    // 2. Page 1 actually loads via the SAME URL path the reader uses
    var loadImage = typeof d.loadImage === "function" ? d.loadImage : function () { return Promise.resolve(false); };
    checks.push(withTimeout(
      Promise.resolve().then(function () { return loadImage(String(d.pageUrl || "")); }),
      timeoutMs, false
    ).then(function (ok) {
      return { id: "pagina1", label: "Página 1 carga", ok: ok === true, detail: ok === true ? "imagen OK" : "no cargó — revisa el bundle/las páginas", applicable: true };
    }));

    // 3. Page count is a sane positive integer
    var total = d.totalPages;
    checks.push(Promise.resolve({
      id: "paginas",
      label: "Número de páginas",
      ok: typeof total === "number" && isFinite(total) && Math.floor(total) === total && total > 1,
      detail: String(total) + " páginas",
      applicable: true,
    }));

    // 4. Relay answers on the CURRENT room (read-only GET /state; needs internet).
    //    Shows the room LOUDLY so the operator always knows if they are testing the
    //    practice/staging room or the LIVE Mass room.
    var fetchImpl = typeof d.fetchImpl === "function" ? d.fetchImpl : null;
    var room = String(d.relayRoom || "?");
    var relayUrl = String(d.relayBase || "") + "/r/" + encodeURIComponent(room) + "/state";
    checks.push(withTimeout(
      fetchImpl
        ? Promise.resolve().then(function () { return fetchImpl(relayUrl); }).then(function (res) { return !!(res && res.ok); })
        : Promise.resolve(false),
      timeoutMs, false
    ).then(function (ok) {
      return { id: "relay", label: "Relay (sala: " + room + ")", ok: ok === true, detail: ok === true ? "conectado" : "sin respuesta — ¿hay internet?", applicable: true };
    }));

    // 5. Native bridge (applicable only inside the iPad shell)
    var isNative = d.isNativeFileMode === true;
    checks.push(Promise.resolve({
      id: "puente",
      label: "Puente nativo",
      ok: isNative ? d.bridgeAvailable === true : true,
      detail: isNative ? (d.bridgeAvailable === true ? "conectado" : "sin conexión con la app") : "—",
      applicable: isNative,
    }));

    return Promise.all(checks).then(function (results) {
      var allOk = true;
      for (var i = 0; i < results.length; i++) {
        if (results[i].applicable && results[i].ok !== true) allOk = false;
      }
      return { checks: results, allOk: allOk };
    });
  }

  /** Renders the card as a dismissible overlay. DOM built with createElement +
   *  textContent only (no innerHTML with dynamic strings). Never throws. */
  function renderCard(result, doc) {
    try {
      var existing = doc.getElementById("sv-selftest-card");
      if (existing && existing.parentNode) existing.parentNode.removeChild(existing);

      var wrap = doc.createElement("div");
      wrap.id = "sv-selftest-card";
      wrap.setAttribute("role", "dialog");
      wrap.style.cssText =
        "position:fixed;inset:0;z-index:2147483000;display:flex;align-items:center;justify-content:center;" +
        "background:rgba(10,10,18,0.72);-webkit-backdrop-filter:blur(2px);backdrop-filter:blur(2px);padding:18px;";

      var card = doc.createElement("div");
      card.style.cssText =
        "max-width:420px;width:100%;border-radius:18px;background:#141428;color:#fff;" +
        "font:16px/1.45 -apple-system,system-ui,sans-serif;box-shadow:0 18px 50px rgba(0,0,0,.5);overflow:hidden;";

      var banner = doc.createElement("div");
      banner.style.cssText =
        "padding:20px 22px;font-size:26px;font-weight:800;letter-spacing:.5px;text-align:center;color:#fff;background:" +
        (result.allOk ? "#1a7f37" : "#b42318") + ";";
      banner.textContent = result.allOk ? "✓ TODO BIEN" : "✗ REVISAR";
      card.appendChild(banner);

      var list = doc.createElement("div");
      list.style.cssText = "padding:14px 20px 6px;";
      for (var i = 0; i < result.checks.length; i++) {
        var c = result.checks[i];
        var row = doc.createElement("div");
        row.style.cssText = "display:flex;justify-content:space-between;gap:12px;padding:7px 0;border-bottom:1px solid rgba(255,255,255,.08);";
        var left = doc.createElement("span");
        left.textContent = (c.applicable ? (c.ok ? "✓ " : "✗ ") : "— ") + c.label;
        left.style.cssText = "font-weight:600;color:" + (c.applicable ? (c.ok ? "#7ee2a0" : "#ff9d8f") : "#9a9ab0") + ";";
        var right = doc.createElement("span");
        right.textContent = c.detail || "";
        right.style.cssText = "color:#c8c8dc;text-align:right;";
        row.appendChild(left);
        row.appendChild(right);
        list.appendChild(row);
      }
      card.appendChild(list);

      var close = doc.createElement("button");
      close.type = "button";
      close.textContent = "Cerrar";
      close.style.cssText =
        "display:block;width:calc(100% - 40px);margin:14px 20px 20px;padding:12px;border:0;border-radius:12px;" +
        "background:rgba(255,255,255,.12);color:#fff;font:600 17px -apple-system,system-ui,sans-serif;";
      close.onclick = function () {
        try { if (wrap.parentNode) wrap.parentNode.removeChild(wrap); } catch (_) {}
      };
      card.appendChild(close);

      wrap.appendChild(card);
      (doc.body || doc.documentElement).appendChild(wrap);
    } catch (_) {
      /* a broken selftest must never break the reader */
    }
  }

  var api = { computeChecks: computeChecks, renderCard: renderCard };
  try { root.svSelftest = api; } catch (_) {}
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
