const PAGES_ORIGIN = "https://alvernia-reader.pages.dev";
const LEGACY_ROUTE_PREFIX = "/alvernia";
const UPLOAD_PREFIX = "uploads/";
const ACTIVE_PREFIX = "active";
const LATEST_KEY = "latest.json";
const ALLOWED_UPLOAD_EXTENSIONS = new Set(["pdf", "key"]);
const SPECIAL_ROUTES = new Set(["/upload", "/download", "/promote"]);
const PAGE_PATH_PREFIX = "/pages/";
const PAGES_MANIFEST_PATH = "/pages.json";
const ROOT_PROXY_HOSTS = new Set([
  "miguelcoro.com",
  "www.miguelcoro.com",
  "miguelbase.com",
  "www.miguelbase.com",
  "miguelworld.com",
  "www.miguelworld.com",
]);

export const normalizeProxyPath = ({ host, pathname }) => {
  if (ROOT_PROXY_HOSTS.has(host)) {
    return { redirectToTrailingSlash: false, proxiedPath: pathname || "/" };
  }

  if (pathname === LEGACY_ROUTE_PREFIX) {
    return { redirectToTrailingSlash: true, proxiedPath: "/" };
  }

  if (pathname === `${LEGACY_ROUTE_PREFIX}/`) {
    return { redirectToTrailingSlash: false, proxiedPath: "/" };
  }

  if (!pathname.startsWith(`${LEGACY_ROUTE_PREFIX}/`)) {
    return { redirectToTrailingSlash: false, proxiedPath: pathname };
  }

  const proxiedPath = pathname.slice(LEGACY_ROUTE_PREFIX.length) || "/";
  return { redirectToTrailingSlash: false, proxiedPath };
};

export const buildProxyUrl = (requestUrl) => {
  const incomingUrl = new URL(requestUrl);
  const { proxiedPath } = normalizeProxyPath({
    host: incomingUrl.host,
    pathname: incomingUrl.pathname,
  });
  return new URL(`${proxiedPath}${incomingUrl.search}`, PAGES_ORIGIN);
};

export const isSpecialRoute = (pathname) => SPECIAL_ROUTES.has(pathname.replace(/\/$/, ""));
export const isPageAssetRoute = (pathname) => pathname === PAGES_MANIFEST_PATH || pathname.startsWith(PAGE_PATH_PREFIX);

const htmlResponse = (body, status = 200) => new Response(body, {
  status,
  headers: {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store",
  },
});

const jsonResponse = (body, status = 200) => new Response(JSON.stringify(body), {
  status,
  headers: {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  },
});

const sanitizeFilename = (name = "archivo") => name.replace(/[^a-zA-Z0-9._-]/g, "_");

const extractExtension = (name = "") => {
  const parts = name.toLowerCase().split(".");
  return parts.length > 1 ? parts.pop() : "";
};

const readLatest = async (env) => {
  const object = await env.ALVERNIA_UPLOADS.get(LATEST_KEY);
  if (!object) return null;
  const text = await object.text();
  return JSON.parse(text);
};

const writeLatest = async (env, payload) => {
  await env.ALVERNIA_UPLOADS.put(LATEST_KEY, JSON.stringify(payload), {
    httpMetadata: { contentType: "application/json" },
  });
};

const createR2Response = (object, fallbackStatus = 404) => {
  if (!object) {
    return new Response("No encontrado.", { status: fallbackStatus });
  }

  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("etag", object.httpEtag);
  headers.set("cache-control", "no-store");
  return new Response(object.body, { status: 200, headers });
};

const buildUploadPage = () => `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Subir archivo - Nuestro Coro</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); }
      h1 { margin: 0 0 8px; font-size: 1.4rem; }
      p { margin: 0 0 18px; color: #b9c0cc; }
      input[type=file] { width: 100%; margin-bottom: 16px; }
      button { width: 100%; padding: 12px 16px; border-radius: 14px; border: 0; background: #2e81e8; color: #fff; font-weight: 600; font-size: 1rem; cursor: pointer; }
      .hint { font-size: 0.85rem; margin-top: 12px; color: #8b93a1; }
    </style>
  </head>
  <body>
    <form class="card" method="post" enctype="multipart/form-data">
      <h1>Subir archivo</h1>
      <p>Archivos soportados: Keynote (.key) o PDF.</p>
      <input type="file" name="file" accept=".key,.pdf,application/pdf" required />
      <button type="submit">Subir</button>
      <p class="hint">Luego entra a <strong>/promote</strong> para activar el nuevo archivo en el lector.</p>
    </form>
  </body>
</html>`;

const buildPromotePage = (latest) => {
  if (!latest) {
    return htmlResponse("<p style=\"font-family:system-ui;\">No hay archivos subidos todavía.</p>");
  }

  const status = latest.promotedAt
    ? `Activo desde ${latest.promotedAt}`
    : latest.promoteRequestedAt
      ? `Promocion solicitado ${latest.promoteRequestedAt}`
      : "Listo para promocionar";

  return htmlResponse(`<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Promote - Nuestro Coro</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); }
      h1 { margin: 0 0 8px; font-size: 1.4rem; }
      p { margin: 0 0 18px; color: #b9c0cc; }
      button { width: 100%; padding: 12px 16px; border-radius: 14px; border: 0; background: #2e81e8; color: #fff; font-weight: 600; font-size: 1rem; cursor: pointer; }
      .meta { font-size: 0.9rem; color: #9aa3b2; margin-bottom: 18px; }
    </style>
  </head>
  <body>
    <form class="card" method="post">
      <h1>Promocionar archivo</h1>
      <p>Archivo: <strong>${latest.uploadName ?? "(sin nombre)"}</strong></p>
      <div class="meta">Estado: ${status}</div>
      <button type="submit">Promote ahora</button>
    </form>
  </body>
</html>`);
};

const buildDownloadPage = (latest) => {
  if (!latest || !latest.pdfKey) {
    return htmlResponse("<p style=\"font-family:system-ui;\">Aun no hay PDF listo para descargar.</p>");
  }

  if (!latest.promotedAt && latest.status !== "ready") {
    return htmlResponse("<p style=\"font-family:system-ui;\">PDF en proceso. Intenta de nuevo en unos segundos.</p>");
  }

  return htmlResponse(`<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Descargar - Nuestro Coro</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); text-align: center; }
      a { display: inline-block; margin-top: 12px; padding: 12px 16px; border-radius: 14px; background: #2e81e8; color: #fff; font-weight: 600; text-decoration: none; }
    </style>
  </head>
  <body>
    <div class="card">
      <div>PDF listo: <strong>${latest.uploadName ?? "archivo"}</strong></div>
      <a href="/download?raw=1">Descargar PDF</a>
    </div>
  </body>
</html>`);
};

const handleUpload = async (request, env) => {
  if (request.method === "GET") {
    return htmlResponse(buildUploadPage());
  }

  if (request.method !== "POST") {
    return new Response("Metodo no permitido", { status: 405 });
  }

  const form = await request.formData();
  const file = form.get("file");
  if (!(file instanceof File)) {
    return htmlResponse("<p style=\"font-family:system-ui;\">Archivo invalido.</p>", 400);
  }

  const extension = extractExtension(file.name);
  if (!ALLOWED_UPLOAD_EXTENSIONS.has(extension)) {
    return htmlResponse("<p style=\"font-family:system-ui;\">Solo .key o .pdf.</p>", 400);
  }

  const safeName = sanitizeFilename(file.name);
  const uploadKey = `${UPLOAD_PREFIX}${crypto.randomUUID()}-${safeName}`;
  await env.ALVERNIA_UPLOADS.put(uploadKey, file.stream(), {
    httpMetadata: { contentType: file.type || "application/octet-stream" },
  });

  const now = new Date().toISOString();
  const latest = {
    uploadKey,
    uploadName: file.name,
    uploadedAt: now,
    status: extension === "pdf" ? "ready" : "uploaded",
    pdfKey: extension === "pdf" ? uploadKey : null,
    promoteRequestedAt: null,
    promotedAt: null,
  };

  await writeLatest(env, latest);

  return htmlResponse(`<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Subido</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); text-align: center; }
      a { display: inline-block; margin-top: 12px; padding: 10px 16px; border-radius: 14px; background: #2e81e8; color: #fff; font-weight: 600; text-decoration: none; }
    </style>
  </head>
  <body>
    <div class="card">
      <div>Archivo subido.</div>
      <a href="/promote">Ir a Promote</a>
      <a href="/download">Descargar ultimo PDF</a>
    </div>
  </body>
</html>`);
};

const handlePromote = async (request, env) => {
  if (request.method === "GET") {
    const latest = await readLatest(env);
    return buildPromotePage(latest);
  }

  if (request.method !== "POST") {
    return new Response("Metodo no permitido", { status: 405 });
  }

  const latest = await readLatest(env);
  if (!latest) {
    return htmlResponse("<p style=\"font-family:system-ui;\">No hay archivos para promover.</p>", 404);
  }

  latest.promoteRequestedAt = new Date().toISOString();
  latest.promotedAt = null;
  await writeLatest(env, latest);
  return htmlResponse("<p style=\"font-family:system-ui;\">Promote solicitado. El Mac convertira el archivo pronto.</p>");
};

const handleDownload = async (request, env) => {
  const latest = await readLatest(env);
  if (!latest || !latest.pdfKey) {
    return htmlResponse("<p style=\"font-family:system-ui;\">Aun no hay PDF listo.</p>", 404);
  }

  if (!new URL(request.url).searchParams.has("raw")) {
    return buildDownloadPage(latest);
  }

  const object = await env.ALVERNIA_UPLOADS.get(latest.pdfKey);
  if (!object) {
    return new Response("PDF no encontrado", { status: 404 });
  }

  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("content-type", "application/pdf");
  headers.set("content-disposition", `attachment; filename="${sanitizeFilename(latest.uploadName || "archivo.pdf")}"`);
  headers.set("cache-control", "no-store");
  return new Response(object.body, { status: 200, headers });
};

const tryServeActivePages = async (requestUrl, env) => {
  if (!env.ALVERNIA_UPLOADS) return null;
  const path = requestUrl.pathname;
  const activeKey = path === PAGES_MANIFEST_PATH
    ? `${ACTIVE_PREFIX}/pages.json`
    : `${ACTIVE_PREFIX}${path}`;

  const object = await env.ALVERNIA_UPLOADS.get(activeKey);
  if (!object) return null;
  return createR2Response(object, 404);
};

const copyResponse = (upstreamResponse, requestHost) => {
  const headers = new Headers(upstreamResponse.headers);
  headers.set("x-alvernia-proxy", requestHost);

  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers,
  });
};

export default {
  async fetch(request, env) {
    const requestUrl = new URL(request.url);
    const pathname = requestUrl.pathname.replace(/\/$/, "") || "/";
    const { redirectToTrailingSlash } = normalizeProxyPath({
      host: requestUrl.host,
      pathname: requestUrl.pathname,
    });

    if (redirectToTrailingSlash) {
      requestUrl.pathname = `${LEGACY_ROUTE_PREFIX}/`;
      return Response.redirect(requestUrl.toString(), 308);
    }

    if (isSpecialRoute(pathname)) {
      if (!env?.ALVERNIA_UPLOADS) {
        return jsonResponse({ error: "R2 no configurado" }, 500);
      }

      if (pathname === "/upload") {
        return handleUpload(request, env);
      }

      if (pathname === "/promote") {
        return handlePromote(request, env);
      }

      if (pathname === "/download") {
        return handleDownload(request, env);
      }
    }

    if (isPageAssetRoute(requestUrl.pathname)) {
      const active = await tryServeActivePages(requestUrl, env);
      if (active) return active;
    }

    const upstreamUrl = buildProxyUrl(request.url);
    const upstreamRequest = new Request(upstreamUrl, request);
    const upstreamResponse = await fetch(upstreamRequest);

    return copyResponse(upstreamResponse, requestUrl.host);
  },
};
