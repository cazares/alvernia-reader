const DEFAULT_API_ORIGIN = "https://unridable-rimose-remy.ngrok-free.dev";

const HOP_BY_HOP_HEADERS = [
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailers",
  "transfer-encoding",
  "upgrade",
];

function normalizeOrigin(value) {
  return value.trim().replace(/\/+$/, "");
}

function isBodylessMethod(method) {
  return method === "GET" || method === "HEAD";
}

function isMediaPath(pathname) {
  return pathname.startsWith("/output/") || pathname.startsWith("/files/");
}

const worker = {
  async fetch(request, env) {
    const apiOrigin = normalizeOrigin(env.API_ORIGIN || DEFAULT_API_ORIGIN);
    const incomingUrl = new URL(request.url);
    const upstreamUrl = new URL(`${apiOrigin}${incomingUrl.pathname}${incomingUrl.search}`);
    const mediaRequest = isMediaPath(incomingUrl.pathname);
    const rangeRequested = request.headers.has("range");
    const bypassEdgeCache = mediaRequest || rangeRequested;

    const forwardedHeaders = new Headers(request.headers);
    for (const header of HOP_BY_HOP_HEADERS) {
      forwardedHeaders.delete(header);
    }
    if (mediaRequest) {
      forwardedHeaders.delete("if-none-match");
      forwardedHeaders.delete("if-modified-since");
      if (request.method === "GET") {
        const requestedRange = String(request.headers.get("range") || "").trim();
        forwardedHeaders.set("range", requestedRange || "bytes=0-");
      }
    }

    forwardedHeaders.set("host", upstreamUrl.host);
    forwardedHeaders.set("x-forwarded-host", incomingUrl.host);
    forwardedHeaders.set("x-forwarded-proto", incomingUrl.protocol.replace(":", ""));

    const connectingIp = request.headers.get("cf-connecting-ip");
    if (connectingIp) {
      forwardedHeaders.set("x-forwarded-for", connectingIp);
      forwardedHeaders.set("x-real-ip", connectingIp);
    }

    const upstreamRequest = new Request(upstreamUrl.toString(), {
      method: request.method,
      headers: forwardedHeaders,
      body: isBodylessMethod(request.method) ? undefined : request.body,
      redirect: "manual",
    });

    try {
      const upstreamResponse = await fetch(
        upstreamRequest,
        bypassEdgeCache
          ? {
              cf: {
                cacheEverything: false,
                cacheTtl: 0,
              },
            }
          : undefined
      );
      const responseHeaders = new Headers(upstreamResponse.headers);
      responseHeaders.set("x-mixterious-api-origin", apiOrigin);
      if (bypassEdgeCache) {
        responseHeaders.set("cache-control", "no-store, max-age=0");
        responseHeaders.set("pragma", "no-cache");
        responseHeaders.set("cdn-cache-control", "no-store");
        responseHeaders.set("cloudflare-cdn-cache-control", "no-store");
      }
      if (mediaRequest) {
        responseHeaders.delete("etag");
        responseHeaders.delete("last-modified");
        if (!responseHeaders.get("accept-ranges")) {
          responseHeaders.set("accept-ranges", "bytes");
        }
      }
      return new Response(upstreamResponse.body, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers: responseHeaders,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown_error";
      return Response.json(
        {
          error: "backend_unreachable",
          message: "Mixterious API upstream is temporarily unavailable.",
          upstream: apiOrigin,
          detail: message,
        },
        { status: 503 }
      );
    }
  },
};

export default worker;
export { normalizeOrigin, isBodylessMethod, isMediaPath };
