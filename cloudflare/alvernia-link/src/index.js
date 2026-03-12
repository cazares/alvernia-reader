const PAGES_ORIGIN = "https://alvernia-reader.pages.dev";
const LEGACY_ROUTE_PREFIX = "/alvernia";
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
  async fetch(request) {
    const requestUrl = new URL(request.url);
    const { redirectToTrailingSlash } = normalizeProxyPath({
      host: requestUrl.host,
      pathname: requestUrl.pathname,
    });

    if (redirectToTrailingSlash) {
      requestUrl.pathname = `${LEGACY_ROUTE_PREFIX}/`;
      return Response.redirect(requestUrl.toString(), 308);
    }

    const upstreamUrl = buildProxyUrl(request.url);
    const upstreamRequest = new Request(upstreamUrl, request);
    const upstreamResponse = await fetch(upstreamRequest);

    return copyResponse(upstreamResponse, requestUrl.host);
  },
};
