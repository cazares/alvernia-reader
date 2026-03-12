const LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "::1"]);
const PRIVATE_IPV4_PATTERNS = [
  /^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$/,
  /^192\.168\.\d{1,3}\.\d{1,3}$/,
  /^172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}$/,
  /^169\.254\.\d{1,3}\.\d{1,3}$/,
];

const normalizeBaseUrl = (value) => String(value || "").trim().replace(/\/$/, "");

const parseUrl = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  const candidate = /^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `http://${text}`;
  try {
    return new URL(candidate);
  } catch {
    return null;
  }
};

export const isLoopbackHost = (value) => {
  const parsed = parseUrl(value);
  if (!parsed) return false;
  return LOCAL_HOSTS.has(String(parsed.hostname || "").trim().toLowerCase());
};

export const isLocalNetworkHost = (value) => {
  const parsed = parseUrl(value);
  if (!parsed) return false;
  const hostname = String(parsed.hostname || "").trim().toLowerCase();
  if (!hostname) return false;
  if (LOCAL_HOSTS.has(hostname)) return true;
  return PRIVATE_IPV4_PATTERNS.some((pattern) => pattern.test(hostname));
};

export const deriveDevServerHost = (candidates = []) => {
  for (const candidate of candidates) {
    const parsed = parseUrl(candidate);
    if (!parsed) continue;
    const host = String(parsed.hostname || "").trim();
    if (!host) continue;
    if (LOCAL_HOSTS.has(host.toLowerCase())) continue;
    return host;
  }
  return "";
};

export const selectConfiguredApiBaseUrl = ({
  envBaseUrl,
  expoConfigBaseUrl,
  defaultBaseUrl = "",
} = {}) => {
  const envValue = normalizeBaseUrl(envBaseUrl);
  if (envValue) return envValue;
  const expoValue = normalizeBaseUrl(expoConfigBaseUrl);
  if (expoValue) return expoValue;
  return normalizeBaseUrl(defaultBaseUrl);
};

export const resolveLocalApiBaseUrl = ({ configuredBaseUrl, devServerCandidates = [] } = {}) => {
  const normalized = normalizeBaseUrl(configuredBaseUrl);
  if (!normalized) return "";
  const parsedBase = parseUrl(normalized);
  if (!parsedBase) return normalized;
  if (!LOCAL_HOSTS.has(String(parsedBase.hostname || "").trim().toLowerCase())) {
    return normalized;
  }

  const derivedHost = deriveDevServerHost(devServerCandidates);
  if (!derivedHost) return normalized;

  const resolved = new URL(parsedBase.toString());
  resolved.hostname = derivedHost;
  return normalizeBaseUrl(resolved.toString());
};

export const resolveFailoverApiBaseUrl = ({
  currentBaseUrl,
  primaryBaseUrl,
  fallbackBaseUrl,
} = {}) => {
  const current = normalizeBaseUrl(currentBaseUrl);
  const primary = normalizeBaseUrl(primaryBaseUrl);
  const fallback = normalizeBaseUrl(fallbackBaseUrl);

  if (!current && !primary && !fallback) return "";

  const nonCurrentCandidate = (candidate) => {
    const normalizedCandidate = normalizeBaseUrl(candidate);
    if (!normalizedCandidate) return "";
    if (normalizedCandidate === current) return "";
    return normalizedCandidate;
  };

  if (current && primary && current !== primary) {
    return primary;
  }

  const fallbackCandidate = nonCurrentCandidate(fallback);
  if (fallbackCandidate) {
    const primaryIsRemote = Boolean(primary) && !isLocalNetworkHost(primary);
    const fallbackIsLoopback = isLoopbackHost(fallbackCandidate);
    if (!(primaryIsRemote && fallbackIsLoopback)) {
      return fallbackCandidate;
    }
  }

  return nonCurrentCandidate(primary);
};
