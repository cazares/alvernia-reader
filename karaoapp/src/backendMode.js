const LOOPBACK_HOST_RE = /^(localhost|127\.0\.0\.1)$/i;
const PRIVATE_IPV4_RE =
  /^(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3})$/i;

const getHostname = (baseUrl) => {
  const raw = String(baseUrl || "").trim();
  if (!raw) return "";
  try {
    return new URL(raw).hostname;
  } catch {
    return "";
  }
};

export const detectBackendMode = (baseUrl) => {
  const hostname = getHostname(baseUrl);
  if (LOOPBACK_HOST_RE.test(hostname) || PRIVATE_IPV4_RE.test(hostname)) return "local_cli";
  return "remote";
};

export const shouldPreferDirectPlaybackForBaseUrl = (baseUrl) =>
  detectBackendMode(baseUrl) === "local_cli";

export const shouldRequestYoutubeUploadForBaseUrl = (baseUrl) =>
  detectBackendMode(baseUrl) !== "local_cli";
