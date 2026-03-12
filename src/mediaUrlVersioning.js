const REV_QUERY_KEY = "mixterious_rev";

const normalizeStampMs = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return Math.max(1, Math.round(parsed * 1000));
};

export const buildJobMediaRevisionToken = (job = null, channel = "media") => {
  const jobId = String(job?.id || "").trim();
  const stampMs =
    normalizeStampMs(job?.render_finished_at) ||
    normalizeStampMs(job?.finished_at) ||
    normalizeStampMs(job?.last_updated_at) ||
    normalizeStampMs(job?.started_at) ||
    normalizeStampMs(job?.created_at);
  const suffix = String(channel || "").trim();
  const parts = [jobId, stampMs || 0, suffix].filter(Boolean);
  return parts.length ? parts.join(":") : null;
};

export const appendMediaRevisionToken = (url, token) => {
  const rawUrl = String(url || "").trim();
  const rawToken = String(token || "").trim();
  if (!rawUrl) return null;
  if (!rawToken) return rawUrl;

  const hashIndex = rawUrl.indexOf("#");
  const beforeHash = hashIndex >= 0 ? rawUrl.slice(0, hashIndex) : rawUrl;
  const hash = hashIndex >= 0 ? rawUrl.slice(hashIndex) : "";
  const queryIndex = beforeHash.indexOf("?");
  const pathname = queryIndex >= 0 ? beforeHash.slice(0, queryIndex) : beforeHash;
  const search = queryIndex >= 0 ? beforeHash.slice(queryIndex + 1) : "";
  const params = new URLSearchParams(search);
  params.set(REV_QUERY_KEY, rawToken);
  const nextSearch = params.toString();
  return `${pathname}${nextSearch ? `?${nextSearch}` : ""}${hash}`;
};
