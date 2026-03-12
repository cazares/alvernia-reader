export const normalizeNetscapeCookies = (raw) =>
  String(raw || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();

const isCookieRow = (line) => {
  let cleaned = String(line || "").trimStart();
  if (!cleaned) return false;
  if (cleaned.startsWith("#HttpOnly_")) cleaned = cleaned.slice(1);
  if (cleaned.startsWith("#")) return false;
  return cleaned.split("\t").length >= 7;
};

const payloadHasCookieRows = (payload) =>
  String(payload || "")
    .split("\n")
    .some((line) => isCookieRow(line));

const HEADER_PATTERN = /^#\s*Netscape HTTP Cookie File\b/i;

const sanitizeCandidatePayload = (payload) => {
  const normalized = normalizeNetscapeCookies(payload);
  if (!normalized) return "";
  const lines = normalized
    .split("\n")
    .map((line) => String(line || "").trim())
    .filter(Boolean);
  if (!lines.length) return "";

  const hasHeader = lines.some((line) => HEADER_PATTERN.test(line));
  const cookieRows = lines.filter((line) => isCookieRow(line));
  if (!cookieRows.length) return "";
  if (!hasHeader) return cookieRows.join("\n");
  return ["# Netscape HTTP Cookie File", ...cookieRows].join("\n");
};

const dedupeCandidates = (candidates) => {
  const out = [];
  const seen = new Set();
  for (const rawCandidate of candidates) {
    const normalized = sanitizeCandidatePayload(rawCandidate);
    if (!normalized) continue;
    if (!payloadHasCookieRows(normalized)) continue;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
};

export const buildCookieCandidates = (raw) => {
  const normalized = normalizeNetscapeCookies(raw);
  if (!normalized) return [];

  const bySeparator = dedupeCandidates(
    normalized.split(/\n(?:-{3,}|={3,}|_{3,})\n/gm)
  );

  const byHeader = dedupeCandidates(
    normalized.split(/(?=^#\s*Netscape HTTP Cookie File\b)/gim)
  );
  if (byHeader.length > 1) return byHeader;

  if (bySeparator.length > 1) return bySeparator;
  if (
    bySeparator.length === 1 &&
    byHeader.length === 1 &&
    bySeparator[0] &&
    byHeader[0] &&
    bySeparator[0] !== byHeader[0]
  ) {
    return bySeparator;
  }

  if (byHeader.length === 1) return byHeader;
  if (bySeparator.length === 1) return bySeparator;
  return [normalized];
};

const toPositiveInt = (value) => {
  const n = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
};

const randomBase36Token = (length = 10) => {
  let token = "";
  while (token.length < length) {
    token += Math.random().toString(36).slice(2);
  }
  return token.slice(0, length);
};

export const buildCreateJobPayload = (query, sourceCookiesNetscape, options = {}) => {
  const opts = options && typeof options === "object" ? options : {};
  const payload = {
    query: String(query || "").trim(),
    idempotency_key:
      String(opts.idempotency_key || opts.idempotencyKey || "").trim() ||
      `ios-${Date.now()}-${randomBase36Token(10)}`,
  };
  const normalizedCookies = normalizeNetscapeCookies(sourceCookiesNetscape);
  if (normalizedCookies) {
    payload.source_cookies_netscape = normalizedCookies;
  }

  if (opts.force === true) payload.force = true;
  if (opts.reset === true) payload.reset = true;
  if (opts.no_parallel === true || opts.noParallel === true) payload.no_parallel = true;

  const ytSearchN = toPositiveInt(opts.yt_search_n ?? opts.ytSearchN);
  if (ytSearchN) payload.yt_search_n = ytSearchN;

  return payload;
};
