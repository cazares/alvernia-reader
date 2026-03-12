const PDF_PAGE_MIN = 1;
const PDF_PAGE_MAX = 10000;

export const clampPdfPage = (value, min = PDF_PAGE_MIN, max = PDF_PAGE_MAX) => {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed)) return min;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
};

export const normalizePdfUrl = (rawUrl = "") => {
  const trimmed = String(rawUrl || "").trim();
  if (!trimmed) return "";

  try {
    const parsed = new URL(trimmed);
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return "";
  }
};

export const buildPdfPageUrl = ({ pdfUrl = "", page = PDF_PAGE_MIN } = {}) => {
  const normalizedPdfUrl = normalizePdfUrl(pdfUrl);
  if (!normalizedPdfUrl) return "";

  const safePage = clampPdfPage(page);
  return `${normalizedPdfUrl}#page=${safePage}`;
};
