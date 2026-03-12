// Runtime API base URL holder.
// This lets the app fail over between hosts without requiring a rebuild.

let _apiBaseUrl = "";

const normalize = (url: string) => String(url || "").trim().replace(/\/$/, "");

export const getApiBaseUrl = (): string => _apiBaseUrl;

export const setApiBaseUrl = (url: string): void => {
  const next = normalize(url);
  if (!next) return;
  _apiBaseUrl = next;
};

export const ensureApiBaseUrl = (url: string): string => {
  if (!_apiBaseUrl) {
    setApiBaseUrl(url);
  }
  return _apiBaseUrl;
};

