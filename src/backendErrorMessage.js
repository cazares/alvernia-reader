import { detectBackendMode } from "./backendMode.js";

export const LOCAL_BACKEND_UNREACHABLE_MESSAGE =
  "Could not connect to the local backend. Make sure Metro and the backend are running, then try again";

export const REMOTE_BACKEND_UNREACHABLE_MESSAGE =
  "Could not connect to the backend. Check your internet connection and try again";

export const looksLikeHtmlErrorPayload = (value) => {
  const text = String(value || "").trim();
  if (!text) return false;
  const lower = text.toLowerCase();
  return (
    lower.startsWith("<!doctype html") ||
    lower.startsWith("<html") ||
    lower.includes("<head>") ||
    lower.includes("<body>") ||
    lower.includes("cloudflare tunnel error") ||
    lower.includes("origin dns error") ||
    lower.includes("error 1033")
  );
};

export const isLikelyNetworkErrorText = (value) => {
  const lower = String(value || "").trim().toLowerCase();
  if (!lower) return false;
  return (
    lower.includes("network request failed") ||
    lower.includes("could not connect to the server") ||
    lower.includes("failed to fetch") ||
    lower.includes("the internet connection appears to be offline")
  );
};

export const resolveBackendUnreachableMessage = (baseUrl) => {
  return detectBackendMode(baseUrl) === "local_cli"
    ? LOCAL_BACKEND_UNREACHABLE_MESSAGE
    : REMOTE_BACKEND_UNREACHABLE_MESSAGE;
};

export const isBackendUnreachableMessage = (value) => {
  return (
    value === LOCAL_BACKEND_UNREACHABLE_MESSAGE ||
    value === REMOTE_BACKEND_UNREACHABLE_MESSAGE
  );
};
