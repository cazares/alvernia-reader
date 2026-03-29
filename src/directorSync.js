export const DEFAULT_DIRECTOR_SYNC_ENDPOINT = "https://signovino.com/director-sync";
export const DEFAULT_DIRECTOR_HEARTBEAT_MS = 4000;
export const DEFAULT_DIRECTOR_SYNC_TIMEOUT_MS = 4500;

export const normalizeSyncSessionCode = (value = "") => value
  .toUpperCase()
  .replace(/[^A-Z0-9]/g, "")
  .slice(0, 12);

export const createDefaultSyncSessionCode = () => `CORO${Math.floor(1000 + Math.random() * 9000)}`;

export const createDirectorKey = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;

export const resolveDirectorSyncEndpoint = (extra = {}) => {
  const customEndpoint = String(extra?.directorSyncEndpoint || "").trim();
  return customEndpoint || DEFAULT_DIRECTOR_SYNC_ENDPOINT;
};

const buildDirectorSyncUrl = (endpoint, sessionCode) => {
  const baseUrl = new URL(endpoint);
  baseUrl.searchParams.set("session", normalizeSyncSessionCode(sessionCode));
  return baseUrl.toString();
};

const parseJsonResponse = async (response) => {
  const payload = await response.json().catch(() => null);
  return payload && typeof payload === "object" ? payload : null;
};

const buildSyncError = (payload, fallbackMessage) => {
  const error = new Error(payload?.error || fallbackMessage);
  error.code = String(payload?.code || "DIRECTOR_SYNC_ERROR");
  return error;
};

const buildNetworkSyncError = (fallbackMessage, code) => {
  const error = new Error(fallbackMessage);
  error.code = code;
  return error;
};

const fetchDirectorSync = async (url, options = {}) => {
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timeoutId = controller
    ? setTimeout(() => controller.abort(), DEFAULT_DIRECTOR_SYNC_TIMEOUT_MS)
    : null;

  try {
    return await fetch(url, {
      ...options,
      signal: controller?.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw buildNetworkSyncError("La conexión con el director tardó demasiado. Intenta de nuevo.", "DIRECTOR_TIMEOUT");
    }
    throw buildNetworkSyncError("No se pudo conectar con la sesión del director.", "DIRECTOR_NETWORK");
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
};

export const readDirectorSyncState = async ({ endpoint, sessionCode }) => {
  const normalizedSessionCode = normalizeSyncSessionCode(sessionCode);
  if (!normalizedSessionCode) return null;

  const response = await fetchDirectorSync(buildDirectorSyncUrl(endpoint, normalizedSessionCode), {
    method: "GET",
    headers: {
      accept: "application/json",
    },
  });

  if (response.status === 404) return null;

  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw buildSyncError(payload, "No se pudo leer la sesión del director.");
  }

  return payload;
};

export const publishDirectorSyncState = async ({
  endpoint,
  sessionCode,
  directorKey,
  page,
  totalPages = 0,
}) => {
  const normalizedSessionCode = normalizeSyncSessionCode(sessionCode);
  if (!normalizedSessionCode) {
    throw new Error("Ingresa un código de sesión válido.");
  }

  const safePage = Math.max(1, Number.parseInt(String(page || "1"), 10) || 1);
  const safeTotalPages = Math.max(0, Number.parseInt(String(totalPages || "0"), 10) || 0);

  const response = await fetchDirectorSync(buildDirectorSyncUrl(endpoint, normalizedSessionCode), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
    },
    body: JSON.stringify({
      directorKey: String(directorKey || "").trim(),
      page: safePage,
      totalPages: safeTotalPages,
    }),
  });

  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw buildSyncError(payload, "No se pudo publicar la página del director.");
  }

  return payload;
};

export const releaseDirectorSyncState = async ({
  endpoint,
  sessionCode,
  directorKey,
}) => {
  const normalizedSessionCode = normalizeSyncSessionCode(sessionCode);
  if (!normalizedSessionCode) return null;

  const response = await fetchDirectorSync(buildDirectorSyncUrl(endpoint, normalizedSessionCode), {
    method: "DELETE",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
    },
    body: JSON.stringify({
      directorKey: String(directorKey || "").trim(),
    }),
  });

  if (response.status === 404) return null;

  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw buildSyncError(payload, "No se pudo liberar la sesión del director.");
  }

  return payload;
};
