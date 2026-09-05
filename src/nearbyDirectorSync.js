import { NativeEventEmitter, NativeModules, Platform } from "react-native";

const nativeModule = NativeModules.DirectorSyncModule;
const nearbyEventEmitter = nativeModule ? new NativeEventEmitter(nativeModule) : null;

export const isNearbyDirectorSyncAvailable = () => Platform.OS === "ios" && Boolean(nativeModule);

export const startNearbyDirector = async (sessionCode) => {
  if (!isNearbyDirectorSyncAvailable()) {
    return Promise.reject(new Error("La sincronización offline solo está disponible en iPad."));
  }
  return nativeModule.startDirector(sessionCode);
};

// LIVE TAKEOVER ("Tomar el control" while connected to the director being replaced). The native
// side announces the new token to that director over the still-open session BEFORE tearing it down,
// so the old director demotes at once instead of at its next ~25 s browser rebuild (45 s of two
// directors on hardware, 2026-09-05). Falls back to the old drop-then-start sequence on a shell that
// predates the method; the old director then learns through discovery, as before.
export const takeoverNearbyDirector = async (sessionCode) => {
  if (!isNearbyDirectorSyncAvailable()) {
    return Promise.reject(new Error("La sincronización offline solo está disponible en iPad."));
  }
  if (typeof nativeModule.takeoverDirector === "function") {
    return nativeModule.takeoverDirector(sessionCode);
  }
  await resetNearbyDirectorSync().catch(() => null);
  return nativeModule.startDirector(sessionCode);
};

export const startNearbyFollower = async (sessionCode) => {
  if (!isNearbyDirectorSyncAvailable()) {
    return Promise.reject(new Error("La sincronización offline solo está disponible en iPad."));
  }
  return nativeModule.startFollower(sessionCode);
};

export const stopNearbyDirectorSync = async () => {
  if (!isNearbyDirectorSyncAvailable()) {
    return null;
  }
  return nativeModule.stop();
};

export const resetNearbyDirectorSync = async () => {
  if (!isNearbyDirectorSyncAvailable()) {
    return null;
  }
  if (typeof nativeModule.resetForAppReset === "function") {
    return nativeModule.resetForAppReset();
  }
  return nativeModule.stop();
};

export const sendNearbyDirectorPageUpdate = async (page, totalPages = 0, context = {}) => {
  if (!isNearbyDirectorSyncAvailable()) {
    return null;
  }
  return nativeModule.sendPageUpdate(
    page,
    totalPages,
    String(context.mode || ""),
    String(context.bookId || ""),
  );
};

export const primeNearbyPermissions = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.primePermissions === "function") {
    return nativeModule.primePermissions();
  }
  return null;
};

export const refreshNearbyDiscovery = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.refreshNearbyDiscovery === "function") {
    return nativeModule.refreshNearbyDiscovery();
  }
  return null;
};

// SPLIT-BRAIN KICK FOR A BRAND-NEW DIRECTOR — browser only, never the advertiser.
//
// becomeDirector used refreshNearbyDiscovery for this, which destroys the advertiser as its first
// act. It fired the instant the device started serving, so every follower whose foundPeer had
// already triggered had its invite evaporate silently and then waited out a timeout. The intent was
// only ever "find other directors fast"; going invisible was an accident of reusing the full
// refresh. Falls back to the full refresh on an older shell that lacks this method.
export const refreshDirectorBrowse = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.refreshDirectorBrowse === "function") {
    return nativeModule.refreshDirectorBrowse();
  }
  if (typeof nativeModule.refreshNearbyDiscovery === "function") {
    return nativeModule.refreshNearbyDiscovery();
  }
  return null;
};

// A human tapping the reconnect button outranks every "don't disturb a live session" heuristic:
// they can already see it is not working. This tears the session down so discovery can resume,
// which refreshNearbyDiscovery alone cannot do.
export const forceFollowerReconnectNow = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.forceFollowerReconnectNow === "function") {
    return nativeModule.forceFollowerReconnectNow();
  }
  return null;
};

export const requestCurrentSnapshot = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.requestCurrentSnapshot === "function") {
    return nativeModule.requestCurrentSnapshot();
  }
  return null;
};

export const requestDirectorTakeover = async () => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.requestDirectorTakeover === "function") {
    return nativeModule.requestDirectorTakeover();
  }
  throw new Error("Director takeover no soportado en esta versión.");
};

export const approveDirectorTakeover = async (requestId) => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.approveDirectorTakeover === "function") {
    return nativeModule.approveDirectorTakeover(String(requestId || ""));
  }
  return null;
};

export const denyDirectorTakeover = async (requestId) => {
  if (!isNearbyDirectorSyncAvailable()) return null;
  if (typeof nativeModule.denyDirectorTakeover === "function") {
    return nativeModule.denyDirectorTakeover(String(requestId || ""));
  }
  return null;
};

export const addNearbyDirectorSyncListener = (listener) => {
  if (!nearbyEventEmitter) {
    return { remove() {} };
  }
  return nearbyEventEmitter.addListener("DirectorSyncEvent", listener);
};
