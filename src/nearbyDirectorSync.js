import { NativeEventEmitter, NativeModules, Platform } from "react-native";

const nativeModule = NativeModules.DirectorSyncModule;
const nearbyEventEmitter = nativeModule ? new NativeEventEmitter(nativeModule) : null;

export const isNearbyDirectorSyncAvailable = () => Platform.OS === "ios" && Boolean(nativeModule);

export const startNearbyDirector = async (sessionCode) => {
  if (!isNearbyDirectorSyncAvailable()) {
    throw new Error("La sincronización offline solo está disponible en iPad.");
  }
  return nativeModule.startDirector(sessionCode);
};

export const startNearbyFollower = async (sessionCode) => {
  if (!isNearbyDirectorSyncAvailable()) {
    throw new Error("La sincronización offline solo está disponible en iPad.");
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

export const addNearbyDirectorSyncListener = (listener) => {
  if (!nearbyEventEmitter) {
    return { remove() {} };
  }
  return nearbyEventEmitter.addListener("DirectorSyncEvent", listener);
};
