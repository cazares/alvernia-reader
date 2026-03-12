const DIRECT_SOURCE_OVERRIDE_DISABLED_RE =
  /direct audio_url\/audio_id overrides are disabled/i;

export const isDirectSourceOverrideRejected = (message) =>
  DIRECT_SOURCE_OVERRIDE_DISABLED_RE.test(String(message || ""));

export const stripDirectSourceOverride = (payload = {}) => {
  const nextPayload = payload && typeof payload === "object" ? { ...payload } : {};
  delete nextPayload.audio_id;
  delete nextPayload.audio_url;
  return nextPayload;
};
