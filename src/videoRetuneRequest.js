export const buildApplyVideoTuningRequest = ({ query, mixLevels }) => {
  const trimmedQuery = String(query || "").trim();
  if (!trimmedQuery) return null;
  return {
    renderOnly: true,
    presetAutoOpen: true,
    queryOverride: trimmedQuery,
    mixLevelsOverride: mixLevels,
  };
};
