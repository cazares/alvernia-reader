const STEM_KEYS = ["vocals", "bass", "drums", "other"];

const clampStemLevel = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 100;
  return Math.max(0, Math.min(150, Math.round(parsed)));
};

export const normalizeMixLevels = (mixLevels = {}) => ({
  vocals: clampStemLevel(mixLevels?.vocals),
  bass: clampStemLevel(mixLevels?.bass),
  drums: clampStemLevel(mixLevels?.drums),
  other: clampStemLevel(mixLevels?.other),
});

export const hasCustomStemMix = (mixLevels = {}) => {
  const normalized = normalizeMixLevels(mixLevels);
  return STEM_KEYS.some((stem) => Math.abs(Number(normalized[stem]) - 100) > 1e-6);
};

const clampOffsetSec = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(Math.max(-15, Math.min(15, parsed)) * 100) / 100;
};

export const buildStartJobRequestPayload = ({
  createJobPayload,
  renderOnly = false,
  upload = false,
  preview = false,
  offsetSec = 0,
  mixLevels,
  mixLevelsOverride,
  audioId,
  audioUrl,
} = {}) => {
  const payload = createJobPayload && typeof createJobPayload === "object" ? { ...createJobPayload } : {};
  const effectiveMixLevels = normalizeMixLevels(mixLevelsOverride || mixLevels);
  const includeStemLevels = Boolean(renderOnly || hasCustomStemMix(effectiveMixLevels));

  return {
    ...payload,
    render_only: Boolean(renderOnly),
    ...(upload ? { upload: true } : {}),
    ...(preview ? { preview: true } : {}),
    offset_sec: clampOffsetSec(offsetSec),
    ...(includeStemLevels ? effectiveMixLevels : {}),
    ...(String(audioId || "").trim() ? { audio_id: String(audioId).trim() } : {}),
    ...(String(audioUrl || "").trim() ? { audio_url: String(audioUrl).trim() } : {}),
  };
};
