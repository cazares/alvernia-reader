export type MixLevelsState = {
  vocals: number;
  bass: number;
  drums: number;
  other: number;
};

export function normalizeMixLevels(
  mixLevels?: Partial<MixLevelsState> | null
): MixLevelsState;

export function hasCustomStemMix(
  mixLevels?: Partial<MixLevelsState> | null
): boolean;

export function buildStartJobRequestPayload(options?: {
  createJobPayload?: Record<string, unknown> | null;
  renderOnly?: boolean;
  upload?: boolean;
  preview?: boolean;
  offsetSec?: number;
  mixLevels?: Partial<MixLevelsState> | null;
  mixLevelsOverride?: Partial<MixLevelsState> | null;
  audioId?: string | null;
  audioUrl?: string | null;
}): Record<string, unknown>;
