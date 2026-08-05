export type BundleManifest = {
  schema?: number;
  bookVersion: string;
  totalPages: number;
  builtFromShellBuild?: number;
  minShellBuild?: number;
  pagePadWidth?: number;
};

export type QuarantineEntry = {
  bookVersion: string;
  failures: number;
  lastFailureAt: number;
};

export type BundleSource = "documents" | "bundled" | "none";

export type DecideBundleContext = {
  docExists?: boolean;
  docManifest?: BundleManifest | null;
  bakedManifest?: BundleManifest | null;
  bakedExists?: boolean;
  forceBundled?: boolean;
  bootAttempts?: number;
  bootProved?: boolean;
  quarantine?: readonly QuarantineEntry[];
};

export type BundleDecision = {
  source: BundleSource;
  reason: string;
  quarantineDoc: boolean;
};

export type HealAction = {
  action: "remount" | "fall-back" | "give-up";
  quarantineCurrent: boolean;
};

export const MAX_BOOT_ATTEMPTS: number;
export const QUARANTINE_FAILURE_LIMIT: number;

export const isValidManifest: (m: unknown) => boolean;
export const isQuarantined: (
  quarantine: readonly QuarantineEntry[] | null | undefined,
  bookVersion: string,
) => boolean;
export const recordBundleFailure: (
  quarantine: readonly QuarantineEntry[] | null | undefined,
  bookVersion: string,
  nowMs: number,
) => QuarantineEntry[];
export const clearBundleFailures: (
  quarantine: readonly QuarantineEntry[] | null | undefined,
  bookVersion: string,
) => QuarantineEntry[];
export const decideBundle: (ctx: DecideBundleContext) => BundleDecision;
export const nextHealAction: (attempt: number, source: BundleSource | null) => HealAction;

/**
 * The DIRECTORY scope the WebView must be granted for a file:// bundle — never the index.html
 * itself, which exposes only that one file and denies every sibling asset. "" when unusable.
 */
export const readAccessDirFor: (uri: string | null | undefined) => string;
