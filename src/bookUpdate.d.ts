export type BookUpdatePointer = { bookVersion: string; base: string };

export type StagedRecord = {
  bookVersion: string;
  ready: boolean;
  readyAt?: number;
  totalPages?: number;
  minShellBuild?: number;
  files?: number;
  error: string | null;
  detail?: unknown;
  at?: number;
};

export type ShouldStageContext = {
  killSwitch?: boolean;
  bookVersion?: string;
  activeBookVersion?: string | null;
  stagedBookVersion?: string | null;
  stagedReady?: boolean;
  /** Absent counts as EXPIRED (M1) — an un-ageable record must not read as "already staged". */
  stagedReadyAt?: number | null;
  quarantine?: readonly { bookVersion: string; failures: number }[];
  webReady?: boolean;
  foreground?: boolean;
  firstSeenAt?: number | null;
  deviceId?: string;
  now?: number;
  minShellBuild?: number;
  shellBuild?: number;
};

// No role field on either context, deliberately (owner decision, 2026-08-03): every role stages and
// applies. Re-adding one here is how the exemption comes back by accident.
export type CanApplyContext = {
  stagedReady?: boolean;
  stagedReadyAt?: number | null;
  lastCheckinOkAt?: number | null;
  meshPeerConnected?: boolean;
  lastPageTurnAt?: number | null;
  /** Room activity, not identity: the last time a page moved in this room, whoever moved it. */
  lastMeshPageAt?: number | null;
  webReady?: boolean;
  minShellBuild?: number;
  shellBuild?: number;
  now?: number;
};

export type FileEntry = { p: string; n: number; h: string; m: string };
export type StagedManifest = {
  bookVersion: string;
  totalPages: number;
  minShellBuild?: number;
  pagePadWidth?: number;
  files: FileEntry[];
};

export type InjectedFs = {
  stat: (p: string) => Promise<{ size: number } | null>;
  exists: (p: string) => Promise<boolean>;
  mkdirp: (p: string) => Promise<void>;
  rmrf: (p: string) => Promise<void>;
  move: (from: string, to: string) => Promise<void>;
  readJson: (p: string) => Promise<unknown>;
  writeJson: (p: string, v: unknown) => Promise<void>;
  walkWithHashes: (dir: string) => Promise<Map<string, { size: number; md5: string }>>;
};

export type InjectedNet = {
  fetchJson: (url: string) => Promise<any>;
  download: (url: string, dest: string) => Promise<void>;
};

export const ALLOWED_HOSTS: string[];
export const BOOK_VERSION_RE: RegExp;
export const STAGED_READY_TTL_MS: number;
export const LIVE_INTERNET_WINDOW_MS: number;
export const ROOM_ACTIVE_WINDOW_MS: number;
export const STAGGER_WINDOW_MIN: number;
export const MIN_CODE_DISTANCE: number;

export const parseBookUpdate: (resp: unknown) => BookUpdatePointer | null;
export const staggerDelayMs: (deviceId: string) => number;
export const shouldStage: (ctx: ShouldStageContext) => { stage: boolean; reason: string };
export const verifyStaged: (
  manifest: StagedManifest | null,
  onDisk: Map<string, { size: number; md5: string }>,
  activeTotalPages?: number,
) => { ok: boolean; problems: string[] };
export const canApplyNow: (ctx: CanApplyContext) => { ok: boolean; reason: string };
export const levenshtein: (a: string, b: string) => number;
export const stageBook: (opts: {
  base: string;
  bookVersion: string;
  fs: InjectedFs;
  net: InjectedNet;
  now?: () => number;
  activeTotalPages?: number;
  shellBuild?: number;
  freeDiskBytes?: number | null;
  stagedDir?: string;
  concurrency?: number;
  onProgress?: (done: number, total: number) => void;
}) => Promise<StagedRecord>;
export const applyStagedBundle: (opts: {
  fs: InjectedFs;
  live?: string;
  staged?: string;
  prevTmp?: string;
}) => Promise<{ ok: boolean; stage: string }>;
