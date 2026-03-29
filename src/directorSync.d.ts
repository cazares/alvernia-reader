export const DEFAULT_DIRECTOR_SYNC_ENDPOINT: string;
export const DEFAULT_DIRECTOR_HEARTBEAT_MS: number;
export const DEFAULT_DIRECTOR_SYNC_TIMEOUT_MS: number;
export function normalizeSyncSessionCode(value?: string): string;
export function createDefaultSyncSessionCode(): string;
export function createDirectorKey(): string;
export function resolveDirectorSyncEndpoint(extra?: Record<string, unknown>): string;
export function readDirectorSyncState(input: {
  endpoint: string;
  sessionCode: string;
}): Promise<null | {
  session: string;
  page?: number;
  totalPages?: number;
  updatedAt?: string;
  directorPresent?: boolean;
  waitingForDirector?: boolean;
  staleDirectorExpired?: boolean;
}>;
export function publishDirectorSyncState(input: {
  endpoint: string;
  sessionCode: string;
  directorKey: string;
  page: number;
  totalPages?: number;
}): Promise<{
  session: string;
  page: number;
  totalPages: number;
  updatedAt: string;
  directorPresent?: boolean;
}>;
export function releaseDirectorSyncState(input: {
  endpoint: string;
  sessionCode: string;
  directorKey: string;
}): Promise<null | {
  released: boolean;
  session: string;
}>;
