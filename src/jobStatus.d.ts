export type NormalizedJobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "unknown";

export function normalizeJobStatus(status?: string | null): NormalizedJobStatus;
export function isJobInProgress(status?: string | null): boolean;
export function isJobSucceeded(status?: string | null): boolean;
export function isJobFailed(status?: string | null): boolean;
export function isJobCancelled(status?: string | null): boolean;
export function isJobTerminal(status?: string | null): boolean;
export function shouldKeepProcessingModalOpen(
  status?: string | null,
  hasOutputUrl?: boolean
): boolean;
