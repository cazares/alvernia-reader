const SUCCESS_STATUSES = new Set(["succeeded", "completed", "complete", "success"]);
const FAILED_STATUSES = new Set(["failed", "error"]);
const CANCELLED_STATUSES = new Set(["cancelled", "canceled"]);
const RUNNING_STATUSES = new Set(["queued", "running", "processing", "starting", "in_progress", "partial_ready"]);

const normalize = (status) => String(status || "").trim().toLowerCase();

export const normalizeJobStatus = (status) => {
  const value = normalize(status);
  if (!value) return "unknown";
  if (SUCCESS_STATUSES.has(value)) return "succeeded";
  if (FAILED_STATUSES.has(value)) return "failed";
  if (CANCELLED_STATUSES.has(value)) return "cancelled";
  if (RUNNING_STATUSES.has(value)) {
    if (value === "queued") return "queued";
    return "running";
  }
  return "unknown";
};

export const isJobInProgress = (status) => {
  const normalized = normalizeJobStatus(status);
  return normalized === "queued" || normalized === "running";
};

export const isJobSucceeded = (status) => normalizeJobStatus(status) === "succeeded";
export const isJobFailed = (status) => normalizeJobStatus(status) === "failed";
export const isJobCancelled = (status) => normalizeJobStatus(status) === "cancelled";

export const isJobTerminal = (status) => {
  const normalized = normalizeJobStatus(status);
  return normalized === "succeeded" || normalized === "failed" || normalized === "cancelled";
};

export const shouldKeepProcessingModalOpen = (status, hasOutputUrl) => {
  if (hasOutputUrl) return false;
  return isJobInProgress(status);
};
