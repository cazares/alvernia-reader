export function isNearbyDirectorSyncAvailable(): boolean;
export function startNearbyDirector(sessionCode: string): Promise<Record<string, unknown>>;
export function startNearbyFollower(sessionCode: string): Promise<Record<string, unknown>>;
export function stopNearbyDirectorSync(): Promise<Record<string, unknown> | null>;
export function resetNearbyDirectorSync(): Promise<Record<string, unknown> | null>;
export function primeNearbyPermissions(): Promise<Record<string, unknown> | null>;
export function refreshNearbyDiscovery(): Promise<Record<string, unknown> | null>;
export function requestCurrentSnapshot(): Promise<Record<string, unknown> | null>;
export function requestDirectorTakeover(): Promise<{ requestId?: string } | null>;
export function approveDirectorTakeover(requestId: string): Promise<Record<string, unknown> | null>;
export function denyDirectorTakeover(requestId: string): Promise<Record<string, unknown> | null>;
export function sendNearbyDirectorPageUpdate(
  page: number,
  totalPages?: number,
  context?: { mode?: "standard" | "nonStandard"; bookId?: string },
): Promise<Record<string, unknown> | null>;
export function addNearbyDirectorSyncListener(listener: (event: {
  type?: string;
  role?: string;
  sessionCode?: string;
  status?: string;
  peerCount?: number;
  directorCount?: number;
  message?: string;
  code?: string;
  requestId?: string;
  requesterName?: string;
  page?: number;
  totalPages?: number;
  mode?: "standard" | "nonStandard";
  bookId?: string;
}) => void): { remove(): void };
