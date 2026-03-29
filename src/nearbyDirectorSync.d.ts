export function isNearbyDirectorSyncAvailable(): boolean;
export function startNearbyDirector(sessionCode: string): Promise<Record<string, unknown>>;
export function startNearbyFollower(sessionCode: string): Promise<Record<string, unknown>>;
export function stopNearbyDirectorSync(): Promise<Record<string, unknown> | null>;
export function sendNearbyDirectorPageUpdate(page: number, totalPages?: number): Promise<Record<string, unknown> | null>;
export function addNearbyDirectorSyncListener(listener: (event: {
  type?: string;
  role?: string;
  sessionCode?: string;
  status?: string;
  peerCount?: number;
  directorCount?: number;
  message?: string;
  code?: string;
  page?: number;
  totalPages?: number;
}) => void): { remove(): void };
