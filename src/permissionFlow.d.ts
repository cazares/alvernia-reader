export type PermissionStatus = "undetermined" | "denied" | "granted" | "provisional" | "ephemeral";

export function isPermissionGranted(status?: string | null): boolean;

export function shouldPromptForNotificationPermission(options?: {
  enableNotifications?: boolean;
  existingStatus?: string | null;
  canAskAgain?: boolean;
}): boolean;

export function canScheduleCompletionNotification(options?: {
  notificationsEnabled?: boolean;
  notificationPermissionStatus?: string | null;
  appIsActive?: boolean;
}): boolean;

export function canWriteToPhotoLibrary(permission?: {
  granted?: boolean;
  accessPrivileges?: string | null;
}): boolean;

export function shouldPromptForPhotoLibraryWritePermission(permission?: {
  granted?: boolean;
  accessPrivileges?: string | null;
  canAskAgain?: boolean;
}): boolean;
