const normalizePermissionStatus = (status) => String(status || "").trim().toLowerCase();

export const isPermissionGranted = (status) => normalizePermissionStatus(status) === "granted";

export const shouldPromptForNotificationPermission = ({
  enableNotifications,
  existingStatus,
  canAskAgain = true,
} = {}) =>
  Boolean(enableNotifications && !isPermissionGranted(existingStatus) && canAskAgain !== false);

export const canScheduleCompletionNotification = ({
  notificationsEnabled,
  notificationPermissionStatus,
  appIsActive,
} = {}) => Boolean(notificationsEnabled && isPermissionGranted(notificationPermissionStatus) && appIsActive === false);

export const canWriteToPhotoLibrary = (permission = {}) =>
  Boolean(permission?.granted || normalizePermissionStatus(permission?.accessPrivileges) === "limited");

export const shouldPromptForPhotoLibraryWritePermission = (permission = {}) =>
  Boolean(!canWriteToPhotoLibrary(permission) && permission?.canAskAgain !== false);
