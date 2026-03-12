import test from "node:test";
import assert from "node:assert/strict";

import {
  canScheduleCompletionNotification,
  canWriteToPhotoLibrary,
  shouldPromptForNotificationPermission,
  shouldPromptForPhotoLibraryWritePermission,
} from "../src/permissionFlow.js";

test("notification permission helper prompts only when enableNotifications is true", () => {
  assert.equal(
    shouldPromptForNotificationPermission({
      enableNotifications: false,
      existingStatus: "undetermined",
      canAskAgain: true,
    }),
    false
  );

  assert.equal(
    shouldPromptForNotificationPermission({
      enableNotifications: true,
      existingStatus: "undetermined",
      canAskAgain: true,
    }),
    true
  );

  assert.equal(
    shouldPromptForNotificationPermission({
      enableNotifications: true,
      existingStatus: "denied",
      canAskAgain: false,
    }),
    false
  );

  assert.equal(
    shouldPromptForNotificationPermission({
      enableNotifications: true,
      existingStatus: "granted",
      canAskAgain: true,
    }),
    false
  );
});

test("completion notification scheduling requires opt-in, granted permission, and background app state", () => {
  assert.equal(
    canScheduleCompletionNotification({
      notificationsEnabled: true,
      notificationPermissionStatus: "granted",
      appIsActive: false,
    }),
    true
  );

  assert.equal(
    canScheduleCompletionNotification({
      notificationsEnabled: false,
      notificationPermissionStatus: "granted",
      appIsActive: false,
    }),
    false
  );

  assert.equal(
    canScheduleCompletionNotification({
      notificationsEnabled: true,
      notificationPermissionStatus: "denied",
      appIsActive: false,
    }),
    false
  );

  assert.equal(
    canScheduleCompletionNotification({
      notificationsEnabled: true,
      notificationPermissionStatus: "granted",
      appIsActive: true,
    }),
    false
  );
});

test("photos save flow requests write permission only when needed", () => {
  const notGrantedYet = { granted: false, canAskAgain: true, accessPrivileges: "none" };
  assert.equal(shouldPromptForPhotoLibraryWritePermission(notGrantedYet), true);

  const granted = { granted: true, canAskAgain: true, accessPrivileges: "all" };
  assert.equal(canWriteToPhotoLibrary(granted), true);
  assert.equal(shouldPromptForPhotoLibraryWritePermission(granted), false);

  const limited = { granted: false, canAskAgain: false, accessPrivileges: "limited" };
  assert.equal(canWriteToPhotoLibrary(limited), true);
  assert.equal(shouldPromptForPhotoLibraryWritePermission(limited), false);

  const denied = { granted: false, canAskAgain: false, accessPrivileges: "none" };
  assert.equal(canWriteToPhotoLibrary(denied), false);
  assert.equal(shouldPromptForPhotoLibraryWritePermission(denied), false);
});
