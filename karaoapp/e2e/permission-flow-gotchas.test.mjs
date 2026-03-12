import test from "node:test";
import assert from "node:assert/strict";

import {
  canScheduleCompletionNotification,
  canWriteToPhotoLibrary,
  isPermissionGranted,
  shouldPromptForNotificationPermission,
  shouldPromptForPhotoLibraryWritePermission,
} from "../src/permissionFlow.js";

test("isPermissionGranted is case-insensitive and whitespace tolerant", () => {
  assert.equal(isPermissionGranted("granted"), true);
  assert.equal(isPermissionGranted(" GRANTED "), true);
  assert.equal(isPermissionGranted("denied"), false);
  assert.equal(isPermissionGranted(undefined), false);
});

test("shouldPromptForNotificationPermission requires opt-in and prompt availability", () => {
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
      existingStatus: "undetermined",
      canAskAgain: false,
    }),
    false
  );
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
      existingStatus: "granted",
      canAskAgain: true,
    }),
    false
  );
});

test("canScheduleCompletionNotification requires enabled + granted + backgrounded app", () => {
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
      notificationsEnabled: true,
      notificationPermissionStatus: "granted",
      appIsActive: true,
    }),
    false
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
});

test("canWriteToPhotoLibrary allows limited access privileges", () => {
  assert.equal(canWriteToPhotoLibrary({ granted: true, accessPrivileges: "all" }), true);
  assert.equal(canWriteToPhotoLibrary({ granted: false, accessPrivileges: "limited" }), true);
  assert.equal(canWriteToPhotoLibrary({ granted: false, accessPrivileges: "none" }), false);
});

test("shouldPromptForPhotoLibraryWritePermission only prompts when write access missing and prompt still possible", () => {
  assert.equal(
    shouldPromptForPhotoLibraryWritePermission({
      granted: false,
      accessPrivileges: "none",
      canAskAgain: true,
    }),
    true
  );
  assert.equal(
    shouldPromptForPhotoLibraryWritePermission({
      granted: false,
      accessPrivileges: "none",
      canAskAgain: false,
    }),
    false
  );
  assert.equal(
    shouldPromptForPhotoLibraryWritePermission({
      granted: false,
      accessPrivileges: "limited",
      canAskAgain: true,
    }),
    false
  );
});
