# Mixterious iOS Permission Matrix

Date: 2026-02-19
Scope: Wave 1 / Block 3 of 4 / Tab 2 (iOS privacy + permissions)

## Prompted iOS Permissions

| Permission | iOS Key | Trigger Surface | Prompt Timing | Required for Core Use? | If Denied |
| --- | --- | --- | --- | --- | --- |
| Notifications (optional) | `NSUserNotificationsUsageDescription` | App launch (first run only); user can later manage via Feedback tab > Background Notifications | App-launch prompt on first-ever start only. | No | App continues processing; no completion alerts in background unless permission is granted. In-app copy links users to iOS Settings. |
| Photos Add-Only | `NSPhotoLibraryAddUsageDescription` | Video tab > Save | Contextual only (user taps Save). Uses write-only permission request (`MediaLibrary.requestPermissionsAsync(true)`). | No | Save action shows error state and app remains usable. |

## Non-Prompted Sensitive Areas (Current Build)

| Capability | Prompted? | Notes |
| --- | --- | --- |
| Camera | No | Not used in this build. |
| Microphone | No | Not used in this build. |
| Contacts | No | Not used in this build. |
| Location | No | Not used in this build. |
| Tracking (ATT) | No | `NSPrivacyTracking=false`; no tracking flow. |

## Runtime Guardrails Implemented

- Notification permission is prompted one time on first launch and tracked in local storage to avoid repeat prompts.
- Background completion notification scheduling now requires all of: terminal job state, app in background, user opt-in enabled, notification permission granted.
- Photos save path now requests write-only media permission and does not require full-library read access.
- Feedback tab contains explicit background notification controls and iOS Settings recovery path when denied.
