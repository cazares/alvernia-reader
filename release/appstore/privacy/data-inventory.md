# Mixterious Data Inventory (iOS)

Date: 2026-02-19
Scope: Wave 1 / Block 3 of 4 / Tab 2

## Summary

Mixterious does not use ad SDKs and does not enable tracking. User data handling is limited to karaoke job processing, optional feedback submission, and local UX preferences.

## Data Elements

| Data Element | Source | Stored Where | Sent Off-Device | Purpose |
| --- | --- | --- | --- | --- |
| Song query text | User input (`Search` tab) | In-memory during session | Yes (`/jobs` API call) | Create karaoke processing job. |
| Selected source video ID | User source picker action | In-memory during session | Yes (`/jobs` API call with `audio_id`) | Improve lyric/audio match reliability. |
| Generated media files | Backend + client download | App cache; optional user Photos library save | Download from backend; user-controlled share/save | Playback, save, share generated karaoke output. |
| Feedback text | User input (`Feedback` tab) | In-memory until submit | Yes (`/feedback`) | User support + product quality improvements. |
| Feedback diagnostics | App runtime (`device model`, iOS version, app version/build, active tab, job status/stage, client logs) | In-memory until submit | Yes (`/feedback`) | Triage and debugging for user-reported issues. |
| Rating seen state + device keys | Generated device/app identifiers and local state | `AsyncStorage` + `SecureStore` | Yes (`/rating/state`, `/rating/mark`) | Prevent repeated rating prompts and sync seen state. |
| UX preferences (`offset`, `mix levels`, `advanced open`, notification opt-in) | User settings/actions | `AsyncStorage` | No | Preserve local app behavior between launches. |

## Privacy/Manifest Alignment Notes

- `PrivacyInfo.xcprivacy` remains aligned with runtime behavior:
  - `NSPrivacyAccessedAPICategoryUserDefaults` maps to local preference/state persistence (`AsyncStorage`).
  - `NSPrivacyAccessedAPICategoryFileTimestamp` maps to file cache/download/save operations.
  - `NSPrivacyAccessedAPICategoryDiskSpace` and `NSPrivacyAccessedAPICategorySystemBootTime` are used by Expo/React Native runtime internals in the shipped stack.
- `NSPrivacyCollectedDataTypes` remains empty and `NSPrivacyTracking` remains `false`.

## Not Collected in Current Build

- Contacts
- Location
- Camera or microphone recordings
- Advertising identifiers for tracking
