# Wave 1 / Block 3 / Tab 2 Status

Date: 2026-02-19
Branch: `dev-appstore-wave1-ios-privacy`

## Update (2026-02-21)

- Feedback-tab `Legal & Privacy` link section was removed from app UI.
- Notification permission flow was changed to request on first app launch (one-time), while keeping background notification scheduling gated behind explicit in-app opt-in + granted permission + background state.
- Removed now-unused legal-link runtime test file (`e2e/legal-links.test.mjs`).

## PASS

- Removed launch-time notification permission request from `App.tsx`.
- Added contextual notification opt-in flow in app UI (Feedback tab) and gated scheduling logic behind explicit opt-in + granted permission + background state.
- Updated Photos save flow to write-only media permission request and improved denied-state UX copy.
- Added in-app `Legal & Privacy` section with links for:
  - Privacy Policy
  - Terms of Use
  - Support
- Added lightweight runtime tests for:
  - Permission trigger/scheduling logic (`e2e/permission-flow.test.mjs`)
  - Legal-link model/render source (`e2e/legal-links.test.mjs`)
- Added privacy docs:
  - `release/appstore/privacy/permission-matrix.md`
  - `release/appstore/privacy/data-inventory.md`
- Verified `ios/Mixterious/PrivacyInfo.xcprivacy` remains consistent with runtime behavior; no manifest changes required in this pass.

## BLOCKED / ATTENTION

- The exact requested command below fails in this environment:
  - `npm -C /Users/cazares/Documents/karaoke-time-by-miguel/karaoapp exec -- tsc -p tsconfig.json --noEmit`
  - Failure: `TS5058: The specified path does not exist: 'tsconfig.json'`
- Stable workaround is now available via package script:
  - `npm -C karaoapp run typecheck`

## Validation Run Log

- `npm -C /Users/cazares/Documents/karaoke-time-by-miguel/karaoapp ci`:
  - PASS after clearing a corrupted prior `node_modules` state by rotating old directory out of workspace path.
- `npm -C /Users/cazares/Documents/karaoke-time-by-miguel/karaoapp exec -- tsc -p tsconfig.json --noEmit`:
  - BLOCKED as noted above.
- `npm -C karaoapp run typecheck`:
  - PASS.
- `npm -C /Users/cazares/Documents/karaoke-time-by-miguel/karaoapp run test:e2e`:
  - PASS (12/12 tests).

## Concrete Follow-Ups

1. Replace placeholder legal URLs with production URLs before App Store submission.
2. Optional cleanup: remove `MODULE_TYPELESS_PACKAGE_JSON` warnings by standardizing module type for Node test helper files.
