# Signo Vivo Director/Follower Master Requirements and Launch Readiness Doc

## Summary

This document is the **master handoff artifact** for fixing and validating nearby Director/Follower sync in Signo Vivo.

It combines three things into one decision-ready reference:

1. the core **happy-path product requirements** for **1 Director and 10 Followers**
2. the **real-device recovery and UX requirements** derived from observed screenshots and field behavior
3. the **quality gate**, including automated tests and launch-readiness expectations, that must be satisfied before the work can be considered complete or ready for App Store launch

This document is intended for another Codex or Claude instance to use as the source of truth for implementation and validation.

This is a **requirements and readiness doc**, not an implementation diary.

## Product Goal

Signo Vivo should allow one Director to control nearby reading state for up to at least ten Followers in a way that feels:

- simple
- calm
- obvious
- non-technical
- dependable during real choir use

The target users are older, non-technical choir members. If the system technically works but feels confusing, fragile, or recovery-heavy, it does not meet this document.

## Primary Scope

This document covers the intended nearby-sync experience when:

- there is exactly **1 Director**
- there are exactly **10 Followers**
- all devices are in the same room
- the app is open and in the foreground
- local device-to-device connectivity is available
- all participating devices are supposed to belong to the same sync session

This document also covers the **normal in-app recovery experience** when the happy path temporarily fails, but only to the extent needed to restore the user back into the intended nearby-sync flow.

## Core Definitions

### Director

The one device that controls the reading state for the session.

### Follower

A device that joins the Director and mirrors the Director's current reading state.

### Current Reading State

The minimum synchronized state that Followers must match from the Director:

- relevant app mode
- active book or document
- current page
- total pages if needed by the UI

### Recovery Flow

The user-facing reconnect or reset path used when a Follower is not properly syncing with the Director.

Recovery exists to restore the happy path. Recovery is not allowed to become the main workflow.

## Master Product Requirements

### 1. Happy-Path Sync Requirements

The app must satisfy all of the following in the normal one-Director, ten-Follower flow:

- The Director can enter and remain in the controlling role.
- All 10 Followers can join the Director's session.
- Followers connect automatically or with only the minimum already-defined product interaction.
- Late-joining Followers catch up to the Director's current state.
- When the Director changes pages, all connected Followers move to the same page.
- Followers must not drift to the wrong page, wrong book, wrong mode, wrong cover, or stale content.
- The Director experience must remain responsive while broadcasting updates.
- The happy path must not require users to restart the app, reinstall the app, or repeatedly press reconnect controls.

### 2. User Experience Requirements

The app must feel understandable to non-technical choir members.

Requirements:

- Sync should feel like part of normal reading, not like a networking tool.
- Users should not need to understand Bluetooth, Wi-Fi, Local Network, peer discovery, sessions, or transport failures.
- Connected state must be visually and linguistically understandable.
- Searching or connecting state must be understandable and not alarming.
- Any instructional copy must be simple, truthful, and non-technical.

### 3. Recovery and Reconnect Requirements

Recovery must exist, but it must be calm and minimal.

Requirements:

- Recovery must be a **single-flow experience**.
- Only one blocking recovery surface may be shown at a time.
- The app must never require the user to guess whether reconnect or reset worked.
- Recovery must resolve into one of three explicit states:
  - connected and following the Director
  - still trying, with one clear next action
  - could not recover, with one simple and truthful next action
- Reset or reconnect actions must preserve persistent user data and content.
- Recovery must not become the normal expected path for ordinary choir use.

### 4. Real-Device UX Requirements From Field Evidence

The following observed states from real-device screenshots are treated as product defects until proven otherwise.

#### Observed Failure A: Blocking `Restableciendo...` spinner over live content

Observed state:

- A blocking reset spinner appears over the reader.

Required behavior:

- The spinner must always lead to a clear end state.
- The user must never be left with only a spinner and no explicit result.
- A reset must not feel endless, ambiguous, or silent.

#### Observed Failure B: Reset confirmation stacked on top of sync modal

Observed state:

- A system alert appears on top of the custom `SINCRONIZACIÓN DE GRUPO` modal.

Why this is harmful:

- It creates layered UI and forces the user to interpret two simultaneous flows.

Required behavior:

- The recovery flow must present only one active dialog or blocking layer at a time.
- If reset is offered from the sync modal, the visible UI must transition cleanly rather than stack.

#### Observed Failure C: Misleading Local Network permission guidance

Observed state:

- The app tells the user to go to Settings and enable Local Network or reinstall the app.
- The provided Settings screenshot shows no obvious Local Network toggle for SignoVivo.

Required behavior:

- Permission instructions must match what the user can realistically find on device.
- The app must not tell the user to enable a setting that is not visibly available through the path it describes.
- Reinstall guidance must not appear in the normal recovery flow.

#### Observed Failure D: Keyboard or accessory strip appears unexpectedly

Observed state:

- A bottom keyboard or accessory strip appears while the sync modal is open.

Why this is harmful:

- It breaks iPad layout, obscures the reader, and makes the app feel unstable or unfinished.

Required behavior:

- Hidden focus behavior must not summon keyboard UI unexpectedly.
- The sync modal must look intentional and complete on iPad.

#### Observed Failure E: Reader returns with no clear proof that sync is restored

Observed state:

- Recovery UI can disappear and leave the user back on the reading screen without clear confirmation of connection state.

Required behavior:

- When recovery ends, the user must know whether they are connected, still trying, or not recovered.
- Returning to the reader alone is not sufficient proof of success.

### 5. Hard “Must Not Happen” Rules

The finished app must not do any of the following:

- show nested alert-plus-modal recovery UI
- end a recovery attempt with spinner-only ambiguity
- rely on hidden input focus that triggers stray keyboard or accessory UI
- tell users to enable a Local Network toggle unless that path has been verified on real devices
- instruct users to uninstall or reinstall as part of ordinary recovery
- return the user to the reader without explicit sync-state clarity
- require repeated reconnect tapping as the normal way to stay in sync

## Acceptance Criteria

The implementation should be considered correct only if all of the following are true.

### Sync correctness

- One Director can successfully start and remain the controlling device.
- Ten Followers can join the same session.
- All ten Followers match the Director's page, book, and relevant mode.
- Late-joining Followers catch up to the Director's current state.
- Sequential page changes remain ordered and consistent.
- No connected Follower stays stuck on an old page while the Director continues moving.

### Recovery correctness

- Triggering reconnect or reset does not produce stacked recovery UI.
- Reset and reconnect always end in an explicit visible state.
- Recovery does not wipe persistent settings or content.
- Recovery guidance is truthful and understandable.
- The sync modal and recovery UI do not accidentally summon a bottom keyboard or accessory strip on iPad.

### User comprehension

- A non-technical user can understand whether the app is connected, trying, or failed.
- A non-technical user can recover from ordinary sync trouble without interpreting technical jargon.

## Required Automated Test Gate

The task is **not done** until all required automated tests pass.

No category below is optional. No failing test may be ignored, skipped, or hand-waved away as “close enough.”

### Unit tests must cover

- sync state transitions
- reconnect press counting and 25-second escalation logic
- duplicate-reset prevention
- stale callback rejection and generation or token guards
- page update ordering
- late-join state application
- mode, book, and page mapping logic
- recovery outcome state selection
- modal visibility coordination so only one recovery surface can be active
- focus behavior around sync input and modal presentation

### Integration tests must cover

- JS-to-native sync flow
- Director session start
- Follower reconnect flow
- soft reset flow
- late-join snapshot delivery
- ordered page propagation across connected Followers
- permission failure handling and resulting user-visible copy
- persistent-versus-volatile state boundaries during reset
- coordination between sync modal, reconnect UI, reset UI, and completion state

### End-to-end automated tests must cover

- one Director and one Follower happy path
- one Director and a late-joining Follower
- reconnect button first press behavior
- reconnect button second press behavior
- third-press escalation behavior
- reset confirmation cancel path
- reset confirmation confirm path
- reset completion returning to the correct state
- Director activation blocked by permission failure
- sync modal layout on iPad
- no stacked recovery surfaces
- no ambiguous post-recovery return to reader
- repeated reset taps do not create concurrent resets or crashes

### Regression tests must explicitly protect against

- Follower stuck on old page
- wrong book or wrong mode after reconnect
- spinner hangs without terminal state
- stacked modal and alert recovery UI
- misleading permission messaging
- unexpected keyboard or accessory bar over sync UI
- stale session callbacks corrupting new session state
- recovery appearing to finish without actually restoring clarity

## Required Physical-Device Validation Gate

Automated green is necessary but not sufficient.

Before the work can be considered complete, physical-device validation must confirm:

- one real Director and at least one real Follower can sync correctly
- Local Network and related permission behavior is correct on real iPad hardware
- the sync modal and recovery UI look correct on real iPad layout
- reconnect and reset restore usable behavior after stale or broken nearby-session state

For higher confidence, the intended target remains:

- one real Director and ten real Followers in the same room

## Definition of Done

The work is done only when all of the following are true:

1. the app satisfies the happy-path one-Director, ten-Follower requirements
2. the app satisfies the screenshot-backed recovery and UX requirements
3. all required automated unit, integration, end-to-end, and regression tests pass
4. no required test remains flaky, skipped, or unresolved without explicit written justification
5. physical-device validation confirms nearby-sync behavior and recovery UX
6. the app feels understandable to non-technical choir members
7. the app does not require manual app restart, reinstall, or technical troubleshooting in the normal intended flow

## High-Level App Store Launch Checklist

Use this as the final high-level launch-readiness checklist.

- [ ] One Director and ten Followers work reliably in the intended real-world room setup.
- [ ] Followers consistently match the Director's page, book, and mode.
- [ ] Late joiners catch up correctly.
- [ ] Recovery exists but is rarely needed during ordinary use.
- [ ] Recovery UI is simple, single-layer, truthful, and understandable.
- [ ] The app never leaves users guessing whether sync is working.
- [ ] Permission guidance matches what users can actually see and do on real iPads.
- [ ] No reinstall or manual restart is required for ordinary usage or ordinary in-app recovery.
- [ ] iPad layout is stable and polished, including sync modal and recovery flows.
- [ ] No stray keyboard, accessory bar, or broken modal layering appears in sync flows.
- [ ] Automated unit, integration, regression, and end-to-end suites all pass.
- [ ] Real-device validation passes on the nearby-sync flows that matter most.
- [ ] The app is calm, reliable, and understandable enough that a non-technical choir member could use it without coaching.
