# Signo Vivo Director/Follower Happy-Path Requirements

## Purpose

This document defines the **basic happy path only** for nearby sync between:

- **1 Director**
- **10 Followers**

The goal is to give another coding agent a clear target for implementation and bug-fixing work. If the app behavior does not match this document, that behavior should be treated as a bug unless it is explicitly listed as out of scope.

This is a **requirements document**, not an implementation plan.

## Scope

This document covers only the normal, intended experience when:

- all 11 devices are in the same physical area
- the app is open and in the foreground on all devices
- local device-to-device connectivity is available
- there is exactly one valid Director for the session
- the 10 Followers are supposed to mirror the Director
- all devices are participating in the same nearby sync session

This document does **not** define conflict resolution, edge cases, recovery from corruption, or multi-director behavior except where needed to protect the happy path.

## Product Intent

Signo Vivo should feel simple and reliable for non-technical choir members. In the happy path:

- the Director controls the reading experience
- Followers automatically stay in sync with the Director
- Followers do not need to understand networking
- Followers do not need to manually troubleshoot in order to remain synced
- the system should feel calm, obvious, and dependable

## Definitions

### Director

The single device that controls page changes for the session.

### Follower

A device that joins the Director's nearby session and mirrors the Director's current reading state.

### Session

A single nearby sync group containing exactly one Director and up to at least 10 Followers.

### Current Reading State

The minimum state Followers must mirror from the Director:

- current app mode relevant to reading
- current book/document selection
- current page number
- total page count when needed by the UI

## Required Happy-Path Outcome

When one Director and ten Followers are used in the same room, the system must support this basic flow:

1. The Director opens the app and becomes the active Director.
2. Ten Followers open the app and join that Director automatically, or with the minimum required taps already defined by the product.
3. Once joined, all ten Followers display the same reading content as the Director.
4. When the Director changes pages, all ten Followers move to the same page.
5. When a Follower joins late, it should land on the Director's current page rather than starting from an old page or blank state.
6. The entire flow should work without requiring users to quit and relaunch the app.

## Functional Requirements

### FR-1: Exactly One Controlling Director

For the happy path, there must be exactly one active Director controlling the session.

Requirements:

- The Director must be able to start a session successfully.
- Followers must connect to that Director, not remain idle when a valid Director is available.
- The happy path must not require users to understand or choose among multiple Directors.

### FR-2: Support at Least 10 Followers

The app must support one Director with **10 simultaneous Followers** in the same session.

Requirements:

- All 10 Followers must be able to join the same Director session.
- All 10 Followers must remain eligible to receive updates while connected.
- The 10th Follower must work just as reliably as the 1st Follower in the happy path.

### FR-3: Automatic or Near-Automatic Connection

Joining the Director should feel simple.

Requirements:

- Followers should connect automatically when the app is in the intended follower flow and a valid Director is available nearby.
- If any user action is required, it must be minimal, obvious, and consistent across all Followers.
- Users should not need to understand Bluetooth, Wi-Fi, local networking, sessions, peer discovery, or troubleshooting concepts during the happy path.

### FR-4: Late Joiners Receive Current State

A Follower that connects after the session is already active must immediately catch up.

Requirements:

- A newly connected Follower must receive the Director's current reading state as soon as connection is established.
- The Follower must land on the Director's current page, not page 1, not a cached old page, and not a loading dead-end.
- The Follower must also match the Director's active mode and active book/document when relevant.

### FR-5: Page Changes Propagate to All Followers

When the Director changes pages, Followers must mirror the change.

Requirements:

- Every page turn initiated by the Director must be sent to all connected Followers.
- All connected Followers must end up on the same page as the Director.
- Sequential page changes must remain in order.
- Followers must not become stuck on an older page while the Director continues moving.

### FR-6: State Consistency Across the Session

All connected Followers must match the Director's reading state.

Requirements:

- Followers must show the same page as the Director.
- Followers must show the same book/document as the Director.
- Followers must show the same relevant app mode as the Director when that mode affects reading behavior.
- Followers must not silently drift into a different book, different mode, wrong cover, wrong placeholder screen, or stale page.

### FR-7: Stable Connected Experience

Once connected, Followers should remain stably connected during ordinary choir use.

Requirements:

- The app must sustain the session while the app stays open in the foreground.
- Normal page turns from the Director must continue reaching connected Followers without requiring manual repair.
- The happy path must not depend on the user repeatedly pressing refresh or restart controls.

### FR-8: Follower UI Must Be Understandable

Followers must have a simple, non-technical understanding of what is happening.

Requirements:

- If a Follower is successfully connected, the UI should make that clear in plain language or obvious visual state.
- If a Follower is searching or connecting, that state should be understandable and not alarming.
- The happy path should not expose jargon-heavy status messaging.

### FR-9: Director UI Must Stay Responsive

The Director must remain easy to use while broadcasting updates.

Requirements:

- Page changes on the Director must feel immediate.
- Starting or running a session must not block the Director from reading or navigating.
- The Director should not have to wait on each Follower one by one before continuing normal use.

## User Experience Requirements

### UX-1: Non-Technical Choir Member Friendly

The intended users are older, non-technical choir members.

Requirements:

- Core sync behavior must feel automatic.
- Wording should be simple and reassuring.
- The happy path must avoid asking users to diagnose networking problems.
- The app should behave as though syncing is just part of normal reading, not a separate technical workflow.

### UX-2: No Manual App Restart Required

The happy path is not acceptable if users must relaunch the app to make sync work.

Requirements:

- A normal session with one Director and ten Followers must work without users killing and reopening the app.
- If the system is working correctly, refresh or reset actions should be unnecessary during normal use.

### UX-3: Fast Catch-Up

When a Follower joins or rejoins normally, it should catch up quickly enough to feel immediate in real usage.

Requirements:

- The Follower should reach the Director's current reading state promptly after joining.
- The user should not sit on a stale or ambiguous screen for an extended time while the session is otherwise healthy.

## Performance and Reliability Requirements

These targets define the happy-path bar. They do not need to describe internal architecture.

### PR-1: Connectability

- One Director and 10 Followers must be able to form a session successfully in a normal rehearsal environment.

### PR-2: Update Delivery

- Director page changes must reach all connected Followers reliably enough that the group stays together in practice.

### PR-3: Ordering

- Followers must apply the newest valid Director update in the correct order.
- Older or stale updates must not overwrite newer valid session state.

### PR-4: Late Callback Safety

- Old transport objects, old callbacks, or stale session artifacts must not corrupt the current active session.

## Preconditions for Acceptance Testing

The following assumptions define the happy-path test setup:

- 11 physical iOS devices are available
- all devices run the same intended app build unless a test explicitly says otherwise
- the app is already installed on all devices
- all devices are in the same room
- the app is in the foreground on all devices
- the devices have whatever local connectivity permissions are required by the product
- there is exactly one Director and exactly ten Followers in the test

## Acceptance Criteria

The implementation should be considered correct only if all of the following are true.

### AC-1: Director Session Starts

- The Director can enter the happy-path session and become the controlling device.

### AC-2: All 10 Followers Join

- Ten Followers can join the same Director session in one run.
- None of the ten Followers are left permanently searching when the Director is valid and nearby.

### AC-3: Initial State Matches

- After connection, all ten Followers match the Director's current book/document, mode, and page.

### AC-4: Page Sync Works Repeatedly

- If the Director moves through several pages in sequence, all ten Followers continue matching the Director.

### AC-5: Late Joiner Catches Up

- If one Follower joins after the Director is already on a later page, that Follower lands on the Director's current page shortly after connecting.

### AC-6: No Stuck Followers

- During the happy path, no connected Follower remains stuck on an old page while the Director keeps changing pages.

### AC-7: No Manual Restart Needed

- The happy path succeeds without requiring users to force-quit, soft reset, or reinstall the app.

### AC-8: Session Feels Simple

- A non-technical user can participate in the happy path without needing technical explanation of the connection system.

## Manual Test Script

Another agent can use this exact script as the target validation pass.

1. Prepare 11 physical devices in the same room.
2. Put 1 device into Director mode.
3. Put 10 devices into the normal Follower flow.
4. Confirm that all 10 Followers connect to the Director.
5. On the Director, navigate to a page in the middle of the book.
6. Confirm that all 10 Followers move to that same page.
7. On the Director, perform at least 10 additional page changes, including both forward and backward movement.
8. Confirm after each change that all 10 Followers match the Director.
9. Close and reopen one Follower only, then return it to the normal Follower flow.
10. Confirm that this late-joining Follower reconnects and lands on the Director's current page.
11. Confirm that the other 9 Followers remain synced throughout.

## Explicit Non-Goals

The following are **out of scope** for this document:

- multiple Directors in the same session area
- conflict resolution between two valid Directors
- sad paths caused by permissions being denied
- sad paths caused by connectivity being disabled
- background-mode behavior
- app termination and relaunch behavior
- factory reset or data reset behavior
- analytics, logging, or debug tooling requirements
- support for more than 10 Followers beyond the requirement that 10 must work
- specialized recovery flows after repeated connection failure

## Implementation Notes for the Next Coding Agent

These are not requirements, but they are intended to keep the work focused.

- Favor the simplest implementation that satisfies the acceptance criteria.
- Do not broaden scope into every possible edge case.
- Fix only what is necessary for the one-Director, ten-Follower happy path to be reliable.
- Treat any behavior that leaves a connected Follower on the wrong page, wrong mode, wrong book, or stale state as a priority defect against this document.
- Treat any implementation as incomplete if it works for a few Followers but not for all 10.

## Definition of Done

The work is done only when:

1. the implementation matches this document
2. the happy-path manual test script passes with 1 Director and 10 Followers
3. the behavior is understandable to non-technical choir members
4. no manual app restart is needed for the normal one-Director, ten-Follower flow
