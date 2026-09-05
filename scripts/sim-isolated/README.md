# Isolated two-simulator mesh testing

Two iPad simulators running this app DO pair over real Multipeer Connectivity (the mesh the choir
uses), which makes the simulator the only way to exercise director/follower behaviour end to end
without a room full of iPads. BLE does not run in a simulator; the relay path runs against a local
`wrangler dev`.

**The reason this directory exists:** a simulator running the PRODUCTION service type joins the real
Wi-Fi mesh, and a simulator director (newest token) hijacks every physical follower into a reconnect
loop. That produced three "slow sync the night before Mass" incidents. Everything built here is
renamed so it cannot see, or be seen by, a real device:

| What | Production | Isolated build |
|---|---|---|
| Multipeer service type + NSBonjourServices | `signovivo` | `svsimtest` |
| Director session code | `1234` | `7777` |
| Relay (native + web bundle) | signovivo-sync worker | `http://localhost:8787` |

`sim-build.sh` refuses to build unless all three substitutions verify.

## Recipe

```
# once: create the throwaway copy (never committed)
cp -R <this tree> ~/sv-sim-build && (cd ~/sv-sim-build && npm ci && cd ios && LANG=en_US.UTF-8 pod install)

# every iteration
bash scripts/sim-isolated/sync-copy.sh        # bring the copy up to this tree
bash scripts/sim-isolated/sim-build.sh        # patch isolation, build web bundle + Release simulator app
bash scripts/sim-isolated/relay-dev.sh --fresh   # in the background; --fresh drops a stale room state

xcrun simctl uninstall <udid> com.cazares.alvernia   # clears AsyncStorage: fresh-boot scenarios need it
xcrun simctl install   <udid> ~/sv-sim-build/build-sim/Build/Products/Release-iphonesimulator/SignoVivo.app
xcrun simctl launch    <udid> com.cazares.alvernia
```

Read the app's own log lines with
`xcrun simctl spawn <udid> log show --last 5m --style compact --predicate 'subsystem == "com.cazares.signovivo"'`
and the room state with `curl -s http://127.0.0.1:8787/r/alvernia-main/state`.

## What this found on its first run (2026-09-04)

Director on the cover, follower on "2. Bendito, Bendito", relay holding page 2: on a native boot with
nobody directing, the web set `state.currentPage = DEFAULT_START_PAGE` (2) without rendering, revealed
on the static page-001 image after the 2.5 s gate, and "Ser Director" broadcast state (2). Present in
build 472. Fixed in web/src/app.js (`renderedPage()` + the post-gate reconciliation), pinned by
e2e/bootStateMatchesScreen.test.mjs.

## Rules

- Never run a simulator on the production service type while physical devices are being tested.
- Never point the copy at the production relay. `sim-build.sh` greps for the production host and aborts.
- The copy is disposable; the scripts are not — they live here so a sync can never delete them.
