// "Every exit from the director role stops publishing" has to include the one nobody looks at: the
// catch branch of becomeDirector for a device that was ALREADY a director (code re-entered while
// directing, the mesh restart failing). 7d2f37b turned relay publishing off there and injected role
// "none" — but roleRef stayed "director" and both heartbeats kept running (stopDirectorHeartbeat only
// ran for a former follower), so the 1 s mesh tick collected DIRECTOR_ROLE_INVALID every second from a
// native module whose role was already "off", and the shell's idea of its role disagreed with what
// the web showed. Skeptic finding on 7d2f37b. Re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");

test("a failed takeover by a device that was already directing stops the heartbeats and mirrors role off", () => {
  const start = APP.indexOf("web follower from nobody. Mirror becomeFollower and turn it off on this exit too.");
  assert.notEqual(start, -1, "the catch-else comment moved");
  const branch = APP.slice(start, APP.indexOf('injectEvent({ type: "role", role: "none" });', start));
  assert.match(branch, /stopDirectorHeartbeat\(\);/, "the catch-else exit leaves both heartbeats running against a native module whose role is already off");
  assert.match(branch, /roleRef\.current = "off";/, "the shell still believes it is the director while the web shows 'none'");
  assert.match(branch, /explicitTransmitterRef\.current = false;/, "a transmitter flag survives the exit and keeps the relay heartbeat gate open");
  assert.match(branch, /setRelayPublishing\(false\);/, "relay publishing is left enabled on this exit");
});

test("the reset confirmation describes what a soft reset now does: back to following, not neutral", () => {
  assert.match(APP, /Se reinicia la conexión: este dispositivo deja de dirigir y vuelve a buscar al director\./,
    "the 'Reparar' copy still says the device stops directing or following — performSoftReset re-enters follower mode");
});
