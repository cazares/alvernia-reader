// A director takeover must broadcast the page the choir is on when the human CONFIRMS — not the page
// captured when the dialog OPENED.
//
// onDirectorCode built the takeover Alert with `onPress: () => becomeDirector(code, knownCurrentPage …)`.
// knownCurrentPage is the page the web attached to request-director — captured when the dialog opened
// and never re-read. becomeDirector WRITES it into currentPageRef and broadcasts it, and then every
// compensator re-asserts that same overwritten ref: the 100 ms mesh heartbeat (10×/s), the 30 s relay
// heartbeat, bridge-ready's director re-assert, Swift's connect-time snapshot. The web does not re-post
// page-changed on becoming director, so the new director's own screen keeps showing the RIGHT page while
// the ref and the broadcasts hold the STALE one — invisible to the operator until their next manual
// turn. The ex-director gets DIRECTOR_CONFLICT, steps down, and requestCurrentSnapshot pulls it onto the
// stale page too. The whole choir HOLDS the wrong page, no flapping, for up to a song.
//
// The premise that made this look safe — "the dialog is a native MODAL Alert, so nothing on the WebView
// can navigate while it is showing" — is true of TOUCH and false of the MESH: the live director's page
// turns still land during the dialog (mesh `page` → currentPageRef + web render). Two real triggers: a
// takeover of a director who is, by definition, still turning pages; and a backup device booting
// mid-Mass, revealed on the boot default after 2.5 s, whose mesh snapshot lands while the dialog is up —
// confirm then broadcasts the BOOT DEFAULT fleet-wide.
//
// Fix: snapshot the mirror when the dialog opens; on confirm, if the mirror moved to a valid page while
// the dialog was up (and the screen moved with it), prefer the live mirror; otherwise keep the captured
// page exactly as before (the mirror may be mid-lag at open). The -1 render-failed sentinel never wins.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");

/** Bounded by two markers that are each ASSERTED present — never runs to end-of-file. */
const between = (from, to, what) => {
  const a = APP.indexOf(from);
  assert.notEqual(a, -1, `${what}: start marker missing`);
  const b = APP.indexOf(to, a);
  assert.notEqual(b, -1, `${what}: end marker missing`);
  return APP.slice(a, b);
};

const dialog = () => between('"⚠️ Ya hay un director activo"', "// ── Web -> Native message router", "takeover dialog");

test("the takeover dialog snapshots the mirror when it OPENS and resolves the page at CONFIRM", () => {
  const d = dialog();
  const snapAt = d.indexOf("const mirrorAtOpen = currentPageRef.current;");
  const alertAt = d.indexOf("Alert.alert(");
  assert.ok(snapAt !== -1, "the mirror is not snapshotted when the dialog opens");
  assert.ok(alertAt !== -1 && snapAt < alertAt, "the snapshot must be taken BEFORE the Alert is shown");
  assert.doesNotMatch(
    d,
    /onPress: \(\) => becomeDirector\(code, knownCurrentPage\b/,
    "confirm still broadcasts the page captured at OPEN — the whole choir holds a stale page for a song",
  );
  assert.match(
    d,
    /onPress: \(\) => becomeDirector\(code, pageAtConfirm\(\)/,
    "confirm must resolve the page at CONFIRM time via pageAtConfirm()",
  );
});

/**
 * Lift pageAtConfirm out of the source and RUN it. A regex proves a function exists; this proves it
 * decides correctly. Executed against a modelled currentPageRef, mirrorAtOpen and knownCurrentPage.
 */
function liftPageAtConfirm() {
  const d = dialog();
  const start = d.indexOf("const pageAtConfirm = () => {");
  assert.notEqual(start, -1, "pageAtConfirm is gone");
  const open = d.indexOf("{", start);
  let depth = 0;
  for (let i = open; i < d.length; i++) {
    if (d[i] === "{") depth++;
    else if (d[i] === "}") {
      depth--;
      if (depth === 0) {
        const body = d.slice(open + 1, i);
        // eslint-disable-next-line no-new-func
        return new Function("currentPageRef", "mirrorAtOpen", "knownCurrentPage", body);
      }
    }
  }
  assert.fail("unbalanced braces in pageAtConfirm");
}

test("pageAtConfirm prefers a mirror that MOVED during the dialog, keeps the captured page otherwise, never the sentinel", () => {
  const f = liftPageAtConfirm();
  const run = (mirrorAtOpen, known, live) => f({ current: live }, mirrorAtOpen, known);
  // The live director turned a page while the dialog was up: follow the screen, not the stale capture.
  assert.equal(run(12, 12, 13), 13, "a page turned during the dialog must win");
  // Backup device booted on the default page; the mesh snapshot landed during the dialog.
  assert.equal(run(2, 2, 40), 40, "a mesh snapshot that landed during the dialog must win over the boot default");
  // The mirror was lagging at open (still the boot default) and did not move: the captured page wins.
  assert.equal(run(2, 40, 2), 40, "an unmoved, lagging mirror must not override the page the web actually sent");
  // Nothing moved: the captured page stands, exactly as before.
  assert.equal(run(12, 12, 12), 12, "an unmoved mirror keeps the captured page");
  // The render-failed sentinel is never a page.
  assert.equal(run(12, 12, -1), 12, "the -1 sentinel must never be broadcast");
});
