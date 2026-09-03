// After a crash, "Volver a dirigir" must put the choir back on the page the director was ON — not on
// the web's boot default.
//
// What shipped: bootstrap reads lastSyncRole + lastDirectorPage, stashes the page in
// restoredDirectorPageRef, shows the resume toast, and makes the device a FOLLOWER. The web boots to
// its default page (2) and posts bridge-ready; the follower branch ADOPTS page 2 into the mirror.
// Nothing re-homes it: this device WAS the only director, so requestCurrentSnapshot gets no answer,
// and native never follows the relay. "Volver a dirigir" sends request-director with currentPage: 2,
// and the resumed director broadcasts the BOOT DEFAULT fleet-wide. Held until a human navigates —
// 5–30 s of reaction time in front of the congregation, not self-correcting.
//
// It was worse than that: lastDirectorPage was written only when the role was TAKEN, so even the
// "restored" value was the page from 11:55 — usually the boot page — not the page at crash time.
// PR #381's "restore the page the director was on before the crash" never worked end-to-end.
//
// Two small fixes, both pinned here and re-injected by scripts/verify-behavioural-guards.mjs:
//   (a) a director's every page turn refreshes lastDirectorPage;
//   (b) bootstrap injects the restored page as a sync-event, which queues until bridge-ready, flushes
//       right after the follower branch adopted page 2, and drives the web to the restored page — so
//       the mirror, the screen, and the request-director payload all carry the crash-time page.
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
  assert.notEqual(a, -1, `${what}: start marker missing — ${from}`);
  const b = APP.indexOf(to, a);
  assert.notEqual(b, -1, `${what}: end marker missing — ${to}`);
  return APP.slice(a, b);
};

test("a director's page turns keep lastDirectorPage fresh, so the restored page is the crash-time page", () => {
  const body = between('case "page-changed": {', 'case "director-code": {', "page-changed handler");
  const clampAt = body.indexOf("currentPageRef.current = page;");
  assert.notEqual(clampAt, -1, "the clamped mirror write is gone");
  const writeAt = body.indexOf("AsyncStorage.setItem(STORAGE_KEYS.lastDirectorPage, String(page))");
  assert.notEqual(writeAt, -1,
    "page turns no longer persist lastDirectorPage — after a crash the 'restored' page is the promotion-time page");
  assert.ok(writeAt > clampAt, "the persist must use the CLAMPED page, so it must follow the clamp");
  assert.match(body.slice(clampAt, writeAt), /if \(roleRef\.current === "director"\)/,
    "the persist must be gated on the director role — followers must not overwrite the director's crash-resume page");
});

test("bootstrap drives the web to the restored page, so 'Volver a dirigir' carries it instead of the boot default", () => {
  const body = between('if (prev === "director") {', ".finally(() => {", "bootstrap director branch");
  const restoreAt = body.indexOf("restoredDirectorPageRef.current = pageStr ? Number(pageStr) : undefined;");
  assert.notEqual(restoreAt, -1, "the restored page is no longer read from storage");
  const readAt = body.indexOf("const restored = restoredDirectorPageRef.current;");
  assert.notEqual(readAt, -1, "bootstrap does not drive the web to the restored page — the resume broadcasts the boot default");
  assert.ok(readAt > restoreAt, "the restored page must be read AFTER it is set");
  assert.match(body.slice(readAt), /typeof restored === "number" && restored > 0/,
    "the inject must be guarded — a missing or non-positive restored page must inject nothing");
  assert.match(
    body.slice(readAt),
    /injectEvent\(\{\s*type: "sync-event",\s*event: \{ type: "page", page: restored,/,
    "the restored page is not injected as a page sync-event — the web stays on its boot default",
  );
});
