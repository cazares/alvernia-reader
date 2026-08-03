// Who gets told there is a new songbook — the arming decision.
//
// This is the most dangerous logic in the distribution design, because it is the only part that
// fans out. Before it existed, a bad bundle could reach exactly one device at a time, by hand.
// After it, one `wrangler` var reaches all eight iPads with no stagger and no cap — and those
// eight are the same model, on the same iOS, running the same bundle, foregrounded within the
// same two minutes, in a building where nothing can be rolled back.
//
// So it is deliberately hard to arm and trivially easy to abort:
//
//   BOOK_UPDATE_VERSION      ""  = DORMANT. The `bookUpdate` field never appears in any response,
//                                 so the client has literally nothing to act on. This is the
//                                 shipped default and the reason M5 can land without a rehearsal.
//   BOOK_UPDATE_DEVICES      ""  = nobody | "<devId>[,<devId>]" = those devices | "*" = the fleet
//   BOOK_UPDATE_ALLOW_FLEET      "*" is IGNORED unless this is exactly "yes". Two independent
//                                 strings must both be set to reach more than a named device —
//                                 fat-fingering one of them cannot fan out (red team A2).
//   BOOK_UPDATE_CONCURRENCY      how many devices may be mid-download at once under "*"
//
// Emptying BOOK_UPDATE_VERSION and redeploying is a ~20 second abort that needs no iPad. What it
// CANNOT do is un-stage the devices already holding a verified copy — the client handles that by
// clearing its staged directory whenever a check-in stops naming its version (NI6).
//
// Pure and dependency-free so it runs under node --test (sync-worker/test/bookArming.test.mjs).
// The worker has essentially no test coverage; this function is not where that streak continues.

/** Seconds after which a device claiming to be mid-download no longer occupies a slot. */
export const STAGE_SLOT_TTL_SEC = 15 * 60;

/**
 * @param env    { BOOK_UPDATE_VERSION, BOOK_UPDATE_DEVICES, BOOK_UPDATE_ALLOW_FLEET,
 *                 BOOK_UPDATE_BASE, BOOK_UPDATE_CONCURRENCY }
 * @param device the requesting device's stored record ({ deviceId, bookVersion, bookStage, ts })
 * @param fleet  every stored device record, used only to count occupied slots
 * @param nowSec current unix seconds
 * @returns { bookVersion, base } to hand back, or null to say nothing at all
 */
export const decideBookUpdate = (env, device, fleet, nowSec) => {
  const version = String((env && env.BOOK_UPDATE_VERSION) || "").trim();
  // DORMANT. Not "disabled by a flag the client checks" — the field is simply absent, so an old
  // or buggy client cannot act on a value that was never sent.
  if (!version) return null;
  if (!/^bv_[0-9a-f]{16}$/.test(version)) return null; // malformed config fails CLOSED

  const base = String((env && env.BOOK_UPDATE_BASE) || "https://signovivo.com").trim();
  const deviceId = String((device && device.deviceId) || "");
  if (!deviceId) return null;

  // A device already on the target book is told nothing — the pointer is not a heartbeat.
  if (device && device.bookVersion === version) return null;

  const armed = String((env && env.BOOK_UPDATE_DEVICES) || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!armed.length) return null;

  // Named devices are always honoured: this is the "prove it on ONE iPad" path, and it must not be
  // subject to the fleet queue.
  if (armed.includes(deviceId)) return { bookVersion: version, base };

  if (!armed.includes("*")) return null;
  // The second, independent switch. A "*" alone does nothing.
  if (String((env && env.BOOK_UPDATE_ALLOW_FLEET) || "") !== "yes") return null;

  // THE THROTTLE GATES *STARTS*, NEVER WORK ALREADY IN PROGRESS.
  //
  // Returning null below is byte-identical to "disarmed", and the client treats an absent pointer
  // as a REVOKE — PdfReaderApp.tsx:1376 deletes the staged directory outright. So throttling a
  // device that has ALREADY downloaded does not delay it, it destroys ~27 MB of verified work and
  // makes it start over, which is the precise opposite of what this throttle exists to do.
  //
  // Gate on bookStage, not bookVersion: a device reports bookVersion = the ACTIVE book
  // (PdfReaderApp.tsx:273) and only flips it after a human applies, so a staged-and-ready device
  // still reports the OLD version and would look unstarted here.
  const self = (fleet || []).find((d) => d && d.deviceId === deviceId);
  const selfStarted =
    self &&
    typeof self.bookStage === "string" &&
    (self.bookStage === "ready" || self.bookStage.startsWith("downloading"));
  if (selfStarted) return { bookVersion: version, base };

  // K-at-a-time. Without this, eight devices × concurrency 3 is ~24 concurrent GETs and ~216 MB on
  // one parish access point, right before Mass.
  const limit = Math.max(1, Number((env && env.BOOK_UPDATE_CONCURRENCY) || 2) || 2);
  const inFlight = (fleet || []).filter(
    (d) =>
      d &&
      d.deviceId !== deviceId &&
      typeof d.bookStage === "string" &&
      d.bookStage.startsWith("downloading") &&
      d.bookVersion !== version &&
      Number(nowSec) - Number(d.ts || 0) < STAGE_SLOT_TTL_SEC,
  ).length;
  if (inFlight >= limit) return null;

  return { bookVersion: version, base };
};
