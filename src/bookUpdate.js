// Download a new songbook over HTTPS, verify it, and swap it in — but only when a human says so.
//
// This is the module that ends "every new PDF needs a TestFlight round". It is written as pure
// logic with `fs`, `storage`, `net` and `now` INJECTED, so the whole thing runs in node
// (e2e/bookUpdate.test.mjs). None of it may be exercised for the first time in a church.
//
// ─── THE THREE RULES THAT SHAPE EVERYTHING BELOW ────────────────────────────────────────────────
//
// 1. THERE IS NO INTERNET AT MASS. Not for any device. Every remedy this codebase has — the relay,
//    the fleet dashboard, staging, rollback — is a practice-day tool. Whatever state a device walks
//    in with at 12:00 is the state it dies with at 13:45. So a failed check is a structural NO-OP,
//    never an error state and never UI: inside the church every check-in fails by definition, and a
//    design that surfaced that would show a permanent error mid-Mass.
//
// 2. STAGING IS AUTOMATIC; APPLYING IS NOT. Downloading bytes nothing reads is safe. Swapping the
//    live songbook is not. An iPad catching a hotspot in the parking lot at 11:55 and swapping its
//    book is the nightmare, and there is no remedy in the room. So `canApplyNow` can only ever say
//    NO, and its load-bearing veto is proof of REAL, SUSTAINED internet — true at practice, false
//    inside the church by definition, needing no clock, no calendar and no Mass-schedule config.
//
// 3. THIS IS A CORRELATED-FAILURE MACHINE. Eight iPads, same model, same iOS, same bundle,
//    foregrounded within the same two minutes. Today a bad bundle can reach one device at a time,
//    by hand. After this, one server string fans out to all eight. Every predicate that looks
//    paranoid — the stagger, the concurrency throttle, the human gate, the expiry — exists because
//    the failure mode this feature introduces is *the whole choir at once, in the one place where
//    nothing can be fixed*.
//
// The apply sequence deliberately mirrors DirectorSyncModule.swift's installReceivedBundle in ORDER
// and in its fail(stage:) vocabulary. That code is battle-tested; this is not. Do not invent a
// second sequence.

/** Only these two hosts may ever serve a bundle. NEVER a host taken from a server response. */
export const ALLOWED_HOSTS = ["signovivo.com", "alvernia-reader.pages.dev"];

/** `bv_` + 16 lowercase hex. Anything else is not a book version and is ignored in silence. */
export const BOOK_VERSION_RE = /^bv_[0-9a-f]{16}$/;

/**
 * The file that lets an applied bundle say what it is.
 *
 * MUST match web/build.mjs's MANIFEST_NAME and the path decideBundle's caller reads
 * (PdfReaderApp.tsx:1154). It is excluded from the manifest's own `files[]` by construction, so
 * `stageBook` has to write it explicitly — see step 7.
 */
export const BUNDLE_MANIFEST_NAME = "bundle-manifest.json";

/** A staged bundle that has sat unconfirmed this long must re-verify before it may be applied. */
export const STAGED_READY_TTL_MS = 12 * 60 * 60 * 1000;
/** How recently a check-in must have succeeded for an apply to count as "real internet". */
export const LIVE_INTERNET_WINDOW_MS = 5 * 60 * 1000;
/** After a cold boot, a device whose last role was director refuses to apply for this long. */
export const DIRECTOR_COLD_BOOT_COOLDOWN_MS = 90 * 60 * 1000;
/** Deterministic per-device spread so eight identical devices do not all start within 20 seconds. */
export const STAGGER_WINDOW_MIN = 20;

// ─── Discovery ──────────────────────────────────────────────────────────────────────────────────

/**
 * Validate the `bookUpdate` pointer that rides the existing /fleet/checkin response.
 *
 * THE POINTER IS DATA, NOT AN INSTRUCTION. A compromised or simply buggy worker must never be able
 * to aim this device at an arbitrary origin, so the host is checked against constants baked into
 * the app rather than trusted from the response. Any violation returns null — silent ignore, zero
 * state change.
 */
export const parseBookUpdate = (resp) => {
  const u = resp && typeof resp === "object" ? resp.bookUpdate : null;
  if (!u || typeof u !== "object") return null;
  const bookVersion = String(u.bookVersion || "");
  if (!BOOK_VERSION_RE.test(bookVersion)) return null;
  let host;
  try {
    const parsed = new URL(String(u.base || ""));
    if (parsed.protocol !== "https:") return null;
    host = parsed.hostname;
  } catch {
    return null;
  }
  if (!ALLOWED_HOSTS.includes(host)) return null;
  return { bookVersion, base: `https://${host}` };
};

/** Stable 0..STAGGER_WINDOW_MIN minute offset derived from the device id. No RNG: reproducible. */
export const staggerDelayMs = (deviceId) => {
  let h = 0;
  for (const ch of String(deviceId || "")) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return (h % STAGGER_WINDOW_MIN) * 60_000;
};

/**
 * May this device begin STAGING? (Writing bytes nothing reads.)
 *
 * Staging and applying deliberately do NOT share a veto set. The practice room is the only place
 * these iPads have internet, and the mesh is up for essentially all of it — so a "no mesh peer
 * connected" staging veto would mean the downloader never runs where it is needed. Mesh activity
 * throttles staging; it does not forbid it.
 */
export const shouldStage = (ctx) => {
  const {
    killSwitch = false,
    bookVersion = "",
    activeBookVersion = null,
    stagedBookVersion = null,
    stagedReady = false,
    quarantine = [],
    webReady = false,
    foreground = true,
    role = "off",
    firstSeenAt = null,
    deviceId = "",
    now = 0,
    minShellBuild = 1,
    shellBuild = 0,
  } = ctx || {};

  if (killSwitch) return { stage: false, reason: "kill-switch" };
  if (!BOOK_VERSION_RE.test(bookVersion)) return { stage: false, reason: "bad-version" };
  if (bookVersion === activeBookVersion) return { stage: false, reason: "already-active" };
  if (stagedBookVersion === bookVersion && stagedReady) return { stage: false, reason: "already-staged" };
  if (Number(minShellBuild) > Number(shellBuild)) return { stage: false, reason: "shell-too-old" };
  // A book that has repeatedly failed to boot is not downloaded again.
  const q = (quarantine || []).find((e) => e && e.bookVersion === bookVersion);
  if (q && Number(q.failures) >= 3) return { stage: false, reason: "quarantined" };
  // NEVER on the boot path: staging competes for I/O with the thing the user is waiting for.
  if (!webReady) return { stage: false, reason: "not-web-ready" };
  if (!foreground) return { stage: false, reason: "background" };
  // OWNER DECISION, 2026-08-03: NO ROLE IS EXEMPT. Every device — director, follower, "off",
  // transmitter-only — downloads the book. The previous rule here refused the director's iPad
  // outright ("the one device the room depends on"), which meant the director could never receive
  // an update at all, by any path: nothing staged, so nothing to apply and nothing a numpad force
  // could act on. Reverting this needs a new owner decision, not a "regression fix".
  //
  // The device's role is NOT an input to this function.
  //
  // OWNER DECISION, 2026-08-03 (second amendment): THE CLIENT STAGGER IS GONE.
  //
  // It delayed the download by hash(deviceId) % 20 minutes so eight iPads would not hit one parish
  // access point at once. Two problems. It is invisible — a device sat doing nothing for up to 19
  // minutes with no way to tell that from a failure, which is exactly how a working rollout reads
  // as a broken one. And it is REDUNDANT: `BOOK_UPDATE_CONCURRENCY` in the worker already caps how
  // many devices may be mid-download (default 2, and it gates STARTS only), which is the same
  // protection — except it is server-tunable, applies to the real fleet rather than to each device
  // guessing in isolation, and can be raised or lowered without a TestFlight round.
  //
  // A named single device being armed for a rehearsal should never wait on a window sized for
  // eight. `staggerDelayMs` is kept and still exported: it is pure, tested, and is the right
  // building block if a future client-side spread is ever wanted again.
  return { stage: true, reason: "ok" };
};

// ─── The completeness gate ──────────────────────────────────────────────────────────────────────

/**
 * Is this staged directory a COMPLETE, correct copy of the manifest's book?
 *
 * Mirrors installReceivedBundle's checks in the same order. `ready` is the ONLY thing that makes a
 * staged directory eligible for promotion, and `resolveBundleUri` has no code path that reads
 * WebBundleStaged at all — so a partially-staged or failed-verification bundle is inert BY
 * CONSTRUCTION. There is no route from a failed check to a swap.
 *
 * `onDisk` is a Map<path, {size, md5}> the caller gathered by walking the staged directory — the
 * files as they ACTUALLY are, never a bookkeeping list of what we believe we wrote.
 */
export const verifyStaged = (manifest, onDisk, activeTotalPages = 0) => {
  const problems = [];
  if (!manifest || !Array.isArray(manifest.files)) return { ok: false, problems: ["no-manifest"] };

  for (const f of manifest.files) {
    const got = onDisk.get(f.p);
    if (!got) { problems.push(`missing:${f.p}`); continue; }
    if (Number(got.size) !== Number(f.n)) { problems.push(`size:${f.p}`); continue; }
    if (String(got.md5 || "").toLowerCase() !== String(f.m || "").toLowerCase()) problems.push(`md5:${f.p}`);
    if (problems.length > 12) break; // enough to diagnose; no point walking 390 files
  }

  // Same floor and same rationale as the Swift installer: a truncated index.html is the failure
  // that produces a blank app rather than an error.
  const index = onDisk.get("index.html");
  if (!index || Number(index.size) <= 200) problems.push("index-too-small");

  // EVERY page by name, not just the first and last (red team NI7). A book missing page 200 passes
  // an endpoints check and strands a song mid-Mass.
  const total = Number(manifest.totalPages || 0);
  const pad = Number(manifest.pagePadWidth || 3);
  const missingPages = [];
  for (let n = 1; n <= total; n += 1) {
    const p = `books/standard/pages/page-${String(n).padStart(pad, "0")}.webp`;
    if (!onDisk.has(p)) missingPages.push(n);
    if (missingPages.length > 5) break;
  }
  if (missingPages.length) problems.push(`pages:${missingPages.join(",")}`);

  // A file on disk that the manifest does not describe is unexplained, and unexplained bytes in a
  // bundle we are about to boot are not acceptable.
  if (onDisk.size !== manifest.files.length) {
    problems.push(`count:${onDisk.size}!=${manifest.files.length}`);
  }

  // ADDITIVE-ONLY, ENFORCED ON THE DEVICE. A book that SHRANK would invalidate every cached page
  // above the new count, every prior SW cache, and every AirDropped copy — reject it outright
  // rather than discover it during Mass.
  if (activeTotalPages && total < Number(activeTotalPages)) {
    problems.push(`shrank:${activeTotalPages}->${total}`);
  }

  return { ok: problems.length === 0, problems };
};

// ─── The apply gate ─────────────────────────────────────────────────────────────────────────────

/**
 * May this device swap the live bundle RIGHT NOW?
 *
 * IT CAN ONLY EVER SAY NO. There is no ambient modal anywhere in this design: a persisted `ready`
 * flag firing a prompt on seven devices at 12:04 was killed three separate ways by the red team,
 * and a modal is a demand for a decision handed to eight people holding instruments. Apply is
 * reachable only from an explicit human action, and this function is the veto on top of it.
 */
export const canApplyNow = (ctx) => {
  const {
    stagedReady = false,
    stagedReadyAt = null,
    lastCheckinOkAt = null,
    meshPeerConnected = false,
    lastPageTurnAt = null,
    lastDirectorSnapshotAt = null,
    role = "off",
    lastKnownRole = null,
    coldBootAt = null,
    webReady = false,
    minShellBuild = 1,
    shellBuild = 0,
    now = 0,
  } = ctx || {};

  if (!stagedReady) return { ok: false, reason: "not-ready" };
  // A `ready` flag persisted from Saturday practice must not still be applicable in the church on
  // Sunday (red team A1).
  if (stagedReadyAt != null && now - stagedReadyAt > STAGED_READY_TTL_MS) {
    return { ok: false, reason: "stale-ready" };
  }
  // THE LOAD-BEARING ONE. Proof of real, sustained internet. True at practice; false inside the
  // church by definition; a parking-lot hotspot usually fails it too. No clock, no calendar.
  if (lastCheckinOkAt == null || now - lastCheckinOkAt > LIVE_INTERNET_WINDOW_MS) {
    return { ok: false, reason: "no-live-internet" };
  }
  // OWNER DECISION, 2026-08-03 (second amendment): AN UPDATE MUST NEVER REQUIRE THE USER TO STOP
  // TOUCHING THE DEVICE.
  //
  // What was here: `recent-page-turn` (60 s after any page turn), `director-active` (10 min after a
  // director snapshot), and a 90-minute director cold-boot cooldown. Together they meant a device
  // someone was actually USING could defer forever — and the only way to land the update was to put
  // the iPad down and wait. On a real device that reads as "the update is broken", because from the
  // user's side it is: checking whether it arrived (typing a song number) IS a page turn, which
  // re-armed the 60-second lockout on every single check. Looking at it prevented it.
  //
  // Gone deliberately. Reverting needs a new owner decision, not a "regression fix".
  //
  // ACCEPTED CONSEQUENCE, stated plainly: the swap remounts the WebView, so it can now land while
  // someone is reading — a blank screen for a beat, then the new book. The owner was told and
  // chose this over an update that never arrives.
  //
  // `meshPeerConnected` STAYS, and it is not a "wait quietly" gate: a connected mesh peer means a
  // rehearsal or Mass is actively running and the whole room would blink at once. It clears on its
  // own the moment the session ends — it never asks anything of the user.
  if (meshPeerConnected) return { ok: false, reason: "mesh-peer" };
  if (!webReady) return { ok: false, reason: "bridge-not-ready" };
  if (Number(minShellBuild) > Number(shellBuild)) return { ok: false, reason: "shell-too-old" };
  return { ok: true, reason: "ok" };
};

// ─── Numpad codes ───────────────────────────────────────────────────────────────────────────────

/** Levenshtein distance — used to prove two codes cannot be confused under stress. */
export const levenshtein = (a, b) => {
  const m = a.length;
  const n = b.length;
  const d = Array.from({ length: m + 1 }, (_, i) => [i, ...Array(n).fill(0)]);
  for (let j = 0; j <= n; j += 1) d[0][j] = j;
  for (let i = 1; i <= m; i += 1) {
    for (let j = 1; j <= n; j += 1) {
      d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    }
  }
  return d[m][n];
};

/**
 * Every operator code must be at least this far from every other one.
 *
 * These are read off a laminated card in poor light, in a church, by someone under stress. The
 * originally-proposed apply code was ONE DIGIT from the soft-reset code (red team H4) — a single
 * misread would have wiped the device's role instead of applying an update.
 */
export const MIN_CODE_DISTANCE = 3;

// ─── Staging ────────────────────────────────────────────────────────────────────────────────────

/**
 * Download and verify a bundle into the staged directory. Returns the new sv_book_staged record.
 *
 * Deps are injected: `net.fetchJson`, `net.download(url, dest)`, `fs.*`, `now()`.
 * Never throws for an expected condition — an offline device gets `{ready:false, error:"network"}`
 * and nothing else happens. Rule 1.
 */
export const stageBook = async (opts) => {
  const {
    base, bookVersion, fs, net, now = () => 0,
    activeTotalPages = 0, shellBuild = 0, freeDiskBytes = null,
    stagedDir = "WebBundleStaged", concurrency = 3,
    onProgress = () => {},
  } = opts;

  const fail = (error, detail) => ({ bookVersion, ready: false, error, detail: detail || null, at: now() });

  // 1. The manifest, pinned by the version we were told to fetch. If the CDN edge hands us a
  //    different edition, abort: this is what makes it impossible for a staged directory to end up
  //    holding a MIX of two books.
  let manifest;
  try {
    manifest = await net.fetchJson(`${base}/bundle-manifest.json?v=${encodeURIComponent(bookVersion)}`);
  } catch {
    return fail("network");
  }
  if (!manifest || manifest.bookVersion !== bookVersion) return fail("version-mismatch");
  if (!Array.isArray(manifest.files) || !manifest.files.length) return fail("bad-manifest");

  // 2. A book that needs native code this shell lacks is never downloaded at all.
  if (Number(manifest.minShellBuild || 1) > Number(shellBuild)) return fail("shell-too-old");

  // 3. Disk. There is no free-space check anywhere else in this pipeline today.
  const needed = manifest.files.reduce((s, f) => s + Number(f.n || 0), 0);
  if (freeDiskBytes != null && freeDiskBytes < needed * 2 + 50 * 1024 * 1024) {
    return fail("disk", { needed, freeDiskBytes });
  }

  // 4. Resume or start clean. `.stage.json` is written FIRST, before any file, so an interrupted
  //    stage is always identifiable rather than an anonymous pile of bytes.
  const stampPath = `${stagedDir}/.stage.json`;
  let resuming = false;
  try {
    const stamp = await fs.readJson(stampPath);
    resuming = !!stamp && stamp.bookVersion === bookVersion;
  } catch { /* no stamp */ }
  if (!resuming) {
    await fs.rmrf(stagedDir);
    await fs.mkdirp(stagedDir);
    await fs.writeJson(stampPath, { bookVersion, startedAt: now() });
  }

  // 5. Fetch what is missing. Files already present at the EXACT size are skipped, and everything
  //    is RE-VERIFIED in step 6 regardless — so resume needs no persisted bookkeeping and cannot
  //    be corrupted by a stale one.
  let done = 0;
  const queue = [...manifest.files];
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    for (;;) {
      const f = queue.shift();
      if (!f) return;
      const dest = `${stagedDir}/${f.p}`;
      const info = await fs.stat(dest).catch(() => null);
      if (!info || Number(info.size) !== Number(f.n)) {
        try {
          await fs.mkdirp(dest.slice(0, dest.lastIndexOf("/")));
          await net.download(`${base}/${f.p}`, dest);
        } catch {
          throw new Error(`download:${f.p}`);
        }
      }
      done += 1;
      if (done % 20 === 0) onProgress(done, manifest.files.length);
    }
  });
  try {
    await Promise.all(workers);
  } catch (e) {
    return fail("download", String(e && e.message));
  }

  // 6. The completeness gate, over the files as they ACTUALLY are on disk.
  let onDisk;
  try {
    onDisk = await fs.walkWithHashes(stagedDir);
  } catch {
    return fail("verify-walk");
  }
  // Our own bookkeeping is not part of the bundle. BUNDLE_MANIFEST_NAME is excluded for the same
  // reason AND because a resumed stage finds the copy step 7 wrote last time — without this the
  // count check below fails by exactly one on every retry.
  onDisk.delete(".stage.json");
  onDisk.delete(BUNDLE_MANIFEST_NAME);
  const verdict = verifyStaged(manifest, onDisk, activeTotalPages);
  if (!verdict.ok) return fail("verify", verdict.problems.slice(0, 8));

  // 7. IDENTITY — the step whose absence made this whole feature a no-op.
  //
  // `bundle-manifest.json` is deliberately absent from its own `files[]` (web/build.mjs:859 — a
  // manifest cannot contain its own hash), so step 5 never downloads it. Nothing else writes it
  // either: its only other producers are the web build and the Swift mesh installer.
  //
  // So without this write, a PERFECT download applies into Documents/WebBundle as a bundle that
  // cannot say what it is — byte-identical in shape to the legacy mesh-pushed copies that
  // decideBundle rule 3 exists to evict (src/bookResolve.js:130). The device then downloads 27 MB,
  // verifies all 389 files, renames the directory into place, and boots the BAKED-IN book anyway,
  // reporting the old bookVersion to the fleet dashboard. Measured on Miguel's iPad across builds
  // 391/392/393: every server-side check passed and the book never once landed.
  //
  // Worse, it never settles: performApplySwap clears sv_book_staged and resolveBundleUri re-points
  // activeBookVersion at the baked book, so the next check-in sees an un-staged, not-yet-active
  // pointer and downloads the entire bundle again, indefinitely.
  //
  // Written AFTER verifyStaged deliberately: the gate asserts onDisk.size === files.length, and a
  // manifest present during the walk would fail that count by one. An incomplete stage therefore
  // never carries an identity, which is exactly the invariant rule 3 relies on.
  try {
    await fs.writeJson(`${stagedDir}/${BUNDLE_MANIFEST_NAME}`, manifest);
  } catch {
    return fail("manifest-write");
  }

  return {
    bookVersion,
    ready: true,
    readyAt: now(),
    totalPages: Number(manifest.totalPages),
    minShellBuild: Number(manifest.minShellBuild || 1),
    files: manifest.files.length,
    error: null,
  };
};

// ─── Apply ──────────────────────────────────────────────────────────────────────────────────────

/**
 * Promote the staged bundle to live. RENAMES ONLY — it moves zero bytes and therefore cannot fail
 * on disk space, which matters because a device that just downloaded 27 MB is the device most
 * likely to be short.
 *
 * NO MID-SWAP RESUME LOGIC, DELIBERATELY. Killed between the two moves, Documents/WebBundle is
 * simply absent, resolveBundleUri falls through to the code-signed bundle, and the app boots into a
 * working songbook with mesh sync intact. A phase-fenced ledger would guard a window measured in
 * milliseconds with code that runs on the boot path — the riskiest code in the app. The manual
 * retry is the cheaper correctness guarantee.
 */
export const applyStagedBundle = async ({ fs, live = "WebBundle", staged = "WebBundleStaged", prevTmp = "WebBundle.prev.tmp" }) => {
  await fs.rmrf(prevTmp);
  const hasLive = await fs.exists(live);
  if (hasLive) {
    try {
      await fs.move(live, prevTmp);
    } catch {
      return { ok: false, stage: "swap-aside" }; // nothing changed
    }
  }
  try {
    await fs.move(staged, live);
  } catch {
    if (hasLive) await fs.move(prevTmp, live).catch(() => {});
    return { ok: false, stage: "swap-in" }; // rolled back
  }
  return { ok: true, stage: "done" };
};
