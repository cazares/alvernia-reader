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
// ─── AMENDMENT — OWNER DECISION, 2026-08-03: NO ROLE IS EXEMPT ──────────────────────────────────
//
// Rules 1 and 3 stand unchanged. What changed is WHO the update reaches: EVERY role — director,
// follower, and "off" — now both DOWNLOADS and APPLIES the songbook. There is no director exemption
// left anywhere in this file, and `role` is no longer an input to either gate.
//
// What this replaced: the director's iPad refused to stage at all; `canApplyNow` refused it outright
// ("the last device on earth that may swap"); a device whose LAST known role was director refused to
// apply for 90 minutes after any cold boot; and every follower refused to apply for 10 minutes after
// hearing a live director's mesh page. All four are gone.
//
// WHY it changed: the rationale for the exemption was put to the owner in full and OVERRIDDEN. An
// update that skips the one device leading the room leaves the fleet split across two books —
// followers on the new songbook, the director on the old one — and the owner judges that outcome
// worse than the swap the exemption was avoiding. Every device gets the same book.
//
// WHAT DID NOT CHANGE, and must not be stripped by anyone reading the above as "the gates came out":
// every veto that is about TIMING or CAPABILITY still applies, and now applies to all roles equally.
// This list is EXHAUSTIVE — it is the whole veto set of both gates, and it is checked by
// e2e/bookUpdate.test.mjs ("every veto reason of both gates is named in the header amendment"), so
// adding a gate without adding it here REDS THE SUITE. An authoritative list that is quietly wrong
// is worse than no list at all.
//
//   shouldStage:  kill-switch · bad-version · already-active · already-staged (while FRESH — see the
//                 M1 amendment) · shell-too-old · quarantined · not-web-ready · background ·
//                 stagger-not-started · stagger-waiting
//                 — and its two YES answers: ok, restage-expired.
//   canApplyNow:  not-ready · stale-ready · no-live-internet · mesh-peer · recent-page-turn ·
//                 room-active · bridge-not-ready · shell-too-old
//
// A director's iPad is subject to exactly those and to nothing else.
//
// ─── SECOND AMENDMENT, 2026-08-03: THE THREE REGRESSIONS THE FIRST ONE SHIPPED ──────────────────
//
// An adversarial review of the amendment above found that removing the role vetoes took three
// things with it that were NOT role vetoes. All three are repaired here; none of the repairs
// reintroduces a role check, and `role` remains absent from both context types.
//
// H1 — THE ROOM-ACTIVITY WINDOW WAS COLLAPSED FROM 10 MINUTES TO 60 SECONDS, ON EVERY DEVICE. The
//      deleted `director-active` veto read `lastDirectorSnapshotAt`, which is written by the mesh
//      page-RECEIVE handler — it fired on FOLLOWERS, not on directors, and it was never a role
//      check. It and `recent-page-turn` were the same timestamp at two decay rates, so deleting it
//      silently cut the in-room quiet window an apply must clear from 10 min to 60 s for everyone.
//      Restored as `lastMeshPageAt` / ROOM_ACTIVE_WINDOW_MS: "did a page move in this room
//      recently", never "who is this device".
//
// H2 — `recent-page-turn` WAS DEAD CODE ON A DIRECTING DEVICE. `lastPageTurnAt` was fed only by the
//      mesh page-RECEIVE handler, which a director breaks out of before reaching it (it ignores its
//      own echoes) — so the gate was structurally unreachable on the one device the owner's
//      decision newly exposed. PdfReaderApp.tsx now stamps BOTH timestamps on the device's OWN page
//      turns as well, so the gate is live for every role. (The timestamps are ROOM facts, not ROLE
//      facts, and are deliberately never cleared on a role transition — see the note there.)
//
// M1 — A STALE-READY STAGED COPY WAS A PERMANENT DEAD END. Practice is the only window with
//      internet, and the mesh is up for nearly all of it, so a device typically stages at practice
//      and is vetoed from applying. After STAGED_READY_TTL_MS the record turns `stale-ready`
//      forever while `shouldStage` answered `already-staged` and never refreshed it: a verified
//      27 MB copy that could never be used, on a device the dashboard reported as "ready" while it
//      rendered the OLD book. `already-staged` now only holds while the record is FRESH, so an
//      expired one is re-verified (files already at the exact size are not re-downloaded) and gets
//      a new `readyAt`. A MISSING `stagedReadyAt` counts as EXPIRED in both gates — fail closed.
//
// The apply sequence deliberately mirrors DirectorSyncModule.swift's installReceivedBundle in ORDER
// and in its fail(stage:) vocabulary. That code is battle-tested; this is not. Do not invent a
// second sequence.

/** Only these two hosts may ever serve a bundle. NEVER a host taken from a server response. */
export const ALLOWED_HOSTS = ["signovivo.com", "alvernia-reader.pages.dev"];

/** `bv_` + 16 lowercase hex. Anything else is not a book version and is ignored in silence. */
export const BOOK_VERSION_RE = /^bv_[0-9a-f]{16}$/;

/** A staged bundle that has sat unconfirmed this long must re-verify before it may be applied. */
export const STAGED_READY_TTL_MS = 12 * 60 * 60 * 1000;
/** How recently a check-in must have succeeded for an apply to count as "real internet". */
export const LIVE_INTERNET_WINDOW_MS = 5 * 60 * 1000;
/**
 * How long a room stays "in session" after the last page moved in it.
 *
 * ROLE-NEUTRAL BY CONSTRUCTION (H1). This is the window the deleted `director-active` veto used to
 * carry, restored under a name that describes the SIGNAL rather than a device: a page moved in this
 * room recently, whoever moved it. Ten minutes, not sixty seconds, because the gap between two
 * songs — announcements, a reading, retuning — routinely exceeds a minute, and a bundle swap that
 * remounts the WebView in that gap is the same accident as one mid-verse.
 */
export const ROOM_ACTIVE_WINDOW_MS = 10 * 60 * 1000;
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
 *
 * THE DEVICE'S ROLE IS NOT AN INPUT (owner decision, 2026-08-03). Director, follower and "off" all
 * download. Everything below is about the book, the shell, the disk and the clock.
 *
 * IT IS ALSO THE ONLY WAY OUT OF A STALE-READY DEAD END (M1). `already-staged` holds only while the
 * staged record is still FRESH by the same TTL `canApplyNow` enforces; once it expires this gate
 * says YES again, the stage re-verifies (files already at the exact size are skipped, so this is a
 * re-hash, not a 27 MB re-download) and writes a new `readyAt`. Without that, a copy that expired
 * before it could ever be applied would be un-appliable AND un-refreshable — permanently.
 */
export const shouldStage = (ctx) => {
  const {
    killSwitch = false,
    bookVersion = "",
    activeBookVersion = null,
    stagedBookVersion = null,
    stagedReady = false,
    stagedReadyAt = null,
    quarantine = [],
    webReady = false,
    foreground = true,
    firstSeenAt = null,
    deviceId = "",
    now = 0,
    minShellBuild = 1,
    shellBuild = 0,
  } = ctx || {};

  if (killSwitch) return { stage: false, reason: "kill-switch" };
  if (!BOOK_VERSION_RE.test(bookVersion)) return { stage: false, reason: "bad-version" };
  if (bookVersion === activeBookVersion) return { stage: false, reason: "already-active" };
  // M1. A record whose readyAt has expired — or that never had one, which fails CLOSED — is exactly
  // the record `canApplyNow` is about to refuse as `stale-ready`. Treating it as "already staged"
  // is what turned that refusal into a permanent dead end, so it does not count here.
  const haveStaged = stagedBookVersion === bookVersion && stagedReady;
  const stagedExpired = stagedReadyAt == null || now - stagedReadyAt > STAGED_READY_TTL_MS;
  if (haveStaged && !stagedExpired) return { stage: false, reason: "already-staged" };
  if (Number(minShellBuild) > Number(shellBuild)) return { stage: false, reason: "shell-too-old" };
  // A book that has repeatedly failed to boot is not downloaded again.
  const q = (quarantine || []).find((e) => e && e.bookVersion === bookVersion);
  if (q && Number(q.failures) >= 3) return { stage: false, reason: "quarantined" };
  // NEVER on the boot path: staging competes for I/O with the thing the user is waiting for.
  if (!webReady) return { stage: false, reason: "not-web-ready" };
  if (!foreground) return { stage: false, reason: "background" };
  if (firstSeenAt == null) return { stage: false, reason: "stagger-not-started" };
  if (now - firstSeenAt < staggerDelayMs(deviceId)) return { stage: false, reason: "stagger-waiting" };
  // A distinct reason so the breadcrumb says WHY a device that already has this book is downloading
  // it again — "restage-expired" on the dashboard is a device recovering, not a device thrashing.
  return { stage: true, reason: haveStaged ? "restage-expired" : "ok" };
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
 * and a modal is a demand for a decision handed to eight people holding instruments. This function
 * decides WHEN the swap happens, never WHETHER it is allowed to happen at all.
 *
 * THE DEVICE'S ROLE IS NOT AN INPUT (owner decision, 2026-08-03). Every veto below is about TIMING
 * or CAPABILITY, and every one of them applies to a director's iPad exactly as it applies to a
 * follower's. See the amendment at the top of this file for what was removed and why.
 */
export const canApplyNow = (ctx) => {
  const {
    stagedReady = false,
    stagedReadyAt = null,
    lastCheckinOkAt = null,
    meshPeerConnected = false,
    lastPageTurnAt = null,
    lastMeshPageAt = null,
    webReady = false,
    minShellBuild = 1,
    shellBuild = 0,
    now = 0,
  } = ctx || {};

  if (!stagedReady) return { ok: false, reason: "not-ready" };
  // A `ready` flag persisted from Saturday practice must not still be applicable in the church on
  // Sunday (red team A1). A record that claims `ready` with NO `readyAt` cannot be aged, so it is
  // treated as expired rather than waved through (M1): the unknown case fails CLOSED, and
  // `shouldStage` will re-verify it and mint a real `readyAt` rather than leave it stuck.
  if (stagedReadyAt == null || now - stagedReadyAt > STAGED_READY_TTL_MS) {
    return { ok: false, reason: "stale-ready" };
  }
  // THE LOAD-BEARING ONE. Proof of real, sustained internet. True at practice; false inside the
  // church by definition; a parking-lot hotspot usually fails it too. No clock, no calendar.
  if (lastCheckinOkAt == null || now - lastCheckinOkAt > LIVE_INTERNET_WINDOW_MS) {
    return { ok: false, reason: "no-live-internet" };
  }
  // A rehearsal or a Mass is happening RIGHT NOW: peers are connected this instant. Fires on every
  // device, including the one that is directing.
  if (meshPeerConnected) return { ok: false, reason: "mesh-peer" };
  // Someone is SINGING. Fed by the mesh page-RECEIVE handler AND by this device's own page turns
  // (H2) — a director used to be invisible to this gate because it breaks out of the receive case
  // before the stamp, which made it dead code on the one device the 2026-08-03 decision newly
  // exposed. It is a TIMING gate and always was; it is now simply reachable from every role.
  if (lastPageTurnAt != null && now - lastPageTurnAt < 60_000) return { ok: false, reason: "recent-page-turn" };
  // THE ROOM IS STILL IN SESSION (H1). The longer decay of the same signal: sixty seconds of quiet
  // is a gap between two songs, not the end of a Mass. `meshPeerConnected` covers only the instant
  // — Multipeer drops and re-forms constantly in a stone building — so without this window an apply
  // could land in a flap between two verses. This is the window the deleted `director-active` veto
  // actually carried; it fired on FOLLOWERS (they are the ones who RECEIVE a director's page) and
  // was never a role check, which is why removing the role vetoes must not have removed it.
  if (lastMeshPageAt != null && now - lastMeshPageAt < ROOM_ACTIVE_WINDOW_MS) {
    return { ok: false, reason: "room-active" };
  }
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
  onDisk.delete(".stage.json"); // our own bookkeeping is not part of the bundle
  const verdict = verifyStaged(manifest, onDisk, activeTotalPages);
  if (!verdict.ok) return fail("verify", verdict.problems.slice(0, 8));

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
