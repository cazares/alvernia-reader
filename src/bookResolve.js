// Which songbook bundle should this device boot?
//
// THE DEFECT THIS REPLACES (D1, docs/choir-pdf-distribution-plan.md §3.2). The old resolver was:
//
//     if (Documents/WebBundle/index.html exists) return it;
//     return bundleDirectory + "WebBundle/index.html";      // not even stat'd
//
// No version compare, no manifest, no health check — and NOTHING in the app ever deletes that
// directory. Its only writer is the Multipeer bundle push. So an iPad that once received a peer
// push and later installed a newer TestFlight build kept rendering the OLD book forever while the
// badge showed the NEW build number, was invisible to the fleet dashboard, and was permanently
// ineligible for a corrective push (the offer compares shell build numbers, which had moved on).
// Demonstrated live on a simulator 2026-08-02: planting a stale Documents/WebBundle made the same
// binary silently render a 372-page book while reporting itself current. It is LIVE in build 383.
//
// It was also a one-way door: no code path re-resolved the URI after a failure, so once that
// directory existed and was broken, the only recovery was delete-and-reinstall the app — i.e.
// TestFlight, the exact thing this project exists to eliminate.
//
// This module is the decision ONLY — no filesystem, no storage, no React. Everything it concludes
// follows from plain data, so the whole table is exercised in e2e/bookResolve.test.mjs rather than
// on eight iPads in a church with no internet. PdfReaderApp.tsx does the I/O and calls in here.

/** Boot attempts with no proven-good boot before we assume the Documents bundle is killing us. */
export const MAX_BOOT_ATTEMPTS = 2;

/** Lifetime failures for one bookVersion before it is refused outright. */
export const QUARANTINE_FAILURE_LIMIT = 3;

/** Shape a bundle-manifest must have before we will believe a word of it. */
export const isValidManifest = (m) =>
  !!m &&
  typeof m === "object" &&
  /^bv_[0-9a-f]{16}$/.test(String(m.bookVersion || "")) &&
  Number.isFinite(Number(m.totalPages)) &&
  Number(m.totalPages) > 0;

/**
 * Has this bookVersion failed enough to be refused outright?
 *
 * A COUNTER, NOT A TOMBSTONE (red team NI5). A single transient jetsam on the oldest iPad must not
 * permanently blacklist the book the whole choir is about to sing from — that device could then
 * never receive that book again, by content hash, forever.
 */
export const isQuarantined = (quarantine, bookVersion) => {
  const e = (quarantine || []).find((q) => q && q.bookVersion === bookVersion);
  return !!e && Number(e.failures) >= QUARANTINE_FAILURE_LIMIT;
};

/** Record one boot failure against a bookVersion. Returns a NEW list (never mutates). */
export const recordBundleFailure = (quarantine, bookVersion, nowMs) => {
  const list = (quarantine || []).filter((q) => q && q.bookVersion);
  const i = list.findIndex((q) => q.bookVersion === bookVersion);
  if (i === -1) return [...list, { bookVersion, failures: 1, lastFailureAt: nowMs }];
  const next = [...list];
  next[i] = { ...next[i], failures: Number(next[i].failures || 0) + 1, lastFailureAt: nowMs };
  return next;
};

/**
 * A bundle that has now PROVEN it boots gets its counter cleared.
 *
 * Required for the "consecutive failures only" rule: without it, three unrelated transient
 * failures spread over months would blacklist a book that works fine today.
 */
export const clearBundleFailures = (quarantine, bookVersion) =>
  (quarantine || []).filter((q) => q && q.bookVersion !== bookVersion);

/**
 * An ISO build stamp, or null if this manifest cannot say when it was built.
 *
 * STRINGS ONLY. `Date.parse` happily reads a bare number as a YEAR — `Date.parse("12345")` is a
 * valid date in the year 12345 — so coercing an arbitrary value would let a junk field out-rank a
 * real timestamp by four millennia.
 */
export const parseBuildStamp = (v) => {
  if (typeof v !== "string") return null;
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
};

/**
 * THE DECISION TABLE. Order is load-bearing; each rule is commented with what it defends.
 *
 * ctx: {
 *   docExists, docManifest, bakedManifest, bakedExists,
 *   forceBundled, bootAttempts, bootProved, quarantine
 * }
 * returns { source: "documents" | "bundled" | "none", reason, quarantineDoc }
 *
 * `source: "none"` means even the code-signed bundle is missing — a build-pipeline defect with no
 * runtime remedy. The caller surfaces it rather than handing the WebView a path to nothing.
 */
export const decideBundle = (ctx) => {
  const {
    docExists = false,
    docManifest = null,
    bakedManifest = null,
    bakedExists = true,
    forceBundled = false,
    bootAttempts = 0,
    bootProved = false,
    quarantine = [],
  } = ctx || {};

  const bundled = (reason, extra) => ({ source: "bundled", reason, quarantineDoc: false, ...extra });

  // 0. The code-signed floor must actually exist. The old resolver returned this path WITHOUT
  //    stat'ing it, so a release that failed to copy ios/WebBundle handed the WebView a URI to
  //    nothing and the app was a black rectangle with no floor at all. release.sh now asserts the
  //    copy, but a resolver that assumes its last resort exists is how you get an unrecoverable
  //    boot; if both bundles are gone we say so instead of pretending.
  if (!bakedExists && !docExists) return { source: "none", reason: "no-bundle-anywhere", quarantineDoc: false };
  if (!bakedExists) {
    // Documents is all we have. Still refuse a KNOWN-BAD one — booting a quarantined bundle with
    // no fallback is worse than surfacing the failure.
    if (docExists && isValidManifest(docManifest) && isQuarantined(quarantine, docManifest.bookVersion)) {
      return { source: "none", reason: "only-documents-and-it-is-quarantined", quarantineDoc: false };
    }
    return { source: "documents", reason: "no-baked-bundle", quarantineDoc: false };
  }

  // 1. Operator panic switch. Deliberately above everything else: when someone has decided the
  //    downloaded book is wrong, no cleverness below may override them.
  if (forceBundled) return bundled("force-bundled");

  // 2. The hard-crash net (§5.10b). An in-session watchdog cannot see a bundle that kills the
  //    process before React renders — which is the failure an old iPad under memory pressure
  //    actually produces. If we have mounted twice without ever reaching bridge-ready, stop
  //    trying: quarantine the Documents copy and boot the floor.
  if (!bootProved && Number(bootAttempts) >= MAX_BOOT_ATTEMPTS) {
    return { source: "bundled", reason: "boot-attempts-exhausted", quarantineDoc: docExists };
  }

  if (!docExists) return bundled("no-documents-bundle");

  // 3. THE RULE THAT RETIRES D1 ACROSS THE ENTIRE INSTALLED BASE.
  //    Every Documents/WebBundle in the field today was written by the Multipeer push, which
  //    predates manifests — so none of them have one. Refusing an unidentifiable bundle evicts all
  //    of them on first launch of this build: no migration, no UserDefaults key, no build-number
  //    compare, no Swift. It is strictly better than evicting only the copies we know we wrote,
  //    which would leave the trap half-closed for anything else that ever writes that directory.
  if (!isValidManifest(docManifest)) {
    return { source: "bundled", reason: "doc-manifest-missing-or-invalid", quarantineDoc: false };
  }

  // 4. If we cannot read the baked manifest we cannot compare, and an unverifiable comparison must
  //    not favour the mutable copy. Prefer the code-signed bytes.
  if (!isValidManifest(bakedManifest)) return bundled("baked-manifest-unreadable");

  // 5. A book that has failed to boot repeatedly is refused, even if it looks newer.
  if (isQuarantined(quarantine, docManifest.bookVersion)) return bundled("quarantined");

  // 6. The shell caught up: a TestFlight install now bakes this very book, so the Documents copy is
  //    redundant. Prefer the code-signed one — it cannot be corrupted by a downloader.
  if (docManifest.bookVersion === bakedManifest.bookVersion) return bundled("shell-caught-up");

  // 7. PREFER KNOWN-GOOD OVER OLDER. A book built by an OLDER shell than the one baked in is the
  //    leftover of a previous install: the app has since been updated and this artifact predates
  //    it. Boot the code-signed bytes. This half of the old rule was always right.
  if (Number(docManifest.builtFromShellBuild) < Number(bakedManifest.builtFromShellBuild)) {
    return bundled("baked-is-newer-shell");
  }

  // 7b. SAME SHELL — the case this entire feature exists to serve, and the one the old rule broke.
  //
  //     What was here: `bakedShell >= docShell -> bundled`. But `builtFromShellBuild` is
  //     version.json's buildNumber read at web-build time (web/build.mjs:901), so it moves ONLY
  //     when a binary is cut. A songbook-only deploy — the entire point of the OTA — therefore
  //     always produces doc == baked, and `>=` refused it. Binary-free book updates were
  //     structurally impossible, which is the exact opposite of this module's purpose.
  //
  //     It survived review because the first OTA ever attempted happened to have a TestFlight
  //     round between the two books (baked 393, doc 394), so it took the `<` branch by luck and
  //     the same-shell case was never once exercised on a device.
  //
  //     Ties break on generatedAt, which advances on EVERY web build. A missing or unparseable
  //     date on either side compares false and falls through to the code-signed bytes — a bundle
  //     of unknown provenance never wins, which keeps the fail-closed posture of the old rule.
  //
  //     SCOPED TO THE TIE DELIBERATELY. When the Documents copy comes from a strictly NEWER shell
  //     it is unambiguously the later artifact and boots on that alone, exactly as before — making
  //     generatedAt load-bearing there would newly refuse any older manifest that predates the
  //     field, i.e. break a case that has always worked.
  if (Number(docManifest.builtFromShellBuild) === Number(bakedManifest.builtFromShellBuild)) {
    const docAt = parseBuildStamp(docManifest.generatedAt);
    const bakedAt = parseBuildStamp(bakedManifest.generatedAt);
    if (!(docAt !== null && bakedAt !== null && docAt > bakedAt)) {
      return bundled("baked-not-older");
    }
  }

  // 8. A manifest-identified, non-quarantined book from a newer shell than the one baked in. This
  //    is the only path that boots a downloaded bundle.
  return { source: "documents", reason: "documents-newer", quarantineDoc: false };
};

/**
 * The self-heal ladder rung for a failed boot (§5.10 / D2).
 *
 * The old watchdog remounted the SAME uri twice and then parked on a "Reintentar" button that
 * remounted the same uri again — an unbounded human-driven loop with no escape, and it never once
 * tried the code-signed copy sitting right there. Recovery costs zero human actions now, and the
 * Multipeer session is never torn down, so a follower keeps following through the whole thing.
 */
export const nextHealAction = (attempt, source) => {
  if (attempt < 1) return { action: "remount", quarantineCurrent: false };
  // Attempt 1+: stop trusting whatever we just tried to boot.
  if (source === "documents") return { action: "fall-back", quarantineCurrent: true };
  if (source === "bundled") return { action: "give-up", quarantineCurrent: false };
  return { action: "fall-back", quarantineCurrent: false };
};
