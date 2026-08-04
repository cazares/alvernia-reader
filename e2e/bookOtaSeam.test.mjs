// THE SEAM: stageBook → applyStagedBundle → decideBundle.
//
// WHY THIS FILE EXISTS. Every part of the OTA was tested and every part passed. bookUpdate.test.mjs
// proved stageBook downloads and verifies 389 files. bookResolve.test.mjs proved decideBundle picks
// correctly from a manifest. Neither ever fed one's OUTPUT into the other's INPUT — and the defect
// lived exactly there:
//
//   `bundle-manifest.json` is excluded from its own files[] (web/build.mjs:859, because a manifest
//   cannot contain its own hash), so stageBook downloaded every file EXCEPT the one that identifies
//   the bundle. The applied Documents/WebBundle was therefore unidentifiable — the precise shape of
//   the legacy mesh-pushed copies decideBundle rule 3 exists to evict. The device downloaded 27 MB,
//   verified all 389 files, swapped the directory in, and booted the baked-in book anyway.
//
// It cost a night of debugging across three TestFlight builds (391/392/393) while every server-side
// probe reported success, because the failure is invisible from outside the device.
//
// So: these tests may NEVER hand-construct the docManifest. They must read whatever stageBook
// actually left on disk, exactly as PdfReaderApp.tsx:1154 does. A test that builds its own manifest
// is testing decideBundle again — which already passed, and which never caught this.
import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import {
  stageBook,
  applyStagedBundle,
  shouldStage,
  canApplyNow,
  BUNDLE_MANIFEST_NAME,
  STAGED_READY_TTL_MS,
} from "../src/bookUpdate.js";
import { decideBundle } from "../src/bookResolve.js";

const BV_NEW = "bv_0123456789abcdef";
const BV_BAKED = "bv_fedcba9876543210";
const md5 = (s) => crypto.createHash("md5").update(s).digest("hex");

/** Mirrors e2e/bookUpdate.test.mjs's fake fs so the two suites cannot drift apart. */
const fakeFs = (initial = {}) => {
  const files = new Map(Object.entries(initial));
  return {
    files,
    stat: async (p) => (files.has(p) ? { size: files.get(p).length } : null),
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(`${p}/`)),
    mkdirp: async () => {},
    rmrf: async (p) => {
      for (const k of [...files.keys()]) if (k === p || k.startsWith(`${p}/`)) files.delete(k);
    },
    move: async (from, to) => {
      for (const k of [...files.keys()]) {
        if (k === from || k.startsWith(`${from}/`)) {
          files.set(to + k.slice(from.length), files.get(k));
          files.delete(k);
        }
      }
    },
    readJson: async (p) => {
      if (!files.has(p)) throw new Error("enoent");
      return JSON.parse(files.get(p));
    },
    writeJson: async (p, v) => {
      files.set(p, JSON.stringify(v));
    },
    walkWithHashes: async (dir) => {
      const out = new Map();
      for (const [k, v] of files) {
        if (k.startsWith(`${dir}/`)) out.set(k.slice(dir.length + 1), { size: v.length, md5: md5(v) });
      }
      return out;
    },
  };
};

/**
 * A server bundle shaped like the real one: the manifest does NOT list itself, because
 * web/build.mjs:859 filters MANIFEST_NAME out of files[]. Faking a manifest that lists itself would
 * make this whole file pass vacuously.
 */
const serverBundle = ({
  pages = 3, bookVersion = BV_NEW, builtFromShellBuild = 394,
  generatedAt = "2026-08-04T04:59:48.417Z",
} = {}) => {
  const body = { "index.html": "x".repeat(500) };
  for (let i = 1; i <= pages; i += 1) {
    body[`books/standard/pages/page-${String(i).padStart(3, "0")}.webp`] = `page${i}-bytes`;
  }
  const manifest = {
    bookVersion,
    totalPages: pages,
    pagePadWidth: 3,
    minShellBuild: 1,
    builtFromShellBuild,
    generatedAt,
    files: Object.entries(body).map(([p, v]) => ({ p, n: v.length, h: "sha", m: md5(v) })),
  };
  assert.ok(
    !manifest.files.some((f) => f.p === BUNDLE_MANIFEST_NAME),
    "fixture drift: the manifest must NOT list itself, or this suite proves nothing",
  );
  return { body, manifest };
};

let fsRef;
const fakeNet = ({ manifest, body }) => ({
  fetchJson: async () => manifest,
  download: async (url, dest) => {
    const rel = url.replace("https://signovivo.com/", "");
    if (!(rel in body)) throw new Error("404");
    fsRef.files.set(dest, body[rel]);
  },
});

/** Stage → apply → read back from disk exactly the way PdfReaderApp.tsx:1153-1156 does. */
const stageApplyResolve = async ({ bakedManifest, quarantine = [] } = {}) => {
  const { body, manifest } = serverBundle();
  fsRef = fakeFs({ "WebBundle/index.html": "old-baked-book" });
  const rec = await stageBook({
    base: "https://signovivo.com",
    bookVersion: BV_NEW,
    fs: fsRef,
    net: fakeNet({ manifest, body }),
    now: () => 111,
    shellBuild: 393,
    activeTotalPages: 2,
  });
  assert.equal(rec.ready, true, `staging failed: ${JSON.stringify(rec)}`);

  const applied = await applyStagedBundle({ fs: fsRef });
  assert.equal(applied.ok, true, `apply failed: ${JSON.stringify(applied)}`);

  const docExists = await fsRef.exists("WebBundle/index.html");
  const docManifest = await fsRef.readJson(`WebBundle/${BUNDLE_MANIFEST_NAME}`).catch(() => null);

  return {
    rec,
    docManifest,
    decision: decideBundle({
      docExists,
      docManifest,
      bakedManifest: bakedManifest ?? {
        bookVersion: BV_BAKED,
        totalPages: 2,
        builtFromShellBuild: 393,
        generatedAt: "2026-08-04T04:37:07.525Z",
      },
      bakedExists: true,
      quarantine,
    }),
  };
};

// ─── The regression itself ──────────────────────────────────────────────────

test("a staged bundle can identify itself after the swap", async () => {
  const { docManifest } = await stageApplyResolve();
  assert.notEqual(docManifest, null, "Documents/WebBundle has no bundle-manifest.json after apply");
  assert.equal(docManifest.bookVersion, BV_NEW);
  assert.equal(Number(docManifest.builtFromShellBuild), 394);
});

test("THE BUG: a downloaded book actually BOOTS instead of falling back to the baked one", async () => {
  const { decision } = await stageApplyResolve();
  assert.equal(
    decision.source,
    "documents",
    `the OTA is a no-op: decideBundle chose "${decision.source}" (${decision.reason}) after a ` +
      `perfect download+apply — this is the defect that made builds 391-393 render the baked book`,
  );
  assert.equal(decision.reason, "documents-newer");
});

test("the completeness gate still passes on a RESUMED stage (manifest must not break the count)", async () => {
  // First pass leaves bundle-manifest.json behind; the .stage.json stamp makes the second pass a
  // resume rather than a clean start. verifyStaged's `count:` check must not trip on our own stamp.
  const { body, manifest } = serverBundle();
  fsRef = fakeFs();
  const first = await stageBook({
    base: "https://signovivo.com", bookVersion: BV_NEW, fs: fsRef,
    net: fakeNet({ manifest, body }), now: () => 111, shellBuild: 393,
  });
  assert.equal(first.ready, true, JSON.stringify(first));
  assert.ok(fsRef.files.has(`WebBundleStaged/${BUNDLE_MANIFEST_NAME}`));

  const second = await stageBook({
    base: "https://signovivo.com", bookVersion: BV_NEW, fs: fsRef,
    net: fakeNet({ manifest, body }), now: () => 222, shellBuild: 393,
  });
  assert.equal(second.ready, true, `resume regressed: ${JSON.stringify(second)}`);
});

// ─── The guards that must survive the fix ───────────────────────────────────
//
// Writing an identity into the staged directory must not become a way to smuggle a bad bundle past
// the checks that were already load-bearing.

test("an INCOMPLETE stage never gains an identity", async () => {
  const { body, manifest } = serverBundle();
  delete body["books/standard/pages/page-002.webp"]; // server 404s one page
  fsRef = fakeFs();
  const rec = await stageBook({
    base: "https://signovivo.com", bookVersion: BV_NEW, fs: fsRef,
    net: fakeNet({ manifest, body }), shellBuild: 393,
  });
  assert.equal(rec.ready, false);
  assert.equal(
    fsRef.files.has(`WebBundleStaged/${BUNDLE_MANIFEST_NAME}`),
    false,
    "a failed stage wrote an identity — rule 3 would then boot an incomplete book",
  );
});

test("rule 7 still refuses a book from an OLDER shell than the baked one", async () => {
  const { decision } = await stageApplyResolve({
    bakedManifest: {
      bookVersion: BV_BAKED, totalPages: 2, builtFromShellBuild: 999,
      generatedAt: "2020-01-01T00:00:00.000Z", // older date must NOT rescue an older shell
    },
  });
  assert.equal(decision.source, "bundled");
  assert.equal(decision.reason, "baked-is-newer-shell");
});

// ─── B: a songbook-only deploy must be able to land ─────────────────────────
//
// `builtFromShellBuild` is version.json's buildNumber at web-build time (web/build.mjs:901), so it
// moves ONLY when a binary is cut. Every PDF-only deploy therefore produces doc == baked. The old
// `bakedShell >= docShell -> bundled` rule refused exactly that case, which made binary-free book
// updates structurally impossible. It was never caught because the first OTA ever attempted had a
// TestFlight round between the two books and took the `<` branch by luck.

const sameShell = (docGeneratedAt, bakedGeneratedAt) =>
  decideBundle({
    docExists: true,
    docManifest: {
      bookVersion: BV_NEW, totalPages: 374, builtFromShellBuild: 395, generatedAt: docGeneratedAt,
    },
    bakedManifest: {
      bookVersion: BV_BAKED, totalPages: 373, builtFromShellBuild: 395, generatedAt: bakedGeneratedAt,
    },
    bakedExists: true,
  });

test("THE GOAL: a PDF-only deploy on the SAME shell build lands", () => {
  const d = sameShell("2026-09-01T00:00:00.000Z", "2026-08-04T00:00:00.000Z");
  assert.equal(
    d.source,
    "documents",
    `a songbook-only update was refused (${d.reason}) — binary-free updates are the whole feature`,
  );
  assert.equal(d.reason, "documents-newer");
});

test("same shell, but the downloaded book is OLDER — refuse it", () => {
  const d = sameShell("2026-08-01T00:00:00.000Z", "2026-08-04T00:00:00.000Z");
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "baked-not-older");
});

test("same shell, identical timestamps — a tie is not newer, so refuse", () => {
  const d = sameShell("2026-08-04T00:00:00.000Z", "2026-08-04T00:00:00.000Z");
  assert.equal(d.source, "bundled");
  assert.equal(d.reason, "baked-not-older");
});

test("unknown provenance never wins: a missing or unparseable date falls back to code-signed", () => {
  for (const bad of [undefined, null, "", "not-a-date", 12345, {}]) {
    assert.equal(sameShell(bad, "2026-08-04T00:00:00.000Z").source, "bundled", `doc date ${bad}`);
    assert.equal(sameShell("2026-09-01T00:00:00.000Z", bad).source, "bundled", `baked date ${bad}`);
  }
});

test("rule 5 still refuses a quarantined book even though it is now identifiable", async () => {
  const { decision } = await stageApplyResolve({
    quarantine: [{ bookVersion: BV_NEW, failures: 3 }],
  });
  assert.equal(decision.source, "bundled");
  assert.equal(decision.reason, "quarantined");
});

// ─── C: the stale-ready deadlock ────────────────────────────────────────────
//
// ANOTHER SEAM, ANOTHER PAIR OF INDIVIDUALLY-CORRECT GATES. canApplyNow refuses a `ready` flag
// older than STAGED_READY_TTL_MS ("must re-verify before it may be applied"); shouldStage refused
// to re-stage a book it already had. Nothing in the app performs that re-verification, so past the
// TTL the device was pinned: the apply refused for being stale, the re-stage refused for already
// existing, and NEITHER state emits a breadcrumb. Each function passes its own suite.

const BOTH_GATES = (ageMs) => {
  const now = 1_000_000_000_000;
  const stagedReadyAt = now - ageMs;
  const ctx = {
    bookVersion: BV_NEW, stagedBookVersion: BV_NEW, stagedReady: true, stagedReadyAt, now,
    minShellBuild: 1, shellBuild: 395,
  };
  return {
    stage: shouldStage({ ...ctx, activeBookVersion: BV_BAKED, webReady: true, foreground: true }),
    apply: canApplyNow({ ...ctx, lastCheckinOkAt: now, webReady: true }),
  };
};

test("fresh staged book: apply is allowed, re-staging is correctly skipped", () => {
  const { stage, apply } = BOTH_GATES(60_000);
  assert.equal(apply.ok, true, `apply refused: ${apply.reason}`);
  assert.equal(stage.stage, false);
  assert.equal(stage.reason, "already-staged");
});

test("THE DEADLOCK: past the TTL, at least one gate must still let the device make progress", () => {
  const { stage, apply } = BOTH_GATES(STAGED_READY_TTL_MS + 60_000);
  assert.equal(apply.ok, false);
  assert.equal(apply.reason, "stale-ready");
  assert.equal(
    stage.stage,
    true,
    `both gates refused (apply=${apply.reason}, stage=${stage.reason}) — the device can never ` +
      `apply and can never re-verify, so this book is permanently unreachable`,
  );
  assert.equal(stage.reason, "ok");
});

test("a stale record still yields to the gates that outrank it", () => {
  const stale = STAGED_READY_TTL_MS + 60_000;
  const now = 1_000_000_000_000;
  const base = {
    bookVersion: BV_NEW, stagedBookVersion: BV_NEW, stagedReady: true,
    stagedReadyAt: now - stale, now, webReady: true, foreground: true, activeBookVersion: BV_BAKED,
    minShellBuild: 1, shellBuild: 395,
  };
  assert.equal(shouldStage({ ...base, killSwitch: true }).reason, "kill-switch");
  assert.equal(shouldStage({ ...base, activeBookVersion: BV_NEW }).reason, "already-active");
  assert.equal(
    shouldStage({ ...base, quarantine: [{ bookVersion: BV_NEW, failures: 3 }] }).reason,
    "quarantined",
  );
  assert.equal(shouldStage({ ...base, webReady: false }).reason, "not-web-ready");
});
