import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { createRequire } from "node:module";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const thisDir = path.dirname(fileURLToPath(import.meta.url));
const easConfigPath = path.resolve(thisDir, "../eas.json");
const appConfigPath = path.resolve(thisDir, "../app.json");
const versionPath = path.resolve(thisDir, "../version.json");
const appRoot = path.resolve(thisDir, "..");

let resolvedExpoConfig = null;

const readJson = (filePath) => JSON.parse(fs.readFileSync(filePath, "utf8"));

const readResolvedExpoConfig = () => {
  if (resolvedExpoConfig) return resolvedExpoConfig;
  const raw = execSync("npx expo config --json", {
    cwd: appRoot,
    env: {
      ...process.env,
      EXPO_NO_DOCTOR: "1",
    },
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  resolvedExpoConfig = JSON.parse(raw);
  return resolvedExpoConfig;
};

test("EAS CLI uses local app version source", () => {
  const easConfig = readJson(easConfigPath);
  assert.equal(easConfig?.cli?.appVersionSource, "local");
});

test("EAS production build keeps store distribution and production channel", () => {
  const easConfig = readJson(easConfigPath);
  assert.equal(easConfig?.build?.production?.distribution, "store");
  assert.equal(easConfig?.build?.production?.channel, "production");
});

test("Static app config keeps standalone reader identity", () => {
  const appConfig = readJson(appConfigPath);
  assert.equal(appConfig?.expo?.name, "SignoVivo");
  assert.equal(appConfig?.expo?.slug, "alvernia-reader");
  assert.equal(appConfig?.expo?.android?.package, "com.cazares.alvernia");
  assert.equal(appConfig?.expo?.ios?.bundleIdentifier, "com.cazares.alvernia");
  assert.equal(appConfig?.expo?.extra?.eas?.projectId, "8973a6b2-a2e5-4268-97ab-4a1b2c4cb555");
});

// Every string in either app config that points at assets/, with a breadcrumb of the key it came
// from, so a failure says WHICH config field is dangling instead of just naming a filename.
const collectAssetRefs = (node, trail, out) => {
  if (typeof node === "string") {
    const m = /^\.?\/?(assets\/[^\s"']+)$/.exec(node);
    if (m) out.set(m[1], trail);
    return out;
  }
  if (Array.isArray(node)) {
    node.forEach((v, i) => collectAssetRefs(v, `${trail}[${i}]`, out));
    return out;
  }
  if (node && typeof node === "object") {
    for (const [k, v] of Object.entries(node)) collectAssetRefs(v, `${trail}.${k}`, out);
  }
  return out;
};

// PNG/JPEG magic bytes. A 0-byte or half-written image still "exists", and existsSync is happy
// with it — the build is not.
const MAGIC = {
  ".png": Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
  ".jpg": Buffer.from([0xff, 0xd8, 0xff]),
  ".jpeg": Buffer.from([0xff, 0xd8, 0xff]),
  ".pdf": Buffer.from("%PDF-", "latin1"),
};

const assertRealAsset = (relativePath, whereFrom) => {
  const fullPath = path.resolve(appRoot, relativePath);
  assert.equal(fs.existsSync(fullPath), true,
    `${whereFrom} points at ${relativePath}, which is not in the repo`);
  const stat = fs.statSync(fullPath);
  assert.equal(stat.isFile(), true, `${relativePath} (${whereFrom}) is not a regular file`);
  assert.ok(stat.size > 0, `${relativePath} (${whereFrom}) is 0 bytes — it exists but ships nothing`);
  const magic = MAGIC[path.extname(relativePath).toLowerCase()];
  if (magic) {
    const head = Buffer.alloc(magic.length);
    const fd = fs.openSync(fullPath, "r");
    try { fs.readSync(fd, head, 0, magic.length, 0); } finally { fs.closeSync(fd); }
    assert.deepEqual(head, magic,
      `${relativePath} (${whereFrom}) does not start with its format's magic bytes — truncated or replaced`);
  }
};

// The old version of this test hard-coded five paths and called existsSync on each. Two things were
// wrong with that and both were measured. It did not read the app configs at all, so its list had
// drifted to files the app no longer references (icon.png / splash.png, while the configs ship
// 03_icon_1024x1024.png / 04_splash_1668x2388.png) — renaming the icon OUT from under app.json left
// it green. And existsSync says nothing about content, so truncating the 15.8 MB songbook to zero
// bytes also left it green. The refs are derived from the configs now, and every asset must have
// real bytes with the right magic number, not merely an inode.
test("Release assets required by app config exist in-repo", () => {
  const appConfig = readJson(appConfigPath);

  // app.json is the STATIC config. app.config.js receives it and overrides some of it, so a
  // dangling path here may be masked today — but it is still the file Expo reads first, and the
  // override is one edit away from being removed. Both are held to the same bar.
  const staticRefs = collectAssetRefs(appConfig?.expo, "app.json expo", new Map());
  assert.ok(staticRefs.size >= 2,
    `app.json declares only ${staticRefs.size} asset path(s) — icon and splash at minimum are expected; ` +
    "if the shape changed, re-derive this test rather than letting it scan nothing");

  // app.config.js is real, executable code: run it the way Expo does instead of grepping it.
  const require_ = createRequire(import.meta.url);
  const withVersion = require_(path.resolve(appRoot, "app.config.js"));
  assert.equal(typeof withVersion, "function", "app.config.js no longer exports a config function");
  const dynamicRefs = collectAssetRefs(withVersion({ config: appConfig?.expo }), "app.config.js", new Map());
  assert.ok(dynamicRefs.size >= 2,
    `the resolved dynamic config declares only ${dynamicRefs.size} asset path(s) — expected icon and splash`);

  for (const [rel, whereFrom] of [...staticRefs, ...dynamicRefs]) assertRealAsset(rel, whereFrom);

  // THE BOOK. It is not named in either app config — the native shell and the web build both reach
  // for it by its stable path — but it is the one asset whose loss is unrecoverable in a building
  // with no internet, so it is checked here too, against an independent witness rather than against
  // itself: web/manifest-baseline.json records the sha256 and page count of the PDF the shipped
  // bundle was rendered from. Bytes that disagree with that mean either a truncated book or a book
  // that was changed without rebuilding the web bundle, and the field would keep serving stale pages.
  assertRealAsset("assets/songbook.pdf", "the shipped songbook");
  const baseline = readJson(path.resolve(appRoot, "web/manifest-baseline.json"));
  assert.ok(baseline?.sourcePdfSha256, "web/manifest-baseline.json has no sourcePdfSha256 to check the book against");
  const bookSha = crypto.createHash("sha256")
    .update(fs.readFileSync(path.resolve(appRoot, "assets/songbook.pdf")))
    .digest("hex");
  assert.equal(bookSha, baseline.sourcePdfSha256,
    "assets/songbook.pdf does not match the book web/manifest-baseline.json was built from — " +
    "the PDF was truncated or replaced without re-running node web/build.mjs");
});

test("Resolved Expo config preserves versioning and embedded-only updates", () => {
  const version = readJson(versionPath);
  const expoConfig = readResolvedExpoConfig();

  assert.equal(expoConfig?.version, version.baseVersion);
  assert.equal(expoConfig?.runtimeVersion, version.baseVersion);
  assert.equal(expoConfig?.newArchEnabled, false);
  assert.deepEqual(expoConfig?.platforms, ["ios", "android"]);
  assert.equal(expoConfig?.android?.package, "com.cazares.alvernia");
  assert.equal(expoConfig?.android?.versionCode, Number(version.buildNumber));
  assert.equal(expoConfig?.ios?.bundleIdentifier, "com.cazares.alvernia");
  assert.equal(expoConfig?.ios?.buildNumber, String(version.buildNumber));
  assert.equal(expoConfig?.updates?.enabled, false);
  assert.equal(expoConfig?.updates?.checkAutomatically, "NEVER");
});
