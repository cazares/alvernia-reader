import fs from "node:fs";
import path from "node:path";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");

const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8"));
const writeJson = (p, v) => fs.writeFileSync(p, JSON.stringify(v, null, 2) + "\n");

const versionPath = path.join(root, "version.json");
const version = readJson(versionPath);

const baseVersion = String(version.baseVersion || "1.0").trim();
const nextBuild = Math.max(1, Number(version.buildNumber || 1) + 1);

version.baseVersion = baseVersion;
version.buildNumber = nextBuild;
writeJson(versionPath, version);

// Keep the offline-web bundle version in lockstep with build number.
const offlineBundlePath = path.join(root, "src", "offlineWebBundle.js");
if (fs.existsSync(offlineBundlePath)) {
  const src = fs.readFileSync(offlineBundlePath, "utf8");
  const out = src.replace(
    /^const OFFLINE_WEB_BUNDLE_VERSION = "(\d+)";/m,
    `const OFFLINE_WEB_BUNDLE_VERSION = "${nextBuild}";`,
  );
  if (out !== src) fs.writeFileSync(offlineBundlePath, out);
}

// Keep Xcode build number in sync (CFBundleVersion = CURRENT_PROJECT_VERSION).
const pbxprojPath = path.join(root, "ios", "SignoVivo.xcodeproj", "project.pbxproj");
if (fs.existsSync(pbxprojPath)) {
  const src = fs.readFileSync(pbxprojPath, "utf8");
  const out = src.replace(/CURRENT_PROJECT_VERSION = \d+;/g, `CURRENT_PROJECT_VERSION = ${nextBuild};`);
  if (out !== src) fs.writeFileSync(pbxprojPath, out);
}

// Best-effort sync for app.json (some setups still read it in places).
const appJsonPath = path.join(root, "app.json");
if (fs.existsSync(appJsonPath)) {
  const appJson = readJson(appJsonPath);
  if (appJson?.expo?.ios) appJson.expo.ios.buildNumber = String(nextBuild);
  if (appJson?.expo?.android) appJson.expo.android.versionCode = nextBuild;
  writeJson(appJsonPath, appJson);
}

console.log(`Bumped build -> ${baseVersion}.${nextBuild}`);

