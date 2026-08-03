#!/usr/bin/env node
/**
 * sim-bundle-lab — plant a Documents/WebBundle state on a booted simulator and see what boots.
 *
 * WHY THIS EXISTS. The boot resolver (src/bookResolve.js) decides which songbook eight iPads
 * render in a building with no internet. Its unit tests cover the decision table, but they cannot
 * prove the wiring: that `FileSystem.documentDirectory` is where we think it is, that a manifest-
 * less directory really is refused on a device, that the self-heal ladder actually fires and
 * actually falls back. Defect D1 was invisible to every static check for months and was only ever
 * caught by planting a stale bundle on a simulator and watching the app render the wrong book
 * while reporting the right build number.
 *
 * The states it can plant map 1:1 to the failure modes that matter:
 *
 *   legacy   a bundle with NO bundle-manifest.json — the exact shape every mesh-pushed copy in
 *            the field has. MUST be refused; this is the D1 fix, and refusing it is what retires
 *            the trap across the whole installed base on first launch.
 *   valid    a well-formed bundle from a NEWER shell build. MUST be adopted — without this case
 *            the "legacy" result proves nothing, because a resolver that always returns the baked
 *            bundle would pass it trivially.
 *   broken   a well-formed bundle whose index.html never completes the bridge handshake. MUST be
 *            quarantined by the self-heal ladder and fall back with ZERO taps.
 *   clean    remove everything and reset the resolver's cached decision.
 *
 * Every planted page is visibly labelled, because the whole class of bug being hunted is one where
 * the app looks completely fine while rendering the wrong book.
 *
 * Usage:
 *   node scripts/sim-bundle-lab.mjs --state legacy|valid|broken|clean [--udid <udid>]
 *   node scripts/sim-bundle-lab.mjs --inspect            # what is on the device right now
 *
 * Simulator only. Requires a booted device and the app installed at least once.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const BUNDLE_ID = "com.cazares.alvernia";
const argv = process.argv.slice(2);
const arg = (n, d = null) => {
  const i = argv.indexOf(n);
  return i === -1 ? d : (argv[i + 1] ?? d);
};

const sh = (cmd, args) => {
  const r = spawnSync(cmd, args, { encoding: "utf8" });
  if (r.status !== 0) throw new Error(`${cmd} ${args.join(" ")} failed:\n${r.stderr || r.stdout}`);
  return (r.stdout || "").trim();
};

const bootedUdid = () => {
  const out = sh("xcrun", ["simctl", "list", "devices", "booted"]);
  const m = out.match(/\(([0-9A-F-]{36})\)\s*\(Booted\)/);
  if (!m) throw new Error("No booted simulator. Boot one first.");
  return m[1];
};

const udid = arg("--udid") || bootedUdid();
const dataDir = sh("xcrun", ["simctl", "get_app_container", udid, BUNDLE_ID, "data"]);
const docs = path.join(dataDir, "Documents");
const webBundle = path.join(docs, "WebBundle");

/** A minimal but REAL web bundle: it must be able to complete the native bridge handshake. */
const plantBundle = ({ label, manifest, handshake }) => {
  fs.rmSync(webBundle, { recursive: true, force: true });
  fs.mkdirSync(webBundle, { recursive: true });
  fs.writeFileSync(
    path.join(webBundle, "index.html"),
    `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${label}</title></head>
<body style="background:#4a0072;color:#fff;font:700 40px/1.3 -apple-system,sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0;text-align:center">
<div>PLANTED BUNDLE<br><span style="font-size:64px">${label}</span></div>
<script>
${handshake
  ? `// Complete the handshake the native shell waits for, so this counts as a healthy boot.
     setTimeout(function(){
       try {
         window.ReactNativeWebView.postMessage(JSON.stringify({
           channel:"signovivo-native", type:"bridge-ready", page:1, totalPages:373, book:"standard"
         }));
       } catch(e) {}
     }, 50);`
  : `// DELIBERATELY SILENT: never posts bridge-ready, so the watchdog must escalate.
     // This is what a bundle that "loads" but is broken actually looks like.`}
</script>
</body></html>`,
  );
  if (manifest) {
    fs.writeFileSync(path.join(webBundle, "bundle-manifest.json"), JSON.stringify(manifest, null, 2));
  }
};

const resetResolverState = () => {
  // The resolver caches its decision (sv.book.resolved) and tracks boot health (sv.book.boot,
  // sv.book.quarantine, sv.book.reverted). A test that leaves those behind measures the PREVIOUS
  // test. RN's AsyncStorage is a plain SQLite/manifest store under RCTAsyncLocalStorage_V1.
  for (const dir of ["RCTAsyncLocalStorage_V1", "RCTAsyncLocalStorage"]) {
    fs.rmSync(path.join(dataDir, "Library", "Application Support", dir), { recursive: true, force: true });
    fs.rmSync(path.join(docs, dir), { recursive: true, force: true });
  }
};

const state = arg("--state");

if (argv.includes("--inspect") || !state) {
  console.log(`device : ${udid}`);
  console.log(`data   : ${dataDir}`);
  const entries = fs.existsSync(docs) ? fs.readdirSync(docs).filter((n) => n.startsWith("WebBundle")) : [];
  console.log(`Documents WebBundle* : ${entries.length ? entries.join(", ") : "(none)"}`);
  if (fs.existsSync(path.join(webBundle, "bundle-manifest.json"))) {
    const m = JSON.parse(fs.readFileSync(path.join(webBundle, "bundle-manifest.json"), "utf8"));
    console.log(`  manifest: ${m.bookVersion} totalPages=${m.totalPages} shell=${m.builtFromShellBuild}`);
  } else if (fs.existsSync(webBundle)) {
    console.log("  manifest: NONE (the legacy / mesh-pushed shape)");
  }
  if (!state) process.exit(0);
}

switch (state) {
  case "clean":
    fs.rmSync(webBundle, { recursive: true, force: true });
    for (const n of fs.existsSync(docs) ? fs.readdirSync(docs) : []) {
      if (n.startsWith("WebBundle")) fs.rmSync(path.join(docs, n), { recursive: true, force: true });
    }
    resetResolverState();
    console.log("✅ cleaned: no Documents bundle, resolver state reset");
    break;

  case "legacy":
    // The D1 shape: a real bundle, no manifest. Every mesh-pushed copy in the field looks like this.
    resetResolverState();
    plantBundle({ label: "LEGACY (no manifest)", manifest: null, handshake: true });
    console.log("✅ planted: manifest-less bundle — MUST be refused (D1)");
    break;

  case "valid":
    // The control case. Without it, "legacy was refused" is unfalsifiable.
    resetResolverState();
    plantBundle({
      label: "VALID (newer shell)",
      manifest: {
        schema: 1,
        bookVersion: "bv_deadbeefdeadbeef",
        totalPages: 373,
        builtFromShellBuild: 99999,
        minShellBuild: 1,
        pagePadWidth: 3,
      },
      handshake: true,
    });
    console.log("✅ planted: valid newer bundle — MUST be adopted");
    break;

  case "broken":
    // Well-formed and newer, so the resolver adopts it — and then it never handshakes.
    resetResolverState();
    plantBundle({
      label: "BROKEN (never handshakes)",
      manifest: {
        schema: 1,
        bookVersion: "bv_badbadbadbadbad0",
        totalPages: 373,
        builtFromShellBuild: 99999,
        minShellBuild: 1,
        pagePadWidth: 3,
      },
      handshake: false,
    });
    console.log("✅ planted: adopted-then-broken bundle — the ladder MUST self-heal with zero taps");
    break;

  default:
    console.error(`unknown --state ${state}`);
    process.exit(2);
}
