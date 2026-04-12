#!/usr/bin/env node
/**
 * Patches react-native-webview's RNCWebViewImpl.m to fix a bug where
 * file:// URLs never reach loadFileURL:allowingReadAccessToURL:.
 *
 * Root cause: `if (request.URL.host)` is truthy for file:// URLs because
 * NSURL.host returns @"" (empty NSString, non-nil) — falsy in Swift but
 * TRUTHY in Objective-C. So loadRequest: is always called and
 * allowingReadAccessToURL is silently ignored.
 *
 * Fix: check host.length > 0 instead.
 *
 * This script is run via npm postinstall so it applies after every
 * `npm install` — including on EAS Build servers.
 */

const fs = require("fs");
const path = require("path");

const target = path.join(
  __dirname,
  "..",
  "node_modules",
  "react-native-webview",
  "apple",
  "RNCWebViewImpl.m",
);

if (!fs.existsSync(target)) {
  console.log("[patch-rn-webview] RNCWebViewImpl.m not found — skipping.");
  process.exit(0);
}

const original = "if (request.URL.host) {";
const patched  = "if ([request.URL.host length] > 0) {";

const content = fs.readFileSync(target, "utf8");

if (content.includes(patched)) {
  console.log("[patch-rn-webview] Already patched — nothing to do.");
  process.exit(0);
}

if (!content.includes(original)) {
  console.warn("[patch-rn-webview] Pattern not found — react-native-webview may have changed. Skipping.");
  process.exit(0);
}

fs.writeFileSync(target, content.replace(original, patched), "utf8");
console.log("[patch-rn-webview] ✔ Patched RNCWebViewImpl.m: file:// URL host-length fix applied.");
