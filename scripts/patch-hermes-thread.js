#!/usr/bin/env node
/**
 * Patches HermesExecutorFactory.cpp to add #include <thread>, which is
 * missing in RN 0.81 and causes a compile failure against the Xcode 26 SDK
 * where libc++ no longer implicitly pulls in <thread> transitively.
 */

const fs = require("fs");
const path = require("path");

const target = path.join(
  __dirname,
  "..",
  "node_modules",
  "react-native",
  "ReactCommon",
  "hermes",
  "executor",
  "HermesExecutorFactory.cpp",
);

if (!fs.existsSync(target)) {
  console.log("[patch-hermes-thread] HermesExecutorFactory.cpp not found — skipping.");
  process.exit(0);
}

const marker = '#include "HermesExecutorFactory.h"';
const insert = '#include <thread>';

const content = fs.readFileSync(target, "utf8");

if (content.includes(insert)) {
  console.log("[patch-hermes-thread] Already patched — nothing to do.");
  process.exit(0);
}

if (!content.includes(marker)) {
  console.warn("[patch-hermes-thread] Anchor not found — file may have changed. Skipping.");
  process.exit(0);
}

fs.writeFileSync(target, content.replace(marker, `${marker}\n${insert}`), "utf8");
console.log("[patch-hermes-thread] ✔ Patched HermesExecutorFactory.cpp: added #include <thread>.");
