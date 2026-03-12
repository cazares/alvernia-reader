#!/usr/bin/env bash

set -euo pipefail

STRICT_MODE=0
if [ "${1:-}" = "--strict" ] || [ "${OTA_DOCTOR_STRICT:-0}" = "1" ]; then
  STRICT_MODE=1
fi

npx expo config --json \
  | node -e '
let raw = "";
process.stdin.on("data", (chunk) => (raw += chunk));
process.stdin.on("end", () => {
  const strict = process.argv[1] === "1";
  let cfg;
  try {
    cfg = JSON.parse(raw);
  } catch (err) {
    console.error("[ota:doctor] Failed to parse expo config JSON.");
    process.exit(1);
  }

  const eas = (cfg.extra && cfg.extra.eas) || {};
  const ota = (cfg.extra && cfg.extra.ota) || {};
  const updates = cfg.updates || {};

  const projectId = String(eas.projectId || "").trim();
  const channel = String(ota.channel || "production").trim();
  const autoCheck = ota.autoCheckOnForeground !== false;
  const updatesEnabled = Boolean(updates.enabled);
  const updatesUrl = String(updates.url || "").trim();
  const runtimeVersion =
    typeof cfg.runtimeVersion === "string"
      ? cfg.runtimeVersion
      : JSON.stringify(cfg.runtimeVersion || {});

  console.log("[ota:doctor] OTA config summary");
  console.log(`[ota:doctor] projectId: ${projectId || "(missing)"}`);
  console.log(`[ota:doctor] channel: ${channel}`);
  console.log(`[ota:doctor] updates.enabled: ${updatesEnabled}`);
  console.log(`[ota:doctor] updates.url: ${updatesUrl || "(missing)"}`);
  console.log(`[ota:doctor] autoCheckOnForeground: ${autoCheck}`);
  console.log(`[ota:doctor] runtimeVersion: ${runtimeVersion}`);

  const failures = [];
  if (strict) {
    if (!projectId) failures.push("projectId is missing");
    if (!updatesEnabled) failures.push("updates.enabled is false");
    if (!updatesUrl) failures.push("updates.url is missing");
  }

  if (!strict) {
    if (!projectId) console.log("[ota:doctor] warning: missing EXPO_PUBLIC_EAS_PROJECT_ID.");
    if (!updatesEnabled) console.log("[ota:doctor] warning: OTA updates are currently disabled.");
  }

  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(`[ota:doctor] error: ${failure}`);
    }
    process.exit(1);
  }
});
' "$STRICT_MODE"
