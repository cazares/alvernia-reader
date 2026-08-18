#!/usr/bin/env node
/**
 * logarchive-to-jsonl — turn a device's own os_log into the same JSONL the relay capture produces,
 * so every existing analyzer works on a cable instead of a network.
 *
 * WHY. Until build 438 this app wrote its breadcrumbs to Cloudflare and nowhere else, so a device
 * could only explain itself when the relay was reachable — never at Mass, where the followers are on
 * no network at all, and not on 2026-08-17 when the account's daily quota was exhausted and the
 * whole fleet went mute mid-investigation. 438 mirrors every dbgLog to os_log under
 * `com.cazares.signovivo`. This converts that stream into the shape stress-capture.mjs writes, which
 * means analyze-resync.mjs and analyze-join-latency.mjs work unchanged against a USB cable, an
 * offline device, or a sysdiagnose pulled after a Mass.
 *
 * It is deliberately a CONVERTER rather than a second family of analyzers: two tools that score the
 * same question differently is how you end up trusting whichever one agrees with you.
 *
 * Getting an archive (needs sudo — reading another device's log is privileged):
 *   sudo /usr/bin/log collect --device-udid <UDID> --last 20m --output run.logarchive
 *
 * Then:
 *   node scripts/logarchive-to-jsonl.mjs run.logarchive > run.jsonl
 *   node scripts/analyze-join-latency.mjs run.jsonl --build 441
 *
 * The emitted line format (DirectorSyncModule.dbgLog) is:
 *   "<role> <peerName> <event> k=v k=v ..."
 */
import { execFileSync } from "node:child_process";

const archive = process.argv[2];
if (!archive) {
  console.error("usage: node scripts/logarchive-to-jsonl.mjs <path.logarchive> [> out.jsonl]");
  process.exit(2);
}

let raw = "";
try {
  raw = execFileSync(
    "/usr/bin/log",
    [
      "show",
      "--archive", archive,
      "--style", "compact",
      "--predicate", 'subsystem == "com.cazares.signovivo"',
    ],
    { encoding: "utf8", maxBuffer: 512 * 1024 * 1024 },
  );
} catch (e) {
  // `log` is also a zsh BUILTIN — invoking it unqualified from a shell silently hits the builtin and
  // fails with "too many arguments". That cost an hour on 2026-08-17 and is why the absolute path is
  // hard-coded above rather than resolved from PATH.
  console.error(`✖ /usr/bin/log show failed: ${e.message}`);
  process.exit(2);
}

const lines = raw.split("\n");
let emitted = 0;
let skipped = 0;

for (const line of lines) {
  // compact style: "2026-08-17 21:06:26.364 Df SignoVivo[3971:7a147] [com.cazares.signovivo:mesh] <msg>"
  const m = line.match(/^(\d{4}-\d{2}-\d{2} [\d:.]+)\s+\S+\s+\S+\s+\[com\.cazares\.signovivo:\w+\]\s+(.*)$/);
  if (!m) continue;
  const [, stamp, msg] = m;

  // The device log has no timezone suffix and `log show` renders in the HOST's local time. Parsing
  // it as local is therefore correct here — but it means `t` is only comparable WITHIN one archive,
  // which is all any analyzer does with it.
  const t = new Date(stamp.replace(" ", "T")).getTime();
  if (!Number.isFinite(t)) { skipped++; continue; }

  const parts = msg.trim().split(/\s+/);
  if (parts.length < 3) { skipped++; continue; }
  const [role, dev, event, ...rest] = parts;

  const row = { t, dev, role, event, src: "swift", build: null };
  for (const kv of rest) {
    const i = kv.indexOf("=");
    if (i <= 0) continue;
    const k = kv.slice(0, i);
    const v = kv.slice(i + 1);
    row[k] = /^-?\d+$/.test(v) ? Number(v) : v === "true" ? true : v === "false" ? false : v;
  }
  process.stdout.write(JSON.stringify(row) + "\n");
  emitted++;
}

if (!emitted) {
  console.error(
    "✖ no com.cazares.signovivo entries found — INCONCLUSIVE (exit 2).\n" +
    "  Is the device on build 438 or later? Earlier builds wrote NOTHING locally, and an empty\n" +
    "  result there means 'this build cannot speak', not 'nothing happened'.",
  );
  process.exit(2);
}
console.error(`converted ${emitted} rows${skipped ? ` (${skipped} unparseable)` : ""}`);
