#!/usr/bin/env node
// log-sink.mjs — a telemetry sink that runs on THIS Mac, not on Cloudflare.
//
// WHY. Device telemetry used to POST to the Cloudflare worker, which shares one 100,000-request
// daily quota with signovivo.com. Debugging therefore competed with the product for the same
// budget, and twice — 2026-08-17 and 2026-08-18 — telemetry won and took the site down.
//
// During a debugging session the devices are on home wifi, three metres from this Mac. There is
// no reason for their breadcrumbs to cross the internet at all. This sink costs nothing, has no
// quota, no signup and no tunnel, and it accepts the exact payload shape the worker's POST /log
// does — so pointing a device here is a URL change, not a code change.
//
//   node scripts/log-sink.mjs                 # listens on 0.0.0.0:8787, writes ./sv-log-<date>.jsonl
//   node scripts/log-sink.mjs --port 9000 --out /tmp/sv.jsonl
//
// Then on each device (Ir a Canto -> diagnostics, or via AsyncStorage): set sv.logSink to the URL
// this prints, and set sv.telemetry to "1". Unset sv.logSink to fall back to the worker.
//
// Prints every line as it arrives so a `tail -f` is unnecessary, and appends JSONL so the existing
// analysis scripts (analyze-join-latency.mjs, analyze-resync.mjs) can read it directly.
import http from "node:http";
import fs from "node:fs";
import os from "node:os";

const arg = (n, d) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? process.argv[i + 1] : d; };
const PORT = Number(arg("port", 8787));
const OUT = arg("out", `sv-log-${new Date().toISOString().slice(0, 10)}.jsonl`);

const lanIp = () => {
  for (const list of Object.values(os.networkInterfaces())) {
    for (const i of list || []) if (i.family === "IPv4" && !i.internal) return i.address;
  }
  return "127.0.0.1";
};

let count = 0;
const stream = fs.createWriteStream(OUT, { flags: "a" });

const server = http.createServer((req, res) => {
  // Permissive CORS on purpose: this is a LAN debugging tool, it holds no secrets, and a preflight
  // failure at 8am on a Sunday is exactly the kind of friction that makes people stop instrumenting.
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  if (req.method === "GET") {
    // Mirrors the worker's GET /log shape closely enough to be useful, and doubles as a "is this
    // thing on?" check from a device browser.
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, received: count, out: OUT }));
    return;
  }

  let body = "";
  req.on("data", (c) => { body += c; if (body.length > 1e7) req.destroy(); });
  req.on("end", () => {
    let rows = [];
    try {
      const parsed = JSON.parse(body || "[]");
      rows = Array.isArray(parsed) ? parsed : [parsed];
    } catch { rows = [{ t: Date.now(), event: "sink:unparseable", raw: body.slice(0, 400) }]; }
    for (const r of rows) {
      count += 1;
      stream.write(JSON.stringify(r) + "\n");
      const t = new Date(Number(r.t) || Date.now()).toLocaleTimeString("en-US", { hour12: false });
      const dev = String(r.dev ?? r.deviceId ?? "?").padEnd(8);
      const role = String(r.role ?? "").padEnd(8);
      const ev = String(r.event ?? "?").padEnd(26);
      const rest = Object.entries(r)
        .filter(([k]) => !["t", "dev", "deviceId", "role", "event", "build"].includes(k))
        .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`)
        .join(" ");
      console.log(`${t} ${dev} ${role} ${ev} ${rest}`);
    }
    // The client reads this policy back, exactly as it does from the worker — so the sink can also
    // retune the fleet's flush interval and level without a rebuild.
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, policy: { logIntervalMs: 2000, logLevel: 4 } }));
  });
});

server.listen(PORT, "0.0.0.0", () => {
  const url = `http://${lanIp()}:${PORT}`;
  console.log(`sv log sink listening on ${url}`);
  console.log(`writing ${OUT}`);
  console.log("");
  console.log("On each device set:  sv.logSink = " + url + "   and  sv.telemetry = 1");
  console.log("Costs Cloudflare nothing. Ctrl-C to stop.");
  console.log("");
});
