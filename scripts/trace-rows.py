#!/usr/bin/env python3
"""Print device-trace rows (JSONL from scripts/pull-device-trace.sh) in a Central-time window.

Usage:
  python3 scripts/trace-rows.py <trace.jsonl> <start HH:MM[:SS]> <end HH:MM[:SS]> [event-prefix ...]

Times are US Central (CT = UTC-5 during daylight time; override with --utc-offset H).
Every field of each row is printed, so peripheral ids, nonces, rssi etc. are visible —
the auto-analyzers summarise; this shows the raw evidence in the owner's clock.

Written 2026-09-05 while reading the three-device hardware test that found the stale BLE
advertisement replay (nonce f88c page 82) and the 45 s takeover demotion.
"""
import datetime
import json
import sys


def parse_clock(s):
    parts = [int(x) for x in s.split(":")]
    while len(parts) < 3:
        parts.append(0)
    return datetime.time(*parts)


def main(argv):
    offset = -5
    if "--utc-offset" in argv:
        i = argv.index("--utc-offset")
        offset = int(argv[i + 1])
        del argv[i:i + 2]
    if len(argv) < 4:
        print(__doc__)
        return 2
    path, start, end = argv[1], parse_clock(argv[2]), parse_clock(argv[3])
    prefixes = argv[4:]
    tz = datetime.timezone(datetime.timedelta(hours=offset))
    with open(path) as fh:
        for line in fh:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if "t" not in row:
                continue
            ts = datetime.datetime.fromtimestamp(row["t"] / 1000, tz)
            if not (start <= ts.time() <= end):
                continue
            ev = str(row.get("event", ""))
            if prefixes and not any(ev.startswith(p) for p in prefixes):
                continue
            extra = {k: v for k, v in row.items() if k not in ("t", "dev", "build", "src")}
            print(ts.strftime("%H:%M:%S.%f")[:-3], row.get("role", "?"), extra)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
