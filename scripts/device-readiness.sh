#!/usr/bin/env bash
# device-readiness — can I read this device's log over the wire, and if not, why not?
#
# Reading a device's unified log needs FOUR things, and each fails silently and differently:
#   1. a DATA cable (a charge-only cable pairs nothing and is the single most common cause)
#   2. the device UNLOCKED
#   3. this Mac TRUSTED on the device ("Trust This Computer?")
#   4. Developer Mode ON (iOS 16+) for the devicectl path
#
# Two tools see different things and neither is complete: xctrace lists devices by HARDWARE UDID —
# which is what `log collect --device-udid` wants — while devicectl reports Developer Mode and the
# installed build but keys on its own CoreDevice UUID. This joins them.
#
# Usage: bash scripts/device-readiness.sh
set -uo pipefail

echo "=== ONLINE (log collect will work) ==="
xcrun xctrace list devices 2>/dev/null \
  | sed -n '/^== Devices ==/,/^== Devices Offline ==/p' \
  | grep -vE "^==|MacBook|^$" \
  | while read -r line; do
      udid=$(echo "$line" | grep -oE '\([0-9A-Fa-f-]{25,}\)$' | tr -d '()')
      name=$(echo "$line" | sed -E 's/ \([^)]*\) \([^)]*\)$//')
      [ -z "$udid" ] && continue
      printf "  %-22s %s\n" "$name" "$udid"
    done

echo
echo "=== OFFLINE (plugged in? unlocked? trusted? DATA cable?) ==="
xcrun xctrace list devices 2>/dev/null \
  | sed -n '/^== Devices Offline ==/,/^== Simulators ==/p' \
  | grep -vE "^==|^$" \
  | while read -r line; do
      udid=$(echo "$line" | grep -oE '\([0-9A-Fa-f-]{25,}\)$' | tr -d '()')
      name=$(echo "$line" | sed -E 's/ \([^)]*\) \([^)]*\)$//')
      [ -z "$udid" ] && continue
      printf "  %-22s %s\n" "$name" "$udid"
    done

echo
echo "=== per-device detail (Developer Mode + installed build) ==="
for ID in $(xcrun devicectl list devices 2>/dev/null | tail -n +3 \
            | grep -oE '[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}' | sort -u); do
  D=$(xcrun devicectl device info details --device "$ID" 2>/dev/null)
  NAME=$(echo "$D" | grep -i marketingName | head -1 | sed 's/.*: //' | cut -c1-24)
  [ -z "$NAME" ] && continue
  DM=$(echo "$D" | grep -i developerModeStatus | head -1 | sed 's/.*: //')
  TUN=$(echo "$D" | grep -i tunnelState | head -1 | sed 's/.*: //')
  APP=$(xcrun devicectl device info apps --device "$ID" 2>/dev/null | grep "com.cazares.alvernia " | head -1 | awk '{print $NF}')
  printf "  %-24s tunnel=%-12s devmode=%-9s build=%s\n" "$NAME" "${TUN:-?}" "${DM:-?}" "${APP:-unreachable}"
done
