#!/usr/bin/env bash
# Mutation probes for the songbook-OTA gate tests (e2e/bookUpdate.test.mjs).
#
# WHY THIS EXISTS. Every test added for H1/H2/H3/M1 was written to catch a SPECIFIC regression, and
# a test that passes both before and after the bug is not a test — it is decoration. The reviewer of
# the 2026-08-03 "no role is exempt" amendment proved that point the hard way: a ONE-LINE early
# return in PdfReaderApp.tsx restored the entire director exemption with all 48 tests still green.
#
# So each probe below reintroduces one regression, verbatim, and asserts the suite goes RED. A probe
# that stays green is reported as a HOLE — that is the interesting result, not the failure.
#
# Nothing here is destructive: each target file is copied to a .probe-backup before mutation and
# restored from that copy afterwards, including on interrupt. It never touches git, so it is safe to
# run against an uncommitted working tree (which is exactly when you want it).
#
# Usage:  bash scripts/mutation-probe-ota-gates.sh
# Exit:   0 = every probe reddened the suite; 1 = at least one hole found.

set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

APP="$ROOT/PdfReaderApp.tsx"
GATES="$ROOT/src/bookUpdate.js"
TEST="e2e/bookUpdate.test.mjs"

backup() { cp "$APP" "$APP.probe-backup"; cp "$GATES" "$GATES.probe-backup"; }
restore() {
  [ -f "$APP.probe-backup" ] && mv "$APP.probe-backup" "$APP"
  [ -f "$GATES.probe-backup" ] && mv "$GATES.probe-backup" "$GATES"
  return 0
}
trap restore EXIT INT TERM

holes=0
n=0

# probe <name> <mutation-shell-command>
probe() {
  local name="$1"; shift
  n=$((n + 1))
  backup
  if ! eval "$@"; then
    printf '  ??  %-58s MUTATION DID NOT APPLY (pattern drifted)\n' "$name"
    holes=$((holes + 1)); restore; return
  fi
  local out
  out="$(node --test "$TEST" 2>&1)"
  local failed
  failed="$(printf '%s' "$out" | grep -c '^not ok ' || true)"
  restore
  if [ "$failed" -gt 0 ]; then
    printf '  ok  %-58s → %s test(s) RED\n' "$name" "$failed"
    printf '%s' "$out" | grep '^not ok ' | sed 's/^/        /'
  else
    printf '  HOLE %-57s → suite stayed GREEN\n' "$name"
    holes=$((holes + 1))
  fi
}

echo "── Mutation probes: songbook OTA gates ─────────────────────────────────────"

# ── H1: the room-activity window ───────────────────────────────────────────────
probe "H1 delete the room-active veto (the shipped regression)" \
  "perl -0pi -e 's/  if \\(lastMeshPageAt != null && now - lastMeshPageAt < ROOM_ACTIVE_WINDOW_MS\\) \\{\n    return \\{ ok: false, reason: \"room-active\" \\};\n  \\}\n//' '$GATES'"

probe "H1 collapse the window back to 60 s" \
  "perl -0pi -e 's/export const ROOM_ACTIVE_WINDOW_MS = 10 \\* 60 \\* 1000;/export const ROOM_ACTIVE_WINDOW_MS = 60 * 1000;/' '$GATES'"

probe "H1 make the window role-derived again" \
  "perl -0pi -e 's/if \\(lastMeshPageAt != null && now - lastMeshPageAt < ROOM_ACTIVE_WINDOW_MS\\)/if (ctx.role !== \"director\" && lastMeshPageAt != null && now - lastMeshPageAt < ROOM_ACTIVE_WINDOW_MS)/' '$GATES'"

# ── H2: the director's own page turns ──────────────────────────────────────────
probe "H2 stop stamping on the device's OWN page turn" \
  "perl -0pi -e 's/          if \\(webReadyRef\\.current\\) noteRoomPageActivity\\(\\);\n//' '$APP'"

probe "H2 stop stamping on mesh RECEIVE" \
  "perl -0pi -e 's/          noteRoomPageActivity\\(\\);\n          \\/\\/ De-dupe/          \\/\\/ De-dupe/' '$APP'"

probe "H2 writer stops feeding the 10-minute clock" \
  "perl -0pi -e 's/    lastMeshPageAtRef\\.current = at;\n//' '$APP'"

probe "H2 clear the clocks on a role transition (becomeFollower)" \
  "perl -0pi -e 's/    roleRef\\.current = \"follower\";/    roleRef.current = \"follower\";\n    lastPageTurnAtRef.current = null;/' '$APP'"

probe "H2 drop lastMeshPageAt on the way into canApplyNow" \
  "perl -0pi -e 's/      lastMeshPageAt: lastMeshPageAtRef\\.current,\n//g' '$APP'"

# ── The role-exemption hole the reviewer demonstrated ──────────────────────────
probe "ROLE one-line early return above canApplyNow (autoApply)" \
  "perl -0pi -e 's/  const autoApplyIfSafe = useCallback\\(async \\(\\) => \\{\n/  const autoApplyIfSafe = useCallback(async () => {\n    if (roleRef.current === \"director\") return;\n/' '$APP'"

probe "ROLE one-line early return above shouldStage (check-in)" \
  "perl -0pi -e 's/        if \\(stagingInFlightRef\\.current\\) return;/        if (stagingInFlightRef.current) return;\n        if (roleRef.current === \"director\") return;/' '$APP'"

probe "ROLE re-grow a role key on the gate context" \
  "perl -0pi -e 's/      meshPeerConnected: meshPeerCountRef\\.current > 0,/      meshPeerConnected: meshPeerCountRef.current > 0,\n      role: roleRef.current,/' '$APP'"

probe "ROLE put a role check back inside canApplyNow itself" \
  "perl -0pi -e 's/  if \\(!stagedReady\\) return \\{ ok: false, reason: \"not-ready\" \\};/  if (ctx.role === \"director\") return { ok: false, reason: \"director\" };\n  if (!stagedReady) return { ok: false, reason: \"not-ready\" };/' '$GATES'"

# ── H3: foreground ordering ────────────────────────────────────────────────────
probe "H3 apply immediately on foreground, before rediscovery" \
  "perl -0pi -e 's/      if \\(syncAvailable\\) \\{\n        \\/\\/ \`\\.finally\`/      void autoApplyIfSafeRef.current?.();\n      if (syncAvailable) {\n        \\/\\/ \`.finally\`/' '$APP'"

probe "H3 drop the settle window (reorder only, no deferral)" \
  "perl -0pi -e 's/    \\}, FOREGROUND_APPLY_SETTLE_MS\\);/    }, 0);/' '$APP'"

probe "H3 stop re-checking AppState at fire time" \
  "perl -0pi -e 's/      if \\(AppState\\.currentState !== \"active\"\\) return;\n//' '$APP'"

# ── M1: the stale-ready dead end ───────────────────────────────────────────────
probe "M1 restore the unconditional already-staged short-circuit" \
  "perl -0pi -e 's/  if \\(haveStaged && !stagedExpired\\) return \\{ stage: false, reason: \"already-staged\" \\};/  if (haveStaged) return { stage: false, reason: \"already-staged\" };/' '$GATES'"

probe "M1 let a MISSING stagedReadyAt skip the apply TTL check" \
  "perl -0pi -e 's/  if \\(stagedReadyAt == null \\|\\| now - stagedReadyAt > STAGED_READY_TTL_MS\\) \\{/  if (stagedReadyAt != null \\&\\& now - stagedReadyAt > STAGED_READY_TTL_MS) {/' '$GATES'"

probe "M1 let a MISSING stagedReadyAt count as fresh when staging" \
  "perl -0pi -e 's/  const stagedExpired = stagedReadyAt == null \\|\\| now - stagedReadyAt > STAGED_READY_TTL_MS;/  const stagedExpired = stagedReadyAt != null \\&\\& now - stagedReadyAt > STAGED_READY_TTL_MS;/' '$GATES'"

probe "M1 stop passing stagedReadyAt from the app to shouldStage" \
  "perl -0pi -e 's/          stagedReadyAt: staged\\?\\.readyAt \\?\\? null,\n          quarantine:/          quarantine:/' '$APP'"

probe "M1 stop retrying the apply for an already-staged copy" \
  "perl -0pi -e 's/          if \\(decision\\.reason === \"already-staged\"\\) await autoApplyIfSafeRef\\.current\\?\\.\\(\\);\n//' '$APP'"

# ── shouldStage vetoes that had no test at all before this change ──────────────
probe "GATE delete the bad-version veto" \
  "perl -0pi -e 's/  if \\(!BOOK_VERSION_RE\\.test\\(bookVersion\\)\\) return \\{ stage: false, reason: \"bad-version\" \\};\n//' '$GATES'"

probe "GATE delete the stagger-not-started veto" \
  "perl -0pi -e 's/  if \\(firstSeenAt == null\\) return \\{ stage: false, reason: \"stagger-not-started\" \\};\n//' '$GATES'"

# ── The self-enforcing header ──────────────────────────────────────────────────
probe "DOC add a gate reason the header list does not name" \
  "perl -0pi -e 's/  if \\(!webReady\\) return \\{ stage: false, reason: \"not-web-ready\" \\};/  if (!webReady) return { stage: false, reason: \"not-web-ready\" };\n  if (ctx.undocumentedThing) return { stage: false, reason: \"brand-new-veto\" };/' '$GATES'"

probe "DOC gut the header amendment's authoritative list" \
  "perl -0pi -e 's/^\\/\\/   canApplyNow:.*\\n\\/\\/                 room-active.*\\n//m' '$GATES'"

echo "────────────────────────────────────────────────────────────────────────────"
if [ "$holes" -eq 0 ]; then
  echo "  $n/$n probes reddened the suite. No holes."
  exit 0
fi
echo "  $holes of $n probes left the suite GREEN — those regressions are UNPINNED."
exit 1
