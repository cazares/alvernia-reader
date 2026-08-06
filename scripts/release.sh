#!/usr/bin/env bash
#
# Lockstep release — ONE version, BOTH surfaces.
#
# The native app is a thin shell that WRAPS the web bundle, so web + native are really one artifact
# and MUST carry the same build number. This script is the only blessed way to cut a build: it bumps
# a single source (version.json), then builds the web bundle, the native IPA, AND deploys the web —
# all from that one number. The native overlay (`b<N>`), the WebBundle baked into the app, and
# signovivo.com's `v<N>` badge therefore always agree.
#
# DO NOT bump/build/deploy one surface by hand — that's exactly how the numbers drift.
#
# Usage:  bash scripts/release.sh            # bump + build web + build native IPA + deploy web (PROD)
#         SKIP_NATIVE=1 bash scripts/release.sh   # web-only refresh (still rebuilds+deploys web at the bumped version)
#         STAGING=1 bash scripts/release.sh   # CANARY: build web at the CURRENT version + deploy to the
#                                             #   isolated Pages preview branch 'staging'. NO version bump,
#                                             #   NO native archive, NEVER touches production ('main') or
#                                             #   TestFlight. Prove a build here (signovivo.com?env=staging)
#                                             #   before promoting to prod.
#
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8
LOG=/tmp/release-native.log
TF_UPLOADED=0

# STAGING is the canary path — physically incapable of touching prod: it implies
# no-native + no-bump and flips the Pages deploy to the preview branch. With STAGING
# unset, everything below is byte-for-byte the production flow.
STAGING="${STAGING:-0}"
if [ "$STAGING" = "1" ]; then
  SKIP_NATIVE=1
  DEPLOY_BRANCH="staging"
else
  DEPLOY_BRANCH="main"
fi

# PREFLIGHT: book consistency. This guard existed but ran ONLY from `preios` (local
# simulator runs) and never on the release path — so `bash scripts/release.sh` could
# publish a book whose song index points past the last rendered page, which is the
# build-325/327 "song N unreachable" class, straight to prod with nothing firing.
# It costs ~1s (pdfinfo + a regex) and it runs in EVERY mode, staging included:
# STAGING still builds and deploys a real bundle that a canary iPad will read.
# STRAY .xcarchive INSIDE ios/. React Native's post-install hook runs `find ios/ -name Info.plist`
# and skips only /Pods, Tests, metainternal, .bundle, build/ and DerivedData/. An .xcarchive sitting
# directly in ios/ matches none of them, so the hook walks into the packaged .app and hits BINARY
# plists (bplist00) — and Ruby 4.0.1 raises "invalid byte sequence in UTF-8" from deep inside
# xcodeproj, sixty lines of backtrace that name neither the archive nor ios/.
#
# Cost the owner two failed builds on 2026-08-06 chasing a locale that was never the problem. The
# check is one `ls`; the failure it prevents is unreadable.
#
# It does NOT move or delete anything: an .xcarchive carries the dSYMs that symbolicate crash
# reports from that build, and these are often the only copy.
STRAY_ARCHIVES=$(ls -d ios/*.xcarchive 2>/dev/null || true)
if [ -n "$STRAY_ARCHIVES" ]; then
  echo "" >&2
  echo "✖ There are .xcarchive bundles inside ios/. pod install WILL fail with a misleading" >&2
  echo "  \"invalid byte sequence in UTF-8\" from xcodeproj — it is reading their binary plists." >&2
  echo "" >&2
  echo "$STRAY_ARCHIVES" | sed 's/^/    /' >&2
  echo "" >&2
  echo "  MOVE them (do not delete — they hold the dSYMs for those builds):" >&2
  echo "    mkdir -p ~/SignoVivo-archives && mv ios/*.xcarchive ~/SignoVivo-archives/" >&2
  exit 1
fi

echo "==> 0/6  Preflight: book consistency (song index vs shipped PDF)"
if [ "${SKIP_GATES:-0}" = "1" ]; then
  echo "         skipped (SKIP_GATES=1)"
else
  node scripts/check-book-consistency.mjs
fi

if [ "$STAGING" = "1" ]; then
  echo "==> 1/6  STAGING/CANARY -> skip bump; build web at the CURRENT version (prod untouched)"
  BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
  echo "         build = $BUILD (unchanged)"
else
  echo "==> 1/6  Bump version.json (+ native CFBundleVersion / Info.plist / app.json / offlineWebBundle)"
  node scripts/bump-build.mjs
  BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
  echo "         build = $BUILD"

  # A BUILD NUMBER IS SPENT THE MOMENT IT IS UPLOADED. App Store Connect refuses a duplicate, so the
  # archive succeeds, ~10 minutes pass, Transporter rejects it, and you start over — twice in one
  # evening on 2026-08-06 (412, then 413).
  #
  # Nothing in this repo has ever recorded what was UPLOADED, only what was BUILT, so the check came
  # down to somebody remembering. The IPA on the Desktop is the best local evidence there is: this
  # script writes exactly one per build, and a build that exists is a number that was very likely
  # delivered. Treat its presence as "spent" and make the operator say otherwise.
  IPA_OUT="$HOME/Desktop/SignoVivo-${BUILD}.ipa"
  if [ -e "$IPA_OUT" ] && [ "${ALLOW_REUSED_BUILD:-0}" != "1" ]; then
    echo "" >&2
    echo "✖ Build $BUILD already has an IPA on your Desktop:" >&2
    echo "    $IPA_OUT   ($(date -r "$IPA_OUT" '+%b %e %H:%M'))" >&2
    echo "" >&2
    echo "  If that one was uploaded, App Store Connect will REFUSE this build after the archive" >&2
    echo "  finishes — about ten wasted minutes. Bump past it:" >&2
    echo "    node scripts/bump-build.mjs && bash scripts/release.sh" >&2
    echo "" >&2
    echo "  If it was never uploaded and you mean to replace it:" >&2
    echo "    ALLOW_REUSED_BUILD=1 bash scripts/release.sh" >&2
    exit 1
  fi
fi

echo "==> 2/6  Rebuild web bundle (bakes v$BUILD into the badge + a content-hashed cache version)"
node web/build.mjs >/dev/null

# PUBLISH-PATH GATES. These existed ONLY on the PR path (.github/workflows/ci.yml) — not on the
# path the artifact actually leaves the building on. `release.sh` could therefore publish a book
# whose pages had been silently re-rendered in place (the build-377 / PR #257 defect, which shipped)
# or whose song index points past the last rendered page. Both are unrecoverable once a device with
# no internet has them, so they must fail HERE, loudly, before anything is uploaded. `set -euo
# pipefail` plus the cleanup_release trap make an abort here crash-safe.
echo "==> 2b/6 Publish gates: boot smoke + additive-only + book consistency"
if [ "${SKIP_GATES:-0}" = "1" ]; then
  # SKIP_GATES=1 is what the OTA path sets. The owner's call, 2026-08-05: the PDF handed to
  # ota-publish.sh is the book he meant to publish, and a gate can only ever say no to a decision
  # already made. Left honoured by an env var rather than deleted, because the NATIVE path through
  # this script still runs them by default — an IPA is not undoable by republishing forward.
  echo "         skipped (SKIP_GATES=1)"
else
  SMOKE_SKIP_BUILD=1 node scripts/smoke-boot.mjs
  node scripts/additive-gate.mjs
  node scripts/check-book-consistency.mjs
fi

# The ios/WebBundle sync is gated on whether a native archive will ACTUALLY happen — not on
# STAGING. Under the old condition, `SKIP_NATIVE=1` (a web-only prod deploy) rewrote the WebBundle
# tree that the next hand-run Xcode build would bake, leaving an IPA carrying a bundle no archive
# in this pipeline ever produced. Staging still skips it, because STAGING implies SKIP_NATIVE=1.
if [ "${SKIP_NATIVE:-0}" = "1" ]; then
  echo "==> 3/6  No native archive this run -> leaving ios/WebBundle untouched"
else
  echo "==> 3/6  Sync web/dist -> ios/WebBundle (native wraps the SAME bundle)"
  rm -rf ios/WebBundle && cp -R web/dist ios/WebBundle

  # HARD ASSERTION (§5.14.1). ios/WebBundle is gitignored and materialized ONLY by the line above.
  # A silent copy miss ships an IPA with NO in-IPA bundle — which makes the L1 self-heal floor
  # nonexistent and hands the WebView a path to nothing, on a fleet with no remedy at Mass. There is
  # no runtime fix for that and there should not be one: it is a release-pipeline defect and the
  # pipeline is where it has to fail.
  node -e '
    const fs=require("fs"),path=require("path");
    const dir="ios/WebBundle", fail=(m)=>{console.error("  ✖ ios/WebBundle: "+m);process.exit(1)};
    if(!fs.existsSync(path.join(dir,"index.html"))) fail("index.html missing");
    if(fs.statSync(path.join(dir,"index.html")).size<=200) fail("index.html is <= 200 B");
    let m; try{ m=JSON.parse(fs.readFileSync(path.join(dir,"bundle-manifest.json"),"utf8")); }
    catch(e){ fail("bundle-manifest.json missing or unparseable ("+e.message+")"); }
    const pages=path.join(dir,"books/standard/pages");
    const have=fs.existsSync(pages)?fs.readdirSync(pages).filter(f=>/^page-\d+\.webp$/.test(f)):[];
    if(have.length!==m.totalPages) fail(`${have.length} page images but manifest says ${m.totalPages}`);
    for(const n of [1,m.totalPages]){
      const f=path.join(pages,`page-${String(n).padStart(m.pagePadWidth||3,"0")}.webp`);
      if(!fs.existsSync(f)||fs.statSync(f).size<=0) fail(`page ${n} missing or empty`);
    }
    const dist=fs.readdirSync("web/dist/books/standard/pages").filter(f=>/^page-\d+\.webp$/.test(f)).length;
    if(dist!==have.length) fail(`page count ${have.length} != web/dist ${dist}`);
    console.log(`         ✅ in-IPA bundle verified: ${m.bookVersion}, ${m.totalPages} pages`);
  '
fi

if [ "${SKIP_NATIVE:-0}" = "1" ]; then
  echo "==> 4/6  SKIP_NATIVE=1 -> skipping the native archive"
else
  echo "==> 4/6  Build native IPA (Release archive + export) — logging to $LOG"
  # Crash-safe cleanup. The archive overwrites the TRACKED ios/Podfile.lock; this restores it.
  # Idempotent and ALWAYS returns 0, so it can never abort the script under `set -e`. Armed via
  # `trap ... EXIT INT TERM` BEFORE any mutation, so a Ctrl-C or crash mid-archive still restores.
  # (It also used to restore director-codes.json, which no longer gets swapped — see below.)
  cleanup_release() {
    git checkout -- ios/Podfile.lock 2>/dev/null || true
    return 0
  }
  trap cleanup_release EXIT INT TERM

  cp ios/Pods/Manifest.lock ios/Podfile.lock         # pod-guard workaround (Ruby 4.0.1 pod install is broken)
  # NOTHING IS BAKED IN HERE ANY MORE. This used to swap a gitignored file of real director phone
  # numbers over a tracked empty one, archive, and restore — and an archive made without that file
  # produced an IPA nobody could direct (the 2026-07-01 Mass outage). The director code is now a
  # plain constant in PdfReaderApp.tsx, so any checkout can archive and there is nothing to forget.
  rm -rf build
  if ! xcodebuild -workspace ios/SignoVivo.xcworkspace -scheme SignoVivo -configuration Release \
        -archivePath build/SignoVivo.xcarchive -sdk iphoneos -allowProvisioningUpdates clean archive >"$LOG" 2>&1; then
    echo "ARCHIVE FAILED — tail of $LOG:"; tail -25 "$LOG"; exit 1   # trap restores codes + Podfile.lock
  fi
  if ! xcodebuild -exportArchive -archivePath build/SignoVivo.xcarchive -exportPath build/export \
        -exportOptionsPlist ios/exportOptions.app-store.plist -allowProvisioningUpdates >>"$LOG" 2>&1; then
    echo "EXPORT FAILED — tail of $LOG:"; tail -25 "$LOG"; exit 1     # trap restores codes + Podfile.lock
  fi
  cleanup_release   # restore ASAP to minimize the PII window; the EXIT trap is the crash-safety net
  cp build/export/SignoVivo.ipa "$HOME/Desktop/SignoVivo-$BUILD.ipa"
  echo "         IPA -> ~/Desktop/SignoVivo-$BUILD.ipa"

  # Hands-off TestFlight upload when App Store Connect API creds are configured.
  # scripts/asc-credentials.env is gitignored (copy scripts/asc-credentials.env.example);
  # the .p8 stays outside the repo. Without creds, fall back to the manual Transporter step.
  [ -f scripts/asc-credentials.env ] && . scripts/asc-credentials.env
  if [ -n "${ASC_KEY_ID:-}" ] && [ -n "${ASC_ISSUER_ID:-}" ]; then
    echo "         Uploading build $BUILD to TestFlight via App Store Connect API..."
    ALT=(xcrun altool --upload-app --type ios --file build/export/SignoVivo.ipa
         --apiKey "$ASC_KEY_ID" --apiIssuer "$ASC_ISSUER_ID")
    [ -n "${ASC_P8_PATH:-}" ] && ALT+=(--p8-file-path "$ASC_P8_PATH")
    if "${ALT[@]}"; then
      TF_UPLOADED=1
      echo "         ✅ Uploaded to TestFlight (build $BUILD). Add it to the choir group in App Store Connect when ready."
    else
      echo "         ⚠️  altool upload failed — fall back: open -a Transporter ~/Desktop/SignoVivo-$BUILD.ipa  then click DELIVER"
    fi
  fi
fi

# PRE-DEPLOY COMPLETENESS GATE (added after the 2026-08-03 outage).
#
# web/dist is built at step 2 and verified only INDIRECTLY, via the ios/WebBundle assertion at
# step 3 — which is skipped entirely when SKIP_NATIVE=1, and which runs BEFORE the ~10-minute
# archive. On 2026-08-03 web/dist was emptied somewhere between step 3 and here, and this line
# happily deployed 24 files instead of 389: signovivo.com served the shell with EVERY page image
# 404, and nothing in the pipeline noticed. wrangler exits 0 — publishing an empty directory is a
# perfectly successful deploy as far as it is concerned.
#
# The tell was the upload count and only the upload count, which no human reads. So assert it here,
# immediately before the bytes leave, against the manifest's own totalPages. This is the last
# moment anything can be checked.
echo "==> 4b/6 Pre-deploy gate: web/dist is a COMPLETE book"
node -e '
  const fs=require("fs"),path=require("path");
  const dir="web/dist", fail=(m)=>{console.error("  ✖ web/dist: "+m);process.exit(1)};
  if(!fs.existsSync(path.join(dir,"index.html"))) fail("index.html missing");
  if(!fs.existsSync(path.join(dir,"app.js"))) fail("app.js missing");
  if(!fs.existsSync(path.join(dir,"sw.js"))) fail("sw.js missing");
  let m; try{ m=JSON.parse(fs.readFileSync(path.join(dir,"bundle-manifest.json"),"utf8")); }
  catch(e){ fail("bundle-manifest.json missing or unparseable ("+e.message+")"); }
  const pagesDir=path.join(dir,"books/standard/pages");
  if(!fs.existsSync(pagesDir)) fail("books/standard/pages/ missing entirely");
  const have=fs.readdirSync(pagesDir).filter(f=>/^page-\d+\.webp$/.test(f));
  if(have.length!==m.totalPages) fail(`${have.length} page images but the manifest says ${m.totalPages}`);
  for(const n of [1,m.totalPages]){
    const f=path.join(pagesDir,`page-${String(n).padStart(m.pagePadWidth||3,"0")}.webp`);
    if(!fs.existsSync(f)||fs.statSync(f).size<=0) fail(`page ${n} missing or empty`);
  }
  // Every entry the OTA will later demand byte-exact must exist on disk NOW.
  const missing=m.files.map(f=>f.p).filter(p=>!fs.existsSync(path.join(dir,p)));
  if(missing.length) fail(`${missing.length} manifest file(s) absent from disk, e.g. ${missing.slice(0,3).join(", ")}`);
  console.log(`         ✅ ${m.bookVersion}, ${m.totalPages} pages, ${m.files.length} files — complete`);
'

if [ "$STAGING" = "1" ]; then
  echo "==> 5/6  Deploy web to the ISOLATED Pages preview branch '$DEPLOY_BRANCH' (NOT signovivo.com)"
else
  echo "==> 5/6  Deploy web to signovivo.com (production = Pages branch '$DEPLOY_BRANCH')"
fi
npx wrangler pages deploy web/dist --project-name alvernia-reader --branch "$DEPLOY_BRANCH" --commit-dirty=true

# REFRESH THE ADDITIVE BASELINE — production deploys only.
#
# additive-gate.mjs compares the next book against `git show HEAD:web/manifest-baseline.json`, and
# NOTHING updated that file: not this script, not CI. On 2026-08-05 it was found two releases stale
# (372 pages while prod served 374), so the gate was silently comparing against a book nobody was
# running — which is exactly how an in-place page edit (the #257 defect it exists to catch) slips
# through unnoticed.
#
# Written AFTER the deploy succeeds, so the baseline always means "what production is actually
# serving". It is a tracked file: commit it with the release, or the next run compares against a
# stale HEAD again. Staging never touches it — prod is the only thing devices download.
if [ "$STAGING" != "1" ]; then
  cp web/dist/bundle-manifest.json web/manifest-baseline.json
  echo "         additive baseline refreshed -> $(node -e "process.stdout.write(require('./web/manifest-baseline.json').bookVersion)") (COMMIT THIS with the release)"
fi

if [ "$STAGING" = "1" ]; then
  echo "==> 6/6  DONE — staging preview deployed. Prove it on the canary iPad (open the preview URL"
  echo "         printed above, or signovivo.com?env=staging once promoted), THEN promote to prod:"
  echo "           bash scripts/release.sh          # full prod (bump + native + deploy main)"
  echo "         Production (branch 'main' / signovivo.com) and TestFlight were NOT touched."
else
  echo "==> 6/6  DONE — signovivo.com == native == v$BUILD"
  if [ "$TF_UPLOADED" = "1" ]; then
    echo "         TestFlight: build $BUILD uploaded automatically. Add it to the choir group when ready."
  elif [ "${SKIP_NATIVE:-0}" != "1" ]; then
    echo "         Final manual step (native to TestFlight):"
    echo "           open -a Transporter ~/Desktop/SignoVivo-$BUILD.ipa   then click DELIVER"
    echo "         (Configure scripts/asc-credentials.env to make this automatic — see .env.example.)"
  fi
fi
