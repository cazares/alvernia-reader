# HANDOFF — SignoVivo / alvernia-reader

*Rewritten 2026-08-05 (~6:00 PM CT). Supersedes the 2026-08-03 handoff, which predates the OTA
being proven and still describes `assets/signo_vivo_372.pdf`, a numpad apply code, and a rollback
mechanism that does not work.*

---

## 0. TL;DR

| | State |
|---|---|
| **Repo HEAD** | `main` @ `dfb5f2b` — everything merged, no open PRs |
| **Web prod** | signovivo.com **v406** · `bv_29146774dc1035b9` · **373 pages** · minShellBuild 400 |
| **OTA** | **ARMED** fleet-wide at `bv_29146774dc1035b9` |
| **Native** | **404** — on the owner's iPhone only. **The choir is on ≤398 and cannot receive OTAs.** |
| **THE OTA WORKS** | Proven BOTH directions on a real device, 2026-08-05 |

**The one command you need:** `scripts/ota-publish.sh <new.pdf>`

---

## 1. THE OTA IS PROVEN — both directions

On the owner's iPhone, 2026-08-05, with the binary never moving:

| | Badge | Book |
|---|---|---|
| installed 404 | `404b · 404w · 373p` | baked |
| **forward** | `404b · 405w · 374p` | 373 → 374 songs, over the air |
| **rollback** | `404b · 406w · 373p` | 374 → 373 songs, over the air |

`b` = native binary · `w` = web bundle build · `p` = pages. **`b` never changing is the proof.**

---

## 2. How to ship a new songbook

```bash
scripts/ota-publish.sh ~/Downloads/braulio-nuevo.pdf
```

That is the entire procedure. It installs the PDF, extends the song index, re-stamps the title
page, renders, runs every gate, deploys to signovivo.com, commits the release record AND the
additive baseline, and arms the fleet. Then a person opens the app on a device.

```bash
scripts/ota-publish.sh <pdf> --devices k3m9x2   # prove on ONE iPad first
scripts/ota-publish.sh <pdf> --dry-run          # build + gates, nothing leaves the Mac
scripts/ota-rollback.sh --list                  # what can I go back to?
scripts/ota-rollback.sh                         # go back one book
scripts/ota-arm.sh --disarm                     # stop NEW devices (already-updated keep it)
```

**On a device:** it updates when you open the app, and the **⟳** button forces it. No code to type,
no dialog. (The numpad code `265134902` still exists as a manual force but nothing routine needs it.)

---

## 3. ⚠️ THE CHOIR CANNOT RECEIVE ANY OF THIS YET

`MIN_SHELL_BUILD = 400` (`web/build.mjs`) and the parish iPads are on **398 or older**. The gate is
correct — pre-400 shells have three *measured* defects that turn an OTA into a ~26 MB retry loop:

- `≤394` no manifest in the staged bundle → evicted → permanent loop
- `≤397` WebView denied `styles.css`/`app.js` → renders as raw HTML → quarantined
- `≤399` `verifyStaged` refuses EVERY shrink → fails, re-stages → loop

**Lowering it does not deliver to those devices; it burns their data forever.** The fix is
distribution: get every iPad onto **404+** via TestFlight. `~/Desktop/SignoVivo-404.ipa` exists and
is verified (codes 4/4, manifest embedded, 373 pages). Until then the OTA reaches one iPhone.

---

## 4. ⚠️ ROLLBACK IS REPUBLISH-FORWARD. There is no other kind.

**Never** roll back with `--base <immutable Pages deployment URL>`, despite `ota-arm.sh`'s own
header historically claiming that is what makes rollback possible. The shipped client never
implemented it:

- `src/bookUpdate.js` `ALLOWED_HOSTS` matches by **exact string equality**, so
  `<hash>.alvernia-reader.pages.dev` and `staging.alvernia-reader.pages.dev` are refused
- a refused pointer is **not ignored** — it is the **REVOKE** path, which **deletes the staged
  bundle**. You would destroy ~27 MB on every device while every gate on the Mac read green

`ota-arm.sh` now blocks that base outright (exit 2), and `e2e/bookUpdate.test.mjs` pins the shell
guard's host list to `ALLOWED_HOSTS` so they cannot drift.

**A `bookVersion → deployment URL` ledger is therefore a WRITE-ONLY FILE. Do not build it.** Git is
already the ledger — `ota-rollback.sh --list` reads real history. Making `--base` real needs
`ALLOWED_HOSTS` to accept subdomains (native constant → TestFlight) **and** a decision about
`decideBundle` rule 7 refusing an older `builtFromShellBuild`.

---

## 5. Delivery has TWO conditions left, and they are physics

`canApplyNow` went from 7 conditions to 2 on 2026-08-05:

```
not-ready         nothing finished downloading
bridge-not-ready  no WebView to swap under (else: permanent blank screen)
```

Removed: `stale-ready` (12 h), `no-live-internet` (5 min check-in proof), `mesh-peer`,
`recent-page-turn`, `director-active`, cold-boot cooldown, the role veto, the client stagger.
`shell-too-old` **moved** to `shouldStage` — refuse the *download*, not the apply.

**Every one of them refused SILENTLY.** That is why a working rollout and a dead one were
indistinguishable for an afternoon. Six tests now assert each gate is *gone*; they go red if one
creeps back. **Reverting any of it needs a new owner decision, not a "regression fix."**

Accepted consequences, owned: an update can land mid-Mass (the WebView remounts, so screens blink),
and a copy staged days ago applies whenever next foregrounded.

---

## 6. Traps that cost real time today

**The title-page stamp must not lie.** It is the only way to tell which book a device holds with no
internet ("¿la suya dice agosto?"). It read `· 372 páginas` on a 371-page book for a full day.
`ota-publish.sh` refuses to publish if the count disagrees; `ota-restamp.sh` fixes it.
`stamp-book-date.mjs` **overlays** — running it on a stamped book smudges page 1 and every gate
still passes, so it refuses and page 1 must be rebuilt from an unstamped source first.

**The pre-arm gate reds on the FIRST try after every deploy.** Always `index.html`, same size,
different md5 — it carries `bookVersion` inline and the Cloudflare alias lags the deployment.
`ota-arm.sh` now retries 6×/15s. Do not "just re-run" a red without reading it.

**Commit `version.json` AND `web/manifest-baseline.json` WITH each release.** `release.sh` prints
"COMMIT THIS" and moves on. Forgetting either makes the next build red against a stale reference and
drifts the repo behind prod. `ota-publish.sh` does it for you.

**The badge briefly shows the BAKED build during an apply** (WebView remount). It looks exactly like
a revert. It is not.

**`page-001 CHANGED IN PLACE` fires on EVERY book update** — the stamp carries the page count.
`ota-publish.sh` overrides that one and *only* that one; a shrink, disappeared page or moved song
aborts.

**Never run two `release.sh` at once** — they share `web/dist`, `/tmp/release-native.log` and
`~/Desktop/SignoVivo-<N>.ipa`. Doing so cost a build number today (`cwebp failed for render-281.png`).

**Never run `npm run test:e2e`** — `e2e/relay-sync.test.mjs` publishes to the PRODUCTION relay room.
Run the 13 named files in `.github/workflows/ci.yml` (216 tests with a built `web/dist`).

---

## 7. Orientation for a cold tab

A Catholic parish choir songbook used LIVE during Mass. ~8 personally-owned iPads (no MDM), one
permanent director: **Braulio Figueroa**. **THERE IS NO INTERNET INSIDE THE CHURCH** — page sync at
Mass is a Multipeer mesh; the Cloudflare relay does not work there. **Stale beats blank.**

One web bundle (`web/src/`) deploys to signovivo.com (Cloudflare Pages, prod branch `main`) *and* is
copied into `ios/WebBundle` inside the IPA. Native is a thin RN shell whose WKWebView loads that
bundle from `file://`.

- The book is `assets/songbook.pdf` — **stable name**, renamed 2026-08-05 from
  `signo_vivo_<pageCount>.pdf` because that name encoded a mutable fact and was reliably stale.
- `src/alverniaManual2SongIndex.js` is the hand-maintained `[song, page]` list. A page is not a song.
- `director-codes.private.json` lives ONLY in the main checkout, is gitignored, and is needed for
  any **native archive** — `release.sh` warns but proceeds without it, producing a build where
  nobody can become director (the build-371 outage). Not needed for web-only OTA work.
- Merging ≠ deployed. Prod moves only when `release.sh` (or `ota-publish.sh`) runs.

**Scripts:** `ota-publish.sh` (the one command) · `ota-rollback.sh` · `ota-restamp.sh` ·
`ota-arm.sh` · `release.sh` · `verify-ota-fetchability.mjs`

---

## 8. What is NOT done

1. **Get the choir onto build 404+** — nothing else matters until this happens (§3).
2. **No visible failure state on-device.** If an update does not land, nothing on screen says why.
   The proposal is a status line + Retry: turn each refusal reason into text instead of silence.
   This is the single highest-value remaining change.
3. **`src/bookUpdate.js` downloads files with no cache-buster** while the manifest fetch uses
   `?v=`. Cloudflare holds assets at `s-maxage=604800`. Not observed biting, but it is the same
   class as the alias lag. Needs a native build; fold into the next one.
4. **`--base` rollback + `decideBundle` rule 7** (§4) — a real decision, not a bug fix.
