# HANDOFF — SignoVivo / alvernia-reader

*Rewritten 2026-08-05 (~11:30 PM CT). Supersedes the earlier 2026-08-05 handoff, which described a
stamping stage, three publish gates and a hand-maintained song index — all deleted that night, and
all of which were actively breaking the thing they were meant to protect.*

---

## 0. TL;DR

| | State |
|---|---|
| **Repo HEAD** | `main` @ `201c753` — everything merged, no open PRs |
| **Web prod** | signovivo.com **v411** · `bv_29032a4e612d83f6` · **373 pages** · minShellBuild 400 |
| **Native** | **404** — owner's iPhone only. **The choir is on ≤398 and cannot receive OTAs.** |
| **The OTA works** | Proven both directions on a real device, and proven with an arbitrary PDF |

**The one command:** `scripts/ota-publish.sh <any.pdf>`

---

## 1. How to ship a songbook

```bash
scripts/ota-publish.sh ~/Downloads/nuevo.pdf
```

Two stages: install the PDF, publish it. It renders, deploys to signovivo.com, commits the release
record **and** the additive baseline, and arms the fleet. Then a person opens the app on a device.

```bash
scripts/ota-publish.sh <pdf> --devices k3m9x2   # one device instead of the fleet
scripts/ota-publish.sh <pdf> --dry-run          # render only; nothing leaves the Mac
scripts/ota-publish.sh                          # no pdf = republish the book already in the repo
scripts/ota-rollback.sh --list                  # what can I go back to?
scripts/ota-rollback.sh                         # go back one book
scripts/ota-arm.sh --disarm                     # stop NEW devices (already-updated keep it)
```

**On a device:** it updates when you open the app; **⟳** forces it. No code to type.

### ANY PDF WORKS, AND NOTHING CHECKS IT

This is a deliberate 2026-08-05 decision, not an oversight. Do not "restore" any of it as a bug fix.

- **Song number IS page number**, derived from the PDF at build time (`web/build.mjs`). There is no
  index to maintain. The old `src/alverniaManual2SongIndex.js` was a hand-edited `[song, page]` list
  that had to be updated for every new book — and it was already an identity map, 316 of 317 entries.
  The file still exists because `src/offlineBooks.ts` and three e2e tests import it; **nothing in the
  publish path reads it.**
- **No gates.** Boot smoke, book consistency and the additive gate no longer run on the OTA path.
  `release.sh` honours `SKIP_GATES=1`, which `ota-deploy.sh` sets. The **native** path still gates by
  default — an IPA cannot be undone by republishing forward.
- **No stamping.** Nothing grafts, deletes or draws on a page.

**Two consequences, owned:** a shrink can strand pages on a device that was offline when it landed,
and a book that fails to render now reaches devices because nothing opens the bundle first. Both are
recoverable by republishing forward.

---

## 2. ⚠️ THE CHOIR STILL CANNOT RECEIVE ANY OF THIS

`MIN_SHELL_BUILD = 400` (`web/build.mjs:891`) and the parish iPads are on **398 or older**. The gate
is correct — pre-400 shells have three *measured* defects that turn an OTA into a ~26 MB retry loop:

- `≤394` no manifest in the staged bundle → evicted → permanent loop
- `≤397` WebView denied `styles.css`/`app.js` → renders as raw HTML → quarantined
- `≤399` `verifyStaged` refuses EVERY shrink → fails, re-stages → loop

**Lowering it does not deliver to those devices; it burns their data forever.** The fix is
distribution: get every iPad onto **404+** via TestFlight. After that, everything is OTA forever.

**This is the only remaining work that matters.** It needs Miguel's hands: TestFlight and ~8
physical iPads.

---

## 3. ⚠️ ROLLBACK IS REPUBLISH-FORWARD. There is no other kind.

**Never** roll back with `--base <immutable Pages deployment URL>`:

- `src/bookUpdate.js` `ALLOWED_HOSTS` matches by **exact string equality**, so
  `<hash>.alvernia-reader.pages.dev` and `staging.alvernia-reader.pages.dev` are refused
- a refused pointer is **not ignored** — it is the **REVOKE** path, which **deletes the staged
  bundle**. You would destroy ~27 MB on every device while every gate on the Mac read green

`ota-arm.sh` blocks that base outright (exit 2). **A `bookVersion → deployment URL` ledger is a
WRITE-ONLY FILE. Do not build it.** Git is the ledger; `ota-rollback.sh --list` reads real history.

Making `--base` real needs `ALLOWED_HOSTS` to accept subdomains (a native constant → TestFlight)
**and** a decision about `decideBundle` rule 7 refusing an older `builtFromShellBuild`. Deliberately
not done: republish-forward works and was used repeatedly on 2026-08-05.

---

## 4. Becoming director — ONE code, in plain source

```
DIRECTOR_CODE = "333444555"          PdfReaderApp.tsx:110
```

### The pill — what the room is doing, and the control for it

Top-left in the native shell, always visible, three states. Tapping acts on whatever it shows.

| Pill | Meaning | Tap |
|---|---|---|
| `DIRECTOR · ✕ Salir` | you are directing | confirm → step down |
| `SIGUIENDO · Dirigir el coro` (quiet, dimmed) | someone else is | confirm → you direct (red takeover warning if they are live) |
| `NADIE DIRIGE · ▶ Dirigir el coro` (amber) | **the seat is empty** | confirm → you direct |

⚠️ **The label names the OUTCOME, never the dialog.** An earlier version read `Tomar el control`, to
match the takeover button. That is wrong roughly half the time it is read: which confirm appears is
decided natively by `liveDirector`, an **8s** window on `lastDirectorSnapshotRef`, while the pill's
`SIGUIENDO` uses a **15s** window on mesh page events — deliberately longer so a radio hiccup does
not flash `NADIE DIRIGE` at the choir mid-hymn. Between 8s and 15s the dialog says `¿Dirigir el
coro?`. Both paths end in the same place, so the label says that and nothing more.

`NADIE DIRIGE` fires ONLY on the mesh's own `self-directed` verdict, never inferred from silence —
guessing would light it during the ~10s of boot discovery on every device every Sunday.

Before this, the badge appeared only for the director, so an empty seat looked exactly like
everything working: follower controls, no badge, and the ⟳ spinner going quiet. Eight iPads could
sit through a whole Mass each turning its own pages with nothing on any screen saying so.

### ONE director, only a human makes one (2026-08-15 — supersedes the two blocks it replaced)

**No device ever becomes director on its own.** Not from a persisted role, not from a crash a
minute ago, not from a tally of how often it has directed. Miguel, the night before Mass, after
watching physical devices split between two directors: *"only ever allow one and only one director."*

Both automatic paths that lived here between 2026-08-05 and build 427 — the "habitual" empty-seat
claim and the crash-resume — decided the seat was empty from "no director page heard in 8s". A
director the radio has DISCOVERED but not yet CONNECTED to sends no pages, and discovery alone can
take 5-30s, so the automatic claim fired beside a human's, carried the NEWER token, and the mesh
(newest wins) demoted the human. Removed in build 428, PR #346. `e2e/singleDirector.test.mjs` pins
that `becomeDirector` has exactly one call site (the confirm in `onDirectorCode`) and that the boot
path only ever calls `becomeFollower`; `scripts/verify-director-rescue-guards.mjs` proves it (a
mutation that puts an automatic claim back is caught).

What a person sees: a device that was directing when its app died comes back as a FOLLOWER with one
toast — *"Estabas dirigiendo. Toca el estado arriba a la izquierda para volver a dirigir."* Two
humans who both take the role converge on the NEWER one within a discovery cycle
(`handleDirectorConflict`, unchanged), and the loser now gets a toast saying so.

`STORAGE_KEYS.directorSessions` is still counted (diagnostic) but decides nothing.

**Two ways in, one path through.** Tap the **pill** (above), or type `DIRECTOR_CODE` on the numpad.
Both land in `onDirectorCode` → **always** a confirm dialog, never a silent promotion →
`becomeDirector()`. To change the code, edit that one line.

There is exactly ONE place that asks native for the role, and a test asserts it stays that way. The
old "Dirigir el coro" button inside the ♪ modal was **removed** 2026-08-06: a director control in a
dialog titled *IR A CANTO* was wrong twice over — nobody looking to direct opens the go-to-song
keypad, and on a 10.2" portrait iPad it sat below the card's fold.

The pill renders only inside the native shell (no mesh on signovivo.com). The web bundle never
learns the code — it asks. **The native `request-director` handler must be in the binary**; the pill
itself ships over the air.

**A director who crashes mid-Mass does NOT resume automatically** (build 428) — it comes back as a
follower and is told to tap the pill. See *ONE director* above.

**There is no codes file, no baking step, and nothing to forget.** Until 2026-08-05 this was a set
of real director phone numbers that `release.sh` swapped out of a gitignored
`director-codes.private.json` at archive time. That machinery bought nothing — taking the role
already requires physically holding a parish iPad, and the confirm dialog, not the secrecy of the
number, is what prevents an accidental takeover. What it cost was real:

- an archive made without that file produced an IPA that installed perfectly and **rejected every
  code** — the 2026-07-01 Mass outage
- it kept four people's phone numbers one `git add` away from a public repo
- only the main checkout had the file, so no worktree could archive

Removed at the owner's call: *"super overkill for what this app is and is meant to become (stay the
same)."* Nine iPads, one permanent director, no MDM. **Any checkout can now archive.**

`333444555` sits at Hamming distance 8+ from every other numpad code (soft reset `744668486`, book
apply `265134902`, force baked `907315268`) — read off a card in poor light, a single misread must
not wipe the device's role instead of granting it. Keep that property if you ever change it.

⚠️ `director-codes.private.json` may still exist in the main checkout. It is unused, still
gitignored, and contains real phone numbers — Miguel's to delete or keep, nobody else's.

---

## 4b. When something goes wrong on a device

**♪ modal → "¿Algo anda mal?"** — collapsed by default, native-shell only, each action confirming
natively before it does anything:

| | |
|---|---|
| Usar el cancionero original | forces the baked book (was `907315268`) |
| Reparar la sincronización | soft reset (was `744668486`) |
| Ver diagnóstico | the crumb log |

Both codes still work. They became buttons because they exist for the five minutes before Mass when
something has already broken — the exact moment nobody can look up a nine-digit number.

**Diagnóstico** shows the build, the page count, **which songbook this device holds**, and the last
200 breadcrumbs across restarts (`── app start ──` marks each boot; the line before it is usually
the last thing that worked). Selectable, so it can be read aloud or pasted into a message.

That book line matters: with the title-page stamp deleted (§4), this is now the only way to answer
"¿qué cancionero tiene?" with no internet — and unlike ink on page 1, it comes from the device's own
resolved state and cannot disagree with the book.

⚠️ `breadcrumb()` persistence has an ordering rule: crumbs buffer in memory until the previous
session is read back, because the first crumb after a crash lands milliseconds into boot and would
otherwise overwrite the history with a one-element array. `e2e/rescueAndDiagnostics.test.mjs` pins it.

---

## 5. Delivery has TWO conditions left, and they are physics

```
not-ready         nothing finished downloading
bridge-not-ready  no WebView to swap under (else: permanent blank screen)
```

Removed 2026-08-05: `stale-ready`, `no-live-internet`, `mesh-peer`, `recent-page-turn`,
`director-active`, cold-boot cooldown, the role veto, the client stagger. `shell-too-old` **moved**
to `shouldStage` — refuse the *download*, not the apply.

**Every one of them refused SILENTLY**, which is why a working rollout and a dead one were
indistinguishable for an afternoon. Six tests assert each gate is *gone*. **Reverting any of it needs
a new owner decision, not a "regression fix."**

Accepted: an update can land mid-Mass (screens blink as the WebView remounts), and a copy staged
days ago applies whenever next foregrounded.

---

## 6. Traps that cost real time

**The pre-arm gate must retry on the VERSION, not the bytes.** `verify-ota-fetchability` fetches the
manifest *from the base* and checks files against **that** manifest — so while Cloudflare's alias
still serves the previous deployment, it verifies the old book against itself and reports a clean
pass. On 2026-08-05 that printed `✅ all 389 checked files are byte-exact … Safe to arm
bv_29146774dc1035b9` then `✖ ABORT`, with every retry unused. Fixed: the version check is now inside
the loop, 20 attempts over ~5 min. **If an arm aborts, wait and re-run the arm — do not redeploy.**

**A failed publish leaves the wrong book committed.** `ota-publish.sh` copies the PDF over
`assets/songbook.pdf` in stage 1, and `ota-deploy.sh` commits it in the release record. So after a
bad publish, `git checkout assets/songbook.pdf` restores **the bad book** — it is what HEAD now holds.
Use `git checkout origin/main -- assets/songbook.pdf`.

**Merging ≠ deployed.** Prod moves only when `release.sh` / `ota-publish.sh` runs.

**Never run two `release.sh` at once** — they share `web/dist`, `/tmp/release-native.log` and
`~/Desktop/SignoVivo-<N>.ipa`.

**Never run `npm run test:e2e`** — `e2e/relay-sync.test.mjs` publishes to the PRODUCTION relay room.
Run the named files in `.github/workflows/ci.yml`.

**A fresh worktree needs `npm ci`** before anything renders.

---

## 7. Orientation for a cold tab

A Catholic parish choir songbook used LIVE during Mass. ~8 personally-owned iPads (no MDM), one
permanent director: **Braulio Figueroa**. **THERE IS NO INTERNET INSIDE THE CHURCH** — page sync at
Mass is a Multipeer mesh; the Cloudflare relay does not work there. **Stale beats blank.**

One web bundle (`web/src/`) deploys to signovivo.com (Cloudflare Pages, prod branch `main`) *and* is
copied into `ios/WebBundle` inside the IPA. Native is a thin RN shell whose WKWebView loads that
bundle from `file://`.

- The book is `assets/songbook.pdf` — stable name; it deliberately does not encode a page count.
- `director-codes.private.json` lives ONLY in the main checkout and is gitignored (see §4).
- The shared checkout at `/Users/cazares/src/alvernia-reader` is usually on `main` and other tabs
  read from it. Work in a fresh worktree; a guard hard-denies writes outside it.

**Scripts:** `ota-publish.sh` (the one command) · `ota-rollback.sh` · `ota-arm.sh` · `ota-deploy.sh`
· `release.sh` · `verify-ota-fetchability.mjs`

---

## 8. What is NOT done

1. **Get the choir onto build 404+** (§2). Nothing else matters until this happens. The next IPA
   already carries the download cache-buster, so it should hold for months.
2. **No visible failure state for the OTA specifically.** If an update does not land, nothing says
   why. The crumb log (§4b) now records it, but only if someone goes looking. A status line + Retry
   would surface it. Lives in `web/src`, so it ships over the air; no binary needed.
3. **`--base` rollback + `decideBundle` rule 7** (§3) — a decision, not a bug fix.
4. **Nothing.** Director codes were the last open decision and are gone (§4).
