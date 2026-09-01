# SignoVivo (alvernia-reader) — rules for any Claude session on this repo

SignoVivo is the iPad songbook the choir uses **during Mass**. One iPad is the
DIRECTOR and turns pages; the others FOLLOW. A wrong page in front of the
congregation is the worst outcome this codebase can produce. Prefer the
conservative fix.

## This file holds INVARIANTS ONLY — never state

**Do not put build numbers, fleet status, "what's in flight", or session progress
in this file, or in any committed doc presented as orientation.** That is exactly
what went wrong before: a `HANDOFF.md` lived at the repo root, said "read it
before touching anything," and went stale three separate times — once by 5 weeks
and 41 builds (#284), once with three claims outright wrong (3f9b842), and finally
sat from 2026-08-18 to 2026-09-01 describing a build and an unmerged branch that
had long been superseded. It was an OPEN finding in `docs/ia-audit-2026-07` the
entire time and still misled the next tab.

Session handoffs are **never committed**. `HANDOFF*.md` is gitignored and pinned by
`e2e/noStaleHandoff.test.mjs`. Write a handoff outside the repo and hand over its
absolute path. For current state, read git: `git log`, the PR list, `version.json`.

## Landmines — each of these has already cost real time

- **Never run `npm run test:e2e`.** Its glob includes `e2e/relay-sync.test.mjs`,
  which publishes to the **production** relay room and flips live followers' pages.
  Run the safe set instead:
  ```
  node --test $(ls e2e/*.test.mjs | grep -v 'relay-sync.test.mjs' | tr '\n' ' ')
  ```
- **Never run iOS simulators during a live device test.** A simulator joins the real
  Wi-Fi mesh, and a sim director (newest token) hijacks every physical follower into
  a reconnect loop. This produced three "slow sync the night before Mass" incidents.
- **Merging to `main` does not deploy.** Only `scripts/release.sh` publishes to
  signovivo.com (`wrangler pages deploy web/dist`). Never assume a merge shipped.
- **`pages-build-deployment` fails on every commit ON PURPOSE.** Do not "fix" it —
  read `docs/pages-auto-deploy-decision.md` first. Unbreaking it could publish repo
  source to the congregation's live site.
- **Run the full safe suite BEFORE committing, not after.** A red test has been
  pushed here by trusting a partial run.
- **Tests here go decorative easily.** A measured 19 of 25 source-text assertions
  stayed green under a real regression. Marker- and character-bounded slices are the
  #1 cause. Read `docs/test-meaningfulness-2026-08.md` before writing a test, and
  mutation-check any assertion you add.

## Where the durable context actually lives

`docs/app-atlas.md` (what the app is), `docs/app-contracts.md` (the wire and DOM
contracts), `docs/test-meaningfulness-2026-08.md` (how to write a test that can
fail), `docs/pages-auto-deploy-decision.md` (why CI is red on purpose).
