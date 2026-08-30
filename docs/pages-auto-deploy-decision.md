# Why the Cloudflare Pages auto-deploy is left broken

**Decision recorded 2026-08-29.** Short version: **do not "fix" the
`pages-build-deployment` checkout failure without disconnecting the git
integration first.** Unbreaking it silently turns every merge to `main` into a
production publish of signovivo.com, using a build nobody in this repo controls.

## The situation

Two workflows run on `main`:

| workflow | what it does |
|---|---|
| `ci` | typecheck, the safe test list, three mutation harnesses, the additive gate, the boot smoke. **No deploy step.** |
| `pages-build-deployment` | Cloudflare Pages' **git integration**. Fires on every push to `main`. |

`pages-build-deployment` has failed at **Checkout** on every commit since at
least 2026-08-19:

```
fatal: No url found for submodule path '.claude/worktrees/determined-carson-439745' in .gitmodules
```

A Claude worktree was committed as a **gitlink** (mode `160000`) around the
build-265 era. Git treats it as a submodule with no `.gitmodules` record and
aborts the checkout, so the Pages build never starts.

## Why that is currently load-bearing

Production Pages is published **by hand**, from `scripts/release.sh`:

```
npx wrangler pages deploy web/dist --project-name alvernia-reader --branch "$DEPLOY_BRANCH" --commit-dirty=true
```

Note **`web/dist`** — a *built* directory (~389 files: every page rendered to
WebP via `pdftoppm`/`cwebp`, the manifest, the inlined page count). It does not
exist until `node web/build.mjs` runs.

There is **no Pages build configuration in this repository** — no
`wrangler.toml` at the root, no `functions/`, no `_worker.js`. So a
git-triggered Pages build would use whatever is configured in the Cloudflare
dashboard. If that is "no build command, deploy the root directory" — the
default — then unbreaking checkout publishes the **repository source** to
signovivo.com instead of the built site.

## The decision

1. **`.claude/worktrees/` is now in `.gitignore`** so another worktree cannot be
   committed this way again. Safe, and has no effect on deployment.
2. **The existing gitlink is deliberately left tracked.**
   `git rm --cached .claude/worktrees/determined-carson-439745` is one line and
   would unbreak the checkout — and therefore start auto-deploying `web/` to the
   congregation's site on every merge, through an unverified build, bypassing
   `release.sh`'s bump → build → verify discipline. That is a change in
   deployment posture, not a cleanup.
3. **Recommended, and only Miguel can do it:** disconnect the Pages git
   integration in the Cloudflare dashboard. `release.sh` is the blessed path;
   an integration that is wired up but permanently failing is a trap for whoever
   next tries to "fix CI". Once it is disconnected, removing the gitlink is
   harmless and worth doing.

## If you are here because you want merges to auto-deploy

Then do it deliberately, in this order: configure the Pages build command
(`npm ci && node web/build.mjs`) and output directory (`web/dist`) in the
dashboard, confirm the runner has `pdftoppm`/`cwebp` available (it very likely
does not — the CI runner installs them via Homebrew on macOS), *then* remove the
gitlink. Skipping the first two steps publishes a broken site.
