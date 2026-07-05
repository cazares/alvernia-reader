# SignoVivo

A Catholic-parish choir songbook, used **live during Mass** to keep every singer on the
music director's current page.

## Architecture

One web app, two ways to run it:

- **Web PWA** — `web/src/app.js` (+ `web/src/index.html`, `styles.css`, `sw.js`), built by
  `web/build.mjs` into `web/dist/` and served at **signovivo.com** (Cloudflare Pages).
- **iOS shell** — an Expo/React Native app (`PdfReaderApp.tsx`) whose WKWebView loads the **same**
  web bundle from `file://` (the built `web/dist` is copied into `ios/WebBundle` at archive time).
  This is what runs on the parish iPads, offline.

Live page-sync during Mass:

- **Multipeer mesh** (`ios/SignoVivo/DirectorSyncModule.swift`) — the director iPad broadcasts its
  page to follower iPads over Bluetooth / peer-to-peer wifi. This is the **church-critical** path:
  the iPads have no internet inside the church.
- **Cloudflare Worker relay** (`sync-worker/src/index.ts`, room `alvernia-main`) — mirrors the page
  to web followers and internet-connected devices over WebSocket.

The songbook is a single public book (the ~371-page Alvernia manual), rendered from
`assets/alvernia_manual_2.pdf` to per-page WebP images + manifests at build time.

## Quick start

```bash
npm ci
npm run typecheck
node --test e2e/repo-minimal-footprint.test.mjs e2e/native-entrypoint.test.mjs \
  e2e/native-stability-config.test.mjs e2e/offline-books-integrity.test.mjs \
  e2e/nearby-sync-contract.test.mjs e2e/permission-flow.test.mjs \
  e2e/svRelayRoom.test.mjs e2e/svSelftest.test.mjs \
  e2e/svSyncDecision.test.mjs                             # the SAFE test subset
node scripts/smoke-boot.mjs                                # build + boot smoke test
```

> ⚠️ Do **not** run `npm run test:e2e` (the bare `e2e/*.test.mjs` glob) casually.
> `e2e/relay-sync.test.mjs` publishes to a relay room; it is disabled by default and refuses the
> live Mass room, but the glob is not the everyday test command. CI runs the safe named subset above.

## Build & deploy

- Web + native, lockstep: `bash scripts/release.sh` (bumps the version, builds web, archives the
  IPA, deploys web to signovivo.com). See `docs/pre-mass-checklist.md` for the safe rollout ritual.
- **Staging/canary:** `STAGING=1 bash scripts/release.sh` deploys to an isolated preview channel
  (pairs with `signovivo.com?env=staging`); it never touches production.
- Roll back a web deploy: `bash scripts/rollback-web.sh`.

## Where to read more

- `docs/app-hardening-plan.md` — the app-wide hardening plan (reconciled to build 374).
- `docs/major-update-2026-07.md` — the Release Safety System + feature roadmap.
- `docs/implementation-log.md` — running log of what's been implemented (start here to pick up work).
- `docs/app-atlas.md` — architecture navigation map.

## Notes

- Offline-first: the parish iPads run fully offline; the web bundle + book images are precached.
- `node_modules`, `ios/Pods`, `web/dist`, and `ios/WebBundle` are generated and not tracked.
