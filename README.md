# Alvernia Reader

Standalone Expo/React Native repo for the Alvernia PDF reader app.

## What this repo contains

- A single mobile app at the repo root
- A static web distribution build pipeline in `web/`
- The hardcoded Alvernia PDF assets in `assets/`
- The current shipping reader entrypoint in `PdfReaderApp.tsx`

`index.js` registers `PdfReaderApp`, so that is the reader currently used on device.

## Quick start

```bash
npm ci
npm run typecheck
npm run test:e2e
```

## Build the shareable web reader

```bash
npm run build:web
```

That generates a static one-page-at-a-time reader in `web/dist/` using rendered page images, which is easier to distribute quickly to non-technical users than TestFlight.

## Run on iOS

```bash
npx expo run:ios -d 'mPad' --configuration Release
```

## Notes

- The reader is currently configured around the hardcoded Alvernia PDF flow.
- The fastest public distribution path is Cloudflare Pages from `web/dist/`.
- The friendliest share path is the Worker custom domain at `https://miguelcoro.com/`.
- On iPhone or iPad, the intended install flow is Safari -> Compartir -> Agregar a pantalla de inicio.
- Deploy that route with `npx wrangler deploy -c cloudflare/alvernia-link/wrangler.jsonc`.
- Generated folders like `node_modules`, `ios/Pods`, and build output are intentionally not tracked.

## Upload + Promote pipeline

The Worker now exposes:

- `/upload` to upload a `.key` or `.pdf`
- `/promote` to request that the latest upload become the active reader content
- `/download` to grab the last converted PDF (optional)

To enable automatic Keynote conversion and promotion on a Mac:

1. Install Poppler for `pdftoppm`:
   `brew install poppler`
2. Make sure Keynote is installed and can open the file.
3. Export these env vars and run the helper:

```bash
export R2_BUCKET=alvernia-reader-uploads
export R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
node scripts/keynote-promote.mjs
```

This helper watches `latest.json` in R2 for a `/promote` request, converts `.key` -> PDF if needed, renders page images via `pdftoppm`, and uploads new `active/pages/*` plus `active/pages.json`.
