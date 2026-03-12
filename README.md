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
- The friendliest share path is the Worker custom domain at `https://miguelworld.com/`.
- Deploy that route with `npx wrangler deploy -c cloudflare/alvernia-link/wrangler.jsonc`.
- Generated folders like `node_modules`, `ios/Pods`, and build output are intentionally not tracked.
