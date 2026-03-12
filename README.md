# Alvernia Reader

Standalone Expo/React Native repo for the Alvernia PDF reader app.

## What this repo contains

- A single mobile app at the repo root
- The hardcoded Alvernia PDF assets in `assets/`
- The current shipping reader entrypoint in `PdfReaderApp.tsx`

`index.js` registers `PdfReaderApp`, so that is the reader currently used on device.

## Quick start

```bash
npm ci
npm run typecheck
node --test e2e/native-entrypoint.test.mjs e2e/native-stability-config.test.mjs e2e/eas-config.test.mjs
```

## Run on iOS

```bash
npx expo run:ios -d 'mPad' --configuration Release
```

## Notes

- The reader is currently configured around the hardcoded Alvernia PDF flow.
- Generated folders like `node_modules`, `ios/Pods`, and build output are intentionally not tracked.
