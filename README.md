# SignoVivo

Single-purpose Expo/React Native reader for the bundled Alvernia PDF.

## What this repo contains

- One mobile app rooted at [`/Users/cazares/src/alvernia-reader-dev-reader-prep/PdfReaderApp.tsx`](/Users/cazares/src/alvernia-reader-dev-reader-prep/PdfReaderApp.tsx)
- The bundled PDF asset in [`/Users/cazares/src/alvernia-reader-dev-reader-prep/assets/alvernia_manual_2.pdf`](/Users/cazares/src/alvernia-reader-dev-reader-prep/assets/alvernia_manual_2.pdf)

`index.js` registers `PdfReaderApp`, so the app is just the offline embedded reader.

## Quick start

```bash
npm ci
npm run typecheck
npm run test:e2e
```

## Run on iOS

```bash
npx expo run:ios -d 'mPad' --configuration Release
```

## Notes

- The app is intentionally offline-first and ships the PDF inside the bundle.
- PDF viewing is the primary behavior and the app is intentionally stripped down for reliable offline launch.
- The shipped iOS app no longer includes nearby sync or local-network permissions.
- Generated folders like `node_modules`, `ios/Pods`, and build output are intentionally not tracked.
