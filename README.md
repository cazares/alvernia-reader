# SignoVivo

Single-purpose Expo/React Native reader for the bundled Alvernia PDF.

## What this repo contains

- One mobile app rooted at [`/Users/cazares/src/alvernia-reader-dev-reader-prep/PdfReaderApp.tsx`](/Users/cazares/src/alvernia-reader-dev-reader-prep/PdfReaderApp.tsx)
- The bundled PDF asset in [`/Users/cazares/src/alvernia-reader-dev-reader-prep/assets/alvernia_manual_2.pdf`](/Users/cazares/src/alvernia-reader-dev-reader-prep/assets/alvernia_manual_2.pdf)
- Native nearby Director sync for iPad in [`/Users/cazares/src/alvernia-reader-dev-reader-prep/ios/SignoVivo/DirectorSyncModule.swift`](/Users/cazares/src/alvernia-reader-dev-reader-prep/ios/SignoVivo/DirectorSyncModule.swift)

`index.js` registers `PdfReaderApp`, so the app is just the embedded PDF reader plus the hidden Director/Follower sync flow.

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
- PDF viewing is the primary behavior; page sync is the only extra feature.
- Mantener presionado el contador de página abre el modo oculto para elegir Director o Seguidor.
- Native iPad sync uses nearby device discovery and is intended to work without internet.
- Generated folders like `node_modules`, `ios/Pods`, and build output are intentionally not tracked.
