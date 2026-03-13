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

## Upload + Promote pipeline (Tunnel)

The Worker exposes:

- `/upload` to upload a `.key` or `.pdf`
- `/promote` to make the latest upload the active reader content
- `/download` to grab the last converted PDF (optional)

Tunnel-based workflow (Mac stays online):

1. Install Cloudflare Tunnel:
   `brew install cloudflared`
2. Install Poppler for `pdftoppm`:
   `brew install poppler`
3. Make sure Keynote is installed and can open the file.
4. Start the local upload server:

```bash
npm run upload:server
```

5. Create and run the tunnel:

```bash
cloudflared tunnel login
cloudflared tunnel create nuestro-coro-upload
cloudflared tunnel route dns nuestro-coro-upload upload.miguelcoro.com
cloudflared tunnel run --url http://localhost:8787 nuestro-coro-upload
```

Once the tunnel is up, `https://miguelcoro.com/upload` and `https://miguelcoro.com/promote` will proxy to your Mac.

## Upload + Promote pipeline (R2 fallback)

If you later enable R2, the Worker can also store uploads in R2 instead of the tunnel. The helper at `scripts/keynote-promote.mjs` will watch `latest.json` in R2 and promote automatically.
