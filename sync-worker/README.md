# signovivo-sync — featherweight live-follow relay

Lets people on **signovivo.com** (any phone browser, no app, no login) follow the
director live. The director's app POSTs the current page; the relay pushes it to
every connected browser. Only the page number crosses the wire (~50 bytes) — the
page images are already cached offline by the site's service worker.

Full design + rationale: [`../docs/web-follower-relay-plan.md`](../docs/web-follower-relay-plan.md).

## Endpoints

| Method | Path | Who | Purpose |
|---|---|---|---|
| `GET` (WebSocket) | `/r/:room/subscribe` | followers | receive the current snapshot + every update |
| `GET` | `/r/:room/state` | followers | poll fallback / first paint (tiny JSON) |
| `POST` | `/r/:room/publish` | director (auth) | set current page, fan out to subscribers |

One church = one room: **`alvernia-main`**. Payload:
```json
{ "v":1, "page":142, "totalPages":370, "mode":"standard", "bookId":"standard", "seq":87, "ts":1733500000 }
```
`seq` is monotonic (stale updates are ignored, so the song can't rewind). `seq:0`
means "no director live yet" — clients stay in free-browse.

## Deploy

Two lanes — pick one.

### Lane A — you run it (keeps the secret on your machine; recommended)
```bash
cd sync-worker
npm install                              # installs wrangler locally
npx wrangler login                       # opens a browser; approve once
npx wrangler secret put RELAY_DIRECTOR_TOKEN   # paste a long random string
npx wrangler deploy                      # prints your https://signovivo-sync.<acct>.workers.dev URL
```

### Lane B — Claude runs it
Grant access in this session (either `npx wrangler login` once here, or provide a
scoped `CLOUDFLARE_API_TOKEN`), then Claude runs the same `deploy` + `secret put`.

## Test it (no app changes needed)

1. Open `test-client.html` in any browser (desktop or phone).
2. Paste your relay URL (the `…workers.dev` one) and the token.
3. Click **Conectar / seguir** on one device, **Publicar página / ▶ Siguiente** on
   another → the follower's big number should update within a second.
4. For the weak-cell sanity check: do this on a phone over cell (not WiFi) and
   watch the log for drops/reconnects. (Coverage is good, so this should be boring.)

## Local dev
```bash
cd sync-worker
cp .dev.vars.example .dev.vars     # set RELAY_DIRECTOR_TOKEN
npm install && npm run dev          # http://localhost:8787
```
Point `test-client.html` at `http://localhost:8787`.

## Custom domain (later)
To serve from `sync.signovivo.com` instead of `*.workers.dev`, add to `wrangler.jsonc`:
```jsonc
"routes": [{ "pattern": "sync.signovivo.com", "custom_domain": true }]
```
then `npx wrangler deploy`. (DNS is already on Cloudflare, so this is one step.)

## Security
- `RELAY_DIRECTOR_TOKEN` is the only write credential. It lives in the director
  app build + as a Worker secret. **Never commit `.dev.vars`** (it's gitignored).
- Reads are public (a page number is not a secret). Worst-case abuse = a troll
  flipping the page; rotate the token to cut them off.
- Production: set `ALLOWED_ORIGINS` to `https://signovivo.com` in `wrangler.jsonc`.
