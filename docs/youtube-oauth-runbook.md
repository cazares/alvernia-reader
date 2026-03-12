# YouTube OAuth Runbook (Server-Safe)

## Goal

Keep Step5 uploads browser-free on the server.

Server behavior should be:
- Use `youtube_token.json` refresh token to renew access tokens.
- Never attempt interactive browser login in production jobs.

## Long-Term Pattern

1. Generate/rotate token on a local machine with browser access.
2. Store both client secrets JSON and token JSON in deployment environment variables.
3. Let startup materialize `/tmp/client_secret.json` and `/tmp/youtube_token.json`.
4. Step5 runs in refresh-token-only mode for non-interactive jobs.

## Local Rotation Command

```bash
cd /Users/cazares/Documents/karaoke-time-by-miguel
python3 scripts/youtube_token_bootstrap.py --client-secrets /absolute/path/client_secret.json
```

The script writes `youtube_token.json` and prints environment variable update commands.

## Required Vars

- `YOUTUBE_CLIENT_SECRETS_JSON=/tmp/client_secret.json`
- `YOUTUBE_CLIENT_SECRETS_JSON_RAW=<contents of client_secret.json>`
- `YOUTUBE_TOKEN_JSON_RAW=<contents of youtube_token.json>`

## Recovery When Step5 Fails Auth

If Step5 reports OAuth token invalid/missing:

1. Re-run local token bootstrap script.
2. Update environment variables above.
3. Redeploy service.

Do not run browser OAuth on the server unless explicitly debugging.
