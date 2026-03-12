# Mixterious API Proxy Worker

This Worker serves `api.miguelendpoint.com/*` and forwards requests to a live upstream API origin.

## Emergency Recovery Flow

1. Start the local backend:
   ```bash
   ./start-backend-local.sh
   ```
2. Start an ngrok tunnel to port 8000 and copy the HTTPS URL:
   ```bash
   ngrok http 8000
   ```
3. Deploy the Worker with that URL:
   ```bash
   ./infra/cloudflare/api-proxy/deploy.sh https://<your-ngrok-url>
   ```
4. Verify:
   ```bash
   curl https://api.miguelendpoint.com/health
   ```

## Notes

- The upstream URL is injected via `API_ORIGIN` at deploy time.
- Keep the upstream process/tunnel running, otherwise the proxy will return HTTP 503.
