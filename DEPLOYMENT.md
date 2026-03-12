# Mixterious Deployment Guide

## Server Requirements

### 1. Install yt-dlp

The server needs yt-dlp installed and up-to-date:

```bash
# Install or update yt-dlp
pip install -U yt-dlp

# Verify installation
yt-dlp --version
```

### 2. Install JavaScript Runtime (Recommended)

yt-dlp needs a JavaScript runtime to solve YouTube's signature challenges. Without this, many videos will fail to download.

**Option A: Install Deno (Recommended)**
```bash
# macOS/Linux
curl -fsSL https://deno.land/install.sh | sh

# Add to PATH in ~/.bashrc or ~/.zshrc
export PATH="$HOME/.deno/bin:$PATH"
```

**Option B: Install Node.js**
```bash
# Already have node? Check version:
node --version  # Should be v16 or higher

# If not installed, use your package manager
# macOS: brew install node
# Ubuntu/Debian: apt install nodejs npm
```

### 3. Environment Variables

Set these environment variables on the server:

```bash
# Enable JavaScript runtimes for signature solving
export MIXTERIOSO_YTDLP_JS_RUNTIMES="deno,node"  # or just "deno" or "node"

# Enable remote components for challenge solving (recommended)
export MIXTERIOSO_YTDLP_REMOTE_COMPONENTS="ejs:github"

# Optional: Custom user agent
export MIXTERIOSO_YTDLP_UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

# Optional: Extractor args for better YouTube compatibility
export MIXTERIOSO_YTDLP_EXTRACTOR_ARGS="youtube:player_client=android,web"

# Optional: Increase timeouts if on slow network
export MIXTERIOSO_YTDLP_SOCKET_TIMEOUT="10"
export MIXTERIOSO_YTDLP_RETRIES="3"
```

### 4. Test the Configuration

Run the test script to verify everything works:

```bash
cd /path/to/mixterioso
source .venv/bin/activate
python3 test_ytdlp_endpoint.py
```

You should see:
- ✅ Local yt-dlp: PASS
- ✅ Remote API: PASS

### 5. Common Issues

**Issue: "Signature solving failed"**
- Install deno or node.js
- Set `MIXTERIOSO_YTDLP_JS_RUNTIMES` and `MIXTERIOSO_YTDLP_REMOTE_COMPONENTS`

**Issue: "Failed to extract audio URL"**
- Update yt-dlp: `pip install -U yt-dlp`
- Check server logs for actual error message
- Try different extractor args: `youtube:player_client=android` or `youtube:player_client=web,android`

**Issue: "No formats found" or 403 errors**
- YouTube may be blocking the server's IP
- Try using cookies (see Cookie Setup below)
- Try different player clients via extractor args

**Issue: Server times out**
- Increase timeouts: `MIXTERIOSO_YTDLP_SOCKET_TIMEOUT=15`
- Check network connectivity to YouTube
- Verify firewall rules allow outbound HTTPS

### 6. Cookie Setup (Advanced)

For better YouTube access, you can provide cookies:

1. Export YouTube cookies using a browser extension like "Get cookies.txt LOCALLY"
2. Save to a file on the server (e.g., `/var/mixterioso/youtube_cookies.txt`)
3. Set environment variable:
   ```bash
   export MIXTERIOSO_YTDLP_COOKIES="/var/mixterioso/youtube_cookies.txt"
   ```

### 7. Restart the Server

After configuration changes:

```bash
# If using systemd:
sudo systemctl restart mixterious-api

# If using docker:
docker-compose restart

# If running manually:
# Kill the process and restart with new environment variables
```

## Quick Setup Script

```bash
#!/bin/bash
# quick-setup.sh - Run on your server

# Update yt-dlp
pip install -U yt-dlp

# Install deno if not present
if ! command -v deno &> /dev/null; then
    curl -fsSL https://deno.land/install.sh | sh
    export PATH="$HOME/.deno/bin:$PATH"
    echo 'export PATH="$HOME/.deno/bin:$PATH"' >> ~/.bashrc
fi

# Set environment variables (add to your .bashrc or systemd service file)
cat >> ~/.bashrc << 'EOF'
# Mixterious yt-dlp config
export MIXTERIOSO_YTDLP_JS_RUNTIMES="deno,node"
export MIXTERIOSO_YTDLP_REMOTE_COMPONENTS="ejs:github"
export MIXTERIOSO_YTDLP_EXTRACTOR_ARGS="youtube:player_client=android,web"
EOF

# Apply changes
source ~/.bashrc

echo "✅ Setup complete! Restart your Mixterious API server now."
```

## Monitoring

Check the server logs regularly for yt-dlp errors:

```bash
# View recent errors
grep "yt-dlp failed" /var/log/mixterious-api.log

# Monitor in real-time
tail -f /var/log/mixterious-api.log | grep youtube
```

## Performance Tips

1. **Use aria2c for faster downloads** (optional):
   ```bash
   apt install aria2  # Ubuntu/Debian
   brew install aria2  # macOS
   ```

2. **Increase concurrent fragments** for faster downloads:
   ```bash
   export MIXTERIOSO_YTDLP_CONCURRENT_FRAGS="8"
   ```

3. **Use caching** to avoid re-downloading:
   - yt-dlp has built-in caching
   - Consider adding Redis for API-level caching

## Security Notes

- Keep yt-dlp updated regularly (YouTube changes frequently)
- Don't expose raw yt-dlp errors to clients in production (sanitize error messages)
- Rate-limit the `/youtube/audio-url` endpoint to avoid abuse
- Consider using a CDN for serving audio files

## Support

For more information:
- yt-dlp documentation: https://github.com/yt-dlp/yt-dlp
- EJS (JavaScript challenge solving): https://github.com/yt-dlp/yt-dlp/wiki/EJS
- Mixterious issues: https://github.com/yourusername/mixterious/issues
