# YouTube Download Fix Summary

## Problem
The iOS app was trying to do "client-side download" but the server endpoint `/youtube/audio-url` was failing with 500 errors. The error message was generic ("Failed to extract audio URL") and didn't reveal the actual problem.

## Root Cause
The server's yt-dlp was likely failing due to:
1. Missing JavaScript runtime (deno/node) for YouTube signature solving
2. Missing configuration for remote components and extractor args
3. Generic error messages hiding the actual yt-dlp errors

## Changes Made

### 1. Improved Error Messages (karaoapi/app.py)
- Now includes actual yt-dlp stderr in error responses
- Shows both stdout and stderr when URL extraction fails
- Makes debugging much easier

### 2. Added Missing yt-dlp Configuration (karaoapi/app.py)
- Added `--js-runtimes` flag if configured
- Added `--remote-components` flag if configured
- These are needed for YouTube signature solving

### 3. Created Test Script (test_ytdlp_endpoint.py)
- Tests yt-dlp locally
- Tests remote API endpoint
- Shows configuration and detailed errors

### 4. Created Deployment Guide (DEPLOYMENT.md)
- Complete setup instructions for the remote server
- Environment variable configuration
- Common issues and solutions
- Quick setup script

## Test Results

**Local Test (MacBook):**
✅ yt-dlp works correctly
✅ Successfully extracts audio URL for "the beatles - let it be"
⚠️  Shows warnings about signature solving (but still works)

**Remote Test (api.miguelendpoint.com):**
❌ Returns 500 error "Failed to extract audio URL"
- Need to check server logs for actual error
- Likely needs deno/node installed and environment variables set

## Next Steps

### On Your Development Machine (✅ Done)
1. ✅ Improved error messages in API code
2. ✅ Added missing yt-dlp configuration
3. ✅ Created test and deployment documentation

### On Remote Server (api.miguelendpoint.com) - ACTION REQUIRED
1. **Install deno** (recommended) or ensure node.js is available
   ```bash
   curl -fsSL https://deno.land/install.sh | sh
   ```

2. **Update yt-dlp**
   ```bash
   pip install -U yt-dlp
   ```

3. **Set environment variables** (add to systemd service or .bashrc)
   ```bash
   export MIXTERIOSO_YTDLP_JS_RUNTIMES="deno,node"
   export MIXTERIOSO_YTDLP_REMOTE_COMPONENTS="ejs:github"
   export MIXTERIOSO_YTDLP_EXTRACTOR_ARGS="youtube:player_client=android,web"
   ```

4. **Deploy the updated API code**
   ```bash
   git pull
   # Restart your API server
   ```

5. **Test the endpoint**
   ```bash
   curl "https://api.miguelendpoint.com/youtube/audio-url?q=test"
   ```
   - Should now show actual error message if it fails
   - Should work if deno/node is installed and configured

6. **Check server logs** for detailed yt-dlp errors
   - Look for "yt-dlp failed" messages
   - Verify the command being run
   - Check stderr output

## How to Verify the Fix

1. Deploy code changes to server
2. Configure environment variables
3. Restart API server
4. Run the test script:
   ```bash
   python3 test_ytdlp_endpoint.py
   ```
5. Test the iOS app - search for "the beatles - let it be"
6. Should now either:
   - ✅ Work successfully, or
   - Show detailed error message explaining what's wrong

## Why This Fix Works

1. **Better Errors**: You'll now see exactly what yt-dlp is complaining about
2. **Signature Solving**: JavaScript runtime enables decoding YouTube's protected URLs
3. **Remote Components**: Downloads the latest challenge solver scripts from GitHub
4. **Extractor Args**: Uses Android/web player clients which are more reliable

## Additional Notes

- The query format "artist - song" is **not the issue** - it works fine locally
- The hyphen and special characters are handled correctly by yt-dlp
- The issue is purely server-side configuration

## Quick Test Commands

Test locally:
```bash
source .venv/bin/activate
python3 test_ytdlp_endpoint.py
```

Test remote endpoint directly:
```bash
curl "https://api.miguelendpoint.com/youtube/audio-url?q=the%20beatles%20-%20let%20it%20be"
```

Test with video ID:
```bash
curl "https://api.miguelendpoint.com/youtube/audio-url?q=CGj85pVzRJs"
```

## Files Changed
- `karaoapi/app.py` - Improved error handling and added yt-dlp config
- `test_ytdlp_endpoint.py` - New test script
- `DEPLOYMENT.md` - New deployment guide
- `FIX_SUMMARY.md` - This file

## Commit Message
```
fix: Add yt-dlp signature solving support + better error messages

- Include yt-dlp stderr in API error responses for better debugging
- Add JS runtime and remote components flags for YouTube signature solving
- Create test script to diagnose yt-dlp issues locally and remotely
- Add comprehensive deployment guide with server setup instructions

The server needs deno/node installed and these env vars set:
- MIXTERIOSO_YTDLP_JS_RUNTIMES="deno,node"
- MIXTERIOSO_YTDLP_REMOTE_COMPONENTS="ejs:github"
- MIXTERIOSO_YTDLP_EXTRACTOR_ARGS="youtube:player_client=android,web"

Generated with [Claude Code](https://claude.ai/code)
via [Happy](https://happy.engineering)

Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: Happy <yesreply@happy.engineering>
```
