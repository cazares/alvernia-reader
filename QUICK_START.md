# Quick Start Guide - Client-First Downloads

## 🚀 Get Up and Running in 5 Minutes

### Step 1: Install Dependencies

```bash
cd karaoapp
npm install
# The new dependency @distube/ytdl-core is already in package.json
```

### Step 2: Configure Server URL

**Option A: Environment Variable (recommended)**
```bash
export EXPO_PUBLIC_API_URL=https://your-server.com
```

**Option B: Edit config file**
Edit `karaoapp/src/config.ts`:
```typescript
export const baseUrl = 'https://your-server.com';
```

### Step 3: Start the Server

```bash
cd karaoapi
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

Server will be available at `http://localhost:8000`

### Step 4: Run the iOS App

```bash
cd karaoapp
npm run ios
```

---

## ✅ Verify It's Working

### Test Download

1. Open the app on iOS simulator or device
2. Enter a YouTube URL or search: `Rick Astley Never Gonna Give You Up`
3. Press download
4. Watch for progress messages:
   - `🔍 Finding audio source...` - Client extracting metadata
   - `📥 Downloading (XX%)...` - Downloading to device
   - `✓ Validating file...` - Checking file integrity
   - `✓ Download complete!` - Success!
   - `☁️ Uploading for processing...` - Sending to server for Whisper

### Check Logs

**Client logs (iOS/React Native):**
Look for these in your terminal:
```
[download-mgr] task created: task_xxx
[extractor] fetching metadata: dQw4w9WgXcQ
[multi] ytdl-core attempt 1/3
[multi] ✓ success: ytdl-core on attempt 1
[download-mgr] ✓ task completed in 12.3s
[metrics] success recorded: {"provider":"ytdl-core",...}
```

**Server logs:**
```
INFO: download strategy requested strategy=local_first
INFO: client metrics received success_rate=95.0
```

---

## 🔍 Quick Debugging

### Downloads Failing?

**Check 1: Is server running?**
```bash
curl http://localhost:8000/health
# Should return: {"status":"ok"}
```

**Check 2: Is config correct?**
```bash
curl http://localhost:8000/config/download-strategy
# Should return: {"strategy":"local_first",...}
```

**Check 3: Try server fallback**
Force server-only mode to test:
```bash
curl -X POST http://localhost:8000/config/download-strategy \
  -H "Content-Type: application/json" \
  -d '{"strategy": "server_only"}'
```

Then try downloading again. If it works, the issue is client-side extraction.

**Restore client-first mode:**
```bash
curl -X POST http://localhost:8000/config/download-strategy \
  -H "Content-Type: application/json" \
  -d '{"strategy": "local_first"}'
```

### Client Extraction Not Working?

**Update ytdl-core:**
```bash
cd karaoapp
npm update @distube/ytdl-core
```

YouTube frequently changes their API, breaking extraction libraries.

### Network Issues?

Client extraction requires internet access. Check:
- Device has internet connection
- Not behind restrictive firewall
- YouTube is accessible from device

---

## 📊 View Metrics

### In App (Add to App.tsx for debugging)

```typescript
import { metricsCollector } from './lib/downloadMetrics';

// Add a button to show metrics
<Button onPress={() => console.log(metricsCollector.getSummary())} />
```

### From Server

```bash
# View recent metrics
tail -f karaoapi/logs/app.log | grep "client metrics"
```

---

## 🎯 Success Indicators

You'll know it's working correctly when you see:

1. ✅ **Download completes** - Song processes and plays
2. ✅ **Client extraction used** - Logs show `ytdl-core` as provider
3. ✅ **Fast downloads** - <15 seconds from search to play
4. ✅ **No server fallback** - Most downloads use client extraction
5. ✅ **Metrics flowing** - Server receives metrics every 5 minutes

---

## 🆘 Common Issues

### "Could not extract video ID"
- **Cause:** Invalid YouTube URL
- **Fix:** Ensure URL is from youtube.com or youtu.be

### "All extraction attempts failed"
- **Cause:** Both client and server extraction failed
- **Fix:** Check if video is available, not private/deleted

### "Network connection lost"
- **Cause:** Internet connection interrupted
- **Fix:** Check network, retry download

### "Not enough storage space"
- **Cause:** Device storage full
- **Fix:** Free up space on device

---

## 📚 Next Steps

Once basic functionality is working:

1. **Run Tests** - `npm test` in karaoapp directory
2. **Manual Testing** - Follow `__tests__/TESTING_CHECKLIST.md`
3. **Monitor Metrics** - Track success rates over time
4. **Deploy to TestFlight** - Share with beta testers

For complete documentation, see `IMPLEMENTATION_SUMMARY.md`.

---

## 🎉 You're Ready!

The system is now configured for client-first downloads. Downloads will:
- Extract YouTube URLs on the client (iOS device)
- Download audio directly to device
- Upload to server only for Whisper processing
- Fall back to server extraction if client fails

**Target metrics:**
- 90%+ success rate
- 80-90% client extraction
- <15s median latency

Happy downloading! 🎵
