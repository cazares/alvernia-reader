# 🚀 Run Everything Locally - Quick Guide

Forget deployment for now - let's get you testing the new client-first downloads in **5 minutes**!

---

## ⚡ Super Quick Start

### Step 1: Start the Backend (Terminal 1)

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapi
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

**You should see:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
```

✅ Leave this running!

### Step 2: Install iOS Dependencies (Terminal 2)

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapp
npm install
```

This installs the new `@distube/ytdl-core` package and others.

### Step 3: Configure for Local Development

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapp
echo "EXPO_PUBLIC_API_URL=http://localhost:8000" > .env
```

### Step 4: Run on iOS Simulator

```bash
npm run ios
```

This will:
- Build the app
- Launch iOS Simulator
- Install and run Mixterious

### Step 5: Test a Download! 🎵

1. App opens in simulator
2. Type: `Rick Astley Never Gonna Give You Up`
3. Press download button
4. Watch Terminal 1 for backend logs
5. Watch the app for progress messages

**What to look for:**

In the app:
```
🔍 Finding audio source...
📥 Downloading (45%)...
✓ Download complete!
☁️ Uploading for processing...
```

In Terminal 1 (backend logs):
```
INFO: download strategy requested extra={'strategy': 'local_first'}
```

In Terminal 2 (iOS logs):
```
[multi] ✓ success: ytdl-core on attempt 1  ← This means client extraction worked!
[download-mgr] ✓ task completed in 12.3s
```

---

## ✅ Verification

Run these to verify everything works:

### 1. Test Backend Health

```bash
curl http://localhost:8000/health
# Should return: {"status":"ok"}
```

### 2. Test New Config Endpoint

```bash
curl http://localhost:8000/config/download-strategy
# Should return: {"strategy":"local_first","enableClientExtraction":true,...}
```

### 3. Test YouTube Extraction

```bash
curl "http://localhost:8000/youtube/audio-url?q=dQw4w9WgXcQ"
# Should return JSON with audio_url, title, duration...
```

---

## 🎯 Success Indicators

You'll know it's working when:

1. ✅ App launches successfully
2. ✅ Download starts without errors
3. ✅ Terminal 1 shows: `INFO: download strategy requested`
4. ✅ Terminal 2 shows: `[multi] ✓ success: ytdl-core`
5. ✅ Song processes and plays

The key indicator: **`[multi] ✓ success: ytdl-core`** means client-side extraction is working! 🎉

---

## 🐛 Troubleshooting

### "Cannot connect to server"

**Check backend is running:**
```bash
curl http://localhost:8000/health
```

If not running, start it:
```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapi
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### "Module not found: @distube/ytdl-core"

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapp
rm -rf node_modules package-lock.json
npm install
```

### Backend won't start

```bash
pip install -r /Users/cazares/Documents/src/mixterioso/requirements.txt
pip install -U "yt-dlp[default]"
```

### iOS build fails

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapp
rm -rf ios/build
cd ios && pod install && cd ..
npm run ios
```

### "All extraction attempts failed"

Update libraries:
```bash
# Update client-side extractor
cd karaoapp
npm update @distube/ytdl-core

# Update server-side fallback
pip install -U "yt-dlp[default]"
```

---

## 📱 Run on Physical iPhone (Optional)

If you want to test on your actual iPhone:

### 1. Find Your Mac's IP

```bash
ipconfig getifaddr en0
# Example output: 192.168.1.100
```

### 2. Update .env

```bash
cd /Users/cazares/Documents/src/mixterioso/karaoapp
echo "EXPO_PUBLIC_API_URL=http://192.168.1.100:8000" > .env
# Replace 192.168.1.100 with YOUR Mac's IP from step 1
```

### 3. Connect iPhone and Run

```bash
# Connect iPhone via USB, unlock it
npm run ios -- --device
```

**Important:** Your Mac and iPhone must be on the **same WiFi network**!

---

## 🎉 That's It!

You should now have:
- ✅ Backend running locally
- ✅ iOS app running in simulator
- ✅ New client-first download system working
- ✅ Metrics flowing to backend

**Next steps:**
1. Try different songs
2. Check metrics in backend logs
3. Follow `karaoapp/__tests__/TESTING_CHECKLIST.md` for thorough testing
4. Deploy later when ready

---

## 📊 Monitor Metrics

Check if metrics are flowing:

```bash
# In Terminal 1 (backend logs), look for:
grep "client metrics" logs/app.log

# Or watch in real-time:
tail -f logs/app.log | grep "client metrics"
```

---

## 🔄 Restart Everything

If things get weird:

```bash
# Terminal 1: Stop backend (Ctrl+C), then restart
cd /Users/cazares/Documents/src/mixterioso/karaoapi
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000

# Terminal 2: Stop app (Ctrl+C), clean, rebuild
cd /Users/cazares/Documents/src/mixterioso/karaoapp
rm -rf node_modules ios/build
npm install
npm run ios
```

---

## 💡 Tips

- Keep Terminal 1 (backend) visible to watch logs
- Look for `[multi] ✓ success: ytdl-core` in Terminal 2 - this confirms client extraction!
- If client extraction fails, it will automatically try server fallback
- Metrics are sent to backend every 5 minutes

---

**Ready?** Just run the commands in "Super Quick Start" above! 🚀
