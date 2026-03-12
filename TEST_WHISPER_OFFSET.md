# Testing Whisper Auto-Offset

## Issue
Original concern: "lyrics are coming in around 1s too late on let it be by the beatles"

## What We Fixed
- PR #121: Added `LD_LIBRARY_PATH` to Dockerfile to fix whisper.cpp library loading
- Previously, whisper was failing silently with exit code 127
- Now deployed in production as karaoapi-00121-fgq

## How to Test

### Method 1: Using iPhone with YouTube URL Paste (Recommended)

Since YouTube is blocking server downloads, use your iPhone:

1. **Find a YouTube URL for "Let It Be":**
   - Go to YouTube and search "the beatles let it be official"
   - Copy the URL (e.g., `https://www.youtube.com/watch?v=QDYfEBY9NM4`)

2. **Open Mixterious App (v1.2.0):**
   - Paste the YouTube URL into the search box
   - Or paste just the video ID: `QDYfEBY9NM4`
   - Tap "Download locally to iPhone"

3. **Wait for Processing:**
   - Download: ~10-30 seconds
   - Server processing: 2-5 minutes
   - Status will show "succeeded" when done

4. **Check the Results:**
   - The video should be generated with synchronized lyrics
   - Play the video and verify if lyrics appear at the correct time
   - Previously they were ~1 second too late

### Method 2: Test with Existing Endpoints

If you have a completed job, check if whisper offset was calculated:

```bash
# Replace {job_id} with your actual job ID
JOB_ID="your-job-id-here"

# Test the new /lyrics endpoint
curl "https://karaoapi-383407170280.us-central1.run.app/jobs/${JOB_ID}/lyrics" | jq

# Expected output should include:
# {
#   "lrc": "[00:00.00]When I find myself...",
#   "timings": [{start_ms: 0, end_ms: 2000, text: "When I find myself"}],
#   "offset_ms": 123.45  <-- THIS IS THE AUTO-DETECTED OFFSET
# }
```

### Method 3: Check Server Files Directly

If you can SSH to the server or have Cloud Storage access:

```bash
# Check if .offset.auto file exists
ls -la /app/timings/the_beatles_let_it_be.offset.auto

# Read the offset value
cat /app/timings/the_beatles_let_it_be.offset.auto

# Should contain a number like: 123.45
```

## What to Look For

### ✅ Success Indicators:
1. **File exists:** `/app/timings/{slug}.offset.auto` file is created
2. **Non-zero offset:** The offset value is not 0 (shows whisper calculated something)
3. **Accurate timing:** When playing the video, lyrics appear in sync with vocals
4. **Logs show whisper ran:** Production logs show whisper.cpp executed without errors

### ❌ Failure Indicators:
1. **No offset file:** Whisper didn't run or failed silently
2. **Zero offset:** Whisper ran but couldn't detect offset
3. **Late lyrics:** Lyrics still appear 1s late (same as before)
4. **Library error:** Logs show "libwhisper.so.1: cannot open shared object file"

## Expected Behavior

**Before Fix (PR #121):**
- Whisper would fail with exit code 127
- No `.offset.auto` file created
- Lyrics would use manual offset or no offset
- Result: ~1 second delay

**After Fix (karaoapi-00121-fgq):**
- Whisper runs successfully
- `.offset.auto` file created with calculated offset
- Lyrics use auto-detected offset
- Result: Accurate timing (within 100-200ms)

## Testing Commands

```bash
# Create a job with YouTube URL paste (from iPhone)
# - Open Mixterious app
# - Paste: youtube.com/watch?v=QDYfEBY9NM4
# - Tap "Download locally to iPhone"
# - Note the job ID from the response

# Once job succeeds, test the new endpoints:
JOB_ID="your-job-id-here"

# Get lyrics with offset
curl "https://karaoapi-383407170280.us-central1.run.app/jobs/${JOB_ID}/lyrics" | jq '.offset_ms'

# Get stems for hybrid rendering
curl "https://karaoapi-383407170280.us-central1.run.app/jobs/${JOB_ID}/stems" | jq

# Download the video and check timing manually
# (This is the most reliable test)
```

## Alternative Test Songs

If "Let It Be" lyrics aren't available, try these popular songs:

1. **"Shape of You" - Ed Sheeran**
   - Very popular, likely has synced lyrics
   - YouTube: `JGwWNGJdvx8`

2. **"Bohemian Rhapsody" - Queen**
   - Classic song, well-documented lyrics
   - YouTube: `fJ9rUzIMcZQ`

3. **"Rolling in the Deep" - Adele**
   - Popular, clear vocals
   - YouTube: `rYEDA3JcQqw`

## Next Steps After Successful Test

1. ✅ Confirm whisper offset is working
2. ✅ Verify lyrics timing is accurate
3. ✅ Test new `/stems` and `/lyrics` endpoints
4. 🔄 Begin iOS VideoRenderer implementation
5. 🔄 Test hybrid rendering end-to-end

## Notes

- YouTube is currently blocking server downloads
- iPhone-first download (with URL paste) is the most reliable method
- The whisper fix is deployed in karaoapi-00121-fgq
- Test as soon as you're back with your iPhone
