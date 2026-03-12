# Manual Testing Checklist

## Device Matrix

Test on the following devices:

- [ ] iPhone SE (2nd gen) - iOS 16
- [ ] iPhone 13 - iOS 17
- [ ] iPhone 15 Pro - iOS 18

## Network Conditions

Test under the following network conditions:

- [ ] WiFi (good connection)
- [ ] Cellular 5G
- [ ] Cellular 4G/LTE
- [ ] Slow 3G (simulated)
- [ ] Intermittent connection (airplane mode toggle)

## Test Scenarios

### Basic Functionality

1. **Popular song download**
   - [ ] Search: "Rick Astley Never Gonna Give You Up"
   - [ ] Expected: Downloads successfully, client extraction
   - [ ] Verify: Audio plays correctly after processing

2. **Obscure song download**
   - [ ] Search: Video with <10k views
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Client or server extraction works

3. **Direct YouTube URL**
   - [ ] Input: `https://www.youtube.com/watch?v=dQw4w9WgXcQ`
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Extracts video ID correctly

4. **Short YouTube URL**
   - [ ] Input: `https://youtu.be/dQw4w9WgXcQ`
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Extracts video ID correctly

5. **Direct video ID**
   - [ ] Input: `dQw4w9WgXcQ`
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Recognizes as video ID

### Edge Cases

6. **Long video (>10 minutes)**
   - [ ] Search: Video >10 minutes
   - [ ] Expected: Downloads successfully
   - [ ] Verify: File size >50MB is supported

7. **Short video (<1 minute)**
   - [ ] Search: Video <1 minute
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Small files are valid

8. **Age-restricted video**
   - [ ] Search: Age-restricted video
   - [ ] Expected: May fail or use server fallback
   - [ ] Verify: Clear error message if fails

9. **Recent upload (<1 week old)**
   - [ ] Search: Very recent video
   - [ ] Expected: Downloads successfully
   - [ ] Verify: Fresh URLs work

### App State Transitions

10. **App backgrounding during download**
    - [ ] Start download
    - [ ] Background app (home button)
    - [ ] Wait 10 seconds
    - [ ] Return to app
    - [ ] Expected: Download continues or completes
    - [ ] Verify: Progress is maintained

11. **App kill and restart during download**
    - [ ] Start download
    - [ ] Force quit app
    - [ ] Restart app
    - [ ] Expected: Download state is lost
    - [ ] Verify: Can start new download

12. **Network loss during download**
    - [ ] Start download
    - [ ] Enable airplane mode mid-download
    - [ ] Wait 5 seconds
    - [ ] Disable airplane mode
    - [ ] Expected: Retry or clear error
    - [ ] Verify: Handles gracefully

### Error Scenarios

13. **Invalid URL/query**
    - [ ] Input: "not a youtube url"
    - [ ] Expected: Clear error message
    - [ ] Verify: Doesn't crash

14. **Unavailable video**
    - [ ] Input: Deleted or private video
    - [ ] Expected: Clear error message
    - [ ] Verify: Server fallback attempted

15. **Low storage scenario**
    - [ ] Fill device storage to <100MB
    - [ ] Start download
    - [ ] Expected: Storage error message
    - [ ] Verify: Doesn't crash

### Performance

16. **Multiple simultaneous downloads**
    - [ ] Start 2-3 downloads
    - [ ] Expected: All complete successfully
    - [ ] Verify: No interference between downloads

17. **Cancel mid-download**
    - [ ] Start download
    - [ ] Hit cancel button
    - [ ] Expected: Download stops immediately
    - [ ] Verify: Partial file is cleaned up

### Resource Usage

18. **Battery impact test**
    - [ ] Start download with full battery
    - [ ] Complete 3-minute song download
    - [ ] Expected: <5% battery drain
    - [ ] Verify: Check battery usage in settings

19. **Thermal test**
    - [ ] Download 3 songs in succession
    - [ ] Expected: Device stays "warm" or cooler
    - [ ] Verify: No hot device or thermal throttling

20. **Memory test**
    - [ ] Download 10 songs
    - [ ] Expected: No memory warnings
    - [ ] Verify: App doesn't crash from memory

## Success Criteria

**Phase 1 (Initial Rollout):**
- [ ] 90%+ downloads succeed
- [ ] 80-90% use client extraction
- [ ] Average time-to-first-play <15s
- [ ] No crashes or freezes
- [ ] Battery impact <5% for 3-min song
- [ ] Device stays warm or cooler

**Red Flags (Rollback Required):**
- [ ] Success rate <75%
- [ ] Multiple crashes reported
- [ ] Thermal throttling or excessive battery drain
- [ ] Server fallback rate >30%

## Metrics to Monitor

After each test session:

- Total downloads attempted: _____
- Successful downloads: _____
- Client extraction success: _____
- Server fallback: _____
- Failures: _____
- Average download time: _____s
- Most common errors: _________

## Notes

Use this space for additional observations:

```
[Your notes here]
```
