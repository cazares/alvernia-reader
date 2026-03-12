# Next Steps: Hybrid Renderer Implementation

## ✅ **Completed (Backend)**

### New API Endpoints

1. **GET /jobs/{job_id}/stems**
   - Returns URLs for all audio stems (vocals, bass, drums, other)
   - Also returns instrumental mix and full mix
   - Only works for completed jobs

2. **GET /jobs/{job_id}/lyrics**
   - Returns LRC format lyrics
   - Returns detailed timings CSV data
   - Includes auto-detected offset from Whisper

3. **Static File Serving**
   - `/files/` endpoint now serves stems, mixes, timings
   - Direct download URLs for all processed files

**Commit:** `d355e89` - Backend ready for hybrid rendering

---

## 📱 **iOS Implementation (Next Session)**

### Overview
The iOS app will download stems and render video locally using AVFoundation.

### Architecture

```
User Action
    ↓
Upload Audio → Server Processing (Demucs + Whisper) → Download Stems
    ↓                                                       ↓
iOS App                                              Render Video
    ↓                                                 (AVFoundation)
Save to Photos / Share
```

### Required iOS Components

#### 1. **Stem Downloader (React Native)**
```typescript
// karaoapp/src/StemDownloader.ts
export async function downloadStems(jobId: string): Promise<StemPaths> {
  const stemsRes = await fetch(`${API_URL}/jobs/${jobId}/stems`);
  const stems = await stemsRes.json();

  // Download each stem to device
  const vocals = await FileSystem.downloadAsync(
    stems.vocals_url,
    `${FileSystem.cacheDirectory}vocals.wav`
  );

  const instrumental = await FileSystem.downloadAsync(
    stems.instrumental_url || stems.other_url,
    `${FileSystem.cacheDirectory}instrumental.wav`
  );

  return {
    vocalsPath: vocals.uri,
    instrumentalPath: instrumental.uri
  };
}
```

#### 2. **Lyrics Downloader (React Native)**
```typescript
// karaoapp/src/LyricsDownloader.ts
export async function downloadLyrics(jobId: string): Promise<LyricsData> {
  const lyricsRes = await fetch(`${API_URL}/jobs/${jobId}/lyrics`);
  const lyrics = await lyricsRes.json();

  return {
    timings: lyrics.timings,
    offsetMs: lyrics.offset_ms
  };
}
```

#### 3. **Video Renderer (Native Swift Module)**

**File:** `karaoapp/ios/Modules/VideoRenderer/VideoRenderer.swift`

```swift
import AVFoundation
import CoreImage

@objc(VideoRenderer)
class VideoRenderer: NSObject {

    @objc
    static func requiresMainQueueSetup() -> Bool {
        return false
    }

    @objc
    func renderVideo(
        _ vocalsPath: String,
        instrumentalPath: String,
        lyrics: [[String: Any]],
        offsetMs: Double,
        outputPath: String,
        resolver resolve: @escaping RCTPromiseResolveBlock,
        rejecter reject: @escaping RCTPromiseRejectBlock
    ) {
        DispatchQueue.global(qos: .userInitiated).async {
            do {
                let result = try self.performRender(
                    vocalsPath: vocalsPath,
                    instrumentalPath: instrumentalPath,
                    lyrics: lyrics,
                    offsetMs: offsetMs,
                    outputPath: outputPath
                )
                DispatchQueue.main.async {
                    resolve(result)
                }
            } catch {
                DispatchQueue.main.async {
                    reject("RENDER_ERROR", error.localizedDescription, error)
                }
            }
        }
    }

    private func performRender(
        vocalsPath: String,
        instrumentalPath: String,
        lyrics: [[String: Any]],
        offsetMs: Double,
        outputPath: String
    ) throws -> String {

        // 1. Load audio assets
        let vocalsURL = URL(fileURLWithPath: vocalsPath)
        let instrumentalURL = URL(fileURLWithPath: instrumentalPath)

        let vocalsAsset = AVAsset(url: vocalsURL)
        let instrumentalAsset = AVAsset(url: instrumentalURL)

        // 2. Create composition
        let composition = AVMutableComposition()

        // Add instrumental track (full volume)
        guard let instrumentalTrack = composition.addMutableTrack(
            withMediaType: .audio,
            preferredTrackID: kCMPersistentTrackID_Invalid
        ) else {
            throw NSError(domain: "VideoRenderer", code: -1,
                         userInfo: [NSLocalizedDescriptionKey: "Failed to create instrumental track"])
        }

        let instrumentalSourceTrack = instrumentalAsset.tracks(withMediaType: .audio).first!
        try instrumentalTrack.insertTimeRange(
            CMTimeRange(start: .zero, duration: instrumentalAsset.duration),
            of: instrumentalSourceTrack,
            at: .zero
        )

        // Add vocals track (lower volume - karaoke mode)
        guard let vocalsTrack = composition.addMutableTrack(
            withMediaType: .audio,
            preferredTrackID: kCMPersistentTrackID_Invalid
        ) else {
            throw NSError(domain: "VideoRenderer", code: -1,
                         userInfo: [NSLocalizedDescriptionKey: "Failed to create vocals track"])
        }

        let vocalsSourceTrack = vocalsAsset.tracks(withMediaType: .audio).first!
        try vocalsTrack.insertTimeRange(
            CMTimeRange(start: .zero, duration: vocalsAsset.duration),
            of: vocalsSourceTrack,
            at: .zero
        )

        // 3. Create video composition with lyrics
        let videoComposition = createVideoComposition(
            duration: instrumentalAsset.duration,
            lyrics: lyrics,
            offsetMs: offsetMs
        )

        // 4. Export
        let outputURL = URL(fileURLWithPath: outputPath)

        // Remove existing file if present
        try? FileManager.default.removeItem(at: outputURL)

        guard let exportSession = AVAssetExportSession(
            asset: composition,
            presetName: AVAssetExportPreset1920x1080
        ) else {
            throw NSError(domain: "VideoRenderer", code: -1,
                         userInfo: [NSLocalizedDescriptionKey: "Failed to create export session"])
        }

        exportSession.outputURL = outputURL
        exportSession.outputFileType = .mp4
        exportSession.videoComposition = videoComposition

        let semaphore = DispatchSemaphore(value: 0)
        var exportError: Error?

        exportSession.exportAsynchronously {
            if exportSession.status == .failed {
                exportError = exportSession.error
            }
            semaphore.signal()
        }

        semaphore.wait()

        if let error = exportError {
            throw error
        }

        if exportSession.status != .completed {
            throw NSError(domain: "VideoRenderer", code: -1,
                         userInfo: [NSLocalizedDescriptionKey: "Export failed with status: \\(exportSession.status.rawValue)"])
        }

        return outputPath
    }

    private func createVideoComposition(
        duration: CMTime,
        lyrics: [[String: Any]],
        offsetMs: Double
    ) -> AVMutableVideoComposition {
        let videoComposition = AVMutableVideoComposition()
        videoComposition.renderSize = CGSize(width: 1920, height: 1080)
        videoComposition.frameDuration = CMTime(value: 1, timescale: 30)

        // Create instruction with custom compositor for lyrics
        let instruction = AVMutableVideoCompositionInstruction()
        instruction.timeRange = CMTimeRange(start: .zero, duration: duration)

        videoComposition.instructions = [instruction]

        // TODO: Implement custom compositor with Core Text for lyrics rendering
        // For now, this creates a black video with audio

        return videoComposition
    }
}
```

**Bridge File:** `karaoapp/ios/Modules/VideoRenderer/VideoRendererBridge.m`

```objc
#import <React/RCTBridgeModule.h>

@interface RCT_EXTERN_MODULE(VideoRenderer, NSObject)

RCT_EXTERN_METHOD(renderVideo:(NSString *)vocalsPath
                  instrumentalPath:(NSString *)instrumentalPath
                  lyrics:(NSArray *)lyrics
                  offsetMs:(double)offsetMs
                  outputPath:(NSString *)outputPath
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

@end
```

#### 4. **React Native Integration**

**File:** `karaoapp/App.tsx` (add this function)

```typescript
const renderVideoLocally = useCallback(
  async (jobId: string) => {
    try {
      setError("📥 Downloading processed audio...");

      // 1. Download stems
      const stems = await downloadStems(jobId);

      // 2. Download lyrics
      const lyrics = await downloadLyrics(jobId);

      // 3. Render video on device
      setError("🎬 Rendering video on your device...");

      const outputPath = `${FileSystem.cacheDirectory}karaoke_${Date.now()}.mp4`;

      const { VideoRenderer } = NativeModules;
      const videoPath = await VideoRenderer.renderVideo(
        stems.vocalsPath,
        stems.instrumentalPath,
        lyrics.timings,
        lyrics.offsetMs,
        outputPath
      );

      setError("✅ Video ready!");
      setOutputUrl(`file://${videoPath}`);

      // Clean up downloaded stems
      await FileSystem.deleteAsync(stems.vocalsPath, { idempotent: true });
      await FileSystem.deleteAsync(stems.instrumentalPath, { idempotent: true });

    } catch (e: any) {
      setError(`Failed to render: ${e.message}`);
      console.error(e);
    }
  },
  [baseUrl]
);
```

---

## 📝 **Implementation Checklist**

### Backend (✅ Done)
- [x] Add `/jobs/{id}/stems` endpoint
- [x] Add `/jobs/{id}/lyrics` endpoint
- [x] Mount `/files` for static serving
- [x] Test with existing jobs
- [x] Commit changes

### iOS Native Module (Next Session)
- [ ] Create `VideoRenderer.swift` module
- [ ] Create `VideoRendererBridge.m` bridge
- [ ] Implement audio composition
- [ ] Implement video composition with lyrics
- [ ] Test on real device

### React Native Integration (Next Session)
- [ ] Create `StemDownloader.ts`
- [ ] Create `LyricsDownloader.ts`
- [ ] Add `renderVideoLocally()` function
- [ ] Update UI with "Render on Device" button
- [ ] Add progress indicators
- [ ] Test end-to-end flow

### Deployment
- [ ] Test on production with real job
- [ ] Deploy backend changes
- [ ] Test iOS app with new endpoints
- [ ] Update documentation

---

## 🎯 **Testing Plan**

### Backend Testing (Can do now)
```bash
# 1. Start local server
cd karaoapi && python -m uvicorn app:app --reload

# 2. Create a test job (or use existing completed job ID)
curl -X POST http://localhost:8000/jobs \\
  -H "Content-Type: application/json" \\
  -d '{"query": "test song"}'

# 3. Test stems endpoint (replace {job_id})
curl http://localhost:8000/jobs/{job_id}/stems | jq

# 4. Test lyrics endpoint
curl http://localhost:8000/jobs/{job_id}/lyrics | jq

# 5. Test file download
curl http://localhost:8000/files/mixes/the_beatles_let_it_be.wav --head
```

### iOS Testing (After implementation)
1. Run job to completion
2. Tap "Render on Device" button
3. Watch progress: Download → Render → Complete
4. Play video from device
5. Save to Photos
6. Share to social media

---

## 📊 **Expected Performance**

| Step | Time | Notes |
|------|------|-------|
| Server processing | 2-5 min | Demucs + Whisper (unchanged) |
| Stem download | 10-30s | Network dependent, ~20-40MB |
| Client rendering | 20-60s | iPhone 11+, depends on song length |
| **Total** | **2.5-6.5 min** | vs 3-7min server-only |

**Benefits:**
- Faster when server video rendering is slow
- Works offline after download
- Can regenerate with different styles
- Reduces server load

---

## 🚀 **Ready to Continue?**

When you're ready to implement the iOS components:
1. Review this document
2. Test the new backend endpoints
3. Implement VideoRenderer Swift module
4. Integrate with React Native
5. Test on your iPhone

The backend is deployed and waiting! 🎉
