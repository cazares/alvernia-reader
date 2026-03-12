# Hybrid Client-Side Rendering Implementation

## Overview
This document outlines the implementation of Option D: Hybrid Approach for on-device karaoke rendering.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   iPhone    │────▶│    Server    │────▶│   iPhone    │
│  (Upload)   │     │  (Process)   │     │  (Render)   │
└─────────────┘     └──────────────┘     └─────────────┘
      │                     │                    │
   Audio File         Demucs + Whisper      AVFoundation
                      Lyrics Sync            Video Render
```

## Backend Changes (karaoapi)

### 1. Add Stems Download Endpoint

**File:** `karaoapi/app.py`

```python
@app.get("/jobs/{job_id}/stems")
async def get_job_stems(job_id: str) -> dict:
    """
    Return URLs for downloading processed audio stems.

    Returns:
        {
            "vocals_url": "https://...",
            "instrumental_url": "https://...",
            "full_mix_url": "https://..."
        }
    """
    job = _get_job(job_id)
    if not job or job.status != "succeeded":
        raise HTTPException(status_code=404, detail="Job not found or not completed")

    slug = job.slug
    stems_dir = SEPARATED_DIR / "htdemucs" / slug
    mix_file = MIXES_DIR / f"{slug}.wav"

    # Check if stems exist (Demucs was run)
    vocals_path = stems_dir / "vocals.wav"
    instrumental_path = stems_dir / "no_vocals.wav"  # Or combined other stems

    result = {}

    if vocals_path.exists():
        result["vocals_url"] = f"/files/separated/htdemucs/{slug}/vocals.wav"
    if instrumental_path.exists():
        result["instrumental_url"] = f"/files/separated/htdemucs/{slug}/no_vocals.wav"
    if mix_file.exists():
        result["full_mix_url"] = f"/files/mixes/{slug}.wav"

    return result


@app.get("/jobs/{job_id}/lyrics")
async def get_job_lyrics(job_id: str) -> dict:
    """
    Return lyrics timings for client-side rendering.

    Returns:
        {
            "lrc": "...",
            "timings": [{...}],
            "offset_ms": 0
        }
    """
    job = _get_job(job_id)
    if not job or job.status != "succeeded":
        raise HTTPException(status_code=404, detail="Job not found or not completed")

    slug = job.slug
    lrc_file = TIMINGS_DIR / f"{slug}.lrc"
    csv_file = TIMINGS_DIR / f"{slug}.csv"
    offset_file = TIMINGS_DIR / f"{slug}.offset.auto"

    result = {}

    if lrc_file.exists():
        result["lrc"] = lrc_file.read_text(encoding="utf-8")

    if csv_file.exists():
        # Parse CSV timings
        import csv
        timings = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                timings.append({
                    "start_ms": float(row["start_ms"]),
                    "end_ms": float(row["end_ms"]),
                    "text": row["text"]
                })
        result["timings"] = timings

    if offset_file.exists():
        result["offset_ms"] = float(offset_file.read_text().strip())
    else:
        result["offset_ms"] = 0

    return result


# Add static file serving for stems
@app.get("/files/{path:path}")
async def serve_file(path: str):
    """Serve processed files (stems, mixes, etc.)"""
    file_path = ROOT / path
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    from fastapi.responses import FileResponse
    return FileResponse(file_path)
```

## iOS Implementation (karaoapp)

### 2. Create Native Video Renderer Module

**File:** `karaoapp/ios/VideoRenderer.swift`

```swift
import AVFoundation
import CoreImage
import UIKit

@objc(VideoRenderer)
class VideoRenderer: NSObject {

    @objc
    func renderVideo(
        _ vocalsUrl: String,
        instrumentalUrl: String,
        lyrics: [[String: Any]],
        outputPath: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        DispatchQueue.global(qos: .userInitiated).async {
            do {
                try self.performRender(
                    vocalsUrl: vocalsUrl,
                    instrumentalUrl: instrumentalUrl,
                    lyrics: lyrics,
                    outputPath: outputPath
                )
                resolve(outputPath)
            } catch {
                reject("RENDER_ERROR", error.localizedDescription, error)
            }
        }
    }

    private func performRender(
        vocalsUrl: String,
        instrumentalUrl: String,
        lyrics: [[String: Any]],
        outputPath: String
    ) throws {
        // 1. Load audio assets
        let vocalsAsset = AVAsset(url: URL(string: vocalsUrl)!)
        let instrumentalAsset = AVAsset(url: URL(string: instrumentalUrl)!)

        // 2. Create composition
        let composition = AVMutableComposition()

        // Add vocal track (muted/lower volume)
        let vocalsTrack = composition.addMutableTrack(
            withMediaType: .audio,
            preferredTrackID: kCMPersistentTrackID_Invalid
        )
        try vocalsTrack?.insertTimeRange(
            CMTimeRange(start: .zero, duration: vocalsAsset.duration),
            of: vocalsAsset.tracks(withMediaType: .audio)[0],
            at: .zero
        )

        // Add instrumental track (full volume)
        let instrumentalTrack = composition.addMutableTrack(
            withMediaType: .audio,
            preferredTrackID: kCMPersistentTrackID_Invalid
        )
        try instrumentalTrack?.insertTimeRange(
            CMTimeRange(start: .zero, duration: instrumentalAsset.duration),
            of: instrumentalAsset.tracks(withMediaType: .audio)[0],
            at: .zero
        )

        // 3. Create video track with lyrics
        let videoTrack = composition.addMutableTrack(
            withMediaType: .video,
            preferredTrackID: kCMPersistentTrackID_Invalid
        )

        // 4. Add lyrics as subtitle track or overlays
        let videoComposition = self.createVideoComposition(
            composition: composition,
            lyrics: lyrics
        )

        // 5. Export
        let outputUrl = URL(fileURLWithPath: outputPath)
        let export = AVAssetExportSession(
            asset: composition,
            presetName: AVAssetExportPreset1920x1080
        )
        export?.videoComposition = videoComposition
        export?.outputURL = outputUrl
        export?.outputFileType = .mp4

        let semaphore = DispatchSemaphore(value: 0)
        export?.exportAsynchronously {
            semaphore.signal()
        }
        semaphore.wait()

        if export?.status != .completed {
            throw NSError(
                domain: "VideoRenderer",
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Export failed"]
            )
        }
    }

    private func createVideoComposition(
        composition: AVComposition,
        lyrics: [[String: Any]]
    ) -> AVMutableVideoComposition {
        let videoComposition = AVMutableVideoComposition()
        videoComposition.renderSize = CGSize(width: 1920, height: 1080)
        videoComposition.frameDuration = CMTime(value: 1, timescale: 30)

        // Create custom compositor for lyrics overlay
        // TODO: Implement lyrics rendering with Core Image/Core Text

        return videoComposition
    }
}
```

### 3. React Native Bridge

**File:** `karaoapp/ios/VideoRendererBridge.m`

```objc
#import <React/RCTBridgeModule.h>

@interface RCT_EXTERN_MODULE(VideoRenderer, NSObject)

RCT_EXTERN_METHOD(renderVideo:(NSString *)vocalsUrl
                  instrumentalUrl:(NSString *)instrumentalUrl
                  lyrics:(NSArray *)lyrics
                  outputPath:(NSString *)outputPath
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)

@end
```

### 4. TypeScript Integration

**File:** `karaoapp/src/VideoRenderer.ts`

```typescript
import { NativeModules } from 'react-native';

interface VideoRenderer {
  renderVideo(
    vocalsUrl: string,
    instrumentalUrl: string,
    lyrics: Array<{ start_ms: number; end_ms: number; text: string }>,
    outputPath: string
  ): Promise<string>;
}

const { VideoRenderer } = NativeModules;

export default VideoRenderer as VideoRenderer;
```

### 5. UI Integration

**File:** `karaoapp/App.tsx` (additions)

```typescript
const renderVideoOnDevice = useCallback(
  async (jobId: string) => {
    try {
      setError("📥 Downloading stems...");

      // 1. Get stems URLs
      const stemsRes = await fetchWithTimeout(
        `${baseUrl}/jobs/${jobId}/stems`
      );
      const stems = await stemsRes.json();

      // 2. Get lyrics
      const lyricsRes = await fetchWithTimeout(
        `${baseUrl}/jobs/${jobId}/lyrics`
      );
      const lyrics = await lyricsRes.json();

      // 3. Download stems to device
      setError("📥 Downloading audio...");
      const vocalsPath = await FileSystem.downloadAsync(
        stems.vocals_url,
        `${FileSystem.cacheDirectory}vocals.wav`
      );
      const instrumentalPath = await FileSystem.downloadAsync(
        stems.instrumental_url,
        `${FileSystem.cacheDirectory}instrumental.wav`
      );

      // 4. Render video on device
      setError("🎬 Rendering video...");
      const outputPath = `${FileSystem.cacheDirectory}karaoke_${Date.now()}.mp4`;

      const videoPath = await VideoRenderer.renderVideo(
        vocalsPath.uri,
        instrumentalPath.uri,
        lyrics.timings,
        outputPath
      );

      setError("✅ Video ready!");
      setOutputUrl(videoPath);

    } catch (e: any) {
      setError(`Failed: ${e.message}`);
    }
  },
  [baseUrl]
);
```

## Testing Steps

1. **Backend Testing:**
   ```bash
   # Start server
   cd karaoapi && python -m uvicorn app:app --reload

   # Test stems endpoint
   curl http://localhost:8000/jobs/{job_id}/stems

   # Test lyrics endpoint
   curl http://localhost:8000/jobs/{job_id}/lyrics
   ```

2. **iOS Testing:**
   ```bash
   cd karaoapp
   npm run ios
   ```

3. **End-to-End Test:**
   - Upload audio file
   - Wait for server processing
   - Download stems to iPhone
   - Render video on iPhone
   - Save to Photos

## Performance Targets

| Step | Time | Notes |
|------|------|-------|
| Stem download | 10-30s | Depends on network |
| Video render | 20-60s | iPhone-dependent |
| **Total** | **30-90s** | vs 2-5min server |

## Benefits

- ✅ No YouTube blocking issues
- ✅ High-quality Demucs separation
- ✅ Faster overall experience
- ✅ Offline playback after download
- ✅ No server video rendering bottleneck

## Next Steps

1. Implement `/jobs/{id}/stems` endpoint
2. Implement `/jobs/{id}/lyrics` endpoint
3. Create VideoRenderer Swift module
4. Add download + render flow to App.tsx
5. Test on real iPhone
6. Deploy to production

## Estimated Timeline

- Backend endpoints: 2-3 hours
- iOS renderer: 1-2 days
- Integration + testing: 1 day
- **Total: 2-3 days**
