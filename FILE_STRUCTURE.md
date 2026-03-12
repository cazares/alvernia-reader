# File Structure - Client-First Downloads

## 📂 Complete File Organization

```
mixterioso/
├── karaoapp/                          # iOS/React Native App
│   ├── src/
│   │   └── config.ts                  # ✨ NEW: Feature flags & remote config
│   │
│   ├── lib/                           # ✨ NEW: Core download system
│   │   ├── youtubeUtils.ts            # Video ID extraction, URL validation
│   │   ├── youtubeExtractor.ts        # YouTube metadata extraction (ytdl-core)
│   │   ├── extractorProvider.ts       # Multi-provider with fallback
│   │   ├── downloadManager.ts         # Download state machine
│   │   ├── downloadErrors.ts          # Error categorization
│   │   └── downloadMetrics.ts         # Client-side metrics collection
│   │
│   ├── __tests__/                     # ✨ NEW: Comprehensive tests
│   │   ├── youtubeExtractor.test.ts   # Unit tests for extraction
│   │   ├── downloadFlow.test.ts       # Integration tests
│   │   └── TESTING_CHECKLIST.md       # Manual testing guide
│   │
│   ├── App.tsx                        # ✏️ MODIFIED: Integrated download manager
│   └── package.json                   # ✏️ MODIFIED: Added @distube/ytdl-core
│
├── karaoapi/                          # Python/FastAPI Server
│   └── app.py                         # ✏️ MODIFIED: Added config & metrics endpoints
│
├── IMPLEMENTATION_SUMMARY.md          # ✨ NEW: Complete documentation
├── QUICK_START.md                     # ✨ NEW: 5-minute setup guide
└── FILE_STRUCTURE.md                  # ✨ NEW: This file

Legend:
  ✨ NEW - Newly created file
  ✏️ MODIFIED - Existing file with modifications
```

---

## 📄 File Descriptions

### Configuration & Core Utils

**`karaoapp/src/config.ts`** (318 lines)
- Download strategy configuration (local_first, server_fallback, server_only)
- Remote config fetching from server
- Feature flags and kill switch support
- Success criteria and phase targets

**`karaoapp/lib/youtubeUtils.ts`** (80 lines)
- Extract video ID from various YouTube URL formats
- Validate video ID format
- Build YouTube URLs and thumbnails
- Check if input is YouTube URL or search query

### YouTube Extraction

**`karaoapp/lib/youtubeExtractor.ts`** (187 lines)
- Client-side YouTube metadata extraction using ytdl-core
- Audio format selection (prefer m4a for iOS)
- URL expiry detection and caching
- Cache management with TTL

**`karaoapp/lib/extractorProvider.ts`** (157 lines)
- Provider adapter pattern for multiple extraction methods
- YtdlCoreProvider (client-side)
- ServerProvider (fallback to existing yt-dlp)
- Multi-provider with retry and exponential backoff

### Download Management

**`karaoapp/lib/downloadManager.ts`** (337 lines)
- Download state machine (queued → resolving → downloading → validating → ready)
- Progress tracking with callbacks
- Resumable downloads using expo-file-system
- File validation and cleanup
- Task management (cancel, cleanup, list)

**`karaoapp/lib/downloadErrors.ts`** (176 lines)
- Error type categorization
- User-friendly error messages
- Retry logic and backoff calculation
- Retryability determination

**`karaoapp/lib/downloadMetrics.ts`** (289 lines)
- Client-side metrics collection
- Success rate, fallback rate, average duration tracking
- Provider statistics
- Error type distribution
- Auto-sync to server every 5 minutes

### Testing

**`karaoapp/__tests__/youtubeExtractor.test.ts`** (143 lines)
- Unit tests for YouTube utils (video ID extraction, URL validation)
- Unit tests for YouTube extractor
- Cache behavior tests
- Error handling tests

**`karaoapp/__tests__/downloadFlow.test.ts`** (237 lines)
- Full download cycle integration tests
- Task management tests
- Error handling and categorization tests
- Provider fallback tests
- Cleanup tests

**`karaoapp/__tests__/TESTING_CHECKLIST.md`** (267 lines)
- Device matrix (iPhone SE, 13, 15 Pro)
- Network conditions (WiFi, 5G, 4G, 3G)
- 20 test scenarios (basic, edge cases, app state, errors, performance)
- Success criteria and red flags
- Metrics tracking template

### Server Endpoints

**`karaoapi/app.py`** (Modified - added 150+ lines)
- `GET /config/download-strategy` - Remote configuration for clients
- `POST /config/download-strategy` - Kill switch to force server-only mode
- `POST /metrics/download` - Receive and log client metrics

### Integration

**`karaoapp/App.tsx`** (Modified)
- Added imports for downloadManager and config
- Replaced downloadAndUpload() function to use new download manager
- Progress tracking with state machine
- Error handling and user feedback

**`karaoapp/package.json`** (Modified)
- Added `@distube/ytdl-core` dependency

### Documentation

**`IMPLEMENTATION_SUMMARY.md`** (472 lines)
- Complete architecture overview
- Setup instructions
- Configuration options
- Monitoring and troubleshooting
- Success metrics and targets
- Deployment checklist

**`QUICK_START.md`** (228 lines)
- 5-minute setup guide
- Quick debugging steps
- Common issues and solutions
- Success indicators

**`FILE_STRUCTURE.md`** (This file)
- Complete file organization
- File descriptions
- Line counts and responsibilities

---

## 📊 Implementation Statistics

- **Files Created:** 11 new files
- **Files Modified:** 3 existing files
- **Total Lines of Code:** ~2,500+ lines
- **Test Coverage:** Unit + Integration tests
- **Documentation:** 3 comprehensive guides

---

## 🎯 Key Components

### 1. Client-Side Extraction (80-90% of downloads)
```
youtubeExtractor.ts → extractorProvider.ts → downloadManager.ts
                                                      ↓
                                              App.tsx (UI)
```

### 2. Server Fallback (10-20% of downloads)
```
extractorProvider.ts → ServerProvider → app.py (/youtube/audio-url)
                                             ↓
                                      downloadManager.ts
```

### 3. Metrics Pipeline
```
downloadManager.ts → downloadMetrics.ts → app.py (/metrics/download)
                                                ↓
                                          Server logs
```

### 4. Remote Configuration
```
App.tsx → config.ts → getEffectiveConfig() → app.py (/config/download-strategy)
                                                   ↓
                                          Control client behavior
```

---

## 🔄 Data Flow

### Download Flow
1. User enters YouTube URL/query
2. `App.tsx` calls `downloadManager.startDownload()`
3. `downloadManager` creates task with state machine
4. **Resolve Stage:** `extractorProvider` tries ytdl-core first, server fallback
5. **Download Stage:** `expo-file-system` downloads audio to device
6. **Validate Stage:** Check file exists and size is valid
7. **Upload Stage:** Upload to server for Whisper processing
8. **Complete:** User can play karaoke track

### Metrics Flow
1. `downloadManager` records attempts/successes/failures
2. `downloadMetrics` aggregates metrics
3. Every 5 minutes: auto-sync to server via `POST /metrics/download`
4. Server logs metrics for monitoring

### Configuration Flow
1. App starts → `getEffectiveConfig()` fetches remote config
2. Server returns strategy (local_first, server_fallback, server_only)
3. App uses strategy to control download behavior
4. Admin can POST to `/config/download-strategy` to change strategy (kill switch)

---

## 🚀 Next Steps

1. **Install dependencies:** `cd karaoapp && npm install`
2. **Configure server URL:** Edit `karaoapp/src/config.ts`
3. **Start server:** `cd karaoapi && python -m uvicorn app:app --reload`
4. **Run app:** `cd karaoapp && npm run ios`
5. **Test downloads:** Use TESTING_CHECKLIST.md
6. **Monitor metrics:** Check server logs and metrics

For detailed instructions, see:
- `QUICK_START.md` - Get running in 5 minutes
- `IMPLEMENTATION_SUMMARY.md` - Complete documentation

---

**Status:** ✅ Implementation Complete
**Ready for:** Testing → TestFlight → Production
