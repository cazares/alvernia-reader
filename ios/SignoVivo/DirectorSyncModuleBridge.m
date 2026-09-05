#import <React/RCTBridgeModule.h>
#import <React/RCTEventEmitter.h>

@interface RCT_EXTERN_MODULE(DirectorSyncModule, RCTEventEmitter)

RCT_EXTERN_METHOD(startDirector:(NSString *)sessionCode
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

// Live takeover: announces the new token to the director being replaced over the still-open
// session, then becomes director with that token — see takeoverDirector in DirectorSyncModule.swift.
RCT_EXTERN_METHOD(takeoverDirector:(NSString *)sessionCode
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(startFollower:(NSString *)sessionCode
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(getDeviceName:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(stop:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(resetForAppReset:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(sendPageUpdate:(nonnull NSNumber *)page
                  totalPages:(nonnull NSNumber *)totalPages
                  mode:(NSString *)mode
                  bookId:(NSString *)bookId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(primePermissions:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(refreshNearbyDiscovery:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

// Browser-only refresh. A brand-new director must find rivals fast WITHOUT going invisible to the
// followers already inviting it — see refreshBrowserOnly in DirectorSyncModule.swift.
RCT_EXTERN_METHOD(refreshDirectorBrowse:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(requestCurrentSnapshot:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

// ⟳ AGAINST A WEDGED SESSION. Implemented in Swift since the ⟳ rebuild and never declared here, so
// on the old architecture (newArchEnabled:false) it was never exported: NativeModules
// .DirectorSyncModule.forceFollowerReconnectNow was undefined, the JS wrapper's `typeof … ===
// "function"` guard — written as an OLD-shell fallback — failed on every shell ever shipped, and the
// call silently resolved null. So the one control that tears down a wedged MCSession has never run:
// refreshNearbyDiscovery cannot help (scheduleNextDiscoveryRefresh skips re-browsing entirely while
// connectedDirectorPeer is set) and requestCurrentSnapshot goes down the dead link. That is exactly
// the 2026-08-17 report — spinner animates, iPad stays on song 59.
RCT_EXTERN_METHOD(forceFollowerReconnectNow:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(setIdleTimerDisabled:(BOOL)disabled
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(requestDirectorTakeover:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(approveDirectorTakeover:(NSString *)requestId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(denyDirectorTakeover:(NSString *)requestId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

@end
