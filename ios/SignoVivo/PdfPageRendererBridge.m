#import <React/RCTBridgeModule.h>

@interface RCT_EXTERN_MODULE(PdfPageRenderer, NSObject)

RCT_EXTERN_METHOD(renderPage:(NSString *)pdfName
                  page:(nonnull NSNumber *)page
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(clearCache:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject)

@end
