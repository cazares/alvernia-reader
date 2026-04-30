#import <React/RCTBridgeModule.h>

@interface RCT_EXTERN_MODULE(PdfPageRenderer, NSObject)

RCT_EXTERN_METHOD(renderPage:(NSString *)pdfName
                  page:(NSInteger)page
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(prefetchPages:(NSString *)pdfName
                  pages:(NSArray<NSNumber *> *)pages
                  resolve:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)

RCT_EXTERN_METHOD(clearCache:(RCTPromiseResolveBlock)resolve
                  reject:(RCTPromiseRejectBlock)reject)

@end
