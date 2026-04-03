#import <React/RCTViewManager.h>

@interface RCT_EXTERN_MODULE(SignoVivoPdfView, RCTViewManager)

RCT_EXPORT_VIEW_PROPERTY(source, NSString)
RCT_EXPORT_VIEW_PROPERTY(page, NSNumber)
RCT_EXPORT_VIEW_PROPERTY(onPageChange, RCTBubblingEventBlock)
RCT_EXPORT_VIEW_PROPERTY(onDocumentLoaded, RCTBubblingEventBlock)
RCT_EXPORT_VIEW_PROPERTY(onError, RCTBubblingEventBlock)

@end
