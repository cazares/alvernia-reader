import Foundation
import PDFKit
import React

@objc(PdfPageRenderer)
final class PdfPageRendererModule: NSObject {
  private var cache: [String: String] = [:]
  private let renderQueue = DispatchQueue(label: "com.alvernia.pdfrender", qos: .userInitiated)

  @objc static func requiresMainQueueSetup() -> Bool { false }

  @objc func renderPage(
    _ pdfName: String,
    page pageNum: Int,
    resolver resolve: @escaping RCTPromiseResolveBlock,
    rejecter reject: @escaping RCTPromiseRejectBlock
  ) {
    let cacheKey = "\(pdfName):\(pageNum)"
    if let cached = cache[cacheKey], FileManager.default.fileExists(atPath: cached) {
      resolve(cached); return
    }
    renderQueue.async { [weak self] in
      guard let self else { return }
      guard
        let url = Bundle.main.url(forResource: pdfName, withExtension: nil)
          ?? Bundle.main.url(forResource: (pdfName as NSString).deletingPathExtension,
                             withExtension: (pdfName as NSString).pathExtension),
        let doc = PDFDocument(url: url),
        pageNum >= 1, pageNum <= doc.pageCount,
        let page = doc.page(at: pageNum - 1)
      else {
        DispatchQueue.main.async { reject("PDF_ERROR", "Cannot open \(pdfName) page \(pageNum)", nil) }
        return
      }
      let bounds = page.bounds(for: .mediaBox)
      let scale: CGFloat = 2.0
      let size = CGSize(width: bounds.width * scale, height: bounds.height * scale)
      let renderer = UIGraphicsImageRenderer(size: size)
      let image = renderer.image { ctx in
        UIColor.white.setFill()
        ctx.fill(CGRect(origin: .zero, size: size))
        let cgCtx = ctx.cgContext
        cgCtx.scaleBy(x: scale, y: scale)
        page.draw(with: .mediaBox, to: cgCtx)
      }
      guard let data = image.jpegData(compressionQuality: 0.82) else {
        DispatchQueue.main.async { reject("PDF_ERROR", "Failed to encode page \(pageNum)", nil) }
        return
      }
      let tmpDir = FileManager.default.temporaryDirectory
      let outURL = tmpDir.appendingPathComponent("sv_pdf_\(pdfName)_p\(pageNum).jpg")
      do {
        try data.write(to: outURL)
        let path = outURL.path
        DispatchQueue.main.async {
          self.cache[cacheKey] = path
          resolve(path)
        }
      } catch {
        DispatchQueue.main.async { reject("PDF_ERROR", error.localizedDescription, error) }
      }
    }
  }

  @objc func clearCache(
    _ resolve: RCTPromiseResolveBlock,
    rejecter reject: RCTPromiseRejectBlock
  ) {
    for path in cache.values { try? FileManager.default.removeItem(atPath: path) }
    cache.removeAll()
    resolve(nil)
  }
}
