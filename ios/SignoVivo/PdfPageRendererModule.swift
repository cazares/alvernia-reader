import Foundation
import PDFKit

@objc(PdfPageRenderer)
class PdfPageRendererModule: NSObject {

  private let renderQueue = DispatchQueue(label: "com.alvernia.pdfrender", attributes: .concurrent)
  private var cache: [String: String] = [:]
  private let cacheLock = NSLock()

  @objc func renderPage(_ pdfName: String, page pageNumber: Int,
                        resolve: @escaping RCTPromiseResolveBlock,
                        reject: @escaping RCTPromiseRejectBlock) {
    renderQueue.async {
      do {
        let path = try self.doRender(pdfName: pdfName, page: pageNumber)
        resolve(path)
      } catch {
        reject("RENDER_ERROR", error.localizedDescription, error)
      }
    }
  }

  @objc func prefetchPages(_ pdfName: String, pages: [Int],
                           resolve: @escaping RCTPromiseResolveBlock,
                           reject: @escaping RCTPromiseRejectBlock) {
    for p in pages {
      renderQueue.async(flags: .barrier) {
        _ = try? self.doRender(pdfName: pdfName, page: p)
      }
    }
    resolve(nil)
  }

  @objc func clearCache(_ resolve: @escaping RCTPromiseResolveBlock,
                        reject: @escaping RCTPromiseRejectBlock) {
    cacheLock.lock()
    let paths = Array(cache.values)
    cache.removeAll()
    cacheLock.unlock()
    for path in paths {
      try? FileManager.default.removeItem(atPath: path)
    }
    resolve(nil)
  }

  private func doRender(pdfName: String, page pageNumber: Int) throws -> String {
    let cacheKey = "\(pdfName)_\(pageNumber)"
    cacheLock.lock()
    if let cached = cache[cacheKey] {
      cacheLock.unlock()
      return cached
    }
    cacheLock.unlock()

    guard let pdfURL = Bundle.main.url(forResource: pdfName, withExtension: "pdf") ?? Bundle.main.url(forResource: pdfName, withExtension: nil),
          let doc = PDFDocument(url: pdfURL),
          pageNumber >= 1 && pageNumber <= doc.pageCount,
          let page = doc.page(at: pageNumber - 1) else {
      throw NSError(domain: "PdfPageRenderer", code: 1, userInfo: [NSLocalizedDescriptionKey: "Cannot load PDF \(pdfName) page \(pageNumber)"])
    }

    let scale: CGFloat = 2.0
    let bounds = page.bounds(for: .mediaBox)
    let size = CGSize(width: bounds.width * scale, height: bounds.height * scale)

    let renderer = UIGraphicsImageRenderer(size: size)
    let image = renderer.image { ctx in
      UIColor.white.setFill()
      ctx.fill(CGRect(origin: .zero, size: size))
      ctx.cgContext.scaleBy(x: scale, y: scale)
      page.draw(with: .mediaBox, to: ctx.cgContext)
    }

    guard let data = image.jpegData(compressionQuality: 0.88) else {
      throw NSError(domain: "PdfPageRenderer", code: 2, userInfo: [NSLocalizedDescriptionKey: "JPEG encode failed"])
    }

    let tmpDir = FileManager.default.temporaryDirectory
    let outURL = tmpDir.appendingPathComponent("pdf_\(cacheKey).jpg")
    try data.write(to: outURL)
    let path = outURL.path

    cacheLock.lock()
    cache[cacheKey] = path
    cacheLock.unlock()
    return path
  }

  @objc static func requiresMainQueueSetup() -> Bool { false }
}
