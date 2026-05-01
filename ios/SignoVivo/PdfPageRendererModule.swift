import Foundation
import PDFKit
import React
import UIKit

@objc(PdfPageRenderer)
class PdfPageRendererModule: NSObject {

  private let renderQueue = DispatchQueue(label: "com.alvernia.pdfrender", attributes: .concurrent)
  private var cache: [String: String] = [:]
  private let cacheLock = NSLock()
  // Cached PDFDocument per name — opening a PDF on every render is expensive.
  private var docCache: [String: PDFDocument] = [:]
  private let docCacheLock = NSLock()

  @objc func renderPage(_ pdfName: String, page pageNumber: Int,
                        resolve: @escaping RCTPromiseResolveBlock,
                        reject: @escaping RCTPromiseRejectBlock) {
    renderQueue.async {
      do {
        let path = try self.doRender(pdfName: pdfName, page: pageNumber)
        resolve(path)
      } catch {
        reject("RENDER_ERROR", "[\(pdfName) p\(pageNumber)] \(error.localizedDescription)", error)
      }
    }
  }

  @objc func prefetchPages(_ pdfName: String, pages: [Int],
                           resolve: @escaping RCTPromiseResolveBlock,
                           reject: @escaping RCTPromiseRejectBlock) {
    // Concurrent (no .barrier) — prefetch pages in parallel for speed.
    for p in pages {
      renderQueue.async {
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
    docCacheLock.lock()
    docCache.removeAll()
    docCacheLock.unlock()
    var failed = 0
    for path in paths {
      do { try FileManager.default.removeItem(atPath: path) }
      catch { failed += 1 }
    }
    if failed > 0 {
      // Non-fatal — files may already be gone; resolve anyway.
      NSLog("[PdfPageRenderer] clearCache: %d file(s) could not be deleted", failed)
    }
    resolve(nil)
  }

  private func cachedDoc(for pdfName: String) throws -> PDFDocument {
    docCacheLock.lock()
    if let doc = docCache[pdfName] { docCacheLock.unlock(); return doc }
    docCacheLock.unlock()

    guard let pdfURL = Bundle.main.url(forResource: pdfName, withExtension: "pdf")
                    ?? Bundle.main.url(forResource: pdfName, withExtension: nil) else {
      throw NSError(domain: "PdfPageRenderer", code: 10,
                    userInfo: [NSLocalizedDescriptionKey: "PDF not found in bundle: \(pdfName)"])
    }
    guard let doc = PDFDocument(url: pdfURL) else {
      throw NSError(domain: "PdfPageRenderer", code: 11,
                    userInfo: [NSLocalizedDescriptionKey: "PDFDocument could not open: \(pdfName)"])
    }
    docCacheLock.lock()
    docCache[pdfName] = doc
    docCacheLock.unlock()
    return doc
  }

  private func doRender(pdfName: String, page pageNumber: Int) throws -> String {
    let cacheKey = "\(pdfName)_\(pageNumber)"
    cacheLock.lock()
    if let cached = cache[cacheKey] {
      // Verify the cached file still exists — temp dir can be purged by iOS.
      let exists = FileManager.default.fileExists(atPath: cached)
      cacheLock.unlock()
      if exists { return cached }
      // Stale cache entry — re-render below.
      cacheLock.lock()
      cache.removeValue(forKey: cacheKey)
      cacheLock.unlock()
    } else {
      cacheLock.unlock()
    }

    let doc = try cachedDoc(for: pdfName)

    guard pageNumber >= 1 && pageNumber <= doc.pageCount else {
      throw NSError(domain: "PdfPageRenderer", code: 12,
                    userInfo: [NSLocalizedDescriptionKey: "Page \(pageNumber) out of range (1…\(doc.pageCount)) for \(pdfName)"])
    }
    guard let page = doc.page(at: pageNumber - 1) else {
      throw NSError(domain: "PdfPageRenderer", code: 13,
                    userInfo: [NSLocalizedDescriptionKey: "PDFPage nil at index \(pageNumber - 1) in \(pdfName)"])
    }

    let scale: CGFloat = 2.0
    let bounds = page.bounds(for: .mediaBox)
    guard bounds.width > 0, bounds.height > 0 else {
      throw NSError(domain: "PdfPageRenderer", code: 14,
                    userInfo: [NSLocalizedDescriptionKey: "Page \(pageNumber) has zero-size mediaBox in \(pdfName)"])
    }
    let size = CGSize(width: bounds.width * scale, height: bounds.height * scale)

    let renderer = UIGraphicsImageRenderer(size: size)
    let image = renderer.image { ctx in
      let cgCtx = ctx.cgContext
      UIColor.white.setFill()
      ctx.fill(CGRect(origin: .zero, size: size))
      // ⚠️ DO NOT SIMPLIFY THIS TRANSFORM — pages will render upside-down if you do.
      // PDF coordinate system: origin bottom-left, y increases upward.
      // UIKit/CGContext coordinate system: origin top-left, y increases downward.
      // Fix: translate origin to bottom of canvas, then flip y axis.
      // Removing either line, changing the sign of scale, or using a plain scaleBy(x:scale,y:scale)
      // will cause every page to render inverted. This has been broken and re-broken multiple times.
      cgCtx.translateBy(x: 0, y: size.height)
      cgCtx.scaleBy(x: scale, y: -scale)
      page.draw(with: .mediaBox, to: cgCtx)
    }

    // Try JPEG first; fall back to PNG so a page is never silently blank.
    let data: Data
    if let jpeg = image.jpegData(compressionQuality: 0.88) {
      data = jpeg
    } else if let png = image.pngData() {
      data = png
      NSLog("[PdfPageRenderer] JPEG encode failed for %@ p%d — fell back to PNG", pdfName, pageNumber)
    } else {
      throw NSError(domain: "PdfPageRenderer", code: 15,
                    userInfo: [NSLocalizedDescriptionKey: "Both JPEG and PNG encode failed for \(pdfName) p\(pageNumber)"])
    }

    let tmpDir = FileManager.default.temporaryDirectory
    let outURL = tmpDir.appendingPathComponent("pdf_\(cacheKey).jpg")
    do {
      try data.write(to: outURL, options: .atomic)
    } catch {
      throw NSError(domain: "PdfPageRenderer", code: 16,
                    userInfo: [NSLocalizedDescriptionKey: "Failed to write temp file for \(pdfName) p\(pageNumber): \(error.localizedDescription)"])
    }
    let path = outURL.path

    cacheLock.lock()
    cache[cacheKey] = path
    cacheLock.unlock()
    return path
  }

  @objc static func requiresMainQueueSetup() -> Bool { false }
}
