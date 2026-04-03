import Foundation
import PDFKit
import React
import UIKit

@objc(SignoVivoPdfView)
final class SignoVivoPdfViewManager: RCTViewManager {
  override static func requiresMainQueueSetup() -> Bool {
    true
  }

  override func view() -> UIView! {
    SignoVivoPdfContainerView()
  }
}

final class SignoVivoPdfContainerView: UIView {
  private let pdfView = PDFView()
  private var currentDocumentURL: URL?
  private var lastReportedPage = 1

  @objc var source: NSString = "" {
    didSet {
      loadDocumentIfNeeded()
    }
  }

  @objc var page: NSNumber = 1 {
    didSet {
      goToPage(Int(truncating: page))
    }
  }

  @objc var onPageChange: RCTBubblingEventBlock?
  @objc var onDocumentLoaded: RCTBubblingEventBlock?
  @objc var onError: RCTBubblingEventBlock?

  override init(frame: CGRect) {
    super.init(frame: frame)

    backgroundColor = .black
    pdfView.translatesAutoresizingMaskIntoConstraints = false
    pdfView.autoScales = true
    pdfView.displayMode = .singlePageContinuous
    pdfView.displayDirection = .vertical
    pdfView.displaysPageBreaks = false
    pdfView.backgroundColor = .black
    pdfView.usePageViewController(false)
    addSubview(pdfView)

    NSLayoutConstraint.activate([
      pdfView.leadingAnchor.constraint(equalTo: leadingAnchor),
      pdfView.trailingAnchor.constraint(equalTo: trailingAnchor),
      pdfView.topAnchor.constraint(equalTo: topAnchor),
      pdfView.bottomAnchor.constraint(equalTo: bottomAnchor),
    ])

    NotificationCenter.default.addObserver(
      self,
      selector: #selector(handlePageChanged),
      name: Notification.Name.PDFViewPageChanged,
      object: pdfView
    )
  }

  required init?(coder: NSCoder) {
    nil
  }

  deinit {
    NotificationCenter.default.removeObserver(self)
  }

  private func loadDocumentIfNeeded() {
    let rawSource = String(source).trimmingCharacters(in: .whitespacesAndNewlines)
    guard !rawSource.isEmpty else {
      pdfView.document = nil
      currentDocumentURL = nil
      return
    }

    guard let documentURL = URL(string: rawSource) else {
      emitError("No pudimos abrir el PDF offline.")
      return
    }

    if documentURL == currentDocumentURL, pdfView.document != nil {
      goToPage(Int(truncating: page))
      return
    }

    guard let document = PDFDocument(url: documentURL) else {
      emitError("No pudimos cargar el documento PDF.")
      return
    }

    currentDocumentURL = documentURL
    pdfView.document = document

    let requestedPage = Int(truncating: page)
    let safePage = max(1, min(requestedPage, document.pageCount))
    lastReportedPage = safePage
    goToPage(safePage)

    onDocumentLoaded?([
      "page": safePage,
      "totalPages": document.pageCount,
    ])
  }

  private func goToPage(_ requestedPage: Int) {
    guard let document = pdfView.document, document.pageCount > 0 else {
      return
    }

    let safePage = max(1, min(requestedPage, document.pageCount))
    guard let targetPage = document.page(at: safePage - 1) else {
      return
    }

    pdfView.go(to: targetPage)
    reportPageChangeIfNeeded(pageNumber: safePage, totalPages: document.pageCount)
  }

  @objc private func handlePageChanged() {
    guard
      let document = pdfView.document,
      let currentPage = pdfView.currentPage
    else {
      return
    }

    let pageNumber = document.index(for: currentPage) + 1
    reportPageChangeIfNeeded(pageNumber: pageNumber, totalPages: document.pageCount)
  }

  private func reportPageChangeIfNeeded(pageNumber: Int, totalPages: Int) {
    guard pageNumber != lastReportedPage || onPageChange == nil else {
      return
    }

    lastReportedPage = pageNumber
    onPageChange?([
      "page": pageNumber,
      "totalPages": totalPages,
    ])
  }

  private func emitError(_ message: String) {
    onError?(["message": message])
  }
}
