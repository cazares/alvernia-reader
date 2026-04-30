import { NativeModules } from "react-native";

const { PdfPageRenderer } = NativeModules as {
  PdfPageRenderer: {
    renderPage(pdfName: string, page: number): Promise<string>;
    prefetchPages(pdfName: string, pages: number[]): Promise<void>;
    clearCache(): Promise<void>;
  } | undefined;
};

const RENDER_TIMEOUT_MS = 15_000;

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`[PdfPageRenderer] timeout: ${label}`)), ms);
    promise.then(
      (v) => { clearTimeout(t); resolve(v); },
      (e) => { clearTimeout(t); reject(e); },
    );
  });
}

export const renderPdfPage = (pdfName: string, page: number): Promise<string> => {
  if (!PdfPageRenderer) return Promise.reject(new Error("PdfPageRenderer native module not available"));
  return withTimeout(
    PdfPageRenderer.renderPage(pdfName, page),
    RENDER_TIMEOUT_MS,
    `${pdfName} p${page}`,
  );
};

export const prefetchPdfPages = (pdfName: string, pages: number[]): Promise<void> => {
  if (!PdfPageRenderer || pages.length === 0) return Promise.resolve();
  return PdfPageRenderer.prefetchPages(pdfName, pages).catch((e) => {
    if (__DEV__) console.warn("[PdfPageRenderer] prefetch failed:", e?.message ?? e);
  });
};

export const clearPdfPageCache = (): Promise<void> => {
  if (!PdfPageRenderer) return Promise.resolve();
  return PdfPageRenderer.clearCache().catch((e) => {
    if (__DEV__) console.warn("[PdfPageRenderer] clearCache failed:", e?.message ?? e);
  });
};
