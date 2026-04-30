import { NativeModules } from "react-native";

const { PdfPageRenderer } = NativeModules as {
  PdfPageRenderer: {
    renderPage(pdfName: string, page: number): Promise<string>;
    prefetchPages(pdfName: string, pages: number[]): Promise<void>;
    clearCache(): Promise<void>;
  };
};

export const renderPdfPage = (pdfName: string, page: number): Promise<string> =>
  PdfPageRenderer.renderPage(pdfName, page);

export const prefetchPdfPages = (pdfName: string, pages: number[]): Promise<void> =>
  PdfPageRenderer.prefetchPages(pdfName, pages).catch(() => {});

export const clearPdfPageCache = (): Promise<void> =>
  PdfPageRenderer.clearCache().catch(() => {});
