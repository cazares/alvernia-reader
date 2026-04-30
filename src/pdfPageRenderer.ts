import { NativeModules } from "react-native";

const { PdfPageRenderer } = NativeModules as {
  PdfPageRenderer: {
    renderPage(pdfName: string, page: number): Promise<string>;
    clearCache(): Promise<void>;
  };
};

export const renderPdfPage = (pdfName: string, page: number): Promise<string> =>
  PdfPageRenderer.renderPage(pdfName, page);

export const clearPdfPageCache = (): Promise<void> =>
  PdfPageRenderer.clearCache();
