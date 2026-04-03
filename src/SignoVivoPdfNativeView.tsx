import { requireNativeComponent, type ViewProps } from "react-native";

type PdfEvent = {
  nativeEvent: {
    page: number;
    totalPages: number;
    message?: string;
  };
};

export type SignoVivoPdfNativeViewProps = ViewProps & {
  source: string;
  page: number;
  onPageChange?: (event: PdfEvent) => void;
  onDocumentLoaded?: (event: PdfEvent) => void;
  onError?: (event: PdfEvent) => void;
};

export default requireNativeComponent<SignoVivoPdfNativeViewProps>("SignoVivoPdfView");
