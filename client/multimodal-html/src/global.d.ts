declare global {
  interface Window {
    __DATA__?: {
      text: string;
      dataframeHtmlContent: string;
    };
  }
}

export {};
