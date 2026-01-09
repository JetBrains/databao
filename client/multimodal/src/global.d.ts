declare global {
  interface Window {
    __DATA__: {
      spec: object;
      text: string;
      dataframeHtmlContent: string;
    };
  }
}

export {};
