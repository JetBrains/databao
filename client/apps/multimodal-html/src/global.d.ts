declare global {
  interface Window {
    __DATA__?: {
      text: string;
      dataframeCsvContent: string;
    };
  }
}

export {};
