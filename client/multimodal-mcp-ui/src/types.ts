export type MultimodalTabType = "DATAFRAME" | "DESCRIPTION" | "CHART";

export interface DatabaoMCPData {
  text?: string;
  dataframeHtmlContent?: string;
  spec?: object;
  thread_id?: string;
  availableModalities?: MultimodalTabType[];
}

export interface MCPContent {
  type: string;
  text?: string;
  [key: string]: unknown;
}

export interface MCPToolResult {
  content?: MCPContent[];
  [key: string]: unknown;
}

export type Status = "INIT" | "LOADING" | "FAILED" | "LOADED";

declare global {
  interface Window {
    __DATABAO_MCP_DATA__: DatabaoMCPData | null;
  }
}
