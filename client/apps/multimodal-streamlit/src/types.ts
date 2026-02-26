export type MultimodalState = {
  generate_chart: boolean;
};

export type AppArgs = {
  availableTabs: MultimodalTabType[];
  spec: Record<string, unknown> | null;
  specError: string | null;
  dataframeHtmlContent: string | null;
  text: string | null;
  isSpecGenerationAvailable: boolean;
};

export const MULTIMODAL_TABS = {
  CHART: "CHART",
  DESCRIPTION: "DESCRIPTION",
  DATAFRAME: "DATAFRAME",
} as const;

export type MultimodalTabType = keyof typeof MULTIMODAL_TABS;

export function isMultimodalTabType(tab: string): tab is MultimodalTabType {
  return tab in MULTIMODAL_TABS;
}

export type Status = "initial" | "loading" | "loaded" | "failed" | "unavailable";
