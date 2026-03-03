export const MULTIMODAL_TABS = {
  DATAFRAME: "DATAFRAME",
  DESCRIPTION: "DESCRIPTION",
  CHART: "CHART",
} as const;

export type MultimodalTabType = keyof typeof MULTIMODAL_TABS;
export type Status = "initial" | "loading" | "loaded" | "failed";

export function isMultimodalTabType(tab: string): tab is MultimodalTabType {
  return tab in MULTIMODAL_TABS;
}
