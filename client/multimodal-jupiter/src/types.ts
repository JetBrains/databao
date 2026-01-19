export const MULTIMODAL_TABS = {
  CHART: "CHART",
  DESCRIPTION: "DESCRIPTION",
  DATAFRAME: "DATAFRAME",
} as const;

export type MultimodalTabType = keyof typeof MULTIMODAL_TABS;
export type Status = "initial" | "computating" | "computated" | "failed";
