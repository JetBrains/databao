import { MultimodalTabType } from "@/types";

export type AllActions = {
  SELECT_MODALITY: SelectModalityAction;
};

export type SelectModalityAction = {
  type: "SELECT_MODALITY";
  value: MultimodalTabType;
};
