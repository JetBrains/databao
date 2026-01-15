import { MultimodalTabType } from "@/types";

export type AllActions = {
  INIT_WIDGET: InitWidgetAction;
  SELECT_MODALITY: SelectModalityAction;
};

export type InitWidgetAction = {
  type: "INIT_WIDGET";
  value: null;
};

export type SelectModalityAction = {
  type: "SELECT_MODALITY";
  value: MultimodalTabType;
};
