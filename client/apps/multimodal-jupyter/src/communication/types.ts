import { AllActions } from "./actions";

export type Action = AllActions[keyof AllActions];

export type MessageRequest = {
  action: {
    type: Action["type"];
    payload: string;
  };
};
