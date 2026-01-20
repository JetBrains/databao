import { Branded } from "@/utilities";

import { AllActions } from "./actions";

export type Action = AllActions[keyof AllActions];
export type MessageId = Branded<string, "MessageId">;

export type MessageRequest = {
  type: "databao_request";
  messageId: MessageId;
  action: {
    type: Action["type"];
    payload: string;
  };
};

export type MessageResponse = {
  type: "databao_response";
  messageId: MessageId;
  success: boolean;
  error: string;
  action: {
    type: Action["type"];
  };
};
