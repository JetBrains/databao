import { Branded } from "@/utilities";

import { AllActions } from "./actions";

export type Action = AllActions[keyof AllActions];
export type MessageId = Branded<string, "MessageId">;

export type MessageRequest = {
  messageId: MessageId;
  action: {
    type: Action["type"];
    payload: string;
  };
};

export type MessageResponse = {
  messageId: MessageId;
  success: boolean;
  error: string;
  action: {
    type: Action["type"];
  };
};
