import { AnyModel } from "@anywidget/types";

import { Action, MessageRequest } from "./types";

export function initCommunication(model: AnyModel) {
  const sendMessage = <ActionT extends Action>(
    action: ActionT["type"],
    payload: ActionT["value"],
  ) => {
    const rawMessage = createRawMessage(action, payload);
    model.send(rawMessage);
  };

  return {
    sendMessage,
  };
}

function createRawMessage(
  action: Action["type"],
  payload: Action["value"],
): MessageRequest {
  return {
    action: {
      type: action,
      payload: JSON.stringify(payload),
    },
  };
}
