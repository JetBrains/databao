import { AnyModel } from "@anywidget/types";

import { Action, MessageId, MessageRequest, MessageResponse } from "./types";

type ResponseEvent = {
  success: boolean;
};

export function initCommunication(model: AnyModel) {
  const sendMessage = async <ActionT extends Action>(
    action: ActionT["type"],
    payload: ActionT["value"],
    timeout: number = 30_000,
  ) => {
    const promise = new Promise<void>((resolve, reject) => {
      const rawMessage = createRawMessage(action, payload);
      model.send(rawMessage);

      const timer = setTimeout(() => {
        reject(new Error("Failed: Timeout exceeded"));
      }, timeout);

      const handler = (event: Event) => {
        clearTimeout(timer);

        const { detail } = event as CustomEvent<ResponseEvent>;

        if (detail?.success) {
          resolve();
        } else {
          reject(new Error("Failed: Request was failed"));
        }
      };

      document.addEventListener(rawMessage.messageId, handler, {
        once: true,
      });
    });

    return promise;
  };

  const onAcceptMessage = (msg: MessageResponse) => {
    document.dispatchEvent(
      new CustomEvent<ResponseEvent>(msg.messageId, {
        detail: {
          success: msg.success,
        },
      }),
    );
  };

  model.on("msg:custom", (msg: MessageResponse) => {
    if (msg.type !== "databao_response") {
      return;
    }
    onAcceptMessage(msg);
  });

  return {
    sendMessage,
  };
}

function createRawMessage(
  action: Action["type"],
  payload: Action["value"],
): MessageRequest {
  const messageId = generateId();

  return {
    type: "databao_request",
    messageId,
    action: {
      type: action,
      payload: JSON.stringify(payload),
    },
  };
}

export function generateId() {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? (crypto.randomUUID() as MessageId)
    : (`id-${Date.now()}-${Math.random().toString(36).slice(2, 11)}` as MessageId);
}
