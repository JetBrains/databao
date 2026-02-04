import { EVENTS } from "./events";

type Message = {
  type: keyof typeof EVENTS;
  status: "loading" | "failed" | "loaded";
  error: string;
  data: string;
};

export function subscribeOnSpecGeneration(
  onSuccess: (data: Record<string, unknown>) => void,
  onError: (err: Error) => void,
  onLoading?: () => void,
  timeout: number = 30000,
) {
  const eventSource = new EventSource("/events");
  let timer: ReturnType<typeof setTimeout>;

  const setupTimeout = () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      onError(new Error("Error: Timeout exceeded"));
      eventSource.close();
    }, timeout);
  };

  const closeConnection = () => {
    clearTimeout(timer);
    eventSource.close();
  };

  eventSource.onmessage = (event: MessageEvent) => {
    const message = JSON.parse(event.data) as Message;
    setupTimeout();

    if (message.type !== EVENTS.GENERATE_SPEC) {
      return;
    }

    if (message.status === "failed") {
      onError(new Error(`Error: ${message.error}`));
      return;
    }

    if (message.status === "loading") {
      onLoading?.();
      return;
    }

    if (!message.data) {
      onError(new Error("Error: no data received"));
      return;
    }

    onSuccess(JSON.parse(message.data));
  };

  eventSource.addEventListener("close", closeConnection);

  eventSource.onerror = (event) => {
    clearTimeout(timer);
    const error =
      event instanceof ErrorEvent && event.error instanceof Error
        ? event.error
        : new Error("Error: EventSource connection error");
    onError(error);
  };

  setupTimeout();

  return closeConnection;
}
