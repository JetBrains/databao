import { App } from "@modelcontextprotocol/ext-apps";
import { useState } from "react";

import { Status } from "../types";

interface UseSpecModelReturn {
  spec: object | null;
  status: Status;
  onLoadSpec: (mcpApp: App, threadId?: string) => Promise<void>;
}

export function useSpecModel(): UseSpecModelReturn {
  const [spec, setSpec] = useState<object | null>(null);
  const [status, setStatus] = useState<Status>("INIT");

  const onLoadSpec = async (mcpApp: App, threadId?: string) => {
    if (status !== "INIT" || !threadId) return;

    setStatus("LOADING");

    try {
      const result = await mcpApp.callServerTool({
        name: "generate_spec",
        arguments: { thread_id: threadId },
      });

      const content = result.content?.find((c) => c.type === "text");
      if (content?.text) {
        const specData = JSON.parse(content.text) as {
          spec?: object;
          error?: string;
        };

        if (specData.error) {
          throw new Error(specData.error);
        }

        if (!specData.spec) {
          throw new Error("No spec found in the response");
        }

        setSpec(specData.spec);
        setStatus("LOADED");
      } else {
        setStatus("LOADED");
      }
    } catch (err) {
      console.error("Failed to load chart: " + String(err));
      setStatus("FAILED");
    }
  };

  return { spec, status, onLoadSpec };
}
