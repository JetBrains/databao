import { App } from "@modelcontextprotocol/ext-apps";
import { useEffect, useRef, useState } from "react";

import { Status } from "@/types";

export function useMCPApp() {
  const mcpApp = useRef(
    new App({
      name: "Databao Visualizer",
      version: "1.0.0",
    }),
  );

  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<Status>("INIT");

  useEffect(() => {
    setStatus("LOADING");

    const initApp = async () => {
      try {
        await mcpApp.current.connect();
        setStatus("LOADED");
      } catch (err) {
        setStatus("FAILED");
        setError("Failed to initialize MCP App: " + String(err));
      }
    };

    initApp();
  }, []);

  return { app: mcpApp.current, error, status };
}
