import { useApp } from "@modelcontextprotocol/ext-apps/react";
import { useState } from "react";

import { MultimodalTabType, MULTIMODAL_TABS, Status } from "@/types";

const ALL_TABS: MultimodalTabType[] = Object.keys(
  MULTIMODAL_TABS,
) as MultimodalTabType[];

interface VisualizationPayload {
  spec?: Record<string, unknown>;
  csvData?: string;
  description?: string;
  availableTabs?: MultimodalTabType[];
}

export function useMcpData() {
  const [specConfig, setSpecConfig] = useState<Record<string, unknown> | null>(
    null,
  );
  const [specCsvData, setSpecCsvData] = useState<string>("");
  const [description, setDescription] = useState<string>("");
  const [availableTabs, setAvailableTabs] =
    useState<MultimodalTabType[]>(ALL_TABS);
  const [specStatus, setSpecStatus] = useState<Status>("initial");

  const { isConnected, error } = useApp({
    appInfo: { name: "Databao Visualization", version: "1.0.0" },
    capabilities: {},
    onAppCreated: (app) => {
      app.ontoolinput = () => {
        setSpecStatus("loading");
      };

      app.ontoolresult = (result) => {
        const textContent = result.content?.find((c) => c.type === "text");

        if (!textContent || textContent.type !== "text" || !textContent.text) {
          setSpecStatus("failed");
          return;
        }

        try {
          const data = JSON.parse(textContent.text) as VisualizationPayload;

          setSpecConfig(data.spec ?? null);
          setSpecCsvData(data.csvData ?? "");
          setDescription(data.description ?? "");

          if (data.availableTabs?.length) {
            setAvailableTabs(data.availableTabs);
          }

          setSpecStatus("loaded");
        } catch {
          setSpecStatus("failed");
        }
      };
    },
  });

  return {
    specConfig,
    specCsvData,
    description,
    availableTabs,
    specStatus,
    isConnected,
    connectionError: error,
  };
}
