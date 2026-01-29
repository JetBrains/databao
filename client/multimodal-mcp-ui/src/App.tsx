import {
  DataframeTable,
  TabModel,
  Tabs,
  VegaChart,
} from "@databao/multimodal-tabs";
import { Text, Theme } from "@radix-ui/themes";
import { useEffect, useState } from "react";

import styles from "./App.module.css";
import { StatusRenderer } from "./components/StatusRenderer";
import { useMCPApp } from "./hooks/useMcpApp";
import { useSpecModel } from "./hooks/useSpecModel";
import { DatabaoMCPData, MCPToolResult, MultimodalTabType } from "./types";

function DatabaoApp() {
  const [data, setData] = useState<DatabaoMCPData | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);

  const {
    app: MCPApp,
    error: connectionError,
    status: connectionStatus,
  } = useMCPApp();

  const { spec, status: specStatus, onLoadSpec } = useSpecModel();

  useEffect(() => {
    MCPApp.ontoolresult = (result: MCPToolResult) => {
      const content = result.content?.find((c) => c.type === "text");
      if (content?.text) {
        try {
          const vizData = JSON.parse(content.text) as DatabaoMCPData;
          setData(vizData);
        } catch (err) {
          setParseError("Failed to parse visualization data: " + err);
        }
      }
    };
  }, [MCPApp]);

  const handleTabChange = (tabType: string) => {
    if (tabType === "CHART") {
      onLoadSpec(MCPApp, data?.thread_id);
    }
  };

  const error = connectionError || parseError;

  const renderChart = (spec: object | null) => (
    <StatusRenderer
      status={specStatus}
      value={spec}
      renderValue={(value) => <VegaChart spec={value} />}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No chart available</Text>}
    />
  );

  const renderDescription = (text: string | null) => (
    <StatusRenderer
      status="LOADED"
      value={text}
      renderValue={(value) => <Text color="gray">{value}</Text>}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No description available</Text>}
    />
  );

  const renderTable = (dataframeHtmlContent: string | null) => (
    <StatusRenderer
      status="LOADED"
      value={dataframeHtmlContent}
      renderValue={(value) => <DataframeTable htmlContent={value} />}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No data available</Text>}
    />
  );

  const renderSystemMessage = (message: string, color: "gray" | "red") => {
    return (
      <Theme>
        <div className={styles.appContainer}>
          <div style={{ padding: "40px", textAlign: "center" }}>
            <Text color={color} size="3">
              {message}
            </Text>
          </div>
        </div>
      </Theme>
    );
  };

  if (connectionStatus === "LOADING") {
    return renderSystemMessage("Initializing...", "gray");
  }

  if (connectionStatus === "FAILED") {
    return renderSystemMessage(`Failed to connect: ${error}`, "red");
  }

  if (!data) {
    return renderSystemMessage("Failed to get data", "red");
  }

  const defaultTabs: Record<MultimodalTabType, TabModel> = {
    DATAFRAME: {
      type: "DATAFRAME",
      title: "Data",
      content: () => renderTable(data.dataframeHtmlContent || null),
    },
    CHART: {
      type: "CHART",
      title: "Chart",
      content: () => renderChart(spec),
    },
    DESCRIPTION: {
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(data.text || null),
    },
  };

  const availableModalities = data.availableModalities || [];
  const tabs = availableModalities
    .map((modality) => defaultTabs[modality])
    .filter((tab) => tab !== undefined);

  if (tabs.length === 0) {
    return renderSystemMessage("No data available", "gray");
  }

  return (
    <Theme>
      <div className={styles.appContainer}>
        <Tabs tabs={tabs} onChangeTab={handleTabChange} />
      </div>
    </Theme>
  );
}

export default DatabaoApp;
