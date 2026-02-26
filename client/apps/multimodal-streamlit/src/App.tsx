import { DataframeTable, Tabs, VegaChart } from "@databao/multimodal-tabs";
import { Text, Theme } from "@radix-ui/themes";
import { useCallback, useEffect, useState } from "react";

import styles from "./app.module.css";
import { StatusRenderer } from "./components/StatusRenderer";
import {
  AppArgs,
  isMultimodalTabType,
  MULTIMODAL_TABS,
  MultimodalState,
  Status,
} from "./types";

type AppProps = {
  data: AppArgs;
  setTriggerValue: (
    name: keyof MultimodalState,
    value: MultimodalState[keyof MultimodalState],
  ) => void;
};

function App({ data, setTriggerValue }: AppProps) {
  const {
    availableTabs,
    spec,
    specError,
    dataframeHtmlContent,
    text,
    isSpecGenerationAvailable,
  } = data;

  const [specStatus, setSpecStatus] = useState<Status>("initial");

  useEffect(() => {
    if (spec) {
      setSpecStatus("loaded");
    } else if (specError) {
      setSpecStatus("failed");
    }
  }, [spec, specError]);

  const handleChangeTab = useCallback(
    (tab: string) => {
      if (!isMultimodalTabType(tab)) {
        console.error("Unknown tab value");
        return;
      }

      if (tab !== MULTIMODAL_TABS.CHART || specStatus !== "initial") {
        return;
      }

      if (!isSpecGenerationAvailable) {
        setSpecStatus("unavailable");
        return;
      }

      setSpecStatus("loading");
      setTriggerValue("generate_chart", true);
    },
    [specStatus, setTriggerValue, isSpecGenerationAvailable],
  );

  const renderChart = () => (
    <StatusRenderer
      status={specStatus}
      value={spec}
      renderValue={(value) => <VegaChart spec={value} />}
      empty={<Text color="gray">No chart available</Text>}
      failed={<Text color="gray">Failed to get data</Text>}
      unavailable={
        <Text color="gray">
          Chart generation is only available for the latest message
        </Text>
      }
    />
  );

  const renderDescription = (t: string) => (
    <StatusRenderer
      status="loaded"
      value={t}
      renderValue={(value) => (
        <Text as="p" style={{ whiteSpace: "pre-wrap" }}>
          {value}
        </Text>
      )}
      empty={<Text color="gray">No description available</Text>}
      failed={<Text color="gray">Failed to get data</Text>}
    />
  );

  const renderTable = (htmlContent: string) => (
    <StatusRenderer
      status="loaded"
      value={htmlContent}
      renderValue={(value) => <DataframeTable htmlContent={value} />}
      empty={<Text color="gray">No data available</Text>}
      failed={<Text color="gray">Failed to get data</Text>}
    />
  );

  const defaultTabs = {
    DATAFRAME: {
      type: MULTIMODAL_TABS.DATAFRAME,
      title: "Data",
      content: () => renderTable(dataframeHtmlContent ?? ""),
    },
    CHART: {
      type: MULTIMODAL_TABS.CHART,
      title: "Chart",
      content: () => renderChart(),
    },
    DESCRIPTION: {
      type: MULTIMODAL_TABS.DESCRIPTION,
      title: "Description",
      content: () => renderDescription(text ?? ""),
    },
  };

  const tabs = availableTabs
    .map((tab) => defaultTabs[tab])
    .filter((tab) => isMultimodalTabType(tab.type));

  return (
    <Theme appearance="dark" style={{ minHeight: "300px" }} asChild>
      <div className={styles.root}>
        <Tabs tabs={tabs} onChangeTab={handleChangeTab} />
      </div>
    </Theme>
  );
}

export default App;
