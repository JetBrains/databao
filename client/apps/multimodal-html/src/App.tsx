import {
  DataframeTable,
  TabModel,
  VegaChart,
  Tabs,
} from "@databao/multimodal-tabs";
import { Spinner, Text, Theme } from "@radix-ui/themes";
import { useEffect, useState } from "react";

import styles from "./App.module.css";
import { subscribeOnSpecGeneration } from "./communication/communication";

type ConnectionStatus = "initial" | "loading" | "failed" | "loaded";

function App() {
  const data = window.__DATA__;

  const [spec, setSpec] = useState<object | null>(null);
  const [specStatus, setSpecStatus] = useState<ConnectionStatus>("initial");

  useEffect(() => {
    setSpecStatus("loading");

    const onSuccess = (data: Record<string, unknown>) => {
      setSpec(data);
      setSpecStatus("loaded");
    };

    const onError = (e: Error) => {
      setSpecStatus("failed");
      console.error(e.message);
    };

    const unsubscribe = subscribeOnSpecGeneration(onSuccess, onError);

    return () => {
      unsubscribe();
    };
  }, []);

  const renderChart = (spec: object | null) => {
    if (specStatus === "initial" || specStatus === "loading") {
      return (
        <div className={styles.loader}>
          <Spinner />
          <Text color="gray">Generating...</Text>
        </div>
      );
    }

    if (specStatus === "failed") {
      return <Text color="gray">Failed to get chart...</Text>;
    }

    if (specStatus === "loaded" && spec) {
      return <VegaChart spec={spec} />;
    }

    return <Text color="gray">No chart available</Text>;
  };

  const renderDescription = (text?: string) => {
    if (text) {
      return <Text color="gray">{text}</Text>;
    }
    return <Text color="gray">No description available</Text>;
  };

  const renderTable = (dataframeHtmlContent?: string) => {
    if (dataframeHtmlContent) {
      return <DataframeTable htmlContent={dataframeHtmlContent} />;
    }
    return <Text color="gray">No data available</Text>;
  };

  const tabs: TabModel[] = [
    {
      type: "TABLE",
      title: "Data",
      content: () => renderTable(data?.dataframeHtmlContent),
    },
    {
      type: "CHART",
      title: "Chart",
      content: () => renderChart(spec),
    },
    {
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(data?.text),
    },
  ];

  return (
    <Theme>
      <div className={styles.appContainer}>
        <Tabs tabs={tabs} />
      </div>
    </Theme>
  );
}

export default App;
