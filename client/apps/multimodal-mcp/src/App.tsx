import { DataframeTable, StatusRenderer, Tabs, VegaChart } from "@databao/multimodal-tabs";
import { Sprite } from "@jetbrains/drt";
import { Text, Theme } from "@radix-ui/themes";

import styles from "./app.module.css";
import { useMcpData } from "./communication/useMcpData";
import { isMultimodalTabType, MULTIMODAL_TABS } from "./types";

function App() {
  const {
    specConfig,
    specCsvData,
    description,
    availableTabs,
    specStatus,
    isConnected,
    connectionError,
  } = useMcpData();

  if (connectionError) {
    return (
      <Theme>
        <Text color="red">Connection failed: {connectionError.message}</Text>
      </Theme>
    );
  }

  if (!isConnected) {
    return (
      <Theme>
        <Text color="gray">Connecting...</Text>
      </Theme>
    );
  }

  const defaultTabs = {
    DATAFRAME: {
      type: MULTIMODAL_TABS.DATAFRAME,
      title: "Data",
      content: () => (
        <div className={styles.heightWrapper}>
          <DataframeTable dataframeCsvData={specCsvData} status={specStatus} />
        </div>
      ),
    },
    CHART: {
      type: MULTIMODAL_TABS.CHART,
      title: "Chart",
      content: () => (
        <div className={styles.heightWrapper}>
          <VegaChart specConfig={specConfig} specData={specCsvData} status={specStatus} />
        </div>
      ),
    },
    DESCRIPTION: {
      type: MULTIMODAL_TABS.DESCRIPTION,
      title: "Description",
      content: () => (
        <StatusRenderer
          getStatus={() => specStatus}
          value={description || null}
          renderValue={(value) => (
            <div className={styles.heightWrapper}>
              <Text color="gray">{value}</Text>
            </div>
          )}
          failed={<Text color="red">Failed to get description</Text>}
          empty={<Text color="gray">No description available</Text>}
        />
      ),
    },
  };

  const tabs = availableTabs
    .filter(isMultimodalTabType)
    .map((tab) => defaultTabs[tab]);

  return (
    <>
      <div style={{ height: "0", width: "0", overflow: "hidden" }}>
        <Sprite />
      </div>
      <Theme style={{ minHeight: "300px", maxHeight: "700px" }} asChild>
        <div className={styles.root}>
          <Tabs tabs={tabs} />
        </div>
      </Theme>
    </>
  );
}

export default App;
