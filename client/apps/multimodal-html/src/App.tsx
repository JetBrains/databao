import {
  DataframeTable,
  TabModel,
  VegaChart,
  Tabs,
} from "@databao/multimodal-tabs";
import { Sprite } from "@jetbrains/drt";
import { Text, Theme } from "@radix-ui/themes";

import styles from "./App.module.css";
import { useSpecGeneration } from "./hooks/useSpecGeneration";

function App() {
  const data = window.__DATA__;
  const { specConfig, specCsvData, specGenerationStatus } = useSpecGeneration();

  const renderChart = () => {
    return (
      <VegaChart
        status={specGenerationStatus}
        specData={specCsvData}
        specConfig={specConfig}
      />
    );
  };

  const renderDescription = () => {
    if (data?.text) {
      return <Text color="gray">{data.text}</Text>;
    }
    return <Text color="gray">No description available</Text>;
  };

  const renderTable = () => {
    if (data?.dataframeCsvContent) {
      return (
        <DataframeTable
          status={"loaded"}
          dataframeCsvData={data?.dataframeCsvContent}
        />
      );
    }
    return <Text color="gray">No data available</Text>;
  };

  const tabs: TabModel[] = [
    {
      type: "TABLE",
      title: "Data",
      content: () => renderTable(),
    },
    {
      type: "CHART",
      title: "Chart",
      content: () => renderChart(),
    },
    {
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(),
    },
  ];

  return (
    <>
      <div
        style={{
          height: "0",
          width: "0",
          overflow: "hidden",
        }}
      >
        <Sprite />
      </div>
      <Theme>
        <div className={styles.appContainer}>
          <Tabs tabs={tabs} />
        </div>
      </Theme>
    </>
  );
}

export default App;
