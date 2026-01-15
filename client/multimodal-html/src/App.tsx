import {
  DataframeTable,
  MultimodalTab,
  MultimodalTabs,
  VegaChart,
} from "@databao/multitabs";
import { Text } from "@radix-ui/themes";

import styles from "./App.module.css";

function App() {
  const data = window.__DATA__;

  if (!data) {
    return <Text color="gray">No data available</Text>;
  }

  const renderChart = (spec: object | null) => {
    if (spec) {
      return <VegaChart spec={spec} />;
    }
    return <Text color="gray">No chart available</Text>;
  };

  const renderDescription = (text: string | null) => {
    if (text) {
      return <Text color="gray">{text}</Text>;
    }
    return <Text color="gray">No description available</Text>;
  };

  const renderTable = (dataframeHtmlContent: string | null) => {
    if (dataframeHtmlContent) {
      return <DataframeTable htmlContent={dataframeHtmlContent} />;
    }
    return <Text color="gray">No data available</Text>;
  };

  const tabs: MultimodalTab[] = [
    {
      type: "CHART",
      title: "Chart",
      content: () => renderChart(data.spec),
    },
    {
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(data.text),
    },
    {
      type: "TABLE",
      title: "Data",
      content: () => renderTable(data.dataframeHtmlContent),
    },
  ];

  return (
    <div className={styles.appContainer}>
      <MultimodalTabs tabs={tabs} />
    </div>
  );
}

export default App;
