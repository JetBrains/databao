import { useModel, useModelState } from "@anywidget/react";
import { DataframeTable, MultimodalTabs, VegaChart } from "@databao/multitabs";
import { Text, Theme } from "@radix-ui/themes";
import { useCallback, useEffect, useRef } from "react";

import styles from "./app.module.css";
import { SelectModalityAction } from "./communication/actions";
import { initCommunication } from "./communication/communication";
import { StatusRenderer } from "./components/StatusRenderer";
import { MULTIMODAL_TABS, MultimodalTabType, Status } from "./types";

function isMultimodalTabType(tab: string): tab is MultimodalTabType {
  return tab in MULTIMODAL_TABS;
}

function App() {
  const model = useModel();
  const communication = useRef(initCommunication(model));

  const [availableTabs] = useModelState<MultimodalTabType[]>(
    "available_modalities",
  );

  const [spec] = useModelState<Record<string, unknown> | null>("spec");
  const [specStatus] = useModelState<Status>("spec_status");

  const [text] = useModelState<string>("text");
  const [textStatus] = useModelState<Status>("text_status");

  const [dataframeHtmlContent] = useModelState<string>(
    "dataframe_html_content",
  );
  const [dataframeHtmlContentStatus] = useModelState<Status>(
    "dataframe_html_content_status",
  );

  useEffect(() => {
    const firstAvailableTab = availableTabs[0];
    if (!firstAvailableTab) return;

    communication.current.sendMessage<SelectModalityAction>(
      "SELECT_MODALITY",
      firstAvailableTab,
    );
  }, [availableTabs]);

  const handleChangeTab = useCallback((tab: string) => {
    if (!isMultimodalTabType(tab)) {
      console.error("Unknown tab value");
      return;
    }

    communication.current.sendMessage<SelectModalityAction>(
      "SELECT_MODALITY",
      tab,
    );
  }, []);

  const renderChart = (spec: Record<string, unknown> | null) => (
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
      status={textStatus}
      value={text}
      renderValue={(value) => <Text color="gray">{value}</Text>}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No description available</Text>}
    />
  );

  const renderTable = (dataframeHtmlContent: string | null) => (
    <StatusRenderer
      status={dataframeHtmlContentStatus}
      value={dataframeHtmlContent}
      renderValue={(value) => <DataframeTable htmlContent={value} />}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No data available</Text>}
    />
  );

  const defaultTabs = {
    DATAFRAME: {
      type: MULTIMODAL_TABS.DATAFRAME,
      title: "Data",
      content: () => renderTable(dataframeHtmlContent),
    },
    CHART: {
      type: MULTIMODAL_TABS.CHART,
      title: "Chart",
      content: () => renderChart(spec),
    },
    DESCRIPTION: {
      type: MULTIMODAL_TABS.DESCRIPTION,
      title: "Description",
      content: () => renderDescription(text),
    },
  };

  const tabs = availableTabs
    .map((tab) => defaultTabs[tab])
    .filter((tab) => isMultimodalTabType(tab.type));

  return (
    <Theme asChild>
      <div className={styles.root}>
        <MultimodalTabs tabs={tabs} onChangeTab={handleChangeTab} />
      </div>
    </Theme>
  );
}

export default App;
