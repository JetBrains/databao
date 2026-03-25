import { useModel, useModelState } from "@anywidget/react";
import {
  DataframeTable,
  Tabs,
  VegaChart,
  StatusRenderer,
} from "@databao/multimodal-tabs";
import { Text, Theme } from "@radix-ui/themes";
import { Sprite } from "@jetbrains/drt";
import { useCallback, useEffect, useRef } from "react";

import styles from "./app.module.css";
import { SelectModalityAction } from "./communication/actions";
import { initCommunication } from "./communication/communication";
import {
  isMultimodalTabType,
  MULTIMODAL_TABS,
  MultimodalTabType,
  Status,
} from "./types";

function App() {
  const model = useModel();
  const communication = useRef(initCommunication(model));

  const [availableTabs] = useModelState<MultimodalTabType[]>(
    "available_modalities",
  );

  const [specConfig] = useModelState<Record<string, unknown> | null>("spec");
  const [specGenerationStatus] = useModelState<Status>("spec_status");
  const [specCsvData] = useModelState<string>("spec_csv_data");

  const [text] = useModelState<string>("text");
  const [textStatus] = useModelState<Status>("text_status");

  const [dataframeCsvContent] = useModelState<string>("dataframe_csv_content");
  const [dataframeCsvContentStatus] = useModelState<Status>(
    "dataframe_csv_content_status",
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

  const renderDescription = (text: string | null) => (
    <StatusRenderer
      getStatus={() => textStatus}
      value={text}
      renderValue={(value) => (
        <div className={styles.heightWrapper}>
          <Text color="gray">{value}</Text>
        </div>
      )}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No description available</Text>}
    />
  );

  const defaultTabs = {
    DATAFRAME: {
      type: MULTIMODAL_TABS.DATAFRAME,
      title: "Data",
      content: () => (
        <div className={styles.heightWrapper}>
          <DataframeTable
            dataframeCsvData={dataframeCsvContent}
            status={dataframeCsvContentStatus}
          />
        </div>
      ),
    },
    CHART: {
      type: MULTIMODAL_TABS.CHART,
      title: "Chart",
      content: () => (
        <div className={styles.heightWrapper}>
          <VegaChart
            status={specGenerationStatus}
            specData={specCsvData}
            specConfig={specConfig}
          />
        </div>
      ),
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
      <Theme style={{ minHeight: "300px", maxHeight: "700px" }} asChild>
        <div className={styles.root}>
          <Tabs tabs={tabs} onChangeTab={handleChangeTab} />
        </div>
      </Theme>
    </>
  );
}

export default App;
