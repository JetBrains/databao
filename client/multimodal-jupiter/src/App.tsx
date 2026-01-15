import { useModel, useModelState } from "@anywidget/react";
import {
  DataframeTable,
  MultimodalTab,
  MultimodalTabs,
  VegaChart,
} from "@databao/multitabs";
import { Spinner, Text } from "@radix-ui/themes";
import { Theme } from "@radix-ui/themes/dist/cjs/index.js";
import { useEffect, useRef } from "react";

import styles from "./app.module.css";
import {
  SelectModalityAction,
  InitWidgetAction,
} from "./communication/actions";
import { initCommunication } from "./communication/communication";
import { WidgetStatus } from "./communication/types";
import { MULTIMODAL_TABS, MultimodalTabType } from "./types";

function App() {
  const model = useModel();
  const communication = useRef(initCommunication(model));

  const [status] = useModelState<WidgetStatus>("status");
  const [spec] = useModelState<Record<string, unknown> | null>("spec");
  const [text] = useModelState<string>("text");
  const [dataframeHtmlContent] = useModelState<string>(
    "dataframe_html_content",
  );

  useEffect(() => {
    communication.current.sendMessage<InitWidgetAction>("INIT_WIDGET", null);
    communication.current.sendMessage<SelectModalityAction>(
      "SELECT_MODALITY",
      "CHART",
    );
  }, []);

  const handleChangeTab = (tab: string) => {
    const multimodalTab = tab as MultimodalTabType;

    if (!MULTIMODAL_TABS[multimodalTab]) {
      return;
    }

    communication.current.sendMessage<SelectModalityAction>(
      "SELECT_MODALITY",
      multimodalTab,
    );
  };

  if (status === "initializing") {
    return <Spinner size="2" />;
  }

  const renderChart = (spec: object | null) => {
    if (spec) {
      return <VegaChart spec={spec} />;
    }

    if (status === "computating") {
      return <Text color="gray">Loading...</Text>;
    }

    return <Text color="gray">No chart available</Text>;
  };

  const renderDescription = (text: string | null) => {
    if (text) {
      return <Text color="gray">{text}</Text>;
    }

    if (status === "computating") {
      return <Text color="gray">Loading...</Text>;
    }

    return <Text color="gray">No description available</Text>;
  };

  const renderTable = (dataframeHtmlContent: string | null) => {
    if (dataframeHtmlContent) {
      return <DataframeTable htmlContent={dataframeHtmlContent} />;
    }

    if (status === "computating") {
      return <Text color="gray">Loading...</Text>;
    }

    return <Text color="gray">No data available</Text>;
  };

  const tabs: MultimodalTab[] = [
    {
      type: "CHART",
      title: "Chart",
      content: () => renderChart(spec),
    },
    {
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(text),
    },
    {
      type: "DATAFRAME",
      title: "Data",
      content: () => renderTable(dataframeHtmlContent),
    },
  ];

  return (
    <Theme asChild>
      <div className={styles.root}>
        <MultimodalTabs tabs={tabs} onChangeTab={handleChangeTab} />
      </div>
    </Theme>
  );
}

export default App;
