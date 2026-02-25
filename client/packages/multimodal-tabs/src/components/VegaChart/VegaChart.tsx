import { VisualizationTool } from "@jetbrains/drt";
import { Text } from "@radix-ui/themes";

import { useDataService } from "@/hooks";
import { Status } from "@/types";

import { StatusRenderer } from "../StatusRenderer";

interface VegaChartProps {
  status: Status;
  specConfig: Record<string, unknown> | null;
  specData: string;
}

export function VegaChart(props: VegaChartProps) {
  const {
    dataService,
    tableInfo,
    status: dataServiceStatus,
  } = useDataService(props.specData);

  const getStatus = (
    specGenerationStatus: Status,
    dataServiceStatus: Status,
  ): Status => {
    if (specGenerationStatus === "failed" || dataServiceStatus === "failed") {
      return "failed";
    }
    if (specGenerationStatus === "loaded" && dataServiceStatus === "loaded") {
      return "loaded";
    }
    if (specGenerationStatus === "loading" || dataServiceStatus === "loading") {
      return "loading";
    }
    return "initial";
  };

  const renderChart = (value: Record<string, unknown>) => {
    if (!dataService || !tableInfo) {
      return <Text color="gray">Failed to get data</Text>;
    }

    return (
      <VisualizationTool
        tableInfo={tableInfo}
        tableDataService={dataService}
        spec={value}
      />
    );
  };

  return (
    <StatusRenderer
      getStatus={() => getStatus(props.status, dataServiceStatus)}
      value={props.specConfig}
      renderValue={renderChart}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No chart available</Text>}
    />
  );
}
