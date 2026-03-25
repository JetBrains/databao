import { TableInfoModel, TableTool } from "@jetbrains/drt";
import { Text } from "@radix-ui/themes";

import { useDataService } from "@/hooks";
import type { Status } from "@/types";

import { StatusRenderer } from "../StatusRenderer";

interface DataframeTableProps {
  dataframeCsvData: string;
  status: Status;
}

export function DataframeTable(props: DataframeTableProps) {
  const {
    dataService,
    tableInfo,
    status: dataServiceStatus,
  } = useDataService(props.dataframeCsvData);

  const getStatus = (
    contentGenerationStatus: Status,
    dataServiceStatus: Status,
  ): Status => {
    if (
      contentGenerationStatus === "failed" ||
      dataServiceStatus === "failed"
    ) {
      return "failed";
    }
    if (
      contentGenerationStatus === "loaded" &&
      dataServiceStatus === "loaded"
    ) {
      return "loaded";
    }
    if (
      contentGenerationStatus === "loading" ||
      dataServiceStatus === "loading"
    ) {
      return "loading";
    }
    return "initial";
  };

  const renderTable = (tableInfo: TableInfoModel) => {
    if (!dataService || !tableInfo) {
      return <Text color="gray">Failed to get data</Text>;
    }

    return (
      <TableTool
        tableInfo={tableInfo}
        tableDataService={dataService}
        fitContainerHeight={false}
        truncated={false}
      />
    );
  };

  return (
    <StatusRenderer
      getStatus={() => getStatus(props.status, dataServiceStatus)}
      value={tableInfo}
      renderValue={renderTable}
      failed={<Text color="gray">Failed to get data</Text>}
      empty={<Text color="gray">No data available</Text>}
    />
  );
}
