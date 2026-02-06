import { TableInfoModel, TableTool } from "@jetbrains/drt";
import { Text } from "@radix-ui/themes";

import { useDataService } from "@/hooks";
import { Status } from "@/types";

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

  const getStatus = (contentStatus: Status, dataStatus: Status): Status => {
    if (contentStatus === "failed" || dataStatus === "failed") {
      return "failed";
    }
    if (contentStatus === "loaded" && dataStatus === "loaded") {
      return "loaded";
    }
    if (contentStatus === "loading" || dataStatus === "loading") {
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
