import type { TableDataService, TableInfoModel } from "@jetbrains/drt";

import { CSVRawDataSource, createDataService } from "@jetbrains/drt";
import { useEffect, useState } from "react";

import type { Status } from "../types";

async function createService(csvContent: string) {
  const dataSource = new CSVRawDataSource(csvContent);
  const service = await createDataService(dataSource);
  const tableInfo = await service.getTableInfo();
  return { service, tableInfo };
}

export function useDataService(csvContent: string | undefined) {
  const [dataService, setDataService] = useState<TableDataService | null>(null);
  const [tableInfo, setTableInfo] = useState<TableInfoModel | null>(null);
  const [status, setStatus] = useState<Status>("initial");

  useEffect(() => {
    if (!csvContent) {
      return;
    }

    setStatus("loading");

    const initDataService = async () => {
      try {
        const { service, tableInfo } = await createService(csvContent);
        setDataService(service);
        setTableInfo(tableInfo);
        setStatus("loaded");
      } catch (error) {
        console.error("Failed to create data service:", error);
        setStatus("failed");
      }
    };

    initDataService();
  }, [csvContent]);

  return { dataService, tableInfo, status };
}
