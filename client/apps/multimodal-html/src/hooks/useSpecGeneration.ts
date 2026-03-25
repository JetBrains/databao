import { useEffect, useState } from "react";

import { subscribeOnSpecGeneration } from "../communication/communication";
import { Status } from "@/types";

export function useSpecGeneration() {
  const [specConfig, setSpecConfig] = useState<Record<string, unknown> | null>(
    null,
  );
  const [specCsvData, setSpecCsvData] = useState<string>("");
  const [specGenerationStatus, setSpecStatus] = useState<Status>("initial");

  useEffect(() => {
    setSpecStatus("loading");

    const onSuccess = (data: Record<string, unknown>) => {
      setSpecConfig(data.spec as Record<string, unknown>);
      setSpecCsvData(data.csvData as string);
      setSpecStatus("loaded");
    };

    const onError = (e: Error) => {
      setSpecStatus("failed");
      console.error(e.message);
    };

    const unsubscribe = subscribeOnSpecGeneration(onSuccess, onError);

    return () => {
      unsubscribe();
    };
  }, []);

  return {
    specConfig,
    specCsvData,
    specGenerationStatus,
  };
}
