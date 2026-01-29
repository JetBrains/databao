import { Spinner, Text } from "@radix-ui/themes";
import { ReactElement } from "react";

import { Status } from "../types";
import styles from "./StatusRenderer.module.css";

export type StatusRendererProps<T> = {
  status: Status;
  value: T | null | undefined;
  renderValue: (value: T) => ReactElement;
  empty: ReactElement;
  failed: ReactElement;
  loadingText?: string;
};

export function StatusRenderer<T>({
  status,
  value,
  renderValue,
  empty,
  failed,
  loadingText = "Loading...",
}: StatusRendererProps<T>) {
  if (status === "INIT" || status === "LOADING") {
    return (
      <div className={styles.loader}>
        <Spinner />
        <Text color="gray">{loadingText}</Text>
      </div>
    );
  }

  if (status === "FAILED") {
    return failed;
  }

  if (status === "LOADED" && value != null) {
    return renderValue(value);
  }

  return empty;
}
