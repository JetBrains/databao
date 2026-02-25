import { Spinner, Text } from "@radix-ui/themes";
import { ReactElement } from "react";

import type { Status } from "../types";

import styles from "./statusRenderer.module.css";

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
  loadingText = "Generating...",
}: StatusRendererProps<T>): ReactElement {
  if (status === "initial" || status === "loading") {
    return (
      <div className={styles.loader}>
        <Spinner />
        <Text color="gray">{loadingText}</Text>
      </div>
    );
  }

  if (status === "failed") {
    return failed;
  }

  if (status === "loaded" && value != null) {
    return renderValue(value);
  }

  return empty;
}
