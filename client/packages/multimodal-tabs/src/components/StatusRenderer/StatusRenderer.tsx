import { Spinner, Text } from "@radix-ui/themes";
import { ReactElement } from "react";

import type { Status } from "@/types";

import styles from "./statusRenderer.module.css";

export type StatusRendererProps<T> = {
  getStatus: () => Status;
  value: T | null | undefined;
  renderValue: (value: T) => ReactElement;
  empty: ReactElement;
  failed: ReactElement;
  loadingText?: string;
};

export function StatusRenderer<T>({
  getStatus,
  value,
  renderValue,
  empty,
  failed,
  loadingText = "Generating...",
}: StatusRendererProps<T>): ReactElement {
  if (getStatus() === "initial" || getStatus() === "loading") {
    return (
      <div className={styles.loader}>
        <Spinner />
        <Text color="gray">{loadingText}</Text>
      </div>
    );
  }

  if (getStatus() === "failed") {
    return failed;
  }

  if (getStatus() === "loaded" && value != null) {
    return renderValue(value);
  }

  return empty;
}
