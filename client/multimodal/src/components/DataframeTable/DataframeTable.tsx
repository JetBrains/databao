import styles from "./dataframeTable.module.css";

interface TableProps {
  htmlContent: string;
}

export function DataframeTable(props: TableProps) {
  return (
    <div
      className={styles.root}
      dangerouslySetInnerHTML={{ __html: props.htmlContent }}
    />
  );
}
